"""Generate embeddings for a chunk JSONL via an OpenAI-compatible API.

Reads corpus_chunks.jsonl, POSTs texts in batches to
{EMBEDDING_API_BASE}/embeddings, and writes chunk ids + float32 vectors to an
.npz archive. Designed for long unattended runs:

  - adaptive throttling: >= EMBED_MIN_INTERVAL between requests, backing off
    on 429/5xx with full Retry-After support (8 attempts, exponential + jitter)
  - a batch that exhausts its retries is SKIPPED and its chunk ids recorded to
    <out>.failed.json — one hot streak of rate limiting never kills the run;
    re-running the script retries exactly those ids
  - periodic .npz saves (EMBED_SAVE_EVERY) so an interrupted run keeps progress

Environment:
    EMBEDDING_API_BASE   e.g. https://api.siliconflow.cn/v1   (required)
    EMBEDDING_API_KEY                                         (required)
    EMBEDDING_MODEL      e.g. BAAI/bge-m3                     (required)

Usage:
    python embed_chunks.py --chunks corpus_chunks.jsonl --out corpus_vectors.npz
"""
import argparse
import json
import os
import random
import time

import numpy as np
import requests

BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "32"))
MIN_INTERVAL = float(os.getenv("EMBED_MIN_INTERVAL", "1.0"))
MAX_ATTEMPTS = int(os.getenv("EMBED_MAX_ATTEMPTS", "8"))
SAVE_EVERY = int(os.getenv("EMBED_SAVE_EVERY", "5000"))
TIMEOUT = int(os.getenv("EMBED_TIMEOUT", "60"))


def batch_texts(items, size):
    """Yield (offset, [text, ...]) batches of at most `size` items."""
    for i in range(0, len(items), size):
        yield i, [it["text"] for it in items[i:i + size]]


def _parse_retry_after(value):
    """Retry-After header (delta-seconds) -> float seconds or None."""
    if not value:
        return None
    try:
        v = float(value)
        return max(v, 0.0)
    except ValueError:
        return None


def load_existing(path):
    """Return (ids list, vectors list) already stored in the .npz, if any."""
    if not os.path.exists(path):
        return [], []
    data = np.load(path, allow_pickle=False)
    return list(data["ids"]), list(data["vectors"])


def embed_batch_retry(url, key, model, texts):
    """One embeddings request with full 429/5xx retry policy. Raises
    RuntimeError after MAX_ATTEMPTS."""
    last_err = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.post(
                f"{url.rstrip('/')}/embeddings",
                headers={"Authorization": f"Bearer {key}"},
                json={"model": model, "input": texts},
                timeout=TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()["data"]
                data.sort(key=lambda d: d["index"])
                return [d["embedding"] for d in data]
            retry_after = _parse_retry_after(resp.headers.get("Retry-After"))
            last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except requests.RequestException as e:
            last_err = str(e)
            retry_after = None
        wait = retry_after if retry_after is not None else \
            min(300.0, 2.0 ** attempt) + random.uniform(0, 1.0)
        print(f"[embed] attempt {attempt + 1}/{MAX_ATTEMPTS} failed ({last_err}); "
              f"retry in {wait:.0f}s")
        time.sleep(wait)
    raise RuntimeError(f"embed batch failed after {MAX_ATTEMPTS} attempts: {last_err}")


def run(chunks_path, out_path, api_base, api_key, model):
    with open(chunks_path, encoding="utf-8") as f:
        items = [json.loads(line) for line in f if line.strip()]
    done_ids, done_vecs = load_existing(out_path)
    seen = set(done_ids)
    todo = [it for it in items if it["chunk_id"] not in seen]
    print(f"chunks={len(items)} already_embedded={len(done_ids)} todo={len(todo)}")

    ids, vecs = list(done_ids), list(done_vecs)
    failed_ids = []
    failed_path = out_path + ".failed.json"
    dim = None
    last_call = 0.0
    started = time.time()
    embedded_total = len(done_ids)

    for offset, texts in batch_texts(todo, BATCH_SIZE):
        wait = MIN_INTERVAL - (time.time() - last_call)
        if wait > 0:
            time.sleep(wait)
        last_call = time.time()
        try:
            embeddings = embed_batch_retry(api_base, api_key, model, texts)
        except RuntimeError as e:
            print(f"[embed] batch skipped after retries: {e}")
            failed_ids.extend(todo[offset + j]["chunk_id"] for j in range(len(texts)))
            continue
        for j, emb in enumerate(embeddings):
            if dim is None:
                dim = len(emb)
                print(f"embedding dim={dim}")
            ids.append(todo[offset + j]["chunk_id"])
            vecs.append(emb)
        embedded_total += len(texts)
        if embedded_total % (BATCH_SIZE * 20) < BATCH_SIZE:
            rate = embedded_total / max((time.time() - started) / 60, 1e-9)
            print(f"  progress {len(ids)}/{len(items)} ({rate:.0f} chunks/min)")

        if len(ids) - len(done_ids) >= SAVE_EVERY or len(ids) == len(items):
            matrix = np.asarray(vecs, dtype=np.float32)
            np.savez_compressed(out_path, ids=np.asarray(ids), vectors=matrix)
            done_ids = list(ids)

    matrix = np.asarray(vecs, dtype=np.float32)
    np.savez_compressed(out_path, ids=np.asarray(ids), vectors=matrix)
    with open(out_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"model": model, "dim": int(matrix.shape[1]), "count": int(matrix.shape[0])}, f)
    if failed_ids:
        with open(failed_path, "w", encoding="utf-8") as f:
            json.dump(failed_ids, f)
        print(f"WARNING: {len(failed_ids)} chunks failed after retries -> {failed_path} "
              f"(re-run this script to retry exactly those)")
    print(f"wrote {matrix.shape[0]} vectors (dim={matrix.shape[1]}) -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", default="corpus_chunks.jsonl")
    ap.add_argument("--out", default="corpus_vectors.npz")
    args = ap.parse_args()
    base = os.getenv("EMBEDDING_API_BASE")
    key = os.getenv("EMBEDDING_API_KEY")
    model = os.getenv("EMBEDDING_MODEL")
    if not (base and key and model):
        raise SystemExit("set EMBEDDING_API_BASE / EMBEDDING_API_KEY / EMBEDDING_MODEL (see .env.example)")
    run(args.chunks, args.out, base, key, model)
