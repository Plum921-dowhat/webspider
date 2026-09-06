"""Generate embeddings for a chunk JSONL via an OpenAI-compatible API.

Reads corpus_chunks.jsonl, POSTs texts in batches to
{EMBEDDING_API_BASE}/embeddings, and writes chunk ids + float32 vectors to an
.npz archive (numpy is the only extra dependency). Re-runs skip ids already
present in the output file, so interrupted runs resume for free.

Environment:
    EMBEDDING_API_BASE  e.g. https://api.openai.com/v1   (required)
    EMBEDDING_API_KEY                                        (required)
    EMBEDDING_MODEL     e.g. text-embedding-3-small        (required)

Usage:
    python embed_chunks.py --chunks corpus_chunks.jsonl --out corpus_vectors.npz
"""
import argparse
import json
import os
import time

import numpy as np
import requests

BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "64"))
SLEEP_BETWEEN = float(os.getenv("EMBED_SLEEP", "0.2"))


def batch_texts(items, size):
    """Yield (offset, [text, ...]) batches of at most `size` items."""
    for i in range(0, len(items), size):
        yield i, [it["text"] for it in items[i:i + size]]


def load_existing(path):
    """Return (ids list, vectors list) already stored in the .npz, if any."""
    if not os.path.exists(path):
        return [], []
    data = np.load(path, allow_pickle=False)
    return list(data["ids"]), list(data["vectors"])


def embed_batch(url, key, model, texts, timeout=60):
    resp = requests.post(
        f"{url.rstrip('/')}/embeddings",
        headers={"Authorization": f"Bearer {key}"},
        json={"model": model, "input": texts},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()["data"]
    # API returns objects with an "index" field; sort to preserve order
    data.sort(key=lambda d: d["index"])
    return [d["embedding"] for d in data]


def run(chunks_path, out_path, api_base, api_key, model):
    with open(chunks_path, encoding="utf-8") as f:
        items = [json.loads(line) for line in f if line.strip()]
    done_ids, done_vecs = load_existing(out_path)
    seen = set(done_ids)
    todo = [it for it in items if it["chunk_id"] not in seen]
    print(f"chunks={len(items)} already_embedded={len(done_ids)} todo={len(todo)}")
    if not todo:
        return

    ids, vecs = list(done_ids), list(done_vecs)
    dim = None
    for offset, texts in batch_texts(todo, BATCH_SIZE):
        for attempt in range(3):
            try:
                embeddings = embed_batch(api_base, api_key, model, texts)
                break
            except (requests.RequestException, KeyError, ValueError) as e:
                if attempt == 2:
                    raise
                print(f"[embed] batch failed ({e}); retry {attempt + 1}/2")
                time.sleep(2 ** attempt)
        for j, emb in enumerate(embeddings):
            if dim is None:
                dim = len(emb)
                print(f"embedding dim={dim}")
            ids.append(todo[offset + j]["chunk_id"])
            vecs.append(emb)
        time.sleep(SLEEP_BETWEEN)
        if len(ids) % (BATCH_SIZE * 20) == 0:
            print(f"  embedded {len(ids)}/{len(items)}")

    matrix = np.asarray(vecs, dtype=np.float32)
    np.savez_compressed(out_path, ids=np.asarray(ids), vectors=matrix)
    with open(out_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"model": model, "dim": int(matrix.shape[1]), "count": int(matrix.shape[0])}, f)
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
