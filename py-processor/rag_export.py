"""Chunk stored articles into a RAG-ready JSONL corpus.

Reads articles from PostgreSQL, splits content_md with chunker.split_markdown,
and writes one JSON object per chunk with full retrieval metadata. Supports
incremental runs via a state file (tracks the max exported article id).

Usage:
    python rag_export.py                                  # incremental
    python rag_export.py --full --out corpus_chunks.jsonl # re-export all
"""
import argparse
import hashlib
import json
import os

from chunker import split_markdown
from store import get_conn, putconn


def _chunk_id(url, chunk_index):
    return hashlib.sha256(url.encode()).hexdigest()[:16] + f"_{chunk_index}"


def article_to_chunks(row, target, overlap, min_chars):
    """row: (id, url, title, content_md, author, published_at, tags,
    source_type, language, quality_score) -> list of chunk dicts."""
    (aid, url, title, content, _author, published, tags, source_type, language, score) = row
    pieces = split_markdown(content or "", target=target, overlap=overlap, min_chars=min_chars)
    n = len(pieces)
    published_iso = published.isoformat() if published else None
    chunks = []
    for i, piece in enumerate(pieces):
        chunks.append({
            "chunk_id": _chunk_id(url, i),
            "article_id": aid,
            "url": url,
            "title": title,
            "heading_path": piece["heading_path"],
            "chunk_index": i,
            "n_chunks": n,
            "text": piece["text"],
            "tags": tags or [],
            "source_type": source_type,
            "language": language,
            "published_at": published_iso,
            "quality_score": float(score) if score is not None else None,
        })
    return chunks


def load_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f).get("last_id", 0)
    except (OSError, json.JSONDecodeError, ValueError):
        return 0


def save_state(path, last_id):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_id": last_id}, f)
    os.replace(tmp, path)


def export_chunks(out_path, state_path, full=False, target=1200, overlap=150, min_chars=30):
    last_id = 0 if full else load_state(state_path)
    conn = get_conn()
    written = 0
    max_id = last_id
    try:
        with conn.cursor(name="rag_export_cur") as cur:
            cur.itersize = 500
            cur.execute(
                """
                SELECT id, url, title, content_md, author, published_at, tags,
                       source_type, language, quality_score
                FROM articles
                WHERE id > %s AND content_md IS NOT NULL
                ORDER BY id
                """,
                (last_id,),
            )
            mode = "w" if full else "a"
            with open(out_path, mode, encoding="utf-8") as f:
                for row in cur:
                    chunks = article_to_chunks(row, target, overlap, min_chars)
                    for c in chunks:
                        f.write(json.dumps(c, ensure_ascii=False) + "\n")
                    written += len(chunks)
                    max_id = max(max_id, row[0])
    finally:
        putconn(conn)
    if not full:
        save_state(state_path, max_id)
    print(f"exported {written} chunks (articles up to id {max_id}) -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="corpus_chunks.jsonl")
    ap.add_argument("--state", default="rag_export_state.json",
                    help="incremental cursor file; stores the max exported article id")
    ap.add_argument("--full", action="store_true", help="re-export everything, overwrite output")
    ap.add_argument("--target", type=int, default=1200, help="target chunk size in chars")
    ap.add_argument("--overlap", type=int, default=150, help="overlap chars between chunks")
    ap.add_argument("--min-chars", type=int, default=30, help="drop chunks shorter than this")
    args = ap.parse_args()
    export_chunks(args.out, args.state, full=args.full,
                  target=args.target, overlap=args.overlap, min_chars=args.min_chars)
