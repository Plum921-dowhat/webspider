"""Import chunked corpus + embeddings into PostgreSQL with pgvector.

Prerequisites (see README "RAG 出口"):
  - Postgres with the pgvector extension (swap the compose image to
    pgvector/pgvector:pg16)
  - corpus_chunks.jsonl (rag_export.py) and corpus_vectors.npz (embed_chunks.py)

Usage:
    python import_pgvector.py --chunks corpus_chunks.jsonl --vectors corpus_vectors.npz
"""
import argparse
import json

import numpy as np
import psycopg2
from psycopg2.extras import execute_values

from config import PG_DSN

_SCHEMA = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS article_chunks (
    chunk_id      TEXT PRIMARY KEY,
    article_id    BIGINT,
    url           TEXT NOT NULL,
    title         TEXT,
    heading_path  TEXT[] DEFAULT '{}',
    chunk_index   INT,
    n_chunks      INT,
    text          TEXT NOT NULL,
    tags          TEXT[] DEFAULT '{}',
    source_type   TEXT,
    language      TEXT,
    published_at  TIMESTAMPTZ,
    quality_score REAL,
    embedding     vector(%(dim)s)
);
"""

_UPSERT = """
INSERT INTO article_chunks
    (chunk_id, article_id, url, title, heading_path, chunk_index, n_chunks,
     text, tags, source_type, language, published_at, quality_score, embedding)
VALUES %s
ON CONFLICT (chunk_id) DO UPDATE SET
    text = EXCLUDED.text,
    heading_path = EXCLUDED.heading_path,
    embedding = EXCLUDED.embedding
"""


def main(chunks_path, vectors_path, batch=200):
    meta_path = vectors_path + ".meta.json"
    with open(meta_path, encoding="utf-8") as f:
        meta = json.load(f)
    print(f"vectors: model={meta['model']} dim={meta['dim']} count={meta['count']}")

    with open(chunks_path, encoding="utf-8") as f:
        chunks = {c["chunk_id"]: c for c in (json.loads(line) for line in f if line.strip())}

    data = np.load(vectors_path, allow_pickle=False)
    ids = list(data["ids"])
    vectors = data["vectors"]

    conn = psycopg2.connect(PG_DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(_SCHEMA, {"dim": meta["dim"]})
        conn.commit()

        def rows():
            for chunk_id, vec in zip(ids, vectors):
                c = chunks.get(chunk_id)
                if c is None:
                    continue
                yield (
                    chunk_id, c.get("article_id"), c["url"], c.get("title"),
                    c.get("heading_path") or [], c.get("chunk_index"), c.get("n_chunks"),
                    c["text"], c.get("tags") or [], c.get("source_type"), c.get("language"),
                    c.get("published_at"), c.get("quality_score"),
                    "[" + ",".join(f"{x:.6g}" for x in vec.tolist()) + "]",
                )

        with conn.cursor() as cur:
            execute_values(cur, _UPSERT, rows(), template=(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)"),
                page_size=batch)
        conn.commit()
        print(f"imported {len(ids)} chunks into article_chunks")
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", default="corpus_chunks.jsonl")
    ap.add_argument("--vectors", default="corpus_vectors.npz")
    args = ap.parse_args()
    main(args.chunks, args.vectors)
