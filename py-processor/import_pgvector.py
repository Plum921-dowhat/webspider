"""Import chunked corpus + embeddings into PostgreSQL with pgvector.

Two-phase write for performance:
  --initial : COPY FROM STDIN (no per-row conflict check), then build the HNSW
              index once at the end — bulk loads are ~10x faster this way
  default   : batched ON CONFLICT upsert for incremental syncs (small volume)

Prerequisites (see README "RAG 出口"):
  - Postgres with the pgvector extension (compose image pgvector/pgvector:pg16)
  - corpus_chunks.jsonl (rag_export.py) and corpus_vectors.npz (embed_chunks.py)

Usage:
    python import_pgvector.py --initial \
        --chunks corpus_chunks.jsonl --vectors corpus_vectors.npz
    python import_pgvector.py --chunks ... --vectors ...   # incremental
"""
import argparse
import csv
import io
import json
import time

import numpy as np
import psycopg2

from config import PG_DSN

BATCH_ROWS = 200       # rows per upsert batch (incremental path)
COMMIT_ROWS = 10000    # rows per transaction on the COPY path
INDEX_PARAMS = "WITH (m = 16, ef_construction = 64)"

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

_COPY_COLUMNS = ("chunk_id, article_id, url, title, heading_path, chunk_index, "
                 "n_chunks, text, tags, source_type, language, published_at, "
                 "quality_score, embedding")


def _vector_literal(vec):
    return "[" + ",".join(f"{x:.6g}" for x in vec.tolist()) + "]"


def _row_values(chunk_id, vec, chunks):
    c = chunks.get(chunk_id)
    if c is None:
        return None
    return (
        chunk_id, c.get("article_id"), c["url"], c.get("title"),
        c.get("heading_path") or [], c.get("chunk_index"), c.get("n_chunks"),
        c["text"], c.get("tags") or [], c.get("source_type"), c.get("language"),
        c.get("published_at"), c.get("quality_score"),
        _vector_literal(vec),
    )


def load_pairs(vectors_path, chunks_path):
    with open(vectors_path + ".meta.json", encoding="utf-8") as f:
        meta = json.load(f)
    print(f"vectors: model={meta['model']} dim={meta['dim']} count={meta['count']}")
    with open(chunks_path, encoding="utf-8") as f:
        chunks = {c["chunk_id"]: c for c in (json.loads(line) for line in f if line.strip())}
    data = np.load(vectors_path, allow_pickle=False)
    return chunks, list(data["ids"]), data["vectors"], meta["dim"]


def _pg_array(values):
    """Postgres text[] literal: {"a","b"} with quote escaping; {} when empty."""
    inner = ",".join('"{}"'.format(v.replace('"', '\\"')) for v in values)
    return "{" + inner + "}"


def copy_initial(conn, ids, vectors, chunks):
    """Fast bulk path: COPY FROM STDIN, one transaction per COMMIT_ROWS rows.
    Each batch serializes into a fresh buffer — no reused-buffer residue."""
    start = time.time()
    copied = 0
    batch_rows = []
    with conn.cursor() as cur:
        for chunk_id, vec in zip(ids, vectors):
            row = _row_values(chunk_id, vec, chunks)
            if row is None:
                continue
            batch_rows.append((
                row[0], row[1], row[2], row[3],
                _pg_array(row[4]), row[5], row[6], row[7], _pg_array(row[8]),
                row[9], row[10], row[11], row[12], row[13],
            ))
            copied += 1
            if len(batch_rows) >= COMMIT_ROWS:
                _copy_rows(cur, conn, batch_rows)
                print(f"  copied {copied} rows ({time.time() - start:.0f}s)")
                batch_rows = []
        if batch_rows:
            _copy_rows(cur, conn, batch_rows)
            print(f"  copied {copied} rows ({time.time() - start:.0f}s)")
    return copied


def _copy_rows(cur, conn, batch_rows):
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerows(batch_rows)
    buf.seek(0)
    cur.copy_expert(
        f"COPY article_chunks ({_COPY_COLUMNS}) FROM STDIN WITH (FORMAT csv)", buf)
    conn.commit()


def upsert_incremental(conn, ids, vectors, chunks):
    from psycopg2.extras import execute_values
    start = time.time()
    imported = 0
    with conn.cursor() as cur:
        pending = []
        for chunk_id, vec in zip(ids, vectors):
            row = _row_values(chunk_id, vec, chunks)
            if row is None:
                continue
            pending.append(row + (_vector_literal(vec),))
            if len(pending) >= BATCH_ROWS:
                execute_values(cur, _UPSERT, pending, template=(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)"),)
                conn.commit()
                imported += len(pending)
                pending = []
        if pending:
            execute_values(cur, _UPSERT, pending, template=(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector)"),)
            conn.commit()
            imported += len(pending)
    print(f"upserted {imported} rows ({time.time() - start:.0f}s)")
    return imported


def build_index(conn):
    start = time.time()
    with conn.cursor() as cur:
        cur.execute("SET maintenance_work_mem = '512MB'")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_chunks_hnsw "
                    f"ON article_chunks USING hnsw (embedding vector_cosine_ops) {INDEX_PARAMS}")
        cur.execute("ANALYZE article_chunks")
    conn.commit()
    print(f"HNSW index ready + ANALYZE ({time.time() - start:.0f}s)")


def index_exists(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = 'idx_chunks_hnsw'")
        return cur.fetchone() is not None


def main(chunks_path, vectors_path, initial):
    conn = psycopg2.connect(PG_DSN)
    try:
        chunks, ids, vectors, dim = load_pairs(vectors_path, chunks_path)
        with conn.cursor() as cur:
            cur.execute(_SCHEMA, {"dim": dim})
        conn.commit()

        had_index = index_exists(conn)
        if initial:
            if had_index:
                print("dropping HNSW index for the bulk load (will be rebuilt)")
                with conn.cursor() as cur:
                    cur.execute("DROP INDEX IF EXISTS idx_chunks_hnsw")
                conn.commit()
            n = copy_initial(conn, ids, vectors, chunks)
        else:
            n = upsert_incremental(conn, ids, vectors, chunks)

        if initial or not had_index:
            build_index(conn)
        else:
            conn.commit()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM article_chunks")
            print(f"done: {n} rows written, table now holds {cur.fetchone()[0]} chunks")
    finally:
        conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunks", default="corpus_chunks.jsonl")
    ap.add_argument("--vectors", default="corpus_vectors.npz")
    ap.add_argument("--initial", action="store_true",
                    help="fast bulk load (COPY) + index build at the end; use for the first import")
    args = ap.parse_args()
    main(args.chunks, args.vectors, args.initial)
