"""One-command incremental corpus -> vectors -> pgvector sync.

Chains the three existing stages, using pgvector as the source of truth for
what has already been ingested:

    1. chunk articles newer than max(article_id) in article_chunks
    2. embed the new chunks (resume-safe: chunk_ids already in the .npz skip)
    3. upsert into pgvector (incremental path)

First ever run: python sync_vectors.py --initial   (full export + COPY + index)
Subsequent:     python sync_vectors.py

Requires EMBEDDING_* env vars (see .env.example). The embedding step can take
about an hour on the full corpus; it is resumable, so re-running continues.
"""
import argparse
import json
import os

from chunker import split_markdown
from config import EMBEDDING_API_BASE, EMBEDDING_API_KEY, EMBEDDING_MODEL
from store import get_conn, putconn

TMP_CHUNKS = "/tmp/sync_chunks.jsonl"
TMP_VECTORS = "/tmp/sync_vectors.npz"


def _max_ingested_article_id(conn):
    with conn.cursor() as cur:
        try:
            cur.execute("SELECT COALESCE(max(article_id), 0) FROM article_chunks")
            return cur.fetchone()[0] or 0
        except Exception:
            conn.rollback()
            return 0


def export_new_chunks(conn, since_id, out_path):
    """Chunk articles with id > since_id into out_path. Returns (chunks, max_id)."""
    from rag_export import article_to_chunks
    written = 0
    max_id = since_id
    with conn.cursor(name="sync_cur") as cur:
        cur.itersize = 500
        cur.execute(
            """
            SELECT id, url, title, content_md, author, published_at, tags,
                   source_type, language, quality_score
            FROM articles
            WHERE id > %s AND content_md IS NOT NULL
            ORDER BY id
            """,
            (since_id,),
        )
        with open(out_path, "w", encoding="utf-8") as f:
            for row in cur:
                for c in article_to_chunks(row, 1200, 150, 30):
                    f.write(json.dumps(c, ensure_ascii=False) + "\n")
                    written += 1
                max_id = max(max_id, row[0])
    return written, max_id


def main(initial):
    if not (EMBEDDING_API_BASE and EMBEDDING_API_KEY and EMBEDDING_MODEL):
        raise SystemExit("set EMBEDDING_API_BASE / EMBEDDING_API_KEY / EMBEDDING_MODEL (see .env.example)")

    conn = get_conn()
    try:
        last_id = 0 if initial else _max_ingested_article_id(conn)
        if not initial and last_id == 0:
            print("article_chunks is empty — this looks like the first run; "
                  "use --initial for the fast COPY path")
        print(f"syncing articles with id > {last_id}")
        written, max_id = export_new_chunks(conn, last_id, TMP_CHUNKS)
        print(f"new chunks: {written}")
    finally:
        putconn(conn)
    if written == 0:
        print("nothing to sync")
        return

    os.environ.setdefault("EMBED_BATCH_SIZE", "32")
    from embed_chunks import run as embed_run
    embed_run(TMP_CHUNKS, TMP_VECTORS, EMBEDDING_API_BASE, EMBEDDING_API_KEY, EMBEDDING_MODEL)

    from import_pgvector import main as import_main
    import_main(TMP_CHUNKS, TMP_VECTORS, initial=initial and last_id == 0)
    print(f"sync complete: corpus now covers articles up to id {max_id}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--initial", action="store_true",
                    help="first run: full export + fast COPY bulk load + index build")
    args = ap.parse_args()
    main(args.initial)
