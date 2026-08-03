"""PostgreSQL storage with upsert-on-url_hash dedup."""
from psycopg2.extras import execute_values
import psycopg2
from config import PG_DSN


def get_conn():
    return psycopg2.connect(PG_DSN)


_INSERT = """
INSERT INTO articles
    (url, url_hash, title, content_md, author, published_at, tags, source_type, language, content_hash)
VALUES %s
ON CONFLICT (url_hash) DO NOTHING
"""


def insert_articles(conn, rows):
    """rows: list of tuples in column order of _INSERT (content_hash last)."""
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, _INSERT, rows, page_size=200)
    conn.commit()
    return len(rows)
