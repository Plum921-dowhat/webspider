"""PostgreSQL storage with upsert-on-url_hash dedup."""
import threading

from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

from config import PG_DSN

# Process-wide pool. ThreadedConnectionPool (not SimpleConnectionPool) because
# FastAPI serves requests in a threadpool and the processor may touch the DB
# from multiple entry points.
_POOL_MIN = 1
_POOL_MAX = 10

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadedConnectionPool(_POOL_MIN, _POOL_MAX, PG_DSN)
    return _pool


def get_conn():
    """Check a connection out of the pool. Pair with putconn(); never call
    conn.close() directly on a pooled connection — that leaks the slot."""
    return _get_pool().getconn()


def putconn(conn, close=False):
    """Return a connection to the pool. Uncommitted transactions are rolled
    back by psycopg2; pass close=True for a broken connection."""
    pool = _pool
    if pool is not None:
        pool.putconn(conn, close=close)


_INSERT = """
INSERT INTO articles
    (url, url_hash, title, content_md, author, published_at, tags, source_type, language, content_hash, quality_score)
VALUES %s
ON CONFLICT (url_hash) DO NOTHING
"""


def insert_articles(conn, rows):
    """rows: list of tuples in column order of _INSERT (content_hash last).

    Returns the number of rows ACTUALLY inserted: with `ON CONFLICT DO NOTHING`
    the command tag `INSERT 0 n` (exposed via cur.rowcount) reflects only the
    rows that were not skipped, so duplicate URLs are no longer miscounted.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, _INSERT, rows, page_size=200)
        inserted = cur.rowcount
    conn.commit()
    return inserted
