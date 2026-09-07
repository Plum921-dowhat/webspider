"""Semantic search over pgvector (RAG retrieval).

Query path: rate-limited (RAG_QPS) -> query embedding via the configured
OpenAI-compatible API (LRU-cached, so repeated queries cost nothing) ->
pgvector cosine Top-K through the HNSW index with metadata filters.
"""
import threading
import time
from functools import lru_cache

import requests

from config import (
    EMBEDDING_API_BASE, EMBEDDING_API_KEY, EMBEDDING_MODEL, RAG_QPS, RAG_TOP_K,
)
from store import get_conn, putconn


class RateLimited(Exception):
    """Raised when the query rate exceeds RAG_QPS; carries retry seconds."""

    def __init__(self, retry_after):
        self.retry_after = retry_after
        super().__init__(f"rate limited, retry in {retry_after:.0f}s")


class SearchUnavailable(Exception):
    """Embedding backend not configured or unreachable."""


_rl_lock = threading.Lock()
_next_ok = [0.0]


def _throttle():
    """Fixed-rate gate: at most 1 query per 1/RAG_QPS seconds."""
    with _rl_lock:
        now = time.monotonic()
        wait = (1.0 / RAG_QPS) - (now - _next_ok[0])
        if wait > 0:
            raise RateLimited(wait)
        _next_ok[0] = now + (1.0 / RAG_QPS)


@lru_cache(maxsize=128)
def _embed_query_cached(query):
    """Query -> embedding vector. LRU so repeated queries skip the API."""
    if not (EMBEDDING_API_BASE and EMBEDDING_API_KEY and EMBEDDING_MODEL):
        raise SearchUnavailable(
            "embedding backend not configured (set EMBEDDING_API_BASE/"
            "EMBEDDING_API_KEY/EMBEDDING_MODEL, see .env.example)")
    resp = requests.post(
        f"{EMBEDDING_API_BASE.rstrip('/')}/embeddings",
        headers={"Authorization": f"Bearer {EMBEDDING_API_KEY}"},
        json={"model": EMBEDDING_MODEL, "input": [query]},
        timeout=30,
    )
    resp.raise_for_status()
    return tuple(resp.json()["data"][0]["embedding"])


def semantic_search(query, k=RAG_TOP_K, source=None, min_quality=None, since=None):
    """Return Top-K chunks with citation metadata, best match first."""
    query = (query or "").strip()
    if not query:
        return []
    _throttle()
    vec = _embed_query_cached(query)
    literal = "[" + ",".join(f"{x:.6g}" for x in vec) + "]"

    where = ["1=1"]
    params = []
    if source:
        where.append("source_type = %s")
        params.append(source)
    if min_quality is not None:
        where.append("quality_score >= %s")
        params.append(min_quality)
    if since:
        where.append("published_at >= %s::timestamptz")
        params.append(since)
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT chunk_id, url, title, heading_path, chunk_index, text,
               source_type, published_at, quality_score,
               1 - (embedding <=> %s::vector) AS score
        FROM article_chunks
        WHERE {where_sql}
        ORDER BY embedding <=> %s::vector
        LIMIT %s
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, [literal] + params + [literal, k])
            rows = cur.fetchall()
    finally:
        putconn(conn)

    return [
        {
            "chunk_id": r[0],
            "url": r[1],
            "title": r[2],
            "heading_path": list(r[3] or []),
            "chunk_index": r[4],
            "text": r[5],
            "source_type": r[6],
            "published_at": r[7].isoformat() if r[7] else None,
            "quality_score": float(r[8]) if r[8] is not None else None,
            "score": round(float(r[9]), 4),
        }
        for r in rows
    ]
