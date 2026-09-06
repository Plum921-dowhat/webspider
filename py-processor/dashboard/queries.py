"""Read-only SQL + Redis queries for the dashboard."""
import json
import os
from datetime import datetime, timezone

import redis

from config import REDIS_URL, STREAM, DLQ_STREAM, CONSUMER_GROUP
from store import get_conn, putconn


def _conn():
    """Pooled connection as a context manager; putconn rolls back any open
    transaction and returns the connection for reuse."""
    return _ConnCtx()


class _ConnCtx:
    def __enter__(self):
        self.conn = get_conn()
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        putconn(self.conn)
        return False


def summary():
    """Total articles, distinct languages, new in last 24h."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                count(*) AS total,
                count(DISTINCT language) AS langs,
                count(*) FILTER (WHERE crawled_at >= now() - interval '24 hours') AS last24
            FROM articles
            """
        )
        total, langs, last24 = cur.fetchone()
    return {"total": total, "languages": langs, "last_24h": last24}


def lang_distribution():
    """Article count grouped by language."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT language, count(*) AS cnt
            FROM articles
            GROUP BY language
            ORDER BY cnt DESC
            """
        )
        rows = cur.fetchall()
    return [{"language": r[0], "count": r[1]} for r in rows]


def source_distribution():
    """Article count grouped by source_type (devto / hashnode / stackexchange...)."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT source_type, count(*) AS cnt
            FROM articles
            GROUP BY source_type
            ORDER BY cnt DESC
            """
        )
        rows = cur.fetchall()
    return [{"source": r[0], "count": r[1]} for r in rows]


def daily(limit=30):
    """Articles per day for the last `limit` days."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT date_trunc('day', crawled_at)::date AS day, count(*) AS cnt
            FROM articles
            WHERE crawled_at >= now() - (%s || ' days')::interval
            GROUP BY day
            ORDER BY day
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [{"day": str(r[0]), "count": r[1]} for r in rows]


def article_by_id(aid):
    """Full article record for the detail view. Returns None if missing."""
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, url, title, content_md, author, published_at, tags,
                   source_type, language, quality_score, crawled_at
            FROM articles
            WHERE id = %s
            """,
            (aid,),
        )
        r = cur.fetchone()
    if r is None:
        return None
    return {
        "id": r[0],
        "url": r[1],
        "title": r[2],
        "content_md": r[3],
        "author": r[4],
        "published_at": r[5].isoformat() if r[5] else None,
        "tags": r[6] or [],
        "source_type": r[7],
        "language": r[8],
        "quality_score": float(r[9]) if r[9] is not None else None,
        "crawled_at": r[10].isoformat() if r[10] else None,
    }


def articles(lang=None, q=None, source=None, page=1, page_size=20):
    """Filtered + searched + paginated article list with content preview.

    lang:   optional language filter.
    q:      case-insensitive search on url OR content_md.
    source: optional source_type filter (devto/hashnode/...).
    """
    page = max(1, int(page))
    page_size = max(1, min(100, int(page_size)))
    offset = (page - 1) * page_size

    where = []
    params = []
    if lang:
        where.append("language = %s")
        params.append(lang)
    if source:
        where.append("source_type = %s")
        params.append(source)
    if q:
        where.append("(url ILIKE %s OR content_md ILIKE %s)")
        params.append(f"%{q}%")
        params.append(f"%{q}%")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT count(*) FROM articles {where_sql}
            """,
            params,
        )
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT id, url, title, language, source_type,
                   length(content_md) AS md_len,
                   left(content_md, 300) AS preview,
                   crawled_at
            FROM articles
            {where_sql}
            ORDER BY id DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = cur.fetchall()

    items = [
        {
            "id": r[0],
            "url": r[1],
            "title": r[2],
            "language": r[3],
            "source_type": r[4],
            "md_len": r[5],
            "preview": r[6],
            "created_at": r[7].isoformat() if r[7] else None,
        }
        for r in rows
    ]
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }


def pipeline():
    """Crawler pipeline health counters, read from Redis (metrics:* keys,
    stream lengths, consumer-group pending). All values are informational.
    """
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2)
    out = {
        "produced": int(r.get("metrics:articles:produced") or 0),
        "stored": int(r.get("metrics:articles:stored") or 0),
        "rejected": int(r.get("metrics:articles:rejected") or 0),
        "rejected_by_reason": {
            k: int(v)
            for k, v in r.hgetall("metrics:articles:rejected_by_reason").items()
        },
        "by_source": {
            k: int(v) for k, v in r.hgetall("metrics:articles:stored_by_source").items()
        },
        "rejected_by_source": {
            k: int(v) for k, v in r.hgetall("metrics:articles:rejected_by_source").items()
        },
        "produced_by_source": {
            k: int(v) for k, v in r.hgetall("metrics:articles:produced_by_source").items()
        },
        "dlq": r.xlen(DLQ_STREAM),
        "stream_length": r.xlen(STREAM),
        "pending": 0,
    }
    try:
        pend = r.xpending(STREAM, CONSUMER_GROUP)
        if isinstance(pend, dict):
            out["pending"] = int(pend.get("pending", 0))
    except redis.ResponseError:
        pass  # consumer group not created yet on a fresh install
    return out


# Per-source staleness thresholds in minutes (no heartbeat for this long =>
# source flagged). Defaults cover the shipped crawl cadences; override via env.
def _stale_thresholds():
    spec = os.getenv("STALE_THRESHOLDS", "devto:120,stackexchange:300,rss:180")
    out = {}
    for part in spec.split(","):
        part = part.strip()
        if ":" in part:
            name, _, mins = part.rpartition(":")
            try:
                out[name] = int(mins)
            except ValueError:
                continue
    return out


def health(hours=24):
    """Crawler activity monitoring: per-source last-run heartbeats (written by
    the Go scheduler into pipeline:runs) plus the monitor sampler's snapshot
    history for the activity chart.
    """
    thresholds = _stale_thresholds()
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=2)

    runs = []
    now = datetime.now(timezone.utc)
    for src, raw in r.hgetall("pipeline:runs").items():
        try:
            rec = json.loads(raw)
        except json.JSONDecodeError:
            continue
        finished = rec.get("finished_at")
        age_min = None
        if finished:
            try:
                age_min = (now - datetime.fromisoformat(finished)).total_seconds() / 60
            except ValueError:
                pass
        limit = thresholds.get(src, 180)
        status = "unknown" if age_min is None else ("ok" if age_min <= limit else "stale")
        runs.append({
            "source": src,
            "last_run_at": finished,
            "age_minutes": round(age_min, 1) if age_min is not None else None,
            "stale_after_minutes": limit,
            "status": status,
            "pages": rec.get("pages"),
            "published": rec.get("published"),
            "failed_pages": rec.get("failed_pages"),
            "publish_errors": rec.get("publish_errors"),
            "duration_ms": rec.get("duration_ms"),
        })
    runs.sort(key=lambda x: x["source"])

    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT taken_at, produced, stored, pending, dlq, by_source
            FROM pipeline_snapshots
            WHERE taken_at >= now() - (%s || ' hours')::interval
            ORDER BY taken_at
            """,
            (max(1, min(hours, 168)),),
        )
        rows = cur.fetchall()

    series = [
        {
            "taken_at": r[0].isoformat(),
            "produced_delta": max(0, r[1] - prev_produced),
            "stored_delta": max(0, r[2] - prev_stored),
            "pending": r[3],
            "dlq": r[4],
            "by_source": r[5] if isinstance(r[5], dict) else {},
        }
        for r, prev_produced, prev_stored in _with_prev(rows)
    ]
    return {
        "now": now.isoformat(),
        "runs": runs,
        "series": series,
    }


def _with_prev(rows):
    prev_p = prev_s = None
    for r in rows:
        yield r, (prev_p if prev_p is not None else r[1]), (prev_s if prev_s is not None else r[2])
        prev_p, prev_s = r[1], r[2]
