"""Read-only SQL queries for the dashboard. Safe: only SELECT."""
from config import PG_DSN
import psycopg2


def _conn():
    return psycopg2.connect(PG_DSN)


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


def articles(lang=None, q=None, page=1, page_size=20):
    """Filtered + searched + paginated article list with content preview.

    lang: optional language filter.
    q:    case-insensitive search on url OR content_md.
    """
    page = max(1, int(page))
    page_size = max(1, min(100, int(page_size)))
    offset = (page - 1) * page_size

    where = []
    params = []
    if lang:
        where.append("language = %s")
        params.append(lang)
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
            SELECT id, url, title, language,
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
            "md_len": r[4],
            "preview": r[5],
            "created_at": r[6].isoformat() if r[6] else None,
        }
        for r in rows
    ]
    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "items": items,
    }
