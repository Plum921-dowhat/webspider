"""Export stored articles to a JSONL corpus file (cursor-based streaming)."""
import argparse
import json

from config import PG_DSN
from store import get_conn, putconn


def export_jsonl(out_path, limit=0, min_len=200, min_score=0.0, lang=None):
    conn = get_conn()
    sql = """
        SELECT url, title, content_md, author, published_at, tags, source_type, language, quality_score
        FROM articles
        WHERE content_md IS NOT NULL AND length(content_md) >= %s
    """
    params = [min_len]
    if min_score > 0:
        sql += " AND quality_score >= %s"
        params.append(min_score)
    if lang:
        sql += " AND language = %s"
        params.append(lang)
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    written = 0
    try:
        with conn.cursor(name="export_cur") as cur:
            cur.itersize = 1000
            cur.execute(sql, params)
            with open(out_path, "w", encoding="utf-8") as f:
                for row in cur:
                    rec = {
                        "url": row[0],
                        "title": row[1],
                        "content_md": row[2],
                        "author": row[3],
                        "published_at": row[4].isoformat() if row[4] else None,
                        "tags": row[5],
                        "source_type": row[6],
                        "language": row[7],
                        "quality_score": float(row[8]) if row[8] is not None else None,
                    }
                    f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    written += 1
    finally:
        putconn(conn)
    print(f"exported {written} docs -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="corpus.jsonl")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-len", type=int, default=200)
    ap.add_argument("--min-score", type=float, default=0.0,
                    help="only export rows with quality_score >= this (0 = no filter)")
    ap.add_argument("--lang", default=None,
                    help="only export rows with this language (e.g. en)")
    args = ap.parse_args()
    export_jsonl(args.out, args.limit, args.min_len, args.min_score, args.lang)
