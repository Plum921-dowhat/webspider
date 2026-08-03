"""Export stored articles to a JSONL corpus file (cursor-based streaming)."""
import argparse
import json
import psycopg2
from config import PG_DSN


def export_jsonl(out_path, limit=0, min_len=200):
    conn = psycopg2.connect(PG_DSN)
    sql = """
        SELECT url, title, content_md, author, published_at, tags, source_type, language
        FROM articles
        WHERE content_md IS NOT NULL AND length(content_md) >= %s
        ORDER BY id
    """
    params = [min_len]
    if limit:
        sql += " LIMIT %s"
        params.append(limit)

    written = 0
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
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                written += 1
    conn.close()
    print(f"exported {written} docs -> {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="corpus.jsonl")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--min-len", type=int, default=200)
    args = ap.parse_args()
    export_jsonl(args.out, args.limit, args.min_len)
