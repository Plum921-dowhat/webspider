"""Dry-run quality gate over stored articles to calibrate thresholds.

Does NOT modify the database. Prints a rejection histogram so you can tune the
MIN_*/MAX_* constants in config.py before opening the floodgates on a full
backfill.

Usage:
    python quality_probe.py            # scan up to 5000 stored rows
    python quality_probe.py --limit 0  # scan everything
"""
import argparse

from quality import assess
from store import get_conn, putconn


def probe(limit):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if limit and limit > 0:
                cur.execute(
                    "SELECT content_md FROM articles WHERE content_md IS NOT NULL "
                    "ORDER BY id DESC LIMIT %s",
                    (limit,),
                )
            else:
                cur.execute(
                    "SELECT content_md FROM articles WHERE content_md IS NOT NULL"
                )
            rows = cur.fetchall()
    finally:
        putconn(conn)

    reasons = {}
    kept = 0
    total = 0
    for (content,) in rows:
        total += 1
        keep, reason, meta = assess(content)
        if keep:
            kept += 1
        else:
            reasons[reason] = reasons.get(reason, 0) + 1

    print(f"scanned={total} kept={kept} rejected={total - kept}")
    print("rejection histogram:")
    for reason, cnt in sorted(reasons.items(), key=lambda kv: -kv[1]):
        print(f"  {reason:14s} {cnt}")
    return reasons


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=5000)
    args = p.parse_args()
    probe(args.limit)
