"""Replay messages parked on the dead-letter stream back into the main stream.

Typical use: DEV.to had an outage (extract_fail storm), the incident is over,
and you want the parked articles re-run through the normal processor pipeline
instead of hand-editing the DLQ.

Notes:
  - Replay bypasses the crawler-side URL dedup SET on purpose: a message can
    only end up on the DLQ *after* its URL was already recorded in `seen:`,
    so re-checking membership here would silently drop every replay. The
    processor's own defenses (quality gate, SimHash LSH, PG ON CONFLICT)
    make a replay idempotent anyway.
  - --delete XDELs each replayed entry so the DLQ tracks what is left to do.

Usage:
    python dlq_replay.py --dry-run          # list what would be replayed
    python dlq_replay.py                    # replay everything, keep entries
    python dlq_replay.py --limit 50 --delete
"""
import argparse
import json

import redis

from config import REDIS_URL, STREAM, DLQ_STREAM


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max entries to replay (0 = all)")
    ap.add_argument("--dry-run", action="store_true", help="show what would be replayed")
    ap.add_argument("--delete", action="store_true", help="XDEL each entry after replaying it")
    args = ap.parse_args()

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    dlq_len = r.xlen(DLQ_STREAM)
    entries = r.xrange(DLQ_STREAM, min="-", max="+", count=args.limit or None)
    print(f"dlq stream={DLQ_STREAM} length={dlq_len} replaying={len(entries)}")

    replayed = skipped = 0
    for msg_id, fields in entries:
        article = fields.get("article")
        if not article:
            skipped += 1
            continue
        try:
            info = json.loads(article)
        except json.JSONDecodeError:
            info = {}
        url = fields.get("url") or info.get("url", "?")
        reason = fields.get("reason", "?")

        if args.dry_run:
            print(f"  [dry-run] {msg_id} reason={reason} url={url}")
            continue

        r.xadd(STREAM, {"article": article})
        replayed += 1
        print(f"  replayed {msg_id} reason={reason} url={url}")
        if args.delete:
            r.xdel(DLQ_STREAM, msg_id)

    if args.dry_run:
        print(f"dry run complete: {len(entries)} entries would be replayed")
        return
    print(f"done: replayed={replayed} skipped={skipped} deleted={'yes' if args.delete else 'no'}")


if __name__ == "__main__":
    main()
