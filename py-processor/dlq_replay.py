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
import time

import redis

from config import REDIS_URL, STREAM, DLQ_STREAM


def replay_round(r, limit, delete, dry_run):
    """Replay up to `limit` oldest DLQ entries. Returns (replayed, skipped)."""
    entries = r.xrange(DLQ_STREAM, min="-", max="+", count=limit or None)
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

        if dry_run:
            print(f"  [dry-run] {msg_id} reason={reason} url={url}")
            continue

        r.xadd(STREAM, {"article": article})
        replayed += 1
        print(f"  replayed {msg_id} reason={reason} url={url}")
        if delete:
            r.xdel(DLQ_STREAM, msg_id)
    return replayed, skipped


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="max entries to replay (0 = all)")
    ap.add_argument("--dry-run", action="store_true", help="show what would be replayed")
    ap.add_argument("--delete", action="store_true", help="XDEL each entry after replaying it")
    ap.add_argument("--loop", action="store_true",
                    help="keep replaying --batch entries every --interval-sec until the DLQ is empty")
    ap.add_argument("--batch", type=int, default=300, help="entries per round in --loop mode")
    ap.add_argument("--interval-sec", type=int, default=900, help="pause between --loop rounds")
    ap.add_argument("--max-rounds", type=int, default=40, help="hard cap on --loop rounds")
    args = ap.parse_args()

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    print(f"dlq stream={DLQ_STREAM} length={r.xlen(DLQ_STREAM)}")

    if args.loop:
        for round_no in range(1, args.max_rounds + 1):
            replayed, skipped = replay_round(r, args.batch, args.delete, args.dry_run)
            remaining = r.xlen(DLQ_STREAM)
            print(f"[round {round_no}/{args.max_rounds}] replayed={replayed} skipped={skipped} dlq_remaining={remaining}")
            if replayed == 0 or args.dry_run:
                break
            if remaining == 0:
                break
            time.sleep(args.interval_sec)
        print(f"loop done: dlq_remaining={r.xlen(DLQ_STREAM)}")
        return

    replayed, skipped = replay_round(r, args.limit, args.delete, args.dry_run)
    if args.dry_run:
        print(f"dry run complete: {replayed + skipped} entries would be replayed")
        return
    print(f"done: replayed={replayed} skipped={skipped} deleted={'yes' if args.delete else 'no'}")


if __name__ == "__main__":
    main()
