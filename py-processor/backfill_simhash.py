"""Backfill content_hash for existing rows using the stable (blake2b) SimHash.

Run once after deploying the new dedupe.py so historical articles gain a
reproducible fingerprint (the old hash() based values are meaningless across
processes and must be recomputed).
"""
from config import SIMHASH_BITS
from dedupe import simhash
from store import get_conn


def backfill(batch=1000):
    conn = get_conn()
    processed = 0
    with conn.cursor(name="backfill_cursor") as cur:
        cur.itersize = batch
        cur.execute("SELECT id, content_md FROM articles WHERE content_hash IS NULL")
        with conn.cursor() as upd:
            for row in cur:
                aid, content = row
                if not content:
                    continue
                chash = simhash(content, bits=SIMHASH_BITS)
                upd.execute(
                    "UPDATE articles SET content_hash = %s WHERE id = %s",
                    (chash, aid),
                )
                processed += 1
        conn.commit()
    conn.close()
    print(f"[backfill] recomputed content_hash for {processed} rows")


if __name__ == "__main__":
    backfill()
