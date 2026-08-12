"""Persistent near-duplicate detection via SimHash LSH stored in Redis.

We band the 64-bit fingerprint into SIMHASH_BANDS buckets. A new document is
treated as a duplicate if it shares a bucket with an already-indexed document
whose fingerprint is within SIMHASH_HAMMING bits (Hamming distance). This keeps
dedup O(1) per band and survives restarts because state lives in Redis.
"""
from config import SIMHASH_BITS, SIMHASH_BANDS, SIMHASH_HAMMING, NEARDUP_TTL
from dedupe import simhash, hamming, bucket_keys

_PREFIX = "simhash:band"


def _band_key(band_idx, bucket_val):
    return f"{_PREFIX}:{band_idx}:{bucket_val}"


def is_duplicate(r, text, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Return True if `text` is a near-duplicate of something already indexed."""
    chash = simhash(text, bits=bits)
    for i, bv in enumerate(bucket_keys(chash, bits=bits, bands=bands)):
        members = r.smembers(_band_key(i, bv))
        for raw in members:
            try:
                existing = int(raw)
            except (TypeError, ValueError):
                continue
            if hamming(chash, existing) <= SIMHASH_HAMMING:
                return True
    return False


def index(r, text, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Register `text`'s fingerprint into the LSH bands."""
    chash = simhash(text, bits=bits)
    for i, bv in enumerate(bucket_keys(chash, bits=bits, bands=bands)):
        key = _band_key(i, bv)
        r.sadd(key, chash)
        if NEARDUP_TTL > 0:
            r.expire(key, NEARDUP_TTL)


def warm_from_db(r, conn, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Populate the LSH index from existing rows that already have a hash."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM articles WHERE content_hash IS NOT NULL"
        )
        count = 0
        for (chash,) in cur:
            if chash is None:
                continue
            for i, bv in enumerate(bucket_keys(int(chash), bits=bits, bands=bands)):
                r.sadd(_band_key(i, bv), int(chash))
            count += 1
    print(f"[neardup] warmed {count} fingerprints into LSH")
