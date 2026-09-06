"""Persistent near-duplicate detection via SimHash LSH stored in Redis.

We band the 64-bit fingerprint into SIMHASH_BANDS buckets. A new document is
treated as a duplicate if it shares a bucket with an already-indexed document
whose fingerprint is within SIMHASH_HAMMING bits (Hamming distance). This keeps
dedup O(1) per band and survives restarts because state lives in Redis.

Note: with B bands and Hamming threshold H <= B-1, pigeonhole guarantees at
least one band is flip-free, so recall within SIMHASH_HAMMING is certain. The
probabilistic part is the reverse direction: unrelated docs may share a band
by chance (then filtered by the exact Hamming check), and docs slightly beyond
the threshold are caught only by luck. Fewer bands = fewer candidates, more
chance catches; tune SIMHASH_BANDS accordingly.
"""
import logging

from config import SIMHASH_BITS, SIMHASH_BANDS, SIMHASH_HAMMING, NEARDUP_TTL
from dedupe import simhash, hamming, bucket_keys

logger = logging.getLogger("neardup")

_PREFIX = "simhash:band"


def _band_key(band_idx, bucket_val):
    return f"{_PREFIX}:{band_idx}:{bucket_val}"


def is_duplicate(r, text, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Return (is_dup, chash): whether `text` is a near-duplicate of something
    already indexed, plus its computed fingerprint. The fingerprint is computed
    exactly once so callers can reuse it for indexing without re-hashing."""
    chash = simhash(text, bits=bits)
    for i, bv in enumerate(bucket_keys(chash, bits=bits, bands=bands)):
        members = r.smembers(_band_key(i, bv))
        for raw in members:
            try:
                existing = int(raw)
            except (TypeError, ValueError):
                continue
            if hamming(chash, existing) <= SIMHASH_HAMMING:
                return True, chash
    return False, chash


def index(r, chash, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Register an already-computed fingerprint (int) into the LSH bands."""
    pipe = r.pipeline(transaction=False)
    for i, bv in enumerate(bucket_keys(chash, bits=bits, bands=bands)):
        key = _band_key(i, bv)
        pipe.sadd(key, chash)
        if NEARDUP_TTL > 0:
            pipe.expire(key, NEARDUP_TTL)
    pipe.execute()


_WARM_SENTINEL = "simhash:warmed:v1"


def warm_from_db(r, conn, bits=SIMHASH_BITS, bands=SIMHASH_BANDS):
    """Populate the LSH index from existing rows that already have a hash.

    Batched via pipeline; runs once per index version (sentinel key), so a
    restart skips the redundant full rebuild because Redis already holds the
    fingerprints. If the warm-up is interrupted, the sentinel is not set and
    the next start re-runs it (SADD is idempotent).
    """
    if r.exists(_WARM_SENTINEL):
        logger.info("LSH warm skipped (already warmed)")
        return
    pipe = r.pipeline(transaction=False)
    count = 0
    with conn.cursor() as cur:
        cur.execute(
            "SELECT content_hash FROM articles WHERE content_hash IS NOT NULL"
        )
        for (chash,) in cur:
            if chash is None:
                continue
            for i, bv in enumerate(bucket_keys(int(chash), bits=bits, bands=bands)):
                key = _band_key(i, bv)
                pipe.sadd(key, int(chash))
                if NEARDUP_TTL > 0:
                    pipe.expire(key, NEARDUP_TTL)
            count += 1
            if count % 500 == 0:
                pipe.execute()
    pipe.execute()
    r.set(_WARM_SENTINEL, "1")
    logger.info("warmed %d fingerprints into LSH", count)
