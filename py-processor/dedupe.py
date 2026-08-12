"""SimHash-based near-duplicate detection (64-bit default).

Uses blake2b for token hashing so the fingerprint is stable across processes
and runs (the builtin hash() is randomized per-process and cannot be used for
persistent dedup).
"""
import hashlib
import re

_TOKEN_RE = re.compile(r"[a-z0-9_]+")


def _tokens(text):
    return _TOKEN_RE.findall(text.lower())


def _token_hash(token, bits):
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & ((1 << bits) - 1)


def simhash(text, bits=64):
    if not text:
        return 0
    v = [0] * bits
    for token in _tokens(text):
        h = _token_hash(token, bits)
        for i in range(bits):
            if (h >> i) & 1:
                v[i] += 1
            else:
                v[i] -= 1
    out = 0
    for i in range(bits):
        if v[i] > 0:
            out |= 1 << i
    return out


def hamming(a, b):
    return (a ^ b).bit_count()


def bucket_keys(h, bits=64, bands=4):
    """LSH banding: split the fingerprint into `bands` bands and return one
    bucket key per band. Two near-duplicates (small Hamming distance) are very
    likely to collide in at least one band.
    """
    if bits % bands != 0:
        raise ValueError("bits must be divisible by bands")
    width = bits // bands
    shift = bits - width
    keys = []
    for _ in range(bands):
        keys.append(h >> shift)
        h <<= width
        h &= (1 << bits) - 1
    return tuple(keys)
