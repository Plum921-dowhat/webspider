"""SimHash-based near-duplicate detection (64-bit default)."""


def simhash(text, bits=64):
    if not text:
        return 0
    v = [0] * bits
    for token in _tokens(text):
        h = hash(token) & ((1 << bits) - 1)
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
    return bin(a ^ b).count("1")


def _tokens(text):
    # simple whitespace + lowercasing; good enough for corpus-level dedup
    return text.lower().split()
