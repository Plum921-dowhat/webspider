"""SimHash fingerprint + LSH banding unit tests."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from dedupe import simhash, hamming, bucket_keys


def _long_text(seed):
    # ~110 tokens: long enough that a 2-word edit moves only a couple of
    # simhash bits (short docs are far more sensitive — see README notes).
    base = (
        "The quick brown fox jumps over the lazy dog while distributed systems "
        "handle consensus among replicas using snapshot isolation and durable "
        "write ahead logs that survive crash recovery scenarios in production "
        "clusters across multiple availability zones and regions "
    )
    return base * 3 + f"seed {seed}"


def test_simhash_stable_across_calls():
    t = _long_text(1)
    assert simhash(t) == simhash(t)


def test_simhash_near_duplicate_small_hamming():
    # changing 2 words in a ~110-token text moves <= SIMHASH_HAMMING (3) bits
    a = simhash(_long_text(1))
    b = simhash(_long_text(1).replace("quick brown", "swift brown").replace("lazy", "sleepy"))
    assert hamming(a, b) <= 3


def test_simhash_unrelated_texts_far_apart():
    a = simhash(_long_text(1))
    b = simhash("Completely different content about cooking pasta carbonara "
                "with eggs pecorino cheese guanciale and black pepper tonight "
                "and baking sourdough bread from scratch in a dutch oven")
    assert hamming(a, b) > 10


def test_simhash_empty_is_zero():
    assert simhash("") == 0


def test_hamming_bits():
    assert hamming(0, 0) == 0
    assert hamming(0, 1) == 1
    assert hamming(0b1010, 0b0101) == 4


def test_bucket_keys_split_and_reassemble():
    h = 0x0123_4567_89AB_CDEF
    keys = bucket_keys(h, bits=64, bands=4)
    assert len(keys) == 4
    # bands are 16-bit slices from the top; reassembly must reproduce h
    reassembled = 0
    for k in keys:
        reassembled = (reassembled << 16) | k
    assert reassembled == h


def test_bucket_keys_guaranteed_collision_within_threshold():
    # Pigeonhole: with 4 bands, flipping <= 3 bits can dirty at most 3 bands,
    # so at least one band value is identical -> LSH bucket must collide.
    h1 = 0xF0E1_D2C3_B4A5_9687
    h2 = h1 ^ (1 << 3) ^ (1 << 20) ^ (1 << 40)  # one bit per band 3,2,1; band 0 clean
    k1, k2 = bucket_keys(h1), bucket_keys(h2)
    assert hamming(h1, h2) == 3
    assert any(x == y for x, y in zip(k1, k2))


def test_bucket_keys_requires_divisible_bits():
    with pytest.raises(ValueError):
        bucket_keys(42, bits=64, bands=5)
