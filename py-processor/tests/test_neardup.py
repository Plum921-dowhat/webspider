"""LSH near-dup logic against a minimal in-memory Redis stub (no server needed)."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from neardup import is_duplicate, index, _band_key


class FakePipeline:
    def __init__(self, store):
        self._store = store._sets
        self._ops = []

    def sadd(self, key, member):
        self._ops.append(("sadd", key, member))

    def expire(self, key, ttl):
        self._ops.append(("expire", key, ttl))

    def execute(self):
        for op in self._ops:
            if op[0] == "sadd":
                self._store.setdefault(op[1], set()).add(op[2])
        self._ops = []
        return True


class FakeRedis:
    """Just enough of redis-py for neardup.is_duplicate / neardup.index."""

    def __init__(self):
        self._sets = {}

    def smembers(self, key):
        return set(self._sets.get(key, ()))

    def pipeline(self, transaction=False):
        return FakePipeline(self)


_TEXT = (
    "The quick brown fox jumps over the lazy dog while distributed systems "
    "handle consensus among replicas using snapshot isolation and durable "
    "write ahead logs that survive crash recovery scenarios in production "
    "clusters across multiple availability zones and regions "
) * 3  # ~110 tokens: a 2-word edit stays within SIMHASH_HAMMING (3)

_VARIANT = _TEXT.replace("quick brown", "swift brown").replace("lazy", "sleepy")


def test_new_text_is_not_duplicate():
    r = FakeRedis()
    dup, chash = is_duplicate(r, _TEXT)
    assert not dup
    assert chash != 0


def test_indexed_text_is_detected_as_duplicate():
    r = FakeRedis()
    _, chash = is_duplicate(r, _TEXT)
    index(r, chash)
    dup, _ = is_duplicate(r, _TEXT)
    assert dup


def test_near_duplicate_detected_after_indexing():
    r = FakeRedis()
    _, chash = is_duplicate(r, _TEXT)
    index(r, chash)
    dup, _ = is_duplicate(r, _VARIANT)
    assert dup


def test_unrelated_text_not_flagged():
    r = FakeRedis()
    _, chash = is_duplicate(r, _TEXT)
    index(r, chash)
    other = ("Completely different content about cooking pasta carbonara "
             "with eggs pecorino cheese guanciale and black pepper tonight")
    dup, _ = is_duplicate(r, other)
    assert not dup


def test_index_writes_four_band_keys():
    r = FakeRedis()
    _, chash = is_duplicate(r, _TEXT)
    index(r, chash)
    assert len(r._sets) == 4
    for key in r._sets:
        assert key.startswith(_band_key("", "")[: len("simhash:band")])
