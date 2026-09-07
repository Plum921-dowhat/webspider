"""embed_chunks pure-function tests (no network)."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from embed_chunks import batch_texts, _parse_retry_after


def test_batch_texts_sizes_and_offsets():
    items = [{"text": f"t{i}"} for i in range(10)]
    batches = list(batch_texts(items, 4))
    assert [b for _, b in batches] == [["t0", "t1", "t2", "t3"],
                                       ["t4", "t5", "t6", "t7"],
                                       ["t8", "t9"]]
    assert [o for o, _ in batches] == [0, 4, 8]


def test_batch_texts_empty():
    assert list(batch_texts([], 4)) == []


def test_parse_retry_after():
    assert _parse_retry_after("30") == 30.0
    assert _parse_retry_after(" 0 ") == 0.0
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("soon") is None
    assert _parse_retry_after("-5") == 0.0
