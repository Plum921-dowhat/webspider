"""Monitor sampler pure-function tests (no Redis/PG needed)."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from monitor import compute_by_source


def test_compute_by_source_deltas():
    prev = {"produced_by_source": {"devto": 100, "rss": 5}, "stored_by_source": {"devto": 90},
            "rejected_by_source": {"devto": 10}}
    cur = {"produced_by_source": {"devto": 152, "rss": 5, "stackexchange": 1},
           "stored_by_source": {"devto": 125, "rss": 2},
           "rejected_by_source": {"devto": 14, "stackexchange": 3}}
    out = compute_by_source(prev, cur)
    assert out["devto"] == {"produced_delta": 52, "stored_delta": 35, "rejected_delta": 4}
    assert out["rss"] == {"produced_delta": 0, "stored_delta": 2, "rejected_delta": 0}
    assert out["stackexchange"] == {"produced_delta": 1, "stored_delta": 0, "rejected_delta": 3}


def test_compute_by_source_first_sample_all_zeroes():
    cur = {"produced_by_source": {"devto": 10}, "stored_by_source": {}}
    out = compute_by_source(None, cur)
    assert out["devto"] == {"produced_delta": 0, "stored_delta": 0, "rejected_delta": 0}


def test_compute_by_source_clamps_negative_deltas():
    prev = {"produced_by_source": {"devto": 100}, "stored_by_source": {}}
    cur = {"produced_by_source": {"devto": 3}, "stored_by_source": {}}  # counter reset
    out = compute_by_source(prev, cur)
    assert out["devto"]["produced_delta"] == 0
