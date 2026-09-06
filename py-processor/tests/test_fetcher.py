"""Fetcher pure-function tests (no network, no sleeps)."""
import sys
import os
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetcher import (
    _parse_retry_after, _backoff_delay, TokenBucket, FETCH_MAX_BACKOFF,
    PermanentFetchError, fetch_many, parse_host_overrides, _get_host_bucket,
)


def test_parse_retry_after_delta_seconds():
    assert _parse_retry_after("30") == 30.0
    assert _parse_retry_after(" 0 ") == 0.0


def test_parse_retry_after_http_date():
    future = format_datetime(datetime.now(timezone.utc) + timedelta(hours=1))
    got = _parse_retry_after(future)
    assert got is not None and 0 < got <= 3600

    past = format_datetime(datetime.now(timezone.utc) - timedelta(hours=1))
    assert _parse_retry_after(past) == 0.0


def test_parse_retry_after_garbage():
    assert _parse_retry_after(None) is None
    assert _parse_retry_after("soon") is None


def test_backoff_delay_honours_retry_after():
    assert _backoff_delay(0, retry_after=7.5) == 7.5


def test_backoff_delay_full_jitter_bounds():
    for attempt in range(0, 6):
        for _ in range(20):
            d = _backoff_delay(attempt)
            assert 0 <= d <= min(FETCH_MAX_BACKOFF, 2.0 ** attempt)


def test_token_bucket_acquire_immediate_when_tokens_available():
    b = TokenBucket(rate=1000.0, capacity=10.0)
    b.acquire(5)  # must not block meaningfully
    b.acquire(5)
    assert b._tokens < 5  # drained


def test_token_bucket_penalise_drains_and_defers():
    b = TokenBucket(rate=10.0, capacity=10.0)
    b.penalise(120.0)
    assert b._tokens == 0.0
    assert b._last > 0  # refill clock pushed into the future via monotonic offset


def test_fetch_many_splits_permanent_from_success(monkeypatch):
    import fetcher as f

    def fake_fetch_one(url):
        if url == "gone":
            raise PermanentFetchError("HTTP 404")
        if url == "flaky":
            return None  # transient failure
        return "html"

    monkeypatch.setattr(f, "fetch_one", fake_fetch_one)
    results, permanent = f.fetch_many(["gone", "flaky", "ok"])
    assert results == {"ok": "html"}
    assert permanent == {"gone": "HTTP 404"}


def test_parse_host_overrides():
    assert parse_host_overrides("dev.to:3, stackoverflow.com:2") == {
        "dev.to": 3.0, "stackoverflow.com": 2.0,
    }
    assert parse_host_overrides("") == {}
    assert parse_host_overrides("bad, x:nota, y:2.5") == {"y": 2.5}


def test_get_host_bucket_per_host_and_override(monkeypatch):
    import fetcher as f

    monkeypatch.setattr(f, "_HOST_OVERRIDES", {"fast.host": 5.0})
    monkeypatch.setattr(f, "_host_buckets", {})
    b1 = f._get_host_bucket("https://a.example.com/x")
    b2 = f._get_host_bucket("https://a.example.com/y")
    b3 = f._get_host_bucket("https://fast.host/z")
    assert b1 is b2  # same host shares one bucket
    assert b1 is not b3
    assert b1._rate == f.FETCH_HOST_QPS  # default per-host rate
    assert b3._rate == 5.0  # override wins
