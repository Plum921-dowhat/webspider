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


def test_token_bucket_penalise_recovery_is_bounded():
    # regression: the old acquire() applied negative elapsed, accumulating
    # token debt so recovery took ~3x the penalty even single-threaded
    import time as _time
    b = TokenBucket(rate=50.0, capacity=50.0)
    b.penalise(1.0)
    start = _time.monotonic()
    b.acquire(1)
    took = _time.monotonic() - start
    assert took < 2.5, f"1s penalty took {took:.2f}s to recover (debt bug?)"


def test_token_bucket_no_negative_tokens_under_concurrent_penalise():
    # regression: 8 threads waiting through a penalty must not compound token
    # debt; recovery stays ~penalty duration instead of stalling for hours
    import time as _time
    import threading as _threading

    b = TokenBucket(rate=1.0, capacity=1.0)
    b.penalise(2.0)
    done = []
    threads = [_threading.Thread(target=lambda: (b.acquire(1), done.append(1)))
               for _ in range(8)]
    start = _time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    took = _time.monotonic() - start
    assert len(done) == 8, "threads did not finish"
    assert took < 12.0, f"8-thread recovery after 2s penalty took {took:.1f}s (debt bug?)"


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


class _FakeBucket:
    def __init__(self):
        self.penalties = []

    def acquire(self, tokens=1):
        pass

    def penalise(self, seconds):
        self.penalties.append(seconds)


class _FakeResp:
    def __init__(self, status):
        self.status_code = status
        self.headers = {}

    @property
    def text(self):
        return ""


def _run_fetch_one(monkeypatch, status_factory):
    import fetcher as f

    calls = {"n": 0}
    fb_global, fb_host = _FakeBucket(), _FakeBucket()

    class FakeSession:
        def get(self, url, timeout=None):
            calls["n"] += 1
            return status_factory(calls["n"])

    monkeypatch.setattr(f, "_get_bucket", lambda: fb_global)
    monkeypatch.setattr(f, "_get_host_bucket", lambda url: fb_host)
    monkeypatch.setattr(f, "_get_session", lambda: FakeSession())
    monkeypatch.setattr(f.time, "sleep", lambda s: None)
    result = f.fetch_one("https://dev.to/some-post")
    return result, calls["n"], fb_host


def test_fetch_one_403_penalises_and_retries(monkeypatch):
    # 403 is a block, not a content verdict: every attempt penalises the host
    # bucket with a growing cooldown and retries instead of failing instantly
    result, calls, fb_host = _run_fetch_one(monkeypatch, lambda n: _FakeResp(403))
    assert result is None
    assert calls == 4  # FETCH_RETRIES(3) + 1
    assert len(fb_host.penalties) == 4
    assert fb_host.penalties[0] < fb_host.penalties[-1]  # growing cooldown


def test_fetch_one_404_is_permanent_no_retries(monkeypatch):
    from fetcher import PermanentFetchError

    try:
        _run_fetch_one(monkeypatch, lambda n: _FakeResp(404))
        raised = False
    except PermanentFetchError:
        raised = True
    assert raised


def test_fetch_one_200_returns_text(monkeypatch):
    result, calls, _ = _run_fetch_one(monkeypatch, lambda n: _FakeResp(200))
    assert result == ""
    assert calls == 1
