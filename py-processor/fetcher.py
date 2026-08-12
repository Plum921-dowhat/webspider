"""Concurrent article fetching with a global token-bucket rate limiter and
smart 429/Retry-After backoff.

The token bucket is process-global: every worker thread shares one limiter so
the aggregate request rate to DEV.to stays within FETCH_QPS. On HTTP 429 we
drain the bucket and push its refill clock far into the future, throttling the
whole pool until the server says it is OK to try again.
"""
import os
import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from config import (
    FETCH_WORKERS, FETCH_QPS, FETCH_TIMEOUT, FETCH_RETRIES, FETCH_MAX_BACKOFF,
)

DEFAULT_HEADERS = {
    "User-Agent": "en-tech-corpus-bot/1.0 (+https://example.com/bot)",
    "Accept": "application/json",
}

_bucket = None
_bucket_lock = threading.Lock()


def _get_bucket():
    global _bucket
    if _bucket is None:
        with _bucket_lock:
            if _bucket is None:
                _bucket = TokenBucket(FETCH_QPS)
    return _bucket


class TokenBucket:
    """Thread-safe token bucket. acquire() blocks until a token is available."""

    def __init__(self, rate, capacity=None):
        self._rate = float(rate)
        self._capacity = float(capacity if capacity else max(1.0, rate))
        self._tokens = self._capacity
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, tokens=1):
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                # time until enough tokens accrue
                wait = (tokens - self._tokens) / self._rate
            time.sleep(wait)

    def penalise(self, seconds):
        """Throttle the whole pool: drain tokens and push the refill clock
        forward so the next acquire waits. Used after a 429."""
        with self._lock:
            self._tokens = 0.0
            self._last = time.monotonic() + float(seconds)


def _parse_retry_after(value):
    """Parse Retry-After header. Returns seconds (float) or None.

    Accepts both delta-seconds ("30") and HTTP-date ("Tue, 04 Aug 2026 12:00:00 GMT").
    """
    if value is None:
        return None
    value = value.strip()
    try:
        return float(value)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        delta = (dt - datetime.now(timezone.utc)).total_seconds()
        return delta if delta > 0 else 0.0
    except (TypeError, ValueError):
        return None


def _backoff_delay(attempt, retry_after=None):
    """Exponential backoff with full jitter. Retry-After (if present) wins."""
    if retry_after is not None:
        return float(retry_after)
    base = min(FETCH_MAX_BACKOFF, 2.0 ** attempt)
    return random.uniform(0, base)


def fetch_one(url):
    """Fetch a single URL. Returns the response text, or None on failure."""
    bucket = _get_bucket()
    last_err = None
    for attempt in range(FETCH_RETRIES + 1):
        try:
            bucket.acquire()
            resp = requests.get(
                url, headers=DEFAULT_HEADERS, timeout=FETCH_TIMEOUT
            )
        except requests.RequestException as e:
            last_err = e
            time.sleep(_backoff_delay(attempt))
            continue

        if resp.status_code == 200:
            return resp.text
        if resp.status_code == 429:
            ra = _parse_retry_after(resp.headers.get("Retry-After"))
            # throttle the whole pool globally
            _get_bucket().penalise(ra if ra is not None else 5.0)
            time.sleep(_backoff_delay(attempt, retry_after=ra))
            last_err = f"429 (Retry-After={ra})"
            continue
        # non-retryable status
        last_err = f"HTTP {resp.status_code}"
        break
    print(f"[fetcher] failed {url}: {last_err}")
    return None


def fetch_many(urls):
    """Fetch many URLs concurrently. Returns {url: text} for successful ones."""
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = {ex.submit(fetch_one, u): u for u in urls}
        for fut in as_completed(futures):
            url = futures[fut]
            text = fut.result()
            if text is not None:
                results[url] = text
    return results
