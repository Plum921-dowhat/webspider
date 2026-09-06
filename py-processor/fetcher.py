"""Concurrent article fetching with a global token-bucket rate limiter and
smart 429/Retry-After backoff.

The token bucket is process-global: every worker thread shares one limiter so
the aggregate request rate to DEV.to stays within FETCH_QPS. On HTTP 429 we
drain the bucket and push its refill clock far into the future, throttling the
whole pool until the server says it is OK to try again.
"""
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import requests

from config import (
    FETCH_WORKERS, FETCH_QPS, FETCH_TIMEOUT, FETCH_RETRIES, FETCH_MAX_BACKOFF,
    FETCH_HOST_QPS, FETCH_HOST_OVERRIDES,
)

logger = logging.getLogger("fetcher")


class PermanentFetchError(Exception):
    """Raised for definitively-gone content (404/410). Retrying — and parking
    on the DLQ — cannot ever recover it, so callers skip it instead."""

DEFAULT_HEADERS = {
    "User-Agent": "en-tech-corpus-bot/1.0 (+https://example.com/bot)",
    # Accept: text/html, NOT application/json — with the JSON accept header
    # dev.to serves raw article JSON (body_html: null posts) for some URLs,
    # which trafilatura cannot parse (mass extract_fail).
    "Accept": "text/html",
}

_bucket = None
_bucket_lock = threading.Lock()
_local = threading.local()


def _get_session():
    """Per-thread requests.Session: reuses TCP/TLS connections across fetches
    within the same worker thread instead of reopening them per request."""
    sess = getattr(_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(DEFAULT_HEADERS)
        _local.session = sess
    return sess


def _get_bucket():
    global _bucket
    if _bucket is None:
        with _bucket_lock:
            if _bucket is None:
                _bucket = TokenBucket(FETCH_QPS)
    return _bucket


def parse_host_overrides(spec):
    """Parse "dev.to:3,stackoverflow.com:2" into {host: qps}. Entries with a
    bad rate are skipped; separators tolerate whitespace."""
    overrides = {}
    for part in (spec or "").split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        host, _, rate = part.rpartition(":")
        try:
            overrides[host.strip().lower()] = float(rate)
        except ValueError:
            continue
    return overrides


_HOST_OVERRIDES = parse_host_overrides(FETCH_HOST_OVERRIDES)
_host_buckets = {}


def _get_host_bucket(url):
    """Per-host token bucket: the global bucket bounds aggregate QPS, this one
    bounds QPS against a single domain (politeness for arbitrary hosts)."""
    host = (urlsplit(url).hostname or "").lower()
    if not host:
        return None
    bucket = _host_buckets.get(host)
    if bucket is None:
        rate = _HOST_OVERRIDES.get(host, FETCH_HOST_QPS)
        bucket = TokenBucket(rate)
        _host_buckets[host] = bucket
    return bucket


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
    host_bucket = _get_host_bucket(url)
    last_err = None
    for attempt in range(FETCH_RETRIES + 1):
        try:
            bucket.acquire()
            if host_bucket is not None:
                host_bucket.acquire()
            resp = _get_session().get(url, timeout=FETCH_TIMEOUT)
        except requests.RequestException as e:
            last_err = e
            time.sleep(_backoff_delay(attempt))
            continue

        if resp.status_code == 200:
            return resp.text
        if resp.status_code in (404, 410):
            # gone for good: raise immediately, no retries, no DLQ
            raise PermanentFetchError(f"HTTP {resp.status_code}")
        if resp.status_code == 429:
            ra = _parse_retry_after(resp.headers.get("Retry-After"))
            # throttle the whole pool globally
            _get_bucket().penalise(ra if ra is not None else 5.0)
            time.sleep(_backoff_delay(attempt, retry_after=ra))
            last_err = f"429 (Retry-After={ra})"
            continue
        if resp.status_code >= 500:
            # transient server errors: retry with backoff (mirrors Go side)
            last_err = f"HTTP {resp.status_code}"
            time.sleep(_backoff_delay(attempt))
            continue
        # non-retryable status
        last_err = f"HTTP {resp.status_code}"
        break
    logger.warning("fetch failed %s: %s", url, last_err)
    return None


def fetch_many(urls):
    """Fetch many URLs concurrently.

    Returns (results, permanent): {url: text} for successful fetches and
    {url: reason} for definitively-gone ones (404/410). Transient failures
    appear in neither map.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    results = {}
    permanent = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futures = {ex.submit(fetch_one, u): u for u in urls}
        for fut in as_completed(futures):
            url = futures[fut]
            try:
                text = fut.result()
            except PermanentFetchError as e:
                permanent[url] = str(e)
                continue
            if text is not None:
                results[url] = text
    return results, permanent
