"""Periodic pipeline health sampler.

Every MONITOR_INTERVAL seconds, snapshot the pipeline state from Redis into
PostgreSQL (pipeline_snapshots) so the dashboard can render activity history
and detect stalls independently of crawler logs. Per-source deltas between
consecutive samples are precomputed into by_source JSONB.

Crawler heartbeats (pipeline:runs hash, written by the Go scheduler) are read
live by the dashboard; the sampler only persists the time series. Snapshots
older than RETENTION_DAYS are pruned.
"""
import json
import logging
import os
import time

import redis

from config import REDIS_URL, STREAM, DLQ_STREAM, CONSUMER_GROUP
from store import get_conn, putconn

INTERVAL = int(os.getenv("MONITOR_INTERVAL", "60"))
RETENTION_DAYS = int(os.getenv("MONITOR_RETENTION_DAYS", "7"))

logger = logging.getLogger("monitor")

_INSERT = """
INSERT INTO pipeline_snapshots (produced, stored, rejected, pending, dlq, stream_len, by_source)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

_PRUNE = """
DELETE FROM pipeline_snapshots WHERE taken_at < now() - (%s || ' days')::interval
"""


def read_state(r):
    """Current pipeline counters from Redis. Raises on Redis failure."""
    pend = r.xpending(STREAM, CONSUMER_GROUP)
    return {
        "taken_at": time.time(),
        "produced": int(r.get("metrics:articles:produced") or 0),
        "stored": int(r.get("metrics:articles:stored") or 0),
        "rejected": int(r.get("metrics:articles:rejected") or 0),
        "pending": int(pend.get("pending", 0)) if isinstance(pend, dict) else 0,
        "dlq": r.xlen(DLQ_STREAM),
        "stream_len": r.xlen(STREAM),
        "produced_by_source": {k: int(v) for k, v in r.hgetall("metrics:articles:produced_by_source").items()},
        "stored_by_source": {k: int(v) for k, v in r.hgetall("metrics:articles:stored_by_source").items()},
    }


def compute_by_source(prev, cur):
    """Per-source deltas between two samples. Sources present in either sample
    are included; negative deltas (counter resets) clamp to 0. prev=None (first
    sample) yields all-zero deltas."""
    if prev is None:
        prev = {"produced_by_source": {}, "stored_by_source": {}}
        zero_first = True
    else:
        zero_first = False
    sources = set(prev.get("produced_by_source", {})) | set(cur.get("produced_by_source", {})) \
        | set(prev.get("stored_by_source", {})) | set(cur.get("stored_by_source", {}))
    out = {}
    for src in sources:
        if zero_first:
            out[src] = {"produced_delta": 0, "stored_delta": 0}
            continue
        p_delta = max(0, cur.get("produced_by_source", {}).get(src, 0)
                      - prev.get("produced_by_source", {}).get(src, 0))
        s_delta = max(0, cur.get("stored_by_source", {}).get(src, 0)
                      - prev.get("stored_by_source", {}).get(src, 0))
        out[src] = {"produced_delta": p_delta, "stored_delta": s_delta}
    return out


def snapshot_once(r, prev):
    """Sample once and persist; returns the new state to use as next prev."""
    cur = read_state(r)
    by_source = compute_by_source(prev, cur) if prev else {
        src: {"produced_delta": 0, "stored_delta": 0}
        for src in set(cur.get("produced_by_source", {})) | set(cur.get("stored_by_source", {}))
    }
    conn = get_conn()
    try:
        with conn.cursor() as cur_db:
            cur_db.execute(_INSERT, (
                cur["produced"], cur["stored"], cur["rejected"],
                cur["pending"], cur["dlq"], cur["stream_len"],
                json.dumps(by_source),
            ))
        conn.commit()
    finally:
        putconn(conn)
    return cur


def prune_old(conn=None):
    owned = conn is None
    conn = conn or get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_PRUNE, (RETENTION_DAYS,))
        conn.commit()
    finally:
        if owned:
            putconn(conn)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=5)
    logger.info("monitor started: interval=%ss retention=%sd", INTERVAL, RETENTION_DAYS)
    prev = None
    last_prune = 0.0
    while True:
        try:
            prev = snapshot_once(r, prev)
            if time.time() - last_prune > 6 * 3600:  # prune ~2x/day
                prune_old()
                last_prune = time.time()
        except Exception as e:
            logger.error("sample failed: %s", e)
            prev = None  # skip a delta across a failed sample
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
