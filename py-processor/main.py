"""Consume DEV.to raw articles from Redis Stream.

Pipeline per message:
  fetch HTML (concurrent, token-bucket limited)
  -> extract Markdown with trafilatura (code fences preserved)
  -> quality gate (wide-in / strict-out)
  -> language filter (on code-stripped text)
  -> persistent near-dup dedup (SimHash LSH in Redis)
  -> store to PostgreSQL

Failure policy (no data loss):
  - fetch/extract failures are XACKed and parked on a dead-letter stream
    (articles_dlq) so they never block the pipeline and stay recoverable.
  - DB insert failures are NOT acked: messages stay in PENDING and are retried
    after a reconnect, so a DB blip never loses data.
  - The SimHash index is written only AFTER a successful insert, so a retry can
    never be rejected by its own fingerprint.

Metrics (written to Redis, read by the dashboard's /api/pipeline):
  metrics:articles:produced            (Go crawler, on publish)
  metrics:articles:stored              (here, real inserted rows)
  metrics:articles:rejected            (here, total gate rejections per batch)
  metrics:articles:rejected_by_reason  (hash: reason -> count)
  metrics:articles:dlq                 (here, messages parked on the DLQ)
"""
import hashlib
import json
import logging
import time
from collections import deque

import redis
from langdetect import detect, LangDetectException
import trafilatura

from config import (
    REDIS_URL, STREAM, DLQ_STREAM, DLQ_SEEN_KEY, DLQ_MAXLEN,
    CONSUMER_GROUP, CONSUMER_NAME,
    BATCH_SIZE, BLOCK_MS, LANG_KEEP, SIMHASH_BITS,
)
from fetcher import fetch_many
from quality import assess, strip_code
from neardup import is_duplicate, index, warm_from_db
from store import get_conn, putconn, insert_articles

logger = logging.getLogger("processor")


def extract_content(html):
    if not html:
        return None
    # output_format="markdown" preserves ``` fenced code blocks, which the
    # quality gate relies on to detect high-value code corpus.
    return trafilatura.extract(
        html,
        include_comments=False,
        output_format="markdown",
        include_tables=True,
    )


_DLQ_PUSH_LUA = """
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  return 0
end
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[7], '*',
           'stream_msg_id', ARGV[1],
           'url', ARGV[2],
           'title', ARGV[3],
           'reason', ARGV[4],
           'detail', ARGV[5],
           'article', ARGV[6])
return 1
"""


def push_dlq(r, msg_id, article, reason, detail=""):
    """Park a poisoned message on the dead-letter stream (never raises).

    Idempotent per stream_msg_id (Lua: SISMEMBER+SADD+XADD): when a DB blip
    makes the batch get retried, a message that already failed fetch/extract
    is parked on the DLQ exactly once instead of accumulating duplicates.
    Returns True when the message was newly parked.
    """
    try:
        first = r.eval(
            _DLQ_PUSH_LUA,
            2,
            DLQ_SEEN_KEY,
            DLQ_STREAM,
            msg_id,
            article.get("url", ""),
            article.get("title", ""),
            reason,
            str(detail)[:500],
            json.dumps(article, ensure_ascii=False, default=str)[:10000],
            DLQ_MAXLEN,
        )
        if first == 1:
            r.incr("metrics:articles:dlq")
        return first == 1
    except Exception as e:
        logger.error("dlq push failed: %s", e)
        return False


def process_batch(r, conn, resp, seen_hashes):
    """Process one batch: parse -> fetch (concurrent) -> extract -> quality ->
    lang -> dedup -> store -> XACK.

    Raises on Redis/DB failure so the caller can reconnect; a message is only
    acked after the whole batch succeeds, so nothing is lost mid-failure.
    """
    if not resp:
        return
    _, messages = resp[0]

    # phase 1: parse JSON (every message in the batch is acked only at the end)
    parsed = []   # (msg_id, article)
    ids_to_ack = []
    for msg_id, fields in messages:
        ids_to_ack.append(msg_id)
        raw = fields.get("article")
        if not raw:
            continue
        try:
            article = json.loads(raw)
        except json.JSONDecodeError:
            continue
        parsed.append((msg_id, article))

    # phase 2: content-in-payload fast path + concurrent fetch for the rest.
    # API-first sources (stackexchange) deliver content_md inline; the HTML
    # fetch + trafilatura extraction is skipped for them entirely.
    direct = []      # (msg_id, article, content)
    to_fetch = []    # (msg_id, article)
    for msg_id, article in parsed:
        md = article.get("content_md")
        if md:
            direct.append((msg_id, article, md))
        else:
            to_fetch.append((msg_id, article))

    # fetch_one is token-bucket limited (global + per-domain); FETCH_WORKERS
    # threads share the limiters so the aggregate QPS holds.
    htmls, gone = fetch_many([a.get("url", "") for _, a in to_fetch])
    stats = {}
    src_rej = {}     # source_type -> rejection count (per-source quality signal)
    extracted = []   # (msg_id, article, content)
    for msg_id, article, content in direct:
        extracted.append((msg_id, article, content))
    for msg_id, article in to_fetch:
        url = article.get("url", "")
        if url in gone:
            # 404/410: content is definitively gone; the DLQ could never
            # recover it, so count as rejected and XACK with the batch.
            stats["not_found"] = stats.get("not_found", 0) + 1
            src_rej[article.get("source_type", "?")] = src_rej.get(article.get("source_type", "?"), 0) + 1
            continue
        content = extract_content(htmls.get(url))
        if not content:
            stats["extract_fail"] = stats.get("extract_fail", 0) + 1
            src_rej[article.get("source_type", "?")] = src_rej.get(article.get("source_type", "?"), 0) + 1
            push_dlq(r, msg_id, article, reason="extract_fail", detail=url)
            continue
        extracted.append((msg_id, article, content))

    # phase 3: quality -> language -> dedup -> assemble store rows
    rows = []
    for msg_id, article, content in extracted:
        keep, reason, meta = assess(content)
        if not keep:
            stats[reason] = stats.get(reason, 0) + 1
            src_rej[article.get("source_type", "?")] = src_rej.get(article.get("source_type", "?"), 0) + 1
            continue

        # language detection on code-stripped text avoids misclassifying
        # code-heavy documents as non-English.
        probe_text = strip_code(content) or content
        try:
            lang = detect(probe_text)
        except LangDetectException:
            lang = "unknown"
        if lang not in LANG_KEEP:
            stats["lang"] = stats.get("lang", 0) + 1
            continue

        # exact + near duplicate detection (single-pass fingerprint reuse)
        dup, chash = is_duplicate(r, content, bits=SIMHASH_BITS)
        if dup:
            stats["dup"] = stats.get("dup", 0) + 1
            continue
        if chash in seen_hashes:
            stats["dup"] = stats.get("dup", 0) + 1
            continue
        seen_hashes.append(chash)

        url = article.get("url", "")
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        rows.append((
            url,
            url_hash,
            article.get("title"),
            content,
            article.get("author"),
            article.get("published_at"),
            article.get("tags", []),
            article.get("source_type", "devto"),
            lang,
            chash,
            float(meta.get("quality_score", 0.0)),
        ))

    if rows:
        # write first, index second: a failed insert leaves no fingerprint, so
        # retrying the same message cannot be rejected by its own simhash.
        stored = insert_articles(conn, rows)
        if stored:
            r.incrby("metrics:articles:stored", stored)
            by_source = {}
            for row in rows:
                src = row[7]
                by_source[src] = by_source.get(src, 0) + 1
            pipe = r.pipeline(transaction=False)
            for src, cnt in by_source.items():
                pipe.hincrby("metrics:articles:stored_by_source", src, cnt)
            pipe.execute()
        for row in rows:
            index(r, row[-2], bits=SIMHASH_BITS)

    # rejection metrics are written only on the path that also XACKs: if the
    # DB insert above raised, the batch will be retried and re-counted once,
    # instead of double-counting on every failed attempt.
    if stats:
        pipe = r.pipeline(transaction=False)
        pipe.incrby("metrics:articles:rejected", sum(stats.values()))
        for reason, cnt in stats.items():
            pipe.hincrby("metrics:articles:rejected_by_reason", reason, cnt)
        for src, cnt in src_rej.items():
            pipe.hincrby("metrics:articles:rejected_by_source", src, cnt)
        pipe.execute()
        logger.info("rejected=%s", stats)
    if ids_to_ack:
        r.xack(STREAM, CONSUMER_GROUP, *ids_to_ack)


def recover_pending(r, conn, seen_hashes, batch_size=BATCH_SIZE):
    """Drain leftover pending (delivered but not XACKed) messages.

    Uses the default failure policy (no ack on DB error), so a restart during
    a DB blip keeps messages in PENDING instead of dropping them.
    """
    while True:
        pending = r.xpending_range(
            STREAM, CONSUMER_GROUP,
            min="-", max="+",
            count=batch_size,
            consumername=CONSUMER_NAME,
        )
        if not pending:
            break
        resp = r.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME, {STREAM: "0"}, count=batch_size
        )
        if not resp:
            break
        process_batch(r, conn, resp, seen_hashes)


def _drop_broken_conn(conn):
    """Roll back best-effort and return a (possibly broken) pooled connection,
    closing it so the pool opens a fresh one next time."""
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception:
        pass
    try:
        putconn(conn, close=True)
    except Exception as e:
        logger.error("releasing broken connection failed: %s", e)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.xgroup_create(STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    conn = get_conn()
    # build LSH index from already-stored articles so we don't re-ingest dups
    warm_from_db(r, conn, bits=SIMHASH_BITS)

    seen_hashes = deque(maxlen=10000)

    # reclaim messages left in PENDING by a previous run (e.g. SIGTERM during a
    # batch) so they are not stuck forever; idempotent thanks to ON CONFLICT
    # and the DLQ-seen guard in push_dlq.
    recover_pending(r, conn, seen_hashes)

    logger.info("consuming stream=%s group=%s dlq=%s", STREAM, CONSUMER_GROUP, DLQ_STREAM)
    while True:
        try:
            if conn is None:
                conn = get_conn()
            resp = r.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME, {STREAM: ">"},
                count=BATCH_SIZE, block=BLOCK_MS,
            )
            if not resp:
                continue
            process_batch(r, conn, resp, seen_hashes)
        except Exception as e:
            logger.error("batch failed: %s; reconnecting in 5s", e)
            _drop_broken_conn(conn)
            conn = None
            time.sleep(5)
            # drain any messages that were delivered but not acked before the
            # failure (they are still in PENDING and would otherwise stall).
            while True:
                try:
                    conn = get_conn()
                    recover_pending(r, conn, seen_hashes)
                    break
                except Exception as e2:
                    logger.error("reconnect failed: %s; retry in 10s", e2)
                    time.sleep(10)


if __name__ == "__main__":
    main()
