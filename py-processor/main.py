"""Consume DEV.to raw articles from Redis Stream.

Pipeline per message:
  fetch HTML (concurrent, token-bucket limited)
  -> extract Markdown with trafilatura (code fences preserved)
  -> quality gate (wide-in / strict-out)
  -> language filter (on code-stripped text)
  -> persistent near-dup dedup (SimHash LSH in Redis)
  -> store to PostgreSQL
"""
import hashlib
import json
from collections import deque

import redis
from langdetect import detect, LangDetectException
import trafilatura

from config import (
    REDIS_URL, STREAM, CONSUMER_GROUP, CONSUMER_NAME,
    BATCH_SIZE, BLOCK_MS, LANG_KEEP, SIMHASH_BITS,
)
from fetcher import fetch_one
from quality import assess, strip_code
from neardup import is_duplicate, index, warm_from_db
from store import get_conn, insert_articles


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


def process_batch(r, conn, resp, seen_hashes, ack_even_on_fail=False):
    """Process one batch: parse -> fetch -> extract -> quality -> lang ->
    dedup -> store -> XACK.
    """
    if not resp:
        return
    _, messages = resp[0]

    batch = []
    ids_to_ack = []
    stats = {}
    for msg_id, fields in messages:
        ids_to_ack.append(msg_id)
        raw = fields.get("article")
        if not raw:
            continue
        try:
            article = json.loads(raw)
        except json.JSONDecodeError:
            continue

        url = article.get("url", "")
        html = fetch_one(url)
        content = extract_content(html)
        if not content:
            stats["extract_fail"] = stats.get("extract_fail", 0) + 1
            continue

        keep, reason, meta = assess(content)
        if not keep:
            stats[reason] = stats.get(reason, 0) + 1
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

        # exact + near duplicate detection
        if is_duplicate(r, content, bits=SIMHASH_BITS):
            stats["dup"] = stats.get("dup", 0) + 1
            continue
        chash = simhash_of(content)
        if chash in seen_hashes:
            stats["dup"] = stats.get("dup", 0) + 1
            continue
        seen_hashes.append(chash)
        if len(seen_hashes) > 10000:
            seen_hashes.popleft()
        index(r, content, bits=SIMHASH_BITS)

        url_hash = hashlib.sha256(url.encode()).hexdigest()
        batch.append((
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
        ))

    stored = 0
    if batch:
        try:
            stored = insert_articles(conn, batch)
        except Exception as e:
            print(f"[error] insert failed: {e}")
            ids_to_ack = [] if not ack_even_on_fail else ids_to_ack

    if stored:
        r.incrby("metrics:articles:stored", stored)
    if stats:
        print(f"[processor] rejected={stats}")
    if ids_to_ack:
        r.xack(STREAM, CONSUMER_GROUP, *ids_to_ack)


def simhash_of(text):
    from dedupe import simhash
    return simhash(text, bits=SIMHASH_BITS)


def recover_pending(r, conn, seen_hashes, batch_size=BATCH_SIZE):
    """On startup, drain leftover pending (delivered but not XACKed) messages."""
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
        process_batch(r, conn, resp, seen_hashes, ack_even_on_fail=True)


def main():
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

    recover_pending(r, conn, seen_hashes)

    print(f"[processor] consuming stream={STREAM} group={CONSUMER_GROUP}")
    while True:
        resp = r.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME, {STREAM: ">"},
            count=BATCH_SIZE, block=BLOCK_MS,
        )
        if not resp:
            continue
        process_batch(r, conn, resp, seen_hashes, ack_even_on_fail=False)


if __name__ == "__main__":
    main()
