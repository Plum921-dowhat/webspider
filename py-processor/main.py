"""Consume DEV.to raw articles from Redis Stream, extract + dedup + store."""
import hashlib
import json
from collections import deque

import redis
from langdetect import detect, LangDetectException
import trafilatura

from config import (
    REDIS_URL, STREAM, CONSUMER_GROUP, CONSUMER_NAME,
    BATCH_SIZE, BLOCK_MS, LANG_KEEP, SIMHASH_BITS, SIMHASH_HAMMING,
)
from dedupe import simhash, hamming
from store import get_conn, insert_articles


def extract_content(url):
    try:
        # trafilatura >= 2.1 removed the `timeout` kwarg from fetch_url.
        # It performs its own download; a hard network hang is mitigated by
        # relying on trafilatura's internal session timeout.
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None
        return trafilatura.extract(downloaded, include_comments=False)
    except Exception:
        return None


def process_batch(r, conn, resp, seen_hashes, ack_even_on_fail=False):
    """Process one batch: parse -> extract -> lang filter -> SimHash dedup -> store -> XACK.

    When ack_even_on_fail is True (used during Pending Recovery), a failed message
    is still XACKed so it cannot loop forever in the pending list.
    """
    if not resp:
        return
    _, messages = resp[0]

    batch = []
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

        url = article.get("url", "")
        content = extract_content(url)
        if not content or len(content) < 200:
            continue

        try:
            lang = detect(content)
        except LangDetectException:
            lang = "unknown"
        if lang not in LANG_KEEP:
            continue

        chash = simhash(content, bits=SIMHASH_BITS)
        # exact dup (same hash seen this run)
        if chash in seen_hashes:
            continue
        # near dup (hamming distance within threshold)
        if any(hamming(chash, h) <= SIMHASH_HAMMING for h in seen_hashes):
            continue
        seen_hashes.append(chash)
        if len(seen_hashes) > 10000:
            seen_hashes.popleft()

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
            # store failed: do NOT ack, leave in pending for later recovery
            print(f"[error] insert failed: {e}")
            ids_to_ack = [] if not ack_even_on_fail else ids_to_ack

    if stored:
        r.incrby("metrics:articles:stored", stored)
    if ids_to_ack:
        r.xack(STREAM, CONSUMER_GROUP, *ids_to_ack)


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
        # ack even on failure so a broken message cannot loop forever
        process_batch(r, conn, resp, seen_hashes, ack_even_on_fail=True)


def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.xgroup_create(STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    conn = get_conn()
    # Scheme A: seen_hashes lives in main() and is passed in as an argument.
    seen_hashes = deque(maxlen=10000)

    # 1. recover pending messages left from a previous run / crash
    recover_pending(r, conn, seen_hashes)

    # 2. then switch to ">" to read only new, undelivered messages
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
