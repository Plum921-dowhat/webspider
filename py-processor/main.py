"""Consume DEV.to raw articles from Redis Stream, extract + dedup + store."""
import hashlib
import json
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
        downloaded = trafilatura.fetch_url(url, timeout=10)
        if not downloaded:
            return None
        return trafilatura.extract(downloaded, include_comments=False)
    except Exception:
        return None


def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        r.xgroup_create(STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

    conn = get_conn()
    seen_hashes = {}  # content_hash -> True in-memory for this run (cheap, secondary)

    print(f"[processor] consuming stream={STREAM} group={CONSUMER_GROUP}")
    while True:
        resp = r.xreadgroup(
            CONSUMER_GROUP, CONSUMER_NAME, {STREAM: ">"},
            count=BATCH_SIZE, block=BLOCK_MS,
        )
        if not resp:
            continue

        batch = []
        ids_to_ack = []
        for _, messages in resp:
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
                if chash in seen_hashes:
                    continue
                dup = any(hamming(chash, h) <= SIMHASH_HAMMING for h in seen_hashes)
                if dup:
                    continue
                seen_hashes[chash] = True

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

        if batch:
            insert_articles(conn, batch)
            r.incrby("metrics:articles:stored", len(batch))
        if ids_to_ack:
            r.xack(STREAM, CONSUMER_GROUP, *ids_to_ack)


if __name__ == "__main__":
    main()
