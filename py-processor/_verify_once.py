"""One-shot end-to-end verification: write a valid-JSON message, consume, fetch,
extract, dedup, store, and XACK. Bypasses shell quoting by writing via redis-py."""
import json
import hashlib
from collections import deque

import redis
import trafilatura
from langdetect import detect, LangDetectException

from config import (
    REDIS_URL, STREAM, CONSUMER_GROUP, CONSUMER_NAME,
    LANG_KEEP, SIMHASH_BITS, SIMHASH_HAMMING,
)
from dedupe import simhash, hamming
from store import get_conn, insert_articles


def main():
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    conn = get_conn()
    seen = deque(maxlen=10000)

    article = {
        "url": "https://dev.to/gde/amazon-bedrock-agents-orchestrating-google-adk-over-a2a-5c2b",
        "title": "Amazon Bedrock Agents Orchestrating Google ADK over A2A",
        "author": "xbill",
        "published_at": "2026-07-29T19:14:47Z",
        "tags": ["agents", "googleadk", "a2aprotocol", "aws"],
        "source_type": "devto",
    }
    payload = json.dumps(article)
    mid = r.xadd(STREAM, {"article": payload})
    print(f"[verify] wrote message {mid} with valid JSON")

    resp = r.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {STREAM: ">"}, count=5, block=3000)
    print(f"[verify] xreadgroup -> {resp}")

    if not resp:
        print("[verify] NO NEW MESSAGE — group already advanced past it")
        return

    _, messages = resp[0]
    for msg_id, fields in messages:
        raw = fields.get("article")
        print(f"[verify] msg {msg_id} article field len={len(raw) if raw else 0}")
        art = json.loads(raw)  # will raise if not valid JSON
        print(f"[verify] parsed json ok: title={art['title']!r}")

        url = art["url"]
        print(f"[verify] fetching {url}")
        downloaded = trafilatura.fetch_url(url)
        print(f"[verify] fetched bytes={len(downloaded) if downloaded else 0}")
        if not downloaded:
            print("[verify] FETCH FAILED (no network to dev.to?)")
            r.xack(STREAM, CONSUMER_GROUP, msg_id)
            continue

        text = trafilatura.extract(downloaded, include_comments=False)
        print(f"[verify] extracted content length={len(text) if text else 0}")
        if not text or len(text) < 200:
            print("[verify] content too short; skipping store")
            r.xack(STREAM, CONSUMER_GROUP, msg_id)
            continue

        try:
            lang = detect(text)
        except LangDetectException:
            lang = "unknown"
        print(f"[verify] detected language={lang}")
        if lang not in LANG_KEEP:
            print(f"[verify] language {lang!r} not in LANG_KEEP={LANG_KEEP}; skip")
            r.xack(STREAM, CONSUMER_GROUP, msg_id)
            continue

        chash = simhash(text, bits=SIMHASH_BITS)
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        row = (
            url, url_hash, art.get("title"), text, art.get("author"),
            art.get("published_at"), art.get("tags", []),
            art.get("source_type", "devto"), lang, chash,
        )
        n = insert_articles(conn, [row])
        print(f"[verify] inserted {n} row(s) into Postgres")
        r.xack(STREAM, CONSUMER_GROUP, msg_id)
        print(f"[verify] acked {msg_id}")


if __name__ == "__main__":
    main()
