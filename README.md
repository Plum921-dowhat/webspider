# WebSpider — EN Tech Intelligence & Pretraining Corpus Pipeline

Phase 0-2: crawl (Go) -> Redis Stream -> process (Python) -> PostgreSQL.

## Architecture
- **go-crawler**: high-concurrency fetcher for DEV.to (official API, no auth).
  - worker pool + token-bucket rate limit + exponential backoff
  - incremental cursor on `published_at`
  - atomic dedup via Redis SET + Lua into Redis Stream
- **Redis Stream** (`articles`): Go -> Python bridge, consumer group `py`.
- **py-processor**: trafilatura extract, langdetect (keep `en`), SimHash dedup,
  psycopg2 bulk insert with `ON CONFLICT DO NOTHING` on `url_hash`.

## Run
```bash
# 1. start infra
docker compose up -d

# 2. build & run crawler
cd go-crawler
go mod tidy
go run ./cmd/crawler

# 3. run processor
cd ../py-processor
pip install -r requirements.txt
python main.py

# 4. export corpus
python export.py --out corpus.jsonl
```

## Config
- `go-crawler/config.yaml`
- `py-processor/config.py` (env overrides: `REDIS_URL`, `PG_DSN`)
