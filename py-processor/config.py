import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.getenv("STREAM", "articles")
DLQ_STREAM = os.getenv("DLQ_STREAM", "articles_dlq")
DLQ_SEEN_KEY = os.getenv("DLQ_SEEN_KEY", "dlq:seen")
DLQ_MAXLEN = int(os.getenv("DLQ_MAXLEN", "20000"))
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "py")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "proc-1")

# PostgreSQL DSN. Example: postgresql://crawler:crawler_pwd@localhost:5432/corpus
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://crawler:crawler_pwd@localhost:5432/corpus",
)

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))
LANG_KEEP = os.getenv("LANG_KEEP", "en").split(",")
SIMHASH_BITS = int(os.getenv("SIMHASH_BITS", "64"))
SIMHASH_HAMMING = int(os.getenv("SIMHASH_HAMMING", "3"))  # <= threshold == duplicate

# --- Fetcher (concurrent download + global token-bucket rate limit) ---
FETCH_WORKERS = int(os.getenv("FETCH_WORKERS", "8"))   # concurrent GETs
FETCH_QPS = float(os.getenv("FETCH_QPS", "6"))         # aggregate QPS to DEV.to
FETCH_TIMEOUT = int(os.getenv("FETCH_TIMEOUT", "15"))
FETCH_RETRIES = int(os.getenv("FETCH_RETRIES", "3"))
FETCH_MAX_BACKOFF = float(os.getenv("FETCH_MAX_BACKOFF", "60"))

# Per-domain politeness cap (the global bucket alone can hammer one host when
# a batch of URLs shares the same domain). Per-host overrides, e.g.
# "dev.to:3,stackoverflow.com:2", raise the cap for high-volume first-party hosts.
FETCH_HOST_QPS = float(os.getenv("FETCH_HOST_QPS", "1.0"))
FETCH_HOST_OVERRIDES = os.getenv("FETCH_HOST_OVERRIDES", "")

# --- SimHash LSH banding (persistent in Redis) ---
SIMHASH_BANDS = int(os.getenv("SIMHASH_BANDS", "4"))
NEARDUP_TTL = int(os.getenv("NEARDUP_TTL", "0"))       # 0 = never expire

# --- Quality gate ("wide in, strict out") ---
MIN_CONTENT_LEN = int(os.getenv("MIN_CONTENT_LEN", "100"))
MIN_CODE_CHARS = int(os.getenv("MIN_CODE_CHARS", "80"))
MAX_LINK_RATIO = float(os.getenv("MAX_LINK_RATIO", "0.30"))
MIN_UNIQUE_RATIO = float(os.getenv("MIN_UNIQUE_RATIO", "0.25"))
MIN_WORDS_NO_CODE = int(os.getenv("MIN_WORDS_NO_CODE", "50"))
