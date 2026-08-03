import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM = os.getenv("STREAM", "articles")
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
