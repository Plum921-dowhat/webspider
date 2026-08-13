-- WebSpider corpus schema
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS articles (
    id            BIGSERIAL PRIMARY KEY,
    url           TEXT NOT NULL,
    url_hash      CHAR(64) NOT NULL UNIQUE,
    title         TEXT,
    content_md    TEXT,
    author        TEXT,
    published_at  TIMESTAMPTZ,
    crawled_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    tags          TEXT[] DEFAULT '{}',
    source_type   TEXT NOT NULL,
    language      TEXT,
    quality_score NUMERIC,
    content_hash  NUMERIC(20)
);

CREATE INDEX IF NOT EXISTS idx_articles_published ON articles (published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_source    ON articles (source_type);
CREATE INDEX IF NOT EXISTS idx_articles_tags      ON articles USING GIN (tags);
