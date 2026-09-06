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

-- Pipeline health snapshots written by the monitor sampler, rendered by the
-- dashboard monitoring panel. Monitors only read; retention ~7 days.
CREATE TABLE IF NOT EXISTS pipeline_snapshots (
    id         BIGSERIAL PRIMARY KEY,
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    produced   BIGINT NOT NULL DEFAULT 0,
    stored     BIGINT NOT NULL DEFAULT 0,
    rejected   BIGINT NOT NULL DEFAULT 0,
    pending    INT    NOT NULL DEFAULT 0,
    dlq        BIGINT NOT NULL DEFAULT 0,
    stream_len BIGINT NOT NULL DEFAULT 0,
    by_source  JSONB  NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_snapshots_taken ON pipeline_snapshots (taken_at DESC);

-- Trigram indexes so dashboard ILIKE '%q%' search on url/content_md uses an
-- index instead of a sequential scan. Only applied automatically on FIRST
-- container init; for an existing database run db/migrations/001_add_trgm.sql.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_articles_content_trgm ON articles USING GIN (content_md gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_articles_url_trgm     ON articles USING GIN (url gin_trgm_ops);
