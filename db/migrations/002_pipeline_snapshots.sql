-- 002: pipeline health snapshots for the dashboard monitoring panel (idempotent).
--   cat db/migrations/002_pipeline_snapshots.sql | \
--     docker compose exec -T postgres psql -U crawler -d corpus
CREATE TABLE IF NOT EXISTS pipeline_snapshots (
    id         BIGSERIAL PRIMARY KEY,
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    produced   BIGINT NOT NULL DEFAULT 0,
    stored     BIGINT NOT NULL DEFAULT 0,
    rejected   BIGINT NOT NULL DEFAULT 0,
    pending    INT    NOT NULL DEFAULT 0,
    dlq        BIGINT NOT NULL DEFAULT 0,
    stream_len BIGINT NOT NULL DEFAULT 0,
    by_source  JSONB  NOT NULL DEFAULT '{}'   -- {src: {produced_delta, stored_delta}}
);
CREATE INDEX IF NOT EXISTS idx_snapshots_taken ON pipeline_snapshots (taken_at DESC);
