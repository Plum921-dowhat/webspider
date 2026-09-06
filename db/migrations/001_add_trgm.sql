-- 001: trigram indexes for dashboard ILIKE search (idempotent).
-- Apply to an existing database (docker-entrypoint-initdb.d only runs on
-- first init):
--   cat db/migrations/001_add_trgm.sql | \
--     docker compose exec -T postgres psql -U crawler -d corpus
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_articles_content_trgm ON articles USING GIN (content_md gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_articles_url_trgm     ON articles USING GIN (url gin_trgm_ops);
