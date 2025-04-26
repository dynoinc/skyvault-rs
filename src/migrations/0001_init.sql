CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    belongs_to JSONB NOT NULL DEFAULT '{}',
    stats JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS changelog (
    id BIGSERIAL PRIMARY KEY,
    changes JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS jobs (
    id BIGSERIAL PRIMARY KEY,
    typ TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    job JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs (status) WHERE status IN ('pending', 'running');
