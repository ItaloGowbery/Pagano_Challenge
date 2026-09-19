CREATE TABLE IF NOT EXISTS readings (
    id          BIGSERIAL PRIMARY KEY,
    tag         TEXT NOT NULL,
    value       DOUBLE PRECISION,
    status      TEXT NOT NULL,
    timestamp   TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS readings_tag_timestamp_idx
    ON readings (tag, timestamp DESC);
