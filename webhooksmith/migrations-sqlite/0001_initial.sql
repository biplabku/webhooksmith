-- SQLite-compatible schema
-- UUIDs stored as TEXT, timestamps as TEXT (ISO 8601), JSON as TEXT

CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id               TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    signing_secret   TEXT NOT NULL,
    description      TEXT,
    enabled          INTEGER NOT NULL DEFAULT 1,
    max_attempts     INTEGER NOT NULL DEFAULT 10,
    initial_delay_ms INTEGER NOT NULL DEFAULT 1000,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS webhook_events (
    id               TEXT PRIMARY KEY,
    endpoint_id      TEXT NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    event_type       TEXT NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending','delivering','delivered','failed','dead')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    scheduled_at     TEXT NOT NULL DEFAULT (datetime('now')),
    delivering_since TEXT,
    idempotency_key  TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(endpoint_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS webhook_events_status_scheduled
    ON webhook_events (status, scheduled_at)
    WHERE status IN ('pending', 'failed');

CREATE TABLE IF NOT EXISTS webhook_delivery_attempts (
    id              TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    attempted_at    TEXT NOT NULL DEFAULT (datetime('now')),
    response_status INTEGER,
    response_body   TEXT,
    duration_ms     INTEGER,
    error           TEXT,
    success         INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS webhook_delivery_attempts_event_id
    ON webhook_delivery_attempts (event_id);

-- Note: updated_at is maintained by the application layer in SQLite
-- (no trigger — trigger BEGIN/END syntax conflicts with statement splitting)
