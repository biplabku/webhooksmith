-- Endpoints: where webhooks are delivered to
CREATE TABLE webhook_endpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url             TEXT NOT NULL,
    signing_secret  TEXT NOT NULL,
    description     TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT true,
    max_attempts    INTEGER NOT NULL DEFAULT 10,
    initial_delay_ms INTEGER NOT NULL DEFAULT 1000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Events: one row per webhook that needs to be delivered
-- Written atomically with your business data (transactional outbox)
CREATE TABLE webhook_events (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id UUID NOT NULL REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'delivering', 'delivered', 'failed', 'dead')),
    attempts    INTEGER NOT NULL DEFAULT 0,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX webhook_events_status_scheduled
    ON webhook_events (status, scheduled_at)
    WHERE status IN ('pending', 'failed');

-- Delivery attempts: full audit log of every HTTP call made
CREATE TABLE webhook_delivery_attempts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    response_status INTEGER,
    response_body   TEXT,
    duration_ms     INTEGER,
    error           TEXT,
    success         BOOLEAN NOT NULL
);

CREATE INDEX webhook_delivery_attempts_event_id
    ON webhook_delivery_attempts (event_id);
