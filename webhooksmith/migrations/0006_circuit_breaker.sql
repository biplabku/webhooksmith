-- Per-endpoint circuit breaker.
-- consecutive_failures: resets to 0 on any success; increments on each failure.
-- circuit_open_until: NULL = closed; non-NULL = open until this timestamp.
ALTER TABLE webhook_endpoints
    ADD COLUMN IF NOT EXISTS consecutive_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS circuit_open_until   TIMESTAMPTZ;
