-- Per-endpoint circuit breaker.
ALTER TABLE webhook_endpoints ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE webhook_endpoints ADD COLUMN circuit_open_until   TEXT;
