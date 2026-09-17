ALTER TABLE webhook_events ADD COLUMN idempotency_key TEXT;

-- Partial unique index: enforces uniqueness only when a key is provided.
-- Rows with idempotency_key IS NULL are unconstrained (backwards-compatible).
CREATE UNIQUE INDEX webhook_events_idempotency
    ON webhook_events (endpoint_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
