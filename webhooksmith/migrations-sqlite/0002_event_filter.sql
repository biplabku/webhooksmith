-- Optional event type filter per endpoint.
-- NULL means the endpoint receives all events from broadcast() (backward compatible).
-- Non-null: JSON array of patterns, e.g. '["order.*","payment.captured"]'
ALTER TABLE webhook_endpoints ADD COLUMN event_filter TEXT;
