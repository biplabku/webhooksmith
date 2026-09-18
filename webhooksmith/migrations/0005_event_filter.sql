-- Optional event type filter per endpoint.
-- NULL means the endpoint receives all events from broadcast() (default, backward compatible).
-- Non-null: TEXT[] of patterns, e.g. '{order.*,payment.captured}'
-- Pattern rules:
--   'order.created'  — exact match
--   'order.*'        — any event starting with 'order.'
--   '*'              — matches all events
ALTER TABLE webhook_endpoints ADD COLUMN event_filter TEXT[];
