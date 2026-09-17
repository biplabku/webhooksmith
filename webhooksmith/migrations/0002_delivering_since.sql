-- Tracks when a worker claimed an event for delivery.
-- A reaper can reset events stuck in 'delivering' for too long (worker crash recovery).
ALTER TABLE webhook_events ADD COLUMN delivering_since TIMESTAMPTZ;
