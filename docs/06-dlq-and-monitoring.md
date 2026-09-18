# DLQ and Monitoring

## Queue stats

```rust
let stats = engine.queue_stats().await?;
println!("pending:   {}", stats.pending);
println!("failed:    {}", stats.failed);   // will retry
println!("dead:      {}", stats.dead);     // exhausted retries → DLQ
println!("delivered: {}", stats.delivered);
```

Set up an alert if `stats.dead > 0` or `stats.failed` grows unboundedly.

## Inspecting the DLQ

```rust
// All dead events for a specific endpoint (paginated)
let dead = engine.dead_events_paged(endpoint_id, 50, 0).await?;
for ev in &dead {
    println!("{}: {} attempts, payload: {}", ev.id, ev.attempts, ev.payload);
}
```

## Retrying from the DLQ

```rust
// Re-queue a single event + reset endpoint circuit breaker
engine.retry_dead(event_id).await?;

// Re-queue ALL dead events for an endpoint + reset circuit
let count = engine.retry_all_dead(endpoint_id).await?;
println!("re-queued {} events", count);
```

Both calls reset the endpoint's circuit breaker state so delivery can proceed
immediately.

## Delivery log

Every attempt (success or failure) is recorded:

```rust
let log = engine.delivery_log(event_id).await?;
for attempt in &log {
    println!(
        "attempt at {}: status={:?}, error={:?}, duration={}ms",
        attempt.attempted_at, attempt.response_status,
        attempt.error, attempt.duration_ms.unwrap_or(0)
    );
}
```

## Cleanup

Delivered events accumulate over time. Prune them on a schedule:

```rust
use std::time::Duration;

// Delete events delivered more than 30 days ago
let deleted = engine.cleanup_delivered(Duration::from_secs(30 * 24 * 3600)).await?;
tracing::info!(deleted, "cleaned delivered events");

// Delete dead events older than 7 days (irretrievable, operator has reviewed)
engine.cleanup_dead(Duration::from_secs(7 * 24 * 3600)).await?;
```

## Stuck delivery recovery

If your worker crashes mid-delivery, events can get stuck in `delivering` state.
The worker calls `recover_stuck_deliveries` automatically on each tick, but you
can call it manually:

```rust
use std::time::Duration;

// Reset events stuck in 'delivering' for > 2 minutes
let recovered = engine.recover_stuck_deliveries(Duration::from_secs(120)).await?;
if recovered > 0 {
    tracing::warn!(recovered, "reset stuck events");
}
```

## Admin API equivalent

All of the above is also available via the admin HTTP endpoints if you've mounted
`webhooksmith_axum::admin(engine)`. See [03-admin-api.md](03-admin-api.md).
