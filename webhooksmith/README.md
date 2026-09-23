# webhooksmith

Webhook delivery for Rust backed by Postgres or SQLite. HMAC-SHA256 signing,
automatic retry with exponential backoff, dead letter queue, and event type filtering.
Postgres: full transactional outbox (atomic writes). SQLite: persistent delivery without Postgres.

**Postgres backend (default — best for production, transactional outbox):**
```toml
[dependencies]
webhooksmith = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

**SQLite backend (desktop apps, CLI tools, embedded — no Postgres needed):**
```toml
[dependencies]
webhooksmith = { version = "0.1", features = ["sqlite"] }
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

With SQLite, use `SqliteEngine` — same API as `WebhookEngine`:
```rust
use webhooksmith::SqliteEngine;

let engine = SqliteEngine::new("sqlite:webhooks.db").await?;
engine.migrate().await?;
// All the same methods: send, broadcast, retry_dead, queue_stats, etc.
```

> **SQLite transactional outbox:** `SqliteEngine::send_in_tx()` provides true atomicity.
> The event is written on the transaction's connection — rollback removes it, commit persists it.
> Both backends support the full transactional outbox pattern.

---

## How it works

1. Your app registers partner webhook endpoints in Postgres or SQLite.
2. When an event happens, you call `engine.send()` — the event is saved to your database.
3. The background worker picks it up and POSTs it with an HMAC-SHA256 signature.
4. Failures retry with exponential backoff. After `max_attempts` failures, the event moves to a dead-letter queue.

No Redis, no queuing service, no external infrastructure. Just your existing database.

---

## Quick start

```rust
use webhooksmith::WebhookEngine;
use serde_json::json;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = WebhookEngine::builder()
        .database_url("postgres://user:pass@localhost/mydb")
        .build()
        .await?;

    // Creates the 3 required tables (safe to call on every startup)
    engine.migrate().await?;

    // Register a partner endpoint
    let endpoint = engine
        .register("https://partner.example.com/webhooks", "your-signing-secret-32chars")
        .await?;

    // Queue a webhook event
    engine.send("order.created", json!({"id": 1001, "total": 49.99}), endpoint.id).await?;

    // Start the delivery worker (blocks until the process exits)
    engine.run().await;
}
```

---

## Transactional outbox

Write your business data and the webhook event in the **same database transaction**.
If the transaction rolls back, the event never exists. If it commits, the event is guaranteed to be delivered.

```rust
let mut tx = engine.pool().begin().await?;

// Your business logic
sqlx::query!("INSERT INTO orders (id, total) VALUES ($1, $2)", order_id, 49.99)
    .execute(&mut *tx)
    .await?;

// Webhook in the same transaction — only queued if this tx commits
engine
    .send_in_tx("order.created", json!({"id": order_id}), endpoint.id, &mut tx)
    .await?;

tx.commit().await?;
```

Fan-out to all endpoints at once, atomically:

```rust
let mut tx = engine.pool().begin().await?;
engine.broadcast_in_tx("order.created", json!({"id": 1001}), &mut tx).await?;
tx.commit().await?;
```

---

## All sending methods

| Method | What it does |
|---|---|
| `send(event_type, payload, endpoint_id)` | Send to one endpoint |
| `send_in_tx(event_type, payload, endpoint_id, &mut tx)` | Send to one endpoint, inside your transaction |
| `send_idempotent(event_type, payload, endpoint_id, key)` | Send, deduplicated by key — safe to retry |
| `send_idempotent_in_tx(event_type, payload, endpoint_id, key, &mut tx)` | Idempotent + transactional |
| `broadcast(event_type, payload)` | Fan-out to ALL enabled endpoints |
| `broadcast_in_tx(event_type, payload, &mut tx)` | Fan-out inside your transaction |
| `broadcast_idempotent(event_type, payload, key)` | Fan-out, deduplicated per endpoint by key |

**Validation rules:**
- `event_type` must be non-empty, non-whitespace, ≤ 256 bytes, no control characters
- `payload` must be valid JSON, ≤ 1 MB

**Return value (`WebhookEvent`):**
```
WebhookEvent {
    id: Uuid,                           // unique event ID
    endpoint_id: Uuid,                  // which endpoint this is for
    event_type: "order.created",
    payload: {"id": 1001, "total": 49.99},
    status: Pending,                    // Pending | Delivering | Delivered | Failed | Dead
    attempts: 0,
    scheduled_at: "2026-01-01T00:00:00Z",
    delivering_since: None,
    idempotency_key: None,
    created_at: "2026-01-01T00:00:00Z",
}
```

---

## Event type filtering

Each endpoint can subscribe to specific event types. `broadcast()` routes automatically.

```rust
// Register with a filter — only receives order events
engine.register_with(webhooksmith::NewEndpoint {
    url: "https://partner.com/hooks".into(),
    signing_secret: "secret-min-16-chars".into(),
    event_filter: Some(vec!["order.*".into()]),
    ..Default::default()  // or specify each field
}).await?;

// Broadcast routes by subscription:
// "order.created" → goes to endpoints subscribed to "order.*" or "*" or no filter
// "payment.captured" → does NOT go to an endpoint subscribed only to "order.*"
engine.broadcast("order.created", payload).await?;

// Update filter at any time
engine.set_event_filter(ep_id, vec!["order.*".into(), "payment.captured".into()]).await?;
engine.clear_event_filter(ep_id).await?;  // receive all events again
```

**Pattern rules:**

| Pattern | Matches |
|---|---|
| `"order.created"` | Exactly `"order.created"` |
| `"order.*"` | Any event starting with `"order."` (e.g. `"order.created"`, `"order.cancelled"`) |
| `"*"` | Everything |
| `None` (no filter) | Everything — default, backward compatible |
| `Some(vec![])` (empty) | Nothing |

`send()` to a specific endpoint always delivers — the filter only applies to `broadcast()`.

---

## Idempotency keys

Protect against double-sends when your code retries on network errors.
The same key for the same endpoint always returns the same event, no matter how many times you call it.

```rust
// First call: creates the event
let ev1 = engine.send_idempotent("order.created", payload.clone(), ep.id, "order-1001").await?;

// Second call (e.g. after a retry): returns the SAME event
let ev2 = engine.send_idempotent("order.created", payload.clone(), ep.id, "order-1001").await?;

assert_eq!(ev1.id, ev2.id); // same event, not a duplicate
```

Key is scoped per `(endpoint_id, key)`. The same key is independent across different endpoints,
which makes `broadcast_idempotent` safe to call multiple times.

---

## Running the worker

```rust
// Blocks forever — put this at the end of main()
engine.run().await;

// Graceful shutdown — current batch drains before exit
engine.run_graceful(async {
    tokio::signal::ctrl_c().await.ok();
}).await;

// One cycle — useful for testing or cron-style invocation
let delivered_count: usize = engine.run_once().await?;
```

**What happens in each cycle:**
1. Reset events stuck in `delivering` state for longer than `stuck_timeout` (crash recovery)
2. Claim a batch of due events with `SELECT FOR UPDATE SKIP LOCKED`
3. Deliver each event concurrently via HTTP POST with HMAC signature
4. Record success or failure; schedule retry or move to DLQ

---

## Endpoint management

```rust
// Register
let ep = engine.register("https://partner.com/hooks", "secret-32chars-min").await?;

// Register with full config
let ep = engine.register_with(webhooksmith::NewEndpoint {
    url: "https://partner.com/hooks".into(),
    signing_secret: "secret-32chars-min".into(),
    description: Some("Partner A".into()),
    max_attempts: Some(10),       // retries before DLQ (default: 10, min: 1)
    initial_delay_ms: Some(1000), // first retry delay ms (default: 1000, min: 1)
    ..Default::default()
}).await?;

// Fetch one
let ep: Option<Endpoint> = engine.endpoint(ep.id).await?;

// List all (ordered by created_at)
let all: Vec<Endpoint> = engine.list_endpoints().await?;

// Paginated list
let page: Vec<Endpoint> = engine.list_endpoints_paged(20, 0).await?; // limit=20, offset=0

// Update (only fields you set are changed)
let updated = engine.update_endpoint(ep.id, webhooksmith::UpdateEndpoint {
    url: Some("https://new-partner.com/hooks".into()),
    max_attempts: Some(5),
    ..Default::default()
}).await?;

// Enable / disable (stops the worker from delivering to this endpoint)
engine.disable_endpoint(ep.id).await?;
engine.enable_endpoint(ep.id).await?;

// Delete (cascade-deletes all events for this endpoint)
engine.delete_endpoint(ep.id).await?;
```

**Validation rules:**
- `url` must be `http://` or `https://` and not point to a private/loopback/link-local address
- `signing_secret` must be ≥ 16 characters
- `max_attempts` must be ≥ 1
- `initial_delay_ms` must be ≥ 1

**`Endpoint` fields:**
```
Endpoint {
    id: Uuid,
    url: "https://partner.com/hooks",
    signing_secret: "your-secret",
    description: Some("Partner A"),
    enabled: true,
    max_attempts: 10,
    initial_delay_ms: 1000,
    event_filter: Some(["order.*"]),    // None = receive all
    consecutive_failures: 0,            // circuit breaker counter
    circuit_open_until: None,           // Some(DateTime) when circuit is open
    created_at: "2026-01-01T00:00:00Z",
    updated_at: "2026-01-01T00:00:00Z",
}
```

---

## Monitoring & queue operations

```rust
// Count events by status — one DB query
let stats: QueueStats = engine.queue_stats().await?;
// QueueStats { pending: 42, delivering: 3, failed: 1, dead: 0, delivered: 1500 }

// Events for one endpoint with a specific status (paginated, newest first)
let failed: Vec<WebhookEvent> = engine
    .events_by_status(ep.id, EventStatus::Failed, 20, 0)
    .await?;

// Events across ALL endpoints (for global monitoring dashboards)
let all_failed: Vec<WebhookEvent> = engine
    .events_global(EventStatus::Failed, 50, 0)
    .await?;

// Full delivery attempt history for one event
let log: Vec<DeliveryAttempt> = engine.delivery_log(event.id).await?;
// DeliveryAttempt { attempted_at, response_status: Some(500), duration_ms: Some(243), error: Some("HTTP 500"), success: false }

// Get one event by ID
let ev: Option<WebhookEvent> = engine.event(event_id).await?;
```

---

## Circuit breaker

After 5 consecutive delivery failures the endpoint's circuit opens. The worker skips it
until the open window expires (5 min → 10 min → 20 min … capped at 320 min, doubling each time).
A single successful delivery resets the counter. Manual DLQ retry (`retry_all_dead`) also resets it.

```rust
let ep = engine.endpoint(ep_id).await?.unwrap();
println!("failures: {}", ep.consecutive_failures);
println!("open until: {:?}", ep.circuit_open_until); // None = closed
```

---

## Dead letter queue

When an event exceeds `max_attempts` failures it moves to `status = Dead`.

```rust
// List dead events for one endpoint (all, unordered)
let dead: Vec<WebhookEvent> = engine.dead_events(ep.id).await?;

// Paginated (newest first)
let page: Vec<WebhookEvent> = engine.dead_events_paged(ep.id, 20, 0).await?;

// Requeue one event (resets attempts to 0)
engine.retry_dead(event.id).await?;
// Returns HooksmithError::InvalidState if event is not dead
// Returns HooksmithError::EventNotFound if event doesn't exist

// Requeue all dead events for one endpoint — returns count requeued
let requeued: u64 = engine.retry_all_dead(ep.id).await?;
```

---

## Cleanup

Delivered and dead events accumulate indefinitely unless cleaned up.

```rust
use std::time::Duration;

// Delete delivered events older than 7 days — returns count deleted
let removed = engine.cleanup_delivered(Duration::from_secs(7 * 86_400)).await?;

// Delete dead events older than 30 days — returns count deleted
let removed = engine.cleanup_dead(Duration::from_secs(30 * 86_400)).await?;
```

Only the specified status is touched — pending, delivering, and failed events are never deleted by cleanup.

---

## Crash recovery

The worker calls this automatically every cycle. You can also call it manually for ops use:

```rust
// Reset events stuck in 'delivering' for > 120 seconds (returns count reset)
let reset = engine.recover_stuck_deliveries(Duration::from_secs(120)).await?;
```

---

## What is sent to the endpoint

The worker sends an HTTP POST with:

```
POST https://partner.example.com/webhooks
Content-Type: application/json
x-hooksmith-signature: v1,<hex-encoded-hmac-sha256>
x-hooksmith-timestamp: 1735689600
x-hooksmith-event-id: 550e8400-e29b-41d4-a716-446655440000
x-hooksmith-event-type: order.created

{"id": 1001, "total": 49.99}
```

**Signature format** (Svix-compatible):
The HMAC-SHA256 is computed over `{timestamp}.{body}` using your signing secret.

**Verifying on the receiving side** with `webhooksmith-axum` (axum) or `webhooksmith-actix` (actix-web):
```rust
use axum::{Router, routing::post, http::StatusCode};
use webhooksmith_axum::{WebhookSecretLayer, VerifiedWebhook};

async fn handle(VerifiedWebhook(payload): VerifiedWebhook) -> StatusCode {
    println!("{}: {:?}", payload.event_type, payload.body);
    StatusCode::OK
}

let app: Router = Router::new()
    .route("/webhooks", post(handle))
    .layer(WebhookSecretLayer::new("your-signing-secret"));
```

**Verifying manually** without the axum crate:
```rust
use webhooksmith::signing;

fn verify_request(secret: &str, timestamp_header: &str, signature_header: &str, body: &[u8]) -> bool {
    let timestamp: i64 = timestamp_header.parse().unwrap_or(0);
    signing::verify(secret, timestamp, body, signature_header)
    // Returns false if: wrong secret, timestamp older than 5 minutes, or tampered body
}
```

---

## Retry behaviour

Failed deliveries are retried with exponential backoff and full jitter:

| Attempt | Max delay |
|---|---|
| 1st retry | `initial_delay_ms` × 2 (default: 2s) |
| 2nd retry | `initial_delay_ms` × 4 (default: 4s) |
| … | … |
| Any | Capped at 1 hour |

After `max_attempts` failures (default: 10), the event moves to `status = Dead` and is not retried again automatically. Use `retry_dead` or `retry_all_dead` to requeue.

**HTTP responses treated as failure:** anything outside 2xx. `3xx` redirects are explicitly not followed.

---

## Configuration

```rust
let engine = WebhookEngine::builder()
    .database_url("postgres://user:pass@localhost/mydb")
    // OR: .pool(existing_sqlx_pool)

    // Worker
    .batch_size(50)                              // events per cycle (default: 50, min: 1)
    .poll_interval(Duration::from_millis(500))   // idle sleep (default: 500ms)
    .http_timeout(Duration::from_secs(30))       // per-request timeout (default: 30s)
    .stuck_timeout(Duration::from_secs(120))     // reaper threshold (default: 120s)
    // http_timeout MUST be < stuck_timeout (panics otherwise)

    // Postgres pool
    .max_connections(20)                         // pool size (default: 20)
    .acquire_timeout(Duration::from_secs(10))    // connection wait limit (default: 10s)

    // Development only — skips SSRF URL validation
    // .allow_insecure_urls()

    .build()
    .await?;
```

---

## SSRF protection

The following URL targets are blocked at endpoint registration and at delivery time (DNS rebinding protection):

- Loopback: `127.x.x.x`, `::1`, `localhost`
- Private IPv4: `10.x`, `172.16–31.x`, `192.168.x`
- Link-local: `169.254.x.x` (AWS metadata endpoint), `fe80::/10`
- CGNAT: `100.64.0.0/10`
- IPv6 unique local: `fc00::/7`
- IPv4-mapped IPv6: `::ffff:10.x.x.x`, etc.

HTTP redirects are blocked at delivery time (the redirect target is not visited).

---

## Error types

```rust
use webhooksmith::HooksmithError;

match result {
    Err(HooksmithError::Database(e))           => // sqlx database error
    Err(HooksmithError::Http(e))               => // reqwest HTTP error
    Err(HooksmithError::EndpointNotFound(id))  => // endpoint doesn't exist
    Err(HooksmithError::EventNotFound(id))     => // event doesn't exist
    Err(HooksmithError::InvalidState(id))      => // operation invalid for current state
    Err(HooksmithError::PayloadTooLarge(n, m)) => // payload n bytes exceeds m byte limit
    Err(HooksmithError::Config(msg))           => // validation error (URL, secret, etc.)
    Err(HooksmithError::Signing(msg))          => // HMAC error
}
```

---

## Database schema

`engine.migrate()` creates three tables:

```sql
webhook_endpoints  -- registered delivery targets
webhook_events     -- outbound events, one row per (endpoint, event)
webhook_delivery_attempts  -- log of every HTTP call made
```

All tables use UUIDs as primary keys. Postgres uses `TIMESTAMPTZ`; SQLite stores timestamps as ISO 8601 TEXT.
The `webhook_events` table has a partial index on `(status, scheduled_at)` for efficient worker queries.

---

## How-to examples

```bash
# Postgres examples need Docker:
docker compose up -d

# Minimal setup — connect, register, send, deliver
cargo run --example basic -p webhooksmith

# Transactional outbox — atomic writes, rollback safety
cargo run --example outbox -p webhooksmith

# Full axum integration — engine in State, graceful shutdown
cargo run --example axum_integration -p webhooksmith

# Multi-tenant — per-customer endpoints, broadcast, idempotent sends
cargo run --example multi_tenant -p webhooksmith

# Monitoring — queue stats, DLQ alerts, retry failed events, cleanup
cargo run --example monitoring -p webhooksmith

# Full end-to-end demo with real axum receiver
cargo run --example demo -p webhooksmith
```

**SQLite examples need no setup** — just pass a file path or `sqlite::memory:`.

---

## Requirements

- Rust 1.75+
- **Postgres backend (default):** Postgres 14+ (`gen_random_uuid()`, `FOR UPDATE SKIP LOCKED`, partial unique indexes, triggers)
- **SQLite backend:** SQLite 3.35+ (supports `RETURNING`; WAL mode enabled automatically)

---

## Changelog

### 0.1.10
- `events_global()` — list events across all endpoints for global monitoring dashboards
- `list_endpoints_paged()` — paginated endpoint listing

### 0.1.9
- `broadcast_idempotent()` — fan-out with per-endpoint deduplication
- `send_idempotent_in_tx()` — idempotent send inside a transaction

### 0.1.8
- SQLite backend via `SqliteEngine` — enable with `features = ["sqlite"]`
- Same API as `WebhookEngine`: `send`, `broadcast`, `retry_dead`, `queue_stats`, etc.
- `SqliteEngine::send_in_tx()` for true atomicity on SQLite

### 0.1.7
- Circuit breaker: after 5 consecutive failures an endpoint is backed off automatically
  (5 min → 10 min → 20 min … capped at 320 min, doubling on each failure window)
- `consecutive_failures` and `circuit_open_until` fields on `Endpoint`

### 0.1.6
- Event type filtering with glob patterns (`"order.*"`, `"*"`)
- `register_with(NewEndpoint { event_filter: ... })` — subscribe endpoints to event subsets
- `set_event_filter()` / `clear_event_filter()` — change subscriptions at runtime
- `broadcast()` now routes by subscription automatically

### 0.1.5
- Idempotent sends: `send_idempotent()` and `broadcast_idempotent()`
- Same key for the same endpoint always returns the same event — safe to retry

### 0.1.0
- Initial release: Postgres-backed webhook delivery
- HMAC-SHA256 signing, exponential backoff retry, dead letter queue
- `send()`, `broadcast()`, `send_in_tx()`, `broadcast_in_tx()`
- SSRF protection on endpoint URLs (blocks private/loopback/link-local targets)

---

## License

MIT OR Apache-2.0
