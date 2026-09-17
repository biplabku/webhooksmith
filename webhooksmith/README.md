# webhooksmith

Webhook delivery engine for Rust applications backed by Postgres.

webhooksmith stores outgoing webhooks in your existing Postgres database and delivers them via a background worker. No additional infrastructure is required beyond Postgres.

## When to use this

- You need to send webhooks to external endpoints from your application
- You want delivery guarantees — events are persisted before delivery is attempted
- You already run Postgres and do not want to add another service
- You need the transactional outbox pattern: your business data and the webhook event written in the same database transaction, so neither exists without the other

## Quick start

```toml
[dependencies]
webhooksmith = "0.1"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

```rust
use webhooksmith::WebhookEngine;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = WebhookEngine::builder()
        .database_url("postgres://user:pass@localhost/mydb")
        .build()
        .await?;

    engine.migrate().await?; // creates the required tables

    let endpoint = engine
        .register("https://partner.example.com/webhooks", "your-secret-32-chars-min")
        .await?;

    // Send to one endpoint
    engine.send("order.created", json!({"id": 1}), endpoint.id).await?;

    // Send to all enabled endpoints at once
    engine.broadcast("order.created", json!({"id": 1})).await?;

    // Idempotent send — duplicate calls return the existing event
    engine
        .send_idempotent("order.created", json!({"id": 1}), endpoint.id, "order-1001")
        .await?;

    // Start the delivery worker — blocks until the process exits
    engine.run().await;
}
```

## Transactional outbox

The outbox pattern ensures your business data and the webhook event are written atomically. If the process crashes after the database write but before delivery, the event is not lost — the worker picks it up on the next restart.

```rust
let mut tx = engine.pool().begin().await?;

// Your business logic and the webhook in the same transaction
sqlx::query!("INSERT INTO orders (id, total) VALUES ($1, $2)", order_id, total)
    .execute(&mut *tx)
    .await?;

engine
    .send_in_tx("order.created", json!({"id": order_id}), endpoint.id, &mut tx)
    .await?;

tx.commit().await?;
// Webhook is queued only if this commit succeeds
```

`broadcast_in_tx` works the same way for fan-out to all endpoints.

## Graceful shutdown

The in-flight delivery batch completes before the worker exits. No new batch is claimed after the shutdown signal fires. Events not yet claimed stay in the database and are picked up on next startup.

```rust
// Shut down on Ctrl-C or SIGTERM:
engine.run_graceful(async {
    tokio::signal::ctrl_c().await.ok();
}).await;
```

## Idempotency keys

If your code retries on network errors, the same event can be enqueued twice. Idempotency keys prevent this.

```rust
// Safe to call multiple times — only one event is created
engine
    .send_idempotent("order.created", payload, endpoint.id, "order-1001-created")
    .await?;
```

The key is scoped per endpoint, so the same key can be used independently across endpoints (useful with `broadcast_idempotent`).

## Receiving webhooks

`webhooksmith-axum` provides a tower middleware and axum extractors for verifying incoming webhook signatures. See [webhooksmith-axum](../webhooksmith-axum/README.md).

## Running the demo

```bash
docker compose up -d
cargo run --example demo -p webhooksmith
```

The demo starts a real axum receiver, sends three events including one via the transactional outbox, simulates a failure and retry, and prints the full delivery log.

## Features

- **Postgres-backed persistence** — events survive process restarts
- **Transactional outbox** — `send_in_tx` / `broadcast_in_tx` write atomically with your data
- **Fan-out** — `broadcast` delivers one event per enabled endpoint in a single SQL statement
- **Idempotency keys** — `send_idempotent` / `broadcast_idempotent` deduplicate per (endpoint, key)
- **Exponential backoff with full jitter** — automatic retry on failure
- **Dead-letter queue** — events that exhaust retries move to DLQ; `retry_dead` requeues them
- **Graceful shutdown** — `run_graceful(signal)` drains the current batch before stopping
- **Worker crash recovery** — `delivering_since` timestamp lets the reaper reset stuck events
- **HMAC-SHA256 signing** — Svix-compatible `v1,<hex>` signature format
- **SSRF protection** — private IPs, loopback, and link-local addresses blocked at registration
- **Redirect protection** — HTTP redirects are not followed during delivery
- **Response body limit** — streamed at most 4 KB regardless of response size
- **Multi-worker safe** — `SELECT FOR UPDATE SKIP LOCKED` prevents double-processing

## Configuration

```rust
let engine = WebhookEngine::builder()
    .database_url("postgres://...")        // or .pool(existing_pool)
    .batch_size(100)                       // events per worker cycle (default: 50)
    .poll_interval(Duration::from_secs(1)) // idle sleep between cycles (default: 500ms)
    .build()
    .await?;
```

Endpoint options:

```rust
engine.register_with(webhooksmith::NewEndpoint {
    url: "https://partner.example.com/webhooks".into(),
    signing_secret: "your-secret-min-16-chars".into(),
    description: Some("Partner A".into()),
    max_attempts: Some(10),       // retries before moving to DLQ (default: 10)
    initial_delay_ms: Some(1000), // first retry delay in ms (default: 1000)
}).await?;
```

## Requirements

- Rust 1.75+
- Postgres 14+ (uses `gen_random_uuid()`, `FOR UPDATE SKIP LOCKED`, partial unique indexes)

## License

MIT OR Apache-2.0
