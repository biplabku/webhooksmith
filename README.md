# webhooksmith

[![crates.io](https://img.shields.io/crates/v/webhooksmith.svg)](https://crates.io/crates/webhooksmith)
[![docs.rs](https://docs.rs/webhooksmith/badge.svg)](https://docs.rs/webhooksmith)
[![MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

Webhook delivery for Rust backed by Postgres. Atomic outbox writes, HMAC-SHA256 signing,
automatic retry with exponential backoff, and dead letter queue. No external services.

---

## Crates

| Crate | Description |
|---|---|
| [`webhooksmith`](webhooksmith/) | Core engine — sending, delivery worker, DLQ, monitoring |
| [`webhooksmith-axum`](webhooksmith-axum/) | Axum extractor for verifying incoming webhooks |

---

## How it works

1. Register partner webhook endpoints in Postgres.
2. Call `engine.send()` — the event is persisted atomically (transactional outbox).
3. The background worker picks it up and POSTs it with an HMAC-SHA256 signature.
4. On failure: exponential backoff → dead letter queue → manual retry.

No Redis. No queuing service. Just your existing Postgres.

---

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

    engine.migrate().await?;  // creates 3 tables, safe on every startup

    let endpoint = engine
        .register("https://partner.example.com/webhooks", "your-signing-secret-32chars")
        .await?;

    // Send a webhook
    engine.send("order.created", json!({"id": 1001}), endpoint.id).await?;

    // Start background delivery worker
    engine.run().await;
}
```

---

## Transactional outbox

Write business data and webhook event in the same transaction — neither exists without the other:

```rust
let mut tx = engine.pool().begin().await?;

sqlx::query!("INSERT INTO orders (id) VALUES ($1)", order_id)
    .execute(&mut *tx).await?;

engine.send_in_tx("order.created", json!({"id": order_id}), endpoint.id, &mut tx).await?;

tx.commit().await?;  // webhook only queued if this succeeds
```

---

## All APIs at a glance

**Sending:**
`send` · `send_in_tx` · `send_idempotent` · `send_idempotent_in_tx`
`broadcast` · `broadcast_in_tx` · `broadcast_idempotent`

**Endpoints:**
`register` · `register_with` · `endpoint` · `list_endpoints` · `list_endpoints_paged`
`update_endpoint` · `enable_endpoint` · `disable_endpoint` · `delete_endpoint`

**Worker:**
`run` · `run_graceful` · `run_once`

**Events & monitoring:**
`event` · `events_by_status` · `events_global` · `queue_stats` · `delivery_log`

**DLQ:**
`dead_events` · `dead_events_paged` · `retry_dead` · `retry_all_dead`

**Cleanup & ops:**
`cleanup_delivered` · `cleanup_dead` · `recover_stuck_deliveries`

→ [Full API reference in webhooksmith/README.md](webhooksmith/README.md)

---

## Receiving webhooks

Add `webhooksmith-axum` to verify incoming signatures in an axum handler:

```toml
[dependencies]
webhooksmith-axum = "0.1"
```

```rust
use axum::{Router, routing::post, http::StatusCode};
use webhooksmith_axum::{WebhookSecretLayer, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderCreated { id: u64 }

async fn handle(TypedWebhook(order): TypedWebhook<OrderCreated>) -> StatusCode {
    println!("order: {}", order.id);
    StatusCode::OK
}

let app: Router = Router::new()
    .route("/webhooks", post(handle))
    .layer(WebhookSecretLayer::new("your-signing-secret"));
```

→ [webhooksmith-axum README](webhooksmith-axum/README.md)

---

## Running locally

```bash
docker compose up -d
cargo run --example demo -p webhooksmith    # full end-to-end demo
cargo run --example basic -p webhooksmith   # minimal setup
cargo run --example outbox -p webhooksmith  # transactional outbox patterns
```

---

## Running the tests

```bash
docker compose up -d
DATABASE_URL=postgres://webhooksmith:webhooksmith@localhost:5432/webhooksmith cargo test
```

159 tests covering unit, integration (real Postgres + real HTTP), edge cases, adversarial, stress, and bombardment scenarios.

---

## Requirements

- Rust 1.75+
- Postgres 14+

## License

MIT OR Apache-2.0
