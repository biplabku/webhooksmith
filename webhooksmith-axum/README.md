# webhooksmith-axum

Axum integration for [webhooksmith](https://crates.io/crates/webhooksmith).

Provides a tower middleware layer and axum extractors that verify incoming webhooksmith webhook signatures. Handles timestamp replay protection, constant-time HMAC comparison, and body size limiting automatically.

## Usage

```toml
[dependencies]
webhooksmith-axum = "0.1"
axum = "0.7"
```

Add `WebhookSecretLayer` to your router and use `VerifiedWebhook` or `TypedWebhook<T>` in your handlers:

```rust
use axum::{Router, routing::post, http::StatusCode};
use hooksmith_axum::{WebhookSecretLayer, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

// Raw payload — verified, body returned as serde_json::Value
async fn handle(VerifiedWebhook(payload): VerifiedWebhook) -> StatusCode {
    println!("{}: {:?}", payload.event_type, payload.body);
    StatusCode::OK
}

// Typed payload — verified and deserialized to T
#[derive(Deserialize)]
struct OrderCreated { order_id: u64 }

async fn handle_typed(TypedWebhook(order): TypedWebhook<OrderCreated>) -> StatusCode {
    println!("order: {}", order.order_id);
    StatusCode::OK
}

let app: Router = Router::new()
    .route("/webhooks", post(handle))
    .route("/orders", post(handle_typed))
    .layer(WebhookSecretLayer::new("your-signing-secret"));
```

## What the extractor checks

| Condition | HTTP response |
|---|---|
| Missing `x-hooksmith-signature` | 401 |
| Invalid or tampered signature | 401 |
| Timestamp older than 5 minutes | 401 (replay protection) |
| Timestamp in the future by more than 5 minutes | 401 |
| Missing `x-hooksmith-timestamp` | 400 |
| Body over 1 MB | 413 |
| Body is not valid JSON | 422 |

## Accessing event metadata

The `WebhookPayload` struct (returned by `VerifiedWebhook`) includes:

```rust
pub struct WebhookPayload {
    pub event_type: String,       // from x-hooksmith-event-type header
    pub event_id: Option<String>, // from x-hooksmith-event-id header
    pub timestamp: i64,           // Unix timestamp from x-hooksmith-timestamp
    pub body: serde_json::Value,  // verified and parsed body
}
```

## Multiple secrets

If you rotate signing secrets, pass multiple space-separated signatures in the `x-hooksmith-signature` header. The extractor accepts the request if any signature matches.

## License

MIT OR Apache-2.0
