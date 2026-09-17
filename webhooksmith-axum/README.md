# webhooksmith-axum

Axum integration for [webhooksmith](https://crates.io/crates/webhooksmith).

Verifies incoming webhooksmith HMAC-SHA256 signatures in an axum handler with one line.
Handles replay protection, constant-time comparison, and body size limiting automatically.

```toml
[dependencies]
webhooksmith-axum = "0.1"
axum = "0.7"
tokio = { version = "1", features = ["full"] }
```

---

## Setup

Add `WebhookSecretLayer` to your router, then use `VerifiedWebhook` or `TypedWebhook<T>`
in any route handler:

```rust
use axum::{Router, routing::post, http::StatusCode};
use webhooksmith_axum::{WebhookSecretLayer, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

// Raw payload — verified, body returned as serde_json::Value
async fn handle_raw(VerifiedWebhook(payload): VerifiedWebhook) -> StatusCode {
    println!("event: {} id: {:?}", payload.event_type, payload.event_id);
    println!("body: {}", payload.body);
    StatusCode::OK
}

// Typed payload — verified and deserialized to your struct
#[derive(Deserialize)]
struct OrderCreated {
    id: u64,
    total: f64,
}

async fn handle_typed(TypedWebhook(order): TypedWebhook<OrderCreated>) -> StatusCode {
    println!("order {} for ${:.2}", order.id, order.total);
    StatusCode::OK
}

#[tokio::main]
async fn main() {
    let app: Router = Router::new()
        .route("/webhooks", post(handle_raw))
        .route("/orders", post(handle_typed))
        .layer(WebhookSecretLayer::new("your-signing-secret"));  // same secret as the sender

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

---

## WebhookPayload fields

`VerifiedWebhook` gives you a `WebhookPayload`:

```rust
pub struct WebhookPayload {
    pub event_type: String,        // from x-hooksmith-event-type header
    pub event_id: Option<String>,  // from x-hooksmith-event-id header (the event UUID)
    pub timestamp: i64,            // Unix timestamp from x-hooksmith-timestamp
    pub body: serde_json::Value,   // verified and parsed JSON body
}
```

Example usage:

```rust
async fn handle(VerifiedWebhook(p): VerifiedWebhook) -> StatusCode {
    match p.event_type.as_str() {
        "order.created"   => handle_order_created(p.body),
        "payment.captured" => handle_payment(p.body),
        _ => {}
    }
    StatusCode::OK
}
```

---

## What gets rejected

| Condition | HTTP status |
|---|---|
| Missing `x-hooksmith-signature` header | 401 Unauthorized |
| Signature does not match | 401 Unauthorized |
| Timestamp older than 5 minutes (replay) | 401 Unauthorized |
| Timestamp more than 5 minutes in the future | 401 Unauthorized |
| Missing `x-hooksmith-timestamp` header | 400 Bad Request |
| Body over 1 MB | 413 Payload Too Large |
| Body is not valid JSON | 422 Unprocessable Entity |
| `WebhookSecretLayer` not on the router | 500 Internal Server Error |

---

## Multiple secrets (secret rotation)

The sender can include multiple space-separated signatures in the header.
The extractor accepts the request if **any** signature matches.

This is useful during secret rotation: old consumers receive the old signature,
new consumers receive both. The extractor accepts either.

---

## Verifying without axum

If you're not using axum, verify signatures directly:

```rust
use webhooksmith::signing;

fn is_valid(secret: &str, request_headers: &Headers, body: &[u8]) -> bool {
    let timestamp: i64 = request_headers
        .get("x-hooksmith-timestamp")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let signature = request_headers
        .get("x-hooksmith-signature")
        .unwrap_or("");

    // Returns false if: wrong secret, timestamp > 5 min old/future, or tampered body
    signing::verify(secret, timestamp, body, signature)
}
```

---

## License

MIT OR Apache-2.0
