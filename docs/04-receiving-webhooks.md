# Receiving Webhooks

Webhooksmith ships two receiver integrations for verifying the HMAC-SHA256
signatures on incoming webhooks: one for **axum** and one for **actix-web**.

---

## axum

```toml
[dependencies]
webhooksmith-axum = "0.1"
axum = "0.7"
```

Apply `WebhookSecretLayer` to your router and use `VerifiedWebhook` or
`TypedWebhook<T>` as handler parameters:

```rust
use axum::{Router, routing::post, http::StatusCode};
use webhooksmith_axum::{WebhookSecretLayer, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderCreated { order_id: u64 }

// Raw — access headers and body
async fn handle_raw(VerifiedWebhook(wh): VerifiedWebhook) -> StatusCode {
    tracing::info!(event_type = %wh.event_type, "received");
    StatusCode::OK
}

// Typed — automatic JSON deserialization
async fn handle_typed(TypedWebhook(order): TypedWebhook<OrderCreated>) -> StatusCode {
    tracing::info!(order_id = order.order_id, "order");
    StatusCode::OK
}

let app = Router::new()
    .route("/webhooks", post(handle_raw))
    .route("/orders", post(handle_typed))
    .layer(WebhookSecretLayer::new("your-signing-secret"));
```

---

## actix-web

```toml
[dependencies]
webhooksmith-actix = "0.1"
actix-web = "4"
```

Register `WebhookSecret` as app data. Then use `VerifiedWebhook` or
`TypedWebhook<T>` as handler parameters:

```rust
use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use webhooksmith_actix::{WebhookSecret, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderCreated { order_id: u64 }

async fn handle_raw(webhook: VerifiedWebhook) -> impl Responder {
    tracing::info!(event_type = %webhook.event_type, "received");
    HttpResponse::Ok().finish()
}

async fn handle_typed(webhook: TypedWebhook<OrderCreated>) -> impl Responder {
    println!("order: {}", webhook.payload.order_id);
    HttpResponse::Ok().finish()
}

HttpServer::new(|| {
    App::new()
        .app_data(WebhookSecret::new("your-signing-secret"))
        .route("/webhooks", web::post().to(handle_raw))
        .route("/orders", web::post().to(handle_typed))
})
.bind("0.0.0.0:8080")?
.run()
.await
```

---

## Rejection responses

Both integrations reject invalid requests with structured JSON errors:

| Condition | Status | `error` field |
|-----------|--------|---------------|
| Missing signature header | 401 | `"missing signature"` |
| Missing timestamp header | 400 | `"missing timestamp"` |
| Timestamp is not an integer | 400 | `"invalid timestamp"` |
| Timestamp skew > 300 s | 401 | `"stale timestamp"` |
| Signature mismatch | 401 | `"invalid signature"` |
| Body > 1 MB | 413 | `"payload too large"` |
| Body is not JSON | 422 | `"body must be JSON"` |

Example rejection:
```json
HTTP 401 Unauthorized
{"error": "invalid signature"}
```

---

## WebhookPayload fields

Both integrations expose the same metadata:

| Field | Description |
|-------|-------------|
| `event_type` | Value of `x-hooksmith-event-type` header |
| `event_id` | `Option<String>` — `None` if header absent (axum) / `Option<String>` (actix) |
| `timestamp` | Unix timestamp from `x-hooksmith-timestamp` |
| `body` | `serde_json::Value` — parsed JSON body (axum) / `Bytes` — raw bytes (actix) |
