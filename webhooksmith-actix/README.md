# webhooksmith-actix

[![crates.io](https://img.shields.io/crates/v/webhooksmith-actix.svg)](https://crates.io/crates/webhooksmith-actix)

Actix-web integration for [webhooksmith](https://crates.io/crates/webhooksmith).

Provides `VerifiedWebhook` and `TypedWebhook<T>` extractors that verify incoming webhook
HMAC-SHA256 signatures and reject forged, stale, or oversized requests.

## Usage

```toml
[dependencies]
webhooksmith-actix = "0.1"
actix-web = "4"
serde = { version = "1", features = ["derive"] }
```

```rust
use actix_web::{web, App, HttpServer, HttpResponse, Responder};
use webhooksmith_actix::{WebhookSecret, VerifiedWebhook, TypedWebhook};
use serde::Deserialize;

#[derive(Deserialize)]
struct OrderCreated { order_id: u64 }

async fn handle_raw(webhook: VerifiedWebhook) -> impl Responder {
    println!("event: {}, id: {}", webhook.event_type, webhook.event_id);
    HttpResponse::Ok().finish()
}

async fn handle_typed(webhook: TypedWebhook<OrderCreated>) -> impl Responder {
    println!("order: {}", webhook.payload.order_id);
    HttpResponse::Ok().finish()
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .app_data(WebhookSecret::new("your-signing-secret"))
            .route("/webhooks", web::post().to(handle_raw))
            .route("/orders", web::post().to(handle_typed))
    })
    .bind("0.0.0.0:8080")?
    .run()
    .await
}
```

## Rejection behaviour

| Condition | Status |
|-----------|--------|
| Missing `x-hooksmith-signature` | 401 |
| Missing `x-hooksmith-timestamp` | 400 |
| Invalid timestamp (non-integer) | 400 |
| Stale timestamp (> 300 s skew) | 401 |
| Signature mismatch | 401 |
| Body > 1 MB | 413 |
| Body is not valid JSON | 422 |

All error responses are JSON: `{"error": "..."}`.

## License

MIT OR Apache-2.0
