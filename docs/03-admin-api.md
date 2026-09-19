# Admin HTTP API

`webhooksmith-axum` ships a ready-to-use admin router for operational visibility
and DLQ management. Mount it anywhere in your axum app:

```toml
[dependencies]
webhooksmith-axum = "0.1"
```

```rust
use std::sync::Arc;
use axum::Router;
use webhooksmith::WebhookEngine;
use webhooksmith_axum::admin;

let engine: Arc<WebhookEngine> = /* your engine */;

let app = Router::new()
    .nest("/admin", admin(Arc::clone(&engine)))
    .route("/api/v1/...", /* your routes */);
```

## Endpoints

### `GET /admin/stats`

Queue health at a glance.

```json
{
  "pending":   3,
  "delivering": 1,
  "failed":    0,
  "dead":      2,
  "delivered": 1042
}
```

### `GET /admin/endpoints?limit=50&offset=0`

Paginated list of registered endpoints, including circuit breaker state.
Query params: `limit` (1–200, default 50) and `offset` (default 0).

```json
[
  {
    "id": "01234567-...",
    "url": "https://partner.example.com/webhooks",
    "enabled": true,
    "max_attempts": 10,
    "consecutive_failures": 0,
    "circuit_open_until": null,
    "event_filter": ["order.*"],
    ...
  }
]
```

### `GET /admin/dlq/:endpoint_id?limit=50&offset=0`

Dead events for one endpoint, newest first. Paginate with `limit` and `offset`.

```bash
curl /admin/dlq/01234567-...?limit=10&offset=0
```

```json
[
  {
    "id": "...",
    "event_type": "order.created",
    "payload": {"id": 1001},
    "attempts": 10,
    "status": "dead",
    ...
  }
]
```

### `POST /admin/dlq/:endpoint_id/retry-all`

Re-queue ALL dead events for this endpoint and reset its circuit breaker.
Returns the count re-queued.

```bash
curl -X POST /admin/dlq/01234567-.../retry-all
```

```json
{ "retried": 42 }
```

### `GET /admin/metrics`

Prometheus scrape endpoint. Returns standard text exposition format (version 0.0.4).
Point your Prometheus scraper at it — no configuration needed.

```
# HELP webhooksmith_events Current number of webhook events by status.
# TYPE webhooksmith_events gauge
webhooksmith_events{status="pending"} 3
webhooksmith_events{status="delivering"} 1
webhooksmith_events{status="failed"} 0
webhooksmith_events{status="dead"} 2
webhooksmith_events{status="delivered"} 1042

# HELP webhooksmith_endpoints Current number of registered endpoints by state.
# TYPE webhooksmith_endpoints gauge
webhooksmith_endpoints{state="enabled"} 5
webhooksmith_endpoints{state="disabled"} 1
webhooksmith_endpoints{state="circuit_open"} 0
```

## Security

The admin router has no built-in authentication. Mount it behind your existing
auth middleware or restrict it to an internal network interface:

```rust
// Example: mount on a separate internal port
let admin_app = Router::new()
    .nest("/", admin(Arc::clone(&engine)));
// bind to 127.0.0.1:9090 instead of 0.0.0.0:8080

let public_app = Router::new()
    .route("/api/...", /* ... */);
```

Or add a Bearer token check with tower middleware before mounting.
