# Event Type Filtering

Endpoints can subscribe to a subset of event types. `broadcast()` routes
automatically — only endpoints whose filter matches receive the event.
`send()` to a specific endpoint always delivers regardless of filter.

## Patterns

| Pattern | Matches |
|---------|---------|
| `"order.created"` | Exactly `order.created` |
| `"order.*"` | `order.created`, `order.updated`, `order`, etc. |
| `"*"` | Everything |
| `None` (default) | Everything |
| `Some(vec![])` | Nothing (receives no broadcast events) |

## Setting a filter at registration

```rust
use webhooksmith::{WebhookEngine, NewEndpoint};

engine.register_with(NewEndpoint {
    url: "https://partner.example.com/webhooks".into(),
    signing_secret: "your-secret".into(),
    event_filter: Some(vec!["order.*".into(), "payment.captured".into()]),
    ..Default::default()
}).await?;
```

## Updating a filter at runtime

```rust
// Subscribe to order events only
engine.set_event_filter(endpoint_id, vec!["order.*".into()]).await?;

// Back to receiving everything
engine.clear_event_filter(endpoint_id).await?;
```

## Routing example

```rust
// Three endpoints
let orders_ep  = engine.register_with(NewEndpoint {
    event_filter: Some(vec!["order.*".into()]),
    ..
}).await?;
let payment_ep = engine.register_with(NewEndpoint {
    event_filter: Some(vec!["payment.captured".into()]),
    ..
}).await?;
let firehose   = engine.register_with(NewEndpoint {
    event_filter: None, // receives all
    ..
}).await?;

// broadcast routes automatically:
engine.broadcast("order.created", json!({})).await?;
// → orders_ep + firehose (NOT payment_ep)

engine.broadcast("payment.captured", json!({})).await?;
// → payment_ep + firehose (NOT orders_ep)
```

## Validation rules

Invalid patterns are rejected at registration/update time:

- Empty string `""` → error
- Trailing dot without wildcard `"order."` → use `"order.*"` instead
- Control characters → error

Valid: `"order.created"`, `"order.*"`, `"*"`, `"payment.captured"`.
