# Transactional Outbox Pattern

Write your business data and the webhook event in a single database transaction.
Either both commit or neither does — no phantom events, no silent drops.

## Postgres (full atomic outbox)

```rust
use webhooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;

async fn create_order(engine: &WebhookEngine, pool: &PgPool, order_id: i64, endpoint_id: uuid::Uuid)
    -> Result<(), Box<dyn std::error::Error>>
{
    let mut tx = pool.begin().await?;

    // Business write
    sqlx::query!("INSERT INTO orders (id, status) VALUES ($1, 'pending')", order_id)
        .execute(&mut *tx)
        .await?;

    // Webhook event — uses the SAME transaction connection
    engine.send_in_tx(
        "order.created",
        json!({"id": order_id}),
        endpoint_id,
        &mut tx,
    ).await?;

    tx.commit().await?; // Both commit together — or both roll back
    Ok(())
}
```

If `tx.commit()` fails, the order row AND the webhook event are both rolled back.
The worker will never see the phantom event.

## SQLite (true atomic outbox)

`SqliteEngine::send_in_tx` also provides true transactional semantics.
SQLite's single-writer model means the transaction connection owns the DB
exclusively — `ROLLBACK` removes the event:

```rust
use webhooksmith::SqliteEngine;

async fn create_order(engine: &SqliteEngine, order_id: i64, endpoint_id: uuid::Uuid)
    -> Result<(), Box<dyn std::error::Error>>
{
    let mut tx = engine.pool().begin().await?;

    sqlx::query("INSERT INTO orders (id) VALUES (?)")
        .bind(order_id)
        .execute(&mut *tx)
        .await?;

    engine.send_in_tx("order.created", json!({"id": order_id}), endpoint_id, &mut tx).await?;

    tx.commit().await?;
    Ok(())
}
```

## Without a transaction

If you don't need atomicity with a business write, use `send()` directly:

```rust
engine.send("order.created", json!({"id": order_id}), endpoint_id).await?;
```

The event is persisted immediately and picked up by the worker on the next tick.
