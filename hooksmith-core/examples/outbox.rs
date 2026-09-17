//! Transactional outbox pattern: write business data and a webhook in the same
//! database transaction. If the transaction fails, no webhook is sent.
//! If it succeeds, the webhook is guaranteed to be delivered.
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example outbox -p hooksmith

use hooksmith::WebhookEngine;
use serde_json::json;

const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().without_time().init();

    let engine = WebhookEngine::builder()
        .database_url(DATABASE_URL)
        .build()
        .await?;
    engine.migrate().await?;

    let endpoint = engine
        .register("https://httpbin.org/post", "your-signing-secret-min-16-chars")
        .await?;

    // ── Successful transaction ────────────────────────────────────────────────
    println!("Scenario 1: committed transaction → webhook queued");
    {
        // Begin a transaction that also owns your business data writes
        let mut tx = engine.pool().begin().await?;

        // Your business logic here (e.g. insert into orders table):
        // sqlx::query!("INSERT INTO orders ...").execute(&mut *tx).await?;

        // Write the webhook event in the SAME transaction.
        // It's only visible after commit — no phantom webhooks.
        let event = engine
            .send_in_tx(
                "order.created",
                json!({ "order_id": 1001, "customer": "alice" }),
                endpoint.id,
                &mut tx,
            )
            .await?;
        println!("  Event {} written inside transaction", event.id);

        tx.commit().await?;
        println!("  Transaction committed → event is now queued");

        engine.run_once().await?;
        let ev = engine.event(event.id).await?.unwrap();
        println!("  Event status after delivery: {:?}", ev.status);
    }

    // ── Failed transaction ────────────────────────────────────────────────────
    println!("\nScenario 2: rolled-back transaction → no webhook");
    {
        let mut tx = engine.pool().begin().await?;

        let event = engine
            .send_in_tx(
                "order.created",
                json!({ "order_id": 9999 }),
                endpoint.id,
                &mut tx,
            )
            .await?;
        println!("  Event {} written inside transaction", event.id);

        // Simulate a business logic failure
        tx.rollback().await?;
        println!("  Transaction rolled back");

        let gone = engine.event(event.id).await?;
        assert!(gone.is_none(), "event must not exist after rollback");
        println!("  Event does not exist in DB ✓ (no phantom webhook)");
    }

    // ── Broadcast to all endpoints atomically ─────────────────────────────────
    println!("\nScenario 3: broadcast to all endpoints in one transaction");
    {
        let mut tx = engine.pool().begin().await?;
        let events = engine
            .broadcast_in_tx(
                "system.maintenance",
                json!({ "start": "2026-01-01T00:00:00Z", "duration_mins": 30 }),
                &mut tx,
            )
            .await?;
        println!("  {} events written atomically", events.len());
        tx.commit().await?;
        println!("  All {} endpoints will be notified", events.len());
    }

    engine.delete_endpoint(endpoint.id).await?;
    println!("\nDone.");
    Ok(())
}
