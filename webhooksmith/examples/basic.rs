//! Minimal setup: connect, register one endpoint, send a webhook, run the worker.
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example basic -p webhooksmith

use webhooksmith::WebhookEngine;
use serde_json::json;

const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().without_time().init();

    // 1. Connect and apply migrations (safe to call every startup)
    let engine = WebhookEngine::builder()
        .database_url(DATABASE_URL)
        .build()
        .await?;
    engine.migrate().await?;
    println!("Connected to Postgres.");

    // 2. Register a webhook endpoint
    //    In production: use https:// and a real URL.
    //    Here we use httpbin.org which echoes back what it receives.
    let endpoint = engine
        .register("https://httpbin.org/post", "your-signing-secret-min-16-chars")
        .await?;
    println!("Registered endpoint: {}", endpoint.id);

    // 3. Send a webhook event
    let event = engine
        .send(
            "order.created",
            json!({ "order_id": 1001, "total": 49.99, "currency": "USD" }),
            endpoint.id,
        )
        .await?;
    println!("Queued event: {} (status: {:?})", event.id, event.status);

    // 4. Run one delivery cycle (normally you call engine.run().await instead)
    let delivered = engine.run_once().await?;
    println!("Delivered: {delivered} event(s)");

    // 5. Check the result
    let result = engine.event(event.id).await?;
    if let Some(ev) = result {
        println!("Event status: {:?}", ev.status);
        let log = engine.delivery_log(ev.id).await?;
        for attempt in &log {
            println!(
                "  Attempt {}: {} ({}ms)",
                attempt.attempted_at,
                if attempt.success { "✓" } else { "✗" },
                attempt.duration_ms.unwrap_or(0),
            );
        }
    }

    // Clean up (optional — remove the test endpoint)
    engine.delete_endpoint(endpoint.id).await?;
    println!("Done.");
    Ok(())
}
