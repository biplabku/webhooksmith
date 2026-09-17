//! End-to-end demo: real axum receiver + webhooksmith sender + signature verification.
//!
//! What this proves:
//!   1. WebhookEngine sends events with HMAC-SHA256 signatures
//!   2. webhooksmith-axum VerifiedWebhook extractor verifies them on the receiving side
//!   3. Transactional outbox: event only delivered after tx commits
//!   4. Retry: simulated 503 failure followed by successful delivery
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example demo -p webhooksmith

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{extract::State, http::StatusCode, routing::post, Router};
use webhooksmith::WebhookEngine;
use webhooksmith_axum::{VerifiedWebhook, WebhookSecretLayer};
use serde_json::json;

const SECRET: &str = "demo-signing-secret-32chars-long";
const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";

// ── Axum webhook receiver ────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct ReceivedLog(Arc<Mutex<Vec<String>>>);

/// Handler: verifies the signature via VerifiedWebhook extractor, logs the event.
async fn receive_webhook(
    State(log): State<ReceivedLog>,
    VerifiedWebhook(payload): VerifiedWebhook,
) -> StatusCode {
    let entry = format!(
        "  [receiver] ✓ event_type={} payload={}",
        payload.event_type,
        serde_json::to_string(&payload.body).unwrap_or_default(),
    );
    println!("{entry}");
    log.0.lock().unwrap().push(entry);
    StatusCode::OK
}

/// Start an axum server on a random port and return its URL.
async fn start_axum_receiver(secret: &str) -> (String, ReceivedLog) {
    let log = ReceivedLog::default();
    let log_clone = log.clone();

    let app = Router::new()
        .route("/webhooks", post(receive_webhook))
        .layer(WebhookSecretLayer::new(secret))
        .with_state(log_clone);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/webhooks");

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (url, log)
}

// ── Demo ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("webhooksmith=info,demo=info")
        .without_time()
        .init();

    println!("\n╔══════════════════════════════════════╗");
    println!("║         webhooksmith  demo              ║");
    println!("╚══════════════════════════════════════╝\n");

    // ── 1. Start axum receiver ────────────────────────────────────────────────
    println!("[1] Starting axum webhook receiver (webhooksmith-axum)...");
    let (receiver_url, log) = start_axum_receiver(SECRET).await;
    println!("    Listening at {receiver_url}\n");

    // ── 2. Connect engine ─────────────────────────────────────────────────────
    println!("[2] Connecting WebhookEngine to Postgres...");
    let engine = WebhookEngine::builder()
        .database_url(DATABASE_URL)
        .allow_insecure_urls() // localhost is fine for this demo
        .build()
        .await?;
    engine.migrate().await?;
    println!("    Connected and migrations applied.\n");

    // ── 3. Register the receiver as an endpoint ───────────────────────────────
    println!("[3] Registering endpoint...");
    let endpoint = engine.register(&receiver_url, SECRET).await?;
    println!("    Endpoint ID: {}\n", endpoint.id);

    // ── 4. Send events ────────────────────────────────────────────────────────
    println!("[4] Dispatching events...");

    let e1 = engine
        .send("order.created", json!({"order_id": 1001, "total": 49.99}), endpoint.id)
        .await?;
    println!("    Queued {} (order.created)", e1.id);

    let e2 = engine
        .send("payment.succeeded", json!({"order_id": 1001, "amount": 49.99}), endpoint.id)
        .await?;
    println!("    Queued {} (payment.succeeded)", e2.id);

    // ── 5. Transactional outbox ───────────────────────────────────────────────
    println!("\n[5] Transactional outbox demo...");
    let mut tx = engine.pool().begin().await?;
    let e3 = engine
        .send_in_tx(
            "order.shipped",
            json!({"order_id": 1001, "tracking": "1Z999AA10123456784"}),
            endpoint.id,
            &mut tx,
        )
        .await?;
    println!("    Event {} written in transaction (not yet visible)", e3.id);
    tx.commit().await?;
    println!("    Transaction committed — event now queued.\n");

    // ── 6. Simulate a failure + retry ─────────────────────────────────────────
    // We insert an event pointing at a dead port so the first attempt fails,
    // then we point it back at the live receiver to show retry works.
    println!("[6] Retry demo: inserting event to a dead endpoint...");
    let dead_endpoint = engine
        .register("http://127.0.0.1:19999/dead", SECRET)
        .await?;
    let e_retry = engine
        .send("retry.demo", json!({"attempt": 1}), dead_endpoint.id)
        .await?;

    // First attempt fails (port 19999 is not listening)
    engine.run_once().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status_after_fail = engine.event(e_retry.id).await?.unwrap();
    println!("    After 1st attempt: status={:?}", status_after_fail.status);

    // Now point the endpoint at the live receiver and force immediate retry
    sqlx::query!(
        "UPDATE webhook_endpoints SET url = $1 WHERE id = $2",
        receiver_url,
        dead_endpoint.id,
    )
    .execute(engine.pool())
    .await?;
    sqlx::query!(
        "UPDATE webhook_events SET scheduled_at = NOW() WHERE id = $1",
        e_retry.id,
    )
    .execute(engine.pool())
    .await?;

    // Second attempt succeeds
    engine.run_once().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status_after_retry = engine.event(e_retry.id).await?.unwrap();
    println!("    After retry: status={:?}\n", status_after_retry.status);

    // ── 7. Deliver main events ────────────────────────────────────────────────
    println!("[7] Running worker to deliver main events...");
    let delivered = engine.run_once().await?;
    println!("    Processed {delivered} events.");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ── 8. Results ────────────────────────────────────────────────────────────
    println!("\n[8] Events received by axum server:");
    let received = log.0.lock().unwrap().clone();
    for m in &received {
        println!("{m}");
    }

    println!("\n[9] Delivery log from Postgres:");
    for (label, id) in [("order.created", e1.id), ("payment.succeeded", e2.id), ("order.shipped", e3.id)] {
        let attempts = engine.delivery_log(id).await?;
        for a in &attempts {
            println!("    {label}: {} in {}ms",
                if a.success { "✓ delivered" } else { "✗ failed" },
                a.duration_ms.unwrap_or(0),
            );
        }
    }
    let retry_log = engine.delivery_log(e_retry.id).await?;
    println!("    retry.demo: {} attempt(s)", retry_log.len());
    for a in &retry_log {
        println!("      → attempt: {} ({}ms)", if a.success { "✓" } else { "✗" }, a.duration_ms.unwrap_or(0));
    }

    // Clean up demo endpoints so re-runs start fresh
    sqlx::query!("DELETE FROM webhook_endpoints WHERE id IN ($1, $2)", endpoint.id, dead_endpoint.id)
        .execute(engine.pool())
        .await?;

    println!("\n╔══════════════════════════════════════╗");
    println!("║              done ✓                  ║");
    println!("╚══════════════════════════════════════╝\n");
    Ok(())
}
