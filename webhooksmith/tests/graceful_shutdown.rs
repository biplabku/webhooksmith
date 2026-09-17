//! Graceful shutdown tests.
//!
//! Properties being verified:
//!   A. In-flight batch completes — events in the current cycle are delivered.
//!   B. No new cycle starts after shutdown signal — remaining events stay pending.
//!   C. Empty worker stops immediately when shutdown is pre-signaled.
//!   D. Delayed signal: worker processes batches until signal arrives, then stops.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine_with_batch(pool: PgPool, batch_size: i64) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(batch_size)
        .build_sync()
}

async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'graceful_shutdown_test_secret') RETURNING id",
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

async fn count_by_status(engine: &WebhookEngine, status: &str) -> i64 {
    sqlx::query_scalar!(
        "SELECT COUNT(*) FROM webhook_events WHERE status = $1",
        status
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
    .unwrap_or(0)
}

// ── Property A: In-flight batch completes before shutdown ─────────────────────
//
// Events in the current run_once() call must be fully delivered
// even if the shutdown signal was sent before run_graceful was called.

#[sqlx::test(migrator = "MIGRATOR")]
async fn current_batch_completes_before_shutdown(pool: PgPool) {
    let server = MockServer::start().await;
    // 50ms delay per request to ensure we're mid-delivery when checking shutdown
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(50)))
        .mount(&server)
        .await;

    let engine = engine_with_batch(pool, 50);
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..3 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // Shutdown already signaled — run_graceful should still complete the first batch
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tx.send(()).unwrap(); // pre-signal

    engine.run_graceful(async { rx.await.ok(); }).await;

    // All 3 must be delivered — the batch was claimed before shutdown was checked
    assert_eq!(count_by_status(&engine, "delivered").await, 3,
        "pre-signaled shutdown must not interrupt the in-flight batch");
    assert_eq!(count_by_status(&engine, "pending").await, 0);
}

// ── Property B: No new cycle starts after shutdown signal ─────────────────────
//
// With batch_size=2 and 4 events, only the first batch (2 events) is delivered.
// The second batch is not claimed — those events stay pending.

#[sqlx::test(migrator = "MIGRATOR")]
async fn no_new_cycle_starts_after_shutdown(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine_with_batch(pool, 2); // small batch so we can count
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..4 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // Pre-signal shutdown
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tx.send(()).unwrap();

    engine.run_graceful(async { rx.await.ok(); }).await;

    let delivered = count_by_status(&engine, "delivered").await;
    let pending = count_by_status(&engine, "pending").await;

    // First batch (2 events) delivered, second batch (2 events) left pending
    assert_eq!(delivered, 2, "only first batch must be delivered");
    assert_eq!(pending, 2, "second batch must remain pending for next startup");
}

// ── Property C: Empty queue — stops immediately on pre-signaled shutdown ───────

#[sqlx::test(migrator = "MIGRATOR")]
async fn stops_immediately_when_queue_empty_and_shutdown_signaled(pool: PgPool) {
    let engine = engine_with_batch(pool, 50);

    // No events enqueued
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tx.send(()).unwrap();

    // Should return quickly — no events, shutdown already signaled
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        engine.run_graceful(async { rx.await.ok(); }),
    ).await;

    assert!(result.is_ok(), "run_graceful must return promptly when queue is empty and shutdown is signaled");
}

// ── Property D: Delayed signal — worker runs until signal arrives ──────────────
//
// Enqueue 2 events, send shutdown 200ms into run_graceful.
// Worker delivers the first batch, then exits when signal arrives during sleep.

#[sqlx::test(migrator = "MIGRATOR")]
async fn worker_runs_until_signal_then_stops(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine_with_batch(pool, 50);
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..2 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // Send shutdown 200ms after starting — after the first cycle completes
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(()).ok();
    });

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        engine.run_graceful(async { rx.await.ok(); }),
    ).await;

    assert!(result.is_ok(), "run_graceful must return after shutdown signal");
    assert_eq!(count_by_status(&engine, "delivered").await, 2);
}

// ── Property E: Shutdown signal after a failure cycle ─────────────────────────
//
// If the worker hits a batch of failed deliveries and shutdown is signaled,
// it must still return cleanly — not hang.

#[sqlx::test(migrator = "MIGRATOR")]
async fn shutdown_after_failed_deliveries(pool: PgPool) {
    let engine = engine_with_batch(pool, 50);

    // Dead endpoint (nothing listening)
    sqlx::query!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ('http://127.0.0.1:19997/dead', 'graceful_shutdown_test_secret')"
    )
    .execute(engine.pool())
    .await
    .unwrap();

    let endpoint_id: uuid::Uuid = sqlx::query_scalar!(
        "SELECT id FROM webhook_endpoints WHERE url = 'http://127.0.0.1:19997/dead'"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tx.send(()).unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        engine.run_graceful(async { rx.await.ok(); }),
    ).await;

    assert!(result.is_ok(), "run_graceful must return cleanly even after delivery failures");

    // Event should be in 'failed' state (attempted once, scheduled for retry)
    assert_eq!(count_by_status(&engine, "failed").await, 1);
}
