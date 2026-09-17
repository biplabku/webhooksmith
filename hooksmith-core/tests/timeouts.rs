//! HTTP timeout and stuck event timeout configuration tests.
//!
//! Properties verified:
//!   A. http_timeout fires and is recorded as a delivery failure.
//!   B. stuck_timeout controls when the reaper rescues events.
//!   C. Default values are sane (30s HTTP, 120s stuck).
//!   D. Configured values are actually used (not silently ignored).

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{EventStatus, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine_with_timeouts(pool: PgPool, http: Duration, stuck: Duration) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .http_timeout(http)
        .stuck_timeout(stuck)
        .build_sync()
}

async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'timeout_test_secret_32chars') RETURNING id",
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── Property A: http_timeout fires and is recorded as failure ─────────────────
//
// Endpoint delays 500ms. Engine configured with 100ms HTTP timeout.
// Delivery must fail with a timeout error, not hang for 30s.

#[sqlx::test(migrator = "MIGRATOR")]
async fn short_http_timeout_records_failure(pool: PgPool) {
    let server = MockServer::start().await;
    // Respond after 500ms — exceeds our 100ms timeout
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(500)))
        .mount(&server)
        .await;

    let engine = engine_with_timeouts(pool, Duration::from_millis(100), Duration::from_secs(120));
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(
        updated.status,
        EventStatus::Failed,
        "delivery must fail when HTTP timeout fires"
    );

    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(!log[0].success, "attempt must be marked unsuccessful");
    assert!(log[0].error.is_some(), "timeout error must be recorded");
    assert!(
        log[0].duration_ms.unwrap_or(0) < 400,
        "delivery must have stopped before 400ms (timeout fired at 100ms)"
    );
}

// ── Property B: Generous http_timeout allows slow endpoints through ────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn generous_http_timeout_allows_slow_endpoint(pool: PgPool) {
    let server = MockServer::start().await;
    // Respond after 200ms
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(200)))
        .mount(&server)
        .await;

    // 1 second timeout — plenty of room for 200ms response
    let engine = engine_with_timeouts(pool, Duration::from_secs(1), Duration::from_secs(120));
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Delivered, "slow-but-not-timed-out endpoint must succeed");
}

// ── Property C: stuck_timeout controls reaper behaviour ───────────────────────
//
// Event stuck in 'delivering' for 6s with stuck_timeout=5s → reaper rescues.
// Same event with stuck_timeout=10s → reaper does NOT rescue (too recent).

#[sqlx::test(migrator = "MIGRATOR")]
async fn short_stuck_timeout_fires_reaper(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // stuck_timeout=5s — event stuck for 6s should be rescued.
    // http_timeout must be < stuck_timeout (1s < 5s).
    let engine = engine_with_timeouts(pool, Duration::from_secs(1), Duration::from_secs(5));
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    // Simulate event stuck in delivering for 6 seconds
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW()-INTERVAL '6 seconds' WHERE id=$1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    // run_once: reaper fires (6s > 5s threshold), resets to pending, then delivers
    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(
        updated.status,
        EventStatus::Delivered,
        "reaper must have rescued the stuck event and it must be delivered"
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn long_stuck_timeout_does_not_rescue_recently_stuck_event(pool: PgPool) {
    let engine = engine_with_timeouts(pool, Duration::from_secs(30), Duration::from_secs(60));
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0) // must not be delivered
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    // Stuck for only 10 seconds — threshold is 60s, so reaper must not fire
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW()-INTERVAL '10 seconds' WHERE id=$1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    // run_once: reaper does NOT fire (10s < 60s threshold)
    // claim_due_events skips 'delivering' events
    // → 0 events processed
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0, "reaper must not fire when event has been stuck for less than stuck_timeout");

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Delivering, "event must still be in delivering");

    server.verify().await;
}

// ── Property D: Timeout relationship warning ──────────────────────────────────
//
// http_timeout should be < stuck_timeout.
// If http_timeout=120s and stuck_timeout=30s, the reaper would reset events
// that are still legitimately waiting for an HTTP response.
// We don't enforce this at runtime (it's a configuration concern), but document
// it here as a test that demonstrates the expected relationship.

#[sqlx::test(migrator = "MIGRATOR")]
async fn default_timeouts_have_correct_relationship(pool: PgPool) {
    let engine = WebhookEngine::builder()
        .pool(pool)
        .build_sync();

    // Verify defaults: HTTP timeout (30s) < stuck timeout (120s)
    // We can't read these directly from the engine, but we can verify the
    // documented constants from the worker module.
    assert!(
        hooksmith::worker::DEFAULT_HTTP_TIMEOUT < hooksmith::worker::DEFAULT_STUCK_TIMEOUT,
        "HTTP timeout must be less than stuck timeout to avoid premature reaper resets"
    );
    let _ = engine; // used to create pool/engine
}

// ── Fix #4: http_timeout >= stuck_timeout panics ──────────────────────────────

#[test]
#[should_panic(expected = "http_timeout")]
fn http_timeout_equal_to_stuck_timeout_panics() {
    let t = Duration::from_secs(30);
    let _ = WebhookEngine::builder().http_timeout(t).stuck_timeout(t).build_sync();
}

#[test]
#[should_panic(expected = "http_timeout")]
fn http_timeout_greater_than_stuck_timeout_panics() {
    let _ = WebhookEngine::builder()
        .http_timeout(Duration::from_secs(60))
        .stuck_timeout(Duration::from_secs(30))
        .build_sync();
}
