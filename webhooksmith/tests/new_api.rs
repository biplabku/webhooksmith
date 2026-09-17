//! Tests for methods added during the analysis pass:
//! endpoint(id), enable/disable_endpoint, cleanup_dead.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

async fn live_ep(engine: &WebhookEngine, server: &MockServer) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'new_api_test_secret_32chars_ok') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── endpoint(id) ──────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn endpoint_by_id_returns_correct_endpoint(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let registered = engine
        .register(&format!("{}/hook", server.uri()), "new_api_test_secret_32chars_ok")
        .await
        .unwrap();

    let fetched = engine.endpoint(registered.id).await.unwrap().unwrap();
    assert_eq!(fetched.id, registered.id);
    assert_eq!(fetched.url, registered.url);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn endpoint_by_id_returns_none_for_missing(pool: PgPool) {
    let engine = engine(pool);
    let result = engine.endpoint(uuid::Uuid::new_v4()).await.unwrap();
    assert!(result.is_none());
}

// ── disable_endpoint / enable_endpoint ────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn disable_stops_delivery_enable_resumes(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep = live_ep(&engine, &server).await;

    // Disable — worker should not claim events
    engine.disable_endpoint(ep).await.unwrap();
    engine.send("test", json!({}), ep).await.unwrap();
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0, "disabled endpoint must not be claimed");

    // Re-enable — worker delivers
    engine.enable_endpoint(ep).await.unwrap();
    let n2 = engine.run_once().await.unwrap();
    assert_eq!(n2, 1, "re-enabled endpoint must be claimed and delivered");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn disable_endpoint_updates_enabled_flag(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    let before = engine.endpoint(ep).await.unwrap().unwrap();
    assert!(before.enabled, "endpoint must be enabled initially");

    engine.disable_endpoint(ep).await.unwrap();
    let after = engine.endpoint(ep).await.unwrap().unwrap();
    assert!(!after.enabled, "endpoint must be disabled");

    engine.enable_endpoint(ep).await.unwrap();
    let re_enabled = engine.endpoint(ep).await.unwrap().unwrap();
    assert!(re_enabled.enabled, "endpoint must be enabled again");
}

// ── cleanup_dead ──────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_dead_removes_old_dlq_events(pool: PgPool) {
    let engine = engine(pool);
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19990/dead', 'new_api_test_secret_32chars_ok', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..5 {
        engine.send("test", json!({"i": i}), dead_ep).await.unwrap();
    }
    engine.run_once().await.unwrap(); // all → dead

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.dead, 5);

    // Backdate so they appear old
    sqlx::query!("UPDATE webhook_events SET created_at = NOW() - INTERVAL '8 days' WHERE status='dead'")
        .execute(engine.pool()).await.unwrap();

    let removed = engine.cleanup_dead(Duration::from_secs(7 * 86_400)).await.unwrap();
    assert_eq!(removed, 5, "old dead events must be cleaned up");

    let stats2 = engine.queue_stats().await.unwrap();
    assert_eq!(stats2.dead, 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_dead_keeps_recent_dlq_events(pool: PgPool) {
    let engine = engine(pool);
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19989/dead', 'new_api_test_secret_32chars_ok', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..3 {
        engine.send("test", json!({"i": i}), dead_ep).await.unwrap();
    }
    engine.run_once().await.unwrap();

    // Just created — should not be cleaned up with 1 day threshold
    let removed = engine.cleanup_dead(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed, 0, "recent dead events must not be cleaned up");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_dead_does_not_touch_delivered_or_failed(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503)) // all fail
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'new_api_test_secret_32chars_ok', 5, 0) RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..3 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap(); // → 3 failed (max_attempts=5)

    sqlx::query!("UPDATE webhook_events SET created_at = NOW() - INTERVAL '30 days'")
        .execute(engine.pool()).await.unwrap();

    let removed = engine.cleanup_dead(Duration::from_secs(1)).await.unwrap();
    assert_eq!(removed, 0, "cleanup_dead must not remove failed events");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.failed, 3, "failed events must remain");
}
