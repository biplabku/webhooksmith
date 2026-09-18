//! Edge case and failure scenario tests.
//! Each test is named after the scenario it exercises.
//! Tests use only the public WebhookEngine API plus raw sqlx for DB setup.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::{error::HooksmithError, EventStatus, NewEndpoint, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, description)
        VALUES ($1, 'test_secret_for_edge_case_tests', 'edge case endpoint')
        RETURNING id
        "#,
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── Bug 1: Zombie worker ──────────────────────────────────────────────────────
//
// Scenario: Worker A is very slow. The reaper resets the event to 'pending'.
// Worker B claims and delivers it. Then Worker A tries to call record_success.
// Expected: the AND status='delivering' guard makes Worker A's update a no-op.

#[sqlx::test(migrator = "MIGRATOR")]
async fn zombie_worker_cannot_corrupt_delivered_event(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    // Simulate: Worker A claimed the event
    sqlx::query!(
        "UPDATE webhook_events SET status = 'delivering', delivering_since = NOW() WHERE id = $1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    // Simulate: reaper fires (worker took >120s)
    sqlx::query!(
        "UPDATE webhook_events SET status = 'pending', delivering_since = NULL WHERE id = $1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    // Worker B claims and delivers normally
    engine.run_once().await.unwrap();

    let after_b = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(after_b.status, EventStatus::Delivered);
    assert_eq!(after_b.attempts, 1);

    // Zombie Worker A's update: sets delivered WHERE status='delivering'.
    // Event is now 'delivered', so the guard fires — 0 rows affected.
    let rows = sqlx::query!(
        "UPDATE webhook_events SET status='delivered', attempts=attempts+1, delivering_since=NULL WHERE id=$1 AND status='delivering'",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap()
    .rows_affected();

    assert_eq!(rows, 0, "zombie update must be a no-op");

    let after_zombie = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(after_zombie.status, EventStatus::Delivered, "zombie must not corrupt state");
    assert_eq!(after_zombie.attempts, 1, "zombie must not increment attempts");
}

// ── Bug 2: Reaper recovers stuck events ──────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn reaper_resets_stuck_event_and_delivery_succeeds(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("stuck.event", json!({}), endpoint_id).await.unwrap();

    // Simulate worker crash: event stuck in 'delivering' for 200s
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW() - INTERVAL '200 seconds' WHERE id=$1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    assert_eq!(
        engine.event(event.id).await.unwrap().unwrap().status,
        EventStatus::Delivering
    );

    // Reaper: 120s timeout, event has been stuck 200s → reset
    let recovered = engine.recover_stuck_deliveries(std::time::Duration::from_secs(120)).await.unwrap();
    assert_eq!(recovered, 1);

    let reset = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(reset.status, EventStatus::Pending);
    assert!(reset.delivering_since.is_none());

    engine.run_once().await.unwrap();

    assert_eq!(
        engine.event(event.id).await.unwrap().unwrap().status,
        EventStatus::Delivered
    );

    server.verify().await;
}

// ── Bug 3: Redirect SSRF protection ──────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn redirect_response_is_not_followed(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(
            ResponseTemplate::new(301)
                .insert_header("location", "http://192.168.1.1/internal"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Failed, "301 must be a failure, not delivery");

    server.verify().await;
}

// ── Bug 4: max_attempts = 0 validation ───────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn max_attempts_zero_is_rejected(pool: PgPool) {
    let engine = engine(pool);

    let result = engine.register_with(NewEndpoint {
        url: "https://api.example.com/hook".into(),
        signing_secret: "a_valid_secret_that_is_long_enough".into(),
        description: None,
        max_attempts: Some(0),
        initial_delay_ms: None,
            event_filter: None,
    }).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HooksmithError::Config(_)));
}

// ── Bug 5: retry_dead_event distinguishes not-found from wrong-state ──────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_dead_on_delivered_event_returns_invalid_state(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();
    assert_eq!(engine.event(event.id).await.unwrap().unwrap().status, EventStatus::Delivered);

    let err = engine.retry_dead(event.id).await.unwrap_err();
    assert!(
        matches!(err, HooksmithError::InvalidState(_)),
        "expected InvalidState, got: {err}"
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_dead_on_nonexistent_returns_not_found(pool: PgPool) {
    let engine = engine(pool);
    let err = engine.retry_dead(uuid::Uuid::new_v4()).await.unwrap_err();
    assert!(matches!(err, HooksmithError::EventNotFound(_)));
}

// ── Bug 6: Payload size limit ─────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn oversized_payload_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let endpoint_id = insert_endpoint(&engine, "https://example.com/hook").await;

    let big = json!({ "data": "x".repeat(1_100_000) });
    let err = engine.send("test.event", big, endpoint_id).await.unwrap_err();
    assert!(matches!(err, HooksmithError::PayloadTooLarge(_, _)));
}

// ── Bug 7: Empty event_type is rejected ───────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn empty_event_type_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let endpoint_id = insert_endpoint(&engine, "https://example.com/hook").await;

    assert!(engine.send("", json!({}), endpoint_id).await.is_err());
    assert!(engine.send("   ", json!({}), endpoint_id).await.is_err());
}

// ── Bug 8: Slow endpoint records duration correctly ───────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn slow_endpoint_records_duration(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(100)))
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    assert_eq!(engine.event(event.id).await.unwrap().unwrap().status, EventStatus::Delivered);

    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(
        log[0].duration_ms.unwrap() >= 100,
        "duration_ms should reflect actual elapsed time"
    );
}

// ── Bug 9: Concurrent workers don't double-process ────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn two_concurrent_workers_claim_different_events(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // Both workers share the same pool
    let engine_a = WebhookEngine::builder()
        .pool(pool.clone())
        .allow_insecure_urls()
        .batch_size(2)
        .build_sync();
    let engine_b = WebhookEngine::builder()
        .pool(pool.clone())
        .allow_insecure_urls()
        .batch_size(2)
        .build_sync();

    let endpoint_id = insert_endpoint(&engine_a, &format!("{}/hook", server.uri())).await;

    for i in 0..4 {
        engine_a.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    let (a, b) = tokio::join!(engine_a.run_once(), engine_b.run_once());
    let total = a.unwrap() + b.unwrap();

    assert_eq!(total, 4, "workers must split work without overlap");

    let attempt_count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts")
        .fetch_one(&pool)
        .await
        .unwrap()
        .unwrap_or(0);

    assert_eq!(attempt_count, 4, "each event must be delivered exactly once");
}
