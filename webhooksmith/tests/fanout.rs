//! Fan-out (broadcast) tests.
//! Each test exercises a specific broadcast scenario against real Postgres + real HTTP.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::{error::HooksmithError, EventStatus, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

async fn mock_endpoint(engine: &WebhookEngine, server: &MockServer) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'broadcast_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── Scenario 1: All enabled endpoints receive the event ───────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_sends_to_all_enabled_endpoints(pool: PgPool) {
    let engine = engine(pool);

    // Three independent receivers
    let server_a = MockServer::start().await;
    let server_b = MockServer::start().await;
    let server_c = MockServer::start().await;

    for s in [&server_a, &server_b, &server_c] {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(s)
            .await;
    }

    mock_endpoint(&engine, &server_a).await;
    mock_endpoint(&engine, &server_b).await;
    mock_endpoint(&engine, &server_c).await;

    // One broadcast creates one event per endpoint
    let events = engine.broadcast("order.created", json!({"id": 1})).await.unwrap();
    assert_eq!(events.len(), 3, "should create one event per endpoint");
    assert!(events.iter().all(|e| e.status == EventStatus::Pending));

    // Worker delivers all three
    let delivered = engine.run_once().await.unwrap();
    assert_eq!(delivered, 3);

    server_a.verify().await;
    server_b.verify().await;
    server_c.verify().await;
}

// ── Scenario 2: Disabled endpoints are skipped ───────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_skips_disabled_endpoints(pool: PgPool) {
    let engine = engine(pool);

    let enabled_server = MockServer::start().await;
    let disabled_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&enabled_server)
        .await;

    // Disabled endpoint must receive zero requests
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&disabled_server)
        .await;

    mock_endpoint(&engine, &enabled_server).await;

    // Insert the disabled endpoint directly
    sqlx::query!(
        "INSERT INTO webhook_endpoints (url, signing_secret, enabled) VALUES ($1, 'broadcast_test_secret_32chars', false)",
        &format!("{}/hook", disabled_server.uri()),
    )
    .execute(engine.pool())
    .await
    .unwrap();

    let events = engine.broadcast("order.created", json!({"id": 1})).await.unwrap();
    assert_eq!(events.len(), 1, "disabled endpoint must not receive an event");

    engine.run_once().await.unwrap();

    enabled_server.verify().await;
    disabled_server.verify().await;
}

// ── Scenario 3: No endpoints → empty vec, no error ───────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_with_no_endpoints_returns_empty(pool: PgPool) {
    let engine = engine(pool);

    let events = engine.broadcast("order.created", json!({"id": 1})).await.unwrap();
    assert!(events.is_empty(), "no endpoints → empty vec, not an error");

    // Worker has nothing to process
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0);
}

// ── Scenario 4: broadcast_in_tx rolls back atomically ────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_in_tx_rolls_back_atomically(pool: PgPool) {
    let engine = engine(pool);

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0) // must not be called — tx rolls back
        .mount(&server)
        .await;

    mock_endpoint(&engine, &server).await;

    // Broadcast inside a transaction, then rollback
    let mut tx = engine.pool().begin().await.unwrap();
    let events = engine
        .broadcast_in_tx("order.created", json!({"id": 1}), &mut tx)
        .await
        .unwrap();
    assert_eq!(events.len(), 1, "event visible inside tx before rollback");

    tx.rollback().await.unwrap();

    // Worker has nothing — the events were rolled back
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0, "worker must find nothing after tx rollback");

    // Verify no event exists in the DB
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool())
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count, 0, "no events must persist after rollback");

    server.verify().await;
}

// ── Scenario 5: broadcast_in_tx commits atomically with business data ─────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_in_tx_commits_with_business_data(pool: PgPool) {
    let engine = engine(pool);

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(2) // two endpoints
        .mount(&server)
        .await;

    mock_endpoint(&engine, &server).await;
    mock_endpoint(&engine, &server).await;

    let mut tx = engine.pool().begin().await.unwrap();
    let events = engine
        .broadcast_in_tx("payment.captured", json!({"amount": 99.99}), &mut tx)
        .await
        .unwrap();
    assert_eq!(events.len(), 2);
    tx.commit().await.unwrap();

    let delivered = engine.run_once().await.unwrap();
    assert_eq!(delivered, 2);

    server.verify().await;
}

// ── Scenario 6: Payload validation applies to broadcast ──────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_rejects_oversized_payload(pool: PgPool) {
    let engine = engine(pool);
    let big = json!({ "data": "x".repeat(1_100_000) });
    let err = engine.broadcast("test.event", big).await.unwrap_err();
    assert!(matches!(err, HooksmithError::PayloadTooLarge(_, _)));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_rejects_empty_event_type(pool: PgPool) {
    let engine = engine(pool);
    assert!(engine.broadcast("", json!({})).await.is_err());
    assert!(engine.broadcast("  ", json!({})).await.is_err());
}

// ── Scenario 7: Each endpoint gets its own independent event record ───────────
// This means one endpoint failing doesn't affect another endpoint's delivery.

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_failures_are_per_endpoint_independent(pool: PgPool) {
    let engine = engine(pool);

    let good_server = MockServer::start().await;
    let dead_url = "http://127.0.0.1:19998/dead"; // nothing listening here

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&good_server)
        .await;

    mock_endpoint(&engine, &good_server).await;

    // Dead endpoint (nothing listening on that port)
    sqlx::query!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'broadcast_test_secret_32chars')",
        dead_url,
    )
    .execute(engine.pool())
    .await
    .unwrap();

    let events = engine.broadcast("order.created", json!({"id": 1})).await.unwrap();
    assert_eq!(events.len(), 2);

    engine.run_once().await.unwrap();

    // Good endpoint delivered, dead endpoint failed — verify via delivery log below.

    let all_logs: Vec<_> = futures::future::join_all(
        events.iter().map(|e| engine.delivery_log(e.id))
    ).await;

    let attempts: Vec<_> = all_logs.into_iter().flatten().flatten().collect();
    let successes = attempts.iter().filter(|a| a.success).count();
    let failures = attempts.iter().filter(|a| !a.success).count();

    assert_eq!(successes, 1, "good endpoint must be delivered");
    assert_eq!(failures, 1, "dead endpoint must be recorded as failure");

    good_server.verify().await;
}
