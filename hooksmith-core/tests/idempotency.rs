//! Idempotency key tests.
//!
//! Properties being verified:
//!   A. First call creates the event.
//!   B. Second call with same key returns the SAME event (no duplicate).
//!   C. Different keys create different events.
//!   D. Same key, different endpoint → separate events (key is per-endpoint).
//!   E. send_idempotent_in_tx: rollback means key is reusable afterward.
//!   F. broadcast_idempotent: fan-out deduplicates per endpoint.
//!   G. Duplicate key does not trigger a second delivery.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'idempotency_test_secret_32chars') RETURNING id",
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── Property A + B: Same key → same event ────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn same_key_returns_same_event(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    let first = engine
        .send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001")
        .await
        .unwrap();

    let second = engine
        .send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001")
        .await
        .unwrap();

    assert_eq!(first.id, second.id, "same key must return the same event id");
    assert_eq!(first.idempotency_key, Some("order-1001".to_string()));

    // Only one event in the DB
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 1, "duplicate call must not create a second event");
}

// ── Property C: Different keys → different events ────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn different_keys_create_different_events(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    let a = engine
        .send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001")
        .await
        .unwrap();
    let b = engine
        .send_idempotent("order.created", json!({"id": 2}), endpoint_id, "order-1002")
        .await
        .unwrap();

    assert_ne!(a.id, b.id);

    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 2);
}

// ── Property D: Same key, different endpoint → independent events ─────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn same_key_different_endpoints_are_independent(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep_a = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let ep_b = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    let event_a = engine
        .send_idempotent("order.created", json!({}), ep_a, "order-1001")
        .await
        .unwrap();
    let event_b = engine
        .send_idempotent("order.created", json!({}), ep_b, "order-1001")
        .await
        .unwrap();

    assert_ne!(event_a.id, event_b.id, "same key is independent per endpoint");

    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 2);
}

// ── Property E: Rollback makes key reusable ───────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn key_is_reusable_after_rollback(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    // Use the key inside a transaction, then roll it back
    let mut tx = engine.pool().begin().await.unwrap();
    let first = engine
        .send_idempotent_in_tx("order.created", json!({"id": 1}), endpoint_id, "order-1001", &mut tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    // Key is now reusable — rollback removed the row
    let second = engine
        .send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001")
        .await
        .unwrap();

    assert_ne!(first.id, second.id, "after rollback the key must be reusable");

    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 1, "only the committed event must exist");
}

// ── Property F: broadcast_idempotent deduplicates per endpoint ────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_idempotent_deduplicates_per_endpoint(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    // Two endpoints
    insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    let first_broadcast = engine
        .broadcast_idempotent("order.created", json!({"id": 1}), "order-1001")
        .await
        .unwrap();
    assert_eq!(first_broadcast.len(), 2, "first broadcast creates 2 events");

    let second_broadcast = engine
        .broadcast_idempotent("order.created", json!({"id": 1}), "order-1001")
        .await
        .unwrap();
    assert_eq!(second_broadcast.len(), 2, "second broadcast returns same 2 events");

    // Same IDs returned both times
    let mut first_ids: Vec<_> = first_broadcast.iter().map(|e| e.id).collect();
    let mut second_ids: Vec<_> = second_broadcast.iter().map(|e| e.id).collect();
    first_ids.sort();
    second_ids.sort();
    assert_eq!(first_ids, second_ids, "duplicate broadcast must return same event IDs");

    // Still only 2 events in DB
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 2, "duplicate broadcast must not create extra events");
}

// ── Property G: Duplicate key causes zero extra deliveries ────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn duplicate_key_causes_zero_extra_deliveries(pool: PgPool) {
    let server = MockServer::start().await;

    // Expect exactly ONE POST, not two
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let engine = engine(pool);
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    // Call twice with same key
    engine.send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001").await.unwrap();
    engine.send_idempotent("order.created", json!({"id": 1}), endpoint_id, "order-1001").await.unwrap();

    // Worker runs — must deliver exactly once
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 1, "worker must see only one event");

    let delivered: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events WHERE status = 'delivered'")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(delivered, 1);

    // Wiremock verifies exactly 1 HTTP call was made
    server.verify().await;
}

// ── Extra: null key (no idempotency) allows duplicates ────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn send_without_key_allows_duplicates(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    // Regular send() called twice — creates two events (expected, no dedup)
    let a = engine.send("order.created", json!({"id": 1}), endpoint_id).await.unwrap();
    let b = engine.send("order.created", json!({"id": 1}), endpoint_id).await.unwrap();

    assert_ne!(a.id, b.id);
    assert_eq!(a.idempotency_key, None);
    assert_eq!(b.idempotency_key, None);

    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 2, "send() without key must allow duplicates");
}
