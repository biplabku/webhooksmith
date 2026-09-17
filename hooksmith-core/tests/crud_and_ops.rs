//! CRUD and operational API tests.
//! Covers: list_endpoints, delete_endpoint, queue_stats, events_by_status,
//!         dead_events_paged, retry_all_dead, cleanup_delivered.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{error::HooksmithError, EventStatus, WebhookEngine};
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

async fn live_endpoint(engine: &WebhookEngine, server: &MockServer) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'crud_ops_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── list_endpoints ─────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn list_endpoints_returns_all(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let ep_a = engine.register(&format!("{}/a", server.uri()), "crud_ops_test_secret_32chars").await.unwrap();
    let ep_b = engine.register(&format!("{}/b", server.uri()), "crud_ops_test_secret_32chars").await.unwrap();

    let list = engine.list_endpoints().await.unwrap();
    assert_eq!(list.len(), 2);

    let ids: Vec<_> = list.iter().map(|e| e.id).collect();
    assert!(ids.contains(&ep_a.id));
    assert!(ids.contains(&ep_b.id));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn list_endpoints_empty_when_none_registered(pool: PgPool) {
    let engine = engine(pool);
    let list = engine.list_endpoints().await.unwrap();
    assert!(list.is_empty());
}

// ── delete_endpoint ────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn delete_endpoint_removes_it_from_list(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let ep = engine.register(&format!("{}/hook", server.uri()), "crud_ops_test_secret_32chars").await.unwrap();
    assert_eq!(engine.list_endpoints().await.unwrap().len(), 1);

    engine.delete_endpoint(ep.id).await.unwrap();
    assert_eq!(engine.list_endpoints().await.unwrap().len(), 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn delete_endpoint_cascade_deletes_events(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = engine.register(&format!("{}/hook", server.uri()), "crud_ops_test_secret_32chars").await.unwrap();

    for i in 0..5 {
        engine.send("test.event", json!({"i": i}), ep.id).await.unwrap();
    }

    let stats_before = engine.queue_stats().await.unwrap();
    assert_eq!(stats_before.pending, 5);

    engine.delete_endpoint(ep.id).await.unwrap();

    let stats_after = engine.queue_stats().await.unwrap();
    assert_eq!(stats_after.pending, 0, "events must be cascade-deleted");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn delete_nonexistent_endpoint_returns_not_found(pool: PgPool) {
    let engine = engine(pool);
    let err = engine.delete_endpoint(uuid::Uuid::new_v4()).await.unwrap_err();
    assert!(matches!(err, HooksmithError::EndpointNotFound(_)));
}

// ── queue_stats ────────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn queue_stats_counts_each_status(pool: PgPool) {
    let server = MockServer::start().await;
    // Good endpoint: 200 for first call, 500 for the rest
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let good_ep = live_endpoint(&engine, &server).await;
    let dead_ep = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19994/dead', 'crud_ops_test_secret_32chars', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    // 3 pending events
    for i in 0..3 {
        engine.send("test", json!({"i": i}), good_ep).await.unwrap();
    }

    let s0 = engine.queue_stats().await.unwrap();
    assert_eq!(s0.pending, 3);
    assert_eq!(s0.failed + s0.dead + s0.delivered + s0.delivering, 0);

    // Deliver 1 (success), 2 become failed
    engine.run_once().await.unwrap();

    let s1 = engine.queue_stats().await.unwrap();
    assert_eq!(s1.delivered, 1, "one event must be delivered");
    assert_eq!(s1.failed, 2, "two events must be in failed state");
    assert_eq!(s1.pending, 0);

    // Enqueue one event to dead endpoint (max_attempts=1) → will go to dead after one failure
    engine.send("test", json!({}), dead_ep).await.unwrap();
    engine.run_once().await.unwrap(); // fails once → goes to dead (max_attempts=1)

    let s2 = engine.queue_stats().await.unwrap();
    assert_eq!(s2.dead, 1, "one event must be in dead state");
    assert_eq!(s2.delivered, 1); // unchanged
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn queue_stats_zero_when_no_events(pool: PgPool) {
    let engine = engine(pool);
    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.dead, 0);
    assert_eq!(stats.delivered, 0);
    assert_eq!(stats.delivering, 0);
}

// ── events_by_status (paginated) ───────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn events_by_status_filters_correctly(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let ep = live_endpoint(&engine, &server).await;
    for i in 0..5 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap();

    let delivered = engine.events_by_status(ep, EventStatus::Delivered, 100, 0).await.unwrap();
    let pending = engine.events_by_status(ep, EventStatus::Pending, 100, 0).await.unwrap();

    assert_eq!(delivered.len(), 5);
    assert_eq!(pending.len(), 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn events_by_status_pagination_works(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let ep = live_endpoint(&engine, &server).await;
    for i in 0..10 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }

    // Page 1: first 3
    let page1 = engine.events_by_status(ep, EventStatus::Pending, 3, 0).await.unwrap();
    assert_eq!(page1.len(), 3);

    // Page 2: next 3
    let page2 = engine.events_by_status(ep, EventStatus::Pending, 3, 3).await.unwrap();
    assert_eq!(page2.len(), 3);

    // No overlap between pages
    let ids1: std::collections::HashSet<_> = page1.iter().map(|e| e.id).collect();
    let ids2: std::collections::HashSet<_> = page2.iter().map(|e| e.id).collect();
    assert!(ids1.is_disjoint(&ids2), "pages must not overlap");

    // Last page: remaining 4
    let page3 = engine.events_by_status(ep, EventStatus::Pending, 100, 6).await.unwrap();
    assert_eq!(page3.len(), 4);
}

// ── dead_events_paged ──────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn dead_events_paged_respects_limit_offset(pool: PgPool) {
    let engine = engine(pool);
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19993/dead', 'crud_ops_test_secret_32chars', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..8 { engine.send("test", json!({"i": i}), dead_ep).await.unwrap(); }
    engine.run_once().await.unwrap();

    let all_dead = engine.dead_events(dead_ep).await.unwrap();
    assert_eq!(all_dead.len(), 8);

    let page1 = engine.dead_events_paged(dead_ep, 3, 0).await.unwrap();
    let page2 = engine.dead_events_paged(dead_ep, 3, 3).await.unwrap();
    let page3 = engine.dead_events_paged(dead_ep, 3, 6).await.unwrap();

    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);
    assert_eq!(page3.len(), 2); // remainder

    // All pages together cover all 8 events without overlap
    let all_paged: std::collections::HashSet<_> = page1.iter()
        .chain(&page2)
        .chain(&page3)
        .map(|e| e.id)
        .collect();
    assert_eq!(all_paged.len(), 8);
}

// ── retry_all_dead ─────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_all_dead_requeues_all_and_returns_count(pool: PgPool) {
    let server = MockServer::start().await;
    // First run: all fail; second run: all succeed
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(500))
        .up_to_n_times(5).mount(&server).await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200))
        .mount(&server).await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'crud_ops_test_secret_32chars', 1, 0) RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..5 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }

    // First attempt: all fail → all go to DLQ (max_attempts=1)
    engine.run_once().await.unwrap();
    assert_eq!(engine.queue_stats().await.unwrap().dead, 5);

    // Bulk retry all
    let retried = engine.retry_all_dead(ep).await.unwrap();
    assert_eq!(retried, 5, "retry_all_dead must return count of requeued events");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.pending, 5, "all events must be back in pending");
    assert_eq!(stats.dead, 0);

    // Second attempt: all succeed
    engine.run_once().await.unwrap();
    assert_eq!(engine.queue_stats().await.unwrap().delivered, 5);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_all_dead_returns_zero_when_nothing_in_dlq(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_endpoint(&engine, &server).await;
    let count = engine.retry_all_dead(ep).await.unwrap();
    assert_eq!(count, 0);
}

// ── cleanup_delivered ──────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_delivered_removes_old_events(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let engine = engine(pool);
    let ep = live_endpoint(&engine, &server).await;

    for i in 0..5 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap();
    assert_eq!(engine.queue_stats().await.unwrap().delivered, 5);

    // Backdate the delivered events so they appear old
    sqlx::query!("UPDATE webhook_events SET created_at = NOW() - INTERVAL '2 days' WHERE status='delivered'")
        .execute(engine.pool()).await.unwrap();

    // cleanup with 1 day threshold — all 5 events are 2 days old → deleted
    let removed = engine.cleanup_delivered(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed, 5);

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_delivered_keeps_recent_events(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let engine = engine(pool);
    let ep = live_endpoint(&engine, &server).await;

    for i in 0..3 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap();

    // Events just delivered (now) — should not be removed with 1 day threshold
    let removed = engine.cleanup_delivered(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed, 0, "recent events must not be cleaned up");
    assert_eq!(engine.queue_stats().await.unwrap().delivered, 3);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_delivered_does_not_touch_pending_or_dead(pool: PgPool) {
    let engine = engine(pool);
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19992/dead', 'crud_ops_test_secret_32chars', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    // 2 pending, 2 dead
    for i in 0..2 { engine.send("test", json!({"i": i}), dead_ep).await.unwrap(); }
    engine.run_once().await.unwrap(); // → 2 dead (max_attempts=1)

    for i in 0..2 {
        engine.send("test", json!({"i": i}), dead_ep).await.unwrap();
        // Keep them pending by not running the worker
    }

    // Backdate all events so they appear old
    sqlx::query!("UPDATE webhook_events SET created_at = NOW() - INTERVAL '30 days'")
        .execute(engine.pool()).await.unwrap();

    // cleanup only removes 'delivered' events — none here
    let removed = engine.cleanup_delivered(Duration::from_secs(1)).await.unwrap();
    assert_eq!(removed, 0, "cleanup must not remove pending or dead events");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.dead, 2, "dead events must remain");
    assert_eq!(stats.pending, 2, "pending events must remain");
}
