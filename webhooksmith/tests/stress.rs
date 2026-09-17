//! Stress tests — correctness under high concurrency and volume.
//!
//! These do NOT assert timing (unreliable in CI).
//! They assert correctness: right number of deliveries, no double-processing,
//! no data corruption, no panics under pool pressure.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'stress_test_secret_32_chars_ok') RETURNING id",
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

async fn count_by_status(pool: &sqlx::PgPool, status: &str) -> i64 {
    sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events WHERE status = $1", status)
        .fetch_one(pool)
        .await
        .unwrap()
        .unwrap_or(0)
}

// ── Test 1: Large batch with small pool — correctness, not speed ──────────────
//
// batch_size=100, pool provided by sqlx::test (small).
// All 100 events must be delivered exactly once. No deadlock.

#[sqlx::test(migrator = "MIGRATOR")]
async fn large_batch_small_pool_delivers_all(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(100)
        .build_sync();

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..100 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    let delivered = engine.run_once().await.unwrap();
    assert_eq!(delivered, 100);

    let db_delivered = count_by_status(engine.pool(), "delivered").await;
    assert_eq!(db_delivered, 100, "all 100 events must be in 'delivered' state");
    assert_eq!(count_by_status(engine.pool(), "pending").await, 0);
    assert_eq!(count_by_status(engine.pool(), "failed").await, 0);
}

// ── Test 2: High volume across multiple cycles ────────────────────────────────
//
// 500 events, batch_size=100, 5 cycles. All 500 delivered exactly once.

#[sqlx::test(migrator = "MIGRATOR")]
async fn high_volume_multiple_cycles_no_duplicates(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(100)
        .build_sync();

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..500 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // Run until everything is delivered
    let mut total = 0;
    for _ in 0..10 {
        let n = engine.run_once().await.unwrap();
        total += n;
        if total >= 500 { break; }
    }

    let delivered = count_by_status(engine.pool(), "delivered").await;
    assert_eq!(delivered, 500, "all 500 events must be delivered");

    // No duplicates — one attempt per event
    let attempt_count: i64 =
        sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts WHERE success = true")
            .fetch_one(engine.pool())
            .await
            .unwrap()
            .unwrap_or(0);
    assert_eq!(attempt_count, 500, "exactly one successful attempt per event");
}

// ── Test 3: Concurrent workers share pool — no double-processing ──────────────
//
// 4 concurrent workers, 200 events, shared pool.
// Each event must be delivered exactly once.

#[sqlx::test(migrator = "MIGRATOR")]
async fn concurrent_workers_no_double_processing(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // Engines share the same underlying pool — models multi-process workers
    // hitting the same Postgres with SKIP LOCKED
    let make_engine = |batch: i64| {
        WebhookEngine::builder()
            .pool(pool.clone())
            .allow_insecure_urls()
            .batch_size(batch)
            .build_sync()
    };

    let e1 = make_engine(50);
    let e2 = make_engine(50);
    let e3 = make_engine(50);
    let e4 = make_engine(50);

    let endpoint_id = insert_endpoint(&e1, &format!("{}/hook", server.uri())).await;

    for i in 0..200 {
        e1.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // All 4 workers run simultaneously
    let (a, b, c, d) = tokio::join!(
        e1.run_once(),
        e2.run_once(),
        e3.run_once(),
        e4.run_once(),
    );
    let total = a.unwrap() + b.unwrap() + c.unwrap() + d.unwrap();
    assert_eq!(total, 200, "4 workers must collectively claim all 200 events");

    // Wait for all spawned delivery tasks to finish
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let delivered = count_by_status(&pool, "delivered").await;
    assert_eq!(delivered, 200, "all 200 events delivered");

    let attempts: i64 =
        sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts")
            .fetch_one(&pool)
            .await
            .unwrap()
            .unwrap_or(0);
    assert_eq!(attempts, 200, "exactly one attempt per event — no double-processing");
}

// ── Test 4: Mixed success and failure under load ──────────────────────────────
//
// 50 events to a good endpoint + 50 events to a dead endpoint.
// All 50 delivered, 50 failed — none lost or doubled.

#[sqlx::test(migrator = "MIGRATOR")]
async fn mixed_success_failure_under_load(pool: PgPool) {
    let good_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&good_server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(100)
        .build_sync();

    let good_ep = insert_endpoint(&engine, &format!("{}/hook", good_server.uri())).await;

    // Dead endpoint: nothing listening on this port
    let dead_ep = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ('http://127.0.0.1:19996/dead', 'stress_test_secret_32_chars_ok') RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..50 {
        engine.send("test.event", json!({"i": i}), good_ep).await.unwrap();
        engine.send("test.event", json!({"i": i}), dead_ep).await.unwrap();
    }

    engine.run_once().await.unwrap(); // 100 events claimed, 50 succeed, 50 fail

    let delivered = count_by_status(engine.pool(), "delivered").await;
    let failed = count_by_status(engine.pool(), "failed").await;

    assert_eq!(delivered, 50, "good endpoint: all 50 delivered");
    assert_eq!(failed, 50, "dead endpoint: all 50 failed and scheduled for retry");
    assert_eq!(count_by_status(engine.pool(), "pending").await, 0);
}

// ── Test 5: Pool pressure — batch >> pool connections ────────────────────────
//
// With batch_size=200 and a small pool (provided by sqlx::test, typically ~5),
// the worker must still complete correctly. Connections queue, not fail.

#[sqlx::test(migrator = "MIGRATOR")]
async fn batch_larger_than_pool_still_correct(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // Force a deliberately small pool relative to batch size
    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(200)
        .build_sync();

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    for i in 0..200 {
        engine.send("test.event", json!({"i": i}), endpoint_id).await.unwrap();
    }

    // This must complete without error — connections queue, not deadlock or fail
    let result = engine.run_once().await;
    assert!(result.is_ok(), "run_once must succeed even when batch >> pool connections");
    assert_eq!(result.unwrap(), 200);

    let delivered = count_by_status(engine.pool(), "delivered").await;
    assert_eq!(delivered, 200);
}

// ── Test 6: Broadcast under load — fan-out to many endpoints ─────────────────
//
// 10 endpoints, 50 broadcast events = 500 total event rows.
// All 500 must be delivered exactly once.

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_fanout_under_load(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(500)
        .build_sync();

    // Register 10 endpoints
    for _ in 0..10 {
        insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    }

    // Broadcast 50 events → 50 × 10 = 500 event rows
    for i in 0..50 {
        let events = engine.broadcast("load.test", json!({"i": i})).await.unwrap();
        assert_eq!(events.len(), 10, "each broadcast must create 10 events");
    }

    let total_pending = count_by_status(engine.pool(), "pending").await;
    assert_eq!(total_pending, 500, "500 pending events before delivery");

    // Deliver all 500 in one batch
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 500);

    let delivered = count_by_status(engine.pool(), "delivered").await;
    assert_eq!(delivered, 500);

    let attempts: i64 =
        sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts WHERE success = true")
            .fetch_one(engine.pool())
            .await
            .unwrap()
            .unwrap_or(0);
    assert_eq!(attempts, 500, "one attempt per event — no doubles");
}

// ── Test 7: Rapid repeated cycles — no state corruption across cycles ─────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn rapid_cycles_no_corruption(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(20)
        .build_sync();

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;

    // 10 cycles: each enqueues 20 events and immediately runs the worker
    for cycle in 0..10 {
        for i in 0..20 {
            engine
                .send("cycle.test", json!({"cycle": cycle, "i": i}), endpoint_id)
                .await
                .unwrap();
        }
        let n = engine.run_once().await.unwrap();
        assert_eq!(n, 20, "cycle {cycle}: must claim exactly 20 events");
    }

    let delivered = count_by_status(engine.pool(), "delivered").await;
    assert_eq!(delivered, 200, "all 200 events across 10 cycles delivered");

    let attempts: i64 =
        sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts")
            .fetch_one(engine.pool())
            .await
            .unwrap()
            .unwrap_or(0);
    assert_eq!(attempts, 200, "exactly one attempt per event across all cycles");
}
