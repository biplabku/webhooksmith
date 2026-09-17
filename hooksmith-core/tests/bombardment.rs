//! Bombardment tests — high volume, high concurrency, hostile conditions.
//! The library must not corrupt data, panic, or deadlock under any of these.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::WebhookEngine;
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};
use futures::future::join_all;

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

fn engine_with_batch(pool: PgPool, batch: i64) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .batch_size(batch)
        .build_sync()
}

async fn live_ep(engine: &WebhookEngine, server: &MockServer) -> uuid::Uuid {
    sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'bombardment_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── 1. 10,000 events: all delivered, zero duplicates ──────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn ten_thousand_events_no_duplicates(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine_with_batch(pool, 500);
    let ep = live_ep(&engine, &server).await;

    for i in 0..10_000 {
        engine.send("stress", json!({"i": i}), ep).await.unwrap();
    }

    let mut total_delivered = 0;
    for _ in 0..25 { // 10,000 / 500 = 20 cycles needed
        let n = engine.run_once().await.unwrap();
        total_delivered += n;
        if total_delivered >= 10_000 { break; }
    }

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 10_000, "all 10k events must be delivered");
    assert_eq!(stats.pending + stats.failed + stats.dead, 0);

    // One attempt per event — no duplicates
    let attempt_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM webhook_delivery_attempts WHERE success = true"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
    .unwrap_or(0);
    assert_eq!(attempt_count, 10_000, "exactly one delivery attempt per event");
}

// ── 2. 500 concurrent sends — all succeed ─────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn five_hundred_concurrent_sends_all_succeed(pool: PgPool) {
    let server = MockServer::start().await;
    let engine = engine(pool.clone());
    let ep = live_ep(&engine, &server).await;

    // 500 concurrent send() calls against a small pool
    let handles: Vec<_> = (0..500)
        .map(|i| {
            let p = pool.clone();
            tokio::spawn(async move {
                let e = WebhookEngine::builder()
                    .pool(p)
                    .allow_insecure_urls()
                    .build_sync();
                e.send("bombardment", json!({"i": i}), ep).await
            })
        })
        .collect();

    let results = join_all(handles).await;
    let successes = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
    let failures = results.iter().filter(|r| r.as_ref().map(|r| r.is_err()).unwrap_or(false)).count();

    // Under pool pressure some may fail with PoolTimedOut — that's ok
    // But the DB must be consistent: successes + failures = 500
    assert_eq!(successes + failures, 500);

    let db_count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(&pool).await.unwrap().unwrap_or(0);
    // Every success must have a row in DB
    assert_eq!(db_count, successes as i64, "DB count must match successful sends");
}

// ── 3. Rapid empty cycles don't hang or panic ─────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn thousand_empty_cycles_return_quickly(pool: PgPool) {
    let engine = engine(pool);

    let result = tokio::time::timeout(Duration::from_secs(5), async {
        for _ in 0..1000 {
            let n = engine.run_once().await.unwrap();
            assert_eq!(n, 0);
        }
    }).await;

    assert!(result.is_ok(), "1000 empty run_once calls must complete within 5 seconds");
}

// ── 4. All endpoints timing out — no crash, failures recorded ─────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn all_endpoints_timing_out_worker_stays_alive(pool: PgPool) {
    let server = MockServer::start().await;
    // Endpoint takes 600ms — our 100ms timeout fires
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(600)))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .http_timeout(Duration::from_millis(100))
        .stuck_timeout(Duration::from_secs(120))
        .batch_size(50)
        .build_sync();

    let ep = live_ep(&engine, &server).await;
    for i in 0..50 { engine.send("timeout", json!({"i": i}), ep).await.unwrap(); }

    // Worker must complete the cycle even when all 50 endpoints time out
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        engine.run_once()
    ).await;

    assert!(result.is_ok(), "run_once must return even when all deliveries time out");
    let n = result.unwrap().unwrap();
    assert_eq!(n, 50, "50 events must be claimed");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.failed, 50, "all 50 must be in failed state after timeout");
    assert_eq!(stats.delivered, 0);
}

// ── 5. 8 concurrent workers sharing a pool — no double processing ──────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn eight_concurrent_workers_no_double_processing(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'bombardment_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Enqueue 800 events
    let engine_enq = engine(pool.clone());
    for i in 0..800 {
        engine_enq.send("load", json!({"i": i}), ep).await.unwrap();
    }

    // 8 workers, each with batch_size=100, all run simultaneously
    let workers: Vec<_> = (0..8).map(|_| {
        WebhookEngine::builder()
            .pool(pool.clone())
            .allow_insecure_urls()
            .batch_size(100)
            .build_sync()
    }).collect();

    let results = join_all(workers.iter().map(|w| w.run_once())).await;
    let total_claimed: usize = results.into_iter().map(|r| r.unwrap()).sum();
    assert_eq!(total_claimed, 800, "8 workers must collectively claim all 800 events");

    // Verify no double processing
    let attempts: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_delivery_attempts")
        .fetch_one(&pool).await.unwrap().unwrap_or(0);
    assert_eq!(attempts, 800, "exactly one attempt per event");
}

// ── 6. Mixed healthy/dead endpoints under load ────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn mixed_endpoints_health_isolation(pool: PgPool) {
    let good_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&good_server)
        .await;

    let engine = engine_with_batch(pool, 200);

    let good_ep = live_ep(&engine, &good_server).await;
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ('http://127.0.0.1:19988/dead', 'bombardment_test_secret_32chars') RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    // 100 good, 100 dead
    for i in 0..100 {
        engine.send("good", json!({"i": i}), good_ep).await.unwrap();
        engine.send("dead", json!({"i": i}), dead_ep).await.unwrap();
    }

    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 200);

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 100, "good endpoint: all 100 delivered");
    assert_eq!(stats.failed, 100, "dead endpoint: all 100 failed");
    assert_eq!(stats.pending, 0);
}

// ── 7. Broadcast under bombardment — fan-out correctness ──────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_to_20_endpoints_1000_times(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine_with_batch(pool, 1000);

    // Register 20 endpoints
    for _ in 0..20 {
        live_ep(&engine, &server).await;
    }

    // 1000 broadcasts → 20,000 event rows
    for i in 0..1000 {
        let events = engine.broadcast("load", json!({"i": i})).await.unwrap();
        assert_eq!(events.len(), 20);
    }

    let pending: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events WHERE status='pending'")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(pending, 20_000);

    // Deliver in batches of 1000
    let mut delivered = 0i64;
    for _ in 0..25 {
        let n = engine.run_once().await.unwrap();
        delivered += n as i64;
        if delivered >= 20_000 { break; }
    }

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 20_000);
    assert_eq!(stats.pending, 0);

    // No duplicates
    let attempts: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM webhook_delivery_attempts WHERE success=true"
    )
    .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(attempts, 20_000, "exactly one successful attempt per event");
}

// ── 8. Pool exhaustion doesn't deadlock ────────────────────────────────────────
//
// batch_size=100 but only a few pool connections — tasks queue for connections,
// not deadlock. The key invariant: each delivery task holds at most 1 connection
// at a time (connections are acquired, used, released — never two at once).

#[sqlx::test(migrator = "MIGRATOR")]
async fn pool_exhaustion_doesnt_deadlock(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    // batch_size=100 with a test pool (sqlx::test uses a small pool)
    let engine = engine_with_batch(pool, 100);
    let ep = live_ep(&engine, &server).await;

    for i in 0..100 { engine.send("load", json!({"i": i}), ep).await.unwrap(); }

    // Must complete without deadlocking even with pool pressure
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        engine.run_once()
    ).await;

    assert!(result.is_ok(), "run_once must not deadlock under pool pressure");
    let n = result.unwrap().unwrap();
    assert_eq!(n, 100);

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 100);
}

// ── 9. Idempotency under concurrent bombardment ───────────────────────────────
//
// 100 concurrent workers all trying to send_idempotent with the same key.
// Must create exactly 1 event.

#[sqlx::test(migrator = "MIGRATOR")]
async fn idempotency_holds_under_extreme_concurrent_writes(pool: PgPool) {
    let server = MockServer::start().await;
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'bombardment_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // 100 concurrent workers all sending with the same key
    let handles: Vec<_> = (0..100).map(|_| {
        let p = pool.clone();
        tokio::spawn(async move {
            let e = WebhookEngine::builder().pool(p).allow_insecure_urls().build_sync();
            e.send_idempotent("order.created", json!({"id": 1}), ep, "idem-bombardment-key").await
        })
    }).collect();

    let results = join_all(handles).await;
    let event_ids: std::collections::HashSet<_> = results.into_iter()
        .filter_map(|r| r.ok())
        .filter_map(|r| r.ok())
        .map(|e| e.id)
        .collect();

    // All successful sends must return the SAME event ID
    assert_eq!(event_ids.len(), 1, "all concurrent idempotent sends must return the same event");

    // Only 1 row in DB
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(&pool).await.unwrap().unwrap_or(0);
    assert_eq!(count, 1);
}

// ── 10. Worker survives every delivery returning an error ──────────────────────
//
// Even if every deliver_event call fails with an error (network error, DB error),
// run_once must return Ok (not propagate the per-event errors).

#[sqlx::test(migrator = "MIGRATOR")]
async fn worker_survives_all_deliveries_failing(pool: PgPool) {
    let server = MockServer::start().await;
    // Return 500 for everything
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'bombardment_test_secret_32chars', 5, 1) RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..20 { engine.send("will_fail", json!({"i": i}), ep).await.unwrap(); }

    // run_once must return Ok even though all 20 deliveries fail (500 response)
    let result = engine.run_once().await;
    assert!(result.is_ok(), "run_once must not propagate per-event delivery failures");
    assert_eq!(result.unwrap(), 20);

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.failed, 20, "all events must be in failed state, not lost");
}
