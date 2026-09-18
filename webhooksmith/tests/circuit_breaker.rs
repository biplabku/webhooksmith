//! Per-endpoint circuit breaker tests.
//!
//! The circuit trips after 5 consecutive failures: the endpoint gets
//! `circuit_open_until` set so the worker skips it. A single success
//! resets `consecutive_failures` and clears the circuit.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::{EventStatus, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

// ── consecutive_failures increments on each failure ──────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn consecutive_failures_increments(pool: PgPool) {
    let server = MockServer::start().await;
    // Always return 500 to trigger failures
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e
        .register(
            &format!("{}/hook", server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();

    // 3 failures
    for _ in 0..3 {
        e.send("test.event", json!({}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
        // Give retry backoff a moment to let scheduled_at pass
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let ep_after = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(ep_after.consecutive_failures, 3);
    // Circuit not open yet (threshold = 5)
    assert!(ep_after.circuit_open_until.is_none());
}

// ── circuit opens after 5 consecutive failures ────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn circuit_opens_after_five_failures(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e
        .register(
            &format!("{}/hook", server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();

    // Drive 5 failures through the worker
    for i in 0..5 {
        e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let ep_after = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(ep_after.consecutive_failures, 5);
    assert!(
        ep_after.circuit_open_until.is_some(),
        "circuit must be open after 5 consecutive failures"
    );
    // The open-until must be in the future
    assert!(ep_after.circuit_open_until.unwrap() > chrono::Utc::now());
}

// ── worker skips circuit-open endpoints ──────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn worker_skips_circuit_open_endpoint(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e
        .register(
            &format!("{}/hook", server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();

    // Trip the circuit (5 failures)
    for i in 0..5 {
        e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Circuit is open — enqueue another event
    let new_event = e
        .send("test.event", json!({"should": "be skipped"}), ep.id)
        .await
        .unwrap();

    // Worker should NOT pick it up (circuit open)
    let claimed = e.run_once().await.unwrap();
    assert_eq!(claimed, 0, "worker must skip circuit-open endpoints");

    // Event must still be pending
    let ev = e.event(new_event.id).await.unwrap().unwrap();
    assert!(
        matches!(ev.status, EventStatus::Pending | EventStatus::Failed),
        "event must not advance while circuit is open"
    );
}

// ── success resets circuit breaker ───────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn success_resets_circuit_breaker(pool: PgPool) {
    let server = MockServer::start().await;

    let e = engine(pool);
    let ep = e
        .register(
            &format!("{}/hook", server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();

    // Manually pump up consecutive_failures to 3 using 500 responses
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(3)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    for i in 0..3 {
        e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let ep_mid = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(ep_mid.consecutive_failures, 3);

    // Now deliver a success
    e.send("test.event", json!({"success": true}), ep.id)
        .await
        .unwrap();
    e.run_once().await.unwrap();

    let ep_after = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(
        ep_after.consecutive_failures, 0,
        "success must reset consecutive_failures"
    );
    assert!(
        ep_after.circuit_open_until.is_none(),
        "success must clear circuit_open_until"
    );
}

// ── independent endpoints: one open circuit doesn't affect another ───────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn circuit_breaker_is_per_endpoint(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let fail_server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&fail_server)
        .await;

    let e = engine(pool);
    let good = e
        .register(
            &format!("{}/hook", server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();
    let bad = e
        .register(
            &format!("{}/hook", fail_server.uri()),
            "circuit_breaker_secret_32chars__",
        )
        .await
        .unwrap();

    // Trip circuit on `bad` endpoint (5 failures)
    for i in 0..5 {
        e.send("test.event", json!({"i": i}), bad.id).await.unwrap();
        e.run_once().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // `good` endpoint should still work fine
    let good_event = e
        .send("test.event", json!({}), good.id)
        .await
        .unwrap();
    e.run_once().await.unwrap();

    let after = e.event(good_event.id).await.unwrap().unwrap();
    assert_eq!(
        after.status,
        EventStatus::Delivered,
        "good endpoint must deliver even when another endpoint's circuit is open"
    );

    let bad_ep = e.endpoint(bad.id).await.unwrap().unwrap();
    assert!(bad_ep.circuit_open_until.is_some(), "bad endpoint must still be circuit-open");
}
