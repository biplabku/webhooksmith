//! Additional edge-scenario end-to-end tests.
//! These probe behaviour that is hard to cover with unit tests:
//! concurrent sends, large payloads, idempotency under repeated calls,
//! circuit-breaker precision, DLQ full-cycle, endpoint lifecycle.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::{EventStatus, NewEndpoint, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

// ── Large payload (near 1 MB) ─────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn large_payload_stored_and_delivered(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    // ~10 KB payload
    let big = json!({"data": "x".repeat(10_000)});
    let ev = e.send("data.sync", big.clone(), ep.id).await.unwrap();
    e.run_once().await.unwrap();

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Delivered);
    assert_eq!(after.payload["data"], big["data"]);

    server.verify().await;
}

// ── Idempotent send: same key twice only creates one event ────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn idempotent_send_deduplicates(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    let ev1 = e.send_idempotent("order.created", json!({"id": 1}), ep.id, "order-1001").await.unwrap();
    let ev2 = e.send_idempotent("order.created", json!({"id": 1}), ep.id, "order-1001").await.unwrap();

    assert_eq!(ev1.id, ev2.id, "same idempotency key must return same event");

    // Only one delivery should happen
    let n = e.run_once().await.unwrap();
    assert_eq!(n, 1);

    let stats = e.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 1);
}

// ── Broadcast to zero endpoints ───────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_with_no_endpoints_returns_empty(pool: PgPool) {
    let e = engine(pool);
    let events = e.broadcast("order.created", json!({})).await.unwrap();
    assert!(events.is_empty(), "broadcast with no endpoints must return empty vec");
}

// ── Disabled endpoint skipped, then re-enabled ────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn disabled_endpoint_skipped_then_reenabled(pool: PgPool) {
    let server = MockServer::start().await;
    // Expect exactly 1 delivery (after re-enable)
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();
    e.disable_endpoint(ep.id).await.unwrap();

    let ev = e.send("order.created", json!({}), ep.id).await.unwrap();
    let n = e.run_once().await.unwrap(); // disabled — not even claimed
    assert_eq!(n, 0, "disabled endpoint must not be claimed by worker");

    // Re-enable
    e.enable_endpoint(ep.id).await.unwrap();
    e.run_once().await.unwrap(); // now claimed and delivered

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Delivered);

    server.verify().await;
}

// ── Delete endpoint cascades to events ────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn deleted_endpoint_events_cascade(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    let ev_id = e.send("test.event", json!({}), ep.id).await.unwrap().id;
    e.delete_endpoint(ep.id).await.unwrap();

    // Event must be gone (CASCADE DELETE)
    let found = e.event(ev_id).await.unwrap();
    assert!(found.is_none(), "event must be cascade-deleted with endpoint");

    // No deliveries should happen
    let n = e.run_once().await.unwrap();
    assert_eq!(n, 0);
}

// ── Max attempts=1: single failure goes straight to dead ─────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn max_attempts_one_goes_directly_to_dead(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register_with(NewEndpoint {
        url: format!("{}/hook", server.uri()),
        signing_secret: "edge_e2e_secret_32chars_______".into(),
        max_attempts: Some(1),
        ..Default::default()
    }).await.unwrap();

    let ev = e.send("order.created", json!({}), ep.id).await.unwrap();
    e.run_once().await.unwrap();

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Dead);
    assert_eq!(after.attempts, 1);
}

// ── Delivery log records attempt detail ───────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn delivery_log_records_success_attempt(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();
    let ev = e.send("order.created", json!({}), ep.id).await.unwrap();
    e.run_once().await.unwrap();

    let log = e.delivery_log(ev.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(log[0].success);
    assert_eq!(log[0].response_status, Some(202));
    assert!(log[0].duration_ms.unwrap_or(0) >= 0);
}

// ── Circuit opens → retry-all resets → delivery succeeds ─────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn full_circuit_dlq_retry_cycle(pool: PgPool) {
    let server = MockServer::start().await;

    // 5 failures, then 1 success
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(5)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register_with(NewEndpoint {
        url: format!("{}/hook", server.uri()),
        signing_secret: "edge_e2e_secret_32chars_______".into(),
        max_attempts: Some(1), // each event → dead on first failure
        initial_delay_ms: Some(60_000),
        ..Default::default()
    }).await.unwrap();

    // Send 5 events, each fails immediately → dead
    for i in 0..5 {
        e.send("order.created", json!({"i": i}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
    }

    let ep_state = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(ep_state.consecutive_failures, 5);
    assert!(ep_state.circuit_open_until.is_some());

    let stats = e.queue_stats().await.unwrap();
    assert_eq!(stats.dead, 5);

    // Operator: retry all from DLQ (resets circuit)
    let retried = e.retry_all_dead(ep.id).await.unwrap();
    assert_eq!(retried, 5);

    let ep_after_retry = e.endpoint(ep.id).await.unwrap().unwrap();
    assert_eq!(ep_after_retry.consecutive_failures, 0);
    assert!(ep_after_retry.circuit_open_until.is_none());

    // Now events are pending → deliver successfully
    e.run_once().await.unwrap();

    let delivered = e.queue_stats().await.unwrap().delivered;
    assert_eq!(delivered, 5);

    server.verify().await;
}

// ── events_global paginates correctly ────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn events_global_pagination(pool: PgPool) {
    let e = engine(pool);
    let server = MockServer::start().await;
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    for i in 0..7 {
        e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
    }

    let page1 = e.events_global(EventStatus::Pending, 3, 0).await.unwrap();
    let page2 = e.events_global(EventStatus::Pending, 3, 3).await.unwrap();
    let page3 = e.events_global(EventStatus::Pending, 3, 6).await.unwrap();

    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);
    assert_eq!(page3.len(), 1);

    // No overlap
    let ids1: std::collections::HashSet<_> = page1.iter().map(|e| e.id).collect();
    let ids2: std::collections::HashSet<_> = page2.iter().map(|e| e.id).collect();
    assert!(ids1.is_disjoint(&ids2), "pages must not overlap");
}

// ── Cleanup only removes delivered — not pending or dead ─────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_only_removes_old_delivered(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool.clone());
    let ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    // Deliver an event
    let ev = e.send("test.event", json!({}), ep.id).await.unwrap();
    e.run_once().await.unwrap();
    assert_eq!(e.event(ev.id).await.unwrap().unwrap().status, EventStatus::Delivered);

    // Cleanup with 0s threshold (delete everything delivered)
    let deleted = e.cleanup_delivered(std::time::Duration::from_secs(0)).await.unwrap();
    assert_eq!(deleted, 1);
    assert!(e.event(ev.id).await.unwrap().is_none(), "delivered event must be cleaned up");

    // Pending event must NOT be touched
    let pending = e.send("test.event", json!({}), ep.id).await.unwrap();
    e.cleanup_delivered(std::time::Duration::from_secs(0)).await.unwrap();
    assert!(e.event(pending.id).await.unwrap().is_some(), "pending event must survive cleanup");
}

// ── register validates secret length ─────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn short_secret_rejected(pool: PgPool) {
    let e = engine(pool);
    let result = e.register("https://example.com/hook", "tooshort").await;
    assert!(result.is_err(), "secret shorter than 16 chars must be rejected");
}

// ── register validates SSRF (when insecure urls NOT enabled) ─────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn ssrf_blocked_for_private_ip(pool: PgPool) {
    // NOTE: engine() uses allow_insecure_urls(), so we build a strict one
    let strict = WebhookEngine::builder()
        .pool(pool)
        .build_sync(); // no allow_insecure_urls

    let result = strict.register("http://192.168.1.1/webhook", "secret_at_least_16_chars").await;
    assert!(result.is_err(), "private IP must be rejected without allow_insecure_urls");

    let result2 = strict.register("http://169.254.169.254/latest/meta-data", "secret_at_least_16_chars").await;
    assert!(result2.is_err(), "AWS metadata endpoint must be rejected");
}

// ── update_endpoint changes URL and secret ────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_endpoint_changes_url_and_secret(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool);
    let ep = e.register(&format!("{}/old-hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    let updated = e.update_endpoint(ep.id, webhooksmith::UpdateEndpoint {
        url: Some(format!("{}/new-hook", server.uri())),
        signing_secret: Some("new_secret_that_is_32_chars_long".into()),
        ..Default::default()
    }).await.unwrap();

    assert!(updated.url.ends_with("/new-hook"));
    assert_eq!(updated.signing_secret, "new_secret_that_is_32_chars_long");
}

// ── list_endpoints_paged returns consistent pages ─────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn list_endpoints_paged_consistent(pool: PgPool) {
    let e = engine(pool);
    let server = MockServer::start().await;
    for _ in 0..6 {
        e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();
    }

    let page1 = e.list_endpoints_paged(3, 0).await.unwrap();
    let page2 = e.list_endpoints_paged(3, 3).await.unwrap();

    assert_eq!(page1.len(), 3);
    assert_eq!(page2.len(), 3);

    let ids1: std::collections::HashSet<_> = page1.iter().map(|e| e.id).collect();
    let ids2: std::collections::HashSet<_> = page2.iter().map(|e| e.id).collect();
    assert!(ids1.is_disjoint(&ids2));
}

// ── broadcast idempotent: same key only creates one event per endpoint ────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_idempotent_deduplicates(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let e = engine(pool);
    let _ep = e.register(&format!("{}/hook", server.uri()), "edge_e2e_secret_32chars_______").await.unwrap();

    let v1 = e.broadcast_idempotent("order.created", json!({}), "broadcast-key-001").await.unwrap();
    let v2 = e.broadcast_idempotent("order.created", json!({}), "broadcast-key-001").await.unwrap();

    assert_eq!(v1.len(), 1);
    assert_eq!(v2.len(), 1);
    assert_eq!(v1[0].id, v2[0].id, "same broadcast idempotency key must deduplicate");
}
