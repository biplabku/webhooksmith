//! Adversarial tests — trying to break the system.
//! Each test probes a boundary, race condition, or edge case.
//! If a test fails it reveals a bug that needs fixing.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{EventStatus, WebhookEngine};
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
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'adversarial_test_secret_32chars') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ── 1. batch_size = 0 panics ──────────────────────────────────────────────────
//
// batch_size=0 → LIMIT 0 → nothing ever delivered. The builder now panics
// to prevent this silent misconfiguration.

#[test]
#[should_panic(expected = "batch_size must be >= 1")]
fn batch_size_zero_panics() {
    // This is NOT an async/sqlx test — we just test the builder validation.
    // We can't call build_sync without a real pool, but the assert fires before
    // the pool is needed (it's in the builder method itself).
    // We have to construct a pool somehow... so we test that the assert message is right
    // by calling batch_size(0) which panics immediately.
    let _ = WebhookEngine::builder().batch_size(0);
}

#[test]
#[should_panic(expected = "batch_size must be >= 1")]
fn batch_size_negative_panics() {
    let _ = WebhookEngine::builder().batch_size(-5);
}

// ── 2. Empty idempotency key ──────────────────────────────────────────────────
//
// An empty string key "" is NOT NULL, so the unique constraint fires on two
// sends with key="" to the same endpoint. But "" is not a meaningful key.
// We should either reject it or treat it as null (no dedup).

#[sqlx::test(migrator = "MIGRATOR")]
async fn empty_idempotency_key_behavior(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    // First send with empty key
    let first = engine.send_idempotent("test", json!({"a": 1}), ep, "").await;

    // Second send with same empty key — what happens?
    let second = engine.send_idempotent("test", json!({"a": 2}), ep, "").await;

    // Both should succeed (second returns the first event due to ON CONFLICT)
    // OR the empty key should be rejected
    match (first, second) {
        (Ok(f), Ok(s)) => {
            // If both succeed, they must return the same event (idempotent)
            assert_eq!(f.id, s.id, "empty key must behave idempotently or be rejected");
        }
        (Ok(_), Err(_)) => {
            // Second call errored — that's acceptable if empty key causes constraint violation
        }
        (Err(_), _) => {
            // Both rejected — also acceptable
        }
    }
}

// ── 3. Negative pagination limit ─────────────────────────────────────────────
//
// SQL LIMIT -1 = unlimited in Postgres. With 10,000 dead events,
// limit=-1 would return all of them. This is a DoS vector.

#[sqlx::test(migrator = "MIGRATOR")]
async fn negative_pagination_limit_is_safe(pool: PgPool) {
    let engine = engine(pool);
    let dead_ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ('http://127.0.0.1:19991/dead', 'adversarial_test_secret_32chars', 1, 0) RETURNING id"
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..5 {
        engine.send("test", json!({"i": i}), dead_ep).await.unwrap();
    }
    engine.run_once().await.unwrap(); // → 5 dead

    // Negative limit is clamped to 0 — returns empty, not all rows.
    let result = engine.dead_events_paged(dead_ep, -1, 0).await.unwrap();
    assert_eq!(result.len(), 0, "negative limit must be clamped to 0, not behave as LIMIT -1 (unlimited)");

    // Negative offset is also clamped to 0 — same as offset=0
    let normal = engine.dead_events_paged(dead_ep, 3, 0).await.unwrap();
    let clamped = engine.dead_events_paged(dead_ep, 3, -5).await.unwrap();
    assert_eq!(normal.len(), clamped.len(), "negative offset must behave as offset=0");
    let normal_ids: Vec<_> = normal.iter().map(|e| e.id).collect();
    let clamped_ids: Vec<_> = clamped.iter().map(|e| e.id).collect();
    assert_eq!(normal_ids, clamped_ids, "negative offset must return same results as offset=0");
}

// ── 4. cleanup_delivered(Duration::ZERO) ─────────────────────────────────────
//
// Duration::ZERO → older_than_secs=0 → WHERE created_at < NOW()
// This removes ALL delivered events, even ones just created milliseconds ago.
// Is this the intended behavior? It removes everything delivered.

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_with_zero_duration_removes_all_delivered(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let engine = engine(pool);
    let ep = live_ep(&engine, &server).await;
    for i in 0..5 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap();

    // All 5 delivered. Cleanup with zero duration.
    let removed = engine.cleanup_delivered(Duration::ZERO).await.unwrap();
    // Document behavior: removes all (including just-delivered)
    println!("cleanup with Duration::ZERO removed {removed} events");
    // This is expected — 0 seconds means older than now = everything delivered before now
    assert_eq!(removed, 5, "Duration::ZERO removes all delivered events (older than now = all)");
}

// ── 5. retry_all_dead does not touch failed events ───────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_all_dead_ignores_failed_events(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(503)).mount(&server).await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'adversarial_test_secret_32chars', 5, 0) RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    for i in 0..3 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }
    engine.run_once().await.unwrap(); // 503s → 3 failed (not dead, max_attempts=5)

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.failed, 3, "events must be in failed state");
    assert_eq!(stats.dead, 0);

    // retry_all_dead must not requeue failed events
    let retried = engine.retry_all_dead(ep).await.unwrap();
    assert_eq!(retried, 0, "retry_all_dead must not touch failed events");

    let stats2 = engine.queue_stats().await.unwrap();
    assert_eq!(stats2.failed, 3, "failed events must remain failed");
    assert_eq!(stats2.pending, 0);
}

// ── 6. Dead events for nonexistent endpoint → empty, not error ───────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn dead_events_for_nonexistent_endpoint_returns_empty(pool: PgPool) {
    let engine = engine(pool);
    let fake_id = uuid::Uuid::new_v4();
    let result = engine.dead_events(fake_id).await;
    assert!(result.is_ok(), "nonexistent endpoint must return Ok, not error");
    assert!(result.unwrap().is_empty(), "must return empty vec");
}

// ── 7. Very long event_type is rejected ───────────────────────────────────────
//
// A 10KB event_type sent as an HTTP header would cause 431 at most servers.
// Cap at 256 bytes.

#[sqlx::test(migrator = "MIGRATOR")]
async fn very_long_event_type_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    let long_type: String = "a".repeat(10_000);
    let result = engine.send(&long_type, json!({}), ep).await;
    assert!(result.is_err(), "event_type > 256 bytes must be rejected");

    // 256 bytes exactly is accepted
    let max_type: String = "a".repeat(256);
    let ok = engine.send(&max_type, json!({}), ep).await;
    assert!(ok.is_ok(), "event_type of exactly 256 bytes must be accepted");

    // 257 bytes is rejected
    let over_type: String = "a".repeat(257);
    let err = engine.send(&over_type, json!({}), ep).await;
    assert!(err.is_err(), "event_type of 257 bytes must be rejected");
}

// ── 8. Concurrent send_idempotent race ────────────────────────────────────────
//
// 20 concurrent tasks all calling send_idempotent with the same key.
// Only 1 event must exist in the DB afterward.

#[sqlx::test(migrator = "MIGRATOR")]
async fn concurrent_idempotent_sends_create_exactly_one_event(pool: PgPool) {
    let engine = engine(pool.clone());
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    let handles: Vec<_> = (0..20)
        .map(|_| {
            let pool2 = pool.clone();
            tokio::spawn(async move {
                let e = WebhookEngine::builder()
                    .pool(pool2)
                    .allow_insecure_urls()
                    .build_sync();
                e.send_idempotent("order.created", json!({"id": 1}), ep, "order-race-test")
                    .await
            })
        })
        .collect();

    let results: Vec<_> = futures::future::join_all(handles).await;
    let successes: Vec<_> = results.into_iter()
        .filter_map(|r| r.ok())
        .filter_map(|r| r.ok())
        .collect();

    // All successful calls must return the same event ID
    let ids: std::collections::HashSet<_> = successes.iter().map(|e| e.id).collect();
    assert_eq!(ids.len(), 1, "all concurrent sends must return the same event ID");

    // Only 1 event in the DB
    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(count, 1, "only 1 event must exist despite 20 concurrent sends");
}

// ── 9. send_idempotent after event is delivered returns delivered event ────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn idempotent_send_after_delivery_returns_delivered_event(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let engine = engine(pool);
    let ep = live_ep(&engine, &server).await;

    // First send + delivery
    let first = engine.send_idempotent("order.created", json!({"id": 1}), ep, "key-123").await.unwrap();
    engine.run_once().await.unwrap();

    let delivered = engine.event(first.id).await.unwrap().unwrap();
    assert_eq!(delivered.status, EventStatus::Delivered);

    // Second send with same key — must return the ALREADY DELIVERED event
    let second = engine.send_idempotent("order.created", json!({"id": 1}), ep, "key-123").await.unwrap();
    assert_eq!(second.id, first.id, "same key must return same event even after delivery");
    assert_eq!(second.status, EventStatus::Delivered, "returned event must show delivered status");

    // Worker must not see a new event to deliver
    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0, "no new delivery must happen for an already-delivered idempotent event");
}

// ── 10. events_by_status(Delivering) mid-cycle ───────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn events_by_status_delivering_returns_in_flight_events(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;
    let event = engine.send("test", json!({}), ep).await.unwrap();

    // Manually claim (simulate in-progress delivery)
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW() WHERE id=$1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    let delivering = engine.events_by_status(ep, EventStatus::Delivering, 100, 0).await.unwrap();
    assert_eq!(delivering.len(), 1);
    assert_eq!(delivering[0].id, event.id);
}

// ── 11. cleanup_delivered does not remove 'delivering' events ─────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn cleanup_does_not_remove_delivering_events(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;
    let event = engine.send("test", json!({}), ep).await.unwrap();

    // Simulate event being delivered (set to 'delivering', backdated)
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW()-INTERVAL '10 days', created_at=NOW()-INTERVAL '10 days' WHERE id=$1",
        event.id
    )
    .execute(engine.pool())
    .await
    .unwrap();

    // Cleanup should NOT remove 'delivering' events — they're still in-flight
    let removed = engine.cleanup_delivered(Duration::from_secs(1)).await.unwrap();
    assert_eq!(removed, 0, "cleanup must not delete events in 'delivering' state");

    let still_there = engine.event(event.id).await.unwrap();
    assert!(still_there.is_some(), "delivering event must not be cleaned up");
}

// ── 12. max_attempts=1 + network timeout → DLQ after one attempt ──────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn single_max_attempt_plus_timeout_goes_to_dlq(pool: PgPool) {
    let server = MockServer::start().await;
    // Endpoint takes 300ms — exceeds 100ms http_timeout
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_millis(300)))
        .mount(&server)
        .await;

    let engine = WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .http_timeout(Duration::from_millis(100))
        .build_sync();

    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'adversarial_test_secret_32chars', 1, 0) RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    let event = engine.send("test", json!({}), ep).await.unwrap();
    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(
        updated.status,
        EventStatus::Dead,
        "timeout with max_attempts=1 must go directly to DLQ"
    );

    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1, "exactly one attempt must be recorded");
    assert!(!log[0].success);
}

// ── 13. Payload exactly at 1MB boundary ──────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn payload_at_exactly_1mb_is_accepted(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    // "x" repeated to fill exactly 1MB (accounting for JSON overhead)
    // json!({"d": "x".repeat(N)}) → '{"d":"' (6) + N + '"}' (2) = N+8 bytes
    let data = "x".repeat(1_048_576 - 8);
    let payload = json!({"d": data});
    let size = payload.to_string().len();
    assert_eq!(size, 1_048_576, "payload must be exactly 1MB");

    let result = engine.send("test.event", payload, ep).await;
    assert!(result.is_ok(), "exactly 1MB payload must be accepted");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn payload_at_1mb_plus_one_byte_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    let data = "x".repeat(1_048_576 - 8 + 1);
    let payload = json!({"d": data});
    assert_eq!(payload.to_string().len(), 1_048_577);

    let result = engine.send("test.event", payload, ep).await;
    assert!(result.is_err(), "1MB+1 byte payload must be rejected");
}

// ── 14. Queue stats consistency under concurrent modifications ────────────────
//
// Run the worker and check queue stats simultaneously — stats must be consistent.

#[sqlx::test(migrator = "MIGRATOR")]
async fn queue_stats_are_consistent(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

    let engine = engine(pool);
    let ep = live_ep(&engine, &server).await;
    for i in 0..10 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }

    engine.run_once().await.unwrap();

    let stats = engine.queue_stats().await.unwrap();
    let total = stats.pending + stats.delivering + stats.failed + stats.dead + stats.delivered;
    assert_eq!(total, 10, "sum of all status counts must equal total events");
}

// ── 15. Whitespace-only event_type variations ─────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn whitespace_event_type_variants_all_rejected(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    let bad_types = [" ", "\t", "\n", "\r\n", "  \t  "];
    for t in &bad_types {
        let result = engine.send(t, json!({}), ep).await;
        assert!(result.is_err(), "whitespace event_type '{t:?}' must be rejected");
    }
}

// ── 16. list_endpoints after delete shows only remaining ─────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn list_endpoints_reflects_deletions(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let ep1 = engine.register(&format!("{}/a", server.uri()), "adversarial_test_secret_32chars").await.unwrap();
    let ep2 = engine.register(&format!("{}/b", server.uri()), "adversarial_test_secret_32chars").await.unwrap();
    let ep3 = engine.register(&format!("{}/c", server.uri()), "adversarial_test_secret_32chars").await.unwrap();

    assert_eq!(engine.list_endpoints().await.unwrap().len(), 3);

    engine.delete_endpoint(ep2.id).await.unwrap();

    let remaining = engine.list_endpoints().await.unwrap();
    assert_eq!(remaining.len(), 2);
    let ids: Vec<_> = remaining.iter().map(|e| e.id).collect();
    assert!(ids.contains(&ep1.id));
    assert!(ids.contains(&ep3.id));
    assert!(!ids.contains(&ep2.id));
}

// ── 17. broadcast idempotent + endpoint deleted mid-operation ─────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_idempotent_after_endpoint_deleted_does_not_create_events(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    // First broadcast
    let first = engine.broadcast_idempotent("order.created", json!({}), "key-del-test").await.unwrap();
    assert_eq!(first.len(), 1);

    // Delete the endpoint
    engine.delete_endpoint(ep).await.unwrap();

    // Second broadcast with same key — no endpoints, no events
    let second = engine.broadcast_idempotent("order.created", json!({}), "key-del-test").await.unwrap();
    assert_eq!(second.len(), 0, "broadcast to deleted endpoint must create no events");

    let count: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool()).await.unwrap().unwrap_or(0);
    assert_eq!(count, 0, "all events must be gone after endpoint deleted");
}

// ── Bug: initial_delay_ms = 0 or negative rejected ────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn initial_delay_ms_zero_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let result = engine.register_with(hooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "adversarial_test_secret_32chars".into(),
        description: None,
        max_attempts: None,
        initial_delay_ms: Some(0), // must be rejected: 0 = no backoff
    }).await;
    assert!(result.is_err(), "initial_delay_ms=0 must be rejected");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn initial_delay_ms_negative_is_rejected(pool: PgPool) {
    let engine = engine(pool);
    let result = engine.register_with(hooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "adversarial_test_secret_32chars".into(),
        description: None,
        max_attempts: None,
        initial_delay_ms: Some(-1), // must be rejected: wraps to u32::MAX on cast
    }).await;
    assert!(result.is_err(), "initial_delay_ms=-1 must be rejected");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn initial_delay_ms_one_is_accepted(pool: PgPool) {
    let engine = engine(pool);
    let result = engine.register_with(hooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "adversarial_test_secret_32chars".into(),
        description: None,
        max_attempts: None,
        initial_delay_ms: Some(1), // minimum valid value
    }).await;
    assert!(result.is_ok(), "initial_delay_ms=1 must be accepted");
}

// ── Bug: IPv6 private addresses not fully blocked ──────────────────────────────

#[test]
fn ipv6_unique_local_is_private() {
    use hooksmith::is_private_ip;
    use std::net::IpAddr;

    let blocked_ipv6 = [
        "fd00::1",      // unique local fc00::/7
        "fc00::1",      // unique local fc00::/7
        "fe80::1",      // link-local fe80::/10
        "fe80::cafe",   // link-local
        "::ffff:10.0.0.1",  // IPv4-mapped private
        "::ffff:192.168.1.1", // IPv4-mapped private
        "::1",          // loopback
        "::",           // unspecified
    ];

    for addr_str in &blocked_ipv6 {
        let ip: IpAddr = addr_str.parse().unwrap();
        assert!(
            is_private_ip(ip) || ip.is_loopback() || ip.is_unspecified(),
            "{addr_str} must be identified as private/restricted"
        );
    }
}

#[test]
fn ipv6_public_addresses_are_allowed() {
    use hooksmith::is_private_ip;
    use std::net::IpAddr;

    let allowed_ipv6 = [
        "2001:4860:4860::8888", // Google DNS
        "2606:4700:4700::1111", // Cloudflare DNS
        "2620:fe::fe",          // Sprint DNS
    ];

    for addr_str in &allowed_ipv6 {
        let ip: IpAddr = addr_str.parse().unwrap();
        assert!(
            !is_private_ip(ip),
            "{addr_str} must not be identified as private"
        );
    }
}

#[test]
fn ipv4_cgnat_range_is_private() {
    use hooksmith::is_private_ip;
    use std::net::IpAddr;
    // 100.64.0.0/10 (RFC 6598 shared address space / carrier-grade NAT)
    let cgnat: IpAddr = "100.64.0.1".parse().unwrap();
    assert!(is_private_ip(cgnat), "100.64.0.1 (CGNAT) must be blocked");
    let public: IpAddr = "100.128.0.1".parse().unwrap(); // outside CGNAT range
    assert!(!is_private_ip(public), "100.128.0.1 must be public");
}

// ── Control characters in event_type cause header injection / permanent failure ──

#[sqlx::test(migrator = "MIGRATOR")]
async fn control_characters_in_event_type_are_rejected(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep = live_ep(&engine, &server).await;

    // These would cause reqwest to reject the HTTP request at send time
    // if stored — making the event permanently stuck failing.
    let bad_types = [
        "order\r\ncreated",      // CRLF injection
        "order\ncreated",        // newline
        "order\rcreated",        // carriage return
        "order\x00created",      // null byte
        "order\x01created",      // SOH control char
        "\x7forder",             // DEL
    ];

    for bad in &bad_types {
        let result = engine.send(bad, json!({}), ep).await;
        assert!(
            result.is_err(),
            "event_type {:?} containing control char must be rejected at enqueue",
            bad
        );
    }

    // Printable non-ASCII is fine (valid UTF-8 HTTP header value)
    let ok = engine.send("order.créé", json!({}), ep).await;
    assert!(ok.is_ok(), "non-ASCII printable event_type must be accepted");
}
