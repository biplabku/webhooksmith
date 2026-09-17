//! Full API exercise — every public method called against real Postgres + real HTTP.
//! This test cannot pass by luck or partial mocking: every assertion reflects
//! actual DB state and actual HTTP traffic.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{
    error::HooksmithError, EventStatus, NewEndpoint, UpdateEndpoint, WebhookEngine,
};
use serde_json::json;
use sqlx::PgPool;
use std::time::Duration;
use wiremock::{
    matchers::{header_exists, method, path},
    Mock, MockServer, ResponseTemplate,
};

// ── Setup helpers ─────────────────────────────────────────────────────────────

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

// ── Full API walkthrough ──────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_endpoint_crud(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    // ── register() ────────────────────────────────────────────────────────────
    let ep1 = engine
        .register(&format!("{}/hook", server.uri()), "exercise_test_secret_32chars_a")
        .await
        .unwrap();
    assert!(!ep1.id.is_nil());
    assert!(ep1.enabled);
    assert_eq!(ep1.max_attempts, 10);
    assert_eq!(ep1.initial_delay_ms, 1000);
    assert_eq!(ep1.created_at.timestamp_millis(), ep1.updated_at.timestamp_millis());

    // ── register_with() ───────────────────────────────────────────────────────
    let ep2 = engine
        .register_with(NewEndpoint {
            url: format!("{}/hook2", server.uri()),
            signing_secret: "exercise_test_secret_32chars_b".into(),
            description: Some("endpoint two".into()),
            max_attempts: Some(5),
            initial_delay_ms: Some(500),
        })
        .await
        .unwrap();
    assert_eq!(ep2.max_attempts, 5);
    assert_eq!(ep2.initial_delay_ms, 500);
    assert_eq!(ep2.description, Some("endpoint two".into()));

    // ── endpoint(id) ──────────────────────────────────────────────────────────
    let fetched = engine.endpoint(ep1.id).await.unwrap().unwrap();
    assert_eq!(fetched.id, ep1.id);
    assert_eq!(fetched.url, ep1.url);

    let missing = engine.endpoint(uuid::Uuid::new_v4()).await.unwrap();
    assert!(missing.is_none());

    // ── list_endpoints() ──────────────────────────────────────────────────────
    let all = engine.list_endpoints().await.unwrap();
    assert_eq!(all.len(), 2);

    // ── list_endpoints_paged() ────────────────────────────────────────────────
    let page1 = engine.list_endpoints_paged(1, 0).await.unwrap();
    let page2 = engine.list_endpoints_paged(1, 1).await.unwrap();
    assert_eq!(page1.len(), 1);
    assert_eq!(page2.len(), 1);
    assert_ne!(page1[0].id, page2[0].id);

    // ── update_endpoint() ─────────────────────────────────────────────────────
    sqlx::query!("SELECT pg_sleep(0.01)").execute(engine.pool()).await.unwrap();
    let updated = engine
        .update_endpoint(ep1.id, UpdateEndpoint {
            description: Some(Some("updated description".into())),
            max_attempts: Some(7),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(updated.description, Some("updated description".into()));
    assert_eq!(updated.max_attempts, 7);
    assert!(updated.updated_at > updated.created_at, "updated_at must advance");

    // ── disable_endpoint() / enable_endpoint() ────────────────────────────────
    let disabled = engine.disable_endpoint(ep1.id).await.unwrap();
    assert!(!disabled.enabled);

    let enabled = engine.enable_endpoint(ep1.id).await.unwrap();
    assert!(enabled.enabled);

    // ── delete_endpoint() ─────────────────────────────────────────────────────
    engine.delete_endpoint(ep2.id).await.unwrap();
    let after_delete = engine.list_endpoints().await.unwrap();
    assert_eq!(after_delete.len(), 1, "ep2 must be gone");

    // EndpointNotFound for already-deleted
    let err = engine.delete_endpoint(ep2.id).await.unwrap_err();
    assert!(matches!(err, HooksmithError::EndpointNotFound(_)));

    // Validation errors
    let bad = engine.register("not-a-url", "short").await;
    assert!(bad.is_err());

    let bad2 = engine
        .register_with(NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "exercise_test_secret_32chars_c".into(),
            description: None,
            max_attempts: Some(0),
            initial_delay_ms: None,
        })
        .await;
    assert!(bad2.is_err(), "max_attempts=0 must be rejected");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_send_and_deliver(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(header_exists("x-hooksmith-signature"))
        .and(header_exists("x-hooksmith-timestamp"))
        .and(header_exists("x-hooksmith-event-id"))
        .and(header_exists("x-hooksmith-event-type"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ack"))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep = engine
        .register(&format!("{}/hook", server.uri()), "exercise_test_secret_32chars_a")
        .await
        .unwrap();

    // ── send() ────────────────────────────────────────────────────────────────
    let ev = engine
        .send("order.created", json!({"order_id": 1001}), ep.id)
        .await
        .unwrap();
    assert_eq!(ev.status, EventStatus::Pending);
    assert_eq!(ev.attempts, 0);
    assert_eq!(ev.event_type, "order.created");
    assert_eq!(ev.payload, json!({"order_id": 1001}));
    assert!(ev.idempotency_key.is_none());

    // ── run_once() ────────────────────────────────────────────────────────────
    let delivered_count = engine.run_once().await.unwrap();
    assert_eq!(delivered_count, 1);

    // ── event() ───────────────────────────────────────────────────────────────
    let after = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Delivered);
    assert_eq!(after.attempts, 1);

    // ── delivery_log() ────────────────────────────────────────────────────────
    let log = engine.delivery_log(ev.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(log[0].success);
    assert_eq!(log[0].response_status, Some(200));
    assert!(log[0].duration_ms.unwrap_or(0) >= 0);
    assert!(log[0].error.is_none());
    let body = log[0].response_body.as_deref().unwrap_or("");
    assert_eq!(body, "ack");

    // Verify correct headers were sent
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(reqs[0].headers.contains_key("x-hooksmith-signature"));
    assert!(reqs[0].headers.contains_key("x-hooksmith-timestamp"));
    assert_eq!(
        reqs[0].headers.get("x-hooksmith-event-type").unwrap().to_str().unwrap(),
        "order.created"
    );
    assert_eq!(
        reqs[0].headers.get("x-hooksmith-event-id").unwrap().to_str().unwrap(),
        ev.id.to_string()
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_transactional_outbox(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep = engine
        .register(&format!("{}/hook", server.uri()), "exercise_test_secret_32chars_a")
        .await
        .unwrap();

    // ── send_in_tx() — commit path ────────────────────────────────────────────
    let mut tx = engine.pool().begin().await.unwrap();
    let ev = engine
        .send_in_tx("payment.captured", json!({"amount": 99}), ep.id, &mut tx)
        .await
        .unwrap();
    // Event is not visible outside the transaction yet
    let not_yet = engine.event(ev.id).await.unwrap();
    assert!(not_yet.is_none(), "event must not be visible before commit");
    tx.commit().await.unwrap();
    // Now it's visible
    let visible = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(visible.status, EventStatus::Pending);

    // ── send_in_tx() — rollback path ──────────────────────────────────────────
    let mut tx2 = engine.pool().begin().await.unwrap();
    let ev2 = engine
        .send_in_tx("payment.failed", json!({"amount": 0}), ep.id, &mut tx2)
        .await
        .unwrap();
    tx2.rollback().await.unwrap();
    let gone = engine.event(ev2.id).await.unwrap();
    assert!(gone.is_none(), "event must be gone after rollback");

    engine.run_once().await.unwrap();
    let delivered = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(delivered.status, EventStatus::Delivered);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_idempotency(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1) // must only receive ONE delivery despite two sends
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep = engine
        .register(&format!("{}/hook", server.uri()), "exercise_test_secret_32chars_a")
        .await
        .unwrap();

    // ── send_idempotent() ─────────────────────────────────────────────────────
    let first = engine
        .send_idempotent("order.created", json!({"id": 1}), ep.id, "order-1001")
        .await
        .unwrap();
    assert_eq!(first.idempotency_key, Some("order-1001".into()));

    let second = engine
        .send_idempotent("order.created", json!({"id": 1}), ep.id, "order-1001")
        .await
        .unwrap();
    assert_eq!(first.id, second.id, "same key must return same event");

    // Deliver — only 1 HTTP call despite 2 sends
    engine.run_once().await.unwrap();
    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 1);
    server.verify().await;

    // ── send_idempotent_in_tx() ───────────────────────────────────────────────
    let mut tx = engine.pool().begin().await.unwrap();
    let _ev = engine
        .send_idempotent_in_tx("order.shipped", json!({"id": 2}), ep.id, "order-1002", &mut tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    // Key is reusable after rollback
    let reuse = engine
        .send_idempotent("order.shipped", json!({"id": 2}), ep.id, "order-1002")
        .await
        .unwrap();
    assert_eq!(reuse.idempotency_key, Some("order-1002".into()));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_broadcast(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);

    // Register 3 endpoints
    for i in 0..3 {
        engine
            .register(
                &format!("{}/hook{i}", server.uri()),
                "exercise_test_secret_32chars_a",
            )
            .await
            .unwrap();
    }

    // ── broadcast() ───────────────────────────────────────────────────────────
    let events = engine
        .broadcast("shipment.update", json!({"status": "in_transit"}))
        .await
        .unwrap();
    assert_eq!(events.len(), 3, "3 endpoints must each receive an event");
    assert!(events.iter().all(|e| e.status == EventStatus::Pending));
    assert!(events.iter().all(|e| e.event_type == "shipment.update"));

    // ── broadcast_in_tx() — rollback ─────────────────────────────────────────
    let mut tx = engine.pool().begin().await.unwrap();
    let tx_events = engine
        .broadcast_in_tx("shipment.update", json!({"status": "rollback"}), &mut tx)
        .await
        .unwrap();
    assert_eq!(tx_events.len(), 3);
    tx.rollback().await.unwrap();
    // The rolled-back events must not exist
    for e in &tx_events {
        assert!(engine.event(e.id).await.unwrap().is_none());
    }

    // ── broadcast_idempotent() ────────────────────────────────────────────────
    let idem1 = engine
        .broadcast_idempotent("order.closed", json!({"id": 99}), "close-99")
        .await
        .unwrap();
    assert_eq!(idem1.len(), 3);
    let ids1: std::collections::HashSet<_> = idem1.iter().map(|e| e.id).collect();

    let idem2 = engine
        .broadcast_idempotent("order.closed", json!({"id": 99}), "close-99")
        .await
        .unwrap();
    let ids2: std::collections::HashSet<_> = idem2.iter().map(|e| e.id).collect();
    assert_eq!(ids1, ids2, "same key must return same events");

    let total: i64 = sqlx::query_scalar!("SELECT COUNT(*) FROM webhook_events")
        .fetch_one(engine.pool())
        .await
        .unwrap()
        .unwrap_or(0);
    assert_eq!(total, 6, "3 from broadcast + 3 from broadcast_idempotent");

    engine.run_once().await.unwrap();
    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 6);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_retry_and_dlq(pool: PgPool) {
    let server = MockServer::start().await;
    // 3 failures (max_attempts=3) → Dead. Then on retry, 200 → Delivered.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .up_to_n_times(3)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms)
         VALUES ($1, 'exercise_test_secret_32chars_a', 3, 1)
         RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    let ev = engine.send("test.retry", json!({}), ep).await.unwrap();

    // Attempt 1: fails → status=Failed
    engine.run_once().await.unwrap();
    sqlx::query!("UPDATE webhook_events SET scheduled_at=NOW() WHERE id=$1", ev.id)
        .execute(engine.pool()).await.unwrap();

    // ── events_by_status() ────────────────────────────────────────────────────
    let failed = engine.events_by_status(ep, EventStatus::Failed, 10, 0).await.unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].attempts, 1);

    // Attempt 2: fails → status=Failed
    engine.run_once().await.unwrap();
    sqlx::query!("UPDATE webhook_events SET scheduled_at=NOW() WHERE id=$1", ev.id)
        .execute(engine.pool()).await.unwrap();

    // ── events_global() ───────────────────────────────────────────────────────
    let global_failed = engine.events_global(EventStatus::Failed, 10, 0).await.unwrap();
    assert_eq!(global_failed.len(), 1);
    assert_eq!(global_failed[0].id, ev.id);

    // Attempt 3: fails → status=Dead (max_attempts=3)
    engine.run_once().await.unwrap();
    let dead_ev = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(dead_ev.status, EventStatus::Dead);
    assert_eq!(dead_ev.attempts, 3);

    // ── dead_events() ─────────────────────────────────────────────────────────
    let dead_list = engine.dead_events(ep).await.unwrap();
    assert_eq!(dead_list.len(), 1);
    assert_eq!(dead_list[0].id, ev.id);

    // ── dead_events_paged() ───────────────────────────────────────────────────
    let dead_paged = engine.dead_events_paged(ep, 5, 0).await.unwrap();
    assert_eq!(dead_paged.len(), 1);

    // ── delivery_log() — 3 attempts ───────────────────────────────────────────
    let log = engine.delivery_log(ev.id).await.unwrap();
    assert_eq!(log.len(), 3);
    assert!(log.iter().all(|a| !a.success));
    assert!(log.iter().all(|a| a.response_status == Some(500)));

    // ── retry_dead() — wrong state after delivery ─────────────────────────────
    let bad_retry = engine.retry_dead(uuid::Uuid::new_v4()).await;
    assert!(matches!(bad_retry, Err(HooksmithError::EventNotFound(_))));

    // ── retry_dead() — correct ────────────────────────────────────────────────
    engine.retry_dead(ev.id).await.unwrap();
    let requeued = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(requeued.status, EventStatus::Pending);
    assert_eq!(requeued.attempts, 0);

    // 4th attempt: succeeds (mock now returns 200)
    engine.run_once().await.unwrap();
    let final_ev = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(final_ev.status, EventStatus::Delivered);

    // ── retry_all_dead() ──────────────────────────────────────────────────────
    // Add 3 more events to DLQ
    for _ in 0..3 {
        let e = engine.send("dlq.test", json!({}), ep).await.unwrap();
        // Force directly to dead for speed
        sqlx::query!(
            "UPDATE webhook_events SET status='dead', attempts=3 WHERE id=$1", e.id
        )
        .execute(engine.pool()).await.unwrap();
    }
    let requeued_count = engine.retry_all_dead(ep).await.unwrap();
    assert_eq!(requeued_count, 3);
    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.pending, 3);
    assert_eq!(stats.dead, 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_queue_stats_and_cleanup(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'exercise_test_secret_32chars_a') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool()).await.unwrap();

    // ── queue_stats() — empty ─────────────────────────────────────────────────
    let empty = engine.queue_stats().await.unwrap();
    assert_eq!(empty.pending, 0);
    assert_eq!(empty.delivered, 0);
    assert_eq!(empty.failed, 0);
    assert_eq!(empty.dead, 0);
    assert_eq!(empty.delivering, 0);

    // Enqueue 5 events
    for i in 0..5 { engine.send("test", json!({"i": i}), ep).await.unwrap(); }

    let pending_stats = engine.queue_stats().await.unwrap();
    assert_eq!(pending_stats.pending, 5);

    // Deliver all
    engine.run_once().await.unwrap();
    let delivered_stats = engine.queue_stats().await.unwrap();
    assert_eq!(delivered_stats.delivered, 5);
    assert_eq!(delivered_stats.pending, 0);

    // QueueStats total equals event count
    let total = delivered_stats.pending + delivered_stats.delivering
        + delivered_stats.failed + delivered_stats.dead + delivered_stats.delivered;
    assert_eq!(total, 5);

    // ── cleanup_delivered() — recent: 0 removed ───────────────────────────────
    let removed = engine.cleanup_delivered(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed, 0, "recent delivered events must not be cleaned up");

    // Backdate and clean
    sqlx::query!("UPDATE webhook_events SET created_at=NOW()-INTERVAL '2 days' WHERE status='delivered'")
        .execute(engine.pool()).await.unwrap();
    let removed2 = engine.cleanup_delivered(Duration::from_secs(86_400)).await.unwrap();
    assert_eq!(removed2, 5, "old delivered events must be removed");
    let after = engine.queue_stats().await.unwrap();
    assert_eq!(after.delivered, 0);

    // ── cleanup_dead() ────────────────────────────────────────────────────────
    sqlx::query!(
        "INSERT INTO webhook_events (endpoint_id, event_type, payload, status, created_at)
         VALUES ($1, 'dead.test', '{}', 'dead', NOW()-INTERVAL '10 days')",
        ep,
    )
    .execute(engine.pool()).await.unwrap();

    let dead_removed = engine.cleanup_dead(Duration::from_secs(7 * 86_400)).await.unwrap();
    assert_eq!(dead_removed, 1);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_graceful_shutdown(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'exercise_test_secret_32chars_a') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool()).await.unwrap();

    for i in 0..5 { engine.send("shutdown.test", json!({"i": i}), ep).await.unwrap(); }

    // Pre-signal shutdown — batch must still complete
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tx.send(()).unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        engine.run_graceful(async { rx.await.ok(); }),
    )
    .await;
    assert!(result.is_ok(), "run_graceful must return");

    let stats = engine.queue_stats().await.unwrap();
    assert_eq!(stats.delivered, 5, "all events in the batch must be delivered");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_reaper(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let engine = engine(pool);
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'exercise_test_secret_32chars_a') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool()).await.unwrap();

    let ev = engine.send("stuck", json!({}), ep).await.unwrap();

    // Simulate crash: event stuck in delivering for 200s
    sqlx::query!(
        "UPDATE webhook_events SET status='delivering', delivering_since=NOW()-INTERVAL '200 seconds' WHERE id=$1",
        ev.id
    )
    .execute(engine.pool()).await.unwrap();

    // ── recover_stuck_deliveries() ────────────────────────────────────────────
    let recovered = engine.recover_stuck_deliveries(Duration::from_secs(120)).await.unwrap();
    assert_eq!(recovered, 1);
    let reset = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(reset.status, EventStatus::Pending);
    assert!(reset.delivering_since.is_none());

    // Deliver normally
    engine.run_once().await.unwrap();
    let done = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(done.status, EventStatus::Delivered);
    server.verify().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exercise_validation_boundaries(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret) VALUES ($1, 'exercise_test_secret_32chars_a') RETURNING id",
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool()).await.unwrap();

    // Payload: exactly 1MB accepted
    let data = "x".repeat(1_048_576 - 8);
    let ok = engine.send("test", json!({"d": data}), ep).await;
    assert!(ok.is_ok(), "exactly 1MB payload must be accepted");

    // Payload: 1MB + 1 byte rejected
    let data2 = "x".repeat(1_048_576 - 8 + 1);
    let err = engine.send("test", json!({"d": data2}), ep).await;
    assert!(matches!(err, Err(HooksmithError::PayloadTooLarge(_, _))));

    // event_type: 256 bytes accepted
    let ok2 = engine.send(&"a".repeat(256), json!({}), ep).await;
    assert!(ok2.is_ok());

    // event_type: 257 bytes rejected
    let err2 = engine.send(&"a".repeat(257), json!({}), ep).await;
    assert!(err2.is_err());

    // event_type: control chars rejected
    let err3 = engine.send("order\r\ncreated", json!({}), ep).await;
    assert!(err3.is_err(), "CRLF in event_type must be rejected");

    // event_type: empty rejected
    let err4 = engine.send("", json!({}), ep).await;
    assert!(err4.is_err());

    // event_type: whitespace only rejected
    let err5 = engine.send("   ", json!({}), ep).await;
    assert!(err5.is_err());
}
