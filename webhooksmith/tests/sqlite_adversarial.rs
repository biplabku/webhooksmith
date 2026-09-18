//! Adversarial and stress tests for the SQLite backend.
//! Tries to break it across every scenario.

#[cfg(feature = "sqlite")]
mod tests {
    use webhooksmith::{EventStatus, NewEndpoint, SqliteEngine};
    use serde_json::json;
    use std::time::Duration;
    use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

    async fn engine() -> SqliteEngine {
        let e = SqliteEngine::builder()
            .database_url("sqlite::memory:")
            .allow_insecure_urls()
            .build()
            .await
            .unwrap();
        e.migrate().await.unwrap();
        e
    }

    // ── Validation ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn empty_event_type_rejected() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        assert!(e.send("", json!({}), ep.id).await.is_err());
        assert!(e.send("   ", json!({}), ep.id).await.is_err());
    }

    #[tokio::test]
    async fn control_chars_in_event_type_rejected() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        assert!(e.send("order\r\ncreated", json!({}), ep.id).await.is_err());
        assert!(e.send("order\x00created", json!({}), ep.id).await.is_err());
    }

    #[tokio::test]
    async fn oversized_payload_rejected() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let big = json!({"data": "x".repeat(1_100_000)});
        assert!(e.send("test", big, ep.id).await.is_err());
    }

    #[tokio::test]
    async fn max_attempts_zero_rejected() {
        let e = engine().await;
        let result = e.register_with(NewEndpoint {
            url: "https://example.com/hook".into(),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            description: None,
            max_attempts: Some(0),
            initial_delay_ms: None,
        }).await;
        assert!(result.is_err(), "max_attempts=0 must be rejected");
    }

    #[tokio::test]
    async fn initial_delay_ms_zero_rejected() {
        let e = engine().await;
        let result = e.register_with(NewEndpoint {
            url: "https://example.com/hook".into(),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: Some(0),
        }).await;
        assert!(result.is_err(), "initial_delay_ms=0 must be rejected");
    }

    // ── Unicode edge cases ────────────────────────────────────────────────────

    #[tokio::test]
    async fn unicode_payload_survives_roundtrip() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        let payload = json!({
            "order": "Заказ №1001",         // Russian
            "note": "注文 🦀",              // Chinese + emoji
            "path": "C:\\Users\\biplab",    // backslash
        });

        let ev = e.send("order.created", payload.clone(), ep.id).await.unwrap();
        let retrieved = e.event(ev.id).await.unwrap().unwrap();

        // Payload must survive DB roundtrip unchanged
        assert_eq!(retrieved.payload["order"], payload["order"]);
        assert_eq!(retrieved.payload["note"], payload["note"]);
        assert_eq!(retrieved.payload["path"], payload["path"]);
    }

    // ── Concurrent sends (pool=1, should serialize safely) ────────────────────

    #[tokio::test]
    async fn concurrent_sends_all_succeed() {
        let e = std::sync::Arc::new(engine().await);
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        // 20 concurrent sends — SQLite pool=1 will queue them
        let handles: Vec<_> = (0..20).map(|i| {
            let e = e.clone();
            let ep_id = ep.id;
            tokio::spawn(async move {
                e.send("test", json!({"i": i}), ep_id).await
            })
        }).collect();

        let results = futures::future::join_all(handles).await;
        let successes = results.iter().filter(|r| r.as_ref().map(|r| r.is_ok()).unwrap_or(false)).count();
        assert_eq!(successes, 20, "all 20 concurrent sends must succeed");

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.pending, 20);
    }

    // ── Idempotency under concurrent load ─────────────────────────────────────

    #[tokio::test]
    async fn concurrent_idempotent_sends_deduplicate() {
        let e = std::sync::Arc::new(engine().await);
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        let handles: Vec<_> = (0..10).map(|_| {
            let e = e.clone();
            let ep_id = ep.id;
            tokio::spawn(async move {
                e.send_idempotent("order.created", json!({"id": 1}), ep_id, "order-9999").await
            })
        }).collect();

        let results = futures::future::join_all(handles).await;
        let event_ids: std::collections::HashSet<_> = results.into_iter()
            .filter_map(|r| r.ok())
            .filter_map(|r| r.ok())
            .map(|e| e.id)
            .collect();

        assert_eq!(event_ids.len(), 1, "all concurrent idempotent sends must return same event");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events")
            .fetch_one(e.pool()).await.unwrap();
        assert_eq!(count, 1);
    }

    // ── DLQ edge cases ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn retry_delivered_event_returns_invalid_state() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let ev = e.send("test", json!({}), ep.id).await.unwrap();
        e.run_once().await.unwrap();

        let err = e.retry_dead(ev.id).await.unwrap_err();
        assert!(matches!(err, webhooksmith::HooksmithError::InvalidState(_)));
    }

    #[tokio::test]
    async fn retry_nonexistent_event_returns_not_found() {
        let e = engine().await;
        let err = e.retry_dead(uuid::Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, webhooksmith::HooksmithError::EventNotFound(_)));
    }

    // ── Endpoint management edge cases ────────────────────────────────────────

    #[tokio::test]
    async fn delete_nonexistent_endpoint_returns_not_found() {
        let e = engine().await;
        let err = e.delete_endpoint(uuid::Uuid::new_v4()).await.unwrap_err();
        assert!(matches!(err, webhooksmith::HooksmithError::EndpointNotFound(_)));
    }

    #[tokio::test]
    async fn disable_then_enable_delivers_pending_events() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        // Disable, send, try to deliver (should deliver 0)
        e.disable_endpoint(ep.id).await.unwrap();
        e.send("test", json!({}), ep.id).await.unwrap();
        let n1 = e.run_once().await.unwrap();
        assert_eq!(n1, 0);

        // Re-enable, deliver (should deliver 1)
        e.enable_endpoint(ep.id).await.unwrap();
        let n2 = e.run_once().await.unwrap();
        assert_eq!(n2, 1);

        server.verify().await;
    }

    // ── Queue stats accuracy ──────────────────────────────────────────────────

    #[tokio::test]
    async fn queue_stats_sum_equals_total_events() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register_with(NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            description: None,
            max_attempts: Some(1),
            initial_delay_ms: Some(1),
        }).await.unwrap();

        // Send 5: 3 will succeed, 2 will fail → dead
        for i in 0..5 { e.send("test", json!({"i": i}), ep.id).await.unwrap(); }
        e.run_once().await.unwrap();

        let stats = e.queue_stats().await.unwrap();
        let total = stats.pending + stats.delivering + stats.failed + stats.dead + stats.delivered;
        assert_eq!(total, 5, "sum of all statuses must equal total events");
    }

    // ── Cleanup doesn't remove wrong status ───────────────────────────────────

    #[tokio::test]
    async fn cleanup_delivered_does_not_touch_pending() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        // 3 pending events (not delivered)
        for i in 0..3 { e.send("test", json!({"i": i}), ep.id).await.unwrap(); }

        // Backdate them
        sqlx::query("UPDATE webhook_events SET created_at = datetime('now', '-2 days')")
            .execute(e.pool()).await.unwrap();

        // cleanup_delivered must not remove pending events
        let removed = e.cleanup_delivered(Duration::from_secs(1)).await.unwrap();
        assert_eq!(removed, 0, "cleanup_delivered must not remove pending events");

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.pending, 3);
    }

    // ── Global event listing ──────────────────────────────────────────────────

    #[tokio::test]
    async fn events_global_spans_all_endpoints() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ep1 = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let ep2 = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        e.send("test", json!({"ep": 1}), ep1.id).await.unwrap();
        e.send("test", json!({"ep": 2}), ep2.id).await.unwrap();

        let global = e.events_global(EventStatus::Pending, 100, 0).await.unwrap();
        assert_eq!(global.len(), 2, "events_global must see events from both endpoints");
    }

    // ── Stress: 500 events, deliver all, check no duplicates ─────────────────

    #[tokio::test]
    async fn stress_500_events_no_duplicates() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        for i in 0..500 { e.send("stress", json!({"i": i}), ep.id).await.unwrap(); }

        let mut delivered = 0;
        for _ in 0..15 { // 500 / ~50 batch = 10 cycles, with margin
            let n = e.run_once().await.unwrap();
            delivered += n;
            if delivered >= 500 { break; }
        }

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.delivered, 500);
        assert_eq!(stats.pending, 0);

        // Exactly 500 delivery attempts (no duplicates)
        let attempts: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM webhook_delivery_attempts WHERE success = 1"
        )
        .fetch_one(e.pool()).await.unwrap();
        assert_eq!(attempts, 500, "exactly 500 successful attempts — no duplicates");
    }

    // ── Pagination correctness ────────────────────────────────────────────────

    #[tokio::test]
    async fn events_by_status_pagination_no_overlap() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        for i in 0..10 { e.send("test", json!({"i": i}), ep.id).await.unwrap(); }

        let page1 = e.events_by_status(ep.id, EventStatus::Pending, 3, 0).await.unwrap();
        let page2 = e.events_by_status(ep.id, EventStatus::Pending, 3, 3).await.unwrap();
        let page3 = e.events_by_status(ep.id, EventStatus::Pending, 3, 6).await.unwrap();
        let page4 = e.events_by_status(ep.id, EventStatus::Pending, 3, 9).await.unwrap();

        assert_eq!(page1.len(), 3);
        assert_eq!(page2.len(), 3);
        assert_eq!(page3.len(), 3);
        assert_eq!(page4.len(), 1);

        let all_ids: Vec<_> = [&page1, &page2, &page3, &page4].iter()
            .flat_map(|p| p.iter().map(|e| e.id))
            .collect();
        let unique: std::collections::HashSet<_> = all_ids.iter().collect();
        assert_eq!(unique.len(), 10, "pages must not overlap");
    }

    // ── Malformed signing secret handled gracefully ───────────────────────────

    #[tokio::test]
    async fn short_signing_secret_rejected() {
        let e = engine().await;
        let err = e.register("https://example.com/hook", "short").await.unwrap_err();
        assert!(matches!(err, webhooksmith::HooksmithError::Config(_)));
    }

    // ── retry_all_dead returns 0 when nothing in DLQ ─────────────────────────

    #[tokio::test]
    async fn retry_all_dead_empty_returns_zero() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let n = e.retry_all_dead(ep.id).await.unwrap();
        assert_eq!(n, 0);
    }
}
