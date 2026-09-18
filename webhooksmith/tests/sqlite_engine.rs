//! End-to-end tests for the SQLite backend.
//! Uses in-memory SQLite — no external DB needed.

#[cfg(feature = "sqlite")]
mod sqlite_tests {
    use webhooksmith::{EventStatus, SqliteEngine};
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

    #[tokio::test]
    async fn sqlite_register_and_send() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        let event = e.send("order.created", json!({"id": 1}), ep.id).await.unwrap();
        assert_eq!(event.status, EventStatus::Pending);

        let n = e.run_once().await.unwrap();
        assert_eq!(n, 1);

        let after = e.event(event.id).await.unwrap().unwrap();
        assert_eq!(after.status, EventStatus::Delivered);
    }

    #[tokio::test]
    async fn sqlite_send_delivers_event() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        let ev = e.send("order.shipped", json!({"id": 2}), ep.id).await.unwrap();
        assert!(e.event(ev.id).await.unwrap().is_some(), "event must exist after send");

        e.run_once().await.unwrap();
        let after = e.event(ev.id).await.unwrap().unwrap();
        assert_eq!(after.status, EventStatus::Delivered);
    }

    #[tokio::test]
    async fn sqlite_idempotency() {
        let e = engine().await;
        let server = MockServer::start().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        let e1 = e.send_idempotent("order.created", json!({}), ep.id, "key-1").await.unwrap();
        let e2 = e.send_idempotent("order.created", json!({}), ep.id, "key-1").await.unwrap();
        assert_eq!(e1.id, e2.id, "same key must return same event");

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events")
            .fetch_one(e.pool()).await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn sqlite_broadcast() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        for _ in 0..3 {
            e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        }

        let events = e.broadcast("announcement", json!({"msg": "hello"})).await.unwrap();
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn sqlite_queue_stats() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500)) // all fail
            .mount(&server)
            .await;

        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            description: None,
            max_attempts: Some(1),
            initial_delay_ms: Some(1),
            event_filter: None,
        }).await.unwrap();

        for i in 0..3 { e.send("test", json!({"i": i}), ep.id).await.unwrap(); }

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.pending, 3);

        e.run_once().await.unwrap(); // all fail → dead (max_attempts=1)
        let stats2 = e.queue_stats().await.unwrap();
        assert_eq!(stats2.dead, 3);
    }

    #[tokio::test]
    async fn sqlite_retry_dead() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        // Register with max_attempts=1 so first failure goes to DLQ
        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            description: None,
            max_attempts: Some(1),
            initial_delay_ms: Some(1),
            event_filter: None,
        }).await.unwrap();

        let ev = e.send("test", json!({}), ep.id).await.unwrap();
        e.run_once().await.unwrap();

        let dead = e.event(ev.id).await.unwrap().unwrap();
        assert_eq!(dead.status, EventStatus::Dead);

        e.retry_dead(ev.id).await.unwrap();
        e.run_once().await.unwrap();
        let final_state = e.event(ev.id).await.unwrap().unwrap();
        assert_eq!(final_state.status, EventStatus::Delivered);
    }

    #[tokio::test]
    async fn sqlite_cleanup() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        for i in 0..5 { e.send("test", json!({"i": i}), ep.id).await.unwrap(); }
        e.run_once().await.unwrap(); // all delivered

        // Backdate
        sqlx::query("UPDATE webhook_events SET created_at = datetime('now', '-2 days') WHERE status='delivered'")
            .execute(e.pool()).await.unwrap();

        let removed = e.cleanup_delivered(Duration::from_secs(86_400)).await.unwrap();
        assert_eq!(removed, 5);

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.delivered, 0);
    }

    #[tokio::test]
    async fn sqlite_list_endpoints() {
        let e = engine().await;
        assert_eq!(e.list_endpoints().await.unwrap().len(), 0);

        e.register("https://a.example.com/hook", "sqlite_test_secret_32chars_ok").await.unwrap();
        e.register("https://b.example.com/hook", "sqlite_test_secret_32chars_ok").await.unwrap();

        assert_eq!(e.list_endpoints().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn sqlite_disable_endpoint_stops_delivery() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let e = engine().await;
        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();

        e.disable_endpoint(ep.id).await.unwrap();
        e.send("test", json!({}), ep.id).await.unwrap();
        let n = e.run_once().await.unwrap();
        assert_eq!(n, 0, "disabled endpoint must not be claimed");

        server.verify().await;
    }

    // ── True transactional outbox ─────────────────────────────────────────────

    #[tokio::test]
    async fn send_in_tx_rollback_removes_event() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0) // must NOT be delivered — event was rolled back
            .mount(&server)
            .await;

        let ep = e.register(
            &format!("{}/hook", server.uri()),
            "sqlite_test_secret_32chars_ok"
        ).await.unwrap();

        let mut tx = e.pool().begin().await.unwrap();
        let ev = e.send_in_tx("order.created", json!({}), ep.id, &mut tx)
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        // After rollback: event must NOT exist
        let found = e.event(ev.id).await.unwrap();
        assert!(found.is_none(), "event must not exist after tx rollback");

        let n = e.run_once().await.unwrap();
        assert_eq!(n, 0, "worker must find nothing after rollback");

        server.verify().await;
    }

    #[tokio::test]
    async fn send_in_tx_commit_persists_and_delivers_event() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let ep = e.register(
            &format!("{}/hook", server.uri()),
            "sqlite_test_secret_32chars_ok"
        ).await.unwrap();

        let mut tx = e.pool().begin().await.unwrap();
        let ev = e.send_in_tx("order.created", json!({"id": 1}), ep.id, &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        // After commit: event must exist
        let found = e.event(ev.id).await.unwrap();
        assert!(found.is_some(), "event must exist after tx commit");

        e.run_once().await.unwrap();
        let after = e.event(ev.id).await.unwrap().unwrap();
        assert_eq!(after.status, EventStatus::Delivered);

        server.verify().await;
    }

    // ── Circuit breaker (SQLite) ───────────────────────────────────────────────

    #[tokio::test]
    async fn sqlite_circuit_opens_after_five_failures() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        // Large initial_delay_ms so retried events never fire during the test window.
        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            initial_delay_ms: Some(60_000), // 60 second retry — won't fire in test
            ..Default::default()
        }).await.unwrap();

        // Send 5 events, deliver each once (all fail → 500) — should not retry
        for i in 0..5 {
            e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
            e.run_once().await.unwrap();
        }

        let ep_after = e.endpoint(ep.id).await.unwrap().unwrap();
        assert_eq!(ep_after.consecutive_failures, 5);
        assert!(ep_after.circuit_open_until.is_some(), "circuit must open after 5 failures");
    }

    #[tokio::test]
    async fn sqlite_success_resets_circuit() {
        let e = engine().await;
        let server = MockServer::start().await;

        // 3 failures then success
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            initial_delay_ms: Some(60_000),
            ..Default::default()
        }).await.unwrap();

        for i in 0..3 {
            e.send("test.event", json!({"i": i}), ep.id).await.unwrap();
            e.run_once().await.unwrap();
        }
        assert_eq!(e.endpoint(ep.id).await.unwrap().unwrap().consecutive_failures, 3);

        e.send("test.event", json!({"success": true}), ep.id).await.unwrap();
        e.run_once().await.unwrap();

        let after = e.endpoint(ep.id).await.unwrap().unwrap();
        assert_eq!(after.consecutive_failures, 0, "success resets counter");
        assert!(after.circuit_open_until.is_none(), "success clears circuit");
    }

    // ── Retry doesn't fire immediately (datetime format fix) ─────────────────

    #[tokio::test]
    async fn failed_event_does_not_retry_immediately() {
        // This tests the fix for the datetime format bug: retried events used
        // SQLite's datetime() format (space separator) which compared less-than
        // our ISO 8601 now_str() (T separator), causing immediate re-claim.
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            max_attempts: Some(10),
            initial_delay_ms: Some(60_000), // 60s retry
            ..Default::default()
        }).await.unwrap();

        e.send("test.event", json!({}), ep.id).await.unwrap();
        // First run: fails → scheduled for 60s later
        let n1 = e.run_once().await.unwrap();
        assert_eq!(n1, 1);

        // Immediately run again: the retry must NOT be due yet
        let n2 = e.run_once().await.unwrap();
        assert_eq!(n2, 0, "retry must not be immediately re-claimed after failure");

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.failed, 1, "event must be in failed state awaiting retry");
        assert_eq!(stats.pending, 0);
    }

    // ── Cleanup removes old delivered events ──────────────────────────────────

    #[tokio::test]
    async fn sqlite_cleanup_delivered_removes_old_events() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let ev = e.send("test.event", json!({}), ep.id).await.unwrap();
        e.run_once().await.unwrap();
        assert_eq!(e.event(ev.id).await.unwrap().unwrap().status, webhooksmith::EventStatus::Delivered);

        // Cleanup with 0s threshold
        let deleted = e.cleanup_delivered(std::time::Duration::from_secs(0)).await.unwrap();
        assert_eq!(deleted, 1);
        assert!(e.event(ev.id).await.unwrap().is_none(), "delivered event must be removed");
    }

    // ── Large payload stored and retrieved correctly ───────────────────────────

    #[tokio::test]
    async fn sqlite_large_payload_roundtrip() {
        let e = engine().await;
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ep = e.register(&format!("{}/hook", server.uri()), "sqlite_test_secret_32chars_ok").await.unwrap();
        let big_payload = json!({"data": "z".repeat(50_000)});
        let ev = e.send("data.sync", big_payload.clone(), ep.id).await.unwrap();
        e.run_once().await.unwrap();

        let after = e.event(ev.id).await.unwrap().unwrap();
        assert_eq!(after.status, webhooksmith::EventStatus::Delivered);
        assert_eq!(after.payload["data"], big_payload["data"]);
    }

    // ── DLQ: retry_all resets circuit and re-queues ───────────────────────────

    #[tokio::test]
    async fn sqlite_dlq_retry_all_full_cycle() {
        let e = engine().await;
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .up_to_n_times(5)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let ep = e.register_with(webhooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "sqlite_test_secret_32chars_ok".into(),
            max_attempts: Some(1),
            initial_delay_ms: Some(60_000),
            ..Default::default()
        }).await.unwrap();

        for i in 0..5 {
            e.send("order.created", json!({"i": i}), ep.id).await.unwrap();
            e.run_once().await.unwrap();
        }

        let stats = e.queue_stats().await.unwrap();
        assert_eq!(stats.dead, 5);

        let retried = e.retry_all_dead(ep.id).await.unwrap();
        assert_eq!(retried, 5);

        let ep_state = e.endpoint(ep.id).await.unwrap().unwrap();
        assert_eq!(ep_state.consecutive_failures, 0);
        assert!(ep_state.circuit_open_until.is_none());

        // Deliver all 5 successfully
        e.run_once().await.unwrap();
        let final_stats = e.queue_stats().await.unwrap();
        assert_eq!(final_stats.delivered, 5);
    }
}
