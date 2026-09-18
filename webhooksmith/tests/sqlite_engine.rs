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
        // SQLite limitation: true transactional outbox (send_in_tx) is not supported
        // because max_connections=1 — the transaction holds the only connection and
        // a second operation would deadlock. Use the Postgres backend for transactional outbox.
        // This test verifies basic send + deliver works correctly.
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
}
