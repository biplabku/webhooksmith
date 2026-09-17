static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{EventStatus, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{
    matchers::{header_exists, method, path},
    Mock, MockServer, ResponseTemplate,
};

/// Build an engine from the test pool.
/// `allow_insecure_urls` lets tests point at localhost (wiremock).
fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

/// Insert a test endpoint via raw SQL, bypassing SSRF validation.
/// In production all endpoints go through engine.register() which validates URLs.
async fn insert_endpoint(engine: &WebhookEngine, url: &str) -> uuid::Uuid {
    sqlx::query_scalar!(
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, description)
        VALUES ($1, 'test_secret_for_e2e_tests', 'test endpoint')
        RETURNING id
        "#,
        url,
    )
    .fetch_one(engine.pool())
    .await
    .unwrap()
}

// ---

#[sqlx::test(migrator = "MIGRATOR")]
async fn happy_path_delivers_event(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(header_exists("x-hooksmith-signature"))
        .and(header_exists("x-hooksmith-timestamp"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .expect(1)
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("order.created", json!({"id": 42}), endpoint_id).await.unwrap();

    assert_eq!(event.status, EventStatus::Pending);

    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 1);

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Delivered);
    assert_eq!(updated.attempts, 1);

    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(log[0].success);
    assert_eq!(log[0].response_status, Some(200));

    server.verify().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn non_2xx_response_retries(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .expect(1)
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    let event = engine.send("payment.failed", json!({"amount": 100}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Failed);
    assert_eq!(updated.attempts, 1);
    assert!(updated.scheduled_at > chrono::Utc::now());

    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(!log[0].success);
    assert_eq!(log[0].response_status, Some(503));

    server.verify().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn exhausted_retries_moves_to_dlq(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let endpoint_id = sqlx::query_scalar!(
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms)
        VALUES ($1, 'test_secret_for_e2e_tests', 3, 0)
        RETURNING id
        "#,
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    for _ in 0..3 {
        sqlx::query!("UPDATE webhook_events SET scheduled_at = NOW() WHERE id = $1", event.id)
            .execute(engine.pool())
            .await
            .unwrap();
        engine.run_once().await.unwrap();
    }

    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(updated.status, EventStatus::Dead, "should be in DLQ after 3 failures");
    assert_eq!(updated.attempts, 3);

    let dead = engine.dead_events(endpoint_id).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].id, event.id);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn dlq_retry_requeues_event(pool: PgPool) {
    let engine = engine(pool);
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

    let endpoint_id = sqlx::query_scalar!(
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms)
        VALUES ($1, 'test_secret_for_e2e_tests', 1, 0)
        RETURNING id
        "#,
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    let event = engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();
    let after_fail = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(after_fail.status, EventStatus::Dead);

    engine.retry_dead(event.id).await.unwrap();
    let requeued = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(requeued.status, EventStatus::Pending);
    assert_eq!(requeued.attempts, 0);

    engine.run_once().await.unwrap();
    let final_state = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(final_state.status, EventStatus::Delivered);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn disabled_endpoint_skips_delivery(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let endpoint_id = sqlx::query_scalar!(
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, enabled)
        VALUES ($1, 'test_secret_for_e2e_tests', false)
        RETURNING id
        "#,
        &format!("{}/hook", server.uri()),
    )
    .fetch_one(engine.pool())
    .await
    .unwrap();

    engine.send("test.event", json!({}), endpoint_id).await.unwrap();

    let n = engine.run_once().await.unwrap();
    assert_eq!(n, 0);

    server.verify().await;
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn signature_headers_are_present_and_verifiable(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let secret = "test_secret_for_e2e_tests";

    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let endpoint_id = insert_endpoint(&engine, &format!("{}/hook", server.uri())).await;
    engine.send("order.created", json!({"id": 1}), endpoint_id).await.unwrap();

    engine.run_once().await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);

    let req = &requests[0];
    let timestamp: i64 = req
        .headers.get("x-hooksmith-timestamp").unwrap()
        .to_str().unwrap().parse().unwrap();
    let sig_header = req.headers.get("x-hooksmith-signature").unwrap().to_str().unwrap();
    let body = req.body.as_slice();

    assert!(
        hooksmith::signing::verify(secret, timestamp, body, sig_header),
        "signature verification failed"
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn transactional_outbox_rolls_back_on_tx_abort(pool: PgPool) {
    let engine = engine(pool);
    let endpoint_id = insert_endpoint(&engine, "https://example.com/hook").await;

    let mut tx = engine.pool().begin().await.unwrap();
    let event = engine
        .send_in_tx("test.event", json!({}), endpoint_id, &mut tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let found = engine.event(event.id).await.unwrap();
    assert!(found.is_none(), "event must not exist after tx rollback");
}
