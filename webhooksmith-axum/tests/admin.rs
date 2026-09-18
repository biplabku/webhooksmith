//! Integration tests for the admin HTTP router.
//! Uses a real Postgres database and a real axum test server.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../webhooksmith/migrations");

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use serde_json::Value;
use sqlx::PgPool;
use tower::ServiceExt;
use webhooksmith::WebhookEngine;
use webhooksmith_axum::admin;
use std::sync::Arc;

fn engine(pool: PgPool) -> Arc<WebhookEngine> {
    Arc::new(
        WebhookEngine::builder()
            .pool(pool)
            .allow_insecure_urls()
            .build_sync(),
    )
}

fn app(engine: Arc<WebhookEngine>) -> Router {
    Router::new().nest("/admin", admin(engine))
}

async fn get_json(app: &Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, json)
}

async fn post_json(app: &Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or_default();
    (status, json)
}

// ── GET /admin/stats ──────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn stats_returns_zero_counts_on_empty_db(pool: PgPool) {
    let e = engine(pool);
    let app = app(e);

    let (status, json) = get_json(&app, "/admin/stats").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["pending"], 0);
    assert_eq!(json["delivering"], 0);
    assert_eq!(json["failed"], 0);
    assert_eq!(json["dead"], 0);
    assert_eq!(json["delivered"], 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn stats_reflects_enqueued_events(pool: PgPool) {
    let e = engine(pool.clone());
    let app = app(Arc::clone(&e));

    let ep = e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();
    e.send("order.created", serde_json::json!({}), ep.id).await.unwrap();
    e.send("order.created", serde_json::json!({}), ep.id).await.unwrap();

    let (status, json) = get_json(&app, "/admin/stats").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["pending"], 2, "two pending events expected");
}

// ── GET /admin/endpoints ──────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn endpoints_lists_all_registered_endpoints(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();
    e.register("https://example2.com/hook", "admin_test_secret_32chars_____").await.unwrap();

    let (status, json) = get_json(&app, "/admin/endpoints").await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().len() >= 2);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn endpoints_exposes_circuit_breaker_fields(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();

    let (status, json) = get_json(&app, "/admin/endpoints").await;
    assert_eq!(status, StatusCode::OK);
    let ep = &json[0];
    assert!(ep.get("consecutive_failures").is_some(), "consecutive_failures must be in response");
    // circuit_open_until may be null
    assert!(ep.get("circuit_open_until").is_some() || ep["circuit_open_until"].is_null());
}

// ── GET /admin/dlq/{id} ───────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn dlq_returns_empty_for_endpoint_with_no_dead_events(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let ep = e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();

    let (status, json) = get_json(&app, &format!("/admin/dlq/{}", ep.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn dlq_returns_dead_events(pool: PgPool) {
    let e = engine(pool.clone());
    let app = app(Arc::clone(&e));

    // Insert endpoint directly with max_attempts=1 so failures go straight to dead
    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'admin_test_secret_32chars_____', 1, 1000) RETURNING id",
        "https://bad-endpoint.example.com/hook",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    e.send("order.failed", serde_json::json!({}), ep).await.unwrap();
    e.send("order.failed", serde_json::json!({}), ep).await.unwrap();

    // Mark both as dead directly (simulates exhausted retries)
    sqlx::query!("UPDATE webhook_events SET status = 'dead' WHERE endpoint_id = $1", ep)
        .execute(&pool)
        .await
        .unwrap();

    let (status, json) = get_json(&app, &format!("/admin/dlq/{}", ep)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), 2);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn dlq_respects_pagination(pool: PgPool) {
    let e = engine(pool.clone());
    let app = app(Arc::clone(&e));

    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'admin_test_secret_32chars_____', 1, 1000) RETURNING id",
        "https://bad-endpoint.example.com/hook",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    // Create 5 dead events
    for i in 0..5 {
        e.send("order.failed", serde_json::json!({"i": i}), ep).await.unwrap();
    }
    sqlx::query!("UPDATE webhook_events SET status = 'dead' WHERE endpoint_id = $1", ep)
        .execute(&pool)
        .await
        .unwrap();

    let (status, page1) = get_json(&app, &format!("/admin/dlq/{}?limit=2&offset=0", ep)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page1.as_array().unwrap().len(), 2);

    let (_, page2) = get_json(&app, &format!("/admin/dlq/{}?limit=2&offset=2", ep)).await;
    assert_eq!(page2.as_array().unwrap().len(), 2);

    let (_, page3) = get_json(&app, &format!("/admin/dlq/{}?limit=2&offset=4", ep)).await;
    assert_eq!(page3.as_array().unwrap().len(), 1);
}

// ── POST /admin/dlq/{id}/retry-all ────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_all_requeues_dead_events_and_resets_circuit(pool: PgPool) {
    let e = engine(pool.clone());
    let app = app(Arc::clone(&e));

    let ep: uuid::Uuid = sqlx::query_scalar!(
        "INSERT INTO webhook_endpoints (url, signing_secret, max_attempts, initial_delay_ms) VALUES ($1, 'admin_test_secret_32chars_____', 1, 1000) RETURNING id",
        "https://bad-endpoint.example.com/hook",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    for _ in 0..3 {
        e.send("order.failed", serde_json::json!({}), ep).await.unwrap();
    }
    // Manually open the circuit + mark events dead
    sqlx::query!(
        "UPDATE webhook_events SET status = 'dead' WHERE endpoint_id = $1",
        ep
    )
    .execute(&pool).await.unwrap();
    sqlx::query!(
        "UPDATE webhook_endpoints SET consecutive_failures = 5, circuit_open_until = NOW() + INTERVAL '10 minutes' WHERE id = $1",
        ep
    )
    .execute(&pool).await.unwrap();

    // POST retry-all
    let (status, json) = post_json(&app, &format!("/admin/dlq/{}/retry-all", ep)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["retried"], 3);

    // Circuit must be reset
    let endpoint = e.endpoint(ep).await.unwrap().unwrap();
    assert_eq!(endpoint.consecutive_failures, 0, "retry-all must reset circuit");
    assert!(endpoint.circuit_open_until.is_none(), "circuit must be closed after retry-all");

    // Events must be pending
    let stats = e.queue_stats().await.unwrap();
    assert_eq!(stats.pending, 3);
    assert_eq!(stats.dead, 0);
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn retry_all_returns_zero_when_no_dead_events(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let ep = e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();

    let (status, json) = post_json(&app, &format!("/admin/dlq/{}/retry-all", ep.id)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["retried"], 0);
}
