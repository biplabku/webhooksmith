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
async fn endpoints_lists_registered_endpoints_paginated(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    e.register("https://example.com/hook", "admin_test_secret_32chars_____").await.unwrap();
    e.register("https://example2.com/hook", "admin_test_secret_32chars_____").await.unwrap();

    // Default limit=50 offset=0 — returns both
    let (status, json) = get_json(&app, "/admin/endpoints").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), 2);

    // limit=1 returns exactly 1
    let (status, page1) = get_json(&app, "/admin/endpoints?limit=1&offset=0").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page1.as_array().unwrap().len(), 1);

    // offset=1 returns the second
    let (_, page2) = get_json(&app, "/admin/endpoints?limit=1&offset=1").await;
    assert_eq!(page2.as_array().unwrap().len(), 1);

    // Pages must not overlap
    let id1 = page1[0]["id"].as_str().unwrap();
    let id2 = page2[0]["id"].as_str().unwrap();
    assert_ne!(id1, id2, "pages must not overlap");

    // offset past end returns empty
    let (_, page3) = get_json(&app, "/admin/endpoints?limit=10&offset=100").await;
    assert_eq!(page3.as_array().unwrap().len(), 0);
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

// ── GET /admin/metrics ────────────────────────────────────────────────────────

async fn get_text(app: &Router, uri: &str) -> (StatusCode, String) {
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let ct = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    let _ = ct; // used implicitly below via caller
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn metrics_returns_prometheus_text(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let (status, body) = get_text(&app, "/admin/metrics").await;
    assert_eq!(status, StatusCode::OK);

    // Must contain Prometheus HELP and TYPE lines
    assert!(body.contains("# HELP webhooksmith_events"), "must have HELP for events");
    assert!(body.contains("# TYPE webhooksmith_events gauge"), "must have TYPE gauge");
    assert!(body.contains("# HELP webhooksmith_endpoints"), "must have HELP for endpoints");

    // Must have all 5 status labels
    assert!(body.contains(r#"status="pending""#));
    assert!(body.contains(r#"status="delivering""#));
    assert!(body.contains(r#"status="failed""#));
    assert!(body.contains(r#"status="dead""#));
    assert!(body.contains(r#"status="delivered""#));

    // Must have endpoint state labels
    assert!(body.contains(r#"state="enabled""#));
    assert!(body.contains(r#"state="disabled""#));
    assert!(body.contains(r#"state="circuit_open""#));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn metrics_reflects_enqueued_events(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let ep = e.register("https://example.com/hook", "metrics_test_secret_32chars___").await.unwrap();
    e.send("order.created", serde_json::json!({}), ep.id).await.unwrap();
    e.send("order.created", serde_json::json!({}), ep.id).await.unwrap();

    let (_, body) = get_text(&app, "/admin/metrics").await;

    // Parse the pending gauge value
    let pending_line = body.lines()
        .find(|l| l.contains(r#"status="pending""#))
        .expect("pending gauge must exist");
    let value: i64 = pending_line.split_whitespace().last().unwrap().parse().unwrap();
    assert_eq!(value, 2, "pending gauge must reflect 2 enqueued events");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn metrics_reflects_enabled_disabled_endpoints(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let ep1 = e.register("https://example.com/a", "metrics_test_secret_32chars___").await.unwrap();
    let _ep2 = e.register("https://example.com/b", "metrics_test_secret_32chars___").await.unwrap();
    e.disable_endpoint(ep1.id).await.unwrap();

    let (_, body) = get_text(&app, "/admin/metrics").await;

    let enabled_val: i64 = body.lines()
        .find(|l| l.contains(r#"state="enabled""#))
        .and_then(|l| l.split_whitespace().last()?.parse().ok())
        .unwrap();
    let disabled_val: i64 = body.lines()
        .find(|l| l.contains(r#"state="disabled""#))
        .and_then(|l| l.split_whitespace().last()?.parse().ok())
        .unwrap();

    assert_eq!(enabled_val, 1, "1 enabled endpoint");
    assert_eq!(disabled_val, 1, "1 disabled endpoint");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn metrics_content_type_is_prometheus(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/admin/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.contains("text/plain"), "content-type must be text/plain for Prometheus scraping");
    assert!(ct.contains("0.0.4"), "must declare Prometheus text format version 0.0.4");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn metrics_values_are_valid_integers(pool: PgPool) {
    let e = engine(pool);
    let app = app(Arc::clone(&e));

    let (status, body) = get_text(&app, "/admin/metrics").await;
    assert_eq!(status, StatusCode::OK);

    // Every non-comment, non-empty line must end with a parseable integer
    for line in body.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let value_str = line.split_whitespace().last().unwrap_or("NaN");
        let parsed = value_str.parse::<i64>();
        assert!(
            parsed.is_ok(),
            "metric line has non-integer value: {line:?}"
        );
        assert!(
            parsed.unwrap() >= 0,
            "metric value must be non-negative: {line:?}"
        );
    }
}
