//! updated_at trigger + update_endpoint tests.
//!
//! Properties:
//!   A. updated_at changes when an endpoint is updated (trigger fires).
//!   B. updated_at equals created_at on a freshly created endpoint.
//!   C. update_endpoint URL change causes delivery to the new URL.
//!   D. update_endpoint disabling stops delivery.
//!   E. update_endpoint with invalid max_attempts rejected.
//!   F. update_endpoint on a non-existent endpoint returns EndpointNotFound.
//!   G. Partial update: unspecified fields are not changed.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{error::HooksmithError, UpdateEndpoint, WebhookEngine};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

// ── Property A + B: Trigger sets updated_at on UPDATE ────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn updated_at_equals_created_at_on_new_endpoint(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    assert_eq!(
        endpoint.updated_at.timestamp_millis(),
        endpoint.created_at.timestamp_millis(),
        "updated_at must equal created_at on a new endpoint"
    );
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn updated_at_changes_after_update(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    let created_at = endpoint.created_at;

    // Wait at least 1ms so the clock advances
    sqlx::query!("SELECT pg_sleep(0.01)").execute(engine.pool()).await.unwrap();

    let updated = engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            description: Some(Some("updated".into())),
            ..Default::default()
        })
        .await
        .unwrap();

    assert!(
        updated.updated_at > created_at,
        "updated_at must be later than created_at after an update (trigger must fire)"
    );
}

// ── Property C: URL change delivers to new URL ────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_url_delivers_to_new_url(pool: PgPool) {
    let engine = engine(pool);

    let old_server = MockServer::start().await;
    let new_server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0) // old URL must receive nothing after the update
        .mount(&old_server)
        .await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1) // new URL must receive the event
        .mount(&new_server)
        .await;

    let endpoint = engine
        .register(&format!("{}/hook", old_server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            url: Some(format!("{}/hook", new_server.uri())),
            ..Default::default()
        })
        .await
        .unwrap();

    engine.send("test.event", json!({}), endpoint.id).await.unwrap();
    engine.run_once().await.unwrap();

    old_server.verify().await;
    new_server.verify().await;
}

// ── Property D: Disabling stops delivery ─────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_disable_stops_delivery(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            enabled: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();

    engine.send("test.event", json!({}), endpoint.id).await.unwrap();
    let n = engine.run_once().await.unwrap();

    assert_eq!(n, 0, "disabled endpoint must not be claimed by the worker");
    server.verify().await;

    // Re-enabling makes it work again
    let server2 = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server2)
        .await;

    engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            enabled: Some(true),
            url: Some(format!("{}/hook", server2.uri())),
            ..Default::default()
        })
        .await
        .unwrap();

    let n2 = engine.run_once().await.unwrap();
    assert_eq!(n2, 1);
    server2.verify().await;
}

// ── Property E: Validation applies to updates ─────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_rejects_invalid_max_attempts(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    let err = engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            max_attempts: Some(0),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(matches!(err, HooksmithError::Config(_)));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_rejects_short_secret(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    let err = engine
        .update_endpoint(endpoint.id, UpdateEndpoint {
            signing_secret: Some("short".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(matches!(err, HooksmithError::Config(_)));
}

// ── Property F: Non-existent endpoint returns EndpointNotFound ────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_nonexistent_endpoint_returns_not_found(pool: PgPool) {
    let engine = engine(pool);
    let err = engine
        .update_endpoint(uuid::Uuid::new_v4(), UpdateEndpoint {
            description: Some(Some("anything".into())),
            ..Default::default()
        })
        .await
        .unwrap_err();

    assert!(matches!(err, HooksmithError::EndpointNotFound(_)));
}

// ── Property G: Partial update — unspecified fields unchanged ─────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn partial_update_leaves_other_fields_unchanged(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;

    let original = engine
        .register_with(hooksmith::NewEndpoint {
            url: format!("{}/hook", server.uri()),
            signing_secret: "trigger_test_secret_32chars".into(),
            description: Some("original description".into()),
            max_attempts: Some(5),
            initial_delay_ms: Some(2000),
        })
        .await
        .unwrap();

    // Only update enabled — everything else must stay the same
    let updated = engine
        .update_endpoint(original.id, UpdateEndpoint {
            enabled: Some(false),
            ..Default::default()
        })
        .await
        .unwrap();

    assert_eq!(updated.url, original.url);
    assert_eq!(updated.signing_secret, original.signing_secret);
    assert_eq!(updated.description, original.description);
    assert_eq!(updated.max_attempts, original.max_attempts);
    assert_eq!(updated.initial_delay_ms, original.initial_delay_ms);
    assert!(!updated.enabled, "only enabled must have changed");
}

// ── Bug fix #3: UpdateEndpoint with allow_insecure_urls still validates URL format ──
//
// Previously, allow_insecure_urls=true skipped ALL URL validation, so
// "not a url at all" was accepted as an endpoint URL.

#[sqlx::test(migrator = "MIGRATOR")]
async fn update_rejects_malformed_url_even_with_insecure_urls_allowed(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let endpoint = engine
        .register(&format!("{}/hook", server.uri()), "trigger_test_secret_32chars")
        .await
        .unwrap();

    // Plain garbage — must be rejected even with allow_insecure_urls
    let err = engine
        .update_endpoint(endpoint.id, hooksmith::UpdateEndpoint {
            url: Some("not a url at all".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, hooksmith::HooksmithError::Config(_)),
        "malformed URL must be rejected even when allow_insecure_urls is set"
    );

    // Non-http scheme — also rejected
    let err2 = engine
        .update_endpoint(endpoint.id, hooksmith::UpdateEndpoint {
            url: Some("ftp://example.com/hook".into()),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err2, hooksmith::HooksmithError::Config(_)));

    // Valid http localhost URL — accepted (allow_insecure_urls=true skips SSRF check)
    let ok = engine
        .update_endpoint(endpoint.id, hooksmith::UpdateEndpoint {
            url: Some(format!("{}/hook", server.uri())),
            ..Default::default()
        })
        .await;
    assert!(ok.is_ok(), "valid http URL must be accepted with allow_insecure_urls");
}
