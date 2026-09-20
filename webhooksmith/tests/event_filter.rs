//! Event type filtering tests — verifies that broadcast() routes events
//! only to endpoints whose event_filter matches the event type.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use webhooksmith::{EventStatus, NewEndpoint, WebhookEngine, event_matches_filter};
use serde_json::json;
use sqlx::PgPool;
use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

async fn endpoint_with_filter(engine: &WebhookEngine, url: &str, filter: Option<Vec<&str>>) -> uuid::Uuid {
    let ep = engine.register_with(NewEndpoint {
        url: url.into(),
        signing_secret: "filter_test_secret_32chars_ok___".into(),
        description: None,
        max_attempts: None,
        initial_delay_ms: None,
        event_filter: filter.map(|f| f.into_iter().map(String::from).collect()),
    }).await.unwrap();
    ep.id
}

// ── Unit tests: pattern matching logic ────────────────────────────────────────

#[test]
fn no_filter_matches_everything() {
    assert!(event_matches_filter("order.created", &None));
    assert!(event_matches_filter("payment.captured", &None));
    assert!(event_matches_filter("anything", &None));
}

#[test]
fn empty_filter_matches_nothing() {
    assert!(!event_matches_filter("order.created", &Some(vec![])));
}

#[test]
fn wildcard_star_matches_all() {
    let f = Some(vec!["*".to_string()]);
    assert!(event_matches_filter("order.created", &f));
    assert!(event_matches_filter("anything", &f));
}

#[test]
fn exact_match_only_matches_that_type() {
    let f = Some(vec!["order.created".to_string()]);
    assert!(event_matches_filter("order.created", &f));
    assert!(!event_matches_filter("order.updated", &f));
    assert!(!event_matches_filter("payment.captured", &f));
}

#[test]
fn prefix_star_matches_correct_namespace() {
    let f = Some(vec!["order.*".to_string()]);
    assert!(event_matches_filter("order.created", &f));
    assert!(event_matches_filter("order.updated", &f));
    assert!(event_matches_filter("order.cancelled", &f));
    assert!(!event_matches_filter("payment.captured", &f));
    assert!(!event_matches_filter("orderx.created", &f)); // prefix must match "order."
}

#[test]
fn prefix_matches_exact_prefix_too() {
    // "order.*" should also match exactly "order" (the prefix itself)
    let f = Some(vec!["order.*".to_string()]);
    assert!(event_matches_filter("order", &f));
}

#[test]
fn multiple_patterns_any_match_succeeds() {
    let f = Some(vec!["order.*".to_string(), "payment.captured".to_string()]);
    assert!(event_matches_filter("order.created", &f));
    assert!(event_matches_filter("payment.captured", &f));
    assert!(!event_matches_filter("payment.failed", &f));
    assert!(!event_matches_filter("shipment.dispatched", &f));
}

// ── Integration: broadcast() routes by filter ─────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_respects_event_filter(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let url = format!("{}/hook", server.uri());

    // Endpoint A: subscribed to "order.*"
    endpoint_with_filter(&engine, &url, Some(vec!["order.*"])).await;

    // Endpoint B: subscribed only to "payment.captured"
    endpoint_with_filter(&engine, &url, Some(vec!["payment.captured"])).await;

    // Endpoint C: no filter — receives everything
    endpoint_with_filter(&engine, &url, None).await;

    // Broadcast "order.created"
    let events = engine.broadcast("order.created", json!({})).await.unwrap();
    // Should go to: endpoint A (matches "order.*") + endpoint C (no filter)
    // Should NOT go to: endpoint B (subscribed only to payment.captured)
    assert_eq!(events.len(), 2, "order.created must go to A and C, not B");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_payment_event_routes_correctly(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let url = format!("{}/hook", server.uri());

    endpoint_with_filter(&engine, &url, Some(vec!["order.*"])).await;          // A
    endpoint_with_filter(&engine, &url, Some(vec!["payment.captured"])).await; // B
    endpoint_with_filter(&engine, &url, None).await;                            // C

    let events = engine.broadcast("payment.captured", json!({})).await.unwrap();
    // Should go to: B (exact match) + C (no filter)
    // Should NOT go to: A (order.* doesn't match payment.captured)
    assert_eq!(events.len(), 2, "payment.captured must go to B and C, not A");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn broadcast_unsubscribed_event_only_reaches_unfiltered_endpoints(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let url = format!("{}/hook", server.uri());

    endpoint_with_filter(&engine, &url, Some(vec!["order.*"])).await;
    endpoint_with_filter(&engine, &url, Some(vec!["payment.*"])).await;
    endpoint_with_filter(&engine, &url, None).await; // receives everything

    // "shipment.dispatched" matches neither order.* nor payment.*
    let events = engine.broadcast("shipment.dispatched", json!({})).await.unwrap();
    assert_eq!(events.len(), 1, "shipment.dispatched only goes to unfiltered endpoint");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn empty_filter_receives_nothing_from_broadcast(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let url = format!("{}/hook", server.uri());

    endpoint_with_filter(&engine, &url, Some(vec![])).await; // empty = receives nothing

    let events = engine.broadcast("any.event", json!({})).await.unwrap();
    assert_eq!(events.len(), 0, "empty filter must receive nothing");
}

// ── set_event_filter and clear_event_filter ───────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn set_and_clear_event_filter(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let url = format!("{}/hook", server.uri());
    let ep_id = endpoint_with_filter(&engine, &url, None).await; // starts with no filter

    // Set filter to only receive order events
    engine.set_event_filter(ep_id, vec!["order.*".into()]).await.unwrap();

    // payment event should NOT be delivered
    let events = engine.broadcast("payment.captured", json!({})).await.unwrap();
    assert_eq!(events.len(), 0, "filtered endpoint must not receive payment.captured");

    // order event should be delivered
    let events2 = engine.broadcast("order.created", json!({})).await.unwrap();
    assert_eq!(events2.len(), 1, "filtered endpoint must receive order.created");

    // Clear filter — back to receiving everything
    engine.clear_event_filter(ep_id).await.unwrap();
    let events3 = engine.broadcast("payment.captured", json!({})).await.unwrap();
    assert_eq!(events3.len(), 1, "unfiltered endpoint must receive all events again");
}

// ── send() bypasses filter ────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn send_bypasses_event_filter(pool: PgPool) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let engine = engine(pool);
    let url = format!("{}/hook", server.uri());

    // Endpoint subscribed only to "order.*"
    let ep_id = endpoint_with_filter(&engine, &url, Some(vec!["order.*"])).await;

    // Directly send a payment event — filter is irrelevant for explicit send()
    let ev = engine.send("payment.captured", json!({}), ep_id).await.unwrap();
    engine.run_once().await.unwrap();

    let after = engine.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Delivered,
        "send() must deliver regardless of event_filter");
}

// ── Wildcard star ─────────────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn wildcard_star_filter_receives_everything(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let url = format!("{}/hook", server.uri());
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    endpoint_with_filter(&engine, &url, Some(vec!["*"])).await;

    let e1 = engine.broadcast("order.created", json!({})).await.unwrap();
    let e2 = engine.broadcast("payment.captured", json!({})).await.unwrap();
    let e3 = engine.broadcast("anything.at.all", json!({})).await.unwrap();

    assert_eq!(e1.len(), 1);
    assert_eq!(e2.len(), 1);
    assert_eq!(e3.len(), 1);
}

// ── Backward compatible ───────────────────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn backward_compatible_no_filter_receives_all(pool: PgPool) {
    let engine = engine(pool);
    let server = MockServer::start().await;
    let url = format!("{}/hook", server.uri());
    engine.register(&url, "filter_test_secret_32chars_ok___").await.unwrap();

    let events = engine.broadcast("order.created", json!({})).await.unwrap();
    assert_eq!(events.len(), 1, "endpoint with no filter must receive all events");

    let events2 = engine.broadcast("payment.captured", json!({})).await.unwrap();
    assert_eq!(events2.len(), 1);
}

// ── event_filter pattern validation ──────────────────────────────────────────

#[sqlx::test(migrator = "MIGRATOR")]
async fn empty_pattern_in_filter_rejected(pool: PgPool) {
    let e = engine(pool);
    let result = e.register_with(webhooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "filter_test_secret_32chars_ok___".into(),
        event_filter: Some(vec!["order.*".into(), "".into()]),
        ..Default::default()
    }).await;
    assert!(result.is_err(), "empty string pattern must be rejected");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn trailing_dot_pattern_rejected(pool: PgPool) {
    let e = engine(pool);
    let result = e.register_with(webhooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "filter_test_secret_32chars_ok___".into(),
        event_filter: Some(vec!["order.".into()]),
        ..Default::default()
    }).await;
    assert!(result.is_err(), "trailing dot without * must be rejected — use 'order.*'");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn valid_patterns_accepted(pool: PgPool) {
    let e = engine(pool);
    let result = e.register_with(webhooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "filter_test_secret_32chars_ok___".into(),
        event_filter: Some(vec!["order.created".into(), "order.*".into(), "*".into()]),
        ..Default::default()
    }).await;
    assert!(result.is_ok(), "valid patterns must be accepted");
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn control_char_in_pattern_rejected(pool: PgPool) {
    let e = engine(pool);
    let result = e.register_with(webhooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "filter_test_secret_32chars_ok___".into(),
        event_filter: Some(vec!["order\ncreated".into()]),
        ..Default::default()
    }).await;
    assert!(result.is_err(), "control char in pattern must be rejected");
}

#[test]
fn newendpoint_default_works_for_struct_update_syntax() {
    let ep = webhooksmith::NewEndpoint {
        url: "https://example.com/hook".into(),
        signing_secret: "valid_secret_32chars_min________".into(),
        ..Default::default()
    };
    assert_eq!(ep.description, None);
    assert_eq!(ep.max_attempts, None);
    assert_eq!(ep.initial_delay_ms, None);
    assert_eq!(ep.event_filter, None);
}

// ── Builder API tests ─────────────────────────────────────────────────────────

#[test]
fn builder_new_creates_endpoint_with_url_and_secret() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________");
    assert_eq!(ep.url, "https://example.com/hook");
    assert_eq!(ep.signing_secret, "valid_secret_32chars_min________");
    assert_eq!(ep.description, None);
    assert_eq!(ep.max_attempts, None);
    assert_eq!(ep.initial_delay_ms, None);
    assert_eq!(ep.event_filter, None);
}

#[test]
fn builder_description_sets_field() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .description("Order service");
    assert_eq!(ep.description, Some("Order service".to_string()));
}

#[test]
fn builder_max_attempts_sets_field() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .max_attempts(5);
    assert_eq!(ep.max_attempts, Some(5));
}

#[test]
fn builder_initial_delay_ms_sets_field() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .initial_delay_ms(2000);
    assert_eq!(ep.initial_delay_ms, Some(2000));
}

#[test]
fn builder_events_sets_filter_from_slice() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .events(["order.*", "payment.captured"]);
    assert_eq!(
        ep.event_filter,
        Some(vec!["order.*".to_string(), "payment.captured".to_string()])
    );
}

#[test]
fn builder_event_adds_single_pattern() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .event("order.*")
        .event("payment.captured");
    assert_eq!(
        ep.event_filter,
        Some(vec!["order.*".to_string(), "payment.captured".to_string()])
    );
}

#[test]
fn builder_events_replaces_previous_filter() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .event("order.*")
        .events(["payment.*"]);  // replaces the previous filter
    assert_eq!(ep.event_filter, Some(vec!["payment.*".to_string()]));
}

#[test]
fn builder_chaining_all_fields() {
    let ep = NewEndpoint::new("https://example.com/hook", "valid_secret_32chars_min________")
        .description("Test endpoint")
        .max_attempts(3)
        .initial_delay_ms(500)
        .events(["order.*", "payment.captured", "refund.issued"]);

    assert_eq!(ep.url, "https://example.com/hook");
    assert_eq!(ep.description, Some("Test endpoint".to_string()));
    assert_eq!(ep.max_attempts, Some(3));
    assert_eq!(ep.initial_delay_ms, Some(500));
    assert_eq!(ep.event_filter, Some(vec![
        "order.*".to_string(),
        "payment.captured".to_string(),
        "refund.issued".to_string(),
    ]));
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn builder_register_with_filter_works_end_to_end(pool: PgPool) {
    let e = engine(pool);

    // Register via builder
    let ep = e.register_with(
        NewEndpoint::new("https://example.com/hook", "filter_test_secret_32chars_ok___")
            .description("Order webhook")
            .events(["order.*"])
            .max_attempts(3),
    ).await.unwrap();

    assert_eq!(ep.description, Some("Order webhook".to_string()));
    assert_eq!(ep.max_attempts, 3);
    assert_eq!(ep.event_filter, Some(vec!["order.*".to_string()]));

    // Matching event is enqueued
    let events = e.broadcast("order.created", serde_json::json!({"id": 1})).await.unwrap();
    assert_eq!(events.len(), 1);

    // Non-matching event is not enqueued
    let events = e.broadcast("payment.captured", serde_json::json!({"id": 2})).await.unwrap();
    assert_eq!(events.len(), 0);
}
