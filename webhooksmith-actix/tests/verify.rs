//! Integration tests for webhooksmith-actix extractors.
//!
//! Each test builds a real actix-web `App` and fires an HTTP request through
//! `test::call_service`. No mocking — real signature computation and verification.

use actix_web::{
    App, HttpResponse, Responder,
    http::StatusCode,
    test,
    web,
};
use serde::Deserialize;
use serde_json::json;
use webhooksmith::signing;
use webhooksmith_actix::{TypedWebhook, VerifiedWebhook, WebhookSecret};

const SECRET: &str = "actix_test_secret_at_least_32ch";

fn sign_body(body: &[u8]) -> (String, String) {
    let ts = chrono::Utc::now().timestamp();
    let sig = signing::sign(SECRET, ts, body).unwrap();
    (ts.to_string(), sig)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn raw_handler(wh: VerifiedWebhook) -> impl Responder {
    HttpResponse::Ok()
        .content_type("application/json")
        .json(json!({
            "event_type": wh.event_type,
            "event_id":   wh.event_id,
            "timestamp":  wh.timestamp,
        }))
}

#[derive(Deserialize)]
struct Order { id: u64 }

async fn typed_handler(wh: TypedWebhook<Order>) -> impl Responder {
    HttpResponse::Ok().json(json!({ "order_id": wh.payload.id }))
}

fn app() -> actix_web::App<
    impl actix_web::dev::ServiceFactory<
        actix_web::dev::ServiceRequest,
        Config = (),
        Response = actix_web::dev::ServiceResponse,
        Error = actix_web::Error,
        InitError = (),
    >,
> {
    App::new()
        .app_data(WebhookSecret::new(SECRET))
        .route("/raw", web::post().to(raw_handler))
        .route("/typed", web::post().to(typed_handler))
}

// ── Happy path ────────────────────────────────────────────────────────────────

#[actix_web::test]
async fn verified_webhook_accepted_with_valid_signature() {
    let svc = test::init_service(app()).await;
    let body = json!({"key": "value"});
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let (ts, sig) = sign_body(&body_bytes);

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .insert_header(("x-hooksmith-event-type", "order.created"))
        .insert_header(("x-hooksmith-event-id", "evt-001"))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["event_type"], "order.created");
    assert_eq!(body["event_id"], "evt-001");
}

#[actix_web::test]
async fn typed_webhook_deserializes_body() {
    let svc = test::init_service(app()).await;
    let body = json!({"id": 42});
    let body_bytes = serde_json::to_vec(&body).unwrap();
    let (ts, sig) = sign_body(&body_bytes);

    let req = test::TestRequest::post()
        .uri("/typed")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["order_id"], 42);
}

// ── Missing headers ───────────────────────────────────────────────────────────

#[actix_web::test]
async fn missing_signature_returns_401() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({})).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", "1234567890"))
        // NO signature header
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "missing signature");
}

#[actix_web::test]
async fn missing_timestamp_returns_400() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({})).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-signature", "v1,deadbeef"))
        // NO timestamp header
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "missing timestamp");
}

// ── Stale timestamp ───────────────────────────────────────────────────────────

#[actix_web::test]
async fn stale_timestamp_returns_401() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({})).unwrap();
    let old_ts = chrono::Utc::now().timestamp() - 400; // 400s ago → stale
    let sig = signing::sign(SECRET, old_ts, &body_bytes).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", old_ts.to_string().as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "stale timestamp");
}

// ── Wrong signature ───────────────────────────────────────────────────────────

#[actix_web::test]
async fn wrong_signature_returns_401() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({"key": "value"})).unwrap();
    let ts = chrono::Utc::now().timestamp();

    // Sign with a DIFFERENT secret
    let wrong_sig = signing::sign("wrong_secret_for_testing_only!!!", ts, &body_bytes).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.to_string().as_str()))
        .insert_header(("x-hooksmith-signature", wrong_sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "invalid signature");
}

#[actix_web::test]
async fn tampered_body_rejected() {
    let svc = test::init_service(app()).await;
    let original = json!({"amount": 100});
    let original_bytes = serde_json::to_vec(&original).unwrap();
    let (ts, sig) = sign_body(&original_bytes);

    // Change the body after signing
    let tampered = serde_json::to_vec(&json!({"amount": 999})).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(tampered)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "tampered body must be rejected");
}

// ── Non-JSON body ─────────────────────────────────────────────────────────────

#[actix_web::test]
async fn non_json_body_returns_422() {
    let svc = test::init_service(app()).await;
    let body_bytes = b"not json at all".to_vec();
    let ts = chrono::Utc::now().timestamp();
    let sig = signing::sign(SECRET, ts, &body_bytes).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "text/plain"))
        .insert_header(("x-hooksmith-timestamp", ts.to_string().as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "body must be JSON");
}

// ── Invalid timestamp ─────────────────────────────────────────────────────────

#[actix_web::test]
async fn non_integer_timestamp_returns_400() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({})).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", "not-a-number"))
        .insert_header(("x-hooksmith-signature", "v1,anything"))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body: serde_json::Value = test::read_body_json(resp).await;
    assert_eq!(body["error"], "invalid timestamp");
}

// ── No secret configured ──────────────────────────────────────────────────────

#[actix_web::test]
async fn missing_app_data_returns_500() {
    // App without WebhookSecret registered
    let svc = test::init_service(
        App::new().route("/raw", web::post().to(raw_handler))
    ).await;

    let body_bytes = serde_json::to_vec(&json!({})).unwrap();
    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", "123"))
        .insert_header(("x-hooksmith-signature", "v1,abc"))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ── Replay attack: reusing signature from another request ─────────────────────

#[actix_web::test]
async fn replayed_signature_with_different_body_rejected() {
    let svc = test::init_service(app()).await;

    // Request 1: legitimate
    let body1 = serde_json::to_vec(&json!({"event": "original"})).unwrap();
    let (ts, sig) = sign_body(&body1);

    // Replay: same sig, different body
    let body2 = serde_json::to_vec(&json!({"event": "injected"})).unwrap();
    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body2)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "replayed signature must be rejected");
}

// ── Future timestamp (within tolerance) ──────────────────────────────────────

#[actix_web::test]
async fn future_timestamp_within_tolerance_accepted() {
    let svc = test::init_service(app()).await;
    let body_bytes = serde_json::to_vec(&json!({"ok": true})).unwrap();
    // 30 seconds in the future — within 300s tolerance
    let future_ts = chrono::Utc::now().timestamp() + 30;
    let sig = signing::sign(SECRET, future_ts, &body_bytes).unwrap();

    let req = test::TestRequest::post()
        .uri("/raw")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", future_ts.to_string().as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::OK, "future timestamp within tolerance must be accepted");
}

// ── TypedWebhook: body fails deserialization ──────────────────────────────────

#[actix_web::test]
async fn typed_webhook_rejects_wrong_schema() {
    let svc = test::init_service(app()).await;
    // Valid JSON but missing the `id` field required by `Order`
    let body_bytes = serde_json::to_vec(&json!({"name": "wrong"})).unwrap();
    let (ts, sig) = sign_body(&body_bytes);

    let req = test::TestRequest::post()
        .uri("/typed")
        .set_payload(body_bytes)
        .insert_header(("content-type", "application/json"))
        .insert_header(("x-hooksmith-timestamp", ts.as_str()))
        .insert_header(("x-hooksmith-signature", sig.as_str()))
        .to_request();

    let resp = test::call_service(&svc, req).await;
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}
