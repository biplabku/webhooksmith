//! True end-to-end tests: engine sends → real actix-web receiver verifies.
//!
//! No wiremock, no mocks, no stubs. A real actix-web server using
//! webhooksmith-actix middleware listens on a random localhost port.
//! The webhooksmith engine delivers signed webhooks to it over TCP.
//! The receiver verifies HMAC-SHA256 and records what it got.
//!
//! This tests the FULL signing→delivery→verification pipeline with real code
//! on both ends, including:
//!   - Headers set correctly by the delivery worker
//!   - Signature computed with the right secret and timestamp
//!   - Receiver verifying HMAC-SHA256 correctly
//!   - Response codes flowing back into the engine's success/failure tracking

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use actix_web::{App, HttpResponse, HttpServer, Responder, web};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::{Arc, Mutex};
use webhooksmith::{EventStatus, NewEndpoint, WebhookEngine};
use webhooksmith_actix::{VerifiedWebhook, WebhookSecret};

// ── Shared state between receiver and test ────────────────────────────────────

#[derive(Clone, Default)]
struct Inbox {
    received: Arc<Mutex<Vec<Value>>>,
}

impl Inbox {
    fn push(&self, v: Value) {
        self.received.lock().unwrap().push(v);
    }
    fn drain(&self) -> Vec<Value> {
        self.received.lock().unwrap().drain(..).collect()
    }
}

// ── Actix-web handler that uses VerifiedWebhook ───────────────────────────────

async fn receiver(
    wh: VerifiedWebhook,
    inbox: web::Data<Inbox>,
) -> impl Responder {
    let body: Value = serde_json::from_slice(&wh.body).unwrap_or_default();
    inbox.push(json!({
        "event_type": wh.event_type,
        "event_id":   wh.event_id,
        "timestamp":  wh.timestamp,
        "payload":    body,
    }));
    HttpResponse::Ok().finish()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

/// Starts a real actix-web server on a random port.
/// Returns (base_url, inbox, server_handle).
async fn start_receiver(secret: &str) -> (String, Inbox, actix_web::dev::ServerHandle) {
    // Grab a free port by binding briefly, then release for actix to use.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let inbox = Inbox::default();
    let inbox_data = web::Data::new(inbox.clone());
    let secret = secret.to_owned();

    let server = HttpServer::new(move || {
        App::new()
            .app_data(WebhookSecret::new(&secret))
            .app_data(inbox_data.clone())
            .route("/hook", web::post().to(receiver))
    })
    .bind(format!("127.0.0.1:{port}"))
    .expect("bind")
    .disable_signals()
    .run();

    let handle = server.handle();
    tokio::spawn(server);

    // Give server a moment to finish binding
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (format!("http://127.0.0.1:{port}"), inbox, handle)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Full cycle: engine sends a signed webhook → real actix server verifies it.
#[sqlx::test(migrator = "MIGRATOR")]
async fn real_send_and_receive(pool: PgPool) {
    const SECRET: &str = "real_e2e_signing_secret_32chars_";

    let (url, inbox, server) = start_receiver(SECRET).await;
    let e = engine(pool);

    let ep = e.register_with(NewEndpoint {
        url: format!("{url}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();

    let ev = e.send("order.created", json!({"id": 42, "amount": 100}), ep.id)
        .await.unwrap();

    assert_eq!(ev.status, EventStatus::Pending);

    // Run the worker — delivers over real TCP to the actix server
    let n = e.run_once().await.unwrap();
    assert_eq!(n, 1);

    // Short wait for the HTTP response to propagate
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Delivered,
        "event must be delivered — real receiver returned 200");

    // Verify the receiver actually got the payload with correct fields
    let received = inbox.drain();
    assert_eq!(received.len(), 1, "receiver must have gotten exactly 1 webhook");
    assert_eq!(received[0]["event_type"], "order.created");
    assert_eq!(received[0]["payload"]["id"], 42);
    assert_eq!(received[0]["payload"]["amount"], 100);
    assert!(!received[0]["event_id"].as_str().unwrap_or("").is_empty(),
        "event_id header must be set");

    server.stop(false).await;
}

/// Wrong secret: receiver rejects, engine records failure.
#[sqlx::test(migrator = "MIGRATOR")]
async fn real_wrong_secret_causes_failure(pool: PgPool) {
    // Server expects "correct_secret..." but engine signs with "wrong_secret..."
    let (url, _inbox, server) = start_receiver("correct_secret_32chars_minimum__").await;
    let e = engine(pool);

    let ep = e.register_with(NewEndpoint {
        url: format!("{url}/hook"),
        signing_secret: "wrong_secret_32chars_minimum____".into(), // WRONG
        initial_delay_ms: Some(60_000), // avoid immediate retry
        ..Default::default()
    }).await.unwrap();

    let ev = e.send("order.created", json!({}), ep.id).await.unwrap();
    e.run_once().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, EventStatus::Failed,
        "event must be failed — receiver returned 401 (wrong secret)");

    let log = e.delivery_log(ev.id).await.unwrap();
    assert_eq!(log.len(), 1);
    assert!(!log[0].success);
    assert_eq!(log[0].response_status, Some(401));

    server.stop(false).await;
}

/// Broadcast to multiple real receivers: each gets its own signed copy.
#[sqlx::test(migrator = "MIGRATOR")]
async fn real_broadcast_to_multiple_receivers(pool: PgPool) {
    const SECRET: &str = "broadcast_e2e_secret_32chars____";

    let (url1, inbox1, server1) = start_receiver(SECRET).await;
    let (url2, inbox2, server2) = start_receiver(SECRET).await;

    let e = engine(pool);
    e.register_with(NewEndpoint {
        url: format!("{url1}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();
    e.register_with(NewEndpoint {
        url: format!("{url2}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();

    let events = e.broadcast("payment.captured", json!({"amount": 500})).await.unwrap();
    assert_eq!(events.len(), 2);

    e.run_once().await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // Both receivers must have gotten the webhook
    let r1 = inbox1.drain();
    let r2 = inbox2.drain();
    assert_eq!(r1.len(), 1, "receiver 1 must get exactly 1 webhook");
    assert_eq!(r2.len(), 1, "receiver 2 must get exactly 1 webhook");

    assert_eq!(r1[0]["event_type"], "payment.captured");
    assert_eq!(r2[0]["event_type"], "payment.captured");
    assert_eq!(r1[0]["payload"]["amount"], 500);

    server1.stop(false).await;
    server2.stop(false).await;
}

/// Transactional outbox: rollback means receiver never gets called.
#[sqlx::test(migrator = "MIGRATOR")]
async fn real_rollback_means_no_delivery(pool: PgPool) {
    const SECRET: &str = "outbox_e2e_secret_32chars_______";

    let (url, inbox, server) = start_receiver(SECRET).await;
    let e = engine(pool.clone());

    let ep = e.register_with(NewEndpoint {
        url: format!("{url}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();

    // Begin tx, enqueue, then ROLLBACK
    let mut tx = pool.begin().await.unwrap();
    e.send_in_tx("order.created", json!({"id": 99}), ep.id, &mut tx).await.unwrap();
    tx.rollback().await.unwrap();

    // Worker should see nothing
    let n = e.run_once().await.unwrap();
    assert_eq!(n, 0, "worker must not claim rolled-back event");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let received = inbox.drain();
    assert!(received.is_empty(), "real receiver must get nothing after tx rollback");

    server.stop(false).await;
}

/// Idempotent send: even if called twice, receiver gets it once.
#[sqlx::test(migrator = "MIGRATOR")]
async fn real_idempotent_send_delivers_once(pool: PgPool) {
    const SECRET: &str = "idempotent_e2e_32chars__________";

    let (url, inbox, server) = start_receiver(SECRET).await;
    let e = engine(pool);

    let ep = e.register_with(NewEndpoint {
        url: format!("{url}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();

    // Send same idempotency key twice
    e.send_idempotent("order.created", json!({"id": 1}), ep.id, "order-idem-001").await.unwrap();
    e.send_idempotent("order.created", json!({"id": 1}), ep.id, "order-idem-001").await.unwrap();

    let n = e.run_once().await.unwrap();
    assert_eq!(n, 1, "only one event must exist despite two idempotent sends");

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let received = inbox.drain();
    assert_eq!(received.len(), 1, "real receiver must get exactly 1 webhook");

    server.stop(false).await;
}

/// SQLite full cycle: send → real receiver → delivered.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_real_send_and_receive() {
    const SECRET: &str = "sqlite_e2e_secret_32chars_______";

    let (url, inbox, server) = start_receiver(SECRET).await;

    let e = webhooksmith::SqliteEngine::builder()
        .database_url("sqlite::memory:")
        .allow_insecure_urls()
        .build()
        .await
        .unwrap();
    e.migrate().await.unwrap();

    let ep = e.register_with(webhooksmith::NewEndpoint {
        url: format!("{url}/hook"),
        signing_secret: SECRET.into(),
        ..Default::default()
    }).await.unwrap();

    let ev = e.send("order.created", json!({"sqlite": true}), ep.id).await.unwrap();
    let n = e.run_once().await.unwrap();
    assert_eq!(n, 1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let after = e.event(ev.id).await.unwrap().unwrap();
    assert_eq!(after.status, webhooksmith::EventStatus::Delivered);

    let received = inbox.drain();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["payload"]["sqlite"], true);

    server.stop(false).await;
}
