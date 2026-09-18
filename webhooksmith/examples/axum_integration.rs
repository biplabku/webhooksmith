//! How-to: Integrate webhooksmith into a real axum application.
//!
//! This shows the full pattern:
//! - Engine shared across handlers via axum State
//! - Transactional outbox in a POST handler
//! - Worker running concurrently with the HTTP server
//! - Graceful shutdown on Ctrl-C
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example axum_integration -p webhooksmith

use std::sync::Arc;
use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use webhooksmith::WebhookEngine;

const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";
const PARTNER_WEBHOOK_URL: &str = "https://httpbin.org/post";
const SIGNING_SECRET: &str = "your-signing-secret-min-32-chars";

// ── App state shared across all handlers ──────────────────────────────────────

#[derive(Clone)]
struct AppState {
    engine: Arc<WebhookEngine>,
    partner_endpoint_id: uuid::Uuid,
}

// ── Request/response types ────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateOrderRequest {
    product_id: i64,
    quantity: u32,
}

#[derive(Serialize)]
struct OrderResponse {
    order_id: i64,
    status: &'static str,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// Create an order and notify the partner in the same transaction.
/// If the order creation fails, no webhook is sent.
async fn create_order(
    State(state): State<AppState>,
    Json(req): Json<CreateOrderRequest>,
) -> Json<OrderResponse> {
    let order_id = 1001i64; // In a real app, this comes from your DB INSERT

    // Transactional outbox: webhook event written atomically with the order
    let mut tx = state.engine.pool().begin().await.unwrap();

    // In a real app, you would INSERT your order here:
    // sqlx::query!("INSERT INTO orders (id, product_id, quantity) VALUES ($1, $2, $3)",
    //     order_id, req.product_id, req.quantity)
    //     .execute(&mut *tx).await.unwrap();

    state.engine
        .send_in_tx(
            "order.created",
            json!({
                "order_id": order_id,
                "product_id": req.product_id,
                "quantity": req.quantity,
            }),
            state.partner_endpoint_id,
            &mut tx,
        )
        .await
        .unwrap();

    tx.commit().await.unwrap();

    Json(OrderResponse { order_id, status: "created" })
}

/// Health check — also shows current queue state
async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stats = state.engine.queue_stats().await.unwrap_or_default();
    Json(json!({
        "status": "ok",
        "webhook_queue": {
            "pending": stats.pending,
            "delivering": stats.delivering,
            "failed": stats.failed,
            "dead": stats.dead,
            "delivered": stats.delivered,
        }
    }))
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().without_time().init();

    // 1. Connect and migrate
    let engine = Arc::new(
        WebhookEngine::builder()
            .database_url(DATABASE_URL)
            .build()
            .await?,
    );
    engine.migrate().await?;

    // 2. Register the partner endpoint (do this once at startup or via admin API)
    let endpoint = engine
        .register(PARTNER_WEBHOOK_URL, SIGNING_SECRET)
        .await?;
    println!("Partner endpoint: {}", endpoint.id);

    // 3. Build the axum app with the engine in shared state
    let state = AppState {
        engine: engine.clone(),
        partner_endpoint_id: endpoint.id,
    };

    let app = Router::new()
        .route("/orders", post(create_order))
        .route("/health", get(health))
        .with_state(state);

    // 4. Shutdown signal — Ctrl-C or SIGTERM
    let _shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        println!("Shutting down...");
    };

    // 5. Run the HTTP server and webhook worker concurrently.
    //    Both stop when the shutdown signal fires.
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("HTTP server on http://0.0.0.0:3000");
    println!("Try: curl -X POST http://localhost:3000/orders -H 'Content-Type: application/json' -d '{{\"product_id\":1,\"quantity\":2}}'");

    tokio::select! {
        // HTTP server
        result = axum::serve(listener, app) => {
            result?;
        }
        // Webhook delivery worker (drains current batch before stopping)
        _ = engine.run_graceful(async {
            tokio::signal::ctrl_c().await.ok();
        }) => {}
    }

    println!("Shutdown complete.");
    Ok(())
}
