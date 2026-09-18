//! Admin HTTP endpoints for webhooksmith operational visibility.
//!
//! Mount with [`admin`] to expose queue stats, endpoint listing, DLQ inspection,
//! and bulk DLQ retry — all backed directly by your [`WebhookEngine`].
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use axum::Router;
//! use webhooksmith::WebhookEngine;
//! use webhooksmith_axum::admin;
//!
//! # async fn example(engine: Arc<WebhookEngine>) {
//! let app: Router = Router::new()
//!     .nest("/admin", admin(engine));
//! # }
//! ```
//!
//! # Endpoints
//!
//! | Method | Path | Description |
//! |--------|------|-------------|
//! | `GET` | `/stats` | Queue stats (pending, delivering, failed, dead, delivered) |
//! | `GET` | `/endpoints` | All registered endpoints |
//! | `GET` | `/dlq/{endpoint_id}` | Dead events for an endpoint (paginated) |
//! | `POST` | `/dlq/{endpoint_id}/retry-all` | Re-queue all dead events + reset circuit |
//!
//! Query params for `/dlq/{id}`: `?limit=50&offset=0` (both optional, defaults shown).

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;
use webhooksmith::WebhookEngine;

// ── State ─────────────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AdminState {
    engine: Arc<WebhookEngine>,
}

// ── Request types ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct Pagination {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
}

fn default_limit() -> i64 { 50 }

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

fn err_json(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorBody { error: msg.into() }))
}

fn not_found(msg: impl Into<String>) -> (StatusCode, Json<ErrorBody>) {
    (StatusCode::NOT_FOUND, Json(ErrorBody { error: msg.into() }))
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn get_stats(State(s): State<AdminState>) -> impl IntoResponse {
    match s.engine.queue_stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => err_json(e.to_string()).into_response(),
    }
}

async fn get_endpoints(State(s): State<AdminState>) -> impl IntoResponse {
    match s.engine.list_endpoints().await {
        Ok(eps) => Json(eps).into_response(),
        Err(e) => err_json(e.to_string()).into_response(),
    }
}

async fn get_dlq(
    State(s): State<AdminState>,
    Path(endpoint_id): Path<Uuid>,
    Query(page): Query<Pagination>,
) -> impl IntoResponse {
    // First check the endpoint exists — dead_events_paged returns Ok([]) for unknown ids.
    match s.engine.endpoint(endpoint_id).await {
        Ok(None) => return not_found(format!("endpoint {endpoint_id} not found")).into_response(),
        Err(e)   => return err_json(e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    match s.engine.dead_events_paged(endpoint_id, page.limit.clamp(1, 200), page.offset.max(0)).await {
        Ok(events) => Json(events).into_response(),
        Err(e)     => err_json(e.to_string()).into_response(),
    }
}

#[derive(Serialize)]
struct RetryAllResponse {
    retried: u64,
}

async fn retry_all_dlq(
    State(s): State<AdminState>,
    Path(endpoint_id): Path<Uuid>,
) -> impl IntoResponse {
    // Validate the endpoint exists before calling retry_all_dead.
    // retry_all_dead returns Ok(0) for unknown ids with no error signal.
    match s.engine.endpoint(endpoint_id).await {
        Ok(None) => return not_found(format!("endpoint {endpoint_id} not found")).into_response(),
        Err(e)   => return err_json(e.to_string()).into_response(),
        Ok(Some(_)) => {}
    }
    match s.engine.retry_all_dead(endpoint_id).await {
        Ok(n) => Json(RetryAllResponse { retried: n }).into_response(),
        Err(e) => err_json(e.to_string()).into_response(),
    }
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Build an axum [`Router`] exposing admin endpoints backed by `engine`.
///
/// Mount it with `router.nest("/admin", admin(engine))` or at the root.
///
/// All endpoints return JSON. The circuit-breaker state (`consecutive_failures`,
/// `circuit_open_until`) is visible on each endpoint via `GET /endpoints`.
pub fn admin(engine: Arc<WebhookEngine>) -> Router {
    let state = AdminState { engine };
    Router::new()
        .route("/stats", get(get_stats))
        .route("/endpoints", get(get_endpoints))
        .route("/dlq/:endpoint_id", get(get_dlq))
        .route("/dlq/:endpoint_id/retry-all", post(retry_all_dlq))
        .with_state(state)
}
