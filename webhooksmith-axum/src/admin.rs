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
//! | `GET` | `/metrics` | Prometheus text exposition (scrape endpoint) |
//!
//! Query params for `/dlq/{id}`: `?limit=50&offset=0` (both optional, defaults shown).
//!
//! ## Prometheus metrics
//!
//! `GET /admin/metrics` returns standard Prometheus text format. Point your
//! Prometheus scraper at it — no extra configuration needed.
//!
//! Metrics exposed:
//! - `webhooksmith_events{status="pending|delivering|failed|dead|delivered"}` — current event counts by status
//! - `webhooksmith_endpoints{state="enabled|disabled|circuit_open"}` — current endpoint counts by state

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
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

// ── Prometheus metrics ────────────────────────────────────────────────────────

/// Render a single Prometheus gauge line.
fn gauge(out: &mut String, name: &str, labels: &str, value: i64) {
    out.push_str(&format!("{name}{{{labels}}} {value}\n"));
}

async fn get_metrics(State(s): State<AdminState>) -> Response {
    // Fetch queue stats and endpoint list concurrently.
    let (stats_res, endpoints_res) = tokio::join!(
        s.engine.queue_stats(),
        s.engine.list_endpoints(),
    );

    let (stats, endpoints) = match (stats_res, endpoints_res) {
        (Ok(st), Ok(ep)) => (st, ep),
        (Err(e), _) | (_, Err(e)) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let mut body = String::with_capacity(512);

    // ── Event counts ──────────────────────────────────────────────────────────
    body.push_str("# HELP webhooksmith_events Current number of webhook events by status.\n");
    body.push_str("# TYPE webhooksmith_events gauge\n");
    gauge(&mut body, "webhooksmith_events", r#"status="pending""#,    stats.pending);
    gauge(&mut body, "webhooksmith_events", r#"status="delivering""#, stats.delivering);
    gauge(&mut body, "webhooksmith_events", r#"status="failed""#,     stats.failed);
    gauge(&mut body, "webhooksmith_events", r#"status="dead""#,       stats.dead);
    gauge(&mut body, "webhooksmith_events", r#"status="delivered""#,  stats.delivered);

    // ── Endpoint counts ───────────────────────────────────────────────────────
    let enabled  = endpoints.iter().filter(|e| e.enabled && e.circuit_open_until.is_none()).count() as i64;
    let disabled = endpoints.iter().filter(|e| !e.enabled).count() as i64;
    let circuit_open = endpoints.iter().filter(|e| {
        e.circuit_open_until.map(|t| t > chrono::Utc::now()).unwrap_or(false)
    }).count() as i64;

    body.push_str("\n# HELP webhooksmith_endpoints Current number of registered endpoints by state.\n");
    body.push_str("# TYPE webhooksmith_endpoints gauge\n");
    gauge(&mut body, "webhooksmith_endpoints", r#"state="enabled""#,      enabled);
    gauge(&mut body, "webhooksmith_endpoints", r#"state="disabled""#,     disabled);
    gauge(&mut body, "webhooksmith_endpoints", r#"state="circuit_open""#, circuit_open);

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
        .into_response()
}

// ── Public entry point ────────────────────────────────────────────────────────

/// Build an axum [`Router`] exposing admin endpoints backed by `engine`.
///
/// Mount it with `router.nest("/admin", admin(engine))` or at the root.
///
/// All endpoints return JSON except `/metrics` which returns Prometheus text.
/// The circuit-breaker state (`consecutive_failures`, `circuit_open_until`)
/// is visible on each endpoint via `GET /endpoints` and `GET /metrics`.
pub fn admin(engine: Arc<WebhookEngine>) -> Router {
    let state = AdminState { engine };
    Router::new()
        .route("/stats", get(get_stats))
        .route("/endpoints", get(get_endpoints))
        .route("/dlq/:endpoint_id", get(get_dlq))
        .route("/dlq/:endpoint_id/retry-all", post(retry_all_dlq))
        .route("/metrics", get(get_metrics))
        .with_state(state)
}
