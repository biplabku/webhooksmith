//! Axum integration for webhooksmith.
//!
//! # Verifying incoming webhooks
//!
//! Add the [`WebhookSecretLayer`] to your router, then use the
//! [`VerifiedWebhook`] extractor in any handler. It automatically verifies the
//! HMAC-SHA256 signature, rejects stale timestamps, and gives you the raw JSON
//! body. Use [`TypedWebhook<T>`] if you want automatic deserialization.
//!
//! ```rust,no_run
//! use axum::{Router, routing::post, http::StatusCode};
//! use webhooksmith_axum::{WebhookSecretLayer, VerifiedWebhook, TypedWebhook};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct OrderCreated { order_id: u64 }
//!
//! async fn handle_raw(VerifiedWebhook(body): VerifiedWebhook) -> StatusCode {
//!     tracing::info!(event_type = %body.event_type, "received webhook");
//!     StatusCode::OK
//! }
//!
//! async fn handle_typed(TypedWebhook(order): TypedWebhook<OrderCreated>) -> StatusCode {
//!     tracing::info!(order_id = order.order_id, "order created");
//!     StatusCode::OK
//! }
//!
//! let app: Router = Router::new()
//!     .route("/webhooks", post(handle_raw))
//!     .route("/orders", post(handle_typed))
//!     .layer(WebhookSecretLayer::new("your-signing-secret"));
//! ```

mod admin;
pub use admin::admin;

use axum::{
    async_trait,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use webhooksmith::signing;
use serde::de::DeserializeOwned;
use tower_layer::Layer;
use std::sync::Arc;

const MAX_BODY_BYTES: usize = 1_048_576; // 1 MB

// ── Secret storage ────────────────────────────────────────────────────────────

/// The signing secret injected by [`WebhookSecretLayer`].
#[derive(Clone)]
struct WebhookSecret(Arc<String>);

// ── Tower layer ───────────────────────────────────────────────────────────────

/// Tower middleware layer that injects the webhook signing secret into
/// request extensions so extractors can verify signatures.
///
/// Apply this to your router once at startup:
/// ```rust,no_run
/// # use axum::Router;
/// # use webhooksmith_axum::WebhookSecretLayer;
/// let app: Router = Router::new()
///     /* ... routes ... */
///     .layer(WebhookSecretLayer::new("your-secret"));
/// ```
#[derive(Clone)]
pub struct WebhookSecretLayer {
    secret: Arc<String>,
}

impl WebhookSecretLayer {
    pub fn new(secret: impl Into<String>) -> Self {
        Self { secret: Arc::new(secret.into()) }
    }
}

impl<S> Layer<S> for WebhookSecretLayer {
    type Service = WebhookSecretService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WebhookSecretService {
            inner,
            secret: self.secret.clone(),
        }
    }
}

/// The middleware service produced by [`WebhookSecretLayer`].
#[derive(Clone)]
pub struct WebhookSecretService<S> {
    inner: S,
    secret: Arc<String>,
}

impl<S, B> tower::Service<Request<B>> for WebhookSecretService<S>
where
    S: tower::Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        req.extensions_mut()
            .insert(WebhookSecret(self.secret.clone()));
        self.inner.call(req)
    }
}

// ── Extractor rejection ───────────────────────────────────────────────────────

/// Rejection type returned when signature verification fails.
#[derive(Debug)]
pub enum WebhookRejection {
    MissingSecret,
    MissingTimestamp,
    MissingSignature,
    BodyTooLarge,
    InvalidSignature,
    InvalidBody(serde_json::Error),
}

impl IntoResponse for WebhookRejection {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::MissingSecret => (StatusCode::INTERNAL_SERVER_ERROR, "webhook secret not configured"),
            Self::MissingTimestamp => (StatusCode::BAD_REQUEST, "missing x-hooksmith-timestamp header"),
            Self::MissingSignature => (StatusCode::UNAUTHORIZED, "missing x-hooksmith-signature header"),
            Self::BodyTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "request body too large"),
            Self::InvalidSignature => (StatusCode::UNAUTHORIZED, "invalid webhook signature"),
            Self::InvalidBody(_) => (StatusCode::UNPROCESSABLE_ENTITY, "invalid JSON body"),
        };
        (status, msg).into_response()
    }
}

// ── Verified webhook payload ──────────────────────────────────────────────────

/// The verified and parsed content of an incoming webhook request.
pub struct WebhookPayload {
    pub event_type: String,
    pub event_id: Option<String>,
    pub timestamp: i64,
    pub body: serde_json::Value,
}

async fn extract_and_verify(req: Request) -> Result<WebhookPayload, WebhookRejection> {
    // Retrieve the secret injected by WebhookSecretLayer
    let secret = req
        .extensions()
        .get::<WebhookSecret>()
        .ok_or(WebhookRejection::MissingSecret)?
        .0
        .clone();

    // Read required headers before consuming the body
    let timestamp: i64 = req
        .headers()
        .get("x-hooksmith-timestamp")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or(WebhookRejection::MissingTimestamp)?;

    let signature = req
        .headers()
        .get("x-hooksmith-signature")
        .and_then(|v| v.to_str().ok())
        .ok_or(WebhookRejection::MissingSignature)?
        .to_owned();

    let event_type = req
        .headers()
        .get("x-hooksmith-event-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_owned();

    let event_id = req
        .headers()
        .get("x-hooksmith-event-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());

    // Buffer the body up to the size limit
    let bytes = axum::body::to_bytes(req.into_body(), MAX_BODY_BYTES)
        .await
        .map_err(|_| WebhookRejection::BodyTooLarge)?;

    // Verify HMAC signature — rejects stale timestamps automatically
    if !signing::verify(&secret, timestamp, &bytes, &signature) {
        tracing::warn!(
            event_type = %event_type,
            "webhook signature verification failed"
        );
        return Err(WebhookRejection::InvalidSignature);
    }

    let body: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(WebhookRejection::InvalidBody)?;

    Ok(WebhookPayload { event_type, event_id, timestamp, body })
}

// ── VerifiedWebhook extractor ─────────────────────────────────────────────────

/// Axum extractor that verifies the webhooksmith HMAC-SHA256 signature and
/// returns the raw JSON payload.
///
/// Rejects with 401 if the signature is missing or invalid.
/// Rejects with 400 if the timestamp header is missing.
/// Requires [`WebhookSecretLayer`] on the router.
pub struct VerifiedWebhook(pub WebhookPayload);

#[async_trait]
impl<S> FromRequest<S> for VerifiedWebhook
where
    S: Send + Sync,
{
    type Rejection = WebhookRejection;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self(extract_and_verify(req).await?))
    }
}

// ── TypedWebhook<T> extractor ─────────────────────────────────────────────────

/// Axum extractor that verifies the webhooksmith signature and deserializes the
/// JSON body into `T`.
///
/// Returns 422 if the body doesn't match `T`.
pub struct TypedWebhook<T>(pub T);

#[async_trait]
impl<S, T> FromRequest<S> for TypedWebhook<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = WebhookRejection;

    async fn from_request(req: Request, _state: &S) -> Result<Self, Self::Rejection> {
        let payload = extract_and_verify(req).await?;
        let typed: T =
            serde_json::from_value(payload.body).map_err(WebhookRejection::InvalidBody)?;
        Ok(Self(typed))
    }
}
