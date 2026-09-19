//! Actix-web integration for webhooksmith.
//!
//! Provides [`VerifiedWebhook`] and [`TypedWebhook<T>`] extractors that verify
//! incoming webhook signatures (HMAC-SHA256) and reject forged, stale, or
//! oversized requests.
//!
//! # Setup
//!
//! Register the signing secret via [`WebhookSecret`] app data, then use
//! [`VerifiedWebhook`] or [`TypedWebhook<T>`] as handler parameters:
//!
//! ```rust,no_run
//! use actix_web::{web, App, HttpServer, HttpResponse, Responder};
//! use webhooksmith_actix::{WebhookSecret, VerifiedWebhook, TypedWebhook};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct OrderCreated { order_id: u64 }
//!
//! async fn handle_raw(webhook: VerifiedWebhook) -> impl Responder {
//!     tracing::info!(event_type = %webhook.event_type, event_id = ?webhook.event_id, "received");
//!     HttpResponse::Ok().finish()
//! }
//!
//! async fn handle_typed(webhook: TypedWebhook<OrderCreated>) -> impl Responder {
//!     tracing::info!(order_id = %webhook.payload.order_id, "order created");
//!     HttpResponse::Ok().finish()
//! }
//!
//! # async fn run() -> std::io::Result<()> {
//! HttpServer::new(|| {
//!     App::new()
//!         .app_data(WebhookSecret::new("your-signing-secret"))
//!         .route("/webhooks", web::post().to(handle_raw))
//!         .route("/orders", web::post().to(handle_typed))
//! })
//! .bind("0.0.0.0:8080")?
//! .run()
//! .await
//! # }
//! ```
//!
//! # Rejection behaviour
//!
//! | Condition | Status | Body |
//! |-----------|--------|------|
//! | Missing `x-hooksmith-signature` | 401 | `{"error":"missing signature"}` |
//! | Missing `x-hooksmith-timestamp` | 400 | `{"error":"missing timestamp"}` |
//! | Invalid timestamp (not an integer) | 400 | `{"error":"invalid timestamp"}` |
//! | Stale timestamp (> 300 s skew) | 401 | `{"error":"stale timestamp"}` |
//! | Signature mismatch | 401 | `{"error":"invalid signature"}` |
//! | Body > 1 MB | 413 | `{"error":"payload too large"}` |
//! | Body is not valid JSON | 422 | `{"error":"body must be JSON"}` |

use actix_web::{
    FromRequest, HttpRequest, HttpResponse,
    dev::Payload,
    error::ResponseError,
    http::StatusCode,
    web::Bytes,
};
use serde::de::DeserializeOwned;
use std::{fmt, future::Future, pin::Pin, sync::Arc};
use webhooksmith::signing;

// ── Constants ─────────────────────────────────────────────────────────────────

const MAX_BODY_BYTES: usize = 1_048_576; // 1 MB
const TIMESTAMP_TOLERANCE_SECS: i64 = 300;

// ── WebhookSecret ─────────────────────────────────────────────────────────────

/// App data holding the webhook signing secret.
///
/// Register once via `.app_data(WebhookSecret::new("your-secret"))`.
#[derive(Clone)]
pub struct WebhookSecret(pub(crate) Arc<String>);

impl WebhookSecret {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(Arc::new(secret.into()))
    }
}

// ── WebhookPayload ────────────────────────────────────────────────────────────

/// Verified webhook metadata and body, produced by the extractors.
#[derive(Debug, Clone)]
pub struct WebhookPayload {
    /// Value of the `x-hooksmith-event-type` header (may be empty if absent).
    pub event_type: String,
    /// Value of the `x-hooksmith-event-id` header. `None` if the header was not sent.
    pub event_id: Option<String>,
    /// Unix timestamp from the `x-hooksmith-timestamp` header.
    pub timestamp: i64,
    /// Raw JSON body bytes (signature already verified).
    pub body: Bytes,
}

// ── Extraction error ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum WebhookError {
    MissingSignature,
    MissingTimestamp,
    InvalidTimestamp,
    StaleTimestamp,
    InvalidSignature,
    PayloadTooLarge,
    BodyNotJson,
    SecretNotConfigured,
    BodyReadError(String),
}

impl fmt::Display for WebhookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingSignature    => write!(f, "missing signature"),
            Self::MissingTimestamp    => write!(f, "missing timestamp"),
            Self::InvalidTimestamp    => write!(f, "invalid timestamp"),
            Self::StaleTimestamp      => write!(f, "stale timestamp"),
            Self::InvalidSignature    => write!(f, "invalid signature"),
            Self::PayloadTooLarge     => write!(f, "payload too large"),
            Self::BodyNotJson         => write!(f, "body must be JSON"),
            Self::SecretNotConfigured => write!(f, "webhook secret not configured"),
            Self::BodyReadError(e)    => write!(f, "body read error: {e}"),
        }
    }
}

impl ResponseError for WebhookError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::MissingSignature  => StatusCode::UNAUTHORIZED,
            Self::MissingTimestamp  => StatusCode::BAD_REQUEST,
            Self::InvalidTimestamp  => StatusCode::BAD_REQUEST,
            Self::StaleTimestamp    => StatusCode::UNAUTHORIZED,
            Self::InvalidSignature  => StatusCode::UNAUTHORIZED,
            Self::PayloadTooLarge   => StatusCode::PAYLOAD_TOO_LARGE,
            Self::BodyNotJson       => StatusCode::UNPROCESSABLE_ENTITY,
            Self::SecretNotConfigured => StatusCode::INTERNAL_SERVER_ERROR,
            Self::BodyReadError(_)  => StatusCode::BAD_REQUEST,
        }
    }

    fn error_response(&self) -> HttpResponse {
        let body = serde_json::json!({"error": self.to_string()});
        HttpResponse::build(self.status_code())
            .content_type("application/json")
            .json(body)
    }
}

// ── Core verification logic ───────────────────────────────────────────────────

async fn extract_and_verify(
    req: &HttpRequest,
    payload: &mut Payload,
) -> Result<WebhookPayload, WebhookError> {
    // Retrieve secret from app data
    let secret = req
        .app_data::<WebhookSecret>()
        .ok_or(WebhookError::SecretNotConfigured)?
        .0
        .clone();

    // Read required headers
    let sig = req
        .headers()
        .get("x-hooksmith-signature")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or(WebhookError::MissingSignature)?;

    let ts_str = req
        .headers()
        .get("x-hooksmith-timestamp")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .ok_or(WebhookError::MissingTimestamp)?;

    let timestamp: i64 = ts_str.parse().map_err(|_| WebhookError::InvalidTimestamp)?;

    // Stale timestamp guard
    let now = chrono::Utc::now().timestamp();
    if (now - timestamp).abs() > TIMESTAMP_TOLERANCE_SECS {
        return Err(WebhookError::StaleTimestamp);
    }

    // Read body with size cap using actix's body extractor
    use futures::StreamExt;
    let mut chunks: Vec<u8> = Vec::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| WebhookError::BodyReadError(e.to_string()))?;
        if chunks.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(WebhookError::PayloadTooLarge);
        }
        chunks.extend_from_slice(&chunk);
    }

    let body = Bytes::from(chunks);

    // Must be valid JSON
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return Err(WebhookError::BodyNotJson);
    }

    // Verify HMAC-SHA256 signature
    if !signing::verify(&secret, timestamp, &body, &sig) {
        return Err(WebhookError::InvalidSignature);
    }

    let event_type = req
        .headers()
        .get("x-hooksmith-event-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();

    let event_id = req
        .headers()
        .get("x-hooksmith-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    Ok(WebhookPayload { event_type, event_id, timestamp, body })
}

// ── VerifiedWebhook extractor ─────────────────────────────────────────────────

/// Actix-web extractor that verifies the incoming webhook signature.
///
/// On success, gives you the raw JSON body and metadata.
/// On failure, responds with a structured JSON error.
///
/// # Example
/// ```rust,no_run
/// use actix_web::{web, HttpResponse, Responder};
/// use webhooksmith_actix::VerifiedWebhook;
///
/// async fn handler(webhook: VerifiedWebhook) -> impl Responder {
///     println!("event_type: {}", webhook.event_type);
///     HttpResponse::Ok().finish()
/// }
/// ```
pub struct VerifiedWebhook(pub WebhookPayload);

impl std::ops::Deref for VerifiedWebhook {
    type Target = WebhookPayload;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl FromRequest for VerifiedWebhook {
    type Error = WebhookError;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let req = req.clone();
        let mut payload = payload.take();
        Box::pin(async move {
            let verified = extract_and_verify(&req, &mut payload).await?;
            Ok(VerifiedWebhook(verified))
        })
    }
}

// ── TypedWebhook<T> extractor ─────────────────────────────────────────────────

/// Actix-web extractor that verifies the signature AND deserializes the JSON body
/// into `T`.
///
/// # Example
/// ```rust,no_run
/// use actix_web::{web, HttpResponse, Responder};
/// use serde::Deserialize;
/// use webhooksmith_actix::TypedWebhook;
///
/// #[derive(Deserialize)]
/// struct OrderCreated { order_id: u64 }
///
/// async fn handler(webhook: TypedWebhook<OrderCreated>) -> impl Responder {
///     println!("order: {}", webhook.payload.order_id);
///     HttpResponse::Ok().finish()
/// }
/// ```
pub struct TypedWebhook<T> {
    pub payload: T,
    pub meta: WebhookPayload,
}

impl<T: DeserializeOwned + 'static> FromRequest for TypedWebhook<T> {
    type Error = WebhookError;
    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let req = req.clone();
        let mut payload = payload.take();
        Box::pin(async move {
            let meta = extract_and_verify(&req, &mut payload).await?;
            let typed: T = serde_json::from_slice(&meta.body)
                .map_err(|_| WebhookError::BodyNotJson)?;
            Ok(TypedWebhook { payload: typed, meta })
        })
    }
}
