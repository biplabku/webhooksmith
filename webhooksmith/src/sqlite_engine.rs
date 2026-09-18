//! SQLite-backed WebhookEngine.
//!
//! Usage:
//! ```rust,no_run
//! use webhooksmith::SqliteEngine;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = SqliteEngine::new("sqlite:webhooks.db").await?;
//!     engine.migrate().await?;
//!     // Same API as WebhookEngine
//!     Ok(())
//! }
//! ```

use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use std::time::Duration;
use uuid::Uuid;

use crate::{
    backends::sqlite as db,
    error::Result,
    model::{
        DeliveryAttempt, Endpoint, EventStatus, NewEndpoint, QueueStats,
        UpdateEndpoint, WebhookEvent,
    },
    worker::SsrfSafeDnsResolver,
};

/// Webhook delivery engine backed by SQLite.
///
/// Identical API to [`WebhookEngine`](crate::WebhookEngine) but uses SQLite
/// instead of Postgres. Useful for smaller apps, desktop tools, CLI programs,
/// and embedded systems that don't run Postgres.
///
/// # SQLite-specific notes
/// - Pool is limited to 1 writer connection (SQLite serializes writes)
/// - `FOR UPDATE SKIP LOCKED` is not available — write serialization provides safety
/// - UUIDs stored as TEXT, JSON stored as TEXT
/// - WAL mode enabled for concurrent reads
pub struct SqliteEngine {
    pool: SqlitePool,
    allow_insecure_urls: bool,
}

impl SqliteEngine {
    /// Connect to a SQLite database.
    ///
    /// `url` can be:
    /// - `"sqlite:webhooks.db"` — file-based
    /// - `"sqlite::memory:"` — in-memory (useful for tests)
    pub async fn new(url: &str) -> Result<Self> {
        Self::builder().database_url(url).build().await
    }

    pub fn builder() -> SqliteEngineBuilder {
        SqliteEngineBuilder::default()
    }

    /// Run pending migrations. Safe to call on every startup.
    /// Idempotent — uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.
    pub async fn migrate(&self) -> Result<()> {
        // Run all migration files in order
        let migrations = [
            include_str!("../migrations-sqlite/0001_initial.sql"),
            include_str!("../migrations-sqlite/0002_event_filter.sql"),
            include_str!("../migrations-sqlite/0003_circuit_breaker.sql"),
        ];
        for sql in &migrations {
            // SQLite doesn't support "ADD COLUMN IF NOT EXISTS" directly, so we
            // handle the "duplicate column" error gracefully.
            if let Err(e) = sqlx::raw_sql(sql).execute(&self.pool).await {
                let msg = e.to_string();
                if !msg.contains("duplicate column") {
                    return Err(crate::error::HooksmithError::Database(e.into()));
                }
                // duplicate column = migration already applied, skip
            }
        }
        Ok(())
    }

    /// Register a new webhook endpoint.
    pub async fn register(&self, url: &str, secret: &str) -> Result<Endpoint> {
        let config = NewEndpoint {
            url: url.to_owned(),
            signing_secret: secret.to_owned(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        if !self.allow_insecure_urls {
            config.validate()?;
        } else {
            config.validate_fields()?;
        }
        db::create_endpoint(&self.pool, config).await
    }

    pub async fn register_with(&self, config: NewEndpoint) -> Result<Endpoint> {
        if !self.allow_insecure_urls {
            config.validate()?;
        } else {
            config.validate_fields()?;
        }
        db::create_endpoint(&self.pool, config).await
    }

    pub async fn update_endpoint(&self, id: Uuid, update: UpdateEndpoint) -> Result<Endpoint> {
        update.validate(self.allow_insecure_urls)?;
        db::update_endpoint_field(
            &self.pool, id,
            update.url, update.signing_secret,
            update.description.is_some(), update.description.flatten(),
            update.enabled, update.max_attempts, update.initial_delay_ms,
        ).await
    }

    pub async fn enable_endpoint(&self, id: Uuid) -> Result<Endpoint> {
        db::update_endpoint_field(&self.pool, id, None, None, false, None, Some(true), None, None).await
    }

    pub async fn disable_endpoint(&self, id: Uuid) -> Result<Endpoint> {
        db::update_endpoint_field(&self.pool, id, None, None, false, None, Some(false), None, None).await
    }

    /// Set the event type filter — only matching events from broadcast() are delivered.
    pub async fn set_event_filter(&self, id: Uuid, patterns: Vec<String>) -> Result<Endpoint> {
        // Store as JSON in SQLite
        let filter_json = serde_json::to_string(&patterns).ok();
        sqlx::query("UPDATE webhook_endpoints SET event_filter = ? WHERE id = ?")
            .bind(filter_json)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        db::get_endpoint_by_id(&self.pool, id).await?
            .ok_or(crate::error::HooksmithError::EndpointNotFound(id))
    }

    /// Remove the event type filter — endpoint receives all events from broadcast() again.
    pub async fn clear_event_filter(&self, id: Uuid) -> Result<Endpoint> {
        sqlx::query("UPDATE webhook_endpoints SET event_filter = NULL WHERE id = ?")
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        db::get_endpoint_by_id(&self.pool, id).await?
            .ok_or(crate::error::HooksmithError::EndpointNotFound(id))
    }

    pub async fn endpoint(&self, id: Uuid) -> Result<Option<Endpoint>> {
        db::get_endpoint_by_id(&self.pool, id).await
    }

    pub async fn list_endpoints(&self) -> Result<Vec<Endpoint>> {
        db::list_endpoints(&self.pool).await
    }

    pub async fn list_endpoints_paged(&self, limit: i64, offset: i64) -> Result<Vec<Endpoint>> {
        db::list_endpoints_paged(&self.pool, limit, offset).await
    }

    pub async fn delete_endpoint(&self, id: Uuid) -> Result<()> {
        db::delete_endpoint(&self.pool, id).await
    }

    // ── Sending ───────────────────────────────────────────────────────────────

    pub async fn send(&self, event_type: &str, payload: serde_json::Value, endpoint_id: Uuid) -> Result<WebhookEvent> {
        crate::storage::validate_enqueue_public(event_type, &payload)?;
        db::enqueue(&self.pool, endpoint_id, event_type, payload).await
    }

    /// Enqueue inside an existing SQLite transaction — true transactional outbox.
    ///
    /// The event is written using the transaction's connection. If `tx` is rolled back,
    /// the event is also rolled back. If `tx` commits, the event is persisted.
    pub async fn send_in_tx(&self, event_type: &str, payload: serde_json::Value, endpoint_id: Uuid, tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>) -> Result<WebhookEvent> {
        crate::storage::validate_enqueue_public(event_type, &payload)?;
        db::enqueue_in_sqlite_tx(tx, endpoint_id, event_type, payload).await
    }

    pub async fn send_idempotent(&self, event_type: &str, payload: serde_json::Value, endpoint_id: Uuid, key: &str) -> Result<WebhookEvent> {
        crate::storage::validate_enqueue_public(event_type, &payload)?;
        db::enqueue_idempotent(&self.pool, endpoint_id, event_type, payload, key).await
    }

    pub async fn broadcast(&self, event_type: &str, payload: serde_json::Value) -> Result<Vec<WebhookEvent>> {
        crate::storage::validate_enqueue_public(event_type, &payload)?;
        db::broadcast(&self.pool, event_type, payload).await
    }

    pub async fn broadcast_idempotent(&self, event_type: &str, payload: serde_json::Value, key: &str) -> Result<Vec<WebhookEvent>> {
        crate::storage::validate_enqueue_public(event_type, &payload)?;
        db::broadcast_idempotent(&self.pool, event_type, payload, key).await
    }

    // ── Querying ──────────────────────────────────────────────────────────────

    pub async fn event(&self, id: Uuid) -> Result<Option<WebhookEvent>> {
        db::get_event(&self.pool, id).await
    }

    pub async fn delivery_log(&self, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
        db::delivery_log(&self.pool, event_id).await
    }

    pub async fn dead_events(&self, endpoint_id: Uuid) -> Result<Vec<WebhookEvent>> {
        db::list_dead_events(&self.pool, endpoint_id).await
    }

    pub async fn dead_events_paged(&self, endpoint_id: Uuid, limit: i64, offset: i64) -> Result<Vec<WebhookEvent>> {
        db::dead_events_paged(&self.pool, endpoint_id, limit, offset).await
    }

    pub async fn queue_stats(&self) -> Result<QueueStats> {
        db::queue_stats(&self.pool).await
    }

    pub async fn events_by_status(&self, endpoint_id: Uuid, status: EventStatus, limit: i64, offset: i64) -> Result<Vec<WebhookEvent>> {
        let s = crate::engine::event_status_to_str(&status);
        db::events_by_status(&self.pool, endpoint_id, s, limit, offset).await
    }

    pub async fn events_global(&self, status: EventStatus, limit: i64, offset: i64) -> Result<Vec<WebhookEvent>> {
        let s = crate::engine::event_status_to_str(&status);
        db::events_global_by_status(&self.pool, s, limit, offset).await
    }

    // ── DLQ ──────────────────────────────────────────────────────────────────

    pub async fn retry_dead(&self, event_id: Uuid) -> Result<()> {
        db::retry_dead_event(&self.pool, event_id).await
    }

    pub async fn retry_all_dead(&self, endpoint_id: Uuid) -> Result<u64> {
        db::retry_all_dead(&self.pool, endpoint_id).await
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    pub async fn cleanup_delivered(&self, older_than: std::time::Duration) -> Result<u64> {
        db::cleanup_delivered(&self.pool, older_than.as_secs() as i64).await
    }

    pub async fn cleanup_dead(&self, older_than: std::time::Duration) -> Result<u64> {
        db::cleanup_dead(&self.pool, older_than.as_secs() as i64).await
    }

    pub async fn recover_stuck_deliveries(&self, timeout: std::time::Duration) -> Result<u64> {
        db::recover_stuck_deliveries(&self.pool, timeout.as_secs() as i64).await
    }

    // ── Worker ────────────────────────────────────────────────────────────────

    /// Run the delivery worker. Blocks until the process exits.
    pub async fn run(&self) -> ! {
        let http_timeout = Duration::from_secs(30);
        let stuck_timeout_secs = 120i64;
        let client = build_client(http_timeout);

        loop {
            let n = self.run_once_inner(&client, stuck_timeout_secs).await;
            if n == 0 {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    pub async fn run_graceful<F: std::future::Future<Output = ()>>(&self, shutdown: F) {
        let http_timeout = Duration::from_secs(30);
        let stuck_timeout_secs = 120i64;
        let client = build_client(http_timeout);
        tokio::pin!(shutdown);
        loop {
            let n = self.run_once_inner(&client, stuck_timeout_secs).await;
            if n == 0 {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                }
            } else {
                tokio::select! {
                    biased;
                    _ = &mut shutdown => return,
                    _ = std::future::ready(()) => {}
                }
            }
        }
    }

    pub async fn run_once(&self) -> Result<usize> {
        let client = build_client(Duration::from_secs(30));
        Ok(self.run_once_inner(&client, 120).await)
    }

    async fn run_once_inner(&self, client: &reqwest::Client, stuck_timeout_secs: i64) -> usize {
        // Recover stuck events
        if let Ok(n) = db::recover_stuck_deliveries(&self.pool, stuck_timeout_secs).await {
            if n > 0 { tracing::warn!(count = n, "reset stuck SQLite events"); }
        }

        let events = match db::claim_due_events(&self.pool, 50).await {
            Ok(e) => e,
            Err(e) => { tracing::error!(error = %e, "claim_due_events failed"); return 0; }
        };

        let count = events.len();
        if count == 0 { return 0; }

        let pool = self.pool.clone();
        let client = client.clone();

        // Deliver sequentially (SQLite is single-writer; parallel tasks would contend)
        for event in events {
            if let Err(e) = deliver_sqlite_event(&pool, &client, &event).await {
                tracing::error!(event_id = %event.id, error = %e, "SQLite delivery failed");
            }
        }

        count
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

// ── Builder ───────────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct SqliteEngineBuilder {
    database_url: Option<String>,
    allow_insecure_urls: bool,
}

impl SqliteEngineBuilder {
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    pub fn allow_insecure_urls(mut self) -> Self {
        self.allow_insecure_urls = true;
        self
    }

    pub async fn build(self) -> Result<SqliteEngine> {
        let url = self.database_url.expect("database_url required");

        let pool = SqlitePoolOptions::new()
            .max_connections(1)  // Single writer — SQLite safety guarantee
            .connect(&url)
            .await?;

        // Enable WAL mode for concurrent reads
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await?;

        Ok(SqliteEngine { pool, allow_insecure_urls: self.allow_insecure_urls })
    }
}

// ── HTTP delivery ─────────────────────────────────────────────────────────────

fn build_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(std::sync::Arc::new(SsrfSafeDnsResolver))
        .build()
        .expect("failed to build HTTP client")
}

async fn deliver_sqlite_event(
    pool: &SqlitePool,
    client: &reqwest::Client,
    event: &WebhookEvent,
) -> Result<()> {
    use crate::worker::MAX_RESPONSE_BODY_BYTES;
    use futures::StreamExt;

    let endpoint = match db::get_endpoint_by_id(pool, event.endpoint_id).await? {
        Some(ep) => ep,
        None => {
            db::record_endpoint_deleted(pool, event.id).await;
            return Ok(());
        }
    };

    if !endpoint.enabled {
        db::reset_to_pending(pool, event.id).await?;
        return Ok(());
    }

    let payload_bytes = serde_json::to_vec(&event.payload)
        .map_err(|e| crate::error::HooksmithError::Config(format!("payload: {e}")))?;

    let timestamp = chrono::Utc::now().timestamp();
    let signature = crate::signing::sign(&endpoint.signing_secret, timestamp, &payload_bytes)?;

    let started = std::time::Instant::now();
    let response = client
        .post(&endpoint.url)
        .header("content-type", "application/json")
        .header("x-hooksmith-timestamp", timestamp.to_string())
        .header("x-hooksmith-signature", &signature)
        .header("x-hooksmith-event-id", event.id.to_string())
        .header("x-hooksmith-event-type", &event.event_type)
        .body(payload_bytes)
        .send()
        .await;

    let duration_ms = started.elapsed().as_millis().min(i32::MAX as u128) as i32;

    match response {
        Ok(resp) => {
            let status = resp.status().as_u16() as i32;
            let mut buf = Vec::with_capacity(MAX_RESPONSE_BODY_BYTES.min(1024));
            let mut stream = resp.bytes_stream();
            while let Some(chunk) = stream.next().await {
                if let Ok(b) = chunk {
                    let rem = MAX_RESPONSE_BODY_BYTES.saturating_sub(buf.len());
                    if rem == 0 { break; }
                    buf.extend_from_slice(&b[..b.len().min(rem)]);
                }
            }
            let body = if buf.is_empty() { None } else { Some(String::from_utf8_lossy(&buf).into_owned()) };

            if (200..300).contains(&status) {
                db::record_success(pool, event.id, status, body, duration_ms).await?;
            } else {
                db::record_failure(pool, event.id, endpoint.id, endpoint.max_attempts, endpoint.initial_delay_ms,
                    format!("HTTP {status}"), Some(status), Some(duration_ms)).await?;
            }
        }
        Err(e) => {
            db::record_failure(pool, event.id, endpoint.id, endpoint.max_attempts, endpoint.initial_delay_ms,
                e.to_string(), None, Some(duration_ms)).await?;
        }
    }

    Ok(())
}
