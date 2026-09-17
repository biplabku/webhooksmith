use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;
use uuid::Uuid;

use crate::{
    error::Result,
    model::{DeliveryAttempt, Endpoint, EventStatus, NewEndpoint, QueueStats, UpdateEndpoint, WebhookEvent},
    storage,
    worker::{self, DeliveryWorker},
};

/// Builder for [`WebhookEngine`].
pub struct WebhookEngineBuilder {
    database_url: Option<String>,
    pool: Option<PgPool>,
    allow_insecure_urls: bool,
    batch_size: i64,
    poll_interval: Duration,
    max_connections: u32,
    acquire_timeout: Duration,
    http_timeout: Duration,
    stuck_timeout: Duration,
}

impl Default for WebhookEngineBuilder {
    fn default() -> Self {
        Self {
            database_url: None,
            pool: None,
            allow_insecure_urls: false,
            batch_size: worker::DEFAULT_BATCH_SIZE,
            poll_interval: worker::DEFAULT_POLL_INTERVAL,
            // Default pool large enough for the default batch.
            // Connections are held briefly (one query at a time per task),
            // so pool < batch_size is still correct — just limits throughput.
            max_connections: 20,
            acquire_timeout: Duration::from_secs(10),
            http_timeout: worker::DEFAULT_HTTP_TIMEOUT,
            stuck_timeout: worker::DEFAULT_STUCK_TIMEOUT,
        }
    }
}

impl WebhookEngineBuilder {
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }

    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    /// Allow http:// and private/loopback addresses as endpoint URLs.
    /// **For local development only** — never use in production.
    pub fn allow_insecure_urls(mut self) -> Self {
        self.allow_insecure_urls = true;
        self
    }

    /// Number of events to claim and deliver per worker cycle (default: 50).
    /// Must be >= 1. Panics if zero or negative.
    pub fn batch_size(mut self, n: i64) -> Self {
        assert!(n >= 1, "batch_size must be >= 1, got {n}");
        self.batch_size = n;
        self
    }

    pub fn poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Maximum Postgres connections in the pool (default: 20).
    ///
    /// For maximum throughput set this close to `batch_size`. Connections are
    /// held for individual queries only (not during HTTP delivery), so
    /// `pool < batch_size` is still correct — it just adds queuing overhead.
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }

    /// How long a task waits to acquire a connection before returning an error
    /// (default: 10s). Under sustained overload this caps the delay rather than
    /// letting tasks hang for sqlx's 30s default.
    pub fn acquire_timeout(mut self, d: Duration) -> Self {
        self.acquire_timeout = d;
        self
    }

    /// Maximum time the worker waits for an HTTP response from an endpoint
    /// (default: 30s). Set lower to detect slow endpoints faster; set higher
    /// if your partners have legitimately slow responses.
    /// Must be less than `stuck_timeout` so the reaper does not reset
    /// in-flight deliveries before the HTTP timeout fires.
    pub fn http_timeout(mut self, d: Duration) -> Self {
        self.http_timeout = d;
        self
    }

    /// How long an event can stay in `delivering` state before the reaper
    /// resets it to `pending` (default: 120s).
    /// Should be comfortably longer than `http_timeout` so legitimate slow
    /// deliveries are not interrupted.
    pub fn stuck_timeout(mut self, d: Duration) -> Self {
        self.stuck_timeout = d;
        self
    }

    pub async fn build(self) -> Result<WebhookEngine> {
        Self::validate_timeouts(self.http_timeout, self.stuck_timeout);
        let WebhookEngineBuilder {
            database_url, pool, allow_insecure_urls,
            batch_size, poll_interval, max_connections, acquire_timeout,
            http_timeout, stuck_timeout,
        } = self;
        let pool = match pool {
            Some(p) => p,
            None => {
                let url = database_url.expect("database_url or pool required");
                PgPoolOptions::new()
                    .max_connections(max_connections)
                    .acquire_timeout(acquire_timeout)
                    .connect(&url)
                    .await?
            }
        };
        Ok(make_engine(pool, allow_insecure_urls, batch_size, poll_interval, http_timeout, stuck_timeout))
    }

    fn validate_timeouts(http_timeout: Duration, stuck_timeout: Duration) {
        assert!(
            http_timeout < stuck_timeout,
            "http_timeout ({:?}) must be less than stuck_timeout ({:?}). \
             If http_timeout >= stuck_timeout the reaper resets events that \
             are still waiting for an HTTP response.",
            http_timeout,
            stuck_timeout
        );
    }

    /// Build synchronously when a pool is already available (useful in tests).
    /// Panics if `database_url` was set instead of `pool`.
    pub fn build_sync(self) -> WebhookEngine {
        Self::validate_timeouts(self.http_timeout, self.stuck_timeout);
        let WebhookEngineBuilder {
            pool, allow_insecure_urls, batch_size, poll_interval,
            http_timeout, stuck_timeout, ..
        } = self;
        let pool = pool.expect("build_sync requires a pool, not a database_url");
        make_engine(pool, allow_insecure_urls, batch_size, poll_interval, http_timeout, stuck_timeout)
    }
}

fn event_status_to_str(status: &EventStatus) -> &'static str {
    match status {
        EventStatus::Pending => "pending",
        EventStatus::Delivering => "delivering",
        EventStatus::Delivered => "delivered",
        EventStatus::Failed => "failed",
        EventStatus::Dead => "dead",
    }
}

fn make_engine(
    pool: PgPool,
    allow_insecure_urls: bool,
    batch_size: i64,
    poll_interval: Duration,
    http_timeout: Duration,
    stuck_timeout: Duration,
) -> WebhookEngine {
    let worker = DeliveryWorker::new(pool.clone())
        .with_batch_size(batch_size)
        .with_poll_interval(poll_interval)
        .with_http_timeout(http_timeout)
        .with_stuck_timeout(stuck_timeout);
    WebhookEngine { pool, worker, allow_insecure_urls }
}

// ── Engine ────────────────────────────────────────────────────────────────────

/// The main entry point for webhooksmith.
///
/// # Quick start
///
/// ```rust,no_run
/// use webhooksmith::WebhookEngine;
/// use serde_json::json;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let engine = WebhookEngine::builder()
///         .database_url("postgres://user:pass@localhost/mydb")
///         .build()
///         .await?;
///
///     engine.migrate().await?;
///
///     let endpoint = engine
///         .register("https://partner.example.com/webhooks", "my-secret-32-chars")
///         .await?;
///
///     // Simple fire-and-forget dispatch
///     engine.send("order.created", json!({"id": 1}), endpoint.id).await?;
///
///     // Transactional outbox — event only delivered if transaction commits
///     let mut tx = engine.pool().begin().await?;
///     engine.send_in_tx("order.created", json!({"id": 2}), endpoint.id, &mut tx).await?;
///     tx.commit().await?;
///
///     // Start the background delivery worker (blocks until process exits)
///     engine.run().await;
/// }
/// ```
pub struct WebhookEngine {
    pool: PgPool,
    worker: DeliveryWorker,
    allow_insecure_urls: bool,
}

impl WebhookEngine {
    /// Start configuring a new engine.
    pub fn builder() -> WebhookEngineBuilder {
        WebhookEngineBuilder::default()
    }

    /// Shortcut: connect directly from a database URL with default settings.
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::builder().database_url(database_url).build().await
    }

    /// Build from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        let worker = DeliveryWorker::new(pool.clone());
        Self { pool, worker, allow_insecure_urls: false }
    }

    /// Run pending database migrations. Safe to call on every startup.
    pub async fn migrate(&self) -> Result<()> {
        sqlx::migrate!()
            .run(&self.pool)
            .await
            .map_err(|e| crate::error::HooksmithError::Database(e.into()))?;
        Ok(())
    }

    /// Register a new webhook endpoint.
    ///
    /// `url` must be an `https://` URL pointing to a public address unless the
    /// engine was built with `.allow_insecure_urls()`.
    ///
    /// `secret` must be at least 16 characters.
    pub async fn register(&self, url: &str, secret: &str) -> Result<Endpoint> {
        let config = NewEndpoint {
            url: url.to_owned(),
            signing_secret: secret.to_owned(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
        };
        if self.allow_insecure_urls {
            storage::create_endpoint_unchecked(&self.pool, config).await
        } else {
            storage::create_endpoint(&self.pool, config).await
        }
    }

    /// Register with full configuration control.
    pub async fn register_with(&self, config: NewEndpoint) -> Result<Endpoint> {
        if self.allow_insecure_urls {
            storage::create_endpoint_unchecked(&self.pool, config).await
        } else {
            storage::create_endpoint(&self.pool, config).await
        }
    }

    /// Update an existing endpoint's fields. Only provided fields are changed.
    /// `updated_at` is set automatically by the database.
    pub async fn update_endpoint(&self, id: Uuid, update: UpdateEndpoint) -> Result<Endpoint> {
        storage::update_endpoint(&self.pool, id, update, self.allow_insecure_urls).await
    }

    /// Disable an endpoint — the worker will not claim its events until re-enabled.
    pub async fn disable_endpoint(&self, id: Uuid) -> Result<Endpoint> {
        storage::update_endpoint(
            &self.pool,
            id,
            UpdateEndpoint { enabled: Some(false), ..Default::default() },
            self.allow_insecure_urls,
        )
        .await
    }

    /// Re-enable a previously disabled endpoint.
    pub async fn enable_endpoint(&self, id: Uuid) -> Result<Endpoint> {
        storage::update_endpoint(
            &self.pool,
            id,
            UpdateEndpoint { enabled: Some(true), ..Default::default() },
            self.allow_insecure_urls,
        )
        .await
    }

    /// Dispatch with an idempotency key — safe to call multiple times.
    ///
    /// If `key` was already used for this endpoint, returns the existing event
    /// without creating a duplicate. Protects against caller retries.
    pub async fn send_idempotent(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        endpoint_id: Uuid,
        key: &str,
    ) -> Result<WebhookEvent> {
        storage::enqueue_idempotent(&self.pool, endpoint_id, event_type, payload, key).await
    }

    /// Idempotent dispatch inside an existing transaction.
    pub async fn send_idempotent_in_tx(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        endpoint_id: Uuid,
        key: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<WebhookEvent> {
        storage::enqueue_idempotent_in_tx(tx, endpoint_id, event_type, payload, key).await
    }

    /// Broadcast with an idempotency key — safe to call multiple times.
    ///
    /// Per-endpoint deduplication: the same key is independent across endpoints.
    pub async fn broadcast_idempotent(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        key: &str,
    ) -> Result<Vec<WebhookEvent>> {
        storage::broadcast_idempotent(&self.pool, event_type, payload, key).await
    }

    /// Broadcast an event to every enabled endpoint atomically.
    ///
    /// Uses a single `INSERT … SELECT` — no extra transaction needed.
    /// Returns one [`WebhookEvent`] per endpoint notified.
    /// Returns an empty vec (not an error) if no endpoints are registered.
    pub async fn broadcast(
        &self,
        event_type: &str,
        payload: serde_json::Value,
    ) -> Result<Vec<WebhookEvent>> {
        storage::broadcast(&self.pool, event_type, payload).await
    }

    /// Broadcast inside an existing transaction (transactional outbox pattern).
    /// Events only exist if the transaction commits.
    pub async fn broadcast_in_tx(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<Vec<WebhookEvent>> {
        storage::broadcast_in_tx(tx, event_type, payload).await
    }

    /// Dispatch a webhook event for delivery.
    pub async fn send(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        endpoint_id: Uuid,
    ) -> Result<WebhookEvent> {
        storage::enqueue(&self.pool, endpoint_id, event_type, payload).await
    }

    /// Dispatch inside an existing transaction (transactional outbox pattern).
    /// The event only exists if the transaction commits — guarantees no silent drops.
    pub async fn send_in_tx(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        endpoint_id: Uuid,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> Result<WebhookEvent> {
        storage::enqueue_in_tx(tx, endpoint_id, event_type, payload).await
    }

    /// Get a single endpoint by ID. Returns `None` if not found.
    pub async fn endpoint(&self, id: Uuid) -> Result<Option<Endpoint>> {
        storage::get_endpoint(&self.pool, id).await
    }

    /// All registered endpoints, ordered by creation time.
    /// For large installations use `list_endpoints_paged`.
    pub async fn list_endpoints(&self) -> Result<Vec<Endpoint>> {
        storage::list_endpoints(&self.pool).await
    }

    /// Paginated endpoint list. Negative limit/offset are clamped to 0.
    pub async fn list_endpoints_paged(&self, limit: i64, offset: i64) -> Result<Vec<Endpoint>> {
        storage::list_endpoints_paged(&self.pool, limit, offset).await
    }

    /// Events across ALL endpoints with a specific status, paginated.
    ///
    /// Use for global monitoring without knowing the endpoint ID:
    /// ```rust,no_run
    /// # use webhooksmith::{WebhookEngine, EventStatus, HooksmithError};
    /// # async fn example(engine: WebhookEngine) -> Result<(), HooksmithError> {
    /// // "Show me all failed events right now"
    /// let failed = engine.events_global(EventStatus::Failed, 50, 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn events_global(
        &self,
        status: EventStatus,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WebhookEvent>> {
        let s = event_status_to_str(&status);
        storage::events_global_by_status(&self.pool, s, limit, offset).await
    }

    /// Delete an endpoint and all its events (via CASCADE).
    /// Returns `EndpointNotFound` if the ID does not exist.
    pub async fn delete_endpoint(&self, id: Uuid) -> Result<()> {
        storage::delete_endpoint(&self.pool, id).await
    }

    /// Counts of webhook events grouped by status.
    /// Use this to monitor queue health and alert on DLQ growth.
    pub async fn queue_stats(&self) -> Result<QueueStats> {
        storage::queue_stats(&self.pool).await
    }

    /// Paginated list of events for an endpoint with a specific status.
    ///
    /// `limit` and `offset` follow standard SQL semantics.
    /// Events are ordered newest-first (`created_at DESC`).
    pub async fn events_by_status(
        &self,
        endpoint_id: Uuid,
        status: EventStatus,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WebhookEvent>> {
        let s = event_status_to_str(&status);
        storage::events_by_status(&self.pool, endpoint_id, s, limit, offset).await
    }

    /// Delivery history for an event.
    pub async fn delivery_log(&self, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
        storage::delivery_log(&self.pool, event_id).await
    }

    /// Events in the dead letter queue for an endpoint (all, unordered).
    pub async fn dead_events(&self, endpoint_id: Uuid) -> Result<Vec<WebhookEvent>> {
        storage::list_dead_events(&self.pool, endpoint_id).await
    }

    /// Paginated dead events for an endpoint, newest first.
    pub async fn dead_events_paged(
        &self,
        endpoint_id: Uuid,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<WebhookEvent>> {
        storage::dead_events_paged(&self.pool, endpoint_id, limit, offset).await
    }

    /// Requeue a single dead event for another delivery attempt.
    pub async fn retry_dead(&self, event_id: Uuid) -> Result<()> {
        storage::retry_dead_event(&self.pool, event_id).await
    }

    /// Requeue every dead event for an endpoint.
    /// Returns the number of events requeued.
    pub async fn retry_all_dead(&self, endpoint_id: Uuid) -> Result<u64> {
        storage::retry_all_dead(&self.pool, endpoint_id).await
    }

    /// Delete delivered events older than `older_than`.
    /// Returns the number of events deleted.
    /// Only removes `delivered` events — pending, failed, and dead require explicit action.
    pub async fn cleanup_delivered(&self, older_than: std::time::Duration) -> Result<u64> {
        storage::cleanup_delivered(&self.pool, older_than.as_secs() as i64).await
    }

    /// Delete dead (DLQ) events older than `older_than`.
    /// Returns the number of events deleted.
    /// Use this periodically to prevent dead events from accumulating indefinitely.
    pub async fn cleanup_dead(&self, older_than: std::time::Duration) -> Result<u64> {
        storage::cleanup_dead(&self.pool, older_than.as_secs() as i64).await
    }

    /// Get a single event by ID.
    pub async fn event(&self, id: Uuid) -> Result<Option<WebhookEvent>> {
        storage::get_event(&self.pool, id).await
    }

    /// Reset events stuck in 'delivering' for longer than `timeout`.
    /// The worker calls this automatically each cycle using `stuck_timeout`.
    /// Expose here for ops use (e.g. manual recovery after a crash).
    pub async fn recover_stuck_deliveries(&self, timeout: std::time::Duration) -> Result<u64> {
        storage::recover_stuck_deliveries(&self.pool, timeout.as_secs() as i64).await
    }

    /// The underlying Postgres pool — use this to begin transactions for the outbox.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Start the background delivery worker. Runs until the process exits.
    pub async fn run(&self) -> ! {
        self.worker.run().await
    }

    /// Run until `shutdown` resolves — for graceful pod / process shutdown.
    ///
    /// The current delivery batch (if any) completes before the worker stops.
    /// No new batch is claimed after the signal fires. At-least-once delivery
    /// is preserved: any unclaimed events are picked up on next startup.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use webhooksmith::WebhookEngine;
    /// # async fn example(engine: WebhookEngine) {
    /// // Graceful shutdown on Ctrl-C (or SIGTERM via tokio::signal::unix):
    /// engine.run_graceful(async {
    ///     tokio::signal::ctrl_c().await.ok();
    /// }).await;
    /// # }
    /// ```
    pub async fn run_graceful<F: std::future::Future<Output = ()>>(&self, shutdown: F) {
        self.worker.run_graceful(shutdown).await
    }

    /// Run one delivery cycle. Returns the number of events processed.
    pub async fn run_once(&self) -> Result<usize> {
        self.worker.run_once().await
    }
}
