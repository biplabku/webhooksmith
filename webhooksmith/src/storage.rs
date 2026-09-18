use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::{HooksmithError, Result},
    model::{DeliveryAttempt, Endpoint, NewEndpoint, QueueStats, UpdateEndpoint, WebhookEvent},
    retry,
};

// Single source of truth — defined in worker.rs where it's primarily used.
use crate::worker::MAX_RESPONSE_BODY_BYTES;
const MAX_PAYLOAD_BYTES: usize = 1_048_576; // 1 MB

pub async fn create_endpoint(pool: &PgPool, new: NewEndpoint) -> Result<Endpoint> {
    new.validate()?;
    let endpoint = sqlx::query_as!(
        Endpoint,
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, description, max_attempts, initial_delay_ms)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
        new.url,
        new.signing_secret,
        new.description,
        new.max_attempts.unwrap_or(10),
        new.initial_delay_ms.unwrap_or(1000),
    )
    .fetch_one(pool)
    .await?;
    Ok(endpoint)
}

/// Creates an endpoint skipping URL/SSRF validation but still validating fields.
/// **For local dev only** — use `create_endpoint` in production.
pub async fn create_endpoint_unchecked(pool: &PgPool, new: NewEndpoint) -> Result<Endpoint> {
    new.validate_fields()?;
    let endpoint = sqlx::query_as!(
        Endpoint,
        r#"
        INSERT INTO webhook_endpoints (url, signing_secret, description, max_attempts, initial_delay_ms)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
        "#,
        new.url,
        new.signing_secret,
        new.description,
        new.max_attempts.unwrap_or(10),
        new.initial_delay_ms.unwrap_or(1000),
    )
    .fetch_one(pool)
    .await?;
    Ok(endpoint)
}

/// Update an endpoint's fields. Only provided fields are changed.
/// The `updated_at` column is set automatically by a database trigger.
pub(crate) async fn update_endpoint(
    pool: &PgPool,
    id: Uuid,
    update: UpdateEndpoint,
    allow_insecure_urls: bool,
) -> Result<Endpoint> {
    update.validate(allow_insecure_urls)?;

    let endpoint = sqlx::query_as!(
        Endpoint,
        r#"
        UPDATE webhook_endpoints
        SET
            url             = COALESCE($2, url),
            signing_secret  = COALESCE($3, signing_secret),
            description     = CASE WHEN $4 THEN $5 ELSE description END,
            enabled         = COALESCE($6, enabled),
            max_attempts    = COALESCE($7, max_attempts),
            initial_delay_ms = COALESCE($8, initial_delay_ms)
        WHERE id = $1
        RETURNING *
        "#,
        id,
        update.url,
        update.signing_secret,
        update.description.is_some(),         // $4: whether to overwrite description
        update.description.flatten(),         // $5: the new description value (or NULL)
        update.enabled,
        update.max_attempts,
        update.initial_delay_ms,
    )
    .fetch_optional(pool)
    .await?
    .ok_or(HooksmithError::EndpointNotFound(id))?;

    Ok(endpoint)
}

pub async fn get_event(pool: &PgPool, id: Uuid) -> Result<Option<WebhookEvent>> {
    let event = sqlx::query_as!(
        WebhookEvent,
        r#"
        SELECT id, endpoint_id, event_type, payload,
               status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        FROM webhook_events WHERE id = $1
        "#,
        id,
    )
    .fetch_optional(pool)
    .await?;
    Ok(event)
}

pub async fn get_endpoint(pool: &PgPool, id: Uuid) -> Result<Option<Endpoint>> {
    let endpoint = sqlx::query_as!(
        Endpoint,
        "SELECT * FROM webhook_endpoints WHERE id = $1",
        id
    )
    .fetch_optional(pool)
    .await?;
    Ok(endpoint)
}

/// Enqueues a webhook event for delivery.
/// For crash-safe delivery use `enqueue_in_tx` instead.
pub async fn enqueue(
    pool: &PgPool,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<WebhookEvent> {
    validate_enqueue(event_type, &payload)?;
    let event = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload)
        VALUES ($1, $2, $3)
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        endpoint_id,
        event_type,
        payload,
    )
    .fetch_one(pool)
    .await?;
    Ok(event)
}

/// Enqueues a webhook event inside an existing transaction (transactional outbox pattern).
/// The webhook is only created if the surrounding transaction commits — no silent drops.
pub async fn enqueue_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<WebhookEvent> {
    validate_enqueue(event_type, &payload)?;
    let event = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload)
        VALUES ($1, $2, $3)
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        endpoint_id,
        event_type,
        payload,
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(event)
}

/// Enqueue with an idempotency key — returns the existing event if the key was already used.
/// Uses ON CONFLICT DO UPDATE (no-op) to always return the row via RETURNING.
pub(crate) async fn enqueue_idempotent(
    pool: &PgPool,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
    idempotency_key: &str,
) -> Result<WebhookEvent> {
    validate_enqueue(event_type, &payload)?;
    let event = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload, idempotency_key)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (endpoint_id, idempotency_key) WHERE idempotency_key IS NOT NULL
        DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        endpoint_id,
        event_type,
        payload,
        idempotency_key,
    )
    .fetch_one(pool)
    .await?;
    Ok(event)
}

/// Enqueue with idempotency key inside an existing transaction.
pub(crate) async fn enqueue_idempotent_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
    idempotency_key: &str,
) -> Result<WebhookEvent> {
    validate_enqueue(event_type, &payload)?;
    let event = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload, idempotency_key)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (endpoint_id, idempotency_key) WHERE idempotency_key IS NOT NULL
        DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        endpoint_id,
        event_type,
        payload,
        idempotency_key,
    )
    .fetch_one(&mut **tx)
    .await?;
    Ok(event)
}

/// Fan-out with idempotency key: one event per enabled endpoint, deduplicated per (endpoint, key).
pub(crate) async fn broadcast_idempotent(
    pool: &PgPool,
    event_type: &str,
    payload: serde_json::Value,
    idempotency_key: &str,
) -> Result<Vec<WebhookEvent>> {
    validate_enqueue(event_type, &payload)?;
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload, idempotency_key)
        SELECT id, $1, $2, $3
        FROM webhook_endpoints
        WHERE enabled = true
        ON CONFLICT (endpoint_id, idempotency_key) WHERE idempotency_key IS NOT NULL
        DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        event_type,
        payload,
        idempotency_key,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

/// Fan-out: inserts one event per enabled endpoint in a single atomic statement.
/// Returns one WebhookEvent per endpoint. Empty if no endpoints are registered.
pub(crate) async fn broadcast(
    pool: &PgPool,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<Vec<WebhookEvent>> {
    validate_enqueue(event_type, &payload)?;
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload)
        SELECT id, $1, $2
        FROM webhook_endpoints
        WHERE enabled = true
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        event_type,
        payload,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

/// Fan-out inside an existing transaction (transactional outbox pattern).
/// Events only exist if the caller's transaction commits.
pub(crate) async fn broadcast_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<Vec<WebhookEvent>> {
    validate_enqueue(event_type, &payload)?;
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        INSERT INTO webhook_events (endpoint_id, event_type, payload)
        SELECT id, $1, $2
        FROM webhook_endpoints
        WHERE enabled = true
        RETURNING id, endpoint_id, event_type, payload,
                  status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        "#,
        event_type,
        payload,
    )
    .fetch_all(&mut **tx)
    .await?;
    Ok(events)
}

/// Claims a batch of due events using SELECT FOR UPDATE SKIP LOCKED.
/// Only claims events for enabled endpoints.
/// Sets delivering_since so a reaper can recover stuck events after worker crashes.
pub async fn claim_due_events(pool: &PgPool, limit: i64) -> Result<Vec<WebhookEvent>> {
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        WITH claimed AS (
            SELECT we.id
            FROM webhook_events we
            JOIN webhook_endpoints ep ON ep.id = we.endpoint_id
            WHERE we.status IN ('pending', 'failed')
              AND we.scheduled_at <= NOW()
              AND ep.enabled = true
            ORDER BY we.scheduled_at
            LIMIT $1
            FOR UPDATE OF we SKIP LOCKED
        )
        UPDATE webhook_events
        SET status = 'delivering', delivering_since = NOW()
        FROM claimed
        WHERE webhook_events.id = claimed.id
        RETURNING webhook_events.id,
                  webhook_events.endpoint_id,
                  webhook_events.event_type,
                  webhook_events.payload,
                  webhook_events.status as "status: _",
                  webhook_events.attempts,
                  webhook_events.scheduled_at,
                  webhook_events.delivering_since,
                  webhook_events.idempotency_key,
                  webhook_events.created_at
        "#,
        limit,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

/// Resets events that have been stuck in 'delivering' for longer than the timeout.
/// Call this periodically (e.g., every 60s) to recover from worker crashes.
pub async fn recover_stuck_deliveries(pool: &PgPool, stuck_after_secs: i64) -> Result<u64> {
    let result = sqlx::query!(
        r#"
        UPDATE webhook_events
        SET status = 'pending', delivering_since = NULL
        WHERE status = 'delivering'
          AND delivering_since < NOW() - ($1 || ' seconds')::INTERVAL
        "#,
        stuck_after_secs.to_string(),
    )
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

pub async fn record_success(
    pool: &PgPool,
    event_id: Uuid,
    response_status: i32,
    response_body: Option<String>,
    duration_ms: i32,
) -> Result<()> {
    // Truncate response body before storing — callers can send huge bodies.
    let body = response_body.map(|b| truncate_utf8(b, MAX_RESPONSE_BODY_BYTES));

    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO webhook_delivery_attempts
            (event_id, response_status, response_body, duration_ms, success)
        VALUES ($1, $2, $3, $4, true)
        "#,
        event_id,
        response_status,
        body,
        duration_ms,
    )
    .execute(&mut *tx)
    .await?;

    // Guard: only update if still in 'delivering' state.
    // If the reaper already reset this event (worker was too slow), this is a no-op.
    // The delivery attempt record above is still written as evidence.
    sqlx::query!(
        r#"
        UPDATE webhook_events
        SET status = 'delivered', attempts = attempts + 1, delivering_since = NULL
        WHERE id = $1 AND status = 'delivering'
        "#,
        event_id,
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

pub async fn record_failure(
    pool: &PgPool,
    event: &WebhookEvent,
    endpoint: &Endpoint,
    error: String,
    response_status: Option<i32>,
    duration_ms: Option<i32>,
) -> Result<()> {
    let mut tx = pool.begin().await?;

    sqlx::query!(
        r#"
        INSERT INTO webhook_delivery_attempts
            (event_id, response_status, duration_ms, error, success)
        VALUES ($1, $2, $3, $4, false)
        "#,
        event.id,
        response_status,
        duration_ms,
        error,
    )
    .execute(&mut *tx)
    .await?;

    // Check current attempts from DB (not from the cached event struct) to avoid
    // double-counting if two workers raced on the same event after a crash.
    let current = sqlx::query_scalar!(
        "SELECT attempts FROM webhook_events WHERE id = $1",
        event.id,
    )
    .fetch_one(&mut *tx)
    .await?;

    let next_attempts = current + 1;

    // Guard: only update if still in 'delivering'. Zombie workers are no-ops.
    if next_attempts >= endpoint.max_attempts {
        sqlx::query!(
            r#"
            UPDATE webhook_events
            SET status = 'dead', attempts = attempts + 1, delivering_since = NULL
            WHERE id = $1 AND status = 'delivering'
            "#,
            event.id,
        )
        .execute(&mut *tx)
        .await?;
    } else {
        let delay = retry::next_delay(
            next_attempts as u32,
            endpoint.initial_delay_ms as u32,
            3_600_000,
        );
        let next_attempt: DateTime<Utc> = Utc::now()
            + chrono::Duration::from_std(delay)
                .map_err(|e| HooksmithError::Config(format!("delay overflow: {e}")))?;

        sqlx::query!(
            r#"
            UPDATE webhook_events
            SET status = 'failed', attempts = attempts + 1,
                scheduled_at = $1, delivering_since = NULL
            WHERE id = $2 AND status = 'delivering'
            "#,
            next_attempt,
            event.id,
        )
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn list_dead_events(pool: &PgPool, endpoint_id: Uuid) -> Result<Vec<WebhookEvent>> {
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        SELECT id, endpoint_id, event_type, payload,
               status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        FROM webhook_events
        WHERE endpoint_id = $1 AND status = 'dead'
        ORDER BY created_at DESC
        "#,
        endpoint_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

/// Resets a 'delivering' event back to 'pending' without counting it as a failed attempt.
/// Used when an endpoint is disabled between claim and delivery.
pub async fn reset_to_pending(pool: &PgPool, event_id: Uuid) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE webhook_events
        SET status = 'pending', delivering_since = NULL, scheduled_at = NOW()
        WHERE id = $1 AND status = 'delivering'
        "#,
        event_id,
    )
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn retry_dead_event(pool: &PgPool, event_id: Uuid) -> Result<()> {
    // First check if the event exists at all, to give a precise error.
    let status = sqlx::query_scalar!(
        "SELECT status FROM webhook_events WHERE id = $1",
        event_id,
    )
    .fetch_optional(pool)
    .await?;

    match status.as_deref() {
        None => return Err(HooksmithError::EventNotFound(event_id)),
        Some("dead") => {}
        Some(_) => return Err(HooksmithError::InvalidState(event_id)),
    }

    sqlx::query!(
        r#"
        UPDATE webhook_events
        SET status = 'pending', attempts = 0, scheduled_at = NOW(), delivering_since = NULL
        WHERE id = $1 AND status = 'dead'
        "#,
        event_id,
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn delivery_log(pool: &PgPool, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
    let attempts = sqlx::query_as!(
        DeliveryAttempt,
        "SELECT * FROM webhook_delivery_attempts WHERE event_id = $1 ORDER BY attempted_at",
        event_id,
    )
    .fetch_all(pool)
    .await?;
    Ok(attempts)
}

/// Records cleanup when an endpoint was deleted after its event was claimed.
///
/// With ON DELETE CASCADE both the event and its endpoint are deleted together.
/// The delivery_attempts INSERT and webhook_events UPDATE may silently fail
/// (FK violation) if the event was cascade-deleted — that is correct behaviour.
/// Never returns an error; the caller should log and continue.
pub(crate) async fn record_endpoint_deleted(pool: &PgPool, event_id: Uuid) {
    let Ok(mut tx) = pool.begin().await else { return };

    // If the event was cascade-deleted, this INSERT fails with FK violation —
    // the transaction rolls back and neither write persists. That is fine.
    let _ = sqlx::query!(
        "INSERT INTO webhook_delivery_attempts (event_id, error, success)
         VALUES ($1, 'endpoint was deleted after event was claimed for delivery', false)",
        event_id,
    )
    .execute(&mut *tx)
    .await;

    let _ = sqlx::query!(
        "UPDATE webhook_events
         SET status = 'dead', delivering_since = NULL
         WHERE id = $1 AND status = 'delivering'",
        event_id,
    )
    .execute(&mut *tx)
    .await;

    let _ = tx.commit().await;
}

const MAX_EVENT_TYPE_BYTES: usize = 256;

/// Public wrapper so the SQLite engine can reuse the same validation.
#[cfg(feature = "sqlite")]
pub fn validate_enqueue_public(event_type: &str, payload: &serde_json::Value) -> Result<()> {
    validate_enqueue(event_type, payload)
}

fn validate_enqueue(event_type: &str, payload: &serde_json::Value) -> Result<()> {
    if event_type.trim().is_empty() {
        return Err(HooksmithError::Config("event_type must not be empty".into()));
    }
    if event_type.len() > MAX_EVENT_TYPE_BYTES {
        return Err(HooksmithError::Config(format!(
            "event_type too long: {} bytes exceeds {MAX_EVENT_TYPE_BYTES} byte limit",
            event_type.len()
        )));
    }
    // event_type is sent as an HTTP header value. Control characters (< 0x20 or DEL)
    // cause reqwest/hyper to reject the request at send time with a confusing error,
    // making the event permanently stuck in the failed retry loop.
    if event_type.chars().any(|c| (c as u32) < 0x20 || c == '\x7f') {
        return Err(HooksmithError::Config(
            "event_type must not contain control characters".into(),
        ));
    }
    // Measure serialized size without allocating the full string.
    // serde_json can write to a byte counter instead of a Vec.
    struct CountingWriter(usize);
    impl std::io::Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut counter = CountingWriter(0);
    serde_json::to_writer(&mut counter, payload)
        .map_err(|e| HooksmithError::Config(format!("payload serialization error: {e}")))?;
    let payload_size = counter.0;
    if payload_size > MAX_PAYLOAD_BYTES {
        return Err(HooksmithError::PayloadTooLarge(payload_size, MAX_PAYLOAD_BYTES));
    }
    Ok(())
}

// ── Endpoint listing and deletion ─────────────────────────────────────────────

pub(crate) async fn list_endpoints(pool: &PgPool) -> Result<Vec<Endpoint>> {
    let endpoints = sqlx::query_as!(
        Endpoint,
        "SELECT * FROM webhook_endpoints ORDER BY created_at"
    )
    .fetch_all(pool)
    .await?;
    Ok(endpoints)
}

pub(crate) async fn list_endpoints_paged(
    pool: &PgPool,
    limit: i64,
    offset: i64,
) -> Result<Vec<Endpoint>> {
    let limit = limit.max(0);
    let offset = offset.max(0);
    let endpoints = sqlx::query_as!(
        Endpoint,
        "SELECT * FROM webhook_endpoints ORDER BY created_at LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    .fetch_all(pool)
    .await?;
    Ok(endpoints)
}

/// Events across ALL endpoints for a given status, paginated.
/// Use this for global monitoring (e.g. "all failed events") without
/// needing to know the endpoint ID.
pub(crate) async fn events_global_by_status(
    pool: &PgPool,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookEvent>> {
    let limit = limit.max(0);
    let offset = offset.max(0);
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        SELECT id, endpoint_id, event_type, payload,
               status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        FROM webhook_events
        WHERE status = $1
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#,
        status,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

pub(crate) async fn delete_endpoint(pool: &PgPool, id: Uuid) -> Result<()> {
    let rows = sqlx::query!("DELETE FROM webhook_endpoints WHERE id = $1", id)
        .execute(pool)
        .await?
        .rows_affected();
    if rows == 0 {
        return Err(HooksmithError::EndpointNotFound(id));
    }
    Ok(())
}

// ── Queue stats ───────────────────────────────────────────────────────────────

/// Counts of webhook events grouped by status. One query, no N+1.
pub(crate) async fn queue_stats(pool: &PgPool) -> Result<QueueStats> {
    let row = sqlx::query!(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE status = 'pending')    AS "pending!",
            COUNT(*) FILTER (WHERE status = 'delivering') AS "delivering!",
            COUNT(*) FILTER (WHERE status = 'failed')     AS "failed!",
            COUNT(*) FILTER (WHERE status = 'dead')       AS "dead!",
            COUNT(*) FILTER (WHERE status = 'delivered')  AS "delivered!"
        FROM webhook_events
        "#,
    )
    .fetch_one(pool)
    .await?;

    Ok(QueueStats {
        pending: row.pending,
        delivering: row.delivering,
        failed: row.failed,
        dead: row.dead,
        delivered: row.delivered,
    })
}

// ── Event listing (paginated) ─────────────────────────────────────────────────

/// Events for an endpoint with a specific status, paginated by scheduled_at DESC.
/// Negative limit is clamped to 0; negative offset is clamped to 0.
pub(crate) async fn events_by_status(
    pool: &PgPool,
    endpoint_id: Uuid,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookEvent>> {
    let limit = limit.max(0);
    let offset = offset.max(0);
    let events = sqlx::query_as!(
        WebhookEvent,
        r#"
        SELECT id, endpoint_id, event_type, payload,
               status as "status: _", attempts, scheduled_at, delivering_since, idempotency_key, created_at
        FROM webhook_events
        WHERE endpoint_id = $1 AND status = $2
        ORDER BY created_at DESC
        LIMIT $3 OFFSET $4
        "#,
        endpoint_id,
        status,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await?;
    Ok(events)
}

/// Dead events for an endpoint, paginated.
pub(crate) async fn dead_events_paged(
    pool: &PgPool,
    endpoint_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookEvent>> {
    events_by_status(pool, endpoint_id, "dead", limit, offset).await
}

// ── Bulk DLQ retry ────────────────────────────────────────────────────────────

/// Requeues every dead event for an endpoint. Returns the number of events requeued.
pub(crate) async fn retry_all_dead(pool: &PgPool, endpoint_id: Uuid) -> Result<u64> {
    let rows = sqlx::query!(
        r#"
        UPDATE webhook_events
        SET status = 'pending', attempts = 0, scheduled_at = NOW(), delivering_since = NULL
        WHERE endpoint_id = $1 AND status = 'dead'
        "#,
        endpoint_id,
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows)
}

// ── Event cleanup / TTL ───────────────────────────────────────────────────────

/// Deletes delivered events older than `older_than_secs`. Returns the count deleted.
/// Does not touch pending, failed, or dead events — those require explicit action.
pub(crate) async fn cleanup_delivered(pool: &PgPool, older_than_secs: i64) -> Result<u64> {
    let rows = sqlx::query!(
        r#"
        DELETE FROM webhook_events
        WHERE status = 'delivered'
          AND created_at < NOW() - ($1 || ' seconds')::INTERVAL
        "#,
        older_than_secs.to_string(),
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows)
}

/// Deletes dead (DLQ) events older than `older_than_secs`. Returns the count deleted.
/// Dead events accumulate if retry_all_dead is not called periodically.
pub(crate) async fn cleanup_dead(pool: &PgPool, older_than_secs: i64) -> Result<u64> {
    let rows = sqlx::query!(
        r#"
        DELETE FROM webhook_events
        WHERE status = 'dead'
          AND created_at < NOW() - ($1 || ' seconds')::INTERVAL
        "#,
        older_than_secs.to_string(),
    )
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows)
}

/// Truncates a string to at most `max_bytes` bytes at a valid UTF-8 boundary.
fn truncate_utf8(s: String, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s;
    }
    let boundary = s
        .char_indices()
        .map(|(i, _)| i)
        .take_while(|&i| i < max_bytes)
        .last()
        .unwrap_or(0);
    s[..boundary].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_valid_utf8() {
        let s = "héllo wörld".to_string();
        let t = truncate_utf8(s, 5);
        assert!(std::str::from_utf8(t.as_bytes()).is_ok());
    }
}
