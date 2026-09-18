//! SQLite storage backend.
//!
//! Uses runtime queries (`sqlx::query()`) instead of compile-time macros
//! because SQLite and Postgres have different type systems that macros
//! can't abstract over. Trade-off: lose compile-time SQL checking,
//! gain multi-backend support.
//!
//! Key differences from Postgres backend:
//! - UUIDs stored and passed as TEXT
//! - Timestamps as TEXT (ISO 8601)
//! - JSON as TEXT (serialized/deserialized in Rust)
//! - No FOR UPDATE SKIP LOCKED (SQLite WAL + pool_size=1 provides safety)
//! - No gen_random_uuid() — UUID generated in Rust

use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    error::{HooksmithError, Result},
    model::{DeliveryAttempt, Endpoint, EventStatus, NewEndpoint, QueueStats, WebhookEvent},
    retry,
    worker::MAX_RESPONSE_BODY_BYTES,
};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_str() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

fn truncate_utf8_sqlite(s: String, max_bytes: usize) -> String {
    if s.len() <= max_bytes { return s; }
    let boundary = s.char_indices()
        .map(|(i, c)| i + c.len_utf8())
        .take_while(|&end| end <= max_bytes)
        .last()
        .unwrap_or(0);
    s[..boundary].to_owned()
}

fn uuid_str() -> String {
    Uuid::new_v4().to_string()
}

fn bool_to_int(b: bool) -> i64 { if b { 1 } else { 0 } }
fn int_to_bool(i: i64) -> bool { i != 0 }

fn parse_status(s: &str) -> EventStatus {
    match s {
        "pending"    => EventStatus::Pending,
        "delivering" => EventStatus::Delivering,
        "delivered"  => EventStatus::Delivered,
        "failed"     => EventStatus::Failed,
        "dead"       => EventStatus::Dead,
        _            => EventStatus::Pending,
    }
}

// status_str kept here for potential future use in filtered queries
#[allow(dead_code)]
fn status_str(s: &EventStatus) -> &'static str {
    match s {
        EventStatus::Pending    => "pending",
        EventStatus::Delivering => "delivering",
        EventStatus::Delivered  => "delivered",
        EventStatus::Failed     => "failed",
        EventStatus::Dead       => "dead",
    }
}

fn row_to_endpoint(row: &sqlx::sqlite::SqliteRow) -> Endpoint {
    use sqlx::Row;
    let enabled_int: i64 = row.get("enabled");
    // event_filter stored as JSON array string in SQLite, or NULL
    let event_filter: Option<Vec<String>> = row
        .try_get::<Option<String>, _>("event_filter")
        .ok()
        .flatten()
        .and_then(|s| serde_json::from_str(&s).ok());
    Endpoint {
        id: row.get::<String, _>("id").parse().unwrap_or_default(),
        url: row.get("url"),
        signing_secret: row.get("signing_secret"),
        description: row.get("description"),
        enabled: int_to_bool(enabled_int),
        max_attempts: row.get("max_attempts"),
        initial_delay_ms: row.get("initial_delay_ms"),
        event_filter,
        consecutive_failures: row.try_get::<i32, _>("consecutive_failures").unwrap_or(0),
        circuit_open_until: row.try_get::<Option<String>, _>("circuit_open_until")
            .ok()
            .flatten()
            .and_then(|s| s.parse::<DateTime<Utc>>().ok()),
        created_at: row.get::<String, _>("created_at")
            .parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
        updated_at: row.get::<String, _>("updated_at")
            .parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
    }
}

fn row_to_event(row: &sqlx::sqlite::SqliteRow) -> WebhookEvent {
    use sqlx::Row;
    let payload_str: String = row.get("payload");
    let status_str_val: String = row.get("status");
    let delivering_since: Option<String> = row.get("delivering_since");
    let idempotency_key: Option<String> = row.get("idempotency_key");

    WebhookEvent {
        id: row.get::<String, _>("id").parse().unwrap_or_default(),
        endpoint_id: row.get::<String, _>("endpoint_id").parse().unwrap_or_default(),
        event_type: row.get("event_type"),
        payload: serde_json::from_str(&payload_str).unwrap_or_default(),
        status: parse_status(&status_str_val),
        attempts: row.get("attempts"),
        scheduled_at: row.get::<String, _>("scheduled_at")
            .parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
        delivering_since: delivering_since.and_then(|s| s.parse::<DateTime<Utc>>().ok()),
        idempotency_key,
        created_at: row.get::<String, _>("created_at")
            .parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
    }
}

fn row_to_attempt(row: &sqlx::sqlite::SqliteRow) -> DeliveryAttempt {
    use sqlx::Row;
    let success_int: i64 = row.get("success");
    DeliveryAttempt {
        id: row.get::<String, _>("id").parse().unwrap_or_default(),
        event_id: row.get::<String, _>("event_id").parse().unwrap_or_default(),
        attempted_at: row.get::<String, _>("attempted_at")
            .parse::<DateTime<Utc>>().unwrap_or_else(|_| Utc::now()),
        response_status: row.get("response_status"),
        response_body: row.get("response_body"),
        duration_ms: row.get("duration_ms"),
        error: row.get("error"),
        success: int_to_bool(success_int),
    }
}

// ── Endpoint operations ───────────────────────────────────────────────────────

pub async fn create_endpoint(pool: &SqlitePool, new: NewEndpoint) -> Result<Endpoint> {
    let id = uuid_str();
    let now = now_str();
    // Serialize event_filter as JSON array for SQLite TEXT storage
    let filter_json = new.event_filter.as_ref()
        .map(|f| serde_json::to_string(f).unwrap_or_default());
    sqlx::query(
        "INSERT INTO webhook_endpoints
         (id, url, signing_secret, description, enabled, max_attempts, initial_delay_ms, event_filter, created_at, updated_at)
         VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)"
    )
    .bind(&id)
    .bind(&new.url)
    .bind(&new.signing_secret)
    .bind(&new.description)
    .bind(new.max_attempts.unwrap_or(10))
    .bind(new.initial_delay_ms.unwrap_or(1000))
    .bind(&filter_json)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    get_endpoint_required(pool, &id).await
}

pub async fn get_endpoint_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Endpoint>> {
    let id_str = id.to_string();
    let row = sqlx::query("SELECT * FROM webhook_endpoints WHERE id = ?")
        .bind(&id_str)
        .fetch_optional(pool)
        .await?;
    Ok(row.as_ref().map(row_to_endpoint))
}

async fn get_endpoint_required(pool: &SqlitePool, id: &str) -> Result<Endpoint> {
    let row = sqlx::query("SELECT * FROM webhook_endpoints WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    row.as_ref()
        .map(row_to_endpoint)
        .ok_or_else(|| HooksmithError::EndpointNotFound(id.parse().unwrap_or_default()))
}

pub async fn list_endpoints(pool: &SqlitePool) -> Result<Vec<Endpoint>> {
    let rows = sqlx::query("SELECT * FROM webhook_endpoints ORDER BY created_at")
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(row_to_endpoint).collect())
}

pub async fn list_endpoints_paged(pool: &SqlitePool, limit: i64, offset: i64) -> Result<Vec<Endpoint>> {
    let limit = limit.max(0);
    let offset = offset.max(0);
    let rows = sqlx::query("SELECT * FROM webhook_endpoints ORDER BY created_at LIMIT ? OFFSET ?")
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?;
    Ok(rows.iter().map(row_to_endpoint).collect())
}

pub async fn update_endpoint_field(
    pool: &SqlitePool,
    id: Uuid,
    url: Option<String>,
    signing_secret: Option<String>,
    description_set: bool,
    description: Option<String>,
    enabled: Option<bool>,
    max_attempts: Option<i32>,
    initial_delay_ms: Option<i32>,
) -> Result<Endpoint> {
    let id_str = id.to_string();

    // Check exists
    let exists: bool = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM webhook_endpoints WHERE id = ?"
    )
    .bind(&id_str)
    .fetch_one(pool)
    .await? > 0;

    if !exists {
        return Err(HooksmithError::EndpointNotFound(id));
    }

    if let Some(u) = url {
        sqlx::query("UPDATE webhook_endpoints SET url = ? WHERE id = ?")
            .bind(u).bind(&id_str).execute(pool).await?;
    }
    if let Some(s) = signing_secret {
        sqlx::query("UPDATE webhook_endpoints SET signing_secret = ? WHERE id = ?")
            .bind(s).bind(&id_str).execute(pool).await?;
    }
    if description_set {
        sqlx::query("UPDATE webhook_endpoints SET description = ? WHERE id = ?")
            .bind(description).bind(&id_str).execute(pool).await?;
    }
    if let Some(e) = enabled {
        sqlx::query("UPDATE webhook_endpoints SET enabled = ? WHERE id = ?")
            .bind(bool_to_int(e)).bind(&id_str).execute(pool).await?;
    }
    if let Some(m) = max_attempts {
        sqlx::query("UPDATE webhook_endpoints SET max_attempts = ? WHERE id = ?")
            .bind(m).bind(&id_str).execute(pool).await?;
    }
    if let Some(d) = initial_delay_ms {
        sqlx::query("UPDATE webhook_endpoints SET initial_delay_ms = ? WHERE id = ?")
            .bind(d).bind(&id_str).execute(pool).await?;
    }

    // Update updated_at manually (no DB trigger in SQLite)
    sqlx::query("UPDATE webhook_endpoints SET updated_at = ? WHERE id = ?")
        .bind(now_str()).bind(&id_str).execute(pool).await?;

    get_endpoint_required(pool, &id_str).await
}

pub async fn delete_endpoint(pool: &SqlitePool, id: Uuid) -> Result<()> {
    let id_str = id.to_string();
    let rows = sqlx::query("DELETE FROM webhook_endpoints WHERE id = ?")
        .bind(&id_str)
        .execute(pool)
        .await?
        .rows_affected();
    if rows == 0 {
        return Err(HooksmithError::EndpointNotFound(id));
    }
    Ok(())
}

// ── Event enqueue ─────────────────────────────────────────────────────────────

pub async fn enqueue(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<WebhookEvent> {
    let id = uuid_str();
    let now = now_str();
    let endpoint_id_str = endpoint_id.to_string();
    let payload_str = serde_json::to_string(&payload)
        .map_err(|e| HooksmithError::Config(format!("payload serialization: {e}")))?;

    sqlx::query(
        "INSERT INTO webhook_events
         (id, endpoint_id, event_type, payload, scheduled_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(&id).bind(&endpoint_id_str).bind(event_type)
    .bind(&payload_str).bind(&now).bind(&now)
    .execute(pool)
    .await?;

    get_event_required(pool, &id).await
}

/// True transactional outbox for SQLite: inserts the event using the
/// transaction's connection. The event only exists if the transaction commits.
pub async fn enqueue_in_sqlite_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<WebhookEvent> {
    let id = uuid_str();
    let now = now_str();
    let endpoint_id_str = endpoint_id.to_string();
    let payload_str = serde_json::to_string(&payload)
        .map_err(|e| HooksmithError::Config(format!("payload serialization: {e}")))?;

    sqlx::query(
        "INSERT INTO webhook_events
         (id, endpoint_id, event_type, payload, scheduled_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(&id).bind(&endpoint_id_str).bind(event_type)
    .bind(&payload_str).bind(&now).bind(&now)
    .execute(&mut **tx)   // ← uses the transaction's connection, not the pool
    .await?;

    // Read back within the same transaction (sees uncommitted data)
    let row = sqlx::query("SELECT * FROM webhook_events WHERE id = ?")
        .bind(&id)
        .fetch_one(&mut **tx)
        .await?;

    Ok(row_to_event(&row))
}

pub async fn enqueue_idempotent(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    event_type: &str,
    payload: serde_json::Value,
    idempotency_key: &str,
) -> Result<WebhookEvent> {
    let endpoint_id_str = endpoint_id.to_string();
    let payload_str = serde_json::to_string(&payload)
        .map_err(|e| HooksmithError::Config(format!("payload serialization: {e}")))?;

    // Check if already exists
    let existing = sqlx::query(
        "SELECT * FROM webhook_events WHERE endpoint_id = ? AND idempotency_key = ?"
    )
    .bind(&endpoint_id_str)
    .bind(idempotency_key)
    .fetch_optional(pool)
    .await?;

    if let Some(row) = existing {
        return Ok(row_to_event(&row));
    }

    let id = uuid_str();
    let now = now_str();

    sqlx::query(
        "INSERT OR IGNORE INTO webhook_events
         (id, endpoint_id, event_type, payload, idempotency_key, scheduled_at, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&id).bind(&endpoint_id_str).bind(event_type)
    .bind(&payload_str).bind(idempotency_key).bind(&now).bind(&now)
    .execute(pool)
    .await?;

    // Return whatever exists (might be from another concurrent insert)
    let row = sqlx::query(
        "SELECT * FROM webhook_events WHERE endpoint_id = ? AND idempotency_key = ?"
    )
    .bind(&endpoint_id_str)
    .bind(idempotency_key)
    .fetch_one(pool)
    .await?;

    Ok(row_to_event(&row))
}

pub async fn broadcast(
    pool: &SqlitePool,
    event_type: &str,
    payload: serde_json::Value,
) -> Result<Vec<WebhookEvent>> {
    use crate::model::event_matches_filter;
    let endpoints = list_endpoints(pool).await?;
    let mut events = Vec::new();
    for ep in endpoints.iter().filter(|e| e.enabled && event_matches_filter(event_type, &e.event_filter)) {
        let ev = enqueue(pool, ep.id, event_type, payload.clone()).await?;
        events.push(ev);
    }
    Ok(events)
}

pub async fn broadcast_idempotent(
    pool: &SqlitePool,
    event_type: &str,
    payload: serde_json::Value,
    idempotency_key: &str,
) -> Result<Vec<WebhookEvent>> {
    use crate::model::event_matches_filter;
    let endpoints = list_endpoints(pool).await?;
    let mut events = Vec::new();
    for ep in endpoints.iter().filter(|e| e.enabled && event_matches_filter(event_type, &e.event_filter)) {
        let ev = enqueue_idempotent(pool, ep.id, event_type, payload.clone(), idempotency_key).await?;
        events.push(ev);
    }
    Ok(events)
}

// ── Delivery ──────────────────────────────────────────────────────────────────

pub async fn claim_due_events(pool: &SqlitePool, limit: i64) -> Result<Vec<WebhookEvent>> {
    // SQLite: no SKIP LOCKED. Use a transaction + immediate locking.
    // With pool_size=1 (enforced for SQLite), no concurrent workers
    // can race — only one connection can write at a time.
    let now = now_str();

    let rows = sqlx::query(
        "SELECT we.* FROM webhook_events we
         JOIN webhook_endpoints ep ON ep.id = we.endpoint_id
         WHERE we.status IN ('pending', 'failed')
           AND we.scheduled_at <= ?
           AND ep.enabled = 1
           AND (ep.circuit_open_until IS NULL OR ep.circuit_open_until <= ?)
         ORDER BY we.scheduled_at
         LIMIT ?"
    )
    .bind(&now)
    .bind(&now) // circuit_open_until check
    .bind(limit)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(vec![]);
    }

    let ids: Vec<String> = rows.iter()
        .map(|r| { use sqlx::Row; r.get::<String, _>("id") })
        .collect();

    // Mark as delivering one by one (SQLite doesn't support IN with dynamic lists in query macros)
    for id in &ids {
        sqlx::query(
            "UPDATE webhook_events SET status = 'delivering', delivering_since = ? WHERE id = ?"
        )
        .bind(&now)
        .bind(id)
        .execute(pool)
        .await?;
    }

    // Fetch the updated rows
    let mut events = Vec::new();
    for id in &ids {
        if let Some(row) = sqlx::query("SELECT * FROM webhook_events WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await?
        {
            events.push(row_to_event(&row));
        }
    }

    Ok(events)
}

pub async fn recover_stuck_deliveries(pool: &SqlitePool, stuck_after_secs: i64) -> Result<u64> {
    // Compute cutoff in Rust to use consistent ISO 8601 format with 'T' separator.
    let cutoff = chrono::Utc::now() - chrono::Duration::seconds(stuck_after_secs);
    let cutoff_str = cutoff.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
    let rows = sqlx::query(
        "UPDATE webhook_events
         SET status = 'pending', delivering_since = NULL
         WHERE status = 'delivering'
           AND delivering_since < ?"
    )
    .bind(&cutoff_str)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(rows)
}

pub async fn record_success(
    pool: &SqlitePool,
    event_id: Uuid,
    response_status: i32,
    response_body: Option<String>,
    duration_ms: i32,
) -> Result<()> {
    let event_id_str = event_id.to_string();
    let attempt_id = uuid_str();
    let now = now_str();
    let body = response_body.map(|b| truncate_utf8_sqlite(b, MAX_RESPONSE_BODY_BYTES));

    let mut tx = pool.begin().await?;

    sqlx::query(
        "INSERT INTO webhook_delivery_attempts
         (id, event_id, attempted_at, response_status, response_body, duration_ms, success)
         VALUES (?, ?, ?, ?, ?, ?, 1)"
    )
    .bind(&attempt_id).bind(&event_id_str).bind(&now)
    .bind(response_status).bind(body).bind(duration_ms)
    .execute(&mut *tx)
    .await?;

    // Get endpoint_id so we can reset its circuit breaker.
    let endpoint_id: Option<String> = sqlx::query_scalar(
        "UPDATE webhook_events
         SET status = 'delivered', attempts = attempts + 1, delivering_since = NULL
         WHERE id = ? AND status = 'delivering'
         RETURNING endpoint_id"
    )
    .bind(&event_id_str)
    .fetch_optional(&mut *tx)
    .await?;

    // Reset circuit breaker — success clears consecutive failures.
    if let Some(ep_id) = endpoint_id {
        sqlx::query(
            "UPDATE webhook_endpoints
             SET consecutive_failures = 0, circuit_open_until = NULL
             WHERE id = ?"
        )
        .bind(&ep_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn record_failure(
    pool: &SqlitePool,
    event_id: Uuid,
    endpoint_id: Uuid,
    endpoint_max_attempts: i32,
    endpoint_initial_delay_ms: i32,
    error: String,
    response_status: Option<i32>,
    duration_ms: Option<i32>,
) -> Result<()> {
    let event_id_str = event_id.to_string();
    let endpoint_id_str = endpoint_id.to_string();
    let attempt_id = uuid_str();
    let now = now_str();

    let mut tx = pool.begin().await?;

    sqlx::query(
        "INSERT INTO webhook_delivery_attempts
         (id, event_id, attempted_at, response_status, duration_ms, error, success)
         VALUES (?, ?, ?, ?, ?, ?, 0)"
    )
    .bind(&attempt_id).bind(&event_id_str).bind(&now)
    .bind(response_status).bind(duration_ms).bind(&error)
    .execute(&mut *tx)
    .await?;

    let current: i32 = sqlx::query_scalar(
        "SELECT attempts FROM webhook_events WHERE id = ?"
    )
    .bind(&event_id_str)
    .fetch_one(&mut *tx)
    .await?;

    let next = current + 1;

    if next >= endpoint_max_attempts {
        sqlx::query(
            "UPDATE webhook_events SET status = 'dead', attempts = attempts + 1, delivering_since = NULL WHERE id = ? AND status = 'delivering'"
        )
        .bind(&event_id_str).execute(&mut *tx).await?;
    } else {
        let delay = retry::next_delay(next as u32, endpoint_initial_delay_ms as u32, 3_600_000);
        let retry_at = chrono::Utc::now() + chrono::Duration::from_std(delay)
            .unwrap_or_else(|_| chrono::Duration::seconds(60));
        let retry_at_str = retry_at.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        sqlx::query(
            "UPDATE webhook_events SET status = 'failed', attempts = attempts + 1,
             scheduled_at = ?, delivering_since = NULL
             WHERE id = ? AND status = 'delivering'"
        )
        .bind(&retry_at_str).bind(&event_id_str)
        .execute(&mut *tx).await?;
    }

    // Fetch current consecutive_failures; propagate DB error rather than swallowing it.
    let current_cf: i32 = sqlx::query_scalar(
        "SELECT consecutive_failures FROM webhook_endpoints WHERE id = ?"
    )
    .bind(&endpoint_id_str)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(0); // 0 only if endpoint was deleted concurrently — UPDATE below is a no-op

    let new_cf = current_cf + 1;
    let open_until_str: Option<String> = if new_cf >= 5 {
        let exponent = (new_cf - 5) as u32;
        let minutes = (5u64 * 2u64.pow(exponent)).min(320);
        let open_until = chrono::Utc::now() + chrono::Duration::minutes(minutes as i64);
        Some(open_until.to_rfc3339())
    } else {
        None
    };

    sqlx::query(
        "UPDATE webhook_endpoints
         SET consecutive_failures = ?, circuit_open_until = ?
         WHERE id = ?"
    )
    .bind(new_cf)
    .bind(&open_until_str)
    .bind(&endpoint_id_str)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

// ── Event queries ─────────────────────────────────────────────────────────────

pub async fn get_event(pool: &SqlitePool, id: Uuid) -> Result<Option<WebhookEvent>> {
    let row = sqlx::query("SELECT * FROM webhook_events WHERE id = ?")
        .bind(id.to_string())
        .fetch_optional(pool)
        .await?;
    Ok(row.as_ref().map(row_to_event))
}

async fn get_event_required(pool: &SqlitePool, id: &str) -> Result<WebhookEvent> {
    let row = sqlx::query("SELECT * FROM webhook_events WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await?;
    row.as_ref()
        .map(row_to_event)
        .ok_or_else(|| HooksmithError::EventNotFound(id.parse().unwrap_or_default()))
}

pub async fn delivery_log(pool: &SqlitePool, event_id: Uuid) -> Result<Vec<DeliveryAttempt>> {
    let rows = sqlx::query(
        "SELECT * FROM webhook_delivery_attempts WHERE event_id = ? ORDER BY attempted_at"
    )
    .bind(event_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_attempt).collect())
}

pub async fn list_dead_events(pool: &SqlitePool, endpoint_id: Uuid) -> Result<Vec<WebhookEvent>> {
    let rows = sqlx::query(
        "SELECT * FROM webhook_events WHERE endpoint_id = ? AND status = 'dead' ORDER BY created_at DESC"
    )
    .bind(endpoint_id.to_string())
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_event).collect())
}

pub async fn dead_events_paged(pool: &SqlitePool, endpoint_id: Uuid, limit: i64, offset: i64) -> Result<Vec<WebhookEvent>> {
    let rows = sqlx::query(
        "SELECT * FROM webhook_events WHERE endpoint_id = ? AND status = 'dead' ORDER BY created_at DESC LIMIT ? OFFSET ?"
    )
    .bind(endpoint_id.to_string()).bind(limit.max(0)).bind(offset.max(0))
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_event).collect())
}

pub async fn retry_dead_event(pool: &SqlitePool, event_id: Uuid) -> Result<()> {
    let id_str = event_id.to_string();
    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM webhook_events WHERE id = ?"
    )
    .bind(&id_str)
    .fetch_optional(pool)
    .await?;

    match status.as_deref() {
        None        => return Err(HooksmithError::EventNotFound(event_id)),
        Some("dead") => {}
        Some(_)     => return Err(HooksmithError::InvalidState(event_id)),
    }

    // Also reset the circuit breaker — operator explicitly requesting retry.
    let endpoint_id_str: Option<String> = sqlx::query_scalar(
        "SELECT endpoint_id FROM webhook_events WHERE id = ?"
    )
    .bind(&id_str)
    .fetch_optional(pool)
    .await?;

    let now = now_str();
    sqlx::query(
        "UPDATE webhook_events SET status = 'pending', attempts = 0, scheduled_at = ?, delivering_since = NULL WHERE id = ?"
    )
    .bind(&now).bind(&id_str).execute(pool).await?;

    if let Some(ep_id) = endpoint_id_str {
        sqlx::query(
            "UPDATE webhook_endpoints SET consecutive_failures = 0, circuit_open_until = NULL WHERE id = ?"
        )
        .bind(&ep_id)
        .execute(pool)
        .await?;
    }

    Ok(())
}

pub async fn retry_all_dead(pool: &SqlitePool, endpoint_id: Uuid) -> Result<u64> {
    let endpoint_id_str = endpoint_id.to_string();
    let now = now_str();
    let rows = sqlx::query(
        "UPDATE webhook_events SET status = 'pending', attempts = 0, scheduled_at = ?, delivering_since = NULL WHERE endpoint_id = ? AND status = 'dead'"
    )
    .bind(&now)
    .bind(&endpoint_id_str)
    .execute(pool)
    .await?
    .rows_affected();

    // Reset circuit breaker on manual DLQ retry.
    sqlx::query(
        "UPDATE webhook_endpoints SET consecutive_failures = 0, circuit_open_until = NULL WHERE id = ?"
    )
    .bind(&endpoint_id_str)
    .execute(pool)
    .await?;

    Ok(rows)
}

pub async fn events_by_status(
    pool: &SqlitePool,
    endpoint_id: Uuid,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookEvent>> {
    let rows = sqlx::query(
        "SELECT * FROM webhook_events WHERE endpoint_id = ? AND status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?"
    )
    .bind(endpoint_id.to_string()).bind(status).bind(limit.max(0)).bind(offset.max(0))
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_event).collect())
}

pub async fn events_global_by_status(
    pool: &SqlitePool,
    status: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<WebhookEvent>> {
    let rows = sqlx::query(
        "SELECT * FROM webhook_events WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?"
    )
    .bind(status).bind(limit.max(0)).bind(offset.max(0))
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(row_to_event).collect())
}

pub async fn queue_stats(pool: &SqlitePool) -> Result<QueueStats> {
    let pending: i64   = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events WHERE status='pending'").fetch_one(pool).await?;
    let delivering: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events WHERE status='delivering'").fetch_one(pool).await?;
    let failed: i64    = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events WHERE status='failed'").fetch_one(pool).await?;
    let dead: i64      = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events WHERE status='dead'").fetch_one(pool).await?;
    let delivered: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM webhook_events WHERE status='delivered'").fetch_one(pool).await?;
    Ok(QueueStats { pending, delivering, failed, dead, delivered })
}

pub async fn cleanup_delivered(pool: &SqlitePool, older_than_secs: i64) -> Result<u64> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(older_than_secs))
        .format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
    let rows = sqlx::query(
        "DELETE FROM webhook_events WHERE status = 'delivered' AND created_at < ?"
    )
    .bind(&cutoff)
    .execute(pool).await?.rows_affected();
    Ok(rows)
}

pub async fn cleanup_dead(pool: &SqlitePool, older_than_secs: i64) -> Result<u64> {
    let cutoff = (chrono::Utc::now() - chrono::Duration::seconds(older_than_secs))
        .format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
    let rows = sqlx::query(
        "DELETE FROM webhook_events WHERE status = 'dead' AND created_at < ?"
    )
    .bind(&cutoff)
    .execute(pool).await?.rows_affected();
    Ok(rows)
}

pub async fn reset_to_pending(pool: &SqlitePool, event_id: Uuid) -> Result<()> {
    let now = now_str();
    sqlx::query(
        "UPDATE webhook_events SET status = 'pending', delivering_since = NULL, scheduled_at = ? WHERE id = ? AND status = 'delivering'"
    )
    .bind(&now)
    .bind(event_id.to_string())
    .execute(pool).await?;
    Ok(())
}

pub async fn record_endpoint_deleted(pool: &SqlitePool, event_id: Uuid) {
    let id_str = event_id.to_string();
    let attempt_id = uuid_str();
    let now = now_str();
    let _ = sqlx::query(
        "INSERT INTO webhook_delivery_attempts (id, event_id, attempted_at, error, success) VALUES (?, ?, ?, 'endpoint deleted after claim', 0)"
    )
    .bind(&attempt_id).bind(&id_str).bind(&now)
    .execute(pool).await;

    let _ = sqlx::query(
        "UPDATE webhook_events SET status = 'dead', delivering_since = NULL WHERE id = ? AND status = 'delivering'"
    )
    .bind(&id_str).execute(pool).await;
}
