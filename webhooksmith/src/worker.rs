use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use futures::{future::join_all, StreamExt};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use sqlx::PgPool;
use tracing::{error, info, warn};

/// Maximum bytes read from an HTTP response body (streaming limit + storage cap).
/// Defined once here; `storage` imports it to stay in sync.
pub(crate) const MAX_RESPONSE_BODY_BYTES: usize = 4096;

use crate::{
    error::{HooksmithError, Result},
    model::{is_private_ip, EventStatus, WebhookEvent},
    signing,
    storage::{self, get_endpoint, record_failure, record_success},
};

// ── SSRF-safe DNS resolver ────────────────────────────────────────────────────
//
// Plugged into the reqwest client so every hostname is resolved at delivery
// time and any private/loopback IP is rejected.
//
// Why this is needed: SSRF validation runs at endpoint *registration* time
// (public IP passes). An attacker can change their DNS entry to a private IP
// after registration. This resolver re-checks every resolution (DNS rebinding
// protection).
//
// Note: the resolver is only called for hostnames. Raw IP addresses in the
// URL (e.g. http://127.0.0.1/hook) bypass DNS resolution in hyper; those are
// caught at registration time by the SSRF URL validation instead.

#[derive(Debug)]
pub struct SsrfSafeDnsResolver;

impl Resolve for SsrfSafeDnsResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            // Resolve via the OS resolver (same path as reqwest's default).
            // Port 0 is a placeholder — reqwest overrides it with the URL's port.
            let addrs: Vec<SocketAddr> = tokio::net::lookup_host(format!("{host}:0"))
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
                .filter(|addr| {
                    !addr.ip().is_loopback()
                        && !addr.ip().is_unspecified()
                        && !is_private_ip(addr.ip())
                })
                .collect();

            if addrs.is_empty() {
                return Err(format!(
                    "SSRF protection blocked delivery: '{host}' resolved only to \
                     private or loopback addresses"
                )
                .into());
            }

            Ok(Box::new(addrs.into_iter()) as Addrs)
        })
    }
}

pub const DEFAULT_BATCH_SIZE: i64 = 50;
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(500);
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_STUCK_TIMEOUT: Duration = Duration::from_secs(120);

fn build_http_client(http_timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(http_timeout)
        // Block redirects — a 301 to an internal URL bypasses SSRF registration check.
        .redirect(reqwest::redirect::Policy::none())
        // Block private/loopback IPs at DNS resolution (DNS rebinding protection).
        .dns_resolver(Arc::new(SsrfSafeDnsResolver))
        .build()
        .expect("failed to build HTTP client")
}

pub struct DeliveryWorker {
    pool: PgPool,
    client: reqwest::Client,
    batch_size: i64,
    poll_interval: Duration,
    stuck_timeout_secs: i64,
}

impl DeliveryWorker {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            client: build_http_client(DEFAULT_HTTP_TIMEOUT),
            batch_size: DEFAULT_BATCH_SIZE,
            poll_interval: DEFAULT_POLL_INTERVAL,
            stuck_timeout_secs: DEFAULT_STUCK_TIMEOUT.as_secs() as i64,
        }
    }

    pub fn with_batch_size(mut self, n: i64) -> Self {
        self.batch_size = n;
        self
    }

    pub fn with_poll_interval(mut self, d: Duration) -> Self {
        self.poll_interval = d;
        self
    }

    /// Maximum time to wait for an HTTP response from an endpoint.
    /// Default: 30 seconds.
    pub fn with_http_timeout(mut self, d: Duration) -> Self {
        self.client = build_http_client(d);
        self
    }

    /// Events stuck in 'delivering' longer than this are reset by the reaper.
    /// Should be longer than your HTTP timeout so legitimate slow deliveries
    /// are not interrupted. Default: 120 seconds.
    pub fn with_stuck_timeout(mut self, d: Duration) -> Self {
        self.stuck_timeout_secs = d.as_secs() as i64;
        self
    }

    /// Runs one full delivery cycle: recover stuck events, claim, deliver all.
    /// Returns the number of events processed.
    /// This is the unit of work — the run loop calls this repeatedly.
    pub async fn run_once(&self) -> Result<usize> {
        let recovered = storage::recover_stuck_deliveries(&self.pool, self.stuck_timeout_secs).await?;
        if recovered > 0 {
            warn!(count = recovered, "reset events stuck in delivering state");
        }

        let events = storage::claim_due_events(&self.pool, self.batch_size).await?;
        let count = events.len();

        if count == 0 {
            return Ok(0);
        }

        info!(count, "claimed events for delivery");

        let tasks: Vec<_> = events
            .into_iter()
            .map(|event| {
                let pool = self.pool.clone();
                let client = self.client.clone();
                tokio::spawn(async move {
                    if let Err(e) = deliver_event(&pool, &client, &event).await {
                        error!(event_id = %event.id, error = %e, "delivery task failed");
                    }
                })
            })
            .collect();

        // Wait for all deliveries in this batch before returning.
        // Delivery errors are logged inside each task.
        // JoinErrors indicate a task panic — log them so they're not invisible.
        for result in join_all(tasks).await {
            if let Err(e) = result {
                error!(error = %e, "delivery task panicked — event will be reset by reaper");
            }
        }

        Ok(count)
    }

    /// Runs the worker loop until the process is killed.
    /// Sleeps poll_interval between empty batches; runs immediately if batch was full.
    pub async fn run(&self) -> ! {
        loop {
            match self.run_once().await {
                Ok(0) => tokio::time::sleep(self.poll_interval).await,
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "worker cycle failed");
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    /// Runs the worker until `shutdown` resolves.
    ///
    /// The current delivery batch completes fully before the worker exits —
    /// in-flight HTTP requests are not interrupted. No new batch is claimed
    /// after the shutdown signal is received.
    pub async fn run_graceful<F: std::future::Future<Output = ()>>(&self, shutdown: F) {
        tokio::pin!(shutdown);
        loop {
            match self.run_once().await {
                Ok(0) => {
                    // Nothing to do — sleep or exit
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => return,
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
                Ok(_) => {
                    // Batch complete — check shutdown before claiming the next one
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => return,
                        _ = std::future::ready(()) => {}
                    }
                }
                Err(e) => {
                    error!(error = %e, "worker cycle failed");
                    tokio::select! {
                        biased;
                        _ = &mut shutdown => return,
                        _ = tokio::time::sleep(self.poll_interval) => {}
                    }
                }
            }
        }
    }
}

/// Reads at most `limit` bytes from a response body by streaming chunks.
/// Stops consuming the stream once the limit is reached — never allocates more.
async fn read_body_limited(response: reqwest::Response, limit: usize) -> Option<String> {
    let mut buf = Vec::with_capacity(limit.min(1024));
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                let remaining = limit.saturating_sub(buf.len());
                if remaining == 0 {
                    break;
                }
                buf.extend_from_slice(&bytes[..bytes.len().min(remaining)]);
            }
            Err(_) => break,
        }
    }
    if buf.is_empty() {
        None
    } else {
        Some(String::from_utf8_lossy(&buf).into_owned())
    }
}

/// Deliver a single webhook event.
///
/// This function is instrumented with a `tracing` span named `webhook.deliver`.
/// Applications using `tracing-opentelemetry` will see it as an OTel span with
/// the following attributes:
///
/// | Attribute | Value |
/// |-----------|-------|
/// | `webhook.event_id` | UUID of the event |
/// | `webhook.event_type` | e.g. `"order.created"` |
/// | `webhook.endpoint_id` | UUID of the endpoint |
/// | `webhook.attempt` | current attempt number |
/// | `http.status_code` | response status (set after delivery) |
/// | `webhook.success` | `true` or `false` |
/// | `webhook.duration_ms` | delivery latency in milliseconds |
#[tracing::instrument(
    name = "webhook.deliver",
    skip(pool, client),
    fields(
        webhook.event_id    = %event.id,
        webhook.event_type  = %event.event_type,
        webhook.endpoint_id = %event.endpoint_id,
        webhook.attempt     = event.attempts + 1,
        http.status_code    = tracing::field::Empty,
        webhook.success     = tracing::field::Empty,
        webhook.duration_ms = tracing::field::Empty,
    )
)]
async fn deliver_event(
    pool: &PgPool,
    client: &reqwest::Client,
    event: &WebhookEvent,
) -> Result<()> {
    // Should never happen if claim_due_events is correct, but guard defensively.
    if event.status != EventStatus::Delivering {
        warn!(event_id = %event.id, status = ?event.status, "skipping event not in delivering state");
        return Ok(());
    }

    let endpoint = match get_endpoint(pool, event.endpoint_id).await? {
        Some(ep) => ep,
        None => {
            // Endpoint was deleted after the event was claimed.
            // ON DELETE CASCADE likely deleted the event too; record_endpoint_deleted
            // handles both cases (event exists or was cascade-deleted) without panicking.
            warn!(
                event_id = %event.id,
                endpoint_id = %event.endpoint_id,
                "endpoint not found during delivery — deleted after claim"
            );
            storage::record_endpoint_deleted(pool, event.id).await;
            tracing::Span::current().record("webhook.success", false);
            return Ok(());
        }
    };

    // Endpoint could have been disabled between claim and delivery.
    if !endpoint.enabled {
        warn!(event_id = %event.id, endpoint_id = %endpoint.id, "endpoint disabled after claim, resetting to pending");
        storage::reset_to_pending(pool, event.id).await?;
        tracing::Span::current().record("webhook.success", false);
        return Ok(());
    }

    // serde_json::Value always serializes — this branch is unreachable in practice,
    // but the error type reflects "payload" not "signing".
    let payload_bytes = serde_json::to_vec(&event.payload)
        .map_err(|e| HooksmithError::Config(format!("payload serialization error: {e}")))?;

    let timestamp = Utc::now().timestamp();
    let signature = signing::sign(&endpoint.signing_secret, timestamp, &payload_bytes)?;

    let started = Instant::now();
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

    // Clamp to i32::MAX before casting — prevents silent overflow if http_timeout
    // is ever set higher than ~24 days (unrealistic, but correct).
    let duration_ms = started.elapsed().as_millis().min(i32::MAX as u128) as i32;
    let span = tracing::Span::current();
    span.record("webhook.duration_ms", duration_ms);

    match response {
        Ok(resp) => {
            let status = resp.status().as_u16() as i32;
            // Stream response body up to the limit — never buffer more than needed.
            // A malicious endpoint returning a gigabyte would otherwise OOM the worker.
            let body = read_body_limited(resp, MAX_RESPONSE_BODY_BYTES).await;

            span.record("http.status_code", status);

            if (200..300).contains(&status) {
                span.record("webhook.success", true);
                info!(event_id = %event.id, status, duration_ms, "delivered");
                record_success(pool, event.id, status, body, duration_ms).await?;
            } else {
                span.record("webhook.success", false);
                warn!(event_id = %event.id, status, duration_ms, "endpoint returned non-2xx");
                record_failure(
                    pool,
                    event,
                    &endpoint,
                    format!("HTTP {status}"),
                    Some(status),
                    Some(duration_ms),
                )
                .await?;
            }
        }
        Err(e) => {
            span.record("webhook.success", false);
            warn!(event_id = %event.id, error = %e, duration_ms, "http error");
            record_failure(pool, event, &endpoint, e.to_string(), None, Some(duration_ms)).await?;
        }
    }

    Ok(())
}
