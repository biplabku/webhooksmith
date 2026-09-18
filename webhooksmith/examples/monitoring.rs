//! How-to: Monitor webhook queue health and alert on failures.
//!
//! Shows how to:
//! - Poll queue stats and log them
//! - Alert when DLQ grows (events that exhausted all retries)
//! - Retry all failed events for a specific endpoint
//! - Clean up old delivered events to control DB size
//!
//! In production, send queue_stats to Prometheus/Datadog/CloudWatch instead of printing.
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example monitoring -p webhooksmith

use std::time::Duration;
use webhooksmith::WebhookEngine;

const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().without_time().init();

    let engine = WebhookEngine::builder()
        .database_url(DATABASE_URL)
        .build()
        .await?;
    engine.migrate().await?;

    // ── Queue health check ────────────────────────────────────────────────────

    let stats = engine.queue_stats().await?;

    println!("=== Webhook Queue Health ===");
    println!("  pending:    {} (waiting to be delivered)", stats.pending);
    println!("  delivering: {} (in-flight right now)", stats.delivering);
    println!("  failed:     {} (will retry automatically)", stats.failed);
    println!("  dead:       {} (exhausted retries — needs attention)", stats.dead);
    println!("  delivered:  {} (success)", stats.delivered);

    // ── Alert on DLQ growth ───────────────────────────────────────────────────
    // In production, send this to PagerDuty/OpsGenie/Slack instead of printing

    if stats.dead > 0 {
        println!("\n⚠️  {} events in dead letter queue — partner may be down", stats.dead);

        // List the first 20 dead events to understand what failed
        let endpoints = engine.list_endpoints().await?;
        for ep in &endpoints {
            let dead = engine.dead_events_paged(ep.id, 20, 0).await?;
            if dead.is_empty() { continue; }

            println!("\n  Endpoint: {} ({})", ep.id, ep.url);
            for event in &dead {
                let log = engine.delivery_log(event.id).await?;
                let last = log.last();
                println!(
                    "    event={} type={} attempts={} last_error={}",
                    event.id,
                    event.event_type,
                    event.attempts,
                    last.and_then(|a| a.error.as_deref()).unwrap_or("unknown"),
                );
            }
        }
    }

    // ── Retry all dead events for a specific endpoint ─────────────────────────

    let endpoints = engine.list_endpoints().await?;
    for ep in &endpoints {
        let dead_count = engine.dead_events(ep.id).await?.len() as u64;
        if dead_count > 0 {
            println!("\nRetrying {} dead events for endpoint {}", dead_count, ep.url);
            let retried = engine.retry_all_dead(ep.id).await?;
            println!("  Requeued {} events", retried);
        }
    }

    // ── Failed events by endpoint ─────────────────────────────────────────────

    use webhooksmith::EventStatus;
    for ep in &endpoints {
        let failed = engine.events_by_status(ep.id, EventStatus::Failed, 10, 0).await?;
        if failed.is_empty() { continue; }
        println!("\n  {} failed events for {} (will auto-retry)", failed.len(), ep.url);
    }

    // ── Cleanup old delivered events (run daily in production) ────────────────

    let removed = engine.cleanup_delivered(Duration::from_secs(7 * 86_400)).await?;
    println!("\nCleaned up {} delivered events older than 7 days", removed);

    let dead_removed = engine.cleanup_dead(Duration::from_secs(30 * 86_400)).await?;
    println!("Cleaned up {} dead events older than 30 days", dead_removed);

    // ── Global view of all failed events ─────────────────────────────────────

    let all_failed = engine.events_global(EventStatus::Failed, 50, 0).await?;
    println!("\n{} total failed events across all endpoints", all_failed.len());

    println!("\n=== Done ===");
    Ok(())
}
