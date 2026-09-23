use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use std::time::Duration;
use uuid::Uuid;
use webhooksmith::{EventStatus, WebhookEngine};

#[derive(Parser)]
#[command(
    name = "webhooksmith",
    about = "Inspect and manage webhooksmith webhook queues",
    version
)]
struct Cli {
    /// Postgres database URL. Also reads WEBHOOKSMITH_DATABASE_URL env var.
    #[arg(long, env = "WEBHOOKSMITH_DATABASE_URL")]
    db_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show queue statistics (pending, delivering, failed, dead, delivered)
    Stats,

    /// List all registered endpoints
    Endpoints,

    /// List events across all endpoints by status
    Events {
        /// Event status: pending, delivering, delivered, failed, dead
        #[arg(long, default_value = "failed")]
        status: String,

        /// Maximum number of events to return
        #[arg(long, short, default_value = "20")]
        limit: i64,
    },

    /// Show full delivery attempt log for a specific event
    Log {
        /// Event UUID
        event_id: Uuid,
    },

    /// Retry a specific dead event (resets attempts to 0)
    Retry {
        /// Event UUID
        event_id: Uuid,
    },

    /// Retry all dead events for a specific endpoint
    RetryAll {
        /// Endpoint UUID
        endpoint_id: Uuid,
    },

    /// Delete old delivered and dead events to reclaim database space
    Cleanup {
        /// Delete delivered events older than N days
        #[arg(long, default_value = "7")]
        delivered_days: u64,

        /// Delete dead events older than N days
        #[arg(long, default_value = "30")]
        dead_days: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let engine = WebhookEngine::builder()
        .database_url(&cli.db_url)
        .build()
        .await?;

    match cli.command {
        Commands::Stats => cmd_stats(&engine).await?,
        Commands::Endpoints => cmd_endpoints(&engine).await?,
        Commands::Events { status, limit } => cmd_events(&engine, &status, limit).await?,
        Commands::Log { event_id } => cmd_log(&engine, event_id).await?,
        Commands::Retry { event_id } => cmd_retry(&engine, event_id).await?,
        Commands::RetryAll { endpoint_id } => cmd_retry_all(&engine, endpoint_id).await?,
        Commands::Cleanup { delivered_days, dead_days } => {
            cmd_cleanup(&engine, delivered_days, dead_days).await?
        }
    }

    Ok(())
}

async fn cmd_stats(engine: &WebhookEngine) -> Result<()> {
    let s = engine.queue_stats().await?;
    println!("Queue statistics");
    println!("  pending:    {}", s.pending);
    println!("  delivering: {}", s.delivering);
    println!("  delivered:  {}", s.delivered);
    println!("  failed:     {}", s.failed);
    println!("  dead:       {}", s.dead);
    Ok(())
}

async fn cmd_endpoints(engine: &WebhookEngine) -> Result<()> {
    let endpoints = engine.list_endpoints().await?;
    if endpoints.is_empty() {
        println!("No endpoints registered.");
        return Ok(());
    }
    println!(
        "{:<36}  {:<8}  {:>4}  {:<42}  {}",
        "ID", "STATUS", "FAIL", "URL", "DESCRIPTION"
    );
    println!("{}", "-".repeat(110));
    for ep in endpoints {
        let status = if ep.circuit_open_until.is_some() {
            "OPEN"
        } else if !ep.enabled {
            "DISABLED"
        } else {
            "OK"
        };
        let desc = ep.description.as_deref().unwrap_or("-");
        let url = truncate(&ep.url, 42);
        println!(
            "{:<36}  {:<8}  {:>4}  {:<42}  {}",
            ep.id, status, ep.consecutive_failures, url, desc
        );
    }
    Ok(())
}

async fn cmd_events(engine: &WebhookEngine, status: &str, limit: i64) -> Result<()> {
    let event_status = parse_status(status)?;
    let events = engine.events_global(event_status, limit, 0).await?;
    if events.is_empty() {
        println!("No {} events.", status);
        return Ok(());
    }
    println!(
        "{:<36}  {:>8}  {:<22}  {}",
        "EVENT_ID", "ATTEMPTS", "CREATED", "TYPE"
    );
    println!("{}", "-".repeat(90));
    for ev in events {
        println!(
            "{:<36}  {:>8}  {:<22}  {}",
            ev.id,
            ev.attempts,
            ev.created_at.format("%Y-%m-%d %H:%M UTC"),
            ev.event_type
        );
    }
    Ok(())
}

async fn cmd_log(engine: &WebhookEngine, event_id: Uuid) -> Result<()> {
    let log = engine.delivery_log(event_id).await?;
    if log.is_empty() {
        println!("No delivery attempts found for event {}.", event_id);
        return Ok(());
    }
    println!("Delivery log for event {}:", event_id);
    println!("{:<22}  {:>6}  {:>6}  {}", "ATTEMPTED", "STATUS", "MS", "ERROR");
    println!("{}", "-".repeat(90));
    for attempt in log {
        let status = attempt
            .response_status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "-".into());
        let ms = attempt
            .duration_ms
            .map(|d| d.to_string())
            .unwrap_or_else(|| "-".into());
        let error = attempt.error.as_deref().unwrap_or("-");
        println!(
            "{:<22}  {:>6}  {:>6}  {}",
            attempt.attempted_at.format("%Y-%m-%d %H:%M:%S"),
            status,
            ms,
            error
        );
    }
    Ok(())
}

async fn cmd_retry(engine: &WebhookEngine, event_id: Uuid) -> Result<()> {
    engine.retry_dead(event_id).await?;
    println!("Event {} requeued.", event_id);
    Ok(())
}

async fn cmd_retry_all(engine: &WebhookEngine, endpoint_id: Uuid) -> Result<()> {
    let count = engine.retry_all_dead(endpoint_id).await?;
    println!("Requeued {} dead event(s) for endpoint {}.", count, endpoint_id);
    Ok(())
}

async fn cmd_cleanup(engine: &WebhookEngine, delivered_days: u64, dead_days: u64) -> Result<()> {
    let delivered = engine
        .cleanup_delivered(Duration::from_secs(delivered_days * 86_400))
        .await?;
    let dead = engine
        .cleanup_dead(Duration::from_secs(dead_days * 86_400))
        .await?;
    println!("Deleted {} delivered events older than {} days.", delivered, delivered_days);
    println!("Deleted {} dead events older than {} days.", dead, dead_days);
    Ok(())
}

fn parse_status(s: &str) -> Result<EventStatus> {
    match s.to_lowercase().as_str() {
        "pending" => Ok(EventStatus::Pending),
        "delivering" => Ok(EventStatus::Delivering),
        "delivered" => Ok(EventStatus::Delivered),
        "failed" => Ok(EventStatus::Failed),
        "dead" => Ok(EventStatus::Dead),
        _ => bail!(
            "unknown status '{}'; valid values: pending, delivering, delivered, failed, dead",
            s
        ),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max - 1])
    }
}
