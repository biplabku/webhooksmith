//! How-to: Multi-tenant webhooks — one endpoint per customer.
//!
//! Common in SaaS: each customer registers their own webhook URL and secret.
//! When events happen, only the relevant customer is notified.
//!
//! Shows:
//! - Registering per-customer endpoints dynamically
//! - Sending to a specific customer's endpoint
//! - Broadcasting to all customers at once
//! - Disabling a customer's endpoint when they cancel
//! - Idempotent sends for retry-safe event delivery
//!
//! Run with:
//!   docker compose up -d
//!   cargo run --example multi_tenant -p webhooksmith

use serde_json::json;
use webhooksmith::{NewEndpoint, WebhookEngine};

const DATABASE_URL: &str = "postgres://hooksmith:hooksmith@localhost:5432/hooksmith";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().without_time().init();

    let engine = WebhookEngine::builder()
        .database_url(DATABASE_URL)
        .allow_insecure_urls() // remove in production
        .build()
        .await?;
    engine.migrate().await?;

    println!("=== Multi-tenant webhook demo ===\n");

    // ── Step 1: Each customer registers their webhook endpoint ─────────────────
    // In a real app, customers provide this via your settings UI

    let customer_a = engine.register_with(NewEndpoint {
        url: "https://hooks.customer-a.com/webhooks".into(),
        signing_secret: generate_secret("customer-a"),
        description: Some("Customer A production endpoint".into()),
        max_attempts: Some(10),
        initial_delay_ms: Some(1000),
    }).await;

    // customer-a.com doesn't exist, so we'll simulate with a fake endpoint
    let customer_a_id = if let Ok(ep) = customer_a {
        println!("Customer A registered: {}", ep.id);
        ep.id
    } else {
        // For the demo, use a placeholder
        println!("(Customer A endpoint skipped — URL not reachable in demo)");
        uuid::Uuid::new_v4()
    };

    let customer_b = engine.register_with(NewEndpoint {
        url: "https://hooks.customer-b.io/events".into(),
        signing_secret: generate_secret("customer-b"),
        description: Some("Customer B webhook".into()),
        max_attempts: Some(5),
        initial_delay_ms: Some(500),
    }).await;

    let customer_b_id = if let Ok(ep) = customer_b {
        println!("Customer B registered: {}", ep.id);
        ep.id
    } else {
        println!("(Customer B endpoint skipped — URL not reachable in demo)");
        uuid::Uuid::new_v4()
    };

    // ── Step 2: Send to a specific customer (event for customer A only) ────────

    println!("\n-- Sending order.created to Customer A only --");
    if let Ok(event) = engine.send(
        "order.created",
        json!({
            "order_id": "ord_abc123",
            "customer_id": "cust_a",
            "amount": 99.99,
        }),
        customer_a_id,
    ).await {
        println!("Queued event {} for Customer A", event.id);
    }

    // ── Step 3: Idempotent send — safe to retry from your side ────────────────

    println!("\n-- Idempotent send (safe to call multiple times) --");
    // Use a business-meaningful key: customer + event + resource ID
    let idem_key = "order.created:cust_a:ord_abc123";

    let e1 = engine.send_idempotent("order.created", json!({"order_id": "ord_abc123"}),
        customer_a_id, idem_key).await;
    let e2 = engine.send_idempotent("order.created", json!({"order_id": "ord_abc123"}),
        customer_a_id, idem_key).await;

    if let (Ok(ev1), Ok(ev2)) = (e1, e2) {
        assert_eq!(ev1.id, ev2.id, "Same event returned both times");
        println!("Same event ID returned: {} (no duplicate!)", ev1.id);
    }

    // ── Step 4: Broadcast to ALL customers (system-wide announcement) ─────────

    println!("\n-- Broadcasting maintenance.scheduled to all customers --");
    let events = engine.broadcast(
        "maintenance.scheduled",
        json!({
            "start": "2026-01-01T02:00:00Z",
            "duration_minutes": 30,
            "affected": "all",
        }),
    ).await?;
    println!("Notified {} customer endpoints", events.len());

    // ── Step 5: Idempotent broadcast (safe even if called twice) ──────────────

    println!("\n-- Idempotent broadcast (only queued once per customer) --");
    let b1 = engine.broadcast_idempotent(
        "maintenance.scheduled",
        json!({"start": "2026-01-01T02:00:00Z"}),
        "maintenance:2026-01-01:scheduled",
    ).await?;
    let b2 = engine.broadcast_idempotent(
        "maintenance.scheduled",
        json!({"start": "2026-01-01T02:00:00Z"}),
        "maintenance:2026-01-01:scheduled",
    ).await?;

    let ids1: std::collections::HashSet<_> = b1.iter().map(|e| e.id).collect();
    let ids2: std::collections::HashSet<_> = b2.iter().map(|e| e.id).collect();
    assert_eq!(ids1, ids2);
    println!("Same {} events returned both times (no duplicates)", b1.len());

    // ── Step 6: Disable a customer's endpoint when they cancel ────────────────

    println!("\n-- Customer B cancels — disabling their endpoint --");
    if engine.disable_endpoint(customer_b_id).await.is_ok() {
        println!("Customer B endpoint disabled — future events will not be delivered");
        // To re-enable if they re-subscribe:
        // engine.enable_endpoint(customer_b_id).await?;
    }

    // ── Step 7: List all active endpoints ────────────────────────────────────

    let all = engine.list_endpoints().await?;
    println!("\nActive endpoints: {}", all.len());
    for ep in &all {
        println!("  {} — {} (enabled: {})", ep.id, ep.description.as_deref().unwrap_or("no description"), ep.enabled);
    }

    // ── Step 8: Queue stats ───────────────────────────────────────────────────

    let stats = engine.queue_stats().await?;
    println!("\nQueue: pending={} delivered={} failed={} dead={}",
        stats.pending, stats.delivered, stats.failed, stats.dead);

    println!("\n=== Done ===");
    Ok(())
}

/// Generate a per-customer signing secret.
/// In production: use a cryptographically random secret stored in your DB.
fn generate_secret(customer_id: &str) -> String {
    format!("whsec_{customer_id}_replace_with_random_32_chars")
}
