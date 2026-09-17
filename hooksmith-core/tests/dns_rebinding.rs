//! DNS rebinding protection tests.
//!
//! The attack: endpoint registered with `https://attacker.com/hook` (public IP
//! at registration time passes SSRF validation). Attacker changes DNS to
//! `10.0.0.1` after registration. Without protection, the worker delivers
//! webhook data to the internal service.
//!
//! The fix: a custom DNS resolver plugged into reqwest that filters out any
//! private/loopback IP at resolution time — not just at registration time.
//!
//! Note: the resolver is only called for hostnames. Raw IPs in URLs (e.g.
//! `http://127.0.0.1/hook`) bypass DNS in hyper and are caught at registration.

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();

use hooksmith::{EventStatus, WebhookEngine};
use hooksmith::worker::SsrfSafeDnsResolver;
use reqwest::dns::{Resolve, Name};
use serde_json::json;
use std::str::FromStr;
use sqlx::PgPool;

fn engine(pool: PgPool) -> WebhookEngine {
    WebhookEngine::builder()
        .pool(pool)
        .allow_insecure_urls()
        .build_sync()
}

// ── Unit: IP filter logic ────────────────────────────────────────────────────

#[test]
fn loopback_is_blocked() {
    use std::net::IpAddr;
    let ip: IpAddr = "127.0.0.1".parse().unwrap();
    assert!(ip.is_loopback());
}

#[test]
fn private_ranges_are_blocked() {
    use hooksmith::model::is_private_ip;
    use std::net::IpAddr;

    let blocked = [
        "10.0.0.1",
        "10.255.255.255",
        "172.16.0.1",
        "172.31.255.255",
        "192.168.0.1",
        "192.168.255.255",
        "169.254.169.254", // AWS metadata
        "169.254.0.1",     // link-local
    ];

    for ip_str in &blocked {
        let ip: IpAddr = ip_str.parse().unwrap();
        assert!(
            is_private_ip(ip),
            "{ip_str} must be identified as private"
        );
    }
}

#[test]
fn public_ips_are_allowed() {
    use hooksmith::model::is_private_ip;
    use std::net::IpAddr;

    let allowed = [
        "8.8.8.8",
        "1.1.1.1",
        "93.184.216.34", // example.com
        "172.15.255.255", // just outside 172.16/12
        "172.32.0.0",     // just outside 172.16-31/12
    ];

    for ip_str in &allowed {
        let ip: IpAddr = ip_str.parse().unwrap();
        assert!(
            !is_private_ip(ip),
            "{ip_str} must not be identified as private"
        );
    }
}

// ── Unit: resolver rejects private hostnames ─────────────────────────────────

#[tokio::test]
async fn resolver_blocks_localhost_hostname() {
    let resolver = SsrfSafeDnsResolver;
    let name = Name::from_str("localhost").unwrap();
    let result = resolver.resolve(name).await;
    assert!(result.is_err(), "resolver must reject 'localhost' (resolves to 127.0.0.1)");
    // Can't call unwrap_err() — Addrs doesn't implement Debug
    let err_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => unreachable!(),
    };
    assert!(
        err_msg.contains("SSRF"),
        "error message must mention SSRF protection, got: {err_msg}"
    );
}

// ── Integration: hostname resolving to private IP → delivery failure ──────────
//
// We register an endpoint with `http://localhost/hook` (via allow_insecure_urls).
// The SSRF-safe resolver blocks the delivery because `localhost` → `127.0.0.1`.
// The event must end up in 'failed' state with a delivery attempt record.
// Nothing should panic or leave the event stuck in 'delivering'.

#[sqlx::test(migrator = "MIGRATOR")]
async fn delivery_to_private_hostname_is_recorded_as_failure(pool: PgPool) {
    let engine = engine(pool);

    // Register endpoint with a hostname that resolves to loopback.
    // allow_insecure_urls() bypasses the registration SSRF check,
    // but the DNS resolver still fires at delivery time.
    let endpoint = engine
        .register("http://localhost/hook", "dns_rebinding_test_secret_ok")
        .await
        .unwrap();

    let event = engine
        .send("test.event", json!({}), endpoint.id)
        .await
        .unwrap();

    engine.run_once().await.unwrap();

    // Event must be in 'failed' state — not stuck in 'delivering', not panicked
    let updated = engine.event(event.id).await.unwrap().unwrap();
    assert_eq!(
        updated.status,
        EventStatus::Failed,
        "delivery to a private hostname must be recorded as a failure"
    );

    // A delivery attempt record must exist
    let log = engine.delivery_log(event.id).await.unwrap();
    assert_eq!(log.len(), 1, "one delivery attempt must be recorded");
    assert!(!log[0].success, "attempt must be marked unsuccessful");

    // reqwest wraps our DNS error — what matters is that the error is non-null
    // and the attempt was recorded (not silently dropped or stuck in 'delivering')
    assert!(
        log[0].error.is_some(),
        "delivery attempt must record an error message"
    );
}
