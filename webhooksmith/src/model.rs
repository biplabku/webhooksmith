use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use crate::error::{HooksmithError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Endpoint {
    pub id: Uuid,
    pub url: String,
    pub signing_secret: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub max_attempts: i32,
    pub initial_delay_ms: i32,
    /// Optional list of event type patterns this endpoint subscribes to.
    ///
    /// `None` = receive all events from `broadcast()` (default).
    /// `Some(vec!["order.*", "payment.captured"])` = only matching events.
    ///
    /// Pattern rules:
    /// - `"order.created"` — exact match
    /// - `"order.*"` — any event starting with `"order."`
    /// - `"*"` — matches all events
    pub event_filter: Option<Vec<String>>,
    /// How many consecutive delivery failures this endpoint has seen since the last success.
    /// Resets to 0 on any successful delivery.
    pub consecutive_failures: i32,
    /// If set, the circuit is open and this endpoint will be skipped until this time.
    pub circuit_open_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NewEndpoint {
    pub url: String,
    pub signing_secret: String,
    pub description: Option<String>,
    pub max_attempts: Option<i32>,
    pub initial_delay_ms: Option<i32>,
    /// Event type filter for `broadcast()`. `None` = receive all events.
    pub event_filter: Option<Vec<String>>,
}

impl NewEndpoint {
    /// Validates all fields including URL SSRF protection.
    /// Use for production endpoint creation.
    pub fn validate(&self) -> Result<()> {
        let url = self.url.parse::<url::Url>()
            .map_err(|_| HooksmithError::Config(format!("invalid URL: {}", self.url)))?;

        if url.scheme() != "https" && url.scheme() != "http" {
            return Err(HooksmithError::Config(
                "endpoint URL must use http or https".into(),
            ));
        }

        let host = url.host_str().unwrap_or("");
        if is_private_host(host) {
            return Err(HooksmithError::Config(format!(
                "endpoint URL must not target private or loopback addresses: {host}"
            )));
        }

        self.validate_fields()
    }

    /// Validates fields only — skips the URL/SSRF check.
    /// Used when `allow_insecure_urls` is set (local dev / tests).
    pub(crate) fn validate_fields(&self) -> Result<()> {
        if self.signing_secret.len() < 16 {
            return Err(HooksmithError::Config(
                "signing_secret must be at least 16 characters".into(),
            ));
        }

        let max_attempts = self.max_attempts.unwrap_or(10);
        if max_attempts < 1 {
            return Err(HooksmithError::Config(
                "max_attempts must be at least 1".into(),
            ));
        }

        // Zero or negative initial_delay_ms causes incorrect retry behaviour:
        // 0 → no backoff (hammers failing endpoint), negative → wraps on u32 cast
        // giving multi-hour delays instead of exponential backoff.
        if let Some(d) = self.initial_delay_ms {
            if d < 1 {
                return Err(HooksmithError::Config(
                    "initial_delay_ms must be at least 1".into(),
                ));
            }
        }

        // Validate event_filter patterns: no empty strings, no control chars.
        // Valid: "order.created", "order.*", "*"
        // Invalid: "", "order.", "  "
        if let Some(patterns) = &self.event_filter {
            for p in patterns {
                if p.trim().is_empty() {
                    return Err(HooksmithError::Config(
                        "event_filter patterns must not be empty".into(),
                    ));
                }
                if p.ends_with('.') && !p.ends_with(".*") {
                    return Err(HooksmithError::Config(format!(
                        "invalid event_filter pattern '{p}': use '*' for wildcard, not trailing dot"
                    )));
                }
                if p.chars().any(|c| (c as u32) < 0x20 || c == '\x7f') {
                    return Err(HooksmithError::Config(
                        "event_filter patterns must not contain control characters".into(),
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Returns true if `event_type` matches any pattern in `filter`.
///
/// Pattern rules:
/// - `"order.created"` — exact match
/// - `"order.*"` — any event whose type starts with `"order."`
/// - `"*"` — matches everything
///
/// `None` filter means "receive all" — always returns true.
pub fn event_matches_filter(event_type: &str, filter: &Option<Vec<String>>) -> bool {
    match filter {
        None => true, // no filter = receive all events
        Some(patterns) if patterns.is_empty() => false, // empty list = receive nothing
        Some(patterns) => patterns.iter().any(|p| {
            if p == "*" { return true; }
            if let Some(prefix) = p.strip_suffix(".*") {
                return event_type == prefix || event_type.starts_with(&format!("{prefix}."));
            }
            event_type == p
        }),
    }
}

/// Returns true for hosts that must not be webhook targets (SSRF protection).
pub fn is_private_host(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    // Parse as IP to catch 127.0.0.1, ::1, 10.x, 172.16-31.x, 192.168.x, 169.254.x (link-local)
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return ip.is_loopback()
            || ip.is_unspecified()
            || is_private_ip(ip);
    }
    false
}

pub fn is_private_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            let o = v4.octets();
            // 10.0.0.0/8
            o[0] == 10
                // 172.16.0.0/12
                || (o[0] == 172 && (16..=31).contains(&o[1]))
                // 192.168.0.0/16
                || (o[0] == 192 && o[1] == 168)
                // 169.254.0.0/16 — link-local / AWS metadata
                || (o[0] == 169 && o[1] == 254)
                // 100.64.0.0/10 — shared address space (RFC 6598, carrier-grade NAT)
                || (o[0] == 100 && (64..=127).contains(&o[1]))
        }
        std::net::IpAddr::V6(v6) => {
            let s = v6.segments();
            // ::1 loopback
            v6.is_loopback()
                // :: unspecified
                || v6.is_unspecified()
                // fc00::/7 — unique local (fd00:: etc.)
                || (s[0] & 0xfe00) == 0xfc00
                // fe80::/10 — link-local
                || (s[0] & 0xffc0) == 0xfe80
                // ::ffff:0:0/96 — IPv4-mapped (could map to private IPv4)
                || (s[0] == 0 && s[1] == 0 && s[2] == 0 && s[3] == 0
                    && s[4] == 0 && s[5] == 0xffff
                    && is_private_ip(std::net::IpAddr::V4(
                        std::net::Ipv4Addr::new(
                            (s[6] >> 8) as u8, s[6] as u8,
                            (s[7] >> 8) as u8, s[7] as u8,
                        )
                    )))
        }
    }
}

/// Fields to update on an existing endpoint. Only provided fields are changed.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpdateEndpoint {
    pub url: Option<String>,
    pub signing_secret: Option<String>,
    pub description: Option<Option<String>>, // Some(None) clears the description
    pub enabled: Option<bool>,
    pub max_attempts: Option<i32>,
    pub initial_delay_ms: Option<i32>,
    /// `Some(None)` clears the filter (endpoint receives all events again).
    /// `Some(Some(vec!["order.*"]))` sets a new filter.
    pub event_filter: Option<Option<Vec<String>>>,
}

impl UpdateEndpoint {
    pub(crate) fn validate(&self, allow_insecure_urls: bool) -> crate::error::Result<()> {
        use crate::error::HooksmithError;

        if let Some(url) = &self.url {
            // Always validate URL structure — only skip the SSRF IP check.
            let parsed = url
                .parse::<url::Url>()
                .map_err(|_| HooksmithError::Config(format!("invalid URL: {url}")))?;
            if parsed.scheme() != "https" && parsed.scheme() != "http" {
                return Err(HooksmithError::Config(
                    "endpoint URL must use http or https".into(),
                ));
            }
            if !allow_insecure_urls {
                let host = parsed.host_str().unwrap_or("");
                if is_private_host(host) {
                    return Err(HooksmithError::Config(format!(
                        "endpoint URL must not target private or loopback addresses: {host}"
                    )));
                }
            }
        }
        if let Some(secret) = &self.signing_secret {
            if secret.len() < 16 {
                return Err(HooksmithError::Config(
                    "signing_secret must be at least 16 characters".into(),
                ));
            }
        }
        if let Some(max) = self.max_attempts {
            if max < 1 {
                return Err(HooksmithError::Config(
                    "max_attempts must be at least 1".into(),
                ));
            }
        }
        if let Some(d) = self.initial_delay_ms {
            if d < 1 {
                return Err(HooksmithError::Config(
                    "initial_delay_ms must be at least 1".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
pub enum EventStatus {
    Pending,
    Delivering,
    Delivered,
    Failed,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct WebhookEvent {
    pub id: Uuid,
    pub endpoint_id: Uuid,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub status: EventStatus,
    pub attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub delivering_since: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Counts of webhook events grouped by status. Useful for monitoring queue health.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueStats {
    pub pending: i64,
    pub delivering: i64,
    pub failed: i64,
    pub dead: i64,
    pub delivered: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DeliveryAttempt {
    pub id: Uuid,
    pub event_id: Uuid,
    pub attempted_at: DateTime<Utc>,
    pub response_status: Option<i32>,
    pub response_body: Option<String>,
    pub duration_ms: Option<i32>,
    pub error: Option<String>,
    pub success: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_loopback() {
        let e = NewEndpoint {
            url: "http://localhost/webhook".into(),
            signing_secret: "a_secure_secret_here".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        assert!(e.validate().is_err());
    }

    #[test]
    fn rejects_aws_metadata() {
        let e = NewEndpoint {
            url: "http://169.254.169.254/latest/meta-data".into(),
            signing_secret: "a_secure_secret_here".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        assert!(e.validate().is_err());
    }

    #[test]
    fn rejects_private_ip() {
        let e = NewEndpoint {
            url: "http://192.168.1.1/hook".into(),
            signing_secret: "a_secure_secret_here".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        assert!(e.validate().is_err());
    }

    #[test]
    fn accepts_public_https() {
        let e = NewEndpoint {
            url: "https://api.example.com/webhooks".into(),
            signing_secret: "a_secure_secret_here".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        assert!(e.validate().is_ok());
    }

    #[test]
    fn rejects_short_secret() {
        let e = NewEndpoint {
            url: "https://api.example.com/webhooks".into(),
            signing_secret: "tooshort".into(),
            description: None,
            max_attempts: None,
            initial_delay_ms: None,
            event_filter: None,
        };
        assert!(e.validate().is_err());
    }
}
