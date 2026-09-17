use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

use crate::error::{HooksmithError, Result};

type HmacSha256 = Hmac<Sha256>;

const MAX_TIMESTAMP_AGE_SECS: i64 = 300; // 5 minutes

/// Signs a webhook payload using HMAC-SHA256.
/// Header format: `v1,<hex_signature>` (Svix-compatible)
pub fn sign(secret: &str, timestamp: i64, payload: &[u8]) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| HooksmithError::Signing(e.to_string()))?;
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(payload);
    let signature = hex::encode(mac.finalize().into_bytes());
    Ok(format!("v1,{signature}"))
}

/// Verifies an incoming webhook signature.
///
/// - Rejects timestamps older than 5 minutes (replay protection).
/// - Uses constant-time comparison (timing attack protection).
/// - Accepts space-separated multiple signatures in the header.
pub fn verify(secret: &str, timestamp: i64, payload: &[u8], header: &str) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    if (now - timestamp).abs() > MAX_TIMESTAMP_AGE_SECS {
        return false;
    }

    let expected = match sign(secret, timestamp, payload) {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Constant-time comparison across all presented signatures.
    // We check all of them (not short-circuit) to avoid timing leakage
    // from the number of signatures in the header.
    let expected_bytes = expected.as_bytes();
    header
        .split(' ')
        .fold(false, |found, sig| {
            let matches: bool = sig.as_bytes().ct_eq(expected_bytes).into();
            found | matches
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ts() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    #[test]
    fn roundtrip() {
        let ts = now_ts();
        let payload = b"{\"event\":\"order.created\"}";
        let sig = sign("secret", ts, payload).unwrap();
        assert!(verify("secret", ts, payload, &sig));
    }

    #[test]
    fn wrong_secret_fails() {
        let ts = now_ts();
        let payload = b"{}";
        let sig = sign("secret_a", ts, payload).unwrap();
        assert!(!verify("secret_b", ts, payload, &sig));
    }

    #[test]
    fn stale_timestamp_rejected() {
        let old_ts = now_ts() - 400; // 400s ago, beyond 300s window
        let payload = b"{}";
        let sig = sign("secret", old_ts, payload).unwrap();
        assert!(!verify("secret", old_ts, payload, &sig));
    }

    #[test]
    fn future_timestamp_rejected() {
        let future_ts = now_ts() + 400;
        let payload = b"{}";
        let sig = sign("secret", future_ts, payload).unwrap();
        assert!(!verify("secret", future_ts, payload, &sig));
    }

    #[test]
    fn multiple_signatures_in_header() {
        let ts = now_ts();
        let payload = b"{}";
        let good_sig = sign("secret", ts, payload).unwrap();
        let header = format!("v1,aaabbbccc {good_sig}");
        assert!(verify("secret", ts, payload, &header));
    }
}
