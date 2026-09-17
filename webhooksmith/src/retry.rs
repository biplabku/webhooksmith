use rand::Rng;
use std::time::Duration;

/// Exponential backoff with full jitter.
/// Formula: random(initial_delay, min(cap, base * 2^attempt))
/// Lower bound is initial_delay_ms, not 0 — prevents immediate re-hammering.
pub fn next_delay(attempt: u32, initial_delay_ms: u32, cap_ms: u32) -> Duration {
    let base = initial_delay_ms as u64;
    let cap = cap_ms as u64;
    let shift = attempt.min(62);
    let ceiling = cap.min(base.saturating_mul(1u64 << shift));
    // Lower bound is base (not 0) to guarantee at least one full initial delay.
    let floor = base.min(ceiling);
    let jittered = rand::thread_rng().gen_range(floor..=ceiling);
    Duration::from_millis(jittered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_within_bounds() {
        for attempt in 0..12u32 {
            let d = next_delay(attempt, 1000, 3_600_000);
            assert!(d.as_millis() <= 3_600_000);
        }
    }

    #[test]
    fn first_attempt_at_least_initial_delay() {
        for _ in 0..200 {
            let d = next_delay(0, 1000, 3_600_000);
            assert!(d.as_millis() >= 1000, "delay was {}ms", d.as_millis());
            assert!(d.as_millis() <= 1000); // attempt=0: ceiling=base*1=base
        }
    }

    #[test]
    fn second_attempt_within_double() {
        for _ in 0..200 {
            let d = next_delay(1, 1000, 3_600_000);
            assert!(d.as_millis() >= 1000);
            assert!(d.as_millis() <= 2000);
        }
    }

    #[test]
    fn caps_at_max() {
        for attempt in 20..30u32 {
            let d = next_delay(attempt, 1000, 60_000);
            assert!(d.as_millis() <= 60_000);
            assert!(d.as_millis() >= 1000);
        }
    }
}
