# Per-Endpoint Circuit Breaker

After 5 consecutive delivery failures the circuit opens and the worker stops
retrying that endpoint. This protects your Postgres connection pool and network
from hammering a dead partner endpoint. The circuit resets automatically on the
next successful delivery, or immediately when you manually retry from the DLQ.

## How it works

| Consecutive failures | Circuit state | Open duration |
|---------------------|---------------|---------------|
| 1–4 | Closed — retrying normally | — |
| 5 | **Open** | 5 minutes |
| 6 | **Open** | 10 minutes |
| 7 | **Open** | 20 minutes |
| … | **Open** | doubles each time, capped at 320 min |
| Any success | Closed | reset |

## Reading circuit state

The circuit state is exposed on every `Endpoint` struct:

```rust
let ep = engine.endpoint(endpoint_id).await?.unwrap();

println!("consecutive_failures: {}", ep.consecutive_failures);
println!("circuit_open_until:   {:?}", ep.circuit_open_until);
```

`circuit_open_until = None` → circuit is closed.
`circuit_open_until = Some(t)` → circuit is open until `t` (UTC).

## Observing via the admin API

If you've mounted the admin router, check circuit state without writing code:

```
GET /admin/endpoints
```

```json
[
  {
    "id": "...",
    "url": "https://partner.example.com/webhooks",
    "consecutive_failures": 5,
    "circuit_open_until": "2024-01-01T12:05:00Z",
    ...
  }
]
```

## Resetting the circuit

**Automatically:** any successful delivery resets `consecutive_failures` to 0
and clears `circuit_open_until`.

**Manually (DLQ retry):** calling `retry_all_dead` or `retry_dead` also resets
the circuit — this is an operator override:

```rust
// Reset circuit + re-queue all dead events for this endpoint
engine.retry_all_dead(endpoint_id).await?;
```

Or via the admin API:

```
POST /admin/dlq/:endpoint_id/retry-all
```

## Tuning

The threshold (5 failures) and backoff schedule are fixed constants.
The circuit only affects `broadcast()` and `send()` delivery — it does NOT
prevent you from enqueuing new events. Enqueued events queue behind the circuit
and will be delivered once the circuit closes.
