# Graceful Shutdown

`run_graceful` drains the current delivery batch before exiting, making it safe
for Kubernetes rolling deploys and `systemd` stop signals.

## Basic usage

```rust
use webhooksmith::WebhookEngine;
use tokio::signal;

let engine: Arc<WebhookEngine> = /* ... */;

engine.run_graceful(async {
    signal::ctrl_c().await.expect("ctrl-c signal");
    tracing::info!("shutdown signal received, draining...");
}).await;

tracing::info!("worker stopped cleanly");
```

## With axum server

Run the HTTP server and the webhook worker concurrently; both stop on SIGTERM:

```rust
use std::sync::Arc;
use tokio::signal;

let engine = Arc::new(engine);
let engine_worker = Arc::clone(&engine);

let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

// Worker task
tokio::spawn(async move {
    engine_worker.run_graceful(async move {
        let _ = shutdown_rx.await;
    }).await;
});

// HTTP server
axum::serve(listener, app)
    .with_graceful_shutdown(async {
        signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(());
    })
    .await?;
```

## What "draining" means

`run_graceful` does not interrupt in-flight HTTP deliveries. It:
1. Finishes the current batch of claimed events
2. Returns without starting another poll loop

Events claimed but not yet completed are reset to `pending` by
`recover_stuck_deliveries` when the next worker instance starts.

## SQLite

`SqliteEngine::run_graceful` has the same signature and behaviour:

```rust
sqlite_engine.run_graceful(signal::ctrl_c()).await;
```
