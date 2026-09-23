//! Webhook delivery for Rust backed by Postgres or SQLite.
//!
//! HMAC-SHA256 signing, exponential backoff retry, dead letter queue,
//! idempotent sends, event-type filtering, circuit breaker, and SSRF protection —
//! all backed by your existing database. No Redis, no external queue service.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use webhooksmith::WebhookEngine;
//! use serde_json::json;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let engine = WebhookEngine::builder()
//!         .database_url("postgres://user:pass@localhost/mydb")
//!         .build()
//!         .await?;
//!
//!     engine.migrate().await?;
//!
//!     let endpoint = engine
//!         .register("https://partner.example.com/webhooks", "your-signing-secret")
//!         .await?;
//!
//!     engine.send("order.created", json!({"id": 1001}), endpoint.id).await?;
//!
//!     // Blocks until the process exits; use run_graceful() for signal handling
//!     engine.run().await;
//! }
//! ```
//!
//! # Transactional outbox
//!
//! Write your business data and the webhook event in the **same database transaction**.
//! If the transaction rolls back, the event never exists.
//!
//! ```rust,ignore
//! # use webhooksmith::WebhookEngine;
//! # use serde_json::json;
//! # use uuid::Uuid;
//! # async fn example(engine: WebhookEngine, endpoint_id: Uuid, order_id: i64) -> Result<(), Box<dyn std::error::Error>> {
//! let mut tx = engine.pool().begin().await?;
//!
//! // Your business logic — use any sqlx query here
//! sqlx::query!("INSERT INTO orders (id) VALUES ($1)", order_id)
//!     .execute(&mut *tx).await?;
//!
//! // Only queued if this transaction commits
//! engine.send_in_tx("order.created", json!({"id": order_id}), endpoint_id, &mut tx).await?;
//!
//! tx.commit().await?;
//! # Ok(()) }
//! ```
//!
//! # Features
//!
//! | Feature | Description |
//! |---|---|
//! | `postgres` (default) | Full transactional outbox using `SELECT FOR UPDATE SKIP LOCKED` |
//! | `sqlite` | `SqliteEngine` — same API, no Postgres required |
//!
//! # See also
//!
//! - [`WebhookEngine`] — core engine (Postgres)
//! - `SqliteEngine` — SQLite engine (requires `features = ["sqlite"]`)
//! - [`worker::DeliveryWorker`] — background delivery loop
//! - [Full documentation](https://github.com/biplabku/webhooksmith)

pub mod engine;
pub mod error;
pub mod model;
pub mod retry;
pub mod signing;
pub(crate) mod storage;
pub mod worker;

#[cfg(feature = "sqlite")]
pub mod backends;

#[cfg(feature = "sqlite")]
pub mod sqlite_engine;

// Re-export for testing and advanced use
pub use model::{is_private_host, is_private_ip};

pub use engine::WebhookEngine;
pub use error::{HooksmithError, Result};
pub use model::{DeliveryAttempt, Endpoint, EventStatus, NewEndpoint, QueueStats, UpdateEndpoint, WebhookEvent};
pub use model::event_matches_filter;
pub use worker::{DeliveryWorker, SsrfSafeDnsResolver};

#[cfg(feature = "sqlite")]
pub use sqlite_engine::SqliteEngine;
