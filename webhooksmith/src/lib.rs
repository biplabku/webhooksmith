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
