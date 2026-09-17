use thiserror::Error;

#[derive(Debug, Error)]
pub enum HooksmithError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("http delivery error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("endpoint not found: {0}")]
    EndpointNotFound(uuid::Uuid),

    #[error("event not found: {0}")]
    EventNotFound(uuid::Uuid),

    #[error("event {0} is not in the expected state for this operation")]
    InvalidState(uuid::Uuid),

    #[error("payload too large: {0} bytes exceeds {1} byte limit")]
    PayloadTooLarge(usize, usize),

    #[error("signing error: {0}")]
    Signing(String),

    #[error("invalid configuration: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, HooksmithError>;
