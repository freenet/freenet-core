//! Error types for contract interface operations.

use serde::{Deserialize, Serialize};

/// Type of errors during interaction with a contract.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum ContractError {
    #[error("de/serialization error: {0}")]
    Deser(String),
    #[error("invalid contract update")]
    InvalidUpdate,
    #[error("invalid contract update, reason: {reason}")]
    InvalidUpdateWithInfo { reason: String },
    #[error("trying to read an invalid state")]
    InvalidState,
    #[error("trying to read an invalid delta")]
    InvalidDelta,
    #[error("{0}")]
    Other(String),
}
