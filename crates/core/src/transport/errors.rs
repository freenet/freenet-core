use std::fmt::Display;
use thiserror::Error;

#[derive(Debug, Error)]
pub(super) enum SenderStreamError {
    #[error("stream closed unexpectedly")]
    Closed,
    #[error("message too big, size: {size}, max size: {max_size}")]
    MessageExceedsLength { size: usize, max_size: usize },
}

#[derive(Debug, Error)]
pub(super) struct ConnectionError;

impl Display for ConnectionError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
