use crate::{conn_manager, message::Message, node::OpExecutionError, ring::RingError};

pub(crate) mod get;
pub(crate) mod join_ring;
pub(crate) mod put;
pub(crate) mod subscribe;

pub(crate) struct OperationResult<S> {
    /// Inhabited if there is a message to return to the other peer.
    pub return_msg: Option<Message>,
    /// None if the operation has been completed.
    pub state: Option<S>,
}

pub(crate) enum Operation {
    JoinRing(join_ring::JoinRingOp),
    Put(put::PutOp),
    Get(get::GetOp),
}

#[derive(Debug, Default)]
pub struct ProbeOp;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
    #[error(transparent)]
    OpStateManagerError(#[from] OpExecutionError),
    #[error("illegal awaiting state")]
    IllegalStateTransition,
    #[error("failed notifying back to the node message loop, channel closed")]
    NotificationError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error(transparent)]
    RingError(#[from] RingError),
}

impl From<rust_fsm::TransitionImpossibleError> for OpError {
    fn from(_: rust_fsm::TransitionImpossibleError) -> Self {
        OpError::IllegalStateTransition
    }
}
