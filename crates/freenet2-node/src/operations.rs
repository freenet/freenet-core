use crate::{conn_manager, message::Message, node::OpExecutionError};

pub(crate) mod get;
pub(crate) mod join_ring;
pub(crate) mod put;
pub(crate) mod routing;
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
}
