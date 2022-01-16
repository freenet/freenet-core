use tokio::sync::mpsc::error::SendError;

use crate::{
    contract::ContractError,
    message::{Message, Transaction, TransactionType},
    node::{ConnectionBridge, ConnectionError, OpManager, PeerKey},
    ring::RingError,
};

use self::join_ring::JoinOpError;

pub(crate) mod get;
pub(crate) mod join_ring;
pub(crate) mod put;
mod state_machine;
pub(crate) mod subscribe;

pub(crate) struct OperationResult {
    /// Inhabited if there is a message to return to the other peer.
    pub return_msg: Option<Message>,
    /// None if the operation has been completed.
    pub state: Option<Operation>,
}

async fn handle_op_result<CB, CErr>(
    op_storage: &OpManager<CErr>,
    conn_manager: &mut CB,
    result: Result<OperationResult, (OpError<CErr>, Transaction)>,
    sender: Option<PeerKey>,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    CErr: std::error::Error,
{
    // FIXME: register changes in the future op commit log
    match result {
        Err((OpError::StatePushed, _)) => {
            // do nothing and continue, the operation will just continue later on
            return Ok(());
        }
        Err((err, tx_id)) => {
            if let Some(sender) = sender {
                conn_manager.send(&sender, Message::Canceled(tx_id)).await?;
            }
            return Err(err);
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: Some(updated_state),
        }) => {
            // updated op
            let id = *msg.id();
            if let Some(target) = msg.target().cloned() {
                conn_manager.send(&target.peer, msg).await?;
            }
            op_storage.push(id, updated_state)?;
        }
        Ok(OperationResult {
            return_msg: None,
            state: Some(updated_state),
        }) => {
            // interim state
            op_storage.push(updated_state.id(), updated_state)?;
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: None,
        }) => {
            // finished the operation at this node, informing back
            if let Some(target) = msg.target().cloned() {
                conn_manager.send(&target.peer, msg).await?;
            }
        }
        Ok(OperationResult {
            return_msg: None,
            state: None,
        }) => {
            // operation finished_completely
        }
    }
    Ok(())
}

pub(crate) enum Operation {
    JoinRing(join_ring::JoinRingOp),
    Put(put::PutOp),
    Get(get::GetOp),
    Subscribe(subscribe::SubscribeOp),
}

impl Operation {
    fn id(&self) -> Transaction {
        use Operation::*;
        match self {
            JoinRing(op) => op.id(),
            Put(op) => op.id(),
            Get(op) => op.id(),
            Subscribe(op) => op.id(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError<S: std::error::Error> {
    #[error(transparent)]
    ConnError(#[from] ConnectionError),
    #[error(transparent)]
    RingError(#[from] RingError),
    #[error(transparent)]
    ContractError(#[from] ContractError<S>),
    #[error(transparent)]
    JoinOp(#[from] JoinOpError),

    #[error("unexpected operation state")]
    UnexpectedOpState,
    #[error("cannot perform a state transition from the current state with the provided input (tx: {0})")]
    InvalidStateTransition(Transaction),
    #[error("failed notifying back to the node message loop, channel closed")]
    NotificationError(#[from] Box<SendError<Message>>),
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[error("op not present: {0}")]
    OpNotPresent(Transaction),
    #[error("max number of retries for tx {0} of op type {1} reached")]
    MaxRetriesExceeded(Transaction, String),

    // user for control flow
    /// This is used as an early interrumpt of an op update when an op
    /// was sent throught the fast path back to the storage.
    #[error("early push of state into the op stack")]
    StatePushed,
}

impl<S> From<SendError<Message>> for OpError<S>
where
    S: std::error::Error,
{
    fn from(err: SendError<Message>) -> OpError<S> {
        OpError::NotificationError(Box::new(err))
    }
}
