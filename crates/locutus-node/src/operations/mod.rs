use crate::{
    conn_manager::{self, ConnectionBridge},
    contract::ContractError,
    message::{Message, Transaction, TransactionType},
    node::OpManager,
    ring::{PeerKeyLocation, RingError},
};

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
    sender: Option<PeerKeyLocation>,
) -> Result<(), OpError<CErr>>
where
    CB: ConnectionBridge,
    CErr: std::error::Error,
{
    match result {
        Err((OpError::StatePushed, _)) => {
            // do nothing and continue, the operation will just continue later on
            return Ok(());
        }
        Err((err, tx_id)) => {
            log::error!("error while processing join request: {}", err);
            if let Some(sender) = sender {
                conn_manager.send(sender, Message::Canceled(tx_id)).await?;
            }
            return Err(err);
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: Some(updated_state),
        }) => {
            // updated op
            let id = *msg.id();
            if let Some(target) = msg.target() {
                conn_manager.send(target.clone(), msg).await?;
            }
            op_storage.push(id, updated_state)?;
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: None,
        }) => {
            // finished the operation at this node, informing back
            if let Some(target) = msg.target() {
                conn_manager.send(target.clone(), msg).await?;
            }
        }
        Ok(OperationResult {
            return_msg: None,
            state: None,
        }) => {
            // operation finished_completely
        }
        _ => return Err(OpError::IllegalStateTransition),
    }
    Ok(())
}

pub(crate) enum Operation {
    JoinRing(join_ring::JoinRingOp),
    Put(put::PutOp),
    Get(get::GetOp),
    Subscribe(subscribe::SubscribeOp),
}

#[derive(Debug, Default)]
pub struct ProbeOp;

#[allow(unused)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError<S: std::error::Error> {
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
    #[error("cannot perform a state transition from the current state with the provided input")]
    IllegalStateTransition,
    #[error("failed notifying back to the node message loop, channel closed")]
    NotificationError(#[from] tokio::sync::mpsc::error::SendError<Message>),
    #[error(transparent)]
    RingError(#[from] RingError),
    #[error(transparent)]
    ContractError(#[from] ContractError<S>),
    /// This is used as an early interrumpt of an op update when an op
    /// was sent throught the fast path back to the storage.
    #[error("early push of state into the op stack")]
    StatePushed,
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[error("failed while processing transaction {0}")]
    TxUpdateFailure(Transaction),
    #[error("max number of retries for tx {0} of op type {1} reached")]
    RetriesNumber(Transaction, String),
}
