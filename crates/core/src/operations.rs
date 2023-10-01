use tokio::sync::mpsc::error::SendError;

use self::op_trait::Operation;
use crate::{
    client_events::{ClientId, HostResult},
    contract::ContractError,
    message::{InnerMessage, Message, Transaction, TransactionType},
    node::{ConnectionBridge, ConnectionError, OpManager, PeerKey},
    operations::{get::GetOp, join_ring::JoinRingOp, put::PutOp, subscribe::SubscribeOp},
    ring::RingError,
};

pub(crate) mod get;
pub(crate) mod join_ring;
pub(crate) mod op_trait;
pub(crate) mod put;
pub(crate) mod subscribe;
pub(crate) mod update;

pub(crate) struct OperationResult {
    /// Inhabited if there is a message to return to the other peer.
    pub return_msg: Option<Message>,
    /// None if the operation has been completed.
    pub state: Option<OpEnum>,
}

pub(crate) struct OpInitialization<Op> {
    sender: Option<PeerKey>,
    op: Op,
}

pub(crate) async fn handle_op_request<Op, CB>(
    op_storage: &OpManager,
    conn_manager: &mut CB,
    msg: Op::Message,
    client_id: Option<ClientId>,
) -> Result<Option<OpEnum>, OpError>
where
    Op: Operation<CB>,
    CB: ConnectionBridge,
{
    let sender;
    let tx = *msg.id();
    let result = {
        let OpInitialization { sender: s, op } = Op::load_or_init(op_storage, &msg)?;
        sender = s;
        op.process_message(conn_manager, op_storage, msg, client_id)
            .await
    };
    handle_op_result(
        op_storage,
        conn_manager,
        result.map_err(|err| (err, tx)),
        sender,
    )
    .await
}

async fn handle_op_result<CB>(
    op_storage: &OpManager,
    conn_manager: &mut CB,
    result: Result<OperationResult, (OpError, Transaction)>,
    sender: Option<PeerKey>,
) -> Result<Option<OpEnum>, OpError>
where
    CB: ConnectionBridge,
{
    // FIXME: register changes in the future op commit log
    match result {
        Err((OpError::StatePushed, _)) => {
            // do nothing and continue, the operation will just continue later on
            return Ok(None);
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
            state: Some(final_state),
        }) if final_state.is_final() => {
            // operation finished_completely with result
            return Ok(Some(final_state));
        }
        Ok(OperationResult {
            return_msg: None,
            state: Some(updated_state),
        }) => {
            // interim state
            let id = OpEnum::id::<CB>(&updated_state);
            op_storage.push(id, updated_state)?;
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
    Ok(None)
}

pub(crate) enum OpEnum {
    JoinRing(Box<join_ring::JoinRingOp>),
    Put(put::PutOp),
    Get(get::GetOp),
    Subscribe(subscribe::SubscribeOp),
}

impl OpEnum {
    fn id<CB: ConnectionBridge>(&self) -> Transaction {
        use OpEnum::*;
        match self {
            JoinRing(op) => *<JoinRingOp as Operation<CB>>::id(op),
            Put(op) => *<PutOp as Operation<CB>>::id(op),
            Get(op) => *<GetOp as Operation<CB>>::id(op),
            Subscribe(op) => *<SubscribeOp as Operation<CB>>::id(op),
        }
    }

    fn is_final(&self) -> bool {
        match self {
            OpEnum::JoinRing(op) if op.finished() => true,
            OpEnum::Put(op) if op.finished() => true,
            OpEnum::Get(op) if op.finished() => true,
            OpEnum::Subscribe(op) if op.finished() => true,
            _ => false,
        }
    }

    pub fn to_host_result(&self, _client_id: ClientId) -> HostResult {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error(transparent)]
    ConnError(#[from] ConnectionError),
    #[error(transparent)]
    RingError(#[from] RingError),
    #[error(transparent)]
    ContractError(#[from] ContractError),

    #[error("unexpected operation state")]
    UnexpectedOpState,
    #[error("cannot perform a state transition from the current state with the provided input (tx: {0})")]
    InvalidStateTransition(Transaction),
    #[error("failed notifying back to the node message loop, channel closed")]
    NotificationError(#[from] Box<SendError<(Message, Option<ClientId>)>>),
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

impl From<SendError<(Message, Option<ClientId>)>> for OpError {
    fn from(err: SendError<(Message, Option<ClientId>)>) -> OpError {
        OpError::NotificationError(Box::new(err))
    }
}
