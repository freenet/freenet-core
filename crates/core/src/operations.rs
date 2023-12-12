#[cfg(debug_assertions)]
use std::backtrace::Backtrace as StdTrace;
use std::{pin::Pin, time::Duration};

use futures::{future::BoxFuture, Future};
use tokio::sync::mpsc::error::SendError;

use crate::{
    client_events::{ClientId, HostResult},
    contract::ContractError,
    message::{InnerMessage, NetMessage, Transaction, TransactionType},
    node::{ConnectionError, NetworkBridge, OpManager, OpNotAvailable, PeerId},
    ring::{Location, PeerKeyLocation, RingError},
    DynError,
};

pub(crate) mod connect;
pub(crate) mod get;
pub(crate) mod put;
pub(crate) mod subscribe;
pub(crate) mod update;

pub(crate) struct OperationResult {
    /// Inhabited if there is a message to return to the other peer.
    pub return_msg: Option<NetMessage>,
    /// None if the operation has been completed.
    pub state: Option<OpEnum>,
}

pub(crate) struct OpInitialization<Op> {
    sender: Option<PeerId>,
    op: Op,
}

pub(crate) async fn handle_op_request<Op, NB>(
    op_manager: &OpManager,
    network_bridge: &mut NB,
    msg: &Op::Message,
) -> Result<Option<OpEnum>, OpError>
where
    Op: Operation,
    NB: NetworkBridge,
{
    let sender;
    let tx = *msg.id();
    let result = {
        let OpInitialization { sender: s, op } = Op::load_or_init(op_manager, msg).await?;
        sender = s;
        op.process_message(network_bridge, op_manager, msg).await
    };
    handle_op_result(op_manager, network_bridge, result, tx, sender).await
}

async fn handle_op_result<CB>(
    op_manager: &OpManager,
    network_bridge: &mut CB,
    result: Result<OperationResult, OpError>,
    tx_id: Transaction,
    sender: Option<PeerId>,
) -> Result<Option<OpEnum>, OpError>
where
    CB: NetworkBridge,
{
    match result {
        Err(OpError::StatePushed) => {
            // do nothing and continue, the operation will just continue later on
            return Ok(None);
        }
        Err(err) => {
            if let Some(sender) = sender {
                network_bridge
                    .send(&sender, NetMessage::Aborted(tx_id))
                    .await?;
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
                network_bridge.send(&target.peer, msg).await?;
            }
            op_manager.push(id, updated_state).await?;
        }

        Ok(OperationResult {
            return_msg: None,
            state: Some(final_state),
        }) if final_state.finalized() => {
            // operation finished_completely with result
            op_manager.completed(tx_id);
            return Ok(Some(final_state));
        }
        Ok(OperationResult {
            return_msg: None,
            state: Some(updated_state),
        }) => {
            // interim state
            let id = *updated_state.id();
            op_manager.push(id, updated_state).await?;
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: None,
        }) => {
            op_manager.completed(tx_id);
            // finished the operation at this node, informing back
            if let Some(target) = msg.target().cloned() {
                network_bridge.send(&target.peer, msg).await?;
            }
        }
        Ok(OperationResult {
            return_msg: None,
            state: None,
        }) => {
            // operation finished_completely
            op_manager.completed(tx_id);
        }
    }
    Ok(None)
}

pub(crate) enum OpEnum {
    Connect(Box<connect::ConnectOp>),
    Put(put::PutOp),
    Get(get::GetOp),
    Subscribe(subscribe::SubscribeOp),
    Update(update::UpdateOp),
}

impl OpEnum {
    delegate::delegate! {
        to match self {
            OpEnum::Connect(op) => op,
            OpEnum::Put(op) => op,
            OpEnum::Get(op) => op,
            OpEnum::Subscribe(op) => op,
            OpEnum::Update(op) => op,
        } {
            pub fn id(&self) -> &Transaction;
            pub fn outcome(&self) -> OpOutcome;
            pub fn finalized(&self) -> bool;
            pub fn record_transfer(&mut self);
        }
    }

    pub fn to_host_result(&self, _client_id: ClientId) -> HostResult {
        todo!()
    }
}

macro_rules! try_from_op_enum {
    ($op_enum:path, $op_type:ty, $transaction_type:expr) => {
        impl TryFrom<OpEnum> for $op_type {
            type Error = OpError;

            fn try_from(value: OpEnum) -> Result<Self, Self::Error> {
                match value {
                    $op_enum(op) => Ok(op),
                    other => Err(OpError::IncorrectTxType(
                        $transaction_type,
                        other.id().transaction_type(),
                    )),
                }
            }
        }
    };
}

try_from_op_enum!(OpEnum::Put, put::PutOp, TransactionType::Put);
try_from_op_enum!(OpEnum::Get, get::GetOp, TransactionType::Get);
try_from_op_enum!(
    OpEnum::Subscribe,
    subscribe::SubscribeOp,
    TransactionType::Subscribe
);
try_from_op_enum!(OpEnum::Update, update::UpdateOp, TransactionType::Update);

pub(crate) enum OpOutcome<'a> {
    /// An op which involves a contract completed successfully.
    ContractOpSuccess {
        target_peer: &'a PeerKeyLocation,
        contract_location: Location,
        /// Time the operation took to initiate.
        first_response_time: Duration,
        /// Size of the payload (contract, state, etc.) in bytes.
        payload_size: usize,
        /// Transfer time of the payload.
        payload_transfer_time: Duration,
    },
    // todo: handle failures stats when it does not complete successfully
    // /// An op which involves a contract completed unsuccessfully.
    // ContractOpFailure {
    //     target_peer: Option<&'a PeerKeyLocation>,
    //     contract_location: Location,
    // },
    /// In transit contract operation.
    Incomplete,
    /// This operation stats are not relevant for this peer.
    Irrelevant,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error(transparent)]
    ConnError(#[from] ConnectionError),
    #[error(transparent)]
    RingError(#[from] RingError),
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    ExecutorError(DynError),

    #[error("unexpected operation state")]
    UnexpectedOpState,
    #[error("cannot perform a state transition from the current state with the provided input (tx: {tx})")]
    InvalidStateTransition {
        tx: Transaction,
        #[cfg(debug_assertions)]
        state: Option<Box<dyn std::fmt::Debug + Send + Sync>>,
        #[cfg(debug_assertions)]
        trace: StdTrace,
    },
    #[error("failed notifying, channel closed")]
    NotificationError,
    #[cfg(debug_assertions)]
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[error("op not present: {0}")]
    OpNotPresent(Transaction),
    #[error("max number of retries for tx {0} of op type `{1}` reached")]
    MaxRetriesExceeded(Transaction, TransactionType),
    #[error("op not available")]
    OpNotAvailable(#[from] OpNotAvailable),

    // user for control flow
    /// This is used as an early interrumpt of an op update when an op
    /// was sent throught the fast path back to the storage.
    #[error("early push of state into the op stack")]
    StatePushed,
}

impl OpError {
    pub fn invalid_transition(tx: Transaction) -> Self {
        Self::InvalidStateTransition {
            tx,
            #[cfg(debug_assertions)]
            state: None,
            #[cfg(debug_assertions)]
            trace: StdTrace::force_capture(),
        }
    }

    pub fn invalid_transition_with_state(
        tx: Transaction,
        state: Box<dyn std::fmt::Debug + Send + Sync>,
    ) -> Self {
        #[cfg(not(debug_assertions))]
        {
            let _ = state;
        }
        Self::InvalidStateTransition {
            tx,
            #[cfg(debug_assertions)]
            state: Some(state),
            #[cfg(debug_assertions)]
            trace: StdTrace::force_capture(),
        }
    }
}

impl<T> From<SendError<T>> for OpError {
    fn from(_: SendError<T>) -> OpError {
        OpError::NotificationError
    }
}

pub(crate) trait Operation
where
    Self: Sized + TryInto<Self::Result>,
{
    type Message: InnerMessage + std::fmt::Display;

    type Result;

    fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>>;

    fn id(&self) -> &Transaction;

    #[allow(clippy::type_complexity)]
    fn process_message<'a, CB: NetworkBridge>(
        self,
        conn_manager: &'a mut CB,
        op_manager: &'a OpManager,
        input: &'a Self::Message,
        // client_id: Option<ClientId>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>>;
}
