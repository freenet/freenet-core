#[cfg(debug_assertions)]
use std::backtrace::Backtrace as StdTrace;
use std::{pin::Pin, time::Duration};

use freenet_stdlib::prelude::ContractKey;
use futures::Future;
use tokio::sync::mpsc::error::SendError;

use crate::{
    client_events::HostResult,
    contract::{ContractError, ExecutorError},
    message::{InnerMessage, MessageStats, NetMessage, NetMessageV1, Transaction, TransactionType},
    node::{ConnectionError, NetworkBridge, OpManager, OpNotAvailable, PeerId},
    ring::{Location, PeerKeyLocation, RingError},
};

pub(crate) mod connect;
pub(crate) mod get;
pub(crate) mod put;
pub(crate) mod subscribe;
pub(crate) mod update;

pub(crate) trait Operation
where
    Self: Sized,
{
    type Message: InnerMessage + std::fmt::Display;

    type Result;

    fn load_or_init<'a>(
        op_manager: &'a OpManager,
        msg: &'a Self::Message,
    ) -> impl Future<Output = Result<OpInitialization<Self>, OpError>> + 'a;

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

#[inline(always)]
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
            tracing::debug!("entered in state pushed to continue with op");
            return Ok(None);
        }
        Err(err) => {
            if let Some(sender) = sender {
                network_bridge
                    .send(&sender, NetMessage::V1(NetMessageV1::Aborted(tx_id)))
                    .await?;
            }
            return Err(err);
        }
        Ok(OperationResult {
            return_msg: None,
            state: Some(final_state),
        }) if final_state.finalized() => {
            if op_manager.failed_parents.remove(&tx_id).is_some() {
                tracing::warn!(
                    "Operation {} reached finalized state after a sub-operation failure; dropping client response",
                    tx_id
                );
                op_manager.completed(tx_id);
                return Ok(None);
            }
            // Check if operation has pending sub-operations
            if op_manager.all_sub_operations_completed(tx_id) {
                // All sub-operations done, finalize immediately
                tracing::info!(
                    ">>> OPERATION COMPLETE | tx={} type={:?} | All sub-operations finished (if any)",
                    tx_id,
                    tx_id.transaction_type()
                );
                op_manager.completed(tx_id);
                return Ok(Some(final_state));
            } else {
                // Parent reached finished state but children still pending
                let pending_count = op_manager.count_pending_sub_operations(tx_id);
                tracing::warn!(
                    ">>> WAITING FOR SUB-OPS | parent={} type={:?} | Pending: {} sub-operation(s) | CLIENT RESPONSE BLOCKED",
                    tx_id,
                    tx_id.transaction_type(),
                    pending_count
                );

                // Store in pending_finalization map (move final_state)
                op_manager.pending_finalization.insert(tx_id, final_state);

                tracing::warn!(
                    ">>> CLIENT RESPONSE DEFERRED | tx={} | Operation in pending_finalization, will complete when sub-ops finish",
                    tx_id
                );

                // Don't send response to client yet - atomicity guarantee
                return Ok(None);
            }
        }
        Ok(OperationResult {
            return_msg: Some(msg),
            state: Some(updated_state),
        }) => {
            // Check if operation is finalized while sending a message (e.g., forwarding upstream)
            if updated_state.finalized() {
                // Operation is complete but needs to send a message
                let id = *msg.id();
                tracing::debug!(%id, "operation finalized with message to send");
                op_manager.completed(id);
                if let Some(target) = msg.target() {
                    tracing::debug!(%id, %target, "sending final message to target");
                    network_bridge.send(&target.peer, msg).await?;
                }
                return Ok(Some(updated_state));
            } else {
                // Normal case: operation in progress, send message and push back
                let id = *msg.id();
                tracing::debug!(%id, "updated op state");
                if let Some(target) = msg.target() {
                    tracing::debug!(%id, %target, "sending updated op state");
                    network_bridge.send(&target.peer, msg).await?;
                    op_manager.push(id, updated_state).await?;
                } else {
                    tracing::debug!(%id, "queueing op state for local processing");
                    debug_assert!(
                        matches!(
                            msg,
                            NetMessage::V1(NetMessageV1::Update(
                                crate::operations::update::UpdateMsg::Broadcasting { .. }
                            ))
                        ),
                        "Only Update::Broadcasting messages should be re-queued locally"
                    );
                    op_manager.notify_op_change(msg, updated_state).await?;
                    return Err(OpError::StatePushed);
                }
            }
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

            if let Some(target) = msg.target() {
                tracing::debug!(%tx_id, target=%target.peer, "sending back message to target");
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
            pub fn outcome(&self) -> OpOutcome<'_>;
            pub fn finalized(&self) -> bool;
            pub fn to_host_result(&self) -> HostResult;
        }
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
    ExecutorError(#[from] ExecutorError),

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
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[error("op not present: {0}")]
    OpNotPresent(Transaction),
    #[error("max number of retries for tx {0} of op type `{1}` reached")]
    MaxRetriesExceeded(Transaction, TransactionType),
    #[error("op not available")]
    OpNotAvailable(#[from] OpNotAvailable),

    // used for control flow
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

/// Start a subscription request as a sub-operation of a parent transaction.
/// This version registers the sub-operation relationship for atomicity tracking and
/// spawns the subscription task in the background to prevent race conditions.
/// Returns the child transaction ID immediately.
fn start_subscription_request(
    op_manager: &OpManager,
    parent_tx: Transaction,
    key: ContractKey,
) -> Transaction {
    // CRITICAL: Register expected sub-operation BEFORE creating child to prevent race condition
    // where child completes before parent reaches finalized state
    op_manager.expect_sub_operation(parent_tx);

    // Create child transaction linked to parent
    let child_tx = Transaction::new_child_of::<subscribe::SubscribeMsg>(&parent_tx);

    // Register parent-child relationship
    op_manager.register_sub_operation(parent_tx, child_tx);

    tracing::warn!(
        ">>> SUB-OP CREATED | parent={} ({:?}) → child={} (SUBSCRIBE) | contract={} | parent_of registered",
        parent_tx,
        parent_tx.transaction_type(),
        child_tx,
        key
    );

    // Verify registration immediately
    if op_manager.is_sub_operation(child_tx) {
        tracing::debug!("✓ parent_of mapping verified for child={}", child_tx);
    } else {
        tracing::error!(
            "✗ parent_of mapping MISSING immediately after registration for child={}",
            child_tx
        );
    }

    // Clone op_manager for background task (all fields are Arc, so this is cheap)
    let op_manager_cloned = op_manager.clone();

    // Spawn subscription task in background to allow parent to proceed to finalization
    tokio::spawn(async move {
        // Yield to allow parent operation to reach finalized state before child completes
        // This prevents race condition where child completes before parent enters pending_finalization
        tokio::task::yield_now().await;

        // Create subscription operation with child transaction
        let sub_op = subscribe::start_op_with_id(key, child_tx);

        // Execute subscription
        match subscribe::request_subscribe(&op_manager_cloned, sub_op).await {
            Ok(_) => {
                tracing::debug!(
                    child_tx = %child_tx,
                    parent_tx = %parent_tx,
                    "subscription sub-operation completed successfully"
                );
            }
            Err(error) => {
                tracing::error!(
                    parent_tx = %parent_tx,
                    child_tx = %child_tx,
                    error = %error,
                    "subscription sub-operation failed"
                );

                // Propagate failure to parent
                let error_msg = format!("{}", error);
                if let Err(e) = op_manager_cloned
                    .sub_operation_failed(child_tx, &error_msg)
                    .await
                {
                    tracing::error!(
                        parent_tx = %parent_tx,
                        child_tx = %child_tx,
                        error = %e,
                        "failed to propagate sub-operation failure to parent"
                    );
                }
            }
        }
    });

    child_tx
}

async fn has_contract(op_manager: &OpManager, key: ContractKey) -> Result<bool, OpError> {
    match op_manager
        .notify_contract_handler(crate::contract::ContractHandlerEvent::GetQuery {
            key,
            return_contract_code: false,
        })
        .await?
    {
        crate::contract::ContractHandlerEvent::GetResponse {
            response: Ok(crate::contract::StoreResponse { state: Some(_), .. }),
            ..
        } => Ok(true),
        _ => Ok(false),
    }
}
