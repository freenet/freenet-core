//! Executes WASM contract and delegate code within a sandboxed environment (`WasmRuntime`).
//! Communicates with the `ContractHandler` and potentially the `OpManager` (via `ExecutorToEventLoopChannel`).
//! See `architecture.md`.

use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::Stream;

use either::Either;
use freenet_stdlib::client_api::{
    ClientError as WsClientError, ClientRequest, ContractError as StdContractError,
    ContractRequest, ContractResponse, DelegateError as StdDelegateError, DelegateRequest,
    HostResponse::{self, DelegateResponse},
    RequestError,
};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use super::storages::Storage;
use crate::config::Config;
use crate::message::Transaction;
use crate::node::OpManager;
use crate::operations::get::GetResult;
use crate::operations::{OpEnum, OpError};
use crate::wasm_runtime::{
    ContractExecError, ContractRuntimeInterface, ContractStore, DelegateRuntimeInterface,
    DelegateStore, Runtime, SecretsStore, StateStore, StateStoreError,
};
use crate::{
    client_events::{ClientId, HostResult},
    operations::{self, Operation},
};

pub(super) mod init_tracker;
pub(super) mod mock_runtime;
#[cfg(test)]
mod pool_tests;
pub(super) mod runtime;

pub(crate) use init_tracker::{
    ContractInitTracker, InitCheckResult, SLOW_INIT_THRESHOLD, STALE_INIT_THRESHOLD,
};
pub(crate) use runtime::RuntimePool;

/// Type alias for the channel used to send operation requests from executors to the event loop.
/// Each request includes a transaction ID and a oneshot sender for the response.
/// This sender is cloneable, allowing multiple executors to share access to the event loop.
pub(crate) type OpRequestSender =
    mpsc::Sender<(Transaction, oneshot::Sender<Result<OpEnum, OpRequestError>>)>;

/// Type alias for the receiver side of the operation request channel.
/// This is held by the event loop to receive requests from all executors.
pub(crate) type OpRequestReceiver =
    mpsc::Receiver<(Transaction, oneshot::Sender<Result<OpEnum, OpRequestError>>)>;

/// Create a channel pair for operation requests from executors to the event loop.
///
/// Returns:
/// - `OpRequestReceiver`: Held by the event loop to receive operation requests
/// - `OpRequestSender`: Cloneable sender given to executors to send requests
pub(crate) fn op_request_channel() -> (OpRequestReceiver, OpRequestSender) {
    // Buffer size matches the old executor_channel for consistency
    let (tx, rx) = mpsc::channel(1000);
    tracing::debug!(buffer_size = 1000, "Created op_request channel");
    (rx, tx)
}

/// Error type for operation requests that can be sent over channels.
#[derive(Debug, Clone)]
pub enum OpRequestError {
    /// The operation failed with an error message
    Failed(String),
    /// The channel was closed before a response was received
    ChannelClosed,
}

impl std::fmt::Display for OpRequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpRequestError::Failed(msg) => write!(f, "Operation failed: {}", msg),
            OpRequestError::ChannelClosed => write!(f, "Channel closed"),
        }
    }
}

impl std::error::Error for OpRequestError {}

#[derive(Debug)]
pub struct ExecutorError {
    inner: Either<Box<RequestError>, anyhow::Error>,
    fatal: bool,
}

enum InnerOpError {
    Upsert(ContractKey),
    Delegate(DelegateKey),
}

impl std::error::Error for ExecutorError {}

impl ExecutorError {
    pub fn other(error: impl Into<anyhow::Error>) -> Self {
        Self {
            inner: Either::Right(error.into()),
            fatal: false,
        }
    }

    /// Call this when an unreachable path is reached but need to avoid panics.
    fn internal_error() -> Self {
        Self {
            inner: Either::Right(anyhow::anyhow!("internal error")),
            fatal: false,
        }
    }

    fn request(error: impl Into<RequestError>) -> Self {
        Self {
            inner: Either::Left(Box::new(error.into())),
            fatal: false,
        }
    }

    fn execution(
        outer_error: crate::wasm_runtime::ContractError,
        op: Option<InnerOpError>,
    ) -> Self {
        use crate::wasm_runtime::RuntimeInnerError;
        let error = outer_error.deref();

        let mut fatal = false;
        if let RuntimeInnerError::ContractExecError(e) = error {
            if matches!(e, ContractExecError::MaxComputeTimeExceeded) {
                fatal = true;
            }
            if let Some(InnerOpError::Upsert(key)) = &op {
                return ExecutorError::request(StdContractError::update_exec_error(*key, e));
            }
        }

        if let RuntimeInnerError::DelegateNotFound(key) = error {
            return ExecutorError::request(StdDelegateError::Missing(key.clone()));
        }

        if let RuntimeInnerError::DelegateExecError(e) = error {
            return ExecutorError::request(StdDelegateError::ExecutionError(format!("{e}").into()));
        }

        if let (
            RuntimeInnerError::SecretStoreError(
                crate::wasm_runtime::SecretStoreError::MissingSecret(secret),
            ),
            Some(InnerOpError::Delegate(key)),
        ) = (error, &op)
        {
            return ExecutorError::request(StdDelegateError::MissingSecret {
                key: key.clone(),
                secret: secret.clone(),
            });
        }

        match error {
            RuntimeInnerError::WasmCompileError(e) => match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e))
                }
                _ => return ExecutorError::other(anyhow::anyhow!("execution error: {e}")),
            },
            RuntimeInnerError::WasmExportError(e) => match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e))
                }
                _ => return ExecutorError::other(anyhow::anyhow!("execution error: {e}")),
            },
            RuntimeInnerError::WasmInstantiationError(e) => match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e))
                }
                _ => return ExecutorError::other(anyhow::anyhow!("execution error: {e}")),
            },
            _ => {}
        }

        let mut err = ExecutorError::other(outer_error);
        err.fatal = fatal;
        err
    }

    pub fn is_request(&self) -> bool {
        matches!(self.inner, Either::Left(_))
    }

    pub fn is_fatal(&self) -> bool {
        self.fatal
    }

    pub fn unwrap_request(self) -> RequestError {
        match self.inner {
            Either::Left(err) => *err,
            Either::Right(_) => panic!(),
        }
    }
}

impl From<RequestError> for ExecutorError {
    fn from(value: RequestError) -> Self {
        Self {
            inner: Either::Left(Box::new(value)),
            fatal: false,
        }
    }
}

impl Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            Either::Left(l) => write!(f, "{}", &**l),
            Either::Right(r) => write!(f, "{}", &**r),
        }
    }
}

impl From<Box<RequestError>> for ExecutorError {
    fn from(value: Box<RequestError>) -> Self {
        Self {
            inner: Either::Left(value),
            fatal: false,
        }
    }
}

type Response = Result<HostResponse, ExecutorError>;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationMode {
    /// Run the node in local-only mode. Useful for development purposes.
    Local,
    /// Standard operation mode.
    Network,
}

impl Display for OperationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationMode::Local => write!(f, "local"),
            OperationMode::Network => write!(f, "network"),
        }
    }
}

pub struct ExecutorToEventLoopChannel<End: sealed::ChannelHalve> {
    op_manager: Arc<OpManager>,
    end: End,
}

/// Creates channels for the mediator that bridges between the new OpRequest channel
/// (used by pooled executors) and the old ExecutorToEventLoop channel (used by the event loop).
///
/// Returns:
/// - `ExecutorToEventLoopChannel<NetworkEventListenerHalve>`: For the event loop
/// - `mpsc::Sender<Transaction>`: For the mediator to send transactions to the event loop
/// - `mpsc::Receiver<OpEnum>`: For the mediator to receive responses from the event loop
pub(crate) fn mediator_channels(
    op_manager: Arc<OpManager>,
) -> (
    ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    mpsc::Sender<Transaction>,
    mpsc::Receiver<OpEnum>,
) {
    let (waiting_for_op_tx, waiting_for_op_rx) = mpsc::channel(1000);
    let (response_for_tx, response_for_rx) = mpsc::channel(1000);

    tracing::debug!(buffer_size = 1000, "Created mediator channels");

    let listener_halve = ExecutorToEventLoopChannel {
        op_manager,
        end: NetworkEventListenerHalve {
            waiting_for_op_rx,
            response_for_tx,
        },
    };

    (listener_halve, waiting_for_op_tx, response_for_rx)
}

/// Maximum number of pending requests before the mediator starts rejecting new ones.
/// This prevents unbounded memory growth if the event loop is slow or unresponsive.
const MAX_PENDING_REQUESTS: usize = 10_000;

/// How long to wait before cleaning up stale pending requests.
/// This should be longer than OP_REQUEST_TIMEOUT to allow normal timeout handling.
const STALE_REQUEST_THRESHOLD: Duration = Duration::from_secs(180);

/// How often to run the stale request cleanup.
const CLEANUP_INTERVAL: Duration = Duration::from_secs(30);

/// Entry in the pending responses map, tracking when the request was added.
struct PendingRequest {
    response_tx: oneshot::Sender<Result<OpEnum, OpRequestError>>,
    created_at: Instant,
}

/// Mediator task that bridges between the new `OpRequestReceiver` (from pooled executors)
/// and the old event loop channels.
///
/// This allows multiple executors to share access to the event loop through a cloneable
/// `OpRequestSender`, while the event loop continues to use the existing
/// `ExecutorToEventLoopChannel<NetworkEventListenerHalve>` interface.
///
/// # Architecture
///
/// ```text
/// ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
/// │  Executor 1  │────┐    │              │    ┌────│  Event Loop  │
/// ├──────────────┤    │    │   Mediator   │    │    ├──────────────┤
/// │  Executor 2  │────┼───▶│              │────┼───▶│  Operations  │
/// ├──────────────┤    │    │  (pending    │    │    │  Processing  │
/// │  Executor N  │────┘    │   HashMap)   │◀───┘    └──────────────┘
/// └──────────────┘         └──────────────┘
/// ```
///
/// # Workflow
///
/// 1. Receives (Transaction, oneshot::Sender) from executors via `op_request_receiver`
/// 2. Forwards the Transaction to the event loop via `to_event_loop_tx`
/// 3. Stores the oneshot sender keyed by transaction in `pending_responses`
/// 4. Receives responses from the event loop via `from_event_loop_rx`
/// 5. Routes responses back to the correct executor via the stored oneshot sender
/// 6. Periodically cleans up stale pending requests to prevent memory leaks
///
/// # Failure Scenarios and Recovery
///
/// ## Executor Drops Before Response
///
/// **Scenario**: An executor times out or is dropped before receiving its response.
/// **Detection**: The `oneshot::Sender::send()` returns `Err` when the receiver is dropped.
/// **Recovery**: The mediator logs a debug message and continues. No cleanup needed since
/// the entry is removed from `pending_responses` when the response arrives.
///
/// ## Event Loop Channel Closes
///
/// **Scenario**: The event loop crashes or its receiving channel is dropped.
/// **Detection**: `to_event_loop_tx.send()` returns `SendError`.
/// **Recovery**: The mediator removes the pending request and notifies the executor
/// with `OpRequestError::ChannelClosed`. The mediator continues running to handle
/// cleanup of remaining pending requests.
///
/// ## Mediator Capacity Exceeded
///
/// **Scenario**: More than `MAX_PENDING_REQUESTS` (10,000) concurrent requests.
/// **Detection**: `pending_responses.len() >= MAX_PENDING_REQUESTS`
/// **Recovery**: New requests are immediately rejected with an error. This provides
/// backpressure to prevent unbounded memory growth. Existing requests continue processing.
///
/// ## Stale Request Cleanup
///
/// **Scenario**: Requests that have been pending longer than `STALE_REQUEST_THRESHOLD` (180s).
/// **Detection**: Periodic cleanup runs every `CLEANUP_INTERVAL` (30s) and checks timestamps.
/// **Recovery**: Stale entries are removed from `pending_responses` and their executors are
/// notified with an error. This handles edge cases where responses are never received
/// (e.g., network partitions, event loop bugs).
///
/// ## Unknown Transaction Response
///
/// **Scenario**: Response received for a transaction not in `pending_responses`.
/// **Detection**: `pending_responses.remove()` returns `None`.
/// **Recovery**: The mediator logs a warning. This can happen legitimately when an executor
/// times out and its response arrives later. No action needed.
///
/// ## All Channels Close
///
/// **Scenario**: Both the executor channel and event loop channel are closed.
/// **Detection**: `tokio::select!` returns from the `else` branch.
/// **Recovery**: The mediator notifies all remaining pending executors with `ChannelClosed`
/// error, then exits gracefully.
///
/// # Thread Safety
///
/// The mediator is designed to be run as a single task. It is not `Sync` because
/// it holds mutable state (`pending_responses`). The `OpRequestSender` is cloneable
/// and can be shared across multiple executor tasks.
pub(crate) async fn run_op_request_mediator(
    mut op_request_receiver: OpRequestReceiver,
    to_event_loop_tx: mpsc::Sender<Transaction>,
    mut from_event_loop_rx: mpsc::Receiver<OpEnum>,
) {
    use std::collections::HashMap;

    let mut pending_responses: HashMap<Transaction, PendingRequest> = HashMap::new();
    let mut cleanup_interval = tokio::time::interval(CLEANUP_INTERVAL);
    cleanup_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!("Op request mediator starting");

    loop {
        tokio::select! {
            // Receive new operation requests from executors
            Some((transaction, response_tx)) = op_request_receiver.recv() => {
                tracing::trace!(
                    tx = %transaction,
                    pending_count = pending_responses.len(),
                    "Mediator received operation request"
                );

                // Check if we're at capacity
                if pending_responses.len() >= MAX_PENDING_REQUESTS {
                    tracing::warn!(
                        tx = %transaction,
                        max = MAX_PENDING_REQUESTS,
                        "Mediator at capacity, rejecting request"
                    );
                    let _ = response_tx.send(Err(OpRequestError::Failed(
                        "mediator at capacity".to_string()
                    )));
                    continue;
                }

                // Store the response channel with timestamp
                pending_responses.insert(transaction, PendingRequest {
                    response_tx,
                    created_at: Instant::now(),
                });

                // Forward transaction to event loop
                if let Err(e) = to_event_loop_tx.send(transaction).await {
                    tracing::error!(
                        tx = %transaction,
                        error = %e,
                        "Failed to forward transaction to event loop - channel closed"
                    );
                    // Remove and notify the waiting executor
                    if let Some(pending) = pending_responses.remove(&transaction) {
                        let _ = pending.response_tx.send(Err(OpRequestError::ChannelClosed));
                    }
                }
            }

            // Receive responses from event loop
            Some(op_result) = from_event_loop_rx.recv() => {
                let transaction = *op_result.id();
                tracing::trace!(
                    tx = %transaction,
                    pending_count = pending_responses.len(),
                    "Mediator received response from event loop"
                );

                // Route response to the waiting executor
                if let Some(pending) = pending_responses.remove(&transaction) {
                    if pending.response_tx.send(Ok(op_result)).is_err() {
                        tracing::debug!(
                            tx = %transaction,
                            "Executor dropped before receiving response"
                        );
                    }
                } else {
                    tracing::warn!(
                        tx = %transaction,
                        "Received response for unknown transaction - executor may have timed out"
                    );
                }
            }

            // Periodic cleanup of stale requests
            _ = cleanup_interval.tick() => {
                let now = Instant::now();
                let stale_threshold = STALE_REQUEST_THRESHOLD;

                // Collect stale transaction IDs
                let stale_txs: Vec<Transaction> = pending_responses
                    .iter()
                    .filter(|(_, pending)| now.duration_since(pending.created_at) > stale_threshold)
                    .map(|(tx, _)| *tx)
                    .collect();

                if !stale_txs.is_empty() {
                    tracing::warn!(
                        stale_count = stale_txs.len(),
                        pending_count = pending_responses.len(),
                        threshold_secs = stale_threshold.as_secs(),
                        "Cleaning up stale pending requests"
                    );

                    for tx in stale_txs {
                        if let Some(pending) = pending_responses.remove(&tx) {
                            tracing::debug!(
                                tx = %tx,
                                age_secs = now.duration_since(pending.created_at).as_secs(),
                                "Removing stale pending request"
                            );
                            // Try to notify the executor (likely already timed out)
                            let _ = pending.response_tx.send(Err(OpRequestError::Failed(
                                "request exceeded stale threshold".to_string()
                            )));
                        }
                    }
                }
            }

            // Both channels closed - exit
            else => {
                tracing::info!(
                    pending_count = pending_responses.len(),
                    "Mediator channels closed, shutting down"
                );
                // Notify any remaining waiters
                for (tx, pending) in pending_responses.drain() {
                    tracing::debug!(tx = %tx, "Notifying orphaned waiter of shutdown");
                    let _ = pending.response_tx.send(Err(OpRequestError::ChannelClosed));
                }
                break;
            }
        }
    }

    tracing::info!("Op request mediator stopped");
}

impl ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    pub async fn transaction_from_executor(&mut self) -> anyhow::Result<Transaction> {
        tracing::trace!("Waiting to receive transaction from executor channel");
        let tx = self.end.waiting_for_op_rx.recv().await.ok_or_else(|| {
            tracing::error!(
                phase = "channel_closed",
                "Executor channel closed - all senders dropped. Possible causes: 1) executor task panic, 2) network timeout cascade, 3) resource constraints"
            );
            anyhow::anyhow!("channel closed")
        })?;
        tracing::trace!(
            tx = %tx,
            "Successfully received transaction from executor channel"
        );
        Ok(tx)
    }

    pub(crate) fn callback(&self) -> ExecutorToEventLoopChannel<Callback> {
        ExecutorToEventLoopChannel {
            op_manager: self.op_manager.clone(),
            end: Callback {
                response_for_tx: self.end.response_for_tx.clone(),
            },
        }
    }
}

impl Stream for ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    type Item = Transaction;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.end.waiting_for_op_rx).poll_recv(cx)
    }
}

impl ExecutorToEventLoopChannel<Callback> {
    pub async fn response(&mut self, result: OpEnum) {
        let tx_id = *result.id();
        if self.end.response_for_tx.send(result).await.is_err() {
            tracing::debug!(
                tx = %tx_id,
                "Failed to send response to executor - channel closed"
            );
        }
    }
}

pub(crate) struct Callback {
    /// sends the callback response to the executor
    response_for_tx: mpsc::Sender<OpEnum>,
}

pub(crate) struct NetworkEventListenerHalve {
    /// this is the receiver end of the Executor halve, which will be sent from the executor
    /// when a callback is expected for a given transaction
    waiting_for_op_rx: mpsc::Receiver<Transaction>,
    /// this is the sender end of the Executor halve receiver, which will communicate
    /// back responses to the executor, it's cloned each tiome a new callback halve is created
    response_for_tx: mpsc::Sender<OpEnum>,
}

mod sealed {
    use super::{Callback, NetworkEventListenerHalve};
    pub trait ChannelHalve {}
    impl ChannelHalve for NetworkEventListenerHalve {}
    impl ChannelHalve for Callback {}
}

trait ComposeNetworkMessage<Op>
where
    Self: Sized,
    Op: Operation + Send + 'static,
{
    fn initiate_op(self, op_manager: &OpManager) -> Op;

    fn resume_op(
        op: Op,
        op_manager: &OpManager,
    ) -> impl Future<Output = Result<(), OpError>> + Send;
}

#[allow(unused)]
struct GetContract {
    instance_id: ContractInstanceId,
    return_contract_code: bool,
}

impl ComposeNetworkMessage<operations::get::GetOp> for GetContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::get::GetOp {
        operations::get::start_op(self.instance_id, self.return_contract_code, false)
    }

    async fn resume_op(op: operations::get::GetOp, op_manager: &OpManager) -> Result<(), OpError> {
        let visited = operations::VisitedPeers::new(&op.id);
        operations::get::request_get(op_manager, op, visited).await
    }
}

#[allow(unused)]
struct SubscribeContract {
    instance_id: ContractInstanceId,
}

impl ComposeNetworkMessage<operations::subscribe::SubscribeOp> for SubscribeContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::subscribe::SubscribeOp {
        operations::subscribe::start_op(self.instance_id)
    }

    async fn resume_op(
        op: operations::subscribe::SubscribeOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::subscribe::request_subscribe(op_manager, op).await
    }
}

struct UpdateContract {
    key: ContractKey,
    new_state: WrappedState,
}

#[derive(Debug)]
pub(crate) enum UpsertResult {
    NoChange,
    Updated(WrappedState),
}

impl ComposeNetworkMessage<operations::update::UpdateOp> for UpdateContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::update::UpdateOp {
        let UpdateContract { key, new_state } = self;
        let related_contracts = RelatedContracts::default();
        // Wrap the computed state as UpdateData::State for the update operation.
        // The executor computes state without committing, expecting update_contract
        // to handle persistence and change detection.
        let update_data = freenet_stdlib::prelude::UpdateData::State(
            freenet_stdlib::prelude::State::from(new_state),
        );
        operations::update::start_op(key, update_data, related_contracts)
    }

    async fn resume_op(
        op: operations::update::UpdateOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::update::request_update(op_manager, op).await
    }
}

pub(crate) trait ContractExecutor: Send + 'static {
    /// Look up the full ContractKey from a ContractInstanceId.
    /// Returns None if the contract is not known to this node.
    fn lookup_key(&self, instance_id: &ContractInstanceId) -> Option<ContractKey>;

    fn fetch_contract(
        &mut self,
        key: ContractKey,
        return_contract_code: bool,
    ) -> impl Future<Output = Result<(Option<WrappedState>, Option<ContractContainer>), ExecutorError>>
           + Send;

    fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = Result<UpsertResult, ExecutorError>> + Send;

    fn register_contract_notifier(
        &mut self,
        key: ContractInstanceId,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>>;

    fn execute_delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attested_contract: Option<&ContractInstanceId>,
    ) -> Response;

    fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo>;
}

/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor<R = Runtime> {
    mode: OperationMode,
    runtime: R,
    pub state_store: StateStore<Storage>,
    /// Notification channels for any clients subscribed to updates for a given contract.
    update_notifications:
        HashMap<ContractInstanceId, Vec<(ClientId, mpsc::UnboundedSender<HostResult>)>>,
    /// Summaries of the state of all clients subscribed to a given contract.
    subscriber_summaries:
        HashMap<ContractInstanceId, HashMap<ClientId, Option<StateSummary<'static>>>>,
    /// Attested contract instances for a given delegate.
    delegate_attested_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,
    /// Tracks contracts that are being initialized and operations queued for them
    init_tracker: ContractInitTracker,

    /// Channel to send operation requests to the event loop (cloneable).
    op_sender: Option<OpRequestSender>,
    /// Reference to the operation manager for initiating operations.
    op_manager: Option<Arc<OpManager>>,
}

impl<R> Executor<R> {
    /// Create a new Executor with optional network operation support.
    /// This is `pub(crate)` because the parameters involve crate-internal types.
    pub(crate) async fn new(
        state_store: StateStore<Storage>,
        ctrl_handler: impl FnOnce() -> anyhow::Result<()>,
        mode: OperationMode,
        runtime: R,
        op_sender: Option<OpRequestSender>,
        op_manager: Option<Arc<OpManager>>,
    ) -> anyhow::Result<Self> {
        ctrl_handler()?;

        Ok(Self {
            mode,
            runtime,
            state_store,
            update_notifications: HashMap::default(),
            subscriber_summaries: HashMap::default(),
            delegate_attested_ids: HashMap::default(),
            init_tracker: ContractInitTracker::new(),
            op_sender,
            op_manager,
        })
    }

    pub fn test_data_dir(identifier: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "freenet-executor-{identifier}-{}-{unique_id}",
            std::process::id()
        ))
    }

    /// Create all stores including StateStore. Used when creating a standalone executor.
    pub(crate) async fn get_stores(
        config: &Config,
    ) -> Result<
        (
            ContractStore,
            DelegateStore,
            SecretsStore,
            StateStore<Storage>,
        ),
        anyhow::Error,
    > {
        const MAX_MEM_CACHE: u32 = 10_000_000;

        let state_store =
            StateStore::new(Storage::new(&config.db_dir()).await?, MAX_MEM_CACHE).unwrap();
        let (contract_store, delegate_store, secret_store) = Self::get_runtime_stores(config)?;

        Ok((contract_store, delegate_store, secret_store, state_store))
    }

    /// Create only the Runtime stores (contract, delegate, secrets) without StateStore.
    /// Used by RuntimePool to create executors that share a StateStore.
    pub(crate) fn get_runtime_stores(
        config: &Config,
    ) -> Result<(ContractStore, DelegateStore, SecretsStore), anyhow::Error> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;

        let contract_store = ContractStore::new(config.contracts_dir(), MAX_SIZE)?;
        let delegate_store = DelegateStore::new(config.delegates_dir(), MAX_SIZE)?;
        let secret_store = SecretsStore::new(config.secrets_dir(), config.secrets.clone())?;

        Ok((contract_store, delegate_store, secret_store))
    }

    async fn op_request<Op, M>(&mut self, request: M) -> Result<Op::Result, ExecutorError>
    where
        Op: Operation + Send + TryFrom<OpEnum, Error = OpError> + 'static,
        <Op as Operation>::Result: TryFrom<Op, Error = OpError>,
        M: ComposeNetworkMessage<Op>,
    {
        let (op_sender, op_manager) = match (&self.op_sender, &self.op_manager) {
            (Some(sender), Some(manager)) => (sender.clone(), manager.clone()),
            _ => {
                return Err(ExecutorError::other(anyhow::anyhow!(
                    "missing op_sender or op_manager"
                )));
            }
        };

        // Create the operation and get its transaction ID
        let op = request.initiate_op(&op_manager);
        let transaction = *op.id();

        // Create a oneshot channel for this specific request's response
        let (response_tx, response_rx) = oneshot::channel();

        // Send the transaction and response channel to the event loop
        op_sender
            .send((transaction, response_tx))
            .await
            .map_err(|_| ExecutorError::other(anyhow::anyhow!("event loop channel closed")))?;

        // Start the network operation
        <M as ComposeNetworkMessage<Op>>::resume_op(op, &op_manager)
            .await
            .map_err(|e| {
                tracing::debug!(
                    tx = %transaction,
                    error = %e,
                    "Failed to resume operation"
                );
                ExecutorError::other(e)
            })?;

        // Wait for the response on our oneshot channel with timeout
        const OP_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
        let op_result = tokio::time::timeout(OP_REQUEST_TIMEOUT, response_rx)
            .await
            .map_err(|_| {
                tracing::warn!(
                    tx = %transaction,
                    timeout_secs = OP_REQUEST_TIMEOUT.as_secs(),
                    "Network operation timed out waiting for response"
                );
                ExecutorError::other(anyhow::anyhow!(
                    "network operation timed out after {} seconds",
                    OP_REQUEST_TIMEOUT.as_secs()
                ))
            })?
            .map_err(|_| {
                ExecutorError::other(anyhow::anyhow!(
                    "response channel closed before receiving result"
                ))
            })?;

        // Handle the result
        let op_enum = op_result.map_err(|e| ExecutorError::other(anyhow::anyhow!("{}", e)))?;

        // Convert to the specific operation type
        let op: Op = op_enum.try_into().map_err(|err: OpError| {
            tracing::error!(
                tx = %transaction,
                error = %err,
                "Expected message of one type but got another"
            );
            ExecutorError::other(err)
        })?;

        // Convert to the result type
        let result = <Op::Result>::try_from(op).map_err(|err| {
            tracing::debug!(
                tx = %transaction,
                error = %err,
                "Failed to convert operation result"
            );
            ExecutorError::other(err)
        })?;

        Ok(result)
    }

    pub fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        let mut subscriptions = Vec::new();
        for (instance_id, client_list) in &self.update_notifications {
            for (client_id, _channel) in client_list {
                subscriptions.push(crate::message::SubscriptionInfo {
                    instance_id: *instance_id,
                    client_id: *client_id,
                    last_update: None,
                });
            }
        }
        subscriptions
    }
}

/// Test fixtures for creating contract-related test data.
///
/// These helpers make it easier to write unit tests for contract module code
/// by providing convenient constructors for common types.
#[cfg(test)]
pub(crate) mod test_fixtures {
    use freenet_stdlib::prelude::*;

    /// Create a test contract key with arbitrary but consistent data
    pub fn make_contract_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let params = Parameters::from(vec![10, 20, 30, 40]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// Create a test contract key with custom code bytes
    pub fn make_contract_key_with_code(code_bytes: &[u8]) -> ContractKey {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![10, 20, 30, 40]);
        ContractKey::from_params_and_code(&params, &code)
    }

    /// Create a test wrapped state from raw bytes
    pub fn make_state(data: &[u8]) -> WrappedState {
        WrappedState::new(data.to_vec())
    }

    /// Create test parameters from raw bytes
    pub fn make_params(data: &[u8]) -> Parameters<'static> {
        Parameters::from(data.to_vec())
    }

    /// Create a test state delta from raw bytes
    pub fn make_delta(data: &[u8]) -> StateDelta<'static> {
        StateDelta::from(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod executor_error_tests {
        use super::*;

        #[test]
        fn test_executor_error_other_is_not_request() {
            let err = ExecutorError::other(anyhow::anyhow!("some error"));
            assert!(!err.is_request());
            assert!(!err.is_fatal());
        }

        #[test]
        fn test_executor_error_request_is_request() {
            let err = ExecutorError::request(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "test".into(),
            });
            assert!(err.is_request());
            assert!(!err.is_fatal());
        }

        #[test]
        fn test_executor_error_internal_error() {
            let err = ExecutorError::internal_error();
            assert!(!err.is_request());
            assert!(!err.is_fatal());
            assert!(err.to_string().contains("internal error"));
        }

        #[test]
        fn test_executor_error_display_left() {
            let err = ExecutorError::request(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "test cause".into(),
            });
            let display = err.to_string();
            assert!(display.contains("test cause") || display.contains("Put"));
        }

        #[test]
        fn test_executor_error_display_right() {
            let err = ExecutorError::other(anyhow::anyhow!("custom error message"));
            assert!(err.to_string().contains("custom error message"));
        }

        #[test]
        fn test_executor_error_from_request_error() {
            let request_err = RequestError::ContractError(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "from conversion".into(),
            });
            let err: ExecutorError = request_err.into();
            assert!(err.is_request());
        }

        #[test]
        fn test_executor_error_from_boxed_request_error() {
            let request_err = Box::new(RequestError::ContractError(StdContractError::Put {
                key: test_fixtures::make_contract_key(),
                cause: "boxed".into(),
            }));
            let err: ExecutorError = request_err.into();
            assert!(err.is_request());
        }

        #[test]
        fn test_unwrap_request_succeeds_for_request_error() {
            let key = test_fixtures::make_contract_key();
            let err = ExecutorError::request(StdContractError::Put {
                key,
                cause: "unwrap test".into(),
            });
            let _unwrapped = err.unwrap_request(); // Should not panic
        }

        #[test]
        #[should_panic]
        fn test_unwrap_request_panics_for_other_error() {
            let err = ExecutorError::other(anyhow::anyhow!("not a request"));
            let _unwrapped = err.unwrap_request(); // Should panic
        }
    }

    mod test_fixtures_tests {
        use super::*;

        #[test]
        fn test_make_contract_key_is_consistent() {
            let key1 = test_fixtures::make_contract_key();
            let key2 = test_fixtures::make_contract_key();
            assert_eq!(key1, key2);
        }

        #[test]
        fn test_make_contract_key_with_different_code() {
            let key1 = test_fixtures::make_contract_key_with_code(&[1, 2, 3]);
            let key2 = test_fixtures::make_contract_key_with_code(&[4, 5, 6]);
            assert_ne!(key1, key2);
        }

        #[test]
        fn test_make_state() {
            let state = test_fixtures::make_state(&[1, 2, 3, 4]);
            assert_eq!(state.as_ref(), &[1, 2, 3, 4]);
        }

        #[test]
        fn test_make_params() {
            let params = test_fixtures::make_params(&[10, 20]);
            assert_eq!(params.as_ref(), &[10, 20]);
        }

        #[test]
        fn test_make_delta() {
            let delta = test_fixtures::make_delta(&[100, 200]);
            assert_eq!(delta.as_ref(), &[100, 200]);
        }
    }
}
