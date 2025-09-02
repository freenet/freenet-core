//! Executes WASM contract and delegate code within a sandboxed environment (`WasmRuntime`).
//! Communicates with the `ContractHandler` and potentially the `OpManager` (via `ExecutorToEventLoopChannel`).
//! See `architecture.md`.

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blake3::traits::digest::generic_array::GenericArray;
use either::Either;
use freenet_stdlib::client_api::{
    ClientError as WsClientError, ClientRequest, ContractError as StdContractError,
    ContractRequest, ContractResponse, DelegateError as StdDelegateError, DelegateRequest,
    HostResponse::{self, DelegateResponse},
    RequestError,
};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self};

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

pub(super) mod mock_runtime;
pub(super) mod runtime;

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

pub(crate) fn executor_channel(
    op_manager: Arc<OpManager>,
) -> (
    ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    ExecutorToEventLoopChannel<ExecutorHalve>,
) {
    // Channel buffer size: In CI environments with limited CPU cores (2 vs 8+ locally),
    // task scheduling can be severely constrained. A larger buffer (1000 vs 10) provides
    // breathing room for the consumer when the scheduler doesn't give it CPU time promptly.
    // This is a pragmatic mitigation for CI's resource constraints - in normal operation
    // these channels shouldn't back up as they only coordinate transaction IDs.
    let (waiting_for_op_tx, waiting_for_op_rx) = mpsc::channel(1000);
    let (response_for_tx, response_for_rx) = mpsc::channel(1000);

    tracing::debug!("Created executor channels with buffer size 1000");

    let listener_halve = ExecutorToEventLoopChannel {
        op_manager: op_manager.clone(),
        end: NetworkEventListenerHalve {
            waiting_for_op_rx,
            response_for_tx,
        },
    };
    let sender_halve = ExecutorToEventLoopChannel {
        op_manager: op_manager.clone(),
        end: ExecutorHalve {
            waiting_for_op_tx,
            response_for_rx,
            completed: HashMap::default(),
            last_capacity_warning: None,
        },
    };
    (listener_halve, sender_halve)
}

#[derive(thiserror::Error, Debug)]
enum CallbackError {
    #[error(transparent)]
    Err(#[from] ExecutorError),
    #[error(transparent)]
    Conversion(#[from] OpError),
    #[error("missing result")]
    MissingResult,
}

impl ExecutorToEventLoopChannel<ExecutorHalve> {
    async fn send_to_event_loop<Op, T>(&mut self, message: T) -> anyhow::Result<Transaction>
    where
        T: ComposeNetworkMessage<Op>,
        Op: Operation + Send + 'static,
    {
        let op = message.initiate_op(&self.op_manager);
        let tx = *op.id();

        // Monitor channel capacity to detect backpressure issues
        // capacity() returns the current number of available slots
        let available_capacity = self.end.waiting_for_op_tx.capacity();
        if available_capacity < 20 {
            // Warn when 80%+ full (less than 20 slots available out of 100)
            // Rate limit warnings to once per 30 seconds to avoid log spam
            let now = std::time::Instant::now();
            let should_warn = self.end.last_capacity_warning.map_or(true, |last| {
                now.duration_since(last) > std::time::Duration::from_secs(30)
            });

            if should_warn {
                self.end.last_capacity_warning = Some(now);
                tracing::warn!(
                    available_capacity,
                    total_capacity = 100,
                    used_capacity = 100 - available_capacity,
                    "Executor channel approaching capacity - possible consumer backlog. \
                     This may indicate the network event listener is not keeping up. \
                     (This warning is rate-limited to once per 30 seconds)"
                );
            }
        } else if available_capacity < 50 {
            // Debug when 50%+ full
            tracing::debug!(
                available_capacity,
                total_capacity = 100,
                used_capacity = 100 - available_capacity,
                "Executor channel usage above 50%"
            );
        }

        self.end.waiting_for_op_tx.send(tx).await.inspect_err(|_| {
            tracing::debug!("failed to send request to executor, channel closed");
        })?;
        <T as ComposeNetworkMessage<Op>>::resume_op(op, &self.op_manager)
            .await
            .map_err(|e| {
                tracing::debug!("failed to resume operation: {e}");
                e
            })?;
        Ok(tx)
    }

    async fn receive_op_result<Op>(&mut self, transaction: Transaction) -> Result<Op, CallbackError>
    where
        Op: Operation + TryFrom<OpEnum, Error = OpError>,
    {
        if let Some(result) = self.end.completed.remove(&transaction) {
            return result.try_into().map_err(CallbackError::Conversion);
        }
        let op_result = self
            .end
            .response_for_rx
            .recv()
            .await
            .ok_or_else(|| ExecutorError::other(anyhow::anyhow!("channel closed")))?;
        if op_result.id() != &transaction {
            self.end.completed.insert(*op_result.id(), op_result);
            return Err(CallbackError::MissingResult);
        }
        op_result.try_into().map_err(CallbackError::Conversion)
    }
}

impl ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    pub async fn transaction_from_executor(&mut self) -> anyhow::Result<Transaction> {
        tracing::trace!("Waiting to receive transaction from executor channel");
        let tx = self
            .end
            .waiting_for_op_rx
            .recv()
            .await
            .ok_or_else(|| {
                tracing::error!("Executor channel closed - all senders have been dropped");
                tracing::error!("This typically happens when: 1) The executor task panicked/exited, 2) Network timeout cascaded to channel closure, 3) Resource constraints in CI");
                anyhow::anyhow!("channel closed")
            })?;
        tracing::trace!("Successfully received transaction from executor channel");
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

impl ExecutorToEventLoopChannel<Callback> {
    pub async fn response(&mut self, result: OpEnum) {
        if self.end.response_for_tx.send(result).await.is_err() {
            tracing::debug!("failed to send response to executor, channel closed");
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

pub struct ExecutorHalve {
    /// communicates the executor is waiting for a callback for a given transaction
    waiting_for_op_tx: mpsc::Sender<Transaction>,
    /// receives the callback response from the `process_message` task after completion
    response_for_rx: mpsc::Receiver<OpEnum>,
    /// stores the completed operations if they haven't been asked for yet in the executor
    completed: HashMap<Transaction, OpEnum>,
    /// Track last warning time to rate-limit channel capacity warnings
    last_capacity_warning: Option<std::time::Instant>,
}

mod sealed {
    use super::{Callback, ExecutorHalve, NetworkEventListenerHalve};
    pub trait ChannelHalve {}
    impl ChannelHalve for NetworkEventListenerHalve {}
    impl ChannelHalve for ExecutorHalve {}
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
    key: ContractKey,
    return_contract_code: bool,
}

impl ComposeNetworkMessage<operations::get::GetOp> for GetContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::get::GetOp {
        operations::get::start_op(self.key, self.return_contract_code, false)
    }

    async fn resume_op(op: operations::get::GetOp, op_manager: &OpManager) -> Result<(), OpError> {
        operations::get::request_get(op_manager, op, HashSet::new()).await
    }
}

#[allow(unused)]
struct SubscribeContract {
    key: ContractKey,
}

impl ComposeNetworkMessage<operations::subscribe::SubscribeOp> for SubscribeContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::subscribe::SubscribeOp {
        operations::subscribe::start_op(self.key)
    }

    async fn resume_op(
        op: operations::subscribe::SubscribeOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::subscribe::request_subscribe(op_manager, op).await
    }
}

#[allow(unused)]
struct PutContract {
    contract: ContractContainer,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
}

impl ComposeNetworkMessage<operations::put::PutOp> for PutContract {
    fn initiate_op(self, op_manager: &OpManager) -> operations::put::PutOp {
        let PutContract {
            contract,
            state,
            related_contracts,
        } = self;
        operations::put::start_op(
            contract,
            related_contracts,
            state,
            op_manager.ring.max_hops_to_live,
            false,
        )
    }

    async fn resume_op(op: operations::put::PutOp, op_manager: &OpManager) -> Result<(), OpError> {
        operations::put::request_put(op_manager, op).await
    }
}

#[allow(unused)]
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
        operations::update::start_op(key, new_state, related_contracts)
    }

    async fn resume_op(
        op: operations::update::UpdateOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::update::request_update(op_manager, op).await
    }
}

pub(crate) trait ContractExecutor: Send + 'static {
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
        key: ContractKey,
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

/// A WASM executor which will run any contracts, delegates, etc. registered.
///
/// This executor will monitor the store directories and databases to detect state changes.
/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor<R = Runtime> {
    mode: OperationMode,
    runtime: R,
    pub state_store: StateStore<Storage>,
    /// Notification channels for any clients subscribed to updates for a given contract.
    update_notifications: HashMap<ContractKey, Vec<(ClientId, mpsc::UnboundedSender<HostResult>)>>,
    /// Summaries of the state of all clients subscribed to a given contract.
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
    /// Attested contract instances for a given delegate.
    delegate_attested_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,

    event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
}

impl<R> Executor<R> {
    pub async fn new(
        state_store: StateStore<Storage>,
        ctrl_handler: impl FnOnce() -> anyhow::Result<()>,
        mode: OperationMode,
        runtime: R,
        event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
    ) -> anyhow::Result<Self> {
        ctrl_handler()?;

        Ok(Self {
            mode,
            runtime,
            state_store,
            update_notifications: HashMap::default(),
            subscriber_summaries: HashMap::default(),
            delegate_attested_ids: HashMap::default(),
            event_loop_channel,
        })
    }

    pub fn test_data_dir(identifier: &str) -> PathBuf {
        std::env::temp_dir().join(format!("freenet-executor-{identifier}"))
    }

    async fn get_stores(
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
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;

        let state_store =
            StateStore::new(Storage::new(&config.db_dir()).await?, MAX_MEM_CACHE).unwrap();
        let contract_store = ContractStore::new(config.contracts_dir(), MAX_SIZE)?;

        let delegate_store = DelegateStore::new(config.delegates_dir(), MAX_SIZE)?;

        let secret_store = SecretsStore::new(config.secrets_dir(), config.secrets.clone())?;

        Ok((contract_store, delegate_store, secret_store, state_store))
    }

    async fn op_request<Op, M>(&mut self, request: M) -> Result<Op::Result, ExecutorError>
    where
        Op: Operation + Send + TryFrom<OpEnum, Error = OpError> + 'static,
        <Op as Operation>::Result: TryFrom<Op, Error = OpError>,
        M: ComposeNetworkMessage<Op>,
    {
        let Some(ch) = &mut self.event_loop_channel else {
            return Err(ExecutorError::other(anyhow::anyhow!(
                "missing event loop channel"
            )));
        };
        let transaction = ch
            .send_to_event_loop(request)
            .await
            .map_err(ExecutorError::other)?;
        // FIXME: must add a way to suspend a request while waiting for result and resume upon getting
        // an answer back so we don't block the executor itself.
        // otherwise it may be possible to end up in a deadlock waiting for a tree of contract
        // dependencies to be resolved
        let result = loop {
            match ch.receive_op_result::<Op>(transaction).await {
                Ok(result) => break result,
                Err(CallbackError::MissingResult) => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
                Err(CallbackError::Conversion(err)) => {
                    tracing::error!("expect message of one type but got an other: {err}");
                    return Err(ExecutorError::other(err));
                }
                Err(CallbackError::Err(other)) => return Err(other),
            }
        };
        let result = <Op::Result>::try_from(result).map_err(|err| {
            tracing::debug!("didn't get result back: {err}");
            ExecutorError::other(err)
        })?;
        Ok(result)
    }

    pub fn get_subscription_info(&self) -> Vec<crate::message::SubscriptionInfo> {
        let mut subscriptions = Vec::new();
        for (contract_key, client_list) in &self.update_notifications {
            for (client_id, _channel) in client_list {
                subscriptions.push(crate::message::SubscriptionInfo {
                    contract_key: *contract_key,
                    client_id: *client_id,
                    last_update: None,
                });
            }
        }
        subscriptions
    }
}
