//! Contract executor.

use std::collections::HashMap;
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

use crate::config::Config;
use crate::message::Transaction;
use crate::node::OpManager;
#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
use crate::operations::get::GetResult;
use crate::operations::{OpEnum, OpError};
use crate::wasm_runtime::{
    ContractRuntimeInterface, ContractStore, DelegateRuntimeInterface, DelegateStore, Runtime,
    SecretsStore, StateStore, StateStoreError,
};
use crate::{
    client_events::{ClientId, HostResult},
    operations::{self, Operation},
    DynError,
};

use super::storages::Storage;

pub(super) mod mock_runtime;
pub(super) mod runtime;

#[derive(Debug)]
pub struct ExecutorError(Either<Box<RequestError>, DynError>);

enum InnerOpError {
    Upsert(ContractKey),
    Delegate(DelegateKey),
}

impl std::error::Error for ExecutorError {}

impl ExecutorError {
    pub fn other(error: impl Into<DynError>) -> Self {
        Self(Either::Right(error.into()))
    }

    /// Call this when an unreachable path is reached but need to avoid panics.
    fn internal_error() -> Self {
        ExecutorError(Either::Right("internal error".into()))
    }

    fn request(error: impl Into<RequestError>) -> Self {
        Self(Either::Left(Box::new(error.into())))
    }

    fn execution(
        outer_error: crate::wasm_runtime::ContractError,
        op: Option<InnerOpError>,
    ) -> Self {
        use crate::wasm_runtime::RuntimeInnerError;
        let error = outer_error.deref();

        if let RuntimeInnerError::ContractExecError(e) = error {
            if let Some(InnerOpError::Upsert(key)) = &op {
                return ExecutorError::request(StdContractError::update_exec_error(key.clone(), e));
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
                _ => return ExecutorError::other(format!("execution error: {e}")),
            },
            RuntimeInnerError::WasmExportError(e) => match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e))
                }
                _ => return ExecutorError::other(format!("execution error: {e}")),
            },
            RuntimeInnerError::WasmInstantiationError(e) => match op {
                Some(InnerOpError::Upsert(key)) => {
                    return ExecutorError::request(StdContractError::update_exec_error(key, e))
                }
                _ => return ExecutorError::other(format!("execution error: {e}")),
            },
            _ => {}
        }

        ExecutorError::other(outer_error)
    }

    pub fn is_request(&self) -> bool {
        matches!(self.0, Either::Left(_))
    }

    pub fn unwrap_request(self) -> RequestError {
        match self.0 {
            Either::Left(err) => *err,
            Either::Right(_) => panic!(),
        }
    }
}

impl From<RequestError> for ExecutorError {
    fn from(value: RequestError) -> Self {
        Self(Either::Left(Box::new(value)))
    }
}

impl Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Either::Left(l) => write!(f, "{}", &**l),
            Either::Right(r) => write!(f, "{}", &**r),
        }
    }
}

impl From<Box<RequestError>> for ExecutorError {
    fn from(value: Box<RequestError>) -> Self {
        Self(Either::Left(value))
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
    // todo: use sensible values for channel buf sizes based on number concurrent tasks running
    // when we are able to suspend execution of a request while waiting for a callback
    let (waiting_for_op_tx, waiting_for_op_rx) = mpsc::channel(1);
    let (response_for_tx, response_for_rx) = mpsc::channel(1);

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
    async fn send_to_event_loop<Op, T>(&mut self, message: T) -> Result<Transaction, DynError>
    where
        T: ComposeNetworkMessage<Op>,
        Op: Operation + Send + 'static,
    {
        let op = message.initiate_op(&self.op_manager);
        let tx = *op.id();
        self.end.waiting_for_op_tx.send(tx).await.map_err(|e| {
            tracing::debug!("failed to send request to executor, channel closed");
            e
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
            .ok_or_else(|| ExecutorError::other("channel closed"))?;
        if op_result.id() != &transaction {
            self.end.completed.insert(*op_result.id(), op_result);
            return Err(CallbackError::MissingResult);
        }
        op_result.try_into().map_err(CallbackError::Conversion)
    }
}

impl ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    pub async fn transaction_from_executor(&mut self) -> Result<Transaction, DynError> {
        let tx = self
            .end
            .waiting_for_op_rx
            .recv()
            .await
            .ok_or("channel closed")?;
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
    fetch_contract: bool,
}

impl ComposeNetworkMessage<operations::get::GetOp> for GetContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::get::GetOp {
        operations::get::start_op(self.key, self.fetch_contract)
    }

    async fn resume_op(op: operations::get::GetOp, op_manager: &OpManager) -> Result<(), OpError> {
        operations::get::request_get(op_manager, op).await
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
        fetch_contract: bool,
    ) -> impl Future<Output = Result<(WrappedState, Option<ContractContainer>), ExecutorError>> + Send;

    fn store_contract(
        &mut self,
        contract: ContractContainer,
    ) -> impl Future<Output = Result<(), ExecutorError>> + Send;

    fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> impl Future<Output = Result<WrappedState, ExecutorError>> + Send;
}

/// A WASM executor which will run any contracts, delegates, etc. registered.
///
/// This executor will monitor the store directories and databases to detect state changes.
/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor<R = Runtime> {
    #[cfg(any(
        all(feature = "local-mode", feature = "network-mode"),
        all(not(feature = "local-mode"), not(feature = "network-mode")),
    ))]
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
        ctrl_handler: impl FnOnce() -> Result<(), DynError>,
        mode: OperationMode,
        runtime: R,
        event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
    ) -> Result<Self, DynError> {
        ctrl_handler()?;

        Ok(Self {
            #[cfg(any(
                all(feature = "local-mode", feature = "network-mode"),
                all(not(feature = "local-mode"), not(feature = "network-mode")),
            ))]
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
        DynError,
    > {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;

        let state_store =
            StateStore::new(Storage::new(&config.db_dir()).await?, MAX_MEM_CACHE).unwrap();
        let contract_store = ContractStore::new(config.contracts_dir(), MAX_SIZE)?;

        let delegate_store = DelegateStore::new(config.delegates_dir(), MAX_SIZE)?;

        let secret_store = SecretsStore::new(config.secrets_dir())?;

        Ok((contract_store, delegate_store, secret_store, state_store))
    }

    async fn op_request<Op, M>(&mut self, request: M) -> Result<Op::Result, ExecutorError>
    where
        Op: Operation + Send + TryFrom<OpEnum, Error = OpError> + 'static,
        <Op as Operation>::Result: TryFrom<Op, Error = OpError>,
        M: ComposeNetworkMessage<Op>,
    {
        let Some(ch) = &mut self.event_loop_channel else {
            return Err(ExecutorError::other("missing event loop channel"));
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
}
