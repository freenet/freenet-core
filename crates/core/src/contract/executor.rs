//! Contract executor.

use std::collections::HashMap;
use std::fmt::Display;
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
use tokio::sync::mpsc::{self};

use crate::message::Transaction;
use crate::node::OpManager;
#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
use crate::operations::get::GetResult;
use crate::operations::{OpEnum, OpError};
use crate::runtime::{
    ContractRuntimeInterface, ContractStore, DelegateRuntimeInterface, DelegateStore, Runtime,
    SecretsStore, StateStore, StateStoreError,
};
use crate::{
    client_events::{ClientId, HostResult},
    node::PeerCliConfig,
    operations::{self, Operation},
    DynError,
};

use super::storages::Storage;

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

    fn execution(outer_error: crate::runtime::ContractError, op: Option<InnerOpError>) -> Self {
        use crate::runtime::RuntimeInnerError;
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
            RuntimeInnerError::SecretStoreError(crate::runtime::SecretStoreError::MissingSecret(
                secret,
            )),
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

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
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
        self.end.waiting_for_op_tx.send(tx).await?;
        <T as ComposeNetworkMessage<Op>>::resume_op(op, &self.op_manager).await?;
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

#[async_trait::async_trait]
trait ComposeNetworkMessage<Op>
where
    Self: Sized,
    Op: Operation + Send + 'static,
{
    fn initiate_op(self, op_manager: &OpManager) -> Op;

    async fn resume_op(op: Op, op_manager: &OpManager) -> Result<(), OpError>;
}

#[allow(unused)]
struct GetContract {
    key: ContractKey,
    fetch_contract: bool,
}

#[async_trait::async_trait]
impl ComposeNetworkMessage<operations::get::GetOp> for GetContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::get::GetOp {
        operations::get::start_op(self.key, self.fetch_contract)
    }

    async fn resume_op(op: operations::get::GetOp, op_manager: &OpManager) -> Result<(), OpError> {
        operations::get::request_get(op_manager, op, None).await
    }
}

#[allow(unused)]
struct SubscribeContract {
    key: ContractKey,
}

#[async_trait::async_trait]
impl ComposeNetworkMessage<operations::subscribe::SubscribeOp> for SubscribeContract {
    fn initiate_op(self, _op_manager: &OpManager) -> operations::subscribe::SubscribeOp {
        operations::subscribe::start_op(self.key)
    }

    async fn resume_op(
        op: operations::subscribe::SubscribeOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::subscribe::request_subscribe(op_manager, op, None).await
    }
}

#[allow(unused)]
struct PutContract {
    contract: ContractContainer,
    state: WrappedState,
    related_contracts: RelatedContracts<'static>,
}

#[async_trait::async_trait]
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
        operations::put::request_put(op_manager, op, None).await
    }
}

#[allow(unused)]
struct UpdateContract {
    key: ContractKey,
    new_state: WrappedState,
}

#[async_trait::async_trait]
impl ComposeNetworkMessage<operations::update::UpdateOp> for UpdateContract {
    fn initiate_op(self, op_manager: &OpManager) -> operations::update::UpdateOp {
        let UpdateContract { key, new_state } = self;
        operations::update::start_op(key, new_state, op_manager.ring.max_hops_to_live)
    }

    async fn resume_op(
        op: operations::update::UpdateOp,
        op_manager: &OpManager,
    ) -> Result<(), OpError> {
        operations::update::request_update(op_manager, op, None).await
    }
}

#[async_trait::async_trait]
pub(crate) trait ContractExecutor: Send + 'static {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError>;
    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ExecutorError>;

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<WrappedState, ExecutorError>;
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
    update_notifications: HashMap<ContractKey, Vec<(ClientId, mpsc::UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
    delegate_attested_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,
    event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
}

#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
impl Executor<Runtime> {
    async fn subscribe(&mut self, key: ContractKey) -> Result<(), ExecutorError> {
        #[cfg(any(
            all(not(feature = "local-mode"), not(feature = "network-mode")),
            all(feature = "local-mode", feature = "network-mode")
        ))]
        {
            if self.mode == OperationMode::Local {
                return Ok(());
            }
        }
        let request = SubscribeContract { key };
        let _sub: operations::subscribe::SubscribeResult = self.op_request(request).await?;
        Ok(())
    }

    #[inline]
    async fn local_state_or_from_network(
        &mut self,
        id: &ContractInstanceId,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        if let Ok(contract) = self.state_store.get(&(*id).into()).await {
            return Ok(Either::Left(contract));
        };
        let request: GetContract = GetContract {
            key: (*id).into(),
            fetch_contract: true,
        };
        let get_result: operations::get::GetResult = self.op_request(request).await?;
        Ok(Either::Right(get_result))
    }

    async fn get_contract_from_network(
        &mut self,
        key: ContractKey,
    ) -> Result<Option<ContractContainer>, ExecutorError> {
        loop {
            if let Ok(Some(contract)) = self.get_contract_locally(&key).await {
                break Ok(Some(contract));
            } else {
                #[cfg(any(
                    all(not(feature = "local-mode"), not(feature = "network-mode")),
                    all(feature = "local-mode", feature = "network-mode")
                ))]
                {
                    if self.mode == OperationMode::Local {
                        return Err(ExecutorError::request(RequestError::ContractError(
                            StdContractError::MissingRelated { key: *key.id() },
                        )));
                    }
                }
                match self
                    .local_state_or_from_network(&key.clone().into())
                    .await?
                {
                    Either::Right(GetResult { state, contract }) => {
                        let Some(contract) = contract else {
                            return Err(ExecutorError::request(RequestError::ContractError(
                                StdContractError::Get {
                                    key,
                                    cause: "missing-contract".into(),
                                },
                            )));
                        };
                        self.verify_and_store_contract(
                            state,
                            contract.clone(),
                            RelatedContracts::default(),
                        )
                        .await?;
                        break Ok(Some(contract));
                    }
                    Either::Left(_state) => continue,
                }
            }
        }
    }
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

    async fn get_stores(
        config: &PeerCliConfig,
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
        let static_conf = crate::config::Config::conf();

        let db_path = crate::config::Config::conf().db_dir();
        let state_store =
            StateStore::new(Storage::new(Some(&db_path)).await?, MAX_MEM_CACHE).unwrap();

        let contract_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("contracts"))
            .unwrap_or_else(|| static_conf.contracts_dir());
        let contract_store = ContractStore::new(contract_dir, MAX_SIZE)?;

        let delegate_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("delegates"))
            .unwrap_or_else(|| static_conf.delegates_dir());
        let delegate_store = DelegateStore::new(delegate_dir, MAX_SIZE)?;

        let secrets_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("secrets"))
            .unwrap_or_else(|| static_conf.secrets_dir());
        let secret_store = SecretsStore::new(secrets_dir)?;

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

impl Executor<Runtime> {
    pub async fn from_config(
        config: PeerCliConfig,
        event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
    ) -> Result<Self, DynError> {
        let (contract_store, delegate_store, secret_store, state_store) =
            Self::get_stores(&config).await?;
        let rt = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
        Executor::new(
            state_store,
            || {
                crate::util::set_cleanup_on_exit()?;
                Ok(())
            },
            OperationMode::Local,
            rt,
            event_loop_channel,
        )
        .await
    }

    pub fn register_contract_notifier(
        &mut self,
        key: ContractKey,
        cli_id: ClientId,
        notification_ch: tokio::sync::mpsc::UnboundedSender<HostResult>,
        summary: Option<StateSummary<'_>>,
    ) -> Result<(), Box<RequestError>> {
        let channels = self.update_notifications.entry(key.clone()).or_default();
        if let Ok(i) = channels.binary_search_by_key(&&cli_id, |(p, _)| p) {
            let (_, existing_ch) = &channels[i];
            if !existing_ch.same_channel(&notification_ch) {
                return Err(RequestError::from(StdContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed").into(),
                })
                .into());
            }
        } else {
            channels.push((cli_id, notification_ch));
        }

        if self
            .subscriber_summaries
            .entry(key.clone())
            .or_default()
            .insert(cli_id, summary.map(StateSummary::into_owned))
            .is_some()
        {
            tracing::warn!(
                "contract {key} already was registered for peer {cli_id}; replaced summary"
            );
        }
        Ok(())
    }

    pub async fn preload(
        &mut self,
        cli_id: ClientId,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
    ) {
        if let Err(err) = self
            .contract_requests(
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                },
                cli_id,
                None,
            )
            .await
        {
            match err.0 {
                Either::Left(err) => tracing::error!("req error: {err}"),
                Either::Right(err) => tracing::error!("other error: {err}"),
            }
        }
    }

    pub async fn handle_request<'a>(
        &mut self,
        id: ClientId,
        req: ClientRequest<'a>,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_requests(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_request(op, None),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(RequestError::Disconnect.into())
            }
            _ => Err(ExecutorError::other("not supported")),
        }
    }

    /// Responde to requests made through any API's from client applications locally.
    pub async fn contract_requests(
        &mut self,
        req: ContractRequest<'_>,
        cli_id: ClientId,
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        match req {
            ContractRequest::Put {
                contract,
                state,
                related_contracts,
            } => {
                self.perform_contract_put(contract, state, related_contracts)
                    .await
            }
            ContractRequest::Update { key, data } => self.perform_contract_update(key, data).await,
            ContractRequest::Get {
                key,
                fetch_contract: contract,
            } => self.perform_contract_get(contract, key).await,
            ContractRequest::Subscribe { key, summary } => {
                let updates =
                    updates.ok_or_else(|| ExecutorError::other("missing update channel"))?;
                self.register_contract_notifier(key.clone(), cli_id, updates, summary)?;
                // by default a subscribe op has an implicit get
                let res = self.perform_contract_get(false, key.clone()).await?;
                #[cfg(any(
                    all(not(feature = "local-mode"), not(feature = "network-mode")),
                    all(feature = "local-mode", feature = "network-mode"),
                    all(feature = "network-mode", not(feature = "network-mode"))
                ))]
                {
                    self.subscribe(key).await?;
                }
                Ok(res)
            }
            _ => Err(ExecutorError::other("not supported")),
        }
    }

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        attestaded_contract: Option<&ContractInstanceId>,
    ) -> Response {
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = GenericArray::from_slice(&cipher);
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = GenericArray::from_slice(&nonce).to_owned();
                tracing::debug!("registering delegate `{key}");
                if let Some(contract) = attestaded_contract {
                    self.delegate_attested_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::error!("failed registering delegate `{key}`: {err}");
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_attested_ids.remove(&key);
                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::error!("failed unregistering delegate `{key}`: {err}");
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::GetSecretRequest {
                key,
                params,
                get_request,
            } => {
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    vec![InboundDelegateMsg::GetSecretRequest(get_request)],
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                    Err(err) => Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Delegate(key.clone())),
                    )),
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                let attested = attestaded_contract.and_then(|contract| {
                    self.delegate_attested_ids
                        .get(&key)
                        .and_then(|contracts| contracts.iter().find(|c| *c == contract))
                });
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    attested.map(|c| c.as_bytes()),
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),

                    Err(err) => {
                        tracing::error!("failed executing delegate `{key}`: {err}");
                        Err(ExecutorError::other(format!(
                            "uncontrolled error while executing `{key}`"
                        )))
                    }
                }
            }
            _ => Err(ExecutorError::other("not supported")),
        }
    }

    async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        let key = contract.key();
        if self.update_notifications.get(&key).is_none() {
            return Ok(ContractResponse::PutResponse { key }.into());
        }
        let params = contract.params();

        if self.get_local_contract(key.id()).await.is_ok() {
            // already existing contract, just try to merge states
            return self
                .perform_contract_update(key, UpdateData::State(state.into()))
                .await;
        }

        self.verify_and_store_contract(state.clone(), contract, related_contracts)
            .await?;

        self.send_update_notification(&key, &params, &state)
            .await
            .map_err(|_| {
                ExecutorError::request(StdContractError::Put {
                    key: key.clone(),
                    cause: "failed while sending notifications".into(),
                })
            })?;
        Ok(ContractResponse::PutResponse { key }.into())
    }

    async fn perform_contract_update(
        &mut self,
        key: ContractKey,
        update: UpdateData<'_>,
    ) -> Response {
        let parameters = {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    RequestError::ContractError(StdContractError::Update {
                        cause: "missing contract parameters".into(),
                        key: key.clone(),
                    })
                })?
        };

        let current_state = self
            .state_store
            .get(&key)
            .await
            .map_err(ExecutorError::other)?
            .clone();

        let updates = vec![update];
        let new_state = self
            .get_updated_state(&parameters, current_state, key.clone(), updates)
            .await?;

        // in the network impl this would be sent over the network
        let summary = self
            .runtime
            .summarize_state(&key, &parameters, &new_state)
            .map_err(ExecutorError::other)?;
        self.send_update_notification(&key, &parameters, &new_state)
            .await?;

        #[cfg(any(
            not(feature = "local-mode"),
            feature = "network-mode",
            all(not(feature = "local-mode"), not(feature = "network-mode"))
        ))]
        {
            #[cfg(any(
                all(feature = "local-mode", feature = "network-mode"),
                all(not(feature = "local-mode"), not(feature = "network-mode"))
            ))]
            {
                if self.mode == OperationMode::Local {
                    return Ok(ContractResponse::UpdateResponse { key, summary }.into());
                }
            }
            // notify peers with deltas from summary in network
            let request = UpdateContract {
                key: key.clone(),
                new_state,
            };
            let _op: operations::update::UpdateResult = self.op_request(request).await?;
        }

        Ok(ContractResponse::UpdateResponse { key, summary }.into())
    }

    /// Attempts to update the state with the provided updates.
    /// If there were no updates, it will return the current state.
    async fn attempt_state_update(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: &WrappedState,
        key: &ContractKey,
        updates: &[UpdateData<'_>],
    ) -> Result<Either<WrappedState, Vec<RelatedContract>>, ExecutorError> {
        let update_modification =
            match self
                .runtime
                .update_state(key, parameters, current_state, updates)
            {
                Ok(result) => result,
                Err(err) => {
                    return Err(ExecutorError::execution(
                        err,
                        Some(InnerOpError::Upsert(key.clone())),
                    ))
                }
            };
        let UpdateModification {
            new_state, related, ..
        } = update_modification;
        let Some(new_state) = new_state else {
            if related.is_empty() {
                // no updates were made, just return old state
                return Ok(Either::Left(current_state.clone()));
            } else {
                return Ok(Either::Right(related));
            }
        };
        let new_state = WrappedState::new(new_state.into_bytes());
        self.state_store
            .update(key, new_state.clone())
            .await
            .map_err(ExecutorError::other)?;
        Ok(Either::Left(new_state))
    }

    /// Given a contract and a series of delta updates, it will try to perform an update
    /// to the contract state and return the new state. If it fails to update the state,
    /// it will return an error.
    ///
    /// If there are missing updates for related contracts, it will try to fetch them from the network.
    async fn get_updated_state(
        &mut self,
        parameters: &Parameters<'_>,
        current_state: WrappedState,
        key: ContractKey,
        mut updates: Vec<UpdateData<'_>>,
    ) -> Result<WrappedState, ExecutorError> {
        let new_state = {
            let start = Instant::now();
            loop {
                let state_update_res = self
                    .attempt_state_update(parameters, &current_state, &key, &updates)
                    .await?;
                let missing = match state_update_res {
                    Either::Left(new_state) => {
                        self.state_store
                            .update(&key, new_state.clone())
                            .await
                            .map_err(ExecutorError::other)?;
                        break new_state;
                    }
                    Either::Right(missing) => missing,
                };
                // some required contracts are missing
                let required_contracts = missing.len() + 1;
                for RelatedContract {
                    contract_instance_id: id,
                    mode,
                } in missing
                {
                    match self.state_store.get(&id.into()).await {
                        Ok(state) => {
                            // in this case we are already subscribed to and are updating this contract,
                            // we can try first with the existing value
                            updates.push(UpdateData::RelatedState {
                                related_to: id,
                                state: state.into(),
                            });
                        }
                        #[cfg(any(
                            all(not(feature = "local-mode"), not(feature = "network-mode")),
                            all(feature = "local-mode", feature = "network-mode")
                        ))]
                        Err(StateStoreError::MissingContract(_))
                            if self.mode == OperationMode::Network =>
                        {
                            let state = match self.local_state_or_from_network(&id).await? {
                                Either::Left(state) => state,
                                Either::Right(GetResult { state, contract }) => {
                                    let Some(contract) = contract else {
                                        return Err(ExecutorError::request(
                                            RequestError::ContractError(StdContractError::Get {
                                                key,
                                                cause: "missing-contract".into(),
                                            }),
                                        ));
                                    };
                                    self.verify_and_store_contract(
                                        state.clone(),
                                        contract,
                                        RelatedContracts::default(),
                                    )
                                    .await?;
                                    state
                                }
                            };
                            updates.push(UpdateData::State(state.into()));
                            match mode {
                                RelatedMode::StateOnce => {}
                                RelatedMode::StateThenSubscribe => {
                                    self.subscribe(id.into()).await?;
                                }
                            }
                        }
                        #[cfg(all(not(feature = "local-mode"), feature = "network-mode"))]
                        Err(StateStoreError::MissingContract(_)) => {
                            let state = match self.local_state_or_from_network(&id).await? {
                                Either::Left(state) => current_state,
                                Either::Right(GetResult { state, contract }) => {
                                    self.verify_and_store_contract(
                                        current_state.clone(),
                                        contract,
                                        RelatedContracts::default(),
                                    )
                                    .await?;
                                    current_state
                                }
                            };
                            updates.push(UpdateData::State(current_state.into()));
                            match mode {
                                RelatedMode::StateOnce => {}
                                RelatedMode::StateThenSubscribe => {
                                    self.subscribe(id.into()).await?;
                                }
                            }
                        }
                        Err(other_err) => {
                            let _ = mode;
                            return Err(ExecutorError::other(other_err));
                        }
                    }
                }
                if updates.len() + 1 /* includes the original contract being updated update */ >= required_contracts
                {
                    // try running again with all the related contracts retrieved
                    continue;
                } else if start.elapsed() > Duration::from_secs(10) {
                    /* make this timeout configurable, and anyway should be controlled globally*/
                    return Err(RequestError::Timeout.into());
                }
            }
        };
        Ok(new_state)
    }

    async fn perform_contract_get(&mut self, contract: bool, key: ContractKey) -> Response {
        let mut got_contract = None;

        #[cfg(any(
            all(not(feature = "local-mode"), not(feature = "network-mode")),
            all(feature = "local-mode", feature = "network-mode"),
        ))]
        {
            if contract && self.mode == OperationMode::Local {
                let Some(contract) = self.get_contract_locally(&key).await? else {
                    return Err(ExecutorError::request(RequestError::from(
                        StdContractError::Get {
                            key: key.clone(),
                            cause: "Missing contract or parameters".into(),
                        },
                    )));
                };
                got_contract = Some(contract);
            } else if contract {
                got_contract = self.get_contract_from_network(key.clone()).await?;
            }
        }

        #[cfg(all(feature = "local-mode", not(feature = "network-mode")))]
        if contract {
            let Some(contract) = self
                .get_contract_locally(&key)
                .await
                .map_err(Either::Left)?
            else {
                return Err(ExecutorError::request(RequestError::from(
                    StdContractError::Get {
                        key: key.clone(),
                        cause: "Missing contract or parameters".into(),
                    },
                )));
            };
            got_contract = Some(contract);
        }

        #[cfg(all(feature = "network-mode", not(feature = "local-mode")))]
        if contract {
            if let Ok(Some(contract)) = self.get_contract_locally(&key).await {
                got_contract = Some(contract);
            } else {
                got_contract = self.get_contract_from_network(key.clone()).await?;
            }
        }

        match self.state_store.get(&key).await {
            Ok(state) => Ok(ContractResponse::GetResponse {
                key,
                contract: got_contract,
                state,
            }
            .into()),
            Err(StateStoreError::MissingContract(_)) => Err(ExecutorError::request(
                RequestError::from(StdContractError::Get {
                    key,
                    cause: "Missing contract state".into(),
                }),
            )),
            Err(err) => Err(ExecutorError::request(RequestError::from(
                StdContractError::Get {
                    key,
                    cause: format!("{err}").into(),
                },
            ))),
        }
    }

    async fn get_local_contract(
        &self,
        id: &ContractInstanceId,
    ) -> Result<State<'static>, Either<Box<RequestError>, DynError>> {
        let Ok(contract) = self.state_store.get(&(*id).into()).await else {
            return Err(Either::Right(
                StdContractError::MissingRelated { key: *id }.into(),
            ));
        };
        let state: &[u8] = unsafe {
            // Safety: this is fine since this will never scape this scope
            std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref())
        };
        Ok(State::from(state))
    }

    async fn verify_and_store_contract<'a>(
        &mut self,
        state: WrappedState,
        trying_container: ContractContainer,
        mut related_contracts: RelatedContracts<'a>,
    ) -> Result<(), ExecutorError> {
        let key = trying_container.key();
        let params = trying_container.params();

        const DEPENDENCY_CYCLE_LIMIT_GUARD: usize = 100;
        let mut iterations = 0;

        let original_key = key.clone();
        let original_state = state.clone();
        let original_params = params.clone();
        let mut trying_key = key;
        let mut trying_state = state;
        let mut trying_params = params;
        let mut trying_contract = Some(trying_container);

        while iterations < DEPENDENCY_CYCLE_LIMIT_GUARD {
            if let Some(contract) = trying_contract.take() {
                self.runtime
                    .contract_store
                    .store_contract(contract)
                    .map_err(ExecutorError::other)?;
            }

            let result = self
                .runtime
                .validate_state(
                    &trying_key,
                    &trying_params,
                    &trying_state,
                    &related_contracts,
                )
                .map_err(|err| {
                    let _ = self.runtime.contract_store.remove_contract(&trying_key);
                    ExecutorError::other(err)
                })?;

            let is_valid = match result {
                ValidateResult::Valid => true,
                ValidateResult::Invalid => false,
                ValidateResult::RequestRelated(related) => {
                    iterations += 1;
                    related_contracts.missing(related);
                    for (id, related) in related_contracts.update() {
                        if related.is_none() {
                            #[cfg(all(feature = "local-mode", not(feature = "network-mode")))]
                            {
                                let current_state = self.get_local_contract(id).await?;
                                *related = Some(current_state);
                            }

                            #[cfg(any(
                                all(not(feature = "local-mode"), not(feature = "network-mode")),
                                all(feature = "local-mode", feature = "network-mode"),
                                all(not(feature = "local-mode"), feature = "network-mode")
                            ))]
                            {
                                match self.local_state_or_from_network(id).await? {
                                    Either::Left(state) => {
                                        *related = Some(state.into());
                                    }
                                    Either::Right(result) => {
                                        let Some(contract) = result.contract else {
                                            return Err(ExecutorError::request(
                                                RequestError::ContractError(
                                                    StdContractError::Get {
                                                        key: (*id).into(),
                                                        cause: "missing-contract".into(),
                                                    },
                                                ),
                                            ));
                                        };
                                        trying_key = (*id).into();
                                        trying_params = contract.params();
                                        trying_state = result.state;
                                        trying_contract = Some(contract);
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    continue;
                }
            };

            if !is_valid {
                return Err(ExecutorError::request(StdContractError::Put {
                    key: trying_key.clone(),
                    cause: "not valid".into(),
                }));
            }

            self.state_store
                .store(
                    trying_key.clone(),
                    trying_state.clone(),
                    trying_params.clone(),
                )
                .await
                .map_err(ExecutorError::other)?;
            if trying_key != original_key {
                trying_key = original_key.clone();
                trying_params = original_params.clone();
                trying_state = original_state.clone();
                continue;
            }
            break;
        }
        if iterations == DEPENDENCY_CYCLE_LIMIT_GUARD {
            return Err(ExecutorError::request(StdContractError::MissingRelated {
                key: *original_key.id(),
            }));
        }
        Ok(())
    }

    async fn send_update_notification<'a>(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'a>,
        new_state: &WrappedState,
    ) -> Result<(), ExecutorError> {
        tracing::debug!(contract = %key, "notify of contract update");
        if let Some(notifiers) = self.update_notifications.get_mut(key) {
            let summaries = self.subscriber_summaries.get_mut(key).unwrap();
            // in general there should be less than 32 failures
            let mut failures = Vec::with_capacity(32);
            for (peer_key, notifier) in notifiers.iter() {
                let peer_summary = summaries.get_mut(peer_key).unwrap();
                let update = match peer_summary {
                    Some(summary) => self
                        .runtime
                        .get_state_delta(key, params, new_state, &*summary)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ExecutorError::execution(err, Some(InnerOpError::Upsert(key.clone())))
                        })?
                        .to_owned()
                        .into(),
                    None => UpdateData::State(State::from(new_state.as_ref()).into_owned()),
                };
                if let Err(err) = notifier.send(Ok(ContractResponse::UpdateNotification {
                    key: key.clone(),
                    update,
                }
                .into()))
                {
                    failures.push(*peer_key);
                    tracing::error!(cli_id = %peer_key, "{err}");
                } else {
                    tracing::debug!(cli_id = %peer_key, contract = %key, "notified of update");
                }
            }
            if !failures.is_empty() {
                notifiers.retain(|(c, _)| !failures.contains(c));
            }
        }
        Ok(())
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Ok(None);
        };
        let Some(contract) = self.runtime.contract_store.fetch_contract(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }
}

impl Executor<crate::contract::MockRuntime> {
    pub async fn new_mock(
        identifier: &str,
        event_loop_channel: ExecutorToEventLoopChannel<ExecutorHalve>,
    ) -> Result<Self, DynError> {
        let data_dir = std::env::temp_dir().join(format!("freenet-executor-{identifier}"));

        let contracts_data_dir = data_dir.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)?;

        let db_path = data_dir.join("db");
        std::fs::create_dir_all(&db_path).expect("directory created");
        let log_file = data_dir.join("_EVENT_LOG_LOCAL");
        crate::config::Config::set_event_log(log_file);
        let state_store =
            StateStore::new(Storage::new(Some(&db_path)).await?, u16::MAX as u32).unwrap();

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            super::MockRuntime { contract_store },
            Some(event_loop_channel),
        )
        .await?;
        Ok(executor)
    }

    pub async fn handle_request<'a>(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'a>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, WsClientError>>>,
    ) -> Response {
        unreachable!()
    }
}

#[async_trait::async_trait]
impl ContractExecutor for Executor<Runtime> {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError> {
        match self.perform_contract_get(fetch_contract, key).await {
            Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                contract,
                state,
                ..
            })) => Ok((state, contract)),
            Err(err) => Err(err),
            Ok(_) => {
                // Safety: check `perform_contract_get` to indeed check this should never happen
                unsafe { std::hint::unreachable_unchecked() }
            }
        }
    }

    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ExecutorError> {
        self.runtime
            .contract_store
            .store_contract(contract)
            .map_err(ExecutorError::other)
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<WrappedState, ExecutorError> {
        let params = if let Some(code) = &code {
            code.params()
        } else {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    ExecutorError::request(StdContractError::Put {
                        key: key.clone(),
                        cause: "missing contract parameters".into(),
                    })
                })?
        };

        let contract = if let Some(code) = self.runtime.contract_store.fetch_contract(&key, &params)
        {
            code
        } else {
            let code = code.ok_or_else(|| {
                ExecutorError::request(StdContractError::MissingContract {
                    key: key.clone().into(),
                })
            })?;
            self.runtime
                .contract_store
                .store_contract(code.clone())
                .map_err(ExecutorError::other)?;
            code
        };

        let mut updates = match update {
            Either::Left(incoming_state) => {
                let result = self
                    .runtime
                    .validate_state(&key, &params, &incoming_state, &related_contracts)
                    .map_err(|err| {
                        let _ = self.runtime.contract_store.remove_contract(&key);
                        ExecutorError::other(err)
                    })?;
                match result {
                    ValidateResult::Valid => {}
                    ValidateResult::Invalid => {
                        return Err(ExecutorError::request(StdContractError::invalid_put(key)));
                    }
                    ValidateResult::RequestRelated(mut related) => {
                        if let Some(key) = related.pop() {
                            return Err(ExecutorError::request(StdContractError::MissingRelated {
                                key,
                            }));
                        } else {
                            return Err(ExecutorError::internal_error());
                        }
                    }
                }

                let request = PutContract {
                    contract,
                    state: incoming_state.clone(),
                    related_contracts: related_contracts.clone(),
                };
                let _op: operations::put::PutResult = self.op_request(request).await?;

                vec![UpdateData::State(incoming_state.clone().into())]
            }
            Either::Right(delta) => {
                let valid = self
                    .runtime
                    .validate_delta(&key, &params, &delta)
                    .map_err(|err| {
                        let _ = self.runtime.contract_store.remove_contract(&key);
                        ExecutorError::other(err)
                    })?;
                if !valid {
                    return Err(ExecutorError::request(StdContractError::invalid_update(
                        key,
                    )));
                }
                // todo: forward delta like we are doing with puts
                vec![UpdateData::Delta(delta)]
            }
        };

        let current_state = match self.state_store.get(&key).await {
            Ok(s) => s,
            Err(StateStoreError::MissingContract(_)) => {
                return Err(ExecutorError::request(StdContractError::MissingContract {
                    key: key.into(),
                }));
            }
            Err(StateStoreError::Any(err)) => return Err(ExecutorError::other(err)),
        };

        for (id, state) in related_contracts
            .states()
            .filter_map(|(id, c)| c.map(|c| (id, c)))
        {
            updates.push(UpdateData::RelatedState {
                related_to: id,
                state,
            });
        }

        let updated_state = match self
            .attempt_state_update(&params, &current_state, &key, &updates)
            .await?
        {
            Either::Left(s) => s,
            Either::Right(mut r) => {
                let Some(c) = r.pop() else {
                    // this branch should be unreachable since attempt_state_update should only
                    return Err(ExecutorError::internal_error());
                };
                return Err(ExecutorError::request(StdContractError::MissingRelated {
                    key: c.contract_instance_id,
                }));
            }
        };
        Ok(updated_state)
    }
}

#[async_trait::async_trait]
impl ContractExecutor for Executor<crate::contract::MockRuntime> {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        _fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError> {
        let Some(parameters) = self
            .state_store
            .get_params(&key)
            .await
            .map_err(ExecutorError::other)?
        else {
            return Err(ExecutorError::other(format!(
                "missing parameters for contract {key}"
            )));
        };
        let Ok(state) = self.state_store.get(&key).await else {
            return Err(ExecutorError::other(format!(
                "missing state for contract {key}"
            )));
        };
        let contract = self
            .runtime
            .contract_store
            .fetch_contract(&key, &parameters);
        Ok((state, contract))
    }

    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ExecutorError> {
        self.runtime
            .contract_store
            .store_contract(contract)
            .map_err(ExecutorError::other)?;
        Ok(())
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        code: Option<ContractContainer>,
    ) -> Result<WrappedState, ExecutorError> {
        // todo: instead allow to perform mutations per contract based on incoming value so we can track
        // state values over the network
        match (state, code) {
            (Either::Left(incoming_state), Some(contract)) => {
                self.state_store
                    .store(key, incoming_state.clone(), contract.params().into_owned())
                    .await
                    .map_err(ExecutorError::other)?;

                let request = PutContract {
                    contract,
                    state: incoming_state.clone(),
                    related_contracts,
                };
                let _op: Result<operations::put::PutResult, _> = self.op_request(request).await;

                return Ok(incoming_state);
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        contract::MockRuntime,
        runtime::{ContractStore, StateStore},
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_dir = tempfile::tempdir()?;
        let contract_store = ContractStore::new(tmp_dir.path().join("executor-test"), MAX_SIZE)?;
        let state_store = StateStore::new(Storage::new(None).await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            state_store,
            || {
                counter += 1;
                Ok(())
            },
            OperationMode::Local,
            MockRuntime { contract_store },
            None,
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
