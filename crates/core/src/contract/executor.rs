//! Contract executor.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blake3::traits::digest::generic_array::GenericArray;
use either::Either;
use freenet_stdlib::client_api::{
    ClientError, ClientRequest, ContractError as CoreContractError, ContractRequest,
    ContractResponse, DelegateError as CoreDelegateError, DelegateRequest,
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
    node::NodeConfig,
    operations::{self, Operation},
    DynError,
};

use super::storages::Storage;

pub struct ExecutorError(Either<Box<RequestError>, DynError>);

impl ExecutorError {
    pub fn other(value: impl Into<DynError>) -> Self {
        Self(Either::Right(value.into()))
    }

    fn request(value: impl Into<RequestError>) -> Self {
        Self(Either::Left(Box::new(value.into())))
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

impl From<ExecutorError> for DynError {
    fn from(value: ExecutorError) -> Self {
        match value.0 {
            Either::Left(l) => l as DynError,
            Either::Right(r) => r,
        }
    }
}

impl From<ExecutorError> for anyhow::Error {
    fn from(value: ExecutorError) -> Self {
        match value.0 {
            Either::Left(l) => anyhow::Error::new(*l),
            Either::Right(r) => anyhow::Error::msg(r),
        }
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

pub(crate) struct ExecutorToEventLoopChannel<End: sealed::ChannelHalve> {
    op_manager: Arc<OpManager>,
    end: End,
}

pub(crate) fn executor_channel(
    op_manager: Arc<OpManager>,
) -> (
    ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
    ExecutorToEventLoopChannel<ExecutorHalve>,
) {
    let (sender, _b) = mpsc::channel(1);
    let listener_halve = ExecutorToEventLoopChannel {
        op_manager: op_manager.clone(),
        end: NetworkEventListenerHalve,
    };
    let sender_halve = ExecutorToEventLoopChannel {
        op_manager: op_manager.clone(),
        end: ExecutorHalve { sender },
    };
    (listener_halve, sender_halve)
}

impl ExecutorToEventLoopChannel<ExecutorHalve> {
    async fn send_to_event_loop<Op, T>(&mut self, message: T) -> Result<(), DynError>
    where
        T: ComposeNetworkMessage<Op>,
        Op: Operation + Send + 'static,
    {
        let op = message.initiate_op(&self.op_manager);
        self.end.sender.send(*op.id()).await?;
        <T as ComposeNetworkMessage<Op>>::resume_op(op, &self.op_manager).await?;
        Ok(())
    }
}

impl ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    pub async fn transaction_from_executor(&mut self) -> Transaction {
        todo!()
    }

    pub async fn response(&mut self, _result: OpEnum) {
        todo!()
    }
}

impl Clone for ExecutorToEventLoopChannel<NetworkEventListenerHalve> {
    fn clone(&self) -> Self {
        todo!()
    }
}

pub(crate) struct NetworkEventListenerHalve;
pub(crate) struct ExecutorHalve {
    sender: mpsc::Sender<Transaction>,
}

mod sealed {
    use super::{ExecutorHalve, NetworkEventListenerHalve};
    pub(crate) trait ChannelHalve {}
    impl ChannelHalve for NetworkEventListenerHalve {}
    impl ChannelHalve for ExecutorHalve {}
}

#[allow(unused)]
#[async_trait::async_trait]
trait ComposeNetworkMessage<Op>
where
    Self: Sized,
    Op: Operation + Send + 'static,
{
    fn initiate_op(self, op_manager: &OpManager) -> Op {
        todo!()
    }

    async fn resume_op(op: Op, op_manager: &OpManager) -> Result<Transaction, OpError> {
        todo!()
    }
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

    async fn resume_op(
        op: operations::get::GetOp,
        op_manager: &OpManager,
    ) -> Result<Transaction, OpError> {
        let id = *op.id();
        operations::get::request_get(op_manager, op, None).await?;
        Ok(id)
    }
}

#[allow(unused)]
struct SubscribeContract {
    key: ContractKey,
}

impl ComposeNetworkMessage<operations::subscribe::SubscribeOp> for SubscribeContract {}

#[allow(unused)]
struct UpdateContract {
    key: ContractKey,
    new_state: WrappedState,
}

impl ComposeNetworkMessage<operations::update::UpdateOp> for UpdateContract {}

#[async_trait::async_trait]
pub(crate) trait ContractExecutor: Send + 'static {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError>;
    async fn store_contract(
        &mut self,
        contract: ContractContainer,
    ) -> Result<(), crate::runtime::ContractError>;

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        params: Option<Parameters<'_>>,
    ) -> Result<WrappedState, crate::runtime::ContractError>;
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
    state_store: StateStore<Storage>,
    update_notifications: HashMap<ContractKey, Vec<(ClientId, mpsc::UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
    delegate_attested_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,
    #[cfg(any(
        not(feature = "local-mode"),
        feature = "network-mode",
        all(not(feature = "local-mode"), not(feature = "network-mode"))
    ))]
    event_loop_channel: Option<ExecutorToEventLoopChannel<ExecutorHalve>>,
}

#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
impl Executor<Runtime> {
    pub(crate) fn event_loop_channel(
        &mut self,
        channel: ExecutorToEventLoopChannel<ExecutorHalve>,
    ) {
        self.event_loop_channel = Some(channel);
    }

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
        let op: operations::subscribe::SubscribeOp = self
            .op_request(request)
            .await
            .map_err(ExecutorError::other)?;
        let _sub: operations::subscribe::SubscribeResult =
            op.try_into().map_err(ExecutorError::other)?;
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
        let op: operations::get::GetOp = self
            .op_request(request)
            .await
            .map_err(ExecutorError::other)?;
        let get_result: operations::get::GetResult = op.try_into().map_err(ExecutorError::other)?;
        Ok(Either::Right(get_result))
    }

    // FIXME: must add suspension and resuming when doing this,
    // otherwise it may be possible to end up in a deadlock waiting for a tree of contract
    // dependencies to be resolved
    async fn op_request<Op, M>(&mut self, request: M) -> Result<Op, DynError>
    where
        Op: Operation + Send + 'static,
        M: ComposeNetworkMessage<Op>,
    {
        debug_assert!(self.event_loop_channel.is_some());
        let channel = match self.event_loop_channel.as_mut() {
            Some(ch) => ch,
            None => {
                // Safety: this should be always set if network mode is ambiguous
                // or using network mode unequivocally
                unsafe { std::hint::unreachable_unchecked() }
            }
        };
        channel.send_to_event_loop(request).await?;
        todo!()
    }
}

impl<R> Executor<R> {
    pub async fn new(
        state_store: StateStore<Storage>,
        ctrl_handler: impl FnOnce() -> Result<(), DynError>,
        mode: OperationMode,
        runtime: R,
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
            #[cfg(any(
                not(feature = "local-mode"),
                feature = "network-mode",
                all(not(feature = "local-mode"), not(feature = "network-mode"))
            ))]
            event_loop_channel: None,
        })
    }

    async fn get_stores(
        config: &NodeConfig,
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

        let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();

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
}

impl Executor<Runtime> {
    pub async fn from_config(config: NodeConfig) -> Result<Self, DynError> {
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
                return Err(RequestError::from(CoreContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed"),
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
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, ClientError>>>,
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
        updates: Option<mpsc::UnboundedSender<Result<HostResponse, ClientError>>>,
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
                let res = self.perform_contract_get(false, key.clone()).await?;
                #[cfg(any(
                    all(not(feature = "local-mode"), not(feature = "network-mode")),
                    all(feature = "local-mode", feature = "network-mode"),
                    all(feature = "network-mode", not(feature = "network-mode"))
                ))]
                {
                    self.subscribe(key).await?;
                }
                // by default a subscribe op has an implicit get
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
                        Err(ExecutorError::other(CoreDelegateError::RegisterError(key)))
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
                    Err(err) if err.delegate_is_missing() => {
                        tracing::error!("delegate not found `{key}`");
                        Err(ExecutorError::request(CoreDelegateError::Missing(key)))
                    }
                    Err(err) => Err(ExecutorError::other(format!(
                        "uncontrolled error while getting secret for `{key}`: {err}"
                    ))),
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
                    Err(err) if err.is_delegate_exec_error() | err.is_execution_error() => {
                        tracing::error!("failed processing messages for delegate `{key}`: {err}");
                        Err(ExecutorError::request(CoreDelegateError::ExecutionError(
                            format!("{err}"),
                        )))
                    }
                    Err(err) if err.delegate_is_missing() => {
                        tracing::error!("delegate not found `{key}`");
                        Err(ExecutorError::request(CoreDelegateError::Missing(key)))
                    }
                    Err(err) if err.secret_is_missing() => {
                        tracing::error!("secret not found `{key}`");
                        Err(ExecutorError::request(CoreDelegateError::MissingSecret {
                            key,
                            secret: err.get_secret_id(),
                        }))
                    }
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
                ExecutorError::request(CoreContractError::Put {
                    key: key.clone(),
                    cause: "failed while sending notifications".to_owned(),
                })
            })?;
        Ok(ContractResponse::PutResponse { key }.into())
    }

    async fn perform_contract_update(
        &mut self,
        key: ContractKey,
        data: UpdateData<'_>,
    ) -> Response {
        let parameters = {
            self.state_store
                .get_params(&key)
                .await
                .map_err(ExecutorError::other)?
                .ok_or_else(|| {
                    RequestError::ContractError(CoreContractError::Update {
                        cause: format!("missing contract parameters: {key}"),
                        key: key.clone(),
                    })
                })?
        };
        let new_state = {
            let state = self
                .state_store
                .get(&key)
                .await
                .map_err(ExecutorError::other)?
                .clone();
            let mut retrieved_contracts = Vec::new();
            retrieved_contracts.push(data);

            let start = Instant::now();
            loop {
                let update_modification =
                    match self
                        .runtime
                        .update_state(&key, &parameters, &state, &retrieved_contracts)
                    {
                        Err(err) if err.is_contract_exec_error() => {
                            return Err(ExecutorError::request(CoreContractError::Update {
                                key: key.clone(),
                                cause: format!("{err}"),
                            }));
                        }
                        Err(err) => return Err(ExecutorError::other(err)),
                        Ok(result) => result,
                    };
                let UpdateModification {
                    new_state, related, ..
                } = update_modification;
                if let Some(new_state) = new_state {
                    let new_state = WrappedState::new(new_state.into_bytes());
                    self.state_store
                        .update(&key, new_state.clone())
                        .await
                        .map_err(ExecutorError::other)?;
                    break new_state;
                } else if !related.is_empty() {
                    // some required contracts are missing
                    let required_contracts = related.len() + 1;
                    for RelatedContract {
                        contract_instance_id: id,
                        mode,
                    } in related
                    {
                        match self.state_store.get(&id.into()).await {
                            Ok(state) => {
                                // in this case we are already subscribed to and are updating this contract,
                                // we can try first with the existing value
                                retrieved_contracts.push(UpdateData::RelatedState {
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
                                        let contract = contract.unwrap(); // fixme: deal with unwrap
                                        self.verify_and_store_contract(
                                            state.clone(),
                                            contract,
                                            RelatedContracts::default(),
                                        )
                                        .await?;
                                        state
                                    }
                                };
                                retrieved_contracts.push(UpdateData::State(state.into()));
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
                                    Either::Left(state) => state,
                                    Either::Right(GetResult { state, contract }) => {
                                        self.verify_and_store_contract(
                                            state.clone(),
                                            contract,
                                            RelatedContracts::default(),
                                        )
                                        .await?;
                                        state
                                    }
                                };
                                retrieved_contracts.push(UpdateData::State(state.into()));
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
                    if retrieved_contracts.len() == required_contracts {
                        // try running again with all the related contracts retrieved
                        continue;
                    } else if start.elapsed() > Duration::from_secs(10) {
                        /* make this timeout configurable, and anyway should be controlled globally*/
                        return Err(RequestError::Timeout.into());
                    }
                } else {
                    // state wasn't updated
                    break state;
                }
            }
        };
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
            let op: operations::update::UpdateOp = self
                .op_request(request)
                .await
                .map_err(ExecutorError::other)?;
            let _update: operations::update::UpdateResult =
                op.try_into().map_err(ExecutorError::other)?;
        }

        Ok(ContractResponse::UpdateResponse { key, summary }.into())
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
                    return Err(ExecutorError::other(RequestError::from(
                        CoreContractError::Get {
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
                return Err(ExecutorError::other(RequestError::from(
                    CoreContractError::Get {
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
            Err(StateStoreError::MissingContract(_)) => Err(ExecutorError::other(
                RequestError::from(CoreContractError::Get {
                    key,
                    cause: "Missing contract state".into(),
                }),
            )),
            Err(err) => Err(ExecutorError::other(RequestError::from(
                CoreContractError::Get {
                    key,
                    cause: format!("{err}"),
                },
            ))),
        }
    }

    #[cfg(any(
        not(feature = "local-mode"),
        feature = "network-mode",
        all(not(feature = "local-mode"), not(feature = "network-mode"))
    ))]
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
                            CoreContractError::MissingRelated { key: *key.id() },
                        )));
                    }
                }
                match self
                    .local_state_or_from_network(&key.clone().into())
                    .await?
                {
                    Either::Right(GetResult { state, contract }) => {
                        let contract = contract.unwrap(); // fixme: deal with unwrap
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

    async fn get_local_contract(
        &self,
        id: &ContractInstanceId,
    ) -> Result<State<'static>, Either<Box<RequestError>, DynError>> {
        let Ok(contract) = self.state_store.get(&(*id).into()).await else {
            return Err(Either::Right(
                CoreContractError::MissingRelated { key: *id }.into(),
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
                                        let contract = result.contract.unwrap(); // fixme: deal with unwrap
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
                return Err(ExecutorError::request(CoreContractError::Put {
                    key: trying_key.clone(),
                    cause: "not valid".to_owned(),
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
            return Err(ExecutorError::request(CoreContractError::MissingRelated {
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
                            let is_contract_exec_error = err.is_contract_exec_error();
                            match is_contract_exec_error {
                                true => ExecutorError::request(CoreContractError::Put {
                                    key: key.clone(),
                                    cause: format!("{err}"),
                                }),
                                _ => ExecutorError::other(err),
                            }
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
    pub async fn new_mock(data_dir: &str) -> Result<Self, DynError> {
        let tmp_path = std::env::temp_dir().join(format!("freenet-executor-{data_dir}"));

        let contracts_data_dir = tmp_path.join("contracts");
        std::fs::create_dir_all(&contracts_data_dir).expect("directory created");
        let contract_store = ContractStore::new(contracts_data_dir, u16::MAX as i64)?;

        // uses inmemory SQLite
        let state_store = StateStore::new(Storage::new().await?, u16::MAX as u32).unwrap();

        let executor = Executor::new(
            state_store,
            || Ok(()),
            OperationMode::Local,
            super::MockRuntime { contract_store },
        )
        .await?;
        Ok(executor)
    }

    pub async fn handle_request<'a>(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'a>,
        _updates: Option<mpsc::UnboundedSender<Result<HostResponse, ClientError>>>,
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

    async fn store_contract(
        &mut self,
        contract: ContractContainer,
    ) -> Result<(), crate::runtime::ContractError> {
        self.runtime.contract_store.store_contract(contract)
    }

    async fn upsert_contract_state(
        &mut self,
        _key: ContractKey,
        _state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        _params: Option<Parameters<'_>>,
    ) -> Result<WrappedState, crate::runtime::ContractError> {
        todo!()
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

    async fn store_contract(
        &mut self,
        contract: ContractContainer,
    ) -> Result<(), crate::runtime::ContractError> {
        self.runtime.contract_store.store_contract(contract)?;
        Ok(())
    }

    async fn upsert_contract_state(
        &mut self,
        key: ContractKey,
        state: Either<WrappedState, StateDelta<'static>>,
        _related_contracts: RelatedContracts<'static>,
        params: Option<Parameters<'_>>,
    ) -> Result<WrappedState, crate::runtime::ContractError> {
        // todo: instead allow to perform mutations per contract based on incoming value so we can track
        // state values over the network
        match (state, params) {
            (Either::Left(state), Some(params)) => {
                self.state_store
                    .store(key, state.clone(), params.into_owned())
                    .await?;
                return Ok(state);
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
        let tmp_path = std::env::temp_dir().join("freenet-test");
        let contract_store = ContractStore::new(tmp_path.join("executor-test"), MAX_SIZE)?;
        let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            state_store,
            || {
                counter += 1;
                Ok(())
            },
            OperationMode::Local,
            MockRuntime { contract_store },
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
