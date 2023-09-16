//! Contract executor.

use std::collections::HashMap;
use std::fmt::Display;
use std::hint::unreachable_unchecked;
use std::time::{Duration, Instant};

use blake3::traits::digest::generic_array::GenericArray;
use either::Either;
use locutus_runtime::prelude::*;
use locutus_stdlib::client_api::{
    ClientError, ClientRequest, ContractError as CoreContractError, ContractRequest,
    ContractResponse, DelegateError as CoreDelegateError, DelegateRequest,
    HostResponse::{self, DelegateResponse},
    RequestError,
};
use tokio::sync::mpsc::UnboundedSender;

#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
use crate::operations::get::GetResult;
use crate::{
    node::{OpManager, P2pBridge},
    operations::{self, op_trait::Operation},
    ClientId, DynError, HostResult, NodeConfig,
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

type Response = Result<HostResponse, ExecutorError>;

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationMode {
    /// Run the node in local-only mode. Useful for development purposes.
    Local,
    /// Standard operation mode.
    Network,
}

#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
// for now just mocking up requests to the network
async fn op_request<Op, M>(_request: M) -> Result<Op, DynError>
where
    Op: Operation<P2pBridge>,
    M: ComposeNetworkMessage<Op>,
{
    todo!()
}

#[allow(unused)]
trait ComposeNetworkMessage<Op>
where
    Self: Sized,
    Op: Operation<P2pBridge>,
{
    fn get_message(self, manager: &OpManager) -> Op::Message {
        todo!()
    }
}

#[allow(unused)]
struct GetContract {
    key: ContractKey,
    fetch_contract: bool,
}

impl ComposeNetworkMessage<operations::get::GetOp> for GetContract {}

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
pub(crate) trait ContractExecutor: Send + Sync + 'static {
    async fn fetch_contract(
        &mut self,
        key: ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError>;
    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ContractError>;
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
    update_notifications: HashMap<ContractKey, Vec<(ClientId, UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
    delegate_attested_ids: HashMap<DelegateKey, Vec<ContractInstanceId>>,
}

impl Executor<Runtime> {
    pub async fn from_config(config: NodeConfig) -> Result<Self, DynError> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let static_conf = crate::config::Config::get_static_conf();

        let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();

        let contract_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("contracts"))
            .unwrap_or_else(|| static_conf.config_paths.local_contracts_dir());
        let contract_store = ContractStore::new(contract_dir, MAX_SIZE)?;

        let delegate_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("delegates"))
            .unwrap_or_else(|| static_conf.config_paths.local_delegates_dir());
        let delegate_store = DelegateStore::new(delegate_dir, MAX_SIZE)?;

        let secrets_dir = config
            .node_data_dir
            .as_ref()
            .map(|d| d.join("secrets"))
            .unwrap_or_else(|| static_conf.config_paths.local_secrets_dir());
        let secret_store = SecretsStore::new(secrets_dir)?;

        Executor::new(
            contract_store,
            delegate_store,
            secret_store,
            state_store,
            || {
                crate::util::set_cleanup_on_exit().unwrap();
            },
            OperationMode::Local,
        )
        .await
    }

    #[allow(unused_variables)]
    pub async fn new(
        contract_store: ContractStore,
        delegate_store: DelegateStore,
        secret_store: SecretsStore,
        contract_state: StateStore<Storage>,
        ctrl_handler: impl FnOnce(),
        mode: OperationMode,
    ) -> Result<Self, DynError> {
        ctrl_handler();

        Ok(Self {
            #[cfg(any(
                all(feature = "local-mode", feature = "network-mode"),
                all(not(feature = "local-mode"), not(feature = "network-mode")),
            ))]
            mode,
            runtime: Runtime::build(contract_store, delegate_store, secret_store, false).unwrap(),
            state_store: contract_state,
            update_notifications: HashMap::default(),
            subscriber_summaries: HashMap::default(),
            delegate_attested_ids: HashMap::default(),
        })
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
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
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
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
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

        if self.get_local_contract(&key.id()).await.is_ok() {
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
            let op: operations::update::UpdateOp =
                op_request(request).await.map_err(ExecutorError::other)?;
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
                let Some(contract) = self
                    .get_contract_locally(&key)
                    .await
                    .map_err(ExecutorError::request)?
                else {
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
                            CoreContractError::MissingRelated { key: key.id() },
                        )));
                    }
                }
                match self
                    .local_state_or_from_network(&key.clone().into())
                    .await?
                {
                    Either::Right(GetResult { state, contract }) => {
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

    #[cfg(any(
        not(feature = "local-mode"),
        feature = "network-mode",
        all(not(feature = "local-mode"), not(feature = "network-mode"))
    ))]
    async fn subscribe(&self, key: ContractKey) -> Result<(), ExecutorError> {
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
        let op: operations::subscribe::SubscribeOp =
            op_request(request).await.map_err(ExecutorError::other)?;
        let _sub: operations::subscribe::SubscribeResult =
            op.try_into().map_err(ExecutorError::other)?;
        Ok(())
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
                                        trying_key = (*id).into();
                                        trying_params = result.contract.params();
                                        trying_state = result.state;
                                        trying_contract = Some(result.contract);
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
                key: original_key.id(),
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
            let mut failures = arrayvec::ArrayVec::<_, 32>::new();
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
                    let _ = failures.try_push(*peer_key);
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

    #[cfg(any(
        not(feature = "local-mode"),
        feature = "network-mode",
        all(not(feature = "local-mode"), not(feature = "network-mode")),
    ))]
    #[inline]
    async fn local_state_or_from_network(
        &self,
        id: &ContractInstanceId,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, ExecutorError> {
        if let Ok(contract) = self.state_store.get(&(*id).into()).await {
            return Ok(Either::Left(contract));
        };
        let request: GetContract = GetContract {
            key: (*id).into(),
            fetch_contract: true,
        };
        let op: operations::get::GetOp = op_request(request).await.map_err(ExecutorError::other)?;
        let get_result: operations::get::GetResult = op.try_into().map_err(ExecutorError::other)?;
        Ok(Either::Right(get_result))
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, RequestError> {
        let Some(parameters) = self.state_store.get_params(key).await.map_err(|e| {
            RequestError::ContractError(CoreContractError::Update {
                cause: format!("{e}"),
                key: key.clone(),
            })
        })?
        else {
            return Ok(None);
        };
        let Some(contract) = self.runtime.contract_store.fetch_contract(key, &parameters) else {
            return Ok(None);
        };
        Ok(Some(contract))
    }
}

#[cfg(test)]
impl Executor<crate::contract::MockRuntime> {
    pub async fn new_mock() -> Result<Self, DynError> {
        todo!()
    }

    pub async fn handle_request<'a>(
        &mut self,
        _id: ClientId,
        _req: ClientRequest<'a>,
        _updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> Response {
        todo!()
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
                unsafe { unreachable_unchecked() }
            }
        }
    }

    async fn store_contract(&mut self, contract: ContractContainer) -> Result<(), ContractError> {
        self.runtime.contract_store.store_contract(contract)
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl ContractExecutor for Executor<crate::contract::MockRuntime> {
    async fn fetch_contract(
        &mut self,
        _key: ContractKey,
        _fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), ExecutorError> {
        todo!()
    }

    async fn store_contract(&mut self, _contract: ContractContainer) -> Result<(), ContractError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use locutus_runtime::{ContractStore, StateStore};

    #[tokio::test(flavor = "multi_thread")]
    async fn local_node_handle() -> Result<(), Box<dyn std::error::Error>> {
        const MAX_SIZE: i64 = 10 * 1024 * 1024;
        const MAX_MEM_CACHE: u32 = 10_000_000;
        let tmp_path = std::env::temp_dir().join("locutus-test");
        let contract_store = ContractStore::new(tmp_path.join("executor-test"), MAX_SIZE)?;
        let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();
        let mut counter = 0;
        Executor::new(
            contract_store,
            DelegateStore::default(),
            SecretsStore::default(),
            state_store,
            || {
                counter += 1;
            },
            OperationMode::Local,
        )
        .await
        .expect("local node with handle");

        assert_eq!(counter, 1);
        Ok(())
    }
}
