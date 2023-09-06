//! Contract executor.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use blake3::traits::digest::generic_array::GenericArray;
use locutus_runtime::prelude::*;
use locutus_stdlib::client_api::HostResponse::DelegateResponse;
use locutus_stdlib::client_api::{
    ClientError, ClientRequest, ContractError as CoreContractError, ContractRequest,
    ContractResponse, DelegateError as CoreDelegateError, DelegateRequest, HostResponse,
    RequestError,
};
use tokio::sync::mpsc::UnboundedSender;

use crate::node::{OpManager, P2pBridge};
#[cfg(any(
    not(feature = "local-mode"),
    feature = "network-mode",
    all(not(feature = "local-mode"), not(feature = "network-mode"))
))]
use crate::operations::get::GetResult;
use crate::operations::{self, op_trait::Operation};
use crate::{either::Either, ClientId, DynError, HostResult, Storage};

type Error = Either<Box<RequestError>, DynError>;
type Response = Result<HostResponse, Error>;

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

/// A WASM executor which will run any contracts, delegates, etc. registered.
///
/// This executor will monitor the store directories and databases to detect state changes.
/// Consumers of the executor are required to poll for new changes in order to be notified
/// of changes or can alternatively use the notification channel.
pub struct Executor {
    #[cfg(any(
        all(feature = "local-mode", feature = "network-mode"),
        all(not(feature = "local-mode"), not(feature = "network-mode")),
    ))]
    mode: OperationMode,
    runtime: Runtime,
    state_store: StateStore<Storage>,
    update_notifications: HashMap<ContractKey, Vec<(ClientId, UnboundedSender<HostResult>)>>,
    subscriber_summaries: HashMap<ContractKey, HashMap<ClientId, Option<StateSummary<'static>>>>,
}

impl Executor {
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
                return Err(Box::new(RequestError::from(CoreContractError::Subscribe {
                    key,
                    cause: format!("Peer {cli_id} already subscribed"),
                })));
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
            .handle_request(
                cli_id,
                ContractRequest::Put {
                    contract,
                    state,
                    related_contracts,
                }
                .into(),
                None,
            )
            .await
        {
            match err {
                Either::Left(err) => tracing::error!("req error: {err}"),
                Either::Right(err) => tracing::error!("other error: {err}"),
            }
        }
    }

    pub async fn handle_request(
        &mut self,
        id: ClientId,
        req: ClientRequest<'static>,
        updates: Option<UnboundedSender<Result<HostResponse, ClientError>>>,
    ) -> Response {
        match req {
            ClientRequest::ContractOp(op) => self.contract_op(op, id, updates).await,
            ClientRequest::DelegateOp(op) => self.delegate_op(op),
            ClientRequest::Disconnect { cause } => {
                if let Some(cause) = cause {
                    tracing::info!("disconnecting cause: {cause}");
                }
                Err(Either::Left(Box::new(RequestError::Disconnect)))
            }
            ClientRequest::GenerateRandData { bytes } => {
                let mut output = vec![0; bytes];
                locutus_runtime::util::generate_random_bytes(&mut output);
                Ok(HostResponse::GenerateRandData(output))
            }
        }
    }

    async fn contract_op(
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
                    updates.ok_or_else(|| Either::Right("missing update channel".into()))?;
                self.register_contract_notifier(key.clone(), cli_id, updates, summary)
                    .map_err(Either::Left)?;
                let res = self.perform_contract_get(false, key.clone()).await?;
                #[cfg(any(
                    all(not(feature = "local-mode"), not(feature = "network-mode")),
                    all(feature = "local-mode", feature = "network-mode"),
                    all(feature = "network-mode", not(feature = "network-mode"))
                ))]
                {
                    Self::subscribe(key).await?;
                }
                // by default a subscribe op has an implicit get
                Ok(res)
            }
        }
    }

    async fn perform_contract_put(
        &mut self,
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'_>,
    ) -> Response {
        // FIXME: in net node, we don't allow puts for existing contract states
        //        if it hits a node which already has it it will get rejected
        //
        //        if there is a conflict, resolve the conflict to see which
        //        is the outdated state:
        //          1. through the arbitraur mechanism
        //          2. a new func which compared two summaries and gives the most fresh
        //          3. could convert into an update with the full state
        //        you can request to several nodes and determine which node has a fresher ver

        let key = contract.key();
        let params = contract.params();

        self.verify_and_store_contract(state.clone(), contract, related_contracts)
            .await?;

        self.send_update_notification(&key, &params, &state)
            .await
            .map_err(|_| {
                Either::Left(Box::new(
                    CoreContractError::Put {
                        key: key.clone(),
                        cause: "failed while sending notifications".to_owned(),
                    }
                    .into(),
                ))
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
                .map_err(|err| Either::Right(err.into()))?
                .ok_or_else(|| {
                    let e = RequestError::ContractError(CoreContractError::Update {
                        cause: format!("missing contract parameters: {key}"),
                        key: key.clone(),
                    });
                    Either::Right(e.into())
                })?
        };
        let new_state = {
            let state = self
                .state_store
                .get(&key)
                .await
                .map_err(Into::into)
                .map_err(Either::Right)?
                .clone();
            let mut retrieved_contracts = Vec::new();
            retrieved_contracts.push(data);

            let start = Instant::now();
            loop {
                let update_modification = self
                    .runtime
                    .update_state(&key, &parameters, &state, &retrieved_contracts)
                    .map_err(|err| {
                        let is_contract_exec_error = err.is_contract_exec_error();
                        match is_contract_exec_error {
                            true => Either::Left(Box::new(
                                CoreContractError::Update {
                                    key: key.clone(),
                                    cause: format!("{err}"),
                                }
                                .into(),
                            )),
                            _ => Either::Right(err.into()),
                        }
                    })?;
                let UpdateModification {
                    new_state, related, ..
                } = update_modification;
                if let Some(new_state) = new_state {
                    let new_state = WrappedState::new(new_state.into_bytes());
                    self.state_store
                        .update(&key, new_state.clone())
                        .await
                        .map_err(|err| Either::Right(err.into()))?;
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
                                        Self::subscribe(id.into()).await?;
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
                                        Self::subscribe(id.into()).await?;
                                    }
                                }
                            }
                            Err(other_err) => {
                                let _ = mode;
                                return Err(Either::Right(other_err.into()));
                            }
                        }
                    }
                    if retrieved_contracts.len() == required_contracts {
                        // try running again with all the related contracts retrieved
                        continue;
                    } else if start.elapsed() > Duration::from_secs(10) {
                        /* make this timeout configurable, and anyway should be controlled globally*/
                        return Err(Either::Left(RequestError::Timeout.into()));
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
            .map_err(Into::into)
            .map_err(Either::Right)?;
        self.send_update_notification(&key, &parameters, &new_state)
            .await?;
        // TODO: in network mode, wait at least for one confirmation
        //       when a node receives a delta from updates, run the update themselves
        //       and send back confirmation
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
                    .map_err(Either::Left)?
                else {
                    return Err(Either::Right(Box::new(RequestError::from(
                        CoreContractError::Get {
                            key: key.clone(),
                            cause: "Missing contract or parameters".into(),
                        },
                    ))));
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
                return Err(Either::Right(Box::new(RequestError::from(
                    CoreContractError::Get {
                        key: key.clone(),
                        cause: "Missing contract or parameters".into(),
                    },
                ))));
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
            Err(StateStoreError::MissingContract(_)) => Err(Either::Right(Box::new(
                RequestError::from(CoreContractError::Get {
                    key,
                    cause: "missing contract state".into(),
                }),
            ))),
            Err(err) => Err(Either::Right(Box::new(RequestError::from(
                CoreContractError::Get {
                    key,
                    cause: format!("{err}"),
                },
            )))),
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
    ) -> Result<Option<ContractContainer>, Error> {
        loop {
            if let Ok(Some(contract)) = self.get_contract_locally(&key).await {
                break Ok(Some(contract));
            } else {
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
    async fn subscribe(key: ContractKey) -> Result<(), Error> {
        let request = SubscribeContract { key };
        let op: operations::subscribe::SubscribeOp =
            op_request(request).await.map_err(Either::Right)?;
        let _sub: operations::subscribe::SubscribeResult =
            op.try_into().map_err(Into::into).map_err(Either::Right)?;
        Ok(())
    }

    async fn verify_and_store_contract<'a>(
        &mut self,
        state: WrappedState,
        trying_container: ContractContainer,
        mut related_contracts: RelatedContracts<'a>,
    ) -> Result<(), Error> {
        let key = trying_container.key();
        let params = trying_container.params();

        #[cfg(all(feature = "local-mode", not(feature = "network-mode")))]
        #[inline]
        async fn get_local_contract(
            store: &StateStore<Storage>,
            id: &ContractInstanceId,
            related: &mut Option<State<'_>>,
        ) -> Result<(), Either<Box<RequestError>, DynError>> {
            let Ok(contract) = store.get(&(*id).into()).await else {
                return Err(Either::Right(
                    CoreContractError::MissingRelated { key: *id }.into(),
                ));
            };
            let state: &[u8] = unsafe {
                // Safety: this is fine since this will never scape this scope
                std::mem::transmute::<&[u8], &'_ [u8]>(contract.as_ref())
            };
            *related = Some(State::from(state));
            Ok(())
        }

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
                    .map_err(Into::into)
                    .map_err(Either::Right)?;
            }

            let result = self
                .runtime
                .validate_state(
                    &trying_key,
                    &trying_params,
                    &trying_state,
                    &related_contracts,
                )
                .map_err(Into::into)
                .map_err(|err| {
                    let _ = self.runtime.contract_store.remove_contract(&trying_key);
                    Either::Right(err)
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
                                get_local_contract(&self.state_store, id, related).await?;
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
                return Err(Either::Left(Box::new(
                    CoreContractError::Put {
                        key: trying_key.clone(),
                        cause: "not valid".to_owned(),
                    }
                    .into(),
                )));
            }

            self.state_store
                .store(
                    trying_key.clone(),
                    trying_state.clone(),
                    trying_params.clone(),
                )
                .await
                .map_err(|err| Either::Right(err.into()))?;
            if trying_key != original_key {
                trying_key = original_key.clone();
                trying_params = original_params.clone();
                trying_state = original_state.clone();
                continue;
            }
            break;
        }
        if iterations == DEPENDENCY_CYCLE_LIMIT_GUARD {
            return Err(Either::Left(Box::new(
                CoreContractError::MissingRelated {
                    key: original_key.id(),
                }
                .into(),
            )));
        }
        Ok(())
    }

    fn delegate_op(&mut self, req: DelegateRequest<'_>) -> Response {
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
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::error!("failed registering delegate `{key}`: {err}");
                        Err(Either::Left(Box::new(
                            CoreDelegateError::RegisterError(key).into(),
                        )))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
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
            } => match self.runtime.inbound_app_message(
                &key,
                &params,
                vec![InboundDelegateMsg::GetSecretRequest(get_request)],
            ) {
                Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                Err(err) if err.delegate_is_missing() => {
                    tracing::error!("delegate not found `{key}`");
                    Err(Either::Left(Box::new(
                        CoreDelegateError::Missing(key).into(),
                    )))
                }
                Err(err) => Err(Either::Right(
                    format!("uncontrolled error while getting secret for `{key}`: {err}").into(),
                )),
            },
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(HostResponse::DelegateResponse { key, values }),
                    Err(err) if err.is_delegate_exec_error() | err.is_execution_error() => {
                        tracing::error!("failed processing messages for delegate `{key}`: {err}");
                        Err(Either::Left(Box::new(
                            CoreDelegateError::ExecutionError(format!("{err}")).into(),
                        )))
                    }
                    Err(err) if err.delegate_is_missing() => {
                        tracing::error!("delegate not found `{key}`");
                        Err(Either::Left(Box::new(
                            CoreDelegateError::Missing(key).into(),
                        )))
                    }
                    Err(err) if err.secret_is_missing() => {
                        tracing::error!("secret not found `{key}`");
                        Err(Either::Left(Box::new(
                            CoreDelegateError::MissingSecret {
                                key,
                                secret: err.get_secret_id(),
                            }
                            .into(),
                        )))
                    }
                    Err(err) => {
                        tracing::error!("failed executing delegate `{key}`: {err}");
                        Err(Either::Right(
                            format!("uncontrolled error while executing `{key}`").into(),
                        ))
                    }
                }
            }
        }
    }

    async fn send_update_notification<'a>(
        &mut self,
        key: &ContractKey,
        params: &Parameters<'a>,
        new_state: &WrappedState,
    ) -> Result<(), Error> {
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
                                true => Either::Left(Box::new(
                                    CoreContractError::Put {
                                        key: key.clone(),
                                        cause: format!("{err}"),
                                    }
                                    .into(),
                                )),
                                _ => Either::Right(err.into()),
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
        all(not(feature = "local-mode"), not(feature = "network-mode")),
        all(feature = "local-mode", feature = "network-mode"),
        feature = "network-mode",
    ))]
    #[inline]
    async fn local_state_or_from_network(
        &self,
        id: &ContractInstanceId,
    ) -> Result<Either<WrappedState, operations::get::GetResult>, Error> {
        if let Ok(contract) = self.state_store.get(&(*id).into()).await {
            return Ok(Either::Left(contract));
        };
        let request: GetContract = GetContract {
            key: (*id).into(),
            fetch_contract: true,
        };
        let op: operations::get::GetOp = op_request(request).await.map_err(Either::Right)?;
        let get_result: operations::get::GetResult =
            op.try_into().map_err(Into::into).map_err(Either::Right)?;
        Ok(Either::Right(get_result))
    }

    async fn get_contract_locally(
        &self,
        key: &ContractKey,
    ) -> Result<Option<ContractContainer>, Box<RequestError>> {
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
