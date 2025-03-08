//! Clients events related logic and type definitions. For example, receival of client events from applications throught the HTTP gateway.

use either::Either;
use freenet_stdlib::{
    client_api::{
        ClientError, ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse,
        QueryResponse,
    },
    prelude::*,
};
use futures::stream::FuturesUnordered;
use futures::{future::BoxFuture, FutureExt, StreamExt};
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{convert::Infallible, fmt::Debug};
use tracing::Instrument;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::contract::{ClientResponsesReceiver, ContractHandlerEvent};
use crate::message::{NodeEvent, QueryResult};
use crate::node::OpManager;
use crate::operations::{get, put, update, OpError};
use crate::{config::GlobalExecutor, contract::StoreResponse};

pub(crate) mod combinator;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub(crate) type BoxedClient = Box<dyn ClientEventsProxy + Send + 'static>;
pub type HostResult = Result<HostResponse, ClientError>;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ClientId(usize);

impl From<ClientId> for usize {
    fn from(val: ClientId) -> Self {
        val.0
    }
}

static CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

impl ClientId {
    pub const FIRST: Self = ClientId(0);

    pub fn next() -> Self {
        ClientId(CLIENT_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthToken(#[serde(deserialize_with = "AuthToken::deser_auth_token")] Arc<str>);

impl AuthToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn generate() -> AuthToken {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut token = [0u8; 32];
        rng.fill(&mut token);
        let token_str = bs58::encode(token).into_string();
        AuthToken::from(token_str)
    }
}

impl std::ops::Deref for AuthToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AuthToken {
    fn deser_auth_token<'de, D>(deser: D) -> Result<Arc<str>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as Deserialize>::deserialize(deser)?;
        Ok(value.into())
    }
}

impl From<String> for AuthToken {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

#[non_exhaustive]
pub struct OpenRequest<'a> {
    pub client_id: ClientId,
    pub request: Box<ClientRequest<'a>>,
    pub notification_channel: Option<UnboundedSender<HostResult>>,
    pub token: Option<AuthToken>,
}

impl Display for OpenRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "client request {{ client: {}, req: {} }}",
            &self.client_id, &*self.request
        )
    }
}

impl<'a> OpenRequest<'a> {
    pub fn into_owned(self) -> OpenRequest<'static> {
        OpenRequest {
            request: Box::new(self.request.into_owned()),
            ..self
        }
    }

    pub fn new(id: ClientId, request: Box<ClientRequest<'a>>) -> Self {
        Self {
            client_id: id,
            request,
            notification_channel: None,
            token: None,
        }
    }

    pub fn with_notification(mut self, ch: UnboundedSender<HostResult>) -> Self {
        self.notification_channel = Some(ch);
        self
    }

    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }
}

pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg>;

    /// Sends a response from the host to the client application.
    fn send(
        &mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<Result<(), ClientError>>;
}

/// Process client events.
pub async fn client_event_handling<ClientEv>(
    op_manager: Arc<OpManager>,
    mut client_events: ClientEv,
    mut client_responses: ClientResponsesReceiver,
    node_controller: tokio::sync::mpsc::Sender<NodeEvent>,
) -> anyhow::Result<Infallible>
where
    ClientEv: ClientEventsProxy + Send + 'static,
{
    let mut results = FuturesUnordered::new();
    loop {
        tokio::select! {
            client_request = client_events.recv() => {
                let req = match client_request {
                    Ok(request) => {
                        tracing::debug!(%request, "got client request event");
                        request
                    }
                    Err(error) if matches!(error.kind(), ErrorKind::Shutdown) => {
                        node_controller.send(NodeEvent::Disconnect { cause: None }).await.ok();
                        anyhow::bail!("shutdown event");
                    }
                    Err(error) => {
                        tracing::debug!(%error, "client error");
                        continue;
                    }
                };
                // fixme: only allow in certain modes (e.g. while testing)
                if let ClientRequest::Disconnect { cause } = &*req.request {
                    node_controller.send(NodeEvent::Disconnect { cause: cause.clone() }).await.ok();
                    anyhow::bail!("shutdown event");
                }
                let cli_id = req.client_id;
                let res = process_open_request(req, op_manager.clone()).await;
                results.push(async move {
                    match res.await {
                        Ok(Some(Either::Left(res))) => (cli_id, Ok(Some(res))),
                        Ok(Some(Either::Right(mut cb))) => {
                            match cb.recv().await {
                                Some(res) => (cli_id, Ok(Some(res))),
                                None => (cli_id, Err(ClientError::from(ErrorKind::ChannelClosed))),
                            }
                        }
                        Ok(None) => (cli_id, Ok(None)),
                        Err(err) => (cli_id, Err(ErrorKind::OperationError { cause: format!("{err}").into() }.into())),
                    }
                });
            }
            res = client_responses.recv() => {
                if let Some((cli_id, res)) = res {
                    if let Ok(result) = &res {
                        tracing::debug!(%result, "sending client response");
                    }
                    if let Err(err) = client_events.send(cli_id, res).await {
                        tracing::debug!("channel closed: {err}");
                        anyhow::bail!(err);
                    }
                }
            }
            res = results.next(), if !results.is_empty() => {
                let Some(f_res) = res else {
                    unreachable!();
                };
                match f_res {
                    (cli_id, Ok(Some(res))) => {
                        let res = match res {
                            QueryResult::Connections(conns) => {
                                Ok(HostResponse::QueryResponse(QueryResponse::ConnectedPeers {
                                    peers: conns.into_iter().map(|p| (p.pub_key.to_string(), p.addr)).collect() }
                                ))
                            }
                            QueryResult::GetResult { key, state, contract } => {
                                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                                    key,
                                    state,
                                    contract,
                                }))
                            }
                        };
                        if let Err(err) = client_events.send(cli_id, res).await {
                            tracing::debug!("channel closed: {err}");
                            anyhow::bail!(err);
                        }
                    }
                    (_, Ok(None)) => continue,
                    // TODO: we should change the API so client requests have a unique id so we can map specific responses
                    // to the specific client request
                    (cli_id, Err(err)) => client_events.send(cli_id, Err(err)).await?,
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Node not connected to network")]
    Disconnected,
    #[error(transparent)]
    Contract(#[from] crate::contract::ContractError),
    #[error(transparent)]
    Op(#[from] OpError),
    #[error(transparent)]
    Executor(#[from] crate::contract::ExecutorError),
    #[error(transparent)]
    Panic(#[from] tokio::task::JoinError),
}

#[inline]
async fn process_open_request(
    mut request: OpenRequest<'static>,
    op_manager: Arc<OpManager>,
) -> BoxFuture<'static, Result<Option<Either<QueryResult, mpsc::Receiver<QueryResult>>>, Error>> {
    let (callback_tx, callback_rx) = if matches!(
        &*request.request,
        ClientRequest::NodeQueries(_) | ClientRequest::ContractOp(ContractRequest::Get { .. })
    ) {
        let (tx, rx) = mpsc::channel(1);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // TODO: wait until we have a peer_id to attempt (should be connected)
    // this will indirectly start actions on the local contract executor
    let fut = async move {
        let client_id = request.client_id;

        let subscription_listener: Option<UnboundedSender<HostResult>> =
            request.notification_channel.take();

        match *request.request {
            ClientRequest::ContractOp(ops) => {
                match ops {
                    ContractRequest::Put {
                        state,
                        contract,
                        related_contracts,
                    } => {
                        let Some(peer_id) = op_manager.ring.connection_manager.get_peer_key()
                        else {
                            tracing::error!("peer id not found at put op, it should be set");
                            return Err(Error::Disconnected);
                        };

                        tracing::debug!(
                            this_peer = %peer_id,
                            "Received put from user event",
                        );

                        let op = put::start_op(
                            contract,
                            related_contracts,
                            state,
                            op_manager.ring.max_hops_to_live,
                        );
                        let op_id = op.id;

                        op_manager
                            .ch_outbound
                            .waiting_for_transaction_result(op_id, client_id)
                            .await
                            .inspect_err(|err| {
                                tracing::error!("Error waiting for transaction result: {}", err);
                            })?;

                        if let Err(err) = put::request_put(&op_manager, op).await {
                            tracing::error!("Put request error: {}", err);
                        }
                    }
                    ContractRequest::Update { key, data } => {
                        let Some(peer_id) = op_manager.ring.connection_manager.get_peer_key()
                        else {
                            tracing::error!("Peer id not found at update op, it should be set");
                            return Err(Error::Disconnected);
                        };

                        tracing::debug!(
                            this_peer = %peer_id,
                            "Received update from user event",
                        );

                        let related_contracts = RelatedContracts::default();

                        let new_state = match op_manager
                            .notify_contract_handler(ContractHandlerEvent::UpdateQuery {
                                key,
                                data,
                                related_contracts: related_contracts.clone(),
                            })
                            .await
                        {
                            Ok(ContractHandlerEvent::UpdateResponse {
                                new_value: Ok(new_val),
                            }) => Ok(new_val),
                            Ok(ContractHandlerEvent::UpdateResponse {
                                new_value: Err(err),
                            }) => Err(OpError::from(err)),
                            Ok(ContractHandlerEvent::UpdateNoChange { key }) => {
                                tracing::debug!(%key, "update with no change, do not start op");
                                return Ok(None);
                            }
                            Err(err) => Err(err.into()),
                            Ok(_) => Err(OpError::UnexpectedOpState),
                        }
                        .inspect_err(|err| tracing::error!(%key, "update query failed: {}", err))?;

                        let op = update::start_op(key, new_state, related_contracts);

                        op_manager
                            .ch_outbound
                            .waiting_for_transaction_result(op.id, client_id)
                            .await
                            .inspect_err(|err| {
                                tracing::error!("Error waiting for transaction result: {}", err);
                            })?;

                        if let Err(err) = update::request_update(&op_manager, op).await {
                            tracing::error!("request update error {}", err)
                        }
                    }
                    ContractRequest::Get {
                        key,
                        return_contract_code,
                    } => {
                        let Some(peer_id) = op_manager.ring.connection_manager.get_peer_key()
                        else {
                            tracing::error!("Peer id not found at get op, it should be set");
                            return Err(Error::Disconnected);
                        };

                        let (state, contract) = match op_manager
                            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                                key,
                                return_contract_code,
                            })
                            .await
                        {
                            Ok(ContractHandlerEvent::GetResponse {
                                response: Ok(StoreResponse { state, contract }),
                                ..
                            }) => (state, contract),
                            Ok(ContractHandlerEvent::GetResponse {
                                response: Err(err), ..
                            }) => {
                                tracing::error!("get query failed: {}", err);
                                return Err(Error::Executor(err));
                            }
                            Err(err) => {
                                tracing::error!("get query failed: {}", err);
                                return Err(Error::Contract(err));
                            }
                            Ok(_) => {
                                tracing::error!("get query failed: UnexpectedOpState");
                                return Err(Error::Op(OpError::UnexpectedOpState));
                            }
                        };

                        if (!return_contract_code && state.is_some())
                            || (return_contract_code && state.is_some() && contract.is_some())
                        {
                            if let Some(state) = state {
                                tracing::debug!(
                                    this_peer = %peer_id,
                                    "Contract found, returning get result",
                                );
                                return Ok(Some(Either::Left(QueryResult::GetResult {
                                    key,
                                    state,
                                    contract,
                                })));
                            }
                        } else {
                            // Initialize a get op.
                            tracing::debug!(
                                this_peer = %peer_id,
                                "Contract not found, starting get op",
                            );

                            let op = get::start_op(key, return_contract_code);

                            op_manager
                                .ch_outbound
                                .waiting_for_transaction_result(op.id, client_id)
                                .await
                                .inspect_err(|err| {
                                    tracing::error!(
                                        "Error waiting for transaction result (get): {}",
                                        err
                                    );
                                })?;

                            if let Err(err) =
                                get::request_get(&op_manager, op, HashSet::new()).await
                            {
                                tracing::error!("get::request_get error: {}", err);
                            }
                        }
                    }
                    ContractRequest::Subscribe { key, summary } => {
                        let op_id =
                            crate::node::subscribe(op_manager.clone(), key, Some(client_id))
                                .await
                                .inspect_err(|err| {
                                    tracing::error!("Subscribe error: {}", err);
                                })?;

                        let Some(subscriber_listener) = subscription_listener else {
                            tracing::error!(%op_id, %client_id, "No subscriber listener");
                            return Ok(None);
                        };

                        let register_listener = op_manager
                            .notify_contract_handler(
                                ContractHandlerEvent::RegisterSubscriberListener {
                                    key,
                                    client_id,
                                    summary,
                                    subscriber_listener,
                                },
                            )
                            .await
                            .inspect_err(|err| {
                                tracing::error!(
                                    %op_id, %client_id,
                                    "Register subscriber listener error: {}", err
                                );
                            });
                        match register_listener {
                            Ok(ContractHandlerEvent::RegisterSubscriberListenerResponse) => {
                                tracing::debug!(
                                    %op_id, %client_id,
                                    "Subscriber listener registered successfully"
                                );
                            }
                            _ => {
                                tracing::error!(
                                    %op_id, %client_id,
                                    "Subscriber listener registration failed"
                                );
                                return Err(Error::Op(OpError::UnexpectedOpState));
                            }
                        }

                        op_manager
                            .ch_outbound
                            .waiting_for_transaction_result(op_id, client_id)
                            .await
                            .inspect_err(|err| {
                                tracing::error!("Error waiting for transaction result: {}", err);
                            })?;
                    }
                    _ => {
                        tracing::error!("Op not supported");
                    }
                }
            }
            ClientRequest::DelegateOp(_op) => {
                todo!("FIXME: delegate op");
            }
            ClientRequest::Disconnect { .. } => {
                unreachable!();
            }
            ClientRequest::NodeQueries(_) => {
                tracing::debug!("Received node queries from user event");

                let Some(tx) = callback_tx else {
                    tracing::error!("callback_tx not available for NodeQueries");
                    unreachable!();
                };

                if let Err(err) = op_manager
                    .notify_node_event(NodeEvent::QueryConnections { callback: tx })
                    .await
                {
                    tracing::error!("notify_node_event(QueryConnections) error: {}", err);
                    return Err(Error::from(err));
                }

                return Ok(Some(Either::Right(callback_rx.unwrap())));
            }
            _ => {
                tracing::error!("Op not supported");
            }
        }
        Ok(None)
    };

    GlobalExecutor::spawn(fut.instrument(tracing::info_span!(
        parent: tracing::Span::current(),
        "process_client_request"
    )))
    .map(|res| match res {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(err)) => Err(err),
        Err(err) => {
            tracing::error!("Error processing client request: {}", err);
            Err(Error::from(err))
        }
    })
    .boxed()
}

pub(crate) mod test {
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use freenet_stdlib::{
        client_api::{ContractRequest, ErrorKind},
        prelude::*,
    };
    use futures::{FutureExt, StreamExt};
    use rand::{seq::SliceRandom, SeedableRng};
    use tokio::net::TcpStream;
    use tokio::sync::watch::Receiver;
    use tokio::sync::Mutex;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use crate::{node::testing_impl::EventId, transport::TransportPublicKey};

    use super::*;

    pub struct MemoryEventsGen<R = rand::rngs::SmallRng> {
        key: TransportPublicKey,
        signal: Receiver<(EventId, TransportPublicKey)>,
        events_to_gen: HashMap<EventId, ClientRequest<'static>>,
        rng: Option<R>,
        internal_state: Option<InternalGeneratorState>,
    }

    impl<R> MemoryEventsGen<R>
    where
        R: RandomEventGenerator,
    {
        pub fn new_with_seed(
            signal: Receiver<(EventId, TransportPublicKey)>,
            key: TransportPublicKey,
            seed: u64,
        ) -> Self {
            Self {
                signal,
                key,
                events_to_gen: HashMap::new(),
                rng: Some(R::seed_from_u64(seed)),
                internal_state: None,
            }
        }

        pub fn rng_params(
            &mut self,
            id: usize,
            num_peers: usize,
            max_contract_num: usize,
            iterations: usize,
        ) {
            let internal_state = InternalGeneratorState {
                this_peer: id,
                num_peers,
                max_contract_num,
                max_iterations: iterations,
                ..Default::default()
            };
            self.internal_state = Some(internal_state);
        }

        async fn generate_rand_event(&mut self) -> Option<ClientRequest<'static>> {
            let (mut rng, mut state) = self
                .rng
                .take()
                .zip(self.internal_state.take())
                .expect("rng should be set");
            let (rng, state, res) = tokio::task::spawn_blocking(move || {
                let res = rng.gen_event(&mut state);
                (rng, state, res)
            })
            .await
            .expect("task shouldn't fail");
            self.rng = Some(rng);
            self.internal_state = Some(state);
            res
        }
    }

    impl MemoryEventsGen {
        #[cfg(test)]
        pub fn new(
            signal: Receiver<(EventId, TransportPublicKey)>,
            key: TransportPublicKey,
        ) -> Self {
            Self {
                signal,
                key,
                events_to_gen: HashMap::new(),
                rng: None,
                internal_state: None,
            }
        }
    }

    impl<R> MemoryEventsGen<R> {
        #[cfg(test)]
        pub fn generate_events(
            &mut self,
            events: impl IntoIterator<Item = (EventId, ClientRequest<'static>)>,
        ) {
            self.events_to_gen.extend(events)
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest> {
            self.events_to_gen.remove(id)
        }
    }

    impl<R> ClientEventsProxy for MemoryEventsGen<R>
    where
        R: RandomEventGenerator + Send,
    {
        fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
            async {
                loop {
                    if self.signal.changed().await.is_ok() {
                        let (ev_id, pk) = self.signal.borrow().clone();
                        if self.rng.is_some() && pk == self.key {
                            let res = OpenRequest {
                                client_id: ClientId::FIRST,
                                request: self
                                    .generate_rand_event()
                                    .await
                                    .ok_or_else(|| ClientError::from(ErrorKind::Disconnect))?
                                    .into(),
                                notification_channel: None,
                                token: None,
                            };
                            return Ok(res.into_owned());
                        } else if pk == self.key {
                            let res = OpenRequest {
                                client_id: ClientId::FIRST,
                                request: self
                                    .generate_deterministic_event(&ev_id)
                                    .expect("event not found")
                                    .into(),
                                notification_channel: None,
                                token: None,
                            };
                            return Ok(res.into_owned());
                        }
                    } else {
                        // probably the process finished, wait for a bit and then kill the thread
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        break Err(ErrorKind::Shutdown.into());
                    }
                }
            }
            .boxed()
        }

        fn send(
            &mut self,
            _id: ClientId,
            response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key, ..
            })) = response
            {
                self.internal_state
                    .as_mut()
                    .expect("state should be set")
                    .owns_contracts
                    .insert(key);
            }
            async { Ok(()) }.boxed()
        }
    }

    pub struct NetworkEventGenerator<R = rand::rngs::SmallRng> {
        id: TransportPublicKey,
        memory_event_generator: MemoryEventsGen<R>,
        ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    }

    impl<R> NetworkEventGenerator<R>
    where
        R: RandomEventGenerator,
    {
        pub fn new(
            id: TransportPublicKey,
            memory_event_generator: MemoryEventsGen<R>,
            ws_client: Arc<Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
        ) -> Self {
            Self {
                id,
                memory_event_generator,
                ws_client,
            }
        }
    }

    impl<R> ClientEventsProxy for NetworkEventGenerator<R>
    where
        R: RandomEventGenerator + Send + Clone,
    {
        fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg> {
            let ws_client_clone = self.ws_client.clone();

            async move {
                loop {
                    let message = {
                        let mut lock = ws_client_clone.try_lock().inspect_err(|_| {
                            tracing::error!(peer = %self.id, "failed to lock ws client");
                        }).inspect(|_| {
                            tracing::debug!(peer = %self.id, "locked ws client");
                        }).unwrap();
                        lock.next().await
                    };

                    match message {
                        Some(Ok(Message::Binary(data))) => {
                            if let Ok((id, pub_key)) =
                            bincode::deserialize::<(EventId, TransportPublicKey)>(&data)
                            {
                                tracing::debug!(peer = %self.id, %id, "Received event from the supervisor");
                                if pub_key == self.id {
                                    let res = OpenRequest {
                                        client_id: ClientId::FIRST,
                                        request: self
                                            .memory_event_generator
                                            .generate_rand_event()
                                            .await
                                            .ok_or_else(|| {
                                                ClientError::from(ErrorKind::Disconnect)
                                            })?
                                            .into(),
                                        notification_channel: None,
                                        token: None,
                                    };
                                    return Ok(res.into_owned());
                                }
                            } else {
                                continue;
                            }
                        }
                        None => {
                            return Err(ClientError::from(ErrorKind::Disconnect));
                        }
                        _ => continue,
                    }
                }
            }
            .boxed()
        }

        fn send(
            &mut self,
            _id: ClientId,
            response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            if let Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                key, ..
            })) = response
            {
                self.memory_event_generator
                    .internal_state
                    .as_mut()
                    .expect("state should be set")
                    .owns_contracts
                    .insert(key);
            }
            async { Ok(()) }.boxed()
        }
    }

    #[derive(Default)]
    pub struct InternalGeneratorState {
        this_peer: usize,
        num_peers: usize,
        current_iteration: usize,
        max_iterations: usize,
        max_contract_num: usize,
        owns_contracts: HashSet<ContractKey>,
        existing_contracts: Vec<ContractContainer>,
    }

    pub trait RandomEventGenerator: Send + 'static {
        fn seed_from_u64(seed: u64) -> Self;

        fn gen_u8(&mut self) -> u8;

        fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize;

        fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T>;

        fn choose_random_from_iter<'a, T>(
            &mut self,
            mut iter: impl ExactSizeIterator<Item = &'a T> + 'a,
        ) -> Option<&'a T> {
            let len = iter.len();
            let idx = self.gen_range(0..len);
            iter.nth(idx)
        }

        /// The goal of this function is to generate a random event that is valid for the current
        /// global state of the network.
        ///
        /// In order to do this all peers must replicate the same exact events so they are aware
        /// of the current global state (basically which contracts have been created so far).
        ///
        /// To guarantee this make sure that calls to this rng are always executed in the same order
        /// at all peers.
        fn gen_event(
            &mut self,
            state: &mut InternalGeneratorState,
        ) -> Option<ClientRequest<'static>> {
            while state.current_iteration < state.max_iterations {
                state.current_iteration += 1;
                let for_this_peer = self.gen_range(0..state.num_peers) == state.this_peer;
                match self.gen_range(0..100) {
                    val if (0..5).contains(&val) => {
                        if state.max_contract_num <= state.existing_contracts.len() {
                            continue;
                        }
                        let contract = self.gen_contract_container();
                        let request = ContractRequest::Put {
                            contract: contract.clone(),
                            state: WrappedState::new(self.random_byte_vec()),
                            related_contracts: RelatedContracts::new(),
                        };
                        state.existing_contracts.push(contract);
                        if !for_this_peer {
                            continue;
                        }
                        return Some(request.into());
                    }
                    val if (5..20).contains(&val) => {
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            if !for_this_peer {
                                continue;
                            }

                            let request = ContractRequest::Put {
                                contract: contract.clone(),
                                state: WrappedState::new(self.random_byte_vec()),
                                related_contracts: RelatedContracts::new(),
                            };

                            tracing::debug!("sending put to an existing contract");

                            return Some(request.into());
                        }
                    }
                    val if (20..35).contains(&val) => {
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            if !for_this_peer {
                                continue;
                            }
                            let key = contract.key();
                            let request = ContractRequest::Get {
                                key,
                                return_contract_code: true,
                            };
                            return Some(request.into());
                        }
                    }
                    val if (35..80).contains(&val) => {
                        let new_state = UpdateData::State(State::from(self.random_byte_vec()));
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            // TODO: It will be used when the delta updates are available
                            // let delta = UpdateData::Delta(StateDelta::from(self.random_byte_vec()));
                            if !for_this_peer {
                                continue;
                            }
                            let request = ContractRequest::Update {
                                key: contract.key(),
                                data: new_state,
                            };
                            if state.owns_contracts.contains(&contract.key()) {
                                return Some(request.into());
                            }
                        }
                    }
                    val if (80..100).contains(&val) => {
                        let summary = StateSummary::from(self.random_byte_vec());

                        let Some(from_existing) = self.choose(state.existing_contracts.as_slice())
                        else {
                            continue;
                        };

                        let key = from_existing.key();
                        if !for_this_peer {
                            continue;
                        }
                        let request = ContractRequest::Subscribe {
                            key,
                            summary: Some(summary),
                        };
                        return Some(request.into());
                    }
                    _ => unreachable!(),
                }
            }
            None
        }

        fn gen_contract_container(&mut self) -> ContractContainer {
            let code = ContractCode::from(self.random_byte_vec());
            let params = Parameters::from(self.random_byte_vec());
            ContractWasmAPIVersion::V1(WrappedContract::new(code.into(), params)).into()
        }

        fn random_byte_vec(&mut self) -> Vec<u8> {
            (0..self.gen_u8())
                .map(|_| self.gen_u8())
                .collect::<Vec<_>>()
        }
    }

    impl RandomEventGenerator for rand::rngs::SmallRng {
        fn gen_u8(&mut self) -> u8 {
            <Self as rand::Rng>::gen(self)
        }

        fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize {
            <Self as rand::Rng>::gen_range(self, range)
        }

        fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T> {
            vec.choose(self)
        }

        fn seed_from_u64(seed: u64) -> Self {
            <Self as SeedableRng>::seed_from_u64(seed)
        }
    }

    #[test]
    fn test_gen_event() {
        const NUM_PEERS: usize = 20;
        const ITERATIONS: usize = 10_000;
        let mut threads = vec![];
        for this_peer in 0..NUM_PEERS {
            let thread = std::thread::spawn(move || {
                let mut rng = <rand::rngs::SmallRng as RandomEventGenerator>::seed_from_u64(15_978);
                let mut state = InternalGeneratorState {
                    this_peer,
                    num_peers: NUM_PEERS,
                    max_contract_num: 10,
                    ..Default::default()
                };
                for _ in 0..ITERATIONS {
                    rng.gen_event(&mut state);
                }
                state
            });
            threads.push(thread);
        }
        let states = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>();

        let first_state = &states[0];
        for state in &states[1..] {
            assert_eq!(first_state.existing_contracts, state.existing_contracts);
        }
    }
}
