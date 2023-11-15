use freenet_stdlib::client_api::ClientRequest;
use freenet_stdlib::client_api::{ClientError, HostResponse};
use futures::future::BoxFuture;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

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

    #[cfg(test)]
    pub(crate) const fn new(id: usize) -> ClientId {
        Self(id)
    }

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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
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

pub(crate) mod test {
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use freenet_stdlib::{
        client_api::{ContractRequest, ErrorKind},
        prelude::*,
    };
    use futures::FutureExt;
    use rand::{seq::SliceRandom, SeedableRng};
    use tokio::sync::watch::Receiver;

    use crate::node::{tests::EventId, PeerKey};

    use super::*;

    pub(crate) struct MemoryEventsGen<R = rand::rngs::SmallRng> {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        events_to_gen: HashMap<EventId, ClientRequest<'static>>,
        rng: Option<R>,
        internal_state: Option<InternalGeneratorState>,
    }

    impl<R> MemoryEventsGen<R>
    where
        R: RandomEventGenerator,
    {
        pub fn new_with_seed(signal: Receiver<(EventId, PeerKey)>, id: PeerKey, seed: u64) -> Self {
            Self {
                signal,
                id,
                events_to_gen: HashMap::new(),
                rng: Some(R::seed_from_u64(seed)),
                internal_state: None,
            }
        }

        pub fn rng_params(&mut self, num_peers: usize, max_contract_num: usize, iterations: usize) {
            // let hasher = &mut rand::thread_rng();
            // let hash = self.id.hash(hasher);
            // let this_peer = hasher.finish();
            let internal_state = InternalGeneratorState {
                this_peer: 0,
                num_peers,
                max_contract_num,
                max_iterations: iterations,
                ..Default::default()
            };
            self.internal_state = Some(internal_state);
        }

        fn generate_rand_event(&mut self) -> Option<ClientRequest<'static>> {
            let (rng, state) = self
                .rng
                .as_mut()
                .zip(self.internal_state.as_mut())
                .expect("rng should be set");
            rng.gen_event(state)
        }
    }

    impl MemoryEventsGen {
        #[cfg(test)]
        pub fn new(signal: Receiver<(EventId, PeerKey)>, id: PeerKey) -> Self {
            Self {
                signal,
                id,
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
                        let (ev_id, pk) = *self.signal.borrow();
                        if self.rng.is_some() && pk == self.id {
                            let res = OpenRequest {
                                client_id: ClientId::FIRST,
                                request: self
                                    .generate_rand_event()
                                    .ok_or_else(|| ClientError::from(ErrorKind::Disconnect))?
                                    .into(),
                                notification_channel: None,
                                token: None,
                            };
                            return Ok(res.into_owned());
                        } else if pk == self.id {
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
                        panic!("finished orphan background thread");
                    }
                }
            }
            .boxed()
        }

        fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
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
        subcribed_contract: HashSet<ContractKey>,
        existing_contracts: Vec<ContractContainer>,
    }

    pub trait RandomEventGenerator {
        fn seed_from_u64(seed: u64) -> Self;

        fn gen_u8(&mut self) -> u8;

        fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize;

        fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T>;

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
                match self.gen_range(0..4) {
                    0 => {
                        if state.max_contract_num <= state.existing_contracts.len() {
                            continue;
                        }
                        let contract = self.gen_contract_container();
                        let request = ContractRequest::Put {
                            contract: contract.clone(),
                            state: WrappedState::new(self.random_byte_vec()),
                            related_contracts: RelatedContracts::new(),
                        };
                        let key = contract.key();
                        state.existing_contracts.push(contract);
                        if for_this_peer {
                            state.owns_contracts.insert(key);
                            return Some(request.into());
                        }
                    }
                    1 => {
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            let delta = UpdateData::Delta(StateDelta::from(self.random_byte_vec()));
                            if !for_this_peer {
                                continue;
                            }
                            let request = ContractRequest::Update {
                                key: contract.key().clone(),
                                data: delta,
                            };
                            if state.owns_contracts.contains(&contract.key()) {
                                return Some(request.into());
                            }
                        }
                    }
                    2 => {
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            if !for_this_peer {
                                continue;
                            }
                            let key = contract.key();
                            let fetch_contract = state.owns_contracts.contains(&key);
                            let request = ContractRequest::Get {
                                key,
                                fetch_contract,
                            };
                            return Some(request.into());
                        }
                    }
                    3 => {
                        if let Some(contract) = self.choose(&state.existing_contracts) {
                            let key = contract.key();
                            let summary = StateSummary::from(self.random_byte_vec());
                            if !for_this_peer || state.subcribed_contract.contains(&key) {
                                continue;
                            }
                            let request = ContractRequest::Subscribe {
                                key,
                                summary: Some(summary),
                            };
                            return Some(request.into());
                        }
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

    impl RandomEventGenerator for fastrand::Rng {
        fn gen_u8(&mut self) -> u8 {
            self.u8(..u8::MAX)
        }

        fn gen_range(&mut self, range: std::ops::Range<usize>) -> usize {
            self.choice(range).expect("non empty")
        }

        fn choose<'a, T>(&mut self, vec: &'a [T]) -> Option<&'a T> {
            self.choice(0..vec.len()).and_then(|choice| vec.get(choice))
        }

        fn seed_from_u64(seed: u64) -> Self {
            Self::with_seed(seed)
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
