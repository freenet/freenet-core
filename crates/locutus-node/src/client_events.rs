use log::log;
use rmpv::Value;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::{error::Error as StdError, fmt::Display};

use locutus_runtime::prelude::{ContractKey, StateDelta, StateSummary};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::{WrappedContract, WrappedState};

pub(crate) mod combinator;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub type BoxedClient = Box<dyn ClientEventsProxy + Send + Sync + 'static>;
pub type HostResult = Result<HostResponse, ClientError>;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ClientId(pub(crate) usize);

impl ClientId {
    pub const FIRST: Self = ClientId(0);

    pub fn new(id: usize) -> Self {
        Self(id)
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientError {
    kind: ErrorKind,
}

impl ClientError {
    pub fn kind(&self) -> ErrorKind {
        self.kind.clone()
    }
}

impl From<ErrorKind> for ClientError {
    fn from(kind: ErrorKind) -> Self {
        ClientError { kind }
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
pub enum ErrorKind {
    #[error("comm channel between client/host closed")]
    ChannelClosed,
    #[error("failed while trying to unpack state for {0}")]
    IncorrectState(ContractKey),
    #[error("error while deserializing: {cause}")]
    DeserializationError { cause: String },
    #[error("client disconnected")]
    Disconnect,
    #[error("node not available")]
    NodeUnavailable,
    #[error(transparent)]
    RequestError(#[from] RequestError),
    #[error("lost the connection with the protocol hanling connections")]
    TransportProtocolDisconnect,
    #[error("unhandled error: {cause}")]
    Unhandled { cause: String },
    #[error("unknown client id: {0}")]
    UnknownClient(ClientId),
}

impl warp::reject::Reject for ErrorKind {}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client error: {}", self.kind)
    }
}

impl StdError for ClientError {}

type HostIncomingMsg = Result<OpenRequest, ClientError>;

#[non_exhaustive]
pub struct OpenRequest {
    pub id: ClientId,
    pub request: ClientRequest,
    pub notification_channel: Option<UnboundedSender<HostResult>>,
}

impl OpenRequest {
    pub fn new(id: ClientId, request: ClientRequest) -> Self {
        Self {
            id,
            request,
            notification_channel: None,
        }
    }

    pub fn with_notification(mut self, ch: UnboundedSender<HostResult>) -> Self {
        self.notification_channel = Some(ch);
        self
    }
}

#[allow(clippy::needless_lifetimes)]
pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv<'a>(&'a mut self) -> Pin<Box<dyn Future<Output = HostIncomingMsg> + Send + Sync + 'a>>;

    /// Sends a response from the host to the client application.
    fn send<'a>(
        &'a mut self,
        id: ClientId,
        response: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + 'a>>;

    // fn cloned(&self) -> BoxedClient;
}

/// A response to a previous [`ClientRequest`]
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum HostResponse {
    PutResponse {
        key: ContractKey,
    },
    /// Successful update
    UpdateResponse {
        key: ContractKey,
        summary: StateSummary<'static>,
    },
    GetResponse {
        contract: Option<WrappedContract<'static>>,
        state: WrappedState,
    },
    /// Message sent when there is an update to a subscribed contract.
    UpdateNotification {
        key: ContractKey,
        update: StateDelta<'static>,
    },
}

impl HostResponse {
    pub fn unwrap_put(self) -> ContractKey {
        if let Self::PutResponse { key } = self {
            key
        } else {
            panic!("called `HostResponse::unwrap_put()` on other than `PutResponse` value")
        }
    }

    pub fn unwrap_get(self) -> (WrappedState, Option<WrappedContract<'static>>) {
        if let Self::GetResponse {
            contract, state, ..
        } = self
        {
            (state, contract)
        } else {
            panic!("called `HostResponse::unwrap_put()` on other than `PutResponse` value")
        }
    }
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum RequestError {
    #[error("put error for contract {key}, reason: {cause}")]
    Put { key: ContractKey, cause: String },
    #[error("update error for contract {key}, reason: {cause}")]
    Update { key: ContractKey, cause: String },
    #[error("failed to get contract {key}, reason: {cause}")]
    Get { key: ContractKey, cause: String },
    #[error("client disconnect")]
    Disconnect,
}

/// A request from a client application to the host.
#[derive(Clone, Serialize, Deserialize, Debug)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest {
    /// Insert a new value in a contract corresponding with the provided key.
    Put {
        contract: WrappedContract<'static>,
        /// Value to upsert in the contract.
        state: WrappedState,
    },
    /// Update an existing contract corresponding with the provided key.
    Update {
        key: ContractKey,
        delta: StateDelta<'static>,
    },
    /// Fetch the current state from a contract corresponding to the provided key.
    Get {
        /// Key of the contract.
        key: ContractKey,
        /// If this flag is set then fetch also the contract itself.
        fetch_contract: bool,
    },
    /// Subscribe to the changes in a given contract. Implicitly starts a get operation
    /// if the contract is not present yet.
    Subscribe {
        key: ContractKey,
    },
    Disconnect {
        cause: Option<String>,
    },
}

impl ClientRequest {
    pub fn is_disconnect(&self) -> bool {
        matches!(self, Self::Disconnect { .. })
    }

    /// Deserializes a `ClientRequest` from a MessagePack encoded request.  
    pub fn decode_mp(msg: Vec<u8>) -> Result<Self, ErrorKind> {
        let msp_value = rmpv::decode::read_value(&mut Cursor::new(msg));

        let req = match msp_value {
            Ok(value) => {
                if value.is_map() {
                    let value_map: HashMap<_, _> = value
                        .as_map()
                        .unwrap()
                        .iter()
                        .map(|(key, val)| (key.as_str().unwrap(), val.clone()))
                        .collect();

                    let mut map_keys = Vec::from_iter(value_map.keys().cloned());
                    map_keys.sort();

                    match map_keys.as_slice() {
                        ["contract", "state"] => ClientRequest::Disconnect { cause: None },
                        ["delta", "key"] => {
                            log::info!("Recived update request");
                            ClientRequest::Update {
                                key: get_key_from_rmpv(value_map.get("key").unwrap().clone()),
                                delta: get_delta_from_rmpv(value_map.get("delta").unwrap().clone()),
                            }
                        }
                        ["fetch_contract", "key"] => ClientRequest::Disconnect { cause: None },
                        ["key"] => ClientRequest::Disconnect { cause: None },
                        ["cause"] => ClientRequest::Disconnect { cause: None },
                        _ => unreachable!(),
                    }
                } else {
                    panic!("Error getting client request value, request is no a map")
                }
            }
            Err(err) => panic!("Error getting client request value: {}", err),
        };

        Ok(req)
    }
}

fn get_key_from_rmpv(value: Value) -> ContractKey {
    let key = value.as_map().unwrap();
    let key_spec = key.get(0).clone().unwrap().1.as_slice().unwrap();
    let _ = key.get(1).unwrap().clone().0;
    let key_str = bs58::encode(&key_spec).into_string();
    ContractKey::from_spec(key_str).unwrap()
}

fn get_contract_from_rmpv(value: Value) -> WrappedContract<'static> {
    todo!()
}

fn get_state_from_rmpv(value: Value) -> WrappedState {
    todo!()
}

fn get_delta_from_rmpv(value: Value) -> StateDelta<'static> {
    let delta = value.as_slice().unwrap().to_vec();
    StateDelta::from(delta)
}

impl Display for ClientRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientRequest::Put {
                contract, state, ..
            } => {
                write!(f, "put request for contract {contract} with state {state}")
            }
            ClientRequest::Update { key, .. } => write!(f, "Update request for {key}"),
            ClientRequest::Get {
                key,
                fetch_contract: contract,
                ..
            } => {
                write!(f, "get request for {key} (fetch full contract: {contract})")
            }
            ClientRequest::Subscribe { key, .. } => write!(f, "subscribe request for {key}"),
            ClientRequest::Disconnect { .. } => write!(f, "client disconnected"),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{collections::HashMap, time::Duration};

    use locutus_runtime::{ContractCode, Parameters};
    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use crate::node::{test::EventId, PeerKey};

    use super::*;

    #[derive(Clone)]
    pub(crate) struct MemoryEventsGen {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<(WrappedContract<'static>, WrappedState)>,
        events_to_gen: HashMap<EventId, ClientRequest>,
        random: bool,
    }

    impl MemoryEventsGen {
        pub fn new(signal: Receiver<(EventId, PeerKey)>, id: PeerKey) -> Self {
            Self {
                signal,
                id,
                non_owned_contracts: Vec::new(),
                owned_contracts: Vec::new(),
                events_to_gen: HashMap::new(),
                random: false,
            }
        }

        /// Contracts that are available in the network to be requested.
        pub fn request_contracts(&mut self, contracts: impl IntoIterator<Item = ContractKey>) {
            self.non_owned_contracts.extend(contracts.into_iter())
        }

        /// Contracts that the user updates.
        pub fn has_contract(
            &mut self,
            contracts: impl IntoIterator<Item = (WrappedContract<'static>, WrappedState)>,
        ) {
            self.owned_contracts.extend(contracts);
        }

        /// Events that the user generate.
        pub fn generate_events(
            &mut self,
            events: impl IntoIterator<Item = (EventId, ClientRequest)>,
        ) {
            self.events_to_gen.extend(events.into_iter())
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest> {
            self.events_to_gen.remove(id)
        }

        fn generate_rand_event(&mut self) -> ClientRequest {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let (contract, state) = self.owned_contracts[contract_no].clone();
                        break ClientRequest::Put { contract, state };
                    }
                    1 if !self.non_owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                        let key = self.non_owned_contracts[contract_no];
                        break ClientRequest::Get {
                            key,
                            fetch_contract: rng.gen_bool(0.5),
                        };
                    }
                    2 if !self.non_owned_contracts.is_empty()
                        || !self.owned_contracts.is_empty() =>
                    {
                        let get_owned = match (
                            self.non_owned_contracts.is_empty(),
                            self.owned_contracts.is_empty(),
                        ) {
                            (false, false) => rng.gen_bool(0.5),
                            (false, true) => false,
                            (true, false) => true,
                            _ => unreachable!(),
                        };
                        let key = if get_owned {
                            let contract_no = rng.gen_range(0..self.owned_contracts.len());
                            *self.owned_contracts[contract_no].0.key()
                        } else {
                            // let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                            // self.non_owned_contracts[contract_no]
                            todo!() // fixme
                        };
                        break ClientRequest::Subscribe { key };
                    }
                    0 => {}
                    1 => {}
                    2 => {
                        let msg = "the joint set of owned and non-owned contracts is empty!";
                        log::error!("{}", msg);
                        panic!("{}", msg)
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    #[allow(clippy::needless_lifetimes)]
    impl ClientEventsProxy for MemoryEventsGen {
        fn recv<'a>(
            &'a mut self,
        ) -> Pin<Box<dyn Future<Output = Result<OpenRequest, ClientError>> + Send + Sync + '_>>
        {
            Box::pin(async move {
                loop {
                    if self.signal.changed().await.is_ok() {
                        let (ev_id, pk) = *self.signal.borrow();
                        if pk == self.id && !self.random {
                            let res = OpenRequest {
                                id: ClientId(1),
                                request: self
                                    .generate_deterministic_event(&ev_id)
                                    .expect("event not found"),
                                notification_channel: None,
                            };
                            return Ok(res);
                        } else if pk == self.id {
                            let res = OpenRequest {
                                id: ClientId(1),
                                request: self.generate_rand_event(),
                                notification_channel: None,
                            };
                            return Ok(res);
                        }
                    } else {
                        log::debug!("sender half of user event gen dropped");
                        // probably the process finished, wait for a bit and then kill the thread
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        panic!("finished orphan background thread");
                    }
                }
            })
        }

        fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn put_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);

        let key = ContractKey::from((
            &gen.arbitrary::<Parameters>()?,
            &gen.arbitrary::<ContractCode>()?,
        ));
        let complete_put: HostResult = Ok(HostResponse::PutResponse { key });
        let encoded = rmp_serde::to_vec(&complete_put)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

        let key = ContractKey::from_spec(key.encode())?;
        let only_spec: HostResult = Ok(HostResponse::PutResponse { key });
        let encoded = rmp_serde::to_vec(&only_spec)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
        Ok(())
    }

    #[test]
    fn update_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);

        let update: HostResult = Ok(HostResponse::UpdateResponse {
            key: gen.arbitrary()?,
            summary: gen.arbitrary()?,
        });
        let encoded = rmp_serde::to_vec(&update)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
        Ok(())
    }

    #[test]
    fn get_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);

        let state: WrappedState = gen.arbitrary()?;
        let complete_get: HostResult = Ok(HostResponse::GetResponse {
            contract: Some(gen.arbitrary()?),
            state: state.clone(),
        });
        let encoded = rmp_serde::to_vec(&complete_get)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

        let incomplete_get: HostResult = Ok(HostResponse::GetResponse {
            contract: None,
            state,
        });
        let encoded = rmp_serde::to_vec(&incomplete_get)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
        Ok(())
    }

    #[test]
    fn update_notification_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = crate::util::test::random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);

        let update_notif: HostResult = Ok(HostResponse::UpdateNotification {
            key: gen.arbitrary()?,
            update: StateDelta::from(gen.arbitrary::<Vec<u8>>()?),
        });
        let encoded = rmp_serde::to_vec(&update_notif)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
        Ok(())
    }
}
