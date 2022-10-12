use locutus_runtime::{RelatedContracts, UpdateData};
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::io::Cursor;
use std::pin::Pin;
use std::{error::Error as StdError, fmt::Display};

use locutus_runtime::prelude::{ContractKey, StateSummary};
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

impl ErrorKind {
    fn deserialization(cause: String) -> Self {
        Self::DeserializationError { cause }
    }
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
pub enum HostResponse<T: Borrow<[u8]> = WrappedState> {
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
        state: T,
    },
    /// Message sent when there is an update to a subscribed contract.
    UpdateNotification {
        key: ContractKey,
        update: UpdateData<'static>,
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

impl std::fmt::Display for HostResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostResponse::PutResponse { key } => f.write_fmt(format_args!("put response: {key}")),
            HostResponse::UpdateResponse { key, .. } => {
                f.write_fmt(format_args!("update response ({key})"))
            }
            HostResponse::GetResponse { state, .. } => {
                f.write_fmt(format_args!("get response: {state}"))
            }
            HostResponse::UpdateNotification { key, .. } => {
                f.write_fmt(format_args!("update notification (key: {key})"))
            }
        }
    }
}

impl<'a> TryFrom<HostResponse> for HostResponse<Cow<'a, [u8]>> {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    fn try_from(owned: HostResponse) -> Result<Self, Self::Error> {
        match owned {
            HostResponse::PutResponse { key } => Ok(HostResponse::PutResponse { key }),
            HostResponse::UpdateResponse { key, summary } => {
                Ok(HostResponse::UpdateResponse { key, summary })
            }
            HostResponse::GetResponse { .. } => Err("self outlives borrowed content".into()),
            HostResponse::UpdateNotification { key, update } => {
                Ok(HostResponse::UpdateNotification { key, update })
            }
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

// TODO: relax this to not neeed 'static
/// A request from a client application to the host.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest {
    /// Insert a new value in a contract corresponding with the provided key.
    Put {
        contract: WrappedContract<'static>,
        /// Value to upsert in the contract.
        state: WrappedState,
        /// Related contracts.
        related_contracts: RelatedContracts,
    },
    /// Update an existing contract corresponding with the provided key.
    Update {
        key: ContractKey,
        data: UpdateData<'static>,
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
        let value = rmpv::decode::read_value(&mut Cursor::new(msg)).map_err(|e| {
            ErrorKind::DeserializationError {
                cause: format!("{e}"),
            }
        })?;

        let req = {
            if value.is_map() {
                let value_map: HashMap<&str, &rmpv::Value> = HashMap::from_iter(
                    value
                        .as_map()
                        .unwrap()
                        .iter()
                        .map(|(key, val)| (key.as_str().unwrap(), val)),
                );

                let mut map_keys = Vec::from_iter(value_map.keys().copied());
                map_keys.sort();
                match map_keys.as_slice() {
                    ["contract", "relatedContracts", "state"] => {
                        let contract = value_map.get("contract").unwrap();
                        ClientRequest::Put {
                            contract: WrappedContract::try_from(*contract)
                                .map_err(ErrorKind::deserialization)?,
                            state: WrappedState::try_from(*value_map.get("state").unwrap())
                                .map_err(ErrorKind::deserialization)?,
                            related_contracts: RelatedContracts::try_from(
                                *value_map.get("relatedContracts").unwrap(),
                            )
                            .map_err(ErrorKind::deserialization)?,
                        }
                    }
                    ["data", "key"] => ClientRequest::Update {
                        key: ContractKey::try_from(*value_map.get("key").unwrap())
                            .map_err(ErrorKind::deserialization)?,
                        data: UpdateData::try_from(*value_map.get("data").unwrap())
                            .map_err(ErrorKind::deserialization)?,
                    },
                    ["fetch_contract", "key"] => ClientRequest::Get {
                        key: ContractKey::try_from(*value_map.get("key").unwrap())
                            .map_err(ErrorKind::deserialization)?,
                        fetch_contract: value_map.get("fetch_contract").unwrap().as_bool().unwrap(),
                    },
                    ["key"] => ClientRequest::Subscribe {
                        key: ContractKey::try_from(*value_map.get("key").unwrap())
                            .map_err(ErrorKind::deserialization)?,
                    },
                    ["cause"] => ClientRequest::Disconnect {
                        cause: Some(
                            value_map
                                .get("cause")
                                .unwrap()
                                .as_str()
                                .unwrap()
                                .to_string(),
                        ),
                    },
                    _ => unreachable!(),
                }
            } else {
                return Err(ErrorKind::DeserializationError {
                    cause: "value is not a map".into(),
                });
            }
        };

        Ok(req)
    }
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
    use std::sync::Arc;
    use std::{collections::HashMap, time::Duration};

    use locutus_runtime::{ContractCode, Parameters, StateDelta};
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
                        break ClientRequest::Put {
                            contract,
                            state,
                            related_contracts: Default::default(),
                        };
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

        let key = ContractKey::from_id(key.encoded_contract_id())?;
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
            update: StateDelta::from(gen.arbitrary::<Vec<u8>>()?).into(),
        });
        let encoded = rmp_serde::to_vec(&update_notif)?;
        let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
        Ok(())
    }

    #[test]
    fn test_handle_update_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request = ClientRequest::Update {
            key: ContractKey::from_id("DCBi7HNZC3QUZRiZLFZDiEduv5KHgZfgBk8WwTiheGq1".to_string())
                .unwrap(),
            data: locutus_runtime::StateDelta::from(vec![0, 1, 2]).into(),
        };
        let msg: Vec<u8> = vec![
            130, 163, 107, 101, 121, 130, 168, 105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 181,
            41, 189, 142, 103, 137, 251, 46, 133, 213, 21, 255, 179, 17, 3, 17, 240, 208, 191, 5,
            215, 72, 60, 41, 194, 14, 217, 228, 225, 251, 209, 100, 164, 99, 111, 100, 101, 192,
            164, 100, 97, 116, 97, 129, 165, 100, 101, 108, 116, 97, 196, 3, 0, 1, 2,
        ];

        let result_client_request = ClientRequest::decode_mp(msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_get_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request = ClientRequest::Get {
            key: ContractKey::from_id("JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC".to_string())
                .unwrap(),
            fetch_contract: false,
        };
        let msg: Vec<u8> = vec![
            130, 163, 107, 101, 121, 130, 164, 115, 112, 101, 99, 196, 32, 255, 17, 144, 159, 194,
            187, 46, 33, 205, 77, 242, 70, 87, 18, 202, 62, 226, 149, 25, 151, 188, 167, 153, 197,
            129, 25, 179, 198, 218, 99, 159, 139, 168, 99, 111, 110, 116, 114, 97, 99, 116, 192,
            174, 102, 101, 116, 99, 104, 95, 99, 111, 110, 116, 114, 97, 99, 116, 194,
        ];
        let result_client_request = ClientRequest::decode_mp(msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_put_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request = ClientRequest::Put {
            contract: WrappedContract::new(
                Arc::new(ContractCode::from(vec![6, 7, 8, 9])),
                Parameters::from(vec![]),
            ),
            state: WrappedState::new(vec![1, 2, 3, 4]),
            related_contracts: Default::default(),
        };

        let msg: Vec<u8> = vec![
            131, 168, 99, 111, 110, 116, 114, 97, 99, 116, 131, 163, 107, 101, 121, 130, 168, 105,
            110, 115, 116, 97, 110, 99, 101, 130, 164, 116, 121, 112, 101, 166, 66, 117, 102, 102,
            101, 114, 164, 100, 97, 116, 97, 220, 0, 32, 204, 181, 41, 204, 189, 204, 142, 103,
            204, 137, 204, 251, 46, 204, 133, 204, 213, 21, 204, 255, 204, 179, 17, 3, 17, 204,
            240, 204, 208, 204, 191, 5, 204, 215, 72, 60, 41, 204, 194, 14, 204, 217, 204, 228,
            204, 225, 204, 251, 204, 209, 100, 164, 99, 111, 100, 101, 192, 164, 100, 97, 116, 97,
            130, 164, 116, 121, 112, 101, 166, 66, 117, 102, 102, 101, 114, 164, 100, 97, 116, 97,
            145, 1, 170, 112, 97, 114, 97, 109, 101, 116, 101, 114, 115, 130, 164, 116, 121, 112,
            101, 166, 66, 117, 102, 102, 101, 114, 164, 100, 97, 116, 97, 145, 2, 165, 115, 116,
            97, 116, 101, 130, 164, 116, 121, 112, 101, 166, 66, 117, 102, 102, 101, 114, 164, 100,
            97, 116, 97, 145, 3, 176, 114, 101, 108, 97, 116, 101, 100, 67, 111, 110, 116, 114, 97,
            99, 116, 115, 130, 164, 116, 121, 112, 101, 163, 77, 97, 112, 164, 100, 97, 116, 97,
            145, 146, 147, 1, 2, 3, 147, 4, 5, 6,
        ];
        let result_client_request = ClientRequest::decode_mp(msg)?;
        assert_eq!(result_client_request, result_client_request);
        Ok(())
    }
}
