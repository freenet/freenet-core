use futures::future::BoxFuture;
use locutus_runtime::{
    ComponentKey, ContractContainer, InboundComponentMsg, OutboundComponentMsg, RelatedContracts,
    UpdateData,
};
use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::{error::Error as StdError, fmt::Display};

use locutus_runtime::prelude::{ContractKey, StateSummary};
use locutus_runtime::prelude::{TryFromTsStd, WsApiError};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use crate::WrappedState;

pub(crate) mod combinator;
#[cfg(feature = "websocket")]
pub(crate) mod websocket;

pub type BoxedClient = Box<dyn ClientEventsProxy + Send + Sync + 'static>;
pub type HostResult = Result<HostResponse<'static>, ClientError>;

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

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

#[non_exhaustive]
pub struct OpenRequest<'a> {
    pub id: ClientId,
    pub request: ClientRequest<'a>,
    pub notification_channel: Option<UnboundedSender<HostResult>>,
}

impl<'a> OpenRequest<'a> {
    pub fn into_owned(self) -> OpenRequest<'static> {
        OpenRequest {
            request: self.request.into_owned(),
            ..self
        }
    }

    pub fn new(id: ClientId, request: ClientRequest<'a>) -> Self {
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

pub trait ClientEventsProxy {
    /// # Cancellation Safety
    /// This future must be safe to cancel.
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg>;

    /// Sends a response from the host to the client application.
    fn send<'a>(
        &mut self,
        id: ClientId,
        response: Result<HostResponse<'a>, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>>;
}

/// A response to a previous [`ClientRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub enum HostResponse<'a, T: Borrow<[u8]> = WrappedState> {
    ContractResponse(
        #[serde(bound(deserialize = "ContractResponse<T>: Deserialize<'de>"))] ContractResponse<T>,
    ),
    ComponentResponse {
        key: ComponentKey,
        #[serde(borrow)]
        values: Vec<OutboundComponentMsg<'a>>,
    },
    /// A requested action which doesn't require an answer was performed successfully.
    Ok,
}

impl HostResponse<'_> {
    pub fn into_owned(self) -> HostResponse<'static> {
        match self {
            HostResponse::ContractResponse(r) => HostResponse::ContractResponse(r),
            HostResponse::ComponentResponse { key, values } => HostResponse::ComponentResponse {
                key,
                values: values
                    .into_iter()
                    .map(OutboundComponentMsg::into_owned)
                    .collect(),
            },
            HostResponse::Ok => HostResponse::Ok,
        }
    }

    pub fn unwrap_put(self) -> ContractKey {
        if let Self::ContractResponse(ContractResponse::PutResponse { key }) = self {
            key
        } else {
            panic!("called `HostResponse::unwrap_put()` on other than `PutResponse` value")
        }
    }

    pub fn unwrap_get(self) -> (WrappedState, Option<ContractContainer>) {
        if let Self::ContractResponse(ContractResponse::GetResponse {
            contract, state, ..
        }) = self
        {
            (state, contract)
        } else {
            panic!("called `HostResponse::unwrap_put()` on other than `PutResponse` value")
        }
    }
}

impl std::fmt::Display for HostResponse<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HostResponse::ContractResponse(res) => match res {
                ContractResponse::PutResponse { key } => {
                    f.write_fmt(format_args!("put response: {key}"))
                }
                ContractResponse::UpdateResponse { key, .. } => {
                    f.write_fmt(format_args!("update response ({key})"))
                }
                ContractResponse::GetResponse { state, .. } => {
                    f.write_fmt(format_args!("get response: {state}"))
                }
                ContractResponse::UpdateNotification { key, .. } => {
                    f.write_fmt(format_args!("update notification (key: {key})"))
                }
            },
            HostResponse::ComponentResponse { .. } => write!(f, "component responses"),
            HostResponse::Ok => write!(f, "ok response"),
        }
    }
}

impl<'a> TryFrom<HostResponse<'a>> for HostResponse<'a, Cow<'a, [u8]>> {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    fn try_from(owned: HostResponse<'a>) -> Result<Self, Self::Error> {
        match owned {
            HostResponse::ContractResponse(res) => match res {
                ContractResponse::PutResponse { key } => {
                    Ok(ContractResponse::PutResponse { key }.into())
                }
                ContractResponse::UpdateResponse { key, summary } => {
                    Ok(ContractResponse::UpdateResponse { key, summary }.into())
                }
                ContractResponse::GetResponse { .. } => {
                    Err("self outlives borrowed content".into())
                }
                ContractResponse::UpdateNotification { key, update } => {
                    Ok(ContractResponse::UpdateNotification { key, update }.into())
                }
            },
            HostResponse::ComponentResponse { key, values } => {
                Ok(HostResponse::ComponentResponse { key, values })
            }
            HostResponse::Ok => Ok(HostResponse::Ok),
        }
    }
}

// todo: add a `AsBytes` trait for state representations
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ContractResponse<T: Borrow<[u8]> = WrappedState> {
    GetResponse {
        contract: Option<ContractContainer>,
        state: T,
    },
    PutResponse {
        key: ContractKey,
    },
    /// Message sent when there is an update to a subscribed contract.
    UpdateNotification {
        key: ContractKey,
        #[serde(borrow)]
        update: UpdateData<'static>,
    },
    /// Successful update
    UpdateResponse {
        key: ContractKey,
        #[serde(borrow)]
        summary: StateSummary<'static>,
    },
}

impl<T: Borrow<[u8]>> From<ContractResponse<T>> for HostResponse<'static, T> {
    fn from(value: ContractResponse<T>) -> HostResponse<'static, T> {
        HostResponse::ContractResponse(value)
    }
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum RequestError {
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    ComponentError(#[from] ComponentError),
    #[error("client disconnect")]
    Disconnect,
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum ComponentError {
    #[error("error while registering component: {0}")]
    RegisterError(ComponentKey),
    #[error("execution error, cause: {0}")]
    ExecutionError(String),
}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum ContractError {
    #[error("failed to get contract {key}, reason: {cause}")]
    Get { key: ContractKey, cause: String },
    #[error("put error for contract {key}, reason: {cause}")]
    Put { key: ContractKey, cause: String },
    #[error("update error for contract {key}, reason: {cause}")]
    Update { key: ContractKey, cause: String },
}

/// A request from a client application to the host.
#[derive(Serialize, Deserialize, Debug, Clone)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest<'a> {
    ComponentOp(#[serde(borrow)] ComponentRequest<'a>),
    ContractOp(#[serde(borrow)] ContractRequest<'a>),
    Disconnect { cause: Option<String> },
}

impl ClientRequest<'_> {
    pub fn into_owned(self) -> ClientRequest<'static> {
        match self {
            ClientRequest::ContractOp(op) => {
                let owned = match op {
                    ContractRequest::Put {
                        contract,
                        state,
                        related_contracts,
                    } => {
                        let related_contracts = related_contracts.into_owned();
                        ContractRequest::Put {
                            contract,
                            state,
                            related_contracts,
                        }
                    }
                    ContractRequest::Update { key, data } => {
                        let data = data.into_owned();
                        ContractRequest::Update { key, data }
                    }
                    ContractRequest::Get {
                        key,
                        fetch_contract,
                    } => ContractRequest::Get {
                        key,
                        fetch_contract,
                    },
                    ContractRequest::Subscribe { key } => ContractRequest::Subscribe { key },
                };
                owned.into()
            }
            ClientRequest::ComponentOp(op) => {
                let op = op.into_owned();
                ClientRequest::ComponentOp(op)
            }
            ClientRequest::Disconnect { cause } => ClientRequest::Disconnect { cause },
        }
    }

    pub fn is_disconnect(&self) -> bool {
        matches!(self, Self::Disconnect { .. })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ContractRequest<'a> {
    /// Insert a new value in a contract corresponding with the provided key.
    Put {
        contract: ContractContainer,
        /// Value to upsert in the contract.
        state: WrappedState,
        /// Related contracts.
        #[serde(borrow)]
        related_contracts: RelatedContracts<'a>,
    },
    /// Update an existing contract corresponding with the provided key.
    Update {
        key: ContractKey,
        #[serde(borrow)]
        data: UpdateData<'a>,
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
    Subscribe { key: ContractKey },
}

impl<'a> From<ContractRequest<'a>> for ClientRequest<'a> {
    fn from(op: ContractRequest<'a>) -> Self {
        ClientRequest::ContractOp(op)
    }
}

/// Deserializes a `ContractRequest` from a MessagePack encoded request.
impl<'a> TryFromTsStd<&[u8]> for ContractRequest<'a> {
    fn try_decode(msg: &[u8]) -> Result<Self, WsApiError> {
        let value = rmpv::decode::read_value(&mut Cursor::new(msg)).map_err(|e| {
            WsApiError::MsgpackDecodeError {
                cause: format!("{e}"),
            }
        })?;

        let req: ContractRequest = {
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
                    ["container", "relatedContracts", "state"] => {
                        let contract = value_map.get("container").unwrap();
                        ContractRequest::Put {
                            contract: ContractContainer::try_decode(*contract)
                                .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                            state: WrappedState::try_decode(*value_map.get("state").unwrap())
                                .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                            related_contracts: RelatedContracts::try_decode(
                                *value_map.get("relatedContracts").unwrap(),
                            )
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?
                            .into_owned(),
                        }
                    }
                    ["data", "key"] => ContractRequest::Update {
                        key: ContractKey::try_decode(*value_map.get("key").unwrap())
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                        data: UpdateData::try_decode(*value_map.get("data").unwrap())
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?
                            .into_owned(),
                    },
                    ["fetchContract", "key"] => ContractRequest::Get {
                        key: ContractKey::try_decode(*value_map.get("key").unwrap())
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                        fetch_contract: value_map.get("fetchContract").unwrap().as_bool().unwrap(),
                    },
                    ["key"] => ContractRequest::Subscribe {
                        key: ContractKey::try_decode(*value_map.get("key").unwrap())
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                    },
                    _ => unreachable!(),
                }
            } else {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "value is not a map".into(),
                });
            }
        };

        Ok(req)
    }
}

impl<'a> From<ComponentRequest<'a>> for ClientRequest<'a> {
    fn from(op: ComponentRequest<'a>) -> Self {
        ClientRequest::ComponentOp(op)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ComponentRequest<'a> {
    ApplicationMessages {
        key: ComponentKey,
        inbound: Vec<InboundComponentMsg<'a>>,
    },
    RegisterComponent(#[serde(borrow)] locutus_runtime::Component<'a>),
    UnregisterComponent(ComponentKey),
}

impl ComponentRequest<'_> {
    pub fn into_owned(self) -> ComponentRequest<'static> {
        match self {
            ComponentRequest::ApplicationMessages { key, inbound } => {
                ComponentRequest::ApplicationMessages {
                    key,
                    inbound: inbound.into_iter().map(|e| e.into_owned()).collect(),
                }
            }
            ComponentRequest::RegisterComponent(cmp) => {
                let cmp = cmp.into_owned();
                ComponentRequest::RegisterComponent(cmp)
            }
            ComponentRequest::UnregisterComponent(key) => {
                ComponentRequest::UnregisterComponent(key)
            }
        }
    }
}

impl Display for ClientRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientRequest::ContractOp(ops) => match ops {
                ContractRequest::Put {
                    contract, state, ..
                } => {
                    write!(f, "put request for contract {contract} with state {state}")
                }
                ContractRequest::Update { key, .. } => write!(f, "Update request for {key}"),
                ContractRequest::Get {
                    key,
                    fetch_contract: contract,
                    ..
                } => {
                    write!(f, "get request for {key} (fetch full contract: {contract})")
                }
                ContractRequest::Subscribe { key, .. } => write!(f, "subscribe request for {key}"),
            },
            ClientRequest::ComponentOp(_op) => write!(f, "component request"),
            ClientRequest::Disconnect { .. } => write!(f, "client disconnected"),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    #![allow(unused)]

    // FIXME: remove unused
    use std::collections::HashMap;
    use std::sync::Arc;

    use futures::FutureExt;
    use locutus_runtime::{ContractCode, ContractContainer, Parameters, WasmAPIVersion};
    use rand::{prelude::Rng, thread_rng};
    use tokio::sync::watch::Receiver;

    use crate::node::{test::EventId, PeerKey};
    use crate::WrappedContract;

    use super::*;

    pub(crate) struct MemoryEventsGen {
        id: PeerKey,
        signal: Receiver<(EventId, PeerKey)>,
        non_owned_contracts: Vec<ContractKey>,
        owned_contracts: Vec<(ContractContainer, WrappedState)>,
        events_to_gen: HashMap<EventId, ClientRequest<'static>>,
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
            contracts: impl IntoIterator<Item = (ContractContainer, WrappedState)>,
        ) {
            self.owned_contracts.extend(contracts);
        }

        /// Events that the user generate.
        pub fn generate_events(
            &mut self,
            events: impl IntoIterator<Item = (EventId, ClientRequest<'static>)>,
        ) {
            self.events_to_gen.extend(events.into_iter())
        }

        fn generate_deterministic_event(&mut self, id: &EventId) -> Option<ClientRequest> {
            self.events_to_gen.remove(id)
        }

        fn generate_rand_event(&mut self) -> ClientRequest<'static> {
            let mut rng = thread_rng();
            loop {
                match rng.gen_range(0u8..3) {
                    0 if !self.owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.owned_contracts.len());
                        let (contract, state) = self.owned_contracts[contract_no].clone();
                        break ContractRequest::Put {
                            contract,
                            state,
                            related_contracts: Default::default(),
                        }
                        .into();
                    }
                    1 if !self.non_owned_contracts.is_empty() => {
                        let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                        let key = self.non_owned_contracts[contract_no].clone();
                        break ContractRequest::Get {
                            key,
                            fetch_contract: rng.gen_bool(0.5),
                        }
                        .into();
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
                            self.owned_contracts[contract_no].0.clone().key()
                        } else {
                            // let contract_no = rng.gen_range(0..self.non_owned_contracts.len());
                            // self.non_owned_contracts[contract_no]
                            todo!("fixme")
                        };
                        break ContractRequest::Subscribe { key }.into();
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

    impl ClientEventsProxy for MemoryEventsGen {
        fn recv(&mut self) -> BoxFuture<'_, Result<OpenRequest<'static>, ClientError>> {
            // async move {
            //     loop {
            //         if self.signal.changed().await.is_ok() {
            //             let (ev_id, pk) = *self.signal.borrow();
            //             if pk == self.id && !self.random {
            //                 let res = OpenRequest {
            //                     id: ClientId(1),
            //                     request: self
            //                         .generate_deterministic_event(&ev_id)
            //                         .expect("event not found"),
            //                     notification_channel: None,
            //                 };
            //                 return Ok(res);
            //             } else if pk == self.id {
            //                 let res = OpenRequest {
            //                     id: ClientId(1),
            //                     request: self.generate_rand_event(),
            //                     notification_channel: None,
            //                 };
            //                 return Ok(res);
            //             }
            //         } else {
            //             log::debug!("sender half of user event gen dropped");
            //             // probably the process finished, wait for a bit and then kill the thread
            //             tokio::time::sleep(Duration::from_secs(1)).await;
            //             panic!("finished orphan background thread");
            //         }
            //     }
            // }
            // .boxed()
            todo!("fixme")
        }

        fn send(
            &mut self,
            _id: ClientId,
            _response: Result<HostResponse, ClientError>,
        ) -> BoxFuture<'_, Result<(), ClientError>> {
            async { Ok(()) }.boxed()
        }
    }

    // #[test]
    // fn put_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let key = ContractKey::from((
    //         &gen.arbitrary::<Parameters>()?,
    //         &gen.arbitrary::<ContractCode>()?,
    //     ));
    //     let complete_put: HostResult = Ok(HostResponse::PutResponse { key });
    //     let encoded = rmp_serde::to_vec(&complete_put)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

    //     let key = ContractKey::from_id(key.encoded_contract_id())?;
    //     let only_spec: HostResult = Ok(HostResponse::PutResponse { key });
    //     let encoded = rmp_serde::to_vec(&only_spec)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn update_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let update: HostResult = Ok(HostResponse::UpdateResponse {
    //         key: gen.arbitrary()?,
    //         summary: gen.arbitrary()?,
    //     });
    //     let encoded = rmp_serde::to_vec(&update)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn get_response_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let state: WrappedState = gen.arbitrary()?;
    //     let complete_get: HostResult = Ok(HostResponse::GetResponse {
    //         contract: Some(gen.arbitrary()?),
    //         state: state.clone(),
    //     });
    //     let encoded = rmp_serde::to_vec(&complete_get)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;

    //     let incomplete_get: HostResult = Ok(HostResponse::GetResponse {
    //         contract: None,
    //         state,
    //     });
    //     let encoded = rmp_serde::to_vec(&incomplete_get)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    // #[test]
    // fn update_notification_serialization() -> Result<(), Box<dyn std::error::Error>> {
    //     let bytes = crate::util::test::random_bytes_1024();
    //     let mut gen = arbitrary::Unstructured::new(&bytes);

    //     let update_notif: HostResult = Ok(HostResponse::UpdateNotification {
    //         key: gen.arbitrary()?,
    //         update: StateDelta::from(gen.arbitrary::<Vec<u8>>()?).into(),
    //     });
    //     let encoded = rmp_serde::to_vec(&update_notif)?;
    //     let _decoded: HostResult = rmp_serde::from_slice(&encoded)?;
    //     Ok(())
    // }

    #[test]
    fn test_handle_update_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Update {
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

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_get_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Get {
            key: ContractKey::from_id("JAgVrRHt88YbBFjGQtBD3uEmRUFvZQqK7k8ypnJ8g6TC".to_string())
                .unwrap(),
            fetch_contract: false,
        }
        .into();
        let msg: Vec<u8> = vec![
            130, 163, 107, 101, 121, 130, 168, 105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 255,
            17, 144, 159, 194, 187, 46, 33, 205, 77, 242, 70, 87, 18, 202, 62, 226, 149, 25, 151,
            188, 167, 153, 197, 129, 25, 179, 198, 218, 99, 159, 139, 164, 99, 111, 100, 101, 192,
            173, 102, 101, 116, 99, 104, 67, 111, 110, 116, 114, 97, 99, 116, 194,
        ];

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }

    #[test]
    fn test_handle_put_request() -> Result<(), Box<dyn std::error::Error>> {
        let expected_client_request: ContractRequest = ContractRequest::Put {
            contract: ContractContainer::Wasm(WasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(vec![1])),
                Parameters::from(vec![2]),
            ))),
            state: WrappedState::new(vec![3]),
            related_contracts: RelatedContracts::from(HashMap::new()),
        };

        let msg: Vec<u8> = vec![
            131, 169, 99, 111, 110, 116, 97, 105, 110, 101, 114, 132, 163, 107, 101, 121, 130, 168,
            105, 110, 115, 116, 97, 110, 99, 101, 196, 32, 135, 191, 81, 248, 20, 212, 21, 0, 62,
            82, 40, 7, 217, 52, 41, 65, 33, 245, 251, 131, 23, 147, 59, 124, 134, 129, 218, 158,
            113, 132, 235, 85, 164, 99, 111, 100, 101, 192, 164, 100, 97, 116, 97, 196, 1, 1, 170,
            112, 97, 114, 97, 109, 101, 116, 101, 114, 115, 196, 1, 2, 167, 118, 101, 114, 115,
            105, 111, 110, 162, 86, 49, 165, 115, 116, 97, 116, 101, 196, 1, 3, 176, 114, 101, 108,
            97, 116, 101, 100, 67, 111, 110, 116, 114, 97, 99, 116, 115, 128,
        ];

        let result_client_request: ContractRequest = ContractRequest::try_decode(&msg)?;
        assert_eq!(result_client_request, expected_client_request);
        Ok(())
    }
}
