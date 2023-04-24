use std::{collections::HashMap, fmt::Display, io::Cursor};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    delegate_interface::{Delegate, DelegateKey, InboundDelegateMsg, OutboundDelegateMsg},
    prelude::{
        ContractKey, RelatedContracts, StateSummary, TryFromTsStd, UpdateData, WrappedState,
        WsApiError,
    },
    versioning::ContractContainer,
};

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

impl From<String> for ClientError {
    fn from(cause: String) -> Self {
        ClientError {
            kind: ErrorKind::Unhandled { cause },
        }
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
pub enum ErrorKind {
    #[error("comm channel between client/host closed")]
    ChannelClosed,
    #[error("error while deserializing: {cause}")]
    DeserializationError { cause: String },
    #[error("client disconnected")]
    Disconnect,
    #[error("failed while trying to unpack state for {0}")]
    IncorrectState(ContractKey),
    #[error("node not available")]
    NodeUnavailable,
    #[error("undhandled error: {0}")]
    Other(String),
    #[error("lost the connection with the protocol hanling connections")]
    TransportProtocolDisconnect,
    #[error("unhandled error: {cause}")]
    Unhandled { cause: String },
    #[error("unknown client id: {0}")]
    UnknownClient(usize),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client error: {}", self.kind)
    }
}

impl std::error::Error for ClientError {}

/// A request from a client application to the host.
#[derive(Serialize, Deserialize, Debug, Clone)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest<'a> {
    DelegateOp(#[serde(borrow)] DelegateRequest<'a>),
    ContractOp(#[serde(borrow)] ContractRequest<'a>),
    GenerateRandData { bytes: usize },
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
            ClientRequest::DelegateOp(op) => {
                let op = op.into_owned();
                ClientRequest::DelegateOp(op)
            }
            ClientRequest::GenerateRandData { bytes } => ClientRequest::GenerateRandData { bytes },
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

impl<'a> From<DelegateRequest<'a>> for ClientRequest<'a> {
    fn from(op: DelegateRequest<'a>) -> Self {
        ClientRequest::DelegateOp(op)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum DelegateRequest<'a> {
    ApplicationMessages {
        key: DelegateKey,
        inbound: Vec<InboundDelegateMsg<'a>>,
    },
    RegisterDelegate {
        #[serde(borrow)]
        component: Delegate<'a>,
        cipher: [u8; 24],
        nonce: [u8; 24],
    },
    UnregisterDelegate(DelegateKey),
}

impl DelegateRequest<'_> {
    pub fn into_owned(self) -> DelegateRequest<'static> {
        match self {
            DelegateRequest::ApplicationMessages { key, inbound } => {
                DelegateRequest::ApplicationMessages {
                    key,
                    inbound: inbound.into_iter().map(|e| e.into_owned()).collect(),
                }
            }
            DelegateRequest::RegisterDelegate {
                component,
                cipher,
                nonce,
            } => {
                let component = component.into_owned();
                DelegateRequest::RegisterDelegate {
                    component,
                    cipher,
                    nonce,
                }
            }
            DelegateRequest::UnregisterDelegate(key) => DelegateRequest::UnregisterDelegate(key),
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
            ClientRequest::DelegateOp(_op) => write!(f, "component request"),
            ClientRequest::Disconnect { .. } => write!(f, "client disconnected"),
            ClientRequest::GenerateRandData { bytes } => write!(f, "generate {bytes} random bytes"),
        }
    }
}

/// A response to a previous [`ClientRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub enum HostResponse<T = WrappedState, U = Vec<u8>> {
    ContractResponse(#[serde(bound(deserialize = "T: DeserializeOwned"))] ContractResponse<T>),
    DelegateResponse {
        key: DelegateKey,
        values: Vec<OutboundDelegateMsg>,
    },
    GenerateRandData(U),
    /// A requested action which doesn't require an answer was performed successfully.
    Ok,
}

impl HostResponse {
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

impl std::fmt::Display for HostResponse {
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
            HostResponse::DelegateResponse { .. } => write!(f, "component responses"),
            HostResponse::Ok => write!(f, "ok response"),
            HostResponse::GenerateRandData(_) => write!(f, "random bytes"),
        }
    }
}

// todo: add a `AsBytes` trait for state representations
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ContractResponse<T = WrappedState> {
    GetResponse {
        contract: Option<ContractContainer>,
        #[serde(bound(deserialize = "T: DeserializeOwned"))]
        state: T,
    },
    PutResponse {
        key: ContractKey,
    },
    /// Message sent when there is an update to a subscribed contract.
    UpdateNotification {
        key: ContractKey,
        #[serde(deserialize_with = "ContractResponse::<T>::deser_update_data")]
        update: UpdateData<'static>,
    },
    /// Successful update
    UpdateResponse {
        key: ContractKey,
        #[serde(deserialize_with = "ContractResponse::<T>::deser_state")]
        summary: StateSummary<'static>,
    },
}

impl<T> ContractResponse<T> {
    fn deser_update_data<'de, D>(deser: D) -> Result<UpdateData<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <UpdateData as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }

    fn deser_state<'de, D>(deser: D) -> Result<StateSummary<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <StateSummary as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }
}

impl<T> From<ContractResponse<T>> for HostResponse<T> {
    fn from(value: ContractResponse<T>) -> HostResponse<T> {
        HostResponse::ContractResponse(value)
    }
}
