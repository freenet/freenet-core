use std::{collections::HashMap, fmt::Display, io::Cursor};

use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};

use crate::{
    delegate_interface::{DelegateKey, InboundDelegateMsg, OutboundDelegateMsg},
    prelude::{
        ContractKey, DelegateContainer, GetSecretRequest, Parameters, RelatedContracts, SecretsId,
        StateSummary, UpdateData, WrappedState,
    },
    versioning::ContractContainer,
};

use super::{TryFromTsStd, WsApiError};

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
    #[error("lost the connection with the protocol hanling connections")]
    TransportProtocolDisconnect,
    #[error("unhandled error: {cause}")]
    Unhandled { cause: String },
    #[error("unknown client id: {0}")]
    UnknownClient(usize),
    #[error(transparent)]
    RequestError(#[from] RequestError),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client error: {}", self.kind)
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum RequestError {
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    DelegateError(#[from] DelegateError),
    #[error("client disconnect")]
    Disconnect,
    #[error("operation timed out")]
    Timeout,
}

/// Errors that may happen while interacting with delegates.
#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum DelegateError {
    #[error("error while registering delegate {0}")]
    RegisterError(DelegateKey),
    #[error("execution error, cause {0}")]
    ExecutionError(String),
    #[error("missing delegate {0}")]
    Missing(DelegateKey),
    #[error("missing secret `{secret}` for delegate {key}")]
    MissingSecret { key: DelegateKey, secret: SecretsId },
}

/// Errors that may happen while interacting with contracts.
#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
pub enum ContractError {
    #[error("failed to get contract {key}, reason: {cause}")]
    Get { key: ContractKey, cause: String },
    #[error("put error for contract {key}, reason: {cause}")]
    Put { key: ContractKey, cause: String },
    #[error("update error for contract {key}, reason: {cause}")]
    Update { key: ContractKey, cause: String },
    #[error("failed to subscribe for contract {key}, reason: {cause}")]
    Subscribe { key: ContractKey, cause: String },
    #[error("missing related contract: {key}")]
    MissingRelated {
        key: crate::contract_interface::ContractInstanceId,
    },
    // todo: actually build a stack of the involved keys
    #[error("dependency contract stack overflow : {key}")]
    ContractStackOverflow {
        key: crate::contract_interface::ContractInstanceId,
    },
}

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
                    ContractRequest::Subscribe { key, summary } => ContractRequest::Subscribe {
                        key,
                        summary: summary.map(StateSummary::into_owned),
                    },
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
    Subscribe {
        key: ContractKey,
        summary: Option<StateSummary<'a>>,
    },
}

impl ContractRequest<'_> {
    pub fn into_owned(self) -> ContractRequest<'static> {
        match self {
            Self::Put {
                contract,
                state,
                related_contracts,
            } => ContractRequest::Put {
                contract,
                state,
                related_contracts: related_contracts.into_owned(),
            },
            Self::Update { key, data } => ContractRequest::Update {
                key,
                data: data.into_owned(),
            },
            Self::Get {
                key,
                fetch_contract,
            } => ContractRequest::Get {
                key,
                fetch_contract,
            },
            Self::Subscribe { key, summary } => ContractRequest::Subscribe {
                key,
                summary: summary.map(StateSummary::into_owned),
            },
        }
    }
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
                    ["key", "summary"] => ContractRequest::Subscribe {
                        key: ContractKey::try_decode(*value_map.get("key").unwrap())
                            .map_err(|err| WsApiError::deserialization(err.to_string()))?,
                        summary: value_map
                            .get("summary")
                            .unwrap()
                            .as_slice()
                            .map(|s| StateSummary::from(s).into_owned()),
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
        #[serde(deserialize_with = "DelegateRequest::deser_params")]
        params: Parameters<'a>,
        #[serde(borrow)]
        inbound: Vec<InboundDelegateMsg<'a>>,
    },
    GetSecretRequest {
        key: DelegateKey,
        #[serde(borrow)]
        params: Parameters<'a>,
        get_request: GetSecretRequest,
    },
    RegisterDelegate {
        delegate: DelegateContainer,
        cipher: [u8; 32],
        nonce: [u8; 24],
    },
    UnregisterDelegate(DelegateKey),
}

impl DelegateRequest<'_> {
    pub const DEFAULT_CIPHER: [u8; 32] = [
        0, 24, 22, 150, 112, 207, 24, 65, 182, 161, 169, 227, 66, 182, 237, 215, 206, 164, 58, 161,
        64, 108, 157, 195, 0, 0, 0, 0, 0, 0, 0, 0,
    ];

    pub const DEFAULT_NONCE: [u8; 24] = [
        57, 18, 79, 116, 63, 134, 93, 39, 208, 161, 156, 229, 222, 247, 111, 79, 210, 126, 127, 55,
        224, 150, 139, 80,
    ];

    pub fn into_owned(self) -> DelegateRequest<'static> {
        match self {
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => DelegateRequest::ApplicationMessages {
                key,
                params: params.into_owned(),
                inbound: inbound.into_iter().map(|e| e.into_owned()).collect(),
            },
            DelegateRequest::GetSecretRequest {
                key,
                get_request,
                params,
            } => DelegateRequest::GetSecretRequest {
                key,
                get_request,
                params: params.into_owned(),
            },
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            },
            DelegateRequest::UnregisterDelegate(key) => DelegateRequest::UnregisterDelegate(key),
        }
    }

    fn deser_params<'de, 'a, D>(deser: D) -> Result<Parameters<'a>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes_vec: Vec<u8> = Deserialize::deserialize(deser)?;
        Ok(Parameters::from(bytes_vec))
    }
}

impl Display for ClientRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientRequest::ContractOp(op) => match op {
                ContractRequest::Put {
                    contract, state, ..
                } => {
                    write!(
                        f,
                        "put request for contract `{contract}` with state {state}"
                    )
                }
                ContractRequest::Update { key, .. } => write!(f, "update request for {key}"),
                ContractRequest::Get {
                    key,
                    fetch_contract: contract,
                    ..
                } => {
                    write!(
                        f,
                        "get request for `{key}` (fetch full contract: {contract})"
                    )
                }
                ContractRequest::Subscribe { key, .. } => {
                    write!(f, "subscribe request for `{key}`")
                }
            },
            ClientRequest::DelegateOp(op) => match op {
                DelegateRequest::ApplicationMessages { key, inbound, .. } => {
                    write!(
                        f,
                        "delegate app request for `{key}` with {} messages",
                        inbound.len()
                    )
                }
                DelegateRequest::GetSecretRequest {
                    get_request: GetSecretRequest { key: secret_id, .. },
                    key,
                    ..
                } => {
                    write!(f, "get delegate secret `{secret_id}` for `{key}`")
                }
                DelegateRequest::RegisterDelegate { delegate, .. } => {
                    write!(f, "delegate register request for `{}`", delegate.key())
                }
                DelegateRequest::UnregisterDelegate(key) => {
                    write!(f, "delegate unregister request for `{key}`")
                }
            },
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
                    f.write_fmt(format_args!("put response for `{key}`"))
                }
                ContractResponse::UpdateResponse { key, .. } => {
                    f.write_fmt(format_args!("update response for `{key}`"))
                }
                ContractResponse::GetResponse { key, .. } => {
                    f.write_fmt(format_args!("get response for `{key}`"))
                }
                ContractResponse::UpdateNotification { key, .. } => {
                    f.write_fmt(format_args!("update notification for `{key}`"))
                }
            },
            HostResponse::DelegateResponse { .. } => write!(f, "delegate responses"),
            HostResponse::Ok => write!(f, "ok response"),
            HostResponse::GenerateRandData(_) => write!(f, "random bytes"),
        }
    }
}

// todo: add a `AsBytes` trait for state representations
#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum ContractResponse<T = WrappedState> {
    GetResponse {
        key: ContractKey,
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
