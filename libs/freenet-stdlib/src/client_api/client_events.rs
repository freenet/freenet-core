use flatbuffers::WIPOffset;
use std::borrow::Cow;
use std::fmt::Display;
use std::net::SocketAddr;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::client_api::TryFromFbs;
use crate::generated::client_request::{
    root_as_client_request, ClientRequestType, ContractRequest as FbsContractRequest,
    ContractRequestType, DelegateRequest as FbsDelegateRequest, DelegateRequestType,
};

use crate::generated::common::{
    ApplicationMessage as FbsApplicationMessage, ApplicationMessageArgs, ContractCode,
    ContractCodeArgs, ContractContainer as FbsContractContainer, ContractContainerArgs,
    ContractInstanceId as FbsContractInstanceId, ContractInstanceIdArgs,
    ContractKey as FbsContractKey, ContractKeyArgs, ContractType, DeltaUpdate, DeltaUpdateArgs,
    RelatedDeltaUpdate, RelatedDeltaUpdateArgs, RelatedStateAndDeltaUpdate,
    RelatedStateAndDeltaUpdateArgs, RelatedStateUpdate, RelatedStateUpdateArgs,
    StateAndDeltaUpdate, StateAndDeltaUpdateArgs, StateUpdate, StateUpdateArgs,
    UpdateData as FbsUpdateData, UpdateDataArgs, UpdateDataType, WasmContractV1,
    WasmContractV1Args,
};
use crate::generated::host_response::{
    finish_host_response_buffer, ClientResponse as FbsClientResponse, ClientResponseArgs,
    ContextUpdated as FbsContextUpdated, ContextUpdatedArgs,
    ContractResponse as FbsContractResponse, ContractResponseArgs, ContractResponseType,
    DelegateKey as FbsDelegateKey, DelegateKeyArgs, DelegateResponse as FbsDelegateResponse,
    DelegateResponseArgs, GetResponse as FbsGetResponse, GetResponseArgs,
    HostResponse as FbsHostResponse, HostResponseArgs, HostResponseType, NotFound as FbsNotFound,
    NotFoundArgs, Ok as FbsOk, OkArgs, OutboundDelegateMsg as FbsOutboundDelegateMsg,
    OutboundDelegateMsgArgs, OutboundDelegateMsgType, PutResponse as FbsPutResponse,
    PutResponseArgs, RequestUserInput as FbsRequestUserInput, RequestUserInputArgs,
    UpdateNotification as FbsUpdateNotification, UpdateNotificationArgs,
    UpdateResponse as FbsUpdateResponse, UpdateResponseArgs,
};
use crate::prelude::ContractContainer::Wasm;
use crate::prelude::ContractWasmAPIVersion::V1;
use crate::prelude::UpdateData::{
    Delta, RelatedDelta, RelatedState, RelatedStateAndDelta, State, StateAndDelta,
};
use crate::{
    delegate_interface::{DelegateKey, InboundDelegateMsg, OutboundDelegateMsg},
    prelude::{
        ContractInstanceId, ContractKey, DelegateContainer, Parameters, RelatedContracts,
        SecretsId, StateSummary, UpdateData, WrappedState,
    },
    versioning::ContractContainer,
};

use super::WsApiError;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientError {
    kind: Box<ErrorKind>,
}

impl ClientError {
    pub fn into_fbs_bytes(self) -> Result<Vec<u8>, Box<ClientError>> {
        use crate::generated::host_response::{Error, ErrorArgs};
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let msg_offset = builder.create_string(&self.to_string());
        let err_offset = Error::create(
            &mut builder,
            &ErrorArgs {
                msg: Some(msg_offset),
            },
        );
        let host_response_offset = FbsHostResponse::create(
            &mut builder,
            &HostResponseArgs {
                response_type: HostResponseType::Ok,
                response: Some(err_offset.as_union_value()),
            },
        );
        finish_host_response_buffer(&mut builder, host_response_offset);
        Ok(builder.finished_data().to_vec())
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }
}

impl From<ErrorKind> for ClientError {
    fn from(kind: ErrorKind) -> Self {
        ClientError {
            kind: Box::new(kind),
        }
    }
}

impl<T: Into<Cow<'static, str>>> From<T> for ClientError {
    fn from(cause: T) -> Self {
        ClientError {
            kind: Box::new(ErrorKind::Unhandled {
                cause: cause.into(),
            }),
        }
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("comm channel between client/host closed")]
    ChannelClosed,
    #[error("error while deserializing: {cause}")]
    DeserializationError { cause: Cow<'static, str> },
    #[error("client disconnected")]
    Disconnect,
    #[error("failed while trying to unpack state for {0}")]
    IncorrectState(ContractKey),
    #[error("node not available")]
    NodeUnavailable,
    #[error("lost the connection with the protocol handling connections")]
    TransportProtocolDisconnect,
    #[error("unhandled error: {cause}")]
    Unhandled { cause: Cow<'static, str> },
    #[error("unknown client id: {0}")]
    UnknownClient(usize),
    #[error(transparent)]
    RequestError(#[from] RequestError),
    #[error("error while executing operation in the network: {cause}")]
    OperationError { cause: Cow<'static, str> },
    // TODO: identify requests by some id so we can inform clients which one failed exactly
    #[error("operation timed out")]
    FailedOperation,
    #[error("peer should shutdown")]
    Shutdown,
    #[error("no ring connections found")]
    EmptyRing,
    #[error("peer has not joined the network yet")]
    PeerNotJoined,
}

impl Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "client error: {}", self.kind)
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum DelegateError {
    #[error("error while registering delegate {0}")]
    RegisterError(DelegateKey),
    #[error("execution error, cause {0}")]
    ExecutionError(Cow<'static, str>),
    #[error("missing delegate {0}")]
    Missing(DelegateKey),
    #[error("missing secret `{secret}` for delegate {key}")]
    MissingSecret { key: DelegateKey, secret: SecretsId },
    #[error("forbidden access to secret: {0}")]
    ForbiddenSecretAccess(SecretsId),
}

/// Errors that may happen while interacting with contracts.
#[derive(Debug, thiserror::Error, Serialize, Deserialize, Clone)]
#[non_exhaustive]
pub enum ContractError {
    #[error("failed to get contract {key}, reason: {cause}")]
    Get {
        key: ContractKey,
        cause: Cow<'static, str>,
    },
    #[error("put error for contract {key}, reason: {cause}")]
    Put {
        key: ContractKey,
        cause: Cow<'static, str>,
    },
    #[error("update error for contract {key}, reason: {cause}")]
    Update {
        key: ContractKey,
        cause: Cow<'static, str>,
    },
    #[error("failed to subscribe for contract {key}, reason: {cause}")]
    Subscribe {
        key: ContractKey,
        cause: Cow<'static, str>,
    },
    // todo: actually build a stack of the involved keys
    #[error("dependency contract stack overflow : {key}")]
    ContractStackOverflow {
        key: crate::contract_interface::ContractInstanceId,
    },
    #[error("missing related contract: {key}")]
    MissingRelated {
        key: crate::contract_interface::ContractInstanceId,
    },
    #[error("missing contract: {key}")]
    MissingContract {
        key: crate::contract_interface::ContractInstanceId,
    },
}

impl ContractError {
    const EXECUTION_ERROR: &'static str = "execution error";
    const INVALID_PUT: &'static str = "invalid put";

    pub fn update_exec_error(key: ContractKey, additional_info: impl std::fmt::Display) -> Self {
        Self::Update {
            key,
            cause: format!(
                "{exec_err}: {additional_info}",
                exec_err = Self::EXECUTION_ERROR
            )
            .into(),
        }
    }

    pub fn invalid_put(key: ContractKey) -> Self {
        Self::Put {
            key,
            cause: Self::INVALID_PUT.into(),
        }
    }

    pub fn invalid_update(key: ContractKey) -> Self {
        Self::Update {
            key,
            cause: Self::INVALID_PUT.into(),
        }
    }
}

/// A request from a client application to the host.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[non_exhaustive]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum ClientRequest<'a> {
    DelegateOp(#[serde(borrow)] DelegateRequest<'a>),
    ContractOp(#[serde(borrow)] ContractRequest<'a>),
    Disconnect {
        cause: Option<Cow<'static, str>>,
    },
    Authenticate {
        token: String,
    },
    NodeQueries(NodeQuery),
    /// Gracefully disconnect from the host.
    Close,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectedPeers {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDiagnostics {
    /// Optional contract key to filter diagnostics for specific contract
    pub contract_key: Option<ContractKey>,
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
                        subscribe,
                        blocking_subscribe,
                    } => {
                        let related_contracts = related_contracts.into_owned();
                        ContractRequest::Put {
                            contract,
                            state,
                            related_contracts,
                            subscribe,
                            blocking_subscribe,
                        }
                    }
                    ContractRequest::Update { key, data } => {
                        let data = data.into_owned();
                        ContractRequest::Update { key, data }
                    }
                    ContractRequest::Get {
                        key,
                        return_contract_code,
                        subscribe,
                        blocking_subscribe,
                    } => ContractRequest::Get {
                        key,
                        return_contract_code,
                        subscribe,
                        blocking_subscribe,
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
            ClientRequest::Disconnect { cause } => ClientRequest::Disconnect { cause },
            ClientRequest::Authenticate { token } => ClientRequest::Authenticate { token },
            ClientRequest::NodeQueries(query) => ClientRequest::NodeQueries(query),
            ClientRequest::Close => ClientRequest::Close,
        }
    }

    pub fn is_disconnect(&self) -> bool {
        matches!(self, Self::Disconnect { .. })
    }

    pub fn try_decode_fbs(msg: &[u8]) -> Result<ClientRequest<'_>, WsApiError> {
        let req = {
            match root_as_client_request(msg) {
                Ok(client_request) => match client_request.client_request_type() {
                    ClientRequestType::ContractRequest => {
                        let contract_request =
                            client_request.client_request_as_contract_request().unwrap();
                        ContractRequest::try_decode_fbs(&contract_request)?.into()
                    }
                    ClientRequestType::DelegateRequest => {
                        let delegate_request =
                            client_request.client_request_as_delegate_request().unwrap();
                        DelegateRequest::try_decode_fbs(&delegate_request)?.into()
                    }
                    ClientRequestType::Disconnect => {
                        let delegate_request =
                            client_request.client_request_as_disconnect().unwrap();
                        let cause = delegate_request
                            .cause()
                            .map(|cause_msg| cause_msg.to_string().into());
                        ClientRequest::Disconnect { cause }
                    }
                    ClientRequestType::Authenticate => {
                        let auth_req = client_request.client_request_as_authenticate().unwrap();
                        let token = auth_req.token();
                        ClientRequest::Authenticate {
                            token: token.to_owned(),
                        }
                    }
                    _ => unreachable!(),
                },
                Err(e) => {
                    let cause = format!("{e}");
                    return Err(WsApiError::deserialization(cause));
                }
            }
        };

        Ok(req)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ContractRequest<'a> {
    /// Insert a new value in a contract corresponding with the provided key.
    Put {
        contract: ContractContainer,
        /// Value to upsert in the contract.
        state: WrappedState,
        /// Related contracts.
        #[serde(borrow)]
        related_contracts: RelatedContracts<'a>,
        /// If this flag is set then subscribe to updates for this contract.
        subscribe: bool,
        /// If true, the PUT response waits for the subscription to complete.
        /// Only meaningful when `subscribe` is true.
        #[serde(default)]
        blocking_subscribe: bool,
    },
    /// Update an existing contract corresponding with the provided key.
    Update {
        key: ContractKey,
        #[serde(borrow)]
        data: UpdateData<'a>,
    },
    /// Fetch the current state from a contract corresponding to the provided key.
    Get {
        /// Instance ID of the contract (the hash of code + params).
        /// Only the instance ID is needed since the client doesn't have the code hash yet.
        key: ContractInstanceId,
        /// If this flag is set then fetch also the contract itself.
        return_contract_code: bool,
        /// If this flag is set then subscribe to updates for this contract.
        subscribe: bool,
        /// If true, the GET response waits for the subscription to complete.
        /// Only meaningful when `subscribe` is true.
        #[serde(default)]
        blocking_subscribe: bool,
    },
    /// Subscribe to the changes in a given contract. Implicitly starts a get operation
    /// if the contract is not present yet.
    Subscribe {
        /// Instance ID of the contract.
        key: ContractInstanceId,
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
                subscribe,
                blocking_subscribe,
            } => ContractRequest::Put {
                contract,
                state,
                related_contracts: related_contracts.into_owned(),
                subscribe,
                blocking_subscribe,
            },
            Self::Update { key, data } => ContractRequest::Update {
                key,
                data: data.into_owned(),
            },
            Self::Get {
                key,
                return_contract_code: fetch_contract,
                subscribe,
                blocking_subscribe,
            } => ContractRequest::Get {
                key,
                return_contract_code: fetch_contract,
                subscribe,
                blocking_subscribe,
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

/// Deserializes a `ContractRequest` from a Flatbuffers message.
impl<'a> TryFromFbs<&FbsContractRequest<'a>> for ContractRequest<'a> {
    fn try_decode_fbs(request: &FbsContractRequest<'a>) -> Result<Self, WsApiError> {
        let req = {
            match request.contract_request_type() {
                ContractRequestType::Get => {
                    let get = request.contract_request_as_get().unwrap();
                    // Extract just the instance ID - GET only needs the instance ID,
                    // not the full key (which may not be complete on the client side)
                    let fbs_key = get.key();
                    let key_bytes: [u8; 32] = fbs_key.instance().data().bytes().try_into().unwrap();
                    let key = ContractInstanceId::new(key_bytes);
                    let fetch_contract = get.fetch_contract();
                    let subscribe = get.subscribe();
                    let blocking_subscribe = get.blocking_subscribe();
                    ContractRequest::Get {
                        key,
                        return_contract_code: fetch_contract,
                        subscribe,
                        blocking_subscribe,
                    }
                }
                ContractRequestType::Put => {
                    let put = request.contract_request_as_put().unwrap();
                    let contract = ContractContainer::try_decode_fbs(&put.container())?;
                    let state = WrappedState::new(put.wrapped_state().bytes().to_vec());
                    let related_contracts =
                        RelatedContracts::try_decode_fbs(&put.related_contracts())?.into_owned();
                    let subscribe = put.subscribe();
                    let blocking_subscribe = put.blocking_subscribe();
                    ContractRequest::Put {
                        contract,
                        state,
                        related_contracts,
                        subscribe,
                        blocking_subscribe,
                    }
                }
                ContractRequestType::Update => {
                    let update = request.contract_request_as_update().unwrap();
                    let key = ContractKey::try_decode_fbs(&update.key())?;
                    let data = UpdateData::try_decode_fbs(&update.data())?.into_owned();
                    ContractRequest::Update { key, data }
                }
                ContractRequestType::Subscribe => {
                    let subscribe = request.contract_request_as_subscribe().unwrap();
                    // Extract just the instance ID for Subscribe
                    let fbs_key = subscribe.key();
                    let key_bytes: [u8; 32] = fbs_key.instance().data().bytes().try_into().unwrap();
                    let key = ContractInstanceId::new(key_bytes);
                    let summary = subscribe
                        .summary()
                        .map(|summary_data| StateSummary::from(summary_data.bytes()));
                    ContractRequest::Subscribe { key, summary }
                }
                _ => unreachable!(),
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
#[non_exhaustive]
pub enum DelegateRequest<'a> {
    ApplicationMessages {
        key: DelegateKey,
        #[serde(deserialize_with = "Parameters::deser_params")]
        params: Parameters<'a>,
        #[serde(borrow)]
        inbound: Vec<InboundDelegateMsg<'a>>,
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

    pub fn key(&self) -> &DelegateKey {
        match self {
            DelegateRequest::ApplicationMessages { key, .. } => key,
            DelegateRequest::RegisterDelegate { delegate, .. } => delegate.key(),
            DelegateRequest::UnregisterDelegate(key) => key,
        }
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
                        "ContractRequest::Put for contract `{contract}` with state {state}"
                    )
                }
                ContractRequest::Update { key, .. } => write!(f, "update request for {key}"),
                ContractRequest::Get {
                    key,
                    return_contract_code: contract,
                    ..
                } => {
                    write!(
                        f,
                        "ContractRequest::Get for key `{key}` (fetch full contract: {contract})"
                    )
                }
                ContractRequest::Subscribe { key, .. } => {
                    write!(f, "ContractRequest::Subscribe for `{key}`")
                }
            },
            ClientRequest::DelegateOp(op) => match op {
                DelegateRequest::ApplicationMessages { key, inbound, .. } => {
                    write!(
                        f,
                        "DelegateRequest::ApplicationMessages for `{key}` with {} messages",
                        inbound.len()
                    )
                }
                DelegateRequest::RegisterDelegate { delegate, .. } => {
                    write!(
                        f,
                        "DelegateRequest::RegisterDelegate for delegate.key()=`{}`",
                        delegate.key()
                    )
                }
                DelegateRequest::UnregisterDelegate(key) => {
                    write!(f, "DelegateRequest::UnregisterDelegate for key `{key}`")
                }
            },
            ClientRequest::Disconnect { .. } => write!(f, "client disconnected"),
            ClientRequest::Authenticate { .. } => write!(f, "authenticate"),
            ClientRequest::NodeQueries(query) => write!(f, "node queries: {:?}", query),
            ClientRequest::Close => write!(f, "close"),
        }
    }
}

/// Deserializes a `DelegateRequest` from a Flatbuffers message.
impl<'a> TryFromFbs<&FbsDelegateRequest<'a>> for DelegateRequest<'a> {
    fn try_decode_fbs(request: &FbsDelegateRequest<'a>) -> Result<Self, WsApiError> {
        let req = {
            match request.delegate_request_type() {
                DelegateRequestType::ApplicationMessages => {
                    let app_msg = request.delegate_request_as_application_messages().unwrap();
                    let key = DelegateKey::try_decode_fbs(&app_msg.key())?;
                    let params = Parameters::from(app_msg.params().bytes());
                    let inbound = app_msg
                        .inbound()
                        .iter()
                        .map(|msg| InboundDelegateMsg::try_decode_fbs(&msg))
                        .collect::<Result<Vec<_>, _>>()?;
                    DelegateRequest::ApplicationMessages {
                        key,
                        params,
                        inbound,
                    }
                }
                DelegateRequestType::RegisterDelegate => {
                    let register = request.delegate_request_as_register_delegate().unwrap();
                    let delegate = DelegateContainer::try_decode_fbs(&register.delegate())?;
                    let cipher =
                        <[u8; 32]>::try_from(register.cipher().bytes().to_vec().as_slice())
                            .unwrap();
                    let nonce =
                        <[u8; 24]>::try_from(register.nonce().bytes().to_vec().as_slice()).unwrap();
                    DelegateRequest::RegisterDelegate {
                        delegate,
                        cipher,
                        nonce,
                    }
                }
                DelegateRequestType::UnregisterDelegate => {
                    let unregister = request.delegate_request_as_unregister_delegate().unwrap();
                    let key = DelegateKey::try_decode_fbs(&unregister.key())?;
                    DelegateRequest::UnregisterDelegate(key)
                }
                _ => unreachable!(),
            }
        };

        Ok(req)
    }
}

/// A response to a previous [`ClientRequest`]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[non_exhaustive]
pub enum HostResponse<T = WrappedState> {
    ContractResponse(#[serde(bound(deserialize = "T: DeserializeOwned"))] ContractResponse<T>),
    DelegateResponse {
        key: DelegateKey,
        values: Vec<OutboundDelegateMsg>,
    },
    QueryResponse(QueryResponse),
    /// A requested action which doesn't require an answer was performed successfully.
    Ok,
}

type Peer = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum QueryResponse {
    ConnectedPeers { peers: Vec<(Peer, SocketAddr)> },
    NetworkDebug(NetworkDebugInfo),
    NodeDiagnostics(NodeDiagnosticsResponse),
    ProximityCache(ProximityCacheInfo),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkDebugInfo {
    pub subscriptions: Vec<SubscriptionInfo>,
    pub connected_peers: Vec<(String, SocketAddr)>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDiagnosticsResponse {
    /// Node information
    pub node_info: Option<NodeInfo>,

    /// Network connectivity information
    pub network_info: Option<NetworkInfo>,

    /// Contract subscription information
    pub subscriptions: Vec<SubscriptionInfo>,

    /// Contract states for specific contracts
    pub contract_states: std::collections::HashMap<ContractKey, ContractState>,

    /// System metrics
    pub system_metrics: Option<SystemMetrics>,

    /// Information about connected peers with detailed data
    pub connected_peers_detailed: Vec<ConnectedPeerInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeInfo {
    pub peer_id: String,
    pub is_gateway: bool,
    pub location: Option<String>,
    pub listening_address: Option<String>,
    pub uptime_seconds: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NetworkInfo {
    pub connected_peers: Vec<(String, String)>, // (peer_id, address)
    pub active_connections: usize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContractState {
    /// Number of nodes subscribed to this contract
    pub subscribers: u32,
    /// Peer IDs of nodes that are subscribed to this contract
    pub subscriber_peer_ids: Vec<String>,
    /// Size of the contract state in bytes
    #[serde(default)]
    pub size_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SystemMetrics {
    pub active_connections: u32,
    pub seeding_contracts: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscriptionInfo {
    pub contract_key: ContractInstanceId,
    pub client_id: usize,
}

/// Basic information about a connected peer
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ConnectedPeerInfo {
    pub peer_id: String,
    pub address: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum NodeQuery {
    ConnectedPeers,
    SubscriptionInfo,
    NodeDiagnostics {
        /// Diagnostic configuration specifying what information to collect
        config: NodeDiagnosticsConfig,
    },
    /// Phase 3: Query proximity cache information for update propagation
    ProximityCacheInfo,
}

/// Phase 3: Proximity cache information for update propagation
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProximityCacheInfo {
    /// Contracts this node is currently caching
    pub my_cache: Vec<ContractCacheEntry>,
    /// What we know about neighbor caches
    pub neighbor_caches: Vec<NeighborCacheInfo>,
    /// Proximity propagation statistics
    pub stats: ProximityStats,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContractCacheEntry {
    /// Full contract key as string
    pub contract_key: String,
    /// 32-bit hash for proximity matching
    pub cache_hash: u32,
    /// When this contract was cached (Unix timestamp)
    pub cached_since: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NeighborCacheInfo {
    /// Peer identifier
    pub peer_id: String,
    /// Contract hashes this neighbor is known to cache
    pub known_contracts: Vec<u32>,
    /// Last update received from this neighbor (Unix timestamp)
    pub last_update: u64,
    /// Number of updates received from this neighbor
    pub update_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProximityStats {
    /// Number of cache announcements sent
    pub cache_announces_sent: u64,
    /// Number of cache announcements received
    pub cache_announces_received: u64,
    /// Updates forwarded via proximity (not subscription)
    pub updates_via_proximity: u64,
    /// Updates forwarded via subscription
    pub updates_via_subscription: u64,
    /// False positives due to hash collisions
    pub false_positive_forwards: u64,
    /// Average number of contracts per neighbor
    pub avg_neighbor_cache_size: f32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeDiagnosticsConfig {
    /// Include basic node information (ID, location, uptime, etc.)
    pub include_node_info: bool,

    /// Include network connectivity information
    pub include_network_info: bool,

    /// Include contract subscription information
    pub include_subscriptions: bool,

    /// Include contract states for specific contracts (empty = all contracts)
    pub contract_keys: Vec<ContractKey>,

    /// Include memory and performance metrics
    pub include_system_metrics: bool,

    /// Include detailed information about connected peers (vs basic peer list)
    pub include_detailed_peer_info: bool,

    /// Include peer IDs of subscribers in contract state information
    pub include_subscriber_peer_ids: bool,
}

impl NodeDiagnosticsConfig {
    /// Create a comprehensive diagnostic config for debugging update propagation issues
    pub fn for_update_propagation_debugging(contract_key: ContractKey) -> Self {
        Self {
            include_node_info: true,
            include_network_info: true,
            include_subscriptions: true,
            contract_keys: vec![contract_key],
            include_system_metrics: true,
            include_detailed_peer_info: true,
            include_subscriber_peer_ids: true,
        }
    }

    /// Create a lightweight diagnostic config for basic node status
    pub fn basic_status() -> Self {
        Self {
            include_node_info: true,
            include_network_info: true,
            include_subscriptions: false,
            contract_keys: vec![],
            include_system_metrics: false,
            include_detailed_peer_info: false,
            include_subscriber_peer_ids: false,
        }
    }

    /// Create a full diagnostic config (all information)
    pub fn full() -> Self {
        Self {
            include_node_info: true,
            include_network_info: true,
            include_subscriptions: true,
            contract_keys: vec![], // empty = all contracts
            include_system_metrics: true,
            include_detailed_peer_info: true,
            include_subscriber_peer_ids: true,
        }
    }
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

    pub fn into_fbs_bytes(self) -> Result<Vec<u8>, Box<ClientError>> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        match self {
            HostResponse::ContractResponse(res) => match res {
                ContractResponse::PutResponse { key } => {
                    let instance_data = builder.create_vector(key.as_bytes());
                    let instance_offset = FbsContractInstanceId::create(
                        &mut builder,
                        &ContractInstanceIdArgs {
                            data: Some(instance_data),
                        },
                    );

                    let code = Some(builder.create_vector(&key.code_hash().0));
                    let key_offset = FbsContractKey::create(
                        &mut builder,
                        &ContractKeyArgs {
                            instance: Some(instance_offset),
                            code,
                        },
                    );

                    let put_offset = FbsPutResponse::create(
                        &mut builder,
                        &PutResponseArgs {
                            key: Some(key_offset),
                        },
                    );

                    let contract_response_offset = FbsContractResponse::create(
                        &mut builder,
                        &ContractResponseArgs {
                            contract_response: Some(put_offset.as_union_value()),
                            contract_response_type: ContractResponseType::PutResponse,
                        },
                    );

                    let response_offset = FbsHostResponse::create(
                        &mut builder,
                        &HostResponseArgs {
                            response: Some(contract_response_offset.as_union_value()),
                            response_type: HostResponseType::ContractResponse,
                        },
                    );

                    finish_host_response_buffer(&mut builder, response_offset);
                    Ok(builder.finished_data().to_vec())
                }
                ContractResponse::UpdateResponse { key, summary } => {
                    let instance_data = builder.create_vector(key.as_bytes());
                    let instance_offset = FbsContractInstanceId::create(
                        &mut builder,
                        &ContractInstanceIdArgs {
                            data: Some(instance_data),
                        },
                    );

                    let code = Some(builder.create_vector(&key.code_hash().0));

                    let key_offset = FbsContractKey::create(
                        &mut builder,
                        &ContractKeyArgs {
                            instance: Some(instance_offset),
                            code,
                        },
                    );

                    let summary_data = builder.create_vector(&summary.into_bytes());

                    let update_response_offset = FbsUpdateResponse::create(
                        &mut builder,
                        &UpdateResponseArgs {
                            key: Some(key_offset),
                            summary: Some(summary_data),
                        },
                    );

                    let contract_response_offset = FbsContractResponse::create(
                        &mut builder,
                        &ContractResponseArgs {
                            contract_response: Some(update_response_offset.as_union_value()),
                            contract_response_type: ContractResponseType::UpdateResponse,
                        },
                    );

                    let response_offset = FbsHostResponse::create(
                        &mut builder,
                        &HostResponseArgs {
                            response: Some(contract_response_offset.as_union_value()),
                            response_type: HostResponseType::ContractResponse,
                        },
                    );

                    finish_host_response_buffer(&mut builder, response_offset);
                    Ok(builder.finished_data().to_vec())
                }
                ContractResponse::GetResponse {
                    key,
                    contract: contract_container,
                    state,
                } => {
                    let instance_data = builder.create_vector(key.as_bytes());
                    let instance_offset = FbsContractInstanceId::create(
                        &mut builder,
                        &ContractInstanceIdArgs {
                            data: Some(instance_data),
                        },
                    );

                    let code = Some(builder.create_vector(&key.code_hash().0));
                    let key_offset = FbsContractKey::create(
                        &mut builder,
                        &ContractKeyArgs {
                            instance: Some(instance_offset),
                            code,
                        },
                    );

                    let container_offset = if let Some(contract) = contract_container {
                        let data = builder.create_vector(contract.key().as_bytes());

                        let instance_offset = FbsContractInstanceId::create(
                            &mut builder,
                            &ContractInstanceIdArgs { data: Some(data) },
                        );

                        let code = Some(builder.create_vector(&contract.key().code_hash().0));
                        let contract_key_offset = FbsContractKey::create(
                            &mut builder,
                            &ContractKeyArgs {
                                instance: Some(instance_offset),
                                code,
                            },
                        );

                        let contract_data =
                            builder.create_vector(contract.clone().unwrap_v1().data.data());
                        let contract_code_hash =
                            builder.create_vector(&contract.clone().unwrap_v1().data.hash().0);

                        let contract_code_offset = ContractCode::create(
                            &mut builder,
                            &ContractCodeArgs {
                                data: Some(contract_data),
                                code_hash: Some(contract_code_hash),
                            },
                        );

                        let contract_params =
                            builder.create_vector(&contract.clone().params().into_bytes());

                        let contract_offset = match contract {
                            Wasm(V1(..)) => WasmContractV1::create(
                                &mut builder,
                                &WasmContractV1Args {
                                    key: Some(contract_key_offset),
                                    data: Some(contract_code_offset),
                                    parameters: Some(contract_params),
                                },
                            ),
                        };

                        Some(FbsContractContainer::create(
                            &mut builder,
                            &ContractContainerArgs {
                                contract_type: ContractType::WasmContractV1,
                                contract: Some(contract_offset.as_union_value()),
                            },
                        ))
                    } else {
                        None
                    };

                    let state_data = builder.create_vector(&state);

                    let get_offset = FbsGetResponse::create(
                        &mut builder,
                        &GetResponseArgs {
                            key: Some(key_offset),
                            contract: container_offset,
                            state: Some(state_data),
                        },
                    );

                    let contract_response_offset = FbsContractResponse::create(
                        &mut builder,
                        &ContractResponseArgs {
                            contract_response_type: ContractResponseType::GetResponse,
                            contract_response: Some(get_offset.as_union_value()),
                        },
                    );

                    let response_offset = FbsHostResponse::create(
                        &mut builder,
                        &HostResponseArgs {
                            response: Some(contract_response_offset.as_union_value()),
                            response_type: HostResponseType::ContractResponse,
                        },
                    );

                    finish_host_response_buffer(&mut builder, response_offset);
                    Ok(builder.finished_data().to_vec())
                }
                ContractResponse::UpdateNotification { key, update } => {
                    let instance_data = builder.create_vector(key.as_bytes());
                    let instance_offset = FbsContractInstanceId::create(
                        &mut builder,
                        &ContractInstanceIdArgs {
                            data: Some(instance_data),
                        },
                    );

                    let code = Some(builder.create_vector(&key.code_hash().0));
                    let key_offset = FbsContractKey::create(
                        &mut builder,
                        &ContractKeyArgs {
                            instance: Some(instance_offset),
                            code,
                        },
                    );

                    let update_data = match update {
                        State(state) => {
                            let state_data = builder.create_vector(&state.into_bytes());
                            let state_update_offset = StateUpdate::create(
                                &mut builder,
                                &StateUpdateArgs {
                                    state: Some(state_data),
                                },
                            );
                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::StateUpdate,
                                    update_data: Some(state_update_offset.as_union_value()),
                                },
                            )
                        }
                        Delta(delta) => {
                            let delta_data = builder.create_vector(&delta.into_bytes());
                            let update_offset = DeltaUpdate::create(
                                &mut builder,
                                &DeltaUpdateArgs {
                                    delta: Some(delta_data),
                                },
                            );
                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::DeltaUpdate,
                                    update_data: Some(update_offset.as_union_value()),
                                },
                            )
                        }
                        StateAndDelta { state, delta } => {
                            let state_data = builder.create_vector(&state.into_bytes());
                            let delta_data = builder.create_vector(&delta.into_bytes());

                            let update_offset = StateAndDeltaUpdate::create(
                                &mut builder,
                                &StateAndDeltaUpdateArgs {
                                    state: Some(state_data),
                                    delta: Some(delta_data),
                                },
                            );

                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::StateAndDeltaUpdate,
                                    update_data: Some(update_offset.as_union_value()),
                                },
                            )
                        }
                        RelatedState { related_to, state } => {
                            let state_data = builder.create_vector(&state.into_bytes());
                            let instance_data =
                                builder.create_vector(related_to.encode().as_bytes());

                            let instance_offset = FbsContractInstanceId::create(
                                &mut builder,
                                &ContractInstanceIdArgs {
                                    data: Some(instance_data),
                                },
                            );

                            let update_offset = RelatedStateUpdate::create(
                                &mut builder,
                                &RelatedStateUpdateArgs {
                                    related_to: Some(instance_offset),
                                    state: Some(state_data),
                                },
                            );

                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::RelatedStateUpdate,
                                    update_data: Some(update_offset.as_union_value()),
                                },
                            )
                        }
                        RelatedDelta { related_to, delta } => {
                            let instance_data =
                                builder.create_vector(related_to.encode().as_bytes());
                            let delta_data = builder.create_vector(&delta.into_bytes());

                            let instance_offset = FbsContractInstanceId::create(
                                &mut builder,
                                &ContractInstanceIdArgs {
                                    data: Some(instance_data),
                                },
                            );

                            let update_offset = RelatedDeltaUpdate::create(
                                &mut builder,
                                &RelatedDeltaUpdateArgs {
                                    related_to: Some(instance_offset),
                                    delta: Some(delta_data),
                                },
                            );

                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::RelatedDeltaUpdate,
                                    update_data: Some(update_offset.as_union_value()),
                                },
                            )
                        }
                        RelatedStateAndDelta {
                            related_to,
                            state,
                            delta,
                        } => {
                            let instance_data =
                                builder.create_vector(related_to.encode().as_bytes());
                            let state_data = builder.create_vector(&state.into_bytes());
                            let delta_data = builder.create_vector(&delta.into_bytes());

                            let instance_offset = FbsContractInstanceId::create(
                                &mut builder,
                                &ContractInstanceIdArgs {
                                    data: Some(instance_data),
                                },
                            );

                            let update_offset = RelatedStateAndDeltaUpdate::create(
                                &mut builder,
                                &RelatedStateAndDeltaUpdateArgs {
                                    related_to: Some(instance_offset),
                                    state: Some(state_data),
                                    delta: Some(delta_data),
                                },
                            );

                            FbsUpdateData::create(
                                &mut builder,
                                &UpdateDataArgs {
                                    update_data_type: UpdateDataType::RelatedStateAndDeltaUpdate,
                                    update_data: Some(update_offset.as_union_value()),
                                },
                            )
                        }
                    };

                    let update_notification_offset = FbsUpdateNotification::create(
                        &mut builder,
                        &UpdateNotificationArgs {
                            key: Some(key_offset),
                            update: Some(update_data),
                        },
                    );

                    let put_response_offset = FbsContractResponse::create(
                        &mut builder,
                        &ContractResponseArgs {
                            contract_response_type: ContractResponseType::UpdateNotification,
                            contract_response: Some(update_notification_offset.as_union_value()),
                        },
                    );

                    let host_response_offset = FbsHostResponse::create(
                        &mut builder,
                        &HostResponseArgs {
                            response_type: HostResponseType::ContractResponse,
                            response: Some(put_response_offset.as_union_value()),
                        },
                    );

                    finish_host_response_buffer(&mut builder, host_response_offset);
                    Ok(builder.finished_data().to_vec())
                }
                ContractResponse::SubscribeResponse { .. } => todo!(),
                ContractResponse::NotFound { instance_id } => {
                    let instance_data = builder.create_vector(instance_id.as_bytes());
                    let instance_offset = FbsContractInstanceId::create(
                        &mut builder,
                        &ContractInstanceIdArgs {
                            data: Some(instance_data),
                        },
                    );

                    let not_found_offset = FbsNotFound::create(
                        &mut builder,
                        &NotFoundArgs {
                            instance_id: Some(instance_offset),
                        },
                    );

                    let contract_response_offset = FbsContractResponse::create(
                        &mut builder,
                        &ContractResponseArgs {
                            contract_response_type: ContractResponseType::NotFound,
                            contract_response: Some(not_found_offset.as_union_value()),
                        },
                    );

                    let response_offset = FbsHostResponse::create(
                        &mut builder,
                        &HostResponseArgs {
                            response: Some(contract_response_offset.as_union_value()),
                            response_type: HostResponseType::ContractResponse,
                        },
                    );

                    finish_host_response_buffer(&mut builder, response_offset);
                    Ok(builder.finished_data().to_vec())
                }
            },
            HostResponse::DelegateResponse { key, values } => {
                let key_data = builder.create_vector(key.bytes());
                let code_hash_data = builder.create_vector(&key.code_hash().0);
                let key_offset = FbsDelegateKey::create(
                    &mut builder,
                    &DelegateKeyArgs {
                        key: Some(key_data),
                        code_hash: Some(code_hash_data),
                    },
                );
                let mut messages: Vec<WIPOffset<FbsOutboundDelegateMsg>> = Vec::new();
                values.iter().for_each(|msg| match msg {
                    OutboundDelegateMsg::ApplicationMessage(app) => {
                        let instance_data = builder.create_vector(key.bytes());
                        let instance_offset = FbsContractInstanceId::create(
                            &mut builder,
                            &ContractInstanceIdArgs {
                                data: Some(instance_data),
                            },
                        );
                        let payload_data = builder.create_vector(&app.payload);
                        let delegate_context_data = builder.create_vector(app.context.as_ref());
                        let app_offset = FbsApplicationMessage::create(
                            &mut builder,
                            &ApplicationMessageArgs {
                                app: Some(instance_offset),
                                payload: Some(payload_data),
                                context: Some(delegate_context_data),
                                processed: app.processed,
                            },
                        );
                        let msg = FbsOutboundDelegateMsg::create(
                            &mut builder,
                            &OutboundDelegateMsgArgs {
                                inbound_type: OutboundDelegateMsgType::common_ApplicationMessage,
                                inbound: Some(app_offset.as_union_value()),
                            },
                        );
                        messages.push(msg);
                    }
                    OutboundDelegateMsg::RequestUserInput(input) => {
                        let message_data = builder.create_vector(input.message.bytes());
                        let mut responses: Vec<WIPOffset<FbsClientResponse>> = Vec::new();
                        input.responses.iter().for_each(|resp| {
                            let response_data = builder.create_vector(resp.bytes());
                            let response = FbsClientResponse::create(
                                &mut builder,
                                &ClientResponseArgs {
                                    data: Some(response_data),
                                },
                            );
                            responses.push(response)
                        });
                        let responses_offset = builder.create_vector(&responses);
                        let input_offset = FbsRequestUserInput::create(
                            &mut builder,
                            &RequestUserInputArgs {
                                request_id: input.request_id,
                                message: Some(message_data),
                                responses: Some(responses_offset),
                            },
                        );
                        let msg = FbsOutboundDelegateMsg::create(
                            &mut builder,
                            &OutboundDelegateMsgArgs {
                                inbound_type: OutboundDelegateMsgType::RequestUserInput,
                                inbound: Some(input_offset.as_union_value()),
                            },
                        );
                        messages.push(msg);
                    }
                    OutboundDelegateMsg::ContextUpdated(context) => {
                        let context_data = builder.create_vector(context.as_ref());
                        let context_offset = FbsContextUpdated::create(
                            &mut builder,
                            &ContextUpdatedArgs {
                                context: Some(context_data),
                            },
                        );
                        let msg = FbsOutboundDelegateMsg::create(
                            &mut builder,
                            &OutboundDelegateMsgArgs {
                                inbound_type: OutboundDelegateMsgType::ContextUpdated,
                                inbound: Some(context_offset.as_union_value()),
                            },
                        );
                        messages.push(msg);
                    }
                    OutboundDelegateMsg::GetContractRequest(_) => {
                        // GetContractRequest should be handled by the executor and never
                        // reach client serialization. If we get here, it's a bug.
                        tracing::error!(
                            "GetContractRequest reached client serialization - this is a bug"
                        );
                    }
                    OutboundDelegateMsg::PutContractRequest(_) => {
                        // PutContractRequest should be handled by the executor and never
                        // reach client serialization. If we get here, it's a bug.
                        tracing::error!(
                            "PutContractRequest reached client serialization - this is a bug"
                        );
                    }
                    OutboundDelegateMsg::UpdateContractRequest(_) => {
                        tracing::error!(
                            "UpdateContractRequest reached client serialization - this is a bug"
                        );
                    }
                    OutboundDelegateMsg::SubscribeContractRequest(_) => {
                        tracing::error!(
                            "SubscribeContractRequest reached client serialization - this is a bug"
                        );
                    }
                    OutboundDelegateMsg::SendDelegateMessage(_) => {
                        tracing::error!(
                            "SendDelegateMessage reached client serialization - this is a bug"
                        );
                    }
                });
                let messages_offset = builder.create_vector(&messages);
                let delegate_response_offset = FbsDelegateResponse::create(
                    &mut builder,
                    &DelegateResponseArgs {
                        key: Some(key_offset),
                        values: Some(messages_offset),
                    },
                );
                let host_response_offset = FbsHostResponse::create(
                    &mut builder,
                    &HostResponseArgs {
                        response_type: HostResponseType::DelegateResponse,
                        response: Some(delegate_response_offset.as_union_value()),
                    },
                );
                finish_host_response_buffer(&mut builder, host_response_offset);
                Ok(builder.finished_data().to_vec())
            }
            HostResponse::Ok => {
                let ok_offset = FbsOk::create(&mut builder, &OkArgs { msg: None });
                let host_response_offset = FbsHostResponse::create(
                    &mut builder,
                    &HostResponseArgs {
                        response_type: HostResponseType::Ok,
                        response: Some(ok_offset.as_union_value()),
                    },
                );
                finish_host_response_buffer(&mut builder, host_response_offset);
                Ok(builder.finished_data().to_vec())
            }
            HostResponse::QueryResponse(_) => unimplemented!(),
        }
    }
}

impl Display for HostResponse {
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
                ContractResponse::SubscribeResponse { key, .. } => {
                    f.write_fmt(format_args!("subscribe response for `{key}`"))
                }
                ContractResponse::NotFound { instance_id } => {
                    f.write_fmt(format_args!("not found for `{instance_id}`"))
                }
            },
            HostResponse::DelegateResponse { .. } => write!(f, "delegate responses"),
            HostResponse::Ok => write!(f, "ok response"),
            HostResponse::QueryResponse(_) => write!(f, "query response"),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
#[non_exhaustive]
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
        #[serde(deserialize_with = "UpdateData::deser_update_data")]
        update: UpdateData<'static>,
    },
    /// Successful update
    UpdateResponse {
        key: ContractKey,
        #[serde(deserialize_with = "StateSummary::deser_state_summary")]
        summary: StateSummary<'static>,
    },
    SubscribeResponse {
        key: ContractKey,
        subscribed: bool,
    },
    /// Contract was not found after exhaustive search.
    /// This is an explicit response that distinguishes "contract doesn't exist"
    /// from other failure modes like timeouts or network errors.
    NotFound {
        /// The instance ID that was searched for.
        instance_id: ContractInstanceId,
    },
}

impl<T> From<ContractResponse<T>> for HostResponse<T> {
    fn from(value: ContractResponse<T>) -> HostResponse<T> {
        HostResponse::ContractResponse(value)
    }
}

#[cfg(test)]
mod client_request_test {
    use crate::client_api::{ContractRequest, TryFromFbs};
    use crate::contract_interface::UpdateData;
    use crate::generated::client_request::root_as_client_request;

    const EXPECTED_ENCODED_CONTRACT_ID: &str = "6kVs66bKaQAC6ohr8b43SvJ95r36tc2hnG7HezmaJHF9";

    #[test]
    fn test_build_contract_put_op_from_fbs() -> Result<(), Box<dyn std::error::Error>> {
        let put_req_op = vec![
            4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 1, 198, 255, 255, 255, 12, 0, 0, 0, 20, 0, 0, 0, 36, 0,
            0, 0, 170, 255, 255, 255, 4, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8,
            8, 0, 10, 0, 9, 0, 4, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 1, 10, 0, 16, 0, 12, 0, 8, 0, 4,
            0, 10, 0, 0, 0, 12, 0, 0, 0, 76, 0, 0, 0, 92, 0, 0, 0, 176, 255, 255, 255, 8, 0, 0, 0,
            16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0,
            85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75,
            210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6,
            7, 8, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 8, 0, 0, 0, 1, 2,
            3, 4, 5, 6, 7, 8, 8, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let request = if let Ok(client_request) = root_as_client_request(&put_req_op) {
            let contract_request = client_request.client_request_as_contract_request().unwrap();
            ContractRequest::try_decode_fbs(&contract_request)?
        } else {
            panic!("failed to decode client request")
        };

        match request {
            ContractRequest::Put {
                contract,
                state,
                related_contracts: _,
                subscribe,
                blocking_subscribe,
            } => {
                assert_eq!(
                    contract.to_string(),
                    "WasmContainer([api=0.0.1](D8fdVLbRyMLw5mZtPRpWMFcrXGN2z8Nq8UGcLGPFBg2W))"
                );
                assert_eq!(contract.unwrap_v1().data.data(), &[1, 2, 3, 4, 5, 6, 7, 8]);
                assert_eq!(state.to_vec(), &[1, 2, 3, 4, 5, 6, 7, 8]);
                assert!(!subscribe);
                assert!(!blocking_subscribe);
            }
            _ => panic!("wrong contract request type"),
        }

        Ok(())
    }

    #[test]
    fn test_build_contract_get_op_from_fbs() -> Result<(), Box<dyn std::error::Error>> {
        let get_req_op = vec![
            4, 0, 0, 0, 244, 255, 255, 255, 16, 0, 0, 0, 0, 0, 0, 1, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 3, 222, 255, 255, 255, 12, 0, 0, 0, 8, 0, 12, 0, 8, 0, 4,
            0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0,
            4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40, 85, 240, 177, 207, 81, 106, 157, 173,
            90, 234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75, 26, 229, 230, 107, 167, 17, 108,
        ];
        let request = if let Ok(client_request) = root_as_client_request(&get_req_op) {
            let contract_request = client_request.client_request_as_contract_request().unwrap();
            ContractRequest::try_decode_fbs(&contract_request)?
        } else {
            panic!("failed to decode client request")
        };

        match request {
            ContractRequest::Get {
                key,
                return_contract_code: fetch_contract,
                subscribe,
                blocking_subscribe,
            } => {
                assert_eq!(key.encode(), EXPECTED_ENCODED_CONTRACT_ID);
                assert!(!fetch_contract);
                assert!(!subscribe);
                assert!(!blocking_subscribe);
            }
            _ => panic!("wrong contract request type"),
        }

        Ok(())
    }

    #[test]
    fn test_build_contract_update_op_from_fbs() -> Result<(), Box<dyn std::error::Error>> {
        let update_op = vec![
            4, 0, 0, 0, 220, 255, 255, 255, 8, 0, 0, 0, 0, 0, 0, 1, 232, 255, 255, 255, 8, 0, 0, 0,
            0, 0, 0, 2, 204, 255, 255, 255, 16, 0, 0, 0, 52, 0, 0, 0, 8, 0, 12, 0, 11, 0, 4, 0, 8,
            0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 2, 210, 255, 255, 255, 4, 0, 0, 0, 8, 0, 0, 0, 1, 2, 3,
            4, 5, 6, 7, 8, 8, 0, 12, 0, 8, 0, 4, 0, 8, 0, 0, 0, 8, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 6, 0, 8, 0, 4, 0, 6, 0, 0, 0, 4, 0, 0, 0, 32, 0, 0, 0, 85, 111, 11, 171, 40,
            85, 240, 177, 207, 81, 106, 157, 173, 90, 234, 2, 250, 253, 75, 210, 62, 7, 6, 34, 75,
            26, 229, 230, 107, 167, 17, 108,
        ];
        let request = if let Ok(client_request) = root_as_client_request(&update_op) {
            let contract_request = client_request.client_request_as_contract_request().unwrap();
            ContractRequest::try_decode_fbs(&contract_request)?
        } else {
            panic!("failed to decode client request")
        };

        match request {
            ContractRequest::Update { key, data } => {
                assert_eq!(
                    key.encoded_contract_id(),
                    "6kVs66bKaQAC6ohr8b43SvJ95r36tc2hnG7HezmaJHF9"
                );
                match data {
                    UpdateData::Delta(delta) => {
                        assert_eq!(delta.to_vec(), &[1, 2, 3, 4, 5, 6, 7, 8])
                    }
                    _ => panic!("wrong update data type"),
                }
            }
            _ => panic!("wrong contract request type"),
        }

        Ok(())
    }
}
