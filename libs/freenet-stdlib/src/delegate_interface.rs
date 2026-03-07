use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    fs::File,
    io::Read,
    ops::Deref,
    path::Path,
};

use blake3::{traits::digest::Digest, Hasher as Blake3};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

use crate::generated::client_request::{
    DelegateKey as FbsDelegateKey, InboundDelegateMsg as FbsInboundDelegateMsg,
    InboundDelegateMsgType,
};

use crate::common_generated::common::SecretsId as FbsSecretsId;

use crate::client_api::{TryFromFbs, WsApiError};
use crate::contract_interface::{RelatedContracts, UpdateData};
use crate::prelude::{ContractInstanceId, WrappedState, CONTRACT_KEY_SIZE};
use crate::versioning::ContractContainer;
use crate::{code_hash::CodeHash, prelude::Parameters};

const DELEGATE_HASH_LENGTH: usize = 32;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delegate<'a> {
    #[serde(borrow)]
    parameters: Parameters<'a>,
    #[serde(borrow)]
    pub data: DelegateCode<'a>,
    key: DelegateKey,
}

impl Delegate<'_> {
    pub fn key(&self) -> &DelegateKey {
        &self.key
    }

    pub fn code(&self) -> &DelegateCode<'_> {
        &self.data
    }

    pub fn code_hash(&self) -> &CodeHash {
        &self.data.code_hash
    }

    pub fn params(&self) -> &Parameters<'_> {
        &self.parameters
    }

    pub fn into_owned(self) -> Delegate<'static> {
        Delegate {
            parameters: self.parameters.into_owned(),
            data: self.data.into_owned(),
            key: self.key,
        }
    }

    pub fn size(&self) -> usize {
        self.parameters.size() + self.data.size()
    }

    pub(crate) fn deserialize_delegate<'de, D>(deser: D) -> Result<Delegate<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Delegate<'de> = Deserialize::deserialize(deser)?;
        Ok(data.into_owned())
    }
}

impl PartialEq for Delegate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Delegate<'_> {}

impl<'a> From<(&DelegateCode<'a>, &Parameters<'a>)> for Delegate<'a> {
    fn from((data, parameters): (&DelegateCode<'a>, &Parameters<'a>)) -> Self {
        Self {
            key: DelegateKey::from_params_and_code(parameters, data),
            parameters: parameters.clone(),
            data: data.clone(),
        }
    }
}

/// Executable delegate
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde_as]
pub struct DelegateCode<'a> {
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    pub(crate) data: Cow<'a, [u8]>,
    // todo: skip serializing and instead compute it
    pub(crate) code_hash: CodeHash,
}

impl DelegateCode<'static> {
    /// Loads the contract raw wasm module, without any version.
    pub fn load_raw(path: &Path) -> Result<Self, std::io::Error> {
        let contract_data = Self::load_bytes(path)?;
        Ok(DelegateCode::from(contract_data))
    }

    pub(crate) fn load_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(contract_data)
    }
}

impl DelegateCode<'_> {
    /// Delegate code hash.
    pub fn hash(&self) -> &CodeHash {
        &self.code_hash
    }

    /// Returns the `Base58` string representation of the delegate key.
    pub fn hash_str(&self) -> String {
        Self::encode_hash(&self.code_hash.0)
    }

    /// Reference to delegate code.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Returns the `Base58` string representation of a hash.
    pub fn encode_hash(hash: &[u8; DELEGATE_HASH_LENGTH]) -> String {
        bs58::encode(hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    pub fn into_owned(self) -> DelegateCode<'static> {
        DelegateCode {
            code_hash: self.code_hash,
            data: Cow::from(self.data.into_owned()),
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl PartialEq for DelegateCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.code_hash == other.code_hash
    }
}

impl Eq for DelegateCode<'_> {}

impl AsRef<[u8]> for DelegateCode<'_> {
    fn as_ref(&self) -> &[u8] {
        self.data.borrow()
    }
}

impl From<Vec<u8>> for DelegateCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = CodeHash::from_code(data.as_slice());
        DelegateCode {
            data: Cow::from(data),
            code_hash: key,
        }
    }
}

impl<'a> From<&'a [u8]> for DelegateCode<'a> {
    fn from(code: &'a [u8]) -> Self {
        let key = CodeHash::from_code(code);
        DelegateCode {
            data: Cow::from(code),
            code_hash: key,
        }
    }
}

#[serde_as]
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct DelegateKey {
    #[serde_as(as = "[_; DELEGATE_HASH_LENGTH]")]
    key: [u8; DELEGATE_HASH_LENGTH],
    code_hash: CodeHash,
}

impl From<DelegateKey> for SecretsId {
    fn from(key: DelegateKey) -> SecretsId {
        SecretsId {
            hash: key.key,
            key: vec![],
        }
    }
}

impl DelegateKey {
    pub const fn new(key: [u8; DELEGATE_HASH_LENGTH], code_hash: CodeHash) -> Self {
        Self { key, code_hash }
    }

    fn from_params_and_code<'a>(
        params: impl Borrow<Parameters<'a>>,
        wasm_code: impl Borrow<DelegateCode<'a>>,
    ) -> Self {
        let code = wasm_code.borrow();
        let key = generate_id(params.borrow(), code);
        Self {
            key,
            code_hash: *code.hash(),
        }
    }

    pub fn encode(&self) -> String {
        bs58::encode(self.key)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    pub fn code_hash(&self) -> &CodeHash {
        &self.code_hash
    }

    pub fn bytes(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn from_params(
        code_hash: impl Into<String>,
        parameters: &Parameters,
    ) -> Result<Self, bs58::decode::Error> {
        let mut code_key = [0; DELEGATE_HASH_LENGTH];
        bs58::decode(code_hash.into())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut code_key)?;
        let mut hasher = Blake3::new();
        hasher.update(code_key.as_slice());
        hasher.update(parameters.as_ref());
        let full_key_arr = hasher.finalize();

        debug_assert_eq!(full_key_arr[..].len(), DELEGATE_HASH_LENGTH);
        let mut key = [0; DELEGATE_HASH_LENGTH];
        key.copy_from_slice(&full_key_arr);

        Ok(Self {
            key,
            code_hash: CodeHash(code_key),
        })
    }
}

impl Deref for DelegateKey {
    type Target = [u8; DELEGATE_HASH_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

impl Display for DelegateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

impl<'a> TryFromFbs<&FbsDelegateKey<'a>> for DelegateKey {
    fn try_decode_fbs(key: &FbsDelegateKey<'a>) -> Result<Self, WsApiError> {
        let mut key_bytes = [0; DELEGATE_HASH_LENGTH];
        key_bytes.copy_from_slice(key.key().bytes().iter().as_ref());
        Ok(DelegateKey {
            key: key_bytes,
            code_hash: CodeHash::from_code(key.code_hash().bytes()),
        })
    }
}

/// Type of errors during interaction with a delegate.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum DelegateError {
    #[error("de/serialization error: {0}")]
    Deser(String),
    #[error("{0}")]
    Other(String),
}

fn generate_id<'a>(
    parameters: &Parameters<'a>,
    code_data: &DelegateCode<'a>,
) -> [u8; DELEGATE_HASH_LENGTH] {
    let contract_hash = code_data.hash();

    let mut hasher = Blake3::new();
    hasher.update(contract_hash.0.as_slice());
    hasher.update(parameters.as_ref());
    let full_key_arr = hasher.finalize();

    debug_assert_eq!(full_key_arr[..].len(), DELEGATE_HASH_LENGTH);
    let mut key = [0; DELEGATE_HASH_LENGTH];
    key.copy_from_slice(&full_key_arr);
    key
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SecretsId {
    #[serde_as(as = "serde_with::Bytes")]
    key: Vec<u8>,
    #[serde_as(as = "[_; 32]")]
    hash: [u8; 32],
}

impl SecretsId {
    pub fn new(key: Vec<u8>) -> Self {
        let mut hasher = Blake3::new();
        hasher.update(&key);
        let hashed = hasher.finalize();
        let mut hash = [0; 32];
        hash.copy_from_slice(&hashed);
        Self { key, hash }
    }

    pub fn encode(&self) -> String {
        bs58::encode(self.hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    pub fn hash(&self) -> &[u8; 32] {
        &self.hash
    }
    pub fn key(&self) -> &[u8] {
        self.key.as_slice()
    }
}

impl Display for SecretsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

impl<'a> TryFromFbs<&FbsSecretsId<'a>> for SecretsId {
    fn try_decode_fbs(key: &FbsSecretsId<'a>) -> Result<Self, WsApiError> {
        let mut key_hash = [0; 32];
        key_hash.copy_from_slice(key.hash().bytes().iter().as_ref());
        Ok(SecretsId {
            key: key.key().bytes().to_vec(),
            hash: key_hash,
        })
    }
}

/// A Delegate is a webassembly code designed to act as an agent for the user on
/// Freenet. Delegates can:
///
///  * Store private data on behalf of the user
///  * Create, read, and modify contracts
///  * Create other delegates
///  * Send and receive messages from other delegates and user interfaces
///  * Ask the user questions and receive answers
///
/// Example use cases:
///
///  * A delegate stores a private key for the user, other components can ask
///    the delegate to sign messages, it will ask the user for permission
///  * A delegate monitors an inbox contract and downloads new messages when
///    they arrive
///
/// # Example
///
/// ```ignore
/// use freenet_stdlib::prelude::*;
///
/// struct MyDelegate;
///
/// #[delegate]
/// impl DelegateInterface for MyDelegate {
///     fn process(
///         ctx: &mut DelegateCtx,
///         _params: Parameters<'static>,
///         _attested: Option<&'static [u8]>,
///         message: InboundDelegateMsg,
///     ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
///         // Access secrets synchronously - no round-trip needed!
///         if let Some(key) = ctx.get_secret(b"private_key") {
///             // use key...
///         }
///         ctx.set_secret(b"new_key", b"value");
///
///         // Read/write context for temporary state within a batch
///         ctx.write(b"some state");
///
///         Ok(vec![])
///     }
/// }
/// ```
pub trait DelegateInterface {
    /// Process inbound message, producing zero or more outbound messages in response.
    ///
    /// # Arguments
    /// - `ctx`: Mutable handle to the delegate's execution environment. Provides:
    ///   - **Context** (temporary): `read()`, `write()`, `len()`, `clear()` - state within a batch
    ///   - **Secrets** (persistent): `get_secret()`, `set_secret()`, `has_secret()`, `remove_secret()`
    /// - `parameters`: The delegate's initialization parameters.
    /// - `attested`: An optional identifier for the client of this function. Usually
    ///   will be a [`ContractInstanceId`].
    /// - `message`: The inbound message to process.
    fn process(
        ctx: &mut crate::delegate_host::DelegateCtx,
        parameters: Parameters<'static>,
        attested: Option<&'static [u8]>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError>;
}

#[serde_as]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DelegateContext(#[serde_as(as = "serde_with::Bytes")] Vec<u8>);

impl DelegateContext {
    pub const MAX_SIZE: usize = 4096 * 10 * 10;

    pub fn new(bytes: Vec<u8>) -> Self {
        assert!(bytes.len() < Self::MAX_SIZE);
        Self(bytes)
    }

    pub fn append(&mut self, bytes: &mut Vec<u8>) {
        assert!(self.0.len() + bytes.len() < Self::MAX_SIZE);
        self.0.append(bytes)
    }

    pub fn replace(&mut self, bytes: Vec<u8>) {
        assert!(bytes.len() < Self::MAX_SIZE);
        let _ = std::mem::replace(&mut self.0, bytes);
    }
}

impl AsRef<[u8]> for DelegateContext {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InboundDelegateMsg<'a> {
    ApplicationMessage(ApplicationMessage),
    UserResponse(#[serde(borrow)] UserInputResponse<'a>),
    GetContractResponse(GetContractResponse),
    PutContractResponse(PutContractResponse),
    UpdateContractResponse(UpdateContractResponse),
    SubscribeContractResponse(SubscribeContractResponse),
    ContractNotification(ContractNotification),
    DelegateMessage(DelegateMessage),
}

impl InboundDelegateMsg<'_> {
    pub fn into_owned(self) -> InboundDelegateMsg<'static> {
        match self {
            InboundDelegateMsg::ApplicationMessage(r) => InboundDelegateMsg::ApplicationMessage(r),
            InboundDelegateMsg::UserResponse(r) => InboundDelegateMsg::UserResponse(r.into_owned()),
            InboundDelegateMsg::GetContractResponse(r) => {
                InboundDelegateMsg::GetContractResponse(r)
            }
            InboundDelegateMsg::PutContractResponse(r) => {
                InboundDelegateMsg::PutContractResponse(r)
            }
            InboundDelegateMsg::UpdateContractResponse(r) => {
                InboundDelegateMsg::UpdateContractResponse(r)
            }
            InboundDelegateMsg::SubscribeContractResponse(r) => {
                InboundDelegateMsg::SubscribeContractResponse(r)
            }
            InboundDelegateMsg::ContractNotification(r) => {
                InboundDelegateMsg::ContractNotification(r)
            }
            InboundDelegateMsg::DelegateMessage(r) => InboundDelegateMsg::DelegateMessage(r),
        }
    }

    pub fn get_context(&self) -> Option<&DelegateContext> {
        match self {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::GetContractResponse(GetContractResponse { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::PutContractResponse(PutContractResponse { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::UpdateContractResponse(UpdateContractResponse {
                context, ..
            }) => Some(context),
            InboundDelegateMsg::SubscribeContractResponse(SubscribeContractResponse {
                context,
                ..
            }) => Some(context),
            InboundDelegateMsg::ContractNotification(ContractNotification { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::DelegateMessage(DelegateMessage { context, .. }) => Some(context),
            _ => None,
        }
    }

    pub fn get_mut_context(&mut self) -> Option<&mut DelegateContext> {
        match self {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::GetContractResponse(GetContractResponse { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::PutContractResponse(PutContractResponse { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::UpdateContractResponse(UpdateContractResponse {
                context, ..
            }) => Some(context),
            InboundDelegateMsg::SubscribeContractResponse(SubscribeContractResponse {
                context,
                ..
            }) => Some(context),
            InboundDelegateMsg::ContractNotification(ContractNotification { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::DelegateMessage(DelegateMessage { context, .. }) => Some(context),
            _ => None,
        }
    }
}

impl From<ApplicationMessage> for InboundDelegateMsg<'_> {
    fn from(value: ApplicationMessage) -> Self {
        Self::ApplicationMessage(value)
    }
}

impl<'a> TryFromFbs<&FbsInboundDelegateMsg<'a>> for InboundDelegateMsg<'a> {
    fn try_decode_fbs(msg: &FbsInboundDelegateMsg<'a>) -> Result<Self, WsApiError> {
        match msg.inbound_type() {
            InboundDelegateMsgType::common_ApplicationMessage => {
                let app_msg = msg.inbound_as_common_application_message().unwrap();
                let mut instance_key_bytes = [0; CONTRACT_KEY_SIZE];
                instance_key_bytes
                    .copy_from_slice(app_msg.app().data().bytes().to_vec().as_slice());
                let app_msg = ApplicationMessage {
                    app: ContractInstanceId::new(instance_key_bytes),
                    payload: app_msg.payload().bytes().to_vec(),
                    context: DelegateContext::new(app_msg.context().bytes().to_vec()),
                    processed: app_msg.processed(),
                };
                Ok(InboundDelegateMsg::ApplicationMessage(app_msg))
            }
            InboundDelegateMsgType::UserInputResponse => {
                let user_response = msg.inbound_as_user_input_response().unwrap();
                let user_response = UserInputResponse {
                    request_id: user_response.request_id(),
                    response: ClientResponse::new(user_response.response().data().bytes().to_vec()),
                    context: DelegateContext::new(
                        user_response.delegate_context().bytes().to_vec(),
                    ),
                };
                Ok(InboundDelegateMsg::UserResponse(user_response))
            }
            _ => unreachable!("invalid inbound delegate message type"),
        }
    }
}

#[non_exhaustive]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApplicationMessage {
    pub app: ContractInstanceId,
    pub payload: Vec<u8>,
    pub context: DelegateContext,
    pub processed: bool,
}

impl ApplicationMessage {
    pub fn new(app: ContractInstanceId, payload: Vec<u8>) -> Self {
        Self {
            app,
            payload,
            context: DelegateContext::default(),
            processed: false,
        }
    }

    pub fn with_context(mut self, context: DelegateContext) -> Self {
        self.context = context;
        self
    }

    pub fn processed(mut self, p: bool) -> Self {
        self.processed = p;
        self
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserInputResponse<'a> {
    pub request_id: u32,
    #[serde(borrow)]
    pub response: ClientResponse<'a>,
    pub context: DelegateContext,
}

impl UserInputResponse<'_> {
    pub fn into_owned(self) -> UserInputResponse<'static> {
        UserInputResponse {
            request_id: self.request_id,
            response: self.response.into_owned(),
            context: self.context,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OutboundDelegateMsg {
    // for the apps
    ApplicationMessage(ApplicationMessage),
    RequestUserInput(
        #[serde(deserialize_with = "OutboundDelegateMsg::deser_user_input_req")]
        UserInputRequest<'static>,
    ),
    // todo: remove when context can be accessed from the delegate environment and we pass it as reference
    ContextUpdated(DelegateContext),
    GetContractRequest(GetContractRequest),
    PutContractRequest(PutContractRequest),
    UpdateContractRequest(UpdateContractRequest),
    SubscribeContractRequest(SubscribeContractRequest),
    SendDelegateMessage(DelegateMessage),
}

impl From<ApplicationMessage> for OutboundDelegateMsg {
    fn from(req: ApplicationMessage) -> Self {
        Self::ApplicationMessage(req)
    }
}

impl From<GetContractRequest> for OutboundDelegateMsg {
    fn from(req: GetContractRequest) -> Self {
        Self::GetContractRequest(req)
    }
}

impl From<PutContractRequest> for OutboundDelegateMsg {
    fn from(req: PutContractRequest) -> Self {
        Self::PutContractRequest(req)
    }
}

impl From<UpdateContractRequest> for OutboundDelegateMsg {
    fn from(req: UpdateContractRequest) -> Self {
        Self::UpdateContractRequest(req)
    }
}

impl From<SubscribeContractRequest> for OutboundDelegateMsg {
    fn from(req: SubscribeContractRequest) -> Self {
        Self::SubscribeContractRequest(req)
    }
}

impl From<DelegateMessage> for OutboundDelegateMsg {
    fn from(msg: DelegateMessage) -> Self {
        Self::SendDelegateMessage(msg)
    }
}

impl OutboundDelegateMsg {
    fn deser_user_input_req<'de, D>(deser: D) -> Result<UserInputRequest<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <UserInputRequest<'de> as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }

    pub fn processed(&self) -> bool {
        match self {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg.processed,
            OutboundDelegateMsg::GetContractRequest(msg) => msg.processed,
            OutboundDelegateMsg::PutContractRequest(msg) => msg.processed,
            OutboundDelegateMsg::UpdateContractRequest(msg) => msg.processed,
            OutboundDelegateMsg::SubscribeContractRequest(msg) => msg.processed,
            OutboundDelegateMsg::SendDelegateMessage(msg) => msg.processed,
            OutboundDelegateMsg::RequestUserInput(_) => true,
            OutboundDelegateMsg::ContextUpdated(_) => true,
        }
    }

    pub fn get_context(&self) -> Option<&DelegateContext> {
        match self {
            OutboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::GetContractRequest(GetContractRequest { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::PutContractRequest(PutContractRequest { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::UpdateContractRequest(UpdateContractRequest {
                context, ..
            }) => Some(context),
            OutboundDelegateMsg::SubscribeContractRequest(SubscribeContractRequest {
                context,
                ..
            }) => Some(context),
            OutboundDelegateMsg::SendDelegateMessage(DelegateMessage { context, .. }) => {
                Some(context)
            }
            _ => None,
        }
    }

    pub fn get_mut_context(&mut self) -> Option<&mut DelegateContext> {
        match self {
            OutboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::GetContractRequest(GetContractRequest { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::PutContractRequest(PutContractRequest { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::UpdateContractRequest(UpdateContractRequest {
                context, ..
            }) => Some(context),
            OutboundDelegateMsg::SubscribeContractRequest(SubscribeContractRequest {
                context,
                ..
            }) => Some(context),
            OutboundDelegateMsg::SendDelegateMessage(DelegateMessage { context, .. }) => {
                Some(context)
            }
            _ => None,
        }
    }
}

/// Request to get contract state from within a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetContractRequest {
    pub contract_id: ContractInstanceId,
    pub context: DelegateContext,
    pub processed: bool,
}

impl GetContractRequest {
    pub fn new(contract_id: ContractInstanceId) -> Self {
        Self {
            contract_id,
            context: Default::default(),
            processed: false,
        }
    }
}

/// Response containing contract state for a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetContractResponse {
    pub contract_id: ContractInstanceId,
    /// The contract state, or None if the contract was not found locally.
    pub state: Option<WrappedState>,
    pub context: DelegateContext,
}

/// Request to store a new contract from within a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PutContractRequest {
    /// The contract code and parameters.
    pub contract: ContractContainer,
    /// The initial state for the contract.
    pub state: WrappedState,
    /// Related contracts that this contract depends on.
    #[serde(deserialize_with = "RelatedContracts::deser_related_contracts")]
    pub related_contracts: RelatedContracts<'static>,
    /// Context for the delegate.
    pub context: DelegateContext,
    /// Whether this request has been processed.
    pub processed: bool,
}

impl PutContractRequest {
    pub fn new(
        contract: ContractContainer,
        state: WrappedState,
        related_contracts: RelatedContracts<'static>,
    ) -> Self {
        Self {
            contract,
            state,
            related_contracts,
            context: Default::default(),
            processed: false,
        }
    }
}

/// Response after attempting to store a contract from a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PutContractResponse {
    /// The ID of the contract that was (attempted to be) stored.
    pub contract_id: ContractInstanceId,
    /// Success (Ok) or error message (Err).
    pub result: Result<(), String>,
    /// Context for the delegate.
    pub context: DelegateContext,
}

/// Request to update an existing contract's state from within a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateContractRequest {
    /// The contract to update.
    pub contract_id: ContractInstanceId,
    /// The update to apply (full state or delta).
    #[serde(deserialize_with = "UpdateContractRequest::deser_update_data")]
    pub update: UpdateData<'static>,
    /// Context for the delegate.
    pub context: DelegateContext,
    /// Whether this request has been processed.
    pub processed: bool,
}

impl UpdateContractRequest {
    pub fn new(contract_id: ContractInstanceId, update: UpdateData<'static>) -> Self {
        Self {
            contract_id,
            update,
            context: Default::default(),
            processed: false,
        }
    }

    fn deser_update_data<'de, D>(deser: D) -> Result<UpdateData<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = <UpdateData<'de> as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }
}

/// Response after attempting to update a contract from a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateContractResponse {
    /// The contract that was updated.
    pub contract_id: ContractInstanceId,
    /// Success (Ok) or error message (Err).
    pub result: Result<(), String>,
    /// Context for the delegate.
    pub context: DelegateContext,
}

/// Request to subscribe to a contract's state changes from within a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeContractRequest {
    /// The contract to subscribe to.
    pub contract_id: ContractInstanceId,
    /// Context for the delegate.
    pub context: DelegateContext,
    /// Whether this request has been processed.
    pub processed: bool,
}

impl SubscribeContractRequest {
    pub fn new(contract_id: ContractInstanceId) -> Self {
        Self {
            contract_id,
            context: Default::default(),
            processed: false,
        }
    }
}

/// Response after attempting to subscribe to a contract from a delegate.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SubscribeContractResponse {
    /// The contract subscribed to.
    pub contract_id: ContractInstanceId,
    /// Success (Ok) or error message (Err).
    pub result: Result<(), String>,
    /// Context for the delegate.
    pub context: DelegateContext,
}

/// A message sent from one delegate to another.
///
/// Delegates can communicate with each other by emitting
/// `OutboundDelegateMsg::SendDelegateMessage` with a `DelegateMessage` targeting
/// another delegate. The runtime delivers it as `InboundDelegateMsg::DelegateMessage`
/// to the target delegate's `process()` function.
///
/// The `sender` field is overwritten by the runtime with the actual sender's key
/// (sender attestation), so delegates cannot spoof their identity.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DelegateMessage {
    /// The delegate to deliver this message to.
    pub target: DelegateKey,
    /// The delegate that sent this message (overwritten by runtime for attestation).
    pub sender: DelegateKey,
    /// Arbitrary message payload.
    pub payload: Vec<u8>,
    /// Delegate context, carried through the processing pipeline.
    pub context: DelegateContext,
    /// Runtime protocol flag indicating whether this message has been delivered.
    pub processed: bool,
}

impl DelegateMessage {
    pub fn new(target: DelegateKey, sender: DelegateKey, payload: Vec<u8>) -> Self {
        Self {
            target,
            sender,
            payload,
            context: DelegateContext::default(),
            processed: false,
        }
    }
}

/// Notification delivered to a delegate when a subscribed contract's state changes.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ContractNotification {
    /// The contract whose state changed.
    pub contract_id: ContractInstanceId,
    /// The new state of the contract.
    pub new_state: WrappedState,
    /// Context for the delegate.
    pub context: DelegateContext,
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NotificationMessage<'a>(
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl TryFrom<&serde_json::Value> for NotificationMessage<'static> {
    type Error = ();

    fn try_from(json: &serde_json::Value) -> Result<NotificationMessage<'static>, ()> {
        // todo: validate format when we have a better idea of what we want here
        let bytes = serde_json::to_vec(json).unwrap();
        Ok(Self(Cow::Owned(bytes)))
    }
}

impl NotificationMessage<'_> {
    pub fn into_owned(self) -> NotificationMessage<'static> {
        NotificationMessage(self.0.into_owned().into())
    }
    pub fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse<'a>(
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl Deref for ClientResponse<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ClientResponse<'_> {
    pub fn new(response: Vec<u8>) -> Self {
        Self(response.into())
    }
    pub fn into_owned(self) -> ClientResponse<'static> {
        ClientResponse(self.0.into_owned().into())
    }
    pub fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserInputRequest<'a> {
    pub request_id: u32,
    #[serde(borrow)]
    /// An interpretable message by the notification system.
    pub message: NotificationMessage<'a>,
    /// If a response is required from the user they can be chosen from this list.
    pub responses: Vec<ClientResponse<'a>>,
}

impl UserInputRequest<'_> {
    pub fn into_owned(self) -> UserInputRequest<'static> {
        UserInputRequest {
            request_id: self.request_id,
            message: self.message.into_owned(),
            responses: self.responses.into_iter().map(|r| r.into_owned()).collect(),
        }
    }
}

#[doc(hidden)]
pub(crate) mod wasm_interface {
    //! Contains all the types to interface between the host environment and
    //! the wasm module execution.
    use super::*;
    use crate::memory::WasmLinearMem;

    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct DelegateInterfaceResult {
        ptr: i64,
        size: u32,
    }

    impl DelegateInterfaceResult {
        pub unsafe fn from_raw(ptr: i64, mem: &WasmLinearMem) -> Self {
            let result = Box::leak(Box::from_raw(crate::memory::buf::compute_ptr(
                ptr as *mut Self,
                mem,
            )));
            #[cfg(feature = "trace")]
            {
                tracing::trace!(
                    "got FFI result @ {ptr} ({:p}) -> {result:?}",
                    ptr as *mut Self
                );
            }
            *result
        }

        #[cfg(feature = "contract")]
        pub fn into_raw(self) -> i64 {
            #[cfg(feature = "trace")]
            {
                tracing::trace!("returning FFI -> {self:?}");
            }
            let ptr = Box::into_raw(Box::new(self));
            #[cfg(feature = "trace")]
            {
                tracing::trace!("FFI result ptr: {ptr:p} ({}i64)", ptr as i64);
            }
            ptr as _
        }

        pub unsafe fn unwrap(
            self,
            mem: WasmLinearMem,
        ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
            let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
            let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
            let value: Result<Vec<OutboundDelegateMsg>, DelegateError> =
                bincode::deserialize(serialized)
                    .map_err(|e| DelegateError::Other(format!("{e}")))?;
            #[cfg(feature = "trace")]
            {
                tracing::trace!(
                    "got result through FFI; addr: {:p} ({}i64, mapped: {ptr:p})
                     serialized: {serialized:?}
                     value: {value:?}",
                    self.ptr as *mut u8,
                    self.ptr
                );
            }
            value
        }
    }

    impl From<Result<Vec<OutboundDelegateMsg>, DelegateError>> for DelegateInterfaceResult {
        fn from(value: Result<Vec<OutboundDelegateMsg>, DelegateError>) -> Self {
            let serialized = bincode::serialize(&value).unwrap();
            let size = serialized.len() as _;
            let ptr = serialized.as_ptr();
            #[cfg(feature = "trace")]
            {
                tracing::trace!(
                    "sending result through FFI; addr: {ptr:p} ({}),\n  serialized: {serialized:?}\n  value: {value:?}",
                    ptr as i64
                );
            }
            std::mem::forget(serialized);
            Self {
                ptr: ptr as i64,
                size,
            }
        }
    }
}
