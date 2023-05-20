use std::{
    borrow::{Borrow, Cow},
    fmt::Display,
    marker::PhantomData,
    ops::Deref,
    path::Path,
};

use blake2::{Blake2s256, Digest};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::code_hash::CodeHash;
use crate::prelude::ContractInstanceId;

const DELEGATE_HASH_LENGTH: usize = 32;

type Secret = Vec<u8>;

// FIXME
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Delegate<'a>(PhantomData<fn() -> &'a ()>);

impl<'a> Delegate<'a> {
    pub fn key(&self) -> &DelegateKey {
        todo!()
    }

    pub fn hash(&self) -> &CodeHash {
        todo!()
    }

    pub fn into_owned(self) -> Delegate<'static> {
        todo!()
    }
}

impl TryFrom<&Path> for Delegate<'static> {
    type Error = std::io::Error;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl From<Vec<u8>> for Delegate<'static> {
    fn from(data: Vec<u8>) -> Self {
        todo!()
    }
}

impl AsRef<[u8]> for Delegate<'_> {
    fn as_ref(&self) -> &[u8] {
        todo!()
    }
}

/// Executable delegate
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde_as]
pub struct DelegateCode<'a> {
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    code: Cow<'a, [u8]>,
    code_hash: CodeHash,
}

impl PartialEq for DelegateCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.code_hash == other.code_hash
    }
}

impl Eq for DelegateCode<'_> {}

impl DelegateCode<'_> {
    pub fn hash(&self) -> &CodeHash {
        &self.code_hash
    }

    pub fn into_owned(self) -> DelegateCode<'static> {
        DelegateCode {
            code_hash: self.code_hash,
            code: Cow::from(self.code.into_owned()),
        }
    }
}

impl AsRef<[u8]> for DelegateCode<'_> {
    fn as_ref(&self) -> &[u8] {
        self.code.borrow()
    }
}

impl TryFrom<&Path> for DelegateCode<'static> {
    type Error = std::io::Error;
    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        let code = Cow::from(std::fs::read(path)?);
        let key = CodeHash::new(code.borrow());
        Ok(Self {
            code,
            code_hash: key,
        })
    }
}

impl From<Vec<u8>> for DelegateCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = CodeHash::new(data.as_slice());
        DelegateCode {
            code: Cow::from(data),
            code_hash: key,
        }
    }
}

#[serde_as]
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct DelegateKey(#[serde_as(as = "[_; DELEGATE_HASH_LENGTH]")] [u8; DELEGATE_HASH_LENGTH]);

impl DelegateKey {
    pub fn new(wasm_code: &[u8]) -> Self {
        let mut hasher = Blake2s256::new();
        hasher.update(wasm_code);
        let hashed = hasher.finalize();
        let mut delegate_key = [0; DELEGATE_HASH_LENGTH];
        delegate_key.copy_from_slice(&hashed);
        Self(delegate_key)
    }

    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    pub fn code_hash(&self) -> &CodeHash {
        todo!()
    }
}

impl Display for DelegateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
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
        let mut hasher = Blake2s256::new();
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

    /// Returns the hash of the contract key only.
    pub fn code_hash(&self) -> &[u8; 32] {
        &self.hash
    }
}

impl Display for SecretsId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

pub trait DelegateInterface {
    /// Process inbound message, producing zero or more outbound messages in response
    /// Note that all state for the delegate must be stored using the secret mechanism.
    fn process(message: InboundDelegateMsg) -> Result<Vec<OutboundDelegateMsg>, DelegateError>;
}

#[non_exhaustive]
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DelegateContext(pub Vec<u8>);

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum InboundDelegateMsg<'a> {
    ApplicationMessage(ApplicationMessage),
    GetSecretResponse(GetSecretResponse),
    RandomBytes(Vec<u8>),
    UserResponse(#[serde(borrow)] UserInputResponse<'a>),
    // GetContractResponse {
    //     contract_id: ContractInstanceId,
    //     #[serde(borrow)]
    //     update_data: UpdateData<'static>,
    //     context: Context
    // },
}

impl InboundDelegateMsg<'_> {
    pub fn into_owned(self) -> InboundDelegateMsg<'static> {
        match self {
            InboundDelegateMsg::ApplicationMessage(r) => InboundDelegateMsg::ApplicationMessage(r),
            InboundDelegateMsg::GetSecretResponse(r) => InboundDelegateMsg::GetSecretResponse(r),
            InboundDelegateMsg::RandomBytes(b) => InboundDelegateMsg::RandomBytes(b),
            InboundDelegateMsg::UserResponse(r) => InboundDelegateMsg::UserResponse(r.into_owned()),
        }
    }

    pub fn get_context(&self) -> Option<&DelegateContext> {
        match self {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::GetSecretResponse(GetSecretResponse { context, .. }) => {
                Some(context)
            }
            _ => None,
        }
    }

    pub fn get_mut_context(&mut self) -> Option<&mut DelegateContext> {
        match self {
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            InboundDelegateMsg::GetSecretResponse(GetSecretResponse { context, .. }) => {
                Some(context)
            }
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GetSecretResponse {
    pub key: SecretsId,
    pub value: Option<Secret>,
    #[serde(skip)]
    pub context: DelegateContext,
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

#[derive(Serialize, Deserialize, Debug)]
pub enum OutboundDelegateMsg {
    // for the apps
    ApplicationMessage(ApplicationMessage),
    RequestUserInput(#[serde(deserialize_with = "deser_func")] UserInputRequest<'static>),
    // todo: remove when context can be accessed from the delegate environment and we pass it as reference
    ContextUpdated(DelegateContext),
    // from the node
    GetSecretRequest(GetSecretRequest),
    SetSecretRequest(SetSecretRequest),
    RandomBytesRequest(usize),
    // GetContractRequest {
    //     mode: RelatedMode,
    //     contract_id: ContractInstanceId,
    // },
}

impl From<GetSecretRequest> for OutboundDelegateMsg {
    fn from(req: GetSecretRequest) -> Self {
        Self::GetSecretRequest(req)
    }
}

impl From<ApplicationMessage> for OutboundDelegateMsg {
    fn from(req: ApplicationMessage) -> Self {
        Self::ApplicationMessage(req)
    }
}

impl OutboundDelegateMsg {
    pub fn processed(&self) -> bool {
        match self {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg.processed,
            OutboundDelegateMsg::GetSecretRequest(msg) => msg.processed,
            OutboundDelegateMsg::RandomBytesRequest(_) => false,
            OutboundDelegateMsg::SetSecretRequest(_) => false,
            OutboundDelegateMsg::RequestUserInput(_) => true,
            OutboundDelegateMsg::ContextUpdated(_) => true,
        }
    }

    pub fn get_context(&self) -> Option<&DelegateContext> {
        match self {
            OutboundDelegateMsg::ApplicationMessage(ApplicationMessage { context, .. }) => {
                Some(context)
            }
            OutboundDelegateMsg::GetSecretRequest(GetSecretRequest { context, .. }) => {
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
            OutboundDelegateMsg::GetSecretRequest(GetSecretRequest { context, .. }) => {
                Some(context)
            }
            _ => None,
        }
    }
}

fn deser_func<'de, D>(deser: D) -> Result<UserInputRequest<'static>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = <UserInputRequest<'de> as Deserialize>::deserialize(deser)?;
    Ok(value.into_owned())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetSecretRequest {
    pub key: SecretsId,
    pub context: DelegateContext,
    pub processed: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SetSecretRequest {
    pub key: SecretsId,
    /// Sets or unsets (if none) a value associated with the key.
    pub value: Option<Secret>,
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
    use crate::WasmLinearMem;

    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct DelegateInterfaceResult {
        ptr: i64,
        size: u32,
    }

    impl DelegateInterfaceResult {
        pub unsafe fn from_raw(ptr: i64, mem: &WasmLinearMem) -> Self {
            let result = Box::leak(Box::from_raw(crate::buf::compute_ptr(
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
            let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
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
