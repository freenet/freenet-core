use std::cell::Cell;
use std::fmt::Display;
use std::sync::Arc;

use freenet_stdlib::{
    client_api::{ClientError, ClientRequest, HostResponse},
    prelude::ContractInstanceId,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config::GlobalRng;
use crate::wasm_runtime::UserSecretContext;

pub type HostResult = Result<HostResponse, ClientError>;

/// Request correlation ID for end-to-end tracing
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct RequestId(u64);

const COUNTER_BLOCK: u64 = 1_000_000;

thread_local! {
    static REQUEST_ID_COUNTER: Cell<u64> = {
        let idx = crate::config::GlobalRng::thread_index();
        Cell::new(1 + idx * COUNTER_BLOCK)
    };
}

impl RequestId {
    pub fn new() -> Self {
        Self(REQUEST_ID_COUNTER.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        }))
    }

    /// Reset the request ID counter to initial state for this thread.
    /// Thread-local, so safe for parallel test execution.
    pub fn reset_counter() {
        let idx = crate::config::GlobalRng::thread_index();
        REQUEST_ID_COUNTER.with(|c| c.set(1 + idx * COUNTER_BLOCK));
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "req-{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ClientId(pub(crate) usize);

impl From<ClientId> for usize {
    fn from(val: ClientId) -> Self {
        val.0
    }
}

thread_local! {
    static CLIENT_ID_COUNTER: Cell<usize> = {
        let idx = crate::config::GlobalRng::thread_index();
        Cell::new(1 + (idx as usize) * (COUNTER_BLOCK as usize))
    };
}

impl ClientId {
    pub const FIRST: Self = ClientId(0);

    pub fn next() -> Self {
        ClientId(CLIENT_ID_COUNTER.with(|c| {
            let v = c.get();
            c.set(v + 1);
            v
        }))
    }

    /// Reset the client ID counter to initial state for this thread.
    /// Thread-local, so safe for parallel test execution.
    pub fn reset_counter() {
        let idx = crate::config::GlobalRng::thread_index();
        CLIENT_ID_COUNTER.with(|c| c.set(1 + (idx as usize) * (COUNTER_BLOCK as usize)));
    }
}

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthToken(#[serde(deserialize_with = "AuthToken::deser_auth_token")] Arc<str>);

impl AuthToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn generate() -> AuthToken {
        let mut token = [0u8; 32];
        GlobalRng::fill_bytes(&mut token);
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
    pub request_id: RequestId,
    pub request: Box<ClientRequest<'a>>,
    pub notification_channel: Option<mpsc::Sender<HostResult>>,
    pub token: Option<AuthToken>,
    pub origin_contract: Option<ContractInstanceId>,
    /// Per-connection per-user secret namespace (hosted mode, P2 of #4381),
    /// derived once at the WS connection boundary from the connection's user
    /// token. `None` outside hosted mode or when no token was presented. This
    /// is carried alongside the request — never read from the request body —
    /// so it cannot be forged by a client.
    pub user_context: Option<UserSecretContext>,
}

impl Display for OpenRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "client request {{ client: {}, request_id: {}, req: {} }}",
            &self.client_id, &self.request_id, &*self.request
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
            request_id: RequestId::new(),
            request,
            notification_channel: None,
            token: None,
            origin_contract: None,
            user_context: None,
        }
    }

    pub fn with_notification(mut self, ch: mpsc::Sender<HostResult>) -> Self {
        self.notification_channel = Some(ch);
        self
    }

    pub fn with_token(mut self, token: Option<AuthToken>) -> Self {
        self.token = token;
        self
    }

    pub fn with_origin_contract(mut self, contract: Option<ContractInstanceId>) -> Self {
        self.origin_contract = contract;
        self
    }

    pub fn with_user_context(mut self, user_context: Option<UserSecretContext>) -> Self {
        self.user_context = user_context;
        self
    }
}
