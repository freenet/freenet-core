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
// Field is `pub(crate)` rather than private because the sibling module
// `client_events::combinator` reads `client_id.0` for tracing fields. Before
// this type was split out of `client_events.rs`, `combinator` was a child of
// the module that defined `ClientId` and could see the private field; as a
// sibling it cannot. `pub(crate)` restores exactly that crate-internal access
// with no widening beyond the crate boundary.
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

/// Where the client connection that issued a request reached the node from.
///
/// This is the trust boundary for **attested application identity**: the node
/// hands a delegate a [`freenet_stdlib::prelude::MessageOrigin`] naming the web
/// app on whose behalf a request runs, and delegates authorize on that name. The
/// client API has no authentication, so the only non-forgeable signal available
/// at the connection boundary is the peer address the kernel recorded for the
/// accepted socket (`ConnectInfo<SocketAddr>`) — the same anchor
/// `decide_user_token` (`client_events::websocket`) already uses to gate the durable
/// per-user token.
///
/// `Local` therefore means "this HOST", not "this user" — see the security
/// notes on [`Self::from_source_ip`].
///
/// **Fail closed:** anything that cannot be *proven* loopback is [`Self::Remote`],
/// including a missing `ConnectInfo`. [`Default`] is `Remote` for the same
/// reason: a construction site that forgets to set the scope loses attestation
/// (a visible functional failure) rather than granting it (a silent
/// vulnerability).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionScope {
    /// The kernel observed the peer address of this connection as loopback.
    Local,
    /// Off-host, or not provably loopback.
    #[default]
    Remote,
}

impl ConnectionScope {
    pub fn is_local(self) -> bool {
        matches!(self, Self::Local)
    }

    /// Classify a connection from the peer address the kernel recorded.
    ///
    /// # Security scope — read before relying on this
    ///
    /// A loopback peer address proves the connection was accepted over the
    /// loopback interface of THIS HOST. It does **not** prove:
    ///
    /// - **the same user.** A different local user account, or an app confined
    ///   from `$HOME` (Flatpak/Snap/container) but able to open a localhost
    ///   socket, is loopback and passes this check. (A same-user *unconfined*
    ///   process is out of scope entirely: it can read `secrets_dir/node_kek`
    ///   and decrypt directly, so no client-API gate could help it.)
    /// - **the same machine as the human.** Behind a colocated reverse proxy
    ///   every client presents the proxy's loopback address, so this classifies
    ///   every request as `Local` and the gate is a no-op for that deployment.
    ///
    /// `None` (no `ConnectInfo` on the request — only reachable from unit tests
    /// that omit it) is `Remote`: we cannot prove loopback, so we must not
    /// attest.
    pub fn from_source_ip(source_ip: Option<std::net::IpAddr>) -> Self {
        match source_ip {
            Some(ip) if is_loopback_source(ip) => Self::Local,
            _ => Self::Remote,
        }
    }
}

/// Whether a source IP is loopback (`127.0.0.0/8`, `::1`), after normalizing an
/// IPv4-mapped IPv6 source (`::ffff:127.0.0.1`) from a dual-stack socket.
///
/// Loopback is the trust anchor for two distinct per-connection decisions —
/// honoring a durable per-user token (`decide_user_token` (`client_events::websocket`))
/// and attesting an application identity ([`ConnectionScope::from_source_ip`]).
/// Both live on this ONE definition on purpose: two copies of "is this local"
/// would be free to drift, and a normalization fixed in one (the IPv4-mapped
/// case) would silently not reach the other.
///
/// Anything off-host cannot forge a loopback source — the kernel sets it from
/// the accepted socket (`ConnectInfo<SocketAddr>`) — so it is a sound,
/// non-spoofable signal that the connection did not cross the network.
pub(crate) fn is_loopback_source(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => v4.is_loopback(),
        std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => v4.is_loopback(),
            None => v6.is_loopback(),
        },
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
    /// Whether the issuing connection is entitled to an *attested* application
    /// identity (see [`ConnectionScope`]). Like `user_context` this rides
    /// alongside the request and is never read from the request body, so a
    /// client cannot forge it.
    pub connection_scope: ConnectionScope,
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
            connection_scope: ConnectionScope::default(),
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

    pub fn with_connection_scope(mut self, scope: ConnectionScope) -> Self {
        self.connection_scope = scope;
        self
    }

    pub fn with_user_context(mut self, user_context: Option<UserSecretContext>) -> Self {
        self.user_context = user_context;
        self
    }
}

#[cfg(test)]
mod connection_scope_tests {
    use super::*;
    use std::net::IpAddr;

    /// GHSA-824h-7x5x-wfmf: the whole gate rests on this classification, so it
    /// must fail CLOSED. A missing `ConnectInfo` cannot prove loopback and must
    /// not be read as local — the same fail-closed rule `decide_user_token`
    /// applies to the durable per-user token.
    #[test]
    fn missing_source_ip_is_remote() {
        assert_eq!(
            ConnectionScope::from_source_ip(None),
            ConnectionScope::Remote
        );
        assert!(!ConnectionScope::from_source_ip(None).is_local());
    }

    #[test]
    fn loopback_sources_are_local_including_ipv4_mapped() {
        for ip in [
            "127.0.0.1",
            // Anywhere in 127.0.0.0/8, not just .1.
            "127.4.5.6",
            "::1",
            // A dual-stack socket reports an IPv4 loopback peer in this form;
            // failing to normalize it would misclassify every local browser on
            // a dual-stack bind as Remote and break the shell outright.
            "::ffff:127.0.0.1",
        ] {
            let parsed: IpAddr = ip.parse().expect("test address must parse");
            assert_eq!(
                ConnectionScope::from_source_ip(Some(parsed)),
                ConnectionScope::Local,
                "{ip} must classify as Local"
            );
        }
    }

    #[test]
    fn off_host_sources_are_remote() {
        for ip in [
            // LAN — the case this fix deliberately breaks (see the PR's
            // limitations): a browser on another machine no longer gets an
            // attested identity.
            "192.168.1.50",
            "10.0.0.7",
            "5.9.111.215",
            "2001:db8::1",
            // An IPv4-mapped NON-loopback address must not be waved through by
            // the normalization above.
            "::ffff:192.168.1.50",
        ] {
            let parsed: IpAddr = ip.parse().expect("test address must parse");
            assert_eq!(
                ConnectionScope::from_source_ip(Some(parsed)),
                ConnectionScope::Remote,
                "{ip} must classify as Remote"
            );
        }
    }

    /// A construction site that forgets to set the scope must lose attestation
    /// (a visible functional failure), never grant it (a silent hole).
    #[test]
    fn default_scope_is_remote() {
        assert_eq!(ConnectionScope::default(), ConnectionScope::Remote);
        assert!(
            !OpenRequest::new(
                ClientId::FIRST,
                Box::new(freenet_stdlib::client_api::ClientRequest::Close),
            )
            .connection_scope
            .is_local()
        );
    }
}
