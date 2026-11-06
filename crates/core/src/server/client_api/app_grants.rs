//! User-approved app grants for native (non-browser) clients (#5264).
//!
//! A browser tab gets its app identity from the shell page the node serves for
//! that app. A native client, such as a desktop build of a Freenet app, has no
//! shell page. This module lets it ask for the identity of one web app, have
//! the user approve that in the browser, and receive a durable, revocable token.
//! A WebSocket connection presenting the token (as `Authorization: Bearer`) is
//! then treated exactly like a browser connection for that app, so delegates
//! see `MessageOrigin::WebApp(<app>)` and the native client sees the same
//! delegate data the app's web UI does.
//!
//! # Flow
//!
//! 1. `POST /v1/app-grants/request` `{ "app": "<contract instance id>",
//!    "client_name": "..." }` returns `{ request_id, approve_url,
//!    expires_in_secs }`.
//! 2. The client opens `approve_url` in the user's browser and long-polls
//!    `GET /v1/app-grants/request/<request_id>` (each poll waits up to 30 s).
//! 3. The node-authored approval page shows the app's contract id, and the
//!    client's self-reported name labelled as unverified, with Allow / Deny.
//! 4. On Allow the node mints the token. The first poll that sees it stores
//!    `sha256(token)` and returns the token.
//!
//! `request_id` is known only to the client (it is never in a browser URL), so
//! the page's URL alone cannot collect the token. The page is addressed by a
//! separate `approve_id`.
//!
//! # Collecting the token more than once
//!
//! The grant is stored once, by the first poll that collects it. If that
//! response is lost (the client crashed, the connection dropped), the holder of
//! `request_id` may poll again and receives the SAME token, so an approval is
//! not wasted and no orphaned grant is left behind. This stays possible until
//! the request expires ([`REQUEST_TTL`]) or the token is first used to open a
//! WebSocket connection, whichever is first; after either, the token exists
//! only in the client.
//!
//! # Gates
//!
//! - Request and poll: loopback peer and loopback `Host`; no `Origin` and no
//!   `Sec-Fetch-*` header, so a browser (or web content) cannot call them;
//!   refused on a hosted node and when any forwarding header is present.
//! - Approval page and approve POST: loopback peer and loopback `Host` (so a
//!   DNS-rebound page is refused), no forwarding headers, not hosted. The page
//!   is refused when the browser says it was requested by another site or not
//!   as a top-level document. The POST additionally needs an `Origin` exactly
//!   equal to the node's own origin and the single-use nonce minted for the
//!   page. The page may not be framed.
//! - The Apps and permissions list and revoke routes use the same loopback
//!   gate, so the list is shown exactly where Revoke works.
//! - A request expires 10 minutes after it was made, approved or not.
//! - On the WebSocket, a grant token is honoured only from the `Authorization`
//!   header, only for a connection classified as local, and not when the
//!   upgrade request carries forwarding headers.
//!
//! The forwarding-header check is a heuristic. A reverse proxy that adds no
//! forwarding headers and rewrites `Host` to a loopback name passes it. That is
//! the same loopback trust boundary the shell-page tokens already rest on: the
//! node cannot tell such a proxy's clients from local ones.
//!
//! # Revocation
//!
//! Grants are listed on the Apps and permissions page with a Revoke button.
//! Revoking deletes the stored row and closes every live connection that was
//! opened with the token.
//!
//! # What this does NOT protect against
//!
//! A malicious program already running as the same user can fetch the approval
//! page itself and click Allow. The approval step defends against web content
//! and gives the user a visible, revocable record; it is not a boundary
//! between local processes of the same user. Closing that needs the
//! authentication half of #5264 (for example passkey-gated approval), which
//! can be added to the approval step without changing the native client's side.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::{ConnectInfo, Path};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::hosted_export::ExportOpManagerHandle;
use super::permission_prompts::{
    host_is_loopback, html_escape, origin_is_exactly_same_origin, peer_is_loopback,
    refuse_when_hosted,
};
use crate::client_events::AuthToken;
use crate::contract::delegate_capabilities::{
    AppIdentity, DelegateCapabilities, GrantToken, GrantTokenHash, MAX_NATIVE_GRANTS,
    sanitize_client_name,
};

/// How long a request stays answerable, approved or not.
pub(crate) const REQUEST_TTL: Duration = Duration::from_secs(10 * 60);

/// How long one poll waits for the user before answering "pending".
pub(crate) const POLL_WAIT: Duration = Duration::from_secs(30);

/// Requests waiting for an answer at once. A user approves one app at a time;
/// the cap bounds what a local caller can make the node hold.
const MAX_PENDING_REQUESTS: usize = 16;

/// Polls held open at once.
const MAX_CONCURRENT_POLLS: usize = 32;

/// The Allow button is enabled only after this delay, so a click aimed at
/// something else the moment the page opens does not land on it.
const ALLOW_DELAY_MS: u64 = 1500;

/// `status`/`error` value a client can act on when the node already holds
/// [`MAX_NATIVE_GRANTS`] grants: the user must revoke one first.
const GRANT_LIMIT: &str = "grant_limit";

const ROUTE_REQUEST: &str = "/v1/app-grants/request";
const ROUTE_POLL: &str = "/v1/app-grants/request/{request_id}";
const ROUTE_APPROVE: &str = "/v1/app-grants/approve/{approve_id}";
const ROUTE_LIST: &str = "/permission/native-grants";
const ROUTE_REVOKE: &str = "/permission/native-grants/revoke";

/// Per-node state for native app grants: requests awaiting the user, and the
/// live connections opened with each grant (so revocation can close them).
/// Persistent grants live in `DelegateCapabilities`, reached through the node's
/// `OpManager`; a hosted node has none, so it never honours a grant.
#[derive(Clone)]
pub(crate) struct AppGrantState(Arc<Inner>);

struct Inner {
    op_manager: ExportOpManagerHandle,
    #[cfg(test)]
    caps_override: Mutex<Option<Arc<DelegateCapabilities>>>,
    pending: Mutex<HashMap<String, PendingRequest>>,
    live: Mutex<HashMap<GrantTokenHash, Vec<(u64, CancellationToken)>>>,
    next_live_id: AtomicU64,
    polls: AtomicUsize,
}

struct PendingRequest {
    approve_id: String,
    app: ContractInstanceId,
    client_name: String,
    created: tokio::time::Instant,
    /// Nonce of the most recently rendered approval page. Taken (cleared) by
    /// the POST that uses it.
    page_nonce: Option<String>,
    outcome: Outcome,
    /// Bumped when `outcome` changes, to wake pollers. Dropping the request
    /// (expiry, first use, denial collected) closes it, which also wakes them.
    changed: watch::Sender<()>,
}

enum Outcome {
    Pending,
    /// `stored` once the first collecting poll has written the grant.
    Approved {
        token: GrantToken,
        stored: bool,
    },
    Denied,
}

/// The result of looking up a token presented on a WebSocket upgrade.
pub(crate) enum GrantLookup {
    /// The token names a grant. Hold [`LiveGrant::guard`] for the life of the
    /// connection: it is how revocation reaches it.
    Granted(LiveGrant),
    /// No grant has this token (never issued, or revoked).
    NotFound,
    /// The grant store could not be consulted right now (node still starting,
    /// or a storage read failed). The token may well be valid; the client
    /// should retry rather than discard it.
    Unavailable,
}

pub(crate) struct LiveGrant {
    pub contract_id: ContractInstanceId,
    pub guard: LiveGrantGuard,
}

/// Registration of one live connection under a grant. Dropping it deregisters.
pub(crate) struct LiveGrantGuard {
    state: AppGrantState,
    hash: GrantTokenHash,
    id: u64,
    cancel: CancellationToken,
}

impl LiveGrantGuard {
    /// Cancelled when the grant is revoked.
    pub(crate) fn revoked(&self) -> CancellationToken {
        self.cancel.clone()
    }
}

impl Drop for LiveGrantGuard {
    fn drop(&mut self) {
        let mut live = self.state.0.live.lock();
        if let Some(conns) = live.get_mut(&self.hash) {
            conns.retain(|(id, _)| *id != self.id);
            if conns.is_empty() {
                live.remove(&self.hash);
            }
        }
    }
}

impl AppGrantState {
    pub(crate) fn new(op_manager: ExportOpManagerHandle) -> Self {
        Self(Arc::new(Inner {
            op_manager,
            #[cfg(test)]
            caps_override: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            next_live_id: AtomicU64::new(0),
            polls: AtomicUsize::new(0),
        }))
    }

    #[cfg(test)]
    pub(crate) fn with_capabilities(caps: Arc<DelegateCapabilities>) -> Self {
        let state = Self::new(ExportOpManagerHandle::default());
        *state.0.caps_override.lock() = Some(caps);
        state
    }

    fn capabilities(&self) -> Option<Arc<DelegateCapabilities>> {
        #[cfg(test)]
        if let Some(caps) = self.0.caps_override.lock().clone() {
            return Some(caps);
        }
        self.0
            .op_manager
            .current()
            .and_then(|om| om.delegate_capabilities())
    }

    /// Resolve a token presented on a WebSocket upgrade to the app it was
    /// granted for. Fails closed: anything but a stored grant yields no
    /// identity, and a store that cannot be consulted is reported as
    /// [`GrantLookup::Unavailable`] so the client can tell it from a
    /// revocation.
    ///
    /// The connection is registered BEFORE the store is read. Revocation
    /// deletes the row and THEN cancels registered connections, so either this
    /// read misses the row or the revocation sees the registration: a
    /// connection cannot slip between the two and outlive its grant.
    pub(crate) fn resolve_connection(&self, token: &AuthToken) -> GrantLookup {
        let Some(caps) = self.capabilities() else {
            return GrantLookup::Unavailable;
        };
        let hash = GrantTokenHash::of(token.as_str());
        let guard = self.register_live(hash);
        match caps.native_grant_for(hash) {
            Ok(Some(grant)) => match grant.app {
                AppIdentity::WebApp(contract_id) => {
                    // The client demonstrably holds the token: stop offering it
                    // to pollers of the request it came from.
                    self.forget_collected_request(hash);
                    GrantLookup::Granted(LiveGrant { contract_id, guard })
                }
            },
            Ok(None) => GrantLookup::NotFound,
            Err(e) => {
                tracing::warn!(error = %e, "Could not read native app grants; refusing the token for now");
                GrantLookup::Unavailable
            }
        }
    }

    fn forget_collected_request(&self, hash: GrantTokenHash) {
        self.0.pending.lock().retain(
            |_, p| !matches!(&p.outcome, Outcome::Approved { token, .. } if token.hash() == hash),
        );
    }

    fn register_live(&self, hash: GrantTokenHash) -> LiveGrantGuard {
        let id = self.0.next_live_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        self.0
            .live
            .lock()
            .entry(hash)
            .or_default()
            .push((id, cancel.clone()));
        LiveGrantGuard {
            state: self.clone(),
            hash,
            id,
            cancel,
        }
    }

    /// Delete a grant and close its live connections. `Ok(false)` if there was
    /// no such grant (live connections are closed regardless).
    fn revoke(&self, caps: &DelegateCapabilities, hash: GrantTokenHash) -> anyhow::Result<bool> {
        // Order matters, see `resolve_connection`: delete first, then cancel.
        let removed = caps.revoke_native_grant(hash)?;
        let conns = self.0.live.lock().remove(&hash).unwrap_or_default();
        for (_, cancel) in conns {
            cancel.cancel();
        }
        Ok(removed)
    }

    #[cfg(test)]
    fn live_connections(&self, hash: GrantTokenHash) -> usize {
        self.0.live.lock().get(&hash).map_or(0, Vec::len)
    }
}

/// Drop expired requests. Called under the lock on every access, so nothing
/// needs a background task.
fn sweep_expired(pending: &mut HashMap<String, PendingRequest>, now: tokio::time::Instant) {
    pending.retain(|_, p| now.duration_since(p.created) < REQUEST_TTL);
}

/// A random hex id for request ids, page ids and nonces.
///
/// `OsRng`, not `GlobalRng`: `request_id` collects a credential and the nonce
/// guards its approval, so neither may come from the simulation RNG, whose
/// seed tests fix (`.claude/rules/code-style.md`, the OsRng exception for key
/// material). Called once per request or page render, never on a hot path.
fn random_id() -> String {
    use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
    let mut bytes = [0u8; 32];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

pub(super) fn routes() -> Router {
    let refused = || axum::middleware::from_fn(refuse_when_hosted);
    Router::new()
        .route(ROUTE_REQUEST, post(create_request).layer(refused()))
        .route(ROUTE_POLL, get(poll_request).layer(refused()))
        .route(
            ROUTE_APPROVE,
            get(approve_page).post(approve_submit).layer(refused()),
        )
        .route(ROUTE_LIST, get(list_native_grants).layer(refused()))
        .route(ROUTE_REVOKE, post(revoke_native_grant).layer(refused()))
}

fn refuse(status: StatusCode, reason: &'static str) -> Response {
    (
        status,
        [("Cache-Control", "no-store")],
        Json(serde_json::json!({ "error": reason })),
    )
        .into_response()
}

fn grant_limit_response() -> Response {
    (
        StatusCode::CONFLICT,
        [("Cache-Control", "no-store")],
        Json(serde_json::json!({
            "status": GRANT_LIMIT,
            "error": GRANT_LIMIT,
            "message": format!(
                "This node already has {MAX_NATIVE_GRANTS} programs allowed. \
                 Revoke one on the Apps and permissions page, then try again."
            ),
        })),
    )
        .into_response()
}

/// Whether `caps` is already at [`MAX_NATIVE_GRANTS`]. A storage error reads
/// as "not at the limit": the authoritative check is the capped insert.
fn at_grant_limit(caps: &DelegateCapabilities) -> bool {
    caps.native_grant_count()
        .is_ok_and(|n| n >= MAX_NATIVE_GRANTS)
}

/// Any header a reverse proxy adds. A grant is handed out to, approved from,
/// and used from this machine only; behind a proxy every client looks like
/// loopback. A heuristic: see the module docs.
pub(crate) fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-forwarded-") || name == "forwarded" || name == "x-real-ip"
    })
}

/// `Origin` is exactly this node: same authority as `Host` (see
/// [`origin_is_exactly_same_origin`]) and the `http` scheme, which is what a
/// page served by the node over loopback carries. Forwarding headers are
/// refused before this is consulted, so no proxy can be in front of us.
fn origin_is_this_node(headers: &HeaderMap) -> bool {
    origin_is_exactly_same_origin(headers)
        && headers
            .get(axum::http::header::ORIGIN)
            .and_then(|o| o.to_str().ok())
            .is_some_and(|o| o.len() > 7 && o[..7].eq_ignore_ascii_case("http://"))
}

/// The request's `Host` re-assembled from its parsed parts (a loopback name
/// and an optional numeric port), or `None` if it is not exactly that. Used to
/// build `approve_url`, so nothing the caller put after the name is echoed.
fn loopback_authority(headers: &HeaderMap) -> Option<String> {
    let host = headers
        .get(axum::http::header::HOST)?
        .to_str()
        .ok()?
        .to_ascii_lowercase();
    let (name, port) = if let Some(rest) = host.strip_prefix('[') {
        let (inner, after) = rest.split_once(']')?;
        let port = if after.is_empty() {
            None
        } else {
            Some(after.strip_prefix(':')?.to_string())
        };
        (format!("[{inner}]"), port)
    } else {
        match host.split_once(':') {
            Some((name, port)) => (name.to_string(), Some(port.to_string())),
            None => (host.clone(), None),
        }
    };
    if !matches!(name.as_str(), "localhost" | "127.0.0.1" | "[::1]") {
        return None;
    }
    match port {
        None => Some(name),
        Some(p) => {
            let port: u16 = p.parse().ok()?;
            Some(format!("{name}:{port}"))
        }
    }
}

/// Gate shared by every route here: the direct peer and the `Host` it asked
/// for are both this machine, and nothing proxied the request. Returns the
/// refusal to send, or `None` to proceed.
fn local_gate(
    connect_info: Option<&Extension<ConnectInfo<SocketAddr>>>,
    headers: &HeaderMap,
) -> Option<Response> {
    if !peer_is_loopback(connect_info.map(|Extension(ci)| ci)) {
        return Some(refuse(StatusCode::FORBIDDEN, "loopback only"));
    }
    if has_forwarding_headers(headers) {
        return Some(refuse(
            StatusCode::FORBIDDEN,
            "proxied requests are refused",
        ));
    }
    if !host_is_loopback(headers) {
        return Some(refuse(StatusCode::FORBIDDEN, "loopback host only"));
    }
    None
}

/// Gate for the request and poll endpoints, which only a native program may
/// call. A browser always sends `Sec-Fetch-*` on these requests and `Origin`
/// on a cross-origin POST; a native client sends neither. Returns the refusal
/// to send, or `None` to proceed.
fn native_client_gate(
    connect_info: Option<&Extension<ConnectInfo<SocketAddr>>>,
    headers: &HeaderMap,
) -> Option<Response> {
    if let Some(refusal) = local_gate(connect_info, headers) {
        return Some(refusal);
    }
    if headers.contains_key(axum::http::header::ORIGIN) {
        return Some(refuse(
            StatusCode::FORBIDDEN,
            "this endpoint is for native clients; requests with an Origin are refused",
        ));
    }
    if headers.keys().any(|n| n.as_str().starts_with("sec-fetch-")) {
        return Some(refuse(
            StatusCode::FORBIDDEN,
            "this endpoint is for native clients; browser requests are refused",
        ));
    }
    None
}

/// The approval page is meant to be opened directly, as a top-level page,
/// from the program that asked. Refuse a load the browser reports as coming
/// from another site or as a subresource/frame (which would also rotate the
/// page's nonce). Browsers that send no `Sec-Fetch-*` headers are not refused.
fn page_fetch_is_direct(headers: &HeaderMap) -> bool {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let dest_ok = header("sec-fetch-dest").is_none_or(|d| d.eq_ignore_ascii_case("document"));
    let site_ok = header("sec-fetch-site")
        .is_none_or(|s| s.eq_ignore_ascii_case("none") || s.eq_ignore_ascii_case("same-origin"));
    dest_ok && site_ok
}

#[derive(Deserialize)]
struct CreateRequest {
    /// The web app's container contract instance id.
    app: String,
    /// Self-reported, display only, shown as unverified.
    #[serde(default)]
    client_name: Option<String>,
}

async fn create_request(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    Json(body): Json<CreateRequest>,
) -> Response {
    if let Some(r) = native_client_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    let Ok(app) = ContractInstanceId::from_base58(&body.app) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid app");
    };
    let Some(caps) = state.capabilities() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "node not ready");
    };
    // Refuse up front rather than after the user has clicked Allow.
    if at_grant_limit(&caps) {
        return grant_limit_response();
    }
    let Some(authority) = loopback_authority(&headers) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid host");
    };
    let client_name = sanitize_client_name(body.client_name.as_deref().unwrap_or(""));
    let request_id = random_id();
    let approve_id = random_id();
    {
        let mut pending = state.0.pending.lock();
        sweep_expired(&mut pending, tokio::time::Instant::now());
        if pending.len() >= MAX_PENDING_REQUESTS {
            return refuse(
                StatusCode::TOO_MANY_REQUESTS,
                "too many requests are waiting for approval",
            );
        }
        let (changed, _) = watch::channel(());
        pending.insert(
            request_id.clone(),
            PendingRequest {
                approve_id: approve_id.clone(),
                app,
                client_name,
                created: tokio::time::Instant::now(),
                page_nonce: None,
                outcome: Outcome::Pending,
                changed,
            },
        );
    }
    tracing::info!(contract_id = %app, "Native app grant requested; waiting for user approval");
    (
        [("Cache-Control", "no-store")],
        Json(serde_json::json!({
            "request_id": request_id,
            "approve_url": format!("http://{authority}/v1/app-grants/approve/{approve_id}"),
            "expires_in_secs": REQUEST_TTL.as_secs(),
        })),
    )
        .into_response()
}

/// Releases a poll slot on drop.
struct PollSlot(AppGrantState);

impl Drop for PollSlot {
    fn drop(&mut self) {
        self.0.0.polls.fetch_sub(1, Ordering::AcqRel);
    }
}

enum PollStep {
    Wait(watch::Receiver<()>),
    Approved(ContractInstanceId, GrantToken),
    GrantLimit,
    StoreFailed,
    Denied,
    Unknown,
}

/// Decide what a poll sees, under the pending lock. The first poll to see an
/// approval stores the grant; later polls of the same request get the same
/// token without storing it again (see the module docs on re-collection).
fn poll_step(
    state: &AppGrantState,
    pending: &mut HashMap<String, PendingRequest>,
    request_id: &str,
) -> PollStep {
    let Some(p) = pending.get_mut(request_id) else {
        return PollStep::Unknown;
    };
    match &mut p.outcome {
        Outcome::Pending => PollStep::Wait(p.changed.subscribe()),
        Outcome::Denied => {
            pending.remove(request_id);
            PollStep::Denied
        }
        Outcome::Approved { token, stored } => {
            if !*stored {
                // Stored under the pending lock, so two polls racing on the
                // same request store it once.
                let Some(caps) = state.capabilities() else {
                    return PollStep::StoreFailed;
                };
                match caps.create_native_grant(
                    token.hash(),
                    AppIdentity::WebApp(p.app),
                    &p.client_name,
                ) {
                    Ok(true) => {
                        *stored = true;
                        tracing::info!(contract_id = %p.app, "Native app grant approved and issued");
                    }
                    Ok(false) => return PollStep::GrantLimit,
                    Err(e) => {
                        tracing::warn!(error = %e, "Could not store a native app grant");
                        return PollStep::StoreFailed;
                    }
                }
            }
            PollStep::Approved(p.app, token.clone())
        }
    }
}

async fn poll_request(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    Path(request_id): Path<String>,
) -> Response {
    if let Some(r) = native_client_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    if state.0.polls.fetch_add(1, Ordering::AcqRel) >= MAX_CONCURRENT_POLLS {
        state.0.polls.fetch_sub(1, Ordering::AcqRel);
        return refuse(StatusCode::TOO_MANY_REQUESTS, "too many polls");
    }
    let _slot = PollSlot(state.clone());
    let deadline = tokio::time::Instant::now() + POLL_WAIT;
    loop {
        let step = {
            let mut pending = state.0.pending.lock();
            sweep_expired(&mut pending, tokio::time::Instant::now());
            poll_step(&state, &mut pending, &request_id)
        };
        match step {
            PollStep::Unknown => {
                return refuse(StatusCode::NOT_FOUND, "unknown or expired request");
            }
            PollStep::Denied => {
                return (
                    [("Cache-Control", "no-store")],
                    Json(serde_json::json!({ "status": "denied" })),
                )
                    .into_response();
            }
            // The request is kept: once the user revokes another grant, a
            // later poll (before the request expires) can still store this one.
            PollStep::GrantLimit => return grant_limit_response(),
            PollStep::StoreFailed => {
                return refuse(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "could not store the grant; poll again",
                );
            }
            PollStep::Approved(app, token) => {
                return (
                    [("Cache-Control", "no-store")],
                    Json(serde_json::json!({
                        "status": "approved",
                        "app": app.to_string(),
                        "token": token.expose(),
                    })),
                )
                    .into_response();
            }
            PollStep::Wait(mut changed) => {
                match tokio::time::timeout_at(deadline, changed.changed()).await {
                    // Changed, or the request went away: look again.
                    Ok(_) => continue,
                    Err(_) => {
                        return (
                            StatusCode::ACCEPTED,
                            [("Cache-Control", "no-store")],
                            Json(serde_json::json!({ "status": "pending" })),
                        )
                            .into_response();
                    }
                }
            }
        }
    }
}

fn page_headers(script_nonce: &str) -> [(&'static str, String); 5] {
    [
        ("X-Frame-Options", "DENY".to_string()),
        (
            "Content-Security-Policy",
            format!(
                "default-src 'none'; style-src 'unsafe-inline'; script-src 'nonce-{script_nonce}'; \
                 connect-src 'self'; form-action 'none'; base-uri 'none'; frame-ancestors 'none'"
            ),
        ),
        ("Cache-Control", "no-store".to_string()),
        ("Referrer-Policy", "no-referrer".to_string()),
        ("X-Content-Type-Options", "nosniff".to_string()),
    ]
}

fn message_page(status: StatusCode, text: &str) -> Response {
    let html = include_str!("app_grants/assets/message.html").replace("{{MESSAGE}}", text);
    (status, page_headers(&random_id()), Html(html)).into_response()
}

/// Fill the approval page. Every value except `client_line` is node-generated
/// and placeholder-free; `client_line` carries requester-chosen text, so it is
/// substituted LAST: a name containing `{{NONCE}}` must not be expanded.
fn render_approve_page(
    app: &ContractInstanceId,
    client_line: &str,
    approve_id: &str,
    nonce: &str,
    script_nonce: &str,
) -> String {
    include_str!("app_grants/assets/approve.html")
        .replace("{{APP}}", &app.to_string())
        .replace("{{APPROVE_ID}}", approve_id)
        .replace("{{NONCE}}", nonce)
        .replace("{{ALLOW_DELAY_MS}}", &ALLOW_DELAY_MS.to_string())
        .replace("{{SCRIPT_NONCE}}", script_nonce)
        .replace("{{CLIENT_LINE}}", client_line)
}

/// The approval page. Rendering mints a fresh nonce for the POST, replacing
/// any earlier one, so only the newest page can answer.
async fn approve_page(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    Path(approve_id): Path<String>,
) -> Response {
    if let Some(r) = local_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    if !page_fetch_is_direct(&headers) {
        return refuse(
            StatusCode::FORBIDDEN,
            "open this page directly from the program that asked",
        );
    }
    if state
        .capabilities()
        .is_some_and(|caps| at_grant_limit(&caps))
    {
        return message_page(
            StatusCode::CONFLICT,
            "Freenet already has the most programs allowed to use apps. Revoke one on the \
             Apps and permissions page, then start again from the program that asked.",
        );
    }
    let nonce = random_id();
    let (app, client_name) = {
        let mut pending = state.0.pending.lock();
        sweep_expired(&mut pending, tokio::time::Instant::now());
        let Some(p) = pending
            .values_mut()
            .find(|p| p.approve_id == approve_id && matches!(p.outcome, Outcome::Pending))
        else {
            drop(pending);
            return message_page(
                StatusCode::GONE,
                "This request has expired or was already answered. Start again from the program that asked.",
            );
        };
        p.page_nonce = Some(nonce.clone());
        (p.app, p.client_name.clone())
    };
    let client_line = if client_name.is_empty() {
        "The program did not name itself.".to_string()
    } else {
        // `<bdi>` isolates the name's text direction from the sentence around
        // it (format characters are also stripped from the name).
        format!(
            "The program calls itself <q><bdi>{}</bdi></q>. Freenet has not verified this name.",
            html_escape(&client_name)
        )
    };
    let script_nonce = random_id();
    let html = render_approve_page(&app, &client_line, &approve_id, &nonce, &script_nonce);
    (page_headers(&script_nonce), Html(html)).into_response()
}

#[derive(Deserialize)]
struct ApproveSubmit {
    nonce: String,
    allow: bool,
}

async fn approve_submit(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    Path(approve_id): Path<String>,
    Json(body): Json<ApproveSubmit>,
) -> Response {
    if let Some(r) = local_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    // Exactly the node's own origin. Not the any-localhost-port check: another
    // local web server's page must not be able to approve.
    if !origin_is_this_node(&headers) {
        return refuse(StatusCode::FORBIDDEN, "origin mismatch");
    }
    let caps = state.capabilities();
    let mut pending = state.0.pending.lock();
    sweep_expired(&mut pending, tokio::time::Instant::now());
    let Some(p) = pending.values_mut().find(|p| p.approve_id == approve_id) else {
        return refuse(StatusCode::GONE, "unknown or expired request");
    };
    if !matches!(p.outcome, Outcome::Pending) {
        return refuse(StatusCode::GONE, "already answered");
    }
    // Single use: the nonce is taken whether or not it matches.
    let Some(expected) = p.page_nonce.take() else {
        return refuse(StatusCode::FORBIDDEN, "invalid nonce");
    };
    let matches: bool =
        subtle::ConstantTimeEq::ct_eq(expected.as_bytes(), body.nonce.as_bytes()).into();
    if !matches {
        return refuse(StatusCode::FORBIDDEN, "invalid nonce");
    }
    // Tell the user now, not after they have been told "Allowed". The request
    // stays pending; the page has to be reopened (new nonce) to answer again.
    if body.allow && caps.as_deref().is_some_and(at_grant_limit) {
        return grant_limit_response();
    }
    p.outcome = if body.allow {
        Outcome::Approved {
            token: GrantToken::generate(),
            stored: false,
        }
    } else {
        Outcome::Denied
    };
    p.changed.send_replace(());
    tracing::info!(contract_id = %p.app, allowed = body.allow, "Native app grant request answered");
    (
        [("Cache-Control", "no-store")],
        Json(serde_json::json!({ "ok": true })),
    )
        .into_response()
}

/// Native app grants for the Apps and permissions page. Same gate as revoke
/// (loopback peer and `Host`, no forwarding headers), so the list is shown
/// exactly where its Revoke buttons work; a cross-origin caller gets an empty
/// list.
async fn list_native_grants(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
) -> Response {
    if local_gate(connect_info.as_ref(), &headers).is_some() {
        return refuse(
            StatusCode::FORBIDDEN,
            "programs allowed to use apps can be managed only from this computer, at http://127.0.0.1",
        );
    }
    let empty = serde_json::json!({ "grants": [] });
    if headers.contains_key(axum::http::header::ORIGIN) && !origin_is_this_node(&headers) {
        return Json(empty).into_response();
    }
    let Some(caps) = state.capabilities() else {
        return Json(empty).into_response();
    };
    match caps.native_grants() {
        Ok(grants) => {
            let rows: Vec<_> = grants
                .into_iter()
                .map(|(hash, g)| {
                    serde_json::json!({
                        "id": hash.to_hex(),
                        "app": g.app.display(),
                        "client_name": g.client_name,
                        "created_at_ms": g.created_at_ms,
                        "last_used_at_ms": g.last_used_at_ms,
                    })
                })
                .collect();
            Json(serde_json::json!({ "grants": rows })).into_response()
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to list native app grants");
            Json(serde_json::json!({ "grants": [], "error": "could not read the grant store" }))
                .into_response()
        }
    }
}

#[derive(Deserialize)]
struct RevokeNativeGrant {
    /// The grant id as `/permission/native-grants` lists it.
    id: String,
}

/// Revoke a native app grant: delete it and close its live connections.
/// State-changing, so the full local gate and an Origin exactly equal to the
/// node's.
async fn revoke_native_grant(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    Json(body): Json<RevokeNativeGrant>,
) -> Response {
    if let Some(r) = local_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    if !origin_is_this_node(&headers) {
        return refuse(StatusCode::FORBIDDEN, "origin mismatch");
    }
    let Some(hash) = GrantTokenHash::from_hex(&body.id) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid id");
    };
    let Some(caps) = state.capabilities() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "node not ready");
    };
    match state.revoke(&caps, hash) {
        Ok(true) => Json(serde_json::json!({ "ok": true })).into_response(),
        Ok(false) => refuse(StatusCode::NOT_FOUND, "no such grant"),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to revoke a native app grant");
            refuse(StatusCode::INTERNAL_SERVER_ERROR, "could not revoke")
        }
    }
}

#[cfg(test)]
mod tests;
