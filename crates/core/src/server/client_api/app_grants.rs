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
//! separate `approve_id`. Both are redacted from the request trace span
//! ([`make_request_span`]).
//!
//! # What a polling client sees
//!
//! - `202 {"status":"pending"}`: the user has not answered yet; poll again.
//! - `200 {"status":"approved","token",...}`: store the token and use it.
//! - `200 {"status":"denied"}`: stop.
//! - `409 {"status":"grant_limit"}`: the node already holds
//!   [`MAX_NATIVE_GRANTS`] grants. Keep polling: the request is kept, and the
//!   next poll waits (up to 30 s) for the user to revoke a grant before trying
//!   again.
//! - `503`: the grant could not be stored right now. Keep polling, with the
//!   same pacing.
//! - `404`: unknown, expired, revoked, or already used; start over.
//!
//! # Collecting the token more than once
//!
//! The grant is stored once, by the first poll that collects it. If that
//! response is lost (the client crashed, the connection dropped), the holder of
//! `request_id` may poll again and receives the SAME token, so an approval is
//! not wasted and no orphaned grant is left behind. A denial can be collected
//! again the same way. This stays possible until the request expires, the
//! token is first used to open a WebSocket connection, or the grant is
//! revoked, whichever is first; after that the token exists only in the client.
//!
//! If a client never polls again after a response it did not receive, the
//! grant stays stored, and listed on the Apps and permissions page, until the
//! user revokes it; nothing removes it automatically. An approval that no poll
//! ever collected stores nothing.
//!
//! A request expires [`REQUEST_TTL`] after it was made if it is not answered,
//! and [`REQUEST_TTL`] after the answer once it is, so a late answer still
//! leaves the client the full window to collect it. Expired requests are swept
//! on every access; nothing needs a timer.
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
//! - On the WebSocket, a grant token is honoured only from the `Authorization`
//!   header, only for a connection classified as local, only with a loopback
//!   `Host`, and not when the upgrade request carries forwarding headers.
//! - These routes sit behind the same router layers as the rest of the client
//!   API (the LAN source filter on a non-loopback bind, and the `Host` checks),
//!   and add their own loopback checks on top.
//!
//! The forwarding-header check is a heuristic. A reverse proxy that adds no
//! forwarding headers and rewrites `Host` to a loopback name passes it. That is
//! the same loopback trust boundary the shell-page tokens already rest on: the
//! node cannot tell such a proxy's clients from local ones.
//!
//! # Revocation
//!
//! Grants are listed on the Apps and permissions page with a Revoke button.
//! Revoking drops any request still offering the token (so it can no longer be
//! collected or stored again), deletes the stored row, and closes every live
//! connection that was opened with the token. It runs as its own task, so it
//! completes even if the HTTP request that asked for it goes away, and it
//! closes connections even if the delete fails.
//!
//! # WebSocket close codes for a token
//!
//! - 4401: the token names nothing (a stale shell-page token, or a grant that
//!   was revoked or never issued). Discard it.
//! - 4503: the grant store could not be consulted right now (node starting,
//!   storage fault, or a stored record this build cannot read). Keep the
//!   token and retry.
//! - 4403: a token sent in the `Authorization` header that this connection
//!   cannot use as a grant: not a local connection, a non-loopback `Host`,
//!   forwarding headers, a hosted node, or a router that serves no grants.
//!   Keep the token; it works only on a direct loopback connection to a node
//!   that serves grants.
//!
//! # Where the grant store lives
//!
//! A network node reaches it through its `OpManager` (the delegate
//! capabilities of its runtime pool). `freenet local` has no `OpManager`, so
//! `node::run_local_node` gives this state a store over the same database
//! ([`AppGrantState::set_local_capabilities`]). A hosted node has none.
//!
//! Grants persist across restarts in the redb store. A build without the
//! `redb` feature keeps them in memory, so they are lost on restart, as that
//! build's delegate capability grants already are.
//!
//! # Storage I/O
//!
//! Every read and write of the grant store runs off the async worker threads
//! ([`off_thread`]) and never while the `pending` lock is held.
//!
//! # What this does NOT protect against
//!
//! A malicious program already running as the same user can fetch the approval
//! page itself and click Allow. The approval step defends against web content
//! and gives the user a visible, revocable record; it is not a boundary
//! between local processes of the same user. Closing that needs the
//! authentication half of #5264 (for example passkey-gated approval), which
//! can be added to the approval step without changing the native client's side.
//!
//! # Time
//!
//! This module reads `tokio::time::Instant` directly rather than a
//! `TimeSource`, like the delegate permission prompts in
//! `contract::user_input`: it is the HTTP layer, which the simulation harness
//! does not drive, and tokio's clock is what its tests pause and advance. This
//! is listed as an exception in `.claude/rules/code-style.md`.

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
use tokio::time::Instant;
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

/// How long a request stays answerable, and how long an answer stays
/// collectable after it is given.
pub(crate) const REQUEST_TTL: Duration = Duration::from_secs(10 * 60);

/// How long one poll waits for the user (or, after a grant-limit or storage
/// refusal, for a change) before answering.
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

/// Path prefixes whose next segment is a capability (`request_id` collects a
/// token; `approve_id` names the approval page) and must not be logged.
const REDACTED_PATH_PREFIXES: [&str; 2] = ["/v1/app-grants/request/", "/v1/app-grants/approve/"];

/// Per-node state for native app grants: requests awaiting the user, and the
/// live connections opened with each grant (so revocation can close them).
/// Persistent grants live in `DelegateCapabilities`, reached through the node's
/// `OpManager`; a hosted node has none, so it never honours a grant.
#[derive(Clone)]
pub(crate) struct AppGrantState(Arc<Inner>);

/// `pending` and `live` are a `Mutex<HashMap>` rather than a `DashMap`: the
/// operations on them are multi-entry (sweeping every expired request, finding
/// a request by `approve_id`, dropping every request for one token hash, waking
/// every waiter) and must see one consistent map. Both are tiny (at most
/// [`MAX_PENDING_REQUESTS`] requests, one entry per live grant connection), and
/// no storage I/O or `.await` happens while either lock is held.
struct Inner {
    op_manager: ExportOpManagerHandle,
    /// The grant store when the node runs without an `OpManager`
    /// (`freenet local`, and tests): set once at startup by
    /// [`AppGrantState::set_local_capabilities`].
    local_caps: std::sync::OnceLock<Arc<DelegateCapabilities>>,
    pending: Mutex<HashMap<String, PendingRequest>>,
    live: Mutex<HashMap<GrantTokenHash, Vec<(u64, CancellationToken)>>>,
    next_live_id: AtomicU64,
    polls: AtomicUsize,
    /// Test hook: when set, the next `resolve_connection` waits on it between
    /// registering the connection and reading the store, so a test can act in
    /// that window deterministically.
    #[cfg(test)]
    read_gate: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

struct PendingRequest {
    approve_id: String,
    app: ContractInstanceId,
    client_name: String,
    /// Creation + [`REQUEST_TTL`]; moved to answer + [`REQUEST_TTL`] when the
    /// user answers.
    expires: Instant,
    /// Nonce of the most recently rendered approval page. Taken (cleared) by
    /// the POST that uses it.
    page_nonce: Option<String>,
    outcome: Outcome,
    /// Bumped when `outcome` changes (or a revocation frees a grant slot), to
    /// wake pollers. Dropping the request (expiry, first use, revocation)
    /// closes it, which also wakes them.
    changed: watch::Sender<()>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreFailure {
    Limit,
    Failed,
}

enum Outcome {
    Pending,
    Approved {
        token: GrantToken,
        /// Set once a poll has written the grant; later polls return the token
        /// without touching storage.
        stored: bool,
        /// A poll is writing the grant right now; others wait for it.
        storing: bool,
        /// Why the last write did not store it, if it did not.
        last_failure: Option<StoreFailure>,
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

/// Run blocking storage work off the async worker threads.
///
/// On a multi-thread runtime (the node) this is `spawn_blocking`. On a
/// current-thread runtime (unit tests, simulation) it runs inline, as the
/// executor's own offload does (`contract::executor::runtime`), because there
/// is no other worker to protect and the blocking pool may be outside the
/// harness's control. `None` if the blocking task panicked or was cancelled.
pub(crate) async fn off_thread<R: Send + 'static>(
    f: impl FnOnce() -> R + Send + 'static,
) -> Option<R> {
    use tokio::runtime::RuntimeFlavor;
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
            match tokio::task::spawn_blocking(f).await {
                Ok(r) => Some(r),
                Err(e) => {
                    tracing::warn!(error = %e, "native app grant storage task failed");
                    None
                }
            }
        }
        _ => Some(f()),
    }
}

impl AppGrantState {
    pub(crate) fn new(op_manager: ExportOpManagerHandle) -> Self {
        Self(Arc::new(Inner {
            op_manager,
            local_caps: std::sync::OnceLock::new(),
            pending: Mutex::new(HashMap::new()),
            live: Mutex::new(HashMap::new()),
            next_live_id: AtomicU64::new(0),
            polls: AtomicUsize::new(0),
            #[cfg(test)]
            read_gate: Mutex::new(None),
        }))
    }

    #[cfg(test)]
    pub(crate) fn with_capabilities(caps: Arc<DelegateCapabilities>) -> Self {
        let state = Self::new(ExportOpManagerHandle::default());
        state.set_local_capabilities(caps);
        state
    }

    /// Give a node that has no `OpManager` (local mode) its grant store. The
    /// network node's store is reached through the `OpManager` instead. A
    /// second call is ignored.
    pub(crate) fn set_local_capabilities(&self, caps: Arc<DelegateCapabilities>) {
        if self.0.local_caps.set(caps).is_err() {
            // Wired once at startup; a second call is a wiring mistake.
            tracing::warn!("native app grant store already set; ignoring repeat wiring");
        }
    }

    pub(crate) fn capabilities(&self) -> Option<Arc<DelegateCapabilities>> {
        self.0
            .op_manager
            .current()
            .and_then(|om| om.delegate_capabilities())
            .or_else(|| self.0.local_caps.get().cloned())
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
    pub(crate) async fn resolve_connection(&self, token: &AuthToken) -> GrantLookup {
        let Some(caps) = self.capabilities() else {
            return GrantLookup::Unavailable;
        };
        let hash = GrantTokenHash::of(token.as_str());
        let guard = self.register_live(hash);
        #[cfg(test)]
        {
            let gate = self.0.read_gate.lock().take();
            if let Some(gate) = gate {
                // A dropped sender also releases the read.
                drop(gate.await);
            }
        }
        // The read, and the hourly last-used refresh it may do, off the
        // async workers.
        match off_thread(move || caps.native_grant_for(hash)).await {
            Some(Ok(Some(grant))) => match grant.app {
                AppIdentity::WebApp(contract_id) => {
                    // The client demonstrably holds the token: stop offering it
                    // to pollers of the request it came from.
                    self.forget_requests_for(hash);
                    GrantLookup::Granted(LiveGrant { contract_id, guard })
                }
            },
            Some(Ok(None)) => GrantLookup::NotFound,
            Some(Err(e)) => {
                tracing::warn!(error = %e, "Could not read native app grants; refusing the token for now");
                GrantLookup::Unavailable
            }
            None => GrantLookup::Unavailable,
        }
    }

    /// Drop every request offering the token with this hash. Their pollers
    /// wake (the watch closes) and see 404.
    fn forget_requests_for(&self, hash: GrantTokenHash) {
        self.0.pending.lock().retain(
            |_, p| !matches!(&p.outcome, Outcome::Approved { token, .. } if token.hash() == hash),
        );
    }

    /// Wake every poller: a revocation may have freed a grant slot.
    fn wake_all_pollers(&self) {
        for p in self.0.pending.lock().values() {
            p.changed.send_replace(());
        }
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

    /// Delete a grant, drop any request still offering its token, and close
    /// its live connections. `Ok(false)` if there was no such grant (the rest
    /// happens regardless).
    ///
    /// Runs as its own task, so it completes even if the HTTP request that
    /// asked for it goes away, and the part that closes connections runs even
    /// if the delete fails or panics (see [`FinishRevoke`]).
    pub(crate) async fn revoke(
        &self,
        caps: Arc<DelegateCapabilities>,
        hash: GrantTokenHash,
    ) -> anyhow::Result<bool> {
        let state = self.clone();
        // Not registered with the task monitor: the task is short (one store
        // delete plus in-memory bookkeeping), awaited right here, and holds
        // only an `Arc` to this state. Detached only so a dropped HTTP request
        // cannot stop it half way; it does not outlive the node in any way
        // that matters for shutdown.
        let task = crate::config::GlobalExecutor::spawn(async move {
            // Dropped at the end of this block, or while unwinding a panic.
            let _finish = FinishRevoke {
                state: state.clone(),
                hash,
            };
            // First, so no poll in the gap below can collect or store the
            // token, and a write already in flight sees its request gone and
            // removes its own row.
            state.forget_requests_for(hash);
            off_thread(move || caps.revoke_native_grant(hash))
                .await
                .unwrap_or_else(|| Err(anyhow::anyhow!("revocation task failed")))
        });
        match task.await {
            Ok(removed) => removed,
            Err(e) => Err(anyhow::anyhow!("revocation task failed: {e}")),
        }
    }

    /// The part of a revocation that must happen whatever the delete did:
    /// drop the requests again, close live connections, wake pollers.
    /// Idempotent. Runs after the delete (see `resolve_connection` for why
    /// the order matters).
    fn finish_revoke(&self, hash: GrantTokenHash) {
        self.forget_requests_for(hash);
        let conns = self.0.live.lock().remove(&hash).unwrap_or_default();
        for (_, cancel) in conns {
            cancel.cancel();
        }
        self.wake_all_pollers();
    }

    /// Hold the next `resolve_connection` between registration and the store
    /// read until the returned sender is used or dropped.
    #[cfg(test)]
    pub(crate) fn hold_next_read(&self) -> tokio::sync::oneshot::Sender<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        *self.0.read_gate.lock() = Some(rx);
        tx
    }

    #[cfg(test)]
    pub(crate) fn live_connections(&self, hash: GrantTokenHash) -> usize {
        self.0.live.lock().get(&hash).map_or(0, Vec::len)
    }
}

/// Runs [`AppGrantState::finish_revoke`] when dropped, so a revocation closes
/// its connections even if the delete panicked.
struct FinishRevoke {
    state: AppGrantState,
    hash: GrantTokenHash,
}

impl Drop for FinishRevoke {
    fn drop(&mut self) {
        self.state.finish_revoke(self.hash);
    }
}

/// Drop expired requests. Called under the lock on every access, so nothing
/// needs a background task.
fn sweep_expired(pending: &mut HashMap<String, PendingRequest>, now: Instant) {
    pending.retain(|_, p| now < p.expires);
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

/// A request URI for logging: the PATH only, with the capability segment of
/// the grant routes replaced. Never the query: the client API carries
/// credentials there (`authToken`, `userToken` on the WebSocket URL).
pub(crate) fn redact_uri_for_log(uri: &axum::http::Uri) -> String {
    let path = uri.path();
    for prefix in REDACTED_PATH_PREFIXES {
        if path.starts_with(prefix) && path.len() > prefix.len() {
            return format!("{prefix}<redacted>");
        }
    }
    path.to_string()
}

/// The request span for the client API's `TraceLayer`: the default span's
/// fields, with the URI reduced to [`redact_uri_for_log`] (path only, grant
/// ids redacted, no query) for every route.
pub(crate) fn make_request_span<B>(req: &axum::http::Request<B>) -> tracing::Span {
    tracing::debug_span!(
        "request",
        method = %req.method(),
        uri = %redact_uri_for_log(req.uri()),
        version = ?req.version(),
    )
}

/// Largest request body any grant route accepts; every body is a small JSON
/// object.
const MAX_BODY_BYTES: usize = 4096;

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
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BODY_BYTES))
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

fn store_failed_response() -> Response {
    refuse(
        StatusCode::SERVICE_UNAVAILABLE,
        "could not store the grant; poll again",
    )
}

/// Whether the node is already at [`MAX_NATIVE_GRANTS`], read off the async
/// workers. A storage error reads as "not at the limit": the authoritative
/// check is the capped insert.
async fn at_grant_limit(caps: Option<Arc<DelegateCapabilities>>) -> bool {
    let Some(caps) = caps else {
        return false;
    };
    off_thread(move || caps.native_grant_count())
        .await
        .is_some_and(|n| n.is_ok_and(|n| n >= MAX_NATIVE_GRANTS))
}

/// Any header a reverse proxy adds. A grant is handed out to, approved from,
/// and used from this machine only; behind a proxy every client looks like
/// loopback. A heuristic: see the module docs.
pub(crate) fn has_forwarding_headers(headers: &HeaderMap) -> bool {
    headers.keys().any(|name| {
        let name = name.as_str();
        name.starts_with("x-forwarded-")
            || matches!(
                name,
                "forwarded"
                    | "x-real-ip"
                    | "via"
                    | "true-client-ip"
                    | "cf-connecting-ip"
                    | "x-client-ip"
            )
    })
}

/// Whether a WebSocket upgrade's headers allow honouring a grant token on it:
/// no forwarding headers and a loopback `Host`, the same header checks as the
/// grant's own HTTP routes. The caller separately requires a local connection
/// scope and a header (not query) token.
pub(crate) fn upgrade_may_use_grant(headers: &HeaderMap) -> bool {
    !has_forwarding_headers(headers) && host_is_loopback(headers)
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

/// Parse a JSON body AFTER the route's gate has run, so a refused caller's
/// body is never parsed. Requires `Content-Type: application/json`, as the
/// `Json` extractor did (for a browser that also forces a CORS preflight on a
/// cross-origin POST).
fn parse_json_body<T: serde::de::DeserializeOwned>(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<T, Box<Response>> {
    let is_json = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("application/json"))
        });
    if !is_json {
        return Err(Box::new(refuse(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected application/json",
        )));
    }
    serde_json::from_slice(body)
        .map_err(|_| Box::new(refuse(StatusCode::BAD_REQUEST, "invalid body")))
}

async fn create_request(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(r) = native_client_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    let body: CreateRequest = match parse_json_body(&headers, &body) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    let Ok(app) = ContractInstanceId::from_base58(&body.app) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid app");
    };
    let Some(caps) = state.capabilities() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "node not ready");
    };
    // Refuse up front rather than after the user has clicked Allow.
    if at_grant_limit(Some(caps)).await {
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
        let now = Instant::now();
        sweep_expired(&mut pending, now);
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
                expires: now + REQUEST_TTL,
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
    /// Wait for a change; answer "pending" at the deadline.
    Wait(watch::Receiver<()>),
    /// The last write was refused; wait for a change before writing again.
    WaitAfterFailure(watch::Receiver<()>),
    /// This poll writes the grant (the request is marked `storing`).
    Store {
        token: GrantToken,
        app: ContractInstanceId,
        client_name: String,
    },
    Approved(ContractInstanceId, GrantToken),
    Denied,
    Unknown,
}

/// Decide what a poll does, under the pending lock. Only decides: any storage
/// write is returned as [`PollStep::Store`] and done after the lock is dropped.
fn poll_step(
    pending: &mut HashMap<String, PendingRequest>,
    request_id: &str,
    waited_after_failure: bool,
) -> PollStep {
    let Some(p) = pending.get_mut(request_id) else {
        return PollStep::Unknown;
    };
    match &mut p.outcome {
        Outcome::Pending => PollStep::Wait(p.changed.subscribe()),
        // Kept until expiry, so a lost response can be collected again.
        Outcome::Denied => PollStep::Denied,
        Outcome::Approved { token, stored, .. } if *stored => {
            PollStep::Approved(p.app, token.clone())
        }
        Outcome::Approved { storing: true, .. } => PollStep::Wait(p.changed.subscribe()),
        Outcome::Approved {
            last_failure: Some(_),
            ..
        } if !waited_after_failure => PollStep::WaitAfterFailure(p.changed.subscribe()),
        Outcome::Approved {
            token,
            storing,
            last_failure,
            ..
        } => {
            *storing = true;
            *last_failure = None;
            PollStep::Store {
                token: token.clone(),
                app: p.app,
                client_name: p.client_name.clone(),
            }
        }
    }
}

/// Write one grant, on a blocking thread. A grant already present (an earlier
/// write that reported an error but committed) counts as stored. The limit is
/// read first, so a client polling at the limit costs a read, not a write.
fn store_grant(
    caps: &DelegateCapabilities,
    token_hash: GrantTokenHash,
    app: ContractInstanceId,
    client_name: &str,
) -> Result<(), StoreFailure> {
    let attempt = || -> anyhow::Result<bool> {
        if caps.has_native_grant(token_hash)? {
            return Ok(true);
        }
        if caps.native_grant_count()? >= MAX_NATIVE_GRANTS {
            return Ok(false);
        }
        caps.create_native_grant(token_hash, AppIdentity::WebApp(app), client_name)
    };
    match attempt() {
        Ok(true) => Ok(()),
        Ok(false) => Err(StoreFailure::Limit),
        Err(e) => {
            tracing::warn!(error = %e, "Could not store a native app grant");
            Err(StoreFailure::Failed)
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
    let deadline = Instant::now() + POLL_WAIT;
    let mut waited_after_failure = false;
    loop {
        let step = {
            let mut pending = state.0.pending.lock();
            sweep_expired(&mut pending, Instant::now());
            poll_step(&mut pending, &request_id, waited_after_failure)
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
            PollStep::Approved(app, token) => return approved_response(app, &token),
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
            // Pace a client polling at the limit (or after a storage error):
            // wait for a change, such as a revocation, or the deadline, then
            // write again once.
            PollStep::WaitAfterFailure(mut changed) => {
                drop(tokio::time::timeout_at(deadline, changed.changed()).await);
                waited_after_failure = true;
            }
            PollStep::Store {
                token,
                app,
                client_name,
            } => {
                return store_and_answer(&state, &request_id, token, app, client_name).await;
            }
        }
    }
}

fn approved_response(app: ContractInstanceId, token: &GrantToken) -> Response {
    (
        [("Cache-Control", "no-store")],
        Json(serde_json::json!({
            "status": "approved",
            "app": app.to_string(),
            "token": token.expose(),
        })),
    )
        .into_response()
}

/// Clears a request's `storing` mark if the store task ends without
/// recording its result (a panic), so the request is not left waiting on a
/// write that will never report. Records a storage failure, so the next poll
/// tries again.
struct StoringGuard {
    state: AppGrantState,
    request_id: String,
    armed: bool,
}

impl Drop for StoringGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut pending = self.state.0.pending.lock();
        if let Some(p) = pending.get_mut(&self.request_id) {
            if let Outcome::Approved {
                stored,
                storing,
                last_failure,
                ..
            } = &mut p.outcome
            {
                *storing = false;
                if !*stored {
                    *last_failure = Some(StoreFailure::Failed);
                }
            }
            p.changed.send_replace(());
        }
    }
}

/// Record a store's result on its request. `false` if the request is no
/// longer offered (revoked, expired, or used).
fn record_store_result(
    state: &AppGrantState,
    request_id: &str,
    result: Result<(), StoreFailure>,
) -> bool {
    let mut pending = state.0.pending.lock();
    sweep_expired(&mut pending, Instant::now());
    let Some(p) = pending.get_mut(request_id) else {
        return false;
    };
    if let Outcome::Approved {
        stored,
        storing,
        last_failure,
        ..
    } = &mut p.outcome
    {
        *storing = false;
        match result {
            Ok(()) => *stored = true,
            Err(f) => *last_failure = Some(f),
        }
    }
    p.changed.send_replace(());
    true
}

/// The write half of a collecting poll: store off the lock and off the async
/// workers, then record the result on the request. Runs as its own task (see
/// [`store_and_answer`]). `None` when the request was no longer offered
/// afterwards; then any row the write may have left is deleted.
async fn store_task(
    state: AppGrantState,
    request_id: String,
    token: GrantToken,
    app: ContractInstanceId,
    client_name: String,
) -> Option<Result<(), StoreFailure>> {
    let mut guard = StoringGuard {
        state: state.clone(),
        request_id: request_id.clone(),
        armed: true,
    };
    let hash = token.hash();
    let result = match state.capabilities() {
        Some(caps) => off_thread(move || store_grant(&caps, hash, app, &client_name))
            .await
            .unwrap_or(Err(StoreFailure::Failed)),
        None => Err(StoreFailure::Failed),
    };
    let still_offered = record_store_result(&state, &request_id, result);
    guard.armed = false;
    if still_offered {
        if result.is_ok() {
            tracing::info!(contract_id = %app, "Native app grant approved and issued");
        }
        return Some(result);
    }
    // Revoked, expired or used while we were writing. Nobody received the
    // token, so any row this write left is a grant no client holds. Delete it
    // whatever `result` says: a write can report an error and still commit.
    let deleted = match state.capabilities() {
        Some(caps) => off_thread(move || caps.revoke_native_grant(hash)).await,
        None => Some(Err(anyhow::anyhow!("grant store unavailable"))),
    };
    match deleted {
        Some(Ok(_)) => {}
        Some(Err(e)) => tracing::warn!(
            error = %e,
            "Could not remove a native app grant written for a request that is gone"
        ),
        None => {
            tracing::warn!("Removing a native app grant written for a request that is gone failed")
        }
    }
    None
}

/// Run [`store_task`] detached and answer from its result. Detached so that a
/// poll whose client disconnects mid-write does not leave the request marked
/// `storing` for good: the task finishes, and the next poll collects.
async fn store_and_answer(
    state: &AppGrantState,
    request_id: &str,
    token: GrantToken,
    app: ContractInstanceId,
    client_name: String,
) -> Response {
    // Not registered with the task monitor, for the same reasons as the
    // revocation task: short (one store write plus bookkeeping), awaited right
    // here, holding only an `Arc`; detached so a dropped poll cannot stop it.
    let task = crate::config::GlobalExecutor::spawn(store_task(
        state.clone(),
        request_id.to_string(),
        token.clone(),
        app,
        client_name,
    ));
    match task.await {
        Ok(Some(Ok(()))) => approved_response(app, &token),
        Ok(Some(Err(StoreFailure::Limit))) => grant_limit_response(),
        Ok(Some(Err(StoreFailure::Failed))) | Err(_) => store_failed_response(),
        Ok(None) => refuse(StatusCode::NOT_FOUND, "unknown or expired request"),
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
/// any earlier one, so only the newest page can answer. A `HEAD` does not
/// render, so it does not rotate the nonce. A browser prerendering the page
/// does render it, and so rotates the nonce; the page then shown is the newest
/// one only if it is the prerendered one.
async fn approve_page(
    method: axum::http::Method,
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
    if at_grant_limit(state.capabilities()).await {
        return message_page(
            StatusCode::CONFLICT,
            "Freenet already has the most programs allowed to use apps. Revoke one on the \
             Apps and permissions page, then start again from the program that asked.",
        );
    }
    let nonce = random_id();
    let (app, client_name) = {
        let mut pending = state.0.pending.lock();
        sweep_expired(&mut pending, Instant::now());
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
        // A HEAD answers with the status GET would give, without rotating
        // the nonce: only a rendered page may replace it.
        if method == axum::http::Method::HEAD {
            return (page_headers(&random_id()), ()).into_response();
        }
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
    body: axum::body::Bytes,
) -> Response {
    if let Some(r) = local_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    // Exactly the node's own origin. Not the any-localhost-port check: another
    // local web server's page must not be able to approve.
    if !origin_is_this_node(&headers) {
        return refuse(StatusCode::FORBIDDEN, "origin mismatch");
    }
    let body: ApproveSubmit = match parse_json_body(&headers, &body) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    // Read before the lock: no storage I/O happens under it.
    let at_limit = body.allow && at_grant_limit(state.capabilities()).await;
    let mut pending = state.0.pending.lock();
    let now = Instant::now();
    sweep_expired(&mut pending, now);
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
    if at_limit {
        return grant_limit_response();
    }
    p.outcome = if body.allow {
        Outcome::Approved {
            token: GrantToken::generate(),
            stored: false,
            storing: false,
            last_failure: None,
        }
    } else {
        Outcome::Denied
    };
    // The client gets the full window to collect the answer.
    p.expires = now + REQUEST_TTL;
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
    match off_thread(move || caps.native_grant_listing()).await {
        Some(Ok(grants)) => {
            let rows: Vec<_> = grants
                .into_iter()
                .map(|(hash, g)| match g {
                    Some(g) => serde_json::json!({
                        "id": hash.to_hex(),
                        "app": g.app.display(),
                        "client_name": g.client_name,
                        "created_at_ms": g.created_at_ms,
                        "last_used_at_ms": g.last_used_at_ms,
                    }),
                    // A row this build cannot read: listed, so it can be
                    // revoked by its id (the table key).
                    None => serde_json::json!({
                        "id": hash.to_hex(),
                        "unreadable": true,
                    }),
                })
                .collect();
            Json(serde_json::json!({ "grants": rows })).into_response()
        }
        failed => {
            if let Some(Err(e)) = failed {
                tracing::warn!(error = %e, "Failed to list native app grants");
            }
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

/// Revoke a native app grant: delete it, drop any request still offering its
/// token, and close its live connections. State-changing, so the full local
/// gate and an Origin exactly equal to the node's.
async fn revoke_native_grant(
    connect_info: Option<Extension<ConnectInfo<SocketAddr>>>,
    Extension(state): Extension<AppGrantState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Some(r) = local_gate(connect_info.as_ref(), &headers) {
        return r;
    }
    if !origin_is_this_node(&headers) {
        return refuse(StatusCode::FORBIDDEN, "origin mismatch");
    }
    let body: RevokeNativeGrant = match parse_json_body(&headers, &body) {
        Ok(b) => b,
        Err(r) => return *r,
    };
    let Some(hash) = GrantTokenHash::from_hex(&body.id) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid id");
    };
    let Some(caps) = state.capabilities() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "node not ready");
    };
    match state.revoke(caps, hash).await {
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
