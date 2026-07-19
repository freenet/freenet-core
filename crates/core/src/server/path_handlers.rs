//! Handle the `web` part of the bundles.
//!
//! Contract web apps are served inside sandboxed iframes to provide origin isolation.
//! The local API server returns a "shell" page that holds the auth token and
//! proxies WebSocket connections via postMessage, while the contract runs in an
//! `<iframe sandbox="allow-scripts allow-forms allow-popups allow-downloads allow-modals"
//!         allow="clipboard-read; clipboard-write">`
//! with an opaque origin that cannot access other contracts' data.
//! Popups inherit the sandbox (no `allow-popups-to-escape-sandbox`); external links
//! are opened via the `open_url` shell bridge message to avoid CORS issues. The
//! injected interceptor also overrides programmatic `window.open` in the iframe,
//! routing http(s) opens through the same `open_url` bridge so a new tab gets the
//! shell's real origin instead of a sandbox-inheriting opaque one (freenet-core#4645).
//! Sandbox content is protected from top-level access via Sec-Fetch-Dest checks in client_api.rs.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum::response::{Html, IntoResponse};
use dashmap::DashMap;
use freenet_stdlib::{
    client_api::{
        ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse, RequestError,
    },
    prelude::*,
};
use tokio::time::Instant;
use tokio::{fs::File, io::AsyncReadExt, sync::mpsc};

use crate::client_events::AuthToken;

use super::{
    ApiVersion, ClientConnection, HostCallbackResult,
    app_packaging::{WebApp, WebContractError},
    client_api::HttpClientApiRequest,
    errors::WebSocketApiError,
};
use tracing::{debug, instrument};

/// Per-contract lock serializing mutations of the webapp cache directory.
///
/// A typical first-time page load of a contract fans out several concurrent
/// subresource requests (`<script>`, `<link>`, `<img>`). Before this lock
/// existed, each one independently observed the cache as cold and raced
/// through `remove_dir_all` + `create_dir_all` + `unpack` against the same
/// target directory, corrupting the unpacked tree and sometimes leaving a
/// valid-looking hash file pointing at a partially-written archive.
///
/// Entries are retained for the lifetime of the process. Each lock is a
/// three-word `tokio::sync::Mutex`, so the memory overhead for a node that
/// has seen N distinct web contracts is trivially bounded.
static CONTRACT_CACHE_LOCKS: LazyLock<DashMap<ContractInstanceId, Arc<tokio::sync::Mutex<()>>>> =
    LazyLock::new(DashMap::new);

async fn acquire_cache_lock(instance_id: &ContractInstanceId) -> tokio::sync::OwnedMutexGuard<()> {
    let mutex = CONTRACT_CACHE_LOCKS
        .entry(*instance_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    mutex.lock_owned().await
}

/// How long a contract's extracted webapp cache is trusted before the next
/// request reconciles it against current network state.
///
/// `serve_sandbox_content` (the `?__sandbox=1` iframe handler) and
/// `variable_content` (subresource handler) both serve from the on-disk cache.
/// Without a freshness check, a republished contract keeps serving the old
/// bundle on these paths until the shell root (`/`) is hit again — only
/// `contract_home` unconditionally re-fetches. See #3977.
///
/// A short TTL re-runs `ensure_contract_cached` periodically. The actual
/// re-extraction still only happens when the state hash changed (see
/// `unpack_if_stale`), so the cost of a same-state refresh is one network GET,
/// not a disk rewrite. 30s keeps the publish-then-verify loop snappy while
/// bounding the GET rate to at most one per contract per window.
const CONTRACT_CACHE_REFRESH_TTL: Duration = Duration::from_secs(30);

/// Per-step timeout for the local presence query in `is_locally_known`.
/// Bounds how long a subresource request waits on the node for the
/// connection-id assignment and the diagnostics answer. On elapse the gate
/// fails closed (treats the contract as unknown), so a wedged or spammed node
/// can't pin request tasks open under a spray of unknown keys.
const PRESENCE_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// Last time each contract's cache was reconciled against the network via
/// `ensure_contract_cached`. Used to gate the TTL refresh so the sandbox and
/// subresource paths don't issue a network GET on every request.
///
/// Like `CONTRACT_CACHE_LOCKS`, entries are retained for the process lifetime;
/// each is a single `Instant`, so the footprint is bounded by the number of
/// distinct web contracts the node has served.
static CONTRACT_CACHE_REFRESH: LazyLock<DashMap<ContractInstanceId, Instant>> =
    LazyLock::new(DashMap::new);

/// Per-contract lock serializing the *decision* to issue a staleness-refresh
/// GET, so a fan-out of concurrent subresource requests after the TTL expiry
/// issues at most one `ensure_contract_cached` GET per contract per window.
///
/// This is deliberately distinct from `CONTRACT_CACHE_LOCKS`: that lock guards
/// the on-disk unpack and is re-taken inside `unpack_if_stale`. `tokio`'s mutex
/// is not reentrant, so the refresh gate — which is held *across* the GET (and
/// therefore across `unpack_if_stale`'s own lock acquisition) — must use its
/// own mutex to avoid a self-deadlock.
static CONTRACT_REFRESH_LOCKS: LazyLock<DashMap<ContractInstanceId, Arc<tokio::sync::Mutex<()>>>> =
    LazyLock::new(DashMap::new);

async fn acquire_refresh_lock(
    instance_id: &ContractInstanceId,
) -> tokio::sync::OwnedMutexGuard<()> {
    let mutex = CONTRACT_REFRESH_LOCKS
        .entry(*instance_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    mutex.lock_owned().await
}

/// True if the contract was reconciled against the network within the last
/// `CONTRACT_CACHE_REFRESH_TTL`. A missing timer reads as not-fresh.
fn cache_reconciled_recently(instance_id: &ContractInstanceId) -> bool {
    CONTRACT_CACHE_REFRESH
        .get(instance_id)
        .map(|last| last.elapsed() < CONTRACT_CACHE_REFRESH_TTL)
        .unwrap_or(false)
}

/// Whether the local node already has `instance_id` in its contract store /
/// hosting cache, or holds an active subscription to it.
///
/// # Why this gate exists (DoS amplification — #3945)
///
/// #3942 made `variable_content` issue a cold-cache network GET so a
/// subresource (`<img src>`) pointing at a contract resolves instead of
/// 404ing (#3940). That widened the attack surface: an unauthenticated
/// request to `/v1/contract/web/<KEY>/...` for a *random* 32-byte `KEY`
/// no longer 404s from the local cache check — it triggers a full network
/// GET (fan-out to remote peers) + unpack. Subresource URLs are
/// machine-fetchable, so an attacker can spray random keys and force the
/// node to issue outbound GETs it would never otherwise issue. Per-key rate
/// is bounded by the 30s fetch timeout but the parallel fan-out is not.
///
/// Gating the cold fetch on local-presence closes that vector while keeping
/// the real #3940 scenario working. The #3940 case is a cross-contract
/// `<img src="…/web/X/img.png">`: the user visits webapp Delta, whose page
/// embeds a subresource from a *different* contract X. The user has NOT
/// visited X's root, so X is NOT in the node's application-subscription set.
/// But the node will have **stored** X in its hosting cache the first time
/// any client (this one or another, on a shared gateway) fetched it — and
/// that store presence is exactly the bar #3945 option 2 names ("already
/// known to the local contract store, pinned/subscribed"). So the gate keys
/// off store/hosting presence, which covers the cross-contract subresource
/// case, while a random never-seen key — present in neither the store nor
/// the subscription set — gets the pre-#3942 404.
///
/// # Signal & mechanism
///
/// The HTTP layer has no direct handle on `op_manager`/`ring`; it only
/// reaches the node over the existing `ClientConnection` channel. So we
/// reuse the same transient-connection pattern as `ensure_contract_cached`
/// and ask the node the *local* `NodeQuery::NodeDiagnostics` query, scoped
/// to this one `instance_id`, with every flag off except `contract_keys`
/// (the store-presence answer) and `include_subscriptions`. This is a pure
/// ring/store lookup — `op_manager.ring.is_hosting_contract` /
/// `is_subscribed` / `hosting_contract_size` — with **no** network GET or
/// fan-out (see the `QueryNodeDiagnostics` handler in `p2p_protoc.rs`),
/// so the gate itself can never be the amplification vector it closes.
///
/// The contract is treated as known if either:
/// - it appears in `contract_states` (the node hosts/stores it, or holds an
///   active subscription lease — the `p2p_protoc.rs` handler only inserts an
///   entry when one of those is true), or
/// - it appears in `subscriptions` (the executor's application-subscription
///   set, populated when a client GETs it with `subscribe = true`).
///
/// On any error or timeout this returns `false` (fail closed): an attacker
/// must not be able to turn a transient node hiccup into an open fetch.
async fn is_locally_known(
    instance_id: ContractInstanceId,
    request_sender: &HttpClientApiRequest,
) -> bool {
    use freenet_stdlib::client_api::{NodeDiagnosticsConfig, NodeQuery, QueryResponse};

    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    if request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token: None,
        })
        .await
        .is_err()
    {
        return false;
    }
    // Fail closed if the node never assigns an id (e.g. it accepted the
    // connection but is wedged): bound the wait so a non-responsive node
    // can't pin the request task open under a spray of unknown keys.
    let client_id = match tokio::time::timeout(PRESENCE_QUERY_TIMEOUT, response_recv.recv()).await {
        Ok(Some(HostCallbackResult::NewId { id })) => id,
        _ => return false,
    };

    // Scope the diagnostics query to this one contract: only the store-presence
    // answer (`contract_keys`) and the application-subscription set
    // (`include_subscriptions`). Everything else is off so the node does the
    // minimum local work and returns no network/system data we don't read.
    let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
        instance_id,
        freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
    );
    let config = NodeDiagnosticsConfig {
        include_node_info: false,
        include_network_info: false,
        include_subscriptions: true,
        contract_keys: vec![key],
        include_system_metrics: false,
        include_detailed_peer_info: false,
        include_subscriber_peer_ids: false,
    };

    let mut known = false;
    if request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics {
                config,
            })),
            auth_token: None,
            origin_contract: None,
            // Internal node-query request: no delegate secrets, no user context.
            user_context: None,
            api_version: Default::default(),
        })
        .await
        .is_ok()
    {
        let recv_result = tokio::time::timeout(PRESENCE_QUERY_TIMEOUT, response_recv.recv()).await;
        if let Ok(Some(HostCallbackResult::Result {
            result: Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(info))),
            ..
        })) = recv_result
        {
            // `contract_states` keys are `ContractKey::Display`, which is the
            // base58 instance-id encoding (see stdlib `NodeDiagnosticsResponse`).
            let in_store = info.contract_states.contains_key(&instance_id.to_string());
            let subscribed = info
                .subscriptions
                .iter()
                .any(|sub| sub.contract_key == instance_id);
            known = in_store || subscribed;
        }
    }

    // Reap the transient client registration regardless of outcome.
    if let Err(err) = request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::Disconnect { cause: None }),
            auth_token: None,
            origin_contract: None,
            // Internal node-query request: no delegate secrets, no user context.
            user_context: None,
            api_version: Default::default(),
        })
        .await
    {
        tracing::warn!("is_locally_known: disconnect send failed: {err}");
    }

    known
}

/// Ensures the contract's webapp cache is populated and not stale before it is
/// served from disk.
///
/// Calls `ensure_contract_cached` when either:
/// - the cache is cold (no `{key}.hash` file on disk), or
/// - more than `CONTRACT_CACHE_REFRESH_TTL` has elapsed since the last
///   reconciliation for this contract.
///
/// For a **cold** cache the GET is additionally gated on the contract being
/// locally KNOWN (see `is_locally_known`): a cold cache for a contract the node
/// neither stores nor subscribes to is the random-key DoS amplification vector
/// #3942 opened, so this returns `Ok(())` without issuing the network GET and
/// the caller serves a 404 from the empty cache directory (the pre-#3942
/// behaviour). See #3945. A **warm-but-stale** refresh is NOT gated: a warm
/// on-disk cache already proves the node legitimately fetched this contract,
/// so refreshing it is not the amplification vector, and gating it would
/// silently regress the #3977 republish-pickup for a warm-but-unsubscribed
/// contract. The warm-and-fresh fast path never reaches either branch, so
/// steady-state requests pay nothing.
///
/// On a successful refresh the per-contract timer is reset. This is what makes
/// the `?__sandbox=1` and subresource paths pick up a republished bundle
/// without requiring a prior hit on the shell root. See #3977.
///
/// # Concurrency
///
/// A typical page load fans out several concurrent subresource requests. To
/// keep the "at most one network GET per contract per window" bound under that
/// fan-out, the refresh decision uses double-checked locking against the
/// dedicated per-contract `CONTRACT_REFRESH_LOCKS` mutex:
///
/// 1. A lock-free freshness check fast-paths the common warm-and-fresh case so
///    steady-state requests never contend on the lock.
/// 2. When a refresh looks due, the refresh lock is taken and the timer is
///    re-checked. The first holder fetches and updates the timer; every
///    follower that queued behind it observes the fresh timer and returns
///    without issuing its own GET. Without this gate, a burst of requests
///    arriving just after the TTL expiry would each fire a redundant GET.
///
/// The refresh lock is intentionally NOT `CONTRACT_CACHE_LOCKS`: the latter is
/// re-acquired inside `unpack_if_stale`, and `tokio`'s mutex is not reentrant,
/// so holding it across the GET would self-deadlock.
///
/// The refresh timer is only advanced on success, so a transient fetch failure
/// does not suppress the next request's retry. `ensure_contract_cached` skips
/// the disk rewrite when the state hash is unchanged (`unpack_if_stale`).
async fn refresh_cache_if_due(
    instance_id: ContractInstanceId,
    request_sender: &HttpClientApiRequest,
) -> Result<(), WebSocketApiError> {
    let hash_path = state_hash_path(&instance_id);
    let cache_warm = tokio::fs::try_exists(&hash_path).await.unwrap_or(false);

    // Fast path: a warm cache reconciled within the TTL needs no work and must
    // not contend on the refresh lock.
    if cache_warm && cache_reconciled_recently(&instance_id) {
        return Ok(());
    }

    // Slow path: refresh looks due. Serialize concurrent refreshers for this
    // contract so only the first issues a GET; the rest re-check below.
    let _guard = acquire_refresh_lock(&instance_id).await;
    // Re-check on the timer alone (not the pre-lock `cache_warm` snapshot): a
    // concurrent refresher that completed while we waited recorded a fresh
    // timer AND populated the cache via `ensure_contract_cached`, so a fresh
    // timer means there is nothing left to do even if our snapshot saw the
    // cache as cold.
    if cache_reconciled_recently(&instance_id) {
        return Ok(());
    }

    // DoS amplification gate (#3945) — COLD path only. A cold cache (no
    // `{key}.hash` on disk) for a contract the node has no local presence for
    // is exactly the random-key enumeration vector #3942 opened: skip the
    // network GET and let the caller serve a 404 from the empty cache
    // directory (the pre-#3942 behavior). A locally-KNOWN instance — the node
    // stores it (the #3940 cross-contract `<img src>` case, where X was stored
    // when the subresource was first loaded for some user) or subscribes to it
    // — falls through and fetches.
    //
    // The WARM-but-stale refresh is deliberately NOT gated: a warm on-disk
    // cache is itself proof the node legitimately fetched this contract
    // before, so a TTL-driven re-fetch of an already-cached bundle is not the
    // random-key amplification vector. Gating it would also silently break the
    // #3977 republish-pickup for a contract that is cached warm but currently
    // unsubscribed (it would serve the stale bundle instead of refreshing).
    // Note `cache_warm` is the PRE-LOCK snapshot, which is exactly right here:
    // a concurrent refresher that warmed the cache while we waited also
    // recorded a fresh timer, so the `cache_reconciled_recently` re-check above
    // already returned for that race — reaching this point with
    // `cache_warm == false` means the cache was genuinely cold for us.
    if !cache_warm && !is_locally_known(instance_id, request_sender).await {
        return Ok(());
    }

    ensure_contract_cached(instance_id, request_sender, None).await?;
    CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());
    Ok(())
}

#[instrument(level = "debug", skip(request_sender))]
pub(super) async fn contract_home(
    key: String,
    request_sender: HttpClientApiRequest,
    assigned_token: AuthToken,
    api_version: ApiVersion,
    query_string: Option<String>,
    sub_path: Option<&str>,
    hosted_mode: bool,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let instance_id = ContractInstanceId::from_bytes(&key).map_err(|err| {
        debug!("contract_home: Failed to parse contract key: {}", err);
        WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        }
    })?;

    // Register the assigned token with origin_contracts so subsequent
    // WebSocket connections from the shell iframe authenticate against
    // the correct contract identity, then fetch + unpack the contract.
    ensure_contract_cached(
        instance_id,
        &request_sender,
        Some((assigned_token.clone(), instance_id)),
    )
    .await?;
    // Record the reconciliation so the iframe load that immediately follows
    // (`?__sandbox=1`) and any subresource fetches reuse this fresh state
    // instead of issuing their own redundant GET within the TTL window.
    CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

    // Return the shell page instead of the contract HTML directly.
    // The shell page wraps the contract in a sandboxed iframe for
    // origin isolation (GHSA-824h-7x5x-wfmf).
    match shell_page(
        &assigned_token,
        &key,
        api_version,
        query_string,
        sub_path,
        hosted_mode,
    ) {
        Ok(b) => Ok(b.into_response()),
        Err(err) => {
            tracing::error!("Failed to generate shell page: {err}");
            Err(WebSocketApiError::NodeError {
                error_cause: format!("Failed to generate shell page: {err}"),
            })
        }
    }
}

/// Fetches the contract from the network (or local storage) and unpacks
/// it into the webapp cache directory if the state hash differs from what
/// is already cached. Returns once the cache is guaranteed to be populated
/// for `instance_id`.
///
/// The optional `assigned_token` is forwarded to `ClientConnection::NewConnection`
/// so the caller can bind a freshly generated auth token to the instance for
/// later WebSocket authentication. Subresource fetches (images, JS, CSS) pass
/// `None` — they only need the cache side-effect.
///
/// # Why subresource requests need this
///
/// `variable_content` used to serve directly from the cache. If a browser
/// requested `/v1/contract/web/<KEY>/image.jpg` before any load of the
/// contract root (e.g. cross-contract `<img src>` from a different webapp),
/// the cache directory did not exist and the request 404'd. See #3940.
async fn ensure_contract_cached(
    instance_id: ContractInstanceId,
    request_sender: &HttpClientApiRequest,
    assigned_token: Option<(AuthToken, ContractInstanceId)>,
) -> Result<(), WebSocketApiError> {
    let (response_sender, mut response_recv) = mpsc::unbounded_channel();
    request_sender
        .send(ClientConnection::NewConnection {
            callbacks: response_sender,
            assigned_token,
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let client_id = if let Some(HostCallbackResult::NewId { id }) = response_recv.recv().await {
        id
    } else {
        return Err(WebSocketApiError::NodeError {
            error_cause: "Couldn't register new client in the node".into(),
        });
    };
    request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(
                ContractRequest::Get {
                    key: instance_id,
                    return_contract_code: true,
                    subscribe: true,
                    blocking_subscribe: false,
                }
                .into(),
            ),
            auth_token: None,
            origin_contract: None,
            // Internal node-query request: no delegate secrets, no user context.
            user_context: None,
            api_version: Default::default(),
        })
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;

    let recv_result =
        tokio::time::timeout(std::time::Duration::from_secs(30), response_recv.recv()).await;
    let outcome = handle_get_response(instance_id, recv_result).await;

    // Disconnect regardless of whether the fetch succeeded, so the node
    // can reap the transient client registration. A send failure means the
    // node is gone, which is already the important signal — we don't fail
    // the user's request over it, but we log at warn! so an operator sees
    // the trail if WebSocket connections subsequently hang.
    if let Err(err) = request_sender
        .send(ClientConnection::Request {
            client_id,
            req: Box::new(ClientRequest::Disconnect { cause: None }),
            auth_token: None,
            origin_contract: None,
            // Internal node-query request: no delegate secrets, no user context.
            user_context: None,
            api_version: Default::default(),
        })
        .await
    {
        tracing::warn!("ensure_contract_cached: disconnect send failed: {err}");
    }

    outcome
}

/// Processes the GetResponse from the node, unpacking into the cache if needed.
async fn handle_get_response(
    instance_id: ContractInstanceId,
    recv_result: Result<Option<HostCallbackResult>, tokio::time::error::Elapsed>,
) -> Result<(), WebSocketApiError> {
    match recv_result {
        // Transient: the 30s fetch wrapper elapsed before the node answered.
        // Use RequestError(Timeout) (not the dual-use OperationError) so the
        // HTTP layer can serve the retry page without also catching terminal
        // node-returned OperationErrors (e.g. banned contracts) — see #3472
        // and the `is_transient` matcher in errors.rs.
        Err(_) => Err(WebSocketApiError::AxumError {
            error: ErrorKind::RequestError(RequestError::Timeout),
        }),
        // Transient: the response channel closed (node restarting / shutting
        // down). ChannelClosed is unambiguously transient, unlike OperationError.
        Ok(None) => Err(WebSocketApiError::AxumError {
            error: ErrorKind::ChannelClosed,
        }),
        Ok(Some(HostCallbackResult::Result {
            result:
                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    contract: Some(contract),
                    state,
                    ..
                })),
            ..
        })) => unpack_if_stale(&contract, state.as_ref()).await,
        Ok(Some(HostCallbackResult::Result {
            result:
                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    contract: None, ..
                })),
            ..
        })) => Err(WebSocketApiError::MissingContract { instance_id }),
        Ok(Some(HostCallbackResult::Result {
            result: Err(err), ..
        })) => {
            tracing::error!("error getting contract `{}`: {err}", instance_id.encode());
            Err(WebSocketApiError::AxumError {
                error: err.kind().clone(),
            })
        }
        Ok(other) => {
            tracing::error!("Unexpected node response: {other:?}");
            Err(WebSocketApiError::NodeError {
                error_cause: format!("Unexpected response from node: {other:?}"),
            })
        }
    }
}

/// Unpacks the contract's web archive into the cache directory if the stored
/// hash differs from the current state hash, or if there is no prior hash on
/// disk. The presence of the hash file is what `variable_content` uses as the
/// "cache is populated" signal — it is written last to make cache staleness
/// detection atomic.
///
/// Takes `CONTRACT_CACHE_LOCKS[instance_id]` for the duration of the mutation
/// so concurrent unpacks for the same contract serialize instead of racing
/// on `remove_dir_all` + `create_dir_all` + `unpack`. The hash is re-read
/// inside the lock — if a prior holder already wrote the current state, the
/// follower exits without repeating the work.
async fn unpack_if_stale(
    contract: &ContractContainer,
    state_bytes: &[u8],
) -> Result<(), WebSocketApiError> {
    let contract_key = contract.key();
    let instance_id = *contract_key.id();
    let path = contract_web_path(&instance_id);
    let current_hash = hash_state(state_bytes);
    let hash_path = state_hash_path(&instance_id);

    let _guard = acquire_cache_lock(&instance_id).await;

    // Re-read the hash under the lock. Concurrent `ensure_contract_cached`
    // callers for the same cold contract each arrive here with their own
    // GetResponse; the first to acquire the lock unpacks and writes the
    // hash, and any that queued behind it see the fresh hash here and
    // return without touching the filesystem again.
    let needs_update = match tokio::fs::read(&hash_path).await {
        Ok(stored_hash_bytes) if stored_hash_bytes.len() == 8 => {
            let stored_hash = u64::from_be_bytes(stored_hash_bytes.try_into().unwrap());
            stored_hash != current_hash
        }
        _ => true,
    };
    if !needs_update {
        return Ok(());
    }

    debug!("State changed or not cached, unpacking webapp");
    let state = State::from(state_bytes);

    fn err(err: WebContractError, contract: &ContractContainer) -> WebSocketApiError {
        let key = contract.key();
        tracing::error!("{err}");
        WebSocketApiError::InvalidParam {
            error_cause: format!("failed unpacking contract: {key}"),
        }
    }

    // Clear existing cache if any; may not exist yet
    let _cleanup = tokio::fs::remove_dir_all(&path).await;
    tokio::fs::create_dir_all(&path)
        .await
        .map_err(|e| WebSocketApiError::NodeError {
            error_cause: format!("Failed to create cache dir: {e}"),
        })?;

    let mut web = WebApp::try_from(state.as_ref()).map_err(|e| err(e, contract))?;
    web.unpack(&path).map_err(|e| err(e, contract))?;

    // Store new hash LAST, so a partial unpack does not leave a stale
    // hash file that would make future requests skip the fetch.
    tokio::fs::write(&hash_path, current_hash.to_be_bytes())
        .await
        .map_err(|e| WebSocketApiError::NodeError {
            error_cause: format!("Failed to write state hash: {e}"),
        })?;

    Ok(())
}

#[instrument(level = "debug", skip(request_sender))]
pub(super) async fn variable_content(
    key: String,
    req_path: String,
    api_version: ApiVersion,
    request_sender: HttpClientApiRequest,
) -> Result<impl IntoResponse, Box<WebSocketApiError>> {
    debug!(
        "variable_content: Processing request for key: {}, path: {}",
        key, req_path
    );
    // compose the correct absolute path
    let instance_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let base_path = contract_web_path(&instance_id);
    debug!("variable_content: Base path resolved to: {:?}", base_path);

    // Fetch + unpack the contract if its cache is cold OR stale. Without the
    // cold-cache fetch, any subresource request (e.g. an <img src> pointing at
    // this contract from a different webapp) would 404 because the cache is
    // only populated by the shell-root handler (`contract_home`). See #3940.
    // The TTL-gated staleness refresh additionally picks up a republished
    // bundle on this path without requiring a prior hit on the shell root.
    // See #3977.
    //
    // The cold-cache GET is gated on the contract being locally KNOWN (see
    // `refresh_cache_if_due` / `is_locally_known`): an unknown random key 404s
    // from the empty cache below instead of triggering an outbound network GET,
    // closing the DoS amplification #3942 opened. See #3945.
    refresh_cache_if_due(instance_id, &request_sender)
        .await
        .map_err(Box::new)?;

    // Parse the full request path URI to extract the relative path using the v1 helper.
    let req_uri =
        req_path
            .parse::<axum::http::Uri>()
            .map_err(|err| WebSocketApiError::InvalidParam {
                error_cause: format!("Failed to parse request path as URI: {err}"),
            })?;
    debug!("variable_content: Parsed request URI: {:?}", req_uri);

    let relative_path = get_file_path(req_uri)?;
    debug!(
        "variable_content: Extracted relative path: {}",
        relative_path
    );

    let file_path = base_path.join(relative_path);
    debug!("variable_content: Full file path to serve: {:?}", file_path);
    debug!(
        "variable_content: Checking if file exists: {}",
        file_path.exists()
    );

    // For JavaScript files, rewrite root-relative asset paths just like we do for HTML.
    // Dioxus embeds paths like "/./assets/app_bg.wasm" inside the JS bundle, which browsers
    // normalize to "/assets/..." (root-relative), bypassing the contract web prefix.
    if file_path.extension().is_some_and(|ext| ext == "js") {
        let content = tokio::fs::read_to_string(&file_path).await.map_err(|err| {
            WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            }
        })?;
        let prefix = format!("/{}/contract/web/{key}/", api_version.prefix());
        let rewritten = content
            .replace("\"/./", &format!("\"{prefix}"))
            .replace("'/./", &format!("'{prefix}"));
        return Ok((
            [(axum::http::header::CONTENT_TYPE, "application/javascript")],
            rewritten,
        )
            .into_response());
    }

    // serve the file
    let mut serve_file = tower_http::services::fs::ServeFile::new(&file_path);
    let fake_req = axum::http::Request::new(axum::body::Body::empty());
    serve_file
        .try_call(fake_req)
        .await
        .map_err(|err| {
            WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            }
            .into()
        })
        .map(|r| r.into_response())
}

/// Escapes characters that are dangerous inside an HTML attribute value.
fn html_escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(ch),
        }
    }
    out
}

/// Validates a deep-link sub-path before it is interpolated into the
/// shell iframe's `data-src` URL (#3841).
///
/// The sub-path comes from the request URL's path component (axum's
/// `{*path}` wildcard), so a query string or fragment is normally split
/// off before it reaches us. This guard rejects:
///
/// - Characters that would break out of the URL path component: `?`
///   starts a query, `#` starts a fragment, `\` is treated as `/` by
///   browsers, and whitespace/control chars (incl. CR/LF) could corrupt
///   the attribute or — once HTML-unescaped by the browser — the
///   surrounding markup.
/// - A leading `/`, so the result stays relative to the contract web
///   prefix rather than becoming an absolute path.
/// - `.` / `..` path segments. This is the SECURITY-CRITICAL check:
///   unlike `sandbox_content_body` (which canonicalizes the on-disk file
///   against the contract cache dir), the dot-segments here would never
///   reach that layer. The browser normalizes `..` in a URL *before*
///   issuing the iframe request, so a `data-src` of
///   `/v1/contract/web/KEY/../OTHER/?__sandbox=1` would be requested as
///   `/v1/contract/web/OTHER/?__sandbox=1` — pointing the iframe at a
///   *different contract* under the current shell's token/origin. We
///   must therefore reject traversal segments here rather than relying
///   on later file-path canonicalization (Codex review, #3841).
fn sanitize_shell_sub_path(sub_path: &str) -> Result<String, WebSocketApiError> {
    if sub_path.starts_with('/') {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "deep-link sub-path must be relative".to_string(),
        });
    }
    if sub_path
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || matches!(c, '?' | '#' | '\\'))
    {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "deep-link sub-path contains an illegal character".to_string(),
        });
    }
    // Reject `.`/`..` segments. Split on `/` rather than using
    // `std::path::Component` so that a trailing-slash directory form like
    // `a/../` and an empty middle segment are both classified from the
    // raw URL text (no OS-specific path semantics). A browser collapses
    // these dot-segments client-side before requesting the iframe URL, so
    // they would escape the contract prefix without ever reaching the
    // on-disk canonicalization in `sandbox_content_body`.
    if sub_path.split('/').any(|seg| seg == "." || seg == "..") {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "deep-link sub-path must not contain '.' or '..' segments".to_string(),
        });
    }
    Ok(sub_path.to_string())
}

/// Generates the shell page HTML that wraps the contract in a sandboxed iframe.
///
/// The shell page holds the auth token and proxies WebSocket connections via
/// postMessage, providing origin isolation between contracts.
fn shell_page(
    auth_token: &AuthToken,
    contract_key: &str,
    api_version: ApiVersion,
    query_string: Option<String>,
    sub_path: Option<&str>,
    hosted_mode: bool,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let version_prefix = api_version.prefix();
    // For a deep-link reload (#3841) the iframe must load the requested
    // sub-page, not the contract root, so the in-iframe webapp starts on
    // the right route. The sub-path is interpolated into the iframe's
    // `data-src`; `sanitize_shell_sub_path` rejects anything that could
    // break out of the URL's path component (`?`, `#`, control chars,
    // CRLF), and the whole `data-src` is HTML-escaped below as a second
    // layer of defence. Path traversal is additionally caught when the
    // iframe later requests `?__sandbox=1` (see `sandbox_content_body`).
    let sub_path = sub_path.map(sanitize_shell_sub_path).transpose()?;
    let base_path = match sub_path.as_deref() {
        Some(sp) => format!("/{version_prefix}/contract/web/{contract_key}/{sp}"),
        None => format!("/{version_prefix}/contract/web/{contract_key}/"),
    };

    // Build the iframe src URL: same path with __sandbox=1 plus any
    // original query params (e.g., ?invitation=...). `__sandbox` is the
    // server-interpreted routing flag and must come only from the line
    // we prepend here. `authToken` is the shell's credential — the
    // freshly-generated one is passed to `freenetBridge(authToken)`
    // below; a value forwarded from `query_string` would only arrive
    // via an attacker-controlled URL (pasted deep link or cross-contract
    // navigate-handler hop that preserved `resolved.search`), so strip
    // it to keep the iframe's `location.search` free of injected
    // credentials that a webapp reading `location.search` might pick up.
    let mut iframe_params = vec!["__sandbox=1".to_string()];
    if let Some(qs) = &query_string {
        for param in qs.split('&') {
            if param.is_empty() {
                continue;
            }
            // Strip any `__sandbox*` param (server-interpreted routing
            // flag) and the auth credential `authToken`. Both are
            // prefix-checked since a future refactor might add
            // variants like `__sandbox_debug` or `authToken2`.
            if param.starts_with("__sandbox") || param.starts_with("authToken") {
                continue;
            }
            iframe_params.push(param.to_string());
        }
    }
    let iframe_src_raw = format!("{}?{}", base_path, iframe_params.join("&"));
    // HTML-escape the iframe src to prevent XSS via crafted query parameters.
    // While browsers typically percent-encode special chars in URLs, we must not
    // rely on that for defense-in-depth.
    let iframe_src = html_escape_attr(&iframe_src_raw);

    // auth_token is base58 (alphanumeric only), safe for unescaped interpolation.
    let auth_token = auth_token.as_str();
    // Use an inline SVG data URI for the default favicon to avoid CORS errors
    // from cross-origin requests. Contracts can override this via the
    // __freenet_shell__ postMessage bridge (type: 'favicon').
    let favicon = format!(
        "data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 640 471'>\
         <path d='{}' fill='%23007FFF' fill-rule='evenodd'/></svg>",
        super::home_page::RABBIT_SVG_PATH,
    );
    // Per-user durable token plumbing (P2-frontend of #4381). In hosted mode
    // the shell page mints/loads a durable per-user bearer secret from
    // `localStorage` and hands it to the bridge so the proxied WebSocket
    // upgrade carries `?userToken=<token>`. The shell is same-origin with the
    // node (so it CAN use localStorage); the sandboxed iframe is a different
    // origin and cannot. One token in localStorage = ONE identity per visitor
    // across every contract app on this node — the intended design.
    //
    // When hosted mode is OFF the shell is BEHAVIOURALLY identical to the
    // pre-#4381 shell: no token snippet, and the same single-argument
    // `freenetBridge(...)` call at the same site. Note this is behavioural, not
    // literal byte-equality — the always-injected SHELL_BRIDGE_JS itself gained
    // a `userToken` argument and an inert, undefined-guarded `if (userToken)`
    // branch that never fires when the bridge is called with one argument. The
    // token is generated client-side from `crypto.getRandomValues` and is NEVER
    // derived from any request input, so there is no injection vector.
    let (user_token_script, bridge_call) = if hosted_mode {
        (
            format!("<script>\n{SHELL_USER_TOKEN_JS}\n</script>\n"),
            // Third arg `true` puts the bridge in hosted mode so it can fail
            // closed when it has no per-user token (http, or storage failure) —
            // see the hostedNoToken branch in SHELL_BRIDGE_JS.
            format!("freenetBridge(\"{auth_token}\", __freenet_user_token, true);"),
        )
    } else {
        // Non-hosted: the original 1-arg call. `hostedMode` is undefined, so the
        // fail-closed branch never triggers and behavior is unchanged.
        (String::new(), format!("freenetBridge(\"{auth_token}\");"))
    };

    // Hosted-mode "shell chrome": a thin, host-controlled bar rendered OUTSIDE
    // the sandboxed app iframe. It is the only place a per-user-data action
    // (export to your own peer) can live — the durable user token is held by
    // the shell and must never reach the sandbox — and the only place the
    // "this is a hosted proxy, not private" disclosure cannot be hidden or
    // spoofed by the contract app. Empty (and the layout unchanged) when hosted
    // mode is off. The export control is a placeholder until the node-side
    // export endpoint lands (P3 `secrets export` over HTTP, scoped to the
    // connection's user token).
    let (hosted_styles, hosted_bar) = if hosted_mode {
        (
            format!("\n<style>{HOSTED_BAR_STYLES}</style>"),
            format!("{HOSTED_BAR_HTML}\n<script>{HOSTED_BAR_JS}</script>"),
        )
    } else {
        (String::new(), String::new())
    };
    // NOTE: every placeholder must be passed as an explicit `name = name`
    // argument. `format!` cannot implicitly capture `{ident}` variables when the
    // format string is produced by a macro (`include_str!`) rather than written
    // as a string literal.
    let html = format!(
        include_str!("path_handlers/assets/shell.html"),
        favicon = favicon,
        hosted_styles = hosted_styles,
        hosted_bar = hosted_bar,
        iframe_src = iframe_src,
        SHELL_BRIDGE_JS = SHELL_BRIDGE_JS,
        user_token_script = user_token_script,
        bridge_call = bridge_call,
    );

    Ok(Html(html))
}

/// Serves the contract's actual HTML content for display inside the sandboxed iframe.
///
/// This is called when the iframe requests `?__sandbox=1`. It reads the cached
/// contract HTML, rewrites asset paths, and injects the WebSocket shim that
/// routes connections through the shell page's postMessage bridge.
///
/// The `sub_path` parameter allows serving pages other than `index.html` for
/// multi-page websites. When `None`, defaults to `index.html`.
#[instrument(level = "debug", skip(request_sender))]
pub(super) async fn serve_sandbox_content(
    key: String,
    api_version: ApiVersion,
    sub_path: Option<&str>,
    request_sender: HttpClientApiRequest,
) -> Result<impl IntoResponse, WebSocketApiError> {
    let page = sub_path.unwrap_or("index.html");
    debug!("serve_sandbox_content: serving iframe content for key: {key}, page: {page}");
    let instance_id =
        ContractInstanceId::from_bytes(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;

    // Reconcile the on-disk cache against current network state before serving.
    // Previously this path only checked `path.exists()` and served whatever was
    // already extracted, so a republished contract kept serving the old bundle
    // here until the shell root (`/`) was hit again. The TTL gate bounds the
    // network GET rate to at most one per contract per window. See #3977.
    refresh_cache_if_due(instance_id, &request_sender).await?;

    let path = contract_web_path(&instance_id);
    if !path.exists() {
        return Err(WebSocketApiError::NodeError {
            error_cause: format!("Contract not cached yet: {key}"),
        });
    }
    sandbox_content_body(&path, &key, api_version, page).await
}

/// Reads a contract HTML page, rewrites paths, and injects the WebSocket shim
/// and navigation interceptor.
async fn sandbox_content_body(
    path: &Path,
    contract_key: &str,
    api_version: ApiVersion,
    page: &str,
) -> Result<impl IntoResponse + use<>, WebSocketApiError> {
    // Sanitize the page path to prevent directory traversal and absolute paths.
    // Path::join with an absolute path replaces the base entirely on Unix,
    // so we must reject absolute paths, parent directory components, and root
    // directory components before joining.
    let normalized = Path::new(page);
    for component in normalized.components() {
        if matches!(
            component,
            std::path::Component::ParentDir | std::path::Component::RootDir
        ) {
            return Err(WebSocketApiError::InvalidParam {
                error_cause: "Path traversal not allowed".to_string(),
            });
        }
    }

    let mut web_path = path.join(page);
    // For directory-style paths, look for index.html inside the directory
    if web_path.is_dir() {
        web_path = web_path.join("index.html");
    }
    // Ensure the resolved path is still under the contract's cache directory
    let canonical_base = path
        .canonicalize()
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let canonical_file = web_path
        .canonicalize()
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("Page not found: {page} ({err})"),
        })?;
    if !canonical_file.starts_with(&canonical_base) {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        });
    }

    // Open the canonical path (not the user-supplied path) to prevent TOCTOU
    // attacks where a symlink could be swapped between canonicalize and open.
    let mut key_file =
        File::open(&canonical_file)
            .await
            .map_err(|err| WebSocketApiError::NodeError {
                error_cause: format!("{err}"),
            })?;
    let mut buf = vec![];
    key_file
        .read_to_end(&mut buf)
        .await
        .map_err(|err| WebSocketApiError::NodeError {
            error_cause: format!("{err}"),
        })?;
    let mut body = String::from_utf8(buf).map_err(|err| WebSocketApiError::NodeError {
        error_cause: format!("{err}"),
    })?;

    // Rewrite root-relative asset paths so they resolve under the contract's web prefix.
    // Dioxus generates paths like /./assets/app.js which browsers normalize to /assets/app.js
    // (root-relative). These bypass the /v1/contract/web/{key}/ prefix and 404.
    let version_prefix = api_version.prefix();
    let prefix = format!("/{version_prefix}/contract/web/{contract_key}/");
    body = body.replace("\"/./", &format!("\"{prefix}"));
    body = body.replace("'/./", &format!("'{prefix}"));

    // Inject the WebSocket shim and navigation interceptor before any other scripts.
    // The shim overrides window.WebSocket so that wasm-bindgen routes connections
    // through the shell page's bridge. The interceptor catches <a> clicks AND
    // overrides programmatic window.open, routing both through postMessage for
    // multi-page navigation without a sandbox-inheriting popup (#4645).
    let injected_scripts =
        format!("<script>{WEBSOCKET_SHIM_JS}</script><script>{NAVIGATION_INTERCEPTOR_JS}</script>");
    if let Some(pos) = body.find("</head>") {
        body.insert_str(pos, &injected_scripts);
    } else if let Some(pos) = body.find("<body") {
        body.insert_str(pos, &injected_scripts);
    } else {
        body = format!("{injected_scripts}{body}");
    }

    Ok(Html(body))
}

/// JavaScript that mints (or loads) the durable per-user token in hosted mode.
///
/// Injected into the shell page (P2-frontend of #4381) ONLY when the node runs
/// in hosted mode. The shell is same-origin with the node, so it can persist a
/// token in `localStorage`; the sandboxed iframe cannot. The token is a 32-byte
/// secret minted from `crypto.getRandomValues` (never from request input),
/// base58 (Bitcoin/bs58 alphabet) encoded, and reused across every visit and
/// every contract app on this node — one durable identity per visitor. The
/// bridge presents it on the proxied WebSocket upgrade as `?userToken=<token>`.
///
/// The server treats the token as an OPAQUE namespace key (it hashes the raw
/// string bytes — see [`crate::wasm_runtime::UserSecretContext::from_token`]),
/// so the encoding is a purely client-side, display-facing choice: older builds
/// stored a hex string and those tokens keep resolving to the same per-user
/// namespace, while new identities are base58 (shorter and less error-prone for
/// a user to copy or transcribe).
///
/// On a non-`https:` page the IIFE returns undefined BEFORE touching
/// `localStorage`, so the durable token is never loaded, minted, or transmitted
/// over a plaintext wire (client mirror of the backend REFUSE-PLAINTEXT-TOKEN
/// invariant — see `decide_user_token`).
///
/// `localStorage` access is wrapped in try/catch so that a browser with storage
/// disabled (private mode quirks, embedded webviews) degrades to an undefined
/// token rather than throwing before the bridge starts; an undefined token means
/// the bridge omits the `userToken` param and the backend treats the connection
/// as a local/anonymous one (see `decide_user_token`).
const SHELL_USER_TOKEN_JS: &str = include_str!("path_handlers/assets/shell_user_token.js");

/// Styles for the hosted-mode "shell chrome" bar (see `shell_page`). Rendered
/// only when hosted mode is on; the bar lives OUTSIDE the sandboxed app iframe.
const HOSTED_BAR_STYLES: &str = include_str!("path_handlers/assets/hosted_bar.css");

/// Markup for the hosted-mode bar: the always-visible "not private" disclosure
/// plus an Account popover with the access-key backup/restore, a "New ID"
/// control to start over with a fresh identity, and the export-to-your-own-peer
/// action. The access key is the per-user token, read from the shell-only
/// `__freenet_user_token` global — it never enters the sandboxed iframe.
const HOSTED_BAR_HTML: &str = include_str!("path_handlers/assets/hosted_bar.html");

/// Behavior for the hosted-mode bar (toggle popover, copy/restore the access
/// key, mint a fresh identity via "New ID", export data). Runs in the trusted
/// shell context.
const HOSTED_BAR_JS: &str = include_str!("path_handlers/assets/hosted_bar.js");

/// JavaScript for the shell page's postMessage bridge.
///
/// The bridge listens for WebSocket requests from the sandboxed iframe,
/// creates real WebSocket connections with the auth token injected, and
/// forwards messages in both directions. Only allows connections to the
/// local API server itself (same origin) to prevent the contract from using the
/// bridge as an open proxy to other localhost services.
///
/// `userToken` is the durable per-user bearer secret minted by
/// `SHELL_USER_TOKEN_JS` in hosted mode; it is `undefined` in non-hosted mode
/// (the bridge is then called with a single argument) and, when present, is
/// appended to the real WebSocket URL as `?userToken=<token>` so the node can
/// scope a per-user delegate-secret namespace (P2 of #4381).
const SHELL_BRIDGE_JS: &str = include_str!("path_handlers/assets/shell_bridge.js");

/// JavaScript WebSocket shim injected into the sandboxed iframe content.
///
/// Overrides `window.WebSocket` so that `web_sys::WebSocket::new()` (which
/// compiles to `new WebSocket(url)` via wasm-bindgen, resolving from global
/// scope at call time) is intercepted and routed through postMessage to the
/// shell page's bridge.
const WEBSOCKET_SHIM_JS: &str = include_str!("path_handlers/assets/websocket_shim.js");

/// JavaScript navigation interceptor injected into sandboxed iframe HTML pages.
///
/// Intercepts clicks on `<a>` elements and sends a postMessage to the shell
/// page, which either opens the URL in a new window (cross-origin) or updates
/// the iframe's `src` (same-origin). This enables multi-page website
/// navigation without weakening the sandbox (no `allow-top-navigation` nor
/// `allow-popups-to-escape-sandbox` needed).
///
/// Cross-origin links MUST be handled regardless of their `target` attribute,
/// because without `allow-popups-to-escape-sandbox` a `target="_blank"` click
/// would open a sandboxed popup with a null origin and the destination site
/// would see CORS failures. This was freenet/river#208: River webapps added
/// `target="_blank"` to every external link, the old interceptor skipped any
/// anchor with an explicit target, and the resulting sandboxed popups broke
/// logged-in pages like GitHub. The `open_url` bridge hands the URL to the
/// shell page, which opens it with a proper origin via `window.open`.
///
/// Same-origin links with an explicit non-`_self` target are left to the
/// browser so webapps that legitimately want multi-tab navigation within
/// their own contract still work.
///
/// The interceptor also overrides programmatic `window.open`: an app that opens
/// a new tab from its own JS bypasses the click/auxclick listeners, and on a
/// hosted node the resulting opaque-origin popup can't read the per-user access
/// key and dead-ends (freenet-core#4645). http(s) new-window opens are forwarded
/// through the same `open_url` bridge (real origin); `_self`/`_parent`/`_top`,
/// non-http(s) schemes, and loopback targets (which `open_url` refuses) fall back
/// to the native open. The returned WindowProxy is dropped (null), matching the
/// shell's `noopener` open.
const NAVIGATION_INTERCEPTOR_JS: &str =
    include_str!("path_handlers/assets/navigation_interceptor.js");

/// Extracts the relative file path from a contract web URI.
///
/// Strips the version and contract key prefix (e.g. `/v1/contract/web/{key}/`)
/// and returns the remaining path (e.g. `assets/app.js`).
fn get_file_path(uri: axum::http::Uri) -> Result<String, Box<WebSocketApiError>> {
    let path_str = uri.path();

    let remainder = if let Some(rem) = path_str.strip_prefix("/v1/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v1/contract/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/") {
        rem
    } else {
        return Err(Box::new(WebSocketApiError::InvalidParam {
            error_cause: format!(
                "URI path '{path_str}' does not start with /v1/contract/ or /v2/contract/"
            ),
        }));
    };

    // remainder contains "{key}/{path}" or just "{key}"
    let file_path = match remainder.split_once('/') {
        Some((_key, path)) => path.to_string(),
        None => "".to_string(),
    };

    Ok(file_path)
}

/// Returns the base directory for webapp cache.
/// Uses XDG cache directory (~/.cache/freenet on Linux) to avoid permission
/// conflicts when multiple users run freenet on the same machine.
fn webapp_cache_dir() -> PathBuf {
    directories::ProjectDirs::from("", "The Freenet Project Inc", "freenet")
        .map(|dirs| dirs.cache_dir().to_path_buf())
        .unwrap_or_else(|| std::env::temp_dir().join("freenet"))
        .join("webapp_cache")
}

fn contract_web_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(instance_id.encode())
}

fn hash_state(state: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(state);
    hasher.finish()
}

fn state_hash_path(instance_id: &ContractInstanceId) -> PathBuf {
    webapp_cache_dir().join(format!("{}.hash", instance_id.encode()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a pair (sender, receiver) suitable for capturing what
    /// `ensure_contract_cached` emits on the client-connection channel.
    fn request_channel() -> (
        HttpClientApiRequest,
        tokio::sync::mpsc::Receiver<ClientConnection>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::channel::<ClientConnection>(4);
        (HttpClientApiRequest::from_sender(tx), rx)
    }

    /// Clears any webapp cache state for `instance_id` on disk. `contract_web_path`
    /// and `state_hash_path` resolve to a shared process-global directory, so
    /// tests that exercise the cache must use unique keys AND scrub any stale
    /// filesystem residue from a prior run before asserting on behaviour.
    ///
    /// Also drops the in-memory `CONTRACT_CACHE_REFRESH` timer (process-global,
    /// like the on-disk cache) so a stale timer from a prior run doesn't flip a
    /// cold-cache assertion into a warm/fresh one.
    async fn clear_cache(instance_id: &ContractInstanceId) {
        tokio::fs::remove_file(state_hash_path(instance_id))
            .await
            .ok();
        tokio::fs::remove_dir_all(contract_web_path(instance_id))
            .await
            .ok();
        CONTRACT_CACHE_REFRESH.remove(instance_id);
        CONTRACT_REFRESH_LOCKS.remove(instance_id);
    }

    /// Regression test for #3940, updated for the #3945 store-presence gate.
    /// `variable_content` must trigger a network fetch when the contract's
    /// webapp cache is cold **and** the contract is locally present. This
    /// models the REAL #3940 cross-contract scenario: a Delta page `<img>`s a
    /// SEPARATE contract X that the node has fetched-and-STORED before (for
    /// some user) but that THIS user never visited at its root — so X is NOT in
    /// the application-subscription set, only in the contract store. The gate
    /// must still resolve it (store presence is the bar #3945 names), proving
    /// the fix does not re-break #3940 for stored-but-unsubscribed contracts.
    ///
    /// Prior to #3942 a cold-cache subpath request returned 404; #3942 made it
    /// fetch; #3945 narrows that fetch to locally-present instances — answered
    /// here via the `NodeDiagnostics` presence query as "node hosts/stores X".
    ///
    /// Verifies the handler emits the `NewConnection` + `Request(Get)` fetch
    /// pair on the client-connection channel for the present instance. The
    /// fetch is cancelled mid-flight (we don't deliver a response) so the test
    /// stays bounded. See `variable_content_skips_fetch_for_unknown_instance`
    /// for the security side of the gate.
    #[tokio::test]
    async fn variable_content_triggers_fetch_on_cache_miss() {
        // Unique 32-byte seed so the resulting contract key does not collide
        // with other tests, and any cache residue from prior runs is scrubbed
        // via `clear_cache`.
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x40;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                variable_content(
                    key.clone(),
                    format!("/v1/contract/web/{key}/image.jpg"),
                    ApiVersion::V1,
                    sender,
                )
                .await
                .map(|_| ())
            })
        };

        // Cold cache → the #3945 gate runs. `expect_fetch_pair_cold` answers
        // the presence query as "node hosts/stores X" (stored-but-unsubscribed,
        // the #3940 cross-contract case), then asserts the resulting
        // `NewConnection` + `Get` fetch pair for our contract key.
        expect_fetch_pair_cold(&mut rx, instance_id).await;

        handler.abort();
        // Clean up after the test — handler was aborted mid-fetch, so no
        // cache was written, but clear defensively to avoid accumulating
        // state in the shared XDG cache dir across runs.
        clear_cache(&instance_id).await;
    }

    /// Security regression for #3945. A cold-cache subresource request for an
    /// UNKNOWN contract (not in the store AND not subscribed) must NOT issue a
    /// network GET — that is the random-key DoS amplification vector #3942
    /// opened. The presence query returns empty `contract_states` and empty
    /// `subscriptions`, so the gate fails closed and the handler serves a 404
    /// from the empty cache directory (pre-#3942 behaviour), issuing no `Get`
    /// on the channel.
    ///
    /// Load-bearing: without the gate the handler would fall straight through
    /// to `ensure_contract_cached` and emit a `NewConnection` + `Get`, which
    /// this test's "no Get" assertion would catch.
    #[tokio::test]
    async fn variable_content_skips_fetch_for_unknown_instance() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x47;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                variable_content(
                    key.clone(),
                    format!("/v1/contract/web/{key}/image.jpg"),
                    ApiVersion::V1,
                    sender,
                )
                .await
                .map(|r| r.into_response())
            })
        };

        // The #3945 presence query runs (cold cache). Answer it as "the node
        // has NO local presence for this contract" — empty contract_states AND
        // empty subscriptions → not locally known.
        answer_presence_query(&mut rx, instance_id, |_query_id| empty_diagnostics()).await;

        // The handler must finish and return a 404 — NO further Get may appear.
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("handler must finish without issuing a network fetch")
            .expect("handler must not panic")
            .expect("unknown-instance request must still resolve to a response");
        assert_eq!(
            result.status(),
            axum::http::StatusCode::NOT_FOUND,
            "an unknown cold-cache subresource must 404, not fetch"
        );

        // `answer_presence_query` already drained the query's Disconnect, so
        // the channel must now be empty — any residual NewConnection/Get here
        // would mean the gate wrongly let a fetch through.
        let mut saw_fetch = false;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                ClientConnection::NewConnection { .. } => saw_fetch = true,
                ClientConnection::Request { req, .. } => {
                    if matches!(
                        req.as_ref(),
                        ClientRequest::ContractOp(ContractRequest::Get { .. })
                    ) {
                        saw_fetch = true;
                    }
                }
            }
        }
        assert!(
            !saw_fetch,
            "unknown-instance request must NOT issue a network fetch (#3945 DoS gate)"
        );

        clear_cache(&instance_id).await;
    }

    /// Fail-closed regression for #3945: when the presence query is NEVER
    /// answered (the node accepted the transient `NewConnection` but never
    /// replies to the `NodeDiagnostics` query), `is_locally_known` must time
    /// out and read as NOT known, so the cold-cache request 404s and issues NO
    /// network GET. This is the DoS guarantee under a wedged node — without the
    /// 5s recv timeout the request task would hang forever, which under a spray
    /// of unknown keys is itself a resource-exhaustion vector.
    ///
    /// Uses paused time so the 5s presence-query timeout elapses via
    /// `advance()` rather than wall-clock, keeping the test fast and
    /// deterministic.
    #[tokio::test(start_paused = true)]
    async fn variable_content_fails_closed_when_presence_query_unanswered() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x48;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                variable_content(
                    key.clone(),
                    format!("/v1/contract/web/{key}/image.jpg"),
                    ApiVersion::V1,
                    sender,
                )
                .await
                .map(|r| r.into_response())
            })
        };

        // Answer the presence query's NewConnection with an id, then go SILENT
        // — never reply to the NodeDiagnostics query. Hold `callbacks` alive so
        // the channel doesn't close (a closed channel would short-circuit the
        // recv with `None`; we want to exercise the TIMEOUT branch specifically).
        let new_conn = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("handler must send NewConnection for the presence query")
            .expect("channel must remain open");
        let _callbacks = match new_conn {
            ClientConnection::NewConnection { callbacks, .. } => {
                callbacks
                    .send(HostCallbackResult::NewId {
                        id: crate::client_events::ClientId::next(),
                    })
                    .expect("callback receiver live for query NewId");
                callbacks
            }
            other => panic!("presence query must open with NewConnection, got: {other:?}"),
        };

        // Drain the diagnostics query request itself (so the handler is now
        // blocked on its recv-with-timeout), then advance past the 5s bound.
        let _query = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("handler must send the NodeDiagnostics query")
            .expect("channel must remain open");
        // Advance past PRESENCE_QUERY_TIMEOUT so the query recv times out → fail closed.
        tokio::time::advance(PRESENCE_QUERY_TIMEOUT + Duration::from_secs(1)).await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("handler must finish once the presence query times out")
            .expect("handler must not panic")
            .expect("request must still resolve to a response");
        assert_eq!(
            result.status(),
            axum::http::StatusCode::NOT_FOUND,
            "an unanswered presence query must fail closed → 404, not fetch"
        );

        // The handler drains its query Disconnect on the way out; nothing after
        // it may be a fetch.
        let mut saw_fetch = false;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                ClientConnection::NewConnection { .. } => saw_fetch = true,
                ClientConnection::Request { req, .. } => {
                    if matches!(
                        req.as_ref(),
                        ClientRequest::ContractOp(ContractRequest::Get { .. })
                    ) {
                        saw_fetch = true;
                    }
                }
            }
        }
        assert!(
            !saw_fetch,
            "a timed-out presence query must NOT issue a network fetch (#3945 fail-closed)"
        );

        clear_cache(&instance_id).await;
    }

    /// Fail-closed regression for #3945: when the node accepts the transient
    /// `NewConnection` (so the SEND succeeds) but never replies with the
    /// `NewId` connection-id assignment, the FIRST `is_locally_known` recv
    /// timeout must fire and read as NOT known — so the cold-cache request 404s
    /// and issues NO network GET. This is the wedged-node case distinct from
    /// `variable_content_fails_closed_when_presence_query_unanswered` (which
    /// DELIVERS the `NewId` and then times out the SECOND, diagnostics-answer,
    /// recv) and from `variable_content_fails_closed_when_node_channel_closed`
    /// (where the `NewConnection` SEND itself fails). Here the gap is between a
    /// successful `NewConnection` send and a missing `NewId`: the first
    /// `tokio::time::timeout(PRESENCE_QUERY_TIMEOUT, recv())` whose `_ => return
    /// false` arm must hold the gate closed. If that arm returned true (fail
    /// open) the handler would proceed to fetch and this test would see a GET.
    ///
    /// Uses paused time so the 5s presence-query timeout elapses via
    /// `advance()` rather than wall-clock, keeping the test fast and
    /// deterministic.
    #[tokio::test(start_paused = true)]
    async fn variable_content_fails_closed_when_newid_never_arrives() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x4c;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                variable_content(
                    key.clone(),
                    format!("/v1/contract/web/{key}/image.jpg"),
                    ApiVersion::V1,
                    sender,
                )
                .await
                .map(|r| r.into_response())
            })
        };

        // Accept the presence query's NewConnection so the SEND succeeds, but
        // NEVER reply with NewId. Hold `callbacks` alive so the channel stays
        // open (a closed channel would short-circuit the recv with `None` and
        // exercise a different path); we want the TIMEOUT branch of the FIRST
        // recv specifically.
        let new_conn = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
            .await
            .expect("handler must send NewConnection for the presence query")
            .expect("channel must remain open");
        let _callbacks = match new_conn {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("presence query must open with NewConnection, got: {other:?}"),
        };

        // The handler is now blocked on its NewId recv-with-timeout. Advance
        // past PRESENCE_QUERY_TIMEOUT so that recv times out → fail closed.
        tokio::time::advance(PRESENCE_QUERY_TIMEOUT + Duration::from_secs(1)).await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("handler must finish once the NewId wait times out")
            .expect("handler must not panic")
            .expect("request must still resolve to a response");
        assert_eq!(
            result.status(),
            axum::http::StatusCode::NOT_FOUND,
            "a missing NewId must fail closed → 404, not fetch"
        );

        // Nothing emitted after the unanswered presence query may be a fetch.
        let mut saw_fetch = false;
        while let Ok(msg) = rx.try_recv() {
            match msg {
                ClientConnection::NewConnection { .. } => saw_fetch = true,
                ClientConnection::Request { req, .. } => {
                    if matches!(
                        req.as_ref(),
                        ClientRequest::ContractOp(ContractRequest::Get { .. })
                    ) {
                        saw_fetch = true;
                    }
                }
            }
        }
        assert!(
            !saw_fetch,
            "a missing NewId must NOT issue a network fetch (#3945 fail-closed)"
        );

        clear_cache(&instance_id).await;
    }

    /// Fail-closed regression for #3945: if the node is gone entirely (the
    /// `ClientConnection` receiver is dropped, so even the presence query's
    /// `NewConnection` send fails), the cold-cache request must 404 and issue
    /// no GET. Covers the `request_sender.send(...).is_err()` branch of
    /// `is_locally_known`.
    #[tokio::test]
    async fn variable_content_fails_closed_when_node_channel_closed() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x49;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        // Drop the receiver immediately so every send on the sender fails.
        let (sender, rx) = request_channel();
        drop(rx);

        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/image.jpg"),
            ApiVersion::V1,
            sender,
        )
        .await
        .map(|r| r.into_response());

        // is_locally_known fails closed → gate skips the fetch → 404 from the
        // empty cache directory. (A dead channel must never surface as a fetch.)
        let response = result.expect("closed-channel cold request must still resolve");
        assert_eq!(
            response.status(),
            axum::http::StatusCode::NOT_FOUND,
            "a closed node channel must fail closed → 404"
        );

        clear_cache(&instance_id).await;
    }

    /// #3945 broaden-signal coverage: a cold cache for a contract that is
    /// SUBSCRIBED but NOT in the store (e.g. the lease outlived LRU eviction)
    /// must still fetch. Proves `is_locally_known`'s OR branch — known =
    /// in-store OR subscribed — not store-presence alone.
    #[tokio::test]
    async fn variable_content_triggers_fetch_for_subscribed_not_stored() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x4a;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                variable_content(
                    key.clone(),
                    format!("/v1/contract/web/{key}/image.jpg"),
                    ApiVersion::V1,
                    sender,
                )
                .await
                .map(|_| ())
            })
        };

        // Presence query: empty contract_states (NOT stored) but the instance
        // IS in subscriptions → known via the subscription branch.
        answer_presence_query(&mut rx, instance_id, |query_id| {
            let mut diag = empty_diagnostics();
            diag.subscriptions
                .push(freenet_stdlib::client_api::SubscriptionInfo {
                    contract_key: instance_id,
                    client_id: query_id.into(),
                });
            diag
        })
        .await;

        // The gate must let the fetch through.
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// #3977-interaction regression for the #3945 cold/warm gate split: a
    /// WARM-but-stale cache for an UNSUBSCRIBED, UNHOSTED contract must still
    /// refresh. The gate is cold-path only, so a warm-but-stale refresh issues
    /// its GET WITHOUT a preceding presence query — even though the contract is
    /// not currently "known". A warm on-disk cache already proves the node
    /// legitimately fetched this contract before, so refreshing it to pick up a
    /// republish (#3977) is not the random-key amplification vector. Without
    /// this split the handler would gate the warm refresh on a presence query
    /// that says "unknown" and serve a stale bundle forever.
    #[tokio::test]
    async fn warm_but_stale_refreshes_without_presence_gate() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x4b;
        let instance_id = ContractInstanceId::new(bytes);
        clear_cache(&instance_id).await;

        // Warm but unreconciled cache (hash present, no refresh timer ⇒ due).
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();

        let (sender, mut rx) = request_channel();
        let handler =
            tokio::spawn(
                async move { refresh_cache_if_due(instance_id, &sender).await.map(|_| ()) },
            );

        // The FIRST message must be the fetch's NewConnection — NOT a presence
        // query. `expect_fetch_pair` (the warm variant) asserts exactly that:
        // it would mis-parse a NodeDiagnostics query as the fetch NewConnection
        // and the subsequent Get assertion would fail.
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// Companion to `variable_content_triggers_fetch_on_cache_miss`: when the
    /// hash file is present AND the contract was reconciled within the refresh
    /// TTL, the handler must NOT issue a fetch. This pins the cache-hit fast
    /// path and prevents a regression where every subpath request re-fetches.
    #[tokio::test]
    async fn variable_content_skips_fetch_when_cache_present_and_fresh() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x41;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        // Prime the cache marker and a served file.
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("image.jpg"), b"fake-jpeg-bytes")
            .await
            .unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        // Mark the contract as just-reconciled so it falls inside the TTL window.
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        let (sender, mut rx) = request_channel();
        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/image.jpg"),
            ApiVersion::V1,
            sender,
        )
        .await;

        let response = result.expect("warm-cache request must succeed");
        let body = response_body(response).await;
        assert_eq!(
            body, "fake-jpeg-bytes",
            "warm-cache path must serve the primed file byte-for-byte"
        );
        assert!(
            rx.try_recv().is_err(),
            "fresh-cache path must not send any NewConnection/Get on the channel"
        );

        // Clean up last so a failed assertion above doesn't leave residue
        // that flips the next run's cold-cache check into warm-cache state.
        clear_cache(&instance_id).await;
    }

    /// Receives the `is_locally_known` (#3945) handshake and asserts it is the
    /// scoped `NodeQueries(NodeDiagnostics)` presence query for `instance_id`.
    ///
    /// Replies to the opening `NewConnection` with a fresh client id, asserts
    /// the diagnostics query is scoped to exactly `instance_id` (no broad
    /// enumeration), sends `reply`, then drains the trailing `Disconnect`. The
    /// reply must use the `query_id` from the request, so it is built by the
    /// caller via the passed closure.
    ///
    /// Leaves the channel positioned at the handler's next message (the real
    /// fetch's `NewConnection`, if the gate let it through).
    async fn answer_presence_query(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
        build_reply: impl FnOnce(
            crate::client_events::ClientId,
        ) -> freenet_stdlib::client_api::NodeDiagnosticsResponse,
    ) {
        use freenet_stdlib::client_api::{NodeQuery, QueryResponse};

        let new_conn = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("handler must send NewConnection for the local-known query")
            .expect("channel must remain open");
        let callbacks = match new_conn {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("local-known query must open with NewConnection, got: {other:?}"),
        };
        callbacks
            .send(HostCallbackResult::NewId {
                id: crate::client_events::ClientId::next(),
            })
            .expect("callback receiver live for query NewId");

        let query = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("handler must send the presence query")
            .expect("channel must remain open");
        let ClientConnection::Request { req, client_id, .. } = query else {
            panic!("expected the NodeDiagnostics request, got: {query:?}");
        };
        if let ClientRequest::NodeQueries(NodeQuery::NodeDiagnostics { config }) = req.as_ref() {
            // The presence query must be scoped to exactly the one contract — a
            // broad/empty `contract_keys` would make the node enumerate ALL
            // hosted contracts on every subresource request.
            assert_eq!(
                config.contract_keys.len(),
                1,
                "presence query must request exactly one contract key"
            );
            assert_eq!(
                *config.contract_keys[0].id(),
                instance_id,
                "presence query must be scoped to the requested instance"
            );
            assert!(
                !config.include_node_info
                    && !config.include_network_info
                    && !config.include_system_metrics
                    && !config.include_detailed_peer_info,
                "presence query must keep the heavy diagnostics flags off"
            );
        } else {
            panic!("local-known query must be NodeQueries(NodeDiagnostics), got: {req:?}");
        }
        let query_id = client_id;
        // The reply rides the SAME `callbacks` sender the handler reads.
        callbacks
            .send(HostCallbackResult::Result {
                id: query_id,
                result: Ok(HostResponse::QueryResponse(QueryResponse::NodeDiagnostics(
                    build_reply(query_id),
                ))),
            })
            .expect("callback receiver live for NodeDiagnostics reply");
        // Drain the trailing Disconnect the query helper sends on its way out.
        let _ = rx.recv().await;
    }

    /// A `NodeDiagnosticsResponse` with every optional field empty. Tests fill
    /// in `contract_states` / `subscriptions` to model presence.
    fn empty_diagnostics() -> freenet_stdlib::client_api::NodeDiagnosticsResponse {
        freenet_stdlib::client_api::NodeDiagnosticsResponse {
            node_info: None,
            network_info: None,
            subscriptions: Vec::new(),
            contract_states: std::collections::HashMap::new(),
            system_metrics: None,
            connected_peers_detailed: Vec::new(),
        }
    }

    /// Answers the #3945 presence query as "the node HOSTS/STORES `instance_id`"
    /// — the realistic #3940 cross-contract case: a Delta page `<img>`s a
    /// separate contract X that the node fetched-and-stored when the subresource
    /// was first loaded for some user, but that THIS user never visited at its
    /// root (so X is not in the application-subscription set). The gate must
    /// still let the fetch through on store presence alone.
    async fn answer_presence_query_hosted(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
    ) {
        answer_presence_query(rx, instance_id, |_query_id| {
            let mut diag = empty_diagnostics();
            // contract_states keyed by ContractKey::Display == instance-id base58.
            diag.contract_states.insert(
                instance_id.to_string(),
                freenet_stdlib::client_api::ContractState {
                    subscribers: 0,
                    subscriber_peer_ids: Vec::new(),
                    size_bytes: 1234,
                },
            );
            diag
        })
        .await;
    }

    /// Drives `serve_sandbox_content` (or `variable_content`) to the point
    /// where it has emitted its `NewConnection` + `Get` pair on the channel,
    /// asserting the contract key on the `Get`, then aborts the in-flight
    /// fetch. Returns once both messages have been observed.
    ///
    /// This is the **warm-but-stale** path: the #3945 presence gate runs ONLY
    /// on a cold cache, so a warm-cache refresh emits the fetch pair directly
    /// with no preceding presence query. Cold-cache tests use
    /// `expect_fetch_pair_cold`, which answers the presence query first.
    ///
    /// Replies to the `NewConnection` callback with a synthetic client id so
    /// the handler progresses past its blocking `NewId` recv to the `Get`.
    async fn expect_fetch_pair(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
    ) {
        let new_conn = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("handler must send NewConnection when a refresh is due")
            .expect("channel must remain open for the duration of the send");
        let callbacks = match new_conn {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("first message must be NewConnection, got: {other:?}"),
        };
        callbacks
            .send(HostCallbackResult::NewId {
                id: crate::client_events::ClientId::next(),
            })
            .expect("callback receiver must be live while handler awaits NewId");

        let get_req = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("handler must follow up with a Get request")
            .expect("channel must remain open");
        match get_req {
            ClientConnection::Request { req, .. } => {
                assert!(
                    matches!(
                        req.as_ref(),
                        ClientRequest::ContractOp(ContractRequest::Get { key: k, .. })
                            if *k == instance_id
                    ),
                    "second message must be Get({instance_id}), got: {req:?}"
                );
            }
            other => panic!("expected ClientConnection::Request, got: {other:?}"),
        }
    }

    /// Cold-cache variant of `expect_fetch_pair`: answers the #3945 presence
    /// query as "node hosts/stores `instance_id`" (the #3940 cross-contract
    /// case) first, then asserts the resulting fetch pair. Use this whenever the
    /// cache is COLD (no `{key}.hash` on disk), where the DoS gate runs.
    async fn expect_fetch_pair_cold(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
    ) {
        answer_presence_query_hosted(rx, instance_id).await;
        expect_fetch_pair(rx, instance_id).await;
    }

    /// Regression test for #3977. `serve_sandbox_content` (the `?__sandbox=1`
    /// iframe handler) must reconcile the on-disk cache against current network
    /// state, NOT serve blindly from disk.
    ///
    /// Before the fix, this handler only checked `path.exists()` and served the
    /// already-extracted bundle, so a republished contract kept serving the old
    /// bundle on the iframe path until the shell root (`/`) was hit again.
    ///
    /// Here the cache is warm (hash file + index.html on disk) but has never
    /// been reconciled (`CONTRACT_CACHE_REFRESH` has no entry), so a refresh is
    /// due and the handler must emit the `NewConnection` + `Get` fetch pair.
    /// The pre-fix code sent nothing on the channel.
    #[tokio::test]
    async fn serve_sandbox_content_triggers_refresh_when_stale() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x44;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        // Warm but unreconciled cache: hash file present, but no refresh timer.
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("index.html"), b"<html>old bundle</html>")
            .await
            .unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();

        let (sender, mut rx) = request_channel();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                serve_sandbox_content(key.clone(), ApiVersion::V1, None, sender)
                    .await
                    .map(|_| ())
            })
        };

        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// Companion to the above: once `serve_sandbox_content` has reconciled a
    /// contract within the TTL window, a subsequent request must serve from
    /// disk WITHOUT issuing another fetch. Pins the TTL fast path so the iframe
    /// load doesn't do a network round-trip on every request.
    #[tokio::test]
    async fn serve_sandbox_content_skips_refresh_when_fresh() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x45;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("index.html"), b"<html>fresh bundle</html>")
            .await
            .unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        // Reconciled just now: inside the TTL window.
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        let (sender, mut rx) = request_channel();
        let result = serve_sandbox_content(key.clone(), ApiVersion::V1, None, sender).await;

        let response = result.expect("fresh-cache sandbox request must succeed");
        let body = response_body(response).await;
        assert!(
            body.contains("fresh bundle"),
            "fresh-cache path must serve the primed index.html, got: {body}"
        );
        assert!(
            rx.try_recv().is_err(),
            "fresh-cache sandbox path must not send any NewConnection/Get on the channel"
        );

        clear_cache(&instance_id).await;
    }

    /// `refresh_cache_if_due` must treat a refresh timer older than
    /// `CONTRACT_CACHE_REFRESH_TTL` as stale and re-fetch, even when the
    /// on-disk cache is warm. This is the path that picks up a mid-session
    /// republish (#3977 impact 3) once the TTL window elapses.
    ///
    /// Uses paused time so the TTL boundary is crossed deterministically by
    /// `advance()` rather than wall-clock subtraction — `Instant::now()` on a
    /// freshly-booted host can be too close to the monotonic origin for a
    /// `checked_sub(TTL)` to succeed, which would make the test flaky.
    #[tokio::test(start_paused = true)]
    async fn refresh_cache_if_due_refetches_after_ttl_expires() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x46;
        let instance_id = ContractInstanceId::new(bytes);
        clear_cache(&instance_id).await;

        // Warm cache, reconciled "now" (paused clock base).
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        // Advance past the TTL so the timer reads as stale.
        tokio::time::advance(CONTRACT_CACHE_REFRESH_TTL + Duration::from_secs(1)).await;

        let (sender, mut rx) = request_channel();
        let handler =
            tokio::spawn(
                async move { refresh_cache_if_due(instance_id, &sender).await.map(|_| ()) },
            );

        // A stale timer must trigger a fetch despite the warm on-disk cache.
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// Services one transient client connection's worth of
    /// `ensure_contract_cached` traffic: replies to `NewConnection` with a
    /// fresh client id, then answers the `Get` with a successful `GetResponse`
    /// whose state hashes to the value already on disk. Because the on-disk
    /// `{key}.hash` matches, `unpack_if_stale` returns early (no `WebApp`
    /// unpack needed), so the refresh succeeds and records the timer.
    ///
    /// Both replies go on the `callbacks` sender from `NewConnection` — that is
    /// the `response_recv` end `ensure_contract_cached` reads from.
    async fn serve_one_get(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        contract: &ContractContainer,
        state: &WrappedState,
    ) {
        let msg = rx.recv().await.expect("leader must issue NewConnection");
        let callbacks = match msg {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("expected NewConnection, got: {other:?}"),
        };
        callbacks
            .send(HostCallbackResult::NewId {
                id: crate::client_events::ClientId::next(),
            })
            .expect("callback receiver live");
        let get = rx.recv().await.expect("Get must follow NewConnection");
        match get {
            ClientConnection::Request { req, .. } => assert!(
                matches!(
                    req.as_ref(),
                    ClientRequest::ContractOp(ContractRequest::Get { .. })
                ),
                "expected Get, got: {req:?}"
            ),
            other => panic!("expected Get request, got: {other:?}"),
        }
        callbacks
            .send(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(
                    ContractResponse::GetResponse {
                        key: contract.key(),
                        contract: Some(contract.clone()),
                        state: state.clone(),
                    },
                )),
            })
            .expect("callback receiver live for GetResponse");
        // Drain the trailing Disconnect the handler sends on the way out.
        let _ = rx.recv().await;
    }

    /// Concurrency regression for the Codex review finding on #3977: a fan-out
    /// of simultaneous requests on a warm-but-stale cache must issue exactly
    /// ONE network GET per contract per window, not one per request.
    ///
    /// Runs the real `refresh_cache_if_due` end-to-end. The leader's GET is
    /// answered with a `GetResponse` whose state hash matches the on-disk
    /// `{key}.hash`, so `unpack_if_stale` returns early, the leader records the
    /// refresh timer, and every follower that queued behind the refresh lock
    /// re-checks, sees the fresh timer, and skips its own GET. The receiver
    /// services exactly one `Get`, then asserts the channel closes with no
    /// second `NewConnection`.
    #[tokio::test]
    async fn refresh_cache_if_due_coalesces_concurrent_refreshes() {
        // Derive the instance id FROM a real contract so the GetResponse key
        // matches and `unpack_if_stale` takes its matching-hash early return.
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![1, 2, 3, 4])),
            Parameters::from(vec![5, 6]),
        )));
        let instance_id = *contract.key().id();
        let state = WrappedState::new(vec![9, 9, 9]);
        clear_cache(&instance_id).await;

        // Warm cache whose stored hash matches the state we'll return, so the
        // refresh succeeds without an actual unpack. No fresh timer ⇒ due.
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let matching_hash = hash_state(state.as_ref());
        tokio::fs::write(state_hash_path(&instance_id), matching_hash.to_be_bytes())
            .await
            .unwrap();

        // Shared channel so a single receiver observes every caller's traffic.
        let (sender, mut rx) = request_channel();
        let mut handlers = Vec::new();
        for _ in 0..8 {
            let sender = sender.clone();
            handlers.push(tokio::spawn(async move {
                refresh_cache_if_due(instance_id, &sender).await.map(|_| ())
            }));
        }
        drop(sender); // channel closes once all 8 handlers finish.

        // Warm cache → the #3945 presence gate does NOT run (it is cold-path
        // only). The leader fetches directly; followers coalesce on the refresh
        // lock and re-check the fresh timer, so only the leader issues a GET.
        // Service exactly one GET (the leader's). Every follower coalesces.
        serve_one_get(&mut rx, &contract, &state).await;

        // After the single served GET, no further NewConnection may appear:
        // a second one would mean a follower issued a redundant GET.
        let mut extra = 0;
        while let Some(msg) = rx.recv().await {
            if matches!(msg, ClientConnection::NewConnection { .. }) {
                extra += 1;
            }
        }
        assert_eq!(
            extra, 0,
            "concurrent refreshers must coalesce to a single GET; saw {extra} extra"
        );

        for h in handlers {
            h.await
                .expect("handler must not panic")
                .expect("refresh must succeed");
        }
        clear_cache(&instance_id).await;
    }

    /// Regression for the failure-path invariant: when `ensure_contract_cached`
    /// returns an error, `refresh_cache_if_due` must NOT record a fresh timer,
    /// so the next request retries instead of being suppressed for the TTL.
    ///
    /// Drives a real refresh whose GET is answered with a `contract: None`
    /// `GetResponse` (which `handle_get_response` maps to `MissingContract`),
    /// then asserts the call returned `Err` AND no timer was inserted. This
    /// pins the "timer advances only on success" property the
    /// `CONTRACT_CACHE_REFRESH.insert` placement after the `?` relies on —
    /// hoisting the insert before the GET would silently break retries.
    #[tokio::test]
    async fn refresh_cache_if_due_does_not_record_timer_on_fetch_failure() {
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![7, 7, 7, 7])),
            Parameters::from(vec![8, 8]),
        )));
        let instance_id = *contract.key().id();
        clear_cache(&instance_id).await;

        // Warm but unreconciled cache so a refresh is due (and no timer yet).
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();

        let (sender, mut rx) = request_channel();
        let handler = tokio::spawn(async move { refresh_cache_if_due(instance_id, &sender).await });

        // Warm cache → the #3945 presence gate does NOT run; the failure-path
        // GET below is reached directly.
        // Service the GET with a contract: None GetResponse → MissingContract.
        let msg = rx.recv().await.expect("must issue NewConnection");
        let callbacks = match msg {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("expected NewConnection, got: {other:?}"),
        };
        callbacks
            .send(HostCallbackResult::NewId {
                id: crate::client_events::ClientId::next(),
            })
            .expect("callback receiver live");
        let _get = rx.recv().await.expect("Get must follow NewConnection");
        callbacks
            .send(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(
                    ContractResponse::GetResponse {
                        key: contract.key(),
                        contract: None,
                        state: WrappedState::new(Vec::new()),
                    },
                )),
            })
            .expect("callback receiver live for GetResponse");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handler)
            .await
            .expect("handler must finish promptly")
            .expect("handler must not panic");
        assert!(
            result.is_err(),
            "a None-contract GetResponse must surface as an error, got: {result:?}"
        );
        assert!(
            !CONTRACT_CACHE_REFRESH.contains_key(&instance_id),
            "a failed refresh must NOT record a timer, or the next request would \
             be suppressed for the whole TTL instead of retrying"
        );

        clear_cache(&instance_id).await;
    }

    /// Direct unit test for `handle_get_response`'s `MissingContract`
    /// branch. Refactoring `handle_get_response` introduced this seam as a
    /// pure-logic boundary; covering each arm here catches regressions
    /// without the full async plumbing of an integration test.
    #[tokio::test]
    async fn handle_get_response_maps_none_contract_to_missing_contract_error() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x42;
        let instance_id = ContractInstanceId::new(bytes);

        let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            instance_id,
            freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
        );
        let result = handle_get_response(
            instance_id,
            Ok(Some(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(
                    ContractResponse::GetResponse {
                        key,
                        contract: None,
                        state: WrappedState::new(Vec::new()),
                    },
                )),
            })),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(WebSocketApiError::MissingContract { instance_id: id }) if id == instance_id
            ),
            "None-contract GetResponse must surface as MissingContract({instance_id}), got: {result:?}"
        );
    }

    /// Companion to the above: a `tokio::time::error::Elapsed` (30s fetch
    /// timeout) surfaces as an `AxumError(RequestError(Timeout))`, not a panic
    /// or hang.  `WebSocketApiError::into_response` maps this to a 503 with
    /// `<meta http-equiv="refresh">` — see #3472.  We use RequestError(Timeout)
    /// rather than the dual-use OperationError so terminal node OperationErrors
    /// (e.g. banned contracts) are NOT swept into the retry page.
    #[tokio::test]
    async fn handle_get_response_maps_timeout_to_request_timeout() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x43;
        let instance_id = ContractInstanceId::new(bytes);

        // Manufacture an Elapsed by racing an already-expired sleep.
        let elapsed = tokio::time::timeout(
            std::time::Duration::from_millis(0),
            std::future::pending::<()>(),
        )
        .await
        .expect_err("timeout must fire");
        let recv_result: Result<Option<HostCallbackResult>, _> = Err(elapsed);

        let result = handle_get_response(instance_id, recv_result).await;
        assert!(
            matches!(
                result,
                Err(WebSocketApiError::AxumError {
                    error: ErrorKind::RequestError(RequestError::Timeout)
                })
            ),
            "30s timeout must map to RequestError(Timeout) (for retry page), got: {result:?}"
        );
    }

    /// A closed response channel (`Ok(None)`, node shutting down) surfaces as
    /// `AxumError(ChannelClosed)` — an unambiguously transient kind that maps
    /// to the 503 retry page (#3472).
    #[tokio::test]
    async fn handle_get_response_maps_channel_closed_to_channel_closed() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x43;
        let instance_id = ContractInstanceId::new(bytes);

        let recv_result: Result<Option<HostCallbackResult>, tokio::time::error::Elapsed> = Ok(None);

        let result = handle_get_response(instance_id, recv_result).await;
        assert!(
            matches!(
                result,
                Err(WebSocketApiError::AxumError {
                    error: ErrorKind::ChannelClosed
                })
            ),
            "closed channel must map to ChannelClosed (for retry page), got: {result:?}"
        );
    }

    /// Extracts the response body as a UTF-8 string for test assertions.
    async fn response_body(resp: impl IntoResponse) -> String {
        let body = resp.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), 1024 * 1024)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn root_relative_asset_paths_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";
        let html = r#"<!DOCTYPE html>
<html>
    <head>
        <title>Test</title>
    <link rel="preload" as="script" href="/./assets/app.js" crossorigin></head>
    <body><div id="main"></div>
    <script type="module" async src="/./assets/app.js"></script>
    </body>
</html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected_href = format!("href=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_href),
            "href not rewritten.\nGot: {result}"
        );

        let expected_src = format!("src=\"/v1/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected_src),
            "src not rewritten.\nGot: {result}"
        );

        // Original root-relative paths should be gone
        assert!(
            !result.contains("\"/./assets/"),
            "original /./assets/ paths still present"
        );

        // WebSocket shim should be injected instead of raw auth token
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected"
        );
    }

    #[tokio::test]
    async fn root_relative_asset_paths_rewritten_v2() {
        let dir = tempfile::tempdir().unwrap();
        let key = "raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv";
        let html = r#"<head><link href="/./assets/app.js"></head><body></body>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V2, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected = format!("href=\"/v2/contract/web/{key}/assets/app.js\"");
        assert!(
            result.contains(&expected),
            "V2 href not rewritten.\nGot: {result}"
        );
        assert!(
            !result.contains("\"/./assets/"),
            "original /./assets/ paths still present in V2"
        );
    }

    #[tokio::test]
    async fn single_quoted_paths_also_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = "<head><script src='/./assets/app.js'></script></head>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        let expected = format!("'/v1/contract/web/{key}/assets/app.js'");
        assert!(
            result.contains(&expected),
            "single-quoted path not rewritten.\nGot: {result}"
        );
    }

    #[tokio::test]
    async fn paths_without_dot_slash_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Paths like "/assets/app.js" (without /.) should NOT be rewritten,
        // only the Dioxus-specific "/./assets/" pattern is targeted.
        let html = r#"<head><link href="/assets/app.css"></head><body></body>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        // The /assets/ path should remain unchanged (no /. prefix)
        assert!(
            result.contains("\"/assets/app.css\""),
            "path without /. was incorrectly rewritten.\nGot: {result}"
        );
    }

    #[tokio::test]
    async fn shell_page_iframe_sandbox_allows_downloads() {
        // Regression for freenet/mail#TBD: webapps that emit blob/object-URL
        // downloads via `<a download>` were silently dropped by Chromium
        // and Safari because the iframe sandbox omitted `allow-downloads`.
        // Lock the token in so a future refactor does not regress the fix.
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;
        assert!(
            html.contains("allow-downloads"),
            "iframe sandbox missing `allow-downloads` — user-initiated \
             file downloads from sandboxed webapps will be silently blocked \
             by the browser. Got HTML:\n{html}"
        );
    }

    #[tokio::test]
    async fn shell_page_hosted_mode_renders_proxy_chrome_bar() {
        // The hosted-mode "shell chrome" bar lives OUTSIDE the sandboxed iframe
        // and carries the "not private" disclosure plus the Account popover
        // (access-key backup/restore + export-to-your-own-peer). It must render
        // in hosted mode and be ABSENT in non-hosted mode so a normal
        // single-user node is unaffected.
        let token = AuthToken::generate();
        let hosted = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, true).unwrap(),
        )
        .await;
        assert!(
            hosted.contains(r#"id="fnbar""#),
            "hosted bar missing: {hosted}"
        );
        assert!(
            hosted.contains("not private"),
            "always-visible disclosure missing"
        );
        assert!(
            hosted.contains("Access key") && hosted.contains("Restore from key"),
            "access-key backup/restore controls missing"
        );
        assert!(hosted.contains("Export data"), "export control missing");
        // The export button must be wired to the node export endpoint, not a
        // placeholder. Pin the route so a refactor cannot silently revert it.
        assert!(
            hosted.contains("/v1/hosted/export"),
            "export button is not wired to the export endpoint"
        );
        // The access key is read from the shell-only token global; it is never
        // injected into the sandboxed iframe.
        assert!(
            hosted.contains("__freenet_user_token"),
            "access-key source global missing"
        );

        let plain = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;
        assert!(
            !plain.contains(r#"id="fnbar""#),
            "non-hosted shell must not render the proxy chrome bar"
        );
        assert!(
            !plain.contains("Export data"),
            "non-hosted shell must not render the export control"
        );
    }

    #[tokio::test]
    async fn shell_page_contains_iframe_and_bridge() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;

        // Shell page must contain sandboxed iframe
        assert!(
            html.contains(
                r#"sandbox="allow-scripts allow-forms allow-popups allow-downloads allow-modals""#
            ),
            "iframe sandbox attribute missing or wrong allowlist"
        );
        // Iframe must grant clipboard via permissions-policy
        assert!(
            html.contains(r#"allow="clipboard-read; clipboard-write""#),
            "iframe permissions-policy missing clipboard grants"
        );
        // Iframe src must include __sandbox=1
        assert!(
            html.contains("__sandbox=1"),
            "iframe src missing __sandbox=1 param"
        );
        // Bridge script must be present
        assert!(
            html.contains("freenetBridge"),
            "bridge script not found in shell page"
        );
        // Auth token must NOT be exposed as window.__FREENET_AUTH_TOKEN__
        assert!(
            !html.contains("__FREENET_AUTH_TOKEN__"),
            "auth token exposed in global variable (security risk)"
        );
        // Auth token should be passed to the bridge function
        assert!(
            html.contains(&format!("freenetBridge(\"{}\")", token.as_str())),
            "auth token not passed to bridge"
        );
        // Default title and favicon must be present
        assert!(
            html.contains("<title>Freenet</title>"),
            "shell page title mismatch"
        );
        assert!(
            html.contains(r#"<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,"#),
            "favicon should use inline data URI, not external URL"
        );
        assert!(
            !html.contains("freenet.org"),
            "shell page must not reference external origins (CORS)"
        );
        // Shell message handler must be present in bridge JS
        assert!(
            html.contains("__freenet_shell__"),
            "bridge JS must handle shell-level messages (title/favicon)"
        );
        // allow-popups-to-escape-sandbox must NOT be present. It was removed because
        // escaped popups gain localhost:7509 origin, allowing malicious web apps to
        // access other apps' data and bypass permission prompts. External links are
        // now opened via the open_url shell bridge message instead. See #1499.
        assert!(
            !html.contains("allow-popups-to-escape-sandbox"),
            "allow-popups-to-escape-sandbox must not be set (security: #1499)"
        );
        // open_url handler must be present in shell bridge JS for external links
        assert!(
            html.contains("open_url"),
            "shell bridge must handle open_url messages for external links"
        );
    }

    /// Regression test for issue #3836: permission prompts must render as an
    /// in-page overlay in the shell DOM, NOT via browser Notifications (which
    /// users block, miss, or dismiss accidentally).
    #[tokio::test]
    async fn shell_page_permission_overlay_present_and_safe() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;

        // Overlay root and accessibility attributes
        assert!(
            html.contains("__freenet_perm_overlay"),
            "permission overlay root element missing from shell JS"
        );
        assert!(
            html.contains("'role', 'dialog'") || html.contains("\"role\", \"dialog\""),
            "overlay must declare role=dialog for a11y"
        );
        assert!(html.contains("aria-modal"), "overlay must set aria-modal");
        // Subscribes to the new SSE endpoint and POSTs back with the response.
        // /permission/pending is still referenced as the bootstrap-on-connect
        // and `resync` reconciliation endpoint, plus the no-EventSource
        // fallback, so the assertion below still holds.
        assert!(
            html.contains("/permission/events"),
            "shell JS must subscribe to /permission/events (SSE)"
        );
        assert!(
            html.contains("/permission/pending"),
            "shell JS must reference /permission/pending for bootstrap/resync"
        );
        assert!(
            html.contains("/respond"),
            "shell JS must POST to /permission/{{nonce}}/respond"
        );
        // The 404 branch is the cross-tab dismissal contract: "another tab
        // answered, hide my card".
        assert!(
            html.contains("r.status === 404"),
            "shell JS must treat 404 on respond as 'already answered' and hide the card"
        );
        // SSE event names the server emits. Pinning these here ensures the
        // shell stays in sync with the gateway's wire format.
        assert!(
            html.contains("'prompt_added'") || html.contains("\"prompt_added\""),
            "shell JS must subscribe to the prompt_added SSE event"
        );
        assert!(
            html.contains("'prompt_removed'") || html.contains("\"prompt_removed\""),
            "shell JS must subscribe to the prompt_removed SSE event"
        );
        // All delegate-controlled strings must go through textContent, never
        // innerHTML — guards against a future refactor re-opening XSS into
        // the trusted shell origin.
        assert!(
            html.contains("function setText(el, text)"),
            "setText helper (textContent-only) missing"
        );
        // Bound the overlay code path by the explicit `perm-overlay-flow`
        // markers in shell_bridge.js, NOT a code anchor. The previous bound
        // (`setInterval(reconcileFromPending` / `EventSource`) stopped SHORT of
        // the SSE `prompt_added`/`prompt_removed` handlers, which ARE part of
        // the prompt-render flow #3836 protects — so a browser Notification
        // reintroduced into an SSE handler would have slipped past this guard.
        // The markers bracket the whole overlay + SSE region so the asserts
        // below scan all of it (#4849 F2).
        let overlay_start = html
            .find("perm-overlay-flow:BEGIN")
            .expect("perm-overlay-flow:BEGIN marker must bracket the overlay flow");
        let overlay_end = html[overlay_start..]
            .find("perm-overlay-flow:END")
            .expect("perm-overlay-flow:END marker must bracket the overlay flow");
        let overlay_slice = &html[overlay_start..overlay_start + overlay_end];
        // The negative asserts below are only meaningful if the slice actually
        // CONTAINS the SSE prompt-render surface. Pin that the marker-bounded
        // region includes the `prompt_added`/`prompt_removed` handlers, so a
        // refactor that moves them past `perm-overlay-flow:END` (shrinking the
        // slice) fails HERE rather than silently making the negative asserts
        // pass vacuously — the exact regression F2 exists to prevent (#4849).
        assert!(
            overlay_slice.contains("'prompt_added'") && overlay_slice.contains("'prompt_removed'"),
            "overlay guard slice must cover the SSE prompt handlers (#4849 F2)"
        );
        assert!(
            !overlay_slice.contains("innerHTML"),
            "overlay code path must not use innerHTML (XSS surface)"
        );

        // The old permission-prompt-via-Notification flow must be gone: the
        // permission OVERLAY code path must not request or construct a browser
        // Notification (#3836 — delegate permission prompts must render as the
        // in-page SSE overlay, never as a browser Notification users
        // block/miss/dismiss). Scoped to `overlay_slice`, NOT the whole shell:
        // browser Notifications are now legitimately used ELSEWHERE in the
        // bridge for new-MESSAGE notifications (a best-effort UX where a
        // missed/dismissed notification is fine, unlike a permission prompt),
        // pinned separately by `bridge_js_notification_proxy_invariants`. The
        // message-notification code sits well before the overlay root, so it is
        // outside this slice.
        assert!(
            !overlay_slice.contains("Notification.requestPermission"),
            "permission overlay must not request browser Notification permission (#3836)"
        );
        assert!(
            !overlay_slice.contains("new Notification("),
            "permission overlay must not construct a browser Notification (#3836)"
        );
        assert!(
            !html.contains("window.open('/permission/")
                && !html.contains("window.open(\"/permission/"),
            "shell must no longer open /permission/{{nonce}} as a popup (#3836)"
        );
        // The visibility-gated polling loop has been replaced by SSE. SSE
        // pushes regardless of tab visibility, so the visibility-skip code
        // path that caused the originating tab to silently miss prompts
        // when in the background MUST NOT be reintroduced. Pin this
        // contract by asserting `visibilityState` no longer appears in the
        // overlay path. If a future change needs visibility gating for some
        // *other* reason, that change must move this assertion or replace
        // the visibility-related JS with a deliberate no-op rather than
        // bringing back the polling-skip loop.
        assert!(
            !html.contains("visibilityState"),
            "overlay must not gate on document.visibilityState; \
             visibility-skip caused background tabs to miss prompts (SSE replaces polling)"
        );

        // Regression test for issue #3857: the overlay must read the new
        // tagged `caller` JSON shape and render the same Delegate /
        // Technical details treatment as the standalone /permission/{nonce}
        // page. A previous version of this code read `p.contract_id` and
        // fell through to "Unknown" — which silently re-shipped the bug
        // for the in-page overlay path even after the standalone page was
        // fixed. Tests below pin every replacement contract:
        //   1. The "Delegate says:" authorship label must survive (codex
        //      review point 2: removing it is a UX/security regression).
        //   2. The truncated-hash helper and tagged-caller formatter must
        //      both be present in the JS.
        //   3. The old `p.contract_id` field name must be gone.
        //   4. The old `<dl class="fn-ctx">` container must be gone.
        //   5. The new `formatCaller` helper must handle "webapp", "none",
        //      and unknown-kind variants so a future MessageOrigin variant
        //      (issue #3860) doesn't render as a bogus identity.
        assert!(
            html.contains("'Delegate says:'") || html.contains("\"Delegate says:\""),
            "shell overlay must render the 'Delegate says:' authorship label (#3857)"
        );
        assert!(
            html.contains("function truncateHash("),
            "shell overlay must define a truncateHash helper for the new disclosure (#3857)"
        );
        assert!(
            html.contains("function formatCaller("),
            "shell overlay must define a formatCaller helper for the tagged caller object (#3857)"
        );
        assert!(
            html.contains("p.caller"),
            "shell overlay must read p.caller from /permission/pending (#3857)"
        );
        assert!(
            !html.contains("p.contract_id"),
            "shell overlay must not read the removed p.contract_id field (#3857)"
        );
        assert!(
            !html.contains("'fn-ctx'") && !html.contains("\"fn-ctx\""),
            "shell overlay must not build the removed <dl class=\"fn-ctx\"> container (#3857)"
        );
        assert!(
            html.contains("'Freenet app '") || html.contains("\"Freenet app \""),
            "formatCaller must render webapp callers as 'Freenet app <hash>' (#3857)"
        );
        assert!(
            html.contains("'No app caller'") || html.contains("\"No app caller\""),
            "formatCaller must render the None / no-app case as 'No app caller' (#3857)"
        );
        assert!(
            html.contains("'Unknown caller'") || html.contains("\"Unknown caller\""),
            "formatCaller must have a forward-compatible fallback for unknown caller kinds (#3857)"
        );
        // The Technical details disclosure is the one the standalone page
        // also exposes; the overlay must mirror it so both code paths show
        // the user the same information.
        assert!(
            html.contains("'Technical details'") || html.contains("\"Technical details\""),
            "shell overlay must include a 'Technical details' disclosure (#3857)"
        );
        // The inline truncated delegate line is the always-visible passive
        // anomaly signal (codex review point 3). It must appear above the
        // Technical details disclosure, not only inside it.
        assert!(
            html.contains("'fn-delegate-line'") || html.contains("\"fn-delegate-line\""),
            "shell overlay must render the inline truncated delegate hash line (#3857)"
        );
    }

    /// Regression test: the iframe must use data-src (not src) so JS can build
    /// the final URL with the hash fragment before triggering the first load.
    /// Previously, src was set in HTML and the hash was sent via postMessage on
    /// the load event, but WASM apps hadn't registered their listener yet.
    /// See: #3747 (comment)
    #[tokio::test]
    async fn shell_page_iframe_uses_data_src_for_deep_linking() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;

        // The iframe must NOT have a src attribute (which would trigger an
        // immediate load before JS can append the hash fragment).
        assert!(
            !html.contains(
                r#"<iframe id="app" sandbox="allow-scripts allow-forms allow-popups allow-downloads" src="#
            ),
            "iframe must use data-src, not src, to avoid loading before JS appends the hash"
        );
        // The iframe must have data-src with the sandbox URL.
        assert!(
            html.contains("data-src=\"/"),
            "iframe must have data-src attribute for JS to read"
        );
    }

    #[tokio::test]
    async fn shell_page_forwards_query_params_to_iframe() {
        let token = AuthToken::generate();
        let qs = Some("invitation=abc123&room=test".to_string());
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, qs, None, false).unwrap(),
        )
        .await;

        // Query params should be forwarded to iframe src
        assert!(
            html.contains("invitation=abc123"),
            "invitation param not forwarded to iframe"
        );
        assert!(
            html.contains("room=test"),
            "room param not forwarded to iframe"
        );
        // __sandbox=1 must always be first
        assert!(
            html.contains("?__sandbox=1&"),
            "__sandbox=1 not first in iframe params"
        );
    }

    /// Regression test for #3841 (deep-link reload). When a sub-path is
    /// threaded into shell generation, the iframe's `data-src` must point
    /// at that sub-page (`/v1/contract/web/KEY/news/?__sandbox=1`) so the
    /// in-iframe webapp starts on the requested route. Before the fix the
    /// shell always pointed the iframe at the contract root, so reloading
    /// a deep link silently dropped the user back at `/`.
    #[tokio::test]
    async fn shell_page_embeds_sub_path_in_iframe_data_src() {
        let token = AuthToken::generate();

        // Directory-style deep link.
        let html = response_body(
            shell_page(
                &token,
                "testkey123",
                ApiVersion::V1,
                None,
                Some("news/"),
                false,
            )
            .unwrap(),
        )
        .await;
        assert!(
            html.contains(r#"data-src="/v1/contract/web/testkey123/news/?__sandbox=1""#),
            "iframe data-src must carry the sub-path; got: {html}"
        );

        // Nested extensionless deep link.
        let html = response_body(
            shell_page(
                &token,
                "testkey123",
                ApiVersion::V1,
                None,
                Some("about/team"),
                false,
            )
            .unwrap(),
        )
        .await;
        assert!(
            html.contains(r#"data-src="/v1/contract/web/testkey123/about/team?__sandbox=1""#),
            "iframe data-src must carry the nested sub-path; got: {html}"
        );

        // `None` sub-path keeps the iframe pointed at the contract root —
        // pins that the new parameter does not change root-load behaviour.
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;
        assert!(
            html.contains(r#"data-src="/v1/contract/web/testkey123/?__sandbox=1""#),
            "root load must still point the iframe at the contract root; got: {html}"
        );
    }

    /// The sub-path is interpolated into the iframe URL's path component,
    /// so query/fragment delimiters, control characters, and `..`/`.`
    /// traversal segments must be rejected before they can corrupt the
    /// `data-src` URL (or, once the browser HTML-unescapes the attribute,
    /// the surrounding markup) or — for `..` — be normalized by the
    /// browser into a different contract's prefix.
    #[test]
    fn sanitize_shell_sub_path_accepts_safe_paths_and_rejects_dangerous() {
        // Safe relative paths used by real multi-page webapps.
        for ok in ["news/", "about/team", "page2", "index.html", "a/b/c/"] {
            assert_eq!(
                sanitize_shell_sub_path(ok).unwrap(),
                ok,
                "{ok} must be accepted unchanged"
            );
        }

        // `..`/`.` segments MUST be rejected (Codex review, #3841): the
        // browser collapses dot-segments in a URL *before* requesting the
        // iframe, so `/v1/contract/web/KEY/../OTHER/` would be normalized
        // to `/v1/contract/web/OTHER/` and load a different contract under
        // the current shell's token. The later `sandbox_content_body`
        // canonicalization never sees the un-normalized traversal, so this
        // guard is the only layer that can stop it.
        for traversal in ["..", "../other", "a/../b", "a/..", "a/./b", "."] {
            assert!(
                matches!(
                    sanitize_shell_sub_path(traversal),
                    Err(WebSocketApiError::InvalidParam { .. })
                ),
                "{traversal:?} (dot-segment) must be rejected"
            );
        }

        // Dangerous inputs that would break out of the URL path component
        // or inject into the attribute/markup must be rejected.
        for bad in [
            "/absolute",        // leading slash escapes the contract prefix
            "news/?evil=1",     // `?` starts a query, corrupting __sandbox=1
            "news/#frag",       // `#` starts a fragment
            "a b",              // whitespace
            "x\r\nInjected: y", // CRLF (header/markup injection surface)
            "back\\slash",      // backslash (browsers may treat as `/`)
            "tab\tafter",       // control char
        ] {
            assert!(
                matches!(
                    sanitize_shell_sub_path(bad),
                    Err(WebSocketApiError::InvalidParam { .. })
                ),
                "{bad:?} must be rejected"
            );
        }
    }

    /// End-to-end regression for #3841: a deep-link reload routed through
    /// `contract_home` (the path `web_subpages` takes for a top-level
    /// document load of a sub-page) must fetch/cache the contract AND
    /// produce a shell whose iframe loads the requested sub-page, not the
    /// contract root. Drives the real `ensure_contract_cached` cycle via
    /// `serve_one_get`, then inspects the rendered shell HTML.
    #[tokio::test]
    async fn contract_home_with_sub_path_renders_shell_for_that_page() {
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 1, 8, 4, 1])),
            Parameters::from(vec![3, 8, 4, 1]),
        )));
        let instance_id = *contract.key().id();
        let key = instance_id.to_string();
        let state = WrappedState::new(vec![4, 2]);
        clear_cache(&instance_id).await;

        // Warm cache whose stored hash matches the state the served GET
        // returns, so `unpack_if_stale` takes its matching-hash early
        // return and the refresh succeeds without a real WebApp unpack.
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        let matching_hash = hash_state(state.as_ref());
        tokio::fs::write(state_hash_path(&instance_id), matching_hash.to_be_bytes())
            .await
            .unwrap();

        let (sender, mut rx) = request_channel();
        let token = AuthToken::generate();
        let handler = {
            let key = key.clone();
            tokio::spawn(async move {
                contract_home(
                    key,
                    sender,
                    token,
                    ApiVersion::V1,
                    None,
                    Some("news/"),
                    false,
                )
                .await
                .map(|resp| resp.into_response())
            })
        };

        // Service the fetch the shell render triggers.
        serve_one_get(&mut rx, &contract, &state).await;

        let resp = handler
            .await
            .expect("contract_home task must not panic")
            .expect("contract_home must succeed once the GET is served");
        let html = response_body(resp).await;
        assert!(
            html.contains(&format!(
                r#"data-src="/v1/contract/web/{key}/news/?__sandbox=1""#
            )),
            "deep-link shell iframe must load the sub-page; got: {html}"
        );

        clear_cache(&instance_id).await;
    }

    #[tokio::test]
    async fn sandbox_content_injects_shims_not_auth_token() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html = r#"<!DOCTYPE html><html><head></head><body>Hello</body></html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        // WS shim must be injected
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected"
        );
        assert!(
            result.contains("window.WebSocket = FreenetWebSocket"),
            "WebSocket override not set"
        );
        // Navigation interceptor must be injected alongside WebSocket shim
        assert!(
            result.contains("type: 'navigate'"),
            "navigation interceptor not injected"
        );
        // Auth token must NOT appear in sandbox content
        assert!(
            !result.contains("__FREENET_AUTH_TOKEN__"),
            "auth token leaked into sandbox content"
        );
    }

    #[tokio::test]
    async fn ws_shim_injected_without_head_tag() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // HTML with <body> but no </head> tag
        let html = "<body><div>Hello</div></body>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected when no </head> tag"
        );
        // Shim should appear before <body
        let shim_pos = result.find("FreenetWebSocket").unwrap();
        let body_pos = result.find("<body").unwrap();
        assert!(
            shim_pos < body_pos,
            "shim should be injected before <body> tag"
        );
    }

    #[tokio::test]
    async fn ws_shim_injected_in_minimal_html() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Minimal HTML with no <head> or <body> tags
        let html = "<div>Hello World</div>";
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in minimal HTML"
        );
        // Shim should be prepended (appears before the content)
        assert!(
            result.starts_with("<script>"),
            "shim should be prepended to content when no head/body tags"
        );
    }

    #[tokio::test]
    async fn shell_page_strips_sandbox_prefixed_params() {
        let token = AuthToken::generate();
        let qs = Some("__sandbox_extra=evil&invitation=abc&__sandboxFoo=bar".to_string());
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, qs, None, false).unwrap(),
        )
        .await;

        // __sandbox-prefixed params must be stripped
        assert!(
            !html.contains("__sandbox_extra"),
            "__sandbox_extra param should be stripped"
        );
        assert!(
            !html.contains("__sandboxFoo"),
            "__sandboxFoo param should be stripped"
        );
        // Normal params should be forwarded
        assert!(
            html.contains("invitation=abc"),
            "normal param should be forwarded"
        );
    }

    /// Regression test for the cross-contract `authToken` injection
    /// surface raised in review. A crafted cross-contract link with
    /// `?authToken=attacker_value` reaches `shell_page` via the
    /// `resolved.search` passthrough in the navigate bridge (or via a
    /// pasted deep link that the subpage redirect forwards). The
    /// iframe URL must never carry an attacker-supplied `authToken`
    /// because any webapp that reads credentials from
    /// `location.search` (Delta, River) would pick it up and use it
    /// as its WebSocket credential.
    #[tokio::test]
    async fn shell_page_strips_auth_token_from_forwarded_query() {
        let token = AuthToken::generate();
        let qs = Some("authToken=attacker_value&invite=abc&authTokenExtra=x".to_string());
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, qs, None, false).unwrap(),
        )
        .await;
        assert!(
            !html.contains("attacker_value"),
            "attacker-supplied authToken value must not reach iframe src"
        );
        assert!(
            !html.contains("authTokenExtra"),
            "authToken-prefixed params must also be stripped"
        );
        assert!(
            html.contains("invite=abc"),
            "harmless params must still be forwarded"
        );
        // The only authToken in the resulting HTML is the
        // freshly-generated one passed to `freenetBridge(authToken)`,
        // not a query-string value in the iframe src.
        assert!(
            html.contains(&format!("freenetBridge(\"{}\"", token.as_str())),
            "shell must still bind the freshly-generated auth token"
        );
    }

    #[tokio::test]
    async fn shell_page_escapes_html_in_query_params() {
        let token = AuthToken::generate();
        let qs = Some("foo=\"><script>alert(1)</script>".to_string());
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, qs, None, false).unwrap(),
        )
        .await;

        // The double quote and angle brackets must be escaped
        assert!(
            !html.contains("\"><script>alert"),
            "unescaped HTML injection in iframe src"
        );
        assert!(
            html.contains("&quot;"),
            "double quote should be HTML-escaped"
        );
    }

    /// Hosted mode (P2-frontend of #4381): the shell page must mint/load a
    /// durable per-user token in `localStorage` and hand it to the bridge as a
    /// second argument, so the proxied WebSocket upgrade carries
    /// `?userToken=<token>`.
    #[tokio::test]
    async fn shell_page_hosted_mode_injects_user_token() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, true).unwrap(),
        )
        .await;

        // The localStorage token-minting snippet must be present.
        assert!(
            html.contains("__freenet_user_token__"),
            "hosted-mode shell must include the durable localStorage token key; got: {html}"
        );
        assert!(
            html.contains("crypto.getRandomValues"),
            "hosted-mode token must be minted from crypto.getRandomValues, not request input"
        );
        assert!(
            html.contains("localStorage.setItem"),
            "hosted-mode token must be persisted to localStorage"
        );
        // New identities must mint a base58 access key: the shell must carry the
        // inline base58 encoder and the Bitcoin/bs58 alphabet, and must NOT use
        // the old hex encoding (`toString(16)`). The server hashes the raw token
        // string, so a previously stored hex token still works — this only pins
        // the format newly minted tokens take. See shell_user_token.js.
        assert!(
            html.contains("base58Encode")
                && html.contains("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"),
            "hosted-mode token must be minted as base58 via the inline encoder; got: {html}"
        );
        assert!(
            !html.contains("toString(16)"),
            "hosted-mode token must no longer be hex-encoded (toString(16)); got: {html}"
        );
        // The bridge must be called with the user-token argument AND the
        // hosted-mode flag (so it can fail closed over http).
        assert!(
            html.contains(&format!(
                "freenetBridge(\"{}\", __freenet_user_token, true);",
                token.as_str()
            )),
            "hosted-mode shell must call freenetBridge with the user token and hosted flag; got: {html}"
        );
        // The bridge must NOT be called in the 1-arg form in hosted mode.
        assert!(
            !html.contains(&format!("freenetBridge(\"{}\");", token.as_str())),
            "hosted-mode shell must not emit the 1-arg freenetBridge call"
        );
    }

    /// Non-hosted mode must be byte-for-byte the pre-#4381 shell: no token
    /// snippet, the original 1-arg `freenetBridge(...)` call, and no `userToken`
    /// string anywhere. This is the no-regression guard for the default path.
    #[tokio::test]
    async fn shell_page_non_hosted_mode_omits_user_token() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;

        // The per-user-token MINTING machinery (the localStorage snippet and
        // its `__freenet_user_token` variable) must be absent: a non-hosted
        // visitor never gets a durable identity. Note the always-injected
        // `SHELL_BRIDGE_JS` still *mentions* `userToken` as an inert, undefined
        // closure argument guarded by `if (userToken)`, so we deliberately do
        // not assert the substring `userToken` is wholly absent — we assert the
        // minting snippet and the 2-arg call (the parts that actually activate
        // the feature) are absent.
        assert!(
            !html.contains("__freenet_user_token"),
            "non-hosted shell must not mint a per-user token; got: {html}"
        );
        // NB: the always-injected bridge legitimately calls `localStorage.setItem`
        // for per-contract notification preferences (consent / snooze — see
        // `bridge_js_notification_proxy_invariants`). That is NOT a per-user
        // identity token, so we do not blanket-ban `setItem` here; the
        // token-persistence guard is the absence of the token key
        // (`__freenet_user_token`, above) and of the 2-arg bridge call (below).
        assert!(
            !html.contains(", __freenet_user_token)"),
            "non-hosted shell must not call freenetBridge with a user token"
        );
        // The original single-argument bridge call must be emitted unchanged
        // (byte-for-byte the pre-#4381 output).
        assert!(
            html.contains(&format!("freenetBridge(\"{}\");", token.as_str())),
            "non-hosted shell must emit the original 1-arg freenetBridge call; got: {html}"
        );
    }

    /// Pins that the per-user-token machinery is wired through the bridge JS
    /// itself (not just the page wrapper): the WS-open handler must append the
    /// `userToken` query param to the real WebSocket URL when a token is set,
    /// and `SHELL_USER_TOKEN_JS` must mint it from OS entropy.
    #[test]
    fn bridge_js_appends_user_token_param() {
        assert!(
            SHELL_BRIDGE_JS.contains("function freenetBridge(authToken, userToken, hostedMode)"),
            "bridge function must accept the per-user token and hosted-mode arguments"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("u.searchParams.set('userToken', userToken)"),
            "bridge must append userToken to the real WebSocket URL"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("if (userToken"),
            "bridge must only append userToken when present (non-hosted = undefined)"
        );
        assert!(
            SHELL_USER_TOKEN_JS.contains("crypto.getRandomValues"),
            "user-token snippet must mint the token from OS entropy"
        );
        assert!(
            SHELL_USER_TOKEN_JS.contains("__freenet_user_token__"),
            "user-token snippet must persist under the durable localStorage key"
        );
    }

    /// freenet/river#408: the browser-notification proxy carries several
    /// security-relevant invariants (a sandboxed contract app hands notifications
    /// to the real-origin shell over the postMessage bridge). Pin them by source
    /// so a refactor can't silently drop them — same discipline as the other
    /// `SHELL_BRIDGE_JS.contains` guards above.
    #[test]
    fn bridge_js_notification_proxy_invariants() {
        // Consent key is derived ONLY from the trusted server-routed path, never
        // from message content, and matches BOTH API versions so a v2 load isn't
        // stranded (permission granted but every notification silently dropped).
        assert!(
            SHELL_BRIDGE_JS.contains(r"/\/v[12]\/contract\/web\/([^/?#]+)/"),
            "notification consent key must derive from the /v[12]/contract/web/<key> path"
        );
        // Every notification is gated on BOTH the browser permission AND this
        // contract's own consent, so one contract's gateway-wide browser grant
        // can't notify the user on behalf of a different contract.
        assert!(
            SHELL_BRIDGE_JS
                .contains("Notification.permission !== 'granted' || !contractHasConsent()"),
            "showAppNotification must gate on browser permission AND per-contract consent"
        );
        // "Not now" must be durable so a contract that re-sends the enable prompt
        // can't re-pin the host-owned bar over the app.
        assert!(
            SHELL_BRIDGE_JS.contains("isNotifySnoozed()")
                && SHELL_BRIDGE_JS.contains("setNotifySnoozed()"),
            "notification dismissal must be enforced via the snooze guard"
        );
        // Notifications pass a rate limiter (per-tag + rolling global cap) so a
        // consented contract can't flood the user with OS notifications.
        assert!(
            SHELL_BRIDGE_JS.contains("notifyLimiter.ok("),
            "notifications must pass the per-tag + global rate limiter"
        );
        // Attacker-controlled notification text is length-capped (text-only).
        assert!(
            SHELL_BRIDGE_JS.contains("String(msg.title).slice(0, 128)"),
            "notification title must be length-capped"
        );
        // The permission prompt is only fired from a real click on the shell
        // affordance (transient activation must come from the shell frame).
        assert!(
            SHELL_BRIDGE_JS.contains("Notification.requestPermission(done)"),
            "permission prompt must be requested from the shell affordance click"
        );
    }

    /// Regression for #4849: the notification-proxy flood-cap (the rolling
    /// global window in `makeNotifyRateLimiter`) must be PERSISTED per-contract
    /// so a full page reload can't reset it. Without this, a consented contract
    /// could fire the whole budget, force a reload (a same-contract v1<->v2
    /// `navigate`, which the shell reloads as cross-contract), and start over
    /// with an empty limiter. The behavioral proof is in
    /// shell_bridge_notifications.test.mjs (the reload rehydration case); this
    /// pins the WIRING at the source level so a refactor can't silently drop
    /// the persistence and re-open the reload-reset hole.
    #[test]
    fn bridge_js_notification_flood_cap_persisted_across_reload() {
        // The limiter is constructed WITH the persistence store, not the old
        // no-arg makeNotifyRateLimiter().
        assert!(
            SHELL_BRIDGE_JS.contains("makeNotifyRateLimiter(makeNotifyRateStore())"),
            "rate limiter must be constructed with the persistence store (#4849)"
        );
        // The store is keyed off the version-less contract consent key (so the
        // window survives a v1<->v2 reload) with a `:rate` suffix, and is backed
        // by sessionStorage (per-tab, same-origin, reload-surviving).
        assert!(
            SHELL_BRIDGE_JS.contains("ckey + ':rate'"),
            "rate window must use a contract-scoped storage key (#4849)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("sessionStorage.getItem(storeKey)")
                && SHELL_BRIDGE_JS.contains("sessionStorage.setItem(storeKey"),
            "rate window must be persisted in sessionStorage (#4849)"
        );
        // The factory actually rehydrates from and saves to the injected store.
        assert!(
            SHELL_BRIDGE_JS.contains("store.load()")
                && SHELL_BRIDGE_JS.contains("store.save(recent)"),
            "limiter must rehydrate from and persist to the injected store (#4849)"
        );
        // The bfcache reset variant is closed by a `pageshow`-persisted resync
        // (the IIFE does not re-run on back-forward-cache restore, so the
        // in-memory window would otherwise stay stale). Pin the wiring so a
        // refactor can't silently drop it.
        assert!(
            SHELL_BRIDGE_JS.contains("pageshow")
                && SHELL_BRIDGE_JS.contains("notifyLimiter.resync()"),
            "flood-cap window must be resynced from the store on bfcache restore (#4849)"
        );
    }

    /// REFUSE-PLAINTEXT-TOKEN, client side (Codex review, #4513): the durable
    /// per-user token is a high-value bearer secret and must never cross a
    /// plaintext wire. Two INDEPENDENT guards enforce this so a refactor of
    /// either can't reopen the leak:
    ///   1. `SHELL_USER_TOKEN_JS` returns undefined on a non-https page BEFORE
    ///      touching localStorage (never loads/mints/transmits the token), and
    ///   2. the bridge WS-open handler gates the `userToken` append on
    ///      `location.protocol === 'https:'`.
    #[test]
    fn user_token_never_transmitted_over_plaintext() {
        // Guard 1: the https check must precede any localStorage access in the
        // minting IIFE, so an http page returns undefined without reading the
        // stored token.
        let https_guard = SHELL_USER_TOKEN_JS
            .find("location.protocol !== 'https:'")
            .expect("user-token snippet must refuse to run on a non-https page");
        // Anchor on the actual localStorage READ (`localStorage.getItem`), not
        // the bare word "localStorage" which also appears in the rationale
        // comment above the guard.
        let first_storage_access = SHELL_USER_TOKEN_JS
            .find("localStorage.getItem")
            .expect("user-token snippet must read from localStorage");
        assert!(
            https_guard < first_storage_access,
            "the https guard must run BEFORE any localStorage access so an http \
             page never even reads a previously-minted token"
        );
        assert!(
            SHELL_USER_TOKEN_JS.contains("return undefined"),
            "the non-https branch must yield an undefined token"
        );

        // Guard 2: the bridge append is gated on https as a second barrier.
        assert!(
            SHELL_BRIDGE_JS.contains("location.protocol === 'https:'"),
            "bridge must gate the userToken append on a secure connection"
        );
        let https_attach_guard = SHELL_BRIDGE_JS
            .find("userToken && location.protocol === 'https:'")
            .expect("bridge must only attach userToken over https");
        let set_user = SHELL_BRIDGE_JS
            .find("u.searchParams.set('userToken', userToken)")
            .expect("bridge must have a userToken append site");
        assert!(
            https_attach_guard < set_user,
            "the https guard must precede the userToken append"
        );
    }

    /// FAIL CLOSED, not shared-Local (Codex review, #4381): a HOSTED browser
    /// with no per-user token must REFUSE to operate, not silently connect onto
    /// the shared Local delegate-secret namespace. The token is absent for two
    /// reasons that BOTH must fail closed — plaintext http (token withheld by
    /// the transmit guards) and https-but-storage/crypto-failure (mint throws,
    /// catch returns undefined). The unified `hostedMode === true && !userToken`
    /// condition covers both, so the test keys off the token-absent condition
    /// rather than re-checking the protocol. The shell must (a) not load the
    /// app, showing a message instead, and (b) refuse all WebSocket opens.
    #[tokio::test]
    async fn hosted_shell_fails_closed_when_no_user_token() {
        // The hosted shell page must pass the hosted flag to the bridge so it
        // CAN fail closed; without the third `true` arg the bridge can't tell
        // it's hosted.
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, true).unwrap(),
        )
        .await;
        assert!(
            html.contains(&format!(
                "freenetBridge(\"{}\", __freenet_user_token, true);",
                token.as_str()
            )),
            "hosted shell must pass the hosted flag (true) to the bridge; got: {html}"
        );

        // Unified guard: hosted AND no token (for ANY reason). Keying off
        // `!userToken` covers both the http (token withheld) and the
        // https-but-storage-failure (mint returned undefined) cases with one
        // condition. Requires hostedMode === true so non-hosted (hostedMode
        // undefined) is always inert, and a truthy token (hosted+https+minted)
        // operates normally.
        assert!(
            SHELL_BRIDGE_JS.contains("hostedMode === true && !userToken"),
            "fail-closed must require hosted mode AND an absent token (any cause)"
        );
        // The guard must NOT re-check the protocol — that would miss the
        // https+storage-failure case (token undefined despite https).
        assert!(
            !SHELL_BRIDGE_JS.contains("hostedMode === true && location.protocol"),
            "fail-closed must not key off the protocol (misses https+no-storage)"
        );

        // Effect 1 — the app is not loaded: the iframe is removed and a message
        // is shown instead. Anchor on the removeChild of the iframe and the
        // alert role.
        assert!(
            SHELL_BRIDGE_JS.contains("removeChild(iframe)"),
            "fail-closed must not load the app iframe"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("role', 'alert'") || SHELL_BRIDGE_JS.contains("'alert'"),
            "fail-closed must render a visible alert message"
        );

        // Effect 2 — the WS-open handler refuses while hostedNoToken, BEFORE it
        // would otherwise open a socket on the shared Local namespace. Assert the
        // refusal check precedes the real WebSocket construction.
        let refuse = SHELL_BRIDGE_JS
            .find("if (hostedNoToken)")
            .expect("WS-open handler must refuse while hosted+no-token");
        let open_socket = SHELL_BRIDGE_JS
            .find("new WebSocket(u.toString()")
            .expect("bridge must have a WebSocket open site");
        assert!(
            refuse < open_socket,
            "the hostedNoToken refusal must precede opening the real socket"
        );
    }

    /// Regression test for #4645: the hosted fail-closed page must give the
    /// user an ACTIONABLE recovery path, not a dead end.
    ///
    /// The dominant real-world trigger is opening a Freenet app link as a NEW
    /// TAB/WINDOW from inside the sandboxed app iframe (the browser's "open
    /// link in new tab", a middle-click, a right-click menu, `window.open`, or
    /// a `target=_blank` link). Such a context inherits the iframe sandbox, so
    /// it has an opaque origin (`window.origin === 'null'`), so `localStorage`
    /// throws and the per-user token can't be read — and the shell fails
    /// closed. The pre-#4645 page only said "reconnect using https / enable
    /// storage", which is useless for that case: the tab already IS https with
    /// storage; the opaque origin is what blocks it. The page must instead
    /// detect the opaque-origin case and tell the user to re-open the address
    /// in a normal tab, surfacing the URL for one-click copy.
    #[test]
    fn fail_closed_page_gives_actionable_recovery_4645() {
        // Detects the opaque-origin (sandboxed new-tab) case. The tell-tale is
        // `window.origin` serializing to the string "null" for an opaque
        // origin (confirmed empirically against try.freenet.org).
        assert!(
            SHELL_BRIDGE_JS.contains("window.origin === 'null'"),
            "fail-closed page must detect the opaque-origin (sandboxed new-tab) \
             case so it can give the right recovery guidance (#4645)"
        );
        // The "open in a normal tab" recovery only helps on a SECURE connection:
        // over http even a fresh tab can't mint a token (SHELL_USER_TOKEN_JS
        // refuses), so the https guidance must win when a page is BOTH sandboxed
        // and plaintext. Pin that the reopen affordance is gated on
        // `opaqueOrigin && !plaintext` rather than opaqueOrigin alone (Codex P3).
        assert!(
            SHELL_BRIDGE_JS.contains("opaqueOrigin && !plaintext"),
            "the re-open recovery must be gated on a secure connection, so an \
             http+sandboxed page is told to use https rather than to re-open a \
             URL that still can't mint a token"
        );
        // For that case it surfaces the current URL so the user can re-open it
        // in a normal top-level tab (where a real origin lets the token mint).
        assert!(
            SHELL_BRIDGE_JS.contains("field.value = location.href"),
            "fail-closed page must surface the page URL for the user to re-open"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("Copy address"),
            "fail-closed page must offer a one-click copy of the address"
        );
        // The recovery copy must be explicit that THIS tab is the one stuck and
        // that reloading / retyping the URL here will keep failing — the exact
        // confusion a user reported (the address bar shows the clean URL, so a
        // reload looks like it should work but stays sandboxed). Steer them to a
        // genuinely new top-level tab.
        assert!(
            SHELL_BRIDGE_JS.contains("brand-new"),
            "recovery copy must tell the user to open a brand-new tab (a reload \
             of this sandbox-inherited tab keeps failing) (#4645)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("Reloading or editing the address in this tab will"),
            "recovery copy must warn that reloading/editing the address in this \
             same tab will not work (#4645)"
        );
        // The three distinct causes (opaque-origin restricted tab, plain http,
        // storage disabled) get distinct headings so the guidance actually
        // matches the situation rather than blaming https/storage for all.
        assert!(
            SHELL_BRIDGE_JS.contains("Open this app in a normal tab")
                && SHELL_BRIDGE_JS.contains("Secure connection required")
                && SHELL_BRIDGE_JS.contains("Browser storage required"),
            "fail-closed page must tailor its heading to each of the three causes"
        );
        // Anti-footgun: the fail-closed block must NOT try to re-open the app
        // via `window.open` — a popup opened from this already-sandboxed context
        // inherits the sandbox and hits the exact same dead end. Recovery is the
        // user opening a fresh top-level tab themselves. (The bridge does call
        // window.open legitimately in the open_url handler far below, so scope
        // the check to the fail-closed rendering block.)
        let block_start = SHELL_BRIDGE_JS
            .find("if (hostedNoToken) {")
            .expect("fail-closed block present");
        // Anchor the block end on CODE from the normal (non-fail-closed) load
        // branch rather than a comment, so a future comment reword can't
        // silently move the boundary. `iframe.getAttribute('data-src')` is the
        // first statement of the else branch and never appears in the
        // fail-closed block.
        let block_end = SHELL_BRIDGE_JS[block_start..]
            .find("iframe.getAttribute('data-src')")
            .expect("fail-closed block is followed by the normal iframe-load branch")
            + block_start;
        let fail_closed_block = &SHELL_BRIDGE_JS[block_start..block_end];
        assert!(
            !fail_closed_block.contains("window.open("),
            "fail-closed recovery must not call window.open (a popup from this \
             sandboxed context inherits the sandbox and re-hits the dead end)"
        );
    }

    /// Regression test for #4645 (second half): the hosted Account popover must
    /// offer a "New ID" control so a user can start over with a fresh identity
    /// from the UI, instead of hand-deleting the token from browser devtools —
    /// the exact friction the try.freenet.org feedback reported. Minting is
    /// delegated to SHELL_USER_TOKEN_JS: clearing the stored key is enough
    /// because the next load mints a new random token when the key is absent.
    #[test]
    fn hosted_bar_offers_new_id_control_4645() {
        // The button exists in the Account popover.
        assert!(
            HOSTED_BAR_HTML.contains("id=\"fnnewid\""),
            "hosted bar must expose a New ID control (#4645)"
        );
        // The handler is wired to that button.
        let handler = HOSTED_BAR_JS
            .find("getElementById('fnnewid')")
            .expect("New ID button must have a click handler");
        // It clears the SAME storage key SHELL_USER_TOKEN_JS mints under, so the
        // reload re-mints a fresh token. Pin the exact key on BOTH sides: a
        // rename on either would silently turn "New ID" into a no-op (clears a
        // key nobody reads) or a broken reset (clears the wrong key).
        assert!(
            HOSTED_BAR_JS.contains("removeItem('__freenet_user_token__')"),
            "New ID must clear the stored per-user token"
        );
        assert!(
            SHELL_USER_TOKEN_JS.contains("__freenet_user_token__"),
            "New ID clears the key SHELL_USER_TOKEN_JS mints under; keep in sync"
        );
        // Destructive action: confirm BEFORE clearing, so a cancelled prompt
        // leaves the current identity intact.
        let confirm = HOSTED_BAR_JS[handler..]
            .find("window.confirm(")
            .map(|o| o + handler)
            .expect("New ID must confirm before discarding the current identity");
        let clear = HOSTED_BAR_JS[handler..]
            .find("removeItem('__freenet_user_token__')")
            .map(|o| o + handler)
            .expect("New ID must clear the token");
        assert!(
            confirm < clear,
            "the confirm prompt must run before the token is cleared, so \
             cancelling keeps the current identity"
        );
        // The reload (which re-mints) comes after clearing.
        let reload = HOSTED_BAR_JS[clear..]
            .find("location.reload()")
            .map(|o| o + clear)
            .expect("New ID must reload so a fresh token mints");
        assert!(clear < reload, "must clear the token before reloading");
    }

    /// The hosted "Move to my peer" migration (#4592) must default to a
    /// ONE-CLICK open against the user's local peer, keeping copy-the-URL only
    /// as a SECONDARY fallback. Before this, the only affordance was a link the
    /// user had to hand-copy and paste into another browser — friction for the
    /// exact action we want them to take. This pins the whole primary/secondary
    /// contract so a refactor can't silently regress it back to copy-only.
    #[test]
    fn hosted_bar_migration_defaults_to_one_click_open_4592() {
        // (a) A PRIMARY "open on my peer" control that opens the peer import
        // page in a new browsing context (so the hosted tab is left intact).
        assert!(
            HOSTED_BAR_HTML.contains("id=\"fnmigrateopen\""),
            "hosted bar must expose a primary 'open on my peer' control (#4592)"
        );
        let open_idx = HOSTED_BAR_HTML
            .find("id=\"fnmigrateopen\"")
            .expect("primary open control present");
        // The control is an anchor with target=_blank in the SAME element, so a
        // plain click (or cmd/middle-click into another profile) opens the peer
        // import page directly — the "direct link" the friction complaint asked
        // for. Assert the target belongs to this control (nearby), not anywhere.
        let target_idx = HOSTED_BAR_HTML
            .find("target=\"_blank\"")
            .expect("the primary open control must target a new browsing context");
        assert!(
            target_idx > open_idx && target_idx - open_idx < 120,
            "target=_blank must be on the primary open control's own element"
        );

        // (b) The mint handler performs the one-click open: it opens a tab and
        // navigates it to the freshly minted LOCAL peer import link, instead of
        // only revealing a box to copy. Both the open and the loopback import
        // path must be present in the migrate handler.
        let mint = HOSTED_BAR_JS
            .find("getElementById('fnmigrate')")
            .expect("Move-to-my-peer button must have a click handler");
        assert!(
            HOSTED_BAR_JS[mint..].contains("window.open("),
            "the migration default must open the peer import page directly \
             (one-click), not merely surface a link to copy"
        );
        // Reverse-tabnabbing hardening: the freshly-opened tab must have its
        // window.opener severed so the destination peer page can't navigate the
        // hosted tab back to a spoofed origin. Set synchronously while the tab is
        // still about:blank; it survives the later peerWin.location navigation.
        assert!(
            HOSTED_BAR_JS[mint..].contains("peerWin.opener = null"),
            "the one-click open must sever window.opener to prevent \
             reverse tabnabbing"
        );
        assert!(
            HOSTED_BAR_JS.contains("/hosted/import?source="),
            "the one-click open must target the local peer's import page"
        );
        // The handler sets the primary control's href too, so the fallback link
        // (used when the pop-up is blocked) points at the same minted link.
        assert!(
            HOSTED_BAR_JS.contains("migrateOpen.href = link"),
            "the primary open control's href must be set to the minted link"
        );

        // (c) Copy-the-URL remains available as the SECONDARY option (kept for a
        // peer on a different computer/browser/profile) — never removed.
        assert!(
            HOSTED_BAR_HTML.contains("id=\"fnmigratecopy\"")
                && HOSTED_BAR_HTML.contains("id=\"fnmigratelink\""),
            "copy-the-URL must remain available as a secondary fallback"
        );
        assert!(
            HOSTED_BAR_JS.contains("getElementById('fnmigratecopy')"),
            "the secondary copy-link control must stay wired to clipboard copy"
        );
    }

    /// Non-hosted mode must NEVER reach the fail-closed path: the bridge is
    /// called with one argument, so `hostedMode` is undefined and the whole
    /// hostedNoToken branch is inert — the app loads and connects over http
    /// exactly as before #4381. (Single-user nodes commonly run over http.)
    #[tokio::test]
    async fn non_hosted_shell_never_fails_closed() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;
        // The 1-arg call leaves hostedMode undefined; `=== true` is then false.
        assert!(
            html.contains(&format!("freenetBridge(\"{}\");", token.as_str())),
            "non-hosted shell must use the 1-arg freenetBridge call; got: {html}"
        );
        assert!(
            !html.contains(", true);"),
            "non-hosted shell must not pass the hosted-mode flag to the bridge"
        );
    }

    /// Isolation-boundary regression (Codex review, #4513): the sandboxed app
    /// must never be able to choose its own per-user (or auth) identity by
    /// putting a `userToken` / `authToken` on the WebSocket URL it asks the
    /// shell to open. The bridge must STRIP any caller-supplied credentials
    /// before injecting its own, and the strip must run BEFORE the conditional
    /// `set('userToken', ...)` — otherwise a caller token survives whenever the
    /// shell's minted token is undefined (localStorage disabled / private mode),
    /// letting the app pick its own secret namespace.
    #[test]
    fn bridge_js_strips_caller_supplied_user_token_before_injecting() {
        let delete_user = SHELL_BRIDGE_JS
            .find("u.searchParams.delete('userToken')")
            .expect("bridge must delete any caller-supplied userToken");
        let delete_auth = SHELL_BRIDGE_JS
            .find("u.searchParams.delete('authToken')")
            .expect("bridge must delete any caller-supplied authToken (defense-in-depth)");
        let set_auth = SHELL_BRIDGE_JS
            .find("u.searchParams.set('authToken', authToken)")
            .expect("bridge must inject the shell's authToken");
        // Anchor on the userToken append itself rather than the full
        // conditional, whose guard expression is allowed to evolve (it now also
        // carries the https barrier — see user_token_never_transmitted_over_plaintext).
        let conditional_set_user = SHELL_BRIDGE_JS
            .find("u.searchParams.set('userToken', userToken)")
            .expect("bridge must conditionally inject the shell's minted userToken");

        // The deletes must precede BOTH injection points, so a caller value can
        // never survive — including the undefined-token path where the
        // conditional set is skipped entirely.
        assert!(
            delete_user < conditional_set_user,
            "delete('userToken') must run before the conditional set so a caller \
             token cannot survive when the shell's token is undefined"
        );
        assert!(
            delete_user < set_auth && delete_auth < set_auth,
            "credential deletes must run before the authToken injection"
        );
    }

    #[test]
    fn bridge_js_contains_origin_check() {
        assert!(
            SHELL_BRIDGE_JS.contains("LOCAL_API_ORIGIN"),
            "bridge JS must validate WebSocket origin"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("u.protocol !== 'ws:'"),
            "bridge JS must explicitly check WebSocket protocol"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("MAX_CONNECTIONS"),
            "bridge JS must limit concurrent connections"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("connections.delete(msg.id)"),
            "bridge JS must clean up connections"
        );
        // Shell message handler must validate types and restrict favicon schemes
        assert!(
            SHELL_BRIDGE_JS.contains("typeof msg.title === 'string'"),
            "bridge JS must type-check title before setting"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("typeof msg.href === 'string'"),
            "bridge JS must type-check favicon href before setting"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("scheme !== 'https' && scheme !== 'data'"),
            "bridge JS must restrict favicon href to https/data schemes"
        );
        // Hash forwarding: iframe→shell must validate # prefix and truncate
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'hash'"),
            "bridge JS must handle hash shell messages"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("h.charAt(0) === '#'"),
            "bridge JS must require # prefix on hash values"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("location.hash.slice(0, 8192)"),
            "bridge JS must truncate hash to 8192 chars"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState"),
            "bridge JS must use replaceState for hash updates to avoid polluting browser history"
        );
        // Initial hash: built into iframe src from data-src for deep linking
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.getAttribute('data-src')"),
            "bridge JS must read base URL from data-src attribute"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src = iframeSrc"),
            "bridge JS must set iframe src from data-src (single load, no race)"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("iframe.addEventListener('load'"),
            "bridge JS must NOT use load event (race with WASM init; hash is in iframe URL via data-src)"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("slice(0, 1024)"),
            "hash limit must be 8192, not 1024"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("popstate"),
            "bridge JS must forward hash on browser back/forward"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("hashchange"),
            "bridge JS must forward hash on manual URL fragment edits"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("if (location.hash)"),
            "bridge JS must not forward empty hash to iframe"
        );
        // Clipboard proxy: shell writes to clipboard on behalf of sandboxed iframe
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'clipboard'"),
            "bridge JS must handle clipboard shell messages"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("navigator.clipboard.writeText"),
            "bridge JS must proxy clipboard writes through the shell"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("msg.text.slice(0, 2048)"),
            "bridge JS must truncate clipboard text to 2048 chars"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("lastClipboard"),
            "bridge JS must rate-limit clipboard writes"
        );
        assert!(
            !SHELL_BRIDGE_JS.contains("clipboard.readText")
                && !SHELL_BRIDGE_JS.contains("clipboard.read("),
            "bridge JS must be clipboard write-only — no read access"
        );
    }

    #[test]
    fn shim_js_validates_message_source() {
        assert!(
            WEBSOCKET_SHIM_JS.contains("event.source !== window.parent"),
            "shim JS must validate message source"
        );
    }

    // Regression guard for the OOPIF zero-copy send() fix. wasm-bindgen hands
    // send() a Uint8Array, which is NOT `instanceof ArrayBuffer`; the pre-fix
    // code therefore left the postMessage transfer list empty and every
    // outbound WS frame was structured-clone COPIED across the process
    // boundary (a ~2.7 s main-thread CPU burst on tab-focus flush). The fix
    // transfers the backing buffer for ArrayBuffer *views* too, copying exactly
    // the view window off `data.buffer` first (works for TypedArrays AND a
    // DataView, which has no `.slice()`) so it never detaches WASM linear
    // memory. The behavioural coverage is in
    // tests/playwright/tests/websocket-shim.spec.ts (a real browser asserting
    // the actual transfer list); this content guard runs in the default CI job
    // and fails fast if the JS is reverted.
    #[test]
    fn shim_js_transfers_array_buffer_views_zero_copy() {
        // The old, buggy one-liner must be gone.
        assert!(
            !WEBSOCKET_SHIM_JS.contains("data instanceof ArrayBuffer ? [data] : []"),
            "shim send() must not use the copy-everything transfer check (OOPIF copy regression)"
        );
        // Views (Uint8Array / DataView) must be recognised and their buffer
        // transferred.
        assert!(
            WEBSOCKET_SHIM_JS.contains("ArrayBuffer.isView(data)"),
            "shim send() must transfer ArrayBuffer views zero-copy"
        );
        // The view window must be copied off data.buffer (NOT data.slice(),
        // which a DataView lacks) before transfer, so WASM linear memory is
        // never detached.
        assert!(
            WEBSOCKET_SHIM_JS.contains("data.buffer.slice("),
            "shim send() must copy the view window off data.buffer (handles DataView too)"
        );
        assert!(
            !WEBSOCKET_SHIM_JS.contains("data.slice()"),
            "shim send() must not call data.slice() (a DataView has no .slice())"
        );
        assert!(
            WEBSOCKET_SHIM_JS.contains("transfer = [buf]"),
            "shim send() must transfer the freshly copied buffer, not the shared/WASM one"
        );
    }

    #[test]
    fn get_path_v1() {
        let req_path = "/v1/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2() {
        let req_path = "/v2/contract/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html";
        let base_dir = PathBuf::from(
            "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/",
        );
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        let result = base_dir.join(parsed);
        assert_eq!(
            PathBuf::from(
                "/tmp/freenet/webapp_cache/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/state.html"
            ),
            result
        );
    }

    #[test]
    fn get_path_v2_web() {
        let req_path =
            "/v2/contract/web/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/assets/app.js";
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let parsed = get_file_path(uri).unwrap();
        assert_eq!(parsed, "assets/app.js");
    }

    #[test]
    fn get_file_path_rejects_unknown_version() {
        let req_path = "/v3/contract/web/somekey/assets/app.js";
        let uri: axum::http::Uri = req_path.parse().unwrap();
        let result = get_file_path(uri);
        assert!(result.is_err(), "expected error for /v3/ prefix");
    }

    #[test]
    fn bridge_js_contains_navigate_handler() {
        // The shell bridge must handle 'navigate' messages for multi-page
        // website navigation within the sandboxed iframe (issue #3833).
        assert!(
            SHELL_BRIDGE_JS.contains("msg.type === 'navigate'"),
            "bridge JS must handle navigate shell messages"
        );
        // Navigate handler must validate that target paths live inside the
        // contract namespace. The shape check is the security boundary —
        // it rejects /v1/node/..., /v1/delegate/..., and other gateway
        // endpoints as navigation targets.
        assert!(
            SHELL_BRIDGE_JS.contains("CONTRACT_PREFIX_RE"),
            "navigate handler must reference the contract-shape regex"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("cleanPath.match(CONTRACT_PREFIX_RE)"),
            "navigate handler must enforce contract-shape check on target path"
        );
        // Same-contract branch: must update iframe.src in place, not do a
        // top-level navigation (preserves auth token and client state).
        assert!(
            SHELL_BRIDGE_JS.contains("newContractPrefix === contractPrefix"),
            "same-contract branch must compare prefixes"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.searchParams.set('__sandbox', '1')"),
            "same-contract branch must add __sandbox=1 to navigated URL"
        );
        // Cross-contract branch: must do a top-level window.location.assign
        // so the gateway's contract_home regenerates a fresh shell + auth
        // token. Reusing the iframe with a different contract would leak
        // the old auth token and misattribute server-side requests
        // (Codex review P1).
        assert!(
            SHELL_BRIDGE_JS.contains("window.location.assign"),
            "cross-contract branch must use top-level navigation so the gateway \
             regenerates a fresh shell + auth token for the new contract"
        );
        // Cross-contract branch must preserve the query string so any
        // app-level routing arguments on the link survive the hop. Dropping
        // `resolved.search` previously stripped query parameters that the
        // destination webapp depended on.
        assert!(
            SHELL_BRIDGE_JS
                .contains("window.location.assign(cleanPath + resolved.search + cappedHash)"),
            "cross-contract branch must preserve the query string via resolved.search"
        );
        // Navigate handler must validate same-origin
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.origin !== location.origin"),
            "navigate handler must reject cross-origin navigation"
        );
        // Sandbox attributes themselves must not be widened — the fix is
        // scoped to the shell-side postMessage handler only.
        assert!(
            !SHELL_BRIDGE_JS.contains("allow-top-navigation"),
            "sandbox attributes must not be widened as part of the cross-contract nav fix"
        );
    }

    /// Decision returned by `navigate_shell_check` mirroring the JS handler.
    #[derive(Debug, PartialEq, Eq)]
    enum NavDecision {
        /// Same-contract hop: update iframe.src in place (keeps the shell).
        SameContract { new_prefix: String },
        /// Cross-contract hop: top-level window.location.assign reloads the
        /// shell with a fresh auth token via contract_home.
        CrossContract { new_prefix: String },
        /// Rejected — reason is only for test diagnostics.
        Reject(&'static str),
    }

    /// Pure-Rust mirror of the JS `navigate` postMessage handler's decision
    /// logic. Uses the `url` crate so WHATWG normalization (`..`, percent
    /// encoding, relative hrefs, protocol-relative URLs) matches what a
    /// browser would do inside `new URL(href, iframe.src)`.
    ///
    /// Returns the decision: accept as same-contract / accept as
    /// cross-contract / reject. Kept in sync with SHELL_BRIDGE_JS — any
    /// change to the JS regex or origin check must update both.
    fn navigate_shell_check(iframe_src: &str, current_prefix: &str, href: &str) -> NavDecision {
        use url::Url;

        if href.len() > 4096 {
            return NavDecision::Reject("href > 4096 bytes");
        }
        let base = match Url::parse(iframe_src) {
            Ok(u) => u,
            Err(_) => return NavDecision::Reject("iframe_src unparseable"),
        };
        let resolved = match base.join(href) {
            Ok(u) => u,
            Err(_) => return NavDecision::Reject("href unparseable"),
        };
        if resolved.origin() != base.origin() {
            return NavDecision::Reject("cross-origin");
        }
        let clean_path = resolved.path();
        let re = regex::Regex::new(r"^(/v[12]/contract/web/[^/]+/)").unwrap();
        let caps = match re.captures(clean_path) {
            Some(c) => c,
            None => return NavDecision::Reject("shape check failed"),
        };
        let new_prefix = caps.get(1).unwrap().as_str().to_string();
        if new_prefix == current_prefix {
            NavDecision::SameContract { new_prefix }
        } else {
            NavDecision::CrossContract { new_prefix }
        }
    }

    const IFRAME_SRC: &str = "http://127.0.0.1:50509/v1/contract/web/AAAA/?__sandbox=1";
    const CURRENT: &str = "/v1/contract/web/AAAA/";

    #[test]
    fn navigate_same_contract_subpage() {
        // Subpage inside the currently-loaded contract → same-contract hop.
        // The shell must NOT do a top-level navigation; it updates iframe.src
        // in place.
        let d = navigate_shell_check(
            IFRAME_SRC,
            CURRENT,
            "http://127.0.0.1:50509/v1/contract/web/AAAA/page2",
        );
        assert_eq!(
            d,
            NavDecision::SameContract {
                new_prefix: "/v1/contract/web/AAAA/".to_string()
            }
        );
    }

    #[test]
    fn navigate_cross_contract_hop() {
        // PRIMARY REGRESSION TEST for the Delta cross-contract-link report.
        // A link to a different contract must be ACCEPTED as a cross-contract
        // hop, which the shell handles via window.location.assign so the
        // gateway can regenerate a fresh auth token via contract_home.
        let d = navigate_shell_check(
            IFRAME_SRC,
            CURRENT,
            "http://127.0.0.1:50509/v1/contract/web/BBBB/welcome",
        );
        assert_eq!(
            d,
            NavDecision::CrossContract {
                new_prefix: "/v1/contract/web/BBBB/".to_string()
            }
        );
    }

    #[test]
    fn navigate_cross_contract_v2_api() {
        assert!(matches!(
            navigate_shell_check(
                IFRAME_SRC,
                CURRENT,
                "http://127.0.0.1:50509/v2/contract/web/CCCC/app"
            ),
            NavDecision::CrossContract { .. }
        ));
    }

    #[test]
    fn navigate_relative_same_contract() {
        // Relative href (most common real-world case for client-side
        // routing): `page2` resolves against iframe src → same-contract.
        assert!(matches!(
            navigate_shell_check(IFRAME_SRC, CURRENT, "page2"),
            NavDecision::SameContract { .. }
        ));
    }

    #[test]
    fn navigate_rejects_gateway_internal_path() {
        // The shape check is the security boundary. Navigation must not
        // become a ladder into non-contract gateway endpoints, including
        // via paths whose literal string matches contract shape but whose
        // WHATWG-normalized form escapes the namespace.
        for evil in [
            "http://127.0.0.1:50509/v1/node/status",
            "http://127.0.0.1:50509/v1/delegate/foo",
            "http://127.0.0.1:50509/api/secret",
            "http://127.0.0.1:50509/",
            "http://127.0.0.1:50509/v1/contract/AAAA/",
            "http://127.0.0.1:50509/v3/contract/web/AAAA/",
        ] {
            assert!(
                matches!(
                    navigate_shell_check(IFRAME_SRC, CURRENT, evil),
                    NavDecision::Reject(_)
                ),
                "non-contract path must be rejected: {evil}"
            );
        }
    }

    #[test]
    fn navigate_rejects_path_traversal() {
        // Path-traversal via `..` would break out of the contract namespace
        // post-normalization. `url::Url` resolves `..` the same way
        // browsers do via `new URL()`.
        for evil in [
            "http://127.0.0.1:50509/v1/contract/web/AAAA/../../node/status",
            "http://127.0.0.1:50509/v1/contract/web/AAAA/../../v1/node/status",
            // Relative variant resolved against IFRAME_SRC.
            "../../node/status",
        ] {
            let d = navigate_shell_check(IFRAME_SRC, CURRENT, evil);
            assert!(
                matches!(d, NavDecision::Reject(_)),
                "traversal must be rejected post-normalization: {evil} -> {d:?}"
            );
        }
    }

    #[test]
    fn navigate_rejects_cross_origin() {
        for evil in [
            "http://evil.example.com/v1/contract/web/AAAA/",
            "https://127.0.0.1:50509/v1/contract/web/AAAA/",
            // Protocol-relative resolves against IFRAME_SRC's scheme but
            // different host → cross-origin.
            "//evil.example.com/v1/contract/web/AAAA/",
        ] {
            assert!(
                matches!(
                    navigate_shell_check(IFRAME_SRC, CURRENT, evil),
                    NavDecision::Reject("cross-origin")
                ),
                "cross-origin must be rejected: {evil}"
            );
        }
    }

    #[test]
    fn navigate_rejects_non_http_schemes() {
        for evil in [
            "javascript:alert(1)",
            "data:text/html,<script>",
            "file:///etc/passwd",
        ] {
            let d = navigate_shell_check(IFRAME_SRC, CURRENT, evil);
            assert!(
                matches!(d, NavDecision::Reject(_)),
                "non-http scheme must be rejected: {evil} -> {d:?}"
            );
        }
    }

    #[test]
    fn navigate_rejects_oversized_href() {
        let huge = format!(
            "http://127.0.0.1:50509/v1/contract/web/AAAA/{}",
            "a".repeat(5000)
        );
        assert!(matches!(
            navigate_shell_check(IFRAME_SRC, CURRENT, &huge),
            NavDecision::Reject("href > 4096 bytes")
        ));
    }

    #[test]
    fn navigate_rejects_empty_contract_key_segment() {
        // `//foo` would leave the key segment empty; regex `[^/]+` rejects.
        assert!(matches!(
            navigate_shell_check(
                IFRAME_SRC,
                CURRENT,
                "http://127.0.0.1:50509/v1/contract/web//foo"
            ),
            NavDecision::Reject(_)
        ));
    }

    #[test]
    fn navigate_rejects_missing_trailing_slash() {
        // `/v1/contract/web/AAAA` without a trailing slash doesn't match the
        // shape regex. Pin this so a future regex tweak can't silently
        // loosen it.
        assert!(matches!(
            navigate_shell_check(
                IFRAME_SRC,
                CURRENT,
                "http://127.0.0.1:50509/v1/contract/web/AAAA"
            ),
            NavDecision::Reject(_)
        ));
    }

    #[test]
    fn navigation_interceptor_js_intercepts_clicks() {
        // The navigation interceptor must catch <a> clicks and route them
        // through postMessage for multi-page navigation (issue #3833).
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("document.addEventListener('click'"),
            "interceptor must listen for click events"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("type: 'navigate'"),
            "interceptor must send navigate messages to shell"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("__freenet_shell__: true"),
            "interceptor must use __freenet_shell__ namespace"
        );
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("e.preventDefault()"),
            "interceptor must prevent default link behavior"
        );
        // Cross-origin links should use open_url instead of navigate
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("type: 'open_url'"),
            "interceptor must route cross-origin links through open_url"
        );
        // Same-origin links: must respect explicit non-_self target so
        // webapps that open multiple tabs within their own contract still
        // work.
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.target"),
            "interceptor must respect target attribute on same-origin links"
        );
        // Must walk up DOM to handle clicks on child elements of <a>
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.parentElement"),
            "interceptor must walk up DOM to find <a> ancestor"
        );
    }

    /// Regression test for freenet/river#208.
    ///
    /// River (and any other webapp) transforms links to include
    /// `target="_blank"`. The original interceptor short-circuited on any
    /// anchor with an explicit target, so cross-origin clicks fell through
    /// to the browser. Without `allow-popups-to-escape-sandbox`, that
    /// produced a sandboxed popup with a null origin, which broke CORS on
    /// every external site (GitHub issues page reported by @lukors).
    ///
    /// Pin the contract: the cross-origin branch MUST be reached before
    /// the target-attribute check, i.e. the origin classification dominates.
    #[test]
    fn navigation_interceptor_handles_cross_origin_target_blank() {
        let js = NAVIGATION_INTERCEPTOR_JS;

        // Anchor the cross-origin check and the target-attribute check and
        // confirm the cross-origin check comes FIRST in the source order.
        let cross_origin_idx = js
            .find("target.origin !== location.origin")
            .expect("cross-origin check present");
        let target_attr_idx = js
            .find("target.target && target.target !== '_self'")
            .expect("target-attribute check present");
        assert!(
            cross_origin_idx < target_attr_idx,
            "cross-origin classification must run before the target-attribute \
             skip, otherwise target=\"_blank\" cross-origin links bypass the \
             open_url bridge (freenet/river#208). cross_origin_idx={cross_origin_idx}, \
             target_attr_idx={target_attr_idx}"
        );

        // The cross-origin branch must call preventDefault and send open_url,
        // not navigate.
        let cross_origin_block = &js[cross_origin_idx..target_attr_idx];
        assert!(
            cross_origin_block.contains("preventDefault"),
            "cross-origin branch must preventDefault before opening popup"
        );
        assert!(
            cross_origin_block.contains("type: 'open_url'"),
            "cross-origin branch must send open_url, not navigate"
        );
    }

    /// Regression test for freenet/freenet-core#3853.
    ///
    /// After #3852 fixed freenet/river#208, the cross-origin click handler
    /// unconditionally `preventDefault`ed and sent `open_url`. Middle-click,
    /// ctrl-click, shift-click and meta-click all collapsed to a single
    /// foreground tab because the interceptor dropped modifier state and
    /// the shell handler called `window.open` with no flags.
    ///
    /// A second latent bug: the listener was `click` only, but middle-click
    /// fires `auxclick` (not `click`), so middle-clicks on cross-origin
    /// links fell through to the browser's default handling and produced
    /// the same null-origin sandboxed popup #3852 was meant to prevent.
    ///
    /// We can only meaningfully preserve shift-click (via a popup window
    /// feature) because browsers refuse to honour background-tab placement
    /// when `window.open` is called outside a direct user gesture. Pin the
    /// minimal contract at both ends:
    ///   1. The interceptor registers BOTH `click` and `auxclick` so
    ///      middle-click is actually intercepted.
    ///   2. The interceptor's cross-origin branch forwards `shiftKey` in
    ///      the posted message, sourced from the MouseEvent.
    ///   3. The shell bridge's `open_url` handler reads `msg.shiftKey` and
    ///      uses the `popup` window feature when it's true.
    #[test]
    fn navigation_interceptor_forwards_shift_key_for_open_url() {
        let js = NAVIGATION_INTERCEPTOR_JS;

        let cross_origin_idx = js
            .find("type: 'open_url'")
            .expect("interceptor open_url branch present");
        let target_attr_idx = js
            .find("target.target && target.target !== '_self'")
            .expect("same-origin target check present");
        let block = &js[cross_origin_idx..target_attr_idx];

        assert!(
            block.contains("shiftKey"),
            "cross-origin open_url postMessage must include shiftKey to honour \
             shift-click as a new-window request (#3853); got block: {block}"
        );
        // Must be sourced from the actual event, not a hardcoded constant.
        assert!(
            block.contains("e.shiftKey"),
            "interceptor must forward `e.shiftKey` from the MouseEvent, not a literal (#3853)"
        );
    }

    /// Regression test for the middle-click half of #3853. Middle-click is
    /// dispatched as `auxclick` in modern browsers, NOT `click`, so the
    /// interceptor must listen on both events. Without the auxclick
    /// listener, middle-clicks on cross-origin `<a target="_blank">` links
    /// bypass the `open_url` routing and fall through to the browser's
    /// default handling, producing a null-origin sandboxed popup (exactly
    /// what #3852 was meant to prevent).
    #[test]
    fn navigation_interceptor_listens_on_click_and_auxclick() {
        let js = NAVIGATION_INTERCEPTOR_JS;
        assert!(
            js.contains("addEventListener('click'"),
            "interceptor must register a click listener"
        );
        assert!(
            js.contains("addEventListener('auxclick'"),
            "interceptor must register an auxclick listener so middle-click \
             on cross-origin links is also routed through open_url (#3853)"
        );
    }

    /// Regression test for freenet/freenet-core#4645.
    ///
    /// Anchor clicks are intercepted, but an app that calls `window.open()`
    /// from its own JS bypasses the click/auxclick listeners. In a sandboxed
    /// iframe (opaque origin, no `allow-popups-to-escape-sandbox`) that popup
    /// inherits the sandbox, gets a null origin, cannot read the per-user
    /// access key, and dead-ends on the "Open this app in a normal tab"
    /// per-user-isolation page — the exact symptom users hit when a hosted
    /// app opens a new tab. The interceptor must therefore override
    /// `window.open` and route http(s) targets through the shell's `open_url`
    /// bridge (real origin), returning null.
    ///
    /// Pin the contract so a future edit can't silently drop the override or
    /// regress the edge cases the review surfaced. Behavioral coverage lives in
    /// `crates/core/tests/playwright/tests/window-open.spec.ts` (runs in CI via
    /// playwright-shell.yml); these source pins are the cheap CI-required guard.
    ///   1. `window.open` is reassigned (the override exists).
    ///   2. The override forwards through the SAME `open_url` bridge as the
    ///      cross-origin anchor path, posting the RESOLVED ABSOLUTE url
    ///      (`resolved.href`) — not the raw arg, which would drop relative opens.
    ///   3. Targets are resolved against `document.baseURI` so the shell gets an
    ///      absolute URL.
    ///   4. Only http/https is forwarded; other schemes fall back to native.
    ///   5. `_self`/`_parent`/`_top` (in-place navigation) fall back to native.
    ///   6. Loopback targets fall back to native (open_url refuses them).
    ///   7. The arg is coerced to a string so URL objects are forwarded.
    #[test]
    fn navigation_interceptor_overrides_window_open() {
        let js = NAVIGATION_INTERCEPTOR_JS;
        assert!(
            js.contains("window.open = function"),
            "interceptor must override window.open so programmatic opens don't \
             create a sandbox-inherited null-origin popup (#4645)"
        );
        // The override is the final construct in the IIFE, so slicing to EOF
        // scopes assertions to it (nothing but the `})();` close follows).
        let open_fn_idx = js
            .find("window.open = function")
            .expect("window.open override present");
        let override_block = &js[open_fn_idx..];
        assert!(
            override_block.contains("type: 'open_url'"),
            "window.open override must forward through the open_url bridge (#4645)"
        );
        assert!(
            override_block.contains("__freenet_shell__: true"),
            "window.open override must use the __freenet_shell__ namespace (#4645)"
        );
        // Must post the RESOLVED absolute URL, not the raw (possibly relative)
        // arg — posting `url` raw would make open_url's `new URL(msg.url)` throw
        // on a relative target and silently drop the open.
        assert!(
            override_block.contains("url: resolved.href"),
            "window.open override must post the resolved absolute URL \
             (resolved.href), not the raw arg (#4645)"
        );
        // Resolve against the iframe base so relative targets become absolute.
        assert!(
            override_block.contains("document.baseURI"),
            "window.open override must resolve targets against document.baseURI \
             so the shell gets an absolute URL (#4645)"
        );
        // http(s)-only forward; everything else falls back to native open.
        assert!(
            override_block.contains("resolved.protocol !== 'http:'")
                && override_block.contains("resolved.protocol !== 'https:'"),
            "window.open override must only forward http(s); other schemes \
             fall back to native (#4645)"
        );
        // In-place navigation targets are not new-window requests -> native.
        // Names are normalized (case-insensitive) before the reserved check.
        assert!(
            override_block.contains("targetName === '_self'")
                && override_block.contains("String(name).toLowerCase()"),
            "window.open override must leave _self/_parent/_top (case-insensitive) \
             to native so in-place navigation isn't turned into a new tab (#4645)"
        );
        // Loopback targets must fall back to native: open_url refuses them, so
        // forwarding would silently drop the open on local nodes.
        assert!(
            override_block.contains("isLoopbackHost(resolved.hostname)"),
            "window.open override must fall back to native for loopback hosts \
             (open_url refuses them) so local-node opens aren't silently dropped (#4645)"
        );
        // Coerce the arg so window.open(new URL(...)) is forwarded, not sent to
        // native (which would recreate the dead end).
        assert!(
            override_block.contains("String(url)"),
            "window.open override must string-coerce the arg so URL objects are \
             forwarded rather than dead-ended (#4645)"
        );
        // Only the shell's DIRECT child forwards: a deeper descendant's parent
        // is an app frame the shell never hears, so it must stay native.
        assert!(
            override_block.contains("window.parent !== window.top"),
            "window.open override must only intercept the shell's direct child \
             (window.parent === window.top), else nested-frame opens are lost (#4645)"
        );
        // Non-forwarded cases delegate to the captured native window.open.
        assert!(
            override_block.contains("fallbackOpen"),
            "window.open override must fall back to native open for the \
             non-forwarded cases (#4645)"
        );
        // The forwarded case drops the WindowProxy (matches the shell's
        // noopener open) and asks for a plain tab (shiftKey false).
        assert!(
            override_block.contains("shiftKey: false"),
            "window.open override must request a plain tab (shiftKey false) (#4645)"
        );
        assert!(
            override_block.contains("return null;"),
            "window.open override must return null for the forwarded case (#4645)"
        );
    }

    /// Regression test for freenet/freenet-core#3853 shell-side.
    ///
    /// The shell `open_url` handler must read `msg.shiftKey` and, when true,
    /// call `window.open` with the `popup` window feature so Firefox honours
    /// the shift-click-opens-new-window intent. Other browsers may fall back
    /// to a tab, which is acceptable.
    #[test]
    fn shell_open_url_handler_honours_shift_key() {
        let js = SHELL_BRIDGE_JS;

        // Locate the open_url branch and bound the slice to the next
        // `else if` branch so assertions can't match unrelated JS.
        let open_url_idx = js
            .find("msg.type === 'open_url'")
            .expect("shell open_url branch present");
        let rest = &js[open_url_idx..];
        let next_branch = rest[1..]
            .find("} else if")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        let block = &rest[..next_branch];

        assert!(
            block.contains("msg.shiftKey"),
            "open_url handler must read msg.shiftKey for new-window intent (#3853)"
        );
        // The popup window feature is the concrete mechanism; pin it so a
        // future refactor that reads shiftKey but forgets the feature is
        // caught.
        assert!(
            block.contains("'noopener,noreferrer,popup'"),
            "open_url handler must pass the `popup` window feature on shift-click \
             so Firefox honours the new-window intent (#3853); got block: {block}"
        );
        // The non-shift path must still use the plain new-tab features so
        // left-click behaviour is unchanged.
        assert!(
            block.contains("'noopener,noreferrer'"),
            "open_url handler must keep the plain new-tab path for non-shift clicks"
        );
    }

    /// Regression test for freenet/river#231.
    ///
    /// The shell `open_url` handler must accept `http:` URLs in addition to
    /// `https:`. The original https-only check silently dropped clicks on
    /// markdown links to plain-HTTP services (e.g. the Network Telemetry
    /// dashboard `http://nova.locut.us:3133/` linked from the Freenet River
    /// channel header) — the user clicked the link and nothing happened, no
    /// console output, no popup, no error. The localhost block stays so a
    /// pasted `http://127.0.0.1:NNNN/` link can't be used to target services
    /// running on the reader's machine.
    #[test]
    fn shell_open_url_handler_accepts_http_and_https_but_blocks_localhost() {
        let js = SHELL_BRIDGE_JS;
        let open_url_idx = js
            .find("msg.type === 'open_url'")
            .expect("shell open_url branch present");
        let rest = &js[open_url_idx..];
        let next_branch = rest[1..]
            .find("} else if")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        let block = &rest[..next_branch];

        // Both schemes accepted. The check must reject ONLY non-http(s),
        // not just non-https.
        assert!(
            block.contains("u.protocol !== 'https:'") && block.contains("u.protocol !== 'http:'"),
            "open_url handler must accept both http: and https: schemes \
             (freenet/river#231); got block: {block}"
        );
        // The check must NOT be a bare https-only filter that drops http: URLs
        // before they reach the localhost block. Pin the precise structure so
        // a future "tighten security" refactor that re-introduces the
        // https-only filter trips this test.
        assert!(
            !block.contains("if (u.protocol !== 'https:') return;"),
            "open_url handler must NOT reject http: URLs outright; the bug \
             this test pins (freenet/river#231) was that an https-only filter \
             silently dropped clicks on http: links the user pasted. Got: {block}"
        );
        // Localhost block must still be present — http: + localhost is the
        // CSRF/private-network surface the original check was guarding against.
        assert!(
            block.contains("'localhost'") && block.contains("'127.0.0.1'"),
            "open_url handler must continue to block localhost/loopback hosts \
             so http: scheme acceptance doesn't open a CSRF surface against \
             services on the reader's machine; got block: {block}"
        );
    }

    /// WHATWG `URL.hostname` serializes an IPv6 literal WITH brackets, so
    /// `new URL('http://[::1]/').hostname === '[::1]'`. The handler must
    /// therefore STRIP the brackets before comparing against `::1`, or the
    /// loopback refusal never matches and a forged link to the viewer's IPv6
    /// loopback slips through. (An earlier version of this test and the code
    /// comment both had the fact inverted — asserting hostname is bracket-LESS —
    /// so the test passed while the IPv6 loopback was in fact unblocked. #4645.)
    #[test]
    fn shell_open_url_handler_blocks_ipv6_loopback() {
        let js = SHELL_BRIDGE_JS;
        let open_url_idx = js
            .find("msg.type === 'open_url'")
            .expect("shell open_url branch present");
        let rest = &js[open_url_idx..];
        let next_branch = rest[1..]
            .find("} else if")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        let block = &rest[..next_branch];

        // The handler must strip surrounding brackets from the hostname before
        // the loopback comparison, so the serialized `[::1]` matches `::1`.
        assert!(
            block.contains(r"replace(/^\[/"),
            "open_url handler must strip the leading bracket from an IPv6 \
             hostname before comparing, else `[::1]` never matches `::1` and \
             IPv6 loopback is unblocked (#4645); got block: {block}"
        );
        assert!(
            block.contains("'::1'"),
            "open_url handler must compare the bracket-stripped hostname \
             against `::1`; got block: {block}"
        );
    }

    /// Direct postMessages from a malicious iframe can synthesize an
    /// `open_url` payload without going through the upstream
    /// `NAVIGATION_INTERCEPTOR_JS` scheme filter, so the shell-side
    /// `new URL().protocol` allow-list is the primary gate against
    /// `javascript:` / `data:` / `file:` / `blob:` / `chrome:`. This
    /// test pins the explicit allow-list shape so a refactor that
    /// drops the explicit comparison (e.g. switches to a regex or a
    /// blocklist) is forced to handle these schemes consciously.
    #[test]
    fn shell_open_url_handler_rejects_dangerous_schemes() {
        let js = SHELL_BRIDGE_JS;
        let open_url_idx = js
            .find("msg.type === 'open_url'")
            .expect("shell open_url branch present");
        let rest = &js[open_url_idx..];
        let next_branch = rest[1..]
            .find("} else if")
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        let block = &rest[..next_branch];

        // The check must be an explicit allow-list of `http:` and `https:`.
        // `new URL('javascript:alert(1)').protocol === 'javascript:'`,
        // and `'javascript:' !== 'http:' && 'javascript:' !== 'https:'`,
        // so the explicit allow-list rejects it. Same for data:, blob:,
        // file:, chrome:, chrome-extension:, vbscript:.
        assert!(
            block.contains("u.protocol !== 'https:'")
                && block.contains("u.protocol !== 'http:'")
                && block.contains("&&"),
            "open_url handler must use an explicit `http:` AND `https:` \
             allow-list (joined with &&) so dangerous schemes \
             (javascript:, data:, file:, blob:, chrome:, vbscript:) \
             are rejected by the shell-side check, which is the \
             primary scheme gate (a malicious iframe can postMessage \
             open_url without going through the upstream interceptor); \
             got block: {block}"
        );
    }

    #[tokio::test]
    async fn sandbox_content_serves_sub_pages() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Create a sub-page
        let sub_html = r#"<!DOCTYPE html><html><head></head><body><h1>News</h1></body></html>"#;
        std::fs::write(dir.path().join("news.html"), sub_html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "news.html")
                .await
                .unwrap(),
        )
        .await;

        // Sub-page content must be served
        assert!(
            result.contains("<h1>News</h1>"),
            "sub-page content not served"
        );
        // WebSocket shim must be injected
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in sub-page"
        );
        // Navigation interceptor must be injected
        assert!(
            result.contains("type: 'navigate'"),
            "navigation interceptor not injected in sub-page"
        );
    }

    #[tokio::test]
    async fn sandbox_content_serves_directory_index() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        // Create a subdirectory with index.html
        std::fs::create_dir(dir.path().join("news")).unwrap();
        let sub_html =
            r#"<!DOCTYPE html><html><head></head><body><h1>News Index</h1></body></html>"#;
        std::fs::write(dir.path().join("news/index.html"), sub_html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "news")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("<h1>News Index</h1>"),
            "directory index.html not served"
        );
        assert!(
            result.contains("FreenetWebSocket"),
            "WebSocket shim not injected in directory index"
        );
    }

    #[tokio::test]
    async fn sandbox_content_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        std::fs::write(dir.path().join("index.html"), "<html></html>").unwrap();

        // Attempting to traverse above the contract directory must fail
        let result =
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "../../../etc/passwd").await;
        assert!(result.is_err(), "path traversal should be rejected");
    }

    #[tokio::test]
    async fn sandbox_content_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        std::fs::write(dir.path().join("index.html"), "<html></html>").unwrap();

        // Absolute paths would make Path::join replace the base directory entirely,
        // so they must be rejected by the component check.
        let result = sandbox_content_body(dir.path(), key, ApiVersion::V1, "/etc/passwd").await;
        assert!(result.is_err(), "absolute path should be rejected");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sandbox_content_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.html"), "<html>secret</html>").unwrap();

        // Create a symlink inside the contract directory pointing outside it.
        // The canonicalize + starts_with check must catch this even though the
        // component-level ParentDir check would not.
        std::os::unix::fs::symlink(
            outside.path().join("secret.html"),
            dir.path().join("escape.html"),
        )
        .unwrap();

        let result = sandbox_content_body(dir.path(), key, ApiVersion::V1, "escape.html").await;
        assert!(result.is_err(), "symlink escape should be rejected");
    }

    #[test]
    fn bridge_js_navigate_pushes_history_state() {
        // Regression test for #3839: in-contract navigation must push a browser
        // history entry so back/forward works and the address bar updates.
        assert!(
            SHELL_BRIDGE_JS.contains("history.pushState"),
            "navigate handler must push a history entry"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("__freenet_nav__: true"),
            "history state must be tagged with __freenet_nav__ so popstate can recognise it"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframePath: newIframePath"),
            "history state must carry the iframe sandbox URL for popstate restore"
        );
        // The pushState URL must be the clean path (without __sandbox=1) so the
        // address bar shows the user-visible subpage URL, not the sandbox flag.
        assert!(
            SHELL_BRIDGE_JS.contains("cleanPath + cappedHash"),
            "pushState URL must be the clean (non-sandbox) path"
        );
    }

    #[test]
    fn bridge_js_popstate_restores_iframe_from_state() {
        // Regression test for #3839: browser back/forward must restore the
        // iframe to the previously-visited subpage by reading history state.
        assert!(
            SHELL_BRIDGE_JS.contains("addEventListener('popstate'"),
            "bridge JS must listen for popstate events"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("state.__freenet_nav__ === true"),
            "popstate handler must check for the __freenet_nav__ marker"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("state.iframePath.indexOf(contractPrefix) === 0"),
            "popstate handler must validate the restored iframe path stays under the contract prefix"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src = state.iframePath"),
            "popstate handler must restore iframe.src from state"
        );
    }

    #[test]
    fn bridge_js_seeds_initial_history_state() {
        // Regression test for #3839: the initial history entry must carry the
        // __freenet_nav__ marker so that navigating back to the first page
        // still restores the iframe via popstate.
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState"),
            "bridge JS must seed history state on load"
        );
        // The replaceState call for hash forwarding must preserve existing
        // state (history.state) rather than passing null, or it would wipe the
        // __freenet_nav__ marker and break back-navigation.
        assert!(
            SHELL_BRIDGE_JS.contains("history.replaceState(history.state"),
            "hash replaceState must preserve the existing state object"
        );
    }

    #[test]
    fn bridge_js_navigate_caps_href_length() {
        // Prevent a malicious contract from bloating history.state / URL by
        // spamming arbitrarily large navigate hrefs.
        assert!(
            SHELL_BRIDGE_JS.contains("msg.href.length > 4096"),
            "navigate handler must cap msg.href length"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("resolved.hash.slice(0, 8192)"),
            "navigate handler must cap the hash component stored in history.state"
        );
    }

    #[test]
    fn bridge_js_hash_update_syncs_nav_state() {
        // When the iframe sends a hash update while sitting on a pushState
        // entry, the stored iframePath must be refreshed to include the new
        // fragment — otherwise back/forward loses the user's fragment.
        assert!(
            SHELL_BRIDGE_JS.contains("curState.__freenet_nav__ === true"),
            "hash handler must detect tagged nav state"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("basePath + h"),
            "hash handler must rewrite iframePath with the new fragment"
        );
    }

    #[test]
    fn bridge_js_popstate_skips_reload_when_iframe_on_target() {
        // bfcache restore can fire popstate while the iframe is already on
        // the target path. Re-assigning iframe.src would tear down live
        // WebSockets for no reason.
        assert!(
            SHELL_BRIDGE_JS.contains("iframe.src.indexOf(state.iframePath) === -1"),
            "popstate handler must skip reload when iframe is already on the target"
        );
    }

    #[test]
    fn bridge_js_cleans_up_websockets_on_navigate() {
        // When navigating to a new page, existing WebSocket connections must be
        // closed to prevent resource leaks from orphaned connections.
        assert!(
            SHELL_BRIDGE_JS.contains("connections.forEach"),
            "navigate handler must close existing WebSocket connections"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("connections.clear()"),
            "navigate handler must clear the connections map"
        );
    }
}
