//! Handle the `web` part of the bundles.
//!
//! Contract web apps are served inside sandboxed iframes to provide origin isolation.
//! The local API server returns a "shell" page that holds the auth token and
//! proxies WebSocket connections via postMessage, while the contract runs in an
//! `<iframe sandbox="allow-scripts allow-forms allow-popups allow-downloads allow-modals"
//!         allow="clipboard-read; clipboard-write">`
//! with an opaque origin that cannot access other contracts' data.
//! Popups inherit the sandbox (no `allow-popups-to-escape-sandbox`); external links
//! are opened via the `open_url` shell bridge message to avoid CORS issues. Sandbox content
//! is protected from top-level access via Sec-Fetch-Dest checks in client_api.rs.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};

use axum::response::{Html, IntoResponse};
use dashmap::DashMap;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, ContractResponse, ErrorKind, HostResponse},
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
    match shell_page(&assigned_token, &key, api_version, query_string, sub_path) {
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
        Err(_) => Err(WebSocketApiError::AxumError {
            error: ErrorKind::OperationError {
                cause: "GET request timed out after 30s".into(),
            },
        }),
        Ok(None) => Err(WebSocketApiError::AxumError {
            error: ErrorKind::OperationError {
                cause: "GET response channel closed (node may be shutting down)".into(),
            },
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
    let html = format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Freenet</title>
<link rel="icon" type="image/svg+xml" href="{favicon}">
<style>*{{margin:0;padding:0}}html,body{{width:100%;height:100%;overflow:hidden}}iframe{{width:100%;height:100%;border:none;display:block}}</style>
</head>
<body>
<iframe id="app" sandbox="allow-scripts allow-forms allow-popups allow-downloads allow-modals" allow="clipboard-read; clipboard-write" data-src="{iframe_src}"></iframe>
<script>
{SHELL_BRIDGE_JS}
</script>
<script>freenetBridge("{auth_token}");</script>
</body>
</html>"##
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
    // through the shell page's bridge. The interceptor catches <a> clicks and
    // routes them through postMessage for multi-page navigation.
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

/// JavaScript for the shell page's postMessage bridge.
///
/// The bridge listens for WebSocket requests from the sandboxed iframe,
/// creates real WebSocket connections with the auth token injected, and
/// forwards messages in both directions. Only allows connections to the
/// local API server itself (same origin) to prevent the contract from using the
/// bridge as an open proxy to other localhost services.
const SHELL_BRIDGE_JS: &str = r#"
function freenetBridge(authToken) {
  'use strict';
  var LOCAL_API_ORIGIN = location.origin;
  var MAX_CONNECTIONS = 32;
  var iframe = document.getElementById('app');
  var connections = new Map();
  var lastClipboard = 0;
  var lastDownload = 0;

  // Build iframe src from data-src, appending any URL hash for deep
  // linking. Using data-src (not src) in the HTML means the iframe
  // doesn't start loading until we set .src here, so there is exactly
  // one load -- with the hash already in the URL.
  var iframeDataSrc = iframe.getAttribute('data-src');
  // Cache the contract web prefix; used by nav/popstate path validation.
  // Cross-contract navigation updates this when it accepts a new path
  // so it always reflects the currently loaded contract (see navigate
  // handler below).
  var CONTRACT_PREFIX_RE = /^(\/v[12]\/contract\/web\/[^/]+\/)/;
  var contractPrefixMatch = iframeDataSrc.match(CONTRACT_PREFIX_RE);
  var contractPrefix = contractPrefixMatch ? contractPrefixMatch[1] : null;
  var iframeSrc = iframeDataSrc;
  if (location.hash) {
    iframeSrc += location.hash.slice(0, 8192);
  }
  iframe.src = iframeSrc;
  // Seed history state so that back-navigating to the initial entry still
  // has an identifiable __freenet_nav__ record. Using replaceState avoids
  // adding a new entry — we just tag the existing one.
  if (contractPrefix) {
    try {
      history.replaceState(
        { __freenet_nav__: true, iframePath: iframeSrc },
        ''
      );
    } catch(e) {}
  }

  function sendToIframe(msg) {
    iframe.contentWindow.postMessage(msg, '*');
  }

  window.addEventListener('message', function(event) {
    if (event.source !== iframe.contentWindow) return;
    var msg = event.data;
    if (!msg) return;

    // Handle shell-level messages (title, favicon) from iframe
    if (msg.__freenet_shell__) {
      if (msg.type === 'title' && typeof msg.title === 'string') {
        // Truncate to prevent UI spoofing with excessively long titles
        document.title = msg.title.slice(0, 128);
      } else if (msg.type === 'favicon' && typeof msg.href === 'string') {
        // Only allow https: and data: schemes to prevent exfiltration
        try {
          var scheme = msg.href.split(':')[0].toLowerCase();
          if (scheme !== 'https' && scheme !== 'data') return;
        } catch(e) { return; }
        var link = document.querySelector('link[rel="icon"]');
        if (link) link.href = msg.href;
      } else if (msg.type === 'hash' && typeof msg.hash === 'string') {
        // Only allow # fragments — reject anything that could modify path/query.
        // Note: replaceState (not pushState) is intentional — avoids polluting
        // browser history with every in-app route change. This also means
        // replaceState does NOT fire popstate or hashchange, preventing loops.
        var h = msg.hash.slice(0, 8192);
        if (h.length > 0 && h.charAt(0) === '#') {
          // Preserve the existing state object (which may carry our
          // __freenet_nav__ marker) so popstate can still restore the iframe.
          // If the current entry is tagged, also update its iframePath to
          // include the new fragment — otherwise back/forward would restore
          // the iframe without the user's current fragment position.
          var curState = history.state;
          if (curState && curState.__freenet_nav__ === true &&
              typeof curState.iframePath === 'string') {
            var basePath = curState.iframePath.split('#')[0];
            history.replaceState(
              { __freenet_nav__: true, iframePath: basePath + h },
              '',
              h
            );
          } else {
            history.replaceState(history.state, '', h);
          }
        }
      } else if (msg.type === 'clipboard' && typeof msg.text === 'string') {
        // Sandboxed iframes can't use navigator.clipboard due to permissions
        // policy. Proxy clipboard writes through the trusted shell instead.
        // Write-only — no readText proxy to prevent exfiltration.
        // Rate-limited to 1 write/sec to prevent clipboard spam from
        // malicious contracts. Requires transient user activation (browser
        // enforced) — works when the iframe sends this in a click handler.
        var now = Date.now();
        if (now - lastClipboard >= 1000) {
          lastClipboard = now;
          try { navigator.clipboard.writeText(msg.text.slice(0, 2048)); } catch(e) {}
        }
      } else if (msg.type === 'download' &&
                 typeof msg.filename === 'string' &&
                 typeof msg.base64 === 'string') {
        // Download proxy: contracts inside the sandboxed (null-origin)
        // iframe can't reliably trigger file downloads — `<a download>`
        // either silently fails (Firefox) or saves to an inaccessible
        // location (Chrome). The shell runs in the real origin and
        // can do it normally.
        //
        // Validation:
        //  - filename: stripped of path separators and leading dots,
        //    capped at 128 chars, no nulls
        //  - mimeType: only a small allowlist (data URLs from arbitrary
        //    types could be exploited by malicious contracts)
        //  - base64: capped at ~10 MiB raw bytes
        //  - rate-limit: 1 download per 2s, same reasoning as clipboard
        var now2 = Date.now();
        if (now2 - lastDownload < 2000) {
          console.warn('[freenet] download rate-limited (>1 per 2s)');
          return;
        }
        // Charge the rate-limit budget for *every* attempt (even rejected
        // ones) so a malicious iframe can't burn host CPU by spamming
        // invalid payloads at high frequency.
        lastDownload = now2;
        var rawName = msg.filename;
        if (rawName.indexOf('\0') !== -1) {
          console.warn('[freenet] download rejected: null byte in filename');
          return;
        }
        // Strip path components — keep only the basename. Normalise
        // both `/` and `\` so a malicious contract can't smuggle in a
        // backslash on POSIX.
        var slash = rawName.lastIndexOf('/');
        if (slash >= 0) rawName = rawName.slice(slash + 1);
        var bslash = rawName.lastIndexOf('\\');
        if (bslash >= 0) rawName = rawName.slice(bslash + 1);
        // Strip leading dots so a contract can't write a dotfile.
        while (rawName.charAt(0) === '.') rawName = rawName.slice(1);
        rawName = rawName.slice(0, 128);
        if (rawName.length === 0) {
          console.warn('[freenet] download rejected: empty filename after sanitisation');
          return;
        }
        var mime = typeof msg.mimeType === 'string' ? msg.mimeType : 'application/octet-stream';
        var ALLOWED_MIME = {
          'application/json': 1,
          'application/octet-stream': 1,
          'text/plain': 1,
          'text/csv': 1
        };
        // Disallowed MIMEs are downgraded to octet-stream rather than
        // rejected, so callers always get *some* download — but log it
        // so the contract author can fix the mismatch.
        if (!ALLOWED_MIME[mime]) {
          console.warn('[freenet] download MIME ' + mime + ' downgraded to application/octet-stream');
          mime = 'application/octet-stream';
        }
        // base64 max length ≈ 4/3 * raw size; 10 MiB raw → ~13.4 MiB b64.
        // Round up to 14 MiB for a small safety margin.
        if (msg.base64.length > 14 * 1024 * 1024) {
          console.warn('[freenet] download rejected: payload exceeds 14 MiB base64 cap');
          return;
        }
        var raw;
        try {
          raw = atob(msg.base64);
        } catch (e) {
          console.warn('[freenet] download rejected: base64 decode failed');
          return;
        }
        var len = raw.length;
        var bytes = new Uint8Array(len);
        for (var i = 0; i < len; i++) bytes[i] = raw.charCodeAt(i) & 0xff;
        var blob;
        try { blob = new Blob([bytes], { type: mime }); } catch (e) {
          console.warn('[freenet] download rejected: Blob construction failed');
          return;
        }
        var url;
        try { url = URL.createObjectURL(blob); } catch (e) {
          console.warn('[freenet] download rejected: createObjectURL failed');
          return;
        }
        var a = document.createElement('a');
        a.href = url;
        a.download = rawName;
        a.style.display = 'none';
        document.body.appendChild(a);
        try { a.click(); } catch (e) {}
        document.body.removeChild(a);
        // Defer revoke so the browser has time to start the download.
        setTimeout(function() { try { URL.revokeObjectURL(url); } catch (e) {} }, 60000);
      } else if (msg.type === 'navigate' && typeof msg.href === 'string') {
        // Navigation from the sandboxed iframe. The iframe cannot navigate
        // the top window itself, so it postMessages the shell, which does
        // one of two things:
        //
        //   1. SAME-CONTRACT hop (subpage inside the current contract's
        //      webapp): update iframe.src in place. This preserves the
        //      running shell, auth token, and in-memory state — matching
        //      what a multi-page webapp expects for client-side routing.
        //
        //   2. CROSS-CONTRACT hop (link to a different Freenet contract):
        //      fall through to a top-level window.location.assign. The
        //      gateway serves a fresh shell via `contract_home` for the
        //      new contract, which generates a new auth token and origin
        //      attribution. Reusing the current iframe for a different
        //      contract would keep the old auth token bound to the
        //      original contract, so the server would misattribute every
        //      subsequent delegate/API request (see PR review: Codex P1).
        //
        // This is the fix for the "Delta cannot link to other Freenet
        // contracts without forcing a new tab" report: cross-contract
        // links now navigate in place via a full shell reload, instead of
        // being silently dropped.
        //
        // Security posture:
        // - Same-origin only (rejects cross-site). The sandbox still
        //   blocks contract JS from reading gateway cookies or same-origin
        //   state.
        // - Target path must match the contract-webapp shape
        //   /v[12]/contract/web/{key}/... . This rejects /v1/node/...,
        //   /v1/delegate/..., or any other gateway endpoint as a
        //   navigation target.
        // - Sandbox iframe attributes are NOT widened. The shell remains
        //   the sole code with top-level navigation authority.
        // - Cross-contract navigation via window.location.assign is the
        //   same privilege level as a user middle-clicking a link today
        //   (target="_blank" + allow-popups already escapes the sandbox
        //   and can reach any Freenet contract). The difference is that
        //   the destination now loads in the same tab instead of a new
        //   one.
        //
        // Cap href length to prevent a malicious contract from bloating
        // history.state or the address bar with arbitrarily large URLs.
        if (msg.href.length > 4096) return;
        try {
          var resolved = new URL(msg.href, iframe.src);
          // Same-origin only.
          if (resolved.origin !== location.origin) return;
          var cleanPath = resolved.pathname;
          // Contract-webapp shape check. This is the security boundary
          // that prevents the handler from being used to navigate to
          // gateway internals (/v1/node/..., /v1/delegate/...) or to
          // non-contract paths in general. The contract-key segment is
          // validated server-side in the freshly-loaded shell path via
          // ContractInstanceId::from_bytes, so we only need a loose
          // shape check here — a bogus key still produces a 4xx from the
          // gateway, not a silent bypass.
          var newPrefixMatch = cleanPath.match(CONTRACT_PREFIX_RE);
          if (!newPrefixMatch) return;
          var newContractPrefix = newPrefixMatch[1];
          // Cap the hash component to match the 8192-byte cap used by
          // the hash-forwarding path; the iframe path is stored in
          // history.state so unbounded hashes would bloat the per-tab
          // history record.
          var cappedHash = resolved.hash ? resolved.hash.slice(0, 8192) : '';

          if (newContractPrefix === contractPrefix) {
            // SAME-CONTRACT: update iframe.src in place. This preserves
            // the running shell, auth token, and client-side state.
            //
            // Close any open WebSocket connections from the previous
            // page to prevent resource leaks. The old iframe document
            // will be destroyed when src changes, orphaning any
            // connection callbacks.
            connections.forEach(function(ws) { try { ws.close(); } catch(e) {} });
            connections.clear();
            // Build new sandbox URL preserving __sandbox=1
            resolved.searchParams.set('__sandbox', '1');
            var newIframePath = resolved.pathname + resolved.search + cappedHash;
            iframe.src = newIframePath;
            // Push a history entry so back/forward navigate between
            // visited subpages, and update the address bar to the
            // non-sandbox URL. The sandbox flag is intentionally omitted
            // from the outer URL; the shell always re-adds it when
            // loading the iframe. See issue #3839.
            try {
              history.pushState(
                { __freenet_nav__: true, iframePath: newIframePath },
                '',
                cleanPath + cappedHash
              );
            } catch(e) {}
          } else {
            // CROSS-CONTRACT: top-level navigation. The gateway's
            // contract_home handler re-runs and generates a fresh auth
            // token + origin attribution for the destination contract.
            // The browser's normal back/forward history takes care of
            // cross-contract restoration — no popstate handling needed.
            //
            // Include `resolved.search` so any query parameters the link
            // carries (e.g. app-level routing args) survive the hop. The
            // destination shell page strips the sensitive routing params
            // (`__sandbox`, `authToken`) before forwarding the rest into
            // the iframe's `location.search`. The gateway's subpage
            // handler redirects non-root HTML loads to the shell route
            // (see `web_subpages` `Sec-Fetch-Dest` handling), which
            // preserves the filtered query string all the way through,
            // so `/v1/contract/web/{key}/page2?invite=…` still lands on
            // a shell that issues an auth token and forwards `invite`
            // into the iframe.
            try {
              window.location.assign(cleanPath + resolved.search + cappedHash);
            } catch(e) {}
          }
        } catch(e) {}
      } else if (msg.type === 'open_url' && typeof msg.url === 'string') {
        // Open external URLs in a new tab. Popups from the sandboxed iframe
        // inherit the opaque origin, breaking CORS on target sites. The shell
        // opens the URL instead, giving proper origin. See issue #1499.
        //
        // Security model: this scheme allow-list is the PRIMARY gate, not
        // defence in depth. A malicious contract iframe can postMessage
        // `open_url` directly without going through the upstream
        // navigation interceptor, so the URL parser + scheme check below
        // is what blocks `javascript:` / `data:` / `file:` etc.
        //
        // Both http and https are accepted because user-pasted markdown
        // links commonly target plain-HTTP self-hosted services (e.g.
        // nova.locut.us:3133, the Freenet network telemetry dashboard,
        // no TLS configured). Auth tokens never travel through this path
        // — the only operation is `window.open(url, '_blank',
        // 'noopener,noreferrer')` — so HTTP doesn't expose credentials.
        // See freenet/river#231.
        //
        // Private networks (RFC1918 192.168/16, 10/8, 172.16-31/12 and
        // RFC4193 fc00::/7, link-local fe80::/10) are deliberately NOT
        // blocked. A user who pastes a link to their home router or NAS
        // expects the link to work; the threat model here is that a
        // *malicious contract* might forge a markdown link to a LAN
        // admin panel and trick the user into clicking, which is a
        // social-engineering attack class we accept.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'https:' && u.protocol !== 'http:') return;
          // URL.hostname strips brackets from IPv6 literals, so a URL
          // `http://[::1]/` parses with hostname `::1`, NOT `[::1]`.
          // Compare against the bracket-less form.
          var h = u.hostname.toLowerCase();
          if (h === 'localhost' || h === '127.0.0.1' || h === '::1' || h === '0.0.0.0') return;
          // Honour shift-click by requesting a popup-style window feature
          // (freenet/freenet-core#3853). Firefox honours this as "open in
          // a new window"; other browsers may still open a tab, which is
          // an acceptable fallback. ctrl / meta / middle-click cannot be
          // preserved from a postMessage handler because browsers only
          // honour background-tab placement when window.open is called
          // from a direct user gesture, so we route those through the
          // same default-tab path as plain left-click.
          if (msg.shiftKey === true) {
            window.open(u.href, '_blank', 'noopener,noreferrer,popup');
          } else {
            window.open(u.href, '_blank', 'noopener,noreferrer');
          }
        } catch(e) {}
      }
      return;
    }

    if (!msg.__freenet_ws__) return;

    switch (msg.type) {
      case 'open': {
        // Limit concurrent connections to prevent resource exhaustion
        if (connections.size >= MAX_CONNECTIONS) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Security: only allow WebSocket connections to the local API server itself.
        // Validate protocol explicitly and compare origin.
        try {
          var u = new URL(msg.url);
          if (u.protocol !== 'ws:' && u.protocol !== 'wss:') {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
          var httpProto = u.protocol === 'wss:' ? 'https:' : 'http:';
          if (httpProto + '//' + u.host !== LOCAL_API_ORIGIN) {
            sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
            return;
          }
        } catch(e) {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          return;
        }
        // Inject auth token into the WebSocket URL
        u.searchParams.set('authToken', authToken);
        var ws = new WebSocket(u.toString(), msg.protocols || undefined);
        ws.binaryType = 'arraybuffer';
        connections.set(msg.id, ws);

        ws.onopen = function() {
          sendToIframe({ __freenet_ws__: true, type: 'open', id: msg.id });
        };
        ws.onmessage = function(e) {
          var transfer = e.data instanceof ArrayBuffer ? [e.data] : [];
          iframe.contentWindow.postMessage({
            __freenet_ws__: true, type: 'message', id: msg.id, data: e.data
          }, '*', transfer);
        };
        ws.onclose = function(e) {
          sendToIframe({
            __freenet_ws__: true, type: 'close', id: msg.id,
            code: e.code, reason: e.reason
          });
          connections.delete(msg.id);
        };
        ws.onerror = function() {
          sendToIframe({ __freenet_ws__: true, type: 'error', id: msg.id });
          connections.delete(msg.id);
        };
        break;
      }
      case 'send': {
        var ws = connections.get(msg.id);
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(msg.data);
        }
        break;
      }
      case 'close': {
        var ws = connections.get(msg.id);
        if (ws) {
          ws.close(msg.code, msg.reason);
          connections.delete(msg.id);
        }
        break;
      }
    }
  });

  // Forward runtime hash changes (browser back/forward, manual URL edits)
  function forwardHash() {
    if (location.hash) {
      sendToIframe({ __freenet_shell__: true, type: 'hash', hash: location.hash.slice(0, 8192) });
    }
  }
  // popstate fires when the user presses back/forward. If the popped entry
  // carries our __freenet_nav__ marker, restore the iframe to the matching
  // subpage. Otherwise, fall back to forwarding the hash. See issue #3839.
  window.addEventListener('popstate', function(ev) {
    var state = ev.state;
    if (state && state.__freenet_nav__ === true && typeof state.iframePath === 'string') {
      // Security: path must still live under this contract's web prefix.
      // A stale state object from a different contract must not be able to
      // redirect the iframe elsewhere.
      if (contractPrefix && state.iframePath.indexOf(contractPrefix) === 0) {
        // No-op if the iframe is already on the target path (e.g. popstate
        // fired from a bfcache restore where iframe state was retained).
        // This avoids a spurious reload that would tear down live WebSocket
        // connections unnecessarily.
        if (iframe.src.indexOf(state.iframePath) === -1) {
          connections.forEach(function(ws) { try { ws.close(); } catch(e) {} });
          connections.clear();
          iframe.src = state.iframePath;
        }
        return;
      }
    }
    forwardHash();
  });
  window.addEventListener('hashchange', forwardHash);

  // Permission prompt overlay: render a modal in the shell page's DOM
  // (outside the sandboxed iframe) whenever a delegate permission prompt
  // is pending. The shell is trusted and same-origin with the gateway, so
  // the sandboxed contract cannot reach into this DOM. See issue #3836.
  //
  // Every open Freenet tab subscribes to /permission/events (Server-Sent
  // Events) and renders the overlay as soon as the gateway pushes an
  // `prompt_added` event. When the user responds in one tab, the gateway
  // emits `prompt_removed` and every tab dismisses its card. This was
  // previously a 3-second polling loop with a visibility-skip optimisation
  // that caused the originating tab to silently miss prompts whenever it
  // wasn't foregrounded; SSE eliminates both the polling-floor latency and
  // the visibility race.
  var overlayRoot = null;
  var overlayCards = {}; // nonce -> card element
  var OVERLAY_CSS =
    '#__freenet_perm_overlay{position:fixed;inset:0;z-index:2147483647;' +
    'background:rgba(8,10,14,0.62);backdrop-filter:blur(4px);' +
    '-webkit-backdrop-filter:blur(4px);display:none;align-items:center;' +
    'justify-content:center;padding:20px;overflow:auto;' +
    'font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;}' +
    '#__freenet_perm_overlay .fn-card{--bg:#0f1419;--fg:#e6e8eb;--card:#1a2028;' +
    '--accent:#3b82f6;--border:#2d3748;--warn:#f59e0b;--muted:#9ca3af;' +
    'background:var(--card);color:var(--fg);border:1px solid var(--border);' +
    'border-radius:14px;padding:28px;max-width:520px;width:100%;margin:12px 0;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.5);box-sizing:border-box;}' +
    '@media (prefers-color-scheme: light){#__freenet_perm_overlay .fn-card{' +
    '--bg:#f5f5f5;--fg:#1a1a1a;--card:#ffffff;--accent:#2563eb;' +
    '--border:#d1d5db;--warn:#d97706;--muted:#6b7280;' +
    'box-shadow:0 12px 40px rgba(0,0,0,0.18);}}' +
    '#__freenet_perm_overlay .fn-header{display:flex;align-items:center;gap:12px;' +
    'margin-bottom:18px;}' +
    '#__freenet_perm_overlay .fn-icon{font-size:28px;line-height:1;}' +
    '#__freenet_perm_overlay .fn-title{font-size:18px;font-weight:600;margin:0;' +
    'color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-msg-label{font-size:11px;color:var(--muted);' +
    'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;}' +
    '#__freenet_perm_overlay .fn-msg{font-size:15px;line-height:1.5;margin:0 0 22px 0;' +
    'padding:14px 16px;background:var(--bg);border-left:3px solid var(--warn);' +
    'border-radius:4px;white-space:pre-wrap;word-wrap:break-word;color:var(--fg);}' +
    '#__freenet_perm_overlay .fn-msg-pre{font-family:ui-monospace,SFMono-Regular,' +
    'Menlo,Monaco,Consolas,monospace;font-size:12px;line-height:1.45;' +
    'max-height:300px;overflow:auto;}' +
    '#__freenet_perm_overlay .fn-btns{display:flex;gap:10px;flex-wrap:wrap;}' +
    '#__freenet_perm_overlay .fn-btn{padding:10px 20px;border-radius:8px;' +
    'font-size:14px;cursor:pointer;flex:1;min-width:100px;font-weight:500;' +
    'border:1px solid var(--border);background:var(--card);color:var(--fg);' +
    'transition:transform 0.12s, opacity 0.12s, filter 0.12s;font-family:inherit;}' +
    '#__freenet_perm_overlay .fn-btn.primary{background:var(--accent);' +
    'color:#fff;border-color:var(--accent);}' +
    '#__freenet_perm_overlay .fn-btn:hover:not(:disabled){transform:translateY(-1px);' +
    'filter:brightness(1.08);}' +
    '#__freenet_perm_overlay .fn-btn:disabled{opacity:0.55;cursor:not-allowed;}' +
    '#__freenet_perm_overlay .fn-delegate-line{font-size:12px;color:var(--muted);' +
    'margin-top:10px;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;}' +
    '#__freenet_perm_overlay .fn-delegate-line .hash{user-select:all;}' +
    '#__freenet_perm_overlay .fn-tech{margin-top:10px;font-size:12px;color:var(--muted);}' +
    '#__freenet_perm_overlay .fn-tech summary{cursor:pointer;user-select:none;}' +
    '#__freenet_perm_overlay .fn-tech dl{margin:8px 0 0 16px;}' +
    '#__freenet_perm_overlay .fn-tech dt{font-weight:600;color:var(--fg);margin-top:6px;}' +
    '#__freenet_perm_overlay .fn-tech dd{margin:2px 0 0 0;font-family:ui-monospace,' +
    'SFMono-Regular,Menlo,Consolas,monospace;word-break:break-all;user-select:all;}' +
    '#__freenet_perm_overlay .fn-timer{margin-top:14px;font-size:12px;' +
    'color:var(--muted);text-align:center;}';
  // Auto-deny duration in seconds, mirroring the standalone /permission/{nonce}
  // fallback page. Tracked client-side only; the server enforces the real
  // timeout and will clear the nonce regardless.
  var OVERLAY_AUTO_DENY_SECONDS = 60;
  function ensureOverlayRoot() {
    if (overlayRoot) return overlayRoot;
    var style = document.createElement('style');
    style.textContent = OVERLAY_CSS;
    document.head.appendChild(style);
    overlayRoot = document.createElement('div');
    overlayRoot.id = '__freenet_perm_overlay';
    overlayRoot.setAttribute('role', 'dialog');
    overlayRoot.setAttribute('aria-modal', 'true');
    overlayRoot.setAttribute('aria-label', 'Delegate permission request');
    document.body.appendChild(overlayRoot);
    // Escape-to-dismiss: routes to the last button in the most-recently-added
    // card, which (by the standard delegate convention Allow Once / Always
    // Allow / Deny) is the Deny button. If the delegate supplied a single
    // label this is a no-op — Escape just does nothing.
    document.addEventListener('keydown', function(e) {
      if (e.key !== 'Escape') return;
      if (!overlayRoot || overlayRoot.style.display === 'none') return;
      var nonces = Object.keys(overlayCards);
      if (nonces.length === 0) return;
      var nonce = nonces[nonces.length - 1];
      var card = overlayCards[nonce];
      var btns = card.querySelectorAll('button');
      if (btns.length < 2) return; // no non-primary option, ignore
      btns[btns.length - 1].click();
      e.preventDefault();
    });
    return overlayRoot;
  }
  function setText(el, text) {
    // textContent avoids any HTML interpretation of delegate-controlled
    // strings. Delegate-provided fields are never parsed as markup.
    el.textContent = text == null ? '' : String(text);
  }
  // Truncate a hash for display: first8…last5. Mirrors truncate_hash() in
  // crates/core/src/server/client_api/permission_prompts.rs so the overlay
  // and the standalone /permission/{nonce} fallback page render identically.
  // Handles multi-byte unicode by iterating Array.from(...) which gives
  // codepoints, not UTF-16 code units.
  function truncateHash(s) {
    if (typeof s !== 'string' || s.length === 0) return '';
    var chars = Array.from(s);
    if (chars.length <= 14) return s;
    return chars.slice(0, 8).join('') + '\u2026' + chars.slice(chars.length - 5).join('');
  }
  // Render the Caller row from the tagged caller object. Forward-compatible:
  // an unknown `kind` (e.g. a future "delegate" variant from issue #3860)
  // falls through to a neutral "Unknown caller" so the overlay does NOT
  // pretend to render an identity it doesn't understand.
  function formatCaller(caller) {
    if (!caller || typeof caller !== 'object') {
      return { display: 'No app caller', full: '' };
    }
    if (caller.kind === 'webapp' && typeof caller.hash === 'string') {
      return {
        display: 'Freenet app ' + truncateHash(caller.hash),
        full: caller.hash,
      };
    }
    if (caller.kind === 'none') {
      return { display: 'No app caller', full: '' };
    }
    return { display: 'Unknown caller', full: '' };
  }
  function createCard(p) {
    var card = document.createElement('div');
    card.className = 'fn-card';
    card.setAttribute('data-nonce', p.nonce);

    var header = document.createElement('div');
    header.className = 'fn-header';
    var icon = document.createElement('span');
    icon.className = 'fn-icon';
    icon.textContent = '\u{1F512}';
    var title = document.createElement('h1');
    title.className = 'fn-title';
    title.textContent = 'Permission Request';
    header.appendChild(icon);
    header.appendChild(title);
    card.appendChild(header);

    // "Delegate says:" authorship label is non-negotiable: a malicious
    // delegate would otherwise be able to write text like "Freenet verified
    // this request" with no way for the user to tell who authored it. The
    // text below the label is delegate-controlled; the label tells the user
    // that. See the trust-model rationale in permission_prompts.rs.
    var msgLabel = document.createElement('div');
    msgLabel.className = 'fn-msg-label';
    msgLabel.textContent = 'Delegate says:';
    card.appendChild(msgLabel);
    // Try to render the delegate-supplied message as pretty-printed JSON
    // when it parses as JSON. Falls back to a plain paragraph for plain
    // text. The pretty form makes structured token requests legible
    // (#190) — users routinely see one-line blobs like
    //   {"token":{"max_age":"31536000 seconds","tier":"Min10"},...}
    // and have to mentally parse them to make a security decision.
    //
    // Security: still rendered via textContent (setText), so no HTML
    // interpretation. Long values are wrapped via CSS (white-space:
    // pre-wrap on .fn-msg-pre). Render is best-effort: any parse error
    // falls back to the original raw string in a <p>.
    var rawMsg = p.message || 'A delegate is requesting permission.';
    var pretty = null;
    if (typeof rawMsg === 'string' && rawMsg.length > 0) {
      var trimmed = rawMsg.trim();
      if (trimmed.length <= 64 * 1024 &&
          (trimmed.charAt(0) === '{' || trimmed.charAt(0) === '[')) {
        try {
          var parsed = JSON.parse(trimmed);
          pretty = JSON.stringify(parsed, null, 2);
          // Cap rendered output at 16 KiB after pretty-printing so a
          // hostile delegate can't force a multi-MiB layout pass.
          if (pretty.length > 16 * 1024) {
            pretty = pretty.slice(0, 16 * 1024) + '\n…';
          }
        } catch (e) {
          pretty = null;
        }
      }
    }
    if (pretty !== null) {
      var msgPre = document.createElement('pre');
      msgPre.className = 'fn-msg fn-msg-pre';
      setText(msgPre, pretty);
      card.appendChild(msgPre);
    } else {
      var msg = document.createElement('p');
      msg.className = 'fn-msg';
      setText(msg, rawMsg);
      card.appendChild(msg);
    }

    var buttons = document.createElement('div');
    buttons.className = 'fn-btns';
    var labels = Array.isArray(p.labels) && p.labels.length > 0 ? p.labels : ['OK'];
    labels.forEach(function(label, idx) {
      var b = document.createElement('button');
      b.className = 'fn-btn' + (idx === 0 ? ' primary' : '');
      setText(b, label);
      b.addEventListener('click', function() {
        respondToPrompt(p.nonce, idx, card);
      });
      buttons.appendChild(b);
    });
    card.appendChild(buttons);

    // Inline truncated delegate hash, always visible. Gives the user a
    // passive anomaly signal: a returning user who recognises their
    // delegate's fingerprint can spot an impostor without expanding the
    // Technical details disclosure. Full hash is in the Technical details
    // pane below and copyable via user-select: all on .hash.
    var delegateLine = document.createElement('div');
    delegateLine.className = 'fn-delegate-line';
    var delegateLabel = document.createElement('span');
    delegateLabel.textContent = 'Delegate: ';
    delegateLine.appendChild(delegateLabel);
    var delegateHashSpan = document.createElement('span');
    delegateHashSpan.className = 'hash';
    var delegateFull = typeof p.delegate_key === 'string' ? p.delegate_key : '';
    setText(delegateHashSpan, truncateHash(delegateFull) || '(none)');
    if (delegateFull) {
      delegateHashSpan.setAttribute('title', delegateFull);
    }
    delegateLine.appendChild(delegateHashSpan);
    card.appendChild(delegateLine);

    // Technical details disclosure. Holds the full delegate hash and the
    // Caller row. Closed by default — the user's decision is timing/intent
    // ("did I just trigger this?"), not hash matching. Power users hover or
    // copy via user-select: all to audit the unabbreviated value.
    var details = document.createElement('details');
    details.className = 'fn-tech';
    var summary = document.createElement('summary');
    summary.textContent = 'Technical details';
    details.appendChild(summary);
    var dl = document.createElement('dl');
    var dtDelegate = document.createElement('dt');
    dtDelegate.textContent = 'Delegate';
    var ddDelegate = document.createElement('dd');
    setText(ddDelegate, delegateFull || '(none)');
    if (delegateFull) {
      ddDelegate.setAttribute('title', delegateFull);
    }
    var dtCaller = document.createElement('dt');
    dtCaller.textContent = 'Caller';
    var ddCaller = document.createElement('dd');
    var callerRendered = formatCaller(p.caller);
    setText(ddCaller, callerRendered.display);
    if (callerRendered.full) {
      ddCaller.setAttribute('title', callerRendered.full);
    }
    dl.appendChild(dtDelegate);
    dl.appendChild(ddDelegate);
    dl.appendChild(dtCaller);
    dl.appendChild(ddCaller);
    details.appendChild(dl);
    card.appendChild(details);

    // Countdown mirroring the standalone permission page. The real timeout
    // lives server-side; this is a hint for the user that the prompt won't
    // wait forever. On expiry the next poll drops the card via the
    // reconciliation path, so we don't need a local hide here.
    var timer = document.createElement('div');
    timer.className = 'fn-timer';
    var remaining = OVERLAY_AUTO_DENY_SECONDS;
    timer.textContent = 'Auto-deny in ' + remaining + 's';
    card._fnTimerId = setInterval(function() {
      remaining -= 1;
      if (remaining <= 0) {
        clearInterval(card._fnTimerId);
        timer.textContent = 'Auto-denied';
        return;
      }
      timer.textContent = 'Auto-deny in ' + remaining + 's';
    }, 1000);
    card.appendChild(timer);
    return card;
  }
  function showCard(nonce, card) {
    var root = ensureOverlayRoot();
    root.appendChild(card);
    root.style.display = 'flex';
    overlayCards[nonce] = card;
    // Move keyboard focus to the primary button so Enter/Space answer the
    // prompt without requiring a mouse click.
    var primary = card.querySelector('.fn-btn.primary');
    if (primary && typeof primary.focus === 'function') {
      try { primary.focus(); } catch (e) {}
    }
  }
  function hideCard(nonce) {
    var card = overlayCards[nonce];
    if (!card) return;
    if (card._fnTimerId) {
      clearInterval(card._fnTimerId);
      card._fnTimerId = null;
    }
    if (card.parentNode) card.parentNode.removeChild(card);
    delete overlayCards[nonce];
    if (overlayRoot && Object.keys(overlayCards).length === 0) {
      overlayRoot.style.display = 'none';
    }
  }
  function respondToPrompt(nonce, index, card) {
    var btns = card.querySelectorAll('button');
    btns.forEach(function(b) { b.disabled = true; b.style.opacity = '0.5'; });
    fetch('/permission/' + encodeURIComponent(nonce) + '/respond', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ index: index })
    }).then(function(r) {
      // 404 means another tab already answered (or it auto-denied) — hide
      // the overlay here as well so the user isn't staring at a dead button.
      if (r.ok || r.status === 404) {
        hideCard(nonce);
      } else {
        btns.forEach(function(b) { b.disabled = false; b.style.opacity = '1'; });
      }
    }).catch(function() {
      btns.forEach(function(b) { b.disabled = false; b.style.opacity = '1'; });
    });
  }
  // Snapshot the current pending list and reconcile against the open
  // overlay cards. Used for initial bootstrap, on `resync` events when an
  // SSE subscriber lagged, and as a fallback while the EventSource is
  // reconnecting.
  function reconcileFromPending() {
    fetch('/permission/pending').then(function(r) { return r.json(); }).then(function(prompts) {
      if (!Array.isArray(prompts)) return;
      var seen = {};
      prompts.forEach(function(p) {
        if (!p || typeof p.nonce !== 'string') return;
        seen[p.nonce] = true;
        if (overlayCards[p.nonce]) return;
        showCard(p.nonce, createCard(p));
      });
      Object.keys(overlayCards).forEach(function(nonce) {
        if (!seen[nonce]) hideCard(nonce);
      });
    }).catch(function() {});
  }

  // Open a Server-Sent Events connection so prompts appear with no polling
  // delay and on every open Freenet tab regardless of foreground/background
  // state. The browser's EventSource auto-reconnects with exponential
  // backoff if the connection drops; on each reconnect we re-bootstrap from
  // /permission/pending so we don't miss anything during the gap.
  //
  // While the EventSource is in the disconnected state (its `error` event
  // has fired and `readyState !== 1`), we run a 3-second polling fallback
  // against /permission/pending so a tab whose stream fails (gateway
  // restart, connection-cap rejection, transient network) still receives
  // prompt updates. The fallback shuts off as soon as the stream re-opens.
  var fallbackPollHandle = null;
  function startFallbackPoll() {
    if (fallbackPollHandle !== null) return;
    fallbackPollHandle = setInterval(reconcileFromPending, 3000);
    reconcileFromPending();
  }
  function stopFallbackPoll() {
    if (fallbackPollHandle === null) return;
    clearInterval(fallbackPollHandle);
    fallbackPollHandle = null;
  }
  if (typeof EventSource !== 'undefined') {
    var es = new EventSource('/permission/events');
    es.addEventListener('prompt_added', function(e) {
      try {
        var p = JSON.parse(e.data);
        if (!p || typeof p.nonce !== 'string') return;
        if (overlayCards[p.nonce]) return;
        showCard(p.nonce, createCard(p));
      } catch (err) {}
    });
    es.addEventListener('prompt_removed', function(e) {
      try {
        var p = JSON.parse(e.data);
        if (!p || typeof p.nonce !== 'string') return;
        hideCard(p.nonce);
      } catch (err) {}
    });
    // The server emits `resync` when its broadcast channel laps a slow
    // subscriber. Reconcile from the polling endpoint instead of clearing
    // first: the reconcile path's diff already adds new cards and hides
    // ones that disappeared, with no flicker on cards that survive.
    es.addEventListener('resync', reconcileFromPending);
    // EventSource fires `open` on initial connect AND on every reconnect.
    // Reconcile each time so a transient disconnect doesn't leave us out
    // of date, and stop the fallback poll if it had taken over.
    es.addEventListener('open', function() {
      stopFallbackPoll();
      reconcileFromPending();
    });
    // `error` fires on connect failure, transient drops, and when the
    // server caps us out. Switch to polling until the EventSource
    // re-opens; the browser auto-reconnects in the background.
    es.addEventListener('error', startFallbackPoll);
    // Initial bootstrap so we're populated before the SSE handshake
    // completes (avoids a brief empty state on slow connections).
    reconcileFromPending();
  } else {
    // EventSource missing in some embedded webviews -- fall back to the
    // legacy 3-second poll so users on those clients still see prompts.
    startFallbackPoll();
  }
}
"#;

/// JavaScript WebSocket shim injected into the sandboxed iframe content.
///
/// Overrides `window.WebSocket` so that `web_sys::WebSocket::new()` (which
/// compiles to `new WebSocket(url)` via wasm-bindgen, resolving from global
/// scope at call time) is intercepted and routed through postMessage to the
/// shell page's bridge.
const WEBSOCKET_SHIM_JS: &str = r#"
(function() {
  'use strict';
  var wsInstances = new Map();
  var idCounter = 0;

  function FreenetWebSocket(url, protocols) {
    this._id = '__fws_' + (++idCounter);
    this.url = url;
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.extensions = '';
    this.protocol = '';
    this.binaryType = 'blob';
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    this._listeners = {};
    wsInstances.set(this._id, this);
    window.parent.postMessage({
      __freenet_ws__: true, type: 'open', id: this._id,
      url: url, protocols: protocols
    }, '*');
  }
  FreenetWebSocket.CONNECTING = 0;
  FreenetWebSocket.OPEN = 1;
  FreenetWebSocket.CLOSING = 2;
  FreenetWebSocket.CLOSED = 3;
  FreenetWebSocket.prototype.send = function(data) {
    if (this.readyState !== 1) throw new DOMException('WebSocket is not open', 'InvalidStateError');
    var transfer = data instanceof ArrayBuffer ? [data] : [];
    window.parent.postMessage({
      __freenet_ws__: true, type: 'send', id: this._id, data: data
    }, '*', transfer);
  };
  FreenetWebSocket.prototype.close = function(code, reason) {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    window.parent.postMessage({
      __freenet_ws__: true, type: 'close', id: this._id, code: code, reason: reason
    }, '*');
  };
  FreenetWebSocket.prototype.addEventListener = function(type, listener) {
    if (!this._listeners[type]) this._listeners[type] = [];
    this._listeners[type].push(listener);
  };
  FreenetWebSocket.prototype.removeEventListener = function(type, listener) {
    if (!this._listeners[type]) return;
    this._listeners[type] = this._listeners[type].filter(function(l) { return l !== listener; });
  };
  FreenetWebSocket.prototype.dispatchEvent = function(event) {
    var handler = this['on' + event.type];
    if (handler) handler.call(this, event);
    var listeners = this._listeners[event.type];
    if (listeners) for (var i = 0; i < listeners.length; i++) listeners[i].call(this, event);
    return true;
  };

  window.addEventListener('message', function(event) {
    // Only accept messages from the parent shell page
    if (event.source !== window.parent) return;
    var msg = event.data;
    if (!msg || !msg.__freenet_ws__) return;
    var ws = wsInstances.get(msg.id);
    if (!ws) return;
    switch (msg.type) {
      case 'open':
        ws.readyState = 1;
        ws.dispatchEvent(new Event('open'));
        break;
      case 'message':
        var data = msg.data;
        if (ws.binaryType === 'blob' && data instanceof ArrayBuffer) data = new Blob([data]);
        ws.dispatchEvent(new MessageEvent('message', { data: data }));
        break;
      case 'close':
        ws.readyState = 3;
        ws.dispatchEvent(new CloseEvent('close', { code: msg.code, reason: msg.reason, wasClean: true }));
        wsInstances.delete(msg.id);
        break;
      case 'error':
        ws.dispatchEvent(new Event('error'));
        break;
    }
  });

  window.WebSocket = FreenetWebSocket;
  if (typeof globalThis !== 'undefined') globalThis.WebSocket = FreenetWebSocket;
})();
"#;

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
const NAVIGATION_INTERCEPTOR_JS: &str = r#"
(function() {
  'use strict';
  // Shared handler for both `click` (primary button) and `auxclick`
  // (non-primary, i.e. middle-click). Middle-click is dispatched via
  // `auxclick` in modern browsers and does NOT fire `click` at all, so
  // without a separate `auxclick` listener middle-clicks on cross-origin
  // <a target="_blank"> links bypass the interceptor entirely and the
  // browser opens a null-origin sandboxed popup (freenet/freenet-core#3853
  // follow-up from #3852).
  function handleAnchorClick(e) {
    var target = e.target;
    // Walk up to find the nearest <a> element (handles clicks on child elements)
    while (target && target.tagName !== 'A') target = target.parentElement;
    if (!target || !target.href) return;
    // Skip javascript: and mailto: links
    var protocol = target.protocol;
    if (protocol && protocol !== 'http:' && protocol !== 'https:') return;
    // Skip links with download attribute
    if (target.hasAttribute('download')) return;
    // Skip links explicitly marked to bypass interception
    if (target.dataset && target.dataset.freenetNoIntercept) return;
    // Classify by origin. Cross-origin always goes through the open_url
    // bridge, regardless of the `target` attribute, because a sandboxed
    // popup would have a null origin and break CORS on the destination
    // (freenet/river#208).
    //
    // Fail-safe default: if the origin comparison throws (pathological URLs
    // that slipped past the protocol check above) we assume cross-origin,
    // because the failure mode we are guarding against is a null-origin
    // sandboxed popup, not an accidental in-contract navigation.
    var isCrossOrigin = true;
    try {
      isCrossOrigin = target.origin !== location.origin;
    } catch(err) {}
    if (isCrossOrigin) {
      e.preventDefault();
      // Forward shift-key state so the shell can honour shift-click
      // as a new-window request (freenet/freenet-core#3853). ctrl /
      // meta / middle-click intent can't be meaningfully preserved
      // from a postMessage handler: browsers only allow background-
      // tab placement when window.open is called directly from a
      // user gesture, and all three collapse to a plain new tab
      // regardless of what we forward. Keep the contract minimal.
      window.parent.postMessage({
        __freenet_shell__: true,
        type: 'open_url',
        url: target.href,
        shiftKey: !!e.shiftKey
      }, '*');
      return;
    }
    // Same-origin link. Respect explicit non-_self targets so webapps
    // that open multiple tabs within their own contract still work.
    if (target.target && target.target !== '_self') return;
    // Same-origin in-contract link: request navigation via shell
    e.preventDefault();
    window.parent.postMessage({
      __freenet_shell__: true, type: 'navigate', href: target.href
    }, '*');
  }
  document.addEventListener('click', handleAnchorClick, true);
  // Catch middle-click and other non-primary button activations.
  document.addEventListener('auxclick', handleAnchorClick, true);
})();
"#;

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
    /// timeout) surfaces as an `AxumError(OperationError)`, not a panic or
    /// hang.  `WebSocketApiError::into_response` maps this to a 503 with
    /// `<meta http-equiv="refresh">` — see #3472.
    #[tokio::test]
    async fn handle_get_response_maps_timeout_to_operation_error() {
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
                    error: ErrorKind::OperationError { .. }
                })
            ),
            "30s timeout must map to OperationError (for retry page), got: {result:?}"
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None, None).unwrap())
                .await;
        assert!(
            html.contains("allow-downloads"),
            "iframe sandbox missing `allow-downloads` — user-initiated \
             file downloads from sandboxed webapps will be silently blocked \
             by the browser. Got HTML:\n{html}"
        );
    }

    #[tokio::test]
    async fn shell_page_contains_iframe_and_bridge() {
        let token = AuthToken::generate();
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None, None).unwrap())
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None, None).unwrap())
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
        // innerHTML must not appear anywhere in the overlay code path. The
        // boundary marker below was previously `setInterval(checkPermissions`
        // but that polling loop is gone; we now bound the slice at the
        // EventSource-fallback `setInterval(reconcileFromPending`.
        let overlay_start = html.find("__freenet_perm_overlay").unwrap();
        let overlay_end = html[overlay_start..]
            .find("setInterval(reconcileFromPending")
            .or_else(|| html[overlay_start..].find("EventSource"))
            .expect("overlay slice must end at the SSE setup or fallback poll");
        let overlay_slice = &html[overlay_start..overlay_start + overlay_end];
        assert!(
            !overlay_slice.contains("innerHTML"),
            "overlay code path must not use innerHTML (XSS surface)"
        );

        // The old Notification flow must be gone: no requestPermission(),
        // no new Notification(...), no window.open('/permission/').
        assert!(
            !html.contains("Notification.requestPermission"),
            "browser Notification permission request must be removed (#3836)"
        );
        assert!(
            !html.contains("new Notification("),
            "browser Notification construction must be removed (#3836)"
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None, None).unwrap())
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs, None).unwrap())
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
            shell_page(&token, "testkey123", ApiVersion::V1, None, Some("news/")).unwrap(),
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, None, None).unwrap())
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
                contract_home(key, sender, token, ApiVersion::V1, None, Some("news/"))
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs, None).unwrap())
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs, None).unwrap())
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
        let html =
            response_body(shell_page(&token, "testkey123", ApiVersion::V1, qs, None).unwrap())
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

    /// `URL.hostname` strips the brackets from IPv6 literals, so a URL
    /// `http://[::1]/` parses with `hostname === '::1'` (no brackets), and
    /// the previous comparison `h === '[::1]'` never matched. The IPv6
    /// loopback was effectively unblocked. The relaxation in
    /// freenet/river#231 (accepting http: in addition to https:) makes
    /// the gap newly reachable for typical home services, so fix it
    /// alongside the http: change.
    #[test]
    fn shell_open_url_handler_blocks_ipv6_loopback_without_brackets() {
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

        // The block list must compare against `::1` (no brackets), the
        // form `URL.hostname` actually returns. The bracketed form is a
        // dead arm.
        assert!(
            block.contains("'::1'"),
            "open_url handler must block the IPv6 loopback hostname `::1` \
             (no brackets — URL.hostname strips them); got block: {block}"
        );
        assert!(
            !block.contains("'[::1]'"),
            "open_url handler must NOT compare against `[::1]` with \
             brackets — that arm is dead because URL.hostname strips \
             brackets from IPv6 literals; got block: {block}"
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
