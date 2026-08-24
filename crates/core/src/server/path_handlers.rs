//! Handle the `web` part of the bundles.
//!
//! Contract web apps are served inside sandboxed iframes to provide origin isolation.
//! The local API server returns a "shell" page that holds the auth token and
//! proxies WebSocket connections via postMessage, while the contract runs in an
//! `<iframe sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox
//!                   allow-downloads allow-modals"
//!         allow="clipboard-read; clipboard-write">`
//! with an opaque origin that cannot access other contracts' data.
//! Popups ESCAPE the sandbox: a new tab opened from the iframe is a normal
//! top-level document at the node's real origin, which re-wraps the target
//! contract in a fresh shell + sandboxed frame. That is what makes
//! `target="_blank"` work identically in every browser — the tab is opened by a
//! real user gesture in the frame that received the click, not by the shell from
//! a `message` handler (which Firefox's popup blocker rejects outright, since
//! `message` is not in `dom.popup_allowed_events`). See the head comment in
//! `navigation_interceptor.js`.
//! Sandbox content is protected from top-level access via Sec-Fetch-Dest checks in client_api.rs.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::{Duration, SystemTime},
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
use tokio::{
    fs::File,
    io::AsyncReadExt,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};

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
///
/// Unlike the two maps above, this one is keyed by an instance id an
/// UNAUTHENTICATED caller supplies in the URL, and the entry is created
/// *before* any gate has decided whether the contract is worth fetching. So it
/// is the one per-key map here an attacker can grow at will, and it is capped
/// at [`MAX_REFRESH_LOCKS`] — see `acquire_refresh_lock` and the
/// per-key-collection rule in `.claude/rules/code-style.md`.
static CONTRACT_REFRESH_LOCKS: LazyLock<
    DashMap<ContractInstanceId, Arc<tokio::sync::Mutex<RefreshState>>>,
> = LazyLock::new(DashMap::new);

/// What a contract's refresh lock guards, beyond the decision itself.
///
/// Kept inside the mutex rather than in a map of its own because the mutex is
/// already the thing that serializes refreshers for one contract, and a second
/// map keyed by a contract id from the URL would be another unbounded per-key
/// collection to bound (`.claude/rules/code-style.md`).
#[derive(Default, Debug)]
struct RefreshState {
    /// When a COLD fetch last failed, and for which contract.
    ///
    /// Without this, every subresource on a page pointing at a contract nobody
    /// can find pays its own full network GET, one after another, because each
    /// queued follower re-checks the cache, still finds it cold, and tries
    /// again: a page with 30 such images spends 30 sequential fetches getting
    /// 30 identical answers. Recording the failure lets the followers stop.
    ///
    /// The id is carried because [`REFRESH_LOCK_OVERFLOW`] stripes are shared
    /// between contracts, so a stripe's state may describe a different one.
    last_cold_failure: Option<(ContractInstanceId, Instant)>,
}

impl RefreshState {
    /// Whether a cold fetch for `instance_id` failed recently enough that
    /// trying again now would just repeat it.
    fn cold_fetch_failed_recently(&self, instance_id: &ContractInstanceId) -> bool {
        self.last_cold_failure
            .is_some_and(|(id, at)| id == *instance_id && at.elapsed() < CONTRACT_CACHE_REFRESH_TTL)
    }
}

/// Cap on retained entries in [`CONTRACT_REFRESH_LOCKS`].
///
/// Generous next to the number of web contracts a node realistically serves,
/// so the prune below effectively never runs in normal operation; it exists so
/// a spray of never-seen keys cannot grow the map without limit.
const MAX_REFRESH_LOCKS: usize = 4096;

/// Shared mutexes used once [`CONTRACT_REFRESH_LOCKS`] is full — see
/// [`refresh_lock_for`].
///
/// A contract maps to a stripe by hash, so two requests for the SAME contract
/// still land on the same mutex and still coalesce. Unrelated contracts sharing
/// a stripe serialize their refresh decisions, which is the price of the
/// overflow state and is why there are enough stripes to make it rare.
const REFRESH_LOCK_OVERFLOW_STRIPES: usize = 64;

static REFRESH_LOCK_OVERFLOW: LazyLock<Vec<Arc<tokio::sync::Mutex<RefreshState>>>> =
    LazyLock::new(|| {
        (0..REFRESH_LOCK_OVERFLOW_STRIPES)
            .map(|_| Arc::new(tokio::sync::Mutex::new(RefreshState::default())))
            .collect()
    });

/// Serializes ADMISSION of new entries to [`CONTRACT_REFRESH_LOCKS`], so
/// [`MAX_REFRESH_LOCKS`] is enforced at insertion rather than approached from
/// both sides at once.
///
/// A plain `len()` check before `insert` is not the cap it looks like: two
/// callers for distinct keys can both read `len() == MAX - 1` and both insert.
/// The overshoot is small, but the per-key-collection rule in
/// `.claude/rules/code-style.md` asks for a maximum enforced at insertion time,
/// and "usually about 4096" is not that. Lookups of an EXISTING lock never take
/// this, so the cost falls only on first sight of a contract, and nothing
/// awaits while holding it.
static REFRESH_LOCK_ADMISSION: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The mutex that coalesces refresh decisions for `instance_id`, keeping
/// [`CONTRACT_REFRESH_LOCKS`] at or under [`MAX_REFRESH_LOCKS`] entries.
///
/// When the map is full, entries no other task holds are dropped first. The
/// `Arc::strong_count == 1` test is what makes that safe: a count of one means
/// the map is the only owner, so no task is waiting on or holding that mutex
/// and re-creating it later cannot break mutual exclusion. `retain` takes each
/// shard's write lock while it runs, so a task racing to clone an entry either
/// gets it first (count 2, retained) or blocks and then creates a fresh one.
///
/// If nothing can be pruned — every one of 4096 contracts refreshing at
/// once — the caller gets a stripe from [`REFRESH_LOCK_OVERFLOW`] rather than a
/// private mutex. A private mutex would silently drop coalescing exactly when
/// the node is busiest, and a warm-but-stale refresh takes no speculative-fetch
/// permit, so nothing else would bound the duplicate GETs that follow.
///
/// Returns the `Arc` rather than the guard so no DashMap reference is alive
/// when the caller awaits the mutex. Holding one across that await would pin a
/// shard's read lock for the length of a network GET, blocking every insert and
/// the prune sweep itself on contracts that merely hash to the same shard.
fn refresh_lock_for(instance_id: &ContractInstanceId) -> Arc<tokio::sync::Mutex<RefreshState>> {
    if let Some(existing) = CONTRACT_REFRESH_LOCKS.get(instance_id).map(|e| e.clone()) {
        return existing;
    }

    let _admission = REFRESH_LOCK_ADMISSION.lock();
    // Re-check: another admission may have inserted this key while we queued.
    if let Some(existing) = CONTRACT_REFRESH_LOCKS.get(instance_id).map(|e| e.clone()) {
        return existing;
    }

    if CONTRACT_REFRESH_LOCKS.len() >= MAX_REFRESH_LOCKS {
        CONTRACT_REFRESH_LOCKS.retain(|_, lock| Arc::strong_count(lock) > 1);
    }
    if CONTRACT_REFRESH_LOCKS.len() >= MAX_REFRESH_LOCKS {
        tracing::debug!(
            "webapp cache: refresh-lock table full; {instance_id} shares an overflow stripe"
        );
        return overflow_refresh_lock(instance_id);
    }

    let mutex = Arc::new(tokio::sync::Mutex::new(RefreshState::default()));
    CONTRACT_REFRESH_LOCKS.insert(*instance_id, mutex.clone());
    mutex
}

/// Deterministic overflow stripe for `instance_id` — the same contract always
/// gets the same one, which is what preserves coalescing when the lock table is
/// full.
fn overflow_refresh_lock(
    instance_id: &ContractInstanceId,
) -> Arc<tokio::sync::Mutex<RefreshState>> {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(instance_id.as_bytes());
    REFRESH_LOCK_OVERFLOW[hasher.finish() as usize % REFRESH_LOCK_OVERFLOW_STRIPES].clone()
}

async fn acquire_refresh_lock(
    instance_id: &ContractInstanceId,
) -> tokio::sync::OwnedMutexGuard<RefreshState> {
    refresh_lock_for(instance_id).lock_owned().await
}

/// How many *speculative* webapp fetches a node will have in flight at once.
///
/// A speculative fetch is a cold-cache network GET for a contract this node has
/// no local trace of — the shape an attacker gets by spraying random keys at
/// `/v{1,2}/contract/web/<KEY>/<path>` (#3945), and equally the shape a
/// legitimate cross-contract `<img src>` takes the first time anyone on this
/// node loads it (#3940). The two are indistinguishable at the HTTP layer, so
/// the bound is on CONCURRENCY rather than on who is asking: real subresource
/// loads are a handful of distinct contracts and never approach it, while a
/// spray saturates it and every further key is refused without touching the
/// network.
///
/// 32 is well above what a page load needs (a page pulls subresources from one
/// or two contracts) and well below what would make the fan-out interesting to
/// an attacker.
///
/// Be precise about what it bounds: CONCURRENT speculative GETs, not their
/// rate. A GET for a key nobody has ends when its retry loop is exhausted,
/// which is usually well before the 30s timeout ceiling, so the sustained rate
/// is 32 divided by however long a failing GET actually takes. What bounds the
/// rate for a REPEATED key is `RefreshState::last_cold_failure`, which stops
/// the same dead contract being re-fetched inside the TTL window; a sprayer
/// using fresh keys every time gets no benefit from that and is bounded only by
/// the concurrency.
const SPECULATIVE_FETCH_LIMIT: usize = 32;

/// How long a cold request queues for a permit before giving up on the lane.
///
/// `tokio`'s semaphore hands out permits FIFO, so queueing briefly is what
/// stops a client that holds `SPECULATIVE_FETCH_LIMIT` requests open from
/// starving everyone else — without it, a saturated lane means every other
/// caller falls back to the presence query and a legitimate first-ever
/// subresource load fails for exactly the reason #5406 was filed about. The
/// wait is short because a queued request holds nothing but a task: a caller
/// that gives up still has the presence-query fallback and, failing that, a
/// fast 404.
const SPECULATIVE_FETCH_WAIT: Duration = Duration::from_secs(2);

/// Take `CONTRACT_CACHE_LOCKS[instance_id]` without waiting. `None` means an
/// unpack for that contract is in flight, which is exactly when the eviction
/// sweep must leave the entry alone.
fn try_acquire_cache_lock(
    instance_id: &ContractInstanceId,
) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    let mutex = CONTRACT_CACHE_LOCKS
        .entry(*instance_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    mutex.try_lock_owned().ok()
}

// =============================================================================
// Webapp cache size bound (LRU)
// =============================================================================

/// Total on-disk size the extracted webapp cache may occupy before the
/// least-recently-used entries are evicted.
///
/// The cache is a pure, recomputable artifact — an unpacked web archive that
/// `unpack_if_stale` re-extracts whenever the contract state hash changes — so
/// a miss costs one re-unpack of state the node already has, and nothing else
/// in the node depends on an entry existing. Until this bound existed the
/// directory had per-contract staleness replacement but no global limit, so it
/// grew by one entry for every webapp the user ever opened and never shrank
/// (measured: 325 MB / 61 entries on one peer, 1.2 GB / 82 entries on another,
/// with entries up to six months untouched).
///
/// 64 MiB is chosen against the observed per-entry distribution: a typical
/// unpacked webapp is a few hundred KB to a few MB, so the budget keeps roughly
/// the last 15-30 distinct webapps the user actually browsed — far more than a
/// browsing session touches — while cutting >90% of the observed footprint.
/// These bytes are additionally invisible to the node's disk accounting: the
/// cache lives under the XDG *cache* dir, whereas `ring/hosting/disk_usage.rs`
/// only walks `contracts_dir` + `wasmtime_cache_dir`, so nothing else bounds it.
const WEBAPP_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// How long an entry is protected from eviction after this process last served
/// from it.
///
/// This is the in-flight-request guard: a request records an access before it
/// touches the cache, so a sweep running concurrently skips the entry instead of
/// competing with the request for it. It must therefore comfortably exceed the
/// 30s network fetch timeout in `ensure_contract_cached` (a request may spend
/// that long merely waiting on the node before reading the unpacked files).
///
/// The guard is a *strong preference*, not an interlock: the check and the
/// `remove_dir_all` are not atomic, and the record is per-process while the
/// directory is per-user, so an eviction racing a request remains possible in
/// principle (see [`enforce_webapp_cache_budget`]). What that costs is bounded:
/// on Unix an already-opened file survives unlinking, and `ServeFile` opens the
/// descriptor before streaming, so a slow download cannot be truncated
/// mid-flight; the worst case is a request that has not opened the file yet
/// falling back to a 404 or a refetch of a cache that is recomputable anyway.
///
/// Being time-bounded is load-bearing (see the cleanup-exemption rule in
/// AGENTS.md): the exemption always expires, so no entry can become permanently
/// un-evictable by being touched once.
const WEBAPP_CACHE_EVICTION_MIN_IDLE: Duration = Duration::from_secs(120);

/// How often an entry's on-disk last-used marker (the `{key}.hash` mtime) is
/// refreshed while it is being served.
///
/// Serving a webapp fans out many subresource requests, so refreshing the mtime
/// on every one would add a filesystem timestamp update per request for no
/// benefit. Throttling to one refresh per contract per 5 minutes keeps the
/// on-disk LRU signal accurate to within 5 minutes, which is far finer than the
/// horizon eviction actually discriminates on (hours to months).
const WEBAPP_CACHE_ACCESS_TOUCH_INTERVAL: Duration = Duration::from_secs(300);

/// Most entries one sweep will delete before giving up and leaving the rest to
/// the next one.
///
/// Steady state evicts zero or one entry per unpack, so this never binds there.
/// It exists for the ONE-OFF case this whole change is motivated by: the first
/// sweep on a node that upgrades with an unbounded legacy cache. The 1.2 GB /
/// 82-entry directory measured on a real peer would otherwise do ~78
/// `remove_dir_all`s inline before the shell page returns — a visible stall on
/// the first webapp load after an upgrade. Capped, that backlog drains over the
/// next handful of unpacks (plus the debounced reconcile sweep) instead of
/// landing on one request.
///
/// Note this bounds the *deletion* half only. The directory walk that precedes
/// it is proportional to the tree and is not capped — it is the price of
/// knowing the size at all, it runs on `spawn_blocking` rather than the
/// reactor, and it shrinks with the cache over the first few sweeps.
const WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP: usize = 8;

/// Minimum interval between budget sweeps that were triggered by a *reconcile*
/// rather than by an unpack.
///
/// An unpack is the only event that grows the cache, so it always sweeps. But a
/// node upgrading with an already-oversized cache may reconcile contracts whose
/// state hash never changes and therefore never unpack, so the reconcile path
/// also gets a chance to sweep — debounced, because unlike an unpack it is not
/// itself expensive and would otherwise pay for a directory walk on every
/// 30-second refresh of every contract.
const WEBAPP_CACHE_SWEEP_INTERVAL: Duration = Duration::from_secs(600);

/// Per-contract record of how recently this process served from the entry.
#[derive(Clone, Copy)]
struct CacheAccess {
    /// Last time any handler served (or attempted to serve) this contract.
    /// Drives the in-flight eviction guard.
    last_access: Instant,
    /// Last time `last_access` was mirrored onto the `{key}.hash` mtime.
    /// Drives the touch throttle only.
    last_persisted: Instant,
}

/// In-memory last-access record, mirrored to disk at
/// `WEBAPP_CACHE_ACCESS_TOUCH_INTERVAL` granularity.
///
/// Bounded by the number of entries actually on disk, which
/// `WEBAPP_CACHE_MAX_BYTES` bounds in turn: it is only ever written where the
/// cache entry is known to exist (a warm `{key}.hash`, or a fetch that just
/// populated one), and the sweep drops the record when it evicts the entry.
/// That gating is load-bearing, not incidental — `variable_content` is reachable
/// unauthenticated with an arbitrary key, so recording an access for a key that
/// has not yet produced a cache entry would hand an attacker an unbounded
/// per-key map (see the per-key-collection rule in
/// `.claude/rules/code-style.md`).
static WEBAPP_CACHE_ACCESS: LazyLock<DashMap<ContractInstanceId, CacheAccess>> =
    LazyLock::new(DashMap::new);

/// What caused a budget sweep to be considered.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SweepTrigger {
    /// The cache just grew — always sweep.
    Unpack,
    /// The cache was reconciled but not rewritten — sweep at most once per
    /// `WEBAPP_CACHE_SWEEP_INTERVAL`.
    Reconcile,
}

/// Sweep bookkeeping for one cache directory: when the last sweep ran (the
/// reconcile debounce) and whether one is running right now.
#[derive(Default, Debug)]
struct SweepState {
    last_sweep: Option<Instant>,
    in_progress: bool,
}

/// Where the extracted webapp cache lives, how large it may grow, and how often
/// that bound is enforced.
///
/// **Injected from the node's configuration, never read from a global.** Every
/// path the handlers touch — `<key>/`, `<key>.hash`, and the sweep root — is
/// derived from `root`, which the router builds once from
/// `WebsocketApiConfig::webapp_cache_dir` and hands to every handler through the
/// axum `State`.
///
/// That injection is a safety property, not a convenience, and it took two
/// attempts to get right. The sweep DELETES, and several tests drive
/// `unpack_if_stale` end to end, so a root read from a process global made
/// `cargo test -p freenet` evict the developer's real
/// `~/.cache/freenet/webapp_cache` down to the production budget — and, because
/// the in-flight guards are per-process while the directory is per-user, it
/// could evict entries a node running as the same user was actively serving.
/// The first fix gated a temp-dir redirect on `#[cfg(test)]`, which covers unit
/// tests only: `cfg(test)` is false when an integration test links the lib as an
/// ordinary dependency, so `tests/playwright_shell.rs` — which boots a real node
/// and fetches a shell page on a plain `cargo test` — still swept the real cache.
/// Threading the root has no such blind spot: a caller that does not supply one
/// does not compile.
///
/// Because the root comes from the node's config, a test node pointed at a
/// `tempfile::tempdir()` data dir gets an isolated cache for free, and two nodes
/// run by the same user no longer share one directory.
#[derive(Clone, Debug)]
pub(crate) struct WebappCache {
    root: PathBuf,
    max_bytes: u64,
    sweep: Arc<parking_lot::Mutex<SweepState>>,
    /// Permits for in-flight speculative fetches — see
    /// [`SPECULATIVE_FETCH_LIMIT`].
    ///
    /// Carried here rather than in a process global for the same reason `root`
    /// is: the bound belongs to a node, and a process can run several (the
    /// simulation harness does). A global would let one simulated node's
    /// traffic refuse another's.
    speculative_fetches: Arc<Semaphore>,
}

impl WebappCache {
    /// The cache a node serves from, bounded by [`WEBAPP_CACHE_MAX_BYTES`].
    ///
    /// One instance per server, cloned into the router state, so the sweep
    /// debounce and in-progress flag are shared across that node's requests.
    ///
    /// Creates the directory and names it in the log, once, here: this is where
    /// the cache takes ownership of a path it will DELETE from, and nothing else
    /// in the node identifies that path. An operator asking "what is removing
    /// files from here" or "where did this disk go" otherwise has nowhere to
    /// look, and a sweeper should say which directory it sweeps.
    ///
    /// Creating it eagerly also converts the two silent-misconfiguration shapes
    /// into a startup warning: a root that exists as a FILE, or one that cannot
    /// be created (permissions, read-only mount). Either leaves every unpack
    /// failing and the sweep scanning nothing, i.e. a cache that never populates
    /// and a bound that never runs, with no error surfaced anywhere because both
    /// paths are best-effort by design. That failure is not fatal and must not
    /// be, since the node serves everything except web contracts perfectly well,
    /// so this warns and carries on rather than refusing to start.
    pub(crate) fn with_root(root: PathBuf) -> Self {
        match std::fs::create_dir_all(&root) {
            Ok(()) => tracing::info!(
                path = %root.display(),
                max_bytes = WEBAPP_CACHE_MAX_BYTES,
                "webapp cache: unpacked web contracts are cached here; \
                 least-recently-used entries are DELETED from here to hold the \
                 directory under its size bound"
            ),
            Err(err) => tracing::warn!(
                path = %root.display(),
                "webapp cache: cannot create the cache directory ({err}); web \
                 contracts will fail to unpack and the size bound will not run. \
                 Check that the path is a directory and is writable, or point \
                 the node elsewhere with FREENET_WEBAPP_CACHE_DIR."
            ),
        }
        Self {
            root,
            max_bytes: WEBAPP_CACHE_MAX_BYTES,
            sweep: Arc::new(parking_lot::Mutex::new(SweepState::default())),
            speculative_fetches: Arc::new(Semaphore::new(SPECULATIVE_FETCH_LIMIT)),
        }
    }

    /// Claim one of this node's speculative-fetch permits, waiting at most
    /// [`SPECULATIVE_FETCH_WAIT`] for one to come free.
    ///
    /// The wait is bounded rather than absent so the lane stays fair (see
    /// [`SPECULATIVE_FETCH_WAIT`]), and bounded rather than open-ended so a
    /// request never queues behind a 30s network GET — that would be the
    /// resource exhaustion the bound exists to prevent.
    async fn speculative_fetch_slot(&self) -> Option<OwnedSemaphorePermit> {
        let lane = self.speculative_fetches.clone();
        // Fast path: a free permit costs no timer and no queue.
        if let Ok(slot) = lane.clone().try_acquire_owned() {
            return Some(slot);
        }
        tokio::time::timeout(SPECULATIVE_FETCH_WAIT, lane.acquire_owned())
            .await
            .ok()
            .and_then(Result::ok)
    }

    /// The directory this cache owns — i.e. the one its sweep deletes from.
    #[cfg(test)]
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// Directory the contract's web archive is unpacked into.
    fn entry_dir(&self, instance_id: &ContractInstanceId) -> PathBuf {
        self.root.join(instance_id.encode())
    }

    /// The `{key}.hash` sentinel: holds the unpacked state's hash, doubles as
    /// the "cache is populated" marker and as the LRU last-used timestamp.
    fn hash_path(&self, instance_id: &ContractInstanceId) -> PathBuf {
        self.root.join(format!("{}.hash", instance_id.encode()))
    }
}

/// Clears `in_progress` however the sweep ends, so a panic mid-sweep cannot
/// wedge the flag on and suppress every future sweep.
struct SweepInProgress(Arc<parking_lot::Mutex<SweepState>>);

impl Drop for SweepInProgress {
    fn drop(&mut self) {
        self.0.lock().in_progress = false;
    }
}

/// One `<instance_id>` entry of the webapp cache as seen by a sweep.
struct WebappCacheEntry {
    instance_id: ContractInstanceId,
    /// The name as it appears on disk — the directory name and the `{key}.hash`
    /// stem, which `scan_webapp_cache` has verified round-trips through
    /// `ContractInstanceId`.
    encoded: String,
    /// Unpacked tree plus the sentinel hash file.
    bytes: u64,
    /// Last-used proxy — see `scan_webapp_cache`.
    last_used: SystemTime,
}

/// Outcome of one sweep. Returned rather than logged-only so tests can assert
/// on the decisions instead of on filesystem side effects alone.
#[derive(Default, Debug)]
struct WebappCacheSweep {
    total_before: u64,
    bytes_freed: u64,
    evicted: Vec<ContractInstanceId>,
}

/// Record that `instance_id` is being served right now, and report whether the
/// on-disk last-used marker is due for a refresh.
///
/// Pure in-memory and synchronous: this runs on every request, so it must not
/// touch the filesystem. The (rare) disk refresh is the caller's job.
fn record_cache_access(instance_id: ContractInstanceId) -> bool {
    use dashmap::mapref::entry::Entry;

    let now = Instant::now();
    match WEBAPP_CACHE_ACCESS.entry(instance_id) {
        Entry::Occupied(mut occupied) => {
            let access = occupied.get_mut();
            access.last_access = now;
            if now.duration_since(access.last_persisted) >= WEBAPP_CACHE_ACCESS_TOUCH_INTERVAL {
                access.last_persisted = now;
                true
            } else {
                false
            }
        }
        Entry::Vacant(vacant) => {
            vacant.insert(CacheAccess {
                last_access: now,
                last_persisted: now,
            });
            true
        }
    }
}

/// True while `instance_id` is inside its post-access eviction grace window.
fn accessed_recently(instance_id: &ContractInstanceId) -> bool {
    WEBAPP_CACHE_ACCESS
        .get(instance_id)
        .map(|access| access.last_access.elapsed() < WEBAPP_CACHE_EVICTION_MIN_IDLE)
        .unwrap_or(false)
}

/// Mirror the last-access time onto the `{key}.hash` mtime, which is what
/// survives a restart and is what the sweep ranks on.
///
/// A timestamp-only update (`filetime::set_file_mtime`, which opens the file and
/// calls `futimens`) rather than a rewrite: the sentinel's *contents* are the
/// state hash that `unpack_if_stale` compares against, and rewriting them would
/// race a concurrent unpack. Best effort — a missing file (cold cache) or a
/// read-only cache dir must never fail a user request.
async fn persist_cache_access_marker(hash_path: PathBuf) {
    let result = tokio::task::spawn_blocking(move || {
        filetime::set_file_mtime(&hash_path, filetime::FileTime::now())
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => debug!("webapp cache: could not refresh last-used marker: {err}"),
        Err(err) => debug!("webapp cache: last-used marker task failed: {err}"),
    }
}

/// Note that a handler is serving `instance_id`, refreshing the on-disk LRU
/// marker when due.
///
/// Only call this where the cache entry is known to exist — see the bounding
/// note on [`WEBAPP_CACHE_ACCESS`].
async fn note_cache_access(cache: &WebappCache, instance_id: ContractInstanceId) {
    if record_cache_access(instance_id) {
        persist_cache_access_marker(cache.hash_path(&instance_id)).await;
    }
}

/// Recursively sum the size of every regular file under `dir`. Unreadable
/// entries contribute 0 rather than erroring: an under-count only means the
/// sweep evicts less than it could, which is the safe direction for a cache
/// whose deletion is the destructive operation.
fn dir_size(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                if let Ok(meta) = entry.metadata() {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }
    total
}

/// Enumerate the webapp cache under `root`, pairing each `<instance_id>`
/// directory with its `<instance_id>.hash` sentinel.
///
/// The last-used signal is the sentinel's mtime, which `note_cache_access`
/// refreshes while a contract is being served and which `unpack_if_stale`
/// rewrites on every re-extraction — so it tracks last USE, not creation.
/// Entries with no sentinel (or an unreadable one) fall back to the directory's
/// own mtime and finally to the epoch, i.e. they sort as the coldest.
///
/// Anything whose name is not a cache entry is ignored entirely: the sweep must
/// never count or delete files it does not own. `from_base58` alone is not a
/// sufficient filter — stdlib zero-pads a short decode instead of rejecting it
/// (`contract_interface/key.rs`), so ordinary names like `tmp`, `data` or
/// `assets` parse into well-formed but *wrong* ids. The name must therefore
/// round-trip: parse, re-encode, and match what is actually on disk. Without
/// that check the sweep would charge a stray directory's bytes to a phantom id,
/// try to delete a path that does not exist, treat the resulting `NotFound` as
/// success, and count bytes it never freed — stopping early, staying over
/// budget, and reporting evictions that deleted nothing.
///
/// Blocking — call from `spawn_blocking`.
fn scan_webapp_cache(root: &Path) -> Vec<WebappCacheEntry> {
    /// Parse a cache-entry name, rejecting anything that does not re-encode to
    /// itself. See the round-trip note on `scan_webapp_cache`.
    fn parse_entry_name(name: &str) -> Option<ContractInstanceId> {
        let instance_id = ContractInstanceId::from_base58(name).ok()?;
        (instance_id.encode() == name).then_some(instance_id)
    }

    // (bytes, sentinel mtime, directory mtime)
    let mut by_id: HashMap<ContractInstanceId, (u64, Option<SystemTime>, Option<SystemTime>)> =
        HashMap::new();
    let Ok(dir_entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    for dir_entry in dir_entries.flatten() {
        let Ok(file_type) = dir_entry.file_type() else {
            continue;
        };
        let file_name = dir_entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if file_type.is_dir() {
            let Some(instance_id) = parse_entry_name(name) else {
                continue;
            };
            let slot = by_id.entry(instance_id).or_insert((0, None, None));
            slot.0 = slot.0.saturating_add(dir_size(&dir_entry.path()));
            slot.2 = dir_entry
                .metadata()
                .ok()
                .and_then(|meta| meta.modified().ok());
        } else if file_type.is_file() {
            let Some(stem) = name.strip_suffix(".hash") else {
                continue;
            };
            let Some(instance_id) = parse_entry_name(stem) else {
                continue;
            };
            let meta = dir_entry.metadata().ok();
            let slot = by_id.entry(instance_id).or_insert((0, None, None));
            slot.0 = slot
                .0
                .saturating_add(meta.as_ref().map(|m| m.len()).unwrap_or(0));
            slot.1 = meta.and_then(|meta| meta.modified().ok());
        }
    }

    by_id
        .into_iter()
        .map(
            |(instance_id, (bytes, hash_mtime, dir_mtime))| WebappCacheEntry {
                // Equal to the on-disk name by construction: `parse_entry_name`
                // admitted the id only because the two already matched.
                encoded: instance_id.encode(),
                instance_id,
                bytes,
                last_used: hash_mtime.or(dir_mtime).unwrap_or(SystemTime::UNIX_EPOCH),
            },
        )
        .collect()
}

/// Delete one cache entry: sentinel first, then the unpacked tree.
///
/// The order is load-bearing. A directory with no `{key}.hash` reads as a COLD
/// cache and is simply re-fetched; a `{key}.hash` with no directory reads as a
/// WARM cache and would serve 404s until the contract's state happened to
/// change. So an interrupted eviction must leave the first shape, never the
/// second — and if the sentinel cannot be removed we leave the entry entirely
/// alone rather than create the second shape deliberately.
async fn remove_cache_entry(root: &Path, encoded: &str) -> std::io::Result<()> {
    match tokio::fs::remove_file(root.join(format!("{encoded}.hash"))).await {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    match tokio::fs::remove_dir_all(root.join(encoded)).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Evict least-recently-used entries until `cache` fits in its budget.
///
/// `in_use` is the contract whose request triggered the sweep; it is never a
/// victim of its own sweep. Two further guards steer eviction away from live
/// requests: an entry served within `WEBAPP_CACHE_EVICTION_MIN_IDLE` is skipped,
/// and an entry whose `CONTRACT_CACHE_LOCKS` mutex is held (an unpack is in
/// flight) is skipped via `try_lock`.
///
/// Those guards are a strong preference, not an interlock — the check and the
/// `remove_dir_all` are not atomic, and both guards are per-process while the
/// directory is per-user, so a request in another process (or one that slipped
/// between the check and the delete) can still lose its entry. That is
/// survivable rather than merely unlikely: the cache is recomputable, and on
/// Unix an already-opened file survives unlinking, so an in-flight `ServeFile`
/// stream completes and the worst case is a 404 or a refetch. See
/// `WEBAPP_CACHE_EVICTION_MIN_IDLE`.
///
/// The bound is best-effort in the other direction too: if every oversized entry
/// is protected, if a single webapp is itself larger than the budget, or if the
/// overage needs more than `WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP` deletions, the
/// sweep leaves the cache over budget and the next one retries. It never deletes
/// a protected entry to hit the number, and a failure to delete one entry never
/// aborts the sweep or propagates to the request.
///
/// # Cost
///
/// Awaited inline by the caller, so it is on the request path. In steady state
/// that is a directory walk plus at most one deletion, which is small change
/// next to the `remove_dir_all` + `unpack` that triggered it. The one expensive
/// case is the first sweep after upgrading a node with an unbounded legacy
/// cache: the walk is proportional to the whole tree (on `spawn_blocking`, so
/// it does not block the reactor) and the deletions are capped — see
/// [`WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP`].
async fn enforce_webapp_cache_budget(
    cache: &WebappCache,
    in_use: Option<ContractInstanceId>,
) -> WebappCacheSweep {
    let root = cache.root.clone();
    let max_bytes = cache.max_bytes;
    let scan_root = root.clone();
    let entries = match tokio::task::spawn_blocking(move || scan_webapp_cache(&scan_root)).await {
        Ok(entries) => entries,
        Err(err) => {
            tracing::warn!("webapp cache: size scan failed, skipping sweep: {err}");
            return WebappCacheSweep::default();
        }
    };

    let total: u64 = entries
        .iter()
        .fold(0u64, |acc, entry| acc.saturating_add(entry.bytes));
    let mut sweep = WebappCacheSweep {
        total_before: total,
        ..Default::default()
    };
    if total <= max_bytes {
        return sweep;
    }

    let mut entries = entries;
    // Oldest use first; base58 key as a deterministic tiebreak so two entries
    // sharing an mtime (common at 1s filesystem granularity) always evict in
    // the same order.
    entries.sort_by(|a, b| {
        a.last_used
            .cmp(&b.last_used)
            .then_with(|| a.encoded.cmp(&b.encoded))
    });

    let mut live = total;
    for entry in &entries {
        if live <= max_bytes || sweep.evicted.len() >= WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP {
            break;
        }
        if Some(entry.instance_id) == in_use || accessed_recently(&entry.instance_id) {
            continue;
        }
        let Some(guard) = try_acquire_cache_lock(&entry.instance_id) else {
            continue;
        };
        match remove_cache_entry(&root, &entry.encoded).await {
            Ok(()) => {
                live = live.saturating_sub(entry.bytes);
                sweep.bytes_freed = sweep.bytes_freed.saturating_add(entry.bytes);
                sweep.evicted.push(entry.instance_id);
                WEBAPP_CACHE_ACCESS.remove(&entry.instance_id);
                // Drop the reconcile timer too: `refresh_cache_if_due` returns
                // early on a fresh timer alone, so an evicted contract with a
                // live timer would serve 404s from the now-empty directory
                // until the TTL expired.
                CONTRACT_CACHE_REFRESH.remove(&entry.instance_id);
            }
            Err(err) => {
                tracing::warn!(
                    "webapp cache: could not evict {}: {err}",
                    entry.encoded.as_str()
                );
            }
        }
        drop(guard);
    }

    if !sweep.evicted.is_empty() {
        tracing::info!(
            evicted = sweep.evicted.len(),
            freed_bytes = sweep.bytes_freed,
            total_before = sweep.total_before,
            still_over_budget = live > max_bytes,
            "webapp cache: evicted least-recently-used entries to fit the size bound"
        );
    } else if live > max_bytes {
        debug!(
            total_bytes = total,
            max_bytes, "webapp cache: over budget but every entry is in use"
        );
    }
    sweep
}

/// Whether a sweep with this `trigger` is due.
///
/// An unpack is the only thing that grows the cache, so it always sweeps.
/// A reconcile rewrote nothing, so it sweeps at most once per
/// `WEBAPP_CACHE_SWEEP_INTERVAL` — otherwise every contract's 30-second refresh
/// would pay for a directory walk.
fn sweep_is_due(trigger: SweepTrigger, last_sweep: Option<Instant>, now: Instant) -> bool {
    match trigger {
        SweepTrigger::Unpack => true,
        SweepTrigger::Reconcile => {
            last_sweep.is_none_or(|prev| now.duration_since(prev) >= WEBAPP_CACHE_SWEEP_INTERVAL)
        }
    }
}

/// Run a budget sweep if `trigger` calls for one and no sweep is already
/// running.
///
/// The in-progress gate is not just an optimisation. Each sweep takes its own
/// `live` snapshot and deletes until *it* has freed the deficit, so N concurrent
/// unpacks would each evict a full deficit's worth and drive the cache well
/// below budget, over-reporting `bytes_freed` as they went. One sweep at a time
/// makes the eviction count match the actual overage.
async fn maybe_enforce_webapp_cache_budget(
    cache: &WebappCache,
    in_use: ContractInstanceId,
    trigger: SweepTrigger,
) {
    let _in_progress = {
        // Scoped so the (sync) lock is released before the await below.
        let mut state = cache.sweep.lock();
        let now = Instant::now();
        if state.in_progress || !sweep_is_due(trigger, state.last_sweep, now) {
            return;
        }
        state.in_progress = true;
        state.last_sweep = Some(now);
        SweepInProgress(Arc::clone(&cache.sweep))
    };
    enforce_webapp_cache_budget(cache, Some(in_use)).await;
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
/// # Why this exists (DoS amplification — #3945)
///
/// #3942 made `variable_content` issue a cold-cache network GET so a
/// subresource (`<img src>`) pointing at a contract resolves instead of
/// 404ing (#3940). That widened the attack surface: an unauthenticated
/// request to `/v1/contract/web/<KEY>/...` for a *random* 32-byte `KEY`
/// no longer 404s from the local cache check — it triggers a full network
/// GET (fan-out to remote peers) + unpack. Subresource URLs are
/// machine-fetchable, so an attacker can spray random keys and force the
/// node to issue outbound GETs it would never otherwise issue. Per-key rate
/// is bounded by the 30s fetch timeout but the parallel fan-out was not.
///
/// #4417 made this a hard GATE on the cold fetch, which closed the vector by
/// also closing #3940 for any contract the node had never seen: a link to an
/// image inside another container 404'd unless the reader had already visited
/// that container's root (#5406). What bounds the fan-out now is
/// [`SPECULATIVE_FETCH_LIMIT`], so this query has become the FALLBACK that
/// runs only when that lane is saturated — the answer that lets a contract the
/// node demonstrably already has skip the queue during a spray, rather than
/// the permission every cold fetch needs.
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
            // Node-internal fetch issued by the HTTP layer itself (webapp cache
            // reconcile), not a client connection. It carries no
            // `origin_contract`, so the scope attests nothing either way.
            connection_scope: crate::client_events::ConnectionScope::Local,
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
            // Node-internal fetch issued by the HTTP layer itself (webapp cache
            // reconcile), not a client connection. It carries no
            // `origin_contract`, so the scope attests nothing either way.
            connection_scope: crate::client_events::ConnectionScope::Local,
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
/// A **cold**-cache GET is speculative — nothing local says the contract
/// exists — so it must claim one of the node's [`SPECULATIVE_FETCH_LIMIT`]
/// permits, held for the duration of the fetch. When they are all in flight,
/// the contract has to be locally KNOWN (see `is_locally_known`) to fetch
/// anyway; otherwise this returns `Ok(())` without issuing the network GET and
/// the caller serves a 404 from the empty cache directory. That is the
/// random-key DoS amplification vector #3942 opened and #3945 raised, bounded
/// rather than closed, so the legitimate half of the same shape — the #3940
/// cross-contract subresource for a contract this node has never seen — works
/// again (#5406). A **warm-but-stale** refresh takes no permit: a warm on-disk
/// cache already proves the node legitimately fetched this contract, so
/// refreshing it is not the amplification vector, and bounding it would
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
///
/// This is also where both cache-reading handlers (`variable_content` and
/// `serve_sandbox_content`) mark the entry as in use for the LRU size bound, so
/// the marking happens exactly once per request and only for entries that exist.
async fn refresh_cache_if_due(
    instance_id: ContractInstanceId,
    request_sender: &HttpClientApiRequest,
    cache: &WebappCache,
) -> Result<(), WebSocketApiError> {
    let hash_path = cache.hash_path(&instance_id);
    let cache_warm = tokio::fs::try_exists(&hash_path).await.unwrap_or(false);

    // The entry is about to be read, so mark it in use before anything else:
    // that both steers a concurrent budget sweep away from it for the duration
    // of this request and keeps its LRU marker current. Gated on `cache_warm`
    // because an arbitrary key reaching this handler has no cache entry yet —
    // see the bounding note on `WEBAPP_CACHE_ACCESS`.
    if cache_warm {
        note_cache_access(cache, instance_id).await;
    }

    // Fast path: a warm cache reconciled within the TTL needs no work and must
    // not contend on the refresh lock.
    if cache_warm && cache_reconciled_recently(&instance_id) {
        return Ok(());
    }

    // Slow path: refresh looks due. Serialize concurrent refreshers for this
    // contract so only the first issues a GET; the rest re-check below.
    let mut refresh = acquire_refresh_lock(&instance_id).await;
    // Re-check under the lock, and RE-STAT rather than trusting the timer
    // alone. The timer is per-process; the cache directory is per-USER, and the
    // documented multi-peer setup (peer-manager.sh) runs several nodes as one
    // user. Another node's budget sweep can therefore evict this entry at any
    // moment, and its `CONTRACT_CACHE_REFRESH.remove` — the in-process
    // mitigation — is invisible to us. Returning on a fresh timer alone would
    // then skip the refetch and serve 404s out of the emptied directory for the
    // rest of our TTL window. Requiring warm AND fresh also still covers the
    // in-process race this check was originally for: a concurrent refresher
    // that completed while we waited both populated the cache and recorded a
    // fresh timer, so it satisfies both halves.
    let still_warm = tokio::fs::try_exists(&hash_path).await.unwrap_or(false);
    if still_warm && cache_reconciled_recently(&instance_id) {
        return Ok(());
    }

    // Cold path only: bound how many contracts this node will speculatively
    // fetch at once, and hold the permit until the GET below returns.
    //
    // A cold cache (no `{key}.hash` on disk) for a contract with no local
    // trace is BOTH the #3940 cross-contract `<img src>` a user is waiting on
    // and the random-key enumeration #3942 opened, and nothing at this layer
    // tells them apart. #4417 resolved that by fetching only for contracts the
    // node already stored or subscribed to, which closed the vector by also
    // closing #3940 for every contract this node had never seen — so a page
    // linking an image inside another container 404'd unless the reader had
    // already visited that container's root (#5406). The bound replaces the
    // refusal: the fetch goes ahead while permits last, and only a caller that
    // finds the lane saturated has to prove local presence first.
    //
    // Ordering matters. The permit is tried FIRST because it is a local
    // atomic, where `is_locally_known` costs a round trip to the node; asking
    // only when the lane is saturated keeps that cost off the path every real
    // page load takes. It also means a locally-known contract can still be
    // fetched once the lane is full — the query is the fallback, not a gate,
    // so nothing that resolved before this change stops resolving now.
    //
    // The WARM-but-stale refresh takes no permit: a warm on-disk cache is
    // itself proof the node legitimately fetched this contract before, so a
    // TTL-driven re-fetch of an already-cached bundle is not the random-key
    // amplification vector, and bounding it would silently break the #3977
    // republish-pickup for a contract cached warm but currently unsubscribed.
    // The check reads `cache_warm || still_warm`: a sentinel seen at EITHER
    // observation is that proof. Requiring both would send a legitimate entry
    // another process just evicted down the speculative path, and requiring
    // only the pre-lock snapshot would miss a concurrent refresher that warmed
    // the cache while we waited.
    let cold = !(cache_warm || still_warm);

    // A cold fetch that just failed will fail again: the answer came from the
    // network and nothing has changed since. Without this, the subresources of
    // a page pointing at an unfindable contract each pay their own full GET in
    // turn. Callers inside the window get the empty-cache 404 rather than the
    // first caller's error, which is also what they got before the fetch
    // existed at all.
    if cold && refresh.cold_fetch_failed_recently(&instance_id) {
        return Ok(());
    }

    // `_speculative_slot` must stay a NAMED binding: it holds the permit for
    // the fetch below, and `let _ = ...` would drop it immediately, silently
    // removing the concurrency bound. Pinned by
    // `an_in_flight_fetch_holds_its_speculative_permit`.
    let mut _speculative_slot = None;
    if cold {
        match cache.speculative_fetch_slot().await {
            Some(slot) => _speculative_slot = Some(slot),
            None if is_locally_known(instance_id, request_sender).await => {}
            None => {
                tracing::debug!(
                    "webapp cache: speculative fetch lane full; not fetching unknown {instance_id}"
                );
                return Ok(());
            }
        }
    }

    let fetched = ensure_contract_cached(instance_id, request_sender, None, cache).await;
    if cold {
        refresh.last_cold_failure = fetched.is_err().then(|| (instance_id, Instant::now()));
    }
    fetched?;
    CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());
    // The fetch populated the entry, so it now exists and is about to be read.
    note_cache_access(cache, instance_id).await;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip(request_sender, cache))]
pub(super) async fn contract_home(
    key: String,
    request_sender: HttpClientApiRequest,
    assigned_token: AuthToken,
    api_version: ApiVersion,
    query_string: Option<String>,
    sub_path: Option<&str>,
    hosted_mode: bool,
    cache: &WebappCache,
) -> Result<impl IntoResponse + use<>, WebSocketApiError> {
    let instance_id = ContractInstanceId::from_base58(&key).map_err(|err| {
        debug!("contract_home: Failed to parse contract key: {}", err);
        WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        }
    })?;

    // Register the assigned token with origin_contracts so subsequent
    // WebSocket connections from the shell iframe authenticate against
    // the correct contract identity, then fetch + unpack the contract.
    //
    // Deliberately NOT behind `SPECULATIVE_FETCH_LIMIT`. This fetch is just as
    // speculative as the subresource one — an unauthenticated `GET
    // /v1/contract/web/<random>/` reaches it with any key — and it has never
    // been bounded or gated, including while #4417's presence gate was on the
    // subresource path. So the bound covers the narrower of two doors, and the
    // wider one stands open exactly as it did before. #3945 called this out and
    // ranked it Low ("top-level page navigations are typically human-paced");
    // bounding a human's first visit to a contract is a different trade from
    // bounding a machine-fetched subresource, and belongs in its own change.
    // Do NOT read `SPECULATIVE_FETCH_LIMIT` as covering the whole webapp-fetch
    // surface.
    ensure_contract_cached(
        instance_id,
        &request_sender,
        Some((assigned_token.clone(), instance_id)),
        cache,
    )
    .await?;
    // The fetch populated the entry, so it now exists and is about to be read
    // by the iframe load that immediately follows. Marking it in use steers a
    // concurrent budget sweep away from it for that request.
    note_cache_access(cache, instance_id).await;
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
    cache: &WebappCache,
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
    // Bound the wait for the connection id. A node that accepts the connection
    // and then never assigns one would otherwise pin this task forever — and
    // with it the speculative-fetch permit the caller is holding, so a wedged
    // node would drain the lane permanently and never refill it.
    //
    // This used to be unreachable: #4417's presence gate ran first, and its own
    // timeouts failed closed, so a cold request never got here unless the node
    // was answering. Removing that gate is what makes the bare `recv()` matter,
    // which is why the timeout arrives with it. Same bound as the presence
    // query, for the same handshake.
    let client_id = match tokio::time::timeout(PRESENCE_QUERY_TIMEOUT, response_recv.recv()).await {
        Ok(Some(HostCallbackResult::NewId { id })) => id,
        _ => {
            return Err(WebSocketApiError::NodeError {
                error_cause: "Couldn't register new client in the node".into(),
            });
        }
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
            // Node-internal fetch issued by the HTTP layer itself (webapp cache
            // reconcile), not a client connection. It carries no
            // `origin_contract`, so the scope attests nothing either way.
            connection_scope: crate::client_events::ConnectionScope::Local,
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
    let outcome = handle_get_response(instance_id, recv_result, cache).await;

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
            // Node-internal fetch issued by the HTTP layer itself (webapp cache
            // reconcile), not a client connection. It carries no
            // `origin_contract`, so the scope attests nothing either way.
            connection_scope: crate::client_events::ConnectionScope::Local,
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
    cache: &WebappCache,
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
        })) => unpack_if_stale(&contract, state.as_ref(), cache).await,
        Ok(Some(HostCallbackResult::Result {
            result:
                Ok(HostResponse::ContractResponse(ContractResponse::GetResponse {
                    contract: None, ..
                })),
            ..
        })) => Err(WebSocketApiError::MissingContract { instance_id }),
        // TRANSIENT: the GET's retry loop exhausted without locating the
        // contract. This is a SUCCESS at the client-API level, not an `Err` —
        // `operations/get/op_ctx_task.rs` deliberately converts exhaustion into
        // `ContractResponse::NotFound` so a client can tell that apart from "the
        // operation failed".
        //
        // Read the PRODUCER, not the variant's doc, for what it means. stdlib
        // documents `NotFound` as "Contract was not found after exhaustive
        // search ... distinguishes 'contract doesn't exist' from other failure
        // modes", i.e. as proof of absence. This node emits it on RETRY-LOOP
        // EXHAUSTION, which is a much weaker claim — "nobody I asked had it",
        // over a ring that may not yet have propagated the contract at all. The
        // two do not say the same thing, and that gap is the whole reason this
        // arm must not answer 404: treating exhaustion as absence is precisely
        // the error being fixed here.
        //
        // Without this arm that distinction was DISCARDED here: `NotFound` is an
        // `Ok(..)` that no arm matched, so it fell through to the catch-all
        // below and became `NodeError { "Unexpected response from node: .." }`.
        // `errors.rs` only maps a `NodeError` to 404 when its message begins
        // with the literal "Contract not found", which that Debug-formatted
        // string does not, so every dead-ended GET on the web route was served
        // as a bare 500 — indistinguishable from a genuine internal failure, and
        // carrying none of the `Retry-After` / `Cache-Control: no-store` headers
        // the transient path sets.
        //
        // On Freenet a `NotFound` is routinely "not found YET" rather than proof
        // of absence: a contract published elsewhere is unreachable from this
        // node until it propagates (the #4404 placement gap), which is a window
        // of minutes to hours. `WebSocketApiError::ContractNotFound` gives it the
        // transient STATUS and headers (503 + `Retry-After`), which is what a
        // programmatic client needs in order to come back later rather than write
        // the contract off.
        //
        // It is a dedicated variant rather than a reuse of the `Err(_)` arm's
        // `RequestError(Timeout)`, because this is not a timeout and must not
        // inherit `retry_loading_page`: that page reloads forever, and the same
        // reply is produced for a key that will never resolve, so a mistyped URL
        // in an open tab would re-issue a network GET every minute for the life
        // of the tab. See the variant's doc in `errors.rs`.
        //
        // 404 would be the WRONG call and is worse than the 500 it replaces: a
        // well-behaved crawler treats 404 as terminal (Atlas marks such a
        // locator seen for good and never retries it), so answering 404 here
        // would permanently exclude every contract that was merely slow to
        // propagate. Only answer 404 where absence is locally PROVEN — which is
        // what the `contract: None` arm above does.
        Ok(Some(HostCallbackResult::Result {
            result: Ok(HostResponse::ContractResponse(ContractResponse::NotFound { .. })),
            ..
        })) => {
            // Plain `info!`, like every other diagnostic in this module, so it
            // lands in the node's log files and NOT in the OTel collector, which
            // only carries enumerated events. Fine for reading one node's log;
            // if anyone wants a fleet-wide dead-ended-GET rate, that needs an
            // enumerated event, not this line.
            tracing::info!(
                instance_id = %instance_id.encode(),
                "contract not found on the network (GET exhausted); serving 503"
            );
            Err(WebSocketApiError::ContractNotFound { instance_id })
        }
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
///
/// Both exits then run [`maybe_enforce_webapp_cache_budget`], which is what
/// keeps the cache from growing without bound (see [`WEBAPP_CACHE_MAX_BYTES`]).
async fn unpack_if_stale(
    contract: &ContractContainer,
    state_bytes: &[u8],
    cache: &WebappCache,
) -> Result<(), WebSocketApiError> {
    let contract_key = contract.key();
    let instance_id = *contract_key.id();
    let path = cache.entry_dir(&instance_id);
    let current_hash = hash_state(state_bytes);
    let hash_path = cache.hash_path(&instance_id);

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
        // Nothing grew, but this is still the one code path every reconcile
        // reaches, so give the debounced sweep a chance: a node that upgrades
        // with an already-oversized cache may keep serving contracts whose
        // state hash never changes and would otherwise never sweep.
        drop(_guard);
        maybe_enforce_webapp_cache_budget(cache, instance_id, SweepTrigger::Reconcile).await;
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

    let unpacked = WebApp::try_from(state.as_ref())
        .and_then(|mut web| web.unpack(&path))
        .map_err(|e| err(e, contract));
    if let Err(unpack_failed) = unpacked {
        // Take the directory back out. A contract that is not a web archive
        // fails here every time it is requested, and the sweep would never
        // reclaim what it leaves: an empty directory contributes 0 bytes to a
        // budget measured in bytes. With the cold fetch no longer gated on
        // local presence, that would be one attacker-named directory per key.
        if let Err(cleanup_failed) = tokio::fs::remove_dir_all(&path).await {
            debug!("webapp cache: could not remove failed unpack dir: {cleanup_failed}");
        }
        return Err(unpack_failed);
    }

    // Store new hash LAST, so a partial unpack does not leave a stale
    // hash file that would make future requests skip the fetch.
    tokio::fs::write(&hash_path, current_hash.to_be_bytes())
        .await
        .map_err(|e| WebSocketApiError::NodeError {
            error_cause: format!("Failed to write state hash: {e}"),
        })?;

    // The unpack above is the only thing that grows the webapp cache, so it is
    // the natural (and cheapest) sweep trigger: the directory walk it costs is
    // small change next to the `remove_dir_all` + `unpack` just performed, and
    // no request that merely reads from a warm cache pays for it. Released the
    // per-contract lock first so the sweep's `try_lock` guard is only reporting
    // on OTHER contracts' in-flight unpacks.
    drop(_guard);
    maybe_enforce_webapp_cache_budget(cache, instance_id, SweepTrigger::Unpack).await;

    Ok(())
}

#[instrument(level = "debug", skip(request_sender, cache))]
pub(super) async fn variable_content(
    key: String,
    req_path: String,
    api_version: ApiVersion,
    request_sender: HttpClientApiRequest,
    cache: &WebappCache,
) -> Result<impl IntoResponse + use<>, Box<WebSocketApiError>> {
    debug!(
        "variable_content: Processing request for key: {}, path: {}",
        key, req_path
    );
    // compose the correct absolute path
    let instance_id =
        ContractInstanceId::from_base58(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;
    let base_path = cache.entry_dir(&instance_id);
    debug!("variable_content: Base path resolved to: {:?}", base_path);

    // Extract the relative asset path from the already-decoded request path.
    //
    // `req_path` is built by the caller from axum's percent-DECODED wildcard
    // segment, so it may legitimately contain characters (spaces, `<`, `>`,
    // backticks, …) that are invalid in a raw URI. The previous implementation
    // re-parsed `req_path` as an `axum::http::Uri`, which rejected any such
    // character with a 400 — so an asset named `my image.png` 404'd/400'd, and
    // because that error carried no CORS header the sandboxed iframe surfaced
    // it as an opaque "CORS error" instead of a real status (user report:
    // SUB0PT1MAL / cirro, 2026-07-29). Strip the prefix textually instead; no
    // URI round-trip, so the decoded name reaches the filesystem unharmed.
    let relative_path = relative_asset_path(&req_path).map_err(Box::new)?;
    debug!(
        "variable_content: Extracted relative path: {}",
        relative_path
    );
    // Reject a path that can never resolve inside the cache BEFORE fetching.
    // There is no reason to pull a contract off the network to serve a
    // traversal, and it keeps a spray of `../` requests from consuming
    // speculative-fetch permits. `resolve_web_asset_path` re-runs this check
    // below; the symlink/TOCTOU half of it can only run once the unpack has
    // happened, which is why the two are not merged.
    if has_escaping_component(Path::new(&relative_path)) {
        return Err(Box::new(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        }));
    }

    // Fetch + unpack the contract if its cache is cold OR stale. Without the
    // cold-cache fetch, any subresource request (e.g. an <img src> pointing at
    // this contract from a different webapp) would 404 because the cache is
    // only populated by the shell-root handler (`contract_home`). See #3940.
    // The TTL-gated staleness refresh additionally picks up a republished
    // bundle on this path without requiring a prior hit on the shell root.
    // See #3977.
    //
    // The cold-cache GET is speculative — nothing local says the key names a
    // real contract — so it claims one of the node's `SPECULATIVE_FETCH_LIMIT`
    // permits. Once those are in flight an unknown key 404s from the empty
    // cache below without touching the network, which bounds the DoS
    // amplification #3942 opened (#3945) without re-breaking #3940 for
    // contracts this node has not seen before (#5406).
    refresh_cache_if_due(instance_id, &request_sender, cache)
        .await
        .map_err(Box::new)?;

    // Resolve the relative path UNDER the contract's cache dir with a
    // traversal guard. do NOT remove — this is the containment check that
    // stops `..%2f..%2fetc%2fpasswd` and `%2fetc%2fpasswd` (which decode to
    // `../../etc/passwd` and `/etc/passwd`) from escaping the cache and
    // serving arbitrary local files. See `resolve_web_asset_path` and
    // hosting/security notes; sibling `sandbox_content_body` has the same
    // guard for HTML pages.
    let file_path = resolve_web_asset_path(&base_path, &relative_path).map_err(Box::new)?;
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
pub(super) fn sanitize_shell_sub_path(sub_path: &str) -> Result<String, WebSocketApiError> {
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
    // …and reject a surviving `%`, because the dot-segment check above sees the
    // path exactly ONCE-decoded (axum's `PercentDecodedStr` decodes path params
    // a single time) while the browser decodes the URL we hand back again. So
    // `%252e%252e` on the wire arrives here as the literal text `%2e%2e` —
    // neither `.` nor `..`, so it passes — and the WHATWG URL parser then
    // treats it as a dot segment and normalizes it away. Measured end to end:
    // `/v1/contract/web/KEYA/%252e%252e/KEYB/index.html` produced an iframe
    // `data-src` the browser resolved to KEYB's page, so KEYB's app ran inside
    // a shell holding KEYA's auth token — the cross-contract confusion this
    // whole function exists to prevent (#3841).
    //
    // Rejecting `%` outright rather than enumerating the encodings of `.`: this
    // path is already once-decoded, so a legitimate sub-path reaches us with
    // its escapes resolved (a filename with a space arrives as a space — and is
    // rejected by the whitespace check above, so that is the established bar).
    // A literal `%` in a contract filename is vanishingly rare, and the cost is
    // a 400 on a deep link, not a broken app.
    if sub_path.contains('%') {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "deep-link sub-path must not contain percent escapes".to_string(),
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
            // flag) and the auth credential `authToken`. Shared with the
            // redirect filter rather than duplicated, so the two cannot
            // drift — and so both get the percent-decoding of the name that
            // a raw prefix match misses (`authT%6Fken=evil` reads back as
            // `authToken` from `location.search` inside the iframe).
            if super::client_api::is_sensitive_query_param(param) {
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
    cache: &WebappCache,
) -> Result<impl IntoResponse + use<>, WebSocketApiError> {
    let page = sub_path.unwrap_or("index.html");
    debug!("serve_sandbox_content: serving iframe content for key: {key}, page: {page}");
    let instance_id =
        ContractInstanceId::from_base58(&key).map_err(|err| WebSocketApiError::InvalidParam {
            error_cause: format!("{err}"),
        })?;

    // Reject a page path that can never resolve BEFORE fetching, for the same
    // reason `variable_content` does: a request that cannot succeed must not
    // spend one of the node's speculative-fetch permits. `sandbox_content_body`
    // runs the identical check again — it is the security boundary and stays
    // there — along with the canonicalization half, which can only run once the
    // bundle is on disk.
    if has_escaping_component(Path::new(page)) {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        });
    }

    // Reconcile the on-disk cache against current network state before serving.
    // Previously this path only checked `path.exists()` and served whatever was
    // already extracted, so a republished contract kept serving the old bundle
    // here until the shell root (`/`) was hit again. The TTL gate bounds the
    // network GET rate to at most one per contract per window. See #3977.
    refresh_cache_if_due(instance_id, &request_sender, cache).await?;

    let path = cache.entry_dir(&instance_id);
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
    // Path::join with an absolute path replaces the base entirely on Unix, and a
    // Windows drive-relative `C:foo` resolves off the base drive, so we reject
    // `..`, root, and drive-prefix components before joining.
    //
    // do NOT remove or weaken — security boundary. Uses the SAME
    // `has_escaping_component` check as `resolve_web_asset_path` (the non-HTML
    // asset path) so the two guards cannot drift.
    let normalized = Path::new(page);
    if has_escaping_component(normalized) {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        });
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

    // Inject the WebSocket shim, navigation interceptor, and title sync before
    // any other scripts. The shim overrides window.WebSocket so that
    // wasm-bindgen routes connections through the shell page's bridge. The
    // interceptor catches <a> clicks AND overrides programmatic window.open,
    // routing both through postMessage for multi-page navigation without a
    // sandbox-inheriting popup (#4645). The title sync forwards this page's
    // <title> to the shell so the browser tab reflects it.
    let injected_scripts = format!(
        "<script>{WEBSOCKET_SHIM_JS}</script><script>{NAVIGATION_INTERCEPTOR_JS}</script><script>{TITLE_SYNC_JS}</script>"
    );
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
/// Intercepts clicks on same-origin in-contract `<a>` elements and asks the
/// shell to move the iframe (`type: 'navigate'`), which is what makes
/// multi-page contract websites work without `allow-top-navigation`.
///
/// It deliberately does NOT touch new-window activations — an explicit
/// non-`_self` `target`, or a ctrl/cmd/shift/middle-click. Those open natively.
/// The iframe carries `allow-popups-to-escape-sandbox`, so the popup is a
/// normal top-level document at the node's real origin: the shell loads (its
/// `frame-src 'self'` matches), `localStorage` and the hosted per-user access
/// key work (freenet-core#4645), and a cross-origin destination sees a real
/// Origin rather than `null` (freenet/river#208).
///
/// Routing new-window opens through the shell's `open_url` bridge instead is
/// what broke the click (#5106). Which mechanism did it is NOT settled: the
/// popup-blocker account (Firefox allows `window.open` only from events in
/// `dom.popup_allowed_events`, and `message` is not one) is a diagnosis from
/// the symptom that no harness reproduces, while #5107 measured that the
/// bridge's loopback refusal drops the forwarded open on a local node in every
/// engine. Both are consequences of the round-trip, and this design removes the
/// round-trip: a real gesture in the frame that received the click opens a tab
/// everywhere, whichever explanation is right. See the head comment in
/// `navigation_interceptor.js`.
///
/// The one cross-origin case still intercepted is a link with NO new-window
/// target: navigating the app frame itself to a foreign origin is refused by
/// the shell's `frame-src 'self'`, so the click would silently do nothing. It
/// is turned into `window.open(..., '_blank', 'noopener,noreferrer')` from
/// inside the click handler, where the gesture is live.
const NAVIGATION_INTERCEPTOR_JS: &str =
    include_str!("path_handlers/assets/navigation_interceptor.js");

/// JavaScript that forwards the sandboxed iframe's `document.title` to the
/// shell via the `__freenet_shell__` / `type: 'title'` postMessage
/// `SHELL_BRIDGE_JS` already handles.
///
/// The shell page's own `<title>` is hardcoded (`shell.html`) because this
/// iframe has no `allow-same-origin` and cannot touch the parent's
/// `document.title` directly. Before this script existed, the shell's title
/// only ever changed for the handful of apps that hand-rolled this exact
/// postMessage themselves (River, Atlas, Delta); every other app — including
/// a static, JS-free contract website — left the browser tab reading
/// "Freenet" forever. Injected into every sandboxed page unconditionally so
/// the tab title is correct with zero per-app opt-in.
const TITLE_SYNC_JS: &str = include_str!("path_handlers/assets/title_sync.js");

/// Strips the version + contract + key prefix from a request path and returns
/// the remaining relative asset path (e.g. `assets/app.js`, or `my image.png`).
///
/// Operates on the raw string rather than an `axum::http::Uri` so a decoded
/// filename containing URI-invalid characters (spaces, `<`, `>`, backticks)
/// survives — re-parsing such a string as a `Uri` is what produced the spurious
/// 400 behind the SUB0PT1MAL/cirro CORS report. Query/fragment stripping is
/// unnecessary here: the caller builds this path from axum's `{*path}` wildcard,
/// which already excludes the query string.
fn relative_asset_path(path_str: &str) -> Result<String, WebSocketApiError> {
    let remainder = if let Some(rem) = path_str.strip_prefix("/v1/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v1/contract/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/web/") {
        rem
    } else if let Some(rem) = path_str.strip_prefix("/v2/contract/") {
        rem
    } else {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: format!(
                "URI path '{path_str}' does not start with /v1/contract/ or /v2/contract/"
            ),
        });
    };

    // remainder contains "{key}/{path}" or just "{key}"
    let file_path = match remainder.split_once('/') {
        Some((_key, path)) => path.to_string(),
        None => String::new(),
    };

    Ok(file_path)
}

/// Whether any component of `path` could escape a base directory when joined:
/// `..` (`ParentDir`), an absolute root (`RootDir`), or a Windows drive prefix
/// (`Prefix`, e.g. `C:foo` — a drive-relative path with NO `RootDir` component,
/// so a `ParentDir | RootDir`-only check would let it through and `Path::join`
/// could resolve it off the base drive's current directory).
///
/// Shared by BOTH web-content containment guards — `resolve_web_asset_path`
/// (non-HTML assets) and `sandbox_content_body` (HTML pages) — so they cannot
/// drift apart. do NOT weaken — this is a security boundary.
fn has_escaping_component(path: &Path) -> bool {
    use std::path::Component;
    path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    })
}

/// Resolves a decoded relative asset path underneath `base` (the contract's
/// on-disk cache directory), rejecting any path that would escape it.
///
/// # Why this exists (path traversal — arbitrary local file read)
///
/// The relative path comes from axum's percent-DECODED `{*path}` wildcard, so
/// `..%2f..%2fetc%2fpasswd` arrives as `../../etc/passwd` and `%2fetc%2fpasswd`
/// as the absolute `/etc/passwd`. `Path::join` with an absolute path REPLACES
/// the base entirely, and `..` components walk out of it — so joining the raw
/// relative path and serving it read any file the node process could open. The
/// sandbox CORS header (`Access-Control-Allow-Origin: *`) is added to this
/// path's responses, which let a malicious web contract's iframe JS read the
/// escaped file cross-origin. This guard closes both.
///
/// A component-level check (`has_escaping_component`) rejects `..`, root, and
/// drive-prefix components WITHOUT requiring the target to exist, so a
/// genuinely-missing asset still falls through to a normal 404 (callers serve
/// `base.join(rel)` and 404 on a missing file). When the resolved path DOES
/// exist we additionally canonicalize and re-verify containment, catching a
/// symlink inside the cache dir (a contract's web archive is attacker-authored)
/// that points outside it.
///
/// The returned path is the CANONICAL one when the target exists, so the caller
/// opens the already-resolved path rather than re-walking the symlinks — closing
/// the check-then-open TOCTOU window exactly as the sibling `sandbox_content_body`
/// does (it opens `canonical_file`, not the user path).
///
/// do NOT remove or weaken — this is a security boundary. Keep in sync with the
/// containment guard in `sandbox_content_body`.
fn resolve_web_asset_path(base: &Path, relative: &str) -> Result<PathBuf, WebSocketApiError> {
    let rel = Path::new(relative);
    if has_escaping_component(rel) {
        return Err(WebSocketApiError::InvalidParam {
            error_cause: "Path traversal not allowed".to_string(),
        });
    }

    let joined = base.join(rel);

    // If the path resolves on disk, its canonical form must live under the
    // canonical base — this catches a symlink inside the cache dir pointing out.
    // Serve the CANONICAL path so a symlink swapped between this check and the
    // open cannot redirect the read outside the base (TOCTOU), matching the
    // canonical-open in `sandbox_content_body`. When the target does not exist
    // (canonicalize fails) there is no symlink to resolve and the lexical scan
    // already rejected `..`/root/prefix, so the plain join is contained; return
    // it and let the caller 404 on the missing file.
    match (joined.canonicalize(), base.canonicalize()) {
        (Ok(canonical_file), Ok(canonical_base)) => {
            if !canonical_file.starts_with(&canonical_base) {
                return Err(WebSocketApiError::InvalidParam {
                    error_cause: "Path traversal not allowed".to_string(),
                });
            }
            Ok(canonical_file)
        }
        _ => Ok(joined),
    }
}

fn hash_state(state: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = ahash::AHasher::default();
    hasher.write(state);
    hasher.finish()
}

/// The cache the handler tests seed and serve from: one per-process temp dir,
/// never the developer's real cache. Production builds its own from the node's
/// config, so nothing here can reach a real directory even by mistake.
#[cfg(test)]
fn test_webapp_cache() -> WebappCache {
    static TEST_CACHE: LazyLock<WebappCache> = LazyLock::new(|| {
        static ROOT: LazyLock<tempfile::TempDir> =
            LazyLock::new(|| tempfile::tempdir().expect("test webapp cache root"));
        WebappCache::with_root(ROOT.path().to_path_buf())
    });
    TEST_CACHE.clone()
}

/// [`test_webapp_cache`] with every speculative-fetch permit already taken, so
/// a cold-cache test exercises the saturated-lane fallback (`is_locally_known`)
/// instead of fetching straight away.
///
/// Shares the singleton's root — the tests that use it want the same cache
/// directory, only a different answer from the lane — and takes its own
/// `Semaphore`, so draining it cannot affect a concurrently running test.
#[cfg(test)]
fn test_webapp_cache_saturated() -> WebappCache {
    // CLOSED rather than merely empty: a closed semaphore refuses instantly,
    // where an empty one makes every caller sit out `SPECULATIVE_FETCH_WAIT`
    // first. These tests are about what happens AFTER the lane is given up on,
    // and coupling each of them to that timer buys nothing and makes them
    // fragile under paused time. The wait itself is covered by
    // `an_in_flight_fetch_holds_its_speculative_permit`, which saturates a real
    // one-permit lane.
    let lane = Semaphore::new(0);
    lane.close();
    WebappCache {
        speculative_fetches: Arc::new(lane),
        ..test_webapp_cache()
    }
}

/// Cache paths of [`test_webapp_cache`], so a test can seed an entry the
/// handlers will then find.
#[cfg(test)]
fn contract_web_path(instance_id: &ContractInstanceId) -> PathBuf {
    test_webapp_cache().entry_dir(instance_id)
}

#[cfg(test)]
fn state_hash_path(instance_id: &ContractInstanceId) -> PathBuf {
    test_webapp_cache().hash_path(instance_id)
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

    /// Clears any webapp cache state for `instance_id` on disk.
    /// `contract_web_path` and `state_hash_path` resolve to the one per-process
    /// temp root of [`test_webapp_cache`], shared by every test in this module,
    /// so tests that exercise the cache must use unique keys AND scrub any stale
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

    // =========================================================================
    // Webapp cache size bound (LRU eviction)
    //
    // These exercise `enforce_webapp_cache_budget` against a `TempDir` root
    // rather than the node's real configured root, so they neither depend on nor
    // disturb residue in the developer's XDG cache. The in-memory
    // side-tables (`WEBAPP_CACHE_ACCESS`, `CONTRACT_CACHE_LOCKS`,
    // `CONTRACT_CACHE_REFRESH`) ARE process-global, so every test uses its own
    // instance ids and `seed_cache_entry` scrubs them first.
    // =========================================================================

    /// Size of the `{key}.hash` sentinel `seed_cache_entry` writes; entry sizes
    /// the sweep sees are payload + this.
    const SENTINEL_BYTES: u64 = 8;

    /// A cache over `root` with an explicit budget and its own sweep state.
    ///
    /// Every cache test builds one of these. Nothing here may reach the default
    /// cache with the production budget: the sweep DELETES, so a test that swept
    /// `crate::config::default_webapp_cache_dir()` would evict the developer's
    /// real cache and, on a machine running a node as the same user, entries
    /// that node is serving. Constructed field-by-field rather than through
    /// `with_root` so a test budget can be set; `with_root`'s own behaviour is
    /// covered by `with_root_creates_the_cache_directory_it_will_sweep`.
    fn cache(root: &Path, max_bytes: u64) -> WebappCache {
        WebappCache {
            root: root.to_path_buf(),
            max_bytes,
            sweep: Arc::new(parking_lot::Mutex::new(SweepState::default())),
            speculative_fetches: Arc::new(Semaphore::new(SPECULATIVE_FETCH_LIMIT)),
        }
    }

    /// Distinct instance id per (test, slot) pair, so process-global state from
    /// a sibling test can never protect or evict this test's entries.
    fn cache_id(test: u8, slot: u8) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xc0;
        bytes[1] = test;
        bytes[2] = slot;
        ContractInstanceId::new(bytes)
    }

    /// Materialize one cache entry of `payload` bytes under `root` whose
    /// last-used marker sits `age` in the past. Returns its total size as the
    /// sweep will account it.
    fn seed_cache_entry(
        root: &Path,
        instance_id: &ContractInstanceId,
        payload: usize,
        age: Duration,
    ) -> u64 {
        WEBAPP_CACHE_ACCESS.remove(instance_id);
        CONTRACT_CACHE_REFRESH.remove(instance_id);
        let encoded = instance_id.encode();
        let dir = root.join(&encoded);
        std::fs::create_dir_all(&dir).expect("create entry dir");
        std::fs::write(dir.join("index.html"), vec![b'x'; payload]).expect("write payload");
        let hash_path = root.join(format!("{encoded}.hash"));
        std::fs::write(&hash_path, 0u64.to_be_bytes()).expect("write sentinel");
        set_marker_age(&hash_path, age);
        payload as u64 + SENTINEL_BYTES
    }

    fn set_marker_age(path: &Path, age: Duration) {
        let when = SystemTime::now() - age;
        filetime::set_file_mtime(path, filetime::FileTime::from_system_time(when))
            .expect("set marker mtime");
    }

    fn dir_present(root: &Path, instance_id: &ContractInstanceId) -> bool {
        root.join(instance_id.encode()).exists()
    }

    /// A contract plus a state carrying a REAL packed web archive, so
    /// `unpack_if_stale` performs a genuine extraction instead of taking its
    /// matching-hash early return. `seed` distinguishes contract keys.
    fn webapp_contract_and_state(seed: &[u8]) -> (ContractContainer, WrappedState) {
        let mut archive = tar::Builder::new(std::io::Cursor::new(Vec::new()));
        let body: &[u8] = b"<html><body>hello</body></html>";
        let mut header = tar::Header::new_gnu();
        header.set_size(body.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        archive
            .append_data(&mut header, "index.html", body)
            .expect("append to archive");
        let packed = WebApp::from_data(Vec::new(), archive)
            .expect("build web app")
            .pack()
            .expect("pack web app");
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(seed.to_vec())),
            Parameters::from(vec![0]),
        )));
        (contract, WrappedState::new(packed))
    }

    fn sentinel_present(root: &Path, instance_id: &ContractInstanceId) -> bool {
        root.join(format!("{}.hash", instance_id.encode())).exists()
    }

    /// `with_root` materializes the directory it is going to sweep, including
    /// missing parents.
    ///
    /// The point is the startup log next to it: nothing else in the node names
    /// the directory this code DELETES from, so the one moment the cache takes
    /// ownership of a path is the moment to say which path it is. Creating it
    /// here is what makes that log a statement of fact rather than of intent,
    /// and it is what turns "the root is a file" or "the root is not writable"
    /// into a startup warning instead of a cache that silently never populates.
    #[test]
    fn with_root_creates_the_cache_directory_it_will_sweep() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("nested").join("webapp_cache");
        assert!(
            !root.exists(),
            "premise: the root must be missing, or creating it proves nothing"
        );

        let cache = WebappCache::with_root(root.clone());

        assert!(
            root.is_dir(),
            "with_root must create the directory (and its parents) it will \
             unpack into and sweep"
        );
        assert_eq!(
            cache.root(),
            root.as_path(),
            "and must still be rooted exactly where it was told"
        );
    }

    /// A root that already exists as a FILE must not panic the server at
    /// startup.
    ///
    /// This is one of the two shapes the eager `create_dir_all` exists to
    /// surface (the other is an unwritable path). Both are operator
    /// misconfigurations, and both leave the webapp cache non-functional, but
    /// neither is fatal to the node: everything except web-contract serving is
    /// unaffected, so the correct response is a warning naming the path, not a
    /// refusal to start. A future `.expect()` here would take a node down over
    /// a stray file.
    #[test]
    fn with_root_tolerates_a_root_that_is_not_a_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path().join("webapp_cache");
        std::fs::write(&root, b"not a directory").expect("seed a file at the root path");

        let cache = WebappCache::with_root(root.clone());

        assert_eq!(
            cache.root(),
            root.as_path(),
            "construction must succeed and keep the configured root"
        );
        assert!(
            root.is_file(),
            "and must not have replaced the operator's file with a directory"
        );
    }

    /// Boundary: a cache whose total is exactly the budget is left untouched.
    #[tokio::test]
    async fn webapp_cache_sweep_is_noop_at_or_under_budget() {
        let root = tempfile::tempdir().expect("tempdir");
        let (old, new) = (cache_id(1, 0), cache_id(1, 1));
        let old_size = seed_cache_entry(root.path(), &old, 4096, Duration::from_secs(86_400));
        let new_size = seed_cache_entry(root.path(), &new, 4096, Duration::from_secs(60));

        let sweep =
            enforce_webapp_cache_budget(&cache(root.path(), old_size + new_size), None).await;

        assert_eq!(sweep.total_before, old_size + new_size);
        assert!(
            sweep.evicted.is_empty(),
            "a cache exactly at budget must not evict: {sweep:?}"
        );
        assert!(dir_present(root.path(), &old) && dir_present(root.path(), &new));
    }

    /// The core property: victims are chosen oldest-USE-first, and the sweep
    /// stops as soon as the cache fits.
    #[tokio::test]
    async fn webapp_cache_sweep_evicts_least_recently_used_first() {
        let root = tempfile::tempdir().expect("tempdir");
        let ids: Vec<_> = (0..4).map(|slot| cache_id(2, slot)).collect();
        // Oldest first: 4 days, 3 days, 2 days, 1 hour.
        let ages = [
            Duration::from_secs(4 * 86_400),
            Duration::from_secs(3 * 86_400),
            Duration::from_secs(2 * 86_400),
            Duration::from_secs(3_600),
        ];
        let mut size = 0;
        for (id, age) in ids.iter().zip(ages) {
            size = seed_cache_entry(root.path(), id, 4096, age);
        }

        // Budget fits exactly two entries, so the two coldest must go.
        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size * 2), None).await;

        assert_eq!(sweep.evicted, vec![ids[0], ids[1]], "sweep: {sweep:?}");
        assert_eq!(sweep.bytes_freed, size * 2);
        assert!(!dir_present(root.path(), &ids[0]));
        assert!(!dir_present(root.path(), &ids[1]));
        assert!(dir_present(root.path(), &ids[2]));
        assert!(dir_present(root.path(), &ids[3]));
    }

    /// Eviction must be least-recently-USED, not least-recently-created:
    /// refreshing the on-disk marker for the oldest-created entry has to move it
    /// to the front of the keep set. This is the end-to-end proof that
    /// `persist_cache_access_marker` feeds the ranking `scan_webapp_cache` reads.
    #[tokio::test]
    async fn webapp_cache_access_marker_makes_an_old_entry_most_recently_used() {
        let root = tempfile::tempdir().expect("tempdir");
        let (oldest, middle, newest) = (cache_id(3, 0), cache_id(3, 1), cache_id(3, 2));
        let size = seed_cache_entry(root.path(), &oldest, 4096, Duration::from_secs(30 * 86_400));
        seed_cache_entry(root.path(), &middle, 4096, Duration::from_secs(86_400));
        seed_cache_entry(root.path(), &newest, 4096, Duration::from_secs(3_600));

        // The oldest-created entry is the one being used right now.
        persist_cache_access_marker(root.path().join(format!("{}.hash", oldest.encode()))).await;

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size), None).await;

        assert_eq!(
            sweep.evicted,
            vec![middle, newest],
            "the touched entry must survive as most-recently-used: {sweep:?}"
        );
        assert!(dir_present(root.path(), &oldest));
    }

    /// The entry whose request triggered the sweep is never its own victim,
    /// even when it is the coldest thing on disk.
    #[tokio::test]
    async fn webapp_cache_sweep_never_evicts_the_entry_in_use() {
        let root = tempfile::tempdir().expect("tempdir");
        let (in_use, other) = (cache_id(4, 0), cache_id(4, 1));
        let size = seed_cache_entry(root.path(), &in_use, 4096, Duration::from_secs(30 * 86_400));
        seed_cache_entry(root.path(), &other, 4096, Duration::from_secs(3_600));

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size), Some(in_use)).await;

        assert_eq!(sweep.evicted, vec![other], "sweep: {sweep:?}");
        assert!(dir_present(root.path(), &in_use));
        assert!(!dir_present(root.path(), &other));
    }

    /// An entry a request touched moments ago is protected even though the
    /// request holds no lock — this is the in-flight guard for the serve paths,
    /// which read the unpacked files without taking `CONTRACT_CACHE_LOCKS`.
    #[tokio::test]
    async fn webapp_cache_sweep_skips_recently_accessed_entry() {
        let root = tempfile::tempdir().expect("tempdir");
        let (serving, other) = (cache_id(5, 0), cache_id(5, 1));
        let size = seed_cache_entry(
            root.path(),
            &serving,
            4096,
            Duration::from_secs(30 * 86_400),
        );
        seed_cache_entry(root.path(), &other, 4096, Duration::from_secs(3_600));

        record_cache_access(serving);
        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size), None).await;

        assert_eq!(sweep.evicted, vec![other], "sweep: {sweep:?}");
        assert!(dir_present(root.path(), &serving));
    }

    /// The in-flight exemption is time-bounded (AGENTS.md: GC exemptions must
    /// expire). Once `WEBAPP_CACHE_EVICTION_MIN_IDLE` has passed, the same entry
    /// is evictable again — otherwise a single visit would pin a webapp forever.
    #[tokio::test(start_paused = true)]
    async fn webapp_cache_sweep_access_exemption_expires() {
        let root = tempfile::tempdir().expect("tempdir");
        let (served, other) = (cache_id(6, 0), cache_id(6, 1));
        let size = seed_cache_entry(root.path(), &served, 4096, Duration::from_secs(30 * 86_400));
        seed_cache_entry(root.path(), &other, 4096, Duration::from_secs(3_600));

        // Pin the window itself, not just that *some* window elapses: the
        // advance below is expressed in terms of the constant, so without this
        // the test would keep passing if the exemption were widened to
        // effectively-permanent. It must outlast the 30s network fetch in
        // `ensure_contract_cached` and stay far short of a browsing session.
        assert!(
            WEBAPP_CACHE_EVICTION_MIN_IDLE > Duration::from_secs(30)
                && WEBAPP_CACHE_EVICTION_MIN_IDLE < Duration::from_secs(3_600),
            "in-flight exemption is not a sane finite window: {WEBAPP_CACHE_EVICTION_MIN_IDLE:?}"
        );

        record_cache_access(served);
        let protected = enforce_webapp_cache_budget(&cache(root.path(), size), None).await;
        assert_eq!(protected.evicted, vec![other], "sweep: {protected:?}");

        tokio::time::advance(WEBAPP_CACHE_EVICTION_MIN_IDLE + Duration::from_secs(1)).await;
        let expired = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(expired.evicted, vec![served], "sweep: {expired:?}");
        assert!(!dir_present(root.path(), &served));
    }

    /// An eviction must never race a re-extraction: while `unpack_if_stale`
    /// holds a contract's cache lock, the sweep leaves that entry alone and
    /// takes the next-coldest victim instead.
    #[tokio::test]
    async fn webapp_cache_sweep_skips_entry_with_unpack_in_flight() {
        let root = tempfile::tempdir().expect("tempdir");
        let (unpacking, other) = (cache_id(7, 0), cache_id(7, 1));
        let size = seed_cache_entry(
            root.path(),
            &unpacking,
            4096,
            Duration::from_secs(30 * 86_400),
        );
        seed_cache_entry(root.path(), &other, 4096, Duration::from_secs(3_600));

        let guard = acquire_cache_lock(&unpacking).await;
        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size), None).await;
        drop(guard);

        assert_eq!(sweep.evicted, vec![other], "sweep: {sweep:?}");
        assert!(dir_present(root.path(), &unpacking));
    }

    /// Evicting must remove the `{key}.hash` sentinel as well as the tree. A
    /// leftover sentinel reads as a WARM cache over an empty directory, which
    /// would 404 every request until the contract's state happened to change.
    #[tokio::test]
    async fn webapp_cache_sweep_removes_sentinel_with_directory() {
        let root = tempfile::tempdir().expect("tempdir");
        let evicted = cache_id(8, 0);
        seed_cache_entry(
            root.path(),
            &evicted,
            4096,
            Duration::from_secs(30 * 86_400),
        );

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(sweep.evicted, vec![evicted], "sweep: {sweep:?}");
        assert!(!dir_present(root.path(), &evicted));
        assert!(
            !sentinel_present(root.path(), &evicted),
            "sentinel left behind would make the empty cache read as warm"
        );
    }

    /// `refresh_cache_if_due` short-circuits on a fresh `CONTRACT_CACHE_REFRESH`
    /// timer alone, so an evicted contract that kept its timer would serve 404s
    /// from the emptied directory for the rest of the TTL window.
    #[tokio::test]
    async fn webapp_cache_sweep_clears_refresh_timer_for_evicted_entry() {
        let root = tempfile::tempdir().expect("tempdir");
        let evicted = cache_id(9, 0);
        seed_cache_entry(
            root.path(),
            &evicted,
            4096,
            Duration::from_secs(30 * 86_400),
        );
        CONTRACT_CACHE_REFRESH.insert(evicted, Instant::now());

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(sweep.evicted, vec![evicted], "sweep: {sweep:?}");
        assert!(
            !CONTRACT_CACHE_REFRESH.contains_key(&evicted),
            "an evicted contract must not keep a fresh reconcile timer"
        );
        assert!(
            !WEBAPP_CACHE_ACCESS.contains_key(&evicted),
            "an evicted contract must not keep an access record"
        );
    }

    /// Resilience: one entry that cannot be removed must not abort the sweep.
    /// The failure is injected by replacing a sentinel with a directory, so
    /// `remove_file` fails deterministically (EISDIR) for any user on any
    /// platform — no permission tricks that root would bypass.
    ///
    /// The unremovable entry is also left INTACT rather than half-deleted: the
    /// alternative (drop the tree, keep the sentinel) is the warm-but-empty
    /// shape that 404s.
    #[tokio::test]
    async fn webapp_cache_sweep_continues_after_entry_removal_failure() {
        let root = tempfile::tempdir().expect("tempdir");
        let (broken, other) = (cache_id(10, 0), cache_id(10, 1));
        let size = seed_cache_entry(root.path(), &broken, 4096, Duration::from_secs(30 * 86_400));
        seed_cache_entry(root.path(), &other, 4096, Duration::from_secs(3_600));

        // Replace the sentinel with a directory of the same name.
        let sentinel = root.path().join(format!("{}.hash", broken.encode()));
        std::fs::remove_file(&sentinel).expect("remove sentinel");
        std::fs::create_dir(&sentinel).expect("sentinel as dir");
        // Sentinel gone as a file, so the entry ranks on its directory mtime.
        set_marker_age(
            &root.path().join(broken.encode()),
            Duration::from_secs(30 * 86_400),
        );

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), size), None).await;

        assert_eq!(
            sweep.evicted,
            vec![other],
            "a failed removal must not abort the sweep: {sweep:?}"
        );
        assert!(
            dir_present(root.path(), &broken),
            "an entry whose sentinel cannot be removed must be left intact"
        );
        assert!(!dir_present(root.path(), &other));
    }

    /// The sweep owns only `<base58>` / `<base58>.hash` pairs: anything else in
    /// the cache root is neither counted nor deleted.
    #[tokio::test]
    async fn webapp_cache_sweep_ignores_unrecognized_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let known = cache_id(11, 0);
        let size = seed_cache_entry(root.path(), &known, 4096, Duration::from_secs(30 * 86_400));
        std::fs::write(root.path().join("README.txt"), vec![b'z'; 8192]).expect("write stray file");
        let stray_dir = root.path().join("not a contract key");
        std::fs::create_dir(&stray_dir).expect("stray dir");
        std::fs::write(stray_dir.join("payload.bin"), vec![b'z'; 8192]).expect("stray payload");

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(
            sweep.total_before, size,
            "unrecognized paths must not be accounted: {sweep:?}"
        );
        assert_eq!(sweep.evicted, vec![known], "sweep: {sweep:?}");
        assert!(root.path().join("README.txt").exists());
        assert!(stray_dir.join("payload.bin").exists());
    }

    /// Scale edge case: when every entry is protected the sweep leaves the cache
    /// over budget rather than deleting something in use, and returns normally
    /// (the next sweep retries).
    #[tokio::test]
    async fn webapp_cache_sweep_stays_over_budget_when_all_entries_protected() {
        let root = tempfile::tempdir().expect("tempdir");
        let (first, second) = (cache_id(12, 0), cache_id(12, 1));
        seed_cache_entry(root.path(), &first, 4096, Duration::from_secs(30 * 86_400));
        seed_cache_entry(root.path(), &second, 4096, Duration::from_secs(30 * 86_400));
        record_cache_access(first);
        record_cache_access(second);

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert!(sweep.evicted.is_empty(), "sweep: {sweep:?}");
        assert_eq!(sweep.bytes_freed, 0);
        assert!(dir_present(root.path(), &first) && dir_present(root.path(), &second));
    }

    /// A missing cache root must read as "no entries", not blow up. Asserted on
    /// the scan directly: a panic inside the sweep's `spawn_blocking` would be
    /// caught by the `JoinError` arm and reported as an empty sweep, so the
    /// sweep-level assertion below cannot tell graceful handling from a
    /// swallowed panic.
    #[test]
    fn webapp_cache_scan_of_missing_root_is_empty_not_a_panic() {
        let root = tempfile::tempdir().expect("tempdir");
        assert!(scan_webapp_cache(&root.path().join("does-not-exist")).is_empty());
    }

    /// An empty cache root must not panic or report anything to evict.
    #[tokio::test]
    async fn webapp_cache_sweep_handles_empty_and_missing_root() {
        let root = tempfile::tempdir().expect("tempdir");
        let empty = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;
        assert_eq!(empty.total_before, 0);
        assert!(empty.evicted.is_empty());

        let missing =
            enforce_webapp_cache_budget(&cache(&root.path().join("does-not-exist"), 0), None).await;
        assert_eq!(missing.total_before, 0);
        assert!(missing.evicted.is_empty());
    }

    /// The on-disk marker refresh is throttled: the first access of a window
    /// persists, subsequent ones don't, and a new window persists again. Without
    /// the throttle every subresource of every page load would pay an
    /// `utimensat`.
    #[tokio::test(start_paused = true)]
    async fn webapp_cache_access_marker_refresh_is_throttled() {
        let id = cache_id(13, 0);
        WEBAPP_CACHE_ACCESS.remove(&id);

        assert!(
            record_cache_access(id),
            "first access of an entry must persist the marker"
        );
        assert!(
            !record_cache_access(id),
            "a second access in the same window must not re-touch the marker"
        );

        tokio::time::advance(WEBAPP_CACHE_ACCESS_TOUCH_INTERVAL - Duration::from_secs(1)).await;
        assert!(!record_cache_access(id), "still inside the throttle window");

        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            record_cache_access(id),
            "a new window must persist the marker again"
        );
    }

    /// The marker refresh must actually move the sentinel's mtime forward (this
    /// is what makes the ranking survive a restart), and must not disturb its
    /// contents — those are the state hash `unpack_if_stale` compares against.
    #[tokio::test]
    async fn webapp_cache_access_marker_updates_mtime_without_touching_contents() {
        let root = tempfile::tempdir().expect("tempdir");
        let id = cache_id(14, 0);
        seed_cache_entry(root.path(), &id, 1024, Duration::from_secs(30 * 86_400));
        let sentinel = root.path().join(format!("{}.hash", id.encode()));
        let before = std::fs::metadata(&sentinel)
            .and_then(|meta| meta.modified())
            .expect("sentinel mtime");

        persist_cache_access_marker(sentinel.clone()).await;

        let after = std::fs::metadata(&sentinel)
            .and_then(|meta| meta.modified())
            .expect("sentinel mtime");
        assert!(after > before, "marker refresh must move the mtime forward");
        assert_eq!(
            std::fs::read(&sentinel).expect("sentinel contents"),
            0u64.to_be_bytes(),
            "the state hash must survive a marker refresh"
        );
    }

    /// A marker refresh against a cold cache (no sentinel yet) is a no-op, not
    /// an error that could fail a user's request.
    #[tokio::test]
    async fn webapp_cache_access_marker_tolerates_missing_sentinel() {
        let root = tempfile::tempdir().expect("tempdir");
        persist_cache_access_marker(root.path().join("absent.hash")).await;
    }

    /// `from_base58` is not a strict filter — stdlib zero-pads a short decode
    /// instead of rejecting it, so ordinary directory names made of base58
    /// characters (`tmp`, `data`, `assets`) parse into well-formed but WRONG
    /// ids. Without the round-trip check the sweep would charge those bytes to
    /// a phantom entry, "evict" a path that does not exist, and count bytes it
    /// never freed — reporting success while staying over budget.
    #[tokio::test]
    async fn webapp_cache_sweep_ignores_names_that_zero_pad_into_valid_ids() {
        // Guard the premise: if stdlib ever made `from_base58` strict, this
        // test would silently stop covering anything.
        let padded =
            ContractInstanceId::from_base58("tmp").expect("stdlib zero-pads short decodes");
        assert_ne!(
            padded.encode(),
            "tmp",
            "premise: a short base58 name must decode to a DIFFERENT id"
        );

        let root = tempfile::tempdir().expect("tempdir");
        let known = cache_id(15, 0);
        let size = seed_cache_entry(root.path(), &known, 4096, Duration::from_secs(30 * 86_400));
        for stray in ["tmp", "data", "assets"] {
            let dir = root.path().join(stray);
            std::fs::create_dir(&dir).expect("stray dir");
            std::fs::write(dir.join("payload.bin"), vec![b'z'; 8192]).expect("stray payload");
        }

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(
            sweep.total_before, size,
            "base58-parseable non-entries must not be accounted: {sweep:?}"
        );
        assert_eq!(sweep.evicted, vec![known], "sweep: {sweep:?}");
        assert_eq!(
            sweep.bytes_freed, size,
            "bytes_freed must only count entries actually deleted: {sweep:?}"
        );
        for stray in ["tmp", "data", "assets"] {
            assert!(root.path().join(stray).join("payload.bin").exists());
        }
    }

    /// Concurrent sweeps must not each evict a full deficit's worth. Each takes
    /// its own `live` snapshot, so without the in-progress gate N simultaneous
    /// unpacks drive the cache well below budget and over-report `bytes_freed`.
    #[tokio::test]
    async fn webapp_cache_concurrent_sweeps_do_not_over_evict() {
        let root = tempfile::tempdir().expect("tempdir");
        let ids: Vec<_> = (0..6).map(|slot| cache_id(16, slot)).collect();
        let mut size = 0;
        for (offset, id) in ids.iter().enumerate() {
            size = seed_cache_entry(
                root.path(),
                id,
                4096,
                Duration::from_secs((30 - offset as u64) * 86_400),
            );
        }
        // Budget for 4 of the 6 entries, so a single correct sweep evicts 2.
        let shared = cache(root.path(), size * 4);
        let in_use = ids[5];

        let mut sweeps = Vec::new();
        for _ in 0..4 {
            let shared = shared.clone();
            sweeps.push(tokio::spawn(async move {
                maybe_enforce_webapp_cache_budget(&shared, in_use, SweepTrigger::Unpack).await;
            }));
        }
        for sweep in sweeps {
            sweep.await.expect("sweep task must not panic");
        }

        let survivors = ids.iter().filter(|id| dir_present(root.path(), id)).count();
        assert_eq!(
            survivors, 4,
            "concurrent sweeps must together evict the deficit exactly once"
        );
    }

    /// The debounce decision, isolated from the filesystem. An unpack grew the
    /// cache so it always sweeps; a reconcile rewrote nothing so it waits out
    /// `WEBAPP_CACHE_SWEEP_INTERVAL`, otherwise every contract's 30-second
    /// refresh would pay for a directory walk.
    #[tokio::test(start_paused = true)]
    async fn webapp_cache_sweep_is_due_debounces_only_reconciles() {
        let now = Instant::now();
        assert!(
            sweep_is_due(SweepTrigger::Reconcile, None, now),
            "a never-swept cache is due"
        );
        assert!(
            !sweep_is_due(SweepTrigger::Reconcile, Some(now), now),
            "a reconcile right after a sweep must be debounced"
        );
        assert!(
            sweep_is_due(SweepTrigger::Unpack, Some(now), now),
            "an unpack grew the cache, so it always sweeps"
        );
        assert!(
            !sweep_is_due(
                SweepTrigger::Reconcile,
                Some(now),
                now + WEBAPP_CACHE_SWEEP_INTERVAL - Duration::from_secs(1)
            ),
            "still inside the debounce window"
        );
        assert!(
            sweep_is_due(
                SweepTrigger::Reconcile,
                Some(now),
                now + WEBAPP_CACHE_SWEEP_INTERVAL
            ),
            "the debounce window must expire"
        );
    }

    // -------------------------------------------------------------------------
    // Wiring: the size bound has to actually RUN, and the in-flight guard has to
    // actually ARM, on the real handler paths. Everything above tests the sweep
    // in isolation, so without these the whole feature could be deleted from
    // `unpack_if_stale` / `refresh_cache_if_due` with a green suite.
    // -------------------------------------------------------------------------

    /// Drives the real reconcile path — `refresh_cache_if_due` →
    /// `ensure_contract_cached` → `handle_get_response` → `unpack_if_stale`
    /// (matching-hash early return) — and asserts the budget sweep ran.
    ///
    /// Pins the `SweepTrigger::Reconcile` call site: delete it and the
    /// over-budget decoys below survive.
    #[tokio::test]
    async fn reconcile_path_enforces_the_webapp_cache_budget() {
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![7, 7, 7, 7])),
            Parameters::from(vec![1]),
        )));
        let instance_id = *contract.key().id();
        let state = WrappedState::new(vec![4, 4, 4]);

        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), SENTINEL_BYTES);
        clear_cache(&instance_id).await;
        WEBAPP_CACHE_ACCESS.remove(&instance_id);

        // Warm + matching hash ⇒ `unpack_if_stale` takes its early return, so
        // this exercises the RECONCILE trigger rather than the unpack one.
        std::fs::create_dir_all(webapp_cache.entry_dir(&instance_id)).expect("entry dir");
        std::fs::write(
            webapp_cache.hash_path(&instance_id),
            hash_state(state.as_ref()).to_be_bytes(),
        )
        .expect("sentinel");

        // Decoys the sweep must evict to get under the (tiny) budget.
        let decoys: Vec<_> = (0..2).map(|slot| cache_id(17, slot)).collect();
        for decoy in &decoys {
            seed_cache_entry(root.path(), decoy, 4096, Duration::from_secs(30 * 86_400));
        }

        let (sender, mut rx) = request_channel();
        let handler = {
            let webapp_cache = webapp_cache.clone();
            tokio::spawn(async move {
                refresh_cache_if_due(instance_id, &sender, &webapp_cache)
                    .await
                    .map(|_| ())
            })
        };
        serve_one_get(&mut rx, &contract, &state).await;
        handler
            .await
            .expect("handler must not panic")
            .expect("reconcile must succeed");

        for decoy in &decoys {
            assert!(
                !dir_present(root.path(), decoy),
                "the reconcile path must enforce the size bound"
            );
        }
        assert!(
            dir_present(root.path(), &instance_id),
            "the contract being reconciled must never be its own sweep's victim"
        );
    }

    /// Same wiring, one layer down and on the UNPACK trigger: `unpack_if_stale`
    /// re-extracts a real web archive and must then sweep. Pins the
    /// `SweepTrigger::Unpack` call site.
    #[tokio::test]
    async fn unpack_enforces_the_webapp_cache_budget() {
        let (contract, state) = webapp_contract_and_state(&[0xa1]);
        let instance_id = *contract.key().id();

        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), SENTINEL_BYTES);
        clear_cache(&instance_id).await;
        WEBAPP_CACHE_ACCESS.remove(&instance_id);

        let decoys: Vec<_> = (0..2).map(|slot| cache_id(18, slot)).collect();
        for decoy in &decoys {
            seed_cache_entry(root.path(), decoy, 4096, Duration::from_secs(30 * 86_400));
        }

        // No sentinel ⇒ a genuine unpack, which is the only event that grows
        // the cache and therefore always sweeps.
        unpack_if_stale(&contract, state.as_ref(), &webapp_cache)
            .await
            .expect("unpack must succeed");

        assert!(
            webapp_cache.hash_path(&instance_id).exists(),
            "premise: the unpack must have actually happened"
        );
        for decoy in &decoys {
            assert!(
                !dir_present(root.path(), decoy),
                "an unpack must enforce the size bound"
            );
        }
    }

    /// The in-flight guard has to arm on the serve path: `refresh_cache_if_due`
    /// must record the access for a warm entry, otherwise a concurrent sweep has
    /// nothing telling it the entry is being read right now. Pins the
    /// `note_cache_access` call site in `refresh_cache_if_due`.
    #[tokio::test]
    async fn serving_a_warm_entry_marks_it_in_use() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xc1;
        bytes[1] = 0x01;
        let instance_id = ContractInstanceId::new(bytes);

        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), u64::MAX);
        clear_cache(&instance_id).await;
        WEBAPP_CACHE_ACCESS.remove(&instance_id);

        std::fs::create_dir_all(webapp_cache.entry_dir(&instance_id)).expect("entry dir");
        std::fs::write(webapp_cache.hash_path(&instance_id), 0u64.to_be_bytes()).expect("sentinel");
        // Fresh reconcile timer ⇒ the warm fast path returns before any fetch,
        // so the access record is the only thing this can be observing.
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        let (sender, _rx) = request_channel();
        refresh_cache_if_due(instance_id, &sender, &webapp_cache)
            .await
            .expect("warm fast path must succeed");

        assert!(
            accessed_recently(&instance_id),
            "serving a warm entry must mark it in use for the eviction guard"
        );
    }

    /// Same, for the shell root: `contract_home` fetches and then serves, so it
    /// must mark the entry in use too. Pins the `note_cache_access` call site in
    /// `contract_home_in`.
    #[tokio::test]
    async fn contract_home_marks_the_entry_in_use() {
        let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(vec![3, 1, 4, 1])),
            Parameters::from(vec![5, 9]),
        )));
        let instance_id = *contract.key().id();
        let state = WrappedState::new(vec![2, 6, 5]);

        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), u64::MAX);
        clear_cache(&instance_id).await;
        WEBAPP_CACHE_ACCESS.remove(&instance_id);

        // Matching hash ⇒ no unpack needed; we only care about the marking.
        std::fs::create_dir_all(webapp_cache.entry_dir(&instance_id)).expect("entry dir");
        std::fs::write(
            webapp_cache.hash_path(&instance_id),
            hash_state(state.as_ref()).to_be_bytes(),
        )
        .expect("sentinel");

        let (sender, mut rx) = request_channel();
        let key = instance_id.to_string();
        let handler = {
            let webapp_cache = webapp_cache.clone();
            tokio::spawn(async move {
                contract_home(
                    key,
                    sender,
                    AuthToken::generate(),
                    ApiVersion::V1,
                    None,
                    None,
                    false,
                    &webapp_cache,
                )
                .await
                .map(|_| ())
            })
        };
        serve_one_get(&mut rx, &contract, &state).await;
        handler
            .await
            .expect("handler must not panic")
            .expect("contract_home must succeed");

        assert!(
            accessed_recently(&instance_id),
            "contract_home must mark the entry in use for the eviction guard"
        );
    }

    /// Cross-process regression. The cache directory is per-USER but the guards
    /// are per-process, and the documented multi-peer setup runs several nodes
    /// as one user. When another process evicts an entry, this process's
    /// reconcile timer is still fresh and knows nothing about it — so returning
    /// on the timer alone served 404s out of the emptied directory for the rest
    /// of the TTL window. The re-stat under the refresh lock must notice the
    /// entry is gone and refetch.
    #[tokio::test]
    async fn eviction_by_another_process_forces_a_refetch_despite_a_fresh_timer() {
        // A real archive: the entry is genuinely cold here, so the refetch this
        // test is asserting on runs a real unpack rather than the matching-hash
        // early return.
        let (contract, state) = webapp_contract_and_state(&[0xb2]);
        let instance_id = *contract.key().id();

        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), u64::MAX);
        clear_cache(&instance_id).await;
        WEBAPP_CACHE_ACCESS.remove(&instance_id);

        // The state another process left behind: entry gone from disk, but OUR
        // reconcile timer still fresh (its `CONTRACT_CACHE_REFRESH.remove` only
        // reached its own process).
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());
        assert!(
            !webapp_cache.hash_path(&instance_id).exists(),
            "premise: the entry must be absent"
        );

        let (sender, mut rx) = request_channel();
        let handler = {
            let webapp_cache = webapp_cache.clone();
            tokio::spawn(async move {
                refresh_cache_if_due(instance_id, &sender, &webapp_cache)
                    .await
                    .map(|_| ())
            })
        };

        // Cold cache with a free speculative-fetch permit, so the GET goes out
        // directly — no presence query ahead of it.
        serve_one_get(&mut rx, &contract, &state).await;
        handler
            .await
            .expect("handler must not panic")
            .expect("refresh must succeed");

        assert!(
            webapp_cache.hash_path(&instance_id).exists(),
            "a fresh timer must not suppress the refetch of an entry another \
             process evicted — otherwise the request 404s for the rest of the TTL"
        );
        // Pins the COLD-fetch `note_cache_access`, which no other test reaches:
        // the entry was cold, so the warm-path call is skipped and this is the
        // only writer. Without it a freshly-fetched entry is unprotected between
        // the fetch and the caller's read of the files.
        assert!(
            accessed_recently(&instance_id),
            "a contract fetched to populate a cold entry must be marked in use \
             before the caller reads it"
        );
    }

    /// The reconcile debounce has to be wired into the sweep gate, not merely
    /// exist: `sweep_is_due` is unit-tested in isolation, so dropping the call
    /// to it would leave every 30-second refresh of every contract paying for a
    /// full recursive directory walk.
    #[tokio::test]
    async fn reconcile_sweeps_are_debounced_in_practice() {
        let root = tempfile::tempdir().expect("tempdir");
        let webapp_cache = cache(root.path(), 0);
        let in_use = cache_id(19, 0);

        let first = cache_id(19, 1);
        seed_cache_entry(root.path(), &first, 4096, Duration::from_secs(30 * 86_400));
        maybe_enforce_webapp_cache_budget(&webapp_cache, in_use, SweepTrigger::Reconcile).await;
        assert!(
            !dir_present(root.path(), &first),
            "premise: the first reconcile must sweep, or the debounce below \
             proves nothing"
        );

        // Re-seed and immediately reconcile again. The budget is still 0, so a
        // sweep that ran would evict — the debounce is the only thing that can
        // keep this entry alive.
        let second = cache_id(19, 2);
        seed_cache_entry(root.path(), &second, 4096, Duration::from_secs(30 * 86_400));
        maybe_enforce_webapp_cache_budget(&webapp_cache, in_use, SweepTrigger::Reconcile).await;

        assert!(
            dir_present(root.path(), &second),
            "a second reconcile inside WEBAPP_CACHE_SWEEP_INTERVAL must skip the \
             sweep entirely"
        );
    }

    /// One sweep deletes at most `WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP`, so the
    /// first sweep on a node upgrading with an unbounded legacy cache cannot
    /// stall a request behind an unbounded number of `remove_dir_all`s. The
    /// remainder is left for the next sweep rather than dropped.
    #[tokio::test]
    async fn webapp_cache_sweep_caps_evictions_per_pass() {
        let root = tempfile::tempdir().expect("tempdir");
        let over_cap = WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP + 3;
        let ids: Vec<_> = (0..over_cap).map(|slot| cache_id(20, slot as u8)).collect();
        for (offset, id) in ids.iter().enumerate() {
            seed_cache_entry(
                root.path(),
                id,
                4096,
                Duration::from_secs((over_cap - offset) as u64 * 86_400),
            );
        }

        let sweep = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;

        assert_eq!(
            sweep.evicted.len(),
            WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP,
            "one sweep must not delete more than the cap: {sweep:?}"
        );
        // The cap must take the COLDEST entries, not an arbitrary prefix.
        assert_eq!(
            sweep.evicted,
            ids[..WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP],
            "the capped sweep must still evict least-recently-used first"
        );
        let survivors = ids.iter().filter(|id| dir_present(root.path(), id)).count();
        assert_eq!(survivors, over_cap - WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP);

        // Still over budget, so the next sweep picks up where this one stopped.
        let next = enforce_webapp_cache_budget(&cache(root.path(), 0), None).await;
        assert_eq!(
            next.evicted.len(),
            over_cap - WEBAPP_CACHE_MAX_EVICTIONS_PER_SWEEP,
            "the remainder must be evicted by the following sweep: {next:?}"
        );
    }

    /// Regression test for #3940 and #5406. `variable_content` must trigger a
    /// network fetch when the contract's webapp cache is cold, WITHOUT any
    /// prior local trace of the contract. This is the real cross-contract
    /// scenario: a page on contract Delta `<img>`s a SEPARATE contract X, and
    /// the reader has never visited X — not at its root, not through any other
    /// page — so X is in neither the contract store nor the subscription set.
    ///
    /// Prior to #3942 a cold-cache subpath request returned 404; #3942 made it
    /// fetch; #4417 narrowed that to locally-present instances only, which put
    /// this case back to 404 (#5406); the speculative-fetch lane restores it
    /// under a concurrency bound.
    ///
    /// Load-bearing in two directions. It asserts the `NewConnection` +
    /// `Request(Get)` fetch pair is what the handler emits, so re-introducing a
    /// presence gate ahead of the fetch fails here (the first message would be
    /// the `NodeDiagnostics` query). And the permit must be tried before the
    /// presence query, not after, or every cold request pays a node round trip.
    /// The fetch is cancelled mid-flight (we don't deliver a response) so the
    /// test stays bounded. See
    /// `variable_content_skips_fetch_for_unknown_instance_when_lane_is_full`
    /// for the bound's security side.
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
                    &test_webapp_cache(),
                )
                .await
                .map(|_| ())
            })
        };

        // Cold cache, unknown contract, free permit → the fetch pair must be
        // the FIRST thing on the channel. A presence query here would mean the
        // #4417 gate is back.
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        // Clean up after the test — handler was aborted mid-fetch, so no
        // cache was written, but clear defensively to avoid accumulating
        // state in the shared XDG cache dir across runs.
        clear_cache(&instance_id).await;
    }

    /// Security regression for #3945, re-pinned on the speculative-fetch bound
    /// (#5406). Once every permit is in flight, a cold-cache subresource
    /// request for an UNKNOWN contract (not in the store AND not subscribed)
    /// must NOT issue a network GET — that is the random-key DoS amplification
    /// vector #3942 opened, and a spray is exactly what saturates the lane. The
    /// presence query returns empty `contract_states` and empty
    /// `subscriptions`, so the fallback reads "not known" and the handler
    /// serves a 404 from the empty cache directory, issuing no `Get`.
    ///
    /// Load-bearing: with the bound removed the handler would fall straight
    /// through to `ensure_contract_cached` and emit a `NewConnection` + `Get`,
    /// which this test's "no Get" assertion catches. The sibling
    /// `variable_content_triggers_fetch_on_cache_miss` pins the other side —
    /// that an unknown contract IS fetched while permits remain — so neither a
    /// re-tightened gate nor a dropped bound can pass both.
    #[tokio::test]
    async fn variable_content_skips_fetch_for_unknown_instance_when_lane_is_full() {
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
                    &test_webapp_cache_saturated(),
                )
                .await
                .map(|r| r.into_response())
            })
        };

        // Lane saturated → the presence query runs as the fallback. Answer it
        // as "the node has NO local presence for this contract" — empty
        // contract_states AND empty subscriptions → not locally known.
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
            "an unknown instance must NOT be fetched once the speculative-fetch \
             lane is saturated (#3945 DoS bound)"
        );

        clear_cache(&instance_id).await;
    }

    /// Fail-closed regression for #3945. With the speculative-fetch lane
    /// saturated and the presence query NEVER answered (the node accepted the
    /// transient `NewConnection` but never replies to the `NodeDiagnostics`
    /// query), `is_locally_known` must time out and read as NOT known, so the
    /// cold-cache request 404s and issues NO network GET. This is the DoS
    /// guarantee under a wedged node — without the 5s recv timeout the request
    /// task would hang forever, which under a spray of unknown keys is itself a
    /// resource-exhaustion vector.
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
                    &test_webapp_cache_saturated(),
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
                    &test_webapp_cache_saturated(),
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

    /// Fail-closed regression for #3945: with the lane saturated and the node
    /// gone entirely (the `ClientConnection` receiver is dropped, so even the
    /// presence query's `NewConnection` send fails), the cold-cache request
    /// must 404 and issue no GET. Covers the `request_sender.send(...).is_err()`
    /// branch of `is_locally_known`.
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
            &test_webapp_cache_saturated(),
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

    /// The other half of `is_locally_known`'s OR, on the saturated-lane
    /// fallback: a contract the node STORES but is not subscribed to must
    /// fetch. That is the cross-contract case a shared gateway sees most —
    /// contract X was fetched for some other reader, so this reader's `<img
    /// src>` finds it in the store — and it is the branch that would silently
    /// stop mattering if the fallback ever narrowed to subscriptions alone.
    #[tokio::test]
    async fn variable_content_triggers_fetch_for_stored_not_subscribed() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x4d;
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
                    &test_webapp_cache_saturated(),
                )
                .await
                .map(|_| ())
            })
        };

        answer_presence_query_hosted(&mut rx, instance_id).await;
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// #3945 broaden-signal coverage, on the saturated-lane fallback: a cold
    /// cache for a contract that is SUBSCRIBED but NOT in the store (e.g. the
    /// lease outlived LRU eviction) must still fetch even with every permit in
    /// flight. Proves `is_locally_known`'s OR branch — known = in-store OR
    /// subscribed — not store-presence alone, and that the fallback is a way
    /// PAST a saturated lane rather than a second gate.
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
                    &test_webapp_cache_saturated(),
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

        // Known contracts fetch even with the lane saturated.
        expect_fetch_pair(&mut rx, instance_id).await;

        handler.abort();
        clear_cache(&instance_id).await;
    }

    /// The bound is on CONCURRENCY, so a permit has to stay claimed for as long
    /// as its GET is in flight — that is the whole difference between "32
    /// speculative fetches at once" and "32 per request, unbounded in
    /// aggregate". With a one-permit lane, a second cold contract arriving
    /// while the first fetch is still outstanding must find the lane full and
    /// fall back to the presence query, and must fetch again once the first
    /// fetch's future is dropped and the permit returns.
    ///
    /// Load-bearing against two opposite mistakes: taking the permit and
    /// dropping it before `ensure_contract_cached` (the second request would
    /// fetch, and no bound would exist), and holding it past the fetch (the
    /// third request would 404 forever once the lane drained).
    #[tokio::test]
    async fn an_in_flight_fetch_holds_its_speculative_permit() {
        let webapp_cache = WebappCache {
            speculative_fetches: Arc::new(Semaphore::new(1)),
            ..test_webapp_cache()
        };

        let mut bytes = [0u8; 32];
        bytes[0] = 0x3b;
        bytes[1] = 0x01;
        let first = ContractInstanceId::new(bytes);
        bytes[1] = 0x02;
        let second = ContractInstanceId::new(bytes);
        clear_cache(&first).await;
        clear_cache(&second).await;

        let (sender, mut rx) = request_channel();
        let in_flight = {
            let (sender, webapp_cache) = (sender.clone(), webapp_cache.clone());
            tokio::spawn(async move { refresh_cache_if_due(first, &sender, &webapp_cache).await })
        };
        // The only permit is now claimed. Hold the callback sender for the rest
        // of the test so this fetch stays genuinely in flight: dropping it
        // closes the channel, which ends the fetch and returns the permit —
        // and would make the assertion below pass for the wrong reason.
        let _in_flight_callbacks = expect_fetch_pair_holding_callbacks(&mut rx, first).await;

        let blocked = {
            let (sender, webapp_cache) = (sender.clone(), webapp_cache.clone());
            tokio::spawn(async move { refresh_cache_if_due(second, &sender, &webapp_cache).await })
        };
        // Lane full → the fallback runs. Answer "not known" and it must give up
        // without a GET.
        answer_presence_query(&mut rx, second, |_query_id| empty_diagnostics()).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), blocked)
            .await
            .expect("the blocked request must resolve, not queue behind the fetch")
            .expect("handler must not panic")
            .expect("a refused speculative fetch is not an error");
        assert!(
            rx.try_recv().is_err(),
            "a request refused by the saturated lane must issue no further \
             messages, and above all no Get"
        );

        // Drop the in-flight fetch: its permit returns and the lane reopens.
        in_flight.abort();
        assert!(
            in_flight.await.is_err(),
            "the in-flight fetch must be cancelled, not have completed on its own"
        );
        let retried = {
            let (sender, webapp_cache) = (sender.clone(), webapp_cache.clone());
            tokio::spawn(async move { refresh_cache_if_due(second, &sender, &webapp_cache).await })
        };
        expect_fetch_pair(&mut rx, second).await;
        retried.abort();

        clear_cache(&first).await;
        clear_cache(&second).await;
    }

    /// A page embedding several subresources from a contract nobody can find
    /// must pay ONE network GET, not one per subresource. Each follower queues
    /// on the refresh lock, finds the cache still cold, and would otherwise
    /// fetch again — 30 images means 30 sequential GETs, each up to the 30s
    /// ceiling, for 30 identical answers. The recorded failure is what stops
    /// them, and this test is the only thing standing between that record and a
    /// future cleanup that drops it as redundant with the refresh timer (it is
    /// not: the timer is only set on SUCCESS, deliberately, so a transient
    /// failure does not suppress the next retry).
    #[tokio::test]
    async fn a_failed_cold_fetch_is_not_repeated_within_the_window() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3b;
        bytes[1] = 0x11;
        let instance_id = ContractInstanceId::new(bytes);
        clear_cache(&instance_id).await;
        CONTRACT_REFRESH_LOCKS.remove(&instance_id);

        let (sender, mut rx) = request_channel();

        // First request: fetches, and the node answers "not found".
        let first = {
            let sender = sender.clone();
            tokio::spawn(async move {
                refresh_cache_if_due(instance_id, &sender, &test_webapp_cache()).await
            })
        };
        let callbacks = expect_fetch_pair_holding_callbacks(&mut rx, instance_id).await;
        callbacks
            .send(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(ContractResponse::NotFound {
                    instance_id,
                })),
            })
            .expect("callback receiver live for the NotFound reply");
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), first)
                .await
                .expect("the first request must resolve")
                .expect("handler must not panic")
                .is_err(),
            "premise: an exhausted GET must surface as an error"
        );
        while rx.try_recv().is_ok() {} // the fetch's trailing Disconnect

        // Second request, same contract, still cold: no GET may go out.
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            refresh_cache_if_due(instance_id, &sender, &test_webapp_cache()),
        )
        .await
        .expect("the second request must resolve, not queue behind a refetch");
        assert!(
            second.is_ok(),
            "a request inside the failure window serves the empty-cache 404, it \
             does not propagate the first caller's error"
        );
        assert!(
            rx.try_recv().is_err(),
            "a cold fetch that just failed must not be repeated inside the \
             window — one dead contract, one GET"
        );

        CONTRACT_REFRESH_LOCKS.remove(&instance_id);
        clear_cache(&instance_id).await;
    }

    /// A node that accepts the fetch's connection but never assigns it an id
    /// must not pin the request — and above all must not pin the
    /// speculative-fetch permit it is holding, because a permit that never
    /// comes back drains the lane for the life of the process.
    ///
    /// Unreachable before this change: #4417's gate ran its own bounded
    /// presence query first and failed closed, so a cold request never reached
    /// the fetch against a silent node. Removing the gate is what put this
    /// handshake on the path, so the bound belongs to the same change. Found by
    /// mutation-testing the traversal check, where the test hung instead of
    /// failing.
    #[tokio::test(start_paused = true)]
    async fn a_silent_node_cannot_pin_a_speculative_permit() {
        let webapp_cache = WebappCache {
            speculative_fetches: Arc::new(Semaphore::new(1)),
            ..test_webapp_cache()
        };
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3b;
        bytes[1] = 0x21;
        let instance_id = ContractInstanceId::new(bytes);
        clear_cache(&instance_id).await;
        CONTRACT_REFRESH_LOCKS.remove(&instance_id);

        let (sender, mut rx) = request_channel();
        let handler = {
            let webapp_cache = webapp_cache.clone();
            tokio::spawn(
                async move { refresh_cache_if_due(instance_id, &sender, &webapp_cache).await },
            )
        };

        // Accept the connection, then go silent — never send `NewId`. Hold the
        // sender so the channel stays OPEN: a closed channel short-circuits the
        // recv, and the timeout is what this test is about.
        let new_conn = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("the fetch must open with NewConnection")
            .expect("channel must remain open");
        let _callbacks = match new_conn {
            ClientConnection::NewConnection { callbacks, .. } => callbacks,
            other => panic!("the fetch must open with NewConnection, got: {other:?}"),
        };

        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(60), handler)
                .await
                .expect("a silent node must not pin the request task")
                .expect("handler must not panic")
                .is_err(),
            "a fetch that never got a client id is a failed fetch"
        );
        assert_eq!(
            webapp_cache.speculative_fetches.available_permits(),
            1,
            "the permit must come back when the fetch gives up, or one wedged \
             node drains the lane for good"
        );

        CONTRACT_REFRESH_LOCKS.remove(&instance_id);
        clear_cache(&instance_id).await;
    }

    /// The production cache must actually hand out `SPECULATIVE_FETCH_LIMIT`
    /// permits. Every other lane test overrides the count, so without this a
    /// mistake in `with_root` — a zero, or a `usize::MAX` that bounds
    /// nothing — would pass the whole suite.
    #[test]
    fn with_root_opens_the_lane_at_the_declared_limit() {
        let root = tempfile::tempdir().expect("tempdir");
        let cache = WebappCache::with_root(root.path().to_path_buf());
        assert_eq!(
            cache.speculative_fetches.available_permits(),
            SPECULATIVE_FETCH_LIMIT,
            "the node's speculative-fetch lane must open at the declared limit"
        );
    }

    /// `CONTRACT_REFRESH_LOCKS` is keyed by a contract id an unauthenticated
    /// caller puts in the URL, and the entry is created before anything has
    /// decided the contract is worth fetching — so without a cap it is an
    /// unbounded per-key map an attacker grows for free by spraying keys (the
    /// per-key-collection rule in `.claude/rules/code-style.md`).
    ///
    /// Three properties, in one test because they cannot safely be separated:
    /// while this holds the table full, a CONCURRENT sibling asking for a lock
    /// correctly receives an overflow stripe instead of a table entry — which
    /// is precisely what the first half asserts against. Split across two
    /// `#[tokio::test]`s they would fail each other under plain `cargo test`,
    /// which runs them as threads in one process (`.claude/rules/testing.md`).
    ///
    /// 1. the table stays at or under its cap under a spray;
    /// 2. pruning never drops a lock another task holds or waits on, which
    ///    would let two refreshers for one contract run at once;
    /// 3. once the table is full of HELD locks, a newcomer gets a shared
    ///    overflow stripe — deterministic per contract, so it still coalesces —
    ///    rather than a private mutex, which would drop coalescing for every
    ///    request at once while nothing bounds the warm refreshes that follow.
    #[tokio::test]
    async fn refresh_lock_table_is_bounded_prunes_safely_and_overflows_per_contract() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3c;
        let held_id = ContractInstanceId::new(bytes);
        let guard = acquire_refresh_lock(&held_id).await;
        let held_lock = CONTRACT_REFRESH_LOCKS
            .get(&held_id)
            .map(|entry| entry.clone())
            .expect("the held lock must be in the table");

        let sprayed: Vec<_> = (0..(MAX_REFRESH_LOCKS + 64))
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = 0x3d;
                bytes[1..9].copy_from_slice(&(i as u64).to_be_bytes());
                ContractInstanceId::new(bytes)
            })
            .collect();
        for id in &sprayed {
            drop(acquire_refresh_lock(id).await);
        }

        assert!(
            CONTRACT_REFRESH_LOCKS.len() <= MAX_REFRESH_LOCKS,
            "a spray of unknown keys must not grow the refresh-lock table past \
             its cap, got {}",
            CONTRACT_REFRESH_LOCKS.len()
        );
        let survivor = CONTRACT_REFRESH_LOCKS
            .get(&held_id)
            .map(|entry| entry.clone())
            .expect("a held refresh lock must survive the prune");
        assert!(
            Arc::ptr_eq(&held_lock, &survivor),
            "the prune must keep the SAME mutex a task is holding, not replace \
             it — a replacement lets two refreshers for one contract run at once"
        );

        // Now fill the table with locks that are HELD, so the prune can reclaim
        // nothing and a newcomer has to take the overflow path.
        let mut held = Vec::with_capacity(MAX_REFRESH_LOCKS);
        let mut held_ids = Vec::with_capacity(MAX_REFRESH_LOCKS);
        for i in 0..MAX_REFRESH_LOCKS {
            let mut bytes = [0u8; 32];
            bytes[0] = 0x3f;
            bytes[1..9].copy_from_slice(&(i as u64).to_be_bytes());
            let id = ContractInstanceId::new(bytes);
            held.push(acquire_refresh_lock(&id).await);
            held_ids.push(id);
        }

        let mut bytes = [0u8; 32];
        bytes[0] = 0x40;
        let newcomer = ContractInstanceId::new(bytes);
        let first = refresh_lock_for(&newcomer);
        let second = refresh_lock_for(&newcomer);
        assert!(
            !CONTRACT_REFRESH_LOCKS.contains_key(&newcomer),
            "a full table must not admit another entry — that is the cap"
        );
        assert!(
            Arc::ptr_eq(&first, &second),
            "two callers for one contract must still meet on the same mutex \
             when the table is full, or the overflow path silently stops \
             coalescing every request at once"
        );
        assert!(
            Arc::ptr_eq(&first, &overflow_refresh_lock(&newcomer)),
            "the full-table path must hand out the contract's overflow stripe"
        );

        drop(guard);
        drop(held);

        // `CONTRACT_REFRESH_LOCKS` is process-global and `cargo test` runs these
        // threads in ONE process, so leaving thousands of entries behind would
        // push a sibling test's `acquire_refresh_lock` toward the overflow
        // stripes and change what it measures. Nextest would never show it (one
        // process per test) — see `.claude/rules/testing.md`.
        for id in sprayed.iter().chain(held_ids.iter()) {
            CONTRACT_REFRESH_LOCKS.remove(id);
        }
        CONTRACT_REFRESH_LOCKS.remove(&held_id);
    }

    /// #3977-interaction regression for the cold/warm split: a WARM-but-stale
    /// cache for an UNSUBSCRIBED, UNHOSTED contract must still refresh. Only a
    /// cold fetch is speculative, so a warm-but-stale refresh issues its GET
    /// without claiming a permit and without a presence query — even though the
    /// contract is not currently "known". A warm on-disk cache already proves
    /// the node legitimately fetched this contract before, so refreshing it to
    /// pick up a republish (#3977) is not the random-key amplification vector.
    /// Without this split the handler would hold warm refreshes behind the
    /// speculative bound and serve a stale bundle whenever it was saturated.
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
        let handler = tokio::spawn(async move {
            refresh_cache_if_due(instance_id, &sender, &test_webapp_cache())
                .await
                .map(|_| ())
        });

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
            &test_webapp_cache(),
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

    /// Regression for the SUB0PT1MAL/cirro CORS report (2026-07-29): an asset
    /// whose (decoded) filename contains a space must be SERVED, not rejected.
    ///
    /// The browser requests `.../my%20image.png`; axum decodes the wildcard to
    /// `my image.png`; the caller rebuilds `/v1/contract/web/{key}/my image.png`
    /// and passes it here. The old code re-parsed that as an `axum::http::Uri`,
    /// which fails on the space with a 400 — and because the sandboxed iframe's
    /// subresource fetch has a null origin, the CORS-less 400 surfaced to the
    /// app as an opaque "CORS error". The fix strips the prefix textually, so
    /// the space survives and the file is served byte-for-byte.
    #[tokio::test]
    async fn variable_content_serves_asset_with_space_in_filename() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x50;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(cache_dir.join("my image.png"), b"png-bytes-here")
            .await
            .unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        let (sender, _rx) = request_channel();
        let response = variable_content(
            key.clone(),
            // The reconstructed path carries the DECODED space, exactly as the
            // caller builds it from axum's `{*path}` wildcard.
            format!("/v1/contract/web/{key}/my image.png"),
            ApiVersion::V1,
            sender,
            &test_webapp_cache(),
        )
        .await
        .expect("a spaced filename must not be rejected as an invalid URI")
        .into_response();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "asset with a space in its name must serve 200, not 400"
        );
        let body = response_body(response).await;
        assert_eq!(body, "png-bytes-here", "must serve the primed file bytes");

        clear_cache(&instance_id).await;
    }

    /// Security regression: a `../`-style traversal in the (decoded) asset path
    /// must NOT read a file outside the contract's cache directory.
    ///
    /// The overflow fallback must stay per-contract. When the lock table is
    /// full, giving each caller a private mutex would drop coalescing for every
    /// request at once — and a warm-but-stale refresh takes no speculative
    /// permit, so nothing else would bound the duplicate GETs that follow.
    /// Striping by contract id keeps two callers for one contract on one mutex.
    #[test]
    fn overflow_refresh_locks_are_shared_per_contract() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3e;
        let id = ContractInstanceId::new(bytes);
        bytes[1] = 0x01;
        let other = ContractInstanceId::new(bytes);

        assert!(
            Arc::ptr_eq(&overflow_refresh_lock(&id), &overflow_refresh_lock(&id)),
            "one contract must always land on the same overflow stripe, or the \
             overflow state stops coalescing anything"
        );
        // Not an assertion that these two differ — a hash collision is legal —
        // only that the stripes are actually distinguishing contracts at all.
        let distinct: std::collections::HashSet<_> = (0u8..64)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[0] = 0x3e;
                bytes[2] = i;
                Arc::as_ptr(&overflow_refresh_lock(&ContractInstanceId::new(bytes)))
            })
            .collect();
        assert!(
            distinct.len() > 1,
            "the overflow stripes must spread contracts, not funnel them onto one"
        );
        let _ = other;
    }

    /// The sandbox handler must refuse a traversal before fetching too. The
    /// sibling assertion for `variable_content`; without it, moving only ONE of
    /// the two checks back after the fetch passes the suite, and this path is
    /// reachable with an arbitrary key just like the other.
    #[tokio::test]
    async fn serve_sandbox_content_rejects_traversal_before_fetching() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x56;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        // Cold cache: without the early check this request WOULD fetch.
        clear_cache(&instance_id).await;
        CONTRACT_REFRESH_LOCKS.remove(&instance_id);

        let (sender, mut rx) = request_channel();
        let err = serve_sandbox_content(
            key,
            ApiVersion::V1,
            Some("../../etc/hostname"),
            sender,
            &test_webapp_cache(),
        )
        .await
        .err()
        .expect("a traversal page path must be refused");
        assert!(
            matches!(err, WebSocketApiError::InvalidParam { .. }),
            "a traversal page path must be an invalid param, got: {err:?}"
        );
        assert!(
            rx.try_recv().is_err(),
            "a traversal page path must not reach the node at all — no fetch, \
             no permit spent"
        );

        CONTRACT_REFRESH_LOCKS.remove(&instance_id);
        clear_cache(&instance_id).await;
    }

    /// A traversal path must be refused BEFORE the speculative fetch, not
    /// after. Two things ride on the ordering: a request that can never resolve
    /// must not spend one of the node's speculative-fetch permits (a spray of
    /// `../` paths would otherwise deny the lane to real subresources), and the
    /// 400 must not be masked by whatever the fetch returns first — with the
    /// check after the fetch, a node error surfaced as a 500 instead
    /// (`web_subpages_error_response_carries_cors_header` caught exactly that).
    #[tokio::test]
    async fn variable_content_rejects_traversal_before_fetching() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x55;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        // Cold cache: without the early check this request WOULD fetch.
        clear_cache(&instance_id).await;

        let (sender, mut rx) = request_channel();
        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/../../etc/hostname"),
            ApiVersion::V1,
            sender,
            &test_webapp_cache(),
        )
        .await;

        assert!(
            matches!(
                result.as_ref().map(|_| ()),
                Err(err) if matches!(err.as_ref(), WebSocketApiError::InvalidParam { .. })
            ),
            "a traversal path must be rejected as an invalid param"
        );
        assert!(
            rx.try_recv().is_err(),
            "a traversal path must not reach the node at all — no fetch, no \
             permit spent"
        );

        clear_cache(&instance_id).await;
    }

    /// `..%2f..%2f…` decodes to `../../…`; the old code joined it onto the cache
    /// dir with no containment check and served whatever it resolved to — an
    /// unauthenticated arbitrary local-file read, made cross-origin-readable by
    /// the sandbox `Access-Control-Allow-Origin: *` header. The guard rejects
    /// the escape.
    #[tokio::test]
    async fn variable_content_rejects_parent_dir_traversal() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x51;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        // Warm cache so we get past the refresh gate, plus a "secret" file
        // planted one level ABOVE the entry dir (i.e. in the cache root) that a
        // successful `../` escape would expose.
        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());
        let secret_name = format!("SECRET-{key}.txt");
        let secret_path = cache_dir.parent().unwrap().join(&secret_name);
        tokio::fs::write(&secret_path, b"TOP-SECRET").await.unwrap();

        let (sender, _rx) = request_channel();
        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/../{secret_name}"),
            ApiVersion::V1,
            sender,
            &test_webapp_cache(),
        )
        .await;

        // Must be rejected (Err), and even if a future refactor returns a
        // response, it must NOT contain the secret bytes.
        let leaked = match result {
            Err(_) => false,
            Ok(r) => response_body(r).await.contains("TOP-SECRET"),
        };
        assert!(
            !leaked,
            "`../` traversal must not read a file outside the contract cache dir"
        );

        tokio::fs::remove_file(&secret_path).await.ok();
        clear_cache(&instance_id).await;
    }

    /// Security regression: an ABSOLUTE path smuggled in via `%2f` (which
    /// decodes to a leading `/`, e.g. `%2fetc%2fhostname` → `/etc/hostname`)
    /// must NOT be served. `Path::join` with an absolute path replaces the base
    /// entirely, so without the guard this read arbitrary absolute paths.
    #[tokio::test]
    async fn variable_content_rejects_absolute_path_escape() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x52;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        // A secret at an absolute path outside any cache dir.
        let secret_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(secret_file.path(), b"ABSOLUTE-SECRET").unwrap();
        let abs = secret_file.path().to_string_lossy().into_owned();

        let (sender, _rx) = request_channel();
        // The `//` mirrors what the caller builds when the decoded segment is
        // itself absolute (`{key}` + `/` + `/abs/path`).
        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/{abs}"),
            ApiVersion::V1,
            sender,
            &test_webapp_cache(),
        )
        .await;

        let leaked = match result {
            Err(_) => false,
            Ok(r) => response_body(r).await.contains("ABSOLUTE-SECRET"),
        };
        assert!(
            !leaked,
            "an absolute-path escape must not read a file outside the cache dir"
        );

        clear_cache(&instance_id).await;
    }

    /// Security regression (end-to-end): a symlink INSIDE the contract cache dir
    /// that points OUTSIDE it must not be served. A contract's unpacked web
    /// archive is attacker-authored, so a planted symlink is a real vector — and
    /// it is the ONE case the lexical `..`/root scan cannot catch (the symlink
    /// name is a plain `Normal` component). Only the canonicalize+containment
    /// half of `resolve_web_asset_path` stops it, so this exercises that half
    /// (which the `../` and absolute tests never reach).
    #[cfg(unix)]
    #[tokio::test]
    async fn variable_content_rejects_symlink_escape() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x53;
        let instance_id = ContractInstanceId::new(bytes);
        let key = instance_id.to_string();
        clear_cache(&instance_id).await;

        let cache_dir = contract_web_path(&instance_id);
        tokio::fs::create_dir_all(&cache_dir).await.unwrap();
        tokio::fs::write(state_hash_path(&instance_id), 0u64.to_be_bytes())
            .await
            .unwrap();
        CONTRACT_CACHE_REFRESH.insert(instance_id, Instant::now());

        // Secret outside the cache root, and a symlink inside the cache dir
        // (a plain-looking `escape.png`) pointing at it.
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"SYMLINK-SECRET").unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), cache_dir.join("escape.png"))
            .unwrap();

        let (sender, _rx) = request_channel();
        let result = variable_content(
            key.clone(),
            format!("/v1/contract/web/{key}/escape.png"),
            ApiVersion::V1,
            sender,
            &test_webapp_cache(),
        )
        .await;

        let leaked = match result {
            Err(_) => false,
            Ok(r) => response_body(r).await.contains("SYMLINK-SECRET"),
        };
        assert!(
            !leaked,
            "a symlink inside the cache dir pointing outside it must not be served"
        );

        tokio::fs::remove_file(cache_dir.join("escape.png"))
            .await
            .ok();
        clear_cache(&instance_id).await;
    }

    /// Direct boundary table for `resolve_web_asset_path`, pinning the guard far
    /// more exhaustively (and cheaply) than the integration tests: it rejects
    /// `..` in any position (leading AND mid-path), absolute and root paths, and
    /// accepts legitimate nested/`.`-prefixed asset paths — so a "hardening"
    /// regression that started rejecting real webapp subresources (e.g.
    /// `assets/app.js`) would fail here instead of shipping green and 400-ing
    /// every Dioxus bundle.
    #[test]
    fn resolve_web_asset_path_boundary_table() {
        let base_dir = tempfile::tempdir().unwrap();
        let base = base_dir.path();

        // Rejected: any `..` segment (leading or mid-path), absolute, root.
        for bad in [
            "../secret",
            "../../etc/passwd",
            "a/../../etc/passwd",
            "assets/../../../etc/passwd",
            "/etc/passwd",
        ] {
            assert!(
                resolve_web_asset_path(base, bad).is_err(),
                "{bad:?} must be rejected as traversal"
            );
        }

        // Accepted: legitimate relative asset paths (targets need not exist —
        // a missing asset is the caller's 404, not a rejection here). Each must
        // resolve to a path contained under the base.
        for good in [
            "a.png",
            "assets/app.js",
            "assets/sub/app_bg.wasm",
            "./a.png",
        ] {
            let resolved = resolve_web_asset_path(base, good)
                .unwrap_or_else(|_| panic!("{good:?} must be accepted"));
            assert!(
                resolved.starts_with(base),
                "{good:?} resolved to {resolved:?}, which escapes the base {base:?}"
            );
        }
    }

    /// Direct test of the shared containment predicate used by BOTH the asset
    /// guard (`resolve_web_asset_path`) and the HTML guard (`sandbox_content_body`).
    /// Pins the `..`/root rejection cross-platform; the `Prefix` (Windows drive)
    /// case is unconstructable on non-Windows (`Path` parses `C:foo` as a single
    /// `Normal` component off Windows) so it is asserted only under `cfg(windows)`.
    #[test]
    fn has_escaping_component_flags_traversal() {
        for bad in ["../x", "a/../../etc", "a/b/../../..", "/etc/passwd"] {
            assert!(
                has_escaping_component(Path::new(bad)),
                "{bad:?} must be flagged as escaping"
            );
        }
        for good in [
            "a.png",
            "assets/app.js",
            "a/b/c.png",
            "./a.png",
            "my image.png",
        ] {
            assert!(
                !has_escaping_component(Path::new(good)),
                "{good:?} is a legitimate contained path"
            );
        }
        #[cfg(windows)]
        {
            // Drive-relative `C:temp` has a Prefix but no RootDir — the exact
            // case a `ParentDir | RootDir`-only check would miss.
            assert!(has_escaping_component(Path::new("C:temp")));
        }
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
    /// Used for both the warm-but-stale refresh and the cold speculative fetch:
    /// neither is preceded by a presence query. Only a cold fetch that finds
    /// the speculative-fetch lane saturated falls back to one, and those tests
    /// answer it with `answer_presence_query` before calling this.
    ///
    /// Replies to the `NewConnection` callback with a synthetic client id so
    /// the handler progresses past its blocking `NewId` recv to the `Get`.
    async fn expect_fetch_pair(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
    ) {
        expect_fetch_pair_holding_callbacks(rx, instance_id).await;
    }

    /// `expect_fetch_pair`, returning the fetch's callback sender.
    ///
    /// Dropping that sender closes the channel the handler is waiting on, so
    /// the fetch ends immediately — fine for a test that aborts the handler
    /// next, wrong for one that needs the fetch to stay outstanding (and to
    /// keep holding its speculative-fetch permit).
    async fn expect_fetch_pair_holding_callbacks(
        rx: &mut tokio::sync::mpsc::Receiver<ClientConnection>,
        instance_id: ContractInstanceId,
    ) -> tokio::sync::mpsc::UnboundedSender<HostCallbackResult> {
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
        callbacks
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
                serve_sandbox_content(
                    key.clone(),
                    ApiVersion::V1,
                    None,
                    sender,
                    &test_webapp_cache(),
                )
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
        let result = serve_sandbox_content(
            key.clone(),
            ApiVersion::V1,
            None,
            sender,
            &test_webapp_cache(),
        )
        .await;

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
        let handler = tokio::spawn(async move {
            refresh_cache_if_due(instance_id, &sender, &test_webapp_cache())
                .await
                .map(|_| ())
        });

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
                refresh_cache_if_due(instance_id, &sender, &test_webapp_cache())
                    .await
                    .map(|_| ())
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
        let handler = tokio::spawn(async move {
            refresh_cache_if_due(instance_id, &sender, &test_webapp_cache()).await
        });

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
            &test_webapp_cache(),
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

        let result = handle_get_response(instance_id, recv_result, &test_webapp_cache()).await;
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

    /// A GET whose retry loop exhausted comes back as `Ok(ContractResponse::
    /// NotFound)` — a SUCCESS at the client-API level, produced deliberately so
    /// a client can tell "absent" apart from "the operation failed". It must be
    /// classified transient, not swept into the unmatched-response catch-all.
    ///
    /// Regression pin. Before the arm existed, `NotFound` matched no arm, fell
    /// into `Ok(other)`, and became `NodeError { "Unexpected response from node:
    /// .." }`, which `errors.rs` renders as a bare 500 because the message does
    /// not begin with "Contract not found". On Freenet a `NotFound` is routinely
    /// "not found YET" (the #4404 placement gap), so a contract published
    /// minutes earlier served a dead-looking 500 to every visitor and every
    /// crawler until it propagated.
    ///
    /// The status assertion is the half that matters, so it is made against the
    /// real `into_response`: asserting only the error VARIANT would still pass
    /// if `errors.rs` later stopped treating `RequestError(Timeout)` as
    /// transient, which is exactly the coupling that broke here.
    #[tokio::test]
    async fn handle_get_response_maps_network_not_found_to_transient_retry() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x44;
        let instance_id = ContractInstanceId::new(bytes);

        let recv_result: Result<Option<HostCallbackResult>, tokio::time::error::Elapsed> =
            Ok(Some(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(ContractResponse::NotFound {
                    instance_id,
                })),
            }));

        let result = handle_get_response(instance_id, recv_result, &test_webapp_cache()).await;
        let err = result.expect_err("a network NotFound must not be treated as a successful fetch");
        assert!(
            matches!(err, WebSocketApiError::ContractNotFound { .. }),
            "a dead-ended GET must get its own classification, not fall through to the \
             unmatched-response catch-all, got: {err:?}"
        );

        let response = err.into_response();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "must serve 503 (retry later). 500 is what this bug produced; 404 would be \
             WORSE than the bug, because a crawler treats 404 as terminal and would \
             permanently drop a contract that was merely slow to propagate"
        );

        // The headers are the half a programmatic client acts on, and they are set
        // by a DIFFERENT file. Asserting the status alone would still pass if
        // `errors.rs` stopped attaching them to this variant.
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("60"),
            "503 without Retry-After tells a client to come back but not when"
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|v| v.to_str().ok()),
            Some("no-store"),
            "an intermediary must not pin this page once the contract arrives"
        );

        // And it must NOT auto-refresh. The identical node reply is produced for a
        // key that will never resolve, so a meta-refresh here re-issues a network
        // GET every minute for the life of any tab left open on a mistyped URL.
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("body must be readable");
        let body = String::from_utf8_lossy(&body);
        assert!(
            !body.contains("http-equiv=\"refresh\""),
            "the not-found page must not reload itself — see browser-assets.md, \
             'assume every open tab pays the cost'"
        );
    }

    /// The catch-all still catches. Carving `NotFound` out of it must not leave it
    /// dead: a response that genuinely makes no sense for a GET (here a
    /// `PutResponse`) must still surface as an unmatched-response error.
    ///
    /// This arm is where the fixed bug hid, and nothing exercised it before.
    #[tokio::test]
    async fn handle_get_response_still_rejects_a_genuinely_unexpected_response() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x45;
        let instance_id = ContractInstanceId::new(bytes);
        let key = freenet_stdlib::prelude::ContractKey::from_id_and_code(
            instance_id,
            freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
        );

        let recv_result: Result<Option<HostCallbackResult>, tokio::time::error::Elapsed> =
            Ok(Some(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Ok(HostResponse::ContractResponse(
                    ContractResponse::PutResponse { key },
                )),
            }));

        let result = handle_get_response(instance_id, recv_result, &test_webapp_cache()).await;
        let err = result.expect_err("a PutResponse is not a valid answer to a GET");
        assert!(
            matches!(err, WebSocketApiError::NodeError { .. }),
            "an unexpected variant must still reach the catch-all, got: {err:?}"
        );
        assert_eq!(
            err.into_response().status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "a genuinely unexpected node response IS a server-side error, and 500 is \
             the right answer for it — that was never the complaint"
        );
    }

    /// A node-returned `Err` keeps its own `ErrorKind`, so `errors.rs` can decide
    /// transient-vs-terminal from the kind. Pinned because the NotFound arm sits
    /// directly above this one and a mis-ordered edit would swallow it.
    #[tokio::test]
    async fn handle_get_response_preserves_a_node_returned_error_kind() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0x3a;
        bytes[1] = 0x46;
        let instance_id = ContractInstanceId::new(bytes);

        let recv_result: Result<Option<HostCallbackResult>, tokio::time::error::Elapsed> =
            Ok(Some(HostCallbackResult::Result {
                id: crate::client_events::ClientId::next(),
                result: Err(ErrorKind::OperationError {
                    cause: "contract banned".into(),
                }
                .into()),
            }));

        let result = handle_get_response(instance_id, recv_result, &test_webapp_cache()).await;
        let err = result.expect_err("a node error must not be treated as a successful fetch");
        assert!(
            matches!(
                err,
                WebSocketApiError::AxumError {
                    error: ErrorKind::OperationError { .. }
                }
            ),
            "the node's own ErrorKind must survive so errors.rs can classify it, got: {err:?}"
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

        let result = handle_get_response(instance_id, recv_result, &test_webapp_cache()).await;
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

    /// The iframe's `sandbox` attribute and the `sandbox` CSP directive served
    /// with contract content must name the SAME tokens.
    ///
    /// Both apply to the app frame, and the effective policy is their
    /// INTERSECTION, so they are not independent knobs:
    ///   - a token in the attribute but not the CSP is withdrawn from every
    ///     contract app the moment the CSP is the narrower of the two (drop
    ///     `allow-forms` there and every form in every app stops submitting);
    ///   - a token in the CSP but not the attribute is dead weight that reads
    ///     as a granted capability.
    ///
    /// They exist for different reasons and neither replaces the other: the
    /// attribute is what the shell asserts about the frame it creates, the CSP
    /// is what the server asserts about the bytes wherever they are embedded
    /// (see `CONTRACT_CONTENT_SANDBOX_CSP`). Keeping them literally equal is
    /// what makes "the app behaves the same either way" checkable rather than
    /// asserted.
    #[tokio::test]
    async fn shell_page_iframe_sandbox_matches_contract_content_csp() {
        let token = AuthToken::generate();
        let html = response_body(
            shell_page(&token, "testkey123", ApiVersion::V1, None, None, false).unwrap(),
        )
        .await;

        let attr_start = html
            .find(r#"sandbox=""#)
            .expect("app iframe carries a sandbox attribute");
        let rest = &html[attr_start + r#"sandbox=""#.len()..];
        let attr = &rest[..rest.find('"').expect("sandbox attribute is quoted")];

        let mut attr_tokens: Vec<&str> = attr.split_whitespace().collect();
        let mut csp_tokens: Vec<&str> = super::super::client_api::CONTRACT_CONTENT_SANDBOX_CSP
            .split_whitespace()
            .skip(1) // the directive name itself
            .collect();
        attr_tokens.sort_unstable();
        csp_tokens.sort_unstable();
        assert_eq!(
            attr_tokens, csp_tokens,
            "the iframe sandbox attribute and CONTRACT_CONTENT_SANDBOX_CSP have \
             drifted. The browser applies BOTH to the app frame and takes the \
             intersection, so whichever is narrower silently wins: add the token \
             to both, or remove it from both, and say which capability you are \
             changing for contract apps"
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
                r#"sandbox="allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-downloads allow-modals""#
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
            "shell page must not reference external origins (CORS). This is a \
             plain substring check over the whole rendered page, including the \
             inlined shell_bridge.js — so a COMMENT that merely mentions a \
             freenet.org host trips it too. If that is what you hit, drop the \
             hostname from the comment rather than loosening this assertion."
        );
        // Shell message handler must be present in bridge JS
        assert!(
            html.contains("__freenet_shell__"),
            "bridge JS must handle shell-level messages (title/favicon)"
        );
        // `allow-popups-to-escape-sandbox` MUST be present: it is what makes a
        // new tab open identically in every browser. The popup carries a real
        // user gesture from the frame that got the click, and lands on the shell
        // at the node's real origin. Routing new-window opens through the shell's
        // `open_url` bridge instead (the previous design) put `window.open`
        // inside a `message` handler, which Firefox's popup blocker refuses
        // outright — `message` is not in `dom.popup_allowed_events`.
        //
        // The flag was removed by PR #3818 (`ec140e09c`, "browser-based
        // permission prompts with iframe security hardening"), motivated by
        // #1499 — citation corrected in #5107, which found the old comment here
        // pointed at #1499 (the delegate-user-interaction feature request)
        // rather than at the change itself.
        //
        // #3818's concern — CONTRACT script executing at the node's real origin
        // — is what re-adding the flag has to answer, and the iframe `sandbox`
        // attribute cannot answer it: an escaped popup is a context we do not
        // control, and from there a contract can re-embed its own bytes itself.
        // The answer is `CONTRACT_CONTENT_SANDBOX_CSP` in `client_api.rs`: every
        // response carrying contract-authored bytes is served a `sandbox` CSP
        // directive, so the opaque origin is decided by this server rather than
        // by whichever context happens to embed it. Do NOT drop that header, and
        // do NOT narrow it to `Sec-Fetch-Dest: document`, without re-deriving
        // the argument — the concrete attack is in the header's comment.
        assert!(
            html.contains("allow-popups-to-escape-sandbox"),
            "escaped popups are required for cross-browser target=\"_blank\""
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
        // Subscribes to the permission-event WebSocket and POSTs back with the
        // response. /permission/pending is still referenced as the
        // bootstrap-on-connect and `resync` reconciliation endpoint, plus the
        // no-WebSocket fallback, so the assertion below still holds.
        assert!(
            html.contains("/permission/events/ws"),
            "shell JS must subscribe to the /permission/events/ws WebSocket"
        );
        assert!(
            html.contains("/permission/pending"),
            "shell JS must reference /permission/pending for bootstrap/resync"
        );
        // #5213 regression pin. The permission channel MUST NOT ride a
        // long-lived HTTP request. Every open tab holds it for the tab's whole
        // life and all Freenet apps share one origin, so an SSE/EventSource
        // (or any other held-open HTTP request) permanently consumes one of
        // the browser's ~6 connections per origin PER TAB. At six tabs the
        // budget is gone and a seventh tab's document request queues forever
        // with no error surfaced — reproduced as a hard stall at tab 7, with
        // tabs 1-6 loading in ~30ms.
        //
        // This asserts on the SHELL JS only. The server still serves the
        // legacy SSE route for tabs opened before a node upgrade, so pinning
        // the absence of the route itself would be wrong; what must not come
        // back is the CLIENT holding one.
        //
        // SCOPE, so nobody mistakes this for the real guard: a substring check
        // can only rule out the ONE spelling of the violation that shipped. A
        // streamed `fetch()`, a long-poll, or any other request the shell
        // never lets finish would reintroduce #5213 and keep this assertion
        // green. The invariant itself ("the shell holds no HTTP request open")
        // is enforced behaviourally in a real browser by
        // crates/core/tests/playwright/tests/connection-exhaustion.spec.ts,
        // which loads the shell, waits, and fails if any request is still
        // unfinished. Keep this cheap check as the fast local signal, but if
        // you change how the shell subscribes, that spec is what must pass.
        //
        // Note that spec runs in a workflow that is path-filtered and NOT a
        // required check, so the strongest guard for this invariant cannot
        // block a merge while this weaker one can. Tracked in #5275.
        assert!(
            !html.contains("EventSource("),
            "shell JS must not open an EventSource: one held-open HTTP request \
             per tab exhausts the browser's ~6-connections-per-origin budget \
             and hangs the 7th Freenet tab (#5213)"
        );
        // The WebSocket has no auto-reconnect (EventSource did), so the shell
        // owns reconnection. Without it a single node restart would leave
        // every open tab permanently on the 3s polling fallback.
        assert!(
            html.contains("schedulePermReconnect"),
            "shell JS must reconnect the permission WebSocket itself (#5213)"
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
        // Event names the server emits. Pinning these here ensures the shell
        // stays in sync with the gateway's wire format. The names and payload
        // shapes are shared by both transports (the WebSocket carries the name
        // inside a JSON envelope; the legacy SSE route uses its `event:`
        // field), so these assertions are transport-independent.
        assert!(
            html.contains("'prompt_added'") || html.contains("\"prompt_added\""),
            "shell JS must handle the prompt_added event"
        );
        assert!(
            html.contains("'prompt_removed'") || html.contains("\"prompt_removed\""),
            "shell JS must handle the prompt_removed event"
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
        // the `prompt_added`/`prompt_removed` handlers, which ARE part of the
        // prompt-render flow #3836 protects — so a browser Notification
        // reintroduced into an event handler would have slipped past this
        // guard. The markers bracket the whole overlay + event-channel region
        // so the asserts below scan all of it (#4849 F2). Note the old code
        // anchor named `EventSource`, which #5213 removed: another reason
        // marker bounds beat code anchors here.
        let overlay_start = html
            .find("perm-overlay-flow:BEGIN")
            .expect("perm-overlay-flow:BEGIN marker must bracket the overlay flow");
        let overlay_end = html[overlay_start..]
            .find("perm-overlay-flow:END")
            .expect("perm-overlay-flow:END marker must bracket the overlay flow");
        let overlay_slice = &html[overlay_start..overlay_start + overlay_end];
        // The negative asserts below are only meaningful if the slice actually
        // CONTAINS the permission prompt-render surface. Pin that the marker-bounded
        // region includes the `prompt_added`/`prompt_removed` handlers, so a
        // refactor that moves them past `perm-overlay-flow:END` (shrinking the
        // slice) fails HERE rather than silently making the negative asserts
        // pass vacuously — the exact regression F2 exists to prevent (#4849).
        assert!(
            overlay_slice.contains("'prompt_added'") && overlay_slice.contains("'prompt_removed'"),
            "overlay guard slice must cover the permission prompt handlers (#4849 F2)"
        );
        assert!(
            !overlay_slice.contains("innerHTML"),
            "overlay code path must not use innerHTML (XSS surface)"
        );

        // The old permission-prompt-via-Notification flow must be gone: the
        // permission OVERLAY code path must not request or construct a browser
        // Notification (#3836 — delegate permission prompts must render as the
        // in-page permission overlay, never as a browser Notification users
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
        // The visibility-gated polling loop was replaced by a pushed channel
        // (SSE originally, a WebSocket since #5213). A push arrives
        // regardless of tab visibility, so the visibility-skip code
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
             visibility-skip caused background tabs to miss prompts (the permission \
             channel replaces polling)"
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
        // A percent-escape that SURVIVES axum's single decode is a second,
        // browser-side decode waiting to happen: `%252e%252e` on the wire
        // arrives here as the literal `%2e%2e`, passes the dot-segment check,
        // and is then normalized to `..` by the URL parser in whatever we hand
        // back — the iframe `data-src`, or the sub-page redirect. Measured
        // end to end: it resolved to ANOTHER contract's page inside a shell
        // holding this contract's auth token.
        for encoded in [
            "%2e%2e/OTHERKEY/index.html",
            "%2E%2E/OTHERKEY/index.html",
            ".%2e/OTHERKEY/index.html",
            "%2e/index.html",
            "sub/%2e%2e/%2e%2e/index.html",
            "a%25b.html",
        ] {
            assert!(
                sanitize_shell_sub_path(encoded).is_err(),
                "{encoded} must be rejected: it is once-decoded here but decoded \
                 again by the browser"
            );
        }

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
                    &test_webapp_cache(),
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

    /// Regression test: the shell page's `<title>` is hardcoded (`shell.html`)
    /// and the sandboxed iframe cannot touch `document.title` on the parent
    /// directly (no `allow-same-origin`), so before `TITLE_SYNC_JS` existed
    /// the browser tab showed "Freenet" forever for any contract that did not
    /// hand-roll its own postMessage sender — which was every contract except
    /// River, Atlas, and Delta. This asserts the sync script is injected
    /// alongside the WS shim and interceptor for EVERY sandboxed page,
    /// including one with no app JS at all.
    #[tokio::test]
    async fn sandbox_content_injects_title_sync() {
        let dir = tempfile::tempdir().unwrap();
        let key = "testkey123";
        let html =
            r#"<!DOCTYPE html><html><head><title>My App</title></head><body>Hello</body></html>"#;
        std::fs::write(dir.path().join("index.html"), html).unwrap();

        let result = response_body(
            sandbox_content_body(dir.path(), key, ApiVersion::V1, "index.html")
                .await
                .unwrap(),
        )
        .await;

        assert!(
            result.contains("type: 'title'"),
            "title sync script not injected"
        );
        assert!(
            result.contains("__freenet_shell__: true"),
            "title sync script must use the shell postMessage bridge protocol"
        );
        // Must forward document.title, not a hardcoded string — this is what
        // makes it work for every app rather than needing per-app wiring.
        assert!(
            TITLE_SYNC_JS.contains("document.title"),
            "title sync script must read document.title, not a fixed string"
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

    /// Pins the shell's peer-restart recovery, which is driven AUTONOMOUSLY by
    /// the node's trusted stale-token close code (PR #4781, server-4401 design).
    /// On a node restart the shell's in-memory auth token is invalidated; the
    /// node answers the reconnecting WebSocket with application close code 4401
    /// (`AUTH_TOKEN_INVALID_CLOSE_CODE`) and closes it. The shell — which owns
    /// the WS and the token — sees that close and re-fetches THIS shell HTML
    /// (minting a fresh token) with a cache-busting top-level `location.replace`.
    /// The shell does NOT depend on the sandboxed app asking (the old, spoofable
    /// `type:'reload'` message path is removed).
    #[test]
    fn bridge_js_reloads_shell_on_auth_token_invalid_close() {
        assert!(
            SHELL_BRIDGE_JS.contains("code === 4401 && !clientClosed"),
            "shell must recover on the node's trusted stale-token close code (4401)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("if (isTrustedStaleTokenClose(e.code, ws._clientClosed))"),
            "recovery must trigger on a SERVER-initiated 4401 close only"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("triggerRecoveryReload()"),
            "the 4401 close must drive the autonomous recovery reload"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("location.replace(decision.url)"),
            "recovery must be a cache-busting top-level navigation (location.replace)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("_freload"),
            "recovery must cache-bust so a stale cached shell (dead token) can't loop"
        );
        // The untrusted, spoofable iframe-initiated reload path must be GONE.
        assert!(
            !SHELL_BRIDGE_JS.contains("msg.type === 'reload'"),
            "the iframe-initiated reload trigger must be removed (recovery is \
             driven by the node's trusted close code, not the app's say-so)"
        );
    }

    /// Drift pin: the JS recovery guard hardcodes the literal `4401`, but the
    /// SERVER side that emits the close frame uses the Rust constant
    /// `AUTH_TOKEN_INVALID_CLOSE_CODE` as its single source of truth. Nothing
    /// but this test ties the two together, so a future change to the constant
    /// would silently break shell recovery (the node would close with a new
    /// code the JS no longer recognizes). This assertion FAILS if they drift.
    #[test]
    fn bridge_js_close_code_matches_rust_constant() {
        use crate::client_events::websocket::AUTH_TOKEN_INVALID_CLOSE_CODE;
        assert!(
            SHELL_BRIDGE_JS.contains(&format!(
                "code === {AUTH_TOKEN_INVALID_CLOSE_CODE} && !clientClosed"
            )),
            "shell_bridge.js must gate recovery on the server's \
             AUTH_TOKEN_INVALID_CLOSE_CODE ({AUTH_TOKEN_INVALID_CLOSE_CODE}); the JS literal \
             drifted from the Rust constant"
        );
    }

    /// Pins the two safeguards on the recovery reload (PR #4781 review, MAJOR #2):
    /// (1) it is UNFORGEABLE — a sandboxed contract cannot manufacture the 4401
    /// trigger by asking the shell to close its own socket, because the close
    /// proxy marks iframe-initiated closes (`_clientClosed`) and clamps any
    /// app-range (4000-4999) code the iframe requests; and (2) the reload cap is
    /// FAIL-CLOSED and storage-independent — it lives in the `_freload` URL param
    /// (not writable by the contract, always present even in private mode), so a
    /// loop is bounded even when sessionStorage is unavailable.
    #[test]
    fn bridge_js_recovery_reload_is_unforgeable_and_bounded() {
        // Unforgeable: iframe-requested closes are marked (`_clientClosed`) so the
        // trusted-close decision rejects them, AND their app-range codes are
        // clamped so 4401 can't even surface to onclose.
        assert!(
            SHELL_BRIDGE_JS.contains("ws._clientClosed = true"),
            "the close proxy must mark iframe-initiated closes so 4401 isn't trusted from them"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("function isTrustedStaleTokenClose(")
                && SHELL_BRIDGE_JS.contains("code === 4401 && !clientClosed"),
            "recovery must only trust a server-initiated 4401 close (not iframe-initiated)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("function clampProxiedCloseCode(")
                && SHELL_BRIDGE_JS.contains("code >= 4000 && code <= 4999"),
            "the close proxy must clamp app-range close codes the iframe requests"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("ws.close(clampProxiedCloseCode(msg.code), msg.reason)"),
            "the close proxy must apply the clamp to the iframe-requested code"
        );
        // Fail-closed, storage-independent cap keyed on the top-document URL.
        assert!(
            SHELL_BRIDGE_JS.contains("function reloadUrlCapDecision("),
            "the reload cap must be computed from the URL (storage-independent, fail-closed)"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("reloadUrlCapDecision(location.href, Date.now())"),
            "recovery must consult the URL-param cap before reloading"
        );
        assert!(
            SHELL_BRIDGE_JS.contains("count >= MAX"),
            "the URL cap must refuse once the per-window reload count is reached"
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
        // The markers bracket showAppNotification so shell_bridge_notifications
        // .test.mjs can extract and drive it. Pin them here: without this,
        // deleting the markers AND the .mjs cases together leaves CI green with
        // the #5043 status coverage silently gone.
        let show_start = SHELL_BRIDGE_JS
            .find("notify-show:BEGIN")
            .expect("notify-show:BEGIN marker must bracket showAppNotification");
        let show_end = SHELL_BRIDGE_JS[show_start..]
            .find("notify-show:END")
            .expect("notify-show:END marker must bracket showAppNotification");
        let show_slice = &SHELL_BRIDGE_JS[show_start..show_start + show_end];
        // Same for the enable-prompt ladder (#5043 item 3).
        let offer_start = SHELL_BRIDGE_JS
            .find("notify-offer:BEGIN")
            .expect("notify-offer:BEGIN marker must bracket maybeOfferNotifications");
        assert!(
            SHELL_BRIDGE_JS[offer_start..].contains("notify-offer:END"),
            "notify-offer:END marker must bracket maybeOfferNotifications"
        );
        // Every notification is gated on BOTH the browser permission AND this
        // contract's own consent. Asserted against the marker-bounded slice, so
        // the gates must live INSIDE showAppNotification — two file-wide
        // `contains` calls would stay green if a refactor moved them out.
        // (Two separate gates since #5043, so each drop can report its own
        // `notification_status` back to the app instead of returning silently.)
        assert!(
            show_slice.contains("Notification.permission !== 'granted'")
                && show_slice.contains("!contractHasConsent()"),
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

    /// Reading `navigator.serviceWorker` throws a SecurityError in a sandboxed
    /// document without 'allow-same-origin': the property exists on Navigator
    /// (so an `'serviceWorker' in navigator` feature-check passes) but its
    /// GETTER throws. In 0.2.107 the eager installNotifyClickListener() call
    /// read it unguarded; the uncaught throw killed freenetBridge before its
    /// message handlers installed and every locally-served web app hung
    /// (#4945). All serviceWorker access must go through the try/catch
    /// accessor.
    #[test]
    fn bridge_js_service_worker_reads_survive_sandboxed_navigator() {
        assert!(
            SHELL_BRIDGE_JS.contains("function serviceWorkerOrNull()"),
            "the try/catch serviceWorker accessor must exist"
        );
        let body_start = SHELL_BRIDGE_JS
            .find("function serviceWorkerOrNull()")
            .unwrap();
        let body = &SHELL_BRIDGE_JS[body_start..body_start + 400];
        assert!(
            body.contains("try {") && body.contains("catch"),
            "serviceWorkerOrNull must guard the navigator.serviceWorker read with try/catch"
        );
        // Outside the accessor, `navigator.serviceWorker` may appear only as
        // the (already try-guarded) register call pinned by the mobile test
        // below — any new access must route through serviceWorkerOrNull().
        assert_eq!(
            SHELL_BRIDGE_JS.matches("navigator.serviceWorker").count(),
            2,
            "raw navigator.serviceWorker reads outside serviceWorkerOrNull() and \
             the try-guarded register call reintroduce the #4945 sandbox crash"
        );
        // The `in`-operator feature check is exactly the pattern that passed in
        // the sandbox and then blew up on read — it must not come back.
        assert!(
            !SHELL_BRIDGE_JS.contains("'serviceWorker' in navigator"),
            "feature-detect by attempting the read (serviceWorkerOrNull), not via `in`"
        );
    }

    /// #5043: a framed app can't read `Notification.permission` (opaque origin),
    /// so the shell reporting a status is its ONLY way to learn a notification
    /// was dropped. Regression pins for the two paths that were silent and were
    /// re-broken during review of the first fix — the rate-limiter drop, and the
    /// async service-worker chain, whose only rejection handler covered
    /// `showNotification` and left a rejected registration lookup (or a
    /// synchronous throw) with no reply at all.
    ///
    /// The exactly-one-status-per-message behavior is verified by driving the
    /// real extracted functions in `shell_bridge_notifications.test.mjs` (cases
    /// 9 and 10, run by the lint-assets CI job). These are source pins for the
    /// two specific silent-return shapes, in the same discipline as the
    /// `SHELL_BRIDGE_JS.contains` guards above, so a refactor that reintroduces
    /// either shape fails here too.
    #[test]
    fn bridge_js_notification_drops_are_never_silent() {
        let start = SHELL_BRIDGE_JS
            .find("notify-show:BEGIN")
            .expect("notify-show:BEGIN marker must bracket showAppNotification");
        let end = SHELL_BRIDGE_JS[start..]
            .find("notify-show:END")
            .expect("notify-show:END marker must bracket showAppNotification");
        let show = &SHELL_BRIDGE_JS[start..start + end];

        // The rate-limiter drop is the most frequent one (a busy room hits the
        // 3s per-tag throttle constantly). `if (...) return;` on one line is the
        // exact shape it had while it was silent.
        assert!(
            !show.contains("if (!notifyLimiter.ok(opts.tag, Date.now())) return;"),
            "the rate-limited drop must post a status, not return silently (#5043)"
        );
        assert!(
            show.contains("notifyLimiter.ok(") && show.contains("notifyStatusToIframe('granted')"),
            "the rate-limited drop must report 'granted' — permission and consent \
             are intact and the shell merely coalesced the message"
        );

        // The service-worker chain needs a terminal .catch: without it a rejected
        // notifyRegistrationReady, a synchronous throw from showNotification, or
        // a non-thenable return all leave the app with no reply.
        let sw_start = show
            .find("notifyRegistrationReady(")
            .expect("the mobile fallback must go through notifyRegistrationReady");
        let sw_chain = &show[sw_start..];
        assert!(
            sw_chain.contains(".catch("),
            "the service-worker delivery chain must end in a .catch so a rejected \
             registration lookup or a throwing showNotification still replies (#5043)"
        );
        // ...and that backstop must not turn one reply into two: every post from
        // the chain goes through the post-at-most-once helper, never through
        // notifyStatusToIframe directly.
        assert!(
            show.contains("var swReplied = false;") && sw_chain.contains("swReply("),
            "the service-worker chain's .catch backstop must be guarded so a throw \
             from the success-path status post can't produce a second reply"
        );
        assert!(
            !sw_chain.contains("notifyStatusToIframe("),
            "the service-worker chain must post via swReply(), which is what bounds \
             it to one reply — a direct notifyStatusToIframe call bypasses that"
        );

        // The constructor path's status post must sit OUTSIDE the try: inside, a
        // throw from it reads as "constructor unsupported" and the worker path
        // displays the SAME notification a second time.
        assert!(
            show.contains("shownByConstructor = true;")
                && show.contains("if (shownByConstructor) {"),
            "the constructor path must record delivery in a flag and report \
             outside the try, so a throwing status post can't double-deliver"
        );
    }

    /// Mobile browsers reject the page-level `new Notification()` constructor, so
    /// the shell must show notifications via a service worker's
    /// `showNotification()`. Pin the wiring by source so a refactor can't
    /// silently drop it and re-break mobile notifications.
    #[test]
    fn bridge_js_registers_notification_service_worker() {
        // The shell registers the same-origin notification service worker.
        assert!(
            SHELL_BRIDGE_JS.contains("NOTIFY_SW_URL = '/freenet-notify-sw.js'")
                && SHELL_BRIDGE_JS.contains("navigator.serviceWorker.register(NOTIFY_SW_URL)"),
            "shell must register the /freenet-notify-sw.js service worker"
        );
        // It falls back to showNotification() — the only path that works on
        // mobile, where `new Notification()` throws.
        assert!(
            SHELL_BRIDGE_JS.contains("reg.showNotification("),
            "shell must show notifications via the service worker on mobile"
        );
        // Desktop is UNCHANGED: the page-level constructor is still used, under
        // the same length cap. (The service worker only engages when it throws.)
        assert!(
            SHELL_BRIDGE_JS.contains("new Notification(title, opts)"),
            "desktop must still use the page-level Notification constructor"
        );
        // Constructor-FIRST ordering: the page-level constructor must appear
        // BEFORE the showNotification fallback. A refactor that inverts them
        // (SW-first) would silently switch desktop to SW-shown notifications and
        // onto the click-forwarding path — this catches it.
        let ctor = SHELL_BRIDGE_JS
            .find("new Notification(title, opts)")
            .expect("constructor call present");
        let sw_show = SHELL_BRIDGE_JS
            .find("reg.showNotification(")
            .expect("showNotification fallback present");
        assert!(
            ctor < sw_show,
            "the page-level constructor must be tried BEFORE the service-worker fallback"
        );
        // Click-routing tag contract: the shell writes the routing tag as
        // `fnTag` in notification data; the worker reads `data.fnTag` (pinned in
        // client_api.rs). A rename on the shell side silently breaks routing.
        assert!(
            SHELL_BRIDGE_JS.contains("fnTag: routeTag"),
            "shell must put the routing tag in notification data as fnTag"
        );
        // When neither the constructor nor the worker can display it, the app is
        // told so it can rely on the in-app unread badge.
        assert!(
            SHELL_BRIDGE_JS.contains("notifyStatusToIframe('undeliverable')"),
            "must report 'undeliverable' when neither the constructor nor the worker can show it"
        );
        // The worker's click (which fires in the worker, not the page) is
        // forwarded to the iframe as the same `notification_click` message.
        assert!(
            SHELL_BRIDGE_JS.contains("__freenet_notify_click__"),
            "the worker's notification click must be forwarded to the iframe"
        );
        // Registration is gated on a secure context, since it fails on a plain
        // http (non-localhost) origin — the desktop constructor covers that.
        assert!(
            SHELL_BRIDGE_JS.contains("window.isSecureContext"),
            "service worker registration must be gated on a secure context"
        );
        // The click-forward listener is a standalone function installed EAGERLY
        // at startup (not only on lazy registration), so a click on a persistent
        // notification that outlived a shell reload is still delivered. Pinned so
        // a refactor can't fold it back into ensureNotifyServiceWorker only.
        assert!(
            SHELL_BRIDGE_JS.contains("function installNotifyClickListener("),
            "the SW click-forward listener must be a standalone, eagerly-installed function"
        );
        // It must be CALLED at BOTH sites: lazily inside ensureNotifyServiceWorker
        // AND eagerly at shell startup. Assert two call sites by count, so
        // removing the eager call — reverting to lazy-only installation and
        // reintroducing the "click lost after reload" bug this fixes — fails
        // this test. (The `function installNotifyClickListener() {` definition
        // is `…()` + ` {`, not `…();`, so it isn't counted here.)
        assert!(
            SHELL_BRIDGE_JS
                .matches("installNotifyClickListener();")
                .count()
                >= 2,
            "installNotifyClickListener() must be called BOTH lazily and eagerly at startup"
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
        let parsed = relative_asset_path(req_path).unwrap();
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
        let parsed = relative_asset_path(req_path).unwrap();
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
        let parsed = relative_asset_path(req_path).unwrap();
        assert_eq!(parsed, "assets/app.js");
    }

    /// A filename with a space must extract cleanly — the old `Uri`-based
    /// extractor rejected it (SUB0PT1MAL/cirro CORS report, 2026-07-29).
    #[test]
    fn relative_asset_path_preserves_space_in_filename() {
        let req_path = "/v1/contract/web/HjpgVdSziPUmxFoBgTdMkQ8xiwhXdv1qn5ouQvSaApzD/my image.png";
        let parsed = relative_asset_path(req_path).unwrap();
        assert_eq!(parsed, "my image.png");
    }

    #[test]
    fn relative_asset_path_rejects_unknown_version() {
        let req_path = "/v3/contract/web/somekey/assets/app.js";
        let result = relative_asset_path(req_path);
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
        // New-window activations are NOT routed through the shell: they open
        // natively so the browser sees a real user gesture (see
        // `navigation_interceptor_leaves_new_window_activations_native`).
        assert!(
            !NAVIGATION_INTERCEPTOR_JS.contains("type: 'open_url'"),
            "interceptor must not post open_url: the shell would then call \
             window.open from a `message` handler, which Firefox's popup \
             blocker refuses (#5087 follow-up)"
        );
        // An explicit non-_self target must be recognized and left alone.
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.target"),
            "interceptor must respect target attribute"
        );
        // Must walk up DOM to handle clicks on child elements of <a>
        assert!(
            NAVIGATION_INTERCEPTOR_JS.contains("target.parentElement"),
            "interceptor must walk up DOM to find <a> ancestor"
        );
    }

    /// Regression test for freenet/river#208, under the escaped-popup design.
    ///
    /// A cross-origin link with NO new-window target would, left alone,
    /// navigate the app frame itself to a foreign origin — which the shell's
    /// `frame-src 'self'` refuses, so the click silently does nothing. It must
    /// therefore open a tab. The open has to happen HERE, inside the
    /// click/auxclick handler, because that is the only place a live user
    /// gesture exists: Firefox allows `window.open` only from events in
    /// `dom.popup_allowed_events`, which includes `click`/`auxclick` but NOT
    /// `message`, so the old "post `open_url` and let the shell open it" route
    /// was blocked outright in Firefox.
    ///
    /// The popup escapes the sandbox (`allow-popups-to-escape-sandbox` on the
    /// shell iframe), so the destination sees a real Origin instead of the
    /// `null` one that broke logged-in sites in river#208.
    #[test]
    fn navigation_interceptor_opens_untargeted_cross_origin_links_in_a_tab() {
        let js = NAVIGATION_INTERCEPTOR_JS;

        let cross_origin_idx = js
            .find("target.origin !== location.origin")
            .expect("cross-origin check present");
        // Bound the slice at the start of the same-origin branch so its
        // `postMessage` can't satisfy the assertions below.
        let same_origin_idx = js
            .find("// Same-origin in-contract link")
            .expect("same-origin in-contract branch present");
        // Fail with a message rather than a raw slice panic if the two branches
        // are ever reordered, the same way the sibling bound below does.
        assert!(
            cross_origin_idx < same_origin_idx,
            "expected the cross-origin classification before the same-origin \
             in-contract branch; if they were reordered, re-anchor this bound \
             rather than dropping the check"
        );
        let block = &js[cross_origin_idx..same_origin_idx];

        assert!(
            block.contains("preventDefault"),
            "cross-origin branch must preventDefault before opening the tab"
        );
        assert!(
            block.contains("window.open(target.href, '_blank', 'noopener,noreferrer')"),
            "cross-origin branch must open the tab itself, from the click \
             handler, with noopener/noreferrer (freenet/river#208)"
        );
        assert!(
            !block.contains("postMessage"),
            "cross-origin branch must not hand the open to the shell: \
             window.open from a `message` handler is popup-blocked in Firefox"
        );
    }

    /// Regression test for the Firefox half of #5087.
    ///
    /// #5087 fixed a blank tab on same-origin `target="_blank"` by routing the
    /// click through the shell's `open_url` bridge. That put `window.open`
    /// inside a `message` handler, and Firefox's popup blocker keys on the
    /// dispatching event type (`dom.popup_allowed_events` — no `message`), so
    /// every such click became a no-op in Firefox while Chrome/Safari kept
    /// working by propagating user activation across the frame tree. Users had
    /// to right-click → "Open in new tab", which bypasses JS entirely.
    ///
    /// The fix is `allow-popups-to-escape-sandbox` on the shell iframe: the
    /// natively-opened popup is a real top-level document at the node origin,
    /// so the shell's `frame-src 'self'` matches and the app frame loads. Pin
    /// that new-window activations are left to the browser, and that modifier
    /// clicks (which the postMessage route could never preserve) are too.
    ///
    /// Pin the two boundaries of "new-window activation" as well, because
    /// review found both drawn wrong on the first attempt and both failures
    /// were silent:
    ///
    ///   - A target is a new-window request only if it names a NEW context.
    ///     `_top`/`_parent` name an ANCESTOR, which the sandbox forbids
    ///     navigating, so returning early for them is a dead click — measured
    ///     in chromium and firefox, where `main` opened a tab and the first
    ///     draft of this branch did nothing at all. The comparison is also
    ///     lowercased, because browsers match the reserved keywords
    ///     ASCII-case-insensitively: `target="_SELF"` read as a new-window
    ///     request sent a cross-origin click into the app frame, which
    ///     `frame-src 'self'` then refused (chromium replaced the frame with
    ///     its error page).
    ///   - `e.button` must be tested for truthiness, not `=== 1`. `auxclick`
    ///     fires for the secondary button too, and `preventDefault` there does
    ///     not suppress the context menu, so a middle-button-only check left
    ///     right-click intercepted: menu AND unwanted tab.
    #[test]
    fn navigation_interceptor_leaves_new_window_activations_native() {
        let js = NAVIGATION_INTERCEPTOR_JS;

        let target_attr_idx = js
            .find("var targetName = target.target")
            .expect("target-attribute check present");
        let cross_origin_idx = js
            .find("target.origin !== location.origin")
            .expect("cross-origin check present");
        assert!(
            target_attr_idx < cross_origin_idx,
            "the new-window skip must run BEFORE origin classification, or a \
             cross-origin target=\"_blank\" gets intercepted again"
        );

        let block = &js[target_attr_idx..cross_origin_idx];
        assert!(
            block.contains(".toLowerCase()"),
            "the target keyword must be compared lowercased, or `target=\"_SELF\"` \
             is treated as a new-window request and a cross-origin click lands in \
             the app frame, which `frame-src 'self'` refuses"
        );
        assert!(
            block.contains("targetName !== '_self'")
                && block.contains("targetName !== '_top'")
                && block.contains("targetName !== '_parent'"),
            "only a target naming a NEW context may fall through: `_top` and \
             `_parent` name an ancestor the sandbox forbids navigating, so \
             handing one back to the browser is a silently dead click"
        );
        assert!(
            block.contains("e.button ||")
                && block.contains("e.ctrlKey")
                && block.contains("e.metaKey")
                && block.contains("e.shiftKey")
                && block.contains("e.altKey"),
            "middle/ctrl/cmd/shift-click must also fall through, restoring the \
             background-tab and new-window placement the postMessage route \
             collapsed into a plain foreground tab (#3853). Test `e.button` for \
             truthiness, not `=== 1`: `auxclick` fires for the secondary button \
             too, and preventDefault there does not suppress the context menu. \
             `altKey` is here for a different reason — it is the save-link \
             gesture, not a new window — but the same conclusion applies"
        );
        assert!(
            !block.contains("e.button === 1"),
            "a middle-button-only check leaves right-click intercepted — the \
             user gets the context menu AND an unwanted tab or app-frame \
             navigation, measured in chromium and firefox"
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
    // SUPERSEDED by PR #5100: new-window activations are no longer routed
    // through the shell's `open_url` bridge, and the `window.open` override is
    // gone. Retained (ignored) as historical documentation of the pre-#5100
    // contract, per the project rule that superseded tests are #[ignore]d with
    // an explanation rather than deleted. The behaviour they pinned is what
    // broke `target="_blank"` in Firefox: `window.open` from a `message`
    // handler is refused by its popup blocker. Current contract is pinned by
    // `navigation_interceptor_leaves_new_window_activations_native` and
    // `navigation_interceptor_opens_untargeted_cross_origin_links_in_a_tab`.
    #[ignore]
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
        //
        // Match `e.preventDefault()` with the receiver and parens, not the bare
        // word: this block also spans the same-origin branch's COMMENT, which
        // discusses being "preventDefault-ed". A bare `preventDefault` needle is
        // satisfied by that prose alone, so deleting the real call here would
        // leave this river#208 / #3852 pin green while cross-origin
        // `target="_blank"` went back to opening null-origin sandboxed popups.
        // Verified by mutation: bare needle survives the deletion, this one does
        // not.
        let cross_origin_block = &js[cross_origin_idx..target_attr_idx];
        assert!(
            cross_origin_block.contains("e.preventDefault()"),
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
    // SUPERSEDED by PR #5100: new-window activations are no longer routed
    // through the shell's `open_url` bridge, and the `window.open` override is
    // gone. Retained (ignored) as historical documentation of the pre-#5100
    // contract, per the project rule that superseded tests are #[ignore]d with
    // an explanation rather than deleted. The behaviour they pinned is what
    // broke `target="_blank"` in Firefox: `window.open` from a `message`
    // handler is refused by its popup blocker. Current contract is pinned by
    // `navigation_interceptor_leaves_new_window_activations_native` and
    // `navigation_interceptor_opens_untargeted_cross_origin_links_in_a_tab`.
    #[ignore]
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

        // Match the payload shape, not a bare `shiftKey`. This region is the
        // one #5106 grew by ~30 comment lines, and a bare needle is satisfiable
        // by prose that merely discusses shift-clicking — the same way a bare
        // `preventDefault` needle in the sibling test above was left matching a
        // comment, mutation-confirmed, until it was tightened.
        assert!(
            block.contains("shiftKey: !!e.shiftKey"),
            "cross-origin open_url postMessage must include shiftKey to honour \
             shift-click as a new-window request (#3853); got block: {block}"
        );
        // Must be sourced from the actual event, not a hardcoded constant.
        assert!(
            block.contains("e.shiftKey"),
            "interceptor must forward `e.shiftKey` from the MouseEvent, not a literal (#3853)"
        );
    }

    /// Pins the same-origin new-window branch as the browser's job, through
    /// three attempts at it.
    ///
    /// Before #5089 this branch fell through to the browser, and the resulting
    /// popup INHERITED the iframe sandbox: opaque origin, so WebKit rendered it
    /// blank (#5087), a hosted node could not read the per-user access key and
    /// dead-ended (#4645), and `/permission/pending` answered 200 with an empty
    /// list to its `Origin: null`, so prompts silently never appeared.
    ///
    /// #5089 routed it through the shell's `open_url` bridge instead. That put
    /// `window.open` inside a `message` handler — refused outright by Firefox's
    /// popup blocker, which gates on the dispatching event type — so the click
    /// became a no-op there (#5106). It also dead-ended on a loopback node,
    /// because the bridge's `open_url` handler refuses `localhost`/`127.0.0.1`
    /// and the anchor branch had no fallback. #5107 reverted it.
    ///
    /// #5100 (this) keeps the fall-through and removes the reason it was ever
    /// costly: with `allow-popups-to-escape-sandbox` on the shell iframe the
    /// natively-opened popup is a real top-level document at the node origin,
    /// which fixes all three of the pre-#5089 symptoms without a bridge hop.
    ///
    /// So the branch must still `return`, and must still not grow a forward.
    /// The whole-file "no `type: 'open_url'` anywhere in the interceptor" pin
    /// lives in `navigation_interceptor_js_intercepts_clicks`; it strictly
    /// subsumes the per-handler forward COUNT this test used to carry while the
    /// cross-origin branch still forwarded, so that count is not repeated here.
    #[test]
    fn navigation_interceptor_leaves_same_origin_target_blank_to_the_browser() {
        let js = NAVIGATION_INTERCEPTOR_JS;

        let target_attr_idx = js
            .find("var targetName = target.target")
            .expect("same-origin target check present");
        let navigate_idx = js
            .find("type: 'navigate'")
            .expect("same-origin in-contract navigate branch present");
        assert!(
            target_attr_idx < navigate_idx,
            "the target-attribute check must precede the in-contract navigate branch"
        );
        let block = &js[target_attr_idx..navigate_idx];

        // Scope the `return;` to the target check itself. A block that runs to
        // the `navigate` branch also spans the modifier skip's `return;` and the
        // cross-origin branch's, so the conjunct would hold with the early
        // return deleted — mutation-confirmed.
        let button_idx = js
            .find("if (e.button ||")
            .expect("modifier/button skip present");
        assert!(
            target_attr_idx < button_idx,
            "the target check must precede the modifier skip"
        );
        let target_block = &js[target_attr_idx..button_idx];
        assert!(
            target_block.contains("targetName !== '_self'") && target_block.contains("return;"),
            "same-origin target=\"_blank\" must fall through to the browser. Routing it \
             through open_url dead-ends on a loopback node, where the shell's open_url \
             handler refuses the host and the click does nothing at all (#5106)"
        );
        assert!(
            !block.contains("type: 'open_url'"),
            "no open_url forward may be added to the same-origin new-window branch ahead \
             of the `return` (#5106). A bridge hop here runs `window.open` from a \
             `message` handler, which Firefox's popup blocker refuses, and is dropped \
             outright for a loopback host. If you are DELIBERATELY re-routing this \
             branch, this test is pinning the old behaviour and should be replaced, \
             not deleted piecemeal"
        );

        // Both assertions above scope to the branch, which leaves an evasion:
        // a re-route added as a SEPARATE EARLIER branch is outside the block
        // entirely, so `find()` still lands on the untouched `_self` check and
        // both pass while the bug is fully present. That is not contrived —
        // "special-case `_blank`, leave named targets alone" is a plausible
        // next attempt:
        //
        //     if (target.target === '_blank') { e.preventDefault(); /* open_url */ return; }
        //     if (target.target && target.target !== '_self') return;
        //
        // While the cross-origin branch still forwarded, the only way to catch
        // that was to count forwards across the whole handler and require
        // exactly one. #5100 removed the last forward, so the file-wide
        // assertion in `navigation_interceptor_js_intercepts_clicks` —
        // NO `type: 'open_url'` anywhere in the interceptor — catches the
        // same evasion and every variant of it. Restore a scoped count here if
        // a forward is ever legitimately reintroduced.
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
    // SUPERSEDED by PR #5100: new-window activations are no longer routed
    // through the shell's `open_url` bridge, and the `window.open` override is
    // gone. Retained (ignored) as historical documentation of the pre-#5100
    // contract, per the project rule that superseded tests are #[ignore]d with
    // an explanation rather than deleted. The behaviour they pinned is what
    // broke `target="_blank"` in Firefox: `window.open` from a `message`
    // handler is refused by its popup blocker. Current contract is pinned by
    // `navigation_interceptor_leaves_new_window_activations_native` and
    // `navigation_interceptor_opens_untargeted_cross_origin_links_in_a_tab`.
    #[ignore]
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

    /// Regression test for the middle-click half of #3853. Middle-click is
    /// dispatched as `auxclick` in modern browsers, NOT `click`, so a
    /// `click`-only listener never sees it.
    ///
    /// Today middle-click is deliberately left native (it is a new-window
    /// activation), so both listeners reach the same early return and the
    /// auxclick registration is not what produces the current behaviour. It
    /// stays because the two events must be CLASSIFIED alike: if the
    /// modifier/button skip is ever narrowed, a `click`-only interceptor would
    /// silently stop covering middle-click again, which is exactly how #3853
    /// happened.
    #[test]
    fn navigation_interceptor_listens_on_click_and_auxclick() {
        let js = NAVIGATION_INTERCEPTOR_JS;
        assert!(
            js.contains("addEventListener('click'"),
            "interceptor must register a click listener"
        );
        assert!(
            js.contains("addEventListener('auxclick'"),
            "interceptor must register an auxclick listener so middle-click is \
             classified too (#3853)"
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
    /// markdown links to plain-HTTP services (the trigger was the Network
    /// Telemetry dashboard linked from the Freenet River channel header,
    /// plain HTTP at the time and since moved to
    /// `https://telemetry.freenet.org/`) — the user clicked the link and
    /// nothing happened, no console output, no popup, no error. The
    /// localhost block stays so a pasted `http://127.0.0.1:NNNN/` link
    /// can't be used to target services running on the reader's machine.
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

    /// The shell `open_url` handler's loopback refusal is a SECURITY control:
    /// host in the list ⇒ refuse. It is what stops a contract forging an
    /// `{__freenet_shell__: true, type: 'open_url'}` message aimed at the
    /// viewer's own loopback services — the local node's API included. Any
    /// contract can post that message directly, so the control is live whether
    /// or not the injected interceptor ever sends one.
    ///
    /// #5100 removed the interceptor's `isLoopbackHost`, which existed only to
    /// PREDICT this refusal so the `window.open` override could fall back to
    /// native rather than forward an open the shell would drop. With the
    /// override gone there is no second list, so the parity half of this test
    /// (added in #5107) has nothing left to compare and is dropped with it —
    /// deliberately, not by accident. What survives is the floor: the refusal
    /// must still name every loopback host.
    ///
    /// Limits, so this is not over-trusted. It reads LITERALS, so #4846
    /// (complete the allow-list; it is exact-match today, e.g. `127.0.0.2` is
    /// not covered) implemented as a rule — `h.startsWith('127.')` — would
    /// remove the literals and trip this test even though the rule is strictly
    /// wider. That is the intended failure: come here and re-express the floor
    /// against the new shape rather than deleting it. It also cannot see a `||`
    /// flipped to `&&`, which is uncovered anywhere; the bracket-strip
    /// normalization that makes `[::1]` match is covered by
    /// `shell_open_url_handler_blocks_ipv6_loopback` above.
    #[test]
    fn shell_open_url_refusal_names_every_loopback_host() {
        // Collect the host literals from each `h === '<host>'` comparison in a
        // region.
        //
        // The needle is NOT self-limiting: it also matches the tail of
        // `hash === '` and `iframePath === '`, which occur elsewhere in
        // shell_bridge.js. What makes this sound is purely the REGION BOUNDS
        // chosen below — do not loosen them on the theory that `h` is unique.
        fn loopback_hosts(region: &str) -> Vec<String> {
            const NEEDLE: &str = "h === '";
            let mut out = Vec::new();
            let mut rest = region;
            while let Some(i) = rest.find(NEEDLE) {
                let after = &rest[i + NEEDLE.len()..];
                let end = after.find('\'').expect(
                    "unterminated host literal in a loopback comparison — the region \
                     bounds or the JS shape changed; failing loudly rather than \
                     comparing a silently truncated list",
                );
                out.push(after[..end].to_string());
                // Advances by at least NEEDLE.len() each iteration, so this
                // terminates; all indices are ASCII-needle finds, so every
                // slice lands on a char boundary.
                rest = &after[end..];
            }
            out.sort();
            out.dedup();
            out
        }

        // Anchor tightly on the refusal itself — `var h = u.hostname` up to the
        // `return;` that drops the message. The sibling tests here bound the
        // open_url branch with the next `} else if`, but open_url is the LAST
        // arm of that chain, so that bound silently runs to end-of-file; a
        // stray `h === 'string'` further down would then be read as a loopback
        // host. Slicing to the `return;` keeps this to the comparison chain.
        let refusal_idx = SHELL_BRIDGE_JS
            .find("var h = u.hostname")
            .expect("shell open_url loopback normalization present");
        let rest = &SHELL_BRIDGE_JS[refusal_idx..];
        let refusal_end = rest
            .find("return;")
            .expect("shell open_url loopback refusal returns");
        let shell_hosts = loopback_hosts(&rest[..refusal_end]);

        for host in ["localhost", "127.0.0.1", "::1", "0.0.0.0"] {
            assert!(
                shell_hosts.iter().any(|h| h == host),
                "the shell's open_url refusal no longer covers `{host}` — or the \
                 extraction above truncated, e.g. because the chain was split into \
                 per-host `if (h === '…') return;` lines, which the `return;` bound \
                 would cut short (check shell_bridge.js by eye). This list is a \
                 security boundary: widen it, never narrow it. Got: {shell_hosts:?}"
            );
        }
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
