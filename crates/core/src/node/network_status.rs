//! Network connection status tracking for the node dashboard.
//!
//! Surfaces diagnostic information (version mismatches, NAT traversal failures,
//! peer connections, subscriptions, operation stats, etc.) so the HTTP homepage
//! and connecting page can show actionable diagnostics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use crate::ring::reconcile::ReconcileActionDivergence;
use crate::ring::{PeerKeyLocation, SubscribedContractSnapshot};
use crate::router::Router;
use crate::transport::metrics::{TRANSPORT_METRICS, TransportSnapshot};

static NETWORK_STATUS: OnceLock<Arc<RwLock<NetworkStatus>>> = OnceLock::new();
static ROUTER: OnceLock<Arc<parking_lot::RwLock<Router>>> = OnceLock::new();

/// Provider returning the current dashboard view of subscribed contracts.
///
/// In production this closure captures the node's `Arc<Ring>` and calls
/// `Ring::dashboard_subscription_snapshot()`; tests can register an
/// arbitrary closure to pin the wiring without constructing a Ring.
pub type SubscriptionProvider =
    Arc<dyn Fn() -> Vec<SubscribedContractSnapshot> + Send + Sync + 'static>;

/// Provider for the per-contract governance snapshot. Same pattern as
/// `SubscriptionProvider`: registered at node startup, replaceable for
/// multi-node test harnesses, used by `get_snapshot` to build the
/// dashboard payload from real back-end data.
///
/// In production this closure captures `Arc<Ring>` and reads from
/// `ring.governance` — the GovernanceManager populated by the
/// meter-driven cost reporters and the HostingManager-driven demand
/// reporters.
pub type GovernanceProvider = Arc<dyn Fn() -> GovernanceSnapshot + Send + Sync + 'static>;

/// Provider for live ring-level stats (connection count, hosted contracts).
/// Same provider pattern as `SubscriptionProvider`/`GovernanceProvider`.
pub type RingStatsProvider = Arc<dyn Fn() -> RingStatsSnapshot + Send + Sync + 'static>;

/// Provider for the contract ban-list snapshot (#4302). Same pattern as
/// `GovernanceProvider`: registered at node startup, replaceable for
/// multi-node test harnesses, read by `get_snapshot` on every dashboard
/// request. In production the closure captures `Arc<Ring>` and calls
/// `Ring::dashboard_ban_list_snapshot()`, which reads the canonical
/// `Ring::contract_ban_list` directly — no mirrored counter to rot.
pub type BanListProvider = Arc<dyn Fn() -> BanListSnapshot + Send + Sync + 'static>;

/// Provider for the demand-driven hosting snapshot (piece A, #4642). Same
/// pattern as the other providers: registered at node startup, replaceable
/// for multi-node test harnesses, read by `get_snapshot` on every dashboard
/// request. In production the closure captures `Arc<Ring>` and calls
/// `Ring::dashboard_hosting_snapshot()`, which reads the canonical hosting
/// cache (Greedy-Dual keep_score + capability-relative RAM budget) — the
/// mechanism that actually governs retention now, replacing the dormant MAD
/// governance detector (#4296).
pub type HostingProvider = Arc<dyn Fn() -> HostingSnapshot + Send + Sync + 'static>;

/// Snapshot of ring-level statistics exposed to the dashboard.
#[derive(Debug, Clone, Default)]
pub struct RingStatsSnapshot {
    /// Number of active ring connections.
    pub connection_count: u32,
    /// Number of contracts this node is currently hosting.
    pub hosted_contracts: u32,
    /// Short base58 Peer ID (12-byte prefix) — how other nodes see this peer.
    pub peer_id: String,
    /// Base58-encoded full 32-byte X25519 public key.
    pub own_pub_key: String,
    /// Total relayed UPDATEs accepted by the per-(sender, contract) rate
    /// limiter since startup (see `ring::update_rate_limit`).
    pub updates_accepted: u64,
    /// Total relayed UPDATEs dropped because the per-(sender, contract)
    /// rate exceeded the limit. A rising value means legitimate traffic
    /// may be getting dropped — operators should watch this.
    pub updates_rate_limited: u64,
    /// Total relayed UPDATEs dropped because the limiter's tracking map
    /// was at capacity (`MAX_TRACKED_PAIRS`). A non-zero value suggests
    /// identity churn / admission pressure, distinct from per-pair rate.
    pub updates_capacity_dropped: u64,
}

static GOVERNANCE_PROVIDER: parking_lot::RwLock<Option<GovernanceProvider>> =
    parking_lot::RwLock::new(None);

/// Register the dashboard's governance data source. Replaces any
/// previously-registered provider.
pub fn set_governance_provider(provider: GovernanceProvider) {
    *GOVERNANCE_PROVIDER.write() = Some(provider);
}

static RING_STATS_PROVIDER: parking_lot::RwLock<Option<RingStatsProvider>> =
    parking_lot::RwLock::new(None);

/// Register the dashboard's ring-stats data source.
pub fn set_ring_stats_provider(provider: RingStatsProvider) {
    *RING_STATS_PROVIDER.write() = Some(provider);
}

#[cfg(test)]
pub(crate) fn clear_ring_stats_provider() {
    *RING_STATS_PROVIDER.write() = None;
}

/// Clear the governance provider. Used by tests once they're added
/// for the dashboard renderer (Phase 4.5).
#[cfg(test)]
#[allow(dead_code)] // consumed by Phase 4.5 dashboard tests
pub(crate) fn clear_governance_provider() {
    *GOVERNANCE_PROVIDER.write() = None;
}

static BAN_LIST_PROVIDER: parking_lot::RwLock<Option<BanListProvider>> =
    parking_lot::RwLock::new(None);

/// Register the dashboard's contract-ban-list data source (#4302).
/// Replaces any previously-registered provider so multi-node in-process
/// harnesses can re-wire to the current node.
pub fn set_ban_list_provider(provider: BanListProvider) {
    *BAN_LIST_PROVIDER.write() = Some(provider);
}

static HOSTING_PROVIDER: parking_lot::RwLock<Option<HostingProvider>> =
    parking_lot::RwLock::new(None);

/// Register the dashboard's demand-driven hosting data source (piece A,
/// #4642). Replaces any previously-registered provider so multi-node
/// in-process harnesses can re-wire to the current node.
pub fn set_hosting_provider(provider: HostingProvider) {
    *HOSTING_PROVIDER.write() = Some(provider);
}

/// Replaceable storage for the subscription provider. Wrapped in a
/// `RwLock<Option<…>>` rather than `OnceLock` so multi-node in-process
/// harnesses can re-wire the dashboard to the live node — and so a
/// repeated wiring is loud (replaces the previous provider) rather than
/// silently retaining the first one set, which is precisely the
/// silent-mirror failure mode this refactor exists to remove.
static SUBSCRIPTION_PROVIDER: parking_lot::RwLock<Option<SubscriptionProvider>> =
    parking_lot::RwLock::new(None);

/// Store a reference to the Router for the dashboard.
pub fn set_router(router: Arc<parking_lot::RwLock<Router>>) {
    // OnceLock::set returns Err if already initialized; this is expected on repeated calls
    #[allow(clippy::let_underscore_must_use)]
    let _ = ROUTER.set(router);
}

/// Get the Router reference (if set).
pub(crate) fn get_router() -> Option<Arc<parking_lot::RwLock<Router>>> {
    ROUTER.get().cloned()
}

/// Register the dashboard's subscription data source. Replaces any
/// previously-registered provider so multi-node in-process harnesses
/// can re-wire to the current node.
pub fn set_subscription_provider(provider: SubscriptionProvider) {
    *SUBSCRIPTION_PROVIDER.write() = Some(provider);
}

/// Clear the subscription provider. Used by tests; not called from
/// production.
#[cfg(test)]
pub(crate) fn clear_subscription_provider() {
    *SUBSCRIPTION_PROVIDER.write() = None;
}

/// Tracked network connection status for diagnostic display.
pub struct NetworkStatus {
    pub gateway_failures: Vec<GatewayFailure>,
    pub connection_attempts: u32,
    pub listening_port: u16,
    pub started_at: Instant,
    /// Known gateway addresses for detecting gateway-only connections.
    pub gateway_addresses: HashSet<SocketAddr>,
    /// Active peer connections.
    pub connected_peers: Vec<ConnectedPeer>,
    /// Freenet version string.
    pub version: String,
    /// This node's ring location.
    pub own_location: Option<f64>,
    /// This node's externally observed address (as seen by peers).
    pub external_address: Option<SocketAddr>,
    /// Operation counters.
    pub op_stats: OperationStats,
    /// NAT traversal counters.
    pub nat_stats: NatStats,
    /// Terminal advertisement-consult counters (hosting redesign piece C).
    pub terminal_consult_stats: TerminalConsultStats,
    /// Computed-upstream vs. stored-`is_upstream`-flag divergence counters
    /// (hosting redesign piece D, #4642 / #4671).
    pub upstream_divergence_stats: UpstreamDivergenceStats,
    /// Reconcile-controller SHADOW comparison counters (hosting redesign
    /// keystone step-2, #4642).
    pub reconcile_shadow_stats: ReconcileShadowStats,
}

/// Per-node counters measuring how often the demand-driven-hosting **computed
/// upstream** (`Ring::most_keyward_hosting_neighbor`) disagrees with the stored
/// `is_upstream` interest flag at the site that still consults the flag
/// (`OpManager::send_unsubscribe_upstream`).
///
/// The reconcile-core keystone (#4642 piece D) replaces the stored flag — which
/// drifts under interest-sync gossip (#4671) — with the computed upstream. This
/// first, behavior-preserving step computes upstream in parallel and records
/// whether the two agree, so the flag's real-world drift rate is legible in
/// production telemetry (via `router_snapshot`) before the flag is deleted. The
/// stored flag still drives the actual decision; these counters observe only.
#[derive(Default)]
pub struct UpstreamDivergenceStats {
    /// Times the computed upstream was compared against the stored `is_upstream`
    /// flag (the denominator — one per `send_unsubscribe_upstream` call).
    pub comparisons: u64,
    /// Of those, the times the computed upstream DISAGREED with the stored flag
    /// (different peer, or one `Some`/the other `None`).
    pub divergences: u64,
}

/// Per-node counters for the reconcile-controller SHADOW comparison (hosting
/// redesign keystone step-2, #4642).
///
/// At each on-`main` hosting decision site the shadow wiring builds a
/// `ReconcileInputs` snapshot, computes what the pure `reconcile` controller
/// WOULD do, and compares it — BY SET MEMBERSHIP — to what the current code
/// actually does. These counters aggregate that comparison so the controller's
/// real-world divergence from today's scattered decisions is legible in
/// production telemetry (via `router_snapshot`) BEFORE any decision is flipped
/// to the controller. The current code still drives every decision; these
/// observe only.
///
/// Volume-conscious: the decision sites increment these local counters
/// in-process (NO per-event telemetry emission); only the aggregate totals are
/// exported on the periodic snapshot cadence.
///
/// The per-action counters are SYMMETRIC-DIFFERENCE tallies: each counts the
/// comparisons in which that action was present in exactly one of the
/// {reconcile, actual} sets. Because the on-`main` sites do not yet implement
/// several controller actions (a collapse never `Retract`s a hosting
/// advertisement; nothing re-roots on upstream loss), a nonzero divergence on
/// those classes is EXPECTED field evidence of the gap the keystone closes, not
/// a health alarm — read them as the reconcile-vs-today delta.
#[derive(Default, Clone, Copy)]
pub struct ReconcileShadowStats {
    /// Total shadow comparisons performed (the denominator — one per decision
    /// site invocation).
    pub comparisons: u64,
    /// Of those, the comparisons whose reconcile action set differed from the
    /// actual behavior's set in ANY action class.
    pub divergences: u64,
    /// Per-action symmetric-difference tallies (action present in exactly one of
    /// the {reconcile, actual} sets).
    pub subscribe_diffs: u64,
    pub renew_diffs: u64,
    pub unsubscribe_diffs: u64,
    pub collapse_diffs: u64,
    pub announce_diffs: u64,
    pub retract_diffs: u64,
    pub reroot_search_diffs: u64,
}

/// Per-node counters for the terminal advertisement consult (invariant 5:
/// "findability is routing + on-demand advertisement").
///
/// A GET/SUBSCRIBE that routes to a terminus (closest peer it can reach,
/// can't route closer) consults the host-advertisements its neighbors sent
/// before returning NotFound. These scalars measure whether that consult
/// actually closes dead-ends. Per-node aggregate only — no per-contract
/// breakdown.
#[derive(Default)]
pub struct TerminalConsultStats {
    /// A terminus consulted its neighbor advertisements (one per terminus).
    pub attempts: u64,
    /// The consult found at least one advertised host to forward to.
    pub hits: u64,
    /// A consult forward resolved the request to Found/Subscribed.
    pub resolved_found: u64,
    /// A consult ran but the request still ended NotFound.
    pub still_not_found: u64,
}

/// A connected peer with metadata.
pub struct ConnectedPeer {
    pub address: SocketAddr,
    pub is_gateway: bool,
    pub location: Option<f64>,
    pub connected_since: Instant,
    pub peer_key_location: Option<PeerKeyLocation>,
}

/// Counters for each operation type: (success, failure).
#[derive(Default)]
pub struct OperationStats {
    pub gets: (u32, u32),
    pub puts: (u32, u32),
    pub updates: (u32, u32),
    pub subscribes: (u32, u32),
    /// Count of broadcast updates received via subscription streaming.
    /// These are push-based and don't have success/failure semantics.
    pub updates_received: u32,
}

/// Maximum number of recent NAT attempts to track for rolling trend.
const NAT_RECENT_WINDOW: usize = 20;

/// NAT traversal attempt counters with rolling trend.
#[derive(Default)]
pub struct NatStats {
    pub attempts: u32,
    pub successes: u32,
    /// Recent NAT attempt results (bounded to last NAT_RECENT_WINDOW).
    /// `true` = success, `false` = failure.
    recent: VecDeque<bool>,
}

impl NatStats {
    /// Record a NAT attempt and update the rolling window.
    pub fn record(&mut self, success: bool) {
        self.attempts = self.attempts.saturating_add(1);
        if success {
            self.successes = self.successes.saturating_add(1);
        }
        if self.recent.len() >= NAT_RECENT_WINDOW {
            self.recent.pop_front();
        }
        self.recent.push_back(success);
    }

    /// Number of recent attempts in the rolling window.
    pub fn recent_attempts(&self) -> u32 {
        self.recent.len() as u32
    }

    /// Number of recent successes in the rolling window.
    pub fn recent_successes(&self) -> u32 {
        self.recent.iter().filter(|&&s| s).count() as u32
    }
}

/// Operation type for recording results and per-operation routing telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub enum OpType {
    Get,
    Put,
    Update,
    Subscribe,
}

impl OpType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OpType::Get => "GET",
            OpType::Put => "PUT",
            OpType::Update => "UPDATE",
            OpType::Subscribe => "SUBSCRIBE",
        }
    }
}

impl std::fmt::Display for OpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A recorded failure when connecting to a gateway.
pub struct GatewayFailure {
    pub address: SocketAddr,
    pub reason: FailureReason,
}

/// Classified reason for a gateway connection failure.
pub enum FailureReason {
    /// Protocol version mismatch between local node and gateway.
    VersionMismatch { local: String, gateway: String },
    /// NAT traversal failed (max connection attempts reached).
    NatTraversalFailed,
    /// Connection timed out.
    Timeout,
    /// Other transport-level failure.
    Other(String),
}

/// Initialize the global network status tracker.
///
/// In production `init()` runs exactly once, at node startup, on one of the
/// two mutually-exclusive bring-up paths (`run_local_node` or `run_node`),
/// so the first call installs the tracker. If it is called again — which in
/// practice only happens in this module's own unit tests, each of which
/// re-initializes the global with its own port/version — the new status is
/// written **in place** into the existing `RwLock` rather than dropped.
///
/// The previous implementation relied on `OnceLock::set`, which is
/// first-write-wins: a second `init()` silently became a no-op. Because the
/// `OnceLock` outlives any single test (it is process-global and cannot be
/// reset by the `TEST_GLOBAL_STATE_LOCK` the tests serialize on), every test
/// running after the first `init()` inherited the first test's
/// `NetworkStatus` — wrong version, leaked gateway failures, leaked op stats.
/// That made the test outcomes order-dependent: they passed in isolation but
/// failed intermittently under parallel execution. Overwriting in place makes
/// `init()` deterministically reset the tracker on every call.
///
/// This refreshes only the `NetworkStatus` value. The separately-registered
/// providers (`SUBSCRIPTION_PROVIDER`, `GOVERNANCE_PROVIDER`,
/// `RING_STATS_PROVIDER`, `ROUTER`) live in their own statics with their own
/// replace-on-set semantics and are intentionally left untouched here.
pub fn init(listening_port: u16, gateway_addrs: HashSet<SocketAddr>, version: String) {
    let status = NetworkStatus {
        gateway_failures: Vec::new(),
        connection_attempts: 0,
        listening_port,
        started_at: Instant::now(),
        gateway_addresses: gateway_addrs,
        connected_peers: Vec::new(),
        version,
        own_location: None,
        external_address: None,
        op_stats: OperationStats::default(),
        nat_stats: NatStats::default(),
        terminal_consult_stats: TerminalConsultStats::default(),
        upstream_divergence_stats: UpstreamDivergenceStats::default(),
        reconcile_shadow_stats: ReconcileShadowStats::default(),
    };
    match NETWORK_STATUS.get() {
        // Already initialized: overwrite the existing tracker in place so
        // repeated `init()` calls are idempotent-overwrite rather than no-ops.
        Some(existing) => {
            if let Ok(mut guard) = existing.write() {
                *guard = status;
            }
        }
        // First call: install the tracker. A concurrent first-init race (two
        // threads both observing `None`) cannot occur in production — `init()`
        // is called exactly once, on a single linear startup path — and is
        // serialized by `TEST_GLOBAL_STATE_LOCK` in tests. If it ever did
        // occur, `set` picks exactly one winner and the loser's status (freshly
        // built, with no accumulated failures/peers/stats) is dropped. That is
        // the one window where the "every call overwrites" contract would not
        // hold, but it is unreachable by construction, so we don't pay to
        // recover the loser's value here.
        None => {
            #[allow(clippy::let_underscore_must_use)]
            let _ = NETWORK_STATUS.set(Arc::new(RwLock::new(status)));
        }
    }
}

/// Check if an address is a known gateway.
pub fn is_known_gateway(addr: &SocketAddr) -> bool {
    NETWORK_STATUS
        .get()
        .and_then(|s| s.read().ok())
        .is_some_and(|s| s.gateway_addresses.contains(addr))
}

/// Record a gateway connection failure with a classified reason.
pub fn record_gateway_failure(address: SocketAddr, reason: FailureReason) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connection_attempts = s.connection_attempts.saturating_add(1);
            // Track NAT failures
            if matches!(reason, FailureReason::NatTraversalFailed) {
                s.nat_stats.record(false);
            }
            s.gateway_failures.push(GatewayFailure { address, reason });
            // Keep only the most recent failures to avoid unbounded growth
            if s.gateway_failures.len() > 20 {
                s.gateway_failures.remove(0);
            }
        }
    }
}

/// Record a successful peer connection.
pub fn record_peer_connected(
    addr: SocketAddr,
    location: Option<f64>,
    peer_key_location: Option<PeerKeyLocation>,
) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            // Remove any existing entry for this address
            s.connected_peers.retain(|p| p.address != addr);
            let is_gateway = s.gateway_addresses.contains(&addr);
            s.connected_peers.push(ConnectedPeer {
                address: addr,
                is_gateway,
                location,
                connected_since: Instant::now(),
                peer_key_location,
            });
            s.gateway_failures.clear();
        }
    }
}

/// Record a peer disconnection.
pub fn record_peer_disconnected(addr: SocketAddr) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connected_peers.retain(|p| p.address != addr);
        }
    }
    // Free the per-peer metrics slot so the bounded table doesn't accumulate
    // stale entries across the lifetime of long-running gateways. LRU
    // eviction in `record_per_peer` is the safety net; this is the
    // best-effort eager cleanup.
    TRANSPORT_METRICS.remove_peer(addr);
}

/// Record an operation result for the dashboard "Operations" panel.
///
/// # Required call sites
///
/// This is a manually-mirrored counter: every code path that delivers a
/// terminal client-visible op result MUST call this exactly once with
/// the matching outcome. Forgetting a call site silently rots the
/// counter (issue #4009 / #4010 prior incidents). Current writers:
///
/// - `node.rs::report_result` for legacy state-machine ops (PUT/GET/
///   UPDATE/SUBSCRIBE that still flow through `OpManager.ops.*`).
/// - `operations/get/op_ctx_task.rs` `Done` arm (client-initiated GET).
/// - `operations/put/op_ctx_task.rs` `Done` arm (client-initiated PUT).
/// - `operations/subscribe/op_ctx_task.rs::deliver_outcome` (client-
///   initiated SUBSCRIBE; covers all `DriverOutcome` variants).
/// - `operations/update/op_ctx_task.rs::deliver_outcome` (client-
///   initiated UPDATE; covers all `DriverOutcome` variants).
///
/// Internal-only operations are intentionally NOT recorded; counting
/// them would inflate the user-facing counter with background traffic
/// (renewals fire every 2 minutes per active subscription, sub-op
/// SUBSCRIBE fires once per PUT/GET completion). Specifically:
///
/// - `operations/subscribe/op_ctx_task.rs::run_renewal_subscribe`
///   (subscription renewal driver, called from
///   `ring::connection_maintenance`).
/// - `operations/subscribe/op_ctx_task.rs::run_executor_subscribe`
///   (executor auto-subscribe, called from
///   `contract::executor::runtime`).
/// - Sub-operation SUBSCRIBE spawned by PUT/GET via
///   `Transaction::new_child_of` (gated in
///   `subscribe::op_ctx_task::deliver_outcome` by
///   `!client_tx.is_sub_operation()`).
/// - Sub-operation GET spawned by subscribe / executor
///   (`get::op_ctx_task::start_sub_op_get` returns through a oneshot
///   and never publishes via `result_router_tx`).
///
/// Audit: `grep -rn "record_op_result" crates/core/src/operations/`
/// must show coverage for every op type with a driver.
pub fn record_op_result(op_type: OpType, success: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            let counter = match op_type {
                OpType::Get => &mut s.op_stats.gets,
                OpType::Put => &mut s.op_stats.puts,
                OpType::Update => &mut s.op_stats.updates,
                OpType::Subscribe => &mut s.op_stats.subscribes,
            };
            if success {
                counter.0 = counter.0.saturating_add(1);
            } else {
                counter.1 = counter.1.saturating_add(1);
            }
        }
    }
}

/// Record a broadcast update received via subscription streaming.
pub fn record_update_received() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.op_stats.updates_received = s.op_stats.updates_received.saturating_add(1);
        }
    }
}

/// Record that a routing terminus consulted its neighbor advertisements
/// (hosting redesign piece C). One increment per terminus, regardless of
/// how many advertised hosts it then tries.
pub fn record_terminal_consult_attempt() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.terminal_consult_stats.attempts = s.terminal_consult_stats.attempts.saturating_add(1);
        }
    }
}

/// Record that a consult found at least one advertised host to forward to.
pub fn record_terminal_consult_hit() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.terminal_consult_stats.hits = s.terminal_consult_stats.hits.saturating_add(1);
        }
    }
}

/// Record that a consult forward resolved the request to Found/Subscribed.
pub fn record_terminal_consult_resolved_found() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.terminal_consult_stats.resolved_found =
                s.terminal_consult_stats.resolved_found.saturating_add(1);
        }
    }
}

/// Record that a consult ran but the request still ended NotFound.
pub fn record_terminal_consult_still_not_found() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.terminal_consult_stats.still_not_found =
                s.terminal_consult_stats.still_not_found.saturating_add(1);
        }
    }
}

/// Read the current terminal advertisement-consult counters (hosting redesign
/// piece C, #4646) for export to the `router_snapshot` telemetry event (#4658).
///
/// Returns `(attempts, hits, resolved_found, still_not_found)` — the four
/// per-node monotonic aggregates recorded by the consult sites — or `None`
/// before the `NETWORK_STATUS` singleton is initialized. `Ring` polls this on
/// the snapshot cadence and copies the values onto `RouterSnapshotInfo` so the
/// production findability baseline (dead-end decomposition) is legible in
/// central telemetry, not just the internal singleton / sim-only metrics.
pub fn terminal_consult_counts() -> Option<(u64, u64, u64, u64)> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let c = &s.terminal_consult_stats;
    Some((c.attempts, c.hits, c.resolved_found, c.still_not_found))
}

/// Record one computed-upstream vs. stored-`is_upstream`-flag comparison
/// (hosting redesign piece D, #4642 / #4671). Increments the `comparisons`
/// denominator every call and the `divergences` numerator when `diverged`.
///
/// Called from `OpManager::send_unsubscribe_upstream`, the site that still
/// consults the stored flag: it computes the upstream in parallel via
/// `Ring::most_keyward_hosting_neighbor` and reports whether the two disagree.
/// Behavior-preserving — the stored flag still drives the decision; this only
/// measures the flag's drift so it is legible in production telemetry before the
/// reconcile-core keystone deletes the flag.
pub fn record_upstream_divergence_comparison(diverged: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.upstream_divergence_stats.comparisons =
                s.upstream_divergence_stats.comparisons.saturating_add(1);
            if diverged {
                s.upstream_divergence_stats.divergences =
                    s.upstream_divergence_stats.divergences.saturating_add(1);
            }
        }
    }
}

/// Read the current computed-upstream vs. stored-flag divergence counters
/// (piece D, #4642 / #4671) for export to the `router_snapshot` telemetry event.
///
/// Returns `(comparisons, divergences)` — the per-node monotonic totals — or
/// `None` before the `NETWORK_STATUS` singleton is initialized. `Ring` polls this
/// on the snapshot cadence and copies the values onto `RouterSnapshotInfo` so the
/// stored-flag drift rate is visible in central telemetry, not just the internal
/// singleton, giving field evidence for the flag's removal.
pub fn upstream_divergence_counts() -> Option<(u64, u64)> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let d = &s.upstream_divergence_stats;
    Some((d.comparisons, d.divergences))
}

/// Record one reconcile-controller SHADOW comparison (keystone step-2, #4642).
///
/// Increments the `comparisons` denominator every call, the `divergences`
/// numerator when the reconcile action set differed from the actual behavior's
/// set in any action class, and each per-action tally whose class was in the
/// symmetric difference. Behavior-preserving — the current code drives the
/// decision; this only measures how far the controller WOULD diverge, so the
/// gap is legible in production telemetry ahead of the flip. Called from the
/// on-`main` hosting decision sites (collapse `send_unsubscribe_upstream`,
/// renewal `contracts_needing_renewal`).
pub fn record_reconcile_shadow_comparison(divergence: ReconcileActionDivergence) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            let r = &mut s.reconcile_shadow_stats;
            r.comparisons = r.comparisons.saturating_add(1);
            if divergence.any() {
                r.divergences = r.divergences.saturating_add(1);
            }
            if divergence.subscribe {
                r.subscribe_diffs = r.subscribe_diffs.saturating_add(1);
            }
            if divergence.renew {
                r.renew_diffs = r.renew_diffs.saturating_add(1);
            }
            if divergence.unsubscribe {
                r.unsubscribe_diffs = r.unsubscribe_diffs.saturating_add(1);
            }
            if divergence.collapse {
                r.collapse_diffs = r.collapse_diffs.saturating_add(1);
            }
            if divergence.announce {
                r.announce_diffs = r.announce_diffs.saturating_add(1);
            }
            if divergence.retract {
                r.retract_diffs = r.retract_diffs.saturating_add(1);
            }
            if divergence.reroot_search {
                r.reroot_search_diffs = r.reroot_search_diffs.saturating_add(1);
            }
        }
    }
}

/// Read the reconcile-controller SHADOW counters (keystone step-2, #4642) for
/// export to the `router_snapshot` telemetry event. Returns a copy of the
/// per-node monotonic totals, or `None` before the `NETWORK_STATUS` singleton is
/// initialized. `Ring` polls this on the snapshot cadence and copies the values
/// onto `RouterSnapshotInfo`.
pub fn reconcile_shadow_counts() -> Option<ReconcileShadowStats> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    Some(s.reconcile_shadow_stats)
}

/// Record a NAT traversal attempt.
pub fn record_nat_attempt(success: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.nat_stats.record(success);
        }
    }
}

/// Set this node's ring location.
pub fn set_own_location(location: f64) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.own_location = Some(location);
        }
    }
}

/// Set this node's externally observed address (as reported by peers).
pub fn set_external_address(addr: SocketAddr) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.external_address = Some(addr);
        }
    }
}

/// Read this node's externally observed address.
///
/// Test-only accessor: production code reads `external_address` via
/// [`get_snapshot`]. Used by the `set_own_addr` TOCTOU regression test in
/// `connection_manager.rs` to verify `own_addr` and `external_address` stay
/// consistent under concurrent writers (issue #4172).
#[cfg(test)]
pub(crate) fn external_address() -> Option<SocketAddr> {
    NETWORK_STATUS
        .get()
        .and_then(|status| status.read().ok().and_then(|s| s.external_address))
}

/// Process-wide serialization lock for tests that read or write the shared
/// `NETWORK_STATUS` global (directly, or indirectly via `set_external_address`
/// / `set_own_addr` / `try_set_own_addr`).
///
/// `NETWORK_STATUS` is process-global. Under `cargo test` (the project's
/// documented pre-commit command — see AGENTS.md) the whole lib-test binary
/// runs in ONE process with tests executing concurrently, so any two tests
/// touching this global race each other. CI's `cargo nextest` happens to mask
/// the race by giving every test its own process, but `cargo test` does not.
///
/// Every test in this crate that touches `NETWORK_STATUS` MUST acquire this
/// lock for its whole body, so the writes/reads are serialized under both
/// `cargo test` and `cargo nextest`. This lives at crate scope (not inside a
/// `mod tests`) precisely so tests in *other* modules — notably the
/// `set_own_addr` TOCTOU regression test in `connection_manager.rs` — can
/// share the exact same lock.
#[cfg(test)]
pub(crate) static TEST_GLOBAL_STATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// --- Snapshot types for rendering ---

/// Snapshot of the current network status for rendering.
pub struct NetworkStatusSnapshot {
    pub failures: Vec<FailureSnapshot>,
    pub connection_attempts: u32,
    pub open_connections: u32,
    pub elapsed_secs: u64,
    pub listening_port: u16,
    pub version: String,
    pub own_location: Option<f64>,
    /// This node's externally observed address (as seen by peers).
    pub external_address: Option<SocketAddr>,
    pub peers: Vec<PeerSnapshot>,
    pub contracts: Vec<ContractSnapshot>,
    pub op_stats: OpStatsSnapshot,
    pub nat_stats: NatStatsSnapshot,
    /// True if all connections are to gateways (no peer-to-peer connections).
    pub gateway_only: bool,
    /// Cumulative bytes uploaded (lifetime, never reset).
    pub bytes_uploaded: u64,
    /// Cumulative bytes downloaded (lifetime, never reset).
    pub bytes_downloaded: u64,
    /// Overall node health level for the "everything looks good" indicator.
    pub health: HealthLevel,
    /// Live ring-level statistics: connection count, hosted contracts.
    pub ring_stats: RingStatsSnapshot,
    /// Period transport metrics (current values, not reset on read).
    pub transport_snapshot: TransportSnapshot,
    /// Per-contract governance state (Phase 4). The dashboard's
    /// verdict block, contracts table, ring inner-ring, and
    /// distribution histogram all read from this. Empty when the
    /// governance manager has no contracts yet (cold start).
    ///
    /// `#[allow(dead_code)]` is on the inner types — see
    /// `GovernanceSnapshot` — and that flows through; CI lint
    /// strictness on `-D warnings` flags the outer field anyway, so
    /// we silence it here too with the same justification.
    #[allow(dead_code)] // consumed by the dashboard renderer (Phase 4.5)
    pub governance: GovernanceSnapshot,
    /// Contract ban list (Phase 7, #4302). Drives the "N contracts on
    /// ban list" tile and the per-entry list in the dashboard's ban-list
    /// card. Read from the canonical `Ring::contract_ban_list` via the
    /// provider closure — no mirrored counter to rot. Empty when nothing
    /// is banned (the common case).
    pub ban_list: BanListSnapshot,
    /// Demand-driven hosting state (piece A, #4642): the capability-relative
    /// RAM budget + per-contract Greedy-Dual keep_score that actually governs
    /// retention. Drives the "Demand-driven eviction" card. Read from the
    /// canonical hosting cache via the provider closure — no mirrored counter.
    pub hosting: HostingSnapshot,
}

/// Snapshot of the contract ban list for the dashboard (#4302).
///
/// Carries only data the canonical `Ring::contract_ban_list` already
/// exposes: the live count, the per-entry list (key + reason + time
/// remaining), and the cumulative capacity-rejection counter. There is
/// no mirrored state — `get_snapshot` rebuilds this on every dashboard
/// request from the provider closure.
#[derive(Debug, Clone, Default)]
pub struct BanListSnapshot {
    /// Number of currently-banned contracts (`ContractBanList::len`).
    /// Drives the count tile.
    pub count: usize,
    /// Total bans rejected because the list was at `MAX_BANNED_CONTRACTS`
    /// capacity. A non-zero value tells operators the cap is being hit.
    pub capacity_rejected_total: u64,
    /// One entry per currently-banned contract.
    pub entries: Vec<BanListEntry>,
}

/// One row of [`BanListSnapshot`]: a banned contract, why it was banned,
/// and how long until the ban lifts.
#[derive(Debug, Clone)]
pub struct BanListEntry {
    /// `ContractInstanceId.to_string()` — the same string the dashboard's
    /// other contract panels render.
    pub instance_id: String,
    /// Why the contract is banned: governance-automatic vs operator-driven.
    pub reason: BanReasonSnapshot,
    /// Seconds until the ban automatically lifts (`expires_at - now`,
    /// clamped at zero). Rendered as a human-readable "Ns" / "Nm" string.
    pub expires_in_secs: u64,
}

/// Public mirror of the `pub(crate)` `ring::contract_ban_list::BanReason`,
/// so the snapshot stays decoupled from the ring-internal enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BanReasonSnapshot {
    /// Auto-flipped by the governance reaper after `BanTriggered`.
    AutoMad,
    /// Operator-driven via the CLI / config flag (#4274).
    Operator,
}

/// Snapshot of the governance system's state, mirrored from
/// `crate::contract::governance::GovernanceManager`. Public-facing
/// mirror so the manager's `pub(crate)` types stay internal.
///
/// Fields are consumed by the dashboard renderer in `home_page.rs`
/// (Phase 4.5, landing in a subsequent commit). The `#[allow]` lives
/// here so the snapshot can be built and tested independently of the
/// renderer landing.
#[allow(dead_code)] // consumed by the dashboard renderer (Phase 4.5)
#[derive(Default)]
pub struct GovernanceSnapshot {
    /// Off / DryRun / Enforce — visible on the dashboard as the
    /// "mode" pill.
    pub mode: GovernanceModeSnapshot,
    /// One entry per contract the manager is currently tracking
    /// AND flagged (Borderline / WouldEvict / Evicted / Banned).
    /// Normal contracts are excluded to keep the snapshot cheap on
    /// large nodes — see `Ring::iter_flagged_scores`.
    pub contracts: Vec<ContractGovernanceEntry>,
    /// Total count of contracts the manager is scoring (including
    /// Normal). Surfaces on the dashboard as "Observed N / 30
    /// contracts needed for scoring" — gives operators a progress
    /// signal during the ramp-up window where the existing
    /// `contracts` list is empty.
    pub observed_count: usize,
    /// `min_samples` from the outlier detector config — the
    /// threshold at which the MAD distribution becomes reliable.
    /// Surfaced so the dashboard can render "Observed N / `M`"
    /// without hard-coding 30.
    pub min_samples: usize,
    /// Network-level statistics from the last reaper tick. Drives
    /// the median / MAD / threshold / sample-size mini-tiles.
    pub norms: NetworkNorms,
    /// When the reaper tick last ran. Surfaces as "Last evaluated
    /// Ns ago" on the dashboard so operators can tell if the
    /// scoring engine is alive even when there are no decisions
    /// to log. None when the reaper has not run yet.
    pub last_tick_at: Option<tokio::time::Instant>,
    /// Map from `ContractInstanceId.to_string()` → governance
    /// state. Lets the Subscribed Contracts table cross-reference
    /// each row to its current governance state without iterating
    /// the `contracts` list per row.
    pub state_by_id: std::collections::HashMap<String, GovernanceStateSnapshot>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GovernanceModeSnapshot {
    Off,
    #[default]
    DryRun,
    Enforce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernanceStateSnapshot {
    Normal,
    Borderline,
    WouldEvict,
    Evicted,
    Banned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernanceTransitionReasonSnapshot {
    FirstSeen,
    BorderlineEntered,
    ThresholdCrossed,
    Evicted,
    BanTriggered,
    Recovered,
    BanLifted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GovernanceSkipReasonSnapshot {
    InsufficientSamples,
    MadCollapsed,
    NoExtractableRatios,
}

#[allow(dead_code)] // consumed by the dashboard renderer (Phase 4.5)
#[derive(Debug, Clone)]
pub struct ContractGovernanceEntry {
    /// `key.id().to_string()` — same string the dashboard's
    /// contracts-table renders.
    pub instance_id: String,
    /// Short-form key (first ~10 chars) for the dashboard's compact
    /// labels on the ring.
    pub instance_id_short: String,
    pub state: GovernanceStateSnapshot,
    pub cost_used: f64,
    pub benefit_score: f64,
    /// log10(cost/benefit). None when benefit_score is too small to
    /// produce a stable ratio.
    pub log_ratio: Option<f64>,
    /// Age in seconds since first observation.
    pub age_secs: u64,
    /// Seconds since the last state transition.
    pub last_transition_secs_ago: u64,
    /// State history (bounded). Newest last.
    pub history: Vec<GovernanceTransitionEntry>,
}

#[allow(dead_code)] // consumed by the dashboard renderer (Phase 4.5)
#[derive(Debug, Clone)]
pub struct GovernanceTransitionEntry {
    /// Seconds ago this transition happened.
    pub secs_ago: u64,
    pub from: GovernanceStateSnapshot,
    pub to: GovernanceStateSnapshot,
    pub reason: GovernanceTransitionReasonSnapshot,
}

#[allow(dead_code)] // consumed by the dashboard renderer (Phase 4.5)
#[derive(Default, Debug, Clone)]
pub struct NetworkNorms {
    pub median_log_ratio: Option<f64>,
    pub mad: Option<f64>,
    pub threshold: Option<f64>,
    pub sample_size: usize,
    /// True if the threshold was clamped at the capacity ceiling
    /// instead of being driven by the MAD-derived value.
    pub capacity_ceiling_binding: bool,
    pub skip_reason: Option<GovernanceSkipReasonSnapshot>,
}

/// Overall health verdict for the node, synthesized from multiple signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthLevel {
    /// Node has peer-to-peer connections and things are working.
    Healthy,
    /// Connected but degraded (gateway-only, or all NAT attempts failing).
    Degraded,
    /// Still trying to establish connections.
    Connecting,
    /// No connections after extended time, or version mismatch.
    Trouble,
}

/// A snapshot of a single failure for display.
pub struct FailureSnapshot {
    pub address: SocketAddr,
    pub reason_html: String,
}

/// Snapshot of a connected peer.
pub struct PeerSnapshot {
    pub address: SocketAddr,
    pub is_gateway: bool,
    pub location: Option<f64>,
    pub connected_secs: u64,
    pub peer_key_location: Option<PeerKeyLocation>,
    /// Cumulative bytes sent to this peer.
    pub bytes_sent: u64,
    /// Cumulative bytes received from this peer.
    pub bytes_received: u64,
}

/// Snapshot of a subscribed contract.
pub struct ContractSnapshot {
    pub key_short: String,
    pub key_full: String,
    /// `ContractKey.id().to_string()` — the 32-byte content hash
    /// portion of the key. Distinct from `key_full` which carries
    /// the full ContractKey encoding (instance id + parameters /
    /// code-hash bookkeeping). Surfaced so the dashboard can
    /// cross-reference this contract against
    /// `GovernanceSnapshot.state_by_id`, which is keyed by
    /// `ContractInstanceId::to_string()`. Codex review of
    /// dashboard-polish PR caught the id/key string mismatch.
    pub instance_id: String,
    pub subscribed_secs: u64,
    pub last_updated_secs: Option<u64>,
    /// Whether this node is genuinely receiving updates for the contract —
    /// the real per-contract freshness signal (`is_hosting` is NOT; only an
    /// active subscription keeps the cache fresh, PR #3699).
    pub is_receiving_updates: bool,
    /// Whether real demand pins the contract (local client subscription or a
    /// registered downstream subscriber).
    pub in_use: bool,
}

/// Snapshot of the demand-driven hosting cache (piece A, #4642) for the
/// local-peer dashboard. This is the mechanism that actually governs
/// retention today — a capability-relative RAM budget plus a Greedy-Dual
/// `keep_score` per contract (`ring/hosting/{cache,demand}.rs`) — and it
/// replaced the dormant MAD `GovernanceManager` (#4296). Default (all zeros,
/// no contracts) when the node hosts nothing yet or the provider is unset.
#[derive(Default, Clone)]
pub struct HostingSnapshot {
    /// Configured RAM-scaled byte budget for hosted contract state.
    pub budget_bytes: u64,
    /// Bytes currently used by hosted contract state. Headroom = budget - used.
    pub used_bytes: u64,
    /// Number of contracts currently in the hosting cache.
    pub contract_count: u64,
    /// Monotonic count of contracts evicted specifically for being over budget.
    pub budget_evictions_total: u64,
    /// Over-budget evictions whose victim had genuine repeat demand (read >= 2
    /// times). Should stay near zero; a rising value means the demand estimate
    /// is mis-ordering the working set (the #4338 miscalibration symptom).
    pub evictions_of_recently_read_total: u64,
    /// Per-contract Greedy-Dual rows in EVICTION order (the next victim under
    /// budget pressure is first). May be truncated by the renderer; the full
    /// count is `contract_count`.
    pub contracts: Vec<HostedContractEntry>,
}

/// One hosted contract's demand-driven eviction row for the dashboard.
#[derive(Clone)]
pub struct HostedContractEntry {
    /// Full contract-key string.
    pub key_full: String,
    /// Truncated key for display.
    pub key_short: String,
    /// Greedy-Dual priority (`eviction_floor + predicted_demand`). Lowest evicts first.
    pub keep_score: f64,
    /// Stored per-contract read-demand estimate (reads/second).
    pub predicted_demand: f64,
    /// Per-contract memory cost (state bytes).
    pub size_bytes: u64,
    /// Read accesses (GET/SUBSCRIBE) observed over this entry's residency.
    pub read_count: u32,
    /// Whether the over-budget sweep would actually consider this contract for
    /// eviction: past its `min_ttl` age gate AND not pinned by demand
    /// (`contract_in_use`). The renderer badges "next to evict" on the first
    /// eligible row, NOT the raw lowest-keep-score row — a within-TTL or
    /// in-use low-score contract is skipped by the real sweep, so badging it
    /// would mislead the operator.
    pub eviction_eligible: bool,
}

/// Snapshot of operation stats. Each tuple is `(success_count, failure_count)`.
#[derive(Default)]
pub struct OpStatsSnapshot {
    pub gets: (u32, u32),
    pub puts: (u32, u32),
    pub updates: (u32, u32),
    pub subscribes: (u32, u32),
    /// Broadcast updates received via subscription streaming.
    pub updates_received: u32,
}

impl OpStatsSnapshot {
    /// Total number of operations (successes + failures across all types).
    pub fn total(&self) -> u32 {
        let sum = |pair: (u32, u32)| pair.0.saturating_add(pair.1);
        sum(self.gets)
            .saturating_add(sum(self.puts))
            .saturating_add(sum(self.updates))
            .saturating_add(sum(self.subscribes))
            .saturating_add(self.updates_received)
    }
}

/// Snapshot of NAT stats with rolling trend.
#[derive(Default)]
pub struct NatStatsSnapshot {
    pub attempts: u32,
    pub successes: u32,
    /// Recent attempts in the rolling window.
    pub recent_attempts: u32,
    /// Recent successes in the rolling window.
    pub recent_successes: u32,
}

/// Get a snapshot of the current network status for the dashboard.
pub fn get_snapshot() -> Option<NetworkStatusSnapshot> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let now = Instant::now();

    let failures = s
        .gateway_failures
        .iter()
        .map(|f| FailureSnapshot {
            address: f.address,
            reason_html: match &f.reason {
                FailureReason::VersionMismatch { local, gateway } => {
                    let local = html_escape(local);
                    let gateway = html_escape(gateway);
                    format!(
                        "<strong>Version mismatch</strong>: Your version: <code>{local}</code> \
                         — Gateway requires: <code>{gateway}</code><br>\
                         Run: <code>cargo install --force freenet --version {gateway}</code>"
                    )
                }
                FailureReason::NatTraversalFailed => {
                    let has_peer_connections = s.connected_peers.iter().any(|p| !p.is_gateway);
                    if has_peer_connections {
                        "<strong>NAT traversal failed</strong>: Could not connect to this \
                         peer. This is normal — not all NAT traversal attempts succeed."
                            .to_string()
                    } else {
                        format!(
                            "<strong>NAT traversal failed</strong>: Can't reach gateway. \
                             Check that UDP port <code>{}</code> is open in your firewall.",
                            s.listening_port
                        )
                    }
                }
                FailureReason::Timeout => {
                    "<strong>Connection timed out</strong>: Gateway did not respond.".to_string()
                }
                FailureReason::Other(msg) => {
                    format!("<strong>Connection failed</strong>: {}", html_escape(msg))
                }
            },
        })
        .collect();

    // Get per-peer transfer stats from transport metrics
    let per_peer_metrics: HashMap<SocketAddr, (u64, u64)> = TRANSPORT_METRICS
        .per_peer_snapshot()
        .into_iter()
        .map(|(addr, sent, recv)| (addr, (sent, recv)))
        .collect();

    let peers: Vec<PeerSnapshot> = s
        .connected_peers
        .iter()
        .map(|p| {
            let (sent, recv) = per_peer_metrics.get(&p.address).copied().unwrap_or((0, 0));
            PeerSnapshot {
                address: p.address,
                is_gateway: p.is_gateway,
                location: p.location,
                connected_secs: now.duration_since(p.connected_since).as_secs(),
                peer_key_location: p.peer_key_location.clone(),
                bytes_sent: sent,
                bytes_received: recv,
            }
        })
        .collect();

    let open_connections = peers.len() as u32;
    let gateway_only = open_connections > 0 && peers.iter().all(|p| p.is_gateway);

    // Read subscribed contracts from the registered provider, which in
    // production points at the canonical lease map in `HostingManager`.
    // Sort order is set inside the provider (most recently updated first;
    // never-updated entries fall to the end with a deterministic key
    // tie-break).
    let contracts: Vec<ContractSnapshot> = SUBSCRIPTION_PROVIDER
        .read()
        .as_ref()
        .map(|provider| {
            provider()
                .into_iter()
                .map(|c| {
                    let key_full = c.key.to_string();
                    let instance_id = c.key.id().to_string();
                    // Use char boundary for safe truncation (contract keys
                    // are base58/ASCII, but be defensive against future
                    // encoding changes).
                    let key_short = if key_full.chars().count() > 12 {
                        let trunc: String = key_full.chars().take(12).collect();
                        format!("{trunc}...")
                    } else {
                        key_full.clone()
                    };
                    ContractSnapshot {
                        key_short,
                        key_full,
                        instance_id,
                        subscribed_secs: c.subscribed_secs,
                        last_updated_secs: c.last_updated_secs,
                        is_receiving_updates: c.is_receiving_updates,
                        in_use: c.in_use,
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    let elapsed_secs = s.started_at.elapsed().as_secs();
    let has_version_mismatch = s
        .gateway_failures
        .iter()
        .any(|f| matches!(f.reason, FailureReason::VersionMismatch { .. }));
    let nat_all_failing = s.nat_stats.attempts > 0 && s.nat_stats.successes == 0;

    let health = if has_version_mismatch || (open_connections == 0 && elapsed_secs > 60) {
        HealthLevel::Trouble
    } else if open_connections == 0 {
        HealthLevel::Connecting
    } else if gateway_only || nat_all_failing {
        HealthLevel::Degraded
    } else {
        HealthLevel::Healthy
    };

    let bytes_uploaded = TRANSPORT_METRICS.cumulative_bytes_sent();
    let bytes_downloaded = TRANSPORT_METRICS.cumulative_bytes_received();

    let ring_stats = RING_STATS_PROVIDER
        .read()
        .as_ref()
        .map(|provider| provider())
        .unwrap_or_default();

    let transport_snapshot = TRANSPORT_METRICS.read_snapshot();

    Some(NetworkStatusSnapshot {
        failures,
        connection_attempts: s.connection_attempts,
        open_connections,
        elapsed_secs,
        listening_port: s.listening_port,
        version: s.version.clone(),
        own_location: s.own_location,
        external_address: s.external_address,
        peers,
        contracts,
        op_stats: OpStatsSnapshot {
            gets: s.op_stats.gets,
            puts: s.op_stats.puts,
            updates: s.op_stats.updates,
            subscribes: s.op_stats.subscribes,
            updates_received: s.op_stats.updates_received,
        },
        nat_stats: NatStatsSnapshot {
            attempts: s.nat_stats.attempts,
            successes: s.nat_stats.successes,
            recent_attempts: s.nat_stats.recent_attempts(),
            recent_successes: s.nat_stats.recent_successes(),
        },
        gateway_only,
        bytes_uploaded,
        bytes_downloaded,
        health,
        ring_stats,
        transport_snapshot,
        governance: GOVERNANCE_PROVIDER
            .read()
            .as_ref()
            .map(|provider| provider())
            .unwrap_or_default(),
        ban_list: BAN_LIST_PROVIDER
            .read()
            .as_ref()
            .map(|provider| provider())
            .unwrap_or_default(),
        hosting: HOSTING_PROVIDER
            .read()
            .as_ref()
            .map(|provider| provider())
            .unwrap_or_default(),
    })
}

/// Classify a ConnectionError::TransportError string into a FailureReason.
pub fn classify_transport_error(error_msg: &str) -> FailureReason {
    if error_msg.contains("Version incompatibility") {
        // Parse versions from the TransportError::ProtocolVersionMismatch Display output:
        // "Version incompatibility with gateway\n  Your client version: {actual}\n  Gateway version: {expected}\n..."
        let local = error_msg
            .split("Your client version:")
            .nth(1)
            .and_then(|s| s.split('\n').next())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let gateway = error_msg
            .split("Gateway version:")
            .nth(1)
            .and_then(|s| s.split('\n').next())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        FailureReason::VersionMismatch { local, gateway }
    } else if error_msg.contains("max connection attempts reached") {
        FailureReason::NatTraversalFailed
    } else {
        FailureReason::Other(error_msg.to_string())
    }
}

/// Minimal HTML escaping for untrusted error messages.
pub(crate) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Format seconds as a human-readable relative time string.
pub(crate) fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        let m = secs / 60;
        let s = secs % 60;
        if s == 0 {
            format!("{m}m")
        } else {
            format!("{m}m {s}s")
        }
    } else {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if m == 0 {
            format!("{h}h")
        } else {
            format!("{h}h {m}m")
        }
    }
}

/// Format seconds as a human-readable "ago" string.
pub(crate) fn format_ago(secs: u64) -> String {
    if secs < 5 {
        "just now".to_string()
    } else {
        format!("{} ago", format_duration(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    /// Tests that touch the shared NETWORK_STATUS global must hold this lock
    /// to prevent interleaving with other tests running in parallel.
    ///
    /// This is the crate-wide [`TEST_GLOBAL_STATE_LOCK`] — shared with the
    /// `set_own_addr` TOCTOU regression test in `connection_manager.rs`, which
    /// also mutates this global. A poisoned lock here would mean some test
    /// panicked while holding it; `.lock()` would then return `Err`, which is
    /// itself a useful signal, so the `.unwrap()` is intentional.
    use super::TEST_GLOBAL_STATE_LOCK as TEST_MUTEX;

    #[test]
    fn test_classify_version_mismatch() {
        let msg = "Version incompatibility with gateway\n  Your client version: 0.1.133\n  Gateway version: 0.1.135\n  \n  To fix this, update your Freenet client:\n    cargo install --force freenet --version 0.1.135";
        let reason = classify_transport_error(msg);
        match reason {
            FailureReason::VersionMismatch { local, gateway } => {
                assert_eq!(local, "0.1.133");
                assert_eq!(gateway, "0.1.135");
            }
            FailureReason::NatTraversalFailed
            | FailureReason::Timeout
            | FailureReason::Other(_) => panic!("Expected VersionMismatch"),
        }
    }

    #[test]
    fn test_classify_nat_traversal() {
        let msg = "failed while establishing connection, reason: max connection attempts reached";
        let reason = classify_transport_error(msg);
        assert!(matches!(reason, FailureReason::NatTraversalFailed));
    }

    #[test]
    fn test_classify_other() {
        let msg = "some unknown error";
        let reason = classify_transport_error(msg);
        assert!(matches!(reason, FailureReason::Other(_)));
    }

    #[test]
    fn test_snapshot_without_init_returns_none() {
        // Without calling init(), get_snapshot returns None (in a fresh process).
        // Can't reliably test since OnceLock is global, but the function handles it.
        let _ = get_snapshot();
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a&b"), "a&amp;b");
    }

    /// End-to-end for #4658: the terminal-consult record sites must feed the
    /// `terminal_consult_counts()` getter that `Ring` polls for the
    /// `router_snapshot` telemetry export. `init()` resets the counters, each
    /// `record_*` bumps its own field, and the getter returns them in
    /// `(attempts, hits, resolved_found, still_not_found)` order — so a wiring
    /// mistake (wrong field, dropped increment, transposed getter tuple) fails
    /// here rather than silently emitting zeros in production.
    #[test]
    fn terminal_consult_counts_reflect_recorded_events() {
        let _lock = TEST_MUTEX.lock().unwrap();
        // Fresh singleton state for a deterministic baseline (init overwrites
        // the process-global tracker in place, zeroing the consult counters).
        init(31337, HashSet::new(), "test".to_string());

        assert_eq!(
            terminal_consult_counts(),
            Some((0, 0, 0, 0)),
            "counters start at zero after init"
        );

        // Distinct counts per field so a transposition can't pass.
        record_terminal_consult_attempt();
        record_terminal_consult_attempt();
        record_terminal_consult_attempt();
        record_terminal_consult_hit();
        record_terminal_consult_hit();
        record_terminal_consult_resolved_found();
        record_terminal_consult_still_not_found();
        record_terminal_consult_still_not_found();
        record_terminal_consult_still_not_found();
        record_terminal_consult_still_not_found();

        assert_eq!(
            terminal_consult_counts(),
            Some((3, 2, 1, 4)),
            "getter returns (attempts, hits, resolved_found, still_not_found)"
        );
    }

    /// End-to-end for piece D (#4642 / #4671): the divergence record site must
    /// feed the `upstream_divergence_counts()` getter that `Ring` polls for the
    /// `router_snapshot` export. `comparisons` bumps every call, `divergences`
    /// only when `diverged`, and the getter returns them in
    /// `(comparisons, divergences)` order — so a dropped increment or transposed
    /// tuple fails here rather than silently emitting zeros in production.
    #[test]
    fn upstream_divergence_counts_reflect_recorded_events() {
        let _lock = TEST_MUTEX.lock().unwrap();
        // Fresh singleton state for a deterministic baseline (init overwrites
        // the process-global tracker in place, zeroing the counters).
        init(31337, HashSet::new(), "test".to_string());

        assert_eq!(
            upstream_divergence_counts(),
            Some((0, 0)),
            "counters start at zero after init"
        );

        // 5 comparisons, 2 of them divergent → distinct so a transposition fails.
        record_upstream_divergence_comparison(false);
        record_upstream_divergence_comparison(true);
        record_upstream_divergence_comparison(false);
        record_upstream_divergence_comparison(true);
        record_upstream_divergence_comparison(false);

        assert_eq!(
            upstream_divergence_counts(),
            Some((5, 2)),
            "getter returns (comparisons, divergences); every call bumps comparisons, \
             only diverged calls bump divergences"
        );
    }

    /// End-to-end for the reconcile-controller SHADOW counters (keystone step-2,
    /// #4642): the record site must feed the `reconcile_shadow_counts()` getter
    /// that `Ring` polls for the `router_snapshot` export. `comparisons` bumps
    /// every call, `divergences` only when any action class diverged, and each
    /// per-action tally bumps only when that class is in the symmetric
    /// difference — so a dropped increment or a mis-wired per-action field fails
    /// here rather than silently emitting zeros in production.
    #[test]
    fn reconcile_shadow_counts_reflect_recorded_events() {
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31337, HashSet::new(), "test".to_string());

        let zero = reconcile_shadow_counts().expect("counters present after init");
        assert_eq!(
            (zero.comparisons, zero.divergences),
            (0, 0),
            "counters start at zero after init"
        );

        // No divergence: bumps comparisons only.
        record_reconcile_shadow_comparison(ReconcileActionDivergence::default());
        // Retract-only divergence (the collapse-site gap).
        record_reconcile_shadow_comparison(ReconcileActionDivergence {
            retract: true,
            ..Default::default()
        });
        // Subscribe + Renew divergence (the renewal-site not-subscribed case).
        record_reconcile_shadow_comparison(ReconcileActionDivergence {
            subscribe: true,
            renew: true,
            ..Default::default()
        });

        let c = reconcile_shadow_counts().expect("counters present");
        assert_eq!(c.comparisons, 3, "every call bumps comparisons");
        assert_eq!(
            c.divergences, 2,
            "only the two diverging calls bump divergences"
        );
        assert_eq!(c.retract_diffs, 1);
        assert_eq!(c.subscribe_diffs, 1);
        assert_eq!(c.renew_diffs, 1);
        // Untouched classes stay at zero.
        assert_eq!(c.unsubscribe_diffs, 0);
        assert_eq!(c.collapse_diffs, 0);
        assert_eq!(c.announce_diffs, 0);
        assert_eq!(c.reroot_search_diffs, 0);
    }

    #[test]
    fn test_failure_snapshot_rendering() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 31337);

        // Test version mismatch rendering
        let reason = FailureReason::VersionMismatch {
            local: "0.1.1".to_string(),
            gateway: "0.1.2".to_string(),
        };
        init(31338, HashSet::new(), "0.1.0".to_string());
        record_gateway_failure(addr, reason);
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.failures.len(), 1);
        assert!(snap.failures[0].reason_html.contains("Version mismatch"));
        assert!(snap.failures[0].reason_html.contains("0.1.1"));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(7200), "2h");
    }

    #[test]
    fn test_format_ago() {
        assert_eq!(format_ago(2), "just now");
        assert_eq!(format_ago(30), "30s ago");
        assert_eq!(format_ago(120), "2m ago");
    }

    #[test]
    fn test_gateway_only_detection() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let gw_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 9, 111, 215)), 31337);
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        init(31339, HashSet::new(), "0.1.148".to_string());

        let status = NETWORK_STATUS.get().unwrap();

        // Gateway-only: one gateway peer connected
        {
            let mut s = status.write().unwrap();
            s.gateway_addresses.insert(gw_addr);
            s.connected_peers.clear();
            s.gateway_failures.clear();
            s.connected_peers.push(ConnectedPeer {
                address: gw_addr,
                is_gateway: true,
                location: Some(0.5),
                connected_since: Instant::now(),
                peer_key_location: None,
            });
        }
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Not gateway-only: add a non-gateway peer
        {
            let mut s = status.write().unwrap();
            s.connected_peers.push(ConnectedPeer {
                address: peer_addr,
                is_gateway: false,
                location: Some(0.3),
                connected_since: Instant::now(),
                peer_key_location: None,
            });
        }
        let snap = get_snapshot().unwrap();
        assert!(!snap.gateway_only);

        // Back to gateway-only: remove non-gateway peer
        {
            let mut s = status.write().unwrap();
            s.connected_peers.retain(|p| p.address != peer_addr);
        }
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Cleanup
        {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
        }
    }

    #[test]
    fn test_op_stats_recording() {
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31340, HashSet::new(), "0.1.148".to_string());

        // Reset op stats to avoid interference from other tests
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.op_stats = OperationStats::default();
        }

        record_op_result(OpType::Get, true);
        record_op_result(OpType::Get, true);
        record_op_result(OpType::Get, false);
        record_op_result(OpType::Put, true);
        // Issue #4010: SUBSCRIBE / UPDATE counters were stuck at
        // zero because the drivers stopped recording outcomes.
        // Cover both op types and both outcomes (success + failure) so
        // the per-op tabulation is pinned end to end. The two
        // `OpType::Update, true` calls are intentional, not a copy
        // paste: we want the success counter to assert `2`, paired
        // with one failure for `1`.
        record_op_result(OpType::Subscribe, true);
        record_op_result(OpType::Subscribe, false);
        record_op_result(OpType::Update, true);
        record_op_result(OpType::Update, true);
        record_op_result(OpType::Update, false);

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.op_stats.gets, (2, 1));
        assert_eq!(snap.op_stats.puts, (1, 0));
        assert_eq!(snap.op_stats.subscribes, (1, 1));
        assert_eq!(snap.op_stats.updates, (2, 1));
    }

    #[test]
    fn test_subscription_tracking_no_provider_wired() {
        // With no provider registered, `get_snapshot()` reports zero
        // subscribed contracts (rather than panicking).
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31341, HashSet::new(), "0.1.148".to_string());
        clear_subscription_provider();
        let snap = get_snapshot().unwrap();
        assert!(snap.contracts.is_empty());
    }

    /// Regression test for the dashboard "Subscribed Contracts" panel.
    ///
    /// Pins the production wiring: when the subscription provider is
    /// registered and yields contracts, `get_snapshot().contracts`
    /// renders them with the same field shape (`key_short`, `key_full`,
    /// `subscribed_secs`, `last_updated_secs`) the dashboard template
    /// expects. The bug this PR fixed was a silent break in this
    /// path after the SUBSCRIBE migration stopped invoking the
    /// legacy mirror — without an end-to-end test, the same break
    /// could happen at the `set_subscription_provider` seam in a
    /// future refactor.
    #[test]
    fn test_subscription_provider_wired_renders_contracts() {
        use freenet_stdlib::prelude::{ContractCode, ContractInstanceId, ContractKey};

        let _lock = TEST_MUTEX.lock().unwrap();
        init(31350, HashSet::new(), "0.1.148".to_string());

        // Two contracts with deterministic keys; the base58 encoding is
        // long enough to exercise the >12-char truncation path.
        let code_hash = *ContractCode::from(vec![0u8; 32]).hash();
        let key_a = ContractKey::from_id_and_code(ContractInstanceId::new([0xAA; 32]), code_hash);
        let key_b = ContractKey::from_id_and_code(ContractInstanceId::new([0xBB; 32]), code_hash);
        let snapshot = vec![
            crate::ring::SubscribedContractSnapshot {
                key: key_a,
                subscribed_secs: 30,
                last_updated_secs: Some(5),
                is_receiving_updates: true,
                in_use: true,
            },
            crate::ring::SubscribedContractSnapshot {
                key: key_b,
                subscribed_secs: 30,
                last_updated_secs: None,
                is_receiving_updates: true,
                in_use: true,
            },
        ];
        set_subscription_provider(Arc::new(move || snapshot.clone()));

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.contracts.len(), 2);
        assert_eq!(snap.contracts[0].key_full, key_a.to_string());
        assert_eq!(snap.contracts[0].subscribed_secs, 30);
        assert_eq!(snap.contracts[0].last_updated_secs, Some(5));
        // key_short should be truncated to the first 12 chars + ellipsis
        // for any base58-encoded ContractKey.
        assert!(snap.contracts[0].key_short.ends_with("..."));
        assert_eq!(snap.contracts[0].key_short.chars().count(), 15);

        // Replacing the provider must take effect immediately — the
        // OnceLock-style "first set wins" pattern would silently fail
        // multi-node test harnesses, exactly the failure mode this
        // refactor exists to remove.
        set_subscription_provider(Arc::new(Vec::new));
        let snap = get_snapshot().unwrap();
        assert!(snap.contracts.is_empty());

        clear_subscription_provider();
    }

    #[test]
    fn test_nat_stats() {
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31342, HashSet::new(), "0.1.148".to_string());

        // Reset NAT stats to avoid interference from other tests
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.nat_stats = NatStats::default();
        }

        record_nat_attempt(true);
        record_nat_attempt(false);
        record_nat_attempt(false);

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.nat_stats.attempts, 3);
        assert_eq!(snap.nat_stats.successes, 1);
    }

    #[test]
    fn test_nat_failure_no_peers_shows_firewall_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 31337);
        init(31343, HashSet::new(), "0.1.148".to_string());

        // Ensure clean state: no peers, no failures
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        record_gateway_failure(addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("Check that UDP port"));
        assert!(html.contains("open in your firewall"));
    }

    #[test]
    fn test_nat_failure_with_peer_connections_shows_benign_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let fail_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 31337);
        init(31344, HashSet::new(), "0.1.148".to_string());

        // Ensure clean state
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        // Connect a non-gateway peer, then record a NAT failure to a different peer
        record_peer_connected(peer_addr, Some(0.5), None);
        record_gateway_failure(fail_addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("This is normal"));
        assert!(!html.contains("firewall"));

        // Cleanup
        record_peer_disconnected(peer_addr);
    }

    #[test]
    fn test_nat_failure_with_only_gateway_connections_shows_firewall_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let gw_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 9, 111, 215)), 31337);
        let fail_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 31337);
        init(31345, HashSet::new(), "0.1.148".to_string());

        // Register gateway and ensure clean state
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.gateway_addresses.insert(gw_addr);
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        // Connect only a gateway peer, then record NAT failure
        record_peer_connected(gw_addr, None, None);
        record_gateway_failure(fail_addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        // With only gateway connections, should still show firewall warning
        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("firewall"));
        assert!(!html.contains("This is normal"));

        // Cleanup
        record_peer_disconnected(gw_addr);
    }

    #[test]
    fn test_nat_stats_rolling_window() {
        let mut nat = NatStats::default();

        // Record 10 successes then 15 failures (25 total)
        for _ in 0..10 {
            nat.record(true);
        }
        for _ in 0..15 {
            nat.record(false);
        }

        // Lifetime counters track everything
        assert_eq!(nat.attempts, 25);
        assert_eq!(nat.successes, 10);

        // Rolling window only keeps last 20 (5 successes + 15 failures)
        assert_eq!(nat.recent_attempts(), 20);
        assert_eq!(nat.recent_successes(), 5);
    }

    #[test]
    fn test_record_peer_connected_updates_location() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9999);
        init(31339, HashSet::new(), "0.1.148".to_string());

        // First call with no location (as handle_successful_connection does initially)
        record_peer_connected(peer_addr, None, None);
        let snap = get_snapshot().unwrap();
        let peer = snap.peers.iter().find(|p| p.address == peer_addr).unwrap();
        assert_eq!(peer.location, None);

        // Second call with location (as connect_peer promotion path now does)
        record_peer_connected(peer_addr, Some(0.42), None);
        let snap = get_snapshot().unwrap();
        let peer = snap.peers.iter().find(|p| p.address == peer_addr).unwrap();
        assert_eq!(peer.location, Some(0.42));

        // Cleanup
        record_peer_disconnected(peer_addr);
    }

    #[test]
    fn test_nat_stats_rolling_window_all_failures() {
        let mut nat = NatStats::default();
        for _ in 0..25 {
            nat.record(false);
        }
        assert_eq!(nat.recent_attempts(), 20);
        assert_eq!(nat.recent_successes(), 0);
    }

    /// Regression: `init()` must seed the global so `get_snapshot()`
    /// returns `Some(...)` immediately — without this, the dashboard
    /// shows "Starting up…" forever in local mode (#3507 / PR #4297).
    #[test]
    fn init_populates_snapshot() {
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31337, HashSet::new(), "0.1.0-test".to_string());
        let snap = get_snapshot();
        assert!(snap.is_some(), "snapshot should be Some after init");
        let s = snap.unwrap();
        assert_eq!(s.version, "0.1.0-test");
        assert_eq!(s.listening_port, 31337);
        assert_eq!(s.open_connections, 0);
    }

    /// Regression: a second `init()` must overwrite the global tracker in
    /// place rather than no-op.
    ///
    /// The original implementation used `OnceLock::set` (first-write-wins),
    /// so once any test initialized the process-global `NETWORK_STATUS`,
    /// every later `init()` was silently dropped. The version/port/failures
    /// from the first call leaked into every subsequent test, making the
    /// whole module order-dependent and intermittently failing under
    /// parallel execution. This test pins the fix deterministically: it
    /// initializes twice, with a recorded failure between the calls, and
    /// asserts the second call's values fully replace the first's.
    #[test]
    fn init_overwrites_existing_global() {
        let _lock = TEST_MUTEX.lock().unwrap();

        // First initialization, then dirty the state so we can prove the
        // second init() resets it (not just changes scalar fields).
        init(40001, HashSet::new(), "first-version".to_string());
        record_gateway_failure(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 31337),
            FailureReason::Timeout,
        );
        let first = get_snapshot().unwrap();
        assert_eq!(first.version, "first-version");
        assert_eq!(first.listening_port, 40001);
        assert_eq!(
            first.failures.len(),
            1,
            "failure recorded against the first tracker"
        );

        // Second initialization with different values must take effect
        // immediately and reset the accumulated state.
        init(40002, HashSet::new(), "second-version".to_string());
        let second = get_snapshot().unwrap();
        assert_eq!(
            second.version, "second-version",
            "second init() version must win (first-write-wins would keep `first-version`)"
        );
        assert_eq!(second.listening_port, 40002);
        assert!(
            second.failures.is_empty(),
            "second init() must reset accumulated gateway failures"
        );
    }

    /// Registered RingStatsProvider must surface in `snap.ring_stats`,
    /// replacing the provider must take effect immediately, and the
    /// no-provider default is all-zeros.
    #[test]
    fn ring_stats_provider_round_trip() {
        let _lock = TEST_MUTEX.lock().unwrap();
        // Use the same init as other tests in this module.
        init(31337, HashSet::new(), "0.1.0-test".to_string());

        // 1. No provider — default.
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.ring_stats.connection_count, 0);
        assert!(snap.ring_stats.own_pub_key.is_empty());

        // 2. Register provider.
        set_ring_stats_provider(Arc::new(|| RingStatsSnapshot {
            connection_count: 42,
            hosted_contracts: 7,
            peer_id: "abc".to_string(),
            own_pub_key: "test-key".to_string(),
            updates_accepted: 1000,
            updates_rate_limited: 13,
            updates_capacity_dropped: 2,
        }));
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.ring_stats.connection_count, 42);
        assert_eq!(snap.ring_stats.peer_id, "abc");
        assert_eq!(snap.ring_stats.own_pub_key, "test-key");
        assert_eq!(snap.ring_stats.updates_accepted, 1000);
        assert_eq!(snap.ring_stats.updates_rate_limited, 13);
        assert_eq!(snap.ring_stats.updates_capacity_dropped, 2);

        // 3. Replace provider — must take effect immediately.
        set_ring_stats_provider(Arc::new(|| RingStatsSnapshot {
            connection_count: 99,
            hosted_contracts: 1,
            peer_id: "xyz".to_string(),
            own_pub_key: "new-key".to_string(),
            ..Default::default()
        }));
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.ring_stats.connection_count, 99);
        assert_eq!(snap.ring_stats.peer_id, "xyz");
        assert_eq!(snap.ring_stats.own_pub_key, "new-key");

        // 4. Clean up.
        clear_ring_stats_provider();
    }
}
