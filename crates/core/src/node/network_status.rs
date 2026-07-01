//! Network connection status tracking for the node dashboard.
//!
//! Surfaces diagnostic information (version mismatches, NAT traversal failures,
//! peer connections, subscriptions, operation stats, etc.) so the HTTP homepage
//! and connecting page can show actionable diagnostics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

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
    /// Count of event-driven re-subscribes this node fired because an UPSTREAM
    /// peer dropped (#4642 piece F). Each is a fresh SUBSCRIBE routed toward the
    /// key immediately on upstream-loss detection (after the shared
    /// ban/backoff/dedup gates), rather than waiting for the periodic renewal
    /// cycle — a per-node measure of how often self-healing re-rooting fired.
    pub event_driven_resubscribes: u64,
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

/// Record an event-driven re-subscribe fired on upstream-peer loss (#4642
/// piece F). Per-node aggregate scalar; incremented once per fresh SUBSCRIBE
/// spawned by `OpManager::on_ring_connection_lost` for a contract whose upstream
/// just dropped.
pub fn record_event_driven_resubscribe() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.op_stats.event_driven_resubscribes =
                s.op_stats.event_driven_resubscribes.saturating_add(1);
        }
    }
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
    /// Event-driven re-subscribes fired on upstream-peer loss (#4642 piece F).
    pub event_driven_resubscribes: u64,
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
            event_driven_resubscribes: s.op_stats.event_driven_resubscribes,
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
    fn test_event_driven_resubscribe_recording() {
        // #4642 piece F: the per-node event-driven re-subscribe scalar must
        // accumulate through `record_event_driven_resubscribe()` and surface in
        // the snapshot.
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31346, HashSet::new(), "0.1.148".to_string());

        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.op_stats = OperationStats::default();
        }

        assert_eq!(
            get_snapshot().unwrap().op_stats.event_driven_resubscribes,
            0
        );

        record_event_driven_resubscribe();
        record_event_driven_resubscribe();
        record_event_driven_resubscribe();

        assert_eq!(
            get_snapshot().unwrap().op_stats.event_driven_resubscribes,
            3
        );
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
            },
            crate::ring::SubscribedContractSnapshot {
                key: key_b,
                subscribed_secs: 30,
                last_updated_secs: None,
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
