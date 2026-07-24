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
    /// Nearest-neighbor ring lattice completeness (the "is greedy routing's base
    /// lattice present" signal). `lattice_has_successor` / `_predecessor` are
    /// whether this peer currently HOLDS (a side is FILLED with) its
    /// closest-higher / closest-lower connected ring neighbor; a peer with BOTH
    /// `true` has a complete both-sides lattice, and the collector aggregates the
    /// fraction of such peers.
    ///
    /// CAVEAT — filled != tight: a held edge may still be LOOSE (the exact
    /// nearest is an unconnected peer between two adjacent peers), so the
    /// both-sides-filled fraction OVERSTATES exact-nearest coverage. A peer
    /// cannot locally determine tightness (that is what the route-to-self probe
    /// discovers), so it is not reported as a boolean; compare the `_distance`
    /// fields across peers to gauge looseness. The `_distance` fields are the
    /// ring distance to each held edge (None when unheld). Read from the live
    /// connection set — no mirrored counter to rot.
    pub lattice_has_successor: bool,
    pub lattice_has_predecessor: bool,
    pub lattice_successor_distance: Option<f64>,
    pub lattice_predecessor_distance: Option<f64>,
    /// Discovery health (route-to-self probe). `lattice_probes_issued` counts
    /// probes FIRED since startup; `lattice_probe_improvements` counts observed
    /// lattice IMPROVEMENTS (a side filled or an edge tightened toward the true
    /// nearest). The two are counted INDEPENDENTLY (an improvement lands a few
    /// maintenance ticks after the probe that caused it, as the CONNECT completes
    /// asynchronously), so the ratio is a convergence-health gauge, not a strict
    /// per-probe success rate. A healthy peer converges to a tight both-sides
    /// lattice and the improvement rate falls toward zero.
    pub lattice_probes_issued: u64,
    pub lattice_probe_improvements: u64,
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
    /// Streamed-transfer abort counters (large-contract failure isolation).
    pub stream_abort_stats: StreamAbortStats,
    /// Relayed-operation counters (routing/hosting attribution).
    pub relayed_op_stats: RelayedOpStats,
    /// Connect-event emission counters (firehose-retirement precursor).
    pub connect_emit_stats: ConnectEmitStats,
    /// Computed-upstream vs. stored-`is_upstream`-flag divergence counters
    /// (hosting redesign piece D, #4642 / #4671).
    pub upstream_divergence_stats: UpstreamDivergenceStats,
    /// Reconcile-controller SHADOW comparison counters, split PER SITE (hosting
    /// redesign keystone step-2, #4642). The FLIP is site-by-site, so each
    /// site's divergence must be separately gate-able — a single global tally
    /// would be dominated by the renewal set size and hide the other signals.
    ///
    /// `collapse`, `renewal`, and `inbound_unsubscribe` are the FLIPPED (P6) sites:
    /// they no longer record a shadow divergence — the controller DRIVES — and their
    /// counters are REPURPOSED to measure the flip's effect (renewal: strict gate
    /// SUPPRESSED a renewal; collapse / inbound_unsubscribe: driven strict-farther
    /// gate DISAGREED with the legacy `should_unsubscribe_upstream` predicate it
    /// replaced, per collapse site). `connection_drop` (re-root on an upstream loss)
    /// and `host_formation` (announce on first host) remain single-aspect EDGE
    /// shadow sites (focused comparison, record-only).
    pub reconcile_shadow_collapse: ReconcileShadowStats,
    pub reconcile_shadow_renewal: ReconcileShadowStats,
    pub reconcile_shadow_inbound_unsubscribe: ReconcileShadowStats,
    pub reconcile_shadow_connection_drop: ReconcileShadowStats,
    pub reconcile_shadow_host_formation: ReconcileShadowStats,
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

/// Which decision site a reconcile shadow comparison came from. The FLIP is
/// site-by-site, so the counters are split per site (keystone step-2, #4642).
///
/// `Collapse` / `Renewal` are the MAINTENANCE sites (full action-set
/// comparison). The other three are single-aspect EDGE sites, each compared on
/// ONE focused action class (see `action_set_divergence_focused`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileShadowSite {
    /// The interest-gated collapse. **FLIPPED (keystone P6, #4642):** the collapse
    /// DECISION is now DRIVEN by `OpManager::reconcile_wants_collapse` at the three
    /// teardown sites (maintenance-loop downstream-expiry, client-disconnect,
    /// inbound-unsubscribe), replacing the legacy ANY-downstream
    /// `Ring::should_unsubscribe_upstream`. The counter is retained but repurposed:
    /// `comparisons` counts collapse-gate decisions and `divergences`/`collapse`
    /// count the times the driven strict-farther gate DISAGREED with that legacy
    /// predicate — the "collapse diff" ship-gate falsifier (~0% in v0.2.93 shadow),
    /// not a shadow divergence. Recorded in `reconcile_wants_collapse`.
    Collapse,
    /// The subscription-renewal set (`Ring::contracts_needing_renewal`, driven
    /// by the recovery loop). **FLIPPED (keystone sub-task 3, #4642):** this site
    /// no longer records a shadow — it DRIVES (`OpManager::reconcile_wants_renewal`
    /// gates the renewal spawn). The counter is retained but its meaning shifted:
    /// `comparisons` now counts renewal-gate decisions and `divergences`/`renew`
    /// count the times the STRICT-farther interest gate SUPPRESSED a renewal the
    /// old ANY-downstream set would have done — the "renewal tracks active demand,
    /// not cache size" ship-gate falsifier (design §5a / #3763), not a divergence.
    Renewal,
    /// A downstream peer unsubscribed (`subscribe::handle_unsubscribe_inbound`):
    /// did that leave us not-in-use (strict-farther), i.e. should the controller
    /// tear down? **FLIPPED (keystone P6, #4642):** this collapse decision now
    /// DRIVES via `reconcile_wants_collapse` (passed this `site`). The counter is
    /// retained but repurposed like `Collapse`: it measures how often the driven
    /// strict-farther gate DISAGREED with the legacy `should_unsubscribe_upstream`
    /// predicate at THIS site (~0.00% in v0.2.93 shadow), not a shadow divergence.
    InboundUnsubscribe,
    /// A ring connection dropped (`OpManager::on_ring_connection_lost`): DRIVES a
    /// storm-safe PROMPT re-root via `reconcile_wants_reroot` /
    /// `spawn_prompt_reroots` (#4642 piece F flip). The counter is retained but
    /// repurposed like `Collapse`/`InboundUnsubscribe`: every drop-affected in-use
    /// candidate evaluated bumps the denominator and a driven prompt re-root sets
    /// `reroot_search`, so this reads as "of N candidates, K drove a prompt
    /// re-root", not a shadow divergence. Focused on `ReRootSearch`.
    ConnectionDrop,
    /// A peer finished host formation and announced hosting
    /// (`finalize_originator_subscribe`, GET `cache_contract_locally`): does the
    /// controller agree it should announce? Focused on `Announce`.
    HostFormation,
}

/// Per-node counters for the reconcile-controller SHADOW comparison AT ONE SITE
/// (hosting redesign keystone step-2, #4642).
///
/// At each on-`main` hosting decision site the shadow wiring builds a
/// `ReconcileInputs` snapshot, computes what the pure `reconcile` controller
/// WOULD do, and compares it — BY SET MEMBERSHIP — to what the current code
/// actually does. These counters aggregate that comparison so the controller's
/// real-world divergence from today's scattered decisions is legible in
/// production telemetry (via `router_snapshot`) BEFORE any decision is flipped
/// to the controller. The current code still drives every decision; these
/// observe only. Held once PER SITE (`ReconcileShadowSite`) because the flip is
/// site-by-site and each site must be separately gate-able.
///
/// Volume-conscious: the decision sites increment these local counters
/// in-process (NO per-event telemetry emission); only the aggregate totals are
/// exported on the periodic snapshot cadence.
///
/// The per-action counters are SYMMETRIC-DIFFERENCE tallies: each counts the
/// comparisons in which that action was present in exactly one of the
/// {reconcile, actual} sets. Several classes diverge BY DESIGN and are field
/// evidence of the gap the keystone closes, NOT a health alarm — read them as
/// the reconcile-vs-today delta:
/// - `retract` / `reroot_search`: no on-`main` driver retracts on teardown or
///   re-roots on upstream loss.
/// - `renew` / `subscribe` / `unsubscribe`: the controller's strict
///   downstream-demand gate and lease-aware `Renew`-vs-`Subscribe` split
///   legitimately disagree with today's ANY-downstream, renew-everything path.
/// - `announce`: a subscribed, state-present, not-yet-advertised host.
///
/// RENEWAL-site reading: the recorded "actual" is what production did THIS TICK,
/// so a contract the loop SKIPPED or DEFERRED (banned, spam-backoff, already
/// pending) records `{}`. A `renew` / `subscribe` diff therefore also counts
/// contracts the controller would keep alive but production merely deferred this
/// tick — the intended per-tick reading, not a bug.
///
/// `retract_diffs` CAVEAT: it measures collapse/renewal of a contract that was
/// EVER announced, not one with a currently-live advertisement — `is_advertised`
/// (`NeighborHostingManager::is_hosted_locally`) is effectively MONOTONIC in
/// production today, because its only clearer (`on_contract_unhosted`) is
/// dead/test-only until the flip wires the `Retract` action. So a nonzero
/// `retract_diffs` reflects the missing retraction driver, as intended.
#[derive(Default, Clone, Copy)]
pub struct ReconcileShadowStats {
    /// Total shadow comparisons performed at this site (the denominator — one
    /// per decision site invocation).
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

/// Streamed-transfer (> 64 KB `streaming_threshold`) abort counters, aggregated
/// per-node so the large-contract failure class (~50% of large fetches were
/// failing) is legible in central telemetry WITHOUT a per-fragment or
/// per-transfer event stream. Recorded at the receiver stream-assembly abort
/// sites (transport `StreamHandle::assemble` for inactivity/cancelled; the GET
/// originator's `assemble_and_cache_stream` for claim-timeout/deserialize) and
/// the sender cwnd-wait abort site; read into `RouterSnapshotInfo` on the
/// existing snapshot cadence. All monotonic lifetime totals (the collector
/// differences them across the cadence). The `frac_*` fields are a
/// fragment-progress histogram bumped ONCE per receiver abort, bucketing how far
/// the transfer got before dying — the WHERE signal.
#[derive(Default)]
pub struct StreamAbortStats {
    /// Receiver aborts: no fragment arrived within the inactivity window.
    /// Recorded in transport `StreamHandle::assemble`, so this AGGREGATES across
    /// GET + PUT + UPDATE inbound streams (any op that assembles a streamed
    /// transfer), not GET fetches alone.
    pub recv_inactivity: u64,
    /// Receiver aborts: stream cancelled / connection closed mid-transfer.
    /// Like `recv_inactivity`, recorded in transport `StreamHandle::assemble`
    /// and therefore AGGREGATED across GET + PUT + UPDATE inbound streams.
    pub recv_cancelled: u64,
    /// Receiver aborts: no inbound stream was ever claimed (claim timed out).
    /// GET-fetch-only: recorded in the GET originator's
    /// `assemble_and_cache_stream`, so PUT/UPDATE streams are not included.
    pub recv_claim_timeout: u64,
    /// Receiver aborts: bytes fully received but payload failed to deserialize
    /// or was structurally invalid (key mismatch / missing state).
    /// GET-fetch-only, same site as `recv_claim_timeout`.
    pub recv_deserialize: u64,
    /// Sender aborts: cwnd-wait timed out (ACKs stopped arriving) — the stream
    /// was failed rather than blocking the connection forever.
    pub send_cwnd: u64,
    /// Fragment-progress histogram, one bump per receiver abort: 0% received.
    pub frac_0: u64,
    /// Fragment-progress histogram: 100% received but still aborted (payload /
    /// deserialize failure after full receipt).
    pub frac_1: u64,
    /// Fragment-progress histogram: (0%, 50%) received.
    pub frac_lt50: u64,
    /// Fragment-progress histogram: [50%, 90%) received.
    pub frac_50_90: u64,
    /// Fragment-progress histogram: [90%, 100%) received.
    pub frac_ge90: u64,
}

/// Immutable copy of [`StreamAbortStats`] returned by [`stream_abort_counts`]
/// for the `Ring` snapshot task to mirror onto `RouterSnapshotInfo`.
#[derive(Clone, Copy, Default)]
pub struct StreamAbortSnapshot {
    pub recv_inactivity: u64,
    pub recv_cancelled: u64,
    pub recv_claim_timeout: u64,
    pub recv_deserialize: u64,
    pub send_cwnd: u64,
    pub frac_0: u64,
    pub frac_1: u64,
    pub frac_lt50: u64,
    pub frac_50_90: u64,
    pub frac_ge90: u64,
}

/// Count of operations this node RELAYED (forwarded a request one hop toward its
/// key, as a routing intermediary — not the client originator). One increment
/// per relay-driver entry. Per-node monotonic totals for routing/hosting
/// attribution on the snapshot cadence; the collector differences them to see
/// how much traffic a peer relays vs. originates.
#[derive(Default)]
pub struct RelayedOpStats {
    pub gets: u64,
    pub puts: u64,
    pub subscribes: u64,
    pub updates: u64,
}

/// Count of connect telemetry events this node EMITTED to the collector, added
/// so the per-event `connect_connected` / `connect_rejected` firehose (~92k
/// events / 18 min) can eventually be replaced by these aggregate snapshot
/// counters. The per-event emission is NOT retired yet — a downstream dashboard
/// likely still consumes the per-event stream — see the `TODO(follow-up)` at the
/// increment site. Per-node monotonic totals.
#[derive(Default)]
pub struct ConnectEmitStats {
    pub accepts_emitted: u64,
    pub rejects_emitted: u64,
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
    /// Summary-first PUT (#4642 step 3-bis): count + bytes shipped for the
    /// new-contract case (no holder found; full state ships hop-by-hop via
    /// the existing `PutMsg::Request` path). See [`record_put_bytes`].
    pub put_probe_new_contract: (u32, u64),
    /// Summary-first PUT: count + bytes shipped for the existing-mesh case
    /// (holder found; only a `StateDelta` ships via `ProbeReconcile`). See
    /// [`record_put_bytes`].
    pub put_probe_existing_mesh_delta: (u32, u64),
}

/// Which path a summary-first PUT took (#4642 step 3-bis telemetry). See
/// [`record_put_bytes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutProbeCase {
    /// No holder found along the probe route — treated as a genuinely new
    /// contract; full state ships hop-by-hop via the existing
    /// `PutMsg::Request` path.
    NewContract,
    /// A holder was found — only a `StateDelta` ships via `ProbeReconcile`.
    ExistingMeshDelta,
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
        stream_abort_stats: StreamAbortStats::default(),
        relayed_op_stats: RelayedOpStats::default(),
        connect_emit_stats: ConnectEmitStats::default(),
        upstream_divergence_stats: UpstreamDivergenceStats::default(),
        reconcile_shadow_collapse: ReconcileShadowStats::default(),
        reconcile_shadow_renewal: ReconcileShadowStats::default(),
        reconcile_shadow_inbound_unsubscribe: ReconcileShadowStats::default(),
        reconcile_shadow_connection_drop: ReconcileShadowStats::default(),
        reconcile_shadow_host_formation: ReconcileShadowStats::default(),
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
/// - `operations/get/op_ctx_task.rs` `Done` arm (client-initiated GET;
///   success flag from `host_result.is_ok()`), its `Exhausted` arm
///   (records a failure — exhaustion publishes `NotFound`, having
///   delivered no state), AND `deliver_outcome`'s `InfrastructureError`
///   arm (records a failure — the `Unexpected`/`InfraError` terminal
///   outcomes publish a synthesized client `Err`). GET cannot fold the
///   success/exhaustion arms into an outcome-shape classifier the way
///   PUT/UPDATE do: a dead-end publishes `Ok(HostResponse::…NotFound)`,
///   so `matches!(Publish(Ok(_)))` would score it as a success (#4828).
/// - `operations/put/op_ctx_task.rs::deliver_outcome` (client-initiated
///   PUT; covers all `DriverOutcome` variants).
/// - `operations/subscribe/op_ctx_task.rs::deliver_outcome` (client-
///   initiated SUBSCRIBE; covers all `DriverOutcome` variants).
/// - `operations/update/op_ctx_task.rs::deliver_outcome` (client-
///   initiated UPDATE; covers all `DriverOutcome` variants).
///
/// `node.rs::report_result` is NOT a writer: the legacy `OpEnum` /
/// mediator path is gone (see `crates/core/CLAUDE.md`), every op runs a
/// task-per-tx driver, and its `Ok` branch has nothing left to report.
/// It was listed here until #4828 — a stale entry on the very doc this
/// section points auditors at.
///
/// Prefer the `deliver_outcome` funnel shape where the driver's outcome
/// type can express failure: it is the one that did NOT rot. Recording
/// in individual driver arms is how #4828 regrew #4009/#4010 on PUT —
/// the success arms carried a hardcoded `true` and every failure arm was
/// simply forgotten, making `op_stats.puts.1` unreachable. Each op's
/// call site is pinned by a source-scrape test in its `op_ctx_task.rs`
/// test module; those pins are the CI gate against the next migration
/// silently dropping the call.
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

/// Record a broadcast update that was received and actually changed local
/// state.
///
/// # Required call sites
///
/// Manually-mirrored counter. BOTH broadcast-apply drivers in
/// `operations/update/op_ctx_task.rs` must call this, each after its
/// `if !changed` early-return (a no-op re-broadcast is not a received
/// update):
///
/// - `drive_relay_broadcast_to` (`BroadcastTo` — every payload BELOW
///   `streaming_threshold`, i.e. the ordinary small-delta case).
/// - `drive_relay_broadcast_to_streaming` (`BroadcastToStreaming` —
///   payloads at/above the threshold, default 64 KB).
///
/// Until #4828 only the streaming twin recorded, so ordinary small deltas
/// — the normal River case — were invisible and an actively-receiving
/// subscriber showed 0 (`server/home_page/cards.rs` renders
/// `updates_received` as the ONLY UPDATE number for subscriber nodes).
/// Pinned by `both_broadcast_drivers_record_update_received`.
///
/// Distinct from [`record_relayed_update`], which counts relayed UPDATE
/// *requests* toward the key (`relayed_op_stats`) and deliberately does
/// NOT count broadcast fan-out.
pub fn record_update_received() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.op_stats.updates_received = s.op_stats.updates_received.saturating_add(1);
        }
    }
}

/// Record a summary-first PUT probe outcome for the dashboard "Operations"
/// panel (#4642 step 3-bis falsifier): which path the originator took
/// (`case`) and how many bytes that path actually shipped.
///
/// Single call site: `operations::put::op_ctx_task::record_put_probe_outcome`,
/// which also feeds the matching `GlobalTestMetrics` counter from the same
/// call so the two can never drift relative to each other (see
/// `.claude/rules/bug-prevention-patterns.md`: manually-mirrored counters).
pub fn record_put_bytes(case: PutProbeCase, bytes: u64) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            let counter = match case {
                PutProbeCase::NewContract => &mut s.op_stats.put_probe_new_contract,
                PutProbeCase::ExistingMeshDelta => &mut s.op_stats.put_probe_existing_mesh_delta,
            };
            counter.0 = counter.0.saturating_add(1);
            counter.1 = counter.1.saturating_add(bytes);
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

/// Bump the fragment-progress histogram bucket for one receiver abort.
/// `received`/`total` come from the stream handle; `None` (no handle, e.g. a
/// claim timeout) counts as 0% (`frac_0`).
fn bump_stream_frac(stats: &mut StreamAbortStats, received: Option<u32>, total: Option<u32>) {
    match (received, total) {
        (Some(r), Some(t)) if t > 0 => {
            if r == 0 {
                stats.frac_0 = stats.frac_0.saturating_add(1);
            } else if r >= t {
                stats.frac_1 = stats.frac_1.saturating_add(1);
            } else {
                // Integer-only bucketing avoids float rounding at the edges.
                let pct = (r as u64) * 100 / (t as u64);
                if pct < 50 {
                    stats.frac_lt50 = stats.frac_lt50.saturating_add(1);
                } else if pct < 90 {
                    stats.frac_50_90 = stats.frac_50_90.saturating_add(1);
                } else {
                    stats.frac_ge90 = stats.frac_ge90.saturating_add(1);
                }
            }
        }
        // No usable fragment counts (claim timeout, or total unknown): treat as
        // zero progress so every abort lands in exactly one bucket.
        _ => stats.frac_0 = stats.frac_0.saturating_add(1),
    }
}

/// Record a receiver stream-assembly abort: no fragment arrived within the
/// inactivity window. Called from the transport `StreamHandle::assemble` site
/// (covers both the GET originator and relay receive paths).
pub fn record_stream_recv_abort_inactivity(received: Option<u32>, total: Option<u32>) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.stream_abort_stats.recv_inactivity =
                s.stream_abort_stats.recv_inactivity.saturating_add(1);
            bump_stream_frac(&mut s.stream_abort_stats, received, total);
        }
    }
}

/// Record a receiver stream-assembly abort: the stream was cancelled or the
/// connection closed mid-transfer.
pub fn record_stream_recv_abort_cancelled(received: Option<u32>, total: Option<u32>) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.stream_abort_stats.recv_cancelled =
                s.stream_abort_stats.recv_cancelled.saturating_add(1);
            bump_stream_frac(&mut s.stream_abort_stats, received, total);
        }
    }
}

/// Record a receiver stream abort: no inbound stream was ever claimed (the
/// claim timed out); zero fragments observed.
pub fn record_stream_recv_abort_claim_timeout() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.stream_abort_stats.recv_claim_timeout =
                s.stream_abort_stats.recv_claim_timeout.saturating_add(1);
            bump_stream_frac(&mut s.stream_abort_stats, None, None);
        }
    }
}

/// Record a receiver stream abort: all bytes arrived but the payload failed to
/// deserialize or was structurally invalid (key mismatch / missing state).
pub fn record_stream_recv_abort_deserialize(received: Option<u32>, total: Option<u32>) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.stream_abort_stats.recv_deserialize =
                s.stream_abort_stats.recv_deserialize.saturating_add(1);
            bump_stream_frac(&mut s.stream_abort_stats, received, total);
        }
    }
}

/// Record a sender stream abort: cwnd-wait timed out (ACKs stopped arriving) and
/// the stream was failed rather than blocking the connection.
pub fn record_stream_send_abort_cwnd() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.stream_abort_stats.send_cwnd = s.stream_abort_stats.send_cwnd.saturating_add(1);
        }
    }
}

/// Read the current streamed-transfer abort counters for export to the
/// `router_snapshot` telemetry event. `None` before the singleton is
/// initialized (i.e. always populated in production snapshots).
pub fn stream_abort_counts() -> Option<StreamAbortSnapshot> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let a = &s.stream_abort_stats;
    Some(StreamAbortSnapshot {
        recv_inactivity: a.recv_inactivity,
        recv_cancelled: a.recv_cancelled,
        recv_claim_timeout: a.recv_claim_timeout,
        recv_deserialize: a.recv_deserialize,
        send_cwnd: a.send_cwnd,
        frac_0: a.frac_0,
        frac_1: a.frac_1,
        frac_lt50: a.frac_lt50,
        frac_50_90: a.frac_50_90,
        frac_ge90: a.frac_ge90,
    })
}

/// Record that this node relayed (forwarded one hop) a GET request.
pub fn record_relayed_get() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.relayed_op_stats.gets = s.relayed_op_stats.gets.saturating_add(1);
        }
    }
}

/// Record that this node relayed (forwarded one hop) a PUT request.
pub fn record_relayed_put() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.relayed_op_stats.puts = s.relayed_op_stats.puts.saturating_add(1);
        }
    }
}

/// Record that this node relayed (forwarded one hop) a SUBSCRIBE request.
pub fn record_relayed_subscribe() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.relayed_op_stats.subscribes = s.relayed_op_stats.subscribes.saturating_add(1);
        }
    }
}

/// Record that this node relayed (forwarded one hop) an UPDATE *request*
/// toward the contract key. Counts both `UpdateMsg::RequestUpdate` and its
/// large-payload variant `RequestUpdateStreaming` (the same logical relayed
/// request). Does NOT count `BroadcastTo` / `BroadcastToStreaming`: those are
/// update-mesh fan-out to interested co-hosts, not a request relayed one hop
/// toward the key, and counting them would conflate relay volume with fan-out
/// volume (one update -> N broadcasts).
pub fn record_relayed_update() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.relayed_op_stats.updates = s.relayed_op_stats.updates.saturating_add(1);
        }
    }
}

/// Read the current relayed-operation counters for export to `router_snapshot`.
/// Returns `(gets, puts, subscribes, updates)` or `None` before the singleton
/// is initialized.
pub fn relayed_op_counts() -> Option<(u64, u64, u64, u64)> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let r = &s.relayed_op_stats;
    Some((r.gets, r.puts, r.subscribes, r.updates))
}

/// Record that a `connect_connected` telemetry event was emitted to the
/// collector (aggregate precursor to retiring the per-event firehose).
pub fn record_connect_accept_emitted() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connect_emit_stats.accepts_emitted =
                s.connect_emit_stats.accepts_emitted.saturating_add(1);
        }
    }
}

/// Record that a `connect_rejected` telemetry event was emitted to the
/// collector (aggregate precursor to retiring the per-event firehose).
pub fn record_connect_reject_emitted() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connect_emit_stats.rejects_emitted =
                s.connect_emit_stats.rejects_emitted.saturating_add(1);
        }
    }
}

/// Read the current connect-event emission counters for export to
/// `router_snapshot`. Returns `(accepts_emitted, rejects_emitted)` or `None`
/// before the singleton is initialized.
pub fn connect_emit_counts() -> Option<(u64, u64)> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let c = &s.connect_emit_stats;
    Some((c.accepts_emitted, c.rejects_emitted))
}

/// Count of this node's active connections that are to gateways (the
/// NAT-stranded fingerprint — a peer stuck on gateways only). Read from the
/// authoritative tracked `connected_peers` list. `None` before the singleton is
/// initialized.
pub fn connections_to_gateways() -> Option<u64> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    Some(s.connected_peers.iter().filter(|p| p.is_gateway).count() as u64)
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

/// Record one reconcile-controller SHADOW comparison AT `site` (keystone
/// step-2, #4642).
///
/// Increments that site's `comparisons` denominator every call, its
/// `divergences` numerator when the reconcile action set differed from the
/// actual behavior's set in any action class, and each per-action tally whose
/// class was in the symmetric difference. Behavior-preserving — the current code
/// drives the decision; this only measures how far the controller WOULD diverge,
/// so the gap is legible in production telemetry ahead of the site-by-site flip.
pub fn record_reconcile_shadow_comparison(
    site: ReconcileShadowSite,
    divergence: ReconcileActionDivergence,
) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            let r = match site {
                ReconcileShadowSite::Collapse => &mut s.reconcile_shadow_collapse,
                ReconcileShadowSite::Renewal => &mut s.reconcile_shadow_renewal,
                ReconcileShadowSite::InboundUnsubscribe => {
                    &mut s.reconcile_shadow_inbound_unsubscribe
                }
                ReconcileShadowSite::ConnectionDrop => &mut s.reconcile_shadow_connection_drop,
                ReconcileShadowSite::HostFormation => &mut s.reconcile_shadow_host_formation,
            };
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

/// A copy of all per-site reconcile-controller SHADOW counters, for export to
/// the `router_snapshot` telemetry event (keystone step-2, #4642).
#[derive(Clone, Copy)]
pub struct ReconcileShadowSnapshot {
    pub collapse: ReconcileShadowStats,
    pub renewal: ReconcileShadowStats,
    pub inbound_unsubscribe: ReconcileShadowStats,
    pub connection_drop: ReconcileShadowStats,
    pub host_formation: ReconcileShadowStats,
}

/// Read the per-site reconcile-controller SHADOW counters (keystone step-2,
/// #4642) for export to the `router_snapshot` telemetry event. Returns copies of
/// the per-node monotonic totals for every site, or `None` before the
/// `NETWORK_STATUS` singleton is initialized. `Ring` polls this on the snapshot
/// cadence and copies the values onto `RouterSnapshotInfo`.
pub fn reconcile_shadow_counts() -> Option<ReconcileShadowSnapshot> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    Some(ReconcileShadowSnapshot {
        collapse: s.reconcile_shadow_collapse,
        renewal: s.reconcile_shadow_renewal,
        inbound_unsubscribe: s.reconcile_shadow_inbound_unsubscribe,
        connection_drop: s.reconcile_shadow_connection_drop,
        host_formation: s.reconcile_shadow_host_formation,
    })
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
    /// Contract-handler fair-queue occupancy (#4917). Read from the queue's own
    /// gauge at snapshot time rather than mirrored into `NetworkStatus`, so it
    /// cannot drift from the canonical value the way a hand-maintained counter
    /// does (`.claude/rules/bug-prevention-patterns.md`, #4009 / #4010).
    pub fair_queue: crate::contract::FairQueueStats,
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
    /// Aggregate on-disk bytes used by persisted contract state
    /// (`DiskUsageTracker::stats().state_bytes`, #4683). `None` until the
    /// disk tracker is configured and seeded (early startup) — distinct from
    /// `Some(0)`, which means seeded-and-empty.
    pub disk_state_bytes: Option<u64>,
    /// Aggregate on-disk bytes used by `*.wasm` code blobs. Same
    /// seeded-gate semantics as `disk_state_bytes`.
    pub disk_wasm_bytes: Option<u64>,
    /// Aggregate on-disk bytes used by the wasmtime compile cache. Same
    /// seeded-gate semantics as `disk_state_bytes`.
    pub disk_compile_cache_bytes: Option<u64>,
    /// Sum of `disk_state_bytes` + `disk_wasm_bytes` + `disk_compile_cache_bytes`
    /// — the aggregate the disk budget bounds. Same seeded-gate semantics.
    pub disk_total_bytes: Option<u64>,
    /// The aggregate disk budget the admission gate checks projected writes
    /// against (`HostingManager::disk_budget_bytes`, #4702). `None` while it
    /// is still `u64::MAX` — i.e. before the first 60s recompute installs a
    /// real value — so the panel can distinguish "not yet computed" from a
    /// genuine (if enormous) budget.
    pub disk_budget_bytes: Option<u64>,
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
    /// eviction: NOT pinned by demand (`contract_in_use`). There is no longer a
    /// `min_ttl` age gate (dropped 2026-07-08). The renderer badges "next to
    /// evict" on the first eligible row, NOT the raw lowest-keep-score row — an
    /// in-use low-score contract is ordered last by the real sweep, so badging it
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
        // Read straight from the queue's gauge — no provider registration and
        // no mirrored counter to keep in sync (#4917).
        fair_queue: crate::contract::fair_queue_stats(),
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

    /// End-to-end for the per-site reconcile-controller SHADOW counters (keystone
    /// step-2, #4642): the record site must feed the `reconcile_shadow_counts()`
    /// getter that `Ring` polls for the `router_snapshot` export. `comparisons`
    /// bumps every call, `divergences` only when any action class diverged, each
    /// per-action tally bumps only when that class is in the symmetric
    /// difference, and — crucially — the two SITES are tallied INDEPENDENTLY (a
    /// collapse-site record must not bleed into the renewal-site counters). So a
    /// dropped increment, a mis-wired per-action field, or a crossed site fails
    /// here rather than silently emitting zeros in production.
    #[test]
    fn reconcile_shadow_counts_reflect_recorded_events() {
        use ReconcileShadowSite::*;
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31337, HashSet::new(), "test".to_string());

        let z = reconcile_shadow_counts().expect("counters present after init");
        assert_eq!(
            (
                z.collapse.comparisons,
                z.renewal.comparisons,
                z.inbound_unsubscribe.comparisons,
                z.connection_drop.comparisons,
                z.host_formation.comparisons,
            ),
            (0, 0, 0, 0, 0),
            "all sites start at zero after init"
        );

        // Collapse site: 2 comparisons, 1 diverging (retract-only gap).
        record_reconcile_shadow_comparison(Collapse, ReconcileActionDivergence::default());
        record_reconcile_shadow_comparison(
            Collapse,
            ReconcileActionDivergence {
                retract: true,
                ..Default::default()
            },
        );
        // Renewal site: 1 comparison, diverging (not-subscribed → Subscribe+Renew).
        record_reconcile_shadow_comparison(
            Renewal,
            ReconcileActionDivergence {
                subscribe: true,
                renew: true,
                ..Default::default()
            },
        );
        // Connection-drop site (an EDGE site): 1 comparison, diverging (reroot).
        record_reconcile_shadow_comparison(
            ConnectionDrop,
            ReconcileActionDivergence {
                reroot_search: true,
                ..Default::default()
            },
        );

        let c = reconcile_shadow_counts().expect("counters present");

        // Collapse site tallies only the collapse-site records.
        assert_eq!(c.collapse.comparisons, 2, "collapse site: every call bumps");
        assert_eq!(
            c.collapse.divergences, 1,
            "collapse site: one diverging call"
        );
        assert_eq!(c.collapse.retract_diffs, 1);
        assert_eq!(c.collapse.subscribe_diffs, 0);

        // Renewal site is independent — the collapse records must not bleed in.
        assert_eq!(
            c.renewal.comparisons, 1,
            "renewal site independent of collapse"
        );
        assert_eq!(c.renewal.subscribe_diffs, 1);
        assert_eq!(c.renewal.renew_diffs, 1);
        assert_eq!(c.renewal.retract_diffs, 0);

        // Connection-drop site is independent of the maintenance sites.
        assert_eq!(c.connection_drop.comparisons, 1);
        assert_eq!(c.connection_drop.reroot_search_diffs, 1);
        assert_eq!(c.connection_drop.renew_diffs, 0);
        // Sites never touched stay zero.
        assert_eq!(c.inbound_unsubscribe.comparisons, 0);
        assert_eq!(c.host_formation.comparisons, 0);
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
            ..Default::default()
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
