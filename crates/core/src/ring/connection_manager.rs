//! # Lock Ordering
//!
//! To prevent deadlocks, all `RwLock`-protected fields in [`ConnectionManager`] **must** be
//! acquired in the following order whenever multiple locks are held simultaneously:
//!
//! 1. `location_for_peer`
//! 2. `connections_by_location`
//! 3. `pending_reservations`
//!
//! Acquiring locks in any other order risks an ABBA deadlock. This ordering was established
//! after a deadlock introduced in PR #3091 (fixed in PR #3095), where
//! `cleanup_stale_reservations` acquired `pending_reservations` before `connections_by_location`,
//! while `prune_connection` acquired them in the opposite order.
//!
//! The following locks are **always acquired independently** (never nested with each other or
//! with the locks above) so they do not participate in the ordering:
//!
//! - `connect_jitter_failures` (`parking_lot::Mutex<(u32, Option<Instant>)>`)
//! - `acceptor_reliability` (`parking_lot::RwLock<BTreeMap<SocketAddr, AcceptorStats>>`)
//! - `recently_failed_addrs` (`parking_lot::Mutex<TrackedBackoff<SocketAddr>>`)

use dashmap::{DashMap, DashSet};
use parking_lot::Mutex;
use std::collections::{BTreeMap, btree_map::Entry};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::config::GlobalRng;
use crate::topology::{Limits, TopologyManager};
use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};

use super::*;

#[derive(Clone)]
pub(crate) struct TransientEntry {
    /// Entry tracking a transient connection that hasn't been added to the ring topology yet.
    /// Transient connections are typically unsolicited inbound connections to gateways.
    /// Advertised location for the transient peer, if known at admission time.
    pub location: Option<Location>,
    /// When this transient entry was created (for expiration cleanup).
    pub created_at: Instant,
}

/// Maximum time a pending reservation can remain before being considered stale.
/// Reservations that exceed this TTL are cleaned up by `cleanup_stale_reservations`
/// to prevent permanent node isolation when CONNECT operations fail to complete.
const PENDING_RESERVATION_TTL: Duration = Duration::from_secs(60);

/// Minimum connections before applying Kleinberg distance scoring on inbound
/// connections. Below this threshold, all inbound connections are accepted to
/// avoid blocking initial bootstrap. With fewer than 3 connections the gap
/// score is too noisy to be useful.
const KLEINBERG_FILTER_MIN_CONNECTIONS: usize = 3;

// ---------------------------------------------------------------------------
// Nearest-neighbor ring lattice (successor + predecessor base edges)
// ---------------------------------------------------------------------------
//
// Kleinberg gap-fill (the k-2 long links) gives O(log^2 n) routing *iff* the
// short-range base lattice — each peer's immediate ring neighbors — is also
// present. Freenet builds the long links but never guaranteed the base lattice:
// the gap-fill objective scores the *nearest* candidate LOWEST (an ordinal
// "who is my immediate neighbor" property cannot emerge from a log-distance
// metric), so greedy routing dead-ends at local minima with no connected
// neighbor closer to the key. This module adds the two-tier structure Chord and
// Symphony ship: guarantee an edge to the nearest connected SUCCESSOR (closest
// higher location, wrapping) AND the nearest connected PREDECESSOR (closest
// lower location), leaving the remaining slots to the untouched Kleinberg
// machinery. Two guaranteed edges per peer is all Kleinberg needs (Theta(1)
// lattice edges); routing stays O(log^2 n).
//
// The behavior is UNCONDITIONALLY ON in production (`nn_lattice_enabled()`
// folds to `const true` and the branch optimizes away). Under `test`/`testing`
// a thread-local override lets a validation harness run the stock (lattice-off)
// arm against the fix (lattice-on) arm on identical seeds. The override is
// THREAD-LOCAL because the simulation harness runs every node on the test's own
// current-thread runtime and relies on thread-local globals (`GlobalRng`,
// simulation time) to stay parallel-test-safe; a process-global flag would
// bleed across concurrently-running sim tests on other threads. It is NOT a
// production config knob.
#[cfg(any(test, feature = "testing"))]
thread_local! {
    static NN_LATTICE_ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    // Test-only "force active regardless of the min-`max_connections` floor"
    // override (see `nn_lattice_active_for`). DEFAULTS TO FALSE so the broad
    // simulation suite — which never touches these toggles and runs at a sparse
    // `max_connections` (e.g. 5) far below the floor — observes the REAL
    // production gate (feature off below the floor). The dedicated control-vs-fix
    // / benefit tests explicitly opt in via `set_nn_lattice_enabled(true)`, which
    // sets this too so those tests keep exercising the ON path at their sparse
    // `max_connections`.
    static NN_LATTICE_FORCE_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Enable/disable the nearest-neighbor ring lattice for the CURRENT THREAD
/// (test/validation only — production is always enabled). The control arm of the
/// findability validation calls `set_nn_lattice_enabled(false)` to reproduce
/// stock (v0.2.95) topology behavior on the same seed as the treatment arm.
///
/// This ALSO sets the `NN_LATTICE_FORCE_ACTIVE` override to `enabled`: any test
/// that explicitly opts into the lattice runs it FULLY ACTIVE regardless of the
/// [`NN_LATTICE_MIN_MAX_CONNECTIONS`] floor (so the dedicated benefit/unit tests
/// keep exercising the mechanism at a sparse `max_connections`). Tests that never
/// call this — the broad simulation suite — keep the default (force-active FALSE)
/// and therefore see the real production gate: the feature is OFF below the floor.
#[cfg(any(test, feature = "testing"))]
pub fn set_nn_lattice_enabled(enabled: bool) {
    NN_LATTICE_ENABLED.with(|c| c.set(enabled));
    NN_LATTICE_FORCE_ACTIVE.with(|c| c.set(enabled));
}

/// Set ONLY the min-`max_connections` floor bypass for the CURRENT THREAD
/// (test/validation only). Independent of the enabled flag. Its main use is the
/// benefit-test cleanup guard, which restores the TRUE production default —
/// enabled `true` (via [`set_nn_lattice_enabled`]) but force-active `false` (the
/// floor applies) — after a run, since `set_nn_lattice_enabled(true)` alone would
/// leave force-active set.
#[cfg(any(test, feature = "testing"))]
pub fn set_nn_lattice_force_active(force: bool) {
    NN_LATTICE_FORCE_ACTIVE.with(|c| c.set(force));
}

/// Whether the min-`max_connections` floor is being bypassed on this thread
/// (test-only; always `false` in production). See [`set_nn_lattice_enabled`] and
/// [`nn_lattice_active_for`].
#[inline]
pub(crate) fn nn_lattice_force_active() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        NN_LATTICE_FORCE_ACTIVE.with(|c| c.get())
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        false
    }
}

/// Whether the nearest-neighbor ring lattice flag is set. Always `true` in
/// production; overridable per-thread in test/testing builds (see
/// [`set_nn_lattice_enabled`]).
///
/// NOTE: this is the raw flag, NOT the full activation gate — a peer with too
/// small a connection budget disables the feature even when this is `true`. Every
/// gate site uses the full gate [`nn_lattice_active_for`] (or
/// `ConnectionManager::nn_lattice_active`); this bare predicate exists only as the
/// flag input to `nn_lattice_active_for` itself.
#[inline]
pub(crate) fn nn_lattice_enabled() -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        NN_LATTICE_ENABLED.with(|c| c.get())
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        true
    }
}

/// Minimum CONFIGURED `max_connections` at which the nearest-neighbor ring
/// lattice (mechanisms 1-4) activates. Below this floor the feature disables
/// itself. Anchored to [`Ring::DEFAULT_MIN_CONNECTIONS`].
///
/// This is keyed on the CONFIGURED `max_connections` (the cap a node is built
/// with — 200 by default in production via `DEFAULT_MAX_CONNECTIONS`), NOT the
/// live connection count. So in PRODUCTION every node is above the floor and the
/// feature is always active; the floor's only real effect is to disable the
/// feature at the sparse scales used by the simulation harness. The gate is
/// therefore production-neutral: any value in `(max sim config, 200)` behaves
/// identically in production.
///
/// Two-tier routing (2 reserved base-lattice edges + `k - 2` Kleinberg long
/// links) needs enough long links to keep the O(log^2 n) structure; below a
/// minimum degree, reserving the 2 edges destabilizes routing. `DEFAULT_MIN_CONNECTIONS`
/// (25) is the principled anchor: a node whose configured `max_connections` is
/// below the network's own minimum-degree target cannot sustain the two-tier
/// structure, so it should not run the lattice.
///
/// Setting the floor at `DEFAULT_MIN_CONNECTIONS` also keeps the WHOLE sparse
/// simulation suite at its pre-lattice baseline: sim configs top out at
/// `max_connections = 16` (with a handful of production-scale max=200 tests that
/// DO exercise the lattice and pass), so a floor of 25 disables the feature for
/// every sparse sim test — they run exactly as they did before this feature
/// existed. A lower floor (e.g. 15) was evaluated but rejected: the sim's DEFAULT
/// config is max=15/16, so a 15 floor would flip those default hosting /
/// subscription / determinism tests to feature-ON — a change from their stock
/// baseline — for no production benefit (the gate reads CONFIGURED max, which is
/// 200 in production, so 15 vs 25 is identical for real nodes). Keeping the sparse
/// suite feature-off is the lower-risk choice.
pub(crate) const NN_LATTICE_MIN_MAX_CONNECTIONS: usize = super::Ring::DEFAULT_MIN_CONNECTIONS;

/// Whether the nearest-neighbor ring lattice is ACTIVE for a peer whose
/// connection budget is `max_connections`. This is the full activation gate used
/// at every feature gate site (acceptance clause, over-cap admission, discovery
/// probe, retention exemption): the flag must be set AND the budget must clear
/// the [`NN_LATTICE_MIN_MAX_CONNECTIONS`] floor (or the test-only force override
/// bypasses the floor — see [`nn_lattice_force_active`]).
#[inline]
pub(crate) fn nn_lattice_active_for(max_connections: usize) -> bool {
    nn_lattice_enabled()
        && (nn_lattice_force_active() || max_connections >= NN_LATTICE_MIN_MAX_CONNECTIONS)
}

/// Maximum number of concurrent CONNECT operations a gateway will route simultaneously.
/// This prevents thundering-herd scenarios where many joiners hit the same gateway at once.
/// The value 8 balances throughput (parallel joins) against overload protection. Non-gateways
/// use `usize::MAX` since they see far fewer concurrent connects.
const MAX_CONCURRENT_GATEWAY_CONNECTS: usize = 8;

/// Absolute over-`max_connections` slack within which a strictly-closer per-side
/// nearest-neighbor lattice edge (mechanism 3) may be admitted at capacity.
///
/// A lattice edge that arrives while the node is at `max_connections` is admitted
/// over the cap (it displaces a long link), but only while the established count
/// is below `max_connections + LATTICE_OVERMAX_SLACK`. This is a HARD ceiling: it
/// bounds the transient over-max excess to a small constant so a burst of
/// ever-closer lattice arrivals can never grow the connection set without limit
/// (the security lens's "no unbounded over-max" requirement). The topology
/// maintenance loop's over-max prune (`topology.rs`, which never sheds a
/// protected lattice edge) sheds a farther non-lattice link each tick to bring
/// the node back to `max_connections`, so the ceiling is only ever reached
/// transiently between ticks. Sized at 2 = the two reserved lattice slots
/// (successor + predecessor), so both edges can tighten in the same window before
/// the prune catches up.
pub(crate) const LATTICE_OVERMAX_SLACK: usize = 2;

/// Base TTL for a peer address in the recently-failed cache after a NAT traversal
/// failure. Repeated failures to the same address scale the TTL exponentially:
/// `min(BASE * 2^count, MAX)` — so 5 min → 10 → 20 → 40 → 60 min (cap).
const FAILED_ADDR_BASE_TTL: Duration = Duration::from_secs(300);

/// Maximum TTL for a repeatedly-failing address. Permanent NAT incompatibility
/// (e.g., symmetric NAT on both sides) caps at 1 hour between retries.
const FAILED_ADDR_MAX_TTL: Duration = Duration::from_secs(3600);

/// Maximum number of failed addresses tracked in the recently-failed cache.
/// Entries are evicted LRU-style when the limit is reached.
const FAILED_ADDR_MAX_ENTRIES: usize = 1024;

/// Per-peer CONNECT acceptance success/failure counts.
/// See [`ConnectionManager::peer_acceptor_reliability`] for scoring formula.
#[derive(Debug, Clone)]
struct AcceptorStats {
    successes: u32,
    attempts: u32,
    last_updated: Instant,
}

/// RAII guard that releases a connect admission slot when dropped.
///
/// This ensures the slot is always released, even when `?` operators cause early returns
/// from error paths. Without this guard, transient errors between `try_admit_connect()`
/// and the manual `release_connect()` call would permanently leak slots, eventually
/// bricking the gateway (with only 8 slots available).
pub(crate) struct ConnectAdmissionGuard {
    connect_in_flight: Arc<AtomicUsize>,
    released: bool,
}

impl ConnectAdmissionGuard {
    fn new(connect_in_flight: Arc<AtomicUsize>) -> Self {
        Self {
            connect_in_flight,
            released: false,
        }
    }

    /// Explicitly release the slot (e.g., when forwarding to the next hop).
    /// After calling this, the Drop impl is a no-op.
    #[cfg(test)]
    pub fn release(mut self) {
        if !self.released {
            self.released = true;
            let prev = self.connect_in_flight.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "connect_in_flight underflow on explicit release");
        }
    }
}

impl Drop for ConnectAdmissionGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            let prev = self.connect_in_flight.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(prev > 0, "connect_in_flight underflow on drop");
        }
    }
}

// ==================== Peer Health Tracking ====================

/// Per-peer routing health statistics.
#[derive(Debug, Clone)]
pub(crate) struct PeerHealthStats {
    pub successes: u64,
    pub failures: u64,
    pub last_success: Option<Instant>,
    pub added_at: Instant,
}

/// Tracks routing success/failure rates per peer for eviction decisions.
///
/// Peers with sustained high failure rates are candidates for eviction from
/// the ring topology. This prevents "black hole" peers that accept connections
/// but fail to route traffic from permanently occupying connection slots.
#[derive(Debug, Clone, Default)]
pub(crate) struct PeerHealthTracker {
    stats: BTreeMap<SocketAddr, PeerHealthStats>,
}

/// Minimum number of routing events before failure-rate eviction applies.
const HEALTH_MIN_EVENTS: u64 = 10;
/// Failure rate threshold (0.0–1.0) above which a peer is considered unhealthy.
const HEALTH_FAILURE_RATE_THRESHOLD: f64 = 0.90;
/// Duration in the ring with zero successes before eviction (with at least 1 failure).
const HEALTH_NO_SUCCESS_TIMEOUT: Duration = Duration::from_secs(600);
/// Duration since last success before a failure burst triggers eviction.
const HEALTH_LAST_SUCCESS_TIMEOUT: Duration = Duration::from_secs(600);
/// Minimum failures since last success for the "recent failure burst" criterion.
const HEALTH_BURST_MIN_FAILURES: u64 = 10;

impl PeerHealthTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize health tracking for a newly added peer.
    pub fn init_peer(&mut self, addr: SocketAddr) {
        self.stats.insert(
            addr,
            PeerHealthStats {
                successes: 0,
                failures: 0,
                last_success: None,
                added_at: Instant::now(),
            },
        );
    }

    /// Remove health tracking for a pruned peer.
    pub fn remove_peer(&mut self, addr: SocketAddr) {
        self.stats.remove(&addr);
    }

    /// Record a successful routing outcome for a peer.
    pub fn record_success(&mut self, addr: SocketAddr) {
        if let Some(stats) = self.stats.get_mut(&addr) {
            stats.successes += 1;
            stats.last_success = Some(Instant::now());
        }
    }

    /// Record a failed routing outcome for a peer.
    pub fn record_failure(&mut self, addr: SocketAddr) {
        if let Some(stats) = self.stats.get_mut(&addr) {
            stats.failures += 1;
        }
    }

    /// Identify peers that should be evicted based on health criteria.
    ///
    /// Never-succeeded peers (0 successes) are always evictable — a peer that
    /// never routes successfully is actively harmful (blocks routing and connection
    /// growth). Other unhealthy peers are protected by `min_connections`.
    pub fn unhealthy_peers(&self, min_connections: usize, current_count: usize) -> Vec<SocketAddr> {
        let now = Instant::now();
        let candidates: Vec<(SocketAddr, bool)> = self
            .stats
            .iter()
            .filter(|(_, stats)| {
                let total = stats.successes + stats.failures;

                // Criterion 1: High failure rate with sufficient sample size
                if total >= HEALTH_MIN_EVENTS {
                    let failure_rate = stats.failures as f64 / total as f64;
                    if failure_rate >= HEALTH_FAILURE_RATE_THRESHOLD {
                        return true;
                    }
                }

                // Criterion 2: In ring > timeout with zero successes and at least 1 failure
                if stats.successes == 0
                    && stats.failures >= 1
                    && now.duration_since(stats.added_at) > HEALTH_NO_SUCCESS_TIMEOUT
                {
                    return true;
                }

                // Criterion 3: Last success was long ago and total failures exceed burst threshold.
                if let Some(last_ok) = stats.last_success {
                    if now.duration_since(last_ok) > HEALTH_LAST_SUCCESS_TIMEOUT
                        && stats.failures > HEALTH_BURST_MIN_FAILURES
                    {
                        return true;
                    }
                }

                false
            })
            .map(|(addr, stats)| (*addr, stats.successes == 0))
            .collect();

        // Partition: never-succeeded peers are always evictable (they block routing
        // and connection growth), other unhealthy peers are protected by min_connections.
        // Note: never-succeeded peers must still pass criterion 2's HEALTH_NO_SUCCESS_TIMEOUT
        // (600s) + at least 1 failure, so freshly connected peers are not affected.
        let (never_succeeded, degraded): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, zero_success)| *zero_success);
        // Safety floor: always retain at least 1 connection to avoid total isolation.
        // The gateway bootstrap recovery path has a 120s delay; keeping one connection
        // gives the node a chance to route while waiting for fresh connections.
        let max_never_succeeded = if current_count > 1 {
            never_succeeded.len().min(current_count - 1)
        } else {
            0
        };
        let mut result: Vec<SocketAddr> = never_succeeded
            .into_iter()
            .take(max_never_succeeded)
            .map(|(a, _)| a)
            .collect();
        let remaining = current_count.saturating_sub(result.len());
        if remaining > min_connections {
            let budget = remaining.saturating_sub(min_connections);
            result.extend(degraded.into_iter().take(budget).map(|(a, _)| a));
        }
        result
    }
}

#[derive(Clone)]
pub(crate) struct ConnectionManager {
    /// Pending connection reservations, keyed by socket address.
    /// Each entry records the advertised location and the time the reservation was created,
    /// allowing stale entries to be expired via `PENDING_RESERVATION_TTL`.
    pending_reservations: Arc<RwLock<BTreeMap<SocketAddr, (Location, Instant)>>>,
    /// Mapping from socket address to location for established and in-progress connections.
    /// Entries are added by `add_connection` (established) and `record_pending_location`
    /// (speculative, during `should_accept`). Removed by `prune_connection` and
    /// `cleanup_stale_reservations` (orphan sweep).
    pub(super) location_for_peer: Arc<RwLock<BTreeMap<SocketAddr, Location>>>,
    pub(super) topology_manager: Arc<RwLock<TopologyManager>>,
    connections_by_location: Arc<RwLock<BTreeMap<Location, Vec<Connection>>>>,
    /// Interim connections ongoing handshake or successfully open connections
    /// Is important to keep track of this so no more connections are accepted prematurely.
    own_location: Arc<AtomicU64>,
    /// Our own socket address, set once we know it (e.g., from gateway observation).
    own_addr: Arc<Mutex<Option<SocketAddr>>>,
    is_gateway: bool,
    /// Test-only override for the placement-migration version floor. `None` in
    /// production (the real `SUBSCRIBE_HINT_MIN_VERSION` applies); `Some(floor)`
    /// only in a sim test that opted into the migration cascade via
    /// `SimNetwork::enable_placement_migration`. See `NodeConfig`.
    subscribe_hint_floor_override: Option<(u8, u8, u16)>,
    /// Mirror of `P2pConnManager.connections[addr].remote_version`, kept on
    /// `ConnectionManager` so op drivers (`operations::put::op_ctx_task`,
    /// reached via `op_manager.ring.connection_manager`) can gate emission of
    /// version-sensitive wire variants. The network-bridge layer
    /// (`P2pConnManager`) is not reachable from op drivers, so the
    /// `peer_supports_subscribe_hint`-style version check cannot read
    /// `P2pConnManager.connections` directly; this mirror closes that gap for
    /// the summary-first PUT probe (#4642 step 3-bis).
    ///
    /// Populated at connection establishment
    /// (`p2p_protoc::connection_lifecycle`, the same call site that populates
    /// `P2pConnManager.connections[addr].remote_version`) via
    /// `record_remote_version`. Never proactively removed: a stale entry can
    /// only matter if a different peer later reconnects at the exact same
    /// socket address with an older version, and any real reconnection at
    /// that address overwrites the entry before it is ever consulted (lookups
    /// only target addresses with a live connection). Unknown addresses
    /// (`None`) fail closed in `version_supports_summary_first_put`.
    #[allow(clippy::type_complexity)]
    remote_version: Arc<RwLock<BTreeMap<SocketAddr, (u8, u8, u16)>>>,
    /// Test-only override for the summary-first PUT probe version floor,
    /// threaded exactly like `subscribe_hint_floor_override`. `None` in
    /// production (the real `SUMMARY_FIRST_PUT_MIN_VERSION` applies);
    /// `Some(floor)` only in a sim test that opted in via
    /// `SimNetwork::enable_summary_first_put`. See `NodeConfig`.
    summary_first_put_floor_override: Option<(u8, u8, u16)>,
    /// Rotating start offset for the per-new-peer migration scan. The scan
    /// examines at most `MIGRATION_SCAN_CAP_PER_NEW_PEER` hosted contracts per
    /// event; advancing this cursor each event makes successive events cover
    /// different windows of the hosting set, so a node hosting more contracts
    /// than the cap doesn't repeatedly examine the same prefix (which would
    /// starve the rest of new-peer-triggered migration).
    migration_scan_cursor: Arc<AtomicUsize>,
    /// Transient connections keyed by socket address.
    transient_connections: Arc<DashMap<SocketAddr, TransientEntry>>,
    transient_in_use: Arc<AtomicUsize>,
    transient_budget: usize,
    transient_ttl: Duration,
    pub min_connections: usize,
    pub max_connections: usize,
    pub rnd_if_htl_above: usize,
    pub pub_key: Arc<TransportPublicKey>,
    /// Number of CONNECT operations currently being routed through this node.
    connect_in_flight: Arc<AtomicUsize>,
    /// Maximum concurrent CONNECT operations this node will route.
    /// Gateways use a bounded value; non-gateways use usize::MAX (unlimited).
    max_concurrent_connects: usize,
    /// Addresses that recently failed NAT traversal. Pre-populated into the
    /// CONNECT `visited` bloom filter so routing nodes skip these peers.
    /// Jitter is applied once at `record_failed_addr` time and stored as a
    /// `retry_after` instant, so consecutive reads are always consistent.
    recently_failed_addrs: Arc<parking_lot::Mutex<TrackedBackoff<SocketAddr>>>,
    /// Peers that have advertised readiness to accept non-CONNECT operations.
    ready_peers: Arc<DashSet<SocketAddr>>,
    /// Minimum connections before this peer advertises readiness.
    /// 0 means readiness gating is disabled (all peers treated as ready).
    pub min_ready_connections: usize,
    /// Tracks when each peer connection was established.
    /// Used for optimistic readiness timeout: if a peer hasn't sent a ReadyState
    /// message but has been connected for longer than `OPTIMISTIC_READY_TIMEOUT`,
    /// we treat them as ready (assuming the ReadyState message was lost).
    connected_since: Arc<RwLock<BTreeMap<SocketAddr, Instant>>>,
    /// Per-peer routing health statistics for eviction decisions.
    pub(crate) peer_health: Arc<Mutex<PeerHealthTracker>>,
    /// Consecutive CONNECT failures for location jitter and the time of the last failure.
    /// Incremented on failed hole-punch, reset on successful connect.
    /// The stored `Option<Instant>` enables time-based decay: consecutive failures
    /// accumulated during an idle period are partially wound down before the next
    /// attempt, preventing artificially inflated jitter after idle periods.
    connect_jitter_failures: Arc<parking_lot::Mutex<(u32, Option<Instant>)>>,
    /// Per-peer CONNECT acceptor reliability. Tracks success/attempt counts
    /// to estimate the probability that a peer can successfully accept a
    /// connection (primarily: NAT hole-punch success rate).
    ///
    /// Key: peer socket address
    /// Value: AcceptorStats (successes, attempts, last_updated)
    acceptor_reliability: Arc<parking_lot::RwLock<BTreeMap<SocketAddr, AcceptorStats>>>,
    /// Nearest-neighbor lattice discovery health counters (telemetry only).
    /// `lattice_probes_issued`: route-to-self probes fired since startup;
    /// `lattice_probe_improvements`: probes that found a strictly-closer peer
    /// (filled or tightened an edge). Incremented from the maintenance loop,
    /// surfaced via the dashboard ring-stats provider.
    lattice_probes_issued: Arc<AtomicU64>,
    lattice_probe_improvements: Arc<AtomicU64>,
}

impl ConnectionManager {
    pub fn new(config: &NodeConfig) -> Self {
        let min_connections = if let Some(v) = config.min_number_conn {
            v
        } else {
            Ring::DEFAULT_MIN_CONNECTIONS
        };

        let max_connections = if let Some(v) = config.max_number_conn {
            v
        } else {
            // Previously gateways were hardcoded to 20 here, which artificially capped
            // them at ~19 ring peers and contributed to CONNECT exclusion death spirals.
            // Gateways now use the same default (200) as regular peers since they need
            // MORE connections for routing diversity, not fewer.
            Ring::DEFAULT_MAX_CONNECTIONS
        };

        let max_upstream_bandwidth = if let Some(v) = config.max_upstream_bandwidth {
            v
        } else {
            Ring::DEFAULT_MAX_UPSTREAM_BANDWIDTH
        };

        let max_downstream_bandwidth = if let Some(v) = config.max_downstream_bandwidth {
            v
        } else {
            Ring::DEFAULT_MAX_DOWNSTREAM_BANDWIDTH
        };

        let rnd_if_htl_above = if let Some(v) = config.rnd_if_htl_above {
            v
        } else {
            Ring::DEFAULT_RAND_WALK_ABOVE_HTL
        };

        let own_location = if let Some(location) = config.location {
            AtomicU64::new(u64::from_le_bytes(location.as_f64().to_le_bytes()))
        } else if let Some(addr) = config.own_addr {
            // if the address is set, compute location from it (gateway case)
            let location = Location::from_address(&addr);
            AtomicU64::new(u64::from_le_bytes(location.as_f64().to_le_bytes()))
        } else {
            // for location here consider -1 == None
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

        let min_ready_connections = config.relay_ready_connections.unwrap_or(0);

        let mut cm = Self::init(
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            (
                config.key_pair.public().clone(),
                config.own_addr,
                own_location,
            ),
            config.is_gateway,
            config.transient_budget,
            config.transient_ttl,
            min_ready_connections,
        );
        // Set after init (rather than threading through init's many call sites):
        // None for all the test-helper constructions, Some(floor) only when a
        // sim opted into the migration cascade.
        cm.subscribe_hint_floor_override = config.subscribe_hint_floor_override;
        cm.summary_first_put_floor_override = config.summary_first_put_floor_override;
        cm
    }

    #[allow(clippy::too_many_arguments)]
    fn init(
        max_upstream_bandwidth: Rate,
        max_downstream_bandwidth: Rate,
        min_connections: usize,
        max_connections: usize,
        rnd_if_htl_above: usize,
        (pub_key, own_addr, own_location): (TransportPublicKey, Option<SocketAddr>, AtomicU64),
        is_gateway: bool,
        transient_budget: usize,
        transient_ttl: Duration,
        min_ready_connections: usize,
    ) -> Self {
        let topology_manager = Arc::new(RwLock::new(TopologyManager::new(Limits {
            max_upstream_bandwidth,
            max_downstream_bandwidth,
            min_connections,
            max_connections,
        })));

        Self {
            connections_by_location: Arc::new(RwLock::new(BTreeMap::new())),
            location_for_peer: Arc::new(RwLock::new(BTreeMap::new())),
            pending_reservations: Arc::new(RwLock::new(BTreeMap::new())),
            topology_manager,
            own_location: own_location.into(),
            own_addr: Arc::new(Mutex::new(own_addr)),
            is_gateway,
            // Default off; `new(config)` overrides from config after init.
            subscribe_hint_floor_override: None,
            remote_version: Arc::new(RwLock::new(BTreeMap::new())),
            // Default off; `new(config)` overrides from config after init.
            summary_first_put_floor_override: None,
            migration_scan_cursor: Arc::new(AtomicUsize::new(0)),
            transient_connections: Arc::new(DashMap::new()),
            transient_in_use: Arc::new(AtomicUsize::new(0)),
            transient_budget,
            transient_ttl,
            min_connections,
            max_connections,
            rnd_if_htl_above,
            pub_key: Arc::new(pub_key),
            connect_in_flight: Arc::new(AtomicUsize::new(0)),
            max_concurrent_connects: if is_gateway {
                MAX_CONCURRENT_GATEWAY_CONNECTS
            } else {
                usize::MAX
            },
            recently_failed_addrs: Arc::new(parking_lot::Mutex::new(TrackedBackoff::new(
                ExponentialBackoff::new(FAILED_ADDR_BASE_TTL, FAILED_ADDR_MAX_TTL),
                FAILED_ADDR_MAX_ENTRIES,
            ))),
            ready_peers: Arc::new(DashSet::new()),
            min_ready_connections,
            connected_since: Arc::new(RwLock::new(BTreeMap::new())),
            peer_health: Arc::new(Mutex::new(PeerHealthTracker::new())),
            connect_jitter_failures: Arc::new(parking_lot::Mutex::new((0, None))),
            acceptor_reliability: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
            lattice_probes_issued: Arc::new(AtomicU64::new(0)),
            lattice_probe_improvements: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Try to admit a new connect operation for routing.
    /// Returns `None` if the node is at capacity for concurrent connects.
    /// Returns `Some(ConnectAdmissionGuard)` on success — the slot is automatically
    /// released when the guard is dropped, preventing leaks on error paths.
    #[must_use]
    pub fn try_admit_connect(&self) -> Option<ConnectAdmissionGuard> {
        loop {
            let current = self.connect_in_flight.load(Ordering::Acquire);
            if current >= self.max_concurrent_connects {
                return None;
            }
            if self
                .connect_in_flight
                .compare_exchange_weak(current, current + 1, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return Some(ConnectAdmissionGuard::new(self.connect_in_flight.clone()));
            }
        }
    }

    /// Whether a node should accept a new node connection or not based
    /// on the relative location and other conditions.
    ///
    /// Does not panic when this node has no location assigned yet: every clause
    /// that needs the own location guards `get_stored_location()` and falls
    /// through to a location-independent decision.
    pub fn should_accept(&self, location: Location, addr: SocketAddr) -> bool {
        // Don't accept connections from ourselves
        if let Some(own_addr) = self.get_own_addr() {
            if own_addr == addr {
                tracing::warn!(
                    addr = %addr,
                    peer_location = %location,
                    "should_accept: rejecting self-connection attempt"
                );
                return false;
            }
        }
        let open = self.connection_count();
        let now = Instant::now();
        let reserved_before = self
            .pending_reservations
            .read()
            .iter()
            .filter(|(_, (_, created))| now.duration_since(*created) <= PENDING_RESERVATION_TTL)
            .count();

        tracing::debug!(
            addr = %addr,
            peer_location = %location,
            open,
            reserved_before,
            is_gateway = self.is_gateway,
            min = self.min_connections,
            max = self.max_connections,
            rnd_if_htl_above = self.rnd_if_htl_above,
            "should_accept: evaluating direct acceptance guard"
        );

        if self.is_gateway && (open > 0 || reserved_before > 0) {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                open,
                reserved_before,
                "Gateway evaluating additional direct connection (post-bootstrap)"
            );
        }

        if self.location_for_peer.read().get(&addr).is_some() {
            // Already connected or pending with this peer. Reject so the CONNECT
            // routes uphill to discover new, unconnected peers. Without this,
            // gap-based targeting repeatedly terminates at the same already-connected
            // peer, preventing connection growth beyond the initial neighborhood.
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "Peer already pending/connected; rejecting to route uphill for diversity"
            );
            return false;
        }

        {
            let mut pending = self.pending_reservations.write();
            pending.insert(addr, (location, Instant::now()));
        }

        let total_conn = match reserved_before
            .checked_add(1)
            .and_then(|val| val.checked_add(open))
        {
            Some(val) => val,
            None => {
                tracing::error!(
                    addr = %addr,
                    peer_location = %location,
                    reserved_before,
                    open,
                    "connection counters would overflow; rejecting connection"
                );
                self.pending_reservations.write().remove(&addr);
                return false;
            }
        };

        if open == 0 {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "should_accept: first connection -> accepting"
            );
            return true;
        }

        // Nearest-neighbor lattice clause (mechanism 3). BEFORE the Kleinberg
        // score paths: unconditionally accept a candidate that would become this
        // peer's new nearest ring-neighbor ON ITS OWN SIDE — its successor side
        // (higher location, signed_distance > 0) or its predecessor side (lower
        // location, signed_distance < 0). The test is STRICTLY PER-SIDE: a
        // candidate on the successor side is compared only to the current nearest
        // successor, never to the predecessor. A global "closer than any current
        // connection" test would only ever tighten ONE edge and let the other
        // side go quiet, which dead-ends ~half of last-mile lookups; both edges
        // are required for the closed undirected cycle greedy-to-closest needs.
        //
        // Under `max_connections` this fires for a fill OR a tighten (bypassing
        // Kleinberg, no displacement). AT/OVER max it is restricted to FILLING an
        // empty side (a genuine routing dead-end), up to the bounded ceiling
        // `max_connections + LATTICE_OVERMAX_SLACK` — see
        // `admits_lattice_edge_over_cap`. A fill over max displaces one long link
        // (the over-max prune in topology.rs then drops a farther, non-lattice
        // connection — never a protected lattice edge). A mere TIGHTEN is NOT
        // force-accepted over max: displacing a long link for a marginal locality
        // gain measurably regresses distant GET routing at low budgets, so a
        // tighten waits for an under-max slot. Self-bounding on each side: it
        // fires only for a candidate closer than every current connection on that
        // side, a strictly decreasing sequence, so it cannot admit an unbounded
        // stream.
        //
        // SECURITY (eclipse) — DISCLOSED and ACCEPTED tradeoff: this makes
        // per-side nearest admission DETERMINISTIC (pre-lattice a very-close peer
        // was often REJECTED by the probabilistic Kleinberg evaluator), and the
        // retention exemption (topology.rs) removes the score-based swap as a
        // safety valve, leaving LIVENESS eviction only — a responsive-but-malicious
        // nearest neighbor is not shed by scoring. The candidate location is
        // ADDRESS-DERIVED (`Location::from_address` masks host bits — a whole IPv4
        // /24, or IPv6 /48, collapses to one location; port participates only for
        // loopback), and the operator blocklist is checked in connect.rs BEFORE
        // should_accept so this force-accept cannot bypass it.
        //
        // The IPv6 cost is NOT symmetric with IPv4, and the honest disclosure is:
        // a single routed IPv6 allocation (an ISP hands out a /48, /56, or /64 per
        // customer) already yields a VAST number of distinct /48 `Location`s
        // essentially for free, so an attacker can cheaply mint locations that
        // straddle a chosen victim's own location and become BOTH of that victim's
        // per-side nearest edges — a targeted last-mile eclipse of that one
        // victim's 2 lattice slots. What it is NOT: a network partition or a global
        // takeover. The damage is bounded to the two slots of a
        // SPECIFICALLY-TARGETED victim (it does not compound across the ring), the
        // exemption only ever protects whoever is GENUINELY per-side nearest, and a
        // closer HONEST peer force-displaces the attacker. The project lead has
        // EXPLICITLY ACCEPTED this tradeoff: cheap IPv6 last-mile eclipse of a
        // single victim's 2 slots, in exchange for the findability the guaranteed
        // lattice buys. A routing-correctness eviction signal (shedding a
        // responsive peer that drops/corrupts payloads) is future work.
        let nn_override = if self.nn_lattice_active() {
            if let Some(me) = self.get_stored_location() {
                let cand_signed = me.signed_distance(location);
                let cand_dist = cand_signed.abs();
                if cand_dist == 0.0 {
                    // A candidate exactly at own location is on neither side.
                    false
                } else {
                    let successor_side = cand_signed > 0.0;
                    let current_nearest = self
                        .nearest_lattice_neighbor_dist(successor_side)
                        .unwrap_or(f64::INFINITY);
                    let fires = cand_dist < current_nearest;
                    if fires {
                        tracing::debug!(
                            addr = %addr,
                            peer_location = %location,
                            cand_dist,
                            current_nearest,
                            side = if successor_side { "successor" } else { "predecessor" },
                            "should_accept: nearest-neighbor lattice force-accept (new nearest on its side)"
                        );
                    }
                    fires
                }
            } else {
                false
            }
        } else {
            false
        };

        // Use actual open connections (not inflated total_conn) for the
        // min_connections threshold. Pending reservations are speculative —
        // many fail to complete — so counting them pushes nodes into the
        // topology evaluator path prematurely, causing rejections when the
        // node genuinely needs more connections.
        // total_conn (which includes pending) is still used for max_connections
        // to prevent over-commitment.
        let accepted = if total_conn >= self.max_connections {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                "should_accept: rejected (max connections reached)"
            );
            false
        } else if open < self.min_connections {
            // Below min_connections: use gap score as a soft probabilistic
            // filter to shape the distribution during bootstrap. We don't use
            // the strict ConnectionEvaluator here because its "beat all in
            // window" policy would accept at most one connection per window,
            // stalling bootstrap when many peers try to connect.
            //
            // Below KLEINBERG_FILTER_MIN_CONNECTIONS we always accept (too few
            // connections for a meaningful gap score). Above that, selectivity
            // scales with how close we are to min_connections:
            //   - At KLEINBERG_FILTER_MIN_CONNECTIONS: floor ~0.9 (accept almost all)
            //   - Approaching min_connections: floor ~0.3 (be selective)
            // This prevents bootstrap stalls when NAT traversal is unreliable
            // while still shaping topology as the node fills up.
            let accepted = if open < KLEINBERG_FILTER_MIN_CONNECTIONS {
                true
            } else if let Some(me) = self.get_stored_location() {
                let score = self.compute_kleinberg_score(me, location);
                // Compute a sliding floor based on how far we are from min_connections.
                // progress=0.0 at KLEINBERG_FILTER_MIN_CONNECTIONS, 1.0 at min_connections.
                let range = self
                    .min_connections
                    .saturating_sub(KLEINBERG_FILTER_MIN_CONNECTIONS);
                let progress = if range > 0 {
                    (open - KLEINBERG_FILTER_MIN_CONNECTIONS) as f64 / range as f64
                } else {
                    1.0
                };
                // Floor slides from 0.9 (desperate for connections) to 0.3 (nearly full).
                let floor = 0.9 - 0.6 * progress;
                let accept_prob = (floor + score).min(1.0);
                tracing::debug!(
                    open,
                    min = self.min_connections,
                    %progress,
                    %floor,
                    %score,
                    %accept_prob,
                    "should_accept: sliding Kleinberg floor"
                );
                GlobalRng::random_range(0.0..1.0) < accept_prob
            } else {
                true
            };

            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                open,
                total_conn,
                accepted,
                "should_accept: below min_connections (Kleinberg soft filter)"
            );
            accepted
        } else {
            // At/above min_connections: feed gap score through the
            // ConnectionEvaluator which picks the best candidate from recent
            // arrivals. This rate-limits acceptance to maintain topology
            // quality while the node has enough connections for routing.
            let accepted = if let Some(me) = self.get_stored_location() {
                let score = self.compute_kleinberg_score(me, location);
                self.topology_manager
                    .write()
                    .evaluate_new_connection_with_score(score, Instant::now())
            } else {
                true
            };

            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                accepted,
                "should_accept: above min_connections (Kleinberg evaluator)"
            );
            accepted
        };
        // Nearest-neighbor lattice force-accept (mechanism 3). A new per-side
        // nearest neighbor is admitted regardless of the Kleinberg verdict. UNDER
        // max this admits a fill OR a tighten (no displacement, just bypassing
        // Kleinberg). AT/OVER max, force-accept is restricted to FILLING an empty
        // side (a genuine dead-end) within the ceiling — a mere tighten waits for
        // an under-max slot rather than displacing a long link (see
        // `admits_lattice_edge_over_cap`).
        let accepted = accepted
            || (nn_override && total_conn < self.max_connections)
            // Exclude THIS candidate's just-inserted reservation (added above) so
            // the reservation-aware per-side concurrency bound counts only OTHER
            // in-flight fills (F1).
            || self.admits_lattice_edge_over_cap(location, Some(addr));
        tracing::debug!(
            addr = %addr,
            peer_location = %location,
            accepted,
            total_conn,
            open_connections = open,
            reserved_connections = self.pending_reservations.read().len(),
            max_connections = self.max_connections,
            min_connections = self.min_connections,
            "should_accept: final decision"
        );
        if !accepted {
            self.pending_reservations.write().remove(&addr);
        } else {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                total_conn,
                "should_accept: accepted (reserving spot)"
            );
            self.record_pending_location(addr, location);
        }
        accepted
    }

    /// Compute the Kleinberg gap score for a candidate connection.
    ///
    /// Measures how much the candidate improves the 1/d distance distribution by
    /// finding the candidate's min distance to its nearest neighbor in log-space.
    ///
    /// Uses non-directional scoring: connection acceptance happens during bootstrap
    /// when the ring is forming. Directional (CW/CCW) analysis is applied later
    /// during steady-state topology management (swaps and pruning in topology.rs).
    fn compute_kleinberg_score(&self, my_location: Location, candidate: Location) -> f64 {
        let candidate_distance = my_location.distance(candidate).as_f64();
        let connections = self.connections_by_location.read();
        crate::topology::small_world_rand::kleinberg_score(
            candidate_distance,
            connections.iter().flat_map(|(loc, conns)| {
                std::iter::repeat_n(my_location.distance(*loc).as_f64(), conns.len())
            }),
        )
    }

    /// Unsigned ring distance to this peer's nearest connected neighbor on the
    /// given side (`successor_side = true` → closest higher location wrapping;
    /// `false` → closest lower location). `None` when own location is unknown or
    /// there is no connected neighbor on that side.
    ///
    /// This is the "who holds my reserved lattice slot on this side" query used
    /// by the per-side acceptance clause and by the route-to-self discovery
    /// probe. The reserved slot is derived on demand from the live connection set
    /// rather than tracked as separate state, so a strictly-closer arrival
    /// automatically becomes the new slot holder and the superseded former
    /// nearest demotes back into the ordinary long-link pool with no bookkeeping.
    pub(crate) fn nearest_lattice_neighbor_dist(&self, successor_side: bool) -> Option<f64> {
        let me = self.get_stored_location()?;
        self.connections_by_location
            .read()
            .keys()
            .filter_map(|loc| {
                let sd = me.signed_distance(*loc);
                // A neighbor exactly at own location (sd == 0.0) belongs to
                // neither side; exclude it.
                if sd != 0.0 && (sd > 0.0) == successor_side {
                    Some(sd.abs())
                } else {
                    None
                }
            })
            .fold(None, |acc, d| Some(acc.map_or(d, |a: f64| a.min(d))))
    }

    /// Record that a route-to-self lattice discovery probe was ISSUED. Telemetry
    /// only — surfaced via the dashboard ring-stats provider.
    pub(crate) fn record_lattice_probe_issued(&self) {
        self.lattice_probes_issued.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that the lattice IMPROVED (a side filled or an edge tightened
    /// toward the true nearest). Counted INDEPENDENTLY of probe issuance because
    /// the improvement lands a few maintenance ticks after the probe that caused
    /// it (the CONNECT completes asynchronously), so tying the two together (the
    /// old coupled counter) miscounted — it scored the first probe as an
    /// improvement on failure and missed the fill that ended a probe burst.
    /// Telemetry only.
    pub(crate) fn record_lattice_probe_improvement(&self) {
        self.lattice_probe_improvements
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Discovery-health counters: (probes issued, probes that found a strictly
    /// closer peer) since startup.
    pub(crate) fn lattice_probe_stats(&self) -> (u64, u64) {
        (
            self.lattice_probes_issued.load(Ordering::Relaxed),
            self.lattice_probe_improvements.load(Ordering::Relaxed),
        )
    }

    /// Whether the nearest-neighbor ring lattice is ACTIVE for THIS peer: the
    /// flag is set AND this peer's `max_connections` clears the minimum-degree
    /// floor ([`NN_LATTICE_MIN_MAX_CONNECTIONS`]). This is the single activation
    /// gate for every feature site on the acceptance path (the per-side
    /// acceptance clause and the over-cap admission); the discovery probe
    /// (`ring.rs`) and the retention exemption (`topology.rs`) apply the same gate
    /// via [`nn_lattice_active_for`] against their own `max_connections`.
    #[inline]
    pub(crate) fn nn_lattice_active(&self) -> bool {
        nn_lattice_active_for(self.max_connections)
    }

    /// Whether a candidate at `loc` would FILL an EMPTY per-side lattice slot: it
    /// is on a ring side (successor if higher location wrapping, predecessor if
    /// lower) that currently has NO connected neighbor at all. This is the strict
    /// subset of "new per-side nearest" that closes a genuine routing dead-end
    /// (no connected neighbor closer to the key on that side), as opposed to
    /// merely TIGHTENING an already-held side. Only a fill justifies displacing a
    /// long link over the `max_connections` cap; see
    /// [`Self::admits_lattice_edge_over_cap`].
    pub(crate) fn would_fill_empty_lattice_side(&self, loc: Location) -> bool {
        if !self.nn_lattice_active() {
            return false;
        }
        let Some(me) = self.get_stored_location() else {
            return false;
        };
        let sd = me.signed_distance(loc);
        if sd == 0.0 {
            return false;
        }
        let successor_side = sd > 0.0;
        // The side currently holds NO connected neighbor (an empty lattice slot).
        self.nearest_lattice_neighbor_dist(successor_side).is_none()
    }

    /// Whether a candidate at `loc` may be admitted even though the node is at or
    /// over `max_connections`. Over-cap admission is restricted to FILLING an
    /// EMPTY per-side lattice slot ([`Self::would_fill_empty_lattice_side`]) —
    /// closing a genuine routing dead-end — within the absolute over-max ceiling
    /// (`max_connections + LATTICE_OVERMAX_SLACK`).
    ///
    /// A mere TIGHTEN of an already-filled side is deliberately NOT admitted over
    /// the cap: it would displace a long-range link for only a marginal locality
    /// gain, and stripping long links measurably regresses distant GET routing at
    /// low connection budgets (validated in the control-vs-fix sim — activating an
    /// unconditional over-cap displacement dropped mean GET find-rate at
    /// max_connections=5). Tightening therefore waits for a slot UNDER max;
    /// over-max displacement is reserved for the dead-end case that motivated
    /// mechanism 3 (invariant 5, findability).
    ///
    /// This is the SINGLE over-cap admission predicate shared by every gate that
    /// can add a ring connection: `should_accept`, the two connection-lifecycle
    /// promotion gates (`p2p_protoc/connection_lifecycle.rs`), and
    /// [`Self::add_connection`]. Keeping the decision in one place is deliberate —
    /// before it was wired through the lifecycle gates, mechanism 3's over-cap
    /// displacement was inert on the real CONNECT path (the gates rejected at
    /// `>= max_connections` first). The ceiling bounds the over-max excess (see
    /// [`LATTICE_OVERMAX_SLACK`]); with fill-only admission the excess is at most
    /// the number of empty sides (<= 2), so the ceiling is a defensive transient
    /// bound rather than a frequently-binding limit.
    ///
    /// RESERVATION-AWARE (F1): the fill decision reads the ESTABLISHED connection
    /// set only (`would_fill_empty_lattice_side`), so without this it would
    /// re-fire for EVERY concurrent candidate on the same empty side — a full node
    /// with an empty side would force-accept + reserve + NAT-complete all of them,
    /// piling several over-cap fills onto one side. We therefore also require that
    /// no OTHER in-flight pending reservation already targets that side
    /// ([`Self::pending_reservation_on_side`]), bounding concurrent over-cap fills
    /// to ONE per side. `exclude_addr` is the candidate under evaluation, whose own
    /// reservation `should_accept`/`add_connection` may have already inserted;
    /// pass `Some(addr)` so it is not counted against itself (`None` when there is
    /// no such self-reservation to exclude).
    pub(crate) fn admits_lattice_edge_over_cap(
        &self,
        loc: Location,
        exclude_addr: Option<SocketAddr>,
    ) -> bool {
        self.would_fill_empty_lattice_side(loc)
            && self.connection_count() < self.max_connections + LATTICE_OVERMAX_SLACK
            && !self.pending_reservation_on_side(loc, exclude_addr)
    }

    /// Whether any OTHER TTL-valid pending reservation already targets the same
    /// ring side as `loc` (successor vs predecessor, relative to own location),
    /// ignoring `exclude_addr` (the candidate currently under evaluation, whose
    /// own reservation the caller has already inserted). Used by
    /// [`Self::admits_lattice_edge_over_cap`] to bound concurrent over-cap lattice
    /// fills to one per side (F1). Returns `false` (nothing blocks) when own
    /// location is unknown or `loc` sits exactly on own location.
    fn pending_reservation_on_side(&self, loc: Location, exclude_addr: Option<SocketAddr>) -> bool {
        let Some(me) = self.get_stored_location() else {
            return false;
        };
        let sd = me.signed_distance(loc);
        if sd == 0.0 {
            return false;
        }
        let successor_side = sd > 0.0;
        let now = Instant::now();
        self.pending_reservations
            .read()
            .iter()
            .any(|(resv_addr, (resv_loc, created))| {
                if Some(*resv_addr) == exclude_addr {
                    return false;
                }
                if now.duration_since(*created) > PENDING_RESERVATION_TTL {
                    return false;
                }
                let rsd = me.signed_distance(*resv_loc);
                rsd != 0.0 && (rsd > 0.0) == successor_side
            })
    }

    /// Record the advertised location for a peer that we have decided to accept.
    ///
    /// This makes the peer discoverable to the routing layer even before the connection
    /// is fully established. The entry is removed automatically if the handshake fails
    /// via `prune_in_transit_connection`.
    pub fn record_pending_location(&self, addr: SocketAddr, location: Location) {
        // Perform the mutation under the write lock, then drop the lock
        // before emitting tracing. The tracing macro can be non-trivial
        // (subscriber appender + format work) and holding `location_for_peer`
        // across it serializes unrelated callers (clippy:
        // `significant_drop_tightening`).
        let already_known = {
            let mut locations = self.location_for_peer.write();
            match locations.entry(addr) {
                Entry::Occupied(_) => true,
                Entry::Vacant(v) => {
                    v.insert(location);
                    false
                }
            }
        };
        if already_known {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "record_pending_location: location already known"
            );
        } else {
            tracing::debug!(
                addr = %addr,
                peer_location = %location,
                "record_pending_location: registering advertised location for peer"
            );
        }
    }

    /// Update this node location, only if not already set.
    ///
    /// This preserves configured locations (set during initialization) while allowing
    /// peers behind NAT to learn their location from the observed address.
    pub fn update_location(&self, loc: Option<Location>) {
        if let Some(loc) = loc {
            // Only update if current location is unset (-1.0)
            let current_bits = self.own_location.load(std::sync::atomic::Ordering::Acquire);
            let current_val = f64::from_le_bytes(current_bits.to_le_bytes());
            if current_val >= 0.0 {
                // Location already set (e.g., from config), don't overwrite
                tracing::debug!(
                    current_location = current_val,
                    new_location = loc.as_f64(),
                    "update_location: preserving existing location"
                );
                return;
            }
            self.own_location.store(
                u64::from_le_bytes(loc.as_f64().to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            );
        } else {
            self.own_location.store(
                u64::from_le_bytes((-1f64).to_le_bytes()),
                std::sync::atomic::Ordering::Release,
            )
        }
    }

    /// Returns this node's PeerKeyLocation.
    ///
    /// If the node's external address is not yet known (e.g., peer behind NAT
    /// that hasn't received ObservedAddress yet), returns a PeerKeyLocation
    /// with PeerAddr::Unknown.
    /// Test-only placement-migration version-floor override (`None` in
    /// production). See `NodeConfig::subscribe_hint_floor_override`.
    pub(crate) fn subscribe_hint_floor_override(&self) -> Option<(u8, u8, u16)> {
        self.subscribe_hint_floor_override
    }

    /// Record the negotiated protocol version for a peer at `addr`, mirroring
    /// `P2pConnManager.connections[addr].remote_version` so op drivers can
    /// read it via `op_manager.ring.connection_manager`. Overwrites any prior
    /// entry for `addr` (a fresh connection at a reused address always wins).
    ///
    /// Called from `p2p_protoc::connection_lifecycle` at the same point
    /// `P2pConnManager` records `remote_version` on its own `connections`
    /// entry — keep the two writes together if that call site changes.
    pub(crate) fn record_remote_version(&self, addr: SocketAddr, version: (u8, u8, u16)) {
        self.remote_version.write().insert(addr, version);
    }

    /// Look up the negotiated protocol version recorded for `addr` via
    /// [`Self::record_remote_version`]. `None` if no connection has been
    /// established at `addr` (or none since this node started).
    pub(crate) fn remote_version(&self, addr: SocketAddr) -> Option<(u8, u8, u16)> {
        self.remote_version.read().get(&addr).copied()
    }

    /// Test-only override for the summary-first PUT probe version floor
    /// (`None` in production). See `NodeConfig::summary_first_put_floor_override`.
    pub(crate) fn summary_first_put_floor_override(&self) -> Option<(u8, u8, u16)> {
        self.summary_first_put_floor_override
    }

    /// Whether the peer at `addr` reports a negotiated protocol version new
    /// enough to understand the summary-first PUT probe/dispatch variants
    /// (`PutMsg::ProbeRequest` / `ProbeResponse` / `ProbeReconcile`).
    ///
    /// Driver-reachable mirror of
    /// `P2pConnManager::peer_supports_subscribe_hint`: op drivers
    /// (`operations::put::op_ctx_task`) only reach `op_manager.ring`, not the
    /// network-bridge layer, so this reads the `remote_version` mirror above
    /// instead of `P2pConnManager.connections`. `None` (unknown version) is
    /// treated as unsupported (fail-closed): an older peer that cannot
    /// deserialize the appended `ProbeRequest` wire tag would drop the
    /// connection, so when in doubt the caller must not emit it.
    pub(crate) fn supports_summary_first_put(&self, addr: SocketAddr) -> bool {
        let remote = self.remote_version(addr);
        let floor = self
            .summary_first_put_floor_override()
            .unwrap_or(crate::node::SUMMARY_FIRST_PUT_MIN_VERSION);
        crate::node::version_supports_summary_first_put(remote, floor)
    }

    /// Reserve the next `window`-sized slice of the hosting set for a migration
    /// scan and advance the rotating cursor by `window`. Returns the start
    /// offset (callers should take it modulo the current hosting-set size). Over
    /// successive new-peer events this rotates coverage across all hosted
    /// contracts rather than repeatedly scanning the same prefix.
    pub(crate) fn next_migration_scan_offset(&self, window: usize) -> usize {
        self.migration_scan_cursor
            .fetch_add(window, Ordering::Relaxed)
    }

    pub fn own_location(&self) -> PeerKeyLocation {
        match self.get_own_addr() {
            Some(addr) => PeerKeyLocation::new((*self.pub_key).clone(), addr),
            None => PeerKeyLocation::with_unknown_addr((*self.pub_key).clone()),
        }
    }

    /// Returns all ring-connected peer locations.
    pub fn location_for_all_peers(&self) -> Vec<Location> {
        self.connections_by_location
            .read()
            .keys()
            .copied()
            .collect()
    }

    /// Returns our own socket address if set.
    pub fn get_own_addr(&self) -> Option<SocketAddr> {
        *self.own_addr.lock()
    }

    /// Returns our own socket address, or `RingError::PeerNotJoined` if not yet established.
    pub fn peer_addr(&self) -> Result<SocketAddr, super::RingError> {
        self.get_own_addr().ok_or(super::RingError::PeerNotJoined)
    }

    /// Returns the stored ring location, if set.
    /// This is the location that was set by update_location(), typically from
    /// the externally observed address received via ObservedAddress message.
    pub fn get_stored_location(&self) -> Option<Location> {
        let bits = self.own_location.load(std::sync::atomic::Ordering::Acquire);
        let val = f64::from_le_bytes(bits.to_le_bytes());
        if val < 0.0 {
            None
        } else {
            Some(Location::new(val))
        }
    }

    /// Look up a PeerKeyLocation by socket address from connections_by_location or transient connections.
    pub fn get_peer_by_addr(&self, addr: SocketAddr) -> Option<PeerKeyLocation> {
        // Phase 1: Check connections by location (direct address match).
        // We release the connections_by_location lock before phase 2 to
        // respect the lock ordering: location_for_peer → connections_by_location.
        {
            let connections = self.connections_by_location.read();
            for conns in connections.values() {
                for conn in conns {
                    if conn.location.socket_addr() == Some(addr) {
                        return Some(conn.location.clone());
                    }
                }
            }
        }

        // Phase 2 (fallback): The transport address may differ from the advertised
        // address stored in PeerKeyLocation. Use location_for_peer to bridge the
        // gap. We acquire location_for_peer first, then re-acquire
        // connections_by_location — this respects the documented lock ordering
        // (location_for_peer → connections_by_location) and avoids deadlock with
        // prune_connection which acquires them in the same order with write locks.
        let location = *self.location_for_peer.read().get(&addr)?;
        // Materialize the resolved PeerKeyLocation under the read lock, then
        // drop the guard before emitting tracing (clippy:
        // `significant_drop_tightening`).
        let resolved = self
            .connections_by_location
            .read()
            .get(&location)
            .and_then(|conns| conns.first().map(|conn| conn.location.clone()));
        if let Some(resolved) = resolved {
            tracing::debug!(
                requested_addr = %addr,
                resolved_via = "location_for_peer",
                location = %location,
                "get_peer_by_addr: resolved via location_for_peer fallback"
            );
            return Some(resolved);
        }

        None
    }

    /// Look up the configured Location for a peer by socket address.
    /// This returns the actual ring location the peer was assigned, not the location
    /// computed from IP address (which would be different).
    #[allow(dead_code)] // Available for future use
    pub fn get_configured_location_for_peer(&self, addr: SocketAddr) -> Option<Location> {
        self.location_for_peer.read().get(&addr).copied()
    }

    /// Look up a PeerKeyLocation by socket address from connections_by_location or transient connections.
    /// Used for connection-based routing when we need full peer info from just an address.
    pub fn get_peer_location_by_addr(&self, addr: SocketAddr) -> Option<PeerKeyLocation> {
        // Check connections by location
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.location.clone());
                }
            }
        }
        drop(connections);

        // Transient connections don't have full PeerKeyLocation info
        None
    }

    /// Look up a PeerKeyLocation by public key from connections_by_location.
    /// Used for finding connected peers when we only have their public key (e.g., from interest manager).
    pub fn get_peer_by_pub_key(
        &self,
        pub_key: &crate::transport::TransportPublicKey,
    ) -> Option<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        for conns in connections.values() {
            for conn in conns {
                if &conn.location.pub_key == pub_key {
                    return Some(conn.location.clone());
                }
            }
        }
        None
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    /// Attempts to register a transient connection, enforcing the configured budget.
    /// Returns `false` when the budget is exhausted, leaving the map unchanged.
    pub fn try_register_transient(&self, addr: SocketAddr, location: Option<Location>) -> bool {
        if self.transient_connections.contains_key(&addr) {
            if let Some(mut entry) = self.transient_connections.get_mut(&addr) {
                entry.location = location;
            }
            return true;
        }

        let current = self.transient_in_use.load(Ordering::Acquire);
        if current >= self.transient_budget {
            return false;
        }

        self.transient_connections.insert(
            addr,
            TransientEntry {
                location,
                created_at: Instant::now(),
            },
        );
        let prev = self.transient_in_use.fetch_add(1, Ordering::SeqCst);
        if prev >= self.transient_budget {
            // Undo if we raced past the budget.
            self.transient_connections.remove(&addr);
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        true
    }

    /// Drops a transient connection and returns its metadata, if it existed.
    /// Also decrements the transient budget counter.
    pub fn drop_transient(&self, addr: SocketAddr) -> Option<TransientEntry> {
        let removed = self
            .transient_connections
            .remove(&addr)
            .map(|(_, entry)| entry);
        if removed.is_some() {
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
        }
        removed
    }

    /// Check whether a peer is currently tracked as transient.
    pub fn is_transient(&self, addr: SocketAddr) -> bool {
        self.transient_connections.contains_key(&addr)
    }

    /// Current number of tracked transient connections.
    pub fn transient_count(&self) -> usize {
        self.transient_in_use.load(Ordering::Acquire)
    }

    /// Maximum transient slots allowed.
    pub fn transient_budget(&self) -> usize {
        self.transient_budget
    }

    /// Time-to-live for transients before automatic drop.
    pub fn transient_ttl(&self) -> Duration {
        self.transient_ttl
    }

    /// Sets the own address if it is not already set, or returns the current address.
    pub fn try_set_own_addr(&self, addr: SocketAddr) -> Option<SocketAddr> {
        let mut own_addr = self.own_addr.lock();
        if own_addr.is_none() {
            *own_addr = Some(addr);
            crate::node::network_status::set_external_address(addr);
            tracing::info!(
                addr = %addr,
                "try_set_own_addr: initialized own address"
            );
            None
        } else {
            tracing::debug!(
                existing = ?*own_addr,
                attempted = %addr,
                "try_set_own_addr: address already set, keeping existing"
            );
            *own_addr
        }
    }

    /// Sets the own address unconditionally.
    /// Used when a peer behind NAT learns their external address from ObservedAddress.
    pub fn set_own_addr(&self, addr: SocketAddr) {
        // Hold the `own_addr` mutex across the `network_status::set_external_address`
        // call so the two state writes are atomic with respect to each other.
        // `own_addr` and `network_status::external_address` mirror the same value;
        // if the lock is released between the writes, two concurrent
        // `set_own_addr(X)` / `set_own_addr(Y)` calls can interleave such that
        // `own_addr` ends up `Y` while `external_address` ends up `X` permanently
        // (writer B's mutex section completes before writer A reaches
        // `set_external_address`). See issue #4172 / PR #4129.
        //
        // This is the same cross-structure atomicity discipline described in
        // `.claude/rules/ring.md` ("Cross-DashMap Lock Discipline") — a Mutex
        // here rather than a DashMap shard guard, but the same spirit applies.
        //
        // The `significant_drop_tightening` clippy lint will want to narrow this
        // lock back down (as PR #4129 did, introducing the race). It MUST NOT:
        // the guard is load-bearing for cross-structure atomicity, not just for
        // serializing the mutex write itself. A blind `cargo clippy --fix` would
        // silently re-introduce the #4172 TOCTOU.
        let old_addr = {
            let mut own_addr = self.own_addr.lock();
            let old = *own_addr;
            *own_addr = Some(addr);
            crate::node::network_status::set_external_address(addr);
            old
        };
        // Tracing is deliberately outside the lock: only the two state writes
        // above need to be atomic w.r.t. each other; logging does not.
        tracing::debug!(
            old_addr = ?old_addr,
            new_addr = %addr,
            "set_own_addr called"
        );
    }

    pub fn prune_alive_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, true)
    }

    pub fn prune_in_transit_connection(&self, addr: SocketAddr) -> Option<Location> {
        self.prune_connection(addr, false)
    }

    /// Clear pending reservations for specific addresses.
    ///
    /// Used during isolation recovery (#3319): when a peer has zero ring connections
    /// but all gateways appear "connected/pending" due to stale reservations from
    /// previous failed CONNECT attempts, this forces them to become retryable
    /// immediately instead of waiting for the 60-second TTL to expire.
    ///
    /// Uses two-phase locking to respect the documented lock ordering
    /// (`location_for_peer` before `pending_reservations`). See module-level
    /// comment for the ordering rationale.
    pub fn clear_pending_reservations_for(&self, addrs: &[SocketAddr]) {
        // Phase 1: Remove from pending_reservations, collect which were actually removed.
        // Release this lock before acquiring location_for_peer to avoid ABBA deadlock
        // with prune_connection (which acquires location_for_peer → pending_reservations).
        let removed: Vec<SocketAddr> = {
            let mut pending = self.pending_reservations.write();
            addrs
                .iter()
                .filter(|addr| pending.remove(addr).is_some())
                .copied()
                .collect()
        }; // pending_reservations lock released

        // Phase 2: Remove corresponding location_for_peer entries so
        // has_connection_or_pending() returns false for these addresses.
        if !removed.is_empty() {
            let mut location_for_peer = self.location_for_peer.write();
            for addr in &removed {
                location_for_peer.remove(addr);
                tracing::debug!(
                    addr = %addr,
                    "Cleared stale pending reservation for isolated peer recovery"
                );
            }
        }
    }

    /// Get the duration of an existing connection by address in milliseconds.
    /// Returns None if the connection doesn't exist.
    pub fn get_connection_duration_ms(&self, addr: SocketAddr) -> Option<u64> {
        let loc = {
            let locations_for_peer = self.location_for_peer.read();
            locations_for_peer.get(&addr).cloned()?
        };

        let conns = self.connections_by_location.read();
        if let Some(conns) = conns.get(&loc) {
            for conn in conns {
                if conn.location.socket_addr() == Some(addr) {
                    return Some(conn.duration_ms());
                }
            }
        }
        None
    }

    /// Add a connection to the ring topology. Returns `true` if the connection
    /// was actually inserted, `false` if it was rejected (e.g., capacity cap).
    pub fn add_connection(
        &self,
        loc: Location,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        was_reserved: bool,
    ) -> bool {
        tracing::info!(
            addr = %addr,
            peer_location = %loc,
            was_reserved = %was_reserved,
            "Adding connection to ring topology"
        );
        // Verify we're not adding a connection to ourselves (if we know our own address)
        debug_assert!(self.get_own_addr().map(|own| own != addr).unwrap_or(true));
        if was_reserved {
            self.pending_reservations.write().remove(&addr);
        }
        let mut lop = self.location_for_peer.write();
        let previous_location = lop.insert(addr, loc);
        drop(lop);

        // Enforce the global cap when adding a new peer (relocations reuse the existing slot).
        // EXCEPTION (mechanism 3): admit a strictly-closer per-side
        // nearest-neighbor lattice edge even at capacity — it displaces a farther
        // long link rather than being rejected — but only within the bounded
        // over-max ceiling (`admits_lattice_edge_over_cap`, see
        // LATTICE_OVERMAX_SLACK). The over-max topology prune sheds a non-lattice
        // connection on the next maintenance tick (lattice edges are protected
        // there), bringing the node back to max. The two connection-lifecycle
        // promotion gates apply the SAME predicate before reaching here, so a
        // lattice edge is no longer silently dropped at the cap on the real
        // CONNECT path.
        if previous_location.is_none() && self.connection_count() >= self.max_connections {
            // Exclude this candidate's own reservation (still present here — it is
            // pruned only in the reject branch below) from the reservation-aware
            // per-side concurrency bound (F1).
            if self.admits_lattice_edge_over_cap(loc, Some(addr)) {
                tracing::debug!(
                    addr = %addr,
                    peer_location = %loc,
                    max = self.max_connections,
                    "add_connection: admitting nearest-neighbor lattice edge over cap (a long link will be pruned)"
                );
            } else {
                tracing::warn!(
                    addr = %addr,
                    peer_location = %loc,
                    max = self.max_connections,
                    "add_connection: rejecting new connection to enforce cap"
                );
                // Roll back bookkeeping since we're refusing the connection.
                self.location_for_peer.write().remove(&addr);
                if was_reserved {
                    self.pending_reservations.write().remove(&addr);
                }
                return false;
            }
        }

        if let Some(prev_loc) = previous_location {
            tracing::debug!(
                addr = %addr,
                prev_location = %prev_loc,
                new_location = %loc,
                "add_connection: replacing existing connection for peer"
            );
            let mut cbl = self.connections_by_location.write();
            if let Some(prev_list) = cbl.get_mut(&prev_loc) {
                if let Some(pos) = prev_list
                    .iter()
                    .position(|c| c.location.socket_addr() == Some(addr))
                {
                    prev_list.swap_remove(pos);
                }
                if prev_list.is_empty() {
                    cbl.remove(&prev_loc);
                }
            }
        }

        {
            let mut cbl = self.connections_by_location.write();
            cbl.entry(loc)
                .or_default()
                .push(Connection::new(PeerKeyLocation::new(pub_key, addr)));
        }

        // Verify the insertion actually persisted — detect silent state corruption.
        let count_after = self.connection_count();
        // F4: the over-cap lattice ceiling (`admits_lattice_edge_over_cap`) is a
        // check-then-insert: it reads `connection_count()` under `max +
        // LATTICE_OVERMAX_SLACK`, then this method inserts. That is NOT atomic, so
        // the ceiling only holds because EVERY connection add runs on the single
        // p2p event-loop task (`connection_lifecycle.rs`) — there is no concurrent
        // adder to race the check against the insert. If a future refactor moves
        // connection-add off that single task, this check-then-insert must be made
        // atomic (or the count re-checked under a lock) or the ceiling can be
        // overshot. This assertion catches a ceiling breach in test/debug builds so
        // that invariant violation is loud rather than silent.
        debug_assert!(
            count_after <= self.max_connections + LATTICE_OVERMAX_SLACK,
            "connection_count {count_after} exceeded the over-cap lattice ceiling \
             (max_connections {} + LATTICE_OVERMAX_SLACK {LATTICE_OVERMAX_SLACK}); the \
             non-atomic ceiling check-then-insert relies on all connection adds \
             running on the single p2p event-loop task — a breach means that \
             single-task invariant was violated",
            self.max_connections,
        );
        let in_location_map = self.location_for_peer.read().contains_key(&addr);
        if !in_location_map || count_after == 0 {
            tracing::error!(
                addr = %addr,
                peer_location = %loc,
                connection_count = count_after,
                in_location_map,
                "add_connection: INVARIANT VIOLATION - connection not found after insertion"
            );
        } else {
            tracing::info!(
                addr = %addr,
                peer_location = %loc,
                connection_count = count_after,
                "add_connection: successfully added to ring"
            );
        }

        // Remove from transient connections if present, since we're now a full ring connection.
        if self.transient_connections.remove(&addr).is_some() {
            self.transient_in_use.fetch_sub(1, Ordering::SeqCst);
        }

        // Track connection time for optimistic readiness timeout.
        self.connected_since.write().insert(addr, Instant::now());

        // Initialize health tracking for the new peer.
        self.peer_health.lock().init_peer(addr);

        true
    }

    pub fn update_peer_identity(
        &self,
        old_addr: SocketAddr,
        new_addr: SocketAddr,
        new_pub_key: TransportPublicKey,
    ) -> bool {
        if old_addr == new_addr {
            tracing::debug!(
                addr = %old_addr,
                "update_peer_identity: same address; skipping"
            );
            return false;
        }

        let mut loc_for_peer = self.location_for_peer.write();
        let Some(loc) = loc_for_peer.remove(&old_addr) else {
            tracing::debug!(
                old_addr = %old_addr,
                new_addr = %new_addr,
                "update_peer_identity: old peer entry not found"
            );
            return false;
        };

        tracing::debug!(
            old_addr = %old_addr,
            new_addr = %new_addr,
            peer_location = %loc,
            "Updating peer identity for active connection"
        );
        loc_for_peer.insert(new_addr, loc);
        drop(loc_for_peer);

        // Hold the cbl write lock for the mutation (including the
        // placeholder-creation warning, emitted before the push it describes),
        // then drop it before acquiring connected_since / peer_health
        // (clippy: `significant_drop_tightening`).
        {
            let mut cbl = self.connections_by_location.write();
            let entry = cbl.entry(loc).or_default();
            if let Some(conn) = entry
                .iter_mut()
                .find(|conn| conn.location.socket_addr() == Some(old_addr))
            {
                // Update the public key and address to match the new peer
                conn.location.pub_key = new_pub_key;
                conn.location.set_addr(new_addr);
            } else {
                // Warn before push to preserve pre-#4129 ordering
                // (observational only, but the order was intentional).
                tracing::warn!(
                    old_addr = %old_addr,
                    peer_location = %loc,
                    "update_peer_identity: connection entry missing; creating placeholder"
                );
                entry.push(Connection::new(PeerKeyLocation::new(new_pub_key, new_addr)));
            }
        }

        // Migrate connected_since and peer_health to the new address.
        {
            let mut cs = self.connected_since.write();
            if let Some(since) = cs.remove(&old_addr) {
                cs.insert(new_addr, since);
            }
        }
        {
            let mut health = self.peer_health.lock();
            if let Some(stats) = health.stats.remove(&old_addr) {
                health.stats.insert(new_addr, stats);
            }
        }

        true
    }

    fn prune_connection(&self, addr: SocketAddr, is_alive: bool) -> Option<Location> {
        let connection_type = if is_alive { "active" } else { "in transit" };
        tracing::info!(
            addr = %addr,
            connection_type,
            "Pruning connection from ring topology"
        );

        let mut locations_for_peer = self.location_for_peer.write();

        let Some(loc) = locations_for_peer.remove(&addr) else {
            if is_alive {
                tracing::debug!("no location found for peer, skip pruning");
                return None;
            } else {
                let removed = self.pending_reservations.write().remove(&addr).is_some();
                if !removed {
                    tracing::warn!(
                        addr = %addr,
                        "prune_connection: no pending reservation to release for in-transit peer"
                    );
                }
            }
            return None;
        };

        let cbl = &mut *self.connections_by_location.write();
        if let Some(bucket) = cbl.get_mut(&loc) {
            if let Some(pos) = bucket
                .iter()
                .position(|c| c.location.socket_addr() == Some(addr))
            {
                bucket.swap_remove(pos);
                if bucket.is_empty() {
                    cbl.remove(&loc);
                }
            }
        }

        if !is_alive {
            self.pending_reservations.write().remove(&addr);
        }

        // Clean up readiness state for the pruned peer
        self.ready_peers.remove(&addr);

        // Clean up connection timestamp for optimistic readiness
        self.connected_since.write().remove(&addr);

        // Clean up health tracking for the pruned peer
        self.peer_health.lock().remove_peer(addr);

        Some(loc)
    }

    pub(crate) fn connection_count(&self) -> usize {
        // Count only established connections tracked by location buckets.
        self.connections_by_location
            .read()
            .values()
            .map(|conns| conns.len())
            .sum()
    }

    #[allow(dead_code)]
    pub(super) fn get_open_connections(&self) -> usize {
        self.connection_count()
    }

    #[allow(dead_code)]
    pub(crate) fn get_reserved_connections(&self) -> usize {
        self.pending_reservations.read().len()
    }

    /// Remove pending reservations that have exceeded `PENDING_RESERVATION_TTL`,
    /// and clean up orphaned `location_for_peer` entries that have no corresponding
    /// established connection or valid pending reservation.
    ///
    /// Orphaned entries arise when a CONNECT operation times out: `should_accept`
    /// inserts into both `pending_reservations` and `location_for_peer`, but
    /// abort handling may only clean the reservation, leaving a phantom location
    /// entry that permanently blocks gateway retries (see #3088).
    ///
    /// Returns the number of stale entries removed (reservations + orphaned locations).
    pub(crate) fn cleanup_stale_reservations(&self) -> usize {
        // Phase 1: Clean expired pending reservations and snapshot surviving addresses.
        // We release the pending_reservations lock before phase 2 to avoid deadlock:
        // prune_connection acquires location_for_peer(W) → pending_reservations(W),
        // so we must not hold pending_reservations while acquiring location_for_peer.
        let (stale_reservations, valid_pending_addrs) = {
            let now = Instant::now();
            let mut pending = self.pending_reservations.write();
            let before = pending.len();
            pending.retain(|addr, (_loc, created)| {
                let age = now.duration_since(*created);
                if age > PENDING_RESERVATION_TTL {
                    tracing::warn!(
                        addr = %addr,
                        age_secs = age.as_secs(),
                        "Removing stale pending reservation"
                    );
                    false
                } else {
                    true
                }
            });
            let stale = before - pending.len();
            let valid: Vec<SocketAddr> = pending.keys().copied().collect();
            (stale, valid)
        }; // pending_reservations lock released

        // Phase 2: Clean orphaned location_for_peer entries — addresses with no
        // established connection and no valid pending reservation. These phantoms
        // cause has_connection_or_pending() to permanently return true (#3088).
        //
        // Lock ordering matches prune_connection: location_for_peer(W) first,
        // then connections_by_location(R). No pending_reservations lock held.
        let orphaned_locations = {
            let mut locations = self.location_for_peer.write();
            let conns = self.connections_by_location.read();
            let before = locations.len();
            locations.retain(|addr, loc| {
                // Keep if there's an established connection at this location for this address
                if conns.get(loc).is_some_and(|conn_list| {
                    conn_list
                        .iter()
                        .any(|c| c.location.socket_addr() == Some(*addr))
                }) {
                    return true;
                }
                // Keep if there's a valid pending reservation (using phase 1 snapshot)
                if valid_pending_addrs.contains(addr) {
                    return true;
                }
                tracing::warn!(
                    addr = %addr,
                    location = %loc,
                    "Removing orphaned location_for_peer entry (no connection or reservation)"
                );
                false
            });
            before - locations.len()
        };

        stale_reservations + orphaned_locations
    }

    /// Remove transient connection entries that have exceeded `transient_ttl`.
    /// Returns the number of expired entries removed.
    ///
    /// Concurrency safety: `DashMap::retain` holds shard-level write locks during
    /// iteration. Concurrent `drop_transient`/`add_connection` calls on the same
    /// shard will block until `retain` releases the lock. If a concurrent call
    /// removes an entry before `retain` visits it, `retain` simply won't see it.
    /// If `retain` removes an entry first, the concurrent `remove()` returns `None`
    /// and skips its `fetch_sub`. Each entry is decremented exactly once.
    pub(crate) fn cleanup_expired_transients(&self) -> usize {
        let now = Instant::now();
        let ttl = self.transient_ttl;
        let mut removed = 0;
        self.transient_connections.retain(|addr, entry| {
            let age = now.duration_since(entry.created_at);
            if age > ttl {
                tracing::debug!(
                    addr = %addr,
                    age_secs = age.as_secs(),
                    "Removing expired transient connection"
                );
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            self.transient_in_use.fetch_sub(removed, Ordering::SeqCst);
        }
        removed
    }

    /// Check whether a peer address has an established connection in `connections_by_location`.
    /// Unlike `has_connection_or_pending`, this checks only fully established ring connections.
    pub fn is_in_ring(&self, addr: SocketAddr) -> bool {
        let connections = self.connections_by_location.read();
        connections
            .values()
            .any(|conns| conns.iter().any(|c| c.location.socket_addr() == Some(addr)))
    }

    pub fn has_connection_or_pending(&self, addr: SocketAddr) -> bool {
        // Drop the location_for_peer read-lock before acquiring connections_by_location
        // to avoid holding two read-locks across the dependent lookup (clippy:
        // `significant_drop_in_scrutinee`). The two structures are independently
        // synchronized; nothing relies on observing them under a single lock.
        let loc = self.location_for_peer.read().get(&addr).copied();
        if let Some(loc) = loc {
            // Verify the location_for_peer entry has a backing established connection.
            // Without this check, stale entries from failed connect operations block the
            // bootstrap loop from retrying gateways (#3244).
            let has_established = self
                .connections_by_location
                .read()
                .get(&loc)
                .is_some_and(|conns| conns.iter().any(|c| c.location.socket_addr() == Some(addr)));
            if has_established {
                return true;
            }
            // No established connection — fall through to check pending reservations.
            // The location_for_peer entry is orphaned and will be cleaned up by
            // cleanup_stale_reservations().
        }
        let pending = self.pending_reservations.read();
        if let Some((_loc, created)) = pending.get(&addr) {
            return Instant::now().duration_since(*created) <= PENDING_RESERVATION_TTL;
        }
        false
    }

    pub(crate) fn inject_reservation(
        &self,
        addr: SocketAddr,
        location: Location,
        created: Instant,
    ) {
        self.pending_reservations
            .write()
            .insert(addr, (location, created));
    }

    pub(crate) fn get_connections_by_location(&self) -> BTreeMap<Location, Vec<Connection>> {
        self.connections_by_location.read().clone()
    }

    /// Route an op to the most optimal target.
    /// Note: this applies readiness gating (`check_readiness=true`). For CONNECT
    /// operations that need to bypass readiness, use `routing_candidates` directly.
    #[cfg(test)]
    pub fn routing(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
        router: &Router,
    ) -> Option<PeerKeyLocation> {
        let (peer, _decision) = self.routing_with_telemetry(target, requesting, skip_list, router);
        peer
    }

    /// Route an op to the most optimal target, returning telemetry about the decision.
    pub fn routing_with_telemetry(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
        router: &Router,
    ) -> (
        Option<PeerKeyLocation>,
        Option<crate::router::RoutingDecisionInfo>,
    ) {
        let candidates = self.routing_candidates(target, requesting, skip_list, true);

        if candidates.is_empty() {
            return (None, None);
        }

        let (selected, decision) =
            router.select_k_best_peers_with_telemetry(candidates.iter(), target, 1);
        let peer = selected.into_iter().next().cloned();
        (peer, Some(decision))
    }

    /// Gather routing candidates after applying skip/transient filters.
    /// When `check_readiness` is true, peers that haven't advertised readiness are filtered out.
    /// CONNECT operations pass `false` to allow routing to not-yet-ready peers.
    pub fn routing_candidates(
        &self,
        target: Location,
        requesting: Option<SocketAddr>,
        skip_list: impl Contains<SocketAddr>,
        check_readiness: bool,
    ) -> Vec<PeerKeyLocation> {
        let connections = self.connections_by_location.read();
        // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
        // This ensures GlobalRng is called in the same order across runs
        let mut sorted_keys: Vec<_> = connections.keys().collect();
        sorted_keys.sort();
        let mut candidates: Vec<PeerKeyLocation> = Vec::new();
        let mut not_ready_fallback: Vec<PeerKeyLocation> = Vec::new();

        for loc in sorted_keys {
            let conns = match connections.get(loc) {
                Some(c) => c,
                None => continue,
            };
            // Sort connections for deterministic selection
            // (Vec ordering may vary based on async connection establishment order)
            let mut sorted_conns: Vec<_> = conns.iter().collect();
            sorted_conns.sort_by_key(|c| c.location.clone());
            let conn = match GlobalRng::choose(&sorted_conns) {
                Some(c) => c,
                None => continue,
            };
            let addr = match conn.location.socket_addr() {
                Some(a) => a,
                None => continue,
            };
            if self.is_transient(addr) {
                continue;
            }
            if let Some(requester) = requesting {
                if requester == addr {
                    continue;
                }
            }
            if skip_list.has_element(addr) {
                continue;
            }
            // Skip peers that haven't advertised readiness (unless bypassed for CONNECT),
            // but collect them as fallback in case all peers fail the readiness check.
            if check_readiness && !self.is_peer_ready(addr) {
                not_ready_fallback.push(conn.location.clone());
                continue;
            }
            candidates.push(conn.location.clone());
        }

        // If all connected peers failed the readiness check, fall back to using them anyway.
        // Same rationale as k_closest_potentially_hosting: routing to not-ready peers is
        // better than returning empty (which causes EmptyRing / no routing target for
        // PUT, UPDATE, and other operations that use this path).
        if check_readiness && candidates.is_empty() && !not_ready_fallback.is_empty() {
            tracing::warn!(
                count = not_ready_fallback.len(),
                target_location = %target,
                "routing_candidates: no ready peers, falling back to not-yet-ready peers"
            );
            candidates = not_ready_fallback;
        }

        tracing::debug!(
            total_locations = connections.len(),
            candidates = candidates.len(),
            target_location = %target,
            self_addr = self
                .get_own_addr()
                .as_ref()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "unknown".into()),
            "routing candidates for next hop (non-transient only)"
        );

        candidates
    }

    pub fn num_connections(&self) -> usize {
        let connections = self.connections_by_location.read();
        let total: usize = connections.values().map(|v| v.len()).sum();
        tracing::debug!(
            unique_locations = connections.len(),
            total_connections = total,
            "num_connections called"
        );
        total
    }

    /// Record that a transport-level connection to `addr` failed, so future
    /// CONNECT requests will mark it as visited in the bloom filter.
    /// Repeated failures increment the count, scaling the TTL exponentially.
    /// Jitter is applied once here at write time, so back-to-back reads always
    /// agree on whether the address is still blocked.
    pub fn record_failed_addr(&self, addr: SocketAddr) {
        self.recently_failed_addrs.lock().record_failure(addr);
    }

    /// Remove `addr` from the failed cache (e.g., after a successful connection).
    pub fn clear_failed_addr(&self, addr: SocketAddr) {
        self.recently_failed_addrs.lock().record_success(&addr);
    }

    /// Return addresses of all peers tracked in `location_for_peer` (established and pending).
    ///
    /// `location_for_peer` contains both fully-established ring connections and entries added
    /// during `should_accept()` for in-progress handshakes. Returning both is intentional:
    /// excluding pending peers from the bloom filter is conservative but correct — we don't
    /// want the connect state machine to route a new request to a peer whose handshake is
    /// still in flight, since that would create a duplicate connection attempt.
    pub fn connected_peer_addrs(&self) -> Vec<SocketAddr> {
        self.location_for_peer.read().keys().copied().collect()
    }

    /// Return addresses that failed NAT traversal within their adaptive TTL.
    /// Addresses with more repeated failures have longer TTLs (up to 1 hour).
    /// Jitter was applied once at record time, so the result is stable across
    /// consecutive calls within the same millisecond.
    pub fn recently_failed_addrs(&self) -> Vec<SocketAddr> {
        self.recently_failed_addrs.lock().keys_in_backoff()
    }

    /// Remove entries whose adaptive TTL has expired. Returns the number removed.
    pub fn cleanup_stale_failed_addrs(&self) -> usize {
        self.recently_failed_addrs.lock().remove_expired_entries()
    }

    /// Clear all recently-failed addresses. Used after suspend/resume when
    /// previously-unreachable peers may be reachable again.
    pub fn cleanup_all_failed_addrs(&self) {
        self.recently_failed_addrs.lock().clear();
    }

    // ==================== CONNECT Jitter ====================

    /// How long between automatic decay steps for the jitter failure counter.
    ///
    /// If this much time has elapsed since the last failure, the counter is reduced
    /// by one step per elapsed interval before the new failure is recorded.  This
    /// prevents failure counts accumulated during one burst of connection attempts
    /// from artificially inflating jitter on a reconnection attempt that happens
    /// much later.
    const JITTER_DECAY_INTERVAL: Duration = Duration::from_secs(5 * 60);

    /// Increment consecutive connect failure counter and return the new count.
    ///
    /// Before incrementing, applies time-based decay: for every full
    /// `JITTER_DECAY_INTERVAL` that has elapsed since the last failure, the
    /// counter is decremented by one (down to 0).
    ///
    /// `now` should be captured once by the caller and shared with other time-sensitive
    /// calls in the same context for a consistent timestamp.
    pub fn increment_connect_jitter_failures(&self, now: Instant) -> u32 {
        let mut guard = self.connect_jitter_failures.lock();
        let (count, last_failure) = &mut *guard;
        // Apply decay proportional to elapsed idle time before recording the new failure.
        // Integer division is intentional: decay only triggers after a complete 5-minute
        // interval (floor rounding). Sub-interval gaps accumulate no decay steps.
        if let Some(ts) = *last_failure {
            let elapsed = now.duration_since(ts);
            // JITTER_DECAY_INTERVAL is a non-zero Duration::from_secs constant, so
            // as_secs() is always > 0 and division is safe without a .max(1) guard.
            let decay_steps = (elapsed.as_secs() / Self::JITTER_DECAY_INTERVAL.as_secs()) as u32;
            *count = count.saturating_sub(decay_steps);
        }
        *count += 1;
        *last_failure = Some(now);
        *count
    }

    /// Reset consecutive connect failure counter (called on successful connect).
    pub fn reset_connect_jitter_failures(&self) {
        let mut guard = self.connect_jitter_failures.lock();
        *guard = (0, None);
    }

    /// Current consecutive connect failure count (without applying decay).
    #[cfg(test)]
    pub fn connect_jitter_failure_count(&self) -> u32 {
        self.connect_jitter_failures.lock().0
    }

    // ==================== CONNECT Acceptor Reliability ====================

    /// How long acceptor reliability stats are retained before expiring.
    /// After this period the peer reverts to the unknown-reliability prior (0.5).
    const ACCEPTOR_STATS_TTL: Duration = Duration::from_secs(30 * 60);

    /// Record the outcome of a CONNECT attempt where `addr` was the acceptor.
    /// Increments the attempt count and, if `success`, the success count.
    pub fn record_acceptor_outcome(&self, addr: SocketAddr, success: bool, now: Instant) {
        let mut map = self.acceptor_reliability.write();
        let entry = map.entry(addr).or_insert(AcceptorStats {
            successes: 0,
            attempts: 0,
            last_updated: now,
        });
        entry.attempts = entry.attempts.saturating_add(1);
        if success {
            entry.successes = entry.successes.saturating_add(1);
        }
        entry.last_updated = now;
    }

    /// Return the estimated reliability of `addr` as a CONNECT acceptor.
    ///
    /// Returns a value in `[0.0, 1.0]`:
    /// - Peers with no history (or expired history): **0.5** (unknown prior)
    /// - Peers with history: Bayesian estimate via Laplace smoothing
    ///   `(successes + 1) / (attempts + 2)`, starting at 0.5 for zero
    ///   observations and converging to the true rate.
    pub fn peer_acceptor_reliability(&self, addr: SocketAddr, now: Instant) -> f64 {
        let map = self.acceptor_reliability.read();
        match map.get(&addr) {
            Some(stats) if now.duration_since(stats.last_updated) < Self::ACCEPTOR_STATS_TTL => {
                (stats.successes as f64 + 1.0) / (stats.attempts as f64 + 2.0)
            }
            _ => 0.5, // unknown prior
        }
    }

    /// Remove acceptor reliability entries older than `ACCEPTOR_STATS_TTL`.
    /// Called from `connection_maintenance` on each tick.
    pub fn cleanup_expired_acceptor_stats(&self, now: Instant) {
        self.acceptor_reliability
            .write()
            .retain(|_, stats| now.duration_since(stats.last_updated) < Self::ACCEPTOR_STATS_TTL);
    }

    #[allow(dead_code)]
    pub(super) fn connected_peers(&self) -> impl Iterator<Item = SocketAddr> {
        let read = self.location_for_peer.read();
        read.keys().copied().collect::<Vec<_>>().into_iter()
    }

    // ==================== Peer Readiness ====================

    /// Mark a peer as ready to accept non-CONNECT operations.
    pub fn mark_peer_ready(&self, addr: SocketAddr) {
        self.ready_peers.insert(addr);
    }

    /// Mark a peer as not ready (e.g., dropped below threshold).
    pub fn mark_peer_not_ready(&self, addr: SocketAddr) {
        self.ready_peers.remove(&addr);
    }

    /// Duration after which a connected peer is optimistically treated as ready,
    /// even if no ReadyState message was received (handles lost messages).
    const OPTIMISTIC_READY_TIMEOUT: Duration = Duration::from_secs(60);

    /// Check if a peer has advertised readiness.
    /// When `min_ready_connections == 0`, all peers are treated as ready.
    /// Falls back to optimistic readiness if peer has been connected longer than
    /// `OPTIMISTIC_READY_TIMEOUT` (handles lost ReadyState messages).
    pub fn is_peer_ready(&self, addr: SocketAddr) -> bool {
        if self.min_ready_connections == 0 {
            return true;
        }
        if self.ready_peers.contains(&addr) {
            return true;
        }
        // Optimistic timeout: treat long-connected peers as ready even without
        // an explicit ReadyState message (covers lost/delayed messages).
        if let Some(since) = self.connected_since.read().get(&addr) {
            if since.elapsed() >= Self::OPTIMISTIC_READY_TIMEOUT {
                return true;
            }
        }
        false
    }

    /// Check if *this* node has crossed the readiness threshold.
    pub fn is_self_ready(&self) -> bool {
        self.min_ready_connections == 0 || self.connection_count() >= self.min_ready_connections
    }

    /// Create a minimal ConnectionManager for unit tests.
    #[cfg(test)]
    pub(crate) fn test_default() -> Self {
        use crate::topology::rate::Rate;
        let keypair = crate::transport::TransportKeypair::new();
        let own_location = AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()));
        Self::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            4,
            10,
            7,
            (keypair.public().clone(), None, own_location),
            false,
            10,
            Duration::from_secs(60),
            0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;
    use crate::topology::rate::Rate;
    use crate::transport::TransportKeypair;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    fn make_connection_manager(
        own_addr: Option<SocketAddr>,
        min_conn: usize,
        max_conn: usize,
        is_gateway: bool,
    ) -> ConnectionManager {
        let keypair = TransportKeypair::new();
        let own_location = if let Some(addr) = own_addr {
            AtomicU64::new(u64::from_le_bytes(
                Location::from_address(&addr).as_f64().to_le_bytes(),
            ))
        } else {
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

        ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            min_conn,
            max_conn,
            7,
            (keypair.public().clone(), own_addr, own_location),
            is_gateway,
            10,
            Duration::from_secs(60),
            0, // readiness gating disabled in tests
        )
    }

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn age_reservation(cm: &ConnectionManager, addr: SocketAddr, age: Duration) {
        let mut pending = cm.pending_reservations.write();
        if let Some(entry) = pending.get_mut(&addr) {
            entry.1 = Instant::now() - age;
        }
    }

    fn age_transient(cm: &ConnectionManager, addr: SocketAddr, age: Duration) {
        if let Some(mut entry) = cm.transient_connections.get_mut(&addr) {
            entry.created_at = Instant::now() - age;
        }
    }

    // ============ summary-first PUT version-gate tests (#4642 step 3-bis) ============

    /// `record_remote_version` / `remote_version` round-trip: an address
    /// with no recorded connection returns `None`, and a recorded version
    /// is returned verbatim.
    #[test]
    fn record_remote_version_then_lookup_roundtrips() {
        let cm = make_connection_manager(Some(make_addr(9000)), 1, 10, false);
        let addr = make_addr(9001);
        assert_eq!(cm.remote_version(addr), None);
        cm.record_remote_version(addr, (0, 2, 94));
        assert_eq!(cm.remote_version(addr), Some((0, 2, 94)));
    }

    /// Emission gate, fail-closed case: a peer with no recorded version
    /// (e.g. never connected, or connected pre-handshake) must not be
    /// treated as summary-first-capable — mirrors the mixed-version
    /// interop guarantee the gate's own unit tests assert in isolation
    /// (`p2p_protoc::tests::summary_first_unknown_remote_version_fails_closed`),
    /// exercised here through the driver-reachable `ConnectionManager`
    /// wrapper instead of the pure predicate directly.
    #[test]
    fn supports_summary_first_put_fails_closed_for_unknown_peer() {
        let cm = make_connection_manager(Some(make_addr(9000)), 1, 10, false);
        let addr = make_addr(9002);
        assert!(!cm.supports_summary_first_put(addr));
    }

    /// Emission gate, known-version case against the real production floor
    /// (no test override set): a pre-floor peer is rejected, an
    /// at-or-above-floor peer is accepted. This is the decision
    /// `try_summary_first_put` consults before ever emitting a
    /// `PutMsg::ProbeRequest` — a pre-floor peer must never receive one
    /// (it cannot bincode-deserialize the appended tag).
    #[test]
    fn supports_summary_first_put_respects_recorded_version_against_production_floor() {
        let cm = make_connection_manager(Some(make_addr(9000)), 1, 10, false);

        let pre_floor_addr = make_addr(9003);
        cm.record_remote_version(pre_floor_addr, (0, 2, 80));
        assert!(!cm.supports_summary_first_put(pre_floor_addr));

        // The exact staggered-rollout boundary (#4642 step 3-bis): the probe
        // wire variants (PutMsg tags 6/7/8) first ship in 0.2.95 (this PR).
        // 0.2.94 is the HIGHEST already-released version and carries NONE of
        // them, so a 0.2.94 peer cannot even decode a probe — the decode
        // failure would drop the connection. A summary-first-capable emitter
        // must therefore fall back to a full-state PUT and NEVER send it a
        // probe. The floor being strictly greater than 0.2.94 enforces this.
        let pre_variant_peer = make_addr(9005);
        cm.record_remote_version(pre_variant_peer, (0, 2, 94));
        assert!(
            !cm.supports_summary_first_put(pre_variant_peer),
            "a 0.2.94 peer has no probe variants; a probe would fail to decode \
             and drop the connection, so the emitter must fall back to a \
             full-state PUT"
        );

        // A peer at the floor (0.2.95, the first release carrying the probe
        // variants + handler) is accepted.
        let at_floor_addr = make_addr(9004);
        cm.record_remote_version(at_floor_addr, crate::node::SUMMARY_FIRST_PUT_MIN_VERSION);
        assert!(cm.supports_summary_first_put(at_floor_addr));
    }

    // ============ cleanup_expired_transients tests ============

    #[test]
    fn test_cleanup_expired_transients_spares_fresh() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, true);
        assert!(cm.try_register_transient(make_addr(8001), None));
        assert_eq!(cm.cleanup_expired_transients(), 0);
        assert_eq!(cm.transient_count(), 1);
    }

    #[test]
    fn test_cleanup_expired_transients_removes_old() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, true);
        assert!(cm.try_register_transient(make_addr(8001), None));
        // Age past the 60s TTL used in tests
        age_transient(&cm, make_addr(8001), Duration::from_secs(61));
        assert_eq!(cm.cleanup_expired_transients(), 1);
        assert_eq!(cm.transient_count(), 0);
    }

    #[test]
    fn test_cleanup_expired_transients_mixed() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, true);
        assert!(cm.try_register_transient(make_addr(8001), None));
        assert!(cm.try_register_transient(make_addr(8002), None));
        // Only age the first one
        age_transient(&cm, make_addr(8001), Duration::from_secs(61));
        assert_eq!(cm.cleanup_expired_transients(), 1);
        assert_eq!(cm.transient_count(), 1);
        assert!(cm.is_transient(make_addr(8002)));
        assert!(!cm.is_transient(make_addr(8001)));
    }

    // ============ Basic ConnectionManager tests ============

    #[test]
    fn test_connection_manager_initial_state() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        assert_eq!(cm.connection_count(), 0);
        assert_eq!(cm.get_own_addr(), Some(make_addr(8000)));
        assert!(!cm.is_gateway());
        assert_eq!(cm.min_connections, 5);
        assert_eq!(cm.max_connections, 20);
    }

    #[test]
    fn test_connection_manager_gateway_mode() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);
        assert!(cm.is_gateway());
    }

    /// Test that should_accept rejects connections from own address
    ///
    /// **Bug scenario prevented (#1806, #1786, #1781, #1827):**
    /// A node must never accept a connection from itself. This can happen when:
    /// - A node's external address is incorrectly resolved to itself
    /// - NAT reflection causes packets to loop back
    /// - Gateway advertises its own address to peers
    ///
    /// Self-connections cause infinite routing loops and state corruption.
    /// This is the PRIMARY defense layer (runs in both debug and release).
    #[test]
    fn rejects_self_connection() {
        let keypair = TransportKeypair::new();
        let addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
            0,
        );

        assert_eq!(cm.get_own_addr(), Some(addr));
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, addr);
        assert!(!accepted, "should_accept must reject self-connection");
    }

    #[test]
    fn accepts_connection_from_different_peer() {
        let own_keypair = TransportKeypair::new();
        let own_addr: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                own_keypair.public().clone(),
                Some(own_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
            0,
        );

        let other_addr: SocketAddr = "127.0.0.2:8001".parse().unwrap();
        let location = Location::new(0.6);
        let accepted = cm.should_accept(location, other_addr);
        assert!(
            accepted,
            "should_accept must accept connection from different peer"
        );
    }

    // ============ should_accept tests ============

    #[test]
    fn test_should_accept_below_min_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        // First connection should always be accepted
        let addr1 = make_addr(8001);
        let loc1 = Location::new(0.1);
        assert!(cm.should_accept(loc1, addr1));

        // Below min connections, should accept more
        let addr2 = make_addr(8002);
        let loc2 = Location::new(0.2);
        assert!(cm.should_accept(loc2, addr2));
    }

    /// Test the sliding admission floor: at low connection counts the floor is ~0.9
    /// (accept almost anything), and as open approaches min_connections the floor
    /// drops to ~0.3 (be selective). Uses seeded RNG for deterministic behavior.
    #[test]
    fn test_sliding_admission_floor() {
        // min_connections=10, max=20, so range = 10 - 3 = 7
        let cm = make_connection_manager(Some(make_addr(8000)), 10, 20, false);
        let keypair = TransportKeypair::new();

        // Add KLEINBERG_FILTER_MIN_CONNECTIONS (3) connections to enter the sliding floor zone
        for i in 0..KLEINBERG_FILTER_MIN_CONNECTIONS {
            let addr = make_addr(8001 + i as u16);
            let loc = Location::new(0.05 * (i + 1) as f64);
            cm.add_connection(loc, addr, keypair.public().clone(), true);
        }
        assert_eq!(cm.connection_count(), 3);

        // At open=3, progress=0.0, floor=0.9 — should accept almost all candidates
        // Test with 100 trials using a seed that produces values in [0, 1)
        let _guard = GlobalRng::seed_guard(42);
        let mut accepted_at_3 = 0;
        for i in 0..100u16 {
            let addr = make_addr(9000 + i);
            // Location far from existing peers → low gap score
            let loc = Location::new(0.99);
            if cm.should_accept(loc, addr) {
                accepted_at_3 += 1;
                // Clean up the pending reservation so it doesn't affect the next trial
                cm.pending_reservations.write().remove(&addr);
            }
        }

        // Now add more connections to get open=9 (close to min_connections=10)
        for i in 3..9 {
            let addr = make_addr(8001 + i as u16);
            let loc = Location::new(0.05 * (i + 1) as f64);
            cm.add_connection(loc, addr, keypair.public().clone(), true);
        }
        assert_eq!(cm.connection_count(), 9);

        // At open=9, progress=6/7≈0.857, floor≈0.386 — should be much more selective
        let _guard2 = GlobalRng::seed_guard(42); // same seed for fair comparison
        let mut accepted_at_9 = 0;
        for i in 0..100u16 {
            let addr = make_addr(10000 + i);
            let loc = Location::new(0.99);
            if cm.should_accept(loc, addr) {
                accepted_at_9 += 1;
                cm.pending_reservations.write().remove(&addr);
            }
        }

        // With floor=0.9, acceptance rate should be ~90%
        // With floor≈0.386, acceptance rate should be ~39%
        // Allow generous margins since gap_score adds some, but the trend must be clear
        assert!(
            accepted_at_3 > accepted_at_9,
            "acceptance rate should decrease as connections approach min_connections: \
             accepted_at_3={accepted_at_3} should be > accepted_at_9={accepted_at_9}"
        );
        assert!(
            accepted_at_3 >= 70,
            "at open=3 (floor=0.9), acceptance rate should be high: got {accepted_at_3}/100"
        );
        assert!(
            accepted_at_9 <= 70,
            "at open=9 (floor≈0.39), acceptance rate should be lower: got {accepted_at_9}/100"
        );
    }

    /// Edge case: min_connections == KLEINBERG_FILTER_MIN_CONNECTIONS (range=0).
    /// Should not panic and should use floor=0.3 (most selective).
    #[test]
    fn test_sliding_floor_degenerate_range() {
        // min_connections=3 == KLEINBERG_FILTER_MIN_CONNECTIONS, range=0
        let cm = make_connection_manager(Some(make_addr(8000)), 3, 10, false);
        let keypair = TransportKeypair::new();

        // Add 3 connections to reach min_connections — but should_accept's outer guard
        // checks open < min_connections, so at open=3 we hit the ConnectionEvaluator
        // path instead. Test that we don't panic with 2 connections (open < min=3,
        // but open < KLEINBERG_FILTER_MIN_CONNECTIONS=3 so unconditional accept).
        for i in 0..2 {
            let addr = make_addr(8001 + i as u16);
            let loc = Location::new(0.1 * (i + 1) as f64);
            cm.add_connection(loc, addr, keypair.public().clone(), true);
        }

        // With open=2 < KLEINBERG_FILTER_MIN_CONNECTIONS=3, should unconditionally accept
        let test_addr = make_addr(9001);
        assert!(cm.should_accept(Location::new(0.5), test_addr));
    }

    #[test]
    fn test_should_accept_at_max_connections() {
        // The should_accept logic calculates total_conn = reserved_before + 1 + open
        // It rejects when total_conn >= max_connections
        // So with max=4 and 3 open connections: total_conn = 0 + 1 + 3 = 4 >= 4, rejected
        // Therefore we can only have max_connections - 1 open before the next is rejected
        //
        // Mechanism 3 (nearest-neighbor lattice) force-accepts a strictly-closer
        // per-side nearest even over max (here the 4th candidate is a new per-side
        // nearest given the derived own location), so disable the lattice to
        // isolate the pure cap arithmetic. The over-max lattice admission is
        // exercised by add_connection_admits_lattice_edge_over_max,
        // admits_lattice_edge_over_cap_predicate, and
        // lifecycle_gates_apply_lattice_over_cap_predicate.
        set_nn_lattice_enabled(false);
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 4, false);
        let keypair = TransportKeypair::new();

        // Accept and add first connection (open=1, next attempt: 0+1+1=2 < 4)
        let addr1 = make_addr(8001);
        let loc1 = Location::new(0.1);
        assert!(cm.should_accept(loc1, addr1));
        cm.add_connection(loc1, addr1, keypair.public().clone(), true);

        // Accept and add second connection (open=2, next attempt: 0+1+2=3 < 4)
        let addr2 = make_addr(8002);
        let loc2 = Location::new(0.2);
        assert!(cm.should_accept(loc2, addr2));
        cm.add_connection(loc2, addr2, keypair.public().clone(), true);

        // Accept and add third connection (open=3, next attempt: 0+1+3=4 >= 4)
        let addr3 = make_addr(8003);
        let loc3 = Location::new(0.3);
        assert!(cm.should_accept(loc3, addr3));
        cm.add_connection(loc3, addr3, keypair.public().clone(), true);

        // Fourth connection should be rejected (open=3, total_conn=4 >= max=4)
        let addr4 = make_addr(8004);
        let loc4 = Location::new(0.4);
        assert!(!cm.should_accept(loc4, addr4));
        set_nn_lattice_enabled(true);
    }

    #[test]
    fn test_should_accept_already_connected_peer() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Accept and add connection
        assert!(cm.should_accept(loc, addr));
        cm.add_connection(loc, addr, keypair.public().clone(), true);

        // Same peer should return false — reject to route uphill for diversity
        assert!(!cm.should_accept(loc, addr));
    }

    // ============ add_connection / prune_connection tests ============

    #[test]
    fn test_add_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        assert_eq!(cm.connection_count(), 0);
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);
    }

    #[test]
    fn test_prune_alive_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);

        let pruned_loc = cm.prune_alive_connection(addr);
        assert_eq!(pruned_loc, Some(loc));
        assert_eq!(cm.connection_count(), 0);
    }

    /// Mechanism 4 liveness bound: the retention exemption lives ONLY in the
    /// score-based topology paths (topology.rs). The liveness prune STILL evicts
    /// a lattice edge — the exemption is never a permanent pin to a dead neighbor.
    #[test]
    fn prune_evicts_lattice_edge_liveness_not_gated() {
        set_nn_lattice_enabled(true);
        // own_addr None so the location starts unset; update_location then pins
        // it to 0.5 (update_location is a no-op once a location is already set).
        let cm = make_connection_manager(None, 2, 10, false);
        cm.update_location(Some(Location::new(0.5)));
        assert_eq!(cm.get_stored_location(), Some(Location::new(0.5)));
        let keypair = TransportKeypair::new();
        let addr = make_addr(8101);
        // A nearest-successor lattice edge.
        cm.add_connection(Location::new(0.52), addr, keypair.public().clone(), false);
        assert!(
            cm.nearest_lattice_neighbor_dist(true).is_some(),
            "the added edge is the current nearest successor (a lattice edge)"
        );
        // Liveness prune removes it despite being a lattice edge.
        let pruned = cm.prune_alive_connection(addr);
        assert!(
            pruned.is_some(),
            "liveness prune must evict even a lattice edge"
        );
        assert_eq!(cm.connection_count(), 0);
        assert!(
            cm.nearest_lattice_neighbor_dist(true).is_none(),
            "the successor slot is now empty (re-discovery will re-fill it)"
        );
    }

    /// Mechanism 3 over-cap admission is FILL-ONLY: a candidate that FILLS an
    /// empty per-side lattice slot (a genuine routing dead-end) is admitted over
    /// max (displacing a long link), but a mere TIGHTEN of an already-filled side
    /// is NOT — it waits for an under-max slot rather than stripping a long link.
    /// The absolute ceiling still bounds the (fill-only) over-max excess.
    #[test]
    fn add_connection_admits_lattice_fill_over_max_not_tighten() {
        set_nn_lattice_enabled(true);
        let max = 3usize;
        let cm = make_connection_manager(None, 2, max, false);
        cm.update_location(Some(Location::new(0.5)));
        assert_eq!(cm.get_stored_location(), Some(Location::new(0.5)));
        let kp = TransportKeypair::new();
        // Fill the SUCCESSOR side to max; the PREDECESSOR side stays empty.
        for (i, l) in [0.60_f64, 0.70, 0.80].into_iter().enumerate() {
            cm.add_connection(
                Location::new(l),
                make_addr(8210 + i as u16),
                kp.public().clone(),
                false,
            );
        }
        assert_eq!(
            cm.connection_count(),
            max,
            "filled to max on the successor side"
        );
        // A strictly-closer SUCCESSOR is a TIGHTEN of an already-filled side ->
        // NOT admitted over max (would strip a long link for marginal gain).
        let tighten = cm.add_connection(
            Location::new(0.55),
            make_addr(8220),
            kp.public().clone(),
            false,
        );
        assert!(
            !tighten,
            "a tighten of a filled side must be rejected over max"
        );
        assert_eq!(cm.connection_count(), max);
        // A PREDECESSOR candidate FILLS the empty predecessor side (a dead-end) ->
        // admitted over max, displacing a long link.
        let fill = cm.add_connection(
            Location::new(0.45),
            make_addr(8230),
            kp.public().clone(),
            false,
        );
        assert!(
            fill,
            "filling an empty lattice side must be admitted over max"
        );
        assert_eq!(
            cm.connection_count(),
            max + 1,
            "over max by 1 until the over-max prune tick restores it"
        );
        // Both sides are now filled: a farther non-lattice peer AND a further
        // tighten of the predecessor side are both rejected over max, so the count
        // stays bounded (no further fills are possible — only two sides exist).
        let farther = cm.add_connection(
            Location::new(0.85),
            make_addr(8240),
            kp.public().clone(),
            false,
        );
        assert!(!farther, "a farther non-lattice peer is rejected over max");
        let tighten_pred = cm.add_connection(
            Location::new(0.48),
            make_addr(8250),
            kp.public().clone(),
            false,
        );
        assert!(
            !tighten_pred,
            "a tighten of the now-filled predecessor side is rejected over max"
        );
        assert_eq!(cm.connection_count(), max + 1, "count stays bounded");
    }

    /// The SINGLE over-cap admission predicate `admits_lattice_edge_over_cap` is
    /// FILL-ONLY and is the exact decision both connection-lifecycle promotion
    /// gates apply on the real CONNECT path (see the source-scrape pin below).
    /// Filling an empty side is admissible over cap; a tighten of a filled side
    /// and a farther non-lattice peer are not.
    #[test]
    fn admits_lattice_edge_over_cap_predicate() {
        set_nn_lattice_enabled(true);
        let max = 3usize;
        let cm = make_connection_manager(None, 2, max, false);
        cm.update_location(Some(Location::new(0.5)));
        let kp = TransportKeypair::new();
        // Successor side filled to the cap; predecessor side empty.
        for (i, l) in [0.60_f64, 0.70, 0.80].into_iter().enumerate() {
            cm.add_connection(
                Location::new(l),
                make_addr(8310 + i as u16),
                kp.public().clone(),
                false,
            );
        }
        assert_eq!(cm.connection_count(), max, "at the cap");
        // Filling the EMPTY predecessor side is admissible over the cap. No
        // competing reservation (`None`), so the reservation-aware bound is a
        // no-op here.
        assert!(
            cm.admits_lattice_edge_over_cap(Location::new(0.45), None),
            "filling an empty lattice side is admissible over the cap"
        );
        assert!(
            cm.would_fill_empty_lattice_side(Location::new(0.45)),
            "the predecessor side is empty"
        );
        // A TIGHTEN of the already-filled successor side is NOT admissible over
        // the cap (it waits for an under-max slot).
        assert!(
            !cm.admits_lattice_edge_over_cap(Location::new(0.55), None),
            "a tighten of a filled side is not admissible over the cap"
        );
        assert!(
            !cm.would_fill_empty_lattice_side(Location::new(0.55)),
            "the successor side is not empty"
        );
        // A farther non-lattice peer is not admissible over the cap.
        assert!(
            !cm.admits_lattice_edge_over_cap(Location::new(0.95), None),
            "a farther non-lattice peer is not admissible over the cap"
        );
        // Disabling the lattice makes the predicate a no-op.
        set_nn_lattice_enabled(false);
        assert!(!cm.admits_lattice_edge_over_cap(Location::new(0.45), None));
        set_nn_lattice_enabled(true);
    }

    /// F1 regression: the over-cap lattice admission is RESERVATION-AWARE, so a
    /// full node with an empty lattice side cannot force-accept EVERY concurrent
    /// candidate on that side — a pending reservation already targeting the side
    /// blocks a second concurrent over-cap fill (bounding concurrent fills to one
    /// per side). `exclude_addr` skips the candidate's own reservation.
    #[test]
    fn admits_lattice_over_cap_is_reservation_aware() {
        set_nn_lattice_enabled(true);
        let max = 3usize;
        let cm = make_connection_manager(None, 2, max, false);
        cm.update_location(Some(Location::new(0.5)));
        let kp = TransportKeypair::new();
        // Successor side filled to the cap; predecessor side empty.
        for (i, l) in [0.60_f64, 0.70, 0.80].into_iter().enumerate() {
            cm.add_connection(
                Location::new(l),
                make_addr(8410 + i as u16),
                kp.public().clone(),
                false,
            );
        }
        assert_eq!(cm.connection_count(), max, "at the cap");
        let pred = Location::new(0.45);
        // With no competing reservation, filling the empty predecessor side is
        // admissible over the cap.
        assert!(
            cm.admits_lattice_edge_over_cap(pred, None),
            "a fill with no competing reservation is admissible"
        );
        // A concurrent pending reservation ALREADY targeting the predecessor side
        // blocks a second over-cap fill on that side.
        let other = make_addr(8420);
        cm.pending_reservations
            .write()
            .insert(other, (Location::new(0.47), Instant::now()));
        assert!(
            !cm.admits_lattice_edge_over_cap(pred, None),
            "a competing predecessor-side reservation blocks a concurrent fill"
        );
        // Excluding that reservation (as when it IS the candidate itself)
        // re-admits — self must not block itself.
        assert!(
            cm.admits_lattice_edge_over_cap(pred, Some(other)),
            "excluding the candidate's own reservation re-admits the fill"
        );
        // A reservation on the OTHER (successor) side does not block a predecessor
        // fill — the bound is strictly per-side.
        cm.pending_reservations.write().clear();
        cm.pending_reservations
            .write()
            .insert(make_addr(8430), (Location::new(0.65), Instant::now()));
        assert!(
            cm.admits_lattice_edge_over_cap(pred, None),
            "a reservation on the opposite side does not block a predecessor fill"
        );
        set_nn_lattice_enabled(true);
    }

    /// Source-scrape pin (B1 regression guard): BOTH connection-lifecycle
    /// promotion gates must apply the shared `admits_lattice_edge_over_cap`
    /// exception (reservation-aware, so they pass the candidate's `peer_addr` to
    /// exclude its own reservation) to their `>= max_connections` reject. Without
    /// this a lattice edge is silently dropped at the cap on the real CONNECT path
    /// and mechanism 3's at-capacity displacement is inert — the exact defect the
    /// external review caught (the over-max unit test passed only because it
    /// bypassed the gate). If a future refactor drops the exception from a gate,
    /// this fails instead of silently regressing.
    #[test]
    fn lifecycle_gates_apply_lattice_over_cap_predicate() {
        const LIFECYCLE_SRC: &str =
            include_str!("../node/network_bridge/p2p_protoc/connection_lifecycle.rs");
        let hits = LIFECYCLE_SRC
            .matches("admits_lattice_edge_over_cap(loc, Some(peer_addr))")
            .count();
        assert!(
            hits >= 2,
            "both lifecycle promotion cap-gates must apply \
             admits_lattice_edge_over_cap(loc, Some(peer_addr)); found {hits} call site(s)"
        );
    }

    #[test]
    fn test_prune_nonexistent_connection() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(9999);
        let pruned_loc = cm.prune_alive_connection(addr);
        assert!(pruned_loc.is_none());
    }

    // ============ Transient connection tests ============

    #[test]
    fn test_transient_connection_registration() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        assert_eq!(cm.transient_count(), 0);
        assert!(!cm.is_transient(addr));

        // Register transient
        assert!(cm.try_register_transient(addr, Some(Location::new(0.5))));
        assert_eq!(cm.transient_count(), 1);
        assert!(cm.is_transient(addr));
    }

    #[test]
    fn test_transient_connection_budget_enforcement() {
        // Create manager with budget of 2
        let keypair = TransportKeypair::new();
        let own_addr = make_addr(8000);
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(own_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            2, // transient_budget = 2
            Duration::from_secs(60),
            0,
        );

        // First two should succeed
        assert!(cm.try_register_transient(make_addr(8001), None));
        assert!(cm.try_register_transient(make_addr(8002), None));
        assert_eq!(cm.transient_count(), 2);

        // Third should fail (over budget)
        assert!(!cm.try_register_transient(make_addr(8003), None));
        assert_eq!(cm.transient_count(), 2);
    }

    #[test]
    fn test_transient_connection_update_existing() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        assert!(cm.try_register_transient(addr, None));
        assert_eq!(cm.transient_count(), 1);

        // Updating existing should succeed without incrementing count
        assert!(cm.try_register_transient(addr, Some(Location::new(0.3))));
        assert_eq!(cm.transient_count(), 1);
    }

    #[test]
    fn test_drop_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        cm.try_register_transient(addr, Some(Location::new(0.5)));
        assert_eq!(cm.transient_count(), 1);

        let entry = cm.drop_transient(addr);
        assert!(entry.is_some());
        assert_eq!(cm.transient_count(), 0);
        assert!(!cm.is_transient(addr));
    }

    #[test]
    fn test_drop_nonexistent_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let entry = cm.drop_transient(make_addr(9999));
        assert!(entry.is_none());
    }

    // ============ update_location tests ============

    #[test]
    fn test_update_location() {
        let cm = make_connection_manager(None, 1, 10, false);

        // Initially no location
        cm.update_location(Some(Location::new(0.5)));
        // Location should be updated (we can't directly read it, but it shouldn't panic)
    }

    #[test]
    fn test_update_location_to_none() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        cm.update_location(None);
        // Should not panic
    }

    #[test]
    fn test_update_location_preserves_existing() {
        // Issue #2773: Once a location is set, subsequent update_location calls
        // should NOT overwrite it. This ensures distance-based tie-breaker uses
        // consistent locations throughout the peer's lifetime.
        let cm = make_connection_manager(None, 1, 10, false);

        // Set initial location
        let initial_loc = Location::new(0.3);
        cm.update_location(Some(initial_loc));
        assert_eq!(cm.get_stored_location(), Some(initial_loc));

        // Try to update to different location - should be ignored
        let new_loc = Location::new(0.7);
        cm.update_location(Some(new_loc));
        assert_eq!(
            cm.get_stored_location(),
            Some(initial_loc),
            "Location should be preserved, not overwritten"
        );
    }

    // ============ try_set_own_addr tests ============

    #[test]
    fn test_try_set_own_addr_when_unset() {
        // `try_set_own_addr` on an unset `own_addr` calls
        // `network_status::set_external_address`, writing the process-global
        // `NETWORK_STATUS`. Hold the crate-wide lock so this does not race
        // `test_set_own_addr_atomic_with_network_status` or the
        // `node::network_status` tests under `cargo test` (one process, all
        // tests concurrent). See `TEST_GLOBAL_STATE_LOCK` rustdoc.
        let _global = crate::node::network_status::TEST_GLOBAL_STATE_LOCK
            .lock()
            .expect("TEST_GLOBAL_STATE_LOCK poisoned by an earlier test panic");
        let cm = make_connection_manager(None, 1, 10, false);

        let addr = make_addr(8000);
        let result = cm.try_set_own_addr(addr);
        assert!(result.is_none()); // Returns None when successfully set
        assert_eq!(cm.get_own_addr(), Some(addr));
    }

    #[test]
    fn test_try_set_own_addr_when_already_set() {
        // No `TEST_GLOBAL_STATE_LOCK` needed: `own_addr` starts `Some`, so
        // `try_set_own_addr` takes the early-return branch and never calls
        // `network_status::set_external_address` — this test does not touch
        // the process-global `NETWORK_STATUS`.
        let original_addr = make_addr(8000);
        let cm = make_connection_manager(Some(original_addr), 1, 10, false);

        let new_addr = make_addr(9000);
        let result = cm.try_set_own_addr(new_addr);
        assert_eq!(result, Some(original_addr)); // Returns existing when already set
        assert_eq!(cm.get_own_addr(), Some(original_addr)); // Unchanged
    }

    // ============ set_own_addr TOCTOU regression test (#4172) ============

    /// Regression test for issue #4172.
    ///
    /// `set_own_addr` writes two mirrored pieces of state: the `own_addr`
    /// mutex and `network_status::external_address`. PR #4129 narrowed the
    /// lock so it was released before `set_external_address` was called,
    /// making the two writes non-atomic. Two concurrent `set_own_addr(X)` /
    /// `set_own_addr(Y)` calls could then interleave so that `own_addr` ended
    /// up `Y` while `external_address` ended up `X` permanently.
    ///
    /// ## How the test detects the bug
    ///
    /// Each round spawns several threads, each calling `set_own_addr` with a
    /// distinct address. After all threads join, `set_own_addr` activity has
    /// completely ceased, so the two mirrored values MUST agree — there is
    /// no in-flight writer that could explain a difference. With the fix the
    /// last mutex holder writes both values atomically, so they always
    /// agree. With the bug (lock released before `set_external_address`) the
    /// thread that performs the globally-last `own_addr` write can differ
    /// from the thread that performs the globally-last `set_external_address`
    /// write, leaving a durable mismatch.
    ///
    /// A single round only catches the race when the final writes happen to
    /// interleave, so the test runs many rounds with a `Barrier` aligning
    /// thread starts to maximize contention. With the bug present this
    /// diverges reliably (empirically within ~100 rounds); the assertion
    /// below is exact (`==`), never probabilistic — the fixed code passes
    /// every round.
    #[test]
    fn test_set_own_addr_atomic_with_network_status() {
        use std::sync::{Arc as StdArc, Barrier};

        // GLOBAL-STATE CONTRACT: this test reads and writes the process-global
        // `network_status::external_address` (via `set_own_addr`). Under
        // `cargo test` — the project's documented pre-commit command (see
        // AGENTS.md) — the whole lib-test binary runs in ONE process with
        // tests executing concurrently, so any other test touching that
        // global races this one's `assert_eq!`. (CI's `cargo nextest` masks
        // the race with per-process isolation; `cargo test` does not.)
        //
        // `connection_manager`'s `test_try_set_own_addr_*` tests and every
        // test in `node::network_status` also touch this global. They are all
        // serialized by the crate-wide `TEST_GLOBAL_STATE_LOCK`, which this
        // test acquires below for its entire body.
        //
        // `network_status::set_external_address` is a no-op until `init()` has
        // run. `init()` is idempotent-overwrite (installs the tracker on the
        // first call, overwrites it in place on later calls), so this is safe
        // even if another test already initialized the global.
        let _global = crate::node::network_status::TEST_GLOBAL_STATE_LOCK
            .lock()
            .expect("TEST_GLOBAL_STATE_LOCK poisoned by an earlier test panic");
        crate::node::network_status::init(0, HashSet::new(), "test".to_string());

        // Use a single ConnectionManager across all rounds: the bug is a
        // durable inconsistency, so any round that diverges fails the test.
        let cm = StdArc::new(make_connection_manager(None, 1, 10, false));

        const ROUNDS: u32 = 4_000;
        const THREADS: u16 = 8;

        // Collect the first divergence (if any) instead of asserting inside
        // the loop. Panicking while `_global` is held would poison
        // `TEST_GLOBAL_STATE_LOCK` and cascade `PoisonError`s into every
        // other test that shares it; instead we record the failure, drop the
        // guard, then assert. The check itself is unchanged — exact equality
        // of the two mirrors after all writers for the round have joined.
        let mut divergence: Option<String> = None;
        for round in 0..ROUNDS {
            let barrier = StdArc::new(Barrier::new(THREADS as usize));
            let mut handles = Vec::with_capacity(THREADS as usize);
            for i in 0..THREADS {
                let cm = StdArc::clone(&cm);
                let barrier = StdArc::clone(&barrier);
                // Distinct port per (round, thread) so each call writes a
                // unique value and a mismatch is unambiguous.
                let port = 20_000u16.wrapping_add((round as u16) << 3).wrapping_add(i);
                let addr = make_addr(port);
                handles.push(std::thread::spawn(move || {
                    // Align starts so the threads' critical sections overlap.
                    barrier.wait();
                    cm.set_own_addr(addr);
                }));
            }
            for h in handles {
                h.join().expect("set_own_addr thread panicked");
            }

            // All writers have finished: the two mirrors MUST agree on
            // whichever writer won the race.
            let own = cm.get_own_addr();
            let external = crate::node::network_status::external_address();
            if own.is_none() {
                divergence = Some(format!(
                    "round {round}: own_addr unset after concurrent writes"
                ));
                break;
            }
            if own != external {
                divergence = Some(format!(
                    "round {round}: own_addr ({own:?}) and \
                     network_status::external_address ({external:?}) diverged — \
                     set_own_addr is not atomic (#4172)"
                ));
                break;
            }
        }

        // Release the shared global-state lock before asserting so a failure
        // does not poison it for other tests.
        drop(_global);
        if let Some(msg) = divergence {
            panic!("{msg}");
        }
    }

    // ============ update_peer_identity tests ============

    #[test]
    fn test_update_peer_identity() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let new_keypair = TransportKeypair::new();

        let old_addr = make_addr(8001);
        let new_addr = make_addr(8002);
        let loc = Location::new(0.5);

        // Add initial connection
        cm.add_connection(loc, old_addr, keypair.public().clone(), false);
        assert_eq!(cm.connection_count(), 1);

        // Update peer identity
        let updated = cm.update_peer_identity(old_addr, new_addr, new_keypair.public().clone());
        assert!(updated);
        assert_eq!(cm.connection_count(), 1);

        // Old address should no longer be connected
        let connections = cm.get_connections_by_location();
        let conns = connections.get(&loc).unwrap();
        assert_eq!(conns[0].location.socket_addr(), Some(new_addr));
    }

    #[test]
    fn test_update_peer_identity_same_addr() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Same address should return false
        let updated = cm.update_peer_identity(addr, addr, keypair.public().clone());
        assert!(!updated);
    }

    #[test]
    fn test_update_peer_identity_nonexistent() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let old_addr = make_addr(9999);
        let new_addr = make_addr(9998);

        let updated = cm.update_peer_identity(old_addr, new_addr, keypair.public().clone());
        assert!(!updated);
    }

    #[test]
    fn test_update_peer_identity_migrates_health_and_connected_since() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let new_keypair = TransportKeypair::new();

        let old_addr = make_addr(8001);
        let new_addr = make_addr(8002);
        let loc = Location::new(0.5);

        // Add connection (this sets connected_since and init_peer)
        cm.add_connection(loc, old_addr, keypair.public().clone(), false);

        // Record some health events on old address
        cm.peer_health.lock().record_success(old_addr);
        cm.peer_health.lock().record_failure(old_addr);
        cm.peer_health.lock().record_success(old_addr);

        // Verify old address has data
        assert!(cm.connected_since.read().contains_key(&old_addr));
        assert_eq!(
            cm.peer_health
                .lock()
                .stats
                .get(&old_addr)
                .unwrap()
                .successes,
            2
        );
        assert_eq!(
            cm.peer_health.lock().stats.get(&old_addr).unwrap().failures,
            1
        );

        // Update identity
        let updated = cm.update_peer_identity(old_addr, new_addr, new_keypair.public().clone());
        assert!(updated);

        // Old address should have no data
        assert!(!cm.connected_since.read().contains_key(&old_addr));
        assert!(!cm.peer_health.lock().stats.contains_key(&old_addr));

        // New address should have migrated data
        assert!(cm.connected_since.read().contains_key(&new_addr));
        assert_eq!(
            cm.peer_health
                .lock()
                .stats
                .get(&new_addr)
                .unwrap()
                .successes,
            2
        );
        assert_eq!(
            cm.peer_health.lock().stats.get(&new_addr).unwrap().failures,
            1
        );
    }

    // ============ routing tests ============

    #[test]
    fn test_routing_empty_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let router = Router::new(&[]);

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_with_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        // Add a connection
        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_some());
    }

    #[test]
    fn test_routing_skips_requester() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Request from the same address should not route back to itself
        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), Some(addr), &empty_set, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_skips_skip_list() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Put peer in skip list
        let mut skip_list = HashSet::new();
        skip_list.insert(addr);

        let result = cm.routing(Location::new(0.5), None, &skip_list, &router);
        assert!(result.is_none());
    }

    #[test]
    fn test_routing_skips_transient() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();
        let router = Router::new(&[]);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);
        cm.add_connection(loc, addr, keypair.public().clone(), false);

        // Mark as transient
        cm.try_register_transient(addr, Some(loc));

        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let result = cm.routing(Location::new(0.5), None, &empty_set, &router);
        assert!(result.is_none());
    }

    // ============ Self-routing prevention tests ============
    //
    // These tests prevent regression of self-routing bugs that caused infinite loops
    // and network congestion. The bugs manifested in several ways:
    //
    // - #1806: Nodes routing messages back to themselves, creating routing loops
    // - #1786: Connection state inconsistency when a node appeared in its own peer list
    // - #1781: PUT operations failing due to self-connections in the routing path
    // - #1827: Gateway nodes incorrectly routing to themselves under high load
    //
    // The fix involves multiple defense layers:
    // 1. should_accept() rejects connection attempts from own address
    // 2. add_connection() has a debug_assert to catch violations in development
    // 3. routing_candidates() filters out the requester to prevent echo routing

    /// Test that add_connection rejects own address (debug mode only)
    ///
    /// **Bug scenario prevented (#1806, #1781, #1827):**
    /// A node could accidentally add itself to its own connection list, typically
    /// when processing connection messages with incorrectly resolved addresses.
    /// This caused infinite routing loops where a node would route to itself,
    /// then route again to itself, consuming CPU and blocking real operations.
    ///
    /// **Why debug-only is acceptable:**
    /// This test verifies a debug_assert that catches programming errors during
    /// development. In release builds, the should_accept() check (which IS tested
    /// in release mode) provides the primary defense. The debug_assert is a
    /// secondary safety net for catching bugs that bypass should_accept().
    ///
    /// **Note:** The should_accept() guard is tested in `rejects_self_connection`
    /// which runs in both debug and release modes.
    #[test]
    #[should_panic(expected = "assertion failed")]
    #[cfg(debug_assertions)]
    fn test_add_connection_rejects_own_address() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager(Some(own_addr), 1, 10, false);
        let keypair = TransportKeypair::new();
        let own_loc = Location::new(0.5);

        // This should panic in debug mode due to the debug_assert
        cm.add_connection(own_loc, own_addr, keypair.public().clone(), false);
    }

    /// Test that routing_candidates() respects requester parameter
    ///
    /// **Bug scenario prevented (#1806, #1786):**
    /// When processing a routing request, if the node sent the response back
    /// to the original requester as a "next hop", this created echo patterns
    /// where messages bounced between two nodes indefinitely. The requester
    /// parameter ensures we never route back to whoever sent us the request.
    ///
    /// **How the original bug manifested:**
    /// Node A sends GET to Node B. Node B, when selecting routing candidates,
    /// would sometimes select Node A as the best candidate (if A was close to
    /// the target location). This caused the GET to bounce back to A, which
    /// would send it back to B, creating a ping-pong loop.
    #[test]
    fn test_routing_candidates_respects_requester() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager(Some(own_addr), 1, 10, false);
        let keypair = TransportKeypair::new();

        // Add several peers
        let requester_addr = make_addr(8001);
        let requester_loc = Location::new(0.3);
        cm.add_connection(
            requester_loc,
            requester_addr,
            keypair.public().clone(),
            false,
        );

        let peer2_addr = make_addr(8002);
        let peer2_loc = Location::new(0.5);
        cm.add_connection(peer2_loc, peer2_addr, keypair.public().clone(), false);

        let peer3_addr = make_addr(8003);
        let peer3_loc = Location::new(0.7);
        cm.add_connection(peer3_loc, peer3_addr, keypair.public().clone(), false);

        // Get routing candidates with requester specified
        let empty_set: HashSet<SocketAddr> = HashSet::new();
        let target = Location::new(0.5);
        let candidates = cm.routing_candidates(target, Some(requester_addr), &empty_set, true);

        // Should have 2 candidates (excluding requester)
        assert_eq!(
            candidates.len(),
            2,
            "routing_candidates should exclude requester"
        );

        // Verify requester is not in candidates
        for peer in &candidates {
            let addr = peer.socket_addr().expect("Peer should have address");
            assert_ne!(
                addr, requester_addr,
                "routing_candidates must not include requester address"
            );
        }

        // Verify other peers are included
        let addrs: HashSet<SocketAddr> =
            candidates.iter().filter_map(|p| p.socket_addr()).collect();
        assert!(addrs.contains(&peer2_addr));
        assert!(addrs.contains(&peer3_addr));
    }

    /// Test that should_accept rejects self-connection with IPv6 addresses
    ///
    /// **Bug scenario prevented (#1806, #1786):**
    /// Self-connection rejection must work regardless of address format.
    /// Early implementations only checked IPv4 addresses, allowing IPv6
    /// loopback connections to create self-loops.
    ///
    /// This test extends `rejects_self_connection` to cover IPv6 addresses.
    #[test]
    fn test_should_accept_rejects_own_addr_ipv6() {
        let keypair = TransportKeypair::new();

        // Test with IPv6 loopback
        let ipv6_addr: SocketAddr = "[::1]:8000".parse().unwrap();
        let cm = ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            1,
            10,
            7,
            (
                keypair.public().clone(),
                Some(ipv6_addr),
                AtomicU64::new(u64::from_le_bytes(0.5f64.to_le_bytes())),
            ),
            false,
            10,
            Duration::from_secs(60),
            0,
        );

        assert_eq!(cm.get_own_addr(), Some(ipv6_addr));
        let location = Location::new(0.5);
        let accepted = cm.should_accept(location, ipv6_addr);
        assert!(
            !accepted,
            "should_accept must reject self-connection for IPv6 addresses"
        );

        // Verify different address is accepted
        let other_addr: SocketAddr = "[::1]:8001".parse().unwrap();
        let accepted_other = cm.should_accept(location, other_addr);
        assert!(
            accepted_other,
            "should_accept must accept connection from different IPv6 address"
        );
    }

    // ============ get_connections_by_location tests ============

    #[test]
    fn test_get_connections_by_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let loc1 = Location::new(0.3);
        let loc2 = Location::new(0.7);

        cm.add_connection(loc1, addr1, keypair.public().clone(), false);
        cm.add_connection(loc2, addr2, keypair.public().clone(), false);

        let connections = cm.get_connections_by_location();
        assert_eq!(connections.len(), 2);
        assert!(connections.contains_key(&loc1));
        assert!(connections.contains_key(&loc2));
    }

    // ============ has_connection_or_pending tests ============

    #[test]
    fn test_has_connection_or_pending() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        assert!(!cm.has_connection_or_pending(addr));

        // After accepting (but not adding), should be pending
        cm.should_accept(loc, addr);
        assert!(cm.has_connection_or_pending(addr));

        // After adding, should still return true
        cm.add_connection(loc, addr, keypair.public().clone(), true);
        assert!(cm.has_connection_or_pending(addr));
    }

    // ============ record_pending_location tests ============

    #[test]
    fn test_record_pending_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        cm.record_pending_location(addr, loc);

        // Location should be recorded
        assert!(cm.location_for_peer.read().contains_key(&addr));
    }

    #[test]
    fn test_record_pending_location_already_exists() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);

        let addr = make_addr(8001);
        let loc1 = Location::new(0.5);
        let loc2 = Location::new(0.7);

        cm.record_pending_location(addr, loc1);
        cm.record_pending_location(addr, loc2);

        // Should keep original location
        let locations = cm.location_for_peer.read();
        assert_eq!(locations.get(&addr), Some(&loc1));
    }

    #[test]
    fn test_peer_addr_returns_address_when_joined() {
        let addr = make_addr(8000);
        let cm = make_connection_manager(Some(addr), 5, 20, false);

        let result = cm.peer_addr();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), addr);
    }

    #[test]
    fn test_peer_addr_returns_error_when_not_joined() {
        let cm = make_connection_manager(None, 5, 20, false);

        let result = cm.peer_addr();
        assert!(result.is_err());
        assert!(matches!(result, Err(super::RingError::PeerNotJoined)));
    }

    #[test]
    fn test_peer_addr_is_consistent_with_get_own_addr() {
        // When get_own_addr returns Some, peer_addr should return Ok with same value
        let addr = make_addr(9000);
        let cm = make_connection_manager(Some(addr), 5, 20, false);
        assert_eq!(cm.get_own_addr(), Some(addr));
        assert_eq!(cm.peer_addr().unwrap(), addr);

        // When get_own_addr returns None, peer_addr should return Err
        let cm_no_addr = make_connection_manager(None, 5, 20, false);
        assert_eq!(cm_no_addr.get_own_addr(), None);
        assert!(cm_no_addr.peer_addr().is_err());
    }

    // ============ pending_reservations TTL tests ============

    #[test]
    fn test_stale_pending_reservations_are_cleaned_up() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let loc1 = Location::new(0.1);
        let loc2 = Location::new(0.2);

        // Create reservations via should_accept
        assert!(cm.should_accept(loc1, addr1));
        assert!(cm.should_accept(loc2, addr2));
        assert_eq!(cm.get_reserved_connections(), 2);

        // Cleanup should remove nothing when entries are fresh
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 0);
        assert_eq!(cm.get_reserved_connections(), 2);

        // Backdate one entry to simulate expiration
        {
            let mut pending = cm.pending_reservations.write();
            if let Some(entry) = pending.get_mut(&addr1) {
                entry.1 = Instant::now() - Duration::from_secs(120);
            }
        }

        // Cleanup should remove only the stale entry
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 1);
        assert_eq!(cm.get_reserved_connections(), 1);
        assert!(!cm.has_connection_or_pending(addr1));
        assert!(cm.has_connection_or_pending(addr2));
    }

    #[test]
    fn test_has_connection_or_pending_ignores_expired_reservation() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Create a reservation via should_accept
        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));

        // Backdate the reservation to simulate expiration
        {
            let mut pending = cm.pending_reservations.write();
            if let Some(entry) = pending.get_mut(&addr) {
                entry.1 = Instant::now() - Duration::from_secs(120);
            }
        }

        // Expired reservation should be invisible
        assert!(
            !cm.has_connection_or_pending(addr),
            "has_connection_or_pending should ignore expired reservations"
        );
    }

    #[test]
    fn test_has_connection_or_pending_sees_fresh_reservation() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        assert!(cm.should_accept(loc, addr));
        assert!(
            cm.has_connection_or_pending(addr),
            "has_connection_or_pending should see fresh reservations"
        );
    }

    /// Regression test for #3319: when a peer has 0 connections and all gateways
    /// appear "connected/pending" due to stale reservations, clear_pending_reservations_for
    /// must make them retryable immediately.
    #[test]
    fn test_clear_pending_reservations_for_unblocks_gateways() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        let gw1_addr = make_addr(31337);
        let gw2_addr = make_addr(31338);
        let other_addr = make_addr(9001);
        let gw1_loc = Location::new(0.5);
        let gw2_loc = Location::new(0.7);
        let other_loc = Location::new(0.3);

        // Simulate failed join_ring_request creating pending reservations
        assert!(cm.should_accept(gw1_loc, gw1_addr));
        assert!(cm.should_accept(gw2_loc, gw2_addr));
        assert!(cm.should_accept(other_loc, other_addr));
        assert_eq!(cm.get_reserved_connections(), 3);

        // All three appear as "connected/pending"
        assert!(cm.has_connection_or_pending(gw1_addr));
        assert!(cm.has_connection_or_pending(gw2_addr));
        assert!(cm.has_connection_or_pending(other_addr));

        // Clear only the gateway addresses
        cm.clear_pending_reservations_for(&[gw1_addr, gw2_addr]);

        // Gateways are now retryable
        assert!(
            !cm.has_connection_or_pending(gw1_addr),
            "gateway 1 should be retryable after clearing"
        );
        assert!(
            !cm.has_connection_or_pending(gw2_addr),
            "gateway 2 should be retryable after clearing"
        );
        // Non-gateway reservation should be untouched
        assert!(
            cm.has_connection_or_pending(other_addr),
            "non-gateway reservation should not be affected"
        );
        assert_eq!(cm.get_reserved_connections(), 1);
    }

    #[test]
    fn test_clear_pending_reservations_for_empty_list() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(31337);
        let loc = Location::new(0.5);

        assert!(cm.should_accept(loc, addr));
        assert_eq!(cm.get_reserved_connections(), 1);

        // Clearing with an empty list should be a no-op
        cm.clear_pending_reservations_for(&[]);
        assert_eq!(cm.get_reserved_connections(), 1);
        assert!(cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_clear_pending_reservations_for_unknown_addrs() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(31337);
        let loc = Location::new(0.5);

        assert!(cm.should_accept(loc, addr));
        assert_eq!(cm.get_reserved_connections(), 1);

        // Clearing unknown addresses should be a no-op
        let unknown = make_addr(9999);
        cm.clear_pending_reservations_for(&[unknown]);
        assert_eq!(cm.get_reserved_connections(), 1);
        assert!(cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_has_connection_or_pending_always_sees_established() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        // Add an established connection
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(
            cm.has_connection_or_pending(addr),
            "has_connection_or_pending should always see established connections"
        );
    }

    #[test]
    fn test_has_connection_or_pending_ttl_boundary() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        assert!(cm.should_accept(Location::new(0.1), addr));

        age_reservation(&cm, addr, PENDING_RESERVATION_TTL - Duration::from_secs(1));
        assert!(cm.has_connection_or_pending(addr));

        age_reservation(&cm, addr, PENDING_RESERVATION_TTL + Duration::from_secs(1));
        assert!(!cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_re_reservation_after_expiry() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let addr = make_addr(8001);
        let loc = Location::new(0.1);

        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));

        age_reservation(&cm, addr, Duration::from_secs(120));
        assert!(!cm.has_connection_or_pending(addr));

        assert!(cm.should_accept(loc, addr));
        assert!(cm.has_connection_or_pending(addr));
    }

    #[test]
    fn test_cleanup_frees_slots_for_new_connections() {
        let cm = make_connection_manager(Some(make_addr(8000)), 2, 3, false);
        let addr1 = make_addr(8001);
        let addr2 = make_addr(8002);
        let addr3 = make_addr(8003);

        assert!(cm.should_accept(Location::new(0.1), addr1));
        assert!(cm.should_accept(Location::new(0.2), addr2));

        age_reservation(&cm, addr1, Duration::from_secs(120));
        assert_eq!(cm.cleanup_stale_reservations(), 1);

        assert!(cm.should_accept(Location::new(0.3), addr3));
    }

    // ============ Admission control tests ============

    #[test]
    fn test_admission_control_gateway_limit() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        // Gateway should have max_concurrent_connects = MAX_CONCURRENT_GATEWAY_CONNECTS
        let mut guards = Vec::new();
        for _ in 0..MAX_CONCURRENT_GATEWAY_CONNECTS {
            let guard = cm.try_admit_connect();
            assert!(guard.is_some(), "should admit within limit");
            guards.push(guard.unwrap());
        }
        assert!(
            cm.try_admit_connect().is_none(),
            "should reject when at capacity"
        );
    }

    #[test]
    fn test_admission_control_release_frees_slot() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        // Fill up all slots
        let mut guards = Vec::new();
        for _ in 0..MAX_CONCURRENT_GATEWAY_CONNECTS {
            guards.push(cm.try_admit_connect().unwrap());
        }
        assert!(cm.try_admit_connect().is_none());

        // Release one slot by dropping the guard
        guards.pop();
        assert!(
            cm.try_admit_connect().is_some(),
            "should admit after release"
        );
    }

    #[test]
    fn test_admission_control_non_gateway_unlimited() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);

        // Non-gateway should have unlimited admission — hold guards to keep slots occupied
        let guards: Vec<_> = (0..100)
            .map(|_| {
                cm.try_admit_connect()
                    .expect("non-gateway should always admit")
            })
            .collect();
        assert_eq!(guards.len(), 100);
    }

    #[test]
    fn test_admission_guard_explicit_release() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        let guard = cm.try_admit_connect().unwrap();
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 1);

        // Explicit release
        guard.release();
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_admission_guard_drop_release() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, true);

        {
            let _guard = cm.try_admit_connect().unwrap();
            assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 1);
        }
        // Guard dropped, slot should be released
        assert_eq!(cm.connect_in_flight.load(Ordering::SeqCst), 0);
    }

    // ============ Phantom location_for_peer cleanup tests (#3088) ============

    /// Regression test for #3088: When open > 0, should_accept inserts into both
    /// pending_reservations and location_for_peer. If the CONNECT times out and only
    /// the reservation is cleaned, a phantom location_for_peer entry remains, causing
    /// has_connection_or_pending to permanently return true for that address.
    #[test]
    fn test_cleanup_removes_orphaned_location_for_peer() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let existing_addr = make_addr(8001);
        let phantom_addr = make_addr(8002);
        let phantom_loc = Location::new(0.5);

        // Establish one real connection so open > 0
        cm.add_connection(
            Location::new(0.1),
            existing_addr,
            keypair.public().clone(),
            false,
        );
        assert_eq!(cm.connection_count(), 1);

        // should_accept with open > 0 calls record_pending_location,
        // creating entries in both pending_reservations and location_for_peer
        assert!(cm.should_accept(phantom_loc, phantom_addr));
        assert!(cm.has_connection_or_pending(phantom_addr));

        // Simulate what happens when a CONNECT times out: the pending reservation
        // expires but location_for_peer is not cleaned (the bug)
        age_reservation(&cm, phantom_addr, Duration::from_secs(120));

        // Before the fix, has_connection_or_pending would still return true
        // because location_for_peer still has the entry
        assert!(
            cm.location_for_peer.read().contains_key(&phantom_addr),
            "phantom entry should exist in location_for_peer"
        );

        // cleanup_stale_reservations should now also clean the orphaned location
        let removed = cm.cleanup_stale_reservations();
        assert_eq!(
            removed, 2,
            "should remove exactly 1 stale reservation + 1 orphaned location"
        );

        assert!(
            !cm.location_for_peer.read().contains_key(&phantom_addr),
            "phantom location_for_peer entry should be removed"
        );
        assert!(
            !cm.has_connection_or_pending(phantom_addr),
            "has_connection_or_pending should return false after cleanup"
        );

        // The established connection should be untouched
        assert!(cm.has_connection_or_pending(existing_addr));
        assert_eq!(cm.connection_count(), 1);
    }

    /// Regression test for #3244: has_connection_or_pending should return false for
    /// orphaned location_for_peer entries IMMEDIATELY — without waiting for
    /// cleanup_stale_reservations to run. The previous behavior caused the bootstrap
    /// loop to stall for up to 65 seconds because it thought all gateways were
    /// "connected/pending" when they actually had only stale location_for_peer entries.
    #[test]
    fn test_has_connection_or_pending_ignores_orphaned_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let existing_addr = make_addr(8001);
        let orphan_addr = make_addr(8002);
        let orphan_loc = Location::new(0.5);

        // Establish one real connection so open > 0
        cm.add_connection(
            Location::new(0.1),
            existing_addr,
            keypair.public().clone(),
            false,
        );

        // should_accept with open > 0 creates entries in both data structures
        assert!(cm.should_accept(orphan_loc, orphan_addr));

        // Expire the pending reservation (simulates connect timeout)
        age_reservation(&cm, orphan_addr, Duration::from_secs(120));

        // The orphaned location_for_peer entry still exists...
        assert!(cm.location_for_peer.read().contains_key(&orphan_addr));
        // ...but has_connection_or_pending should return false because there's
        // no established connection backing it (the fix for #3244).
        assert!(
            !cm.has_connection_or_pending(orphan_addr),
            "has_connection_or_pending should return false for orphaned location_for_peer \
             entries without established connections (#3244)"
        );

        // Established connection should still be visible
        assert!(cm.has_connection_or_pending(existing_addr));
    }

    /// Verify that cleanup_stale_reservations does NOT remove location_for_peer
    /// entries that have a corresponding established connection.
    #[test]
    fn test_cleanup_preserves_established_connection_locations() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let addr = make_addr(8001);
        let loc = Location::new(0.3);

        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(cm.location_for_peer.read().contains_key(&addr));

        let removed = cm.cleanup_stale_reservations();
        assert_eq!(removed, 0);
        assert!(cm.location_for_peer.read().contains_key(&addr));
        assert!(cm.has_connection_or_pending(addr));
    }

    /// Verify that a peer can be promoted to ring after its transient entry expires.
    /// This is the key scenario from #3113: transient TTL fires before CONNECT completes,
    /// removing the tracking entry but preserving the transport. When CONNECT succeeds,
    /// the peer must still be added to ring topology.
    #[test]
    fn test_expired_transient_promotion() {
        let cm = make_connection_manager(Some(make_addr(8000)), 1, 10, false);
        let keypair = TransportKeypair::new();

        let addr = make_addr(8001);
        let loc = Location::new(0.5);

        // Step 1: Register transient connection (simulates inbound connection)
        assert!(cm.try_register_transient(addr, Some(loc)));
        assert!(cm.is_transient(addr));
        assert!(!cm.is_in_ring(addr));

        // Step 2: Drop transient (simulates TTL expiry)
        let entry = cm.drop_transient(addr);
        assert!(entry.is_some());
        assert!(!cm.is_transient(addr));
        assert!(!cm.is_in_ring(addr));

        // Step 3: CONNECT succeeds — add_connection should work
        cm.add_connection(loc, addr, keypair.public().clone(), false);
        assert!(cm.is_in_ring(addr));
        assert_eq!(cm.connection_count(), 1);
    }

    /// Verify that `prune_in_transit_connection` clears the phantom
    /// entry directly (Fix 2 of #3088).
    #[test]
    fn test_prune_in_transit_clears_phantom_location() {
        let cm = make_connection_manager(Some(make_addr(8000)), 5, 20, false);
        let keypair = TransportKeypair::new();
        let existing_addr = make_addr(8001);
        let phantom_addr = make_addr(8002);

        // Establish one real connection so open > 0
        cm.add_connection(
            Location::new(0.1),
            existing_addr,
            keypair.public().clone(),
            false,
        );

        // Accept a new peer (creates location_for_peer + pending_reservations entries)
        assert!(cm.should_accept(Location::new(0.5), phantom_addr));
        assert!(cm.has_connection_or_pending(phantom_addr));

        // prune_in_transit_connection should clear the location_for_peer entry
        cm.prune_in_transit_connection(phantom_addr);

        assert!(
            !cm.location_for_peer.read().contains_key(&phantom_addr),
            "prune_in_transit_connection should remove the location_for_peer entry"
        );
        // prune_connection(addr, false) also removes the pending_reservations entry,
        // so has_connection_or_pending should now return false
        assert!(
            !cm.has_connection_or_pending(phantom_addr),
            "has_connection_or_pending should return false after prune_in_transit_connection"
        );
    }

    // ============ Readiness gating tests ============

    fn make_connection_manager_with_readiness(
        own_addr: Option<SocketAddr>,
        min_conn: usize,
        max_conn: usize,
        is_gateway: bool,
        min_ready_connections: usize,
    ) -> ConnectionManager {
        let keypair = TransportKeypair::new();
        let own_location = if let Some(addr) = own_addr {
            AtomicU64::new(u64::from_le_bytes(
                Location::from_address(&addr).as_f64().to_le_bytes(),
            ))
        } else {
            AtomicU64::new(u64::from_le_bytes((-1f64).to_le_bytes()))
        };

        ConnectionManager::init(
            Rate::new_per_second(1_000_000.0),
            Rate::new_per_second(1_000_000.0),
            min_conn,
            max_conn,
            7,
            (keypair.public().clone(), own_addr, own_location),
            is_gateway,
            10,
            Duration::from_secs(60),
            min_ready_connections,
        )
    }

    #[test]
    fn test_is_peer_ready_with_gating_disabled() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 0);
        // With min_ready_connections=0, all peers are considered ready
        let peer_addr = make_addr(9000);
        assert!(cm.is_peer_ready(peer_addr));
    }

    #[test]
    fn test_is_peer_ready_with_gating_enabled() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 2);
        let peer_addr = make_addr(9000);

        // Unknown peer is not ready
        assert!(!cm.is_peer_ready(peer_addr));

        // Mark peer ready
        cm.mark_peer_ready(peer_addr);
        assert!(cm.is_peer_ready(peer_addr));

        // Mark peer not ready
        cm.mark_peer_not_ready(peer_addr);
        assert!(!cm.is_peer_ready(peer_addr));
    }

    #[test]
    fn test_is_self_ready_threshold() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 2);

        // With 0 connections, not ready
        assert!(!cm.is_self_ready());
        assert_eq!(cm.connection_count(), 0);

        // Add 1 connection — still not ready
        let peer1 = TransportKeypair::new();
        let addr1 = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr1, peer1.public().clone(), false);
        assert!(!cm.is_self_ready());

        // Add 2nd connection — now ready
        let peer2 = TransportKeypair::new();
        let addr2 = make_addr(9002);
        cm.add_connection(Location::new(0.7), addr2, peer2.public().clone(), false);
        assert!(cm.is_self_ready());
    }

    #[test]
    fn test_is_self_ready_disabled() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 0);
        // With min_ready_connections=0, always ready
        assert!(cm.is_self_ready());
    }

    #[test]
    fn test_routing_candidates_filters_unready_peers() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager_with_readiness(Some(own_addr), 1, 10, false, 2);

        // Add two peers
        let peer1 = TransportKeypair::new();
        let addr1 = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr1, peer1.public().clone(), false);

        let peer2 = TransportKeypair::new();
        let addr2 = make_addr(9002);
        cm.add_connection(Location::new(0.7), addr2, peer2.public().clone(), false);

        // Neither peer is ready — fallback returns all not-ready peers rather than
        // empty (to prevent EmptyRing failures, see #3356).
        let target = Location::new(0.5);
        let skip = HashSet::<SocketAddr>::new();
        let candidates = cm.routing_candidates(target, Some(own_addr), &skip, true);
        assert_eq!(
            candidates.len(),
            2,
            "all-not-ready fallback should return both peers"
        );

        // Mark peer1 ready — only peer1 should appear (fallback does NOT activate
        // when at least one peer is ready).
        cm.mark_peer_ready(addr1);
        let candidates = cm.routing_candidates(target, Some(own_addr), &skip, true);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].socket_addr(), Some(addr1));

        // Mark peer2 ready — both should appear
        cm.mark_peer_ready(addr2);
        let candidates = cm.routing_candidates(target, Some(own_addr), &skip, true);
        assert_eq!(candidates.len(), 2);
    }

    #[test]
    fn test_prune_connection_cleans_up_ready_peers() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager_with_readiness(Some(own_addr), 1, 10, false, 2);

        let peer1 = TransportKeypair::new();
        let addr1 = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr1, peer1.public().clone(), false);
        cm.mark_peer_ready(addr1);
        assert!(cm.is_peer_ready(addr1));

        // Prune the connection — should clean up ready state
        cm.prune_alive_connection(addr1);
        assert!(
            !cm.is_peer_ready(addr1),
            "ready state should be cleaned up after prune"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_is_peer_ready_optimistic_timeout() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 2);
        let peer = TransportKeypair::new();
        let addr = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr, peer.public().clone(), false);

        // Peer not in ready_peers and connection is fresh — not ready
        assert!(!cm.is_peer_ready(addr));

        // Advance past the 60s optimistic timeout
        tokio::time::advance(Duration::from_secs(61)).await;

        // Now the optimistic timeout kicks in — peer treated as ready
        assert!(
            cm.is_peer_ready(addr),
            "peer should be optimistically ready after 60s"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_is_peer_ready_optimistic_timeout_not_yet() {
        let cm = make_connection_manager_with_readiness(Some(make_addr(8000)), 1, 10, false, 2);
        let peer = TransportKeypair::new();
        let addr = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr, peer.public().clone(), false);

        // Advance only 30s — not enough for the 60s timeout
        tokio::time::advance(Duration::from_secs(30)).await;

        assert!(
            !cm.is_peer_ready(addr),
            "peer should NOT be optimistically ready after only 30s"
        );
    }

    #[test]
    fn test_connected_since_cleanup_on_prune() {
        let own_addr = make_addr(8000);
        let cm = make_connection_manager_with_readiness(Some(own_addr), 1, 10, false, 2);
        let peer = TransportKeypair::new();
        let addr = make_addr(9001);
        cm.add_connection(Location::new(0.3), addr, peer.public().clone(), false);

        // Verify connected_since was populated
        assert!(
            cm.connected_since.read().contains_key(&addr),
            "connected_since should be populated after add_connection"
        );

        // Prune the connection
        cm.prune_alive_connection(addr);

        // connected_since should be cleaned up
        assert!(
            !cm.connected_since.read().contains_key(&addr),
            "connected_since should be cleaned up after prune"
        );
    }

    // ============ Concurrent stress tests (#3105) ============
    //
    // These tests exercise the documented lock ordering invariant:
    //   location_for_peer → connections_by_location → pending_reservations
    //
    // A deadlock manifests as a test timeout. Each test spawns many concurrent
    // tasks that call methods acquiring multiple locks in various combinations.

    /// Stress test: concurrent add_connection + prune_connection + should_accept.
    ///
    /// Exercises all three RwLock-guarded maps simultaneously to detect ABBA
    /// deadlocks. This is the scenario that caused the deadlock fixed in #3095:
    /// `cleanup_stale_reservations` acquired `pending_reservations` before
    /// `connections_by_location`, while `prune_connection` did the opposite.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_connection_lifecycle_stress() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use std::sync::Arc;

        const NUM_TASKS: usize = 80;
        const OPS_PER_TASK: usize = 200;
        const SEED: u64 = 0xDEAD_BEEF_CAFE_3105;

        let cm = Arc::new(make_connection_manager(Some(make_addr(7000)), 2, 50, false));

        let barrier = Arc::new(tokio::sync::Barrier::new(NUM_TASKS));

        let mut handles = Vec::with_capacity(NUM_TASKS);
        for task_id in 0..NUM_TASKS {
            let cm = Arc::clone(&cm);
            let barrier = Arc::clone(&barrier);

            handles.push(tokio::spawn(async move {
                // Each task gets a deterministic RNG derived from the global seed
                // and its task_id, so runs are reproducible.
                let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(task_id as u64));

                // Wait for all tasks to be ready before starting, maximizing contention.
                barrier.wait().await;

                for _ in 0..OPS_PER_TASK {
                    // Pick a peer port in a small range to create contention on the
                    // same addresses across tasks.
                    let port: u16 = rng.random_range(9000..9050);
                    let addr = make_addr(port);
                    let loc = Location::new(rng.random_range(0.01..0.99));

                    match rng.random_range(0u8..8) {
                        // should_accept: acquires pending_reservations(R), location_for_peer(R/W),
                        // pending_reservations(W), topology_manager(W), connections_by_location(R)
                        0 => {
                            let _ = cm.should_accept(loc, addr);
                        }
                        // add_connection: acquires pending_reservations(W), location_for_peer(W),
                        // connections_by_location(W)
                        1 => {
                            let keypair = TransportKeypair::new();
                            cm.add_connection(loc, addr, keypair.public().clone(), rng.random());
                        }
                        // prune_alive_connection: acquires location_for_peer(W),
                        // connections_by_location(W)
                        2 => {
                            let _ = cm.prune_alive_connection(addr);
                        }
                        // prune_in_transit_connection: acquires location_for_peer(W),
                        // connections_by_location(W), pending_reservations(W)
                        3 => {
                            let _ = cm.prune_in_transit_connection(addr);
                        }
                        // cleanup_stale_reservations: acquires pending_reservations(W),
                        // then location_for_peer(W), connections_by_location(R)
                        4 => {
                            let _ = cm.cleanup_stale_reservations();
                        }
                        // Read-heavy path: has_connection_or_pending acquires
                        // location_for_peer(R), pending_reservations(R)
                        5 => {
                            let _ = cm.has_connection_or_pending(addr);
                            let _ = cm.connection_count();
                            let _ = cm.num_connections();
                        }
                        // get_peer_by_addr: acquires connections_by_location(R),
                        // location_for_peer(R)
                        6 => {
                            let _ = cm.get_peer_by_addr(addr);
                        }
                        // Transient operations (DashMap, mostly lock-free but interact
                        // with other bookkeeping)
                        7 => {
                            if rng.random() {
                                let _ = cm.try_register_transient(addr, Some(loc));
                            } else {
                                let _ = cm.drop_transient(addr);
                            }
                        }
                        _ => unreachable!(),
                    }

                    // Yield occasionally to let the scheduler interleave tasks.
                    if rng.random_range(0..4) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        // All tasks must complete within 30 seconds; a hang indicates a deadlock.
        let result =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(handles)).await;

        let results = result.expect("DEADLOCK DETECTED: concurrent ConnectionManager operations did not complete within 30 seconds");
        for (i, r) in results.into_iter().enumerate() {
            r.unwrap_or_else(|e| panic!("task {i} panicked: {e}"));
        }
    }

    /// Stress test: concurrent admission control under gateway limits.
    ///
    /// Exercises the CAS loop in `try_admit_connect` with many concurrent tasks
    /// to verify the atomic counter never goes negative or exceeds the limit,
    /// and that guards properly release slots on drop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_admission_control_stress() {
        use std::sync::Arc;

        const NUM_TASKS: usize = 80;
        const OPS_PER_TASK: usize = 500;

        // Gateway mode: limited to MAX_CONCURRENT_GATEWAY_CONNECTS (8)
        let cm = Arc::new(make_connection_manager(Some(make_addr(7000)), 2, 50, true));

        let barrier = Arc::new(tokio::sync::Barrier::new(NUM_TASKS));

        let mut handles = Vec::with_capacity(NUM_TASKS);
        for _ in 0..NUM_TASKS {
            let cm = Arc::clone(&cm);
            let barrier = Arc::clone(&barrier);

            handles.push(tokio::spawn(async move {
                barrier.wait().await;

                for _ in 0..OPS_PER_TASK {
                    // Acquire a slot, hold it briefly, then release via drop
                    if let Some(guard) = cm.try_admit_connect() {
                        // Verify invariant: in-flight count is within bounds
                        let current = cm.connect_in_flight.load(Ordering::SeqCst);
                        assert!(
                            current <= MAX_CONCURRENT_GATEWAY_CONNECTS,
                            "in-flight count {current} exceeded limit {MAX_CONCURRENT_GATEWAY_CONNECTS}"
                        );
                        // Hold the guard briefly to create contention
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                }
            }));
        }

        let result =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(handles)).await;

        let results = result.expect(
            "DEADLOCK DETECTED: concurrent admission control did not complete within 30 seconds",
        );
        for (i, r) in results.into_iter().enumerate() {
            r.unwrap_or_else(|e| panic!("task {i} panicked: {e}"));
        }

        // After all tasks complete, all slots must be released
        assert_eq!(
            cm.connect_in_flight.load(Ordering::SeqCst),
            0,
            "all admission slots should be released after test completes"
        );
    }

    /// Stress test: concurrent cleanup + accept + prune (the exact deadlock scenario from #3095).
    ///
    /// This specifically targets the lock ordering between `cleanup_stale_reservations`
    /// and `prune_connection`. The original deadlock was:
    ///   Thread A: cleanup_stale_reservations → pending_reservations(W), connections_by_location(R)
    ///   Thread B: prune_connection → location_for_peer(W), connections_by_location(W), pending_reservations(W)
    ///
    /// The fix ensures cleanup releases pending_reservations before acquiring location_for_peer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_cleanup_vs_prune_stress() {
        use rand::rngs::StdRng;
        use rand::{Rng, SeedableRng};
        use std::sync::Arc;

        const NUM_TASKS: usize = 60;
        const OPS_PER_TASK: usize = 300;
        const SEED: u64 = 0xCAFE_BABE_3095_DEAD;

        let cm = Arc::new(make_connection_manager(Some(make_addr(7000)), 2, 50, false));

        let barrier = Arc::new(tokio::sync::Barrier::new(NUM_TASKS));

        let mut handles = Vec::with_capacity(NUM_TASKS);
        for task_id in 0..NUM_TASKS {
            let cm = Arc::clone(&cm);
            let barrier = Arc::clone(&barrier);

            handles.push(tokio::spawn(async move {
                let mut rng = StdRng::seed_from_u64(SEED.wrapping_add(task_id as u64));
                barrier.wait().await;

                for _ in 0..OPS_PER_TASK {
                    let port: u16 = rng.random_range(9000..9030);
                    let addr = make_addr(port);
                    let loc = Location::new(rng.random_range(0.01..0.99));

                    match rng.random_range(0u8..6) {
                        // Inject a reservation that will look stale to cleanup
                        0 => {
                            cm.inject_reservation(
                                addr,
                                loc,
                                Instant::now() - Duration::from_secs(120),
                            );
                        }
                        // should_accept (creates both reservation + location_for_peer)
                        1 => {
                            let _ = cm.should_accept(loc, addr);
                        }
                        // cleanup_stale_reservations (the method that caused #3095)
                        2 => {
                            let _ = cm.cleanup_stale_reservations();
                        }
                        // prune_connection variants (the other side of the deadlock)
                        3 => {
                            if rng.random() {
                                let _ = cm.prune_alive_connection(addr);
                            } else {
                                let _ = cm.prune_in_transit_connection(addr);
                            }
                        }
                        // add_connection (interacts with all three maps)
                        4 => {
                            let keypair = TransportKeypair::new();
                            cm.add_connection(loc, addr, keypair.public().clone(), rng.random());
                        }
                        // clear_pending_reservations_for (#3319 isolation recovery)
                        5 => {
                            let addrs: Vec<_> = (0..3)
                                .map(|_| make_addr(rng.random_range(9000..9030)))
                                .collect();
                            cm.clear_pending_reservations_for(&addrs);
                        }
                        _ => unreachable!(),
                    }

                    if rng.random_range(0..3) == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }

        let result =
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(handles)).await;

        let results = result.expect(
            "DEADLOCK DETECTED: concurrent cleanup vs prune did not complete within 30 seconds",
        );
        for (i, r) in results.into_iter().enumerate() {
            r.unwrap_or_else(|e| panic!("task {i} panicked: {e}"));
        }
    }

    // ============ PeerHealthTracker tests ============

    #[test]
    fn test_peer_health_tracker_records_success_failure() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9001);
        tracker.init_peer(addr);

        tracker.record_success(addr);
        tracker.record_success(addr);
        tracker.record_failure(addr);

        let stats = tracker.stats.get(&addr).unwrap();
        assert_eq!(stats.successes, 2);
        assert_eq!(stats.failures, 1);
        assert!(stats.last_success.is_some());
    }

    #[test]
    fn test_peer_health_tracker_evicts_high_failure_rate() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9002);
        tracker.init_peer(addr);

        // 1 success + 19 failures = 20 events, 95% failure rate
        tracker.record_success(addr);
        for _ in 0..19 {
            tracker.record_failure(addr);
        }

        let unhealthy = tracker.unhealthy_peers(1, 5);
        assert!(
            unhealthy.contains(&addr),
            "Peer with 95% failure rate (20 events) should be evicted"
        );
    }

    #[test]
    fn test_peer_health_tracker_spares_low_sample_peer() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9003);
        tracker.init_peer(addr);

        // 3 failures / 0 successes, but < 10 total events
        for _ in 0..3 {
            tracker.record_failure(addr);
        }

        let unhealthy = tracker.unhealthy_peers(1, 5);
        // Should NOT be evicted: only 3 events, below HEALTH_MIN_EVENTS=10,
        // and added_at is recent (not past HEALTH_NO_SUCCESS_TIMEOUT).
        assert!(
            !unhealthy.contains(&addr),
            "Peer with only 3 events should not be evicted by failure rate"
        );
    }

    #[test]
    fn test_peer_health_tracker_spares_degraded_peer_at_min_connections() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9004);
        tracker.init_peer(addr);

        // 1 success then 19 failures = 95% failure rate, above threshold
        tracker.record_success(addr);
        for _ in 0..19 {
            tracker.record_failure(addr);
        }

        // current_count=3, min_connections=3 => degraded peers (with successes) protected
        let unhealthy = tracker.unhealthy_peers(3, 3);
        assert!(
            unhealthy.is_empty(),
            "Degraded peer with successes should not be evicted at min_connections"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_peer_health_tracker_no_success_timeout() {
        let mut tracker = PeerHealthTracker::new();
        let addr_old = make_addr(9005);
        let addr_young = make_addr(9006);

        tracker.init_peer(addr_old);
        tracker.init_peer(addr_young);

        tracker.record_failure(addr_old);
        tracker.record_failure(addr_young);

        // Advance past the no-success timeout (10 min + 1 min margin)
        tokio::time::advance(Duration::from_secs(660)).await;

        // Record fresh failures to advance "now" for the young peer
        tracker.remove_peer(addr_young);
        tracker.init_peer(addr_young);
        tracker.record_failure(addr_young);

        let unhealthy = tracker.unhealthy_peers(1, 5);
        assert!(
            unhealthy.contains(&addr_old),
            "Peer added 11 min ago with 0 successes and 1 failure should be evicted"
        );
        assert!(
            !unhealthy.contains(&addr_young),
            "Freshly added peer should NOT be evicted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_peer_health_tracker_recent_failure_burst() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9007);
        tracker.init_peer(addr);

        // Record some early successes
        for _ in 0..5 {
            tracker.record_success(addr);
        }

        // Advance past the last-success timeout
        tokio::time::advance(Duration::from_secs(660)).await;

        // Now record a burst of failures (> HEALTH_BURST_MIN_FAILURES=10)
        for _ in 0..15 {
            tracker.record_failure(addr);
        }

        let unhealthy = tracker.unhealthy_peers(1, 5);
        assert!(
            unhealthy.contains(&addr),
            "Peer with last success >10 min ago and 15 failures should be evicted"
        );
    }

    #[test]
    fn test_peer_health_tracker_cleanup_on_prune() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9008);
        tracker.init_peer(addr);
        tracker.record_failure(addr);

        assert!(tracker.stats.contains_key(&addr));
        tracker.remove_peer(addr);
        assert!(!tracker.stats.contains_key(&addr));
    }

    #[test]
    fn test_peer_health_tracker_fresh_slate_on_reconnect() {
        let mut tracker = PeerHealthTracker::new();
        let addr = make_addr(9009);

        // First connection: accumulate failures
        tracker.init_peer(addr);
        for _ in 0..20 {
            tracker.record_failure(addr);
        }
        let stats_before = tracker.stats.get(&addr).unwrap().failures;
        assert_eq!(stats_before, 20);

        // Disconnect and reconnect
        tracker.remove_peer(addr);
        tracker.init_peer(addr);

        let stats_after = tracker.stats.get(&addr).unwrap();
        assert_eq!(stats_after.successes, 0);
        assert_eq!(stats_after.failures, 0);
        assert!(stats_after.last_success.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn test_peer_health_tracker_never_succeeded_evicted_below_min() {
        let mut tracker = PeerHealthTracker::new();
        let never_ok = make_addr(9010);
        let has_ok = make_addr(9011);

        tracker.init_peer(never_ok);
        tracker.init_peer(has_ok);

        // Advance past no-success timeout so the never-succeeded peer qualifies
        tokio::time::advance(Duration::from_secs(660)).await;

        // never_ok: 0 successes, 5 failures
        for _ in 0..5 {
            tracker.record_failure(never_ok);
        }
        // has_ok: 1 success then 19 failures = 20 events, 95% failure rate
        tracker.record_success(has_ok);
        for _ in 0..19 {
            tracker.record_failure(has_ok);
        }

        // current_count=2, min_connections=2 => old logic would evict nobody.
        // New logic: never-succeeded peers are always evictable, but degraded
        // peers (has_ok with successes>0) are protected by min.
        let unhealthy = tracker.unhealthy_peers(2, 2);
        assert!(
            unhealthy.contains(&never_ok),
            "Never-succeeded peer should be evicted even at min_connections"
        );
        assert!(
            !unhealthy.contains(&has_ok),
            "Degraded peer with successes should be protected by min_connections"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_peer_health_tracker_safety_floor_retains_one_connection() {
        let mut tracker = PeerHealthTracker::new();
        let addr_a = make_addr(9012);
        let addr_b = make_addr(9013);
        let addr_c = make_addr(9014);

        tracker.init_peer(addr_a);
        tracker.init_peer(addr_b);
        tracker.init_peer(addr_c);

        // Advance past no-success timeout
        tokio::time::advance(Duration::from_secs(660)).await;

        // All 3 peers: 0 successes, failures only
        for addr in [addr_a, addr_b, addr_c] {
            for _ in 0..5 {
                tracker.record_failure(addr);
            }
        }

        // current_count=3, min_connections=3, all never-succeeded.
        // Safety floor: should evict at most 2, retaining 1 connection.
        let unhealthy = tracker.unhealthy_peers(3, 3);
        assert_eq!(
            unhealthy.len(),
            2,
            "Should evict at most current_count-1 never-succeeded peers, got {}",
            unhealthy.len()
        );
    }

    // ============ failed_addr / adaptive TTL tests ============

    #[test]
    fn test_failed_addr_backoff_base_cases() {
        // Verify the ExponentialBackoff config used for failed addresses
        let backoff = ExponentialBackoff::new(FAILED_ADDR_BASE_TTL, FAILED_ADDR_MAX_TTL);
        // First failure (count=1): base TTL = 5 min
        assert_eq!(backoff.delay_for_failures(1), Duration::from_secs(300));
        // Second failure: 10 min
        assert_eq!(backoff.delay_for_failures(2), Duration::from_secs(600));
        // Third: 20 min
        assert_eq!(backoff.delay_for_failures(3), Duration::from_secs(1200));
        // Fourth: 40 min
        assert_eq!(backoff.delay_for_failures(4), Duration::from_secs(2400));
        // Fifth: would be 80 min, capped to 60 min
        assert_eq!(backoff.delay_for_failures(5), FAILED_ADDR_MAX_TTL);
    }

    #[test]
    fn test_failed_addr_backoff_cap_and_overflow() {
        let backoff = ExponentialBackoff::new(FAILED_ADDR_BASE_TTL, FAILED_ADDR_MAX_TTL);
        // High counts and extreme values should cap at MAX_TTL, never panic
        assert_eq!(backoff.delay_for_failures(100), FAILED_ADDR_MAX_TTL);
        assert_eq!(backoff.delay_for_failures(u32::MAX), FAILED_ADDR_MAX_TTL);
    }

    #[test]
    fn test_failed_addr_backoff_monotonic() {
        let backoff = ExponentialBackoff::new(FAILED_ADDR_BASE_TTL, FAILED_ADDR_MAX_TTL);
        // TTL should never decrease with increasing failure count
        let mut prev = backoff.delay_for_failures(1);
        for count in 2..=20 {
            let current = backoff.delay_for_failures(count);
            assert!(current >= prev, "TTL decreased at count={count}");
            prev = current;
        }
    }

    #[test]
    fn test_record_and_query_failed_addrs() {
        let cm = make_connection_manager(Some(make_addr(9000)), 1, 10, false);
        let addr = make_addr(9001);

        // No failures initially
        assert!(cm.recently_failed_addrs().is_empty());

        // Record a failure — addr should appear in the list
        cm.record_failed_addr(addr);
        let failed = cm.recently_failed_addrs();
        assert!(failed.contains(&addr));

        // Internal count should be 1 after first failure
        assert_eq!(cm.recently_failed_addrs.lock().failure_count(&addr), 1);
    }

    #[test]
    fn test_record_failed_addr_accumulates_count() {
        let cm = make_connection_manager(Some(make_addr(9100)), 1, 10, false);
        let addr = make_addr(9101);

        for expected in 1..=5u32 {
            cm.record_failed_addr(addr);
            assert_eq!(
                cm.recently_failed_addrs.lock().failure_count(&addr),
                expected
            );
        }
    }

    #[test]
    fn test_clear_failed_addr_resets_count() {
        let cm = make_connection_manager(Some(make_addr(9200)), 1, 10, false);
        let addr = make_addr(9201);

        cm.record_failed_addr(addr);
        cm.record_failed_addr(addr);
        assert_eq!(cm.recently_failed_addrs.lock().failure_count(&addr), 2);

        // Clear removes the entry entirely
        cm.clear_failed_addr(addr);
        assert_eq!(cm.recently_failed_addrs.lock().failure_count(&addr), 0);

        // Re-recording starts from count=1
        cm.record_failed_addr(addr);
        assert_eq!(cm.recently_failed_addrs.lock().failure_count(&addr), 1);
    }

    #[test]
    fn test_cleanup_stale_failed_addrs_respects_adaptive_ttl() {
        let cm = make_connection_manager(Some(make_addr(9300)), 1, 10, false);
        let addr_once = make_addr(9301); // 1 failure → expires soon
        let addr_many = make_addr(9302); // 4 failures → expires much later

        cm.record_failed_addr(addr_once);
        for _ in 0..4 {
            cm.record_failed_addr(addr_many);
        }

        // Simulate time passage: force addr_once's retry_after into the past
        // while leaving addr_many's retry_after far in the future.
        {
            let mut tracker = cm.recently_failed_addrs.lock();
            tracker.set_retry_after(&addr_once, Instant::now() - Duration::from_secs(1));
            // addr_many's retry_after remains its original future instant
        }

        let removed = cm.cleanup_stale_failed_addrs();
        assert_eq!(removed, 1, "only the single-failure entry should expire");

        let remaining = cm.recently_failed_addrs();
        assert!(!remaining.contains(&addr_once));
        assert!(remaining.contains(&addr_many));
    }

    #[test]
    fn test_recently_failed_addrs_stable_across_consecutive_calls() {
        // Core invariant this PR establishes: jitter is applied once at record_failed_addr
        // time, so two consecutive calls to recently_failed_addrs() within the same
        // millisecond always agree on which addresses are blocked.
        let cm = make_connection_manager(Some(make_addr(9400)), 1, 10, false);
        let addr = make_addr(9401);

        cm.record_failed_addr(addr);

        let first = cm.recently_failed_addrs();
        let second = cm.recently_failed_addrs();

        assert_eq!(
            first, second,
            "recently_failed_addrs must return identical results on consecutive calls"
        );
        assert!(first.contains(&addr));
    }

    #[test]
    fn test_acceptor_reliability_scoring() {
        // Reliability scoring should give lower scores to peers with more failures
        // and higher scores to peers with more successes. No cap — all peers remain
        // routable, just with different weights.
        let own_addr: SocketAddr = "10.0.0.100:9000".parse().unwrap();
        let cm = make_connection_manager(Some(own_addr), 1, 200, true);
        let now = Instant::now();

        let good_addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();
        let bad_addr: SocketAddr = "10.0.0.2:9000".parse().unwrap();
        let unknown_addr: SocketAddr = "10.0.0.3:9000".parse().unwrap();

        // Good peer: 8 successes, 2 failures
        for _ in 0..8 {
            cm.record_acceptor_outcome(good_addr, true, now);
        }
        for _ in 0..2 {
            cm.record_acceptor_outcome(good_addr, false, now);
        }

        // Bad peer: 1 success, 9 failures
        cm.record_acceptor_outcome(bad_addr, true, now);
        for _ in 0..9 {
            cm.record_acceptor_outcome(bad_addr, false, now);
        }

        let good_score = cm.peer_acceptor_reliability(good_addr, now);
        let bad_score = cm.peer_acceptor_reliability(bad_addr, now);
        let unknown_score = cm.peer_acceptor_reliability(unknown_addr, now);

        // Good peer: (8+1)/(10+2) = 0.75
        assert!(
            (good_score - 0.75).abs() < 0.01,
            "good peer should have ~0.75 reliability, got {}",
            good_score
        );
        // Bad peer: (1+1)/(10+2) ≈ 0.167
        assert!(
            (bad_score - 1.0 / 6.0).abs() < 0.01,
            "bad peer should have ~0.167 reliability, got {}",
            bad_score
        );
        // Unknown peer: 0.5 prior
        assert!(
            (unknown_score - 0.5).abs() < f64::EPSILON,
            "unknown peer should have 0.5 reliability, got {}",
            unknown_score
        );
        // Ordering: good > unknown > bad
        assert!(
            good_score > unknown_score,
            "good peer should be more reliable than unknown"
        );
        assert!(
            unknown_score > bad_score,
            "unknown peer should be more reliable than bad peer"
        );
    }

    #[test]
    fn test_acceptor_reliability_failure_only_peers() {
        // Peers with only failures (no successes) should score below the 0.5
        // unknown prior, with more failures producing lower scores.
        let own_addr: SocketAddr = "10.0.0.100:9000".parse().unwrap();
        let cm = make_connection_manager(Some(own_addr), 1, 200, true);
        let now = Instant::now();

        let high_failure_addr: SocketAddr = "10.0.0.1:9000".parse().unwrap();
        let low_failure_addr: SocketAddr = "10.0.0.2:9000".parse().unwrap();

        // High failure: 0 successes, 10 failures → (0+1)/(10+2) ≈ 0.083
        for _ in 0..10 {
            cm.record_acceptor_outcome(high_failure_addr, false, now);
        }
        // Low failure: 0 successes, 3 failures → (0+1)/(3+2) = 0.2
        for _ in 0..3 {
            cm.record_acceptor_outcome(low_failure_addr, false, now);
        }

        let high_score = cm.peer_acceptor_reliability(high_failure_addr, now);
        let low_score = cm.peer_acceptor_reliability(low_failure_addr, now);

        assert!(
            low_score > high_score,
            "peer with fewer failures ({}) should have higher reliability than peer with more ({})",
            low_score,
            high_score
        );
        // Both should be below the unknown prior of 0.5
        assert!(high_score < 0.5, "high-failure peer should be below prior");
        assert!(low_score < 0.5, "low-failure peer should be below prior");
    }

    #[test]
    fn test_acceptor_reliability_ttl_expiry() {
        let cm = crate::ring::ConnectionManager::test_default();
        let addr: SocketAddr = "10.0.0.7:9000".parse().unwrap();
        let now = Instant::now();

        // Record failures
        for _ in 0..5 {
            cm.record_acceptor_outcome(addr, false, now);
        }
        let score = cm.peer_acceptor_reliability(addr, now);
        assert!(score < 0.3, "5 failures should give low score, got {score}");

        // After TTL expires, should return to prior
        let expired = now + ConnectionManager::ACCEPTOR_STATS_TTL + Duration::from_secs(1);
        let score = cm.peer_acceptor_reliability(addr, expired);
        assert!(
            (score - 0.5).abs() < f64::EPSILON,
            "expired stats should return 0.5 prior, got {score}"
        );

        // Cleanup should remove the entry
        cm.cleanup_expired_acceptor_stats(expired);
        // Recording again should start fresh
        cm.record_acceptor_outcome(addr, true, expired);
        let score = cm.peer_acceptor_reliability(addr, expired);
        // 1 success, 0 failures: (1+1)/(1+2) = 0.667
        assert!(
            (score - 2.0 / 3.0).abs() < 0.01,
            "fresh entry after cleanup should reflect new data, got {score}"
        );
    }
}
