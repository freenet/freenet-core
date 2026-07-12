//! Ring protocol logic and supporting types.
//!
//! Mainly maintains a healthy and optimal pool of connections to other peers in the network
//! and routes requests to the optimal peers.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Weak, atomic::AtomicU64};
use std::time::Duration;
// Intentional wall-clock type for suspend/resume detection in
// `connection_maintenance`. See `classify_suspend_jump` for why the DST
// `TimeSource` abstraction is the *wrong* primitive here (it returns
// simulation time, which would never reflect a real OS suspend). This is
// the one call site in `crates/core/` that needs a real monotonic wall
// clock; renaming sidesteps the crates/core/ DST rule-lint grep while
// keeping intent explicit to the reader.
use std::time::Instant as WallClockInstant;
use tokio::time::Instant;

use tracing::Instrument;

use either::Either;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

pub use hosting::{
    AddClientSubscriptionResult, ClientDisconnectResult, SubscribeResult,
    SubscribedContractSnapshot,
};

use crate::message::TransactionType;
use crate::topology::TopologyAdjustment;
use crate::topology::rate::Rate;
use crate::tracing::{NetEventLog, NetEventRegister};

use crate::transport::TransportPublicKey;
use crate::util::{Contains, time_source::InstantTimeSrc};
use crate::{
    config::{GlobalExecutor, GlobalRng, OPERATION_TTL},
    message::Transaction,
    node::{self, EventLoopNotificationsSender, NodeConfig, OpManager, PeerId},
    router::Router,
};

// Issue #4350 invariants, pinned at compile time so a future edit to any of the
// renewal-timing constants can't silently reopen the discarded-reply bug. The
// outer cancel deadline is defined in `renewal_outer_cancel` as exactly
// `RENEWAL_TASK_BUDGET + NOTIFICATION_SEND_TIMEOUT + RENEWAL_OUTER_DEADLINE_MARGIN`,
// so it clears the driver's worst case (a full-budget slow attempt plus a fully
// backpressured `release_pending_op_slot` cleanup) by exactly the margin. These
// asserts pin the surrounding relationships that definition depends on.
const _: () = {
    let budget = Ring::RENEWAL_TASK_BUDGET.as_secs();
    // The cleanup-headroom margin must be non-zero: the outer cancel
    // (= budget + cleanup + margin) has to be STRICTLY greater than the
    // worst-case self-termination time (budget + cleanup), or it could fire
    // exactly as cleanup completes (issue #4350, Codex review).
    assert!(
        Ring::RENEWAL_OUTER_DEADLINE_MARGIN.as_secs() > 0,
        "RENEWAL_OUTER_DEADLINE_MARGIN must be > 0 so the outer cancel strictly \
         exceeds BUDGET + cleanup timeout (issue #4350)"
    );
    // A renewal must be allowed at least one real attempt: the no-start floor
    // has to sit below the total budget.
    assert!(
        Ring::RENEWAL_MIN_ATTEMPT_BUDGET.as_secs() < budget,
        "RENEWAL_MIN_ATTEMPT_BUDGET must be < RENEWAL_TASK_BUDGET (issue #4350)"
    );
    // No single attempt may exceed the total budget.
    assert!(
        Ring::RENEWAL_PER_ATTEMPT_TIMEOUT.as_secs() <= budget,
        "RENEWAL_PER_ATTEMPT_TIMEOUT must be <= RENEWAL_TASK_BUDGET (issue #4350)"
    );
    // The per-attempt renewal wait stays below the global operation TTL so a
    // renewal never out-waits a normal client subscribe attempt.
    assert!(
        Ring::RENEWAL_PER_ATTEMPT_TIMEOUT.as_secs() < OPERATION_TTL.as_secs(),
        "RENEWAL_PER_ATTEMPT_TIMEOUT must be < OPERATION_TTL (issue #4350)"
    );
};

mod broken_invariants;
mod connection_backoff;
mod connection_manager;
pub(crate) mod contract_ban_list;
pub(crate) use connection_manager::ConnectionManager;
// Nearest-neighbor ring lattice (successor+predecessor base edges).
// `nn_lattice_active_for` is the full activation gate — the flag AND the
// minimum-degree floor (`NN_LATTICE_MIN_MAX_CONNECTIONS`) — used by the
// free-function retention path in `crate::topology`; the acceptance clause and
// discovery probe apply the same gate via `ConnectionManager::nn_lattice_active`.
// The setter is `pub` so `dev_tool` can flip the stock/fix arm from the
// findability validation harness; it is a no-op-in-production, test-only override
// (see the module docs).
pub(crate) use connection_manager::nn_lattice_active_for;
#[cfg(any(test, feature = "testing"))]
pub use connection_manager::{set_nn_lattice_enabled, set_nn_lattice_force_active};
mod connection;
mod hosting;
pub(crate) use broken_invariants::{BrokenInvariant, BrokenInvariantsTracker};
/// Pre-write admission-gate rejection type (#4683, PR 3). Surfaced to the
/// executor chokepoints so a write that would overflow the aggregate disk
/// budget is refused before any bytes land.
pub(crate) use hosting::DiskBudgetExceeded;
/// The pre-A2 flat 1 GiB budget, used as the upgrade-migration sentinel in
/// `config::ConfigArgs::build` so an upgraded node re-derives its hosting budget
/// instead of keeping the historically-pinned default (#4565).
pub(crate) use hosting::LEGACY_FLAT_HOSTING_BUDGET_BYTES;
/// Single source of truth for the default hosted-contract-state budget.
/// `config::default_max_hosting_storage()` resolves to this so the
/// operator-facing default and the in-code fallback can never drift. The
/// default is RAM-scaled (capability-relative, A2) rather than a flat constant.
pub(crate) use hosting::default_hosting_budget_bytes;
/// Re-export the reconcile controller (pure decision core, its input/action
/// types, and the shadow-mode set-membership comparator) so the node layer can
/// build inputs and run the shadow compare (keystone step-2, #4642).
pub(crate) use hosting::reconcile;
pub use hosting::{AccessType, RecordAccessResult};
/// Aggregate disk-budget defaults (#4683). `config` resolves the persisted
/// `hosting-disk-pct` / `max-hosting-disk` defaults from these so the operator-
/// facing defaults and the in-code sizing math share one source of truth.
pub(crate) use hosting::{DEFAULT_HOSTING_DISK_PCT, DEFAULT_MAX_HOSTING_DISK_BYTES};
/// Clamp bounds re-exported only for the config-default round-trip test.
#[cfg(test)]
pub(crate) use hosting::{MAX_DEFAULT_HOSTING_BUDGET_BYTES, MIN_DEFAULT_HOSTING_BUDGET_BYTES};
pub mod interest;
mod live_tx;
mod location;
pub(crate) mod peer_cache;
mod peer_connection_backoff;
mod peer_key_location;
mod placement_migration_metrics;
pub mod topology_registry;
pub(crate) mod update_rate_limit;

// GET auto-subscribe (`AUTO_SUBSCRIBE_ON_GET`) was REMOVED in piece E of the
// demand-driven hosting redesign (docs/design/demand-driven-hosting.md §9,
// .claude/rules/hosting-invariants.md anti-patterns table). Auto-installing a
// durable subscription on every GET manufactures demand that no client asked
// for — the "GET-auto-subscribe" half of the relay-caching anti-pattern. A GET
// may still host on the return path under the demand gauge (evictable,
// non-durable; see `cache_contract_locally`), and a client that wants ongoing
// freshness sets `subscribe=true` explicitly. do NOT re-add auto-subscribe on
// GET — see hosting-invariants (invariants 1 & 2).

/// Per-beneficiary weight for a local-client subscription when
/// computing the LIVE benefit snapshot each governance reaper tick.
/// Strong signal: a real user on this node is currently subscribed.
/// Hard to fake without running an actual freenet client locally.
///
/// Sourced from the design doc — see "Sybil weighting falls out
/// naturally" in `docs/design/contract-hardening.md`.
const LOCAL_DEMAND_WEIGHT: f64 = 1.0;

/// Per-beneficiary weight for a downstream peer's CURRENT subscription
/// when computing the LIVE benefit snapshot. Weaker signal because peer
/// identity is attacker-rotatable — without an identity layer, a single
/// attacker can spin up many peers and each "subscribes." We weight
/// each forwarded subscriber at 0.1 so an attacker would need 10
/// rotating peers to fake the standing demand of one local user.
const FORWARDED_DEMAND_WEIGHT: f64 = 0.1;

/// Interval between governance reaper ticks. Each tick applies
/// decay and runs MAD-based outlier detection across the population.
/// One minute balances responsiveness (a sustained-cost contract is
/// flagged within a minute or two of crossing the threshold) against
/// CPU overhead (MAD over the entire contract set every tick).
const GOVERNANCE_TICK_INTERVAL: Duration = Duration::from_secs(60);

use connection_backoff::ConnectionBackoff;
pub use connection_backoff::ConnectionFailureReason;
pub(crate) use peer_connection_backoff::PeerConnectionBackoff;

pub use self::live_tx::LiveTransactionTracker;
pub use connection::Connection;
pub use interest::PeerKey;
pub use location::{Distance, Location};
pub use peer_key_location::{KnownPeerKeyLocation, PeerAddr, PeerKeyLocation};

/// Thread safe and friendly data structure to keep track of the local knowledge
/// of the state of the ring.
///
// Note: For now internally we wrap some of the types internally with locks and/or use
// multithreaded maps. In the future if performance requires it some of this can be moved
// towards a more lock-free multithreading model if necessary.
/// Backoff state for contract-directed CONNECT attempts.
struct ContractConnectState {
    current_backoff: Duration,
    last_attempt: Instant,
}

pub(crate) struct Ring {
    pub max_hops_to_live: usize,
    pub connection_manager: ConnectionManager,
    pub router: Arc<RwLock<Router>>,
    pub live_tx_tracker: LiveTransactionTracker,
    hosting_manager: hosting::HostingManager,
    /// Per-contract record of detected CRDT-invariant violations (e.g. a
    /// non-idempotent `update_state`). Used to gate outbound broadcast
    /// so a broken contract's state changes don't leave the node.
    broken_invariants: BrokenInvariantsTracker,
    /// Per-contract governance scoring + reaper. Routes every
    /// per-contract resource report through `ingest_cost` so the
    /// governance state for a contract reflects what the meter
    /// already records. Mode defaults to `DryRun` per the staged-
    /// rollout plan.
    pub(crate) governance: Arc<crate::contract::governance::GovernanceManager>,
    /// Front-line per-(sender, contract) UPDATE rate limit. Catches
    /// flood patterns at the receive boundary in milliseconds, before
    /// the slower MAD-based governance reaper can react. See
    /// `crate::ring::update_rate_limit` and `docs/design/contract-hardening.md`
    /// Phase 2.
    pub(crate) update_rate_limiter: Arc<update_rate_limit::UpdateRateLimiter>,
    /// Per-contract ban list. Populated by the governance reaper on
    /// `BanTriggered` / `BanLifted` transitions; consulted at the
    /// inbound dispatch site to drop wire requests for banned
    /// contracts. Phase 7 of the contract-hardening plan
    /// (`docs/design/contract-hardening.md`) and the auto-detection
    /// half of issue #4274 (operator-CLI blocklist).
    pub(crate) contract_ban_list: Arc<contract_ban_list::ContractBanList>,
    event_register: Box<dyn NetEventRegister>,
    op_manager: RwLock<Option<Weak<OpManager>>>,
    /// Whether this peer is a gateway or not. This will affect behavior of the node when acquiring
    /// and dropping connections.
    pub(crate) is_gateway: bool,
    /// Shared connection backoff tracker for all connection failure types.
    connection_backoff: Arc<parking_lot::Mutex<ConnectionBackoff>>,
    /// Per-contract backoff for contract-directed CONNECT attempts.
    contract_connect_backoff: Mutex<HashMap<ContractKey, ContractConnectState>>,
    /// Injectable time source used by `connection_maintenance`. Using `util::TimeSource`
    /// (which returns `tokio::time::Instant`) lets tests supply `SharedMockTimeSource` for
    /// fine-grained control without pausing the entire tokio runtime.
    pub(crate) time_source: Arc<dyn crate::util::time_source::TimeSource + Send + Sync>,
    /// Directory for persisting the peer address cache. When set, the peer cache
    /// is periodically saved here and loaded on startup for fast reconnection.
    pub(crate) peer_cache_dir: Option<std::path::PathBuf>,
    /// Per-node compiled-WASM module-cache occupancy + eviction telemetry
    /// (#4440). Constructed once here and threaded as an `Arc`: the
    /// `RuntimePool` clones it (via `op_manager.ring`) into its labeled module
    /// caches, which *publish* into it; the `emit_router_snapshot_telemetry`
    /// task *reads* it. Threading the `Arc` (rather than a process-global)
    /// keeps the gauges per-node so unit tests stay isolated (#4488).
    module_cache_metrics: Arc<crate::wasm_runtime::ModuleCacheMetrics>,
    /// Per-node placement-migration activity counters (#4404 follow-up).
    /// Constructed once here and shared via `Arc`: the migration SEND site
    /// (`p2p_protoc::migration`) and the two RECEIVE sites (`node::process_message`)
    /// increment it through `op_manager.ring`, while
    /// `emit_router_snapshot_telemetry` reads it on the snapshot cadence. Threading
    /// the `Arc` (rather than a process-global) keeps the counters per-node so unit
    /// tests stay isolated. Together with the placement-quality gauge it lets us
    /// observe whether the placement migration is firing and whether it actually
    /// pulls hosting closer to each contract's key.
    placement_migration_metrics: Arc<placement_migration_metrics::PlacementMigrationMetrics>,
    /// Shutdown signal for the long-lived background tasks spawned in
    /// [`Ring::new`]. Triggered once on node teardown (via
    /// [`Ring::trigger_shutdown`], fired from `ShutdownTeardown::drop`).
    ///
    /// Every long sleep / `interval.tick()` in those loops races this token
    /// via `tokio::select!` so a shutdown returns promptly instead of waiting
    /// for the longest outstanding sleep to elapse (up to 5 minutes for
    /// `interest_heartbeat`). See issue #4278 and
    /// `.claude/rules/code-style.md` ("Backoff sleeps MUST be interruptible").
    ///
    /// `CancellationToken` is level-triggered: once cancelled, every present
    /// and future `cancelled()` await returns immediately, so there is no
    /// missed-wakeup race for a task that is between sleeps when shutdown
    /// fires.
    shutdown: CancellationToken,
}

// /// A data type that represents the fact that a peer has been blacklisted
// /// for some action. Has to be coupled with that action
// #[derive(Debug)]
// struct Blacklisted {
//     since: Instant,
//     peer: PeerKey,
// }

/// Guard that ensures `complete_subscription_request` is called even if the
/// subscription task panics. This prevents contracts from being stuck in
/// `pending_subscription_requests` forever.
pub(crate) struct SubscriptionRecoveryGuard {
    op_manager: Arc<OpManager>,
    contract_key: ContractKey,
    completed: bool,
}

impl SubscriptionRecoveryGuard {
    pub(crate) fn new(op_manager: Arc<OpManager>, contract_key: ContractKey) -> Self {
        Self {
            op_manager,
            contract_key,
            completed: false,
        }
    }

    pub(crate) fn complete(mut self, success: bool) {
        self.op_manager
            .ring
            .complete_subscription_request(&self.contract_key, success);
        self.completed = true;
    }
}

impl Drop for SubscriptionRecoveryGuard {
    fn drop(&mut self) {
        if !self.completed {
            // Task panicked or was cancelled before completion - treat as failure
            tracing::warn!(
                contract = %self.contract_key,
                "Subscription recovery task terminated unexpectedly, marking as failed"
            );
            self.op_manager
                .ring
                .complete_subscription_request(&self.contract_key, false);
        }
    }
}

/// State of the `Ring -> OpManager` weak back-reference, as observed by
/// the `connection_maintenance` loop.
///
/// The startup-vs-shutdown distinction is the whole point: a `None` slot
/// (not attached yet) must keep waiting, while a `Some(weak)` that no longer
/// upgrades (attached then dropped) must terminate the loop. Conflating the
/// two leaves the maintenance task spinning forever on a dead `Weak` —
/// the #3308 zombie.
enum OpManagerState<T> {
    /// `attach_op_manager` has not run yet — normal startup window. The
    /// maintenance loop should keep waiting for attachment.
    NotAttached,
    /// The `OpManager` is alive and was successfully upgraded.
    Live(Arc<T>),
    /// The back-reference was attached once but the owning `Arc` has since
    /// been dropped. Nothing re-attaches it, so the loop owner is dead and
    /// the maintenance task must terminate instead of spinning on a `Weak`
    /// that can never upgrade again (#3308).
    Detached,
}

/// Classify a `RwLock<Option<Weak<T>>>` back-reference into the three states
/// the maintenance loop cares about. Generic over `T` so it can be
/// unit-tested with a stand-in `Arc` instead of a full `OpManager` (#3308).
fn classify_op_manager_ref<T>(slot: &RwLock<Option<Weak<T>>>) -> OpManagerState<T> {
    let upgraded = slot.read().as_ref().map(|weak| weak.clone().upgrade());
    match upgraded {
        None => OpManagerState::NotAttached,
        Some(Some(strong)) => OpManagerState::Live(strong),
        Some(None) => OpManagerState::Detached,
    }
}

/// Pure core of [`Ring::is_subscription_root`]'s neighbor scan: is there NO
/// connected neighbor that is both *routable* (a renewal could route to it) and
/// strictly closer to the contract than this node?
///
/// `my_distance` is this node's ring distance to the contract. Each neighbor is
/// `(its location, is it routable)`:
/// - A neighbor with no known location can't be compared, so it never makes us
///   "not the root" on distance grounds (it is skipped).
/// - A non-routable neighbor (transient or not-yet-ready) is skipped, mirroring
///   the eligibility `k_closest_potentially_hosting` applies — so the predicate
///   agrees with where a renewal would actually route (#4440).
///
/// Returns `true` when no routable neighbor is strictly closer (this node is the
/// effective terminus), `false` otherwise. Factored out as a pure function so the
/// distance/eligibility logic has direct unit coverage without a full `Ring`.
fn no_closer_routable_neighbor(
    my_distance: Distance,
    contract_location: Location,
    neighbors: impl Iterator<Item = (Option<Location>, bool)>,
) -> bool {
    for (peer_loc, routable) in neighbors {
        if !routable {
            continue;
        }
        if let Some(peer_loc) = peer_loc {
            if peer_loc.distance(contract_location) < my_distance {
                return false;
            }
        }
    }
    true
}

/// Pure core of [`Ring::most_keyward_hosting_neighbor`]: from candidate hosting
/// neighbors already paired with their ring distance to the contract, pick the
/// one STRICTLY closer to the contract than `my_distance` (the most-keyward
/// host), breaking ties on equal distance by ascending socket address so the
/// choice is deterministic (design §6 point 2 total order). The
/// `most_keyward_hosting_neighbor` caller pre-filters candidates through
/// `pkl.location()?`, which requires a resolvable socket address, so no
/// addressless candidate ever reaches this helper and every item here carries a
/// concrete distance and a concrete address for the tiebreak.
///
/// Returns `None` when no candidate is strictly closer than us — either the
/// candidate set is empty or every candidate is farther-or-equal (so we are the
/// terminus / a stranded host; the caller distinguishes those). The strict `<`
/// is the acyclicity guarantee: a peer at exactly our distance never becomes our
/// upstream. Factored out so the strict-closer + deterministic-tiebreak
/// selection has direct unit coverage without a heavyweight async `Ring`
/// fixture, mirroring [`no_closer_routable_neighbor`].
///
/// This is the computed-upstream primitive of the demand-driven-hosting redesign
/// (#4642 piece D): the eventual replacement for the stored `is_upstream`
/// interest flag, which drifts under gossip (#4671). It is introduced here
/// behavior-preservingly — computed alongside the stored flag for divergence
/// telemetry — ahead of the reconcile-core keystone that computes upstream
/// everywhere.
fn most_keyward_among(
    my_distance: Distance,
    candidates: impl Iterator<Item = (PeerKeyLocation, Distance)>,
) -> Option<PeerKeyLocation> {
    candidates
        .filter(|(_, dist)| *dist < my_distance)
        .min_by(|(a, ad), (b, bd)| {
            ad.cmp(bd)
                .then_with(|| a.socket_addr().cmp(&b.socket_addr()))
        })
        .map(|(pkl, _)| pkl)
}

// NOTE (#4642 keystone sub-task 3, "the flip"): the RENEWAL-site shadow helpers
// (`renewal_shadow_actual` + `record_renewal_shadow`) were REMOVED when the
// renewal site was flipped from record-only shadow to actually DRIVING the
// controller's decision. The drive lives in `OpManager::reconcile_wants_renewal`
// (built from a fresh at-emission snapshot) and is consulted inline in the
// renewal loop; the RENEWAL reconcile counter now measures the flip's
// interest-gate suppression rate rather than a shadow divergence.

/// Race an `.await` point in a background loop against the Ring shutdown
/// token. Returns `true` if shutdown fired (the caller should stop its loop)
/// and `false` if the wrapped future completed first (carry on).
///
/// This is the single chokepoint that makes every long sleep / `interval.tick()`
/// in the [`Ring::new`] background tasks interruptible (issue #4278). Keeping it
/// in one place means a new loop only has to call this helper to satisfy the
/// `.claude/rules/code-style.md` "backoff sleeps MUST be interruptible" rule.
///
/// `CancellationToken::cancelled` is cancellation-safe and level-triggered, so
/// wrapping it in `select!` here introduces no missed-wakeup window: if the
/// token is already cancelled when this is called, the shutdown arm wins
/// immediately.
///
/// **`biased;` justification** (per `.claude/rules/code-style.md`):
/// - *Why biased:* shutdown must deterministically win when both the timer and
///   the token are ready in the same poll, so a teardown is never delayed by an
///   extra sleep cycle.
/// - *Starvation:* none. The non-shutdown arm is a one-shot timer (`sleep` /
///   `interval.tick()`), not a high-throughput channel, and the shutdown arm is
///   terminal (the caller breaks its loop on `true`). No hot arm exists for the
///   bias to starve, so no per-iteration cap is needed.
/// - *Cancellation safety:* both arms are cancellation-safe — dropping the
///   loser (a timer future or `cancelled()`) discards no work.
async fn sleep_or_shutdown<F>(shutdown: &CancellationToken, wait: F) -> bool
where
    F: std::future::Future<Output = ()>,
{
    tokio::select! {
        biased;
        _ = shutdown.cancelled() => true,
        _ = wait => false,
    }
}

/// Result of pruning a connection.
#[derive(Debug, Default)]
pub struct PruneConnectionResult {
    /// Orphaned transactions that need to be retried or failed.
    pub orphaned_transactions: Vec<Transaction>,
    /// True if this prune caused us to drop below the readiness threshold.
    pub became_unready: bool,
}

impl Ring {
    pub const DEFAULT_MIN_CONNECTIONS: usize = 25;

    pub const DEFAULT_MAX_CONNECTIONS: usize = 200;

    const DEFAULT_MAX_UPSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    const DEFAULT_MAX_DOWNSTREAM_BANDWIDTH: Rate = Rate::new_per_second(1_000_000.0);

    /// Above this number of remaining hops, randomize which node a message which be forwarded to.
    const DEFAULT_RAND_WALK_ABOVE_HTL: usize = 7;

    /// Max hops to be performed for certain operations (e.g. propagating connection of a peer in the network).
    pub const DEFAULT_MAX_HOPS_TO_LIVE: usize = 10;

    pub fn new<ER: NetEventRegister + Clone>(
        config: &NodeConfig,
        event_loop_notifier: EventLoopNotificationsSender,
        event_register: ER,
        is_gateway: bool,
        connection_manager: ConnectionManager,
        task_monitor: &crate::node::background_task_monitor::BackgroundTaskMonitor,
    ) -> anyhow::Result<Arc<Self>> {
        let live_tx_tracker = LiveTransactionTracker::new();

        let max_hops_to_live = if let Some(v) = config.max_hops_to_live {
            v
        } else {
            Self::DEFAULT_MAX_HOPS_TO_LIVE
        };

        // Single shutdown signal for every long-lived background task spawned
        // below. Created up front so `refresh_router` (spawned before the
        // `Ring` struct literal exists) shares the same token that gets moved
        // into the struct. See issue #4278.
        let shutdown = CancellationToken::new();

        let router = Arc::new(RwLock::new(Router::new(&[])));
        crate::node::network_status::set_router(router.clone());
        task_monitor.register(
            "refresh_router",
            GlobalExecutor::spawn(Self::refresh_router(
                router.clone(),
                event_register.clone(),
                shutdown.clone(),
            )),
        );

        // Interval for topology snapshot registration (1 second in test mode)
        // Registers subscription topology with the global registry for validation
        #[cfg(any(test, feature = "testing"))]
        const TOPOLOGY_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

        // Just initialize with a fake location, this will be later updated when the peer has an actual location assigned.
        let peer_cache_dir = if is_gateway {
            // Gateways don't need peer cache — peers connect to them.
            None
        } else {
            Some(config.config.data_dir())
        };
        let time_source: Arc<dyn crate::util::time_source::TimeSource + Send + Sync> =
            Arc::new(InstantTimeSrc::new());
        // Production always passes `None`, which resolves to
        // `GovernanceConfig::default()`. The override exists only so
        // simulation tests can inject compressed timescales + lower
        // `min_samples` to drive the rate-limit → MAD → evict → ban
        // chain within a paused-time sim. See issue #4301.
        let governance_config = config
            .governance_config_override
            .clone()
            .unwrap_or_default();
        let governance = Arc::new(crate::contract::governance::GovernanceManager::new(
            governance_config,
            time_source.clone(),
        ));
        let ring = Ring {
            max_hops_to_live,
            router,
            connection_manager,
            // Production passes the Ring's default `Arc<InstantTimeSrc>`
            // (wall clock). Simulation tests can inject a controllable clock via
            // `NodeConfig::hosting_time_source_override` so hosting-cache TTL /
            // eviction is deterministic (#4642 piece A). Same `Arc` clone as the
            // rest of the Ring when no override is set.
            hosting_manager: hosting::HostingManager::with_time_source(
                config.config.max_hosting_storage,
                config
                    .hosting_time_source_override
                    .clone()
                    .unwrap_or_else(|| time_source.clone()),
            ),
            broken_invariants: BrokenInvariantsTracker::new(time_source.clone()),
            governance,
            update_rate_limiter: Arc::new(update_rate_limit::UpdateRateLimiter::new(
                time_source.clone(),
            )),
            contract_ban_list: Arc::new(contract_ban_list::ContractBanList::new(
                time_source.clone(),
            )),
            live_tx_tracker: live_tx_tracker.clone(),
            event_register: Box::new(event_register),
            op_manager: RwLock::new(None),
            is_gateway,
            connection_backoff: Arc::new(Mutex::new(ConnectionBackoff::new())),
            contract_connect_backoff: Mutex::new(HashMap::new()),
            time_source,
            peer_cache_dir,
            // One sink per node, shared with the module caches via the `Arc`
            // (the `RuntimePool` reaches it through `op_manager.ring`). See
            // the field docs and #4488.
            module_cache_metrics: Arc::new(crate::wasm_runtime::ModuleCacheMetrics::new()),
            // One placement-migration counter sink per node, shared with the
            // migration send/receive sites via the `Arc` (reached through
            // `op_manager.ring`). See the field docs.
            placement_migration_metrics: Arc::new(
                placement_migration_metrics::PlacementMigrationMetrics::default(),
            ),
            shutdown,
        };

        if let Some(loc) = config.location {
            if config.own_addr.is_none() && is_gateway {
                return Err(anyhow::anyhow!("own_addr is required for gateways"));
            }
            ring.connection_manager.update_location(Some(loc));
        }

        let ring = Arc::new(ring);
        let current_span = tracing::Span::current();
        let span = if current_span.is_none() {
            tracing::info_span!("connection_maintenance")
        } else {
            tracing::info_span!(parent: current_span, "connection_maintenance")
        };

        task_monitor.register(
            "connection_maintenance",
            GlobalExecutor::spawn({
                let fut = ring
                    .clone()
                    .connection_maintenance(event_loop_notifier, live_tx_tracker)
                    .instrument(span);
                async move {
                    if let Err(e) = fut.await {
                        tracing::error!(error = %e, "connection_maintenance exited with error");
                    }
                }
            }),
        );

        // Spawn periodic subscription state telemetry task
        task_monitor.register(
            "emit_subscription_state_telemetry",
            GlobalExecutor::spawn(Self::emit_subscription_state_telemetry(
                ring.clone(),
                Self::SUBSCRIPTION_STATE_INTERVAL,
            )),
        );

        // Spawn periodic subscription recovery task to fix "orphaned hosters"
        // (peers that have contracts cached but aren't in the subscription tree)
        task_monitor.register(
            "recover_orphaned_subscriptions",
            GlobalExecutor::spawn(Self::recover_orphaned_subscriptions(
                ring.clone(),
                Self::SUBSCRIPTION_RECOVERY_INTERVAL,
            )),
        );

        // Spawn periodic GET subscription cache sweep task
        // Cleans up expired GET-triggered subscriptions to maintain bounded memory
        task_monitor.register(
            "sweep_get_subscription_cache",
            GlobalExecutor::spawn(Self::sweep_get_subscription_cache(
                ring.clone(),
                Self::GET_SUBSCRIPTION_SWEEP_INTERVAL,
            )),
        );

        // Spawn periodic topology snapshot registration task (test mode only)
        // This allows SimNetwork to validate subscription topology during tests
        #[cfg(any(test, feature = "testing"))]
        task_monitor.register(
            "register_topology_snapshots",
            GlobalExecutor::spawn(Self::register_topology_snapshots_periodically(
                ring.clone(),
                TOPOLOGY_SNAPSHOT_INTERVAL,
            )),
        );

        // Spawn periodic router model snapshot telemetry (every 5 minutes)
        task_monitor.register(
            "emit_router_snapshot_telemetry",
            GlobalExecutor::spawn(Self::emit_router_snapshot_telemetry(
                ring.clone(),
                Duration::from_secs(60 * 5),
            )),
        );

        // Spawn periodic contract-directed CONNECT task.
        // When a peer is a "subscription root" (closest to contract among neighbors),
        // it sends CONNECTs toward the contract's ring location to merge disconnected
        // subscription subtrees.
        const CONTRACT_CONNECT_INTERVAL: Duration = Duration::from_secs(30);
        task_monitor.register(
            "contract_directed_connects",
            GlobalExecutor::spawn(Self::contract_directed_connects(
                ring.clone(),
                CONTRACT_CONNECT_INTERVAL,
            )),
        );

        // Spawn periodic interest heartbeat task.
        // Sends full Interests { hashes } to each connected peer to keep
        // interest entries alive and prevent the death spiral where expired
        // entries block broadcast delivery.
        task_monitor.register(
            "interest_heartbeat",
            GlobalExecutor::spawn(Self::interest_heartbeat(ring.clone())),
        );

        // NOTE: the advertisement-layer anti-entropy re-request (#4642 spec step 1,
        // "Fix 1") is NOT a separate task — it is piggybacked on the
        // `interest_heartbeat` loop above (see the `HostingStateRequest` send
        // there). A separate task with its own `GlobalRng` initial-delay draw
        // perturbed the shared per-thread seeded RNG stream that the deterministic
        // simulation harness depends on; riding the existing heartbeat loop adds no
        // RNG consumer and no second timer, so the sim stream is unchanged.

        // Spawn periodic governance reaper tick.
        // Computes per-contract state from accumulated cost/benefit
        // samples and logs `ReaperDecision`s. Mode defaults to `Off`
        // (see `GovernanceConfig` in contract/governance.rs): the MAD
        // outlier detector is dormant and being replaced by demand-driven
        // eviction (#4296, #4642), so this task is a no-op on default
        // nodes and only ever logs (never evicts) even when an operator
        // explicitly flips it to DryRun/Enforce. The dashboard reads the
        // governance snapshot independently of the tick; the tick is only
        // the engine that updates state.
        task_monitor.register(
            "governance_reaper",
            GlobalExecutor::spawn(Self::governance_reaper_loop(ring.clone())),
        );

        Ok(ring)
    }

    pub fn attach_op_manager(&self, op_manager: &Arc<OpManager>) {
        self.op_manager.write().replace(Arc::downgrade(op_manager));
    }

    /// Shared per-node module-cache telemetry sink (#4440 / #4488). The
    /// `RuntimePool` clones this into its labeled module caches (which publish
    /// occupancy/eviction into it) while the snapshot task reads it; threading
    /// this `Arc` is what replaced the old `MODULE_CACHE_METRICS` process-global.
    pub(crate) fn module_cache_metrics(&self) -> Arc<crate::wasm_runtime::ModuleCacheMetrics> {
        self.module_cache_metrics.clone()
    }

    /// Shared per-node placement-migration counter sink (#4404 follow-up). The
    /// migration send/receive sites clone this to increment `sent` / `received`
    /// / `acted`; the snapshot task reads it on the `router_snapshot` cadence.
    pub(crate) fn placement_migration_metrics(
        &self,
    ) -> Arc<placement_migration_metrics::PlacementMigrationMetrics> {
        self.placement_migration_metrics.clone()
    }

    /// Signal the long-lived background tasks (spawned in [`Ring::new`]) to
    /// shut down. Idempotent and thread-safe — calling it more than once, or
    /// from multiple threads, is a no-op after the first call.
    ///
    /// Fired from `ShutdownTeardown::drop` on every `run_node` exit path so a
    /// graceful (or error-triggered) shutdown doesn't wait for the longest
    /// outstanding sleep in those loops to elapse. See issue #4278.
    pub(crate) fn trigger_shutdown(&self) {
        self.shutdown.cancel();
    }

    /// A clone of the background-task shutdown token. Long-lived loops race
    /// their sleeps against [`CancellationToken::cancelled`] on this token via
    /// `tokio::select!`.
    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub(crate) fn upgrade_op_manager(&self) -> Option<Arc<OpManager>> {
        self.op_manager
            .read()
            .as_ref()
            .and_then(|weak| weak.clone().upgrade())
    }

    /// Classify the current state of the `OpManager` back-reference.
    ///
    /// The maintenance loop must distinguish "not attached yet" (normal
    /// startup window, before [`Ring::attach_op_manager`] runs) from
    /// "attached then dropped" (the node has been torn down and the
    /// `Arc<OpManager>` is gone). The first case must keep waiting; the
    /// second must terminate the loop so it doesn't become a zombie
    /// spinning forever on a `Weak` that can never upgrade again (#3308).
    ///
    /// This is the sole production instantiation of the generic
    /// [`classify_op_manager_ref`] (with `T = OpManager`); the unit tests
    /// instantiate it with a stand-in `Arc` to exercise the same logic.
    fn op_manager_state(&self) -> OpManagerState<OpManager> {
        classify_op_manager_ref(&self.op_manager)
    }

    pub fn is_gateway(&self) -> bool {
        self.is_gateway
    }

    pub fn open_connections(&self) -> usize {
        self.connection_manager.connection_count()
    }

    /// Record a connection failure to the backoff tracker.
    pub fn record_connection_failure(&self, target: Location, reason: ConnectionFailureReason) {
        let mut backoff = self.connection_backoff.lock();
        backoff.record_failure_with_reason(target, reason);
    }

    /// Record a short, non-escalating backoff for `target` (#4362).
    ///
    /// Used when a capacity `Rejected` arrives while the node is still below
    /// `min_connections`: we want to throttle the retry cadence briefly so it
    /// stops hammering a saturated gateway neighborhood every fast-tick,
    /// WITHOUT stamping the escalating 30s→600s backoff that would trap the
    /// under-connected node. See `ConnectionBackoff::record_short_reject_backoff`.
    pub fn record_connection_short_reject_backoff(&self, target: Location) {
        let mut backoff = self.connection_backoff.lock();
        backoff.record_short_reject_backoff(target);
    }

    /// Record a successful connection to clear backoff.
    pub fn record_connection_success(&self, target: Location) {
        let mut backoff = self.connection_backoff.lock();
        backoff.record_success(target);
    }

    /// Check if a target is currently in backoff.
    pub fn is_in_connection_backoff(&self, target: Location) -> bool {
        self.connection_backoff.lock().is_in_backoff(target)
    }

    /// Periodic cleanup of expired backoff entries.
    pub fn cleanup_connection_backoff(&self) {
        self.connection_backoff.lock().cleanup_expired();
    }

    /// Reset all connection backoff state. Used during isolation recovery
    /// when the node has had zero ring connections for an extended period.
    pub fn reset_all_connection_backoff(&self) {
        self.connection_backoff.lock().clear();
    }

    // ==================== Contract-Directed CONNECT ====================

    /// Initial backoff before retrying a contract-directed CONNECT.
    const INITIAL_CONTRACT_CONNECT_BACKOFF: Duration = Duration::from_secs(30);
    /// Maximum backoff cap for contract-directed CONNECTs (24 hours).
    const MAX_CONTRACT_CONNECT_BACKOFF: Duration = Duration::from_secs(24 * 60 * 60);
    /// Maximum contract-directed CONNECTs per cycle.
    const MAX_CONTRACT_CONNECTS_PER_CYCLE: usize = 2;

    /// If this peer is the body-holding subscription root for the contract
    /// identified by `instance_id`, returns the resolved [`ContractKey`];
    /// otherwise returns `None`.
    ///
    /// "Body-holding subscription root" means: this peer hosts the contract (has
    /// the body) AND no connected neighbor is closer to the contract's ring
    /// location than this peer — the "body-holding terminus" from the
    /// placement-migration design. Such a peer has no peer closer than itself to
    /// subscribe to, so a renewal toward the contract would dead-end and retry
    /// forever — the #4440 renewal storm. The renewal driver (which holds only
    /// the instance id) uses this to short-circuit (proposal 1); it returns the
    /// key so the caller can refresh the local lease without a second lookup.
    ///
    /// Resolves the hosted [`ContractKey`] by matching `instance_id` against the
    /// hosting set (a node hosts at most one contract per instance id, so the
    /// match is exact), then delegates to [`Self::is_subscription_root`], whose
    /// definition already requires `is_hosting_contract` (= has body) and
    /// closest-connected, so the two never disagree. Returns `None` when the
    /// contract is not hosted (no body → not a body-holding terminus).
    pub(crate) fn body_holding_subscription_root_key(
        &self,
        instance_id: &ContractInstanceId,
    ) -> Option<ContractKey> {
        // `hosting_contract_keys()` clones the hosting set and we linear-scan for
        // the matching instance id. This runs at most once per renewal task
        // (`SUBSCRIPTION_RECOVERY_INTERVAL` = 30 s, bounded by
        // `MAX_RECOVERY_ATTEMPTS_PER_INTERVAL` per cycle), so the clone+scan is
        // not on any hot path. A reverse instance-id → key index on the hosting
        // cache would avoid the clone if this ever becomes per-message.
        let key = self
            .hosting_manager
            .hosting_contract_keys()
            .into_iter()
            .find(|k| k.id() == instance_id)?;
        self.is_subscription_root(&key).then_some(key)
    }

    /// Record that a renewal short-circuited because this node is the
    /// body-holding subscription root for the contract (#4440 proposal 1).
    pub(crate) fn record_renewal_terminus_satisfied(&self) {
        self.placement_migration_metrics
            .record_renewal_terminus_satisfied();
    }

    /// Returns true if this peer is the closest to the contract among its connected neighbors
    /// (i.e., it's a subscription root for this contract).
    ///
    /// `pub(crate)` so the reconcile input-builder (keystone step-2, #4642) can
    /// populate `ReconcileInputs::is_verified_root`. Note the `is_hosting_contract`
    /// precondition below: a peer that does not yet hold the body is not reported
    /// as root (the builder inherits that binary hosting semantics).
    pub(crate) fn is_subscription_root(&self, contract_key: &ContractKey) -> bool {
        if !self.is_hosting_contract(contract_key) {
            return false;
        }
        let contract_location = Location::from(contract_key);
        let my_location = match self.connection_manager.own_location().location() {
            Some(loc) => loc,
            None => return false,
        };
        let my_distance = my_location.distance(contract_location);

        let connections = self.connection_manager.get_connections_by_location();
        let neighbors = connections.iter().flat_map(|(_loc, conns)| {
            conns.iter().map(|conn| {
                // A peer counts as a "closer routable neighbor" (so I'm NOT the
                // root) only if a renewal could actually route to it. Match the
                // ACTUAL eligibility `k_closest_potentially_hosting` applies, and
                // mind its asymmetry between the two filters:
                //
                //  * Transient peers (short-TTL CONNECT-coordination slots) are
                //    excluded UNCONDITIONALLY by `k_closest_potentially_hosting`
                //    (no fallback — see its `is_transient(addr)` /
                //    `skipped_transient` branch). So a transient closer neighbor
                //    is NOT a real route target — exclude it here too. Without
                //    this, a node whose only closer neighbor is transient would
                //    fail to recognise itself as the terminus, wire-renew,
                //    dead-end (the transient is excluded by `k_closest`), and
                //    storm (#4440).
                //
                //  * Not-yet-ready peers are excluded by `k_closest` ONLY when
                //    ready candidates exist; if there are none it FALLS BACK to
                //    the not-ready peers (its `not_ready_fallback` path). So a
                //    not-ready closer neighbor CAN still be a route target. Treat
                //    it as routable
                //    here (conservative): excluding it would wrongly classify the
                //    node as the root in early-startup / low-degree topologies and
                //    suppress a renewal that would in fact route to that closer
                //    peer, delaying upstream subscription propagation. The cost of
                //    treating a not-ready peer as routable is at most one extra
                //    wire renewal (re-evaluated next cycle once it is ready),
                //    which is strictly safer than suppressing a needed renewal.
                //
                // Addressless peers are treated as routable (conservative): they
                // bypass the addr-keyed filters in `k_closest` too, so a renewal
                // could still route to one.
                let routable = match conn.location.socket_addr() {
                    Some(addr) => !self.connection_manager.is_transient(addr),
                    None => true,
                };
                (conn.location.location(), routable)
            })
        });

        no_closer_routable_neighbor(my_distance, contract_location, neighbors)
    }

    /// Compute the CURRENT upstream for a contract this peer hosts: among
    /// `hosting_neighbors` (connected neighbors that have advertised hosting the
    /// contract, from `NeighborHostingManager::neighbors_with_contract_id`), the
    /// one CLOSEST to the contract's key that is STRICTLY closer to the key than
    /// this peer. Returns `None` when no such neighbor exists — either because
    /// this peer is the most-keyward host it can see (a terminus, design §5d-a)
    /// or because closer neighbors exist but none host the contract (a stranded
    /// host that must re-root, §5d-b). No current caller distinguishes those two
    /// `None` cases — this step is observation-only; a later keystone step will
    /// (via `is_subscription_root`).
    ///
    /// This is the *computed upstream* of the demand-driven-hosting design (§4,
    /// #4642 piece D): derived on demand from live neighbor-hosting
    /// advertisements + ring distance, never a stored formation flag. "Strictly
    /// closer to the key" makes the upstream relation a strict descent toward the
    /// key, so it is **acyclic by construction** (§4 point 2); the deterministic
    /// peer-address tiebreak makes equidistant hosts a total order (§6 point 2).
    ///
    /// It is introduced here alongside — not in place of — the stored
    /// `is_upstream` interest flag: the reconcile-core keystone will compute
    /// upstream everywhere and delete the stored flag, but this first step only
    /// computes it in parallel to measure how often the two disagree (the
    /// `hosting_upstream_computed_vs_stored_divergence` telemetry recorded at
    /// `OpManager::send_unsubscribe_upstream`), producing field evidence for the
    /// flag's drift (#4671). No decision consults this method yet.
    pub(crate) fn most_keyward_hosting_neighbor(
        &self,
        instance_id: &ContractInstanceId,
        hosting_neighbors: &[TransportPublicKey],
    ) -> Option<PeerKeyLocation> {
        let contract_location = Location::from(instance_id);
        let my_location = self.connection_manager.own_location().location()?;
        let my_distance = my_location.distance(contract_location);

        // Resolve each advertised pub key to a live connection with a known
        // location, then delegate the strict-closer + deterministic-tiebreak
        // selection to `most_keyward_among` (unit-tested in `most_keyward_tests`).
        let candidates = hosting_neighbors
            .iter()
            .filter_map(|pk| self.connection_manager.get_peer_by_pub_key(pk))
            .filter_map(|pkl| {
                let dist = pkl.location()?.distance(contract_location);
                Some((pkl, dist))
            });
        most_keyward_among(my_distance, candidates)
    }

    /// Check if a contract-directed CONNECT is currently in backoff.
    fn is_in_contract_connect_backoff(&self, contract_key: &ContractKey) -> bool {
        let backoff = self.contract_connect_backoff.lock();
        if let Some(state) = backoff.get(contract_key) {
            state.last_attempt.elapsed() < state.current_backoff
        } else {
            false
        }
    }

    /// Record a contract-directed CONNECT attempt. Doubles the backoff (up to cap).
    fn record_contract_connect_attempt(&self, contract_key: &ContractKey) {
        let mut backoff = self.contract_connect_backoff.lock();
        let now = self.time_source.now();
        let state = backoff
            .entry(*contract_key)
            .or_insert_with(|| ContractConnectState {
                current_backoff: Self::INITIAL_CONTRACT_CONNECT_BACKOFF,
                last_attempt: now,
            });
        state.last_attempt = now;
        state.current_backoff = (state.current_backoff * 2).min(Self::MAX_CONTRACT_CONNECT_BACKOFF);
    }

    /// Periodic task: when this peer is a subscription root for a contract,
    /// initiate a CONNECT toward the contract's ring location to merge
    /// disconnected subscription subtrees.
    async fn contract_directed_connects(ring: Arc<Self>, interval: Duration) {
        let shutdown = ring.shutdown_token();
        // Random initial delay to prevent thundering herd.
        let initial_delay = Duration::from_secs(GlobalRng::random_range(30u64..=60u64));
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(initial_delay)).await {
            return;
        }

        let mut tick_interval = tokio::time::interval(interval);
        tick_interval.tick().await; // skip first immediate tick

        loop {
            if sleep_or_shutdown(&shutdown, async {
                tick_interval.tick().await;
            })
            .await
            {
                return;
            }

            // Skip if we have too few connections to be meaningful.
            let conn_count = ring.connection_manager.connection_count();
            if conn_count < 2 {
                continue;
            }

            let contracts = ring.hosting_contract_keys();
            if contracts.is_empty() {
                continue;
            }

            let Some(op_manager) = ring.upgrade_op_manager() else {
                continue;
            };

            let mut connects_this_cycle = 0;

            for contract_key in &contracts {
                if connects_this_cycle >= Self::MAX_CONTRACT_CONNECTS_PER_CYCLE {
                    break;
                }

                if !ring.is_subscription_root(contract_key) {
                    // No longer root — a closer connection now exists.
                    // If we had backoff state, we previously sent a CONNECT as root.
                    // Now that a closer peer is connected, expire the subscription
                    // so it re-routes through the closer peer on the next renewal cycle.
                    let had_backoff = ring
                        .contract_connect_backoff
                        .lock()
                        .remove(contract_key)
                        .is_some();
                    if had_backoff {
                        ring.force_subscription_renewal(contract_key);
                        tracing::info!(
                            contract = %contract_key,
                            "No longer subscription root after contract-directed CONNECT; \
                             expired subscription to re-route through closer peer"
                        );
                    }
                    continue;
                }

                let contract_location = Location::from(contract_key);
                let my_location = ring.connection_manager.own_location().location();
                let my_distance = my_location.map(|l| l.distance(contract_location).as_f64());

                if ring.is_in_contract_connect_backoff(contract_key) {
                    continue;
                }

                // Emit telemetry for the root detection.
                crate::tracing::telemetry::send_standalone_event(
                    "subscription_root_detected",
                    serde_json::json!({
                        "contract": contract_key.to_string(),
                        "contract_location": contract_location.as_f64(),
                        "neighbor_count": conn_count,
                        "my_distance": my_distance,
                    }),
                );

                ring.record_contract_connect_attempt(contract_key);

                let backoff_secs = {
                    let b = ring.contract_connect_backoff.lock();
                    b.get(contract_key)
                        .map(|s| s.current_backoff.as_secs())
                        .unwrap_or(0)
                };

                tracing::info!(
                    contract = %contract_key,
                    %contract_location,
                    backoff_secs,
                    "Initiating contract-directed CONNECT as subscription root"
                );

                crate::tracing::telemetry::send_standalone_event(
                    "contract_directed_connect",
                    serde_json::json!({
                        "contract": contract_key.to_string(),
                        "contract_location": contract_location.as_f64(),
                        "my_distance": my_distance,
                        "backoff_secs": backoff_secs,
                    }),
                );

                let skip_list = HashSet::new();
                match ring
                    .acquire_new(
                        contract_location,
                        &skip_list,
                        &op_manager.to_event_listener,
                        &ring.live_tx_tracker,
                        &op_manager,
                    )
                    .await
                {
                    Ok(Some(tx)) => {
                        tracing::debug!(
                            %tx,
                            contract = %contract_key,
                            "Contract-directed CONNECT initiated"
                        );
                        connects_this_cycle += 1;
                    }
                    Ok(None) => {
                        tracing::debug!(
                            contract = %contract_key,
                            "Contract-directed CONNECT: no routing target found"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            contract = %contract_key,
                            error = %e,
                            "Contract-directed CONNECT failed"
                        );
                    }
                }
            }
        }
    }

    /// Periodic heartbeat: send `Interests { hashes }` to each connected peer.
    ///
    /// This prevents the death spiral where interest entries expire because no
    /// broadcasts are flowing, which in turn prevents broadcasts from ever
    /// flowing again. By periodically re-sending the full interest set, we
    /// Periodic governance reaper loop. Ticks every
    /// `GOVERNANCE_TICK_INTERVAL` and:
    ///
    ///   1. Calls `governance_tick(interval)` which snapshots live
    ///      benefit, applies cost decay, and runs the MAD detector
    ///      over the per-contract distribution.
    ///   2. Logs each `ReaperDecision` at INFO level with the from→to
    ///      state and reason. In `DryRun` mode (the default) this is
    ///      the only effect; in `Enforce` mode the caller would also
    ///      emit `EvictContract` events here. Enforce-mode wiring
    ///      lands in a subsequent commit.
    ///   3. Logs the network-norms summary (median, MAD, threshold,
    ///      sample size) at DEBUG so a busy node doesn't spam INFO.
    ///
    /// **Cancellation safety:** the loop's only `.await` points are the
    /// shutdown-raced `tokio::time::sleep` / `interval.tick()` (via
    /// [`sleep_or_shutdown`]). No channel sends, no long-running operations —
    /// the design-doc rule that "the dashboard reflects the back-end" is
    /// respected here too: the reaper writes governance state; readers consume
    /// it asynchronously. On shutdown the loop returns immediately rather than
    /// finishing the current sleep (#4278).
    async fn governance_reaper_loop(ring: Arc<Self>) {
        // Initial delay so a fleet-wide restart doesn't have every node
        // ticking in lockstep. Derived from the node's own ring location
        // (already random + node-distinct) rather than from `GlobalRng`,
        // because consuming from `GlobalRng` at startup shifts the
        // deterministic seed state and breaks simulation tests whose
        // routing/topology decisions are RNG-driven (the
        // `test_get_routing_coverage_low_htl` regression was caught by
        // CI on PR #4270). 30-90 second offset; the tick itself runs
        // every minute below.
        let own_loc = ring
            .connection_manager
            .own_location()
            .location()
            .map(|l| l.as_f64())
            .unwrap_or(0.5);
        let initial_delay_secs = 30 + ((own_loc * 60.0) as u64);
        let initial_delay = Duration::from_secs(initial_delay_secs);
        let shutdown = ring.shutdown_token();
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(initial_delay)).await {
            return;
        }

        let mut interval = tokio::time::interval(GOVERNANCE_TICK_INTERVAL);
        interval.tick().await; // Skip first immediate tick.
        let mut last_tick = ring.time_source.now();

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }
            let now = ring.time_source.now();
            let elapsed = now.saturating_duration_since(last_tick);
            last_tick = now;

            let result = ring.governance_tick(elapsed);

            // Cycle the UPDATE rate limiter's idle-entry cleanup on
            // the same cadence. Keeps the `(sender, contract)` map
            // bounded; CLEANUP_AGE is 5 minutes so this drops entries
            // for pairs that haven't tried an UPDATE since then.
            ring.update_rate_limiter.cleanup();

            // Phase 7 ban-list maintenance. Defense-in-depth — the
            // BanLifted decisions below explicitly unban, but if any
            // entry slipped through the reaper (e.g. mode flipped off
            // mid-window), cleanup catches it on the next tick.
            ring.contract_ban_list.cleanup();

            // Sweep expired broken-invariant flags on the same cadence so a
            // suppressed contract (especially a false-positive) recovers
            // after BROKEN_INVARIANT_TTL instead of being bricked forever.
            ring.broken_invariants.cleanup();

            // Network-norms summary at DEBUG — useful when debugging
            // calibration but too noisy for INFO on a healthy node.
            tracing::debug!(
                median = ?result.median_log_ratio,
                mad = ?result.mad,
                threshold = ?result.threshold,
                sample_size = result.sample_size,
                capacity_ceiling_binding = result.capacity_ceiling_binding,
                skip_reason = ?result.skip_reason,
                "governance reaper tick",
            );

            // Phase 7 enforcement wiring: BanTriggered → ban list;
            // BanLifted → unban. Done AFTER the cleanup() so a
            // freshly-added entry (with expiry = now + ban_ttl) is
            // not immediately swept by the same tick's cleanup. The
            // two calls are sequential in this task — no concurrent
            // race — the ordering is purely to defend the
            // not-quite-expired-yet invariant.
            Self::apply_ban_decisions(
                &ring.contract_ban_list,
                &result.decisions,
                ring.time_source.now() + ring.governance.ban_ttl(),
            );

            // Decisions at INFO — these are the dashboard-relevant
            // events. Empty in healthy state, surfaces every
            // transition during incidents.
            for decision in result.decisions {
                tracing::info!(
                    contract = %decision.key,
                    from = ?decision.from,
                    to = ?decision.to,
                    reason = ?decision.reason,
                    actionable = decision.actionable,
                    "governance state transition",
                );
            }
        }
    }

    /// Translate a batch of governance decisions into ban-list
    /// mutations. Pulled out of the reaper loop so the wiring is
    /// directly testable: a missing or reversed branch here would
    /// silently break Phase 7 enforcement even with the
    /// `GovernanceManager` emitting correct transitions.
    ///
    /// **`actionable` filter:** non-actionable decisions (DryRun
    /// mode) are skipped. Today the `GovernanceManager` only emits
    /// `BanTriggered` in Enforce mode so this is a defense-in-depth
    /// guard — but a future "shadow" mode that wants to surface
    /// would-have-banned transitions in the dashboard must not
    /// silently begin enforcing them through this wiring.
    pub(crate) fn apply_ban_decisions(
        ban_list: &contract_ban_list::ContractBanList,
        decisions: &[crate::contract::governance::ReaperDecision],
        ban_expiry: tokio::time::Instant,
    ) {
        use crate::contract::governance::TransitionReason;
        for decision in decisions {
            if !decision.actionable {
                continue;
            }
            #[allow(clippy::wildcard_enum_match_arm)]
            match decision.reason {
                TransitionReason::BanTriggered => {
                    ban_list.ban(
                        decision.key,
                        ban_expiry,
                        contract_ban_list::BanReason::AutoMad,
                    );
                }
                TransitionReason::BanLifted => {
                    ban_list.unban(&decision.key);
                }
                _ => {}
            }
        }
    }

    /// keep entries alive on the remote side.
    ///
    /// Sends are spread evenly across the interval to avoid bursts.
    async fn interest_heartbeat(ring: Arc<Self>) {
        use crate::ring::interest::INTEREST_HEARTBEAT_INTERVAL;

        let shutdown = ring.shutdown_token();
        // Random initial delay to prevent synchronized heartbeats across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(15u64..=45u64));
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(initial_delay)).await {
            return;
        }

        let mut interval = tokio::time::interval(INTEREST_HEARTBEAT_INTERVAL);
        interval.tick().await; // Skip first immediate tick

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            let Some(op_manager) = ring.upgrade_op_manager() else {
                continue;
            };

            // Get our current interest hashes. NOTE: unlike the pre-Fix-1 loop,
            // an empty `hashes` does NOT skip the cycle. The InterestSync interest
            // send is gated on `hashes` per-peer below, but the piggybacked
            // advertisement-layer re-request (Fix 1) must run for ANY connected
            // node: every node consults its `neighbor_contracts` view for terminal
            // GET/SUBSCRIBE advertisement lookups (invariant 5) and Source-1 UPDATE
            // fan-out, even a pure relay that hosts nothing and has no interests,
            // so that view must be reconciled on the anti-entropy cadence — else a
            // dropped retraction leaves a phantom advertised host unhealed
            // indefinitely. The only gate is having connected peers (below).
            let hashes = op_manager.interest_manager.get_all_interest_hashes();

            // Get all connected peer addresses (deduplicated)
            let connections = ring.connection_manager.get_connections_by_location();
            let peer_addrs: Vec<std::net::SocketAddr> = {
                let mut seen = HashSet::new();
                connections
                    .values()
                    .flat_map(|conns| conns.iter())
                    .filter_map(|conn| conn.location.socket_addr())
                    .filter(|addr| seen.insert(*addr))
                    .collect()
            };

            if peer_addrs.is_empty() {
                continue;
            }

            let num_peers = peer_addrs.len();
            // num_peers >= 1 guaranteed by the is_empty() check above
            let spread_delay = INTEREST_HEARTBEAT_INTERVAL / num_peers as u32;

            tracing::debug!(
                num_peers,
                num_hashes = hashes.len(),
                "Interest heartbeat: sending Interests to peers"
            );

            let sender = op_manager.to_event_listener.notifications_sender();
            let mut peers_sent = 0usize;
            for (i, peer_addr) in peer_addrs.into_iter().enumerate() {
                // InterestSync STATE heartbeat: only when we actually have
                // interest hashes to advertise.
                if !hashes.is_empty() {
                    let message = crate::message::InterestMessage::Interests {
                        hashes: hashes.clone(),
                    };
                    if let Err(e) = sender
                        .send(either::Either::Right(
                            crate::message::NodeEvent::SendInterestMessage {
                                target: peer_addr,
                                message,
                            },
                        ))
                        .await
                    {
                        // Channel send failure means the receiver is dropped (node
                        // shutting down). No point sending to remaining peers.
                        tracing::debug!(
                            peer = %peer_addr,
                            error = %e,
                            "Interest heartbeat: failed to queue message"
                        );
                        break;
                    }
                }

                // Advertisement-layer anti-entropy (#4642 spec step 1, "Fix 1"),
                // PIGGYBACKED on the InterestSync heartbeat's cycle / peer / cadence.
                // Re-request this neighbor's full hosted-contract set; the
                // `HostingStateResponse` handler REPLACES our view of it, so a
                // dropped on-connect exchange or a dropped per-eviction retraction
                // self-heals within one heartbeat interval (both directions, since
                // every node runs this). ALWAYS sent when the cycle runs —
                // decoupled from the interest send above, so an advertising-but-not-
                // interested node (e.g. just-restarted with disk-loaded hosts) still
                // reconciles. Carried in THIS loop rather than a separate background
                // task on purpose: an independent task's random initial-delay draw
                // from the shared `GlobalRng` shifts every seeded simulation's RNG
                // stream (perturbing routing/topology) and a second per-node timer
                // adds cross-run determinism surface — reusing this loop keeps the
                // sim RNG stream byte-identical. It is WASM-free / state-free (only
                // contract IDs move), so it cannot re-arm the #4440/#4473 summarize
                // storm.
                // Best-effort and NON-BLOCKING: `try_send`, never `send().await`.
                // This is the anti-entropy backstop that heals next cycle, so a
                // request dropped under backpressure is the DESIGNED behavior — and
                // it rides the cap-2048 event-loop notification channel (the
                // #4145/#4231/#4442 wedge channel), which a blocking send inside a
                // background loop must never stall on (channel-safety rule). Mirrors
                // the sibling retraction path (`announce_contract_unhosted` uses
                // `try_notify_node_event`). No `break` on error: shutdown is caught
                // by the `sleep_or_shutdown` on the spread delay below (and by the
                // interest `send().await` above), so a dropped/closed send here just
                // moves on.
                if let Err(e) = sender.try_send(either::Either::Right(
                    crate::message::NodeEvent::SendNetMessage {
                        target: peer_addr,
                        msg: Box::new(crate::message::NetMessage::V1(
                            crate::message::NetMessageV1::NeighborHosting {
                                message:
                                    crate::message::NeighborHostingMessage::HostingStateRequest,
                            },
                        )),
                    },
                )) {
                    tracing::debug!(
                        peer = %peer_addr,
                        error = %e,
                        "Interest heartbeat: hosting re-request dropped (best-effort; heals next cycle)"
                    );
                }
                peers_sent += 1;

                // Spread sends evenly across the interval (skip delay after last).
                // `spread_delay` can be many seconds when peer count is low, so
                // race it against shutdown too (#4278).
                if i + 1 < num_peers
                    && sleep_or_shutdown(&shutdown, tokio::time::sleep(spread_delay)).await
                {
                    return;
                }
            }

            crate::tracing::telemetry::send_standalone_event(
                "interest_heartbeat_cycle",
                serde_json::json!({
                    "peers_sent": peers_sent,
                    "interest_hashes": hashes.len(),
                }),
            );
        }
    }

    /// Register events with the event system.
    /// This is used by operations to emit failure and other events.
    pub async fn register_events<'a>(
        &self,
        events: either::Either<
            crate::tracing::NetEventLog<'a>,
            Vec<crate::tracing::NetEventLog<'a>>,
        >,
    ) {
        self.event_register.register_events(events).await;
    }

    /// Maximum number of route events to load from the AOF event log on startup
    /// and during periodic refresh. Caps memory use while retaining enough history
    /// for the isotonic estimators to converge.
    const ROUTER_HISTORY_LIMIT: usize = 10_000;

    async fn refresh_router<ER: NetEventRegister>(
        router: Arc<RwLock<Router>>,
        register: ER,
        shutdown: CancellationToken,
    ) {
        // Load routing history immediately on startup so the router doesn't
        // start cold — without this, peers route suboptimally for ~5 minutes
        // until the first periodic refresh.
        //
        // Health gauge (#4440): the startup read is recorded too, with the same
        // semantics as the periodic loop (any Ok read = success, empty or not).
        // Without this, a node whose startup read succeeds but whose every later
        // periodic refresh fails would report `last_success_age_secs == null`
        // forever — masking that it did succeed once.
        match register.get_router_events(Self::ROUTER_HISTORY_LIMIT).await {
            Ok(history) if !history.is_empty() => {
                BACKGROUND_TASK_HEALTH.record_refresh_router_success();
                tracing::info!(
                    events = history.len(),
                    "Restored routing history from event log"
                );
                *router.write() = Router::new(&history);
            }
            Ok(_) => {
                BACKGROUND_TASK_HEALTH.record_refresh_router_success();
                tracing::debug!("No routing history to restore on startup");
            }
            Err(error) => {
                BACKGROUND_TASK_HEALTH.record_refresh_router_failure();
                tracing::warn!(%error, "Failed to load routing history on startup, starting cold");
            }
        }

        let mut interval = tokio::time::interval(Duration::from_secs(60 * 5));
        interval.tick().await;
        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }
            let history = match register.get_router_events(Self::ROUTER_HISTORY_LIMIT).await {
                Ok(h) => {
                    // Health gauge (#4440): a successful read clears the
                    // consecutive-failure run and stamps the last-success time so
                    // a *persistently* failing refresh (silent since the #4438
                    // non-fatal hotfix) becomes observable on the snapshot
                    // cadence. The read succeeded regardless of whether the
                    // history is empty, so record success here, not below.
                    BACKGROUND_TASK_HEALTH.record_refresh_router_success();
                    h
                }
                Err(error) => {
                    // get_router_events errors are transient and recoverable —
                    // e.g. file-descriptor exhaustion ("os error 24") when the
                    // AOF segment file can't be opened during a connection-churn
                    // spike. Router refresh is a best-effort optimization, so we
                    // keep the existing in-memory router in place and retry on
                    // the next interval rather than killing the node. (A genuine
                    // panic inside this task is still caught by the
                    // BackgroundTaskMonitor — only this expected, transient read
                    // failure is swallowed here.)
                    //
                    // Health gauge (#4440): bump the consecutive-failure run so
                    // the escalation #4438 deliberately left non-fatal is at
                    // least visible in telemetry.
                    BACKGROUND_TASK_HEALTH.record_refresh_router_failure();
                    tracing::error!(
                        error = %error,
                        "Failed to refresh routing history from event log; \
                         keeping existing router and retrying on next interval"
                    );
                    continue;
                }
            };
            if !history.is_empty() {
                *router.write() = Router::new(&history);
            }
        }
    }

    /// Periodically emit a router model snapshot as an EventKind::RouterSnapshot event.
    ///
    /// This captures the isotonic regression curves and model state, including the
    /// connect forward estimator if available via OpManager.
    async fn emit_router_snapshot_telemetry(ring: Arc<Self>, interval_duration: Duration) {
        let shutdown = ring.shutdown_token();
        let mut interval = tokio::time::interval(interval_duration);
        // Skip the first immediate tick
        interval.tick().await;

        // Previous lifetime broadcast-stream-failure count, so each snapshot can
        // emit the per-window delta directly (#4440) without a stateful
        // collector. Loop-local because this task is the sole reader; the
        // monotonic totals are still emitted for collector-side differencing.
        let mut prev_broadcast_stream_failures_total: u64 = crate::node::BROADCAST_STREAM_METRICS
            .snapshot()
            .streaming_failures_total;

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            let mut snapshot = ring.router.read().snapshot();

            // Try to include connect forward estimator data.
            // Drop the read guard immediately after `snapshot()` so unrelated
            // consumers don't queue behind us for the four field assignments
            // (clippy: `significant_drop_tightening`).
            if let Some(op_manager) = ring.upgrade_op_manager() {
                let (curve, data_range, events, adjustments) =
                    op_manager.connect_forward_estimator.read().snapshot();
                snapshot.connect_forward_curve = Some(curve);
                snapshot.connect_forward_data_range = Some(data_range);
                snapshot.connect_forward_events = Some(events);
                snapshot.connect_forward_peer_adjustments = Some(adjustments);
            }

            // Node-health gauges (#4440): make fd-exhaustion headroom observable
            // on the existing snapshot cadence. Best-effort — `None` where the
            // platform can't report it (see `read_fd_usage`).
            let (open_fds, fd_soft_limit) = read_fd_usage();
            snapshot.open_fds = open_fds;
            snapshot.fd_soft_limit = fd_soft_limit;

            // Nearest-neighbor ring-lattice completeness + probe health (#4760),
            // mirrored from the home-page ring-stats provider (see
            // `node/p2p_impl.rs`) onto the central-telemetry snapshot cadence so
            // the lattice fix's network-wide impact — fraction of peers holding
            // both immediate-neighbor edges, median edge distances, probe success
            // rate — is a one-line query over central telemetry as 0.2.96 rolls
            // out (#4642). Same calls, same `None`/`0` semantics as the home page:
            // a distance is `None` when that side is unheld or own location is
            // unknown; the probe counters are `0` before any probe fires.
            let cm = &ring.connection_manager;
            let lattice_successor = cm.nearest_lattice_neighbor_dist(true);
            let lattice_predecessor = cm.nearest_lattice_neighbor_dist(false);
            let (lattice_probes_issued, lattice_probe_improvements) = cm.lattice_probe_stats();
            snapshot.lattice_has_successor = Some(lattice_successor.is_some());
            snapshot.lattice_has_predecessor = Some(lattice_predecessor.is_some());
            snapshot.lattice_successor_distance = lattice_successor;
            snapshot.lattice_predecessor_distance = lattice_predecessor;
            snapshot.lattice_probes_issued = Some(lattice_probes_issued);
            snapshot.lattice_probe_improvements = Some(lattice_probe_improvements);

            // Compiled-WASM module-cache occupancy + eviction gauges (#4440),
            // read from the per-node `Arc` the caches publish into (they live
            // behind the contract-handler channel, unreachable from here; the
            // `RuntimePool` shares this `Arc` via `op_manager.ring`).
            //
            // Force a fresh recompute of the interest split (cold-evictable /
            // interested bytes + would-reclassify) FIRST, so this snapshot is
            // fresh even on a cache that's been idle since its last mutation —
            // the throttled get/insert/remove refresh is unbounded on a quiet
            // node (#4441/#4534 shadow-staleness fix). No-op before the runtime
            // pool is built. Cheap O(entries), once per snapshot.
            ring.module_cache_metrics.refresh_interest_shadow_now();
            let mc = ring.module_cache_metrics.snapshot();
            snapshot.contract_module_cache_entries = Some(mc.contract_entries);
            snapshot.contract_module_cache_total_bytes = Some(mc.contract_total_bytes);
            snapshot.contract_module_cache_budget_bytes = Some(mc.contract_budget_bytes);
            snapshot.contract_module_cache_evictions_total = Some(mc.contract_evictions_total);
            snapshot.delegate_module_cache_entries = Some(mc.delegate_entries);
            snapshot.delegate_module_cache_total_bytes = Some(mc.delegate_total_bytes);
            snapshot.delegate_module_cache_budget_bytes = Some(mc.delegate_budget_bytes);
            snapshot.delegate_module_cache_evictions_total = Some(mc.delegate_evictions_total);

            // Capability-relative hosting-budget gauges (#4642 A2): the
            // RAM-scaled budget, its current occupancy (occupancy/utilization
            // ratio = current / budget, headroom = 1 - that; derived by the
            // collector), the hosted-contract count, and the monotonic
            // budget-triggered eviction counter. These let us observe in
            // production whether the RAM-scaled budget keeps a small box off the
            // #4565 OOM path. Per-node aggregate scalars only.
            let hosting = ring.hosting_manager.hosting_cache_stats();
            snapshot.hosting_budget_bytes = Some(hosting.budget_bytes);
            snapshot.hosting_current_bytes = Some(hosting.current_bytes);
            snapshot.hosting_contract_count = Some(hosting.contract_count);
            snapshot.hosting_budget_evictions_total = Some(hosting.budget_evictions_total);

            // Demand-ordered eviction gauge (#4642 A3): the
            // #4338 miscalibration signal (evictions of repeatedly-read
            // contracts). Per-node aggregate scalar on the same cadence.
            snapshot.hosting_evictions_of_recently_read_total =
                Some(hosting.evictions_of_recently_read_total);
            // PUT-durability falsifier (#4642): unread-seed evictions (a PUT
            // seed evicted before its first reader) and their running age sum,
            // on the same periodic cadence. Differenced against
            // `hosting_budget_evictions_total` (the total-eviction denominator)
            // this measures whether the "PUT is not read-demand" decision is
            // discarding fresh content before it can be found.
            snapshot.hosting_evicted_unread_total = Some(hosting.evicted_unread_total);
            snapshot.hosting_evicted_unread_age_secs_sum =
                Some(hosting.evicted_unread_age_secs_sum);
            // OOM-valve falsifier (#4642 subscriber-primary rework): evictions
            // that shed a subscribed contract under genuine RAM overflow. Stays 0
            // until the Overflow trigger is wired (mechanism-only this release).
            snapshot.hosting_oom_valve_evictions_total = Some(hosting.oom_valve_evictions_total);
            // Subscribed-eviction falsifier (#4642 subscriber-primary rework): the
            // count of evictions that shed a SUBSCRIBED contract (the riskiest new
            // behavior). Unlike the OOM-valve counter this can go nonzero as soon
            // as the rework ships (AtCapacity now sheds subscribed as a last
            // resort). Same periodic cadence.
            snapshot.hosting_subscribed_evictions_total = Some(hosting.subscribed_evictions_total);
            // Phantom-hosting falsifier (SUBSCRIBE-retirement step 10 §1d):
            // current count of contracts in-use via a downstream subscriber with
            // NO state on disk (contract_in_use && !contract_state_present). After
            // the register-after-state fix this should read 0; a nonzero value
            // means a hop registered demand it cannot serve (#4404/#4612 phantom).
            // redb-scoped by construction (contract_state_present is
            // conservative-true elsewhere). Same hand-mirror footgun as the gauges
            // above — invisible to the collector unless added to event_kind_to_json
            // too (pinned by router_snapshot_json_includes_phantom_in_use_gauge).
            snapshot.phantom_in_use_contracts = Some(ring.hosting_manager.phantom_in_use_count());
            // Local-client GET hit-rate (#4642 A3): served-locally vs routed to
            // the network. Driven by the real serve/forward decision in the
            // client GET handler, so it is read from the manager counters (not
            // cache membership).
            snapshot.hosting_local_hits_total = Some(ring.hosting_manager.local_get_serves());
            snapshot.hosting_local_misses_total = Some(ring.hosting_manager.local_get_forwards());

            // Aggregate on-disk usage gauges (#4683): state (delta-tracked) +
            // WASM blobs + wasmtime compile cache (both du-measured) + their sum.
            // `None` until the tracker is configured and seeded (early startup),
            // in which case the fields stay unset. Observational only in this PR.
            if let Some(disk) = ring.hosting_manager.disk_usage_stats() {
                snapshot.hosting_disk_state_bytes = Some(disk.state_bytes);
                snapshot.hosting_disk_wasm_bytes = Some(disk.wasm_bytes);
                snapshot.hosting_disk_compile_cache_bytes = Some(disk.compile_cache_bytes);
                snapshot.hosting_disk_total_bytes = Some(disk.total_bytes);
            }

            // Terminal advertisement-consult counters (hosting redesign piece C,
            // #4646). Exported here per #4658 so the production findability
            // baseline — the dead-end decomposition (off-path-advertised-host
            // dead-ends that C closes: attempts→hits→resolved_found, vs the
            // no-host-near-key residual `still_not_found` that needs piece D) —
            // is legible in central telemetry ahead of the piece-E / 0.2.92
            // decision, instead of only in the internal singleton and sim-only
            // metrics. Read from the per-node network_status singleton where the
            // consult sites record them; `None` only before the singleton is
            // initialized (i.e. always populated in production snapshots).
            if let Some((attempts, hits, resolved_found, still_not_found)) =
                crate::node::network_status::terminal_consult_counts()
            {
                snapshot.terminal_consult_attempts = Some(attempts);
                snapshot.terminal_consult_hits = Some(hits);
                snapshot.terminal_consult_resolved_found = Some(resolved_found);
                snapshot.terminal_consult_still_not_found = Some(still_not_found);
            }

            // Computed-upstream vs. stored-`is_upstream`-flag divergence counters
            // (#4642 piece D / #4671). Recorded at `send_unsubscribe_upstream`
            // where the stored flag is still consulted; exported here so the
            // stored flag's real-world drift rate is legible in central telemetry
            // ahead of the reconcile-core keystone that deletes the flag. Read
            // from the per-node network_status singleton; `None` only before the
            // singleton is initialized (i.e. always populated in production).
            if let Some((comparisons, divergences)) =
                crate::node::network_status::upstream_divergence_counts()
            {
                snapshot.upstream_computed_vs_stored_comparisons = Some(comparisons);
                snapshot.upstream_computed_vs_stored_divergences = Some(divergences);
            }

            // Reconcile-controller SHADOW comparison counters, split PER SITE
            // (keystone step-2, #4642). Recorded at the on-`main` hosting decision
            // sites (collapse via `send_unsubscribe_upstream`, renewal via
            // `contracts_needing_renewal`) where the pure `reconcile` controller
            // is run in parallel and compared BY SET MEMBERSHIP to the current
            // behavior; exported here so the controller's real-world divergence
            // from today's scattered decisions is legible in central telemetry
            // BEFORE any decision is flipped to it. Split per site because the
            // flip is site-by-site and each must be separately gate-able. Same
            // hand-mirror footgun as the counters above — a new
            // `RouterSnapshotInfo` field is invisible to the collector unless
            // added here AND in `event_kind_to_json` (pinned by
            // `router_snapshot_json_includes_reconcile_shadow_counters`). Read
            // from the per-node network_status singleton; `None` only before it is
            // initialized.
            if let Some(shadow) = crate::node::network_status::reconcile_shadow_counts() {
                // Every assignment reads uniformly from `shadow.<site>.<field>`
                // (NO aliasing), so the mirror-seam pin
                // `reconcile_shadow_export_maps_each_field_to_its_own_site` can
                // verify each snapshot field is fed from its OWN site — a
                // field-swap here would silently emit the wrong per-site value.
                //
                // MAINTENANCE sites (collapse, renewal): full per-action export
                // (all 9 counters).
                snapshot.reconcile_shadow_collapse_comparisons = Some(shadow.collapse.comparisons);
                snapshot.reconcile_shadow_collapse_divergences = Some(shadow.collapse.divergences);
                snapshot.reconcile_shadow_collapse_subscribe_diffs =
                    Some(shadow.collapse.subscribe_diffs);
                snapshot.reconcile_shadow_collapse_renew_diffs = Some(shadow.collapse.renew_diffs);
                snapshot.reconcile_shadow_collapse_unsubscribe_diffs =
                    Some(shadow.collapse.unsubscribe_diffs);
                snapshot.reconcile_shadow_collapse_collapse_diffs =
                    Some(shadow.collapse.collapse_diffs);
                snapshot.reconcile_shadow_collapse_announce_diffs =
                    Some(shadow.collapse.announce_diffs);
                snapshot.reconcile_shadow_collapse_retract_diffs =
                    Some(shadow.collapse.retract_diffs);
                snapshot.reconcile_shadow_collapse_reroot_search_diffs =
                    Some(shadow.collapse.reroot_search_diffs);
                snapshot.reconcile_shadow_renewal_comparisons = Some(shadow.renewal.comparisons);
                snapshot.reconcile_shadow_renewal_divergences = Some(shadow.renewal.divergences);
                snapshot.reconcile_shadow_renewal_subscribe_diffs =
                    Some(shadow.renewal.subscribe_diffs);
                snapshot.reconcile_shadow_renewal_renew_diffs = Some(shadow.renewal.renew_diffs);
                snapshot.reconcile_shadow_renewal_unsubscribe_diffs =
                    Some(shadow.renewal.unsubscribe_diffs);
                snapshot.reconcile_shadow_renewal_collapse_diffs =
                    Some(shadow.renewal.collapse_diffs);
                snapshot.reconcile_shadow_renewal_announce_diffs =
                    Some(shadow.renewal.announce_diffs);
                snapshot.reconcile_shadow_renewal_retract_diffs =
                    Some(shadow.renewal.retract_diffs);
                snapshot.reconcile_shadow_renewal_reroot_search_diffs =
                    Some(shadow.renewal.reroot_search_diffs);
                // EDGE sites: focused on one class each, so comparisons +
                // divergences fully capture the signal.
                snapshot.reconcile_shadow_inbound_unsubscribe_comparisons =
                    Some(shadow.inbound_unsubscribe.comparisons);
                snapshot.reconcile_shadow_inbound_unsubscribe_divergences =
                    Some(shadow.inbound_unsubscribe.divergences);
                snapshot.reconcile_shadow_connection_drop_comparisons =
                    Some(shadow.connection_drop.comparisons);
                snapshot.reconcile_shadow_connection_drop_divergences =
                    Some(shadow.connection_drop.divergences);
                snapshot.reconcile_shadow_host_formation_comparisons =
                    Some(shadow.host_formation.comparisons);
                snapshot.reconcile_shadow_host_formation_divergences =
                    Some(shadow.host_formation.divergences);
            }

            // Keep the hosting manager's copy of our own ring location current so
            // the proximity-prior demand estimate (#4642 A3) can turn a contract
            // key into a distance. Best-effort: only push a known location.
            if let Some(own_loc) = ring.connection_manager.own_location().location() {
                ring.hosting_manager.set_own_location(own_loc);
            }

            // Interest-weighted (two-tier) module-cache SHADOW gauges
            // (#4441/#4534): always-on, independent of the
            // FREENET_MODULE_CACHE_INTEREST_TIERED feature flag. They quantify
            // what the two-tier policy WOULD reclaim/reclassify; the
            // migration-admission counter now records actual admissions the
            // interested-occupancy gate (#4534) RECOVERS versus the old raw gate.
            snapshot.contract_module_cache_cold_evictable_bytes =
                Some(mc.contract_cold_evictable_bytes);
            snapshot.contract_module_cache_interested_bytes = Some(mc.contract_interested_bytes);
            snapshot.contract_module_cache_evictions_would_reclassify_total =
                Some(mc.contract_evictions_would_reclassify_total);
            snapshot.migration_admission_recovered_total =
                Some(mc.migration_admission_recovered_total);

            // UPDATE-broadcast stream-assembly failure gauge (#4440): the exact
            // signal that flagged the v0.2.73 incident. The broadcast queue
            // publishes monotonic totals into the process-global; emit the totals
            // (for collector-side differencing) plus the per-window failure delta
            // sampled here.
            let bs = crate::node::BROADCAST_STREAM_METRICS.snapshot();
            let broadcast_stream_failures_delta = window_delta(
                bs.streaming_failures_total,
                &mut prev_broadcast_stream_failures_total,
            );
            snapshot.broadcast_stream_attempts_total = Some(bs.streaming_attempts_total);
            snapshot.broadcast_stream_failures_total = Some(bs.streaming_failures_total);
            snapshot.broadcast_stream_failures_last_snapshot =
                Some(broadcast_stream_failures_delta);

            // Background-task health gauges (#4440): make a persistently-failing
            // (but non-fatal, post-#4438) `refresh_router` observable.
            let rr_health = BACKGROUND_TASK_HEALTH.refresh_router_snapshot();
            snapshot.refresh_router_last_success_age_secs = rr_health.last_success_age_secs;
            snapshot.refresh_router_consecutive_failures = Some(rr_health.consecutive_failures);

            // Placement-quality gauge (#4404 follow-up): host-to-hosted-key
            // ring-distance distribution. If the SubscribeHint placement
            // migration is working, hosting drifts toward each contract's key,
            // so this distribution tightens over time. Cheap — a few thousand
            // distance computations at most, every 5 minutes. Skip gracefully if
            // the node has no ring location yet (emit nothing) or hosts nothing
            // (emit count=0, distances absent). No lock is held across an await:
            // `hosting_contract_keys()` returns an owned `Vec` and the distance
            // math is pure.
            let own_location = ring
                .connection_manager
                .own_location()
                .location()
                .map(|loc| loc.as_f64());
            if let Some(node_loc) = own_location {
                let hosted_keys = ring.hosting_contract_keys();
                let contract_locations: Vec<f64> = hosted_keys
                    .iter()
                    .map(|key| Location::from(key).as_f64())
                    .collect();
                if let Some(stats) =
                    placement_migration_metrics::placement_quality(node_loc, &contract_locations)
                {
                    // `stats.count` equals `contract_locations.len()`; read it here
                    // (rather than the Vec len) so the field stays live in
                    // non-test builds.
                    snapshot.hosted_contracts_count = Some(stats.count);
                    snapshot.hosted_key_distance_median = Some(stats.median);
                    snapshot.hosted_key_distance_p90 = Some(stats.p90);
                    snapshot.hosted_key_distance_min = Some(stats.min);
                    snapshot.hosted_key_distance_mean = Some(stats.mean);
                    snapshot.hosted_key_distance_frac_within_0_1 = Some(stats.frac_within_0_1);
                } else {
                    // Node hosts nothing: count is 0, distance gauges absent.
                    snapshot.hosted_contracts_count = Some(0);
                }
            }

            // Placement-migration activity counters (#4404 follow-up): is the
            // migration firing, and at what rate? Monotonic lifetime totals read
            // from the per-node counter sink the send/receive sites publish into.
            let pm = ring.placement_migration_metrics.snapshot();
            snapshot.subscribe_hint_sent = Some(pm.sent);
            snapshot.subscribe_hint_received = Some(pm.received);
            snapshot.subscribe_hint_acted = Some(pm.acted);
            snapshot.renewal_terminus_satisfied = Some(pm.renewal_terminus_satisfied);
            // Per-gate refusal + directed-subscribe outcome breakdown (#4534
            // diagnostics).
            snapshot.subscribe_hint_refused_version = Some(pm.received_refused_version_floor);
            snapshot.subscribe_hint_refused_already_hosting =
                Some(pm.received_refused_already_hosting);
            snapshot.subscribe_hint_refused_holder = Some(pm.received_refused_holder_mismatch);
            snapshot.subscribe_hint_refused_cache = Some(pm.received_refused_cache_admission);
            snapshot.subscribe_hint_acted_succeeded = Some(pm.acted_succeeded);
            snapshot.subscribe_hint_acted_failed = Some(pm.acted_failed);

            tracing::info!(
                failure_events = snapshot.failure_events,
                success_events = snapshot.success_events,
                prediction_active = snapshot.prediction_active,
                consider_n_closest_peers = snapshot.consider_n_closest_peers,
                open_fds = ?snapshot.open_fds,
                fd_soft_limit = ?snapshot.fd_soft_limit,
                contract_module_cache_entries = mc.contract_entries,
                contract_module_cache_total_bytes = mc.contract_total_bytes,
                contract_module_cache_evictions_total = mc.contract_evictions_total,
                broadcast_stream_attempts_total = bs.streaming_attempts_total,
                broadcast_stream_failures_total = bs.streaming_failures_total,
                broadcast_stream_failures_last_snapshot = broadcast_stream_failures_delta,
                refresh_router_last_success_age_secs = ?rr_health.last_success_age_secs,
                refresh_router_consecutive_failures = rr_health.consecutive_failures,
                hosted_contracts_count = ?snapshot.hosted_contracts_count,
                hosted_key_distance_median = ?snapshot.hosted_key_distance_median,
                hosted_key_distance_p90 = ?snapshot.hosted_key_distance_p90,
                hosted_key_distance_frac_within_0_1 = ?snapshot.hosted_key_distance_frac_within_0_1,
                subscribe_hint_sent = pm.sent,
                subscribe_hint_received = pm.received,
                subscribe_hint_acted = pm.acted,
                renewal_terminus_satisfied = pm.renewal_terminus_satisfied,
                terminal_consult_attempts = ?snapshot.terminal_consult_attempts,
                terminal_consult_hits = ?snapshot.terminal_consult_hits,
                terminal_consult_resolved_found = ?snapshot.terminal_consult_resolved_found,
                terminal_consult_still_not_found = ?snapshot.terminal_consult_still_not_found,
                upstream_computed_vs_stored_comparisons =
                    ?snapshot.upstream_computed_vs_stored_comparisons,
                upstream_computed_vs_stored_divergences =
                    ?snapshot.upstream_computed_vs_stored_divergences,
                "router_snapshot"
            );

            if let Some(event) = NetEventLog::router_snapshot(&ring, snapshot) {
                ring.event_register
                    .register_events(Either::Left(event))
                    .await;
            }
        }
    }

    /// Periodically emit subscription_state telemetry events for all active subscriptions.
    ///
    /// This enables the telemetry dashboard to reconstruct historical subscription trees
    /// and show accurate subscription state at any point in time.
    async fn emit_subscription_state_telemetry(ring: Arc<Self>, interval_duration: Duration) {
        let shutdown = ring.shutdown_token();
        let mut interval = tokio::time::interval(interval_duration);
        // Skip the first immediate tick
        interval.tick().await;

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            // Get subscription states from the new lease-based model
            let subscription_states = ring.get_subscription_states();

            if subscription_states.is_empty() {
                continue;
            }

            tracing::debug!(
                subscription_count = subscription_states.len(),
                "Emitting periodic subscription state telemetry"
            );

            // Log subscription states (simplified - no upstream/downstream in new model)
            for (key, has_client, is_active, _expires_at) in subscription_states {
                tracing::trace!(
                    %key,
                    has_client_subscription = has_client,
                    is_active_subscription = is_active,
                    "Subscription state"
                );
            }
        }
    }

    /// Maximum renewal tasks spawned per tick.
    ///
    /// At 30s intervals, this yields up to 20 renewals/minute. Since
    /// `contracts_needing_renewal()` returns contracts expiring within the
    /// 2-minute renewal window, this effectively handles ~40 concurrent
    /// subscriptions before renewals can't keep up. The mid-cycle channel
    /// capacity check (RENEWAL_STOP_CAPACITY_FRACTION) provides backpressure
    /// if the network can't absorb this many.
    const MAX_RECOVERY_ATTEMPTS_PER_INTERVAL: usize = 10;

    /// Maximum phantom repair fetches (#4612) spawned per recovery tick.
    ///
    /// Bounds the producer (the #4440/#4610 lesson: never an unbounded
    /// producer feeding the shared serial paths): at 30s intervals this is at
    /// most 8 sub-op GETs/minute network-wide fetch pressure from phantom
    /// repair, regardless of how many phantoms the relay path accumulated.
    /// Per-contract pacing/giving-up lives in
    /// `HostingManager::reconcile_phantom_in_use` (cooldown + max attempts).
    const MAX_PHANTOM_REPAIRS_PER_INTERVAL: usize = 4;

    /// Skip renewal cycle when channel remaining capacity falls below this
    /// fraction of max (i.e. channel is more than 50% full).
    const RENEWAL_DEFER_CAPACITY_FRACTION: usize = 2; // channel_max / 2

    /// Stop spawning mid-cycle when remaining capacity falls below this
    /// fraction of max (i.e. channel is more than 75% full).
    const RENEWAL_STOP_CAPACITY_FRACTION: usize = 4; // channel_max / 4

    /// Interval for periodic subscription state telemetry snapshots.
    pub(crate) const SUBSCRIPTION_STATE_INTERVAL: Duration = Duration::from_secs(60);

    /// Interval for periodic subscription recovery attempts.
    ///
    /// This recovers "orphaned hosters" - peers that have contracts in cache
    /// but failed to establish subscription (no upstream in subscription tree).
    pub(crate) const SUBSCRIPTION_RECOVERY_INTERVAL: Duration = Duration::from_secs(30);

    /// Total wall-clock budget the renewal driver gives **itself** across all
    /// of a renewal's network attempts.
    ///
    /// Issue #4350: `recover_orphaned_subscriptions` wraps each renewal task in
    /// an outer cancel deadline ([`Self::renewal_outer_cancel`]), but the
    /// renewal driver's per-attempt network wait used the global
    /// [`OPERATION_TTL`] (60 s) while the old outer deadline was only 25 s. A
    /// peer answering between 25 s and 60 s was killed by the outer cancel
    /// *mid-await* — the in-flight reply then landed on the renewal task's
    /// already-dropped capacity-1 receiver (the dominant source of
    /// `try_forward_driver_reply` closed-receiver drops; log-level fixed in
    /// PR #4351). The driver now clamps **each** attempt's timeout to the budget
    /// remaining until this deadline, so no attempt — first or retry — can still
    /// be awaiting when the outer cancel fires.
    ///
    /// Derived from the config interval (most of one recovery cycle) rather than
    /// independently hardcoded. 20 s.
    pub(crate) const RENEWAL_TASK_BUDGET: Duration =
        Duration::from_secs(Self::SUBSCRIPTION_RECOVERY_INTERVAL.as_secs() - 10);

    /// Maximum single-attempt network wait on the renewal path. Each attempt is
    /// actually capped at `min(this, remaining task budget)` (see
    /// [`Self::RENEWAL_TASK_BUDGET`]); this is the ceiling for the first
    /// attempt when the full budget is available. Set equal to the total budget
    /// so a single slow peer may consume the whole budget in one attempt;
    /// multi-peer renewals naturally get shorter per-attempt waits as the
    /// remaining budget shrinks.
    pub(crate) const RENEWAL_PER_ATTEMPT_TIMEOUT: Duration = Self::RENEWAL_TASK_BUDGET;

    /// Slack between a renewal task's worst-case self-termination time
    /// (`RENEWAL_TASK_BUDGET` + a fully backpressured `release_pending_op_slot`
    /// cleanup) and the hard outer cancel deadline. Gives the driver room to
    /// finish its own cleanup and telemetry before the outer cancel could fire.
    /// 5 s.
    pub(crate) const RENEWAL_OUTER_DEADLINE_MARGIN: Duration = Duration::from_secs(5);

    /// Hard outer cancel deadline wrapping each spawned renewal task in
    /// `recover_orphaned_subscriptions`. Pure backstop for a *genuinely wedged*
    /// driver: in normal operation the driver self-terminates within
    /// [`Self::RENEWAL_TASK_BUDGET`] (slow peer) and then runs cleanup, so this
    /// deadline never fires.
    ///
    /// Issue #4350 (Codex review): it must clear the driver's worst case —
    /// `RENEWAL_TASK_BUDGET` (a full slow attempt) **plus** a fully
    /// backpressured `release_pending_op_slot`, which awaits up to
    /// [`OpManager::NOTIFICATION_SEND_TIMEOUT`] (30 s) — or the outer cancel
    /// could interrupt cleanup, leaving the per-attempt waiter in
    /// `pending_op_results` (the very closed-receiver drop this change removes).
    /// Deadline = `BUDGET (20 s) + NOTIFICATION_SEND_TIMEOUT (30 s) +
    /// RENEWAL_OUTER_DEADLINE_MARGIN (5 s) = 55 s`.
    ///
    /// Outliving the 30 s recovery interval is safe: `mark_subscription_pending`
    /// (released via `SubscriptionRecoveryGuard` on completion **or** cancel)
    /// blocks a second concurrent renewal for the same contract, and the
    /// per-cycle spawn count is bounded by `MAX_RECOVERY_ATTEMPTS_PER_INTERVAL`
    /// and the channel-capacity gates — so a long-running task can't accumulate.
    pub(crate) fn renewal_outer_cancel() -> Duration {
        Self::RENEWAL_TASK_BUDGET
            + crate::node::OpManager::NOTIFICATION_SEND_TIMEOUT
            + Self::RENEWAL_OUTER_DEADLINE_MARGIN
    }

    /// Floor below which the renewal driver will **not** start another attempt:
    /// a remaining budget this small can't complete a useful network round-trip
    /// before the task budget (and then the outer cancel) elapses, so the
    /// driver stops cleanly and lets the next 30 s recovery cycle retry rather
    /// than starting an attempt doomed to be cut short. 2 s.
    pub(crate) const RENEWAL_MIN_ATTEMPT_BUDGET: Duration = Duration::from_secs(2);

    /// Interval for periodic GET subscription cache sweep.
    pub(crate) const GET_SUBSCRIPTION_SWEEP_INTERVAL: Duration = Duration::from_secs(60);

    /// Periodically attempt to recover "orphaned hosters" - contracts we're hosting
    /// but don't have an upstream subscription for.
    ///
    /// This can happen when:
    /// - The initial subscription after GET/PUT failed (network issues, timeout)
    /// - Our upstream peer disconnected and we haven't found a new one
    /// - A race condition left us hosting without subscription
    ///
    /// The task respects existing backoff mechanisms to avoid subscription spam.
    ///
    /// **Connection gating (#3676):** This task skips renewal cycles entirely when
    /// the node has zero ring connections. Without this gate, disconnected peers
    /// generate a subscribe retry storm — thousands of subscribe requests per cycle
    /// that all fail immediately because there's no one to send them to. Telemetry
    /// showed 3 peers with 0 connections generating 96% of all subscribe traffic.
    async fn recover_orphaned_subscriptions(ring: Arc<Self>, interval_duration: Duration) {
        let shutdown = ring.shutdown_token();
        // Wait indefinitely for the first ring connection before starting
        // subscription recovery. The per-cycle connection check below is the
        // real gate; this just avoids running the loop body with no peers.
        //
        // The 500ms poll is itself <1s, but the *loop* can park here forever on
        // a node that never connects — so race shutdown here too, otherwise a
        // never-connected node would hang teardown indefinitely (#4278).
        let mut wait_logged = false;
        loop {
            if sleep_or_shutdown(&shutdown, tokio::time::sleep(Duration::from_millis(500))).await {
                return;
            }
            if ring.open_connections() > 0 {
                tracing::info!(
                    hosted_contracts = ring.hosting_contract_keys().len(),
                    "Ring connection established, starting subscription recovery"
                );
                break;
            }
            // Log periodically so operators can diagnose stuck nodes.
            if !wait_logged {
                wait_logged = true;
                tracing::info!(
                    hosted_contracts = ring.hosting_contract_keys().len(),
                    "Waiting for ring connection before starting subscription recovery"
                );
            }
        }

        // Small jitter (2-5s) after first connection to let the ring stabilize
        // slightly before flooding with subscribe requests.
        let jitter = Duration::from_secs(GlobalRng::random_range(2u64..=5u64));
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(jitter)).await {
            return;
        }

        let mut interval = tokio::time::interval(interval_duration);
        // Skip the first immediate tick — we run the first pass immediately
        // below (no tick wait) so client subscriptions get prompt renewal.
        interval.tick().await;

        let mut first_pass = true;

        loop {
            if first_pass {
                first_pass = false;
            } else if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            // Always run expiry sweeps, even when disconnected. Stale
            // subscriptions and downstream subscribers must be cleaned up
            // to keep interest manager counts accurate. Only the renewal
            // spawning (below) is gated on having connections.
            //
            // First, expire any stale subscriptions
            let expired = ring.expire_stale_subscriptions();
            if !expired.is_empty() {
                tracing::debug!(
                    expired_count = expired.len(),
                    "Expired {} stale subscriptions",
                    expired.len()
                );
            }

            // Expire stale downstream subscribers and decrement interest manager
            let ds_expired = ring.expire_stale_downstream_subscribers();
            if !ds_expired.is_empty() {
                tracing::debug!(
                    expired_count = ds_expired.len(),
                    "Expired stale downstream subscribers"
                );

                if let Some(op_manager) = ring.upgrade_op_manager() {
                    for (contract, expired_count) in &ds_expired {
                        // Decrement interest manager for each expired peer
                        for _ in 0..*expired_count {
                            op_manager
                                .interest_manager
                                .remove_downstream_subscriber(contract);
                        }

                        // Send Unsubscribe upstream if no remaining interest.
                        // FLIP (keystone P6, #4642): the collapse decision is driven
                        // by the reconcile controller's strict-farther interest gate
                        // (`reconcile_wants_collapse` = `!contract_in_use`), replacing
                        // the legacy ANY-downstream `should_unsubscribe_upstream`. The
                        // teardown still targets the STORED upstream (narrow flip).
                        if op_manager.reconcile_wants_collapse(
                            contract,
                            crate::node::network_status::ReconcileShadowSite::Collapse,
                        ) {
                            let op_mgr = op_manager.clone();
                            let contract = *contract;
                            GlobalExecutor::spawn(async move {
                                op_mgr.send_unsubscribe_upstream(&contract).await;
                            });
                        }
                    }
                }
            }

            // Gate: skip renewal spawning if we have no ring connections (#3676).
            // Subscribe requests require connected peers to route through.
            // Without this, disconnected peers flood the notification channel
            // with doomed subscribe requests every 30 seconds.
            if ring.open_connections() == 0 {
                tracing::debug!("Skipping subscription renewal: no ring connections");
                continue;
            }

            // Phantom in-use reconciliation (#4612): enforce `in-use ⇒
            // has-state`. The transit-relay SUBSCRIBE path registers
            // downstream subscribers without ever fetching the contract's
            // state, leaving this node advertised as a "host" it cannot be
            // (GET dead-ends #4404; summarize storm #4610 before the #4611
            // gate). Repair each phantom with a bounded one-shot sub-op GET;
            // after MAX_PHANTOM_REPAIR_ATTEMPTS failures drop the phantom
            // registration instead of keeping it forever. Runs after the
            // connection gate: the repair fetch needs peers to route through,
            // and a disconnected node must not burn its bounded attempts on
            // fetches that are doomed locally.
            let repairs = ring.reconcile_phantom_in_use(Self::MAX_PHANTOM_REPAIRS_PER_INTERVAL);
            if !repairs.is_empty()
                && let Some(op_manager) = ring.upgrade_op_manager()
            {
                for repair in repairs {
                    // Single-variant match: the #4770 cycling `Drop` arm was
                    // NEUTRALIZED in step 10 §1c (register-after-state makes a
                    // genuine phantom unrepresentable, so dropping only churns).
                    // Only the bounded `Fetch` rollout net survives. A re-added
                    // Drop variant would break this match, forcing an explicit
                    // decision.
                    match repair {
                        crate::ring::hosting::PhantomRepair::Fetch(key) => {
                            tracing::info!(
                                contract = %key,
                                "phantom in-use contract (no stored state): \
                                 starting one-shot repair fetch"
                            );
                            // Fire-and-forget: the side effect (contract +
                            // state cached locally) is what restores the
                            // invariant; the next 30s pass observes the store.
                            let _ = crate::operations::get::op_ctx_task::start_sub_op_get(
                                &op_manager,
                                *key.id(),
                                true,
                            );
                        }
                    }
                }
            }

            // Get contracts that need subscription renewal (have client subscriptions)
            let mut contracts_needing_renewal = ring.contracts_needing_renewal();

            if contracts_needing_renewal.is_empty() {
                tracing::debug!(
                    hosted = ring.hosting_contract_keys().len(),
                    "No contracts needing subscription renewal"
                );
                continue;
            }

            tracing::info!(
                needing_renewal = contracts_needing_renewal.len(),
                hosted = ring.hosting_contract_keys().len(),
                "Starting subscription renewal cycle"
            );

            // Shuffle to prevent starvation: without this, the same failing contracts
            // (first N in iteration order) would always be tried first, blocking later
            // contracts from ever being attempted when they hit the batch limit.
            GlobalRng::shuffle(&mut contracts_needing_renewal);

            // Get op_manager to spawn subscription requests
            let Some(op_manager) = ring.upgrade_op_manager() else {
                tracing::debug!("OpManager not available for subscription renewal");
                continue;
            };

            // Backpressure: reduce batch size when the notification channel is
            // congested, but never skip entirely. Renewals are critical-path —
            // skipping a full cycle when the channel is busy lets subscriptions
            // expire, which causes cascading failures as the subscription tree
            // thins out and remaining renewals take longer paths.
            let sender = op_manager.to_event_listener.notifications_sender();
            let channel_remaining = sender.capacity();
            let channel_max = sender.max_capacity();

            let batch_limit =
                if channel_remaining < channel_max / Self::RENEWAL_DEFER_CAPACITY_FRACTION {
                    // Channel >50% full: allow a reduced batch (quarter of normal)
                    // so critical renewals still get through. Always attempt at least 1.
                    let reduced = (Self::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL / 4).max(1);
                    tracing::warn!(
                        channel_remaining,
                        channel_max,
                        batch_limit = reduced,
                        contracts = contracts_needing_renewal.len(),
                        "Notification channel >50% full, reducing renewal batch size"
                    );
                    reduced
                } else {
                    Self::MAX_RECOVERY_ATTEMPTS_PER_INTERVAL
                };

            let mut attempted = 0;
            let mut skipped = 0;
            // Per-tick budget on the number of interest-gate EVALUATIONS
            // (`OpManager::reconcile_wants_renewal`) we run. Each evaluation builds
            // a FRESH `ReconcileInputs` snapshot: a synchronous redb
            // `get_state_size` read plus an `is_subscription_root` neighbor-map
            // scan. The `attempted >= batch_limit` break below counts only SPAWNED
            // renewals; a gate-SUPPRESSED candidate does `continue` WITHOUT
            // advancing `attempted`, so on a high-hosting peer (every peer, now
            // that every-hop placement is live) a single ~30s tick could otherwise
            // build hundreds-to-thousands of these snapshots when the gate
            // suppresses most candidates. Counting every gate evaluation against
            // this budget restores the pre-flip `shadow_budget` bound: at most
            // `batch_limit` snapshots per tick regardless of how many are
            // suppressed. (Codex P2, #4725.)
            let mut evaluated = 0;

            // Reconcile-controller FLIP (keystone sub-task 3, #4642), RENEWAL
            // site. The controller no longer records a shadow divergence here; it
            // DRIVES: for each candidate the loop calls
            // `OpManager::reconcile_wants_renewal`, which builds a FRESH
            // `ReconcileInputs` snapshot AT EMISSION time and renews only while the
            // controller wants the contract's place in the mesh maintained. Each
            // snapshot is a redb state-store read plus an `is_subscription_root`
            // neighbor scan, so the per-tick cost is bounded by the `evaluated`
            // budget below (at most `batch_limit` gate evaluations per tick),
            // mirroring the old shadow sample budget. The `attempted >=
            // batch_limit` break bounds only the SPAWNED renewals and must NOT be
            // relied on to bound the evaluations, because a gate-suppressed
            // candidate does not advance `attempted`.
            for contract in contracts_needing_renewal {
                // Limit concurrent renewal attempts to avoid overwhelming the network
                if attempted >= batch_limit {
                    tracing::debug!(
                        limit = batch_limit,
                        "Reached max renewal attempts for this interval, remaining will be tried next cycle"
                    );
                    break;
                }

                // Stop early if the channel is filling from our own spawns.
                let remaining_now = sender.capacity();
                if remaining_now < channel_max / Self::RENEWAL_STOP_CAPACITY_FRACTION {
                    tracing::warn!(
                        channel_remaining = remaining_now,
                        attempted,
                        "Notification channel >75% full during renewal spawning, stopping early"
                    );
                    break;
                }

                // Phase 7 egress gate (#4373). Don't renew a subscription
                // for a contract we have banned: a renewal re-registers
                // interest via the same outbound-SUBSCRIBE machinery as a
                // client-initiated request, but unlike the four
                // `start_client_*` originator entry points this scheduler
                // doesn't pass through `reject_if_contract_banned`. Without
                // this check the node keeps emitting outbound SUBSCRIBE
                // renewals for a banned-but-still-subscribed contract on
                // every maintenance cycle until the ban TTL lifts. Checked
                // before `can_request_subscription` so a banned contract is
                // skipped regardless of its spam-backoff state.
                if ring.contract_ban_list.is_banned(contract.id()) {
                    tracing::debug!(
                        %contract,
                        phase = "subscription_renewal_banned_skip",
                        "skipping subscription renewal for banned contract"
                    );
                    skipped += 1;
                    continue;
                }

                // Check spam prevention (respects exponential backoff and pending checks)
                if !ring.can_request_subscription(&contract) {
                    skipped += 1;
                    continue;
                }

                // Per-tick evaluation budget (Codex P2, #4725): stop building
                // expensive `ReconcileInputs` snapshots once we've run
                // `batch_limit` interest-gate evaluations this tick, whether they
                // led to a spawn or a suppression. This is the bound that keeps a
                // high-hosting peer from building hundreds-to-thousands of
                // snapshots per tick when the gate suppresses most candidates (the
                // `attempted >= batch_limit` break above does NOT cover this,
                // because a suppressed candidate never advances `attempted`).
                // Remaining candidates are retried next cycle.
                if evaluated >= batch_limit {
                    tracing::debug!(
                        limit = batch_limit,
                        attempted,
                        skipped,
                        "Reached max renewal-gate evaluations for this interval, \
                         remaining will be tried next cycle"
                    );
                    break;
                }
                evaluated += 1;

                // FLIP (#4642 keystone): interest-gated renewal DRIVEN by the
                // reconcile controller's interest gate (design §5a
                // `contract_in_use`). Build a FRESH snapshot at emission time and
                // renew iff a local client, a STRICTLY-farther downstream
                // subscriber (piece D), OR recent local GET/PUT access (invariant
                // 3: reads/PUTs are permanent demand — a read-only / PUT-only
                // contract stays renewed without a subscription) depends on this
                // peer hosting. When it goes false — no longer in use — SKIP: the lease
                // lapses and the chain collapses inward (non-renewal is the collapse
                // primitive; the #3763 storm fix). The gate narrows the
                // ANY-downstream candidate set that `contracts_needing_renewal`
                // built to the strict gate; the suppression is recorded on the
                // RENEWAL reconcile counter (post-flip that counter measures the
                // flip's effect — the "renewal tracks active demand, not cache size"
                // ship-gate falsifier — not a shadow divergence).
                if !op_manager.reconcile_wants_renewal(&contract) {
                    tracing::debug!(
                        %contract,
                        "renewal skipped: reconcile interest gate says not in use \
                         (strict-farther gate) — letting the lease lapse so the \
                         chain collapses inward"
                    );
                    crate::node::network_status::record_reconcile_shadow_comparison(
                        crate::node::network_status::ReconcileShadowSite::Renewal,
                        crate::ring::reconcile::ReconcileActionDivergence {
                            renew: true,
                            ..Default::default()
                        },
                    );
                    skipped += 1;
                    continue;
                }
                // Renewal proceeds: record a non-divergent renewal-gate evaluation
                // so the counter's denominator (`comparisons`) tracks total gate
                // decisions and the suppression ratio stays legible.
                crate::node::network_status::record_reconcile_shadow_comparison(
                    crate::node::network_status::ReconcileShadowSite::Renewal,
                    crate::ring::reconcile::ReconcileActionDivergence::default(),
                );

                // Mark as pending and spawn subscription request
                if ring.mark_subscription_pending(contract) {
                    attempted += 1;

                    // Re-root and renewal share ONE spawn path. Per design §5b
                    // "re-rooting is just the renewal stream following the computed
                    // upstream", the connection-drop PROMPT re-root (#4642 piece F,
                    // `OpManager::spawn_prompt_reroots`) calls this SAME helper, so
                    // the storm-safety scaffolding (jitter, recovery guard,
                    // outer-cancel deadline, per-contract pending dedup) has a single
                    // source of truth and cannot drift between the two callers.
                    Self::spawn_renewal_subscribe_task(
                        op_manager.clone(),
                        contract,
                        shutdown.clone(),
                    );
                }
                // `mark_subscription_pending` returning false (a renewal for this
                // contract is already in flight) needs no separate handling: the
                // controller drive + counter above already ran for this contract
                // this tick, and the in-flight guard is the per-contract dedup.
            }

            if attempted > 0 || skipped > 0 {
                tracing::info!(
                    attempted,
                    skipped_rate_limited = skipped,
                    "Subscription renewal cycle complete"
                );
            }

            // Simulation-test observability (no-op in production — gated on a
            // current network name, like the wire-attempt/terminus counters):
            // record this cycle's spawn count so a migration-at-scale test can
            // assert the per-node renewal rate cap holds (`attempted` is bounded
            // by `MAX_RECOVERY_ATTEMPTS_PER_INTERVAL` above). Only when this
            // cycle actually spawned renewals — a zero batch never raises the
            // per-node max, so skip it to avoid creating empty registry entries.
            // See #4601.
            if attempted > 0 {
                if let Some(addr) = ring.connection_manager.get_own_addr() {
                    crate::ring::topology_registry::record_renewal_cycle_batch(
                        addr,
                        attempted as u64,
                    );
                }
            }
        }
    }

    /// Spawn ONE renewal / re-root subscribe task for `contract_key` and return
    /// immediately. The single storm-safe spawn path shared by the periodic
    /// renewal loop ([`recover_orphaned_subscriptions`]) and the event-driven
    /// connection-drop PROMPT re-root ([`OpManager::spawn_prompt_reroots`], #4642
    /// piece F).
    ///
    /// # Why one helper, not two
    ///
    /// Per the demand-driven-hosting design §5b, "re-rooting is not a separate
    /// operation — it is just the renewal stream following the computed upstream."
    /// A prompt re-root and a scheduled renewal are the SAME wire action
    /// (`run_renewal_subscribe`, which routes a single SUBSCRIBE toward the key to
    /// the current computed upstream); the only difference is the trigger (a drop
    /// event vs. a lease-age tick). Keeping ONE spawn path means the storm-safety
    /// scaffolding lives in one place and cannot drift between callers (the
    /// "manually-mirrored side effect" bug class, `bug-prevention-patterns.md`).
    ///
    /// # Make-before-break
    ///
    /// This issues a fresh SUBSCRIBE; it does NOT tear down any existing lease
    /// first. The old upstream registration lapses on its own once this peer stops
    /// renewing toward it (design §5b / §6), and the peer keeps serving its local
    /// copy throughout (invariant 1 serve-DURING). So a re-root never drops the
    /// subscription before the new root is acquired.
    ///
    /// # Per-contract rate cap (caller-owned)
    ///
    /// The caller MUST have already claimed the per-contract slot via
    /// `mark_subscription_pending` (which blocks a second concurrent
    /// renewal/re-root for the same contract until this task's
    /// [`SubscriptionRecoveryGuard`] releases it) — that mark, plus
    /// `can_request_subscription`'s exponential backoff, is the per-contract wire
    /// rate cap. This helper only owns the jitter spread + outer-cancel deadline.
    pub(crate) fn spawn_renewal_subscribe_task(
        op_manager: Arc<OpManager>,
        contract_key: ContractKey,
        shutdown: CancellationToken,
    ) {
        // Spread tasks across the interval to avoid thundering-herd bursts. On a
        // mass disconnect this jitter is what turns O(contracts) simultaneous
        // re-subscribes into a spread-out trickle — the storm-safety knob for the
        // piece-F prompt re-root caller as much as for the renewal loop.
        let jitter_ms = GlobalRng::random_range(0u64..=15_000);

        GlobalExecutor::spawn(async move {
            // Guard ensures complete_subscription_request is called even on panic.
            // Created BEFORE the jitter sleep so an early shutdown return below
            // still clears the `mark_subscription_pending` flag set above — the
            // guard's Drop marks the request failed (#4278).
            let guard = SubscriptionRecoveryGuard::new(op_manager.clone(), contract_key);

            // The jitter can be up to 15s; bail (dropping the guard, which
            // completes the pending request as failed) before doing any
            // renewal work if the node is shutting down (#4278).
            if sleep_or_shutdown(
                &shutdown,
                tokio::time::sleep(Duration::from_millis(jitter_ms)),
            )
            .await
            {
                return;
            }

            let instance_id = *contract_key.id();
            // Renewal driver: same machinery as
            // client-initiated SUBSCRIBE, with delivery
            // returned to this task instead of via
            // `result_router_tx`. `is_renewal=true` so
            // the responder skips sending state.
            //
            // Outer renewal cancel deadline: a pure backstop for a
            // *genuinely wedged* driver. In normal operation the
            // renewal driver self-terminates within
            // `RENEWAL_TASK_BUDGET` (slow peer) and then runs its
            // cleanup, so this deadline never fires.
            //
            // Issue #4350: the driver clamps each attempt's wait to
            // the budget remaining until its own task deadline
            // (`RENEWAL_TASK_BUDGET`, 20 s), so no attempt — first
            // or retry — is still awaiting when this outer cancel
            // fires; and the deadline is sized
            // (`renewal_outer_cancel`, 55 s) to clear the driver's
            // worst case (full-budget attempt + a fully
            // backpressured `release_pending_op_slot` cleanup, up to
            // `NOTIFICATION_SEND_TIMEOUT`). A task outliving the 30 s
            // recovery interval is safe: `mark_subscription_pending`
            // (released by `SubscriptionRecoveryGuard` on completion
            // or cancel) blocks a second concurrent renewal for the
            // same contract.
            let renewal_tx =
                crate::message::Transaction::new::<crate::operations::subscribe::SubscribeMsg>();
            let renewal_deadline = Self::renewal_outer_cancel();
            let outcome_enum = match tokio::time::timeout(
                renewal_deadline,
                crate::operations::subscribe::run_renewal_subscribe(
                    op_manager.clone(),
                    instance_id,
                    renewal_tx,
                ),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(_) => crate::operations::subscribe::RenewalOutcome::Failed {
                    reason: format!(
                        "renewal task exceeded {}s cycle deadline",
                        renewal_deadline.as_secs()
                    ),
                },
            };

            let (outcome, error_msg) = match outcome_enum {
                crate::operations::subscribe::RenewalOutcome::Success => {
                    tracing::info!(
                        %contract_key,
                        "Subscription renewal succeeded"
                    );
                    guard.complete(true);
                    ("success", None)
                }
                crate::operations::subscribe::RenewalOutcome::ChannelCongestion => {
                    // Channel congestion is a local resource issue, not a
                    // protocol failure. Don't penalize with backoff — just
                    // clear the pending mark so the contract is eligible on
                    // the next cycle.
                    tracing::warn!(
                        %contract_key,
                        "Subscription renewal skipped (channel full), will retry next cycle"
                    );
                    guard.complete(true);
                    ("dropped_channel_full", None)
                }
                crate::operations::subscribe::RenewalOutcome::Failed { reason } => {
                    tracing::debug!(
                        %contract_key,
                        error = %reason,
                        "Subscription renewal failed (will retry with backoff)"
                    );
                    guard.complete(false);
                    ("failed", Some(reason))
                }
            };

            crate::tracing::telemetry::send_standalone_event(
                "subscription_renewal_outcome",
                serde_json::json!({
                    "contract": contract_key.to_string(),
                    "outcome": outcome,
                    "error": error_msg,
                }),
            );
        });
    }

    /// Background task to sweep expired entries from the GET subscription cache.
    ///
    /// When contracts are evicted (past max entries and beyond TTL), this task
    /// cleans up the local subscription state. The upstream peer will eventually
    /// prune us when updates fail to deliver.
    async fn sweep_get_subscription_cache(ring: Arc<Self>, interval_duration: Duration) {
        let shutdown = ring.shutdown_token();
        // Add random initial delay to prevent synchronized sweeps across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(10u64..=30u64));
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(initial_delay)).await {
            return;
        }

        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // Skip first immediate tick

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            // Sweep expired entries from GET subscription cache
            let crate::ring::hosting::HostingSweepResult {
                expired,
                evicted_in_use_teardown,
            } = ring.sweep_expired_get_subscriptions();

            // Do NOT early-continue when `expired` is empty: pending
            // reclamation retries below must run every cycle, otherwise
            // entries queued by the in-use / queue-full skip points stay
            // leaked indefinitely whenever the cache is under budget
            // (the common case). See Codex r10 P2.
            if !expired.is_empty() {
                tracing::debug!(
                    expired_count = expired.len(),
                    "GET subscription cache sweep found expired entries"
                );
            }

            // Reclaim on-disk storage for the expired contracts so the hosting
            // budget is a real disk bound. The sweep task only holds an
            // `Arc<Ring>`; reach the `OpManager` the same way
            // `recover_orphaned_subscriptions` does, via the weak back-reference.
            // `reclaim_evicted_contract` re-checks the subscription gate per
            // key before emitting the eviction event.
            let op_manager = ring.upgrade_op_manager();
            if op_manager.is_none() {
                // The weak back-reference is dropped only during node shutdown;
                // surface the skipped reclamation so an unexpected `None` (and
                // the resulting on-disk leak for this cycle) is observable.
                tracing::debug!(
                    expired_count = expired.len(),
                    "OpManager unavailable during GET subscription sweep — \
                     on-disk reclamation skipped for the expired contracts this cycle"
                );
            }

            // Sync the `InterestManager` for any still-in-use victim the sweep
            // shed as a last resort: `teardown_evicted_in_use_contract` cleared
            // the hosting maps but the `InterestManager` lives on `OpManager`, so
            // ghost `interested_peers` / `peer_contracts` / `local_client_count`
            // entries survive unless we replay the same removals here (PR #4734
            // Fix 1). No-op in the common zero-subscriber sweep. Run BEFORE the
            // `unregister_local_hosting` loop below so that call observes the
            // now-zeroed subscriber counts and reports full interest loss (→
            // retraction), mirroring the GET/PUT host-formation paths.
            if let Some(op_manager) = &op_manager {
                for teardown in &evicted_in_use_teardown {
                    op_manager.interest_manager.remove_evicted_in_use(
                        &teardown.key,
                        &teardown.downstream_peers,
                        teardown.local_client_count,
                    );
                }
            }

            // Clean up local subscription state for each expired contract.
            // Note: under the subscriber-primary ordering (#4642, invariant 3) a
            // contract with subscribers is ordered LAST but NOT hard-pinned, so
            // `sweep_expired_hosting()` CAN shed a still-in-use contract as a last
            // resort — and when it does, it has already torn down that contract's
            // subscription state (downstream + client subscriptions + upstream
            // lease) so `contract_in_use` is false before we reclaim it. The
            // `ring.unsubscribe(&key)` below is therefore a belt-and-suspenders
            // no-op for those (its lease is already gone) and, for a
            // zero-subscriber eviction, drops any lingering upstream lease.
            // The `expected_generation` snapshot is captured atomically with
            // the eviction decision in `HostingCache::record_access` /
            // `sweep_expired`; it is re-checked at deletion time by
            // `RuntimePool::remove_contract` to close the re-host race.
            //
            // Also retract the local hosting advertisement for each evicted
            // contract, mirroring the GET (`get/op_ctx_task.rs`) and PUT
            // (`put/op_ctx_task.rs`) host-formation paths (PR #4734 Fix 1):
            // clear `LocalInterest.hosting` via `unregister_local_hosting` and
            // collect the contracts that thereby lose ALL interest so the
            // neighbor advertisement can be retracted below. Without this an
            // evicted contract keeps `hosting = true` and its advertisement
            // lingers on neighbors — a stale-interest leak, now widened because
            // subscribed contracts are sweep-evictable under invariant 3.
            let mut removed_contracts = Vec::new();
            for (key, expected_generation) in expired {
                ring.unsubscribe(&key);
                tracing::info!(
                    %key,
                    "Cleaned up expired hosting subscription from local state"
                );
                if let Some(op_manager) = &op_manager {
                    if op_manager.interest_manager.unregister_local_hosting(&key) {
                        removed_contracts.push(key);
                    }
                    crate::operations::reclaim_evicted_contract(
                        op_manager,
                        key,
                        expected_generation,
                    );
                }
            }

            // Retract neighbor advertisements for contracts that lost all local
            // interest above. The sweep never ADDS interest, so `added` is always
            // empty here (unlike the GET/PUT paths). `broadcast_change_interests`
            // is a no-op when `removed_contracts` is empty (the common case).
            if let Some(op_manager) = &op_manager {
                crate::operations::broadcast_change_interests(
                    op_manager,
                    Vec::new(),
                    removed_contracts,
                )
                .await;
            }

            // Retry pending reclamations queued by the two skip points
            // (fair-queue rejection of `EvictContract` and the
            // `contract_in_use` skip in `RuntimePool::remove_contract`).
            // The snapshot iterates without holding the DashMap shard
            // guards. Each retry routes through
            // `reclaim_evicted_contract`, which re-checks
            // `contract_in_use` — entries that are still in use stay in
            // the queue (no event emitted) and will be retried next
            // cycle. Successful reclamations clear their pending entry
            // in `RuntimePool::remove_contract`.
            if let Some(op_manager) = &op_manager {
                let pending = ring.pending_reclamation_snapshot();
                if !pending.is_empty() {
                    tracing::debug!(
                        pending_count = pending.len(),
                        "Retrying pending reclamations from previous skipped \
                         `EvictContract` events"
                    );
                    for (key, expected_generation) in pending {
                        crate::operations::reclaim_evicted_contract(
                            op_manager,
                            key,
                            expected_generation,
                        );
                    }
                }
            }

            // Disk-usage accounting maintenance (#4683). Runs OUTSIDE any
            // hosting-cache write lock (the du-walks would stall cache readers).
            // First tick lazily seeds the tracker from the true on-disk state
            // total; every tick re-walks the `du`-measured WASM-blob and
            // compile-cache totals so telemetry stays fresh. Observational only
            // in this PR — no admission/eviction decision reads these yet.
            //
            // The seed + refresh are synchronous, blocking disk I/O: a recursive
            // `std::fs` walk of `contracts_dir` + the wasmtime cache dir on every
            // tick, plus a one-time sync redb `load_all_hosting_metadata` on the
            // seeding tick. `contracts_dir` is unbounded and non-self-pruning, so
            // on a node hosting many contracts the walk can run for a while with
            // no `.await` point. Push it onto a blocking thread so it can never
            // stall other tasks on the async reactor — the same discipline
            // `secrets_store/sweep.rs` uses for its "disk walk + ReDb reads".
            let ring_for_disk = ring.clone();
            if let Err(err) = tokio::task::spawn_blocking(move || {
                #[cfg(feature = "redb")]
                ring_for_disk.hosting_manager.seed_disk_tracker_if_absent();
                ring_for_disk.hosting_manager.refresh_disk_usage();
            })
            .await
            {
                // The walk is observational only; a panic here must not wedge the
                // sweep loop, but it must not be swallowed silently either.
                tracing::warn!(%err, "disk-usage accounting maintenance task failed");
            }

            // Eviction floor (#4683, PR 2): recompute
            // `effective = min(ram_budget, disk_budget)` and install it as the
            // hosting-cache budget so the next `evict_over_budget` sheds state
            // down to the tighter of the two resource floors. The free-space read
            // is the determinism seam — production reads the data-dir mount; a
            // failed read falls back to `u64::MAX`, which makes the disk budget
            // clamp to its cap (degrade to "cap only", never to zero). The
            // du-walks above already ran OUTSIDE any cache lock (in the blocking
            // task); only the O(1) `set_budget_bytes` inside
            // `recompute_effective_budget` takes the cache write lock.
            let available = ring
                .hosting_manager
                .disk_available_bytes()
                .unwrap_or(u64::MAX);
            ring.hosting_manager.recompute_effective_budget(available);
        }
    }

    /// Periodically register topology snapshots for simulation testing.
    ///
    /// This task only runs when `CURRENT_NETWORK_NAME` is set (i.e., during SimNetwork tests).
    /// It allows SimNetwork to validate subscription topology by querying the global registry.
    #[cfg(any(test, feature = "testing"))]
    async fn register_topology_snapshots_periodically(
        ring: Arc<Self>,
        interval_duration: Duration,
    ) {
        use topology_registry::{get_current_network_name, register_topology_snapshot};

        tracing::info!("Topology snapshot registration task started");

        let shutdown = ring.shutdown_token();
        // Add small initial delay to let network stabilize (use short delay in tests)
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // Skip first immediate tick

        loop {
            if sleep_or_shutdown(&shutdown, async {
                interval.tick().await;
            })
            .await
            {
                return;
            }

            // Only register if we're in a simulation context
            let Some(network_name) = get_current_network_name() else {
                tracing::debug!("Topology snapshot: no network name set, skipping");
                continue;
            };

            let Some(peer_addr) = ring.connection_manager.get_own_addr() else {
                tracing::debug!("Topology snapshot: no peer address yet, skipping");
                continue;
            };

            // Use get_stored_location() for consistency with set_upstream distance check.
            // This ensures topology validation uses the same location as the tie-breaker.
            let location = ring
                .connection_manager
                .get_stored_location()
                .map(|l| l.as_f64())
                .unwrap_or(0.0);

            let snapshot = ring
                .hosting_manager
                .generate_topology_snapshot(peer_addr, location);
            let contract_count = snapshot.contracts.len();
            register_topology_snapshot(&network_name, snapshot);

            tracing::info!(
                %peer_addr,
                location,
                network = %network_name,
                contract_count,
                "Registered topology snapshot"
            );
        }
    }

    /// Record an access to a contract in the hosting cache.
    ///
    /// This adds or refreshes the contract in the unified hosting cache.
    /// ALL contracts in the hosting cache get subscription renewal.
    ///
    /// Returns a `RecordAccessResult` containing:
    /// - `is_new`: Whether this contract was newly added (vs. refreshed existing)
    /// - `evicted`: Contracts that were evicted to make room
    pub fn host_contract(
        &self,
        key: ContractKey,
        size_bytes: u64,
        access_type: AccessType,
    ) -> RecordAccessResult {
        self.hosting_manager
            .record_contract_access(key, size_bytes, access_type)
    }

    /// Record a GET access to a contract in the hosting cache.
    ///
    /// Returns a `RecordAccessResult` indicating whether this was a new addition
    /// and which contracts were evicted (if any).
    pub fn record_get_access(&self, key: ContractKey, size_bytes: u64) -> RecordAccessResult {
        self.host_contract(key, size_bytes, AccessType::Get)
    }

    /// Record that a local-client GET was answered from local hosted state
    /// (a hit) — see [`HostingManager::record_local_get_serve`]. (#4642 A3)
    pub fn record_get_served_locally(&self) {
        self.hosting_manager.record_local_get_serve();
    }

    /// Record that a local-client GET was routed to the network (a forward/miss)
    /// — see [`HostingManager::record_local_get_forward`]. (#4642 A3)
    pub fn record_get_forwarded(&self) {
        self.hosting_manager.record_local_get_forward();
    }

    /// Number of client GETs this node answered from local hosted state (A3
    /// serve-DURING hit counter). Read accessor for the counter incremented by
    /// [`Self::record_get_served_locally`]; used by the serve-DURING sim
    /// falsifier to assert a demandless-copy GET was served locally, not routed.
    #[cfg(any(test, feature = "testing"))]
    pub fn local_get_serves(&self) -> u64 {
        self.hosting_manager.local_get_serves()
    }

    /// Number of client GETs this node routed to the network (A3 forward/miss
    /// counter). Read accessor for the counter incremented by
    /// [`Self::record_get_forwarded`]; the serve-DURING falsifier asserts this
    /// stays `0` for a demandless-copy GET (the node never went dark).
    #[cfg(any(test, feature = "testing"))]
    pub fn local_get_forwards(&self) -> u64 {
        self.hosting_manager.local_get_forwards()
    }

    /// Whether this node is hosting this contract (has it in cache).
    #[inline]
    pub fn is_hosting_contract(&self, key: &ContractKey) -> bool {
        self.hosting_manager.is_hosting_contract(key)
    }

    /// The composed #4610 summarize/broadcast gate (see
    /// [`HostingManager::should_summarize_or_broadcast`]):
    /// `(is_hosting_contract || contract_in_use) && contract_state_present`.
    /// Skips phantom (interested-but-stateless) contracts while still serving
    /// evicted-but-in-use ones whose state remains on disk.
    #[inline]
    pub fn should_summarize_or_broadcast(&self, key: &ContractKey) -> bool {
        self.hosting_manager.should_summarize_or_broadcast(key)
    }

    /// Set the storage reference for hosting metadata persistence.
    ///
    /// Must be called after executor creation. This enables automatic
    /// cleanup of persisted metadata when contracts are evicted.
    pub fn set_hosting_storage(&self, storage: crate::contract::storages::Storage) {
        self.hosting_manager.set_storage(storage);
    }

    /// Install the aggregate disk-usage tracker's paths (#4683). Called once at
    /// startup with the node's mode-resolved contracts dir and the relocated
    /// wasmtime compile-cache dir. The tracker is seeded lazily on the first
    /// sweep tick.
    pub fn set_hosting_disk_paths(
        &self,
        contracts_dir: std::path::PathBuf,
        wasmtime_cache_dir: std::path::PathBuf,
        hosting_disk_pct: f64,
        max_hosting_disk: u64,
    ) {
        self.hosting_manager
            .configure_disk_tracker(contracts_dir, wasmtime_cache_dir);
        // #4683 eviction-floor sizing knobs (mirrors the disk paths install; the
        // config is only reachable here). The 60s sweep's recompute reads them.
        self.hosting_manager
            .configure_disk_budget(hosting_disk_pct, max_hosting_disk);
    }

    /// Drop the ring's clones of the redb `Storage` handle (hosting metadata +
    /// broken-invariants persistence). Called on node shutdown so the redb
    /// `Database` Arc count can fall to zero and release the on-disk file lock
    /// — otherwise an in-process restart against the same data dir deadlocks on
    /// the still-held lock. See issue #4401.
    pub(crate) fn clear_redb_storage(&self) {
        self.hosting_manager.clear_storage();
        self.broken_invariants.clear_storage();
    }

    /// Load hosting cache from persisted storage.
    ///
    /// Call this during startup after storage is available to restore
    /// the hosting cache from the previous run. Also migrates legacy contracts
    /// that have state but no hosting metadata.
    ///
    /// # Arguments
    /// * `storage` - The storage backend
    /// * `code_hash_lookup` - Function to look up CodeHash from ContractInstanceId.
    ///   Uses ContractStore which has the id->code_hash mapping.
    #[cfg(feature = "redb")]
    pub fn load_hosting_cache<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, redb::Error>
    where
        F: Fn(
            &freenet_stdlib::prelude::ContractInstanceId,
        ) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        self.hosting_manager
            .load_from_storage(storage, code_hash_lookup)
    }

    /// Load hosting cache from persisted storage (sqlite version).
    ///
    /// Also migrates legacy contracts that have state but no hosting metadata.
    #[cfg(all(feature = "sqlite", not(feature = "redb")))]
    pub async fn load_hosting_cache<F>(
        &self,
        storage: &crate::contract::storages::Storage,
        code_hash_lookup: F,
    ) -> Result<usize, crate::contract::storages::sqlite::SqlDbError>
    where
        F: Fn(
            &freenet_stdlib::prelude::ContractInstanceId,
        ) -> Option<freenet_stdlib::prelude::CodeHash>,
    {
        self.hosting_manager
            .load_from_storage(storage, code_hash_lookup)
            .await
    }

    pub fn record_request(
        &self,
        recipient: PeerKeyLocation,
        target: Location,
        request_type: TransactionType,
    ) {
        self.connection_manager
            .topology_manager
            .write()
            .record_request(recipient, target, request_type);
    }

    /// Add a connection to the ring topology.
    ///
    /// Returns `true` if this connection caused us to cross the readiness threshold
    /// (i.e., we just became ready to accept non-CONNECT operations).
    /// Returns `false` if the connection was rejected (e.g., capacity cap) or we
    /// were already ready.
    pub async fn add_connection(&self, loc: Location, peer: PeerId, was_reserved: bool) -> bool {
        tracing::info!(
            peer = %peer,
            peer_location = %loc,
            this = ?self.connection_manager.get_own_addr(),
            was_reserved = %was_reserved,
            "Adding connection to peer"
        );
        let min_ready = self.connection_manager.min_ready_connections;
        let was_ready = min_ready == 0 || self.connection_manager.connection_count() >= min_ready;

        let addr = peer.socket_addr();
        let pub_key = peer.pub_key().clone();
        let added = self
            .connection_manager
            .add_connection(loc, addr, pub_key, was_reserved);
        if !added {
            tracing::warn!(
                peer = %peer,
                peer_location = %loc,
                "Ring rejected connection - not updating caches or logging connection event"
            );
            return false;
        }
        if let Some(own_loc) = self.connection_manager.own_location().location() {
            crate::node::network_status::set_own_location(own_loc.as_f64());
        }
        // ConnectEvent::Connected telemetry is emitted by the CONNECT state
        // machine with proper transaction context; not duplicated here (#3578).
        self.refresh_density_request_cache();

        let is_ready = self.connection_manager.is_self_ready();
        // Return true only if we just crossed the threshold
        !was_ready && is_ready
    }

    pub fn update_connection_identity(&self, old_peer: &PeerId, new_peer: PeerId) {
        if self.connection_manager.update_peer_identity(
            old_peer.socket_addr(),
            new_peer.socket_addr(),
            new_peer.pub_key().clone(),
        ) {
            self.refresh_density_request_cache();
        }
    }

    fn refresh_density_request_cache(&self) {
        let cbl = self.connection_manager.get_connections_by_location();
        let topology_manager = &mut self.connection_manager.topology_manager.write();
        let _refreshed = topology_manager.refresh_cache(&cbl);
    }

    /// Returns a filtered iterator for peers that are not connected to this node already.
    pub fn is_not_connected<'a>(
        &self,
        peers: impl Iterator<Item = &'a PeerKeyLocation>,
    ) -> impl Iterator<Item = &'a PeerKeyLocation> + Send {
        let mut filtered = Vec::new();
        for peer in peers {
            if let Some(addr) = peer.socket_addr() {
                if !self.connection_manager.has_connection_or_pending(addr) {
                    filtered.push(peer);
                }
            } else {
                // If address is unknown, include the peer
                filtered.push(peer);
            }
        }
        filtered.into_iter()
    }

    /// Return the most optimal peer for hosting a given contract.
    ///
    /// This function only considers connected peers, not the node itself.
    #[inline]
    pub fn closest_potentially_hosting(
        &self,
        contract_key: &ContractKey,
        skip_list: impl Contains<std::net::SocketAddr>,
    ) -> Option<PeerKeyLocation> {
        let router = self.router.read();
        let target = Location::from(contract_key);
        let (peer, decision) = self
            .connection_manager
            .routing_with_telemetry(target, None, skip_list, &router);

        if let Some(decision) = &decision {
            tracing::debug!(
                target_location = %target.as_f64(),
                strategy = ?decision.strategy,
                num_candidates = decision.candidates.len(),
                total_routing_events = decision.total_routing_events,
                selected = peer.is_some(),
                "routing_decision"
            );
        }

        peer
    }

    /// Get k best peers for hosting a contract, ranked by routing predictions.
    /// Accepts either &ContractKey or &ContractInstanceId (both implement From<&T> for Location).
    pub fn k_closest_potentially_hosting<K>(
        &self,
        contract_id: &K,
        skip_list: impl Contains<std::net::SocketAddr> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation>
    where
        for<'a> Location: From<&'a K>,
    {
        // Router read-lock is only needed for the final `select_*` call;
        // building the candidate list does not need it. Acquire it late to
        // keep the critical section short (clippy: `significant_drop_tightening`).
        let target_location = Location::from(contract_id);

        let mut seen = HashSet::new();
        let mut candidates: Vec<PeerKeyLocation> = Vec::new();
        let mut not_ready_fallback: Vec<PeerKeyLocation> = Vec::new();
        let mut skipped_not_ready: usize = 0;
        let mut skipped_transient: usize = 0;

        let connections = self.connection_manager.get_connections_by_location();
        // Sort keys for deterministic iteration order (HashMap iteration is non-deterministic)
        // This ensures the `seen.insert()` check behaves consistently across runs
        let mut sorted_keys: Vec<_> = connections.keys().collect();
        sorted_keys.sort();
        for loc in sorted_keys {
            let conns = connections.get(loc).expect("key exists");
            // Sort connections for deterministic iteration order
            let mut sorted_conns: Vec<_> = conns.iter().collect();
            sorted_conns.sort_by_key(|c| c.location.clone());
            for conn in sorted_conns {
                if let Some(addr) = conn.location.socket_addr() {
                    if skip_list.has_element(addr) || !seen.insert(addr) {
                        continue;
                    }
                    // Skip transient peers — these are short-TTL connections used for
                    // CONNECT coordination, not stable routing targets. PUT/UPDATE
                    // already exclude them via `ConnectionManager::routing_candidates`
                    // (see connection_manager.rs:1578); previously GET/SUBSCRIBE
                    // could route through them and waste hops on a forwarder that
                    // was about to be dropped. Issue #4222 / #3570.
                    if self.connection_manager.is_transient(addr) {
                        tracing::debug!(
                            %addr,
                            target_location = %target_location.as_f64(),
                            "k_closest: skipping transient peer"
                        );
                        skipped_transient += 1;
                        continue;
                    }
                    // Skip peers that haven't advertised readiness, but collect them
                    // as fallback candidates in case all peers fail the readiness check.
                    if !self.connection_manager.is_peer_ready(addr) {
                        tracing::debug!(
                            %addr,
                            target_location = %target_location.as_f64(),
                            "k_closest: skipping peer not yet ready"
                        );
                        not_ready_fallback.push(conn.location.clone());
                        skipped_not_ready += 1;
                        continue;
                    }
                } else {
                    // Addressless candidates bypass every filter above (skip
                    // list, dedup, transient, readiness) because all of them
                    // key on the socket addr. If the router then selects one,
                    // the GET advance helpers can't use it as a wire target
                    // and the hop dies. Keep the inclusion (the candidate may
                    // gain an addr by send time) but make it visible (#4361).
                    tracing::debug!(
                        peer = ?conn.location,
                        target_location = %target_location.as_f64(),
                        "k_closest: including addressless candidate (bypasses addr-keyed filters)"
                    );
                }
                candidates.push(conn.location.clone());
            }
        }

        // If all connected peers failed the readiness check, fall back to using them anyway.
        // This prevents GET/SUBSCRIBE operations from failing with EmptyRing when the node
        // is connected but peers haven't yet sent ReadyState messages (e.g., early after
        // connecting, or in network topologies where the min_ready_connections threshold is
        // never satisfied). A warn-level log is emitted so operators know gating was bypassed.
        // Note: ConnectionManager::routing_candidates has the same fallback for PUT/UPDATE.
        if candidates.is_empty() && !not_ready_fallback.is_empty() {
            tracing::warn!(
                count = not_ready_fallback.len(),
                target_location = %target_location.as_f64(),
                "k_closest: no ready peers available, falling back to not-yet-ready peers to avoid EmptyRing"
            );
            candidates = not_ready_fallback;
        }

        // If the entire connection set was filtered out and transients carried
        // weight in that filtering, warn the operator. There is no transient
        // fallback by design — transient peers are short-TTL coordination slots
        // and routing through them just wastes hops — but a sustained
        // empty-candidate state means the local node has no viable routing
        // options for this hop, which is operator-visible information that
        // would otherwise only surface as an `EmptyRing` higher up.
        if candidates.is_empty() && skipped_transient > 0 {
            tracing::warn!(
                skipped_transient,
                target_location = %target_location.as_f64(),
                "k_closest: no viable peers — all eligible connections were transient \
                 (no fallback by design; routing will fail this hop)"
            );
        }

        // Sort candidates for deterministic input to select_k_best_peers
        candidates.sort();

        // Note: We intentionally do NOT fall back to known_locations here.
        // known_locations may contain peers we're not currently connected to,
        // and attempting to route to them would require establishing a new connection
        // which may fail (especially in NAT scenarios without coordination).
        // It's better to return fewer candidates than unreachable ones.

        let (selected, decision) = self.router.read().select_k_best_peers_with_telemetry(
            candidates.iter(),
            target_location,
            k,
        );
        // `selected` borrows from `candidates`, not from the router guard, so
        // the read lock is released here before the tracing/collect below.

        tracing::debug!(
            target_location = %target_location.as_f64(),
            strategy = ?decision.strategy,
            num_candidates = decision.candidates.len(),
            total_routing_events = decision.total_routing_events,
            selected_count = selected.len(),
            "routing_decision"
        );

        tracing::debug!(
            target_location = %target_location.as_f64(),
            candidates_found = selected.len(),
            skipped_not_ready,
            skipped_transient,
            "k_closest_potentially_hosting result"
        );

        selected.into_iter().cloned().collect()
    }

    pub fn routing_finished(&self, event: crate::router::RouteEvent) {
        self.connection_manager
            .topology_manager
            .write()
            .report_outbound_request(event.peer.clone(), event.contract_location);

        // Update peer health tracking based on routing outcome.
        if let Some(addr) = event.peer.socket_addr() {
            let mut health = self.connection_manager.peer_health.lock();
            match &event.outcome {
                crate::router::RouteOutcome::Success { .. }
                | crate::router::RouteOutcome::SuccessUntimed => {
                    health.record_success(addr);
                }
                crate::router::RouteOutcome::Failure => {
                    health.record_failure(addr);
                }
            }
        }

        self.router.write().add_event(event);
    }

    // ==================== Subscription Management (Lease-Based) ====================

    /// Subscribe to a contract with a lease.
    ///
    /// Creates a new subscription or renews an existing one. The subscription
    /// will expire after `SUBSCRIPTION_LEASE_DURATION` unless renewed.
    pub fn subscribe(&self, contract: ContractKey) -> SubscribeResult {
        self.hosting_manager.subscribe(contract)
    }

    /// Unsubscribe from a contract.
    ///
    /// Removes the active subscription. The contract may still be hosted
    /// (in the hosting cache) until evicted by LRU.
    pub fn unsubscribe(&self, contract: &ContractKey) {
        self.hosting_manager.unsubscribe(contract)
    }

    /// Check if we have an active (non-expired) subscription to a contract.
    pub fn is_subscribed(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.is_subscribed(contract)
    }

    /// Whether the FULL contract state (code + params + state) is present on
    /// disk for `contract` — the cheap on-disk state-store existence check, NOT
    /// the in-memory hosting cache. Hosting is binary: a `true` from a healthy
    /// store means this peer holds the whole contract.
    ///
    /// CAVEAT — conservative assume-present fallback: this returns `true` (not
    /// `false`) when the state store is transiently unavailable — a redb read
    /// error, an unset storage handle before startup, or a non-redb build. So a
    /// `true` is "present, OR presence currently unknowable", never a hard
    /// guarantee. In the reconcile shadow (keystone step-2, #4642) that can
    /// slightly INFLATE the renewal-site `announce` divergence during a transient
    /// store outage (the controller would `Announce` a host we can't confirm has
    /// the body); it is a startup/outage transient, documented so the counter is
    /// read correctly rather than mistaken for a real anomaly.
    pub(crate) fn contract_state_present(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.contract_state_present(contract)
    }

    /// Whether at least one LOCAL WebSocket client is currently subscribed to
    /// `contract` (real local demand, distinct from downstream peer
    /// subscribers). Used by the reconcile input-builder (keystone step-2,
    /// #4642) for `ReconcileInputs::has_local_client`.
    pub(crate) fn has_client_subscriptions(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.has_client_subscriptions(contract.id())
    }

    /// Lease-valid downstream subscriber peer keys for `contract` (see
    /// [`hosting::HostingManager::downstream_subscriber_peers`]). Used by the
    /// reconcile input-builder (keystone step-2, #4642) to apply the piece-D
    /// strictly-farther filter.
    pub(crate) fn downstream_subscriber_peers(&self, contract: &ContractKey) -> Vec<PeerKey> {
        self.hosting_manager.downstream_subscriber_peers(contract)
    }

    /// Get all contracts with active subscriptions.
    pub fn get_subscribed_contracts(&self) -> Vec<ContractKey> {
        self.hosting_manager.get_subscribed_contracts()
    }

    /// Bounded, sort-free lookup of currently-subscribed `ContractKey`s whose
    /// instance-id is in `wanted` (see
    /// [`hosting::HostingManager::subscribed_keys_in`]). Used by the reconcile
    /// connection-drop shadow (keystone step-2, #4642) to avoid the O(S log S)
    /// full-set scan of [`Self::get_subscribed_contracts`] per disconnect.
    pub(crate) fn subscribed_keys_in(
        &self,
        wanted: &std::collections::HashSet<ContractInstanceId>,
        scan_cap: usize,
        max_matches: usize,
    ) -> Vec<ContractKey> {
        self.hosting_manager
            .subscribed_keys_in(wanted, scan_cap, max_matches)
    }

    /// Force-expire a contract's subscription so it gets renewed through the
    /// current best route on the next recovery cycle.
    fn force_subscription_renewal(&self, contract: &ContractKey) {
        self.hosting_manager.force_subscription_renewal(contract);
    }

    /// Expire stale subscriptions and return the contracts that were expired.
    ///
    /// Should be called periodically by a background task.
    pub fn expire_stale_subscriptions(&self) -> Vec<ContractKey> {
        self.hosting_manager.expire_stale_subscriptions()
    }

    // ==================== Downstream Subscriber Tracking ====================

    pub fn add_downstream_subscriber(&self, contract: &ContractKey, peer: PeerKey) -> bool {
        let outcome = self
            .hosting_manager
            .add_downstream_subscriber(contract, peer);
        // No governance demand is ingested here anymore. Benefit is a
        // LIVE SNAPSHOT read fresh each reaper tick from the hosting
        // manager's standing subscriber count (see
        // `Ring::governance_tick` and
        // `HostingManager::downstream_subscriber_count`), not an
        // accumulator fed by subscribe events. That makes the
        // Sybil-on-renewal concern moot: renewals merely extend a lease
        // the snapshot already counts, so they cannot inflate benefit —
        // the count reflects the CURRENT lease-valid subscriber set, no
        // matter how often each peer renews.
        //
        // Downstream demand is still weighted low (FORWARDED_DEMAND_WEIGHT
        // = 0.1) when the snapshot is computed, because peer identities
        // are attacker-rotatable: without an identity layer a single
        // attacker can spin up many peers that each "subscribe", so each
        // forwarded subscriber is worth one tenth of a real local client.
        // Caller-facing bool preserved for backward compat: Rejected
        // → false; NewAdd or Renewal → true (the peer is tracked).
        !matches!(
            outcome,
            crate::ring::hosting::AddSubscriberOutcome::Rejected
        )
    }

    #[allow(dead_code)] // Only used in tests
    pub fn renew_downstream_subscriber(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        self.hosting_manager
            .renew_downstream_subscriber(contract, peer)
    }

    pub fn remove_downstream_subscriber(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        self.hosting_manager
            .remove_downstream_subscriber(contract, peer)
    }

    pub fn has_downstream_subscribers(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.has_downstream_subscribers(contract)
    }

    /// Whether something still depends on this node hosting `contract` — a
    /// live local client subscription or a downstream peer subscriber.
    ///
    /// Used to gate hosting-cache eviction reclamation: a contract that is in
    /// use must not have its on-disk state/code deleted. See
    /// `HostingManager::contract_in_use` for why an active upstream network
    /// subscription alone does NOT make the contract in-use (the renewal
    /// machinery would refresh the lease unboundedly).
    pub(crate) fn contract_in_use(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.contract_in_use(contract)
    }

    /// Single helper for every state-write chokepoint. Does the three
    /// things a chokepoint MUST do, in order:
    ///
    /// 1. Bump the per-contract write generation (closes the
    ///    `EvictContract` re-host race — see
    ///    `HostingManager::state_generation`).
    /// 2. Refresh the hosting-cache snapshot of that generation so
    ///    already-hosted contracts don't leak on eviction after this
    ///    write — see `HostingCache::refresh_entry_generation`.
    /// 3. Report `state_size` bytes against this contract on the
    ///    `StateBytesWritten` axis of the topology meter, feeding the
    ///    governance scoring layer (see `crate::governance`).
    ///
    /// Every state-write chokepoint in the executor MUST go through
    /// this helper, NOT call the three primitives by hand. The
    /// "manually-mirrored side effects after a task-per-tx migration"
    /// pattern in `.claude/rules/bug-prevention-patterns.md` lists this
    /// exact failure mode: one site drops the report and governance
    /// silently undercounts that path for months before anyone notices.
    /// Pre-write admission gate for a state write (#4683, PR 3). Call this
    /// BEFORE the `state_store.{store,update}` at every chokepoint: it rejects a
    /// write that would push aggregate on-disk usage past the disk budget, so no
    /// bytes ever land for a rejected write (no rollback needed for the write
    /// itself). Read-only — the `+delta` is applied by
    /// [`Self::commit_state_write`] only on the post-write success path, so a
    /// rejected or later-failed write never mutates the tracker.
    ///
    /// A no-op admit until the disk tracker is seeded (early startup), so it
    /// never spuriously blocks a write before the aggregate is meaningful.
    ///
    /// On rejection the chokepoint converts the error to a non-fatal
    /// `ExecutorError::request(StdContractError::Put/Update)` → `new_value: Err`,
    /// which for PUT rides `PutMsg::Error` to the client and the network.
    pub(crate) fn admit_state_write(
        &self,
        contract: &ContractKey,
        new_size: usize,
    ) -> Result<(), DiskBudgetExceeded> {
        self.hosting_manager
            .admit_state_write(contract, new_size as u64)
    }

    /// Pre-write admission gate for a state **UPDATE** to an already-hosted
    /// contract (#4683). Growth-only: a shrinking or size-holding UPDATE
    /// (`new_size <= old`) is admitted unconditionally, even when the aggregate
    /// is over budget, because an UPDATE mutates an already-counted footprint and
    /// rejecting it would stall CRDT convergence without freeing any bytes (and a
    /// relayed UPDATE rejection is silently dropped — no one would learn of the
    /// stall). Only genuine growth is subjected to the aggregate bound. Use this
    /// at UPDATE / re-PUT-merge chokepoints; PUT of a NEW contract keeps the hard
    /// [`Self::admit_state_write`] gate.
    pub(crate) fn admit_state_update(
        &self,
        contract: &ContractKey,
        new_size: usize,
    ) -> Result<(), DiskBudgetExceeded> {
        self.hosting_manager
            .admit_state_update(contract, new_size as u64)
    }

    /// Pre-write admission gate for a newly-stored (deduped) WASM code blob
    /// (#4683, PR 3). Call this BEFORE `runtime.store_contract` for a blob that
    /// is not already on disk. See [`Self::admit_state_write`] for the
    /// deferred-delta / rollback discipline.
    pub(crate) fn admit_wasm_write(&self, blob_len: usize) -> Result<(), DiskBudgetExceeded> {
        self.hosting_manager.admit_wasm_write(blob_len as u64)
    }

    /// Charge a newly-stored (deduped) WASM code blob to the disk tracker on the
    /// post-store success path (#4683). Call this AFTER a successful
    /// `runtime.store_contract` for a blob that was not already on disk, so the
    /// aggregate reflects it immediately (burst protection + so the state gate on
    /// the same PUT sees it). See [`super::hosting::HostingManager::record_wasm_write`].
    pub(crate) fn record_wasm_write(&self, blob_len: usize) {
        self.hosting_manager.record_wasm_write(blob_len as u64);
    }

    /// Subtract a WASM code blob's contribution from the disk tracker on contract
    /// removal (#4683). Mirror of [`Self::record_wasm_write`].
    pub(crate) fn record_wasm_removed(&self, blob_len: usize) {
        self.hosting_manager.record_wasm_removed(blob_len as u64);
    }

    /// Test-only passthroughs to the `HostingManager` disk-budget seams (#4683).
    /// `hosting_manager` is a private field, so an executor-path test holding an
    /// `Arc<OpManager>` cannot reach the seeding/budget helpers directly; these
    /// forward to them so a wired `Executor<_>` can be driven into a disk-budget
    /// rejection without waiting for the 60s recompute. No production behavior.
    #[cfg(test)]
    pub(crate) fn seed_disk_tracker_for_test<I>(&self, rows: I)
    where
        I: IntoIterator<Item = (ContractKey, u64)>,
    {
        self.hosting_manager.seed_disk_tracker_for_test(rows);
    }

    #[cfg(test)]
    pub(crate) fn configure_disk_budget_for_test(&self, disk_pct: f64, max_hosting_disk: u64) {
        self.hosting_manager
            .configure_disk_budget(disk_pct, max_hosting_disk);
    }

    #[cfg(test)]
    pub(crate) fn recompute_effective_budget_for_test(&self, available: u64) -> Option<u64> {
        self.hosting_manager.recompute_effective_budget(available)
    }

    #[cfg(test)]
    pub(crate) fn disk_budget_bytes_for_test(&self) -> u64 {
        self.hosting_manager.disk_budget_bytes()
    }

    #[cfg(test)]
    pub(crate) fn disk_usage_stats_for_test(&self) -> Option<hosting::DiskUsageStats> {
        self.hosting_manager.disk_usage_stats()
    }

    pub(crate) fn commit_state_write(&self, contract: &ContractKey, state_size: usize) {
        let new_gen = self.hosting_manager.bump_state_generation(contract);
        self.hosting_manager
            .refresh_cache_generation(contract, new_gen);
        // Disk-usage accounting (#4683): maintain the aggregate on-disk state
        // total by signed delta (`new − previous_for_key`). Observational only
        // in this PR — no admission/eviction decision reads it yet. No-op until
        // the tracker is seeded.
        self.hosting_manager
            .record_state_write(contract, state_size as u64);
        self.report_contract_resource_usage(
            *contract.id(),
            crate::topology::meter::ResourceType::StateBytesWritten,
            state_size as f64,
        );
    }

    /// Subtract a reclaimed contract's state bytes from the aggregate disk-usage
    /// tracker (#4683). Called from the executor's reclaim path on successful
    /// state deletion. Observational only in this PR; no-op until the tracker is
    /// seeded.
    pub(crate) fn record_state_removed(&self, contract: &ContractKey) {
        self.hosting_manager.record_state_removed(contract);
    }

    /// Read the current state-write generation for `contract` (0 if never written).
    pub(crate) fn state_generation(&self, contract: &ContractKey) -> u64 {
        self.hosting_manager.state_generation(contract)
    }

    /// Forget the state-write generation entry for `contract` after a
    /// successful disk reclamation. Keeps the generation map bounded.
    pub(crate) fn forget_state_generation(&self, contract: &ContractKey) {
        self.hosting_manager.forget_state_generation(contract)
    }

    /// Add `contract` to the pending-reclamation retry queue with the
    /// captured `expected_generation`. Called from the two skip points
    /// that drop an `EvictContract` event before it can complete (fair
    /// queue rejection, `contract_in_use` skip in `RuntimePool::remove_contract`).
    /// See `HostingManager::pending_reclamation_add` for the queue's
    /// invariants and how the periodic sweep retries entries.
    pub(crate) fn pending_reclamation_add(&self, contract: ContractKey, expected_generation: u64) {
        self.hosting_manager
            .pending_reclamation_add(contract, expected_generation)
    }

    /// Remove `contract` from the pending-reclamation retry queue after
    /// a successful disk reclamation. See
    /// `HostingManager::pending_reclamation_remove`.
    pub(crate) fn pending_reclamation_remove(&self, contract: &ContractKey) {
        self.hosting_manager.pending_reclamation_remove(contract)
    }

    /// Snapshot the pending-reclamation queue for the periodic sweep
    /// to iterate without holding any DashMap shard guard. See
    /// `HostingManager::pending_reclamation_snapshot`.
    pub(crate) fn pending_reclamation_snapshot(&self) -> Vec<(ContractKey, u64)> {
        self.hosting_manager.pending_reclamation_snapshot()
    }

    pub fn expire_stale_downstream_subscribers(&self) -> Vec<(ContractKey, usize)> {
        self.hosting_manager.expire_stale_downstream_subscribers()
    }

    /// Reconcile downstream-driven in-use contracts against the state store
    /// (#4612): emit repair fetches for phantoms (in-use, stateless) and
    /// drops for unrepairable ones. See
    /// `HostingManager::reconcile_phantom_in_use`.
    pub(crate) fn reconcile_phantom_in_use(
        &self,
        max_fetches: usize,
    ) -> Vec<crate::ring::hosting::PhantomRepair> {
        self.hosting_manager.reconcile_phantom_in_use(max_fetches)
    }

    pub fn should_unsubscribe_upstream(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.should_unsubscribe_upstream(contract)
    }

    /// Check if this node is actively receiving updates for a contract.
    ///
    /// Returns true only when we have an active network subscription or local
    /// client subscriptions. The hosting LRU cache alone is not sufficient,
    /// since cached state may be stale after subscription expiry.
    pub fn is_receiving_updates(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.is_receiving_updates(contract)
    }

    /// Get contracts that need subscription renewal.
    ///
    /// Returns contracts where:
    /// - We have an active subscription that will expire soon, OR
    /// - We have client subscriptions but no active network subscription, OR
    /// - We have hosted contracts without active subscriptions (THE FIX)
    pub fn contracts_needing_renewal(&self) -> Vec<ContractKey> {
        self.hosting_manager.contracts_needing_renewal()
    }

    // ==================== Client Subscription Management ====================

    /// Register a client subscription for a contract (WebSocket client subscribed).
    ///
    /// Returns information about the operation for telemetry.
    pub fn add_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> AddClientSubscriptionResult {
        // No governance demand is ingested here anymore. A local client
        // subscription is the strong demand signal (full
        // LOCAL_DEMAND_WEIGHT = 1.0), but benefit is now a LIVE SNAPSHOT
        // read fresh each reaper tick from
        // `HostingManager::local_client_count` (see
        // `Ring::governance_tick`), not an accumulator fed here. The
        // snapshot counts the currently-subscribed client set, so an
        // idempotent re-subscribe cannot inflate benefit and there is no
        // need to gate on `is_new_for_client` for scoring purposes.
        self.hosting_manager
            .add_client_subscription(instance_id, client_id)
    }

    /// Remove a single client's subscription to `instance_id`.
    ///
    /// Used on the failure path of a subscribe=true GET/PUT (step 10 §1e): the
    /// client subscription is registered up-front in `client_events` (for the
    /// #4524 don't-miss-updates ordering), so a dead-end GET or a failed PUT
    /// would otherwise leave it registered — `contract_in_use` true with no state
    /// — until the client disconnects (a phantom). The driver removes it on any
    /// terminal that delivered no state. A no-op returning `false` when the
    /// client had no subscription to the contract, so it is safe to call
    /// unconditionally on the failure path. Returns `true` if this was the
    /// contract's last client subscription.
    pub(crate) fn remove_client_subscription(
        &self,
        instance_id: &ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> bool {
        self.hosting_manager
            .remove_client_subscription(instance_id, client_id)
    }

    /// Record the start of an in-flight subscribe=true GET/PUT for
    /// `(instance_id, client_id)` (review Fix 5). See
    /// `HostingManager::begin_inflight_subscribe`.
    pub(crate) fn begin_inflight_subscribe(
        &self,
        instance_id: ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) {
        self.hosting_manager
            .begin_inflight_subscribe(instance_id, client_id)
    }

    /// Record the completion of one in-flight subscribe=true GET/PUT and return
    /// how many remain in flight for `(instance_id, client_id)` (review Fix 5).
    /// See `HostingManager::end_inflight_subscribe`.
    pub(crate) fn end_inflight_subscribe(
        &self,
        instance_id: ContractInstanceId,
        client_id: crate::client_events::ClientId,
    ) -> usize {
        self.hosting_manager
            .end_inflight_subscribe(instance_id, client_id)
    }

    /// Remove a client from all its subscriptions (used when client disconnects).
    ///
    /// Returns a [`ClientDisconnectResult`] with:
    /// - `affected_contracts`: all contracts where the client was subscribed (for cleanup)
    pub fn remove_client_from_all_subscriptions(
        &self,
        client_id: crate::client_events::ClientId,
    ) -> ClientDisconnectResult {
        self.hosting_manager
            .remove_client_from_all_subscriptions(client_id)
    }

    /// Get all hosted contract keys from the hosting cache.
    pub fn hosting_contract_keys(&self) -> Vec<ContractKey> {
        self.hosting_manager.hosting_contract_keys()
    }

    /// Get the cached state size in bytes for a hosted contract.
    pub fn hosting_contract_size(&self, key: &ContractKey) -> u64 {
        self.hosting_manager.hosting_contract_size(key)
    }

    /// Get the number of contracts in the hosting cache.
    /// This is the actual count of contracts this node is caching/hosting.
    pub fn hosting_contracts_count(&self) -> usize {
        self.hosting_manager.hosting_contracts_count()
    }

    /// Number of active network subscription leases this node currently holds.
    ///
    /// Together with [`hosting_contracts_count`](Self::hosting_contracts_count)
    /// and [`active_demand_count`](Self::active_demand_count), this is the
    /// per-node measurement a simulation test asserts on to detect a #3763-style
    /// subscription storm: a healthy node's subscription count tracks active
    /// demand, NOT cache size.
    ///
    /// Test/sim-only accessor (read by `ControlledSimulationResult`).
    #[cfg(any(test, feature = "testing"))]
    pub fn active_subscription_count(&self) -> usize {
        self.hosting_manager.active_subscription_count()
    }

    /// Number of contracts this node has *real demand* for — a local client
    /// subscription or a downstream subscriber — EXCLUDING cache-only hosting.
    ///
    /// Reads the live `InterestManager` (on the `OpManager`) via the Ring's
    /// back-reference. Returns `None` if the `OpManager` is not attached yet or
    /// has been torn down (the normal startup / shutdown windows) — distinct
    /// from `Some(0)` ("attached, genuinely no demand"). This distinction is
    /// load-bearing for the #3763 no-storm invariant: a no-storm assertion must
    /// NOT treat a `None` (unmeasurable, detached node) as "demand == 0" and
    /// falsely conclude there was no storm. See
    /// [`InterestManager::active_demand_count`](crate::ring::interest::InterestManager::active_demand_count).
    ///
    /// Test/sim-only accessor (read by `ControlledSimulationResult`).
    #[cfg(any(test, feature = "testing"))]
    pub fn active_demand_count(&self) -> Option<usize> {
        self.upgrade_op_manager()
            .map(|op_manager| op_manager.interest_manager.active_demand_count())
    }

    /// Number of *upstream* peers this node has recorded for `contract` — i.e.
    /// peers it subscribed THROUGH (its parent in the subscription tree). Reads
    /// the live `InterestManager`'s `is_upstream` edges.
    ///
    /// This projects the subscription-tree upstream edge that the topology
    /// snapshot does NOT carry (it hardcodes `upstream: None`), giving a sim test
    /// a way to assert a KNOWN `subscriber → upstream` edge — e.g. verify the
    /// edge exists, crash the upstream, then observe the edge re-form (#4642
    /// piece F). NOTE: an upstream edge is only recorded when the subscription
    /// resolved at a real connected peer (not at the originator/root), so a test
    /// must place the contract key so the subscribe routes through an
    /// intermediate hop. Returns `None` if the `OpManager` is not attached
    /// (unmeasurable) — distinct from `Some(0)` ("attached, no upstream edge").
    #[cfg(any(test, feature = "testing"))]
    pub fn upstream_interest_count(&self, contract: &ContractKey) -> Option<usize> {
        self.upgrade_op_manager().map(|op_manager| {
            op_manager
                .interest_manager
                .get_interested_peers(contract)
                .into_iter()
                .filter(|(_, interest)| interest.is_upstream)
                .count()
        })
    }

    /// Get subscription state for all contracts (for telemetry).
    ///
    /// Returns: (contract, has_client_subscription, is_active_subscription, expires_at)
    pub fn get_subscription_states(&self) -> Vec<(ContractKey, bool, bool, Option<Instant>)> {
        self.hosting_manager.get_subscription_states()
    }

    /// Snapshot of every active subscription for the local-peer dashboard.
    /// Reads directly from the canonical lease map.
    pub fn dashboard_subscription_snapshot(&self) -> Vec<SubscribedContractSnapshot> {
        self.hosting_manager.dashboard_subscription_snapshot()
    }

    /// Snapshot of per-contract governance state for the local-peer
    /// dashboard. Reads directly from the canonical `GovernanceManager`
    /// state — no mirror, no cache, no derived recomputation. If a
    /// state appears here it's because the manager computed it from
    /// real meter samples and real subscription events.
    pub fn dashboard_governance_snapshot(&self) -> crate::node::network_status::GovernanceSnapshot {
        use crate::contract::governance as gov;
        use crate::node::network_status as ns;

        let now = self.time_source.now();
        let mode = match self.governance.mode() {
            gov::GovernanceMode::Off => ns::GovernanceModeSnapshot::Off,
            gov::GovernanceMode::DryRun => ns::GovernanceModeSnapshot::DryRun,
            gov::GovernanceMode::Enforce => ns::GovernanceModeSnapshot::Enforce,
        };

        let map_state = |s: gov::GovernanceState| match s {
            gov::GovernanceState::Normal => ns::GovernanceStateSnapshot::Normal,
            gov::GovernanceState::Borderline => ns::GovernanceStateSnapshot::Borderline,
            gov::GovernanceState::WouldEvict => ns::GovernanceStateSnapshot::WouldEvict,
            gov::GovernanceState::Evicted => ns::GovernanceStateSnapshot::Evicted,
            gov::GovernanceState::Banned => ns::GovernanceStateSnapshot::Banned,
        };
        let map_reason = |r: gov::TransitionReason| match r {
            gov::TransitionReason::FirstSeen => ns::GovernanceTransitionReasonSnapshot::FirstSeen,
            gov::TransitionReason::BorderlineEntered => {
                ns::GovernanceTransitionReasonSnapshot::BorderlineEntered
            }
            gov::TransitionReason::ThresholdCrossed => {
                ns::GovernanceTransitionReasonSnapshot::ThresholdCrossed
            }
            gov::TransitionReason::Evicted => ns::GovernanceTransitionReasonSnapshot::Evicted,
            gov::TransitionReason::BanTriggered => {
                ns::GovernanceTransitionReasonSnapshot::BanTriggered
            }
            gov::TransitionReason::Recovered => ns::GovernanceTransitionReasonSnapshot::Recovered,
            gov::TransitionReason::BanLifted => ns::GovernanceTransitionReasonSnapshot::BanLifted,
        };

        // Only iterate flagged contracts for the dashboard mirror —
        // the renderer hides Normal anyway. Avoids cloning thousands of
        // entries per refresh on a busy node. See `iter_flagged_scores`.
        let contracts: Vec<ns::ContractGovernanceEntry> = self
            .governance
            .iter_flagged_scores()
            .into_iter()
            .map(|(id, score)| {
                let instance_id = id.to_string();
                let instance_id_short = if instance_id.chars().count() > 12 {
                    let trunc: String = instance_id.chars().take(12).collect();
                    format!("{trunc}...")
                } else {
                    instance_id.clone()
                };
                let history = score
                    .history
                    .iter()
                    .map(|t| ns::GovernanceTransitionEntry {
                        secs_ago: now.saturating_duration_since(t.at).as_secs(),
                        from: map_state(t.from),
                        to: map_state(t.to),
                        reason: map_reason(t.reason),
                    })
                    .collect();
                ns::ContractGovernanceEntry {
                    instance_id,
                    instance_id_short,
                    state: map_state(score.state),
                    cost_used: score.cost_used,
                    benefit_score: score.benefit_score,
                    log_ratio: score.log_ratio(self.governance.benefit_floor()),
                    age_secs: now.saturating_duration_since(score.first_seen).as_secs(),
                    last_transition_secs_ago: now
                        .saturating_duration_since(score.last_transition)
                        .as_secs(),
                    history,
                }
            })
            .collect();

        let norms = match self.governance.latest_norms() {
            Some(n) => ns::NetworkNorms {
                median_log_ratio: n.median_log_ratio,
                mad: n.mad,
                threshold: n.threshold,
                sample_size: n.sample_size,
                capacity_ceiling_binding: n.capacity_ceiling_binding,
                skip_reason: n.skip_reason.map(|r| match r {
                    crate::governance::SkipReason::InsufficientSamples => {
                        ns::GovernanceSkipReasonSnapshot::InsufficientSamples
                    }
                    crate::governance::SkipReason::MadCollapsed => {
                        ns::GovernanceSkipReasonSnapshot::MadCollapsed
                    }
                    crate::governance::SkipReason::NoExtractableRatios => {
                        ns::GovernanceSkipReasonSnapshot::NoExtractableRatios
                    }
                }),
            },
            None => ns::NetworkNorms::default(),
        };

        // state_by_id: map ContractInstanceId → state for the
        // Subscribed Contracts table's Gov column cross-reference.
        // Walking iter_flagged_scores() once for the `contracts`
        // list above gave us the flagged set; for unflagged
        // contracts the absence from this map means "Normal".
        let state_by_id: std::collections::HashMap<String, ns::GovernanceStateSnapshot> = contracts
            .iter()
            .map(|c| (c.instance_id.clone(), c.state))
            .collect();

        let observed_count = self.governance.len();
        let min_samples = self.governance.outlier_min_samples();
        let last_tick_at = self.governance.latest_norms().map(|n| n.at);

        ns::GovernanceSnapshot {
            mode,
            contracts,
            observed_count,
            min_samples,
            norms,
            last_tick_at,
            state_by_id,
        }
    }

    /// Snapshot of the demand-driven hosting state for the local-peer
    /// dashboard (piece A, #4642). Reads the canonical hosting cache — the
    /// capability-relative RAM budget + per-contract Greedy-Dual keep_score
    /// that actually governs retention today, replacing the dormant MAD
    /// governance detector (#4296). No mirror, no cache: the aggregate gauges
    /// and per-contract rows come straight from the `HostingManager`, so the
    /// panel can't drift the way a mirrored counter would.
    ///
    /// Per-contract rows are returned in EVICTION order (next victim first).
    /// The renderer bounds how many it displays; the full count is
    /// `contract_count`.
    pub fn dashboard_hosting_snapshot(&self) -> crate::node::network_status::HostingSnapshot {
        use crate::node::network_status as ns;

        let stats = self.hosting_manager.hosting_cache_stats();
        // Two-phase: `dashboard_hosting_scores()` returns owned rows with the
        // `hosting_cache` read lock already dropped, so folding in
        // `is_eviction_eligible` (which reads only the subscription maps via
        // `contract_in_use`) holds no cache lock — no re-lock deadlock.
        let contracts: Vec<ns::HostedContractEntry> = self
            .hosting_manager
            .dashboard_hosting_scores()
            .into_iter()
            .map(|row| {
                let eviction_eligible = self.hosting_manager.is_eviction_eligible(&row);
                let key_full = row.key.to_string();
                let key_short = if key_full.chars().count() > 12 {
                    let trunc: String = key_full.chars().take(12).collect();
                    format!("{trunc}...")
                } else {
                    key_full.clone()
                };
                ns::HostedContractEntry {
                    key_full,
                    key_short,
                    keep_score: row.keep_score,
                    predicted_demand: row.predicted_demand,
                    size_bytes: row.size_bytes,
                    read_count: row.read_count,
                    eviction_eligible,
                }
            })
            .collect();

        // Aggregate on-disk usage (#4683) + the disk budget the admission gate
        // checks against (#4702), for the same "measuring…" pre-seed gate the
        // `hosting_disk_*` telemetry gauges use (see the population above at
        // the `RouterSnapshot` disk-usage block): `None` until the tracker is
        // seeded, so an unseeded tracker is distinguishable from genuine zero
        // usage.
        let disk = self.hosting_manager.disk_usage_stats();
        let disk_state_bytes = disk.map(|d| d.state_bytes);
        let disk_wasm_bytes = disk.map(|d| d.wasm_bytes);
        let disk_compile_cache_bytes = disk.map(|d| d.compile_cache_bytes);
        let disk_total_bytes = disk.map(|d| d.total_bytes);
        // `disk_budget_bytes` starts at `u64::MAX` until the first 60s
        // recompute installs a real value (see `HostingManager::
        // recompute_effective_budget`); surface that as `None` rather than an
        // astronomical byte count.
        let raw_disk_budget = self.hosting_manager.disk_budget_bytes();
        let disk_budget_bytes = (raw_disk_budget != u64::MAX).then_some(raw_disk_budget);

        ns::HostingSnapshot {
            budget_bytes: stats.budget_bytes,
            used_bytes: stats.current_bytes,
            contract_count: stats.contract_count,
            budget_evictions_total: stats.budget_evictions_total,
            evictions_of_recently_read_total: stats.evictions_of_recently_read_total,
            contracts,
            disk_state_bytes,
            disk_wasm_bytes,
            disk_compile_cache_bytes,
            disk_total_bytes,
            disk_budget_bytes,
        }
    }

    /// Snapshot of the contract ban list for the local-peer dashboard
    /// (#4302). Reads directly from the canonical `contract_ban_list` —
    /// no mirror, no cache. The count, capacity-rejection counter, and
    /// per-entry list (key + reason + time remaining) all come from the
    /// list's own accessors, so the panel can't drift the way a mirrored
    /// counter would.
    pub fn dashboard_ban_list_snapshot(&self) -> crate::node::network_status::BanListSnapshot {
        use crate::node::network_status as ns;
        use crate::ring::contract_ban_list::BanReason;

        let map_reason = |r: BanReason| match r {
            BanReason::AutoMad => ns::BanReasonSnapshot::AutoMad,
            BanReason::Operator => ns::BanReasonSnapshot::Operator,
        };

        let mut entries: Vec<ns::BanListEntry> = self
            .contract_ban_list
            .snapshot()
            .into_iter()
            .map(|e| ns::BanListEntry {
                instance_id: e.contract.to_string(),
                reason: map_reason(e.reason),
                expires_in_secs: e.remaining.as_secs(),
            })
            .collect();
        // Stable display order: soonest-to-lift first, then by id so the
        // dashboard doesn't reshuffle rows between refreshes (DashMap
        // iteration order is unspecified).
        entries.sort_by(|a, b| {
            a.expires_in_secs
                .cmp(&b.expires_in_secs)
                .then_with(|| a.instance_id.cmp(&b.instance_id))
        });

        // Count LIVE entries — derive from the filtered snapshot, not
        // `ContractBanList::len()`. `len()` includes entries that have
        // expired but not yet been swept by `cleanup()`/`unban()`;
        // `snapshot()` filters those out with the same `now < expires_at`
        // predicate as `is_banned`. Using `len()` here would let the
        // count tile read "1 contract banned" while the entry list (and
        // the wire boundary) show zero — an inconsistency in the
        // expiry-before-sweep window. (Codex review on #4464.)
        ns::BanListSnapshot {
            count: entries.len(),
            capacity_rejected_total: self.contract_ban_list.capacity_rejected_total(),
            entries,
        }
    }

    /// Record that a state update was observed for `contract`.
    /// No-op if the contract is not currently subscribed.
    pub fn record_contract_update(&self, contract: &ContractKey) {
        self.hosting_manager.record_contract_update(contract)
    }

    /// True if `contract` has been flagged as violating a CRDT invariant
    /// (e.g. non-idempotent `update_state`). Callers on the broadcast path
    /// must skip emission when this returns true to prevent the broken
    /// contract from generating a propagation storm.
    pub fn is_contract_broken(&self, contract: &ContractKey) -> bool {
        self.broken_invariants.is_broken(contract.id())
    }

    /// Mark `contract` as broken with `kind`. Idempotent. See
    /// [`broken_invariants`] module docs.
    pub(crate) fn record_broken_invariant(&self, contract: ContractKey, kind: BrokenInvariant) {
        self.broken_invariants.record(*contract.id(), kind);
    }

    /// Wire persistent storage for the broken-invariants tracker. Called
    /// once at executor wiring time.
    pub(crate) fn set_broken_invariants_storage(
        &self,
        storage: crate::contract::storages::Storage,
    ) {
        self.broken_invariants.set_storage(storage);
    }

    /// Report a per-contract resource sample to the topology meter.
    ///
    /// Lightweight non-blocking write: takes the topology manager's
    /// write lock briefly to insert into the running-average store.
    /// Safe to call from executor commit paths — does NOT use channels
    /// (cf. `.claude/rules/channel-safety.md`); the May 2026 deadlock
    /// pattern (`#4145`) is not reachable through this surface.
    ///
    /// Used by `Executor::commit_state_update` to attribute state-write
    /// bytes to a contract, and (in follow-up work) by the WASM call
    /// wrappers for CPU/fuel and the broadcast dispatcher for fanout
    /// cost. Per-contract attribution lets the shared governance
    /// outlier-detection module (`crate::governance`) score contracts
    /// against the network's observed cost-per-benefit distribution.
    pub(crate) fn report_contract_resource_usage(
        &self,
        contract_id: freenet_stdlib::prelude::ContractInstanceId,
        resource: crate::topology::meter::ResourceType,
        amount: f64,
    ) {
        // Use the injectable `TimeSource` (rather than `Instant::now()`
        // directly) so deterministic simulation tests can drive this
        // path. Per `.claude/rules/code-style.md` "Need current time?
        // → USE: TimeSource trait" — the executor commit path will reach
        // here from inside simulated nodes once the governance scoring
        // integration tests land, and reading wall-clock there would
        // break determinism.
        let now = self.time_source.now();
        let mut topo = self.connection_manager.topology_manager.write();
        topo.report_resource_usage(
            &crate::topology::meter::AttributionSource::Contract(contract_id),
            resource,
            amount,
            now,
        );
        drop(topo);
        // Also feed the governance manager — the meter stores rates
        // for telemetry/the dashboard's bandwidth view, while the
        // governance manager aggregates these samples into the
        // cost/benefit ratio that drives state. Both ingest in
        // parallel from this one entry point so there's no risk of
        // them diverging (governance reading from a stale meter
        // snapshot, etc).
        //
        // Per-resource weight: identity (1.0) for now. Future tuning
        // could weight CPU-µs and fanout-cost higher than
        // state-bytes, but until we have live data to calibrate
        // against, unit weights match what the design doc proposed
        // as a starting point.
        let weight = resource_weight(resource);
        self.governance.ingest_cost(contract_id, amount * weight);
    }

    // Note: there is intentionally no `ingest_contract_demand` method
    // here, and demand is no longer pushed on subscribe events at all.
    // Benefit is a LIVE SNAPSHOT pulled each reaper tick by
    // `governance_tick` from the hosting manager's standing subscriber
    // counts (`local_client_count` + `downstream_subscriber_count`).
    // A push entry point would re-introduce the accumulate-on-event
    // model this redesign removed (#4296).

    /// Run one governance reaper tick. Caller (the periodic
    /// `governance_reaper_loop` task) is expected to feed
    /// `tick_interval` = wall-time since the previous tick for cost
    /// decay. Returns the `ReaperTickResult` for the caller to act on
    /// (emit `EvictContract` events for actionable decisions, log
    /// dry-run decisions, surface stats on the dashboard).
    ///
    /// Before ticking, this builds the LIVE benefit snapshot for every
    /// contract the governance manager is tracking: for each tracked
    /// contract id, `LOCAL_DEMAND_WEIGHT × current local-client count +
    /// FORWARDED_DEMAND_WEIGHT × current (non-expired) downstream
    /// subscriber count`, read fresh from the hosting manager. This is
    /// the cost-per-current-beneficiary denominator — a popular
    /// contract keeps a high benefit because its beneficiaries are
    /// counted live, not decayed from old subscribe events.
    pub(crate) fn governance_tick(
        &self,
        tick_interval: Duration,
    ) -> crate::contract::governance::ReaperTickResult {
        // Single-pass live benefit snapshot: one iteration over
        // `client_subscriptions` and one over `downstream_subscribers`
        // (see `HostingManager::beneficiary_counts`), then filter to the
        // contracts governance is tracking. This replaces the previous
        // shape that deep-cloned every `ContractScore` (incl. its
        // history Vec) via `iter_scores()` and then re-scanned the whole
        // `downstream_subscribers` map per contract — O(N×M) per 60s
        // tick. `tracked_ids()` returns keys only (no score clone).
        let tracked = self.governance.tracked_ids();
        let mut benefits = self
            .hosting_manager
            .beneficiary_counts(LOCAL_DEMAND_WEIGHT, FORWARDED_DEMAND_WEIGHT);
        // Keep only tracked contracts (a contract with beneficiaries but
        // no ingested cost has no score and must not enter the map —
        // preserves the prior per-contract behavior).
        benefits.retain(|id, _| tracked.contains(id));
        self.governance.tick(tick_interval, &benefits)
    }

    /// Test-only accessor: current local-client beneficiary count for a
    /// contract, read from the hosting manager. Used by the governance
    /// sim e2e tests to assert the live benefit snapshot reflects real
    /// subscriptions.
    #[cfg(all(test, feature = "simulation_tests"))]
    pub(crate) fn hosting_manager_local_client_count(
        &self,
        instance_id: &ContractInstanceId,
    ) -> usize {
        self.hosting_manager.local_client_count(instance_id)
    }

    /// Test-only accessor: current (non-expired) downstream subscriber
    /// count for a contract, read from the hosting manager.
    #[cfg(all(test, feature = "simulation_tests"))]
    pub(crate) fn hosting_manager_downstream_subscriber_count(
        &self,
        instance_id: &ContractInstanceId,
    ) -> usize {
        self.hosting_manager
            .downstream_subscriber_count(instance_id)
    }
}

/// Per-resource weight for cost aggregation. Defaults to 1.0 for all
/// dimensions so total cost = simple sum of meter samples. Hooks here
/// for future tuning if any one dimension proves to dominate the
/// signal-to-noise ratio.
fn resource_weight(resource: crate::topology::meter::ResourceType) -> f64 {
    use crate::topology::meter::ResourceType::*;
    match resource {
        InboundBandwidthBytes | OutboundBandwidthBytes => 1.0,
        ExecCpuMicros | ExecFuelUnits => 1.0,
        StateBytesWritten | BroadcastFanoutCost => 1.0,
    }
}

/// Bridge the transport layer's per-peer wire-byte counters into the
/// topology meter so `TopologyManager::adjust_topology` can make load-aware
/// connection decisions in production (#3453).
///
/// `TRANSPORT_METRICS.per_peer_snapshot()` exposes *cumulative* sent/received
/// byte counts per peer socket address. The topology meter, by contrast,
/// accumulates each reported sample into a sliding-window sum and divides by
/// the elapsed wall-time (`now − oldest_sample`) to produce a rate. We diff
/// the current snapshot against the previous tick's counts (`prev`) to recover
/// the bytes transferred *during the interval*, and feed that delta as one
/// sample per direction:
///   - received bytes → `InboundBandwidthBytes`
///   - sent bytes     → `OutboundBandwidthBytes`
///
/// `interval_start` MUST be the time of the *previous* maintenance tick, not
/// the current one. The meter divides the windowed byte sum by
/// `query_time − oldest_sample_time` with a 1-second floor
/// (`RunningAverage::get_rate_at_time`). `adjust_topology` queries the meter at
/// the current tick time immediately after this call, so timestamping the
/// interval's whole byte delta at the interval START makes the very first
/// sample divide by the real interval length (e.g. ~60s) instead of flooring
/// to 1s. Timestamping at the interval END would treat a full interval's bytes
/// as if they were transferred in a single second, overstating the rate by up
/// to the tick interval (~60×) and triggering spurious connection removals
/// under ordinary traffic (#3453 review).
///
/// Only deltas with a resolvable `PeerKeyLocation` are reported: an address
/// that doesn't (yet) map to a ring peer — e.g. a transient handshake
/// connection — is skipped rather than attributed to a phantom source, so we
/// don't pollute the meter with samples that can't be matched against the
/// node's actual neighbors.
///
/// Three things are bounded as peers churn (#3453 review):
///   - `prev` is pruned of peers absent from `snapshot`, so it stays bounded
///     by the transport metrics table size (`MAX_TRACKED_PEERS`).
///   - The topology meter's per-source bandwidth meters AND
///     `source_creation_times` are pruned to the set of peers resolved this
///     tick via `retain_peer_sources`, so neither grows without bound (and
///     `adjust_topology`, which iterates `source_creation_times` every tick,
///     stays O(live peers)).
///
/// This takes the topology manager's write lock once per reported delta plus
/// once for the retain. It performs no channel sends and no `.await`, so it is
/// safe to call inline on the `connection_maintenance` tick
/// (cf. `.claude/rules/channel-safety.md`).
fn feed_peer_bandwidth_to_meter(
    connection_manager: &ConnectionManager,
    snapshot: &[(SocketAddr, u64, u64)],
    prev: &mut HashMap<SocketAddr, (u64, u64)>,
    interval_start: Instant,
) {
    use crate::topology::meter::{AttributionSource, ResourceType};

    // Peers resolved to a ring `PeerKeyLocation` this tick. Used to bound the
    // meter / source_creation_times to the live connection set below.
    let mut live_peers: std::collections::HashSet<PeerKeyLocation> =
        std::collections::HashSet::new();

    for &(addr, cum_sent, cum_recv) in snapshot {
        // Cumulative counters only ever increase, but use saturating
        // subtraction so a counter reset (e.g. the per-peer slot was evicted
        // and re-created since the last tick) can never underflow into a
        // huge bogus delta.
        let (prev_sent, prev_recv) = prev.get(&addr).copied().unwrap_or((0, 0));
        let sent_delta = cum_sent.saturating_sub(prev_sent);
        let recv_delta = cum_recv.saturating_sub(prev_recv);

        // Always record the latest cumulative counts so the next tick diffs
        // against them, even when this tick produced no usable delta.
        prev.insert(addr, (cum_sent, cum_recv));

        let Some(peer) = connection_manager.get_peer_by_addr(addr) else {
            // No ring peer for this address (transient/handshake connection,
            // or just-torn-down). Skip rather than attribute to a phantom.
            continue;
        };
        // Track every resolvable peer (even with a zero delta this tick) as
        // live so retain below doesn't evict a currently-connected, currently
        // idle peer's accumulated samples.
        live_peers.insert(peer.clone());

        if sent_delta == 0 && recv_delta == 0 {
            continue;
        }

        let source = AttributionSource::Peer(peer);
        let mut topo = connection_manager.topology_manager.write();
        if recv_delta > 0 {
            topo.report_resource_usage(
                &source,
                ResourceType::InboundBandwidthBytes,
                recv_delta as f64,
                interval_start,
            );
        }
        if sent_delta > 0 {
            topo.report_resource_usage(
                &source,
                ResourceType::OutboundBandwidthBytes,
                sent_delta as f64,
                interval_start,
            );
        }
    }

    // Bound the meter / source_creation_times to the live peer set so they
    // don't accumulate departed peers forever (#3453 review). Non-Peer
    // (Contract/Delegate) sources are retained by `retain_peer_sources`.
    connection_manager
        .topology_manager
        .write()
        .retain_peer_sources(&live_peers);

    // Drop prior-tick entries for peers that vanished from the snapshot so
    // `prev` stays bounded as peers churn (#3453). After the loop above,
    // every snapshot address is present in `prev`, so `prev.len() >
    // snapshot.len()` holds exactly when some prior peer is no longer in the
    // snapshot — the only case where a prune is needed.
    if prev.len() > snapshot.len() {
        let live: std::collections::HashSet<SocketAddr> =
            snapshot.iter().map(|&(addr, _, _)| addr).collect();
        prev.retain(|addr, _| live.contains(addr));
    }
}

impl Ring {
    // ==================== Subscription Retry Spam Prevention ====================

    /// Check if a subscription request can be made for a contract.
    /// Returns false if request is already pending or in backoff period.
    pub fn can_request_subscription(&self, contract: &ContractKey) -> bool {
        self.hosting_manager.can_request_subscription(contract)
    }

    /// Mark a subscription request as in-flight.
    /// Returns false if already pending.
    pub fn mark_subscription_pending(&self, contract: ContractKey) -> bool {
        self.hosting_manager.mark_subscription_pending(contract)
    }

    /// Mark a subscription request as completed.
    /// If success is false, applies exponential backoff.
    pub fn complete_subscription_request(&self, contract: &ContractKey, success: bool) {
        self.hosting_manager
            .complete_subscription_request(contract, success)
    }

    // ==================== Hosting Cache Management ====================

    /// Touch a contract in the hosting cache (refresh TTL without adding).
    ///
    /// Called when a user GET serves a hosted contract from local cache.
    pub fn touch_hosting(&self, key: &ContractKey) {
        self.hosting_manager.touch_hosting(key)
    }

    /// Mark a contract as accessed by a local client (HTTP/WebSocket).
    pub fn mark_local_client_access(&self, key: &ContractKey) {
        self.hosting_manager.mark_local_client_access(key)
    }

    // NOTE: `has_local_client_access` (the plain, non-recency variant) had its
    // only production caller removed by serve-DURING (#4642 R3 piece C): the
    // originator GET gate now serves on `interest_manager.has_local_interest`
    // (a fresh in-mesh copy) rather than on whether the LOCAL user had touched
    // the contract. The flag is still maintained (`mark_local_client_access`)
    // and read via the recency variant below; the plain read accessor survives
    // only on `HostingManager` for its unit tests.

    /// Whether a local client GET/PUT touched this contract within the renewal age
    /// gate (`SUBSCRIPTION_LEASE_DURATION`) — the read/PUT demand signal the
    /// reconcile input-builder feeds into `contract_in_use` (invariant 3).
    pub fn has_recent_local_client_access(&self, key: &ContractKey) -> bool {
        self.hosting_manager.has_recent_local_client_access(key)
    }

    /// Sweep for expired entries in the hosting cache.
    ///
    /// Returns a [`HostingSweepResult`]: the `(ContractKey, write_generation)`
    /// reclaim pairs plus, for any still-in-use victim shed as a last resort,
    /// the subscription state torn down (so the caller can sync the
    /// `InterestManager`). Under subscriber-primary ordering a contract with
    /// subscribers is evicted LAST (shed only when nothing with fewer
    /// subscribers is eligible). The generation snapshot is carried through
    /// `EvictContract` so the deletion-time guard can detect a re-host race.
    pub fn sweep_expired_hosting(&self) -> crate::ring::hosting::HostingSweepResult {
        self.hosting_manager.sweep_expired_hosting()
    }

    // ==================== Legacy GET Auto-Subscription (delegating to hosting cache) ====================
    /// Sweep for expired entries (delegated to hosting cache).
    ///
    /// Returns a [`HostingSweepResult`] (see [`Self::sweep_expired_hosting`]).
    pub fn sweep_expired_get_subscriptions(&self) -> crate::ring::hosting::HostingSweepResult {
        // Delegate to hosting cache
        self.sweep_expired_hosting()
    }

    // ==================== Connection Pruning ====================

    /// Prune a peer connection.
    ///
    /// Returns orphaned transactions that need to be retried or failed.
    /// In the new lease-based subscription model, subscriptions are not tied to specific
    /// peers, so no subscription pruning is needed when a peer disconnects.
    pub async fn prune_connection(&self, peer: PeerId) -> PruneConnectionResult {
        use crate::tracing::DisconnectReason;

        tracing::debug!(%peer, "Removing connection");
        crate::node::network_status::record_peer_disconnected(peer.socket_addr());
        let orphaned_transactions = self
            .live_tx_tracker
            .prune_transactions_from_peer(peer.socket_addr());

        if !orphaned_transactions.is_empty() {
            tracing::debug!(
                %peer,
                orphaned_count = orphaned_transactions.len(),
                "Connection pruned with orphaned transactions"
            );
        }

        let min_ready = self.connection_manager.min_ready_connections;
        let was_ready = self.connection_manager.is_self_ready();

        // Capture connection duration before pruning
        let connection_duration_ms = self
            .connection_manager
            .get_connection_duration_ms(peer.socket_addr());

        // This case would be when a connection is being open, so peer location hasn't been recorded yet
        let Some(_loc) = self
            .connection_manager
            .prune_alive_connection(peer.socket_addr())
        else {
            return PruneConnectionResult {
                orphaned_transactions,
                became_unready: false,
            };
        };

        if let Some(event) = NetEventLog::disconnected_with_context(
            self,
            &peer,
            DisconnectReason::Pruned,
            connection_duration_ms,
            None, // bytes_sent not tracked yet
            None, // bytes_received not tracked yet
        ) {
            self.event_register
                .register_events(Either::Left(event))
                .await;
        }

        let is_ready = self.connection_manager.is_self_ready();
        let became_unready = min_ready > 0 && was_ready && !is_ready;

        PruneConnectionResult {
            orphaned_transactions,
            became_unready,
        }
    }

    async fn connection_maintenance(
        self: Arc<Self>,
        notifier: EventLoopNotificationsSender,
        live_tx_tracker: LiveTransactionTracker,
    ) -> anyhow::Result<()> {
        let is_gateway = self.is_gateway;
        let shutdown = self.shutdown_token();
        tracing::info!(is_gateway, "Connection maintenance task starting");
        #[cfg(not(test))]
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(60);
        #[cfg(test)]
        const CHECK_TICK_DURATION: Duration = Duration::from_secs(2);

        // Faster tick when below min_connections, so initial mesh formation
        // doesn't bottleneck on the 60-second steady-state interval.
        #[cfg(not(test))]
        const FAST_CHECK_TICK_DURATION: Duration = Duration::from_secs(5);
        #[cfg(test)]
        const FAST_CHECK_TICK_DURATION: Duration = Duration::from_secs(1);

        const REGENERATE_DENSITY_MAP_INTERVAL: Duration = Duration::from_secs(60);

        /// Base number of concurrent connection acquisition attempts (steady-state).
        const BASE_CONCURRENT_CONNECTIONS: usize = 3;

        let mut check_interval = tokio::time::interval(CHECK_TICK_DURATION);
        check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut refresh_density_map = tokio::time::interval(REGENERATE_DENSITY_MAP_INTERVAL);
        refresh_density_map.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // if the peer is just starting wait a bit before
        // we even attempt acquiring more connections
        if sleep_or_shutdown(&shutdown, tokio::time::sleep(Duration::from_secs(2))).await {
            // Shutdown before we ever did any work — park (see #4292 note below).
            std::future::pending::<()>().await;
        }

        let mut pending_conn_adds = BTreeSet::new();
        let mut last_backoff_cleanup = self.time_source.now();
        let mut last_health_check = self.time_source.now();
        let mut last_peer_cache_save = self.time_source.now();
        const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(300);
        // How often to snapshot the peer cache to disk.
        const PEER_CACHE_SAVE_INTERVAL: Duration = Duration::from_secs(30);
        const BACKOFF_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
        /// Duration of zero ring connections before escalating recovery.
        /// Uses a shorter threshold initially (before first successful connection)
        /// so that cold-start failures after OS restart recover faster (#3737).
        const ISOLATION_ESCALATION_THRESHOLD: Duration = Duration::from_secs(120);
        const INITIAL_ISOLATION_ESCALATION_THRESHOLD: Duration = Duration::from_secs(30);
        /// Max time to hold a deferred swap drop before abandoning it.
        const DEFERRED_SWAP_DROP_TTL: Duration = Duration::from_secs(120);

        // --- Nearest-neighbor lattice discovery (route-to-self probe) ---
        //
        // Mechanism 2: to fill each peer's empty successor+predecessor lattice
        // slots, periodically route a self-targeted CONNECT toward own_location
        // (reusing acquire_new + the existing CONNECT path — no wire change).
        // Every self-initiated CONNECT already pre-populates the visited bloom
        // with all currently-connected peers (`start_client_connect`), so the
        // probe routes PAST everything already held and terminates at the nearest
        // UNCONNECTED peer to own_location; successive probes fill outward on BOTH
        // ring sides as each newly-connected peer is excluded from the next probe.
        // The terminus installs the edge via the per-side clause in
        // `should_accept`. Runs CONCURRENTLY with long-link targeting; long links
        // are never gated on lattice completion (they supply the weak connectivity
        // that lets route-to-self converge from a cold start).
        //
        // CONTINUOUS, DECAYING DISCOVERY — the probe NEVER stops. It keeps
        // route-to-self probing even when both sides are filled, so a filled-but-
        // LOOSE edge (holding a farther neighbor while the exact nearest is an
        // unconnected peer) keeps tightening toward this peer's TRUE nearest ring
        // neighbor. But the EFFORT DECAYS: the probability of finding a strictly-
        // closer neighbor drops as the peer converges, so an unproductive probe
        // (found nothing closer) backs the interval off exponentially toward
        // tau_max, while any IMPROVEMENT (a side filled OR an edge tightened) or a
        // LOST edge resets it to the aggressive tau0 and probes promptly. The
        // interval floors at tau_max (a churn-tied maximum), so a fully-converged
        // peer still re-checks periodically — decayed, but never silent. This
        // replaces the earlier fill-only STOP-when-filled probe: relying on the
        // passive per-side acceptance clause alone left ~half the ring's last-mile
        // edges loose at the exact-nearest layer, so the tightening lattice is what
        // pulls each peer onto its true ring neighbors. tau_max is the Chord
        // half-life floor — it must comfortably exceed the churn interval so a
        // dropped edge is re-formed well within a node's lifetime. The steady-state
        // cost is one route-to-self CONNECT per peer roughly every tau_max.
        #[cfg(not(test))]
        const LATTICE_PROBE_TAU0: Duration = Duration::from_secs(5);
        #[cfg(test)]
        const LATTICE_PROBE_TAU0: Duration = Duration::from_secs(1);
        #[cfg(not(test))]
        const LATTICE_PROBE_TAU_MAX: Duration = Duration::from_secs(300);
        #[cfg(test)]
        const LATTICE_PROBE_TAU_MAX: Duration = Duration::from_secs(8);

        /// How often to probe a gateway for version discovery (#3677).
        #[cfg(not(test))]
        const GATEWAY_VERSION_PROBE_INTERVAL: Duration = Duration::from_secs(4 * 3600);
        #[cfg(test)]
        const GATEWAY_VERSION_PROBE_INTERVAL: Duration = Duration::from_secs(10);
        const GATEWAY_PROBE_JITTER_FACTOR: f64 = 0.2;

        // Deferred swap drops: (addr, queued_at) using time_source for
        // deterministic simulation support.
        let mut deferred_swap_drops: Vec<(SocketAddr, tokio::time::Instant)> = Vec::new();

        // Shared exponential-backoff delay calculator for the lattice probe
        // (code-style: use crate::util::backoff, don't hand-roll doubling). The
        // interval is `delay(backoff_attempt) = tau0 * 2^attempt`, capped at
        // tau_max.
        let lattice_probe_backoff = crate::util::backoff::ExponentialBackoff::new(
            LATTICE_PROBE_TAU0,
            LATTICE_PROBE_TAU_MAX,
        );

        // Nearest-neighbor lattice discovery state (mechanism 2). Tracks the next
        // route-to-self probe time, the current backoff attempt (0 = aggressive
        // tau0; grows one step per fire, resets to 0 on an improvement or a lost
        // edge), and the per-side nearest-neighbor DISTANCES observed at the
        // previous tick — used to detect a fill OR a tighten (a distance that
        // strictly decreased) as an improvement. Seeded to fire on the first
        // eligible tick.
        struct LatticeProbeState {
            next_at: Instant,
            backoff_attempt: u32,
            last_sides: Option<LatticeSides>,
        }
        let mut lattice_probe = LatticeProbeState {
            next_at: self.time_source.now(),
            backoff_attempt: 0,
            last_sides: None,
        };
        let mut zero_connections_since: Option<Instant> = None;
        // Track whether we've ever had ring connections. Before the first
        // successful connection, use a shorter isolation escalation threshold
        // for faster recovery from cold-start failures (#3737).
        let mut ever_had_connections = false;

        // Prior-tick cumulative per-peer wire-byte counts, keyed by the
        // transport socket address. Each maintenance tick we diff the current
        // `TRANSPORT_METRICS.per_peer_snapshot()` against this to derive the
        // bytes transferred *during the tick*, then feed that delta into the
        // topology meter so `adjust_topology` can make load-aware decisions
        // (#3453). Bounded: entries for peers absent from the latest snapshot
        // are pruned each tick, so this never outgrows the transport metrics
        // table (capped at `MAX_TRACKED_PEERS`).
        //
        // Seed with the current cumulative counts so the first maintenance
        // tick attributes only the bytes transferred *during* that first
        // interval, not the entire history accumulated before the loop
        // started (which would over-count any pre-maintenance bootstrap
        // traffic into a single inflated first sample).
        let mut prev_peer_bandwidth: HashMap<SocketAddr, (u64, u64)> =
            crate::transport::metrics::TRANSPORT_METRICS
                .per_peer_snapshot()
                .into_iter()
                .map(|(addr, sent, recv)| (addr, (sent, recv)))
                .collect();
        // Time of the previous bandwidth feed (the start of the interval whose
        // byte delta the next feed reports). Seeded with "now" so the first
        // feed's delta is divided by the real first-interval length rather than
        // flooring to the meter's 1s minimum (#3453 review — see
        // `feed_peer_bandwidth_to_meter`).
        let mut prev_bandwidth_tick = self.time_source.now();

        // Gateway version probe: random initial delay to prevent thundering herd.
        // The loop guard (`!is_gateway`) ensures gateways never probe themselves.
        let initial_probe_delay_secs =
            GlobalRng::random_u64() % GATEWAY_VERSION_PROBE_INTERVAL.as_secs();
        let mut next_gateway_probe =
            self.time_source.now() + Duration::from_secs(initial_probe_delay_secs);

        // Adaptive fast-tick backoff: increase the fast-tick interval when
        // connection count stops growing, to avoid hammering the network
        // with CONNECTs indefinitely (#3578).
        let mut last_conn_count: usize = 0;
        let mut no_progress_ticks: u32 = 0;
        // After this many consecutive no-progress ticks, start doubling
        // the fast-tick interval.
        const FAST_TICK_BACKOFF_THRESHOLD: u32 = 6; // 30s at 5s/tick

        // Maximum fast-tick multiplier: caps backoff at the normal tick rate.
        // Derived from tick ratio so invariant holds in both prod and test cfg.
        const MAX_FAST_TICK_MULTIPLIER: u32 =
            (CHECK_TICK_DURATION.as_secs() / FAST_CHECK_TICK_DURATION.as_secs()) as u32;

        // Suspend/resume detection.
        //
        // We compare two clocks that differ only under OS suspend:
        //
        //   * `boot_time::Instant` → CLOCK_BOOTTIME on Linux. Advances while
        //     the machine is suspended.
        //   * `std::time::Instant` → CLOCK_MONOTONIC on Linux. Does *not*
        //     advance while the machine is suspended.
        //
        // The delta between them across a loop iteration is the amount of
        // time the machine was suspended. Using just `boot_elapsed` alone
        // also counts scheduler stalls and virtual-time jumps in simulation
        // tests (`tokio::time::start_paused(true)`), which previously caused
        // false positives: the Apr 2026 nightly logs showed
        // `boot_elapsed_secs=139` on CI runners under load, which tripped the
        // 30s test-only threshold and fired `DropAllConnections` mid-test,
        // wiping in-flight GET ops and tripping the `debug_assert!` in
        // `GetMsg::Request` handling. Comparing against a monotonic baseline
        // eliminates that: a heavy-CPU stall advances *both* clocks equally
        // (delta ≈ 0) and doesn't trip the detector, while a real suspend
        // advances only boot time (delta ≈ suspend duration) and does.
        let mut last_boot_time = boot_time::Instant::now();
        let mut last_mono_time = WallClockInstant::now();
        // 2x the check tick is plenty of headroom to tell a real suspend
        // (minutes) from the sub-tick jitter that a healthy monotonic clock
        // can still exhibit relative to CLOCK_BOOTTIME.
        const SUSPEND_DETECTION_THRESHOLD: Duration = CHECK_TICK_DURATION.saturating_mul(2);

        let mut this_peer = None;
        'maintenance: loop {
            // Stop promptly on node teardown. The OpManager-dropped check below
            // (`OpManagerState::Detached`) is the other exit, but the shutdown
            // token fires from `ShutdownTeardown::drop` *before* the OpManager
            // Arc is dropped, so check it here too so we don't keep running a
            // full maintenance pass during teardown (#4278).
            if shutdown.is_cancelled() {
                tracing::info!(
                    is_gateway,
                    "Shutdown signalled; connection maintenance ending"
                );
                break 'maintenance;
            }

            // Update clock tracking at the top of every iteration (including
            // early-continue paths) so elapsed time doesn't accumulate during
            // startup. The four clock operations below MUST stay back-to-back
            // with no intervening `.await` — any suspension point between
            // them lets the two clocks drift relative to one another for
            // reasons unrelated to suspend/resume, which would poison the
            // delta in `classify_suspend_jump` below. Keep them as a block.
            let boot_elapsed = last_boot_time.elapsed();
            let mono_elapsed = last_mono_time.elapsed();
            last_boot_time = boot_time::Instant::now();
            last_mono_time = WallClockInstant::now();
            let suspend_jump = classify_suspend_jump(boot_elapsed, mono_elapsed);
            // Diagnostic: a small monotonic-ahead skew is normal non-atomic
            // read jitter; a large one would indicate a virtualization TSC
            // anomaly or a monotonic clock going backwards. Surface it so
            // nobody has to rediscover the clock-ordering assumption the
            // hard way.
            if let Some(skew) = mono_elapsed.checked_sub(boot_elapsed)
                && skew > Duration::from_millis(100)
            {
                tracing::warn!(
                    mono_ahead_ms = skew.as_millis() as u64,
                    boot_elapsed_ms = boot_elapsed.as_millis() as u64,
                    mono_elapsed_ms = mono_elapsed.as_millis() as u64,
                    "connection_maintenance: monotonic clock is significantly \
                     ahead of boot clock — possible virtualization TSC anomaly \
                     or monotonic clock regression; suspend detection is \
                     saturated to zero for this iteration"
                );
            }

            let op_manager = match self.op_manager_state() {
                OpManagerState::Live(op_manager) => op_manager,
                OpManagerState::NotAttached => {
                    // Still in the startup window before attach_op_manager;
                    // wait for the owner to wire itself up.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                OpManagerState::Detached => {
                    // The OpManager was attached and then dropped (node
                    // shutdown). Stop the maintenance work instead of spinning
                    // forever on a Weak that can never upgrade again (#3308).
                    // We do NOT `return Ok(())` here: this task is registered
                    // with `BackgroundTaskMonitor`, whose `wait_for_any_exit`
                    // treats any clean return as a fatal "task exited
                    // unexpectedly". Per the #4292 convention, a monitored
                    // long-lived task whose work has ended must hand a
                    // non-completing future to the monitor — so we break out
                    // of the loop and park below.
                    tracing::info!(
                        is_gateway,
                        "OpManager dropped; connection maintenance loop ending (parking)"
                    );
                    break 'maintenance;
                }
            };
            let Some(this_addr) = &this_peer else {
                let Some(addr) = self.connection_manager.get_own_addr() else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                this_peer = Some(addr);
                continue;
            };
            // avoid connecting to the same peer multiple times
            let mut skip_list = HashSet::new();
            skip_list.insert(*this_addr);

            // Resets both connection (location-based) and gateway (address-based)
            // backoff state, and clears stale pending reservations for gateways.
            // Used during isolation recovery to ensure all gateways are retryable
            // when the node has zero ring connections (#3319).
            // Wakes any task sleeping on gateway backoff
            // (`initial_join_procedure`) so it can retry immediately.
            let reset_all_backoff = || {
                self.reset_all_connection_backoff();
                op_manager.gateway_backoff.lock().clear();
                op_manager.gateway_backoff_cleared.notify_waiters();
                // Also clear stale pending reservations for gateways — without this,
                // gateways appear "connected/pending" via has_connection_or_pending()
                // even after backoff is reset, blocking retry attempts (#3319).
                let gateway_addrs: Vec<_> = op_manager
                    .configured_gateways
                    .iter()
                    .filter_map(|gw| gw.socket_addr())
                    .collect();
                self.connection_manager
                    .clear_pending_reservations_for(&gateway_addrs);
            };

            // Suspend/resume detection: if boot time advanced much more than
            // monotonic time, the machine was suspended. Both `suspend_jump`
            // and the underlying clock reads happen at the top of the loop so
            // they include early-continue time.
            if suspend_jump > SUSPEND_DETECTION_THRESHOLD {
                tracing::warn!(
                    boot_elapsed_secs = boot_elapsed.as_secs(),
                    mono_elapsed_secs = mono_elapsed.as_secs(),
                    suspend_jump_secs = suspend_jump.as_secs(),
                    "Detected suspend/resume (boot-time jump) — dropping all connections and clearing state"
                );
                reset_all_backoff();
                // Clear recently-failed addresses since they may be reachable again.
                self.connection_manager.cleanup_all_failed_addrs();
                // Drop all connections (including transient gateway connections).
                // After suspend, transport sockets are dead but connection entries
                // persist as zombies — keepalive tasks exit on socket error but
                // don't trigger connection cleanup. The bootstrap loop then sends
                // CONNECT messages into dead sockets that never reach the gateway.
                notifier
                    .notifications_sender
                    .send(Either::Right(crate::message::NodeEvent::DropAllConnections))
                    .await
                    .map_err(|error| {
                        tracing::debug!(?error, "Failed to send DropAllConnections");
                        error
                    })?;
                zero_connections_since = None;
            }

            // Periodic cleanup of expired backoff entries
            if last_backoff_cleanup.elapsed() > BACKOFF_CLEANUP_INTERVAL {
                self.cleanup_connection_backoff();
                last_backoff_cleanup = self.time_source.now();
            }

            // Clean up stale pending reservations to prevent permanent isolation
            // when CONNECT operations fail to complete cleanly.
            let stale_removed = self.connection_manager.cleanup_stale_reservations();
            if stale_removed > 0 {
                tracing::warn!(
                    stale_removed,
                    "Cleaned up stale reservations and orphaned location entries"
                );
            }

            // Capture a single `now` for all TTL/cleanup checks in this tick so that
            // they all see the same moment rather than drifting across calls. Using
            // `self.time_source` (rather than `Instant::now()` directly) allows tests
            // to supply a `SharedMockTimeSource` without pausing the whole tokio runtime.
            let tick_now = self.time_source.now();

            // Expire old NAT traversal failure entries
            self.connection_manager.cleanup_stale_failed_addrs();

            // Expire acceptor reliability entries for peers whose TTL has elapsed
            self.connection_manager
                .cleanup_expired_acceptor_stats(tick_now);

            // Clean up expired transient connections
            let expired_transients = self.connection_manager.cleanup_expired_transients();
            if expired_transients > 0 {
                tracing::debug!(
                    expired_transients,
                    "Cleaned up expired transient connections"
                );
            }

            // Periodic peer health check: evict peers with sustained routing failures.
            if last_health_check.elapsed() > HEALTH_CHECK_INTERVAL {
                last_health_check = self.time_source.now();
                let current_ring = self.connection_manager.connection_count();
                let unhealthy = self
                    .connection_manager
                    .peer_health
                    .lock()
                    .unhealthy_peers(self.connection_manager.min_connections, current_ring);
                for addr in unhealthy {
                    tracing::warn!(
                        peer = %addr,
                        "Evicting unhealthy peer (sustained routing failures)"
                    );
                    if let Err(e) = notifier
                        .notifications_sender
                        .send(Either::Right(crate::message::NodeEvent::DropConnection(
                            addr,
                        )))
                        .await
                    {
                        tracing::debug!(error = ?e, "Failed to send DropConnection for unhealthy peer");
                    }
                }
            }

            // Periodically save peer cache for fast reconnection after restart.
            if last_peer_cache_save.elapsed() > PEER_CACHE_SAVE_INTERVAL {
                last_peer_cache_save = self.time_source.now();
                if let Some(ref dir) = self.peer_cache_dir {
                    let cache = peer_cache::PeerCache::snapshot_from(
                        &self.connection_manager,
                        self.time_source.as_ref(),
                    );
                    if !cache.peers.is_empty() {
                        if let Err(e) = cache.save(dir) {
                            tracing::warn!(error = %e, "Failed to save peer cache");
                        }
                    }
                }
            }

            // Isolation recovery: when we have zero ring connections for too long,
            // reset all backoff state so we can retry aggressively (#2928).
            let current_conn_count = self.connection_manager.connection_count();
            // Expose to update check task for version mismatch decisions (#3204).
            crate::transport::set_open_connection_count(current_conn_count);
            if current_conn_count == 0 {
                // Use shorter threshold before first successful connection so
                // cold-start failures (e.g., after OS restart) recover in ~30s
                // instead of ~120s. Once the node has connected at least once,
                // use the steady-state threshold. See #3737.
                let threshold = if ever_had_connections {
                    ISOLATION_ESCALATION_THRESHOLD
                } else {
                    INITIAL_ISOLATION_ESCALATION_THRESHOLD
                };
                if let Some(since) = zero_connections_since {
                    if since.elapsed() > threshold {
                        tracing::warn!(
                            is_gateway,
                            isolated_for_secs = since.elapsed().as_secs(),
                            threshold_secs = threshold.as_secs(),
                            ever_connected = ever_had_connections,
                            "Node isolated with zero ring connections — resetting all backoff state"
                        );
                        reset_all_backoff();
                        zero_connections_since = Some(self.time_source.now());
                    }
                } else {
                    zero_connections_since = Some(self.time_source.now());
                    tracing::warn!(
                        is_gateway,
                        "Zero ring connections detected — starting isolation timer"
                    );
                }
            } else if zero_connections_since.take().is_some() {
                ever_had_connections = true;
                tracing::info!(
                    connections = current_conn_count,
                    "Recovered from zero-connection state"
                );
            }

            // Periodic gateway version probe: initiate a CONNECT to a gateway so the
            // transport handshake exchanges version info, even when the ring is full (#3677).
            //
            // The combined predicate (`!is_gateway && !configured_gateways.is_empty() &&
            // now >= next_probe`) is extracted into `should_probe_gateway` so each branch
            // is unit-testable. Without the empty-gateways guard merged into the same
            // condition, the index/modulo on the next line would panic.
            if should_probe_gateway(
                is_gateway,
                !op_manager.configured_gateways.is_empty(),
                self.time_source.now(),
                next_gateway_probe,
            ) {
                let gw_index =
                    GlobalRng::random_u64() as usize % op_manager.configured_gateways.len();
                let gateway = &op_manager.configured_gateways[gw_index];

                if let Err(e) =
                    crate::operations::connect::gateway_version_probe(gateway, &op_manager).await
                {
                    tracing::debug!(
                        error = %e,
                        gateway = %gateway,
                        "Gateway version probe failed, will retry next cycle"
                    );
                }

                let base_secs = GATEWAY_VERSION_PROBE_INTERVAL.as_secs() as f64;
                let jitter_range = base_secs * GATEWAY_PROBE_JITTER_FACTOR;
                let uniform_01 = (GlobalRng::random_u64() as f64) / (u64::MAX as f64);
                let jittered_secs = base_secs + jitter_range * (2.0 * uniform_01 - 1.0);
                next_gateway_probe =
                    self.time_source.now() + Duration::from_secs_f64(jittered_secs.max(1.0));
            }

            // Gateway bootstrap fallback: at zero connections, acquire_new always
            // fails (no routing candidates). Connect to gateways directly (#3219).
            //
            // Note: initial_join_procedure may also be attempting gateway connections
            // concurrently. is_not_connected provides best-effort dedup via pending
            // reservations (should_accept creates one on each join_ring_request), so
            // the second caller typically sees the gateway as "pending" and skips it.
            // Occasional duplicate CONNECTs are harmless — the connect state machine
            // handles them gracefully.
            if current_conn_count == 0 && !op_manager.configured_gateways.is_empty() {
                let eligible: Vec<_> = {
                    let backoff = op_manager.gateway_backoff.lock();
                    self.is_not_connected(op_manager.configured_gateways.iter())
                        .filter(|gw| {
                            gw.socket_addr()
                                .map(|addr| !backoff.is_in_backoff(addr))
                                .unwrap_or(false) // skip gateways without addresses
                        })
                        .cloned()
                        .collect()
                };

                if eligible.is_empty() {
                    tracing::debug!(
                        total_gateways = op_manager.configured_gateways.len(),
                        "Zero connections — all gateways connected/pending or in backoff"
                    );
                } else {
                    let attempt_count = eligible.len().min(BASE_CONCURRENT_CONNECTIONS);
                    tracing::info!(
                        eligible = eligible.len(),
                        attempting = attempt_count,
                        "Zero connections — attempting gateway bootstrap"
                    );
                    for gw in eligible.iter().take(BASE_CONCURRENT_CONNECTIONS) {
                        match crate::operations::connect::join_ring_request(gw, &op_manager, None)
                            .await
                        {
                            Ok(()) => tracing::debug!(gateway = %gw, "Gateway bootstrap initiated"),
                            Err(e) => {
                                tracing::warn!(gateway = %gw, error = %e, "Gateway bootstrap failed")
                            }
                        }
                    }
                }
            }

            // Scale concurrent connection limit based on deficit to min_connections.
            // During bootstrap (far below min_connections), allow more parallel attempts
            // to avoid stalling when slots fill with slow/timing-out transactions.
            let max_concurrent = calculate_max_concurrent_connections(
                current_conn_count,
                self.connection_manager.min_connections,
            );

            // Drain pending connections, initiating multiple attempts per tick
            // (up to max_concurrent) for faster mesh formation. Counts only this
            // node's own in-flight acquisitions, NOT CONNECTs it is relaying for
            // others (#4348).
            let mut active_count = live_tx_tracker.active_acquisition_transaction_count();
            // Under-min nodes bypass per-target backoff to escape the straggler
            // trap (#4348); see `should_respect_location_backoff`.
            let respect_backoff = should_respect_location_backoff(
                current_conn_count,
                self.connection_manager.min_connections,
            );
            while let Some(ideal_location) = pending_conn_adds.pop_first() {
                if respect_backoff && self.is_in_connection_backoff(ideal_location) {
                    tracing::debug!(
                        target_location = %ideal_location,
                        "Skipping connection attempt - target in backoff"
                    );
                    // Intentionally not re-queued: adjust_topology will re-request
                    // this location on the next cycle if still below min_connections.
                    continue;
                }
                if active_count >= max_concurrent {
                    tracing::debug!(
                        active_connections = active_count,
                        max_concurrent,
                        target_location = %ideal_location,
                        "At max concurrent connections, re-queuing location"
                    );
                    pending_conn_adds.insert(ideal_location);
                    break;
                }
                tracing::debug!(
                    active_connections = active_count,
                    max_concurrent,
                    target_location = %ideal_location,
                    "Attempting to acquire new connection"
                );
                let tx = self
                    .acquire_new(
                        ideal_location,
                        &skip_list,
                        &notifier,
                        &live_tx_tracker,
                        &op_manager,
                    )
                    .await
                    .map_err(|error| {
                        tracing::error!(
                            ?error,
                            "FATAL: Connection maintenance task failed - shutting down"
                        );
                        error
                    })?;
                if tx.is_none() {
                    let conns = self.connection_manager.connection_count();
                    tracing::debug!(
                        connections = conns,
                        target_location = %ideal_location,
                        "acquire_new returned None - likely no peers to query through"
                    );
                    // Don't record a backoff against the target location here.
                    // acquire_new returning None means we have insufficient routing
                    // candidates locally — the target location itself is fine.
                    // Backing off the target would block future attempts when we
                    // gain more connections and could actually route to it.
                    // adjust_topology will re-request this location on the next tick.
                } else {
                    active_count += 1;
                    tracing::info!(
                        active_connections = active_count,
                        "Successfully initiated connection acquisition"
                    );
                }
            }

            let current_connections = self.connection_manager.connection_count();
            let pending_connection_targets = pending_conn_adds.len();
            let peers = self.connection_manager.get_connections_by_location();
            let connections_considered: usize = peers.values().map(|c| c.len()).sum();

            let mut neighbor_locations: BTreeMap<_, Vec<_>> = peers
                .iter()
                .map(|(loc, conns)| {
                    let conns: Vec<_> = conns
                        .iter()
                        .filter(|conn| {
                            conn.location
                                .socket_addr()
                                .map(|addr| !live_tx_tracker.has_live_connection(addr))
                                .unwrap_or(true)
                        })
                        .cloned()
                        .collect();
                    (*loc, conns)
                })
                .filter(|(_, conns)| !conns.is_empty())
                .collect();

            if neighbor_locations.is_empty() && connections_considered > 0 {
                tracing::debug!(
                    current_connections,
                    connections_considered,
                    live_tx_peers = live_tx_tracker.len(),
                    "Neighbor filtering removed all candidates; using all connections"
                );

                neighbor_locations = peers
                    .iter()
                    .map(|(loc, conns)| (*loc, conns.clone()))
                    .filter(|(_, conns)| !conns.is_empty())
                    .collect();
            }

            if current_connections > self.connection_manager.max_connections {
                // When over capacity, consider all connections for removal regardless of live_tx filter.
                neighbor_locations = peers.clone();
            }

            tracing::debug!(
                current_connections,
                candidates = peers.len(),
                live_tx_peers = live_tx_tracker.len(),
                "Evaluating topology maintenance"
            );

            // Feed the per-peer bandwidth measured since the previous tick into
            // the topology meter BEFORE adjust_topology reads it. Without this
            // the meter sees no peer-attributed bandwidth samples in production,
            // so `calculate_usage_proportion` always reports ~0% usage and
            // `adjust_topology` perpetually takes the "add connections" branch —
            // the load-aware add/hold/remove logic never engages (#3453).
            let bandwidth_tick_now = self.time_source.now();
            feed_peer_bandwidth_to_meter(
                &self.connection_manager,
                &crate::transport::metrics::TRANSPORT_METRICS.per_peer_snapshot(),
                &mut prev_peer_bandwidth,
                // Timestamp this interval's byte delta at the PREVIOUS tick
                // (interval start) so the meter divides by the real interval
                // length, not the 1s floor. See feed_peer_bandwidth_to_meter.
                prev_bandwidth_tick,
            );
            prev_bandwidth_tick = bandwidth_tick_now;

            let adjustment = self
                .connection_manager
                .topology_manager
                .write()
                .adjust_topology(
                    &neighbor_locations,
                    &self.connection_manager.own_location().location(),
                    self.time_source.now(),
                    current_connections,
                );

            tracing::debug!(
                adjustment = ?adjustment,
                current_connections,
                is_gateway,
                pending_adds = pending_connection_targets,
                "Topology adjustment result"
            );

            match adjustment {
                TopologyAdjustment::AddConnections(target_locs) => {
                    let allowed = calculate_allowed_connection_additions(
                        current_connections,
                        pending_connection_targets,
                        self.connection_manager.min_connections,
                        self.connection_manager.max_connections,
                        target_locs.len(),
                    );

                    if allowed == 0 {
                        tracing::debug!(
                            requested = target_locs.len(),
                            current_connections,
                            pending = pending_connection_targets,
                            min_connections = self.connection_manager.min_connections,
                            max_connections = self.connection_manager.max_connections,
                            "Skipping queuing new connection targets – backlog already satisfies capacity constraints"
                        );
                    } else {
                        let total_pending_after = pending_connection_targets + allowed;
                        tracing::debug!(
                            requested = target_locs.len(),
                            allowed,
                            total_pending_after,
                            "Queuing additional connection targets"
                        );
                        pending_conn_adds.extend(target_locs.into_iter().take(allowed));
                    }
                }
                TopologyAdjustment::RemoveConnections(should_disconnect_peers) => {
                    for peer in should_disconnect_peers {
                        if let Some(addr) = peer.socket_addr() {
                            notifier
                                .notifications_sender
                                .send(Either::Right(crate::message::NodeEvent::DropConnection(
                                    addr,
                                )))
                                .await
                                .map_err(|error| {
                                    tracing::debug!(
                                        error = ?error,
                                        "Shutting down connection maintenance task"
                                    );
                                    error
                                })?;
                        }
                    }
                }
                TopologyAdjustment::SwapConnection {
                    remove,
                    add_location,
                } => {
                    // Connect-first swap: defer the drop until the replacement
                    // connects (connection_count > min_connections), preventing
                    // undershoot that would block future swaps. Deferred drops
                    // expire after DEFERRED_SWAP_DROP_TTL if replacement fails.
                    if let Some(addr) = remove.socket_addr() {
                        tracing::info!(
                            remove_peer = %remove,
                            add_target = %add_location,
                            "Topology swap: queuing replacement connection (drop deferred)"
                        );
                        pending_conn_adds.insert(add_location);
                        // Deduplicate: don't queue the same peer twice if
                        // consecutive swaps select it before the first executes.
                        if !deferred_swap_drops.iter().any(|(a, _)| *a == addr) {
                            deferred_swap_drops.push((addr, tick_now));
                        }
                    } else {
                        tracing::warn!(
                            remove_peer = %remove,
                            "Topology swap skipped: peer has no socket address"
                        );
                    }
                }
                TopologyAdjustment::NoChange => {}
            }

            // Nearest-neighbor lattice discovery (mechanism 2): inject a
            // route-to-self probe target (own_location), gated by exponential
            // backoff, to FILL each peer's empty successor/predecessor lattice
            // slots. Queued into pending_conn_adds so it is acquired next tick
            // alongside long-link targets. No wire change: the probe is a plain
            // CONNECT toward own_location whose bloom already excludes held peers,
            // so it lands on the nearest UNCONNECTED peer and the terminus
            // installs the edge via the per-side clause in should_accept.
            //
            // CONTINUOUS, DECAYING discovery: the probe keeps firing even when both
            // sides are filled, so a filled-but-loose edge tightens toward the TRUE
            // nearest. `lattice_probe_progress` classifies the change since the last
            // tick — an improvement (a side filled OR an edge tightened) or a
            // regression (a side lost) resets the cadence to the aggressive tau0 and
            // probes promptly, while a plateau (nothing closer found) lets the
            // interval grow geometrically toward tau_max. The probe never goes
            // silent; the effort decays as the peer converges (see the module-level
            // discovery comment above).
            if self.connection_manager.nn_lattice_active() {
                if let Some(me) = self.connection_manager.get_stored_location() {
                    let probe_now = self.time_source.now();
                    let succ = self.connection_manager.nearest_lattice_neighbor_dist(true);
                    let pred = self.connection_manager.nearest_lattice_neighbor_dist(false);
                    let curr = LatticeSides { succ, pred };
                    let sides_held = u8::from(succ.is_some()) + u8::from(pred.is_some());

                    // Classify the change since the previous tick: an IMPROVEMENT is
                    // a side newly filled OR an already-held side whose nearest got
                    // strictly closer (a successful tighten); a REGRESSION is a side
                    // that was lost.
                    let progress = lattice_probe_progress(lattice_probe.last_sides, curr);
                    if progress.improved {
                        self.connection_manager.record_lattice_probe_improvement();
                    }

                    // An improvement or a lost edge resets the cadence to the
                    // aggressive tau0 and probes promptly: a lost edge is a routing
                    // dead-end to re-fill NOW, and an improvement means progress is
                    // being made, so keep tightening aggressively (there may be an
                    // even-closer neighbor still to find). A plateau leaves the
                    // growing backoff untouched (see the fire site below).
                    if progress.improved || progress.regressed {
                        lattice_probe.backoff_attempt = 0;
                        lattice_probe.next_at = probe_now;
                    }

                    // CONTINUOUS discovery: no sides-based stop condition — the only
                    // gate is timing. The probe keeps firing (decayed toward tau_max
                    // on a plateau) so a converged peer still re-checks and a
                    // filled-but-loose edge keeps tightening toward the true nearest.
                    if probe_now >= lattice_probe.next_at {
                        self.connection_manager.record_lattice_probe_issued();

                        pending_conn_adds.insert(me);

                        // +/-20% jitter to avoid synchronized probe bursts across
                        // peers that bootstrapped together.
                        let jitter = crate::config::GlobalRng::random_range(0.8..=1.2);
                        let interval = lattice_probe_backoff.delay(lattice_probe.backoff_attempt);
                        lattice_probe.next_at = probe_now + interval.mul_f64(jitter);
                        // Grow the interval one step for the NEXT fire; an
                        // improvement or a regression resets it back to 0 above.
                        lattice_probe.backoff_attempt =
                            lattice_probe.backoff_attempt.saturating_add(1);

                        tracing::debug!(
                            sides_held,
                            succ_dist = ?succ,
                            pred_dist = ?pred,
                            backoff_attempt = lattice_probe.backoff_attempt,
                            interval_secs = interval.as_secs(),
                            "lattice discovery: queued route-to-self probe"
                        );
                    }
                    // Record the observed per-side distances each tick (whether or
                    // not we probed) so the next tick's progress check compares
                    // against the latest reality.
                    lattice_probe.last_sides = Some(curr);
                }
            }

            // Execute deferred swap drops: only drop as many peers as we
            // have headroom above min_connections to avoid undershooting.
            // Expire stale entries whose replacement never connected.
            {
                let before_len = deferred_swap_drops.len();
                deferred_swap_drops.retain(|(_, queued_at)| {
                    tick_now.saturating_duration_since(*queued_at) < DEFERRED_SWAP_DROP_TTL
                });
                let expired = before_len - deferred_swap_drops.len();
                if expired > 0 {
                    tracing::debug!(
                        expired,
                        "Deferred swap drops expired (replacement never connected)"
                    );
                }

                if !deferred_swap_drops.is_empty() {
                    let fresh_count = self.connection_manager.connection_count();
                    let min_conn = self.connection_manager.min_connections;
                    let n_to_drop = deferred_swap_drops_to_execute(
                        fresh_count,
                        min_conn,
                        deferred_swap_drops.len(),
                    );
                    for (addr, _) in deferred_swap_drops.drain(..n_to_drop) {
                        tracing::info!(
                            peer = %addr,
                            connections = fresh_count,
                            "Executing deferred swap drop (replacement connected)"
                        );
                        notifier
                            .notifications_sender
                            .send(Either::Right(crate::message::NodeEvent::DropConnection(
                                addr,
                            )))
                            .await
                            .map_err(|error| {
                                tracing::debug!(
                                    ?error,
                                    "Shutting down connection maintenance task"
                                );
                                error
                            })?;
                    }
                }
            }

            let needs_fast_tick = current_connections < self.connection_manager.min_connections;

            if needs_fast_tick {
                // Adaptive backoff: reset on any connection count change
                // (gain OR loss), otherwise slow down. A loss means topology
                // changed and we should re-enter aggressive mode.
                if current_connections != last_conn_count {
                    no_progress_ticks = 0;
                } else {
                    no_progress_ticks = no_progress_ticks.saturating_add(1);
                }
                last_conn_count = current_connections;

                let multiplier = if no_progress_ticks <= FAST_TICK_BACKOFF_THRESHOLD {
                    1u32
                } else {
                    let excess = no_progress_ticks - FAST_TICK_BACKOFF_THRESHOLD;
                    2u32.saturating_pow(excess).min(MAX_FAST_TICK_MULTIPLIER)
                };
                // Apply ±20% jitter to prevent synchronized CONNECT bursts
                // across peers that bootstrapped simultaneously.
                let jitter: f64 = crate::config::GlobalRng::random_range(0.8..=1.2);
                let adaptive_duration =
                    FAST_CHECK_TICK_DURATION.mul_f64(multiplier as f64 * jitter);

                if multiplier > 1 {
                    tracing::debug!(
                        current_connections,
                        min_connections = self.connection_manager.min_connections,
                        no_progress_ticks,
                        tick_interval_secs = adaptive_duration.as_secs(),
                        "Fast-tick backed off due to no connection progress"
                    );
                }

                // Uses sleep() instead of the check_interval so we don't need a
                // second Interval object. We reset check_interval on transition
                // back to steady-state to avoid an immediate burst tick.
                // The shutdown arm makes the (up to multi-second) wait
                // interruptible (#4278); the loop's top-of-iteration
                // `is_cancelled()` check then breaks out.
                crate::deterministic_select! {
                  _ = refresh_density_map.tick() => {
                    self.refresh_density_request_cache();
                  },
                  _ = tokio::time::sleep(adaptive_duration) => {},
                  _ = shutdown.cancelled() => {},
                }
            } else {
                // Reached min_connections: reset backoff for next time.
                no_progress_ticks = 0;
                last_conn_count = current_connections;

                // Reset the interval on transition from fast to normal tick so
                // accumulated missed ticks don't cause an immediate burst.
                check_interval.reset();
                // Shutdown arm: interrupt the up-to-60s steady-state tick (#4278).
                crate::deterministic_select! {
                  _ = refresh_density_map.tick() => {
                    self.refresh_density_request_cache();
                  },
                  _ = check_interval.tick() => {},
                  _ = shutdown.cancelled() => {},
                }
            }
        }

        // Intentional teardown parking (#4292): the loop only breaks on node
        // shutdown — either the OpManager has been dropped (the `Detached`
        // arm) or the shutdown token fired (#4278) — so there is no more work
        // to do. This task is registered with `BackgroundTaskMonitor`; parking
        // on a never-resolving future — rather than returning `Ok(())` — keeps
        // `wait_for_any_exit` from misreading orderly teardown as a fatal
        // background-task exit. The unreachable `Ok(())` after it keeps the
        // `anyhow::Result<()>` signature.
        std::future::pending::<()>().await;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, notifier, live_tx_tracker, op_manager), fields(peer = %self.connection_manager.pub_key))]
    async fn acquire_new(
        &self,
        ideal_location: Location,
        skip_list: &HashSet<SocketAddr>,
        notifier: &EventLoopNotificationsSender,
        live_tx_tracker: &LiveTransactionTracker,
        op_manager: &Arc<OpManager>,
    ) -> anyhow::Result<Option<Transaction>> {
        let current_connections = self.connection_manager.connection_count();
        let is_gateway = self.is_gateway;

        tracing::debug!(
            current_connections,
            is_gateway,
            target_location = %ideal_location,
            "acquire_new: attempting to find peer to query"
        );

        let query_target = {
            // The router read-lock is only needed for the single
            // `select_k_best_peers_with_telemetry` call; routing_candidates
            // takes its own connection-manager locks. Acquire late to keep
            // the critical section short (clippy: `significant_drop_tightening`).
            let num_connections = self.connection_manager.num_connections();
            tracing::debug!(
                target_location = %ideal_location,
                num_connections,
                skip_list_size = skip_list.len(),
                self_addr = ?self.connection_manager.get_own_addr(),
                "Looking for peer to route through"
            );
            // CONNECT operations bypass readiness gating — peers need to route
            // through ANY ring connection to acquire new connections, even if those
            // connections haven't advertised readiness yet. Without this, peers get
            // stuck below the readiness threshold: they can't initiate CONNECTs
            // because routing() filters out all their "not ready" connections,
            // but they can't become ready without more connections.
            let candidates = self.connection_manager.routing_candidates(
                ideal_location,
                None,
                skip_list,
                false, // bypass readiness — this is a CONNECT for connection acquisition
            );
            let selected = if !candidates.is_empty() {
                let (selected, _) = self.router.read().select_k_best_peers_with_telemetry(
                    candidates.iter(),
                    ideal_location,
                    1,
                );
                selected.into_iter().next().cloned()
            } else {
                None
            };
            if let Some(target) = selected {
                tracing::debug!(
                    query_target = %target,
                    target_location = %ideal_location,
                    "connection_maintenance selected routing target"
                );
                target
            } else {
                tracing::warn!(
                    current_connections,
                    is_gateway,
                    target_location = %ideal_location,
                    "acquire_new: no routing candidates found - cannot find peer to query"
                );
                return Ok(None);
            }
        };

        let joiner = self.connection_manager.own_location();
        tracing::debug!(
            this_peer = %joiner,
            query_target_peer = %query_target,
            target_location = %ideal_location,
            "Sending connect request via connection_maintenance"
        );

        // Driver computes ttl / target_connections / exclude_addrs internally
        // (see start_client_connect at op_ctx_task.rs). acquire_new only
        // needs to allocate the joiner tx, register it with
        // live_tx_tracker, and spawn the driver task.
        let _ = notifier;
        let tx = Transaction::new::<crate::operations::connect::ConnectMsg>();

        let gateway_addr = match query_target.socket_addr() {
            Some(addr) => addr,
            None => {
                tracing::warn!(
                    target_location = %ideal_location,
                    "acquire_new: selected query target has no socket address, skipping spawn"
                );
                return Ok(None);
            }
        };

        // Register tx with the live transaction tracker BEFORE spawning the
        // driver. Otherwise the driver's first Response could land on the
        // bypass before the registration completes. (The acquisition-throttle
        // registration is done inside `start_client_connect`, shared by every
        // self-initiated CONNECT — ring acquisition, gateway join, and probe.)
        live_tx_tracker.add_transaction(gateway_addr, tx);

        let op_manager_spawn = op_manager.clone();
        let gateway = query_target;
        let joiner_for_driver = joiner;
        GlobalExecutor::spawn(async move {
            if let Err(err) = crate::operations::connect::op_ctx_task::start_client_connect(
                tx,
                gateway,
                gateway_addr,
                &op_manager_spawn,
                joiner_for_driver,
                ideal_location,
                None,
            )
            .await
            {
                tracing::debug!(
                    %tx,
                    %err,
                    "acquire_new: CONNECT driver completed with error"
                );
            }
        });

        tracing::debug!(tx = %tx, "Connect request sent");
        Ok(Some(tx))
    }

    /// Register a topology snapshot for this peer with the global registry.
    ///
    /// This should be called periodically during simulation tests to enable
    /// topology validation. The snapshot captures the current subscription
    /// state for all contracts.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)] // Used by SimNetwork tests
    pub fn register_topology_snapshot(&self, network_name: &str) {
        let Some(peer_addr) = self.connection_manager.get_own_addr() else {
            return;
        };
        // Use get_stored_location() for consistency with set_upstream distance check.
        let location = self
            .connection_manager
            .get_stored_location()
            .map(|l| l.as_f64())
            .unwrap_or(0.0);

        let snapshot = self
            .hosting_manager
            .generate_topology_snapshot(peer_addr, location);
        topology_registry::register_topology_snapshot(network_name, snapshot);
    }

    /// Get a topology snapshot for this peer without registering it.
    #[cfg(any(test, feature = "testing"))]
    #[allow(dead_code)] // Used by SimNetwork tests
    pub fn get_topology_snapshot(&self) -> Option<topology_registry::TopologySnapshot> {
        let peer_addr = self.connection_manager.get_own_addr()?;
        // Use get_stored_location() for consistency with set_upstream distance check.
        let location = self
            .connection_manager
            .get_stored_location()
            .map(|l| l.as_f64())
            .unwrap_or(0.0);

        Some(
            self.hosting_manager
                .generate_topology_snapshot(peer_addr, location),
        )
    }
}

/// Calculate the maximum number of concurrent connection acquisition attempts.
///
/// During bootstrap (below `min_connections`), scales up from the base to allow
/// more parallel attempts, preventing stalls when slots fill with slow transactions.
/// Once at or above `min_connections`, returns the base value.
fn calculate_max_concurrent_connections(
    current_connections: usize,
    min_connections: usize,
) -> usize {
    /// Base concurrent connection slots (steady-state).
    const BASE: usize = 3;
    /// How many missing connections map to one additional concurrent slot.
    const CONNECTIONS_PER_EXTRA_SLOT: usize = 3;

    if current_connections >= min_connections {
        return BASE;
    }
    let deficit = min_connections - current_connections;
    // Cap at half of min_connections, floored at BASE.
    let bootstrap_cap = (min_connections / 2).max(BASE);
    (BASE + deficit / CONNECTIONS_PER_EXTRA_SLOT).min(bootstrap_cap)
}

/// Whether `connection_maintenance` should honor per-target-location connection
/// backoff for a node with `current_connections` open connections.
///
/// Backoff is honored only once the node has reached `min_connections`. A node
/// still below min MUST keep probing even recently-rejected ring regions: its
/// under-connection is a local capacity problem, not the target's fault (cf. the
/// ring.md "Backoff Target Must Match Failure Cause" rule), and the 30s→600s
/// location backoff stamped on a `Rejected` would otherwise trap a poorly
/// positioned node permanently below min — the straggler tail in #4348. This
/// mirrors the zero-connection re-bootstrap escape, extended to the whole
/// under-min regime; at/above min, steady-state backoff is unchanged.
///
/// Storm safety: bypassing location backoff below min does NOT let a stuck node
/// hammer indefinitely. The maintenance loop's adaptive fast-tick backoff still
/// stretches the retry cadence toward the steady ~60s `CHECK_TICK` after
/// consecutive no-progress ticks (connection count not changing), per-tick
/// attempts remain bounded by [`calculate_max_concurrent_connections`], and the
/// separate gateway re-bootstrap backoff (`gateway_backoff`) is untouched.
fn should_respect_location_backoff(current_connections: usize, min_connections: usize) -> bool {
    current_connections >= min_connections
}

fn calculate_allowed_connection_additions(
    current_connections: usize,
    pending_connections: usize,
    min_connections: usize,
    max_connections: usize,
    requested: usize,
) -> usize {
    if requested == 0 {
        return 0;
    }

    let effective_connections = current_connections.saturating_add(pending_connections);
    if effective_connections >= max_connections {
        return 0;
    }

    let mut available_capacity = max_connections - effective_connections;

    if current_connections < min_connections {
        let deficit_to_min = min_connections.saturating_sub(effective_connections);
        available_capacity = available_capacity.min(deficit_to_min);
    }

    available_capacity.min(requested)
}

/// Compute how many deferred swap-drops can be executed this tick.
///
/// A deferred drop is safe to execute only when a replacement peer has actually
/// connected.  The guard: `current_connections > min_connections + pending_drops`
/// — meaning we have at least one extra connection above what's needed to cover
/// all pending drops plus the minimum.  Each drop we commit to reduces the
/// effective count by one, so we re-evaluate per element.
///
/// Drops are sent as async events; `connection_count()` won't reflect them until
/// the events are processed.  We track the decrement locally instead.
fn deferred_swap_drops_to_execute(
    current_connections: usize,
    min_connections: usize,
    pending_drops: usize,
) -> usize {
    let mut effective_count = current_connections;
    let mut n_to_drop = 0usize;
    for _ in 0..pending_drops {
        let remaining_pending = pending_drops - n_to_drop;
        if effective_count > min_connections.saturating_add(remaining_pending) {
            n_to_drop += 1;
            effective_count = effective_count.saturating_sub(1);
        } else {
            break;
        }
    }
    n_to_drop
}

/// Amount of wall time the machine was suspended across a maintenance loop
/// iteration, derived from the two clocks the caller samples at the top of
/// each loop pass.
///
/// ## Clock pairing
///
/// On Linux/Android/openBSD/L4Re, `boot_time::Instant` uses `CLOCK_BOOTTIME`
/// (advances during suspend) and `std::time::Instant` uses `CLOCK_MONOTONIC`
/// (does not advance during suspend). The delta across a loop iteration is
/// the amount of time the machine was suspended:
///
///   * Real suspend → boot advances by the suspend duration, monotonic
///     stays flat, delta ≈ suspend duration.
///   * Scheduler stall / heavy CPU work → both advance equally, delta ≈ 0.
///   * Virtual-time jumps under `tokio::time::start_paused(true)` → neither
///     wall clock is touched by tokio's virtual clock; both still advance
///     by whatever real wall time elapsed during the iteration, delta ≈ 0.
///
/// Using just `boot_elapsed` alone against a fixed threshold conflates all
/// three cases, which caused spurious `DropAllConnections` under CI load
/// (the 2026-04-14 nightly logs showed `boot_elapsed_secs=139` tripping the
/// old 30s test-only threshold mid-test).
///
/// ## Non-Linux platforms
///
/// On macOS / FreeBSD / Emscripten the `boot_time` crate resolves to the
/// same clock Rust's `std::time::Instant` uses (`mach_continuous_time` on
/// Darwin; `CLOCK_MONOTONIC` fallback on FreeBSD and Emscripten per the
/// boot_time-0.1.3 source). Both sides of the subtraction therefore advance
/// together and the delta stays near zero — the detector becomes a safe
/// no-op on those platforms rather than a false-positive source. Freenet
/// production gateways and CI runners are Linux, where the detector is
/// load-bearing; the no-op elsewhere is an intentional safe-degradation.
///
/// ## Monotonic-ahead diagnostic
///
/// `saturating_sub` intentionally clamps to zero if `mono_elapsed >
/// boot_elapsed`. Small (O(ns)..O(µs)) negative deltas are normal — the
/// two clocks are read back-to-back, not atomically, so scheduling jitter
/// between the two reads shows up as skew. A *large* negative delta would
/// indicate something pathological (virtualization TSC jump after live
/// migration, or a monotonic clock going backwards); the caller logs a
/// diagnostic warning in that case instead of silently swallowing the
/// signal.
#[inline]
fn classify_suspend_jump(boot_elapsed: Duration, mono_elapsed: Duration) -> Duration {
    boot_elapsed.saturating_sub(mono_elapsed)
}

/// Compute the per-snapshot window delta of a monotonic counter and advance the
/// running previous-total in one step (#4440).
///
/// `current_total` is this snapshot's lifetime count; `prev_total` is the count
/// at the previous snapshot. Returns `current_total - prev_total` (saturating,
/// so a counter reset can never underflow to a huge bogus rate) and stores
/// `current_total` into `*prev_total` for the next call. Extracted from the
/// snapshot loop and unit-tested because a future reorder of the
/// compute-then-advance steps would silently zero the broadcast-failure incident
/// signal with no test failure otherwise.
fn window_delta(current_total: u64, prev_total: &mut u64) -> u64 {
    let delta = current_total.saturating_sub(*prev_total);
    *prev_total = current_total;
    delta
}

/// Process-global health gauges for monitored background tasks (#4440).
///
/// A task that the [`BackgroundTaskMonitor`](crate::node::background_task_monitor)
/// only watches for *death* can still run for hours while every individual run
/// fails — `refresh_router`'s `get_router_events` errors were made non-fatal by
/// the v0.2.74 #4438 hotfix (a transient fd-exhaustion read failure must not
/// kill the node), so a *persistent* failure is now completely silent. This
/// records, per monitored task, the time of the last successful run and the
/// current run of consecutive failures, so the snapshot task can emit
/// `last_success_age` / `consecutive_failures` gauges on the existing
/// `router_snapshot` cadence (partially addresses #4440 item (a)).
///
/// The task *publishes* (`record_success` / `record_failure`) and the `Ring`
/// snapshot task *reads* (`refresh_router`), mirroring `BROADCAST_STREAM_METRICS`
/// (and the module-cache metrics, which were a sibling process-global until
/// #4488 threaded them as a per-node `Arc`). Currently scoped to
/// `refresh_router`, the one monitored task with a non-fatal per-run failure
/// mode; add fields here if another monitored task grows the same pattern.
///
/// Per-node meaning holds only in single-node-per-process production. In a
/// multi-node simulation every node's `refresh_router` shares this
/// process-global, so the snapshot reads the aggregate (last-write-wins
/// last-success across nodes; the consecutive-failure run is shared) — the same
/// caveat that drove #4488 for the module-cache metrics.
static BACKGROUND_TASK_HEALTH: std::sync::LazyLock<BackgroundTaskHealth> =
    std::sync::LazyLock::new(BackgroundTaskHealth::new);

/// Monotonic process clock for the background-task health gauges. `tokio::time`
/// so the age respects `start_paused(true)` virtual time under simulation
/// (a `std::time::Instant` would advance in real wall-clock and break
/// deterministic tests). Sampled at publish time and at snapshot read time; the
/// age is the difference, so the absolute epoch is irrelevant.
static BACKGROUND_TASK_HEALTH_EPOCH: std::sync::LazyLock<Instant> =
    std::sync::LazyLock::new(Instant::now);

/// Health gauges for one monitored background task. See [`BACKGROUND_TASK_HEALTH`].
struct TaskHealth {
    /// Millis-since-[`BACKGROUND_TASK_HEALTH_EPOCH`] of the last successful run,
    /// or [`NO_SUCCESS`](Self::NO_SUCCESS) if the task has never succeeded.
    last_success_millis: AtomicU64,
    /// Consecutive failures since the last success (reset to 0 on success).
    consecutive_failures: AtomicU64,
}

impl TaskHealth {
    /// Sentinel for "no successful run yet". `u64::MAX` millis is ~584 million
    /// years of uptime, so it can never collide with a real elapsed value.
    const NO_SUCCESS: u64 = u64::MAX;

    const fn new() -> Self {
        Self {
            last_success_millis: AtomicU64::new(Self::NO_SUCCESS),
            consecutive_failures: AtomicU64::new(0),
        }
    }
}

/// Per-task background-task health, currently just `refresh_router`. See
/// [`BACKGROUND_TASK_HEALTH`].
struct BackgroundTaskHealth {
    refresh_router: TaskHealth,
}

/// A point-in-time read of one task's health for telemetry emission.
#[derive(Debug, Clone, Copy)]
struct TaskHealthSnapshot {
    /// Seconds since the last successful run, or `None` if it never succeeded.
    last_success_age_secs: Option<u64>,
    /// Consecutive failures since the last success.
    consecutive_failures: u64,
}

impl BackgroundTaskHealth {
    fn new() -> Self {
        Self {
            refresh_router: TaskHealth::new(),
        }
    }

    /// Record a successful run of `refresh_router`: stamp the last-success time
    /// and clear the consecutive-failure run. Cheap `Relaxed` atomics.
    fn record_refresh_router_success(&self) {
        let elapsed_millis = BACKGROUND_TASK_HEALTH_EPOCH.elapsed().as_millis() as u64;
        self.refresh_router
            .last_success_millis
            .store(elapsed_millis, std::sync::atomic::Ordering::Relaxed);
        self.refresh_router
            .consecutive_failures
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Record a failed run of `refresh_router` (transient `get_router_events`
    /// error). Bumps the consecutive-failure run; does not touch last-success.
    fn record_refresh_router_failure(&self) {
        self.refresh_router
            .consecutive_failures
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read `refresh_router`'s health for telemetry.
    fn refresh_router_snapshot(&self) -> TaskHealthSnapshot {
        let last_success_millis = self
            .refresh_router
            .last_success_millis
            .load(std::sync::atomic::Ordering::Relaxed);
        let consecutive_failures = self
            .refresh_router
            .consecutive_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_success_age_secs = if last_success_millis == TaskHealth::NO_SUCCESS {
            None
        } else {
            // saturating: if the clock and the stored stamp disagree (they
            // shouldn't — same monotonic source), never underflow to a huge age.
            let now_millis = BACKGROUND_TASK_HEALTH_EPOCH.elapsed().as_millis() as u64;
            Some(now_millis.saturating_sub(last_success_millis) / 1000)
        };
        TaskHealthSnapshot {
            last_success_age_secs,
            consecutive_failures,
        }
    }
}

/// Best-effort snapshot of this process's open file-descriptor count and the
/// `RLIMIT_NOFILE` soft limit, for the node-health telemetry gauges (#4440).
///
/// fd exhaustion (open fds reaching the soft limit → `EMFILE`) drove the
/// v0.2.73 gateway crash-loop and was invisible to central telemetry at the
/// time. Emitting these on the existing `router_snapshot` cadence makes the
/// headroom observable to operators.
///
/// Returns `(open_fds, fd_soft_limit)`. Either element is `None` on a platform
/// where it can't be read cheaply: the open-fd count is Linux-only (via
/// `/proc/self/fd`); the soft limit is any-unix (via `getrlimit`).
fn read_fd_usage() -> (Option<u64>, Option<u64>) {
    (read_open_fd_count(), read_fd_soft_limit())
}

/// Count this process's open file descriptors by enumerating `/proc/self/fd`.
///
/// Returns `None` if the process is already at its fd limit — `read_dir` itself
/// needs a descriptor and fails with `EMFILE` at the cliff. The companion
/// [`read_fd_soft_limit`] still reports in that case, and the 5-minute cadence
/// captures the *approach* to the limit, which is the actionable signal.
#[cfg(target_os = "linux")]
fn read_open_fd_count() -> Option<u64> {
    // `read_dir` itself holds one descriptor open for the duration of the
    // iteration, so it is included in the entry count; subtract it to report
    // the steady-state number of descriptors the process actually holds.
    let count = std::fs::read_dir("/proc/self/fd").ok()?.count() as u64;
    Some(count.saturating_sub(1))
}

#[cfg(not(target_os = "linux"))]
fn read_open_fd_count() -> Option<u64> {
    None
}

/// Read the current `RLIMIT_NOFILE` soft limit (the ceiling that triggers
/// `EMFILE`). Mirrors the `getrlimit` read in `bin/freenet.rs::raise_fd_limit`.
#[cfg(unix)]
fn read_fd_soft_limit() -> Option<u64> {
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `getrlimit` with a valid resource id and an exclusively-borrowed,
    // initialized out-param is sound; the return code is checked before the
    // struct is read.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limits) } != 0 {
        return None;
    }
    // `rlim_t` is `u64` on linux-gnu (CI target) but varies across unix targets;
    // normalize to `u64`. The cast is redundant on linux-gnu, hence the scoped
    // allow rather than a bare `as u64` that trips clippy.
    #[allow(clippy::unnecessary_cast)]
    Some(limits.rlim_cur as u64)
}

#[cfg(not(unix))]
fn read_fd_soft_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod fd_usage_tests {
    //! Validates the node-health fd gauges (#4440): the open-fd count and the
    //! `RLIMIT_NOFILE` soft limit must report sane values on Linux so the
    //! fd-exhaustion headroom signal that was missing during the v0.2.73
    //! incident is trustworthy.

    #[cfg(target_os = "linux")]
    #[test]
    fn read_fd_usage_reports_sane_values_on_linux() {
        let (open, soft) = super::read_fd_usage();
        let open = open.expect("linux reports an open-fd count via /proc/self/fd");
        let soft = soft.expect("unix reports the RLIMIT_NOFILE soft limit");
        // A running process always holds at least stdin/stdout/stderr.
        assert!(open > 0, "expected a positive open-fd count, got {open}");
        // The kernel enforces open-fds <= soft limit, so this is invariant, not
        // a heuristic — it pins that we read the two values the right way round.
        assert!(
            soft >= open,
            "open fds ({open}) must not exceed the soft limit ({soft})"
        );
    }
}

#[cfg(test)]
mod background_task_health_tests {
    //! Validates the background-task health gauges (#4440): a persistently
    //! failing `refresh_router` (non-fatal since the #4438 hotfix) must surface
    //! as a rising `consecutive_failures` and a stale `last_success_age`, and a
    //! task that has never succeeded must report `None` age (not a bogus zero).
    //! Tests a LOCAL `BackgroundTaskHealth` so they never touch the shared
    //! process-global.

    use super::BackgroundTaskHealth;

    /// Before any success, the age is `None` (the `NO_SUCCESS` sentinel), not a
    /// misleading age of 0. Failures accumulate without touching last-success.
    #[tokio::test(start_paused = true)]
    async fn never_succeeded_reports_none_age_and_accumulates_failures() {
        let h = BackgroundTaskHealth::new();
        let s = h.refresh_router_snapshot();
        assert_eq!(
            s.last_success_age_secs, None,
            "no success yet => None age, never a bogus 0"
        );
        assert_eq!(s.consecutive_failures, 0);

        h.record_refresh_router_failure();
        h.record_refresh_router_failure();
        let s = h.refresh_router_snapshot();
        assert_eq!(
            s.last_success_age_secs, None,
            "still never succeeded after failures"
        );
        assert_eq!(s.consecutive_failures, 2, "failures accumulate");
    }

    /// A success stamps the time (age starts at ~0 and grows with virtual time)
    /// and clears the consecutive-failure run; a later failure run grows again
    /// while the age keeps climbing from the last success.
    #[tokio::test(start_paused = true)]
    async fn success_resets_failures_and_stamps_age() {
        let h = BackgroundTaskHealth::new();
        h.record_refresh_router_failure();
        h.record_refresh_router_failure();

        h.record_refresh_router_success();
        let s = h.refresh_router_snapshot();
        assert_eq!(s.consecutive_failures, 0, "success clears the failure run");
        assert_eq!(
            s.last_success_age_secs,
            Some(0),
            "age is ~0 immediately after success"
        );

        // Advance virtual time; the age must reflect time since last success.
        tokio::time::advance(std::time::Duration::from_secs(90)).await;
        let s = h.refresh_router_snapshot();
        assert_eq!(
            s.last_success_age_secs,
            Some(90),
            "age grows with time since last success"
        );

        // A new failure run grows consecutive_failures but does NOT refresh the
        // last-success stamp — the age keeps climbing, which is the signal that
        // refresh is stuck.
        h.record_refresh_router_failure();
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        let s = h.refresh_router_snapshot();
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(
            s.last_success_age_secs,
            Some(100),
            "age unaffected by failures; still measured from last success"
        );
    }

    /// Source-scrape pin: the startup read in `refresh_router` (the `match`
    /// before the periodic loop) must record into the health gauge on every
    /// arm, with the SAME semantics as the loop — both `Ok` arms record a
    /// success, the `Err` arm records a failure. Without this, a node whose
    /// startup read succeeds but whose later periodic refreshes all fail would
    /// report `last_success_age_secs == null` forever, masking that it did
    /// succeed once.
    ///
    /// Asserting against the process-global `BACKGROUND_TASK_HEALTH` after
    /// running `refresh_router` would be racy (concurrent tests share the
    /// global), so this pins the call sites in source instead — three startup
    /// records plus two in the loop = five total.
    #[test]
    fn refresh_router_records_health_on_startup_and_in_loop() {
        let src = include_str!("ring.rs");
        // Restrict to the `refresh_router` fn body so unrelated mentions (this
        // test, the rustdoc on the metrics struct) don't count. The body runs
        // from its signature to the start of the next method.
        let start = src
            .find("async fn refresh_router")
            .expect("refresh_router fn present");
        let after = &src[start..];
        let end = after
            .find("async fn emit_router_snapshot_telemetry")
            .expect("next method present");
        let body = &after[..end];

        let successes = body.matches(".record_refresh_router_success()").count();
        let failures = body.matches(".record_refresh_router_failure()").count();
        // Startup: 2 Ok arms (success) + 1 Err arm (failure). Loop: 1 Ok
        // (success) + 1 Err (failure). Total 3 success + 2 failure.
        assert_eq!(
            successes, 3,
            "refresh_router must record a success on both startup Ok arms and \
             the loop Ok arm (got {successes}); a dropped startup record would \
             mask a startup-only success in the health gauge"
        );
        assert_eq!(
            failures, 2,
            "refresh_router must record a failure on both the startup Err arm \
             and the loop Err arm (got {failures})"
        );
    }
}

#[cfg(test)]
mod window_delta_tests {
    //! Pins the per-snapshot window-delta of the monotonic broadcast-failure
    //! counter (#4440): it must return `current - prev` AND advance `prev` to
    //! `current`, so a reorder of those two steps (which would silently zero
    //! the incident signal) fails CI.

    use super::window_delta;

    #[test]
    fn broadcast_failure_window_delta_computes_and_advances_prev() {
        let mut prev: u64 = 10;
        // First window: 13 - 10 = 3, and prev advances to 13.
        assert_eq!(window_delta(13, &mut prev), 3, "delta over the window");
        assert_eq!(prev, 13, "prev advanced to current");
        // No new failures: 13 - 13 = 0, prev stays 13.
        assert_eq!(window_delta(13, &mut prev), 0, "no failures => zero delta");
        assert_eq!(prev, 13);
        // Next window: 20 - 13 = 7, prev advances to 20.
        assert_eq!(window_delta(20, &mut prev), 7, "delta resumes from prev");
        assert_eq!(prev, 20, "prev advanced to current again");
        // Counter reset / restart must saturate to 0, never underflow.
        assert_eq!(
            window_delta(5, &mut prev),
            0,
            "reset saturates, no underflow"
        );
        assert_eq!(prev, 5, "prev tracks the reset value");
    }
}

#[cfg(test)]
mod resource_meter_bridge_tests {
    //! Regression tests for #3453: the topology resource meter was never fed
    //! per-peer bandwidth data in production, so `adjust_topology` always saw
    //! ~0% usage and perpetually took the "add connections" branch. These
    //! tests drive `feed_peer_bandwidth_to_meter` — the production bridge from
    //! `TRANSPORT_METRICS.per_peer_snapshot()` into the meter — and assert the
    //! meter actually receives the samples, exercising the load-aware
    //! remove/hold/add decision logic that was previously dead in production.

    // `super::*` brings in ConnectionManager, TopologyAdjustment, Location,
    // SocketAddr, HashMap, BTreeMap, Duration, and tokio's Instant.
    use super::*;
    use crate::topology::meter::{AttributionSource, ResourceType};
    use crate::transport::TransportKeypair;

    /// Mirror of `topology::constants::SOURCE_RAMP_UP_DURATION` (5 min). That
    /// constant is `pub(super)` to the topology module, so we restate it here.
    /// Samples reported older than this window use their measured attributed
    /// rate rather than the P50 ramp-up extrapolation.
    const SOURCE_RAMP_UP_DURATION: Duration = Duration::from_secs(5 * 60);

    /// Add `n` ring connections to `cm`, returning their socket addresses in
    /// the order added. Each gets a distinct loopback address and location.
    fn add_connections(cm: &ConnectionManager, n: usize) -> Vec<SocketAddr> {
        let mut addrs = Vec::with_capacity(n);
        for i in 0..n {
            let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
            // Spread locations across the ring so each peer is a distinct
            // neighbor location (adjust_topology keys neighbors by location).
            let loc = Location::new((i as f64 + 0.5) / n as f64);
            let keypair = TransportKeypair::new();
            assert!(cm.add_connection(loc, addr, keypair.public().clone(), false));
            addrs.push(addr);
        }
        addrs
    }

    /// Drive the bridge once per simulated second, placing the first sample at
    /// `first_sample` and one sample per second thereafter for `ticks` seconds.
    /// `per_sec_recv[i]` is the bytes-received delta attributed to `addrs[i]`
    /// each second; cumulative counters are simulated by accumulating the
    /// per-second deltas (mirrors production, where each maintenance tick feeds
    /// one delta per peer). Returns the final `prev` map for inspection.
    ///
    /// Callers that want the meter to report a *measured* rate (rather than the
    /// ramp-up P50 estimate) must arrange two things, both governed by the
    /// sample timestamps relative to the eventual `adjust_topology` query time
    /// `q`:
    ///   1. The first sample (the source's creation time) must be older than
    ///      `SOURCE_RAMP_UP_DURATION` before `q`, or `extrapolated_usage`
    ///      treats the source as ramping up and substitutes the network P50.
    ///   2. The samples must extend up to just before `q`, because the meter's
    ///      rate is `sum(retained samples) / (q − oldest_retained_sample)`.
    ///      A window that ends far in the past inflates the denominator and
    ///      dilutes the rate toward zero. The meter retains only the most
    ///      recent `RUNNING_AVERAGE_WINDOW` (100) samples.
    fn drive_bridge(
        cm: &ConnectionManager,
        addrs: &[SocketAddr],
        per_sec_recv: &[u64],
        first_sample: Instant,
        ticks: u64,
    ) -> HashMap<SocketAddr, (u64, u64)> {
        let mut prev: HashMap<SocketAddr, (u64, u64)> = HashMap::new();
        let mut cumulative: Vec<u64> = vec![0; addrs.len()];
        for t in 0..ticks {
            for (i, c) in cumulative.iter_mut().enumerate() {
                *c += per_sec_recv[i];
            }
            let snapshot: Vec<(SocketAddr, u64, u64)> = addrs
                .iter()
                .zip(cumulative.iter())
                .map(|(&addr, &recv)| (addr, 0u64, recv))
                .collect();
            let at = first_sample + Duration::from_secs(t);
            feed_peer_bandwidth_to_meter(cm, &snapshot, &mut prev, at);
        }
        prev
    }

    /// The core regression: with the bridge feeding high per-peer bandwidth,
    /// `adjust_topology` reaches the resource-overload "remove" branch. Before
    /// the fix nothing fed the meter, so this path was unreachable in
    /// production and the node would instead always try to ADD connections.
    #[test_log::test]
    fn bridge_feeds_meter_so_overloaded_node_removes() {
        // test_default: min_connections=4, max_connections=10,
        // max_upstream/downstream = 1_000_000 bytes/sec.
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 6);

        // `extrapolated_usage` SUMS the per-source rates across all peers, so
        // 200_000 B/s inbound per peer × 6 peers = 1_200_000 B/s = 1.2× the
        // 1_000_000 downstream limit → usage proportion 1.2 > 0.9 → remove.
        let per_sec_recv = vec![200_000u64; 6];

        // Choose the query time `now`, then lay samples from
        // (now − RAMP_UP − 30s) up to (now − 1s): the first sample is older
        // than the ramp-up window (so the source reports its measured rate),
        // and the retained 100-sample tail ends ~1s before `now` (so the rate
        // isn't diluted by a stale denominator). See `drive_bridge` docs.
        let now = Instant::now();
        let first_sample = now - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        let ticks = SOURCE_RAMP_UP_DURATION.as_secs() + 29; // last sample ≈ now − 1s
        drive_bridge(&cm, &addrs, &per_sec_recv, first_sample, ticks);

        let mut neighbor_locations = BTreeMap::new();
        let conns = cm.get_connections_by_location();
        for (loc, c) in &conns {
            neighbor_locations.insert(*loc, c.clone());
        }

        let adjustment =
            cm.topology_manager
                .write()
                .adjust_topology(&neighbor_locations, &None, now, 6);

        assert!(
            matches!(adjustment, TopologyAdjustment::RemoveConnections(_)),
            "overloaded node fed via the bridge must remove a connection, got {adjustment:?}"
        );
    }

    /// Demonstrates the bug: with NO bridge call (the production state before
    /// this fix), the meter is empty and `adjust_topology` on a node above
    /// min_connections takes the low-usage "add" branch. This is the symptom
    /// #3453 describes; the test above proves the bridge cures it.
    #[test_log::test]
    fn empty_meter_never_removes() {
        let cm = ConnectionManager::test_default();
        let _addrs = add_connections(&cm, 6);

        let mut neighbor_locations = BTreeMap::new();
        let conns = cm.get_connections_by_location();
        for (loc, c) in &conns {
            neighbor_locations.insert(*loc, c.clone());
        }

        let adjustment = cm.topology_manager.write().adjust_topology(
            &neighbor_locations,
            &None,
            Instant::now(),
            6,
        );

        assert!(
            !matches!(adjustment, TopologyAdjustment::RemoveConnections(_)),
            "with an unfed meter the remove branch must be unreachable (the #3453 bug), got {adjustment:?}"
        );
    }

    /// The bridge must attribute samples to the right `ResourceType` and only
    /// for peers that resolve to a ring `PeerKeyLocation`.
    #[test_log::test]
    fn bridge_records_inbound_and_outbound_for_known_peer() {
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 1);
        let peer = cm
            .get_peer_by_addr(addrs[0])
            .expect("peer is a ring connection");

        let at = Instant::now();
        let mut prev = HashMap::new();
        // First snapshot establishes the baseline; cumulative counts start here.
        feed_peer_bandwidth_to_meter(&cm, &[(addrs[0], 1_000, 2_000)], &mut prev, at);

        let source = AttributionSource::Peer(peer);
        // `attributed_usage_rate` takes `&mut self` (it can refresh the
        // meter's cached estimate), so a write guard is required.
        let mut topo = cm.topology_manager.write();
        // First tick: delta == cumulative (prev was 0), so both directions get
        // a sample equal to the cumulative count.
        let outbound = topo.attributed_usage_rate(
            &source,
            &ResourceType::OutboundBandwidthBytes,
            at + Duration::from_secs(1),
        );
        let inbound = topo.attributed_usage_rate(
            &source,
            &ResourceType::InboundBandwidthBytes,
            at + Duration::from_secs(1),
        );
        drop(topo);

        assert_eq!(outbound.expect("outbound recorded").per_second(), 1_000.0);
        assert_eq!(inbound.expect("inbound recorded").per_second(), 2_000.0);
    }

    /// An address with no ring connection (e.g. a transient handshake) must be
    /// skipped, not attributed to a phantom source. `prev` is still updated so
    /// the next tick diffs correctly if the peer later resolves.
    #[test_log::test]
    fn bridge_skips_unresolvable_address() {
        let cm = ConnectionManager::test_default();
        // Six real peers above min_connections so the only thing that could
        // push usage into the remove branch is bandwidth attribution.
        let _addrs = add_connections(&cm, 6);
        // An address that is NOT a ring connection.
        let unknown: SocketAddr = "127.0.0.1:55555".parse().unwrap();

        // Feed enormous traffic for the unknown address across a full window.
        let now = Instant::now();
        let window_end = now - SOURCE_RAMP_UP_DURATION - Duration::from_secs(30);
        let mut prev = HashMap::new();
        let mut cum = 0u64;
        for t in 0..120u64 {
            cum += 10_000_000; // far above any limit, IF it were attributed
            feed_peer_bandwidth_to_meter(
                &cm,
                &[(unknown, 0, cum)],
                &mut prev,
                window_end - Duration::from_secs(120 - t),
            );
        }

        let mut neighbor_locations = BTreeMap::new();
        for (loc, c) in &cm.get_connections_by_location() {
            neighbor_locations.insert(*loc, c.clone());
        }
        let adjustment =
            cm.topology_manager
                .write()
                .adjust_topology(&neighbor_locations, &None, now, 6);

        // The unknown address contributed no meter sample, so usage stays ~0
        // and the node must NOT remove — proving the skip.
        assert!(
            !matches!(adjustment, TopologyAdjustment::RemoveConnections(_)),
            "traffic for an unresolvable address must not be attributed, got {adjustment:?}"
        );
        // But prev is still updated so a future tick computes the delta from here.
        assert_eq!(prev.get(&unknown).copied(), Some((0, cum)));
    }

    /// A counter that goes backwards (per-peer slot evicted then re-created,
    /// resetting cumulative counts) must not underflow into a giant bogus
    /// delta. saturating_sub clamps it to zero.
    #[test_log::test]
    fn bridge_handles_counter_reset_without_underflow() {
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 1);
        let peer = cm.get_peer_by_addr(addrs[0]).unwrap();
        let source = AttributionSource::Peer(peer);

        let t0 = Instant::now();
        let mut prev = HashMap::new();
        // Establish a high cumulative count.
        feed_peer_bandwidth_to_meter(&cm, &[(addrs[0], 0, 1_000_000)], &mut prev, t0);
        // Counter reset: cumulative drops to a small value. Delta must be 0,
        // not u64::MAX - something.
        let t1 = t0 + Duration::from_secs(1);
        feed_peer_bandwidth_to_meter(&cm, &[(addrs[0], 0, 10)], &mut prev, t1);

        let rate = cm
            .topology_manager
            .write()
            .attributed_usage_rate(
                &source,
                &ResourceType::InboundBandwidthBytes,
                t1 + Duration::from_secs(1),
            )
            .unwrap()
            .per_second();
        // Only the first tick's 1_000_000 sample is present; the reset tick
        // contributed a 0 delta (no sample). Sum=1_000_000 over a ~2s window.
        assert!(
            rate > 0.0 && rate.is_finite(),
            "counter reset must clamp to a finite, non-underflowed rate, got {rate}"
        );
    }

    /// `prev` must stay bounded as peers churn: entries for peers absent from
    /// the latest snapshot are pruned so it never outgrows the transport
    /// metrics table.
    #[test_log::test]
    fn bridge_prunes_departed_peers_from_prev() {
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 3);

        let at = Instant::now();
        let mut prev = HashMap::new();
        let full: Vec<_> = addrs.iter().map(|&a| (a, 1u64, 1u64)).collect();
        feed_peer_bandwidth_to_meter(&cm, &full, &mut prev, at);
        assert_eq!(prev.len(), 3);

        // Next tick: only one peer remains in the snapshot. The other two must
        // be pruned from prev.
        feed_peer_bandwidth_to_meter(
            &cm,
            &[(addrs[0], 2, 2)],
            &mut prev,
            at + Duration::from_secs(1),
        );
        assert_eq!(prev.len(), 1, "departed peers must be pruned from prev");
        assert!(prev.contains_key(&addrs[0]));
    }

    /// #3453 review (P1): the per-interval byte delta must be timestamped at
    /// the interval START so the meter divides it by the real interval length,
    /// not the 1-second floor in `RunningAverage::get_rate_at_time`. A single
    /// tick reporting one interval's worth of bytes must yield
    /// `bytes / interval`, NOT `bytes / 1s` (which would overstate the rate by
    /// the interval length and trigger spurious removals).
    #[test_log::test]
    fn bridge_timestamps_delta_at_interval_start_no_inflation() {
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 1);
        let peer = cm.get_peer_by_addr(addrs[0]).unwrap();
        let source = AttributionSource::Peer(peer);

        // 60_000 bytes transferred over a 60s interval = 1000 B/s. Production
        // passes the PREVIOUS tick time as `interval_start`.
        let interval = Duration::from_secs(60);
        let interval_start = Instant::now();
        let query_time = interval_start + interval;

        let mut prev = HashMap::new();
        feed_peer_bandwidth_to_meter(&cm, &[(addrs[0], 0, 60_000)], &mut prev, interval_start);

        let rate = cm
            .topology_manager
            .write()
            .attributed_usage_rate(&source, &ResourceType::InboundBandwidthBytes, query_time)
            .expect("inbound recorded")
            .per_second();

        // Correct: 60_000 / 60s = 1000 B/s. The interval-END bug would give
        // 60_000 / 1s = 60_000 B/s (60× inflation).
        assert!(
            (rate - 1000.0).abs() < 1.0,
            "interval delta must be divided by the real interval, got {rate} B/s (expected ~1000)"
        );
        assert!(
            rate < 2000.0,
            "rate must not be inflated toward the 1s-floor (delta/1s = 60000), got {rate}"
        );
    }

    /// #3453 review (P2): peers that leave the live set must be pruned from the
    /// topology meter (and `source_creation_times`), not accumulate forever.
    /// After a peer departs, a subsequent bridge tick that no longer resolves
    /// it must drop its meter samples.
    #[test_log::test]
    fn bridge_prunes_departed_peer_from_meter() {
        let cm = ConnectionManager::test_default();
        let addrs = add_connections(&cm, 2);
        let peer0 = cm.get_peer_by_addr(addrs[0]).unwrap();
        let peer1 = cm.get_peer_by_addr(addrs[1]).unwrap();
        let src0 = AttributionSource::Peer(peer0);
        let src1 = AttributionSource::Peer(peer1);

        let t0 = Instant::now();
        let mut prev = HashMap::new();
        // Tick 1: both peers transfer bytes — both get meter entries.
        feed_peer_bandwidth_to_meter(
            &cm,
            &[(addrs[0], 0, 10_000), (addrs[1], 0, 10_000)],
            &mut prev,
            t0,
        );
        let q = t0 + Duration::from_secs(60);
        {
            let mut topo = cm.topology_manager.write();
            assert!(
                topo.attributed_usage_rate(&src0, &ResourceType::InboundBandwidthBytes, q)
                    .is_some(),
                "peer0 should have a meter entry after tick 1"
            );
            assert!(
                topo.attributed_usage_rate(&src1, &ResourceType::InboundBandwidthBytes, q)
                    .is_some(),
                "peer1 should have a meter entry after tick 1"
            );
        }

        // peer1 disconnects from the ring; only peer0 remains a resolvable
        // connection. The transport snapshot drops the departed peer too.
        cm.prune_alive_connection(addrs[1]);
        assert!(
            cm.get_peer_by_addr(addrs[1]).is_none(),
            "peer1 must no longer resolve after prune"
        );

        // Tick 2: only peer0 present in the snapshot. peer1 is no longer in the
        // live set, so its meter samples must be pruned.
        feed_peer_bandwidth_to_meter(&cm, &[(addrs[0], 0, 20_000)], &mut prev, t0);
        {
            let mut topo = cm.topology_manager.write();
            assert!(
                topo.attributed_usage_rate(&src0, &ResourceType::InboundBandwidthBytes, q)
                    .is_some(),
                "live peer0 must keep its meter entry"
            );
            assert!(
                topo.attributed_usage_rate(&src1, &ResourceType::InboundBandwidthBytes, q)
                    .is_none(),
                "departed peer1 must be pruned from the meter"
            );
        }
    }
}

#[cfg(test)]
mod k_closest_source_tests {
    //! Source-scrape pin tests for `Ring::k_closest_potentially_hosting`.
    //!
    //! Constructing a real `Ring` for behavioral tests requires significant
    //! scaffolding (NodeConfig, EventLoopNotificationsSender, NetEventRegister,
    //! BackgroundTaskMonitor) that does not currently exist in the test suite.
    //! These tests instead scrape the production source to ensure load-bearing
    //! filter invariants stay wired in — a future refactor that silently drops
    //! the transient filter (issue #4222) or the readiness fallback should fail
    //! CI rather than silently regress production routing behavior.
    //!
    //! Behavioral coverage exists transitively via the simulation_integration
    //! tests; a dedicated integration test for the transient filter is filed
    //! as a follow-up to the Ring test-scaffolding work.
    fn production_source() -> &'static str {
        const FULL: &str = include_str!("ring.rs");
        // ring.rs has inline `#[cfg(test)]` annotations on individual const
        // declarations inside `connection_maintenance` (lines ~1894–1940), so
        // a plain `find("#[cfg(test)]")` would cut off the file mid-impl and
        // miss the function we want to scrape. Anchor on the first *top-level*
        // test module declaration instead.
        let cutoff = FULL
            .find("\n#[cfg(test)]\nmod ")
            .expect("ring.rs must have a top-level #[cfg(test)] mod section");
        &FULL[..cutoff]
    }

    fn extract_fn_body<'a>(source: &'a str, signature_prefix: &str) -> &'a str {
        let start = source
            .find(signature_prefix)
            .unwrap_or_else(|| panic!("could not find {signature_prefix}"));
        let brace = source[start..].find('{').expect("fn sig must have body");
        let body_start = start + brace + 1;
        let bytes = source.as_bytes();
        let mut depth: i32 = 1;
        let mut i = body_start;
        while i < bytes.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &source[body_start..i];
                    }
                }
                _ => {}
            }
            i += 1;
        }
        panic!("unbalanced braces while extracting {signature_prefix}");
    }

    /// Issue #4222: `k_closest_potentially_hosting` must skip transient peers
    /// so GET/SUBSCRIBE doesn't route through about-to-be-dropped connections.
    /// The PUT/UPDATE path already filters them via `routing_candidates`; this
    /// pin makes sure the GET path stays in sync.
    #[test]
    fn k_closest_potentially_hosting_filters_transient_peers() {
        let src = production_source();
        let body = extract_fn_body(src, "pub fn k_closest_potentially_hosting<K>(");
        assert!(
            body.contains("is_transient(addr)"),
            "k_closest_potentially_hosting must call is_transient(addr) on each \
             candidate connection. If the filter was deliberately removed, also \
             update .claude/rules/ring.md (which documents the filter rule) and \
             this test. Issue #4222 / #3570."
        );
        assert!(
            body.contains("skipped_transient"),
            "k_closest_potentially_hosting must surface a skipped_transient \
             counter so operators can see when the filter is removing peers."
        );
    }

    /// The existing readiness fallback must remain — without it, a node whose
    /// peers haven't yet sent ReadyState would fail every GET with EmptyRing.
    /// Pinning it here so a future refactor of the filter chain can't silently
    /// drop the fallback.
    #[test]
    fn k_closest_potentially_hosting_preserves_not_ready_fallback() {
        let src = production_source();
        let body = extract_fn_body(src, "pub fn k_closest_potentially_hosting<K>(");
        assert!(
            body.contains("not_ready_fallback"),
            "k_closest_potentially_hosting must keep the not-yet-ready peer \
             fallback so cold-start nodes don't fail every GET with EmptyRing."
        );
    }

    /// #4440: `is_subscription_root`'s routability mapping must stay in sync with
    /// `k_closest_potentially_hosting`'s eligibility asymmetry, because the
    /// renewal short-circuit only suppresses a wire renewal when no routable
    /// neighbor is closer. The two filters differ:
    ///   * transient peers are excluded UNCONDITIONALLY by k_closest → the root
    ///     check must call `is_transient(addr)` and treat transient as
    ///     non-routable, else a node whose only closer neighbor is transient
    ///     fails to recognise itself as the terminus and storms.
    ///   * not-ready peers are k_closest's *fallback* when no ready candidate
    ///     exists → the root check must NOT exclude them (treat as routable),
    ///     else it wrongly classifies a node as root in cold-start / low-degree
    ///     topologies and suppresses a renewal that would in fact route to the
    ///     closer (not-ready) peer.
    ///
    /// This pin fails the build if the mapping silently drifts from that intent.
    #[test]
    fn is_subscription_root_routability_matches_k_closest_eligibility() {
        let src = production_source();
        let body = extract_fn_body(
            src,
            "fn is_subscription_root(&self, contract_key: &ContractKey) -> bool {",
        );
        assert!(
            body.contains("is_transient(addr)"),
            "is_subscription_root must call is_transient(addr) when deciding whether a \
             closer neighbor is routable — k_closest excludes transient peers \
             unconditionally, so a transient closer neighbor must NOT keep this node \
             from being the terminus (#4440)."
        );
        // The not-ready filter (`is_peer_ready`) must NOT appear in the routability
        // mapping: k_closest falls back to not-ready peers, so they remain valid
        // route targets and the root check must treat them as routable. Anchoring on
        // the absence of `is_peer_ready` guards against a future edit that
        // "symmetrises" the two filters and reintroduces the false-positive-root bug.
        assert!(
            !body.contains("is_peer_ready"),
            "is_subscription_root must NOT exclude not-ready peers (do not call \
             is_peer_ready in the routability mapping): k_closest falls back to \
             not-ready peers, so a not-ready closer neighbor is still a valid route \
             target and must keep this node from short-circuiting its renewal (#4440)."
        );
    }

    /// PR #4734 Fix 1: the periodic hosting sweep must retract the local hosting
    /// advertisement for every contract it evicts, exactly like the GET/PUT
    /// host-formation paths (`cache_contract_locally` / the PUT relay store). An
    /// evicted contract that keeps `LocalInterest.hosting = true` and does not
    /// retract its neighbor advertisement is a stale-interest leak — now widened
    /// because subscribed contracts are sweep-evictable under invariant 3.
    ///
    /// The GET/PUT pins (`cache_contract_locally_syncs_interest_on_subscribed_eviction`)
    /// only cover `remove_evicted_in_use`; this pins the sweep's matching
    /// `unregister_local_hosting` + `broadcast_change_interests` so a future refactor
    /// can't silently drop the retraction from the maintenance path.
    #[test]
    fn sweep_retracts_hosting_interest_on_eviction() {
        let src = production_source();
        let body = extract_fn_body(
            src,
            "async fn sweep_get_subscription_cache(ring: Arc<Self>, interval_duration: Duration) {",
        );
        // Teardown of a still-in-use victim (subscribed contract shed as a last
        // resort) must sync the InterestManager, matching GET/PUT.
        assert!(
            body.contains("remove_evicted_in_use"),
            "sweep must call remove_evicted_in_use to sync the InterestManager for \
             a subscribed contract it sheds (mirrors GET/PUT)."
        );
        // Every evicted contract must clear its local hosting flag …
        assert!(
            body.contains("unregister_local_hosting"),
            "sweep must call unregister_local_hosting for each evicted contract so \
             the evicted contract does not keep LocalInterest.hosting = true \
             (PR #4734 Fix 1 — mirrors GET/PUT host-formation paths)."
        );
        // … and retract the neighbor advertisement for those that lost all interest.
        assert!(
            body.contains("broadcast_change_interests"),
            "sweep must call broadcast_change_interests to retract neighbor \
             advertisements for evicted contracts (PR #4734 Fix 1)."
        );
        // Ordering: the remove_evicted_in_use CALL must run BEFORE the
        // unregister_local_hosting CALL so the latter observes zeroed subscriber
        // counts and reports full interest loss (→ retraction), matching the
        // GET/PUT comment discipline. Anchor on the call expressions (`.method(`),
        // not the bare identifiers, so a prose mention in a leading comment (e.g.
        // "Run BEFORE the `unregister_local_hosting` loop") can't fool the check.
        let teardown_pos = body
            .find("interest_manager.remove_evicted_in_use(")
            .expect("remove_evicted_in_use call present");
        let unregister_pos = body
            .find("interest_manager.unregister_local_hosting(")
            .expect("unregister_local_hosting call present");
        assert!(
            teardown_pos < unregister_pos,
            "remove_evicted_in_use must precede unregister_local_hosting in the sweep \
             so interest loss is reported against the already-zeroed subscriber counts."
        );
    }

    /// Source-scrape pin (keystone sub-task 3 "the flip", #4642): the RENEWAL
    /// site is FLIPPED — it no longer records a shadow, it DRIVES. The renewal
    /// loop must gate its real renewal spawn on the reconcile controller via
    /// `OpManager::reconcile_wants_renewal`, and the record-only shadow helper
    /// (`record_renewal_shadow`) must be gone. A regression that re-introduces the
    /// record-only helper, or drops the drive gate, trips this guard.
    #[test]
    fn renewal_site_is_driven_not_shadowed() {
        let src = production_source();
        // The record-only renewal shadow helper is removed by the flip.
        assert!(
            !src.contains("fn record_renewal_shadow("),
            "record_renewal_shadow must be REMOVED — the renewal site now drives, \
             it does not record a shadow"
        );
        assert!(
            !src.contains("fn renewal_shadow_actual("),
            "renewal_shadow_actual must be REMOVED — the renewal site now drives"
        );
        // The renewal loop drives the controller's interest gate before spawning.
        let loop_body = extract_fn_body(src, "async fn recover_orphaned_subscriptions(");
        assert!(
            loop_body.contains("reconcile_wants_renewal"),
            "the renewal loop must gate its spawn on the reconcile controller \
             (OpManager::reconcile_wants_renewal) — the flip"
        );
        assert!(
            loop_body.contains("mark_subscription_pending"),
            "the renewal loop still drives the real renewal via mark_subscription_pending"
        );
        // The gate must be consulted BEFORE the pending-mark/spawn (interest-gated
        // renewal), not after the work is already scheduled.
        let gate = loop_body
            .find("reconcile_wants_renewal")
            .expect("gate present");
        let spawn = loop_body
            .find("mark_subscription_pending(contract)")
            .expect("spawn present");
        assert!(
            gate < spawn,
            "the reconcile interest gate must run BEFORE mark_subscription_pending \
             so a torn-down contract is never renewed"
        );
    }

    /// Single-source-of-truth pin (#4642 piece F): the periodic renewal loop and
    /// the event-driven connection-drop PROMPT re-root spawn through the SAME
    /// helper (`spawn_renewal_subscribe_task`), so the storm-safety scaffolding
    /// (jitter, recovery guard, outer-cancel deadline) cannot drift between the two
    /// callers — the "manually-mirrored side effect" bug class
    /// (`bug-prevention-patterns.md`). The re-root caller (`spawn_prompt_reroots`)
    /// lives in `op_state_manager.rs` and is pinned from that side by
    /// `connection_drop_re_root_is_driven`; this end pins the renewal loop.
    #[test]
    fn renewal_and_reroot_share_one_spawn_path() {
        let src = production_source();
        // The shared helper exists and owns the actual renewal wire action.
        assert!(
            src.contains("fn spawn_renewal_subscribe_task("),
            "the shared renewal/re-root spawn helper must exist"
        );
        assert!(
            src.contains("run_renewal_subscribe("),
            "the shared spawn helper must drive run_renewal_subscribe"
        );
        // The renewal loop delegates to the shared helper rather than inlining the
        // spawn (which is what let it drift from the re-root path before extraction).
        let loop_body = extract_fn_body(src, "async fn recover_orphaned_subscriptions(");
        assert!(
            loop_body.contains("spawn_renewal_subscribe_task"),
            "the renewal loop must delegate its spawn to spawn_renewal_subscribe_task, \
             not inline it — otherwise the re-root path silently rots against it"
        );
        // ...and does NOT re-inline the run_renewal_subscribe call directly.
        assert!(
            !loop_body.contains("run_renewal_subscribe("),
            "the renewal loop must NOT inline run_renewal_subscribe — it belongs to \
             the shared helper so both callers stay in lockstep"
        );
    }

    /// Regression pin for the per-tick renewal-gate EVALUATION budget (Codex P2,
    /// #4725). The interest gate `OpManager::reconcile_wants_renewal` builds a
    /// fresh `ReconcileInputs` snapshot (a redb `get_state_size` read + an
    /// `is_subscription_root` neighbor scan) on EVERY call. The pre-fix loop
    /// bounded only SPAWNED renewals via `attempted >= batch_limit`, but a
    /// gate-suppressed candidate `continue`s WITHOUT advancing `attempted`, so on
    /// a high-hosting peer (every peer, now that every-hop placement is live) one
    /// ~30s maintenance tick could evaluate the gate for the whole renewal set —
    /// hundreds-to-thousands of snapshots instead of the intended ~`batch_limit`.
    /// This pins that a DISTINCT evaluation budget (`evaluated`) breaks the loop
    /// BEFORE the expensive gate runs, and is advanced on every gate evaluation
    /// (not only on spawn, which is what `attempted` counts). A refactor that
    /// drops the budget or reverts to an `attempted`-only bound fails CI here.
    #[test]
    fn renewal_gate_evaluations_bounded_before_gate() {
        let src = production_source();
        let body = extract_fn_body(src, "async fn recover_orphaned_subscriptions(");

        let budget_break = body.find("if evaluated >= batch_limit").expect(
            "the renewal loop must bound the number of interest-gate evaluations \
             per tick with `if evaluated >= batch_limit { break }` (Codex P2, \
             #4725) — otherwise a gate-suppressed candidate never advances \
             `attempted` and the loop builds an unbounded number of \
             ReconcileInputs snapshots per tick",
        );
        let gate = body
            .find("reconcile_wants_renewal(&contract)")
            .expect("renewal loop must consult the reconcile interest gate");
        assert!(
            budget_break < gate,
            "the evaluation-budget break (offset {budget_break}) must run BEFORE \
             the expensive reconcile_wants_renewal gate (offset {gate}) so the \
             per-snapshot cost is bounded by batch_limit"
        );

        // The budget must advance on every gate evaluation (between the break and
        // the gate), which is what distinguishes it from the `attempted` spawn cap.
        let increment = body.find("evaluated += 1").expect(
            "the evaluation budget `evaluated` must be incremented per gate \
             evaluation, not only when a renewal is spawned",
        );
        assert!(
            budget_break < increment && increment < gate,
            "`evaluated += 1` (offset {increment}) must sit between the budget \
             break (offset {budget_break}) and the gate (offset {gate}) so every \
             gate evaluation is counted toward the per-tick bound"
        );
    }

    /// Behavioral regression for the per-tick evaluation budget (Codex P2,
    /// #4725). Models the renewal loop's exact two-counter structure and asserts
    /// the number of expensive interest-gate evaluations stays bounded by
    /// `batch_limit` regardless of how many candidates the gate suppresses. The
    /// pre-fix `attempted`-only bound (encoded in `simulate_attempted_only`)
    /// evaluated the WHOLE candidate set when the gate suppressed everything —
    /// this is the unbounded-per-heartbeat-read cost the fix removes.
    #[test]
    fn renewal_gate_evaluation_count_is_bounded_when_suppressed() {
        // Faithful model of the loop's per-tick counting: `attempted` is the
        // spawn cap (advanced only on a spawn) and `evaluated` is the evaluation
        // budget (advanced on every gate call). Returns (evaluations, spawns).
        fn simulate(
            candidates: usize,
            batch_limit: usize,
            gate: impl Fn(usize) -> bool,
        ) -> (usize, usize) {
            let mut attempted = 0usize;
            let mut evaluated = 0usize;
            for i in 0..candidates {
                if attempted >= batch_limit {
                    break; // spawn cap
                }
                if evaluated >= batch_limit {
                    break; // evaluation budget (the fix)
                }
                evaluated += 1;
                if !gate(i) {
                    continue; // suppressed: does NOT advance `attempted`
                }
                attempted += 1;
            }
            (evaluated, attempted)
        }

        // The pre-fix model: only the `attempted` spawn cap bounds the loop.
        fn simulate_attempted_only(
            candidates: usize,
            batch_limit: usize,
            gate: impl Fn(usize) -> bool,
        ) -> usize {
            let mut attempted = 0usize;
            let mut evaluated = 0usize;
            for i in 0..candidates {
                if attempted >= batch_limit {
                    break;
                }
                evaluated += 1;
                if !gate(i) {
                    continue;
                }
                attempted += 1;
            }
            evaluated
        }

        let batch_limit = 10usize;
        let many = 5_000usize;

        // Pathological case: the gate suppresses every candidate. Evaluations
        // must still be capped at `batch_limit`, and nothing spawns.
        let (evaluated, attempted) = simulate(many, batch_limit, |_| false);
        assert!(
            evaluated <= batch_limit,
            "gate evaluations ({evaluated}) must be bounded by batch_limit \
             ({batch_limit}) even when every candidate is suppressed"
        );
        assert_eq!(
            attempted, 0,
            "no renewals spawn when every candidate is suppressed"
        );

        // Demonstrates the bug the budget fixes: the old attempted-only bound
        // evaluated the WHOLE set when the gate suppressed everything.
        let unbounded = simulate_attempted_only(many, batch_limit, |_| false);
        assert_eq!(
            unbounded, many,
            "sanity: without the evaluation budget an all-suppressing gate builds \
             a snapshot for every candidate — the unbounded cost the fix removes"
        );

        // Happy path: the gate wants every candidate. The evaluation budget must
        // NOT reduce throughput below the spawn cap.
        let (evaluated, attempted) = simulate(many, batch_limit, |_| true);
        assert_eq!(
            attempted, batch_limit,
            "the spawn cap is still reached in the happy path"
        );
        assert!(
            evaluated <= batch_limit,
            "evaluations never exceed batch_limit"
        );
    }

    /// Hardening pin (keystone step-2 completion, #4642): the
    /// network_status → router_snapshot MIRROR SEAM. The export block hand-copies
    /// each per-site counter into a matching `RouterSnapshotInfo` field; a field
    /// swap (e.g. feeding `reconcile_shadow_collapse_comparisons` from
    /// `shadow.renewal`) would silently emit the wrong per-site value to the
    /// collector with no compile error. This asserts every one of the 24 export
    /// assignments reads from `shadow.<SITE>.<FIELD>` for its OWN site, so a swap
    /// fails CI here. Whitespace-normalized so rustfmt line-wrapping is irrelevant.
    #[test]
    fn reconcile_shadow_export_maps_each_field_to_its_own_site() {
        let src = production_source();
        let block = extract_fn_body(
            src,
            "if let Some(shadow) = crate::node::network_status::reconcile_shadow_counts()",
        );
        // Normalize all runs of whitespace to single spaces.
        let norm = block.split_whitespace().collect::<Vec<_>>().join(" ");

        let full: &[&str] = &[
            "comparisons",
            "divergences",
            "subscribe_diffs",
            "renew_diffs",
            "unsubscribe_diffs",
            "collapse_diffs",
            "announce_diffs",
            "retract_diffs",
            "reroot_search_diffs",
        ];
        let edge: &[&str] = &["comparisons", "divergences"];
        let sites: [(&str, &[&str]); 5] = [
            ("collapse", full),
            ("renewal", full),
            ("inbound_unsubscribe", edge),
            ("connection_drop", edge),
            ("host_formation", edge),
        ];

        let mut checked = 0usize;
        for (site, fields) in sites {
            for field in fields {
                let expected = format!(
                    "snapshot.reconcile_shadow_{site}_{field} = Some(shadow.{site}.{field});"
                );
                assert!(
                    norm.contains(&expected),
                    "mirror-seam: export must contain `{expected}` — a field-swap here \
                     silently emits the wrong per-site value to the collector"
                );
                checked += 1;
            }
        }
        assert_eq!(checked, 24, "expected exactly 24 export assignments");
    }
}

#[cfg(test)]
mod renewal_ban_gate_source_tests {
    //! Source-scrape pin test for the subscription-renewal ban gate (#4373).
    //!
    //! `recover_orphaned_subscriptions` spawns a `run_renewal_subscribe`
    //! driver for every contract in `contracts_needing_renewal()`. That
    //! driver emits an outbound SUBSCRIBE using the same machinery as a
    //! client-initiated request, but — unlike the four `start_client_*`
    //! originator entry points — the renewal scheduler does NOT route
    //! through `operations::reject_if_contract_banned`. So before #4373 a
    //! contract that was banned while the node still held an active
    //! subscription kept emitting outbound SUBSCRIBE renewals on every
    //! maintenance cycle until the ban TTL lifted.
    //!
    //! The fix adds an `is_banned` gate in the renewal loop. As the
    //! `k_closest_source_tests` module above documents, building a real
    //! `Ring` for a behavioral test requires scaffolding that does not yet
    //! exist in this suite, so — mirroring the `*_dispatch_gates_banned_contracts`
    //! pins in `contract_ban_list.rs` — this test scrapes the production
    //! source to ensure the gate stays wired in and keeps running BEFORE
    //! the renewal is spawned. A refactor that drops or reorders the gate
    //! would fail CI rather than silently re-open the egress leak.

    fn production_source() -> &'static str {
        const FULL: &str = include_str!("ring.rs");
        let cutoff = FULL
            .find("\n#[cfg(test)]\nmod ")
            .expect("ring.rs must have a top-level #[cfg(test)] mod section");
        &FULL[..cutoff]
    }

    fn extract_fn_body<'a>(source: &'a str, signature_prefix: &str) -> &'a str {
        let start = source
            .find(signature_prefix)
            .unwrap_or_else(|| panic!("could not find {signature_prefix}"));
        let brace = source[start..].find('{').expect("fn sig must have body");
        let body_start = start + brace + 1;
        let bytes = source.as_bytes();
        let mut depth: i32 = 1;
        let mut i = body_start;
        while i < bytes.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &source[body_start..i];
                    }
                }
                _ => {}
            }
            i += 1;
        }
        panic!("unbalanced braces while extracting {signature_prefix}");
    }

    #[test]
    fn renewal_loop_gates_banned_contracts_before_spawning() {
        let src = production_source();
        let body = extract_fn_body(
            src,
            "async fn recover_orphaned_subscriptions(ring: Arc<Self>",
        );

        let gate_pos = body.find("contract_ban_list.is_banned").expect(
            "the subscription-renewal loop must gate on \
             `contract_ban_list.is_banned` so a banned-but-still-subscribed \
             contract stops emitting outbound SUBSCRIBE renewals (#4373). If \
             this gate was removed, the egress leak is back.",
        );

        // The gate must precede the spam-prevention check: a banned contract
        // is skipped regardless of its `can_request_subscription` backoff
        // state, so order matters.
        let can_request_pos = body
            .find("can_request_subscription(&contract)")
            .expect("renewal loop must still consult can_request_subscription");
        assert!(
            gate_pos < can_request_pos,
            "ban gate (offset {gate_pos}) must run BEFORE the \
             can_request_subscription spam check (offset {can_request_pos}) so \
             a banned contract is always skipped"
        );

        // The gate must precede the renewal spawn. Both `mark_subscription_pending`
        // (which flips per-contract pending state) and `run_renewal_subscribe`
        // (the outbound-egress driver) must only be reached for non-banned
        // contracts.
        let mark_pending_pos = body
            .find("mark_subscription_pending(contract)")
            .expect("renewal loop must still call mark_subscription_pending");
        assert!(
            gate_pos < mark_pending_pos,
            "ban gate (offset {gate_pos}) must run BEFORE \
             mark_subscription_pending (offset {mark_pending_pos})"
        );
        // The outbound-egress driver (`run_renewal_subscribe`) now lives in the
        // shared `spawn_renewal_subscribe_task` helper (#4642 piece F extraction);
        // the loop reaches it via that call, which must only run for non-banned
        // contracts.
        let spawn_pos = body
            .find("spawn_renewal_subscribe_task(")
            .expect("renewal loop must still spawn via spawn_renewal_subscribe_task");
        assert!(
            gate_pos < spawn_pos,
            "ban gate (offset {gate_pos}) must run BEFORE the \
             spawn_renewal_subscribe_task outbound-egress spawn (offset {spawn_pos})"
        );
    }
}

#[cfg(test)]
mod renewal_ban_gate_behavior_tests {
    //! Behavioral coverage for the predicate the renewal-loop ban gate
    //! relies on (#4373): a contract on the `ContractBanList` reports
    //! `is_banned == true` (so the loop's `continue` fires), and an
    //! un-banned contract reports `false` (so renewal proceeds). This
    //! exercises the exact `contract_ban_list.is_banned(contract.id())`
    //! call the loop makes, including the `ContractKey::id()` projection,
    //! against the real ban-list type and a controllable time source.

    use super::contract_ban_list::{BanReason, ContractBanList};
    // `TimeSource` is needed in scope for the `ts.now()` trait method.
    use crate::util::time_source::{SharedMockTimeSource, TimeSource};
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
    use std::sync::Arc;
    use std::time::Duration;

    fn mk_key(byte: u8) -> ContractKey {
        // A full `ContractKey` (not a bare instance id) so the test
        // exercises the same `contract.id()` projection the renewal loop's
        // `is_banned(contract.id())` gate performs.
        ContractKey::from_id_and_code(
            ContractInstanceId::new([byte; 32]),
            CodeHash::new([byte; 32]),
        )
    }

    #[test]
    fn banned_contract_is_skipped_for_renewal_while_unbanned_proceeds() {
        let ts = SharedMockTimeSource::new();
        let ban_list = ContractBanList::new(Arc::new(ts.clone()));

        let banned = mk_key(1);
        let healthy = mk_key(2);

        // Ban one contract; leave the other alone.
        ban_list.ban(
            *banned.id(),
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );

        // The renewal loop's gate is `if is_banned(contract.id()) { continue }`.
        // Banned -> skipped; un-banned -> proceeds to renewal.
        assert!(
            ban_list.is_banned(banned.id()),
            "a banned contract must be skipped for subscription renewal (#4373)"
        );
        assert!(
            !ban_list.is_banned(healthy.id()),
            "a non-banned contract must still be eligible for renewal"
        );

        // Once the ban TTL lifts, the formerly-banned contract becomes
        // eligible for renewal again — the gate is time-bounded, matching
        // the issue's "self-resolves when the ban TTL expires" note.
        ts.advance_time(Duration::from_secs(61));
        assert!(
            !ban_list.is_banned(banned.id()),
            "after the ban TTL expires the contract is eligible for renewal again"
        );
    }
}

#[cfg(test)]
mod suspend_jump_tests {
    use super::classify_suspend_jump;
    use std::time::Duration;

    /// Scheduler stall: both clocks advance equally (the loop task was
    /// starved, but the machine wasn't suspended). The detector must NOT
    /// treat this as a suspend event — that was the root cause of the
    /// 2026-04-14 nightly `test_get_reliability_with_latency` false
    /// positive at `boot_elapsed_secs=139`.
    #[test]
    fn scheduler_stall_is_not_suspend() {
        let boot = Duration::from_secs(139);
        let mono = Duration::from_secs(139);
        assert_eq!(classify_suspend_jump(boot, mono), Duration::ZERO);
    }

    /// Real OS suspend: CLOCK_BOOTTIME advances by the suspend duration,
    /// CLOCK_MONOTONIC stays essentially flat. The detector must surface
    /// the full suspend duration so the caller can compare it against the
    /// threshold and trigger the DropAllConnections recovery path.
    #[test]
    fn real_suspend_is_detected() {
        let boot = Duration::from_secs(3600);
        let mono = Duration::from_millis(50);
        assert_eq!(
            classify_suspend_jump(boot, mono),
            Duration::from_secs(3600) - Duration::from_millis(50)
        );
    }

    /// Healthy tick: both clocks advance by roughly the tick interval and
    /// the delta is near zero.
    #[test]
    fn healthy_tick_is_not_suspend() {
        let boot = Duration::from_millis(2050);
        let mono = Duration::from_millis(2048);
        assert_eq!(classify_suspend_jump(boot, mono), Duration::from_millis(2));
    }

    /// Monotonic exceeding boot (can happen from scheduling jitter between
    /// the two non-atomic clock reads, or from virtualization TSC anomalies)
    /// must saturate to zero rather than underflow. A large skew here is
    /// logged separately by `connection_maintenance` as a diagnostic.
    #[test]
    fn monotonic_ahead_of_boot_saturates_to_zero() {
        let boot = Duration::from_millis(100);
        let mono = Duration::from_millis(101);
        assert_eq!(classify_suspend_jump(boot, mono), Duration::ZERO);
    }

    /// Threshold comparison: the value the live code compares against the
    /// detection threshold must be the delta, not `boot_elapsed` alone.
    /// A 139s scheduler stall at a 30s threshold would trip the old
    /// detector; with the delta it does not.
    #[test]
    fn threshold_comparison_rejects_scheduler_stall() {
        let threshold = Duration::from_secs(30);
        let boot = Duration::from_secs(139);
        let mono = Duration::from_secs(139);
        assert!(classify_suspend_jump(boot, mono) <= threshold);
    }

    /// Threshold comparison: a real suspend of two full check ticks must
    /// exceed the 2x-tick detection threshold.
    #[test]
    fn threshold_comparison_accepts_real_suspend() {
        let threshold = Duration::from_secs(4);
        let boot = Duration::from_secs(300);
        let mono = Duration::from_millis(3);
        assert!(classify_suspend_jump(boot, mono) > threshold);
    }
}

/// Predicate controlling when `connection_maintenance` fires a gateway version probe.
///
/// Extracted as a free function so each branch (gateway role, empty configuration,
/// timing) can be exercised in unit tests without standing up an `OpManager`. The
/// caller relies on `has_configured_gateways` to gate the modulo-indexing into
/// `op_manager.configured_gateways` — flipping that flag at the call site (rather
/// than inside this function) keeps the borrow of the gateway slice local to the
/// caller.
#[inline]
fn should_probe_gateway(
    is_gateway: bool,
    has_configured_gateways: bool,
    now: Instant,
    next_probe: Instant,
) -> bool {
    !is_gateway && has_configured_gateways && now >= next_probe
}

#[cfg(test)]
mod gateway_version_probe_predicate_tests {
    use super::should_probe_gateway;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn fires_on_non_gateway_with_configured_gateways_at_due_time() {
        let now = Instant::now();
        let next_probe = now - Duration::from_secs(1);
        assert!(should_probe_gateway(false, true, now, next_probe));
    }

    #[test]
    fn skipped_when_running_as_gateway() {
        // Gateways must never probe themselves — they are the destination, not
        // the originator. This is the protection that keeps the gateway loop
        // from generating phantom CONNECTs.
        let now = Instant::now();
        let next_probe = now - Duration::from_secs(1);
        assert!(!should_probe_gateway(true, true, now, next_probe));
    }

    #[test]
    fn skipped_when_no_gateways_are_configured() {
        // The empty-gateways case is the boundary that previously sat in an
        // inner `if` and was unreachable from any test. Merging it into the
        // predicate makes the modulo-indexing in the caller structurally safe.
        let now = Instant::now();
        let next_probe = now - Duration::from_secs(1);
        assert!(!should_probe_gateway(false, false, now, next_probe));
    }

    #[test]
    fn skipped_before_due_time() {
        let now = Instant::now();
        let next_probe = now + Duration::from_secs(60);
        assert!(!should_probe_gateway(false, true, now, next_probe));
    }

    #[test]
    fn fires_at_exact_due_time_boundary() {
        // `>=` semantics: a now equal to next_probe should fire, not skip.
        let now = Instant::now();
        assert!(should_probe_gateway(false, true, now, now));
    }

    #[test]
    fn gateway_with_no_configured_gateways_is_still_skipped() {
        let now = Instant::now();
        assert!(!should_probe_gateway(true, false, now, now));
    }
}

#[cfg(test)]
mod max_concurrent_connections_tests {
    use super::calculate_max_concurrent_connections;

    #[test]
    fn at_min_connections_returns_base() {
        assert_eq!(calculate_max_concurrent_connections(25, 25), 3);
    }

    #[test]
    fn above_min_connections_returns_base() {
        assert_eq!(calculate_max_concurrent_connections(30, 25), 3);
    }

    #[test]
    fn large_deficit_scales_up() {
        // deficit=15, 3 + 15/3 = 8, cap = 25/2 = 12 → 8
        assert_eq!(calculate_max_concurrent_connections(10, 25), 8);
    }

    #[test]
    fn full_deficit_capped_at_half_min() {
        // deficit=25, 3 + 25/3 = 11, cap = 25/2 = 12 → 11
        assert_eq!(calculate_max_concurrent_connections(0, 25), 11);
    }

    #[test]
    fn small_deficit_adds_nothing() {
        // deficit=2, 3 + 2/3 = 3, cap = 25/2 = 12 → 3
        assert_eq!(calculate_max_concurrent_connections(23, 25), 3);
    }

    #[test]
    fn very_small_min_connections() {
        // min_conns=1, deficit=1, 3 + 0 = 3, cap = max(0, 3) = 3 → 3
        assert_eq!(calculate_max_concurrent_connections(0, 1), 3);
        // min_conns=2, deficit=2, 3 + 0 = 3, cap = max(1, 3) = 3 → 3
        assert_eq!(calculate_max_concurrent_connections(0, 2), 3);
    }

    #[test]
    fn high_min_connections_scales_cap() {
        // deficit=50, 3 + 50/3 = 19, cap = 50/2 = 25 → 19
        assert_eq!(calculate_max_concurrent_connections(0, 50), 19);
    }
}

#[cfg(test)]
mod respect_location_backoff_tests {
    use super::should_respect_location_backoff;

    /// Regression guard for the under-min backoff escape (#4348): a node below
    /// min_connections must bypass per-target location backoff so it cannot be
    /// trapped below min by a stamped `Rejected` backoff; at/above min, backoff
    /// is respected as before. Pins the inequality direction and boundary.
    #[test]
    fn below_min_bypasses_backoff() {
        assert!(!should_respect_location_backoff(0, 10));
        assert!(!should_respect_location_backoff(7, 10));
        assert!(!should_respect_location_backoff(9, 10));
    }

    #[test]
    fn at_or_above_min_respects_backoff() {
        assert!(should_respect_location_backoff(10, 10)); // exactly min
        assert!(should_respect_location_backoff(11, 10));
        assert!(should_respect_location_backoff(20, 10));
    }
}

#[cfg(test)]
mod pending_additions_tests {
    use super::calculate_allowed_connection_additions;

    #[test]
    fn respects_minimum_when_backlog_exists() {
        let allowed = calculate_allowed_connection_additions(1, 24, 25, 200, 24);
        assert_eq!(allowed, 0, "Backlog should satisfy minimum deficit");
    }

    #[test]
    fn permits_requests_until_minimum_is_met() {
        let allowed = calculate_allowed_connection_additions(1, 0, 25, 200, 24);
        assert_eq!(allowed, 24);
    }

    #[test]
    fn caps_additions_at_available_capacity() {
        let allowed = calculate_allowed_connection_additions(190, 5, 25, 200, 10);
        assert_eq!(allowed, 5);
    }

    #[test]
    fn respects_requested_when_capacity_allows() {
        let allowed = calculate_allowed_connection_additions(50, 0, 25, 200, 3);
        assert_eq!(allowed, 3);
    }
}

#[cfg(test)]
mod op_manager_state_tests {
    use super::{OpManagerState, classify_op_manager_ref};
    use parking_lot::RwLock;
    use std::sync::{Arc, Weak};

    // Stand-in for `OpManager`: `classify_op_manager_ref` is generic over the
    // pointee, so we exercise the exact production logic without building a
    // full node. The three slot states map 1:1 onto the maintenance loop's
    // startup / running / shutdown decisions (#3308).

    #[test]
    fn empty_slot_is_not_attached() {
        // Mirrors the startup window before `attach_op_manager` runs.
        let slot: RwLock<Option<Weak<u32>>> = RwLock::new(None);
        assert!(matches!(
            classify_op_manager_ref(&slot),
            OpManagerState::NotAttached
        ));
    }

    #[test]
    fn live_weak_upgrades_to_live() {
        let owner = Arc::new(7u32);
        let slot: RwLock<Option<Weak<u32>>> = RwLock::new(Some(Arc::downgrade(&owner)));
        match classify_op_manager_ref(&slot) {
            OpManagerState::Live(got) => assert_eq!(*got, 7),
            OpManagerState::NotAttached | OpManagerState::Detached => {
                panic!("expected Live while the owner Arc is alive")
            }
        }
    }

    #[test]
    fn dropped_owner_is_detached_not_not_attached() {
        // The #3308 regression: the slot was attached (`Some(weak)`) but the
        // owning Arc has been dropped. This MUST classify as `Detached`
        // (terminate the loop), NOT `NotAttached` (which spins forever).
        let owner = Arc::new(7u32);
        let weak = Arc::downgrade(&owner);
        let slot: RwLock<Option<Weak<u32>>> = RwLock::new(Some(weak));
        drop(owner);
        assert!(
            matches!(classify_op_manager_ref(&slot), OpManagerState::Detached),
            "a dropped owner must be Detached so connection_maintenance exits \
             instead of zombie-spinning (#3308)"
        );
    }

    /// End-to-end wiring guard for the maintenance loop's `Detached` arm.
    ///
    /// The pure classifier tests above only pin the `slot -> state` mapping.
    /// They would all stay green if a refactor swapped the loop's `Detached`
    /// arm back to `continue` (the original #3308 bug) or to a bare
    /// `return Ok(())` (the #4292 monitor-convention violation). This test
    /// pins the *action*: a maintenance-shaped task that polls
    /// `classify_op_manager_ref`, breaks on `Detached`, and then parks must,
    /// once its owner `Arc` is dropped, hand a *non-completing* future to a
    /// real `BackgroundTaskMonitor` — `wait_for_any_exit` must NOT fire.
    ///
    /// Mirrors `reference_ping::spawn_with_unbindable_target_does_not_exit`.
    #[tokio::test(start_paused = true)]
    async fn detached_maintenance_task_parks_without_tripping_monitor() {
        use crate::node::background_task_monitor::BackgroundTaskMonitor;
        use std::time::Duration;

        // Shared back-reference slot, exactly like `Ring::op_manager`.
        let owner = Arc::new(0u32);
        let slot: Arc<RwLock<Option<Weak<u32>>>> =
            Arc::new(RwLock::new(Some(Arc::downgrade(&owner))));

        let task_slot = Arc::clone(&slot);
        let handle = tokio::spawn(async move {
            // Reproduces the maintenance loop's arm-to-action wiring without
            // standing up a full Ring: poll the back-reference, act per state.
            loop {
                match classify_op_manager_ref(&task_slot) {
                    // Owner still alive: keep running (the Live work is a
                    // no-op here; the point is that it does NOT exit).
                    OpManagerState::Live(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    // Startup window: keep waiting.
                    OpManagerState::NotAttached => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    // Owner dropped: stop the loop, then park (#3308 / #4292).
                    OpManagerState::Detached => break,
                }
            }
            // Teardown parking: must NOT return cleanly, or the monitor fires.
            std::future::pending::<()>().await;
        });

        let monitor = BackgroundTaskMonitor::new();
        monitor.register("connection_maintenance_sim", handle);

        // Let the task observe `Live` at least once and start polling.
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;

        // Drop the owner: the weak now fails to upgrade -> `Detached`.
        drop(owner);

        // Give the task time to observe `Detached`, break, and park.
        tokio::time::advance(Duration::from_millis(200)).await;
        tokio::task::yield_now().await;

        // The monitor must NOT fire: orderly teardown parks instead of
        // returning. A `continue` (zombie spin) or a `return Ok(())`
        // (monitor-convention violation) would both fail this assertion —
        // the latter by resolving `wait_for_any_exit`.
        let exit = monitor.wait_for_any_exit();
        tokio::pin!(exit);
        let still_parked = tokio::time::timeout(Duration::from_millis(100), &mut exit)
            .await
            .is_err();
        assert!(
            still_parked,
            "Detached maintenance task must park, not exit: a clean return \
             would trip BackgroundTaskMonitor::wait_for_any_exit and crash \
             the node (#3308 / #4292)"
        );
    }
}

#[cfg(test)]
mod refresh_router_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use either::Either;
    use futures::FutureExt;
    use parking_lot::RwLock;
    use tokio_util::sync::CancellationToken;

    use crate::ring::PeerKeyLocation;
    use crate::ring::location::Location;
    use crate::router::{RouteEvent, RouteOutcome, Router};
    use crate::tracing::{NetEventLog, NetEventRegister};

    /// Mock register that returns pre-populated RouteEvents on startup.
    #[derive(Clone)]
    struct WarmStartRegister {
        events: Vec<RouteEvent>,
    }

    impl NetEventRegister for WarmStartRegister {
        fn register_events<'a>(
            &'a self,
            _events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> futures::future::BoxFuture<'a, ()> {
            async {}.boxed()
        }

        fn notify_of_time_out(
            &mut self,
            _tx: crate::message::Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> futures::future::BoxFuture<'_, ()> {
            async {}.boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn get_router_events(
            &self,
            number: usize,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            let events = self.events.iter().take(number).cloned().collect();
            async move { Ok(events) }.boxed()
        }
    }

    fn make_route_events(count: usize) -> Vec<RouteEvent> {
        (0..count)
            .map(|_| RouteEvent {
                peer: PeerKeyLocation::random(),
                contract_location: Location::random(),
                outcome: RouteOutcome::Success {
                    time_to_response_start: Duration::from_millis(50),
                    payload_size: 1000,
                    payload_transfer_time: Duration::from_millis(100),
                },
                op_type: None,
            })
            .collect()
    }

    #[tokio::test]
    async fn refresh_router_loads_history_on_startup() {
        let events = make_route_events(100);
        let register = WarmStartRegister {
            events: events.clone(),
        };
        let router = Arc::new(RwLock::new(Router::new(&[])));

        // Verify the router starts empty
        let snapshot = router.read().snapshot();
        assert_eq!(snapshot.failure_events, 0);
        assert_eq!(snapshot.success_events, 0);

        // Run refresh_router with a timeout so it doesn't loop forever.
        // The startup load happens before the interval loop, so it completes
        // within the first few milliseconds.
        tokio::time::timeout(
            Duration::from_millis(100),
            super::Ring::refresh_router(router.clone(), register, CancellationToken::new()),
        )
        .await
        .ok(); // timeout is expected — the function loops forever

        // Verify the router was populated from the historical events
        let snapshot = router.read().snapshot();
        assert_eq!(
            snapshot.success_events, 100,
            "Router should have been populated with startup history"
        );
    }

    #[tokio::test]
    async fn refresh_router_handles_empty_history() {
        let register = WarmStartRegister { events: vec![] };
        let router = Arc::new(RwLock::new(Router::new(&[])));

        tokio::time::timeout(
            Duration::from_millis(100),
            super::Ring::refresh_router(router.clone(), register, CancellationToken::new()),
        )
        .await
        .ok();

        // Router should remain empty
        let snapshot = router.read().snapshot();
        assert_eq!(snapshot.failure_events, 0);
        assert_eq!(snapshot.success_events, 0);
    }

    /// Mock register whose `get_router_events` fails the first `fail_first`
    /// times it is called, then returns the configured events on every
    /// subsequent call. Records the total call count so tests can confirm the
    /// refresh loop keeps polling past a transient error rather than dying.
    #[derive(Clone)]
    struct FlakyRegister {
        events: Vec<RouteEvent>,
        fail_first: usize,
        calls: Arc<AtomicUsize>,
    }

    impl NetEventRegister for FlakyRegister {
        fn register_events<'a>(
            &'a self,
            _events: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> futures::future::BoxFuture<'a, ()> {
            async {}.boxed()
        }

        fn notify_of_time_out(
            &mut self,
            _tx: crate::message::Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> futures::future::BoxFuture<'_, ()> {
            async {}.boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn get_router_events(
            &self,
            number: usize,
        ) -> futures::future::BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail = call < self.fail_first;
            let events: Vec<RouteEvent> = self.events.iter().take(number).cloned().collect();
            async move {
                if fail {
                    // Mimics the production failure: an AOF segment read failing
                    // under fd exhaustion ("No file descriptors available").
                    Err(anyhow::anyhow!(
                        "No file descriptors available (os error 24)"
                    ))
                } else {
                    Ok(events)
                }
            }
            .boxed()
        }
    }

    /// Regression test: a transient `get_router_events` error inside the
    /// periodic refresh loop must NOT terminate the task (which, as a monitored
    /// background task, would be node-fatal and crash-loop the gateway).
    ///
    /// The mock fails the startup load plus the first few periodic ticks, then
    /// succeeds. We assert the loop kept polling past the errors (call count >
    /// the number of injected failures) AND eventually recovered by loading the
    /// history — both impossible if the error arm had `return`ed.
    #[tokio::test(start_paused = true)]
    async fn refresh_router_survives_transient_get_router_events_error() {
        let events = make_route_events(100);
        let calls = Arc::new(AtomicUsize::new(0));
        // Fail the startup load (call 0) and the first 3 periodic ticks
        // (calls 1..=3), then succeed from call 4 onward.
        let register = FlakyRegister {
            events: events.clone(),
            fail_first: 4,
            calls: calls.clone(),
        };
        let router = Arc::new(RwLock::new(Router::new(&[])));

        // The periodic loop ticks every 5 minutes; under start_paused the
        // timeout auto-advances virtual time, so ~40 min covers >5 ticks and
        // lets the loop poll well past the injected failures and recover.
        tokio::time::timeout(
            Duration::from_secs(60 * 40),
            super::Ring::refresh_router(router.clone(), register, CancellationToken::new()),
        )
        .await
        .ok(); // timeout is expected — the loop runs forever when healthy

        // The loop must have kept polling past every injected failure rather
        // than returning on the first Err.
        let total_calls = calls.load(Ordering::SeqCst);
        assert!(
            total_calls > 4,
            "refresh loop should keep polling past transient errors, but only \
             called get_router_events {total_calls} times (<= the 4 injected failures), \
             implying it returned/died instead of retrying"
        );

        // And it must have recovered: once get_router_events started succeeding,
        // the router got populated from the historical events.
        let snapshot = router.read().snapshot();
        assert_eq!(
            snapshot.success_events, 100,
            "router should have recovered and loaded history after the transient \
             errors cleared"
        );
    }

    /// Regression for #4278: the periodic `refresh_router` loop ticks every 5
    /// minutes. With the shutdown token cancelled, the task must return
    /// *promptly* (well within one tick) rather than parking on the 5-minute
    /// `interval.tick()`. Before the fix there was no token to race against, so
    /// the only way out of the loop was the 5-minute tick — a shutdown would
    /// hang for up to that long.
    ///
    /// Uses `start_paused`: virtual time only advances when every task is idle,
    /// so if the loop were genuinely blocked on the 5-minute tick this
    /// `timeout` would *fire* (the test would fail on the `expect`). The fix
    /// makes the future resolve at t≈0 because the token is already cancelled.
    #[tokio::test(start_paused = true)]
    async fn refresh_router_returns_promptly_on_shutdown() {
        let register = WarmStartRegister { events: vec![] };
        let router = Arc::new(RwLock::new(Router::new(&[])));

        let shutdown = CancellationToken::new();
        // Cancel before we even start: the very first interruptible `.await`
        // (the 5-minute periodic tick) must observe the cancellation and bail.
        shutdown.cancel();

        // 1s of *virtual* time is generous; a working loop returns at t≈0,
        // a broken (uninterruptible) one would block until the 5-minute tick
        // and trip this timeout.
        tokio::time::timeout(
            Duration::from_secs(1),
            super::Ring::refresh_router(router.clone(), register, shutdown),
        )
        .await
        .expect("refresh_router must return promptly once the shutdown token is cancelled");
    }
}

#[cfg(test)]
mod deferred_swap_drop_tests {
    use super::deferred_swap_drops_to_execute;

    /// 3-node ring: current=2, min=1, pending=1.
    /// The original bug: headroom = 2-1 = 1 > 0, so the old code fired the drop,
    /// kicking the only other peer and leaving the node isolated.
    /// Fixed: guard requires current > min + pending (2 > 1+1 = 2) → false.
    #[test]
    fn three_node_ring_no_drop_without_replacement() {
        assert_eq!(deferred_swap_drops_to_execute(2, 1, 1), 0);
    }

    /// Replacement connected in a 3-node ring: current=3, min=1, pending=1.
    /// 3 > 1+1=2 → true. One drop allowed.
    #[test]
    fn three_node_ring_drop_when_replacement_connected() {
        assert_eq!(deferred_swap_drops_to_execute(3, 1, 1), 1);
    }

    /// At min_connections with no pending drops: current=10, min=10, pending=0.
    /// No pending drops means nothing to execute.
    #[test]
    fn no_pending_drops_returns_zero() {
        assert_eq!(deferred_swap_drops_to_execute(10, 10, 0), 0);
    }

    /// Large network, no replacements yet: current=12, min=10, pending=3.
    /// 12 > 10+3=13 → false. None dropped.
    #[test]
    fn large_network_no_drop_without_replacement() {
        assert_eq!(deferred_swap_drops_to_execute(12, 10, 3), 0);
    }

    /// Large network, exactly enough for one replacement: current=13, min=10, pending=3.
    /// i=0: remaining=3, 13 > 13 → false. Still none.
    #[test]
    fn large_network_boundary_no_drop() {
        assert_eq!(deferred_swap_drops_to_execute(13, 10, 3), 0);
    }

    /// Large network, one replacement connected: current=14, min=10, pending=3.
    /// i=0: remaining=3, 14 > 13 → true (effective=13, n=1)
    /// i=1: remaining=2, 13 > 12 → true (effective=12, n=2)
    /// i=2: remaining=1, 12 > 11 → true (effective=11, n=3)
    /// All 3 dropped; after drops effective=11 ≥ min=10.
    #[test]
    fn large_network_all_replacements_connected() {
        assert_eq!(deferred_swap_drops_to_execute(14, 10, 3), 3);
    }

    /// current exactly at min with one pending: current=10, min=10, pending=1.
    /// 10 > 10+1=11 → false. No drop.
    #[test]
    fn at_min_with_pending_no_drop() {
        assert_eq!(deferred_swap_drops_to_execute(10, 10, 1), 0);
    }

    /// current = min + 1 with one pending: current=11, min=10, pending=1.
    /// 11 > 11 → false. No drop (1 extra but still need the pending slot covered).
    #[test]
    fn one_above_min_with_pending_no_drop() {
        assert_eq!(deferred_swap_drops_to_execute(11, 10, 1), 0);
    }

    /// current = min + 2 with one pending: current=12, min=10, pending=1.
    /// 12 > 11 → true. One drop allowed.
    #[test]
    fn two_above_min_with_one_pending_drops_one() {
        assert_eq!(deferred_swap_drops_to_execute(12, 10, 1), 1);
    }

    /// Overflow-safe: current=0, min=0, pending=0.
    #[test]
    fn all_zero_returns_zero() {
        assert_eq!(deferred_swap_drops_to_execute(0, 0, 0), 0);
    }

    /// current < min (below minimum, e.g. still bootstrapping): no drops.
    #[test]
    fn below_min_connections_no_drop() {
        assert_eq!(deferred_swap_drops_to_execute(5, 10, 2), 0);
    }
}

/// Source-grep pin test: lock down the set of bare `Instant::now()` call
/// sites in this file so a future change can't silently reintroduce a
/// wall-clock time read on the connection-maintenance path.
///
/// All production time reads in `ring.rs` go through `self.time_source.now()`
/// (the injectable `TimeSource`) so simulation/governance tests can drive the
/// connection-maintenance loop under `tokio::time::start_paused(true)` (or a
/// `SharedMockTimeSource`) deterministically. See #4277 and the #4260 fix that
/// migrated the first such call site (`report_contract_resource_usage`).
///
/// Two categories are deliberately exempt and are NOT bare `Instant::now()`:
///   * `boot_time::Instant::now()` / `WallClockInstant::now()` — the OS
///     suspend/resume detector in `connection_maintenance`, which by design
///     needs real wall-clock time (see `.claude/rules/code-style.md`).
///     These carry a type prefix, so they don't match a *bare* `Instant::now()`.
///
/// The only remaining bare `Instant::now()` call sites live in two
/// `#[cfg(test)]` modules:
///   * `gateway_version_probe_predicate_tests` (6 sites) — constructs inputs
///     for the pure `should_probe_gateway` predicate (no production time read).
///   * `resource_meter_bridge_tests` (8 sites) — drives the #3453 bandwidth
///     bridge directly with synthetic timestamps; `feed_peer_bandwidth_to_meter`
///     itself takes the time as a parameter, and the production caller in
///     `connection_maintenance` supplies `self.time_source.now()`.
///
/// If you add a new production time read, route it through
/// `self.time_source.now()` rather than bumping this count.
/// Per-side nearest-neighbor lattice distances observed at a maintenance tick:
/// the unsigned ring distance to the nearest connected successor / predecessor,
/// or `None` when that side has no connected neighbor. Used by
/// [`lattice_probe_progress`] to detect a fill OR a tighten between ticks.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LatticeSides {
    pub succ: Option<f64>,
    pub pred: Option<f64>,
}

/// Classification of the lattice change between two consecutive maintenance
/// ticks, for the CONTINUOUS discovery backoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LatticeProbeProgress {
    /// A side was newly FILLED, or an already-held side's nearest neighbor got
    /// strictly CLOSER (a successful tighten). Resets the probe backoff to tau0.
    pub improved: bool,
    /// A side was LOST (a held neighbor dropped, leaving the side empty). Resets
    /// the backoff to tau0 and re-probes promptly to re-fill the dead-end.
    pub regressed: bool,
}

/// Classify the lattice change between the previous tick's per-side nearest
/// distances (`prev`, `None` on the first observation) and this tick's (`curr`),
/// for CONTINUOUS discovery. An IMPROVEMENT — a side newly filled OR an
/// already-held side whose nearest got strictly closer — keeps the probe
/// aggressive; a REGRESSION — a side lost — re-probes promptly; a plateau
/// (neither) lets the backoff decay toward tau_max. This is the tighten-aware
/// replacement for the old fill-only STOP-when-filled gate: a filled side is no
/// longer a stop condition, only a plateau input, so the probe keeps tightening
/// a loose edge toward the peer's TRUE nearest ring neighbor.
pub(crate) fn lattice_probe_progress(
    prev: Option<LatticeSides>,
    curr: LatticeSides,
) -> LatticeProbeProgress {
    let Some(prev) = prev else {
        // First observation: any held side is a fill from the cold-start baseline.
        return LatticeProbeProgress {
            improved: curr.succ.is_some() || curr.pred.is_some(),
            regressed: false,
        };
    };
    // A side improved if it went empty -> held (a fill) or held -> strictly closer
    // (a tighten). Distances are derived from fixed peer locations, so a side's
    // nearest only changes when the connection set changes; a strict `<` is a real
    // tighten, not float jitter.
    let side_improved = |p: Option<f64>, c: Option<f64>| match (p, c) {
        (None, Some(_)) => true,
        (Some(pd), Some(cd)) => cd < pd,
        _ => false,
    };
    let side_regressed = |p: Option<f64>, c: Option<f64>| matches!((p, c), (Some(_), None));
    LatticeProbeProgress {
        improved: side_improved(prev.succ, curr.succ) || side_improved(prev.pred, curr.pred),
        regressed: side_regressed(prev.succ, curr.succ) || side_regressed(prev.pred, curr.pred),
    }
}

#[cfg(test)]
mod lattice_probe_state_machine_tests {
    use super::{LatticeSides, lattice_probe_progress};
    use crate::util::backoff::ExponentialBackoff;
    use std::time::Duration;

    /// CONTINUOUS discovery has NO sides-based stop condition. A tick where both
    /// sides are filled and UNCHANGED is a plateau (neither improved nor
    /// regressed) — the backoff grows, but the probe still fires on timing, so
    /// discovery never goes silent when both sides are filled. This is the
    /// behavioral reversal of the old fill-only stop-when-filled gate.
    #[test]
    fn both_sides_filled_and_unchanged_is_a_plateau_not_a_stop() {
        let both = LatticeSides {
            succ: Some(0.05),
            pred: Some(0.05),
        };
        let p = lattice_probe_progress(Some(both), both);
        assert!(
            !p.improved && !p.regressed,
            "a steady filled state is a plateau, not a stop"
        );
    }

    /// A newly-filled side (empty -> held) is an improvement.
    #[test]
    fn fill_is_an_improvement() {
        let prev = LatticeSides {
            succ: None,
            pred: Some(0.1),
        };
        let curr = LatticeSides {
            succ: Some(0.2),
            pred: Some(0.1),
        };
        let p = lattice_probe_progress(Some(prev), curr);
        assert!(p.improved && !p.regressed);
    }

    /// A TIGHTEN — an already-held side whose nearest gets strictly closer — is an
    /// improvement. This is exactly the signal the fill-only probe ignored.
    #[test]
    fn tighten_of_a_filled_side_is_an_improvement() {
        let prev = LatticeSides {
            succ: Some(0.20),
            pred: Some(0.10),
        };
        let curr = LatticeSides {
            succ: Some(0.08),
            pred: Some(0.10),
        };
        let p = lattice_probe_progress(Some(prev), curr);
        assert!(
            p.improved,
            "a strictly-closer nearest on a filled side is an improvement"
        );
        assert!(!p.regressed);
    }

    /// A lost side (held -> empty) is a regression; re-widening a held side (a
    /// farther nearest, e.g. the closest dropped and a farther one remains) is NOT
    /// an improvement.
    #[test]
    fn lost_side_is_a_regression_and_widening_is_not_improvement() {
        let prev = LatticeSides {
            succ: Some(0.05),
            pred: Some(0.05),
        };
        let lost = LatticeSides {
            succ: None,
            pred: Some(0.05),
        };
        let p = lattice_probe_progress(Some(prev), lost);
        assert!(p.regressed && !p.improved);

        let widened = LatticeSides {
            succ: Some(0.30),
            pred: Some(0.05),
        };
        let p2 = lattice_probe_progress(Some(prev), widened);
        assert!(
            !p2.improved && !p2.regressed,
            "a farther nearest on a still-held side is a plateau, not an improvement"
        );
    }

    /// First observation: any held side counts as a fill from the cold baseline;
    /// nothing held is a plateau.
    #[test]
    fn first_observation_classification() {
        let held = LatticeSides {
            succ: Some(0.3),
            pred: None,
        };
        let p = lattice_probe_progress(None, held);
        assert!(p.improved && !p.regressed);

        let empty = LatticeSides {
            succ: None,
            pred: None,
        };
        let p0 = lattice_probe_progress(None, empty);
        assert!(!p0.improved && !p0.regressed);
    }

    /// The probe backoff uses the shared `ExponentialBackoff` and DECAYS toward
    /// tau_max on a plateau, never stopping. The maintenance loop grows the attempt
    /// one step per fire and resets it to 0 on an improvement/regression; mirror
    /// that rule here and assert the interval is always finite (the probe keeps
    /// firing — continuous, never silent), monotonic, and floored at tau_max.
    #[test]
    fn backoff_decays_to_tau_max_and_never_stops() {
        let tau0 = Duration::from_secs(5);
        let tau_max = Duration::from_secs(300);
        let backoff = ExponentialBackoff::new(tau0, tau_max);
        assert_eq!(backoff.delay(0), tau0, "attempt 0 probes at tau0");

        let mut attempt: u32 = 0;
        let mut last = Duration::ZERO;
        for _ in 0..20 {
            let d = backoff.delay(attempt);
            assert!(d >= last, "interval is monotonic non-decreasing");
            assert!(d <= tau_max, "interval floors at tau_max, never longer");
            last = d;
            attempt = attempt.saturating_add(1);
        }
        assert_eq!(
            last, tau_max,
            "the backoff saturates at (floors at) tau_max"
        );

        // An improvement or a lost edge resets the attempt to 0 -> aggressive tau0.
        assert_eq!(backoff.delay(0), tau0);
    }
}

#[cfg(test)]
mod instant_now_pin_test {
    /// Number of bare `Instant::now()` call sites expected in this file, all
    /// in test code. Production code must use `self.time_source.now()`.
    /// 6 in `gateway_version_probe_predicate_tests` + 8 in
    /// `resource_meter_bridge_tests` (#3453).
    const EXPECTED_BARE_INSTANT_NOW: usize = 14;

    #[test]
    fn no_unexpected_bare_instant_now_call_sites() {
        let src = include_str!("ring.rs");
        // Build the needle from fragments so this test's own source line does
        // not itself contain a verbatim bare call-site token that it would count.
        let needle = format!("Instant{}now()", "::");
        let mut count = 0usize;
        for line in src.lines() {
            // Strip a `//` line comment so doc/comment mentions (including this
            // test's own docs) don't count.
            let code = match line.split_once("//") {
                Some((before, _)) => before,
                None => line,
            };
            // Count bare occurrences — i.e. not preceded by an identifier or
            // path separator (excludes `boot_time::Instant::now()` and
            // `WallClockInstant::now()`).
            let bytes = code.as_bytes();
            let mut idx = 0;
            while let Some(pos) = code[idx..].find(&needle) {
                let abs = idx + pos;
                let prev_is_pathy = abs
                    .checked_sub(1)
                    .map(|p| {
                        let c = bytes[p];
                        c == b':' || c == b'_' || c.is_ascii_alphanumeric()
                    })
                    .unwrap_or(false);
                if !prev_is_pathy {
                    count += 1;
                }
                idx = abs + needle.len();
            }
        }
        assert_eq!(
            count, EXPECTED_BARE_INSTANT_NOW,
            "Unexpected number of bare wall-clock time-read call sites in ring.rs. \
             Production code must read time via `self.time_source.now()` so the \
             connection-maintenance loop stays deterministic under start_paused \
             (#4277). If you intentionally added or removed a TEST-only call site, \
             update EXPECTED_BARE_INSTANT_NOW; if this fired on PRODUCTION code, \
             migrate it to `self.time_source.now()` instead of bumping the count."
        );
    }
}

/// Pin tests for the periodic hosting-advertisement re-request (#4642 spec step
/// 1, "Fix 1"): the reliability backstop that heals a dropped on-connect
/// advertisement exchange or a dropped per-eviction retraction. It is PIGGYBACKED
/// on the `interest_heartbeat` loop (NOT a separate task) so it adds no
/// `GlobalRng` draw / second timer that would perturb the deterministic sim
/// harness. These source-scrape pins lock that: the re-request rides
/// `interest_heartbeat`, and it carries only contract IDs (no state), so it can't
/// re-arm the #4440/#4473 summarize storm.
#[cfg(test)]
mod hosting_heartbeat_pin_test {
    fn extract_interest_heartbeat_body() -> &'static str {
        let src = include_str!("ring.rs");
        let head = ["async fn ", "interest_heartbeat("].concat();
        let start = src
            .find(&head)
            .expect("`async fn interest_heartbeat(` must exist in ring.rs");
        let body_open = src[start..].find('{').map(|off| start + off).unwrap();
        let mut depth: i32 = 0;
        let mut end = body_open;
        for (i, ch) in src[body_open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = body_open + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        &src[start..end]
    }

    #[test]
    fn hosting_re_request_is_piggybacked_on_interest_heartbeat() {
        let body = extract_interest_heartbeat_body();
        assert!(
            body.contains("HostingStateRequest"),
            "the advertisement-layer re-request (Fix 1) must be PIGGYBACKED on \
             `interest_heartbeat` — it must send a `HostingStateRequest` in that \
             loop. Do NOT move it to a separate task: a task with its own \
             `GlobalRng` initial-delay draw perturbs the shared seeded RNG stream \
             the deterministic sim harness depends on."
        );
    }

    #[test]
    fn hosting_re_request_carries_no_state() {
        // Advertisement-layer ONLY: the re-request must NOT reach into the STATE
        // anti-entropy (no WASM summarize / state fetch), or it would risk
        // re-arming the #4440/#4473 summarize storm the ring.md gate guards against.
        let body = extract_interest_heartbeat_body();
        assert!(
            !body.contains("get_contract_summary") && !body.contains("summarize_state"),
            "the piggybacked hosting re-request must carry only contract IDs — \
             never fetch / summarize STATE — so it cannot re-arm the summarize storm."
        );
    }

    #[test]
    fn no_separate_hosting_heartbeat_task() {
        let src = include_str!("ring.rs");
        // Runtime-compose so this test does not match itself.
        let needle = ["async fn ", "hosting_heartbeat("].concat();
        assert!(
            !src.contains(&needle),
            "there must be NO separate `hosting_heartbeat` task — its startup \
             `GlobalRng` draw perturbed the deterministic sim harness. The \
             re-request is piggybacked on `interest_heartbeat` instead."
        );
    }
}

/// Direct unit coverage for the pure core of `is_subscription_root` /
/// `body_holding_subscription_root_key` (#4440). Building a full `Ring`
/// fixture is heavyweight (async `Ring::new` spawns background tasks), so the
/// distance + routability decision is factored into the pure
/// `no_closer_routable_neighbor` helper and tested here. The hosting-gate and
/// instance-id resolution are covered end-to-end by the simulation test
/// `test_subscription_root_renewal_does_not_storm`.
#[cfg(test)]
mod subscription_root_predicate_tests {
    use super::no_closer_routable_neighbor;
    use crate::ring::location::Location;

    // Contract at 0.50; "me" hosting it at distance 0.05 (I'm at 0.55).
    const CONTRACT: f64 = 0.50;
    fn contract_loc() -> Location {
        Location::new(CONTRACT)
    }
    fn my_distance() -> super::Distance {
        Location::new(0.55).distance(contract_loc())
    }

    #[test]
    fn no_neighbors_means_terminus() {
        // Hosting, closest by default (nobody else) → terminus.
        assert!(no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            std::iter::empty(),
        ));
    }

    #[test]
    fn routable_closer_neighbor_means_not_terminus() {
        // A ready, non-transient peer AT the key (distance 0) is strictly closer
        // → I am NOT the terminus.
        let neighbors = [(Some(Location::new(CONTRACT)), true)];
        assert!(!no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            neighbors.into_iter(),
        ));
    }

    #[test]
    fn farther_routable_neighbor_keeps_terminus() {
        // A routable peer that is FARTHER from the key than me (at 0.20,
        // distance 0.30 > my 0.05) doesn't unseat me.
        let neighbors = [(Some(Location::new(0.20)), true)];
        assert!(no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            neighbors.into_iter(),
        ));
    }

    #[test]
    fn closer_but_non_routable_neighbor_keeps_terminus() {
        // The ONLY closer neighbor is non-routable (a transient peer, which
        // `k_closest` excludes unconditionally). A renewal could not route to
        // it, so I am still the effective terminus — the #4440 false-negative
        // guard. (Not-ready peers are mapped to routable=true by
        // `is_subscription_root` because `k_closest` falls back to them, so they
        // do NOT reach this `routable=false` case — see the mapping there.)
        let neighbors = [(Some(Location::new(CONTRACT)), false)];
        assert!(no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            neighbors.into_iter(),
        ));
    }

    #[test]
    fn closer_routable_among_non_routable_means_not_terminus() {
        // Mixed: one closer non-routable peer (ignored) AND one closer routable
        // peer (decisive) → NOT the terminus.
        let neighbors = [
            (Some(Location::new(0.51)), false), // closer but non-routable → skip
            (Some(Location::new(0.49)), true),  // closer AND routable → unseats
        ];
        assert!(!no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            neighbors.into_iter(),
        ));
    }

    #[test]
    fn locationless_neighbor_does_not_unseat() {
        // A routable neighbor with no known location can't be compared on
        // distance, so it never makes us "not the root" on its own.
        let neighbors = [(None, true)];
        assert!(no_closer_routable_neighbor(
            my_distance(),
            contract_loc(),
            neighbors.into_iter(),
        ));
    }
}

/// Direct unit coverage for the pure core of `Ring::most_keyward_hosting_neighbor`
/// (piece D, computed-upstream selection). Building a full `Ring` fixture is
/// heavyweight (async `Ring::new` spawns background tasks) and a peer's ring
/// location is derived from its (masked) socket address, so distances aren't
/// freely settable through a real `ConnectionManager`. The strict-closer +
/// deterministic-tiebreak selection is therefore factored into the pure
/// `most_keyward_among` helper and tested here with distances supplied directly;
/// the pub-key resolution / own-location glue is a thin wrapper covered
/// end-to-end by the computed-upstream simulation proofs (later keystone steps).
#[cfg(test)]
mod most_keyward_tests {
    use super::most_keyward_among;
    use crate::ring::PeerKeyLocation;
    use crate::ring::location::{Distance, Location};
    use crate::transport::TransportKeypair;
    use std::net::SocketAddr;

    // Contract at 0.50; "me" hosting it at distance 0.10 (I'm at 0.60).
    const CONTRACT: f64 = 0.50;
    fn my_distance() -> Distance {
        Location::new(0.60).distance(Location::new(CONTRACT))
    }

    /// A candidate peer at `addr` whose distance-to-contract is `dist`. The
    /// distance is supplied directly (the helper takes it pre-computed), so it is
    /// decoupled from the address — the address only drives the tiebreak.
    fn candidate(addr: &str, dist: f64) -> (PeerKeyLocation, Distance) {
        let addr: SocketAddr = addr.parse().unwrap();
        let pk = TransportKeypair::new().public().clone();
        (PeerKeyLocation::new(pk, addr), Distance::new(dist))
    }

    #[test]
    fn picks_closest_strictly_closer_neighbor() {
        // Two neighbors, both strictly closer than my 0.10: one at 0.05, one at
        // 0.02. The most-keyward (smallest distance) wins.
        let near = candidate("127.0.0.1:9001", 0.02);
        let mid = candidate("127.0.0.1:9002", 0.05);
        let want = near.0.clone();
        let picked = most_keyward_among(my_distance(), [mid, near].into_iter());
        assert_eq!(
            picked,
            Some(want),
            "the neighbor closest to the key (0.02) must be chosen over the farther-but-still-closer one (0.05)"
        );
    }

    #[test]
    fn excludes_farther_or_equal_neighbors() {
        // This test PINS the strict `<` boundary (the acyclicity guarantee)
        // against a regression to `<=`, so the "equal" candidate's distance must
        // be BIT-IDENTICAL to `my_distance()`. The literal 0.10 is NOT: the ring
        // distance `Location::new(0.60).distance(Location::new(0.50))` is
        // `0.09999999999999998`, whereas `Distance::new(0.10)` stores
        // `0.10000000000000001`, so a candidate built from `0.10` is actually
        // strictly GREATER and is excluded as "farther" under BOTH `<` and `<=`
        // — pinning nothing. Feeding `my_distance()` back in (via `as_f64`, which
        // `Distance::new` stores verbatim for values <= 0.5) makes the candidate
        // exactly equal, so `<` excludes it (None) while a regression to `<=`
        // would include and pick it (Some): the two now disagree and this test
        // fails under `<=`.
        let equal_dist = my_distance().as_f64();

        // A strictly-closer neighbor (0.02) coexists with one at EXACTLY my
        // distance and one FARTHER (0.15). Only the strictly-closer one is
        // eligible, so it is chosen — the equal and farther peers are excluded.
        let closer = candidate("127.0.0.1:9001", 0.02);
        let equal = candidate("127.0.0.1:9002", equal_dist);
        let farther = candidate("127.0.0.1:9003", 0.15);
        let want = closer.0.clone();
        let picked = most_keyward_among(my_distance(), [equal, farther, closer].into_iter());
        assert_eq!(
            picked,
            Some(want),
            "only the strictly-closer neighbor is eligible"
        );

        // With ONLY an equal-distance neighbor present, the strict `<` excludes
        // it → None (a peer at our exact distance never becomes our upstream, the
        // acyclicity guarantee). This is the assertion that DISTINGUISHES `<`
        // from `<=`: under the shipped `<` the equal candidate yields None, while
        // a regression to `<=` would yield `Some(equal)` and fail here.
        let equal_only = candidate("127.0.0.1:9002", equal_dist);
        assert_eq!(
            most_keyward_among(my_distance(), std::iter::once(equal_only)),
            None,
            "a neighbor at exactly our distance is farther-or-equal and must be excluded (pins `<`, not `<=`)"
        );
    }

    #[test]
    fn none_when_no_strictly_closer_neighbor() {
        // Empty candidate set → None.
        assert_eq!(
            most_keyward_among(my_distance(), std::iter::empty()),
            None,
            "no candidates → no computed upstream"
        );
        // All candidates farther than us → None (we are the terminus / stranded).
        let far_a = candidate("127.0.0.1:9001", 0.15);
        let far_b = candidate("127.0.0.1:9002", 0.30);
        assert_eq!(
            most_keyward_among(my_distance(), [far_a, far_b].into_iter()),
            None,
            "every candidate farther than us → no strictly-closer upstream"
        );
    }

    #[test]
    fn deterministic_tiebreak_on_equal_distance() {
        // Two neighbors equidistant from the key (both 0.03, both strictly closer
        // than 0.10) but with different socket addresses. The tiebreak picks the
        // smaller socket address, and the result is independent of iteration
        // order.
        let low = candidate("127.0.0.1:9001", 0.03);
        let high = candidate("127.0.0.1:9002", 0.03);
        let want = low.0.clone();

        let picked_fwd = most_keyward_among(my_distance(), [low.clone(), high.clone()].into_iter());
        let picked_rev = most_keyward_among(my_distance(), [high, low].into_iter());

        assert_eq!(
            picked_fwd,
            Some(want.clone()),
            "equidistant tie must resolve to the smaller socket address"
        );
        assert_eq!(
            picked_rev, picked_fwd,
            "the tiebreak must be deterministic regardless of iteration order"
        );
    }
}

#[cfg(test)]
mod sleep_or_shutdown_tests {
    use super::sleep_or_shutdown;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    /// The wrapped future completes before shutdown → returns `false` (carry on).
    #[tokio::test(start_paused = true)]
    async fn returns_false_when_wait_completes_first() {
        let shutdown = CancellationToken::new();
        let cancelled =
            sleep_or_shutdown(&shutdown, tokio::time::sleep(Duration::from_millis(10))).await;
        assert!(
            !cancelled,
            "an uncancelled token must let the wrapped sleep complete"
        );
    }

    /// Shutdown fires mid-wait → returns `true` (stop the loop) and does NOT
    /// block for the full sleep. Regression for the #4278 failure mode: a
    /// 5-minute sleep that ignores shutdown.
    #[tokio::test(start_paused = true)]
    async fn returns_true_promptly_when_cancelled_mid_wait() {
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        // Cancel after a short virtual delay, well before the long sleep ends.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            token.cancel();
        });

        let start = tokio::time::Instant::now();
        let cancelled =
            sleep_or_shutdown(&shutdown, tokio::time::sleep(Duration::from_secs(300))).await;
        let elapsed = start.elapsed();

        assert!(cancelled, "a cancelled token must short-circuit the sleep");
        assert!(
            elapsed < Duration::from_secs(300),
            "shutdown must not wait out the full 5-minute sleep (waited {elapsed:?})"
        );
    }

    /// Already-cancelled token → returns `true` immediately, even with a long
    /// wait. `CancellationToken` is level-triggered, so a task that reaches the
    /// helper after shutdown already fired still bails (no missed-wakeup race).
    #[tokio::test(start_paused = true)]
    async fn returns_true_when_already_cancelled() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let cancelled =
            sleep_or_shutdown(&shutdown, tokio::time::sleep(Duration::from_secs(300))).await;
        assert!(
            cancelled,
            "an already-cancelled token must short-circuit immediately"
        );
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingError {
    #[error(transparent)]
    ConnError(#[from] Box<node::ConnectionError>),
    /// Retained for completeness; client_events maps it to a stable
    /// `ClientError::EmptyRing`. Currently unconstructed.
    #[error("No ring connections found")]
    #[allow(dead_code)]
    EmptyRing,
    #[error("Ran out of, or haven't found any, hosting peers for contract {0}")]
    NoHostingPeers(ContractInstanceId),
    #[error("Peer has not joined the network yet (no ring location established)")]
    PeerNotJoined,
}
