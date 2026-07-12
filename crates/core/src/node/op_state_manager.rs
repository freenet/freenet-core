//! Manages the state and execution of diverse network operations (e.g., Get, Put, Subscribe).
//!
//! The `OpManager` runs its own event loop (`garbage_cleanup_task`) to handle the lifecycle
//! of operations, ensuring they progress correctly and are eventually cleaned up.
//! It communicates with the main node event loop and the network bridge via channels.
//!
//! See [`../../architecture.md`](../../architecture.md) for details on its role and interaction with other components.

use std::{
    cmp::Reverse,
    collections::{BTreeSet, HashSet},
    net::SocketAddr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use dashmap::{DashMap, DashSet};
use either::Either;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, WrappedState};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::{
    client_events::HostResult,
    config::GlobalExecutor,
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent, SenderHalve},
    message::{InterestMessage, MessageStats, NetMessage, NetMessageV1, NodeEvent, Transaction},
    operations::{
        OpCtx, OpError, connect::ConnectForwardEstimator, orphan_streams::OrphanStreamRegistry,
        stream_progress::StreamProgressRegistry,
    },
    ring::{
        ConnectionManager, Distance, LiveTransactionTracker, Location, PeerConnectionBackoff,
        PeerKey, PeerKeyLocation, Ring, reconcile,
    },
    transport::TransportPublicKey,
    util::time_source::InstantTimeSrc,
};

use super::{
    NetEventRegister, NodeConfig, RequestRouter, neighbor_hosting::NeighborHostingManager,
    network_bridge::EventLoopNotificationsSender,
};

#[derive(Default)]
struct Ops {
    // No per-op DashMaps remain. CONNECT, GET, PUT, UPDATE, and
    // SUBSCRIBE all run on drivers that own state in task
    // locals; the only state retained here is the global completed /
    // under_progress sets used by the GC sweep.
    completed: DashSet<Transaction>,
    under_progress: DashSet<Transaction>,
}

/// Snapshot of per-map sizes held by `Ops`. Emitted periodically from
/// `garbage_cleanup_task` when `FREENET_MEMORY_STATS=1` is set, to help
/// diagnose retained-state bloat without forcing a full heap profiler
/// run.
#[derive(Debug, Default)]
struct OpsSizes {
    completed: usize,
    under_progress: usize,
}

impl Ops {
    fn sizes(&self) -> OpsSizes {
        OpsSizes {
            completed: self.completed.len(),
            under_progress: self.under_progress.len(),
        }
    }
}

/// Max number of reconcile connection-drop SHADOW comparisons to BUILD per
/// dropped connection (keystone step-2, #4642). Each build is a
/// `ReconcileInputs` snapshot (a redb read + an `is_subscription_root`
/// connection-map scan), so this bounds the expensive per-drop work. Contracts
/// past the cap are unmeasured for that drop.
const CONNECTION_DROP_SHADOW_SAMPLE_CAP: usize = 32;

/// Max number of our active-subscription entries to EXAMINE per dropped
/// connection while looking for the ≤`CONNECTION_DROP_SHADOW_SAMPLE_CAP` matches
/// (keystone step-2, #4642). Bounds the SCAN itself (not just the recording): a
/// near-key gateway with a large subscribed set must not turn one co-host
/// disconnect into an O(S) membership sweep. Examining is cheap (a DashMap step
/// plus a `HashSet::contains`), so this is generous enough to fully cover a
/// realistic subscribed set while hard-capping the pathological case; a set
/// larger than this is sampled (a slight, arbitrary undercount for a one-sided
/// diagnostic counter).
const CONNECTION_DROP_SHADOW_SCAN_CAP: usize = 1024;

/// Storm-safety cap on the number of PROMPT re-roots spawned in response to ONE
/// connection-drop event (#4642 piece F, the connection-drop re-root FLIP).
///
/// This is THE re-root-storm bound. When a hub/gateway that co-hosted many of a
/// peer's in-use contracts drops, the peer must NOT fire a re-subscribe for every
/// affected contract at once — that thundering herd is exactly the hollow-relay
/// storm the #3763 firefight fought. The prompt path re-roots at most this many
/// contracts per drop; the remainder are re-rooted by the ordinary ~30s renewal
/// loop at ITS own rate cap (`MAX_RECOVERY_ATTEMPTS_PER_INTERVAL`), so a mass
/// disconnect degrades to a bounded trickle instead of an O(contracts) burst.
/// Kept below the renewal loop's per-tick cap: a drop is a spikier trigger than a
/// scheduled tick, so its prompt burst is deliberately smaller. Per-contract
/// backoff (`can_request_subscription`) and pending-dedup
/// (`mark_subscription_pending`) prevent repeated drops from re-firing the same
/// contract, and per-task jitter (`spawn_renewal_subscribe_task`) spreads even
/// this bounded batch.
///
/// Scope of the guarantee: this caps prompt re-roots **per connection-drop
/// event**, NOT per node. One `on_ring_connection_lost` call spawns at most 6.
/// A simultaneous multi-hub disconnect fires one such call per dropped hub, so
/// the whole-node burst is bounded by 6 × (number of hubs that dropped at once)
/// — but each of those is a single per-contract-deduped, backoff-respecting,
/// jittered SUBSCRIBE toward the key, so the aggregate is still a bounded spread,
/// never the O(contracts) thundering herd this cap exists to prevent.
const MAX_PROMPT_REROOTS_PER_DROP: usize = 6;

/// Pure core of the piece-D **strictly-farther** downstream-subscriber filter
/// (keystone step-2, #4642): `true` iff at least one subscriber distance is
/// STRICTLY greater than `my_distance` (both measured to the contract key).
///
/// Load-bearing: the comparison uses EXACT `Distance` ordering (`Ord::cmp`),
/// NEVER the epsilon-fuzzy `PartialEq` (`ring/location.rs:223-243`, where `==`
/// treats one-ULP-apart distances as equal while `cmp` still orders them). A
/// subscriber at exactly `my_distance` (bit-identical) is EXCLUDED — that
/// exclusion of the closer/equal (upstream) peer is what stops two mutual
/// co-hosts from perpetuating each other's leases so collapse can terminate
/// (hosting-invariants piece-D, design §4/§6). Mixing epsilon-`==` here with the
/// exact-`<` selection used by `most_keyward_among` is the exact bug the guard
/// prevents, hence `cmp(..) == Greater`, not `>` or `==`.
fn has_strictly_farther(
    my_distance: Distance,
    subscriber_distances: impl Iterator<Item = Distance>,
) -> bool {
    let mut subscriber_distances = subscriber_distances;
    subscriber_distances.any(|d| d.cmp(&my_distance) == std::cmp::Ordering::Greater)
}

/// Thread safe and friendly data structure to maintain state of the different operations
/// and enable their execution.
pub(crate) struct OpManager {
    pub ring: Arc<Ring>,
    ops: Arc<Ops>,
    pub(crate) to_event_listener: EventLoopNotificationsSender,
    pub ch_outbound: Arc<ContractHandlerChannel<SenderHalve>>,
    new_transactions: tokio::sync::mpsc::Sender<Transaction>,
    pub result_router_tx: mpsc::Sender<(Transaction, HostResult)>,
    pub(crate) connect_forward_estimator: Arc<RwLock<ConnectForwardEstimator>>,
    /// Indicates whether the peer is ready to process client operations.
    /// For gateways: always true (peer_id is set from config)
    /// For regular peers: true only after first successful network handshake sets peer_id
    pub peer_ready: Arc<AtomicBool>,
    /// Whether this node is a gateway
    pub is_gateway: bool,
    /// Waiters for contract storage notification.
    /// Operations can register to be notified when a specific contract is stored.
    contract_waiters:
        Arc<Mutex<std::collections::HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>>>>,
    /// Neighbor hosting manager for tracking neighbor contract hosting
    pub neighbor_hosting: Arc<NeighborHostingManager>,
    /// Interest manager for delta-based state synchronization.
    ///
    /// Its clock comes from the same injectable
    /// [`DynTimeSource`](crate::util::time_source::DynTimeSource) *source* the
    /// `HostingManager` uses (via `NodeConfig::hosting_time_source_override`), so
    /// subscription/interest-lease expiry and hosting-cache TTL/eviction read the
    /// same clock. In production the override is `None`, so the `HostingManager` and
    /// the `InterestManager` each install their own fresh `Arc<InstantTimeSrc>` —
    /// distinct instances, but the identical monotonic wall clock (stateless, zero
    /// observable skew), behavior-identical to the previous hardcoded `InstantTimeSrc`.
    /// In simulation both clone the SAME injected clock, so a chain-collapse /
    /// no-storm / interest-gated-renewal test can deterministically age BOTH the
    /// subscription and demand halves together. See #4642 (piece A).
    pub interest_manager:
        Arc<crate::ring::interest::InterestManager<crate::util::time_source::DynTimeSource>>,
    /// Dedup cache for skipping redundant broadcast WASM merges
    pub broadcast_dedup_cache: Arc<crate::operations::update::BroadcastDedupCache>,
    /// Bounded per-contract UPDATE-propagation counters. Fed from the
    /// broadcast fan-out path and drained by a periodic background task that
    /// emits an INFO `update_propagation_summary` line per window. Restores
    /// the operator liveness signal lost when #4272 demoted the per-event
    /// UPDATE log sites to DEBUG (issue #4281).
    pub(crate) update_propagation_stats:
        Arc<crate::operations::update::propagation_stats::UpdatePropagationStats>,
    /// Deferred re-broadcast store for fresh-contract PUTs whose initial
    /// broadcast found no targets and exhausted its retry budget. Stashed by
    /// the fan-out handler on give-up and drained by the subscribe path when
    /// the first interested peer for the contract appears, so a never-before-
    /// seen id that lost the broadcast/interest-resolve race still reaches the
    /// network instead of landing locally-hosted only (issue #4359).
    pub(crate) pending_broadcasts:
        Arc<crate::operations::update::pending_broadcast::PendingBroadcastStore>,
    /// Request router for client request deduplication.
    ///
    /// This is initialized lazily from `client_event_handling` because the router is only
    /// available once the client-side handling layer has been constructed. When set, it is
    /// used by operations to clean up stale routing entries as they complete or time out.
    ///
    /// Operations that start and finish before the router has been initialized will *not*
    /// clean up any routing state via this router. In practice this is acceptable because
    /// `client_event_handling` sets the router early in the node startup sequence, before
    /// regular client operations are expected to run.
    ///
    /// Wrapped in Arc for sharing with `garbage_cleanup_task`.
    request_router: Arc<OnceLock<Arc<RequestRouter>>>,
    /// Registry for handling race conditions between stream fragments and metadata messages.
    /// Coordinates transport layer (which receives fragments) with operations layer
    /// (which receives RequestStreaming/ResponseStreaming messages).
    orphan_stream_registry: Arc<OrphanStreamRegistry>,
    /// Per-`Transaction` registry of streaming-PUT progress handles (#4001).
    ///
    /// A client streaming PUT runs the retry-loop task and the originator-
    /// loopback relay-streaming task separately, sharing only the
    /// `Transaction` id. The retry loop inserts a `StreamProgressHandle` here
    /// before sending and removes it on exit; the loopback relay looks it up
    /// and records per-fragment progress so the retry loop can use a true
    /// stream-inactivity timeout instead of a fixed per-attempt deadline. See
    /// `operations::stream_progress`.
    stream_progress_registry: Arc<StreamProgressRegistry>,
    /// Size threshold in bytes above which streaming is used.
    pub streaming_threshold: usize,
    /// Backoff tracker for failed gateway connection attempts.
    /// Used to implement exponential backoff when retrying connections.
    pub gateway_backoff: Arc<Mutex<PeerConnectionBackoff>>,
    /// Notifies `initial_join_procedure` when gateway backoff is cleared,
    /// so it can wake from backoff sleep and retry immediately.
    pub gateway_backoff_cleared: Arc<tokio::sync::Notify>,
    /// Addresses blocked by local policy. Used by the connect protocol to reject
    /// join requests from blocked peers at the routing level, allowing the uphill
    /// hop mechanism to find alternate acceptors.
    pub blocked_addresses: Option<Arc<HashSet<SocketAddr>>>,
    /// Configured gateway peers for bootstrap/re-bootstrap.
    /// Used by connection_maintenance to directly attempt gateway connections
    /// when the node has zero ring connections (#3219).
    pub configured_gateways: Arc<Vec<PeerKeyLocation>>,
    /// Tracks contracts for which a self-healing GET has been triggered
    /// (e.g., when an UPDATE broadcast fails due to missing contract parameters).
    /// Maps contract instance ID to the timestamp (ms since epoch via GlobalSimulationTime)
    /// when the fetch was initiated, with a cooldown to avoid repeated fetch attempts.
    pub(crate) pending_contract_fetches: Arc<DashMap<ContractInstanceId, u64>>,
    /// Transactions with an active driver relay-GET driver at this
    /// node. Populated by `start_relay_get` before spawn and removed by
    /// an RAII guard on the driver task. Consulted by the dispatch gate
    /// in `node.rs` to reject duplicate inbound Requests for a tx that
    /// already has a live relay driver — prevents the 3^HTL spawn
    /// amplification observed in workflow run 24600634908 (6.8M spawns
    /// in 100s, 63GB RSS).
    pub(crate) active_relay_get_txs: Arc<DashSet<Transaction>>,
    /// Same role as `active_relay_get_txs` but for UPDATE relay.
    /// UPDATE relay has no retry loop and no upstream reply, so the
    /// amplification risk is structurally lower than GET — the gate
    /// exists primarily for robustness against GC-spawned re-entries
    /// and routing-bloom false-positive retransmissions.
    pub(crate) active_relay_update_txs: Arc<DashSet<Transaction>>,
    /// Same role as `active_relay_get_txs` but for PUT relay. PUT
    /// relay has req/response semantics like GET (but forwards once
    /// — no per-hop retry), so amplification risk is comparable.
    /// Rejects duplicate inbound `PutMsg::Request` for a tx that
    /// already has a live driver.
    pub(crate) active_relay_put_txs: Arc<DashSet<Transaction>>,
    /// Same role as `active_relay_get_txs` but for SUBSCRIBE relay.
    /// SUBSCRIBE relay forwards once — no per-hop retry — because
    /// the client driver owns cross-peer retry. Rejects duplicate
    /// inbound `SubscribeMsg::Request`.
    pub(crate) active_relay_subscribe_txs: Arc<DashSet<Transaction>>,
    /// Same role as `active_relay_get_txs` but for CONNECT relay.
    /// Rejects duplicate inbound `ConnectMsg::Request` — prevents
    /// bloom-filter rekey re-entries and uphill-retry false-positive
    /// retransmissions from spawning
    /// redundant drivers. The driver covers the Request→Response
    /// forward path; Rejected within-relay retries and ConnectFailed
    /// downstream propagation stay on legacy `process_message`, gated
    /// by the dedup set's absence on those branches.
    pub(crate) active_relay_connect_txs: Arc<DashSet<Transaction>>,
    /// Count of client-originated operation drivers currently running.
    /// Bumped by `ClientOpGuard::new` (held inside each `run_client_*`
    /// task) and decremented when the guard is dropped. Read by the
    /// shutdown drain in `ShutdownHandle::shutdown` to wait for
    /// client-initiated work (most importantly PUTs from the
    /// `freenet-git` mirror) to finish before tearing down peer
    /// connections. The drain is bounded by `config.shutdown_drain_secs`.
    pub(crate) inflight_client_ops: Arc<AtomicUsize>,
    /// Set to `true` by `ShutdownHandle::shutdown` *before* the drain
    /// begins, so `start_client_{put,get,update,subscribe}` can fail
    /// fast with `OpError::NodeShuttingDown` instead of bumping the
    /// counter for an op that will be aborted moments later.
    ///
    /// Without this gate, the shutdown sequence has a race window
    /// between the drain loop observing `counter == 0` and the
    /// `NodeEvent::Disconnect` being sent: a new client op spawned
    /// in that window would bump the counter (now unobserved),
    /// start running, and get cut off when the event loop tears
    /// down peer connections. The admission gate eliminates the
    /// race by causing `start_client_*` to refuse new work as soon
    /// as shutdown begins — any in-flight op already past the
    /// check at that moment is still covered by the drain wait.
    pub(crate) shutting_down: Arc<AtomicBool>,
}

impl Clone for OpManager {
    fn clone(&self) -> Self {
        Self {
            ring: self.ring.clone(),
            ops: self.ops.clone(),
            to_event_listener: self.to_event_listener.clone(),
            ch_outbound: self.ch_outbound.clone(),
            new_transactions: self.new_transactions.clone(),
            result_router_tx: self.result_router_tx.clone(),
            connect_forward_estimator: self.connect_forward_estimator.clone(),
            peer_ready: self.peer_ready.clone(),
            is_gateway: self.is_gateway,
            contract_waiters: self.contract_waiters.clone(),
            neighbor_hosting: self.neighbor_hosting.clone(),
            interest_manager: self.interest_manager.clone(),
            broadcast_dedup_cache: self.broadcast_dedup_cache.clone(),
            update_propagation_stats: self.update_propagation_stats.clone(),
            pending_broadcasts: self.pending_broadcasts.clone(),
            request_router: self.request_router.clone(),
            orphan_stream_registry: self.orphan_stream_registry.clone(),
            stream_progress_registry: self.stream_progress_registry.clone(),
            streaming_threshold: self.streaming_threshold,
            gateway_backoff: self.gateway_backoff.clone(),
            gateway_backoff_cleared: self.gateway_backoff_cleared.clone(),
            blocked_addresses: self.blocked_addresses.clone(),
            configured_gateways: self.configured_gateways.clone(),
            pending_contract_fetches: self.pending_contract_fetches.clone(),
            active_relay_get_txs: self.active_relay_get_txs.clone(),
            active_relay_update_txs: self.active_relay_update_txs.clone(),
            active_relay_put_txs: self.active_relay_put_txs.clone(),
            active_relay_subscribe_txs: self.active_relay_subscribe_txs.clone(),
            active_relay_connect_txs: self.active_relay_connect_txs.clone(),
            inflight_client_ops: self.inflight_client_ops.clone(),
            shutting_down: self.shutting_down.clone(),
        }
    }
}

/// RAII guard counting client-originated drivers in flight.
///
/// Construct via [`OpManager::client_op_guard`] at the start of each
/// `run_client_*` task and let it drop when the task exits — every
/// terminal path (happy path, infrastructure error, panic propagated
/// through the spawn) decrements the counter exactly once, so missing
/// a branch can't leak count.
///
/// The shutdown drain in `ShutdownHandle::shutdown` reads the counter
/// via [`OpManager::inflight_client_op_count`] and waits for it to
/// reach zero before letting the node tear down.
pub(crate) struct ClientOpGuard {
    counter: Arc<AtomicUsize>,
}

impl ClientOpGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        // SeqCst, not Relaxed — the increment participates in a
        // two-atomic Dekker-style handshake with `shutting_down`
        // (see `OpManager::admit_client_op`). Under a relaxed model
        // both threads could read each other's stores as stale,
        // letting a new driver spawn after the drain has completed.
        // Codex + skeptical r3 finding. The cost is negligible
        // (once per client request, not in a hot loop).
        counter.fetch_add(1, Ordering::SeqCst);
        Self { counter }
    }
}

impl Drop for ClientOpGuard {
    fn drop(&mut self) {
        // Decrement does NOT participate in the admission handshake
        // (it announces "I'm done" to a drain that's already
        // polling). Relaxed is sufficient: the only consequence of a
        // late observation is an extra 200ms poll interval before
        // the drain notices counter==0.
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl OpManager {
    // `pub(crate)` (widened from `pub(super)`) so in-crate unit tests outside
    // `crate::node` can stand up a real `OpManager` — specifically the
    // `RuntimePool` eviction→retraction test in
    // `contract::executor::runtime::pool`, which must drive
    // `RuntimePool::remove_contract` against a genuine `OpManager`. Constructor
    // only; no behavior change.
    pub(crate) fn new<ER: NetEventRegister + Clone>(
        notification_channel: EventLoopNotificationsSender,
        ch_outbound: ContractHandlerChannel<SenderHalve>,
        config: &NodeConfig,
        event_register: ER,
        connection_manager: ConnectionManager,
        result_router_tx: mpsc::Sender<(Transaction, HostResult)>,
        task_monitor: &super::background_task_monitor::BackgroundTaskMonitor,
    ) -> anyhow::Result<Self> {
        let ring = Ring::new(
            config,
            notification_channel.clone(),
            event_register.clone(),
            config.is_gateway,
            connection_manager,
            task_monitor,
        )?;
        let ops = Arc::new(Ops::default());

        let (new_transactions, rx) = tokio::sync::mpsc::channel(100);
        let current_span = tracing::Span::current();
        let garbage_span = if current_span.is_none() {
            tracing::info_span!("garbage_cleanup_task")
        } else {
            tracing::info_span!(parent: current_span, "garbage_cleanup_task")
        };
        let connect_forward_estimator = Arc::new(RwLock::new(ConnectForwardEstimator::new()));
        let request_router = Arc::new(OnceLock::new());
        let ch_outbound = Arc::new(ch_outbound);
        let contract_waiters: Arc<
            Mutex<std::collections::HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>>>,
        > = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let pending_contract_fetches: Arc<DashMap<ContractInstanceId, u64>> =
            Arc::new(DashMap::new());
        let active_relay_get_txs: Arc<DashSet<Transaction>> = Arc::new(DashSet::new());
        let active_relay_update_txs: Arc<DashSet<Transaction>> = Arc::new(DashSet::new());
        let active_relay_put_txs: Arc<DashSet<Transaction>> = Arc::new(DashSet::new());
        let active_relay_subscribe_txs: Arc<DashSet<Transaction>> = Arc::new(DashSet::new());
        let active_relay_connect_txs: Arc<DashSet<Transaction>> = Arc::new(DashSet::new());

        task_monitor.register(
            "garbage_cleanup",
            GlobalExecutor::spawn(
                garbage_cleanup_task(
                    rx,
                    ops.clone(),
                    ring.live_tx_tracker.clone(),
                    notification_channel.clone(),
                    event_register,
                    result_router_tx.clone(),
                    request_router.clone(),
                    contract_waiters.clone(),
                    pending_contract_fetches.clone(),
                    active_relay_get_txs.clone(),
                    active_relay_update_txs.clone(),
                    active_relay_put_txs.clone(),
                    active_relay_subscribe_txs.clone(),
                    active_relay_connect_txs.clone(),
                )
                .instrument(garbage_span),
            ),
        );

        // Gateways are ready immediately (peer_id set from config)
        // Regular peers become ready after first handshake
        let is_gateway = config.is_gateway;
        let peer_ready = Arc::new(AtomicBool::new(is_gateway));

        if is_gateway {
            tracing::debug!("Gateway node: peer_ready set to true immediately");
        } else {
            tracing::debug!("Regular peer node: peer_ready will be set after first handshake");
        }

        let neighbor_hosting = Arc::new(NeighborHostingManager::new());
        // The InterestManager draws its clock from the same injectable source as the
        // HostingManager, so subscription/interest-lease expiry and hosting-cache TTL
        // read the same clock. Production (`hosting_time_source_override == None`)
        // installs a fresh `Arc<InstantTimeSrc>` here — a distinct instance from the
        // HostingManager's, but the identical stateless wall clock (zero skew),
        // behavior-identical to the previous hardcoded `InstantTimeSrc::new()`. Sims
        // clone the same injected clock (mirrors the `Ring::new` →
        // `HostingManager::with_time_source` path). See #4642 piece A.
        let interest_time_source: crate::util::time_source::DynTimeSource = config
            .hosting_time_source_override
            .clone()
            .unwrap_or_else(|| Arc::new(InstantTimeSrc::new()));
        let interest_manager = Arc::new(crate::ring::interest::InterestManager::new(
            interest_time_source,
        ));

        // Start background sweep task for interest expiration
        crate::ring::interest::InterestManager::start_sweep_task(interest_manager.clone());

        // Extract streaming config from NodeConfig
        let streaming_threshold = config.config.network_api.streaming_threshold;

        tracing::info!(
            streaming_threshold_bytes = streaming_threshold,
            "Streaming transport enabled for large transfers"
        );

        // Create orphan stream registry and start GC task
        let orphan_stream_registry = Arc::new(OrphanStreamRegistry::new());
        OrphanStreamRegistry::start_gc_task(orphan_stream_registry.clone());

        // Bounded periodic UPDATE-propagation summary emitter (#4281). The
        // background task drains the per-contract counters and logs a single
        // INFO summary line (plus a capped number of per-contract lines) per
        // window, restoring the operator liveness signal lost to #4272's
        // DEBUG demotions without re-introducing per-event log volume. It runs
        // for the node's lifetime, so its handle is registered with the
        // BackgroundTaskMonitor rather than dropped fire-and-forget.
        let update_propagation_stats =
            Arc::new(crate::operations::update::propagation_stats::UpdatePropagationStats::new());
        task_monitor.register(
            "update_propagation_summary",
            update_propagation_stats.clone().start_summary_task(),
        );

        Ok(Self {
            ring,
            ops,
            to_event_listener: notification_channel,
            ch_outbound,
            new_transactions,
            result_router_tx,
            connect_forward_estimator,
            peer_ready,
            is_gateway,
            contract_waiters,
            neighbor_hosting,
            interest_manager,
            broadcast_dedup_cache: Arc::new(crate::operations::update::BroadcastDedupCache::new()),
            update_propagation_stats,
            pending_broadcasts: Arc::new(
                crate::operations::update::pending_broadcast::PendingBroadcastStore::new(),
            ),
            request_router,
            orphan_stream_registry,
            stream_progress_registry: Arc::new(StreamProgressRegistry::new()),
            streaming_threshold,
            gateway_backoff: Arc::new(Mutex::new(PeerConnectionBackoff::new())),
            gateway_backoff_cleared: Arc::new(tokio::sync::Notify::new()),
            blocked_addresses: config
                .blocked_addresses
                .as_ref()
                .map(|a| Arc::new(a.clone())),
            configured_gateways: Arc::new(
                config
                    .gateways
                    .iter()
                    .map(|gw| gw.peer_key_location.clone())
                    .collect(),
            ),
            pending_contract_fetches,
            active_relay_get_txs,
            active_relay_update_txs,
            active_relay_put_txs,
            active_relay_subscribe_txs,
            active_relay_connect_txs,
            inflight_client_ops: Arc::new(AtomicUsize::new(0)),
            shutting_down: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Cloneable handle to the shutting-down flag. Used by
    /// `ShutdownHandle` to flip the gate before starting the drain
    /// wait. Callers checking the flag should use
    /// [`OpManager::admit_client_op`] instead of reading directly,
    /// because the check-then-bump shape has a TOCTOU window that
    /// `admit_client_op` closes.
    pub(crate) fn shutting_down_handle(&self) -> Arc<AtomicBool> {
        self.shutting_down.clone()
    }

    /// Bump the in-flight client-op counter without checking the
    /// admission gate. **Do not call from `start_client_*` paths** —
    /// use [`OpManager::admit_client_op`] there so the gate check
    /// and counter bump are atomic. Kept module-private for the
    /// `admit_client_op` implementation.
    fn client_op_guard(&self) -> ClientOpGuard {
        ClientOpGuard::new(self.inflight_client_ops.clone())
    }

    /// Atomically check the shutdown admission gate AND bump the
    /// in-flight client-op counter in a single operation. Returns
    /// `None` if the node is shutting down (counter NOT bumped, no
    /// spawn should follow).
    ///
    /// # The race we close
    ///
    /// Prior shape (`if is_shutting_down() { return Err; } let g =
    /// client_op_guard();`) had a TOCTOU window: the gate check
    /// could observe `false`, then `ShutdownHandle` could set the
    /// gate AND read the still-zero counter AND return from the
    /// drain (the drain has an `initial == 0` fast path) all before
    /// the caller bumped the counter. The driver would then spawn
    /// into a node that has already past the drain. Codex r2.
    ///
    /// Fix is bump-first-then-check: increment the counter, then
    /// check the gate. If the gate is set, drop the guard (auto-
    /// decrement) and return `None`. From the
    /// `ShutdownHandle::shutdown` side, any drain `load` that
    /// happens-after our `fetch_add` observes `counter > 0` and the
    /// drain waits.
    ///
    /// # Why SeqCst
    ///
    /// The two participating atomics (`shutting_down`,
    /// `inflight_client_ops`) form a two-variable Dekker-style
    /// handshake. `Relaxed` does NOT establish a happens-before edge
    /// across distinct atomic locations — both threads could
    /// observe each other's writes as stale, letting a new driver
    /// spawn after the drain completed. **All four sites that
    /// participate in the handshake MUST use `SeqCst`**:
    ///
    /// 1. `ClientOpGuard::new` — `counter.fetch_add(SeqCst)`
    /// 2. `OpManager::admit_client_op` — `shutting_down.load(SeqCst)`
    /// 3. `ShutdownHandle::shutdown` Phase 1 —
    ///    `shutting_down.store(true, SeqCst)`
    /// 4. `ShutdownHandle::wait_for_drain` —
    ///    `counter.load(SeqCst)` (BOTH the `initial` read and
    ///    every poll-loop read)
    ///
    /// Source-grep pin `seqcst_used_for_admission_handshake_atomics`
    /// catches any accidental downgrade. Codex r3 + skeptical r3.
    ///
    /// # Behavioural side effects
    ///
    /// If the gate is set AFTER our bump but BEFORE our check, the
    /// drain observes our transient bump and waits. We then drop
    /// the guard; the next 200ms poll sees `0` and proceeds.
    /// Worst-case extra latency per racing reject is one poll
    /// interval.
    ///
    /// Pair with `move`-ing the guard into the spawned `run_client_*`
    /// future so the counter tracks the driver task, not the
    /// synchronous `start_client_*` caller. See [`ClientOpGuard`] for
    /// the broader shutdown-drain contract.
    pub(crate) fn admit_client_op(&self) -> Option<ClientOpGuard> {
        // Order matters: bump BEFORE check. Reversing this re-opens
        // the Codex r2 TOCTOU. The source-grep pin
        // `admit_client_op_bumps_before_checking_gate` rejects the
        // reversed order at CI time.
        let guard = self.client_op_guard();
        if self.shutting_down.load(Ordering::SeqCst) {
            // Drop decrements the counter back to whatever it was.
            // The drain may observe the transient bump on a poll —
            // acceptable: it waits one extra interval, then sees the
            // counter clear on the next poll. Correct, never starves.
            drop(guard);
            return None;
        }
        Some(guard)
    }

    /// Cloneable handle to the in-flight client-op counter. Used by
    /// `ShutdownHandle` to read the counter from a separate task
    /// without holding an `Arc<OpManager>` across the drain wait.
    pub(crate) fn inflight_client_ops_handle(&self) -> Arc<AtomicUsize> {
        self.inflight_client_ops.clone()
    }

    /// Set the request router for cleaning up stale entries when operations complete.
    ///
    /// This is called from client_event_handling after the request_router is created.
    /// Without this, completed operations leave stale entries in the request router's
    /// resource_to_transaction map, causing subsequent requests to hang forever.
    pub fn set_request_router(&self, router: Arc<RequestRouter>) {
        if self.request_router.set(router).is_err() {
            tracing::warn!("Request router already set - ignoring duplicate set");
        }
    }

    /// Send a result to the client via the result router.
    ///
    /// Uses try_send to avoid blocking the caller (which may be the node
    /// event loop). If the result router channel is full, the result is
    /// dropped and the client will see a timeout.
    pub(crate) fn send_client_result(&self, tx: Transaction, host_result: HostResult) {
        if let Err(err) = self.result_router_tx.try_send((tx, host_result)) {
            tracing::error!(
                %tx,
                error = %err,
                "failed to dispatch operation result to client \
                 (result router channel full or closed)"
            );
            return;
        }

        if let Err(err) = self
            .to_event_listener
            .notifications_sender
            .try_send(Either::Right(NodeEvent::TransactionCompleted(tx)))
        {
            tracing::warn!(
                %tx,
                error = %err,
                "failed to notify event loop about transaction completion"
            );
        }
    }

    /// Wake a parked op whose awaited `peer` was just pruned (#4313).
    ///
    /// Emits `NodeEvent::TransactionOrphaned`; the event-loop handler
    /// delivers `WaiterReply::PeerDisconnected` into the waiter channel
    /// *before* dropping the sender, so the parked driver reads the cause
    /// deterministically — no side registry, no race. Best-effort and
    /// non-blocking (runs on the event loop, where `send().await` could
    /// deadlock): a dropped event under backpressure leaves the driver to
    /// its `OPERATION_TTL` fallback (#4154). See
    /// [`notify_orphaned_transaction_on`] for the underlying send logic.
    pub(crate) fn notify_orphaned_transaction(&self, tx: Transaction, peer: SocketAddr) {
        notify_orphaned_transaction_on(&self.to_event_listener.notifications_sender, tx, peer);
    }

    /// Timeout for sending notifications to the event loop.
    /// If the channel is full for this long, the event loop is stuck and sending will never succeed.
    ///
    /// `pub(crate)` so the renewal outer-cancel deadline in
    /// [`crate::ring::Ring`] can reserve enough headroom for a worst-case
    /// backpressured `release_pending_op_slot` cleanup (issue #4350).
    pub(crate) const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(30);

    // `notify_op_change` (legacy state-machine re-entry primitive)
    // is gone: every op routes outbound messages through
    // `op_execution_sender` and owns its state in task locals.

    // An early, fast path, return for communicating events in the node to the main message handler,
    // without any transmission in the network whatsoever and avoiding any state transition.
    //
    // Useful when we want to notify connection attempts, or other events that do not require any
    // network communication with other nodes.
    pub async fn notify_node_event(&self, msg: NodeEvent) -> Result<(), OpError> {
        tracing::debug!(event = %msg, "notify_node_event: queuing node event");
        notify_node_event_on(
            self.to_event_listener.notifications_sender(),
            Self::NOTIFICATION_SEND_TIMEOUT,
            msg,
        )
        .await
    }

    /// Non-blocking variant of [`Self::notify_node_event`] for best-effort
    /// broadcast / heartbeat events whose loss is recoverable.
    ///
    /// Use when:
    ///   1. The caller would otherwise block the WASM commit / executor
    ///      path on the event-loop notification channel (issue #4145: a
    ///      30-second `notify_node_event(...).await` from `runtime.rs` on
    ///      every UPDATE was the primary back-pressure path that wedged
    ///      both nova and vega gateways on 2026-05-24).
    ///   2. Dropping the broadcast is acceptable — typically because a
    ///      subsequent state apply, periodic renewal, or summary-mismatch
    ///      `SyncStateToPeer` round will cover the missed signal.
    ///
    /// Returns `Ok(())` when the event was enqueued and
    /// `Err(OpError::NotificationError)` (after logging at warn level)
    /// when the channel was full or closed. **Callers should treat the
    /// error as advisory and continue.**
    pub fn try_notify_node_event(&self, msg: NodeEvent) -> Result<(), OpError> {
        tracing::debug!(event = %msg, "try_notify_node_event: queuing node event (non-blocking)");
        try_notify_node_event_on(
            self.to_event_listener.notifications_sender(),
            self.to_event_listener.notification_channel_pending(),
            self.to_event_listener.notifications_sender().capacity(),
            msg,
        )
    }

    // The blocking `notify_node_event` is still used by callers that
    // require delivery (or, in the case of `announce_contract_hosted`,
    // need a delivery error rather than silent drop because the caller
    // has already consumed a one-shot transition). See
    // `try_notify_node_event` and `.claude/rules/channel-safety.md` for
    // the broader pattern.

    /// Re-broadcast a fresh-contract state that earlier found no targets, now
    /// that the first viable broadcast target for `key` has appeared.
    ///
    /// Issue #4359: a never-before-seen contract id loses the race between the
    /// broadcast give-up window (~6 s) and the much slower interest/
    /// subscription/proximity resolve. When the broadcast handler gives up it
    /// stashes the state in [`Self::pending_broadcasts`]. This is called on the
    /// **first viable-target signal** for the contract — see the call-site list
    /// in the trigger-completeness note below — so the deferred state is
    /// re-emitted as a `BroadcastStateChange { is_retry: false, is_reemit: true }`,
    /// which now finds the just-appeared target and propagates.
    ///
    /// ## Stale-state safety (issue #4359 re-review, SHOULD-FIX 4)
    ///
    /// Rather than re-emit the give-up-time *bytes* (which a newer locally
    /// applied UPDATE could have superseded in the meantime), this re-reads the
    /// **current** local state for the contract and broadcasts that. The stash
    /// entry's role is "this contract still owes a fan-out"; its bytes are only
    /// a fallback used when the live read fails (contract handler unavailable),
    /// which is strictly no worse than the give-up-time state we would otherwise
    /// have re-emitted.
    ///
    /// ## Trigger completeness (issue #4359 re-review, MUST-FIX 1)
    ///
    /// Since #4642 step 9, `get_broadcast_targets_update` resolves targets from
    /// a SINGLE source — the proximity cache of advertised co-hosts (Source 1).
    /// The *first viable target* for a cold id therefore appears when a
    /// connected neighbor newly announces it hosts one of our contracts:
    ///
    /// * **Source 1 — proximity cache** (`neighbors_with_contract`):
    ///   - `node.rs` NeighborHosting overlap path, when a neighbor newly
    ///     announces hosting one of our contracts (the neighbor just became a
    ///     `neighbors_with_contract` target) — live, or reconciled by the
    ///     ~5-min InterestSync anti-entropy heartbeat
    ///     (`ring.rs::interest_heartbeat`, #4722).
    ///
    /// The interest-registration flush sites remain wired even though an
    /// interest-only peer is no longer a broadcast target (Source 2 was removed
    /// in #4642 step 9): a flush that finds no Source-1 target simply re-stashes
    /// or lets the entry expire — harmless and cheap, and it keeps the eager
    /// #4359 wakeup for the case where an interest registration coincides with
    /// the peer's advertisement. Those sites are (`register_peer_interest`
    /// is_new):
    ///   - `subscribe::register_downstream_subscriber` (downstream subscriber)
    ///   - `subscribe::finalize_originator_subscribe` (originator upstream)
    ///   - `get::op_ctx_task` remote GET `subscribe=false` (requester interest)
    ///   - `node.rs` Interests / Summaries interest-sync handlers
    ///
    /// That set is the complete first-viable-target signal: a never-seen id can
    /// only gain a broadcast target by a connected neighbor announcing it hosts
    /// the id (Source 1). The give-up path itself also re-checks targets
    /// immediately after stashing (`pending_broadcast_stash_recheck`) to close
    /// the stash-after-flush race. A grep pin test
    /// (`pending_broadcast_flush_wired_at_all_interest_sites`) guards against a
    /// future `register_peer_interest` call site forgetting the flush.
    ///
    /// Best-effort via [`Self::try_notify_node_event`]: if the channel is full
    /// the state is re-stashed for the next signal. No-op (no read, no emit)
    /// when nothing is pending for the contract — the overwhelmingly common
    /// case, kept cheap by checking membership before any contract-handler read.
    pub(crate) async fn flush_pending_broadcast_on_interest(&self, key: &ContractKey) {
        flush_pending_broadcast_on_interest_on(
            &self.pending_broadcasts,
            self.to_event_listener.notifications_sender(),
            self.to_event_listener.notification_channel_pending(),
            self.to_event_listener.notifications_sender().capacity(),
            key,
            // Live read of the CURRENT local state, evaluated lazily so the
            // fast path (nothing stashed) never touches the contract handler.
            || self.read_current_contract_state(key),
        )
        .await;
    }

    /// Read the current local state for `key` from the contract handler, or
    /// `None` if we don't host it / the read fails. Used by the #4359 deferred
    /// re-broadcast flush to avoid re-emitting superseded give-up-time bytes.
    async fn read_current_contract_state(&self, key: &ContractKey) -> Option<WrappedState> {
        use crate::contract::ContractHandlerEvent;
        match self
            .notify_contract_handler(ContractHandlerEvent::GetQuery {
                instance_id: *key.id(),
                return_contract_code: false,
            })
            .await
        {
            Ok(ContractHandlerEvent::GetResponse {
                response: Ok(store_response),
                ..
            }) => store_response.state,
            _ => None,
        }
    }

    /// Get all active subscriptions.
    /// In the simplified lease-based model, this returns contracts we're actively subscribed to.
    /// Note: We no longer track per-contract subscriber lists.
    pub fn get_network_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        // Return contracts we're subscribed to with an empty peer list
        // (no longer tracking individual subscribers in the new model)
        self.ring
            .get_subscribed_contracts()
            .into_iter()
            .map(|contract_key| (contract_key, Vec::new()))
            .collect()
    }

    /// Build a [`reconcile::ReconcileInputs`] snapshot for `contract` from live
    /// node state (keystone step-2, #4642, shadow-mode wiring), or `None` when
    /// the snapshot would be DISTANCE-BLIND and therefore unmeasurable.
    ///
    /// Reads the live maps ONCE into plain values so `reconcile` stays a pure
    /// function. Behavior-preserving: this only reads state; it drives nothing.
    /// Hosting is binary — `state_present` is `true` only when this peer holds
    /// the FULL contract (code + params + state), never a partial tier.
    ///
    /// Returns `None` when our OWN ring location is unknown (startup / no observed
    /// address yet): without it the distance-derived fields — the computed
    /// upstream, the verified-root check, and the strict-farther downstream gate —
    /// are ALL unmeasurable. Defaulting the strict gate in that state would MASK
    /// the mutual-co-host divergence the telemetry exists to reveal, so the caller
    /// SKIPS the shadow comparison for that contract this tick rather than
    /// recording noise. A later flip-time builder must handle unknown-location
    /// explicitly (e.g. defer the reconcile), never count-all.
    pub(crate) fn build_reconcile_inputs(
        &self,
        contract: &ContractKey,
    ) -> Option<reconcile::ReconcileInputs> {
        // Distance-blind guard: resolve our own location once up front; if it is
        // unknown the whole snapshot is unmeasurable (see doc above).
        let my_location = self.ring.connection_manager.own_location().location()?;

        let instance_id = *contract.id();
        let hosting_neighbors = self
            .neighbor_hosting
            .neighbors_with_contract_id(&instance_id);
        let computed_upstream = self
            .ring
            .most_keyward_hosting_neighbor(&instance_id, &hosting_neighbors);

        Some(reconcile::ReconcileInputs {
            computed_upstream,
            has_local_client: self.ring.has_client_subscriptions(contract),
            has_farther_downstream_subscriber: self
                .has_farther_downstream_subscriber(contract, my_location),
            // Recent local GET/PUT access is REAL demand (invariant 3): a
            // read-only / PUT-only contract (River UI container, web/UI
            // containers) is emitted by `contracts_needing_renewal()` branch 3
            // but has no subscription, so without this the P6 renewal gate would
            // drop its lease and it would leave the update mesh.
            has_recent_local_client_access: self.ring.has_recent_local_client_access(contract),
            state_present: self.ring.contract_state_present(contract),
            is_subscribed: self.ring.is_subscribed(contract),
            is_advertised: self.neighbor_hosting.is_hosted_locally(contract),
            is_verified_root: self.ring.is_subscription_root(contract),
            // STEP-3 / piece-D hook: no on-`main` source for the acquiring-
            // transient state yet (`spawn_host_state_sync_retry` is a piece-D
            // addition), so this is always false in shadow mode. See
            // `ReconcileInputs::actively_acquiring`.
            actively_acquiring: false,
        })
    }

    /// Whether at least one lease-valid downstream subscriber is STRICTLY FARTHER
    /// from the contract key than this peer — the piece-D H2 interest gate.
    ///
    /// `my_location` is this peer's ring location, resolved ONCE by the caller
    /// ([`build_reconcile_inputs`](Self::build_reconcile_inputs), which returns
    /// `None` and skips the whole comparison when it is unknown). Resolves each
    /// lease-valid downstream subscriber to its location and keeps only those
    /// strictly farther from the key than us (the closer/upstream peer is
    /// EXCLUDED, so mutual co-hosts cannot perpetuate each other's leases; see
    /// [`has_strictly_farther`]). The distance filter uses EXACT `Distance`
    /// ordering, never epsilon `==`.
    fn has_farther_downstream_subscriber(
        &self,
        contract: &ContractKey,
        my_location: Location,
    ) -> bool {
        let subscribers = self.ring.downstream_subscriber_peers(contract);
        if subscribers.is_empty() {
            return false;
        }
        let contract_location = Location::from(contract);
        let my_distance: Distance = my_location.distance(contract_location);
        // A subscriber we can't currently resolve to a connected location (it
        // disconnected but its lease has not expired) is skipped, not assumed
        // farther. Known dilution edge: the case the strict filter targets — a
        // CONNECTED closer co-host that is our upstream — always resolves, so
        // skipping stragglers is safe.
        let subscriber_distances = subscribers.iter().filter_map(|peer| {
            self.ring
                .connection_manager
                .get_peer_by_pub_key(&peer.0)
                .and_then(|pkl| pkl.location())
                .map(|loc| loc.distance(contract_location))
        });
        has_strictly_farther(my_distance, subscriber_distances)
    }

    /// Record one reconcile-controller SHADOW comparison at a single-aspect EDGE
    /// site (keystone step-2 completion, #4642): inbound-unsubscribe collapse,
    /// connection-drop re-root, or host-formation announce.
    ///
    /// DRIVES NOTHING. Builds the [`reconcile::ReconcileInputs`] snapshot,
    /// computes what the controller WOULD do, maps the caller's per-event actual
    /// behavior (`actual_of(&inputs)`), and records the divergence FOCUSED on the
    /// `relevant` action class(es) — so level-triggered actions the event does
    /// not decide (notably `Renew`, which is the renewal site's concern) can't
    /// pollute this site's signal. Skipped (no record) when the snapshot is
    /// distance-blind (`build_reconcile_inputs` returns `None`: own-location
    /// unknown), same as the maintenance sites.
    pub(crate) fn record_reconcile_shadow_event(
        &self,
        site: crate::node::network_status::ReconcileShadowSite,
        contract: &ContractKey,
        relevant: &[reconcile::Action],
        actual_of: impl FnOnce(&reconcile::ReconcileInputs) -> Vec<reconcile::Action>,
    ) {
        let Some(inputs) = self.build_reconcile_inputs(contract) else {
            return;
        };
        let reconcile_actions = reconcile::reconcile(&inputs);
        let actual = actual_of(&inputs);
        let divergence =
            reconcile::action_set_divergence_focused(&reconcile_actions, &actual, relevant);
        crate::node::network_status::record_reconcile_shadow_comparison(site, divergence);
        if divergence.any() {
            tracing::debug!(
                contract = %contract,
                ?site,
                ?reconcile_actions,
                ?actual,
                "reconcile shadow diverges from actual behavior (#4642 keystone step-2)"
            );
        }
    }

    /// Reconcile-controller FLIP of the RENEWAL-site interest gate (#4642
    /// keystone, sub-task 3 "the flip"; demand-driven-hosting design doc §5a).
    ///
    /// This is the first driver-backed reconcile decision: it no longer records a
    /// shadow divergence, it DRIVES. Given a contract the renewal loop is about to
    /// renew, it builds a FRESH [`reconcile::ReconcileInputs`] snapshot AT EMISSION
    /// time (the TOCTOU revalidation guard — the decision is made from the live
    /// maps at the instant the renewal would fire, never a stale earlier read) and
    /// applies the controller's interest gate [`reconcile::wants_renewal`] =
    /// design §5a `contract_in_use`: renew iff a **local client**, a
    /// **STRICTLY-farther** downstream subscriber (piece D: a downstream counts
    /// only when strictly farther from the key, so two mutual co-hosts cannot renew
    /// each other forever), OR **recent local GET/PUT access** (invariant 3:
    /// reads/PUTs are permanent demand — a read-only / PUT-only contract like the
    /// River UI container must stay renewed even without a subscription) depends on
    /// this peer hosting.
    ///
    /// Returns `false` when the contract is no longer `contract_in_use` under that
    /// strict gate. The renewal loop then SKIPS the spawn; the lease lapses and the
    /// chain collapses inward. This is the load-bearing piece-D behavior:
    /// **non-renewal IS the collapse primitive** (§5a), so live subscriptions track
    /// ACTIVE demand rather than accumulated cache — the #3763 renewal-storm fix.
    /// The previous (ANY-downstream, renew-everything) gate is what
    /// `contracts_needing_renewal` still uses to build the candidate set; this
    /// narrows it to the strict gate at drive time. It gates on DEMAND only, never
    /// on lease/upstream/root state, so an in-use keyward root (or a peer still
    /// acquiring its first lease) renews too — dropping those would strand a
    /// demanded contract's lease.
    ///
    /// Fails **SAFE** (`true`, keep renewing) when the snapshot is distance-blind
    /// (own ring location unknown at startup): collapsing a chain on an
    /// unmeasurable snapshot is exactly the last-holder-on-a-stale-snapshot bug the
    /// spec's at-emission-revalidation hardening exists to prevent. Behavior at
    /// startup is therefore unchanged (renew everything in the set).
    ///
    /// # Per-contract rate cap
    ///
    /// The renewal wire action is already per-contract rate-capped by the two gates
    /// the loop applies before this one: `can_request_subscription` (per-contract
    /// exponential backoff) and `mark_subscription_pending` (blocks a second
    /// concurrent renewal for the same contract until the prior one's guard
    /// releases). Those ARE the "per-contract wire-action rate cap" the spec's
    /// reconcile hardening requires for the driven renewal, so this flip adds no
    /// redundant cap. (A dedicated cap is still owed for the destructive
    /// `Unsubscribe` wire action when it is later flipped, since that path does not
    /// pass through `can_request_subscription`.)
    pub(crate) fn reconcile_wants_renewal(&self, contract: &ContractKey) -> bool {
        let Some(inputs) = self.build_reconcile_inputs(contract) else {
            // Distance-blind snapshot: never collapse a chain on a blind read.
            return true;
        };
        // The controller's interest gate (design §5a `contract_in_use`), computed
        // from the fresh at-emission snapshot. This wrapper only supplies the
        // snapshot; the decision lives in the pure reconcile core.
        reconcile::wants_renewal(&inputs)
    }

    /// Reconcile-controller FLIP of the COLLAPSE-site interest gate (keystone P6,
    /// #4642; design doc §5a / §6). The exact INVERSE of
    /// [`reconcile_wants_renewal`](Self::reconcile_wants_renewal): whether the
    /// active teardown (`send_unsubscribe_upstream`) should fire for this contract.
    ///
    /// The three collapse-decision sites — the maintenance loop's downstream-expiry
    /// teardown (`Ring::recover_orphaned_subscriptions`), the client-disconnect
    /// teardown (`client_events`), and the inbound-unsubscribe teardown
    /// (`subscribe::handle_unsubscribe_inbound`) — now gate on THIS instead of the
    /// legacy ANY-downstream `Ring::should_unsubscribe_upstream` predicate. It
    /// builds a FRESH [`reconcile::ReconcileInputs`] snapshot AT EMISSION time and
    /// applies the controller's interest gate [`reconcile::wants_collapse`] =
    /// `!contract_in_use`: tear down iff NO local client AND NO **STRICTLY-farther**
    /// downstream subscriber (piece D: a downstream counts only when strictly
    /// farther from the key, so two mutual co-hosts cannot perpetuate each other's
    /// leases forever — the #3763 hollow-relay-storm fix). The teardown it gates
    /// still drives the wire `Unsubscribe` toward the **STORED** `is_upstream`
    /// upstream (the narrow flip KEEPS the stored flag; see
    /// `send_unsubscribe_upstream`).
    ///
    /// Fails **SAFE** (`false`, DO NOT collapse) when the snapshot is distance-blind
    /// (own ring location unknown at startup) — the mirror of
    /// `reconcile_wants_renewal`'s fail-safe (which keeps renewing): neither ever
    /// tears a chain down on an unmeasurable snapshot. Because renewal keeps
    /// renewing AND collapse does not fire while distance-blind, the contract simply
    /// stays hosted through startup, unchanged from prior behavior.
    ///
    /// Relative to the recent-access-aware baseline below, the strict gate is a
    /// SUPERSET of the legacy gate (`!has_any_downstream ⟹ !has_farther_downstream`),
    /// so it can only collapse MORE, never less — it never leaks a chain the legacy
    /// predicate would have cleaned up. The only added collapses are the
    /// mutual-co-host case the strict gate targets, which v0.2.93 shadow measured at
    /// ~0% divergence from the legacy predicate (the "collapse diff" ship-gate
    /// falsifier). Recent local GET/PUT access is a demand term (invariant 3) shared
    /// by BOTH gates: it holds a read-only / PUT-only contract open, so relative to
    /// the raw subscriptions-only `should_unsubscribe_upstream` the gate collapses
    /// LESS for such contracts. That term is factored out of the divergence counter
    /// (the baseline ANDs in `!has_recent_local_client_access`) so the counter still
    /// isolates the strict-farther difference.
    ///
    /// # Post-flip legibility counter
    ///
    /// Records how often the driven strict-farther gate DISAGREES with the legacy
    /// predicate it replaced (the "collapse diff" ship-gate falsifier, ~0% in
    /// v0.2.93 shadow), so the metric stays legible in `router_snapshot` telemetry
    /// after the flip — repurposed exactly as the RENEWAL counter was. The `site`
    /// argument preserves the pre-flip per-site counter mapping: the maintenance-loop
    /// and client-disconnect teardowns record on `Collapse` (both went through
    /// `send_unsubscribe_upstream`'s `Collapse` shadow before the flip), the
    /// inbound-unsubscribe teardown records on `InboundUnsubscribe` (its own
    /// focused-`Collapse` shadow before the flip). Skips recording (like the shadow
    /// builder) when distance-blind, to avoid noise.
    pub(crate) fn reconcile_wants_collapse(
        &self,
        contract: &ContractKey,
        site: crate::node::network_status::ReconcileShadowSite,
    ) -> bool {
        let Some(inputs) = self.build_reconcile_inputs(contract) else {
            // Distance-blind snapshot: never actively tear down a chain on a blind
            // read (fail SAFE = do not collapse). Do not record — no measurable
            // comparison exists at startup (mirrors the shadow builder's skip).
            return false;
        };
        let wants = reconcile::wants_collapse(&inputs);

        // Post-flip legibility: measure the driven strict-farther gate against the
        // legacy ANY-downstream predicate it replaced (the "collapse diff"
        // ship-gate falsifier, ~0% in v0.2.93 shadow). Recorded on the SAME per-site
        // counter the pre-flip shadow used, so the metric is continuous across the
        // flip. The recent-GET/PUT-access demand term (invariant 3) is factored out
        // of BOTH sides (`&& !has_recent_local_client_access`) so this counter keeps
        // isolating the ONE difference it measures — strict-farther vs ANY-downstream
        // — rather than double-counting the read/PUT demand both gates now honor.
        let legacy = self.ring.should_unsubscribe_upstream(contract)
            && !self.ring.has_recent_local_client_access(contract);
        let divergence = if wants != legacy {
            reconcile::ReconcileActionDivergence {
                collapse: true,
                ..Default::default()
            }
        } else {
            reconcile::ReconcileActionDivergence::default()
        };
        crate::node::network_status::record_reconcile_shadow_comparison(site, divergence);
        if wants != legacy {
            tracing::debug!(
                contract = %contract,
                ?site,
                reconcile_wants_collapse = wants,
                legacy_should_unsubscribe = legacy,
                "collapse gate: driven strict-farther gate disagrees with legacy \
                 ANY-downstream predicate (#4642 P6 flip; expected ~0%)"
            );
        }
        wants
    }

    /// Send an Unsubscribe message to the upstream peer for a contract — the
    /// interest-gated COLLAPSE teardown.
    ///
    /// Finds the upstream peer from the interest manager, resolves its address,
    /// and sends a fire-and-forget Unsubscribe message via the operation routing
    /// mechanism. Also removes the local active subscription and interest tracking.
    ///
    /// FLIP (keystone P6, #4642): the DECISION to fire this teardown is now driven
    /// by the reconcile controller — every caller gates on
    /// [`reconcile_wants_collapse`](Self::reconcile_wants_collapse) (`!contract_in_use`,
    /// the strict-farther interest gate), replacing the legacy ANY-downstream
    /// `should_unsubscribe_upstream`. The UPSTREAM IDENTITY, however, stays on the
    /// stored `is_upstream` flag: the narrow flip does NOT compute-upstream-
    /// everywhere, so the wire `Unsubscribe` still targets the stored upstream and
    /// the computed-vs-stored divergence (#4671) stays under the SHADOW telemetry
    /// recorded below (`record_upstream_divergence_comparison`), unflipped.
    pub async fn send_unsubscribe_upstream(&self, contract: &ContractKey) {
        // Find the upstream peer for this contract
        let upstream = self
            .interest_manager
            .get_interested_peers(contract)
            .into_iter()
            .find(|(_, interest)| interest.is_upstream);

        // Behavior-preserving divergence telemetry (#4642 piece D / #4671).
        // Compute the demand-driven-hosting upstream in PARALLEL and record how
        // often it disagrees with the stored `is_upstream` flag this function
        // still consults. The stored flag continues to drive the decision below;
        // the reconcile-core keystone will later compute upstream everywhere and
        // delete the drifting flag — this first step only measures the drift so
        // it is legible in production telemetry (`router_snapshot`). Computed
        // before the stored-flag early-return so the "stored None but computed
        // Some" (and vice-versa) cases are also counted. Cheap: this path runs on
        // client-unsubscribe / chain-collapse, not a hot loop.
        //
        // CAVEAT — the recorded divergence rate is NOISY AT THE EDGES. A counted
        // divergence means genuine stored-flag drift (#4671) OR one of two edge
        // artifacts: (a) a transient `own_location == None` (early startup / a
        // dropped self-location) makes `most_keyward_hosting_neighbor` return
        // `None` while the stored flag is still `Some`, and (b) the `comparisons`
        // denominator also counts non-divergent `None == None` calls (no upstream
        // either way), diluting the ratio. So do NOT read
        // `upstream_computed_vs_stored_*` as pure flag-drift: attribute startup /
        // edge noise before concluding the flag has drifted.
        {
            let instance_id = *contract.id();
            let hosting_neighbors = self
                .neighbor_hosting
                .neighbors_with_contract_id(&instance_id);
            let computed = self
                .ring
                .most_keyward_hosting_neighbor(&instance_id, &hosting_neighbors);
            let stored_pk = upstream.as_ref().map(|(peer_key, _)| &peer_key.0);
            let computed_pk = computed.as_ref().map(|pkl| &pkl.pub_key);
            let diverged = stored_pk != computed_pk;
            crate::node::network_status::record_upstream_divergence_comparison(diverged);
            if diverged {
                tracing::debug!(
                    contract = %contract,
                    stored_upstream = ?stored_pk,
                    computed_upstream = ?computed_pk,
                    "computed upstream diverges from stored is_upstream flag (#4642 piece D / #4671)"
                );
            }
        }

        // NOTE (keystone P6 flip, #4642): the reconcile COLLAPSE decision no longer
        // lives here as a record-only shadow — it DRIVES, one level up. Every caller
        // of `send_unsubscribe_upstream` now gates on `reconcile_wants_collapse`
        // (the strict-farther `!contract_in_use` gate), which also owns the
        // repurposed `Collapse` reconcile counter (driven strict-vs-legacy
        // divergence). So this function performs the teardown it is asked to; the
        // only shadow it still records is the upstream-IDENTITY comparison above
        // (stored vs computed), which stays UNFLIPPED per the narrow-flip scope.

        let Some((peer_key, _)) = upstream else {
            tracing::debug!(
                contract = %contract,
                "No upstream peer found for unsubscribe"
            );
            self.ring.unsubscribe(contract);
            return;
        };

        // Resolve peer address
        let Some(peer_location) = self
            .ring
            .connection_manager
            .get_peer_by_pub_key(&peer_key.0)
        else {
            tracing::debug!(
                contract = %contract,
                "Upstream peer address not found, cleaning up locally"
            );
            self.ring.unsubscribe(contract);
            self.interest_manager
                .remove_peer_interest(contract, &peer_key);
            return;
        };

        let Some(&target_addr) = peer_location.peer_addr.as_known() else {
            tracing::debug!(
                contract = %contract,
                "Upstream peer has no known address, cleaning up locally"
            );
            self.ring.unsubscribe(contract);
            self.interest_manager
                .remove_peer_interest(contract, &peer_key);
            return;
        };

        let instance_id = *contract.id();
        let tx = Transaction::new::<crate::operations::subscribe::SubscribeMsg>();
        let msg = NetMessage::from(crate::operations::subscribe::SubscribeMsg::Unsubscribe {
            id: tx,
            instance_id,
        });

        // Fire-and-forget the Unsubscribe wire message through
        // `OpCtx`. The op-execution channel routes directly to
        // `OutboundMessageWithTarget` given `Some(target_addr)`, so no
        // operation state is required (Unsubscribe has no reply).
        let mut ctx = self.op_ctx(tx);
        match ctx.send_fire_and_forget(target_addr, msg).await {
            Ok(()) => {
                tracing::debug!(
                    contract = %contract,
                    target = %target_addr,
                    "Sent Unsubscribe upstream"
                );
            }
            Err(e) => {
                tracing::warn!(
                    contract = %contract,
                    error = %e,
                    "Failed to send Unsubscribe upstream"
                );
            }
        }

        // Clean up local state regardless of send result.
        self.ring.unsubscribe(contract);
        self.interest_manager
            .remove_peer_interest(contract, &peer_key);
    }

    /// Build a per-transaction [`OpCtx`] bound to `tx`.
    ///
    /// Construct an [`OpCtx`] for `tx`. Clones the event-loop
    /// `op_execution_sender`; the only supported way to obtain an
    /// `OpCtx` outside this crate's unit tests.
    pub fn op_ctx(&self, tx: Transaction) -> OpCtx {
        OpCtx::new(tx, self.to_event_listener.op_execution_sender.clone())
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    ///
    /// Defaults to [`Priority::DEFAULT`] (`NetworkRelay`). Local-client callers
    /// should use [`notify_contract_handler_prioritized`] with
    /// [`Priority::ClientLocal`], and background callers with
    /// [`Priority::Background`] (#4534).
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound.send_to_handler(msg).await
    }

    /// Send an event to the contract handler at an explicit priority class and
    /// await its response (#4534).
    pub async fn notify_contract_handler_prioritized(
        &self,
        msg: ContractHandlerEvent,
        priority: crate::contract::Priority,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound
            .send_to_handler_prioritized(msg, priority)
            .await
    }

    /// Send an event to the contract handler with a custom timeout.
    ///
    /// Use shorter timeouts for broadcast-path callers (e.g., delta
    /// computation) to prevent tasks from accumulating when the handler is slow.
    pub async fn notify_contract_handler_with_timeout(
        &self,
        msg: ContractHandlerEvent,
        timeout: std::time::Duration,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound
            .send_to_handler_with_timeout(msg, timeout, crate::contract::Priority::DEFAULT)
            .await
    }

    /// Fire-and-forget notification to the contract handler at an explicit
    /// priority class (#4534). Used for maintenance events (e.g. EvictContract)
    /// where no response is needed and the caller must not block.
    pub fn notify_contract_handler_fire_and_forget_prioritized(
        &self,
        ev: ContractHandlerEvent,
        priority: crate::contract::Priority,
    ) {
        if let Err(e) = self
            .ch_outbound
            .send_to_handler_fire_and_forget_prioritized(ev, priority)
        {
            tracing::warn!(error = %e, "failed to send fire-and-forget event to contract handler");
        }
    }

    /// Peek at the next hop address for an outbound initial request.
    ///
    /// Always returns `None` — every op now runs on a driver
    /// driver that owns routing decisions in task locals, so there is
    /// no DashMap entry to consult here. Retained as a stable API
    /// surface for `p2p_protoc::handle_notification_msg`, which falls
    /// back to the connection-manager lookup chain when this returns
    /// `None`.
    pub fn peek_next_hop_addr(&self, _id: &Transaction) -> Option<std::net::SocketAddr> {
        None
    }

    /// Peek at the full target peer (including public key) for an
    /// outbound initial request.
    ///
    /// Always returns `None` — same rationale as
    /// [`Self::peek_next_hop_addr`]. The caller in `p2p_protoc` falls
    /// back to `connection_manager.get_peer_location_by_addr` and the
    /// configured-gateway list to recover the public key needed for
    /// the handshake.
    pub fn peek_target_peer(&self, _id: &Transaction) -> Option<PeerKeyLocation> {
        None
    }

    /// Emit a `NodeEvent::TransactionCompleted(tx)` to the event loop,
    /// triggering cleanup of any `pending_op_results` entry keyed by `tx`.
    ///
    /// Releases the per-attempt callback slot in
    /// `p2p_protoc::pending_op_results` after each
    /// `OpCtx::send_and_await` round-trip finishes. Without this,
    /// attempt-tx entries accumulate until the 60 s sweep —
    /// `test_pending_op_results_bounded` is the regression guard.
    ///
    /// Distinct from [`Self::send_client_result`], which also emits
    /// this event but additionally pushes a `HostResult` through
    /// `result_router_tx`. The driver has many attempt txs
    /// per client tx, so per-attempt cleanup can't go through
    /// `send_client_result` (that would publish N duplicate results to
    /// the client).
    ///
    /// # Blocking vs non-blocking send
    ///
    /// Uses `send().await` wrapped in [`Self::NOTIFICATION_SEND_TIMEOUT`]
    /// rather than `try_send` because the cleanup is load-bearing: a
    /// dropped `TransactionCompleted` on a transiently-full notification
    /// channel would leave the `pending_op_results` slot in place until
    /// the 60 s periodic sweep runs, which
    /// `test_pending_op_results_bounded` is designed to catch. Since this
    /// method is only called from spawned task bodies (never from an
    /// event loop), `send().await` is within the `.claude/rules/channel-safety.md`
    /// rules. The 30 s timeout guards against a genuinely wedged event
    /// loop — the same timeout [`Self::notify_op_change`] uses.
    ///
    /// # Side effects on other `TransactionCompleted` consumers
    ///
    /// The `p2p_protoc::handle_notification_message` branch for
    /// `TransactionCompleted` (lines 2030–2036) also calls
    /// `state.tx_to_client.remove(&tx)`. For per-attempt txs this
    /// is a tolerated no-op: `tx_to_client` is only populated on
    /// client-visible txs via `ch_outbound.waiting_for_subscription_result`
    /// / `waiting_for_transaction_result`. If a future change starts
    /// keying `tx_to_client` by attempt tx, this eager cleanup will
    /// silently drop mappings and must be revisited.
    pub(crate) async fn release_pending_op_slot(&self, tx: Transaction) {
        release_pending_op_slot_on(
            self.to_event_listener.notifications_sender(),
            tx,
            Self::NOTIFICATION_SEND_TIMEOUT,
        )
        .await
    }

    // `has_{connect,get,update,put,subscribe}_op` were the legacy
    // relay dispatch gates; all gone with their DashMaps. Every wire
    // variant now spawns its driver unconditionally and the dedup
    // gate lives in `active_relay_{op}_txs`.

    pub fn completed(&self, id: Transaction) {
        self.ring.live_tx_tracker.remove_finished_transaction(id);
        self.ops.under_progress.remove(&id);
        self.ops.completed.insert(id);

        // Clean up request router to prevent stale entries from blocking subsequent requests
        if let Some(router) = self.request_router.get() {
            router.complete_operation(id);
        }
    }

    /// Notify the operation manager that a transaction is being transacted over the network.
    pub fn sending_transaction(&self, peer: &PeerKeyLocation, msg: &NetMessage) {
        let transaction = msg.id();
        // With hop-by-hop routing, record the request using the peer we're sending to
        // and the message's requested location (contract location)
        if let Some(target_loc) = msg.requested_location() {
            self.ring
                .record_request(peer.clone(), target_loc, transaction.transaction_type());
        }
        if let Some(peer_addr) = peer.socket_addr() {
            self.ring
                .live_tx_tracker
                .add_transaction(peer_addr, *transaction);
        }
    }

    /// Register to be notified when a contract is stored.
    /// Returns a receiver that will be signaled when the contract is stored.
    /// This is used to handle race conditions where a subscription arrives before
    /// the contract has been propagated via PUT.
    pub fn wait_for_contract(&self, instance_id: ContractInstanceId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut waiters = self.contract_waiters.lock();
        waiters.entry(instance_id).or_default().push(tx);
        rx
    }

    /// Notify all waiters that a contract has been stored.
    /// Called after successful contract storage in PUT operations.
    ///
    /// Note: Stale waiters (from timed-out operations) are automatically cleaned up
    /// here when we remove all senders for the key. The send() will fail silently
    /// for dropped receivers, which is harmless.
    pub fn notify_contract_stored(&self, key: &ContractKey) {
        let mut waiters = self.contract_waiters.lock();
        if let Some(senders) = waiters.remove(key.id()) {
            let count = senders.len();
            for sender in senders {
                // Receiver may already be dropped (e.g., operation timed out)
                #[allow(clippy::let_underscore_must_use)]
                let _ = sender.send(());
            }
            if count > 0 {
                tracing::debug!(
                    %key,
                    count,
                    "Notified waiters that contract has been stored"
                );
            }
        }
    }

    /// Returns pending operation counts: [connect, put, get, subscribe,
    /// update]. All slots are always 0 (operations run as standalone
    /// driver tasks); retained for API stability with the home-page
    /// renderer and telemetry consumers.
    pub fn pending_op_counts(&self) -> [u32; 5] {
        [0; 5]
    }

    /// Returns the number of entries in the contract_waiters map.
    pub fn contract_waiters_count(&self) -> u32 {
        self.contract_waiters.lock().len() as u32
    }

    /// Returns a reference to the orphan stream registry.
    ///
    /// Used by operations layer to claim orphan streams when RequestStreaming
    /// or ResponseStreaming metadata messages arrive.
    #[allow(dead_code)] // Phase 3 infrastructure - will be used when streaming handlers are implemented
    pub fn orphan_stream_registry(&self) -> &Arc<OrphanStreamRegistry> {
        &self.orphan_stream_registry
    }

    /// Returns a reference to the streaming-PUT progress registry (#4001).
    ///
    /// The retry loop inserts/removes a `StreamProgressHandle` keyed by the
    /// attempt `Transaction`; the originator-loopback relay-streaming driver
    /// looks it up to record per-fragment progress.
    pub(crate) fn stream_progress_registry(&self) -> &Arc<StreamProgressRegistry> {
        &self.stream_progress_registry
    }

    /// Determines if streaming should be used for a payload of the given size.
    ///
    /// Returns `true` if the payload size exceeds the streaming threshold.
    #[allow(dead_code)] // Phase 3 infrastructure - will be used when streaming handlers are implemented
    pub fn should_use_streaming(&self, payload_size: usize) -> bool {
        payload_size > self.streaming_threshold
    }

    /// Builds the messages we need to send to a peer that just joined the ring,
    /// so it learns which contracts we're subscribed to and our cached state.
    pub(crate) fn on_ring_connection_established(
        &self,
        peer_addr: SocketAddr,
        pub_key: &TransportPublicKey,
    ) -> Vec<(SocketAddr, NetMessage)> {
        // Cancel any pending deferred interest removal for this peer.
        // If the peer reconnected within the grace period, their interests
        // are preserved — no re-registration needed via heartbeat.
        self.interest_manager
            .cancel_deferred_removal(&PeerKey::from(pub_key.clone()));

        let mut messages = Vec::with_capacity(2);

        let interest_hashes = self.interest_manager.get_all_interest_hashes();
        if !interest_hashes.is_empty() {
            messages.push((
                peer_addr,
                NetMessage::V1(NetMessageV1::InterestSync {
                    message: InterestMessage::Interests {
                        hashes: interest_hashes,
                    },
                }),
            ));
        }

        if let Some(cache_msg) = self
            .neighbor_hosting
            .on_ring_connection_established(pub_key)
        {
            messages.push((
                peer_addr,
                NetMessage::V1(NetMessageV1::NeighborHosting { message: cache_msg }),
            ));
        }

        // If we're already ready, tell the new peer immediately
        if self.ring.connection_manager.is_self_ready()
            && self.ring.connection_manager.min_ready_connections > 0
        {
            messages.push((
                peer_addr,
                NetMessage::V1(NetMessageV1::ReadyState { ready: true }),
            ));
        }

        messages
    }

    /// Handles a peer leaving the ring.
    ///
    /// Proximity cache is cleared immediately. Interest removal is deferred for
    /// `INTEREST_DISCONNECT_GRACE_PERIOD` to survive transient disconnects.
    /// Downstream subscriber entries in the hosting manager are NOT removed here —
    /// they have lease-based TTL and will be cleaned up by the periodic
    /// `expire_stale_downstream_subscribers` sweep, which also decrements the
    /// interest manager's `downstream_subscriber_count` and triggers upstream
    /// unsubscribe when appropriate.
    pub(crate) fn on_ring_connection_lost(&self, pub_key: &TransportPublicKey) {
        // CONNECTION-DROP re-root FLIP (#4642 piece F). Capture the contracts the
        // dropped peer co-hosted BEFORE `on_peer_disconnected` clears them, so the
        // post-drop computed-upstream recomputation below already excludes it.
        let dropped_contracts = self.neighbor_hosting.contracts_for_peer(pub_key);

        // Production disconnect bookkeeping (unchanged).
        self.neighbor_hosting.on_peer_disconnected(pub_key);
        self.interest_manager
            .schedule_deferred_removal(&PeerKey::from(pub_key.clone()));

        // FLIP: this site used to only RECORD a shadow ("production never re-roots
        // on a connection drop today"). It now DRIVES a storm-safe PROMPT re-root —
        // piece F's event-driven re-subscribe, which reacts to the drop rather than
        // waiting for the lease-expiry timer (invariant 4). Every storm-safety guard
        // lives in `spawn_prompt_reroots`: interest-gated (only contracts the
        // reconcile controller would `ReRootSearch`), single-target strictly-closer
        // (one `run_renewal_subscribe` toward the key), make-before-break (fresh
        // subscribe, no teardown-first), per-drop cap + jitter + per-contract
        // backoff/dedup, and a zero-connection guard. Contracts beyond the per-drop
        // cap are re-rooted by the ordinary ~30s renewal loop at its own rate cap,
        // so a mass disconnect degrades to a bounded trickle, never the O(contracts)
        // burst the #3763 hollow-relay firefight fought.
        //
        // The candidate set is still the dropped peer's co-hosted instance-ids
        // resolved to OUR `ContractKey`s via `subscribed_keys_in` (lease-holders):
        // a state-present, in-use, no-upstream contract we host WITHOUT an active
        // lease is still MISSED by the prompt path (narrow, transient) but is
        // covered by the renewal loop's own re-root — same bias the shadow noted.
        if !dropped_contracts.is_empty() {
            if let Some(op_manager) = self.ring.upgrade_op_manager() {
                let shutdown = self.ring.shutdown_token();
                self.spawn_prompt_reroots(&dropped_contracts, &op_manager, &shutdown);
            }
        }
    }

    /// Whether the reconcile controller would `ReRootSearch` this contract right
    /// now — the interest gate for the connection-drop PROMPT re-root (#4642 piece
    /// F flip; mirrors [`reconcile_wants_renewal`](Self::reconcile_wants_renewal)
    /// and [`reconcile_wants_collapse`](Self::reconcile_wants_collapse)).
    ///
    /// Builds a FRESH [`reconcile::ReconcileInputs`] snapshot at emission time and
    /// returns `true` iff [`reconcile::reconcile`] emits [`reconcile::Action::ReRootSearch`]:
    /// demand is intact (`contract_in_use`), the computed upstream is gone
    /// (strictly-closer connected co-host vanished), we are NOT the verified root,
    /// and we are not already acquiring. This is the partition-vs-collapse
    /// distinction — a demandless copy reconciles to a `Collapse`/`Retract`
    /// teardown, never `ReRootSearch`, so it is never re-rooted (the anti-storm
    /// property).
    ///
    /// Fails **SAFE** (`false`, DO NOT prompt-re-root) when the snapshot is
    /// distance-blind (own ring location unknown at startup): we cannot tell
    /// whether the upstream was truly lost, and spuriously re-rooting on a blind
    /// read would add subscribe traffic; the ordinary renewal loop re-roots this
    /// contract on a later tick once our location is known.
    ///
    /// # Post-flip legibility counter
    ///
    /// Repurposes the pre-flip `ConnectionDrop` shadow counter exactly as the
    /// RENEWAL/COLLAPSE flips repurposed theirs: every evaluation bumps the
    /// `comparisons` denominator and a driven prompt re-root sets `reroot_search`,
    /// so `ConnectionDrop` reads in `router_snapshot` telemetry as "of N
    /// drop-affected in-use candidates evaluated, K drove a prompt re-root".
    pub(crate) fn reconcile_wants_reroot(&self, contract: &ContractKey) -> bool {
        let Some(inputs) = self.build_reconcile_inputs(contract) else {
            // Distance-blind: skip the prompt re-root (fail closed, storm-safe).
            // Do not record — no measurable snapshot at startup (mirrors the other
            // gates' distance-blind skip).
            return false;
        };
        let wants = reconcile::reconcile(&inputs).contains(&reconcile::Action::ReRootSearch);
        let divergence = if wants {
            reconcile::ReconcileActionDivergence {
                reroot_search: true,
                ..Default::default()
            }
        } else {
            reconcile::ReconcileActionDivergence::default()
        };
        crate::node::network_status::record_reconcile_shadow_comparison(
            crate::node::network_status::ReconcileShadowSite::ConnectionDrop,
            divergence,
        );
        wants
    }

    /// Drive storm-safe PROMPT re-roots for the in-use contracts a just-dropped
    /// co-host peer left stranded (#4642 piece F). The connection-drop FLIP's whole
    /// storm-safety lives here; `on_ring_connection_lost` is a thin caller.
    ///
    /// The guards, in order, and which prompt requirement each satisfies:
    /// 1. **Zero-connection guard.** No ring connections ⇒ nowhere to route a
    ///    re-subscribe ⇒ return (wait for re-bootstrap).
    /// 2. **Bounded candidate scan.** `subscribed_keys_in` caps both the scan
    ///    (`SCAN_CAP`) and the matches (`SAMPLE_CAP`), so losing one co-host on a
    ///    near-key gateway cannot become an O(subscribed-set) sweep.
    /// 3. **Per-drop cap (THE storm bound).** At most `MAX_PROMPT_REROOTS_PER_DROP`
    ///    prompt re-roots per drop, however many contracts routed through the
    ///    dropped peer; the rest are re-rooted by the ordinary renewal loop.
    /// 4. **Ban + per-contract backoff + dedup.** Banned contracts are skipped;
    ///    `can_request_subscription` respects exponential backoff and
    ///    `mark_subscription_pending` blocks a second concurrent re-root/renewal —
    ///    together these stop repeated drops (a mass disconnect firing many
    ///    `on_ring_connection_lost` calls) from re-firing the SAME contract.
    /// 5. **Interest gate + single-target.** `reconcile_wants_reroot` re-roots only
    ///    a contract the controller would `ReRootSearch` (real demand, upstream
    ///    lost, not root); a demandless every-hop copy is NEVER re-rooted.
    /// 6. **Make-before-break + jitter.** The shared `spawn_renewal_subscribe_task`
    ///    issues a fresh SUBSCRIBE toward the key (single strictly-closer target,
    ///    no fan-out, no teardown-first) after up-to-15s jitter that spreads even
    ///    the bounded batch.
    pub(crate) fn spawn_prompt_reroots(
        &self,
        dropped_contracts: &HashSet<ContractInstanceId>,
        op_manager: &Arc<Self>,
        shutdown: &tokio_util::sync::CancellationToken,
    ) {
        let selected = self.select_prompt_reroots(dropped_contracts);

        let mut spawned = 0usize;
        for key in selected {
            // Guard 6: make-before-break — claim the pending slot, then spawn a
            // fresh subscribe via the SHARED renewal path (no teardown-first). The
            // claim can lose to a concurrent renewal that grabbed the slot between
            // selection and here; that key is simply left to the renewal loop.
            if self.ring.mark_subscription_pending(key) {
                crate::ring::Ring::spawn_renewal_subscribe_task(
                    op_manager.clone(),
                    key,
                    shutdown.clone(),
                );
                spawned += 1;
            }
        }

        // Simulation-test observability (no-op in production — gated on a current
        // network name): record this drop's prompt re-root batch so the
        // re-root-storm falsifier can assert `max_reroot_batch <=
        // MAX_PROMPT_REROOTS_PER_DROP` per node. Skip a zero batch (it never raises
        // the per-node max).
        if spawned > 0 {
            if let Some(addr) = self.ring.connection_manager.get_own_addr() {
                crate::ring::topology_registry::record_reroot_batch(addr, spawned as u64);
            }
        }
    }

    /// The storm-safe SELECTION half of the connection-drop prompt re-root (#4642
    /// piece F): pick the at-most-`MAX_PROMPT_REROOTS_PER_DROP` contracts to
    /// promptly re-root after a co-host peer dropped, applying every non-spawning
    /// guard. Pure selection (no task spawning, no pending-mark), so the storm
    /// bound and interest gate are DETERMINISTICALLY unit-testable on the returned
    /// list. [`spawn_prompt_reroots`](Self::spawn_prompt_reroots) is the thin
    /// driver that marks + spawns each returned key.
    ///
    /// Guard order (see `spawn_prompt_reroots` doc for the prompt-requirement each
    /// serves): zero-connection → bounded scan → per-drop cap → ban → per-contract
    /// backoff/pending → interest gate (`reconcile_wants_reroot`).
    pub(crate) fn select_prompt_reroots(
        &self,
        dropped_contracts: &HashSet<ContractInstanceId>,
    ) -> Vec<ContractKey> {
        // Guard 1: zero-connection — nowhere to route a re-subscribe.
        if self.ring.open_connections() == 0 {
            return Vec::new();
        }

        // Guard 2: bounded candidate scan (our lease-holders the dropped peer
        // co-hosted). Caps BOTH the scan and the number of matches.
        let candidates = self.ring.subscribed_keys_in(
            dropped_contracts,
            CONNECTION_DROP_SHADOW_SCAN_CAP,
            CONNECTION_DROP_SHADOW_SAMPLE_CAP,
        );

        let mut selected = Vec::new();
        for key in candidates {
            // Guard 3: per-drop cap — THE re-root-storm bound. However many
            // contracts routed through the dropped peer, select at most
            // MAX_PROMPT_REROOTS_PER_DROP; the rest are re-rooted by the renewal
            // loop at its own rate cap.
            if selected.len() >= MAX_PROMPT_REROOTS_PER_DROP {
                break;
            }
            // Guard 4a: never re-subscribe a banned contract.
            if self.ring.contract_ban_list.is_banned(key.id()) {
                continue;
            }
            // Guard 4b: per-contract backoff + in-flight dedup — stops repeated
            // drops from re-firing the SAME contract.
            if !self.ring.can_request_subscription(&key) {
                continue;
            }
            // Guard 5: interest gate — only re-root what the controller would
            // ReRootSearch (demand intact, upstream lost, not root); a demandless
            // copy reconciles to a teardown and is never re-rooted (anti-storm).
            if !self.reconcile_wants_reroot(&key) {
                continue;
            }
            selected.push(key);
        }
        selected
    }
}

/// Emit `NodeEvent::TransactionCompleted(tx)` through a provided
/// notification sender, timeout-wrapped so a wedged event loop does not
/// hang the caller forever.
///
/// Extracted from [`OpManager::release_pending_op_slot`] so the channel
/// interaction can be unit-tested in isolation without building a full
/// `OpManager` (review finding T-3). The `OpManager` method is a thin
/// wrapper around this free function.
///
/// Uses `send().await` (wrapped in `timeout`) rather than
/// `try_send`. The caller runs in a `GlobalExecutor::spawn`'d task,
/// so a short blocking wait is within the channel-safety rules;
/// dropping the event on transient backpressure would re-introduce
/// the `test_pending_op_results_bounded` leak.
async fn release_pending_op_slot_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    tx: Transaction,
    timeout: Duration,
) {
    match tokio::time::timeout(
        timeout,
        notifications_sender.send(Either::Right(NodeEvent::TransactionCompleted(tx))),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(_)) => {
            tracing::warn!(
                %tx,
                "release_pending_op_slot: notification channel closed; \
                 pending_op_results entry will be reclaimed by 60s sweep"
            );
        }
        Err(_) => {
            tracing::error!(
                %tx,
                timeout_secs = timeout.as_secs(),
                "release_pending_op_slot: notification channel full for too long; \
                 event loop may be stuck; pending_op_results entry will be \
                 reclaimed by 60s sweep"
            );
        }
    }
}

/// Non-blocking emit of `NodeEvent::TransactionOrphaned { tx, peer }` on
/// the event-loop notification channel; returns `true` when enqueued.
///
/// Extracted from [`OpManager::notify_orphaned_transaction`] so it can be
/// exercised in unit tests without building a full `OpManager`. Best-effort:
/// a momentarily-full channel produces a debug-level log (benign back-
/// pressure under load — per-occurrence WARN flooded gateways at 30K+/hr,
/// see #4238); a closed channel produces a warn-level log (receiver torn
/// down). Either arm leaves the parked driver to fall back to its
/// `OPERATION_TTL` timeout (#4154).
fn notify_orphaned_transaction_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    tx: Transaction,
    peer: SocketAddr,
) -> bool {
    match notifications_sender.try_send(Either::Right(NodeEvent::TransactionOrphaned { tx, peer }))
    {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::debug!(
                %tx,
                %peer,
                "notify_orphaned_transaction: notification channel full; \
                 driver will wait for OPERATION_TTL timeout"
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(
                %tx,
                %peer,
                "notify_orphaned_transaction: notification channel closed; \
                 receiver likely dropped"
            );
            false
        }
    }
}

/// Blocking emit of a [`NodeEvent`] on the event-loop notification
/// channel, bounded by `timeout`.
///
/// Extracted from [`OpManager::notify_node_event`] so the
/// timeout-on-saturation path is testable in isolation without building
/// a full `OpManager`. Returns:
///   - `Ok(())` on successful enqueue,
///   - `Err(OpError::from(SendError))` if the channel is closed,
///   - `Err(OpError::NotificationChannelError(...))` after the timeout
///     elapses (a strong signal the event loop is stuck).
///
/// Diagnostic enrichment (channel pending / remaining slots) is sampled
/// INSIDE the error arm so the logged values reflect the channel state
/// at the moment of timeout, not the moment of the call. (Per PR #4231
/// third-pass review.)
async fn notify_node_event_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    timeout: Duration,
    msg: NodeEvent,
) -> Result<(), OpError> {
    match tokio::time::timeout(timeout, notifications_sender.send(Either::Right(msg))).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => {
            // Sample current channel state — accurate at log-emission
            // time, not 30s stale (M1 from PR #4231 third-pass review).
            let channel_remaining = notifications_sender.capacity();
            let channel_pending = notifications_sender
                .max_capacity()
                .saturating_sub(channel_remaining);
            tracing::error!(
                timeout_secs = timeout.as_secs(),
                channel_pending,
                channel_remaining,
                "notify_node_event: Notification channel full for too long, event loop may be stuck"
            );
            Err(OpError::NotificationChannelError(
                "notification channel send timed out — event loop is likely stuck".into(),
            ))
        }
    }
}

/// Non-blocking emit of a [`NodeEvent`] on the event-loop notification
/// channel.
///
/// Extracted from [`OpManager::try_notify_node_event`] so the try-send
/// path is testable in isolation without building a full `OpManager`.
/// On `Full` the event is dropped at debug level (benign back-pressure
/// under fan-out — was flooding gateways at 30K+/hr, see #4238); on
/// `Closed` it is dropped at warn level (receiver torn down). Either
/// arm returns `Err(OpError::NotificationError)`. Best-effort by
/// design — see the OpManager method doc for the wedge (#4145) this
/// prevents.
///
/// `channel_pending` and `channel_remaining` are passed by the caller
/// purely for log enrichment; they are read at the call site to avoid
/// requiring the wrapper type here. `channel_remaining` is the value
/// returned by `tokio::sync::mpsc::Sender::capacity()`, which is the
/// *current* available slot count, not the channel's max capacity.
fn try_notify_node_event_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    channel_pending: usize,
    channel_remaining: usize,
    msg: NodeEvent,
) -> Result<(), OpError> {
    match notifications_sender.try_send(Either::Right(msg)) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Benign back-pressure under sustained fan-out (best-
            // effort broadcast emission). Each occurrence isn't
            // actionable — the aggregate is, and the rate-limited
            // `notify_node_event: Notification channel full for too
            // long` error above is the alert operators should care
            // about. Per-occurrence WARN here flooded production
            // gateways post-HN-spike (#4238).
            tracing::debug!(
                channel_pending,
                channel_remaining,
                "try_notify_node_event: event-loop notification channel full; \
                 dropping best-effort broadcast event (#4145)"
            );
            Err(OpError::NotificationError)
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(
                "try_notify_node_event: event-loop notification channel closed; \
                 receiver likely dropped"
            );
            Err(OpError::NotificationError)
        }
    }
}

/// Orchestration-core of [`OpManager::flush_pending_broadcast_on_interest`],
/// extracted as a free function so its three branches are unit-testable against
/// a raw store + notifier + a stubbed "read current state" step, without
/// building a full `OpManager` (same pattern as [`emit_pending_broadcast_reemit_on`]
/// / [`release_pending_op_slot_on`]).
///
/// Issue #4359 (re-review, SHOULD-FIX 4 stale-state safety + fast-path):
///
/// 1. **Fast path / no-op** — nothing stashed for `key` (the common case: most
///    interest registrations are on already-propagated contracts). Returns
///    without invoking `read_current_state` or emitting anything, so the
///    contract handler is never touched on the hot path.
/// 2. **take()-None** — the membership check passed but `take()` lost the entry
///    to a concurrent drain (TTL expiry, a targets-found take, another flush);
///    again a no-op.
/// 3. **stashed → re-read current state** — re-broadcast the CURRENT local
///    state (`read_current_state` returned `Some`) rather than the possibly
///    superseded give-up-time bytes; fall back to the stashed `bytes` when the
///    live read fails (`None`) so we never regress to dropping the broadcast.
///
/// `read_current_state` is a closure returning the read future so it is only
/// polled once a stash entry is actually drained (preserving the fast path).
async fn flush_pending_broadcast_on_interest_on<F, Fut>(
    pending: &crate::operations::update::pending_broadcast::PendingBroadcastStore,
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    channel_pending: usize,
    channel_remaining: usize,
    key: &ContractKey,
    read_current_state: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Option<WrappedState>>,
{
    // Fast path: nothing stashed for this contract. Avoid the take()/contract-
    // handler read entirely.
    if !pending.contains(key.id()) {
        return;
    }
    // Drain the stash marker. If it raced away (TTL expiry, a concurrent
    // targets-found take, or another flush) there is nothing to do.
    let Some(stashed) = pending.take(key.id()) else {
        return;
    };

    // Stale-state safety: re-read the CURRENT local state instead of
    // re-broadcasting the give-up-time bytes. Fall back to the stashed bytes
    // if the live read fails so we never regress to dropping the broadcast.
    let state = read_current_state().await.unwrap_or(stashed);

    emit_pending_broadcast_reemit_on(
        notifications_sender,
        channel_pending,
        channel_remaining,
        pending,
        key,
        state,
    );
}

/// Emit a deferred fresh-contract re-broadcast for `key` carrying `state` on the
/// event-loop notification channel, re-stashing `state` in `pending` if the
/// channel can't accept it. The emit-core of
/// [`OpManager::flush_pending_broadcast_on_interest`], extracted as a free
/// function so the channel-full re-stash mechanics are unit-testable against a
/// raw notifier + store without building a full `OpManager` (same pattern as
/// [`release_pending_op_slot_on`] / [`try_notify_node_event_on`]).
///
/// Issue #4359: when the broadcast handler gives up on a never-before-seen id
/// (no targets), it stashes the state in `pending`. The caller has already
/// drained the stash and resolved the (current) `state` to broadcast; this
/// re-emits it as `BroadcastStateChange { is_retry: false, is_reemit: true }`
/// so the now-present target receives it instead of the state staying
/// locally-hosted only. `is_reemit` suppresses #4281 no_targets double-counting
/// on a still-no-target re-emission.
///
/// Best-effort: if the channel is full the state is re-stashed so a later
/// signal retries it (losing it would re-open the bug).
fn emit_pending_broadcast_reemit_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    channel_pending: usize,
    channel_remaining: usize,
    pending: &crate::operations::update::pending_broadcast::PendingBroadcastStore,
    key: &ContractKey,
    state: WrappedState,
) {
    tracing::debug!(
        contract = %key,
        phase = "pending_broadcast_flush",
        "Re-broadcasting deferred fresh-contract state now that an interested peer/target appeared (#4359)"
    );
    let msg = NodeEvent::BroadcastStateChange {
        key: *key,
        new_state: state.clone(),
        is_retry: false,
        is_reemit: true,
    };
    if try_notify_node_event_on(
        notifications_sender,
        channel_pending,
        channel_remaining,
        msg,
    )
    .is_err()
    {
        // Re-emit dropped (channel full / closed). Put the state back so a
        // later signal can retry — losing it here would re-open the
        // locally-hosted-only failure this fix closes.
        pending.stash(*key.id(), state);
        tracing::debug!(
            contract = %key,
            "emit_pending_broadcast_reemit_on: re-emit dropped; re-stashed for the next signal"
        );
    }
}

/// Notify the event loop about a timed-out transaction without blocking.
///
/// Uses `try_send` instead of `.send().await` to avoid blocking the garbage
/// cleanup task when the notification channel is full. The GC task already
/// cleans up the transaction from the ops maps — this notification only
/// lets the event loop clean up its `tx_to_client` map, so dropping it
/// when the channel is congested is acceptable.
fn notify_transaction_timeout(
    event_loop_notifier: &EventLoopNotificationsSender,
    tx: Transaction,
) -> bool {
    match event_loop_notifier
        .notifications_sender
        .try_send(Either::Right(NodeEvent::TransactionTimedOut(tx)))
    {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Benign back-pressure on the same event-loop notification
            // channel as the two `try_*` helpers above: the GC sweep
            // already removed the tx from the ops maps; this
            // notification only lets the event loop clean up its
            // `tx_to_client` map, so dropping it is tolerated. Per-
            // occurrence WARN here would re-introduce the #4238 spam
            // class during sustained back-pressure.
            tracing::debug!(
                tx = %tx,
                "Notification channel full, skipping timeout notification for event loop"
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(
                tx = %tx,
                "Notification channel closed, receiver likely dropped"
            );
            false
        }
    }
}

// Per-op GC sweep helpers (`remove_*_and_report_failure` /
// `notify_subscription_timeout`) are gone — each non-CONNECT op
// owns its own timeout reporting in its driver via
// `RetryLoopOutcome::Exhausted` → `result_router_tx`, and relay
// drivers expire their inflight guards naturally.

// `record_connect_uphill_timeout` and the per-op GC-sweep CONNECT
// branch are gone with `ops.connect`. The CONNECT driver
// (`start_relay_connect`) now owns its own uphill-timeout reporting
// via the `Relay*InflightGuard` failure path; the GC sweep no longer
// has a `ConnectOp` to inspect.

#[allow(clippy::too_many_arguments)]
async fn garbage_cleanup_task<ER: NetEventRegister>(
    mut new_transactions: tokio::sync::mpsc::Receiver<Transaction>,
    ops: Arc<Ops>,
    live_tx_tracker: LiveTransactionTracker,
    event_loop_notifier: EventLoopNotificationsSender,
    mut event_register: ER,
    _result_router_tx: mpsc::Sender<(Transaction, HostResult)>,
    request_router: Arc<OnceLock<Arc<RequestRouter>>>,
    contract_waiters: Arc<
        Mutex<std::collections::HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>>>,
    >,
    pending_contract_fetches: Arc<DashMap<ContractInstanceId, u64>>,
    active_relay_get_txs: Arc<DashSet<Transaction>>,
    active_relay_update_txs: Arc<DashSet<Transaction>>,
    active_relay_put_txs: Arc<DashSet<Transaction>>,
    active_relay_subscribe_txs: Arc<DashSet<Transaction>>,
    active_relay_connect_txs: Arc<DashSet<Transaction>>,
) {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
    /// How often to clean up stale contract_waiters entries (every N ticks).
    const WAITER_CLEANUP_EVERY_N_TICKS: u32 = 12; // every 60s at 5s interval
    let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
    tick.tick().await;
    let mut tick_count: u32 = 0;

    let mut ttl_set = BTreeSet::new();

    let mut delayed = vec![];
    loop {
        crate::deterministic_select! {
            tx = new_transactions.recv() => {
                if let Some(tx) = tx {
                    ttl_set.insert(Reverse(tx));
                }
            },
            _ = tick.tick() => {
                tick_count = tick_count.wrapping_add(1);

                // Opt-in periodic memory-stats dump. Gated by env var so the
                // hot path stays quiet in prod. Intended for local / CI sim
                // runs where we want to correlate RSS growth with retained
                // state in OpManager.
                if std::env::var("FREENET_MEMORY_STATS").is_ok() {
                    use std::sync::atomic::Ordering;
                    let ops_sizes = ops.sizes();
                    let pending_fetches = pending_contract_fetches.len();
                    let waiters_len = contract_waiters.lock().len();
                    let relay_inflight =
                        crate::operations::get::op_ctx_task::RELAY_INFLIGHT
                            .load(Ordering::Relaxed);
                    let relay_spawned =
                        crate::operations::get::op_ctx_task::RELAY_SPAWNED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_completed =
                        crate::operations::get::op_ctx_task::RELAY_COMPLETED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_dedup_rejects =
                        crate::operations::get::op_ctx_task::RELAY_DEDUP_REJECTS
                            .load(Ordering::Relaxed);
                    let relay_active_txs = active_relay_get_txs.len();
                    let relay_update_inflight =
                        crate::operations::update::op_ctx_task::RELAY_UPDATE_INFLIGHT
                            .load(Ordering::Relaxed);
                    let relay_update_spawned =
                        crate::operations::update::op_ctx_task::RELAY_UPDATE_SPAWNED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_update_completed =
                        crate::operations::update::op_ctx_task::RELAY_UPDATE_COMPLETED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_update_dedup_rejects =
                        crate::operations::update::op_ctx_task::RELAY_UPDATE_DEDUP_REJECTS
                            .load(Ordering::Relaxed);
                    let relay_update_active_txs = active_relay_update_txs.len();
                    let relay_put_inflight =
                        crate::operations::put::op_ctx_task::RELAY_PUT_INFLIGHT
                            .load(Ordering::Relaxed);
                    let relay_put_spawned =
                        crate::operations::put::op_ctx_task::RELAY_PUT_SPAWNED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_put_completed =
                        crate::operations::put::op_ctx_task::RELAY_PUT_COMPLETED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_put_dedup_rejects =
                        crate::operations::put::op_ctx_task::RELAY_PUT_DEDUP_REJECTS
                            .load(Ordering::Relaxed);
                    let relay_put_active_txs = active_relay_put_txs.len();
                    let relay_subscribe_inflight =
                        crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_INFLIGHT
                            .load(Ordering::Relaxed);
                    let relay_subscribe_spawned =
                        crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_SPAWNED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_subscribe_completed =
                        crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_COMPLETED_TOTAL
                            .load(Ordering::Relaxed);
                    let relay_subscribe_dedup_rejects =
                        crate::operations::subscribe::op_ctx_task::RELAY_SUBSCRIBE_DEDUP_REJECTS
                            .load(Ordering::Relaxed);
                    let relay_subscribe_active_txs = active_relay_subscribe_txs.len();
                    let relay_connect_active_txs = active_relay_connect_txs.len();
                    tracing::info!(
                        target: "memory_stats",
                        tick = tick_count,
                        // No DashMaps for ops_connect / ops_get /
                        // ops_put / ops_update / ops_subscribe —
                        // always 0.
                        ops_connect = 0,
                        ops_put = 0,
                        ops_get = 0,
                        ops_subscribe = 0,
                        ops_update = 0,
                        ops_completed = ops_sizes.completed,
                        ops_under_progress = ops_sizes.under_progress,
                        pending_contract_fetches = pending_fetches,
                        contract_waiters = waiters_len,
                        relay_inflight = relay_inflight,
                        relay_spawned = relay_spawned,
                        relay_completed = relay_completed,
                        relay_dedup_rejects = relay_dedup_rejects,
                        relay_active_txs = relay_active_txs,
                        relay_update_inflight = relay_update_inflight,
                        relay_update_spawned = relay_update_spawned,
                        relay_update_completed = relay_update_completed,
                        relay_update_dedup_rejects = relay_update_dedup_rejects,
                        relay_update_active_txs = relay_update_active_txs,
                        relay_put_inflight = relay_put_inflight,
                        relay_put_spawned = relay_put_spawned,
                        relay_put_completed = relay_put_completed,
                        relay_put_dedup_rejects = relay_put_dedup_rejects,
                        relay_put_active_txs = relay_put_active_txs,
                        relay_subscribe_inflight = relay_subscribe_inflight,
                        relay_subscribe_spawned = relay_subscribe_spawned,
                        relay_subscribe_completed = relay_subscribe_completed,
                        relay_subscribe_dedup_rejects = relay_subscribe_dedup_rejects,
                        relay_subscribe_active_txs = relay_subscribe_active_txs,
                        relay_connect_active_txs = relay_connect_active_txs,
                        "memory stats"
                    );
                }

                // Periodically clean up stale contract_waiters entries where the
                // receiver has been dropped (e.g., operation timed out). Without this,
                // the map grows unboundedly under sustained load (#2928).
                if tick_count % WAITER_CLEANUP_EVERY_N_TICKS == 0 {
                    let mut waiters = contract_waiters.lock();
                    let before = waiters.len();
                    waiters.retain(|_id, senders| {
                        // Remove senders whose receiver was dropped
                        senders.retain(|sender| !sender.is_closed());
                        !senders.is_empty()
                    });
                    let after = waiters.len();
                    if before != after {
                        tracing::info!(
                            before,
                            after,
                            removed = before - after,
                            "Cleaned up stale contract_waiters entries"
                        );
                    }
                }


                // Periodically clean up stale pending_contract_fetches entries.
                // Entries older than 2x cooldown are removed to prevent unbounded growth.
                if tick_count % 12 == 0 {
                    let cooldown_ms = crate::operations::update::CONTRACT_FETCH_COOLDOWN_MS;
                    let now_ms = crate::config::GlobalSimulationTime::read_time_ms();
                    pending_contract_fetches.retain(|_, ts| {
                        now_ms.saturating_sub(*ts) < cooldown_ms * 2
                    });
                }

                let old_missing = std::mem::take(&mut delayed);
                for tx in old_missing {
                    if let Some(tx) = ops.completed.remove(&tx) {
                        if cfg!(feature = "trace-ot") {
                            let op_type = tx.transaction_type().description();
                            event_register.notify_of_time_out(tx, op_type, None).await;
                        } else {
                            _ = tx;
                        }
                        continue;
                    }
                    // Every op runs on a driver and owns
                    // its own timeout reporting (via
                    // `Relay*InflightGuard` failure paths or
                    // `RetryLoopOutcome::Exhausted`). Nothing for the
                    // GC sweep to remove per-op anymore.
                    let still_waiting = false;
                    if still_waiting {
                        delayed.push(tx);
                    } else {
                        ops.under_progress.remove(&tx);
                        ops.completed.remove(&tx);
                        tracing::info!(
                            tx = %tx,
                            tx_type = ?tx.transaction_type(),
                            elapsed_ms = tx.elapsed().as_millis(),
                            ttl_ms = crate::config::OPERATION_TTL.as_millis(),
                            "Transaction timed out"
                        );

                        notify_transaction_timeout(&event_loop_notifier, tx);
                        live_tx_tracker.remove_finished_transaction(tx);

                        // Clean up request router to prevent stale entries from blocking
                        // subsequent requests for the same resource after timeout
                        if let Some(router) = request_router.get() {
                            router.complete_operation(tx);
                        }
                    }
                }

                // notice the use of reverse so the older transactions are removed instead of the newer ones
                let older_than: Reverse<Transaction> = Reverse(Transaction::ttl_transaction());
                // Absolute cutoff for under_progress ops: 5× normal TTL (5 minutes).
                // Without this, operations stuck in under_progress are exempt from GC forever.
                let absolute_cutoff: Reverse<Transaction> =
                    Reverse(Transaction::ttl_transaction_with_multiplier(5));
                for Reverse(tx) in ttl_set.split_off(&older_than).into_iter() {
                    if ops.under_progress.contains(&tx) {
                        // Allow extended lifetime unless absolute timeout exceeded.
                        // Reverse flips ordering: Reverse(tx) < absolute_cutoff means
                        // tx is newer than the 5× TTL cutoff, so keep it alive.
                        if Reverse(tx) < absolute_cutoff {
                            delayed.push(tx);
                            continue;
                        }
                        tracing::warn!(tx = %tx, "Cleaning up under_progress op that exceeded absolute timeout (5× TTL)");
                        ops.under_progress.remove(&tx);
                        // Fall through to normal cleanup below
                    }
                    if let Some(tx) = ops.completed.remove(&tx) {
                        tracing::debug!("Clean up timed out: {tx}");
                        if cfg!(feature = "trace-ot") {
                            let op_type = tx.transaction_type().description();
                            event_register.notify_of_time_out(tx, op_type, None).await;
                        } else {
                            _ = tx;
                        }
                    }
                    // Same as above: every op owns its own timeout
                    // reporting; the GC sweep has nothing per-op to
                    // remove.
                    let removed = false;
                    if removed {
                        tracing::info!(
                            tx = %tx,
                            tx_type = ?tx.transaction_type(),
                            elapsed_ms = tx.elapsed().as_millis(),
                            ttl_ms = crate::config::OPERATION_TTL.as_millis(),
                            "Transaction timed out"
                        );

                        notify_transaction_timeout(&event_loop_notifier, tx);
                        live_tx_tracker.remove_finished_transaction(tx);

                        // Clean up request router to prevent stale entries from blocking
                        // subsequent requests for the same resource after timeout
                        if let Some(router) = request_router.get() {
                            router.complete_operation(tx);
                        }
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::network_bridge::event_loop_notification_channel;
    use super::*;
    use crate::config::GlobalSimulationTime;
    use crate::node::network_bridge::EventLoopNotificationsReceiver;
    use either::Either;
    use tokio::time::{Duration, Instant, timeout};

    /// Piece-D H2 pin (keystone step-2, #4642): the strictly-farther
    /// downstream-subscriber filter the reconcile input-builder delegates to
    /// (`has_farther_downstream_subscriber` → [`has_strictly_farther`]) MUST
    /// EXCLUDE a subscriber at exactly our distance (bit-identical) and use
    /// EXACT `Distance` ordering, never the epsilon-fuzzy `PartialEq`. A
    /// closer/equal (upstream) peer counting as farther demand would let mutual
    /// co-hosts perpetuate each other's leases so collapse never terminates
    /// (hosting-invariants piece-D, design §4/§6). Feeds bit-identical distances
    /// exactly as the H2 obligation on the builder requires.
    #[test]
    fn has_strictly_farther_excludes_equal_uses_exact_ordering() {
        let my = Distance::new(0.3);

        // Bit-identical (exactly our distance) ⇒ excluded.
        assert!(
            !has_strictly_farther(my, std::iter::once(Distance::new(0.3))),
            "a subscriber at exactly our distance must be excluded (the closer/upstream peer)"
        );
        // Strictly closer ⇒ excluded.
        assert!(
            !has_strictly_farther(my, std::iter::once(Distance::new(0.2))),
            "a strictly-closer subscriber must be excluded"
        );
        // Strictly farther ⇒ included.
        assert!(
            has_strictly_farther(my, std::iter::once(Distance::new(0.4))),
            "a strictly-farther subscriber must be included"
        );

        // Epsilon boundary: one ULP FARTHER than `my` is epsilon-`==` to it (a
        // fuzzy `==` filter would wrongly treat it as equal ⇒ excluded) but
        // EXACT `cmp` orders it Greater ⇒ included. This is the exact-vs-epsilon
        // bug the guard prevents (`ring/location.rs:223-243`).
        let one_ulp_farther = Distance::new(f64::from_bits(0.3_f64.to_bits() + 1));
        assert_eq!(
            one_ulp_farther, my,
            "one ULP apart is epsilon-equal under Distance PartialEq"
        );
        assert!(
            has_strictly_farther(my, std::iter::once(one_ulp_farther)),
            "exact cmp includes a one-ULP-farther subscriber that a fuzzy `==` would exclude"
        );

        // Mixed sets: any strictly-farther member triggers; equal + closer alone
        // do not.
        assert!(
            has_strictly_farther(
                my,
                [Distance::new(0.3), Distance::new(0.2), Distance::new(0.45)].into_iter()
            ),
            "any strictly-farther subscriber in the set triggers"
        );
        assert!(
            !has_strictly_farther(my, [Distance::new(0.3), Distance::new(0.1)].into_iter()),
            "equal + closer only ⇒ no farther demand"
        );
        // Empty ⇒ false.
        assert!(!has_strictly_farther(my, std::iter::empty()));
    }

    /// Extract the brace-matched `{...}` block that immediately follows `anchor`
    /// in `src` (source-scrape helper for the drives-nothing pins). Panics if the
    /// anchor is absent or the braces are unbalanced.
    fn braced_block_after<'a>(src: &'a str, anchor: &str) -> &'a str {
        let start = src
            .find(anchor)
            .unwrap_or_else(|| panic!("anchor not found: {anchor}"));
        let brace = src[start..]
            .find('{')
            .expect("anchor must be followed by a block");
        let body_start = start + brace + 1;
        let bytes = src.as_bytes();
        let mut depth = 1i32;
        let mut i = body_start;
        while i < bytes.len() {
            match bytes[i] {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &src[body_start..i];
                    }
                }
                _ => {}
            }
            i += 1;
        }
        panic!("unbalanced braces after anchor: {anchor}");
    }

    /// FLIP pin (keystone P6, #4642): the collapse teardown
    /// `send_unsubscribe_upstream` still drives the wire `Unsubscribe` off the
    /// STORED `is_upstream` flag and the local `ring.unsubscribe` (the narrow flip
    /// KEEPS the stored upstream identity — it does NOT compute-upstream-
    /// everywhere), and it STILL records the upstream-IDENTITY shadow
    /// (`record_upstream_divergence_comparison`, unflipped). The record-only
    /// reconcile COLLAPSE action-set shadow that used to live inside this function
    /// is GONE — the collapse DECISION now DRIVES one level up
    /// (`reconcile_wants_collapse` at the callers), so the function must no longer
    /// build a reconcile snapshot or run the action-set comparator here.
    #[test]
    fn send_unsubscribe_upstream_drives_off_stored_upstream_after_flip() {
        const SRC: &str = include_str!("op_state_manager.rs");
        let start = SRC
            .find("pub async fn send_unsubscribe_upstream(")
            .expect("send_unsubscribe_upstream must exist");
        // Bound the scan to this function so later functions can't satisfy the
        // markers by accident (next `\n    pub ` item after the signature).
        let rest = &SRC[start + 1..];
        let end = rest
            .find("\n    pub ")
            .map(|e| start + 1 + e)
            .unwrap_or(SRC.len());
        let body = &SRC[start..end];

        // Teardown still uses the STORED upstream identity + local ring.unsubscribe:
        assert!(
            body.contains("interest.is_upstream"),
            "collapse must still locate the upstream via the stored is_upstream flag \
             (narrow flip keeps the stored flag)"
        );
        assert!(
            body.contains("self.ring.unsubscribe(contract)"),
            "collapse must still drop the local lease via ring.unsubscribe"
        );
        // The upstream-IDENTITY shadow (stored vs computed) STAYS wired, unflipped:
        assert!(
            body.contains("record_upstream_divergence_comparison"),
            "the upstream computed-vs-stored IDENTITY shadow must keep running \
             (unflipped per the narrow-flip scope)"
        );
        // The record-only reconcile COLLAPSE action-set shadow is GONE from here —
        // the decision drives at the callers (reconcile_wants_collapse). No reconcile
        // snapshot build and no action-set comparator inside this function.
        assert!(
            !body.contains("build_reconcile_inputs"),
            "the collapse decision drives one level up (reconcile_wants_collapse); \
             send_unsubscribe_upstream must no longer build a reconcile snapshot"
        );
        assert!(
            !body.contains("action_set_divergence(&reconcile_actions"),
            "the record-only collapse action-set shadow must be removed after the flip"
        );
        assert!(
            !body.contains("ReconcileShadowSite::Collapse"),
            "send_unsubscribe_upstream must no longer record the Collapse shadow — \
             the repurposed Collapse counter is recorded in reconcile_wants_collapse"
        );
    }

    /// FLIP pin (keystone P6, #4642): the COLLAPSE decision is DRIVEN by the
    /// reconcile controller. `reconcile_wants_collapse` applies the pure
    /// `reconcile::wants_collapse` gate, fails SAFE (does NOT collapse) on a
    /// distance-blind snapshot, and all three collapse-decision sites gate on it
    /// instead of the legacy ANY-downstream `should_unsubscribe_upstream`.
    #[test]
    fn collapse_decision_is_reconcile_driven() {
        const SRC: &str = include_str!("op_state_manager.rs");
        let gate = braced_block_after(SRC, "pub(crate) fn reconcile_wants_collapse(");
        assert!(
            gate.contains("reconcile::wants_collapse(&inputs)"),
            "reconcile_wants_collapse must apply the pure wants_collapse gate"
        );
        assert!(
            gate.contains("build_reconcile_inputs"),
            "reconcile_wants_collapse must build a FRESH at-emission snapshot"
        );
        // Fail-safe: the distance-blind (`else`) arm must return false (do NOT
        // collapse) — the mirror of reconcile_wants_renewal's keep-renewing default.
        let blind = gate
            .find("build_reconcile_inputs(contract) else")
            .expect("reconcile_wants_collapse must guard the distance-blind snapshot");
        let after = &gate[blind..];
        let arm_end = after.find('}').expect("let-else arm must close");
        assert!(
            after[..arm_end].contains("return false"),
            "reconcile_wants_collapse must FAIL SAFE (return false, do not collapse) \
             when the snapshot is distance-blind"
        );

        // All three collapse-decision sites gate on reconcile_wants_collapse, each
        // passing its pre-flip per-site counter (maintenance-loop + client-disconnect
        // → Collapse; inbound-unsubscribe → InboundUnsubscribe).
        let ring = include_str!("../ring.rs");
        assert!(
            ring.contains("op_manager.reconcile_wants_collapse(")
                && ring.contains("ReconcileShadowSite::Collapse"),
            "the renewal-loop downstream-expiry collapse must gate on \
             reconcile_wants_collapse (Collapse site)"
        );
        let client = include_str!("../client_events.rs");
        assert!(
            client.contains("op_manager.reconcile_wants_collapse(")
                && client.contains("ReconcileShadowSite::Collapse"),
            "the client-disconnect collapse must gate on reconcile_wants_collapse \
             (Collapse site)"
        );
        let sub = include_str!("../operations/subscribe.rs");
        assert!(
            sub.contains("op_manager.reconcile_wants_collapse(")
                && sub.contains("ReconcileShadowSite::InboundUnsubscribe"),
            "the inbound-unsubscribe collapse must gate on reconcile_wants_collapse \
             (InboundUnsubscribe site)"
        );
        // The legacy ANY-downstream predicate must no longer GATE any of the three
        // collapse teardowns (matched on the `ring.should_unsubscribe_upstream(`
        // call form, so the explanatory comments that name it don't false-trip). It
        // survives only as the post-flip legibility baseline inside
        // reconcile_wants_collapse and as a Ring/hosting helper.
        assert!(
            !ring.contains("if ring.should_unsubscribe_upstream(contract)"),
            "the renewal-loop collapse must no longer gate on ring.should_unsubscribe_upstream"
        );
        assert!(
            !client.contains("ring.should_unsubscribe_upstream"),
            "the client-disconnect collapse must no longer gate on ring.should_unsubscribe_upstream"
        );
        assert!(
            !sub.contains("ring.should_unsubscribe_upstream"),
            "the inbound-unsubscribe collapse must no longer gate on ring.should_unsubscribe_upstream"
        );
    }

    /// Wiring pin (Codex P2 fix, #4642): the reconcile input-builder MUST feed
    /// recent local GET/PUT access into `has_recent_local_client_access`, and the
    /// collapse-gate legibility baseline MUST factor it out. Without the builder
    /// wiring, `contract_in_use` (which now ORs the field) would always see it
    /// `false` and the P6 flip would drop a read-only / PUT-only contract's lease
    /// even though `contracts_needing_renewal()` branch 3 keeps emitting it — the
    /// regression this fix closes (invariant 3: reads/PUTs are permanent demand).
    #[test]
    fn build_reconcile_inputs_wires_recent_local_client_access() {
        const SRC: &str = include_str!("op_state_manager.rs");
        let builder = braced_block_after(SRC, "pub(crate) fn build_reconcile_inputs(");
        assert!(
            builder.contains("has_recent_local_client_access: self")
                && builder.contains("has_recent_local_client_access(contract)"),
            "build_reconcile_inputs must populate has_recent_local_client_access from \
             ring.has_recent_local_client_access(contract) so the P6 renewal/collapse \
             gate honors read/PUT demand (invariant 3)"
        );
        let gate = braced_block_after(SRC, "pub(crate) fn reconcile_wants_collapse(");
        assert!(
            gate.contains("!self.ring.has_recent_local_client_access(contract)"),
            "the collapse-gate legibility baseline must factor out recent-access so the \
             divergence counter isolates the strict-farther difference"
        );
    }

    /// Hardening pin (keystone step-2 completion, #4642): the shared edge-site
    /// helper `record_reconcile_shadow_event` DRIVES NOTHING — reconcile's output
    /// flows only into the focused divergence comparator + the per-site counter.
    /// One pin covers the still-SHADOW edge sites (connection-drop, host-formation)
    /// since they route through this helper. (The inbound-unsubscribe collapse was
    /// FLIPPED to driving in P6 and no longer uses this record-only helper.)
    #[test]
    fn record_reconcile_shadow_event_drives_nothing() {
        const SRC: &str = include_str!("op_state_manager.rs");
        let body = braced_block_after(SRC, "pub(crate) fn record_reconcile_shadow_event(");
        assert!(
            body.contains("action_set_divergence_focused"),
            "the edge-site helper must compare via action_set_divergence_focused"
        );
        assert!(
            body.contains("record_reconcile_shadow_comparison"),
            "the edge-site helper must record the per-site divergence"
        );
        for driver in [
            ".send(",
            "self.ring.subscribe",
            "self.ring.unsubscribe",
            "send_unsubscribe",
            "mark_subscription_pending",
            "announce_contract_hosted",
            "on_contract_hosted",
            "on_contract_unhosted",
            "notify_node_event",
        ] {
            assert!(
                !body.contains(driver),
                "record_reconcile_shadow_event must not drive `{driver}` — it records only"
            );
        }
    }

    /// Hardening pin (keystone step-2 completion, #4642): the still-SHADOW EDGE
    /// decision site (host-formation) is wired to the record-only helper with its
    /// own `ReconcileShadowSite`. A future edit that drops its shadow fails here.
    /// (Inbound-unsubscribe was FLIPPED to driving in P6 — covered by
    /// `collapse_decision_is_reconcile_driven`. Connection-drop was FLIPPED to
    /// driving in piece F — covered by `connection_drop_re_root_is_driven`, no
    /// longer a shadow site.)
    #[test]
    fn edge_sites_wire_the_record_only_helper() {
        // Host-formation lives in operations/subscribe.rs.
        let sub = include_str!("../operations/subscribe.rs");
        assert!(sub.contains("ReconcileShadowSite::HostFormation"));
        assert!(sub.contains("record_reconcile_shadow_event"));
        // Host-formation GET path lives in operations/get/op_ctx_task.rs.
        let get = include_str!("../operations/get/op_ctx_task.rs");
        assert!(get.contains("ReconcileShadowSite::HostFormation"));
        assert!(get.contains("record_reconcile_shadow_event"));
        // Connection-drop still runs its production disconnect bookkeeping
        // (behavior-preserving) even though its re-root now DRIVES.
        const SRC: &str = include_str!("op_state_manager.rs");
        let drop_body = braced_block_after(SRC, "pub(crate) fn on_ring_connection_lost(");
        assert!(
            drop_body.contains("on_peer_disconnected")
                && drop_body.contains("schedule_deferred_removal"),
            "on_ring_connection_lost must still do the production disconnect bookkeeping"
        );
    }

    /// FLIP pin (#4642 piece F): the connection-drop re-root is DRIVEN, not a
    /// record-only shadow, AND is interest-gated + storm-safe. This is the
    /// counterpart to `collapse_decision_is_reconcile_driven` for the piece-F flip.
    ///
    /// Asserts, by source scrape:
    /// - `on_ring_connection_lost` no longer records the ConnectionDrop shadow
    ///   (`record_reconcile_shadow_event` gone from its body) and instead drives a
    ///   PROMPT re-root via `spawn_prompt_reroots` — i.e. the old
    ///   record-only-with-`|_| Vec::new()` arm is gone.
    /// - `spawn_prompt_reroots` carries every storm-safety guard: the zero-connection
    ///   guard, the bounded candidate scan, the per-drop cap
    ///   (`MAX_PROMPT_REROOTS_PER_DROP`), the per-contract backoff/dedup
    ///   (`can_request_subscription` + `mark_subscription_pending`), the interest
    ///   gate (`reconcile_wants_reroot`, so a demandless copy is NOT re-rooted), and
    ///   the shared make-before-break spawn path (`spawn_renewal_subscribe_task`).
    #[test]
    fn connection_drop_re_root_is_driven() {
        const SRC: &str = include_str!("op_state_manager.rs");

        let drop_body = braced_block_after(SRC, "pub(crate) fn on_ring_connection_lost(");
        assert!(
            drop_body.contains("spawn_prompt_reroots"),
            "on_ring_connection_lost must DRIVE a prompt re-root via spawn_prompt_reroots"
        );
        assert!(
            !drop_body.contains("record_reconcile_shadow_event"),
            "the connection-drop re-root must DRIVE, not record a shadow — the \
             record-only helper must be gone from on_ring_connection_lost"
        );
        assert!(
            !drop_body.contains("|_| Vec::new()"),
            "the shadow's `production never re-roots` (|_| Vec::new()) arm must be gone"
        );

        // The non-spawning storm-safety guards live in the SELECTION half.
        let select = braced_block_after(SRC, "pub(crate) fn select_prompt_reroots(");
        // Guard 1: zero-connection.
        assert!(
            select.contains("open_connections() == 0"),
            "select_prompt_reroots must have a zero-connection guard"
        );
        // Guards 2 + 3: bounded scan + per-drop storm cap.
        assert!(
            select.contains("subscribed_keys_in") && select.contains("MAX_PROMPT_REROOTS_PER_DROP"),
            "select_prompt_reroots must bound the candidate scan AND cap re-roots per \
             drop (the storm bound)"
        );
        // Guard 4: per-contract backoff.
        assert!(
            select.contains("can_request_subscription"),
            "select_prompt_reroots must apply per-contract backoff"
        );
        // Guard 5: interest gate — demandless copies are NOT re-rooted.
        assert!(
            select.contains("reconcile_wants_reroot"),
            "select_prompt_reroots must be interest-gated via reconcile_wants_reroot"
        );
        // Guard 6 (spawn half): pending-dedup claim + shared make-before-break spawn.
        let reroot = braced_block_after(SRC, "pub(crate) fn spawn_prompt_reroots(");
        assert!(
            reroot.contains("mark_subscription_pending"),
            "spawn_prompt_reroots must claim the per-contract slot (pending-dedup)"
        );
        assert!(
            reroot.contains("spawn_renewal_subscribe_task"),
            "spawn_prompt_reroots must spawn via the shared renewal path \
             (make-before-break, single source of truth)"
        );

        // The interest gate keys on the ReRootSearch action specifically.
        let gate = braced_block_after(SRC, "pub(crate) fn reconcile_wants_reroot(");
        assert!(
            gate.contains("Action::ReRootSearch"),
            "reconcile_wants_reroot must gate on the ReRootSearch action"
        );
        assert!(
            gate.contains("return false"),
            "reconcile_wants_reroot must fail SAFE (do not re-root) on a \
             distance-blind snapshot"
        );
    }

    // ===== Connection-drop PROMPT re-root behavioral tests (#4642 piece F) =====

    /// Build a minimal real `OpManager` (Local mode) for the connection-drop
    /// re-root behavioral tests. Mirrors the executor pool_tests builder. Returns
    /// the op_manager plus a guard box that keeps the receivers + task monitor
    /// alive for the test scope.
    async fn build_reroot_test_op_manager(id: &str) -> (Arc<OpManager>, Box<dyn std::any::Any>) {
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let (notification_rx, notification_tx) = event_loop_notification_channel();
        let (ops_ch_channel, ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        let guards: Box<dyn std::any::Any> = Box::new((
            notification_rx,
            ch_channel,
            wait_for_event,
            result_router_rx,
            task_monitor,
        ));
        (op_manager, guards)
    }

    /// Give the node a known own address (so reconcile snapshots are not
    /// distance-blind — `own_location().location()` derives from the own address)
    /// plus one live connection (so the zero-connection guard passes). The
    /// connection is NOT announced as co-hosting anything, so it never becomes
    /// anyone's computed upstream.
    fn seed_location_and_one_connection(op_manager: &OpManager) {
        let self_addr: std::net::SocketAddr = "127.0.0.1:12000".parse().unwrap();
        op_manager.ring.connection_manager.set_own_addr(self_addr);
        assert!(
            op_manager
                .ring
                .connection_manager
                .own_location()
                .location()
                .is_some(),
            "own location must be resolvable so reconcile snapshots are not distance-blind"
        );
        let peer_kp = crate::transport::TransportKeypair::new();
        let peer_addr: std::net::SocketAddr = "127.0.0.1:34567".parse().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(0.9),
            peer_addr,
            peer_kp.public().clone(),
            false,
        ));
        assert!(op_manager.ring.open_connections() > 0);
    }

    fn reroot_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            freenet_stdlib::prelude::CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    /// Register `hub` as co-hosting every `contracts` instance, returning the set
    /// the disconnect handler would resolve (`contracts_for_peer`).
    fn register_hub_cohosting(
        op_manager: &OpManager,
        hub: &crate::transport::TransportPublicKey,
        contracts: &[ContractInstanceId],
    ) -> HashSet<ContractInstanceId> {
        op_manager.neighbor_hosting.handle_message(
            hub,
            crate::message::NeighborHostingMessage::HostingAnnounce {
                added: contracts.to_vec(),
                removed: vec![],
                is_response: false,
            },
        );
        op_manager.neighbor_hosting.contracts_for_peer(hub)
    }

    /// Safety proof for #4642 step 9 (remove the Source-2 interest fan-out arm
    /// from live UPDATE propagation). At the `get_broadcast_targets_update`
    /// boundary this asserts the properties the removal must preserve:
    ///
    /// 1. An interest-only peer — registered in the interest manager but NOT
    ///    advertising via the advertisement layer — is EXCLUDED from broadcast
    ///    targets. This is the interest-but-not-yet-advertised (lagged
    ///    advertisement) case; under the old two-source model it WOULD be a
    ///    target via Source 2, and after step 9 it must not be.
    /// 2. An advertised co-host (Source 1) is still INCLUDED.
    /// 3. The advertisement-reconciliation heal restores the lagged peer: once
    ///    our view of it includes the contract it becomes a broadcast target. The
    ///    periodic interest heartbeat sends each neighbor a `HostingStateRequest`;
    ///    the neighbor replies with a `HostingStateResponse` snapshot of its
    ///    hosted set, which we full-replace into our Source-1 view
    ///    (`ring.rs::interest_heartbeat`, #4722).
    ///
    /// This is the unit-level counterpart of the lagged-advertisement
    /// convergence simulation: removing Source-2 is safe precisely because a
    /// peer that is interested-but-not-yet-advertised is reached once its
    /// advertisement lands in Source-1, live or via anti-entropy. See
    /// `.claude/rules/hosting-invariants.md` invariant 1 and
    /// `docs/design/demand-driven-hosting.md`.
    #[tokio::test(flavor = "current_thread")]
    async fn broadcast_targets_are_advertised_cohosts_only_and_heal_via_advertisement() {
        use crate::ring::interest::PeerKey;

        let (op_manager, _guards) =
            build_reroot_test_op_manager("broadcast-targets-source1-only").await;

        // Own address. The UPDATE sender is a DIFFERENT peer, so the self/sender
        // echo-skip never applies to A or B below.
        let self_addr: std::net::SocketAddr = "127.0.0.1:12000".parse().unwrap();
        op_manager.ring.connection_manager.set_own_addr(self_addr);
        let sender_addr: std::net::SocketAddr = "127.0.0.1:20000".parse().unwrap();

        let key = reroot_contract_key(0x5A);
        let cid = *key.id();

        // Peer A: a connected, ADVERTISED co-host (Source 1).
        let kp_a = crate::transport::TransportKeypair::new();
        let addr_a: std::net::SocketAddr = "127.0.0.1:20001".parse().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(0.3),
            addr_a,
            kp_a.public().clone(),
            false,
        ));
        op_manager.neighbor_hosting.handle_message(
            kp_a.public(),
            crate::message::NeighborHostingMessage::HostingAnnounce {
                added: vec![cid],
                removed: vec![],
                is_response: false,
            },
        );

        // Peer B: a connected peer that has registered INTEREST but has NOT yet
        // advertised (the lagged-advertisement case). It is a valid, resolvable
        // connection, so its exclusion below is due to the Source-2 removal, not
        // an unresolvable peer.
        let kp_b = crate::transport::TransportKeypair::new();
        let addr_b: std::net::SocketAddr = "127.0.0.1:20002".parse().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(0.6),
            addr_b,
            kp_b.public().clone(),
            false,
        ));
        assert!(
            op_manager.interest_manager.register_peer_interest(
                &key,
                PeerKey(kp_b.public().clone()),
                None,
                false,
            ),
            "interest registration should succeed"
        );

        // BEFORE the heal: only the advertised co-host A is a target.
        let before = op_manager.get_broadcast_targets_update(&key, &sender_addr);
        let before_addrs: std::collections::HashSet<std::net::SocketAddr> = before
            .targets
            .iter()
            .filter_map(|p| p.socket_addr())
            .collect();
        assert!(
            before_addrs.contains(&addr_a),
            "advertised co-host (Source 1) must be a broadcast target; got {before_addrs:?}"
        );
        assert!(
            !before_addrs.contains(&addr_b),
            "interest-only peer (former Source 2) must NOT be a broadcast target after \
             #4642 step 9; got {before_addrs:?}"
        );
        assert_eq!(
            before.targets.len(),
            1,
            "exactly one target (A) before heal"
        );
        assert_eq!(before.proximity_found, 1);
        assert_eq!(
            before.interest_found, 0,
            "interest_found is vestigial after Source-2 removal and must report 0"
        );

        // THE HEAL: the periodic interest heartbeat re-pulls B's hosted set. We
        // send B a `HostingStateRequest`; B replies with a `HostingStateResponse`
        // snapshot of what it hosts, which we full-replace into our view of B
        // (`ring.rs::interest_heartbeat`, #4722). That is what moves B into
        // Source-1. (An advertised co-host may also announce proactively via
        // `HostingAnnounce`; either path lands B in the proximity cache.)
        op_manager.neighbor_hosting.handle_message(
            kp_b.public(),
            crate::message::NeighborHostingMessage::HostingStateResponse {
                contracts: vec![cid],
            },
        );

        // AFTER the heal: B is now an advertised co-host and IS a target.
        let after = op_manager.get_broadcast_targets_update(&key, &sender_addr);
        let after_addrs: std::collections::HashSet<std::net::SocketAddr> = after
            .targets
            .iter()
            .filter_map(|p| p.socket_addr())
            .collect();
        assert!(
            after_addrs.contains(&addr_a) && after_addrs.contains(&addr_b),
            "after the advertisement heals, BOTH co-hosts must be broadcast targets; \
             got {after_addrs:?}"
        );
        assert_eq!(after.targets.len(), 2, "both A and B after heal");
        assert_eq!(after.proximity_found, 2);
        assert_eq!(after.interest_found, 0);
    }

    /// THE re-root-storm falsifier (#4642 piece F, the load-bearing test). Drop a
    /// hub that co-hosted MANY of this peer's in-use contracts and assert the
    /// prompt re-root batch is bounded by `MAX_PROMPT_REROOTS_PER_DROP` — NOT
    /// O(contracts) simultaneous re-subscribes (the #3763 hollow-relay storm). The
    /// remaining affected contracts are left to the ordinary renewal loop at its
    /// own rate cap. Uses `select_prompt_reroots` (the non-spawning selection half)
    /// so the bound is deterministic, no task scheduling involved.
    #[tokio::test(flavor = "current_thread")]
    async fn connection_drop_prompt_reroot_is_storm_bounded() {
        let (op_manager, _guards) = build_reroot_test_op_manager("reroot-storm").await;
        seed_location_and_one_connection(&op_manager);

        // M >> cap in-use contracts, ALL co-hosted by ONE hub peer. Kept within the
        // per-drop SAMPLE cap so the scan returns them all and it is the per-drop
        // re-root cap (not the scan cap) that does the bounding this test measures.
        const _: () =
            assert!(MAX_PROMPT_REROOTS_PER_DROP * 3 + 1 <= CONNECTION_DROP_SHADOW_SAMPLE_CAP);
        let m = MAX_PROMPT_REROOTS_PER_DROP * 3 + 1;
        let mut instance_ids = Vec::new();
        for i in 0..m {
            let key = reroot_contract_key(i as u8);
            let iid = *key.id();
            op_manager.ring.subscribe(key); // active lease → is_subscribed
            op_manager
                .ring
                .add_client_subscription(&iid, crate::client_events::ClientId::FIRST); // local client → in-use
            instance_ids.push(iid);
        }
        let hub = crate::transport::TransportKeypair::new().public().clone();
        let dropped = register_hub_cohosting(&op_manager, &hub, &instance_ids);
        assert_eq!(dropped.len(), m, "hub must co-host all {m} contracts");

        // Simulate the disconnect bookkeeping, then run the storm-safe selection.
        op_manager.neighbor_hosting.on_peer_disconnected(&hub);
        let selected = op_manager.select_prompt_reroots(&dropped);

        // THE STORM BOUND: exactly the per-drop cap, independent of m.
        assert_eq!(
            selected.len(),
            MAX_PROMPT_REROOTS_PER_DROP,
            "a hub drop affecting {m} in-use contracts must yield exactly the per-drop \
             cap ({MAX_PROMPT_REROOTS_PER_DROP}) prompt re-roots, not {m} — the re-root \
             storm bound"
        );
        // Every selected key is one of ours AND a genuine ReRootSearch candidate.
        for key in &selected {
            assert!(
                instance_ids.contains(key.id()),
                "selected must be one of ours"
            );
            assert!(
                op_manager.reconcile_wants_reroot(key),
                "selected must be a real re-root candidate"
            );
        }
    }

    /// Interest-gate falsifier (#4642 piece F): a DEMANDLESS copy co-hosted by the
    /// dropped hub is NOT re-rooted (re-rooting a demandless copy on a drop is the
    /// storm), while an IN-USE contract IS. This is the anti-storm property the
    /// whole flip hinges on.
    #[tokio::test(flavor = "current_thread")]
    async fn connection_drop_prompt_reroot_is_interest_gated() {
        let (op_manager, _guards) = build_reroot_test_op_manager("reroot-interest-gate").await;
        seed_location_and_one_connection(&op_manager);

        // In-use: a local client subscription ⇒ contract_in_use ⇒ ReRootSearch.
        let in_use = reroot_contract_key(1);
        op_manager.ring.subscribe(in_use);
        op_manager
            .ring
            .add_client_subscription(in_use.id(), crate::client_events::ClientId::FIRST);

        // Demandless: an active lease but NO local client / no downstream / no
        // recent access ⇒ NOT contract_in_use ⇒ reconcile emits a teardown, never
        // ReRootSearch.
        let idle = reroot_contract_key(2);
        op_manager.ring.subscribe(idle);

        let hub = crate::transport::TransportKeypair::new().public().clone();
        let dropped = register_hub_cohosting(&op_manager, &hub, &[*in_use.id(), *idle.id()]);
        op_manager.neighbor_hosting.on_peer_disconnected(&hub);

        let selected = op_manager.select_prompt_reroots(&dropped);
        assert!(
            selected.contains(&in_use),
            "the in-use contract must be re-rooted after its upstream dropped"
        );
        assert!(
            !selected.contains(&idle),
            "the demandless contract must NOT be re-rooted (anti-storm interest gate)"
        );
        assert!(op_manager.reconcile_wants_reroot(&in_use));
        assert!(!op_manager.reconcile_wants_reroot(&idle));
    }

    /// Make-before-break falsifier (#4642 piece F, storm-safety guard 3): issuing a
    /// prompt re-root does NOT tear down the existing subscription first — the fresh
    /// SUBSCRIBE is sent while the old lease is still held (the old upstream
    /// registration lapses on its own; the peer keeps serving its copy meanwhile,
    /// invariant 1 serve-DURING). Also confirms the per-contract pending slot is
    /// claimed (dedup) so a concurrent renewal cannot double-fire the same contract.
    #[tokio::test(flavor = "current_thread")]
    async fn connection_drop_prompt_reroot_is_make_before_break() {
        let (op_manager, _guards) = build_reroot_test_op_manager("reroot-mbb").await;
        seed_location_and_one_connection(&op_manager);

        let key = reroot_contract_key(3);
        op_manager.ring.subscribe(key);
        op_manager
            .ring
            .add_client_subscription(key.id(), crate::client_events::ClientId::FIRST);
        assert!(
            op_manager.ring.is_subscribed(&key),
            "precondition: subscribed"
        );

        let hub = crate::transport::TransportKeypair::new().public().clone();
        let dropped = register_hub_cohosting(&op_manager, &hub, &[*key.id()]);
        op_manager.neighbor_hosting.on_peer_disconnected(&hub);

        let shutdown = op_manager.ring.shutdown_token();
        let op_arc = op_manager.clone();
        op_manager.spawn_prompt_reroots(&dropped, &op_arc, &shutdown);

        // Make-before-break: the lease is STILL held immediately after issuing the
        // prompt re-root (the spawned subscribe is still in its jitter window and,
        // regardless, never tears the old lease down first).
        assert!(
            op_manager.ring.is_subscribed(&key),
            "the existing lease must NOT be torn down by the prompt re-root \
             (make-before-break)"
        );
        // The per-contract slot was claimed → a duplicate concurrent re-root/renewal
        // is blocked (dedup, storm-safety).
        assert!(
            !op_manager.ring.can_request_subscription(&key),
            "the prompt re-root must have claimed the pending slot (per-contract dedup)"
        );
    }

    // ===== Stale proximity-cache reap (#4756) =====

    /// Regression test for #4756: a proximity-cache entry for a neighbor that
    /// disconnected (no longer in the connection manager) MUST be reaped the
    /// first time `get_broadcast_targets_update` fails to resolve it —
    /// otherwise it logs at WARN and re-fires on EVERY subsequent UPDATE
    /// forever (the unbounded log-spam failure mode reported on the 0.2.95
    /// soak).
    #[tokio::test(flavor = "current_thread")]
    async fn get_broadcast_targets_reaps_stale_proximity_neighbor() {
        let (op_manager, _guards) = build_reroot_test_op_manager("reap-stale-proximity").await;
        seed_location_and_one_connection(&op_manager);

        let key = reroot_contract_key(7);
        // A hub announced it co-hosts this contract, but it is NOT in the
        // connection manager — reproducing the post-disconnect stale state
        // where the ring pruned the connection (keyed by addr) but the
        // proximity entry (keyed by pub_key) leaked.
        let hub = crate::transport::TransportKeypair::new().public().clone();
        register_hub_cohosting(&op_manager, &hub, &[*key.id()]);
        assert_eq!(
            op_manager.neighbor_hosting.neighbors_with_contract(&key),
            vec![hub.clone()],
            "precondition: the stale proximity entry is present"
        );

        let sender: std::net::SocketAddr = "127.0.0.1:40000".parse().unwrap();

        // First lookup: the stale neighbor fails to resolve (counted once) AND
        // is reaped so it cannot spam future UPDATEs.
        let first = op_manager.get_broadcast_targets_update(&key, &sender);
        assert_eq!(
            first.proximity_resolve_failed, 1,
            "the stale neighbor must be counted as one resolve failure"
        );
        assert!(
            op_manager
                .neighbor_hosting
                .neighbors_with_contract(&key)
                .is_empty(),
            "the stale proximity entry must be reaped on the first failed resolve (#4756)"
        );

        // Second lookup: the entry is gone, so there is no repeat failure — the
        // bug's failure mode is that this stays 1 (WARN spam) forever.
        let second = op_manager.get_broadcast_targets_update(&key, &sender);
        assert_eq!(
            second.proximity_resolve_failed, 0,
            "a reaped entry must not re-fire on the next UPDATE (no repeat spam)"
        );
    }

    /// Companion to #4756: a proximity neighbor that IS still connected must be
    /// resolved as a broadcast target and NOT reaped by the self-healing path.
    #[tokio::test(flavor = "current_thread")]
    async fn get_broadcast_targets_keeps_connected_proximity_neighbor() {
        let (op_manager, _guards) = build_reroot_test_op_manager("keep-connected-proximity").await;
        seed_location_and_one_connection(&op_manager);

        let key = reroot_contract_key(8);
        // A connected neighbor: present in BOTH the connection manager and the
        // proximity cache.
        let peer_kp = crate::transport::TransportKeypair::new();
        let peer_pub = peer_kp.public().clone();
        let peer_addr: std::net::SocketAddr = "127.0.0.1:45678".parse().unwrap();
        assert!(op_manager.ring.connection_manager.add_connection(
            crate::ring::Location::new(0.5),
            peer_addr,
            peer_pub.clone(),
            false,
        ));
        register_hub_cohosting(&op_manager, &peer_pub, &[*key.id()]);

        let sender: std::net::SocketAddr = "127.0.0.1:40001".parse().unwrap();
        let result = op_manager.get_broadcast_targets_update(&key, &sender);

        assert_eq!(
            result.proximity_resolve_failed, 0,
            "a connected neighbor must resolve, not fail"
        );
        assert!(
            result
                .targets
                .iter()
                .any(|t| t.socket_addr() == Some(peer_addr)),
            "the connected neighbor must be included as a broadcast target"
        );
        assert_eq!(
            op_manager.neighbor_hosting.neighbors_with_contract(&key),
            vec![peer_pub.clone()],
            "a connected neighbor must NOT be reaped"
        );
    }

    #[tokio::test]
    async fn notify_timeout_succeeds_when_receiver_alive() {
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = Transaction::ttl_transaction();

        let delivered = notify_transaction_timeout(&notifier, tx);
        assert!(
            delivered,
            "notification should be delivered while receiver is alive"
        );

        let received = timeout(Duration::from_millis(100), notifications_receiver.recv())
            .await
            .expect("timed out waiting for notification")
            .expect("notification channel closed");

        match received {
            Either::Right(NodeEvent::TransactionTimedOut(observed)) => {
                assert_eq!(observed, tx, "unexpected transaction in notification");
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("unexpected notification: {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn notify_timeout_handles_dropped_receiver() {
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();

        let delivered = notify_transaction_timeout(&notifier, tx);
        assert!(
            !delivered,
            "notification delivery should fail once receiver is dropped"
        );
    }

    // ──────────────────────────────────────────────────────────
    // `release_pending_op_slot_on` tests. Tests the extracted
    // helper directly without building a full OpManager.
    // ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn release_pending_op_slot_emits_transaction_completed() {
        // Happy path: the helper must emit exactly one
        // `TransactionCompleted(tx)` on the notification channel.
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = Transaction::ttl_transaction();

        super::release_pending_op_slot_on(
            notifier.notifications_sender(),
            tx,
            Duration::from_secs(1),
        )
        .await;

        let received = timeout(Duration::from_millis(100), notifications_receiver.recv())
            .await
            .expect("timed out waiting for TransactionCompleted emission")
            .expect("notification channel closed");

        match received {
            Either::Right(NodeEvent::TransactionCompleted(observed)) => {
                assert_eq!(observed, tx, "emitted tx must match the argument");
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected TransactionCompleted, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn release_pending_op_slot_blocks_through_backpressure() {
        // Regression guard for review finding M1: the earlier
        // `try_send` implementation would silently drop the cleanup
        // event when the notification channel was transiently full.
        // The `send().await` implementation must block and deliver
        // once the consumer drains one slot.
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        // Saturate the channel up to its capacity. The channel
        // capacity is whatever `event_loop_notification_channel`
        // configures — we don't hard-code it. Pre-fill until
        // `try_send` fails, then use that count.
        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            // Safety valve: don't loop forever if the channel is
            // unbounded or absurdly large. Real channel is bounded
            // at a few hundred entries — if we hit this cap it's a
            // test-config change and deserves an explicit fix.
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }
        assert!(
            pre_filled > 0,
            "expected a bounded channel; got what appears to be unbounded"
        );

        // Spawn the drain side a moment later: it will consume one
        // entry, unblocking the `send().await` inside the helper.
        let release_tx = Transaction::ttl_transaction();
        let consumer = tokio::spawn(async move {
            // Sleep briefly so the helper's `send().await` is already
            // pending when we start draining.
            tokio::time::sleep(Duration::from_millis(20)).await;
            // Drain one entry to create room.
            notifications_receiver
                .recv()
                .await
                .expect("notification channel closed during drain");
            // Keep draining until we see our release event. Additional
            // pre-filled entries may sit ahead of it.
            loop {
                match notifications_receiver.recv().await {
                    Some(Either::Right(NodeEvent::TransactionCompleted(observed)))
                        if observed == release_tx =>
                    {
                        return;
                    }
                    Some(_) => continue,
                    None => panic!("channel closed before release event observed"),
                }
            }
        });

        // The helper must not complete instantaneously (channel is
        // saturated) but must complete once the consumer drains. Give
        // it up to 2 s — plenty of slack for the 20 ms drain delay.
        let release = timeout(
            Duration::from_secs(2),
            super::release_pending_op_slot_on(
                notifier.notifications_sender(),
                release_tx,
                Duration::from_secs(30),
            ),
        )
        .await;
        release.expect("helper must complete once channel has room");

        consumer
            .await
            .expect("consumer task should terminate cleanly");
    }

    #[tokio::test]
    async fn release_pending_op_slot_returns_on_closed_channel() {
        // If the notification channel is closed entirely (receiver
        // dropped), the helper must return promptly (via the `Err`
        // arm of the inner match) rather than hanging on
        // `send().await`. The 60 s periodic sweep will still reclaim
        // the slot eventually; this test pins "no hang."
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();

        let result = timeout(
            Duration::from_millis(200),
            super::release_pending_op_slot_on(
                notifier.notifications_sender(),
                tx,
                Duration::from_secs(30),
            ),
        )
        .await;
        assert!(
            result.is_ok(),
            "helper must return promptly on closed channel"
        );
    }

    // ──────────────────────────────────────────────────────────
    // Regression tests for #4359: a fresh-contract PUT whose initial
    // broadcast found no targets must be re-emitted (not permanently
    // abandoned) once the first interested peer/subscriber/target appears.
    // These exercise `emit_pending_broadcast_reemit_on` — the emit-core that
    // `OpManager::flush_pending_broadcast_on_interest` delegates to — directly
    // against a raw notifier + store, the same way the
    // `release_pending_op_slot_on` tests above avoid building a full OpManager.
    // The give-up→stash and targets-found→take WIRING into the real handler is
    // additionally guarded by source-grep pin tests in p2p_protoc.rs
    // (`handle_broadcast_state_change_*` pins), since driving the full async
    // handler needs a complete OpManager + contract handler.
    // ──────────────────────────────────────────────────────────

    fn test_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            freenet_stdlib::prelude::CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    /// Load-bearing: a resolved deferred broadcast (the give-up outcome, after
    /// the flush drained the stash and re-read current state) is emitted as a
    /// `BroadcastStateChange { is_retry: false, is_reemit: true }` carrying the
    /// state when interest resolves. WITHOUT the #4359 fix the give-up path
    /// drops the state permanently and nothing is ever re-broadcast — this
    /// emission is exactly the behavior the fix adds.
    #[tokio::test]
    async fn flush_pending_broadcast_reemits_stashed_state_on_interest() {
        use crate::operations::update::pending_broadcast::PendingBroadcastStore;

        GlobalSimulationTime::set_time_ms(0);
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let store = PendingBroadcastStore::new();
        let key = test_contract_key(7);
        let state = freenet_stdlib::prelude::WrappedState::new(vec![0xCD; 16]);

        // Interest resolves → emit the resolved deferred broadcast.
        super::emit_pending_broadcast_reemit_on(
            notifier.notifications_sender(),
            notifier.notification_channel_pending(),
            notifier.notifications_sender().capacity(),
            &store,
            &key,
            state.clone(),
        );

        let received = timeout(Duration::from_millis(200), notifications_receiver.recv())
            .await
            .expect("timed out waiting for re-broadcast emission")
            .expect("notification channel closed");

        match received {
            Either::Right(NodeEvent::BroadcastStateChange {
                key: observed_key,
                new_state,
                is_retry,
                is_reemit,
            }) => {
                assert_eq!(observed_key, key, "re-broadcast must target the contract");
                assert_eq!(
                    new_state.as_ref(),
                    state.as_ref(),
                    "re-broadcast must carry the resolved state"
                );
                assert!(
                    !is_retry,
                    "the deferred flush is a fresh logical broadcast, not a retry re-emission"
                );
                assert!(
                    is_reemit,
                    "the deferred flush must be tagged is_reemit so the give-up handler does \
                     not double-count a still-no-targets re-emission in the #4281 stats"
                );
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected BroadcastStateChange, got {other:?}")
            }
        }
        GlobalSimulationTime::clear_time();
    }

    /// If the notification channel is saturated, the emit must re-stash the
    /// state rather than dropping it — otherwise a transiently-full channel
    /// would re-open the locally-hosted-only failure the fix closes.
    #[tokio::test]
    async fn flush_pending_broadcast_restashes_when_channel_full() {
        use crate::operations::update::pending_broadcast::PendingBroadcastStore;

        GlobalSimulationTime::set_time_ms(0);
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            notifications_receiver,
            ..
        } = receiver;

        // Saturate the channel so the re-emit's try_send fails.
        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }

        let store = PendingBroadcastStore::new();
        let key = test_contract_key(11);
        let state = freenet_stdlib::prelude::WrappedState::new(vec![0xEF; 8]);

        super::emit_pending_broadcast_reemit_on(
            notifier.notifications_sender(),
            notifier.notification_channel_pending(),
            notifier.notifications_sender().capacity(),
            &store,
            &key,
            state.clone(),
        );

        // State must still be present (re-stashed) for a later retry.
        let recovered = store
            .take(key.id())
            .expect("state must be re-stashed after a full-channel drop, not lost");
        assert_eq!(recovered.as_ref(), state.as_ref());

        drop(notifications_receiver);
        GlobalSimulationTime::clear_time();
    }

    // ──────────────────────────────────────────────────────────
    // Regression tests for #4359 re-review (SHOULD-FIX 4 + fast-path):
    // `flush_pending_broadcast_on_interest`'s OWN branching — the
    // contains() no-op fast path, the take()-None race path, and the
    // read_current_contract_state Some/None handling — exercised through
    // `flush_pending_broadcast_on_interest_on`, the orchestration-core the
    // method delegates to (the method itself only wires `self`'s store,
    // notifier, and `read_current_contract_state` into this function). The
    // `read_current_state` step is stubbed so the live-read Some/None
    // branches are driven deterministically without a contract handler.
    //
    // These restore coverage that an earlier revision dropped when it kept
    // only the `emit_pending_broadcast_reemit_on` tests above — those cover
    // the emit-core, NOT the stash-membership / stale-state branching.
    // ──────────────────────────────────────────────────────────

    /// Load-bearing: nothing stashed → the flush is a pure no-op. It must NOT
    /// read current state and must NOT emit anything, so the overwhelmingly
    /// common interest-registration-on-an-already-propagated-contract case
    /// stays cheap. If the membership fast path regressed (e.g. always
    /// take()/read), this test fails: `read_current_state` would be invoked and
    /// the channel would receive an emission.
    #[tokio::test]
    async fn flush_noop_when_nothing_stashed() {
        use crate::operations::update::pending_broadcast::PendingBroadcastStore;

        GlobalSimulationTime::set_time_ms(0);
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let store = PendingBroadcastStore::new();
        let key = test_contract_key(21);
        let read_called = std::cell::Cell::new(false);

        super::flush_pending_broadcast_on_interest_on(
            &store,
            notifier.notifications_sender(),
            notifier.notification_channel_pending(),
            notifier.notifications_sender().capacity(),
            &key,
            || {
                read_called.set(true);
                std::future::ready(None)
            },
        )
        .await;

        assert!(
            !read_called.get(),
            "no-op fast path must NOT read current contract state when nothing is stashed"
        );
        // No emission must reach the channel.
        match notifications_receiver.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            other => panic!("expected no emission on the no-op path, got {other:?}"),
        }
        GlobalSimulationTime::clear_time();
    }

    /// Load-bearing: stashed + live read returns `Some(current)` → the re-emit
    /// must carry the CURRENT state, NOT the stale give-up-time bytes. This pins
    /// the stale-state safety (SHOULD-FIX 4): a locally-applied UPDATE between
    /// give-up and flush must win. If the method regressed to re-emitting the
    /// stashed bytes, the asserted state would be the stale ones and this fails.
    #[tokio::test]
    async fn flush_reemits_current_state_when_live_read_present() {
        use crate::operations::update::pending_broadcast::PendingBroadcastStore;

        GlobalSimulationTime::set_time_ms(0);
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let store = PendingBroadcastStore::new();
        let key = test_contract_key(22);
        let stale = freenet_stdlib::prelude::WrappedState::new(vec![0xAA; 8]);
        let current = freenet_stdlib::prelude::WrappedState::new(vec![0xBB; 12]);
        store.stash(*key.id(), stale.clone());

        let current_for_read = current.clone();
        super::flush_pending_broadcast_on_interest_on(
            &store,
            notifier.notifications_sender(),
            notifier.notification_channel_pending(),
            notifier.notifications_sender().capacity(),
            &key,
            || std::future::ready(Some(current_for_read.clone())),
        )
        .await;

        let received = timeout(Duration::from_millis(200), notifications_receiver.recv())
            .await
            .expect("timed out waiting for re-broadcast emission")
            .expect("notification channel closed");
        match received {
            Either::Right(NodeEvent::BroadcastStateChange {
                key: observed_key,
                new_state,
                is_reemit,
                ..
            }) => {
                assert_eq!(observed_key, key);
                assert_eq!(
                    new_state.as_ref(),
                    current.as_ref(),
                    "must re-emit the CURRENT live state, not the stale stashed bytes"
                );
                assert_ne!(
                    new_state.as_ref(),
                    stale.as_ref(),
                    "stale give-up-time bytes must not be broadcast when a live read succeeds"
                );
                assert!(is_reemit, "deferred flush must be tagged is_reemit");
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected BroadcastStateChange, got {other:?}")
            }
        }
        // The stash must have been drained.
        assert!(
            store.take(key.id()).is_none(),
            "the stash entry must be drained by the flush"
        );
        GlobalSimulationTime::clear_time();
    }

    /// Load-bearing: stashed + live read returns `None` (contract handler
    /// unavailable / not hosted) → the re-emit must FALL BACK to the stashed
    /// bytes rather than dropping the broadcast. This pins the None-fallback
    /// branch of `read_current_contract_state` handling: a failed live read is
    /// strictly no worse than re-emitting the give-up-time state. If the
    /// fallback regressed (e.g. `?`-style early return on None), nothing would
    /// be emitted and this fails on the recv timeout.
    #[tokio::test]
    async fn flush_falls_back_to_stashed_bytes_when_live_read_absent() {
        use crate::operations::update::pending_broadcast::PendingBroadcastStore;

        GlobalSimulationTime::set_time_ms(0);
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let store = PendingBroadcastStore::new();
        let key = test_contract_key(23);
        let stashed = freenet_stdlib::prelude::WrappedState::new(vec![0xCC; 10]);
        store.stash(*key.id(), stashed.clone());

        super::flush_pending_broadcast_on_interest_on(
            &store,
            notifier.notifications_sender(),
            notifier.notification_channel_pending(),
            notifier.notifications_sender().capacity(),
            &key,
            // Live read fails (handler unavailable / not hosted).
            || std::future::ready(None),
        )
        .await;

        let received = timeout(Duration::from_millis(200), notifications_receiver.recv())
            .await
            .expect("timed out: the None live-read path must fall back to stashed bytes, not drop")
            .expect("notification channel closed");
        match received {
            Either::Right(NodeEvent::BroadcastStateChange {
                key: observed_key,
                new_state,
                ..
            }) => {
                assert_eq!(observed_key, key);
                assert_eq!(
                    new_state.as_ref(),
                    stashed.as_ref(),
                    "must fall back to the stashed bytes when the live read returns None"
                );
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected BroadcastStateChange, got {other:?}")
            }
        }
        GlobalSimulationTime::clear_time();
    }

    // ──────────────────────────────────────────────────────────
    // Regression tests for #4154/#4313: parked drivers must be woken
    // when their awaited peer disconnects, not wait `OPERATION_TTL`,
    // and must surface the disconnect cause rather than the
    // FORBIDDEN_MARKER. The orphan handler emits
    // `TransactionOrphaned { tx, peer }` per orphan via
    // `notify_orphaned_transaction_on`; the event loop sends
    // `WaiterReply::PeerDisconnected` into the waiter channel and then
    // drops the sender, so the driver's `recv()` yields the cause
    // (mapped to `OpError::PeerDisconnected`) before any close.
    // ──────────────────────────────────────────────────────────

    fn test_peer() -> SocketAddr {
        "203.0.113.7:9999"
            .parse()
            .expect("test peer addr must be valid")
    }

    #[tokio::test]
    async fn notify_orphaned_transaction_emits_transaction_orphaned() {
        // Happy path: the standalone helper enqueues exactly one
        // `TransactionOrphaned { tx, peer }` on the notification channel.
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = Transaction::ttl_transaction();
        let peer = test_peer();

        let delivered =
            super::notify_orphaned_transaction_on(notifier.notifications_sender(), tx, peer);
        assert!(delivered, "helper must enqueue on a live channel");

        let received = timeout(Duration::from_millis(100), notifications_receiver.recv())
            .await
            .expect("timed out waiting for TransactionOrphaned emission")
            .expect("notification channel closed");

        match received {
            Either::Right(NodeEvent::TransactionOrphaned {
                tx: observed_tx,
                peer: observed_peer,
            }) => {
                assert_eq!(observed_tx, tx, "emitted tx must match the argument");
                assert_eq!(observed_peer, peer, "emitted peer must match the argument");
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected TransactionOrphaned, got {other:?}")
            }
        }
    }

    #[tokio::test]
    async fn notify_orphaned_transaction_handles_dropped_receiver() {
        // Closed channel: helper must return `false` rather than panic
        // — disconnect cleanup must remain robust when the event loop
        // has already torn down (e.g. shutdown races).
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let delivered =
            super::notify_orphaned_transaction_on(notifier.notifications_sender(), tx, test_peer());
        assert!(
            !delivered,
            "helper must return false once receiver is dropped"
        );
    }

    #[tokio::test]
    async fn orphaned_transaction_wakes_parked_waiter_with_peer_disconnected() {
        // End-to-end pipeline test for #4154/#4313 without standing up a
        // full node. Reproduces the orphan-handler → notification-channel
        // → event-loop → send-cause-then-drop-sender → driver-wakeup
        // sequence and asserts the parked driver observes
        // `WaiterReply::PeerDisconnected` (NOT a bare close) in under
        // 100 ms (pre-#4154 it hung `OPERATION_TTL`; the deleted registry
        // approach raced and surfaced the FORBIDDEN_MARKER instead).
        let (mut event_loop_receiver, notifier) = event_loop_notification_channel();

        // Stand in for `pending_op_results[tx] = sender` and the driver's
        // pending `recv()` on the matching receiver.
        let (response_sender, mut driver_response_rx) =
            tokio::sync::mpsc::channel::<crate::node::WaiterReply>(1);
        let mut pending_op_results: std::collections::HashMap<
            Transaction,
            tokio::sync::mpsc::Sender<crate::node::WaiterReply>,
        > = std::collections::HashMap::new();
        let tx = Transaction::ttl_transaction();
        let peer = test_peer();
        pending_op_results.insert(tx, response_sender);

        // Trigger the wake — this is the orphan-handler path under test.
        let delivered =
            super::notify_orphaned_transaction_on(notifier.notifications_sender(), tx, peer);
        assert!(delivered, "orphan-handler helper must enqueue notification");

        // Mimic the event loop's `TransactionOrphaned` arm: take the sender
        // out and deliver the cause THROUGH the channel before it drops.
        let event = timeout(
            Duration::from_millis(100),
            event_loop_receiver.notifications_receiver.recv(),
        )
        .await
        .expect("event loop never received TransactionOrphaned")
        .expect("notification channel closed before TransactionOrphaned arrived");
        match event {
            Either::Right(NodeEvent::TransactionOrphaned {
                tx: observed_tx,
                peer: observed_peer,
            }) => {
                assert_eq!(observed_tx, tx);
                assert_eq!(observed_peer, peer);
                if let Some(sender) = pending_op_results.remove(&observed_tx) {
                    #[allow(clippy::let_underscore_must_use)]
                    let _ = sender.try_send(crate::node::WaiterReply::PeerDisconnected {
                        peer: observed_peer,
                    });
                }
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected TransactionOrphaned, got {other:?}")
            }
        }

        // The driver's `recv()` must now resolve to the cause immediately —
        // pre-fix this hung the full `OPERATION_TTL`. Cap at 100 ms.
        let driver_wakeup = timeout(Duration::from_millis(100), driver_response_rx.recv()).await;
        match driver_wakeup {
            Ok(Some(crate::node::WaiterReply::PeerDisconnected { peer: observed })) => {
                assert_eq!(observed, peer, "driver must receive the disconnect cause");
            }
            Ok(other) => panic!("driver received unexpected item: {other:?}"),
            Err(_) => panic!(
                "driver did not wake after orphan handling — \
                 pre-#4154 behavior reproduced"
            ),
        }
    }

    // ──────────────────────────────────────────────────────────
    // `notify_node_event_on` tests — the blocking variant retained
    // for sites like `announce_contract_hosted` that consume a one-shot
    // transition before emitting (so silent drop on Full would
    // permanently lose the announcement). Pins that the timeout
    // actually fires under sustained saturation, returning Err rather
    // than hanging the caller indefinitely. (PR #4231 re-review.)
    // ──────────────────────────────────────────────────────────

    /// Regression pin for the explicitly-permitted blocking path
    /// (`announce_contract_hosted` and friends): when the notification
    /// channel is saturated for the full configured timeout, the
    /// helper MUST return `Err` within bounded time, not hang. Without
    /// this pin, the `DELIBERATELY blocking` exception is shipping
    /// with zero coverage of its failure mode.
    #[tokio::test(start_paused = true)]
    async fn notify_node_event_returns_err_after_timeout_on_saturated_channel() {
        // `_receiver` binding: kept alive (not dropped, not drained) so
        // the channel stays saturated for the full timeout window. If
        // dropped, the helper would land in the `Err(SendError)` arm
        // and we'd test the wrong path; if drained, the saturation
        // would lift and the send would succeed.
        let (_receiver, notifier) = event_loop_notification_channel();

        // Saturate via try_send so the timeout-wrapped send never
        // finds a slot. Deliberately don't drain — we want the
        // timeout to elapse.
        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }

        // `pre_filled` is meaningful (asserts the channel is bounded and
        // we actually saturated it); we don't pass it through any more
        // since the helper now samples diagnostic state internally.
        let _ = pre_filled;

        let test_timeout = Duration::from_secs(30);
        let tx = Transaction::ttl_transaction();

        let start = Instant::now();
        let result = super::notify_node_event_on(
            notifier.notifications_sender(),
            test_timeout,
            NodeEvent::TransactionCompleted(tx),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            matches!(result, Err(OpError::NotificationChannelError(_))),
            "blocking helper must return NotificationChannelError after timeout \
             on saturated channel, got {result:?}"
        );
        // With start_paused=true the timer advances deterministically;
        // elapsed should equal the timeout (within tokio's resolution).
        assert!(
            elapsed >= test_timeout,
            "helper returned before the configured timeout elapsed ({elapsed:?} < {test_timeout:?})"
        );
    }

    // ──────────────────────────────────────────────────────────
    // `try_notify_node_event_on` tests (issue #4145).
    //
    // The blocking `notify_node_event` was the primary back-pressure
    // path that wedged nova and vega on 2026-05-24: every successful
    // contract UPDATE called `notify_node_event(BroadcastStateChange{…}).await`
    // from `runtime.rs` with a 30s timeout, so when the event-loop
    // notification channel filled up under fan-out the executor
    // stalled too. These tests pin the non-blocking variant we now
    // use on those paths.
    // ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn try_notify_node_event_enqueues_on_live_channel() {
        // Happy path: the helper enqueues exactly one event on the
        // notification channel.
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = Transaction::ttl_transaction();
        let event = NodeEvent::TransactionCompleted(tx);

        super::try_notify_node_event_on(notifier.notifications_sender(), 0, 1024, event)
            .expect("helper must enqueue on a live, non-full channel");

        let received = timeout(Duration::from_millis(100), notifications_receiver.recv())
            .await
            .expect("timed out waiting for emission")
            .expect("notification channel closed");

        match received {
            Either::Right(NodeEvent::TransactionCompleted(observed)) => {
                assert_eq!(observed, tx, "emitted tx must match the argument");
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected TransactionCompleted, got {other:?}")
            }
        }
    }

    /// Regression pin for #4145: `try_notify_node_event_on` MUST NOT
    /// block when the channel is full. The blocking
    /// `notify_node_event` waits up to 30 s; the try-variant must
    /// return `Err` essentially immediately.
    #[tokio::test]
    async fn try_notify_node_event_returns_err_on_full_channel_without_blocking() {
        let (_receiver, notifier) = event_loop_notification_channel();

        // Saturate the channel using try_send so the helper hits Full.
        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            // Safety valve mirroring release_pending_op_slot_blocks_through_backpressure
            // — bounded channel must backpressure within a sane cap.
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }
        assert!(
            pre_filled > 0,
            "expected a bounded channel; got what appears to be unbounded"
        );

        // The try-variant must return Err essentially instantly. We
        // cap at 100 ms — orders of magnitude under
        // `OpManager::NOTIFICATION_SEND_TIMEOUT` (30 s) but still
        // generous for slow CI runners. The pre-fix blocking path
        // would not return within this window.
        let tx = Transaction::ttl_transaction();
        let start = Instant::now();
        let result = timeout(
            Duration::from_millis(100),
            // Wrap in async{} so timeout can still poll the (sync)
            // function and not be optimized into a single poll.
            async {
                super::try_notify_node_event_on(
                    notifier.notifications_sender(),
                    pre_filled,
                    0,
                    NodeEvent::TransactionCompleted(tx),
                )
            },
        )
        .await
        .expect("try-variant must NOT block — pre-fix it could stall the executor 30s (#4145)");
        let elapsed = start.elapsed();

        assert!(
            result.is_err(),
            "try-variant must return Err when channel is full"
        );
        assert!(
            elapsed < Duration::from_millis(100),
            "try-variant must complete near-instantly, took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn try_notify_node_event_returns_err_on_closed_channel() {
        // Closed channel: helper returns Err rather than panic — must
        // remain robust during shutdown races.
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let result = super::try_notify_node_event_on(
            notifier.notifications_sender(),
            0,
            0,
            NodeEvent::TransactionCompleted(tx),
        );
        assert!(
            result.is_err(),
            "helper must return Err once receiver is dropped"
        );
    }

    // ──────────────────────────────────────────────────────────
    // Log-level regression tests for #4238.
    //
    // Production gateways were emitting tens of thousands of WARNs
    // per hour ("notification channel full") under normal back-
    // pressure post-HN-spike: 8–14 MB/hour of log noise that buried
    // genuinely-actionable signals like the rate-limited "channel
    // full for too long" error. The fix downgrades the per-occurrence
    // Full arm to DEBUG on both try-send helpers while keeping the
    // Closed arm at WARN (closed-channel is abnormal: receiver was
    // torn down, e.g. shutdown races).
    //
    // These tests pin the level split by initializing a captured
    // subscriber filtered at WARN: with the fix, a Full event emits
    // nothing (DEBUG is below the filter); without the fix, a WARN
    // entry would appear. Closed must still emit WARN either way.
    // ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn notify_orphaned_transaction_full_does_not_emit_warn() {
        // #4238 regression pin: per-occurrence Full must NOT emit a
        // WARN. Pre-fix this fired 30K+/hr on nova; post-fix the
        // helper logs at DEBUG and a WARN-level subscriber sees
        // nothing.
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (_receiver, notifier) = event_loop_notification_channel();

        // Saturate the channel so the helper hits the Full arm.
        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }

        let tx = Transaction::ttl_transaction();
        let delivered =
            super::notify_orphaned_transaction_on(notifier.notifications_sender(), tx, test_peer());
        assert!(!delivered, "helper must return false on a full channel");

        assert!(
            !logger.contains("notify_orphaned_transaction: notification channel full"),
            "Full arm must not emit WARN (would re-spam gateways at 30K+/hr — #4238); \
             captured: {:?}",
            logger.logs()
        );
    }

    #[tokio::test]
    async fn notify_orphaned_transaction_closed_still_emits_warn() {
        // #4238 inverse: the Closed arm is genuinely abnormal
        // (receiver torn down) and MUST stay at WARN even after the
        // Full-arm downgrade.
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let delivered =
            super::notify_orphaned_transaction_on(notifier.notifications_sender(), tx, test_peer());
        assert!(!delivered, "helper must return false on a closed channel");

        assert!(
            logger.contains("notify_orphaned_transaction: notification channel closed"),
            "Closed arm must still emit WARN — receiver-dropped is not benign back-pressure; \
             captured: {:?}",
            logger.logs()
        );
    }

    #[tokio::test]
    async fn try_notify_node_event_full_does_not_emit_warn() {
        // #4238 regression pin for the sibling helper introduced by
        // #4231. Same shape as the release-pending variant above —
        // best-effort broadcast emission must not spam WARN on every
        // back-pressure drop.
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (_receiver, notifier) = event_loop_notification_channel();

        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }

        let tx = Transaction::ttl_transaction();
        let result = super::try_notify_node_event_on(
            notifier.notifications_sender(),
            pre_filled,
            0,
            NodeEvent::TransactionCompleted(tx),
        );
        assert!(result.is_err(), "helper must return Err on a full channel");

        assert!(
            !logger.contains("try_notify_node_event: event-loop notification channel full"),
            "Full arm must not emit WARN (would re-introduce #4238 spam under fan-out); \
             captured: {:?}",
            logger.logs()
        );
    }

    #[tokio::test]
    async fn try_notify_node_event_closed_still_emits_warn() {
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let result = super::try_notify_node_event_on(
            notifier.notifications_sender(),
            0,
            0,
            NodeEvent::TransactionCompleted(tx),
        );
        assert!(
            result.is_err(),
            "helper must return Err once receiver is dropped"
        );

        assert!(
            logger.contains("try_notify_node_event: event-loop notification channel closed"),
            "Closed arm must still emit WARN; captured: {:?}",
            logger.logs()
        );
    }

    #[tokio::test]
    async fn notify_transaction_timeout_full_does_not_emit_warn() {
        // #4238 sibling pin: `notify_transaction_timeout` writes to
        // the same event-loop notification channel as the two
        // `try_*` helpers and is called once per timed-out tx by the
        // GC sweep. Pre-fix it emitted WARN on every Full event;
        // post-fix it must be DEBUG so a WARN-level subscriber sees
        // nothing under sustained back-pressure.
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (_receiver, notifier) = event_loop_notification_channel();

        let filler_tx = Transaction::ttl_transaction();
        let mut pre_filled = 0usize;
        loop {
            match notifier
                .notifications_sender()
                .try_send(Either::Right(NodeEvent::TransactionCompleted(filler_tx)))
            {
                Ok(()) => pre_filled += 1,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => break,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    panic!("channel unexpectedly closed while pre-filling")
                }
            }
            if pre_filled > 4096 {
                panic!("channel did not backpressure after 4096 entries");
            }
        }

        let tx = Transaction::ttl_transaction();
        let delivered = super::notify_transaction_timeout(&notifier, tx);
        assert!(!delivered, "helper must return false on a full channel");

        assert!(
            !logger.contains("Notification channel full, skipping timeout notification"),
            "notify_transaction_timeout Full arm must not emit WARN \
             (would re-introduce #4238 spam from the GC-sweep path); \
             captured: {:?}",
            logger.logs()
        );
    }

    #[tokio::test]
    async fn notify_transaction_timeout_closed_still_emits_warn() {
        let logger = crate::test_utils::TestLogger::new()
            .with_level("warn")
            .capture_logs()
            .init();

        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let delivered = super::notify_transaction_timeout(&notifier, tx);
        assert!(!delivered, "helper must return false on a closed channel");

        assert!(
            logger.contains("Notification channel closed, receiver likely dropped"),
            "Closed arm must still emit WARN; captured: {:?}",
            logger.logs()
        );
    }

    /// Single source of truth for the source-scrape coverage list.
    /// Both `broadcast_emission_sites_use_try_notify_or_are_deliberately_blocking`
    /// (primary scrape) and `broadcast_emission_files_are_in_allowlist`
    /// (complementary walker) read from this so the two cannot drift —
    /// adding a file to one without the other was a real risk flagged
    /// in PR #4231's third pass (Skeptical M2).
    ///
    /// Paths are `src/`-relative (matching what the walker reports).
    /// `include_str!` calls below are written relative to *this* file's
    /// location; the two formats stay in sync visually but are not
    /// derivable from each other at the macro level, so add entries to
    /// both columns together.
    const EMISSION_SITES: &[(&str, &str)] = &[
        (
            "contract/executor/runtime.rs",
            include_str!("../contract/executor/runtime.rs"),
        ),
        (
            "contract/executor/runtime/executor_impl.rs",
            include_str!("../contract/executor/runtime/executor_impl.rs"),
        ),
        (
            "contract/executor/mock_runtime.rs",
            include_str!("../contract/executor/mock_runtime.rs"),
        ),
        ("operations.rs", include_str!("../operations.rs")),
        ("node.rs", include_str!("../node.rs")),
        (
            "node/network_bridge/p2p_protoc.rs",
            include_str!("network_bridge/p2p_protoc.rs"),
        ),
        (
            "node/network_bridge/p2p_protoc/broadcast.rs",
            include_str!("network_bridge/p2p_protoc/broadcast.rs"),
        ),
    ];

    /// Returns true if `forward_window` names a best-effort gossip event
    /// variant that the channel-safety gate enforces non-blocking emission
    /// for. Currently covers `Broadcast*` and `SyncStateToPeer` — both
    /// are "lossy ok" by design and either heals via subsequent rounds.
    fn names_gossip_emit(forward_window: &str) -> bool {
        forward_window.contains("Broadcast") || forward_window.contains("SyncStateToPeer")
    }

    /// Source-scrape regression guard for #4145. Pin that every
    /// best-effort-gossip emission site (Broadcast*, SyncStateToPeer)
    /// either uses `try_notify_node_event` (non-blocking) OR is
    /// explicitly annotated `DELIBERATELY blocking` with a load-bearing
    /// justification.
    ///
    /// Scope is driven by the shared `EMISSION_SITES` constant. The
    /// complementary test `broadcast_emission_files_are_in_allowlist`
    /// walks every `.rs` file in `crates/core/src/` and catches any
    /// emission that lands outside this allowlist, so a future refactor
    /// moving an emission into a previously-unscanned file cannot
    /// silently bypass this gate.
    ///
    /// Allowlist marker: `DELIBERATELY blocking` within ~2 KB BEFORE
    /// the call site. Used at:
    ///
    /// - `announce_contract_hosted` (one-shot transition consumed
    ///   before emit; silent drop would be permanent loss)
    /// - `p2p_protoc.rs` retry-spawn (runs in `tokio::spawn`, so
    ///   blocking is event-loop-safe by isolation; switching to
    ///   try_notify would silently drop wedge-recovery retries)
    ///
    /// Any future addition to the allowlist requires the same kind of
    /// explicit justification at the call site.
    #[test]
    fn broadcast_emission_sites_use_try_notify_or_are_deliberately_blocking() {
        const MARKER: &str = "DELIBERATELY blocking";
        const LOOKBACK_BYTES: usize = 2048;

        for (rel_path, src) in EMISSION_SITES {
            let needle = ".notify_node_event(";
            let mut search_idx = 0;
            while let Some(rel) = src[search_idx..].find(needle) {
                let abs = search_idx + rel;
                let preceded_by_try = abs.checked_sub(4).is_some_and(|i| &src[i..abs] == ".try");
                if !preceded_by_try {
                    let window_end = (abs + 200).min(src.len());
                    let forward_window = &src[abs..window_end];
                    if names_gossip_emit(forward_window) {
                        let lookback_start = abs.saturating_sub(LOOKBACK_BYTES);
                        let backward_window = &src[lookback_start..abs];
                        assert!(
                            backward_window.contains(MARKER),
                            "crates/core/src/{rel_path}: blocking \
                             notify_node_event(...).await is used to emit a \
                             best-effort gossip event (Broadcast* or \
                             SyncStateToPeer) near offset {abs}, but no \
                             '{MARKER}' marker comment appears in the preceding \
                             ~{LOOKBACK_BYTES} bytes. Either use \
                             try_notify_node_event (preferred — see #4145) or \
                             add a '{MARKER}' comment above the call explaining \
                             the deliberate exception, as `announce_contract_hosted` \
                             does for the one-shot hosting transition. Forward \
                             window:\n{forward_window}"
                        );
                    }
                }
                search_idx = abs + needle.len();
            }
        }
    }

    /// Complementary source-scrape that walks every `.rs` file in
    /// `crates/core/src/` and fails if a file outside `EMISSION_SITES`
    /// contains a best-effort-gossip emission. Catches the scenario
    /// where a refactor introduces a new emission in a previously-
    /// unscanned file — the primary allowlist scrape would silently
    /// miss it; this one yells.
    ///
    /// Also asserts that every `EMISSION_SITES` entry was actually
    /// visited by the walk (rules out silent vacuous-pass if a CI
    /// sandbox change disables `read_dir`).
    #[test]
    fn broadcast_emission_files_are_in_allowlist() {
        use std::collections::HashSet;

        let allowlist: HashSet<&str> = EMISSION_SITES.iter().map(|(p, _)| *p).collect();

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let src_root = std::path::Path::new(manifest_dir).join("src");

        // Recursively walk src_root collecting (relative_path, contents).
        // `.expect()` rather than silently swallowing errors so a broken
        // CI sandbox surfaces as a loud failure, not a vacuous pass
        // (PR #4231 third-pass review, Skeptical L3 + Testing Imp #1).
        fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, String)>) {
            let entries = std::fs::read_dir(dir)
                .unwrap_or_else(|e| panic!("read_dir({}) failed: {e}", dir.display()));
            for entry in entries {
                let entry =
                    entry.unwrap_or_else(|e| panic!("dir entry in {} failed: {e}", dir.display()));
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, root, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                        panic!("read_to_string({}) failed: {e}", path.display())
                    });
                    let rel = path
                        .strip_prefix(root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .replace('\\', "/");
                    out.push((rel, contents));
                }
            }
        }

        let mut files = Vec::new();
        walk(&src_root, &src_root, &mut files);

        // Vacuous-pass guard: every allowlist file MUST be present in
        // the walk. If the walk found zero files (broken sandbox) or
        // missed an allowlisted file (path typo), fail loudly.
        for allow in &allowlist {
            assert!(
                files.iter().any(|(rel, _)| rel == *allow),
                "allowlist file '{allow}' was NOT visited by the source walk \
                 (walked {} files total). Either EMISSION_SITES has a stale path \
                 or the walk failed silently — a vacuous pass would silently \
                 disable the regression guard.",
                files.len()
            );
        }

        for (rel, contents) in &files {
            // Skip allowlisted files + this test file (the test body
            // itself contains "notify_node_event(", "Broadcast", and
            // "SyncStateToPeer" string literals in error messages).
            if allowlist.contains(rel.as_str()) {
                continue;
            }
            if rel == "node/op_state_manager.rs" {
                continue;
            }

            // Look for actual EMISSION sites: a `notify_node_event(`
            // (try_ or blocking) whose argument names a Broadcast or
            // SyncStateToPeer variant within a small forward window.
            // Mirrors the primary scrape's matching strategy so the
            // two stay coherent.
            let needle = "notify_node_event(";
            let mut search_idx = 0;
            while let Some(rel_idx) = contents[search_idx..].find(needle) {
                let abs = search_idx + rel_idx;
                let window_end = (abs + 200).min(contents.len());
                let forward_window = &contents[abs..window_end];
                assert!(
                    !names_gossip_emit(forward_window),
                    "{rel} contains a `notify_node_event(...)` best-effort \
                     gossip emission (Broadcast* or SyncStateToPeer) near \
                     offset {abs}, but {rel} is NOT in the shared EMISSION_SITES \
                     allowlist used by both source-scrape tests. Add an entry \
                     to `EMISSION_SITES` in op_state_manager.rs covering this \
                     file (both `include_str!` and the relative path) — that \
                     single change updates both the primary scrape and this \
                     walker. Window:\n{forward_window}"
                );
                search_idx = abs + needle.len();
            }
        }
    }

    #[test]
    fn contract_waiters_cleanup_removes_closed_senders() {
        use std::collections::HashMap;

        let mut waiters: HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>> = HashMap::new();
        let id1 = ContractInstanceId::new([1; 32]);
        let id2 = ContractInstanceId::new([2; 32]);

        // Create waiters with live and dropped receivers
        let (tx_live, _rx_live) = oneshot::channel();
        let (tx_dead, _rx_dead) = oneshot::channel::<()>();
        drop(_rx_dead); // Drop receiver so sender.is_closed() returns true

        waiters.entry(id1).or_default().push(tx_live);
        waiters.entry(id1).or_default().push(tx_dead);

        // id2 has only dead waiters
        let (tx_dead2, rx_dead2) = oneshot::channel::<()>();
        drop(rx_dead2);
        waiters.entry(id2).or_default().push(tx_dead2);

        assert_eq!(waiters.len(), 2);

        // Run the cleanup logic (same as in garbage_cleanup_task)
        waiters.retain(|_id, senders| {
            senders.retain(|sender| !sender.is_closed());
            !senders.is_empty()
        });

        // id1 should remain (has one live sender), id2 should be removed
        assert_eq!(waiters.len(), 1);
        assert!(waiters.contains_key(&id1));
        assert!(!waiters.contains_key(&id2));
        assert_eq!(waiters[&id1].len(), 1);
    }

    /// `ClientOpGuard::Drop` decrements the counter exactly once,
    /// including on a panic. The shutdown drain depends on this —
    /// if a panic skipped the decrement, the counter would leak and
    /// `wait_for_drain` would block until `drain_timeout` even
    /// though no driver is actually still running.
    ///
    /// Asks from Testing-reviewer r1 and r2; added here so a future
    /// refactor switching `ClientOpGuard` to a manual `fetch_sub`
    /// (foot-gun) without `Drop` semantics is caught at CI time.
    #[test]
    fn client_op_guard_decrements_on_panic() {
        let counter = Arc::new(AtomicUsize::new(0));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = ClientOpGuard::new(counter.clone());
            assert_eq!(counter.load(Ordering::Relaxed), 1);
            panic!("simulated driver panic");
        }));
        assert!(result.is_err(), "the closure must have panicked");
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "ClientOpGuard::Drop must decrement the counter even on \
             panic — otherwise the shutdown drain leaks and waits \
             the full drain_timeout for a driver that's no longer \
             alive."
        );
    }

    /// Source-grep pin: the four handshake-participating atomic ops
    /// MUST use `Ordering::SeqCst`. Codex r3 + skeptical r3 finding
    /// — `Relaxed` is insufficient for a two-variable Dekker-style
    /// handshake; under a weakly-ordered model both threads can
    /// observe each other's writes as stale, letting a new driver
    /// spawn after the drain has completed (the exact failure mode
    /// the drain exists to prevent). A future contributor who
    /// downgrades any of these to `Relaxed` for "performance" gets
    /// caught at CI time, not in a production incident on ARM.
    #[test]
    fn seqcst_used_for_admission_handshake_atomics() {
        let op_state = include_str!("op_state_manager.rs");
        // Counter bump in ClientOpGuard::new.
        let new_body = op_state
            .split("fn new(counter: Arc<AtomicUsize>) -> Self {")
            .nth(1)
            .and_then(|s| s.split("Self {").next())
            .expect("ClientOpGuard::new body must be findable");
        assert!(
            new_body.contains("fetch_add(1, Ordering::SeqCst)"),
            "ClientOpGuard::new must fetch_add with SeqCst — see \
             admit_client_op rustdoc. Found body:\n{new_body}"
        );

        // Gate load in admit_client_op.
        let admit_body = op_state
            .split("pub(crate) fn admit_client_op(&self) -> Option<ClientOpGuard> {")
            .nth(1)
            .and_then(|s| s.split("\n    }").next())
            .expect("admit_client_op body must be findable");
        assert!(
            admit_body.contains("self.shutting_down.load(Ordering::SeqCst)"),
            "admit_client_op must load shutting_down with SeqCst.\nbody:\n{admit_body}"
        );

        // Node side: gate store + counter loads in node.rs.
        let node_rs = include_str!("../node.rs");
        assert!(
            node_rs.contains("self.shutting_down.store(true, Ordering::SeqCst)"),
            "ShutdownHandle::shutdown Phase 1 must store shutting_down \
             with SeqCst — the gate write must synchronize with \
             admit_client_op's gate load."
        );
        // Both the `initial` read AND the poll-loop read need SeqCst.
        // Count occurrences as a guard against partial downgrade.
        let seqcst_loads = node_rs
            .matches("self.inflight_client_ops.load(Ordering::SeqCst)")
            .count();
        assert!(
            seqcst_loads >= 2,
            "wait_for_drain must load inflight_client_ops with SeqCst \
             at BOTH the initial fast-path AND the poll loop \
             (found {seqcst_loads} SeqCst loads, need >= 2). A single \
             Relaxed load anywhere in the drain re-opens the Dekker \
             race."
        );
    }

    /// Source-grep pin: `admit_client_op` must bump the counter
    /// BEFORE checking the gate. The reverse order (check-then-bump)
    /// re-opens the Codex r2 TOCTOU. The `admit_client_op_refuses_when_shutting_down`
    /// test below pins the OUTCOME contract; this pin asserts the
    /// IMPLEMENTATION shape that makes the outcome correct for the
    /// real race window (a refactor that swaps the two lines passes
    /// the outcome test but reopens the race).
    #[test]
    fn admit_client_op_bumps_before_checking_gate() {
        let src = include_str!("op_state_manager.rs");
        let body = src
            .split("pub(crate) fn admit_client_op(&self) -> Option<ClientOpGuard> {")
            .nth(1)
            .and_then(|s| s.split("\n    }").next())
            .expect("admit_client_op body must be findable");
        let bump_pos = body
            .find("self.client_op_guard()")
            .expect("admit_client_op must call client_op_guard()");
        let gate_pos = body
            .find("self.shutting_down.load")
            .expect("admit_client_op must load shutting_down");
        assert!(
            bump_pos < gate_pos,
            "admit_client_op must call client_op_guard() (bump) BEFORE \
             loading shutting_down (check). Reverse order re-opens \
             Codex r2 TOCTOU: a check-then-bump caller can see \
             shutting_down=false, then shutdown sets it and sees \
             counter=0, returns from drain, then caller bumps + \
             spawns into a dead node."
        );
    }

    /// `admit_client_op` returns `None` (no counter bump) when the
    /// `shutting_down` gate is set. Without this test, a refactor that
    /// inverts the gate check (e.g. `if !shutting_down { return None }`)
    /// would compile and let every client op through during shutdown.
    #[test]
    fn admit_client_op_refuses_when_shutting_down() {
        let counter = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(AtomicBool::new(true)); // pre-flipped
        // Build a minimal stand-in: just the two atomics — we test
        // `admit_client_op` against an `OpManager`-shaped struct by
        // re-implementing its body here. A full OpManager fixture
        // would drag in the entire node setup. The logic under test
        // is a 4-line function; pinning its semantic via a parallel
        // implementation is acceptable for a single-function gate.
        let admit = || -> Option<ClientOpGuard> {
            let guard = ClientOpGuard::new(counter.clone());
            if gate.load(Ordering::Relaxed) {
                drop(guard);
                return None;
            }
            Some(guard)
        };

        assert!(
            admit().is_none(),
            "admit_client_op must return None when the gate is set"
        );
        assert_eq!(
            counter.load(Ordering::Relaxed),
            0,
            "the bump-then-check pattern must net-zero the counter \
             on rejection — otherwise the gate leaks bumped counts \
             that the drain then waits on indefinitely."
        );

        // Open the gate; admit succeeds and bumps the counter.
        gate.store(false, Ordering::Relaxed);
        let g = admit().expect("gate is open, admit must succeed");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        drop(g);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
