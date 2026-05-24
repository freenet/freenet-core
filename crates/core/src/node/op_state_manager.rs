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
    sync::{Arc, OnceLock, atomic::AtomicBool},
    time::Duration,
};

use dashmap::{DashMap, DashSet};
use either::Either;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
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
    },
    ring::{
        ConnectionManager, LiveTransactionTracker, PeerConnectionBackoff, PeerKey, PeerKeyLocation,
        Ring,
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
    /// Interest manager for delta-based state synchronization
    pub interest_manager: Arc<crate::ring::interest::InterestManager<InstantTimeSrc>>,
    /// Dedup cache for skipping redundant broadcast WASM merges
    pub broadcast_dedup_cache: Arc<crate::operations::update::BroadcastDedupCache>,
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
            request_router: self.request_router.clone(),
            orphan_stream_registry: self.orphan_stream_registry.clone(),
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
        }
    }
}

impl OpManager {
    pub(super) fn new<ER: NetEventRegister + Clone>(
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
        let interest_manager = Arc::new(crate::ring::interest::InterestManager::new(
            InstantTimeSrc::new(),
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
            request_router,
            orphan_stream_registry,
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
        })
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

    /// Non-blocking variant of [`Self::release_pending_op_slot`] for callers
    /// that run on the network event loop (where `send().await` could
    /// deadlock). Used by `P2pBridge::handle_orphaned_transactions` to wake
    /// drivers whose downstream peer has just disconnected (#4154). The
    /// notification is best-effort: on a transiently-full channel the
    /// driver falls back to its `OPERATION_TTL` timeout. See
    /// [`try_release_pending_op_slot_on`] for the underlying send logic.
    pub(crate) fn try_release_pending_op_slot(&self, tx: Transaction) {
        try_release_pending_op_slot_on(&self.to_event_listener.notifications_sender, tx);
    }

    /// Timeout for sending notifications to the event loop.
    /// If the channel is full for this long, the event loop is stuck and sending will never succeed.
    const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(30);

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
        match tokio::time::timeout(
            Self::NOTIFICATION_SEND_TIMEOUT,
            self.to_event_listener
                .notifications_sender
                .send(Either::Right(msg)),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => {
                tracing::error!(
                    timeout_secs = Self::NOTIFICATION_SEND_TIMEOUT.as_secs(),
                    channel_pending = self.to_event_listener.notification_channel_pending(),
                    channel_remaining = self.to_event_listener.notifications_sender().capacity(),
                    "notify_node_event: Notification channel full for too long, event loop may be stuck"
                );
                Err(OpError::NotificationChannelError(
                    "notification channel send timed out — event loop is likely stuck".into(),
                ))
            }
        }
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

    /// Send an Unsubscribe message to the upstream peer for a contract.
    ///
    /// Finds the upstream peer from the interest manager, resolves its address,
    /// and sends a fire-and-forget Unsubscribe message via the operation routing
    /// mechanism. Also removes the local active subscription and interest tracking.
    pub async fn send_unsubscribe_upstream(&self, contract: &ContractKey) {
        // Find the upstream peer for this contract
        let upstream = self
            .interest_manager
            .get_interested_peers(contract)
            .into_iter()
            .find(|(_, interest)| interest.is_upstream);

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
    #[allow(dead_code)]
    pub fn op_ctx(&self, tx: Transaction) -> OpCtx {
        OpCtx::new(tx, self.to_event_listener.op_execution_sender.clone())
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound.send_to_handler(msg).await
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
            .send_to_handler_with_timeout(msg, timeout)
            .await
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

    /// Get the current hop count (remaining HTL) for an operation.
    /// Always returns `None` — no live op reports a hop here.
    /// Kept for API compatibility with the tracing consumer at
    /// `crates/core/src/tracing.rs:1266`.
    pub fn get_current_hop(&self, _id: &Transaction) -> Option<usize> {
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
        self.neighbor_hosting.on_peer_disconnected(pub_key);
        self.interest_manager
            .schedule_deferred_removal(&PeerKey::from(pub_key.clone()));
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

/// Non-blocking emit of `NodeEvent::TransactionCompleted(tx)` on the
/// event-loop notification channel; returns `true` when enqueued.
///
/// Extracted from [`OpManager::try_release_pending_op_slot`] so it can be
/// exercised in unit tests without building a full `OpManager`. Best-effort:
/// a momentarily-full or closed channel produces a warning and the parked
/// driver falls back to its `OPERATION_TTL` timeout (#4154).
fn try_release_pending_op_slot_on(
    notifications_sender: &mpsc::Sender<Either<NetMessage, NodeEvent>>,
    tx: Transaction,
) -> bool {
    match notifications_sender.try_send(Either::Right(NodeEvent::TransactionCompleted(tx))) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            tracing::warn!(
                %tx,
                "try_release_pending_op_slot: notification channel full; \
                 driver will wait for OPERATION_TTL timeout"
            );
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            tracing::warn!(
                %tx,
                "try_release_pending_op_slot: notification channel closed; \
                 receiver likely dropped"
            );
            false
        }
    }
}

/// Non-blocking emit of a [`NodeEvent`] on the event-loop notification
/// channel.
///
/// Extracted from [`OpManager::try_notify_node_event`] so the try-send
/// path is testable in isolation without building a full `OpManager`.
/// On `Full` or `Closed` the event is dropped, a warn-level log is
/// emitted, and the function returns `Err(OpError::NotificationError)`.
/// Best-effort by design — see the OpManager method doc for the wedge
/// (#4145) this prevents.
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
            tracing::warn!(
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
            tracing::warn!(
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
    use crate::node::network_bridge::EventLoopNotificationsReceiver;
    use either::Either;
    use tokio::time::{Duration, Instant, timeout};

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
    // Regression tests for #4154: parked drivers must be woken when
    // their downstream peer disconnects, not wait `OPERATION_TTL`.
    // Pre-fix, `handle_orphaned_transactions` only logged orphans and
    // a forwarded GET blocked the full 60 s before retrying. The fix
    // emits `TransactionCompleted(tx)` per orphan via
    // `try_release_pending_op_slot_on`, the event loop drops the
    // matching `pending_op_results` sender, and the driver's `recv()`
    // returns `None` — `send_and_await` maps that to
    // `OpError::NotificationError` and the retry loop advances.
    // ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn try_release_pending_op_slot_emits_transaction_completed() {
        // Happy path: the standalone helper enqueues exactly one
        // `TransactionCompleted(tx)` on the notification channel.
        let (receiver, notifier) = event_loop_notification_channel();
        let EventLoopNotificationsReceiver {
            mut notifications_receiver,
            ..
        } = receiver;

        let tx = Transaction::ttl_transaction();

        let delivered = super::try_release_pending_op_slot_on(notifier.notifications_sender(), tx);
        assert!(delivered, "helper must enqueue on a live channel");

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
    async fn try_release_pending_op_slot_handles_dropped_receiver() {
        // Closed channel: helper must return `false` rather than panic
        // — disconnect cleanup must remain robust when the event loop
        // has already torn down (e.g. shutdown races).
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();
        let delivered = super::try_release_pending_op_slot_on(notifier.notifications_sender(), tx);
        assert!(
            !delivered,
            "helper must return false once receiver is dropped"
        );
    }

    #[tokio::test]
    async fn orphaned_transaction_wakes_parked_pending_op_results_waiter() {
        // End-to-end pipeline test for #4154 without standing up a full
        // node. Reproduces the driver → notification-channel → event-
        // loop → sender-drop → driver-wakeup sequence and asserts it
        // completes in under 100 ms (pre-fix it would hang `OPERATION_TTL`).
        let (mut event_loop_receiver, notifier) = event_loop_notification_channel();

        // Stand in for `pending_op_results[tx] = sender` and the driver's
        // pending `recv()` on the matching receiver.
        let (response_sender, mut driver_response_rx) =
            tokio::sync::mpsc::channel::<crate::message::NetMessage>(1);
        let mut pending_op_results: std::collections::HashMap<
            Transaction,
            tokio::sync::mpsc::Sender<crate::message::NetMessage>,
        > = std::collections::HashMap::new();
        let tx = Transaction::ttl_transaction();
        pending_op_results.insert(tx, response_sender);

        // Trigger the wake — this is the orphan-handler path under test.
        let delivered = super::try_release_pending_op_slot_on(notifier.notifications_sender(), tx);
        assert!(delivered, "orphan-handler helper must enqueue notification");

        // Mimic the event loop's `TransactionCompleted` arm by dropping the
        // sender out of `pending_op_results`.
        let event = timeout(
            Duration::from_millis(100),
            event_loop_receiver.notifications_receiver.recv(),
        )
        .await
        .expect("event loop never received TransactionCompleted")
        .expect("notification channel closed before TransactionCompleted arrived");
        match event {
            Either::Right(NodeEvent::TransactionCompleted(observed)) => {
                assert_eq!(observed, tx);
                pending_op_results.remove(&observed);
            }
            other @ Either::Left(_) | other @ Either::Right(_) => {
                panic!("expected TransactionCompleted, got {other:?}")
            }
        }

        // The driver's `recv()` must now resolve to `None` immediately —
        // pre-fix this hung the full `OPERATION_TTL`. Cap at 100 ms.
        let driver_wakeup = timeout(Duration::from_millis(100), driver_response_rx.recv()).await;
        match driver_wakeup {
            Ok(None) => {
                // Channel closed, driver wakes with `Err(NotificationError)` — correct.
            }
            Ok(Some(msg)) => panic!("driver received unexpected message: {msg:?}"),
            Err(_) => panic!(
                "driver did not wake after orphan handling — \
                 pre-#4154 behavior reproduced"
            ),
        }
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

    /// Source-scrape regression guard for #4145. Pin that every
    /// Broadcast-event emission site either uses `try_notify_node_event`
    /// (non-blocking) OR is explicitly annotated `DELIBERATELY blocking`
    /// with a load-bearing justification.
    ///
    /// Scope covers every file in the freenet crate that currently
    /// emits a Broadcast* event:
    ///   - contract/executor/runtime.rs (executor commit path)
    ///   - contract/executor/mock_runtime.rs (test variant; mirrors prod)
    ///   - operations.rs (hosting + interest gossip)
    ///   - node/network_bridge/p2p_protoc.rs (retry-spawn path)
    ///
    /// Allowlist marker: `DELIBERATELY blocking` within ~2 KB BEFORE
    /// the call site. Used only at `announce_contract_hosted` today —
    /// see that comment for why a one-shot transition forces blocking
    /// emission. Any future addition to the allowlist requires the same
    /// kind of explicit justification.
    #[test]
    fn broadcast_emission_sites_use_try_notify_or_are_deliberately_blocking() {
        const MARKER: &str = "DELIBERATELY blocking";
        const LOOKBACK_BYTES: usize = 2048;

        for (path, src) in [
            (
                "crates/core/src/contract/executor/runtime.rs",
                include_str!("../contract/executor/runtime.rs"),
            ),
            (
                "crates/core/src/contract/executor/mock_runtime.rs",
                include_str!("../contract/executor/mock_runtime.rs"),
            ),
            (
                "crates/core/src/operations.rs",
                include_str!("../operations.rs"),
            ),
            (
                "crates/core/src/node/network_bridge/p2p_protoc.rs",
                include_str!("network_bridge/p2p_protoc.rs"),
            ),
        ] {
            let needle = ".notify_node_event(";
            let mut search_idx = 0;
            while let Some(rel) = src[search_idx..].find(needle) {
                let abs = search_idx + rel;
                let preceded_by_try = abs.checked_sub(4).is_some_and(|i| &src[i..abs] == ".try");
                if !preceded_by_try {
                    let window_end = (abs + 200).min(src.len());
                    let forward_window = &src[abs..window_end];
                    if forward_window.contains("Broadcast") {
                        let lookback_start = abs.saturating_sub(LOOKBACK_BYTES);
                        let backward_window = &src[lookback_start..abs];
                        assert!(
                            backward_window.contains(MARKER),
                            "{path}: blocking notify_node_event(...).await is used \
                             to emit a Broadcast event near offset {abs}, but no \
                             '{MARKER}' marker comment appears in the preceding \
                             ~{LOOKBACK_BYTES} bytes. Either use try_notify_node_event \
                             (preferred — see #4145) or add a '{MARKER}' comment \
                             above the call explaining the deliberate exception, \
                             as `announce_contract_hosted` does for the one-shot \
                             hosting transition. Forward window:\n{forward_window}"
                        );
                    }
                }
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
}
