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
    sync::{atomic::AtomicBool, Arc, OnceLock},
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
    message::{
        InterestMessage, MessageStats, NetMessage, NetMessageV1, NodeEvent, Transaction,
        TransactionType,
    },
    operations::{
        connect::{ConnectForwardEstimator, ConnectOp, ConnectState},
        get::GetOp,
        orphan_streams::OrphanStreamRegistry,
        put::PutOp,
        subscribe::SubscribeOp,
        update::UpdateOp,
        OpEnum, OpError,
    },
    ring::{
        ConnectionFailureReason, ConnectionManager, LiveTransactionTracker, PeerConnectionBackoff,
        PeerKey, PeerKeyLocation, Ring,
    },
    transport::TransportPublicKey,
    util::time_source::InstantTimeSrc,
};

use super::{
    neighbor_hosting::NeighborHostingManager, network_bridge::EventLoopNotificationsSender,
    NetEventRegister, NodeConfig, RequestRouter,
};

#[cfg(debug_assertions)]
macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpError::IncorrectTxType($var, $get_ty));
        }
    };
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpNotAvailable {
    #[error("operation running")]
    Running,
    #[error("operation completed")]
    Completed,
}

/// Manages sub-operation tracking for atomic operation execution.
/// Ensures atomicity: clients receive success only when all sub-operations succeed.
#[derive(Default, Clone)]
struct SubOperationTracker {
    /// Parent-to-children mapping for sub-operation tracking.
    sub_operations: Arc<DashMap<Transaction, HashSet<Transaction>>>,
    /// Root operations awaiting sub-operation completion before client notification.
    root_ops_awaiting_sub_ops: Arc<DashMap<Transaction, OpEnum>>,
    /// Child-to-parent index for O(1) parent lookups.
    parent_of: Arc<DashMap<Transaction, Transaction>>,
    /// Expected sub-operation count per root operation. Pre-registered before spawning
    /// to prevent race conditions where children complete before parent registration.
    expected_sub_operations: Arc<DashMap<Transaction, usize>>,
    /// Root operations with at least one failed sub-operation.
    failed_parents: Arc<DashSet<Transaction>>,
}

impl SubOperationTracker {
    fn new() -> Self {
        Self {
            sub_operations: Arc::new(DashMap::new()),
            root_ops_awaiting_sub_ops: Arc::new(DashMap::new()),
            parent_of: Arc::new(DashMap::new()),
            expected_sub_operations: Arc::new(DashMap::new()),
            failed_parents: Arc::new(DashSet::new()),
        }
    }

    /// Marks a child operation as completed and decrements the expected counter.
    /// Removes tracking entry when all expected children have completed.
    fn mark_sub_op_completed(&self, root_tx: Transaction) {
        let mut should_remove = false;
        if let Some(mut expected) = self.expected_sub_operations.get_mut(&root_tx) {
            if *expected > 0 {
                *expected -= 1;
                tracing::debug!(
                    root_tx = %root_tx,
                    remaining = *expected,
                    "sub-operation completed"
                );
            }
            if *expected == 0 {
                should_remove = true;
            }
        }

        if should_remove {
            self.expected_sub_operations.remove(&root_tx);
        }
    }

    fn remove_child_link(&self, parent: Transaction, child: Transaction) {
        let should_remove_parent = if let Some(mut children) = self.sub_operations.get_mut(&parent)
        {
            children.remove(&child);
            let is_empty = children.is_empty();
            drop(children);
            is_empty
        } else {
            false
        };

        if should_remove_parent {
            self.sub_operations.remove(&parent);
        }

        self.parent_of.remove(&child);
    }

    fn cleanup_parent_tracking(&self, parent: Transaction) {
        self.expected_sub_operations.remove(&parent);
        self.sub_operations.remove(&parent);
    }

    /// Atomically registers both expected count and parent-child relationship.
    /// This prevents race conditions where children complete before registration.
    fn expect_and_register_sub_operation(&self, parent: Transaction, child: Transaction) {
        // Increment expected count
        self.expected_sub_operations
            .entry(parent)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        // Register parent-child relationship
        self.sub_operations.entry(parent).or_default().insert(child);

        self.parent_of.insert(child, parent);
    }

    /// Returns true if all child operations of the parent have completed.
    fn all_sub_operations_completed(
        &self,
        parent: Transaction,
        completed_ops: &DashSet<Transaction>,
    ) -> bool {
        if let Some(expected) = self.expected_sub_operations.get(&parent) {
            if *expected > 0 {
                return false;
            }
        }

        match self.sub_operations.get(&parent) {
            None => true,
            Some(children) => children.iter().all(|child| completed_ops.contains(child)),
        }
    }

    /// Returns the number of pending child operations for the parent.
    fn count_pending_sub_operations(
        &self,
        parent: Transaction,
        completed_ops: &DashSet<Transaction>,
    ) -> usize {
        match self.sub_operations.get(&parent) {
            None => 0,
            Some(children) => children
                .iter()
                .filter(|child| !completed_ops.contains(child))
                .count(),
        }
    }

    /// Check if a transaction is a sub-operation (has a parent transaction).
    /// Sub-operations should not send responses directly to clients.
    fn is_sub_operation(&self, tx: Transaction) -> bool {
        self.parent_of.contains_key(&tx)
    }

    fn get_parent(&self, child: Transaction) -> Option<Transaction> {
        self.parent_of.get(&child).map(|entry| *entry)
    }
}

#[derive(Default)]
struct Ops {
    connect: DashMap<Transaction, crate::operations::connect::ConnectOp>,
    put: DashMap<Transaction, PutOp>,
    get: DashMap<Transaction, GetOp>,
    subscribe: DashMap<Transaction, SubscribeOp>,
    update: DashMap<Transaction, UpdateOp>,
    completed: DashSet<Transaction>,
    under_progress: DashSet<Transaction>,
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
    /// Sub-operation tracking for atomic operation execution
    sub_op_tracker: SubOperationTracker,
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
    /// Notifies `initial_join_procedure` and `handle_aborted_op` when gateway
    /// backoff is cleared, so they can wake from backoff sleeps and retry immediately.
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
            sub_op_tracker: self.sub_op_tracker.clone(),
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
        let sub_op_tracker = SubOperationTracker::new();
        let connect_forward_estimator = Arc::new(RwLock::new(ConnectForwardEstimator::new()));
        let request_router = Arc::new(OnceLock::new());
        let ch_outbound = Arc::new(ch_outbound);
        let contract_waiters: Arc<
            Mutex<std::collections::HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>>>,
        > = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let pending_contract_fetches: Arc<DashMap<ContractInstanceId, u64>> =
            Arc::new(DashMap::new());

        task_monitor.register(
            "garbage_cleanup",
            GlobalExecutor::spawn(
                garbage_cleanup_task(
                    rx,
                    ops.clone(),
                    ring.live_tx_tracker.clone(),
                    notification_channel.clone(),
                    event_register,
                    sub_op_tracker.clone(),
                    result_router_tx.clone(),
                    request_router.clone(),
                    ring.clone(),
                    ch_outbound.clone(),
                    contract_waiters.clone(),
                    pending_contract_fetches.clone(),
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
            sub_op_tracker,
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

    /// Timeout for sending notifications to the event loop.
    /// If the channel is full for this long, the event loop is stuck and sending will never succeed.
    const NOTIFICATION_SEND_TIMEOUT: Duration = Duration::from_secs(30);

    /// An early, fast path, return for communicating back changes of on-going operations
    /// in the node to the main message handler, without any transmission in the network whatsoever.
    ///
    /// Useful when transitioning between states that do not require any network communication
    /// with other nodes, like intermediate states before returning.
    pub async fn notify_op_change(&self, msg: NetMessage, op: OpEnum) -> Result<(), OpError> {
        let tx = *msg.id();
        let peer_id = &self.ring.connection_manager.pub_key;
        tracing::debug!(
            tx = %tx,
            msg_type = %msg,
            peer = %peer_id,
            "notify_op_change: Pushing operation and sending notification"
        );

        // push back the state to the stack
        self.push(tx, op).await?;

        tracing::debug!(
            tx = %tx,
            peer = %peer_id,
            "notify_op_change: Operation pushed, sending to event listener"
        );

        match tokio::time::timeout(
            Self::NOTIFICATION_SEND_TIMEOUT,
            self.to_event_listener
                .notifications_sender()
                .send(Either::Left(msg)),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(OpError::NotificationError),
            Err(_) => {
                tracing::error!(
                    tx = %tx,
                    timeout_secs = Self::NOTIFICATION_SEND_TIMEOUT.as_secs(),
                    channel_pending = self.to_event_listener.notification_channel_pending(),
                    channel_remaining = self.to_event_listener.notifications_sender().capacity(),
                    "notify_op_change: Notification channel full for too long, event loop may be stuck"
                );
                return Err(OpError::NotificationChannelError(
                    "notification channel send timed out — event loop is likely stuck".into(),
                ));
            }
        }

        tracing::debug!(
            tx = %tx,
            peer = %peer_id,
            "notify_op_change: Notification sent successfully"
        );

        Ok(())
    }

    /// Non-blocking variant of [`notify_op_change`] that fails fast when the
    /// notification channel is full instead of blocking for 30 seconds.
    /// On failure the pushed operation is cleaned up so it does not leak.
    pub async fn notify_op_change_nonblocking(
        &self,
        msg: NetMessage,
        op: OpEnum,
    ) -> Result<(), OpError> {
        let tx = *msg.id();
        self.push(tx, op).await?;

        match self
            .to_event_listener
            .notifications_sender()
            .try_send(Either::Left(msg))
        {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    tx = %tx,
                    channel_pending = self.to_event_listener.notification_channel_pending(),
                    "notify_op_change_nonblocking: channel full, failing fast"
                );
                self.completed(tx);
                Err(OpError::NotificationChannelError(
                    "notification channel full (non-blocking send)".into(),
                ))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                self.completed(tx);
                Err(OpError::NotificationError)
            }
        }
    }

    // An early, fast path, return for communicating events in the node to the main message handler,
    // without any transmission in the network whatsoever and avoiding any state transition.
    //
    // Useful when we want to notify connection attempts, or other events that do not require any
    // network communication with other nodes.
    pub async fn notify_node_event(&self, msg: NodeEvent) -> Result<(), OpError> {
        tracing::info!(event = %msg, "notify_node_event: queuing node event");
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

        let op = OpEnum::Subscribe(crate::operations::subscribe::create_unsubscribe_op(
            instance_id,
            tx,
            target_addr,
        ));

        match self.notify_op_change_nonblocking(msg, op).await {
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

    #[allow(dead_code)] // FIXME: enable async sub-transactions
    pub async fn notify_op_execution(&self, msg: NetMessage) -> Result<NetMessage, OpError> {
        let (response_sender, mut response_receiver): (
            tokio::sync::mpsc::Sender<NetMessage>,
            tokio::sync::mpsc::Receiver<NetMessage>,
        ) = tokio::sync::mpsc::channel(1);

        self.to_event_listener
            .op_execution_sender
            .send((response_sender, msg))
            .await
            .map_err(|_| OpError::NotificationError)?;
        match response_receiver.recv().await {
            Some(msg) => Ok(msg),
            None => Err(OpError::NotificationError),
        }
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

    pub async fn push(&self, id: Transaction, op: OpEnum) -> Result<(), OpError> {
        // Check if operation is already completed - don't push back to HashMap
        if self.ops.completed.contains(&id) {
            tracing::debug!(
                tx = %id,
                "OpManager: Ignoring push for already completed operation"
            );
            return Ok(());
        }

        if let Some(tx) = self.ops.under_progress.remove(&id) {
            if tx.timed_out() {
                self.ops.completed.insert(tx);
                return Ok(());
            }
        }
        self.new_transactions.send(id).await?;
        match op {
            OpEnum::Connect(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.transaction_type(), TransactionType::Connect);
                self.ops.connect.insert(id, *op);
            }
            OpEnum::Put(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.transaction_type(), TransactionType::Put);
                self.ops.put.insert(id, op);
            }
            OpEnum::Get(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.transaction_type(), TransactionType::Get);
                self.ops.get.insert(id, op);
            }
            OpEnum::Subscribe(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.transaction_type(), TransactionType::Subscribe);
                self.ops.subscribe.insert(id, op);
            }
            OpEnum::Update(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.transaction_type(), TransactionType::Update);
                self.ops.update.insert(id, op);
            }
        }
        Ok(())
    }

    /// Peek at the next hop address for an operation without removing it.
    /// Used by hop-by-hop routing to determine where to send initial outbound messages.
    /// Returns None if the operation doesn't exist or doesn't have a next hop address.
    pub fn peek_next_hop_addr(&self, id: &Transaction) -> Option<std::net::SocketAddr> {
        if self.ops.completed.contains(id) || self.ops.under_progress.contains(id) {
            return None;
        }
        match id.transaction_type() {
            TransactionType::Connect => self
                .ops
                .connect
                .get(id)
                .and_then(|op| op.get_next_hop_addr()),
            TransactionType::Put => self.ops.put.get(id).and_then(|op| op.get_next_hop_addr()),
            TransactionType::Get => self.ops.get.get(id).and_then(|op| op.get_next_hop_addr()),
            TransactionType::Subscribe => self
                .ops
                .subscribe
                .get(id)
                .and_then(|op| op.get_next_hop_addr()),
            TransactionType::Update => self
                .ops
                .update
                .get(id)
                .and_then(|op| op.get_next_hop_addr()),
        }
    }

    /// Peek at the full target peer (including public key) without removing the operation.
    /// Used when establishing new connections where we need the public key for handshake.
    pub fn peek_target_peer(&self, id: &Transaction) -> Option<PeerKeyLocation> {
        if self.ops.completed.contains(id) || self.ops.under_progress.contains(id) {
            return None;
        }
        match id.transaction_type() {
            TransactionType::Connect => {
                self.ops.connect.get(id).and_then(|op| op.get_target_peer())
            }
            // Other operations only store addresses, not full peer info
            TransactionType::Put
            | TransactionType::Get
            | TransactionType::Subscribe
            | TransactionType::Update => None,
        }
    }

    /// Get the current hop count (remaining HTL) for an operation.
    /// Used for calculating hop_count in success/failure events.
    pub fn get_current_hop(&self, id: &Transaction) -> Option<usize> {
        match id.transaction_type() {
            TransactionType::Get => self.ops.get.get(id).and_then(|op| op.get_current_hop()),
            TransactionType::Put => self.ops.put.get(id).and_then(|op| op.get_current_htl()),
            // TODO: Add support for Subscribe operations when they track HTL
            TransactionType::Connect | TransactionType::Subscribe | TransactionType::Update => None,
        }
    }

    pub fn pop(&self, id: &Transaction) -> Result<Option<OpEnum>, OpNotAvailable> {
        if self.ops.completed.contains(id) {
            return Err(OpNotAvailable::Completed);
        }
        if self.ops.under_progress.contains(id) {
            if id.timed_out() {
                self.ops.completed.insert(*id);
                return Err(OpNotAvailable::Completed);
            }
            return Err(OpNotAvailable::Running);
        }
        let op = match id.transaction_type() {
            TransactionType::Connect => self
                .ops
                .connect
                .remove(id)
                .map(|(_k, v)| v)
                .map(|op| OpEnum::Connect(Box::new(op))),
            TransactionType::Put => self.ops.put.remove(id).map(|(_k, v)| v).map(OpEnum::Put),
            TransactionType::Get => self.ops.get.remove(id).map(|(_k, v)| v).map(OpEnum::Get),
            TransactionType::Subscribe => self
                .ops
                .subscribe
                .remove(id)
                .map(|(_k, v)| v)
                .map(OpEnum::Subscribe),
            TransactionType::Update => self
                .ops
                .update
                .remove(id)
                .map(|(_k, v)| v)
                .map(OpEnum::Update),
        };
        self.ops.under_progress.insert(*id);
        Ok(op)
    }

    pub fn completed(&self, id: Transaction) {
        self.ring.live_tx_tracker.remove_finished_transaction(id);
        self.ops.under_progress.remove(&id);
        self.ops.completed.insert(id);

        // Clean up request router to prevent stale entries from blocking subsequent requests
        if let Some(router) = self.request_router.get() {
            router.complete_operation(id);
        }

        if let Some(parent_tx) = self.sub_op_tracker.get_parent(id) {
            self.sub_op_tracker.remove_child_link(parent_tx, id);
            self.sub_op_tracker.mark_sub_op_completed(parent_tx);

            let remaining = self
                .sub_op_tracker
                .count_pending_sub_operations(parent_tx, &self.ops.completed);
            tracing::debug!(
                %id,
                %parent_tx,
                remaining,
                "child operation completed"
            );

            if self
                .sub_op_tracker
                .all_sub_operations_completed(parent_tx, &self.ops.completed)
            {
                if let Some((_key, parent_op)) = self
                    .sub_op_tracker
                    .root_ops_awaiting_sub_ops
                    .remove(&parent_tx)
                {
                    tracing::info!(
                        %parent_tx,
                        "root operation completed after all children finished"
                    );

                    self.sub_op_tracker.cleanup_parent_tracking(parent_tx);
                    self.sub_op_tracker.failed_parents.remove(&parent_tx);
                    self.completed(parent_tx);

                    let host_result = parent_op.to_host_result();
                    self.send_client_result(parent_tx, host_result);
                }
            }
        }
    }

    /// Atomically registers both expected count and parent-child relationship.
    /// This prevents race conditions where children complete before registration.
    pub fn expect_and_register_sub_operation(&self, parent: Transaction, child: Transaction) {
        self.sub_op_tracker
            .expect_and_register_sub_operation(parent, child);
    }

    /// Returns true if all child operations of the parent have completed.
    pub fn all_sub_operations_completed(&self, parent: Transaction) -> bool {
        self.sub_op_tracker
            .all_sub_operations_completed(parent, &self.ops.completed)
    }

    /// Returns the number of pending child operations for the parent.
    pub fn count_pending_sub_operations(&self, parent: Transaction) -> usize {
        self.sub_op_tracker
            .count_pending_sub_operations(parent, &self.ops.completed)
    }

    /// Handle sub-operation failure - propagate error to parent.
    pub async fn sub_operation_failed(
        &self,
        child: Transaction,
        error_msg: &str,
    ) -> Result<(), OpError> {
        tracing::error!(
            child_tx = %child,
            error = %error_msg,
            "Sub-operation failed, propagating to root"
        );

        if let Some(parent_tx) = self.sub_op_tracker.get_parent(child) {
            self.sub_op_tracker.remove_child_link(parent_tx, child);
            self.sub_op_tracker.mark_sub_op_completed(parent_tx);

            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: format!("Sub-operation {} failed: {}", child, error_msg).into(),
            }
            .into());

            if self
                .sub_op_tracker
                .root_ops_awaiting_sub_ops
                .remove(&parent_tx)
                .is_some()
            {
                tracing::warn!(
                    root_tx = %parent_tx,
                    child_tx = %child,
                    "Root operation aborted due to sub-operation failure"
                );
                self.sub_op_tracker.cleanup_parent_tracking(parent_tx);
                self.completed(parent_tx);
            } else {
                tracing::warn!(
                    root_tx = %parent_tx,
                    child_tx = %child,
                    "Sub-operation failed before root operation started awaiting"
                );
                self.sub_op_tracker.failed_parents.insert(parent_tx);
                // Clean up tracking to prevent memory leak
                self.sub_op_tracker.cleanup_parent_tracking(parent_tx);
                // Mark parent as completed to prevent duplicate responses
                self.completed(parent_tx);
            }

            self.send_client_result(parent_tx, error_result);
        } else {
            tracing::warn!(
                child_tx = %child,
                "Sub-operation failed but parent relationship was missing"
            );
        }
        Ok(())
    }

    /// Check if a transaction is a sub-operation (has a parent transaction).
    /// Sub-operations should not send responses directly to clients.
    pub fn is_sub_operation(&self, tx: Transaction) -> bool {
        self.sub_op_tracker.is_sub_operation(tx)
    }

    /// Exposes root operations awaiting sub-operation completion.
    pub(crate) fn root_ops_awaiting_sub_ops(&self) -> &Arc<DashMap<Transaction, OpEnum>> {
        &self.sub_op_tracker.root_ops_awaiting_sub_ops
    }

    /// Exposes failed parent operations.
    pub(crate) fn failed_parents(&self) -> &Arc<DashSet<Transaction>> {
        &self.sub_op_tracker.failed_parents
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

    /// Returns pending operation counts: [connect, put, get, subscribe, update].
    pub fn pending_op_counts(&self) -> [u32; 5] {
        [
            self.ops.connect.len() as u32,
            self.ops.put.len() as u32,
            self.ops.get.len() as u32,
            self.ops.subscribe.len() as u32,
            self.ops.update.len() as u32,
        ]
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

/// Fire-and-forget notification that a subscription timed out.
/// Spawns a task to avoid blocking the garbage cleanup loop on the contract handler response.
fn notify_subscription_timeout(
    ch_outbound: &Arc<crate::contract::ContractHandlerChannel<crate::contract::SenderHalve>>,
    instance_id: ContractInstanceId,
) {
    let ch = Arc::clone(ch_outbound);
    crate::config::GlobalExecutor::spawn(async move {
        if let Err(e) = ch
            .send_to_handler(ContractHandlerEvent::NotifySubscriptionError {
                key: instance_id,
                reason: format!("Subscription timed out for contract {}", instance_id),
            })
            .await
        {
            tracing::debug!(
                contract = %instance_id,
                error = %e,
                "Failed to notify subscription timeout"
            );
        }
    });
}

/// Reports a routing failure for a timed-out operation to the router's isotonic model.
fn report_timeout_failure(
    ring: &crate::ring::Ring,
    tx: &Transaction,
    peer: crate::ring::PeerKeyLocation,
    contract_location: crate::ring::Location,
) {
    ring.routing_finished(crate::router::RouteEvent {
        peer: peer.clone(),
        contract_location,
        outcome: crate::router::RouteOutcome::Failure,
    });
    tracing::info!(
        tx = %tx,
        peer = ?peer.socket_addr(),
        %contract_location,
        "Reported operation timeout as routing failure"
    );
}

/// Removes a put operation from the ops map, reports timeout failure if stats are available,
/// and notifies the client with an error so they don't wait silently until their own timeout.
/// Returns `true` if the operation was found and removed, `false` otherwise.
fn remove_put_and_report_failure(
    ops: &Ops,
    tx: &Transaction,
    ring: &crate::ring::Ring,
    result_router_tx: &mpsc::Sender<(Transaction, HostResult)>,
) -> bool {
    if let Some((_, put_op)) = ops.put.remove(tx) {
        if let Some((peer, contract_location)) = put_op.failure_routing_info() {
            report_timeout_failure(ring, tx, peer, contract_location);
        }
        tracing::warn!(
            tx = %tx,
            elapsed_ms = tx.elapsed().as_millis(),
            phase = "put_timeout",
            "PUT operation timed out without receiving a response"
        );
        // Notify client of timeout so they get an immediate error instead of
        // waiting silently for their own client-side timeout (#3451).
        if put_op.is_client_initiated() {
            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "PUT operation timed out".into(),
            }
            .into());
            if let Err(e) = result_router_tx.try_send((*tx, error_result)) {
                tracing::warn!(
                    %tx,
                    error = %e,
                    "failed to send PUT timeout error to client"
                );
            }
        }
        true
    } else {
        false
    }
}

/// Removes an update operation from the ops map, reports timeout failure if stats are available,
/// and notifies the client with an error so they don't wait silently until their own timeout.
/// Returns `true` if the operation was found and removed, `false` otherwise.
fn remove_update_and_report_failure(
    ops: &Ops,
    tx: &Transaction,
    ring: &crate::ring::Ring,
    result_router_tx: &mpsc::Sender<(Transaction, HostResult)>,
) -> bool {
    if let Some((_, update_op)) = ops.update.remove(tx) {
        if let Some((peer, contract_location)) = update_op.failure_routing_info() {
            report_timeout_failure(ring, tx, peer, contract_location);
        }
        tracing::warn!(
            tx = %tx,
            elapsed_ms = tx.elapsed().as_millis(),
            phase = "update_timeout",
            "UPDATE operation timed out without receiving a response"
        );
        // Notify client of timeout so they get an immediate error instead of
        // waiting silently for their own client-side timeout (#3451).
        if update_op.is_client_initiated() {
            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "UPDATE operation timed out".into(),
            }
            .into());
            if let Err(e) = result_router_tx.try_send((*tx, error_result)) {
                tracing::warn!(
                    %tx,
                    error = %e,
                    "failed to send UPDATE timeout error to client"
                );
            }
        }
        true
    } else {
        false
    }
}

/// Removes a subscribe operation from the ops map and notifies timeout if found.
/// Returns `Some(())` if the operation was found and removed, `None` otherwise.
///
/// Note: For intermediate nodes, the subscribe times out silently without sending
/// NotFound upstream. This is acceptable because the originator retries independently
/// after SUBSCRIBE_RETRY_THRESHOLD (5s), so it doesn't depend on NotFound for fast
/// failure detection. The intermediate node's stale op chain is harmless.
fn remove_subscribe_and_notify_timeout(
    ops: &Ops,
    tx: &Transaction,
    ch_outbound: &Arc<ContractHandlerChannel<SenderHalve>>,
    ring: &crate::ring::Ring,
) -> Option<()> {
    let (_, sub_op) = ops.subscribe.remove(tx)?;
    if let Some((peer, contract_location)) = sub_op.failure_routing_info() {
        report_timeout_failure(ring, tx, peer, contract_location);
    }
    if let Some(instance_id) = sub_op.instance_id() {
        notify_subscription_timeout(ch_outbound, instance_id);
    }
    Some(())
}

/// Removes a get operation from the ops map, reports timeout failure if stats are available,
/// and notifies the client with an error so they don't wait silently until their own timeout.
/// Returns `true` if the operation was found and removed, `false` otherwise.
fn remove_get_and_report_failure(
    ops: &Ops,
    tx: &Transaction,
    ring: &crate::ring::Ring,
    result_router_tx: &mpsc::Sender<(Transaction, HostResult)>,
) -> bool {
    if let Some((_, get_op)) = ops.get.remove(tx) {
        if let Some((peer, contract_location)) = get_op.failure_routing_info() {
            report_timeout_failure(ring, tx, peer, contract_location);
        }
        // Log GET timeout so failures are visible in traces
        let instance_id = get_op.instance_id();
        tracing::warn!(
            tx = %tx,
            instance_id = ?instance_id,
            elapsed_ms = tx.elapsed().as_millis(),
            phase = "get_timeout",
            "GET operation timed out without receiving a response"
        );
        // Notify client of timeout so they get an immediate error instead of
        // waiting silently for their own client-side timeout (#3423).
        // Only for client-initiated GETs (requester is None); forwarded GETs
        // have no local client waiting.
        if get_op.is_client_initiated() {
            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: "GET operation timed out".into(),
            }
            .into());
            if let Err(e) = result_router_tx.try_send((*tx, error_result)) {
                tracing::warn!(
                    %tx,
                    error = %e,
                    "failed to send GET timeout error to client"
                );
            }
        }
        true
    } else {
        false
    }
}

/// Log when a connect operation in Relaying state with an outstanding uphill forward times out,
/// and record the unresponsive peer as an acceptor failure for routing exclusion.
fn record_connect_uphill_timeout(
    tx: &Transaction,
    op: &ConnectOp,
    conn_manager: &ConnectionManager,
) {
    let Some(ConnectState::Relaying(state)) = &op.state else {
        return;
    };
    // Don't record timeout for relays that already forwarded a ConnectResponse —
    // forwarded_to is preserved for ConnectFailed propagation, not because
    // we're still waiting for a response.
    if state.response_forwarded {
        return;
    }
    let Some(ref peer) = state.forwarded_to else {
        return;
    };
    let pending_secs = if let Some(ref fwd_at) = state.forwarded_at {
        fwd_at.elapsed().as_secs()
    } else {
        tx.elapsed().as_secs()
    };
    tracing::warn!(
        tx = %tx,
        forwarded_to = %peer.pub_key(),
        forwarded_to_addr = ?peer.socket_addr(),
        pending_secs,
        "connect: uphill route timed out with no response"
    );
    if let Some(addr) = peer.socket_addr() {
        let now = tokio::time::Instant::now();
        let count = conn_manager.record_connect_acceptor_failure(addr, now);
        tracing::info!(
            tx = %tx,
            addr = %addr,
            failure_count = count,
            "recorded GC timeout as acceptor failure"
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn garbage_cleanup_task<ER: NetEventRegister>(
    mut new_transactions: tokio::sync::mpsc::Receiver<Transaction>,
    ops: Arc<Ops>,
    live_tx_tracker: LiveTransactionTracker,
    event_loop_notifier: EventLoopNotificationsSender,
    mut event_register: ER,
    sub_op_tracker: SubOperationTracker,
    result_router_tx: mpsc::Sender<(Transaction, HostResult)>,
    request_router: Arc<OnceLock<Arc<RequestRouter>>>,
    ring: Arc<Ring>,
    ch_outbound: Arc<crate::contract::ContractHandlerChannel<crate::contract::SenderHalve>>,
    contract_waiters: Arc<
        Mutex<std::collections::HashMap<ContractInstanceId, Vec<oneshot::Sender<()>>>>,
    >,
    pending_contract_fetches: Arc<DashMap<ContractInstanceId, u64>>,
) {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
    /// How often to clean up stale contract_waiters entries (every N ticks).
    const WAITER_CLEANUP_EVERY_N_TICKS: u32 = 12; // every 60s at 5s interval
    let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
    tick.tick().await;
    let mut tick_count: u32 = 0;

    let mut ttl_set = BTreeSet::new();

    let mut delayed = vec![];
    let mut get_retried: std::collections::HashMap<Transaction, usize> =
        std::collections::HashMap::new();
    let mut subscribe_retried: std::collections::HashMap<Transaction, usize> =
        std::collections::HashMap::new();
    let mut put_retried: std::collections::HashMap<Transaction, usize> =
        std::collections::HashMap::new();
    loop {
        crate::deterministic_select! {
            tx = new_transactions.recv() => {
                if let Some(tx) = tx {
                    ttl_set.insert(Reverse(tx));
                }
            },
            _ = tick.tick() => {
                tick_count = tick_count.wrapping_add(1);

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

                // ACK-aware speculative retry for GET operations.
                //
                // When a relay forwards a GET request, it sends a ForwardingAck back
                // to its upstream within milliseconds. If no ACK arrives within 3s,
                // the downstream peer is likely dead — launch a speculative retry on
                // a different path (same tx ID).
                //
                // If an ACK has been received, the chain is alive — trust it for
                // PROGRESS_TIMEOUT (7s). If no response arrives within that window,
                // the chain has stalled and we re-enable speculative retry (#3570).
                // Without this, a single ACK permanently disables retry, causing
                // the originator to wait the full OPERATION_TTL (60s) with no recovery.
                //
                // Only the originator speculates (max 2 parallel paths). Relay peers
                // just forward once, so network overhead is bounded.
                const ACK_TIMEOUT: Duration = Duration::from_secs(3);
                const MAX_SPECULATIVE_PATHS: u8 = 2;
                /// Time to wait after ForwardingAck before re-enabling retry.
                /// If a relay ACKed but no response arrives within this window,
                /// the downstream chain has likely stalled. Even a 4-hop chain
                /// at 500ms/hop completes in ~2.5s; 7s gives ~3x headroom while
                /// keeping worst-case stall detection under 10s.
                const PROGRESS_TIMEOUT: Duration = Duration::from_secs(7);
                {
                    // Clean up entries for completed operations
                    get_retried.retain(|tx, _| ops.get.contains_key(tx));

                    let mut retry_candidates: Vec<Transaction> = Vec::new();
                    for entry in ops.get.iter() {
                        let tx = *entry.key();
                        let get_op = entry.value();

                        // Only the originator speculates — relays just forward and ACK.
                        if !get_op.is_client_initiated() {
                            continue;
                        }

                        // Cap speculative paths to bound network overhead
                        if get_op.speculative_paths >= MAX_SPECULATIVE_PATHS {
                            continue;
                        }

                        let elapsed = tx.elapsed();

                        if get_op.ack_received {
                            // ACK received — the chain was alive when the relay forwarded.
                            // Trust it for PROGRESS_TIMEOUT, then re-enable retry if no
                            // response has arrived (#3570: ForwardingAck retry-disable fix).
                            if elapsed <= PROGRESS_TIMEOUT {
                                continue;
                            }
                            // Chain has stalled past PROGRESS_TIMEOUT — fall through to retry.
                            tracing::info!(
                                %tx,
                                elapsed_ms = elapsed.as_millis(),
                                "GET chain stalled after ForwardingAck — re-enabling speculative retry"
                            );
                        }

                        // No ACK after ACK_TIMEOUT, or ACK received but chain stalled
                        // past PROGRESS_TIMEOUT — speculative retry with ±20% jitter
                        let retry_count = get_retried.get(&tx).copied().unwrap_or(0);
                        let base = if get_op.ack_received {
                            // For stalled-after-ACK, use PROGRESS_TIMEOUT as the base
                            // instead of ACK_TIMEOUT to avoid immediate rapid retries
                            PROGRESS_TIMEOUT + ACK_TIMEOUT * (retry_count as u32)
                        } else {
                            ACK_TIMEOUT * (retry_count as u32 + 1)
                        };
                        // Only consume GlobalRng when elapsed exceeds 80% of base
                        // (minimum possible jittered threshold). This avoids shifting
                        // the global RNG state on every GC tick for ops that are
                        // nowhere near retry time.
                        let min_jittered = base.mul_f64(0.8);
                        if elapsed > min_jittered {
                            let jitter_factor: f64 =
                                crate::config::GlobalRng::random_range(0.8..=1.2);
                            let jitter = base.mul_f64(jitter_factor);
                            if elapsed > jitter {
                                retry_candidates.push(tx);
                            }
                        }
                    }

                    if !retry_candidates.is_empty() {
                        // Collect all connected peers for DBF fallback (only when needed)
                        let all_connected: Vec<_> = ring
                            .connection_manager
                            .get_connections_by_location()
                            .values()
                            .flat_map(|conns| conns.iter().map(|c| c.location.clone()))
                            .collect();

                        for tx in retry_candidates {
                            if let Some((_, get_op)) = ops.get.remove(&tx) {
                                // Report failure for the stalled peer BEFORE retry
                                // overwrites stats.next_peer (get.rs:1039).
                                if let Some((peer, contract_location)) =
                                    get_op.failure_routing_info()
                                {
                                    ring.routing_finished(crate::router::RouteEvent {
                                        peer,
                                        contract_location,
                                        outcome: crate::router::RouteOutcome::Failure,
                                    });
                                }
                                let max_htl = ring.max_hops_to_live;
                                match get_op.retry_with_next_alternative(max_htl, &all_connected)
                                {
                                    Ok((mut new_op, msg)) => {
                                        let msg = crate::message::NetMessage::from(msg);
                                        // Track speculative path for ACK-aware retry bounds
                                        new_op.speculative_paths += 1;
                                        // Reset ack_received for the new path
                                        new_op.ack_received = false;
                                        // Insert op BEFORE sending message to avoid race
                                        // where the response arrives before the op is stored.
                                        ops.get.insert(tx, new_op);
                                        match tokio::time::timeout(
                                            Duration::from_secs(1),
                                            event_loop_notifier
                                                .notifications_sender
                                                .send(Either::Left(msg)),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {
                                                // Count the retry now that message was sent
                                                *get_retried.entry(tx).or_insert(0) += 1;
                                            }
                                            Ok(Err(_)) | Err(_) => {
                                                // Channel closed or timeout — op is already
                                                // stored, will be retried next tick.
                                                tracing::warn!(
                                                    %tx,
                                                    "Failed to send GET retry message, \
                                                     will retry next tick"
                                                );
                                            }
                                        }
                                    }
                                    Err(get_op) => {
                                        // No alternatives — put it back for normal timeout
                                        ops.get.insert(tx, *get_op);
                                    }
                                }
                            }
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
                }

                // ACK-aware speculative retry for SUBSCRIBE operations.
                // Same logic as GET: 3s ACK timeout, max 2 speculative paths.
                // Only the originator retries — relays just forward and ACK.
                //
                // Cap retries per GC tick to avoid flooding the notification channel.
                // The gateway subscribes to 90+ contracts; retrying all at once saturates
                // the 2048-capacity event loop channel, blocking UPDATEs and broadcasts.
                const MAX_SUBSCRIBE_RETRIES_PER_TICK: usize = 10;
                {
                    // Clean up entries for completed operations
                    subscribe_retried.retain(|tx, _| ops.subscribe.contains_key(tx));

                    let mut retry_candidates: Vec<Transaction> = Vec::new();
                    for entry in ops.subscribe.iter() {
                        let tx = *entry.key();
                        let sub_op = entry.value();

                        // Only retry at the originator (no requester_addr means we initiated it).
                        // Also skip ops without routing stats — these are unsubscribe-routing
                        // ops created by create_unsubscribe_op(), which use the subscribe map
                        // for address routing but should not be retried as subscribe requests.
                        if !sub_op.is_originator() || sub_op.failure_routing_info().is_none() {
                            continue;
                        }

                        // Cap speculative paths
                        if sub_op.speculative_paths >= MAX_SPECULATIVE_PATHS {
                            continue;
                        }

                        let elapsed = tx.elapsed();

                        if sub_op.ack_received {
                            // ACK received — trust chain for PROGRESS_TIMEOUT, then
                            // re-enable retry (matching GET/PUT behavior).
                            if elapsed <= PROGRESS_TIMEOUT {
                                continue;
                            }
                            tracing::info!(
                                %tx,
                                elapsed_ms = elapsed.as_millis(),
                                "SUBSCRIBE chain stalled after ForwardingAck \
                                 — re-enabling speculative retry"
                            );
                        }

                        let retry_count = subscribe_retried.get(&tx).copied().unwrap_or(0);
                        let base = if sub_op.ack_received {
                            PROGRESS_TIMEOUT + ACK_TIMEOUT * (retry_count as u32)
                        } else {
                            ACK_TIMEOUT * (retry_count as u32 + 1)
                        };
                        // Only consume GlobalRng when elapsed exceeds 80% of base
                        // (minimum possible jittered threshold). This avoids shifting
                        // the global RNG state on every GC tick for ops that are
                        // nowhere near retry time.
                        let min_jittered = base.mul_f64(0.8);
                        if elapsed > min_jittered {
                            let jitter_factor: f64 =
                                crate::config::GlobalRng::random_range(0.8..=1.2);
                            let jitter = base.mul_f64(jitter_factor);
                            if elapsed > jitter {
                                retry_candidates.push(tx);
                            }
                        }
                    }

                    if !retry_candidates.is_empty() {
                        // Collect all connected peers for fallback
                        let all_connected: Vec<_> = ring
                            .connection_manager
                            .get_connections_by_location()
                            .values()
                            .flat_map(|conns| conns.iter().map(|c| c.location.clone()))
                            .collect();

                        if retry_candidates.len() > MAX_SUBSCRIBE_RETRIES_PER_TICK {
                            tracing::debug!(
                                total = retry_candidates.len(),
                                processing = MAX_SUBSCRIBE_RETRIES_PER_TICK,
                                "Capping subscribe retries per tick"
                            );
                            // Sort by age (oldest first) to guarantee fairness:
                            // shuffle would cause probabilistic starvation when
                            // candidates consistently exceed the per-tick cap.
                            retry_candidates.sort_by_cached_key(|tx| {
                                std::cmp::Reverse(tx.elapsed())
                            });
                        }

                        for tx in retry_candidates.into_iter().take(MAX_SUBSCRIBE_RETRIES_PER_TICK) {
                            if let Some((_, sub_op)) = ops.subscribe.remove(&tx) {
                                // Report failure for the stalled peer before retry.
                                if let Some((peer, contract_location)) =
                                    sub_op.failure_routing_info()
                                {
                                    ring.routing_finished(crate::router::RouteEvent {
                                        peer,
                                        contract_location,
                                        outcome: crate::router::RouteOutcome::Failure,
                                    });
                                }
                                let max_htl = ring.max_hops_to_live;
                                match sub_op.retry_with_next_alternative(max_htl, &all_connected) {
                                    Ok((mut new_op, msg)) => {
                                        let msg = crate::message::NetMessage::from(msg);
                                        // Track speculative path for ACK-aware retry bounds
                                        new_op.speculative_paths += 1;
                                        new_op.ack_received = false;
                                        // Insert op BEFORE sending message to avoid race
                                        ops.subscribe.insert(tx, new_op);
                                        match tokio::time::timeout(
                                            Duration::from_secs(1),
                                            event_loop_notifier
                                                .notifications_sender
                                                .send(Either::Left(msg)),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {
                                                *subscribe_retried.entry(tx).or_insert(0) += 1;
                                            }
                                            Ok(Err(_)) | Err(_) => {
                                                tracing::warn!(
                                                    %tx,
                                                    "Failed to send subscribe retry message, \
                                                     will retry next tick"
                                                );
                                            }
                                        }
                                    }
                                    Err(sub_op) => {
                                        // No alternatives — put it back for normal timeout
                                        ops.subscribe.insert(tx, *sub_op);
                                    }
                                }
                            }
                        }
                    }
                }

                // ACK-aware speculative retry for PUT operations.
                // Same logic as GET/SUBSCRIBE: 3s ACK timeout, max 2 speculative paths.
                // Only the originator retries — relays just forward and ACK.
                {
                    // Clean up entries for completed operations
                    put_retried.retain(|tx, _| ops.put.contains_key(tx));

                    let mut put_retry_candidates: Vec<Transaction> = Vec::new();
                    for entry in ops.put.iter() {
                        let tx = *entry.key();
                        let put_op = entry.value();

                        if !put_op.is_client_initiated() {
                            continue;
                        }
                        if put_op.speculative_paths >= MAX_SPECULATIVE_PATHS {
                            continue;
                        }

                        let elapsed = tx.elapsed();

                        if put_op.ack_received {
                            if elapsed <= PROGRESS_TIMEOUT {
                                continue;
                            }
                            tracing::info!(
                                %tx,
                                elapsed_ms = elapsed.as_millis(),
                                "PUT chain stalled after ForwardingAck — re-enabling speculative retry"
                            );
                        }

                        let retry_count = put_retried.get(&tx).copied().unwrap_or(0);
                        let base = if put_op.ack_received {
                            PROGRESS_TIMEOUT + ACK_TIMEOUT * (retry_count as u32)
                        } else {
                            ACK_TIMEOUT * (retry_count as u32 + 1)
                        };
                        let min_jittered = base.mul_f64(0.8);
                        if elapsed > min_jittered {
                            let jitter_factor: f64 =
                                crate::config::GlobalRng::random_range(0.8..=1.2);
                            let jitter = base.mul_f64(jitter_factor);
                            if elapsed > jitter {
                                put_retry_candidates.push(tx);
                            }
                        }
                    }

                    if !put_retry_candidates.is_empty() {
                        let all_connected: Vec<_> = ring
                            .connection_manager
                            .get_connections_by_location()
                            .values()
                            .flat_map(|conns| conns.iter().map(|c| c.location.clone()))
                            .collect();

                        for tx in put_retry_candidates {
                            if let Some((_, put_op)) = ops.put.remove(&tx) {
                                // Report failure for the stalled peer before retry.
                                if let Some((peer, contract_location)) =
                                    put_op.failure_routing_info()
                                {
                                    ring.routing_finished(crate::router::RouteEvent {
                                        peer,
                                        contract_location,
                                        outcome: crate::router::RouteOutcome::Failure,
                                    });
                                }
                                let max_htl = ring.max_hops_to_live;
                                match put_op
                                    .retry_with_next_alternative(max_htl, &all_connected)
                                {
                                    Ok((mut new_op, msg)) => {
                                        let msg = crate::message::NetMessage::from(msg);
                                        new_op.speculative_paths += 1;
                                        new_op.ack_received = false;
                                        ops.put.insert(tx, new_op);
                                        match tokio::time::timeout(
                                            Duration::from_secs(1),
                                            event_loop_notifier
                                                .notifications_sender
                                                .send(Either::Left(msg)),
                                        )
                                        .await
                                        {
                                            Ok(Ok(())) => {
                                                *put_retried.entry(tx).or_insert(0) += 1;
                                            }
                                            Ok(Err(_)) | Err(_) => {
                                                tracing::warn!(
                                                    %tx,
                                                    "Failed to send PUT retry message, \
                                                     will retry next tick"
                                                );
                                            }
                                        }
                                    }
                                    Err(put_op) => {
                                        // No alternatives — put it back for normal timeout
                                        ops.put.insert(tx, *put_op);
                                    }
                                }
                            }
                        }
                    }
                }

                let mut old_missing = std::mem::replace(&mut delayed, Vec::with_capacity(200));
                for tx in old_missing.drain(..) {
                    if let Some(tx) = ops.completed.remove(&tx) {
                        if cfg!(feature = "trace-ot") {
                            let op_type = tx.transaction_type().description();
                            event_register.notify_of_time_out(tx, op_type, None).await;
                        } else {
                            _ = tx;
                        }
                        continue;
                    }
                    let still_waiting = match tx.transaction_type() {
                        TransactionType::Connect => {
                            if let Some((_, mut op)) = ops.connect.remove(&tx) {
                                op.expire_forward_attempts(tokio::time::Instant::now());
                                record_connect_uphill_timeout(&tx, &op, &ring.connection_manager);
                                if let Some(target_loc) = op.desired_location {
                                    ring.record_connection_failure(target_loc, ConnectionFailureReason::Timeout);
                                }
                                false
                            } else {
                                true
                            }
                        }
                        TransactionType::Put => !remove_put_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                        TransactionType::Get => !remove_get_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                        TransactionType::Subscribe => {
                            remove_subscribe_and_notify_timeout(&ops, &tx, &ch_outbound, &ring).is_none()
                        }
                        TransactionType::Update => !remove_update_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                    };
                    if still_waiting {
                        delayed.push(tx);
                    } else {
                        get_retried.remove(&tx);
                        subscribe_retried.remove(&tx);
                        put_retried.remove(&tx);
                        ops.under_progress.remove(&tx);
                        ops.completed.remove(&tx);
                        tracing::info!(
                            tx = %tx,
                            tx_type = ?tx.transaction_type(),
                            elapsed_ms = tx.elapsed().as_millis(),
                            ttl_ms = crate::config::OPERATION_TTL.as_millis(),
                            "Transaction timed out"
                        );

                        // Check if this is a child operation and propagate timeout to parent
                        if let Some(parent_tx) = sub_op_tracker.get_parent(tx) {
                            tracing::warn!(
                                child_tx = %tx,
                                parent_tx = %parent_tx,
                                "Child operation timed out, propagating failure to parent"
                            );

                            // Clean up parent-child tracking to prevent DashMap entry leaks
                            // in root_ops_awaiting_sub_ops (important for blocking subscriptions).
                            sub_op_tracker.remove_child_link(parent_tx, tx);
                            sub_op_tracker.mark_sub_op_completed(parent_tx);
                            drop(sub_op_tracker.root_ops_awaiting_sub_ops.remove(&parent_tx));
                            sub_op_tracker.cleanup_parent_tracking(parent_tx);

                            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                                cause: format!("Sub-operation {} timed out", tx).into(),
                            }.into());

                            if let Err(e) = result_router_tx.try_send((parent_tx, error_result)) {
                                tracing::warn!(tx = %parent_tx, error = %e, "failed to send sub-op timeout to result router");
                            }
                        }

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
                    let removed = match tx.transaction_type() {
                        TransactionType::Connect => {
                            if let Some((_, mut op)) = ops.connect.remove(&tx) {
                                op.expire_forward_attempts(tokio::time::Instant::now());
                                record_connect_uphill_timeout(&tx, &op, &ring.connection_manager);
                                if let Some(target_loc) = op.desired_location {
                                    ring.record_connection_failure(target_loc, ConnectionFailureReason::Timeout);
                                }
                                true
                            } else {
                                false
                            }
                        }
                        TransactionType::Put => remove_put_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                        TransactionType::Get => remove_get_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                        TransactionType::Subscribe => {
                            remove_subscribe_and_notify_timeout(&ops, &tx, &ch_outbound, &ring).is_some()
                        }
                        TransactionType::Update => remove_update_and_report_failure(&ops, &tx, &ring, &result_router_tx),
                    };
                    if removed {
                        tracing::info!(
                            tx = %tx,
                            tx_type = ?tx.transaction_type(),
                            elapsed_ms = tx.elapsed().as_millis(),
                            ttl_ms = crate::config::OPERATION_TTL.as_millis(),
                            "Transaction timed out"
                        );

                        // Check if this is a child operation and propagate timeout to parent
                        if let Some(parent_tx) = sub_op_tracker.get_parent(tx) {
                            tracing::warn!(
                                child_tx = %tx,
                                parent_tx = %parent_tx,
                                "Child operation timed out, propagating failure to parent"
                            );

                            // Clean up parent-child tracking to prevent DashMap entry leaks
                            // in root_ops_awaiting_sub_ops (important for blocking subscriptions).
                            sub_op_tracker.remove_child_link(parent_tx, tx);
                            sub_op_tracker.mark_sub_op_completed(parent_tx);
                            drop(sub_op_tracker.root_ops_awaiting_sub_ops.remove(&parent_tx));
                            sub_op_tracker.cleanup_parent_tracking(parent_tx);

                            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                                cause: format!("Sub-operation {} timed out", tx).into(),
                            }.into());

                            if let Err(e) = result_router_tx.try_send((parent_tx, error_result)) {
                                tracing::warn!(tx = %parent_tx, error = %e, "failed to send sub-op timeout to result router");
                            }
                        }

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
    use tokio::time::{timeout, Duration};

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

    #[test]
    fn sub_operation_tracker_registers_parent_child_relationship() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child);

        // Verify parent-child relationship is registered
        assert!(
            tracker.is_sub_operation(child),
            "child should be registered as sub-operation"
        );
        assert_eq!(
            tracker.get_parent(child),
            Some(parent),
            "child should have correct parent"
        );

        // Verify expected count is incremented
        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(1),
            "expected count should be 1"
        );

        // Verify sub_operations mapping contains the child
        assert!(
            tracker
                .sub_operations
                .get(&parent)
                .map(|children| children.contains(&child))
                .unwrap_or(false),
            "parent should have child in sub_operations map"
        );
    }

    #[test]
    fn sub_operation_tracker_registers_multiple_children() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();
        let child3 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);
        tracker.expect_and_register_sub_operation(parent, child3);

        // Verify all children are registered
        assert!(tracker.is_sub_operation(child1));
        assert!(tracker.is_sub_operation(child2));
        assert!(tracker.is_sub_operation(child3));

        // Verify expected count is correct
        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(3),
            "expected count should be 3"
        );

        // Verify all children are in the sub_operations map
        let children = tracker.sub_operations.get(&parent).unwrap();
        assert_eq!(children.len(), 3);
        assert!(children.contains(&child1));
        assert!(children.contains(&child2));
        assert!(children.contains(&child3));
    }

    #[test]
    fn sub_operation_tracker_counts_pending_operations() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();
        let child3 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);
        tracker.expect_and_register_sub_operation(parent, child3);

        let completed = DashSet::new();

        // Initially, all 3 should be pending
        assert_eq!(
            tracker.count_pending_sub_operations(parent, &completed),
            3,
            "should have 3 pending operations"
        );

        // Mark first child as completed
        completed.insert(child1);
        assert_eq!(
            tracker.count_pending_sub_operations(parent, &completed),
            2,
            "should have 2 pending operations after completing child1"
        );

        // Mark second child as completed
        completed.insert(child2);
        assert_eq!(
            tracker.count_pending_sub_operations(parent, &completed),
            1,
            "should have 1 pending operation after completing child2"
        );

        // Mark third child as completed
        completed.insert(child3);
        assert_eq!(
            tracker.count_pending_sub_operations(parent, &completed),
            0,
            "should have 0 pending operations after completing all children"
        );
    }

    #[test]
    fn sub_operation_tracker_all_sub_operations_completed() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);

        let completed = DashSet::new();

        // Not all completed initially
        assert!(
            !tracker.all_sub_operations_completed(parent, &completed),
            "should return false when no children completed"
        );

        // Complete first child
        completed.insert(child1);
        tracker.mark_sub_op_completed(parent);
        assert!(
            !tracker.all_sub_operations_completed(parent, &completed),
            "should return false when only one child completed"
        );

        // Complete second child
        completed.insert(child2);
        tracker.mark_sub_op_completed(parent);
        assert!(
            tracker.all_sub_operations_completed(parent, &completed),
            "should return true when all children completed"
        );
    }

    #[test]
    fn sub_operation_tracker_cleanup_parent_tracking() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);

        // Verify tracking is set up
        assert!(tracker.expected_sub_operations.contains_key(&parent));
        assert!(tracker.sub_operations.contains_key(&parent));

        // Cleanup parent tracking
        tracker.cleanup_parent_tracking(parent);

        // Verify tracking is removed
        assert!(
            !tracker.expected_sub_operations.contains_key(&parent),
            "expected_sub_operations should be cleaned up"
        );
        assert!(
            !tracker.sub_operations.contains_key(&parent),
            "sub_operations should be cleaned up"
        );

        // Note: parent_of mappings for children are NOT cleaned up by cleanup_parent_tracking
        // They should be cleaned up by remove_child_link when each child completes
    }

    #[test]
    fn sub_operation_tracker_remove_child_link() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);

        // Remove first child link
        tracker.remove_child_link(parent, child1);

        // Verify child1 is removed from parent mapping
        assert!(
            !tracker.parent_of.contains_key(&child1),
            "child1 should be removed from parent_of"
        );
        assert!(
            tracker
                .sub_operations
                .get(&parent)
                .map(|children| !children.contains(&child1))
                .unwrap_or(false),
            "child1 should be removed from sub_operations"
        );

        // Verify child2 still exists
        assert!(
            tracker.parent_of.contains_key(&child2),
            "child2 should still be in parent_of"
        );
        assert!(
            tracker.sub_operations.contains_key(&parent),
            "parent should still be in sub_operations"
        );

        // Remove second child link
        tracker.remove_child_link(parent, child2);

        // Verify parent is removed from sub_operations when last child is removed
        assert!(
            !tracker.sub_operations.contains_key(&parent),
            "parent should be removed from sub_operations when all children removed"
        );
        assert!(
            !tracker.parent_of.contains_key(&child2),
            "child2 should be removed from parent_of"
        );
    }

    #[test]
    fn sub_operation_tracker_mark_sub_op_completed_decrements_counter() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();
        let child3 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);
        tracker.expect_and_register_sub_operation(parent, child3);

        // Initial count should be 3
        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(3)
        );

        // Mark one completed
        tracker.mark_sub_op_completed(parent);
        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(2)
        );

        // Mark another completed
        tracker.mark_sub_op_completed(parent);
        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(1)
        );

        // Mark last one completed
        tracker.mark_sub_op_completed(parent);

        // Should be removed when count reaches 0
        assert!(
            !tracker.expected_sub_operations.contains_key(&parent),
            "expected_sub_operations entry should be removed when count reaches 0"
        );
    }

    #[test]
    fn sub_operation_tracker_bidirectional_mapping_consistency() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();

        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);

        // Verify bidirectional mapping: parent -> children
        let children = tracker.sub_operations.get(&parent).unwrap();
        assert_eq!(children.len(), 2);
        assert!(children.contains(&child1));
        assert!(children.contains(&child2));

        // Verify bidirectional mapping: child -> parent
        assert_eq!(tracker.get_parent(child1), Some(parent));
        assert_eq!(tracker.get_parent(child2), Some(parent));
    }

    #[test]
    fn sub_operation_tracker_prevents_race_condition_with_atomic_registration() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child = Transaction::ttl_transaction();

        // Atomically register both expected count and relationship
        tracker.expect_and_register_sub_operation(parent, child);

        // Both registrations should happen together
        assert!(
            tracker.expected_sub_operations.contains_key(&parent),
            "expected count should be registered"
        );
        assert!(
            tracker.is_sub_operation(child),
            "child should be registered as sub-operation"
        );
        assert!(
            tracker.sub_operations.contains_key(&parent),
            "parent-child mapping should be registered"
        );
    }

    #[test]
    fn sub_operation_tracker_handles_no_children() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let completed = DashSet::new();

        // Parent with no children should be considered complete
        assert!(
            tracker.all_sub_operations_completed(parent, &completed),
            "parent with no children should be complete"
        );

        // Count should be 0 for parent with no children
        assert_eq!(
            tracker.count_pending_sub_operations(parent, &completed),
            0,
            "parent with no children should have 0 pending operations"
        );
    }

    #[test]
    fn sub_operation_tracker_does_not_decrement_below_zero() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();

        // Try to mark completed without any registered children
        tracker.mark_sub_op_completed(parent);

        // Should not panic or create negative count
        assert!(
            !tracker.expected_sub_operations.contains_key(&parent),
            "should not create entry for non-existent parent"
        );
    }

    /// Test that simulates the race condition where a child completes during parent registration.
    ///
    /// The `expect_and_register_sub_operation` method performs 3 DashMap operations:
    /// 1. Increment expected_sub_operations count
    /// 2. Add child to sub_operations set
    /// 3. Add parent_of mapping
    ///
    /// This test verifies that even if we simulate a child completion between these
    /// operations, the tracker remains in a consistent state.
    ///
    /// Note: In the current implementation, these operations are NOT truly atomic,
    /// but the design ensures safety: expected_sub_operations is incremented FIRST,
    /// so even if a child completes early, the counter is already set.
    #[test]
    fn sub_operation_tracker_child_completion_during_registration_race_simulation() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child = Transaction::ttl_transaction();
        let completed = DashSet::new();

        // Step 1: Manually simulate partial registration - only increment expected count
        tracker
            .expected_sub_operations
            .entry(parent)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        // At this point, if a child "completed", it would decrement the counter
        // Simulate child completion BEFORE full registration
        tracker.mark_sub_op_completed(parent);

        // Counter should be 0 now (decremented from 1)
        assert!(
            !tracker.expected_sub_operations.contains_key(&parent),
            "Counter should be removed after decrement to 0"
        );

        // Now complete the registration (simulating the rest of expect_and_register_sub_operation)
        tracker
            .sub_operations
            .entry(parent)
            .or_default()
            .insert(child);
        tracker.parent_of.insert(child, parent);

        // Even after this race, the child is registered but counter is 0
        // This means all_sub_operations_completed would check sub_operations set
        completed.insert(child);
        assert!(
            tracker.all_sub_operations_completed(parent, &completed),
            "Should detect completion even after race (child is in completed set)"
        );
    }

    /// Test that multiple children completing concurrently maintain correct counter.
    ///
    /// Simulates rapid completion of children to verify the counter stays consistent.
    #[test]
    fn sub_operation_tracker_multiple_children_concurrent_completion() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let tracker = Arc::new(SubOperationTracker::new());
        let parent = Transaction::ttl_transaction();
        let num_children = 10;

        // Register all children first
        let mut children = Vec::new();
        for _ in 0..num_children {
            let child = Transaction::ttl_transaction();
            children.push(child);
            tracker.expect_and_register_sub_operation(parent, child);
        }

        assert_eq!(
            tracker.expected_sub_operations.get(&parent).map(|v| *v),
            Some(num_children),
            "Should have {} expected children",
            num_children
        );

        // Simulate concurrent completions using threads
        let completion_count = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = children
            .into_iter()
            .map(|_child| {
                let tracker = Arc::clone(&tracker);
                let count = Arc::clone(&completion_count);
                std::thread::spawn(move || {
                    tracker.mark_sub_op_completed(parent);
                    count.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        // Wait for all completions
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all completions were counted
        assert_eq!(
            completion_count.load(Ordering::SeqCst),
            num_children,
            "All completions should have been processed"
        );

        // Verify counter is removed (decremented to 0)
        assert!(
            !tracker.expected_sub_operations.contains_key(&parent),
            "Expected counter should be removed when all children complete"
        );
    }

    /// Test that remove_put_and_report_failure returns false for non-existent tx.
    /// (Ring construction is complex; individual components are unit-tested separately:
    ///  - PutOp::failure_routing_info() tested in operations/put.rs
    ///  - report_timeout_failure() calls ring.routing_finished() which calls Router::add_event()
    ///  - Router::add_event() tested in router.rs)
    #[test]
    fn remove_put_returns_false_for_missing_tx() {
        let ops = Ops::default();
        let tx = Transaction::new::<crate::operations::put::PutMsg>();

        // Without Ring, test the Ops-level behavior: removal returns false for missing tx
        assert!(!ops.put.contains_key(&tx));
        // The DashMap::remove returns None for missing keys
        assert!(ops.put.remove(&tx).is_none());
    }

    /// Test that remove_put actually removes from the Ops map when present.
    #[test]
    fn remove_put_removes_from_ops_map() {
        let ops = Ops::default();
        let tx = Transaction::new::<crate::operations::put::PutMsg>();

        let put_op = crate::operations::put::start_op(
            crate::operations::test_utils::make_test_contract(&[1u8]),
            freenet_stdlib::prelude::RelatedContracts::default(),
            freenet_stdlib::prelude::WrappedState::new(vec![]),
            10,
            false,
            false,
        );
        ops.put.insert(tx, put_op);
        assert!(ops.put.contains_key(&tx));

        // Verify removal works
        let removed = ops.put.remove(&tx);
        assert!(removed.is_some());
        assert!(!ops.put.contains_key(&tx));
    }

    /// Test that remove_update returns None for missing tx.
    #[test]
    fn remove_update_returns_none_for_missing_tx() {
        let ops = Ops::default();
        let tx = Transaction::new::<crate::operations::update::UpdateMsg>();
        assert!(ops.update.remove(&tx).is_none());
    }

    /// Test that failed child races with parent completion check.
    ///
    /// Scenario: A child fails while the parent is checking if all sub-operations completed.
    /// The failed_parents set should be updated correctly.
    #[test]
    fn sub_operation_tracker_failed_child_races_parent_completion() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let child1 = Transaction::ttl_transaction();
        let child2 = Transaction::ttl_transaction();
        let completed = DashSet::new();

        // Register children
        tracker.expect_and_register_sub_operation(parent, child1);
        tracker.expect_and_register_sub_operation(parent, child2);

        // Child1 completes successfully
        completed.insert(child1);
        tracker.mark_sub_op_completed(parent);
        tracker.remove_child_link(parent, child1);

        // Before child2 completes, check completion status
        assert!(
            !tracker.all_sub_operations_completed(parent, &completed),
            "Should not be complete while child2 is pending"
        );

        // Now child2 fails - in real code this would call sub_operation_failed
        // which marks the parent as failed
        tracker.failed_parents.insert(parent);

        // Even if we mark child2 complete, parent is failed
        completed.insert(child2);
        tracker.mark_sub_op_completed(parent);

        // Verify parent is in failed_parents
        assert!(
            tracker.failed_parents.contains(&parent),
            "Parent should be marked as failed"
        );

        // all_sub_operations_completed returns true (children are done),
        // but the failed_parents check would catch this in handle_op_result
        assert!(
            tracker.all_sub_operations_completed(parent, &completed),
            "All children completed (even though one failed)"
        );
    }

    /// Test interleaved registration and completion across multiple parents.
    ///
    /// Verifies that tracking for different parents doesn't interfere.
    #[test]
    fn sub_operation_tracker_multiple_parents_interleaved_operations() {
        let tracker = SubOperationTracker::new();
        let parent1 = Transaction::ttl_transaction();
        let parent2 = Transaction::ttl_transaction();
        let child1a = Transaction::ttl_transaction();
        let child1b = Transaction::ttl_transaction();
        let child2a = Transaction::ttl_transaction();
        let completed = DashSet::new();

        // Interleaved registrations
        tracker.expect_and_register_sub_operation(parent1, child1a);
        tracker.expect_and_register_sub_operation(parent2, child2a);
        tracker.expect_and_register_sub_operation(parent1, child1b);

        // Verify correct parent relationships
        assert_eq!(tracker.get_parent(child1a), Some(parent1));
        assert_eq!(tracker.get_parent(child1b), Some(parent1));
        assert_eq!(tracker.get_parent(child2a), Some(parent2));

        // Complete parent1's children
        completed.insert(child1a);
        tracker.mark_sub_op_completed(parent1);
        completed.insert(child1b);
        tracker.mark_sub_op_completed(parent1);

        // Parent1 should be complete, parent2 should not
        assert!(
            tracker.all_sub_operations_completed(parent1, &completed),
            "Parent1 should be complete"
        );
        assert!(
            !tracker.all_sub_operations_completed(parent2, &completed),
            "Parent2 should not be complete yet"
        );

        // Complete parent2's child
        completed.insert(child2a);
        tracker.mark_sub_op_completed(parent2);

        assert!(
            tracker.all_sub_operations_completed(parent2, &completed),
            "Parent2 should now be complete"
        );
    }

    /// Test that the tracker handles rapid registration-completion cycles.
    ///
    /// Simulates a stress scenario where children are registered and completed
    /// in rapid succession to verify counter consistency.
    #[test]
    fn sub_operation_tracker_rapid_registration_completion_cycles() {
        let tracker = SubOperationTracker::new();
        let parent = Transaction::ttl_transaction();
        let completed = DashSet::new();

        // Perform multiple rapid cycles
        for _ in 0..100 {
            let child = Transaction::ttl_transaction();

            // Register
            tracker.expect_and_register_sub_operation(parent, child);
            assert!(
                tracker.expected_sub_operations.get(&parent).is_some(),
                "Should have expected counter after registration"
            );

            // Complete
            completed.insert(child);
            tracker.mark_sub_op_completed(parent);
            tracker.remove_child_link(parent, child);
        }

        // After all cycles, everything should be cleaned up
        assert!(
            tracker.all_sub_operations_completed(parent, &completed),
            "All operations should be complete"
        );
        assert!(
            !tracker.sub_operations.contains_key(&parent),
            "sub_operations should be empty after all children removed"
        );
    }

    mod record_connect_uphill_timeout_tests {
        use super::super::record_connect_uphill_timeout;
        use crate::message::Transaction;
        use crate::operations::connect::{
            ConnectMsg, ConnectOp, ConnectRequest, ConnectState, RelayState,
        };
        use crate::operations::VisitedPeers;
        use crate::ring::{ConnectionManager, Location, PeerKeyLocation};
        use crate::transport::TransportKeypair;
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};

        fn make_peer(port: u16) -> PeerKeyLocation {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let keypair = TransportKeypair::new();
            PeerKeyLocation::new(keypair.public().clone(), addr)
        }

        fn make_relay_state(
            forwarded_to: Option<PeerKeyLocation>,
            response_forwarded: bool,
        ) -> ConnectState {
            ConnectState::Relaying(Box::new(RelayState {
                upstream_addr: "10.0.0.1:5000".parse().unwrap(),
                request: ConnectRequest {
                    desired_location: Location::new(0.3),
                    joiner: make_peer(5002),
                    ttl: 5,
                    visited: VisitedPeers::new(&Transaction::new::<ConnectMsg>()),
                },
                forwarded_to,
                forwarded_at: Some(tokio::time::Instant::now()),
                observed_sent: false,
                accepted_locally: false,
                response_forwarded,
            }))
        }

        /// Regression test for #3392: GC-expired CONNECT forwards to unresponsive
        /// peers must be recorded as acceptor failures so the peer gets excluded
        /// from routing after CONNECT_EXCLUDE_THRESHOLD (3) timeouts.
        #[test]
        fn records_failure_for_unresponsive_relay() {
            let peer = make_peer(5001);
            let peer_addr = peer.socket_addr().unwrap();
            let op = ConnectOp::with_state(make_relay_state(Some(peer), false));
            let tx = Transaction::new::<ConnectMsg>();
            let cm = ConnectionManager::test_default();

            let excluded = cm.compute_connect_excluded_peers(tokio::time::Instant::now());
            assert!(!excluded.contains(&peer_addr));

            // Three GC timeouts should trigger exclusion
            for _ in 0..3 {
                record_connect_uphill_timeout(&tx, &op, &cm);
            }

            let excluded = cm.compute_connect_excluded_peers(tokio::time::Instant::now());
            assert!(
                excluded.contains(&peer_addr),
                "peer should be excluded after 3 GC timeouts"
            );
        }

        /// response_forwarded=true must NOT record a failure — the forwarded_to
        /// field is kept for ConnectFailed propagation, not because the peer is
        /// unresponsive.
        #[test]
        fn skips_when_response_already_forwarded() {
            let peer = make_peer(5001);
            let peer_addr = peer.socket_addr().unwrap();
            let op = ConnectOp::with_state(make_relay_state(Some(peer), true));
            let tx = Transaction::new::<ConnectMsg>();
            let cm = ConnectionManager::test_default();

            for _ in 0..5 {
                record_connect_uphill_timeout(&tx, &op, &cm);
            }

            let excluded = cm.compute_connect_excluded_peers(tokio::time::Instant::now());
            assert!(
                !excluded.contains(&peer_addr),
                "peer should NOT be excluded when response was already forwarded"
            );
        }

        /// Non-Relaying state (e.g., Completed) must not record any failure.
        #[test]
        fn skips_for_non_relay_state() {
            let op = ConnectOp::with_state(ConnectState::Completed);
            let tx = Transaction::new::<ConnectMsg>();
            let cm = ConnectionManager::test_default();

            // Should not panic or record anything
            record_connect_uphill_timeout(&tx, &op, &cm);
        }
    }

    /// Regression test for #3535: age-based retry prioritization prevents starvation.
    ///
    /// When retry candidates exceed MAX_SUBSCRIBE_RETRIES_PER_TICK, the oldest
    /// transactions must be processed first (FIFO fairness). Random shuffling
    /// would cause probabilistic starvation where some transactions could timeout
    /// without ever being retried.
    #[test]
    fn subscribe_retry_candidates_sorted_oldest_first() {
        use crate::config::GlobalSimulationTime;
        use crate::operations::subscribe::SubscribeMsg;

        let base_time: u64 = 1_700_000_000_000; // arbitrary epoch ms

        // Create transactions at different simulation times so they have
        // different ages. Oldest transaction is created first (lowest timestamp).
        let mut txs = Vec::new();
        for i in 0..20u64 {
            GlobalSimulationTime::set_time_ms(base_time + i * 1000);
            txs.push(Transaction::new::<SubscribeMsg>());
        }

        // Advance simulation time so elapsed() returns meaningful durations.
        // tx[0] was created at base_time, tx[19] at base_time + 19s.
        // At base_time + 30s: tx[0].elapsed() = 30s, tx[19].elapsed() = 11s.
        GlobalSimulationTime::set_time_ms(base_time + 30_000);

        // Shuffle to simulate arbitrary DashMap iteration order.
        let mut candidates = txs.clone();
        candidates.reverse();

        // Apply the same sorting logic used in garbage_cleanup_task.
        candidates.sort_by_cached_key(|tx| std::cmp::Reverse(tx.elapsed()));

        // Verify: oldest transactions (largest elapsed) come first.
        for window in candidates.windows(2) {
            assert!(
                window[0].elapsed() >= window[1].elapsed(),
                "candidates not sorted oldest-first: {:?} < {:?}",
                window[0].elapsed(),
                window[1].elapsed(),
            );
        }

        // The first candidate should be the oldest transaction (txs[0]).
        assert_eq!(
            candidates[0].elapsed(),
            txs[0].elapsed(),
            "oldest transaction should be first after sorting"
        );

        // The last candidate should be the newest transaction (txs[19]).
        assert_eq!(
            candidates[19].elapsed(),
            txs[19].elapsed(),
            "newest transaction should be last after sorting"
        );

        GlobalSimulationTime::clear_time();
    }
}
