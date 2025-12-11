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
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};

use dashmap::{DashMap, DashSet};
use either::Either;
use freenet_stdlib::prelude::ContractKey;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::{
    client_events::HostResult,
    config::GlobalExecutor,
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent, SenderHalve},
    message::{MessageStats, NetMessage, NodeEvent, Transaction, TransactionType},
    operations::{
        connect::ConnectForwardEstimator, get::GetOp, put::PutOp, subscribe::SubscribeOp,
        update::UpdateOp, OpEnum, OpError,
    },
    ring::{ConnectionManager, LiveTransactionTracker, PeerKeyLocation, Ring},
};

use super::{
    network_bridge::EventLoopNotificationsSender, proximity_cache::ProximityCacheManager,
    NetEventRegister, NodeConfig,
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
    contract_waiters: Arc<Mutex<std::collections::HashMap<ContractKey, Vec<oneshot::Sender<()>>>>>,
    /// Proximity cache manager for tracking neighbor contract caches
    pub proximity_cache: Arc<ProximityCacheManager>,
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
            proximity_cache: self.proximity_cache.clone(),
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
    ) -> anyhow::Result<Self> {
        let ring = Ring::new(
            config,
            notification_channel.clone(),
            event_register.clone(),
            config.is_gateway,
            connection_manager,
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

        GlobalExecutor::spawn(
            garbage_cleanup_task(
                rx,
                ops.clone(),
                ring.live_tx_tracker.clone(),
                notification_channel.clone(),
                event_register,
                sub_op_tracker.clone(),
                result_router_tx.clone(),
            )
            .instrument(garbage_span),
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

        let proximity_cache = Arc::new(ProximityCacheManager::new());

        Ok(Self {
            ring,
            ops,
            to_event_listener: notification_channel,
            ch_outbound: Arc::new(ch_outbound),
            new_transactions,
            result_router_tx,
            connect_forward_estimator,
            peer_ready,
            is_gateway,
            sub_op_tracker,
            contract_waiters: Arc::new(Mutex::new(std::collections::HashMap::new())),
            proximity_cache,
        })
    }

    fn spawn_client_result(&self, tx: Transaction, host_result: HostResult) {
        let router_tx = self.result_router_tx.clone();
        let notifier = self.to_event_listener.clone();
        GlobalExecutor::spawn(async move {
            if let Err(err) = router_tx.send((tx, host_result)).await {
                tracing::error!(
                    %tx,
                    error = %err,
                    "failed to dispatch operation result to client"
                );
                return;
            }

            if let Err(err) = notifier
                .notifications_sender
                .send(Either::Right(NodeEvent::TransactionCompleted(tx)))
                .await
            {
                tracing::warn!(
                    %tx,
                    error = %err,
                    "failed to notify event loop about transaction completion"
                );
            }
        });
    }

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

        self.to_event_listener
            .notifications_sender()
            .send(Either::Left(msg))
            .await?;

        tracing::debug!(
            tx = %tx,
            peer = %peer_id,
            "notify_op_change: Notification sent successfully"
        );

        Ok(())
    }

    // An early, fast path, return for communicating events in the node to the main message handler,
    // without any transmission in the network whatsoever and avoiding any state transition.
    //
    // Useful when we want to notify connection attempts, or other events that do not require any
    // network communication with other nodes.
    pub async fn notify_node_event(&self, msg: NodeEvent) -> Result<(), OpError> {
        tracing::info!(event = %msg, "notify_node_event: queuing node event");
        self.to_event_listener
            .notifications_sender
            .send(Either::Right(msg))
            .await
            .map_err(Into::into)
    }

    /// Get all network subscription information
    /// Returns a map of contract keys to lists of subscribing peers (as PeerKeyLocations)
    pub fn get_network_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerKeyLocation>)> {
        self.ring
            .all_network_subscriptions()
            .into_iter()
            .map(|(contract_key, subscribers)| {
                let peers: Vec<PeerKeyLocation> = subscribers.into_iter().collect();
                (contract_key, peers)
            })
            .collect()
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
            _ => None,
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
                    self.spawn_client_result(parent_tx, host_result);
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

            self.spawn_client_result(parent_tx, error_result);
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
    pub fn wait_for_contract(&self, key: ContractKey) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut waiters = self.contract_waiters.lock();
        waiters.entry(key).or_default().push(tx);
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
        if let Some(senders) = waiters.remove(key) {
            let count = senders.len();
            for sender in senders {
                // Ignore errors if receiver was dropped (e.g., operation timed out)
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
}

async fn notify_transaction_timeout(
    event_loop_notifier: &EventLoopNotificationsSender,
    tx: Transaction,
) -> bool {
    match event_loop_notifier
        .notifications_sender
        .send(Either::Right(NodeEvent::TransactionTimedOut(tx)))
        .await
    {
        Ok(()) => true,
        Err(err) => {
            tracing::warn!(
                tx = %tx,
                error = ?err,
                "Failed to notify event loop about timed out transaction; receiver likely dropped"
            );
            false
        }
    }
}

async fn garbage_cleanup_task<ER: NetEventRegister>(
    mut new_transactions: tokio::sync::mpsc::Receiver<Transaction>,
    ops: Arc<Ops>,
    live_tx_tracker: LiveTransactionTracker,
    event_loop_notifier: EventLoopNotificationsSender,
    mut event_register: ER,
    sub_op_tracker: SubOperationTracker,
    result_router_tx: mpsc::Sender<(Transaction, HostResult)>,
) {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
    let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
    tick.tick().await;

    let mut ttl_set = BTreeSet::new();

    let mut delayed = vec![];
    loop {
        tokio::select! {
            tx = new_transactions.recv() => {
                if let Some(tx) = tx {
                    ttl_set.insert(Reverse(tx));
                }
            }
            _ = tick.tick() => {
                let mut old_missing = std::mem::replace(&mut delayed, Vec::with_capacity(200));
                for tx in old_missing.drain(..) {
                    if let Some(tx) = ops.completed.remove(&tx) {
                        if cfg!(feature = "trace-ot") {
                            event_register.notify_of_time_out(tx).await;
                        } else {
                            _ = tx;
                        }
                        continue;
                    }
                    let still_waiting = match tx.transaction_type() {
                        TransactionType::Connect => ops.connect.remove(&tx).is_none(),
                        TransactionType::Put => ops.put.remove(&tx).is_none(),
                        TransactionType::Get => ops.get.remove(&tx).is_none(),
                        TransactionType::Subscribe => ops.subscribe.remove(&tx).is_none(),
                        TransactionType::Update => ops.update.remove(&tx).is_none(),
                    };
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

                        // Check if this is a child operation and propagate timeout to parent
                        if let Some(parent_tx) = sub_op_tracker.get_parent(tx) {
                            tracing::warn!(
                                child_tx = %tx,
                                parent_tx = %parent_tx,
                                "Child operation timed out, propagating failure to parent"
                            );

                            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                                cause: format!("Sub-operation {} timed out", tx).into(),
                            }.into());

                            let _ = result_router_tx.send((parent_tx, error_result)).await;
                        }

                        notify_transaction_timeout(&event_loop_notifier, tx).await;
                        live_tx_tracker.remove_finished_transaction(tx);
                    }
                }

                // notice the use of reverse so the older transactions are removed instead of the newer ones
                let older_than: Reverse<Transaction> = Reverse(Transaction::ttl_transaction());
                for Reverse(tx) in ttl_set.split_off(&older_than).into_iter() {
                    if ops.under_progress.contains(&tx) {
                        delayed.push(tx);
                        continue;
                    }
                    if let Some(tx) = ops.completed.remove(&tx) {
                        tracing::debug!("Clean up timed out: {tx}");
                        if cfg!(feature = "trace-ot") {
                            event_register.notify_of_time_out(tx).await;
                        } else {
                            _ = tx;
                        }
                    }
                    let removed = match tx.transaction_type() {
                        TransactionType::Connect => ops.connect.remove(&tx).is_some(),
                        TransactionType::Put => ops.put.remove(&tx).is_some(),
                        TransactionType::Get => ops.get.remove(&tx).is_some(),
                        TransactionType::Subscribe => ops.subscribe.remove(&tx).is_some(),
                        TransactionType::Update => ops.update.remove(&tx).is_some(),
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

                            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                                cause: format!("Sub-operation {} timed out", tx).into(),
                            }.into());

                            let _ = result_router_tx.send((parent_tx, error_result)).await;
                        }

                        notify_transaction_timeout(&event_loop_notifier, tx).await;
                        live_tx_tracker.remove_finished_transaction(tx);
                    }
                }
            }
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

        let delivered = notify_transaction_timeout(&notifier, tx).await;
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
            other => panic!("unexpected notification: {other:?}"),
        }
    }

    #[tokio::test]
    async fn notify_timeout_handles_dropped_receiver() {
        let (receiver, notifier) = event_loop_notification_channel();
        drop(receiver);

        let tx = Transaction::ttl_transaction();

        let delivered = notify_transaction_timeout(&notifier, tx).await;
        assert!(
            !delivered,
            "notification delivery should fail once receiver is dropped"
        );
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
}
