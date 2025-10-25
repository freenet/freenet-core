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
use tokio::sync::mpsc;
use tracing::Instrument;

use crate::{
    client_events::HostResult,
    config::GlobalExecutor,
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent, SenderHalve},
    message::{MessageStats, NetMessage, NodeEvent, Transaction, TransactionType},
    node::PeerId,
    operations::{
        connect::ConnectOp, get::GetOp, put::PutOp, subscribe::SubscribeOp, update::UpdateOp,
        OpEnum, OpError,
    },
    ring::{ConnectionManager, LiveTransactionTracker, Ring},
};

use super::{network_bridge::EventLoopNotificationsSender, NetEventRegister, NodeConfig};

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

#[derive(Default)]
struct Ops {
    connect: DashMap<Transaction, ConnectOp>,
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
    /// Indicates whether the peer is ready to process client operations.
    /// For gateways: always true (peer_id is set from config)
    /// For regular peers: true only after first successful network handshake sets peer_id
    pub peer_ready: Arc<AtomicBool>,
    /// Whether this node is a gateway
    pub is_gateway: bool,
    /// Maps parent transaction to set of child transactions for sub-operation tracking
    sub_operations: Arc<DashMap<Transaction, HashSet<Transaction>>>,
    /// Operations that reached Finished state but have pending children
    pub(crate) pending_finalization: Arc<DashMap<Transaction, OpEnum>>,
    /// Reverse index: child -> parent (for O(1) parent lookup)
    parent_of: Arc<DashMap<Transaction, Transaction>>,
    /// Count of sub-operations that are expected but haven't completed yet
    /// This prevents race condition where sub-op completes before parent reaches finalized state
    expected_sub_operations: Arc<DashMap<Transaction, usize>>,
    /// Tracks parents that experienced a sub-operation failure before they could finalize
    pub(crate) failed_parents: Arc<DashSet<Transaction>>,
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
            peer_ready: self.peer_ready.clone(),
            is_gateway: self.is_gateway,
            sub_operations: self.sub_operations.clone(),
            pending_finalization: self.pending_finalization.clone(),
            parent_of: self.parent_of.clone(),
            expected_sub_operations: self.expected_sub_operations.clone(),
            failed_parents: self.failed_parents.clone(),
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
        GlobalExecutor::spawn(
            garbage_cleanup_task(
                rx,
                ops.clone(),
                ring.live_tx_tracker.clone(),
                notification_channel.clone(),
                event_register,
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

        Ok(Self {
            ring,
            ops,
            to_event_listener: notification_channel,
            ch_outbound: Arc::new(ch_outbound),
            new_transactions,
            result_router_tx,
            peer_ready,
            is_gateway,
            sub_operations: Arc::new(DashMap::new()),
            pending_finalization: Arc::new(DashMap::new()),
            parent_of: Arc::new(DashMap::new()),
            expected_sub_operations: Arc::new(DashMap::new()),
            failed_parents: Arc::new(DashSet::new()),
        })
    }

    fn decrement_expected_counter(&self, parent_tx: Transaction) {
        tracing::debug!("decrement_expected_counter START: parent={}", parent_tx);
        let mut should_remove = false;
        tracing::debug!("decrement_expected_counter: attempting get_mut");
        if let Some(mut expected) = self.expected_sub_operations.get_mut(&parent_tx) {
            tracing::debug!("decrement_expected_counter: got mut, value={}", *expected);
            if *expected > 0 {
                *expected -= 1;
                tracing::debug!(
                    parent_tx = %parent_tx,
                    expected_remaining = *expected,
                    "decremented expected sub-operation count"
                );
            }
            if *expected == 0 {
                should_remove = true;
            }
        } else {
            tracing::debug!(
                "decrement_expected_counter: parent not found in expected_sub_operations"
            );
        }

        if should_remove {
            tracing::debug!("decrement_expected_counter: removing entry");
            self.expected_sub_operations.remove(&parent_tx);
        }
        tracing::debug!("decrement_expected_counter END");
    }

    fn remove_child_link(&self, parent: Transaction, child: Transaction) {
        tracing::debug!(
            "remove_child_link START: parent={}, child={}",
            parent,
            child
        );

        // Track if we need to remove the parent entry
        let should_remove_parent = if let Some(mut children) = self.sub_operations.get_mut(&parent)
        {
            tracing::debug!("remove_child_link: got mut children, removing child");
            children.remove(&child);
            let is_empty = children.is_empty();
            tracing::debug!("remove_child_link: children empty={}", is_empty);
            // Explicitly drop the lock BEFORE we do other operations
            drop(children);
            is_empty
        } else {
            false
        };

        // Now that the lock is released, we can safely do other operations
        if should_remove_parent {
            tracing::debug!("remove_child_link: removing parent entry from sub_operations");
            self.sub_operations.remove(&parent);
        }

        tracing::debug!("remove_child_link: removing from parent_of");
        self.parent_of.remove(&child);
        tracing::debug!("remove_child_link END");
    }

    fn cleanup_parent_tracking(&self, parent: Transaction) {
        self.expected_sub_operations.remove(&parent);
        self.sub_operations.remove(&parent);
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
        self.to_event_listener
            .notifications_sender
            .send(Either::Right(msg))
            .await
            .map_err(Into::into)
    }

    /// Get all network subscription information
    /// Returns a map of contract keys to lists of subscribing peers
    pub fn get_network_subscriptions(&self) -> Vec<(ContractKey, Vec<PeerId>)> {
        self.ring
            .all_network_subscriptions()
            .into_iter()
            .map(|(contract_key, subscribers)| {
                let peer_ids: Vec<PeerId> = subscribers.into_iter().map(|sub| sub.peer).collect();
                (contract_key, peer_ids)
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

        tracing::debug!(
            "completed() called for tx={} | is_sub_op={} | parent_of contains key={}",
            id,
            self.is_sub_operation(id),
            self.parent_of.contains_key(&id)
        );

        // Check if this is a sub-operation that can trigger parent finalization
        if let Some(parent_entry) = self.parent_of.get(&id) {
            let parent_tx = *parent_entry;
            // CRITICAL: Drop the read lock BEFORE calling remove_child_link
            // to prevent deadlock when remove_child_link tries to write-lock parent_of
            drop(parent_entry);
            tracing::debug!("✓ Found parent {} for child {}", parent_tx, id);

            // Update bookkeeping so only outstanding children remain tracked
            tracing::debug!("Before remove_child_link");
            self.remove_child_link(parent_tx, id);
            tracing::debug!("After remove_child_link");

            tracing::debug!("Before decrement_expected_counter");
            self.decrement_expected_counter(parent_tx);
            tracing::debug!("After decrement_expected_counter");

            tracing::debug!("Before count_pending_sub_operations");
            let remaining = self.count_pending_sub_operations(parent_tx);
            tracing::debug!("Remaining sub-ops for parent {}: {}", parent_tx, remaining);

            tracing::info!(
                ">>> SUB-OP FINISHED | child={} ({:?}) | parent={} ({:?}) | remaining={}",
                id,
                id.transaction_type(),
                parent_tx,
                parent_tx.transaction_type(),
                remaining
            );

            // Check if ALL siblings are now complete
            if self.all_sub_operations_completed(parent_tx) {
                // All siblings completed, check if parent is pending finalization
                if let Some((_key, parent_op)) = self.pending_finalization.remove(&parent_tx) {
                    tracing::warn!(
                        ">>> PARENT UNBLOCKED | parent={} ({:?}) | All sub-operations done",
                        parent_tx,
                        parent_tx.transaction_type()
                    );

                    self.cleanup_parent_tracking(parent_tx);
                    self.failed_parents.remove(&parent_tx);
                    self.completed(parent_tx);

                    let host_result = parent_op.to_host_result();
                    self.spawn_client_result(parent_tx, host_result);
                } else {
                    tracing::debug!(
                        parent_tx = %parent_tx,
                        "all sub-operations complete but parent not deferred"
                    );
                }
            } else {
                tracing::debug!(
                    parent_tx = %parent_tx,
                    completed_child = %id,
                    remaining_sub_ops = remaining,
                    "parent still waiting for sub-operations"
                );
            }
        } else {
            // This is a top-level operation (not a sub-operation)
            tracing::debug!(
                "Operation completed (not a sub-operation) | TX: {} ({})",
                id,
                id.transaction_type()
            );
        }
    }

    /// Register that a sub-operation is expected for a parent.
    /// MUST be called BEFORE initiating the sub-operation to prevent race conditions.
    pub fn expect_sub_operation(&self, parent: Transaction) {
        self.expected_sub_operations
            .entry(parent)
            .and_modify(|count| *count += 1)
            .or_insert(1);

        tracing::debug!(
            parent_tx = %parent,
            expected_count = self.expected_sub_operations.get(&parent).map(|e| *e).unwrap_or(0),
            "incremented expected sub-operation count"
        );
    }

    /// Register a sub-operation relationship between parent and child transactions.
    pub fn register_sub_operation(&self, parent: Transaction, child: Transaction) {
        self.sub_operations
            .entry(parent)
            .or_insert_with(HashSet::new)
            .insert(child);

        self.parent_of.insert(child, parent);

        tracing::debug!(
            parent_tx = %parent,
            child_tx = %child,
            "registered sub-operation relationship"
        );
    }

    /// Check if all sub-operations of a parent have completed.
    pub fn all_sub_operations_completed(&self, parent: Transaction) -> bool {
        // First check if there are still expected sub-operations that haven't completed
        if let Some(expected) = self.expected_sub_operations.get(&parent) {
            if *expected > 0 {
                tracing::debug!(
                    parent_tx = %parent,
                    expected_remaining = *expected,
                    "parent has expected sub-operations remaining"
                );
                return false;
            }
        }

        // Then check if all registered sub-operations have completed
        match self.sub_operations.get(&parent) {
            None => true, // No sub-operations
            Some(children) => children
                .iter()
                .all(|child| self.ops.completed.contains(child)),
        }
    }

    /// Count how many sub-operations are still pending for a parent.
    pub fn count_pending_sub_operations(&self, parent: Transaction) -> usize {
        tracing::debug!("count_pending_sub_operations START: parent={}", parent);
        let count = match self.sub_operations.get(&parent) {
            None => {
                tracing::debug!("count_pending_sub_operations: no children found");
                0
            }
            Some(children) => {
                tracing::debug!(
                    "count_pending_sub_operations: found {} children",
                    children.len()
                );
                let pending = children
                    .iter()
                    .filter(|child| !self.ops.completed.contains(child))
                    .count();
                tracing::debug!("count_pending_sub_operations: {} pending", pending);
                pending
            }
        };
        tracing::debug!("count_pending_sub_operations END: count={}", count);
        count
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
            "Sub-operation failed, propagating to parent"
        );

        if let Some(parent_entry) = self.parent_of.get(&child) {
            let parent_tx = *parent_entry;
            self.remove_child_link(parent_tx, child);
            self.decrement_expected_counter(parent_tx);

            let error_result = Err(freenet_stdlib::client_api::ErrorKind::OperationError {
                cause: format!("Sub-operation {} failed: {}", child, error_msg).into(),
            }
            .into());

            if let Some((_key, _)) = self.pending_finalization.remove(&parent_tx) {
                tracing::warn!(
                    parent_tx = %parent_tx,
                    child_tx = %child,
                    "Parent operation aborted due to sub-operation failure"
                );
                self.cleanup_parent_tracking(parent_tx);
                self.completed(parent_tx);
            } else {
                tracing::warn!(
                    parent_tx = %parent_tx,
                    child_tx = %child,
                    "Sub-operation failed before parent entered pending finalization"
                );
                self.failed_parents.insert(parent_tx);
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
        self.parent_of.contains_key(&tx)
    }

    /// Notify the operation manager that a transaction is being transacted over the network.
    pub fn sending_transaction(&self, peer: &PeerId, msg: &NetMessage) {
        let transaction = msg.id();
        if let (Some(recipient), Some(target)) = (msg.target(), msg.requested_location()) {
            self.ring
                .record_request(recipient.clone(), target, transaction.transaction_type());
        }
        self.ring
            .live_tx_tracker
            .add_transaction(peer.clone(), *transaction);
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
                    if still_waiting  {
                        delayed.push(tx);
                    } else {
                        ops.under_progress.remove(&tx);
                        ops.completed.remove(&tx);
                        tracing::debug!("Transaction timed out: {tx}");
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
                        tracing::debug!("Transaction timed out: {tx}");
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
}
