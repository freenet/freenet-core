use std::{cmp::Reverse, collections::BTreeSet, sync::Arc, time::Duration};

use dashmap::{DashMap, DashSet};
use either::Either;
use tracing::Instrument;

use crate::{
    config::GlobalExecutor,
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent, SenderHalve},
    message::{MessageStats, NetMessage, Transaction, TransactionType},
    operations::{
        connect::ConnectOp, get::GetOp, put::PutOp, subscribe::SubscribeOp, update::UpdateOp,
        OpEnum, OpError,
    },
    ring::{LiveTransactionTracker, Ring},
};

use super::{network_bridge::EventLoopNotificationsSender, NetEventRegister, NodeConfig, PeerId};

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
    to_event_listener: EventLoopNotificationsSender,
    pub ch_outbound: ContractHandlerChannel<SenderHalve>,
    new_transactions: tokio::sync::mpsc::Sender<Transaction>,
}

impl OpManager {
    pub(super) fn new<ER: NetEventRegister + Clone>(
        notification_channel: EventLoopNotificationsSender,
        ch_outbound: ContractHandlerChannel<SenderHalve>,
        config: &NodeConfig,
        event_register: ER,
    ) -> Result<Self, anyhow::Error> {
        let ring = Ring::new(
            config,
            notification_channel.clone(),
            event_register.clone(),
            config.is_gateway,
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
                event_register,
            )
            .instrument(garbage_span),
        );

        Ok(Self {
            ring,
            ops,
            to_event_listener: notification_channel,
            ch_outbound,
            new_transactions,
        })
    }

    /// An early, fast path, return for communicating back changes of on-going operations
    /// in the node to the main message handler, without any transmission in the network whatsoever.
    ///
    /// Useful when transitioning between states that do not require any network communication
    /// with other nodes, like intermediate states before returning.
    pub async fn notify_op_change(&self, msg: NetMessage, op: OpEnum) -> Result<(), OpError> {
        // push back the state to the stack
        self.push(*msg.id(), op).await?;
        self.to_event_listener
            .send(Either::Left(msg))
            .await
            .map_err(Into::into)
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound.send_to_handler(msg).await
    }

    pub async fn push(&self, id: Transaction, op: OpEnum) -> Result<(), OpError> {
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
        self.ops.completed.insert(id);
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

async fn garbage_cleanup_task<ER: NetEventRegister>(
    mut new_transactions: tokio::sync::mpsc::Receiver<Transaction>,
    ops: Arc<Ops>,
    live_tx_tracker: LiveTransactionTracker,
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
                    let timed_out = tx.timed_out();
                    if still_waiting && !timed_out {
                        delayed.push(tx);
                    } else {
                        if still_waiting && timed_out {
                            ops.under_progress.remove(&tx);
                            ops.completed.remove(&tx);
                        }
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
                        if cfg!(feature = "trace-ot") {
                            event_register.notify_of_time_out(tx).await;
                        } else {
                            _ = tx;
                        }
                        continue;
                    }
                    let removed = match tx.transaction_type() {
                        TransactionType::Connect => ops.connect.remove(&tx).is_some(),
                        TransactionType::Put => ops.put.remove(&tx).is_some(),
                        TransactionType::Get => ops.get.remove(&tx).is_some(),
                        TransactionType::Subscribe => ops.subscribe.remove(&tx).is_some(),
                        TransactionType::Update => ops.update.remove(&tx).is_some(),
                    };
                    if removed {
                        live_tx_tracker.remove_finished_transaction(tx);
                    }
                }
            }
        }
    }
}
