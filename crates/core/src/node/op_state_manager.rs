use std::{cmp::Reverse, collections::BTreeSet, sync::Arc, time::Duration};

use dashmap::{DashMap, DashSet};
use either::Either;
use tokio::sync::Mutex;

use crate::{
    config::GlobalExecutor,
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent, SenderHalve},
    dev_tool::ClientId,
    message::{Message, Transaction, TransactionType},
    operations::{
        connect::ConnectOp,
        get::{self, GetOp},
        put::PutOp,
        subscribe::SubscribeOp,
        update::UpdateOp,
        OpEnum, OpError,
    },
    ring::Ring,
};

use super::{network_bridge::EventLoopNotificationsSender, PeerKey};

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

/// Thread safe and friendly data structure to maintain state of the different operations
/// and enable their execution.
pub(crate) struct OpManager {
    connect: Arc<DashMap<Transaction, ConnectOp>>,
    put: Arc<DashMap<Transaction, PutOp>>,
    get: Arc<DashMap<Transaction, GetOp>>,
    subscribe: Arc<DashMap<Transaction, SubscribeOp>>,
    update: Arc<DashMap<Transaction, UpdateOp>>,
    completed: Arc<DashSet<Transaction>>,
    under_progress: Arc<DashSet<Transaction>>,
    to_event_listener: EventLoopNotificationsSender,
    // todo: remove the need for a mutex here if possible
    ch_outbound: Mutex<ContractHandlerChannel<SenderHalve>>,
    new_transactions: tokio::sync::mpsc::Sender<Transaction>,
    pub ring: Arc<Ring>,
}

impl OpManager {
    pub(super) fn new(
        ring: Arc<Ring>,
        notification_channel: EventLoopNotificationsSender,
        contract_handler: ContractHandlerChannel<SenderHalve>,
    ) -> Self {
        let connect = Arc::new(DashMap::new());
        let put = Arc::new(DashMap::new());
        let get = Arc::new(DashMap::new());
        let subscribe = Arc::new(DashMap::new());
        let update = Arc::new(DashMap::new());
        let completed = Arc::new(DashSet::new());
        let under_progress = Arc::new(DashSet::new());

        let (new_transactions, rx) = tokio::sync::mpsc::channel(100);
        GlobalExecutor::spawn(garbage_cleanup_task(
            rx,
            connect.clone(),
            put.clone(),
            get.clone(),
            subscribe.clone(),
            update.clone(),
            completed.clone(),
            under_progress.clone(),
        ));

        Self {
            connect,
            put,
            get,
            subscribe,
            update,
            completed,
            under_progress,
            to_event_listener: notification_channel,
            ch_outbound: Mutex::new(contract_handler),
            new_transactions,
            ring,
        }
    }

    /// An early, fast path, return for communicating back changes of on-going operations
    /// in the node to the main message handler, without any transmission in the network whatsoever.
    ///
    /// Useful when transitioning between states that do not require any network communication
    /// with other nodes, like intermediate states before returning.
    pub async fn notify_op_change(
        &self,
        msg: Message,
        op: OpEnum,
        client_id: Option<ClientId>,
    ) -> Result<(), OpError> {
        // push back the state to the stack
        self.push(*msg.id(), op).await?;
        self.to_event_listener
            .send(Either::Left((msg, client_id)))
            .await
            .map_err(Into::into)
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent,
        client_id: Option<ClientId>,
    ) -> Result<ContractHandlerEvent, ContractError> {
        self.ch_outbound
            .lock()
            .await
            .send_to_handler(msg, client_id)
            .await
    }

    pub async fn recv_from_handler(&self) -> crate::contract::EventId {
        todo!()
    }

    pub async fn push(&self, id: Transaction, op: OpEnum) -> Result<(), OpError> {
        self.under_progress.remove(&id);
        self.new_transactions.send(id).await?;
        match op {
            OpEnum::Connect(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::Connect);
                self.connect.insert(id, *op);
            }
            OpEnum::Put(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::Put);
                self.put.insert(id, op);
            }
            OpEnum::Get(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::Get);
                self.get.insert(id, op);
            }
            OpEnum::Subscribe(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::Subscribe);
                self.subscribe.insert(id, op);
            }
            OpEnum::Update(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::Update);
                self.update.insert(id, op);
            }
        }
        Ok(())
    }

    pub fn pop(&self, id: &Transaction) -> Result<Option<OpEnum>, OpNotAvailable> {
        if self.completed.contains(id) {
            return Err(OpNotAvailable::Completed);
        }
        if self.under_progress.contains(id) {
            return Err(OpNotAvailable::Running);
        }
        let op = match id.tx_type() {
            TransactionType::Connect => self
                .connect
                .remove(id)
                .map(|(_k, v)| v)
                .map(|op| OpEnum::Connect(Box::new(op))),
            TransactionType::Put => self.put.remove(id).map(|(_k, v)| v).map(OpEnum::Put),
            TransactionType::Get => self.get.remove(id).map(|(_k, v)| v).map(OpEnum::Get),
            TransactionType::Subscribe => self
                .subscribe
                .remove(id)
                .map(|(_k, v)| v)
                .map(OpEnum::Subscribe),
            TransactionType::Update => self.update.remove(id).map(|(_k, v)| v).map(OpEnum::Update),
        };
        self.under_progress.insert(*id);
        Ok(op)
    }

    pub fn completed(&self, id: Transaction) {
        self.completed.insert(id);
    }

    pub fn prune_connection(&self, peer: PeerKey) {
        // pending ops will be cleaned up by the garbage collector on time out
        self.ring.prune_connection(peer);
    }
}

#[allow(clippy::too_many_arguments)]
async fn garbage_cleanup_task(
    mut new_transactions: tokio::sync::mpsc::Receiver<Transaction>,
    connect: Arc<DashMap<Transaction, ConnectOp>>,
    put: Arc<DashMap<Transaction, PutOp>>,
    get: Arc<DashMap<Transaction, GetOp>>,
    subscribe: Arc<DashMap<Transaction, SubscribeOp>>,
    update: Arc<DashMap<Transaction, UpdateOp>>,
    completed: Arc<DashSet<Transaction>>,
    under_progress: Arc<DashSet<Transaction>>,
) {
    const CLEANUP_INTERVAL: Duration = Duration::from_secs(5);
    let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
    tick.tick().await;

    let mut ttl_set = BTreeSet::new();

    let remove_old = move |ttl_set: &mut BTreeSet<Reverse<Transaction>>,
                           delayed: &mut Vec<Transaction>| {
        // generate a random id, since those are sortable by time
        // it will allow to get any older transactions, notice the use of reverse
        // so the older transactions are removed instead of the newer ones
        let older_than: Reverse<Transaction> = Reverse(Transaction::new::<get::GetMsg>());
        let mut old_missing = std::mem::replace(delayed, Vec::with_capacity(200));
        for tx in old_missing.drain(..) {
            if completed.remove(&tx).is_some() {
                continue;
            }
            let still_waiting = match tx.tx_type() {
                TransactionType::Connect => connect.remove(&tx).is_none(),
                TransactionType::Put => put.remove(&tx).is_none(),
                TransactionType::Get => get.remove(&tx).is_none(),
                TransactionType::Subscribe => subscribe.remove(&tx).is_none(),
                TransactionType::Update => update.remove(&tx).is_none(),
            };
            if still_waiting {
                delayed.push(tx);
            }
        }
        for Reverse(tx) in ttl_set.split_off(&older_than).into_iter() {
            if under_progress.contains(&tx) {
                delayed.push(tx);
                continue;
            }
            if completed.remove(&tx).is_some() {
                continue;
            }
            match tx.tx_type() {
                TransactionType::Connect => {
                    connect.remove(&tx);
                }
                TransactionType::Put => {
                    put.remove(&tx);
                }
                TransactionType::Get => {
                    get.remove(&tx);
                }
                TransactionType::Subscribe => {
                    subscribe.remove(&tx);
                }
                TransactionType::Update => {
                    update.remove(&tx);
                }
            }
        }
    };

    let mut delayed = vec![];
    loop {
        tokio::select! {
            tx = new_transactions.recv() => {
                if let Some(tx) = tx {
                    ttl_set.insert(Reverse(tx));
                }
            }
            _ = tick.tick() => {
                remove_old(&mut ttl_set, &mut delayed);
            }
        }
    }
}
