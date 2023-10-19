use std::{collections::BTreeMap, time::Instant};

use dashmap::DashMap;
use either::Either;
use parking_lot::RwLock;
use tokio::sync::{mpsc::error::SendError, Mutex};

use crate::{
    contract::{
        ContractError, ContractHandlerEvent, ContractHandlerToEventLoopChannel,
        NetEventListenerHalve,
    },
    dev_tool::ClientId,
    message::{Message, Transaction, TransactionType},
    operations::{
        connect::ConnectOp, get::GetOp, put::PutOp, subscribe::SubscribeOp, update::UpdateOp,
        OpEnum, OpError,
    },
    ring::Ring,
};

use super::{conn_manager::EventLoopNotificationsSender, PeerKey};

/// Thread safe and friendly data structure to maintain state of the different operations
/// and enable their execution.
pub(crate) struct OpManager {
    join_ring: DashMap<Transaction, ConnectOp>,
    put: DashMap<Transaction, PutOp>,
    get: DashMap<Transaction, GetOp>,
    subscribe: DashMap<Transaction, SubscribeOp>,
    update: DashMap<Transaction, UpdateOp>,
    to_event_listener: EventLoopNotificationsSender,
    // todo: remove the need for a mutex here
    ch_outbound: Mutex<ContractHandlerToEventLoopChannel<NetEventListenerHalve>>,
    // FIXME: think of an optimal strategy to check for timeouts and clean up garbage
    _ops_ttl: RwLock<BTreeMap<Instant, Vec<Transaction>>>,
    pub ring: Ring,
}

#[cfg(debug_assertions)]
macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpError::IncorrectTxType($var, $get_ty));
        }
    };
}

impl OpManager {
    pub(super) fn new(
        ring: Ring,
        notification_channel: EventLoopNotificationsSender,
        contract_handler: ContractHandlerToEventLoopChannel<NetEventListenerHalve>,
    ) -> Self {
        Self {
            join_ring: DashMap::default(),
            put: DashMap::default(),
            get: DashMap::default(),
            subscribe: DashMap::default(),
            update: DashMap::default(),
            ring,
            to_event_listener: notification_channel,
            ch_outbound: Mutex::new(contract_handler),
            _ops_ttl: RwLock::new(BTreeMap::new()),
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
    ) -> Result<(), SendError<(Message, Option<ClientId>)>> {
        // push back the state to the stack
        self.push(*msg.id(), op).expect("infallible");
        self.to_event_listener
            .send(Either::Left((msg, client_id)))
            .await
            .map_err(|err| SendError(err.0.unwrap_left()))
    }

    // /// Send an internal message to this node event loop.
    // pub async fn notify_internal_op(&self, msg: NodeEvent) -> Result<(), SendError<NodeEvent>> {
    //     self.to_event_listener
    //         .send(Either::Right(msg))
    //         .await
    //         .map_err(|err| SendError(err.0.unwrap_right()))
    // }

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

    pub fn push(&self, id: Transaction, op: OpEnum) -> Result<(), OpError> {
        match op {
            OpEnum::JoinRing(op) => {
                #[cfg(debug_assertions)]
                check_id_op!(id.tx_type(), TransactionType::JoinRing);
                self.join_ring.insert(id, *op);
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

    pub fn pop(&self, id: &Transaction) -> Option<OpEnum> {
        match id.tx_type() {
            TransactionType::JoinRing => self
                .join_ring
                .remove(id)
                .map(|(_k, v)| v)
                .map(|op| OpEnum::JoinRing(Box::new(op))),
            TransactionType::Put => self.put.remove(id).map(|(_k, v)| v).map(OpEnum::Put),
            TransactionType::Get => self.get.remove(id).map(|(_k, v)| v).map(OpEnum::Get),
            TransactionType::Subscribe => self
                .subscribe
                .remove(id)
                .map(|(_k, v)| v)
                .map(OpEnum::Subscribe),
            TransactionType::Update => self.update.remove(id).map(|(_k, v)| v).map(OpEnum::Update),
            TransactionType::Canceled => unreachable!(),
        }
    }

    pub fn prune_connection(&self, peer: PeerKey) {
        // pending ops will be cleaned up by the garbage collector on time out
        self.ring.prune_connection(peer);
    }
}
