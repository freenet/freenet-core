use std::{collections::BTreeMap, time::Instant};

use dashmap::DashMap;
use either::Either;
use parking_lot::RwLock;
use tokio::sync::{
    mpsc::{error::SendError, Sender},
    Mutex,
};

use crate::{
    contract::{CHSenderHalve, ContractError, ContractHandlerChannel, ContractHandlerEvent},
    message::{Message, NodeEvent, Transaction, TransactionType},
    operations::{
        get::GetOp, join_ring::JoinRingOp, put::PutOp, subscribe::SubscribeOp, OpEnum, OpError,
    },
    ring::Ring,
};

use super::PeerKey;

/// Thread safe and friendly data structure to maintain state of the different operations
/// and enable their execution.
pub(crate) struct OpManager<CErr> {
    join_ring: DashMap<Transaction, JoinRingOp>,
    put: DashMap<Transaction, PutOp>,
    get: DashMap<Transaction, GetOp>,
    subscribe: DashMap<Transaction, SubscribeOp>,
    notification_channel: Sender<Either<Message, NodeEvent>>,
    contract_handler: Mutex<ContractHandlerChannel<CErr, CHSenderHalve>>,
    // FIXME: think of an optimal strategy to check for timeouts and clean up garbage
    _ops_ttl: RwLock<BTreeMap<Instant, Vec<Transaction>>>,
    pub ring: Ring,
}

macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpError::IncorrectTxType($var, $get_ty));
        }
    };
}

impl<CErr> OpManager<CErr>
where
    CErr: std::error::Error,
{
    pub fn new(
        ring: Ring,
        notification_channel: Sender<Either<Message, NodeEvent>>,
        contract_handler: ContractHandlerChannel<CErr, CHSenderHalve>,
    ) -> Self {
        Self {
            join_ring: DashMap::default(),
            put: DashMap::default(),
            get: DashMap::default(),
            subscribe: DashMap::default(),
            ring,
            notification_channel,
            contract_handler: Mutex::new(contract_handler),
            _ops_ttl: RwLock::new(BTreeMap::new()),
        }
    }

    /// An early, fast path, return for communicating back changes of on-going operations
    /// in the node to the main message handler receiving loop, without any transmission in
    /// the network whatsoever.
    ///
    /// Useful when transitioning between states that do not require any network communication
    /// with other nodes, like intermediate states before returning.
    pub async fn notify_op_change(
        &self,
        msg: Message,
        op: OpEnum,
    ) -> Result<(), SendError<Message>> {
        // push back the state to the stack
        self.push(*msg.id(), op).expect("infallible");
        self.notification_channel
            .send(Either::Left(msg))
            .await
            .map_err(|err| SendError(err.0.unwrap_left()))
    }

    /// Send an internal message to this node event loop.
    pub async fn notify_internal_op(&self, msg: NodeEvent) -> Result<(), SendError<NodeEvent>> {
        self.notification_channel
            .send(Either::Right(msg))
            .await
            .map_err(|err| SendError(err.0.unwrap_right()))
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent<CErr>,
    ) -> Result<ContractHandlerEvent<CErr>, ContractError<CErr>> {
        self.contract_handler
            .lock()
            .await
            .send_to_handler(msg)
            .await
    }

    pub fn push(&self, id: Transaction, op: OpEnum) -> Result<(), OpError<CErr>> {
        match op {
            OpEnum::JoinRing(tx) => {
                check_id_op!(id.tx_type(), TransactionType::JoinRing);
                self.join_ring.insert(id, *tx);
            }
            OpEnum::Put(tx) => {
                check_id_op!(id.tx_type(), TransactionType::Put);
                self.put.insert(id, tx);
            }
            OpEnum::Get(tx) => {
                check_id_op!(id.tx_type(), TransactionType::Get);
                self.get.insert(id, tx);
            }
            OpEnum::Subscribe(tx) => {
                check_id_op!(id.tx_type(), TransactionType::Subscribe);
                self.subscribe.insert(id, tx);
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
            TransactionType::Canceled => unreachable!(),
        }
    }

    pub fn prune_connection(&self, peer: PeerKey) {
        // pending ops will be cleaned up by the garbage collector on time out
        self.ring.prune_connection(peer);
    }
}
