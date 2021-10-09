use std::{collections::BTreeMap, time::Duration};

use dashmap::DashMap;
use tokio::sync::mpsc::{error::SendError, Sender};

use crate::{
    contract::{ContractError, ContractHandlerChannel, ContractHandlerEvent},
    message::{Message, Transaction, TransactionType},
    operations::{get::GetOp, join_ring::JoinRingOp, put::PutOp, Operation},
    ring::Ring,
};

/// Thread safe and friendly data structure to maintain state.
pub(crate) struct OpStateStorage<CErr> {
    join_ring: DashMap<Transaction, JoinRingOp>,
    put: DashMap<Transaction, PutOp>,
    get: DashMap<Transaction, GetOp>,
    notification_channel: Sender<Message>,
    contract_handler: ContractHandlerChannel<CErr>,
    pub ring: Ring,
    // FIXME: think of an optiomal strategy to check for timeouts and clean up garbage
    ops_ttl: BTreeMap<Duration, Vec<Transaction>>,
}

macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpExecError::IncorrectTxType($var, $get_ty));
        }
    };
}

impl<CErr> OpStateStorage<CErr> {
    pub fn new(
        ring: Ring,
        notification_channel: Sender<Message>,
        contract_handler: ContractHandlerChannel<CErr>,
    ) -> Self {
        Self {
            join_ring: DashMap::default(),
            put: DashMap::default(),
            get: DashMap::default(),
            ring,
            notification_channel,
            contract_handler,
            ops_ttl: BTreeMap::new(),
        }
    }

    /// A fast path for communicating back changes to on-going operations in the node.
    /// This will then be processed back on the message receiving loop.
    pub async fn notify_change(&self, msg: Message) -> Result<(), SendError<Message>> {
        self.notification_channel.send(msg).await
    }

    /// Send an event to the contract handler and await a response event from it if successful.
    pub async fn notify_contract_handler(
        &self,
        msg: ContractHandlerEvent<CErr>,
    ) -> Result<ContractHandlerEvent<CErr>, ContractError<CErr>> {
        self.contract_handler.send_to_handler(msg).await
    }

    pub fn push(&self, id: Transaction, op: Operation) -> Result<(), OpExecError> {
        match op {
            Operation::JoinRing(tx) => {
                check_id_op!(id.tx_type(), TransactionType::JoinRing);
                self.join_ring.insert(id, tx);
            }
            Operation::Put(tx) => {
                check_id_op!(id.tx_type(), TransactionType::Put);
                self.put.insert(id, tx);
            }
            Operation::Get(tx) => {
                check_id_op!(id.tx_type(), TransactionType::Put);
                self.get.insert(id, tx);
            }
        }
        Ok(())
    }

    pub fn pop(&self, id: &Transaction) -> Option<Operation> {
        match id.tx_type() {
            TransactionType::JoinRing => self
                .join_ring
                .remove(id)
                .map(|(_k, v)| v)
                .map(Operation::JoinRing),
            TransactionType::Put => self.put.remove(id).map(|(_k, v)| v).map(Operation::Put),
            TransactionType::Get => self.get.remove(id).map(|(_k, v)| v).map(Operation::Get),
            TransactionType::Canceled => todo!(),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub(crate) enum OpExecError {
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionType, TransactionType),
    #[error("failed while processing transaction {0}")]
    TxUpdateFailure(Transaction),
}
