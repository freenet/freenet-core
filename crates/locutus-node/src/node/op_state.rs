use dashmap::DashMap;
use tokio::sync::mpsc::{error::SendError, Sender};

use crate::{
    message::{Message, Transaction, TransactionType},
    operations::{get::GetOp, join_ring::JoinRingOp, put::PutOp, Operation},
    ring::Ring,
};

/// Thread safe and friendly data structure to maintain state.
pub(crate) struct OpStateStorage {
    join_ring: DashMap<Transaction, JoinRingOp>,
    put: DashMap<Transaction, PutOp>,
    get: DashMap<Transaction, GetOp>,
    notification_channel: Sender<Message>,
    pub ring: Ring,
}

macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpExecError::IncorrectTxType($var, $get_ty));
        }
    };
}

impl OpStateStorage {
    pub fn new(ring: Ring, notification_channel: Sender<Message>) -> Self {
        Self {
            join_ring: DashMap::default(),
            put: DashMap::default(),
            get: DashMap::default(),
            ring,
            notification_channel,
        }
    }

    /// A fast path for communicating back changes to on-going operations in the node.
    /// This will then be processed back on the message receiving loop.
    pub async fn notify_change(&self, msg: Message) -> Result<(), SendError<Message>> {
        self.notification_channel.send(msg).await
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
