use std::{collections::HashMap, sync::Arc};

use crate::{
    message::{Transaction, TransactionTypeId},
    operations::{
        join_ring::{self, JoinRingOp},
        put::{self, PutOp},
        Operation,
    },
    ring::Ring,
};

pub(crate) struct OpStateStorage {
    join_ring: HashMap<Transaction, JoinRingOp>,
    put: HashMap<Transaction, PutOp>,
    pub ring: Arc<Ring>,
}

macro_rules! check_id_op {
    ($get_ty:expr, $var:path) => {
        if !matches!($get_ty, $var) {
            return Err(OpExecutionError::IncorrectTxType(
                TransactionTypeId::JoinRing,
                $get_ty,
            ));
        }
    };
}

impl OpStateStorage {
    pub fn new() -> Self {
        Self {
            join_ring: HashMap::default(),
            put: HashMap::default(),
            ring: Arc::new(Ring::new()),
        }
    }

    pub fn push(&mut self, id: Transaction, op: Operation) -> Result<(), OpExecutionError> {
        match op {
            Operation::JoinRing(tx) => {
                check_id_op!(id.tx_type(), TransactionTypeId::JoinRing);
                self.join_ring.insert(id, tx);
            }
            Operation::Put(tx) => {
                check_id_op!(id.tx_type(), TransactionTypeId::Put);
                self.put.insert(id, tx);
            }
        }
        Ok(())
    }

    pub fn pop(&mut self, id: &Transaction) -> Option<Operation> {
        match id.tx_type() {
            TransactionTypeId::JoinRing => self.join_ring.remove(id).map(Operation::JoinRing),
            TransactionTypeId::Put => self.put.remove(id).map(Operation::Put),
            TransactionTypeId::Canceled => todo!(),
        }
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub(crate) enum OpExecutionError {
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionTypeId, TransactionTypeId),
    #[error("failed while processing transaction {0}")]
    TxUpdateFailure(Transaction),
}
