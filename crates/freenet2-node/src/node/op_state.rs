use std::sync::Arc;

use crate::{
    message::{Transaction, TransactionTypeId},
    operations::{join_ring, put, OpsMap},
    ring::Ring,
};

pub(crate) struct OpStateStorage {
    ops: OpsMap,
    pub ring: Arc<Ring>,
}

impl OpStateStorage {
    pub fn new() -> Self {
        Self {
            ops: OpsMap::new(),
            ring: Arc::new(Ring::new()),
        }
    }

    pub fn push_join_ring_op(
        &mut self,
        id: Transaction,
        tx: join_ring::JoinRingOp,
    ) -> Result<(), OpExecutionError> {
        if !matches!(id.tx_type(), TransactionTypeId::JoinRing) {
            return Err(OpExecutionError::IncorrectTxType(
                TransactionTypeId::JoinRing,
                id.tx_type(),
            ));
        }
        self.ops.join_ring.insert(id, tx);
        Ok(())
    }

    pub fn pop_join_ring_op(&mut self, id: &Transaction) -> Option<join_ring::JoinRingOp> {
        self.ops.join_ring.remove(id)
    }

    pub fn push_put_op(&mut self, id: Transaction, tx: put::PutOp) -> Result<(), OpExecutionError> {
        if !matches!(id.tx_type(), TransactionTypeId::Put) {
            return Err(OpExecutionError::IncorrectTxType(
                TransactionTypeId::Put,
                id.tx_type(),
            ));
        }
        self.ops.put.insert(id, tx);
        Ok(())
    }

    pub fn pop_put_op(&mut self, id: &Transaction) -> Option<put::PutOp> {
        self.ops.put.remove(id)
    }
}

#[derive(Debug, thiserror::Error, Clone)]
pub(crate) enum OpExecutionError {
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionTypeId, TransactionTypeId),
    #[error("failed while processing transaction {0}")]
    TxUpdateFailure(Transaction),
}
