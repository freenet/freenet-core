use crate::{
    message::{Transaction, TransactionTypeId},
    operations::{join_ring, OpsMap},
};

pub(crate) struct OpStateStorage {
    ops: OpsMap,
}

impl OpStateStorage {
    pub fn new() -> Self {
        Self { ops: OpsMap::new() }
    }

    pub fn push_join_ring_op(
        &mut self,
        id: Transaction,
        tx: join_ring::JoinRingOpState,
    ) -> Result<(), OpStateError> {
        if !matches!(id.tx_type(), TransactionTypeId::JoinRing) {
            return Err(OpStateError::IncorrectTxType(
                TransactionTypeId::JoinRing,
                id.tx_type(),
            ));
        }
        self.ops.join_ring.insert(id, tx);
        Ok(())
    }

    pub fn pop_join_ring_op(&mut self, id: &Transaction) -> Option<join_ring::JoinRingOpState> {
        self.ops.join_ring.remove(id)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpStateError {
    #[error("unspected transaction type, trying to get a {0:?} from a {1:?}")]
    IncorrectTxType(TransactionTypeId, TransactionTypeId),
}

#[cfg(test)]
mod tests {}
