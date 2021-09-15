use crate::{
    message::Transaction,
    operations::{AssociatedTxType, JoinRingOp, OpsMap},
};

pub(super) struct OpStateStorage {
    ops: OpsMap,
}

impl OpStateStorage {
    pub fn new() -> Self {
        Self { ops: OpsMap::new() }
    }

    pub fn push_join_ring_op(&mut self, id: Transaction, tx: JoinRingOp) -> Result<(), ()> {
        let op_type = <JoinRingOp as AssociatedTxType>::tx_type_id();
        if !matches!(id.tx_type(), OP_TYPE) {
            return Err(());
        }
        self.ops.join_ring.insert(id, tx);
        Ok(())
    }

    pub fn pop_join_ring_op(&mut self, id: &Transaction) -> Option<JoinRingOp> {
        self.ops.join_ring.remove(id)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn example() {
        todo!()
    }
}
