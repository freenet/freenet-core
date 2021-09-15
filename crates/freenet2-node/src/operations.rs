//! Keeps track of the join operation state in this machine.
use rust_fsm::*;
use serde::{Deserialize, Serialize};

use crate::message::TransactionTypeId;

pub(crate) use sealed_op_types::{OperationType, OpsMap};

state_machine! {
    derive(Debug)
    pub(crate) JoinRingOp(Connecting)

    Connecting =>  {
        Connecting => OCReceived [OCReceived],
        OCReceived => Connected [Connected],
        Connected => Connected [Connected],
    },
    OCReceived(Connected) => Connected [Connected],
}

#[derive(Debug, Default)]
pub struct ProbeOp;

/// Get the transaction type associated to a given operation type.
pub(crate) trait AssociatedTxType: sealed_op_types::SealedAssocTxType {
    fn tx_type_id() -> TransactionTypeId;
}

impl<T> AssociatedTxType for T
where
    T: sealed_op_types::SealedAssocTxType,
{
    fn tx_type_id() -> TransactionTypeId {
        <Self as sealed_op_types::SealedAssocTxType>::tx_type_id()
    }
}

mod sealed_op_types {
    use super::*;
    use crate::message::Transaction;

    pub(crate) trait SealedAssocTxType {
        fn tx_type_id() -> TransactionTypeId;
    }

    macro_rules! op_type_enumeration {
        (decl struct { $($field:ident: $var:tt -> $tx_ty:tt),+ } ) => {
            #[repr(u8)]
            #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
            pub(crate) enum OperationType {
                $($var,)+
            }

            $(
                impl SealedAssocTxType for $var {
                    fn tx_type_id() -> TransactionTypeId {
                        TransactionTypeId::$tx_ty
                    }
                }
            )+

            #[derive(Debug)]
            pub(crate) struct OpsMap {
                $( pub $field: std::collections::HashMap<Transaction, $var>),+,
            }

            impl OpsMap {
                pub fn new() -> Self {
                    Self {
                        $( $field: std::collections::HashMap::new()),+,
                    }
                }
            }
        };
    }

    op_type_enumeration!(decl struct {
        join_ring: JoinRingOp -> OpenConnection,
        probe_peers: ProbeOp -> Probe
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_fsm::StateMachine;

    #[test]
    fn join_ring_transitions() {
        let mut join_op_host_1 = StateMachine::<JoinRingOp>::new();
        let res = join_op_host_1
            .consume(&JoinRingOpInput::Connecting)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::OCReceived));

        let mut join_op_host_2 = StateMachine::<JoinRingOp>::new();
        let res = join_op_host_2
            .consume(&JoinRingOpInput::OCReceived)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::Connected));

        let res = join_op_host_1
            .consume(&JoinRingOpInput::Connected)
            .unwrap()
            .unwrap();
        assert!(matches!(res, JoinRingOpOutput::Connected));
    }
}
