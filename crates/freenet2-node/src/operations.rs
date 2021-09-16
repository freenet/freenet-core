use serde::{Deserialize, Serialize};

use crate::{
    conn_manager,
    message::{Message, TransactionTypeId},
    node::OpStateError,
};
use join_ring::JoinRingOp;
pub(crate) use sealed_op_types::OpsMap;

pub(crate) mod join_ring;

pub(crate) struct OperationResult<S> {
    /// Inhabited if there is a message to return to the other peer.
    pub return_msg: Option<Message>,
    /// None if the operation has been completed.
    pub state: Option<S>,
}

#[derive(Debug, Default)]
pub struct ProbeOp;

#[derive(Debug, thiserror::Error)]
pub(crate) enum OpError {
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
    #[error(transparent)]
    OpStateManagerError(#[from] OpStateError),
}

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
        join_ring: JoinRingOp -> JoinRing,
        probe_peers: ProbeOp -> Probe
    });
}
