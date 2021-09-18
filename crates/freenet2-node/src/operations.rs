use serde::{Deserialize, Serialize};

use crate::{
    conn_manager,
    message::{Message, Transaction},
    node::OpExecutionError,
};

use self::join_ring::JoinRingOp;

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
    OpStateManagerError(#[from] OpExecutionError),
    #[error("illegal awaiting state")]
    IllegalStateTransition,
}

macro_rules! op_type_enumeration {
    (decl struct { $($field:ident: $var:tt),+ } ) => {
        #[repr(u8)]
        #[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
        pub(crate) enum OperationType {
            $($var,)+
        }

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
    join_ring: JoinRingOp,
    probe_peers: ProbeOp
});
