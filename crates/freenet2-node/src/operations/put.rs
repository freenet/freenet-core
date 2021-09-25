use std::marker::PhantomData;

use crate::{conn_manager::ConnectionBridge, message::Transaction, node::OpStateStorage};

pub(crate) use self::messages::PutMsg;

use super::OpError;

/// This is just a placeholder for now!
pub(crate) struct PutOp(PhantomData<()>);

impl PutOp {
    pub fn new() -> Self {
        PutOp(PhantomData)
    }
}

pub(crate) async fn put_op<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    put_op: PutMsg,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    Ok(())
}

/// Request to insert/update a value into a contract.
pub(crate) async fn request_put<CB>(
    op_storage: &mut OpStateStorage,
    conn_manager: &mut CB,
    put_op: PutOp,
) -> Result<(), OpError>
where
    CB: ConnectionBridge,
{
    todo!()
}

mod messages {
    use crate::conn_manager::PeerKeyLocation;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum PutMsg {
        RouteValue { key: Vec<u8>, value: Vec<u8> },
    }

    impl PutMsg {
        pub fn id(&self) -> &Transaction {
            todo!()
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            todo!()
        }
    }
}
