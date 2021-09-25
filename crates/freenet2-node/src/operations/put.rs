use std::marker::PhantomData;

use crate::message::Transaction;

pub(crate) use self::messages::PutMsg;

/// This is just a placeholder for now!
pub(crate) struct PutOp(PhantomData<()>);

impl PutOp {
    pub fn new() -> Self {
        PutOp(PhantomData)
    }
}

mod messages {
    use crate::conn_manager::PeerKeyLocation;

    use super::*;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
    pub(crate) enum PutMsg {}

    impl PutMsg {
        pub fn id(&self) -> &Transaction {
            todo!()
        }

        pub fn sender(&self) -> Option<&PeerKeyLocation> {
            todo!()
        }
    }
}
