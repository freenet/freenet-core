// TODO: complete update logic in the network

use freenet_stdlib::prelude::*;
use futures::future::BoxFuture;

use super::{OpError, OpOutcome, Operation};
use crate::{
    client_events::{ClientId, HostResult},
    node::{NetworkBridge, OpManager},
};

pub(crate) use self::messages::UpdateMsg;

pub(crate) struct UpdateOp {}

impl UpdateOp {
    pub fn outcome(&self) -> OpOutcome {
        OpOutcome::Irrelevant
    }

    pub fn finalized(&self) -> bool {
        todo!()
    }

    pub fn record_transfer(&mut self) {}

    pub(super) fn to_host_result(&self) -> HostResult {
        todo!()
    }
}

pub(crate) struct UpdateResult {}

impl TryFrom<UpdateOp> for UpdateResult {
    type Error = OpError;

    fn try_from(_value: UpdateOp) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl Operation for UpdateOp {
    type Message = UpdateMsg;
    type Result = UpdateResult;

    fn load_or_init<'a>(
        _op_manager: &'a crate::node::OpManager,
        _msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<super::OpInitialization<Self>, OpError>> {
        todo!()
    }

    fn id(&self) -> &crate::message::Transaction {
        todo!()
    }

    fn process_message<'a, NB: NetworkBridge>(
        self,
        _conn_manager: &'a mut NB,
        _op_manager: &'a crate::node::OpManager,
        _input: &Self::Message,
        // _client_id: Option<ClientId>,
    ) -> std::pin::Pin<
        Box<dyn futures::Future<Output = Result<super::OperationResult, OpError>> + Send + 'a>,
    > {
        todo!()
    }
}

// todo: new_state should be a delta when possible!
pub(crate) fn start_op(_key: ContractKey, _new_state: WrappedState, _htl: usize) -> UpdateOp {
    todo!()
}

pub(crate) async fn request_update(
    _op_manager: &OpManager,
    _update_op: UpdateOp,
    _client_id: Option<ClientId>,
) -> Result<(), OpError> {
    todo!()
}

mod messages {
    use std::fmt::Display;

    use serde::{Deserialize, Serialize};

    use crate::{
        message::{InnerMessage, Transaction},
        ring::PeerKeyLocation,
    };

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum UpdateMsg {}

    impl InnerMessage for UpdateMsg {
        fn id(&self) -> &Transaction {
            todo!()
        }

        fn target(&self) -> Option<&PeerKeyLocation> {
            todo!()
        }

        fn terminal(&self) -> bool {
            todo!()
        }

        fn requested_location(&self) -> Option<crate::ring::Location> {
            todo!()
        }
    }

    impl Display for UpdateMsg {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            todo!()
        }
    }
}
