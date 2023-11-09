//! Change from state_machine to a simple "Operation" trait

use std::pin::Pin;

use futures::{future::BoxFuture, Future};

use crate::{
    client_events::ClientId,
    message::{InnerMessage, Transaction},
    node::{NetworkBridge, OpManager},
    operations::{OpError, OpInitialization, OperationResult},
};

pub(crate) trait Operation
where
    Self: Sized + TryInto<Self::Result>,
{
    type Message: InnerMessage + std::fmt::Display;

    type Result;

    fn load_or_init<'a>(
        op_storage: &'a OpManager,
        msg: &'a Self::Message,
    ) -> BoxFuture<'a, Result<OpInitialization<Self>, OpError>>;

    fn id(&self) -> &Transaction;

    #[allow(clippy::type_complexity)]
    fn process_message<'a, CB: NetworkBridge>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager,
        input: &'a Self::Message,
        client_id: Option<ClientId>,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, OpError>> + Send + 'a>>;
}
