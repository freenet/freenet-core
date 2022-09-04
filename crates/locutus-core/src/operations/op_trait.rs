//! Change from state_machine to a simple "Operation" trait

use std::pin::Pin;

use futures::Future;

use crate::{
    message::{InnerMessage, Transaction},
    node::OpManager,
    operations::{OpError, OpInitialization, OperationResult},
};

pub(crate) trait Operation<CErr, CB>
where
    Self: Sized,
    CErr: std::error::Error,
{
    type Message: InnerMessage;

    type Error: Into<OpError<CErr>>;

    fn load_or_init(
        op_storage: &OpManager<CErr>,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError<CErr>>;

    //     fn new(transaction: Transaction, builder: Self::Builder) -> Self;

    fn id(&self) -> &Transaction;

    #[allow(clippy::type_complexity)]
    fn process_message<'a>(
        self,
        conn_manager: &'a mut CB,
        op_storage: &'a OpManager<CErr>,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, Self::Error>> + Send + 'a>>;
}
