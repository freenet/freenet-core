//! Change from state_machine to a simple "Operation" trait

use std::pin::Pin;

use futures::Future;

use crate::{
    message::{InnerMessage, Transaction},
    node::OpManager,
    operations::{OpError, OpInitialization, OperationResult},
};

pub(crate) trait Operation<CErr>
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
    fn process_message(
        self,
        op_storage: &OpManager<CErr>,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, Self::Error>>>>;
}

// EXAMPLE:

enum GetError {}

impl<CErr: std::error::Error> From<GetError> for OpError<CErr> {
    fn from(_val: GetError) -> Self {
        todo!()
    }
}

pub struct FakeGet {
    id: Transaction,
}

impl<CErr: std::error::Error> Operation<CErr> for FakeGet {
    type Message = super::get::GetMsg;

    type Error = GetError;

    fn load_or_init(
        _op_storage: &OpManager<CErr>,
        msg: &Self::Message,
    ) -> Result<OpInitialization<Self>, OpError<CErr>> {
        // todo: basically move the load/initialization logic from the handle_<op>_request inside here
        Ok(OpInitialization {
            op: Self { id: *msg.id() },
            sender: None,
        })
    }

    fn id(&self) -> &Transaction {
        &self.id
    }

    fn process_message(
        self,
        op_storage: &OpManager<CErr>,
        input: Self::Message,
    ) -> Pin<Box<dyn Future<Output = Result<OperationResult, Self::Error>>>> {
        // todo: add all internal logic here
        Box::pin(async move {
            match input {
                super::get::GetMsg::RequestGet { .. } => {
                    println!("do something");
                }
                _ => println!("do something else"),
            }
            Ok(OperationResult {
                return_msg: None,
                state: None,
            })
        })
    }
}
