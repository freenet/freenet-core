use locutus_runtime::{prelude::ContractKey, ContractRuntimeError, Parameters};

mod handler;
mod test;

use crate::WrappedState;
pub use handler::sqlite::{Pool as SqlitePool, SQLiteContractHandler};
#[cfg(test)]
pub(crate) use handler::test::{TestContractHandler, TestContractStoreError};
pub(crate) use handler::{
    contract_handler_channel, CHSenderHalve, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, SqlDbError, StoreResponse,
};
pub(crate) use test::MockRuntime;
#[cfg(test)]
pub(crate) use test::{MemoryContractHandler, SimStoreError};

pub(crate) async fn contract_handling<'a, CH, Err>(
    mut contract_handler: CH,
) -> Result<(), ContractError<Err>>
where
    CH: ContractHandler<Error = Err> + Send + 'static,
    Err: std::error::Error + Send + 'static,
{
    loop {
        let res = contract_handler.channel().recv_from_listener().await?;
        match res {
            (
                _id,
                ContractHandlerEvent::FetchQuery {
                    key,
                    fetch_contract,
                },
            ) => {
                let _contract = if fetch_contract {
                    let params = Parameters::from(vec![]); // FIXME
                    contract_handler
                        .contract_store()
                        .fetch_contract(&key, &params)
                } else {
                    None
                };
                let _response = {
                    let _contract = if fetch_contract {
                        let parameters = Parameters::from(vec![]); // FIXME
                        contract_handler
                            .contract_store()
                            .fetch_contract(&key, &parameters)
                    } else {
                        None
                    };
                    todo!("get state from state store");
                    Ok(StoreResponse {
                        state: None,
                        contract: _contract,
                    })
                };

                contract_handler
                    .channel()
                    .send_to_listener(
                        _id,
                        ContractHandlerEvent::FetchResponse {
                            key,
                            response: _response,
                        },
                    )
                    .await?;
            }
            (id, ContractHandlerEvent::Cache(contract)) => {
                match contract_handler.contract_store().store_contract(contract) {
                    Ok(_) => {
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Ok(())))
                            .await?;
                    }
                    Err(err) => {
                        let err = ContractError::ContractRuntimeError(err);
                        contract_handler
                            .channel()
                            .send_to_listener(id, ContractHandlerEvent::CacheResult(Err(err)))
                            .await?;
                    }
                }
            }
            (
                _id,
                ContractHandlerEvent::PushQuery {
                    key: _key,
                    state: _state,
                },
            ) => {
                // TODO: Perform put request
                let put_result = Ok(WrappedState::from(vec![]));
                // let _put_result = contract_handler
                //     .handle_request(ClientRequest::Put {
                //         contract: todo!(),
                //         state: _state,
                //     })
                //     .await
                //     .map(|r| {
                //         let _r = r.unwrap_put();
                //         unimplemented!();
                //     });
                contract_handler
                    .channel()
                    .send_to_listener(
                        _id,
                        ContractHandlerEvent::PushResponse {
                            new_value: put_result,
                        },
                    )
                    .await?;
            }
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError<CErr> {
    #[error("handler channel dropped")]
    ChannelDropped(Box<ContractHandlerEvent<CErr>>),
    #[error("contract {0} not found in storage")]
    ContractNotFound(ContractKey),
    #[error("")]
    ContractRuntimeError(ContractRuntimeError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error("no response received from handler")]
    NoEvHandlerResponse,
    #[error("failed while storing a contract")]
    StorageError(CErr),
}

impl From<SqlDbError> for ContractError<SqlDbError> {
    fn from(err: SqlDbError) -> Self {
        Self::StorageError(err)
    }
}
