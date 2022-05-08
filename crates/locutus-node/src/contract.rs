use locutus_runtime::ContractRuntimeError;

mod handler;
mod test;

#[cfg(test)]
pub(crate) use handler::test::{TestContractHandler, TestContractStoreError};
pub(crate) use handler::{
    contract_handler_channel, CHSenderHalve, ContractHandler, ContractHandlerChannel,
    ContractHandlerEvent, SQLiteContractHandler, SqlDbError, StoreResponse,
};
use locutus_stdlib::prelude::{ContractKey, Parameters};
pub(crate) use test::MockRuntime;
#[cfg(test)]
pub(crate) use test::{MemoryContractHandler, SimStoreError};

pub(crate) async fn contract_handling<CH, Err>(
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
                id,
                ContractHandlerEvent::FetchQuery {
                    key,
                    fetch_contract,
                },
            ) => {
                let contract = if fetch_contract {
                    let params = Parameters::from(vec![]); // FIXME
                    contract_handler
                        .contract_store()
                        .fetch_contract(&key, &params)
                } else {
                    None
                };

                let response = contract_handler
                    .get_value(&key)
                    .await
                    .map(|value| StoreResponse { value, contract });

                contract_handler
                    .channel()
                    .send_to_listener(id, ContractHandlerEvent::FetchResponse { key, response })
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
            (id, ContractHandlerEvent::PushQuery { key, value }) => {
                let put_result = contract_handler.put_value(&key, value).await;
                contract_handler
                    .channel()
                    .send_to_listener(
                        id,
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
