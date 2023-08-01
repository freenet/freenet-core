use std::{collections::HashMap, pin::Pin, str::FromStr};

use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use locutus_runtime::{
    ContractContainer, ContractError, ContractExecError, ContractRuntimeInterface, ContractStore,
    Parameters, StateStorage, StateStore, StateStoreError, ValidateResult,
};
use locutus_stdlib::client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse};
use once_cell::sync::Lazy;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteRow},
    ConnectOptions, Row, SqlitePool,
};

use crate::contract::ContractKey;
use crate::{config::CONFIG, contract::test::MockRuntime, WrappedState};

use super::super::handler::{CHListenerHalve, MAX_MEM_CACHE};
use super::super::{ContractHandler, ContractHandlerChannel};

// Is fine to clone this as it wraps by an Arc.
static POOL: Lazy<SqlitePool> = Lazy::new(|| {
    let mut opts = if cfg!(test) {
        SqliteConnectOptions::from_str("sqlite::memory:").unwrap()
    } else {
        let conn_str = CONFIG.config_paths.db_dir.join("locutus.db");
        tracing::info!("loading contract store from {conn_str:?}");
        SqliteConnectOptions::new()
            .create_if_missing(true)
            .filename(conn_str)
    };
    opts.log_statements(tracing::log::LevelFilter::Debug);
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(async move { SqlitePool::connect_with(opts).await })
    })
    .unwrap()
});

async fn create_contracts_table() -> Result<(), SqlDbError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS states (
                    contract        BLOB PRIMARY KEY,
                    state           BLOB,
                    params          BLOB
                )",
    )
    .execute(&*POOL)
    .await?;
    Ok(())
}

#[derive(Clone)]
pub struct Pool(SqlitePool);

impl Pool {
    pub async fn new() -> Result<Self, SqlDbError> {
        create_contracts_table().await?;
        Ok(Self(POOL.clone()))
    }
}

#[async_trait::async_trait]
impl StateStorage for Pool {
    type Error = SqlDbError;

    async fn store(
        &mut self,
        key: ContractKey,
        state: locutus_runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT INTO states (contract, state) 
                     VALUES ($1, $2) 
                     ON CONFLICT(contract) DO UPDATE SET state = excluded.state
                     ",
        )
        .bind(key.bytes())
        .bind(state.as_ref())
        .execute(&self.0)
        .await?;
        Ok(())
    }

    async fn get(
        &self,
        key: &ContractKey,
    ) -> Result<Option<locutus_runtime::WrappedState>, Self::Error> {
        match sqlx::query("SELECT state FROM states WHERE contract = ?")
            .bind(key.bytes())
            .map(|row: SqliteRow| Some(WrappedState::new(row.get("state"))))
            .fetch_one(&self.0)
            .await
        {
            Ok(result) => Ok(result),
            Err(sqlx::Error::RowNotFound) => Ok(None),
            Err(_) => Err(SqlDbError::ContractNotFound),
        }
    }

    async fn store_params(
        &mut self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            "INSERT OR REPLACE INTO states (contract, params) 
                     VALUES ($1, $2)
                     ON CONFLICT(contract) DO UPDATE SET params = excluded.params
                     ",
        )
        .bind(key.bytes())
        .bind(params.as_ref())
        .execute(&self.0)
        .await?;
        Ok(())
    }

    fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Parameters<'static>>, Self::Error>> + Send + 'a>>
    {
        Box::pin(async move {
            match sqlx::query("SELECT params FROM states WHERE contract = ?")
                .bind(key.bytes())
                .map(|row: SqliteRow| Some(Parameters::from(row.get::<Vec<u8>, _>("params"))))
                .fetch_one(&self.0)
                .await
            {
                Ok(result) => Ok(result),
                Err(sqlx::Error::RowNotFound) => Ok(None),
                Err(_) => Err(SqlDbError::ContractNotFound),
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqlDbError {
    #[error("Contract not found")]
    ContractNotFound,
    #[error(transparent)]
    SqliteError(#[from] sqlx::Error),
    #[error(transparent)]
    RuntimeError(#[from] ContractError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    StateStore(#[from] StateStoreError),
}

pub struct SQLiteContractHandler<R> {
    channel: ContractHandlerChannel<SqlDbError, CHListenerHalve>,
    store: ContractStore,
    runtime: R,
    state_store: StateStore<Pool>,
    params: HashMap<ContractKey, Parameters<'static>>,
}

impl<R> SQLiteContractHandler<R>
where
    R: ContractRuntimeInterface + 'static,
{
    /// number of max bytes allowed to be stored in the cache
    const MEM_SIZE: u32 = 10_000_000;

    async fn new(
        channel: ContractHandlerChannel<SqlDbError, CHListenerHalve>,
        store: ContractStore,
        runtime: R,
    ) -> Result<Self, SqlDbError> {
        Ok(SQLiteContractHandler {
            channel,
            store,
            runtime,
            state_store: StateStore::new(Pool::new().await?, Self::MEM_SIZE)?,
            params: HashMap::default(),
        })
    }

    async fn get_contract(
        &self,
        key: &ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), SqlDbError> {
        let state = self.state_store.get(key).await?;
        let contract = fetch_contract
            .then(|| {
                let params = self.params.get(key).ok_or(SqlDbError::ContractNotFound)?;
                self.store
                    .fetch_contract(key, params)
                    .ok_or(SqlDbError::ContractNotFound)
            })
            .transpose()?;
        Ok((state, contract))
    }
}

impl From<ContractHandlerChannel<SqlDbError, CHListenerHalve>>
    for SQLiteContractHandler<MockRuntime>
{
    fn from(
        channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
    ) -> Self {
        let store =
            ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE).unwrap();
        let runtime = MockRuntime {};
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(SQLiteContractHandler::new(channel, store, runtime))
        })
        .expect("handler initialization")
    }
}

impl<R> ContractHandler for SQLiteContractHandler<R>
where
    R: ContractRuntimeInterface + Send + Sync + 'static,
    Self: From<ContractHandlerChannel<SqlDbError, CHListenerHalve>>,
{
    type Error = SqlDbError;
    type Store = Pool;

    #[inline(always)]
    fn channel(&mut self) -> &mut ContractHandlerChannel<Self::Error, CHListenerHalve> {
        &mut self.channel
    }

    #[inline(always)]
    fn contract_store(&mut self) -> &mut ContractStore {
        &mut self.store
    }

    fn handle_request<'a, 's: 'a>(
        &'s mut self,
        req: ClientRequest<'a>,
    ) -> BoxFuture<'a, Result<HostResponse, Self::Error>> {
        async move {
            match req {
                ClientRequest::ContractOp(ops) => match ops {
                    ContractRequest::Get {
                        key,
                        fetch_contract,
                    } => {
                        let (state, contract) = self.get_contract(&key, fetch_contract).await?;
                        Ok(ContractResponse::GetResponse {
                            key,
                            contract,
                            state,
                        }
                        .into())
                    }
                    ContractRequest::Put {
                        contract,
                        state,
                        related_contracts,
                    } => {
                        let key = contract.key();
                        let params = contract.params();
                        match self.get_contract(&key, false).await {
                            Ok((_old_state, _)) => {
                                return Err(
                                    ContractError::from(ContractExecError::DoublePut(key)).into()
                                )
                            }
                            Err(SqlDbError::ContractNotFound) => {}
                            Err(other) => return Err(other),
                        }

                        let result = self.runtime.validate_state(
                            &key,
                            &params,
                            &state,
                            related_contracts,
                        )?;
                        // FIXME: should deal with additional related contracts requested
                        if result != ValidateResult::Valid {
                            todo!("return error");
                        }
                        let _params: Parameters<'static> = params.clone().into_bytes().into();
                        self.state_store.store(key, state, None).await?;
                        todo!()
                    }
                    _ => unreachable!(),
                },
                ClientRequest::DelegateOp(_op) => unreachable!(),
                ClientRequest::Disconnect { .. } => unreachable!(),
                ClientRequest::GenerateRandData { bytes: _ } => unreachable!(),
            }
        }
        .boxed()
    }

    fn state_store(&mut self) -> &mut StateStore<Self::Store> {
        &mut self.state_store
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::contract::contract_handler_channel;
    use crate::WrappedContract;
    use locutus_runtime::{ContractWasmAPIVersion, StateDelta};
    use locutus_stdlib::prelude::ContractCode;

    use super::SQLiteContractHandler;
    use super::*;

    // Prepare and get handler for an in-memory sqlite db
    async fn get_handler() -> Result<SQLiteContractHandler<MockRuntime>, SqlDbError> {
        let (_, ch_handler) = contract_handler_channel();
        let store: ContractStore =
            ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE).unwrap();
        SQLiteContractHandler::new(ch_handler, store, MockRuntime {}).await
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn contract_handler() -> Result<(), anyhow::Error> {
        // Create a sqlite handler and initialize the database
        let mut handler = get_handler().await?;

        // Generate a contract
        let contract_bytes = b"Test contract value".to_vec();
        let contract: ContractContainer =
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
                Arc::new(ContractCode::from(contract_bytes.clone())),
                Parameters::from(vec![]),
            )));

        // Get contract parts
        let state = WrappedState::new(contract_bytes.clone());
        handler
            .handle_request(
                ContractRequest::Put {
                    contract: contract.clone(),
                    state: state.clone(),
                    related_contracts: Default::default(),
                }
                .into(),
            )
            .await?
            .unwrap_put();
        let (get_result_value, _) = handler
            .handle_request(
                ContractRequest::Get {
                    key: contract.key().clone(),
                    fetch_contract: false,
                }
                .into(),
            )
            .await?
            .unwrap_get();
        assert_eq!(state, get_result_value);

        // Update the contract state with a new delta
        let delta = StateDelta::from(b"New test contract value".to_vec());
        handler
            .handle_request(
                ContractRequest::Update {
                    key: contract.key().clone(),
                    data: delta.into(),
                }
                .into(),
            )
            .await?;
        // let (new_get_result_value, _) = handler
        //     .handle_request(ContractOps::Get {
        //         key: *contract.key(),
        //         contract: false,
        //     })
        //     .await?
        //     .unwrap_summary();
        // assert_eq!(delta, new_get_result_value);
        todo!("get summary and compare with delta");
    }
}
