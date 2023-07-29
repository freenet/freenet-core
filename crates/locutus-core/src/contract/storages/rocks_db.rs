use rocksdb::{Options, DB};
use std::{collections::HashMap, pin::Pin};

use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use locutus_runtime::{
    ContractContainer, ContractError, ContractExecError, ContractRuntimeInterface, ContractStore,
    Parameters, StateStorage, StateStore, StateStoreError, ValidateResult,
};
use locutus_stdlib::client_api::{ClientRequest, ContractRequest, ContractResponse, HostResponse};

use crate::contract::ContractKey;
use crate::{config::CONFIG, contract::test::MockRuntime, WrappedState};

use super::super::handler::{CHListenerHalve, MAX_MEM_CACHE};
use super::super::{ContractHandler, ContractHandlerChannel};

pub struct RocksDb(DB);

impl RocksDb {
    pub async fn new() -> Result<Self, rocksdb::Error> {
        let path = CONFIG.config_paths.db_dir.join("locutus.db");
        tracing::info!("loading contract store from {path:?}");

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_log_level(rocksdb::LogLevel::Debug);

        let db = DB::open(&opts, path).unwrap();

        Ok(Self(db))
    }
}

impl RocksDb {
    const STATE_SUFFIX: &[u8] = "_key".as_bytes();
    const PARAMS_SUFFIX: &[u8] = "_params".as_bytes();
}

#[async_trait::async_trait]
impl StateStorage for RocksDb {
    type Error = rocksdb::Error;

    async fn store(
        &mut self,
        key: ContractKey,
        state: locutus_runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        self.0
            .put([key.bytes(), RocksDb::STATE_SUFFIX].concat(), state)?;

        Ok(())
    }

    async fn get(
        &self,
        key: &ContractKey,
    ) -> Result<Option<locutus_runtime::WrappedState>, Self::Error> {
        match self.0.get([key.bytes(), RocksDb::STATE_SUFFIX].concat()) {
            Ok(result) => {
                if let Some(r) = result.map(|r| Some(WrappedState::new(r))) {
                    Ok(r)
                } else {
                    tracing::debug!(
                        "failed getting contract: `{key}` {}",
                        key.encoded_code_hash()
                            .map(|ch| format!("(with code hash: `{ch}`)"))
                            .unwrap_or(String::new())
                    );
                    Ok(None)
                }
            }
            Err(e) => {
                if rocksdb::ErrorKind::NotFound == e.kind() {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn store_params(
        &mut self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        self.0
            .put([key.bytes(), RocksDb::PARAMS_SUFFIX].concat(), params)?;

        Ok(())
    }

    fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Parameters<'static>>, Self::Error>> + Send + 'a>>
    {
        Box::pin(async move {
            match self.0.get([key.bytes(), RocksDb::PARAMS_SUFFIX].concat()) {
                Ok(result) => Ok(result
                    .map(|r| Some(Parameters::from(r)))
                    .expect("vec bytes")),
                Err(e) => {
                    if rocksdb::ErrorKind::NotFound == e.kind() {
                        Ok(None)
                    } else {
                        Err(e)
                    }
                }
            }
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RocksDbError {
    #[error("Contract not found")]
    ContractNotFound,
    #[error(transparent)]
    RocksError(#[from] rocksdb::Error),
    #[error(transparent)]
    RuntimeError(#[from] ContractError),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    StateStore(#[from] StateStoreError),
}

pub struct RocksDbContractHandler<R> {
    channel: ContractHandlerChannel<RocksDbError, CHListenerHalve>,
    store: ContractStore,
    runtime: R,
    state_store: StateStore<RocksDb>,
    params: HashMap<ContractKey, Parameters<'static>>,
}

impl<R> RocksDbContractHandler<R>
where
    R: ContractRuntimeInterface + 'static,
{
    /// number of max bytes allowed to be stored in the cache
    const MEM_SIZE: u32 = 10_000_000;

    async fn new(
        channel: ContractHandlerChannel<RocksDbError, CHListenerHalve>,
        store: ContractStore,
        runtime: R,
    ) -> Result<Self, RocksDbError> {
        Ok(RocksDbContractHandler {
            channel,
            store,
            runtime,
            state_store: StateStore::new(RocksDb::new().await?, Self::MEM_SIZE)?,
            params: HashMap::default(),
        })
    }

    async fn get_contract(
        &self,
        key: &ContractKey,
        fetch_contract: bool,
    ) -> Result<(WrappedState, Option<ContractContainer>), RocksDbError> {
        let state = self.state_store.get(key).await?;
        let contract = fetch_contract
            .then(|| {
                let params = self.params.get(key).ok_or(RocksDbError::ContractNotFound)?;
                self.store
                    .fetch_contract(key, params)
                    .ok_or(RocksDbError::ContractNotFound)
            })
            .transpose()?;
        Ok((state, contract))
    }
}

impl From<ContractHandlerChannel<RocksDbError, CHListenerHalve>>
    for RocksDbContractHandler<MockRuntime>
{
    fn from(
        channel: ContractHandlerChannel<<Self as ContractHandler>::Error, CHListenerHalve>,
    ) -> Self {
        let store =
            ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE).unwrap();
        let runtime = MockRuntime {};
        tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(RocksDbContractHandler::new(channel, store, runtime))
        })
        .expect("handler initialization")
    }
}

impl<R> ContractHandler for RocksDbContractHandler<R>
where
    R: ContractRuntimeInterface + Send + Sync + 'static,
    Self: From<ContractHandlerChannel<RocksDbError, CHListenerHalve>>,
{
    type Error = RocksDbError;
    type Store = RocksDb;

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
                            Err(RocksDbError::ContractNotFound) => {}
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

    use crate::{contract::contract_handler_channel, WrappedContract};
    use locutus_runtime::{ContractWasmAPIVersion, StateDelta};
    use locutus_stdlib::prelude::ContractCode;

    use super::*;

    // Prepare and get handler for rocksdb
    async fn get_handler() -> Result<RocksDbContractHandler<MockRuntime>, RocksDbError> {
        let (_, ch_handler) = contract_handler_channel();
        let store: ContractStore =
            ContractStore::new(CONFIG.config_paths.contracts_dir.clone(), MAX_MEM_CACHE).unwrap();
        RocksDbContractHandler::new(ch_handler, store, MockRuntime {}).await
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn contract_handler() -> Result<(), anyhow::Error> {
        // Create a rocksdb handler and initialize the database
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
