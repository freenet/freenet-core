use locutus_runtime::{Parameters, StateStorage};
use rocksdb::{Options, DB};

use crate::{config::Config, contract::ContractKey, WrappedState};

pub struct RocksDb(DB);

impl RocksDb {
    #[cfg_attr(feature = "sqlite", allow(unused))]
    pub async fn new() -> Result<Self, rocksdb::Error> {
        let path = Config::get_static_conf()
            .config_paths
            .db_dir
            .join("locutus.db");
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

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
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
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{
        contract::{
            contract_handler_channel, ContractHandler, MockRuntime, NetworkContractHandler,
        },
        ClientId, DynError, WrappedContract,
    };
    use locutus_runtime::{ContractContainer, ContractWasmAPIVersion, StateDelta};
    use locutus_stdlib::{client_api::ContractRequest, prelude::ContractCode};

    use super::*;

    // Prepare and get handler for rocksdb
    async fn get_handler() -> Result<NetworkContractHandler<MockRuntime>, DynError> {
        let (_, ch_handler) = contract_handler_channel();
        let handler = NetworkContractHandler::build(ch_handler, ()).await?;
        Ok(handler)
    }

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn contract_handler() -> Result<(), DynError> {
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
                ClientId::FIRST,
                None,
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
                ClientId::FIRST,
                None,
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
                ClientId::FIRST,
                None,
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
