use std::{fs::File, io::Write, sync::Arc};

use freenet::dev_tool::{
    Config, ContractStore, DelegateStore, Executor, OperationMode, SecretsStore, StateStore,
    Storage,
};
use freenet_stdlib::prelude::*;
use futures::TryFutureExt;
use tokio::sync::RwLock;

use crate::wasm_runtime::DeserializationFmt;

use super::ExecutorConfig;

#[derive(Clone)]
pub(super) struct AppState {
    pub(crate) local_node: Arc<RwLock<Executor>>,
    config: ExecutorConfig,
}

impl AppState {
    const MAX_MEM_CACHE: u32 = 10_000_000;
    const DEFAULT_MAX_DELEGATE_SIZE: i64 = 10 * 1024 * 1024;

    pub async fn new(config: &ExecutorConfig) -> Result<Self, anyhow::Error> {
        let contract_store =
            ContractStore::new(Config::conf().contracts_dir(), config.max_contract_size)?;
        let delegate_store = DelegateStore::new(
            Config::conf().delegates_dir(),
            Self::DEFAULT_MAX_DELEGATE_SIZE,
        )?;
        let secrets_store = SecretsStore::new(Config::conf().secrets_dir())?;
        let state_store = StateStore::new(
            Storage::new(&Config::conf().db_dir()).await?,
            Self::MAX_MEM_CACHE,
        )?;
        let rt = freenet::dev_tool::Runtime::build(
            contract_store,
            delegate_store,
            secrets_store,
            false,
        )?;
        Ok(AppState {
            local_node: Arc::new(RwLock::new(
                Executor::new(
                    state_store,
                    || {
                        freenet::util::set_cleanup_on_exit()?;
                        Ok(())
                    },
                    OperationMode::Local,
                    rt,
                    None,
                )
                .map_err(|err| anyhow::anyhow!(err))
                .await?,
            )),
            config: config.clone(),
        })
    }

    pub fn printout_deser<R: AsRef<[u8]> + ?Sized>(&self, data: &R) -> Result<(), std::io::Error> {
        fn write_res(config: &ExecutorConfig, pprinted: &str) -> Result<(), std::io::Error> {
            if let Some(p) = &config.output_file {
                let mut f = File::create(p)?;
                f.write_all(pprinted.as_bytes())?;
            } else if config.terminal_output {
                tracing::debug!("{pprinted}");
            }
            Ok(())
        }
        if let Some(DeserializationFmt::Json) = self.config.ser_format {
            let deser: serde_json::Value = serde_json::from_slice(data.as_ref())?;
            let pp = serde_json::to_string_pretty(&deser)?;
            write_res(&self.config, &pp)?;
        }
        Ok(())
    }
}
