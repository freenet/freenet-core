use std::{fs::File, io::Write, sync::Arc};

use freenet::dev_tool::{
    Config, ContractStore, DelegateStore, Executor, OperationMode, SecretsStore, StateStore,
    Storage,
};
use freenet_stdlib::prelude::*;
use tokio::sync::RwLock;

use crate::{local_node::DeserializationFmt, DynError};

use super::LocalNodeCliConfig;

#[derive(Clone)]
pub(super) struct AppState {
    pub(crate) local_node: Arc<RwLock<Executor>>,
    config: LocalNodeCliConfig,
}

impl AppState {
    const MAX_MEM_CACHE: u32 = 10_000_000;

    pub async fn new(config: &LocalNodeCliConfig) -> Result<Self, DynError> {
        let contract_dir = Config::conf().contracts_dir();
        let contract_store = ContractStore::new(contract_dir, config.max_contract_size)?;
        let state_store = StateStore::new(Storage::new().await?, Self::MAX_MEM_CACHE).unwrap();
        Ok(AppState {
            local_node: Arc::new(RwLock::new(
                Executor::new(
                    contract_store,
                    DelegateStore::default(),
                    SecretsStore::default(),
                    state_store,
                    || {
                        freenet::util::set_cleanup_on_exit().unwrap();
                    },
                    OperationMode::Local,
                )
                .await?,
            )),
            config: config.clone(),
        })
    }

    pub fn printout_deser<R: AsRef<[u8]> + ?Sized>(&self, data: &R) -> Result<(), std::io::Error> {
        fn write_res(config: &LocalNodeCliConfig, pprinted: &str) -> Result<(), std::io::Error> {
            if let Some(p) = &config.output_file {
                let mut f = File::create(p)?;
                f.write_all(pprinted.as_bytes())?;
            } else if config.terminal_output {
                tracing::debug!("{pprinted}");
            }
            Ok(())
        }
        match self.config.ser_format {
            Some(DeserializationFmt::Json) => {
                let deser: serde_json::Value = serde_json::from_slice(data.as_ref())?;
                let pp = serde_json::to_string_pretty(&deser)?;
                write_res(&self.config, &pp)?;
            }
            #[cfg(feature = "messagepack")]
            Some(DeserializationFmt::MessagePack) => {
                let deser = rmpv::decode::read_value(&mut data.as_ref())
                    .map_err(|_err| std::io::ErrorKind::InvalidData)?;
                let pp = format!("{deser}");
                write_res(&self.config, &pp)?;
            }
            _ => {}
        }
        Ok(())
    }
}
