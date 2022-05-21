use std::{fs::File, io::Write, sync::Arc};

use locutus_node::SqlitePool;
use locutus_runtime::{ContractStore, StateStore};
use tokio::sync::RwLock;

use crate::{
    config::{Config, DeserializationFmt},
    DynError, LocalNode,
};

#[derive(Clone)]
pub struct AppState {
    pub(crate) local_node: Arc<RwLock<LocalNode>>,
    config: Config,
}

impl AppState {
    const MAX_MEM_CACHE: u32 = 10_000_000;

    pub async fn new(config: &Config) -> Result<Self, DynError> {
        let tmp_path = std::env::temp_dir().join("locutus").join("contracts");
        std::fs::create_dir_all(&tmp_path)?;
        let contract_store =
            ContractStore::new(tmp_path.join("contracts"), config.max_contract_size);
        let state_store = StateStore::new(SqlitePool::new().await?, Self::MAX_MEM_CACHE).unwrap();
        Ok(AppState {
            local_node: Arc::new(RwLock::new(
                LocalNode::new(contract_store, state_store).await?,
            )),
            config: config.clone(),
        })
    }

    pub fn printout_deser<R: AsRef<[u8]> + ?Sized>(&self, data: &R) -> Result<(), std::io::Error> {
        fn write_res(config: &Config, pprinted: &str) -> Result<(), std::io::Error> {
            if let Some(p) = &config.output_file {
                let mut f = File::create(p)?;
                f.write_all(pprinted.as_bytes())?;
            } else if config.terminal_output {
                println!("{pprinted}");
            }
            Ok(())
        }
        match self.config.deser_format {
            #[cfg(feature = "json")]
            Some(DeserializationFmt::Json) => {
                let deser: serde_json::Value = serde_json::from_slice(data.as_ref())?;
                let pp = serde_json::to_string_pretty(&deser)?;
                write_res(&self.config, &*pp)?;
            }
            #[cfg(feature = "messagepack")]
            Some(DeserializationFmt::MessagePack) => {
                let deser = rmpv::decode::read_value(&mut data.as_ref())
                    .map_err(|_err| std::io::ErrorKind::InvalidData)?;
                let pp = format!("{deser}");
                write_res(&self.config, &*pp)?;
            }
            _ => {}
        }
        Ok(())
    }
}
