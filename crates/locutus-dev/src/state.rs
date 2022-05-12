use std::{fs::File, io::Write, sync::Arc};

use locutus_runtime::{prelude::ContractKey, WrappedState};
use locutus_stdlib::interface::State;
use parking_lot::RwLock;

use crate::{
    config::{Config, DeserializationFmt},
    DynError, LocalNode,
};

#[derive(Clone)]
pub struct AppState {
    local_node: Arc<RwLock<LocalNode>>,
    config: Config,
}

impl AppState {
    pub fn new(config: &Config) -> Result<Self, DynError> {
        Ok(AppState {
            local_node: Arc::new(RwLock::new(LocalNode::new())),
            config: config.clone(),
        })
    }

    pub fn put(&mut self, key: ContractKey, state: WrappedState) {
        let node = &mut *self.local_node.write();
        node.contract_state.insert(key, state);
    }

    pub fn load_state(&self, key: &ContractKey) -> Result<State, DynError> {
        let node = &*self.local_node.read();
        node.contract_state
            .get(key)
            .ok_or_else(|| format!("initial state not uploaded for {key}").into())
            .map(|s| State::from((&*s).to_vec()))
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

        #[cfg(feature = "json")]
        {
            if let Some(DeserializationFmt::Json) = self.config.deser_format {
                let deser: serde_json::Value = serde_json::from_slice(data.as_ref())?;
                let pp = serde_json::to_string_pretty(&deser)?;
                write_res(&self.config, &*pp)?;
            }
        }
        #[cfg(feature = "messagepack")]
        {
            if let Some(DeserializationFmt::MessagePack) = self.config.deser_format {
                let deser = rmpv::decode::read_value(&mut data.as_ref())
                    .map_err(|_err| std::io::ErrorKind::InvalidData)?;
                let pp = format!("{deser}");
                write_res(&self.config, &*pp)?;
            }
        }
        Ok(())
    }
}
