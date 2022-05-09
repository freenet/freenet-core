use std::{fs::File, io::Write, sync::Arc};

use locutus_runtime::WrappedState;
use locutus_stdlib::interface::State;
use parking_lot::RwLock;

use crate::{Cli, DeserializationFmt, DynError};

#[derive(Clone)]
pub(crate) struct AppState {
    state: Arc<RwLock<Option<WrappedState>>>,
    config: Cli,
}

impl AppState {
    pub fn new(config: &Cli) -> Result<Self, DynError> {
        Ok(AppState {
            state: Arc::new(RwLock::new(None)),
            config: config.clone(),
        })
    }

    pub fn put(&mut self, state: WrappedState) {
        let s = &mut *self.state.write();
        s.replace(state);
    }

    pub fn load_state(&self) -> Result<State, DynError> {
        let r = &*self.state.read();
        r.clone()
            .ok_or_else(|| "Initial state not uploaded".into())
            .map(|s| State::from((&*s).to_vec()))
    }

    pub fn printout_deser<R: AsRef<[u8]> + ?Sized>(&self, data: &R) -> Result<(), std::io::Error> {
        fn write_res(config: &Cli, pprinted: &str) -> Result<(), std::io::Error> {
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
