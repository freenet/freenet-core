use freenet_stdlib::prelude::*;
use rocksdb::{Options, DB};

use crate::runtime::StateStorage;
use crate::{config::Config, contract::ContractKey};

pub struct RocksDb(DB);

impl RocksDb {
    #[cfg_attr(feature = "sqlite", allow(unused))]
    pub async fn new() -> Result<Self, rocksdb::Error> {
        let path = Config::conf().db_dir().join("freenet.db");
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

    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        self.0
            .put([key.as_bytes(), RocksDb::STATE_SUFFIX].concat(), state)?;

        Ok(())
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        match self.0.get([key.as_bytes(), RocksDb::STATE_SUFFIX].concat()) {
            Ok(result) => {
                if let Some(r) = result.map(|r| Some(WrappedState::new(r))) {
                    Ok(r)
                } else {
                    tracing::debug!(
                        "failed getting contract: `{key}` {}",
                        key.encoded_code_hash()
                            .map(|ch| format!("(with code hash: `{ch}`)"))
                            .unwrap_or_default()
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
            .put([key.as_bytes(), RocksDb::PARAMS_SUFFIX].concat(), params)?;

        Ok(())
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        match self
            .0
            .get([key.as_bytes(), RocksDb::PARAMS_SUFFIX].concat())
        {
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
