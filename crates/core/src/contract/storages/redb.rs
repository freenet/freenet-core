use std::path::Path;

use freenet_stdlib::prelude::*;
use redb::{Database, TableDefinition};

use crate::wasm_runtime::StateStorage;

const CONTRACT_PARAMS_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_params");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

pub struct ReDb(Database);

impl ReDb {
    pub async fn new(data_dir: &Path) -> Result<Self, redb::Error> {
        let db_path = data_dir.join("db");
        tracing::info!("loading contract store from {db_path:?}");
        match Database::create(db_path).map(Self) {
            Ok(db) => {
                let txn = db.0.begin_write()?;
                {
                    txn.open_table(STATE_TABLE).map_err(|e| {
                        tracing::error!(error = %e, "failed to open STATE_TABLE");
                        e
                    })?;

                    txn.open_table(CONTRACT_PARAMS_TABLE).map_err(|e| {
                        tracing::error!(error = %e, "failed to open CONTRACT_PARAMS_TABLE");
                        e
                    })?;
                }
                txn.commit()?;

                Ok(db)
            },
            Err(e) => {
                tracing::info!("failed to load contract store: {e}");
                Err(e.into())
            }
        }
    }
}

impl StateStorage for ReDb {
    type Error = redb::Error;

    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(key.as_bytes(), state.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(STATE_TABLE)?;
            tbl.get(key.as_bytes())?
        };

        match val {
            Some(v) => Ok(Some(WrappedState::new(v.value().to_vec()))),
            None => Ok(None),
        }
    }

    async fn store_params(
        &mut self,
        key: ContractKey,
        params: Parameters<'static>,
    ) -> Result<(), Self::Error> {
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.insert(key.as_bytes(), params.as_ref())?;
        }
        txn.commit().map_err(Into::into)
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(CONTRACT_PARAMS_TABLE)?;
            tbl.get(key.as_bytes())?
        };

        match val {
            Some(v) => Ok(Some(Parameters::from(v.value().to_vec()))),
            None => Ok(None),
        }
    }
}
