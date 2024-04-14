use std::path::Path;

use freenet_stdlib::prelude::*;
use redb::{Database, TableDefinition};

use crate::wasm_runtime::StateStorage;

const CONTRACT_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("contract");
const STATE_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("state");

pub struct ReDb(Database);

impl ReDb {
    pub async fn new(db_path: &Path) -> Result<Self, redb::Error> {
        tracing::info!("loading contract store from {db_path:?}");

        Database::create(db_path).map(Self).map_err(Into::into)
    }
}

impl ReDb {
    const STATE_SUFFIX: &'static [u8] = "_key".as_bytes();
    const PARAMS_SUFFIX: &'static [u8] = "_params".as_bytes();
}

impl StateStorage for ReDb {
    type Error = redb::Error;

    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error> {
        let txn = self.0.begin_write()?;

        {
            let mut tbl = txn.open_table(STATE_TABLE)?;
            tbl.insert(
                [key.as_bytes(), Self::STATE_SUFFIX].concat().as_slice(),
                state.as_ref(),
            )?;
        }
        txn.commit().map_err(Into::into)
    }

    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(STATE_TABLE)?;
            tbl.get([key.as_bytes(), Self::STATE_SUFFIX].concat().as_slice())?
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
            let mut tbl = txn.open_table(CONTRACT_TABLE)?;
            tbl.insert(
                [key.as_bytes(), Self::PARAMS_SUFFIX].concat().as_slice(),
                params.as_ref(),
            )?;
        }
        txn.commit().map_err(Into::into)
    }

    async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, Self::Error> {
        let txn = self.0.begin_read()?;

        let val = {
            let tbl = txn.open_table(CONTRACT_TABLE)?;
            tbl.get([key.as_bytes(), Self::PARAMS_SUFFIX].concat().as_slice())?
        };

        match val {
            Some(v) => Ok(Some(Parameters::from(v.value().to_vec()))),
            None => Ok(None),
        }
    }
}
