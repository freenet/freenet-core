use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

use dashmap::DashMap;
use locutus_stdlib::prelude::{ContractCode, Parameters};
use stretto::Cache;

use crate::{contract::WrappedContract, ContractRuntimeError, RuntimeResult};

use super::ContractKey;

type ContractKeyCodePart = [u8; 32];

/// Handle contract blob storage on the file system.
#[derive(Clone)]
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<ContractKeyCodePart, Arc<ContractCode<'static>>>,
    // todo: persist this somewhere
    key_to_code_part: DashMap<ContractKey, ContractKeyCodePart>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

impl ContractStore {
    /// # Arguments
    /// - max_size: max size in bytes of the contracts being cached
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> Self {
        if !contracts_dir.exists() {
            std::fs::create_dir_all(&contracts_dir)
                .map_err(|err| tracing::error!("error creating contract dir: {err}"))
                .expect("coudln't create dir");
        }
        const ERR: &str = "failed to build mem cache";
        Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            key_to_code_part: DashMap::new(),
        }
    }

    /// Returns a copy of the contract bytes if available, none otherwise.
    pub fn fetch_contract<'a>(
        &self,
        key: &ContractKey,
        params: &Parameters<'a>,
    ) -> Option<WrappedContract<'a>> {
        let contract_hash = if let Some(s) = key.contract_part() {
            s
        } else {
            tracing::warn!("requested partially unspecified contract `{key}`");
            return None;
        };
        if let Some(data) = self.contract_cache.get(contract_hash) {
            Some(WrappedContract::new(data.value().clone(), params.clone()))
        } else {
            let path = bs58::encode(contract_hash)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string()
                .to_lowercase();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let owned_params = Parameters::from(params.as_ref().to_owned());
            let WrappedContract { data, params, .. } =
                WrappedContract::try_from((&*key_path, owned_params))
                    .map_err(|err| {
                        tracing::debug!("contract not found: {err}");
                        err
                    })
                    .ok()?;

            // add back the contract part to the mem store
            let size = data.data().len() as i64;
            self.contract_cache
                .insert(*contract_hash, data.clone(), size);
            Some(WrappedContract::new(data, params))
        }
    }

    /// Store a copy of the contract in the local store, in case it hasn't been stored previously.
    pub fn store_contract(&mut self, contract: WrappedContract) -> RuntimeResult<()> {
        let contract_hash = contract.key().contract_part().ok_or_else(|| {
            tracing::warn!(
                "trying to store partially unspecified contract `{}`",
                contract.key()
            );
            ContractRuntimeError::UnwrapContract
        })?;
        if self.contract_cache.get(contract_hash).is_some() {
            return Ok(());
        }
        self.key_to_code_part
            .insert(*contract.key(), *contract_hash);

        let key_path = bs58::encode(contract_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase();
        let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        if let Ok(code) = WrappedContract::get_data_from_fs(&key_path) {
            let size = code.data().len() as i64;
            self.contract_cache
                .insert(*contract_hash, Arc::new(code), size);
            return Ok(());
        }

        // insert in the memory cache
        let size = contract.code().data().len() as i64;
        let data = contract.code().data().to_vec();
        self.contract_cache
            .insert(*contract_hash, Arc::new(ContractCode::from(data)), size);

        let mut file = File::create(key_path)?;
        file.write_all(contract.code().data())?;

        Ok(())
    }

    pub fn get_contract_path(&mut self, key: &ContractKey) -> RuntimeResult<PathBuf> {
        let contract_hash = match key.contract_part() {
            Some(k) => *k,
            None => {
                // part not specified, then try it from the fullkey to code map
                *self
                    .key_to_code_part
                    .get(key)
                    .ok_or_else(|| {
                        tracing::warn!("trying to store partially unspecified contract `{key}`");
                        ContractRuntimeError::UnwrapContract
                    })?
                    .value()
            }
        };

        let key_path = bs58::encode(contract_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase();
        Ok(self.contracts_dir.join(key_path).with_extension("wasm"))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = ContractStore::new(
            std::env::temp_dir().join("locutus_test").join("contracts"),
            10_000,
        );
        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![0, 1, 2])),
            [0, 1].as_ref().into(),
        );
        store.store_contract(contract.clone())?;
        let f = store.fetch_contract(contract.key(), &([0, 1].as_ref().into()));
        assert!(f.is_some());
        Ok(())
    }
}
