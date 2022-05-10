use std::{path::PathBuf, sync::Arc};

use locutus_stdlib::prelude::{ContractData, Parameters};
use stretto::Cache;

use crate::{contract::Contract, RuntimeResult};

use super::ContractKey;

type KeyContractPart = [u8; 64];

/// Handle contract blob storage on the file system.
#[derive(Clone)]
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<KeyContractPart, Arc<ContractData<'static>>>,
    // cached_params: Cache<ContractKey, Contract>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

impl ContractStore {
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> Self {
        const ERR: &str = "failed to build mem cache";
        Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            // cached_params: Cache::new(10_000, max_size).expect(ERR),
        }
    }
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub fn fetch_contract<'a>(
        &self,
        key: &ContractKey,
        params: &Parameters<'a>,
    ) -> Option<Contract<'a>> {
        let contract_hash = key.contract_part();
        if let Some(data) = self.contract_cache.get(contract_hash) {
            Some(Contract::new(data.value().clone(), params.clone()))
        } else {
            let path = bs58::encode(contract_hash)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let owned_params = Parameters::from(params.as_ref().to_owned());
            let Contract { data, params, .. } = Contract::try_from((&*key_path, owned_params))
                .map_err(|err| {
                    tracing::debug!("contract not found: {err}");
                    err
                })
                .ok()?;

            // add back the contract part to the mem store
            let size = data.data().len() as i64;
            self.contract_cache
                .insert(*contract_hash, data.clone(), size);
            Some(Contract::new(data, params))
        }
    }

    /// Store a copy of the contract in the local store.
    pub fn store_contract(&mut self, contract: Contract) -> RuntimeResult<()> {
        //fixme
        // {
        //     // store the contract portion of the spec
        //     let key = contract.data().key();

        //     // insert in the memory cache
        //     let size = contract.data().data().len() as i64;
        //     self.contract_cache.insert(*key, contract.data(), size);

        //     // write to disc
        //     let key_path: PathBuf = key.into();
        //     let key_path = self.contracts_dir.join(key_path).with_extension("wasm");
        //     if key_path.exists() {
        //         return Ok(());
        //     }
        //     let mut file = File::create(key_path)?;
        //     file.write_all(contract.data())?;
        // }

        Ok(())
    }
}
