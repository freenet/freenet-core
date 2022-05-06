use std::path::PathBuf;

use locutus_stdlib::prelude::{ContractData, ContractSpecification, Parameters};
use stretto::Cache;

use crate::{contract::Contract, RuntimeResult};

use super::ContractKey;

type KeyContractPart = [u8; 64];

/// Handle contract blob storage on the file system.
#[derive(Clone)]
pub struct ContractStore {
    contracts_dir: PathBuf,
    contract_cache: Cache<KeyContractPart, Contract>,
    cached_params: Cache<ContractKey, Contract>,
}
// TODO: add functionality to delete old contracts which have not been used for a while
//       to keep the total speed used under a configured threshold

impl ContractStore {
    pub fn new(contracts_dir: PathBuf, max_size: i64) -> Self {
        const ERR: &str = "failed to build mem cache";
        Self {
            contract_cache: Cache::new(100, max_size).expect(ERR),
            contracts_dir,
            cached_params: Cache::new(10_000, max_size).expect(ERR),
        }
    }
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub fn fetch_contract<'a>(
        &self,
        key: &ContractKey,
        params: &Parameters<'a>,
    ) -> Option<ContractSpecification<'a>> {
        let contract_hash = key.contract_part();
        let whole_spec_key = key.bytes();
        if let Some(contract) = self.contract_cache.get(contract_hash) {
            // Ok(Some(contract.as_ref().clone()))
            todo!()
        } else {
            let path = bs58::encode(contract_hash)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string();
            let key_path = self.contracts_dir.join(path).with_extension("wasm");
            let contract = Contract::try_from(&*key_path)
                .map_err(|err| {
                    tracing::debug!("contract not found: {err}");
                    err
                })
                .ok()?;

            // add back the contract part to the mem store
            let size = contract.data().len() as i64;
            self.contract_cache.insert(
                *contract_hash,
                Contract::new(contract.data().to_vec()),
                size,
            );

            Some(ContractSpecification::new(
                ContractData::from(contract.data().to_vec()),
                params.clone(),
            ))
        }
    }

    /// Store a copy of the contract in the local store.
    pub fn store_contract(&mut self, contract: ContractSpecification) -> RuntimeResult<()> {
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
