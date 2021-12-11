use super::{Contract, ContractError, ContractKey};

/// Handle contract blob storage on the file system.
pub(crate) struct ContractStore {}

impl ContractStore {
    pub fn new() -> Self {
        Self {}
    }
    /// Returns a copy of the contract bytes if available, none otherwise.
    pub async fn fetch_contract<CErr>(
        &self,
        _key: &ContractKey,
    ) -> Result<Option<Contract>, ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        todo!()
    }

    /// Store a copy of the contract in the local store.
    pub async fn store_contract<CErr>(
        &mut self,
        _contract: Contract,
    ) -> Result<(), ContractError<CErr>>
    where
        CErr: std::error::Error,
    {
        // TODO
        Ok(())
    }
}
