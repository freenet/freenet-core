use crate::node::SimStorageError;

use super::{
    store::{ContractHandler, ContractHandlerChannel, StoreResponse},
    Contract, ContractKey, ContractValue,
};

pub(crate) struct MemoryContractHandler {
    channel: ContractHandlerChannel<SimStorageError>,
}

impl MemoryContractHandler {
    pub fn new(channel: ContractHandlerChannel<SimStorageError>) -> Self {
        MemoryContractHandler { channel }
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = SimStorageError;

    #[inline(always)]
    fn channel(&self) -> &ContractHandlerChannel<Self::Error> {
        &self.channel
    }

    async fn fetch_contract(&self, _key: &ContractKey) -> Result<StoreResponse, Self::Error> {
        todo!()
    }

    async fn store_contract(&mut self, _contract: Contract) -> Result<(), Self::Error> {
        todo!()
    }

    async fn put_value(
        &mut self,
        _contract: &ContractKey,
        _value: ContractValue,
    ) -> Result<ContractValue, Self::Error> {
        todo!()
    }
}

#[test]
fn serialization() -> Result<(), Box<dyn std::error::Error>> {
    let bytes = crate::test_utils::random_bytes_1024();
    let mut gen = arbitrary::Unstructured::new(&bytes);
    let contract: Contract = gen.arbitrary().map_err(|_| "failed gen arb data")?;

    let serialized = bincode::serialize(&contract)?;
    let deser: Contract = bincode::deserialize(&serialized)?;
    assert_eq!(deser.data, contract.data);
    assert_eq!(deser.key, contract.key);
    Ok(())
}
