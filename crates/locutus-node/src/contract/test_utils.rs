use crate::operations::put::ContractPutValue;

use super::{
    store::{ContractHandler, ContractHandlerChannel},
    Contract, ContractKey,
};

pub(crate) struct MemoryContractHandler {
    channel: ContractHandlerChannel<String>,
}

impl MemoryContractHandler {
    pub fn new(channel: ContractHandlerChannel<String>) -> Self {
        MemoryContractHandler { channel }
    }
}

#[async_trait::async_trait]
impl ContractHandler for MemoryContractHandler {
    type Error = String;

    #[inline(always)]
    fn channel(&self) -> &ContractHandlerChannel<Self::Error> {
        &self.channel
    }

    async fn fetch_contract(&self, key: &ContractKey) -> Result<Option<Contract>, Self::Error> {
        todo!()
    }

    async fn store_contract(&mut self, contract: Contract) -> Result<(), Self::Error> {
        todo!()
    }

    async fn put_value(&mut self, contract: &ContractKey) -> Result<ContractPutValue, Self::Error> {
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
