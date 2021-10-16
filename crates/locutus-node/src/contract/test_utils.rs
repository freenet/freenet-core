use crate::operations::put::ContractValue;

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

    async fn fetch_contract(&self, _key: &ContractKey) -> Result<Option<Contract>, Self::Error> {
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
