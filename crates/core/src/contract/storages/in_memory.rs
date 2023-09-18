use crate::runtime::{ContractKey, StateStorage, WrappedState};
use dashmap::DashMap;

#[derive(Default, Clone)]
pub(crate) struct MemKVStore(DashMap<ContractKey, WrappedState>);

#[async_trait::async_trait]
impl StateStorage for MemKVStore {
    type Error = String;

    async fn store(
        &mut self,
        _key: ContractKey,
        _state: crate::runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get(
        &self,
        _key: &ContractKey,
    ) -> Result<Option<crate::runtime::WrappedState>, Self::Error> {
        todo!()
    }

    async fn store_params(
        &mut self,
        _key: ContractKey,
        _state: crate::runtime::Parameters<'static>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get_params<'a>(
        &'a self,
        _key: &'a ContractKey,
    ) -> Result<Option<crate::runtime::Parameters<'static>>, Self::Error> {
        todo!()
    }
}

impl MemKVStore {
    pub fn new() -> Self {
        Self::default()
    }
}
