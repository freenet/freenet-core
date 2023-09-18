use dashmap::DashMap;
use locutus_runtime::{ContractKey, StateStorage, WrappedState};

#[derive(Default, Clone)]
pub(crate) struct MemKVStore(DashMap<ContractKey, WrappedState>);

#[async_trait::async_trait]
impl StateStorage for MemKVStore {
    type Error = String;

    async fn store(
        &mut self,
        _key: ContractKey,
        _state: locutus_runtime::WrappedState,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get(
        &self,
        _key: &ContractKey,
    ) -> Result<Option<locutus_runtime::WrappedState>, Self::Error> {
        todo!()
    }

    async fn store_params(
        &mut self,
        _key: ContractKey,
        _state: locutus_runtime::Parameters<'static>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    async fn get_params<'a>(
        &'a self,
        _key: &'a ContractKey,
    ) -> Result<Option<locutus_runtime::Parameters<'static>>, Self::Error> {
        todo!()
    }
}

impl MemKVStore {
    pub fn new() -> Self {
        Self::default()
    }
}
