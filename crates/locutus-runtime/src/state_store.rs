use locutus_stdlib::prelude::ContractKey;
use stretto::AsyncCache;

use crate::{DynError, WrappedState};

#[derive(thiserror::Error, Debug)]
pub enum StateStoreError {
    #[error(transparent)]
    Any(#[from] DynError),
    #[error("missing contract")]
    MissingContract,
}

#[async_trait::async_trait]
pub trait StateStorage {
    type Error;
    async fn store(&mut self, key: ContractKey, state: WrappedState) -> Result<(), Self::Error>;
    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedState>, Self::Error>;
}

pub struct StateStore<S> {
    mem_cache: AsyncCache<ContractKey, WrappedState>,
    store: S,
}

impl<S> StateStore<S>
where
    S: StateStorage,
    <S as StateStorage>::Error: Into<DynError>,
{
    const AVG_STATE_SIZE: usize = 1_000;

    /// # Arguments
    /// - max_size: max number of bytes for the mem cache
    pub fn new(store: S, max_size: u32) -> Result<Self, StateStoreError> {
        let counters = max_size as usize / Self::AVG_STATE_SIZE * 10;
        Ok(Self {
            mem_cache: AsyncCache::new(counters, max_size as i64)
                .map_err(|err| StateStoreError::Any(Box::new(err)))?,
            store,
        })
    }

    pub async fn store(
        &mut self,
        key: ContractKey,
        state: WrappedState,
    ) -> Result<(), StateStoreError> {
        self.store
            .store(key, state.clone())
            .await
            .map_err(Into::into)?;
        let cost = state.size() as i64;
        self.mem_cache.insert(key, state, cost).await;
        Ok(())
    }

    pub async fn get(&self, key: &ContractKey) -> Result<WrappedState, StateStoreError> {
        if let Some(v) = self.mem_cache.get(key) {
            return Ok(v.value().clone());
        }
        self.store
            .get(key)
            .await
            .map_err(Into::into)?
            .ok_or(StateStoreError::MissingContract)
    }
}
