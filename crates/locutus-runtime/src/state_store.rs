use std::future::Future;
use std::pin::Pin;

use locutus_stdlib::prelude::{ContractKey, Parameters};
use stretto::AsyncCache;

use crate::{DynError, WrappedV1State};

#[derive(thiserror::Error, Debug)]
pub enum StateStoreError {
    #[error(transparent)]
    Any(#[from] DynError),
    #[error("missing contract: {0}")]
    MissingContract(ContractKey),
}

#[async_trait::async_trait]
#[allow(clippy::type_complexity)]
pub trait StateStorage {
    type Error;
    async fn store(&mut self, key: ContractKey, state: WrappedV1State) -> Result<(), Self::Error>;
    async fn store_params(
        &mut self,
        key: ContractKey,
        state: Parameters<'static>,
    ) -> Result<(), Self::Error>;
    async fn get(&self, key: &ContractKey) -> Result<Option<WrappedV1State>, Self::Error>;
    fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Parameters<'static>>, Self::Error>> + Send + 'a>>;
}

pub struct StateStore<S: StateStorage> {
    state_mem_cache: AsyncCache<ContractKey, WrappedV1State>,
    // params_mem_cache: AsyncCache<ContractKey, Parameters<'static>>,
    store: S,
}

impl<S> StateStore<S>
where
    S: StateStorage + Send + Sync + 'static,
    <S as StateStorage>::Error: Into<DynError>,
{
    const AVG_STATE_SIZE: usize = 1_000;

    /// # Arguments
    /// - max_size: max number of bytes for the mem cache
    pub fn new(store: S, max_size: u32) -> Result<Self, StateStoreError> {
        let counters = max_size as usize / Self::AVG_STATE_SIZE * 10;
        Ok(Self {
            state_mem_cache: AsyncCache::new(counters, max_size as i64, tokio::spawn)
                .map_err(|err| StateStoreError::Any(Box::new(err)))?,
            // params_mem_cache: AsyncCache::new(counters, max_size as i64)
            //     .map_err(|err| StateStoreError::Any(Box::new(err)))?,
            store,
        })
    }

    pub async fn store(
        &mut self,
        key: ContractKey,
        state: WrappedV1State,
        params: Option<Parameters<'static>>,
    ) -> Result<(), StateStoreError> {
        self.store
            .store(key.clone(), state.clone())
            .await
            .map_err(Into::into)?;
        let cost = state.size() as i64;
        self.state_mem_cache.insert(key.clone(), state, cost).await;
        if let Some(params) = params {
            self.store
                .store_params(key, params.clone())
                .await
                .map_err(Into::into)?;
            // let cost = params.size();
            // self.params_mem_cache.insert(key, params, cost as i64).await;
        }
        Ok(())
    }

    pub async fn get(&self, key: &ContractKey) -> Result<WrappedV1State, StateStoreError> {
        if let Some(v) = self.state_mem_cache.get(key).await {
            return Ok(v.value().clone());
        }
        self.store
            .get(key)
            .await
            .map_err(Into::into)?
            .ok_or_else(|| StateStoreError::MissingContract(key.clone()))
    }

    pub fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Pin<Box<dyn Future<Output = Result<Parameters<'static>, StateStoreError>> + Send + 'a>>
    {
        // if let Some(v) = self.params_mem_cache.get(key) {
        //     return Ok(v.value().clone());
        // }
        Box::pin(async move {
            self.store
                .get_params(key)
                .await
                .map_err(Into::into)?
                .ok_or_else(|| StateStoreError::MissingContract(key.clone()))
        })
    }
}
