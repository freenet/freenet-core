use core::future::Future;
use freenet_stdlib::prelude::*;
use stretto::AsyncCache;

#[derive(thiserror::Error, Debug)]
pub enum StateStoreError {
    #[error(transparent)]
    Any(#[from] anyhow::Error),
    #[error("missing contract: {0}")]
    MissingContract(ContractKey),
}

impl From<StateStoreError> for crate::wasm_runtime::ContractError {
    fn from(value: StateStoreError) -> Self {
        match value {
            StateStoreError::Any(err) => {
                crate::wasm_runtime::ContractError::from(anyhow::format_err!(err))
            }
            err @ StateStoreError::MissingContract(_) => {
                crate::wasm_runtime::ContractError::from(anyhow::format_err!(err))
            }
        }
    }
}

pub trait StateStorage {
    type Error;
    fn store(
        &mut self,
        key: ContractKey,
        state: WrappedState,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn store_params(
        &mut self,
        key: ContractKey,
        state: Parameters<'static>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn get(
        &self,
        key: &ContractKey,
    ) -> impl Future<Output = Result<Option<WrappedState>, Self::Error>> + Send;
    fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> impl Future<Output = Result<Option<Parameters<'static>>, Self::Error>> + Send + 'a;
}

pub struct StateStore<S: StateStorage> {
    state_mem_cache: AsyncCache<ContractKey, WrappedState>,
    // params_mem_cache: AsyncCache<ContractKey, Parameters<'static>>,
    store: S,
}

impl<S> StateStore<S>
where
    S: StateStorage + Send + 'static,
    <S as StateStorage>::Error: Into<anyhow::Error>,
{
    const AVG_STATE_SIZE: usize = 1_000;

    /// # Arguments
    /// - max_size: max number of bytes for the mem cache
    pub fn new(store: S, max_size: u32) -> Result<Self, StateStoreError> {
        let counters = max_size as usize / Self::AVG_STATE_SIZE * 10;
        Ok(Self {
            state_mem_cache: AsyncCache::new(counters, max_size as i64, tokio::spawn)
                .map_err(|err| StateStoreError::Any(anyhow::anyhow!(err)))?,
            // params_mem_cache: AsyncCache::new(counters, max_size as i64)
            //     .map_err(|err| StateStoreError::Any(Box::new(err)))?,
            store,
        })
    }

    pub async fn update(
        &mut self,
        key: &ContractKey,
        state: WrappedState,
    ) -> Result<(), StateStoreError> {
        // only allow updates for existing contracts
        if self.state_mem_cache.get(key).await.is_none() {
            self.store
                .get(key)
                .await
                .map_err(Into::into)?
                .ok_or_else(|| StateStoreError::MissingContract(*key))?;
        }
        self.store
            .store(*key, state.clone())
            .await
            .map_err(Into::into)?;
        let cost = state.size() as i64;
        self.state_mem_cache.insert(*key, state, cost).await;
        Ok(())
    }

    pub async fn store(
        &mut self,
        key: ContractKey,
        state: WrappedState,
        params: Parameters<'static>,
    ) -> Result<(), StateStoreError> {
        self.store
            .store(key, state.clone())
            .await
            .map_err(Into::into)?;
        let cost = state.size() as i64;
        self.state_mem_cache.insert(key, state, cost).await;
        self.store
            .store_params(key, params.clone())
            .await
            .map_err(Into::into)?;
        // let cost = params.size();
        // self.params_mem_cache.insert(key, params, cost as i64).await;
        Ok(())
    }

    pub async fn get(&self, key: &ContractKey) -> Result<WrappedState, StateStoreError> {
        if let Some(v) = self.state_mem_cache.get(key).await {
            return Ok(v.value().clone());
        }
        let r = self.store.get(key).await.map_err(Into::into)?;
        r.ok_or_else(|| StateStoreError::MissingContract(*key))
    }

    pub async fn get_params<'a>(
        &'a self,
        key: &'a ContractKey,
    ) -> Result<Option<Parameters<'static>>, StateStoreError> {
        // if let Some(v) = self.params_mem_cache.get(key) {
        //     return Ok(v.value().clone());
        // }
        let r = self.store.get_params(key).await.map_err(Into::into)?;
        Ok(r)
    }
}
