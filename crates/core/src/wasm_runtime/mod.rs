mod contract;
mod contract_store;
mod delegate;
pub(crate) mod delegate_api;
mod delegate_store;
pub(crate) mod engine;
mod error;
pub(crate) mod mock_state_storage;
mod native_api;
mod runtime;
mod secrets_store;
pub(crate) mod simulation_runtime;
mod state_store;
mod store;
#[cfg(all(test, feature = "wasmer-backend"))]
mod tests;

pub(crate) use contract::ContractRuntimeInterface;
pub use contract_store::ContractStore;
pub(crate) use delegate::DelegateRuntimeInterface;
pub use delegate_store::DelegateStore;
pub(crate) use engine::BackendEngine;
pub(crate) use error::{ContractError, RuntimeInnerError, RuntimeResult};
pub use mock_state_storage::MockStateStorage;
pub use runtime::{ContractExecError, Runtime, DEFAULT_MODULE_CACHE_CAPACITY};
pub(crate) use runtime::{RuntimeConfig, SharedModuleCache};
pub(crate) use secrets_store::SecretStoreError;
pub use secrets_store::SecretsStore;
// NOTE: InMemoryContractStore and SimulationStores are available but currently unused
// They provide infrastructure for more sophisticated simulation scenarios
#[allow(unused_imports)]
pub(crate) use simulation_runtime::{InMemoryContractStore, SimulationStores};
pub use state_store::StateStore;
pub(crate) use state_store::{StateStorage, StateStoreError};
