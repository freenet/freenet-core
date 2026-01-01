//! In-memory runtime for deterministic simulation testing.
//!
//! This module provides fully in-memory implementations of all stores
//! used by the contract executor. Unlike the production runtime which
//! uses disk-based storage, this runtime keeps everything in memory,
//! enabling:
//!
//! - Deterministic behavior (no disk I/O timing variance)
//! - State persistence across node crash/restart (via Arc sharing)
//! - State inspection for test assertions
//!
//! # Usage in SimNetwork
//!
//! ```ignore
//! // Create shared stores for a node
//! let stores = SimulationStores::new();
//!
//! // On restart, clone the same stores (Arc sharing preserves state)
//! let stores_for_restart = stores.clone();
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use freenet_stdlib::prelude::*;

use super::mock_state_storage::MockStateStorage;

/// In-memory contract code storage for simulation.
///
/// Unlike `ContractStore` which persists WASM files to disk, this stores
/// everything in memory using HashMaps. The Arc wrapper allows sharing
/// across node restarts.
#[derive(Clone, Default)]
pub struct InMemoryContractStore {
    inner: Arc<Mutex<InMemoryContractStoreInner>>,
}

#[derive(Default)]
struct InMemoryContractStoreInner {
    /// Map from code hash to contract code
    code_by_hash: HashMap<CodeHash, Arc<ContractCode<'static>>>,
    /// Map from instance ID to (code hash, parameters)
    instance_to_code: HashMap<ContractInstanceId, (CodeHash, Parameters<'static>)>,
}

impl InMemoryContractStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch a contract by key and parameters.
    pub fn fetch_contract(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer> {
        let inner = self.inner.lock().unwrap();
        let code_hash = key.code_hash();

        // Try to get the code by hash
        let code = inner.code_by_hash.get(code_hash)?;

        Some(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
            WrappedContract::new(code.clone(), params.clone().into_owned()),
        )))
    }

    /// Store a contract in memory.
    pub fn store_contract(&self, contract: ContractContainer) -> Result<(), anyhow::Error> {
        let (key, code, params) = match contract {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                let key = *contract_v1.key();
                let code = contract_v1.code().clone();
                let params = contract_v1.params().into_owned();
                (key, code, params)
            }
            _ => return Err(anyhow::anyhow!("unsupported contract type")),
        };

        let code_hash = *key.code_hash();
        let mut inner = self.inner.lock().unwrap();

        // Store code by hash (deduplicates same code with different params)
        inner.code_by_hash.insert(code_hash, Arc::new(code.into_owned()));

        // Map instance ID to code hash and params
        inner.instance_to_code.insert(*key.id(), (code_hash, params));

        Ok(())
    }

    /// Get code hash from instance ID.
    pub fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash> {
        let inner = self.inner.lock().unwrap();
        inner.instance_to_code.get(id).map(|(hash, _)| *hash)
    }

    /// Get the number of stored contracts (for testing).
    pub fn contract_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.code_by_hash.len()
    }

    /// Get the number of contract instances (for testing).
    pub fn instance_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.instance_to_code.len()
    }

    /// Clear all stored contracts (for testing).
    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.code_by_hash.clear();
        inner.instance_to_code.clear();
    }
}

/// All stores needed for simulation, bundled together.
///
/// This is designed to be cloned and shared across node restarts.
/// All internal storage uses Arc, so clones share the same data.
#[derive(Clone)]
pub struct SimulationStores {
    /// In-memory contract code storage
    pub contract_store: InMemoryContractStore,
    /// In-memory contract state storage
    pub state_storage: MockStateStorage,
}

impl Default for SimulationStores {
    fn default() -> Self {
        Self::new()
    }
}

impl SimulationStores {
    /// Create a new set of empty simulation stores.
    pub fn new() -> Self {
        Self {
            contract_store: InMemoryContractStore::new(),
            state_storage: MockStateStorage::new(),
        }
    }

    /// Get a summary of stored data (for debugging/testing).
    pub fn summary(&self) -> SimulationStoresSummary {
        SimulationStoresSummary {
            contract_count: self.contract_store.contract_count(),
            instance_count: self.contract_store.instance_count(),
            state_operation_count: self.state_storage.operation_count(),
        }
    }
}

/// Summary of simulation stores state (for debugging).
#[derive(Debug, Clone)]
pub struct SimulationStoresSummary {
    pub contract_count: usize,
    pub instance_count: usize,
    pub state_operation_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_contract_store_basic() {
        let store = InMemoryContractStore::new();

        // Create a test contract
        let code = ContractCode::from(vec![0x00, 0x61, 0x73, 0x6d]); // WASM magic
        let params = Parameters::from(vec![1, 2, 3]);
        let contract = WrappedContract::new(Arc::new(code), params.clone());
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));

        // Store it
        store.store_contract(container).unwrap();

        // Should be fetchable
        let key = contract.key();
        let fetched = store.fetch_contract(&key, &params);
        assert!(fetched.is_some());

        assert_eq!(store.contract_count(), 1);
        assert_eq!(store.instance_count(), 1);
    }

    #[test]
    fn test_in_memory_contract_store_sharing() {
        let store1 = InMemoryContractStore::new();

        // Store a contract
        let code = ContractCode::from(vec![0x00, 0x61, 0x73, 0x6d]);
        let params = Parameters::from(vec![1, 2, 3]);
        let contract = WrappedContract::new(Arc::new(code), params.clone());
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract.clone()));
        store1.store_contract(container).unwrap();

        // Clone the store (simulating restart)
        let store2 = store1.clone();

        // Should be visible in the clone
        let key = contract.key();
        let fetched = store2.fetch_contract(&key, &params);
        assert!(fetched.is_some(), "Cloned store should see the same data");
    }

    #[test]
    fn test_simulation_stores_sharing() {
        let stores1 = SimulationStores::new();

        // Store a contract
        let code = ContractCode::from(vec![0x00, 0x61, 0x73, 0x6d]);
        let params = Parameters::from(vec![1, 2, 3]);
        let contract = WrappedContract::new(Arc::new(code), params.clone());
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(contract));
        stores1.contract_store.store_contract(container).unwrap();

        // Clone (simulating node restart)
        let stores2 = stores1.clone();

        // Data should be shared
        assert_eq!(stores1.contract_store.contract_count(), stores2.contract_store.contract_count());
    }
}
