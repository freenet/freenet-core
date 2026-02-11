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

/// Check if a WASM binary contains debug information.
pub fn is_debug_wasm(wasm_bytes: &[u8]) -> bool {
    use wasmer::wasmparser::Parser;
    for payload in Parser::new(0).parse_all(wasm_bytes) {
        match payload {
            Ok(wasmer::wasmparser::Payload::CustomSection(section)) => {
                if section.name().starts_with(".debug_") {
                    return true;
                }
            }
            Ok(_) => {}
            Err(_) => return false,
        }
    }
    false
}

#[cfg(test)]
mod wasm_validation_tests {
    use super::*;

    #[test]
    fn test_is_debug_wasm() {
        // Minimal WASM module: [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
        // followed by a custom section named ".debug_info"
        let mut wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        
        // Custom section ID is 0
        wasm.push(0); 
        
        // Section content: [name_len, name, data]
        let name = ".debug_info";
        let section_data = vec![0x01, 0x02, 0x03]; // some dummy data
        
        let mut content = vec![];
        content.push(name.len() as u8);
        content.extend_from_slice(name.as_bytes());
        content.extend_from_slice(&section_data);
        
        // Section length (leb128 encoded, but for small values it's just the value)
        wasm.push(content.len() as u8);
        wasm.extend_from_slice(&content);

        assert!(is_debug_wasm(&wasm));
        
        // Minimal module without debug info should return false
        let release_wasm = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
        assert!(!is_debug_wasm(&release_wasm));
    }
}
