mod contract;
mod contract_store;
mod delegate;
pub(crate) mod delegate_api;
mod delegate_store;
pub(crate) mod engine;
mod error;
pub(crate) mod mock_state_storage;
mod module_cache;
mod native_api;
mod runtime;
pub mod secret_export;
pub mod secret_snapshots;
mod secrets_store;
pub(crate) mod simulation_runtime;
mod state_store;
#[cfg(all(test, feature = "wasmtime-backend"))]
mod tests;

pub(crate) use contract::{ContractRuntimeBridge, ContractRuntimeInterface, ContractStoreBridge};
pub use contract_store::ContractStore;
pub(crate) use delegate::DelegateRuntimeInterface;
pub use delegate_store::DelegateStore;
pub(crate) use engine::BackendEngine;
pub(crate) use error::{ContractError, RuntimeInnerError, RuntimeResult};
pub use mock_state_storage::MockStateStorage;
pub use module_cache::default_module_cache_budget_bytes;
pub(crate) use module_cache::{
    DELEGATE_MODULE_CACHE_BUDGET_DIVISOR, ModuleCache, ModuleCacheMetrics,
    contract_cache_occupancy_pct,
};
// Clamp bounds are referenced only by the config-default round-trip test, which
// asserts the resolved default lands within [MIN, MAX] without hardcoding the
// byte values (so the test can't drift from the clamp). Gated to test builds so
// the re-export isn't an unused import under `-D warnings` in release.
#[cfg(test)]
pub(crate) use module_cache::{
    MAX_DEFAULT_MODULE_CACHE_BUDGET_BYTES, MIN_DEFAULT_MODULE_CACHE_BUDGET_BYTES,
};
pub(crate) use native_api::{
    CREATED_DELEGATES_COUNT, DELEGATE_INHERITED_ORIGINS, DELEGATE_SUBSCRIPTIONS,
    DelegateContextCache, new_delegate_context_cache,
};
// Only constructed by name in test code (e.g. resolve_message_origin tests);
// production read/write paths access the entry through the DashMap without
// naming the type, so gate the re-export to avoid an unused-import warning.
#[cfg(test)]
pub(crate) use native_api::InheritedOriginsEntry;
pub use runtime::{ContractExecError, Runtime};
pub(crate) use runtime::{RuntimeConfig, SharedModuleCache};
pub use secrets_store::{
    ExportSecretEntry, SecretScope, SecretStoreError, SecretsStore, UserSecretContext,
};
// NOTE: InMemoryContractStore and SimulationStores are available but currently unused
// They provide infrastructure for more sophisticated simulation scenarios
#[allow(unused_imports)]
pub(crate) use simulation_runtime::{InMemoryContractStore, SimulationStores};
pub use state_store::StateStore;
pub(crate) use state_store::{MAX_STATE_SIZE, StateStorage, StateStoreError};

/// Rename a code-hash-named WASM file from the legacy all-lowercase Base58
/// name to the canonical mixed-case name (issue #4214).
///
/// `freenet-stdlib` used to lowercase `CodeHash::encode()` output, which is the
/// name both the contract and delegate stores use for on-disk WASM files. Once
/// stdlib stops lowercasing, a node upgrading across that change would compute
/// the mixed-case name and fail to find code it persisted earlier — silently
/// re-fetching contracts from the network and orphaning locally-registered
/// delegate code. This one-time, self-healing rename keeps that state reachable.
///
/// `encoded` is the canonical `CodeHash::encode()` output. It is a no-op when
/// the canonical file already exists, when the encoding has no uppercase
/// characters (nothing to migrate), or when no legacy file is present.
fn migrate_legacy_lowercased_code_file(dir: &std::path::Path, encoded: &str, extension: &str) {
    let canonical = dir.join(encoded).with_extension(extension);
    if canonical.exists() {
        return;
    }
    let legacy_encoded = encoded.to_lowercase();
    if legacy_encoded == encoded {
        return;
    }
    let legacy = dir.join(&legacy_encoded).with_extension(extension);
    if !legacy.exists() {
        return;
    }
    match std::fs::rename(&legacy, &canonical) {
        Ok(()) => tracing::info!(
            "migrated legacy lowercased code file {} -> {}",
            legacy.display(),
            canonical.display()
        ),
        Err(e) => tracing::warn!(
            "failed to migrate legacy lowercased code file {}: {e}",
            legacy.display()
        ),
    }
}

#[cfg(test)]
mod migration_tests {
    use super::migrate_legacy_lowercased_code_file;

    #[test]
    fn renames_legacy_lowercased_file_to_canonical() {
        let dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(dir.path()).unwrap();
        // Canonical encoding contains uppercase Base58 characters.
        let canonical = "AbCdEf123";
        let legacy = dir.path().join("abcdef123").with_extension("wasm");
        std::fs::write(&legacy, b"wasm-bytes").unwrap();

        migrate_legacy_lowercased_code_file(dir.path(), canonical, "wasm");

        let canonical_path = dir.path().join(canonical).with_extension("wasm");
        assert!(canonical_path.exists(), "canonical file should exist");
        assert!(!legacy.exists(), "legacy file should be renamed away");
        assert_eq!(std::fs::read(&canonical_path).unwrap(), b"wasm-bytes");
    }

    #[test]
    fn prefers_canonical_when_both_exist() {
        let dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(dir.path()).unwrap();
        let canonical = "AbCdEf123";
        let canonical_path = dir.path().join(canonical).with_extension("wasm");
        let legacy = dir.path().join("abcdef123").with_extension("wasm");
        std::fs::write(&canonical_path, b"canonical").unwrap();
        std::fs::write(&legacy, b"legacy").unwrap();

        migrate_legacy_lowercased_code_file(dir.path(), canonical, "wasm");

        // Canonical is left untouched and the legacy file is not consumed.
        assert_eq!(std::fs::read(&canonical_path).unwrap(), b"canonical");
        assert!(legacy.exists(), "legacy file must not clobber canonical");
    }

    #[test]
    fn noop_when_encoding_has_no_uppercase() {
        let dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(dir.path()).unwrap();
        // All-lowercase canonical name: there is no distinct legacy name, so the
        // helper must not touch an identically-named file.
        let canonical = "abcdef123";
        let path = dir.path().join(canonical).with_extension("wasm");
        std::fs::write(&path, b"data").unwrap();

        migrate_legacy_lowercased_code_file(dir.path(), canonical, "wasm");

        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"data");
    }

    #[test]
    fn noop_when_no_legacy_file_present() {
        let dir = crate::util::tests::get_temp_dir();
        std::fs::create_dir_all(dir.path()).unwrap();
        // Canonical has uppercase but nothing is on disk: must not create files.
        migrate_legacy_lowercased_code_file(dir.path(), "AbCdEf123", "wasm");
        assert!(!dir.path().join("AbCdEf123").with_extension("wasm").exists());
        assert!(!dir.path().join("abcdef123").with_extension("wasm").exists());
    }
}
