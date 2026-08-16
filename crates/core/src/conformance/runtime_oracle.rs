//! The production oracle: real WASM, real wasmtime, real limits.
//!
//! Conformance findings are only worth anything if they were produced by the same
//! runtime configuration the network actually runs. A check that passes under a
//! permissive test harness and fails in production (or the reverse) would make
//! `fdev conformance` an unreliable oracle for authors and, later, would make peers
//! disagree about the same evidence. So this wraps [`crate::wasm_runtime::Runtime`]
//! directly rather than reimplementing anything.

use std::sync::Arc;

use freenet_stdlib::prelude::{
    ContractCode, ContractContainer, ContractInstanceId, ContractKey, ContractWasmAPIVersion,
    Parameters, RelatedContracts, StateSummary, UpdateData, UpdateModification, ValidateResult,
    WrappedContract, WrappedState,
};

use super::oracle::{ConformanceOracle, OracleError};
use crate::wasm_runtime::{
    ContractError as RuntimeContractError, ContractExecError, ContractRuntimeInterface,
    ContractStore, DelegateStore, Runtime, RuntimeInnerError, SecretsStore,
};

/// Byte budget for the throwaway contract store backing a standalone oracle.
const STANDALONE_CONTRACT_STORE_BYTES: u64 = 1024 * 1024 * 1024;
const STANDALONE_DELEGATE_STORE_BYTES: u64 = 10_000_000;

/// Why an oracle could not be built.
///
/// A concrete type rather than a boxed error because callers — `fdev`, tests, and
/// eventually the node — need to tell "this WASM is not loadable" apart from "this
/// machine could not make a temp directory". Only the first is a statement about
/// the contract.
#[derive(Debug, thiserror::Error)]
pub enum OracleBuildError {
    #[error("could not create scratch directory for the conformance runtime: {0}")]
    Scratch(#[from] std::io::Error),
    #[error("could not open the scratch storage backend: {0}")]
    Storage(String),
    #[error("could not build the contract runtime: {0}")]
    Runtime(#[from] RuntimeContractError),
}

/// A single contract, pinned to its code and parameters, executable on demand.
pub struct RuntimeOracle {
    runtime: Runtime,
    key: ContractKey,
    parameters: Parameters<'static>,
    /// Kept alive for as long as the oracle runs: dropping it deletes the store the
    /// runtime reads the contract from.
    _scratch: Option<tempfile::TempDir>,
}

impl RuntimeOracle {
    /// Build an oracle over raw WASM in a throwaway store.
    ///
    /// This is the `fdev` path and the offline-replay path: no node, no hosted
    /// state, nothing that outlives the check.
    pub async fn standalone(wasm: Vec<u8>, parameters: Vec<u8>) -> Result<Self, OracleBuildError> {
        let scratch = tempfile::TempDir::new()?;
        let db = crate::contract::storages::Storage::new(scratch.path())
            .await
            .map_err(|e| OracleBuildError::Storage(e.to_string()))?;
        let contract_store = ContractStore::new(
            scratch.path().join("contract"),
            STANDALONE_CONTRACT_STORE_BYTES,
            db.clone(),
        )?;
        let delegate_store = DelegateStore::new(
            scratch.path().join("delegate"),
            STANDALONE_DELEGATE_STORE_BYTES,
            db.clone(),
        )?;
        // A conformance oracle never runs a delegate, so the secrets store exists
        // only to satisfy the runtime's constructor. Pointing it at the scratch
        // directory keeps the freshly-generated keys inside the temp dir, where they
        // die with it.
        let secrets_dir = scratch.path().join("secrets");
        std::fs::create_dir_all(&secrets_dir)?;
        let secrets = crate::config::Secrets::load_for_secrets_dir(&secrets_dir)?;
        let secrets_store = SecretsStore::new(secrets_dir, secrets, db)?;
        let mut runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;

        let parameters = Parameters::from(parameters);
        let container = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
            Arc::new(ContractCode::from(wasm)),
            parameters.clone(),
        )));
        let key = container.key();
        runtime.contract_store.store_contract(container)?;

        // Compile now rather than lazily on the first check. `store_contract` only
        // verifies that the key matches the bytes, so malformed WASM or a module
        // missing the contract ABI would otherwise surface as an `Inconclusive`
        // result much later — and "we could not judge this contract" would be
        // indistinguishable from "this contract would not load", letting a run
        // against a broken file report success.
        runtime.compile_check(&key, &parameters)?;

        Ok(Self {
            runtime,
            key,
            parameters,
            _scratch: Some(scratch),
        })
    }

    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    pub fn instance_id(&self) -> ContractInstanceId {
        *self.key.id()
    }

    pub fn parameters(&self) -> &Parameters<'static> {
        &self.parameters
    }
}

impl ConformanceOracle for RuntimeOracle {
    fn validate_state(
        &mut self,
        state: &[u8],
        related: &RelatedContracts<'_>,
    ) -> Result<ValidateResult, OracleError> {
        let state = WrappedState::new(state.to_vec());
        self.runtime
            .validate_state(&self.key, &self.parameters, &state, related)
            .map_err(classify)
    }

    fn update_state(
        &mut self,
        state: &[u8],
        updates: &[UpdateData<'_>],
    ) -> Result<UpdateModification<'static>, OracleError> {
        let state = WrappedState::new(state.to_vec());
        self.runtime
            .update_state(&self.key, &self.parameters, &state, updates)
            .map_err(classify)
    }

    fn summarize_state(&mut self, state: &[u8]) -> Result<Vec<u8>, OracleError> {
        let state = WrappedState::new(state.to_vec());
        self.runtime
            .summarize_state(&self.key, &self.parameters, &state)
            .map(StateSummary::into_bytes)
            .map_err(classify)
    }

    fn get_state_delta(&mut self, state: &[u8], summary: &[u8]) -> Result<Vec<u8>, OracleError> {
        let state = WrappedState::new(state.to_vec());
        let summary = StateSummary::from(summary.to_vec());
        self.runtime
            .get_state_delta(&self.key, &self.parameters, &state, &summary)
            .map(|delta| delta.into_bytes())
            .map_err(classify)
    }
}

/// Classify a runtime failure by its *typed* provenance.
///
/// Not by message text. A contract can return a rejection whose text mentions gas or
/// timeouts, and the repo has already been bitten by string-matched classification
/// letting a contract self-inflict a class it should not be able to reach (see the
/// note on [`ContractExecError::SchedulerOverloaded`]). Here the stakes are the same
/// shape: a contract that could forge a `Resource` classification could make its own
/// violations permanently inconclusive.
fn classify(err: RuntimeContractError) -> OracleError {
    match err.deref() {
        RuntimeInnerError::ContractExecError(exec) => match exec {
            ContractExecError::OutOfGas
            | ContractExecError::MaxComputeTimeExceeded
            | ContractExecError::SchedulerOverloaded => OracleError::resource(err.to_string()),
            ContractExecError::ContractError(_) => OracleError::contract(err.to_string()),
            ContractExecError::DoublePut(_)
            | ContractExecError::InvalidArrayLength(_)
            | ContractExecError::MissingContractExports { .. }
            | ContractExecError::UnexpectedResult => OracleError::runtime(err.to_string()),
        },
        RuntimeInnerError::Any(_)
        | RuntimeInnerError::BufferError(_)
        | RuntimeInnerError::IOError(_)
        | RuntimeInnerError::SecretStoreError(_)
        | RuntimeInnerError::Serialization(_)
        | RuntimeInnerError::DelegateNotFound(_)
        | RuntimeInnerError::DelegateIdentityMismatch { .. }
        | RuntimeInnerError::DelegateExecError(_)
        | RuntimeInnerError::ContractNotFound(_)
        | RuntimeInnerError::ContractIdentityMismatch { .. }
        | RuntimeInnerError::WasmError(_) => OracleError::runtime(err.to_string()),
    }
}
