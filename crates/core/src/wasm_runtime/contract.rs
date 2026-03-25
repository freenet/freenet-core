use super::engine::{WasmEngine, WasmError};
use super::native_api::CONTRACT_IO;
use super::{ContractExecError, RuntimeResult};
use freenet_stdlib::prelude::{
    CodeHash, ContractContainer, ContractInstanceId, ContractInterfaceResult, ContractKey,
    Parameters, RelatedContracts, StateDelta, StateSummary, UpdateData, UpdateModification,
    ValidateResult, WrappedState,
};

/// Maximum buffer size for streaming refill pattern.
const STREAMING_BUF_CAP: usize = 64 * 1024; // 64KB

pub(crate) trait ContractRuntimeInterface {
    /// Verify that the state is valid, given the parameters.
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult>;

    /// Determine whether this delta is a valid update for this contract.
    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>>;

    /// Generate a concise summary of a state.
    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>>;

    /// Generate a state delta using a summary from the current state.
    fn get_state_delta(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        delta_to: &StateSummary<'_>,
    ) -> RuntimeResult<StateDelta<'static>>;
}

/// Abstracts contract code storage needed by the production ContractExecutor.
///
/// Implemented by `Runtime` (delegates to `ContractStore`) and `MockWasmRuntime`
/// (delegates to `InMemoryContractStore`), allowing the production executor logic
/// to work with either backend.
pub(crate) trait ContractStoreBridge {
    fn code_hash_from_id(&self, id: &ContractInstanceId) -> Option<CodeHash>;

    fn fetch_contract_code(
        &self,
        key: &ContractKey,
        params: &Parameters<'_>,
    ) -> Option<ContractContainer>;

    fn store_contract(&mut self, contract: ContractContainer) -> Result<(), anyhow::Error>;

    fn remove_contract(&mut self, key: &ContractKey) -> Result<(), anyhow::Error>;

    fn ensure_key_indexed(&mut self, key: &ContractKey) -> Result<(), anyhow::Error>;
}

/// Combined trait: WASM execution + contract storage. Implemented by `Runtime`
/// (production) and `MockWasmRuntime` (simulation).
pub(crate) trait ContractRuntimeBridge:
    ContractRuntimeInterface + ContractStoreBridge + Send + 'static
{
}

/// RAII guard that cleans up CONTRACT_IO entries for an instance on drop.
/// Ensures cleanup runs even if the contract call panics.
struct ContractIoGuard {
    instance_id: i64,
}

impl ContractIoGuard {
    fn new(instance_id: i64) -> Self {
        Self { instance_id }
    }
}

impl Drop for ContractIoGuard {
    fn drop(&mut self) {
        // Each contract call inserts at most 3 entries (one per buffer argument),
        // so the total CONTRACT_IO size is bounded by concurrent contract calls.
        // The retain scan is acceptable at this scale; if concurrency grows
        // significantly, switch to a two-level map (instance_id → per-buffer map).
        CONTRACT_IO.retain(|(id, _), _| *id != self.instance_id);
    }
}

impl ContractRuntimeInterface for super::Runtime {
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult> {
        let req_bytes = parameters.size() + state.size();
        let mut running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let _io_guard = ContractIoGuard::new(running.id);

        let result = (|| -> RuntimeResult<ValidateResult> {
            let linear_mem = self.linear_mem(&running.handle)?;

            let param_buf_ptr =
                self.write_contract_buf(&running, parameters.as_ref(), STREAMING_BUF_CAP)?;
            let state_buf_ptr =
                self.write_contract_buf(&running, state.as_ref(), STREAMING_BUF_CAP)?;
            let related_buf_ptr =
                self.write_contract_buf_serialized(&running, related, STREAMING_BUF_CAP)?;

            let result = self.engine.call_3i64_blocking(
                &running.handle,
                "validate_state",
                param_buf_ptr as i64,
                state_buf_ptr as i64,
                related_buf_ptr as i64,
            );
            let result = classify_result(result)?;

            // SAFETY: `result` is the return value from the WASM `validate_state` call and
            // `linear_mem` points to the instance's live linear memory, so `from_raw`
            // reads a valid, in-bounds result descriptor.
            let is_valid = unsafe {
                ContractInterfaceResult::from_raw(result, &linear_mem)
                    .unwrap_validate_state_res(linear_mem)
                    .map_err(Into::<ContractExecError>::into)?
            };
            Ok(is_valid)
        })();

        self.drop_running_instance(&mut running);
        result
    }

    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>> {
        let req_bytes =
            parameters.size() + state.size() + update_data.iter().map(|e| e.size()).sum::<usize>();
        let mut running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let _io_guard = ContractIoGuard::new(running.id);

        let result = (|| -> RuntimeResult<UpdateModification<'static>> {
            let linear_mem = self.linear_mem(&running.handle)?;

            let param_buf_ptr =
                self.write_contract_buf(&running, parameters.as_ref(), STREAMING_BUF_CAP)?;
            let state_buf_ptr =
                self.write_contract_buf(&running, state.as_ref(), STREAMING_BUF_CAP)?;
            let update_data_buf_ptr =
                self.write_contract_buf_serialized(&running, update_data, STREAMING_BUF_CAP)?;

            let result = self.engine.call_3i64_blocking(
                &running.handle,
                "update_state",
                param_buf_ptr as i64,
                state_buf_ptr as i64,
                update_data_buf_ptr as i64,
            );
            let result = classify_result(result)?;

            // SAFETY: `result` is the return value from the WASM `update_state` call and
            // `linear_mem` points to the instance's live linear memory, so `from_raw`
            // reads a valid, in-bounds result descriptor.
            let update_res = unsafe {
                ContractInterfaceResult::from_raw(result, &linear_mem)
                    .unwrap_update_state(linear_mem)
                    .map_err(Into::<ContractExecError>::into)?
            };
            Ok(update_res)
        })();

        self.drop_running_instance(&mut running);
        result
    }

    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>> {
        let req_bytes = parameters.size() + state.size();
        let mut running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let _io_guard = ContractIoGuard::new(running.id);

        let result = (|| -> RuntimeResult<StateSummary<'static>> {
            let linear_mem = self.linear_mem(&running.handle)?;

            let param_buf_ptr =
                self.write_contract_buf(&running, parameters.as_ref(), STREAMING_BUF_CAP)?;
            let state_buf_ptr =
                self.write_contract_buf(&running, state.as_ref(), STREAMING_BUF_CAP)?;

            let result = self.engine.call_2i64_blocking(
                &running.handle,
                "summarize_state",
                param_buf_ptr as i64,
                state_buf_ptr as i64,
            );
            let result = classify_result(result)?;

            // SAFETY: `result` is the return value from the WASM `summarize_state` call and
            // `linear_mem` points to the instance's live linear memory, so `from_raw`
            // reads a valid, in-bounds result descriptor.
            let summary = unsafe {
                ContractInterfaceResult::from_raw(result, &linear_mem)
                    .unwrap_summarize_state(linear_mem)
                    .map_err(Into::<ContractExecError>::into)?
            };
            Ok(summary)
        })();

        self.drop_running_instance(&mut running);
        result
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        summary: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'static>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let mut running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let _io_guard = ContractIoGuard::new(running.id);

        let result = (|| -> RuntimeResult<StateDelta<'static>> {
            let linear_mem = self.linear_mem(&running.handle)?;

            let param_buf_ptr =
                self.write_contract_buf(&running, parameters.as_ref(), STREAMING_BUF_CAP)?;
            let state_buf_ptr =
                self.write_contract_buf(&running, state.as_ref(), STREAMING_BUF_CAP)?;
            let summary_buf_ptr =
                self.write_contract_buf(&running, summary.as_ref(), STREAMING_BUF_CAP)?;

            let result = self.engine.call_3i64_blocking(
                &running.handle,
                "get_state_delta",
                param_buf_ptr as i64,
                state_buf_ptr as i64,
                summary_buf_ptr as i64,
            );
            let result = classify_result(result)?;

            // SAFETY: `result` is the return value from the WASM `get_state_delta` call and
            // `linear_mem` points to the instance's live linear memory, so `from_raw`
            // reads a valid, in-bounds result descriptor.
            let delta = unsafe {
                ContractInterfaceResult::from_raw(result, &linear_mem)
                    .unwrap_get_state_delta(linear_mem)
                    .map_err(Into::<ContractExecError>::into)?
            };
            Ok(delta)
        })();

        self.drop_running_instance(&mut running);
        result
    }
}

/// Convert a WasmError from the engine into the appropriate RuntimeResult error.
fn classify_result(result: Result<i64, WasmError>) -> RuntimeResult<i64> {
    match result {
        Ok(value) => Ok(value),
        Err(WasmError::OutOfGas) => Err(ContractExecError::OutOfGas.into()),
        Err(WasmError::Timeout) => Err(ContractExecError::MaxComputeTimeExceeded.into()),
        Err(e) => Err(e.into()),
    }
}
