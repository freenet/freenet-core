use super::engine::{WasmEngine, WasmError};
use super::{ContractExecError, RuntimeResult};
use freenet_stdlib::prelude::{
    ContractInterfaceResult, ContractKey, Parameters, RelatedContracts, StateDelta, StateSummary,
    UpdateData, UpdateModification, ValidateResult, WrappedState,
};

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

impl ContractRuntimeInterface for super::Runtime {
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult> {
        let req_bytes = parameters.size() + state.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.handle)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.handle, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.handle, state)?;
            state_buf.write(state)?;
            state_buf.ptr()
        };
        let related_buf_ptr = {
            let serialized = bincode::serialize(related)?;
            let mut related_buf = self.init_buf(&running.handle, &serialized)?;
            related_buf.write(serialized)?;
            related_buf.ptr()
        };

        let result = self.engine.call_3i64_blocking(
            &running.handle,
            "validate_state",
            param_buf_ptr as i64,
            state_buf_ptr as i64,
            related_buf_ptr as i64,
        );
        let result = classify_result(result)?;

        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_validate_state_res(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(is_valid)
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
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.handle)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.handle, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.handle, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let update_data_buf_ptr = {
            let serialized = bincode::serialize(update_data)?;
            let mut update_data_buf = self.init_buf(&running.handle, &serialized)?;
            update_data_buf.write(serialized)?;
            update_data_buf.ptr()
        };

        let result = self.engine.call_3i64_blocking(
            &running.handle,
            "update_state",
            param_buf_ptr as i64,
            state_buf_ptr as i64,
            update_data_buf_ptr as i64,
        );
        let result = classify_result(result)?;

        let update_res = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_update_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(update_res)
    }

    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>> {
        let req_bytes = parameters.size() + state.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.handle)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.handle, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.handle, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };

        let result = self.engine.call_2i64_blocking(
            &running.handle,
            "summarize_state",
            param_buf_ptr as i64,
            state_buf_ptr as i64,
        );
        let result = classify_result(result)?;

        let summary = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_summarize_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(summary)
    }

    fn get_state_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        state: &WrappedState,
        summary: &StateSummary<'a>,
    ) -> RuntimeResult<StateDelta<'static>> {
        let req_bytes = parameters.size() + state.size() + summary.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.handle)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.handle, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.handle, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let summary_buf_ptr = {
            let mut summary_buf = self.init_buf(&running.handle, summary)?;
            summary_buf.write(summary)?;
            summary_buf.ptr()
        };

        let result = self.engine.call_3i64_blocking(
            &running.handle,
            "get_state_delta",
            param_buf_ptr as i64,
            state_buf_ptr as i64,
            summary_buf_ptr as i64,
        );
        let result = classify_result(result)?;

        let delta = unsafe {
            ContractInterfaceResult::from_raw(result, &linear_mem)
                .unwrap_get_state_delta(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };

        Ok(delta)
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
