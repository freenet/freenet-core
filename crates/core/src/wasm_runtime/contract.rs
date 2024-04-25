use freenet_stdlib::prelude::{
    ContractInterfaceResult, ContractKey, Parameters, RelatedContracts, StateDelta, StateSummary,
    UpdateData, UpdateModification, ValidateResult, WrappedState,
};
use wasmer::TypedFunction;

use super::{ContractExecError, RuntimeResult};

type FfiReturnTy = i64;

pub(crate) trait ContractRuntimeInterface {
    /// Verify that the state is valid, given the parameters. This will be used before a peer
    /// caches a new state.
    fn validate_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        related: &RelatedContracts<'_>,
    ) -> RuntimeResult<ValidateResult>;

    /// Verify that a delta is valid - at least as much as possible. The goal is to prevent DDoS of
    /// a contract by sending a large number of invalid delta updates. This allows peers
    /// to verify a delta before forwarding it.
    fn validate_delta(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        delta: &StateDelta<'_>,
    ) -> RuntimeResult<bool>;

    /// Determine whether this delta is a valid update for this contract. If it is, return the modified state,
    /// else return error.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `update_state` is applied twice to a value, then the second will be ignored.
    /// - Application of `update_state` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
        update_data: &[UpdateData<'_>],
    ) -> RuntimeResult<UpdateModification<'static>>;

    /// Generate a concise summary of a state that can be used to create deltas relative to this state.
    ///
    /// This allows flexible and efficient state synchronization between peers.
    fn summarize_state(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'_>,
        state: &WrappedState,
    ) -> RuntimeResult<StateSummary<'static>>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
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
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state)?;
            state_buf.ptr()
        };
        let related_buf_ptr = {
            let serialized = bincode::serialize(related)?;
            let mut related_buf = self.init_buf(&running.instance, &serialized)?;
            related_buf.write(serialized)?;
            related_buf.ptr()
        };

        let validate_func: TypedFunction<(i64, i64, i64), FfiReturnTy> =
            running
                .instance
                .exports
                .get_typed_function(&self.wasm_store, "validate_state")?;
        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(
                    &mut self.wasm_store,
                    param_buf_ptr as i64,
                    state_buf_ptr as i64,
                    related_buf_ptr as i64,
                )?,
                &linear_mem,
            )
            .unwrap_validate_state_res(linear_mem)
            .map_err(Into::<ContractExecError>::into)?
        };
        Ok(is_valid)
    }

    fn validate_delta<'a>(
        &mut self,
        key: &ContractKey,
        parameters: &Parameters<'a>,
        delta: &StateDelta<'a>,
    ) -> RuntimeResult<bool> {
        // todo: if we keep this hot in memory on next calls overwrite the buffer with new delta
        let req_bytes = parameters.size() + delta.size();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let delta_buf_ptr = {
            let mut delta_buf = self.init_buf(&running.instance, delta)?;
            delta_buf.write(delta)?;
            delta_buf.ptr()
        };

        let validate_func: TypedFunction<(i64, i64), FfiReturnTy> = running
            .instance
            .exports
            .get_typed_function(&self.wasm_store, "validate_delta")?;
        let is_valid = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(
                    &mut self.wasm_store,
                    param_buf_ptr as i64,
                    delta_buf_ptr as i64,
                )?,
                &linear_mem,
            )
            .unwrap_validate_delta_res(linear_mem)
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
        // todo: if we keep this hot in memory some things to take into account:
        //       - over subsequent requests state size may change
        //       - the delta may not be necessarily the same size
        let req_bytes =
            parameters.size() + state.size() + update_data.iter().map(|e| e.size()).sum::<usize>();
        let running = self.prepare_contract_call(key, parameters, req_bytes)?;
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let update_data_buf_ptr = {
            let serialized = bincode::serialize(update_data)?;
            let mut update_data_buf = self.init_buf(&running.instance, &serialized)?;
            update_data_buf.write(serialized)?;
            update_data_buf.ptr()
        };

        let validate_func: TypedFunction<(i64, i64, i64), FfiReturnTy> =
            running
                .instance
                .exports
                .get_typed_function(&self.wasm_store, "update_state")?;
        let update_res = unsafe {
            ContractInterfaceResult::from_raw(
                validate_func.call(
                    &mut self.wasm_store,
                    param_buf_ptr as i64,
                    state_buf_ptr as i64,
                    update_data_buf_ptr as i64,
                )?,
                &linear_mem,
            )
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
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };

        let summary_func: TypedFunction<(i64, i64), FfiReturnTy> = running
            .instance
            .exports
            .get_typed_function(&self.wasm_store, "summarize_state")?;

        let result = unsafe {
            let int_res = ContractInterfaceResult::from_raw(
                summary_func.call(
                    &mut self.wasm_store,
                    param_buf_ptr as i64,
                    state_buf_ptr as i64,
                )?,
                &linear_mem,
            );
            int_res
                .unwrap_summarize_state(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(result)
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
        let linear_mem = self.linear_mem(&running.instance)?;

        let param_buf_ptr = {
            let mut param_buf = self.init_buf(&running.instance, parameters)?;
            param_buf.write(parameters)?;
            param_buf.ptr()
        };
        let state_buf_ptr = {
            let mut state_buf = self.init_buf(&running.instance, state)?;
            state_buf.write(state.clone())?;
            state_buf.ptr()
        };
        let summary_buf_ptr = {
            let mut summary_buf = self.init_buf(&running.instance, summary)?;
            summary_buf.write(summary)?;
            summary_buf.ptr()
        };

        let get_state_delta_func: TypedFunction<(i64, i64, i64), FfiReturnTy> = running
            .instance
            .exports
            .get_typed_function(&self.wasm_store, "get_state_delta")?;

        let result = unsafe {
            let int_res = {
                ContractInterfaceResult::from_raw(
                    get_state_delta_func.call(
                        &mut self.wasm_store,
                        param_buf_ptr as i64,
                        state_buf_ptr as i64,
                        summary_buf_ptr as i64,
                    )?,
                    &linear_mem,
                )
            };
            int_res
                .unwrap_get_state_delta(linear_mem)
                .map_err(Into::<ContractExecError>::into)?
        };
        Ok(result)
    }
}
