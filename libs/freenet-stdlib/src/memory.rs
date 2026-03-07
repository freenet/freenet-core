//! Internally used functionality to interact between WASM and the host environment.
//! Most of the usage of types is unsafe and requires knowledge on how
//! the WASM runtime is set and used. Use with caution.
//!
//! End users should be using higher levels of abstraction to write contracts
//! and shouldn't need to manipulate functions and types in this module directly.
//! Use with caution.

pub mod buf;

#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct WasmLinearMem {
    pub start_ptr: *const u8,
    pub size: u64,
}

impl WasmLinearMem {
    /// # Safety
    /// Ensure that the passed pointer is a valid pointer to the start of
    /// the WASM linear memory
    pub unsafe fn new(start_ptr: *const u8, size: u64) -> Self {
        Self { start_ptr, size }
    }
}

#[cfg(feature = "contract")]
pub mod wasm_interface {
    use crate::prelude::*;

    fn set_logger() -> Result<(), ContractInterfaceResult> {
        #[cfg(feature = "trace")]
        {
            use crate::prelude::*;
            use tracing_subscriber as tra;
            if let Err(err) = tra::fmt()
                .with_env_filter("warn,freenet_stdlib=trace")
                .try_init()
            {
                return Err(ContractInterfaceResult::from(Err::<ValidateResult, _>(
                    ContractError::Other(format!("{}", err)),
                )));
            }
        }
        Ok(())
    }

    pub fn inner_validate_state<T: ContractInterface>(
        parameters: i64,
        state: i64,
        related: i64,
    ) -> i64 {
        if let Err(e) = set_logger().map_err(|e| e.into_raw()) {
            return e;
        }
        let parameters = unsafe {
            let param_buf = &*(parameters as *const super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.bytes_written());
            Parameters::from(bytes)
        };
        let state = unsafe {
            let state_buf = &*(state as *const super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.bytes_written());
            State::from(bytes)
        };
        let related: RelatedContracts = unsafe {
            let related = &*(related as *const super::buf::BufferBuilder);
            let bytes = &*std::ptr::slice_from_raw_parts(related.start(), related.bytes_written());
            match bincode::deserialize(bytes) {
                Ok(v) => v,
                Err(err) => {
                    return ContractInterfaceResult::from(Err::<::core::primitive::bool, _>(
                        ContractError::Deser(format!("{}", err)),
                    ))
                    .into_raw()
                }
            }
        };
        let result = <T as ContractInterface>::validate_state(parameters, state, related);
        ContractInterfaceResult::from(result).into_raw()
    }

    pub fn inner_update_state<T: ContractInterface>(
        parameters: i64,
        state: i64,
        updates: i64,
    ) -> i64 {
        if let Err(e) = set_logger().map_err(|e| e.into_raw()) {
            return e;
        }
        let parameters = unsafe {
            let param_buf = &mut *(parameters as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.bytes_written());
            Parameters::from(bytes)
        };
        let state = unsafe {
            let state_buf = &mut *(state as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.bytes_written());
            State::from(bytes)
        };
        let updates = unsafe {
            let updates = &mut *(updates as *mut super::buf::BufferBuilder);
            let bytes = &*std::ptr::slice_from_raw_parts(updates.start(), updates.bytes_written());
            match bincode::deserialize(bytes) {
                Ok(v) => v,
                Err(err) => {
                    return ContractInterfaceResult::from(Err::<ValidateResult, _>(
                        ContractError::Deser(format!("{}", err)),
                    ))
                    .into_raw()
                }
            }
        };
        let result = <T as ContractInterface>::update_state(parameters, state, updates);
        ContractInterfaceResult::from(result).into_raw()
    }

    pub fn inner_summarize_state<T: ContractInterface>(parameters: i64, state: i64) -> i64 {
        if let Err(e) = set_logger().map_err(|e| e.into_raw()) {
            return e;
        }
        let parameters = unsafe {
            let param_buf = &mut *(parameters as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.bytes_written());
            Parameters::from(bytes)
        };
        let state = unsafe {
            let state_buf = &mut *(state as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.bytes_written());
            State::from(bytes)
        };
        let summary = <T as ContractInterface>::summarize_state(parameters, state);
        ContractInterfaceResult::from(summary).into_raw()
    }

    pub fn inner_get_state_delta<T: ContractInterface>(
        parameters: i64,
        state: i64,
        summary: i64,
    ) -> i64 {
        if let Err(e) = set_logger().map_err(|e| e.into_raw()) {
            return e;
        }
        let parameters = unsafe {
            let param_buf = &mut *(parameters as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(param_buf.start(), param_buf.bytes_written());
            Parameters::from(bytes)
        };
        let state = unsafe {
            let state_buf = &mut *(state as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(state_buf.start(), state_buf.bytes_written());
            State::from(bytes)
        };
        let summary = unsafe {
            let summary_buf = &mut *(summary as *mut super::buf::BufferBuilder);
            let bytes =
                &*std::ptr::slice_from_raw_parts(summary_buf.start(), summary_buf.bytes_written());
            StateSummary::from(bytes)
        };
        let new_delta = <T as ContractInterface>::get_state_delta(parameters, state, summary);
        ContractInterfaceResult::from(new_delta).into_raw()
    }
}
