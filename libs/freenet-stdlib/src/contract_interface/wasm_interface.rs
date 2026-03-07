//! Contains all the types to interface between the host environment and
//! the wasm module execution.
use super::*;
use crate::memory::WasmLinearMem;

#[repr(i32)]
enum ResultKind {
    ValidateState = 0,
    ValidateDelta = 1,
    UpdateState = 2,
    SummarizeState = 3,
    StateDelta = 4,
}

impl From<i32> for ResultKind {
    fn from(v: i32) -> Self {
        match v {
            0 => ResultKind::ValidateState,
            1 => ResultKind::ValidateDelta,
            2 => ResultKind::UpdateState,
            3 => ResultKind::SummarizeState,
            4 => ResultKind::StateDelta,
            _ => panic!(),
        }
    }
}

#[doc(hidden)]
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ContractInterfaceResult {
    ptr: i64,
    kind: i32,
    size: u32,
}

impl ContractInterfaceResult {
    /// Deserialize a validate state result from WASM memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `mem` is a valid WASM linear memory containing
    /// the serialized result at the offset specified by `self.ptr`, with at least
    /// `self.size` bytes available.
    pub unsafe fn unwrap_validate_state_res(
        self,
        mem: WasmLinearMem,
    ) -> Result<ValidateResult, ContractError> {
        #![allow(clippy::let_and_return)]
        let kind = ResultKind::from(self.kind);
        match kind {
            ResultKind::ValidateState => {
                let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
                let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                let value = bincode::deserialize(serialized)
                    .map_err(|e| ContractError::Other(format!("{e}")))?;
                #[cfg(feature = "trace")]
                self.log_input(serialized, &value, ptr);
                value
            }
            _ => unreachable!(),
        }
    }

    /// Deserialize an update state result from WASM memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `mem` is a valid WASM linear memory containing
    /// the serialized result at the offset specified by `self.ptr`, with at least
    /// `self.size` bytes available.
    pub unsafe fn unwrap_update_state(
        self,
        mem: WasmLinearMem,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let kind = ResultKind::from(self.kind);
        match kind {
            ResultKind::UpdateState => {
                let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
                let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                let value: Result<UpdateModification<'_>, ContractError> =
                    bincode::deserialize(serialized)
                        .map_err(|e| ContractError::Other(format!("{e}")))?;
                #[cfg(feature = "trace")]
                self.log_input(serialized, &value, ptr);
                // TODO: it may be possible to not own this value while deserializing
                //       under certain circumstances (e.g. when the cotnract mem is kept alive)
                value.map(|r| r.into_owned())
            }
            _ => unreachable!(),
        }
    }

    /// Deserialize a summarize state result from WASM memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `mem` is a valid WASM linear memory containing
    /// the serialized result at the offset specified by `self.ptr`, with at least
    /// `self.size` bytes available.
    pub unsafe fn unwrap_summarize_state(
        self,
        mem: WasmLinearMem,
    ) -> Result<StateSummary<'static>, ContractError> {
        let kind = ResultKind::from(self.kind);
        match kind {
            ResultKind::SummarizeState => {
                let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
                let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                let value: Result<StateSummary<'static>, ContractError> =
                    bincode::deserialize(serialized)
                        .map_err(|e| ContractError::Other(format!("{e}")))?;
                #[cfg(feature = "trace")]
                self.log_input(serialized, &value, ptr);
                // TODO: it may be possible to not own this value while deserializing
                //       under certain circumstances (e.g. when the contract mem is kept alive)
                value.map(|s| StateSummary::from(s.into_bytes()))
            }
            _ => unreachable!(),
        }
    }

    /// Deserialize a state delta result from WASM memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `mem` is a valid WASM linear memory containing
    /// the serialized result at the offset specified by `self.ptr`, with at least
    /// `self.size` bytes available.
    pub unsafe fn unwrap_get_state_delta(
        self,
        mem: WasmLinearMem,
    ) -> Result<StateDelta<'static>, ContractError> {
        let kind = ResultKind::from(self.kind);
        match kind {
            ResultKind::StateDelta => {
                let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
                let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                let value: Result<StateDelta<'static>, ContractError> =
                    bincode::deserialize(serialized)
                        .map_err(|e| ContractError::Other(format!("{e}")))?;
                #[cfg(feature = "trace")]
                self.log_input(serialized, &value, ptr);
                // TODO: it may be possible to not own this value while deserializing
                //       under certain circumstances (e.g. when the contract mem is kept alive)
                value.map(|d| StateDelta::from(d.into_bytes()))
            }
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "contract")]
    pub fn into_raw(self) -> i64 {
        #[cfg(feature = "trace")]
        {
            tracing::trace!("returning FFI -> {self:?}");
        }
        let ptr = Box::into_raw(Box::new(self));
        #[cfg(feature = "trace")]
        {
            tracing::trace!("FFI result ptr: {ptr:p} ({}i64)", ptr as i64);
        }
        ptr as _
    }

    /// Reconstruct a `ContractInterfaceResult` from a raw pointer in WASM memory.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` points to a valid `ContractInterfaceResult` within the WASM linear memory
    /// - `mem` is a valid WASM linear memory reference
    /// - The pointer was previously created by `into_raw`
    pub unsafe fn from_raw(ptr: i64, mem: &WasmLinearMem) -> Self {
        let result = Box::leak(Box::from_raw(crate::memory::buf::compute_ptr(
            ptr as *mut Self,
            mem,
        )));
        #[cfg(feature = "trace")]
        {
            tracing::trace!(
                "got FFI result @ {ptr} ({:p}) -> {result:?}",
                ptr as *mut Self
            );
        }
        *result
    }

    #[cfg(feature = "trace")]
    fn log_input<T: std::fmt::Debug>(&self, serialized: &[u8], value: &T, ptr: *mut u8) {
        tracing::trace!(
            "got result through FFI; addr: {:p} ({}i64, mapped: {ptr:p})
             serialized: {serialized:?}
             value: {value:?}",
            self.ptr as *mut u8,
            self.ptr
        );
    }
}

#[cfg(feature = "contract")]
macro_rules! conversion {
    ($value:ty: $kind:expr) => {
        impl From<$value> for ContractInterfaceResult {
            fn from(value: $value) -> Self {
                let kind = $kind as i32;
                // TODO: research if there is a safe way to just transmute the pointer in memory
                //       independently of the architecture when stored in WASM and accessed from
                //       the host, maybe even if is just for some architectures
                let serialized = bincode::serialize(&value).unwrap();
                let size = serialized.len() as _;
                let ptr = serialized.as_ptr();
                #[cfg(feature = "trace")] {
                    tracing::trace!(
                        "sending result through FFI; addr: {ptr:p} ({}),\n  serialized: {serialized:?}\n  value: {value:?}",
                        ptr as i64
                    );
                }
                std::mem::forget(serialized);
                Self { kind, ptr: ptr as i64, size }
            }
        }
    };
}

#[cfg(feature = "contract")]
conversion!(Result<ValidateResult, ContractError>: ResultKind::ValidateState);
#[cfg(feature = "contract")]
conversion!(Result<bool, ContractError>: ResultKind::ValidateDelta);
#[cfg(feature = "contract")]
conversion!(Result<UpdateModification<'static>, ContractError>: ResultKind::UpdateState);
#[cfg(feature = "contract")]
conversion!(Result<StateSummary<'static>, ContractError>: ResultKind::SummarizeState);
#[cfg(feature = "contract")]
conversion!(Result<StateDelta<'static>, ContractError>: ResultKind::StateDelta);
