//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use crate::{contract::ContractKey, RuntimeResult};

pub trait RuntimeInterface {
    /// Determine whether this value is valid for this contract
    fn validate_value(&mut self, key: &ContractKey, value: &[u8]) -> RuntimeResult<bool>;

    /// Determine whether this value is a valid update for this contract. If it is, return the modified value,
    /// else return error and the original value.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `value_update` is applied twice to a value, then the second will be ignored.
    /// - Application of `value_update` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_value(
        &mut self,
        key: &ContractKey,
        value: &[u8],
        value_update: &[u8],
    ) -> RuntimeResult<Vec<u8>>;

    /// Obtain any other related contracts for this value update. Typically used to ensure
    /// update has been fully propagated.
    fn related_contracts(
        &mut self,
        key: &ContractKey,
        value_update: &[u8],
    ) -> RuntimeResult<Vec<ContractKey>>;

    /// Extract some data from the value and return it.
    /// E.g. `extractor` might contain a byte range, which will be extracted
    /// from the value and returned.
    ///
    /// In case extractor is none, then return the whole value.
    fn extract(
        &mut self,
        key: &ContractKey,
        extractor: Option<&[u8]>,
        value: &[u8],
    ) -> RuntimeResult<Vec<u8>>;
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("invalid put value")]
    InvalidPutValue(Vec<u8>),

    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),
}
