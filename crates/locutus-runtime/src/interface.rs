//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use crate::contract::ContractKey;

pub trait RuntimeInterface {
    /// Determine whether this value is valid for this contract
    fn validate_value(&self, key: &ContractKey, value: &[u8]) -> bool;

    /// Determine whether this value is a valid update for this contract. If it is, return the modified value,
    /// else return error and the original value.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `value_update` is applied twice to a value, then the second will be ignored.
    /// - Application of `value_update` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    fn update_value(
        &self,
        key: &ContractKey,
        value: &[u8],
        value_update: &[u8],
    ) -> Result<Vec<u8>, ContractUpdateError>;

    /// Obtain any other related contracts for this value update. Typically used to ensure
    /// update has been fully propagated.
    fn related_contracts(&self, key: &ContractKey, value_update: &[u8]) -> Vec<ContractKey>;

    /// Extract some data from the value and return it.
    /// E.g. `extractor` might contain a byte range, which will be extracted
    /// from the value and returned.
    ///
    /// In case extractor is none, then return the whole value.
    fn extract(&self, key: &ContractKey, extractor: Option<&[u8]>, value: &[u8]) -> Vec<u8>;
}

#[derive(thiserror::Error, Debug)]
pub enum ContractUpdateError {
    #[error("invalid put value")]
    InvalidValue(Vec<u8>),
}
