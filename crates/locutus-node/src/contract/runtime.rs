#![allow(unused)] // FIXME: remove this attr
//! Interface and related utilities for interacted with the compiled WASM contracts.
//! All valid contracts must implement this interface.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.
use super::ContractKey;

pub(super) type ContractUpdateResult<T> = Result<T, ContractUpdateError>;

pub(crate) struct ContractRuntime {}

impl ContractRuntime {
    /// Determine whether this value is valid for this contract
    pub fn validate_value(value: &[u8]) -> bool {
        todo!()
    }

    /// Determine whether this value is a valid update for this contract. If it is, return the modified value,
    /// else return error and the original value.
    ///
    /// The contract must be implemented in a way such that this function call is idempotent:
    /// - If the same `value_update` is applied twice to a value, then the second will be ignored.
    /// - Application of `value_update` is "order invariant", no matter what the order in which the values are
    ///   applied, the resulting value must be exactly the same.
    pub fn update_value(value: Vec<u8>, value_update: &[u8]) -> ContractUpdateResult<Vec<u8>> {
        Ok(value_update.to_vec())
    }

    /// Obtain any other related contracts for this value update. Typically used to ensure
    /// update has been fully propagated.
    pub fn related_contracts(value_update: &[u8]) -> Vec<ContractKey> {
        todo!()
    }

    /// Extract some data from the value and return it.
    /// E.g. `extractor` might contain a byte range, which will be extracted
    /// from the value and returned.
    ///
    /// In case extractor is none, then return the whole value.
    pub fn extract(extractor: Option<&[u8]>, value: &[u8]) -> Vec<u8> {
        todo!()
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ContractUpdateError {
    #[error("invalid put value")]
    InvalidValue(Vec<u8>),
}
