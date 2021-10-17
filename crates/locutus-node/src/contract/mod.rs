use blake2::{Blake2b, Digest};
use serde::{Deserialize, Deserializer, Serialize};

use crate::ring::Location;

mod interface;
mod store;
mod test_utils;

pub(crate) use store::{
    ContractHandler, ContractHandlerChannel, ContractHandlerEvent, StoreResponse,
};
pub(crate) use test_utils::MemoryContractHandler;

const CONTRACT_KEY_SIZE: usize = 64;

// FIXME: naively implementing clone here is going to be a perf issue in the future likely
/// Main abstraction for representing a contract in binary form.
/// Potentially expensive to clone.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct Contract {
    data: Vec<u8>,
    #[serde(serialize_with = "<[_]>::serialize")]
    #[serde(deserialize_with = "contract_key_deser")]
    key: [u8; CONTRACT_KEY_SIZE],
}

impl Contract {
    pub fn new(data: Vec<u8>) -> Self {
        let mut hasher = Blake2b::new();
        hasher.update(&data);
        let key_arr = hasher.finalize();
        debug_assert_eq!((&key_arr[..]).len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);

        Self { data, key }
    }

    pub fn key(&self) -> ContractKey {
        ContractKey(self.key)
    }
}

impl PartialEq for Contract {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract {}

/// The key representing a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash, arbitrary::Arbitrary)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct ContractKey(
    #[serde(deserialize_with = "contract_key_deser")]
    #[serde(serialize_with = "<[_]>::serialize")]
    [u8; 64],
);

impl ContractKey {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }

    /// The corresponding location to this contract
    #[inline]
    pub fn location(&self) -> Location {
        Location::from(self)
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = hex::encode(self.0);
        write!(f, "{}", r)
    }
}

impl<'a> arbitrary::Arbitrary<'a> for Contract {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(Contract::new(data))
    }
}

// A bit wasteful but cannot deserialize directly into [u8; 64]
// with current version of serde
fn contract_key_deser<'de, D>(deserializer: D) -> Result<[u8; 64], D::Error>
where
    D: Deserializer<'de>,
{
    let data: Vec<u8> = Deserialize::deserialize(deserializer)?;
    let mut key = [0u8; CONTRACT_KEY_SIZE];
    key.copy_from_slice(&data);
    Ok(key)
}

// FIXME: naively implementing clone here will be a future perf problem, must be improved
/// The value for a contract. Potentially very expensive to clone.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
// #[cfg_attr(test, derive(arbitrary::Arbitrary))]
#[derive(arbitrary::Arbitrary)]
pub(crate) struct ContractValue(Vec<u8>);

impl ContractValue {
    pub fn new(bytes: Vec<u8>) -> Self {
        ContractValue(bytes)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ContractError<SErr: std::error::Error> {
    #[error("failed while storing a contract")]
    StorageError(#[from] SErr),
}
