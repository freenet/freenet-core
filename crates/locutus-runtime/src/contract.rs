use std::{ops::Deref, path::PathBuf, sync::Arc};

use blake2::{Blake2b512, Digest};
use serde::{Deserialize, Deserializer, Serialize};

use crate::ContractRuntimeError;

const CONTRACT_KEY_SIZE: usize = 64;

/// Main abstraction for representing a contract in binary form.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    data: Arc<Vec<u8>>,
    #[serde(serialize_with = "<[_]>::serialize")]
    #[serde(deserialize_with = "contract_key_deser")]
    key: [u8; CONTRACT_KEY_SIZE],
}

impl Contract {
    pub fn new(data: Vec<u8>) -> Self {
        let mut hasher = Blake2b512::new();
        hasher.update(&data);
        let key_arr = hasher.finalize();
        debug_assert_eq!((&key_arr[..]).len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);

        Self {
            data: Arc::new(data),
            key,
        }
    }

    pub fn key(&self) -> ContractKey {
        ContractKey(self.key)
    }

    pub fn data(&self) -> &[u8] {
        &*self.data
    }
}

impl TryInto<Vec<u8>> for Contract {
    type Error = ContractRuntimeError;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data).map_err(|_| ContractRuntimeError::UnwrapContract)
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
pub struct ContractKey(
    #[serde(deserialize_with = "contract_key_deser")]
    #[serde(serialize_with = "<[_]>::serialize")]
    [u8; 64],
);

impl ContractKey {
    pub fn bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<ContractKey> for PathBuf {
    fn from(val: ContractKey) -> Self {
        let r = hex::encode(val.0);
        PathBuf::from(r)
    }
}

impl Deref for ContractKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let r = hex::encode(self.0);
        write!(f, "{}", &r[..8])
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

/// The value for a contract.
#[derive(
    Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
pub struct ContractValue(Arc<Vec<u8>>);

impl ContractValue {
    pub fn new(bytes: Vec<u8>) -> Self {
        ContractValue(Arc::new(bytes))
    }
}

impl Deref for ContractValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}
