//! Main abstraction for representing a contract in binary form.

#[cfg(test)]
use arbitrary::Arbitrary;
use blake2::{Blake2b, Digest};
use serde::{Deserialize, Deserializer, Serialize};

use crate::ring::Location;

const CONTRACT_KEY_SIZE: usize = 64;

#[derive(Debug, Serialize, Deserialize)]
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

    fn assigned_location(&self) -> Location {
        Location::from(self.key())
    }
}


/// The key representing a contract.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
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

#[cfg(test)]
impl<'a> Arbitrary<'a> for Contract {
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::random_bytes_1024;

    #[test]
    fn serialization() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = random_bytes_1024();
        let mut gen = arbitrary::Unstructured::new(&bytes);

        let contract: Contract = gen.arbitrary().map_err(|_| "failed gen arb data")?;

        let serialized = bincode::serialize(&contract)?;
        let deser: Contract = bincode::deserialize(&serialized)?;
        assert_eq!(deser.data, contract.data);
        assert_eq!(deser.key, contract.key);
        Ok(())
    }
}
