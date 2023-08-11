use std::fmt::Display;

use blake2::{Blake2s256, Digest};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

const CONTRACT_KEY_SIZE: usize = 32;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize, Hash)]
#[cfg_attr(
    all(any(test, feature = "testing"), target_family = "unix"),
    derive(arbitrary::Arbitrary)
)]
pub struct CodeHash(#[serde_as(as = "[_; CONTRACT_KEY_SIZE]")] pub(crate) [u8; CONTRACT_KEY_SIZE]);

impl CodeHash {
    pub fn new(wasm_code: &[u8]) -> Self {
        let mut hasher = Blake2s256::new();
        hasher.update(wasm_code);
        let hashed = hasher.finalize();
        let mut delegate_key = [0; CONTRACT_KEY_SIZE];
        delegate_key.copy_from_slice(&hashed);
        Self(delegate_key)
    }

    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
            .to_lowercase()
    }
}

impl From<&[u8; CONTRACT_KEY_SIZE]> for CodeHash {
    fn from(value: &[u8; CONTRACT_KEY_SIZE]) -> Self {
        Self(*value)
    }
}

impl Display for CodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}
