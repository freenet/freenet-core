use std::{fmt::Display, ops::Deref};

use blake3::{traits::digest::Digest, Hasher as Blake3};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

const CONTRACT_KEY_SIZE: usize = 32;

#[serde_as]
#[derive(PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct CodeHash(#[serde_as(as = "[_; CONTRACT_KEY_SIZE]")] pub(crate) [u8; CONTRACT_KEY_SIZE]);

impl CodeHash {
    pub const fn new(value: [u8; CONTRACT_KEY_SIZE]) -> Self {
        Self(value)
    }

    pub fn from_code(wasm_code: &[u8]) -> Self {
        let mut hasher = Blake3::new();
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

impl Deref for CodeHash {
    type Target = [u8; CONTRACT_KEY_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for CodeHash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl From<&[u8; CONTRACT_KEY_SIZE]> for CodeHash {
    fn from(value: &[u8; CONTRACT_KEY_SIZE]) -> Self {
        Self(*value)
    }
}

impl TryFrom<&[u8]> for CodeHash {
    type Error = std::io::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != CONTRACT_KEY_SIZE {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        let mut this = [0u8; CONTRACT_KEY_SIZE];
        this.copy_from_slice(value);
        Ok(Self(this))
    }
}

impl Display for CodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

impl std::fmt::Debug for CodeHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CodeHash").field(&self.encode()).finish()
    }
}
