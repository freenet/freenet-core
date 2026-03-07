//! Contract executable code representation.
//!
//! This module provides the `ContractCode` type which represents the executable
//! WASM bytecode for a contract, along with its hash.

use std::borrow::Cow;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use blake3::{traits::digest::Digest, Hasher as Blake3};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

use crate::code_hash::CodeHash;

use super::key::internal_fmt_key;
use super::CONTRACT_KEY_SIZE;

/// The executable contract.
///
/// It is the part of the executable belonging to the full specification
/// and does not include any other metadata (like the parameters).
#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct ContractCode<'a> {
    // TODO: conver this to Arc<[u8]> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    pub(crate) data: Cow<'a, [u8]>,
    // todo: skip serializing and instead compute it
    pub(crate) code_hash: CodeHash,
}

impl ContractCode<'static> {
    /// Loads the contract raw wasm module, without any version.
    pub fn load_raw(path: &Path) -> Result<Self, std::io::Error> {
        let contract_data = Self::load_bytes(path)?;
        Ok(ContractCode::from(contract_data))
    }

    pub(crate) fn load_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(contract_data)
    }
}

impl ContractCode<'_> {
    /// Contract code hash.
    pub fn hash(&self) -> &CodeHash {
        &self.code_hash
    }

    /// Returns the `Base58` string representation of the contract key.
    pub fn hash_str(&self) -> String {
        Self::encode_hash(&self.code_hash.0)
    }

    /// Reference to contract code.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Extracts the owned contract code data as a `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data.to_vec()
    }

    /// Returns the `Base58` string representation of a hash.
    pub fn encode_hash(hash: &[u8; CONTRACT_KEY_SIZE]) -> String {
        bs58::encode(hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> ContractCode<'static> {
        ContractCode {
            data: self.data.into_owned().into(),
            code_hash: self.code_hash,
        }
    }

    fn gen_hash(data: &[u8]) -> CodeHash {
        let mut hasher = Blake3::new();
        hasher.update(data);
        let key_arr = hasher.finalize();
        debug_assert_eq!(key_arr[..].len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);
        CodeHash(key)
    }
}

impl From<Vec<u8>> for ContractCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = ContractCode::gen_hash(&data);
        ContractCode {
            data: Cow::from(data),
            code_hash: key,
        }
    }
}

impl<'a> From<&'a [u8]> for ContractCode<'a> {
    fn from(data: &'a [u8]) -> ContractCode<'a> {
        let hash = ContractCode::gen_hash(data);
        ContractCode {
            data: Cow::from(data),
            code_hash: hash,
        }
    }
}

impl PartialEq for ContractCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.code_hash == other.code_hash
    }
}

impl Eq for ContractCode<'_> {}

impl std::fmt::Display for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract( key: ")?;
        internal_fmt_key(&self.code_hash.0, f)?;
        let data: String = if self.data.len() > 8 {
            self.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{data}])")
    }
}

impl std::fmt::Debug for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContractCode")
            .field("hash", &self.code_hash);
        Ok(())
    }
}
