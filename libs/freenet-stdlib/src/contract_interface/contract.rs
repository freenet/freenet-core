//! Complete contract specification combining code and parameters.
//!
//! This module provides the `Contract` type which represents a complete
//! contract with both executable code and runtime parameters.

use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Serialize};

use crate::parameters::Parameters;

use super::code::ContractCode;
use super::key::{internal_fmt_key, ContractKey};

/// A complete contract specification requires a `parameters` section
/// and a `contract` section.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct Contract<'a> {
    #[serde(borrow)]
    pub parameters: Parameters<'a>,
    #[serde(borrow)]
    pub data: ContractCode<'a>,
    // todo: skip serializing and instead compute it
    key: ContractKey,
}

impl<'a> Contract<'a> {
    /// Returns a contract from [contract code](ContractCode) and given [parameters](Parameters).
    pub fn new(contract: ContractCode<'a>, parameters: Parameters<'a>) -> Contract<'a> {
        let key = ContractKey::from_params_and_code(&parameters, &contract);
        Contract {
            parameters,
            data: contract,
            key,
        }
    }

    /// Key portion of the specification.
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    /// Code portion of the specification.
    pub fn into_code(self) -> ContractCode<'a> {
        self.data
    }
}

impl TryFrom<Vec<u8>> for Contract<'static> {
    type Error = std::io::Error;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        let mut reader = Cursor::new(data);

        let params_len = reader.read_u64::<LittleEndian>()?;
        let mut params_buf = vec![0; params_len as usize];
        reader.read_exact(&mut params_buf)?;
        let parameters = Parameters::from(params_buf);

        let contract_len = reader.read_u64::<LittleEndian>()?;
        let mut contract_buf = vec![0; contract_len as usize];
        reader.read_exact(&mut contract_buf)?;
        let contract = ContractCode::from(contract_buf);

        let key = ContractKey::from_params_and_code(&parameters, &contract);

        Ok(Contract {
            parameters,
            data: contract,
            key,
        })
    }
}

impl PartialEq for Contract<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract<'_> {}

impl std::fmt::Display for Contract<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ContractSpec( key: ")?;
        internal_fmt_key(&self.key, f)?;
        let data: String = if self.data.data.len() > 8 {
            self.data.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{data}])")
    }
}
