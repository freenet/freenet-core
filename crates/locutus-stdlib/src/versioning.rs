use std::fmt;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt};
use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::prelude::WrappedContract;
use crate::{
    contract_interface::ContractKey,
    prelude::{ContractCode, Parameters, TryFromTsStd, WsApiError},
};

/// Wrapper that allows contract versioning. This enum maintains the types of contracts that are
/// allowed and their corresponding version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContractContainer {
    Wasm(WasmAPIVersion),
}

impl ContractContainer {
    /// Return the `ContractContainer` content from the specified path as as `Vec<u8>`.
    pub(crate) fn get_contract_data_from_fs(path: &Path) -> Result<Vec<u8>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(contract_data)
    }

    /// Return the `ContractKey` from the specific contract version.
    pub fn key(&self) -> ContractKey {
        match self {
            Self::Wasm(WasmAPIVersion::V1(contract_v1)) => contract_v1.key().clone(),
        }
    }

    /// Return the `Parameters` from the specific contract version.
    pub fn params(&self) -> Parameters<'static> {
        match self {
            Self::Wasm(WasmAPIVersion::V1(contract_v1)) => contract_v1.params().clone(),
        }
    }

    /// Return the contract code from the specific contract version as `Vec<u8>`.
    pub fn data(&self) -> Vec<u8> {
        match self {
            Self::Wasm(WasmAPIVersion::V1(contract_v1)) => contract_v1.clone().try_into().unwrap(),
        }
    }
}

impl Display for ContractContainer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ContractContainer::Wasm(wasm_version) => {
                write!(f, "Wasm container {wasm_version}")
            }
        }
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for ContractContainer {
    type Error = std::io::Error;

    fn try_from((path, params): (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let mut contract_data =
            Cursor::new(ContractContainer::get_contract_data_from_fs(path).unwrap());

        // Get contract version
        let version_size = contract_data
            .read_u32::<BigEndian>()
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        let mut version_data = vec![0; version_size as usize];
        contract_data
            .read_exact(&mut version_data)
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        let version: Version = serde_json::from_slice(version_data.as_slice())
            .map_err(|_| std::io::ErrorKind::InvalidData)?;

        // Get Contract code
        let mut code_data: Vec<u8> = vec![];
        contract_data
            .read_to_end(&mut code_data)
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        let contract_code = Arc::new(ContractCode::from(code_data));

        match version.to_string().as_str() {
            "0.0.1" => Ok(ContractContainer::Wasm(WasmAPIVersion::V1(
                WrappedContract::new(contract_code, params),
            ))),
            _ => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }
}

impl TryFromTsStd<&rmpv::Value> for ContractContainer {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
        let container_map: HashMap<&str, &rmpv::Value> = match value.as_map() {
            Some(map_value) => HashMap::from_iter(
                map_value
                    .iter()
                    .map(|(key, val)| (key.as_str().unwrap(), val)),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding ContractContainer, input value is not a map"
                        .to_string(),
                })
            }
        };

        let container_version = match container_map.get("version") {
            Some(version_value) => (*version_value).as_str().unwrap(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding ContractContainer, version not found".to_string(),
                })
            }
        };

        match container_version {
            "V1" => {
                let contract = WrappedContract::try_decode(value).map_err(|e| {
                    WsApiError::MsgpackDecodeError {
                        cause: format!("{e}"),
                    }
                })?;
                Ok(ContractContainer::Wasm(WasmAPIVersion::V1(contract)))
            }
            _ => unreachable!(),
        }
    }
}

/// Contains the different versions available for wasm contracts.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum WasmAPIVersion {
    V1(WrappedContract),
}

impl Display for WasmAPIVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            WasmAPIVersion::V1(contract_v1) => {
                write!(f, "version 0.0.1 of contract {contract_v1}")
            }
        }
    }
}

impl From<ContractContainer> for Version {
    fn from(contract: ContractContainer) -> Version {
        match contract {
            ContractContainer::Wasm(WasmAPIVersion::V1(_)) => Version::new(0, 0, 1),
        }
    }
}
