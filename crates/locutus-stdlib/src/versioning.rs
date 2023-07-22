use std::fmt;
use std::fmt::{Display, Formatter};
use std::path::Path;
use std::sync::Arc;

use semver::Version;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::client_api::{TryFromTsStd, WsApiError};
use crate::parameters::Parameters;
use crate::prelude::WrappedContract;
use crate::{contract_interface::ContractKey, prelude::ContractCode};

/// Wrapper that allows contract versioning. This enum maintains the types of contracts that are
/// allowed and their corresponding version.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContractContainer {
    Wasm(WasmAPIVersion),
}

impl ContractContainer {
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

    pub fn unwrap_v1(self) -> WrappedContract {
        match self {
            Self::Wasm(WasmAPIVersion::V1(contract_v1)) => contract_v1,
        }
    }
}

impl Display for ContractContainer {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ContractContainer::Wasm(wasm_version) => {
                write!(f, "wasm container {wasm_version}")
            }
        }
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for ContractContainer {
    type Error = std::io::Error;

    fn try_from((path, params): (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

        let (contract_code, version) = ContractCode::load_versioned_from_path(path)?;

        match version {
            version if version == VERSION_0_0_1 => Ok(ContractContainer::Wasm(WasmAPIVersion::V1(
                WrappedContract::new(Arc::new(contract_code), params),
            ))),
            _ => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }
}

impl<'a, P> TryFrom<(Vec<u8>, P)> for ContractContainer
where
    P: std::ops::Deref<Target = Parameters<'a>>,
{
    type Error = std::io::Error;

    fn try_from((versioned_contract_bytes, params): (Vec<u8>, P)) -> Result<Self, Self::Error> {
        let params = params.deref().clone().into_owned();
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

        let (contract_code, version) =
            ContractCode::load_versioned_from_bytes(versioned_contract_bytes)?;

        match version {
            version if version == VERSION_0_0_1 => Ok(ContractContainer::Wasm(WasmAPIVersion::V1(
                WrappedContract::new(Arc::new(contract_code), params),
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

impl From<WasmAPIVersion> for ContractContainer {
    fn from(value: WasmAPIVersion) -> Self {
        ContractContainer::Wasm(value)
    }
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
