use std::fmt;
use std::fmt::{Display, Formatter};
use std::io::{Cursor, Read};
use std::path::Path;
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use semver::Version;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

use crate::client_api::{TryFromTsStd, WsApiError};
use crate::parameters::Parameters;
use crate::prelude::{CodeHash, Delegate, DelegateCode, DelegateKey, WrappedContract};
use crate::{contract_interface::ContractKey, prelude::ContractCode};

/// Contains the different versions available for WASM delegates.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DelegateWasmAPIVersion {
    V1(
        #[serde(deserialize_with = "DelegateWasmAPIVersion::deserialize_delegate")]
        Delegate<'static>,
    ),
}

impl DelegateWasmAPIVersion {
    fn deserialize_delegate<'de, D>(deser: D) -> Result<Delegate<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Delegate<'de> = Deserialize::deserialize(deser)?;
        Ok(data.into_owned())
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DelegateContainer {
    Wasm(DelegateWasmAPIVersion),
}

impl DelegateContainer {
    pub fn key(&self) -> &DelegateKey {
        match self {
            Self::Wasm(DelegateWasmAPIVersion::V1(delegate_v1)) => delegate_v1.key(),
        }
    }

    pub fn code(&self) -> &DelegateCode {
        match self {
            Self::Wasm(DelegateWasmAPIVersion::V1(delegate_v1)) => delegate_v1.code(),
        }
    }

    pub fn code_hash(&self) -> &CodeHash {
        match self {
            Self::Wasm(DelegateWasmAPIVersion::V1(delegate_v1)) => delegate_v1.code_hash(),
        }
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for DelegateContainer {
    type Error = std::io::Error;

    fn try_from((path, params): (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

        let (contract_code, version) = DelegateCode::load_versioned_from_path(path)?;

        match version {
            version if version == VERSION_0_0_1 => {
                let delegate = Delegate::from((&contract_code, &params));
                Ok(DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(
                    delegate,
                )))
            }
            _ => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }
}

impl<'a, P> TryFrom<(Vec<u8>, P)> for DelegateContainer
where
    P: std::ops::Deref<Target = Parameters<'a>>,
{
    type Error = std::io::Error;

    fn try_from((versioned_contract_bytes, params): (Vec<u8>, P)) -> Result<Self, Self::Error> {
        let params = params.deref().clone().into_owned();
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

        let (contract_code, version) =
            DelegateCode::load_versioned_from_bytes(versioned_contract_bytes)?;

        match version {
            version if version == VERSION_0_0_1 => {
                let delegate = Delegate::from((&contract_code, &params));
                Ok(DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(
                    delegate,
                )))
            }
            _ => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }
}

impl DelegateCode<'static> {
    fn load_versioned(
        mut contract_data: Cursor<Vec<u8>>,
    ) -> Result<(Self, Version), std::io::Error> {
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

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

        if version == VERSION_0_0_1 {
            let mut code_hash = [0u8; 32];
            contract_data.read_exact(&mut code_hash)?;
        }

        // Get Contract code
        let mut code_data: Vec<u8> = vec![];
        contract_data
            .read_to_end(&mut code_data)
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        Ok((DelegateCode::from(code_data), version))
    }

    /// Loads contract code which has been versioned from the fs.
    pub fn load_versioned_from_path(path: &Path) -> Result<(Self, Version), std::io::Error> {
        let contract_data = Cursor::new(Self::load_bytes(path)?);
        Self::load_versioned(contract_data)
    }

    /// Loads contract code which has been versioned from the fs.
    pub fn load_versioned_from_bytes(
        versioned_code: Vec<u8>,
    ) -> Result<(Self, Version), std::io::Error> {
        let contract_data = Cursor::new(versioned_code);
        Self::load_versioned(contract_data)
    }
}

impl DelegateCode<'_> {
    pub fn to_bytes_versioned(
        &self,
        version: &Version,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match version {
            ver if ver == &Version::new(0, 0, 1) => {
                let mut serialized_version = serde_json::to_vec(&ver)
                    .map_err(|e| format!("couldn't serialize contract version: {e}"))?;
                let output_size =
                    std::mem::size_of::<u32>() + serialized_version.len() + self.data().len();
                let mut output: Vec<u8> = Vec::with_capacity(output_size);
                output.write_u32::<BigEndian>(serialized_version.len() as u32)?;
                output.append(&mut serialized_version);
                output.extend(self.hash().0.iter());
                output.extend(self.data());
                Ok(output)
            }
            _ => panic!("version not supported"),
        }
    }
}

/// Contains the different versions available for WASM contracts.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]

pub enum ContractWasmAPIVersion {
    V1(WrappedContract),
}

impl From<ContractWasmAPIVersion> for ContractContainer {
    fn from(value: ContractWasmAPIVersion) -> Self {
        ContractContainer::Wasm(value)
    }
}

impl Display for ContractWasmAPIVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ContractWasmAPIVersion::V1(contract_v1) => {
                write!(f, "version 0.0.1 of contract {contract_v1}")
            }
        }
    }
}

/// Wrapper that allows contract versioning. This enum maintains the types of contracts that are
/// allowed and their corresponding version.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ContractContainer {
    Wasm(ContractWasmAPIVersion),
}

impl From<ContractContainer> for Version {
    fn from(contract: ContractContainer) -> Version {
        match contract {
            ContractContainer::Wasm(ContractWasmAPIVersion::V1(_)) => Version::new(0, 0, 1),
        }
    }
}

impl ContractContainer {
    /// Return the `ContractKey` from the specific contract version.
    pub fn key(&self) -> ContractKey {
        match self {
            Self::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => contract_v1.key().clone(),
        }
    }

    /// Return the `Parameters` from the specific contract version.
    pub fn params(&self) -> Parameters<'static> {
        match self {
            Self::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => contract_v1.params().clone(),
        }
    }

    /// Return the contract code from the specific contract version as `Vec<u8>`.
    pub fn data(&self) -> Vec<u8> {
        match self {
            Self::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => {
                contract_v1.clone().try_into().unwrap()
            }
        }
    }

    pub fn unwrap_v1(self) -> WrappedContract {
        match self {
            Self::Wasm(ContractWasmAPIVersion::V1(contract_v1)) => contract_v1,
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
            version if version == VERSION_0_0_1 => Ok(ContractContainer::Wasm(
                ContractWasmAPIVersion::V1(WrappedContract::new(Arc::new(contract_code), params)),
            )),
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
            version if version == VERSION_0_0_1 => Ok(ContractContainer::Wasm(
                ContractWasmAPIVersion::V1(WrappedContract::new(Arc::new(contract_code), params)),
            )),
            _ => Err(std::io::ErrorKind::InvalidData.into()),
        }
    }
}

impl ContractCode<'static> {
    fn load_versioned(
        mut contract_data: Cursor<Vec<u8>>,
    ) -> Result<(Self, Version), std::io::Error> {
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

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

        if version == VERSION_0_0_1 {
            let mut code_hash = [0u8; 32];
            contract_data.read_exact(&mut code_hash)?;
        }

        // Get Contract code
        let mut code_data: Vec<u8> = vec![];
        contract_data
            .read_to_end(&mut code_data)
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        Ok((ContractCode::from(code_data), version))
    }

    /// Loads contract code which has been versioned from the fs.
    pub fn load_versioned_from_path(path: &Path) -> Result<(Self, Version), std::io::Error> {
        let contract_data = Cursor::new(Self::load_bytes(path)?);
        Self::load_versioned(contract_data)
    }

    /// Loads contract code which has been versioned from the fs.
    pub fn load_versioned_from_bytes(
        versioned_code: Vec<u8>,
    ) -> Result<(Self, Version), std::io::Error> {
        let contract_data = Cursor::new(versioned_code);
        Self::load_versioned(contract_data)
    }
}

impl ContractCode<'_> {
    pub fn to_bytes_versioned(
        &self,
        version: &Version,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match version {
            ver if ver == &Version::new(0, 0, 1) => {
                let mut serialized_version = serde_json::to_vec(&ver)
                    .map_err(|e| format!("couldn't serialize contract version: {e}"))?;
                let output_size =
                    std::mem::size_of::<u32>() + serialized_version.len() + self.data().len();
                let mut output: Vec<u8> = Vec::with_capacity(output_size);
                output.write_u32::<BigEndian>(serialized_version.len() as u32)?;
                output.append(&mut serialized_version);
                output.extend(self.hash.0.iter());
                output.extend(self.data());
                Ok(output)
            }
            _ => panic!("version not supported"),
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
                Ok(ContractContainer::Wasm(ContractWasmAPIVersion::V1(
                    contract,
                )))
            }
            _ => unreachable!(),
        }
    }
}
