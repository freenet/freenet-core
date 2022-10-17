use locutus_stdlib::prelude::{ContractCode, ContractKey, Parameters};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::{borrow::Borrow, fmt::Display, fs::File, io::Read, ops::Deref, path::Path, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WrappedContract {
    #[serde(
        serialize_with = "WrappedContract::ser_contract_data",
        deserialize_with = "WrappedContract::deser_contract_data"
    )]
    pub(crate) data: Arc<ContractCode<'static>>,
    #[serde(
        serialize_with = "WrappedContract::ser_params",
        deserialize_with = "WrappedContract::deser_params"
    )]
    pub(crate) params: Parameters<'static>,
    pub(crate) key: ContractKey,
}

impl PartialEq for WrappedContract {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for WrappedContract {}

impl WrappedContract {
    pub fn new(data: Arc<ContractCode<'static>>, params: Parameters<'static>) -> WrappedContract {
        let key = ContractKey::from((&params, &*data));
        WrappedContract { data, params, key }
    }

    #[inline]
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    #[inline]
    pub fn code(&self) -> &Arc<ContractCode<'static>> {
        &self.data
    }

    #[inline]
    pub fn params(&self) -> &Parameters<'static> {
        &self.params
    }

    pub(crate) fn get_data_from_fs(path: &Path) -> Result<ContractCode<'static>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(ContractCode::from(contract_data))
    }

    fn ser_contract_data<S>(data: &Arc<ContractCode<'_>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        data.serialize(ser)
    }

    fn deser_contract_data<'de, D>(_deser: D) -> Result<Arc<ContractCode<'static>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let data: ContractCode<'de> = Deserialize::deserialize(deser)?;
        // Ok(Arc::new(data))
        todo!()
    }

    fn ser_params<S>(data: &Parameters<'_>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        data.serialize(ser)
    }

    fn deser_params<'de, D>(_deser: D) -> Result<Parameters<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // let data: ContractCode<'de> = Deserialize::deserialize(deser)?;
        // Ok(Arc::new(data))
        todo!()
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for WrappedContract {
    type Error = std::io::Error;
    fn try_from(data: (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let (path, params) = data;
        let data = Arc::new(Self::get_data_from_fs(path)?);
        Ok(WrappedContract::new(data, params))
    }
}

impl TryFrom<&rmpv::Value> for WrappedContract {
    type Error = String;

    fn try_from(value: &rmpv::Value) -> Result<Self, Self::Error> {
        let contract_map: HashMap<&str, &rmpv::Value> = HashMap::from_iter(
            value
                .as_map()
                .unwrap()
                .iter()
                .map(|(key, val)| (key.as_str().unwrap(), val)),
        );

        let key_value = contract_map.get("key").unwrap();
        let key_map: HashMap<&str, &rmpv::Value> = HashMap::from_iter(
            key_value
                .as_map()
                .unwrap()
                .iter()
                .map(|(key, val)| (key.as_str().unwrap(), val)),
        );
        let key_instance = *key_map.get("instance").unwrap();
        let key = ContractKey::try_from(key_instance).unwrap();

        let contract_data = contract_map.get("data").unwrap();
        let data = {
            let contract = ContractCode::try_from(*contract_data).unwrap().into_owned();
            Arc::new(contract)
        };

        let contract_params = contract_map.get("parameters").unwrap();
        let params = Parameters::try_from(*contract_params).unwrap().into_owned();

        Ok(Self { data, params, key })
    }
}

impl TryInto<Vec<u8>> for WrappedContract {
    type Error = ContractRuntimeError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data)
            .map(|r| r.into_bytes())
            .map_err(|_| ContractRuntimeError::UnwrapContract)
    }
}

impl Display for WrappedContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract(")?;
        self.key.fmt(f)?;
        write!(f, ")")
    }
}

#[cfg(feature = "testing")]
impl<'a> arbitrary::Arbitrary<'a> for WrappedContract {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;
        let data: ContractCode = Arbitrary::arbitrary(u)?;
        let param_bytes: Vec<u8> = Arbitrary::arbitrary(u)?;
        let params = Parameters::from(param_bytes);
        let key = ContractKey::from((&params, &data));
        Ok(Self {
            data: Arc::new(data),
            params,
            key,
        })
    }
}

/// The state for a contract.
#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct WrappedState(
    #[serde(
        serialize_with = "WrappedState::ser_state",
        deserialize_with = "WrappedState::deser_state"
    )]
    Arc<Vec<u8>>,
);

impl WrappedState {
    pub fn new(bytes: Vec<u8>) -> Self {
        WrappedState(Arc::new(bytes))
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    fn ser_state<S>(data: &Arc<Vec<u8>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serde_bytes::serialize(&**data, ser)
    }

    fn deser_state<'de, D>(deser: D) -> Result<Arc<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Vec<u8> = serde_bytes::deserialize(deser)?;
        Ok(Arc::new(data))
    }
}

impl From<Vec<u8>> for WrappedState {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl TryFrom<&rmpv::Value> for WrappedState {
    type Error = String;

    fn try_from(value: &rmpv::Value) -> Result<Self, Self::Error> {
        let state = value.as_slice().unwrap().to_vec();
        Ok(WrappedState::from(state))
    }
}

impl AsRef<[u8]> for WrappedState {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for WrappedState {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl Borrow<[u8]> for WrappedState {
    fn borrow(&self) -> &[u8] {
        &*self.0
    }
}

impl std::fmt::Display for WrappedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data: String = if self.0.len() > 8 {
            let last_4 = self.0.len() - 4;
            self.0[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.0[last_4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.0.iter().copied().map(char::from).collect()
        };
        write!(f, "ContractState(data: [{}])", data)
    }
}
