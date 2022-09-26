use locutus_stdlib::prelude::{ContractCode, ContractKey, Parameters};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{borrow::Borrow, fmt::Display, fs::File, io::Read, ops::Deref, path::Path, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct WrappedContract<'a> {
    #[serde(
        serialize_with = "inner_ser_contract_data",
        deserialize_with = "inner_deser_contract_data"
    )]
    pub(crate) data: Arc<ContractCode<'a>>,
    pub(crate) params: Parameters<'a>,
    pub(crate) key: ContractKey,
}

fn inner_ser_contract_data<S>(data: &Arc<ContractCode<'_>>, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    data.serialize(ser)
}

fn inner_deser_contract_data<'de, D>(deser: D) -> Result<Arc<ContractCode<'static>>, D::Error>
where
    D: Deserializer<'de>,
{
    let data: ContractCode<'static> = Deserialize::deserialize(deser)?;
    Ok(Arc::new(data))
}

impl PartialEq for WrappedContract<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for WrappedContract<'_> {}

impl<'a> WrappedContract<'a> {
    pub fn new(data: Arc<ContractCode<'a>>, params: Parameters<'a>) -> WrappedContract<'a> {
        let key = ContractKey::from((&params, &*data));
        WrappedContract { data, params, key }
    }

    #[inline]
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    #[inline]
    pub fn code(&self) -> &Arc<ContractCode<'a>> {
        &self.data
    }

    #[inline]
    pub fn params(&self) -> &Parameters<'a> {
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
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for WrappedContract<'static> {
    type Error = std::io::Error;
    fn try_from(data: (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let (path, params) = data;
        let data = Arc::new(Self::get_data_from_fs(path)?);
        Ok(WrappedContract::new(data, params))
    }
}

impl TryInto<Vec<u8>> for WrappedContract<'static> {
    type Error = ContractRuntimeError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data)
            .map(|r| r.into_data())
            .map_err(|_| ContractRuntimeError::UnwrapContract)
    }
}

impl Display for WrappedContract<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract(")?;
        self.key.fmt(f)?;
        write!(f, ")")
    }
}

#[cfg(feature = "testing")]
impl<'a> arbitrary::Arbitrary<'a> for WrappedContract<'_> {
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
        serialize_with = "inner_ser_state",
        deserialize_with = "inner_deser_state"
    )]
    Arc<Vec<u8>>,
);

fn inner_ser_state<S>(data: &Arc<Vec<u8>>, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serde_bytes::serialize(&**data, ser)
}

fn inner_deser_state<'de, D>(deser: D) -> Result<Arc<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let data: Vec<u8> = serde_bytes::deserialize(deser)?;
    Ok(Arc::new(data))
}

impl WrappedState {
    pub fn new(bytes: Vec<u8>) -> Self {
        WrappedState(Arc::new(bytes))
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for WrappedState {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
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
