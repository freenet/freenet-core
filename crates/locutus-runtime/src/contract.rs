use locutus_stdlib::prelude::{ContractData, ContractKey, Parameters};
use serde::Serialize;
use std::{fmt::Display, fs::File, io::Read, ops::Deref, path::Path, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
pub struct Contract<'a> {
    pub(crate) data: Arc<ContractData<'a>>,
    pub(crate) params: Parameters<'a>,
    pub(crate) key: ContractKey,
}

impl PartialEq for Contract<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract<'_> {}

impl<'a> Contract<'a> {
    pub fn new(data: Arc<ContractData<'a>>, params: Parameters<'a>) -> Contract<'a> {
        let key = ContractKey::from((&params, &*data));
        Contract { data, params, key }
    }

    #[inline]
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    #[inline]
    pub fn data(&self) -> &Arc<ContractData> {
        &self.data
    }

    pub(crate) fn get_data_from_fs(
        path: &Path,
    ) -> Result<ContractData<'static>, ContractRuntimeError> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(ContractData::from(contract_data))
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for Contract<'static> {
    type Error = ContractRuntimeError;
    fn try_from(data: (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let (path, params) = data;
        let data = Arc::new(Self::get_data_from_fs(path)?);
        Ok(Contract::new(data, params))
    }
}

impl TryInto<Vec<u8>> for Contract<'static> {
    type Error = ContractRuntimeError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data)
            .map(|r| r.into_data())
            .map_err(|_| ContractRuntimeError::UnwrapContract)
    }
}

impl Display for Contract<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract(")?;
        self.key.fmt(f)?;
        write!(f, ")")
    }
}

#[cfg(feature = "testing")]
impl<'a> arbitrary::Arbitrary<'a> for Contract<'_> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;
        let data: ContractData = Arbitrary::arbitrary(u)?;
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
pub struct State(Arc<Vec<u8>>);

impl State {
    pub fn new(bytes: Vec<u8>) -> Self {
        State(Arc::new(bytes))
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for State {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl AsRef<[u8]> for State {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Deref for State {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data: String = if self.0.len() > 8 {
            (&self.0[..4])
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain((&self.0[4..]).iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.0.iter().copied().map(char::from).collect()
        };
        write!(f, "ContractState(data: [{}])", data)
    }
}
