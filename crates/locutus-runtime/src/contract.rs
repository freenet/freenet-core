use locutus_stdlib::prelude::{ContractData, ContractKey, ContractSpecification, Parameters};
use serde::Serialize;
use std::{fmt::Display, fs::File, io::Read, ops::Deref, path::Path, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct Contract(Arc<ContractSpecification<'static>>);

impl Contract {
    pub fn new(data: ContractData<'static>, params: Parameters<'static>) -> Contract {
        let spec = ContractSpecification::new(data, params);
        Contract(Arc::new(spec))
    }

    #[inline]
    pub fn key(&self) -> &ContractKey {
        self.0.key()
    }

    #[inline]
    pub fn data(&self) -> &ContractData {
        self.0.data()
    }
}

impl<'a> TryFrom<(&'a Path, Parameters<'static>)> for Contract {
    type Error = ContractRuntimeError;
    fn try_from(data: (&'a Path, Parameters<'static>)) -> Result<Self, Self::Error> {
        let (path, params) = data;
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        let data = ContractData::from(contract_data);
        let spec = ContractSpecification::new(data, params);
        Ok(Contract(Arc::new(spec)))
    }
}

impl TryInto<Vec<u8>> for Contract {
    type Error = ContractRuntimeError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.0)
            .map(|r| r.into_data().into_data())
            .map_err(|_| ContractRuntimeError::UnwrapContract)
    }
}

impl Display for Contract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
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
