use locutus_stdlib::prelude::{ContractData, ContractSpecification, Parameters};
use serde::Serialize;
use std::{fs::File, io::Read, ops::Deref, path::Path, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct Contract(Arc<ContractData<'static>>);

impl Contract {
    pub fn new(bytes: Vec<u8>) -> Contract {
        Contract(Arc::new(ContractData::from(bytes)))
    }

    #[inline]
    pub fn key(&self) -> &[u8] {
        self.0.key()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.0.data()
    }

    pub fn into_spec(self, parameters: Parameters) -> ContractSpecification {
        let contract = ContractData::from(self.0.data().to_vec());
        ContractSpecification::new(contract, parameters)
    }
}

impl<'a> TryFrom<&'a Path> for Contract {
    type Error = ContractRuntimeError;
    fn try_from(path: &'a Path) -> Result<Self, Self::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(Contract(Arc::new(ContractData::from(contract_data))))
    }
}

impl TryInto<Vec<u8>> for Contract {
    type Error = ContractRuntimeError;
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.0)
            .map(|r| r.into_data())
            .map_err(|_| ContractRuntimeError::UnwrapContract)
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
