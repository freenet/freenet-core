use locutus_stdlib::prelude::{Contract as StdContract, ContractKey};
use serde::Serialize;
use std::{fmt::Display, fs::File, io::Read, ops::Deref, path::PathBuf, sync::Arc};

use crate::ContractRuntimeError;

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[derive(Clone, Debug, Serialize, PartialEq, Eq, serde::Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct Contract(Arc<StdContract>);

impl Display for Contract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl Contract {
    pub fn new(bytes: Vec<u8>) -> Contract {
        Contract(Arc::new(StdContract::new(bytes)))
    }

    #[inline]
    pub fn key(&self) -> ContractKey {
        self.0.key()
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        self.0.data()
    }
}

impl TryFrom<PathBuf> for Contract {
    type Error = ContractRuntimeError;
    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(Contract(Arc::new(StdContract::new(contract_data))))
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
pub struct ContractState(Arc<Vec<u8>>);

impl ContractState {
    pub fn new(bytes: Vec<u8>) -> Self {
        ContractState(Arc::new(bytes))
    }
}

impl Deref for ContractState {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl std::fmt::Display for ContractState {
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
