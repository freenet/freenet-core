//! Arc-wrapped versions of contract types for efficient sharing.
//!
//! This module provides `WrappedState` and `WrappedContract` which use `Arc`
//! for efficient sharing of state and contract data across threads.

use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{Bytes, DeserializeAs, SerializeAs};

use crate::parameters::Parameters;

use super::code::ContractCode;
use super::key::ContractKey;
use super::state::State;

// TODO:  get rid of this when State is internally an Arc<[u8]>
/// The state for a contract.
#[derive(PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
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
        S: serde::Serializer,
    {
        Bytes::serialize_as(&**data, ser)
    }

    fn deser_state<'de, D>(deser: D) -> Result<Arc<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Vec<u8> = Bytes::deserialize_as(deser)?;
        Ok(Arc::new(data))
    }
}

impl From<Vec<u8>> for WrappedState {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(bytes)
    }
}

impl From<&'_ [u8]> for WrappedState {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_owned())
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
        &self.0
    }
}

impl Borrow<[u8]> for WrappedState {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl From<WrappedState> for State<'static> {
    fn from(value: WrappedState) -> Self {
        match Arc::try_unwrap(value.0) {
            Ok(v) => State::from(v),
            Err(v) => State::from(v.as_ref().to_vec()),
        }
    }
}

impl std::fmt::Display for WrappedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ContractState(data: [0x")?;
        for b in self.0.iter().take(8) {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "...])")
    }
}

impl std::fmt::Debug for WrappedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

/// Just as `freenet_stdlib::Contract` but with some convenience impl.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WrappedContract {
    #[serde(
        serialize_with = "WrappedContract::ser_contract_data",
        deserialize_with = "WrappedContract::deser_contract_data"
    )]
    pub data: Arc<ContractCode<'static>>,
    #[serde(deserialize_with = "Parameters::deser_params")]
    pub params: Parameters<'static>,
    pub key: ContractKey,
}

impl PartialEq for WrappedContract {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for WrappedContract {}

impl WrappedContract {
    pub fn new(data: Arc<ContractCode<'static>>, params: Parameters<'static>) -> WrappedContract {
        let key = ContractKey::from_params_and_code(&params, &*data);
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

    fn ser_contract_data<S>(data: &Arc<ContractCode<'_>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        data.serialize(ser)
    }

    fn deser_contract_data<'de, D>(deser: D) -> Result<Arc<ContractCode<'static>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: ContractCode<'de> = Deserialize::deserialize(deser)?;
        Ok(Arc::new(data.into_owned()))
    }
}

impl TryInto<Vec<u8>> for WrappedContract {
    type Error = ();
    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        Arc::try_unwrap(self.data)
            .map(|r| r.into_bytes())
            .map_err(|_| ())
    }
}

impl Display for WrappedContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.key, f)
    }
}

#[cfg(feature = "testing")]
impl<'a> arbitrary::Arbitrary<'a> for WrappedContract {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        use arbitrary::Arbitrary;
        let data = <ContractCode as Arbitrary>::arbitrary(u)?.into_owned();
        let param_bytes: Vec<u8> = Arbitrary::arbitrary(u)?;
        let params = Parameters::from(param_bytes);
        let key = ContractKey::from_params_and_code(&params, &data);
        Ok(Self {
            data: Arc::new(data),
            params,
            key,
        })
    }
}
