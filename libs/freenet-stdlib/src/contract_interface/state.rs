//! Contract state types: State, StateDelta, and StateSummary.

use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
};

use serde::{Deserialize, Serialize};
use serde_with::serde_as;

#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct State<'a>(
    // TODO: conver this to Arc<[u8]> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl State<'_> {
    /// Gets the number of bytes of data stored in the `State`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn into_owned(self) -> State<'static> {
        State(self.0.into_owned().into())
    }

    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_owned()
    }

    /// Acquires a mutable reference to the owned form of the `State` data.
    pub fn to_mut(&mut self) -> &mut Vec<u8> {
        self.0.to_mut()
    }
}

impl From<Vec<u8>> for State<'_> {
    fn from(state: Vec<u8>) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for State<'a> {
    fn from(state: &'a [u8]) -> Self {
        State(Cow::from(state))
    }
}

impl AsRef<[u8]> for State<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for State<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for State<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::io::Read for State<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.as_ref().read(buf)
    }
}

/// Represents a modification to some state - similar to a diff in source code.
///
/// The exact format of a delta is determined by the contract. A [contract](Contract) implementation will determine whether
/// a delta is valid - perhaps by verifying it is signed by someone authorized to modify the
/// contract state. A delta may be created in response to a [State Summary](StateSummary) as part of the State
/// Synchronization mechanism.
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct StateDelta<'a>(
    // TODO: conver this to Arc<[u8]> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl StateDelta<'_> {
    /// Gets the number of bytes of data stored in the `StateDelta`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_owned()
    }

    pub fn into_owned(self) -> StateDelta<'static> {
        StateDelta(self.0.into_owned().into())
    }
}

impl From<Vec<u8>> for StateDelta<'_> {
    fn from(delta: Vec<u8>) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl<'a> From<&'a [u8]> for StateDelta<'a> {
    fn from(delta: &'a [u8]) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl AsRef<[u8]> for StateDelta<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateDelta<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StateDelta<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Summary of `State` changes.
///
/// Given a contract state, this is a small piece of data that can be used to determine a delta
/// between two contracts as part of the state synchronization mechanism. The format of a state
/// summary is determined by the state's contract.
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct StateSummary<'a>(
    // TODO: conver this to Arc<[u8]> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl StateSummary<'_> {
    /// Extracts the owned data as a `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0.into_owned()
    }

    /// Gets the number of bytes of data stored in the `StateSummary`.
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn into_owned(self) -> StateSummary<'static> {
        StateSummary(self.0.into_owned().into())
    }

    pub fn deser_state_summary<'de, D>(deser: D) -> Result<StateSummary<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <StateSummary as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }
}

impl From<Vec<u8>> for StateSummary<'_> {
    fn from(state: Vec<u8>) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for StateSummary<'a> {
    fn from(state: &'a [u8]) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl AsRef<[u8]> for StateSummary<'_> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateSummary<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StateSummary<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
