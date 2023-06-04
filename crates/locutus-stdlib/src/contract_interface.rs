//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    fmt::Display,
    fs::File,
    hash::{Hash, Hasher},
    io::{Cursor, Read},
    ops::{Deref, DerefMut},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use blake2::{Blake2s256, Digest};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use semver::Version;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::serde_as;

use crate::{
    client_api::{TryFromTsStd, WsApiError},
    code_hash::CodeHash,
    parameters::Parameters,
};

const CONTRACT_KEY_SIZE: usize = 32;

/// Type of errors during interaction with a contract.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum ContractError {
    #[error("de/serialization error: {0}")]
    Deser(String),
    #[error("invalid contract update")]
    InvalidUpdate,
    #[error("trying to read an invalid state")]
    InvalidState,
    #[error("trying to read an invalid delta")]
    InvalidDelta,
    #[error("{0}")]
    Other(String),
}

/// An update to a contract state or any required related contracts to update that state.
#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateModification<'a> {
    #[serde(borrow)]
    pub new_state: Option<State<'a>>,
    /// Request an other contract so updates can be resolved.
    pub related: Vec<RelatedContract>,
}

impl<'a> UpdateModification<'a> {
    /// Constructor for self when the state is valid.
    pub fn valid(new_state: State<'a>) -> Self {
        Self {
            new_state: Some(new_state),
            related: vec![],
        }
    }

    /// Constructor for self when this contract still is missing some [`RelatedContract`]
    /// to proceed with any verification or updates.
    pub fn requires(related: Vec<RelatedContract>) -> Self {
        Self {
            new_state: None,
            related,
        }
    }

    /// Unwraps self returning a [`State`].
    ///
    /// Panics if self does not contain a state.
    pub fn unwrap_valid(self) -> State<'a> {
        match self.new_state {
            Some(s) => s,
            _ => panic!("failed unwrapping state in modification"),
        }
    }

    /// Gets the pending related contracts.
    pub fn get_related(&self) -> &[RelatedContract] {
        &self.related
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> UpdateModification<'static> {
        let Self { new_state, related } = self;
        UpdateModification {
            new_state: new_state.map(|s| State::from(s.into_bytes())),
            related,
        }
    }
}

/// The contracts related to a parent or root contract. Tipically this means
/// contracts which state requires to be verified or integrated in some way with
/// the parent contract.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
pub struct RelatedContracts<'a> {
    #[serde(borrow)]
    map: HashMap<ContractInstanceId, Option<State<'a>>>,
}

impl RelatedContracts<'_> {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> RelatedContracts<'static> {
        let mut map = HashMap::with_capacity(self.map.len());
        for (k, v) in self.map {
            map.insert(k, v.map(|s| s.into_owned()));
        }
        RelatedContracts { map }
    }
}

impl RelatedContracts<'static> {
    pub fn states(self) -> impl Iterator<Item = (ContractInstanceId, Option<State<'static>>)> {
        self.map.into_iter()
    }
}

impl<'a> RelatedContracts<'a> {
    pub fn update<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = (&ContractInstanceId, &mut Option<State<'a>>)> + 'b {
        self.map.iter_mut()
    }
}

impl<'a> TryFrom<&'a rmpv::Value> for RelatedContracts<'a> {
    type Error = String;

    fn try_from(value: &'a rmpv::Value) -> Result<Self, Self::Error> {
        let related_contracts: HashMap<ContractInstanceId, Option<State<'a>>> =
            HashMap::from_iter(value.as_map().unwrap().iter().map(|(key, val)| {
                let id = ContractInstanceId::from_bytes(key.as_slice().unwrap()).unwrap();
                let state = State::from(val.as_slice().unwrap());
                (id, Some(state))
            }));
        Ok(RelatedContracts::from(related_contracts))
    }
}

impl<'a> TryFromTsStd<&'a rmpv::Value> for RelatedContracts<'a> {
    fn try_decode(value: &'a rmpv::Value) -> Result<Self, WsApiError> {
        let related_contracts: HashMap<ContractInstanceId, Option<State<'a>>> =
            HashMap::from_iter(value.as_map().unwrap().iter().map(|(key, val)| {
                let id = ContractInstanceId::from_bytes(key.as_slice().unwrap()).unwrap();
                let state = State::from(val.as_slice().unwrap());
                (id, Some(state))
            }));
        Ok(RelatedContracts::from(related_contracts))
    }
}

impl<'a> From<HashMap<ContractInstanceId, Option<State<'a>>>> for RelatedContracts<'a> {
    fn from(related_contracts: HashMap<ContractInstanceId, Option<State<'a>>>) -> Self {
        Self {
            map: related_contracts,
        }
    }
}

/// A contract related to an other contract and the specification
/// of the kind of update notifications that should be received by this contract.
#[derive(Debug, Serialize, Deserialize)]
pub struct RelatedContract {
    pub contract_instance_id: ContractInstanceId,
    pub mode: RelatedMode,
    // todo: add a timeout so we stop listening/subscribing eventually
}

/// Specification of the notifications of interest from a related contract.
#[derive(Debug, Serialize, Deserialize)]
pub enum RelatedMode {
    /// Retrieve the state once, don't be concerned with subsequent changes.
    StateOnce,
    /// Retrieve the state once, and then subscribe to updates.
    StateThenSubscribe,
}

/// The result of calling the [`ContractInterface::validate_state`] function.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidateResult {
    Valid,
    Invalid,
    /// The peer will attempt to retrieve the requested contract states
    /// and will call validate_state() again when it retrieves them.
    RequestRelated(Vec<ContractInstanceId>),
}

/// Update notifications for a contract or a related contract.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum UpdateData<'a> {
    State(#[serde(borrow)] State<'a>),
    Delta(#[serde(borrow)] StateDelta<'a>),
    StateAndDelta {
        #[serde(borrow)]
        state: State<'a>,
        #[serde(borrow)]
        delta: StateDelta<'a>,
    },
    RelatedState {
        related_to: ContractInstanceId,
        #[serde(borrow)]
        state: State<'a>,
    },
    RelatedDelta {
        related_to: ContractInstanceId,
        #[serde(borrow)]
        delta: StateDelta<'a>,
    },
    RelatedStateAndDelta {
        related_to: ContractInstanceId,
        #[serde(borrow)]
        state: State<'a>,
        #[serde(borrow)]
        delta: StateDelta<'a>,
    },
}

impl UpdateData<'_> {
    pub fn size(&self) -> usize {
        match self {
            UpdateData::State(state) => state.size(),
            UpdateData::Delta(delta) => delta.size(),
            UpdateData::StateAndDelta { state, delta } => state.size() + delta.size(),
            UpdateData::RelatedState { state, .. } => state.size() + CONTRACT_KEY_SIZE,
            UpdateData::RelatedDelta { delta, .. } => delta.size() + CONTRACT_KEY_SIZE,
            UpdateData::RelatedStateAndDelta { state, delta, .. } => {
                state.size() + delta.size() + CONTRACT_KEY_SIZE
            }
        }
    }

    pub fn unwrap_delta(&self) -> &StateDelta<'_> {
        match self {
            UpdateData::Delta(delta) => delta,
            _ => panic!(),
        }
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> UpdateData<'static> {
        match self {
            UpdateData::State(s) => UpdateData::State(State::from(s.into_bytes())),
            UpdateData::Delta(d) => UpdateData::Delta(StateDelta::from(d.into_bytes())),
            UpdateData::StateAndDelta { state, delta } => UpdateData::StateAndDelta {
                delta: StateDelta::from(delta.into_bytes()),
                state: State::from(state.into_bytes()),
            },
            UpdateData::RelatedState { related_to, state } => UpdateData::RelatedState {
                related_to,
                state: State::from(state.into_bytes()),
            },
            UpdateData::RelatedDelta { related_to, delta } => UpdateData::RelatedDelta {
                related_to,
                delta: StateDelta::from(delta.into_bytes()),
            },
            UpdateData::RelatedStateAndDelta {
                related_to,
                state,
                delta,
            } => UpdateData::RelatedStateAndDelta {
                related_to,
                state: State::from(state.into_bytes()),
                delta: StateDelta::from(delta.into_bytes()),
            },
        }
    }
}

impl<'a> From<StateDelta<'a>> for UpdateData<'a> {
    fn from(delta: StateDelta<'a>) -> Self {
        UpdateData::Delta(delta)
    }
}

impl<'a> TryFromTsStd<&'a rmpv::Value> for UpdateData<'a> {
    fn try_decode(value: &'a rmpv::Value) -> Result<Self, WsApiError> {
        let value_map: HashMap<_, _> = HashMap::from_iter(
            value
                .as_map()
                .unwrap()
                .iter()
                .map(|(key, val)| (key.as_str().unwrap(), val)),
        );
        let mut map_keys = Vec::from_iter(value_map.keys().copied());
        map_keys.sort();
        match map_keys.as_slice() {
            ["delta"] => {
                let delta = value_map.get("delta").unwrap();
                Ok(UpdateData::Delta(StateDelta::from(
                    delta.as_slice().unwrap(),
                )))
            }
            ["state"] => {
                let state = value_map.get("state").unwrap();
                Ok(UpdateData::Delta(StateDelta::from(
                    state.as_slice().unwrap(),
                )))
            }
            ["delta", "state"] => {
                let state = value_map.get("state").unwrap();
                let delta = value_map.get("delta").unwrap();
                Ok(UpdateData::StateAndDelta {
                    state: State::from(state.as_slice().unwrap()),
                    delta: StateDelta::from(delta.as_slice().unwrap()),
                })
            }
            ["delta", "relatedTo"] => {
                let delta = value_map.get("delta").unwrap();
                let related_to = value_map.get("relatedTo").unwrap();
                Ok(UpdateData::RelatedDelta {
                    delta: StateDelta::from(delta.as_slice().unwrap()),
                    related_to: ContractInstanceId::from_bytes(related_to.as_slice().unwrap())
                        .unwrap(),
                })
            }
            ["state", "relatedTo"] => {
                let state = value_map.get("state").unwrap();
                let related_to = value_map.get("relatedTo").unwrap();
                Ok(UpdateData::RelatedState {
                    state: State::from(state.as_slice().unwrap()),
                    related_to: ContractInstanceId::from_bytes(related_to.as_slice().unwrap())
                        .unwrap(),
                })
            }
            ["delta", "state", "relatedTo"] => {
                let state = value_map.get("state").unwrap();
                let delta = value_map.get("delta").unwrap();
                let related_to = value_map.get("relatedTo").unwrap();
                Ok(UpdateData::RelatedStateAndDelta {
                    state: State::from(state.as_slice().unwrap()),
                    delta: StateDelta::from(delta.as_slice().unwrap()),
                    related_to: ContractInstanceId::from_bytes(related_to.as_slice().unwrap())
                        .unwrap(),
                })
            }
            _ => unreachable!(),
        }
    }
}

/// Trait to implement for the contract building.
///
/// Contains all necessary methods to interact with the contract.
///
/// # Examples
///
/// Implementing `ContractInterface` on a type:
///
/// ```
/// # use locutus_stdlib::prelude::*;
/// struct Contract;
///
/// #[contract]
/// impl ContractInterface for Contract {
///     fn validate_state(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///         _related: RelatedContracts
///     ) -> Result<ValidateResult, ContractError> {
///         Ok(ValidateResult::Valid)
///     }
///
///     fn validate_delta(
///         _parameters: Parameters<'static>,
///         _delta: StateDelta<'static>
///     ) -> Result<bool, ContractError> {
///         Ok(true)
///     }
///
///     fn update_state(
///         _parameters: Parameters<'static>,
///         state: State<'static>,
///         _data: Vec<UpdateData>,
///     ) -> Result<UpdateModification<'static>, ContractError> {
///         Ok(UpdateModification::valid(state))
///     }
///
///     fn summarize_state(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///     ) -> Result<StateSummary<'static>, ContractError> {
///         Ok(StateSummary::from(vec![]))
///     }
///
///     fn get_state_delta(
///         _parameters: Parameters<'static>,
///         _state: State<'static>,
///         _summary: StateSummary<'static>,
///     ) -> Result<StateDelta<'static>, ContractError> {
///         Ok(StateDelta::from(vec![]))
///     }
/// }
/// ```
// ANCHOR: contractifce
/// # ContractInterface
///
/// This trait defines the core functionality for managing and updating a contract's state.
/// Implementations must ensure that state delta updates are *commutative*. In other words,
/// when applying multiple delta updates to a state, the order in which these updates are
/// applied should not affect the final state. Once all deltas are applied, the resulting
/// state should be the same, regardless of the order in which the deltas were applied.
///
/// Noncompliant behavior, such as failing to obey the commutativity rule, may result
/// in the contract being deprioritized or removed from the p2p network.
pub trait ContractInterface {
    /// Verify that the state is valid, given the parameters.
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>;

    /// Verify that a delta is valid if possible, returns false if and only delta is
    /// definitely invalid, true otherwise.
    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError>;

    /// Update the state to account for the new data
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError>;

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError>;

    /// Generate a state delta using a summary from the current state.
    /// This along with [`Self::summarize_state`] allows flexible and efficient
    /// state synchronization between peers.
    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError>;
}
// ANCHOR_END: contractifce

/// A complete contract specification requires a `parameters` section
/// and a `contract` section.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract<'a> {
    #[serde(borrow)]
    pub parameters: Parameters<'a>,
    #[serde(borrow)]
    pub data: ContractCode<'a>,
    // todo: skip serializing and instead compute it
    key: ContractKey,
}

impl<'a> Contract<'a> {
    /// Returns a contract from [contract code](ContractCode) and given [parameters](Parameters).
    pub fn new(contract: ContractCode<'a>, parameters: Parameters<'a>) -> Contract<'a> {
        let key = ContractKey::from((&parameters, &contract));
        Contract {
            parameters,
            data: contract,
            key,
        }
    }

    /// Key portion of the specification.
    pub fn key(&self) -> &ContractKey {
        &self.key
    }

    /// Code portion of the specification.
    pub fn into_code(self) -> ContractCode<'a> {
        self.data
    }
}

impl TryFrom<Vec<u8>> for Contract<'static> {
    type Error = std::io::Error;

    fn try_from(data: Vec<u8>) -> Result<Self, Self::Error> {
        let mut reader = Cursor::new(data);

        let params_len = reader.read_u64::<LittleEndian>()?;
        let mut params_buf = vec![0; params_len as usize];
        reader.read_exact(&mut params_buf)?;
        let parameters = Parameters::from(params_buf);

        let contract_len = reader.read_u64::<LittleEndian>()?;
        let mut contract_buf = vec![0; contract_len as usize];
        reader.read_exact(&mut contract_buf)?;
        let contract = ContractCode::from(contract_buf);

        let key = ContractKey::from((&parameters, &contract));

        Ok(Contract {
            parameters,
            data: contract,
            key,
        })
    }
}

impl PartialEq for Contract<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for Contract<'_> {}

impl std::fmt::Display for Contract<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ContractSpec( key: ")?;
        internal_fmt_key(&self.key.instance.0, f)?;
        let data: String = if self.data.data.len() > 8 {
            self.data.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{data}])")
    }
}

#[cfg(all(any(test, feature = "testing"), target_family = "unix"))]
impl<'a> arbitrary::Arbitrary<'a> for Contract<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let contract: ContractCode = u.arbitrary()?;
        let parameters: Vec<u8> = u.arbitrary()?;
        let parameters = Parameters::from(parameters);

        let key = ContractKey::from((&parameters, &contract));

        Ok(Contract {
            data: contract,
            parameters,
            key,
        })
    }
}

/// Data associated with a contract that can be retrieved by Applications and Delegates.
///
/// For efficiency and flexibility, contract state is represented as a simple [u8] byte array.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "testing", derive(arbitrary::Arbitrary))]
pub struct State<'a>(
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

impl<'a> From<Vec<u8>> for State<'a> {
    fn from(state: Vec<u8>) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for State<'a> {
    fn from(state: &'a [u8]) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> AsRef<[u8]> for State<'a> {
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

impl<'a> DerefMut for State<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> std::io::Read for State<'a> {
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
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    Cow<'a, [u8]>,
);

impl<'a> StateDelta<'a> {
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

impl<'a> From<Vec<u8>> for StateDelta<'a> {
    fn from(delta: Vec<u8>) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl<'a> From<&'a [u8]> for StateDelta<'a> {
    fn from(delta: &'a [u8]) -> Self {
        StateDelta(Cow::from(delta))
    }
}

impl<'a> AsRef<[u8]> for StateDelta<'a> {
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

impl<'a> DerefMut for StateDelta<'a> {
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
pub struct StateSummary<'a>(
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
}

impl<'a> From<Vec<u8>> for StateSummary<'a> {
    fn from(state: Vec<u8>) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for StateSummary<'a> {
    fn from(state: &'a [u8]) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> AsRef<[u8]> for StateSummary<'a> {
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

impl<'a> DerefMut for StateSummary<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(all(any(test, feature = "testing"), target_family = "unix"))]
impl<'a> arbitrary::Arbitrary<'a> for StateSummary<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(StateSummary::from(data))
    }
}

/// The executable contract.
///
/// It is the part of the executable belonging to the full specification
/// and does not include any other metadata (like the parameters).
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractCode<'a> {
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    data: Cow<'a, [u8]>,
    // todo: skip serializing and instead compute it
    hash: CodeHash,
}

impl ContractCode<'static> {
    /// Loads contract code which has been versioned from the fs.
    pub fn load_versioned(path: &Path) -> Result<(Self, Version), std::io::Error> {
        const VERSION_0_0_1: Version = Version::new(0, 0, 1);

        let mut contract_data = Cursor::new(Self::load_bytes(path)?);

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

    pub fn load_raw(path: &Path) -> Result<Self, std::io::Error> {
        let contract_data = Self::load_bytes(path)?;
        Ok(ContractCode::from(contract_data))
    }

    fn load_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
        let mut contract_file = File::open(path)?;
        let mut contract_data = if let Ok(md) = contract_file.metadata() {
            Vec::with_capacity(md.len() as usize)
        } else {
            Vec::new()
        };
        contract_file.read_to_end(&mut contract_data)?;
        Ok(contract_data)
    }
}

impl ContractCode<'_> {
    /// Contract key hash.
    pub fn hash(&self) -> &CodeHash {
        &self.hash
    }

    /// Returns the `Base58` string representation of the contract key.
    pub fn hash_str(&self) -> String {
        Self::encode_hash(&self.hash.0)
    }

    /// Reference to contract code.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Extracts the owned contract code data as a `Vec<u8>`.
    pub fn into_bytes(self) -> Vec<u8> {
        self.data.to_vec()
    }

    /// Returns the `Base58` string representation of a hash.
    pub fn encode_hash(hash: &[u8; CONTRACT_KEY_SIZE]) -> String {
        bs58::encode(hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> ContractCode<'static> {
        ContractCode {
            data: self.data.into_owned().into(),
            hash: self.hash,
        }
    }

    fn gen_hash(data: &[u8]) -> CodeHash {
        let mut hasher = Blake2s256::new();
        hasher.update(data);
        let key_arr = hasher.finalize();
        debug_assert_eq!(key_arr[..].len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);
        CodeHash(key)
    }

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

impl From<Vec<u8>> for ContractCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = ContractCode::gen_hash(&data);
        ContractCode {
            data: Cow::from(data),
            hash: key,
        }
    }
}

impl<'a> From<&'a [u8]> for ContractCode<'a> {
    fn from(data: &'a [u8]) -> ContractCode {
        let hash = ContractCode::gen_hash(data);
        ContractCode {
            data: Cow::from(data),
            hash,
        }
    }
}

impl<'a> TryFromTsStd<&'a rmpv::Value> for ContractCode<'a> {
    fn try_decode(value: &'a rmpv::Value) -> Result<Self, WsApiError> {
        let data = value.as_slice().unwrap();
        Ok(ContractCode::from(data))
    }
}

impl PartialEq for ContractCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for ContractCode<'_> {}

#[cfg(all(any(test, feature = "testing"), target_family = "unix"))]
impl<'a> arbitrary::Arbitrary<'a> for ContractCode<'static> {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let data: Vec<u8> = u.arbitrary()?;
        Ok(ContractCode::from(data))
    }
}

impl std::fmt::Display for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract( key: ")?;
        internal_fmt_key(&self.hash.0, f)?;
        let data: String = if self.data.len() > 8 {
            self.data[..4]
                .iter()
                .map(|b| char::from(*b))
                .chain("...".chars())
                .chain(self.data[4..].iter().map(|b| char::from(*b)))
                .collect()
        } else {
            self.data.iter().copied().map(char::from).collect()
        };
        write!(f, ", data: [{data}])")
    }
}

/// The key representing the hash of the contract executable code hash and a set of `parameters`.
#[serde_as]
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash)]
#[cfg_attr(
    all(any(test, feature = "testing"), target_family = "unix"),
    derive(arbitrary::Arbitrary)
)]
#[repr(transparent)]
pub struct ContractInstanceId(#[serde_as(as = "[_; CONTRACT_KEY_SIZE]")] [u8; CONTRACT_KEY_SIZE]);

impl ContractInstanceId {
    /// `Base58` string representation of the `contract id`.
    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    /// Build `ContractId` from the binary representation.
    fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, bs58::decode::Error> {
        let mut spec = [0; CONTRACT_KEY_SIZE];
        bs58::decode(bytes)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut spec)?;
        Ok(Self(spec))
    }
}

impl FromStr for ContractInstanceId {
    type Err = bs58::decode::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ContractInstanceId::from_bytes(s)
    }
}

impl TryFrom<String> for ContractInstanceId {
    type Error = bs58::decode::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        ContractInstanceId::from_bytes(s)
    }
}

impl<'a, T, U> From<(T, U)> for ContractInstanceId
where
    T: Borrow<Parameters<'a>>,
    U: Borrow<ContractCode<'a>>,
{
    fn from(val: (T, U)) -> Self {
        let (parameters, code_data) = (val.0.borrow(), val.1.borrow());
        generate_id(parameters, code_data)
    }
}

impl Display for ContractInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

/// A complete key specification, that represents a cryptographic hash that identifies the contract.
#[serde_as]
#[derive(Debug, Eq, Clone, Serialize, Deserialize)]
#[cfg_attr(
    all(any(test, feature = "testing"), target_family = "unix"),
    derive(arbitrary::Arbitrary)
)]
pub struct ContractKey {
    instance: ContractInstanceId,
    code: Option<CodeHash>,
}

impl PartialEq for ContractKey {
    fn eq(&self, other: &Self) -> bool {
        self.instance == other.instance
    }
}

impl std::hash::Hash for ContractKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.instance.0.hash(state);
    }
}

impl From<ContractInstanceId> for ContractKey {
    fn from(instance: ContractInstanceId) -> Self {
        Self {
            instance,
            code: None,
        }
    }
}

impl From<ContractKey> for ContractInstanceId {
    fn from(key: ContractKey) -> Self {
        key.instance
    }
}

impl<'a, T, U> From<(T, U)> for ContractKey
where
    T: Borrow<Parameters<'a>>,
    U: Borrow<ContractCode<'a>>,
{
    fn from(val: (T, U)) -> Self {
        let (parameters, code_data) = (val.0.borrow(), val.1.borrow());
        let id = generate_id(parameters, code_data);
        let code_hash = code_data.hash();
        Self {
            instance: id,
            code: Some(code_hash.clone()),
        }
    }
}

impl ContractKey {
    /// Builds a partial [`ContractKey`](ContractKey), the contract code part is unspecified.
    pub fn from_id(instance: impl Into<String>) -> Result<Self, bs58::decode::Error> {
        let instance = ContractInstanceId::try_from(instance.into())?;
        Ok(Self {
            instance,
            code: None,
        })
    }

    /// Gets the whole spec key hash.
    pub fn bytes(&self) -> &[u8] {
        self.instance.0.as_ref()
    }

    /// Returns the hash of the contract code only, if the key is fully specified.
    pub fn code_hash(&self) -> Option<&CodeHash> {
        self.code.as_ref()
    }

    /// Returns the encoded hash of the contract code, if the key is fully specified.
    pub fn encoded_code_hash(&self) -> Option<String> {
        self.code.as_ref().map(|c| {
            bs58::encode(c.0)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string()
        })
    }

    /// Returns the contract key from the encoded hash of the contract code and the given
    /// parameters.
    pub fn from_params(
        code_hash: impl Into<String>,
        parameters: Parameters,
    ) -> Result<Self, bs58::decode::Error> {
        let mut code_key = [0; CONTRACT_KEY_SIZE];
        bs58::decode(code_hash.into())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut code_key)?;

        let mut hasher = Blake2s256::new();
        hasher.update(code_key);
        hasher.update(parameters.as_ref());
        let full_key_arr = hasher.finalize();

        let mut spec = [0; CONTRACT_KEY_SIZE];
        spec.copy_from_slice(&full_key_arr);
        Ok(Self {
            instance: ContractInstanceId(spec),
            code: Some(CodeHash(code_key)),
        })
    }

    /// Returns the `Base58` encoded string of the [`ContractInstanceId`](ContractInstanceId).
    pub fn encoded_contract_id(&self) -> String {
        self.instance.encode()
    }

    pub fn id(&self) -> ContractInstanceId {
        self.instance
    }
}

impl Deref for ContractKey {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.instance.0
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.instance.fmt(f)
    }
}

impl TryFromTsStd<&rmpv::Value> for ContractKey {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
        let key_map: HashMap<&str, &rmpv::Value> = match value.as_map() {
            Some(map_value) => HashMap::from_iter(
                map_value
                    .iter()
                    .map(|(key, val)| (key.as_str().unwrap(), val)),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding ContractKey, input value is not a map".to_string(),
                })
            }
        };

        let instance_id = match key_map.get("instance") {
            Some(instance_value) => bs58::encode(&instance_value.as_slice().unwrap()).into_string(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, instance not found".to_string(),
                })
            }
        };

        Ok(ContractKey::from_id(instance_id).unwrap())
    }
}

fn generate_id<'a>(
    parameters: &Parameters<'a>,
    code_data: &ContractCode<'a>,
) -> ContractInstanceId {
    let contract_hash = code_data.hash();

    let mut hasher = Blake2s256::new();
    hasher.update(contract_hash.0);
    hasher.update(parameters.as_ref());
    let full_key_arr = hasher.finalize();

    debug_assert_eq!(full_key_arr[..].len(), CONTRACT_KEY_SIZE);
    let mut spec = [0; CONTRACT_KEY_SIZE];
    spec.copy_from_slice(&full_key_arr);
    ContractInstanceId(spec)
}

#[inline]
fn internal_fmt_key(
    key: &[u8; CONTRACT_KEY_SIZE],
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let r = bs58::encode(key)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string();
    write!(f, "{}", &r[..8])
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

impl From<&'_ [u8]> for WrappedState {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes.to_owned())
    }
}

impl TryFromTsStd<&rmpv::Value> for WrappedState {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
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
        write!(f, "ContractState(data: [{data}])")
    }
}

/// Just as `locutus_stdlib::Contract` but with some convenience impl.
#[non_exhaustive]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WrappedContract {
    #[serde(
        serialize_with = "WrappedContract::ser_contract_data",
        deserialize_with = "WrappedContract::deser_contract_data"
    )]
    pub data: Arc<ContractCode<'static>>,
    #[serde(
        serialize_with = "WrappedContract::ser_params",
        deserialize_with = "WrappedContract::deser_params"
    )]
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

    fn ser_contract_data<S>(data: &Arc<ContractCode<'_>>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
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

    fn ser_params<S>(data: &Parameters<'_>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        data.serialize(ser)
    }

    fn deser_params<'de, D>(deser: D) -> Result<Parameters<'static>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data: Parameters<'de> = Deserialize::deserialize(deser)?;
        Ok(data.into_owned())
    }
}

impl TryFromTsStd<&rmpv::Value> for WrappedContract {
    fn try_decode(value: &rmpv::Value) -> Result<Self, WsApiError> {
        let contract_map: HashMap<&str, &rmpv::Value> = match value.as_map() {
            Some(map_value) => HashMap::from_iter(
                map_value
                    .iter()
                    .map(|(key, val)| (key.as_str().unwrap(), val)),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, input value is not a map".to_string(),
                })
            }
        };

        let key = match contract_map.get("key") {
            Some(key_value) => ContractKey::try_decode(*key_value).unwrap(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, key not found".to_string(),
                })
            }
        };

        let data = match contract_map.get("data") {
            Some(contract_data_value) => Arc::new(
                ContractCode::try_decode(*contract_data_value)
                    .unwrap()
                    .into_owned(),
            ),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, data not found".to_string(),
                })
            }
        };

        let params = match contract_map.get("parameters") {
            Some(params_value) => Parameters::try_decode(*params_value).unwrap().into_owned(),
            _ => {
                return Err(WsApiError::MsgpackDecodeError {
                    cause: "Failed decoding WrappedContract, parameters not found".to_string(),
                })
            }
        };

        Ok(Self { data, params, key })
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

#[doc(hidden)]
pub(crate) mod wasm_interface {
    //! Contains all the types to interface between the host environment and
    //! the wasm module execution.
    use super::*;
    use crate::WasmLinearMem;

    #[repr(i32)]
    enum ResultKind {
        ValidateState = 0,
        ValidateDelta = 1,
        UpdateState = 2,
        SummarizeState = 3,
        StateDelta = 4,
    }

    impl From<i32> for ResultKind {
        fn from(v: i32) -> Self {
            match v {
                0 => ResultKind::ValidateState,
                1 => ResultKind::ValidateDelta,
                2 => ResultKind::UpdateState,
                3 => ResultKind::SummarizeState,
                4 => ResultKind::StateDelta,
                _ => panic!(),
            }
        }
    }

    #[doc(hidden)]
    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    pub struct ContractInterfaceResult {
        ptr: i64,
        kind: i32,
        size: u32,
    }

    impl ContractInterfaceResult {
        pub unsafe fn unwrap_validate_state_res(
            self,
            mem: WasmLinearMem,
        ) -> Result<ValidateResult, ContractError> {
            #![allow(clippy::let_and_return)]
            let kind = ResultKind::from(self.kind);
            match kind {
                ResultKind::ValidateState => {
                    let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
                    let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                    let value = bincode::deserialize(serialized)
                        .map_err(|e| ContractError::Other(format!("{e}")))?;
                    #[cfg(feature = "trace")]
                    self.log_input(serialized, &value, ptr);
                    value
                }
                _ => unreachable!(),
            }
        }

        pub unsafe fn unwrap_validate_delta_res(
            self,
            mem: WasmLinearMem,
        ) -> Result<bool, ContractError> {
            #![allow(clippy::let_and_return)]
            let kind = ResultKind::from(self.kind);
            match kind {
                ResultKind::ValidateDelta => {
                    let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
                    let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                    let value = bincode::deserialize(serialized)
                        .map_err(|e| ContractError::Other(format!("{e}")))?;
                    #[cfg(feature = "trace")]
                    self.log_input(serialized, &value, ptr);
                    value
                }
                _ => unreachable!(),
            }
        }

        pub unsafe fn unwrap_update_state(
            self,
            mem: WasmLinearMem,
        ) -> Result<UpdateModification<'static>, ContractError> {
            let kind = ResultKind::from(self.kind);
            match kind {
                ResultKind::UpdateState => {
                    let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
                    let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                    let value: Result<UpdateModification<'_>, ContractError> =
                        bincode::deserialize(serialized)
                            .map_err(|e| ContractError::Other(format!("{e}")))?;
                    #[cfg(feature = "trace")]
                    self.log_input(serialized, &value, ptr);
                    // TODO: it may be possible to not own this value while deserializing
                    //       under certain circumstances (e.g. when the cotnract mem is kept alive)
                    value.map(|r| r.into_owned())
                }
                _ => unreachable!(),
            }
        }

        pub unsafe fn unwrap_summarize_state(
            self,
            mem: WasmLinearMem,
        ) -> Result<StateSummary<'static>, ContractError> {
            let kind = ResultKind::from(self.kind);
            match kind {
                ResultKind::SummarizeState => {
                    let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
                    let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                    let value: Result<StateSummary<'static>, ContractError> =
                        bincode::deserialize(serialized)
                            .map_err(|e| ContractError::Other(format!("{e}")))?;
                    #[cfg(feature = "trace")]
                    self.log_input(serialized, &value, ptr);
                    // TODO: it may be possible to not own this value while deserializing
                    //       under certain circumstances (e.g. when the contract mem is kept alive)
                    value.map(|s| StateSummary::from(s.into_bytes()))
                }
                _ => unreachable!(),
            }
        }

        pub unsafe fn unwrap_get_state_delta(
            self,
            mem: WasmLinearMem,
        ) -> Result<StateDelta<'static>, ContractError> {
            let kind = ResultKind::from(self.kind);
            match kind {
                ResultKind::StateDelta => {
                    let ptr = crate::buf::compute_ptr(self.ptr as *mut u8, &mem);
                    let serialized = std::slice::from_raw_parts(ptr as *const u8, self.size as _);
                    let value: Result<StateDelta<'static>, ContractError> =
                        bincode::deserialize(serialized)
                            .map_err(|e| ContractError::Other(format!("{e}")))?;
                    #[cfg(feature = "trace")]
                    self.log_input(serialized, &value, ptr);
                    // TODO: it may be possible to not own this value while deserializing
                    //       under certain circumstances (e.g. when the contract mem is kept alive)
                    value.map(|d| StateDelta::from(d.into_bytes()))
                }
                _ => unreachable!(),
            }
        }

        pub fn into_raw(self) -> i64 {
            #[cfg(feature = "trace")]
            {
                tracing::trace!("returning FFI -> {self:?}");
            }
            let ptr = Box::into_raw(Box::new(self));
            #[cfg(feature = "trace")]
            {
                tracing::trace!("FFI result ptr: {ptr:p} ({}i64)", ptr as i64);
            }
            ptr as _
        }

        pub unsafe fn from_raw(ptr: i64, mem: &WasmLinearMem) -> Self {
            let result = Box::leak(Box::from_raw(crate::buf::compute_ptr(
                ptr as *mut Self,
                mem,
            )));
            #[cfg(feature = "trace")]
            {
                tracing::trace!(
                    "got FFI result @ {ptr} ({:p}) -> {result:?}",
                    ptr as *mut Self
                );
            }
            *result
        }

        #[cfg(feature = "trace")]
        fn log_input<T: std::fmt::Debug>(&self, serialized: &[u8], value: &T, ptr: *mut u8) {
            tracing::trace!(
                "got result through FFI; addr: {:p} ({}i64, mapped: {ptr:p})
                 serialized: {serialized:?}
                 value: {value:?}",
                self.ptr as *mut u8,
                self.ptr
            );
        }
    }

    macro_rules! conversion {
        ($value:ty: $kind:expr) => {
            impl From<$value> for ContractInterfaceResult {
                fn from(value: $value) -> Self {
                    let kind = $kind as i32;
                    // TODO: research if there is a safe way to just transmute the pointer in memory
                    //       independently of the architecture when stored in WASM and accessed from
                    //       the host, maybe even if is just for some architectures
                    let serialized = bincode::serialize(&value).unwrap();
                    let size = serialized.len() as _;
                    let ptr = serialized.as_ptr();
                    #[cfg(feature = "trace")] {
                        tracing::trace!(
                            "sending result through FFI; addr: {ptr:p} ({}),\n  serialized: {serialized:?}\n  value: {value:?}",
                            ptr as i64
                        );
                    }
                    std::mem::forget(serialized);
                    Self { kind, ptr: ptr as i64, size }
                }
            }
        };
    }

    conversion!(Result<ValidateResult, ContractError>: ResultKind::ValidateState);
    conversion!(Result<bool, ContractError>: ResultKind::ValidateDelta);
    conversion!(Result<UpdateModification<'static>, ContractError>: ResultKind::UpdateState);
    conversion!(Result<StateSummary<'static>, ContractError>: ResultKind::SummarizeState);
    conversion!(Result<StateDelta<'static>, ContractError>: ResultKind::StateDelta);
}

#[cfg(all(test, target_family = "unix"))]
mod test {
    use super::*;
    use once_cell::sync::Lazy;
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    static RND_BYTES: Lazy<[u8; 1024]> = Lazy::new(|| {
        let mut bytes = [0; 1024];
        let mut rng = SmallRng::from_entropy();
        rng.fill(&mut bytes);
        bytes
    });

    #[test]
    fn key_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let code = ContractCode::from(vec![1, 2, 3]);
        let expected = ContractKey::from((Parameters::from(vec![]), &code));
        // let encoded_key = expected.encode();
        // println!("encoded key: {encoded_key}");
        // let encoded_code = expected.contract_part_as_str();
        // println!("encoded key: {encoded_code}");

        let decoded = ContractKey::from_params(code.hash_str(), [].as_ref().into())?;
        assert_eq!(expected, decoded);
        assert_eq!(expected.code_hash(), decoded.code_hash());
        Ok(())
    }

    #[test]
    fn key_ser() -> Result<(), Box<dyn std::error::Error>> {
        let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
        let expected: ContractKey = gen.arbitrary()?;
        let encoded = bs58::encode(expected.bytes()).into_string();
        // println!("encoded key: {encoded}");

        let serialized = bincode::serialize(&expected)?;
        let deserialized: ContractKey = bincode::deserialize(&serialized)?;
        let decoded = bs58::encode(deserialized.bytes()).into_string();
        assert_eq!(encoded, decoded);
        assert_eq!(deserialized, expected);
        Ok(())
    }

    #[test]
    fn contract_ser() -> Result<(), Box<dyn std::error::Error>> {
        let mut gen = arbitrary::Unstructured::new(&*RND_BYTES);
        let expected: Contract = gen.arbitrary()?;

        let serialized = bincode::serialize(&expected)?;
        let deserialized: Contract = bincode::deserialize(&serialized)?;
        assert_eq!(deserialized, expected);
        Ok(())
    }
}
