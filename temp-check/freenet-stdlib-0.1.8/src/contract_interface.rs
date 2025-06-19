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

use blake3::{traits::digest::Digest, Hasher as Blake3};
use byteorder::{LittleEndian, ReadBytesExt};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::serde_as;

use crate::client_api::TryFromFbs;
use crate::common_generated::common::{
    ContractKey as FbsContractKey, UpdateData as FbsUpdateData, UpdateDataType,
};
use crate::generated::client_request::RelatedContracts as FbsRelatedContracts;
use crate::{client_api::WsApiError, code_hash::CodeHash, parameters::Parameters};

pub(crate) const CONTRACT_KEY_SIZE: usize = 32;

/// Type of errors during interaction with a contract.
#[derive(Debug, thiserror::Error, Serialize, Deserialize)]
pub enum ContractError {
    #[error("de/serialization error: {0}")]
    Deser(String),
    #[error("invalid contract update")]
    InvalidUpdate,
    #[error("invalid contract update, reason: {reason}")]
    InvalidUpdateWithInfo { reason: String },
    #[error("trying to read an invalid state")]
    InvalidState,
    #[error("trying to read an invalid delta")]
    InvalidDelta,
    #[error("{0}")]
    Other(String),
}

/// An update to a contract state or any required related contracts to update that state.
// todo: this should be an enum probably
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

    /// Unwraps self returning a [`State`].
    ///
    /// Panics if self does not contain a state.
    pub fn unwrap_valid(self) -> State<'a> {
        match self.new_state {
            Some(s) => s,
            _ => panic!("failed unwrapping state in modification"),
        }
    }
}

impl UpdateModification<'_> {
    /// Constructor for self when this contract still is missing some [`RelatedContract`]
    /// to proceed with any verification or updates.
    pub fn requires(related: Vec<RelatedContract>) -> Result<Self, ContractError> {
        if related.is_empty() {
            return Err(ContractError::InvalidUpdateWithInfo {
                reason: "At least one related contract is required".into(),
            });
        }
        Ok(Self {
            new_state: None,
            related,
        })
    }

    /// Gets the pending related contracts.
    pub fn get_related(&self) -> &[RelatedContract] {
        &self.related
    }

    /// Copies the data if not owned and returns an owned version of self.
    pub fn into_owned(self) -> UpdateModification<'static> {
        let Self { new_state, related } = self;
        UpdateModification {
            new_state: new_state.map(State::into_owned),
            related,
        }
    }

    pub fn requires_dependencies(&self) -> bool {
        !self.related.is_empty()
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

    pub fn deser_related_contracts<'de, D>(deser: D) -> Result<RelatedContracts<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <RelatedContracts as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }
}

impl RelatedContracts<'static> {
    pub fn states(&self) -> impl Iterator<Item = (&ContractInstanceId, &Option<State<'static>>)> {
        self.map.iter()
    }
}

impl<'a> RelatedContracts<'a> {
    pub fn update(
        &mut self,
    ) -> impl Iterator<Item = (&ContractInstanceId, &mut Option<State<'a>>)> + '_ {
        self.map.iter_mut()
    }

    pub fn missing(&mut self, contracts: Vec<ContractInstanceId>) {
        for key in contracts {
            self.map.entry(key).or_default();
        }
    }
}

impl<'a> TryFromFbs<&FbsRelatedContracts<'a>> for RelatedContracts<'a> {
    fn try_decode_fbs(related_contracts: &FbsRelatedContracts<'a>) -> Result<Self, WsApiError> {
        let mut map = HashMap::with_capacity(related_contracts.contracts().len());
        for related in related_contracts.contracts().iter() {
            let id = ContractInstanceId::from_bytes(related.instance_id().data().bytes()).unwrap();
            let state = State::from(related.state().bytes());
            map.insert(id, Some(state));
        }
        Ok(RelatedContracts::from(map))
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

    pub(crate) fn get_self_states<'a>(
        updates: &[UpdateData<'a>],
    ) -> Vec<(Option<State<'a>>, Option<StateDelta<'a>>)> {
        let mut own_states = Vec::with_capacity(updates.len());
        for update in updates {
            match update {
                UpdateData::State(state) => own_states.push((Some(state.clone()), None)),
                UpdateData::Delta(delta) => own_states.push((None, Some(delta.clone()))),
                UpdateData::StateAndDelta { state, delta } => {
                    own_states.push((Some(state.clone()), Some(delta.clone())))
                }
                _ => {}
            }
        }
        own_states
    }

    pub(crate) fn deser_update_data<'de, D>(deser: D) -> Result<UpdateData<'static>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <UpdateData as Deserialize>::deserialize(deser)?;
        Ok(value.into_owned())
    }
}

impl<'a> From<StateDelta<'a>> for UpdateData<'a> {
    fn from(delta: StateDelta<'a>) -> Self {
        UpdateData::Delta(delta)
    }
}

impl<'a> TryFromFbs<&FbsUpdateData<'a>> for UpdateData<'a> {
    fn try_decode_fbs(update_data: &FbsUpdateData<'a>) -> Result<Self, WsApiError> {
        match update_data.update_data_type() {
            UpdateDataType::StateUpdate => {
                let update = update_data.update_data_as_state_update().unwrap();
                let state = State::from(update.state().bytes());
                Ok(UpdateData::State(state))
            }
            UpdateDataType::DeltaUpdate => {
                let update = update_data.update_data_as_delta_update().unwrap();
                let delta = StateDelta::from(update.delta().bytes());
                Ok(UpdateData::Delta(delta))
            }
            UpdateDataType::StateAndDeltaUpdate => {
                let update = update_data.update_data_as_state_and_delta_update().unwrap();
                let state = State::from(update.state().bytes());
                let delta = StateDelta::from(update.delta().bytes());
                Ok(UpdateData::StateAndDelta { state, delta })
            }
            UpdateDataType::RelatedStateUpdate => {
                let update = update_data.update_data_as_related_state_update().unwrap();
                let state = State::from(update.state().bytes());
                let related_to =
                    ContractInstanceId::from_bytes(update.related_to().data().bytes()).unwrap();
                Ok(UpdateData::RelatedState { related_to, state })
            }
            UpdateDataType::RelatedDeltaUpdate => {
                let update = update_data.update_data_as_related_delta_update().unwrap();
                let delta = StateDelta::from(update.delta().bytes());
                let related_to =
                    ContractInstanceId::from_bytes(update.related_to().data().bytes()).unwrap();
                Ok(UpdateData::RelatedDelta { related_to, delta })
            }
            UpdateDataType::RelatedStateAndDeltaUpdate => {
                let update = update_data
                    .update_data_as_related_state_and_delta_update()
                    .unwrap();
                let state = State::from(update.state().bytes());
                let delta = StateDelta::from(update.delta().bytes());
                let related_to =
                    ContractInstanceId::from_bytes(update.related_to().data().bytes()).unwrap();
                Ok(UpdateData::RelatedStateAndDelta {
                    related_to,
                    state,
                    delta,
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
/// # use freenet_stdlib::prelude::*;
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
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
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
        let key = ContractKey::from_params_and_code(&parameters, &contract);
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

        let key = ContractKey::from_params_and_code(&parameters, &contract);

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

/// Data associated with a contract that can be retrieved by Applications and Delegates.
///
/// For efficiency and flexibility, contract state is represented as a simple [u8] byte array.
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

/// The executable contract.
///
/// It is the part of the executable belonging to the full specification
/// and does not include any other metadata (like the parameters).
#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct ContractCode<'a> {
    // TODO: conver this to Arc<[u8]> instead
    #[serde_as(as = "serde_with::Bytes")]
    #[serde(borrow)]
    pub(crate) data: Cow<'a, [u8]>,
    // todo: skip serializing and instead compute it
    pub(crate) code_hash: CodeHash,
}

impl ContractCode<'static> {
    /// Loads the contract raw wasm module, without any version.
    pub fn load_raw(path: &Path) -> Result<Self, std::io::Error> {
        let contract_data = Self::load_bytes(path)?;
        Ok(ContractCode::from(contract_data))
    }

    pub(crate) fn load_bytes(path: &Path) -> Result<Vec<u8>, std::io::Error> {
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
    /// Contract code hash.
    pub fn hash(&self) -> &CodeHash {
        &self.code_hash
    }

    /// Returns the `Base58` string representation of the contract key.
    pub fn hash_str(&self) -> String {
        Self::encode_hash(&self.code_hash.0)
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
            code_hash: self.code_hash,
        }
    }

    fn gen_hash(data: &[u8]) -> CodeHash {
        let mut hasher = Blake3::new();
        hasher.update(data);
        let key_arr = hasher.finalize();
        debug_assert_eq!(key_arr[..].len(), CONTRACT_KEY_SIZE);
        let mut key = [0; CONTRACT_KEY_SIZE];
        key.copy_from_slice(&key_arr);
        CodeHash(key)
    }
}

impl From<Vec<u8>> for ContractCode<'static> {
    fn from(data: Vec<u8>) -> Self {
        let key = ContractCode::gen_hash(&data);
        ContractCode {
            data: Cow::from(data),
            code_hash: key,
        }
    }
}

impl<'a> From<&'a [u8]> for ContractCode<'a> {
    fn from(data: &'a [u8]) -> ContractCode<'a> {
        let hash = ContractCode::gen_hash(data);
        ContractCode {
            data: Cow::from(data),
            code_hash: hash,
        }
    }
}

impl PartialEq for ContractCode<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.code_hash == other.code_hash
    }
}

impl Eq for ContractCode<'_> {}

impl std::fmt::Display for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract( key: ")?;
        internal_fmt_key(&self.code_hash.0, f)?;
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

impl std::fmt::Debug for ContractCode<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContractCode")
            .field("hash", &self.code_hash);
        Ok(())
    }
}

/// The key representing the hash of the contract executable code hash and a set of `parameters`.
#[serde_as]
#[derive(PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Hash)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
#[repr(transparent)]
pub struct ContractInstanceId(#[serde_as(as = "[_; CONTRACT_KEY_SIZE]")] [u8; CONTRACT_KEY_SIZE]);

impl ContractInstanceId {
    pub fn from_params_and_code<'a>(
        params: impl Borrow<Parameters<'a>>,
        code: impl Borrow<ContractCode<'a>>,
    ) -> Self {
        generate_id(params.borrow(), code.borrow())
    }

    pub const fn new(key: [u8; CONTRACT_KEY_SIZE]) -> Self {
        Self(key)
    }

    /// `Base58` string representation of the `contract id`.
    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Build `ContractId` from the binary representation.
    pub fn from_bytes(bytes: impl AsRef<[u8]>) -> Result<Self, bs58::decode::Error> {
        let mut spec = [0; CONTRACT_KEY_SIZE];
        bs58::decode(bytes)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut spec)?;
        Ok(Self(spec))
    }
}

impl Deref for ContractInstanceId {
    type Target = [u8; CONTRACT_KEY_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
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

impl Display for ContractInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encode())
    }
}

impl std::fmt::Debug for ContractInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ContractInstanceId")
            .field(&self.encode())
            .finish()
    }
}

/// A complete key specification, that represents a cryptographic hash that identifies the contract.
#[serde_as]
#[derive(Debug, Eq, Copy, Clone, Serialize, Deserialize)]
#[cfg_attr(
    any(feature = "testing", all(test, any(unix, windows))),
    derive(arbitrary::Arbitrary)
)]
pub struct ContractKey {
    instance: ContractInstanceId,
    code: Option<CodeHash>,
}

impl ContractKey {
    pub fn from_params_and_code<'a>(
        params: impl Borrow<Parameters<'a>>,
        wasm_code: impl Borrow<ContractCode<'a>>,
    ) -> Self {
        let code = wasm_code.borrow();
        let id = generate_id(params.borrow(), code);
        let code_hash = code.hash();
        Self {
            instance: id,
            code: Some(*code_hash),
        }
    }

    /// Builds a partial [`ContractKey`](ContractKey), the contract code part is unspecified.
    pub fn from_id(instance: impl Into<String>) -> Result<Self, bs58::decode::Error> {
        let instance = ContractInstanceId::try_from(instance.into())?;
        Ok(Self {
            instance,
            code: None,
        })
    }

    /// Gets the whole spec key hash.
    pub fn as_bytes(&self) -> &[u8] {
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
            .onto(&mut code_key)?;

        let mut hasher = Blake3::new();
        hasher.update(code_key.as_slice());
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

    pub fn id(&self) -> &ContractInstanceId {
        &self.instance
    }
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

impl Deref for ContractKey {
    type Target = [u8; CONTRACT_KEY_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.instance.0
    }
}

impl std::fmt::Display for ContractKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.instance.fmt(f)
    }
}

impl<'a> TryFromFbs<&FbsContractKey<'a>> for ContractKey {
    fn try_decode_fbs(key: &FbsContractKey<'a>) -> Result<Self, WsApiError> {
        let key_bytes: [u8; CONTRACT_KEY_SIZE] = key.instance().data().bytes().try_into().unwrap();
        let instance = ContractInstanceId::new(key_bytes);
        let code = key
            .code()
            .map(|code_hash| CodeHash::from_code(code_hash.bytes()));
        Ok(ContractKey { instance, code })
    }
}

fn generate_id<'a>(
    parameters: &Parameters<'a>,
    code_data: &ContractCode<'a>,
) -> ContractInstanceId {
    let contract_hash = code_data.hash();

    let mut hasher = Blake3::new();
    hasher.update(contract_hash.0.as_slice());
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
        self.key.fmt(f)
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

#[doc(hidden)]
pub(crate) mod wasm_interface {
    //! Contains all the types to interface between the host environment and
    //! the wasm module execution.
    use super::*;
    use crate::memory::WasmLinearMem;

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
                    let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
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
                    let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
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
                    let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
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
                    let ptr = crate::memory::buf::compute_ptr(self.ptr as *mut u8, &mem);
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

        #[cfg(feature = "contract")]
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
            let result = Box::leak(Box::from_raw(crate::memory::buf::compute_ptr(
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

    #[cfg(feature = "contract")]
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

    #[cfg(feature = "contract")]
    conversion!(Result<ValidateResult, ContractError>: ResultKind::ValidateState);
    #[cfg(feature = "contract")]
    conversion!(Result<bool, ContractError>: ResultKind::ValidateDelta);
    #[cfg(feature = "contract")]
    conversion!(Result<UpdateModification<'static>, ContractError>: ResultKind::UpdateState);
    #[cfg(feature = "contract")]
    conversion!(Result<StateSummary<'static>, ContractError>: ResultKind::SummarizeState);
    #[cfg(feature = "contract")]
    conversion!(Result<StateDelta<'static>, ContractError>: ResultKind::StateDelta);
}

#[cfg(all(test, any(unix, windows)))]
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
        let expected = ContractKey::from_params_and_code(Parameters::from(vec![]), &code);
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
        let encoded = bs58::encode(expected.as_bytes()).into_string();
        // println!("encoded key: {encoded}");

        let serialized = bincode::serialize(&expected)?;
        let deserialized: ContractKey = bincode::deserialize(&serialized)?;
        let decoded = bs58::encode(deserialized.as_bytes()).into_string();
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

pub mod encoding {
    //! Helper types for interaction between wasm and host boundaries.
    use std::{collections::HashSet, marker::PhantomData};

    use serde::de::DeserializeOwned;

    use super::*;

    pub enum MergeResult {
        Success,
        RequestRelated(RelatedContractsContainer),
        Error(ContractError),
    }

    #[derive(Default)]
    pub struct RelatedContractsContainer {
        contracts: HashMap<ContractInstanceId, State<'static>>,
        pending: HashSet<ContractInstanceId>,
        not_found: HashSet<ContractInstanceId>,
    }

    impl From<RelatedContracts<'static>> for RelatedContractsContainer {
        fn from(found: RelatedContracts<'static>) -> Self {
            let mut not_found = HashSet::new();
            let mut contracts = HashMap::with_capacity(found.map.len());
            for (id, state) in found.map.into_iter() {
                match state {
                    Some(state) => {
                        contracts.insert(id, state);
                    }
                    None => {
                        not_found.insert(id);
                    }
                }
            }
            RelatedContractsContainer {
                contracts,
                pending: HashSet::new(),
                not_found,
            }
        }
    }

    impl From<RelatedContractsContainer> for Vec<crate::contract_interface::RelatedContract> {
        fn from(related: RelatedContractsContainer) -> Self {
            related
                .pending
                .into_iter()
                .map(|id| RelatedContract {
                    contract_instance_id: id,
                    mode: RelatedMode::StateOnce,
                })
                .collect()
        }
    }

    impl From<Vec<UpdateData<'static>>> for RelatedContractsContainer {
        fn from(updates: Vec<UpdateData<'static>>) -> Self {
            let mut this = RelatedContractsContainer::default();
            for update in updates {
                match update {
                    UpdateData::RelatedState { related_to, state } => {
                        this.contracts.insert(related_to, state);
                    }
                    UpdateData::RelatedStateAndDelta {
                        related_to, state, ..
                    } => {
                        this.contracts.insert(related_to, state);
                    }
                    _ => {}
                }
            }
            this
        }
    }

    impl RelatedContractsContainer {
        pub fn get<C: TypedContract>(
            &self,
            params: &C::Parameters,
        ) -> Result<Related<C>, <<C as EncodingAdapter>::SelfEncoder as Encoder<C>>::Error>
        {
            let id = <C as TypedContract>::instance_id(params);
            if let Some(res) = self.contracts.get(&id) {
                match <<C as EncodingAdapter>::SelfEncoder>::deserialize(res.as_ref()) {
                    Ok(state) => return Ok(Related::Found { state }),
                    Err(err) => return Err(err),
                }
            }
            if self.pending.contains(&id) {
                return Ok(Related::RequestPending);
            }
            if self.not_found.contains(&id) {
                return Ok(Related::NotFound);
            }
            Ok(Related::NotRequested)
        }

        pub fn request<C: TypedContract>(&mut self, id: ContractInstanceId) {
            self.pending.insert(id);
        }

        pub fn merge(&mut self, other: Self) {
            let Self {
                contracts,
                pending,
                not_found,
            } = other;
            self.pending.extend(pending);
            self.not_found.extend(not_found);
            self.contracts.extend(contracts);
        }
    }

    pub enum Related<C: TypedContract> {
        /// The state was previously requested and found
        Found { state: C },
        /// The state was previously requested but not found
        NotFound,
        /// The state was previously requested but request is still in flight
        RequestPending,
        /// The state was not previously requested, this enum can be included
        /// in the MergeResult return value which will request it
        NotRequested,
    }

    /// A contract state and it's associated types which can be encoded and decoded
    /// via an specific encoder.
    pub trait EncodingAdapter
    where
        Self: Sized,
    {
        type Parameters;
        type Delta;
        type Summary;

        type SelfEncoder: Encoder<Self>;
        type ParametersEncoder: Encoder<Self::Parameters>;
        type DeltaEncoder: Encoder<Self::Delta>;
        type SummaryEncoder: Encoder<Self::Summary>;
    }

    pub enum TypedUpdateData<T: EncodingAdapter> {
        RelatedState { state: T },
        RelatedDelta { delta: T::Delta },
        RelatedStateAndDelta { state: T, delta: T::Delta },
    }

    impl<T: EncodingAdapter> TypedUpdateData<T> {
        pub fn from_other<Parent>(value: &TypedUpdateData<Parent>) -> Self
        where
            Parent: EncodingAdapter,
            T: for<'x> From<&'x Parent>,
            T::Delta: for<'x> From<&'x Parent::Delta>,
        {
            match value {
                TypedUpdateData::RelatedState { state } => {
                    let state = T::from(state);
                    TypedUpdateData::RelatedState { state }
                }
                TypedUpdateData::RelatedDelta { delta } => {
                    let delta: T::Delta = <T as EncodingAdapter>::Delta::from(delta);
                    TypedUpdateData::RelatedDelta { delta }
                }
                TypedUpdateData::RelatedStateAndDelta { state, delta } => {
                    let state = T::from(state);
                    let delta: T::Delta = <T as EncodingAdapter>::Delta::from(delta);
                    TypedUpdateData::RelatedStateAndDelta { state, delta }
                }
            }
        }
    }

    impl<T: EncodingAdapter> TryFrom<(Option<T>, Option<T::Delta>)> for TypedUpdateData<T> {
        type Error = ContractError;
        fn try_from((state, delta): (Option<T>, Option<T::Delta>)) -> Result<Self, Self::Error> {
            match (state, delta) {
                (None, None) => Err(ContractError::InvalidState),
                (None, Some(delta)) => Ok(Self::RelatedDelta { delta }),
                (Some(state), None) => Ok(Self::RelatedState { state }),
                (Some(state), Some(delta)) => Ok(Self::RelatedStateAndDelta { state, delta }),
            }
        }
    }

    pub trait TypedContract: EncodingAdapter {
        fn instance_id(params: &Self::Parameters) -> ContractInstanceId;

        fn verify(
            &self,
            parameters: Self::Parameters,
            related: RelatedContractsContainer,
        ) -> Result<ValidateResult, ContractError>;

        fn merge(
            &mut self,
            parameters: &Self::Parameters,
            update: TypedUpdateData<Self>,
            related: &RelatedContractsContainer,
        ) -> MergeResult;

        fn summarize(&self, parameters: Self::Parameters) -> Result<Self::Summary, ContractError>;

        fn delta(
            &self,
            parameters: Self::Parameters,
            summary: Self::Summary,
        ) -> Result<Self::Delta, ContractError>;
    }

    pub trait Encoder<T> {
        type Error: Into<ContractError>;
        fn deserialize(bytes: &[u8]) -> Result<T, Self::Error>;
        fn serialize(value: &T) -> Result<Vec<u8>, Self::Error>;
    }

    pub struct JsonEncoder<T>(PhantomData<T>);

    impl<T> Encoder<T> for JsonEncoder<T>
    where
        T: DeserializeOwned + Serialize,
    {
        type Error = serde_json::Error;

        fn deserialize(bytes: &[u8]) -> Result<T, Self::Error> {
            serde_json::from_slice(bytes)
        }

        fn serialize(value: &T) -> Result<Vec<u8>, Self::Error> {
            serde_json::to_vec(value)
        }
    }

    impl From<serde_json::Error> for ContractError {
        fn from(value: serde_json::Error) -> Self {
            ContractError::Deser(format!("{value}"))
        }
    }

    pub struct BincodeEncoder<T>(PhantomData<T>);

    impl<T> Encoder<T> for BincodeEncoder<T>
    where
        T: DeserializeOwned + Serialize,
    {
        type Error = bincode::Error;

        fn deserialize(bytes: &[u8]) -> Result<T, Self::Error> {
            bincode::deserialize(bytes)
        }

        fn serialize(value: &T) -> Result<Vec<u8>, Self::Error> {
            bincode::serialize(value)
        }
    }

    impl From<bincode::Error> for ContractError {
        fn from(value: bincode::Error) -> Self {
            ContractError::Deser(format!("{value}"))
        }
    }

    pub fn inner_validate_state<T>(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>
    where
        T: EncodingAdapter + TypedContract,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?;
        let typed_state = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let related_container = RelatedContractsContainer::from(related);
        typed_state.verify(typed_params, related_container)
    }

    pub fn inner_update_state<T>(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError>
    where
        T: EncodingAdapter + TypedContract,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError: From<
            <<T as EncodingAdapter>::DeltaEncoder as Encoder<<T as EncodingAdapter>::Delta>>::Error,
        >,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?;
        let mut typed_state = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let self_updates = UpdateData::get_self_states(&data);
        let related_container = RelatedContractsContainer::from(data);
        for (state, delta) in self_updates {
            let state = state
                .map(|s| <<T as EncodingAdapter>::SelfEncoder>::deserialize(s.as_ref()))
                .transpose()?;
            let delta = delta
                .map(|d| <<T as EncodingAdapter>::DeltaEncoder>::deserialize(d.as_ref()))
                .transpose()?;
            let typed_update = TypedUpdateData::try_from((state, delta))?;
            match typed_state.merge(&typed_params, typed_update, &related_container) {
                MergeResult::Success => {}
                MergeResult::RequestRelated(req) => {
                    return UpdateModification::requires(req.into());
                }
                MergeResult::Error(err) => return Err(err),
            }
        }
        let encoded = <<T as EncodingAdapter>::SelfEncoder>::serialize(&typed_state)?;
        Ok(UpdateModification::valid(encoded.into()))
    }

    pub fn inner_summarize_state<T>(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError>
    where
        T: EncodingAdapter + TypedContract,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError:
            From<
                <<T as EncodingAdapter>::SummaryEncoder as Encoder<
                    <T as EncodingAdapter>::Summary,
                >>::Error,
            >,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?;
        let typed_state = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let summary = typed_state.summarize(typed_params)?;
        let encoded = <<T as EncodingAdapter>::SummaryEncoder>::serialize(&summary)?;
        Ok(encoded.into())
    }

    pub fn inner_state_delta<T>(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError>
    where
        T: EncodingAdapter + TypedContract,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError:
            From<
                <<T as EncodingAdapter>::SummaryEncoder as Encoder<
                    <T as EncodingAdapter>::Summary,
                >>::Error,
            >,
        ContractError: From<
            <<T as EncodingAdapter>::DeltaEncoder as Encoder<<T as EncodingAdapter>::Delta>>::Error,
        >,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?;
        let typed_state = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let typed_summary =
            <<T as EncodingAdapter>::SummaryEncoder>::deserialize(summary.as_ref())?;
        let summary = typed_state.delta(typed_params, typed_summary)?;
        let encoded = <<T as EncodingAdapter>::DeltaEncoder>::serialize(&summary)?;
        Ok(encoded.into())
    }
}
