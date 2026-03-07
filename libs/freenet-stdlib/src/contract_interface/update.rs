//! Contract update mechanisms and related contract management.
//!
//! This module provides types for updating contract state, managing related contracts,
//! and validation results.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::client_api::{TryFromFbs, WsApiError};
use crate::common_generated::common::{UpdateData as FbsUpdateData, UpdateDataType};
use crate::generated::client_request::RelatedContracts as FbsRelatedContracts;

use super::{ContractError, ContractInstanceId, State, StateDelta, CONTRACT_KEY_SIZE};

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
