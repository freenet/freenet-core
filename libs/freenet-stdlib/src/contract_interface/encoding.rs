//! Helper types for interaction between wasm and host boundaries.
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

use serde::{de::DeserializeOwned, Serialize};

use super::*;
use crate::parameters::Parameters;

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
        let mut contracts = HashMap::new();
        for (id, state) in found.states() {
            match state {
                Some(state) => {
                    contracts.insert(*id, state.clone());
                }
                None => {
                    not_found.insert(*id);
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
    ) -> Result<Related<C>, <<C as EncodingAdapter>::SelfEncoder as Encoder<C>>::Error> {
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
    ContractError: From<
        <<T as EncodingAdapter>::SummaryEncoder as Encoder<<T as EncodingAdapter>::Summary>>::Error,
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
    ContractError: From<
        <<T as EncodingAdapter>::SummaryEncoder as Encoder<<T as EncodingAdapter>::Summary>>::Error,
    >,
    ContractError: From<
        <<T as EncodingAdapter>::DeltaEncoder as Encoder<<T as EncodingAdapter>::Delta>>::Error,
    >,
{
    let typed_params =
        <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?;
    let typed_state = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
    let typed_summary = <<T as EncodingAdapter>::SummaryEncoder>::deserialize(summary.as_ref())?;
    let summary = typed_state.delta(typed_params, typed_summary)?;
    let encoded = <<T as EncodingAdapter>::DeltaEncoder>::serialize(&summary)?;
    Ok(encoded.into())
}
