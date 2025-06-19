use crate::{
    contract_interface::{
        ContractError, ContractInstanceId, RelatedContracts, State, UpdateData, ValidateResult,
    },
    typed_contract::{MergeResult, RelatedContractsContainer},
};

impl<'a> From<&'a State<'static>> for State<'static> {
    fn from(value: &'a State<'static>) -> Self {
        value.clone()
    }
}

pub trait ComponentParameter {
    fn contract_id(&self) -> Option<ContractInstanceId>;
}

pub trait Mergeable<Other> {
    fn merge(&mut self, other: Other);
}

pub trait ContractComponent: std::any::Any + Sized {
    type Context;
    type Parameters: ComponentParameter;
    type Delta;
    type Summary;

    /// Corresponds to ContractInterface `validate_state`
    fn verify<Child, Ctx>(
        &self,
        parameters: &Self::Parameters,
        context: &Ctx,
        related: &RelatedContractsContainer,
    ) -> Result<ValidateResult, ContractError>
    where
        Child: ContractComponent,
        Self::Context: for<'x> From<&'x Ctx>;

    /// Corresponds to ContractInterface `update_state`
    fn merge(
        &mut self,
        parameters: &Self::Parameters,
        update: &TypedUpdateData<Self>,
        related: &RelatedContractsContainer,
    ) -> MergeResult;

    /// Corresponds to ContractInterface `summarize`
    fn summarize<ParentSummary>(
        &self,
        parameters: &Self::Parameters,
        summary: &mut ParentSummary,
    ) -> Result<(), ContractError>
    where
        ParentSummary: Mergeable<<Self as ContractComponent>::Summary>;

    /// Corresponds to ContractInterface `delta`
    fn delta(
        &self,
        parameters: &Self::Parameters,
        summary: &Self::Summary,
    ) -> Result<Self::Delta, ContractError>;
}

pub enum TypedUpdateData<T: ContractComponent> {
    RelatedState { state: T },
    RelatedDelta { delta: T::Delta },
    RelatedStateAndDelta { state: T, delta: T::Delta },
}

impl<T: ContractComponent> TypedUpdateData<T> {
    pub fn from_other<Parent>(value: &TypedUpdateData<Parent>) -> Self
    where
        Parent: ContractComponent,
        <T as ContractComponent>::Delta: for<'x> From<&'x Parent::Delta>,
        T: for<'x> From<&'x Parent>,
    {
        match value {
            TypedUpdateData::RelatedState { state } => {
                let state = T::from(state);
                TypedUpdateData::RelatedState { state }
            }
            TypedUpdateData::RelatedDelta { delta } => {
                let delta: T::Delta = <T as ContractComponent>::Delta::from(delta);
                TypedUpdateData::RelatedDelta { delta }
            }
            TypedUpdateData::RelatedStateAndDelta { state, delta } => {
                let state = T::from(state);
                let delta: T::Delta = <T as ContractComponent>::Delta::from(delta);
                TypedUpdateData::RelatedStateAndDelta { state, delta }
            }
        }
    }
}

impl<T: ContractComponent> TryFrom<(Option<T>, Option<T::Delta>)> for TypedUpdateData<T> {
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

#[allow(unused)]
impl<T> ContractComponent for Vec<T>
where
    T: ContractComponent + Clone,
    <T as ContractComponent>::Delta: Clone + Mergeable<<T as ContractComponent>::Delta>,
{
    type Context = T::Context;
    type Parameters = T::Parameters;
    type Delta = T::Delta;
    type Summary = T::Summary;

    fn verify<Child, Ctx>(
        &self,
        parameters: &Self::Parameters,
        context: &Ctx,
        related: &RelatedContractsContainer,
    ) -> Result<ValidateResult, ContractError>
    where
        Child: ContractComponent,
        Self::Context: for<'x> From<&'x Ctx>,
    {
        for v in self {
            match v.verify::<Child, Ctx>(parameters, context, related)? {
                ValidateResult::Invalid => return Ok(ValidateResult::Invalid),
                ValidateResult::RequestRelated(related) => {
                    return Ok(ValidateResult::RequestRelated(related))
                }
                ValidateResult::Valid => {}
            }
        }
        Ok(ValidateResult::Valid)
    }

    fn merge(
        &mut self,
        parameters: &Self::Parameters,
        update: &TypedUpdateData<Self>,
        related: &RelatedContractsContainer,
    ) -> MergeResult {
        for v in self {
            let update = match update {
                TypedUpdateData::RelatedState { state } => TypedUpdateData::RelatedState {
                    state: state[0].clone(),
                },
                TypedUpdateData::RelatedDelta { delta } => TypedUpdateData::RelatedDelta {
                    delta: delta.clone(),
                },
                TypedUpdateData::RelatedStateAndDelta { state, delta } => {
                    TypedUpdateData::RelatedStateAndDelta {
                        state: state[0].clone(),
                        delta: delta.clone(),
                    }
                }
            };
            match v.merge(parameters, &update, related) {
                MergeResult::RequestRelated(req) => return MergeResult::RequestRelated(req),
                MergeResult::Error(err) => return MergeResult::Error(err),
                MergeResult::Success => {}
            }
        }
        MergeResult::Success
    }

    fn summarize<ParentSummary>(
        &self,
        parameters: &Self::Parameters,
        summary: &mut ParentSummary,
    ) -> Result<(), ContractError>
    where
        ParentSummary: Mergeable<<Self as ContractComponent>::Summary>,
    {
        for v in self {
            v.summarize(parameters, summary);
        }
        Ok(())
    }

    fn delta(
        &self,
        parameters: &Self::Parameters,
        summary: &Self::Summary,
    ) -> Result<Self::Delta, ContractError> {
        let mut delta: Option<<T as ContractComponent>::Delta> = None;
        for v in self {
            let other_delta = v.delta(parameters, summary)?;
            if let Some(delta) = &mut delta {
                delta.merge(other_delta);
            } else {
                delta = Some(other_delta)
            }
        }
        delta.ok_or(ContractError::InvalidDelta)
    }
}

pub struct NoContext;

impl<'x, T> From<&'x T> for NoContext {
    fn from(_: &'x T) -> Self {
        NoContext
    }
}

pub mod from_bytes {
    use serde::de::DeserializeOwned;

    use crate::{
        contract_interface::{
            encoding::{Encoder, EncodingAdapter},
            StateSummary, UpdateModification,
        },
        parameters::Parameters,
    };

    use super::*;

    pub fn inner_validate_state<T, Child, Ctx>(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError>
    where
        T: ContractComponent + EncodingAdapter + DeserializeOwned,
        <T as EncodingAdapter>::Parameters: Into<<T as ContractComponent>::Parameters>,
        for<'x> <T as ContractComponent>::Context: From<&'x Ctx>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
        Child: ContractComponent,
        <Child as ContractComponent>::Parameters:
            for<'x> From<&'x <T as ContractComponent>::Parameters>,
        <Child as ContractComponent>::Context: for<'x> From<&'x T>,
        Ctx: for<'x> From<&'x T>,
    {
        let typed_params: <T as ContractComponent>::Parameters =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?.into();
        let typed_state: T = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let related_container = RelatedContractsContainer::from(related);
        let ctx = Ctx::from(&typed_state);
        match typed_state.verify::<Child, Ctx>(&typed_params, &ctx, &related_container)? {
            ValidateResult::Valid => {}
            ValidateResult::Invalid => return Ok(ValidateResult::Invalid),
            ValidateResult::RequestRelated(related) => {
                return Ok(ValidateResult::RequestRelated(related))
            }
        }
        Ok(ValidateResult::Valid)
    }

    pub fn inner_update_state<T, Child>(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError>
    where
        T: ContractComponent + EncodingAdapter,
        <T as EncodingAdapter>::Parameters: Into<<T as ContractComponent>::Parameters>,
        <T as EncodingAdapter>::Delta: Into<<T as ContractComponent>::Delta>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError: From<
            <<T as EncodingAdapter>::DeltaEncoder as Encoder<<T as EncodingAdapter>::Delta>>::Error,
        >,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
        Child: ContractComponent,
        <Child as ContractComponent>::Parameters:
            for<'x> From<&'x <T as ContractComponent>::Parameters>,
        <Child as ContractComponent>::Delta: for<'x> From<&'x <T as ContractComponent>::Delta>,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?.into();
        let mut typed_state: T =
            <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let self_updates = UpdateData::get_self_states(&data);
        let related_container = RelatedContractsContainer::from(data);
        for (state, delta) in self_updates {
            let state = state
                .map(|s| <<T as EncodingAdapter>::SelfEncoder>::deserialize(s.as_ref()))
                .transpose()?;
            let delta = delta
                .map(|d| {
                    <<T as EncodingAdapter>::DeltaEncoder>::deserialize(d.as_ref()).map(Into::into)
                })
                .transpose()?;
            let typed_update = TypedUpdateData::try_from((state, delta))?;
            match typed_state.merge(&typed_params, &typed_update, &related_container) {
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
    ) -> Result<<T as ContractComponent>::Summary, ContractError>
    where
        T: ContractComponent + EncodingAdapter,
        <T as EncodingAdapter>::Parameters: Into<<T as ContractComponent>::Parameters>,
        <T as ContractComponent>::Summary:
            for<'x> From<&'x T> + Mergeable<<T as ContractComponent>::Summary>,
        ContractError: From<
            <<T as EncodingAdapter>::ParametersEncoder as Encoder<
                <T as EncodingAdapter>::Parameters,
            >>::Error,
        >,
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?.into();
        let typed_state: T = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let mut summary = <<T as ContractComponent>::Summary>::from(&typed_state);
        typed_state.summarize(&typed_params, &mut summary)?;
        Ok(summary)
    }

    pub fn inner_state_delta<T>(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<<T as ContractComponent>::Delta, ContractError>
    where
        T: ContractComponent + EncodingAdapter,
        <T as EncodingAdapter>::Parameters: Into<<T as ContractComponent>::Parameters>,
        <T as EncodingAdapter>::Summary: Into<<T as ContractComponent>::Summary>,
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
        ContractError: From<<<T as EncodingAdapter>::SelfEncoder as Encoder<T>>::Error>,
    {
        let typed_params =
            <<T as EncodingAdapter>::ParametersEncoder>::deserialize(parameters.as_ref())?.into();
        let typed_state: T = <<T as EncodingAdapter>::SelfEncoder>::deserialize(state.as_ref())?;
        let typed_summary =
            <<T as EncodingAdapter>::SummaryEncoder>::deserialize(summary.as_ref())?.into();
        typed_state.delta(&typed_params, &typed_summary)
    }
}
