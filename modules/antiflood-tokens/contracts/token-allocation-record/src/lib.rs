use locutus_aft_interface::{
    AllocationError, InvalidReason, TokenAllocationRecord, TokenAssignment, TokenDelegateParameters,
};
use locutus_stdlib::prelude::*;
use rsa::{pkcs1v15::VerifyingKey, sha2::Sha256};

pub struct TokenAllocContract;

#[contract]
impl ContractInterface for TokenAllocContract {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let assigned_tokens = TokenAllocationRecord::try_from(state)?;
        let params = TokenDelegateParameters::try_from(parameters)?;
        for (_tier, assignments) in (&assigned_tokens).into_iter() {
            for assignment in assignments {
                if assignment.is_valid(&params).is_err() {
                    return Ok(ValidateResult::Invalid);
                }
            }
        }
        Ok(ValidateResult::Valid)
    }

    /// The contract verifies that the release times for a tier matches the tier.
    ///
    /// For example, a 15:30 UTC release time isn't permitted for hour_1 tier, but 15:00 UTC is permitted.
    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let assigned_token = TokenAssignment::try_from(delta)?;
        let params = TokenDelegateParameters::try_from(parameters)?;
        Ok(assigned_token.is_valid(&params).is_ok())
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut assigned_tokens = TokenAllocationRecord::try_from(state)?;
        let params = TokenDelegateParameters::try_from(parameters)?;
        for update in data {
            match update {
                UpdateData::State(s) => {
                    let new_assigned_tokens = TokenAllocationRecord::try_from(s)?;
                    assigned_tokens
                        .merge(new_assigned_tokens, &params)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                }
                UpdateData::Delta(d) => {
                    let new_assigned_token = TokenAssignment::try_from(d)?;
                    assigned_tokens
                        .append(new_assigned_token, &params)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                }
                UpdateData::StateAndDelta { state, delta } => {
                    let new_assigned_tokens = TokenAllocationRecord::try_from(state)?;
                    assigned_tokens
                        .merge(new_assigned_tokens, &params)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                    let new_assigned_token = TokenAssignment::try_from(delta)?;
                    assigned_tokens
                        .append(new_assigned_token, &params)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                }
                _ => unreachable!(),
            }
        }
        let update = assigned_tokens.try_into()?;
        Ok(UpdateModification::valid(update))
    }

    fn summarize_state(
        _parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        let assigned_tokens = TokenAllocationRecord::try_from(state)?;
        let summary = assigned_tokens.summarize();
        summary.try_into()
    }

    fn get_state_delta(
        _parameters: Parameters<'static>,
        state: State<'static>,
        _summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // FIXME: this will be broken because problem with node
        let assigned_tokens = TokenAllocationRecord::try_from(state)?;
        //let summary = TokenAllocationSummary::try_from(summary)?;
        //let delta = assigned_tokens.delta(&summary);
        assigned_tokens.try_into()
    }
}

trait TokenAssignmentExt {
    fn is_valid(&self, params: &TokenDelegateParameters) -> Result<(), InvalidReason>;
}

impl TokenAssignmentExt for TokenAssignment {
    fn is_valid(&self, params: &TokenDelegateParameters) -> Result<(), InvalidReason> {
        use rsa::signature::Verifier;
        if !self.tier.is_valid_slot(self.time_slot) {
            return Err(InvalidReason::InvalidSlot);
        }
        let msg =
            TokenAssignment::signature_content(&self.time_slot, self.tier, &self.assignment_hash);
        let verifying_key = VerifyingKey::<Sha256>::from(params.generator_public_key.clone());
        if verifying_key.verify(&msg, &self.signature).is_err() {
            // not signed by the private key of this generator
            return Err(InvalidReason::SignatureMismatch);
        }
        Ok(())
    }
}

trait TokenAllocationRecordExt {
    fn merge(
        &mut self,
        other: Self,
        params: &TokenDelegateParameters,
    ) -> Result<(), AllocationError>;
    fn append(
        &mut self,
        assignment: TokenAssignment,
        params: &TokenDelegateParameters,
    ) -> Result<(), AllocationError>;
}

impl TokenAllocationRecordExt for TokenAllocationRecord {
    fn merge(
        &mut self,
        other: Self,
        params: &TokenDelegateParameters,
    ) -> Result<(), AllocationError> {
        for (_, assignments) in other.into_iter() {
            for assignment in assignments {
                self.append(assignment, params)?;
            }
        }
        Ok(())
    }

    fn append(
        &mut self,
        assignment: TokenAssignment,
        params: &TokenDelegateParameters,
    ) -> Result<(), AllocationError> {
        match self.get_mut_tier(&assignment.tier) {
            Some(list) => {
                if list.binary_search(&assignment).is_err() {
                    match assignment.is_valid(params) {
                        Ok(_) => {
                            list.push(assignment);
                            list.sort_unstable();
                            Ok(())
                        }
                        Err(err) => Err(AllocationError::invalid_assignment(assignment, err)),
                    }
                } else {
                    Err(AllocationError::allocated_slot(&assignment))
                }
            }
            None => {
                self.insert(assignment.tier, vec![assignment]);
                Ok(())
            }
        }
    }
}
