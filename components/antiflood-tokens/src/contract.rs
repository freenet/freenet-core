use super::*;

impl TokenAssignment {
    fn is_valid(&self, params: &TokenParameters) -> bool {
        if !self.tier.is_valid_slot(self.time_slot) {
            return false;
        }
        let pk = PublicKey::from_bytes(&self.assignee).unwrap();
        if pk != params.generator_public_key {
            return false;
        }
        let msg = Self::to_be_signed(&self.time_slot, &self.assignee, self.tier);
        if params
            .generator_public_key
            .verify(&msg, &self.signature)
            .is_err()
        {
            // not signed by the privte key of this contract
            return false;
        }
        true
    }
}

impl TokenAssignmentLedger {
    fn merge(&mut self, other: Self, params: &TokenParameters) -> Result<(), ContractError> {
        for (_, assignments) in other.tokens_by_tier {
            for assignment in assignments {
                self.append(assignment, params)?;
            }
        }
        Ok(())
    }

    fn append(
        &mut self,
        assignment: TokenAssignment,
        params: &TokenParameters,
    ) -> Result<(), ContractError> {
        match self.tokens_by_tier.get_mut(&assignment.tier) {
            Some(list) => {
                if list.binary_search(&assignment).is_err() {
                    if assignment.is_valid(params) {
                        list.push(assignment);
                        list.sort_unstable();
                        Ok(())
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            }
            None => {
                self.tokens_by_tier
                    .insert(assignment.tier, vec![assignment]);
                Ok(())
            }
        }
    }
}

impl ContractInterface for TokenAssignmentLedger {
    // TODO: only can validate that the slot is valid, nothing else?
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let assigned_tokens = TokenAssignmentLedger::try_from(state)?;
        let params = TokenParameters::try_from(parameters)?;
        for (_tier, assignments) in assigned_tokens.tokens_by_tier.iter() {
            for assignment in assignments {
                if !assignment.is_valid(&params) {
                    return Ok(ValidateResult::Invalid);
                }
            }
        }
        Ok(ValidateResult::Valid)
    }

    /// The contract verifies that the release times for a tier matches the tier.
    ///
    /// For example, a 15:30 UTC release time isn't permitted for hour_1 tier, but 15:00 UTC is permitted.
    // TODO: only can validate that the slot is valid, nothing else?
    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        let assigned_token = TokenAssignment::try_from(delta)?;
        let params = TokenParameters::try_from(parameters)?;
        Ok(assigned_token.is_valid(&params))
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut assigned_tokens = TokenAssignmentLedger::try_from(state)?;
        let params = TokenParameters::try_from(parameters)?;
        for update in data {
            match update {
                UpdateData::State(s) => {
                    let new_assigned_tokens = TokenAssignmentLedger::try_from(s)?;
                    assigned_tokens.merge(new_assigned_tokens, &params)?;
                }
                UpdateData::Delta(d) => {
                    let new_assigned_token = TokenAssignment::try_from(d)?;
                    assigned_tokens.append(new_assigned_token, &params)?;
                }
                // does this send the prev state + new delta?
                UpdateData::StateAndDelta { state, delta } => {
                    let new_assigned_tokens = TokenAssignmentLedger::try_from(state)?;
                    assigned_tokens.merge(new_assigned_tokens, &params)?;
                    let new_assigned_token = TokenAssignment::try_from(delta)?;
                    assigned_tokens.append(new_assigned_token, &params)?;
                }
                // UpdateData::RelatedState { related_to, state } => {
                //     todo!()
                // }
                // UpdateData::RelatedDelta { related_to, delta } => todo!(),
                // UpdateData::RelatedStateAndDelta {
                //     related_to,
                //     state,
                //     delta,
                // } => todo!(),
                _ => unreachable!(),
            }
        }
        todo!()
    }

    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        // FIXME: the state summary must be a summary of tokens assigned per tier
        todo!()
    }

    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        // FIXME: return only the assignments not present in the summary
        todo!()
    }
}
