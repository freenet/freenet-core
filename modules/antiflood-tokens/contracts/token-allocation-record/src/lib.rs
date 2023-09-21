use freenet_aft_interface::{
    AllocationError, TokenAllocationRecord, TokenAssignment, TokenDelegateParameters,
};
use freenet_stdlib::prelude::*;
use rsa::{pkcs1v15::VerifyingKey, sha2::Sha256, RsaPublicKey};

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
        #[allow(clippy::redundant_clone)]
        let verifying_key = VerifyingKey::<Sha256>::new(params.generator_public_key.clone());
        for (_tier, assignments) in (&assigned_tokens).into_iter() {
            for assignment in assignments {
                if assignment.is_valid(&verifying_key).is_err() {
                    log_verification_err(&params.generator_public_key, "validate state");
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
        #[allow(clippy::redundant_clone)]
        let verifying_key = VerifyingKey::<Sha256>::new(params.generator_public_key.clone());
        let verification = assigned_token.is_valid(&verifying_key);
        if verification.is_err() {
            log_verification_err(&params.generator_public_key, "validate delta");
        }
        log_succesful_ver(&params.generator_public_key, "validate delta");
        Ok(verification.is_ok())
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
        let mut assigned_tokens = TokenAllocationRecord::try_from(state)?;
        let params = TokenDelegateParameters::try_from(parameters)?;
        let verifying_key = VerifyingKey::<Sha256>::new(params.generator_public_key.clone());
        for update in data {
            match update {
                UpdateData::State(s) => {
                    let new_assigned_tokens = TokenAllocationRecord::try_from(s)?;
                    assigned_tokens
                        .merge(new_assigned_tokens, &verifying_key)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            log_verification_err(
                                &params.generator_public_key,
                                "update state (state)",
                            );
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                    log_succesful_ver(&params.generator_public_key, "update state (state)")
                }
                UpdateData::Delta(d) => {
                    let new_assigned_token = TokenAssignment::try_from(d)?;
                    assigned_tokens
                        .append(new_assigned_token, &verifying_key)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            log_verification_err(
                                &params.generator_public_key,
                                "update state (delta)",
                            );
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                    log_succesful_ver(&params.generator_public_key, "update state (delta)")
                }
                UpdateData::StateAndDelta { state, delta } => {
                    let new_assigned_tokens = TokenAllocationRecord::try_from(state)?;
                    assigned_tokens
                        .merge(new_assigned_tokens, &verifying_key)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            log_verification_err(
                                &params.generator_public_key,
                                "update state (state and delta)",
                            );
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                    let new_assigned_token = TokenAssignment::try_from(delta)?;
                    assigned_tokens
                        .append(new_assigned_token, &verifying_key)
                        .map_err(|err| {
                            tracing::error!("{err}");
                            log_verification_err(
                                &params.generator_public_key,
                                "update state (state and delta)",
                            );
                            ContractError::InvalidUpdateWithInfo {
                                reason: format!("{err}"),
                            }
                        })?;
                    log_succesful_ver(
                        &params.generator_public_key,
                        "update state (state and delta)",
                    )
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

#[allow(unused)]
fn log_succesful_ver(pub_key: &RsaPublicKey, target: &str) {
    #[cfg(target_family = "wasm")]
    {
        use rsa::pkcs8::EncodePublicKey;
        let pk = pub_key
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .split_whitespace()
            .collect::<String>();
        // freenet_stdlib::log::info(&format!(
        //     "successful verification with key: {pk} @ {target}"
        // ));
    }
}

#[allow(unused)]
fn log_verification_err(pub_key: &RsaPublicKey, target: &str) {
    #[cfg(target_family = "wasm")]
    {
        use rsa::pkcs8::EncodePublicKey;
        let pk = pub_key
            .to_public_key_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .split_whitespace()
            .collect::<String>();
        freenet_stdlib::log::info(&format!("erroneous verification with key: {pk} @ {target}"));
    }
}

trait TokenAllocationRecordExt {
    fn merge(&mut self, other: Self, key: &VerifyingKey<Sha256>) -> Result<(), AllocationError>;
    fn append(
        &mut self,
        assignment: TokenAssignment,
        params: &VerifyingKey<Sha256>,
    ) -> Result<(), AllocationError>;
}

impl TokenAllocationRecordExt for TokenAllocationRecord {
    fn merge(&mut self, other: Self, key: &VerifyingKey<Sha256>) -> Result<(), AllocationError> {
        for (_, assignments) in other.into_iter() {
            for assignment in assignments {
                self.append(assignment, key)?;
            }
        }
        Ok(())
    }

    fn append(
        &mut self,
        assignment: TokenAssignment,
        key: &VerifyingKey<Sha256>,
    ) -> Result<(), AllocationError> {
        match self.get_mut_tier(&assignment.tier) {
            Some(list) => {
                if list.binary_search(&assignment).is_err() {
                    match assignment.is_valid(key) {
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
