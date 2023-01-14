use chrono::{DateTime, Duration, SubsecRound, Timelike, Utc};
use ed25519_dalek::{Keypair, PublicKey, Signature, Signer, Verifier};
use locutus_stdlib::{prelude::*, time};
use serde::{Deserialize, Serialize};

struct TokenComponent;

#[component]
impl ComponentInterface for TokenComponent {
    fn process(messages: InboundComponentMsg) -> Result<Vec<OutboundComponentMsg>, ComponentError> {
        todo!()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Tier {
    Min1,
    Min5,
    Min10,
    Min30,
    Hour1,
    Hour3,
    Hour6,
    Hour12,
    Day1,
    Day7,
    Day15,
    Day30,
    Day90,
    Day180,
    Day360,
}

impl Tier {
    fn is_valid_slot(&self, dt: DateTime<Utc>) -> bool {
        match self {
            Tier::Min1 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                vns && vs
            }
            Tier::Min5 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() % 5 == 0;
                vns && vs && vm
            }
            Tier::Min10 => todo!(),
            Tier::Min30 => todo!(),
            Tier::Hour1 => todo!(),
            Tier::Hour3 => todo!(),
            Tier::Hour6 => todo!(),
            Tier::Hour12 => todo!(),
            Tier::Day1 => todo!(),
            Tier::Day7 => todo!(),
            Tier::Day15 => todo!(),
            Tier::Day30 => todo!(),
            Tier::Day90 => todo!(),
            Tier::Day180 => todo!(),
            Tier::Day360 => todo!(),
        }
    }

    /// Given a datetime, get the next closest free slot for this tier.
    fn next_assignment(&self, mut date: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Tier::Min1 => {
                date += Duration::minutes(1);
            }
            Tier::Min5 => {
                date += Duration::minutes(5);
            }
            Tier::Min10 => {
                date += Duration::minutes(10);
            }
            Tier::Min30 => {
                date += Duration::minutes(30);
            }
            Tier::Hour1 => {
                date += Duration::hours(1);
            }
            Tier::Hour3 => {
                date += Duration::hours(3);
            }
            Tier::Hour6 => {
                date += Duration::hours(6);
            }
            Tier::Hour12 => {
                date += Duration::hours(12);
            }
            Tier::Day1 => {
                date += Duration::days(1);
            }
            Tier::Day7 => {
                date += Duration::days(7);
            }
            Tier::Day15 => {
                date += Duration::days(15);
            }
            Tier::Day30 => {
                date += Duration::days(30);
            }
            Tier::Day90 => date += Duration::days(90),
            Tier::Day180 => date += Duration::days(180),
            Tier::Day360 => {
                date += Duration::days(360);
            }
        }
        date = date.with_second(0).unwrap();
        date = date.trunc_subsecs(0);
        date
    }
}

#[derive(Serialize, Deserialize)]
struct TokenParameters {
    generator_public_key: PublicKey,
}

impl TryFrom<Parameters<'_>> for TokenParameters {
    type Error = ContractError;
    fn try_from(params: Parameters<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(params.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}

type Assignment = Vec<u8>;

#[derive(Serialize, Deserialize)]
pub struct TokenAssignmentLedger {
    /// A list of issued tokens.
    ///
    /// This is categorized by tiers and then sorted by time slot.
    tokens_by_tier: hashbrown::HashMap<Tier, Vec<TokenAssignment>>,
}

/// Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
impl TokenAssignmentLedger {
    /// Assigns the next theoretical free slot. This could be out of sync due to other concurrent requests so it may fail
    /// to validate at the node. In that case the application should retry again, after refreshing the ledger version.
    pub fn assign(&mut self, assignee: Assignment, tier: Tier, private_key: Keypair) {
        let next = self.next_free_assignment(tier);
        let assignment = TokenAssignment::assign(private_key, tier, assignee, next);
    }

    fn next_free_assignment(&self, tier: Tier) -> DateTime<Utc> {
        let now = Utc::now();
        match self.tokens_by_tier.get(&tier) {
            Some(_others) => {
                //
                todo!()
            }
            None => tier.next_assignment(now),
        }
    }

    fn merge(&mut self, other: Self, params: &TokenParameters) -> Result<(), ContractError> {
        Ok(())
    }

    fn append(
        &mut self,
        other: TokenAssignment,
        params: &TokenParameters,
    ) -> Result<(), ContractError> {
        Ok(())
    }
}

impl TryFrom<State<'_>> for TokenAssignmentLedger {
    type Error = ContractError;

    fn try_from(state: State<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(state.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}

#[derive(Serialize, Deserialize)]
struct TokenAssignment {
    tier: Tier,
    time_slot: DateTime<Utc>,
    /// The assignment, the recipient decides whether this assignment is valid based on this field.
    /// This will often be a PublicKey.
    assignee: Assignment,
    /// `(tier, issue_time, assigned_to)` must be signed by `generator_public_key`
    signature: Signature,
}

impl TokenAssignment {
    /// This function should be used by applications to get a new assigment.
    pub fn assign(
        private_key: Keypair,
        tier: Tier,
        assignee: Assignment,
        time_slot: DateTime<Utc>,
    ) -> Self {
        let msg = Self::to_be_signed(&time_slot, &assignee, tier);
        let signature = private_key.sign(&msg);
        TokenAssignment {
            tier,
            time_slot,
            assignee,
            signature,
        }
    }

    /// The `(tier, issue_time, assigned_to)` tuple that has to be verified as bytes.
    fn to_be_signed(issue_time: &DateTime<Utc>, assigned_to: &Assignment, tier: Tier) -> Vec<u8> {
        todo!()
    }

    fn validate(&self, params: &TokenParameters) -> bool {
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

impl TryFrom<StateDelta<'_>> for TokenAssignment {
    type Error = ContractError;

    fn try_from(state: StateDelta<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(state.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
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
                if !assignment.validate(&params) {
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
        Ok(assigned_token.validate(&params))
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
