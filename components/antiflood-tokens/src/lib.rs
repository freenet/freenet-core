use chrono::{DateTime, Timelike, Utc};
use ed25519_dalek::{PublicKey, Signature, Verifier};
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
    Month3,
    Month6,
    Month12,
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
            Tier::Month3 => todo!(),
            Tier::Month6 => todo!(),
            Tier::Month12 => todo!(),
        }
    }

    /// Given a datetime, get the next closest free slot for this tier.
    fn next_assignment(&self, current: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Tier::Min1 => {
                todo!()
            }
            Tier::Min5 => todo!(),
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
            Tier::Month3 => todo!(),
            Tier::Month6 => todo!(),
            Tier::Month12 => todo!(),
        }
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
struct TokenContract {
    /// A list of issued tokens
    tokens_by_tier: hashbrown::HashMap<Tier, hashbrown::HashMap<DateTime<Utc>, TokenAssignment>>,
}

impl TryFrom<State<'_>> for TokenContract {
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
    issue_time: DateTime<Utc>,
    /// The assignment, the recipient decides whether this assignment is valid based on this field.
    /// This will often be a PublicKey.
    assigned_to: Assignment,
    /// `(tier, issue_time, assigned_to)` must be signed by `generator_public_key`
    signature: Signature,
}

impl TokenAssignment {
    fn assign(tier: Tier) -> Self {
        let date = time::now();
        todo!()
    }

    fn to_be_signed(&self) -> Vec<u8> {
        todo!()
    }

    fn validate(&self, params: &TokenParameters) -> bool {
        if !self.tier.is_valid_slot(self.issue_time) {
            return false;
        }
        let pk = PublicKey::from_bytes(&self.assigned_to).unwrap();
        if pk != params.generator_public_key {
            return false;
        }
        let msg = self.to_be_signed();
        if params
            .generator_public_key
            .verify(&msg, &self.signature)
            .is_err()
        {
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

#[contract]
impl ContractInterface for TokenContract {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        _related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        let assigned_tokens = TokenContract::try_from(state)?;
        let params = TokenParameters::try_from(parameters)?;
        for (_tier, assignments) in assigned_tokens.tokens_by_tier.iter() {
            for (_slot, assignment) in assignments {
                if !assignment.validate(&params) {
                    return Ok(ValidateResult::Invalid);
                }
            }
        }
        Ok(ValidateResult::Valid)
    }

    /// The contract verifies that the release times for a tier match the tier.
    ///
    /// For example, a 15:30 UTC release time isn't permitted for hour_1 tier, but 15:00 UTC is permitted.
    /// Note: Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
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
        let assigned_tokens = TokenContract::try_from(state)?;
        let params = TokenParameters::try_from(parameters)?;
        todo!()
    }

    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> Result<StateSummary<'static>, ContractError> {
        todo!()
    }

    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<StateDelta<'static>, ContractError> {
        todo!()
    }
}
