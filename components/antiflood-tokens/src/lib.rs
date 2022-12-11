use ed25519_dalek::{PublicKey, Signature};
use locutus_stdlib::prelude::*;
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

#[derive(Serialize, Deserialize)]
struct TokenParameters {
    generator_public_key: PublicKey,
    // current_date_utc : Date, ??? is this necessary
}

impl TryFrom<Parameters<'_>> for TokenParameters {
    type Error = ContractError;
    fn try_from(params: Parameters<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(params.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}

type DateTime = i64;
type Assignment = Vec<u8>;

#[derive(Serialize, Deserialize)]
struct TokenContract {
    /// A list of issued tokens
    tokens_by_tier: hashbrown::HashMap<Tier, hashbrown::HashMap<DateTime, TokenAssignment>>,
}

#[derive(Serialize, Deserialize)]
struct TokenAssignment {
    tier: Tier,
    issue_time: DateTime,
    /// The assignment, the recipient decides whether this assignment
    /// is valid based on this field. This will often be a PublicKey.
    assigned_to: Assignment,
    /// `(tier, issue_time, assigned_to)` must be signed
    /// by `generator_public_key`
    signature: Signature,
}

/*
The contract verifies that the release times for a tier match the tier. For example, a 15:30 UTC release time isn't permitted for hour_1 tier, but 15:00 UTC is permitted.
Note: Conflicting assignments for the same time slot are not permitted and indicate that the generator is broken or malicious.
*/
impl TokenAssignment {
    pub fn assign(&mut self, assignment: TokenAssignment) {
        todo!()
    }
}

#[contract]
impl ContractInterface for TokenContract {
    fn validate_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        related: RelatedContracts<'static>,
    ) -> Result<ValidateResult, ContractError> {
        todo!()
    }

    fn validate_delta(
        parameters: Parameters<'static>,
        delta: StateDelta<'static>,
    ) -> Result<bool, ContractError> {
        todo!()
    }

    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        data: Vec<UpdateData<'static>>,
    ) -> Result<UpdateModification<'static>, ContractError> {
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
