use chrono::{DateTime, Duration, SubsecRound, Timelike, Utc};
use ed25519_dalek::{PublicKey, Signature};
use locutus_stdlib::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

type Assignment = Vec<u8>;

/// Contracts making use of the allocation must implement a type with this trait that allows
/// extracting the criteria for the given contract.
pub trait TokenAllocation: DeserializeOwned {
    fn get_criteria(&self) -> AllocationCriteria;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
    pub fn is_valid_slot(&self, dt: DateTime<Utc>) -> bool {
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

    /// Given a datetime, get the oldest free slot for this tier.
    pub fn next_assignment(&self, mut date: DateTime<Utc>) -> DateTime<Utc> {
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

#[non_exhaustive]
#[derive(Serialize, Deserialize)]
pub struct TokenParameters {
    pub generator_public_key: PublicKey,
}

impl TryFrom<Parameters<'_>> for TokenParameters {
    type Error = ContractError;
    fn try_from(params: Parameters<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(params.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}

#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub struct AllocationCriteria {
    pub frequency: Tier,
    pub time_to_live: std::time::Duration,
}

#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenAllocationRecord {
    /// A list of issued tokens.
    ///
    /// This is categorized by tiers and then sorted by time slot.
    pub tokens_by_tier: hashbrown::HashMap<Tier, Vec<TokenAssignment>>,
}

impl TryFrom<State<'_>> for TokenAllocationRecord {
    type Error = ContractError;

    fn try_from(state: State<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(state.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[must_use]
pub struct TokenAssignment {
    pub tier: Tier,
    pub time_slot: DateTime<Utc>,
    /// The assignment, the recipient decides whether this assignment is valid based on this field.
    /// This will often be a PublicKey.
    pub assignee: Assignment,
    /// `(tier, issue_time, assigned_to)` must be signed by `generator_public_key`
    pub signature: Signature,
}

impl PartialEq for TokenAssignment {
    fn eq(&self, other: &Self) -> bool {
        self.tier == other.tier && self.time_slot == other.time_slot
    }
}

impl Eq for TokenAssignment {}

impl PartialOrd for TokenAssignment {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.time_slot.cmp(&other.time_slot))
    }
}

impl Ord for TokenAssignment {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time_slot.cmp(&other.time_slot)
    }
}

impl TokenAssignment {
    /// The `(tier, issue_time, assigned_to)` tuple that has to be verified as bytes.
    pub fn to_be_signed(
        issue_time: &DateTime<Utc>,
        assigned_to: &Assignment,
        tier: Tier,
    ) -> Vec<u8> {
        todo!()
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
