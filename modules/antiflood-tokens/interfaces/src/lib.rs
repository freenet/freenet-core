use chrono::{DateTime, Datelike, Duration, SubsecRound, Timelike, Utc};
use ed25519_dalek::{PublicKey, Signature};
use hashbrown::HashMap;
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
    Day365,
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
            Tier::Min10 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() % 10 == 0;
                vns && vs && vm
            }
            Tier::Min30 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() % 30 == 0;
                vns && vs && vm
            }
            Tier::Hour1 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                vns && vs && vm
            }
            Tier::Hour3 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() % (60 * 3) == 0;
                vns && vs && vm && vh
            }
            Tier::Hour6 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() % (60 * 6) == 0;
                vns && vs && vm && vh
            }
            Tier::Hour12 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() % (60 * 12) == 0;
                vns && vs && vm && vh
            }
            Tier::Day1 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                vns && vs && vm && vh
            }
            Tier::Day7 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 7 == 0;
                vns && vs && vm && vh && vd
            }
            Tier::Day15 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 15 == 0;
                vns && vs && vm && vh && vd
            }
            Tier::Day30 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 30 == 0;
                vns && vs && vm && vh && vd
            }
            Tier::Day90 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 90 == 0;
                vns && vs && vm && vh && vd
            }
            Tier::Day180 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 180 == 0;
                vns && vs && vm && vh && vd
            }
            Tier::Day365 => {
                let vns = dt.nanosecond() == 0;
                let vs = dt.second() == 0;
                let vm = dt.minute() == 0;
                let vh = dt.hour() == 0;
                let vd = dt.day() % 365 == 0;
                vns && vs && vm && vh && vd
            }
        }
    }

    pub fn tier_duration(&self) -> std::time::Duration {
        match self {
            Tier::Min1 => Duration::minutes(1).to_std().unwrap(),
            Tier::Min5 => Duration::minutes(5).to_std().unwrap(),
            Tier::Min10 => Duration::minutes(10).to_std().unwrap(),
            Tier::Min30 => Duration::minutes(30).to_std().unwrap(),
            Tier::Hour1 => Duration::hours(1).to_std().unwrap(),
            Tier::Hour3 => Duration::hours(3).to_std().unwrap(),
            Tier::Hour6 => Duration::hours(6).to_std().unwrap(),
            Tier::Hour12 => Duration::hours(12).to_std().unwrap(),
            Tier::Day1 => Duration::days(1).to_std().unwrap(),
            Tier::Day7 => Duration::days(7).to_std().unwrap(),
            Tier::Day15 => Duration::days(15).to_std().unwrap(),
            Tier::Day30 => Duration::days(30).to_std().unwrap(),
            Tier::Day90 => Duration::days(90).to_std().unwrap(),
            Tier::Day180 => Duration::days(180).to_std().unwrap(),
            Tier::Day365 => Duration::days(365).to_std().unwrap(),
        }
    }

    /// Normalized the datetime to be the next valid date from the provided one compatible with the tier.
    pub fn normalize_to_next(&self, mut time: DateTime<Utc>) -> DateTime<Utc> {
        match self {
            Tier::Min1 => todo!(),
            Tier::Min5 => todo!(),
            Tier::Min10 => todo!(),
            Tier::Min30 => todo!(),
            Tier::Hour1 => todo!(),
            Tier::Hour3 => todo!(),
            Tier::Hour6 => todo!(),
            Tier::Hour12 => todo!(),
            Tier::Day1 => {
                let is_rounded = time.hour() == 0
                    && time.minute() == 0
                    && time.second() == 0
                    && time.nanosecond() == 0;
                if !is_rounded {
                    let duration = chrono::Duration::from_std(self.tier_duration()).unwrap();
                    time = time.with_second(0).unwrap().with_minute(0).unwrap();
                    time = time.trunc_subsecs(0);
                    time += duration;
                }
            }
            Tier::Day7 => todo!(),
            Tier::Day15 => todo!(),
            Tier::Day30 => todo!(),
            Tier::Day90 => todo!(),
            Tier::Day180 => todo!(),
            Tier::Day365 => todo!(),
        }
        time
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

#[derive(Debug, thiserror::Error)]
pub enum AllocationError {
    #[error("the max age allowed is 730 days")]
    IncorrectMaxAge,
}

#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub struct AllocationCriteria {
    pub frequency: Tier,
    /// Maximum age of the allocated token.
    pub max_age: std::time::Duration,
}

impl AllocationCriteria {
    pub fn new(frequency: Tier, max_age: std::time::Duration) -> Result<Self, AllocationError> {
        if max_age <= std::time::Duration::from_secs(3600 * 24 * 365 * 2) {
            Ok(Self { frequency, max_age })
        } else {
            Err(AllocationError::IncorrectMaxAge)
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub struct TokenAllocationRecord {
    /// A list of issued tokens.
    ///
    /// This is categorized by tiers and then sorted by time slot.
    tokens_by_tier: HashMap<Tier, Vec<TokenAssignment>>,
}

impl TokenAllocationRecord {
    pub fn get_tier(&self, tier: &Tier) -> Option<&[TokenAssignment]> {
        self.tokens_by_tier.get(tier).map(|t| t.as_slice())
    }

    pub fn get_mut_tier(&mut self, tier: &Tier) -> Option<&mut Vec<TokenAssignment>> {
        self.tokens_by_tier.get_mut(tier)
    }

    pub fn new(mut tokens: HashMap<Tier, Vec<TokenAssignment>>) -> Self {
        for (_, assignments) in &mut tokens {
            assignments.sort_unstable();
        }
        Self {
            tokens_by_tier: tokens,
        }
    }

    pub fn insert(&mut self, tier: Tier, assignments: Vec<TokenAssignment>) {
        self.tokens_by_tier.insert(tier, assignments);
    }
}

impl<'a> IntoIterator for &'a TokenAllocationRecord {
    type Item = (&'a Tier, &'a Vec<TokenAssignment>);

    type IntoIter = hashbrown::hash_map::Iter<'a, Tier, Vec<TokenAssignment>>;

    fn into_iter(self) -> Self::IntoIter {
        self.tokens_by_tier.iter()
    }
}

impl IntoIterator for TokenAllocationRecord {
    type Item = (Tier, Vec<TokenAssignment>);

    type IntoIter = hashbrown::hash_map::IntoIter<Tier, Vec<TokenAssignment>>;

    fn into_iter(self) -> Self::IntoIter {
        self.tokens_by_tier.into_iter()
    }
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

impl TokenAssignment {
    /// The `(tier, issue_time, assigned_to)` tuple that has to be verified as bytes.
    pub fn to_be_signed(
        issue_time: &DateTime<Utc>,
        assigned_to: &Assignment,
        tier: Tier,
    ) -> Vec<u8> {
        todo!()
    }

    pub fn next_slot(&self) -> DateTime<Utc> {
        self.time_slot + Duration::from_std(self.tier.tier_duration()).unwrap()
    }

    pub fn previous_slot(&self) -> DateTime<Utc> {
        self.time_slot - Duration::from_std(self.tier.tier_duration()).unwrap()
    }
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

impl TryFrom<StateDelta<'_>> for TokenAssignment {
    type Error = ContractError;

    fn try_from(state: StateDelta<'_>) -> Result<Self, Self::Error> {
        let this = bincode::deserialize_from(state.as_ref())
            .map_err(|err| ContractError::Deser(format!("{err}")))?;
        Ok(this)
    }
}
