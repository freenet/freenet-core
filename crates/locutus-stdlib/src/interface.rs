//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
};

pub struct Parameters<'a>(&'a [u8]);

impl<'a> Parameters<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a> From<&'a [u8]> for Parameters<'a> {
    fn from(s: &'a [u8]) -> Self {
        Parameters(s)
    }
}

impl<'a> AsRef<[u8]> for Parameters<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

pub struct State<'a>(Cow<'a, [u8]>);

impl<'a> State<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }

    pub fn to_mut(&mut self) -> &mut Vec<u8> {
        self.0.to_mut()
    }
}

impl<'a> From<Vec<u8>> for State<'a> {
    fn from(state: Vec<u8>) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for State<'a> {
    fn from(state: &'a [u8]) -> Self {
        State(Cow::from(state))
    }
}

impl<'a> AsRef<[u8]> for State<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for State<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for State<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct StateDelta<'a>(Cow<'a, [u8]>);

impl<'a> StateDelta<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl<'a> From<Vec<u8>> for StateDelta<'a> {
    fn from(state: Vec<u8>) -> Self {
        StateDelta(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for StateDelta<'a> {
    fn from(state: &'a [u8]) -> Self {
        StateDelta(Cow::from(state))
    }
}

impl<'a> AsRef<[u8]> for StateDelta<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateDelta<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for StateDelta<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct StateSummary<'a>(Cow<'a, [u8]>);

impl<'a> StateSummary<'a> {
    pub fn into_owned(self) -> Vec<u8> {
        self.0.into_owned()
    }
}

impl<'a> From<Vec<u8>> for StateSummary<'a> {
    fn from(state: Vec<u8>) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> From<&'a [u8]> for StateSummary<'a> {
    fn from(state: &'a [u8]) -> Self {
        StateSummary(Cow::from(state))
    }
}

impl<'a> StateSummary<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a> AsRef<[u8]> for StateSummary<'a> {
    fn as_ref(&self) -> &[u8] {
        match &self.0 {
            Cow::Borrowed(arr) => arr,
            Cow::Owned(arr) => arr.as_ref(),
        }
    }
}

impl<'a> Deref for StateSummary<'a> {
    type Target = Cow<'a, [u8]>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> DerefMut for StateSummary<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[doc(hidden)]
#[repr(i32)]
pub enum UpdateResult {
    ValidUpdate = 0i32,
    ValidNoChange = 1i32,
    Invalid = 2i32,
}

impl From<ContractError> for UpdateResult {
    fn from(err: ContractError) -> Self {
        match err {
            ContractError::InvalidUpdate => UpdateResult::Invalid,
        }
    }
}

impl TryFrom<i32> for UpdateResult {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::ValidUpdate),
            1 => Ok(Self::ValidNoChange),
            2 => Ok(Self::Invalid),
            _ => Err(()),
        }
    }
}

pub enum UpdateModification {
    ValidUpdate(State<'static>),
    NoChange,
}

#[derive(Debug)]
pub enum ContractError {
    InvalidUpdate,
}

pub trait ContractInterface {
    /// Verify that the state is valid, given the parameters. This will be used before a peer
    /// caches a new state.
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool;

    /// Verify that a delta is valid - at least as much as possible. The goal is to prevent DDoS of
    /// a contract by sending a large number of invalid delta updates. This allows peers
    /// to verify a delta before forwarding it.
    fn validate_delta(parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool;

    /// Update the state to account for the state_delta, assuming it is valid
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> Result<UpdateModification, ContractError>;

    /// Generate a concise summary of a state that can be used to create deltas
    /// relative to this state. This allows flexible and efficient state synchronization between peers.
    fn summarize_state(
        parameters: Parameters<'static>,
        state: State<'static>,
    ) -> StateSummary<'static>;

    /// Generate a state delta using a summary from the current state. This along with
    /// [`Self::summarize_state`] allows flexible and efficient state synchronization between peers.
    fn get_state_delta(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> StateDelta<'static>;

    /// Updates the current state from the provided summary.
    fn update_state_from_summary(
        parameters: Parameters<'static>,
        state: State<'static>,
        summary: StateSummary<'static>,
    ) -> Result<UpdateModification, ContractError>;
}
