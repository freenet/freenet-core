//! Interface and related utilities for interaction with the compiled WASM contracts.
//! Contracts have an isomorphic interface which partially maps to this interface,
//! allowing interaction between the runtime and the contracts themselves.
//!
//! This abstraction layer shouldn't leak beyond the contract handler.

use std::{
    borrow::Cow,
    ops::{Deref, DerefMut},
};

use crate::buffer::BufferBuilder;

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

pub struct StateDelta<'a>(&'a [u8]);

impl<'a> From<StateDelta<'a>> for Vec<u8> {
    fn from(val: StateDelta<'a>) -> Self {
        val.0.to_owned()
    }
}

impl<'a> StateDelta<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a> AsRef<[u8]> for StateDelta<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> From<&'a [u8]> for StateDelta<'a> {
    fn from(delta: &'a [u8]) -> Self {
        StateDelta(delta)
    }
}

pub struct StateSummary<'a>(&'a [u8]);

impl<'a> From<StateSummary<'a>> for Vec<u8> {
    fn from(val: StateSummary<'a>) -> Self {
        val.0.to_owned()
    }
}

impl<'a> StateSummary<'a> {
    pub fn size(&self) -> usize {
        self.0.len()
    }
}

impl<'a> AsRef<[u8]> for StateSummary<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> From<&'a [u8]> for StateSummary<'a> {
    fn from(bytes: &'a [u8]) -> Self {
        StateSummary(bytes)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecError {
    #[error("invalid put value")]
    InvalidPutValue,

    #[error("insufficient memory, needed {req} bytes but had {free} bytes")]
    InsufficientMemory { req: usize, free: usize },

    #[error("could not cast array length of {0} to max size (i32::MAX)")]
    InvalidArrayLength(usize),

    #[error("unexpected result from contract interface")]
    UnexpectedResult,
}

#[repr(i32)]
pub enum UpdateResult {
    ValidUpdate = 0i32,
    ValidNoChange = 1i32,
    Invalid = 2i32,
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

pub trait ContractInterface {
    fn validate_state(parameters: Parameters<'static>, state: State<'static>) -> bool;
    fn validate_delta(parameters: Parameters<'static>, delta: StateDelta<'static>) -> bool;
    fn update_state(
        parameters: Parameters<'static>,
        state: State<'static>,
        delta: StateDelta<'static>,
    ) -> UpdateResult;
}
