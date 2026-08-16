//! The offline replay corpus.
//!
//! A bundle is everything needed to re-run conformance checks against one contract
//! away from the network: the exact WASM, the exact parameters, and the states,
//! deltas and summaries observed in the wild. It is the hinge of the development
//! loop the RFC describes — real peer traffic becomes a bundle, the bundle is
//! replayed offline with the production verifier, and a failure found there is
//! reproducible forever after as a regression test.
//!
//! It is deliberately a *different* thing from the node's internal sample store. The
//! store is tuned for bounded incremental capture; the bundle is a flat, portable,
//! self-describing file. Keeping them separate means the on-disk schema can change
//! without breaking every corpus anyone has archived.

use std::path::Path;
use std::sync::Arc;

use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

use super::generator::Corpus;
use super::verifier::Bytes;

pub const BUNDLE_SCHEMA_VERSION: u16 = 1;

/// Magic prefix so a truncated or unrelated file fails fast with a clear message
/// rather than as a confusing deserialization error deep in bincode.
const BUNDLE_MAGIC: &[u8; 8] = b"FRNTCNF1";

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("not a conformance bundle (bad magic)")]
    BadMagic,
    #[error("bundle schema {found} is not supported (this build reads {supported})")]
    UnsupportedSchema { found: u16, supported: u16 },
    #[error("decode: {0}")]
    Decode(String),
    #[error("bundle carries no contract code and none was supplied separately")]
    MissingCode,
}

/// One observed `base + update -> result` step.
///
/// Transitions are more valuable than loose states because they carry the context
/// that makes a delta replayable: which summary it was generated against, and what
/// the peer that produced it ended up with.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transition {
    pub base_state: Vec<u8>,
    pub delta: Option<Vec<u8>>,
    pub incoming_state: Option<Vec<u8>>,
    pub summary: Option<Vec<u8>>,
    pub result_state: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayBundle {
    pub schema_version: u16,
    /// The contract's own WASM. Optional so a bundle can reference a contract by
    /// hash when the code is already available locally and shipping megabytes again
    /// would be wasteful.
    pub code: Option<Vec<u8>>,
    pub code_hash: [u8; 32],
    pub parameters: Vec<u8>,
    pub instance: Option<ContractInstanceId>,
    pub states: Vec<Vec<u8>>,
    pub deltas: Vec<Vec<u8>>,
    pub summaries: Vec<Vec<u8>>,
    pub transitions: Vec<Transition>,
    pub related: Vec<(ContractInstanceId, Vec<u8>)>,
    /// Free-form provenance: which node, which release, when. Never load-bearing.
    pub note: Option<String>,
}

impl ReplayBundle {
    pub fn new(code: Vec<u8>, parameters: Vec<u8>) -> Self {
        let code_hash = *blake3::hash(&code).as_bytes();
        Self {
            schema_version: BUNDLE_SCHEMA_VERSION,
            code: Some(code),
            code_hash,
            parameters,
            instance: None,
            states: Vec::new(),
            deltas: Vec::new(),
            summaries: Vec::new(),
            transitions: Vec::new(),
            related: Vec::new(),
            note: None,
        }
    }

    pub fn total_bytes(&self) -> usize {
        self.code.as_ref().map_or(0, Vec::len)
            + self.states.iter().map(Vec::len).sum::<usize>()
            + self.deltas.iter().map(Vec::len).sum::<usize>()
            + self.summaries.iter().map(Vec::len).sum::<usize>()
            + self
                .transitions
                .iter()
                .map(|t| {
                    t.base_state.len()
                        + t.result_state.len()
                        + t.delta.as_ref().map_or(0, Vec::len)
                        + t.incoming_state.as_ref().map_or(0, Vec::len)
                        + t.summary.as_ref().map_or(0, Vec::len)
                })
                .sum::<usize>()
    }

    /// Flatten into the material the generator works from.
    ///
    /// Transitions contribute their endpoint states too: a result state that a peer
    /// actually reached is by construction a state the contract produced, which
    /// makes it exactly the kind of input the algebraic laws are about.
    pub fn to_corpus(&self) -> Corpus {
        let mut states: Vec<Bytes> = self
            .states
            .iter()
            .map(|s| Arc::from(s.as_slice()))
            .collect();
        let mut deltas: Vec<Bytes> = self
            .deltas
            .iter()
            .map(|d| Arc::from(d.as_slice()))
            .collect();
        let mut summaries: Vec<Bytes> = self
            .summaries
            .iter()
            .map(|s| Arc::from(s.as_slice()))
            .collect();

        for transition in &self.transitions {
            states.push(Arc::from(transition.base_state.as_slice()));
            states.push(Arc::from(transition.result_state.as_slice()));
            if let Some(state) = &transition.incoming_state {
                states.push(Arc::from(state.as_slice()));
            }
            if let Some(delta) = &transition.delta {
                deltas.push(Arc::from(delta.as_slice()));
            }
            if let Some(summary) = &transition.summary {
                summaries.push(Arc::from(summary.as_slice()));
            }
        }

        let related = self
            .related
            .iter()
            .map(|(id, state)| {
                (
                    *id,
                    Some(freenet_stdlib::prelude::State::from(state.clone())),
                )
            })
            .collect::<std::collections::HashMap<_, _>>();

        Corpus {
            states,
            deltas,
            summaries,
            related: freenet_stdlib::prelude::RelatedContracts::from(related),
        }
        .deduplicated()
    }

    pub fn encode(&self) -> Result<Vec<u8>, BundleError> {
        let mut out = Vec::with_capacity(self.total_bytes() + 64);
        out.extend_from_slice(BUNDLE_MAGIC);
        out.extend_from_slice(&self.schema_version.to_le_bytes());
        let body = bincode::serialize(self).map_err(|e| BundleError::Decode(e.to_string()))?;
        out.extend_from_slice(&body);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BundleError> {
        if bytes.len() < BUNDLE_MAGIC.len() + 2 || &bytes[..BUNDLE_MAGIC.len()] != BUNDLE_MAGIC {
            return Err(BundleError::BadMagic);
        }
        let version = u16::from_le_bytes([bytes[8], bytes[9]]);
        if version != BUNDLE_SCHEMA_VERSION {
            return Err(BundleError::UnsupportedSchema {
                found: version,
                supported: BUNDLE_SCHEMA_VERSION,
            });
        }
        bincode::deserialize(&bytes[10..]).map_err(|e| BundleError::Decode(e.to_string()))
    }

    pub fn write_to(&self, path: &Path) -> Result<(), BundleError> {
        std::fs::write(path, self.encode()?)?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, BundleError> {
        Self::decode(&std::fs::read(path)?)
    }
}
