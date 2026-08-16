//! Choosing which contracts a peer watches, and when it moves on.
//!
//! Focus selection has one security requirement that shapes the whole design: **it
//! must not be controllable by the contract being tested.** A contract author picks
//! their own contract id, so any selection rule that is a pure function of the id
//! (nearest to the peer's ring location, lowest hash, most active) is a rule the
//! author can aim or dodge — they could grind ids until their contract is never
//! anyone's focus, or until it is *everyone's* focus and the checking cost becomes
//! an amplification vector.
//!
//! So selection is keyed on a per-peer secret salt. Scores are
//! `blake3(salt || epoch || contract_id)`, and the peer watches the lowest few.
//! Without knowing a peer's salt an author cannot predict, or influence, whether
//! that peer is looking at them; and because every peer has a different salt, the
//! network as a whole covers a broad spread of contracts while each peer watches
//! almost none.
//!
//! Rotation is just an epoch bump, which reshuffles every score at once.

use std::collections::HashSet;

use freenet_stdlib::prelude::ContractInstanceId;
use serde::{Deserialize, Serialize};

/// How many contracts one peer watches at a time.
///
/// Small on purpose. Discovery is meant to be sparse: coverage comes from many peers
/// each doing a little, not from any peer doing a lot. Operational tunable.
pub const DEFAULT_MAX_FOCUS_CONTRACTS: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FocusSelector {
    /// Per-peer secret. Persisted so focus does not reshuffle on every restart,
    /// which would keep resetting samples that need hours to accumulate.
    salt: [u8; 32],
    max_focus: usize,
    epoch: u64,
}

impl FocusSelector {
    pub fn new(salt: [u8; 32], max_focus: usize) -> Self {
        Self {
            salt,
            max_focus,
            epoch: 0,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn max_focus(&self) -> usize {
        self.max_focus
    }

    /// Move to the next focus period. Every score changes, so the set is redrawn.
    pub fn rotate(&mut self) {
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// The contracts this peer should watch right now, given what it hosts.
    ///
    /// Deterministic for a given `(salt, epoch, candidates)`, so a peer that
    /// restarts mid-period resumes watching the same contracts instead of
    /// abandoning a half-built sample.
    pub fn select(&self, candidates: &[ContractInstanceId]) -> Vec<ContractInstanceId> {
        let mut scored: Vec<([u8; 32], ContractInstanceId)> = candidates
            .iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|id| (self.score(id), *id))
            .collect();
        // Tie-break on the id so the result is total-ordered even in the
        // astronomically unlikely case of a score collision.
        scored.sort_by(|(sa, ia), (sb, ib)| {
            sa.cmp(sb).then_with(|| ia.as_bytes().cmp(ib.as_bytes()))
        });
        scored
            .into_iter()
            .take(self.max_focus)
            .map(|(_, id)| id)
            .collect()
    }

    fn score(&self, id: &ContractInstanceId) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"freenet-conformance-focus-v1");
        hasher.update(&self.salt);
        hasher.update(&self.epoch.to_le_bytes());
        hasher.update(id.as_bytes());
        *hasher.finalize().as_bytes()
    }
}
