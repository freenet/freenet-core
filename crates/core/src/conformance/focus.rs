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

    /// Resume at a known epoch.
    ///
    /// Rotation only means anything if it accumulates. A peer that restarted back to
    /// epoch zero would re-select the same first focus set every time, so a contract
    /// author able to cause restarts could hold a peer's attention on whatever epoch
    /// zero happens to pick — or keep it away from themselves forever.
    pub fn resuming_at(salt: [u8; 32], max_focus: usize, epoch: u64) -> Self {
        Self {
            salt,
            max_focus: max_focus.max(1),
            epoch,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn candidates(n: u8) -> Vec<ContractInstanceId> {
        (0..n).map(id).collect()
    }

    /// A peer watches at most `max_focus` contracts, however many it hosts.
    ///
    /// This is the bound the whole design rests on: discovery is meant to be sparse,
    /// and a peer that quietly watched everything it hosted would turn a diagnostic
    /// into an amplification vector on the busiest peers.
    #[test]
    fn selection_is_bounded_by_max_focus() {
        let selector = FocusSelector::new([1; 32], 2);
        assert_eq!(selector.select(&candidates(50)).len(), 2);

        // And it cannot exceed what is actually available.
        assert_eq!(selector.select(&candidates(1)).len(), 1);
        assert!(selector.select(&[]).is_empty());
    }

    /// The same peer, same period, same hosted set: the same answer.
    ///
    /// Sampling a contract usefully takes hours, so a peer that redrew its focus on
    /// every call — or on every restart — would keep abandoning half-built corpora
    /// and never finish one.
    #[test]
    fn selection_is_stable_within_an_epoch() {
        let selector = FocusSelector::new([2; 32], 3);
        let pool = candidates(20);
        let first = selector.select(&pool);
        assert_eq!(first, selector.select(&pool));

        // Order of the candidate list must not matter either: a peer's hosted set is
        // not an ordered thing, and iteration order should not change what it
        // watches.
        let mut shuffled = pool.clone();
        shuffled.reverse();
        assert_eq!(first, selector.select(&shuffled));

        // Nor should duplicates, which a hosted-set enumeration could produce.
        let mut duplicated = pool.clone();
        duplicated.extend(pool.iter().copied());
        assert_eq!(first, selector.select(&duplicated));
    }

    /// Rotation redraws the set rather than merely appending to it.
    #[test]
    fn rotation_redraws_the_selection() {
        let mut selector = FocusSelector::new([3; 32], 2);
        let pool = candidates(40);
        let before = selector.select(&pool);

        let mut changed = false;
        // Any single rotation could coincidentally reselect the same pair, so allow
        // a few before concluding rotation does nothing.
        for _ in 0..8 {
            selector.rotate();
            if selector.select(&pool) != before {
                changed = true;
                break;
            }
        }
        assert!(
            changed,
            "rotating did not change the focus set in eight epochs, so rotation is \
             not reshuffling anything"
        );
        assert!(selector.epoch() > 0, "rotate must advance the epoch");
    }

    /// The security property: selection is not controllable by the contract author.
    ///
    /// An author picks their own contract id, so any rule that is a pure function of
    /// the id can be aimed or dodged — grind ids until no peer ever watches you, or
    /// until every peer does and the checking cost becomes the attack. Keying on a
    /// per-peer secret is what removes that lever, and this test is what says the
    /// salt is actually load-bearing rather than decorative.
    #[test]
    fn different_peers_watch_different_contracts() {
        let pool = candidates(60);
        let a = FocusSelector::new([0xAA; 32], 2).select(&pool);
        let b = FocusSelector::new([0xBB; 32], 2).select(&pool);
        let c = FocusSelector::new([0xCC; 32], 2).select(&pool);

        assert!(
            !(a == b && b == c),
            "three peers with different salts chose identical focus sets, so the \
             salt is not affecting selection and an author could predict it"
        );
    }

    /// Changing the salt changes the answer for the SAME contract, which is the
    /// same property from the contract's point of view.
    #[test]
    fn a_contract_cannot_tell_whether_it_is_watched_without_the_salt() {
        let target = id(7);
        // A small pool and many peers, so the expected count sits far from both
        // bounds: each peer draws 2 of 10, so a given contract is watched by roughly
        // a fifth of them. With 60 candidates and 40 peers the expectation is ~1.3
        // and a result of zero says nothing about the salt — the first version of
        // this test was underpowered rather than wrong, and read as a real failure.
        let pool = candidates(10);
        let peers = 100u8;

        let watched = (0..peers)
            .filter(|i| {
                FocusSelector::new([*i; 32], 2)
                    .select(&pool)
                    .contains(&target)
            })
            .count();

        // BOTH bounds matter, and the lower one is the bound that bites. `watched
        // < 40` alone is satisfied by `watched == 0`, which is precisely the
        // outcome an author wants: a contract nobody ever checks. Removing the salt
        // from scoring makes selection a pure function of the id, and this contract
        // then lands in the same bucket for every peer — all of them or none.
        // Asserting only the upper bound passed under exactly that mutation.
        assert!(
            watched > 0 && watched < peers as usize,
            "this contract was watched by {watched} of {peers} peers; being watched by \
             none or by all means selection is decided by the id, and an author \
             could grind for either"
        );
    }
}
