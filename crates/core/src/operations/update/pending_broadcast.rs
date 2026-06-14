//! Deferred re-broadcast store for fresh-contract PUTs that found no targets.
//!
//! ## Why this exists (issue #4359)
//!
//! When a brand-new contract id is PUT, the node applies the state locally and
//! emits a `BroadcastStateChange` to fan it out to interested peers. For a
//! never-before-seen id, no peer has subscribed/announced interest yet, so the
//! initial broadcast resolves **zero** targets. The fan-out handler retries a
//! few times over ~6 s (`MAX_BROADCAST_RETRIES` × `BROADCAST_RETRY_BASE_DELAY`)
//! and then **gives up permanently**. But a fresh id's interest/subscription
//! resolves on a *much* longer timescale (the second-PUT-of-the-same-id case in
//! #4359 propagated only ~31 s after the PUT). The give-up window is shorter
//! than the interest-resolve latency, so the state silently lands
//! locally-hosted only: the originating node's GET looks healthy while every
//! other node GETs `NotFound`.
//!
//! Rather than lengthen the give-up timeout (a band-aid that still races), this
//! store keeps the last broadcast state for a contract that exhausted its retry
//! budget with no targets. When the *first* interested peer / subscriber for
//! that contract appears later (`register_peer_interest` returns "new"), the
//! subscribe path drains the stash and re-emits a single `BroadcastStateChange`
//! — which now finds the freshly-registered target and propagates.
//!
//! ## Bounding (per `.claude/rules/code-style.md` per-key-collection rule and
//! the AGENTS.md "cleanup exemptions must be time-bounded" rule)
//!
//! The store is bounded two ways so a churn of network-influenced fresh ids
//! cannot pin memory:
//!
//! * **Size**: at most [`MAX_PENDING_BROADCASTS`] contracts. A new insert at
//!   capacity evicts the oldest entry (it pruning expired entries first).
//! * **Age (TTL)**: an entry older than [`PENDING_BROADCAST_TTL`] is dropped on
//!   the next insert/take and never re-broadcast. A fresh PUT that never gains a
//!   subscriber is therefore released, not pinned forever.
//!
//! Timestamps come from [`GlobalSimulationTime`](crate::config::GlobalSimulationTime)
//! so the TTL is deterministic under the simulation clock (DST).

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, WrappedState};

use crate::config::GlobalSimulationTime;

/// Maximum number of distinct contracts with a deferred broadcast pending. A
/// fresh insert at capacity evicts the oldest entry rather than growing the
/// map. 1024 comfortably covers the realistic count of fresh-id PUTs in flight
/// on a busy node while capping worst-case memory at a bounded multiple of the
/// largest stashed state.
const MAX_PENDING_BROADCASTS: usize = 1024;

/// How long a deferred broadcast stays eligible for re-emission. After this the
/// entry is dropped without re-broadcasting: if no subscriber/interest appeared
/// within the window, the fresh PUT is treated as locally-hosted-only and the
/// stash is released. 5 minutes is well past the observed ~31 s
/// interest-resolve latency for fresh ids (#4359) with a wide safety margin,
/// while still bounding how long an un-propagated state lingers.
const PENDING_BROADCAST_TTL_MS: u64 = 5 * 60 * 1_000;

struct PendingEntry {
    state: WrappedState,
    inserted_at_ms: u64,
}

/// Bounded store of broadcasts that found no targets and are awaiting the first
/// interested peer to re-fan-out to. See module docs.
pub(crate) struct PendingBroadcastStore {
    entries: DashMap<ContractInstanceId, PendingEntry>,
}

impl PendingBroadcastStore {
    pub(crate) fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }

    /// Stash the latest broadcast `state` for `contract` after its initial
    /// broadcast exhausted its retry budget with zero targets. A later
    /// re-stash for the same contract overwrites the previous state (the newest
    /// state is the one worth propagating). Prunes expired entries and enforces
    /// the size cap on the way in.
    pub(crate) fn stash(&self, contract: ContractInstanceId, state: WrappedState) {
        let now_ms = GlobalSimulationTime::read_time_ms();
        self.prune_expired(now_ms);

        // Enforce the size cap only when inserting a genuinely new contract;
        // overwriting an existing entry never grows the map.
        if !self.entries.contains_key(&contract) && self.entries.len() >= MAX_PENDING_BROADCASTS {
            self.evict_oldest();
        }

        self.entries.insert(
            contract,
            PendingEntry {
                state,
                inserted_at_ms: now_ms,
            },
        );
    }

    /// Remove and return the pending broadcast state for `contract`, if any and
    /// not expired. Called when the first interested peer/subscriber appears so
    /// the caller can re-emit `BroadcastStateChange` and let it find the new
    /// target. An expired entry is removed and `None` is returned (no
    /// re-broadcast).
    pub(crate) fn take(&self, contract: &ContractInstanceId) -> Option<WrappedState> {
        let (_, entry) = self.entries.remove(contract)?;
        let now_ms = GlobalSimulationTime::read_time_ms();
        if now_ms.saturating_sub(entry.inserted_at_ms) >= PENDING_BROADCAST_TTL_MS {
            return None;
        }
        Some(entry.state)
    }

    /// Drop entries older than the TTL. Cheap: a single pass over the map, only
    /// performed on the (rare) stash path.
    fn prune_expired(&self, now_ms: u64) {
        self.entries
            .retain(|_, e| now_ms.saturating_sub(e.inserted_at_ms) < PENDING_BROADCAST_TTL_MS);
    }

    /// Evict the single oldest entry to make room under the size cap.
    fn evict_oldest(&self) {
        let oldest = self
            .entries
            .iter()
            .min_by_key(|e| e.value().inserted_at_ms)
            .map(|e| *e.key());
        if let Some(key) = oldest {
            self.entries.remove(&key);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    fn state(byte: u8) -> WrappedState {
        WrappedState::new(vec![byte; 8])
    }

    #[test]
    fn stash_then_take_returns_state_and_removes_entry() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1), state(0xAA));
        assert_eq!(store.len(), 1);

        let taken = store.take(&cid(1)).expect("pending state present");
        assert_eq!(taken.as_ref(), &[0xAA; 8]);
        assert_eq!(store.len(), 0, "take must remove the entry");
        assert!(store.take(&cid(1)).is_none(), "second take yields nothing");
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn restash_overwrites_with_newest_state() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1), state(0x01));
        store.stash(cid(1), state(0x02));
        assert_eq!(store.len(), 1, "re-stash must not duplicate the contract");
        assert_eq!(store.take(&cid(1)).unwrap().as_ref(), &[0x02; 8]);
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn expired_entry_is_not_rebroadcast() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1), state(0x01));

        // Advance past the TTL: the entry must be dropped, not returned.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        assert!(
            store.take(&cid(1)).is_none(),
            "an entry past its TTL must not be re-broadcast"
        );
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn prune_drops_expired_on_stash() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();
        store.stash(cid(1), state(0x01));

        // Advance past TTL, then stash a different contract: the prune on the
        // stash path must evict the stale entry.
        GlobalSimulationTime::set_time_ms(PENDING_BROADCAST_TTL_MS + 1);
        store.stash(cid(2), state(0x02));
        assert_eq!(store.len(), 1, "stale entry must be pruned on stash");
        assert!(store.take(&cid(1)).is_none());
        assert!(store.take(&cid(2)).is_some());
        GlobalSimulationTime::clear_time();
    }

    #[test]
    fn size_cap_evicts_oldest() {
        GlobalSimulationTime::set_time_ms(0);
        let store = PendingBroadcastStore::new();

        // Fill to capacity, each at a distinct (increasing) timestamp so
        // "oldest" is well-defined.
        for i in 0..MAX_PENDING_BROADCASTS {
            GlobalSimulationTime::set_time_ms(i as u64);
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            store.stash(ContractInstanceId::new(bytes), state(1));
        }
        assert_eq!(store.len(), MAX_PENDING_BROADCASTS);

        // One more (still within TTL) evicts the oldest (i == 0), keeping the
        // map at the cap.
        GlobalSimulationTime::set_time_ms(MAX_PENDING_BROADCASTS as u64);
        let overflow = {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xFF;
            bytes[16] = 0xFF;
            ContractInstanceId::new(bytes)
        };
        store.stash(overflow, state(2));
        assert_eq!(store.len(), MAX_PENDING_BROADCASTS, "cap holds");

        let oldest = {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&0u64.to_le_bytes());
            ContractInstanceId::new(bytes)
        };
        assert!(
            store.take(&oldest).is_none(),
            "oldest entry must have been evicted"
        );
        assert!(store.take(&overflow).is_some(), "new entry retained");
        GlobalSimulationTime::clear_time();
    }
}
