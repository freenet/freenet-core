//! Per-contract tracking of detected CRDT-invariant violations.
//!
//! A non-idempotent `update_state` (i.e. `update_state(update_state(S, U), U) != update_state(S, U)`)
//! breaks the convergence guarantee Freenet contracts rely on and produces a
//! self-perpetuating broadcast storm: every peer's merge generates a new
//! byte-different state, which propagates, which every peer re-merges.
//!
//! When the merge path's idempotency probe (in `Executor`) detects this on a
//! sampled merge, it calls [`BrokenInvariantsTracker::record`] which:
//!
//! 1. Inserts the flag into an in-memory `DashMap` keyed on the contract
//!    instance id, so subsequent same-process reads see it immediately.
//! 2. Persists the flag to the executor's ReDb storage so the detection
//!    survives node restarts (a known-broken contract should not re-engage
//!    the storm just because the process bounced).
//!
//! Reads (via [`BrokenInvariantsTracker::is_broken`]) gate the broadcast-
//! emission path so a flagged contract's state changes never leave the
//! node. A "fixed" version of the same contract has a different code hash
//! → different `ContractKey` → unaffected, so this flag is intentionally
//! permanent for the given instance id.

use std::sync::Arc;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;

use crate::contract::storages::Storage;

/// The kind of CRDT invariant a contract was observed to violate. Currently
/// only one variant; future work may add violations like non-commutativity
/// or non-determinism detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokenInvariant {
    /// `update_state(update_state(S, U), U) != update_state(S, U)` was observed
    /// for at least one sampled (state, update) pair.
    NonIdempotent,
}

impl BrokenInvariant {
    /// Single-byte on-disk encoding. Stable across releases.
    fn to_byte(self) -> u8 {
        match self {
            BrokenInvariant::NonIdempotent => 0,
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b {
            0 => Some(BrokenInvariant::NonIdempotent),
            _ => None,
        }
    }
}

/// Tracks per-contract broken-invariant flags with optional persistent backing.
///
/// `set_storage` is called once during node startup, at which point the
/// tracker reads any previously-persisted flags into the in-memory map.
/// Until storage is wired, reads still work (empty map) and writes are
/// in-memory only — this matches the pattern used by `HostingManager`.
#[derive(Default)]
pub(crate) struct BrokenInvariantsTracker {
    flags: Arc<DashMap<ContractInstanceId, BrokenInvariant>>,
    storage: std::sync::OnceLock<Storage>,
}

impl BrokenInvariantsTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if `id` has any broken-invariant flag set.
    pub fn is_broken(&self, id: &ContractInstanceId) -> bool {
        self.flags.contains_key(id)
    }

    /// Returns the broken-invariant flag for `id`, if any. Used by tests
    /// and future detectors that distinguish kinds; production callers
    /// only need `is_broken`.
    #[cfg(test)]
    pub fn get(&self, id: &ContractInstanceId) -> Option<BrokenInvariant> {
        self.flags.get(id).map(|r| *r.value())
    }

    /// Mark `id` as broken with `kind`. Idempotent — repeat calls are no-ops
    /// once the in-memory entry exists. Persists best-effort to storage if
    /// wired; persistence errors are logged but never block the in-memory
    /// flag from taking effect (we'd rather suppress the storm than crash
    /// on a database hiccup).
    pub fn record(&self, id: ContractInstanceId, kind: BrokenInvariant) {
        let was_new = self.flags.insert(id, kind).is_none();
        if was_new {
            tracing::warn!(
                contract = %id,
                invariant = ?kind,
                event = "broken_invariant_detected",
                "Marking contract as broken — gating outbound broadcast and merge propagation"
            );
            // Persistence is currently only wired for the redb backend;
            // sqlite-only builds keep the in-memory flag but skip the
            // on-disk hydration. This is the same trade-off
            // `HostingManager` makes — see #4279 deferred follow-up to
            // add sqlite parity.
            #[cfg(feature = "redb")]
            if let Some(storage) = self.storage.get() {
                if let Err(e) = storage.store_broken_invariant(&id, kind.to_byte()) {
                    tracing::warn!(
                        contract = %id,
                        error = %e,
                        "Failed to persist broken-invariant flag (in-memory flag still active)"
                    );
                }
            }
        }
    }

    /// Remove the broken-invariant flag for `id`. Use with extreme care:
    /// this is the operator escape hatch for the rare case where the
    /// probe was a false positive (most plausibly a probe trap that
    /// shouldn't have been observed at all — see
    /// `Executor::maybe_probe_idempotency`). Re-enables outbound
    /// broadcast and commit for the contract. Does not unflag remotely.
    ///
    /// Not currently exposed to a CLI / WS API by this PR — added so the
    /// debug-CLI follow-up has a stable surface to call into. Returns
    /// the previous flag if any.
    #[allow(dead_code)] // wired in follow-up PR
    pub fn clear(&self, id: &ContractInstanceId) -> Option<BrokenInvariant> {
        let previous = self.flags.remove(id).map(|(_, v)| v);
        if previous.is_some() {
            tracing::warn!(
                contract = %id,
                event = "broken_invariant_cleared",
                "Operator cleared broken-invariant flag — outbound broadcast re-enabled"
            );
        }
        previous
    }

    /// Wire persistent storage. Called once at startup; idempotent on the
    /// `OnceLock` so callers cannot accidentally re-init with a different
    /// database. Hydrates the in-memory map from previously-persisted
    /// entries on first wiring.
    pub fn set_storage(&self, storage: Storage) {
        if self.storage.set(storage.clone()).is_err() {
            tracing::warn!("BrokenInvariantsTracker storage already set; ignoring re-init");
            return;
        }
        #[cfg(feature = "redb")]
        match storage.load_all_broken_invariants() {
            Ok(entries) => {
                for (id, byte) in entries {
                    if let Some(kind) = BrokenInvariant::from_byte(byte) {
                        self.flags.insert(id, kind);
                    } else {
                        tracing::warn!(
                            contract = %id,
                            byte,
                            "Skipping unknown broken-invariant byte on load"
                        );
                    }
                }
                tracing::debug!(
                    count = self.flags.len(),
                    "Loaded broken-invariant flags from storage"
                );
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load broken-invariant flags from storage");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_id(seed: u8) -> ContractInstanceId {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        ContractInstanceId::new(bytes)
    }

    #[test]
    fn record_then_query_returns_true() {
        let t = BrokenInvariantsTracker::new();
        let id = fake_id(1);
        assert!(!t.is_broken(&id));
        t.record(id, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&id));
        assert_eq!(t.get(&id), Some(BrokenInvariant::NonIdempotent));
    }

    #[test]
    fn record_is_idempotent() {
        let t = BrokenInvariantsTracker::new();
        let id = fake_id(2);
        t.record(id, BrokenInvariant::NonIdempotent);
        t.record(id, BrokenInvariant::NonIdempotent);
        // No panic, no duplicate entries; map size remains 1.
        assert!(t.is_broken(&id));
    }

    #[test]
    fn unrelated_contracts_unaffected() {
        let t = BrokenInvariantsTracker::new();
        let broken = fake_id(3);
        let healthy = fake_id(4);
        t.record(broken, BrokenInvariant::NonIdempotent);
        assert!(t.is_broken(&broken));
        assert!(!t.is_broken(&healthy));
    }

    #[test]
    fn byte_roundtrip_stable() {
        // Iterating the (currently single-variant) set keeps the test
        // honest as new BrokenInvariant kinds are added.
        let kinds: &[BrokenInvariant] = &[BrokenInvariant::NonIdempotent];
        for kind in kinds {
            assert_eq!(BrokenInvariant::from_byte(kind.to_byte()), Some(*kind));
        }
        // Unknown bytes are rejected (forward-compat: skip rather than panic).
        assert_eq!(BrokenInvariant::from_byte(255), None);
    }
}
