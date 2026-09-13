//! Delayed labelling store for ambiguous `NotFound` route attempts (#4485).
//!
//! An operation that exhausts with only `NotFound` (and possibly timeout)
//! outcomes cannot tell "this contract exists but routing dead-ended" (a real
//! routing failure of each peer asked) from "this contract does not exist" (no
//! information about any peer). Training on the second kind is label noise, and
//! a synthetic bake-off showed it is not harmless even when absent keys are
//! uniform: model error rose 1.5-1.8x at 5 % absent requests and 6-9x at 20 %,
//! and best-of-10 candidate selection dropped 7-11 points versus delayed
//! labelling at equal data.
//!
//! So under [`AmbiguousNotFoundPolicy::Delayed`] such attempts are PARKED here,
//! keyed by contract, and turned into `Failure` labels only if this node later
//! sees evidence the contract exists (see `Ring::release_parked_not_found`).
//! Parked attempts that expire are never trained.
//!
//! # Bounds
//!
//! - Every entry lives at most [`PARKED_NOT_FOUND_TTL`]. Nothing refreshes an
//!   entry, so per `.claude/rules/code-style.md` the caps REJECT new entries
//!   rather than evicting incumbents: incumbents roll off on their own within
//!   one TTL, so a newcomer's wait is bounded.
//! - At most [`MAX_PARKED_NOT_FOUND`] entries in total, and at most
//!   [`MAX_PARKED_NOT_FOUND_PER_CONTRACT`] per contract, so one hot missing key
//!   cannot hold the whole store.
//! - Entry values are fixed-size (a peer key and address, a location, an op
//!   type and an instant), so the entry cap is also the byte bound: a few
//!   hundred KiB at the cap.
//!
//! Saturation and outcomes are counted ([`ParkedNotFoundStats`]) and logged at
//! info level from the ring maintenance loop, so they are visible in release
//! builds.
//!
//! [`AmbiguousNotFoundPolicy::Delayed`]: crate::operations::route_attempt::AmbiguousNotFoundPolicy::Delayed

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::node::network_status::OpType;
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

/// How long a parked attempt waits for evidence that its contract exists.
pub(crate) const PARKED_NOT_FOUND_TTL: Duration = Duration::from_secs(5 * 60);

/// Total parked attempts across all contracts.
pub(crate) const MAX_PARKED_NOT_FOUND: usize = 2048;

/// Parked attempts for any single contract.
pub(crate) const MAX_PARKED_NOT_FOUND_PER_CONTRACT: usize = 32;

/// Cumulative outcome counters. Every parked attempt ends in exactly one of
/// `released`, `expired`; an attempt refused at a cap is counted in `rejected`
/// and never parked.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParkedNotFoundStats {
    pub parked: u64,
    pub released: u64,
    pub expired: u64,
    pub rejected: u64,
    /// Entries currently parked.
    pub live: u64,
}

struct Parked {
    instance_id: ContractInstanceId,
    peer: PeerKeyLocation,
    op_type: OpType,
    parked_at: Instant,
}

#[derive(Default)]
struct Inner {
    next_seq: u64,
    /// All live entries in parking order, which is also expiry order.
    by_seq: BTreeMap<u64, Parked>,
    /// Live sequence numbers per contract.
    by_contract: HashMap<ContractInstanceId, Vec<u64>>,
}

#[derive(Default)]
pub(crate) struct ParkedNotFoundStore {
    inner: Mutex<Inner>,
    /// Mirror of the live entry count, so the per-state-write `release` hook
    /// can skip the lock when nothing is parked (the common case).
    live: AtomicUsize,
    parked: AtomicU64,
    released: AtomicU64,
    expired: AtomicU64,
    rejected: AtomicU64,
}

impl ParkedNotFoundStore {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Park the `NotFound` attempts one operation made against `peers` for
    /// `instance_id`. Attempts beyond a cap are rejected (counted, not parked).
    pub(crate) fn park(
        &self,
        instance_id: ContractInstanceId,
        op_type: OpType,
        peers: Vec<PeerKeyLocation>,
        now: Instant,
    ) {
        if peers.is_empty() {
            return;
        }
        let mut inner = self.inner.lock();
        self.purge_expired_locked(&mut inner, now);
        for peer in peers {
            let for_contract = inner.by_contract.get(&instance_id).map_or(0, Vec::len);
            if inner.by_seq.len() >= MAX_PARKED_NOT_FOUND
                || for_contract >= MAX_PARKED_NOT_FOUND_PER_CONTRACT
            {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let seq = inner.next_seq;
            inner.next_seq += 1;
            inner.by_seq.insert(
                seq,
                Parked {
                    instance_id,
                    peer,
                    op_type,
                    parked_at: now,
                },
            );
            inner.by_contract.entry(instance_id).or_default().push(seq);
            self.parked.fetch_add(1, Ordering::Relaxed);
        }
        self.live.store(inner.by_seq.len(), Ordering::Relaxed);
    }

    /// Evidence that `instance_id` exists: remove its unexpired parked attempts
    /// and return them as `Failure` route events, in parking order.
    pub(crate) fn release(
        &self,
        instance_id: &ContractInstanceId,
        now: Instant,
    ) -> Vec<RouteEvent> {
        if self.live.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let mut inner = self.inner.lock();
        self.purge_expired_locked(&mut inner, now);
        let Some(seqs) = inner.by_contract.remove(instance_id) else {
            return Vec::new();
        };
        let events: Vec<RouteEvent> = seqs
            .into_iter()
            .filter_map(|seq| inner.by_seq.remove(&seq))
            .map(|p| RouteEvent {
                peer: p.peer,
                contract_location: Location::from(&p.instance_id),
                outcome: RouteOutcome::Failure,
                op_type: Some(p.op_type),
            })
            .collect();
        self.live.store(inner.by_seq.len(), Ordering::Relaxed);
        self.released
            .fetch_add(events.len() as u64, Ordering::Relaxed);
        events
    }

    /// Drop every entry parked at least [`PARKED_NOT_FOUND_TTL`] ago. Called
    /// from `park`/`release` and periodically from the ring maintenance loop.
    pub(crate) fn purge_expired(&self, now: Instant) {
        let mut inner = self.inner.lock();
        self.purge_expired_locked(&mut inner, now);
    }

    fn purge_expired_locked(&self, inner: &mut Inner, now: Instant) {
        while let Some(entry) = inner.by_seq.first_entry() {
            if now.saturating_duration_since(entry.get().parked_at) < PARKED_NOT_FOUND_TTL {
                break;
            }
            let (seq, parked) = entry.remove_entry();
            if let Some(seqs) = inner.by_contract.get_mut(&parked.instance_id) {
                seqs.retain(|s| *s != seq);
                if seqs.is_empty() {
                    inner.by_contract.remove(&parked.instance_id);
                }
            }
            self.expired.fetch_add(1, Ordering::Relaxed);
        }
        self.live.store(inner.by_seq.len(), Ordering::Relaxed);
    }

    pub(crate) fn stats(&self) -> ParkedNotFoundStats {
        let live = self.inner.lock().by_seq.len() as u64;
        ParkedNotFoundStats {
            parked: self.parked.load(Ordering::Relaxed),
            released: self.released.load(Ordering::Relaxed),
            expired: self.expired.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            live,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn peer(port: u16) -> PeerKeyLocation {
        PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            format!("10.1.0.1:{port}").parse().unwrap(),
        )
    }

    fn id(b: u8) -> ContractInstanceId {
        ContractInstanceId::new([b; 32])
    }

    fn addrs(events: &[RouteEvent]) -> Vec<std::net::SocketAddr> {
        events
            .iter()
            .map(|e| e.peer.socket_addr().unwrap())
            .collect()
    }

    #[test]
    fn release_returns_parked_attempts_as_failures_once() {
        let store = ParkedNotFoundStore::new();
        let t0 = Instant::now();
        let (a, b) = (peer(1), peer(2));
        store.park(id(1), OpType::Get, vec![a.clone(), b.clone()], t0);
        store.park(id(2), OpType::Subscribe, vec![peer(3)], t0);

        let events = store.release(&id(1), t0 + Duration::from_secs(10));
        assert_eq!(
            addrs(&events),
            vec![a.socket_addr().unwrap(), b.socket_addr().unwrap()]
        );
        assert!(
            events
                .iter()
                .all(|e| matches!(e.outcome, RouteOutcome::Failure))
        );
        assert!(
            events
                .iter()
                .all(|e| e.contract_location == Location::from(&id(1)))
        );
        assert!(events.iter().all(|e| e.op_type == Some(OpType::Get)));

        assert!(
            store
                .release(&id(1), t0 + Duration::from_secs(11))
                .is_empty(),
            "a released attempt must not be released twice"
        );
        let stats = store.stats();
        assert_eq!(
            (stats.parked, stats.released, stats.expired, stats.live),
            (3, 2, 0, 1)
        );
    }

    #[test]
    fn release_for_an_unknown_contract_is_empty() {
        let store = ParkedNotFoundStore::new();
        store.park(id(1), OpType::Get, vec![peer(1)], Instant::now());
        assert!(store.release(&id(9), Instant::now()).is_empty());
        assert_eq!(store.stats().live, 1);
    }

    #[test]
    fn expired_attempts_are_never_released() {
        let store = ParkedNotFoundStore::new();
        let t0 = Instant::now();
        store.park(id(1), OpType::Get, vec![peer(1)], t0);
        store.park(
            id(1),
            OpType::Get,
            vec![peer(2)],
            t0 + Duration::from_secs(60),
        );

        // The first entry is exactly TTL old: expired. The second is not.
        let events = store.release(&id(1), t0 + PARKED_NOT_FOUND_TTL);
        assert_eq!(addrs(&events), vec!["10.1.0.1:2".parse().unwrap()]);
        let stats = store.stats();
        assert_eq!((stats.released, stats.expired, stats.live), (1, 1, 0));
    }

    #[test]
    fn purge_expires_without_release_and_counts() {
        let store = ParkedNotFoundStore::new();
        let t0 = Instant::now();
        store.park(id(1), OpType::Get, vec![peer(1), peer(2)], t0);
        store.purge_expired(t0 + PARKED_NOT_FOUND_TTL - Duration::from_millis(1));
        assert_eq!(store.stats().live, 2, "not yet expired");
        store.purge_expired(t0 + PARKED_NOT_FOUND_TTL);
        let stats = store.stats();
        assert_eq!((stats.expired, stats.live), (2, 0));
        assert!(store.release(&id(1), t0 + PARKED_NOT_FOUND_TTL).is_empty());
    }

    #[test]
    fn per_contract_cap_rejects_and_leaves_room_for_other_contracts() {
        let store = ParkedNotFoundStore::new();
        let now = Instant::now();
        let hot: Vec<_> = (0..MAX_PARKED_NOT_FOUND_PER_CONTRACT + 5)
            .map(|i| peer(i as u16))
            .collect();
        store.park(id(1), OpType::Get, hot, now);
        store.park(id(2), OpType::Get, vec![peer(9000)], now);
        let stats = store.stats();
        assert_eq!(stats.rejected, 5);
        assert_eq!(stats.live, MAX_PARKED_NOT_FOUND_PER_CONTRACT as u64 + 1);
        assert_eq!(
            store.release(&id(1), now).len(),
            MAX_PARKED_NOT_FOUND_PER_CONTRACT
        );
        assert_eq!(store.release(&id(2), now).len(), 1);
    }

    #[test]
    fn global_cap_rejects_new_entries_until_incumbents_expire() {
        let store = ParkedNotFoundStore::new();
        let t0 = Instant::now();
        let contracts = MAX_PARKED_NOT_FOUND / MAX_PARKED_NOT_FOUND_PER_CONTRACT;
        for c in 0..contracts {
            let peers = (0..MAX_PARKED_NOT_FOUND_PER_CONTRACT)
                .map(|i| peer(i as u16))
                .collect();
            let mut raw = [0u8; 32];
            raw[..8].copy_from_slice(&(c as u64).to_le_bytes());
            store.park(ContractInstanceId::new(raw), OpType::Get, peers, t0);
        }
        assert_eq!(store.stats().live, MAX_PARKED_NOT_FOUND as u64);

        let late = id(0xEE);
        store.park(
            late,
            OpType::Get,
            vec![peer(1)],
            t0 + Duration::from_secs(1),
        );
        let stats = store.stats();
        assert_eq!(stats.rejected, 1, "a full store rejects, never evicts");
        assert_eq!(stats.live, MAX_PARKED_NOT_FOUND as u64);

        // Once the incumbents age out, the newcomer is admitted.
        store.park(late, OpType::Get, vec![peer(2)], t0 + PARKED_NOT_FOUND_TTL);
        let stats = store.stats();
        assert_eq!(stats.expired, MAX_PARKED_NOT_FOUND as u64);
        assert_eq!(stats.live, 1);
        assert_eq!(store.release(&late, t0 + PARKED_NOT_FOUND_TTL).len(), 1);
    }
}
