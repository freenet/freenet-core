//! Node-local memory of the `RequestUpdate` transactions this node has already
//! relayed (#3911, ANT-2026-B9HNLA3P).
//!
//! `UpdateMsg::RequestUpdate` and `RequestUpdateStreaming` carry no hops-to-live
//! and no visited list, and each relay excludes only itself and the previous hop
//! when it picks the next one. For a contract no reachable peer hosts, the
//! request can therefore come back to a node it already passed through. The
//! per-node in-flight gate (`OpManager::active_relay_update_txs`) does not stop
//! that: its entry is removed as soon as the relay driver has queued the
//! forward, well before the request returns, so the cycle went on until a link
//! dropped or a node restarted.
//!
//! Adding a hop counter to the wire variants would break decoding on every peer
//! that predates it (bincode is positional, and the repo's rule is to append a
//! new variant rather than add a field to an existing one). This node-local
//! record needs no wire change: a relay remembers each request transaction it
//! accepted for at least [`RETENTION`], and drops a request whose transaction it
//! has already seen. Every node in a cycle remembers the transaction, so the
//! cycle ends the first time the request returns to any of them.
//!
//! An honest request is relayed once per node: the originator creates the
//! transaction once and every hop forwards it under the same id, and a relay
//! never sends the same request back to a peer on the path except in a cycle.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

use crate::message::Transaction;

/// How long a relayed request transaction is remembered, at least.
///
/// A cycle survives only if one lap takes longer than this. A lap is a few hops
/// of transfer time: a request near `MAX_STATE_SIZE` (50 MiB) at the default
/// 1.25 MB/s per-connection pacing takes about 42 s per hop, so ten minutes
/// covers a lap of more than a dozen such hops. An entry lives between one and
/// two of these periods, see [`RecentRelayedRequests`].
pub(crate) const RETENTION: Duration = Duration::from_secs(600);

/// Most transactions held in one generation. Two generations are kept, so the
/// record holds at most twice this many ids (16 bytes each, about 1-2 MiB with
/// table overhead).
///
/// When a generation fills before [`RETENTION`] has passed it is rotated early,
/// which shortens how long the oldest ids are remembered. Doing that needs
/// more than this many distinct request transactions to reach this node within
/// one lap of a cycle; each of them is charged to the per-(sender, contract)
/// UPDATE rate limiter and the per-sender new-pair budget before it gets here.
pub(crate) const GENERATION_CAP: usize = 32_768;

/// Two-generation set of recently relayed request transactions.
///
/// A plain `Mutex` rather than a `DashSet`: rotation moves the whole current
/// generation to the previous one, which has to be atomic with the lookup in
/// both, and the critical section is a couple of hash operations.
pub(crate) struct RecentRelayedRequests {
    inner: parking_lot::Mutex<Generations>,
    retention: Duration,
    generation_cap: usize,
    /// Requests dropped because their transaction had already been relayed
    /// here. Counted so a drop is visible in release builds, where the
    /// per-drop `debug!` is compiled out.
    returned_total: AtomicU64,
}

struct Generations {
    current: HashSet<Transaction>,
    previous: HashSet<Transaction>,
    current_started: Option<Instant>,
}

impl RecentRelayedRequests {
    pub(crate) fn new() -> Self {
        Self::with_limits(RETENTION, GENERATION_CAP)
    }

    pub(crate) fn with_limits(retention: Duration, generation_cap: usize) -> Self {
        Self {
            inner: parking_lot::Mutex::new(Generations {
                current: HashSet::new(),
                previous: HashSet::new(),
                current_started: None,
            }),
            retention,
            generation_cap: generation_cap.max(1),
            returned_total: AtomicU64::new(0),
        }
    }

    /// Record `tx` as relayed by this node. Returns `true` the first time a
    /// transaction is seen within the retention window and `false` when it has
    /// already been relayed here, in which case the caller must drop it.
    pub(crate) fn first_sighting(&self, tx: Transaction, now: Instant) -> bool {
        let mut g = self.inner.lock();
        let started = *g.current_started.get_or_insert(now);
        let age = now.saturating_duration_since(started);
        if age >= self.retention.saturating_mul(2) {
            // Both generations are older than the retention window.
            g.current.clear();
            g.previous.clear();
            g.current_started = Some(now);
        } else if age >= self.retention || g.current.len() >= self.generation_cap {
            let Generations {
                current, previous, ..
            } = &mut *g;
            std::mem::swap(current, previous);
            current.clear();
            g.current_started = Some(now);
        }
        if g.current.contains(&tx) || g.previous.contains(&tx) {
            drop(g);
            self.returned_total.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        g.current.insert(tx);
        true
    }

    /// Requests dropped so far because they returned to this node.
    pub(crate) fn returned_total(&self) -> u64 {
        self.returned_total.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        let g = self.inner.lock();
        g.current.len() + g.previous.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::update::UpdateMsg;

    fn tx() -> Transaction {
        Transaction::new::<UpdateMsg>()
    }

    #[test]
    fn a_returning_transaction_is_refused_and_counted() {
        let recent = RecentRelayedRequests::new();
        let now = Instant::now();
        let t = tx();
        assert!(recent.first_sighting(t, now));
        assert!(!recent.first_sighting(t, now + Duration::from_secs(1)));
        assert!(!recent.first_sighting(t, now + RETENTION - Duration::from_secs(1)));
        assert_eq!(recent.returned_total(), 2);
        assert!(
            recent.first_sighting(tx(), now),
            "a different tx is admitted"
        );
    }

    /// An id is remembered for at least the retention period across a rotation,
    /// and forgotten once both generations have aged out.
    #[test]
    fn retention_survives_one_rotation_and_then_expires() {
        let retention = Duration::from_secs(10);
        let recent = RecentRelayedRequests::with_limits(retention, 1024);
        let start = Instant::now();
        let t = tx();
        // Opens the first generation at `start`.
        assert!(recent.first_sighting(tx(), start));
        assert!(recent.first_sighting(t, start + Duration::from_secs(9)));
        // Rotates the generation holding `t` into `previous`.
        assert!(recent.first_sighting(tx(), start + Duration::from_secs(10)));
        assert!(
            !recent.first_sighting(t, start + Duration::from_secs(18)),
            "an id must be remembered for the whole retention period"
        );
        assert!(
            recent.first_sighting(t, start + Duration::from_secs(40)),
            "an id older than two periods is forgotten"
        );
    }

    #[test]
    fn the_record_never_holds_more_than_two_generations() {
        let recent = RecentRelayedRequests::with_limits(Duration::from_secs(600), 8);
        let now = Instant::now();
        for _ in 0..100 {
            assert!(recent.first_sighting(tx(), now));
            assert!(recent.len() <= 16, "held {}", recent.len());
        }
    }
}
