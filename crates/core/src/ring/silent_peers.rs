//! Per-node record of peers that recently failed to answer a GET.
//!
//! A GET forwarded to a peer that never replies costs the full attempt wait
//! (`OPERATION_TTL`, 60 s). The router learns from the timeout only through
//! its failure-probability model, which needs history before it changes a
//! ranking, so for the next minute or two the same node keeps forwarding
//! further GETs to the same silent peer. This record lets GET next-hop and
//! first-hop selection route around such a peer for a short window, when an
//! alternative exists.
//!
//! Scope, deliberately small:
//!
//! - It is a preference, never a veto. The selection code only honours it when
//!   another candidate (strictly closer to the key than this node) exists, so
//!   a sole holder that is down is still asked.
//! - It does not change any deadline or retry budget.
//! - It is not [`ConnectionManager::recently_failed_addrs`], which backs off
//!   CONNECT attempts to addresses we could not reach, with minute-to-hour TTLs.
//!   This tracks established connections that went quiet on one request type.
//! - It is not `PeerHealthTracker`, which decides connection eviction.
//!
//! A peer leaves the record when it times out of the window, when any message
//! arrives from it (a peer that talks is alive), or when its connection is
//! removed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;

/// How long a peer that did not answer a GET is deprioritised for GET routing.
///
/// Long enough to cover the retries of the GETs that queued behind the one
/// that timed out (each of those waits 60 s on the same peer), short enough
/// that a peer that was only briefly stalled is not shunned for long. The
/// 60-120 s range was chosen as roughly one to two attempt deadlines; it is
/// not derived from a measurement of how long real peers stay silent.
pub(crate) const SILENT_PEER_WINDOW: Duration = Duration::from_secs(90);

/// Maximum peers tracked at once. Entries are a socket address and an
/// `Instant`, so this bounds memory trivially; it exists so a node with many
/// failing peers cannot grow the map without limit. When full, the entry that
/// has been in the record longest is dropped (it is the closest to expiry).
pub(crate) const SILENT_PEER_CAP: usize = 64;

#[derive(Debug, Default)]
pub(crate) struct SilentPeers {
    entries: Mutex<HashMap<SocketAddr, Instant>>,
    /// Mirror of `entries.len()` so the per-inbound-message `clear` can skip
    /// the lock in the common case of an empty record.
    len: AtomicUsize,
}

impl SilentPeers {
    /// Record that `addr` got a GET and did not answer, at `now`.
    pub(crate) fn record(&self, addr: SocketAddr, now: Instant) {
        let mut entries = self.entries.lock();
        entries.retain(|_, at| now.saturating_duration_since(*at) < SILENT_PEER_WINDOW);
        if !entries.contains_key(&addr) && entries.len() >= SILENT_PEER_CAP {
            if let Some(oldest) = entries.iter().min_by_key(|(_, at)| **at).map(|(a, _)| *a) {
                entries.remove(&oldest);
            }
        }
        entries.insert(addr, now);
        self.len.store(entries.len(), Ordering::Relaxed);
    }

    /// Forget `addr`: it sent us a message, or its connection is gone.
    pub(crate) fn clear(&self, addr: SocketAddr) {
        if self.len.load(Ordering::Relaxed) == 0 {
            return;
        }
        let mut entries = self.entries.lock();
        entries.remove(&addr);
        self.len.store(entries.len(), Ordering::Relaxed);
    }

    /// The peers still inside their window at `now`.
    pub(crate) fn active(&self, now: Instant) -> Vec<SocketAddr> {
        if self.len.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let mut entries = self.entries.lock();
        entries.retain(|_, at| now.saturating_duration_since(*at) < SILENT_PEER_WINDOW);
        self.len.store(entries.len(), Ordering::Relaxed);
        let mut out: Vec<SocketAddr> = entries.keys().copied().collect();
        out.sort();
        out
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], 1000 + n))
    }

    #[test]
    fn record_then_expire() {
        let s = SilentPeers::default();
        let t0 = Instant::now();
        s.record(addr(1), t0);
        assert_eq!(s.active(t0 + Duration::from_secs(1)), vec![addr(1)]);
        assert_eq!(
            s.active(t0 + SILENT_PEER_WINDOW - Duration::from_millis(1)),
            vec![addr(1)]
        );
        assert!(s.active(t0 + SILENT_PEER_WINDOW).is_empty());
        assert_eq!(s.len(), 0, "expired entries are dropped, not just hidden");
    }

    #[test]
    fn rerecording_restarts_the_window() {
        let s = SilentPeers::default();
        let t0 = Instant::now();
        s.record(addr(1), t0);
        let t1 = t0 + Duration::from_secs(60);
        s.record(addr(1), t1);
        assert_eq!(s.active(t0 + Duration::from_secs(120)), vec![addr(1)]);
    }

    #[test]
    fn clear_removes_only_that_peer() {
        let s = SilentPeers::default();
        let t0 = Instant::now();
        s.record(addr(1), t0);
        s.record(addr(2), t0);
        s.clear(addr(1));
        assert_eq!(s.active(t0), vec![addr(2)]);
        s.clear(addr(3));
        assert_eq!(s.active(t0), vec![addr(2)]);
    }

    #[test]
    fn cap_drops_the_oldest_entry() {
        let s = SilentPeers::default();
        let t0 = Instant::now();
        for i in 0..SILENT_PEER_CAP as u16 {
            s.record(addr(i), t0 + Duration::from_millis(u64::from(i)));
        }
        assert_eq!(s.len(), SILENT_PEER_CAP);
        let newest = t0 + Duration::from_secs(1);
        s.record(addr(1000), newest);
        assert_eq!(s.len(), SILENT_PEER_CAP, "cap holds");
        let active = s.active(newest);
        assert!(active.contains(&addr(1000)));
        assert!(!active.contains(&addr(0)), "oldest was evicted");
        // Re-recording an existing peer never evicts another one.
        s.record(addr(1000), newest + Duration::from_millis(1));
        assert_eq!(s.len(), SILENT_PEER_CAP);
        assert!(s.active(newest).contains(&addr(1)));
    }
}
