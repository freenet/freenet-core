//! Bound on streaming PUT relay drivers still waiting for their stream
//! (ANT-2026-16KQ0AS7).
//!
//! A `PutMsg::RequestStreaming` starts a relay driver whose first step waits up
//! to `STREAM_CLAIM_TIMEOUT` (60 s) for the announced stream. The announcement
//! is a small message and the stream a separate transfer, so the number of
//! drivers waiting for a stream was decided by the peers sending
//! announcements; the transport's per-connection stream limits do not count an
//! announcement whose stream never comes.
//!
//! A permit covers only that UNCLAIMED phase: it is taken when the driver
//! starts and released as soon as the claim ends, whether the stream arrived or
//! not. What happens after the claim is bounded by the transport's
//! per-connection inbound-stream and byte limits, as for any stream.
//!
//! Two bounds:
//!
//! - **Per source**, at most [`PER_SOURCE_CAP`] unclaimed drivers. A source is
//!   the previous hop's IPv4 address or IPv6 /64 (an IPv4-mapped address counts
//!   as IPv4), so more ports or more addresses in one /64 add nothing. A new
//!   announcement over this cap is refused.
//! - **Node-wide**, at most [`NODE_WIDE_CAP`]. At this cap the OLDEST unclaimed
//!   driver is evicted to admit the newcomer, rather than refusing it. An
//!   honest stream is claimed within about one round trip of its announcement,
//!   so the oldest waiter is the one least likely to ever see its stream, and
//!   no set of sources can lock the node out by holding every permit for the
//!   claim timeout.
//!
//! A refused announcement and an evicted driver both answer their upstream with
//! a `PutMsg::Error` (one reply per announcement, so no amplification); the
//! upstream would otherwise wait out its whole attempt.

use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::oneshot;

/// Unclaimed streaming PUT relay drivers one source (the previous hop) may
/// have at once.
///
/// Sized for an honest gateway relaying many joiners' PUTs, which all count
/// against that gateway's address. A permit is held only from the
/// announcement until the stream is claimed, i.e. until fragment 1 arrives.
///
/// # How long fragment 1 can lag its announcement on a busy link
///
/// Outbound streams on one connection are INTERLEAVED, not sent one after
/// another. On the UDP session each stream is its own task
/// (`PeerConnection::outbound_stream_with_id` / `pipe_stream_to_remote`
/// push a spawned `send_stream` onto `outbound_stream_futures`), and each
/// task admits its packets one at a time: wait for congestion-window
/// space, then reserve that one packet from the connection's token bucket.
/// On the QUIC path quinn multiplexes streams itself. So fragment 1 of a
/// new stream does not wait for earlier transfers to finish. It waits for
/// window space, which opens within about one round trip, and for the
/// single packets the other active streams on the link have reserved ahead
/// of it, about one packet per active stream. The sender emits fragment 1
/// right after its announcement, and fragment 1 also carries a copy of it.
///
/// So the lag is about one round trip plus K packet times for K active
/// streams on the link: at K = 32 and 1.25 MB/s, about 36 ms on top of
/// the round trip. Taking 300 ms per permit as a generous figure, 32
/// permits let one previous hop have about 100 streaming PUTs per second
/// announced through it before any is refused. A streaming PUT carries
/// more than the 64 KiB streaming threshold, so that is at least 6.5 MB/s
/// of PUT payload over one link. If field data shows refusals from honest
/// gateways, raise this constant: the node-wide cap with eviction is what
/// bounds memory, not this one.
pub(crate) const PER_SOURCE_CAP: usize = 32;

/// Unclaimed streaming PUT relay drivers this node holds across all sources.
/// At this cap the oldest is evicted. A driver waiting for its stream holds
/// a few KB, so a full set is a few MiB.
pub(crate) const NODE_WIDE_CAP: usize = 512;

/// Where an announcement came from, at the granularity one host controls.
/// Also the source unit of `operations::relay_admission`, and groups the
/// transient connection budget's per-source share
/// (`ConnectionManager::try_register_transient`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Source {
    V4(u32),
    /// The top 64 bits of an IPv6 address: a host usually holds a whole /64.
    V6Prefix(u64),
}

impl Source {
    pub(crate) fn of(addr: SocketAddr) -> Self {
        match addr.ip() {
            IpAddr::V4(v4) => Source::V4(u32::from(v4)),
            IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                Some(v4) => Source::V4(u32::from(v4)),
                None => Source::V6Prefix((u128::from(v6) >> 64) as u64),
            },
        }
    }
}

/// Why an announcement was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    SourceAtCap,
}

pub(crate) struct RelayStreamAdmission {
    state: parking_lot::Mutex<State>,
    per_source_cap: usize,
    node_wide_cap: usize,
    refused_total: AtomicU64,
    evicted_total: AtomicU64,
}

/// One lock for both maps: eviction moves an entry out of `unclaimed` and
/// decrements its source in the same step, and the critical section is a
/// few map operations per streaming PUT.
#[derive(Default)]
struct State {
    next_seq: u64,
    per_source: HashMap<Source, usize>,
    /// Admitted, still-unclaimed drivers in admission order. The sender wakes
    /// the driver when it is evicted.
    unclaimed: BTreeMap<u64, (Source, oneshot::Sender<()>)>,
}

impl State {
    fn release_source(&mut self, source: Source) {
        if let Some(count) = self.per_source.get_mut(&source) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.per_source.remove(&source);
            }
        }
    }
}

/// One admitted driver's unclaimed phase. Dropping it releases the slot.
pub(crate) struct RelayStreamPermit {
    admission: Arc<RelayStreamAdmission>,
    seq: u64,
    evicted: oneshot::Receiver<()>,
}

impl RelayStreamPermit {
    /// Resolves if this driver is evicted to make room for a newer one.
    /// Never resolves otherwise.
    pub(crate) async fn evicted(&mut self) {
        if (&mut self.evicted).await.is_err() {
            // The sender is gone without an eviction: the slot was released,
            // which only this permit's own drop does.
            std::future::pending::<()>().await;
        }
    }
}

impl Drop for RelayStreamPermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock();
        // Absent if this driver was evicted: eviction already released it.
        if let Some((source, _)) = state.unclaimed.remove(&self.seq) {
            state.release_source(source);
        }
    }
}

impl RelayStreamAdmission {
    pub(crate) fn new() -> Self {
        Self::with_caps(PER_SOURCE_CAP, NODE_WIDE_CAP)
    }

    pub(crate) fn with_caps(per_source_cap: usize, node_wide_cap: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(State::default()),
            per_source_cap: per_source_cap.max(1),
            node_wide_cap: node_wide_cap.max(1),
            refused_total: AtomicU64::new(0),
            evicted_total: AtomicU64::new(0),
        }
    }

    /// Admit one driver for an announcement from `from`. At the node-wide cap
    /// the oldest unclaimed driver is evicted to make room; over the
    /// per-source cap the newcomer is refused.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        from: SocketAddr,
    ) -> Result<RelayStreamPermit, Refusal> {
        let source = Source::of(from);
        let mut state = self.state.lock();
        if state.per_source.get(&source).copied().unwrap_or(0) >= self.per_source_cap {
            drop(state);
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            return Err(Refusal::SourceAtCap);
        }
        while state.unclaimed.len() >= self.node_wide_cap {
            let Some((_, (victim_source, wake))) = state.unclaimed.pop_first() else {
                break;
            };
            state.release_source(victim_source);
            // An error only means the victim's driver is already gone (its
            // permit is dropping); its slot is released here either way.
            match wake.send(()) {
                Ok(()) | Err(()) => {}
            }
            self.evicted_total.fetch_add(1, Ordering::Relaxed);
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        let (wake, evicted) = oneshot::channel();
        state.unclaimed.insert(seq, (source, wake));
        *state.per_source.entry(source).or_insert(0) += 1;
        drop(state);
        Ok(RelayStreamPermit {
            admission: self.clone(),
            seq,
            evicted,
        })
    }

    /// Announcements refused so far.
    pub(crate) fn refused_total(&self) -> u64 {
        self.refused_total.load(Ordering::Relaxed)
    }

    /// Drivers evicted so far.
    pub(crate) fn evicted_total(&self) -> u64 {
        self.evicted_total.load(Ordering::Relaxed)
    }

    /// Drivers currently holding a permit.
    pub(crate) fn unclaimed(&self) -> usize {
        self.state.lock().unclaimed.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn one_host_on_many_ports_shares_one_budget() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(3, 100));
        let permits: Vec<_> = (0..3)
            .map(|port| {
                admission
                    .try_admit(addr(&format!("203.0.113.7:{}", 40000 + port)))
                    .expect("under the source cap")
            })
            .collect();
        assert_eq!(
            admission.try_admit(addr("203.0.113.7:50000")).err(),
            Some(Refusal::SourceAtCap),
            "a fresh port on the same address must not get a fresh budget"
        );
        assert!(
            admission.try_admit(addr("203.0.113.8:40000")).is_ok(),
            "another host is unaffected"
        );
        assert_eq!(admission.refused_total(), 1);
        drop(permits);
    }

    #[test]
    fn an_ipv6_slash_64_is_one_source() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(2, 100));
        let _a = admission.try_admit(addr("[2001:db8:1:2::1]:1000")).unwrap();
        let _b = admission
            .try_admit(addr("[2001:db8:1:2:ffff::9]:2000"))
            .unwrap();
        assert_eq!(
            admission
                .try_admit(addr("[2001:db8:1:2:abcd::5]:3000"))
                .err(),
            Some(Refusal::SourceAtCap)
        );
        assert!(admission.try_admit(addr("[2001:db8:1:3::1]:1000")).is_ok());
    }

    #[test]
    fn ipv4_mapped_ipv6_counts_as_the_ipv4_address() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(1, 100));
        let _a = admission.try_admit(addr("198.51.100.4:1")).unwrap();
        assert_eq!(
            admission.try_admit(addr("[::ffff:198.51.100.4]:2")).err(),
            Some(Refusal::SourceAtCap)
        );
    }

    /// At the node-wide cap the newcomer is admitted and the oldest unclaimed
    /// driver is told it was evicted; its slot and its source's count are
    /// released at once.
    #[tokio::test]
    async fn the_node_wide_cap_evicts_the_oldest_unclaimed_driver() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(10, 2));
        let mut oldest = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        let mut second = admission.try_admit(addr("192.0.2.2:1")).unwrap();
        let _newcomer = admission
            .try_admit(addr("192.0.2.3:1"))
            .expect("at the node-wide cap a newcomer is admitted");
        assert_eq!(admission.unclaimed(), 2);
        assert_eq!(admission.evicted_total(), 1);
        tokio::time::timeout(std::time::Duration::from_secs(1), oldest.evicted())
            .await
            .expect("the oldest driver is told it was evicted");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), second.evicted())
                .await
                .is_err(),
            "a younger driver is not evicted"
        );
        drop(oldest);
        assert_eq!(
            admission.unclaimed(),
            2,
            "an evicted permit's drop releases nothing twice"
        );
        // The evicted source's count was released: it may admit again.
        let source_only_one = Arc::new(RelayStreamAdmission::with_caps(1, 1));
        let first = source_only_one.try_admit(addr("192.0.2.9:1")).unwrap();
        let _replacement = source_only_one.try_admit(addr("192.0.2.8:1")).unwrap();
        drop(first);
        assert!(source_only_one.try_admit(addr("192.0.2.9:2")).is_ok());
    }

    #[test]
    fn dropping_a_permit_returns_its_slots() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(1, 4));
        let permit = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        assert!(admission.try_admit(addr("192.0.2.1:2")).is_err());
        drop(permit);
        assert_eq!(admission.unclaimed(), 0);
        assert!(
            admission.state.lock().per_source.is_empty(),
            "an idle source keeps no entry"
        );
        assert!(admission.try_admit(addr("192.0.2.1:2")).is_ok());
    }

    /// A refusal at the source cap takes no node-wide slot and evicts nobody.
    #[test]
    fn a_source_refusal_evicts_nobody() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(1, 2));
        let _a = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        let _b = admission.try_admit(addr("192.0.2.2:1")).unwrap();
        for port in 2..50 {
            assert!(
                admission
                    .try_admit(addr(&format!("192.0.2.1:{port}")))
                    .is_err()
            );
        }
        assert_eq!(admission.unclaimed(), 2);
        assert_eq!(admission.evicted_total(), 0);
    }
}
