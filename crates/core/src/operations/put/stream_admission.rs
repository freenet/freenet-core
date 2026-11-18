//! Concurrency bound on streaming PUT relay drivers (ANT-2026-16KQ0AS7).
//!
//! A `PutMsg::RequestStreaming` spawns a relay driver whose first step waits up
//! to `STREAM_CLAIM_TIMEOUT` (60 s) for the announced stream. The announcement
//! is a small message and the stream is a separate transfer, so without a bound
//! the number of waiting drivers, and the memory they hold, was decided by the
//! peers sending announcements. The transport bounds inbound streams per
//! connection, but an announcement whose stream never comes is counted against
//! nothing there.
//!
//! This admits a new driver only while the announcing source has fewer than
//! [`PER_SOURCE_CAP`] drivers in flight and the node has fewer than
//! [`NODE_WIDE_CAP`]. A source is one IPv4 address or one IPv6 /64, so a host
//! does not gain capacity by opening connections from more ports or more
//! addresses in its own prefix. A permit is held for the driver's whole life and
//! released when it ends, so an honest sender's capacity comes back as its
//! streams complete.
//!
//! A refused announcement is dropped without a reply. Replying would turn every
//! refused message into an outbound one; the upstream's own response timeout
//! already ends its wait.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;

/// Streaming PUT relay drivers one source may have in flight at once.
///
/// A PUT large enough to stream is rare, and a relay holds one driver per PUT
/// it is passing on, so an honest peer, or several honest peers behind one
/// NAT, stays far below this.
pub(crate) const PER_SOURCE_CAP: usize = 32;

/// Streaming PUT relay drivers this node runs at once across all sources.
///
/// A driver waiting for a stream that never arrives holds about 6.5 KB, so a
/// full set of idle drivers is a few MiB.
pub(crate) const NODE_WIDE_CAP: usize = 512;

/// Where an announcement came from, at the granularity one host controls.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Source {
    V4(u32),
    /// The top 64 bits of an IPv6 address: a host usually holds a whole /64.
    V6Prefix(u64),
}

impl Source {
    fn of(addr: SocketAddr) -> Self {
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
    NodeAtCap,
}

pub(crate) struct RelayStreamAdmission {
    per_source: DashMap<Source, usize>,
    total: AtomicUsize,
    per_source_cap: usize,
    node_wide_cap: usize,
    refused_total: AtomicU64,
}

/// One admitted driver. Dropping it releases the slot.
pub(crate) struct RelayStreamPermit {
    admission: Arc<RelayStreamAdmission>,
    source: Source,
}

impl RelayStreamAdmission {
    pub(crate) fn new() -> Self {
        Self::with_caps(PER_SOURCE_CAP, NODE_WIDE_CAP)
    }

    pub(crate) fn with_caps(per_source_cap: usize, node_wide_cap: usize) -> Self {
        Self {
            per_source: DashMap::new(),
            total: AtomicUsize::new(0),
            per_source_cap,
            node_wide_cap,
            refused_total: AtomicU64::new(0),
        }
    }

    /// Admit one driver for an announcement from `from`, or say why not.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        from: SocketAddr,
    ) -> Result<RelayStreamPermit, Refusal> {
        // Reserve the node-wide slot first; give it back if the source is full.
        let reserved = self
            .total
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < self.node_wide_cap).then_some(n + 1)
            });
        if reserved.is_err() {
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            return Err(Refusal::NodeAtCap);
        }
        let source = Source::of(from);
        let mut count = self.per_source.entry(source).or_insert(0);
        if *count >= self.per_source_cap {
            drop(count);
            self.total.fetch_sub(1, Ordering::AcqRel);
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            return Err(Refusal::SourceAtCap);
        }
        *count += 1;
        drop(count);
        Ok(RelayStreamPermit {
            admission: self.clone(),
            source,
        })
    }

    /// Announcements refused so far.
    pub(crate) fn refused_total(&self) -> u64 {
        self.refused_total.load(Ordering::Relaxed)
    }

    /// Drivers currently admitted.
    pub(crate) fn in_flight(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }
}

impl Drop for RelayStreamPermit {
    fn drop(&mut self) {
        // Remove the source's entry when its last permit goes, so the map holds
        // only sources with drivers in flight.
        self.admission
            .per_source
            .remove_if_mut(&self.source, |_, count| {
                *count = count.saturating_sub(1);
                *count == 0
            });
        self.admission.total.fetch_sub(1, Ordering::AcqRel);
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

    #[test]
    fn the_node_wide_cap_binds_across_sources() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(10, 2));
        let _a = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        let _b = admission.try_admit(addr("192.0.2.2:1")).unwrap();
        assert_eq!(
            admission.try_admit(addr("192.0.2.3:1")).err(),
            Some(Refusal::NodeAtCap)
        );
        assert_eq!(admission.in_flight(), 2);
    }

    #[test]
    fn dropping_a_permit_returns_both_slots() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(1, 1));
        let permit = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        assert!(admission.try_admit(addr("192.0.2.1:2")).is_err());
        drop(permit);
        assert_eq!(admission.in_flight(), 0);
        assert!(
            admission.per_source.is_empty(),
            "an idle source keeps no entry"
        );
        assert!(admission.try_admit(addr("192.0.2.1:2")).is_ok());
    }

    /// A refusal at the source cap must not leak the node-wide slot it reserved.
    #[test]
    fn a_source_refusal_does_not_consume_node_capacity() {
        let admission = Arc::new(RelayStreamAdmission::with_caps(1, 2));
        let _a = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        for port in 2..50 {
            assert!(
                admission
                    .try_admit(addr(&format!("192.0.2.1:{port}")))
                    .is_err()
            );
        }
        assert_eq!(admission.in_flight(), 1);
        assert!(admission.try_admit(addr("192.0.2.9:1")).is_ok());
    }
}
