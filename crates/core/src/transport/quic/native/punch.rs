//! Punching for a native connection (#5940, design 2.4).
//!
//! Both sides of a native connection send small punch datagrams to the
//! other's address before and during the handshake, so each NAT has a
//! mapping for the other side when its first QUIC packet arrives. A punch is
//! [`PUNCH_LEN`] bytes, [`PUNCH_FIRST_BYTE`] then 8 random ones; the
//! receiver's demux drops it as `Other` and counts it.
//!
//! Punches go every [`PUNCH_INTERVAL`], at most [`PUNCHES_PER_DIAL`] per dial.
//! What native setups send, punches and a client's Initials together, is
//! bounded by a [`PunchBudget`], charged when a setup starts at what that
//! setup can send at most, so a setup that is refused sends nothing:
//! - [`PUNCH_BUDGET_BYTES`] a minute per destination: one IPv4 address, or
//!   one IPv6 /64 (an IPv4-mapped address counts as IPv4);
//! - [`NODE_PUNCH_BUDGET_BYTES`] a minute for the node, all destinations
//!   together.
//!
//! The budget remembers [`PUNCH_BUDGET_ENTRIES`] destinations. A new one
//! replaces the least recently charged only once that one's budget has
//! refilled, so forgetting a destination never gives it back bytes early;
//! otherwise the new one is refused.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
use crate::transport::normalize_mapped_addr;

/// Time between two punches of one dial.
pub(crate) const PUNCH_INTERVAL: Duration = Duration::from_millis(250);

/// Punches one dial sends at most: 8 s at [`PUNCH_INTERVAL`].
pub(crate) const PUNCHES_PER_DIAL: u32 = 32;

/// Bytes one destination may be sent per [`PUNCH_BUDGET_WINDOW`] by native
/// setups, punches and Initials together.
pub(crate) const PUNCH_BUDGET_BYTES: u64 = 64 * 1024;

/// Bytes the node may send per [`PUNCH_BUDGET_WINDOW`] by native setups to
/// all destinations together. Provisional, until A5 measures setup rates.
pub(crate) const NODE_PUNCH_BUDGET_BYTES: u64 = 1024 * 1024;

/// The window both budgets refill over.
pub(crate) const PUNCH_BUDGET_WINDOW: Duration = Duration::from_secs(60);

/// Destinations the budget remembers.
pub(crate) const PUNCH_BUDGET_ENTRIES: usize = 4096;

/// Padded datagrams a native client sends at most in its window. From a
/// 100 ms initial RTT the probe timeout is about 300 ms and doubles, and
/// quinn (0.11.19) sends two probes per timeout, so an unanswered client
/// sends 1 + 2 + 2 + 2 + 2 = 9 Initials in 8 s, then one padded close when
/// the dial is dropped. Two more cover a Retry's restart. Checked against
/// what a dial to a silent peer really sends (`setup::tests`).
pub(crate) const INITIALS_PER_DIAL: u64 = 12;

/// The most one dial's punches can send.
pub(crate) const PUNCH_BYTES_PER_DIAL: u64 = PUNCHES_PER_DIAL as u64 * PUNCH_LEN as u64;

/// The most a native client's setup can send toward its server: its punches
/// and its Initials (each padded to the 1200-byte minimum).
pub(crate) const CLIENT_BYTES_PER_DIAL: u64 = PUNCH_BYTES_PER_DIAL
    + INITIALS_PER_DIAL * crate::transport::quic::config::MAX_UDP_PAYLOAD as u64;

/// Why a charge was refused. Nothing is taken on a refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PunchRefusal {
    /// The destination's budget is spent.
    Destination,
    /// The node's budget is spent.
    Node,
    /// The destination is new, and the least recently charged one has not
    /// refilled yet, so it cannot be forgotten.
    Table,
}

/// What one budget is kept for: an IPv4 address or an IPv6 /64.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Destination {
    V4([u8; 4]),
    V6([u8; 8]),
}

impl Destination {
    fn of(to: SocketAddr) -> Self {
        match normalize_mapped_addr(to).ip() {
            IpAddr::V4(ip) => Destination::V4(ip.octets()),
            IpAddr::V6(ip) => {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&ip.octets()[..8]);
                Destination::V6(prefix)
            }
        }
    }
}

/// A token bucket of `capacity` bytes refilling over [`PUNCH_BUDGET_WINDOW`].
#[derive(Clone, Copy)]
struct Bucket {
    /// Bytes left, as of `at`.
    left: u64,
    at: Duration,
}

impl Bucket {
    fn full(capacity: u64, now: Duration) -> Self {
        Self {
            left: capacity,
            at: now,
        }
    }

    /// Credit what has refilled since `at`. The bucket's time moves on by
    /// what the credited bytes took, so the part of a byte not yet earned is
    /// kept for the next charge; a full bucket earns nothing more, so its
    /// time is now.
    fn refill(&mut self, capacity: u64, now: Duration) {
        let elapsed = now.saturating_sub(self.at).as_nanos();
        let window = PUNCH_BUDGET_WINDOW.as_nanos();
        let refill = capacity as u128 * elapsed / window;
        if refill == 0 {
            if self.left >= capacity {
                self.at = now;
            }
            return;
        }
        self.left = (self.left as u128 + refill).min(capacity as u128) as u64;
        self.at = if self.left == capacity {
            now
        } else {
            let earned = refill * window / capacity as u128;
            self.at + Duration::from_nanos(earned as u64)
        };
    }
}

/// The node's punch budgets, on the caller's clock: one per destination and
/// one for the node.
pub(crate) struct PunchBudget {
    state: Mutex<BudgetState>,
}

struct BudgetState {
    destinations: lru::LruCache<Destination, Bucket>,
    node: Option<Bucket>,
}

impl Default for PunchBudget {
    fn default() -> Self {
        Self::with_entries(PUNCH_BUDGET_ENTRIES)
    }
}

impl PunchBudget {
    fn with_entries(entries: usize) -> Self {
        Self {
            state: Mutex::new(BudgetState {
                destinations: lru::LruCache::new(
                    NonZeroUsize::new(entries).unwrap_or(NonZeroUsize::MIN),
                ),
                node: None,
            }),
        }
    }

    /// Take `bytes` for a setup toward `to` at `now`, from `to`'s
    /// destination budget and the node's, or neither.
    pub(crate) fn charge(
        &self,
        to: SocketAddr,
        bytes: u64,
        now: Duration,
    ) -> Result<(), PunchRefusal> {
        let mut state = self.state.lock();
        let BudgetState { destinations, node } = &mut *state;
        let node = node.get_or_insert_with(|| Bucket::full(NODE_PUNCH_BUDGET_BYTES, now));
        node.refill(NODE_PUNCH_BUDGET_BYTES, now);
        if node.left < bytes {
            return Err(PunchRefusal::Node);
        }
        let key = Destination::of(to);
        if !destinations.contains(&key) && destinations.len() == destinations.cap().get() {
            let refilled = destinations.peek_lru().is_some_and(|(_, oldest)| {
                let mut oldest = *oldest;
                oldest.refill(PUNCH_BUDGET_BYTES, now);
                oldest.left >= PUNCH_BUDGET_BYTES
            });
            if !refilled {
                return Err(PunchRefusal::Table);
            }
            destinations.pop_lru();
        }
        let bucket = destinations.get_or_insert_mut(key, || Bucket::full(PUNCH_BUDGET_BYTES, now));
        bucket.refill(PUNCH_BUDGET_BYTES, now);
        if bucket.left < bytes {
            return Err(PunchRefusal::Destination);
        }
        bucket.left -= bytes;
        node.left -= bytes;
        Ok(())
    }
}

/// One punch datagram: the reserved first byte and 8 random bytes.
pub(crate) fn punch_datagram() -> [u8; PUNCH_LEN] {
    let mut datagram = [0u8; PUNCH_LEN];
    datagram[0] = PUNCH_FIRST_BYTE;
    GlobalRng::fill_bytes(&mut datagram[1..]);
    datagram
}

/// Send punches to `to` through `send` every [`PUNCH_INTERVAL`], the first at
/// once, until [`PUNCHES_PER_DIAL`] have gone or `until` (on `clock`) has
/// passed. Never returns early otherwise: the caller races it with the
/// handshake and drops it when that ends. A failed send counts as sent, as a
/// lost datagram would.
pub(crate) async fn punch<T: TimeSource>(
    send: impl Fn(SocketAddr, &[u8; PUNCH_LEN]),
    to: SocketAddr,
    clock: &T,
    until: Duration,
) {
    for sent in 0..PUNCHES_PER_DIAL {
        if clock.now() >= until {
            return;
        }
        send(to, &punch_datagram());
        if sent + 1 < PUNCHES_PER_DIAL {
            clock.sleep_cancelable(PUNCH_INTERVAL).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::simulation::VirtualTime;
    use crate::transport::quic::tests::{bounded, drive};

    fn to(ip: &str) -> SocketAddr {
        SocketAddr::new(ip.parse().unwrap(), 31337)
    }

    /// The constants these figures rest on, as literals: a change to any is
    /// a change to the bound the PR description and design state.
    #[test]
    fn the_budget_has_these_values() {
        assert_eq!(PUNCH_INTERVAL, Duration::from_millis(250));
        assert_eq!(PUNCHES_PER_DIAL, 32);
        assert_eq!(PUNCH_BYTES_PER_DIAL, 288);
        assert_eq!(INITIALS_PER_DIAL, 12);
        assert_eq!(CLIENT_BYTES_PER_DIAL, 288 + 12 * 1200);
        assert_eq!(PUNCH_BUDGET_BYTES, 65_536);
        assert_eq!(NODE_PUNCH_BUDGET_BYTES, 1_048_576);
        assert_eq!(PUNCH_BUDGET_WINDOW, Duration::from_secs(60));
        assert_eq!(PUNCH_BUDGET_ENTRIES, 4096);
    }

    /// A destination takes PUNCH_BUDGET_BYTES a minute: four client setups
    /// at once, the fifth refused, another destination unaffected, half of it
    /// back after half a minute and all of it after a minute.
    #[test]
    fn a_destination_takes_its_budget_per_minute() {
        let budget = PunchBudget::default();
        let dials = PUNCH_BUDGET_BYTES / CLIENT_BYTES_PER_DIAL;
        assert_eq!(dials, 4);
        let t0 = Duration::from_secs(100);
        for _ in 0..dials {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
                Ok(())
            );
        }
        assert_eq!(
            budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
            Err(PunchRefusal::Destination)
        );
        assert_eq!(
            budget.charge(to("198.51.100.2"), CLIENT_BYTES_PER_DIAL, t0),
            Ok(())
        );
        let half = t0 + PUNCH_BUDGET_WINDOW / 2;
        let left = PUNCH_BUDGET_BYTES - dials * CLIENT_BYTES_PER_DIAL;
        let available = left + PUNCH_BUDGET_BYTES / 2;
        for _ in 0..available / CLIENT_BYTES_PER_DIAL {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, half),
                Ok(())
            );
        }
        assert_eq!(
            budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, half),
            Err(PunchRefusal::Destination)
        );
        let later = half + PUNCH_BUDGET_WINDOW;
        for _ in 0..dials {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, later),
                Ok(())
            );
        }
    }

    /// A refused charge takes nothing, and every port and the IPv4-mapped
    /// form of one IPv4 address share its budget.
    #[test]
    fn one_ipv4_address_is_one_destination() {
        let budget = PunchBudget::default();
        let now = Duration::ZERO;
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES - 10, now),
            Ok(())
        );
        assert_eq!(
            budget.charge(to("198.51.100.1"), 11, now),
            Err(PunchRefusal::Destination)
        );
        let other_port: SocketAddr = "198.51.100.1:9".parse().unwrap();
        let mapped: SocketAddr = "[::ffff:198.51.100.1]:31337".parse().unwrap();
        assert_eq!(budget.charge(other_port, 5, now), Ok(()));
        assert_eq!(budget.charge(mapped, 5, now), Ok(()));
        assert_eq!(
            budget.charge(to("198.51.100.1"), 1, now),
            Err(PunchRefusal::Destination)
        );
    }

    /// Every address in one IPv6 /64 shares one budget; another /64 has its
    /// own.
    #[test]
    fn one_ipv6_slash_64_is_one_destination() {
        let budget = PunchBudget::default();
        let now = Duration::ZERO;
        assert_eq!(
            budget.charge(to("2001:db8:1:2::1"), PUNCH_BUDGET_BYTES, now),
            Ok(())
        );
        for other in ["2001:db8:1:2::2", "2001:db8:1:2:ffff:ffff:ffff:ffff"] {
            assert_eq!(
                budget.charge(to(other), 1, now),
                Err(PunchRefusal::Destination),
                "{other}"
            );
        }
        assert_eq!(budget.charge(to("2001:db8:1:3::1"), 1, now), Ok(()));
    }

    /// The node's budget bounds all destinations together: 71 client setups
    /// to 71 destinations, and the 72nd refused for the node, whatever its
    /// destination; it refills over the window.
    #[test]
    fn the_node_budget_bounds_all_destinations_together() {
        let budget = PunchBudget::default();
        let now = Duration::ZERO;
        let dials = NODE_PUNCH_BUDGET_BYTES / CLIENT_BYTES_PER_DIAL;
        assert_eq!(dials, 71);
        for i in 0..dials {
            let dest = SocketAddr::from(([198, 51, (i / 250) as u8, (i % 250) as u8 + 1], 1));
            assert_eq!(
                budget.charge(dest, CLIENT_BYTES_PER_DIAL, now),
                Ok(()),
                "{i}"
            );
        }
        assert_eq!(
            budget.charge(to("203.0.113.9"), CLIENT_BYTES_PER_DIAL, now),
            Err(PunchRefusal::Node)
        );
        let later = now + PUNCH_BUDGET_WINDOW;
        assert_eq!(
            budget.charge(to("203.0.113.9"), CLIENT_BYTES_PER_DIAL, later),
            Ok(())
        );
    }

    /// Small charges at a fine interval lose nothing to rounding: from empty,
    /// taking every byte available each millisecond takes exactly the budget
    /// in one window.
    #[test]
    fn small_charges_lose_nothing_to_rounding() {
        let budget = PunchBudget::default();
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, Duration::ZERO),
            Ok(())
        );
        let mut taken = 0;
        for ms in 1..=PUNCH_BUDGET_WINDOW.as_millis() as u64 {
            while budget
                .charge(to("198.51.100.1"), 1, Duration::from_millis(ms))
                .is_ok()
            {
                taken += 1;
            }
        }
        assert_eq!(taken, PUNCH_BUDGET_BYTES);
    }

    /// With the table full, a new destination is refused while the least
    /// recently charged one has not refilled, so forgetting it would give
    /// bytes back early; once that one has refilled, it is forgotten and the
    /// new one is taken. A refusal for the table takes nothing.
    #[test]
    fn a_destination_is_forgotten_only_once_it_has_refilled() {
        let budget = PunchBudget::with_entries(2);
        let t0 = Duration::ZERO;
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, t0),
            Ok(())
        );
        assert_eq!(budget.charge(to("198.51.100.2"), 1, t0), Ok(()));
        assert_eq!(
            budget.charge(to("198.51.100.3"), 1, t0),
            Err(PunchRefusal::Table)
        );
        // .1 refills over the window; .3 then replaces it.
        let later = t0 + PUNCH_BUDGET_WINDOW;
        assert_eq!(budget.charge(to("198.51.100.3"), 1, later), Ok(()));
        // .1 was forgotten with its budget full, so starting it full again
        // gives nothing back early (.2, refilled too, makes room for it).
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, later),
            Ok(())
        );
        assert_eq!(
            budget.charge(to("198.51.100.1"), 1, later),
            Err(PunchRefusal::Destination)
        );
    }

    /// A punch is the reserved byte and 8 bytes from GlobalRng.
    #[test]
    fn a_punch_is_the_reserved_byte_and_eight_random_ones() {
        let _seed = GlobalRng::seed_guard(7);
        let a = punch_datagram();
        let b = punch_datagram();
        assert_eq!(a.len(), PUNCH_LEN);
        assert_eq!(a[0], PUNCH_FIRST_BYTE);
        assert_ne!(a[1..], b[1..]);
        assert_eq!(
            crate::transport::demux::classify(&a),
            crate::transport::metrics::DemuxClass::Other
        );
    }

    /// Punches go at once and then every PUNCH_INTERVAL, at most
    /// PUNCHES_PER_DIAL of them, and none at or after `until`.
    #[test]
    fn punches_follow_the_interval_and_stop() {
        bounded(|| async {
            for (until, expected) in [
                (Duration::from_secs(60), PUNCHES_PER_DIAL as usize),
                (Duration::from_secs(1), 4),
                (Duration::ZERO, 0),
            ] {
                let time = VirtualTime::new();
                let sent = Arc::new(Mutex::new(Vec::new()));
                let (clock, record) = (time.clone(), sent.clone());
                let target = to("198.51.100.1");
                drive(&time, Duration::from_secs(120), async move {
                    let at = clock.clone();
                    punch(
                        move |dest, datagram| {
                            assert_eq!(dest, target);
                            assert_eq!(datagram[0], PUNCH_FIRST_BYTE);
                            record.lock().unwrap().push(at.now());
                        },
                        target,
                        &clock,
                        until,
                    )
                    .await
                })
                .await;
                let sent = sent.lock().unwrap().clone();
                assert_eq!(sent.len(), expected, "until {until:?}");
                for (i, at) in sent.iter().enumerate() {
                    assert_eq!(*at, PUNCH_INTERVAL * i as u32, "punch {i}");
                }
            }
        });
    }
}
