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
//! replaces the least recently charged (a refused charge counts as a use)
//! only once that one's budget has refilled, so forgetting a destination
//! never gives it back bytes early; otherwise the new one is refused.
//!
//! Destinations this node has completed a native setup with (Hello exchanged)
//! within [`PROVEN_TTL`] are proven. A setup to a proven destination is
//! charged to a separate allowance first, [`ALLOWANCE_BYTES`] a minute per
//! destination and [`NODE_ALLOWANCE_BYTES`] for the node, so pressure on the
//! shared budget does not stop a reconnect to a known-good peer. A spent
//! allowance falls back to the shared budget, so a proven destination is
//! never worse off than an unproven one. The proven set holds
//! [`PROVEN_ENTRIES`] destinations; a new one replaces the least recently
//! used only once that one's allowance has refilled, so forgetting a
//! destination never gives it allowance early, and otherwise the new one is
//! not recorded.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
use crate::transport::{TRANSPORT_METRICS, normalize_mapped_addr};

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

/// How long a completed native setup keeps its destination proven.
pub(crate) const PROVEN_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Destinations the proven set remembers.
pub(crate) const PROVEN_ENTRIES: usize = 1024;

/// Bytes one proven destination's allowance holds per [`PUNCH_BUDGET_WINDOW`]:
/// two client setups of two dials each (a dial and its overlapping second).
pub(crate) const ALLOWANCE_BYTES: u64 = 4 * CLIENT_BYTES_PER_DIAL;

/// Bytes the node's allowance holds per [`PUNCH_BUDGET_WINDOW`], all proven
/// destinations together: about 17 client setups a minute. Sized
/// conservatively from 0.2.164 field counters (see the PR): refusals were
/// near zero there, so this only has to cover reconnect bursts.
pub(crate) const NODE_ALLOWANCE_BYTES: u64 = 512 * 1024;

/// Padded datagrams a native client sends at most in its window. From a
/// 100 ms initial RTT the probe timeout is about 300 ms and doubles, and
/// quinn (0.11.19) sends two probes per timeout, so an unanswered client
/// sends 1 + 2 + 2 + 2 + 2 = 9 Initials in 8 s, then one padded close when
/// the dial is dropped: 10. A Retry does not reset quinn's probe count, so a
/// client answered with a Retry and then nothing sends one Initial more:
/// 10 and the close, 11. 12 leaves one datagram of margin. Both are checked
/// against what a dial really sends (`setup::tests`).
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
    proven: lru::LruCache<Destination, Proven>,
    node_allowance: Option<Bucket>,
}

/// A proven destination: when it was last proven, and its allowance.
struct Proven {
    at: Duration,
    allowance: Bucket,
}

impl Default for PunchBudget {
    fn default() -> Self {
        Self::with_entries(PUNCH_BUDGET_ENTRIES)
    }
}

impl PunchBudget {
    fn with_entries(entries: usize) -> Self {
        Self::with_capacities(entries, PROVEN_ENTRIES)
    }

    fn with_capacities(entries: usize, proven: usize) -> Self {
        let cap = |n: usize| NonZeroUsize::new(n).unwrap_or(NonZeroUsize::MIN);
        Self {
            state: Mutex::new(BudgetState {
                destinations: lru::LruCache::new(cap(entries)),
                node: None,
                proven: lru::LruCache::new(cap(proven)),
                node_allowance: None,
            }),
        }
    }

    /// Record that a native setup with `to` completed at `now`: it is proven
    /// until [`PROVEN_TTL`] has passed, and proving it again renews that
    /// without refilling its allowance. If the set is full and the least
    /// recently used destination's allowance has not refilled, `to` is not
    /// recorded.
    pub(crate) fn mark_proven(&self, to: SocketAddr, now: Duration) {
        let key = Destination::of(to);
        let mut state = self.state.lock();
        if let Some(entry) = state.proven.get_mut(&key) {
            entry.at = now;
            return;
        }
        if state.proven.len() == state.proven.cap().get() {
            let refilled = state.proven.peek_lru().is_some_and(|(_, oldest)| {
                let mut allowance = oldest.allowance;
                allowance.refill(ALLOWANCE_BYTES, now);
                allowance.left >= ALLOWANCE_BYTES
            });
            if !refilled {
                return;
            }
            state.proven.pop_lru();
        }
        state.proven.put(
            key,
            Proven {
                at: now,
                allowance: Bucket::full(ALLOWANCE_BYTES, now),
            },
        );
    }

    /// Whether `to` is proven at `now`.
    #[cfg(test)]
    fn is_proven(&self, to: SocketAddr, now: Duration) -> bool {
        let state = self.state.lock();
        state
            .proven
            .peek(&Destination::of(to))
            .is_some_and(|p| now.saturating_sub(p.at) < PROVEN_TTL)
    }

    /// Take `bytes` from `key`'s allowance and the node's, if `key` is
    /// proven at `now`. Returns whether it did; takes nothing otherwise.
    fn charge_allowance(
        state: &mut BudgetState,
        key: Destination,
        bytes: u64,
        now: Duration,
    ) -> Option<bool> {
        let entry = state.proven.peek(&key)?;
        if now.saturating_sub(entry.at) >= PROVEN_TTL {
            return None;
        }
        let node = state
            .node_allowance
            .get_or_insert_with(|| Bucket::full(NODE_ALLOWANCE_BYTES, now));
        node.refill(NODE_ALLOWANCE_BYTES, now);
        let entry = state.proven.get_mut(&key)?;
        entry.allowance.refill(ALLOWANCE_BYTES, now);
        if bytes > entry.allowance.left || bytes > node.left {
            return Some(false);
        }
        entry.allowance.left -= bytes;
        node.left -= bytes;
        Some(true)
    }

    /// Take `bytes` for a setup toward `to` at `now`, from `to`'s
    /// destination budget and the node's, or neither.
    pub(crate) fn charge(
        &self,
        to: SocketAddr,
        bytes: u64,
        now: Duration,
    ) -> Result<(), PunchRefusal> {
        // More than a destination ever holds: refused without touching the
        // table, so it neither forgets an entry nor adds one.
        if bytes > PUNCH_BUDGET_BYTES {
            return Err(PunchRefusal::Destination);
        }
        let mut state = self.state.lock();
        let key = Destination::of(to);
        // A proven destination draws on its allowance, not the shared budget.
        let proven_but_spent = match Self::charge_allowance(&mut state, key, bytes, now) {
            Some(true) => {
                TRANSPORT_METRICS.record_connect_native_punch_allowance_used();
                return Ok(());
            }
            Some(false) => {
                TRANSPORT_METRICS.record_connect_native_punch_allowance_exhausted();
                true
            }
            None => false,
        };
        let result = Self::charge_shared(&mut state, key, bytes, now);
        if proven_but_spent && result.is_err() {
            TRANSPORT_METRICS.record_connect_native_punch_allowance_refused();
        }
        result
    }

    fn charge_shared(
        state: &mut BudgetState,
        key: Destination,
        bytes: u64,
        now: Duration,
    ) -> Result<(), PunchRefusal> {
        let BudgetState {
            destinations, node, ..
        } = state;
        let node = node.get_or_insert_with(|| Bucket::full(NODE_PUNCH_BUDGET_BYTES, now));
        node.refill(NODE_PUNCH_BUDGET_BYTES, now);
        if node.left < bytes {
            return Err(PunchRefusal::Node);
        }
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
/// once. A fixed cadence on purpose, with no jitter: it is a bounded pacer
/// (at most [`PUNCHES_PER_DIAL`], cut off at `until`), not a retry after a
/// failure, and both sides punching on the same schedule is what opens the
/// two NATs within one interval of each other.
///
/// It stops once [`PUNCHES_PER_DIAL`] have gone or `until` (on `clock`) has
/// passed, and never returns early otherwise: the caller races it with the
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
        assert_eq!(PROVEN_TTL, Duration::from_secs(21_600));
        assert_eq!(PROVEN_ENTRIES, 1024);
        assert_eq!(ALLOWANCE_BYTES, 4 * CLIENT_BYTES_PER_DIAL);
        assert_eq!(NODE_ALLOWANCE_BYTES, 524_288);
    }

    /// A completed native setup records its destination as proven; without
    /// this call the allowance is never available.
    #[test]
    fn a_completed_native_setup_proves_its_destination() {
        let driver = include_str!("../upgrade/driver/native.rs");
        let setup = driver
            .split("async fn run_native_setup")
            .nth(1)
            .and_then(|rest| rest.split("fn native_connection").next())
            .expect("run_native_setup");
        let proven = setup
            .find("punch_budget.mark_proven(dial.addr")
            .expect("run_native_setup proves the destination");
        let failed = setup
            .find("A QUIC-native setup gave no connection")
            .unwrap();
        assert!(proven > failed, "proven only after the setup succeeded");
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

    /// Spend a destination's whole shared budget, as pressure on it would.
    fn exhaust_shared(budget: &PunchBudget, ip: &str, now: Duration) {
        for _ in 0..PUNCH_BUDGET_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(budget.charge(to(ip), CLIENT_BYTES_PER_DIAL, now), Ok(()));
        }
        assert_eq!(
            budget.charge(to(ip), CLIENT_BYTES_PER_DIAL, now),
            Err(PunchRefusal::Destination)
        );
    }

    /// A destination whose shared budget is spent is refused unless it is
    /// proven, and a proven one is served from the allowance, up to the
    /// allowance and no further. Fails if the allowance is removed.
    #[test]
    fn a_proven_destination_is_served_when_the_shared_budget_is_spent() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, "198.51.100.1", t0);
        budget.mark_proven(to("198.51.100.1"), t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
                Ok(())
            );
        }
        // The allowance is bounded: past it the shared budget answers.
        assert_eq!(
            budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
            Err(PunchRefusal::Destination)
        );
        // The allowance refills over the window like the budget.
        assert_eq!(
            budget.charge(
                to("198.51.100.1"),
                CLIENT_BYTES_PER_DIAL,
                t0 + PUNCH_BUDGET_WINDOW
            ),
            Ok(())
        );
    }

    /// Allowance charges leave the shared budgets alone, so proven reconnects
    /// do not use up what unproven destinations share.
    #[test]
    fn the_allowance_does_not_draw_on_the_shared_budget() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        budget.mark_proven(to("198.51.100.1"), t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
                Ok(())
            );
        }
        // The destination's own shared budget is still whole.
        let dials = PUNCH_BUDGET_BYTES / CLIENT_BYTES_PER_DIAL;
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
    }

    /// An unproven destination gets no allowance.
    #[test]
    fn an_unproven_destination_gets_no_allowance() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, "198.51.100.1", t0);
        assert!(!budget.is_proven(to("198.51.100.1"), t0));
        assert_eq!(
            budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
            Err(PunchRefusal::Destination)
        );
    }

    /// A destination stays proven for PROVEN_TTL after its last proof, and
    /// renewing the proof does not refill its allowance.
    #[test]
    fn proof_expires_and_renewal_does_not_refill() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, "198.51.100.1", t0);
        budget.mark_proven(to("198.51.100.1"), t0);
        let dials = ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL;
        for _ in 0..dials {
            assert_eq!(
                budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
                Ok(())
            );
        }
        budget.mark_proven(to("198.51.100.1"), t0);
        assert_eq!(
            budget.charge(to("198.51.100.1"), CLIENT_BYTES_PER_DIAL, t0),
            Err(PunchRefusal::Destination)
        );
        // Just before expiry the proof holds; at expiry it does not.
        let last = t0 + PROVEN_TTL;
        budget.mark_proven(to("198.51.100.1"), last - Duration::from_secs(1));
        assert!(budget.is_proven(to("198.51.100.1"), last));
        assert!(!budget.is_proven(
            to("198.51.100.1"),
            last + PROVEN_TTL - Duration::from_secs(1)
        ));
        // Expired: refused again once the shared budget is spent.
        let late = last + PROVEN_TTL;
        exhaust_shared(&budget, "198.51.100.1", late);
    }

    /// All proven destinations together are held to the node allowance: what
    /// a node serves is at most its shared node budget plus its allowance,
    /// and the allowance does add to it.
    #[test]
    fn the_node_allowance_bounds_all_proven_destinations() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        let mut served = 0u64;
        for host in 1..=200u8 {
            let to = to(&format!("198.51.100.{host}"));
            budget.mark_proven(to, t0);
            while budget.charge(to, CLIENT_BYTES_PER_DIAL, t0).is_ok() {
                served += CLIENT_BYTES_PER_DIAL;
            }
        }
        assert!(served > NODE_PUNCH_BUDGET_BYTES);
        assert!(served <= NODE_PUNCH_BUDGET_BYTES + NODE_ALLOWANCE_BYTES);
        let probe = to("198.51.100.250");
        budget.mark_proven(probe, t0);
        assert_eq!(
            budget.charge(probe, CLIENT_BYTES_PER_DIAL, t0),
            Err(PunchRefusal::Node)
        );
    }

    /// A full proven set does not forget a destination whose allowance has
    /// not refilled: the newcomer is not recorded, and is recorded once the
    /// allowance has refilled.
    #[test]
    fn forgetting_a_proven_destination_never_refills_its_allowance() {
        let budget = PunchBudget::with_capacities(PUNCH_BUDGET_ENTRIES, 2);
        let t0 = Duration::from_secs(100);
        for ip in ["198.51.100.1", "198.51.100.2"] {
            budget.mark_proven(to(ip), t0);
            assert_eq!(budget.charge(to(ip), CLIENT_BYTES_PER_DIAL, t0), Ok(()));
        }
        budget.mark_proven(to("198.51.100.3"), t0);
        assert!(!budget.is_proven(to("198.51.100.3"), t0));
        assert!(budget.is_proven(to("198.51.100.1"), t0));
        // A minute on, every allowance is whole and the newcomer is taken.
        let later = t0 + PUNCH_BUDGET_WINDOW;
        budget.mark_proven(to("198.51.100.3"), later);
        assert!(budget.is_proven(to("198.51.100.3"), later));
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

    /// A charge larger than any destination's budget is refused for its
    /// destination and leaves the table as it was: the oldest entry is not
    /// forgotten and the new destination is not added.
    #[test]
    fn a_charge_larger_than_a_destination_budget_leaves_the_table_alone() {
        let budget = PunchBudget::with_entries(1);
        let t0 = Duration::ZERO;
        assert_eq!(budget.charge(to("198.51.100.1"), 1, t0), Ok(()));
        let later = t0 + PUNCH_BUDGET_WINDOW;
        assert_eq!(
            budget.charge(to("198.51.100.2"), PUNCH_BUDGET_BYTES + 1, later),
            Err(PunchRefusal::Destination)
        );
        // .1 is still remembered: it is refused at once for its own spent
        // byte if charged its whole budget at t0 again.
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, t0),
            Err(PunchRefusal::Destination)
        );
    }

    /// A charge refused for its destination takes nothing from the node
    /// budget: after it, the node budget still holds exactly what the
    /// accepted charges left.
    #[test]
    fn a_refused_destination_charge_leaves_the_node_budget_alone() {
        let budget = PunchBudget::default();
        let now = Duration::ZERO;
        assert_eq!(
            budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, now),
            Ok(())
        );
        for _ in 0..100 {
            assert_eq!(
                budget.charge(to("198.51.100.1"), PUNCH_BUDGET_BYTES, now),
                Err(PunchRefusal::Destination)
            );
        }
        // The node has NODE - one destination left: fill it exactly from
        // fifteen other destinations, then one byte more is the node's refusal.
        let left = NODE_PUNCH_BUDGET_BYTES - PUNCH_BUDGET_BYTES;
        assert_eq!(left / PUNCH_BUDGET_BYTES, 15);
        for i in 0..15u8 {
            let dest = SocketAddr::from(([203, 0, 113, i + 1], 1));
            assert_eq!(budget.charge(dest, PUNCH_BUDGET_BYTES, now), Ok(()), "{i}");
        }
        assert_eq!(
            budget.charge(to("192.0.2.1"), 1, now),
            Err(PunchRefusal::Node)
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
