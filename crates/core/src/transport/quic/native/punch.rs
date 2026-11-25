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
//! A peer this node has completed a native setup with (Hello exchanged),
//! identified by its address (an IPv4 address or an IPv6 /64, whatever the
//! port) and its key, is proven for [`PROVEN_TTL`]. A setup to that same address and key is charged to a
//! separate allowance first, [`ALLOWANCE_BYTES`] a minute per proven peer
//! and [`NODE_ALLOWANCE_BYTES`] for the node, so pressure on the shared
//! budget does not stop a reconnect to a known-good peer. All proven peers at
//! one address together draw at most [`IP_ALLOWANCE_BYTES`] a minute. A
//! setup to another key at that address gets the shared budget only. A spent
//! allowance falls back to the shared budget, so a proven peer is never
//! worse off than an unproven one. The proven set holds [`PROVEN_ENTRIES`]
//! peers; an entry that has expired, or else the least recently used, is
//! replaced only once its allowance has refilled, so forgetting a peer never
//! gives it allowance early, and otherwise the new peer is not recorded.
//!
//! Only native completions prove a peer: a connection established over UDP
//! does not. Until the UDP transport is retired the UDP fallback covers
//! those, and afterwards every connection is native.

use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::time::Duration;

use parking_lot::Mutex;

use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
use crate::transport::{TRANSPORT_METRICS, TransportPublicKey, normalize_mapped_addr};

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

/// How long a completed native setup keeps its peer proven. Provisional,
/// until the field gives a reconnect-interval distribution.
pub(crate) const PROVEN_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Peers the proven set remembers. Provisional.
pub(crate) const PROVEN_ENTRIES: usize = 1024;

/// Bytes one proven peer's allowance holds per [`PUNCH_BUDGET_WINDOW`]: one
/// client setup of two dials (a dial and its overlapping second). A rate
/// limit on bytes sent, not a memory budget. Provisional, until the refusal
/// counters split by proven and unproven peers have field data.
pub(crate) const ALLOWANCE_BYTES: u64 = 2 * CLIENT_BYTES_PER_DIAL;

/// Bytes all proven peers at one address (an IPv4 address or an IPv6 /64)
/// may draw together per [`PUNCH_BUDGET_WINDOW`]: four client dials, the
/// shared per-destination figure. A rate limit on bytes sent, not a memory
/// budget. Provisional.
pub(crate) const IP_ALLOWANCE_BYTES: u64 = 4 * CLIENT_BYTES_PER_DIAL;

/// Peers admitted as gateway joiners that the proven set remembers, apart
/// from the peers this node dialed. Provisional.
pub(crate) const JOIN_PROVEN_ENTRIES: usize = 256;

/// Bytes the node's allowance holds per [`PUNCH_BUDGET_WINDOW`], all proven
/// peers together: 71 client dials a minute, so 36 proven peers each using
/// their whole allowance are needed to spend it. A rate limit on bytes sent,
/// not a memory budget. Provisional.
pub(crate) const NODE_ALLOWANCE_BYTES: u64 = 1024 * 1024;

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
    proven: lru::LruCache<ProvenPeer, Proven>,
    /// Peers a gateway admitted as joiners: a partition of their own, so the
    /// volume of joins cannot forget a peer this node dialed.
    joined: lru::LruCache<ProvenPeer, Proven>,
    /// What all proven peers at one address (an IPv4 address or an IPv6
    /// /64) may draw together.
    addresses: lru::LruCache<Destination, Bucket>,
    node_allowance: Option<Bucket>,
}

/// A proven peer: its address (an IPv4 address or an IPv6 /64, so a peer
/// whose NAT rebinds its port keeps its proof) and its key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProvenPeer {
    at: Destination,
    key: TransportPublicKey,
}

impl ProvenPeer {
    fn of(addr: SocketAddr, key: &TransportPublicKey) -> Self {
        Self {
            at: Destination::of(addr),
            key: key.clone(),
        }
    }
}

/// When a peer was last proven, and its allowance.
struct Proven {
    at: Duration,
    allowance: Bucket,
}

impl Proven {
    fn expired(&self, now: Duration) -> bool {
        now.saturating_sub(self.at) >= PROVEN_TTL
    }

    fn refilled(&self, now: Duration) -> bool {
        let mut allowance = self.allowance;
        allowance.refill(ALLOWANCE_BYTES, now);
        allowance.left >= ALLOWANCE_BYTES
    }
}

/// What one setup's first dial adds to the snapshot counters, decided with
/// the charge so the counts come from the decision.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counts {
    used: bool,
    exhausted_peer: bool,
    exhausted_node: bool,
    refused_proven: bool,
    refused_unproven: bool,
}

impl Counts {
    fn record(self) {
        let Counts {
            used,
            exhausted_peer,
            exhausted_node,
            refused_proven,
            refused_unproven,
        } = self;
        if used {
            TRANSPORT_METRICS.record_connect_native_punch_allowance_used();
        }
        if exhausted_peer {
            TRANSPORT_METRICS.record_connect_native_punch_allowance_exhausted_peer();
        }
        if exhausted_node {
            TRANSPORT_METRICS.record_connect_native_punch_allowance_exhausted_node();
        }
        if refused_proven {
            TRANSPORT_METRICS.record_connect_native_punch_refused(true);
        }
        if refused_unproven {
            TRANSPORT_METRICS.record_connect_native_punch_refused(false);
        }
    }
}

/// What the allowance did with one charge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllowanceOutcome {
    /// The peer is not proven (or its proof expired).
    Unproven,
    /// The charge was taken from the allowance.
    Used,
    /// The peer's own allowance, or that of all proven peers at its address,
    /// is spent.
    PeerSpent,
    /// The node's allowance is spent.
    NodeSpent,
}

impl Default for PunchBudget {
    fn default() -> Self {
        Self::with_entries(PUNCH_BUDGET_ENTRIES)
    }
}

impl PunchBudget {
    fn with_entries(entries: usize) -> Self {
        Self::with_capacities(entries, PROVEN_ENTRIES, JOIN_PROVEN_ENTRIES)
    }

    fn with_capacities(entries: usize, proven: usize, joined: usize) -> Self {
        let cap = |n: usize| NonZeroUsize::new(n).unwrap_or(NonZeroUsize::MIN);
        Self {
            state: Mutex::new(BudgetState {
                destinations: lru::LruCache::new(cap(entries)),
                node: None,
                proven: lru::LruCache::new(cap(proven)),
                joined: lru::LruCache::new(cap(joined)),
                addresses: lru::LruCache::new(cap(proven + joined)),
                node_allowance: None,
            }),
        }
    }

    /// Record that a native setup with the peer at `to` holding `key`
    /// completed at `now`: that peer is proven until [`PROVEN_TTL`] has
    /// passed, and proving it again renews that without refilling its
    /// allowance. If the set is full and no entry can be forgotten without
    /// giving it allowance early, the peer is not recorded.
    pub(crate) fn mark_proven(&self, to: SocketAddr, key: &TransportPublicKey, now: Duration) {
        let peer = ProvenPeer::of(to, key);
        let mut state = self.state.lock();
        // A joined peer that now completes a setup this node dialed moves
        // to the dialed set with its allowance as it stands, so the move
        // never refills it. If there is no room, it stays joined.
        let moved = state.joined.pop(&peer);
        let entry = moved.map(|mut entry| {
            entry.at = now;
            entry
        });
        let kept = entry.as_ref().map(|entry| Proven {
            at: entry.at,
            allowance: entry.allowance,
        });
        if !Self::insert_proof(&mut state.proven, peer.clone(), now, entry) {
            if let Some(entry) = kept {
                state.joined.put(peer, entry);
            }
        }
    }

    /// [`Self::mark_proven`] for a peer a gateway admitted as a joiner, kept
    /// apart from the peers this node dialed. A peer already proven by a
    /// setup this node dialed is renewed there instead.
    pub(crate) fn mark_proven_joined(
        &self,
        to: SocketAddr,
        key: &TransportPublicKey,
        now: Duration,
    ) {
        let peer = ProvenPeer::of(to, key);
        let mut state = self.state.lock();
        if let Some(entry) = state.proven.get_mut(&peer) {
            entry.at = now;
            return;
        }
        Self::insert_proof(&mut state.joined, peer, now, None);
    }

    /// Add `peer` to `set` with `entry` (a whole allowance if `None`);
    /// whether it is there afterwards.
    fn insert_proof(
        set: &mut lru::LruCache<ProvenPeer, Proven>,
        peer: ProvenPeer,
        now: Duration,
        entry: Option<Proven>,
    ) -> bool {
        if let Some(existing) = set.get_mut(&peer) {
            existing.at = now;
            return true;
        }
        if set.len() == set.cap().get() {
            // An expired proof goes before a live one.
            let expired = set
                .iter()
                .find(|(_, entry)| entry.expired(now) && entry.refilled(now))
                .map(|(peer, _)| peer.clone());
            match expired {
                Some(old) => {
                    set.pop(&old);
                }
                None => {
                    if !set
                        .peek_lru()
                        .is_some_and(|(_, oldest)| oldest.refilled(now))
                    {
                        return false;
                    }
                    set.pop_lru();
                }
            }
        }
        set.put(
            peer,
            entry.unwrap_or(Proven {
                at: now,
                allowance: Bucket::full(ALLOWANCE_BYTES, now),
            }),
        );
        true
    }

    /// Whether the peer at `to` holding `key` is proven at `now`.
    #[cfg(test)]
    fn is_proven(&self, to: SocketAddr, key: &TransportPublicKey, now: Duration) -> bool {
        let state = self.state.lock();
        let peer = ProvenPeer::of(to, key);
        [&state.proven, &state.joined]
            .iter()
            .any(|set| set.peek(&peer).is_some_and(|p| !p.expired(now)))
    }

    /// Take `bytes` from `peer`'s allowance, its address's and the node's,
    /// if `peer` is proven at `now` and all three hold them; takes nothing
    /// otherwise.
    fn charge_allowance(
        state: &mut BudgetState,
        peer: &ProvenPeer,
        bytes: u64,
        now: Duration,
    ) -> AllowanceOutcome {
        let live = |set: &lru::LruCache<ProvenPeer, Proven>| {
            set.peek(peer).is_some_and(|p| !p.expired(now))
        };
        let BudgetState {
            proven,
            joined,
            addresses,
            node_allowance,
            ..
        } = state;
        let set = if live(proven) {
            proven
        } else if live(joined) {
            joined
        } else {
            return AllowanceOutcome::Unproven;
        };
        let node = node_allowance.get_or_insert_with(|| Bucket::full(NODE_ALLOWANCE_BYTES, now));
        node.refill(NODE_ALLOWANCE_BYTES, now);
        // An address's bucket is whole once refilled, so one forgotten after
        // that is recreated whole; one still spent is never forgotten.
        if !addresses.contains(&peer.at) && addresses.len() == addresses.cap().get() {
            let refilled = addresses.peek_lru().is_some_and(|(_, oldest)| {
                let mut oldest = *oldest;
                oldest.refill(IP_ALLOWANCE_BYTES, now);
                oldest.left >= IP_ALLOWANCE_BYTES
            });
            if !refilled {
                return AllowanceOutcome::PeerSpent;
            }
            addresses.pop_lru();
        }
        let address =
            addresses.get_or_insert_mut(peer.at, || Bucket::full(IP_ALLOWANCE_BYTES, now));
        address.refill(IP_ALLOWANCE_BYTES, now);
        let Some(entry) = set.get_mut(peer) else {
            return AllowanceOutcome::Unproven;
        };
        entry.allowance.refill(ALLOWANCE_BYTES, now);
        if bytes > entry.allowance.left || bytes > address.left {
            return AllowanceOutcome::PeerSpent;
        }
        if bytes > node.left {
            return AllowanceOutcome::NodeSpent;
        }
        entry.allowance.left -= bytes;
        address.left -= bytes;
        node.left -= bytes;
        AllowanceOutcome::Used
    }

    /// Take `bytes` for a setup toward `to` at `now` from the shared budgets
    /// (`to`'s destination budget and the node's), or neither. Nothing is
    /// counted.
    pub(crate) fn charge(
        &self,
        to: SocketAddr,
        bytes: u64,
        now: Duration,
    ) -> Result<(), PunchRefusal> {
        self.charge_inner(to, None, bytes, now, false).0
    }

    /// Take `bytes` for a dial toward the peer at `to` holding `key`: from
    /// the allowance if that exact pair is proven and the allowance has
    /// room, else from the shared budgets. `first` is the dial that starts
    /// the setup, as opposed to its overlapping second dial: only it is
    /// counted, so the counters read in setups.
    pub(crate) fn charge_dial(
        &self,
        to: SocketAddr,
        key: &TransportPublicKey,
        bytes: u64,
        now: Duration,
        first: bool,
    ) -> Result<(), PunchRefusal> {
        let (result, counts) = self.charge_inner(to, Some(key), bytes, now, first);
        counts.record();
        result
    }

    /// The charge, and what to count for it (nothing unless `count`).
    fn charge_inner(
        &self,
        to: SocketAddr,
        key: Option<&TransportPublicKey>,
        bytes: u64,
        now: Duration,
        count: bool,
    ) -> (Result<(), PunchRefusal>, Counts) {
        // More than a destination ever holds: refused without touching the
        // table, so it neither forgets an entry nor adds one.
        if bytes > PUNCH_BUDGET_BYTES {
            return (Err(PunchRefusal::Destination), Counts::default());
        }
        let mut state = self.state.lock();
        let outcome = match key {
            Some(key) => Self::charge_allowance(&mut state, &ProvenPeer::of(to, key), bytes, now),
            None => AllowanceOutcome::Unproven,
        };
        let mut counts = Counts::default();
        if outcome == AllowanceOutcome::Used {
            counts.used = count;
            return (Ok(()), counts);
        }
        let result = Self::charge_shared(&mut state, Destination::of(to), bytes, now);
        if count && key.is_some() {
            counts.exhausted_peer = outcome == AllowanceOutcome::PeerSpent;
            counts.exhausted_node = outcome == AllowanceOutcome::NodeSpent;
            if result.is_err() {
                if outcome == AllowanceOutcome::Unproven {
                    counts.refused_unproven = true;
                } else {
                    counts.refused_proven = true;
                }
            }
        }
        (result, counts)
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
        assert_eq!(ALLOWANCE_BYTES, 2 * CLIENT_BYTES_PER_DIAL);
        assert_eq!(NODE_ALLOWANCE_BYTES, 1_048_576);
        assert_eq!(IP_ALLOWANCE_BYTES, 4 * CLIENT_BYTES_PER_DIAL);
        assert_eq!(JOIN_PROVEN_ENTRIES, 256);
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

    fn key() -> TransportPublicKey {
        crate::transport::TransportKeypair::new().public().clone()
    }

    const PEER: &str = "198.51.100.1";

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

    fn dial(
        budget: &PunchBudget,
        ip: &str,
        key: &TransportPublicKey,
        now: Duration,
    ) -> (Result<(), PunchRefusal>, Counts) {
        budget.charge_inner(to(ip), Some(key), CLIENT_BYTES_PER_DIAL, now, true)
    }

    fn used() -> Counts {
        Counts {
            used: true,
            ..Counts::default()
        }
    }

    /// A proven peer is served from its allowance when the shared budget is
    /// spent, up to the allowance and no further, and each setup is counted
    /// where it is decided. Fails if the allowance is removed.
    #[test]
    fn a_proven_peer_is_served_when_the_shared_budget_is_spent() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, PEER, t0);
        budget.mark_proven(to(PEER), &k, t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(dial(&budget, PEER, &k, t0), (Ok(()), used()));
        }
        // The allowance is bounded: past it the shared budget answers, and
        // the setup is counted as exhausted and then refused as proven.
        assert_eq!(
            dial(&budget, PEER, &k, t0),
            (
                Err(PunchRefusal::Destination),
                Counts {
                    exhausted_peer: true,
                    refused_proven: true,
                    ..Counts::default()
                }
            )
        );
        // The allowance refills over the window like the budget.
        assert_eq!(
            dial(&budget, PEER, &k, t0 + PUNCH_BUDGET_WINDOW),
            (Ok(()), used())
        );
    }

    /// The proof is the address and the key, not the port: a peer whose NAT
    /// rebinds keeps its allowance, and another key at the same address
    /// gets the shared budget only.
    #[test]
    fn the_key_decides_and_the_port_does_not() {
        let budget = PunchBudget::default();
        let (k, other) = (key(), key());
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, PEER, t0);
        budget.mark_proven(to(PEER), &k, t0);
        let unproven_refused = (
            Err(PunchRefusal::Destination),
            Counts {
                refused_unproven: true,
                ..Counts::default()
            },
        );
        assert_eq!(dial(&budget, PEER, &other, t0), unproven_refused);
        // Another port, with the same key, is the same peer.
        let rebound = SocketAddr::new(PEER.parse().unwrap(), 31338);
        assert_eq!(
            budget.charge_inner(rebound, Some(&k), CLIENT_BYTES_PER_DIAL, t0, true),
            (Ok(()), used())
        );
        // So is the IPv4-mapped form of the address.
        let mapped = SocketAddr::new("::ffff:198.51.100.1".parse().unwrap(), 31337);
        assert_eq!(
            budget.charge_inner(mapped, Some(&k), CLIENT_BYTES_PER_DIAL, t0, true),
            (Ok(()), used())
        );
    }

    /// All proven peers at one address draw at most IP_ALLOWANCE_BYTES
    /// together, however many keys there are: a shared address cannot take
    /// the node's allowance.
    #[test]
    fn one_address_gets_a_bounded_allowance() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        let keys: Vec<_> = (0..10).map(|_| key()).collect();
        for k in &keys {
            budget.mark_proven(to(PEER), k, t0);
        }
        exhaust_shared(&budget, PEER, t0);
        let mut served = 0;
        for k in &keys {
            for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
                let (result, counts) = dial(&budget, PEER, k, t0);
                if result.is_ok() {
                    served += CLIENT_BYTES_PER_DIAL;
                } else {
                    assert!(counts.exhausted_peer && counts.refused_proven);
                }
            }
        }
        assert_eq!(served, IP_ALLOWANCE_BYTES);
        // After a window it refills like the others.
        assert_eq!(
            dial(&budget, PEER, &keys[9], t0 + PUNCH_BUDGET_WINDOW),
            (Ok(()), used())
        );
    }

    /// Joins have a partition of their own: however many peers join, none
    /// of the peers this node dialed is forgotten, and the joined ones stay
    /// within their own bound.
    #[test]
    fn joins_cannot_forget_a_dialed_peer() {
        let budget = PunchBudget::with_capacities(PUNCH_BUDGET_ENTRIES, 2, 3);
        let dialed = [key(), key()];
        let t0 = Duration::from_secs(100);
        for (i, k) in dialed.iter().enumerate() {
            budget.mark_proven(to(&format!("198.51.100.{}", i + 1)), k, t0);
        }
        let mut now = t0;
        let joiners: Vec<_> = (0..20).map(|_| key()).collect();
        for (i, k) in joiners.iter().enumerate() {
            // A minute apart, so every allowance has refilled and the
            // partition can forget freely.
            now += PUNCH_BUDGET_WINDOW;
            budget.mark_proven_joined(to(&format!("203.0.113.{}", i + 1)), k, now);
        }
        for (i, k) in dialed.iter().enumerate() {
            assert!(budget.is_proven(to(&format!("198.51.100.{}", i + 1)), k, now));
        }
        let kept = joiners
            .iter()
            .enumerate()
            .filter(|(i, k)| budget.is_proven(to(&format!("203.0.113.{}", i + 1)), k, now))
            .count();
        assert_eq!(kept, 3);
        // A joined peer's setup draws on its allowance.
        let last = joiners.len() - 1;
        let ip = format!("203.0.113.{}", last + 1);
        exhaust_shared(&budget, &ip, now);
        assert_eq!(dial(&budget, &ip, &joiners[last], now), (Ok(()), used()));
    }

    /// A joined peer that then completes a setup this node dialed keeps the
    /// allowance it had: moving it does not refill it.
    #[test]
    fn dialing_a_joined_peer_does_not_refill_its_allowance() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        budget.mark_proven_joined(to(PEER), &k, t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(dial(&budget, PEER, &k, t0), (Ok(()), used()));
        }
        budget.mark_proven(to(PEER), &k, t0);
        {
            let state = budget.state.lock();
            assert!(state.joined.peek(&ProvenPeer::of(to(PEER), &k)).is_none());
            assert!(state.proven.peek(&ProvenPeer::of(to(PEER), &k)).is_some());
        }
        exhaust_shared(&budget, PEER, t0);
        assert!(dial(&budget, PEER, &k, t0).1.exhausted_peer);
    }

    /// Allowance charges leave the shared budgets alone.
    #[test]
    fn the_allowance_does_not_draw_on_the_shared_budget() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        budget.mark_proven(to(PEER), &k, t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(dial(&budget, PEER, &k, t0), (Ok(()), used()));
        }
        exhaust_shared(&budget, PEER, t0);
    }

    /// An overlapping second dial is charged like the first but not counted,
    /// so the counters read in setups; the shared budget's refusal of an
    /// unproven setup is counted as such.
    #[test]
    fn only_a_setups_first_dial_is_counted() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        budget.mark_proven(to(PEER), &k, t0);
        assert_eq!(dial(&budget, PEER, &k, t0), (Ok(()), used()));
        assert_eq!(
            budget.charge_inner(to(PEER), Some(&k), CLIENT_BYTES_PER_DIAL, t0, false),
            (Ok(()), Counts::default())
        );
        // Spent: the uncounted dial is refused and counts nothing.
        exhaust_shared(&budget, PEER, t0);
        assert_eq!(
            budget.charge_inner(to(PEER), Some(&k), CLIENT_BYTES_PER_DIAL, t0, false),
            (Err(PunchRefusal::Destination), Counts::default())
        );
    }

    /// An unproven peer gets no allowance.
    #[test]
    fn an_unproven_peer_gets_no_allowance() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        exhaust_shared(&budget, PEER, t0);
        assert!(!budget.is_proven(to(PEER), &k, t0));
        assert_eq!(
            dial(&budget, PEER, &k, t0).1,
            Counts {
                refused_unproven: true,
                ..Counts::default()
            }
        );
    }

    /// The proof lasts PROVEN_TTL from the last time it was made, checked at
    /// the charge: one second short of it the allowance serves, at it the
    /// shared budget alone answers. Renewing does not refill the allowance.
    #[test]
    fn proof_expires_at_the_ttl_and_renewal_does_not_refill() {
        let budget = PunchBudget::default();
        let k = key();
        let t0 = Duration::from_secs(100);
        budget.mark_proven(to(PEER), &k, t0);
        for _ in 0..ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL {
            assert_eq!(dial(&budget, PEER, &k, t0), (Ok(()), used()));
        }
        budget.mark_proven(to(PEER), &k, t0);
        exhaust_shared(&budget, "198.51.100.9", t0);
        let spent = dial(&budget, PEER, &k, t0);
        assert!(spent.1.exhausted_peer, "renewal did not refill");
        // A fresh window, the shared budget spent: inside the TTL the
        // allowance serves, at the TTL it does not.
        let inside = t0 + PROVEN_TTL - Duration::from_secs(1);
        exhaust_shared(&budget, PEER, inside);
        assert_eq!(dial(&budget, PEER, &k, inside), (Ok(()), used()));
        let at = t0 + PROVEN_TTL;
        // The shared budget is still spent (one second of refill is short
        // of a dial), so the refusal is the expired proof's.
        assert_eq!(
            dial(&budget, PEER, &k, at).1,
            Counts {
                refused_unproven: true,
                ..Counts::default()
            }
        );
    }

    /// The node allowance holds out for enough proven peers: it takes 36 of
    /// them each spending their whole allowance to empty it, and what the
    /// node serves stays within the shared node budget plus the allowance.
    #[test]
    fn the_node_allowance_bounds_all_proven_peers() {
        let budget = PunchBudget::default();
        let t0 = Duration::from_secs(100);
        let per_peer = ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL;
        assert_eq!(NODE_ALLOWANCE_BYTES / CLIENT_BYTES_PER_DIAL, 71);
        let keys: Vec<_> = (0..40).map(|_| key()).collect();
        let mut from_allowance = 0u64;
        let mut peers_to_drain = 0;
        for (i, k) in keys.iter().enumerate() {
            let ip = format!("198.51.100.{}", i + 1);
            budget.mark_proven(to(&ip), k, t0);
            for _ in 0..per_peer {
                let (result, counts) = dial(&budget, &ip, k, t0);
                if counts.used {
                    from_allowance += CLIENT_BYTES_PER_DIAL;
                } else if from_allowance + CLIENT_BYTES_PER_DIAL > NODE_ALLOWANCE_BYTES {
                    // The node allowance is spent: the dial is reported as
                    // such, and is served or refused by the shared budget.
                    assert!(counts.exhausted_node);
                    assert_eq!(counts.refused_proven, result.is_err());
                }
            }
            if peers_to_drain == 0 && from_allowance + CLIENT_BYTES_PER_DIAL > NODE_ALLOWANCE_BYTES
            {
                peers_to_drain = i + 1;
            }
        }
        assert!(from_allowance <= NODE_ALLOWANCE_BYTES);
        assert!(from_allowance + CLIENT_BYTES_PER_DIAL > NODE_ALLOWANCE_BYTES);
        // 71 dials of allowance, two per peer: the 36th peer finds it spent.
        assert_eq!(peers_to_drain, 36);
        let probe = key();
        budget.mark_proven(to("198.51.100.250"), &probe, t0);
        let (_, counts) = dial(&budget, "198.51.100.250", &probe, t0);
        assert!(counts.exhausted_node);
    }

    /// A full proven set forgets an expired proof before a live one, and
    /// forgets nothing whose allowance has not refilled: the newcomer is not
    /// recorded then, and is once the allowance has refilled.
    #[test]
    fn forgetting_a_proven_peer_never_refills_its_allowance() {
        let budget = PunchBudget::with_capacities(PUNCH_BUDGET_ENTRIES, 2, 2);
        let (k1, k2, k3) = (key(), key(), key());
        let t0 = Duration::from_secs(100);
        for (ip, k) in [("198.51.100.1", &k1), ("198.51.100.2", &k2)] {
            budget.mark_proven(to(ip), k, t0);
            assert_eq!(dial(&budget, ip, k, t0), (Ok(()), used()));
        }
        budget.mark_proven(to("198.51.100.3"), &k3, t0);
        assert!(!budget.is_proven(to("198.51.100.3"), &k3, t0));
        assert!(budget.is_proven(to("198.51.100.1"), &k1, t0));
        assert!(budget.is_proven(to("198.51.100.2"), &k2, t0));
        // A minute on every allowance is whole. The least recently used,
        // .1 (.2 was charged after it), is the one forgotten.
        let later = t0 + PUNCH_BUDGET_WINDOW;
        budget.mark_proven(to("198.51.100.3"), &k3, later);
        assert!(budget.is_proven(to("198.51.100.3"), &k3, later));
        assert!(!budget.is_proven(to("198.51.100.1"), &k1, later));
        assert!(budget.is_proven(to("198.51.100.2"), &k2, later));
    }

    /// A full proven set forgets an expired proof before a live one, even
    /// when the live one is the less recently used.
    #[test]
    fn an_expired_proof_is_forgotten_before_a_live_one() {
        let budget = PunchBudget::with_capacities(PUNCH_BUDGET_ENTRIES, 2, 2);
        let (a, b, c) = (key(), key(), key());
        let t0 = Duration::from_secs(100);
        let hour = Duration::from_secs(3600);
        budget.mark_proven(to("198.51.100.1"), &a, t0);
        budget.mark_proven(to("198.51.100.2"), &b, t0 + hour);
        // Charging .1 makes it the more recently used of the two.
        assert_eq!(
            dial(&budget, "198.51.100.1", &a, t0 + 2 * hour),
            (Ok(()), used())
        );
        let end = t0 + PROVEN_TTL;
        budget.mark_proven(to("198.51.100.3"), &c, end);
        assert!(
            !budget.is_proven(to("198.51.100.1"), &a, end),
            "expired one went"
        );
        assert!(budget.is_proven(to("198.51.100.2"), &b, end));
        assert!(budget.is_proven(to("198.51.100.3"), &c, end));
    }

    /// `source` with line comments removed.
    fn code_only(source: &str) -> String {
        source
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The body of `fn name`, up to the next function at the same depth.
    fn body_of<'a>(code: &'a str, name: &str, next: &str) -> &'a str {
        let from = code.find(name).unwrap_or_else(|| panic!("{name}"));
        let rest = &code[from..];
        &rest[..rest.find(next).unwrap_or_else(|| panic!("{next}"))]
    }

    /// Native completions prove a peer, and nothing else does: the dialed
    /// setup after its `?` on success, and a gateway's completed join. A
    /// connection established over UDP is not marked, since the UDP
    /// fallback covers it until the transport is retired.
    #[test]
    fn only_native_completions_prove_a_peer() {
        let native = code_only(include_str!("../upgrade/driver/native.rs"));
        let setup = body_of(&native, "async fn run_native_setup", "fn native_connection");
        let failed = setup.find("outcome_of(&error)").expect("the failure arm");
        let ended = failed + setup[failed..].find("})?;").expect("its `?`");
        let marked = setup
            .find("\n        self.punch_budget\n            .mark_proven(")
            .expect("marked at statement level");
        assert!(marked > ended, "proven only after the setup succeeded");
        assert_eq!(setup.matches(".mark_proven(").count(), 1);

        let driver = code_only(include_str!("../upgrade/driver.rs"));
        let join = body_of(&driver, "fn hand_off_join", "    fn route(");
        assert_eq!(join.matches(".mark_proven_joined(").count(), 1);
        let marked = join.find(".mark_proven_joined(").unwrap();
        let sent = join.find("to_node.try_send").unwrap();
        let failed = join.find("Err(_) =>").unwrap();
        assert!(
            sent < marked && marked < failed,
            "a join proves its peer only when handed over"
        );

        let sources = [
            include_str!("../../quic.rs"),
            include_str!("../upgrade/driver.rs"),
            include_str!("../upgrade/driver/native.rs"),
            include_str!("../upgrade/machine.rs"),
            include_str!("setup.rs"),
            include_str!("exchange.rs"),
            include_str!("../path.rs"),
        ];
        let count = |needle: &str| -> usize {
            sources
                .iter()
                .map(|source| code_only(source).matches(needle).count())
                .sum()
        };
        assert_eq!(
            count(".mark_proven("),
            1,
            "UDP-established connections must not prove a peer"
        );
        assert_eq!(count(".mark_proven_joined("), 1);
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
