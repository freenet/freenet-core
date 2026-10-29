//! A connection's peer identity and the key-to-address index.
//!
//! The fields of [`IdentitySlot`] and [`KeyIndex`] are private to this
//! module, so only the functions here can bind a key to a connection, point
//! the index at a connection, or remove an index entry. Code outside can
//! still replace a whole slot with a new, unbound [`IdentitySlot`]; the
//! bridge does that only when it constructs a connection entry. The bridge
//! changes identities through [`bind`] and [`release`], wrapped by
//! `P2pConnManager::bind_identity` and `P2pConnManager::release_identity`.
//!
//! Rules kept here:
//! - A connection is bound at most once and keeps its key until it closes.
//! - A claimed key is not bound while another open connection holds it from
//!   a dial that was heard from within [`HOLD_WINDOW`]. The claim can be kept
//!   on its connection as pending; it is bound when the key is released, or
//!   on a later attempt once the dial has gone quiet.
//! - The index maps a key to an open connection bound to it whenever one
//!   exists. A connection that closes moves or removes its key's entry
//!   unless the entry points at another open connection bound to the key.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use crate::transport::TransportPublicKey;

use super::ConnectionEntry;

/// How a connection's peer identity was bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IdentitySource {
    /// The key this node dialled, bound when the outbound connection was
    /// established.
    Dialled,
    /// A key named for the connection without a dial of it: the key an
    /// expected-inbound registration carried for an inbound connection
    /// matched to it by address, the joiner key of the peer's own first
    /// ConnectRequest, or the key a ConnectPeer named for a connection that
    /// had none. Once bound it is held to the same rules as a dialled key
    /// (it never changes), and it is not bound while a dialled connection
    /// elsewhere holds the key.
    Claimed,
}

/// A claim that was not bound because a dialled connection held the key.
/// It is bound later (the key is released, or the dial goes quiet), if its
/// connection is still open and still without an identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingClaim {
    pub(super) key: TransportPublicKey,
    /// Whether the claim, had it been bound, would have promoted the
    /// connection into the ring (an expected-inbound match or a
    /// ConnectPeer). A ConnectRequest claim does not promote.
    pub(super) promote: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
enum State {
    #[default]
    Unbound,
    Pending(PendingClaim),
    Bound {
        key: TransportPublicKey,
        source: IdentitySource,
    },
}

/// The identity of one connection. Starts unbound; only [`bind`] changes it.
#[derive(Debug, Default)]
pub(super) struct IdentitySlot(State);

impl IdentitySlot {
    /// The key bound to the connection, if any.
    pub(super) fn key(&self) -> Option<&TransportPublicKey> {
        match &self.0 {
            State::Bound { key, .. } => Some(key),
            State::Unbound | State::Pending(_) => None,
        }
    }

    /// How the bound key was bound, if the connection has one.
    pub(super) fn source(&self) -> Option<IdentitySource> {
        match &self.0 {
            State::Bound { source, .. } => Some(*source),
            State::Unbound | State::Pending(_) => None,
        }
    }

    /// The claim waiting for its key to be released, if any.
    pub(super) fn pending(&self) -> Option<&PendingClaim> {
        match &self.0 {
            State::Pending(claim) => Some(claim),
            State::Unbound | State::Bound { .. } => None,
        }
    }
}

/// Peer key to the address of an open connection bound to it.
#[derive(Debug, Default)]
pub(super) struct KeyIndex(BTreeMap<TransportPublicKey, SocketAddr>);

impl KeyIndex {
    pub(super) fn get(&self, key: &TransportPublicKey) -> Option<&SocketAddr> {
        self.0.get(key)
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &TransportPublicKey) -> bool {
        self.0.contains_key(key)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The outcome of [`bind`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum BindOutcome {
    /// The connection had no identity and is now bound to the key.
    Bound,
    /// The connection was already bound to the key. Nothing changed.
    Same,
    /// The connection is bound to another key, which it keeps. Nothing
    /// changed.
    Mismatch(TransportPublicKey),
    /// A claimed key that a dialled connection at another address holds and
    /// was heard from within [`HOLD_WINDOW`]. The connection stays without
    /// an identity (with the claim pending, if the caller asked).
    HeldByDial(SocketAddr),
    /// There is no open connection at the address. Nothing changed.
    NoConnection,
}

/// How long a dialled connection holds its key against a claim after it last
/// received an authenticated packet. A live connection is heard from at
/// least every keepalive interval (5 s), so three missed intervals mean the
/// remote has most likely gone, for example restarted at another address;
/// past this the claim is bound rather than waiting for the old session's
/// idle timeout.
pub(super) const HOLD_WINDOW: std::time::Duration = std::time::Duration::from_secs(15);

/// Whether the connection `entry` is bound to `key` by a dial it has heard
/// from within [`HOLD_WINDOW`]. A connection whose transport does not report
/// receipt (a test double) counts as heard from.
fn holds_by_live_dial(entry: &ConnectionEntry, key: &TransportPublicKey) -> bool {
    entry.identity.key() == Some(key)
        && entry.identity.source() == Some(IdentitySource::Dialled)
        && entry
            .receipt
            .as_ref()
            .is_none_or(|stamp| stamp.since_last_receipt() <= HOLD_WINDOW)
}

/// What [`bind`] would do for `key` at `addr` from `source`, without doing
/// it, and another open connection bound to `key`, if any (preferring the
/// one the index points at).
pub(super) fn check(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    index: &KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
    source: IdentitySource,
) -> (BindOutcome, Option<SocketAddr>) {
    let Some(entry) = connections.get(&addr) else {
        return (BindOutcome::NoConnection, None);
    };
    if let Some(bound) = entry.identity.key() {
        let outcome = if bound == key {
            BindOutcome::Same
        } else {
            BindOutcome::Mismatch(bound.clone())
        };
        return (outcome, None);
    }
    if source == IdentitySource::Claimed {
        if let Some((dialled, _)) = connections
            .iter()
            .find(|(other, entry)| **other != addr && holds_by_live_dial(entry, key))
        {
            return (BindOutcome::HeldByDial(*dialled), Some(*dialled));
        }
    }
    let indexed = index.get(key).copied().filter(|holder| {
        *holder != addr
            && connections
                .get(holder)
                .is_some_and(|other| other.identity.key() == Some(key))
    });
    let holder = indexed.or_else(|| other_holder(connections, addr, key, |_| true));
    (BindOutcome::Bound, holder)
}

/// Bind `key` from `source` to the connection at `addr`, as [`check`]
/// decides. On [`BindOutcome::HeldByDial`], `pending` decides whether the
/// claim is kept on the connection to be bound later: `Some(promote)` keeps
/// it (see [`PendingClaim::promote`]; a claim already pending for the same
/// key keeps `promote` if either says so), `None` leaves the connection as
/// it was. Returns the outcome and, on a bind, another open connection bound
/// to the same key.
pub(super) fn bind(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
    source: IdentitySource,
    pending: Option<bool>,
) -> (BindOutcome, Option<SocketAddr>) {
    let (outcome, holder) = check(connections, index, addr, key, source);
    let Some(entry) = connections.get_mut(&addr) else {
        return (outcome, holder);
    };
    match &outcome {
        BindOutcome::Bound => {
            entry.identity.0 = State::Bound {
                key: key.clone(),
                source,
            };
            index.0.insert(key.clone(), addr);
        }
        BindOutcome::HeldByDial(_) => {
            if let Some(promote) = pending {
                let promote = promote
                    || entry
                        .identity
                        .pending()
                        .is_some_and(|claim| claim.key == *key && claim.promote);
                entry.identity.0 = State::Pending(PendingClaim {
                    key: key.clone(),
                    promote,
                });
            }
        }
        BindOutcome::Same | BindOutcome::Mismatch(_) | BindOutcome::NoConnection => {}
    }
    (outcome, holder)
}

/// The outcome of [`release`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Release {
    /// No other open connection is bound to the key: its index entry was
    /// removed, and the key is no longer connected.
    Released,
    /// The index now points at this other open connection bound to the same
    /// key.
    Moved(SocketAddr),
    /// The index points at another open connection bound to the key.
    /// Nothing changed.
    NotOwner,
}

/// The connection at `addr`, bound to `key`, is closing. Updates the index
/// for it. Call with the closing connection either still in `connections`
/// or already removed; it is never chosen as the new holder.
///
/// Relies on the index invariant kept by [`bind`] and this function:
/// whenever an open connection is bound to a key, the index maps the key to
/// one of them. So an entry that points at another open connection bound to
/// the key is left alone ([`Release::NotOwner`]); otherwise (it points at the
/// closing connection, or, against the invariant, at nothing) the key moves
/// to another holder, preferring one `in_ring` reports in the ring, then a
/// dialled one, or is released.
pub(super) fn release(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
    in_ring: impl Fn(SocketAddr) -> bool,
) -> Release {
    if let Some(indexed) = index.get(key).copied() {
        if indexed != addr
            && connections
                .get(&indexed)
                .is_some_and(|other| other.identity.key() == Some(key))
        {
            return Release::NotOwner;
        }
    }
    let dialled =
        |entry: &ConnectionEntry| entry.identity.source() == Some(IdentitySource::Dialled);
    let next = other_holder(connections, addr, key, |(a, e)| in_ring(a) && dialled(e))
        .or_else(|| other_holder(connections, addr, key, |(a, _)| in_ring(a)))
        .or_else(|| other_holder(connections, addr, key, |(_, e)| dialled(e)))
        .or_else(|| other_holder(connections, addr, key, |_| true));
    match next {
        Some(next) => {
            index.0.insert(key.clone(), next);
            Release::Moved(next)
        }
        None => {
            index.0.remove(key);
            Release::Released
        }
    }
}

/// The open connection, other than any at `exclude`, with a pending claim of
/// `key`. The newest one (highest connection id) if there are several.
pub(super) fn pending_claimant(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    exclude: SocketAddr,
    key: &TransportPublicKey,
) -> Option<(SocketAddr, bool)> {
    connections
        .iter()
        .filter(|(addr, _)| **addr != exclude)
        .filter_map(|(addr, entry)| {
            entry
                .identity
                .pending()
                .filter(|claim| claim.key == *key)
                .map(|claim| (entry.connection_id, *addr, claim.promote))
        })
        .max_by_key(|(id, _, _)| *id)
        .map(|(_, addr, promote)| (addr, promote))
}

/// Every pending claim: the connection, the claimed key and whether it
/// would promote.
pub(super) fn pending_claims(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
) -> Vec<(SocketAddr, TransportPublicKey, bool)> {
    connections
        .iter()
        .filter_map(|(addr, entry)| {
            entry
                .identity
                .pending()
                .map(|claim| (*addr, claim.key.clone(), claim.promote))
        })
        .collect()
}

/// An open connection other than `exclude` bound to `key` that `select`
/// accepts.
fn other_holder(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    exclude: SocketAddr,
    key: &TransportPublicKey,
    select: impl Fn((SocketAddr, &ConnectionEntry)) -> bool,
) -> Option<SocketAddr> {
    connections
        .iter()
        .find(|(addr, entry)| {
            **addr != exclude && entry.identity.key() == Some(key) && select((**addr, entry))
        })
        .map(|(addr, _)| *addr)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::transport::{ReceiptStamp, TransportKeypair};

    fn key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([192, 0, 2, 1], port))
    }

    /// A clock for receipt stamps: the time now, and the last receipt.
    #[derive(Clone, Default)]
    struct Clock {
        now: Arc<AtomicU64>,
        last: Arc<AtomicU64>,
    }

    impl Clock {
        fn stamp(&self) -> ReceiptStamp {
            let now = self.now.clone();
            ReceiptStamp::new(
                self.last.clone(),
                Arc::new(move || now.load(Ordering::Relaxed)),
            )
        }

        fn advance(&self, by: std::time::Duration) {
            self.now.fetch_add(by.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    struct Table {
        connections: BTreeMap<SocketAddr, ConnectionEntry>,
        index: KeyIndex,
        next_id: u64,
    }

    impl Table {
        fn new() -> Self {
            Self {
                connections: BTreeMap::new(),
                index: KeyIndex::default(),
                next_id: 0,
            }
        }

        fn open(&mut self, at: SocketAddr, receipt: Option<ReceiptStamp>) {
            let (sender, _receiver) = tokio::sync::mpsc::channel(1);
            self.next_id += 1;
            let now = tokio::time::Instant::now();
            self.connections.insert(
                at,
                ConnectionEntry {
                    sender,
                    identity: IdentitySlot::default(),
                    receipt,
                    connection_id: self.next_id,
                    created_at: now,
                    link_use: super::super::zombie_sweep::LinkUseStamp::new(now),
                    remote_version: None,
                },
            );
        }

        fn bind(
            &mut self,
            at: SocketAddr,
            key: &TransportPublicKey,
            source: IdentitySource,
            pending: Option<bool>,
        ) -> BindOutcome {
            bind(
                &mut self.connections,
                &mut self.index,
                at,
                key,
                source,
                pending,
            )
            .0
        }

        fn close(
            &mut self,
            at: SocketAddr,
            key: &TransportPublicKey,
            in_ring: &[SocketAddr],
        ) -> Release {
            self.connections.remove(&at);
            release(&self.connections, &mut self.index, at, key, |a| {
                in_ring.contains(&a)
            })
        }
    }

    #[test]
    fn bind_outcomes() {
        let mut t = Table::new();
        let (k, other) = (key(), key());
        t.open(addr(1), None);
        assert_eq!(
            t.bind(addr(9), &k, IdentitySource::Dialled, None),
            BindOutcome::NoConnection
        );
        assert_eq!(
            t.bind(addr(1), &k, IdentitySource::Dialled, None),
            BindOutcome::Bound
        );
        assert_eq!(
            t.bind(addr(1), &k, IdentitySource::Claimed, None),
            BindOutcome::Same
        );
        assert_eq!(
            t.bind(addr(1), &other, IdentitySource::Claimed, None),
            BindOutcome::Mismatch(k.clone())
        );
        assert_eq!(t.connections[&addr(1)].identity.key(), Some(&k));
        assert_eq!(t.index.get(&k), Some(&addr(1)));
        assert!(!t.index.contains_key(&other));
    }

    #[test]
    fn a_claim_is_held_by_a_live_dial_and_kept_only_when_asked() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.open(addr(3), None);
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, None),
            BindOutcome::HeldByDial(addr(1))
        );
        assert!(t.connections[&addr(2)].identity.pending().is_none());
        assert_eq!(
            t.bind(addr(3), &k, IdentitySource::Claimed, Some(false)),
            BindOutcome::HeldByDial(addr(1))
        );
        assert_eq!(
            t.connections[&addr(3)].identity.pending(),
            Some(&PendingClaim {
                key: k.clone(),
                promote: false
            })
        );
        // A dial is not held back by another dial.
        t.open(addr(4), None);
        assert_eq!(
            t.bind(addr(4), &k, IdentitySource::Dialled, None),
            BindOutcome::Bound
        );
        assert_eq!(t.index.get(&k), Some(&addr(4)));
    }

    #[test]
    fn a_pending_claim_keeps_promote_if_either_attempt_had_it() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(true));
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(false));
        assert!(t.connections[&addr(2)].identity.pending().unwrap().promote);
    }

    #[test]
    fn a_dial_not_heard_from_within_the_window_does_not_hold_its_key() {
        let mut t = Table::new();
        let k = key();
        let clock = Clock::default();
        t.open(addr(1), Some(clock.stamp()));
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        clock.advance(HOLD_WINDOW);
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, Some(true)),
            BindOutcome::HeldByDial(addr(1)),
            "heard from exactly one window ago still holds"
        );
        clock.advance(std::time::Duration::from_millis(1));
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, Some(true)),
            BindOutcome::Bound
        );
        assert_eq!(t.index.get(&k), Some(&addr(2)));
        // The quiet dial's later close leaves the claim's binding.
        assert_eq!(t.close(addr(1), &k, &[]), Release::NotOwner);
        assert_eq!(t.index.get(&k), Some(&addr(2)));
        // A receipt makes a dial hold again.
        let k2 = key();
        let clock2 = Clock::default();
        t.open(addr(3), Some(clock2.stamp()));
        t.bind(addr(3), &k2, IdentitySource::Dialled, None);
        clock2.advance(HOLD_WINDOW * 2);
        clock2
            .last
            .store(clock2.now.load(Ordering::Relaxed), Ordering::Relaxed);
        t.open(addr(4), None);
        assert_eq!(
            t.bind(addr(4), &k2, IdentitySource::Claimed, None),
            BindOutcome::HeldByDial(addr(3))
        );
    }

    #[test]
    fn release_outcomes() {
        let mut t = Table::new();
        let k = key();
        // Released: the last connection for the key.
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Claimed, None);
        assert_eq!(t.close(addr(1), &k, &[]), Release::Released);
        assert!(t.index.is_empty());

        // NotOwner: the index points at another open holder.
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, None);
        t.open(addr(3), None);
        t.bind(addr(3), &k, IdentitySource::Claimed, None);
        assert_eq!(t.index.get(&k), Some(&addr(3)));
        assert_eq!(t.close(addr(2), &k, &[]), Release::NotOwner);
        assert_eq!(t.index.get(&k), Some(&addr(3)));
        assert_eq!(t.close(addr(3), &k, &[]), Release::Released);
    }

    #[test]
    fn release_moves_the_key_to_an_in_ring_holder_then_a_dialled_one() {
        let mut t = Table::new();
        let k = key();
        // Ports order the map: the first holder found is the claimed one
        // outside the ring, so the preference is what picks the others.
        // The claims bind before the dials, which would hold them back.
        for port in 10..=13 {
            t.open(addr(port), None);
        }
        for (port, source) in [
            (10, IdentitySource::Claimed),
            (12, IdentitySource::Claimed),
            (11, IdentitySource::Dialled),
            (13, IdentitySource::Dialled),
        ] {
            assert_eq!(t.bind(addr(port), &k, source, None), BindOutcome::Bound);
        }
        assert_eq!(t.index.get(&k), Some(&addr(13)));
        assert_eq!(
            t.close(addr(13), &k, &[addr(12)]),
            Release::Moved(addr(12)),
            "in the ring first"
        );
        assert_eq!(
            t.close(addr(12), &k, &[]),
            Release::Moved(addr(11)),
            "then dialled"
        );
        assert_eq!(t.close(addr(11), &k, &[]), Release::Moved(addr(10)));
        assert_eq!(t.close(addr(10), &k, &[]), Release::Released);
    }

    #[test]
    fn release_prefers_a_dial_among_holders_in_the_ring() {
        let mut t = Table::new();
        let k = key();
        for port in 40..=42 {
            t.open(addr(port), None);
        }
        for (port, source) in [
            (40, IdentitySource::Claimed),
            (41, IdentitySource::Dialled),
            (42, IdentitySource::Dialled),
        ] {
            assert_eq!(t.bind(addr(port), &k, source, None), BindOutcome::Bound);
        }
        assert_eq!(
            t.close(addr(42), &k, &[addr(40), addr(41)]),
            Release::Moved(addr(41))
        );
    }

    #[test]
    fn release_without_an_index_entry_moves_or_releases() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(20), None);
        t.bind(addr(20), &k, IdentitySource::Claimed, None);
        t.open(addr(21), None);
        t.bind(addr(21), &k, IdentitySource::Claimed, None);
        t.index.0.remove(&k);
        assert_eq!(t.close(addr(21), &k, &[]), Release::Moved(addr(20)));
        assert_eq!(t.index.get(&k), Some(&addr(20)));
        t.index.0.remove(&k);
        assert_eq!(t.close(addr(20), &k, &[]), Release::Released);
    }

    #[test]
    fn pending_claimant_is_the_newest_and_pending_claims_lists_all() {
        let mut t = Table::new();
        let (k, other) = (key(), key());
        t.open(addr(30), None);
        t.bind(addr(30), &k, IdentitySource::Dialled, None);
        t.open(addr(30 + 2), None);
        t.bind(addr(32), &k, IdentitySource::Claimed, Some(false));
        t.open(addr(31), None);
        t.bind(addr(31), &k, IdentitySource::Claimed, Some(true));
        t.open(addr(33), None);
        t.bind(addr(33), &other, IdentitySource::Claimed, Some(true));
        assert_eq!(
            pending_claimant(&t.connections, addr(30), &k),
            Some((addr(31), true)),
            "the newest connection, not the lowest address"
        );
        assert_eq!(
            pending_claimant(&t.connections, addr(31), &k),
            Some((addr(32), false))
        );
        assert_eq!(pending_claims(&t.connections).len(), 2);
        assert!(pending_claimant(&t.connections, addr(30), &other).is_none());
    }
}
