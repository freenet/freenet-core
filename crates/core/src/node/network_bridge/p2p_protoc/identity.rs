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
//! - One connection per key. A key is not bound to a connection while
//!   another open connection is bound to it, whether the new one was dialled
//!   or claimed and however the holder was bound. The new connection can be
//!   kept waiting, as a pending claim, and keeps the key it waits for: a
//!   later attempt to bind another key to it is a mismatch. A holder that
//!   has not been heard from within [`HOLD_WINDOW`] is closed by the caller
//!   ([`BindOutcome::HeldByQuiet`]); a holder that is heard from is never
//!   closed for a claim. When the holder closes, the newest pending claim of
//!   the key is bound.
//! - The index maps each bound key to the one connection bound to it.
//!
//! Known limit: crossed sessions. A peer that reaches this node at two
//! address pairs, established at nearly the same time, can leave each side
//! bound to the session the other keeps waiting. Both stay heard from, so
//! neither is replaced as quiet. Requests still flow over both, but state
//! keyed by the peer is not attributed to the session the peer uses until one
//! of the two closes. If the pair is idle, both sessions can be lost
//! instead: a waiting connection is never in the ring, so the zombie sweep
//! reaps it on each side, and the peers stay unlinked until they reconnect.
//! Rare; a claim that waits this long is counted ([`LONG_CLAIM_WAIT`],
//! `claim_held_long`, an upper bound on crossed sessions) so field data shows
//! how often. Making them converge is #6040.

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
    /// had none. Also any key bound after waiting for another connection to
    /// release it, whatever started the wait.
    Claimed,
}

/// A binding that was not made because another connection held the key. It
/// is made later (the holder closes, or is replaced once quiet), if its
/// connection is still open and still without an identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PendingClaim {
    pub(super) key: TransportPublicKey,
    /// Whether the binding, once made, promotes the connection into the
    /// ring (a dial, an expected-inbound match, or a ConnectPeer; a
    /// ConnectRequest does not promote).
    pub(super) promote: bool,
    /// When the connection first waited for the key.
    pub(super) since: tokio::time::Instant,
    /// Whether the wait has been counted as longer than [`LONG_CLAIM_WAIT`].
    pub(super) counted_long: bool,
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
    #[cfg(test)]
    pub(super) fn source(&self) -> Option<IdentitySource> {
        match &self.0 {
            State::Bound { source, .. } => Some(*source),
            State::Unbound | State::Pending(_) => None,
        }
    }

    /// The binding waiting for its key to be released, if any.
    pub(super) fn pending(&self) -> Option<&PendingClaim> {
        match &self.0 {
            State::Pending(claim) => Some(claim),
            State::Unbound | State::Bound { .. } => None,
        }
    }
}

/// Peer key to the address of the open connection bound to it, and whether
/// a claim may be pending on some connection (set when [`bind`] keeps one,
/// cleared by [`clear_pending_hint`] once none is left). The flag lets the
/// event loop arm its retry timer only while there is something to retry.
#[derive(Debug, Default)]
pub(super) struct KeyIndex(BTreeMap<TransportPublicKey, SocketAddr>, bool);

impl KeyIndex {
    /// Whether a claim may be pending on some connection. False only when
    /// none is.
    pub(super) fn may_have_pending_claims(&self) -> bool {
        self.1
    }

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
    /// The connection is bound to another key, or waiting for another key,
    /// which it keeps. Nothing changed.
    Mismatch(TransportPublicKey),
    /// The key is bound to a connection at another address that was heard
    /// from within [`HOLD_WINDOW`]. The connection stays without an identity
    /// (kept waiting, if the caller asked).
    Held(SocketAddr),
    /// The key is bound to a connection at another address that has not
    /// been heard from within [`HOLD_WINDOW`]. Not bound either (kept
    /// waiting, if the caller asked): the caller closes the quiet holder, and
    /// its release binds the newest pending claim of the key.
    HeldByQuiet(SocketAddr),
    /// There is no open connection at the address. Nothing changed.
    NoConnection,
}

/// How long a connection holds its key against a newer connection after it
/// last received an authenticated packet. Past it the connection is taken to
/// belong to a peer that has gone, for example restarted at another address,
/// and is replaced by the newer one rather than kept until the old session's
/// idle timeout.
///
/// Twice the transport's longest keepalive interval (15 s, which its
/// keepalive backs off to while pings go unanswered): a live remote answers
/// at least one of two consecutive pings in that time unless both are lost.
/// The transport itself keeps a silent connection for much longer before
/// closing it, since small packets are lost on some NAT paths; replacing a
/// connection here costs it, so the window must not be shorter than a single
/// lost keepalive round trip. The receipt stamp records when the
/// connection's task processed a packet, not when it arrived, so a backlog
/// on that task also counts toward the window.
pub(super) const HOLD_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);

/// The connection, other than `exclude`, bound to `key`, and whether it was
/// heard from within [`HOLD_WINDOW`]. A connection whose transport does not
/// report receipt (a test double) counts as heard from.
pub(super) fn key_holder(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    exclude: Option<SocketAddr>,
    key: &TransportPublicKey,
) -> Option<(SocketAddr, bool)> {
    let live = |entry: &ConnectionEntry| {
        entry
            .receipt
            .as_ref()
            .is_none_or(|stamp| stamp.since_last_receipt() <= HOLD_WINDOW)
    };
    let mut quiet = None;
    for (addr, entry) in connections {
        if Some(*addr) == exclude || entry.identity.key() != Some(key) {
            continue;
        }
        if live(entry) {
            return Some((*addr, true));
        }
        quiet.get_or_insert((*addr, false));
    }
    quiet
}

/// What [`bind`] would do for `key` at `addr`, without doing it.
pub(super) fn check(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    addr: SocketAddr,
    key: &TransportPublicKey,
) -> BindOutcome {
    let Some(entry) = connections.get(&addr) else {
        return BindOutcome::NoConnection;
    };
    if let Some(bound) = entry.identity.key() {
        return if bound == key {
            BindOutcome::Same
        } else {
            BindOutcome::Mismatch(bound.clone())
        };
    }
    // A waiting connection keeps the key it waits for.
    if let Some(claim) = entry.identity.pending() {
        if claim.key != *key {
            return BindOutcome::Mismatch(claim.key.clone());
        }
    }
    match key_holder(connections, Some(addr), key) {
        Some((holder, true)) => BindOutcome::Held(holder),
        Some((holder, false)) => BindOutcome::HeldByQuiet(holder),
        None => BindOutcome::Bound,
    }
}

/// Bind `key` from `source` to the connection at `addr`, as [`check`]
/// decides. On [`BindOutcome::Held`] or [`BindOutcome::HeldByQuiet`],
/// `pending` decides whether the connection is kept waiting for the key:
/// `Some(promote)` keeps it (see [`PendingClaim::promote`]; a claim already
/// pending for the same key keeps `promote` if either says so), `None`
/// leaves the connection as it was.
pub(super) fn bind(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
    source: IdentitySource,
    pending: Option<bool>,
) -> BindOutcome {
    let outcome = check(connections, addr, key);
    let Some(entry) = connections.get_mut(&addr) else {
        return outcome;
    };
    match &outcome {
        BindOutcome::Bound => {
            entry.identity.0 = State::Bound {
                key: key.clone(),
                source,
            };
            index.0.insert(key.clone(), addr);
        }
        BindOutcome::Held(_) | BindOutcome::HeldByQuiet(_) => {
            if let Some(promote) = pending {
                // `check` returned no mismatch, so any earlier claim is of
                // this key, and the claim keeps the time it was first made.
                let earlier = entry.identity.pending();
                let promote = promote || earlier.is_some_and(|claim| claim.promote);
                let since = earlier.map_or_else(tokio::time::Instant::now, |claim| claim.since);
                let counted_long = earlier.is_some_and(|claim| claim.counted_long);
                entry.identity.0 = State::Pending(PendingClaim {
                    key: key.clone(),
                    promote,
                    since,
                    counted_long,
                });
                index.1 = true;
            }
        }
        BindOutcome::Same | BindOutcome::Mismatch(_) | BindOutcome::NoConnection => {}
    }
    outcome
}

/// The outcome of [`release`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Release {
    /// The index entry for the key pointed at the closing connection (or,
    /// against the one-connection-per-key rule, at nothing) and is removed.
    Released,
    /// The index points at another open connection bound to the key, which
    /// the one-connection-per-key rule does not let arise. Nothing changed.
    NotOwner,
}

/// The connection at `addr`, bound to `key`, is closing: remove the index
/// entry for the key unless it points at another open connection bound to
/// it. Call with the closing connection either still in `connections` or
/// already removed.
pub(super) fn release(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
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
    index.0.remove(key);
    Release::Released
}

/// The open connection, other than any at `exclude`, with a pending claim of
/// `key`, and that claim. The newest one (highest connection id) if there
/// are several.
pub(super) fn pending_claimant(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    exclude: Option<SocketAddr>,
    key: &TransportPublicKey,
) -> Option<(SocketAddr, PendingClaim)> {
    connections
        .iter()
        .filter(|(addr, _)| Some(**addr) != exclude)
        .filter_map(|(addr, entry)| {
            entry
                .identity
                .pending()
                .filter(|claim| claim.key == *key)
                .map(|claim| (entry.connection_id, *addr, claim.clone()))
        })
        .max_by_key(|(id, _, _)| *id)
        .map(|(_, addr, claim)| (addr, claim))
}

/// How long a claim can wait on a holder that is still heard from before
/// [`mark_long_waits`] counts it (see the module's known limit).
pub(super) const LONG_CLAIM_WAIT: std::time::Duration =
    std::time::Duration::from_secs(3 * HOLD_WINDOW.as_secs());

/// The connections with a pending claim of `key` made at least
/// [`LONG_CLAIM_WAIT`] before `now` and not yet counted; each is marked
/// counted, so a claim is returned once. The caller calls this only while a
/// holder that is heard from holds `key`.
pub(super) fn mark_long_waits(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    key: &TransportPublicKey,
    now: tokio::time::Instant,
) -> Vec<SocketAddr> {
    let mut marked = Vec::new();
    for (addr, entry) in connections.iter_mut() {
        if let State::Pending(claim) = &mut entry.identity.0 {
            if claim.key == *key
                && !claim.counted_long
                && now.saturating_duration_since(claim.since) >= LONG_CLAIM_WAIT
            {
                claim.counted_long = true;
                marked.push(*addr);
            }
        }
    }
    marked
}

/// Mark the newest pending claim of `key` as promoting: it takes the ring
/// place of a holder in the ring that is being replaced for it.
pub(super) fn promote_newest_claim(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    key: &TransportPublicKey,
) {
    if let Some((claimant, _)) = pending_claimant(connections, None, key) {
        if let Some(entry) = connections.get_mut(&claimant) {
            if let State::Pending(claim) = &mut entry.identity.0 {
                claim.promote = true;
            }
        }
    }
}

/// Every key with a pending claim on some connection, once each.
pub(super) fn pending_keys(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
) -> Vec<TransportPublicKey> {
    let keys: std::collections::BTreeSet<_> = connections
        .values()
        .filter_map(|entry| entry.identity.pending().map(|claim| claim.key.clone()))
        .collect();
    keys.into_iter().collect()
}

/// Clear the index's pending-claim flag if no connection has a pending claim.
pub(super) fn clear_pending_hint(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
) {
    if index.1
        && !connections
            .values()
            .any(|entry| entry.identity.pending().is_some())
    {
        index.1 = false;
    }
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

        fn receive(&self) {
            self.last
                .store(self.now.load(Ordering::Relaxed), Ordering::Relaxed);
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
        }

        fn close(&mut self, at: SocketAddr, key: &TransportPublicKey) -> Release {
            self.connections.remove(&at);
            release(&self.connections, &mut self.index, at, key)
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

    /// One connection per key, whatever the source of either: a claim and a
    /// dial both wait for a holder bound by a claim or a dial, and are kept
    /// waiting only when asked.
    #[test]
    fn a_key_is_bound_to_one_connection_whatever_the_source() {
        for holder_source in [IdentitySource::Dialled, IdentitySource::Claimed] {
            for new_source in [IdentitySource::Dialled, IdentitySource::Claimed] {
                let mut t = Table::new();
                let k = key();
                t.open(addr(1), None);
                t.bind(addr(1), &k, holder_source, None);
                t.open(addr(2), None);
                assert_eq!(
                    t.bind(addr(2), &k, new_source, None),
                    BindOutcome::Held(addr(1)),
                    "{new_source:?} beside {holder_source:?}"
                );
                assert!(t.connections[&addr(2)].identity.pending().is_none());
                assert_eq!(
                    t.bind(addr(2), &k, new_source, Some(false)),
                    BindOutcome::Held(addr(1))
                );
                let claim = t.connections[&addr(2)].identity.pending().unwrap();
                assert_eq!((&claim.key, claim.promote), (&k, false));
                assert_eq!(t.index.get(&k), Some(&addr(1)));
            }
        }
    }

    #[test]
    fn a_pending_claim_keeps_promote_if_either_attempt_had_it() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Dialled, Some(true));
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(false));
        assert!(t.connections[&addr(2)].identity.pending().unwrap().promote);
    }

    /// A waiting connection keeps the key it waits for: binding another key
    /// to it is a mismatch, kept or not, and leaves the claim as it was.
    #[test]
    fn a_waiting_connection_keeps_the_key_it_waits_for() {
        let mut t = Table::new();
        let (k, other) = (key(), key());
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(true));
        for pending in [None, Some(false), Some(true)] {
            for source in [IdentitySource::Dialled, IdentitySource::Claimed] {
                assert_eq!(
                    t.bind(addr(2), &other, source, pending),
                    BindOutcome::Mismatch(k.clone())
                );
            }
        }
        assert_eq!(
            check(&t.connections, addr(2), &other),
            BindOutcome::Mismatch(k.clone())
        );
        let claim = t.connections[&addr(2)].identity.pending().unwrap();
        assert_eq!((&claim.key, claim.promote), (&k, true));
        assert_eq!(t.connections[&addr(2)].identity.key(), None);
        assert!(!t.index.contains_key(&other));
    }

    /// A claim that has waited `LONG_CLAIM_WAIT` is marked once: not before,
    /// not again on a later call, and only claims of the given key.
    #[test]
    fn a_long_wait_is_marked_once_per_claim() {
        let mut t = Table::new();
        let (k, other) = (key(), key());
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(true));
        t.open(addr(3), None);
        t.bind(addr(3), &other, IdentitySource::Dialled, None);
        t.open(addr(4), None);
        t.bind(addr(4), &other, IdentitySource::Claimed, Some(true));
        let since = t.connections[&addr(2)].identity.pending().unwrap().since;
        let due = since + LONG_CLAIM_WAIT;
        let early = due - std::time::Duration::from_millis(1);
        assert!(mark_long_waits(&mut t.connections, &k, early).is_empty());
        assert_eq!(mark_long_waits(&mut t.connections, &k, due), vec![addr(2)]);
        assert!(
            mark_long_waits(&mut t.connections, &k, due).is_empty(),
            "once"
        );
        // A later attempt merged into the claim keeps it counted.
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(false));
        assert!(mark_long_waits(&mut t.connections, &k, due).is_empty());
        assert!(
            !t.connections[&addr(4)]
                .identity
                .pending()
                .unwrap()
                .counted_long
        );
    }

    /// A claim keeps the time it was first made when a later attempt for the
    /// same key is merged into it.
    #[test]
    fn a_merged_claim_keeps_its_first_time() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Dialled, None);
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(false));
        // A time distinct from now, so a merge that took now would differ.
        let first = t.connections[&addr(2)].identity.pending().unwrap().since
            + std::time::Duration::from_secs(10);
        if let State::Pending(claim) = &mut t.connections.get_mut(&addr(2)).unwrap().identity.0 {
            claim.since = first;
        }
        t.bind(addr(2), &k, IdentitySource::Claimed, Some(true));
        assert_eq!(
            t.connections[&addr(2)].identity.pending().unwrap().since,
            first
        );
    }

    #[test]
    fn a_holder_not_heard_from_within_the_window_is_quiet() {
        let mut t = Table::new();
        let k = key();
        let clock = Clock::default();
        t.open(addr(1), Some(clock.stamp()));
        t.bind(addr(1), &k, IdentitySource::Claimed, None);
        t.open(addr(2), None);
        clock.advance(HOLD_WINDOW);
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, Some(true)),
            BindOutcome::Held(addr(1)),
            "heard from exactly one window ago still holds"
        );
        clock.advance(std::time::Duration::from_millis(1));
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, Some(true)),
            BindOutcome::HeldByQuiet(addr(1)),
            "a quiet holder still holds the key until the caller closes it"
        );
        assert_eq!(key_holder(&t.connections, None, &k), Some((addr(1), false)));
        assert_eq!(t.close(addr(1), &k), Release::Released);
        assert_eq!(
            pending_claimant(&t.connections, None, &k).map(|(a, c)| (a, c.promote)),
            Some((addr(2), true))
        );
        assert_eq!(
            t.bind(addr(2), &k, IdentitySource::Claimed, None),
            BindOutcome::Bound
        );
        // A receipt makes a holder hold again.
        clock.receive();
        let k2 = key();
        t.open(addr(3), Some(clock.stamp()));
        t.bind(addr(3), &k2, IdentitySource::Dialled, None);
        clock.advance(HOLD_WINDOW * 2);
        clock.receive();
        t.open(addr(4), None);
        assert_eq!(
            t.bind(addr(4), &k2, IdentitySource::Claimed, None),
            BindOutcome::Held(addr(3))
        );
    }

    #[test]
    fn release_outcomes() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(1), None);
        t.bind(addr(1), &k, IdentitySource::Claimed, None);
        assert_eq!(t.close(addr(1), &k), Release::Released);
        assert!(t.index.is_empty());
        // A missing entry is released too.
        t.open(addr(2), None);
        t.bind(addr(2), &k, IdentitySource::Claimed, None);
        t.index.0.remove(&k);
        assert_eq!(t.close(addr(2), &k), Release::Released);
        // An entry for another open holder is left (against the rule; only
        // reachable by editing the index directly).
        t.open(addr(3), None);
        t.bind(addr(3), &k, IdentitySource::Claimed, None);
        assert_eq!(t.close(addr(4), &k), Release::NotOwner);
        assert_eq!(t.index.get(&k), Some(&addr(3)));
    }

    #[test]
    fn pending_claimant_is_the_newest_and_pending_keys_lists_each_key() {
        let mut t = Table::new();
        let (k, other) = (key(), key());
        t.open(addr(30), None);
        t.bind(addr(30), &k, IdentitySource::Dialled, None);
        t.open(addr(32), None);
        t.bind(addr(32), &k, IdentitySource::Claimed, Some(false));
        t.open(addr(31), None);
        t.bind(addr(31), &k, IdentitySource::Claimed, Some(true));
        t.open(addr(33), None);
        t.bind(addr(33), &other, IdentitySource::Claimed, Some(true));
        let newest = |t: &Table, exclude| {
            pending_claimant(&t.connections, exclude, &k).map(|(a, c)| (a, c.promote))
        };
        assert_eq!(
            newest(&t, Some(addr(30))),
            Some((addr(31), true)),
            "the newest connection, not the lowest address"
        );
        assert_eq!(newest(&t, Some(addr(31))), Some((addr(32), false)));
        assert_eq!(pending_keys(&t.connections), vec![k.clone()]);
        assert!(pending_claimant(&t.connections, None, &other).is_none());
        assert!(t.index.may_have_pending_claims());
        clear_pending_hint(&t.connections, &mut t.index);
        assert!(
            t.index.may_have_pending_claims(),
            "claims are still pending"
        );
    }

    #[test]
    fn the_pending_hint_clears_once_no_claim_is_pending() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(60), None);
        t.bind(addr(60), &k, IdentitySource::Dialled, None);
        t.open(addr(61), None);
        assert!(!t.index.may_have_pending_claims());
        t.bind(addr(61), &k, IdentitySource::Claimed, None);
        assert!(
            !t.index.may_have_pending_claims(),
            "a claim not kept does not set it"
        );
        t.bind(addr(61), &k, IdentitySource::Claimed, Some(false));
        assert!(t.index.may_have_pending_claims());
        t.connections.remove(&addr(61));
        clear_pending_hint(&t.connections, &mut t.index);
        assert!(!t.index.may_have_pending_claims());
    }

    #[test]
    fn promote_newest_claim_marks_the_newest_claim() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(80), None);
        t.bind(addr(80), &k, IdentitySource::Claimed, None);
        t.open(addr(81), None);
        t.bind(addr(81), &k, IdentitySource::Claimed, Some(false));
        promote_newest_claim(&mut t.connections, &k);
        assert!(t.connections[&addr(81)].identity.pending().unwrap().promote);
    }

    /// A live remote can miss one keepalive round trip without its
    /// connection being taken for gone: the window spans at least two of the
    /// transport's longest keepalive intervals.
    #[test]
    fn the_hold_window_spans_two_keepalive_intervals() {
        assert!(HOLD_WINDOW >= 2 * crate::transport::peer_connection::MAX_KEEPALIVE_INTERVAL);
    }

    /// The window is exactly 30 s: long enough for a lost keepalive round
    /// trip (pinned above), and well inside the transport's 120 s idle
    /// timeout, so a connection whose remote has gone is replaced long before
    /// the transport would close it.
    #[test]
    fn the_hold_window_is_thirty_seconds() {
        assert_eq!(HOLD_WINDOW, std::time::Duration::from_secs(30));
    }

    /// A holder whose transport does not report receipt counts as heard
    /// from, so it holds its key however long ago it was opened.
    #[test]
    fn a_holder_without_a_receipt_stamp_counts_as_heard_from() {
        let mut t = Table::new();
        let k = key();
        t.open(addr(70), None);
        t.bind(addr(70), &k, IdentitySource::Dialled, None);
        t.open(addr(71), None);
        assert_eq!(key_holder(&t.connections, None, &k), Some((addr(70), true)));
        assert_eq!(
            t.bind(addr(71), &k, IdentitySource::Claimed, Some(true)),
            BindOutcome::Held(addr(70))
        );
    }
}
