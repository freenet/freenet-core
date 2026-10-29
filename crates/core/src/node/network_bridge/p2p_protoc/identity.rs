//! A connection's peer identity and the key-to-address index.
//!
//! The fields of [`IdentitySlot`] and [`KeyIndex`] are private to this
//! module, so the functions here are the only code that can bind a key to a
//! connection, point the index at a connection, or remove an index entry.
//! The rest of the bridge reads through the getters and changes identities
//! only through [`bind`] and [`release`] (wrapped by
//! `P2pConnManager::bind_identity` and `P2pConnManager::release_identity`).
//!
//! Rules kept here:
//! - A connection is bound at most once and keeps its key until it closes.
//! - A claimed key is not bound while another open connection holds it from
//!   a dial. The claim is kept on its connection as pending, and is bound
//!   when the key is released.
//! - The index maps a key to an open connection bound to it whenever one
//!   exists. A connection that closes removes its key's entry only when the
//!   entry points at it, and moves it to another open holder if there is
//!   one.

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
/// It is bound when that key is released, if its connection is still open
/// and still without an identity.
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
    /// A claimed key that a dialled connection at another address holds.
    /// The connection stays without an identity, with the claim pending.
    HeldByDial(SocketAddr),
    /// There is no open connection at the address. Nothing changed.
    NoConnection,
}

/// What [`bind`] would do for `key` at `addr` from `source`, without doing
/// it, and another open connection bound to `key`, if any (preferring the
/// one the index points at).
fn check(
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
        if let Some(dialled) = other_holder(connections, addr, key, Some(IdentitySource::Dialled)) {
            return (BindOutcome::HeldByDial(dialled), Some(dialled));
        }
    }
    let indexed = index.get(key).copied().filter(|holder| {
        *holder != addr
            && connections
                .get(holder)
                .is_some_and(|other| other.identity.key() == Some(key))
    });
    let holder = indexed.or_else(|| other_holder(connections, addr, key, None));
    (BindOutcome::Bound, holder)
}

/// Bind `key` from `source` to the connection at `addr`, as [`check`]
/// decides. On [`BindOutcome::HeldByDial`] the claim is kept on the
/// connection as pending, with `promote` (see [`PendingClaim::promote`]).
/// Returns the outcome and, on a bind, another open connection bound to the
/// same key.
pub(super) fn bind(
    connections: &mut BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
    source: IdentitySource,
    promote: bool,
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
        BindOutcome::Same | BindOutcome::Mismatch(_) | BindOutcome::NoConnection => {}
    }
    (outcome, holder)
}

/// The outcome of [`release`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Release {
    /// The index pointed at the closing connection and no other open
    /// connection is bound to the key: the entry was removed, and the key is
    /// no longer connected.
    Released,
    /// The index pointed at the closing connection and now points at
    /// another open connection bound to the same key.
    Moved(SocketAddr),
    /// The index did not point at the closing connection. Nothing changed.
    NotOwner,
}

/// The connection at `addr`, bound to `key`, is closing. Updates the index
/// for it. Call with the closing connection either still in `connections`
/// or already removed; it is never chosen as the new holder.
pub(super) fn release(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    index: &mut KeyIndex,
    addr: SocketAddr,
    key: &TransportPublicKey,
) -> Release {
    if index.get(key) != Some(&addr) {
        return Release::NotOwner;
    }
    let next = other_holder(connections, addr, key, Some(IdentitySource::Dialled))
        .or_else(|| other_holder(connections, addr, key, None));
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
/// `key`. The newest one if there are several.
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

/// An open connection other than `exclude` bound to `key`, from `source` when
/// given.
fn other_holder(
    connections: &BTreeMap<SocketAddr, ConnectionEntry>,
    exclude: SocketAddr,
    key: &TransportPublicKey,
    source: Option<IdentitySource>,
) -> Option<SocketAddr> {
    connections
        .iter()
        .find(|(addr, entry)| {
            **addr != exclude
                && entry.identity.key() == Some(key)
                && source.is_none_or(|source| entry.identity.source() == Some(source))
        })
        .map(|(addr, _)| *addr)
}
