//! The peer key a connection's transport holds.
//!
//! The transport learns its peer's key from the node, never from anything
//! the peer sends over the connection: the key it dialled, for an outbound
//! connection, and the key the node promoted the connection into the ring
//! under (`PeerConnectionApi::set_peer_identity`), which the node takes from
//! the identity it bound to the connection at establishment. A QUIC upgrade
//! of the connection (#5940) authenticates the peer against this key.
//!
//! The key does not change once held: a later, different key is refused and
//! the held key kept.

use crate::transport::TransportPublicKey;

/// The peer key a connection's transport holds, if any.
#[derive(Debug, Default)]
pub(super) struct PeerIdentity(Option<TransportPublicKey>);

/// What [`PeerIdentity::update`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PeerIdentityUpdate {
    /// No key was held; the key is now held.
    Set,
    /// The key was already held. Nothing changed.
    Same,
    /// Another key is held, and kept. Nothing changed.
    Refused,
}

impl PeerIdentity {
    /// The key this node dialled the peer with.
    pub(super) fn dialled(key: TransportPublicKey) -> Self {
        Self(Some(key))
    }

    /// The held key, if any.
    pub(super) fn key(&self) -> Option<&TransportPublicKey> {
        self.0.as_ref()
    }

    /// Hold `key`, unless another key is held.
    pub(super) fn update(&mut self, key: TransportPublicKey) -> PeerIdentityUpdate {
        match &self.0 {
            None => {
                self.0 = Some(key);
                PeerIdentityUpdate::Set
            }
            Some(held) if *held == key => PeerIdentityUpdate::Same,
            Some(_) => PeerIdentityUpdate::Refused,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn key() -> TransportPublicKey {
        TransportKeypair::new().public().clone()
    }

    #[test]
    fn an_unknown_identity_is_set_once_and_then_kept() {
        let (first, second) = (key(), key());
        let mut identity = PeerIdentity::default();
        assert_eq!(identity.key(), None);
        assert_eq!(identity.update(first.clone()), PeerIdentityUpdate::Set);
        assert_eq!(identity.key(), Some(&first));
        assert_eq!(identity.update(first.clone()), PeerIdentityUpdate::Same);
        assert_eq!(
            identity.update(second),
            PeerIdentityUpdate::Refused,
            "a different key is refused"
        );
        assert_eq!(identity.key(), Some(&first), "the held key is kept");
    }

    #[test]
    fn a_dialled_identity_refuses_another_key() {
        let (dialled, other) = (key(), key());
        let mut identity = PeerIdentity::dialled(dialled.clone());
        assert_eq!(identity.update(dialled.clone()), PeerIdentityUpdate::Same);
        assert_eq!(identity.update(other), PeerIdentityUpdate::Refused);
        assert_eq!(identity.key(), Some(&dialled));
    }
}
