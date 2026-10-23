//! Connection IDs that carry a keyed MAC.
//!
//! quinn answers a short-header packet for a connection it does not know
//! with a stateless reset, unless its connection-ID generator says the ID
//! cannot be one of its own (quinn-proto `Endpoint::handle`). A reset tells a
//! prober that the port speaks QUIC. quinn's own `HashedConnectionIdGenerator`
//! validates with a non-cryptographic hash and says it can be spoofed, so
//! this generator appends a truncated keyed BLAKE3 MAC of a random nonce: an
//! ID passes `validate` only if whoever made it knew the key.

use std::time::Duration;

use quinn::ConnectionId;
use quinn::ConnectionIdGenerator;
// quinn does not re-export it; `validate` cannot be implemented without it.
use quinn_proto::InvalidCid;
use subtle::ConstantTimeEq;

use crate::config::GlobalRng;

/// Random bytes at the start of each ID.
const NONCE_LEN: usize = 8;

/// MAC bytes after the nonce. 8 bytes: a forged ID passes with probability
/// 2^-64.
const MAC_LEN: usize = 8;

/// The length of every ID this generator makes.
pub(crate) const CID_LEN: usize = NONCE_LEN + MAC_LEN;

/// Makes and validates connection IDs of a random nonce and a keyed MAC over
/// it. Every generator of one endpoint must share the key, because quinn
/// validates incoming IDs with the endpoint's generator and makes each
/// connection's IDs with another.
pub(crate) struct MacConnectionIdGenerator {
    key: [u8; 32],
}

impl MacConnectionIdGenerator {
    /// A new key, from the OS entropy pool (key material never comes from
    /// `GlobalRng`; see `config::os_random`). The nonces stay on
    /// [`GlobalRng`], so a seeded run makes the same nonces.
    pub(crate) fn new_key() -> std::io::Result<[u8; 32]> {
        let mut key = [0u8; 32];
        super::config::os_random(&mut key)?;
        Ok(key)
    }

    pub(crate) fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    fn mac(&self, nonce: &[u8]) -> [u8; MAC_LEN] {
        let hash = blake3::keyed_hash(&self.key, nonce);
        let mut mac = [0u8; MAC_LEN];
        mac.copy_from_slice(&hash.as_bytes()[..MAC_LEN]);
        mac
    }
}

impl ConnectionIdGenerator for MacConnectionIdGenerator {
    fn generate_cid(&mut self) -> ConnectionId {
        let mut bytes = [0u8; CID_LEN];
        GlobalRng::fill_bytes(&mut bytes[..NONCE_LEN]);
        let mac = self.mac(&bytes[..NONCE_LEN]);
        bytes[NONCE_LEN..].copy_from_slice(&mac);
        ConnectionId::new(&bytes)
    }

    fn validate(&self, cid: &ConnectionId) -> Result<(), InvalidCid> {
        if cid.len() != CID_LEN {
            return Err(InvalidCid);
        }
        let (nonce, mac) = cid.split_at(NONCE_LEN);
        // Constant-time comparison (subtle), so the time taken does not say
        // how many leading bytes of a guess were right.
        if bool::from(self.mac(nonce)[..].ct_eq(mac)) {
            Ok(())
        } else {
            Err(InvalidCid)
        }
    }

    fn cid_len(&self) -> usize {
        CID_LEN
    }

    fn cid_lifetime(&self) -> Option<Duration> {
        None
    }
}
