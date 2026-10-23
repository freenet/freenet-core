//! TEST-ONLY: a plain Ed25519 raw-public-key identity (RFC 7250) that is NOT
//! derived from any transport key, for tests where a peer presents a key of
//! its own instead of the one it is expected to hold.

use std::sync::Arc;

use ring_crypto::signature::{self, KeyPair};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;

use super::identity::ED25519_SPKI_PREFIX;
use crate::config::GlobalRng;

/// DER prefix of an Ed25519 PKCS#8 v1 private key (RFC 8410): the seed
/// follows.
const PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// An Ed25519 identity from [`GlobalRng`], signed by ring's own Ed25519.
pub(crate) struct TestIdentity {
    pub key: Arc<CertifiedKey>,
    /// The raw public key as sent in the handshake (its SPKI DER).
    pub spki: Vec<u8>,
}

impl TestIdentity {
    pub(crate) fn generate() -> Self {
        let mut seed = [0u8; 32];
        GlobalRng::fill_bytes(&mut seed);
        let pair = signature::Ed25519KeyPair::from_seed_unchecked(&seed)
            .expect("any 32 bytes are an Ed25519 seed");
        let spki = [&ED25519_SPKI_PREFIX[..], pair.public_key().as_ref()].concat();
        let pkcs8 = [&PKCS8_PREFIX[..], &seed[..]].concat();
        let signer = rustls::crypto::ring::sign::any_eddsa_type(&PrivatePkcs8KeyDer::from(pkcs8))
            .expect("a PKCS#8 Ed25519 key");
        let key = Arc::new(CertifiedKey::new(
            vec![CertificateDer::from(spki.clone())],
            signer,
        ));
        Self { key, spki }
    }
}
