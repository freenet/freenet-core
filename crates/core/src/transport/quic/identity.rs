//! Peer identity in the QUIC handshake (#5940 step c3).
//!
//! Each node authenticates in the TLS 1.3 handshake with a raw public key
//! (RFC 7250) derived from the X25519 transport key it already has
//! ([`TransportKeypair`]), so QUIC adds no key, no file and no message:
//!
//! - **Signing** uses XEdDSA (Signal's specification) with the X25519
//!   secret. An XEdDSA signature is an ordinary Ed25519 signature (scheme
//!   0x0807) under the Edwards form of the X25519 public key with sign bit 0,
//!   so the peer verifies it with stock Ed25519 code. The 64 bytes of nonce
//!   randomness come from the OS entropy pool, never `GlobalRng`
//!   (`.claude/rules/code-style.md`, the OsRng exception for key material).
//! - **The raw public key** a node presents is that Edwards key, in an
//!   Ed25519 SubjectPublicKeyInfo.
//! - **Verification** is pinned: each side is given the transport key it
//!   expects the peer to hold (the client knows the server's from the UDP
//!   transport session; the server is told the client's when the client is
//!   allowed to connect). The verifier converts that expected X25519 key to
//!   its Edwards form with sign bit 0, the convention XEdDSA signs under,
//!   requires the presented key to be exactly that key (compared in constant
//!   time), and verifies the handshake signature with ring's Ed25519 against
//!   the EXPECTED key. Any other key, a key in another encoding, a chain, or
//!   another signature scheme fails the handshake.
//!
//! A man in the middle that terminates the handshake cannot sign as the
//! expected key, and the TLS 1.3 signature covers the transcript, so a
//! relayed handshake cannot be completed with a different peer either.

use std::sync::Arc;

use ring_crypto::signature;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::{CertifiedKey, Signer, SigningKey};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, Error, PeerMisbehaved,
    SignatureAlgorithm, SignatureScheme,
};
use subtle::ConstantTimeEq;

use super::config::{self, TlsConfigError};
use crate::transport::crypto::{TransportKeypair, TransportPublicKey};

/// DER prefix of an Ed25519 SubjectPublicKeyInfo (RFC 8410); the 32-byte
/// key follows.
pub(crate) const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// Length of an Ed25519 SubjectPublicKeyInfo: the prefix and the key.
pub(crate) const ED25519_SPKI_LEN: usize = ED25519_SPKI_PREFIX.len() + 32;

/// The sign bit XEdDSA signs under and the verifier converts with. XEdDSA
/// fixes it to 0: the signer negates its scalar when its Edwards public key
/// has sign bit 1 (`xeddsa::xed25519::PrivateKey::calculate_key_pair`).
const XEDDSA_SIGN_BIT: u8 = 0;

/// A transport public key that has no Edwards form (the Montgomery point
/// u = -1). No key made from a secret is one; a peer could only claim one.
#[derive(Debug, thiserror::Error)]
#[error("the transport public key has no Ed25519 form")]
pub(crate) struct UnusableKey;

/// The Ed25519 public key (sign bit 0) of an X25519 transport public key:
/// the key XEdDSA signatures by its holder verify under.
pub(crate) fn ed25519_public_key(key: &TransportPublicKey) -> Result<[u8; 32], UnusableKey> {
    use xeddsa::xeddsa::ConvertMont;
    xeddsa::xed25519::PublicKey(*key.as_bytes())
        .convert_mont(XEDDSA_SIGN_BIT)
        .map_err(|_| UnusableKey)
}

fn spki_of(ed25519: &[u8; 32]) -> [u8; ED25519_SPKI_LEN] {
    let mut spki = [0u8; ED25519_SPKI_LEN];
    spki[..ED25519_SPKI_PREFIX.len()].copy_from_slice(&ED25519_SPKI_PREFIX);
    spki[ED25519_SPKI_PREFIX.len()..].copy_from_slice(ed25519);
    spki
}

/// The raw public key a peer is expected to present: the Edwards form of its
/// transport key.
#[derive(Clone)]
pub(crate) struct PeerKey {
    ed25519: [u8; 32],
    spki: [u8; ED25519_SPKI_LEN],
}

/// Compared in constant time, as every key comparison here is.
impl PartialEq for PeerKey {
    fn eq(&self, other: &Self) -> bool {
        bool::from(self.ed25519.ct_eq(&other.ed25519))
    }
}

impl Eq for PeerKey {}

impl std::fmt::Debug for PeerKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PeerKey({})",
            bs58::encode(&self.ed25519[..12]).into_string()
        )
    }
}

impl PeerKey {
    /// The key the holder of `key` presents.
    pub(crate) fn of(key: &TransportPublicKey) -> Result<Self, UnusableKey> {
        let ed25519 = ed25519_public_key(key)?;
        Ok(Self {
            ed25519,
            spki: spki_of(&ed25519),
        })
    }

    /// The SubjectPublicKeyInfo DER this key is presented as.
    #[cfg(test)]
    pub(crate) fn spki(&self) -> &[u8; ED25519_SPKI_LEN] {
        &self.spki
    }

    /// The Ed25519 public key.
    #[cfg(test)]
    pub(crate) fn ed25519(&self) -> &[u8; 32] {
        &self.ed25519
    }

    /// Whether `presented` is exactly this key's SubjectPublicKeyInfo. The
    /// bytes are compared in constant time; the length is public.
    fn matches(&self, presented: &CertificateDer<'_>) -> bool {
        let presented = presented.as_ref();
        presented.len() == ED25519_SPKI_LEN && bool::from(presented.ct_eq(&self.spki))
    }

    /// Check that `presented` is this key and that `dss` is its Ed25519
    /// signature of `message`. The signature is checked against THIS key,
    /// not the presented bytes.
    fn verify(
        &self,
        message: &[u8],
        presented: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.verify_signature(message, presented, dss.scheme, dss.signature())?;
        Ok(HandshakeSignatureValid::assertion())
    }

    /// [`Self::verify`] over the parts of a `DigitallySignedStruct`.
    fn verify_signature(
        &self,
        message: &[u8],
        presented: &CertificateDer<'_>,
        scheme: SignatureScheme,
        signature: &[u8],
    ) -> Result<(), Error> {
        if !self.matches(presented) {
            return Err(unexpected_key());
        }
        if scheme != SignatureScheme::ED25519 {
            return Err(PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme.into());
        }
        signature::UnparsedPublicKey::new(&signature::ED25519, &self.ed25519)
            .verify(message, signature)
            .map_err(|_| Error::InvalidCertificate(CertificateError::BadSignature))
    }

    /// Check a presented end entity and chain before any signature.
    fn check_presented(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
    ) -> Result<(), Error> {
        if intermediates.is_empty() && self.matches(end_entity) {
            Ok(())
        } else {
            Err(unexpected_key())
        }
    }
}

fn unexpected_key() -> Error {
    Error::InvalidCertificate(CertificateError::ApplicationVerificationFailure)
}

fn no_tls12() -> Error {
    Error::General("TLS 1.2 is not supported".into())
}

/// The node's own QUIC identity: its transport key as a signing key and raw
/// public key, for both roles.
pub(crate) struct QuicIdentity {
    key: Arc<CertifiedKey>,
}

impl std::fmt::Debug for QuicIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicIdentity")
            .field("key", &self.key.key)
            .finish()
    }
}

impl QuicIdentity {
    /// The identity of the holder of `keypair`.
    pub(crate) fn new(keypair: &TransportKeypair) -> Result<Self, UnusableKey> {
        let signing = XeddsaSigningKey::new(keypair)?;
        let spki = signing.public.spki.to_vec();
        Ok(Self {
            key: Arc::new(CertifiedKey::new(
                vec![CertificateDer::from(spki)],
                Arc::new(signing),
            )),
        })
    }

    /// The raw public key and signer, as rustls takes them.
    pub(crate) fn certified_key(&self) -> Arc<CertifiedKey> {
        self.key.clone()
    }

    /// TLS for connecting to the holder of `server`.
    pub(crate) fn client_tls(
        &self,
        server: &PeerKey,
    ) -> Result<quinn::crypto::rustls::QuicClientConfig, TlsConfigError> {
        config::tls_client_config(self.certified_key(), Arc::new(PinnedServerKey::new(server)))
    }

    /// TLS for accepting a connection from the holder of `client`.
    pub(crate) fn server_tls(
        &self,
        client: &PeerKey,
    ) -> Result<quinn::crypto::rustls::QuicServerConfig, TlsConfigError> {
        config::tls_server_config(self.certified_key(), Arc::new(PinnedClientKey::new(client)))
    }

    /// TLS that completes no handshake: the endpoint's default server
    /// configuration, used only for an attempt the endpoint accepted
    /// without a configuration of its own (`QuicEndpoint::accept` never does).
    pub(crate) fn refusing_server_tls(
        &self,
    ) -> Result<quinn::crypto::rustls::QuicServerConfig, TlsConfigError> {
        config::tls_server_config(self.certified_key(), Arc::new(RefuseEveryClient))
    }
}

/// The X25519 transport secret as an XEdDSA signing key. The secret is
/// zeroized on drop (`xeddsa::xed25519::PrivateKey` is `ZeroizeOnDrop`) and
/// never printed: `Debug` shows only the public key.
pub(crate) struct XeddsaSigningKey {
    secret: Arc<xeddsa::xed25519::PrivateKey>,
    public: PeerKey,
}

impl std::fmt::Debug for XeddsaSigningKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XeddsaSigningKey")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl XeddsaSigningKey {
    pub(crate) fn new(keypair: &TransportKeypair) -> Result<Self, UnusableKey> {
        Ok(Self {
            secret: Arc::new(keypair.auth_token_signer()),
            public: PeerKey::of(keypair.public())?,
        })
    }
}

impl SigningKey for XeddsaSigningKey {
    fn choose_scheme(&self, offered: &[SignatureScheme]) -> Option<Box<dyn Signer>> {
        offered
            .contains(&SignatureScheme::ED25519)
            .then(|| -> Box<dyn Signer> {
                Box::new(XeddsaSigner {
                    secret: self.secret.clone(),
                })
            })
    }

    fn public_key(&self) -> Option<SubjectPublicKeyInfoDer<'_>> {
        Some(SubjectPublicKeyInfoDer::from(&self.public.spki[..]))
    }

    fn algorithm(&self) -> SignatureAlgorithm {
        SignatureAlgorithm::ED25519
    }
}

/// One handshake's signer, sharing the signing key's secret.
struct XeddsaSigner {
    secret: Arc<xeddsa::xed25519::PrivateKey>,
}

impl std::fmt::Debug for XeddsaSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XeddsaSigner").finish_non_exhaustive()
    }
}

impl Signer for XeddsaSigner {
    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(xeddsa_sign(&self.secret, message).to_vec())
    }

    fn scheme(&self) -> SignatureScheme {
        SignatureScheme::ED25519
    }
}

/// XEdDSA-sign `message`. The nonce randomness is from the OS (SysRng), not
/// `GlobalRng`: it is secret input to the signature nonce, and a seeded
/// `GlobalRng` is known to anyone who reads a test's seed (code-style.md,
/// the OsRng exception). Signing happens once per handshake, never in a
/// per-packet path.
fn xeddsa_sign(secret: &xeddsa::xed25519::PrivateKey, message: &[u8]) -> [u8; 64] {
    use xeddsa::xeddsa::Sign;
    secret.sign(message, rand_core10::UnwrapErr(rand10::rngs::SysRng))
}

/// A client's check of the server: it must present exactly the expected key.
#[derive(Debug)]
pub(crate) struct PinnedServerKey {
    expected: PeerKey,
}

impl PinnedServerKey {
    pub(crate) fn new(expected: &PeerKey) -> Self {
        Self {
            expected: expected.clone(),
        }
    }
}

impl ServerCertVerifier for PinnedServerKey {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        self.expected.check_presented(end_entity, intermediates)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(no_tls12())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.expected.verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// A server's check of the client: it must present exactly the expected key.
#[derive(Debug)]
pub(crate) struct PinnedClientKey {
    expected: PeerKey,
}

impl PinnedClientKey {
    pub(crate) fn new(expected: &PeerKey) -> Self {
        Self {
            expected: expected.clone(),
        }
    }
}

impl ClientCertVerifier for PinnedClientKey {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        self.expected.check_presented(end_entity, intermediates)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(no_tls12())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        self.expected.verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// A server's check that accepts no client.
#[derive(Debug)]
struct RefuseEveryClient;

impl ClientCertVerifier for RefuseEveryClient {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        Err(unexpected_key())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(no_tls12())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(unexpected_key())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
