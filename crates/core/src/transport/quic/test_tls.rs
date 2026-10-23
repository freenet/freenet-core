//! TEST-ONLY TLS identities: Ed25519 raw public keys (RFC 7250) and
//! verifiers that accept ANY such key.
//!
//! A verifier here checks that the peer presented an Ed25519 raw public key
//! and that the handshake signature verifies against that key, so the
//! handshake is a real TLS 1.3 handshake with a real proof of the key. It
//! does not check WHICH key: binding a key to a peer's node identity is step
//! c3 (#5940), and until then nothing outside tests may build these.

use std::sync::Arc;

use ring_crypto::signature::{self, KeyPair};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, Error, SignatureScheme};

use crate::config::GlobalRng;

/// DER prefix of an Ed25519 SubjectPublicKeyInfo (RFC 8410): the key follows.
const SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// DER prefix of an Ed25519 PKCS#8 v1 private key (RFC 8410): the seed
/// follows.
const PKCS8_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// An Ed25519 identity for one test endpoint, from [`GlobalRng`].
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
        let spki = [&SPKI_PREFIX[..], pair.public_key().as_ref()].concat();
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

/// The Ed25519 key in a raw-public-key "certificate", if it is one.
fn ed25519_key<'a>(cert: &'a CertificateDer<'_>) -> Result<&'a [u8], Error> {
    cert.strip_prefix(&SPKI_PREFIX[..])
        .filter(|key| key.len() == 32)
        .ok_or(Error::InvalidCertificate(
            rustls::CertificateError::BadEncoding,
        ))
}

fn verify(
    message: &[u8],
    cert: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
) -> Result<HandshakeSignatureValid, Error> {
    if dss.scheme != SignatureScheme::ED25519 {
        return Err(Error::PeerMisbehaved(
            rustls::PeerMisbehaved::SignedHandshakeWithUnadvertisedSigScheme,
        ));
    }
    signature::UnparsedPublicKey::new(&signature::ED25519, ed25519_key(cert)?)
        .verify(message, dss.signature())
        .map_err(|_| Error::InvalidCertificate(rustls::CertificateError::BadSignature))?;
    Ok(HandshakeSignatureValid::assertion())
}

/// Accepts a server presenting any Ed25519 raw public key it can sign with.
#[derive(Debug)]
pub(crate) struct AcceptAnyServerKey;

impl ServerCertVerifier for AcceptAnyServerKey {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        ed25519_key(end_entity)?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 is not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// Requires a client to present an Ed25519 raw public key, and accepts any
/// it can sign with.
#[derive(Debug)]
pub(crate) struct AcceptAnyClientKey;

impl ClientCertVerifier for AcceptAnyClientKey {
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
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        ed25519_key(end_entity)?;
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General("TLS 1.2 is not supported".into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}
