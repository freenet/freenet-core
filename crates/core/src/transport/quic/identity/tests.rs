//! Unit tests for the QUIC identity: the X25519 to Ed25519 conversion, the
//! XEdDSA signer, and the pinned verifiers, each checked against code that
//! does not share this module's path (curve25519-dalek 5 for the conversion,
//! ed25519-dalek for verification; production verifies with ring).

use rustls::pki_types::CertificateDer;
use rustls::sign::SigningKey;
use rustls::{Error, SignatureScheme};

use super::*;
use crate::config::GlobalRng;
use crate::transport::crypto::{TransportKeypair, TransportPublicKey};

const SEED: u64 = 0x0000_C31D;

/// The Edwards form, sign bit 0, of the Montgomery u-coordinate `u`, by
/// curve25519-dalek 5 (production converts with the xeddsa crate, which
/// uses curve25519-dalek 4).
fn independent_edwards(u: [u8; 32]) -> Option<[u8; 32]> {
    curve25519_dalek::montgomery::MontgomeryPoint(u)
        .to_edwards(0)
        .map(|point| point.compress().to_bytes())
}

/// Sign `message` the way a handshake does: through rustls's `SigningKey`
/// and `Signer` traits.
fn handshake_sign(keys: &TransportKeypair, message: &[u8]) -> Vec<u8> {
    XeddsaSigningKey::new(keys)
        .unwrap()
        .choose_scheme(&[SignatureScheme::ED25519])
        .expect("Ed25519 offered")
        .sign(message)
        .unwrap()
}

fn cert(bytes: &[u8]) -> CertificateDer<'static> {
    CertificateDer::from(bytes.to_vec())
}

/// The conversion agrees with an independent implementation for random
/// u-coordinates, half of them with bit 255 set (X25519 ignores that bit,
/// RFC 7748 section 5, and so does the bare conversion), and refuses u = -1,
/// the one value with no Edwards form, as the independent one does. As an
/// IDENTITY (`PeerKey::of`) an encoding with bit 255 set is refused, so one
/// secret cannot authenticate as two byte-distinct transport keys; its
/// canonical twin is accepted.
#[test]
fn the_conversion_matches_an_independent_implementation() {
    let _seed = GlobalRng::seed_guard(SEED);
    for i in 0..1024 {
        let mut u = [0u8; 32];
        GlobalRng::fill_bytes(&mut u);
        if i % 2 == 0 {
            u[31] |= 0x80;
        } else {
            u[31] &= 0x7f;
        }
        let ours = ed25519_public_key(&TransportPublicKey::from_bytes(u)).ok();
        assert_eq!(ours, independent_edwards(u), "u = {}", hex::encode(u));
        let mut flipped = u;
        flipped[31] ^= 0x80;
        assert_eq!(
            ed25519_public_key(&TransportPublicKey::from_bytes(flipped)).ok(),
            ours,
            "bit 255 of u is ignored by the conversion"
        );
        let with_high_bit = if u[31] & 0x80 != 0 { u } else { flipped };
        assert!(
            PeerKey::of(&TransportPublicKey::from_bytes(with_high_bit)).is_err(),
            "an encoding with bit 255 set is not an identity"
        );
        if let Some(edwards) = ours {
            assert_eq!(edwards[31] & 0x80, 0, "sign bit 0");
        }
    }
    // u = p - 1 = 2^255 - 20, little-endian.
    let mut minus_one = [0xffu8; 32];
    minus_one[0] = 0xec;
    minus_one[31] = 0x7f;
    assert!(independent_edwards(minus_one).is_none());
    assert!(ed25519_public_key(&TransportPublicKey::from_bytes(minus_one)).is_err());
    assert!(PeerKey::of(&TransportPublicKey::from_bytes(minus_one)).is_err());
}

/// For many keypairs the key the verifier expects (converted from the
/// public key) is the key the signer signs under (derived from the secret,
/// as XEdDSA does), and that key matches the independent conversion. About
/// half of the secrets have a natural Edwards key with sign bit 1, which
/// XEdDSA negates; the test checks both kinds occurred, so it covers the
/// negation.
#[test]
fn the_signer_and_the_verifier_agree_on_the_key() {
    use xeddsa::xeddsa::CalculateKeyPair;
    let _seed = GlobalRng::seed_guard(SEED);
    let mut natural_sign = [0usize; 2];
    for _ in 0..256 {
        let keys = TransportKeypair::new();
        let secret = keys.auth_token_signer();
        let natural = curve25519_dalek::edwards::EdwardsPoint::mul_base_clamped(secret.0)
            .compress()
            .to_bytes();
        natural_sign[usize::from(natural[31] >> 7)] += 1;
        let (_, signer_public): ([u8; 32], [u8; 32]) = secret.calculate_key_pair(0);
        let expected = PeerKey::of(keys.public()).unwrap();
        assert_eq!(expected.ed25519(), &signer_public);
        assert_eq!(
            Some(*expected.ed25519()),
            independent_edwards(*keys.public().as_bytes())
        );
        assert_eq!(XeddsaSigningKey::new(&keys).unwrap().public, expected);
        let mut spki = ED25519_SPKI_PREFIX.to_vec();
        spki.extend_from_slice(&signer_public);
        assert_eq!(expected.spki()[..], spki[..]);
    }
    assert!(
        natural_sign[0] > 0 && natural_sign[1] > 0,
        "both natural sign bits occurred: {natural_sign:?}"
    );
}

/// A handshake signature verifies with ed25519-dalek (and with ring, the
/// production verifier) under the converted key, and fails with either
/// when any one bit of the signature or the message is flipped.
#[test]
fn signatures_verify_with_an_independent_ed25519_and_fail_when_altered() {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let _seed = GlobalRng::seed_guard(SEED);
    for round in 0..16 {
        let keys = TransportKeypair::new();
        let peer = PeerKey::of(keys.public()).unwrap();
        let mut message = vec![0u8; 64 + round * 37];
        GlobalRng::fill_bytes(&mut message);
        let signature = handshake_sign(&keys, &message);
        assert_eq!(signature.len(), 64);
        let dalek = VerifyingKey::from_bytes(peer.ed25519()).unwrap();
        let verifies = |message: &[u8], signature: &[u8]| -> (bool, bool) {
            let sig: [u8; 64] = signature.try_into().unwrap();
            (
                dalek.verify(message, &Signature::from_bytes(&sig)).is_ok(),
                peer.verify_signature(
                    message,
                    &cert(peer.spki()),
                    SignatureScheme::ED25519,
                    signature,
                )
                .is_ok(),
            )
        };
        assert_eq!(verifies(&message, &signature), (true, true));
        for byte in 0..64 {
            let mut altered = signature.clone();
            altered[byte] ^= 1 << (byte % 8);
            assert_eq!(
                verifies(&message, &altered),
                (false, false),
                "signature byte {byte}"
            );
        }
        let mut altered = message.clone();
        altered[round] ^= 0x10;
        assert_eq!(verifies(&altered, &signature), (false, false), "message");
    }
}

/// Two signatures of one message under one seeded `GlobalRng` differ: the
/// nonce randomness comes from the OS, not from the seeded generator.
#[test]
fn the_signature_nonce_does_not_come_from_global_rng() {
    let keys = {
        let _seed = GlobalRng::seed_guard(SEED);
        TransportKeypair::new()
    };
    let sign = || {
        let _seed = GlobalRng::seed_guard(SEED);
        handshake_sign(&keys, b"one handshake transcript")
    };
    let first = sign();
    let second = sign();
    assert_ne!(first, second, "the same seed gave the same signature");
}

/// The verifier accepts exactly the expected key's SPKI as the end entity
/// with no chain, and refuses every other presentation: another node's key,
/// a plain Ed25519 key, the same Edwards point with sign bit 1, the expected
/// key with a bit flipped, a truncated or extended encoding, a certificate
/// chain, and arbitrary DER.
#[test]
fn the_verifier_accepts_only_the_expected_key() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let expected = PeerKey::of(keys.public()).unwrap();
    let exact = cert(expected.spki());
    assert!(expected.check_presented(&exact, &[]).is_ok());

    let other = PeerKey::of(TransportKeypair::new().public()).unwrap();
    let foreign = super::super::test_tls::TestIdentity::generate().spki;
    let mut negated = expected.spki().to_vec();
    negated[ED25519_SPKI_LEN - 1] ^= 0x80;
    let mut refused: Vec<(&str, Vec<u8>)> = vec![
        ("another node's key", other.spki().to_vec()),
        ("a plain Ed25519 key", foreign),
        ("the sign-bit-1 point", negated),
        (
            "truncated",
            expected.spki()[..ED25519_SPKI_LEN - 1].to_vec(),
        ),
        ("extended", [&expected.spki()[..], &[0]].concat()),
        ("empty", Vec::new()),
        ("the bare key", expected.ed25519().to_vec()),
        // An X.509 certificate header, not a SubjectPublicKeyInfo.
        (
            "arbitrary DER",
            vec![0x30, 0x82, 0x01, 0x0a, 0x30, 0x81, 0xb5],
        ),
    ];
    for byte in 0..ED25519_SPKI_LEN {
        let mut flipped = expected.spki().to_vec();
        flipped[byte] ^= 1;
        refused.push(("one flipped bit", flipped));
    }
    let server = PinnedServerKey::new(&expected);
    let client = PinnedClientKey::new(&expected);
    let name = rustls::pki_types::ServerName::try_from("freenet").unwrap();
    let now = rustls::pki_types::UnixTime::now();
    for (what, bytes) in &refused {
        let presented = cert(bytes);
        assert!(
            expected.check_presented(&presented, &[]).is_err(),
            "{what} accepted"
        );
        assert!(
            server
                .verify_server_cert(&presented, &[], &name, &[], now)
                .is_err(),
            "server verifier accepted {what}"
        );
        assert!(
            client.verify_client_cert(&presented, &[], now).is_err(),
            "client verifier accepted {what}"
        );
    }
    let chain = [cert(expected.spki())];
    assert!(expected.check_presented(&exact, &chain).is_err(), "a chain");
    assert!(
        server
            .verify_server_cert(&exact, &chain, &name, &[], now)
            .is_err()
    );
    assert!(client.verify_client_cert(&exact, &chain, now).is_err());
    assert!(
        server
            .verify_server_cert(&exact, &[], &name, &[], now)
            .is_ok()
    );
    assert!(client.verify_client_cert(&exact, &[], now).is_ok());
    for verifier in [
        server.requires_raw_public_keys(),
        client.requires_raw_public_keys(),
    ] {
        assert!(verifier, "raw public keys only");
    }
    assert!(client.offer_client_auth() && client.client_auth_mandatory());
    assert_eq!(
        server.supported_verify_schemes(),
        vec![SignatureScheme::ED25519]
    );
    assert_eq!(
        client.supported_verify_schemes(),
        vec![SignatureScheme::ED25519]
    );
}

/// A valid signature is refused when it is by another key than the
/// expected one (presented as itself or as the expected key), when it is
/// declared under another scheme, and when the presented key is not the
/// expected one even though the signature is by the expected key.
#[test]
fn a_signature_counts_only_from_the_expected_key_and_scheme() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let intruder = TransportKeypair::new();
    let expected = PeerKey::of(keys.public()).unwrap();
    let intruder_key = PeerKey::of(intruder.public()).unwrap();
    let message = b"transcript";
    let good = handshake_sign(&keys, message);
    let forged = handshake_sign(&intruder, message);
    let verify = |presented: &PeerKey, scheme, signature: &[u8]| {
        expected.verify_signature(message, &cert(presented.spki()), scheme, signature)
    };
    assert!(verify(&expected, SignatureScheme::ED25519, &good).is_ok());
    assert!(matches!(
        verify(&expected, SignatureScheme::ED25519, &forged),
        Err(Error::InvalidCertificate(
            rustls::CertificateError::BadSignature
        ))
    ));
    assert!(matches!(
        verify(&intruder_key, SignatureScheme::ED25519, &forged),
        Err(Error::InvalidCertificate(
            rustls::CertificateError::ApplicationVerificationFailure
        ))
    ));
    assert!(verify(&intruder_key, SignatureScheme::ED25519, &good).is_err());
    for scheme in [
        SignatureScheme::ED448,
        SignatureScheme::ECDSA_NISTP256_SHA256,
        SignatureScheme::RSA_PSS_SHA256,
    ] {
        assert!(verify(&expected, scheme, &good).is_err(), "{scheme:?}");
    }
}

/// The signing key offers only Ed25519, presents the converted key, and
/// never prints the secret.
#[test]
fn the_signing_key_offers_ed25519_and_hides_the_secret() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let signing = XeddsaSigningKey::new(&keys).unwrap();
    assert!(
        signing
            .choose_scheme(&[
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::RSA_PSS_SHA256
            ])
            .is_none()
    );
    let signer = signing
        .choose_scheme(&[SignatureScheme::RSA_PSS_SHA256, SignatureScheme::ED25519])
        .unwrap();
    assert_eq!(signer.scheme(), SignatureScheme::ED25519);
    assert_eq!(signing.algorithm(), rustls::SignatureAlgorithm::ED25519);
    let expected = PeerKey::of(keys.public()).unwrap();
    assert_eq!(signing.public_key().unwrap().as_ref(), &expected.spki()[..]);

    let identity = QuicIdentity::new(&keys).unwrap();
    let secret = keys.auth_token_signer().0;
    let rendered = [
        format!("{signing:?}"),
        format!("{signer:?}"),
        format!("{identity:?}"),
        format!("{:?}", identity.certified_key()),
    ];
    for text in &rendered {
        for window in secret.windows(4) {
            assert!(
                !text.contains(&hex::encode(window)),
                "secret bytes in {text}"
            );
            assert!(
                !text.contains(&format!("{}, {}, {}", window[0], window[1], window[2])),
                "secret bytes in {text}"
            );
        }
        assert!(!text.contains(&bs58::encode(secret).into_string()));
    }
}

/// The little-endian 256-bit sum of `a` and `b`, or `None` past 2^256.
fn add_le(a: &[u8; 32], b: &[u8; 32]) -> Option<[u8; 32]> {
    let mut out = [0u8; 32];
    let mut carry = 0u16;
    for i in 0..32 {
        let sum = u16::from(a[i]) + u16::from(b[i]) + carry;
        out[i] = sum as u8;
        carry = sum >> 8;
    }
    (carry == 0).then_some(out)
}

/// Transport keys whose Edwards form has small order are refused: u = 0,
/// u = 1, and the two order-8 points, which an attacker could claim and
/// then forge a CertificateVerify for without a secret (Ed25519 verification
/// does not check the order of the key). So are their non-canonical
/// encodings u + p that still fit in 255 bits, and u = p - 1, which has no
/// Edwards form. Each listed point is checked to really be small-order by
/// curve25519-dalek 5, so the list cannot rot into ordinary points.
#[test]
fn small_order_and_non_canonical_keys_are_refused() {
    fn le(hex: &str) -> [u8; 32] {
        hex::decode(hex).unwrap().try_into().unwrap()
    }
    let zero = [0u8; 32];
    let mut one = [0u8; 32];
    one[0] = 1;
    let small_order = [
        ("u = 0", zero),
        ("u = 1", one),
        (
            "order 8, first",
            le("e0eb7a7c3b41b8ae1656e3faf19fc46ada098deb9c32b1fd866205165f49b800"),
        ),
        (
            "order 8, second",
            le("5f9c95bca3508c24b1d0b1559c83ef5b04445cc4581c8e86d8224eddd09f1157"),
        ),
    ];
    let p = super::FIELD_PRIME;
    for (what, u) in small_order {
        let edwards = curve25519_dalek::montgomery::MontgomeryPoint(u)
            .to_edwards(0)
            .expect("small-order points have an Edwards form");
        assert!(edwards.is_small_order(), "{what} is small-order");
        assert!(super::is_canonical(&u), "{what} is canonical");
        assert!(
            PeerKey::of(&TransportPublicKey::from_bytes(u)).is_err(),
            "{what} accepted as an identity"
        );
        // u + p, when it still fits in 255 bits: the same point, encoded
        // non-canonically.
        if let Some(alias) = add_le(&u, &p).filter(|a| a[31] & 0x80 == 0) {
            assert!(!super::is_canonical(&alias));
            assert!(
                PeerKey::of(&TransportPublicKey::from_bytes(alias)).is_err(),
                "{what} + p accepted as an identity"
            );
        }
    }
    let mut minus_one = p;
    minus_one[0] = 0xec;
    for (what, u) in [
        ("p - 1", minus_one),
        ("p", p),
        ("2^255 - 1", {
            let mut m = [0xffu8; 32];
            m[31] = 0x7f;
            m
        }),
    ] {
        assert!(
            PeerKey::of(&TransportPublicKey::from_bytes(u)).is_err(),
            "{what} accepted as an identity"
        );
    }
    // Ordinary keys from secrets are canonical and accepted.
    let _seed = GlobalRng::seed_guard(SEED);
    for _ in 0..64 {
        let keys = TransportKeypair::new();
        assert!(super::is_canonical(keys.public().as_bytes()));
        assert!(PeerKey::of(keys.public()).is_ok());
    }
}

/// A signature draws exactly the 64 random bytes taken from the OS before
/// signing: all of them are consumed (so none is wasted and the nonce gets
/// the full 64), and `DrawnRandom` panics if `xeddsa` asks for more, so
/// bytes are never repeated or invented.
#[test]
fn a_signature_draws_exactly_the_drawn_randomness() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let mut drawn = super::DrawnRandom::new([3u8; super::XEDDSA_RANDOM_BYTES]);
    super::sign_with(&keys.auth_token_signer(), b"transcript", &mut drawn);
    assert_eq!(
        drawn.used,
        super::XEDDSA_RANDOM_BYTES,
        "one signature consumes exactly the drawn bytes"
    );
    let signature = super::xeddsa_sign(&keys.auth_token_signer(), b"transcript").unwrap();
    let peer = PeerKey::of(keys.public()).unwrap();
    assert!(
        peer.verify_signature(
            b"transcript",
            &cert(peer.spki()),
            SignatureScheme::ED25519,
            &signature
        )
        .is_ok()
    );
    let mut random = super::DrawnRandom::new([7u8; super::XEDDSA_RANDOM_BYTES]);
    let mut first = [0u8; 40];
    rand_core10::TryRng::try_fill_bytes(&mut random, &mut first).unwrap();
    let mut rest = [0u8; 24];
    rand_core10::TryRng::try_fill_bytes(&mut random, &mut rest).unwrap();
    let more = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut one = [0u8; 1];
        rand_core10::TryRng::try_fill_bytes(&mut random, &mut one).unwrap();
    }));
    assert!(more.is_err(), "a 65th byte is refused, not repeated");
}

/// A peer key prints as its transport key's fingerprint, as
/// `TransportPublicKey` itself does, not as Edwards bytes.
#[test]
fn a_peer_key_prints_as_its_transport_key() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let peer = PeerKey::of(keys.public()).unwrap();
    assert_eq!(format!("{peer:?}"), format!("PeerKey({})", keys.public()));
    assert_eq!(peer.transport_key(), keys.public());
}

/// A key with a torsion component (the holder's key plus a point of order
/// 8, or of order 2) is refused: its holder's signatures would verify under
/// it for some nonces, so one secret could authenticate as several keys.
/// The torsioned key is built in Edwards form and mapped back to its
/// Montgomery u, which is what a peer would claim.
#[test]
fn a_key_with_a_torsion_component_is_refused() {
    use curve25519_dalek::constants::EIGHT_TORSION;
    use curve25519_dalek::edwards::CompressedEdwardsY;
    let _seed = GlobalRng::seed_guard(SEED);
    let mut refused = 0;
    for _ in 0..16 {
        let keys = TransportKeypair::new();
        let honest = PeerKey::of(keys.public()).unwrap();
        let a = CompressedEdwardsY(*honest.ed25519()).decompress().unwrap();
        // EIGHT_TORSION[1] has order 8, [4] order 2.
        for t in [EIGHT_TORSION[1], EIGHT_TORSION[4]] {
            let mixed = a + t;
            assert!(!mixed.is_small_order() && !mixed.is_torsion_free());
            let u = mixed.to_montgomery().to_bytes();
            assert!(
                PeerKey::of(&TransportPublicKey::from_bytes(u)).is_err(),
                "a key with a torsion component was accepted"
            );
            refused += 1;
        }
    }
    assert_eq!(refused, 32);
}
