use std::path::Path;

use serde::{Deserialize, Serialize};
use x25519_dalek::{PublicKey, SharedSecret, StaticSecret};

/// X25519 keypair for transport layer key exchange.
///
/// Uses Curve25519 Diffie-Hellman for key exchange, which is:
/// - ~100-1000x faster than RSA-2048 for key generation and exchange
/// - 32 bytes vs 256 bytes for public keys
/// - Used by TLS 1.3, WireGuard, Signal, etc.
#[derive(Clone)]
pub struct TransportKeypair {
    pub(super) public: TransportPublicKey,
    pub(super) secret: TransportSecretKey,
}

impl std::fmt::Debug for TransportKeypair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportKeypair")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl PartialEq for TransportKeypair {
    fn eq(&self, other: &Self) -> bool {
        self.public == other.public
    }
}

impl Eq for TransportKeypair {}

impl TransportKeypair {
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        // Save as hex-encoded secret key (32 bytes = 64 hex chars)
        let hex = hex::encode(self.secret.0.as_bytes());
        file.write_all(hex.as_bytes())?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path)?;
        let mut hex = String::new();
        file.read_to_string(&mut hex)?;
        let bytes = hex::decode(hex.trim())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if bytes.len() != 32 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid key length",
            ));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(&bytes);
        let secret = StaticSecret::from(key_bytes);
        let public = PublicKey::from(&secret);
        Ok(Self {
            public: TransportPublicKey(public),
            secret: TransportSecretKey(secret),
        })
    }
}

impl Default for TransportKeypair {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportKeypair {
    pub fn new() -> Self {
        // Generate 32 random bytes and create a StaticSecret from them
        let random_bytes: [u8; 32] = rand::random();
        Self::from_secret_bytes(random_bytes)
    }

    pub fn from_secret_bytes(bytes: [u8; 32]) -> Self {
        let secret = StaticSecret::from(bytes);
        let public = PublicKey::from(&secret);
        TransportKeypair {
            public: TransportPublicKey(public),
            secret: TransportSecretKey(secret),
        }
    }

    pub fn public(&self) -> &TransportPublicKey {
        &self.public
    }

    #[allow(dead_code)]
    pub(crate) fn secret(&self) -> &TransportSecretKey {
        &self.secret
    }
}

// Custom serialization to make TransportKeypair serializable
impl Serialize for TransportKeypair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // Serialize the secret key bytes (public key can be derived)
        let bytes = self.secret.0.as_bytes();
        bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TransportKeypair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(Self::from_secret_bytes(bytes))
    }
}

/// X25519 public key (32 bytes).
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TransportPublicKey(pub(super) PublicKey);

impl TransportPublicKey {
    /// Get the raw 32-byte public key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Create from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        TransportPublicKey(PublicKey::from(bytes))
    }

    /// Save the public key to a file in hex format.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        let hex = hex::encode(self.0.as_bytes());
        file.write_all(hex.as_bytes())?;
        Ok(())
    }
}

impl Serialize for TransportPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TransportPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(TransportPublicKey(PublicKey::from(bytes)))
    }
}

impl std::fmt::Debug for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display first 16 bytes as base58 for a compact representation
        write!(
            f,
            "{}",
            bs58::encode(&self.0.as_bytes()[..16]).into_string()
        )
    }
}

/// X25519 secret key for ECDH key exchange.
#[derive(Clone)]
pub(crate) struct TransportSecretKey(pub(super) StaticSecret);

impl TransportSecretKey {
    /// Perform Diffie-Hellman key exchange with a remote public key.
    /// Returns a 32-byte shared secret.
    pub fn diffie_hellman(&self, remote_public: &TransportPublicKey) -> SharedSecret {
        self.0.diffie_hellman(&remote_public.0)
    }
}

impl std::fmt::Debug for TransportSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportSecretKey").finish_non_exhaustive()
    }
}

impl PartialEq for TransportSecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes() == other.0.as_bytes()
    }
}

impl Eq for TransportSecretKey {}

impl Serialize for TransportSecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.as_bytes().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TransportSecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(TransportSecretKey(StaticSecret::from(bytes)))
    }
}

/// Ephemeral X25519 keypair for single-use key exchange.
///
/// This is used during the handshake to provide forward secrecy.
/// Each connection generates a new ephemeral keypair.
///
/// Note: We use StaticSecret internally because EphemeralSecret requires
/// rand_core 0.6 which conflicts with our rand version. StaticSecret provides
/// the same cryptographic properties when used only once.
pub struct EphemeralKeypair {
    pub public: TransportPublicKey,
    secret: StaticSecret,
}

impl EphemeralKeypair {
    /// Generate a new ephemeral keypair.
    pub fn new() -> Self {
        // Generate random bytes and create a StaticSecret from them
        // (StaticSecret::from clamps the bytes internally just like EphemeralSecret)
        let random_bytes: [u8; 32] = rand::random();
        let secret = StaticSecret::from(random_bytes);
        let public = PublicKey::from(&secret);
        Self {
            public: TransportPublicKey(public),
            secret,
        }
    }

    /// Perform Diffie-Hellman key exchange and consume the ephemeral keypair.
    /// Returns a 32-byte shared secret.
    pub fn diffie_hellman(self, remote_public: &TransportPublicKey) -> SharedSecret {
        self.secret.diffie_hellman(&remote_public.0)
    }
}

impl Default for EphemeralKeypair {
    fn default() -> Self {
        Self::new()
    }
}

/// Derive symmetric encryption keys from a shared secret using HKDF.
///
/// Returns two 16-byte AES-128 keys:
/// - first_to_second: Key for messages from first party to second party
/// - second_to_first: Key for messages from second party to first party
///
/// The keys are derived using a canonical ordering of public keys to ensure
/// both parties derive the same keys regardless of which order they pass them.
pub fn derive_symmetric_keys(
    shared_secret: &[u8],
    first_public: &[u8; 32],
    second_public: &[u8; 32],
) -> ([u8; 16], [u8; 16]) {
    use hkdf::Hkdf;
    use sha2::Sha256;

    // Sort public keys canonically to ensure both parties derive same keys
    let (lower, higher) = if first_public < second_public {
        (first_public, second_public)
    } else {
        (second_public, first_public)
    };

    // Build info string using canonical order for domain separation
    let mut info = Vec::with_capacity(64);
    info.extend_from_slice(lower);
    info.extend_from_slice(higher);

    let hk = Hkdf::<Sha256>::new(None, shared_secret);

    let mut key_material = [0u8; 32];
    hk.expand(&info, &mut key_material)
        .expect("32 bytes is valid output length for HKDF-SHA256");

    // lower_to_higher key is first half, higher_to_lower is second half
    let mut lower_to_higher = [0u8; 16];
    let mut higher_to_lower = [0u8; 16];
    lower_to_higher.copy_from_slice(&key_material[..16]);
    higher_to_lower.copy_from_slice(&key_material[16..]);

    // Return keys in the order that makes sense for the caller
    if first_public < second_public {
        // first is "lower", so first_to_second = lower_to_higher
        (lower_to_higher, higher_to_lower)
    } else {
        // first is "higher", so first_to_second = higher_to_lower
        (higher_to_lower, lower_to_higher)
    }
}

#[cfg(test)]
thread_local! {
    static CACHED_KEYPAIR: std::cell::RefCell<Option<TransportKeypair>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for TransportKeypair {
    fn arbitrary(_u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Cache the keypair to avoid repeated generation
        Ok(CACHED_KEYPAIR.with(|cached| {
            let mut cached = cached.borrow_mut();
            match &*cached {
                Some(k) => k.clone(),
                None => {
                    let key = TransportKeypair::new();
                    cached.replace(key.clone());
                    key
                }
            }
        }))
    }
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for TransportPublicKey {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let keypair = TransportKeypair::arbitrary(u)?;
        Ok(keypair.public)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_generation_is_fast() {
        use std::time::Instant;

        let start = Instant::now();
        for _ in 0..100 {
            let _ = TransportKeypair::new();
        }
        let elapsed = start.elapsed();

        // X25519 key generation should be < 1ms per key on average
        // RSA-2048 takes 50-100ms per key
        assert!(
            elapsed.as_millis() < 100,
            "100 key generations took {}ms (should be < 100ms)",
            elapsed.as_millis()
        );
    }

    #[test]
    fn diffie_hellman_key_exchange() {
        let alice = TransportKeypair::new();
        let bob = TransportKeypair::new();

        // Both sides compute the same shared secret
        let alice_shared = alice.secret.diffie_hellman(&bob.public);
        let bob_shared = bob.secret.diffie_hellman(&alice.public);

        assert_eq!(alice_shared.as_bytes(), bob_shared.as_bytes());
    }

    #[test]
    fn ephemeral_key_exchange() {
        let static_keypair = TransportKeypair::new();
        let ephemeral = EphemeralKeypair::new();

        let ephemeral_public = ephemeral.public.clone();
        let shared_from_ephemeral = ephemeral.diffie_hellman(&static_keypair.public);
        let shared_from_static = static_keypair.secret.diffie_hellman(&ephemeral_public);

        assert_eq!(
            shared_from_ephemeral.as_bytes(),
            shared_from_static.as_bytes()
        );
    }

    #[test]
    fn symmetric_key_derivation() {
        let alice = TransportKeypair::new();
        let bob = TransportKeypair::new();

        let shared = alice.secret.diffie_hellman(&bob.public);

        let (a_to_b, b_to_a) = derive_symmetric_keys(
            shared.as_bytes(),
            alice.public.as_bytes(),
            bob.public.as_bytes(),
        );

        // Keys should be different
        assert_ne!(a_to_b, b_to_a);

        // Same derivation with same inputs should produce same keys
        let (a_to_b_2, b_to_a_2) = derive_symmetric_keys(
            shared.as_bytes(),
            alice.public.as_bytes(),
            bob.public.as_bytes(),
        );
        assert_eq!(a_to_b, a_to_b_2);
        assert_eq!(b_to_a, b_to_a_2);

        // Both parties should derive the same keys regardless of argument order
        // When Bob calls with (bob, alice), his "first_to_second" is bob_to_alice
        // and his "second_to_first" is alice_to_bob
        let (bob_to_alice, alice_to_bob) = derive_symmetric_keys(
            shared.as_bytes(),
            bob.public.as_bytes(),
            alice.public.as_bytes(),
        );
        // Both parties should agree on the same keys for each direction
        assert_eq!(a_to_b, alice_to_bob);
        assert_eq!(b_to_a, bob_to_alice);
    }

    #[test]
    fn public_key_size() {
        let keypair = TransportKeypair::new();
        assert_eq!(keypair.public.as_bytes().len(), 32);
    }

    #[test]
    fn serialization_roundtrip() {
        let keypair = TransportKeypair::new();
        let serialized = bincode::serialize(&keypair).unwrap();
        let deserialized: TransportKeypair = bincode::deserialize(&serialized).unwrap();

        assert_eq!(keypair.public, deserialized.public);
        assert_eq!(
            keypair.secret.0.as_bytes(),
            deserialized.secret.0.as_bytes()
        );
    }

    #[test]
    fn save_and_load_keypair() {
        let keypair = TransportKeypair::new();
        let temp_dir = std::env::temp_dir();
        let key_path = temp_dir.join("test_x25519_keypair.key");

        keypair.save(&key_path).unwrap();
        let loaded = TransportKeypair::load(&key_path).unwrap();

        assert_eq!(keypair.public, loaded.public);
        assert_eq!(keypair.secret.0.as_bytes(), loaded.secret.0.as_bytes());

        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn display_produces_unique_strings() {
        use std::collections::HashSet;

        let mut seen = HashSet::new();
        for i in 0..100 {
            let keypair = TransportKeypair::new();
            let display_str = format!("{}", keypair.public());
            assert!(
                seen.insert(display_str.clone()),
                "Duplicate display string found at key {}: {}",
                i,
                display_str
            );
        }
    }
}
