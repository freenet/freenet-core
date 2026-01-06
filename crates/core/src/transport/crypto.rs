use std::path::Path;

use chacha20poly1305::{aead::Aead, ChaCha20Poly1305, KeyInit, Nonce};
use serde::{Deserialize, Serialize};
use x25519_dalek::{PublicKey, StaticSecret};
use zeroize::ZeroizeOnDrop;

use crate::config::GlobalRng;

/// Size of X25519 public key
pub const X25519_PUBLIC_KEY_SIZE: usize = 32;

/// Size of ChaCha20Poly1305 authentication tag
const CHACHA_TAG_SIZE: usize = 16;

/// Packet type discriminator for intro packets (X25519 encrypted)
pub const PACKET_TYPE_INTRO: u8 = 0x01;

/// Packet type discriminator for symmetric packets (AES-GCM encrypted)
pub const PACKET_TYPE_SYMMETRIC: u8 = 0x02;

/// Size of packet type discriminator
pub const PACKET_TYPE_SIZE: usize = 1;

/// Size of encrypted intro packet:
/// packet_type (1) + ephemeral_public (32) + encrypted_data + tag (16)
#[allow(dead_code)] // Used in tests and as public API
pub const fn intro_packet_size(plaintext_len: usize) -> usize {
    PACKET_TYPE_SIZE + X25519_PUBLIC_KEY_SIZE + plaintext_len + CHACHA_TAG_SIZE
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransportKeypair {
    pub(super) public: TransportPublicKey,
    pub(super) secret: TransportSecretKey,
}

impl TransportKeypair {
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let path = path.as_ref();
        // Use atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        let mut file = File::create(&temp_path)?;
        // Save as hex-encoded private key bytes
        let hex = hex::encode(self.secret.0.as_bytes());
        file.write_all(hex.as_bytes())?;
        file.sync_all()?; // Ensure data is flushed to disk
        std::fs::rename(&temp_path, path)?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> std::io::Result<Self> {
        use std::fs::File;
        use std::io::Read;

        let mut file = File::open(path)?;
        let mut hex_str = String::new();
        file.read_to_string(&mut hex_str)?;

        let bytes = hex::decode(hex_str.trim())
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

        Ok(TransportKeypair {
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
        // Generate random bytes for the secret key using GlobalRng
        // This allows deterministic key generation in simulation mode
        let mut secret_bytes = [0u8; 32];
        GlobalRng::fill_bytes(&mut secret_bytes);
        let secret = StaticSecret::from(secret_bytes);
        let public = PublicKey::from(&secret);
        TransportKeypair {
            public: TransportPublicKey(public),
            secret: TransportSecretKey(secret),
        }
    }

    pub fn public(&self) -> &TransportPublicKey {
        &self.public
    }

    #[cfg(test)]
    pub(crate) fn secret(&self) -> &TransportSecretKey {
        &self.secret
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TransportPublicKey(PublicKey);

impl PartialOrd for TransportPublicKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransportPublicKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl TransportPublicKey {
    /// Encrypt data using static-ephemeral X25519 key exchange.
    ///
    /// Returns: packet_type (1 byte) || ephemeral_public (32 bytes) || ciphertext || tag (16 bytes)
    ///
    /// The encryption uses:
    /// 1. Generate ephemeral X25519 keypair
    /// 2. Compute shared_secret = ECDH(ephemeral_private, recipient_static_public)
    /// 3. Use shared_secret as ChaCha20Poly1305 key with zero nonce (safe because key is unique per message)
    /// 4. Return packet_type || ephemeral_public || AEAD_encrypt(data)
    pub fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        // Generate ephemeral keypair using GlobalRng for deterministic simulation
        let mut ephemeral_bytes = [0u8; 32];
        GlobalRng::fill_bytes(&mut ephemeral_bytes);
        let ephemeral_secret = StaticSecret::from(ephemeral_bytes);
        let ephemeral_public = PublicKey::from(&ephemeral_secret);

        // Compute shared secret
        let shared_secret = ephemeral_secret.diffie_hellman(&self.0);

        // Use shared secret as ChaCha20Poly1305 key
        let cipher = ChaCha20Poly1305::new(shared_secret.as_bytes().into());

        // Zero nonce is safe because we use a fresh ephemeral key for each encryption
        let nonce = Nonce::default();

        let ciphertext = cipher.encrypt(&nonce, data).expect("encryption failure");

        // Prepend packet type and ephemeral public key
        let mut result =
            Vec::with_capacity(PACKET_TYPE_SIZE + X25519_PUBLIC_KEY_SIZE + ciphertext.len());
        result.push(PACKET_TYPE_INTRO);
        result.extend_from_slice(ephemeral_public.as_bytes());
        result.extend_from_slice(&ciphertext);
        result
    }

    /// Save the public key to a file in hex format.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use std::fs::File;
        use std::io::Write;

        let path = path.as_ref();
        // Use atomic write: write to temp file, then rename
        let temp_path = path.with_extension("tmp");
        let mut file = File::create(&temp_path)?;
        let hex = hex::encode(self.0.as_bytes());
        file.write_all(hex.as_bytes())?;
        file.sync_all()?; // Ensure data is flushed to disk
        std::fs::rename(&temp_path, path)?;
        Ok(())
    }

    /// Get the raw bytes of the public key
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Create from raw bytes
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        TransportPublicKey(PublicKey::from(bytes))
    }
}

// Custom Serialize/Deserialize for TransportPublicKey
impl Serialize for TransportPublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for TransportPublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};

        struct PublicKeyVisitor;

        impl<'de> Visitor<'de> for PublicKeyVisitor {
            type Value = TransportPublicKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("32 bytes for X25519 public key")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if v.len() != 32 {
                    return Err(E::custom(format!("expected 32 bytes, got {}", v.len())));
                }
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(v);
                Ok(TransportPublicKey(PublicKey::from(bytes)))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut bytes = [0u8; 32];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| Error::invalid_length(i, &self))?;
                }
                Ok(TransportPublicKey(PublicKey::from(bytes)))
            }
        }

        deserializer.deserialize_bytes(PublicKeyVisitor)
    }
}

impl std::fmt::Debug for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use first 12 bytes of public key encoded in base58 for display
        write!(
            f,
            "{}",
            bs58::encode(&self.0.as_bytes()[..12]).into_string()
        )
    }
}

/// Secret key wrapper that zeroizes memory on drop.
///
/// Note: x25519_dalek's StaticSecret implements Zeroize internally when the
/// `zeroize` feature is enabled (which it is via our dependency).
#[derive(Clone, ZeroizeOnDrop)]
pub(crate) struct TransportSecretKey(#[zeroize(skip)] StaticSecret);

impl TransportSecretKey {
    /// Decrypt data that was encrypted with our public key using static-ephemeral X25519.
    ///
    /// Input format: packet_type (1 byte) || ephemeral_public (32 bytes) || ciphertext || tag (16 bytes)
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, DecryptionError> {
        if data.len() < PACKET_TYPE_SIZE + X25519_PUBLIC_KEY_SIZE + CHACHA_TAG_SIZE {
            return Err(DecryptionError::InvalidLength);
        }

        // Check packet type
        if data[0] != PACKET_TYPE_INTRO {
            return Err(DecryptionError::InvalidPacketType);
        }

        // Extract ephemeral public key (skip packet type byte)
        let mut ephemeral_bytes = [0u8; 32];
        ephemeral_bytes
            .copy_from_slice(&data[PACKET_TYPE_SIZE..PACKET_TYPE_SIZE + X25519_PUBLIC_KEY_SIZE]);
        let ephemeral_public = PublicKey::from(ephemeral_bytes);

        // Compute shared secret
        let shared_secret = self.0.diffie_hellman(&ephemeral_public);

        // Use shared secret as ChaCha20Poly1305 key
        let cipher = ChaCha20Poly1305::new(shared_secret.as_bytes().into());

        // Zero nonce (same as encryption)
        let nonce = Nonce::default();

        // Decrypt the ciphertext
        let ciphertext = &data[PACKET_TYPE_SIZE + X25519_PUBLIC_KEY_SIZE..];
        cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| DecryptionError::DecryptionFailed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecryptionError {
    InvalidLength,
    DecryptionFailed,
    InvalidPacketType,
}

impl std::fmt::Display for DecryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecryptionError::InvalidLength => write!(f, "invalid ciphertext length"),
            DecryptionError::DecryptionFailed => write!(f, "decryption failed"),
            DecryptionError::InvalidPacketType => write!(f, "invalid packet type"),
        }
    }
}

impl std::error::Error for DecryptionError {}

// Implement equality for TransportSecretKey by comparing derived public keys
impl PartialEq for TransportSecretKey {
    fn eq(&self, other: &Self) -> bool {
        PublicKey::from(&self.0) == PublicKey::from(&other.0)
    }
}

impl Eq for TransportSecretKey {}

impl std::fmt::Debug for TransportSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransportSecretKey")
            .field("public", &PublicKey::from(&self.0))
            .finish()
    }
}

// Custom Serialize/Deserialize for TransportSecretKey
impl Serialize for TransportSecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for TransportSecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{Error, Visitor};

        struct SecretKeyVisitor;

        impl<'de> Visitor<'de> for SecretKeyVisitor {
            type Value = TransportSecretKey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("32 bytes for X25519 secret key")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                if v.len() != 32 {
                    return Err(E::custom(format!("expected 32 bytes, got {}", v.len())));
                }
                let mut bytes = [0u8; 32];
                bytes.copy_from_slice(v);
                Ok(TransportSecretKey(StaticSecret::from(bytes)))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut bytes = [0u8; 32];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| Error::invalid_length(i, &self))?;
                }
                Ok(TransportSecretKey(StaticSecret::from(bytes)))
            }
        }

        deserializer.deserialize_bytes(SecretKeyVisitor)
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
#[test]
fn key_sizes_and_decryption() {
    let pair = TransportKeypair::new();
    let mut sym_key_bytes = [0u8; 16];
    crate::config::GlobalRng::fill_bytes(&mut sym_key_bytes);
    let encrypted: Vec<u8> = pair.public.encrypt(&sym_key_bytes);

    // X25519 intro packet: 1 (packet type) + 32 (ephemeral pub) + 16 (data) + 16 (tag) = 65 bytes
    assert_eq!(encrypted.len(), intro_packet_size(16));
    assert!(
        encrypted.len() <= super::packet_data::MAX_PACKET_SIZE,
        "packet size is too big"
    );

    let bytes = pair.secret.decrypt(&encrypted).unwrap();
    assert_eq!(bytes, sym_key_bytes.as_slice());
}

#[cfg(test)]
mod display_uniqueness_tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn display_produces_unique_strings_for_100_keys() {
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

#[cfg(test)]
mod crypto_tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_default_creates_valid_keypair() {
        let keypair = TransportKeypair::default();
        // Verify we can encrypt/decrypt with the default keypair
        let data = b"test data";
        let encrypted = keypair.public.encrypt(data);
        let decrypted = keypair.secret.decrypt(&encrypted).unwrap();
        assert_eq!(data.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_keypair_save_and_load() {
        let keypair = TransportKeypair::new();
        let temp_dir = std::env::temp_dir();
        let key_path = temp_dir.join("test_x25519_keypair.key");

        // Save the keypair
        keypair.save(&key_path).unwrap();

        // Verify file exists and contains hex data
        let mut contents = String::new();
        std::fs::File::open(&key_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        assert_eq!(contents.len(), 64); // 32 bytes as hex = 64 chars

        // Load the key back
        let loaded_keypair = TransportKeypair::load(&key_path).unwrap();

        // Test that encryption/decryption works across save/load
        let data = b"round trip test";
        let encrypted = keypair.public.encrypt(data);
        let decrypted = loaded_keypair.secret.decrypt(&encrypted).unwrap();
        assert_eq!(data.as_slice(), decrypted.as_slice());

        // Cleanup
        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn test_public_key_save() {
        let keypair = TransportKeypair::new();
        let temp_dir = std::env::temp_dir();
        let key_path = temp_dir.join("test_x25519_pubkey.key");

        // Save the public key
        keypair.public.save(&key_path).unwrap();

        // Verify file exists and contains hex data
        let mut contents = String::new();
        std::fs::File::open(&key_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        assert_eq!(contents.len(), 64); // 32 bytes as hex = 64 chars

        // Cleanup
        std::fs::remove_file(&key_path).ok();
    }

    #[test]
    fn test_debug_impl_uses_display() {
        let keypair = TransportKeypair::new();
        let display_str = format!("{}", keypair.public);
        let debug_str = format!("{:?}", keypair.public);

        // Debug should use the Display implementation
        assert_eq!(display_str, debug_str);
    }

    #[test]
    fn test_secret_accessor() {
        let keypair = TransportKeypair::new();
        let secret = keypair.secret();

        // Verify the secret can decrypt what the public key encrypts
        let data = b"secret accessor test";
        let encrypted = keypair.public.encrypt(data);
        let decrypted = secret.decrypt(&encrypted).unwrap();
        assert_eq!(data.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_intro_packet_size() {
        // Protocol version (8) + symmetric key (16) = 24 bytes plaintext
        // Encrypted: 1 (packet type) + 32 (ephemeral pub) + 24 (data) + 16 (tag) = 73 bytes
        assert_eq!(intro_packet_size(24), 73);

        // Compare to old RSA: 256 bytes
        // New X25519: 73 bytes (3.5x smaller!)
    }

    #[test]
    fn test_public_key_serialization() {
        let keypair = TransportKeypair::new();

        // Serialize
        let serialized = bincode::serialize(&keypair.public).unwrap();

        // Deserialize
        let deserialized: TransportPublicKey = bincode::deserialize(&serialized).unwrap();

        // Should be equal
        assert_eq!(keypair.public, deserialized);
    }

    #[test]
    fn test_decryption_error_invalid_length() {
        let keypair = TransportKeypair::new();

        // Too short - less than ephemeral public key + tag
        let short_data = [0u8; 40];
        let result = keypair.secret.decrypt(&short_data);
        assert_eq!(result.unwrap_err(), DecryptionError::InvalidLength);
    }

    #[test]
    fn test_decryption_error_wrong_key() {
        let keypair1 = TransportKeypair::new();
        let keypair2 = TransportKeypair::new();

        let data = b"test data";
        let encrypted = keypair1.public.encrypt(data);

        // Try to decrypt with wrong key
        let result = keypair2.secret.decrypt(&encrypted);
        assert_eq!(result.unwrap_err(), DecryptionError::DecryptionFailed);
    }

    #[test]
    fn test_decryption_error_tampered_ciphertext() {
        let keypair = TransportKeypair::new();

        let data = b"test data";
        let mut encrypted = keypair.public.encrypt(data);

        // Tamper with ciphertext
        encrypted[40] ^= 0xFF;

        let result = keypair.secret.decrypt(&encrypted);
        assert_eq!(result.unwrap_err(), DecryptionError::DecryptionFailed);
    }
}

#[cfg(test)]
mod packet_type_tests {
    use super::*;

    #[test]
    fn test_intro_packet_has_correct_type_byte() {
        let keypair = TransportKeypair::new();
        let data = b"test data for intro packet";
        let encrypted = keypair.public.encrypt(data);

        assert_eq!(
            encrypted[0], PACKET_TYPE_INTRO,
            "First byte of intro packet should be PACKET_TYPE_INTRO (0x01)"
        );
        assert_eq!(encrypted.len(), intro_packet_size(data.len()));
    }

    #[test]
    fn test_intro_packet_decryption_validates_type() {
        let keypair = TransportKeypair::new();
        let data = b"test";
        let mut encrypted = keypair.public.encrypt(data);

        // Corrupt the packet type byte
        encrypted[0] = PACKET_TYPE_SYMMETRIC; // Wrong type

        let result = keypair.secret.decrypt(&encrypted);
        assert!(
            matches!(result, Err(DecryptionError::InvalidPacketType)),
            "Decryption should fail with InvalidPacketType when packet type is wrong"
        );
    }

    #[test]
    fn test_intro_packet_decryption_rejects_invalid_type() {
        let keypair = TransportKeypair::new();
        let data = b"test";
        let mut encrypted = keypair.public.encrypt(data);

        // Set invalid packet type
        encrypted[0] = 0xFF;

        let result = keypair.secret.decrypt(&encrypted);
        assert!(
            matches!(result, Err(DecryptionError::InvalidPacketType)),
            "Decryption should fail with InvalidPacketType for unknown type 0xFF"
        );
    }

    #[test]
    fn test_intro_packet_type_preserved_after_encryption() {
        let keypair = TransportKeypair::new();

        // Test with various data sizes
        for size in [0, 16, 24, 100, 500] {
            let data = vec![0xAB; size];
            let encrypted = keypair.public.encrypt(&data);

            assert_eq!(
                encrypted[0], PACKET_TYPE_INTRO,
                "Packet type should be PACKET_TYPE_INTRO for data size {}",
                size
            );

            // Verify successful decryption
            let decrypted = keypair.secret.decrypt(&encrypted).unwrap();
            assert_eq!(
                decrypted, data,
                "Decrypted data should match for size {}",
                size
            );
        }
    }
}
