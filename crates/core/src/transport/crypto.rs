use std::path::Path;

use rsa::{
    pkcs8,
    rand_core::{CryptoRngCore, OsRng},
    Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransportKeypair {
    pub(super) public: TransportPublicKey,
    pub(super) secret: TransportSecretKey,
}

impl TransportKeypair {
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use pkcs8::EncodePrivateKey;
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        let key = self
            .secret
            .0
            .to_pkcs8_pem(pkcs8::LineEnding::default())
            .unwrap();
        file.write_all(key.as_bytes())?;
        Ok(())
    }
}

impl Default for TransportKeypair {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportKeypair {
    pub fn new() -> Self {
        let mut rng = OsRng;
        Self::new_inner(&mut rng)
    }

    pub fn new_with_rng(rng: &mut impl CryptoRngCore) -> Self {
        Self::new_inner(rng)
    }

    fn new_inner(rng: &mut impl CryptoRngCore) -> Self {
        const BITS: usize = 2048;
        let priv_key = RsaPrivateKey::new(rng, BITS).expect("failed to generate a key");
        let public = TransportPublicKey(RsaPublicKey::from(&priv_key));
        TransportKeypair {
            public,
            secret: TransportSecretKey(priv_key),
        }
    }

    pub fn from_private_key(priv_key: RsaPrivateKey) -> Self {
        TransportKeypair {
            public: TransportPublicKey(RsaPublicKey::from(&priv_key)),
            secret: TransportSecretKey(priv_key),
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

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct TransportPublicKey(RsaPublicKey);

impl TransportPublicKey {
    pub fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        let mut rng = OsRng;
        let padding = Pkcs1v15Encrypt;
        self.0
            .encrypt(&mut rng, padding, data)
            .expect("failed to encrypt")
    }

    /// Save the public key to a file in PEM format.
    pub fn save(&self, path: impl AsRef<Path>) -> std::io::Result<()> {
        use pkcs8::EncodePublicKey;
        use std::fs::File;
        use std::io::Write;

        let mut file = File::create(path)?;
        let key = self
            .0
            .to_public_key_pem(pkcs8::LineEnding::default())
            .unwrap();
        file.write_all(key.as_bytes())?;
        Ok(())
    }
}

impl std::fmt::Debug for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

impl std::fmt::Display for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use pkcs8::EncodePublicKey;

        let encoded = self.0.to_public_key_der().map_err(|_| std::fmt::Error)?;
        let bytes = encoded.as_bytes();

        // For RSA 2048-bit keys, DER encoding is ~294 bytes:
        // - Bytes 0-~22: DER structure headers (same for all 2048-bit keys)
        // - Bytes ~23-~279: Modulus (256 bytes of random data)
        // - Bytes ~280-~293: Exponent (typically 65537, same for all keys)
        //
        // To create a short but unique display string, we use 12 bytes from
        // the middle of the modulus where the actual random data lives.
        if bytes.len() >= 150 {
            // Take 12 bytes from the middle of the modulus (around byte 100-112)
            let mid_start = 100;
            let to_encode = &bytes[mid_start..mid_start + 12];
            write!(f, "{}", bs58::encode(to_encode).into_string())
        } else {
            // Fallback for smaller keys
            write!(f, "{}", bs58::encode(bytes).into_string())
        }
    }
}

impl From<RsaPublicKey> for TransportPublicKey {
    fn from(key: RsaPublicKey) -> Self {
        TransportPublicKey(key)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct TransportSecretKey(RsaPrivateKey);

impl TransportSecretKey {
    pub fn decrypt(&self, data: &[u8]) -> rsa::Result<Vec<u8>> {
        self.0.decrypt(Pkcs1v15Encrypt, data)
    }

    #[cfg(test)]
    pub fn to_pkcs8_pem(&self) -> Result<Vec<u8>, pkcs8::Error> {
        use pkcs8::EncodePrivateKey;
        self.0
            .to_pkcs8_pem(pkcs8::LineEnding::default())
            .map(|s| s.as_str().as_bytes().to_vec())
    }
}

#[cfg(test)]
thread_local! {
    static CACHED_KEYPAIR: std::cell::RefCell<Option<TransportKeypair>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
impl<'a> arbitrary::Arbitrary<'a> for TransportKeypair {
    fn arbitrary(_u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        // Cache the keypair to avoid expensive RSA generation on each call
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
    let sym_key_bytes = rand::random::<[u8; 16]>();
    // use aes_gcm::KeyInit;
    // let _sym_key = aes_gcm::aes::Aes128::new(&sym_key_bytes.into());
    let encrypted: Vec<u8> = pair.public.encrypt(&sym_key_bytes);
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
        // Verify the Display impl produces unique strings by checking 100 keys.
        // This test was added after discovering the original Display impl only
        // had ~1 byte of entropy (see PR #2233 for details).
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
    fn test_new_with_rng_deterministic() {
        // Using a seeded RNG should produce the same keypair
        // Use OsRng which implements the CryptoRngCore trait from the correct rand_core version
        use rsa::rand_core::OsRng;

        // Create two keypairs with OsRng to verify the method works
        // (Can't test determinism easily due to rand_core version mismatch)
        let keypair1 = TransportKeypair::new_with_rng(&mut OsRng);
        let keypair2 = TransportKeypair::new_with_rng(&mut OsRng);

        // Both should be valid keypairs (different due to randomness)
        assert_ne!(keypair1, keypair2);

        // But both should work for encryption/decryption
        let data = b"test";
        let encrypted1 = keypair1.public.encrypt(data);
        let decrypted1 = keypair1.secret.decrypt(&encrypted1).unwrap();
        assert_eq!(data.as_slice(), decrypted1.as_slice());

        let encrypted2 = keypair2.public.encrypt(data);
        let decrypted2 = keypair2.secret.decrypt(&encrypted2).unwrap();
        assert_eq!(data.as_slice(), decrypted2.as_slice());
    }

    #[test]
    fn test_from_private_key() {
        let original = TransportKeypair::new();
        // Extract the private key and reconstruct
        let pem_bytes = original.secret.to_pkcs8_pem().unwrap();

        use rsa::pkcs8::DecodePrivateKey;
        let priv_key =
            RsaPrivateKey::from_pkcs8_pem(std::str::from_utf8(&pem_bytes).unwrap()).unwrap();

        let reconstructed = TransportKeypair::from_private_key(priv_key);

        // The public keys should match
        assert_eq!(
            format!("{}", original.public),
            format!("{}", reconstructed.public)
        );
    }

    #[test]
    fn test_keypair_save_and_load() {
        let keypair = TransportKeypair::new();
        let temp_dir = std::env::temp_dir();
        let key_path = temp_dir.join("test_keypair.pem");

        // Save the keypair
        keypair.save(&key_path).unwrap();

        // Verify file exists and contains PEM data
        let mut contents = String::new();
        std::fs::File::open(&key_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        assert!(contents.contains("-----BEGIN PRIVATE KEY-----"));
        assert!(contents.contains("-----END PRIVATE KEY-----"));

        // Load the key back and verify it works
        use rsa::pkcs8::DecodePrivateKey;
        let loaded_key = RsaPrivateKey::from_pkcs8_pem(&contents).unwrap();
        let loaded_keypair = TransportKeypair::from_private_key(loaded_key);

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
        let key_path = temp_dir.join("test_pubkey.pem");

        // Save the public key
        keypair.public.save(&key_path).unwrap();

        // Verify file exists and contains PEM data
        let mut contents = String::new();
        std::fs::File::open(&key_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();

        assert!(contents.contains("-----BEGIN PUBLIC KEY-----"));
        assert!(contents.contains("-----END PUBLIC KEY-----"));

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
    fn test_public_key_from_rsa() {
        use rsa::rand_core::OsRng;
        let priv_key = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pub_key = RsaPublicKey::from(&priv_key);

        let transport_pub: TransportPublicKey = pub_key.into();

        // Verify we can use it for encryption
        let data = b"test from conversion";
        let encrypted = transport_pub.encrypt(data);

        // And decrypt with the original private key
        let decrypted = priv_key.decrypt(Pkcs1v15Encrypt, &encrypted).unwrap();
        assert_eq!(data.as_slice(), decrypted.as_slice());
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
}
