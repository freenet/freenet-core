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
