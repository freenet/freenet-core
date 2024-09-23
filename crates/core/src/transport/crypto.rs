use rand::rngs::OsRng;
use rsa::{pkcs8, Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransportKeypair {
    pub(super) public: TransportPublicKey,
    pub(super) secret: TransportSecretKey,
}

impl Default for TransportKeypair {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportKeypair {
    pub fn new() -> Self {
        let mut rng = OsRng;
        // Key size, can be adjusted
        const BITS: usize = 2048;
        let priv_key = RsaPrivateKey::new(&mut rng, BITS).expect("failed to generate a key");
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
}

impl std::fmt::Debug for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(&self, f)
    }
}

impl std::fmt::Display for TransportPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use pkcs8::EncodePublicKey;

        let encoded = self.0.to_public_key_der().map_err(|_| std::fmt::Error)?;
        if encoded.as_bytes().len() >= 16 {
            let bytes = encoded.as_bytes();
            let first_six = &bytes[..6];
            let last_six = &bytes[bytes.len() - 6..];
            let to_encode = [first_six, last_six].concat();
            write!(f, "{}", bs58::encode(to_encode).into_string())
        } else {
            write!(f, "{}", bs58::encode(encoded.as_bytes()).into_string())
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

    pub fn to_pkcs8_pem(&self) -> Result<Vec<u8>, pkcs8::Error> {
        use pkcs8::EncodePrivateKey;

        #[cfg(unix)]
        let line_endings = pkcs8::LineEnding::LF;

        #[cfg(windows)]
        let line_endings = pkcs8::LineEnding::CRLF;

        self.0
            .to_pkcs8_pem(line_endings)
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
