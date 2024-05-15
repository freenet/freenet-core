use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        let pub_key = RsaPublicKey::from(&priv_key);

        TransportKeypair {
            public: TransportPublicKey(pub_key),
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
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug, Hash)]
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TransportSecretKey(RsaPrivateKey);

impl TransportSecretKey {
    pub fn decrypt(&self, data: &[u8]) -> rsa::Result<Vec<u8>> {
        let padding = Pkcs1v15Encrypt;
        self.0.decrypt(padding, data)
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
