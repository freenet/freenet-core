use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};

pub(super) struct TransportKeypair {
    pub public: TransportPublicKey,
    pub secret: TransportSecretKey,
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
}

pub(super) struct TransportPublicKey(RsaPublicKey);

impl TransportPublicKey {
    pub fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        let mut rng = OsRng;
        let padding = Pkcs1v15Encrypt;
        self.0
            .encrypt(&mut rng, padding, data)
            .expect("failed to encrypt")
    }
}

pub(super) struct TransportSecretKey(RsaPrivateKey);

impl TransportSecretKey {
    pub fn decrypt(&self, data: &[u8]) -> Vec<u8> {
        let padding = Pkcs1v15Encrypt;
        self.0.decrypt(padding, data).expect("failed to decrypt")
    }
}
