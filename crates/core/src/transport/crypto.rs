use rand::rngs::OsRng;
use rsa::{Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey};

pub(crate) struct TransportKeypair {
    public: TransportPublicKey,
    secret: TransportSecretKey,
}

impl TransportKeypair {
    pub fn new() -> Self {
        let mut rng = OsRng;
        let bits = 2048; // Key size, can be adjusted
        let priv_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
        let pub_key = RsaPublicKey::from(&priv_key);

        TransportKeypair {
            public: TransportPublicKey(pub_key),
            secret: TransportSecretKey(priv_key),
        }
    }
}

pub(crate) struct TransportPublicKey(RsaPublicKey);

impl TransportPublicKey {
    pub fn encrypt(&self, data: &[u8]) -> Vec<u8> {
        let mut rng = OsRng;
        let padding = Pkcs1v15Encrypt::default();
        self.0
            .encrypt(&mut rng, padding, data)
            .expect("failed to encrypt")
    }
}

pub(crate) struct TransportSecretKey(RsaPrivateKey);

impl TransportSecretKey {
    pub fn decrypt(&self, data: &[u8]) -> Vec<u8> {
        let padding = Pkcs1v15Encrypt::default();
        self.0.decrypt(padding, data).expect("failed to decrypt")
    }
}
