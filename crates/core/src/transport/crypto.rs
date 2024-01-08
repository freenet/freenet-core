use aes::cipher::consts::U16;
use aes::cipher::generic_array::GenericArray;
use ecies::utils::generate_keypair;
use ecies::{decrypt, encrypt, SecpError};
use rand::rngs::OsRng;
use rand::RngCore;

pub(crate) struct TransportKeypair {
    public: TransportPublicKey,
    secret: TransportSecretKey,
}

impl TransportKeypair {
    pub fn new() -> Self {
        let (sk, pk) = generate_keypair();
        TransportKeypair {
            public: TransportPublicKey(pk.serialize()),
            secret: TransportSecretKey(sk.serialize()),
        }
    }
}

pub(crate) struct TransportPublicKey([u8; 65]);

impl TransportPublicKey {
    pub fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>, SecpError> {
        encrypt(&self.0, data)
    }
}

pub(crate) struct TransportSecretKey([u8; 32]);

impl TransportSecretKey {
    pub fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>, SecpError> {
        decrypt(&self.0, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_keypair_encrypt_decrypt() {
        let keypair = TransportKeypair::new();
        let data = b"hello world";
        let encrypted = keypair.public.encrypt(data).unwrap();
        let decrypted = keypair.secret.decrypt(&encrypted).unwrap();
        assert_eq!(data, decrypted.as_slice());
    }
}
