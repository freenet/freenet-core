use std::{collections::HashMap, fs, path::PathBuf};

use chacha20poly1305::{aead::Aead, Error as EncryptionError, XChaCha20Poly1305, XNonce};
use locutus_stdlib::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum SecretStoreError {
    #[error("encryption error: {0}")]
    Encryption(EncryptionError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("missing cipher")]
    MissingCipher,
}

#[derive(Clone)]
struct Encryption {
    cipher: XChaCha20Poly1305,
    nonce: XNonce,
}

#[derive(Default)]
pub struct SecretsStore {
    base_path: PathBuf,
    ciphers: HashMap<ComponentKey, Encryption>,
}

impl SecretsStore {
    pub fn register_component(
        &mut self,
        component: ComponentKey,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> Result<(), SecretStoreError> {
        // FIXME: store/initialize the cyphers from disc
        let encryption = Encryption { cipher, nonce };
        self.ciphers.insert(component, encryption);
        Ok(())
    }

    pub fn store_secret(
        &mut self,
        component: &ComponentKey,
        key: &SecretsId,
        plaintext: Vec<u8>,
    ) -> Result<(), SecretStoreError> {
        let secret_path = self.base_path.join(component.encode()).join(key.encode());
        let encryption = self
            .ciphers
            .get(component)
            .ok_or(SecretStoreError::MissingCipher)?;
        let ciphertext = encryption
            .cipher
            .encrypt(&encryption.nonce, plaintext.as_ref())
            .map_err(SecretStoreError::Encryption)?;
        fs::write(secret_path, ciphertext)?;
        Ok(())
    }

    pub fn remove_secret(
        &mut self,
        component: &ComponentKey,
        key: &SecretsId,
    ) -> Result<(), SecretStoreError> {
        let secret_path = self.base_path.join(component.encode()).join(key.encode());
        match fs::remove_file(secret_path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn get_secret(
        &self,
        component: &ComponentKey,
        key: &SecretsId,
    ) -> Result<Vec<u8>, SecretStoreError> {
        let secret_path = self.base_path.join(component.encode()).join(key.encode());
        let encryption = self
            .ciphers
            .get(component)
            .ok_or(SecretStoreError::MissingCipher)?;
        let ciphertext = fs::read(secret_path)?;
        let plaintext = encryption
            .cipher
            .decrypt(&encryption.nonce, ciphertext.as_ref())
            .map_err(SecretStoreError::Encryption)?;
        Ok(plaintext)
    }
}
