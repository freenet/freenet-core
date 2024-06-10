use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use chacha20poly1305::{aead::Aead, Error as EncryptionError, XChaCha20Poly1305, XNonce};
use dashmap::DashMap;
use freenet_stdlib::prelude::*;

use crate::config::Secrets;

use super::{
    store::{SafeWriter, StoreFsManagement},
    RuntimeResult,
};

type SecretKey = [u8; 32];

#[derive(Debug, thiserror::Error)]
pub enum SecretStoreError {
    #[error("encryption error: {0}")]
    Encryption(EncryptionError),
    #[error("{0}")]
    IO(#[from] std::io::Error),
    #[error("missing cipher")]
    MissingCipher,
    #[error("missing secret: {0}")]
    MissingSecret(SecretsId),
}

#[derive(Clone)]
struct Encryption {
    cipher: XChaCha20Poly1305,
    nonce: XNonce,
}

pub struct SecretsStore {
    base_path: PathBuf,
    #[allow(unused)]
    secrets: Secrets,
    ciphers: HashMap<DelegateKey, Encryption>,
    key_to_secret_part: Arc<DashMap<DelegateKey, (u64, HashSet<SecretKey>)>>,
    index_file: SafeWriter<Self>,
    key_file: PathBuf,
    default_encryption: Encryption,
}

pub(super) struct ConcatenatedSecretKeys(Vec<u8>);

impl AsRef<[u8]> for ConcatenatedSecretKeys {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<'x> TryFrom<&'x [u8]> for ConcatenatedSecretKeys {
    type Error = std::io::Error;

    fn try_from(value: &'x [u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.to_vec()))
    }
}

impl StoreFsManagement for SecretsStore {
    type MemContainer = Arc<DashMap<DelegateKey, (u64, HashSet<SecretKey>)>>;
    type Key = DelegateKey;
    type Value = ConcatenatedSecretKeys;

    fn insert_in_container(
        container: &mut Self::MemContainer,
        (key, new_offset): (Self::Key, u64),
        value: Self::Value,
    ) {
        let split_secrets = value
            .0
            .chunks(32)
            .map(|chunk| {
                let mut fixed = [0u8; 32];
                fixed.copy_from_slice(chunk);
                fixed
            })
            .collect::<HashSet<_>>();
        match container.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(mut delegate) => {
                // if an update was to happen from an other process new value would be loaded here
                let (offset, secret_hashes) = delegate.get_mut();
                *offset = new_offset;
                secret_hashes.extend(split_secrets);
            }
            dashmap::mapref::entry::Entry::Vacant(delegate) => {
                delegate.insert((new_offset, split_secrets));
            }
        }
    }
}

impl SecretsStore {
    pub fn new(secrets_dir: PathBuf, secrets: Secrets) -> RuntimeResult<Self> {
        let mut key_to_secret_part = Arc::new(DashMap::new());
        let key_file = secrets_dir.join("KEY_DATA");
        if !key_file.exists() {
            std::fs::create_dir_all(&secrets_dir).map_err(|err| {
                tracing::error!("error creating delegate dir: {err}");
                err
            })?;
            File::create(secrets_dir.join("KEY_DATA"))?;
        } else {
            Self::load_from_file(&key_file, &mut key_to_secret_part)?;
        }
        Self::watch_changes(key_to_secret_part.clone(), &key_file)?;

        let index_file = SafeWriter::new(&key_file, false)?;
        Ok(Self {
            base_path: secrets_dir,
            ciphers: HashMap::new(),
            key_to_secret_part,
            index_file,
            key_file,
            default_encryption: Encryption {
                cipher: secrets.cipher(),
                nonce: secrets.nonce(),
            },
            secrets,
        })
    }

    pub fn register_delegate(
        &mut self,
        delegate: DelegateKey,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> Result<(), SecretStoreError> {
        if nonce != self.default_encryption.nonce {
            let encryption = Encryption { cipher, nonce };
            self.ciphers.insert(delegate, encryption);
        }
        Ok(())
    }

    pub fn store_secret(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
        plaintext: Vec<u8>,
    ) -> RuntimeResult<()> {
        let delegate_path = self.base_path.join(delegate.encode());
        let secret_file_path = delegate_path.join(key.encode());
        let secret_key = *key.hash();
        let encryption = self
            .ciphers
            .get(delegate)
            .unwrap_or(&self.default_encryption);

        let ciphertext = encryption
            .cipher
            .encrypt(&encryption.nonce, plaintext.as_ref())
            .map_err(|err| {
                if encryption.nonce == self.default_encryption.nonce {
                    SecretStoreError::MissingCipher
                } else {
                    SecretStoreError::Encryption(err)
                }
            })?;

        // Update index
        let hashes = self.key_to_secret_part.entry(delegate.clone());
        match hashes {
            dashmap::mapref::entry::Entry::Occupied(mut v) => {
                let current_version_offset = v.get().0;
                let secret_hashes = &mut v.get_mut().1;
                let mut value = vec![];
                for hash in &*secret_hashes {
                    value.extend_from_slice(hash);
                }
                // first mark the old entry (if it exists) as removed
                Self::remove(&self.key_file, current_version_offset)?;
                let new_offset = Self::insert(
                    &mut self.index_file,
                    delegate.clone(),
                    &ConcatenatedSecretKeys(value),
                )?;
                secret_hashes.insert(secret_key);
                v.get_mut().0 = new_offset;
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let offset = Self::insert(
                    &mut self.index_file,
                    delegate.clone(),
                    &ConcatenatedSecretKeys(secret_key.to_vec()),
                )?;
                v.insert((offset, HashSet::from([secret_key])));
            }
        }

        fs::create_dir_all(&delegate_path)?;
        tracing::debug!("storing secret `{key}` at {secret_file_path:?}");
        let mut file = File::create(secret_file_path)?;
        file.write_all(&ciphertext)?;
        Ok(())
    }

    pub fn remove_secret(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
    ) -> Result<(), SecretStoreError> {
        let secret_path = self.base_path.join(delegate.encode()).join(key.encode());
        match fs::remove_file(secret_path) {
            Ok(_) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn get_secret(
        &self,
        delegate: &DelegateKey,
        key: &SecretsId,
    ) -> Result<Vec<u8>, SecretStoreError> {
        let secret_path = self.base_path.join(delegate.encode()).join(key.encode());
        let encryption = self
            .ciphers
            .get(delegate)
            .unwrap_or(&self.default_encryption);

        let ciphertext =
            fs::read(secret_path).map_err(|_| SecretStoreError::MissingSecret(key.clone()))?;
        let plaintext = encryption
            .cipher
            .decrypt(&encryption.nonce, ciphertext.as_ref())
            .map_err(|err| {
                if encryption.nonce == self.default_encryption.nonce {
                    SecretStoreError::MissingCipher
                } else {
                    SecretStoreError::Encryption(err)
                }
            })?;
        Ok(plaintext)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use aes_gcm::KeyInit;
    use chacha20poly1305::aead::{AeadCore, OsRng};

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let secrets_dir = std::env::temp_dir()
            .join("freenet-test")
            .join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let mut store = SecretsStore::new(secrets_dir, Default::default())?;

        let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));

        let cipher = XChaCha20Poly1305::new(&XChaCha20Poly1305::generate_key(&mut OsRng));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let secret_id = SecretsId::new(vec![0, 1, 2]);
        let text = vec![0, 1, 2];

        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        store.store_secret(delegate.key(), &secret_id, text)?;
        let f = store.get_secret(delegate.key(), &secret_id);

        assert!(f.is_ok());
        Ok(())
    }
}
