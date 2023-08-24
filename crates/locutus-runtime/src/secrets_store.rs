use blake3::traits::digest::generic_array::GenericArray;
use chacha20poly1305::{aead::Aead, Error as EncryptionError, KeyInit, XChaCha20Poly1305, XNonce};
use dashmap::DashMap;
use locutus_stdlib::client_api::DelegateRequest;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    iter::FromIterator,
    path::PathBuf,
    sync::Arc,
};

use crate::store::{StoreEntriesContainer, StoreFsManagement};
use crate::RuntimeResult;
use locutus_stdlib::prelude::*;

type SecretKey = [u8; 32];

#[derive(Serialize, Deserialize, Default)]
struct KeyToEncryptionMap(Vec<(DelegateKey, Vec<SecretKey>)>);

impl StoreEntriesContainer for KeyToEncryptionMap {
    type MemContainer = Arc<DashMap<DelegateKey, Vec<SecretKey>>>;
    type Key = DelegateKey;
    type Value = Vec<SecretKey>;

    fn update(self, container: &mut Self::MemContainer) {
        for (k, v) in self.0 {
            container.insert(k, v);
        }
    }

    fn replace(container: &Self::MemContainer) -> Self {
        KeyToEncryptionMap::from(&**container)
    }

    fn insert(container: &mut Self::MemContainer, key: Self::Key, value: Self::Value) {
        if let Some(element) = container.get(&key.clone()) {
            let mut secrets = element.value().clone();
            secrets.extend(value);
            container.insert(key, secrets);
        } else {
            container.insert(key, value);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretStoreError {
    #[error("encryption error: {0}")]
    Encryption(EncryptionError),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("missing cipher")]
    MissingCipher,
    #[error("missing secret: {0}")]
    MissingSecret(SecretsId),
}

impl From<&DashMap<DelegateKey, Vec<SecretKey>>> for KeyToEncryptionMap {
    fn from(vals: &DashMap<DelegateKey, Vec<SecretKey>>) -> Self {
        let mut map = vec![];
        for r in vals.iter() {
            map.push((r.key().clone(), r.value().clone()));
        }
        Self(map)
    }
}

#[derive(Clone)]
struct Encryption {
    cipher: XChaCha20Poly1305,
    nonce: XNonce,
}

#[derive(Default)]
pub struct SecretsStore {
    base_path: PathBuf,
    ciphers: HashMap<DelegateKey, Encryption>,
    key_to_secret_part: Arc<DashMap<DelegateKey, Vec<SecretKey>>>,
}

static LOCK_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();
static KEY_FILE_PATH: once_cell::sync::OnceCell<PathBuf> = once_cell::sync::OnceCell::new();

impl StoreFsManagement<KeyToEncryptionMap> for SecretsStore {}

static DEFAULT_CIPHER: Lazy<XChaCha20Poly1305> = Lazy::new(|| {
    let arr = GenericArray::from_slice(&DelegateRequest::DEFAULT_CIPHER);
    XChaCha20Poly1305::new(arr)
});

static DEFAULT_NONCE: Lazy<XNonce> =
    Lazy::new(|| GenericArray::from_slice(&DelegateRequest::DEFAULT_NONCE).to_owned());

static DEFAULT_ENCRYPTION: Lazy<Encryption> = Lazy::new(|| Encryption {
    cipher: (*DEFAULT_CIPHER).clone(),
    nonce: *DEFAULT_NONCE,
});

impl SecretsStore {
    pub fn new(secrets_dir: PathBuf) -> RuntimeResult<Self> {
        let key_to_secret_part;
        let _ = LOCK_FILE_PATH.try_insert(secrets_dir.join("__LOCK"));
        let key_file = match KEY_FILE_PATH
            .try_insert(secrets_dir.join("KEY_DATA"))
            .map_err(|(e, _)| e)
        {
            Ok(f) => f,
            Err(f) => f,
        };
        if !key_file.exists() {
            std::fs::create_dir_all(&secrets_dir).map_err(|err| {
                tracing::error!("error creating delegate dir: {err}");
                err
            })?;
            key_to_secret_part = Arc::new(DashMap::new());
            File::create(secrets_dir.join("KEY_DATA"))?;
        } else {
            let map = Self::load_from_file(
                KEY_FILE_PATH.get().unwrap().as_path(),
                LOCK_FILE_PATH.get().unwrap().as_path(),
            )?;
            key_to_secret_part = Arc::new(DashMap::from_iter(map.0));
        }
        Self::watch_changes(
            key_to_secret_part.clone(),
            KEY_FILE_PATH.get().unwrap().as_path(),
            LOCK_FILE_PATH.get().unwrap().as_path(),
        )?;
        Ok(Self {
            base_path: secrets_dir,
            ciphers: HashMap::new(),
            key_to_secret_part,
        })
    }

    pub fn register_delegate(
        &mut self,
        delegate: DelegateKey,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> Result<(), SecretStoreError> {
        // FIXME: store/initialize the cyphers from disc
        if nonce != *DEFAULT_NONCE {
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
        let encryption = self.ciphers.get(delegate).unwrap_or(&*DEFAULT_ENCRYPTION);

        let ciphertext = encryption
            .cipher
            .encrypt(&encryption.nonce, plaintext.as_ref())
            .map_err(|err| {
                if encryption.nonce == *DEFAULT_NONCE {
                    SecretStoreError::MissingCipher
                } else {
                    SecretStoreError::Encryption(err)
                }
            })?;

        self.key_to_secret_part
            .insert(delegate.clone(), vec![secret_key]);

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
        let encryption = self.ciphers.get(delegate).unwrap_or(&*DEFAULT_ENCRYPTION);

        let ciphertext =
            fs::read(secret_path).map_err(|_| SecretStoreError::MissingSecret(key.clone()))?;
        let plaintext = encryption
            .cipher
            .decrypt(&encryption.nonce, ciphertext.as_ref())
            .map_err(|err| {
                if encryption.nonce == *DEFAULT_NONCE {
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
    use chacha20poly1305::aead::{AeadCore, KeyInit, OsRng};

    #[test]
    fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let secrets_dir = std::env::temp_dir()
            .join("locutus-test")
            .join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let mut store = SecretsStore::new(secrets_dir)?;

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
