use std::{
    collections::HashSet,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use chacha20poly1305::{aead::Aead, Error as EncryptionError, XChaCha20Poly1305, XNonce};
use dashmap::DashMap;
use freenet_stdlib::prelude::*;

use crate::config::Secrets;
use crate::contract::storages::Storage;

use super::RuntimeResult;

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
    ciphers: std::collections::HashMap<DelegateKey, Encryption>,
    /// In-memory index: DelegateKey -> Set of secret key hashes
    /// This is populated from ReDb on startup and kept in sync
    key_to_secret_part: Arc<DashMap<DelegateKey, HashSet<SecretKey>>>,
    /// ReDb storage for persistent index
    db: Storage,
    default_encryption: Encryption,
}

impl SecretsStore {
    pub fn new(secrets_dir: PathBuf, secrets: Secrets, db: Storage) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&secrets_dir).map_err(|err| {
            tracing::error!("error creating secrets dir: {err}");
            err
        })?;

        // Load index from ReDb
        let key_to_secret_part = Arc::new(DashMap::new());
        match db.load_all_secrets_index() {
            Ok(entries) => {
                for (delegate_key, secret_keys) in entries {
                    let secret_set: HashSet<SecretKey> = secret_keys.into_iter().collect();
                    key_to_secret_part.insert(delegate_key, secret_set);
                }
                tracing::debug!(
                    "Loaded {} secrets index entries from ReDb",
                    key_to_secret_part.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load secrets index from ReDb: {e}");
            }
        }

        // Migrate from legacy KEY_DATA file if it exists
        let key_file = secrets_dir.join("KEY_DATA");
        if key_file.exists() {
            if let Err(e) = Self::migrate_from_legacy(&key_file, &db, &key_to_secret_part) {
                tracing::warn!("Failed to migrate legacy KEY_DATA: {e}");
            }
        }

        Ok(Self {
            base_path: secrets_dir,
            ciphers: std::collections::HashMap::new(),
            key_to_secret_part,
            db,
            default_encryption: Encryption {
                cipher: secrets.cipher(),
                nonce: secrets.nonce(),
            },
            secrets,
        })
    }

    /// Migrate data from the legacy KEY_DATA file to ReDb.
    /// After successful migration, renames the file to KEY_DATA.migrated.
    fn migrate_from_legacy(
        key_file: &PathBuf,
        db: &Storage,
        key_to_secret_part: &DashMap<DelegateKey, HashSet<SecretKey>>,
    ) -> RuntimeResult<()> {
        use super::store::StoreFsManagement;

        tracing::info!("Migrating secrets index from legacy KEY_DATA to ReDb");

        // The legacy format stores concatenated secret keys
        struct ConcatenatedSecretKeys(Vec<u8>);

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

        // Use a temporary DashMap for the legacy loader
        let mut legacy_container: Arc<DashMap<DelegateKey, (u64, HashSet<SecretKey>)>> =
            Arc::new(DashMap::new());

        // Load from legacy file format
        struct LegacyLoader;
        impl super::store::StoreFsManagement for LegacyLoader {
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
                        let (offset, secret_hashes) = delegate.get_mut();
                        *offset = new_offset;
                        secret_hashes.extend(split_secrets);
                    }
                    dashmap::mapref::entry::Entry::Vacant(delegate) => {
                        delegate.insert((new_offset, split_secrets));
                    }
                }
            }

            fn clear_container(container: &mut Self::MemContainer) {
                container.clear();
            }
        }

        LegacyLoader::load_from_file(key_file, &mut legacy_container)?;

        let count = legacy_container.len();
        tracing::info!("Found {count} entries in legacy KEY_DATA file");

        // Migrate each entry to ReDb
        let mut migrated = 0;
        for entry in legacy_container.iter() {
            let delegate_key = entry.key();
            let secret_keys: Vec<[u8; 32]> = entry.value().1.iter().copied().collect();

            // Store in ReDb
            if let Err(e) = db.store_secrets_index(delegate_key, &secret_keys) {
                tracing::warn!("Failed to migrate secrets index entry: {e}");
                continue;
            }

            // Update in-memory map
            key_to_secret_part.insert(delegate_key.clone(), entry.value().1.clone());
            migrated += 1;
        }

        tracing::info!("Migrated {migrated}/{count} secrets index entries to ReDb");

        // Rename the legacy file to mark it as migrated
        let migrated_path = key_file.with_extension("migrated");
        if let Err(e) = std::fs::rename(key_file, &migrated_path) {
            tracing::warn!("Failed to rename KEY_DATA to .migrated: {e}");
        } else {
            tracing::info!("Renamed legacy KEY_DATA to {migrated_path:?}");
        }

        Ok(())
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

        // CRITICAL ORDER: Write file first, then update index.
        // This ensures get_secret() can find the file even if we crash between
        // operations. If we update index first and crash before file write,
        // the index would point to a non-existent file.

        // Step 1: Write secret to disk first
        fs::create_dir_all(&delegate_path)?;
        tracing::debug!("storing secret `{key}` at {secret_file_path:?}");
        let mut file = File::create(&secret_file_path)?;
        file.write_all(&ciphertext)?;
        file.sync_all()?; // Ensure durability before updating index

        // Step 2: Update index in ReDb and in-memory
        let mut current_secrets: Vec<[u8; 32]> = self
            .key_to_secret_part
            .get(delegate)
            .map(|entry| entry.value().iter().copied().collect())
            .unwrap_or_default();

        // Add the new secret key if not already present
        if !current_secrets.contains(&secret_key) {
            current_secrets.push(secret_key);
        }

        // Store in ReDb
        self.db
            .store_secrets_index(delegate, &current_secrets)
            .map_err(|e| anyhow::anyhow!("Failed to store secrets index: {e}"))?;

        // Update in-memory map
        let secret_set: HashSet<SecretKey> = current_secrets.into_iter().collect();
        self.key_to_secret_part.insert(delegate.clone(), secret_set);

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

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));

        let cipher = XChaCha20Poly1305::new(&XChaCha20Poly1305::generate_key(&mut OsRng));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let secret_id = SecretsId::new(vec![0, 1, 2]);
        let text = vec![0, 1, 2];

        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        store.store_secret(delegate.key(), &secret_id, text)?;
        let f = store.get_secret(delegate.key(), &secret_id);

        assert!(f.is_ok());
        // Clean up after test
        let _ = std::fs::remove_dir_all(&secrets_dir);
        Ok(())
    }
}
