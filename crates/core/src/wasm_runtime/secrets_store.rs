use std::{
    collections::HashSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    // Wall-clock SystemTime (not the project-wide TimeSource trait) is the
    // correct abstraction here: thin_snapshots compares "now" against
    // file-name epoch_ms values that must remain meaningful across process
    // restarts. TimeSource returns simulation-relative Duration, which has
    // no stable epoch and can't be compared to persisted timestamps.
    time::SystemTime,
};

use chacha20poly1305::{Error as EncryptionError, XChaCha20Poly1305, XNonce, aead::Aead};
use dashmap::DashMap;
use freenet_stdlib::prelude::*;

use crate::config::Secrets;
use crate::contract::storages::Storage;

use super::RuntimeResult;
use super::secret_snapshots::{
    RetentionPolicy, SnapshotMetadata, list_snapshots, next_snapshot_path, snapshot_dir_for,
    thin_snapshots,
};

/// Environment variable that disables snapshot-on-write for delegate secrets.
/// Snapshots are on by default; this is only for ops who explicitly want the
/// previous behavior (e.g. extreme disk-pressure scenarios).
const DISABLE_SNAPSHOTS_ENV: &str = "FREENET_DISABLE_SECRET_SNAPSHOTS";

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
    /// No snapshot file matched the requested `timestamp_ms` in the
    /// secret's `.snapshots/{secret_id}/` directory. Distinct from
    /// `IO`/`MissingSecret` so the CLI (issue #4036) can surface a
    /// "no such snapshot, try `list_snapshots` first" message instead
    /// of a generic filesystem error.
    #[error("no snapshot for secret {key} at timestamp_ms {timestamp_ms}")]
    SnapshotNotFound { key: SecretsId, timestamp_ms: u64 },
}

#[derive(Clone)]
struct Encryption {
    cipher: XChaCha20Poly1305,
    nonce: XNonce,
}

/// Storage layer for delegate secrets and their snapshot history.
///
/// **Synchronization.** This type is NOT internally synchronized.
/// `store_secret` and `remove_secret` take `&mut self` so the borrow
/// checker forbids concurrent writes against the same instance, and
/// `get_secret` taking `&self` cannot run concurrently with a write
/// either. If a future caller wraps this type in interior mutability
/// (e.g. `Arc<Mutex<SecretsStore>>`), they must hold the lock across
/// the snapshot+rename+index sequence in `store_secret`; otherwise two
/// concurrent writes for the same `(delegate, secret_id)` could race
/// on the snapshot path or the active-file rename window.
pub struct SecretsStore {
    base_path: PathBuf,
    #[allow(unused)]
    secrets: Secrets,
    ciphers: std::collections::HashMap<DelegateKey, Encryption>,
    /// In-memory index: DelegateKey -> Set of secret key hashes.
    /// Populated from ReDb on startup and kept in sync with it; never
    /// updated unless the corresponding ReDb write succeeded.
    key_to_secret_part: Arc<DashMap<DelegateKey, HashSet<SecretKey>>>,
    /// ReDb storage for persistent index
    db: Storage,
    default_encryption: Encryption,
    /// Snapshot retention policy. Snapshots are taken before any overwrite
    /// of an existing secret so a buggy delegate or accidental write cannot
    /// silently destroy prior values.
    retention: RetentionPolicy,
    snapshots_enabled: bool,
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
            retention: RetentionPolicy::default(),
            snapshots_enabled: std::env::var_os(DISABLE_SNAPSHOTS_ENV).is_none(),
        })
    }

    /// Override the retention policy. Intended for tests that want to
    /// exercise edge cases without waiting real wall-clock time.
    #[cfg(test)]
    pub(crate) fn set_retention_policy(&mut self, policy: RetentionPolicy) {
        self.retention = policy;
    }

    /// Override the snapshots-enabled flag at runtime. Intended for tests
    /// that want to exercise the disabled path without mutating the
    /// process-wide environment.
    #[cfg(test)]
    pub(crate) fn set_snapshots_enabled(&mut self, enabled: bool) {
        self.snapshots_enabled = enabled;
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

    /// Remove a delegate's cipher entry. Used to rollback `register_delegate`
    /// when a subsequent operation (e.g., storing the delegate) fails.
    pub fn remove_delegate_cipher(&mut self, delegate: &DelegateKey) {
        self.ciphers.remove(delegate);
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

        fs::create_dir_all(&delegate_path)?;

        // CRITICAL ORDER: hard-link prior value into snapshot history, write
        // new ciphertext to a tmp path, fsync, then atomically rename
        // tmp → active. This way:
        //   - the active path is never absent: a crash leaves either the old
        //     or new ciphertext (atomic rename guarantees no half-state),
        //   - the snapshot points at the OLD inode, which is unaffected by
        //     the new write because the new write goes through a fresh
        //     inode and only `rename` makes it visible at the active path,
        //   - update index AFTER the active rename so a crash between rename
        //     and index-update still gives `get_secret` the new value.
        if self.snapshots_enabled
            && secret_file_path.exists()
            && let Err(e) = self.snapshot_prior_value(&delegate_path, key, &secret_file_path)
        {
            // Snapshotting is best-effort. A failure here must not block the
            // primary write — the user's data still gets through. Log so
            // disk problems surface in monitoring.
            tracing::warn!(
                "failed to snapshot prior secret value for delegate {}: {e}",
                delegate.encode()
            );
        }

        tracing::debug!("storing secret `{key}` at {secret_file_path:?}");
        // Write to a sibling tmp path so the active path's inode never has
        // a half-written state. We pick a fixed suffix (rather than a
        // random one) because `&mut self` makes concurrent in-process
        // store_secret calls impossible; a stale `.tmp` from a prior
        // crashed run is overwritten harmlessly here.
        let tmp_path = secret_file_path.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&ciphertext)?;
            file.sync_all()?;
        }
        // Atomic on POSIX (and on Rust >=1.56 Windows: MoveFileExW with
        // MOVEFILE_REPLACE_EXISTING). If this rename fails, the active
        // path still holds the old value (or is empty if it never existed)
        // and the new ciphertext sits in the tmp file for forensics.
        if let Err(err) = fs::rename(&tmp_path, &secret_file_path) {
            // Best-effort cleanup of the tmp file so we don't leave debris.
            // A failure here is purely cosmetic; log and continue with the
            // primary error from rename().
            if let Err(rm_err) = fs::remove_file(&tmp_path) {
                tracing::debug!(
                    "failed to clean up tmp file {tmp_path:?} after rename failure: {rm_err}"
                );
            }
            return Err(err.into());
        }

        // Update index in ReDb and in-memory only after the active path has
        // the new value durably committed.
        let mut current_secrets: Vec<[u8; 32]> = self
            .key_to_secret_part
            .get(delegate)
            .map(|entry| entry.value().iter().copied().collect())
            .unwrap_or_default();

        if !current_secrets.contains(&secret_key) {
            current_secrets.push(secret_key);
        }

        self.db
            .store_secrets_index(delegate, &current_secrets)
            .map_err(|e| anyhow::anyhow!("Failed to store secrets index: {e}"))?;

        let secret_set: HashSet<SecretKey> = current_secrets.into_iter().collect();
        self.key_to_secret_part.insert(delegate.clone(), secret_set);

        // Best-effort thin of the snapshot history. Failures here only mean
        // we keep more snapshots than the policy targets, which is harmless
        // and self-correcting on the next write.
        if self.snapshots_enabled {
            let snap_dir = snapshot_dir_for(&delegate_path, key);
            if snap_dir.exists() {
                thin_snapshots(&snap_dir, &self.retention, SystemTime::now());
            }
        }

        Ok(())
    }

    /// Capture the existing active secret file as a snapshot. Uses
    /// hard-link so the active inode and snapshot inode coexist; the
    /// subsequent `rename(tmp → active)` updates the active dir-entry to
    /// a different inode without touching the snapshot.
    ///
    /// On filesystems that do not support hard links (FAT, some network
    /// mounts), falls back to `fs::copy`. The active file is unchanged
    /// either way, so callers that crash mid-snapshot just lose the
    /// snapshot, never the live value.
    fn snapshot_prior_value(
        &self,
        delegate_path: &Path,
        key: &SecretsId,
        secret_file_path: &Path,
    ) -> std::io::Result<()> {
        let snap_dir = snapshot_dir_for(delegate_path, key);
        fs::create_dir_all(&snap_dir)?;
        let snap_path = next_snapshot_path(&snap_dir)?;
        match fs::hard_link(secret_file_path, &snap_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => {
                // Hard-link unsupported (FAT, cross-device, etc.). Copy
                // is slower but always works. The active file is not
                // mutated in this code path so the copy can't tear.
                fs::copy(secret_file_path, &snap_path).map(|_| ())
            }
        }
    }

    pub fn remove_secret(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
    ) -> Result<(), SecretStoreError> {
        let delegate_path = self.base_path.join(delegate.encode());
        let secret_path = delegate_path.join(key.encode());
        let snap_dir = snapshot_dir_for(&delegate_path, key);

        // Best-effort delete of the snapshot history. Removing a secret means
        // the user no longer wants any version of that value retained.
        if snap_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&snap_dir) {
                tracing::warn!(
                    "failed to remove snapshots for {} / {key}: {e}",
                    delegate.encode()
                );
            }
        }

        match fs::remove_file(&secret_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }

        // Update persistent index FIRST. The previous version of this
        // method updated the in-memory map unconditionally and only
        // logged ReDb failures, which let a transient DB error
        // resurrect the deleted entry on the next restart (because
        // `new()` rebuilds the in-memory map from ReDb). Mirroring
        // `store_secret`, we treat persistence failure as fatal here
        // and only mutate the in-memory map after ReDb commits.
        let secret_key = *key.hash();
        let mut current: Vec<SecretKey> = self
            .key_to_secret_part
            .get(delegate)
            .map(|e| e.value().iter().copied().collect())
            .unwrap_or_default();
        current.retain(|k| k != &secret_key);

        self.db
            .store_secrets_index(delegate, &current)
            .map_err(|e| std::io::Error::other(format!("Failed to update secrets index: {e}")))?;

        let secret_set: HashSet<SecretKey> = current.into_iter().collect();
        self.key_to_secret_part.insert(delegate.clone(), secret_set);

        Ok(())
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

    /// Enumerate the snapshot history for a given `(delegate, secret_id)`
    /// pair, oldest-first. Returns an empty vector if the secret was never
    /// overwritten (no snapshot directory exists). Does not decrypt;
    /// callers that want the plaintext can `restore_snapshot` and then
    /// `get_secret`.
    pub fn list_snapshots(
        &self,
        delegate: &DelegateKey,
        key: &SecretsId,
    ) -> Result<Vec<SnapshotMetadata>, SecretStoreError> {
        let delegate_path = self.base_path.join(delegate.encode());
        let snap_dir = snapshot_dir_for(&delegate_path, key);
        Ok(list_snapshots(&snap_dir)?)
    }

    /// Promote a previously-captured snapshot back to the active path.
    ///
    /// Mirrors the durability discipline of `store_secret`: the current
    /// active value (if any) is snapshotted first (so restore is itself
    /// reversible), then the chosen snapshot is copied to a `.tmp` file,
    /// fsynced, and atomically renamed onto the active path. The ReDb
    /// index and in-memory cache are updated last so a crash between the
    /// rename and the index update still leaves the active value
    /// readable on the next `get_secret`.
    ///
    /// If multiple snapshots share `timestamp_ms` (collision suffixes
    /// from same-millisecond writes), the unsuffixed file wins; absent
    /// that, the lowest-numbered suffix wins. To restore a specific
    /// collision-suffix entry, callers can use [`list_snapshots`] and
    /// pick the entry's `path` directly (a future API may take
    /// `SnapshotMetadata` directly).
    ///
    /// Does NOT require the delegate's cipher to be registered — restore
    /// is byte-level copy, not re-encryption. The restored ciphertext
    /// remains decryptable by whatever cipher wrote it.
    ///
    /// # Errors
    /// - `SnapshotNotFound` if no snapshot matches `timestamp_ms`
    /// - `IO` for filesystem errors during the copy / rename / fsync
    pub fn restore_snapshot(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
        timestamp_ms: u64,
    ) -> Result<(), SecretStoreError> {
        let delegate_path = self.base_path.join(delegate.encode());
        let secret_file_path = delegate_path.join(key.encode());
        let snap_dir = snapshot_dir_for(&delegate_path, key);

        // Find the requested snapshot. Disambiguation rule: unsuffixed
        // file wins, then lowest-numbered suffix. `list_snapshots`
        // already sorts by (timestamp_ms, suffix.unwrap_or(0)), and
        // `None` < `Some(_)` is encoded via that 0-default — fine because
        // collision suffixes start at 0, so the unsuffixed entry sorts
        // alongside `.0`; we explicitly prefer unsuffixed below.
        let entries = list_snapshots(&snap_dir)?;
        let chosen = entries
            .iter()
            .filter(|m| m.timestamp_ms == timestamp_ms)
            .min_by_key(|m| match m.suffix {
                None => (0u32, 0u32),
                Some(s) => (1, s),
            })
            .ok_or_else(|| SecretStoreError::SnapshotNotFound {
                key: key.clone(),
                timestamp_ms,
            })?;
        let chosen_path = chosen.path.clone();
        // Snapshot the value currently at the active path so the restore
        // operation is itself reversible. Mirrors store_secret's
        // best-effort logging — the primary operation (restore) must
        // not fail just because we couldn't preserve the value being
        // replaced.
        if self.snapshots_enabled
            && secret_file_path.exists()
            && let Err(e) = self.snapshot_prior_value(&delegate_path, key, &secret_file_path)
        {
            tracing::warn!(
                "failed to snapshot active value before restore for delegate {}: {e}",
                delegate.encode()
            );
        }

        // Read snapshot ciphertext, write through a sibling tmp file with
        // an atomic rename so the active path never tears. `&mut self`
        // makes concurrent in-process restore impossible; a stale `.tmp`
        // from a prior crashed run gets overwritten harmlessly here.
        let ciphertext = fs::read(&chosen_path)?;
        fs::create_dir_all(&delegate_path)?;
        let tmp_path = secret_file_path.with_extension("tmp");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&ciphertext)?;
            file.sync_all()?;
        }
        if let Err(err) = fs::rename(&tmp_path, &secret_file_path) {
            if let Err(rm_err) = fs::remove_file(&tmp_path) {
                tracing::debug!(
                    "failed to clean up tmp file {tmp_path:?} after rename failure: {rm_err}"
                );
            }
            return Err(err.into());
        }

        // Index update: only needed if the entry was previously removed
        // (e.g. user called `remove_secret` then realized they wanted a
        // value back). In the common case the secret is already in the
        // index and the write below is idempotent.
        let secret_key = *key.hash();
        let mut current_secrets: Vec<[u8; 32]> = self
            .key_to_secret_part
            .get(delegate)
            .map(|entry| entry.value().iter().copied().collect())
            .unwrap_or_default();
        if !current_secrets.contains(&secret_key) {
            current_secrets.push(secret_key);
            self.db
                .store_secrets_index(delegate, &current_secrets)
                .map_err(|e| {
                    std::io::Error::other(format!("Failed to update secrets index: {e}"))
                })?;
            let secret_set: HashSet<SecretKey> = current_secrets.into_iter().collect();
            self.key_to_secret_part.insert(delegate.clone(), secret_set);
        }

        // Best-effort thin: we may have just doubled the snapshot count
        // (active-value snapshot above). Failures here only mean we keep
        // more snapshots than the policy targets, self-correcting on the
        // next write.
        if self.snapshots_enabled && snap_dir.exists() {
            thin_snapshots(&snap_dir, &self.retention, SystemTime::now());
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wasm_runtime::secret_snapshots::{RetentionBucket, RetentionPolicy};
    use aes_gcm::KeyInit;
    use chacha20poly1305::aead::{AeadCore, OsRng};
    use std::time::Duration;

    async fn create_test_db(path: &std::path::Path) -> Storage {
        Storage::new(path).await.expect("failed to create test db")
    }

    fn fresh_cipher() -> (XChaCha20Poly1305, XNonce) {
        let cipher = XChaCha20Poly1305::new(&XChaCha20Poly1305::generate_key(&mut OsRng));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        (cipher, nonce)
    }

    #[tokio::test]
    async fn store_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![0, 1, 2].into(), &vec![].into()));

        let (cipher, nonce) = fresh_cipher();
        let secret_id = SecretsId::new(vec![0, 1, 2]);
        let text = vec![0, 1, 2];

        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        store.store_secret(delegate.key(), &secret_id, text)?;
        let f = store.get_secret(delegate.key(), &secret_id);

        assert!(f.is_ok());
        // Clean up after test
        let _cleanup = std::fs::remove_dir_all(&secrets_dir);
        Ok(())
    }

    /// Regression: writing a secret twice should leave a snapshot of the
    /// prior value behind. The active path holds the new ciphertext; the
    /// snapshot directory holds a decryptable copy of the prior ciphertext.
    #[tokio::test]
    async fn second_write_snapshots_prior_value() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![1].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![42]);

        store.store_secret(delegate.key(), &secret_id, b"v1".to_vec())?;
        // Sleep 2ms to guarantee a distinct epoch-millis stamp on the snapshot.
        // Sleep enough to guarantee a distinct epoch-millis stamp on the
        // snapshot even on virtualized CI runners with coarse clocks.
        // A test that lands two writes in the same millisecond would
        // exercise the collision-suffix branch instead, which has its own
        // test in the secret_snapshots module.
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"v2".to_vec())?;

        // Active value is the latest write.
        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"v2".to_vec()
        );

        // Exactly one snapshot exists, holding the prior ciphertext.
        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        let entries: Vec<_> = std::fs::read_dir(&snap_dir)?.flatten().collect();
        assert_eq!(
            entries.len(),
            1,
            "expected exactly one snapshot, got {entries:?}"
        );

        // The snapshot is decryptable by the same cipher and yields the prior
        // plaintext, proving snapshots aren't just opaque junk on disk.
        let ciphertext = std::fs::read(entries[0].path())?;
        let encryption = store
            .ciphers
            .get(delegate.key())
            .expect("cipher registered");
        let plaintext = encryption
            .cipher
            .decrypt(&encryption.nonce, ciphertext.as_ref())
            .expect("snapshot ciphertext should decrypt with the registered cipher");
        assert_eq!(plaintext, b"v1".to_vec());
        Ok(())
    }

    /// Burst writes within a single retention slot collapse to a small
    /// number of snapshots — the policy must bound disk usage.
    #[tokio::test]
    async fn burst_writes_are_thinned() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        // Tight policy: keep 3 most-recent, plus one per minute (max 1
        // bucket) — i.e. up to 4 snapshots total.
        store.set_retention_policy(RetentionPolicy {
            keep_last: 3,
            buckets: vec![RetentionBucket {
                interval: Duration::from_secs(60),
                max_count: 1,
            }],
            max_age: None,
        });

        let delegate = Delegate::from((&vec![2].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![7]);

        for i in 0u32..50 {
            store.store_secret(delegate.key(), &secret_id, i.to_le_bytes().to_vec())?;
            // Force distinct epoch-millis stamps so the snapshot files don't
            // collide and the count actually reflects the policy.
            // Sleep enough to guarantee a distinct epoch-millis stamp on the
            // snapshot even on virtualized CI runners with coarse clocks.
            // A test that lands two writes in the same millisecond would
            // exercise the collision-suffix branch instead, which has its own
            // test in the secret_snapshots module.
            std::thread::sleep(Duration::from_millis(5));
        }

        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        let count = std::fs::read_dir(&snap_dir)?.count();
        assert!(
            count <= 4,
            "tight policy should bound snapshot count to <=4; got {count}"
        );
        // We should have at least keep_last - 1 = 2 (after 50 writes there's
        // always strictly more than `keep_last` snapshots in flight).
        assert!(count >= 2, "expected snapshots to be retained; got {count}");
        Ok(())
    }

    /// Regression for the previous remove_secret index leak: after removal,
    /// the ReDb secrets index and the in-memory map must no longer claim
    /// the secret exists, and the snapshot history must be gone.
    #[tokio::test]
    async fn remove_secret_clears_index_and_snapshots() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![3].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![9]);

        store.store_secret(delegate.key(), &secret_id, b"a".to_vec())?;
        // Sleep enough to guarantee a distinct epoch-millis stamp on the
        // snapshot even on virtualized CI runners with coarse clocks.
        // A test that lands two writes in the same millisecond would
        // exercise the collision-suffix branch instead, which has its own
        // test in the secret_snapshots module.
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"b".to_vec())?;

        // Pre-conditions: index has the key, snapshot dir is populated.
        let secret_hash = *secret_id.hash();
        let pre_index = store
            .db
            .get_secrets_index(delegate.key())
            .expect("index lookup")
            .unwrap_or_default();
        assert!(
            pre_index.contains(&secret_hash),
            "index should contain the secret before removal"
        );
        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        assert!(
            snap_dir.exists(),
            "snapshot dir should exist before removal"
        );

        store.remove_secret(delegate.key(), &secret_id)?;

        // Post-conditions: index entry gone in BOTH ReDb and the in-memory
        // map, file gone, snapshot dir gone.
        let post_index = store
            .db
            .get_secrets_index(delegate.key())
            .expect("index lookup")
            .unwrap_or_default();
        assert!(
            !post_index.contains(&secret_hash),
            "ReDb index still contains removed secret hash"
        );
        let in_mem = store
            .key_to_secret_part
            .get(delegate.key())
            .map(|e| e.value().contains(&secret_hash))
            .unwrap_or(false);
        assert!(!in_mem, "in-memory map still contains removed secret hash");

        assert!(
            !snap_dir.exists(),
            "snapshot dir should be deleted with the secret"
        );
        assert!(matches!(
            store.get_secret(delegate.key(), &secret_id),
            Err(SecretStoreError::MissingSecret(_))
        ));
        Ok(())
    }

    /// Removing a never-written secret must be a no-op success and must
    /// leave the index in a sane state (empty, not containing a phantom).
    #[tokio::test]
    async fn remove_nonexistent_secret_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![4].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![11]);

        store.remove_secret(delegate.key(), &secret_id)?;

        let post_index = store
            .db
            .get_secrets_index(delegate.key())
            .expect("index lookup")
            .unwrap_or_default();
        assert!(post_index.is_empty());
        Ok(())
    }

    /// Disabling snapshots via `set_snapshots_enabled(false)` must skip
    /// both the snapshot-on-write and the post-write thinning paths so
    /// no `.snapshots/` directory is ever created.
    #[tokio::test]
    async fn disabled_flag_suppresses_snapshots() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        store.set_snapshots_enabled(false);

        let delegate = Delegate::from((&vec![6].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![14]);

        store.store_secret(delegate.key(), &secret_id, b"a".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"b".to_vec())?;

        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        assert!(
            !snap_dir.exists(),
            "no snapshot dir should be created when snapshots are disabled"
        );
        // Active path still holds the latest write.
        assert_eq!(store.get_secret(delegate.key(), &secret_id)?, b"b".to_vec());
        Ok(())
    }

    /// Two delegates using the same `SecretsId` must keep their snapshot
    /// histories disjoint — pin that the snapshot dir is rooted at the
    /// per-delegate path, not at `base_path`.
    #[tokio::test]
    async fn delegates_have_disjoint_snapshot_histories() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate_a = Delegate::from((&vec![10].into(), &vec![].into()));
        let delegate_b = Delegate::from((&vec![11].into(), &vec![].into()));
        let (ca, na) = fresh_cipher();
        let (cb, nb) = fresh_cipher();
        store.register_delegate(delegate_a.key().clone(), ca, na)?;
        store.register_delegate(delegate_b.key().clone(), cb, nb)?;
        let shared_id = SecretsId::new(vec![99]);

        // Two writes per delegate against the same SecretsId.
        for value in [&b"a1"[..], &b"a2"[..]] {
            store.store_secret(delegate_a.key(), &shared_id, value.to_vec())?;
            std::thread::sleep(Duration::from_millis(5));
        }
        for value in [&b"b1"[..], &b"b2"[..]] {
            store.store_secret(delegate_b.key(), &shared_id, value.to_vec())?;
            std::thread::sleep(Duration::from_millis(5));
        }

        let snap_a = secrets_dir
            .join(delegate_a.key().encode())
            .join(".snapshots")
            .join(shared_id.encode());
        let snap_b = secrets_dir
            .join(delegate_b.key().encode())
            .join(".snapshots")
            .join(shared_id.encode());
        assert!(
            snap_a != snap_b,
            "snapshot dirs must differ across delegates"
        );
        assert!(snap_a.exists() && snap_b.exists());

        // Each delegate has exactly one snapshot (one prior overwrite each).
        assert_eq!(std::fs::read_dir(&snap_a)?.count(), 1);
        assert_eq!(std::fs::read_dir(&snap_b)?.count(), 1);

        // And get_secret on each delegate returns its own most-recent value.
        assert_eq!(
            store.get_secret(delegate_a.key(), &shared_id)?,
            b"a2".to_vec()
        );
        assert_eq!(
            store.get_secret(delegate_b.key(), &shared_id)?,
            b"b2".to_vec()
        );
        Ok(())
    }

    /// First write of a brand-new secret must NOT create a snapshot dir
    /// (there's no prior value to preserve).
    #[tokio::test]
    async fn first_write_creates_no_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![5].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![13]);

        store.store_secret(delegate.key(), &secret_id, b"first".to_vec())?;

        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        assert!(
            !snap_dir.exists(),
            "no snapshot should exist after a single write"
        );
        Ok(())
    }

    /// list_snapshots on a never-written secret returns an empty Vec (not an
    /// error). This mirrors `next_snapshot_path` + the missing-dir branch of
    /// `list_snapshots` in secret_snapshots.rs.
    #[tokio::test]
    async fn list_snapshots_on_unwritten_secret_is_empty() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![20].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![21]);

        let snaps = store.list_snapshots(delegate.key(), &secret_id)?;
        assert!(snaps.is_empty(), "no writes → no snapshots");
        Ok(())
    }

    /// list_snapshots returns each snapshot, oldest-first, with the right
    /// timestamp_ms. After two overwrites we should see two snapshots
    /// (the v1 cipher → snapshot from the v2 write, and the v2 cipher →
    /// snapshot from the v3 write).
    #[tokio::test]
    async fn list_snapshots_returns_history() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![30].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![31]);

        store.store_secret(delegate.key(), &secret_id, b"v1".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"v2".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"v3".to_vec())?;

        let snaps = store.list_snapshots(delegate.key(), &secret_id)?;
        assert_eq!(snaps.len(), 2, "expected two snapshots after 3 writes");
        assert!(
            snaps[0].timestamp_ms <= snaps[1].timestamp_ms,
            "must be oldest-first"
        );
        Ok(())
    }

    /// Happy-path restore: after writing v1 and v2, restoring v1's snapshot
    /// must put v1 back at the active path. The snapshot taken before the
    /// restore preserves v2 so the operation is reversible.
    #[tokio::test]
    async fn restore_snapshot_replaces_active_value() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![40].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![41]);

        store.store_secret(delegate.key(), &secret_id, b"v1".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"v2".to_vec())?;

        // Confirm active = v2.
        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"v2".to_vec()
        );

        // Pick the (only) snapshot — it holds the v1 ciphertext.
        let snaps = store.list_snapshots(delegate.key(), &secret_id)?;
        assert_eq!(snaps.len(), 1);
        let v1_ts = snaps[0].timestamp_ms;

        store.restore_snapshot(delegate.key(), &secret_id, v1_ts)?;

        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"v1".to_vec(),
            "restore must put the v1 plaintext back"
        );

        // After restore there must be a snapshot of v2 (the value that was
        // replaced) so the operation is reversible.
        let snaps_after = store.list_snapshots(delegate.key(), &secret_id)?;
        assert!(
            !snaps_after.is_empty(),
            "restore must snapshot the prior active value; got {} snapshots",
            snaps_after.len()
        );
        Ok(())
    }

    /// Restoring an unknown timestamp must return SnapshotNotFound, not a
    /// generic IO error, so the CLI can give a precise message.
    #[tokio::test]
    async fn restore_snapshot_unknown_timestamp_errors() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![50].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![51]);

        store.store_secret(delegate.key(), &secret_id, b"a".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"b".to_vec())?;

        let err = store
            .restore_snapshot(delegate.key(), &secret_id, 0)
            .expect_err("timestamp 0 should not exist");
        match err {
            SecretStoreError::SnapshotNotFound { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 0);
            }
            SecretStoreError::Encryption(_)
            | SecretStoreError::IO(_)
            | SecretStoreError::MissingCipher
            | SecretStoreError::MissingSecret(_) => {
                panic!("expected SnapshotNotFound, got {err:?}");
            }
        }
        Ok(())
    }

    /// Restore after remove_secret must re-add the entry to the ReDb index
    /// and the in-memory map. Without this, `get_secret` would return the
    /// restored value but the secret would be invisible to delegate code
    /// that iterates the index.
    #[tokio::test]
    async fn restore_after_remove_repopulates_index() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![60].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![61]);

        store.store_secret(delegate.key(), &secret_id, b"keep".to_vec())?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(delegate.key(), &secret_id, b"overwrite".to_vec())?;

        // Grab the snapshot stamp BEFORE removing the secret. `remove_secret`
        // also deletes the snapshot directory, so we need the timestamp now.
        let snaps = store.list_snapshots(delegate.key(), &secret_id)?;
        assert_eq!(snaps.len(), 1);
        let prior_ts = snaps[0].timestamp_ms;

        // Now copy the snapshot ciphertext aside so we can replay it after
        // `remove_secret` wipes the .snapshots dir. This simulates an
        // operator backing up the snapshot file before deletion.
        let snap_src = snaps[0].path.clone();
        let snap_backup = temp_dir.path().join("backup-snapshot");
        std::fs::copy(&snap_src, &snap_backup)?;

        store.remove_secret(delegate.key(), &secret_id)?;

        // Re-stage the saved snapshot at the same on-disk location so the
        // restore code can find it.
        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        std::fs::create_dir_all(&snap_dir)?;
        std::fs::copy(&snap_backup, snap_src)?;

        // Confirm pre-condition: index does NOT contain the secret yet.
        let secret_hash = *secret_id.hash();
        let in_mem_before = store
            .key_to_secret_part
            .get(delegate.key())
            .map(|e| e.value().contains(&secret_hash))
            .unwrap_or(false);
        assert!(!in_mem_before, "index should be empty after remove_secret");

        store.restore_snapshot(delegate.key(), &secret_id, prior_ts)?;

        // Post-condition: index contains the secret again AND get_secret
        // returns the restored value.
        let post_index = store
            .db
            .get_secrets_index(delegate.key())
            .expect("index lookup")
            .unwrap_or_default();
        assert!(
            post_index.contains(&secret_hash),
            "ReDb index must re-include the restored secret"
        );
        let in_mem_after = store
            .key_to_secret_part
            .get(delegate.key())
            .map(|e| e.value().contains(&secret_hash))
            .unwrap_or(false);
        assert!(
            in_mem_after,
            "in-memory map must re-include the restored secret"
        );
        // The snapshot was taken when "overwrite" was written, but it
        // holds the PRIOR active value at that point: "keep".
        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"keep".to_vec()
        );
        Ok(())
    }

    /// When multiple snapshots share `timestamp_ms` (collision suffixes
    /// from same-millisecond writes), `restore_snapshot` MUST pick the
    /// unsuffixed file first, then the lowest-numbered suffix. Documented
    /// as a behavioral contract on the public method, so pin it directly
    /// rather than relying on the list-side ordering test.
    #[tokio::test]
    async fn restore_snapshot_prefers_unsuffixed_collision()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::wasm_runtime::secret_snapshots::SNAPSHOT_NAME_WIDTH;

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        // Permissive retention so thin_snapshots doesn't drop the
        // hand-crafted ancient-timestamped files between restore calls.
        store.set_retention_policy(RetentionPolicy {
            keep_last: 100,
            buckets: vec![],
            max_age: None,
        });

        let delegate = Delegate::from((&vec![70].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![71]);

        // Seed an active value so restore has something to overwrite (and
        // can take its own pre-restore snapshot). The plaintext doesn't
        // matter for this test; we compare ciphertext after restore.
        store.store_secret(delegate.key(), &secret_id, b"active".to_vec())?;

        // Hand-craft three "same timestamp" snapshot files with distinct
        // ciphertexts, so we can identify which one wins. We do the file
        // surgery directly instead of going through store_secret because
        // we need the collision case, which the natural-write path only
        // hits under extreme contention.
        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        std::fs::create_dir_all(&snap_dir)?;
        let stamp = 1_700_000_000_000u64;
        let base = format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH);
        // Encrypt three distinguishable plaintexts with the registered
        // cipher and write the ciphertexts as the three "collision"
        // snapshots. After restore + get_secret we identify the winner
        // by the recovered plaintext.
        let encryption = store
            .ciphers
            .get(delegate.key())
            .expect("cipher registered");
        let mk = |pt: &[u8]| -> Vec<u8> {
            encryption
                .cipher
                .encrypt(&encryption.nonce, pt)
                .expect("encrypt")
        };
        std::fs::write(snap_dir.join(&base), mk(b"unsuffixed-winner"))?;
        std::fs::write(snap_dir.join(format!("{base}.0")), mk(b"suffix-0"))?;
        std::fs::write(snap_dir.join(format!("{base}.1")), mk(b"suffix-1"))?;

        store.restore_snapshot(delegate.key(), &secret_id, stamp)?;
        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"unsuffixed-winner".to_vec(),
            "unsuffixed file must win the collision tiebreak"
        );

        // Now remove the unsuffixed entry and restore again: lowest
        // surviving suffix wins.
        std::fs::remove_file(snap_dir.join(&base))?;
        store.restore_snapshot(delegate.key(), &secret_id, stamp)?;
        assert_eq!(
            store.get_secret(delegate.key(), &secret_id)?,
            b"suffix-0".to_vec(),
            "with the unsuffixed entry gone, lowest-numbered suffix wins"
        );
        Ok(())
    }
}
