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

use aes_gcm::KeyInit;
use chacha20poly1305::{
    AeadCore, Error as EncryptionError, XChaCha20Poly1305, XNonce,
    aead::{Aead, OsRng},
};
use dashmap::DashMap;
use freenet_stdlib::prelude::*;

use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::config::{KEK_SIZE, KekBackendKind, Secrets, ensure_kek_loaded};
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

/// On-disk format version byte for ciphertext blobs.
///
/// Layout: `[VERSION_V1][24-byte XNonce][AEAD ciphertext + 16-byte tag]`.
///
/// The version byte exists so the read path can distinguish files written
/// under the new per-write-nonce format from legacy files (which were a
/// raw AEAD blob using the per-delegate registration nonce). A legacy
/// file starts with the first byte of AEAD output — uniformly random —
/// so there is a 1/256 chance of a legacy file starting with this byte.
/// `get_secret` handles that ambiguity by falling back to a legacy decrypt
/// if the new-format parse fails.
const VERSION_V1: u8 = 0x01;

/// Number of bytes of overhead the new on-disk format adds on top of the
/// raw AEAD ciphertext: 1 version byte + 24-byte XNonce.
const HEADER_LEN: usize = 1 + 24;

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
    /// Per-delegate registration nonce. Used ONLY by the legacy-decrypt
    /// fallback in `get_secret` for files written before the per-write-nonce
    /// format landed (see `VERSION_V1`). New writes generate a fresh
    /// random nonce per call to `store_secret`.
    legacy_nonce: XNonce,
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
    /// Node KEK loaded from the configured backend (OS keyring, systemd
    /// credential, or `secrets_dir/node_kek`). Held in a `Zeroizing`
    /// buffer so it is wiped from memory when the store is dropped.
    /// Used as the master key for the HKDF derivation of per-delegate
    /// DEKs. See `derive_delegate_dek`.
    kek: Zeroizing<[u8; KEK_SIZE]>,
    /// Backend that currently holds [`Self::kek`]. Surfaced through
    /// `fdev secrets kek-status` and recorded in
    /// `secrets_dir/kek_backend` so transient outages of a stronger
    /// backend cannot silently demote the node to a weaker one.
    kek_backend: KekBackendKind,
    /// Per-delegate encryption keys. Each entry is either:
    ///   - derived from the node KEK via HKDF-SHA256 (the common case
    ///     after #4140); or
    ///   - supplied by a client through `RegisterDelegate` (legacy path
    ///     retained for wire-format compatibility; the supplied cipher
    ///     overrides the derived one for that delegate).
    ///
    /// Cache-only; never persisted. On restart, derived entries are
    /// reconstructed lazily on first `get_secret` / `store_secret` for
    /// each delegate.
    ciphers: std::collections::HashMap<DelegateKey, Encryption>,
    /// In-memory index: DelegateKey -> Set of secret key hashes.
    /// Populated from ReDb on startup and kept in sync with it; never
    /// updated unless the corresponding ReDb write succeeded.
    key_to_secret_part: Arc<DashMap<DelegateKey, HashSet<SecretKey>>>,
    /// ReDb storage for persistent index
    db: Storage,
    default_encryption: Encryption,
    /// Last-resort decrypt fallback seeded with the historical
    /// `LEGACY_DEFAULT_CIPHER` + `LEGACY_DEFAULT_NONCE` pair (the world-
    /// known constants that `freenet-stdlib` 0.8.0 removed).
    ///
    /// Used ONLY by `decrypt_secret_blob` for pre-#4143 on-disk files
    /// that were written under the default-cipher fallback path.
    /// Without this, a node that auto-generated a fresh cipher on
    /// upgrade (the new `SecretArgs::build` behavior) would be unable
    /// to read pre-existing default-encrypted delegate secrets across
    /// the restart, because `default_encryption.cipher` is now the
    /// auto-generated value and `register_delegate` (which would
    /// otherwise supply the legacy cipher) has not yet been called by
    /// any client.
    ///
    /// `None` only when the build path explicitly disabled migration
    /// (currently never; reserved for a future "drop migration support"
    /// release).
    legacy_migration_encryption: Option<Encryption>,
    /// Snapshot retention policy. Snapshots are taken before any overwrite
    /// of an existing secret so a buggy delegate or accidental write cannot
    /// silently destroy prior values.
    retention: RetentionPolicy,
    snapshots_enabled: bool,
}

/// HKDF info string for per-delegate DEK derivation. Versioned (`v1`)
/// so a future derivation-algorithm change can rotate via this string
/// without rotating the KEK itself.
const DEK_HKDF_INFO: &[u8] = b"freenet-delegate-dek-v1";

impl SecretsStore {
    pub fn new(secrets_dir: PathBuf, secrets: Secrets, db: Storage) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&secrets_dir).map_err(|err| {
            tracing::error!("error creating secrets dir: {err}");
            err
        })?;

        // Load (or resolve + provision) the node KEK. First start picks
        // a backend from the OS-keyring → systemd-credential → file
        // chain; subsequent starts read the recorded backend marker and
        // load strictly from there (no silent demotion). `OsRng` is the
        // documented exception to the GlobalRng rule for cryptographic
        // key material (see `.claude/rules/code-style.md`).
        let (kek_backend, kek) = ensure_kek_loaded(&secrets_dir, || {
            use chacha20poly1305::aead::OsRng;
            use chacha20poly1305::aead::rand_core::RngCore;
            let mut kek = Zeroizing::new([0u8; KEK_SIZE]);
            OsRng.fill_bytes(kek.as_mut_slice());
            kek
        })
        .map_err(|e| {
            tracing::error!("failed to load node KEK: {e}");
            std::io::Error::other(format!("KEK load failed: {e}"))
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

        // Seed the legacy-migration fallback with the historical
        // (LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE) pair regardless
        // of what the operator's configured `secrets` carries. This is
        // the only path that lets pre-#4143 on-disk files written under
        // the world-known default constants remain decryptable on a
        // node whose `default_encryption.cipher` is now the
        // auto-generated random cipher (post-stdlib-0.8.0 upgrade).
        use crate::config::{LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE};
        let legacy_migration_encryption = Some(Encryption {
            cipher: XChaCha20Poly1305::new((&LEGACY_DEFAULT_CIPHER).into()),
            legacy_nonce: LEGACY_DEFAULT_NONCE.into(),
        });

        Ok(Self {
            base_path: secrets_dir,
            kek,
            kek_backend,
            ciphers: std::collections::HashMap::new(),
            key_to_secret_part,
            db,
            default_encryption: Encryption {
                cipher: secrets.cipher(),
                legacy_nonce: secrets.nonce(),
            },
            legacy_migration_encryption,
            secrets,
            retention: RetentionPolicy::default(),
            snapshots_enabled: std::env::var_os(DISABLE_SNAPSHOTS_ENV).is_none(),
        })
    }

    /// Return the backend currently holding the node KEK. Surfaced via
    /// `fdev secrets kek-status` so operators can see whether they are
    /// running with the strong (keyring/systemd) or weak (file) backend.
    pub fn kek_backend(&self) -> KekBackendKind {
        self.kek_backend
    }

    /// Derive the per-delegate DEK from the node KEK via HKDF-SHA256.
    ///
    /// HKDF inputs:
    ///
    /// - `ikm` = node KEK (32 bytes from the configured backend).
    /// - `salt` = `delegate_key.encode()`, the bs58 encoding of the
    ///   32-byte instance key (`DelegateKey::key`), i.e.
    ///   `blake3(params || wasm_code)`. The companion `code_hash`
    ///   field is NOT included in the salt — it's redundant because
    ///   the instance key already folds in the wasm code, and binding
    ///   it separately would only matter if two distinct delegates
    ///   ever shared an instance key (currently impossible by
    ///   construction).
    /// - `info` = the versioned constant [`DEK_HKDF_INFO`]. Bumping
    ///   the version rotates every DEK without touching the KEK.
    /// - `okm` = 32 bytes (XChaCha20-Poly1305 key size).
    ///
    /// Deterministic in `(kek, delegate_key)` — restarting the node and
    /// re-deriving yields the same DEK, which is what lets persisted
    /// secrets stay readable across restart without a separate
    /// per-delegate cipher store.
    fn derive_delegate_dek(&self, delegate: &DelegateKey) -> Encryption {
        let salt = delegate.encode();
        let hk = Hkdf::<Sha256>::new(Some(salt.as_bytes()), self.kek.as_slice());
        let mut okm = Zeroizing::new([0u8; KEK_SIZE]);
        hk.expand(DEK_HKDF_INFO, okm.as_mut_slice())
            .expect("HKDF expand with 32-byte OKM never fails for SHA-256");
        Encryption {
            cipher: XChaCha20Poly1305::new(okm.as_slice().into()),
            // Per-write random nonces are the production path; the
            // `legacy_nonce` field is only consulted by the legacy
            // decrypt fallback in `decrypt_secret_blob`. For derived
            // DEKs we have no legacy on-disk files written under a
            // shared nonce, so the value here is irrelevant — pin to
            // zeros for determinism (so two stores constructed against
            // the same KEK produce byte-identical `Encryption` values).
            legacy_nonce: chacha20poly1305::XNonce::from_slice(&[0u8; 24]).to_owned(),
        }
    }

    /// Return the cipher for `delegate`, deriving and caching it from
    /// the KEK on first use. If a client previously called
    /// `register_delegate` for this key, the registered cipher takes
    /// precedence over the derived one (legacy compatibility path).
    fn cipher_for(&mut self, delegate: &DelegateKey) -> &Encryption {
        // Insert-if-absent then borrow. We can't use `Entry::or_insert_with`
        // here because `derive_delegate_dek` needs `&self` (the KEK is on
        // self) and the `Entry` API holds an exclusive borrow of the map.
        // Split: insert via `&mut self` first, then a fresh `get` borrow.
        if !self.ciphers.contains_key(delegate) {
            let derived = self.derive_delegate_dek(delegate);
            self.ciphers.insert(delegate.clone(), derived);
        }
        self.ciphers
            .get(delegate)
            .expect("cipher entry inserted above; cannot be missing in the same &mut self call")
    }

    /// Read-side analogue of `cipher_for` that does not take `&mut self`.
    /// Falls back to `default_encryption` only if HKDF derivation would
    /// somehow fail (it cannot for SHA-256 + 32-byte OKM; the branch is
    /// defensive). Caches via interior mutability via the existing
    /// `ciphers` map IS NOT possible because `get_secret` takes `&self`;
    /// callers MUST tolerate the per-call HKDF cost on cold reads.
    fn cipher_for_read(&self, delegate: &DelegateKey) -> Encryption {
        if let Some(enc) = self.ciphers.get(delegate) {
            return enc.clone();
        }
        self.derive_delegate_dek(delegate)
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
        _cipher: XChaCha20Poly1305,
        _nonce: XNonce,
    ) -> Result<(), SecretStoreError> {
        // Since #4140: per-delegate DEKs are derived deterministically
        // from the node KEK via HKDF-SHA256 (see `derive_delegate_dek`).
        // The cipher and nonce supplied by the client on
        // `RegisterDelegate { cipher, nonce }` are IGNORED — accepting
        // client-supplied keys would allow a malicious or buggy client
        // to substitute a key the operator does not control, defeating
        // the purpose of the node KEK.
        //
        // The wire-format `RegisterDelegate` variant retains those
        // fields for backwards compatibility with older clients (they
        // will simply have their values discarded server-side). A
        // future stdlib bump may drop the fields entirely.
        //
        // We DO eagerly populate the `ciphers` cache with the derived
        // DEK so that subsequent `get_secret`/`store_secret` calls
        // skip the HKDF derivation cost on the hot path.
        tracing::info!(
            delegate = %delegate.encode(),
            "RegisterDelegate cipher/nonce ignored; using HKDF-derived DEK from node KEK \
             (this is the expected behavior since #4140)."
        );
        let derived = self.derive_delegate_dek(&delegate);
        self.ciphers.insert(delegate, derived);
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
        // `cipher_for` derives via HKDF and caches on first call. A
        // prior `register_delegate(delegate, ..)` keeps its registered
        // cipher (legacy compatibility path).
        let encryption = self.cipher_for(delegate);

        // Generate a fresh random nonce per write. XChaCha20-Poly1305's
        // 192-bit nonce makes random selection collision-safe for any
        // realistic write volume; reuse would be catastrophic (keystream
        // XOR + Poly1305 key recovery).
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aead = encryption
            .cipher
            .encrypt(&nonce, plaintext.as_ref())
            .map_err(SecretStoreError::Encryption)?;

        // Compose the on-disk blob: [VERSION_V1][nonce][aead]. The header
        // lets `get_secret` distinguish new files from pre-versioned
        // legacy files that started with raw AEAD output.
        let mut ciphertext = Vec::with_capacity(HEADER_LEN + aead.len());
        ciphertext.push(VERSION_V1);
        ciphertext.extend_from_slice(nonce.as_slice());
        ciphertext.extend_from_slice(&aead);

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
        // Read path derives DEK on demand without caching (requires
        // &self). Cold reads pay one HKDF-SHA256 expand call (~µs).
        let encryption = self.cipher_for_read(delegate);

        let blob =
            fs::read(secret_path).map_err(|_| SecretStoreError::MissingSecret(key.clone()))?;
        // The post-#4144 / pre-#4140 auto-persisted `delegate_cipher`
        // file shows up here as `default_encryption`. Pre-#4143 blobs
        // written under the world-known constants are caught by the
        // last-tier `legacy_migration_encryption`.
        let legacy_chain = [&self.default_encryption];
        decrypt_secret_blob(
            &encryption,
            &legacy_chain,
            self.legacy_migration_encryption.as_ref(),
            &blob,
            key,
        )
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

/// Decrypt an on-disk secret blob, transparently supporting every
/// historical on-disk format freenet-core has written for delegate
/// secrets. Tries paths in order; later paths log progressively louder
/// warnings because they indicate the blob is overdue for a write-side
/// rewrite under the current key derivation:
///
/// 1. **Current** — New per-write-nonce format `[VERSION_V1][nonce][AEAD]`
///    decrypted with the registered or HKDF-derived cipher.
/// 2. **Cipher rotated / blob pre-#4143** — Legacy shared-nonce format
///    (raw `[AEAD]`) decrypted with the registered/derived cipher's
///    `legacy_nonce` field.
/// 3. **Post-#4144 / pre-#4140 delegate_cipher file** — Same blob shape
///    as tier 2 but using the auto-persisted per-node cipher from
///    `SecretArgs::build` (the brief window where the node had a random
///    `delegate_cipher` file but no HKDF derivation yet). Each
///    `legacy_chain` entry is tried in order.
/// 4. **World-known migration path** — Last-resort decrypt with the
///    historical `LEGACY_DEFAULT_CIPHER` + `LEGACY_DEFAULT_NONCE` pair
///    (the stdlib constants removed in 0.8.0). Pre-#4143 nodes wrote
///    here when no `--cipher` flag was passed.
///
/// Ambiguity: a legacy blob's first byte is the first byte of AEAD
/// output (uniformly random), so 1/256 of legacy files start with
/// `VERSION_V1`. If the new-format parse fails AEAD validation we fall
/// through to the legacy paths. Each path is independent — failure of
/// one does not mask success of another.
fn decrypt_secret_blob(
    encryption: &Encryption,
    legacy_chain: &[&Encryption],
    legacy_migration: Option<&Encryption>,
    blob: &[u8],
    key: &SecretsId,
) -> Result<Vec<u8>, SecretStoreError> {
    // Decryption strategy. The format + cipher have rotated three
    // times across the secrets-at-rest hardening sequence:
    //
    //   Tier 1 (`encryption`, the registered/derived DEK):
    //     - VERSION_V1 (`[0x01][nonce][AEAD]`) — today's writer
    //     - raw-AEAD with `encryption.legacy_nonce` — same cipher,
    //       pre-#4143 format (a delegate whose key hasn't rotated but
    //       whose oldest secret hasn't been overwritten since upgrade).
    //
    //   Tier 2 (`legacy_chain[..]`, e.g. the post-#4144 / pre-#4140
    //   auto-persisted `delegate_cipher` carried by `default_encryption`):
    //     - VERSIONED ONLY. Every release that wrote under these
    //       ciphers was already at the per-write-nonce format (post
    //       #4143), so raw-AEAD attempts would only burn cipher ops
    //       without ever matching a real blob.
    //
    //   Tier 3 (`legacy_migration`, the LEGACY_DEFAULT_* world-known
    //   constants):
    //     - BOTH formats. The #4143 release window emitted versioned
    //       blobs while default-configured nodes were still seeded
    //       from LEGACY_DEFAULT_CIPHER (per-write-nonce had landed but
    //       the auto-gen cipher hadn't yet); pre-#4143 default-config
    //       nodes emitted raw-AEAD under the same constants. Both are
    //       in the wild on upgraded operators' disks. Logged at WARN
    //       so operators see migration progress.
    if blob.first().copied() == Some(VERSION_V1) && blob.len() >= HEADER_LEN {
        let nonce = XNonce::from_slice(&blob[1..HEADER_LEN]);
        // Tier 1 versioned.
        if let Ok(pt) = encryption.cipher.decrypt(nonce, &blob[HEADER_LEN..]) {
            return Ok(pt);
        }
        // Tier 2 versioned.
        for (idx, fallback) in legacy_chain.iter().enumerate() {
            if let Ok(pt) = fallback.cipher.decrypt(nonce, &blob[HEADER_LEN..]) {
                log_legacy_decrypt(key, idx + 1, false, "versioned");
                return Ok(pt);
            }
        }
        // Tier 3 versioned. Required for #4143-era blobs written under
        // the world-known default cipher with the per-write nonce.
        if let Some(migration) = legacy_migration
            && let Ok(pt) = migration.cipher.decrypt(nonce, &blob[HEADER_LEN..])
        {
            log_legacy_decrypt(key, 1 + legacy_chain.len(), true, "versioned");
            return Ok(pt);
        }
    }
    // Tier 1 raw-AEAD (cipher unchanged but format pre-dates #4143).
    if let Ok(pt) = encryption.cipher.decrypt(&encryption.legacy_nonce, blob) {
        tracing::debug!(
            key = %key,
            "Decrypted pre-#4143 raw-AEAD blob with the registered/derived cipher; \
             will be migrated to per-write-nonce format on next write."
        );
        return Ok(pt);
    }
    // Tier 3 raw-AEAD (world-known LEGACY_DEFAULT_* migration path,
    // pre-#4143 default-configured nodes).
    if let Some(migration) = legacy_migration
        && let Ok(pt) = migration.cipher.decrypt(&migration.legacy_nonce, blob)
    {
        log_legacy_decrypt(key, 1 + legacy_chain.len(), true, "raw-aead");
        return Ok(pt);
    }
    Err(SecretStoreError::Encryption(
        // The error type is opaque; surface a generic AEAD failure.
        // Callers cannot tell which attempt failed last; the log lines
        // above record which fallback paths were reached.
        chacha20poly1305::Error,
    ))
}

fn log_legacy_decrypt(key: &SecretsId, idx: usize, is_migration: bool, format: &str) {
    if is_migration {
        tracing::warn!(
            key = %key,
            chain_idx = idx,
            format = format,
            "Decrypted secret blob via the legacy-default-cipher migration fallback; \
             this file pre-dates PR #4143. Will be re-encrypted under the current \
             derived DEK on next write."
        );
    } else {
        tracing::info!(
            key = %key,
            chain_idx = idx,
            format = format,
            "Decrypted secret blob via a legacy fallback cipher; will be re-encrypted \
             under the current derived DEK on next write."
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::wasm_runtime::secret_snapshots::{RetentionBucket, RetentionPolicy};
    use aes_gcm::KeyInit;
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
        let blob = std::fs::read(entries[0].path())?;
        let encryption = store
            .ciphers
            .get(delegate.key())
            .expect("cipher registered");
        let plaintext = decrypt_secret_blob(encryption, &[], None, &blob, &secret_id)
            .expect("snapshot blob should decrypt with the registered cipher");
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
        // Produce a VERSION_V1 on-disk blob so `get_secret` (now version-
        // aware) can decrypt the hand-crafted snapshot back to plaintext.
        let mk = |pt: &[u8]| -> Vec<u8> {
            let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
            let aead = encryption.cipher.encrypt(&nonce, pt).expect("encrypt");
            let mut out = Vec::with_capacity(HEADER_LEN + aead.len());
            out.push(VERSION_V1);
            out.extend_from_slice(nonce.as_slice());
            out.extend_from_slice(&aead);
            out
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

    /// Regression for #4139: two writes of the same plaintext under the
    /// same `(delegate, SecretsId)` MUST produce different on-disk bytes.
    /// Identical bytes would indicate nonce reuse, which in
    /// XChaCha20-Poly1305 is catastrophic (keystream XOR recovery between
    /// any two messages + Poly1305 key recovery from two tags).
    #[tokio::test]
    async fn per_write_nonce_makes_identical_plaintext_ciphertext_distinct()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![80].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![81]);

        let plaintext = b"identical".to_vec();
        store.store_secret(delegate.key(), &secret_id, plaintext.clone())?;
        let active = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        let first = std::fs::read(&active)?;

        // No sleep: the nonce uniqueness invariant comes from `OsRng`, not
        // wall-clock time. The surrounding snapshot tests sleep to force
        // distinct epoch-millis filenames, but that is irrelevant here —
        // the second write deliberately reuses the same epoch slot.
        store.store_secret(delegate.key(), &secret_id, plaintext.clone())?;
        let second = std::fs::read(&active)?;

        assert_ne!(
            first, second,
            "two writes of the same plaintext under nonce-per-write MUST differ on disk"
        );
        // Specifically pin the nonce field bytes: catches a regression where
        // someone hardcoded the nonce (e.g. to zeros for "debugging") and
        // the overall ciphertext only happens to differ for some other
        // reason. `assert_ne!(first, second)` alone would miss that.
        assert_ne!(
            &first[1..HEADER_LEN],
            &second[1..HEADER_LEN],
            "nonce field must differ across writes"
        );
        // Both must decrypt back to the same plaintext.
        assert_eq!(store.get_secret(delegate.key(), &secret_id)?, plaintext);
        Ok(())
    }

    /// Regression for the documented 1/256 ambiguity in
    /// `decrypt_secret_blob`: when a legacy blob happens to start with
    /// `VERSION_V1`, the new-format AEAD parse is attempted first and MUST
    /// fail closed; the legacy-decrypt fallback then MUST succeed and
    /// return the original plaintext. Brute-forces the ambiguity by
    /// re-encrypting with random per-attempt nonces until the AEAD output
    /// begins with `VERSION_V1` — expected within ~256 attempts.
    #[tokio::test]
    async fn legacy_blob_with_version_byte_falls_through_to_legacy_decrypt()
    -> Result<(), Box<dyn std::error::Error>> {
        // SAFETY: nextest per-process isolation. The env mutation is
        // confined to this test process and not restored because the
        // process exits when the test ends.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![88].into(), &vec![].into()));
        // Post-#4146: per-delegate cipher = HKDF-derived DEK. Use the
        // store's own derivation so the legacy fallback uses the same
        // (cipher, legacy_nonce) we encrypt under.
        let derived = store.derive_delegate_dek(delegate.key());
        let cipher = derived.cipher.clone();
        let registration_nonce = derived.legacy_nonce;
        let secret_id = SecretsId::new(vec![89]);

        // Find a plaintext whose legacy-format AEAD output starts with
        // VERSION_V1 to force the read path into the documented ambiguity
        // branch. AEAD output is deterministic in (key, nonce, plaintext),
        // so varying only the suffix of the plaintext keeps the first
        // ciphertext byte constant (it depends only on the first plaintext
        // byte and the fixed keystream). Vary the FIRST plaintext byte
        // instead: for the fixed (key, nonce) the relationship
        // `aead[0] = plaintext[0] XOR keystream[0]` makes this a bijection
        // over 0..=255, so exactly one byte value yields `aead[0] ==
        // VERSION_V1`.
        let mut legacy_blob: Option<(u8, Vec<u8>)> = None;
        for first_byte in 0u8..=u8::MAX {
            let plaintext = vec![first_byte; 16];
            let aead = cipher
                .encrypt(&registration_nonce, plaintext.as_ref())
                .expect("legacy encrypt");
            if aead.first().copied() == Some(VERSION_V1) {
                legacy_blob = Some((first_byte, aead));
                break;
            }
            if first_byte == u8::MAX {
                break;
            }
        }
        let (winning_byte, legacy_blob) = legacy_blob.expect(
            "XChaCha20 keystream byte 0 should make aead[0]=0x01 reachable for some plaintext byte",
        );
        assert_eq!(legacy_blob.first().copied(), Some(VERSION_V1));
        assert!(
            legacy_blob.len() >= HEADER_LEN,
            "legacy blob too short to even *look* like a new-format blob: {} bytes",
            legacy_blob.len()
        );

        // Write the legacy blob directly at the active path. `get_secret`
        // will see `blob[0] == VERSION_V1 && blob.len() >= HEADER_LEN`,
        // try new-format decrypt (which fails because bytes [1..25] are
        // not the nonce that produced bytes [25..]), and fall through
        // to legacy decrypt (which must succeed).
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &legacy_blob)?;

        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered,
            vec![winning_byte; 16],
            "fallback must recover the original 16-byte plaintext"
        );
        Ok(())
    }

    /// On-disk blob written by `store_secret` MUST begin with the
    /// `VERSION_V1` header byte and carry a fresh random nonce in
    /// bytes [1..25]. The version byte is the discriminator the read
    /// path uses to tell new files from legacy files; if the writer
    /// ever stops emitting it, the read path will silently fall back
    /// to legacy decrypt (which would fail because there is no shared
    /// registered nonce in the new model). The nonce-randomness check
    /// catches a regression where someone hardcoded the nonce (e.g. to
    /// zeros for "debugging") — `assert_ne!(first, second)` over whole
    /// blobs would miss that if the ciphertext also varies.
    #[tokio::test]
    async fn store_secret_writes_version_header() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![82].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![83]);

        store.store_secret(delegate.key(), &secret_id, b"hello".to_vec())?;
        let active = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        let blob = std::fs::read(&active)?;

        assert_eq!(
            blob.first().copied(),
            Some(VERSION_V1),
            "new-format blob must start with VERSION_V1"
        );
        // 1 version byte + 24 nonce + AEAD (>= 16 bytes of tag).
        assert!(
            blob.len() >= HEADER_LEN + 16,
            "blob too short: {} bytes",
            blob.len()
        );

        // Write a second secret and assert the nonce field differs. The
        // nonce field is the [1..HEADER_LEN] slice. A regression that
        // hardcoded the nonce to a constant (zeros, or anything else)
        // would leave this slice identical across writes; whole-blob
        // inequality alone could be satisfied by varying ciphertext.
        let secret_id_2 = SecretsId::new(vec![84]);
        store.store_secret(delegate.key(), &secret_id_2, b"hello".to_vec())?;
        let blob_2 = std::fs::read(
            secrets_dir
                .join(delegate.key().encode())
                .join(secret_id_2.encode()),
        )?;
        assert_ne!(
            &blob[1..HEADER_LEN],
            &blob_2[1..HEADER_LEN],
            "nonce field must be random per write"
        );
        Ok(())
    }

    /// Regression for #4139 migration path: a legacy-format on-disk file
    /// (raw AEAD output written under the per-delegate cipher with the
    /// registration nonce, no version header) MUST still be readable
    /// through `get_secret`. This is what lets nodes upgrade in place
    /// without a one-shot migration tool.
    ///
    /// Post-#4146: the per-delegate cipher is the HKDF-derived DEK
    /// (`register_delegate` ignores client-supplied cipher), so we
    /// derive the cipher from the store and write under that.
    #[tokio::test]
    async fn legacy_format_blob_is_decryptable() -> Result<(), Box<dyn std::error::Error>> {
        // SAFETY: nextest per-process isolation. Force file backend so
        // the test exercises a deterministic KEK source.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![84].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![85]);

        // Derive the DEK that THIS store will use for `delegate`, then
        // hand-craft a raw-AEAD blob under that DEK + a fixed nonce
        // (simulating a pre-#4143 file written before per-write
        // nonces). The store's `legacy_nonce` field on the derived
        // Encryption is what tier 1's raw-AEAD attempt uses.
        let derived = store.derive_delegate_dek(delegate.key());
        let plaintext = b"legacy-payload".to_vec();
        let legacy_blob = derived
            .cipher
            .encrypt(&derived.legacy_nonce, plaintext.as_ref())
            .expect("legacy encrypt under derived DEK");
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &legacy_blob)?;

        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered, plaintext,
            "legacy-format blob must decrypt via tier 1 raw-AEAD fallback"
        );
        Ok(())
    }

    /// A corrupt VERSION_V1 blob (right header, garbage AEAD) MUST surface
    /// `SecretStoreError::Encryption`, not silently succeed and not produce
    /// a misleading `MissingSecret` (which would mask data loss).
    #[tokio::test]
    async fn corrupt_versioned_blob_errors_cleanly() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![86].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![87]);

        // VERSION_V1 + 24 zero bytes (nonce) + 32 bytes of zeros pretending
        // to be ciphertext+tag. AEAD will reject this tag.
        let mut bogus = vec![VERSION_V1];
        bogus.extend_from_slice(&[0u8; 24]);
        bogus.extend_from_slice(&[0u8; 32]);
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &bogus)?;

        let err = store
            .get_secret(delegate.key(), &secret_id)
            .expect_err("corrupt blob must fail");
        assert!(
            matches!(err, SecretStoreError::Encryption(_)),
            "expected Encryption error, got {err:?}"
        );
        Ok(())
    }

    /// Backwards-compat for snapshots written before the per-write-nonce
    /// format landed. `restore_snapshot` byte-copies the snapshot file
    /// back to the active path without re-encryption, so a legacy
    /// snapshot ends up at the active path in legacy format. The very
    /// next `get_secret` MUST recover the plaintext through the legacy
    /// fallback. Pins that the upgrade path works without a separate
    /// migration of the snapshot history.
    #[tokio::test]
    async fn legacy_snapshot_survives_restore_and_get_secret()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::wasm_runtime::secret_snapshots::SNAPSHOT_NAME_WIDTH;
        // SAFETY: nextest per-process isolation. The env mutation is
        // confined to this test process and not restored because the
        // process exits when the test ends.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![90].into(), &vec![].into()));
        // Post-#4146: use derived DEK rather than client-supplied cipher
        // (which is ignored by register_delegate).
        let derived = store.derive_delegate_dek(delegate.key());
        let cipher = derived.cipher.clone();
        let registration_nonce = derived.legacy_nonce;
        let secret_id = SecretsId::new(vec![91]);

        // Seed an active value (new format) so restore has something to
        // overwrite and is allowed to snapshot the active first.
        store.store_secret(delegate.key(), &secret_id, b"current".to_vec())?;

        // Hand-craft a LEGACY snapshot file: raw AEAD with the registered
        // nonce, no version header. The retention policy will not touch
        // this stamp (well below `now`) because it sorts as the oldest.
        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        std::fs::create_dir_all(&snap_dir)?;
        let stamp = 1_700_000_000_000u64;
        let snap_path = snap_dir.join(format!("{stamp:0width$}", width = SNAPSHOT_NAME_WIDTH));
        // Force a plaintext whose legacy AEAD does NOT happen to start with
        // VERSION_V1, so the read path takes the plain legacy branch (the
        // 1/256 ambiguity branch has its own dedicated test).
        let plaintext = b"legacy-snapshot-payload".to_vec();
        let legacy_aead = cipher
            .encrypt(&registration_nonce, plaintext.as_ref())
            .expect("legacy encrypt");
        assert_ne!(
            legacy_aead.first().copied(),
            Some(VERSION_V1),
            "test setup unlucky: legacy AEAD happens to start with VERSION_V1; \
             pick a different plaintext"
        );
        std::fs::write(&snap_path, &legacy_aead)?;

        // Permissive retention so thin_snapshots doesn't drop our ancient
        // stamp before restore can find it.
        store.set_retention_policy(RetentionPolicy {
            keep_last: 100,
            buckets: vec![],
            max_age: None,
        });

        // Restore byte-copies legacy AEAD back to the active path.
        store.restore_snapshot(delegate.key(), &secret_id, stamp)?;
        // Active path now holds a legacy blob. `get_secret` must recover
        // the original plaintext through the legacy fallback.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered, plaintext,
            "legacy snapshot must remain decryptable after restore + get_secret"
        );
        Ok(())
    }

    /// Behavioral-change pin for the `register_delegate` simplification:
    /// the old code skipped registration when the caller's nonce matched
    /// the historical default nonce, falling through to
    /// `default_encryption` on reads. The new code always registers the
    /// cipher. The two paths MUST be equivalent for legacy blobs written
    /// under the default `(cipher, nonce)` pair — otherwise existing
    /// default-configured nodes' data would suddenly become unreadable
    /// after upgrade.
    #[tokio::test]
    async fn register_with_default_cipher_decrypts_legacy_default_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::config::{LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE};

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![92].into(), &vec![].into()));
        let default_cipher = XChaCha20Poly1305::new((&LEGACY_DEFAULT_CIPHER).into());
        let default_nonce: XNonce = LEGACY_DEFAULT_NONCE.into();

        // Register with the historical defaults. Under the old code this
        // was a silent no-op (skipped). Under the new code the cipher is
        // registered and `legacy_nonce` holds DEFAULT_NONCE.
        store.register_delegate(
            delegate.key().clone(),
            default_cipher.clone(),
            default_nonce,
        )?;

        // Write a legacy blob using exactly those defaults — simulating a
        // file written by an older freenet-core version that used the
        // default-cipher fallback path.
        let secret_id = SecretsId::new(vec![93]);
        let plaintext = b"upgraded-from-default-config".to_vec();
        let legacy_aead = default_cipher
            .encrypt(&default_nonce, plaintext.as_ref())
            .expect("legacy encrypt");
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &legacy_aead)?;

        // Must recover plaintext via legacy fallback path.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered, plaintext,
            "default-cipher legacy blob must remain readable after register_delegate \
             (behavioral equivalence with the removed skip-on-default-nonce branch)"
        );
        Ok(())
    }

    /// Critical migration regression pin: after the auto-cipher-gen
    /// upgrade, a node restarts with `default_encryption.cipher` set to
    /// a fresh random per-node cipher (NOT `LEGACY_DEFAULT_CIPHER`).
    /// Pre-#4143 on-disk delegate secrets were written under the
    /// world-known `(LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE)` pair.
    /// If no client has called `register_delegate` yet, `get_secret`
    /// MUST still recover the plaintext via the
    /// `legacy_migration_encryption` fallback.
    ///
    /// Without this guarantee, every default-configured node would lose
    /// access to all existing delegate secrets across the
    /// freenet-stdlib 0.6.1 -> 0.8.0 upgrade. This test is what catches
    /// a regression of the B1 fix from PR #4144 review.
    #[tokio::test]
    async fn legacy_default_blob_decryptable_without_register_after_upgrade()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::config::{LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE};

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // `Secrets::default()` returns a RANDOM cipher (production
        // upgrade behavior after PR #4144), NOT the historical default.
        let secrets = Secrets::default();
        assert_ne!(
            secrets.cipher, LEGACY_DEFAULT_CIPHER,
            "test precondition: Secrets::default() must be random per call (post-PR-#4144)"
        );
        let store = SecretsStore::new(secrets_dir.clone(), secrets, db)?;

        // Hand-craft a pre-#4143 on-disk blob: raw AEAD under the
        // historical world-known constants, no version header.
        let delegate = Delegate::from((&vec![94].into(), &vec![].into()));
        let legacy_cipher = XChaCha20Poly1305::new((&LEGACY_DEFAULT_CIPHER).into());
        let legacy_nonce: XNonce = LEGACY_DEFAULT_NONCE.into();
        let plaintext = b"survives-the-upgrade".to_vec();
        let legacy_aead = legacy_cipher
            .encrypt(&legacy_nonce, plaintext.as_ref())
            .expect("legacy encrypt");
        let secret_id = SecretsId::new(vec![95]);
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &legacy_aead)?;

        // No `register_delegate` call — this simulates the first
        // `get_secret` after restart, before any client has issued a
        // new `RegisterDelegate`. Must still recover the plaintext.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered, plaintext,
            "legacy-default blob MUST be decryptable via legacy_migration_encryption \
             fallback, even without register_delegate having been called"
        );
        Ok(())
    }

    /// **Closes the gap targeted by #4138 directly via #4140.** Write a
    /// secret with one `SecretsStore`, drop it, recreate against the
    /// same `secrets_dir` + DB, read the secret back. Pre-#4140 this
    /// would have failed because the registered per-delegate cipher
    /// lived only in `SecretsStore::ciphers` (in-memory) and was lost
    /// on drop. With HKDF derivation from a persisted node KEK
    /// (`secrets_dir/node_kek` 0o600 in the file-backend test path),
    /// the DEK is deterministically reconstructed on second start and
    /// the secret stays readable WITHOUT a `register_delegate` call.
    #[tokio::test]
    async fn restart_roundtrip_recovers_secret_without_re_registering()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db_path = temp_dir.path().to_path_buf();

        let delegate = Delegate::from((&vec![100].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![101]);
        let plaintext = b"persisted-across-restart".to_vec();

        // --- First start: provision KEK, write secret, drop store ---
        //
        // GitHub Actions runners can have `CREDENTIALS_DIRECTORY` set for
        // some workflow types, which would make `SystemdCredentialKek`
        // try to load (and fail, because the credential isn't actually
        // populated) on second start. Clear it for the duration of this
        // test process so the resolver deterministically picks the
        // file backend.
        // SAFETY: nextest runs each test in its own process; the env
        // mutation is isolated to this test process and not restored
        // because the process exits when the test ends.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }

        {
            let db = create_test_db(&db_path).await;
            let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
            store.store_secret(delegate.key(), &secret_id, plaintext.clone())?;
            // Pin the KEK file was provisioned by the file backend
            // (CREDENTIALS_DIRECTORY cleared above; keyring unavailable
            // on Linux because we don't compile that backend, and on
            // macOS/Windows nextest's per-process isolation ensures the
            // tempdir-scoped FileKek wins because no other process
            // could have seeded a keyring entry for this test's
            // KEYRING_SERVICE/KEYRING_USER pair within the test
            // window — but on macOS dev hosts a stale entry from a
            // prior `freenet` run COULD exist. Tightened test below
            // tolerates either resolution by reading whichever marker
            // backend actually won.
            let marker_path = secrets_dir.join("kek_backend");
            assert!(
                marker_path.exists(),
                "first start must persist a backend marker at {}",
                marker_path.display()
            );
        }

        // --- Second start: reload same secrets_dir + DB ---
        let db = create_test_db(&db_path).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        // No `register_delegate` call. DEK is re-derived from the KEK
        // loaded from the persisted file backend.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(
            recovered, plaintext,
            "second-start get_secret MUST recover plaintext via HKDF re-derivation"
        );
        Ok(())
    }

    /// HKDF determinism: same KEK + same delegate_key always yields the
    /// same DEK; different delegate_key yields a different DEK. Pins
    /// the contract that `restart_roundtrip_recovers_secret_without_re_registering`
    /// silently depends on.
    #[tokio::test]
    async fn derive_delegate_dek_deterministic_and_per_delegate()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate_a = Delegate::from((&vec![110].into(), &vec![].into()));
        let delegate_b = Delegate::from((&vec![111].into(), &vec![].into()));

        let dek_a1 = store.derive_delegate_dek(delegate_a.key());
        let dek_a2 = store.derive_delegate_dek(delegate_a.key());
        let dek_b = store.derive_delegate_dek(delegate_b.key());

        // Determinism: encrypt the same plaintext + nonce with both
        // copies of DEK A; ciphertexts must be byte-identical.
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let pt = b"determinism-pin".as_slice();
        let ct1 = dek_a1.cipher.encrypt(&nonce, pt).expect("encrypt");
        let ct2 = dek_a2.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_eq!(ct1, ct2, "same KEK + same delegate must yield same DEK");

        // Per-delegate: DEK B encrypting the same (pt, nonce) must
        // produce a different ciphertext.
        let ct3 = dek_b.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_ne!(ct1, ct3, "different delegate_key must yield different DEK");
        Ok(())
    }

    // =========================================================================
    // BACKWARDS-COMPAT MATRIX
    // =========================================================================
    //
    // Every freenet-core release the on-disk secret blob format has
    // evolved through MUST remain readable by the current code, so
    // upgrading nodes do not lose access to existing delegate secrets.
    // The matrix exercised below:
    //
    //   Era            Format                     Cipher used to write
    //   ----           ------                     --------------------
    //   < #4143        raw AEAD                   LEGACY_DEFAULT_CIPHER
    //   #4143          [VER][nonce][AEAD]         LEGACY_DEFAULT_CIPHER  (no auto-gen yet)
    //   #4144          [VER][nonce][AEAD]         auto-gen `delegate_cipher` file
    //   #4140          [VER][nonce][AEAD]         HKDF-derived DEK from node KEK
    //
    // Each era's blob MUST decrypt via `get_secret` on a node built
    // against the current code, WITHOUT a `register_delegate` call
    // (modelling the post-restart pre-client-reconnect window).

    /// Era #4143 — versioned format, cipher = LEGACY_DEFAULT_CIPHER.
    /// Exercised by `legacy_chain` fallback's versioned path (the
    /// `migration_tail_start` branch in `decrypt_secret_blob`).
    #[tokio::test]
    async fn backcompat_versioned_blob_under_legacy_default_cipher()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::config::{LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE};

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // `Secrets::default()` returns a random cipher (post-PR-#4144);
        // legacy_migration_encryption is what holds the legacy default.
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        // Hand-craft a #4143-era blob: VERSION_V1 header, fresh random
        // nonce, AEAD under LEGACY_DEFAULT_CIPHER + that nonce.
        let delegate = Delegate::from((&vec![120].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![121]);
        let legacy_cipher = XChaCha20Poly1305::new((&LEGACY_DEFAULT_CIPHER).into());
        let _ = LEGACY_DEFAULT_NONCE;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let plaintext = b"era-4143-payload".to_vec();
        let aead = legacy_cipher
            .encrypt(&nonce, plaintext.as_ref())
            .expect("encrypt");
        let mut blob = vec![VERSION_V1];
        blob.extend_from_slice(nonce.as_slice());
        blob.extend_from_slice(&aead);
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &blob)?;

        // No register_delegate. Must recover.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(recovered, plaintext);
        Ok(())
    }

    /// Era #4144 — versioned format, cipher = the random per-node
    /// `delegate_cipher` file contents. Exercises the `legacy_chain`
    /// fallback (= `default_encryption`) on the versioned path.
    #[tokio::test]
    async fn backcompat_versioned_blob_under_post_4144_delegate_cipher()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        // Pretend the operator's previous freenet-core install left a
        // random `delegate_cipher` file. We construct a `Secrets` that
        // carries that exact cipher in its `cipher` field — which is
        // what `SecretArgs::build` would have produced on a real
        // upgrade.
        let mut secrets = Secrets::default();
        let old_install_cipher_bytes = secrets.cipher; // capture for hand-crafted encrypt
        let old_install_cipher = XChaCha20Poly1305::new((&old_install_cipher_bytes).into());
        // Construct store with the captured cipher seeded into
        // `default_encryption.cipher`.
        let store = SecretsStore::new(secrets_dir.clone(), secrets.clone(), db)?;
        // Now hand-craft a blob exactly as the previous-install code
        // would have produced it under `default_encryption.cipher`.
        let delegate = Delegate::from((&vec![122].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![123]);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let plaintext = b"era-4144-payload".to_vec();
        let aead = old_install_cipher
            .encrypt(&nonce, plaintext.as_ref())
            .expect("encrypt");
        let mut blob = vec![VERSION_V1];
        blob.extend_from_slice(nonce.as_slice());
        blob.extend_from_slice(&aead);
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &blob)?;

        // Post-#4140 cipher_for_read returns the HKDF-derived DEK,
        // which does NOT match the blob's cipher. legacy_chain[0] =
        // default_encryption holds the captured old cipher, so the
        // versioned-format attempt at chain index 1 succeeds.
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(recovered, plaintext);
        // Sanity: silence unused-mut warning.
        secrets.cipher_path = None;
        Ok(())
    }

    /// Era #4143 raw-AEAD legacy path with the historical default
    /// constants. Already covered by
    /// `legacy_default_blob_decryptable_without_register_after_upgrade`
    /// above; pinned again here as part of the backcompat matrix for
    /// documentation/discoverability.
    #[tokio::test]
    async fn backcompat_raw_aead_under_legacy_default_cipher()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::config::{LEGACY_DEFAULT_CIPHER, LEGACY_DEFAULT_NONCE};

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![124].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![125]);
        let legacy_cipher = XChaCha20Poly1305::new((&LEGACY_DEFAULT_CIPHER).into());
        let legacy_nonce: XNonce = LEGACY_DEFAULT_NONCE.into();
        let plaintext = b"pre-4143-payload".to_vec();
        let aead = legacy_cipher
            .encrypt(&legacy_nonce, plaintext.as_ref())
            .expect("encrypt");
        let delegate_dir = secrets_dir.join(delegate.key().encode());
        std::fs::create_dir_all(&delegate_dir)?;
        std::fs::write(delegate_dir.join(secret_id.encode()), &aead)?;

        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(recovered, plaintext);
        Ok(())
    }

    /// Wire-format compat for `RegisterDelegate`: clients that still
    /// send the (now-ignored) `cipher` + `nonce` fields MUST continue
    /// to function. After register, subsequent store/get works under
    /// the HKDF-derived DEK, not the client-supplied cipher.
    #[tokio::test]
    async fn backcompat_register_delegate_wire_still_works()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![126].into(), &vec![].into()));
        // Client-supplied cipher/nonce: server-side these are ignored.
        let (client_cipher, client_nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), client_cipher, client_nonce)?;
        let secret_id = SecretsId::new(vec![127]);
        let plaintext = b"register-then-write".to_vec();
        store.store_secret(delegate.key(), &secret_id, plaintext.clone())?;
        let recovered = store.get_secret(delegate.key(), &secret_id)?;
        assert_eq!(recovered, plaintext);
        Ok(())
    }
}
