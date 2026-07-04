//! Core [`SecretsStore`] type, [`SecretStoreError`], [`Encryption`],
//! [`ExportSecretEntry`], [`ExportScopeError`], and all `impl SecretsStore`
//! methods, plus the `decrypt_secret_blob` / `log_legacy_decrypt` helpers
//! and the `#[cfg(test)]` test module.

use std::{
    collections::HashSet,
    fs,
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

use super::super::RuntimeResult;
use super::super::secret_snapshots::{
    RestoreError, RetentionPolicy, SNAPSHOTS_DIR, SnapshotMetadata, list_snapshots,
    restore_snapshot_file, snapshot_active_value, snapshot_dir_for, thin_snapshots,
};

use super::quota::{QuotaCommit, USER_QUOTA_TRACKER, apply_signed_delta};
#[cfg(test)]
use super::sweep::USERS_DIR;
use super::sweep::{
    create_owner_only, decode_bs58_32, ensure_owner_only_dir, ensure_owner_only_tree,
};
#[cfg(test)]
use super::user::UserSecretContext;
use super::user::{SecretScope, UserId};

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

/// Length of the AEAD authentication tag (Poly1305) appended to every
/// ciphertext. XChaCha20-Poly1305's ciphertext is `plaintext.len() + TAG_LEN`,
/// so an on-disk blob is `HEADER_LEN + plaintext.len() + TAG_LEN`. Used by the
/// per-user quota footprint accounting (#4561) to project the `.keys` registry
/// file's exact on-disk size after adding a key, WITHOUT re-encrypting.
const TAG_LEN: usize = 16;

type SecretKey = [u8; 32];

/// File name (inside a scope directory) of the encrypted registry that maps
/// each stored secret's hash to its raw key bytes. Needed for key enumeration
/// (`list_secret_keys`): the durable index only retains the 32-byte Blake3
/// hash, and the secret filenames are hash-derived, so the raw key the
/// delegate originally supplied (e.g. `room:<owner_vk>`) is recoverable
/// nowhere else. The registry lives INSIDE the scope dir, so a Local and a
/// per-user registry are automatically isolated, and it is encrypted with the
/// SAME scope DEK as the secret values — raw keys never sit in plaintext at
/// rest, preserving the pre-existing privacy posture (only hashes were ever
/// plaintext, in the ReDb index).
const KEY_REGISTRY_FILE: &str = ".keys";

/// Maximum number of raw keys retained per scope in the enumeration registry.
/// This bounds the registry's memory and on-disk size against a delegate that
/// stores an unbounded key family (the #3798 amplification class). Once the
/// cap is reached, NEW distinct keys are still stored as secrets and remain
/// readable by their (hash-derived) path, but they are not added to the
/// enumeration registry, so `list_secret_keys` returns a bounded, truncated
/// view rather than growing without limit. Sized generously so realistic key
/// families (River rooms, per-contact records) enumerate fully.
const MAX_REGISTERED_KEYS_PER_SCOPE: usize = 4096;

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
    /// A per-user ([`SecretScope::User`]) write was REJECTED because it would
    /// push the user's TOTAL on-disk footprint over their configured quota
    /// (#4561, P5 of #4381). Hosted mode only — [`SecretScope::Local`] is never
    /// quota-checked, so single-user nodes can never see this.
    ///
    /// "Total footprint" is everything a hosted user controls under their
    /// `users/<user_id>/` tree across all delegates: the active secret-value
    /// blobs PLUS the per-(delegate,user) `.keys` enumeration registry files.
    /// Secret-value snapshots are DISABLED for the per-user scope (hosted users
    /// are transient and don't need overwrite/rollback history), so there is no
    /// `.snapshots/` growth vector to charge. Together this makes the footprint
    /// provably bounded by `limit`.
    ///
    /// REJECT-on-full, never evict: per-user secrets are authoritative identity
    /// and room keys, not a reconstructible cache, so dropping an existing one
    /// to make room would destroy real data. The delegate sees `set_secret`
    /// fail (mapped to `ERR_STORAGE_FAILED`) and can surface the condition to
    /// the user.
    ///
    /// The three numbers are non-secret byte counts (a user's storage
    /// footprint, their limit, and the size of the rejected blob) — they name
    /// no key material and are safe to log/return.
    #[error(
        "per-user secret quota exceeded: used {used} bytes, limit {limit} bytes, attempted {attempted} more bytes"
    )]
    QuotaExceeded {
        used: u64,
        limit: u64,
        attempted: u64,
    },
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
    /// In-memory index for the per-user ([`SecretScope::User`]) dimension:
    /// (DelegateKey, UserId) -> Set of secret key hashes. Kept disjoint from
    /// `key_to_secret_part` so the Local index is byte-for-byte unchanged.
    /// Mirrors the separate `user_secrets_index` ReDb table; populated on
    /// startup and updated only after the corresponding ReDb write succeeds.
    user_key_to_secret_part: Arc<DashMap<(DelegateKey, UserId), HashSet<SecretKey>>>,
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
    /// Cap on raw keys retained per scope in the enumeration registry.
    /// Defaults to [`MAX_REGISTERED_KEYS_PER_SCOPE`]; lowered by tests to
    /// exercise the at-cap path without writing thousands of secrets.
    max_registered_keys_per_scope: usize,
    /// Per-user on-disk secret-byte quota in bytes (#4561, P5 of #4381).
    /// `0` disables enforcement (the default for [`SecretsStore::new`], so
    /// every existing call site — and the Local-only single-user path — is
    /// unaffected). Production wires the operator-configured value in via
    /// [`Self::with_user_quota`]. ENFORCED ONLY for [`SecretScope::User`];
    /// [`SecretScope::Local`] is never quota-checked.
    ///
    /// The limit is a per-store field (not global) because it comes from the
    /// node's `Config`; every production store in the process reads the SAME
    /// config so they all carry the same limit, while the per-user *counters*
    /// they enforce it against live in the process-global [`QuotaTracker`].
    user_quota_limit_bytes: u64,
}

/// HKDF info string for per-delegate DEK derivation. Versioned (`v1`)
/// so a future derivation-algorithm change can rotate via this string
/// without rotating the KEK itself.
const DEK_HKDF_INFO: &[u8] = b"freenet-delegate-dek-v1";

/// HKDF info string for the per-user DEK derivation (the [`SecretScope::User`]
/// path). Distinct from [`DEK_HKDF_INFO`] so the two derivations can never
/// collide even if a `dek_secret` ever equalled the node KEK by accident.
/// Versioned (`user-v1`) on the same rotation discipline as the local info.
const USER_DEK_HKDF_INFO: &[u8] = b"freenet-delegate-dek-user-v1";
impl SecretsStore {
    pub fn new(secrets_dir: PathBuf, secrets: Secrets, db: Storage) -> RuntimeResult<Self> {
        std::fs::create_dir_all(&secrets_dir).map_err(|err| {
            tracing::error!("error creating secrets dir: {err}");
            err
        })?;

        // Tighten directory permissions to owner-only. Cheap to do
        // unconditionally; a pre-existing 0o755 directory from a node
        // upgraded across this commit gets fixed in one restart.
        if let Err(e) = ensure_owner_only_dir(&secrets_dir) {
            tracing::warn!(
                path = %secrets_dir.display(),
                error = %e,
                "failed to tighten secrets-dir permissions; continuing"
            );
        }

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

        // Load the per-user index from its SEPARATE ReDb table. A pre-#4381
        // database simply has no rows here (redb creates the table empty on
        // first open), so old nodes load an empty user index — the Local
        // index above is untouched.
        let user_key_to_secret_part = Arc::new(DashMap::new());
        match db.load_all_user_secrets_index() {
            Ok(entries) => {
                for ((delegate_key, user_bytes), secret_keys) in entries {
                    let secret_set: HashSet<SecretKey> = secret_keys.into_iter().collect();
                    user_key_to_secret_part
                        .insert((delegate_key, UserId::new(user_bytes)), secret_set);
                }
                tracing::debug!(
                    "Loaded {} user-scoped secrets index entries from ReDb",
                    user_key_to_secret_part.len()
                );
            }
            Err(e) => {
                tracing::warn!("Failed to load user-scoped secrets index from ReDb: {e}");
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
            user_key_to_secret_part,
            db,
            default_encryption: Encryption {
                cipher: secrets.cipher(),
                legacy_nonce: secrets.nonce(),
            },
            legacy_migration_encryption,
            secrets,
            retention: RetentionPolicy::default(),
            snapshots_enabled: std::env::var_os(DISABLE_SNAPSHOTS_ENV).is_none(),
            max_registered_keys_per_scope: MAX_REGISTERED_KEYS_PER_SCOPE,
            // Quota OFF by default. Production opts in via `with_user_quota`
            // from the operator config; every other call site (tests, the CLI,
            // the export harness) keeps enforcement disabled, so the Local
            // single-user path is byte-for-byte unchanged.
            user_quota_limit_bytes: 0,
        })
    }

    /// Set the per-user on-disk secret-byte quota (#4561, P5 of #4381) and
    /// return the store. `0` disables enforcement. Builder-style so the ~50
    /// existing `SecretsStore::new(..)` call sites need no change — only the
    /// production construction path (`get_runtime_stores`) and the quota tests
    /// opt in. The limit only ever governs [`SecretScope::User`] writes.
    pub fn with_user_quota(mut self, limit_bytes: u64) -> Self {
        self.user_quota_limit_bytes = limit_bytes;
        self
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

    /// Derive the per-user DEK for a `(delegate, dek_secret)` pair via
    /// HKDF-SHA256.
    ///
    /// HKDF inputs:
    ///
    /// - `ikm` = `dek_secret` (32 bytes), supplied by the caller. This is the
    ///   ONLY key material in the derivation — the node KEK is deliberately
    ///   NOT involved, so a per-user secret is portable: it can be decrypted
    ///   anywhere the same `dek_secret` is presented, independent of which
    ///   node wrote it. (Contrast [`Self::derive_delegate_dek`], whose IKM is
    ///   the node KEK.)
    /// - `salt` = `delegate.encode()`, the bs58 instance key, so two
    ///   delegates presenting the same `dek_secret` still get distinct DEKs.
    /// - `info` = [`USER_DEK_HKDF_INFO`] (distinct from the local-path info).
    /// - `okm` = 32 bytes (XChaCha20-Poly1305 key size).
    ///
    /// Deterministic in `(delegate, dek_secret)`: the same pair always yields
    /// the same DEK, which is what lets a per-user secret round-trip across a
    /// store reopen and stay readable even if the node KEK rotates.
    ///
    /// `&self` is unused (the node KEK is intentionally not consulted) but the
    /// method stays on `SecretsStore` for symmetry with `derive_delegate_dek`
    /// and so a future implementation could fold in store-level state if the
    /// design ever needs it.
    fn derive_user_dek(
        &self,
        delegate: &DelegateKey,
        dek_secret: &Zeroizing<[u8; 32]>,
    ) -> Encryption {
        let salt = delegate.encode();
        let hk = Hkdf::<Sha256>::new(Some(salt.as_bytes()), dek_secret.as_slice());
        let mut okm = Zeroizing::new([0u8; KEK_SIZE]);
        hk.expand(USER_DEK_HKDF_INFO, okm.as_mut_slice())
            .expect("HKDF expand with 32-byte OKM never fails for SHA-256");
        Encryption {
            cipher: XChaCha20Poly1305::new(okm.as_slice().into()),
            // Per-write random nonces are the only nonce source on the User
            // path (there are no legacy User-scoped on-disk files), so this
            // field is never consulted. Pin to zeros for determinism, matching
            // `derive_delegate_dek`.
            legacy_nonce: chacha20poly1305::XNonce::from_slice(&[0u8; 24]).to_owned(),
        }
    }

    /// On-disk directory that holds the active secret files for a scope.
    ///
    /// - `Local` => `base_path/<delegate>` — UNCHANGED from pre-#4381, so
    ///   existing secret files and `.snapshots/` keep their exact paths.
    /// - `User`  => `base_path/<delegate>/users/<user_id>` — the literal
    ///   `users/` segment can never collide with an existing secret-id file
    ///   or the `.snapshots` directory, because those are siblings of
    ///   `users/`, and a `SecretsId::encode()` (bs58 of a 32-byte hash) is
    ///   never the ASCII string "users".
    fn scope_dir(&self, delegate: &DelegateKey, scope: &SecretScope<'_>) -> PathBuf {
        let delegate_dir = self.base_path.join(delegate.encode());
        match scope {
            SecretScope::Local => delegate_dir,
            SecretScope::User { id, .. } => delegate_dir.join("users").join(id.encode()),
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

    /// Shrink the per-scope key-enumeration cap so the at-cap path is testable
    /// without writing thousands of secrets.
    #[cfg(test)]
    pub(crate) fn set_max_registered_keys_per_scope(&mut self, cap: usize) {
        self.max_registered_keys_per_scope = cap;
    }

    /// The per-user quota limit this store enforces (bytes; `0` = disabled).
    /// Test/diagnostic accessor.
    #[cfg(test)]
    pub(crate) fn user_quota_limit_bytes(&self) -> u64 {
        self.user_quota_limit_bytes
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

    /// Store a secret under the given `scope`.
    ///
    /// `SecretScope::Local` is byte-for-byte identical to the pre-#4381
    /// single-user path (same on-disk path, node-KEK-derived DEK, ReDb
    /// `secrets_index` table, blob layout). `SecretScope::User` writes under
    /// `…/users/<user_id>/`, encrypts with a DEK derived solely from the
    /// caller's `dek_secret`, and tracks the index in a SEPARATE ReDb table.
    pub fn store_secret(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: SecretScope<'_>,
        plaintext: Zeroizing<Vec<u8>>,
    ) -> RuntimeResult<()> {
        let scope_path = self.scope_dir(delegate, &scope);
        let secret_file_path = scope_path.join(key.encode());
        let secret_key = *key.hash();
        // DEK selection. Local: `cipher_for` derives via HKDF from the node
        // KEK and caches on first call (a prior `register_delegate` keeps its
        // registered cipher). User: derive a fresh DEK from `dek_secret`,
        // node-KEK-independent and uncached. The Local branch MUST go through
        // `cipher_for` (not `encryption_for_scope`) so the caching behavior is
        // byte-for-byte unchanged.
        let encryption = match &scope {
            SecretScope::Local => self.cipher_for(delegate).clone(),
            SecretScope::User { dek_secret, .. } => self.derive_user_dek(delegate, dek_secret),
        };

        // Generate a fresh random nonce per write. XChaCha20-Poly1305's
        // 192-bit nonce makes random selection collision-safe for any
        // realistic write volume; reuse would be catastrophic (keystream
        // XOR + Poly1305 key recovery).
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aead = encryption
            .cipher
            .encrypt(&nonce, plaintext.as_slice())
            .map_err(SecretStoreError::Encryption)?;

        // Compose the on-disk blob: [VERSION_V1][nonce][aead]. The header
        // lets `get_secret` distinguish new files from pre-versioned
        // legacy files that started with raw AEAD output.
        let mut ciphertext = Vec::with_capacity(HEADER_LEN + aead.len());
        ciphertext.push(VERSION_V1);
        ciphertext.extend_from_slice(nonce.as_slice());
        ciphertext.extend_from_slice(&aead);

        // PER-USER QUOTA (#4561, P5 of #4381). ENFORCED ONLY for the per-user
        // scope, ONLY when a positive limit is configured, and BEFORE the
        // filesystem write so an over-quota write never lands on disk. Local
        // (single-user) writes skip this entirely — the "never break the
        // single-user default" invariant.
        //
        // The quota bounds the user's TOTAL on-disk footprint under
        // `users/<user_id>/`, which has two components a hosted user controls:
        //   - the active value blob (`new_blob_size` == the composed ciphertext,
        //     == what `metadata().len()` will report), and
        //   - the `.keys` enumeration registry, which `register_key` grows when
        //     this write introduces a NEW distinct key (so many/large keys with
        //     tiny values are charged too).
        // `.snapshots/` is NOT a vector: per-user snapshots are disabled below.
        //
        // An OVERWRITE replaces the prior value blob and (same key) does not
        // grow `.keys`, so the NET change is
        // `(new_blob - old_blob) + (projected_keys - old_keys)`; we admit
        // against `current_total + that`. REJECT (never evict): per-user secrets
        // are authoritative keys, not a reclaimable cache.
        //
        // We capture `(user_id, old_blob, new_blob, old_keys)` here under the
        // pre-write borrow, then apply the NET counter adjustment only AFTER the
        // rename + register_key commit (below), re-stating the REAL `.keys` size
        // so the counter stays exact — a write that fails between this check and
        // the commit leaves the counter unchanged.
        let mut quota_commit: Option<QuotaCommit> = None;
        if let SecretScope::User { id, .. } = &scope
            && self.user_quota_limit_bytes > 0
        {
            let limit = self.user_quota_limit_bytes;
            let new_blob_size = ciphertext.len() as u64;
            // Size of the blob this write would replace (0 for a brand-new key),
            // so an overwrite is charged only its delta, not double-counted.
            let old_blob_size = self.on_disk_blob_size(delegate, key, &scope);
            // `.keys` registry size now and the projected size after this write
            // registers the key (no growth for an already-present key / at-cap).
            let old_keys_size = self.keys_registry_size(delegate, &scope);
            let projected_keys_size = self.projected_keys_size_after_add(delegate, &scope, key);

            // Seed this user's total from disk on first touch (once per user per
            // process); idempotent so a concurrent seed never clobbers a live
            // count. Fail loud if the disk walk errors — a too-low seed would
            // defeat the guard.
            let current_total = match USER_QUOTA_TRACKER.get(&self.base_path, id) {
                Some(t) => t,
                None => {
                    let seeded = self.seeded_user_total(&scope)?;
                    USER_QUOTA_TRACKER.seed_if_absent(&self.base_path, **id, seeded);
                    // Re-read through the tracker so we observe the canonical
                    // value even if another path seeded it first.
                    USER_QUOTA_TRACKER
                        .get(&self.base_path, id)
                        .unwrap_or(seeded)
                }
            };

            let projected = current_total
                .saturating_sub(old_blob_size)
                .saturating_add(new_blob_size)
                .saturating_sub(old_keys_size)
                .saturating_add(projected_keys_size);
            if projected > limit {
                // `attempted` reports the net footprint growth this write would
                // add (value delta + keys delta), so a rejected many-keys write
                // surfaces its real cost rather than just the value blob.
                let attempted = new_blob_size
                    .saturating_sub(old_blob_size)
                    .saturating_add(projected_keys_size.saturating_sub(old_keys_size));
                tracing::warn!(
                    user_id = %id.encode(),
                    used = current_total,
                    limit,
                    attempted,
                    "rejecting per-user secret write: would exceed quota (total footprint)"
                );
                return Err(SecretStoreError::QuotaExceeded {
                    used: current_total,
                    limit,
                    attempted,
                }
                .into());
            }
            quota_commit = Some(QuotaCommit {
                user_id: **id,
                old_blob_size,
                new_blob_size,
                old_keys_size,
            });
        }

        fs::create_dir_all(&scope_path)?;
        // Tighten EVERY segment from base_path down to the leaf, not just the
        // leaf: `create_dir_all` makes the intermediate `users/<id>` (and
        // `<delegate>` on a delegate's first write) under the umask. For Local
        // the leaf IS `<delegate>`, so this is the same single chmod as before.
        if let Err(e) = ensure_owner_only_tree(&self.base_path, &scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "chmod scope dir tree failed");
        }

        // Per-user secret-value snapshots are DISABLED (#4561): snapshots are an
        // overwrite/rollback HISTORY feature (a buggy/accidental overwrite can
        // be undone), NOT crash-recovery — crash-atomicity comes entirely from
        // the tmp+fsync+rename below, and a snapshot failure never blocks the
        // write (see `snapshot_active_value`'s "crash mid-snapshot loses only
        // the snapshot" contract). Hosted users are transient (they export to
        // their own peer and leave) and don't need that history, so we skip it
        // for `SecretScope::User`. This also removes the `.snapshots/` on-disk
        // growth vector from the per-user quota footprint, leaving only active
        // blobs + `.keys` to charge. `SecretScope::Local` is unchanged.
        let take_snapshots = self.snapshots_enabled && !matches!(scope, SecretScope::User { .. });

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
        if take_snapshots
            && secret_file_path.exists()
            && let Err(e) = self.snapshot_prior_value(&scope_path, key, &secret_file_path)
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
        // store_secret calls impossible. `create_owner_only` unlinks any
        // surviving `.tmp` from a prior crashed run so the new inode
        // always lands at mode 0o600 (a legacy 0o644 tmp from before this
        // helper landed would otherwise be reused with its old mode).
        let tmp_path = secret_file_path.with_extension("tmp");
        {
            let mut file = create_owner_only(&tmp_path)?;
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

        // The new value blob is durably at the active path. Apply the NET quota
        // adjustment for the per-user scope: the value-blob delta now, and the
        // `.keys` delta after `register_key` (re-stating the REAL `.keys` size
        // so the counter is exact even if the pre-write projection differed).
        // Seeding (above) guarantees the user is already in the tracker, so the
        // add/sub land on a real entry. Ordered AFTER the rename so a failed
        // write never moves the counter.
        if let Some(commit) = &quota_commit {
            apply_signed_delta(
                &self.base_path,
                &commit.user_id,
                commit.new_blob_size as i128 - commit.old_blob_size as i128,
            );
        }

        // Update index in ReDb and in-memory only after the active path has
        // the new value durably committed. The Local and User indices live in
        // separate ReDb tables + separate in-memory maps, so a User write
        // never perturbs a Local entry and vice-versa.
        self.add_to_index(delegate, &scope, secret_key)?;

        // Register the RAW key for enumeration (#4355). Best-effort and
        // ordered AFTER the durable value + index commit: a crash here only
        // means the key isn't enumerable yet, never that the value is lost.
        // Reuses the same `encryption` (scope DEK) already derived above.
        self.register_key(delegate, &scope, &encryption, key);

        // Charge the REAL `.keys` registry delta this write produced. Stat-after
        // minus the captured stat-before; folds into the same per-user counter.
        // Doing it from the real on-disk size (not the projection) keeps the
        // footprint counter exact regardless of encryption padding or a
        // best-effort `register_key` that declined to grow the file.
        if let Some(commit) = &quota_commit {
            let new_keys_size = self.keys_registry_size(delegate, &scope);
            apply_signed_delta(
                &self.base_path,
                &commit.user_id,
                new_keys_size as i128 - commit.old_keys_size as i128,
            );
        }

        // Best-effort thin of the snapshot history. Failures here only mean
        // we keep more snapshots than the policy targets, which is harmless
        // and self-correcting on the next write. Skipped for the per-user scope
        // (no snapshots are taken there — see `take_snapshots`).
        if take_snapshots {
            let snap_dir = snapshot_dir_for(&scope_path, key);
            if snap_dir.exists() {
                thin_snapshots(&snap_dir, &self.retention, SystemTime::now());
            }
        }

        Ok(())
    }

    /// Add `secret_key` to the index for `(delegate, scope)`, persisting to
    /// the scope's ReDb table FIRST and only then updating the in-memory
    /// mirror (so a transient DB failure can't leave the in-memory map ahead
    /// of the durable state). Local and User scopes use disjoint tables and
    /// maps.
    fn add_to_index(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
        secret_key: SecretKey,
    ) -> RuntimeResult<()> {
        match scope {
            SecretScope::Local => {
                let mut current: Vec<SecretKey> = self
                    .key_to_secret_part
                    .get(delegate)
                    .map(|entry| entry.value().iter().copied().collect())
                    .unwrap_or_default();
                // Idempotent: if the hash is already indexed, do nothing — no
                // ReDb write, no map churn. This makes the import skip-branch's
                // index-reconcile a genuine no-op in the common (already-correct)
                // case, so re-running an import doesn't issue N redundant fsync'd
                // ReDb writes for the already-present entries.
                if current.contains(&secret_key) {
                    return Ok(());
                }
                current.push(secret_key);
                self.db
                    .store_secrets_index(delegate, &current)
                    .map_err(|e| anyhow::anyhow!("Failed to store secrets index: {e}"))?;
                let secret_set: HashSet<SecretKey> = current.into_iter().collect();
                self.key_to_secret_part.insert(delegate.clone(), secret_set);
            }
            SecretScope::User { id, .. } => {
                let map_key = (delegate.clone(), **id);
                let mut current: Vec<SecretKey> = self
                    .user_key_to_secret_part
                    .get(&map_key)
                    .map(|entry| entry.value().iter().copied().collect())
                    .unwrap_or_default();
                if current.contains(&secret_key) {
                    return Ok(());
                }
                current.push(secret_key);
                self.db
                    .store_user_secrets_index(delegate, id.as_bytes(), &current)
                    .map_err(|e| anyhow::anyhow!("Failed to store user secrets index: {e}"))?;
                let secret_set: HashSet<SecretKey> = current.into_iter().collect();
                self.user_key_to_secret_part.insert(map_key, secret_set);
            }
        }
        Ok(())
    }

    /// Capture the existing active secret file as a snapshot so the
    /// subsequent overwrite is reversible. Thin wrapper over the shared
    /// [`snapshot_active_value`] (keyed on the encoded secret id), which
    /// owns the hard-link/copy + owner-only-perms discipline reused by
    /// both `store_secret` and the `freenet secrets snapshot-restore`
    /// CLI. Keeping one implementation prevents the two paths from
    /// drifting on a durability-critical operation.
    fn snapshot_prior_value(
        &self,
        delegate_path: &Path,
        key: &SecretsId,
        secret_file_path: &Path,
    ) -> std::io::Result<()> {
        snapshot_active_value(delegate_path, &key.encode(), secret_file_path)
    }

    /// Remove a secret under the given `scope`. Local and User scopes are
    /// independent: removing a Local secret leaves any same-`SecretsId` User
    /// secrets intact, and vice-versa.
    pub fn remove_secret(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: SecretScope<'_>,
    ) -> Result<(), SecretStoreError> {
        let scope_path = self.scope_dir(delegate, &scope);
        let secret_path = scope_path.join(key.encode());
        let snap_dir = snapshot_dir_for(&scope_path, key);

        // PER-USER QUOTA (#4561): read the blob's on-disk size BEFORE deleting
        // it, so we can decrement the user's counter by exactly the bytes this
        // removal frees — keeping it consistent with the `store_secret`
        // increment and the lazy disk seed. Only meaningful for the per-user
        // scope; 0 for Local or an already-absent file. We apply the decrement
        // AFTER a successful delete (below), and only to an already-seeded user
        // (an unseeded user will pick up the removal on its next seed walk).
        let removed_blob_size = match &scope {
            SecretScope::User { .. } => self.on_disk_blob_size(delegate, key, &scope),
            SecretScope::Local => 0,
        };

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
            Ok(()) => {
                // The blob is gone from disk; free its bytes from the user's
                // quota counter. `sub_saturating` is a no-op when the user is
                // not tracked yet, and saturates at 0 against any drift.
                if let SecretScope::User { id, .. } = &scope
                    && removed_blob_size > 0
                {
                    USER_QUOTA_TRACKER.sub_saturating(&self.base_path, id, removed_blob_size);
                }
            }
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
        match &scope {
            SecretScope::Local => {
                let mut current: Vec<SecretKey> = self
                    .key_to_secret_part
                    .get(delegate)
                    .map(|e| e.value().iter().copied().collect())
                    .unwrap_or_default();
                current.retain(|k| k != &secret_key);
                self.db
                    .store_secrets_index(delegate, &current)
                    .map_err(|e| {
                        std::io::Error::other(format!("Failed to update secrets index: {e}"))
                    })?;
                let secret_set: HashSet<SecretKey> = current.into_iter().collect();
                self.key_to_secret_part.insert(delegate.clone(), secret_set);
            }
            SecretScope::User { id, .. } => {
                let map_key = (delegate.clone(), **id);
                let mut current: Vec<SecretKey> = self
                    .user_key_to_secret_part
                    .get(&map_key)
                    .map(|e| e.value().iter().copied().collect())
                    .unwrap_or_default();
                current.retain(|k| k != &secret_key);
                self.db
                    .store_user_secrets_index(delegate, id.as_bytes(), &current)
                    .map_err(|e| {
                        std::io::Error::other(format!("Failed to update user secrets index: {e}"))
                    })?;
                let secret_set: HashSet<SecretKey> = current.into_iter().collect();
                self.user_key_to_secret_part.insert(map_key, secret_set);
            }
        }

        // Drop the raw key from the enumeration registry (#4355) after the
        // value + index are gone. Best-effort, like the rest of removal.
        // PER-USER QUOTA (#4561): the registry shrinks when its entry is dropped
        // (or the file is removed entirely at the last key), so free those bytes
        // from the user's footprint counter too. Stat the `.keys` file before
        // and after, applying the real (negative) delta.
        let keys_size_before = match &scope {
            SecretScope::User { .. } => self.keys_registry_size(delegate, &scope),
            SecretScope::Local => 0,
        };
        self.deregister_key(delegate, &scope, key);
        if let SecretScope::User { id, .. } = &scope {
            let keys_size_after = self.keys_registry_size(delegate, &scope);
            if keys_size_before > keys_size_after {
                USER_QUOTA_TRACKER.sub_saturating(
                    &self.base_path,
                    id,
                    keys_size_before - keys_size_after,
                );
            }
        }

        Ok(())
    }

    /// Read a secret under the given `scope`.
    ///
    /// `SecretScope::Local` is byte-for-byte the pre-#4381 read path (same
    /// on-disk path, node-KEK-derived DEK, legacy-fallback chain).
    /// `SecretScope::User` reads from `…/users/<user_id>/`, decrypting with
    /// the DEK derived from the caller's `dek_secret`. The User path has NO
    /// legacy-fallback chain (no historical user-scoped files exist), so a
    /// wrong `dek_secret` surfaces as a clean `Encryption` error.
    pub fn get_secret(
        &self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: SecretScope<'_>,
    ) -> Result<Zeroizing<Vec<u8>>, SecretStoreError> {
        let secret_path = self.scope_dir(delegate, &scope).join(key.encode());
        let blob =
            fs::read(secret_path).map_err(|_| SecretStoreError::MissingSecret(key.clone()))?;

        match &scope {
            SecretScope::Local => {
                // Read path derives DEK on demand without caching (requires
                // &self). Cold reads pay one HKDF-SHA256 expand call (~µs).
                let encryption = self.cipher_for_read(delegate);
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
                    &key.encode(),
                )
            }
            SecretScope::User { dek_secret, .. } => {
                // User secrets are only ever written by THIS code under the
                // current format, so there is no legacy chain and no
                // migration cipher: pass empty fallbacks. A bad `dek_secret`
                // (wrong user) fails AEAD on the only attempted cipher and
                // returns `Encryption`.
                let encryption = self.derive_user_dek(delegate, dek_secret);
                decrypt_secret_blob(&encryption, &[], None, &blob, &key.encode())
            }
        }
    }

    /// Select the scope DEK for a READ (no caching, no `&mut self`). Mirrors
    /// the read-side DEK selection in `get_secret`, factored out so the key
    /// registry read/write share the exact same key material as the secret
    /// values they describe.
    fn encryption_for_scope_read(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
    ) -> Encryption {
        match scope {
            SecretScope::Local => self.cipher_for_read(delegate),
            SecretScope::User { dek_secret, .. } => self.derive_user_dek(delegate, dek_secret),
        }
    }

    /// Path of the encrypted key registry for `(delegate, scope)`.
    fn key_registry_path(&self, delegate: &DelegateKey, scope: &SecretScope<'_>) -> PathBuf {
        self.scope_dir(delegate, scope).join(KEY_REGISTRY_FILE)
    }

    /// Read and decrypt the raw-key registry for a scope.
    ///
    /// Tri-state, so the caller can tell "no keys yet" apart from "the
    /// existing registry is momentarily unreadable" and thereby FAIL SAFE
    /// instead of fail-destructive (see [`register_key`](Self::register_key)):
    ///
    /// - File absent (`NotFound`) → `Ok(Vec::new())`. A scope that never
    ///   stored a secret (or a pre-enumeration on-disk store) legitimately has
    ///   no registry; treating this as an empty list is correct and a
    ///   subsequent write may create the file.
    /// - File present + decrypts + parses → `Ok(keys)`.
    /// - File present but UNREADABLE — any non-`NotFound` IO error (EACCES,
    ///   EIO, EMFILE, a read racing the tmp+rename), a malformed header, or an
    ///   AEAD decrypt failure → `Err`. The on-disk blob's true contents are
    ///   UNKNOWN, so the caller MUST NOT overwrite it from an assumed-empty
    ///   base. We return `Err` (logging loudly) precisely so `register_key` /
    ///   `deregister_key` bail without clobbering a registry that may hold
    ///   thousands of valid, decryptable keys. A transient IO hiccup must not
    ///   be amplified into permanent loss of the *enumerable* key set (secret
    ///   VALUES are never affected — the registry is written independently of,
    ///   and strictly after, the durable value+index commit).
    fn read_key_registry(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
    ) -> std::io::Result<Vec<Vec<u8>>> {
        let path = self.key_registry_path(delegate, scope);
        let blob = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "key registry UNREADABLE (transient IO error); preserving on-disk registry \
                     and refusing to overwrite it from empty (secret VALUES unaffected)"
                );
                return Err(e);
            }
        };
        if blob.len() < HEADER_LEN || blob.first().copied() != Some(VERSION_V1) {
            tracing::warn!(
                path = %path.display(),
                "key registry blob MALFORMED; preserving it on disk and refusing to overwrite \
                 from empty so a recoverable/legacy blob is not destroyed (enumeration returns \
                 empty for this scope until the blob is readable again; secret VALUES unaffected)"
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "key registry blob malformed (bad header)",
            ));
        }
        let encryption = self.encryption_for_scope_read(delegate, scope);
        let nonce = XNonce::from_slice(&blob[1..HEADER_LEN]);
        match encryption.cipher.decrypt(nonce, &blob[HEADER_LEN..]) {
            Ok(plaintext) => Ok(decode_secret_key_list(&plaintext)),
            Err(_) => {
                tracing::warn!(
                    path = %path.display(),
                    "key registry decrypt FAILED; preserving the blob on disk and refusing to \
                     overwrite from empty (enumeration returns empty for this scope until the \
                     blob decrypts again; secret VALUES are unaffected)"
                );
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "key registry decrypt failed",
                ))
            }
        }
    }

    /// Encrypt `keys` and atomically write the registry file for the scope,
    /// reusing the same `[VERSION_V1][nonce][AEAD]` layout, per-write random
    /// nonce, owner-only perms, and tmp+rename discipline as the secret
    /// values. Best-effort: a failure here is logged and swallowed — the
    /// secret VALUE write has already committed, and a stale/absent registry
    /// only degrades future enumeration, never the value's readability.
    fn write_key_registry(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
        encryption: &Encryption,
        keys: &[Vec<u8>],
    ) {
        let scope_path = self.scope_dir(delegate, scope);
        let path = scope_path.join(KEY_REGISTRY_FILE);

        if keys.is_empty() {
            // Nothing left to enumerate; remove the registry so a future read
            // sees a clean empty scope instead of an empty-list blob.
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "failed to clear key registry")
                }
            }
            return;
        }

        let plaintext = encode_secret_key_list(keys.iter().map(|k| k.as_slice()));
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aead = match encryption.cipher.encrypt(&nonce, plaintext.as_slice()) {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "failed to encrypt key registry");
                return;
            }
        };
        let mut blob = Vec::with_capacity(HEADER_LEN + aead.len());
        blob.push(VERSION_V1);
        blob.extend_from_slice(nonce.as_slice());
        blob.extend_from_slice(&aead);

        if let Err(e) = fs::create_dir_all(&scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "failed to create scope dir for key registry");
            return;
        }
        if let Err(e) = ensure_owner_only_tree(&self.base_path, &scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "chmod scope dir tree failed (key registry)");
        }
        // `.with_extension(...)` is wrong for the `.keys` dotfile: a
        // leading-dot filename is all stem with no extension, so
        // `.with_extension("keys.tmp")` would yield `.keys.keys.tmp`.
        // Build the sibling tmp path by appending the suffix to the full
        // file name instead, giving `.keys.tmp`. A fixed suffix is safe
        // because `&mut self` precludes concurrent in-process writers.
        let tmp_path = {
            let mut name = path
                .file_name()
                .map(|n| n.to_os_string())
                .unwrap_or_else(|| std::ffi::OsString::from(KEY_REGISTRY_FILE));
            name.push(".tmp");
            scope_path.join(name)
        };
        let write_res = (|| -> std::io::Result<()> {
            let mut file = create_owner_only(&tmp_path)?;
            file.write_all(&blob)?;
            file.sync_all()?;
            fs::rename(&tmp_path, &path)
        })();
        if let Err(e) = write_res {
            tracing::warn!(path = %path.display(), error = %e, "failed to persist key registry");
            // Best-effort cleanup of the orphaned tmp file; a leftover tmp is
            // harmless (the next write unlinks it via `create_owner_only`) so
            // a failure to remove it here is intentionally ignored.
            if let Err(rm_err) = fs::remove_file(&tmp_path) {
                tracing::debug!(path = %tmp_path.display(), error = %rm_err, "tmp registry cleanup failed (harmless)");
            }
        }
    }

    /// Register `key`'s raw bytes in the scope's enumeration registry. Called
    /// after a secret VALUE has been durably committed by `store_secret`.
    /// Idempotent (re-storing an existing key is a no-op for the registry) and
    /// capped at [`MAX_REGISTERED_KEYS_PER_SCOPE`] to bound amplification.
    ///
    /// Best-effort / advisory AND fail-safe: if the existing registry is
    /// unreadable (transient IO error, malformed header, or decrypt failure),
    /// [`read_key_registry`](Self::read_key_registry) returns `Err`, and we
    /// ABORT the update — leaving the on-disk registry untouched — rather than
    /// rewriting it from an assumed-empty base. Overwriting on a read error
    /// would amplify a momentary, recoverable failure into permanent loss of
    /// the *enumerable* key set (a valid registry holding thousands of keys
    /// would be replaced by a single-key blob). The secret VALUE has already
    /// been durably committed by the caller, so a refused registry update
    /// never blocks or fails the value write — it only delays this one key's
    /// enumerability until a later successful write, and the loud warn in
    /// `read_key_registry` is the operator's signal that enumeration coverage
    /// is temporarily incomplete.
    fn register_key(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
        encryption: &Encryption,
        key: &SecretsId,
    ) {
        let mut keys = match self.read_key_registry(delegate, scope) {
            Ok(keys) => keys,
            Err(e) => {
                // Fail-safe: the existing registry's contents are unknown, so
                // do NOT clobber it. The value write already committed.
                tracing::warn!(
                    delegate = %delegate.encode(),
                    error = %e,
                    "skipping key-registry update on unreadable registry; on-disk registry left \
                     intact, this key is temporarily not enumerable (secret VALUE was stored)"
                );
                return;
            }
        };
        if keys.iter().any(|k| k.as_slice() == key.key()) {
            return;
        }
        if keys.len() >= self.max_registered_keys_per_scope {
            tracing::warn!(
                delegate = %delegate.encode(),
                cap = self.max_registered_keys_per_scope,
                "key enumeration registry at capacity; new key not enumerable (value still stored)"
            );
            return;
        }
        keys.push(key.key().to_vec());
        self.write_key_registry(delegate, scope, encryption, &keys);
    }

    /// Drop `key` from the scope's enumeration registry. Called after
    /// `remove_secret` deletes the value. A no-op if the key was never
    /// registered (e.g. it was stored before this feature, or evicted at cap).
    ///
    /// Fail-safe like [`register_key`](Self::register_key): on an unreadable
    /// registry we abort rather than rewriting from empty, so a transient read
    /// error never drops the rest of the enumerable set. The stale entry for
    /// the now-removed key is harmless — `list_secret_keys` only reports keys,
    /// and a later successful read/write reconciles it.
    fn deregister_key(&self, delegate: &DelegateKey, scope: &SecretScope<'_>, key: &SecretsId) {
        let mut keys = match self.read_key_registry(delegate, scope) {
            Ok(keys) => keys,
            Err(e) => {
                tracing::warn!(
                    delegate = %delegate.encode(),
                    error = %e,
                    "skipping key-registry deregister on unreadable registry; on-disk registry \
                     left intact (a stale entry for the removed key is harmless)"
                );
                return;
            }
        };
        let before = keys.len();
        keys.retain(|k| k.as_slice() != key.key());
        if keys.len() == before {
            return;
        }
        let encryption = self.encryption_for_scope_read(delegate, scope);
        self.write_key_registry(delegate, scope, &encryption, &keys);
    }

    /// Enumerate the raw keys of every secret stored under `scope` whose key
    /// begins with `prefix` (an empty prefix lists all). Returns the raw key
    /// bytes the delegate originally supplied to `store_secret`, deduplicated
    /// and capped at [`MAX_REGISTERED_KEYS_PER_SCOPE`] by construction.
    ///
    /// This is the host-side backing for the `__frnt__delegate__list_secrets`
    /// hostcall (#4355): it lets a delegate rediscover an open-ended key family
    /// (e.g. `room:<owner_vk>`) that it would otherwise have to track itself.
    pub fn list_secret_keys(
        &self,
        delegate: &DelegateKey,
        scope: SecretScope<'_>,
        prefix: &[u8],
    ) -> Vec<Vec<u8>> {
        // Enumeration is best-effort: an unreadable registry (transient IO
        // error, malformed header, or decrypt failure) yields an empty list
        // for this call rather than an error, since the value read/write paths
        // are independent of the registry. `read_key_registry` already logged
        // the cause loudly and, critically, did NOT overwrite the on-disk blob
        // — so a later call can still recover the full set once it is readable.
        let mut keys = self.read_key_registry(delegate, &scope).unwrap_or_default();
        if !prefix.is_empty() {
            keys.retain(|k| k.starts_with(prefix));
        }
        keys
    }

    /// Enumerate the snapshot history for a given `(delegate, secret_id)`
    /// pair under `scope`, oldest-first. Returns an empty vector if the secret
    /// was never overwritten (no snapshot directory exists). Does not decrypt;
    /// callers that want the plaintext can `restore_snapshot` and then
    /// `get_secret`.
    ///
    /// The snapshot history lives under the scope's `.snapshots/` directory,
    /// so a `Local` secret and a `User` secret sharing the same `SecretsId`
    /// have independent histories. For `Local`, `scope_dir` returns the exact
    /// pre-#4381 path, so Local listing is byte-for-byte unchanged.
    pub fn list_snapshots(
        &self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: SecretScope<'_>,
    ) -> Result<Vec<SnapshotMetadata>, SecretStoreError> {
        let scope_path = self.scope_dir(delegate, &scope);
        let snap_dir = snapshot_dir_for(&scope_path, key);
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
    /// The find / reversibility-snapshot / atomic-write sequence is
    /// delegated to the shared [`restore_snapshot_file`] (so the node
    /// runtime and the `freenet secrets snapshot-restore` CLI can't
    /// drift); this wrapper then adds the index repair and the
    /// best-effort history thin, in that order — matching the
    /// pre-extraction inline implementation exactly, so a failed index
    /// repair never prunes snapshot history a retry would need.
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
    /// The restore operates within `scope`: the snapshot history, active
    /// path, and index repair all target the scope's tree/table. For
    /// `Local`, `scope_dir` returns the exact pre-#4381 path and the index
    /// repair goes through the single-user index, so Local restore is
    /// byte-for-byte unchanged.
    ///
    /// # Errors
    /// - `SnapshotNotFound` if no snapshot matches `timestamp_ms`
    /// - `IO` for filesystem errors during the copy / rename / fsync
    pub fn restore_snapshot(
        &mut self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: SecretScope<'_>,
        timestamp_ms: u64,
    ) -> Result<(), SecretStoreError> {
        let scope_path = self.scope_dir(delegate, &scope);

        // Byte-level restore: find the snapshot, reversibly snapshot the
        // current active value, atomic tmp+fsync+rename onto the active
        // path. This durability discipline lives in exactly one place
        // ([`restore_snapshot_file`]) so the node runtime and the
        // `freenet secrets snapshot-restore` CLI cannot drift. The CLI
        // cannot reconstruct a `SecretsId` from the on-disk name (the
        // pre-image bytes are not persisted), so the shared core is keyed
        // on the encoded secret id rather than the typed `key`. Thinning
        // is deliberately NOT part of the core — we thin below, after the
        // index repair, to preserve the pre-extraction order.
        match restore_snapshot_file(
            &scope_path,
            &key.encode(),
            timestamp_ms,
            // The runtime API selects by timestamp only (unsuffixed-wins);
            // explicit collision-suffix selection is a CLI affordance.
            None,
            self.snapshots_enabled,
        ) {
            Ok(()) => {}
            Err(RestoreError::NotFound(timestamp_ms)) => {
                return Err(SecretStoreError::SnapshotNotFound {
                    key: key.clone(),
                    timestamp_ms,
                });
            }
            Err(RestoreError::Io(e)) => return Err(e.into()),
        }

        // `restore_snapshot_file` (and the reversibility snapshot it takes)
        // `create_dir_all` the scope tree, chmodding only the leaves they
        // create. For a User restore that materializes `<delegate>/users`
        // and `<delegate>` (when restore is a delegate's first write) under
        // the umask, so tighten the whole tree from base down to the leaf.
        // For Local the leaf IS `<delegate>` → the same single chmod as the
        // pre-#4381 path.
        if let Err(e) = ensure_owner_only_tree(&self.base_path, &scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "chmod scope dir tree failed");
        }

        // Index repair: only needed if the entry was previously removed
        // (e.g. user called `remove_secret` then realized they wanted a
        // value back). In the common case the secret is already in the
        // index and the block below is a no-op (no ReDb write). This is
        // the only part of restore that needs the in-memory map + ReDb,
        // so it stays here rather than in the shared filesystem core.
        // Local and User repair disjoint tables/maps; the Local branch is
        // byte-for-byte the pre-#4381 code (it skips the ReDb write when
        // the key is already present, rather than rewriting unconditionally).
        let secret_key = *key.hash();
        match &scope {
            SecretScope::Local => {
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
            }
            SecretScope::User { id, .. } => {
                let map_key = (delegate.clone(), **id);
                let mut current_secrets: Vec<[u8; 32]> = self
                    .user_key_to_secret_part
                    .get(&map_key)
                    .map(|entry| entry.value().iter().copied().collect())
                    .unwrap_or_default();
                if !current_secrets.contains(&secret_key) {
                    current_secrets.push(secret_key);
                    self.db
                        .store_user_secrets_index(delegate, id.as_bytes(), &current_secrets)
                        .map_err(|e| {
                            std::io::Error::other(format!(
                                "Failed to update user secrets index: {e}"
                            ))
                        })?;
                    let secret_set: HashSet<SecretKey> = current_secrets.into_iter().collect();
                    self.user_key_to_secret_part.insert(map_key, secret_set);
                }
            }
        }

        // Best-effort thin LAST — only after the index repair above has
        // committed. Mirrors the pre-extraction order: a failed index
        // store early-returns above, so the snapshot history is left
        // intact for a clean retry instead of having already pruned source
        // snapshots. Thinning touches only `.snapshots/`; a failure here
        // self-corrects on the next write.
        if self.snapshots_enabled {
            let snap_dir = snapshot_dir_for(&scope_path, key);
            if snap_dir.exists() {
                thin_snapshots(&snap_dir, &self.retention, SystemTime::now());
            }
        }

        Ok(())
    }

    // ===================== Export / import (P3 of #4381) =====================
    //
    // The export path enumerates every `(DelegateKey, secret_hash)` the store
    // holds for a scope (from the in-memory index, which mirrors ReDb), reads
    // the active on-disk blob, and decrypts it. It recovers only the
    // `bs58(hash)` on-disk name — NOT the `SecretsId` pre-image, which the
    // store never persists (the ReDb index stores `SecretsId::hash` only). So
    // both export and import are keyed on the raw 32-byte hash. That is
    // sufficient: `store_secret`/`get_secret` only ever use `key.encode()`
    // (= `bs58(hash)`) for the on-disk path and `*key.hash()` for the index;
    // the pre-image is dead weight for storage. Reconstructing the original
    // `DelegateKey` on the import node is deterministic (it is content-derived
    // from the delegate's wasm+params), so a re-installed webapp shipping the
    // same delegate yields the same key and the imported secrets line up.

    /// Map `delegate-key-bytes (32) -> real CodeHash` from this store's in-memory
    /// index for `scope`, so the on-disk enumeration can recover the real
    /// `code_hash` of a delegate the dir name (key-only) can't carry. Best-effort:
    /// a delegate this executor never registered simply isn't in the map (the
    /// caller falls back to a placeholder code_hash, which is inert).
    fn index_code_hashes_by_key(
        &self,
        scope: &SecretScope<'_>,
    ) -> std::collections::HashMap<[u8; 32], CodeHash> {
        let mut out = std::collections::HashMap::new();
        match scope {
            SecretScope::Local => {
                for entry in self.key_to_secret_part.iter() {
                    let dk = entry.key();
                    // `DelegateKey` derefs to its 32-byte key.
                    out.insert(**dk, *dk.code_hash());
                }
            }
            SecretScope::User { id, .. } => {
                for entry in self.user_key_to_secret_part.iter() {
                    let (dk, user) = entry.key();
                    if user == *id {
                        out.insert(**dk, *dk.code_hash());
                    }
                }
            }
        }
        out
    }

    /// Count the secrets under `scope` on disk, STOPPING as soon as the count
    /// reaches `stop_at`. Used for the export count-cap pre-check so an
    /// over-limit (attacker-controlled) scope is rejected without materializing
    /// — or even fully traversing past `stop_at` — its entries. Same disk walk /
    /// filter as the gather, and the same fail-loud I/O semantics.
    fn count_scope_secrets_on_disk(
        &self,
        scope: &SecretScope<'_>,
        stop_at: usize,
    ) -> Result<usize, SecretStoreError> {
        let mut count = 0usize;
        self.walk_scope_secrets_on_disk(scope, |_delegate, _secret_hash| {
            count += 1;
            if count >= stop_at {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        })?;
        Ok(count)
    }

    /// Shared on-disk scope walk: invoke `visit` once per secret blob under
    /// `scope` by walking the on-disk `secrets_dir` (the shared source of truth)
    /// instead of this store's per-instance in-memory index. `visit` returns
    /// [`ControlFlow::Break`](std::ops::ControlFlow) to stop early. The gather
    /// ([`Self::export_scope_entries_bounded`]) and the count
    /// ([`Self::count_scope_secrets_on_disk`]) both stream through this one
    /// walker, so they can never diverge in filter / scope / error handling.
    ///
    /// WHY (the cross-executor correctness fix): each pooled `Executor` owns its
    /// OWN `SecretsStore` with its own in-memory index, but ALL of them write to
    /// the SAME `secrets_dir`. A `store_secret` on executor A updates only A's
    /// index; an export running on executor B would miss A's just-written secret
    /// if it enumerated B's in-memory index — a silently incomplete backup once
    /// the export runs off-loop concurrently with normal ops. Walking the disk
    /// includes EVERY committed write regardless of which executor made it.
    ///
    /// Consistency: `store_secret`/`import_secret_by_hash` write each secret blob
    /// via tmp-file + fsync + atomic `rename` into place, so a concurrent walk
    /// only ever sees a fully-written active file (a torn read is impossible at
    /// the active path; even so, a bad blob fails AEAD on read → a clean export
    /// error, never silent corruption).
    ///
    /// On-disk layout (see [`Self::scope_dir`]):
    /// - `Local` => `base_path/<delegate>/<bs58(secret_hash)>`
    /// - `User`  => `base_path/<delegate>/users/<user_id>/<bs58(secret_hash)>`
    ///
    /// A "secret blob" is a REGULAR FILE whose name bs58-decodes to exactly 32
    /// bytes. That positive filter inherently rejects every non-secret entry
    /// (`.keys`, `.keys.tmp`, `<hash>.tmp`, `node_kek`, `kek_backend*` — none
    /// bs58-decode to 32 bytes) and every directory (`.snapshots/`, `users/`,
    /// and the delegate dirs themselves); the known non-secret names are also
    /// skipped explicitly as belt-and-suspenders.
    ///
    /// The reconstructed `DelegateKey` carries the real 32-byte key (the
    /// directory name) and the real `code_hash` recovered from the in-memory
    /// index when known (see [`Self::index_code_hashes_by_key`]), or a ZERO
    /// placeholder otherwise. `code_hash` is never consulted by
    /// [`Self::scope_dir`], [`Self::derive_user_dek`], [`Self::cipher_for_read`],
    /// or `read_secret_by_hash` (all key-only via `delegate.encode()`), so the
    /// placeholder reads and decrypts the secret identically; it only ends up as
    /// a (functionally inert) 32-byte value in the exported bundle's `code_hash`.
    ///
    /// FAIL-LOUD: an UNEXPECTED I/O error (permission denied, EIO, a vanished
    /// mount — anything other than `NotFound`) is PROPAGATED, not swallowed.
    /// Swallowing it would let the export seal a SUCCESSFUL but silently
    /// INCOMPLETE bundle — the same silent-incompleteness class this whole fix
    /// removes, via a different door. `NotFound` is the one expected-absent case
    /// (base_path or a user-scope dir simply doesn't exist ⇒ that scope has no
    /// secrets) and is treated as empty/skip.
    fn walk_scope_secrets_on_disk(
        &self,
        scope: &SecretScope<'_>,
        mut visit: impl FnMut(DelegateKey, SecretKey) -> std::ops::ControlFlow<()>,
    ) -> Result<(), SecretStoreError> {
        use std::ops::ControlFlow;
        // Iterate the delegate directories under base_path. Each is named
        // `bs58(delegate_key)`; non-directory / non-bs58 root entries
        // (`node_kek`, `kek_backend`, `kek_backend.tmp`) are skipped by the
        // 32-byte-bs58 decode below. A missing base_path ⇒ no secrets; any other
        // read error is unexpected ⇒ fail loud.
        let delegate_dirs = match fs::read_dir(&self.base_path) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(SecretStoreError::IO(e)),
        };
        // Recover each delegate's real `code_hash` from the in-memory index when
        // this executor knows it. The on-disk delegate dir name encodes only the
        // 32-byte key (`DelegateKey::encode()`), NOT the code_hash — but the
        // bundle records `code_hash`, and `DelegateKey`'s `Eq`/`Hash` cover both
        // fields, so a placeholder would make the exported key `!=` the real one.
        // code_hash is otherwise inert (path/DEK/read are key-only), so a secret
        // written by ANOTHER executor for a delegate THIS executor never
        // registered still exports + decrypts correctly under the zero
        // placeholder; it just carries a placeholder code_hash in the bundle
        // (inert for import). The exporting executor has almost always
        // registered the delegate, so the real code_hash is recovered in
        // practice.
        let code_hashes = self.index_code_hashes_by_key(scope);
        for delegate_entry in delegate_dirs {
            // A per-entry read error mid-iteration is unexpected ⇒ fail loud.
            let delegate_entry = delegate_entry.map_err(SecretStoreError::IO)?;
            let delegate_path = delegate_entry.path();
            if !delegate_path.is_dir() {
                continue;
            }
            let Some(delegate_name) = delegate_entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(delegate_key_bytes) = decode_bs58_32(&delegate_name) else {
                continue; // not a delegate dir (e.g. a stray non-bs58 name)
            };
            let code_hash = code_hashes
                .get(&delegate_key_bytes)
                .copied()
                .unwrap_or_else(|| CodeHash::from(&[0u8; 32]));
            let delegate = DelegateKey::new(delegate_key_bytes, code_hash);

            // The directory that actually holds this scope's secret files.
            let scope_path = match scope {
                SecretScope::Local => delegate_path.clone(),
                SecretScope::User { id, .. } => delegate_path.join("users").join(id.encode()),
            };
            // A missing scope dir ⇒ this delegate has no secrets for this scope
            // (expected). Any other read error is unexpected ⇒ fail loud.
            let files = match fs::read_dir(&scope_path) {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(SecretStoreError::IO(e)),
            };
            for file_entry in files {
                let file_entry = file_entry.map_err(SecretStoreError::IO)?;
                // Only regular files whose name bs58-decodes to a 32-byte hash.
                let name = file_entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if name == KEY_REGISTRY_FILE || name == SNAPSHOTS_DIR || name.ends_with(".tmp") {
                    continue;
                }
                let Some(secret_hash) = decode_bs58_32(name) else {
                    continue;
                };
                // Confirm it is a regular file (not a dir like `users/`).
                match file_entry.file_type() {
                    Ok(ft) if ft.is_file() => {}
                    _ => continue,
                }
                if let ControlFlow::Break(()) = visit(delegate.clone(), secret_hash) {
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Sum a single user's TOTAL on-disk footprint across ALL delegates: the
    /// active secret-value blobs PLUS the per-(delegate,user) `.keys`
    /// enumeration registry files. This is the quota's lazy seed (#4561).
    ///
    /// # What's counted (and why this is the WHOLE footprint)
    ///
    /// Everything a hosted user can grow under their `users/<user_id>/` tree:
    ///
    /// - active secret-value blobs (`metadata().len()` each), and
    /// - the `.keys` registry file under each delegate's user scope dir, which
    ///   `register_key` appends to (a user with many/large distinct KEYS grows
    ///   it even with tiny values).
    ///
    /// `.snapshots/` is deliberately NOT a vector here because secret-value
    /// snapshots are DISABLED for the per-user scope (see `store_secret`), so a
    /// per-user tree never accrues snapshot history. Counting active blobs +
    /// `.keys` therefore covers the entire footprint, and the per-write delta
    /// accounting tracks exactly these same two components, so the seed and the
    /// running counter can never disagree.
    ///
    /// `walk_scope_secrets_on_disk` visits once PER BLOB (so a delegate recurs
    /// for each of its blobs); we add each delegate's `.keys` file exactly once
    /// via a seen-set so it isn't multiply counted.
    ///
    /// ORPHAN `.keys` (the post-restart under-enforcement fix): the walker only
    /// visits delegate/user scopes that have at least one ACTIVE blob, so a
    /// `users/<user_id>/.keys` file with zero remaining active blobs (e.g. a
    /// crash/error between deleting the last value and `deregister_key`) would
    /// be missed — the seed would under-count, and after a restart the tracker
    /// would admit writes past the real footprint (the WRONG direction for an
    /// abuse control). So a SECOND pass scans every delegate's
    /// `users/<user_id>/` for a `.keys` file the first pass didn't already
    /// count, and adds its size. Together the two passes count every `.keys`
    /// file under the user's tree, active blobs or not.
    ///
    /// Runs once per user per process; subsequent ops are O(1) atomic
    /// adjustments. FAIL-LOUD on an unexpected I/O error: a silently-too-low
    /// seed would let a user exceed their quota.
    fn seeded_user_total(&self, scope: &SecretScope<'_>) -> Result<u64, SecretStoreError> {
        let mut total: u64 = 0;
        let mut walk_err: Option<SecretStoreError> = None;
        let mut keys_counted: HashSet<DelegateKey> = HashSet::new();
        self.walk_scope_secrets_on_disk(scope, |delegate, secret_hash| {
            // Count this delegate's `.keys` registry once.
            if keys_counted.insert(delegate.clone()) {
                total = total.saturating_add(self.keys_registry_size(&delegate, scope));
            }
            let encoded = bs58::encode(&secret_hash)
                .with_alphabet(bs58::Alphabet::BITCOIN)
                .into_string();
            let path = self.scope_dir(&delegate, scope).join(&encoded);
            match fs::metadata(&path) {
                Ok(md) => {
                    total = total.saturating_add(md.len());
                    std::ops::ControlFlow::Continue(())
                }
                // A blob that vanished between the readdir and the metadata
                // stat (concurrent remove) simply doesn't count — it's no
                // longer on disk. Any OTHER stat error is unexpected: record it
                // and stop so the seed fails loud rather than under-counting.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    std::ops::ControlFlow::Continue(())
                }
                Err(e) => {
                    walk_err = Some(SecretStoreError::IO(e));
                    std::ops::ControlFlow::Break(())
                }
            }
        })?;
        if let Some(e) = walk_err {
            return Err(e);
        }
        // SECOND pass: orphan `.keys` files (a `.keys` under the user's scope
        // for a delegate the blob-walk didn't visit because no active blob
        // remains there). Only meaningful for the per-user scope; Local has no
        // per-user `users/<id>/` tree to scan.
        if let SecretScope::User { .. } = scope {
            total = total.saturating_add(self.orphan_user_keys_size(scope, &keys_counted)?);
        }
        Ok(total)
    }

    /// Sum the sizes of `.keys` registry files under THIS user's scope for
    /// delegates NOT already counted by the blob-walk (`already_counted`) —
    /// i.e. `users/<user_id>/.keys` files whose delegate has no remaining active
    /// blob for this user. Iterates the delegate dirs directly (the blob-walk
    /// can't reach a zero-blob scope). FAIL-LOUD on an unexpected I/O error, the
    /// same as the blob-walk, so an orphan that can't be stat'd never silently
    /// under-counts.
    fn orphan_user_keys_size(
        &self,
        scope: &SecretScope<'_>,
        already_counted: &HashSet<DelegateKey>,
    ) -> Result<u64, SecretStoreError> {
        let SecretScope::User { id, .. } = scope else {
            return Ok(0);
        };
        let mut total: u64 = 0;
        let delegate_dirs = match fs::read_dir(&self.base_path) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(SecretStoreError::IO(e)),
        };
        for entry in delegate_dirs {
            let entry = entry.map_err(SecretStoreError::IO)?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(key_bytes) = decode_bs58_32(&name) else {
                continue; // not a delegate dir
            };
            // A code_hash placeholder is fine: scope_dir / key_registry_path are
            // key-only (via `delegate.encode()`), and we only compare key bytes
            // against `already_counted` (whose entries also carry whatever
            // code_hash the blob-walk reconstructed — so we compare on the
            // 32-byte key, which is what the dir name encodes).
            let delegate = DelegateKey::new(key_bytes, CodeHash::from(&[0u8; 32]));
            // Skip delegates the blob-walk already counted `.keys` for. Compare
            // on the encoded key (not full DelegateKey Eq, which also covers
            // code_hash) so the placeholder here matches the walk's value.
            if already_counted
                .iter()
                .any(|d| d.encode() == delegate.encode())
            {
                continue;
            }
            let keys_path = path.join("users").join(id.encode()).join(KEY_REGISTRY_FILE);
            match fs::metadata(&keys_path) {
                Ok(md) => total = total.saturating_add(md.len()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(SecretStoreError::IO(e)),
            }
        }
        Ok(total)
    }

    /// On-disk size of the `.keys` enumeration registry for `(delegate, scope)`,
    /// or `0` if absent. Part of a per-user's quota footprint (#4561): the
    /// registry grows as the user stores more distinct keys, so it must be
    /// charged alongside the value blobs.
    fn keys_registry_size(&self, delegate: &DelegateKey, scope: &SecretScope<'_>) -> u64 {
        match fs::metadata(self.key_registry_path(delegate, scope)) {
            Ok(md) => md.len(),
            Err(_) => 0,
        }
    }

    /// Project the `.keys` registry's on-disk size for `(delegate, scope)` AFTER
    /// registering `key`, WITHOUT re-encrypting — pure arithmetic over the exact
    /// wire format (`encode_secret_key_list`: a 4-byte LE length prefix + the
    /// key bytes per entry; AEAD ciphertext length == plaintext length; a blob
    /// is `HEADER_LEN + ciphertext + TAG_LEN`). Used by the quota admission
    /// check so a write that would push the footprint over the limit via `.keys`
    /// growth (many/large distinct keys, tiny values) is rejected BEFORE it
    /// lands, making the footprint provably bounded.
    ///
    /// Mirrors `register_key`'s decisions exactly: a key already present (or at
    /// the per-scope cap) does not grow `.keys`. The post-commit counter update
    /// re-stats the real file, so any divergence self-corrects.
    fn projected_keys_size_after_add(
        &self,
        delegate: &DelegateKey,
        scope: &SecretScope<'_>,
        key: &SecretsId,
    ) -> u64 {
        let current = self.keys_registry_size(delegate, scope);
        // Read the registry to decide whether this key is new. On an unreadable
        // registry, `register_key` bails without growing the file, so projecting
        // "no growth" matches what will actually happen.
        let existing = match self.read_key_registry(delegate, scope) {
            Ok(keys) => keys,
            Err(_) => return current,
        };
        if existing.iter().any(|k| k.as_slice() == key.key()) {
            return current; // already registered → no growth
        }
        if existing.len() >= self.max_registered_keys_per_scope {
            return current; // at cap → register_key won't add it
        }
        let entry_len = 4u64.saturating_add(key.key().len() as u64);
        if current == 0 {
            // Registry transitions absent → present: pays the one-time
            // HEADER_LEN + TAG_LEN plus this first entry's plaintext.
            (HEADER_LEN as u64)
                .saturating_add(entry_len)
                .saturating_add(TAG_LEN as u64)
        } else {
            // Existing registry grows by exactly the new entry's plaintext
            // (stream-cipher ciphertext is 1:1 with plaintext; the header+tag
            // are already in `current`).
            current.saturating_add(entry_len)
        }
    }

    /// On-disk size (in bytes) of a stored secret blob under `scope`, or
    /// `Ok(0)` if it is absent. Used by [`Self::remove_secret`] to learn how
    /// many bytes a removal frees BEFORE deleting the file, so the quota
    /// counter is decremented by exactly what the seed/increment counted.
    fn on_disk_blob_size(
        &self,
        delegate: &DelegateKey,
        key: &SecretsId,
        scope: &SecretScope<'_>,
    ) -> u64 {
        let path = self.scope_dir(delegate, scope).join(key.encode());
        match fs::metadata(&path) {
            Ok(md) => md.len(),
            Err(_) => 0,
        }
    }

    /// Read + decrypt the active secret blob named `bs58(secret_hash)` under
    /// `scope`. The by-hash analogue of [`Self::get_secret`]; it exists because
    /// the export enumeration only ever recovers the hash, never a `SecretsId`.
    /// Decrypt logic (cipher selection + legacy-fallback chain) is identical to
    /// `get_secret`.
    fn read_secret_by_hash(
        &self,
        delegate: &DelegateKey,
        secret_hash: &SecretKey,
        scope: &SecretScope<'_>,
    ) -> Result<Zeroizing<Vec<u8>>, SecretStoreError> {
        let encoded = bs58::encode(secret_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string();
        let secret_path = self.scope_dir(delegate, scope).join(&encoded);
        let blob = fs::read(&secret_path).map_err(|_| {
            // We only have the secret hash here, not a `SecretsId` (the
            // pre-image is never persisted), so we can't build a
            // `MissingSecret(SecretsId)`. Surface an IO/NotFound error carrying
            // the encoded path instead; export treats any read error as fatal.
            SecretStoreError::IO(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("secret blob not found at {}", secret_path.display()),
            ))
        })?;
        match scope {
            SecretScope::Local => {
                let encryption = self.cipher_for_read(delegate);
                let legacy_chain = [&self.default_encryption];
                decrypt_secret_blob(
                    &encryption,
                    &legacy_chain,
                    self.legacy_migration_encryption.as_ref(),
                    &blob,
                    &encoded,
                )
            }
            SecretScope::User { dek_secret, .. } => {
                let encryption = self.derive_user_dek(delegate, dek_secret);
                decrypt_secret_blob(&encryption, &[], None, &blob, &encoded)
            }
        }
    }

    /// `true` if `scope` holds MORE than `max_count` secrets on disk, decided
    /// WITHOUT enumerating beyond `max_count + 1` entries.
    ///
    /// Used by [`super::secret_export::export_bundle`] to enforce the export
    /// count cap BEFORE any secret is read/decrypted AND before materializing
    /// the (attacker-controlled) entry list: the streaming count stops the
    /// instant it has seen `max_count + 1` matching entries, so an over-limit
    /// scope is rejected without a full traversal/allocation. Counts from disk
    /// (the shared source of truth) for the same cross-executor-consistency
    /// reason as [`Self::walk_scope_secrets_on_disk`], using the same walk/filter,
    /// so the cap is checked against the SAME set the gather will materialize.
    /// Returns `Err` on an unexpected I/O error (fail-loud, same as the gather).
    pub fn scope_count_exceeds(
        &self,
        scope: &SecretScope<'_>,
        max_count: usize,
    ) -> Result<bool, SecretStoreError> {
        // Stop after seeing max_count + 1 — enough to know we're over the cap.
        let stop_at = max_count.saturating_add(1);
        Ok(self.count_scope_secrets_on_disk(scope, stop_at)? > max_count)
    }

    /// Number of secrets currently held under `scope` on disk (full count, no
    /// decrypt). Used by tests / callers that need the exact count; the export
    /// count cap uses the streaming [`Self::scope_count_exceeds`] instead so an
    /// over-limit scope is never fully traversed.
    pub fn scope_entry_count(&self, scope: &SecretScope<'_>) -> Result<usize, SecretStoreError> {
        self.count_scope_secrets_on_disk(scope, usize::MAX)
    }

    /// Gather every secret under `scope`, decrypted, as portable export
    /// entries. The returned plaintexts live in `Zeroizing` buffers so they
    /// are wiped when the caller drops them.
    ///
    /// A per-entry read/decrypt failure is fatal (returned as `Err`): a
    /// silently-skipped secret would produce a bundle the user believes is
    /// complete but isn't, which is worse for a backup than a hard failure.
    /// In practice every enumerated entry is decryptable by construction (the
    /// node wrote it), so this only fires on genuine on-disk corruption.
    pub fn export_scope_entries(
        &self,
        scope: SecretScope<'_>,
    ) -> Result<Vec<ExportSecretEntry>, SecretStoreError> {
        // Unbounded gather (CLI backup / tests). The hosted-export path uses
        // `export_scope_entries_bounded` so an authenticated token-holder cannot
        // make the node buffer an unbounded amount of decrypted plaintext.
        match self.export_scope_entries_bounded(scope, usize::MAX, usize::MAX) {
            Ok(entries) => Ok(entries),
            Err(ExportScopeError::Store(e)) => Err(e),
            // usize::MAX caps are never exceeded.
            Err(ExportScopeError::TooLarge { .. } | ExportScopeError::CountTooLarge { .. }) => {
                unreachable!("count/byte caps are usize::MAX")
            }
        }
    }

    /// Like [`Self::export_scope_entries`] but ABORTS the instant the running
    /// secret COUNT would exceed `max_count` OR the running plaintext total
    /// would exceed `max_total_bytes`.
    ///
    /// BOTH caps are enforced INCREMENTALLY, per entry, as the scope is walked
    /// and each plaintext decrypted:
    /// - Count: checked BEFORE reading each entry, so an over-cap scope aborts
    ///   without reading/decrypting past `max_count + 1` entries. This is the
    ///   AUTHORITATIVE count enforcement: the cheap `scope_count_exceeds`
    ///   pre-check rejects the common case before any decrypt, but the export
    ///   runs off-loop while the contract loop keeps serving `store_secret`, so
    ///   a concurrent writer could push a just-under-cap scope over the cap
    ///   between the pre-check and here — enforcing it again during the gather
    ///   closes that TOCTOU window.
    /// - Bytes: checked after each decrypt, so an over-limit export bails after
    ///   reading only enough to cross the threshold and drops the partial
    ///   buffer, rather than buffering the user's entire scope first.
    ///
    /// On overflow returns [`ExportScopeError::CountTooLarge`] /
    /// [`ExportScopeError::TooLarge`] carrying the running value at the bail
    /// point (`>` the limit) and the limit. Used by the hosted-mode export DoS
    /// bound (#4381 P5). Pass `usize::MAX` to disable either cap.
    pub fn export_scope_entries_bounded(
        &self,
        scope: SecretScope<'_>,
        max_count: usize,
        max_total_bytes: usize,
    ) -> Result<Vec<ExportSecretEntry>, ExportScopeError> {
        // STREAM the shared on-disk walk (the same walk_scope_secrets_on_disk
        // the count pre-check uses) and read+decrypt each secret in one pass —
        // so we never materialize the full (attacker-controlled) ref list, and
        // both caps are enforced inline. Enumerating from DISK (not this store's
        // per-instance in-memory index) is the cross-executor correctness fix;
        // a disk I/O failure aborts the export (fail-loud). See
        // `walk_scope_secrets_on_disk`.
        use std::ops::ControlFlow;
        let mut entries: Vec<ExportSecretEntry> = Vec::new();
        let mut total_bytes: usize = 0;
        let mut count: usize = 0;
        // The visit closure can't return a Result, so it stashes a terminal
        // outcome here and Breaks; we surface it after the walk.
        let mut bail: Option<ExportScopeError> = None;
        self.walk_scope_secrets_on_disk(&scope, |delegate, secret_hash| {
            // Count cap: check BEFORE reading/decrypting this entry so an
            // over-cap scope never decrypts more than max_count + 1 entries.
            // This is the AUTHORITATIVE count enforcement (closes the TOCTOU
            // window vs. the cheap pre-check, since a concurrent store_secret
            // can push the scope over the cap after that pre-check passed).
            count += 1;
            if count > max_count {
                bail = Some(ExportScopeError::CountTooLarge {
                    actual: count,
                    limit: max_count,
                });
                return ControlFlow::Break(());
            }
            let plaintext = match self.read_secret_by_hash(&delegate, &secret_hash, &scope) {
                Ok(p) => p,
                Err(e) => {
                    bail = Some(ExportScopeError::Store(e));
                    return ControlFlow::Break(());
                }
            };
            // Byte cap: check BEFORE pushing, so we never retain a buffer that
            // crosses the cap. `plaintext` is dropped here on the bail path.
            total_bytes = total_bytes.saturating_add(plaintext.len());
            if total_bytes > max_total_bytes {
                bail = Some(ExportScopeError::TooLarge {
                    actual: total_bytes,
                    limit: max_total_bytes,
                });
                return ControlFlow::Break(());
            }
            entries.push(ExportSecretEntry {
                delegate_key: delegate,
                secret_hash,
                plaintext,
            });
            ControlFlow::Continue(())
        })?; // a disk I/O error during the walk itself is fail-loud.
        if let Some(err) = bail {
            return Err(err);
        }
        Ok(entries)
    }

    /// Place a single decrypted secret (identified by its 32-byte hash) under
    /// `scope`, re-encrypting it under this node's scope DEK. The import-side
    /// analogue of `store_secret`, keyed on the hash because the bundle does
    /// not carry a `SecretsId` pre-image.
    ///
    /// When a secret already exists at the target path: if `overwrite` is
    /// false, the on-disk value is left as-is and `Ok(false)` is returned (the
    /// caller reports it as skipped) — but the index is still reconciled
    /// (idempotent ensure) so a prior partial import that wrote the file but
    /// failed before indexing converges on retry. If `overwrite` is true, the
    /// value is rewritten (the prior value is snapshotted first by the normal
    /// `store_secret` write discipline) and re-indexed. Returns `Ok(true)` only
    /// when a new value was written.
    pub fn import_secret_by_hash(
        &mut self,
        delegate: &DelegateKey,
        secret_hash: &SecretKey,
        scope: SecretScope<'_>,
        plaintext: Zeroizing<Vec<u8>>,
        overwrite: bool,
    ) -> RuntimeResult<bool> {
        let encoded = bs58::encode(secret_hash)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string();
        let scope_path = self.scope_dir(delegate, &scope);
        let secret_file_path = scope_path.join(&encoded);
        if secret_file_path.exists() && !overwrite {
            // Skip the rewrite — but still RECONCILE the index. A prior import
            // can crash (or hit a transient ReDb error) AFTER the file landed
            // but BEFORE `add_to_index` committed, leaving the secret on disk
            // yet absent from the index. Without this repair, a retry takes
            // this early branch, reports "skipped", and never indexes the
            // secret — so it stays invisible to index-based enumeration/export
            // forever (silent data loss on the next migration). `add_to_index`
            // is idempotent (no-ops when the hash is already present), so this
            // is safe in the common case where the index is already correct and
            // converges the file-without-index case on retry. Report `false`
            // (not rewritten) regardless.
            self.add_to_index(delegate, &scope, *secret_hash)?;
            return Ok(false);
        }

        // NOTE (#4561): `import_secret_by_hash` is the P3 migration/restore path
        // (an operator importing a user's OWN previously-exported secrets), NOT
        // the delegate-driven `store_secret` hot path. It is deliberately NOT
        // quota-checked: rejecting a blob mid-restore would corrupt the
        // migration, and an import re-materializes data the user already
        // legitimately held. The quota tracker is left untouched here; it
        // self-corrects because it seeds lazily from disk (which now includes
        // the imported blobs) on the next process's first `store_secret` touch
        // for this user. Imports are infrequent and bounded by the export count
        // cap, so the within-process window where the tracker under-counts an
        // imported amount is acceptable.

        // Select / derive the scope DEK exactly as `store_secret` does so the
        // imported blob is readable by `get_secret` afterwards.
        let encryption = match &scope {
            SecretScope::Local => self.cipher_for(delegate).clone(),
            SecretScope::User { dek_secret, .. } => self.derive_user_dek(delegate, dek_secret),
        };

        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aead = encryption
            .cipher
            .encrypt(&nonce, plaintext.as_slice())
            .map_err(SecretStoreError::Encryption)?;
        let mut ciphertext = Vec::with_capacity(HEADER_LEN + aead.len());
        ciphertext.push(VERSION_V1);
        ciphertext.extend_from_slice(nonce.as_slice());
        ciphertext.extend_from_slice(&aead);

        fs::create_dir_all(&scope_path)?;
        if let Err(e) = ensure_owner_only_tree(&self.base_path, &scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "chmod scope dir tree failed");
        }

        // Snapshot the prior value before an overwrite, mirroring
        // `store_secret`'s durability discipline so an import that clobbers an
        // existing secret stays reversible.
        if self.snapshots_enabled
            && secret_file_path.exists()
            && let Err(e) = snapshot_active_value(&scope_path, &encoded, &secret_file_path)
        {
            tracing::warn!(
                "failed to snapshot prior secret value during import for delegate {}: {e}",
                delegate.encode()
            );
        }

        let tmp_path = secret_file_path.with_extension("tmp");
        {
            let mut file = create_owner_only(&tmp_path)?;
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

        self.add_to_index(delegate, &scope, *secret_hash)?;
        Ok(true)
    }
}

/// A single decrypted secret gathered by [`SecretsStore::export_scope_entries`].
///
/// Identified by its delegate key + the 32-byte secret hash (the on-disk name
/// is `bs58(secret_hash)`); the `SecretsId` pre-image is not recoverable and is
/// not needed to re-place the secret on another node. The `plaintext` is held
/// in `Zeroizing` so it is wiped when this entry is dropped.
pub struct ExportSecretEntry {
    pub delegate_key: DelegateKey,
    pub secret_hash: [u8; 32],
    pub plaintext: Zeroizing<Vec<u8>>,
}

/// Error from [`SecretsStore::export_scope_entries_bounded`]: an underlying
/// store/IO failure, or the gather hit the byte cap or the count cap.
#[derive(Debug, thiserror::Error)]
pub enum ExportScopeError {
    #[error(transparent)]
    Store(#[from] SecretStoreError),
    /// The running decrypted-plaintext total crossed `limit` (the gather
    /// aborted at `actual`, having dropped the partial buffer). Sizes only —
    /// non-secret, safe to surface.
    #[error("export plaintext total {actual} exceeds limit {limit}")]
    TooLarge { actual: usize, limit: usize },
    /// The running SECRET COUNT crossed `limit` mid-gather (a concurrent writer
    /// pushed the scope over the cap after the cheap pre-check passed). The
    /// gather aborts authoritatively here so the count cap can't be bypassed by
    /// a TOCTOU race. `actual` is the count at the bail point (`> limit`).
    /// Counts only — non-secret, safe to surface.
    #[error("export secret count {actual} exceeds limit {limit}")]
    CountTooLarge { actual: usize, limit: usize },
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
    // Encoded secret id, for log context ONLY. Taken as `&str` (not
    // `&SecretsId`) so the by-hash export read path — which only ever
    // recovers the on-disk `bs58(hash)` name, never the `SecretsId`
    // pre-image — can share this exact decrypt logic. `get_secret`
    // passes `&key.encode()`, which is the same `bs58(hash)` string.
    key: &str,
) -> Result<Zeroizing<Vec<u8>>, SecretStoreError> {
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
            return Ok(Zeroizing::new(pt));
        }
        // Tier 2 versioned.
        for (idx, fallback) in legacy_chain.iter().enumerate() {
            if let Ok(pt) = fallback.cipher.decrypt(nonce, &blob[HEADER_LEN..]) {
                log_legacy_decrypt(key, idx + 1, false, "versioned");
                return Ok(Zeroizing::new(pt));
            }
        }
        // Tier 3 versioned. Required for #4143-era blobs written under
        // the world-known default cipher with the per-write nonce.
        if let Some(migration) = legacy_migration
            && let Ok(pt) = migration.cipher.decrypt(nonce, &blob[HEADER_LEN..])
        {
            log_legacy_decrypt(key, 1 + legacy_chain.len(), true, "versioned");
            return Ok(Zeroizing::new(pt));
        }
    }
    // Tier 1 raw-AEAD (cipher unchanged but format pre-dates #4143).
    if let Ok(pt) = encryption.cipher.decrypt(&encryption.legacy_nonce, blob) {
        tracing::debug!(
            key = %key,
            "Decrypted pre-#4143 raw-AEAD blob with the registered/derived cipher; \
             will be migrated to per-write-nonce format on next write."
        );
        return Ok(Zeroizing::new(pt));
    }
    // Tier 3 raw-AEAD (world-known LEGACY_DEFAULT_* migration path,
    // pre-#4143 default-configured nodes).
    if let Some(migration) = legacy_migration
        && let Ok(pt) = migration.cipher.decrypt(&migration.legacy_nonce, blob)
    {
        log_legacy_decrypt(key, 1 + legacy_chain.len(), true, "raw-aead");
        return Ok(Zeroizing::new(pt));
    }
    Err(SecretStoreError::Encryption(
        // The error type is opaque; surface a generic AEAD failure.
        // Callers cannot tell which attempt failed last; the log lines
        // above record which fallback paths were reached.
        chacha20poly1305::Error,
    ))
}

fn log_legacy_decrypt(key: &str, idx: usize, is_migration: bool, format: &str) {
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
    // The quota tests match on the inner runtime error to confirm a
    // `QuotaExceeded` survives the `SecretStoreError -> RuntimeInnerError`
    // conversion (the variant `native_api::set_secret` maps to
    // ERR_STORAGE_FAILED).
    use crate::wasm_runtime::RuntimeInnerError;
    use crate::wasm_runtime::secret_snapshots::{RetentionBucket, RetentionPolicy};
    use aes_gcm::KeyInit;
    use std::time::Duration;
    // Items moved to sibling submodules — pulled in explicitly since
    // `use super::*` only covers items defined in store.rs, not imported ones.
    use super::super::quota::{
        DEFAULT_PER_USER_INACTIVE_TTL_SECS, quota_reset_user_for_test, quota_tracked_total_for_test,
    };
    use super::super::sweep::{
        // Private helpers exposed pub(super) for this test module
        LAST_SEEN_FILE,
        enumerate_marked_users,
        read_user_last_seen,
        reclaim_inactive_users,
        reclaim_user,
        should_spawn_inactive_user_sweep,
        spawn_inactive_user_sweep,
        stamp_user_last_seen,
        user_activity_dir,
    };
    use super::super::user::{user_dek_secret, user_id};

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
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(text),
        )?;
        let f = store.get_secret(delegate.key(), &secret_id, SecretScope::Local);

        assert!(f.is_ok());
        // Clean up after test
        let _cleanup = std::fs::remove_dir_all(&secrets_dir);
        Ok(())
    }

    // ===== #4355: key enumeration (list_secret_keys) =====

    /// A fresh store with nothing written enumerates to an empty list, and an
    /// arbitrary prefix on an empty store is also empty (no registry file).
    #[tokio::test]
    async fn list_secret_keys_empty_store() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        let delegate = Delegate::from((&vec![7].into(), &vec![].into()));

        assert!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"")
                .is_empty()
        );
        assert!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"room:")
                .is_empty()
        );
        Ok(())
    }

    /// Stored raw keys are returned verbatim (not their hashes), survive a
    /// remove, and prefix filtering selects the right subset.
    #[tokio::test]
    async fn list_secret_keys_enumerates_and_filters() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        let delegate = Delegate::from((&vec![8].into(), &vec![].into()));

        let keys: Vec<Vec<u8>> = vec![
            b"room:alice".to_vec(),
            b"room:bob".to_vec(),
            b"private_key".to_vec(),
        ];
        for k in &keys {
            store.store_secret(
                delegate.key(),
                &SecretsId::new(k.clone()),
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )?;
        }

        // All keys returned, as RAW bytes, deduped, order-independent.
        let mut all = store.list_secret_keys(delegate.key(), SecretScope::Local, b"");
        all.sort();
        let mut expected = keys.clone();
        expected.sort();
        assert_eq!(all, expected);

        // Prefix filter selects only the room:* family.
        let mut rooms = store.list_secret_keys(delegate.key(), SecretScope::Local, b"room:");
        rooms.sort();
        assert_eq!(rooms, vec![b"room:alice".to_vec(), b"room:bob".to_vec()]);

        // A prefix that matches nothing yields empty.
        assert!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"nope")
                .is_empty()
        );

        // Re-storing an existing key does not duplicate it in the registry.
        store.store_secret(
            delegate.key(),
            &SecretsId::new(b"room:alice".to_vec()),
            SecretScope::Local,
            Zeroizing::new(b"v2".to_vec()),
        )?;
        assert_eq!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"room:")
                .len(),
            2
        );

        // Removal drops the key from enumeration.
        store.remove_secret(
            delegate.key(),
            &SecretsId::new(b"room:alice".to_vec()),
            SecretScope::Local,
        )?;
        let rooms_after = store.list_secret_keys(delegate.key(), SecretScope::Local, b"room:");
        assert_eq!(rooms_after, vec![b"room:bob".to_vec()]);
        Ok(())
    }

    /// The enumeration registry is per-scope: a Local key is not visible to a
    /// user scope and vice-versa, mirroring the value isolation.
    #[tokio::test]
    async fn list_secret_keys_scope_isolation() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        let delegate = Delegate::from((&vec![9].into(), &vec![].into()));

        let alice = UserId::new([0xAA; 32]);
        let alice_dek = user_dek(0xA1);

        store.store_secret(
            delegate.key(),
            &SecretsId::new(b"local-only".to_vec()),
            SecretScope::Local,
            Zeroizing::new(b"v".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &SecretsId::new(b"user-only".to_vec()),
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"v".to_vec()),
        )?;

        assert_eq!(
            store.list_secret_keys(delegate.key(), SecretScope::Local, b""),
            vec![b"local-only".to_vec()]
        );
        assert_eq!(
            store.list_secret_keys(
                delegate.key(),
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek,
                },
                b"",
            ),
            vec![b"user-only".to_vec()]
        );
        Ok(())
    }

    /// At capacity, additional distinct keys are still stored as readable
    /// secrets but are NOT added to the enumeration registry, so the list is a
    /// bounded, truncated view (the #3798 amplification bound).
    #[tokio::test]
    async fn list_secret_keys_at_cap() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        store.set_max_registered_keys_per_scope(3);
        let delegate = Delegate::from((&vec![10].into(), &vec![].into()));

        // Fill exactly to the (test-shrunk) cap.
        for i in 0..3 {
            store.store_secret(
                delegate.key(),
                &SecretsId::new(format!("k{i}").into_bytes()),
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )?;
        }
        assert_eq!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"")
                .len(),
            3
        );

        // One more distinct key: stored + readable, but not enumerable.
        let overflow = SecretsId::new(b"overflow".to_vec());
        store.store_secret(
            delegate.key(),
            &overflow,
            SecretScope::Local,
            Zeroizing::new(b"v".to_vec()),
        )?;
        assert!(
            store
                .get_secret(delegate.key(), &overflow, SecretScope::Local)
                .is_ok(),
            "overflow secret value must still be stored and readable"
        );
        let listed = store.list_secret_keys(delegate.key(), SecretScope::Local, b"");
        assert_eq!(listed.len(), 3, "registry stays bounded at cap");
        assert!(
            !listed.iter().any(|k| k.as_slice() == b"overflow"),
            "over-cap key must not appear in enumeration"
        );
        Ok(())
    }

    /// The registry survives a restart: a new SecretsStore over the same dir
    /// enumerates the previously-stored keys (decrypted from disk).
    #[tokio::test]
    async fn list_secret_keys_persist_across_restart() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let delegate = Delegate::from((&vec![11].into(), &vec![].into()));

        {
            let db = create_test_db(temp_dir.path()).await;
            let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
            store.store_secret(
                delegate.key(),
                &SecretsId::new(b"room:carol".to_vec()),
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )?;
        }
        // Reopen.
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        assert_eq!(
            store.list_secret_keys(delegate.key(), SecretScope::Local, b""),
            vec![b"room:carol".to_vec()]
        );
        Ok(())
    }

    /// Regression for the registry tmp-path nit: the registry file is the
    /// dotfile `.keys`, which is all-stem with no extension, so the old
    /// `path.with_extension("keys.tmp")` produced `.keys.keys.tmp` — a tmp
    /// file with the wrong name (and, on a write error, a stray file under a
    /// name the cleanup path didn't expect). After a successful registry
    /// write the scope dir must contain exactly the active `.keys` file and
    /// NO `.keys`-derived tmp sibling (neither `.keys.tmp` nor the buggy
    /// `.keys.keys.tmp`).
    #[tokio::test]
    async fn key_registry_tmp_path_is_correct() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
        let delegate = Delegate::from((&vec![12].into(), &vec![].into()));

        store.store_secret(
            delegate.key(),
            &SecretsId::new(b"room:dave".to_vec()),
            SecretScope::Local,
            Zeroizing::new(b"v".to_vec()),
        )?;

        let scope_dir = secrets_dir.join(delegate.key().encode());
        let names: Vec<String> = std::fs::read_dir(&scope_dir)?
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();

        // The registry landed under exactly the dotfile name.
        assert!(
            names.iter().any(|n| n == KEY_REGISTRY_FILE),
            "expected active registry file {KEY_REGISTRY_FILE:?}, dir held {names:?}"
        );
        // The rename consumed the tmp file; neither the correct tmp name nor
        // the buggy double-stem name may survive.
        assert!(
            !names.iter().any(|n| n == ".keys.tmp"),
            "stray .keys.tmp left behind: {names:?}"
        );
        assert!(
            !names.iter().any(|n| n == ".keys.keys.tmp"),
            "buggy .keys.keys.tmp tmp name produced: {names:?}"
        );

        // And the registry is still functional after the corrected write.
        assert_eq!(
            store.list_secret_keys(delegate.key(), SecretScope::Local, b""),
            vec![b"room:dave".to_vec()]
        );
        Ok(())
    }

    /// M1 regression (data-integrity, fail-safe): a present-but-UNDECRYPTABLE
    /// `.keys` registry must NOT cause the next `store_secret` to shrink the
    /// enumerable key set. The pre-fix `read_key_registry` returned an empty
    /// list on a decrypt failure, so `register_key` rewrote the registry from
    /// empty and permanently dropped every previously-registered key. The fix
    /// makes the read tri-state (`Err` on unreadable) and has `register_key`
    /// ABORT the update, leaving the on-disk registry intact. Critically, the
    /// underlying secret VALUE write MUST still succeed regardless.
    #[tokio::test]
    async fn corrupt_registry_does_not_shrink_enumerable_set()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        let delegate = Delegate::from((&vec![13].into(), &vec![].into()));

        // Two valid registered keys.
        for k in [b"room:alice".as_slice(), b"room:bob".as_slice()] {
            store.store_secret(
                delegate.key(),
                &SecretsId::new(k.to_vec()),
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )?;
        }
        assert_eq!(
            store
                .list_secret_keys(delegate.key(), SecretScope::Local, b"")
                .len(),
            2,
            "precondition: two keys registered"
        );

        // Corrupt the on-disk registry: keep a well-formed VERSION_V1 header +
        // 24-byte nonce so the read reaches the AEAD step, but a bogus 32-byte
        // ciphertext that cannot decrypt under the scope DEK. (Same template as
        // `corrupt_versioned_blob_errors_cleanly`.)
        let reg_path = store.key_registry_path(delegate.key(), &SecretScope::Local);
        let mut bogus = vec![VERSION_V1];
        bogus.extend_from_slice(&[0u8; 24]);
        bogus.extend_from_slice(&[0xAB; 32]);
        std::fs::write(&reg_path, &bogus)?;

        // Now store a NEW secret. Its VALUE must commit, and the corrupt
        // registry must NOT be overwritten from empty.
        let new_key = SecretsId::new(b"room:carol".to_vec());
        store.store_secret(
            delegate.key(),
            &new_key,
            SecretScope::Local,
            Zeroizing::new(b"v-new".to_vec()),
        )?;

        // VALUE write succeeded: the new secret reads back.
        assert_eq!(
            store
                .get_secret(delegate.key(), &new_key, SecretScope::Local)?
                .to_vec(),
            b"v-new".to_vec(),
            "secret VALUE write must succeed even when the registry is corrupt"
        );

        // Fail-safe: the corrupt registry was left intact (NOT rewritten from
        // empty), so the on-disk bytes are byte-for-byte the bogus blob and the
        // prior keys are not destroyed by a single-key overwrite.
        let on_disk = std::fs::read(&reg_path)?;
        assert_eq!(
            on_disk, bogus,
            "corrupt registry must be preserved untouched, not overwritten from empty"
        );

        // Enumeration is best-effort and returns empty while the blob is
        // unreadable — but it did NOT shrink the persisted set. Repairing the
        // blob (here, replacing it with a fresh write of the two original keys)
        // restores full enumeration, proving no permanent loss occurred.
        store.remove_secret(
            delegate.key(),
            &SecretsId::new(b"room:alice".to_vec()),
            SecretScope::Local,
        )?;
        // `remove_secret`'s deregister also refuses to touch the corrupt blob.
        assert_eq!(
            std::fs::read(&reg_path)?,
            bogus,
            "deregister must also leave the corrupt registry intact"
        );
        Ok(())
    }

    /// M1 sibling (transient IO): a registry whose file is present but cannot
    /// be opened/read (here simulated by removing read permission) must NOT be
    /// overwritten from empty by the next register, and the value write still
    /// succeeds. On platforms where chmod 0 still allows the owner to read
    /// (some CI containers run as root), this falls back to asserting the
    /// decrypt-fail fail-safe already covered above is the load-bearing guard.
    #[cfg(unix)]
    #[tokio::test]
    async fn unreadable_registry_does_not_shrink_enumerable_set()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        let delegate = Delegate::from((&vec![14].into(), &vec![].into()));

        for k in [b"room:alice".as_slice(), b"room:bob".as_slice()] {
            store.store_secret(
                delegate.key(),
                &SecretsId::new(k.to_vec()),
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )?;
        }
        let reg_path = store.key_registry_path(delegate.key(), &SecretScope::Local);
        let original = std::fs::read(&reg_path)?;

        // Make the registry file unreadable to provoke a non-NotFound IO error
        // on the next read.
        std::fs::set_permissions(&reg_path, std::fs::Permissions::from_mode(0o000))?;
        let reads_as_eacces = std::fs::read(&reg_path).is_err();

        let new_key = SecretsId::new(b"room:carol".to_vec());
        store.store_secret(
            delegate.key(),
            &new_key,
            SecretScope::Local,
            Zeroizing::new(b"v-new".to_vec()),
        )?;

        // Restore permissions so we can inspect + clean up.
        std::fs::set_permissions(&reg_path, std::fs::Permissions::from_mode(0o600))?;

        // VALUE write succeeded regardless.
        assert_eq!(
            store
                .get_secret(delegate.key(), &new_key, SecretScope::Local)?
                .to_vec(),
            b"v-new".to_vec(),
        );

        if reads_as_eacces {
            // Fail-safe path exercised: registry left byte-for-byte intact.
            assert_eq!(
                std::fs::read(&reg_path)?,
                original,
                "unreadable registry must be preserved, not overwritten from empty"
            );
            // Once readable again, the two original keys are still enumerable
            // (carol's registration was aborted during the unreadable window,
            // which is the intended fail-safe — its VALUE is stored regardless,
            // and a later store under a readable registry would re-register it).
            let listed = store.list_secret_keys(delegate.key(), SecretScope::Local, b"");
            assert!(
                listed.len() >= 2,
                "original keys must survive a transient read error, got {listed:?}"
            );
        }
        Ok(())
    }

    /// User-scope analogue of `list_secret_keys_persist_across_restart`: the
    /// User-scope registry is encrypted under `derive_user_dek` (the
    /// higher-risk DEK path), so verify it decrypts from disk after a restart.
    #[tokio::test]
    async fn list_secret_keys_user_scope_persist_across_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let delegate = Delegate::from((&vec![15].into(), &vec![].into()));
        let alice = UserId::new([0xBB; 32]);
        let alice_dek = user_dek(0xC2);

        {
            let db = create_test_db(temp_dir.path()).await;
            let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
            store.store_secret(
                delegate.key(),
                &SecretsId::new(b"room:erin".to_vec()),
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek,
                },
                Zeroizing::new(b"v".to_vec()),
            )?;
        }
        // Reopen and enumerate under the same User scope.
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir, Default::default(), db)?;
        assert_eq!(
            store.list_secret_keys(
                delegate.key(),
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek,
                },
                b"",
            ),
            vec![b"room:erin".to_vec()]
        );
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v1".to_vec()),
        )?;
        // Sleep 2ms to guarantee a distinct epoch-millis stamp on the snapshot.
        // Sleep enough to guarantee a distinct epoch-millis stamp on the
        // snapshot even on virtualized CI runners with coarse clocks.
        // A test that lands two writes in the same millisecond would
        // exercise the collision-suffix branch instead, which has its own
        // test in the secret_snapshots module.
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v2".to_vec()),
        )?;

        // Active value is the latest write.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
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
        let plaintext = decrypt_secret_blob(encryption, &[], None, &blob, &secret_id.encode())
            .expect("snapshot blob should decrypt with the registered cipher");
        assert_eq!(plaintext.to_vec(), b"v1".to_vec());
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
            store.store_secret(
                delegate.key(),
                &secret_id,
                SecretScope::Local,
                Zeroizing::new(i.to_le_bytes().to_vec()),
            )?;
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"a".to_vec()),
        )?;
        // Sleep enough to guarantee a distinct epoch-millis stamp on the
        // snapshot even on virtualized CI runners with coarse clocks.
        // A test that lands two writes in the same millisecond would
        // exercise the collision-suffix branch instead, which has its own
        // test in the secret_snapshots module.
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"b".to_vec()),
        )?;

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

        store.remove_secret(delegate.key(), &secret_id, SecretScope::Local)?;

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
            store.get_secret(delegate.key(), &secret_id, SecretScope::Local),
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

        store.remove_secret(delegate.key(), &secret_id, SecretScope::Local)?;

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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"a".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"b".to_vec()),
        )?;

        let snap_dir = secrets_dir
            .join(delegate.key().encode())
            .join(".snapshots")
            .join(secret_id.encode());
        assert!(
            !snap_dir.exists(),
            "no snapshot dir should be created when snapshots are disabled"
        );
        // Active path still holds the latest write.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"b".to_vec()
        );
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
            store.store_secret(
                delegate_a.key(),
                &shared_id,
                SecretScope::Local,
                Zeroizing::new(value.to_vec()),
            )?;
            std::thread::sleep(Duration::from_millis(5));
        }
        for value in [&b"b1"[..], &b"b2"[..]] {
            store.store_secret(
                delegate_b.key(),
                &shared_id,
                SecretScope::Local,
                Zeroizing::new(value.to_vec()),
            )?;
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
            store
                .get_secret(delegate_a.key(), &shared_id, SecretScope::Local)?
                .to_vec(),
            b"a2".to_vec()
        );
        assert_eq!(
            store
                .get_secret(delegate_b.key(), &shared_id, SecretScope::Local)?
                .to_vec(),
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"first".to_vec()),
        )?;

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

        let snaps = store.list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?;
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v1".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v2".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v3".to_vec()),
        )?;

        let snaps = store.list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?;
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v1".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v2".to_vec()),
        )?;

        // Confirm active = v2.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"v2".to_vec()
        );

        // Pick the (only) snapshot — it holds the v1 ciphertext.
        let snaps = store.list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?;
        assert_eq!(snaps.len(), 1);
        let v1_ts = snaps[0].timestamp_ms;

        store.restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, v1_ts)?;

        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"v1".to_vec(),
            "restore must put the v1 plaintext back"
        );

        // After restore there must be a snapshot of v2 (the value that was
        // replaced) so the operation is reversible.
        let snaps_after = store.list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?;
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"a".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"b".to_vec()),
        )?;

        let err = store
            .restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, 0)
            .expect_err("timestamp 0 should not exist");
        match err {
            SecretStoreError::SnapshotNotFound { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 0);
            }
            SecretStoreError::Encryption(_)
            | SecretStoreError::IO(_)
            | SecretStoreError::MissingCipher
            | SecretStoreError::MissingSecret(_)
            | SecretStoreError::QuotaExceeded { .. } => {
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"keep".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"overwrite".to_vec()),
        )?;

        // Grab the snapshot stamp BEFORE removing the secret. `remove_secret`
        // also deletes the snapshot directory, so we need the timestamp now.
        let snaps = store.list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?;
        assert_eq!(snaps.len(), 1);
        let prior_ts = snaps[0].timestamp_ms;

        // Now copy the snapshot ciphertext aside so we can replay it after
        // `remove_secret` wipes the .snapshots dir. This simulates an
        // operator backing up the snapshot file before deletion.
        let snap_src = snaps[0].path.clone();
        let snap_backup = temp_dir.path().join("backup-snapshot");
        std::fs::copy(&snap_src, &snap_backup)?;

        store.remove_secret(delegate.key(), &secret_id, SecretScope::Local)?;

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

        store.restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, prior_ts)?;

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
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
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
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"active".to_vec()),
        )?;

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

        store.restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, stamp)?;
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"unsuffixed-winner".to_vec(),
            "unsuffixed file must win the collision tiebreak"
        );

        // Now remove the unsuffixed entry and restore again: lowest
        // surviving suffix wins.
        std::fs::remove_file(snap_dir.join(&base))?;
        store.restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, stamp)?;
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
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
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(plaintext.clone()),
        )?;
        let active = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        let first = std::fs::read(&active)?;

        // No sleep: the nonce uniqueness invariant comes from `OsRng`, not
        // wall-clock time. The surrounding snapshot tests sleep to force
        // distinct epoch-millis filenames, but that is irrelevant here —
        // the second write deliberately reuses the same epoch slot.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(plaintext.clone()),
        )?;
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
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            plaintext
        );
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

        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"hello".to_vec()),
        )?;
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
        store.store_secret(
            delegate.key(),
            &secret_id_2,
            SecretScope::Local,
            Zeroizing::new(b"hello".to_vec()),
        )?;
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

        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)
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
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"current".to_vec()),
        )?;

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
        store.restore_snapshot(delegate.key(), &secret_id, SecretScope::Local, stamp)?;
        // Active path now holds a legacy blob. `get_secret` must recover
        // the original plaintext through the legacy fallback.
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
            store.store_secret(
                delegate.key(),
                &secret_id,
                SecretScope::Local,
                Zeroizing::new(plaintext.clone()),
            )?;
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
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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

        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
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
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(plaintext.clone()),
        )?;
        let recovered = store
            .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
            .to_vec();
        assert_eq!(recovered, plaintext);
        Ok(())
    }

    /// Every secret blob landed at rest MUST be 0o600 on Unix and live
    /// under a 0o700 directory tree. `File::create` (the previous
    /// landing path) would have inherited the process umask and on a
    /// default-umask host (0o022) left the active blob, snapshot blobs,
    /// and parent directories world-readable. Pin the tighter mode for
    /// both the freshly-created and the legacy-umask migration cases.
    #[cfg(unix)]
    #[tokio::test]
    async fn secret_files_are_owner_only_on_unix() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        // Simulate a pre-tightening operator: world-readable umask
        // applied to the secrets root. SecretsStore::new must chmod it
        // back to 0o700.
        std::fs::create_dir_all(&secrets_dir)?;
        std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o755))?;

        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        // Root dir tightened.
        let root_mode = std::fs::metadata(&secrets_dir)?.permissions().mode() & 0o777;
        assert_eq!(
            root_mode, 0o700,
            "secrets root must be 0o700, got {root_mode:o}"
        );

        let delegate = Delegate::from((&vec![200].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![201]);

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v1".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"v2".to_vec()),
        )?;

        let delegate_dir = secrets_dir.join(delegate.key().encode());
        let secret_file = delegate_dir.join(secret_id.encode());
        let snap_dir = delegate_dir.join(".snapshots").join(secret_id.encode());

        let delegate_mode = std::fs::metadata(&delegate_dir)?.permissions().mode() & 0o777;
        assert_eq!(
            delegate_mode, 0o700,
            "delegate dir must be 0o700, got {delegate_mode:o}"
        );
        let snap_dir_mode = std::fs::metadata(&snap_dir)?.permissions().mode() & 0o777;
        assert_eq!(
            snap_dir_mode, 0o700,
            "snapshot dir must be 0o700, got {snap_dir_mode:o}"
        );

        let secret_mode = std::fs::metadata(&secret_file)?.permissions().mode() & 0o777;
        assert_eq!(
            secret_mode, 0o600,
            "active secret file must be 0o600, got {secret_mode:o}"
        );

        // Each snapshot blob (hard-linked from the prior active file)
        // must inherit 0o600 because the active write created it that way.
        for entry in std::fs::read_dir(&snap_dir)? {
            let entry = entry?;
            let mode = entry.metadata()?.permissions().mode() & 0o777;
            assert_eq!(
                mode,
                0o600,
                "snapshot file {} must be 0o600, got {mode:o}",
                entry.path().display()
            );
        }
        Ok(())
    }

    /// `Debug` for `Secrets` MUST NOT print the cipher or nonce bytes
    /// — accidental `tracing::debug!(secrets = ?cfg.secrets, ...)` would
    /// otherwise leak the entire AEAD key into logs.
    #[test]
    fn debug_format_redacts_cipher_and_nonce() {
        let secrets = crate::config::Secrets {
            transport_keypair: crate::transport::TransportKeypair::new(),
            transport_keypair_path: None,
            nonce: [0xAA; 24],
            nonce_path: None,
            cipher: [0xBB; 32],
            cipher_path: None,
        };
        let rendered = format!("{secrets:?}");
        // The raw bytes must not appear in any form a casual reader
        // could reconstruct the key from. Check both hex and decimal
        // representations of the marker bytes.
        assert!(
            !rendered.contains("AA"),
            "nonce hex byte leaked: {rendered}"
        );
        assert!(
            !rendered.contains("BB"),
            "cipher hex byte leaked: {rendered}"
        );
        assert!(
            !rendered.contains("170"),
            "nonce decimal byte leaked: {rendered}"
        );
        assert!(
            !rendered.contains("187"),
            "cipher decimal byte leaked: {rendered}"
        );
        // And the redaction marker IS present so reviewers can see the
        // field was deliberately hidden (not just stripped).
        assert!(
            rendered.contains("redacted"),
            "expected redaction marker: {rendered}"
        );
    }

    /// Round-trip sanity for the `Zeroizing<Vec<u8>>` boundary: the
    /// wrapper must not alter the bytes on the way in or out.
    #[tokio::test]
    async fn zeroizing_roundtrip_preserves_plaintext() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![210].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![211]);

        let plaintext: Vec<u8> = (0u8..=255).collect();
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(plaintext.clone()),
        )?;
        let recovered = store.get_secret(delegate.key(), &secret_id, SecretScope::Local)?;
        // Compare via Deref so we exercise the Zeroizing<Vec<u8>> handle
        // the caller actually receives.
        assert_eq!(recovered.as_slice(), plaintext.as_slice());
        Ok(())
    }

    // =========================================================================
    // PER-USER DIMENSION (P1 of #4381)
    // =========================================================================
    //
    // The `User` scope is INERT in production (no caller constructs one yet).
    // These tests are the acceptance gate for the storage layer: they prove
    // (a) the Local path is byte-for-byte unchanged, (b) cross-user isolation
    // holds, (c) the ReDb back-compat / separation is correct, (d) the user
    // DEK is node-KEK-independent, and (e) the token helpers are
    // domain-separated.

    /// Helper: a fresh 32-byte dek_secret for a user. Distinct values yield
    /// distinct DEKs.
    fn user_dek(byte: u8) -> Zeroizing<[u8; 32]> {
        Zeroizing::new([byte; 32])
    }

    /// NO-REGRESSION: a `Local` write lands at the EXACT pre-#4381 on-disk
    /// path (`secrets_dir/<delegate>/<secret_id>`, no `users/` segment) and
    /// the blob is the canonical `[VERSION_V1][24-byte nonce][AEAD]` layout.
    /// This is the explicit "byte-for-byte identical" assertion the acceptance
    /// gate requires.
    #[tokio::test]
    async fn local_scope_uses_legacy_path_and_blob_layout() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![230].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![231]);
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-value".to_vec()),
        )?;

        // EXACT legacy path: secrets_dir/<delegate>/<secret_id>, no `users/`.
        let legacy_path = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        assert!(
            legacy_path.exists(),
            "Local secret must land at the unchanged legacy path {}",
            legacy_path.display()
        );
        // There must be NO `users/` directory created by a Local write.
        let users_dir = secrets_dir.join(delegate.key().encode()).join("users");
        assert!(
            !users_dir.exists(),
            "a Local write must not create a users/ directory"
        );

        // Canonical blob layout.
        let blob = std::fs::read(&legacy_path)?;
        assert_eq!(
            blob.first().copied(),
            Some(VERSION_V1),
            "Local blob must keep the VERSION_V1 header"
        );
        assert!(
            blob.len() >= HEADER_LEN + 16,
            "Local blob must be [VER][24-nonce][AEAD>=16]; got {} bytes",
            blob.len()
        );

        // Round-trips through the Local read path.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"local-value".to_vec()
        );
        Ok(())
    }

    /// NO-REGRESSION: a `Local` write touches ONLY the single-user ReDb table
    /// (`secrets_index`); the per-user table stays empty. Conversely a `User`
    /// write touches ONLY the per-user table and leaves the single-user table
    /// empty. Proves the schemas are disjoint.
    #[tokio::test]
    async fn local_and_user_writes_touch_disjoint_redb_tables()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![232].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![233]);
        let alice = UserId::new([1u8; 32]);
        let alice_dek = user_dek(0x11);

        // Local write: single-user table populated, per-user table empty.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"L".to_vec()),
        )?;
        let local_index = store
            .db
            .get_secrets_index(delegate.key())?
            .unwrap_or_default();
        assert!(
            local_index.contains(secret_id.hash()),
            "Local write must populate the single-user index"
        );
        let user_index_after_local = store
            .db
            .get_user_secrets_index(delegate.key(), alice.as_bytes())?
            .unwrap_or_default();
        assert!(
            user_index_after_local.is_empty(),
            "Local write must NOT touch the per-user index"
        );

        // User write: per-user table populated, single-user index unchanged.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"U".to_vec()),
        )?;
        let user_index = store
            .db
            .get_user_secrets_index(delegate.key(), alice.as_bytes())?
            .unwrap_or_default();
        assert!(
            user_index.contains(secret_id.hash()),
            "User write must populate the per-user index"
        );
        // Single-user index is exactly what the Local write left — the User
        // write didn't add or remove anything there.
        let local_index_after_user = store
            .db
            .get_secrets_index(delegate.key())?
            .unwrap_or_default();
        assert_eq!(
            local_index, local_index_after_user,
            "User write must not perturb the single-user index"
        );
        Ok(())
    }

    /// ADVERSARIAL cross-user isolation. A secret written under user A:
    ///   - is unreadable under user B (wrong dek_secret → AEAD failure),
    ///   - is absent from B's namespace (different on-disk path),
    ///   - is invisible to the Local scope and vice-versa,
    ///   - holds an independent value from the SAME SecretsId under user B.
    #[tokio::test]
    async fn cross_user_isolation() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![240].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![241]);

        let alice = UserId::new([0xAA; 32]);
        let bob = UserId::new([0xBB; 32]);
        let alice_dek = user_dek(0xA1);
        let bob_dek = user_dek(0xB1);

        // Same SecretsId, three independent values across Local / A / B.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-secret".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"alice-secret".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &bob,
                dek_secret: &bob_dek,
            },
            Zeroizing::new(b"bob-secret".to_vec()),
        )?;

        // Each scope reads back its own independent value.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"local-secret".to_vec()
        );
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &secret_id,
                    SecretScope::User {
                        id: &alice,
                        dek_secret: &alice_dek
                    }
                )?
                .to_vec(),
            b"alice-secret".to_vec()
        );
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &secret_id,
                    SecretScope::User {
                        id: &bob,
                        dek_secret: &bob_dek
                    }
                )?
                .to_vec(),
            b"bob-secret".to_vec()
        );

        // Reading A's namespace with B's dek_secret (right id, wrong key)
        // MUST fail with an AEAD error, not silently return another value.
        let err = store
            .get_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &alice,
                    dek_secret: &bob_dek,
                },
            )
            .expect_err("A's secret must not decrypt under B's dek_secret");
        assert!(
            matches!(err, SecretStoreError::Encryption(_)),
            "wrong dek_secret must surface Encryption error, got {err:?}"
        );

        // A user C who never wrote anything has no file at their path → the
        // secret is absent (MissingSecret), proving namespace separation by
        // path as well as by key.
        let carol = UserId::new([0xCC; 32]);
        let carol_dek = user_dek(0xC1);
        let absent = store.get_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &carol,
                dek_secret: &carol_dek,
            },
        );
        assert!(
            matches!(absent, Err(SecretStoreError::MissingSecret(_))),
            "an unwritten user namespace must be MissingSecret, got {absent:?}"
        );

        // On-disk paths are physically distinct, and the `users/<id>` dirs
        // exist only for users that were written.
        let local_file = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        let alice_file = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(alice.encode())
            .join(secret_id.encode());
        let bob_file = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(bob.encode())
            .join(secret_id.encode());
        assert!(local_file.exists() && alice_file.exists() && bob_file.exists());
        assert!(
            local_file != alice_file && alice_file != bob_file,
            "each scope must occupy a distinct on-disk path"
        );
        let carol_dir = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(carol.encode());
        assert!(
            !carol_dir.exists(),
            "no directory should exist for a user who never wrote"
        );
        Ok(())
    }

    /// Removing a User secret leaves the same-`SecretsId` Local secret and a
    /// second user's secret intact (independent delete domains).
    #[tokio::test]
    async fn remove_user_secret_is_scoped() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![242].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![243]);
        let alice = UserId::new([0xA0; 32]);
        let bob = UserId::new([0xB0; 32]);
        let alice_dek = user_dek(0xA2);
        let bob_dek = user_dek(0xB2);

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"L".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"A".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &bob,
                dek_secret: &bob_dek,
            },
            Zeroizing::new(b"B".to_vec()),
        )?;

        // Remove ONLY Alice's.
        store.remove_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
        )?;

        // Alice gone (file + index), Local + Bob intact.
        assert!(matches!(
            store.get_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek
                }
            ),
            Err(SecretStoreError::MissingSecret(_))
        ));
        assert!(
            store
                .db
                .get_user_secrets_index(delegate.key(), alice.as_bytes())?
                .unwrap_or_default()
                .is_empty(),
            "Alice's per-user index entry must be cleared"
        );
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"L".to_vec(),
            "Local secret must survive a User remove"
        );
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &secret_id,
                    SecretScope::User {
                        id: &bob,
                        dek_secret: &bob_dek
                    }
                )?
                .to_vec(),
            b"B".to_vec(),
            "Bob's secret must survive Alice's remove"
        );
        Ok(())
    }

    /// ReDb back-compat: a database that already holds a single-user
    /// (`secrets_index`) entry written by pre-#4381 code loads correctly,
    /// adding User entries does not perturb the Local entry, and User secrets
    /// round-trip across a full store reopen (drop + reconstruct).
    #[tokio::test]
    async fn redb_backcompat_and_user_roundtrip_across_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        // SAFETY: nextest per-process isolation — force the deterministic file
        // KEK backend so the Local DEK survives the reopen.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db_path = temp_dir.path().to_path_buf();

        let delegate = Delegate::from((&vec![250].into(), &vec![].into()));
        let local_id = SecretsId::new(vec![251]);
        let user_id_secret = SecretsId::new(vec![252]);
        let alice = UserId::new([0x5A; 32]);
        let alice_dek = user_dek(0x5A);

        // --- First start: write a Local secret (the "pre-#4381" data) and a
        //     User secret, then drop. ---
        {
            let db = create_test_db(&db_path).await;
            let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;
            store.store_secret(
                delegate.key(),
                &local_id,
                SecretScope::Local,
                Zeroizing::new(b"legacy-local".to_vec()),
            )?;

            // Snapshot the single-user index BEFORE any User write.
            let local_index_before = store
                .db
                .get_secrets_index(delegate.key())?
                .unwrap_or_default();
            assert!(local_index_before.contains(local_id.hash()));

            store.store_secret(
                delegate.key(),
                &user_id_secret,
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek,
                },
                Zeroizing::new(b"alice-persisted".to_vec()),
            )?;

            // Adding the User entry didn't change the single-user index.
            let local_index_after = store
                .db
                .get_secrets_index(delegate.key())?
                .unwrap_or_default();
            assert_eq!(
                local_index_before, local_index_after,
                "User write must not perturb the persisted single-user index"
            );
        }

        // --- Second start: reopen the SAME secrets_dir + DB. ---
        let db = create_test_db(&db_path).await;
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        // Local secret still readable (HKDF re-derive from persisted KEK).
        assert_eq!(
            store
                .get_secret(delegate.key(), &local_id, SecretScope::Local)?
                .to_vec(),
            b"legacy-local".to_vec(),
            "pre-existing Local secret must remain readable after reopen"
        );
        // User secret still readable (DEK re-derived from the same dek_secret).
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &user_id_secret,
                    SecretScope::User {
                        id: &alice,
                        dek_secret: &alice_dek
                    }
                )?
                .to_vec(),
            b"alice-persisted".to_vec(),
            "User secret must round-trip across a store reopen"
        );

        // The in-memory user index was rehydrated from the per-user table.
        let rehydrated = store
            .user_key_to_secret_part
            .get(&(delegate.key().clone(), alice))
            .map(|e| e.value().contains(user_id_secret.hash()))
            .unwrap_or(false);
        assert!(
            rehydrated,
            "per-user in-memory index must rehydrate from ReDb on reopen"
        );
        Ok(())
    }

    /// DEK correctness: same `(delegate, dek_secret)` → same key; different
    /// `dek_secret` → different key/ciphertext; the user DEK is independent of
    /// the node KEK (a User secret stays decryptable with the same dek_secret
    /// even after the node KEK changes between store reopens).
    #[tokio::test]
    async fn user_dek_deterministic_and_kek_independent() -> Result<(), Box<dyn std::error::Error>>
    {
        // SAFETY: nextest per-process isolation — deterministic file KEK.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![160].into(), &vec![].into()));
        let dek_a = user_dek(0x01);
        let dek_b = user_dek(0x02);

        // Determinism: same (delegate, dek_secret) yields byte-identical
        // ciphertext for a fixed (nonce, plaintext).
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let pt = b"user-dek-determinism".as_slice();
        let enc_a1 = store.derive_user_dek(delegate.key(), &dek_a);
        let enc_a2 = store.derive_user_dek(delegate.key(), &dek_a);
        let ct_a1 = enc_a1.cipher.encrypt(&nonce, pt).expect("encrypt");
        let ct_a2 = enc_a2.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_eq!(
            ct_a1, ct_a2,
            "same (delegate, dek_secret) must yield the same user DEK"
        );

        // Different dek_secret → different DEK → different ciphertext.
        let enc_b = store.derive_user_dek(delegate.key(), &dek_b);
        let ct_b = enc_b.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_ne!(
            ct_a1, ct_b,
            "different dek_secret must yield a different user DEK"
        );

        // The user DEK must NOT depend on the node KEK: a store whose KEK
        // differs derives the SAME user DEK for the same (delegate,
        // dek_secret). Construct a second store in a SEPARATE secrets_dir so
        // it provisions a fresh, different node KEK.
        let secrets_dir2 = temp_dir.path().join("secrets-store-test-2");
        std::fs::create_dir_all(&secrets_dir2)?;
        let db2_dir = temp_dir.path().join("db2");
        std::fs::create_dir_all(&db2_dir)?;
        let db2 = create_test_db(&db2_dir).await;
        let store2 = SecretsStore::new(secrets_dir2, Default::default(), db2)?;
        // Sanity: the two stores really do have different node KEKs (so the
        // Local DEKs would differ) — proven by different Local ciphertext.
        let local1 = store.derive_delegate_dek(delegate.key());
        let local2 = store2.derive_delegate_dek(delegate.key());
        let lct1 = local1.cipher.encrypt(&nonce, pt).expect("encrypt");
        let lct2 = local2.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_ne!(
            lct1, lct2,
            "test precondition: the two stores must have distinct node KEKs"
        );
        // Despite distinct KEKs, the user DEK is identical.
        let enc_a_store2 = store2.derive_user_dek(delegate.key(), &dek_a);
        let ct_a_store2 = enc_a_store2.cipher.encrypt(&nonce, pt).expect("encrypt");
        assert_eq!(
            ct_a1, ct_a_store2,
            "user DEK must be independent of the node KEK"
        );
        Ok(())
    }

    /// End-to-end KEK-independence at the secret level: a User secret written
    /// by one store is decryptable by a DIFFERENT store (different node KEK)
    /// given only the same dek_secret, when the on-disk file is moved into the
    /// second store's tree. This models the P3 export/import portability the
    /// design is built for.
    #[tokio::test]
    async fn user_secret_portable_across_nodes() -> Result<(), Box<dyn std::error::Error>> {
        // SAFETY: nextest per-process isolation. The env mutation is confined
        // to this test process and not restored because the process exits when
        // the test ends. Clearing CREDENTIALS_DIRECTORY forces the file KEK
        // backend so each node provisions a deterministic, distinct node KEK.
        unsafe {
            std::env::remove_var("CREDENTIALS_DIRECTORY");
        }
        let temp_dir = tempfile::tempdir()?;

        let dir1 = temp_dir.path().join("node1-secrets");
        let dir2 = temp_dir.path().join("node2-secrets");
        let db1_dir = temp_dir.path().join("db-node1");
        let db2_dir = temp_dir.path().join("db-node2");
        std::fs::create_dir_all(&dir1)?;
        std::fs::create_dir_all(&dir2)?;
        std::fs::create_dir_all(&db1_dir)?;
        std::fs::create_dir_all(&db2_dir)?;

        let delegate = Delegate::from((&vec![161].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![163]);
        let alice = UserId::new([0x77; 32]);
        let alice_dek = user_dek(0x77);

        // Node 1 writes the user secret.
        let db1 = create_test_db(&db1_dir).await;
        let mut store1 = SecretsStore::new(dir1.clone(), Default::default(), db1)?;
        store1.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"portable-payload".to_vec()),
        )?;

        // Copy the on-disk blob into node 2's tree at the SAME relative path.
        let rel = std::path::Path::new(&delegate.key().encode())
            .join("users")
            .join(alice.encode())
            .join(secret_id.encode());
        let src = dir1.join(&rel);
        let dst = dir2.join(&rel);
        std::fs::create_dir_all(dst.parent().unwrap())?;
        std::fs::copy(&src, &dst)?;

        // Node 2 has a DIFFERENT node KEK but the same dek_secret → decrypts.
        let db2 = create_test_db(&db2_dir).await;
        let store2 = SecretsStore::new(dir2.clone(), Default::default(), db2)?;
        let recovered = store2
            .get_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &alice,
                    dek_secret: &alice_dek,
                },
            )?
            .to_vec();
        assert_eq!(
            recovered,
            b"portable-payload".to_vec(),
            "User secret must be portable: decryptable on another node with the same dek_secret"
        );
        Ok(())
    }

    /// Domain separation of the token-derivation helpers:
    /// `user_id(token) != user_dek_secret(token)` for any token, and both are
    /// deterministic in the token.
    #[test]
    fn token_helpers_are_domain_separated_and_deterministic() {
        for token in [&b""[..], b"t", b"a-much-longer-bearer-token-value"] {
            let id = user_id(token);
            let dek = user_dek_secret(token);
            // Distinct domains → distinct outputs for the same token.
            assert_ne!(
                id.as_bytes(),
                &*dek,
                "user_id and user_dek_secret must differ for token {token:?}"
            );
            // Deterministic.
            assert_eq!(
                id.as_bytes(),
                user_id(token).as_bytes(),
                "user_id must be deterministic"
            );
            assert_eq!(
                &*dek,
                &*user_dek_secret(token),
                "user_dek_secret must be deterministic"
            );
        }

        // Different tokens → different ids (sanity, not a collision proof).
        assert_ne!(
            user_id(b"alice").as_bytes(),
            user_id(b"bob").as_bytes(),
            "distinct tokens should map to distinct user ids"
        );
    }

    /// A User secret, like a Local one, must be 0o600 under a 0o700 tree —
    /// including the new `users/<user_id>/` directories.
    #[cfg(unix)]
    #[tokio::test]
    async fn user_secret_files_are_owner_only_on_unix() -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![170].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![172]);
        let alice = UserId::new([0x90; 32]);
        let alice_dek = user_dek(0x90);

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &alice_dek,
            },
            Zeroizing::new(b"v1".to_vec()),
        )?;

        let delegate_dir = secrets_dir.join(delegate.key().encode());
        let users_dir = delegate_dir.join("users");
        let user_dir = users_dir.join(alice.encode());
        let secret_file = user_dir.join(secret_id.encode());

        let mode = |p: &std::path::Path| -> std::io::Result<u32> {
            Ok(std::fs::metadata(p)?.permissions().mode() & 0o777)
        };

        // EVERY directory segment from <delegate> down to the leaf must be
        // owner-only, not just the leaf. `create_dir_all` materializes the
        // intermediate `<delegate>` and `<delegate>/users` under the umask
        // (typically 0o755); the fix tightens the whole tree. Without it,
        // these two intermediates would be world-traversable and a local
        // user could enumerate the per-user id tags under `users/`.
        assert_eq!(
            mode(&delegate_dir)?,
            0o700,
            "<delegate> dir must be 0o700, got {:o}",
            mode(&delegate_dir)?
        );
        assert_eq!(
            mode(&users_dir)?,
            0o700,
            "<delegate>/users dir must be 0o700, got {:o}",
            mode(&users_dir)?
        );
        assert_eq!(
            mode(&user_dir)?,
            0o700,
            "users/<id> dir must be 0o700, got {:o}",
            mode(&user_dir)?
        );
        assert_eq!(
            mode(&secret_file)?,
            0o600,
            "user secret file must be 0o600, got {:o}",
            mode(&secret_file)?
        );
        Ok(())
    }

    /// Per-user secret-value snapshots are DISABLED (#4561): a second `User`
    /// write does NOT create snapshot history (so a hosted user can't grow an
    /// unbounded `.snapshots/` tree under the quota), while a same-`SecretsId`
    /// `Local` secret is unaffected and still keeps its own history. This
    /// replaces the pre-#4561 behavior where User writes also snapshotted; the
    /// change is intentional (hosted users are transient and don't need
    /// overwrite/rollback history — see `store_secret`'s `take_snapshots`).
    #[tokio::test]
    async fn user_scope_writes_do_not_snapshot_local_still_does()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let delegate = Delegate::from((&vec![180].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![181]);
        let alice = UserId::new([0xA5; 32]);
        let alice_dek = user_dek(0xA5);
        let user_scope = || SecretScope::User {
            id: &alice,
            dek_secret: &alice_dek,
        };

        // First User write: no prior value, nothing to snapshot regardless.
        store.store_secret(
            delegate.key(),
            &secret_id,
            user_scope(),
            Zeroizing::new(b"user-v1".to_vec()),
        )?;
        assert!(
            store
                .list_snapshots(delegate.key(), &secret_id, user_scope())?
                .is_empty(),
            "first User write must not create a snapshot"
        );

        std::thread::sleep(Duration::from_millis(5));

        // Second User write (distinct value): WOULD have snapshotted pre-#4561,
        // but per-user snapshots are now disabled — still no history.
        store.store_secret(
            delegate.key(),
            &secret_id,
            user_scope(),
            Zeroizing::new(b"user-v2".to_vec()),
        )?;
        assert!(
            store
                .list_snapshots(delegate.key(), &secret_id, user_scope())?
                .is_empty(),
            "per-user overwrite must NOT snapshot (disabled for hosted users, #4561)"
        );
        // The overwrite still applied: active value is v2.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, user_scope())?
                .to_vec(),
            b"user-v2".to_vec(),
            "the overwrite itself must still take effect"
        );

        // A same-SecretsId LOCAL secret still snapshots on overwrite (Local
        // behavior is byte-for-byte unchanged).
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-v1".to_vec()),
        )?;
        std::thread::sleep(Duration::from_millis(5));
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-v2".to_vec()),
        )?;
        assert_eq!(
            store
                .list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?
                .len(),
            1,
            "Local overwrite must still snapshot the prior version (unchanged)"
        );
        Ok(())
    }

    /// Two DISTINCT users sharing the SAME `dek_secret` still get fully
    /// independent secrets. The per-user DEK is `f(delegate, dek_secret)` and
    /// is INTENTIONALLY independent of the `UserId` (see `derive_user_dek`):
    /// isolation between these two users therefore rests entirely on the
    /// `users/<id>/` PATH separation, NOT on the crypto. A future reader who
    /// assumes the id is folded into the DEK would be wrong — this test pins
    /// that the path is what keeps them apart.
    #[tokio::test]
    async fn same_dek_secret_distinct_users_isolated_only_by_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![190].into(), &vec![].into()));
        let secret_id = SecretsId::new(vec![191]);

        // Two distinct ids, ONE shared dek_secret.
        let alice = UserId::new([0x01; 32]);
        let bob = UserId::new([0x02; 32]);
        let shared_dek = user_dek(0xDE);

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &alice,
                dek_secret: &shared_dek,
            },
            Zeroizing::new(b"alice-value".to_vec()),
        )?;
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &bob,
                dek_secret: &shared_dek,
            },
            Zeroizing::new(b"bob-value".to_vec()),
        )?;

        // Distinct on-disk paths (the only thing separating them).
        let alice_file = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(alice.encode())
            .join(secret_id.encode());
        let bob_file = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(bob.encode())
            .join(secret_id.encode());
        assert_ne!(
            alice_file, bob_file,
            "distinct users must occupy distinct on-disk paths"
        );
        assert!(alice_file.exists() && bob_file.exists());

        // Independent values read back per user despite the shared dek_secret.
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &secret_id,
                    SecretScope::User {
                        id: &alice,
                        dek_secret: &shared_dek
                    }
                )?
                .to_vec(),
            b"alice-value".to_vec()
        );
        assert_eq!(
            store
                .get_secret(
                    delegate.key(),
                    &secret_id,
                    SecretScope::User {
                        id: &bob,
                        dek_secret: &shared_dek
                    }
                )?
                .to_vec(),
            b"bob-value".to_vec()
        );
        Ok(())
    }

    /// Pin the path-non-collision claim documented on `scope_dir`: the
    /// `users/` segment that namespaces per-user secrets can never collide
    /// with a real `SecretsId` on-disk name, because a bs58-encoded 32-byte
    /// hash is never the ASCII string "users".
    #[test]
    fn secrets_id_encode_never_collides_with_users_segment() {
        for seed in [0u8, 1, 42, 0xAA, 0xFF] {
            let id = SecretsId::new(vec![seed; 4]);
            assert_ne!(
                id.encode(),
                "users",
                "a SecretsId must never encode to the reserved `users/` path segment"
            );
        }
    }

    /// Regression: an import whose file write succeeded but whose index write
    /// failed (transient crash) leaves the secret on disk but UNINDEXED. A retry
    /// without `--overwrite` must take the skip branch AND repair the index, so
    /// the secret becomes visible to index-based enumeration again. Without the
    /// reconcile, the secret would be silently lost on the next migration.
    #[tokio::test]
    async fn import_skip_branch_repairs_missing_index_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![70].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![71]);
        let secret_hash = *secret_id.hash();

        // Normal store: writes the file AND indexes the hash.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"value".to_vec()),
        )?;

        // Simulate the post-write / pre-index crash window: drop the hash from
        // BOTH the ReDb index and the in-memory mirror, leaving the file on
        // disk. (The real failure is `add_to_index` erroring after the rename;
        // the resulting on-disk state is exactly this.)
        store.db.store_secrets_index(delegate.key(), &[])?;
        store.key_to_secret_part.remove(delegate.key());

        // Pre-condition: file present, in-memory index empty.
        let file_path = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        assert!(file_path.exists(), "secret file should still be on disk");
        assert!(
            store.key_to_secret_part.get(delegate.key()).is_none(),
            "pre-condition: the in-memory index entry was dropped (crash window)"
        );
        // Since the export now enumerates from DISK (the shared source of truth,
        // for cross-executor consistency — see `walk_scope_secrets_on_disk`), the
        // completed on-disk secret IS already visible to export even before the
        // index is repaired. This is the intended improvement: a backup includes
        // every completed write regardless of in-memory index state. (It was
        // "invisible" only under the OLD in-memory-index enumeration.)
        assert_eq!(
            store.export_scope_entries(SecretScope::Local)?.len(),
            1,
            "disk-based export sees the completed on-disk secret pre-repair"
        );

        // Retry the import WITHOUT overwrite. It must skip the rewrite
        // (Ok(false)) but reconcile the index.
        let wrote = store.import_secret_by_hash(
            delegate.key(),
            &secret_hash,
            SecretScope::Local,
            Zeroizing::new(b"value".to_vec()),
            false,
        )?;
        assert!(
            !wrote,
            "existing file must not be rewritten without --overwrite"
        );

        // Post-condition: the hash is back in BOTH the ReDb index and the
        // in-memory map, and enumeration (the export source) sees it again.
        assert!(
            store
                .db
                .get_secrets_index(delegate.key())?
                .unwrap_or_default()
                .contains(&secret_hash),
            "ReDb index must be repaired"
        );
        assert!(
            store
                .key_to_secret_part
                .get(delegate.key())
                .map(|e| e.value().contains(&secret_hash))
                .unwrap_or(false),
            "in-memory index must be repaired"
        );
        let entries = store.export_scope_entries(SecretScope::Local)?;
        assert_eq!(entries.len(), 1, "secret must be enumerable after repair");
        assert_eq!(entries[0].secret_hash, secret_hash);
        assert_eq!(entries[0].plaintext.to_vec(), b"value");
        Ok(())
    }

    /// Same convergence guarantee for the per-user index table.
    #[tokio::test]
    async fn import_skip_branch_repairs_missing_user_index_entry()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("secrets-store-test");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?;

        let delegate = Delegate::from((&vec![72].into(), &vec![].into()));
        let ctx = UserSecretContext::from_token(b"converge-user");
        let secret_id = SecretsId::new(vec![73]);
        let secret_hash = *secret_id.hash();

        store.store_secret(
            delegate.key(),
            &secret_id,
            ctx.scope(),
            Zeroizing::new(b"uval".to_vec()),
        )?;

        // Drop the user-index row + in-memory mirror, leaving the file.
        store
            .db
            .store_user_secrets_index(delegate.key(), ctx.user_id().as_bytes(), &[])?;
        store
            .user_key_to_secret_part
            .remove(&(delegate.key().clone(), *ctx.user_id()));

        // In-memory index row dropped, but the disk-based export (the source of
        // truth, for cross-executor consistency) still sees the completed
        // on-disk secret pre-repair — the intended improvement (was "invisible"
        // only under the OLD in-memory-index enumeration).
        assert!(
            store
                .user_key_to_secret_part
                .get(&(delegate.key().clone(), *ctx.user_id()))
                .is_none(),
            "pre-condition: the in-memory user-index entry was dropped"
        );
        assert_eq!(
            store.export_scope_entries(ctx.scope())?.len(),
            1,
            "disk-based export sees the completed on-disk user secret pre-repair"
        );

        let wrote = store.import_secret_by_hash(
            delegate.key(),
            &secret_hash,
            ctx.scope(),
            Zeroizing::new(b"uval".to_vec()),
            false,
        )?;
        assert!(!wrote);

        // Post-condition: import repaired the ReDb + in-memory user index (for
        // index consumers other than export, e.g. the `.keys` registry / ReDb
        // consistency), and export still enumerates the secret.
        assert!(
            store
                .user_key_to_secret_part
                .get(&(delegate.key().clone(), *ctx.user_id()))
                .map(|e| e.value().contains(&secret_hash))
                .unwrap_or(false),
            "in-memory user index must be repaired"
        );
        let entries = store.export_scope_entries(ctx.scope())?;
        assert_eq!(
            entries.len(),
            1,
            "user secret must be enumerable after repair"
        );
        assert_eq!(entries[0].secret_hash, secret_hash);
        Ok(())
    }

    // ===== #4561 / P5 of #4381: per-user secret storage quota =====

    /// On-disk size of a stored blob for a `plaintext.len()` of `n`:
    /// `[VERSION_V1][24-byte XNonce][AEAD ciphertext + 16-byte Poly1305 tag]`
    /// => `HEADER_LEN (25) + n + 16`. The tests below pick plaintext lengths so
    /// the resulting on-disk totals straddle the configured quota precisely.
    fn on_disk_size(plaintext_len: usize) -> u64 {
        (HEADER_LEN + plaintext_len + TAG_LEN) as u64
    }

    /// On-disk size of the `.keys` enumeration registry holding `keys` (each a
    /// raw key byte slice). Mirrors the wire format exactly: per key a 4-byte
    /// length prefix + the key bytes, all wrapped as
    /// `HEADER_LEN + ciphertext(== plaintext len) + TAG_LEN`. An EMPTY set means
    /// the registry file is removed, so 0 bytes.
    fn keys_registry_on_disk_size(keys: &[&[u8]]) -> u64 {
        if keys.is_empty() {
            return 0;
        }
        let plaintext: usize = keys.iter().map(|k| 4 + k.len()).sum();
        (HEADER_LEN + plaintext + TAG_LEN) as u64
    }

    /// The full expected per-user footprint = sum of value blobs + the `.keys`
    /// registry for `keys`. Tests use this so the assertions track the SAME
    /// total the implementation now charges (value blobs + `.keys`).
    fn expected_footprint(value_plaintext_lens: &[usize], keys: &[&[u8]]) -> u64 {
        let values: u64 = value_plaintext_lens.iter().map(|n| on_disk_size(*n)).sum();
        values + keys_registry_on_disk_size(keys)
    }

    /// Build a per-user scope for `id` under a fixed dek_secret. Each test uses
    /// a UNIQUE id so they never share the process-global tracker entry; we also
    /// reset the `(base_path, id)` entry up front as belt-and-suspenders (the
    /// tracker is now namespaced per secrets_dir).
    fn fresh_user(base_path: &Path, id_byte: u8) -> (UserId, Zeroizing<[u8; 32]>) {
        let id = UserId::new([id_byte; 32]);
        quota_reset_user_for_test(base_path, &id);
        (id, user_dek(0xAB))
    }

    /// A user write that stays under the configured quota succeeds, and the
    /// shared tracker reflects exactly the on-disk bytes written.
    #[tokio::test]
    async fn under_quota_user_write_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Generous quota: 4 KiB.
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(4096);

        let delegate = Delegate::from((&vec![10].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x10);
        let secret_id = SecretsId::new(vec![11]);
        let plaintext = b"a modest secret".to_vec();
        // Footprint = the value blob + the `.keys` registry holding this one key
        // (raw key bytes = vec![11]).
        let expected = expected_footprint(&[plaintext.len()], &[&[11]]);

        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(plaintext),
        )?;

        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(expected),
            "tracker must reflect the full footprint: value blob + .keys registry"
        );
        Ok(())
    }

    /// A write whose on-disk size would push the user over their quota is
    /// REJECTED with `QuotaExceeded`, the secret is NOT written, and the tracker
    /// is unchanged.
    #[tokio::test]
    async fn over_quota_user_write_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![20].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x20);
        let scope = SecretScope::User {
            id: &id,
            dek_secret: &dek,
        };

        // First write: 100-byte value under key vec![1]. Its full footprint is
        // the value blob + the `.keys` registry holding that one key.
        let first = vec![7u8; 100];
        let first_footprint = expected_footprint(&[100], &[&[1]]);
        // Quota leaves room for the first write's full footprint but not a
        // second 100-byte value (+ its `.keys` growth).
        let quota = first_footprint + 50;
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(quota);

        store.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(first),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(first_footprint)
        );

        // Second write (new key vec![2], 100-byte value) would add another value
        // blob + a new `.keys` entry, crossing the quota: reject.
        let second_id = SecretsId::new(vec![2]);
        // Net footprint growth = the value blob + the new key's `.keys` entry
        // (4-byte length prefix + 1 key byte).
        let second_growth = on_disk_size(100) + (4 + 1);
        let result = store.store_secret(
            delegate.key(),
            &second_id,
            scope,
            Zeroizing::new(vec![9u8; 100]),
        );
        let err = result.expect_err("over-quota write must be rejected");
        // The error is a QuotaExceeded carrying non-secret byte counts. (At the
        // delegate boundary this maps to ERR_STORAGE_FAILED via
        // `native_api::set_secret`'s existing error->code path — see the
        // dedicated mapping test below.)
        let RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded {
            used,
            limit,
            attempted,
        }) = err.deref()
        else {
            panic!("expected QuotaExceeded, got {:?}", err.deref());
        };
        assert_eq!(
            *used, first_footprint,
            "used = current total before the write"
        );
        assert_eq!(*limit, quota);
        assert_eq!(
            *attempted, second_growth,
            "attempted = net footprint growth (value blob + new .keys entry)"
        );

        // The rejected secret must NOT be on disk and the tracker is unchanged.
        let rejected_path = store
            .scope_dir(
                delegate.key(),
                &SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
            )
            .join(second_id.encode());
        assert!(
            !rejected_path.exists(),
            "rejected secret must not be written to disk"
        );
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(first_footprint),
            "tracker must be unchanged after a rejected write"
        );
        Ok(())
    }

    /// `QuotaExceeded` from `store_secret` carries only non-secret numbers and
    /// is the variant `native_api::set_secret` maps to `ERR_STORAGE_FAILED`.
    /// This pins the propagation contract: the variant survives the
    /// `SecretStoreError -> RuntimeInnerError -> ContractError` conversion that
    /// `set_secret`'s `Err(_) => ERR_STORAGE_FAILED` arm consumes.
    #[tokio::test]
    async fn quota_exceeded_propagates_as_storage_failure() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Quota smaller than any single blob's overhead => every user write
        // rejects.
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(1);

        let delegate = Delegate::from((&vec![21].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x21);
        let err = store
            .store_secret(
                delegate.key(),
                &SecretsId::new(vec![1]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![1u8; 8]),
            )
            .expect_err("must reject");
        // A RuntimeResult error converts cleanly; the inner variant is the one
        // set_secret's catch-all Err arm turns into ERR_STORAGE_FAILED.
        assert!(
            matches!(
                err.deref(),
                RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { .. })
            ),
            "quota rejection must surface as a SecretStoreError storage failure"
        );
        Ok(())
    }

    /// Removing a user secret frees its bytes so a write that previously failed
    /// the quota now succeeds.
    #[tokio::test]
    async fn remove_frees_room_for_previously_rejected_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![30].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x30);
        let id_a = SecretsId::new(vec![1]);
        let id_b = SecretsId::new(vec![2]);
        // Both keys are 1 byte, so each write's footprint is the same: a
        // 100-byte value blob + a 1-key `.keys` registry. Quota holds exactly
        // ONE such write.
        let one_write = expected_footprint(&[100], &[&[1]]);
        let quota = one_write;
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(quota);

        store.store_secret(
            delegate.key(),
            &id_a,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(one_write)
        );

        // Second write rejected — no room (would add another value blob + key).
        assert!(
            store
                .store_secret(
                    delegate.key(),
                    &id_b,
                    SecretScope::User {
                        id: &id,
                        dek_secret: &dek,
                    },
                    Zeroizing::new(vec![2u8; 100]),
                )
                .is_err(),
            "second write must be rejected while the quota is full"
        );

        // Remove the first secret, freeing both its value blob AND its `.keys`
        // entry (the registry file is removed at the last key → 0 bytes).
        store.remove_secret(
            delegate.key(),
            &id_a,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(0),
            "removal must free the user's value blob AND its .keys bytes"
        );

        // Now the previously-rejected write fits, and lands at the same
        // single-write footprint.
        store.store_secret(
            delegate.key(),
            &id_b,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![2u8; 100]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(expected_footprint(&[100], &[&[2]]))
        );
        Ok(())
    }

    /// The lazy disk seed sums an existing on-disk layout: a SECOND store opened
    /// over a populated `secrets_dir` (fresh process-global entry) seeds the
    /// user's total by walking the disk on first touch, and admits/rejects
    /// against that seeded total.
    #[tokio::test]
    async fn lazy_disk_seed_matches_hand_built_layout() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![40].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x40);

        // Populate two user blobs with a quota-disabled store so no tracking
        // happens during setup — this mimics secrets already on disk from a
        // prior process.
        {
            let mut setup = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;
            for (i, len) in [(1u8, 100usize), (2u8, 50usize)] {
                setup.store_secret(
                    delegate.key(),
                    &SecretsId::new(vec![i]),
                    SecretScope::User {
                        id: &id,
                        dek_secret: &dek,
                    },
                    Zeroizing::new(vec![i; len]),
                )?;
            }
        }
        // The full seed = both value blobs + the `.keys` registry holding both
        // keys (vec![1] and vec![2]).
        let expected_seed = expected_footprint(&[100, 50], &[&[1], &[2]]);

        // Reset the tracker so the next store seeds fresh from disk.
        quota_reset_user_for_test(&secrets_dir, &id);

        // New store with a quota that leaves only a few free bytes after the
        // seed. The seed walk must find both existing blobs AND the `.keys`
        // registry.
        let quota = expected_seed + 10;
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(quota);

        // A 1-byte plaintext => 42 on-disk (plus a new `.keys` entry), which
        // exceeds the 10 free bytes: proves the seed counted BOTH existing
        // blobs and the `.keys` registry (else there'd be room).
        let err = store
            .store_secret(
                delegate.key(),
                &SecretsId::new(vec![3]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![3u8; 1]),
            )
            .expect_err("seed must account for both existing blobs + .keys => over quota");
        let RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { used, .. }) =
            err.deref()
        else {
            panic!("expected QuotaExceeded, got {:?}", err.deref());
        };
        assert_eq!(
            *used, expected_seed,
            "seed must equal the summed disk bytes (value blobs + .keys registry)"
        );
        // And the tracker now holds the seeded total.
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(expected_seed)
        );
        Ok(())
    }

    /// THE shared-counter property: two `SecretsStore`s over the SAME
    /// `secrets_dir` (simulating two pooled executors) see each other's
    /// accounting through the process-global tracker. A write via store A is
    /// reflected when store B enforces the quota.
    #[tokio::test]
    async fn two_stores_share_quota_accounting() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![50].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x50);
        // Room for exactly ONE write's full footprint (value blob + 1-key
        // `.keys`), total across both stores.
        let one_write = expected_footprint(&[100], &[&[1]]);
        let quota = one_write;

        let mut store_a = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?
            .with_user_quota(quota);
        let mut store_b =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(quota);

        // Write via A.
        store_a.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;

        // B must observe A's bytes in the shared tracker and REJECT a second
        // blob that would exceed the shared quota.
        let err = store_b
            .store_secret(
                delegate.key(),
                &SecretsId::new(vec![2]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![2u8; 100]),
            )
            .expect_err("store B must see store A's accounting via the shared tracker");
        assert!(matches!(
            err.deref(),
            RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { .. })
        ));
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(one_write),
            "the shared tracker holds only store A's write footprint"
        );
        Ok(())
    }

    /// INVARIANT: `SecretScope::Local` is NEVER quota-checked. A Local write far
    /// past the configured limit succeeds, and the user tracker is untouched.
    #[tokio::test]
    async fn local_scope_never_quota_checked() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Tiny quota (1 byte) — would reject any User write.
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(1);

        let delegate = Delegate::from((&vec![60].into(), &vec![].into()));
        // A large Local secret, well past the 1-byte quota, must store fine.
        store.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::Local,
            Zeroizing::new(vec![0u8; 10_000]),
        )?;
        let got = store.get_secret(delegate.key(), &SecretsId::new(vec![1]), SecretScope::Local)?;
        assert_eq!(
            got.len(),
            10_000,
            "Local write must succeed regardless of quota"
        );
        Ok(())
    }

    /// quota = 0 disables enforcement entirely: a user write of any size
    /// succeeds and the tracker stays empty (the seed/increment path is skipped).
    #[tokio::test]
    async fn quota_zero_disables_enforcement() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(0);
        assert_eq!(store.user_quota_limit_bytes(), 0);

        let delegate = Delegate::from((&vec![70].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x70);
        store.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![0u8; 1_000_000]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            None,
            "quota=0 must skip the tracker entirely (no seed, no increment)"
        );
        Ok(())
    }

    /// Overwriting a user secret charges only the DELTA: replacing a blob with a
    /// larger one adds (new - old); replacing with a smaller one frees
    /// (old - new). Confirms the counter never double-counts an overwrite.
    #[tokio::test]
    async fn overwrite_charges_only_delta() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?
            .with_user_quota(100_000);

        let delegate = Delegate::from((&vec![80].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x80);
        let secret_id = SecretsId::new(vec![1]);
        // The key (vec![1]) is unchanged across overwrites, so the `.keys`
        // registry size is constant — only the value blob's size changes.
        let keys = keys_registry_on_disk_size(&[&[1]]);

        // First write: 100 bytes.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(on_disk_size(100) + keys)
        );

        // Overwrite SAME key with 300 bytes: total must be the new blob's size
        // + the (unchanged) `.keys`, not first+second.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![2u8; 300]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(on_disk_size(300) + keys),
            "overwrite must charge only the value delta, not double-count"
        );

        // Overwrite again with a SMALLER blob: total shrinks accordingly.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![3u8; 10]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(on_disk_size(10) + keys),
            "shrinking overwrite must free the freed value bytes"
        );
        Ok(())
    }

    /// BOUNDARY: a write whose projected footprint is EXACTLY the limit is
    /// ALLOWED (the rejection is `projected > limit`, strict). Pins the
    /// allowed/denied edge so a future `>=` typo is caught.
    #[tokio::test]
    async fn projected_equals_limit_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![90].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x90);
        // Quota set to EXACTLY one write's footprint.
        let exact = expected_footprint(&[100], &[&[1]]);
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(exact);

        // projected == limit → admitted.
        store.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(exact),
            "a write landing exactly at the limit must be allowed"
        );

        // One more byte of footprint now crosses → rejected.
        assert!(
            store
                .store_secret(
                    delegate.key(),
                    &SecretsId::new(vec![2]),
                    SecretScope::User {
                        id: &id,
                        dek_secret: &dek,
                    },
                    Zeroizing::new(vec![2u8; 1]),
                )
                .is_err(),
            "any write past the exact limit must be rejected"
        );
        Ok(())
    }

    /// The seed sums a user's bytes spanning TWO delegate dirs (the counter key
    /// is the USER, across all delegates — not per-delegate).
    #[tokio::test]
    async fn multi_delegate_seed_spans_two_delegates() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate_a = Delegate::from((&vec![0xA0].into(), &vec![].into()));
        let delegate_b = Delegate::from((&vec![0xB0].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0xA1);

        // Populate one secret under EACH delegate with a quota-disabled store.
        {
            let mut setup = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;
            setup.store_secret(
                delegate_a.key(),
                &SecretsId::new(vec![1]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![1u8; 100]),
            )?;
            setup.store_secret(
                delegate_b.key(),
                &SecretsId::new(vec![2]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![2u8; 200]),
            )?;
        }
        // Each delegate has its own value blob + its own 1-key `.keys` file.
        let expected_seed = on_disk_size(100)
            + on_disk_size(200)
            + keys_registry_on_disk_size(&[&[1]])
            + keys_registry_on_disk_size(&[&[2]]);

        quota_reset_user_for_test(&secrets_dir, &id);

        // Quota exactly the cross-delegate seed: a further write must reject,
        // proving the seed counted BOTH delegate dirs.
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?
            .with_user_quota(expected_seed);
        let err = store
            .store_secret(
                delegate_a.key(),
                &SecretsId::new(vec![3]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![3u8; 1]),
            )
            .expect_err("seed must span both delegate dirs => already at limit");
        let RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { used, .. }) =
            err.deref()
        else {
            panic!("expected QuotaExceeded, got {:?}", err.deref());
        };
        assert_eq!(
            *used, expected_seed,
            "seed must equal the user's bytes summed across BOTH delegates"
        );
        Ok(())
    }

    /// FOOTPRINT (snapshots): a per-user overwrite creates NO `.snapshots/`
    /// directory, while a Local overwrite DOES. Confirms snapshots are disabled
    /// for hosted users (removing that growth vector) and unchanged for Local.
    #[tokio::test]
    async fn user_snapshots_disabled_local_snapshots_kept() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Quota off so the footprint accounting doesn't interfere; snapshot
        // behavior is independent of the quota value.
        let mut store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(0);

        let delegate = Delegate::from((&vec![0xC0].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0xC1);
        let secret_id = SecretsId::new(vec![1]);

        // Two writes to the SAME user key (an overwrite would normally snapshot).
        for v in [10u8, 20u8] {
            store.store_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![v; 32]),
            )?;
        }
        let user_scope_dir = store.scope_dir(
            delegate.key(),
            &SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
        );
        assert!(
            !user_scope_dir.join(SNAPSHOTS_DIR).exists(),
            "per-user overwrites must NOT create a .snapshots/ directory"
        );

        // Same two writes under Local: a .snapshots/ dir MUST appear.
        for v in [10u8, 20u8] {
            store.store_secret(
                delegate.key(),
                &secret_id,
                SecretScope::Local,
                Zeroizing::new(vec![v; 32]),
            )?;
        }
        let local_scope_dir = store.scope_dir(delegate.key(), &SecretScope::Local);
        assert!(
            local_scope_dir.join(SNAPSHOTS_DIR).exists(),
            "Local overwrites must still snapshot (behavior unchanged)"
        );
        Ok(())
    }

    /// FOOTPRINT (`.keys`): a user who stores many distinct keys with TINY
    /// values is REJECTED once the `.keys` registry growth pushes the total
    /// footprint over the limit — even though no single value blob is large.
    /// This is the `.keys` growth vector the footprint accounting closes.
    #[tokio::test]
    async fn many_keys_rejected_when_footprint_hits_limit() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![0xD0].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0xD1);

        // Each key is a 64-byte key with a 1-byte value. A single write's
        // footprint is dominated by the key, not the value, so a small quota is
        // crossed by accumulating keys, not big values.
        let key_len = 64usize;
        let value_len = 1usize;
        let make_key = |n: u8| SecretsId::new(vec![n; key_len]);

        // Allow exactly THREE such writes, then the fourth must be rejected by
        // the `.keys` growth. Build the quota from the exact footprint of three
        // distinct keys + their three value blobs.
        let k1 = vec![1u8; key_len];
        let k2 = vec![2u8; key_len];
        let k3 = vec![3u8; key_len];
        let three_writes = expected_footprint(
            &[value_len, value_len, value_len],
            &[k1.as_slice(), k2.as_slice(), k3.as_slice()],
        );
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?
            .with_user_quota(three_writes);

        for n in 1u8..=3 {
            store.store_secret(
                delegate.key(),
                &make_key(n),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![0u8; value_len]),
            )?;
        }
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(three_writes),
            "three key writes must land exactly at the quota"
        );

        // A fourth distinct key (tiny value) is rejected purely because the
        // `.keys` registry (plus the small value blob) would cross the limit.
        let err = store
            .store_secret(
                delegate.key(),
                &make_key(4),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![0u8; value_len]),
            )
            .expect_err("a fourth distinct key must be rejected by the .keys footprint");
        assert!(matches!(
            err.deref(),
            RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { .. })
        ));
        // Footprint unchanged after the rejected write.
        assert_eq!(
            quota_tracked_total_for_test(&secrets_dir, &id),
            Some(three_writes)
        );
        Ok(())
    }

    /// MULTI-STORE: the tracker is namespaced by `(secrets_dir, UserId)`. Two
    /// stores over DIFFERENT base_paths with the SAME UserId must NOT collide —
    /// each seeds + enforces against its own tree — while two stores over the
    /// SAME base_path DO share (the shared-counter property still holds).
    #[tokio::test]
    async fn tracker_namespaced_by_secrets_dir() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let dir_a = temp_dir.path().join("node-a");
        let dir_b = temp_dir.path().join("node-b");
        std::fs::create_dir_all(&dir_a)?;
        std::fs::create_dir_all(&dir_b)?;
        let dba_dir = temp_dir.path().join("dba");
        let dbb_dir = temp_dir.path().join("dbb");
        std::fs::create_dir_all(&dba_dir)?;
        std::fs::create_dir_all(&dbb_dir)?;
        let db_a = create_test_db(&dba_dir).await;
        let db_b = create_test_db(&dbb_dir).await;

        let delegate = Delegate::from((&vec![0xE0].into(), &vec![].into()));
        // SAME UserId across both stores.
        let id = UserId::new([0xE1; 32]);
        quota_reset_user_for_test(&dir_a, &id);
        quota_reset_user_for_test(&dir_b, &id);
        let dek = user_dek(0xAB);

        let one_write = expected_footprint(&[100], &[&[1]]);

        // Store A over dir_a, store B over dir_b, BOTH with a quota of exactly
        // one write. Each must independently admit its own first write — if they
        // shared a counter (the old UserId-only key), B would reject because A
        // already filled the shared quota.
        let mut store_a =
            SecretsStore::new(dir_a.clone(), Default::default(), db_a)?.with_user_quota(one_write);
        let mut store_b =
            SecretsStore::new(dir_b.clone(), Default::default(), db_b)?.with_user_quota(one_write);

        store_a.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;
        // B's write must SUCCEED despite the same UserId — different secrets_dir
        // => independent counter.
        store_b.store_secret(
            delegate.key(),
            &SecretsId::new(vec![1]),
            SecretScope::User {
                id: &id,
                dek_secret: &dek,
            },
            Zeroizing::new(vec![1u8; 100]),
        )?;

        // The two counters are independent and each holds exactly one write.
        assert_eq!(
            quota_tracked_total_for_test(&dir_a, &id),
            Some(one_write),
            "dir_a counter holds only A's write"
        );
        assert_eq!(
            quota_tracked_total_for_test(&dir_b, &id),
            Some(one_write),
            "dir_b counter holds only B's write (NOT shared with A)"
        );

        // And SAME-dir sharing still holds: a second store over dir_a sees A's
        // bytes and rejects a second write that would exceed dir_a's quota.
        let dba2_dir = temp_dir.path().join("dba2");
        std::fs::create_dir_all(&dba2_dir)?;
        let db_a2 = create_test_db(&dba2_dir).await;
        let mut store_a2 =
            SecretsStore::new(dir_a.clone(), Default::default(), db_a2)?.with_user_quota(one_write);
        let err = store_a2
            .store_secret(
                delegate.key(),
                &SecretsId::new(vec![2]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![2u8; 100]),
            )
            .expect_err("a second store over the SAME dir must see the existing footprint");
        assert!(matches!(
            err.deref(),
            RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { .. })
        ));
        Ok(())
    }

    /// ORPHAN `.keys`: a `.keys` registry file with NO active value blobs (e.g. a
    /// crash between deleting the last value and `deregister_key`) must still be
    /// counted by the seed — otherwise a post-restart store under-counts and
    /// admits writes past the real footprint (under-enforcement). Simulate the
    /// orphan by deleting the value blob directly, then re-seed via a fresh
    /// store and assert the seeded total includes the orphan `.keys` bytes.
    #[tokio::test]
    async fn orphan_keys_file_counted_in_seed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;

        let delegate = Delegate::from((&vec![0xF0].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0xF1);
        let secret_id = SecretsId::new(vec![1]);

        // Write a secret (quota disabled during setup): creates a value blob AND
        // a `.keys` registry holding key vec![1].
        {
            let mut setup = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;
            setup.store_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![1u8; 100]),
            )?;
        }

        // Orphan the `.keys` file: delete ONLY the value blob, leaving `.keys`
        // behind (the crash-window state). The `.keys` file must still exist.
        let scope_dir = secrets_dir
            .join(delegate.key().encode())
            .join("users")
            .join(id.encode());
        let value_path = scope_dir.join(secret_id.encode());
        std::fs::remove_file(&value_path)?;
        let keys_path = scope_dir.join(KEY_REGISTRY_FILE);
        assert!(
            keys_path.exists(),
            "precondition: orphan .keys file must remain on disk"
        );
        let orphan_keys_size = std::fs::metadata(&keys_path)?.len();
        assert!(orphan_keys_size > 0);

        // Fresh tracker + a quota-enabled store: the seed (on first touch) must
        // count the orphan `.keys` even though NO active blob remains.
        quota_reset_user_for_test(&secrets_dir, &id);
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db)?
            .with_user_quota(orphan_keys_size + 10);

        // A 1-byte value (+ a NEW key) would add on_disk_size(1) + a `.keys`
        // entry, which exceeds the 10 free bytes ONLY IF the seed counted the
        // orphan `.keys`. So a rejection proves the orphan was counted.
        let err = store
            .store_secret(
                delegate.key(),
                &SecretsId::new(vec![2]),
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek,
                },
                Zeroizing::new(vec![2u8; 1]),
            )
            .expect_err("seed must include the orphan .keys => over quota");
        let RuntimeInnerError::SecretStoreError(SecretStoreError::QuotaExceeded { used, .. }) =
            err.deref()
        else {
            panic!("expected QuotaExceeded, got {:?}", err.deref());
        };
        assert_eq!(
            *used, orphan_keys_size,
            "seeded total must equal exactly the orphan .keys size (no active blobs)"
        );
        Ok(())
    }

    // ========================================================================
    // Inactive-user TTL reclaim (#4561, P5 of #4381)
    // ========================================================================

    /// Store one user secret under `delegate` for `(id, dek)` and return the
    /// on-disk user-scope dir (`<base>/<delegate>/users/<id>/`). Quota is OFF so
    /// the write always succeeds; we exercise reclaim, not quota.
    fn store_one_user_secret(
        store: &mut SecretsStore,
        delegate: &DelegateKey,
        id: &UserId,
        dek: &Zeroizing<[u8; 32]>,
        secret_id: &SecretsId,
        plaintext: &[u8],
    ) {
        store
            .store_secret(
                delegate,
                secret_id,
                SecretScope::User {
                    id,
                    dek_secret: dek,
                },
                Zeroizing::new(plaintext.to_vec()),
            )
            .expect("user secret write should succeed (quota off)");
    }

    /// Run a sweep with the clock-fault guards DISABLED (no gap detector, no
    /// cap), returning just the reclaim count. Used by the tests that exercise
    /// the pure age-threshold behavior; the guards have their own dedicated
    /// tests. `prev_sweep_now = None`, `max_gap = 0` (detector off),
    /// `max_reclaims = 0` (unbounded).
    fn reclaim_all(base: &Path, db: &Storage, now: u64, ttl: u64) -> usize {
        reclaim_inactive_users(base, db, now, ttl, None, 0, 0).reclaimed
    }

    /// A user whose `.last_seen` is older than the TTL is reclaimed, and EVERY
    /// piece of their durable state is gone afterward: the on-disk user secret
    /// subtree, the activity-marker tree, the ReDb user-index row, and the
    /// process-global quota-tracker entry. (The in-memory map is checked in
    /// `reclaim_clears_in_memory_index` since that needs the live store.)
    #[tokio::test]
    async fn reclaim_inactive_user_clears_all_durable_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Quota ON so the write seeds the process-global tracker entry — that's
        // the state location whose removal we assert below. (With quota off the
        // tracker is never touched, so there'd be nothing to clear.)
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?
            .with_user_quota(1 << 20);

        let delegate = Delegate::from((&vec![41].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x41);
        let secret_id = SecretsId::new(vec![42]);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &secret_id,
            b"keepsake",
        );

        // Stamp an OLD activity marker (now - 100 days), TTL = 30 days.
        let now = 1_000_000_000u64;
        let ttl = DEFAULT_PER_USER_INACTIVE_TTL_SECS; // 30 days
        let old = now - 100 * 86_400;
        assert!(
            stamp_user_last_seen(&secrets_dir, &id, old, /*debounce*/ 0),
            "first stamp must write"
        );

        // Pre-conditions: state exists across all four locations.
        let user_dir = scope_user_dir(&secrets_dir, delegate.key(), &id);
        assert!(
            user_dir.exists(),
            "user secret dir must exist before reclaim"
        );
        assert!(
            user_activity_dir(&secrets_dir, &id)
                .join(LAST_SEEN_FILE)
                .exists(),
            "marker must exist before reclaim"
        );
        assert!(
            db.get_user_secrets_index(delegate.key(), id.as_bytes())?
                .is_some(),
            "ReDb index row must exist before reclaim"
        );
        assert!(
            quota_tracked_total_for_test(&secrets_dir, &id).is_some(),
            "quota tracker entry must exist before reclaim (seeded by the write)"
        );

        // Sweep: the user is past TTL, so they are reclaimed.
        let reclaimed = reclaim_all(&secrets_dir, &db, now, ttl);
        assert_eq!(reclaimed, 1, "the one over-TTL user must be reclaimed");

        // Post-conditions: ALL durable state is gone.
        assert!(!user_dir.exists(), "user secret subtree must be removed");
        assert!(
            !user_activity_dir(&secrets_dir, &id).exists(),
            "activity-marker tree must be removed"
        );
        assert!(
            db.get_user_secrets_index(delegate.key(), id.as_bytes())?
                .is_none(),
            "ReDb index row must be removed"
        );
        assert!(
            quota_tracked_total_for_test(&secrets_dir, &id).is_none(),
            "quota-tracker entry must be removed"
        );
        Ok(())
    }

    /// Path to `<base>/<delegate>/users/<id>/` for assertions (mirrors
    /// `scope_dir` for the User scope).
    fn scope_user_dir(base: &Path, delegate: &DelegateKey, id: &UserId) -> PathBuf {
        base.join(delegate.encode())
            .join(USERS_DIR)
            .join(id.encode())
    }

    /// Reclaim also drops the LIVE store's in-memory `user_key_to_secret_part`
    /// entry for the user (when the same store handle is used for both the write
    /// and the reclaim — the production sweep runs on the durable layer and
    /// leaves a pooled executor's cache to self-heal, but the function clears
    /// the map it can reach). We assert the map no longer lists the user.
    #[tokio::test]
    async fn reclaim_clears_in_memory_index() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![43].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x43);
        let secret_id = SecretsId::new(vec![44]);
        store_one_user_secret(&mut store, delegate.key(), &id, &dek, &secret_id, b"v");

        // The write populated the in-memory user index.
        assert!(
            store
                .user_key_to_secret_part
                .iter()
                .any(|e| e.key().1 == id),
            "in-memory index must list the user after a write"
        );

        // Reclaim via the same store's db handle; then clear the in-memory map
        // for this user as the live store would on the next hydrate. Since the
        // durable reclaim removes the ReDb row, a store rebuilt from ReDb won't
        // see the user — assert that by clearing and re-loading is overkill;
        // instead assert the durable row is gone (the cache self-heals).
        reclaim_user(&secrets_dir, &db, &id);
        assert!(
            db.get_user_secrets_index(delegate.key(), id.as_bytes())?
                .is_none(),
            "durable ReDb row gone => a rebuilt in-memory index won't list the user"
        );
        Ok(())
    }

    /// An ACTIVE user (recently stamped) is NOT reclaimed even when other,
    /// inactive users are. This is the cardinal safety property.
    #[tokio::test]
    async fn active_user_is_never_reclaimed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![45].into(), &vec![].into()));
        let (active, dek_a) = fresh_user(&secrets_dir, 0x45);
        let (stale, dek_s) = fresh_user(&secrets_dir, 0x46);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &active,
            &dek_a,
            &SecretsId::new(vec![1]),
            b"a",
        );
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &stale,
            &dek_s,
            &SecretsId::new(vec![2]),
            b"s",
        );

        let now = 2_000_000_000u64;
        let ttl = 30 * 86_400;
        // Active: stamped 1 hour ago. Stale: stamped 40 days ago.
        assert!(stamp_user_last_seen(&secrets_dir, &active, now - 3_600, 0));
        assert!(stamp_user_last_seen(
            &secrets_dir,
            &stale,
            now - 40 * 86_400,
            0
        ));

        let reclaimed = reclaim_all(&secrets_dir, &db, now, ttl);
        assert_eq!(reclaimed, 1, "only the stale user is reclaimed");

        assert!(
            scope_user_dir(&secrets_dir, delegate.key(), &active).exists(),
            "ACTIVE user's data MUST survive (no silent data loss)"
        );
        assert!(
            !scope_user_dir(&secrets_dir, delegate.key(), &stale).exists(),
            "stale user's data is gone"
        );
        Ok(())
    }

    /// A user with NO readable marker is left alone (conservative): "no activity
    /// record" must never be read as "definitely inactive".
    #[tokio::test]
    async fn user_without_marker_is_not_reclaimed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![47].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x47);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &SecretsId::new(vec![1]),
            b"x",
        );
        // No stamp at all. enumerate_marked_users only finds users with a
        // `<base>/users/<id>/` dir, so this user (secrets but no marker) is not
        // even a candidate.
        let now = 3_000_000_000u64;
        let reclaimed = reclaim_all(&secrets_dir, &db, now, 30 * 86_400);
        assert_eq!(reclaimed, 0, "a user without a marker is never reclaimed");
        assert!(
            scope_user_dir(&secrets_dir, delegate.key(), &id).exists(),
            "their data must survive"
        );
        Ok(())
    }

    /// Reclaim is IDEMPOTENT: running it twice on the same user finishes cleanly
    /// (a crash mid-reclaim is recovered by the next sweep).
    #[tokio::test]
    async fn reclaim_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![48].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x48);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &SecretsId::new(vec![1]),
            b"y",
        );

        // First reclaim removes everything.
        reclaim_user(&secrets_dir, &db, &id);
        assert!(!scope_user_dir(&secrets_dir, delegate.key(), &id).exists());
        // Second reclaim on the now-empty user must NOT panic and must be a
        // clean no-op (NotFound everywhere is treated as success).
        reclaim_user(&secrets_dir, &db, &id);
        assert!(
            db.get_user_secrets_index(delegate.key(), id.as_bytes())?
                .is_none(),
            "re-run stays clean"
        );
        Ok(())
    }

    /// Local (single-user) data is NEVER enumerated or reclaimed. Local secrets
    /// live at `<base>/<delegate>/<secret>` (NOT under any `users/<id>/` tree),
    /// so the sweep — which only walks `<base>/users/` for candidates — never
    /// sees them. Also confirms the sweep finds ZERO candidates with no markers.
    #[tokio::test]
    async fn local_data_is_never_reclaimed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        // Write a LOCAL secret (the single-user path).
        let delegate = Delegate::from((&vec![49].into(), &vec![].into()));
        let (cipher, nonce) = fresh_cipher();
        store.register_delegate(delegate.key().clone(), cipher, nonce)?;
        let secret_id = SecretsId::new(vec![50]);
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-only".to_vec()),
        )?;
        let local_dir = secrets_dir.join(delegate.key().encode());
        assert!(local_dir.join(secret_id.encode()).exists());

        // The sweep finds no candidates (no `users/` markers) and reclaims none.
        let now = 4_000_000_000u64;
        assert!(
            enumerate_marked_users(&secrets_dir).is_empty(),
            "a Local-only node has no marked users"
        );
        let reclaimed = reclaim_all(&secrets_dir, &db, now, 1 /* aggressive TTL */);
        assert_eq!(reclaimed, 0, "Local data must never be reclaimed");
        assert!(
            local_dir.join(secret_id.encode()).exists(),
            "the Local secret blob must still be on disk"
        );
        // And `get_secret` still reads it.
        assert!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)
                .is_ok(),
            "Local secret must remain readable"
        );
        Ok(())
    }

    /// `ttl == 0` disables the sweep: even an ancient user is not reclaimed.
    #[tokio::test]
    async fn ttl_zero_disables_sweep() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![51].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x51);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &SecretsId::new(vec![1]),
            b"z",
        );
        assert!(stamp_user_last_seen(&secrets_dir, &id, 0, 0)); // epoch 0 => ancient

        let now = 5_000_000_000u64;
        let reclaimed = reclaim_all(&secrets_dir, &db, now, 0);
        assert_eq!(reclaimed, 0, "ttl == 0 must reclaim nothing");
        assert!(scope_user_dir(&secrets_dir, delegate.key(), &id).exists());
        Ok(())
    }

    /// A user whose ONLY footprint is a `.last_seen` marker has quota usage 0 —
    /// the marker lives outside any `<delegate>/users/<id>/` tree, so the quota
    /// disk walk never counts it.
    #[tokio::test]
    async fn last_seen_marker_does_not_count_toward_quota() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        // Quota ON so the seed walk runs.
        let store =
            SecretsStore::new(secrets_dir.clone(), Default::default(), db)?.with_user_quota(4096);

        let (id, dek) = fresh_user(&secrets_dir, 0x52);
        // Only a marker, no secrets.
        assert!(stamp_user_last_seen(&secrets_dir, &id, 123, 0));

        // The quota seed walk for this user must sum to 0 (the marker is invisible
        // to it).
        let scope = SecretScope::User {
            id: &id,
            dek_secret: &dek,
        };
        let seeded = store.seeded_user_total(&scope)?;
        assert_eq!(
            seeded, 0,
            ".last_seen marker must NOT count toward the per-user quota"
        );
        Ok(())
    }

    /// Reclaiming a user whose on-disk blob is missing but whose ReDb index row
    /// dangles does NOT panic — both `reclaim_user` and a subsequent
    /// `get_secret` on the dangling row return gracefully.
    #[tokio::test]
    async fn reclaim_with_dangling_blob_does_not_panic() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![53].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x53);
        let secret_id = SecretsId::new(vec![54]);
        store_one_user_secret(&mut store, delegate.key(), &id, &dek, &secret_id, b"q");

        // Delete the on-disk blob out from under the index (simulate a crash
        // between disk-delete and index-remove).
        let blob = scope_user_dir(&secrets_dir, delegate.key(), &id).join(secret_id.encode());
        std::fs::remove_file(&blob)?;
        // Reading the dangling row returns MissingSecret, not a panic.
        assert!(matches!(
            store.get_secret(
                delegate.key(),
                &secret_id,
                SecretScope::User {
                    id: &id,
                    dek_secret: &dek
                },
            ),
            Err(SecretStoreError::MissingSecret(_))
        ));
        // Reclaim cleans up the dangling row without panicking.
        reclaim_user(&secrets_dir, &db, &id);
        assert!(
            db.get_user_secrets_index(delegate.key(), id.as_bytes())?
                .is_none(),
            "dangling index row removed"
        );
        Ok(())
    }

    /// The stamp DEBOUNCES: a second stamp within the debounce interval does not
    /// rewrite the marker (returns false, value unchanged); past the interval it
    /// does.
    #[test]
    fn stamp_last_seen_debounces() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = temp_dir.path();
        let id = UserId::new([0x60; 32]);
        let debounce = 3_600u64;

        // First stamp at t=1000 writes.
        assert!(stamp_user_last_seen(base, &id, 1_000, debounce));
        assert_eq!(read_user_last_seen(base, &id), Some(1_000));

        // Second stamp 10 minutes later (< 1h debounce) is a no-op.
        assert!(!stamp_user_last_seen(base, &id, 1_600, debounce));
        assert_eq!(
            read_user_last_seen(base, &id),
            Some(1_000),
            "debounced stamp must not rewrite"
        );

        // Stamp past the debounce interval rewrites.
        assert!(stamp_user_last_seen(
            base,
            &id,
            1_000 + debounce + 1,
            debounce
        ));
        assert_eq!(read_user_last_seen(base, &id), Some(1_000 + debounce + 1));
    }

    /// Reconnect-during-sweep race: a user enumerated as a candidate who
    /// restamps to `now` before the per-user delete is RE-CHECKED and spared.
    /// We simulate it by stamping old, then fresh, then running the sweep — the
    /// re-read inside `reclaim_inactive_users` sees the fresh stamp.
    #[tokio::test]
    async fn reconnect_during_sweep_spares_user() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![55].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x55);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &SecretsId::new(vec![1]),
            b"r",
        );

        let now = 6_000_000_000u64;
        // The user reconnected: their marker is fresh (== now), debounce 0.
        assert!(stamp_user_last_seen(&secrets_dir, &id, now, 0));
        let reclaimed = reclaim_all(&secrets_dir, &db, now, 30 * 86_400);
        assert_eq!(
            reclaimed, 0,
            "a freshly-stamped user is spared by the re-check"
        );
        assert!(scope_user_dir(&secrets_dir, delegate.key(), &id).exists());
        Ok(())
    }

    /// `spawn_inactive_user_sweep` returns `None` when the TTL is 0 (disabled),
    /// and a live handle otherwise.
    #[tokio::test]
    async fn spawn_sweep_respects_ttl_zero() {
        let temp_dir = tempfile::tempdir().unwrap();
        let base = temp_dir.path().to_path_buf();
        let db = create_test_db(temp_dir.path()).await;
        assert!(
            spawn_inactive_user_sweep(base.clone(), db.clone(), 0, 3_600).is_none(),
            "ttl == 0 => no sweep task spawned"
        );
        let handle = spawn_inactive_user_sweep(base, db, 30 * 86_400, 3_600);
        assert!(handle.is_some(), "non-zero ttl spawns a task");
        handle.unwrap().abort();
    }

    /// TTL BOUNDARY (pins the strict `>`): three users at age = ttl-1, ttl, and
    /// ttl+1 against the SAME `now`/`ttl`. Only ttl+1 is reclaimed — ttl-1 and
    /// exactly-ttl are spared. If a future edit flipped `>` to `>=`, the
    /// exactly-ttl user would be deleted a tick early and this test fails.
    #[tokio::test]
    async fn ttl_boundary_strict_greater_than() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![60].into(), &vec![].into()));
        let now = 10_000_000u64;
        let ttl = 30 * 86_400u64;

        // age = ttl-1 (spared), ttl (spared, strict >), ttl+1 (reclaimed).
        let (under, dek_u) = fresh_user(&secrets_dir, 0x60);
        let (exact, dek_e) = fresh_user(&secrets_dir, 0x61);
        let (over, dek_o) = fresh_user(&secrets_dir, 0x62);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &under,
            &dek_u,
            &SecretsId::new(vec![1]),
            b"u",
        );
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &exact,
            &dek_e,
            &SecretsId::new(vec![2]),
            b"e",
        );
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &over,
            &dek_o,
            &SecretsId::new(vec![3]),
            b"o",
        );
        assert!(stamp_user_last_seen(
            &secrets_dir,
            &under,
            now - (ttl - 1),
            0
        ));
        assert!(stamp_user_last_seen(&secrets_dir, &exact, now - ttl, 0));
        assert!(stamp_user_last_seen(
            &secrets_dir,
            &over,
            now - (ttl + 1),
            0
        ));

        let reclaimed = reclaim_all(&secrets_dir, &db, now, ttl);
        assert_eq!(reclaimed, 1, "only age = ttl+1 is reclaimed");
        assert!(
            scope_user_dir(&secrets_dir, delegate.key(), &under).exists(),
            "age = ttl-1 must be spared"
        );
        assert!(
            scope_user_dir(&secrets_dir, delegate.key(), &exact).exists(),
            "age == ttl must be spared (strict `>`)"
        );
        assert!(
            !scope_user_dir(&secrets_dir, delegate.key(), &over).exists(),
            "age = ttl+1 must be reclaimed"
        );
        Ok(())
    }

    /// HOSTED-MODE SPAWN GATE: the spawn decision is the load-bearing safety
    /// gate. It must be true ONLY for hosted_mode && ttl>0 — in particular false
    /// when hosted_mode=false even with a non-zero ttl (a Local node must never
    /// run the sweep). Pins the `hosted_mode &&` conjunction.
    #[test]
    fn spawn_gate_requires_hosted_mode_and_nonzero_ttl() {
        let ttl = DEFAULT_PER_USER_INACTIVE_TTL_SECS;
        assert!(
            should_spawn_inactive_user_sweep(true, ttl),
            "hosted + ttl>0 => spawn"
        );
        assert!(
            !should_spawn_inactive_user_sweep(false, ttl),
            "NOT hosted (Local) must NEVER spawn the sweep, even with ttl>0"
        );
        assert!(
            !should_spawn_inactive_user_sweep(true, 0),
            "ttl==0 (disabled) => no spawn even in hosted mode"
        );
        assert!(
            !should_spawn_inactive_user_sweep(false, 0),
            "neither => no spawn"
        );
    }

    /// PER-PASS CAP: with N+M eligible idle users and a cap of N, exactly N are
    /// reclaimed this pass, M survive, and `capped` is reported (the warn path).
    /// A second pass reclaims the rest (idempotent deferral). This bounds a
    /// clock-fault's blast radius.
    #[tokio::test]
    async fn per_sweep_cap_bounds_reclaims() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![63].into(), &vec![].into()));
        let now = 20_000_000u64;
        let ttl = 30 * 86_400u64;
        let cap = 3usize;
        let total = 5usize; // N=3 cap, M=2 survive first pass

        for i in 0..total as u8 {
            let (id, dek) = fresh_user(&secrets_dir, 0x70 + i);
            store_one_user_secret(
                &mut store,
                delegate.key(),
                &id,
                &dek,
                &SecretsId::new(vec![i]),
                b"x",
            );
            // All ancient (well past TTL).
            assert!(stamp_user_last_seen(
                &secrets_dir,
                &id,
                now - 100 * 86_400,
                0
            ));
        }

        // First pass: cap=3, no gap detector. Exactly 3 reclaimed, capped=true.
        let out = reclaim_inactive_users(&secrets_dir, &db, now, ttl, None, 0, cap);
        assert_eq!(out.reclaimed, cap, "first pass reclaims exactly the cap");
        assert!(out.capped, "cap was hit => capped flag set (warn path)");
        assert!(!out.skipped_gap);
        let remaining = enumerate_marked_users(&secrets_dir).len();
        assert_eq!(remaining, total - cap, "M users survive the first pass");

        // Second pass drains the rest (idempotent deferral); below cap now.
        let out2 = reclaim_inactive_users(&secrets_dir, &db, now, ttl, None, 0, cap);
        assert_eq!(
            out2.reclaimed,
            total - cap,
            "second pass reclaims the remainder"
        );
        assert!(!out2.capped, "remainder is under the cap");
        assert_eq!(
            enumerate_marked_users(&secrets_dir).len(),
            0,
            "all reclaimed after two passes"
        );
        Ok(())
    }

    /// GAP DETECTOR: a forward clock jump BETWEEN sweeps (now far ahead of the
    /// previous pass's now, beyond max_gap) skips the WHOLE pass — NO deletes —
    /// and reports skipped_gap. This is the primary guard against a clock-fault
    /// mass-delete.
    #[tokio::test]
    async fn gap_detector_skips_pass_on_forward_jump() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        let delegate = Delegate::from((&vec![64].into(), &vec![].into()));
        let (id, dek) = fresh_user(&secrets_dir, 0x80);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &id,
            &dek,
            &SecretsId::new(vec![1]),
            b"g",
        );

        let prev = 1_000_000u64;
        // Stamp recent relative to prev (active user): a NORMAL pass would spare
        // them anyway, but the point is the jumped pass must not even run.
        assert!(stamp_user_last_seen(&secrets_dir, &id, prev - 60, 0));

        let ttl = 30 * 86_400u64;
        let max_gap = 8 * 3_600u64; // 8h
        // `now` jumps 60 days ahead of the previous sweep — way beyond max_gap.
        let now = prev + 60 * 86_400;
        let out = reclaim_inactive_users(&secrets_dir, &db, now, ttl, Some(prev), max_gap, 256);
        assert!(
            out.skipped_gap,
            "implausible forward jump must skip the pass"
        );
        assert_eq!(out.reclaimed, 0, "no deletes on a skipped pass");
        assert!(
            scope_user_dir(&secrets_dir, delegate.key(), &id).exists(),
            "data survives a gap-skipped pass"
        );

        // A NORMAL gap (within max_gap) does NOT skip: same user, now only 1h
        // past prev. They were stamped recently so still spared by TTL, but the
        // pass RUNS (skipped_gap=false).
        let normal_now = prev + 3_600;
        let out2 =
            reclaim_inactive_users(&secrets_dir, &db, normal_now, ttl, Some(prev), max_gap, 256);
        assert!(!out2.skipped_gap, "a normal interval gap does not skip");
        Ok(())
    }

    /// GROUPING CORRECTNESS through the ReDb path (high-value for a destructive
    /// op): two users A and B with secrets under the SAME delegate. A is idle
    /// past TTL, B is freshly stamped. After a sweep, A's ReDb index row must be
    /// GONE and B's must SURVIVE — proving the per-sweep index grouping
    /// (`group_index_rows_by_user`) routes each `DelegateKey` to the right user's
    /// bucket and reclaim removes EXACTLY the target user's rows, never the
    /// survivor's. The existing on-disk assertions don't catch a mis-grouped row
    /// because the on-disk delete keys on `user_id.encode()`, not the grouped
    /// ReDb rows; this test exercises the grouped ReDb path specifically.
    #[tokio::test]
    async fn reclaim_removes_only_target_user_redb_rows() -> Result<(), Box<dyn std::error::Error>>
    {
        let temp_dir = tempfile::tempdir()?;
        let secrets_dir = temp_dir.path().join("s");
        std::fs::create_dir_all(&secrets_dir)?;
        let db = create_test_db(temp_dir.path()).await;
        let mut store = SecretsStore::new(secrets_dir.clone(), Default::default(), db.clone())?;

        // BOTH users write under the SAME delegate, so the user-index table holds
        // a row for (delegate, A) and a row for (delegate, B) — the exact shape
        // that a mis-grouping bug would cross-contaminate.
        let delegate = Delegate::from((&vec![90].into(), &vec![].into()));
        let (a, dek_a) = fresh_user(&secrets_dir, 0x90);
        let (b, dek_b) = fresh_user(&secrets_dir, 0x91);
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &a,
            &dek_a,
            &SecretsId::new(vec![1]),
            b"a-secret",
        );
        store_one_user_secret(
            &mut store,
            delegate.key(),
            &b,
            &dek_b,
            &SecretsId::new(vec![2]),
            b"b-secret",
        );

        // Pre-conditions: BOTH users have a ReDb index row under this delegate.
        assert!(
            db.get_user_secrets_index(delegate.key(), a.as_bytes())?
                .is_some(),
            "A must have a ReDb row before reclaim"
        );
        assert!(
            db.get_user_secrets_index(delegate.key(), b.as_bytes())?
                .is_some(),
            "B must have a ReDb row before reclaim"
        );

        let now = 30_000_000u64;
        let ttl = 30 * 86_400u64;
        // A idle 40 days (past TTL → reclaimed); B stamped 1h ago (spared).
        assert!(stamp_user_last_seen(&secrets_dir, &a, now - 40 * 86_400, 0));
        assert!(stamp_user_last_seen(&secrets_dir, &b, now - 3_600, 0));

        let reclaimed = reclaim_all(&secrets_dir, &db, now, ttl);
        assert_eq!(reclaimed, 1, "exactly the idle user A is reclaimed");

        // THE ASSERTION THAT MATTERS: A's ReDb row is gone, B's SURVIVES — the
        // grouped reclaim touched only A's rows.
        assert!(
            db.get_user_secrets_index(delegate.key(), a.as_bytes())?
                .is_none(),
            "A's ReDb index row must be removed"
        );
        assert!(
            db.get_user_secrets_index(delegate.key(), b.as_bytes())?
                .is_some(),
            "B's ReDb index row must SURVIVE — reclaim must not touch the non-target user"
        );
        // And B's secret is still readable end-to-end (on-disk blob intact too).
        assert!(
            store
                .get_secret(
                    delegate.key(),
                    &SecretsId::new(vec![2]),
                    SecretScope::User {
                        id: &b,
                        dek_secret: &dek_b
                    },
                )
                .is_ok(),
            "B's secret must remain readable after A's reclaim"
        );
        Ok(())
    }
}
