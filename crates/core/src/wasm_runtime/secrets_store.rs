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
    RestoreError, RetentionPolicy, SnapshotMetadata, list_snapshots, restore_snapshot_file,
    snapshot_active_value, snapshot_dir_for, thin_snapshots,
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

/// Identifier for a per-user secret namespace.
///
/// A `UserId` is a 32-byte opaque tag that partitions a delegate's secret
/// storage into independent per-user namespaces. In hosted mode (P2 of #4381)
/// it is derived from a connection's user token via [`user_id`] and carried in
/// a [`UserSecretContext`]; outside hosted mode no `UserId` is constructed and
/// every secret operation stays [`SecretScope::Local`].
///
/// The bytes are not secret on their own (they only name a namespace), so
/// `UserId` does NOT zeroize — unlike the `dek_secret` that travels alongside
/// it in [`SecretScope::User`], which is held in `Zeroizing`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UserId([u8; 32]);

impl UserId {
    /// Construct a `UserId` from its raw 32 bytes.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Raw 32-byte tag.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// bs58 (BITCOIN alphabet) encoding of the 32-byte tag, used as the
    /// on-disk path segment under `users/`. Mirrors `DelegateKey::encode`
    /// and `SecretsId::encode` so all three render consistently in paths
    /// and logs.
    pub fn encode(&self) -> String {
        bs58::encode(self.0)
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string()
    }
}

impl std::fmt::Debug for UserId {
    /// Render as the bs58 encoding rather than a raw byte array. The tag is
    /// not secret, but the encoded form is what appears in paths and logs,
    /// so matching it keeps `{:?}` output greppable against the filesystem.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserId({})", self.encode())
    }
}

/// Scope selector for a secret read/write/remove.
///
/// `Local` is the historical single-user path: it MUST behave byte-for-byte
/// identically to pre-#4381 code (same on-disk path, same node-KEK-derived
/// DEK, same ReDb table). `User` adds an optional per-user dimension whose
/// DEK is derived purely from a caller-provided `dek_secret` (NOT the node
/// KEK), making per-user secrets portable by design (P3 export). The `User`
/// scope is selected only in hosted mode (P2 of #4381), when a connection
/// presents a user token; the borrowed `id`/`dek_secret` come from that
/// connection's [`UserSecretContext`].
///
/// The `dek_secret` is borrowed as `&Zeroizing<[u8; 32]>` so the caller
/// retains ownership and the value is wiped when the caller drops it; the
/// store never copies it into a longer-lived buffer.
pub enum SecretScope<'a> {
    /// Single-user / node-local path. Byte-for-byte identical to pre-#4381.
    Local,
    /// Per-user path keyed by `id`, encrypted under a DEK derived solely
    /// from `dek_secret` (node-KEK-independent).
    User {
        id: &'a UserId,
        dek_secret: &'a Zeroizing<[u8; 32]>,
    },
}

/// A per-connection user secret namespace, derived ONCE at the WebSocket
/// connection boundary from a durable user token (P2 of #4381, hosted mode).
///
/// This is the owned counterpart to [`SecretScope::User`]: it holds the
/// `user_id` tag and the `dek_secret` (the latter in `Zeroizing` so it is
/// wiped on drop), and lends them out as a borrowed [`SecretScope::User`] via
/// [`Self::scope`] for the duration of a single secret operation.
///
/// # Security invariant
///
/// A `UserSecretContext` is constructed in EXACTLY ONE place — at WS
/// connection establishment, from the connection's user token (see
/// [`UserSecretContext::from_token`]). It then travels immutably with the
/// connection. Nothing reachable from a delegate's WASM, a delegate message
/// body, a `ClientRequest`, or the app contract id can construct, mutate, or
/// substitute it: the only public constructor takes the raw token bytes and
/// derives both fields deterministically via the domain-separated [`user_id`]
/// / [`user_dek_secret`] hashes. This is what makes the per-user namespace
/// unforgeable from inside the sandbox.
#[derive(Clone)]
pub struct UserSecretContext {
    user_id: UserId,
    dek_secret: Zeroizing<[u8; 32]>,
}

impl UserSecretContext {
    /// Derive a `UserSecretContext` from a connection's opaque user token.
    ///
    /// This is the ONLY constructor. Both the namespace tag and the DEK
    /// secret come solely from `token` via the domain-separated derivations,
    /// so the resulting scope cannot be influenced by anything other than the
    /// token presented at the connection boundary.
    ///
    /// The token is sensitive; this never logs it or the derived secret.
    pub fn from_token(token: &[u8]) -> Self {
        Self {
            user_id: user_id(token),
            dek_secret: user_dek_secret(token),
        }
    }

    /// The non-secret namespace tag for this user. Safe to log.
    pub fn user_id(&self) -> &UserId {
        &self.user_id
    }

    /// Borrow this context as a [`SecretScope::User`] for one secret call.
    ///
    /// The returned scope borrows `self`, so the `dek_secret` is never copied
    /// into a longer-lived buffer — it lives exactly as long as `self`.
    pub fn scope(&self) -> SecretScope<'_> {
        SecretScope::User {
            id: &self.user_id,
            dek_secret: &self.dek_secret,
        }
    }
}

impl std::fmt::Debug for UserSecretContext {
    /// Render only the non-secret `user_id`. The `dek_secret` is NEVER
    /// included so that `{:?}` on any struct that transitively holds a
    /// `UserSecretContext` (e.g. the `DelegateRequest` contract-handler event)
    /// cannot leak key material into logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserSecretContext")
            .field("user_id", &self.user_id)
            .field("dek_secret", &"<redacted>")
            .finish()
    }
}

/// Domain-separation prefix for [`user_id`]. Distinct from
/// [`USER_DEK_SECRET_DOMAIN`] so the same token cannot yield a user id that
/// collides with a dek-secret (and vice-versa).
const USER_ID_DOMAIN: &[u8] = b"freenet-user-id";

/// Domain-separation prefix for [`user_dek_secret`].
const USER_DEK_SECRET_DOMAIN: &[u8] = b"freenet-user-dek";

/// Derive a [`UserId`] from an opaque bearer token.
///
/// `user_id(token) = blake3(USER_ID_DOMAIN || token)`. The domain prefix is
/// distinct from [`user_dek_secret`]'s so the two derivations are
/// independent: knowing a user's id reveals nothing about their dek-secret.
///
/// Consumed by [`UserSecretContext::from_token`] at the WS connection boundary
/// (P2 of #4381, hosted mode).
///
/// The token is sensitive; this function never logs it. The returned id is a
/// non-secret namespace tag, so it is not wrapped in `Zeroizing`.
pub fn user_id(token: &[u8]) -> UserId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(USER_ID_DOMAIN);
    hasher.update(token);
    UserId(*hasher.finalize().as_bytes())
}

/// Derive a per-user DEK secret (HKDF IKM) from an opaque bearer token.
///
/// `user_dek_secret(token) = blake3(USER_DEK_SECRET_DOMAIN || token)`,
/// returned in `Zeroizing` so it is wiped on drop. Distinct domain prefix
/// from [`user_id`] guarantees `user_id(token) != user_dek_secret(token)`
/// as byte strings for every token.
///
/// Consumed by [`UserSecretContext::from_token`] (see [`user_id`]). The token
/// is sensitive; this function never logs it or the derived secret.
pub fn user_dek_secret(token: &[u8]) -> Zeroizing<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(USER_DEK_SECRET_DOMAIN);
    hasher.update(token);
    Zeroizing::new(*hasher.finalize().as_bytes())
}

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

/// `File::create` opens the file with the process umask, which on most
/// distros leaves it world-readable. Every secret blob we land at rest
/// MUST be owner-only. This helper opens at mode 0o600 on Unix in the
/// same syscall as the create — no window where the file is readable
/// under the umask. Windows: no-op (per-user profile dir + ACL is
/// already restrictive).
///
/// **Stale-tmp mode preservation.** `OpenOptions::mode` is only
/// honored on the *create* path: when the file already exists, the
/// existing mode is preserved and the `0o600` request is silently
/// ignored. The `truncate(true)` flag rewrites the *content* of a
/// surviving `.tmp` from a prior crashed run but leaves its mode
/// alone — which on an upgraded host can be the legacy 0o644 from
/// before this helper landed. Belt-and-suspenders: unlink any
/// pre-existing inode at `path` so the open always lands on a fresh
/// 0o600 file.
pub(super) fn create_owner_only(path: &Path) -> std::io::Result<File> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Ensure the secrets-root directory is mode 0o700 on Unix. Operators
/// who created the directory before the permission tightening landed
/// inherited the umask (often 0o755 = world-readable directory entries).
/// We `chmod` it down on every `SecretsStore::new` so a single restart
/// is sufficient to migrate. Windows: no-op.
#[cfg(unix)]
pub(super) fn ensure_owner_only_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    let mode = perms.mode() & 0o777;
    if mode != 0o700 {
        tracing::warn!(
            path = %path.display(),
            existing_mode = format_args!("{mode:o}"),
            "secrets directory was not 0o700; tightening to owner-only"
        );
        perms.set_mode(0o700);
        std::fs::set_permissions(path, perms)?;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn ensure_owner_only_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Tighten EVERY directory segment from `base` (exclusive) down to `full`
/// (inclusive) to owner-only.
///
/// `create_dir_all(full)` materializes all missing intermediate segments
/// under the process umask (typically 0o755 = world-readable directory
/// entries), but the callers historically only chmodded the leaf. For the
/// per-user [`SecretScope::User`] path that leaf is
/// `<delegate>/users/<user_id>`, so the freshly-created `<delegate>/users`
/// and (on a delegate's first write) `<delegate>` would be left
/// world-traversable — a local user could enumerate `users/` subdir names
/// (the per-user id tags) and write timing, violating the owner-only-dir
/// invariant (#4141). This walks the components strictly below `base` and
/// applies [`ensure_owner_only_dir`] to each.
///
/// For [`SecretScope::Local`] `full == base/<delegate>`, so the only
/// segment below `base` is `<delegate>` and this performs the SAME single
/// chmod the pre-#4381 code did — Local behavior is byte-for-byte
/// unchanged. `base` itself is never touched here (it is tightened once in
/// [`SecretsStore::new`]). Best-effort by contract of its callers: they log
/// and continue on a chmod error rather than failing the primary write.
fn ensure_owner_only_tree(base: &Path, full: &Path) -> std::io::Result<()> {
    // The relative path from base to full names exactly the segments we
    // must tighten. If `full` is not under `base` (should never happen —
    // both are derived from `self.base_path`), tighten only the leaf as a
    // conservative fallback.
    let Ok(rel) = full.strip_prefix(base) else {
        return ensure_owner_only_dir(full);
    };
    let mut current = base.to_path_buf();
    for component in rel.components() {
        current.push(component);
        ensure_owner_only_dir(&current)?;
    }
    Ok(())
}

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

        fs::create_dir_all(&scope_path)?;
        // Tighten EVERY segment from base_path down to the leaf, not just the
        // leaf: `create_dir_all` makes the intermediate `users/<id>` (and
        // `<delegate>` on a delegate's first write) under the umask. For Local
        // the leaf IS `<delegate>`, so this is the same single chmod as before.
        if let Err(e) = ensure_owner_only_tree(&self.base_path, &scope_path) {
            tracing::warn!(path = %scope_path.display(), error = %e, "chmod scope dir tree failed");
        }

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

        // Update index in ReDb and in-memory only after the active path has
        // the new value durably committed. The Local and User indices live in
        // separate ReDb tables + separate in-memory maps, so a User write
        // never perturbs a Local entry and vice-versa.
        self.add_to_index(delegate, &scope, secret_key)?;

        // Best-effort thin of the snapshot history. Failures here only mean
        // we keep more snapshots than the policy targets, which is harmless
        // and self-correcting on the next write.
        if self.snapshots_enabled {
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

    /// Enumerate every `(DelegateKey, secret_hash)` held for `scope`.
    ///
    /// Reads from the in-memory index maps (kept in lock-step with ReDb), so
    /// it reflects exactly what `get_secret` could read. For `User` scope only
    /// the `id` field of the scope is consulted (the `dek_secret` is unused
    /// here — enumeration is a metadata walk, not a decrypt).
    fn enumerate_scope(&self, scope: &SecretScope<'_>) -> Vec<(DelegateKey, SecretKey)> {
        let mut out = Vec::new();
        match scope {
            SecretScope::Local => {
                for entry in self.key_to_secret_part.iter() {
                    let delegate = entry.key().clone();
                    for hash in entry.value() {
                        out.push((delegate.clone(), *hash));
                    }
                }
            }
            SecretScope::User { id, .. } => {
                for entry in self.user_key_to_secret_part.iter() {
                    let (delegate, user) = entry.key();
                    if user != *id {
                        continue;
                    }
                    for hash in entry.value() {
                        out.push((delegate.clone(), *hash));
                    }
                }
            }
        }
        out
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
        let refs = self.enumerate_scope(&scope);
        let mut entries = Vec::with_capacity(refs.len());
        for (delegate, secret_hash) in refs {
            let plaintext = self.read_secret_by_hash(&delegate, &secret_hash, &scope)?;
            entries.push(ExportSecretEntry {
                delegate_key: delegate,
                secret_hash,
                plaintext,
            });
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

    /// Snapshot history is per-scope: a second `User` write snapshots the
    /// prior version, which `list_snapshots(.., User)` enumerates and
    /// `restore_snapshot(.., User)` rolls back to — all without touching a
    /// same-`SecretsId` `Local` secret's history or active value.
    #[tokio::test]
    async fn user_scope_second_write_creates_listable_snapshot()
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

        // A Local secret with the SAME SecretsId — must be unaffected by all
        // the User-scope writes/restore below.
        store.store_secret(
            delegate.key(),
            &secret_id,
            SecretScope::Local,
            Zeroizing::new(b"local-untouched".to_vec()),
        )?;

        // First User write: no prior value, so no snapshot yet.
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

        // Second User write (distinct value): snapshots the prior version.
        store.store_secret(
            delegate.key(),
            &secret_id,
            user_scope(),
            Zeroizing::new(b"user-v2".to_vec()),
        )?;
        let snaps = store.list_snapshots(delegate.key(), &secret_id, user_scope())?;
        assert_eq!(
            snaps.len(),
            1,
            "second User write must snapshot the prior version"
        );

        // Active is v2; restoring the snapshot rolls back to v1.
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, user_scope())?
                .to_vec(),
            b"user-v2".to_vec()
        );
        store.restore_snapshot(
            delegate.key(),
            &secret_id,
            user_scope(),
            snaps[0].timestamp_ms,
        )?;
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, user_scope())?
                .to_vec(),
            b"user-v1".to_vec(),
            "restore must put the User v1 plaintext back"
        );

        // The same-SecretsId Local secret is byte-for-byte untouched: its
        // value is unchanged and it has no snapshot history (it was written
        // exactly once).
        assert_eq!(
            store
                .get_secret(delegate.key(), &secret_id, SecretScope::Local)?
                .to_vec(),
            b"local-untouched".to_vec(),
            "User-scope writes/restore must not perturb the Local secret"
        );
        assert!(
            store
                .list_snapshots(delegate.key(), &secret_id, SecretScope::Local)?
                .is_empty(),
            "Local secret written once must have no snapshot history"
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

        // Pre-condition: file present, index empty → invisible to enumeration.
        let file_path = secrets_dir
            .join(delegate.key().encode())
            .join(secret_id.encode());
        assert!(file_path.exists(), "secret file should still be on disk");
        assert!(
            store.export_scope_entries(SecretScope::Local)?.is_empty(),
            "pre-condition: unindexed secret must be invisible to enumeration"
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

        assert!(
            store.export_scope_entries(ctx.scope())?.is_empty(),
            "pre-condition: unindexed user secret invisible"
        );

        let wrote = store.import_secret_by_hash(
            delegate.key(),
            &secret_hash,
            ctx.scope(),
            Zeroizing::new(b"uval".to_vec()),
            false,
        )?;
        assert!(!wrote);

        let entries = store.export_scope_entries(ctx.scope())?;
        assert_eq!(
            entries.len(),
            1,
            "user secret must be enumerable after repair"
        );
        assert_eq!(entries[0].secret_hash, secret_hash);
        Ok(())
    }
}
