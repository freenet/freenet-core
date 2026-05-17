//! Node KEK (Key Encryption Key) backend abstraction for delegate
//! secrets-at-rest.
//!
//! The KEK is a 32-byte secret used as the master key from which every
//! per-delegate DEK (Data Encryption Key) is derived via HKDF-SHA256.
//! See `wasm_runtime/secrets_store.rs::derive_delegate_dek`.
//!
//! ## Backends
//!
//! Three backends, tried in order during the first-start resolver:
//!
//! 1. **OS keyring** (`KeyringKek`) — `keyring` crate over the platform
//!    secret store (macOS Keychain, Windows Credential Manager, Linux
//!    Secret Service / kernel keyutils). Disk never sees the key.
//! 2. **Systemd credential** (`SystemdCredentialKek`) —
//!    `$CREDENTIALS_DIRECTORY/freenet-kek` populated by systemd
//!    `LoadCredentialEncrypted=`. Decrypted by systemd before unit start;
//!    backs the typical headless-Linux deployment.
//! 3. **File** (`FileKek`) — `secrets_dir/node_kek` (0o600,
//!    atomic-create). The "no better option" fallback for unmanaged
//!    headless installs. Emits a one-line WARN at start so operators
//!    know they are running with the weakest backend.
//!
//! The resolver runs only on first start with no `kek_backend` recorded
//! in the node config. Once chosen, the backend is persisted (see
//! `KekBackendKind`) and subsequent starts use it exclusively — a
//! transient keyring-daemon outage surfaces as a hard error, never a
//! silent demotion to a weaker backend.
//!
//! ## Security model
//!
//! - The KEK itself never appears in application memory beyond the few
//!   microseconds of `derive_delegate_dek`. Backends MUST zeroize
//!   intermediate buffers on drop where possible (`Zeroizing<Vec<u8>>`).
//! - DEKs are derived deterministically from `(KEK, delegate_key)` and
//!   cached in-process. They are NEVER persisted — restarting the node
//!   re-derives them from the same KEK.
//! - Per-write nonces (PR #4143) are random and prepended to each
//!   ciphertext blob; nonce uniqueness does not depend on the KEK.

use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use super::CIPHER_SIZE;

/// Filename (relative to `secrets_dir`) of the file-KEK fallback.
pub(crate) const NODE_KEK_FILENAME: &str = "node_kek";

/// Filename (relative to `secrets_dir`) of the marker recording which
/// backend currently holds the node KEK. Plain UTF-8, one line, one of:
/// `keyring`, `systemd`, `file`. Persisting the choice prevents a
/// transient keyring-daemon outage from silently demoting the node to
/// a weaker backend on next start — the recorded backend MUST load
/// successfully or the node fails to boot. Operators move between
/// backends via `fdev secrets kek-migrate --to <kind>`, which atomically
/// rewrites this marker after the new backend successfully stores the
/// KEK.
pub(crate) const KEK_BACKEND_MARKER_FILENAME: &str = "kek_backend";

/// Filename of the systemd-credential KEK (relative to
/// `$CREDENTIALS_DIRECTORY`). Operators set this via
/// `LoadCredentialEncrypted=freenet-kek:/path/to/encrypted` in the
/// systemd unit; the daemon decrypts and exposes the plaintext under
/// this name before the unit starts.
pub(crate) const SYSTEMD_CRED_NAME: &str = "freenet-kek";

/// `keyring` crate "service" identifier used for the OS keyring entry.
/// Together with `KEYRING_USER` it uniquely names the KEK in the
/// platform's secret store. Rotating these strings is a hard break and
/// MUST coincide with a KEK migration step.
pub(crate) const KEYRING_SERVICE: &str = "freenet-core";
pub(crate) const KEYRING_USER: &str = "node-kek";

/// Length of the node KEK in bytes. Matches XChaCha20-Poly1305 key
/// size so the KEK can directly key the HKDF input.
pub const KEK_SIZE: usize = CIPHER_SIZE;

/// Tag identifying which backend currently holds the KEK. Persisted in
/// the node config so a transient outage of a stronger backend cannot
/// silently demote to a weaker one. To change backends, operators run
/// `fdev secrets kek-migrate --to <kind>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KekBackendKind {
    Keyring,
    Systemd,
    File,
}

impl KekBackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            KekBackendKind::Keyring => "keyring",
            KekBackendKind::Systemd => "systemd",
            KekBackendKind::File => "file",
        }
    }
}

impl std::fmt::Display for KekBackendKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum KekError {
    #[error("KEK file I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("OS keyring backend unavailable or denied: {0}")]
    Keyring(String),
    #[error("Systemd credential `{name}` not found (CREDENTIALS_DIRECTORY={dir:?})")]
    SystemdMissing { name: String, dir: Option<PathBuf> },
    #[error("KEK on disk is {actual} bytes; expected {expected}")]
    InvalidLength { actual: usize, expected: usize },
    #[error("KEK already exists in backend; refusing to overwrite without explicit rotation")]
    AlreadyExists,
    #[error("No KEK backend available — keyring/systemd/file all failed")]
    NoBackend,
}

/// Behavior every KEK backend MUST implement. `load` returns `Ok(None)`
/// when no KEK exists yet (first start); the resolver then calls
/// `store` to seed one.
pub trait KekBackend: Send + Sync {
    /// Identify which backend this is. Surfaced via
    /// `fdev secrets kek-status` and recorded in the
    /// `secrets_dir/kek_backend` marker file.
    fn kind(&self) -> KekBackendKind;
    /// Read the current KEK from this backend.
    ///
    /// Returns `Ok(None)` when no KEK has been provisioned yet
    /// (first-start flow — the resolver then calls `store` to seed
    /// one). Returns `Err` for transport / permission / format
    /// failures so the caller can distinguish them from a clean
    /// "absent" state.
    fn load(&self) -> Result<Option<Zeroizing<[u8; KEK_SIZE]>>, KekError>;
    /// Persist a new KEK. MUST fail with `KekError::AlreadyExists` if a
    /// KEK is already present — rotation goes through a dedicated path
    /// (`fdev secrets kek-rotate`) so accidental double-`store` cannot
    /// silently destroy the existing key.
    fn store(&self, kek: &[u8; KEK_SIZE]) -> Result<(), KekError>;
    /// Remove the KEK from this backend.
    ///
    /// Idempotent: if no KEK was present, returns `Ok(())`. Used by
    /// `fdev secrets kek-migrate` after the migration target has
    /// successfully stored the KEK.
    fn delete(&self) -> Result<(), KekError>;
}

// =============================================================================
// Backend: OS keyring (apple Keychain / windows Credential Manager /
// linux Secret Service or kernel keyutils)
// =============================================================================

pub struct KeyringKek {
    entry: keyring::Entry,
}

impl KeyringKek {
    /// Open a handle to the OS-keyring KEK entry under
    /// `(KEYRING_SERVICE, KEYRING_USER)`. Does not actually contact the
    /// keyring daemon — that happens on `load` / `store`. Failure here
    /// indicates the `keyring` crate could not build an `Entry` at all
    /// (e.g. missing feature support for the target platform).
    pub fn new() -> Result<Self, KekError> {
        let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_USER)
            .map_err(|e| KekError::Keyring(format!("Entry::new failed: {e}")))?;
        Ok(Self { entry })
    }
}

impl KekBackend for KeyringKek {
    fn kind(&self) -> KekBackendKind {
        KekBackendKind::Keyring
    }

    fn load(&self) -> Result<Option<Zeroizing<[u8; KEK_SIZE]>>, KekError> {
        match self.entry.get_secret() {
            Ok(bytes) => {
                if bytes.len() != KEK_SIZE {
                    return Err(KekError::InvalidLength {
                        actual: bytes.len(),
                        expected: KEK_SIZE,
                    });
                }
                let mut buf = Zeroizing::new([0u8; KEK_SIZE]);
                buf.copy_from_slice(&bytes);
                Ok(Some(buf))
            }
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(KekError::Keyring(format!("get_secret failed: {e}"))),
        }
    }

    fn store(&self, kek: &[u8; KEK_SIZE]) -> Result<(), KekError> {
        // Refuse to overwrite — keyring API has no atomic create-only
        // primitive, so explicitly check first. Race window is narrow
        // (the resolver only calls this on first start with sole
        // ownership of the keyring service/user pair).
        if self.entry.get_secret().is_ok() {
            return Err(KekError::AlreadyExists);
        }
        self.entry
            .set_secret(kek)
            .map_err(|e| KekError::Keyring(format!("set_secret failed: {e}")))
    }

    fn delete(&self) -> Result<(), KekError> {
        match self.entry.delete_credential() {
            Ok(()) => Ok(()),
            Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(KekError::Keyring(format!("delete_credential failed: {e}"))),
        }
    }
}

// =============================================================================
// Backend: systemd credential ($CREDENTIALS_DIRECTORY/freenet-kek)
// =============================================================================

pub struct SystemdCredentialKek {
    path: PathBuf,
}

impl SystemdCredentialKek {
    /// Construct the backend handle if the systemd
    /// `CREDENTIALS_DIRECTORY` environment variable is set (i.e. the
    /// freenet process was started by systemd with
    /// `LoadCredentialEncrypted=freenet-kek:...` on the unit).
    ///
    /// Returns `Some(_)` if viable; `None` if `CREDENTIALS_DIRECTORY`
    /// is unset, in which case the resolver moves on to the next
    /// backend in the fallback chain.
    pub fn new() -> Option<Self> {
        let dir = std::env::var_os("CREDENTIALS_DIRECTORY")?;
        Some(Self {
            path: PathBuf::from(dir).join(SYSTEMD_CRED_NAME),
        })
    }
}

impl KekBackend for SystemdCredentialKek {
    fn kind(&self) -> KekBackendKind {
        KekBackendKind::Systemd
    }

    fn load(&self) -> Result<Option<Zeroizing<[u8; KEK_SIZE]>>, KekError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                if bytes.len() != KEK_SIZE {
                    return Err(KekError::InvalidLength {
                        actual: bytes.len(),
                        expected: KEK_SIZE,
                    });
                }
                let mut buf = Zeroizing::new([0u8; KEK_SIZE]);
                buf.copy_from_slice(&bytes);
                Ok(Some(buf))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(KekError::SystemdMissing {
                name: SYSTEMD_CRED_NAME.to_string(),
                dir: self.path.parent().map(|p| p.to_path_buf()),
            }),
            Err(e) => Err(KekError::Io(e)),
        }
    }

    fn store(&self, _kek: &[u8; KEK_SIZE]) -> Result<(), KekError> {
        // Systemd credentials are populated by the service manager, not
        // by freenet. An operator who selects this backend MUST provide
        // the credential out-of-band; the resolver should never reach
        // store() on this backend with a freshly generated KEK.
        Err(KekError::Io(io::Error::other(
            "systemd credentials are populated by the service manager; freenet cannot \
             write them. Generate the KEK out-of-band and configure \
             `LoadCredentialEncrypted=freenet-kek:/path` on the unit.",
        )))
    }

    fn delete(&self) -> Result<(), KekError> {
        // Same reasoning as `store`: deletion is the service manager's
        // responsibility. We surface as a no-op success so
        // `fdev secrets kek-migrate --from systemd` can proceed.
        Ok(())
    }
}

// =============================================================================
// Backend: file (secrets_dir/node_kek, 0o600, atomic create)
// =============================================================================

pub struct FileKek {
    path: PathBuf,
}

impl FileKek {
    /// Construct the file-backend handle pointing at
    /// `secrets_dir/node_kek` (NODE_KEK_FILENAME). Does not touch the
    /// filesystem — that happens on `load` / `store`.
    pub fn new(secrets_dir: &Path) -> Self {
        Self {
            path: secrets_dir.join(NODE_KEK_FILENAME),
        }
    }
}

impl KekBackend for FileKek {
    fn kind(&self) -> KekBackendKind {
        KekBackendKind::File
    }

    fn load(&self) -> Result<Option<Zeroizing<[u8; KEK_SIZE]>>, KekError> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                if bytes.len() != KEK_SIZE {
                    return Err(KekError::InvalidLength {
                        actual: bytes.len(),
                        expected: KEK_SIZE,
                    });
                }
                let mut buf = Zeroizing::new([0u8; KEK_SIZE]);
                buf.copy_from_slice(&bytes);
                Ok(Some(buf))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(KekError::Io(e)),
        }
    }

    fn store(&self, kek: &[u8; KEK_SIZE]) -> Result<(), KekError> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&self.path).map_err(|e| {
            if e.kind() == io::ErrorKind::AlreadyExists {
                KekError::AlreadyExists
            } else {
                KekError::Io(e)
            }
        })?;
        use std::io::Write;
        file.write_all(kek)?;
        file.sync_all()?;
        Ok(())
    }

    fn delete(&self) -> Result<(), KekError> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(KekError::Io(e)),
        }
    }
}

// =============================================================================
// Resolver: first-start backend selection
// =============================================================================

/// Walk the preferred backend chain and return the first one that
/// successfully `load`s an existing KEK, OR the first one that
/// successfully `store`s a freshly generated one. The selected backend
/// is returned along with the KEK so the caller can persist the
/// `KekBackendKind` in node config.
///
/// Order: `Keyring` → `Systemd` → `File`.
///
/// `kek_supplier` is called once if every backend's `load` returns
/// `Ok(None)`; it MUST return a freshly generated KEK seeded from OS
/// entropy. Injected as a closure rather than called inline so tests
/// can supply a deterministic value.
pub fn resolve_first_start(
    secrets_dir: &Path,
    mut kek_supplier: impl FnMut() -> Zeroizing<[u8; KEK_SIZE]>,
) -> Result<(KekBackendKind, Zeroizing<[u8; KEK_SIZE]>), KekError> {
    // Stage 1: try to LOAD an existing KEK from any backend. This
    // handles the case where a prior install configured one but the
    // node config got reset.
    let backends: Vec<Box<dyn KekBackend>> = build_chain(secrets_dir);
    for backend in &backends {
        match backend.load() {
            Ok(Some(kek)) => {
                tracing::info!(
                    backend = %backend.kind(),
                    "Loaded existing KEK from backend"
                );
                return Ok((backend.kind(), kek));
            }
            Ok(None) => continue,
            Err(e) => {
                tracing::debug!(
                    backend = %backend.kind(),
                    "Backend load failed: {e}"
                );
                continue;
            }
        }
    }

    // Stage 2: no KEK anywhere. Generate one and try to store in the
    // first writable backend.
    let new_kek = kek_supplier();
    for backend in &backends {
        match backend.store(&new_kek) {
            Ok(()) => {
                let kind = backend.kind();
                if kind == KekBackendKind::File {
                    tracing::warn!(
                        "Provisioned KEK to the FILE backend ({}/{NODE_KEK_FILENAME}). \
                         This is the weakest option — anyone with read access to the \
                         secrets directory can decrypt all delegate secrets. Configure \
                         a stronger backend (OS keyring or systemd credential) and \
                         migrate with `fdev secrets kek-migrate`.",
                        secrets_dir.display()
                    );
                } else {
                    tracing::info!(backend = %kind, "Provisioned new KEK to backend");
                }
                return Ok((kind, new_kek));
            }
            Err(e) => {
                tracing::debug!(
                    backend = %backend.kind(),
                    "Backend store failed: {e}; falling through"
                );
                continue;
            }
        }
    }

    Err(KekError::NoBackend)
}

/// Load the KEK from a pre-selected backend. Used on every start after
/// the first. A failure here MUST be surfaced as a hard error — the
/// caller (config layer) must not silently fall back to another backend,
/// since that would re-derive different DEKs and make every existing
/// delegate secret unreadable.
pub fn load_from_backend(
    kind: KekBackendKind,
    secrets_dir: &Path,
) -> Result<Zeroizing<[u8; KEK_SIZE]>, KekError> {
    let backend = build_backend_for(kind, secrets_dir)?;
    match backend.load()? {
        Some(kek) => Ok(kek),
        None => Err(KekError::NoBackend),
    }
}

/// Read the persisted backend choice from `secrets_dir/kek_backend`.
/// Returns `Ok(None)` if the marker does not exist yet (first-start
/// flow). Returns an error if the marker exists but contains an
/// unrecognized value, so a corrupt marker fails the boot rather than
/// silently demoting to file backend.
pub fn read_backend_marker(secrets_dir: &Path) -> Result<Option<KekBackendKind>, KekError> {
    let path = secrets_dir.join(KEK_BACKEND_MARKER_FILENAME);
    match std::fs::read_to_string(&path) {
        Ok(s) => match s.trim() {
            "keyring" => Ok(Some(KekBackendKind::Keyring)),
            "systemd" => Ok(Some(KekBackendKind::Systemd)),
            "file" => Ok(Some(KekBackendKind::File)),
            other => Err(KekError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{KEK_BACKEND_MARKER_FILENAME} contains unrecognized backend `{other}`; \
                     expected one of: keyring, systemd, file"
                ),
            ))),
        },
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(KekError::Io(e)),
    }
}

/// Write the chosen backend kind to `secrets_dir/kek_backend`. Uses
/// `OpenOptions::create_new` so a pre-existing marker is preserved —
/// callers performing migration are expected to go through
/// `replace_backend_marker`, which handles atomic overwrite.
fn write_backend_marker(secrets_dir: &Path, kind: KekBackendKind) -> Result<(), KekError> {
    std::fs::create_dir_all(secrets_dir)?;
    let path = secrets_dir.join(KEK_BACKEND_MARKER_FILENAME);
    use std::io::Write;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&path)?;
    writeln!(file, "{}", kind.as_str())?;
    file.sync_all()?;
    Ok(())
}

/// Atomically replace the persisted backend marker with `kind`. Used
/// by `fdev secrets kek-migrate` after the target backend has
/// successfully stored the migrated KEK. Writes to a sibling `.tmp`
/// file with the same 0o600 perms + `sync_all` discipline as
/// `write_backend_marker`, then `rename`s onto the live marker (atomic
/// on POSIX, `MoveFileExW MOVEFILE_REPLACE_EXISTING` on Windows).
///
/// A crash between the tmp write and the rename leaves the OLD marker
/// in place — the next node start loads from the (still-populated)
/// source backend, which is the correct safe-fallback behavior. A
/// crash after the rename leaves the NEW marker in place — the source
/// backend may still hold the old KEK (cleanup happens after this call
/// in `kek-migrate`), but the next node start loads from the new
/// backend, which is the intended end state.
pub fn replace_backend_marker(secrets_dir: &Path, kind: KekBackendKind) -> Result<(), KekError> {
    std::fs::create_dir_all(secrets_dir)?;
    let tmp = secrets_dir.join(format!("{KEK_BACKEND_MARKER_FILENAME}.tmp"));
    let path = secrets_dir.join(KEK_BACKEND_MARKER_FILENAME);
    use std::io::Write;
    {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts.open(&tmp)?;
        writeln!(file, "{}", kind.as_str())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// End-to-end KEK acquisition. On first start (no marker present) this
/// runs the fallback resolver, persists the chosen backend kind, and
/// returns the KEK. On subsequent starts it reads the marker and loads
/// strictly from the recorded backend — a load failure here is a hard
/// boot error.
///
/// `kek_supplier` is called only on first start when every backend's
/// `load` returns `Ok(None)`. It MUST return a freshly generated KEK
/// seeded from OS entropy. Tests can inject a deterministic value.
pub fn ensure_kek_loaded(
    secrets_dir: &Path,
    kek_supplier: impl FnMut() -> Zeroizing<[u8; KEK_SIZE]>,
) -> Result<(KekBackendKind, Zeroizing<[u8; KEK_SIZE]>), KekError> {
    match read_backend_marker(secrets_dir)? {
        Some(kind) => {
            let kek = load_from_backend(kind, secrets_dir)?;
            tracing::debug!(
                backend = %kind,
                "Loaded KEK from previously-recorded backend"
            );
            Ok((kind, kek))
        }
        None => {
            let (kind, kek) = resolve_first_start(secrets_dir, kek_supplier)?;
            write_backend_marker(secrets_dir, kind)?;
            tracing::info!(
                backend = %kind,
                "Recorded chosen KEK backend in {KEK_BACKEND_MARKER_FILENAME}"
            );
            Ok((kind, kek))
        }
    }
}

fn build_chain(secrets_dir: &Path) -> Vec<Box<dyn KekBackend>> {
    let mut chain: Vec<Box<dyn KekBackend>> = Vec::new();
    if let Ok(b) = KeyringKek::new() {
        chain.push(Box::new(b));
    }
    if let Some(b) = SystemdCredentialKek::new() {
        chain.push(Box::new(b));
    }
    chain.push(Box::new(FileKek::new(secrets_dir)));
    chain
}

/// Public constructor used by `fdev secrets kek-migrate` and other
/// out-of-process tooling to build a backend handle for a specific
/// `KekBackendKind`. In-process callers (e.g. `SecretsStore::new`)
/// should prefer `ensure_kek_loaded`, which combines marker resolution
/// with backend construction.
pub fn build_backend_for(
    kind: KekBackendKind,
    secrets_dir: &Path,
) -> Result<Box<dyn KekBackend>, KekError> {
    match kind {
        KekBackendKind::Keyring => Ok(Box::new(KeyringKek::new()?)),
        KekBackendKind::Systemd => {
            SystemdCredentialKek::new()
                .map(|b| Box::new(b) as _)
                .ok_or(KekError::SystemdMissing {
                    name: SYSTEMD_CRED_NAME.to_string(),
                    dir: None,
                })
        }
        KekBackendKind::File => Ok(Box::new(FileKek::new(secrets_dir))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_kek(byte: u8) -> Zeroizing<[u8; KEK_SIZE]> {
        Zeroizing::new([byte; KEK_SIZE])
    }

    #[test]
    fn file_kek_roundtrip_and_0o600() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileKek::new(dir.path());
        assert!(backend.load().expect("load").is_none(), "fresh dir empty");
        let kek = fresh_kek(0x42);
        backend.store(&kek).expect("store");
        let loaded = backend.load().expect("load").expect("present");
        assert_eq!(*loaded, *kek);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join(NODE_KEK_FILENAME))
                .expect("metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "KEK file must be 0o600, got {mode:o}");
        }
    }

    #[test]
    fn file_kek_store_twice_refuses_to_overwrite() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileKek::new(dir.path());
        backend.store(&fresh_kek(1)).expect("first store");
        let err = backend
            .store(&fresh_kek(2))
            .expect_err("second store must fail");
        assert!(
            matches!(err, KekError::AlreadyExists),
            "expected AlreadyExists, got {err:?}"
        );
        // First-store value must be unchanged.
        let loaded = backend.load().expect("load").expect("present");
        assert_eq!(*loaded, *fresh_kek(1));
    }

    #[test]
    fn file_kek_invalid_length_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join(NODE_KEK_FILENAME);
        std::fs::write(&path, b"too-short").expect("seed garbage");
        let backend = FileKek::new(dir.path());
        let err = backend.load().expect_err("load must fail");
        assert!(
            matches!(err, KekError::InvalidLength { .. }),
            "expected InvalidLength, got {err:?}"
        );
    }

    #[test]
    fn file_kek_delete_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = FileKek::new(dir.path());
        backend.delete().expect("missing delete is no-op");
        backend.store(&fresh_kek(7)).expect("store");
        backend.delete().expect("present delete ok");
        assert!(backend.load().expect("load").is_none());
    }

    #[test]
    fn resolver_provisions_file_when_others_fail() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Keyring may or may not be available on the CI host. Drive
        // the resolver and assert one of the backends won; the test
        // doesn't care which, only that the resolver doesn't error
        // when at least the file backend is reachable.
        let supplied = fresh_kek(0xAA);
        let (kind, kek) = resolve_first_start(dir.path(), || supplied.clone()).expect("resolve");
        match kind {
            KekBackendKind::File => assert_eq!(*kek, *supplied),
            KekBackendKind::Keyring | KekBackendKind::Systemd => {
                // Acceptable — backend was writable in the test environment.
            }
        }
    }

    #[test]
    fn marker_roundtrip_and_unknown_value_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(read_backend_marker(dir.path()).expect("read").is_none());
        write_backend_marker(dir.path(), KekBackendKind::File).expect("write");
        assert_eq!(
            read_backend_marker(dir.path()).expect("read"),
            Some(KekBackendKind::File)
        );
        // Overwrite with garbage and confirm read errors instead of
        // silently picking a default.
        let path = dir.path().join(KEK_BACKEND_MARKER_FILENAME);
        std::fs::write(&path, b"oops").expect("overwrite");
        let err = read_backend_marker(dir.path()).expect_err("garbage marker must error");
        assert!(
            matches!(err, KekError::Io(_)),
            "expected Io InvalidData, got {err:?}"
        );
    }

    #[test]
    fn ensure_kek_loaded_first_start_writes_marker_and_returns_kek() {
        let dir = tempfile::tempdir().expect("tempdir");
        let supplied = fresh_kek(0xCC);
        let (kind, kek) =
            ensure_kek_loaded(dir.path(), || supplied.clone()).expect("first-start ensure");
        // Marker MUST exist after first-start.
        let marker_kind = read_backend_marker(dir.path())
            .expect("read")
            .expect("present");
        assert_eq!(marker_kind, kind);
        // On a fresh dir with no usable keyring/systemd, the file
        // backend wins and KEK equals what we supplied.
        if kind == KekBackendKind::File {
            assert_eq!(*kek, *supplied);
        }
    }

    #[test]
    fn ensure_kek_loaded_second_start_uses_recorded_backend_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Seed: file backend with a known KEK, marker recording File.
        FileKek::new(dir.path())
            .store(&fresh_kek(0x11))
            .expect("seed file backend");
        write_backend_marker(dir.path(), KekBackendKind::File).expect("write marker");

        // Second start: kek_supplier MUST NOT be called (assert via
        // unreachable closure).
        let (kind, kek) = ensure_kek_loaded(dir.path(), || {
            unreachable!("second start must not invoke kek_supplier")
        })
        .expect("second-start ensure");
        assert_eq!(kind, KekBackendKind::File);
        assert_eq!(*kek, *fresh_kek(0x11));
    }

    #[test]
    fn ensure_kek_loaded_second_start_errors_when_recorded_backend_empty() {
        // Marker says File but no node_kek file present → hard error,
        // never silent fallback to keyring/systemd resolver.
        let dir = tempfile::tempdir().expect("tempdir");
        write_backend_marker(dir.path(), KekBackendKind::File).expect("write marker");
        let err = ensure_kek_loaded(dir.path(), || fresh_kek(0xFF))
            .expect_err("missing KEK behind recorded backend must error");
        assert!(
            matches!(err, KekError::NoBackend | KekError::Io(_)),
            "expected NoBackend or Io, got {err:?}"
        );
    }

    #[test]
    fn resolver_loads_existing_file_kek_on_second_call() {
        let dir = tempfile::tempdir().expect("tempdir");
        FileKek::new(dir.path())
            .store(&fresh_kek(0x55))
            .expect("seed file backend");
        // Second resolve_first_start call MUST load the existing KEK
        // from the file backend without overwriting it. (Keyring path
        // may not even reach a usable backend on this CI host; either
        // way we should see the same KEK we stored.)
        let supplied = fresh_kek(0x99);
        let (_kind, kek) = resolve_first_start(dir.path(), || supplied.clone()).expect("resolve");
        // If file path won, KEK is 0x55. If keyring won, KEK could be
        // anything that was already there. Pin the file case
        // explicitly by also checking the file content.
        let file_kek = FileKek::new(dir.path())
            .load()
            .expect("load")
            .expect("present");
        assert_eq!(
            *file_kek,
            *fresh_kek(0x55),
            "file KEK must not be overwritten"
        );
        // And the resolver returned SOME kek, not the freshly generated one.
        assert_ne!(*kek, *supplied);
    }
}
