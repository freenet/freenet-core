use std::path::Path;

use aes_gcm::KeyInit;
use chacha20poly1305::{XChaCha20Poly1305, XNonce, aead::OsRng};

use super::*;

const NONCE_SIZE: usize = 24;
const CIPHER_SIZE: usize = 32;

/// Filename (relative to `secrets_dir`) of the auto-persisted per-node
/// delegate cipher introduced in freenet-core PR after the removal of
/// `DelegateRequest::DEFAULT_CIPHER` from freenet-stdlib 0.8.0. Nodes
/// generate this on first start (if no `--cipher` flag is supplied) and
/// reuse it across restarts.
pub(crate) const DELEGATE_CIPHER_FILENAME: &str = "delegate_cipher";

/// Historical `DelegateRequest::DEFAULT_NONCE` value, retained in core
/// purely for **read-side legacy decrypt** of on-disk delegate secret
/// files that pre-date the per-write-nonce format (freenet-core PR #4143).
///
/// New writes never use this value — every `store_secret` generates a
/// fresh random 24-byte nonce via `OsRng` and persists it inline with
/// the ciphertext under the version-prefixed format. This constant
/// exists only to populate `Secrets::nonce` so the existing
/// `Encryption::legacy_nonce` fallback in `SecretsStore::get_secret`
/// can still decrypt pre-#4143 files written under the old default
/// fallback path.
///
/// Removed from `freenet-stdlib` public API in 0.8.0 because exposing
/// it as a public const was what allowed default-configured nodes to
/// encrypt under a world-known nonce.
pub(crate) const LEGACY_DEFAULT_NONCE: [u8; 24] = [
    57, 18, 79, 116, 63, 134, 93, 39, 208, 161, 156, 229, 222, 247, 111, 79, 210, 126, 127, 55,
    224, 150, 139, 80,
];

/// Historical `DelegateRequest::DEFAULT_CIPHER` value, retained in core
/// purely for **read-side legacy decrypt**. See [`LEGACY_DEFAULT_NONCE`].
///
/// Tests in this module use this constant directly to construct legacy
/// `Secrets` snapshots; production code never seeds `Secrets::cipher`
/// from it — `SecretArgs::build` auto-generates a fresh cipher per node
/// and persists it to `secrets_dir/delegate_cipher`.
pub(crate) const LEGACY_DEFAULT_CIPHER: [u8; 32] = [
    0, 24, 22, 150, 112, 207, 24, 65, 182, 161, 169, 227, 66, 182, 237, 215, 206, 164, 58, 161, 64,
    108, 157, 195, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// Generate a fresh 32-byte XChaCha20-Poly1305 key via `OsRng`.
///
/// `OsRng` is the documented exception to the project-wide
/// `.claude/rules/code-style.md` ban on `rand::thread_rng()` /
/// `rand::random()` in `crates/core/`: cryptographic key material MUST
/// come from the OS entropy pool (e.g. `/dev/urandom`), not from a
/// deterministic simulation RNG. This call runs at node startup, before
/// any `TimeSource` / `GlobalRng` simulation harness would be in scope.
fn generate_cipher_key() -> [u8; CIPHER_SIZE] {
    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let mut out = [0u8; CIPHER_SIZE];
    out.copy_from_slice(key.as_slice());
    out
}

/// Persist a cipher key to disk, creating the file atomically with
/// 0o600 permissions on Unix (no window where another user could open
/// the file at a more permissive mode).
///
/// Uses `OpenOptions::create_new(true)` so the call fails with
/// `AlreadyExists` if the file appeared between the caller's existence
/// check and this write — preventing a TOCTOU where two concurrent
/// freenet processes starting against the same `secrets_dir` overwrite
/// each other's cipher.
fn save_cipher_new(path: &Path, key: &[u8; CIPHER_SIZE]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Atomic 0o600 — no window between create and chmod where the
        // file is readable under the process umask.
        opts.mode(0o600);
    }
    let mut file = opts.open(path)?;
    file.write_all(key)?;
    file.sync_all()?;
    Ok(())
}

impl ConfigArgs {
    pub(super) fn read_secrets(
        path_to_key: Option<PathBuf>,
        path_to_nonce: Option<PathBuf>,
        path_to_cipher: Option<PathBuf>,
    ) -> std::io::Result<Secrets> {
        let transport_keypair = if let Some(ref path_to_key) = path_to_key {
            read_transport_keypair(path_to_key)?
        } else {
            TransportKeypair::new()
        };
        // Nonces became per-write in PR #4143 — the file-based nonce is no
        // longer used for encryption. If the operator still has an explicit
        // `--nonce` path on their config, honor the file content for legacy
        // decrypt; otherwise use the historical default so pre-#4143 on-disk
        // files written under the old fallback path remain readable.
        let nonce = if let Some(ref path_to_nonce) = path_to_nonce {
            tracing::warn!(
                "`nonce` config is deprecated since per-write nonces landed; the file at \
                 {path:?} is used only as a legacy-decrypt nonce for pre-existing secrets.",
                path = path_to_nonce
            );
            read_nonce(path_to_nonce)?
        } else {
            LEGACY_DEFAULT_NONCE
        };
        // No `--cipher` path → caller is using the legacy "read_secrets"
        // entry that does not know about secrets_dir; preserve legacy
        // semantics by populating with the historical default so existing
        // ciphertexts remain decryptable. New nodes go through
        // `SecretArgs::build`, which auto-generates a fresh cipher.
        let cipher = if let Some(ref path_to_cipher) = path_to_cipher {
            read_cipher(path_to_cipher)?
        } else {
            LEGACY_DEFAULT_CIPHER
        };

        Ok(Secrets {
            transport_keypair,
            transport_keypair_path: path_to_key,
            nonce,
            nonce_path: path_to_nonce,
            cipher,
            cipher_path: path_to_cipher,
        })
    }
}

#[derive(Debug, Default, Clone, clap::Parser, serde::Serialize, serde::Deserialize)]
pub struct SecretArgs {
    /// Path to the X25519 keypair for the transport layer.
    #[clap(long, value_parser, default_value=None, env = "TRANSPORT_KEYPAIR")]
    pub transport_keypair: Option<PathBuf>,

    /// Path to the nonce file for encrypting data.
    #[clap(long, value_parser, default_value=None, env = "NONCE")]
    pub nonce: Option<PathBuf>,

    /// Path to the cipher file for encrypting data.
    #[clap(long, value_parser, default_value=None, env = "CIPHER")]
    pub cipher: Option<PathBuf>,
}

impl SecretArgs {
    pub(super) fn build(self, secrets_dir: Option<&Path>) -> std::io::Result<Secrets> {
        let (transport_keypair_path, transport_keypair) =
            if let Some(ref explicit_path) = self.transport_keypair {
                // Explicit --transport-keypair path provided: load from it
                let keypair = read_transport_keypair(explicit_path)?;
                (self.transport_keypair, keypair)
            } else if let Some(dir) = secrets_dir {
                let default_path = dir.join("transport_keypair");
                if default_path.exists() {
                    // Auto-load persisted keypair
                    tracing::info!(
                        path = %default_path.display(),
                        "Loading persisted transport keypair"
                    );
                    let keypair = read_transport_keypair(&default_path)?;
                    (Some(default_path), keypair)
                } else {
                    // Generate new keypair and persist it
                    std::fs::create_dir_all(dir)?;
                    let keypair = TransportKeypair::new();
                    keypair.save(&default_path)?;
                    tracing::info!(
                        path = %default_path.display(),
                        "Generated and saved new transport keypair"
                    );
                    (Some(default_path), keypair)
                }
            } else {
                // No secrets_dir (e.g. tests): ephemeral keypair
                (None, TransportKeypair::new())
            };
        let nonce = self.nonce.as_ref().map(read_nonce).transpose()?;
        let (nonce_path, nonce) = if let Some(nonce) = nonce {
            tracing::warn!(
                "`--nonce` / NONCE config is deprecated since per-write nonces landed; the \
                 supplied value is used only as a legacy-decrypt nonce for pre-existing secrets."
            );
            (self.nonce, nonce)
        } else {
            // Pre-#4143 on-disk files were written under the historical
            // default nonce when the operator did not supply one; keep
            // it as the legacy-decrypt fallback so upgrades stay
            // readable. New writes generate per-write random nonces.
            (None, LEGACY_DEFAULT_NONCE)
        };

        // Cipher: explicit `--cipher` path wins. Otherwise auto-generate
        // and persist under `secrets_dir/delegate_cipher`, mirroring the
        // existing `transport_keypair` auto-persist pattern. Without a
        // `secrets_dir` (in tests), fall back to an ephemeral random
        // cipher — explicitly NOT the historical default, since the
        // default was a world-known key.
        let explicit_cipher = self.cipher.as_ref().map(read_cipher).transpose()?;
        let (cipher_path, cipher) = if let Some(cipher) = explicit_cipher {
            (self.cipher, cipher)
        } else if let Some(dir) = secrets_dir {
            let default_path = dir.join(DELEGATE_CIPHER_FILENAME);
            if default_path.exists() {
                tracing::info!(
                    path = %default_path.display(),
                    "Loading persisted delegate cipher"
                );
                let cipher = read_cipher(&default_path)?;
                (Some(default_path), cipher)
            } else {
                std::fs::create_dir_all(dir)?;
                let cipher = generate_cipher_key();
                match save_cipher_new(&default_path, &cipher) {
                    Ok(()) => {
                        tracing::info!(
                            path = %default_path.display(),
                            "Generated and saved new delegate cipher"
                        );
                        (Some(default_path), cipher)
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                        // Race lost: another freenet process (or our own
                        // earlier startup attempt) wrote the cipher file
                        // in the window between `default_path.exists()`
                        // returning false and our `save_cipher_new`. The
                        // file is now authoritative — load it.
                        tracing::info!(
                            path = %default_path.display(),
                            "Cipher file appeared concurrently; loading the winning copy"
                        );
                        let cipher = read_cipher(&default_path)?;
                        (Some(default_path), cipher)
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            // No secrets_dir (tests): ephemeral random cipher. Crucially
            // NOT the historical world-known default — the whole point of
            // dropping `DelegateRequest::DEFAULT_CIPHER` was to ensure no
            // code path silently encrypts under a public constant.
            (None, generate_cipher_key())
        };

        Ok(Secrets {
            transport_keypair,
            transport_keypair_path,
            nonce,
            nonce_path,
            cipher,
            cipher_path,
        })
    }

    pub(super) fn merge(&mut self, other: Secrets) {
        if self.transport_keypair.is_none() {
            self.transport_keypair = other.transport_keypair_path;
        }

        if self.nonce.is_none() {
            self.nonce = other.nonce_path;
        }

        if self.cipher.is_none() {
            self.cipher = other.cipher_path;
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Secrets {
    #[serde(skip)]
    pub transport_keypair: TransportKeypair,
    #[serde(rename = "transport_keypair", skip_serializing_if = "Option::is_none")]
    pub transport_keypair_path: Option<PathBuf>,
    #[serde(skip)]
    pub nonce: [u8; 24],
    #[serde(rename = "nonce", skip_serializing_if = "Option::is_none")]
    pub nonce_path: Option<PathBuf>,
    #[serde(skip)]
    pub cipher: [u8; 32],
    #[serde(rename = "cipher", skip_serializing_if = "Option::is_none")]
    pub cipher_path: Option<PathBuf>,
}

// Only used in tests
#[cfg(test)]
impl Default for Secrets {
    fn default() -> Self {
        let transport_keypair = TransportKeypair::new();
        // Random cipher per test instance — explicitly NOT the historical
        // world-known constant. Production `SecretArgs::build` auto-
        // generates + persists; tests don't need persistence.
        let cipher = generate_cipher_key();
        // Pin the nonce to the legacy default so any test that hand-
        // crafts a legacy on-disk blob and then calls `SecretsStore::new`
        // with `Default::default()` exercises the same legacy-decrypt
        // path that production upgrades use.
        let nonce = LEGACY_DEFAULT_NONCE;

        Secrets {
            transport_keypair,
            transport_keypair_path: None,
            nonce,
            nonce_path: None,
            cipher,
            cipher_path: None,
        }
    }
}

impl Secrets {
    #[inline]
    pub fn nonce(&self) -> XNonce {
        self.nonce.into()
    }

    #[inline]
    pub fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new((&self.cipher).into())
    }

    #[inline]
    pub fn transport_keypair(&self) -> &TransportKeypair {
        &self.transport_keypair
    }
}

fn read_nonce(path_to_nonce: impl AsRef<Path>) -> std::io::Result<[u8; NONCE_SIZE]> {
    let path_to_nonce = path_to_nonce.as_ref();
    let mut nonce_file = File::open(path_to_nonce).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open key file {}: {e}", path_to_nonce.display()),
        )
    })?;
    let mut buf = [0u8; NONCE_SIZE];
    nonce_file.read_exact(&mut buf).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to read key file {}: {e}", path_to_nonce.display()),
        )
    })?;

    Ok::<_, std::io::Error>(buf)
}

fn read_cipher(path_to_cipher: impl AsRef<Path>) -> std::io::Result<[u8; CIPHER_SIZE]> {
    let path_to_cipher = path_to_cipher.as_ref();
    let mut cipher_file = File::open(path_to_cipher).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to open key file {}: {e}", path_to_cipher.display()),
        )
    })?;
    let mut buf = [0u8; CIPHER_SIZE];
    cipher_file.read_exact(&mut buf).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("Failed to read key file {}: {e}", path_to_cipher.display()),
        )
    })?;

    Ok::<_, std::io::Error>(buf)
}

fn read_transport_keypair(path_to_key: impl AsRef<Path>) -> std::io::Result<TransportKeypair> {
    let path = path_to_key.as_ref();
    match TransportKeypair::load(path) {
        Ok(keypair) => Ok(keypair),
        Err(e) => {
            // Check if this looks like an old RSA PEM key
            if let Ok(content) = std::fs::read_to_string(path) {
                if content.trim().starts_with("-----BEGIN") {
                    tracing::warn!(
                        path = %path.display(),
                        "Found RSA PEM key (legacy format). Generating new X25519 keypair. \
                         The old key file will be overwritten."
                    );
                    let keypair = TransportKeypair::new();
                    keypair.save(path)?;

                    // Also update the companion public key file if it exists.
                    // Derive the public key path from the private key filename:
                    //   "gw1_private_key.pem" → "gw1_public_key.pem"
                    //   "private_key.pem" → "public_key.pem"
                    if let Some(parent) = path.parent() {
                        let filename = path.file_name().and_then(|f| f.to_str()).unwrap_or("");
                        let public_filename = filename.replace("private", "public");
                        let public_key_path = parent.join(&public_filename);
                        if public_key_path.exists() {
                            if let Err(e) = keypair.public().save(&public_key_path) {
                                tracing::warn!(
                                    path = %public_key_path.display(),
                                    error = %e,
                                    "Failed to update public key file"
                                );
                            } else {
                                tracing::info!(
                                    path = %public_key_path.display(),
                                    "Updated public key file to X25519 format"
                                );
                            }
                        }
                    }

                    return Ok(keypair);
                }
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_different_files() {
        let transport_keypair = TransportKeypair::new();
        let nonce = [0u8; NONCE_SIZE];
        let cipher = [0u8; CIPHER_SIZE];

        let transport_keypair_file = tempfile::NamedTempFile::new().unwrap();
        let mut nonce_file = tempfile::NamedTempFile::new().unwrap();
        let mut cipher_file = tempfile::NamedTempFile::new().unwrap();

        // write secrets to files using hex format (new X25519 format)
        transport_keypair
            .save(transport_keypair_file.path())
            .unwrap();
        nonce_file.write_all(&nonce).unwrap();
        cipher_file.write_all(&cipher).unwrap();

        let secrets = Secrets {
            transport_keypair,
            transport_keypair_path: Some(transport_keypair_file.path().to_path_buf()),
            nonce,
            nonce_path: Some(nonce_file.path().to_path_buf()),
            cipher,
            cipher_path: Some(cipher_file.path().to_path_buf()),
        };

        let secret_args = SecretArgs {
            transport_keypair: Some(transport_keypair_file.path().to_path_buf()),
            nonce: Some(nonce_file.path().to_path_buf()),
            cipher: Some(cipher_file.path().to_path_buf()),
        };

        let loaded_secrets = secret_args.build(None).unwrap();
        assert_eq!(secrets, loaded_secrets);
    }

    /// `SecretArgs::default().build(None)` (no `--cipher`, no `secrets_dir`)
    /// MUST produce a random ephemeral cipher — NOT the historical
    /// `LEGACY_DEFAULT_CIPHER` constant. The whole point of removing
    /// `DelegateRequest::DEFAULT_CIPHER` from freenet-stdlib 0.8.0 was to
    /// guarantee that no code path silently encrypts under a public
    /// constant. This test pins that invariant.
    #[test]
    fn test_load_default_generates_random_cipher() {
        let secret_args = SecretArgs::default();
        let loaded_secrets = secret_args.build(None).unwrap();
        assert_ne!(
            LEGACY_DEFAULT_CIPHER, loaded_secrets.cipher,
            "build(None) must NOT seed the historical default cipher"
        );
        assert_ne!(
            [0u8; CIPHER_SIZE], loaded_secrets.cipher,
            "build(None) must produce a non-zero cipher"
        );
        // Two invocations must produce DIFFERENT ciphers — confirms the
        // RNG is actually being consulted per call.
        let another = SecretArgs::default().build(None).unwrap();
        assert_ne!(
            loaded_secrets.cipher, another.cipher,
            "two ephemeral builds must produce distinct random ciphers"
        );
        // Nonce stays at LEGACY_DEFAULT_NONCE (read-side fallback only).
        assert_eq!(LEGACY_DEFAULT_NONCE, loaded_secrets.nonce);
    }

    /// `SecretArgs::default().build(Some(dir))` auto-persists a cipher
    /// to `dir/delegate_cipher` on first call and reloads the same
    /// bytes on the second call. Mirrors the existing `transport_keypair`
    /// auto-persist behavior and is the upgrade path that replaces the
    /// removed `DEFAULT_CIPHER` fallback.
    #[test]
    fn test_cipher_auto_persist_and_reload() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let secrets_dir = tmp_dir.path();
        let args1 = SecretArgs::default();
        let secrets1 = args1.build(Some(secrets_dir)).unwrap();

        let cipher_path = secrets_dir.join(DELEGATE_CIPHER_FILENAME);
        assert!(cipher_path.exists(), "cipher file should be created");
        assert_eq!(
            secrets1.cipher_path.as_deref(),
            Some(cipher_path.as_path()),
            "cipher_path should point at the persisted file"
        );
        assert_ne!(
            LEGACY_DEFAULT_CIPHER, secrets1.cipher,
            "auto-generated cipher must NOT equal the historical default"
        );
        // File permissions on Unix must be 0o600 (owner read/write only).
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&cipher_path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "cipher file must be 0o600, got {mode:o}");
        }

        let args2 = SecretArgs::default();
        let secrets2 = args2.build(Some(secrets_dir)).unwrap();
        assert_eq!(
            secrets1.cipher, secrets2.cipher,
            "second build must reload the persisted cipher"
        );
    }

    /// Passing `--cipher /path/that/does/not/exist` MUST error rather
    /// than silently fall back to any default. The whole behavioral
    /// contract of removing `DEFAULT_CIPHER` is that there is no
    /// silent fallback to a known-bad key.
    #[test]
    fn test_missing_cipher_path_is_hard_error() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let missing = tmp_dir.path().join("does-not-exist");
        let args = SecretArgs {
            cipher: Some(missing),
            ..Default::default()
        };
        let err = args
            .build(None)
            .expect_err("missing cipher path must error");
        // Surface as a file-not-found IO error from read_cipher.
        assert_eq!(
            err.kind(),
            std::io::ErrorKind::NotFound,
            "expected NotFound, got {err:?}"
        );
    }

    #[test]
    fn test_keypair_auto_persist_and_reload() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let secrets_dir = tmp_dir.path();

        // First build: no keypair file exists, should generate and save
        let args1 = SecretArgs::default();
        let secrets1 = args1.build(Some(secrets_dir)).unwrap();

        let keypair_path = secrets_dir.join("transport_keypair");
        assert!(keypair_path.exists(), "keypair file should be created");
        assert_eq!(
            secrets1.transport_keypair_path.as_deref(),
            Some(keypair_path.as_path())
        );

        // Second build: file exists, should load the same keypair
        let args2 = SecretArgs::default();
        let secrets2 = args2.build(Some(secrets_dir)).unwrap();

        assert_eq!(
            secrets1.transport_keypair.public(),
            secrets2.transport_keypair.public(),
            "reloaded keypair should have the same public key"
        );
        assert_eq!(
            secrets2.transport_keypair_path.as_deref(),
            Some(keypair_path.as_path()),
            "reloaded keypair should preserve the path"
        );
    }

    #[test]
    fn test_explicit_keypair_overrides_secrets_dir() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let secrets_dir = tmp_dir.path();

        // Pre-populate the default location
        let args_seed = SecretArgs::default();
        let seeded = args_seed.build(Some(secrets_dir)).unwrap();

        // Create a different keypair at an explicit path
        let explicit_file = tempfile::NamedTempFile::new().unwrap();
        let different_keypair = TransportKeypair::new();
        different_keypair.save(explicit_file.path()).unwrap();

        // Build with explicit path — should use that, not the default
        let args = SecretArgs {
            transport_keypair: Some(explicit_file.path().to_path_buf()),
            ..Default::default()
        };
        let loaded = args.build(Some(secrets_dir)).unwrap();

        assert_eq!(
            loaded.transport_keypair.public(),
            different_keypair.public()
        );
        assert_ne!(
            loaded.transport_keypair.public(),
            seeded.transport_keypair.public(),
            "explicit path should override auto-persisted keypair"
        );
    }
}
