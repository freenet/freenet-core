//! Portable, encrypted-at-rest export/import of delegate secrets (P3 of
//! #4381, closes #4035).
//!
//! This module produces a single self-describing bundle file from the secrets
//! a node holds for a given [`SecretScope`], and re-places those secrets on
//! another node. The primary use case is the hosted → self-host migration: a
//! user who tried Freenet through a hosted gateway downloads their per-user
//! delegate secrets and re-imports them into their own single-user peer. It
//! also serves the normal-node backup/restore case via the [`SecretScope::Local`]
//! path (#4035's original framing).
//!
//! # Bundle format
//!
//! The on-disk bundle is a flat byte string:
//!
//! ```text
//! [MAGIC "FNSX" (4B)]
//! [bundle_format_version u8]   // BUNDLE_FORMAT_V1
//! [kdf_id u8]                  // KdfId: 1 = Argon2id(passphrase), 2 = token-HKDF
//! [salt (16B)]                 // random; Argon2id salt (also stored for token-HKDF for uniformity)
//! [nonce (24B)]                // random XChaCha20-Poly1305 nonce
//! [AEAD ciphertext + 16B tag]  // XChaCha20-Poly1305 over the CBOR payload
//! ```
//!
//! The header (everything before the AEAD ciphertext) is bound as additional
//! authenticated data (AAD), so a tampered version / kdf-id / salt / nonce
//! causes a clean authentication failure rather than a confusing downstream
//! parse error.
//!
//! The AEAD plaintext is the CBOR serialization of [`BundlePayload`]: a
//! versioned list of `{delegate_key, code_hash, secret_hash, plaintext}`
//! entries plus light metadata. CBOR is used (over bincode) because the bundle
//! is a durable, cross-version artifact: a self-describing, schema-evolvable
//! encoding is the right call for something users keep on a USB stick for
//! months.
//!
//! # Key derivation
//!
//! - `--passphrase`: 32-byte key = Argon2id(passphrase, salt) with the default
//!   OWASP-ish parameters from the `argon2` crate. The random per-bundle salt
//!   means the same passphrase produces a different key (and different bytes)
//!   every export.
//! - `--use-token-key`: 32-byte key = HKDF-SHA256 over the user token with a
//!   distinct domain string. This lets a hosted user who only has their opaque
//!   token (not a separately-chosen passphrase) still encrypt/decrypt the
//!   bundle. The token is the same secret that derives their per-user DEK, so
//!   this adds no new trust assumption.
//!
//! # Security
//!
//! - The bundle at rest is ALWAYS encrypted; this module never writes a
//!   plaintext secret to disk. Plaintext lives only in `Zeroizing` buffers in
//!   memory during the operation.
//! - **Operator-sees-plaintext-during-export is inherent.** Building the bundle
//!   requires the node to decrypt each secret (it can, by construction — it
//!   holds the scope DEK). The hosted operator therefore observes plaintext in
//!   process memory for the duration of an export. This is the same disclosure
//!   the hosted model already has (the node runs the delegate), and is
//!   documented in the CLI help. A user who wants zero operator exposure should
//!   self-host from the start.

use std::io::Write as _;

use argon2::Argon2;
use chacha20poly1305::{
    AeadCore, XChaCha20Poly1305, XNonce,
    aead::{Aead, OsRng, Payload, rand_core::RngCore},
};
use freenet_stdlib::prelude::{CodeHash, DelegateKey};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use zeroize::{Zeroize, Zeroizing};

use super::secrets_store::{ExportSecretEntry, SecretScope, SecretsStore, UserSecretContext};

/// File magic. Lets `import` reject an obviously-wrong file (a truncated
/// download, a different format) before attempting any crypto.
const MAGIC: &[u8; 4] = b"FNSX";

/// Bundle wire-format version. Bump on any change to the header layout or the
/// CBOR schema that isn't backward-compatible.
const BUNDLE_FORMAT_V1: u8 = 1;

/// Length of the random salt stored in the header.
const SALT_LEN: usize = 16;

/// Header length: MAGIC(4) + version(1) + kdf_id(1) + salt(16) + nonce(24).
const HEADER_LEN: usize = 4 + 1 + 1 + SALT_LEN + 24;

/// HKDF domain string for the token-derived bundle key. Distinct from the
/// per-user DEK domain (`freenet-user-dek` in `secrets_store`) so the bundle
/// key and the storage DEK are independent even though both derive from the
/// same token.
const TOKEN_BUNDLE_HKDF_INFO: &[u8] = b"freenet-secret-bundle-token-v1";

/// Upper bound on the number of secrets a single [`export_bundle`] call will
/// gather, checked BEFORE any secret is read/decrypted.
///
/// This is a defense-in-depth cap on the worst-case work of one export — even
/// though the hosted path now runs the export off the contract loop (see
/// `RuntimePool::export_user_secrets`), an unbounded enumerate+decrypt is still
/// CPU/IO the node shouldn't be coerced into doing without limit by an
/// authenticated token-holder (the hosted-export DoS surface of #4381 P5).
///
/// 10,000 is deliberately far above any legitimate per-user delegate-secret
/// set: a hosted user accumulates a handful of secrets per delegate they
/// interact with, so a real export is tens to low-hundreds of entries. A user
/// who genuinely exceeds this is not a normal hosted user, and the request is
/// rejected (HTTP 413) rather than silently truncated, so no data is lost — the
/// limit can be raised by an operator follow-up if a real workload ever needs
/// it.
pub const MAX_EXPORT_SECRET_COUNT: usize = 10_000;

/// Upper bound on the cumulative DECRYPTED plaintext bytes a single
/// [`export_bundle`] call will gather, checked incrementally as secrets are
/// read so the loop bails early instead of buffering an unbounded amount of
/// plaintext in memory.
///
/// 256 MiB is far above any legitimate per-user secret payload (delegate
/// secrets are keys/tokens/small blobs, kilobytes each), but bounds the
/// in-memory `Zeroizing` plaintext buffers and the resulting bundle so a
/// single export cannot be used to exhaust node memory. Like the count cap,
/// exceeding it is a hard 413, never a silent truncation.
pub const MAX_EXPORT_TOTAL_PLAINTEXT_BYTES: usize = 256 * 1024 * 1024;

/// Which key-derivation function produced the bundle key. Recorded in the
/// header so `import` derives the matching key without the user having to
/// re-specify the method.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum KdfId {
    /// Argon2id over a user passphrase + the header salt.
    Argon2idPassphrase = 1,
    /// HKDF-SHA256 over the opaque user token.
    TokenHkdf = 2,
}

impl KdfId {
    fn from_byte(b: u8) -> Result<Self, ExportError> {
        match b {
            1 => Ok(KdfId::Argon2idPassphrase),
            2 => Ok(KdfId::TokenHkdf),
            other => Err(ExportError::UnknownKdf(other)),
        }
    }
}

/// The secret material a bundle key can be derived from. The caller picks one;
/// `import` must present the SAME one the bundle was created with (the header
/// records which KDF, but the secret itself is never stored).
pub enum BundleKeyMaterial<'a> {
    /// A user-chosen passphrase (Argon2id-stretched).
    Passphrase(&'a [u8]),
    /// The opaque per-user token (HKDF-derived). Same secret used for the
    /// per-user storage DEK.
    Token(&'a [u8]),
}

impl BundleKeyMaterial<'_> {
    fn kdf_id(&self) -> KdfId {
        match self {
            BundleKeyMaterial::Passphrase(_) => KdfId::Argon2idPassphrase,
            BundleKeyMaterial::Token(_) => KdfId::TokenHkdf,
        }
    }
}

/// One secret in the bundle. `delegate_key` + `code_hash` reconstruct the
/// [`DelegateKey`] on import; `secret_hash` is the on-disk `bs58`-encoded name.
///
/// The byte vectors serialize as CBOR arrays (not byte strings) — a minor size
/// overhead avoided by `serde_bytes`, which we deliberately do NOT add as a
/// dependency: a backup artifact's correctness and self-description matter far
/// more than a few bytes of CBOR framing.
///
/// `plaintext` is the only secret field. A manual [`Drop`] wipes it via
/// [`Zeroize::zeroize`] so the in-memory copies this struct holds — both the
/// payload `seal_bundle` builds on export and the payload `open_bundle`
/// deserializes on import — are scrubbed when dropped, matching the module's
/// "plaintext lives only in zeroizing buffers" guarantee. (`Zeroizing<Vec<u8>>`
/// would be cleaner but does not implement serde's `Serialize`/`Deserialize`,
/// so we keep a plain `Vec<u8>` for the wire and wipe it on drop instead.)
#[derive(Serialize, Deserialize)]
struct BundleEntry {
    delegate_key: Vec<u8>,
    code_hash: Vec<u8>,
    secret_hash: Vec<u8>,
    plaintext: Vec<u8>,
}

impl Drop for BundleEntry {
    fn drop(&mut self) {
        // Only the plaintext is secret; the key/hash fields are public
        // identifiers, so wiping `plaintext` alone is sufficient. Dropping a
        // `Vec<BundleEntry>` drops each entry, so wiping here covers the whole
        // `BundlePayload.entries` vector without a separate `Drop` on the payload.
        self.plaintext.zeroize();
    }
}

/// The CBOR-serialized, then encrypted, body of a bundle.
#[derive(Serialize, Deserialize)]
struct BundlePayload {
    /// Schema version of the CBOR payload (independent of the on-disk header
    /// version). Bump when the entry shape changes.
    schema_version: u32,
    /// Human-readable note on what scope produced the bundle ("local" or
    /// "user"). Informational; import does not depend on it.
    source_scope: String,
    /// Unix seconds at export time. Informational.
    created_unix_secs: u64,
    entries: Vec<BundleEntry>,
}

const PAYLOAD_SCHEMA_V1: u32 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    /// The export would gather more than [`MAX_EXPORT_SECRET_COUNT`] secrets or
    /// more than [`MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`] of decrypted plaintext.
    /// Distinct from the other variants so the hosted-export HTTP layer can map
    /// it to a 413 (Payload Too Large) rather than a generic 500. The `limit`
    /// and `actual` are non-secret sizes (counts/bytes), safe to surface.
    #[error("export too large: {what} {actual} exceeds limit {limit}")]
    TooLarge {
        what: &'static str,
        actual: usize,
        limit: usize,
    },
    #[error("secrets store error: {0}")]
    Store(#[from] super::secrets_store::SecretStoreError),
    #[error("runtime error: {0}")]
    Runtime(String),
    #[error("CBOR serialization error: {0}")]
    CborSer(String),
    #[error("CBOR deserialization error: {0}")]
    CborDe(String),
    #[error("argon2 key derivation failed: {0}")]
    Argon2(String),
    /// The AEAD `encrypt` call failed while SEALING a bundle. Distinct from
    /// [`Self::AuthFailed`] (a DECRYPT/authentication failure) so an export-side
    /// error isn't misreported as "wrong passphrase". In practice this only
    /// fires on absurd input sizes; it is not reachable for normal bundles.
    #[error("bundle encryption failed")]
    EncryptFailed,
    #[error("bundle authentication failed: wrong passphrase/token or corrupt bundle")]
    AuthFailed,
    #[error("not a freenet secrets bundle (bad magic)")]
    BadMagic,
    #[error("unsupported bundle format version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown KDF id {0} in bundle header")]
    UnknownKdf(u8),
    #[error("bundle truncated: {0}")]
    Truncated(&'static str),
    #[error("bundle entry has malformed {field} length {len} (expected {expected})")]
    BadEntryField {
        field: &'static str,
        len: usize,
        expected: usize,
    },
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Outcome of an import: per-entry placed/skipped accounting.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImportReport {
    /// Secrets newly written (or overwritten, when `overwrite` was set).
    pub imported: usize,
    /// Secrets skipped because a value already existed and `overwrite` was
    /// false. Each is `(delegate_bs58, secret_bs58)`.
    pub skipped: Vec<(String, String)>,
}

/// Derive the 32-byte bundle key from the chosen secret material + salt,
/// returning it in a `Zeroizing` buffer.
fn derive_bundle_key(
    material: &BundleKeyMaterial<'_>,
    salt: &[u8; SALT_LEN],
) -> Result<Zeroizing<[u8; 32]>, ExportError> {
    let mut key = Zeroizing::new([0u8; 32]);
    match material {
        BundleKeyMaterial::Passphrase(pass) => {
            // Default Argon2id params from the crate (m=19456 KiB, t=2, p=1 as
            // of argon2 0.5 — OWASP's recommended floor). `hash_password_into`
            // is the low-level "give me raw bytes" API; we are not producing a
            // PHC string.
            Argon2::default()
                .hash_password_into(pass, salt, key.as_mut_slice())
                .map_err(|e| ExportError::Argon2(e.to_string()))?;
        }
        BundleKeyMaterial::Token(token) => {
            // Token path: HKDF-SHA256 with the salt as HKDF salt and a distinct
            // domain string as info. Argon2 stretching is unnecessary here —
            // the token is already a high-entropy 32-byte-equivalent secret,
            // not a low-entropy human passphrase.
            let hk = Hkdf::<Sha256>::new(Some(salt.as_slice()), token);
            hk.expand(TOKEN_BUNDLE_HKDF_INFO, key.as_mut_slice())
                .expect("HKDF expand with 32-byte OKM never fails for SHA-256");
        }
    }
    Ok(key)
}

/// Serialize + encrypt `entries` into a complete bundle byte string.
///
/// Pure function over already-decrypted entries — this is the unit-testable
/// core, independent of any `SecretsStore`.
fn seal_bundle(
    entries: &[ExportSecretEntry],
    source_scope: &str,
    material: &BundleKeyMaterial<'_>,
) -> Result<Vec<u8>, ExportError> {
    let payload = BundlePayload {
        schema_version: PAYLOAD_SCHEMA_V1,
        source_scope: source_scope.to_string(),
        created_unix_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        entries: entries
            .iter()
            .map(|e| BundleEntry {
                delegate_key: e.delegate_key.bytes().to_vec(),
                code_hash: e.delegate_key.code_hash().as_ref().to_vec(),
                secret_hash: e.secret_hash.to_vec(),
                plaintext: e.plaintext.to_vec(),
            })
            .collect(),
    };

    // CBOR-serialize into a Zeroizing buffer (it holds plaintext secrets).
    let mut plaintext = Zeroizing::new(Vec::new());
    ciborium::ser::into_writer(&payload, &mut *plaintext)
        .map_err(|e| ExportError::CborSer(e.to_string()))?;

    // Random salt + nonce per bundle.
    // OsRng (not GlobalRng) is the documented exception for cryptographic
    // material; see `.claude/rules/code-style.md`.
    let mut salt = [0u8; SALT_LEN];
    OsRng.fill_bytes(&mut salt);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);

    let kdf_id = material.kdf_id();
    let key = derive_bundle_key(material, &salt)?;
    let cipher = <XChaCha20Poly1305 as chacha20poly1305::KeyInit>::new(key.as_slice().into());

    // Assemble the header first; it is bound as AAD so the version / kdf / salt
    // / nonce can't be swapped without tripping authentication.
    let mut out = Vec::with_capacity(HEADER_LEN + plaintext.len() + 16);
    out.extend_from_slice(MAGIC);
    out.push(BUNDLE_FORMAT_V1);
    out.push(kdf_id as u8);
    out.extend_from_slice(&salt);
    out.extend_from_slice(nonce.as_slice());
    debug_assert_eq!(out.len(), HEADER_LEN);

    let aad = out.clone(); // the header bytes written so far
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext.as_slice(),
                aad: &aad,
            },
        )
        .map_err(|_| {
            // Encryption only fails on absurd input sizes (not reachable for a
            // real bundle). Report it as an export-side encrypt failure, NOT
            // AuthFailed — the latter reads as "wrong passphrase" on decrypt.
            ExportError::EncryptFailed
        })?;
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Decrypt + parse a bundle byte string back into its payload. Pure function —
/// the unit-testable inverse of [`seal_bundle`].
fn open_bundle(
    bundle: &[u8],
    material: &BundleKeyMaterial<'_>,
) -> Result<BundlePayload, ExportError> {
    if bundle.len() < HEADER_LEN {
        return Err(ExportError::Truncated("shorter than header"));
    }
    if &bundle[0..4] != MAGIC {
        return Err(ExportError::BadMagic);
    }
    let version = bundle[4];
    if version != BUNDLE_FORMAT_V1 {
        return Err(ExportError::UnsupportedVersion(version));
    }
    let kdf_id = KdfId::from_byte(bundle[5])?;
    // The header records which KDF was used; the caller must present matching
    // material. If they don't, key derivation still runs but AEAD auth fails —
    // a clean `AuthFailed`, not a panic.
    if kdf_id != material.kdf_id() {
        return Err(ExportError::AuthFailed);
    }
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&bundle[6..6 + SALT_LEN]);
    let nonce_start = 6 + SALT_LEN;
    let nonce = XNonce::from_slice(&bundle[nonce_start..nonce_start + 24]);
    let aad = &bundle[..HEADER_LEN];
    let ciphertext = &bundle[HEADER_LEN..];

    let key = derive_bundle_key(material, &salt)?;
    let cipher = <XChaCha20Poly1305 as chacha20poly1305::KeyInit>::new(key.as_slice().into());
    let plaintext = cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| ExportError::AuthFailed)?;

    let payload: BundlePayload = ciborium::de::from_reader(plaintext.as_slice())
        .map_err(|e| ExportError::CborDe(e.to_string()))?;
    Ok(payload)
}

/// Export every secret under `scope` from `store` into an encrypted bundle.
///
/// `scope` selects what to gather:
/// - [`SecretScope::Local`] — all single-user (normal-node) secrets.
/// - [`SecretScope::User`] — all per-user secrets for one hosted user. Build
///   the scope from the user's token via [`UserSecretContext::from_token`] and
///   pass `ctx.scope()`.
///
/// The bundle is encrypted under `material`. Returns the bundle bytes; the
/// caller writes them to a file (or stdout). Plaintext secrets exist only in
/// the in-memory `Zeroizing` buffers gathered here.
pub fn export_bundle(
    store: &SecretsStore,
    scope: SecretScope<'_>,
    material: &BundleKeyMaterial<'_>,
) -> Result<Vec<u8>, ExportError> {
    export_bundle_with_limits(
        store,
        scope,
        material,
        MAX_EXPORT_SECRET_COUNT,
        MAX_EXPORT_TOTAL_PLAINTEXT_BYTES,
    )
}

/// [`export_bundle`] with the per-user bound made explicit, so tests can
/// exercise the cap at small limits deterministically (the public entry point
/// passes the production [`MAX_EXPORT_SECRET_COUNT`] /
/// [`MAX_EXPORT_TOTAL_PLAINTEXT_BYTES`]).
fn export_bundle_with_limits(
    store: &SecretsStore,
    scope: SecretScope<'_>,
    material: &BundleKeyMaterial<'_>,
    max_count: usize,
    max_total_bytes: usize,
) -> Result<Vec<u8>, ExportError> {
    let scope_label = match &scope {
        SecretScope::Local => "local",
        SecretScope::User { .. } => "user",
    };

    // Defense-in-depth bound (#4381 P5): reject an over-large export so an
    // authenticated token-holder cannot coerce the node into unbounded crypto/IO
    // or memory. BOTH checks happen BEFORE/DURING the heavy work, never after:
    //   - The count check STREAMS the on-disk walk and stops the instant it has
    //     seen max_count + 1 entries (no full traversal, no allocation of the
    //     attacker-controlled entry list) — see `scope_count_exceeds`.
    //   - The byte check is enforced INCREMENTALLY inside
    //     `export_scope_entries_bounded`, which aborts the decrypt loop the
    //     instant the running plaintext total crosses the cap and drops the
    //     partial buffer — so a user under the count cap but with huge secrets
    //     never gets multi-GiB decrypted+buffered before the rejection.
    // Both are hard rejections (never silent truncation), so no user data is
    // lost — the caller surfaces a 413 and an operator can raise the limit if a
    // real workload ever needs it. A disk I/O failure during the count
    // propagates (fail-loud) rather than passing the cap on a partial walk.
    if store.scope_count_exceeds(&scope, max_count)? {
        return Err(ExportError::TooLarge {
            what: "secret count",
            // The streaming count stops at max_count + 1, so we report that as a
            // lower bound on the true (over-cap) count rather than a full count.
            actual: max_count + 1,
            limit: max_count,
        });
    }

    let entries = store
        .export_scope_entries_bounded(scope, max_total_bytes)
        .map_err(|e| match e {
            super::secrets_store::ExportScopeError::Store(store_err) => {
                ExportError::Store(store_err)
            }
            super::secrets_store::ExportScopeError::TooLarge { actual, limit } => {
                ExportError::TooLarge {
                    what: "total plaintext bytes",
                    actual,
                    limit,
                }
            }
        })?;

    seal_bundle(&entries, scope_label, material)
}

/// Import every secret from `bundle` into `store` at the chosen `target_scope`.
///
/// The bundle is decrypted with `material` (which must match what it was
/// created with). Each entry is placed at its original `(DelegateKey,
/// secret_hash)`. `overwrite` controls collision handling: when false, an entry
/// whose secret already exists is left untouched and recorded in
/// [`ImportReport::skipped`]; when true, it is overwritten (the prior value is
/// snapshotted first).
///
/// Atomicity is limited to the DECRYPT phase: [`open_bundle`] is all-or-nothing,
/// so a wrong passphrase/token or a corrupt bundle fails before ANY write. The
/// per-entry WRITE loop is NOT transactional — if entry K fails to write,
/// entries `0..K` are already committed. Re-running the same import is safe:
/// entries are idempotent by their `(delegate, secret_hash)` key (an
/// already-present secret is skipped, or with `--overwrite` re-written to the
/// same value), so a retry converges without duplicating or corrupting state.
pub fn import_bundle(
    store: &mut SecretsStore,
    bundle: &[u8],
    material: &BundleKeyMaterial<'_>,
    target_scope: &TargetScope,
    overwrite: bool,
) -> Result<ImportReport, ExportError> {
    let payload = open_bundle(bundle, material)?;

    let mut report = ImportReport::default();
    for entry in &payload.entries {
        // Reconstruct the typed DelegateKey from the two 32-byte halves.
        let delegate_bytes: [u8; 32] =
            entry
                .delegate_key
                .as_slice()
                .try_into()
                .map_err(|_| ExportError::BadEntryField {
                    field: "delegate_key",
                    len: entry.delegate_key.len(),
                    expected: 32,
                })?;
        let code_hash_bytes: [u8; 32] =
            entry
                .code_hash
                .as_slice()
                .try_into()
                .map_err(|_| ExportError::BadEntryField {
                    field: "code_hash",
                    len: entry.code_hash.len(),
                    expected: 32,
                })?;
        let secret_hash: [u8; 32] =
            entry
                .secret_hash
                .as_slice()
                .try_into()
                .map_err(|_| ExportError::BadEntryField {
                    field: "secret_hash",
                    len: entry.secret_hash.len(),
                    expected: 32,
                })?;
        let delegate = DelegateKey::new(delegate_bytes, CodeHash::from(&code_hash_bytes));

        let plaintext = Zeroizing::new(entry.plaintext.clone());
        let scope = target_scope.as_scope();
        let wrote = store
            .import_secret_by_hash(&delegate, &secret_hash, scope, plaintext, overwrite)
            .map_err(|e| ExportError::Runtime(e.to_string()))?;
        if wrote {
            report.imported += 1;
        } else {
            report.skipped.push((
                delegate.encode(),
                bs58::encode(secret_hash)
                    .with_alphabet(bs58::Alphabet::BITCOIN)
                    .into_string(),
            ));
        }
    }
    Ok(report)
}

/// Owned target scope for an import. [`SecretScope`] borrows its user context,
/// so this owns a [`UserSecretContext`] when targeting a user namespace and
/// lends out a borrowed [`SecretScope`] per entry via [`Self::as_scope`].
pub enum TargetScope {
    /// Place imported secrets at the single-user / node-local scope. The
    /// primary self-host target.
    Local,
    /// Place imported secrets under a per-user namespace (for round-trip
    /// testing or re-hosting). Built from the user's token.
    User(UserSecretContext),
}

impl TargetScope {
    /// Construct a [`TargetScope::User`] from an opaque user token.
    pub fn user_from_token(token: &[u8]) -> Self {
        TargetScope::User(UserSecretContext::from_token(token))
    }

    fn as_scope(&self) -> SecretScope<'_> {
        match self {
            TargetScope::Local => SecretScope::Local,
            TargetScope::User(ctx) => ctx.scope(),
        }
    }
}

/// Write `bundle` to `path` at mode 0o600 (Unix), refusing to clobber an
/// existing file. The bundle is encrypted, but we still keep it owner-only and
/// avoid silently overwriting a prior backup.
pub fn write_bundle_file(path: &std::path::Path, bundle: &[u8]) -> Result<(), ExportError> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(path)?;
    f.write_all(bundle)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::secrets_store::UserSecretContext;
    use freenet_stdlib::prelude::{Delegate, SecretsId};

    async fn new_store(dir: &std::path::Path) -> SecretsStore {
        let secrets_dir = dir.join("secrets");
        std::fs::create_dir_all(&secrets_dir).unwrap();
        let db = Storage::new(dir).await.expect("db");
        SecretsStore::new(secrets_dir, Default::default(), db).expect("store")
    }

    /// Build a `SecretsStore` over a SPECIFIC, SHARED `secrets_dir` but with its
    /// OWN ReDb (in `db_dir`). Two such stores model two pooled executors: they
    /// write to the same on-disk `secrets_dir` but each keeps its own in-memory
    /// index — exactly the cross-executor divergence the disk-enumeration export
    /// fix must tolerate.
    async fn new_store_sharing(
        secrets_dir: &std::path::Path,
        db_dir: &std::path::Path,
    ) -> SecretsStore {
        std::fs::create_dir_all(secrets_dir).unwrap();
        std::fs::create_dir_all(db_dir).unwrap();
        let db = Storage::new(db_dir).await.expect("db");
        SecretsStore::new(secrets_dir.to_path_buf(), Default::default(), db).expect("store")
    }

    fn delegate(code: u8) -> Delegate<'static> {
        Delegate::from((&vec![code].into(), &vec![].into()))
    }

    /// Store `plaintext` for `secret_id` at the given user scope and return the
    /// 32-byte secret hash (so the test can assert by hash after a round-trip).
    fn put_user(
        store: &mut SecretsStore,
        d: &Delegate<'static>,
        ctx: &UserSecretContext,
        secret_id: &SecretsId,
        plaintext: &[u8],
    ) -> [u8; 32] {
        store
            .store_secret(
                d.key(),
                secret_id,
                ctx.scope(),
                Zeroizing::new(plaintext.to_vec()),
            )
            .expect("store user secret");
        *secret_id.hash()
    }

    #[tokio::test]
    async fn user_scope_passphrase_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;

        let token = b"user-token-abc";
        let ctx = UserSecretContext::from_token(token);

        // Two delegates, multiple secrets, for the same user.
        let d1 = delegate(1);
        let d2 = delegate(2);
        let s1 = SecretsId::new(b"alpha".to_vec());
        let s2 = SecretsId::new(b"beta".to_vec());
        let s3 = SecretsId::new(b"gamma".to_vec());
        let h1 = put_user(&mut store, &d1, &ctx, &s1, b"plain-1");
        let h2 = put_user(&mut store, &d1, &ctx, &s2, b"plain-2");
        let h3 = put_user(&mut store, &d2, &ctx, &s3, b"plain-3");

        // A DIFFERENT user's secret must NOT appear in the first user's export.
        let other_ctx = UserSecretContext::from_token(b"other-user");
        put_user(&mut store, &d1, &other_ctx, &s1, b"other-plain");

        let pass = BundleKeyMaterial::Passphrase(b"correct horse battery staple");
        let bundle = export_bundle(&store, ctx.scope(), &pass).expect("export");

        // Bundle is encrypted at rest: no known plaintext appears verbatim.
        assert!(!contains(&bundle, b"plain-1"));
        assert!(!contains(&bundle, b"plain-2"));
        assert!(!contains(&bundle, b"plain-3"));

        // Import into a FRESH node at Local scope.
        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let report =
            import_bundle(&mut fresh, &bundle, &pass, &TargetScope::Local, false).expect("import");
        assert_eq!(report.imported, 3, "all three of this user's secrets land");
        assert!(report.skipped.is_empty());

        // Every secret decrypts to its original plaintext at Local scope.
        assert_eq!(read_local(&fresh, &d1, &h1), b"plain-1");
        assert_eq!(read_local(&fresh, &d1, &h2), b"plain-2");
        assert_eq!(read_local(&fresh, &d2, &h3), b"plain-3");
    }

    #[tokio::test]
    async fn token_keyed_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"opaque-bearer-token";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(7);
        let s = SecretsId::new(b"k".to_vec());
        let h = put_user(&mut store, &d, &ctx, &s, b"secret-value");

        let material = BundleKeyMaterial::Token(token);
        let bundle = export_bundle(&store, ctx.scope(), &material).expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let report = import_bundle(&mut fresh, &bundle, &material, &TargetScope::Local, false)
            .expect("import");
        assert_eq!(report.imported, 1);
        assert_eq!(read_local(&fresh, &d, &h), b"secret-value");
    }

    #[tokio::test]
    async fn local_scope_round_trip() {
        // Normal-node migration: Local export → Local import.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let d = delegate(3);
        let s = SecretsId::new(b"local-secret".to_vec());
        store
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"loc".to_vec()),
            )
            .unwrap();
        let h = *s.hash();

        let pass = BundleKeyMaterial::Passphrase(b"pw");
        let bundle = export_bundle(&store, SecretScope::Local, &pass).expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let report =
            import_bundle(&mut fresh, &bundle, &pass, &TargetScope::Local, false).expect("import");
        assert_eq!(report.imported, 1);
        assert_eq!(read_local(&fresh, &d, &h), b"loc");
    }

    #[tokio::test]
    async fn wrong_passphrase_fails_clean_no_write() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let d = delegate(4);
        let s = SecretsId::new(b"x".to_vec());
        store
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )
            .unwrap();

        let bundle = export_bundle(
            &store,
            SecretScope::Local,
            &BundleKeyMaterial::Passphrase(b"right"),
        )
        .expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let err = import_bundle(
            &mut fresh,
            &bundle,
            &BundleKeyMaterial::Passphrase(b"wrong"),
            &TargetScope::Local,
            false,
        )
        .expect_err("wrong passphrase must fail");
        assert!(matches!(err, ExportError::AuthFailed), "got {err:?}");

        // No secret was written to the fresh store.
        assert!(
            fresh
                .export_scope_entries(SecretScope::Local)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn wrong_token_fails_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let ctx = UserSecretContext::from_token(b"tok-a");
        let d = delegate(5);
        let s = SecretsId::new(b"y".to_vec());
        put_user(&mut store, &d, &ctx, &s, b"v");
        let bundle = export_bundle(&store, ctx.scope(), &BundleKeyMaterial::Token(b"tok-a"))
            .expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let err = import_bundle(
            &mut fresh,
            &bundle,
            &BundleKeyMaterial::Token(b"tok-b"),
            &TargetScope::Local,
            false,
        )
        .expect_err("wrong token must fail");
        assert!(matches!(err, ExportError::AuthFailed), "got {err:?}");
    }

    #[tokio::test]
    async fn passphrase_bundle_rejects_token_material() {
        // Cross-method mismatch: a passphrase bundle opened with token
        // material must fail clean (the header kdf_id won't match).
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let d = delegate(6);
        let s = SecretsId::new(b"z".to_vec());
        store
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"v".to_vec()),
            )
            .unwrap();
        let bundle = export_bundle(
            &store,
            SecretScope::Local,
            &BundleKeyMaterial::Passphrase(b"pw"),
        )
        .expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let err = import_bundle(
            &mut fresh,
            &bundle,
            &BundleKeyMaterial::Token(b"pw"),
            &TargetScope::Local,
            false,
        )
        .expect_err("token material on a passphrase bundle must fail");
        assert!(matches!(err, ExportError::AuthFailed), "got {err:?}");
    }

    #[tokio::test]
    async fn collision_without_overwrite_skips_and_reports() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let d = delegate(8);
        let s = SecretsId::new(b"dup".to_vec());
        store
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"orig".to_vec()),
            )
            .unwrap();
        let h = *s.hash();
        let pass = BundleKeyMaterial::Passphrase(b"pw");
        let bundle = export_bundle(&store, SecretScope::Local, &pass).expect("export");

        // Import into a store that ALREADY has a different value for the same
        // (delegate, secret_hash). Without --overwrite it must skip + report.
        let tmp2 = tempfile::tempdir().unwrap();
        let mut target = new_store(tmp2.path()).await;
        target
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"existing".to_vec()),
            )
            .unwrap();

        let report =
            import_bundle(&mut target, &bundle, &pass, &TargetScope::Local, false).expect("import");
        assert_eq!(report.imported, 0);
        assert_eq!(report.skipped.len(), 1);
        // Existing value untouched.
        assert_eq!(read_local(&target, &d, &h), b"existing");

        // With overwrite the bundle value replaces it.
        let report =
            import_bundle(&mut target, &bundle, &pass, &TargetScope::Local, true).expect("import");
        assert_eq!(report.imported, 1);
        assert!(report.skipped.is_empty());
        assert_eq!(read_local(&target, &d, &h), b"orig");
    }

    #[tokio::test]
    async fn user_scope_import_round_trip() {
        // Completeness: import back into a User scope and read it there.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"round-trip-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(9);
        let s = SecretsId::new(b"u".to_vec());
        let h = put_user(&mut store, &d, &ctx, &s, b"uservalue");
        let material = BundleKeyMaterial::Token(token);
        let bundle = export_bundle(&store, ctx.scope(), &material).expect("export");

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let target = TargetScope::user_from_token(token);
        let report = import_bundle(&mut fresh, &bundle, &material, &target, false).expect("import");
        assert_eq!(report.imported, 1);
        // Readable at the User scope on the fresh node.
        let read = fresh
            .get_secret(d.key(), &s, ctx.scope())
            .expect("read user secret");
        assert_eq!(read.to_vec(), b"uservalue");
        // And by-hash enumeration sees exactly one entry for this user.
        let entries = fresh.export_scope_entries(ctx.scope()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].secret_hash, h);
    }

    #[tokio::test]
    async fn bad_magic_and_truncation_rejected() {
        // `BundlePayload` deliberately has no `Debug` impl (it holds
        // plaintext), so we can't use `expect_err`; match on the result.
        let pass = BundleKeyMaterial::Passphrase(b"pw");
        match open_bundle(
            b"not-a-bundle-but-long-enough-header-xxxxxxxxxxxxxxxx",
            &pass,
        ) {
            Err(ExportError::BadMagic) => {}
            other => panic!("expected BadMagic, got {:?}", other.err()),
        }
        match open_bundle(b"short", &pass) {
            Err(ExportError::Truncated(_)) => {}
            other => panic!("expected Truncated, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn empty_scope_exports_empty_bundle() {
        // Exporting a scope with no secrets yields a valid, decryptable,
        // zero-entry bundle (not an error).
        let tmp = tempfile::tempdir().unwrap();
        let store = new_store(tmp.path()).await;
        let pass = BundleKeyMaterial::Passphrase(b"pw");
        let bundle = export_bundle(&store, SecretScope::Local, &pass).expect("export empty");
        let payload = open_bundle(&bundle, &pass).expect("open empty");
        assert!(payload.entries.is_empty());

        let tmp2 = tempfile::tempdir().unwrap();
        let mut fresh = new_store(tmp2.path()).await;
        let report =
            import_bundle(&mut fresh, &bundle, &pass, &TargetScope::Local, false).expect("import");
        assert_eq!(report, ImportReport::default());
    }

    #[tokio::test]
    async fn tampering_with_header_or_ciphertext_fails_auth() {
        // Pins the AAD guarantee: a single flipped byte in the header region
        // (bound as AEAD additional data) or in the ciphertext body must make
        // decryption fail with AuthFailed, never silently succeed.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let d = delegate(11);
        let s = SecretsId::new(b"tamper".to_vec());
        store
            .store_secret(
                d.key(),
                &s,
                SecretScope::Local,
                Zeroizing::new(b"sensitive".to_vec()),
            )
            .unwrap();
        let pass = BundleKeyMaterial::Passphrase(b"pw");
        let bundle = export_bundle(&store, SecretScope::Local, &pass).expect("export");
        // Sanity: the untampered bundle opens.
        open_bundle(&bundle, &pass).expect("untampered bundle must open");

        // (a) Flip a salt byte in the header. Salt lives at offset 6..6+SALT_LEN
        // (after MAGIC(4) + version(1) + kdf_id(1)) and is part of the AAD, so a
        // flip both changes the derived key AND breaks the AAD binding.
        let mut header_tampered = bundle.clone();
        header_tampered[6] ^= 0x01;
        match open_bundle(&header_tampered, &pass) {
            Err(ExportError::AuthFailed) => {}
            other => panic!("header tamper must fail auth, got {:?}", other.err()),
        }

        // (b) Flip a ciphertext byte (just past the header).
        let mut body_tampered = bundle.clone();
        let last = body_tampered.len() - 1;
        body_tampered[last] ^= 0x01;
        match open_bundle(&body_tampered, &pass) {
            Err(ExportError::AuthFailed) => {}
            other => panic!("ciphertext tamper must fail auth, got {:?}", other.err()),
        }
    }

    #[tokio::test]
    async fn export_over_count_limit_rejected_before_decrypt() {
        // An export gathering more secrets than the count cap is rejected with a
        // typed `TooLarge` error (mapped to a 413 at the HTTP layer), and the
        // check happens on the cheap metadata walk before any decrypt. Uses a
        // tiny limit so the test is deterministic without storing 10k secrets.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"count-limit-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(1);
        // Three secrets for the same user; cap at 2.
        for i in 0..3u8 {
            let s = SecretsId::new(vec![b'k', i]);
            put_user(&mut store, &d, &ctx, &s, b"v");
        }
        let material = BundleKeyMaterial::Token(token);
        let err = export_bundle_with_limits(&store, ctx.scope(), &material, 2, usize::MAX)
            .expect_err("over-count export must be rejected");
        let ExportError::TooLarge {
            what,
            actual,
            limit,
        } = err
        else {
            panic!("expected TooLarge(secret count), got {err:?}");
        };
        assert_eq!(what, "secret count");
        assert_eq!(actual, 3);
        assert_eq!(limit, 2);
    }

    #[tokio::test]
    async fn export_at_count_limit_succeeds() {
        // Boundary: count exactly AT the limit still exports (the check is `>`,
        // not `>=`), so the cap never rejects a legitimate at-capacity user.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"at-limit-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(2);
        for i in 0..2u8 {
            let s = SecretsId::new(vec![b'k', i]);
            put_user(&mut store, &d, &ctx, &s, b"v");
        }
        let material = BundleKeyMaterial::Token(token);
        let bundle = export_bundle_with_limits(&store, ctx.scope(), &material, 2, usize::MAX)
            .expect("export at the count limit must succeed");
        let payload = open_bundle(&bundle, &material).expect("open");
        assert_eq!(payload.entries.len(), 2);
    }

    #[tokio::test]
    async fn export_over_byte_limit_rejected() {
        // An export whose total decrypted plaintext exceeds the byte cap is
        // rejected with a typed `TooLarge` error. Uses a tiny byte limit and a
        // payload that exceeds it.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"byte-limit-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(3);
        let s = SecretsId::new(b"big".to_vec());
        // 100 bytes of plaintext, cap at 50.
        put_user(&mut store, &d, &ctx, &s, &[7u8; 100]);
        let material = BundleKeyMaterial::Token(token);
        let err = export_bundle_with_limits(&store, ctx.scope(), &material, usize::MAX, 50)
            .expect_err("over-byte export must be rejected");
        let ExportError::TooLarge {
            what,
            actual,
            limit,
        } = err
        else {
            panic!("expected TooLarge(total plaintext bytes), got {err:?}");
        };
        assert_eq!(what, "total plaintext bytes");
        assert_eq!(actual, 100);
        assert_eq!(limit, 50);
    }

    #[tokio::test]
    async fn export_scope_entries_bounded_bails_incrementally() {
        // INCREMENTAL bail (#4531 P5): the byte cap aborts the decrypt loop the
        // instant the running total crosses the limit, WITHOUT decrypting and
        // buffering the whole scope. With three 40-byte secrets and a 50-byte
        // cap, the bail must fire at total=80 (after the SECOND secret), i.e.
        // before the third is ever read — the reported `actual` is 80, not 120.
        use crate::wasm_runtime::secrets_store::ExportScopeError;
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"incremental-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(9);
        for i in 0..3u8 {
            let s = SecretsId::new(vec![b'b', i]);
            put_user(&mut store, &d, &ctx, &s, &[1u8; 40]);
        }
        // `ExportSecretEntry` deliberately has no `Debug` (it holds decrypted
        // plaintext), so match on the result rather than `expect_err`.
        let err = match store.export_scope_entries_bounded(ctx.scope(), 50) {
            Ok(_) => panic!("over-byte gather must bail, not succeed"),
            Err(e) => e,
        };
        let ExportScopeError::TooLarge { actual, limit } = err else {
            panic!("expected ExportScopeError::TooLarge, got {err:?}");
        };
        assert_eq!(limit, 50);
        // Bailed at the SECOND secret (40 + 40 = 80 > 50), NOT after buffering
        // all three (which would be 120). This is the proof that the partial
        // buffer was dropped mid-loop.
        assert_eq!(
            actual, 80,
            "must bail the instant the running total crosses the cap, \
             not after gathering the whole scope"
        );
    }

    #[tokio::test]
    async fn export_under_production_limits_succeeds() {
        // A normal small export goes through the PUBLIC entry point (which uses
        // the production caps), so the cap never regresses a real user.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"normal-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(4);
        let s = SecretsId::new(b"ordinary".to_vec());
        let h = put_user(&mut store, &d, &ctx, &s, b"ordinary-value");
        let material = BundleKeyMaterial::Token(token);
        let bundle = export_bundle(&store, ctx.scope(), &material).expect("normal export succeeds");
        let payload = open_bundle(&bundle, &material).expect("open");
        assert_eq!(payload.entries.len(), 1);
        assert_eq!(payload.entries[0].secret_hash.as_slice(), h.as_slice());
    }

    #[tokio::test]
    async fn export_includes_secret_written_by_a_different_executor() {
        // THE #4 REGRESSION (cross-executor staleness): under the pooled-executor
        // model two executors share one on-disk `secrets_dir` but each keeps its
        // OWN in-memory index. A secret stored via executor A is NOT in executor
        // B's index. The export — which may run on B — must still include A's
        // secret, because it enumerates from DISK (the shared source of truth),
        // not from B's stale in-memory index. With the old in-memory enumeration
        // this export would silently OMIT the secret → incomplete backup.
        let tmp = tempfile::tempdir().unwrap();
        let shared_secrets = tmp.path().join("shared-secrets");
        let token = b"cross-executor-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(1);
        let s_a = SecretsId::new(b"written-by-A".to_vec());
        let s_b = SecretsId::new(b"written-by-B".to_vec());

        // Executor A's store: writes secret A to the shared dir.
        let mut store_a = new_store_sharing(&shared_secrets, &tmp.path().join("db-a")).await;
        let h_a = put_user(&mut store_a, &d, &ctx, &s_a, b"value-A");

        // Executor B's store: a SEPARATE in-memory index over the SAME secrets
        // dir. It only knows about the secret IT writes (B); it has never seen A.
        let mut store_b = new_store_sharing(&shared_secrets, &tmp.path().join("db-b")).await;
        let h_b = put_user(&mut store_b, &d, &ctx, &s_b, b"value-B");

        // Export via executor B. The bundle MUST contain BOTH secrets — A's
        // (written by the other executor, absent from B's in-memory index) and
        // B's own. Pre-fix, A's secret would be missing.
        let material = BundleKeyMaterial::Token(token);
        let bundle =
            export_bundle(&store_b, ctx.scope(), &material).expect("export via B succeeds");
        let payload = open_bundle(&bundle, &material).expect("open");

        let exported: std::collections::HashSet<Vec<u8>> = payload
            .entries
            .iter()
            .map(|e| e.secret_hash.clone())
            .collect();
        assert!(
            exported.contains(h_a.as_slice()),
            "export run on executor B MUST include the secret written by executor A \
             (disk is the source of truth, not B's stale in-memory index) — #4531/#4 fix"
        );
        assert!(
            exported.contains(h_b.as_slice()),
            "export must also include B's own secret"
        );
        assert_eq!(payload.entries.len(), 2, "exactly the two stored secrets");

        // The count cap (scope_entry_count) must also see BOTH (it walks disk).
        assert_eq!(
            store_b.scope_entry_count(&ctx.scope()).unwrap(),
            2,
            "scope_entry_count must count both on-disk secrets, not just B's index"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn export_fails_loud_on_unexpected_io_error() {
        // FAIL-LOUD (#4563 P2): an unexpected I/O error during enumeration must
        // make the export FAIL, not seal a silently-partial/empty bundle. We
        // make the user's on-disk scope dir UNREADABLE (chmod 000) so read_dir
        // returns PermissionDenied (not NotFound) — the export must return Err.
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"io-error-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(1);
        let s = SecretsId::new(b"k".to_vec());
        let _ = put_user(&mut store, &d, &ctx, &s, b"v");

        // The scope dir that now holds the secret: base/<delegate>/users/<id>.
        let scope_dir = tmp
            .path()
            .join("secrets")
            .join(d.key().encode())
            .join("users")
            .join(ctx.user_id().encode());
        assert!(scope_dir.is_dir(), "scope dir must exist after the write");

        // Make it unreadable. If running as root (chmod 000 is bypassed), skip —
        // the branch is also covered by the count path's identical fail-loud.
        std::fs::set_permissions(&scope_dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let still_readable = std::fs::read_dir(&scope_dir).is_ok();
        if still_readable {
            // root or a permissive FS: restore and skip the assertion.
            std::fs::set_permissions(&scope_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            return;
        }

        let material = BundleKeyMaterial::Token(token);
        let result = export_bundle(&store, ctx.scope(), &material);
        // Restore perms so tempdir cleanup works regardless of the assertion.
        std::fs::set_permissions(&scope_dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            result.is_err(),
            "an unreadable scope dir must make the export FAIL, not seal a partial bundle"
        );
    }

    #[tokio::test]
    async fn export_of_absent_scope_is_clean_empty_not_error() {
        // A user with NO stored secrets has no on-disk users/<id> dir → read_dir
        // NotFound, which is EXPECTED-absent → a clean EMPTY bundle, NOT an error
        // (must not be conflated with the fail-loud I/O-error path).
        let tmp = tempfile::tempdir().unwrap();
        let store = new_store(tmp.path()).await;
        let token = b"never-stored-anything";
        let ctx = UserSecretContext::from_token(token);
        let material = BundleKeyMaterial::Token(token);
        let bundle = export_bundle(&store, ctx.scope(), &material)
            .expect("absent user scope must export a clean empty bundle, not error");
        let payload = open_bundle(&bundle, &material).expect("open empty");
        assert!(payload.entries.is_empty());
    }

    #[tokio::test]
    async fn count_cap_check_streams_and_stops_early() {
        // The count-cap check must decide "over the cap" WITHOUT enumerating the
        // whole (attacker-controlled) scope. We store 5 secrets and assert
        // scope_count_exceeds(cap=2) is true. The streaming counter stops after
        // seeing 3 (cap+1), so it never materializes all 5 — the gather
        // (export_scope_entries_bounded) is never reached for an over-cap scope.
        let tmp = tempfile::tempdir().unwrap();
        let mut store = new_store(tmp.path()).await;
        let token = b"count-stream-user";
        let ctx = UserSecretContext::from_token(token);
        let d = delegate(1);
        for i in 0..5u8 {
            let s = SecretsId::new(vec![b'k', i]);
            put_user(&mut store, &d, &ctx, &s, b"v");
        }
        // Over the cap of 2.
        assert!(
            store.scope_count_exceeds(&ctx.scope(), 2).unwrap(),
            "5 secrets must exceed a cap of 2"
        );
        // At/under the cap is NOT exceeded (boundary: 5 with cap 5 → not over).
        assert!(
            !store.scope_count_exceeds(&ctx.scope(), 5).unwrap(),
            "exactly-at-cap must NOT be flagged as exceeding"
        );
        assert!(
            !store.scope_count_exceeds(&ctx.scope(), 10).unwrap(),
            "under-cap must NOT be flagged as exceeding"
        );
        // And the full export of an over-cap scope is rejected as TooLarge.
        let material = BundleKeyMaterial::Token(token);
        let err = export_bundle_with_limits(&store, ctx.scope(), &material, 2, usize::MAX)
            .expect_err("over-count export must be rejected");
        let ExportError::TooLarge { what, limit, .. } = err else {
            panic!("expected TooLarge(secret count), got {err:?}");
        };
        assert_eq!(what, "secret count");
        assert_eq!(limit, 2);
    }

    fn read_local(store: &SecretsStore, d: &Delegate<'static>, hash: &[u8; 32]) -> Vec<u8> {
        // Read by reconstructing the SecretsId path via the by-hash helper is
        // not public; instead enumerate and find by hash.
        let entries = store.export_scope_entries(SecretScope::Local).unwrap();
        entries
            .into_iter()
            .find(|e| &e.secret_hash == hash && e.delegate_key == *d.key())
            .map(|e| e.plaintext.to_vec())
            .expect("secret present at Local scope")
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }
}
