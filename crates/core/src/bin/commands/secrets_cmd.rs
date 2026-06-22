//! `freenet secrets` subcommand: manage the node KEK backend.
//!
//! - `kek-status` — report current backend + KEK fingerprint
//! - `kek-rotate` — generate a new KEK, re-encrypt every active
//!   secret + snapshot, swap the KEK atomically in the backend,
//!   delete the previous value
//! - `kek-migrate --to <X>` — copy the KEK from the current backend to
//!   target backend, update the marker, delete from the source
//!   (operator-confirmable)
//! - `snapshot-list` — inspect the per-secret snapshot history (the
//!   on-write backups created since #4034). Metadata only; never prints
//!   plaintext. Read-only, so safe with the node running, but a stopped
//!   node gives a point-in-time-consistent view.
//! - `snapshot-restore` — roll a delegate secret back to an earlier
//!   snapshot. The current value is itself snapshotted first, so the
//!   restore is reversible. The node MUST be stopped.
//!
//! The KEK operations require the node process to be stopped (the
//! on-disk secrets directory is exclusively owned during rotation /
//! migration so a concurrent `freenet` write does not race the rewrite);
//! so does `snapshot-restore`, which writes the active secret file.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
use chrono::{DateTime, Utc};
use freenet::dev_tool::{
    BundleKeyMaterial, ExportError, KEK_SIZE, KekBackendKind, RestoreError, RetentionPolicy,
    SecretScope, Secrets, SecretsStore, Storage, TargetScope, UserSecretContext, build_backend_for,
    export_bundle, import_bundle, list_snapshots, load_from_backend, read_backend_marker,
    replace_backend_marker, restore_snapshot_file, snapshot_dir_for_encoded, thin_snapshots,
    write_backend_marker, write_bundle_file,
};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[derive(clap::Parser, Clone, Debug)]
pub struct SecretsCliConfig {
    #[clap(subcommand)]
    pub command: SecretsCommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub enum SecretsCommand {
    /// Print the active KEK backend and the KEK fingerprint
    /// (`SHA-256(KEK)[..8]` in hex). Does NOT print the KEK itself.
    KekStatus(KekStatusArgs),
    /// Explicitly provision the node KEK into a specific backend
    /// BEFORE the first node start. Use this to opt in to the OS
    /// keyring backend (which the auto-resolver intentionally skips
    /// to avoid an unexpected Keychain / Credential Manager prompt
    /// at startup).
    KekInit(KekInitArgs),
    /// Generate a new KEK, re-encrypt every active secret + snapshot
    /// under the new derived DEKs, atomically swap the KEK in the
    /// backend, delete the previous value. The node MUST be stopped.
    KekRotate(KekRotateArgs),
    /// Move the KEK from its current backend to a target backend.
    /// Useful when migrating from `file` to `keyring` after installing
    /// a secret-storage daemon, or in reverse during disaster recovery.
    KekMigrate(KekMigrateArgs),
    /// List the per-secret snapshot history (the on-write backups
    /// created since #4034). Metadata only — never prints plaintext
    /// secret values. Read-only.
    SnapshotList(SnapshotListArgs),
    /// Roll a delegate secret back to an earlier snapshot. The current
    /// value is itself snapshotted first, so the restore is reversible.
    /// The node MUST be stopped.
    SnapshotRestore(SnapshotRestoreArgs),
    /// Export a scope's delegate secrets into a single encrypted, portable
    /// bundle file (P3 of #4381). Primary use: a hosted user downloading
    /// their per-user secrets to re-import into their own peer; also backs
    /// up a normal node's Local secrets. The node MUST be stopped.
    ///
    /// SECURITY: the bundle is ALWAYS encrypted at rest (passphrase- or
    /// token-derived key). Building it requires the node to decrypt each
    /// secret in memory, so an operator running this command observes the
    /// plaintext during the export — inherent to the hosted model.
    Export(ExportArgs),
    /// Import a secrets bundle produced by `secrets export` (P3 of #4381),
    /// re-placing each secret under its original delegate. Default target is
    /// the single-user (Local) scope — the path a user takes when moving to
    /// their own peer. The node MUST be stopped.
    Import(ImportArgs),
}

#[derive(clap::Parser, Clone, Debug)]
pub struct KekStatusArgs {
    /// Path to the node's secrets directory (where `node_kek` /
    /// `kek_backend` / `delegate_cipher` live).
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct KekInitArgs {
    /// Path to the node's secrets directory. Will be created if it
    /// doesn't yet exist.
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Backend to provision the KEK into: `keyring`, `systemd`, or `file`.
    ///
    /// Choosing `keyring` triggers the OS keyring write here (which is
    /// what produces the platform's consent dialog on macOS/Linux for
    /// dev/unsigned builds) — the act of running this command is the
    /// operator's explicit consent.
    #[clap(long)]
    pub backend: String,
    /// Acknowledge that this is an interactive provisioning step.
    #[clap(long)]
    pub yes: bool,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct KekRotateArgs {
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Skip the interactive confirmation prompt (for scripted use).
    #[clap(long)]
    pub yes: bool,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct KekMigrateArgs {
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Target backend: `keyring`, `systemd`, or `file`.
    #[clap(long)]
    pub to: String,
    #[clap(long)]
    pub yes: bool,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct SnapshotListArgs {
    /// Path to the node's secrets directory (the parent of the
    /// per-delegate secret subdirectories).
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Restrict the listing to a single delegate, by its bs58-encoded
    /// delegate key (as shown in the unfiltered listing).
    #[clap(long)]
    pub delegate: Option<String>,
    /// Restrict the listing to a single secret, by its bs58-encoded id
    /// (as shown in the per-delegate listing). Requires `--delegate`.
    /// When set, every individual snapshot (timestamp + size) is printed
    /// instead of just the per-secret count.
    #[clap(long, requires = "delegate")]
    pub secret: Option<String>,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct SnapshotRestoreArgs {
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// bs58-encoded delegate key (copy it from `snapshot-list`).
    #[clap(long)]
    pub delegate: String,
    /// bs58-encoded secret id (copy it from `snapshot-list`).
    #[clap(long)]
    pub secret: String,
    /// `timestamp_ms` of the snapshot to restore (copy it from
    /// `snapshot-list`).
    #[clap(long)]
    pub timestamp_ms: u64,
    /// Collision suffix (the `suffix` column from `snapshot-list`), for
    /// the rare case of multiple same-millisecond snapshots at one
    /// timestamp. Omit to restore the unsuffixed snapshot at the
    /// timestamp (the `-` row, and the only entry when there is no
    /// collision); pass the number to target a specific collision row.
    #[clap(long)]
    pub suffix: Option<u32>,
    /// Skip the interactive confirmation prompt (for scripted use).
    #[clap(long)]
    pub yes: bool,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct ExportArgs {
    /// Path to the node's secrets directory (the on-disk secret blobs).
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Path to the node's data directory (the ReDb `db` file with the
    /// secrets index). This is the node's `--db-dir` (or the data dir under
    /// which the runtime created `db`), NOT the secrets dir. Required because
    /// the export walks the secrets index to enumerate what to gather.
    #[clap(long, value_parser)]
    pub db_dir: PathBuf,
    /// Export ALL single-user (Local) secrets — the normal-node backup case.
    /// Mutually exclusive with `--user-token`.
    #[clap(long, conflicts_with = "user_token")]
    pub local: bool,
    /// Export every per-user secret for the user identified by this opaque
    /// token (the hosted → self-host migration case). Mutually exclusive with
    /// `--local`. Pass `--user-token` ALONE to select the user scope and source
    /// the token safely (from `FREENET_USER_TOKEN`, else an interactive prompt),
    /// or `--user-token <tok>` to provide it inline. WARNING: an inline value is
    /// exposed via the process listing and shell history — prefer the env var or
    /// the prompt.
    #[clap(long, num_args = 0..=1, default_missing_value = "")]
    pub user_token: Option<String>,
    /// Encrypt the bundle under a key derived from a passphrase (Argon2id). This
    /// is the DEFAULT key method. The passphrase is read from
    /// `FREENET_SECRET_PASSPHRASE`, else this flag's inline value, else an
    /// interactive hidden prompt (pass `--passphrase` alone to force the
    /// prompt). WARNING: an inline value is exposed via the process listing and
    /// shell history — prefer the env var or the prompt.
    #[clap(long, num_args = 0..=1, default_missing_value = "", conflicts_with = "use_token_key")]
    pub passphrase: Option<String>,
    /// Encrypt the bundle under a key derived from the `--user-token` (HKDF),
    /// so a hosted user who only has their token needs no separate passphrase.
    /// Requires `--user-token`. Mutually exclusive with `--passphrase`.
    #[clap(long, requires = "user_token")]
    pub use_token_key: bool,
    /// Output file for the encrypted bundle. Refuses to overwrite an existing
    /// file. Written owner-only (0o600 on Unix).
    #[clap(long, value_parser)]
    pub out: PathBuf,
}

#[derive(clap::Parser, Clone, Debug)]
pub struct ImportArgs {
    /// The encrypted bundle file produced by `secrets export`.
    #[clap(value_parser)]
    pub bundle: PathBuf,
    /// Path to the target node's secrets directory.
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
    /// Path to the target node's data directory (the ReDb `db`). See the
    /// matching `--db-dir` note on `export`.
    #[clap(long, value_parser)]
    pub db_dir: PathBuf,
    /// Decrypt the bundle with a passphrase (Argon2id-keyed bundle). This is the
    /// DEFAULT decrypt method. The passphrase is read from
    /// `FREENET_SECRET_PASSPHRASE`, else this flag's inline value, else an
    /// interactive hidden prompt (pass `--passphrase` alone to force the
    /// prompt). WARNING: an inline value is exposed via the process listing and
    /// shell history — prefer the env var or the prompt.
    #[clap(long, num_args = 0..=1, default_missing_value = "", conflicts_with = "use_token_key")]
    pub passphrase: Option<String>,
    /// Decrypt the bundle with the user token instead of a passphrase (a bundle
    /// exported with `--use-token-key`). Selects the token-decrypt method; the
    /// token value is resolved like `--token`. Mutually exclusive with
    /// `--passphrase`.
    #[clap(long)]
    pub use_token_key: bool,
    /// The user token value for token-decrypt (used with `--use-token-key`).
    /// Resolved from `FREENET_USER_TOKEN`, else this flag's inline value, else an
    /// interactive prompt. WARNING: an inline value is exposed via the process
    /// listing and shell history — prefer the env var or the prompt.
    #[clap(long, num_args = 0..=1, default_missing_value = "", conflicts_with = "passphrase")]
    pub token: Option<String>,
    /// Place imported secrets at the single-user (Local) scope. This is the
    /// DEFAULT when neither `--local` nor `--into-user` is given — it is the
    /// path a user takes when migrating to their own peer. Mutually exclusive
    /// with `--into-user`.
    #[clap(long, conflicts_with = "into_user")]
    pub local: bool,
    /// Place imported secrets under a per-user scope keyed by this token
    /// (round-trip / re-hosting). Mutually exclusive with `--local`. Pass
    /// `--into-user` alone to be prompted for the token, or `--into-user <tok>`
    /// to provide it inline (exposed via the process listing — prefer the
    /// prompt).
    #[clap(long, num_args = 0..=1, default_missing_value = "")]
    pub into_user: Option<String>,
    /// Overwrite an existing secret at the same delegate+id. Without this an
    /// existing value is left untouched and reported as skipped.
    #[clap(long)]
    pub overwrite: bool,
}

pub async fn run(config: SecretsCliConfig) -> Result<()> {
    match config.command {
        SecretsCommand::KekStatus(args) => kek_status(args).await,
        SecretsCommand::KekInit(args) => kek_init(args).await,
        SecretsCommand::KekRotate(args) => kek_rotate(args).await,
        SecretsCommand::KekMigrate(args) => kek_migrate(args).await,
        SecretsCommand::SnapshotList(args) => snapshot_list(args).await,
        SecretsCommand::SnapshotRestore(args) => snapshot_restore(args).await,
        SecretsCommand::Export(args) => secrets_export(args).await,
        SecretsCommand::Import(args) => secrets_import(args).await,
    }
}

fn parse_backend_kind(s: &str) -> Result<KekBackendKind> {
    match s {
        "keyring" => Ok(KekBackendKind::Keyring),
        "systemd" => Ok(KekBackendKind::Systemd),
        "file" => Ok(KekBackendKind::File),
        other => Err(anyhow!(
            "unrecognized backend `{other}`; expected keyring|systemd|file"
        )),
    }
}

async fn kek_init(args: KekInitArgs) -> Result<()> {
    let target = parse_backend_kind(&args.backend)?;

    // Prior provisioning short-circuits — refuse rather than racing
    // with an existing KEK. Operators migrating between backends after
    // first start use `kek-migrate`, not `kek-init`.
    if let Some(existing) =
        read_backend_marker(&args.secrets_dir).context("failed to read kek_backend marker")?
    {
        bail!(
            "KEK already provisioned on `{existing}` backend in {}. Use \
             `freenet secrets kek-migrate --to {target}` to switch backends.",
            args.secrets_dir.display()
        );
    }

    if !args.yes {
        eprintln!(
            "About to opt in to the `{target}` backend for the node KEK at {}.\n\
             - `keyring`: writes a freshly generated KEK to the OS secret store.\n\
               * macOS:   binds to current binary signature; ad-hoc / dev builds re-prompt\n\
                          every start; signed builds re-prompt after each upgrade.\n\
               * Windows: silent write to Credential Manager.\n\
               * Linux:   NOT supported in this build (workspace ships `keyring` without\n\
                          Linux-native features to avoid the libdbus build dep).\n\
             - `systemd`: marker-only opt-in; the credential MUST be provisioned out-of-band\n\
                          via `LoadCredentialEncrypted=freenet-kek:/path` on the unit.\n\
                          The node will fail to start if the credential is not present.\n\
             - `file`:    writes `node_kek` (0o600) to the secrets dir. Weakest option;\n\
                          operator must protect the directory.\n\
             Re-run with --yes to proceed.",
            args.secrets_dir.display()
        );
        bail!("aborted (no --yes)");
    }

    std::fs::create_dir_all(&args.secrets_dir).with_context(|| {
        format!(
            "failed to create secrets dir {}",
            args.secrets_dir.display()
        )
    })?;

    match target {
        KekBackendKind::Systemd => {
            // Systemd credentials are populated by the service manager.
            // `kek-init --backend systemd` is a marker-only opt-in: it
            // records the operator's intent so the node loads from the
            // systemd credential on first start instead of running the
            // auto-resolver and provisioning to file. The credential
            // itself MUST exist by the time the node boots, set up via
            // `LoadCredentialEncrypted=freenet-kek:/path` on the unit.
            write_backend_marker(&args.secrets_dir, target).context(
                "failed to write kek_backend marker for systemd opt-in (no KEK was provisioned)",
            )?;
            println!(
                "Opted in to `systemd` backend by writing kek_backend marker. \
                 The KEK itself MUST be provisioned by the service manager via \
                 `LoadCredentialEncrypted=freenet-kek:/path` on the freenet unit. \
                 The node will fail to start if the credential is absent."
            );
            Ok(())
        }
        KekBackendKind::Keyring | KekBackendKind::File => {
            let backend = build_backend_for(target, &args.secrets_dir).with_context(|| {
                format!(
                    "failed to construct `{target}` backend handle (is it available on this \
                     platform?)"
                )
            })?;

            // Cryptographic key material — `OsRng` (not `GlobalRng`)
            // is the right choice here: KEK is the node's root secret,
            // must come from the OS entropy pool, never from a
            // seedable simulation RNG.
            let mut kek_bytes = Zeroizing::new([0u8; KEK_SIZE]);
            OsRng.fill_bytes(kek_bytes.as_mut_slice());

            backend.store(&kek_bytes).with_context(|| {
                format!(
                    "failed to store KEK in `{target}` backend (existing entry? delete first \
                     via OS tools)"
                )
            })?;

            // Atomic with rollback: if the marker write fails AFTER
            // `backend.store` succeeded, the KEK is orphaned in the
            // backend. Roll it back via `backend.delete()` and report
            // the compound failure so the operator can re-run cleanly
            // instead of hitting `KekError::AlreadyExists` next time.
            if let Err(marker_err) = write_backend_marker(&args.secrets_dir, target) {
                let scrub = backend.delete();
                let scrub_msg = match scrub {
                    Ok(()) => format!(
                        "Rolled back the orphaned KEK in `{target}` backend; safe to re-run."
                    ),
                    Err(e) => format!(
                        "Marker write failed AND rollback of the stored KEK in `{target}` \
                         backend also failed ({e}); scrub the entry manually before re-running."
                    ),
                };
                return Err(anyhow!(
                    "failed to write kek_backend marker after provisioning: {marker_err}. {scrub_msg}"
                ));
            }

            let mut hasher = Sha256::new();
            hasher.update(kek_bytes.as_slice());
            let fp = hasher.finalize();
            let fp_hex: String = fp[..8].iter().map(|b| format!("{b:02x}")).collect();

            println!("Provisioned KEK into `{target}` backend.");
            println!("KEK fingerprint (SHA-256[..8]): {fp_hex}");
            Ok(())
        }
    }
}

async fn kek_status(args: KekStatusArgs) -> Result<()> {
    let kind = match read_backend_marker(&args.secrets_dir)
        .context("failed to read kek_backend marker")?
    {
        Some(k) => k,
        None => {
            println!("KEK backend: <not provisioned yet — first node start will resolve>");
            return Ok(());
        }
    };
    println!("KEK backend: {kind}");

    // Load the KEK only to compute a fingerprint; buffer wiped on
    // scope exit (Zeroizing). Direct `load_from_backend` (not
    // `ensure_kek_loaded`) so an operator who removes the marker
    // between our read and the load gets a clean error instead of the
    // resolver triggering provisioning + the previously-`unreachable!`
    // supplier path.
    let kek = load_from_backend(kind, &args.secrets_dir)
        .with_context(|| format!("failed to load KEK from `{kind}` backend"))?;

    let mut hasher = Sha256::new();
    hasher.update(kek.as_slice());
    let fp = hasher.finalize();
    let fp_hex: String = fp[..8].iter().map(|b| format!("{b:02x}")).collect();
    println!("KEK fingerprint (SHA-256[..8]): {fp_hex}");

    Ok(())
}

async fn kek_rotate(_args: KekRotateArgs) -> Result<()> {
    // KEK rotation requires walking every secret + snapshot on disk,
    // re-deriving DEKs under the new KEK, decrypting under the OLD
    // DEKs, re-encrypting under the new DEKs with fresh nonces, and
    // swapping the KEK in the backend.
    //
    // Bail BEFORE checking `--yes`. Without this ordering an operator
    // who runs `kek-rotate` once, reads the safety blurb, takes a
    // backup, and re-runs with `--yes` is rewarded with a "not
    // implemented" error — actively misleading. The not-implemented
    // status is unconditional, so it must surface unconditionally.
    //
    // Two-phase crash-safe variant (write `.rot` shadow files, swap
    // KEK only after all shadows exist, then atomically rename shadows
    // → active on next start) is tracked in a follow-up under #4137.
    bail!(
        "kek-rotate on-disk walk not yet implemented in this build. \
         Operators wanting to rotate today should: stop the node, run \
         `freenet secrets kek-migrate --to file --yes` (or to a new backend), \
         move the existing secrets dir aside, start the node fresh, and \
         re-upload delegate secrets via clients. Tracked under #4137."
    );
}

async fn kek_migrate(args: KekMigrateArgs) -> Result<()> {
    let target = parse_backend_kind(&args.to)?;

    let current = read_backend_marker(&args.secrets_dir)
        .context("failed to read kek_backend marker")?
        .ok_or_else(|| {
            anyhow!(
                "no KEK provisioned yet (kek_backend marker missing); \
                 start the node once to provision before migrating"
            )
        })?;

    if current == target {
        println!("KEK is already on the `{target}` backend; nothing to do.");
        return Ok(());
    }

    if !args.yes {
        eprintln!(
            "About to migrate KEK from `{current}` to `{target}` in {}.",
            args.secrets_dir.display()
        );
        eprintln!(
            "Stop the freenet node before running this command. \
             Re-run with --yes to proceed."
        );
        bail!("aborted (no --yes)");
    }

    // Load KEK from source.
    let src_backend =
        build_backend_for(current, &args.secrets_dir).context("failed to open source backend")?;
    let kek = src_backend
        .load()
        .context("failed to load KEK from source backend")?
        .ok_or_else(|| anyhow!("source backend reports no KEK present"))?;

    // Store in target. `store` enforces no-overwrite — if the target
    // already has a KEK (e.g. a stale entry from a prior aborted
    // migration), the operator must delete it manually before retrying.
    let dst_backend =
        build_backend_for(target, &args.secrets_dir).context("failed to open target backend")?;
    dst_backend
        .store(&kek)
        .context("failed to store KEK in target backend (existing entry? delete first)")?;

    // Atomic-rewrite the marker BEFORE deleting from source. If the
    // process crashes here, the source still holds the KEK and the
    // marker now points at the target — the next start will load from
    // target successfully (which has the same KEK bytes).
    //
    // Uses `replace_backend_marker` (kek.rs) so the tmp file is written
    // with the same 0o600 + sync_all discipline as the in-process
    // first-start marker — no perms drift, no unsynced data.
    replace_backend_marker(&args.secrets_dir, target)
        .context("failed to atomically rewrite kek_backend marker to point at target")?;

    // Delete from source last. If this fails, the migration is still
    // valid (target has the KEK + marker points at target). Operator
    // should manually scrub the source backend entry.
    if let Err(e) = src_backend.delete() {
        eprintln!(
            "WARNING: failed to delete KEK from `{current}` backend after migration: {e}\n\
             The migration succeeded (target has the KEK + marker points there), but the \
             stale source entry should be removed manually."
        );
    }

    println!("Migrated KEK from `{current}` to `{target}`.");
    Ok(())
}

/// Format an epoch-millis timestamp as an RFC3339 UTC string for
/// display. Falls back to a raw annotation if the value is out of range
/// (should never happen for a real snapshot filename).
fn format_ts(timestamp_ms: u64) -> String {
    DateTime::<Utc>::from_timestamp_millis(timestamp_ms as i64)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| format!("{timestamp_ms}ms"))
}

/// Reject an operator-supplied delegate/secret id that isn't a single,
/// normal path component, so a fat-fingered or pasted `--delegate` /
/// `--secret` (e.g. one containing `/`, `\`, or `..`) can't make the
/// `--secrets-dir` join escape the secrets tree and clobber or disclose
/// a file elsewhere. On-disk ids are bs58 (no separators, no dots), so
/// this never rejects a value copied from `snapshot-list`.
fn validate_component(label: &str, value: &str) -> Result<()> {
    use std::path::Component;
    let mut comps = std::path::Path::new(value).components();
    match (comps.next(), comps.next()) {
        (Some(Component::Normal(c)), None) if c == std::ffi::OsStr::new(value) => Ok(()),
        _ => bail!(
            "invalid {label} `{value}`: must be a single path component with no `/`, `\\`, or \
             `..` — copy the exact value shown by `freenet secrets snapshot-list`"
        ),
    }
}

/// Enumerate the immediate subdirectory names of `dir`, sorted. Used to
/// walk delegate directories under the secrets root and per-secret
/// snapshot directories under a delegate's `.snapshots/`.
fn subdir_names(dir: &std::path::Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type().is_ok_and(|ft| ft.is_dir())
            && let Some(name) = entry.file_name().to_str()
        {
            out.push(name.to_string());
        }
    }
    out.sort();
    Ok(out)
}

async fn snapshot_list(args: SnapshotListArgs) -> Result<()> {
    if !args.secrets_dir.exists() {
        bail!("secrets dir {} does not exist", args.secrets_dir.display());
    }
    if let Some(d) = &args.delegate {
        validate_component("delegate", d)?;
    }
    if let Some(s) = &args.secret {
        validate_component("secret", s)?;
    }

    // Detailed single-secret view (`--delegate X --secret Y`): print
    // every snapshot row.
    if let Some(secret) = &args.secret {
        // clap's `requires = "delegate"` guarantees this is Some.
        let delegate = args
            .delegate
            .as_ref()
            .expect("clap enforces --secret requires --delegate");
        let delegate_dir = args.secrets_dir.join(delegate);
        if !delegate_dir.is_dir() {
            bail!(
                "no delegate directory `{delegate}` under {}",
                args.secrets_dir.display()
            );
        }
        let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
        let snaps = list_snapshots(&snap_dir)
            .with_context(|| format!("failed to list snapshots in {}", snap_dir.display()))?;
        println!("Delegate {delegate}");
        println!("Secret   {secret}");
        if snaps.is_empty() {
            println!("  (no snapshot history)");
            return Ok(());
        }
        println!(
            "  {:>16}  {:<20}  {:>9}  suffix",
            "timestamp_ms", "utc", "bytes"
        );
        for s in &snaps {
            let suffix = s
                .suffix
                .map(|x| x.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "  {:>16}  {:<20}  {:>9}  {suffix}",
                s.timestamp_ms,
                format_ts(s.timestamp_ms),
                s.size_bytes,
            );
        }
        println!("  {} snapshot(s)", snaps.len());
        return Ok(());
    }

    // Summary view: one or all delegates, per-secret snapshot counts.
    let delegates = match &args.delegate {
        Some(d) => {
            if !args.secrets_dir.join(d).is_dir() {
                bail!(
                    "no delegate directory `{d}` under {}",
                    args.secrets_dir.display()
                );
            }
            vec![d.clone()]
        }
        None => subdir_names(&args.secrets_dir)?,
    };

    if delegates.is_empty() {
        println!(
            "No delegate directories found under {}",
            args.secrets_dir.display()
        );
        return Ok(());
    }

    println!("Secrets dir: {}", args.secrets_dir.display());
    for delegate in &delegates {
        let delegate_dir = args.secrets_dir.join(delegate);
        let snapshots_root = delegate_dir.join(".snapshots");
        println!("Delegate {delegate}");
        let secrets = if snapshots_root.is_dir() {
            subdir_names(&snapshots_root)?
        } else {
            Vec::new()
        };
        let mut any = false;
        for secret in &secrets {
            let snap_dir = snapshot_dir_for_encoded(&delegate_dir, secret);
            // Propagate (don't swallow) a read/stat error: during the
            // recovery scenario this tool exists for, silently rendering
            // an unreadable snapshot dir as "no history" would mislead
            // the operator. Matches the detail view's `?` handling.
            let snaps = list_snapshots(&snap_dir)
                .with_context(|| format!("failed to list snapshots in {}", snap_dir.display()))?;
            if snaps.is_empty() {
                continue;
            }
            any = true;
            let latest = snaps
                .last()
                .map(|s| format_ts(s.timestamp_ms))
                .unwrap_or_default();
            println!(
                "  Secret {secret}: {} snapshot(s), latest {latest}",
                snaps.len()
            );
        }
        if !any {
            println!("  (no snapshot history)");
        }
    }
    Ok(())
}

async fn snapshot_restore(args: SnapshotRestoreArgs) -> Result<()> {
    validate_component("delegate", &args.delegate)?;
    validate_component("secret", &args.secret)?;
    let delegate_dir = args.secrets_dir.join(&args.delegate);
    let snap_dir = snapshot_dir_for_encoded(&delegate_dir, &args.secret);
    if !snap_dir.is_dir() {
        bail!(
            "no snapshot history for secret `{}` of delegate `{}` under {} \
             (run `freenet secrets snapshot-list` to see what's available)",
            args.secret,
            args.delegate,
            args.secrets_dir.display()
        );
    }

    // Human-readable selector for messages: "timestamp_ms=N" or
    // "timestamp_ms=N suffix=K".
    let selector = match args.suffix {
        Some(s) => format!("timestamp_ms={} suffix={s}", args.timestamp_ms),
        None => format!("timestamp_ms={}", args.timestamp_ms),
    };

    if !args.yes {
        eprintln!(
            "About to restore secret `{}` of delegate `{}` to the snapshot at \
             {selector} in {}.\n\
             - The current value is snapshotted first, so this is reversible.\n\
             - The freenet node MUST be stopped (a running node may concurrently \
               write this secret).\n\
             Re-run with --yes to proceed.",
            args.secret,
            args.delegate,
            args.secrets_dir.display()
        );
        bail!("aborted (no --yes)");
    }

    // Byte-level restore via the shared core — the same code the node
    // runtime uses, so the durability discipline can't drift. The
    // reversibility snapshot is always enabled for the CLI (manual
    // recovery should be undoable).
    match restore_snapshot_file(
        &delegate_dir,
        &args.secret,
        args.timestamp_ms,
        args.suffix,
        true,
    ) {
        Ok(()) => {
            // Bound the history (incl. the reversibility snapshot just
            // added), matching the node runtime's default retention.
            // Best-effort: the restore has already succeeded.
            let snap_dir = snapshot_dir_for_encoded(&delegate_dir, &args.secret);
            thin_snapshots(
                &snap_dir,
                &RetentionPolicy::default(),
                std::time::SystemTime::now(),
            );
            println!(
                "Restored secret `{}` of delegate `{}` to snapshot {selector}.",
                args.secret, args.delegate
            );
            Ok(())
        }
        Err(RestoreError::NotFound(_)) => bail!(
            "no snapshot at {selector} for secret `{}` of delegate `{}`. Run \
             `freenet secrets snapshot-list --secrets-dir {} --delegate {} --secret {}` to see \
             available timestamps (and suffixes).",
            args.secret,
            args.delegate,
            args.secrets_dir.display(),
            args.delegate,
            args.secret
        ),
        Err(RestoreError::Io(e)) => Err(anyhow::Error::new(e).context("snapshot restore failed")),
    }
}

/// Open the target node's `SecretsStore` for an export/import operation.
///
/// The store needs both the on-disk secrets dir (the blobs) and the ReDb
/// `Storage` under the data dir (the index). Both are required because the
/// export enumerates the index; the snapshot CLIs above only touch the
/// filesystem, which is why they don't need the db dir.
async fn open_store(
    secrets_dir: &std::path::Path,
    db_dir: &std::path::Path,
) -> Result<SecretsStore> {
    if !secrets_dir.exists() {
        bail!("secrets dir {} does not exist", secrets_dir.display());
    }
    let db = Storage::new(db_dir)
        .await
        .with_context(|| format!("failed to open secrets index db under {}", db_dir.display()))?;
    // Load the node's persisted `Secrets` (delegate cipher / legacy nonce) so
    // the Local-scope legacy-decrypt fallback chain matches the running node.
    // User-scope secrets don't use this (they derive purely from the token),
    // and post-#4140 Local secrets derive from the node KEK; this only matters
    // for pre-#4140 Local blobs written under the auto-persisted cipher.
    let secrets = Secrets::load_for_secrets_dir(secrets_dir)
        .with_context(|| format!("failed to load node secrets from {}", secrets_dir.display()))?;
    SecretsStore::new(secrets_dir.to_path_buf(), secrets, db)
        .map_err(|e| anyhow!("failed to open secrets store: {e}"))
}

/// Map an [`ExportError`] to an `anyhow::Error`, keeping the auth-failure case
/// as a clear, actionable message and passing every other variant through with
/// its own `Display`.
fn map_export_err(e: ExportError) -> anyhow::Error {
    if matches!(e, ExportError::AuthFailed) {
        return anyhow!(
            "bundle authentication failed: wrong passphrase/token, mismatched key method \
             (passphrase vs token), or a corrupt bundle. No secrets were written."
        );
    }
    anyhow::Error::new(e)
}

/// Resolve a secret input (passphrase / user token) from the safest available
/// source, so secrets don't have to be passed on argv (where they leak via the
/// process table and shell history).
///
/// Priority:
///   1. Environment variable `env_var` (when `Some`), if set and non-empty.
///   2. The CLI `flag` value, if provided (last-resort convenience; the caller's
///      `--help` warns it is exposed via process listing).
///   3. An interactive prompt, when stdin is a TTY (hidden/no-echo for
///      passphrases). Non-interactive + no env + no flag is an error.
///
/// The result is `Zeroizing<String>` so it is wiped on drop. `label` names the
/// secret in the prompt and in the not-provided error. `env_var` is `None` for
/// inputs that have no dedicated env var (e.g. the `--into-user` target token,
/// which would be ambiguous with the decrypt token's env var).
///
/// Note on ordering: env beats flag deliberately — a script that sets the env
/// var should win over a stale flag, and it lets the unit test assert the
/// preference without a TTY. (Both are non-interactive sources; a human at a
/// terminal who passes neither gets the prompt.)
fn resolve_secret(
    flag: Option<&str>,
    env_var: Option<&str>,
    label: &str,
    hidden: bool,
) -> Result<Zeroizing<String>> {
    use std::io::IsTerminal;

    if let Some(env_var) = env_var
        && let Some(v) = std::env::var_os(env_var)
        && !v.is_empty()
    {
        let s = v
            .into_string()
            .map_err(|_| anyhow!("{env_var} is not valid UTF-8"))?;
        return Ok(Zeroizing::new(s));
    }

    // A non-empty flag value is the last-resort source. An EMPTY flag value
    // (e.g. bare `--user-token` via default_missing_value) means "flag present
    // to select the scope, but source the value safely" — fall through to the
    // prompt rather than using "".
    if let Some(flag) = flag
        && !flag.is_empty()
    {
        return Ok(Zeroizing::new(flag.to_string()));
    }

    if std::io::stdin().is_terminal() {
        let prompt = format!("Enter {label}: ");
        let value = if hidden {
            // No-echo prompt for passphrases.
            rpassword::prompt_password(&prompt)
                .with_context(|| format!("failed to read {label} from the terminal"))?
        } else {
            use std::io::Write as _;
            eprint!("{prompt}");
            std::io::stderr().flush().ok();
            let mut line = String::new();
            std::io::stdin()
                .read_line(&mut line)
                .with_context(|| format!("failed to read {label} from the terminal"))?;
            line.trim_end_matches(['\r', '\n']).to_string()
        };
        if value.is_empty() {
            bail!("empty {label}; aborting");
        }
        return Ok(Zeroizing::new(value));
    }

    let env_hint = match env_var {
        Some(e) => format!("set {e}, "),
        None => String::new(),
    };
    bail!(
        "no {label} provided: {env_hint}pass the corresponding flag, or run interactively \
         (stdin is not a TTY, so there is nothing to prompt)"
    )
}

/// Environment variable names for the safe secret sources. Documented in the
/// CLI `--help` for each flag so operators know the preferred input path.
const ENV_PASSPHRASE: &str = "FREENET_SECRET_PASSPHRASE";
const ENV_USER_TOKEN: &str = "FREENET_USER_TOKEN";

async fn secrets_export(args: ExportArgs) -> Result<()> {
    // Exactly one of --local / --user-token selects the source scope.
    if !args.local && args.user_token.is_none() {
        bail!("specify the source scope: --local (all single-user secrets) or --user-token <tok>");
    }

    // Key method: `--use-token-key` derives the bundle key from the user token
    // (requires --user-token, enforced by clap). Otherwise the default is a
    // passphrase, resolved below from env / flag / interactive prompt — so the
    // operator does NOT have to pass it on argv. No "neither specified" error:
    // absent --use-token-key we resolve a passphrase (prompting if interactive).

    // Refuse to export against a non-existent node database. `Storage::new`
    // CREATES `<db_dir>/db` if absent, so a wrong/empty --db-dir would silently
    // open a brand-new empty index and emit a valid-looking but EMPTY bundle —
    // the operator would think they had a backup but captured nothing. Require
    // the db file to already exist for export (import may legitimately create a
    // fresh db on a new peer). The "db" filename mirrors `Storage::new`.
    let db_file = args.db_dir.join("db");
    if !db_file.exists() {
        bail!(
            "no node database found at {} — is --db-dir correct and is the node initialized? \
             (export refuses to run against a missing/empty database to avoid emitting an empty bundle)",
            db_file.display()
        );
    }

    let store = open_store(&args.secrets_dir, &args.db_dir).await?;

    // Resolve the user token (scope selector, and bundle key when
    // --use-token-key) from a safe source. Only needed for the user scope.
    // `args.local` and `--user-token` are mutually exclusive (clap), so the
    // token is resolved exactly when a user-scope export was requested.
    let user_token: Option<Zeroizing<String>> = if args.local {
        None
    } else {
        Some(resolve_secret(
            args.user_token.as_deref(),
            Some(ENV_USER_TOKEN),
            "user token",
            false,
        )?)
    };

    // Build the source scope. For the user scope we derive a UserSecretContext
    // from the resolved token; its `.scope()` borrows the context, so the
    // context must outlive the export call.
    let user_ctx = user_token
        .as_ref()
        .map(|t| UserSecretContext::from_token(t.as_bytes()));
    let scope = if args.local {
        SecretScope::Local
    } else {
        user_ctx
            .as_ref()
            .expect("user scope selected => context built")
            .scope()
    };

    // Resolve the passphrase (only on the passphrase-key path) from a safe
    // source, hidden-prompted if interactive. The owned buffers must outlive
    // `export_bundle` since `BundleKeyMaterial` borrows them.
    let passphrase: Option<Zeroizing<String>> = if args.use_token_key {
        None
    } else {
        Some(resolve_secret(
            args.passphrase.as_deref(),
            Some(ENV_PASSPHRASE),
            "passphrase",
            true,
        )?)
    };
    let material = if let Some(pass) = passphrase.as_ref() {
        BundleKeyMaterial::Passphrase(pass.as_bytes())
    } else {
        // --use-token-key: derive the bundle key from the resolved user token.
        BundleKeyMaterial::Token(
            user_token
                .as_ref()
                .expect("--use-token-key requires --user-token")
                .as_bytes(),
        )
    };

    eprintln!(
        "Note: building the bundle decrypts every secret in this node's memory. \
         If this node is operated by someone other than you (hosted mode), they \
         can observe the plaintext during this export. The bundle FILE is always \
         encrypted at rest."
    );

    let bundle = export_bundle(&store, scope, &material).map_err(map_export_err)?;
    write_bundle_file(&args.out, &bundle).map_err(|e| {
        // Special-case the refuse-to-clobber path with a clearer message;
        // everything else passes through with its own Display.
        if let ExportError::Io(io) = &e
            && io.kind() == std::io::ErrorKind::AlreadyExists
        {
            return anyhow!(
                "refusing to overwrite existing file {}: choose a fresh --out path",
                args.out.display()
            );
        }
        anyhow::Error::new(e).context("failed to write bundle file")
    })?;

    println!(
        "Wrote encrypted secrets bundle to {} ({} bytes).",
        args.out.display(),
        bundle.len()
    );
    Ok(())
}

async fn secrets_import(args: ImportArgs) -> Result<()> {
    let bundle = fs::read(&args.bundle)
        .with_context(|| format!("failed to read bundle file {}", args.bundle.display()))?;

    let mut store = open_store(&args.secrets_dir, &args.db_dir).await?;

    // Decrypt method: token-decrypt when --use-token-key, else passphrase
    // (the default). The secret VALUE for the chosen method is resolved from a
    // safe source (env / flag / prompt) so it need not appear on argv. The
    // owned buffer must outlive `import_bundle` (BundleKeyMaterial borrows it).
    let secret = if args.use_token_key {
        resolve_secret(
            args.token.as_deref(),
            Some(ENV_USER_TOKEN),
            "user token",
            false,
        )?
    } else {
        resolve_secret(
            args.passphrase.as_deref(),
            Some(ENV_PASSPHRASE),
            "passphrase",
            true,
        )?
    };
    let material = if args.use_token_key {
        BundleKeyMaterial::Token(secret.as_bytes())
    } else {
        BundleKeyMaterial::Passphrase(secret.as_bytes())
    };

    // Target scope: Local by default (the self-host migration path), or a
    // per-user scope when --into-user is given. The token for the target scope
    // is also resolved from a safe source. The owned TargetScope holds any
    // UserSecretContext for the duration of the import.
    let into_user_token: Option<Zeroizing<String>> = if args.into_user.is_some() {
        // A dedicated env var would be ambiguous with the decrypt token's env
        // var, so the target-scope token resolves from its flag or an
        // interactive prompt only (env_var = None). Most imports use --local and
        // skip this entirely.
        Some(resolve_secret(
            args.into_user.as_deref(),
            None,
            "target user token (--into-user)",
            false,
        )?)
    } else {
        None
    };
    let target = match into_user_token.as_ref() {
        Some(tok) => TargetScope::user_from_token(tok.as_bytes()),
        None => TargetScope::Local,
    };

    let report = import_bundle(&mut store, &bundle, &material, &target, args.overwrite)
        .map_err(map_export_err)?;

    println!("Imported {} secret(s).", report.imported);
    if !report.skipped.is_empty() {
        println!(
            "Skipped {} secret(s) that already existed (re-run with --overwrite to replace):",
            report.skipped.len()
        );
        for (delegate, secret) in &report.skipped {
            println!("  delegate {delegate} secret {secret}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(dir: &std::path::Path, backend: &str, yes: bool) -> KekInitArgs {
        KekInitArgs {
            secrets_dir: dir.to_path_buf(),
            backend: backend.to_string(),
            yes,
        }
    }

    #[tokio::test]
    async fn parse_backend_kind_accepts_valid_and_rejects_garbage() {
        assert!(matches!(
            parse_backend_kind("keyring").unwrap(),
            KekBackendKind::Keyring
        ));
        assert!(matches!(
            parse_backend_kind("systemd").unwrap(),
            KekBackendKind::Systemd
        ));
        assert!(matches!(
            parse_backend_kind("file").unwrap(),
            KekBackendKind::File
        ));
        let err = parse_backend_kind("garbage").unwrap_err().to_string();
        assert!(
            err.contains("unrecognized backend"),
            "expected unrecognized-backend error, got: {err}"
        );
    }

    #[tokio::test]
    async fn kek_init_refuses_when_marker_already_exists() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path()).unwrap();
        write_backend_marker(dir.path(), KekBackendKind::File).expect("seed marker");

        let err = kek_init(args(dir.path(), "file", true))
            .await
            .expect_err("must refuse when marker exists");
        let msg = err.to_string();
        assert!(
            msg.contains("already provisioned"),
            "expected already-provisioned error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn kek_init_without_yes_aborts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = kek_init(args(dir.path(), "file", false))
            .await
            .expect_err("must abort without --yes");
        assert!(err.to_string().contains("aborted"));
        // No marker written.
        assert!(read_backend_marker(dir.path()).unwrap().is_none());
        // No node_kek file written either.
        assert!(!dir.path().join("node_kek").exists());
    }

    #[tokio::test]
    async fn kek_init_file_backend_writes_kek_and_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        kek_init(args(dir.path(), "file", true))
            .await
            .expect("file kek-init must succeed");
        // Marker exists and points at file.
        assert_eq!(
            read_backend_marker(dir.path()).unwrap(),
            Some(KekBackendKind::File)
        );
        // node_kek file exists and is KEK_SIZE bytes.
        let kek_path = dir.path().join("node_kek");
        let bytes = std::fs::read(&kek_path).expect("read node_kek");
        assert_eq!(bytes.len(), KEK_SIZE);
        // Second invocation must refuse (marker now present).
        let err = kek_init(args(dir.path(), "file", true))
            .await
            .expect_err("second kek-init must refuse");
        assert!(err.to_string().contains("already provisioned"));
    }

    #[tokio::test]
    async fn kek_init_systemd_writes_marker_only_without_storing_kek() {
        let dir = tempfile::tempdir().expect("tempdir");
        kek_init(args(dir.path(), "systemd", true))
            .await
            .expect("systemd marker-only opt-in must succeed");
        assert_eq!(
            read_backend_marker(dir.path()).unwrap(),
            Some(KekBackendKind::Systemd)
        );
        // node_kek MUST NOT have been written — systemd credentials
        // are populated by the service manager.
        assert!(
            !dir.path().join("node_kek").exists(),
            "systemd opt-in must not write a file-backend KEK"
        );
    }

    fn migrate_args(dir: &std::path::Path, to: &str, yes: bool) -> KekMigrateArgs {
        KekMigrateArgs {
            secrets_dir: dir.to_path_buf(),
            to: to.to_string(),
            yes,
        }
    }

    #[tokio::test]
    async fn kek_migrate_refuses_when_marker_missing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = kek_migrate(migrate_args(dir.path(), "file", true))
            .await
            .expect_err("must refuse when marker is absent");
        let msg = err.to_string();
        assert!(
            msg.contains("no KEK provisioned"),
            "expected no-KEK-provisioned error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn kek_migrate_short_circuits_when_current_equals_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Seed marker only; do NOT write node_kek. The short-circuit
        // must trip BEFORE touching either backend.
        write_backend_marker(dir.path(), KekBackendKind::File).expect("seed marker");
        kek_migrate(migrate_args(dir.path(), "file", true))
            .await
            .expect("idempotent migrate must succeed");
        // Marker unchanged. No node_kek created.
        assert_eq!(
            read_backend_marker(dir.path()).unwrap(),
            Some(KekBackendKind::File)
        );
        assert!(
            !dir.path().join("node_kek").exists(),
            "idempotent migrate must not touch backends"
        );
    }

    #[tokio::test]
    async fn kek_migrate_without_yes_aborts() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Seed a real file-backend KEK so the abort happens at the
        // --yes check, not earlier.
        kek_init(args(dir.path(), "file", true))
            .await
            .expect("seed kek-init");
        let err = kek_migrate(migrate_args(dir.path(), "systemd", false))
            .await
            .expect_err("must abort without --yes");
        assert!(err.to_string().contains("aborted"));
        // Marker still file; node_kek still present.
        assert_eq!(
            read_backend_marker(dir.path()).unwrap(),
            Some(KekBackendKind::File)
        );
        assert!(dir.path().join("node_kek").exists());
    }

    #[tokio::test]
    async fn kek_migrate_rejects_unknown_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_backend_marker(dir.path(), KekBackendKind::File).expect("seed marker");
        let err = kek_migrate(migrate_args(dir.path(), "garbage", true))
            .await
            .expect_err("must reject unknown backend");
        assert!(
            err.to_string().contains("unrecognized backend"),
            "expected unrecognized-backend error, got: {err}"
        );
    }

    fn status_args(dir: &std::path::Path) -> KekStatusArgs {
        KekStatusArgs {
            secrets_dir: dir.to_path_buf(),
        }
    }

    fn rotate_args(dir: &std::path::Path, yes: bool) -> KekRotateArgs {
        KekRotateArgs {
            secrets_dir: dir.to_path_buf(),
            yes,
        }
    }

    #[tokio::test]
    async fn kek_status_succeeds_when_marker_absent() {
        // First-start flow: no marker, no KEK. `kek-status` must Ok
        // with the not-provisioned message, NOT bail.
        let dir = tempfile::tempdir().expect("tempdir");
        kek_status(status_args(dir.path()))
            .await
            .expect("kek-status with no marker must succeed");
    }

    #[tokio::test]
    async fn kek_status_succeeds_after_kek_init() {
        // Happy path: marker present + KEK loadable → fingerprint
        // computed without error.
        let dir = tempfile::tempdir().expect("tempdir");
        kek_init(args(dir.path(), "file", true))
            .await
            .expect("seed kek-init");
        kek_status(status_args(dir.path()))
            .await
            .expect("kek-status after init must succeed");
    }

    #[tokio::test]
    async fn kek_status_fails_when_marker_points_at_missing_kek() {
        // Path 3: marker says file, but node_kek is absent.
        // `load_from_backend` returns `KekError::NoBackend`, which
        // kek_status must propagate as an error (not panic, not silently
        // succeed). Regression guard for the TOCTOU-panic fix (B4).
        let dir = tempfile::tempdir().expect("tempdir");
        write_backend_marker(dir.path(), KekBackendKind::File).expect("seed marker");
        let err = kek_status(status_args(dir.path()))
            .await
            .expect_err("must error when marker points at empty backend");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to load KEK"),
            "expected load-failure error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn kek_rotate_always_bails_with_not_implemented() {
        // kek_rotate is unconditionally not-implemented in this build.
        // Pin that with --yes set — catches accidental gating on --yes
        // (the bail must surface BEFORE the --yes check).
        let dir = tempfile::tempdir().expect("tempdir");
        let err = kek_rotate(rotate_args(dir.path(), true))
            .await
            .expect_err("kek-rotate must always error");
        assert!(
            err.to_string().contains("not yet implemented"),
            "expected not-implemented bail, got: {err}"
        );
    }

    #[tokio::test]
    async fn kek_migrate_fails_when_source_backend_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Marker says file, but no node_kek exists. The load() must
        // return Ok(None) (not panic), and kek_migrate must surface a
        // clear error rather than writing garbage to the target.
        write_backend_marker(dir.path(), KekBackendKind::File).expect("seed marker");
        let err = kek_migrate(migrate_args(dir.path(), "systemd", true))
            .await
            .expect_err("must fail when source backend has no KEK");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("source backend reports no KEK present")
                || msg.contains("failed to load KEK from source backend"),
            "expected source-empty error, got: {msg}"
        );
        // Marker still file — must NOT have been rewritten on failure.
        assert_eq!(
            read_backend_marker(dir.path()).unwrap(),
            Some(KekBackendKind::File)
        );
    }

    // ----- snapshot-list / snapshot-restore -----

    /// Lay out `<secrets_dir>/<delegate>/<secret>` (active) and a single
    /// `<delegate>/.snapshots/<secret>/<stamp:020>` snapshot.
    fn seed_snapshot(
        secrets_dir: &std::path::Path,
        delegate: &str,
        secret: &str,
        active: &[u8],
        stamp: u64,
        snapshot: &[u8],
    ) {
        let delegate_dir = secrets_dir.join(delegate);
        std::fs::create_dir_all(&delegate_dir).unwrap();
        std::fs::write(delegate_dir.join(secret), active).unwrap();
        let snap_dir = delegate_dir.join(".snapshots").join(secret);
        std::fs::create_dir_all(&snap_dir).unwrap();
        std::fs::write(snap_dir.join(format!("{stamp:020}")), snapshot).unwrap();
    }

    fn list_args(
        dir: &std::path::Path,
        delegate: Option<&str>,
        secret: Option<&str>,
    ) -> SnapshotListArgs {
        SnapshotListArgs {
            secrets_dir: dir.to_path_buf(),
            delegate: delegate.map(str::to_string),
            secret: secret.map(str::to_string),
        }
    }

    fn restore_args(
        dir: &std::path::Path,
        delegate: &str,
        secret: &str,
        timestamp_ms: u64,
        yes: bool,
    ) -> SnapshotRestoreArgs {
        SnapshotRestoreArgs {
            secrets_dir: dir.to_path_buf(),
            delegate: delegate.to_string(),
            secret: secret.to_string(),
            timestamp_ms,
            suffix: None,
            yes,
        }
    }

    #[tokio::test]
    async fn snapshot_list_empty_dir_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        snapshot_list(list_args(dir.path(), None, None))
            .await
            .expect("list on empty dir must succeed");
    }

    #[tokio::test]
    async fn snapshot_list_missing_dir_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("nope");
        let err = snapshot_list(list_args(&missing, None, None))
            .await
            .expect_err("missing secrets dir must error");
        assert!(err.to_string().contains("does not exist"));
    }

    #[tokio::test]
    async fn snapshot_list_summary_and_detail_views_succeed() {
        let dir = tempfile::tempdir().expect("tempdir");
        seed_snapshot(dir.path(), "delegateA", "secretX", b"cur", 7, b"old");
        // Summary (all delegates) and single-delegate views.
        snapshot_list(list_args(dir.path(), None, None))
            .await
            .expect("summary list ok");
        snapshot_list(list_args(dir.path(), Some("delegateA"), None))
            .await
            .expect("single-delegate list ok");
        // Detailed single-secret view.
        snapshot_list(list_args(dir.path(), Some("delegateA"), Some("secretX")))
            .await
            .expect("detail list ok");
    }

    #[tokio::test]
    async fn snapshot_list_unknown_delegate_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = snapshot_list(list_args(dir.path(), Some("nope"), None))
            .await
            .expect_err("unknown delegate must error");
        assert!(err.to_string().contains("no delegate directory"));
    }

    #[tokio::test]
    async fn snapshot_restore_without_yes_aborts_and_preserves_active() {
        let dir = tempfile::tempdir().expect("tempdir");
        seed_snapshot(dir.path(), "D", "S", b"current", 42, b"old");
        let err = snapshot_restore(restore_args(dir.path(), "D", "S", 42, false))
            .await
            .expect_err("must abort without --yes");
        assert!(err.to_string().contains("aborted"));
        // Active value untouched.
        assert_eq!(
            std::fs::read(dir.path().join("D").join("S")).unwrap(),
            b"current"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_unknown_secret_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = snapshot_restore(restore_args(dir.path(), "D", "missing", 1, true))
            .await
            .expect_err("must error when no snapshot history");
        assert!(err.to_string().contains("no snapshot history"));
    }

    #[tokio::test]
    async fn snapshot_restore_round_trip_restores_prior_value() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Recent timestamp so the seeded snapshot survives the default
        // retention's 2-year max_age cap during the restore's thin pass.
        let stamp = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64)
            - 60_000;
        seed_snapshot(dir.path(), "D", "S", b"current", stamp, b"old-value");
        snapshot_restore(restore_args(dir.path(), "D", "S", stamp, true))
            .await
            .expect("restore must succeed");
        assert_eq!(
            std::fs::read(dir.path().join("D").join("S")).unwrap(),
            b"old-value"
        );
        // Reversible: the prior "current" value was snapshotted, so the
        // history now has at least 2 entries.
        let snap_dir = dir.path().join("D").join(".snapshots").join("S");
        let count = std::fs::read_dir(&snap_dir).unwrap().count();
        assert!(count >= 2, "expected reversibility snapshot, got {count}");
    }

    #[tokio::test]
    async fn snapshot_restore_unknown_timestamp_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        seed_snapshot(dir.path(), "D", "S", b"current", 100, b"old");
        let err = snapshot_restore(restore_args(dir.path(), "D", "S", 999, true))
            .await
            .expect_err("must error on unknown timestamp");
        assert!(err.to_string().contains("no snapshot at timestamp_ms=999"));
        // Active untouched on the error path.
        assert_eq!(
            std::fs::read(dir.path().join("D").join("S")).unwrap(),
            b"current"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_rejects_path_traversal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = snapshot_restore(restore_args(dir.path(), "../../etc", "S", 1, true))
            .await
            .expect_err("traversal in --delegate must be rejected");
        assert!(err.to_string().contains("invalid delegate"));
        let err = snapshot_restore(restore_args(dir.path(), "D", "../../etc/passwd", 1, true))
            .await
            .expect_err("traversal in --secret must be rejected");
        assert!(err.to_string().contains("invalid secret"));
    }

    #[tokio::test]
    async fn snapshot_list_rejects_path_traversal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = snapshot_list(list_args(dir.path(), Some("../x"), None))
            .await
            .expect_err("traversal in --delegate must be rejected");
        assert!(err.to_string().contains("invalid delegate"));
        let err = snapshot_list(list_args(dir.path(), Some("D"), Some("a/b")))
            .await
            .expect_err("separator in --secret must be rejected");
        assert!(err.to_string().contains("invalid secret"));
    }

    #[tokio::test]
    async fn snapshot_list_detail_empty_history_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Delegate dir + active secret present, but no `.snapshots/` yet:
        // the detail view's "(no snapshot history)" branch.
        let delegate_dir = dir.path().join("D");
        std::fs::create_dir_all(&delegate_dir).unwrap();
        std::fs::write(delegate_dir.join("S"), b"v").unwrap();
        snapshot_list(list_args(dir.path(), Some("D"), Some("S")))
            .await
            .expect("detail view with no history must succeed");
    }

    #[test]
    fn snapshot_list_secret_requires_delegate_at_parse() {
        use clap::Parser;
        // `--secret` without `--delegate` must fail clap parsing — pins the
        // `requires = "delegate"` invariant the `.expect()` in the detail
        // view depends on.
        let res = SnapshotListArgs::try_parse_from([
            "snapshot-list",
            "--secrets-dir",
            "/tmp/x",
            "--secret",
            "y",
        ]);
        assert!(
            res.is_err(),
            "--secret without --delegate must fail to parse"
        );
    }

    #[tokio::test]
    async fn snapshot_restore_suffix_targets_collision_entry() {
        // Same-millisecond collisions produce unsuffixed + `.0` + `.1`
        // rows in `snapshot-list`; `--suffix` must be able to target each.
        let dir = tempfile::tempdir().expect("tempdir");
        let secrets_dir = dir.path();
        let snap_dir = secrets_dir.join("D").join(".snapshots").join("S");
        std::fs::create_dir_all(&snap_dir).unwrap();
        let base = format!("{:020}", 1000u64);
        std::fs::write(snap_dir.join(&base), b"unsuffixed").unwrap();
        std::fs::write(snap_dir.join(format!("{base}.0")), b"suffix-zero").unwrap();
        std::fs::write(snap_dir.join(format!("{base}.1")), b"suffix-one").unwrap();
        std::fs::create_dir_all(secrets_dir.join("D")).unwrap();
        std::fs::write(secrets_dir.join("D").join("S"), b"current").unwrap();

        // A non-existent suffix is rejected (not silently downgraded), and
        // the error names the suffix. Run first so it can't see mutated state.
        let mut args = restore_args(secrets_dir, "D", "S", 1000, true);
        args.suffix = Some(9);
        let err = snapshot_restore(args)
            .await
            .expect_err("missing suffix must error");
        assert!(err.to_string().contains("suffix=9"));
        assert_eq!(
            std::fs::read(secrets_dir.join("D").join("S")).unwrap(),
            b"current"
        );

        // Explicit suffix targets the exact collision row.
        let mut args = restore_args(secrets_dir, "D", "S", 1000, true);
        args.suffix = Some(1);
        snapshot_restore(args)
            .await
            .expect("restore .1 must succeed");
        assert_eq!(
            std::fs::read(secrets_dir.join("D").join("S")).unwrap(),
            b"suffix-one"
        );
    }

    // ----- export / import CLI handlers -----

    use freenet::dev_tool::SecretsStore as TestStore;
    use freenet_stdlib::prelude::{Delegate, SecretsId};
    use zeroize::Zeroizing as Zng;

    /// Seed a Local-scope secret into a fresh node layout and return the
    /// `(secrets_dir, db_dir)` paths. The seeding store (and its exclusive
    /// ReDb lock) is dropped before returning so the CLI handler can re-open.
    async fn seed_local_secret(
        root: &std::path::Path,
        delegate_code: u8,
        secret_id: &[u8],
        plaintext: &[u8],
    ) -> (PathBuf, PathBuf) {
        let secrets_dir = root.join("secrets");
        let db_dir = root.join("data");
        std::fs::create_dir_all(&secrets_dir).unwrap();
        std::fs::create_dir_all(&db_dir).unwrap();
        {
            let db = Storage::new(&db_dir).await.unwrap();
            let secrets = Secrets::load_for_secrets_dir(&secrets_dir).unwrap();
            let mut store = TestStore::new(secrets_dir.clone(), secrets, db).unwrap();
            let d = Delegate::from((&vec![delegate_code].into(), &vec![].into()));
            let s = SecretsId::new(secret_id.to_vec());
            store
                .store_secret(
                    d.key(),
                    &s,
                    SecretScope::Local,
                    Zng::new(plaintext.to_vec()),
                )
                .unwrap();
        }
        (secrets_dir, db_dir)
    }

    fn export_args(
        secrets_dir: &std::path::Path,
        db_dir: &std::path::Path,
        out: &std::path::Path,
        passphrase: &str,
    ) -> ExportArgs {
        ExportArgs {
            secrets_dir: secrets_dir.to_path_buf(),
            db_dir: db_dir.to_path_buf(),
            local: true,
            user_token: None,
            passphrase: Some(passphrase.to_string()),
            use_token_key: false,
            out: out.to_path_buf(),
        }
    }

    fn import_args(
        bundle: &std::path::Path,
        secrets_dir: &std::path::Path,
        db_dir: &std::path::Path,
        passphrase: &str,
    ) -> ImportArgs {
        ImportArgs {
            bundle: bundle.to_path_buf(),
            secrets_dir: secrets_dir.to_path_buf(),
            db_dir: db_dir.to_path_buf(),
            passphrase: Some(passphrase.to_string()),
            use_token_key: false,
            token: None,
            local: true,
            into_user: None,
            overwrite: false,
        }
    }

    #[tokio::test]
    async fn cli_export_then_import_round_trip() {
        let src = tempfile::tempdir().unwrap();
        let (secrets_dir, db_dir) =
            seed_local_secret(src.path(), 1, b"cli-secret", b"cli-plaintext").await;
        let bundle_path = src.path().join("bundle.bin");

        secrets_export(export_args(&secrets_dir, &db_dir, &bundle_path, "pw"))
            .await
            .expect("export must succeed");
        assert!(bundle_path.exists(), "bundle file written");
        // Encrypted at rest: the file bytes don't contain the known plaintext.
        let bytes = std::fs::read(&bundle_path).unwrap();
        assert!(
            !bytes
                .windows(b"cli-plaintext".len())
                .any(|w| w == b"cli-plaintext"),
            "bundle must not contain plaintext at rest"
        );

        // Import into a fresh node.
        let dst = tempfile::tempdir().unwrap();
        let dst_secrets = dst.path().join("secrets");
        let dst_db = dst.path().join("data");
        std::fs::create_dir_all(&dst_secrets).unwrap();
        std::fs::create_dir_all(&dst_db).unwrap();
        secrets_import(import_args(&bundle_path, &dst_secrets, &dst_db, "pw"))
            .await
            .expect("import must succeed");

        // Verify by reopening the destination store and reading the secret.
        let db = Storage::new(&dst_db).await.unwrap();
        let secrets = Secrets::load_for_secrets_dir(&dst_secrets).unwrap();
        let store = TestStore::new(dst_secrets.clone(), secrets, db).unwrap();
        let d = Delegate::from((&vec![1u8].into(), &vec![].into()));
        let s = SecretsId::new(b"cli-secret".to_vec());
        let got = store
            .get_secret(d.key(), &s, SecretScope::Local)
            .expect("imported secret present");
        assert_eq!(got.to_vec(), b"cli-plaintext");
    }

    #[tokio::test]
    async fn cli_export_refuses_to_overwrite_out() {
        let src = tempfile::tempdir().unwrap();
        let (secrets_dir, db_dir) = seed_local_secret(src.path(), 2, b"s", b"p").await;
        let out = src.path().join("exists.bin");
        std::fs::write(&out, b"prior").unwrap();
        let err = secrets_export(export_args(&secrets_dir, &db_dir, &out, "pw"))
            .await
            .expect_err("must refuse to overwrite an existing --out");
        assert!(
            err.to_string().contains("refusing to overwrite"),
            "got: {err}"
        );
        // Prior file untouched.
        assert_eq!(std::fs::read(&out).unwrap(), b"prior");
    }

    #[tokio::test]
    async fn cli_import_wrong_passphrase_fails_no_write() {
        let src = tempfile::tempdir().unwrap();
        let (secrets_dir, db_dir) = seed_local_secret(src.path(), 3, b"s", b"p").await;
        let bundle_path = src.path().join("b.bin");
        secrets_export(export_args(&secrets_dir, &db_dir, &bundle_path, "right"))
            .await
            .expect("export");

        let dst = tempfile::tempdir().unwrap();
        let dst_secrets = dst.path().join("secrets");
        let dst_db = dst.path().join("data");
        std::fs::create_dir_all(&dst_secrets).unwrap();
        std::fs::create_dir_all(&dst_db).unwrap();
        let err = secrets_import(import_args(&bundle_path, &dst_secrets, &dst_db, "wrong"))
            .await
            .expect_err("wrong passphrase must fail");
        assert!(
            err.to_string().contains("authentication failed"),
            "got: {err}"
        );

        // Destination store has no secrets.
        let db = Storage::new(&dst_db).await.unwrap();
        let secrets = Secrets::load_for_secrets_dir(&dst_secrets).unwrap();
        let store = TestStore::new(dst_secrets.clone(), secrets, db).unwrap();
        assert!(
            store
                .export_scope_entries(SecretScope::Local)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cli_export_requires_scope_and_key() {
        let src = tempfile::tempdir().unwrap();
        let (secrets_dir, db_dir) = seed_local_secret(src.path(), 4, b"s", b"p").await;
        let out = src.path().join("o.bin");
        // No scope selected.
        let mut a = export_args(&secrets_dir, &db_dir, &out, "pw");
        a.local = false;
        let err = secrets_export(a).await.expect_err("no scope must error");
        assert!(err.to_string().contains("source scope"), "got: {err}");

        // No passphrase source: the flag is None, FREENET_SECRET_PASSPHRASE is
        // unset, and the test runner's stdin is not a TTY — so passphrase
        // resolution must error clearly (the default key method now resolves
        // from env/flag/prompt rather than requiring the flag).
        // SAFETY: test-scoped env mutation; nextest per-process isolation.
        unsafe {
            std::env::remove_var(ENV_PASSPHRASE);
        }
        let mut a = export_args(&secrets_dir, &db_dir, &out, "pw");
        a.passphrase = None;
        let err = secrets_export(a)
            .await
            .expect_err("no passphrase source must error");
        assert!(
            err.to_string().contains("no passphrase provided"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn cli_export_refuses_missing_db() {
        // A --db-dir that exists but lacks the node's `db` file must NOT be
        // silently opened as a fresh empty index (which would emit an empty
        // bundle). Export must error clearly instead.
        let src = tempfile::tempdir().unwrap();
        let secrets_dir = src.path().join("secrets");
        let db_dir = src.path().join("data"); // exists, but no `db` file inside
        std::fs::create_dir_all(&secrets_dir).unwrap();
        std::fs::create_dir_all(&db_dir).unwrap();
        let out = src.path().join("o.bin");

        let err = secrets_export(export_args(&secrets_dir, &db_dir, &out, "pw"))
            .await
            .expect_err("missing db must error");
        assert!(
            err.to_string().contains("no node database found"),
            "got: {err}"
        );
        // No bundle file was written.
        assert!(
            !out.exists(),
            "no bundle should be written on the error path"
        );
    }

    // ----- resolve_secret -----

    #[test]
    fn resolve_secret_prefers_env_over_flag() {
        // Use a unique env var name so this test can't race other tests that
        // read the production FREENET_* vars.
        let env_name = "FREENET_TEST_RESOLVE_PREF";
        // SAFETY: a test-private env var name not read elsewhere; set then
        // removed within this test. nextest runs each test in its own process.
        unsafe {
            std::env::set_var(env_name, "from-env");
        }
        let got = resolve_secret(Some("from-flag"), Some(env_name), "thing", false)
            .expect("env source must resolve");
        assert_eq!(&*got, "from-env", "env must win over the flag");
        // SAFETY: test-private env var name removed within this test; nextest
        // runs each test in its own process.
        unsafe {
            std::env::remove_var(env_name);
        }
    }

    #[test]
    fn resolve_secret_falls_back_to_flag() {
        // Env var unset (and unique to this test) → the flag value is used.
        let env_name = "FREENET_TEST_RESOLVE_FLAG";
        // SAFETY: as above.
        unsafe {
            std::env::remove_var(env_name);
        }
        let got = resolve_secret(Some("flag-value"), Some(env_name), "thing", false)
            .expect("flag source must resolve");
        assert_eq!(&*got, "flag-value");
    }

    #[test]
    fn resolve_secret_errors_when_no_source_in_non_interactive() {
        // No env, no flag. Under `cargo test`/nextest stdin is not a TTY, so the
        // prompt branch is skipped and this must error clearly (not hang).
        let env_name = "FREENET_TEST_RESOLVE_MISSING";
        // SAFETY: as above.
        unsafe {
            std::env::remove_var(env_name);
        }
        let err = resolve_secret(None, Some(env_name), "passphrase", true)
            .expect_err("no source in non-interactive mode must error");
        let msg = err.to_string();
        assert!(
            msg.contains("no passphrase provided") && msg.contains(env_name),
            "error should name the secret and the env var, got: {msg}"
        );
    }
}
