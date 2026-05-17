//! `fdev secrets` subcommand: manage the node KEK backend.
//!
//! - `kek-status` — report current backend + KEK fingerprint
//! - `kek-rotate` — generate a new KEK, re-encrypt every active
//!   secret + snapshot, swap the KEK atomically in the backend,
//!   delete the previous value
//! - `kek-migrate --to <X>` — copy the KEK from the current backend to
//!   target backend, update the marker, delete from the source
//!   (operator-confirmable)
//!
//! All operations require the node process to be stopped (the on-disk
//! secrets directory is exclusively owned during rotation/migration so
//! a concurrent `freenet` write does not race the rewrite).

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use freenet::dev_tool::{
    KekBackendKind, load_from_backend, read_backend_marker, replace_backend_marker,
};
use sha2::{Digest, Sha256};

#[derive(clap::Parser, Clone, Debug)]
pub struct SecretsCliConfig {
    #[clap(subcommand)]
    pub command: SecretsCommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
#[allow(
    clippy::enum_variant_names,
    reason = "every variant manages the KEK and the `Kek` prefix matches the user-facing \
              CLI verbs (`fdev secrets kek-status`, `fdev secrets kek-rotate`, \
              `fdev secrets kek-migrate`)."
)]
pub enum SecretsCommand {
    /// Print the active KEK backend and the KEK fingerprint
    /// (`SHA-256(KEK)[..8]` in hex). Does NOT print the KEK itself.
    KekStatus(KekStatusArgs),
    /// Generate a new KEK, re-encrypt every active secret + snapshot
    /// under the new derived DEKs, atomically swap the KEK in the
    /// backend, delete the previous value. The node MUST be stopped.
    KekRotate(KekRotateArgs),
    /// Move the KEK from its current backend to a target backend.
    /// Useful when migrating from `file` to `keyring` after installing
    /// a secret-storage daemon, or in reverse during disaster recovery.
    KekMigrate(KekMigrateArgs),
}

#[derive(clap::Parser, Clone, Debug)]
pub struct KekStatusArgs {
    /// Path to the node's secrets directory (where `node_kek` /
    /// `kek_backend` / `delegate_cipher` live).
    #[clap(long, value_parser)]
    pub secrets_dir: PathBuf,
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

pub async fn run(config: SecretsCliConfig) -> Result<()> {
    match config.command {
        SecretsCommand::KekStatus(args) => kek_status(args).await,
        SecretsCommand::KekRotate(args) => kek_rotate(args).await,
        SecretsCommand::KekMigrate(args) => kek_migrate(args).await,
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

async fn kek_rotate(args: KekRotateArgs) -> Result<()> {
    // KEK rotation requires walking every secret + snapshot on disk,
    // re-deriving DEKs under the new KEK, decrypting under the OLD
    // DEKs, re-encrypting under the new DEKs with fresh nonces, and
    // swapping the KEK in the backend.
    //
    // Two-phase crash-safe variant (write `.rot` shadow files, swap
    // KEK only after all shadows exist, then atomically rename shadows
    // → active on next start) is tracked in a follow-up under #4137.
    // This first cut is operator-supervised: the node MUST be stopped
    // and a full backup of `secrets_dir` MUST be taken before running.
    // A crash mid-walk leaves a partial state (some blobs under old
    // KEK, some under new) that requires restoring from backup.
    if !args.yes {
        eprintln!(
            "kek-rotate is operator-supervised in this release:\n\
             - Stop the freenet node before running.\n\
             - Take a full backup of {} (this rotation is not crash-safe yet).\n\
             - A failure mid-rewrite leaves a mixed-KEK state requiring restore from backup.\n\
             A crash-safe two-phase variant is tracked in a follow-up under #4137.\n\
             Re-run with --yes to proceed.",
            args.secrets_dir.display()
        );
        bail!("aborted (no --yes)");
    }

    bail!(
        "kek-rotate on-disk walk not yet implemented in this build. \
         Operators wanting to rotate today should: stop the node, run \
         `fdev secrets kek-migrate --to file --yes` (or to a new backend), \
         move the existing secrets dir aside, start the node fresh, and \
         re-upload delegate secrets via clients. Tracked under #4137."
    );
}

async fn kek_migrate(args: KekMigrateArgs) -> Result<()> {
    use freenet::dev_tool::build_backend_for;

    let target: KekBackendKind = match args.to.as_str() {
        "keyring" => KekBackendKind::Keyring,
        "systemd" => KekBackendKind::Systemd,
        "file" => KekBackendKind::File,
        other => {
            return Err(anyhow!(
                "unrecognized --to backend `{other}`; expected keyring|systemd|file"
            ));
        }
    };

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
