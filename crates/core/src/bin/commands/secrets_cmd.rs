//! `freenet secrets` subcommand: manage the node KEK backend.
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
use chacha20poly1305::aead::{OsRng, rand_core::RngCore};
use freenet::dev_tool::{
    KEK_SIZE, KekBackendKind, build_backend_for, load_from_backend, read_backend_marker,
    replace_backend_marker, write_backend_marker,
};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[derive(clap::Parser, Clone, Debug)]
pub struct SecretsCliConfig {
    #[clap(subcommand)]
    pub command: SecretsCommand,
}

#[derive(clap::Subcommand, Clone, Debug)]
#[allow(
    clippy::enum_variant_names,
    reason = "every variant manages the KEK and the `Kek` prefix matches the user-facing \
              CLI verbs (`freenet secrets kek-status`, `freenet secrets kek-rotate`, \
              `freenet secrets kek-migrate`)."
)]
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

pub async fn run(config: SecretsCliConfig) -> Result<()> {
    match config.command {
        SecretsCommand::KekStatus(args) => kek_status(args).await,
        SecretsCommand::KekInit(args) => kek_init(args).await,
        SecretsCommand::KekRotate(args) => kek_rotate(args).await,
        SecretsCommand::KekMigrate(args) => kek_migrate(args).await,
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
}
