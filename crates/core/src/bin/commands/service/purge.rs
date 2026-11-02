use anyhow::{Context, Result};
use directories::ProjectDirs;
use freenet::tracing::tracer::get_log_dir;
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use super::linux::home_dir_for_user;
use super::wrapper::log_wrapper_event;

/// Per-platform directory the wrapper writes its lifecycle log to. Mirrors the
/// path `service_logs` tails, so `doctor`'s reap events land in the same place a
/// user would already be looking.
pub(super) fn doctor_log_dir() -> PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "macos")]
    {
        home.join("Library/Logs/freenet")
    }
    #[cfg(not(target_os = "macos"))]
    {
        home.join(".local/state/freenet")
    }
}

/// Reap stale `freenet network` processes, escalating to SIGKILL for any that
/// ignore the initial SIGTERM.
///
/// `kill_stale_freenet_processes` (the wrapper-startup reaper) sends a single
/// `pkill` (SIGTERM) and returns. That is right for the startup path — the
/// wrapper is about to relaunch and a lingering drainer will exit on its own —
/// but `doctor` is invoked precisely because a node is WEDGED, and the original
/// incident's orphan ignored SIGTERM for >11s (see `heal_stale_orphan_or_defer`
/// in `generate_wrapper_script`). So here we SIGTERM, wait, then SIGKILL the
/// holdouts. Returns the number of processes that were still matching after the
/// SIGTERM pass (i.e. needed escalation), for the summary output.
#[cfg(unix)]
fn reap_stale_freenet_processes_escalating(log_dir: &Path) -> usize {
    let uid = std::process::Command::new("id")
        .arg("-u")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();
    let escalated = kill_pattern_escalating(uid.trim(), "freenet network", 12);
    if escalated > 0 {
        log_wrapper_event(
            log_dir,
            "doctor: stale freenet network process(es) ignored SIGTERM, sent SIGKILL",
        );
    } else {
        log_wrapper_event(log_dir, "doctor: reaped stale freenet network process(es)");
    }
    escalated
}

/// SIGTERM a user-scoped process set matched by `pattern` (a `pgrep -f`
/// substring), wait up to `grace_secs` for graceful exit polling once a second,
/// then SIGKILL any survivor. Returns the count of survivors that needed the
/// SIGKILL escalation.
///
/// Parameterized on `pattern` (not hardcoded to "freenet network") purely so the
/// escalation timing/decision is unit-testable against a controllable child;
/// production callers always pass "freenet network".
#[cfg(unix)]
pub(super) fn kill_pattern_escalating(uid: &str, pattern: &str, grace_secs: usize) -> usize {
    use std::time::Duration;

    // SIGTERM pass. Ignore the result: a non-match (nothing to kill) is the
    // common, healthy case, and we re-check liveness via pgrep below regardless.
    std::process::Command::new("pkill")
        .args(["-f", "-u", uid, pattern])
        .status()
        .ok();

    // Wait for graceful exit, escalating as soon as the survivors are quiet.
    for _ in 0..grace_secs {
        std::thread::sleep(Duration::from_secs(1));
        if pids_matching(uid, pattern).is_empty() {
            return 0;
        }
    }

    // SIGKILL any survivor.
    let survivors = pids_matching(uid, pattern).len();
    if survivors > 0 {
        std::process::Command::new("pkill")
            .args(["-9", "-f", "-u", uid, pattern])
            .status()
            .ok();
        std::thread::sleep(Duration::from_secs(1));
    }
    survivors
}

/// PIDs of user-scoped processes whose command line matches `pattern`
/// (`pgrep -f`). Empty on any error.
#[cfg(unix)]
pub(super) fn pids_matching(uid: &str, pattern: &str) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .args(["-f", "-u", uid, pattern])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| {
            s.split_whitespace()
                .filter_map(|p| p.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Recover a wedged service install (issue #3967). Orchestrates the manual
/// recovery that `restart` cannot do:
///
///   1. Re-template the wrapper/unit to the CURRENT binary via the install
///      path. This closes the bootstrap gap: the self-heal wrapper (#4408) can
///      only reach disk through `freenet update`, which a node wedged on a
///      stale orphan never runs — so an already-wedged install never adopts the
///      fix on its own. Idempotent on a healthy install.
///   2. Stop the managed service so launchd/systemd releases its child cleanly.
///   3. Reap stale orphaned `freenet network` processes (PPID=1, holding the
///      port on an old binary), escalating SIGTERM -> SIGKILL for holdouts.
///   4. Start the service so the freshly-templated wrapper launches on a clean
///      port.
///
/// Cross-platform: it calls the cfg-gated install/stop/start helpers and the
/// unix reaper, so it compiles on every target.
pub(super) fn service_doctor(system: bool) -> Result<()> {
    println!("freenet service doctor: recovering service install...");

    // 1. Re-template wrapper/unit to the current binary.
    //    no_linger=false: preserve the default linger behaviour when
    //    re-templating a user service so a recovered headless install keeps
    //    auto-updating across logout (linger enabling is idempotent).
    println!("  - Re-templating service wrapper/unit to the current binary...");
    super::install_service(system, false)?;

    // 2. Stop the managed service (best-effort: a wedged install may show the
    //    agent as not-running, in which case stop is a no-op we don't want to
    //    abort on).
    println!("  - Stopping the managed service...");
    if let Err(e) = super::stop_service(system) {
        println!("    (stop reported: {e} — continuing; the service may already be down)");
    }

    // 3. Reap stale orphans (the step `restart` lacks).
    #[cfg(unix)]
    {
        let log_dir = doctor_log_dir();
        println!("  - Reaping stale 'freenet network' processes...");
        let escalated = reap_stale_freenet_processes_escalating(&log_dir);
        if escalated > 0 {
            println!("    ({escalated} process(es) ignored SIGTERM and were force-killed)");
        }
    }
    #[cfg(target_os = "windows")]
    {
        println!("  - Reaping stale 'freenet network' processes...");
        super::wrapper::kill_stale_freenet_processes(&doctor_log_dir());
    }

    // 4. Start fresh so the new wrapper text loads on a clean port.
    println!("  - Starting the service...");
    super::start_service(system)?;

    println!("freenet service doctor: done. The service has been re-templated and restarted.");
    println!(
        "If the dashboard still shows an old version, hard-refresh the page to clear cached assets."
    );
    Ok(())
}

/// Determine whether the user wants to purge data directories.
///
/// - `--purge` → true
/// - `--keep-data` → false
/// - Neither → prompt interactively (defaults to false if stdin is not a TTY)
pub fn should_purge(purge: bool, keep_data: bool) -> Result<bool> {
    if purge {
        return Ok(true);
    }
    if keep_data {
        return Ok(false);
    }

    use std::io::{self, BufRead, IsTerminal, Write};

    // Check if stdin is a TTY for interactive prompting
    if io::stdin().is_terminal() {
        print!("Also remove all Freenet data, config, and logs? [y/N] ");
        io::stdout().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        let answer = line.trim().to_ascii_lowercase();
        Ok(answer == "y" || answer == "yes")
    } else {
        println!(
            "Non-interactive mode: keeping data. Use --purge to also remove data, config, and logs."
        );
        Ok(false)
    }
}

/// Remove a directory if it exists, printing what is being removed.
pub(super) fn remove_if_exists(label: &str, path: &Path) -> Result<()> {
    if path.exists() {
        println!("Removing {label}: {}", path.display());
        std::fs::remove_dir_all(path)
            .with_context(|| format!("Failed to remove {label} directory: {}", path.display()))?;
    }
    Ok(())
}

/// Re-export for callers outside this module (e.g. `uninstall::run`) that
/// also need to collapse empty parent folders after their own cleanup
/// passes. Runtime use is Windows-only (see `uninstall::collapse_windows_bin_tree`)
/// but the function is exposed on every platform so that tests for the
/// caller-side helper can run under Linux CI too.
pub fn remove_dir_if_empty_pub(path: &Path) {
    remove_dir_if_empty(path)
}

/// Remove a directory only if it is empty. Unlike `remove_if_exists`, this
/// never recursively deletes — it is intended for cleaning up empty parent
/// folders after their children have been removed (e.g. collapsing an empty
/// `%APPDATA%\The Freenet Project Inc\Freenet\` once its `config` subfolder
/// is gone). Any error other than "not empty" is swallowed; the parent is
/// expendable and we should not fail the uninstall over it.
pub(super) fn remove_dir_if_empty(path: &Path) {
    if !path.is_dir() {
        return;
    }
    let Ok(mut entries) = std::fs::read_dir(path) else {
        return;
    };
    if entries.next().is_some() {
        return;
    }
    match std::fs::remove_dir(path) {
        Ok(()) => println!("Removing empty dir: {}", path.display()),
        Err(err) => {
            // Best-effort: the parent may have been re-populated by a
            // concurrent process, or the user lacks permission. Either
            // way, don't abort the uninstall.
            eprintln!("Note: could not remove empty dir {}: {err}", path.display());
        }
    }
}

/// Public wrapper for use by the `uninstall` command.
pub fn purge_data(system_mode: bool) -> Result<()> {
    purge_data_dirs(system_mode)
}

/// Remove Freenet data, config, cache, and log directories.
///
/// When `system_mode` is true on Linux, resolves directories for the service
/// user (via SUDO_USER) rather than root's home directory.
pub(super) fn purge_data_dirs(#[allow(unused_variables)] system_mode: bool) -> Result<()> {
    // On Linux with --system, the service runs as the SUDO_USER, not root.
    // We need to resolve that user's directories, not root's.
    #[cfg(target_os = "linux")]
    let home_override: Option<std::path::PathBuf> = if system_mode {
        std::env::var("SUDO_USER")
            .ok()
            .map(|u| home_dir_for_user(&u))
    } else {
        None
    };
    #[cfg(not(target_os = "linux"))]
    let home_override: Option<std::path::PathBuf> = None;

    // If we have a home override (system mode on Linux), purge the service
    // user's XDG dirs via the manually-constructed paths. Otherwise use
    // ProjectDirs, which resolves from the current user.
    if let Some(ref home) = home_override {
        for (label, dir) in linux_system_purge_dirs(home) {
            remove_if_exists(label, &dir)?;
        }
    } else {
        let leaves = DataLeaves::from_project_dirs();
        purge_leaves_and_collapse(&leaves)?;
    }

    Ok(())
}

/// XDG leaf directories that `--system` mode must purge for the service user.
///
/// These MUST match the paths the running node actually creates. On Linux the
/// `directories` crate lowercases the application name when building
/// `ProjectDirs` (`ProjectDirs::from("", "The Freenet Project Inc", "Freenet")`
/// yields `~/.local/share/freenet`, not `~/.local/share/Freenet`), and
/// `get_log_dir` uses `~/.local/state/freenet`. The previous hardcoded
/// uppercase `Freenet` paths matched nothing on disk, so
/// `sudo freenet uninstall --purge --system` reported success while silently
/// leaving all of the user's contracts, delegates, and database behind (#3907).
///
/// Extracted as a pure function (parameterised on `home`) so the path logic is
/// unit-testable without mutating process-level `SUDO_USER`/home state. This is
/// only reached in `--system` mode, which only resolves a `home_override` on
/// Linux; the `cfg_attr` suppresses the dead-code lint on macOS/Windows, where
/// `home_override` is always `None` so the function is never called.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) fn linux_system_purge_dirs(home: &Path) -> [(&'static str, PathBuf); 4] {
    [
        ("data", home.join(".local/share/freenet")),
        ("config", home.join(".config/freenet")),
        ("cache", home.join(".cache/freenet")),
        ("logs", home.join(".local/state/freenet")),
    ]
}

/// The full set of leaf directories `purge_data_dirs` needs to touch in
/// non-system mode. Grouping these into a value type makes the purge logic
/// testable without mocking `ProjectDirs` (a tricky proposition given it
/// reads process-level env vars).
#[derive(Debug, Default, Clone)]
pub(super) struct DataLeaves {
    /// `data_local_dir` — on Windows this is `%LOCALAPPDATA%\...\data`.
    pub(super) data_local: Option<PathBuf>,
    /// Pre-#3739 Roaming data path, only populated on Windows where it
    /// differs from `data_local`.
    pub(super) data_roaming: Option<PathBuf>,
    /// `config_dir` (Roaming on Windows, e.g.
    /// `%APPDATA%\...\Freenet\config`). Only populated when it differs
    /// from both data paths (matches the macOS case where
    /// `config_dir == data_dir`). On Windows the running node does
    /// not write here — it writes to `config_local` — but several
    /// other call sites (`config.rs:1661` id-set fallback, `report.rs`
    /// config-file scan) still resolve through `config_dir()`, and an
    /// older install may have written to it before the live node
    /// switched to Local AppData. Cleaning both is correct.
    pub(super) config: Option<PathBuf>,
    /// `config_local_dir` (Local on Windows, e.g.
    /// `%LOCALAPPDATA%\...\Freenet\config`). This is what the running
    /// node actually writes to (`Config::build` in config.rs uses
    /// `defaults.config_local_dir()`), which is *not* the same as
    /// `config_dir` on Windows. Without this, `freenet uninstall
    /// --purge` left the live config folder behind. Only populated
    /// when distinct from `data_local`, `data_roaming`, and `config`
    /// to avoid double removal — on Linux/macOS the `directories`
    /// crate aliases `config_local_dir` to `config_dir`, so this
    /// stays `None` and the existing leaves cover those platforms.
    pub(super) config_local: Option<PathBuf>,
    /// `cache_dir` for the uppercase project bundle.
    pub(super) cache: Option<PathBuf>,
    /// Lowercase-variant cache used by the webapp cache on case-sensitive
    /// filesystems.
    pub(super) cache_lowercase: Option<PathBuf>,
    /// Log dir (as returned by `tracing::get_log_dir`).
    pub(super) log: Option<PathBuf>,
    /// Whether the parents of our leaves are Freenet-owned and therefore
    /// safe to collapse if empty.
    ///
    /// True on Windows — `ProjectDirs` there builds
    /// `%LOCALAPPDATA%\The Freenet Project Inc\Freenet\{data,config,cache}`
    /// and `get_log_dir` returns `%LOCALAPPDATA%\freenet\logs`; the
    /// immediate parents (`...\Freenet`, `...\freenet`) exist only for us.
    ///
    /// False on Linux/macOS. On Linux the `directories` crate lowercases the
    /// application name, so `ProjectDirs` builds `~/.local/share/freenet`,
    /// `~/.config/freenet`, `~/.cache/freenet` (note the lowercase `freenet`,
    /// not `Freenet`); macOS uses its own `~/Library/...` scheme. The parents
    /// of those leaves are shared XDG/OS hierarchies used by every other app
    /// on the system — collapsing them would at best no-op (by luck) and at
    /// worst delete an otherwise-empty shared root on a fresh account. The
    /// safety must be enforced at the type level, not left to
    /// `remove_dir_if_empty`'s runtime check.
    pub(super) collapse_parents: bool,
}

impl DataLeaves {
    pub(super) fn from_project_dirs() -> Self {
        let mut leaves = DataLeaves::default();

        if let Some(dirs) = ProjectDirs::from("", "The Freenet Project Inc", "Freenet") {
            let data_dir = dirs.data_local_dir().to_path_buf();
            leaves.data_local = Some(data_dir.clone());

            let roaming = dirs.data_dir().to_path_buf();
            if roaming != data_dir {
                leaves.data_roaming = Some(roaming.clone());
            }

            let config_dir = dirs.config_dir().to_path_buf();
            if config_dir != data_dir && Some(&config_dir) != leaves.data_roaming.as_ref() {
                leaves.config = Some(config_dir);
            }

            let config_local_dir = dirs.config_local_dir().to_path_buf();
            if config_local_dir != data_dir
                && Some(&config_local_dir) != leaves.data_roaming.as_ref()
                && Some(&config_local_dir) != leaves.config.as_ref()
            {
                leaves.config_local = Some(config_local_dir);
            }

            leaves.cache = Some(dirs.cache_dir().to_path_buf());
        } else {
            eprintln!(
                "Warning: Could not determine Freenet directories. Data and config may not have been removed."
            );
        }

        if let Some(dirs) = ProjectDirs::from("", "The Freenet Project Inc", "freenet") {
            let cache_lower = dirs.cache_dir().to_path_buf();
            if cache_lower.exists() {
                leaves.cache_lowercase = Some(cache_lower);
            }
        }

        leaves.log = get_log_dir();
        leaves.collapse_parents = cfg!(target_os = "windows");

        leaves
    }
}

/// Remove every populated leaf directory and then collapse any Freenet-owned
/// parent folder that the removal left empty. This is the #3904 fix: on
/// Windows the per-leaf calls removed `...\Freenet\config` but not its now-
/// empty `...\Freenet\` parent. By collecting parents, deduping, and visiting
/// them deepest-first, we tidy up without ever attempting to remove a parent
/// that still holds a sibling app's data.
pub(super) fn purge_leaves_and_collapse(leaves: &DataLeaves) -> Result<()> {
    let mut parents: Vec<PathBuf> = Vec::new();
    let collect = |leaf: &Path, acc: &mut Vec<PathBuf>| {
        if leaves.collapse_parents {
            push_parent(leaf, acc);
        }
    };

    if let Some(ref data_local) = leaves.data_local {
        remove_if_exists("data", data_local)?;
        collect(data_local, &mut parents);
    }
    if let Some(ref roaming) = leaves.data_roaming {
        remove_if_exists("data (legacy roaming)", roaming)?;
        collect(roaming, &mut parents);
    }
    if let Some(ref config) = leaves.config {
        remove_if_exists("config (legacy roaming)", config)?;
        collect(config, &mut parents);
    }
    if let Some(ref config_local) = leaves.config_local {
        remove_if_exists("config", config_local)?;
        collect(config_local, &mut parents);
    }
    if let Some(ref cache) = leaves.cache {
        remove_if_exists("cache", cache)?;
        collect(cache, &mut parents);
    }
    if let Some(ref cache_lower) = leaves.cache_lowercase {
        remove_if_exists("cache", cache_lower)?;
        collect(cache_lower, &mut parents);
    }
    if let Some(ref log) = leaves.log {
        remove_if_exists("logs", log)?;
        collect(log, &mut parents);
    }

    // Sort ascending, dedup consecutive duplicates, then reverse so that
    // deepest paths are processed first. If a hypothetical future caller
    // ever adds a grandparent alongside its parent, this ordering ensures
    // the grandparent is only evaluated after its child has been collapsed.
    parents.sort();
    parents.dedup();
    for parent in parents.into_iter().rev() {
        remove_dir_if_empty(&parent);
    }

    Ok(())
}

fn push_parent(leaf: &Path, acc: &mut Vec<PathBuf>) {
    if let Some(parent) = leaf.parent() {
        acc.push(parent.to_path_buf());
    }
}
