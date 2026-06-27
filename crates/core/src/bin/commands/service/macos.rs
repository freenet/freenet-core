#[cfg(target_os = "macos")]
use anyhow::Context;
#[cfg(target_os = "macos")]
use anyhow::Result;
#[cfg(target_os = "macos")]
use std::path::{Path, PathBuf};

// macOS implementation using launchd
// --system flag is not supported on macOS (launchd daemons need different setup)
#[cfg(target_os = "macos")]
pub(super) fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent: freenet service install"
        );
    }
    install_macos_service()
}

/// Derive the path of the macOS auto-update wrapper script from a home
/// directory. Single source of truth shared by the install path (which
/// writes the script), the update path, and the uninstall path (which
/// removes it). Keep this in sync with `update.rs`'s wrapper derivation.
#[cfg(target_os = "macos")]
pub(super) fn wrapper_script_path(home_dir: &Path) -> PathBuf {
    home_dir.join(".local/bin/freenet-service-wrapper.sh")
}

/// Remove the wrapper script and its sidecar/backup files written by the
/// install (`*.sh`, `*.sh.hash`) and update (`*.sh.bak`) paths.
///
/// Idempotent: a missing file is not an error, so uninstalling twice (or
/// uninstalling an install that never ran an update) succeeds cleanly.
/// Returns an error only if a present file cannot be removed.
///
/// Regression target for #4290: install wrote three files, uninstall
/// removed zero, leaving stale wrapper artifacts in `~/.local/bin`.
#[cfg(target_os = "macos")]
pub(super) fn remove_wrapper_files(wrapper_path: &Path) -> Result<()> {
    use std::fs;

    // `.sh` itself, plus the `.sh.hash` sidecar (#4286) and any `.sh.bak`
    // backup left by `freenet update`. Derived via `with_extension` so they
    // track the wrapper path rather than being independently hardcoded.
    let targets = [
        wrapper_path.to_path_buf(),
        wrapper_path.with_extension("sh.hash"),
        wrapper_path.with_extension("sh.bak"),
    ];

    for target in &targets {
        match fs::remove_file(target) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("Failed to remove wrapper file {}", target.display())
                });
            }
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn install_macos_service() -> Result<()> {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let launch_agents_dir = home_dir.join("Library/LaunchAgents");
    fs::create_dir_all(&launch_agents_dir).context("Failed to create LaunchAgents directory")?;

    // Create log directory in proper macOS location
    let log_dir = home_dir.join("Library/Logs/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    // Create wrapper script for auto-update support.
    // launchd doesn't have ExecStopPost like systemd, so we use a wrapper
    // that checks exit code 42 (update needed) and runs update before restart.
    let wrapper_path = wrapper_script_path(&home_dir);
    let wrapper_dir = wrapper_path
        .parent()
        .context("wrapper path has no parent directory")?;
    fs::create_dir_all(wrapper_dir).context("Failed to create wrapper directory")?;
    let wrapper_content = generate_wrapper_script(&exe_path);
    fs::write(&wrapper_path, &wrapper_content).context("Failed to write wrapper script")?;
    fs::set_permissions(&wrapper_path, fs::Permissions::from_mode(0o755))
        .context("Failed to make wrapper script executable")?;

    // Sidecar records the wrapper's SHA-256 so a later `freenet update`
    // can distinguish "Freenet's wrapper" from a hand-edited one before
    // overwriting (#3967). A failed sidecar write only weakens future
    // user-modification protection — warn and continue.
    let wrapper_hash_path = wrapper_path.with_extension("sh.hash");
    let wrapper_hash = super::super::update::wrapper_content_hash(&wrapper_content);
    if let Err(e) =
        super::super::update::write_wrapper_hash_sidecar(&wrapper_hash_path, &wrapper_hash)
    {
        eprintln!(
            "Warning: failed to write wrapper hash sidecar at {}: {}.",
            wrapper_hash_path.display(),
            e
        );
    }

    let plist_content = generate_plist(&wrapper_path, &log_dir);
    let plist_path = launch_agents_dir.join("org.freenet.node.plist");

    fs::write(&plist_path, plist_content).context("Failed to write plist file")?;

    println!("Freenet service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "macos")]
pub fn generate_wrapper_script(binary_path: &Path) -> String {
    format!(
        r#"#!/bin/bash
# Freenet service wrapper for auto-update support.
# This wrapper monitors exit code 42 (update needed) and runs update before restart.
# Includes exponential backoff to prevent rapid restart loops on repeated failures.
# On startup, kills any stale 'freenet network' processes to avoid port conflicts.

# Mark the node child as supervised (issue #4580). This tells `freenet network`
# that exit code 42 (update needed) will actually be caught and applied by this
# wrapper, so it logs an informational message rather than a loud "no supervisor,
# update will not be applied" error on exit.
export {supervised_env}=1

BACKOFF=10       # Initial backoff in seconds
MAX_BACKOFF=300  # Maximum backoff (5 minutes)
CONSECUTIVE_FAILURES=0
# Give up (exit the wrapper) after this many consecutive failures, so a
# persistently-failing node — a committed version that crash-loops, or an update
# that never succeeds — does not thrash and poll GitHub forever. Mirrors the
# in-process run-wrapper's WRAPPER_MAX_CONSECUTIVE_FAILURES cap (service.rs) and
# stays well above the #4073 crash-loop rollback threshold (3) so rollback always
# fires first. launchd's KeepAlive.SuccessfulExit=false means exit 1 is treated
# as a failure and is NOT auto-respawned, so this is a genuine terminal stop.
MAX_CONSECUTIVE_FAILURES=50
PORT_CONFLICT_KILLS=0
MAX_PORT_CONFLICT_KILLS=3  # Give up after this many kill attempts

# Lifecycle messages route through `logger`, which writes to macOS unified
# logging (auto-rotated, queryable via `log show --predicate 'process ==
# "logger"' --info`). Previously these were appended to a fixed
# ~/Library/Logs/freenet/freenet.log that the cleanup pass never touched
# (its mtime stayed fresh while being written), so the file grew without
# bound on long-running nodes (issue #4251). The transient
# freenet.error.log.last scratch file below is overwritten on every launch
# and so does not accumulate.
log_event() {{
    logger -t freenet "$1"
}}

# Exit the wrapper loop once consecutive failures hit the cap, so a node that
# never comes up healthy stops restarting (and stops polling GitHub) instead of
# looping forever. Called right after each failure increment.
give_up_if_failing() {{
    if [ "$CONSECUTIVE_FAILURES" -ge "$MAX_CONSECUTIVE_FAILURES" ]; then
        log_event "Giving up after $CONSECUTIVE_FAILURES consecutive failures; stopping wrapper (exit 1) for operator intervention."
        exit 1
    fi
}}

# Print a binary's full version identity line, e.g.
#   Freenet version: 0.2.71 (abc1234)
# Bounded by a 5s timeout so a wedged binary can't stall the whole self-heal
# before ExecStart. `timeout` may be absent on a bare macOS box, so fall back
# to invoking the binary directly when it isn't on PATH.
version_line() {{
    if command -v timeout >/dev/null 2>&1; then
        timeout 5 "$1" --version 2>/dev/null | grep '^Freenet version:'
    else
        "$1" --version 2>/dev/null | grep '^Freenet version:'
    fi
}}

# Print the on-disk binary's full version identity line. Used on the exit-43
# self-heal path to tell a stale orphan apart from a legitimate second
# instance running the SAME binary we would launch.
ondisk_version() {{
    version_line "{binary}"
}}

# Enumerate PIDs of `freenet network` processes owned by the current user.
holder_pids() {{
    pgrep -f -u "$(id -u)" "freenet network" 2>/dev/null
}}

# Resolve a PID's executable path on macOS. `ps -o command=` yields the full
# argv. The wrapper launches the node as `<binary> network`, so the image path
# is everything up to the trailing ` network` argument. We strip that suffix
# rather than `awk '{{print $1}}'`-splitting on whitespace, because an install
# path that contains a space (e.g. `/Users/Some User/bin/freenet`) would be
# re-truncated by whitespace splitting — the exact regression that switching
# from `ps -o comm=` to `-o command=` was meant to avoid. Empty if the process
# is gone. (If the holder's argv ever stops ending in ` network` this yields the
# full command line, which still drives the version comparison correctly.)
holder_exe() {{
    ps -o command= -p "$1" 2>/dev/null | sed 's/ network$//'
}}

# Stale-orphan self-heal for exit 43.
#
# WHY this exists: the binary returns exit 43 ("another instance is already
# running") whenever it cleanly detects something already holding the
# service port. launchd's plist sets KeepAlive.SuccessfulExit=false, so if
# this wrapper responds with `exit 0`, launchd treats it as an intentional
# stop and NEVER respawns us. (systemd has the same trap via
# RestartPreventExitStatus=43.) That is correct for a real second instance,
# but catastrophic when the port holder is an ORPHANED `freenet network`
# (PPID=1, detached from any wrapper) still running a STALE OLD binary: every
# new spawn detects it, exits 43, we stand down, and the orphan serves stale
# assets forever (issue #3967).
#
# So before deferring, decide per holder. We ONLY kill a process that is an
# ORPHAN (PPID==1, adopted by launchd/init and not this wrapper's own child) —
# a deliberately hand-run `freenet network` is parented by a user shell, so its
# PPID != 1 and we always defer to it (never SIGKILL a developer's instance or
# truncate a supervised upgrade's drain). Among orphans we kill when EITHER:
#   * its version line differs from the binary we would launch (stale binary),
#     but only when we can actually read our own on-disk version (a non-empty
#     ondisk); if ondisk is unreadable mid-update we must NOT treat "differs"
#     as a kill signal, or we'd cull a healthy current node, OR
#   * we cannot read the holder's own version at all (binary gone/unreadable),
#     in which case an init-adopted orphan is presumed stale.
# Kill is SIGTERM, then SIGKILL (the original incident's orphan ignored SIGTERM
# for >11s). Returns 0 = killed a stale orphan, relaunch; 1 = defer.
heal_stale_orphan_or_defer() {{
    local ondisk pid exe holder_ver ppid version_mismatch killed=1
    ondisk="$(ondisk_version)"
    for pid in $(holder_pids); do
        # Never touch our own child (the instance we just ran in this loop).
        [ "$pid" = "$WRAPPER_CHILD_PID" ] && continue
        ppid="$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')"
        exe="$(holder_exe "$pid")"
        holder_ver=""
        if [ -n "$exe" ] && [ -x "$exe" ]; then
            holder_ver="$(version_line "$exe")"
        fi
        # Orphan-only: a non-init-adopted holder is a real second instance (or a
        # user-run node) and is always deferred to, regardless of version.
        version_mismatch=1
        if [ -n "$ondisk" ] && [ -n "$holder_ver" ] && [ "$holder_ver" != "$ondisk" ]; then
            version_mismatch=0
        fi
        if [ "$ppid" = "1" ] && {{ [ "$version_mismatch" = "0" ] || [ -z "$holder_ver" ]; }}; then
            log_event "Exit 43: port holder PID $pid is a STALE orphan (holder='$holder_ver' ondisk='$ondisk' ppid=$ppid). Killing and relaunching."
            kill -TERM "$pid" 2>/dev/null || true
            # Wait up to ~12s for graceful exit, then escalate to SIGKILL.
            local waited=0
            while kill -0 "$pid" 2>/dev/null && [ $waited -lt 12 ]; do
                sleep 1
                waited=$((waited + 1))
            done
            if kill -0 "$pid" 2>/dev/null; then
                log_event "Exit 43: stale orphan PID $pid ignored SIGTERM after ${{waited}}s, sending SIGKILL"
                kill -KILL "$pid" 2>/dev/null || true
                sleep 1
            fi
            killed=0
        else
            log_event "Exit 43: deferring to port holder PID $pid (holder='$holder_ver' ondisk='$ondisk' ppid=$ppid) — current version, user-supervised, or version undeterminable"
        fi
    done
    return $killed
}}

# Kill any stale freenet network processes before starting.
# This handles the case where a previous launch daemon restart left a child
# process still holding the port (e.g. port 7509).
# Scoped to the current user to avoid killing processes owned by other users.
if pkill -f -u "$(id -u)" "freenet network" 2>/dev/null; then
    log_event "Killed stale freenet network process(es) on startup"
    sleep 2
fi

# Forward a SIGTERM (sent by launchd on stop / restart) to the node so it can
# run its graceful drain, rather than relying solely on launchd group-signaling
# the whole process group. The node handles SIGTERM by draining in-flight
# client drivers before closing peer connections. Harmless if launchd already
# group-signals — the child just receives a (deduplicated) TERM either way.
forward_term() {{
    [ -n "$WRAPPER_CHILD_PID" ] && kill -TERM "$WRAPPER_CHILD_PID" 2>/dev/null || true
}}
trap forward_term TERM

while true; do
    # Launch in the background so we know our own child's PID. This lets the
    # exit-43 self-heal path avoid mistaking our just-exited child for a
    # stale orphan still holding the port, and lets the TERM trap above forward
    # launchd's stop signal to the node for a graceful drain.
    "{binary}" network 2>"$HOME/Library/Logs/freenet/freenet.error.log.last" &
    WRAPPER_CHILD_PID=$!
    # `wait` is interrupted by the trapped TERM; re-wait so we collect the
    # child's real exit status after it finishes draining.
    wait $WRAPPER_CHILD_PID
    EXIT_CODE=$?
    while kill -0 "$WRAPPER_CHILD_PID" 2>/dev/null; do
        wait $WRAPPER_CHILD_PID
        EXIT_CODE=$?
    done

    if [ $EXIT_CODE -eq 42 ]; then
        log_event "Update needed, running freenet update..."
        # Pass the node's exit code so crash-loop auto-rollback (#4073) can tell
        # a post-stop restart from a manual update and count crashes of a
        # probationary version.
        if {post_stop_env}=$EXIT_CODE "{binary}" update --quiet; then
            log_event "Update successful, restarting..."
            CONSECUTIVE_FAILURES=0
            PORT_CONFLICT_KILLS=0
            BACKOFF=10
            sleep 2
        else
            CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
            give_up_if_failing
            log_event "Update failed (attempt $CONSECUTIVE_FAILURES), backing off $BACKOFF seconds..."
            sleep $BACKOFF
            BACKOFF=$((BACKOFF * 2))
            [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
        fi
        continue
    elif [ $EXIT_CODE -eq 43 ]; then
        # Another instance holds the port. Before standing down (which under
        # launchd SuccessfulExit=false would stop us forever), check whether
        # the holder is a stale orphan running an old binary and, if so, kill
        # it and relaunch instead of deferring. See heal_stale_orphan_or_defer.
        if heal_stale_orphan_or_defer; then
            log_event "Killed stale orphan holding the port on exit 43, relaunching"
            CONSECUTIVE_FAILURES=0
            PORT_CONFLICT_KILLS=0
            BACKOFF=10
            sleep 2
            continue
        fi
        log_event "Another instance (current version) is already running, exiting cleanly"
        exit 0
    elif [ $EXIT_CODE -eq 0 ]; then
        log_event "Normal shutdown"
        exit 0
    else
        # Check if this looks like a port-already-in-use failure.
        if grep -q "already in use" "$HOME/Library/Logs/freenet/freenet.error.log.last" 2>/dev/null; then
            PORT_CONFLICT_KILLS=$((PORT_CONFLICT_KILLS + 1))
            if [ $PORT_CONFLICT_KILLS -le $MAX_PORT_CONFLICT_KILLS ]; then
                log_event "Port conflict detected (attempt $PORT_CONFLICT_KILLS/$MAX_PORT_CONFLICT_KILLS) — killing stale freenet process and retrying..."
                pkill -f -u "$(id -u)" "freenet network" 2>/dev/null || true
                sleep 2
                BACKOFF=10
                continue
            else
                log_event "Port conflict persists after $MAX_PORT_CONFLICT_KILLS kill attempts. Manual intervention may be required ('pkill freenet'). Backing off..."
            fi
        else
            # #4073 crash-loop rollback / self-heal: a genuine non-graceful crash
            # (panic, SIGSEGV/SIGABRT, early-startup error — NOT a port conflict).
            # Run the post-stop updater with the exit code forwarded: if a
            # freshly-installed version on probation keeps crashing it rolls back
            # to the previous binary, and a genuine newer release self-heals
            # forward. Exit 0 => binary changed (rolled back or updated) =>
            # restart immediately; any other status => fall through to backoff.
            if {post_stop_env}=$EXIT_CODE "{binary}" update --quiet; then
                log_event "Post-crash update/rollback applied (exit $EXIT_CODE); restarting..."
                CONSECUTIVE_FAILURES=0
                PORT_CONFLICT_KILLS=0
                BACKOFF=10
                sleep 2
                continue
            fi
        fi
        CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
        give_up_if_failing
        PORT_CONFLICT_KILLS=0
        log_event "Exited with code $EXIT_CODE, restarting after backoff..."
        sleep $BACKOFF
        BACKOFF=$((BACKOFF * 2))
        [ $BACKOFF -gt $MAX_BACKOFF ] && BACKOFF=$MAX_BACKOFF
    fi
done
"#,
        binary = binary_path.display(),
        supervised_env = super::super::auto_update::SUPERVISED_ENV_VAR,
        post_stop_env = super::super::rollback::POST_STOP_EXIT_CODE_ENV_VAR,
    )
}

#[cfg(target_os = "macos")]
pub(crate) fn generate_plist(wrapper_path: &Path, log_dir: &Path) -> String {
    // Note: wrapper_path is the auto-update wrapper script, not the freenet binary directly.
    // The wrapper handles the loop: run freenet, check exit code, update if needed.
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>org.freenet.node</string>
    <key>ProgramArguments</key>
    <array>
        <string>{wrapper}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <!--
        Logging
        - The node's tracing layer writes its own size-capped, hourly-
          rotated logs to {log_dir}/freenet.YYYY-MM-DD-HH.log
          (LOG_RETENTION_HOURS + LOG_DIR_MAX_BYTES; see
          crates/core/src/tracing.rs).
        - launchd previously appended to fixed freenet.log / freenet.error.log
          that the time-based cleanup never pruned, so they grew without
          bound (issue #4251). macOS does not offer a journal target for
          launchd, so the cleanest option is /dev/null — diagnostics
          remain available via `freenet service report`, which collects
          the rotated tracing logs.
    -->
    <key>StandardOutPath</key>
    <string>/dev/null</string>
    <key>StandardErrorPath</key>
    <string>/dev/null</string>
    <key>SoftResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
    <key>HardResourceLimits</key>
    <dict>
        <key>NumberOfFiles</key>
        <integer>65536</integer>
    </dict>
</dict>
</plist>
"#,
        wrapper = wrapper_path.display(),
        log_dir = log_dir.display()
    )
}

/// Bail with "--system not supported on macOS" for commands that don't apply.
#[cfg(target_os = "macos")]
fn check_no_system_flag(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On macOS, use the default user agent commands without --system."
        );
    }
    Ok(())
}

/// Stop and remove the Freenet launchd agent. Does not purge data.
/// Returns true if a service was found and removed.
#[cfg(target_os = "macos")]
pub fn stop_and_remove_service(_system: bool) -> Result<bool> {
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;
    stop_and_remove_service_at(&home_dir)
}

/// Core of macOS uninstall, parametrized on the home directory so the
/// cleanup-ordering invariant is unit-testable without touching the real
/// `$HOME` or shelling out to `launchctl` (which only runs when the plist is
/// present).
#[cfg(target_os = "macos")]
pub(super) fn stop_and_remove_service_at(home_dir: &Path) -> Result<bool> {
    use std::fs;

    let plist_path = home_dir.join("Library/LaunchAgents/org.freenet.node.plist");

    // Remove the auto-update wrapper script and its sidecar/backup files
    // (#4290). install writes `*.sh` + `*.sh.hash`; `freenet update` may
    // leave a `*.sh.bak`. Missing files are not an error (idempotent), so we
    // run this BEFORE the plist early-return: a partial or repeated uninstall
    // can leave the plist already gone while the wrapper artifacts remain, and
    // those must still be cleaned up.
    remove_wrapper_files(&wrapper_script_path(home_dir))?;

    if !plist_path.exists() {
        return Ok(false);
    }

    if let Some(plist_path_str) = plist_path.to_str() {
        // Unload the service if loaded (ignore errors as it may not be loaded)
        let unload_status = std::process::Command::new("launchctl")
            .args(["unload", plist_path_str])
            .status();
        if let Err(e) = unload_status {
            eprintln!("Warning: Failed to unload service: {}", e);
        }
    }

    // Remove the plist file
    fs::remove_file(&plist_path).context("Failed to remove plist file")?;

    Ok(true)
}

#[cfg(target_os = "macos")]
pub(super) fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    check_no_system_flag(system)?;

    stop_and_remove_service(system)?;

    println!("Freenet service uninstalled.");

    if super::purge::should_purge(purge, keep_data)? {
        super::purge::purge_data_dirs(false)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) fn service_status(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let output = std::process::Command::new("launchctl")
        .args(["list", "org.freenet.node"])
        .output()
        .context("Failed to check service status")?;

    if output.status.success() {
        println!("Freenet service is running.");
        if !output.stdout.is_empty() {
            println!("{}", String::from_utf8_lossy(&output.stdout));
        }
    } else {
        println!("Freenet service is not running.");
        std::process::exit(3); // Standard exit code for "not running"
    }

    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) fn start_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    if !plist_path.exists() {
        anyhow::bail!("Service not installed. Run 'freenet service install' first.");
    }

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["load", plist_path_str])
        .status()
        .context("Failed to start service")?;

    if status.success() {
        println!("Freenet service started.");
        println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    } else {
        anyhow::bail!("Failed to start service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    let plist_path_str = plist_path
        .to_str()
        .context("Plist path contains invalid UTF-8")?;

    let status = std::process::Command::new("launchctl")
        .args(["unload", plist_path_str])
        .status()
        .context("Failed to stop service")?;

    if status.success() {
        println!("Freenet service stopped.");
    } else {
        anyhow::bail!("Failed to stop service");
    }

    Ok(())
}

#[cfg(target_os = "macos")]
pub(super) fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    stop_service(false)?;
    start_service(false)
}

#[cfg(target_os = "macos")]
pub(super) fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/Logs/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    super::log_utils::tail_with_rotation(&log_dir, base_name)
}
