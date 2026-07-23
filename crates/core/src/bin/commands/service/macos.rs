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
# fires first. The give-up path exits 0 (NOT non-zero): the plist sets
# KeepAlive.SuccessfulExit=false, so launchd RESPAWNS on a non-zero exit and only
# treats a clean exit 0 as an intentional terminal stop (same reasoning as the
# exit-0/exit-43 paths below). A non-zero exit here would just relaunch a fresh
# wrapper with CONSECUTIVE_FAILURES reset to 0, defeating the cap entirely.
MAX_CONSECUTIVE_FAILURES=50
# A child that ran healthily for at least this long before exiting is treated as
# "made progress": its failure does NOT count toward the consecutive cap. This
# keeps the cap aimed at a tight CRASH LOOP (repeated fast failures) rather than
# at a node that runs fine for a long time and then crashes occasionally — the
# latter must keep being restarted, not eventually give up. Comfortably past the
# #4073 60s commit window and the 300s max backoff.
MIN_HEALTHY_RUNTIME=300
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
# looping forever. Called right after each failure increment. Exits 0 so launchd
# (KeepAlive.SuccessfulExit=false) treats it as an intentional stop and does NOT
# respawn — a non-zero exit would be respawned and reset the counter.
give_up_if_failing() {{
    if [ "$CONSECUTIVE_FAILURES" -ge "$MAX_CONSECUTIVE_FAILURES" ]; then
        log_event "Giving up after $CONSECUTIVE_FAILURES consecutive failures; stopping wrapper for operator intervention (clean exit so launchd does not respawn)."
        exit 0
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
    CHILD_START=$(date +%s)
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
    # How long the child ran this cycle. A long healthy run before a later
    # non-zero exit clears the consecutive-failure streak (see give_up_if_failing
    # / MIN_HEALTHY_RUNTIME) so only a tight crash loop trips the cap.
    CHILD_RUNTIME=$(( $(date +%s) - CHILD_START ))
    if [ "$CHILD_RUNTIME" -ge "$MIN_HEALTHY_RUNTIME" ]; then
        CONSECUTIVE_FAILURES=0
    fi

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
            UPDATE_RC=$?
            if [ "$UPDATE_RC" -eq {already_up_to_date} ]; then
                # Benign no-op: `freenet update` exited "already up to date" — no
                # update was performed because we are already current, the target
                # is rate-limited, pinned known-bad, or install-gated (#4073).
                # This is NOT a failure, so it must NOT count toward the give-up
                # cap; just restart the same binary after a short pause.
                log_event "Update reported nothing to do (exit $UPDATE_RC); restarting without counting a failure."
                PORT_CONFLICT_KILLS=0
                BACKOFF=10
                sleep 2
                continue
            fi
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
        already_up_to_date = super::super::update::EXIT_CODE_ALREADY_UP_TO_DATE,
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

/// Outcome of a `launchctl bootout`/`bootstrap` invocation, classified from
/// the exit status and stderr.
///
/// Pure so the classification is unit-testable on every CI platform without
/// shelling out to `launchctl` (which only exists on macOS). See the
/// "extract the platform-independent decision logic into a pure function"
/// rule in `.claude/rules/deployment.md`.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum LaunchctlOutcome {
    /// The agent reached the requested state.
    Ok,
    /// The agent was already in the requested state (not loaded when booting
    /// out, already loaded when bootstrapping). Idempotent success.
    AlreadyInState,
    /// The invocation genuinely failed; the payload is a human-readable reason.
    Failed(String),
}

/// Whether the requested end state already held, given a `launchctl` invocation
/// that reported failure. `desired_loaded` is what the caller was aiming for:
/// `true` for `bootstrap` (want it loaded), `false` for `bootout` (want it gone).
/// `still_loaded` is the observed state *after* the call.
///
/// This is the load-bearing half of the classification, and it deliberately
/// does NOT try to read meaning out of launchd's errno. On macOS 15 both
/// idempotent no-ops collapse onto the same opaque message:
///
/// ```text
/// $ launchctl bootout gui/501 …   # when not loaded
/// Boot-out failed: 5: Input/output error
/// $ launchctl bootstrap gui/501 … # when already loaded
/// Bootstrap failed: 5: Input/output error
/// ```
///
/// errno 5 is also what a genuine failure returns, so no amount of string or
/// exit-code matching can separate "already in that state" from "actually
/// broke" — the only reliable signal is to observe the end state and ask
/// whether it is the one we wanted.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) fn reconcile_launchctl_failure(
    desired_loaded: bool,
    still_loaded: bool,
) -> LaunchctlOutcome {
    if desired_loaded == still_loaded {
        // The call complained, but the world is how we wanted it. Treat the
        // complaint as noise: a `stop` of an already-stopped service, or a
        // `start` of an already-running one, must succeed idempotently.
        LaunchctlOutcome::AlreadyInState
    } else if desired_loaded {
        LaunchctlOutcome::Failed("service did not come up".to_string())
    } else {
        LaunchctlOutcome::Failed("service is still loaded".to_string())
    }
}

/// Strip launchd's boilerplate "Try re-running as root" advice from stderr so
/// it doesn't get echoed alongside our own, more actionable hint.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) fn clean_launchctl_stderr(stderr: &str) -> String {
    stderr
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.to_ascii_lowercase().starts_with("try re-running"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Classify a `launchctl` invocation from its raw exit code and stderr, without
/// consulting the end state.
///
/// A clean exit 0 with nothing on stderr is the only unambiguous success:
/// launchd's legacy `load`/`unload` subcommands print `Unload failed: 5:
/// Input/output error` while *still exiting 0*, which is precisely how #4900's
/// `service disable` came to report "Freenet service stopped." for a stop that
/// never happened. Every other combination is provisional — the caller must
/// resolve it against the observed end state via
/// [`reconcile_launchctl_failure`].
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
pub(super) fn classify_launchctl(code: Option<i32>, stderr: &str) -> LaunchctlOutcome {
    let cleaned = clean_launchctl_stderr(stderr);

    match code {
        Some(0) if cleaned.is_empty() => LaunchctlOutcome::Ok,
        // Exit 0 *with* a diagnostic: launchd lied about succeeding.
        Some(0) => LaunchctlOutcome::Failed(cleaned),
        Some(c) if cleaned.is_empty() => LaunchctlOutcome::Failed(format!("exit code {c}")),
        Some(c) => LaunchctlOutcome::Failed(format!("exit code {c}: {cleaned}")),
        // Killed by a signal.
        None if cleaned.is_empty() => LaunchctlOutcome::Failed("terminated by signal".to_string()),
        None => LaunchctlOutcome::Failed(format!("terminated by signal: {cleaned}")),
    }
}

/// The launchd domain target for the current user's GUI session, e.g. `gui/501`.
#[cfg(target_os = "macos")]
fn gui_domain_target() -> String {
    // SAFETY: libc::getuid is always safe; it returns the current user ID
    // without touching user-provided pointers.
    let uid = unsafe { libc::getuid() };
    format!("gui/{uid}")
}

/// Whether the Freenet agent is currently present in the user's GUI domain.
///
/// `launchctl print` exits 0 when the service exists in the domain and non-zero
/// (with "Could not find service ... in domain") when it does not. This is the
/// ground truth used to disambiguate launchd's opaque errno 5 — see
/// [`reconcile_launchctl_failure`].
#[cfg(target_os = "macos")]
fn service_is_loaded() -> bool {
    std::process::Command::new("launchctl")
        .arg("print")
        .arg(format!("{}/org.freenet.node", gui_domain_target()))
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Run `launchctl <subcommand> gui/<uid> <plist>`, then resolve the result
/// against the observed end state.
///
/// `desired_loaded` is the state the caller wants to end up in: `true` for
/// `bootstrap`, `false` for `bootout`. When launchd reports failure we re-probe
/// the domain rather than trusting the errno, because macOS returns the same
/// opaque `5: Input/output error` for a genuine failure and for a no-op.
#[cfg(target_os = "macos")]
fn run_launchctl(
    subcommand: &str,
    plist_path: &Path,
    desired_loaded: bool,
) -> Result<LaunchctlOutcome> {
    let output = std::process::Command::new("launchctl")
        .arg(subcommand)
        .arg(gui_domain_target())
        .arg(plist_path)
        .output()
        .with_context(|| format!("Failed to run `launchctl {subcommand}`"))?;

    let outcome = classify_launchctl(
        output.status.code(),
        &String::from_utf8_lossy(&output.stderr),
    );

    // Only a clean exit is trusted outright; anything else is checked against
    // reality before being reported to the operator as an error.
    Ok(match outcome {
        LaunchctlOutcome::Ok => LaunchctlOutcome::Ok,
        LaunchctlOutcome::AlreadyInState => LaunchctlOutcome::AlreadyInState,
        LaunchctlOutcome::Failed(reason) => {
            match reconcile_launchctl_failure(desired_loaded, service_is_loaded()) {
                // Genuinely failed: keep launchd's own message, which is more
                // specific than our end-state summary.
                LaunchctlOutcome::Failed(_) => LaunchctlOutcome::Failed(reason),
                LaunchctlOutcome::AlreadyInState => LaunchctlOutcome::AlreadyInState,
                LaunchctlOutcome::Ok => LaunchctlOutcome::Ok,
            }
        }
    })
}

#[cfg(target_os = "macos")]
pub(super) fn start_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    start_service_quiet(system)?;
    println!("Freenet service started.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    Ok(())
}

/// `start_service` without the user-facing success banner, so callers that
/// know the node will not actually come up (e.g. `service disable`, which
/// restarts the agent only so it re-reads the disable marker and parks idle)
/// don't print "started" and a dashboard URL that nothing is listening on.
#[cfg(target_os = "macos")]
pub(super) fn start_service_quiet(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    if !plist_path.exists() {
        anyhow::bail!("Service not installed. Run 'freenet service install' first.");
    }

    // `bootstrap` (not the deprecated `load`): see stop_service for why.
    match run_launchctl("bootstrap", &plist_path, true)? {
        LaunchctlOutcome::Ok | LaunchctlOutcome::AlreadyInState => Ok(()),
        LaunchctlOutcome::Failed(reason) => {
            anyhow::bail!(
                "Failed to start service: {reason}\n\
                 Try `launchctl bootstrap gui/$(id -u) {}` for richer errors.",
                plist_path.display()
            )
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;

    let plist_path = dirs::home_dir()
        .context("Failed to get home directory")?
        .join("Library/LaunchAgents/org.freenet.node.plist");

    // `bootout`, not the deprecated `unload`: against a live agent, `unload`
    // fails with `Unload failed: 5: Input/output error` while still exiting 0,
    // so the service kept running and the CLI printed "stopped" anyway. The
    // rest of the codebase already standardized on the modern domain-target
    // subcommands — see `launch_at_login.rs::launchctl_bootout` and the
    // `update.rs` wrapper-restart path.
    match run_launchctl("bootout", &plist_path, false)? {
        LaunchctlOutcome::Ok | LaunchctlOutcome::AlreadyInState => {
            println!("Freenet service stopped.");
            Ok(())
        }
        LaunchctlOutcome::Failed(reason) => {
            anyhow::bail!(
                "Failed to stop service: {reason}\n\
                 Try `launchctl bootout gui/$(id -u) {}` for richer errors.",
                plist_path.display()
            )
        }
    }
}

#[cfg(target_os = "macos")]
pub(super) fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    stop_service(false)?;
    start_service(false)
}

/// `restart_service` that does not claim the node came up. Used by
/// `service disable`, where the restart exists purely so the agent re-reads
/// the disable marker and parks in its idle state.
#[cfg(target_os = "macos")]
pub(super) fn restart_service_quiet(system: bool) -> Result<()> {
    check_no_system_flag(system)?;
    stop_service(false)?;
    start_service_quiet(false)
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

#[cfg(test)]
mod tests {
    use super::{
        LaunchctlOutcome, classify_launchctl, clean_launchctl_stderr, reconcile_launchctl_failure,
    };

    /// Regression pin for the #4900 report: `launchctl unload` against a live
    /// agent printed `Unload failed: 5: Input/output error` on stderr but
    /// still exited 0, so the CLI reported "Freenet service stopped." for a
    /// service that was still running. Exit code alone is not a valid signal.
    #[test]
    fn zero_exit_with_stderr_diagnostic_is_a_failure() {
        assert_eq!(
            classify_launchctl(Some(0), "Unload failed: 5: Input/output error\n"),
            LaunchctlOutcome::Failed("Unload failed: 5: Input/output error".to_string()),
            "a launchctl diagnostic on stderr must not be reported as success"
        );
    }

    #[test]
    fn clean_zero_exit_is_ok() {
        assert_eq!(classify_launchctl(Some(0), ""), LaunchctlOutcome::Ok);
        // Whitespace-only stderr, and launchd's boilerplate root advice, both
        // count as "no diagnostic".
        assert_eq!(classify_launchctl(Some(0), "  \n\t "), LaunchctlOutcome::Ok);
        assert_eq!(
            classify_launchctl(
                Some(0),
                "Try re-running the command as root for richer errors."
            ),
            LaunchctlOutcome::Ok
        );
    }

    #[test]
    fn nonzero_exit_carries_code_and_cleaned_reason() {
        assert_eq!(
            classify_launchctl(Some(5), "Boot-out failed: 5: Input/output error"),
            LaunchctlOutcome::Failed("exit code 5: Boot-out failed: 5: Input/output error".into())
        );
        assert_eq!(
            classify_launchctl(Some(1), ""),
            LaunchctlOutcome::Failed("exit code 1".to_string())
        );
    }

    /// `None` means killed by a signal — never success.
    #[test]
    fn signal_termination_is_a_failure() {
        assert_eq!(
            classify_launchctl(None, ""),
            LaunchctlOutcome::Failed("terminated by signal".to_string())
        );
        assert_eq!(
            classify_launchctl(None, "partial output"),
            LaunchctlOutcome::Failed("terminated by signal: partial output".to_string())
        );
    }

    /// launchd's "Try re-running the command as root" line is boilerplate that
    /// appears on every failure; echoing it would crowd out our own, more
    /// actionable `launchctl bootout gui/$(id -u) <plist>` hint.
    #[test]
    fn root_advice_boilerplate_is_stripped() {
        assert_eq!(
            clean_launchctl_stderr(
                "Boot-out failed: 5: Input/output error\n\
                 Try re-running the command as root for richer errors.\n"
            ),
            "Boot-out failed: 5: Input/output error"
        );
        // Matching is case-insensitive and survives leading whitespace.
        assert_eq!(clean_launchctl_stderr("   TRY RE-RUNNING as root\n"), "");
        // Multiple real diagnostics are preserved, joined readably.
        assert_eq!(
            clean_launchctl_stderr("first problem\nsecond problem\n"),
            "first problem; second problem"
        );
    }

    /// The core of the fix. On macOS 15 an idempotent no-op and a genuine
    /// failure are indistinguishable by errno — `bootout` when not loaded and
    /// `bootstrap` when already loaded BOTH return `5: Input/output error`,
    /// the same code a real failure returns. Only the observed end state can
    /// tell them apart.
    #[test]
    fn end_state_matching_desire_is_an_idempotent_no_op() {
        // `stop` (want unloaded) on an already-stopped service.
        assert_eq!(
            reconcile_launchctl_failure(false, false),
            LaunchctlOutcome::AlreadyInState
        );
        // `start` (want loaded) on an already-running service.
        assert_eq!(
            reconcile_launchctl_failure(true, true),
            LaunchctlOutcome::AlreadyInState
        );
    }

    #[test]
    fn end_state_contradicting_desire_is_a_real_failure() {
        // Wanted it gone, it is still there.
        assert_eq!(
            reconcile_launchctl_failure(false, true),
            LaunchctlOutcome::Failed("service is still loaded".to_string())
        );
        // Wanted it up, it did not come up.
        assert_eq!(
            reconcile_launchctl_failure(true, false),
            LaunchctlOutcome::Failed("service did not come up".to_string())
        );
    }

    /// Guard against a tempting "simplification": reading errno 5 as
    /// already-in-state. That would silently convert a genuine stop failure
    /// (service still running) into a reported success — reintroducing the
    /// exact #4900 symptom through a different door.
    #[test]
    fn errno_five_alone_must_not_imply_already_in_state() {
        let stderr = "Boot-out failed: 5: Input/output error";
        // Same stderr, same exit code, opposite verdicts — driven purely by
        // the end state. Any classifier keying on the message would be wrong
        // for one of these two cases.
        assert_eq!(
            reconcile_launchctl_failure(false, false),
            LaunchctlOutcome::AlreadyInState,
            "not loaded after bootout: {stderr} is noise"
        );
        assert_eq!(
            reconcile_launchctl_failure(false, true),
            LaunchctlOutcome::Failed("service is still loaded".to_string()),
            "still loaded after bootout: {stderr} is a real failure"
        );
    }
}
