// Every item in this module is `#[cfg(target_os = "linux")]`, so gate the
// imports too — otherwise they are unused on macOS/Windows builds.
#[cfg(target_os = "linux")]
use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use std::path::Path;

/// Path to the system-wide systemd service file.
#[cfg(target_os = "linux")]
const SYSTEM_SERVICE_PATH: &str = "/etc/systemd/system/freenet.service";

/// Check if a system-wide Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_system_service() -> bool {
    Path::new(SYSTEM_SERVICE_PATH).exists()
}

/// Check if a user-level Freenet service is installed.
#[cfg(target_os = "linux")]
fn has_user_service() -> bool {
    dirs::home_dir()
        .map(|h| h.join(".config/systemd/user/freenet.service").exists())
        .unwrap_or(false)
}

/// Recursively chown a directory to the given user (best-effort).
/// Used after creating directories with sudo so the service user can write to them.
#[cfg(target_os = "linux")]
fn chown_to_user(path: &Path, username: &str) {
    let _status = std::process::Command::new("chown")
        .args(["-R", username, &path.display().to_string()])
        .status();
}

/// Look up a user's home directory from /etc/passwd via `getent passwd`.
/// Falls back to `/home/{username}` if getent is unavailable.
#[cfg(target_os = "linux")]
pub(super) fn home_dir_for_user(username: &str) -> std::path::PathBuf {
    // Try getent passwd which works with NSS (LDAP, NIS, etc.)
    if let Ok(output) = std::process::Command::new("getent")
        .args(["passwd", username])
        .output()
    {
        if output.status.success() {
            let line = String::from_utf8_lossy(&output.stdout);
            // Format: username:x:uid:gid:gecos:home:shell
            if let Some(home) = line.split(':').nth(5) {
                let home = home.trim();
                if !home.is_empty() {
                    return std::path::PathBuf::from(home);
                }
            }
        }
    }
    std::path::PathBuf::from(format!("/home/{username}"))
}

/// Resolve whether to use system or user mode.
/// If `--system` is passed, use system mode. Otherwise auto-detect based on
/// which service file exists, defaulting to user mode.
#[cfg(target_os = "linux")]
fn use_system_mode(system_flag: bool) -> bool {
    // Auto-detect: if only system service exists, use system mode
    system_flag || (has_system_service() && !has_user_service())
}

/// Run a systemctl command, using --user or not based on system mode.
#[cfg(target_os = "linux")]
fn systemctl(system_mode: bool, args: &[&str]) -> Result<std::process::ExitStatus> {
    let mut cmd = std::process::Command::new("systemctl");
    if !system_mode {
        cmd.arg("--user");
    }
    cmd.args(args);
    let status = cmd.status().context("Failed to run systemctl")?;
    Ok(status)
}

/// Run a systemctl command with helpful error on user-session failures.
#[cfg(target_os = "linux")]
fn systemctl_with_hint(system_mode: bool, args: &[&str], action: &str) -> Result<()> {
    let status = systemctl(system_mode, args)?;
    if status.success() {
        return Ok(());
    }

    if system_mode {
        anyhow::bail!("Failed to {action}");
    }

    // Check if this looks like a user session bus issue
    let hint = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .stderr(std::process::Stdio::piped())
        .output()
        .ok()
        .and_then(|out| {
            let stderr = String::from_utf8_lossy(&out.stderr);
            if stderr.contains("bus")
                || stderr.contains("XDG_RUNTIME_DIR")
                || stderr.contains("Failed to connect")
            {
                Some(
                    "\n\nHint: User systemd session not available (common in containers/LXC).\n\
                     Try: sudo freenet service install --system",
                )
            } else {
                None
            }
        })
        .unwrap_or("");

    anyhow::bail!("Failed to {action}{hint}");
}

#[cfg(target_os = "linux")]
pub(super) fn install_service(system: bool) -> Result<()> {
    if system {
        install_system_service()
    } else {
        install_user_service()
    }
}

#[cfg(target_os = "linux")]
fn install_user_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let home_dir = dirs::home_dir().context("Failed to get home directory")?;

    let service_dir = home_dir.join(".config/systemd/user");
    fs::create_dir_all(&service_dir).context("Failed to create systemd user directory")?;

    // Create log directory - use ~/.local/state/freenet for XDG compliance
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;

    let service_content = generate_user_service_file(&exe_path, &log_dir);
    let service_path = service_dir.join("freenet.service");

    fs::write(&service_path, &service_content).context("Failed to write service file")?;

    // Sidecar records the unit's SHA-256 so a later `freenet update` can
    // distinguish "Freenet's unit" from a hand-edited one before
    // overwriting (#4287). A failed sidecar write only weakens future
    // user-modification protection — warn and continue.
    let hash_path = service_path.with_extension("service.hash");
    let unit_hash = super::super::update::wrapper_content_hash(&service_content);
    if let Err(e) = super::super::update::write_wrapper_hash_sidecar(&hash_path, &unit_hash) {
        eprintln!(
            "Warning: failed to write service hash sidecar at {}: {}.",
            hash_path.display(),
            e
        );
    }

    // Reload systemd user daemon
    systemctl_with_hint(false, &["daemon-reload"], "reload systemd daemon")?;

    // Enable the service
    systemctl_with_hint(false, &["enable", "freenet"], "enable service")?;

    println!("Freenet user service installed successfully.");
    println!();
    println!("To start the service now:");
    println!("  freenet service start");
    println!();
    println!("The service will start automatically on login.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
fn install_system_service() -> Result<()> {
    use std::fs;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;

    // Get the user to run the service as.
    // When running with sudo, SUDO_USER has the original (non-root) user.
    let username = std::env::var("SUDO_USER")
        .or_else(|_| std::env::var("USER"))
        .or_else(|_| std::env::var("LOGNAME"))
        .context(
            "Could not determine username. Set the USER environment variable \
             or run with sudo (which sets SUDO_USER).",
        )?;

    if username == "root" {
        anyhow::bail!(
            "Refusing to install system service running as root.\n\
             Run with sudo from a non-root user account so SUDO_USER is set,\n\
             or set the USER environment variable to the desired service user."
        );
    }

    // Look up the user's home directory from /etc/passwd.
    // When running with sudo, dirs::home_dir() returns /root which is wrong.
    let home_dir = home_dir_for_user(&username);

    // Create log directory and fix ownership (we're running as root via sudo,
    // but the service will run as the target user).
    let log_dir = home_dir.join(".local/state/freenet");
    fs::create_dir_all(&log_dir).context("Failed to create log directory")?;
    chown_to_user(&log_dir, &username);

    let service_content = generate_system_service_file(&exe_path, &log_dir, &username, &home_dir);

    fs::write(SYSTEM_SERVICE_PATH, &service_content).with_context(|| {
        format!(
            "Failed to write service file to {SYSTEM_SERVICE_PATH}. \
             Are you running as root? Try: sudo freenet service install --system"
        )
    })?;

    // Sidecar records the unit's SHA-256 so a later `freenet update` can
    // distinguish "Freenet's unit" from a hand-edited one before
    // overwriting (#4287). Lives next to the root-owned unit and is
    // written by the same root process, so root owns it too. A failed
    // sidecar write only weakens future user-modification protection —
    // warn and continue.
    let system_hash_path = Path::new(SYSTEM_SERVICE_PATH).with_extension("service.hash");
    let unit_hash = super::super::update::wrapper_content_hash(&service_content);
    if let Err(e) = super::super::update::write_wrapper_hash_sidecar(&system_hash_path, &unit_hash)
    {
        eprintln!(
            "Warning: failed to write service hash sidecar at {}: {}.",
            system_hash_path.display(),
            e
        );
    }

    // Reload systemd daemon (system-level, no --user)
    let status = systemctl(true, &["daemon-reload"])?;
    if !status.success() {
        anyhow::bail!("Failed to reload systemd daemon");
    }

    // Enable the service
    let status = systemctl(true, &["enable", "freenet"])?;
    if !status.success() {
        anyhow::bail!("Failed to enable service");
    }

    println!("Freenet system service installed successfully.");
    println!("  Service runs as user: {username}");
    println!();
    println!("To start the service now:");
    println!("  sudo freenet service start --system");
    println!();
    println!("The service will start automatically on boot.");
    println!("Logs will be written to: {}", log_dir.display());

    Ok(())
}

#[cfg(target_os = "linux")]
pub fn generate_user_service_file(binary_path: &Path, log_dir: &Path) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target
# Crash-loop rate limiting (issue #4551). StartLimitBurst / StartLimitIntervalSec
# are [Unit]-section directives (#4570 moved them here from [Service], where systemd
# SILENTLY IGNORES them — which had disabled the limiter entirely, so with
# Restart=always + RestartSec=10 a crashing node restarted every ~10s forever).
#
# Only a RAPID loop trips this: more than 5 starts within 120s. A legitimate
# single restart (auto-update, an occasional reboot, a one-off crash) is nowhere
# near 5-in-120s and is never penalised. A boot-wedge / fast crash exits 45 (a
# counted failure — see SuccessExitStatus in [Service]), so a tight no-fix loop of
# those trips the limiter and the unit stops; a node that ran healthily first
# exits the burst-exempt code 42 (see crate::node::p2p_impl). BOTH 42 and 45 still
# fire the ExecStopPost `freenet update` self-heal in [Service], so a boot-crash
# that a newer release fixes recovers even though exit 45 is counted.
#
# StartLimitAction=none (the default, made explicit) STOPS the unit on trip (it
# enters "failed") rather than rebooting the host — the desired terminal state
# for a crash loop: better than looping forever, and loud to the operator.
# Recover with `systemctl reset-failed freenet && systemctl start freenet` (the
# lockout also clears on the next reboot / `freenet update`).
StartLimitBurst=5
StartLimitIntervalSec=120
StartLimitAction=none

[Service]
Type=simple
# Mark the node as supervised (issue #4580): tells `freenet network` that exit
# code 42 (update needed) will be applied by ExecStopPost below, so it logs an
# informational message instead of a loud "no supervisor" error on update exit.
# systemd also sets INVOCATION_ID for every service, which the node detects as a
# fallback; this makes the intent explicit and robust.
Environment=FREENET_SUPERVISED=1
# #4551: advertise to the node that THIS unit understands the fast-crash exit code
# 45 — it keeps 45 out of SuccessExitStatus (so 45 counts toward StartLimitBurst),
# sets StartLimitAction=none, and runs `freenet update` on exit 42 OR 45. The node
# emits 45 only when it sees this marker, so a binary running under an OLD unit
# (auto-updated but not reinstalled) or the mac/Windows wrapper keeps emitting the
# self-healing exit 42 instead of a 45 the supervisor would mishandle.
Environment={fast_crash_marker}=1
# Stale-orphan self-heal (issue #3967): RestartPreventExitStatus=43 below
# means an exit 43 ("another instance already running") never restarts the
# unit. That is correct for a legitimate second instance, but if the port
# holder is an ORPHANED `freenet network` (PPID=1) still running an OLD
# binary, the unit would stand down and the orphan would serve stale assets
# forever. This pre-flight runs before every start: it finds the port
# holder, and kills it ONLY when it is an init-adopted orphan (PPID==1) whose
# `Freenet version:` line differs from the binary this unit would launch (or
# whose version can't be read). A user-run `freenet network` (parented by a
# shell, PPID!=1) is always left alone, as is a current-version orphan.
#
# systemd performs its own $VAR/${{VAR}} expansion on Exec* lines BEFORE handing
# the string to /bin/sh, so every dollar the SHELL must see is written as $$
# here (systemd collapses $$ -> a single $ for sh). Self-match guards: the
# pre-flight sh's OWN argv contains the literal "freenet network" (it is the
# substring `pgrep -f` matches), so the pre-flight excludes its own PID ($$$$ ->
# the sh's $$) and PID 1 from the holder loop. We deliberately do NOT anchor on
# the holder's exe equalling THIS unit's on-disk binary: a #3967 orphan is, by
# definition, running an OLD/DIFFERENT binary, so an `exe == on-disk binary`
# guard would skip exactly the orphan we must kill. The PPID==1 + version-line
# checks below are what distinguish a stale orphan from a legitimate holder.
# PPID is read after the final ')' in /proc/PID/stat (comm is parenthesized) so
# a comm containing whitespace can't shift the field. The '-' prefix means a
# failure here never blocks the start.
ExecStartPre=-/bin/sh -c 'self=$$$$; ondisk=$$(timeout 5 {binary} --version 2>/dev/null | grep "^Freenet version:"); for pid in $$(pgrep -f -u "$$(id -u)" "freenet network" 2>/dev/null); do [ "$$pid" = "$$self" ] && continue; [ "$$pid" = "1" ] && continue; exe=$$(readlink -f /proc/$$pid/exe 2>/dev/null); hv=""; [ -x "$$exe" ] && hv=$$(timeout 5 "$$exe" --version 2>/dev/null | grep "^Freenet version:"); ppid=$$(sed "s/.*) //" /proc/$$pid/stat 2>/dev/null | awk "{{print \$$2}}"); mismatch=1; [ -n "$$ondisk" ] && [ -n "$$hv" ] && [ "$$hv" != "$$ondisk" ] && mismatch=0; if [ "$$ppid" = "1" ] && {{ [ "$$mismatch" = "0" ] || [ -z "$$hv" ]; }}; then kill -TERM "$$pid" 2>/dev/null || true; w=0; while kill -0 "$$pid" 2>/dev/null && [ $$w -lt 12 ]; do sleep 1; w=$$((w+1)); done; kill -0 "$$pid" 2>/dev/null && kill -KILL "$$pid" 2>/dev/null || true; fi; done'
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops. The actual
# crash-loop cap (StartLimit*) lives in the [Unit] section above (#4551).
RestartSec=10
# Allow 45 seconds for graceful shutdown before SIGKILL.
# The node handles SIGTERM by (1) waiting up to `shutdown-drain-secs`
# (default 30s) for in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
# to finish, then (2) closing peer connections. The 15s headroom over
# the default drain covers peer-connection teardown + spawn-task
# cleanup. If you raise `shutdown-drain-secs`, raise this in lockstep.
TimeoutStopSec=45

# Auto-update / crash-loop rollback / boot-crash self-heal: run `freenet update`
# before systemd restarts the service on ANY non-graceful exit — every
# $EXIT_STATUS except 0 (graceful) and 43 (another instance already running).
# That covers the voluntary update-needed exit 42, the fast-crash code 45
# (#4551), AND the crash codes the listener watchdog never produces: panics
# (101), SIGABRT/SIGKILL/SIGSEGV (134/137/139, which systemd may pass as signal
# NAMES like ABRT/KILL/SEGV), and early-startup errors (1). `freenet update`
# then either steps forward (42 / a newer release) or, for a crash of a
# freshly-installed version still on probation, counts it and — once it keeps
# crashing — rolls back to the previous binary (#4073). `freenet update` is a
# SEPARATE process that can succeed even when `freenet network` crashes at
# startup, preserving the #4549 self-heal.
# This does NOT exempt crash codes from the limiter: ExecStopPost runs AFTER the
# process exits (the '-' prefix means its own result never affects restart) and
# 45 / 1 / 101 / signals stay OUT of SuccessExitStatus below, so a no-fix tight
# loop still trips StartLimitBurst and the unit stops. A `case` (not `&&`/`||`)
# avoids shell-precedence pitfalls. $$EXIT_STATUS is doubled so systemd passes a
# literal $EXIT_STATUS through to sh. {post_stop_env} forwards that status to
# `freenet update` so #4073 can tell a post-stop restart from a manual update
# and classify the crash; an OLD binary (e.g. one we rolled back TO) ignores the
# unknown env var.
ExecStopPost=-/bin/sh -c 'case "$$EXIT_STATUS" in 0|43) ;; *) {post_stop_env}="$$EXIT_STATUS" {binary} update --quiet ;; esac'
# Exit 42 (auto-update) and 43 (another instance) are clean exits, so they are
# not counted as failures — without this, rapid update cycles (exit 42 →
# ExecStopPost → restart) could exhaust the burst limit and kill the service.
# Exit 45 (fast-crash / boot-wedge, #4551) is deliberately NOT whitelisted here:
# it must COUNT toward StartLimitBurst so a tight crash loop trips the [Unit]
# limiter and the unit stops — even though the ExecStopPost above still runs
# `freenet update` for it. Counting (StartLimit) and self-heal (ExecStopPost) are
# independent: the former bounds a no-fix loop, the latter rescues a boot-crash a
# newer release fixes. (44 is the internal bundle-update-staged code, never
# emitted by `freenet network`.)
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging
# - The node's tracing layer writes its own size-capped, hourly-rotated
#   logs to {log_dir}/freenet.YYYY-MM-DD-HH.log (LOG_RETENTION_HOURS +
#   LOG_DIR_MAX_BYTES; see crates/core/src/tracing.rs).
# - systemd's StandardOutput/StandardError previously appended to a fixed
#   freenet.log / freenet.error.log that the time-based cleanup never
#   pruned (mtime stayed fresh while the file was being written), so they
#   grew without bound on long-running nodes (issue #4251).
# - Routing both to the journal lets journald handle rotation, and panics
#   or pre-tracing-init output remain queryable via
#   `journalctl --user-unit freenet`.
StandardOutput=journal
StandardError=journal
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit for user service)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=default.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display(),
        fast_crash_marker = super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR,
        post_stop_env = super::super::rollback::POST_STOP_EXIT_CODE_ENV_VAR,
    )
}

#[cfg(target_os = "linux")]
pub fn generate_system_service_file(
    binary_path: &Path,
    log_dir: &Path,
    username: &str,
    home_dir: &Path,
) -> String {
    format!(
        r#"[Unit]
Description=Freenet Node
Documentation=https://freenet.org
After=network-online.target
Wants=network-online.target
# Crash-loop rate limiting (issue #4551) — see the matching comment in the user
# unit. StartLimit* live in [Unit] (#4570); systemd silently ignores them in
# [Service]. Only a rapid loop (>5 starts / 120s) trips; a fast crash exits the
# counted code 45 while a healthy-then-died node exits the burst-exempt 42.
# StartLimitAction=none stops the unit (it does NOT reboot the host) on trip.
StartLimitBurst=5
StartLimitIntervalSec=120
StartLimitAction=none

[Service]
Type=simple
User={username}
Environment=HOME={home}
# Mark the node as supervised (issue #4580): see the matching comment in the
# user unit. Tells `freenet network` that exit code 42 (update needed) will be
# applied by ExecStopPost below, so it logs an informational message instead of
# a loud "no supervisor" error on update exit.
Environment=FREENET_SUPERVISED=1
# #4551 fast-crash-aware marker — see the matching comment in the user unit. The
# node emits exit 45 only when this is present (this unit handles 45); otherwise it
# keeps the self-healing exit 42.
Environment={fast_crash_marker}=1
# Stale-orphan self-heal (issue #3967): see the matching comment in the user
# unit (including the systemd $$-escaping, the PPID-after-final-')' parse, and
# why we do NOT anchor on the holder's exe equalling this unit's on-disk binary
# — a #3967 orphan runs an OLD/DIFFERENT binary, so that anchor would skip the
# very process we must kill). The self-PID and PID-1 skips exclude the
# pre-flight's own sh (whose argv contains the literal "freenet network").
# RestartPreventExitStatus=43 means an exit 43 never restarts the unit, so an
# init-adopted orphan (PPID==1) running an OLD binary would hold the port
# forever. This pre-flight kills the holder ONLY when it is such an orphan whose
# `Freenet version:` differs from (or can't be read against) the binary this
# unit launches; a user-run instance (PPID!=1) is always left alone. The '-'
# prefix means a failure here never blocks the start.
ExecStartPre=-/bin/sh -c 'self=$$$$; ondisk=$$(timeout 5 {binary} --version 2>/dev/null | grep "^Freenet version:"); for pid in $$(pgrep -f -u "$$(id -u)" "freenet network" 2>/dev/null); do [ "$$pid" = "$$self" ] && continue; [ "$$pid" = "1" ] && continue; exe=$$(readlink -f /proc/$$pid/exe 2>/dev/null); hv=""; [ -x "$$exe" ] && hv=$$(timeout 5 "$$exe" --version 2>/dev/null | grep "^Freenet version:"); ppid=$$(sed "s/.*) //" /proc/$$pid/stat 2>/dev/null | awk "{{print \$$2}}"); mismatch=1; [ -n "$$ondisk" ] && [ -n "$$hv" ] && [ "$$hv" != "$$ondisk" ] && mismatch=0; if [ "$$ppid" = "1" ] && {{ [ "$$mismatch" = "0" ] || [ -z "$$hv" ]; }}; then kill -TERM "$$pid" 2>/dev/null || true; w=0; while kill -0 "$$pid" 2>/dev/null && [ $$w -lt 12 ]; do sleep 1; w=$$((w+1)); done; kill -0 "$$pid" 2>/dev/null && kill -KILL "$$pid" 2>/dev/null || true; fi; done'
ExecStart={binary} network
Restart=always
# Wait 10 seconds before restart to avoid rapid restart loops. The actual
# crash-loop cap (StartLimit*) lives in the [Unit] section above (#4551).
RestartSec=10
# Allow 45 seconds for graceful shutdown before SIGKILL.
# The node handles SIGTERM by (1) waiting up to `shutdown-drain-secs`
# (default 30s) for in-flight client PUT/GET/UPDATE/SUBSCRIBE drivers
# to finish, then (2) closing peer connections. The 15s headroom over
# the default drain covers peer-connection teardown + spawn-task
# cleanup. If you raise `shutdown-drain-secs`, raise this in lockstep.
TimeoutStopSec=45

# Auto-update / crash-loop rollback / boot-crash self-heal: run `freenet update`
# before systemd restarts the service on ANY non-graceful exit — every
# $EXIT_STATUS except 0 (graceful) and 43 (another instance already running).
# That covers the voluntary update-needed exit 42, the fast-crash code 45
# (#4551), AND the crash codes the listener watchdog never produces: panics
# (101), SIGABRT/SIGKILL/SIGSEGV (134/137/139, which systemd may pass as signal
# NAMES like ABRT/KILL/SEGV), and early-startup errors (1). `freenet update`
# then either steps forward (42 / a newer release) or, for a crash of a
# freshly-installed version still on probation, counts it and — once it keeps
# crashing — rolls back to the previous binary (#4073). `freenet update` is a
# SEPARATE process that can succeed even when `freenet network` crashes at
# startup, preserving the #4549 self-heal.
# This does NOT exempt crash codes from the limiter: ExecStopPost runs AFTER the
# process exits (the '-' prefix means its own result never affects restart) and
# 45 / 1 / 101 / signals stay OUT of SuccessExitStatus below, so a no-fix tight
# loop still trips StartLimitBurst and the unit stops. A `case` (not `&&`/`||`)
# avoids shell-precedence pitfalls. $$EXIT_STATUS is doubled so systemd passes a
# literal $EXIT_STATUS through to sh. {post_stop_env} forwards that status to
# `freenet update` so #4073 can tell a post-stop restart from a manual update
# and classify the crash; an OLD binary (e.g. one we rolled back TO) ignores the
# unknown env var.
ExecStopPost=-/bin/sh -c 'case "$$EXIT_STATUS" in 0|43) ;; *) {post_stop_env}="$$EXIT_STATUS" {binary} update --quiet ;; esac'
# Exit 42 (auto-update) and 43 (another instance) are clean exits, so they are
# not counted as failures — without this, rapid update cycles (exit 42 →
# ExecStopPost → restart) could exhaust the burst limit and kill the service.
# Exit 45 (fast-crash / boot-wedge, #4551) is deliberately NOT whitelisted here:
# it must COUNT toward StartLimitBurst so a tight crash loop trips the [Unit]
# limiter and the unit stops — even though the ExecStopPost above still runs
# `freenet update` for it. Counting (StartLimit) and self-heal (ExecStopPost) are
# independent: the former bounds a no-fix loop, the latter rescues a boot-crash a
# newer release fixes. (44 is the internal bundle-update-staged code, never
# emitted by `freenet network`.)
SuccessExitStatus=42 43
# Exit code 43 = another instance is already running on the port.
# Do NOT restart — the existing instance is healthy.
RestartPreventExitStatus=43

# Logging
# - The node's tracing layer writes its own size-capped, hourly-rotated
#   logs to {log_dir}/freenet.YYYY-MM-DD-HH.log (LOG_RETENTION_HOURS +
#   LOG_DIR_MAX_BYTES; see crates/core/src/tracing.rs).
# - systemd's StandardOutput/StandardError previously appended to a fixed
#   freenet.log / freenet.error.log that the time-based cleanup never
#   pruned (mtime stayed fresh while the file was being written), so they
#   grew without bound on long-running nodes (issue #4251).
# - Routing both to the journal lets journald handle rotation, and panics
#   or pre-tracing-init output remain queryable via
#   `journalctl -u freenet`.
StandardOutput=journal
StandardError=journal
SyslogIdentifier=freenet

# Resource limits to prevent runaway resource consumption
# File descriptors needed for network connections
LimitNOFILE=65536
# Memory limit (2GB soft limit)
MemoryMax=2G
# CPU quota (200% = 2 cores max)
CPUQuota=200%

[Install]
WantedBy=multi-user.target
"#,
        binary = binary_path.display(),
        log_dir = log_dir.display(),
        username = username,
        home = home_dir.display(),
        fast_crash_marker = super::super::auto_update::SYSTEMD_FAST_CRASH_ENV_VAR,
        post_stop_env = super::super::rollback::POST_STOP_EXIT_CODE_ENV_VAR,
    )
}

/// Stop, disable, and remove the Freenet service file. Does not purge data.
/// Returns true if a service was found and removed.
#[cfg(target_os = "linux")]
pub fn stop_and_remove_service(system: bool) -> Result<bool> {
    use std::fs;

    let system_mode = use_system_mode(system);

    let service_path = if system_mode {
        std::path::PathBuf::from(SYSTEM_SERVICE_PATH)
    } else {
        dirs::home_dir()
            .context("Failed to get home directory")?
            .join(".config/systemd/user/freenet.service")
    };

    if !service_path.exists() {
        return Ok(false);
    }

    // Stop the service if running (best-effort, may already be stopped)
    let _stop = systemctl(system_mode, &["stop", "freenet"]);

    // Disable the service (best-effort, may already be disabled)
    let _disable = systemctl(system_mode, &["disable", "freenet"]);

    fs::remove_file(&service_path).context("Failed to remove service file")?;

    // Reload systemd (best-effort, failure is non-fatal during uninstall)
    drop(systemctl(system_mode, &["daemon-reload"]));

    Ok(true)
}

#[cfg(target_os = "linux")]
pub(super) fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    stop_and_remove_service(system)?;

    println!("Freenet service uninstalled.");

    if super::purge::should_purge(purge, keep_data)? {
        let system_mode = use_system_mode(system);
        super::purge::purge_data_dirs(system_mode)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn service_status(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    let status = systemctl(system_mode, &["status", "freenet"])?;
    std::process::exit(status.code().unwrap_or(1));
}

#[cfg(target_os = "linux")]
pub(super) fn start_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["start", "freenet"], "start service")?;
    println!("Freenet service started.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn stop_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["stop", "freenet"], "stop service")?;
    println!("Freenet service stopped.");
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn restart_service(system: bool) -> Result<()> {
    let system_mode = use_system_mode(system);
    systemctl_with_hint(system_mode, &["restart", "freenet"], "restart service")?;
    println!("Freenet service restarted.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");
    Ok(())
}

#[cfg(target_os = "linux")]
pub(super) fn service_logs(error_only: bool) -> Result<()> {
    let log_dir = dirs::home_dir()
        .context("Failed to get home directory")?
        .join(".local/state/freenet");

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    super::log_utils::tail_with_rotation(&log_dir, base_name)
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use std::path::Path;

    use super::{generate_system_service_file, generate_user_service_file};

    /// Byte offset of the section header line (a line whose trimmed content is
    /// exactly `[name]`). Matches by LINE, not a naive substring search, so a
    /// bracketed section name mentioned inside a comment (e.g. "...ignored in
    /// `[Service]`" inside the `[Unit]` section, #4551) is never mistaken for the
    /// real section header.
    fn section_header_offset(unit: &str, name: &str) -> usize {
        let header = format!("[{name}]");
        let mut offset = 0usize;
        for line in unit.split_inclusive('\n') {
            if line.trim() == header {
                return offset;
            }
            offset += line.len();
        }
        panic!("unit must contain a {header} section header line");
    }

    /// Body of the `[name]` section: everything between that section's header line
    /// and the next section header line. Header lines are matched by line (a
    /// comment containing a bracketed name is ignored), so this is robust to
    /// `[Service]`/`[Unit]` being referenced inside comments.
    fn section<'a>(unit: &'a str, name: &str) -> &'a str {
        let header = format!("[{name}]");
        let mut offset = 0usize;
        let mut body_start: Option<usize> = None;
        for line in unit.split_inclusive('\n') {
            let trimmed = line.trim();
            match body_start {
                None => {
                    if trimmed == header {
                        body_start = Some(offset + line.len());
                    }
                }
                Some(start) => {
                    if trimmed.starts_with('[') && trimmed.ends_with(']') && !trimmed.contains(' ')
                    {
                        return &unit[start..offset];
                    }
                }
            }
            offset += line.len();
        }
        match body_start {
            Some(start) => &unit[start..],
            None => panic!("unit must contain {header} section"),
        }
    }

    fn assert_start_limit_directives_are_in_unit_section(unit_name: &str, unit: &str) {
        let unit_header = section_header_offset(unit, "Unit");
        let service_header = section_header_offset(unit, "Service");
        let unit_section = section(unit, "Unit");
        let service_section = section(unit, "Service");

        for directive in ["StartLimitBurst=5", "StartLimitIntervalSec=120"] {
            let occurrences = unit.lines().filter(|line| line.trim() == directive).count();
            assert_eq!(
                occurrences, 1,
                "{unit_name} unit must emit exactly one {directive}"
            );

            let directive_offset = unit
                .find(directive)
                .unwrap_or_else(|| panic!("{unit_name} unit must contain {directive}"));
            assert!(
                directive_offset > unit_header && directive_offset < service_header,
                "{directive} must appear after [Unit] and before [Service] in {unit_name} unit"
            );
            assert!(
                unit_section.lines().any(|line| line.trim() == directive),
                "{directive} must be in [Unit] for {unit_name} unit"
            );
            assert!(
                !service_section.lines().any(|line| line.trim() == directive),
                "{directive} must not be in [Service] for {unit_name} unit"
            );
        }
    }

    #[test]
    fn user_unit_places_start_limit_directives_in_unit_section() {
        let unit = generate_user_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
        );

        assert_start_limit_directives_are_in_unit_section("user", &unit);
    }

    #[test]
    fn system_unit_places_start_limit_directives_in_unit_section() {
        let unit = generate_system_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
            "testuser",
            Path::new("/home/test"),
        );

        assert_start_limit_directives_are_in_unit_section("system", &unit);
    }

    #[test]
    fn systemd_units_keep_auto_update_success_exit_status() {
        let user_unit = generate_user_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
        );
        let system_unit = generate_system_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
            "testuser",
            Path::new("/home/test"),
        );

        assert!(user_unit.contains("SuccessExitStatus=42"));
        assert!(system_unit.contains("SuccessExitStatus=42"));
    }

    #[test]
    fn systemd_units_pass_node_exit_code_to_post_stop_update() {
        // #4073 crash-loop auto-rollback: ExecStopPost must (a) fire on ANY
        // non-graceful exit (every status except 0 and 43) so panics / signals /
        // early errors are covered, and (b) forward the node's stop status to
        // `freenet update` via the env var so the updater can classify the
        // crash. systemd escapes `$$` to a literal `$`.
        let env = super::super::super::rollback::POST_STOP_EXIT_CODE_ENV_VAR;
        // Broadened guard: skip only 0 and 43; run update (with the status
        // forwarded) for everything else.
        let expected = format!(r#"case "$$EXIT_STATUS" in 0|43) ;; *) {env}="$$EXIT_STATUS""#);
        let user_unit = generate_user_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
        );
        let system_unit = generate_system_service_file(
            Path::new("/usr/local/bin/freenet"),
            Path::new("/home/test/.local/state/freenet"),
            "testuser",
            Path::new("/home/test"),
        );
        for (name, unit) in [("user", &user_unit), ("system", &system_unit)] {
            assert!(
                unit.contains(&expected),
                "{name} unit must fire the post-stop update on any non-graceful exit and \
                 forward {env} (#4073)"
            );
            // Regression guard: the OLD narrow `42|45)` form must be gone.
            assert!(
                !unit.contains("42|45)"),
                "{name} unit still uses the narrow 42|45 ExecStopPost guard, missing \
                 panic/signal/early-error crashes (#4073 M1)"
            );
        }
    }
}
