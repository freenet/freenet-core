#[cfg(target_os = "windows")]
use anyhow::Context;
#[cfg(target_os = "windows")]
use anyhow::Result;
#[cfg(target_os = "windows")]
use super::log_utils::find_latest_log_file;

// Windows implementation
// Note: Windows service management requires either:
// 1. Running as a Windows Service (requires service registration)
// 2. Using Task Scheduler for user-level autostart
// For now, we provide Task Scheduler-based autostart

#[cfg(target_os = "windows")]
pub(super) fn install_service(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default scheduled task: freenet service install"
        );
    }

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let exe_path_str = exe_path
        .to_str()
        .context("Executable path contains invalid UTF-8")?;

    // Register Freenet to start at logon via the registry Run key.
    // This requires no admin privileges — HKCU is user-writable.
    // The wrapper manages the freenet network child process, handles
    // auto-update (exit code 42), crash backoff, log capture, and
    // shows a system tray icon.
    let run_command = format!("\"{}\" service run-wrapper", exe_path_str);
    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let (run_key, _) = hkcu
        .create_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .context("Failed to open registry Run key")?;
    run_key
        .set_value("Freenet", &run_command)
        .context("Failed to write Freenet registry entry")?;

    // Also clean up any legacy scheduled task from older installs
    drop(
        std::process::Command::new("schtasks")
            .args(["/delete", "/tn", "Freenet", "/f"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status(),
    );

    println!("Freenet autostart registered successfully.");
    println!();
    println!("To start Freenet now:");
    println!("  freenet service start");
    println!();
    println!("Freenet will start automatically when you log in.");
    println!("A system tray icon will appear with status and controls.");

    Ok(())
}

#[cfg(target_os = "windows")]
fn check_no_system_flag_windows(system: bool) -> Result<()> {
    if system {
        anyhow::bail!(
            "The --system flag is only supported on Linux.\n\
             On Windows, use the default service commands without --system."
        );
    }
    Ok(())
}

/// Stop and remove Freenet autostart. Kills running process, removes registry
/// Run key, and cleans up any legacy scheduled task. Does not purge data.
/// Returns true if Freenet was registered.
#[cfg(target_os = "windows")]
pub fn stop_and_remove_service(_system: bool) -> Result<bool> {
    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let run_key = hkcu
        .open_subkey_with_flags(
            r"Software\Microsoft\Windows\CurrentVersion\Run",
            winreg::enums::KEY_READ | winreg::enums::KEY_WRITE,
        )
        .context("Failed to open registry Run key")?;

    let had_registry = run_key.delete_value("Freenet").is_ok();

    // Kill the running Freenet service processes (wrapper + node child).
    // Targets only the service's own processes by command line so an
    // unrelated freenet.exe — e.g. the GUI installer — is never killed
    // (issue #4205).
    kill_freenet_service_processes();

    // Also clean up any legacy scheduled task from older installs
    let had_task = std::process::Command::new("schtasks")
        .args(["/query", "/tn", "Freenet"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if had_task {
        drop(
            std::process::Command::new("schtasks")
                .args(["/delete", "/tn", "Freenet", "/f"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status(),
        );
    }

    Ok(had_registry || had_task)
}

#[cfg(target_os = "windows")]
pub(super) fn uninstall_service(system: bool, purge: bool, keep_data: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    stop_and_remove_service(system)?;

    println!("Freenet autostart uninstalled.");

    if super::purge::should_purge(purge, keep_data)? {
        super::purge::purge_data_dirs(false)?;
        println!("All Freenet data, config, and logs removed.");
    }

    Ok(())
}

#[cfg(target_os = "windows")]
pub(super) fn service_status(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    let hkcu = winreg::RegKey::predef(winreg::enums::HKEY_CURRENT_USER);
    let registered = hkcu
        .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
        .ok()
        .and_then(|k| k.get_value::<String, _>("Freenet").ok())
        .is_some();

    if registered {
        println!("Freenet autostart is registered.");
        // Check if actually running
        let running = std::process::Command::new("tasklist")
            .args(["/fi", "imagename eq freenet.exe", "/fo", "csv", "/nh"])
            .output()
            .map(|o| {
                let stdout = String::from_utf8_lossy(&o.stdout);
                stdout.contains("freenet.exe")
            })
            .unwrap_or(false);
        if running {
            println!("Freenet is currently running.");
        } else {
            println!("Freenet is not currently running.");
        }
    } else {
        println!("Freenet autostart is not registered.");
        std::process::exit(3);
    }

    Ok(())
}

#[cfg(target_os = "windows")]
pub(super) fn start_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;

    // Spawn the wrapper as a detached process that survives parent exit.
    // CREATE_NEW_PROCESS_GROUP (0x200) + DETACHED_PROCESS (0x08) ensures
    // the child is not killed when the parent's console or job object closes.
    use std::os::windows::process::CommandExt;
    const DETACHED_PROCESS: u32 = 0x00000008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;
    std::process::Command::new(&exe_path)
        .args(["service", "run-wrapper"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
        .spawn()
        .context("Failed to start Freenet")?;

    println!("Freenet started.");
    println!("Open http://127.0.0.1:7509/ in your browser to view your Freenet dashboard.");

    Ok(())
}

/// Strip the leading executable path from a Windows process command line,
/// returning just the argument portion (everything after the program name).
///
/// The program path is normally quoted — both Rust's `Command` and the
/// registry `Run` key quote it — in which case it can be stripped exactly.
/// For an unquoted path we fall back to splitting on the first whitespace;
/// that is only ambiguous for unquoted paths containing spaces, which
/// Windows avoids for the processes Freenet itself spawns.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(super) fn command_line_args(cmdline: &str) -> &str {
    let trimmed = cmdline.trim_start();
    if let Some(after_open_quote) = trimmed.strip_prefix('"') {
        after_open_quote
            .find('"')
            .map_or("", |close| after_open_quote[close + 1..].trim_start())
    } else {
        trimmed
            .find(char::is_whitespace)
            .map_or("", |space| trimmed[space..].trim_start())
    }
}

/// Which Freenet service process a `freenet.exe` command line represents.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FreenetServiceProcess {
    /// The `service run-wrapper` supervisor (also the Windows tray process).
    Wrapper,
    /// The `network` node child the wrapper supervises.
    Node,
}

/// Classify a `freenet.exe` process from its full command line.
///
/// Matching is by the **subcommand prefix** — the first argument token after
/// the executable path — not by scanning every token. A blanket token scan
/// would misfire on a command line where `network` or `run-wrapper` appears
/// as an option value or path component rather than the subcommand, e.g.
/// `freenet local --config-dir network`.
///
/// Returns `None` for every other `freenet.exe` invocation, which must never
/// be killed: the GUI installer (launched with no subcommand — issue #4205),
/// `service stop`, `update`, or any directly-run `freenet` CLI command. Note
/// that a node started manually via the top-level `freenet network`
/// subcommand IS classified as `Node` — `service stop` is meant to stop a
/// running node regardless of how it was launched.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(super) fn classify_freenet_process(cmdline: &str) -> Option<FreenetServiceProcess> {
    let mut args = command_line_args(cmdline).split_whitespace();
    match args.next()? {
        "network" => Some(FreenetServiceProcess::Node),
        "service" if args.next() == Some("run-wrapper") => Some(FreenetServiceProcess::Wrapper),
        _ => None,
    }
}

/// Parse the `<pid>\t<command line>` lines produced by the PowerShell process
/// listing in `list_freenet_processes` into `(pid, command_line)` pairs.
/// Lines that do not begin with a numeric PID are ignored.
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
pub(super) fn parse_freenet_process_listing(stdout: &str) -> Vec<(u32, String)> {
    stdout
        .lines()
        .filter_map(|line| {
            let line = line.trim_end_matches('\r');
            let (pid, cmdline) = line.split_once('\t').unwrap_or((line, ""));
            Some((pid.trim().parse::<u32>().ok()?, cmdline.to_string()))
        })
        .collect()
}

/// Enumerate `freenet.exe` processes and their command lines via PowerShell.
///
/// Uses `Get-CimInstance` rather than `wmic`, which is deprecated and no
/// longer installed by default on recent Windows releases. Returns `None` if
/// the process listing could not be obtained.
///
/// A process whose `CommandLine` cannot be read (owned by another user) is
/// listed with an empty command line and so is never classified as a service
/// process. The wrapper, node, and installer all run as the same user, so
/// this never hides one of our own processes.
///
/// `.output()` captures stdout/stderr through fresh pipes and does not inherit
/// stdin, so this spawn is safe even after `FreeConsole()` (see the spawn-site
/// audit note on `run_wrapper`).
#[cfg(target_os = "windows")]
fn list_freenet_processes() -> Option<Vec<(u32, String)>> {
    let output = std::process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "Get-CimInstance Win32_Process -Filter \"Name='freenet.exe'\" | \
             ForEach-Object { \"$($_.ProcessId)`t$($_.CommandLine)\" }",
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(parse_freenet_process_listing(&String::from_utf8_lossy(
        &output.stdout,
    )))
}

/// Force-terminate a process by PID via `taskkill`. Returns whether `taskkill`
/// reported success.
///
/// All three standard handles are nulled so the spawn succeeds even when the
/// caller has detached from its console via `FreeConsole()` — the auto-update
/// restart path reaches here from the wrapper. See
/// `.claude/rules/bug-prevention-patterns.md`.
#[cfg(target_os = "windows")]
pub(super) fn taskkill_pid(pid: u32) -> bool {
    std::process::Command::new("taskkill")
        .args(["/f", "/pid", &pid.to_string()])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Enumerate `freenet.exe` processes and `taskkill` every one classified as
/// `kind`, excluding the current process. Returns the number terminated.
#[cfg(target_os = "windows")]
pub(super) fn kill_freenet_processes_matching(kind: FreenetServiceProcess) -> usize {
    let our_pid = std::process::id();
    let Some(processes) = list_freenet_processes() else {
        return 0;
    };
    let mut killed = 0;
    for (pid, cmdline) in processes {
        if pid != our_pid && classify_freenet_process(&cmdline) == Some(kind) && taskkill_pid(pid) {
            killed += 1;
        }
    }
    killed
}

/// Stop the running Freenet service — the `service run-wrapper` supervisor and
/// the `network` node child it manages.
///
/// Unlike a blanket `taskkill /im freenet.exe`, this targets only the
/// service's own processes (classified by command line), so it never kills an
/// unrelated `freenet.exe` that merely shares the image name: the GUI
/// installer (issue #4205), a concurrent `freenet` CLI command, or the caller
/// itself.
///
/// Wrappers are killed first, then nodes in a short re-enumerating loop. Only
/// a live wrapper spawns `network` children, and `taskkill /f` merely
/// *requests* termination (`TerminateProcess` is asynchronous), so a wrapper
/// can briefly outlive the wrapper pass — long enough to spawn a node the
/// first node enumeration missed. Each loop pass re-enumerates; once no
/// wrapper this sweep terminated is still alive, the node set is stable. The
/// loop stops as soon as a pass kills nothing — everything is gone, or the
/// remainder is unkillable and retrying will not help — and is bounded to 3
/// passes (200ms apart, comfortably over normal `TerminateProcess` latency)
/// so an unkillable process cannot hang the caller.
///
/// Known limitation: a wrapper mid-self-update can `spawn_new_wrapper` inside
/// its own async-termination window; that successor is not re-enumerated and
/// may survive. This matches the previous blanket `taskkill`'s exposure and
/// requires a self-update to coincide with a stop to the millisecond.
///
/// Returns the count of successful `taskkill` requests — which counts a node
/// re-killed across passes more than once, so callers only test `> 0`. A `0`
/// result means nothing was running, the process listing was unavailable, or
/// every `taskkill` failed; callers treat all three as "nothing was stopped".
#[cfg(target_os = "windows")]
pub(crate) fn kill_freenet_service_processes() -> usize {
    let mut killed = kill_freenet_processes_matching(FreenetServiceProcess::Wrapper);
    for _ in 0..3 {
        let nodes = kill_freenet_processes_matching(FreenetServiceProcess::Node);
        killed += nodes;
        if nodes == 0 {
            break;
        }
        // Give a still-dying wrapper time to finish so a node it respawns is
        // visible to the next enumeration.
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    killed
}

#[cfg(target_os = "windows")]
pub(super) fn stop_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;

    if kill_freenet_service_processes() > 0 {
        println!("Freenet stopped.");
        Ok(())
    } else {
        anyhow::bail!("Failed to stop Freenet. It may not be running.")
    }
}

#[cfg(target_os = "windows")]
pub(super) fn restart_service(system: bool) -> Result<()> {
    check_no_system_flag_windows(system)?;
    drop(stop_service(false));
    // Give it a moment to stop
    std::thread::sleep(std::time::Duration::from_secs(2));
    start_service(false)
}

#[cfg(target_os = "windows")]
pub(super) fn service_logs(error_only: bool) -> Result<()> {
    use freenet::tracing::tracer::get_log_dir;
    use std::time::Duration;

    let log_dir = get_log_dir().context(
        "Could not determine log directory. \
         Ensure Freenet has been run at least once via 'freenet service run-wrapper'.",
    )?;

    let base_name = if error_only {
        "freenet.error"
    } else {
        "freenet"
    };

    // Also check the wrapper log (now date-rotated: freenet-wrapper.YYYY-MM-DD.log)
    let wrapper_log = find_latest_log_file(&log_dir, "freenet-wrapper");

    let mut current_log = match find_latest_log_file(&log_dir, base_name) {
        Some(log_path) => {
            println!("Log file: {}", log_path.display());
            if let Some(ref wl) = wrapper_log {
                println!("Wrapper log: {}", wl.display());
            }
            log_path
        }
        None => {
            if let Some(ref wl) = wrapper_log {
                println!("No node logs found, showing wrapper log:");
                let status = std::process::Command::new("powershell")
                    .args([
                        "-Command",
                        &format!("Get-Content -Path '{}' -Tail 50 -Wait", wl.display()),
                    ])
                    .status()
                    .context("Failed to open wrapper log")?;
                std::process::exit(status.code().unwrap_or(1));
            } else {
                anyhow::bail!(
                    "No log files found in {}.\n\
                     Ensure Freenet has been run at least once.",
                    log_dir.display()
                );
            }
        }
    };

    println!("Press Ctrl+C to stop.\n");

    loop {
        let mut child = std::process::Command::new("powershell")
            .args([
                "-Command",
                &format!(
                    "Get-Content -Path '{}' -Tail 50 -Wait",
                    current_log.display()
                ),
            ])
            .spawn()
            .context("Failed to spawn PowerShell for log tailing")?;

        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    if !status.success() {
                        // Fallback: open in notepad
                        drop(
                            std::process::Command::new("notepad")
                                .arg(&current_log)
                                .spawn(),
                        );
                    }
                    std::process::exit(status.code().unwrap_or(1));
                }
                Ok(None) => {}
                Err(e) => {
                    drop(child.kill());
                    drop(child.wait());
                    anyhow::bail!("Error waiting on PowerShell process: {e}");
                }
            }

            std::thread::sleep(Duration::from_secs(5));

            if let Some(newer_log) = find_latest_log_file(&log_dir, base_name) {
                if newer_log != current_log {
                    println!("\n--- Log rotated to: {} ---\n", newer_log.display());
                    drop(child.kill());
                    drop(child.wait());
                    current_log = newer_log;
                    break;
                }
            }
        }
    }
}
