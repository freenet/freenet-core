//! Registration of the `freenet://` URL scheme with the operating system (#5726).
//!
//! What gets registered, per platform:
//!
//! * **Linux**: `~/.local/share/applications/freenet-url-handler.desktop` with
//!   `MimeType=x-scheme-handler/freenet;` and `Exec="<bin>" open -- %u`. Then
//!   `xdg-mime default` makes it the handler, but only when no handler is set
//!   (a handler the user chose is never replaced).
//! * **Windows**: `HKCU\Software\Classes\freenet` with `URL Protocol` and
//!   `shell\open\command` = `"<exe>" open -- "%1"`. HKCU, so no admin rights.
//! * **macOS**: nothing at runtime. The `Freenet.app` bundle declares the scheme
//!   in its `Info.plist` (`scripts/package-macos.sh`) and LaunchServices
//!   delivers links to the running menu-bar wrapper (`tray.rs`). A bare binary
//!   outside a bundle cannot receive URLs on macOS.
//!
//! # Why the registered path survives auto-update
//!
//! The registration names the binary at its installed path. `freenet update`
//! installs a new release by renaming it onto that same path (`replace_binary`),
//! and crash-loop rollback restores to it too, so the path stays valid. The
//! updater never reads or writes anything here, and nothing here touches the
//! systemd unit, the Run key, or any file the updater or wrapper uses.
//!
//! # Existing installs
//!
//! The updater replaces the binary without re-running the installer, and the
//! OLD binary runs `freenet update`. So the only code that can register the
//! handler for someone who installed before this existed is the NEW binary, on
//! start: [`spawn_self_registration`]. It acts only when the running binary is
//! the one a Freenet-managed install launches (the user systemd unit's
//! `ExecStart`, or the Windows Run key), does nothing when the registration is
//! already current, and can never fail or delay the node.

#[cfg(any(target_os = "linux", test))]
use std::path::Path;
#[cfg(any(target_os = "linux", target_os = "windows"))]
use std::path::PathBuf;

use anyhow::Result;
use clap::Subcommand;

#[cfg(any(target_os = "linux", test))]
/// File name of the Linux desktop entry (also its desktop-file id).
pub const DESKTOP_FILE_NAME: &str = "freenet-url-handler.desktop";

#[cfg(any(target_os = "linux", test))]
/// MIME type desktop environments use for URL scheme handlers.
pub const SCHEME_MIME_TYPE: &str = "x-scheme-handler/freenet";

#[cfg(any(target_os = "linux", test))]
/// Marker line identifying a desktop entry Freenet wrote, so uninstall never
/// removes a file someone else put at the same path.
pub const DESKTOP_MARKER: &str = "X-Freenet-Managed=true";

/// Registry key (under HKCU) for the Windows URL protocol.
#[cfg(target_os = "windows")]
pub const WINDOWS_CLASS_KEY: &str = r"Software\Classes\freenet";

/// What a registration attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOutcome {
    /// The handler was written or updated.
    Registered,
    /// Everything was already current; nothing was written.
    AlreadyCurrent,
    /// The desktop entry is current but another application is the default
    /// handler for `freenet://`; left as the user chose it.
    ForeignDefault(String),
    /// Not applicable on this system (reason given).
    Skipped(&'static str),
}

// ── Linux ───────────────────────────────────────────────────────────────────

/// Characters that cannot be represented safely in a desktop-entry `Exec` key
/// without the spec's two-level escaping. Paths containing them are refused
/// rather than escaped: the simplest escape that is obviously correct is none.
#[cfg(any(target_os = "linux", test))]
fn exec_path_is_representable(path: &str) -> bool {
    !path.is_empty()
        && path.starts_with('/')
        && !path
            .chars()
            .any(|c| c.is_control() || matches!(c, '"' | '`' | '$' | '\\' | '%'))
}

/// Render the desktop entry for `binary`. Returns `None` for a path that
/// cannot be written into `Exec` safely.
///
/// `Exec` quotes the path (it may contain spaces) and passes the link as `%u`
/// after a literal `--`: the launcher substitutes `%u` as ONE argv entry with
/// no shell, and `--` stops the link being read as a flag.
#[cfg(any(target_os = "linux", test))]
pub fn render_desktop_entry(binary: &Path) -> Option<String> {
    let path = binary.to_str()?;
    if !exec_path_is_representable(path) {
        return None;
    }
    Some(format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=Freenet\n\
         Comment=Open freenet:// links with your local Freenet peer\n\
         Exec=\"{path}\" open -- %u\n\
         Terminal=false\n\
         NoDisplay=true\n\
         MimeType={SCHEME_MIME_TYPE};\n\
         {DESKTOP_MARKER}\n"
    ))
}

#[cfg(any(target_os = "linux", test))]
/// What to do about the `xdg-mime` default, given what it reports now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefaultAction {
    /// No handler is set: make ours the default.
    SetOurs,
    /// Ours is already the default.
    Keep,
    /// Another application is the default; leave the user's choice alone.
    LeaveForeign(String),
}

#[cfg(any(target_os = "linux", test))]
/// Pure decision on the `xdg-mime query default` output.
pub fn decide_default_action(current_default: &str) -> DefaultAction {
    let current = current_default.trim();
    if current.is_empty() {
        DefaultAction::SetOurs
    } else if current == DESKTOP_FILE_NAME {
        DefaultAction::Keep
    } else {
        DefaultAction::LeaveForeign(current.to_string())
    }
}

#[cfg(any(target_os = "linux", test))]
/// Remove our association line(s) from a `mimeapps.list` body, leaving
/// everything else byte-for-byte. Returns `None` when there is nothing to
/// remove. A line that lists us alongside other handlers is left alone.
pub fn strip_mimeapps_association(content: &str) -> Option<String> {
    let ours = |line: &str| {
        let line = line.trim();
        line == format!("{SCHEME_MIME_TYPE}={DESKTOP_FILE_NAME}")
            || line == format!("{SCHEME_MIME_TYPE}={DESKTOP_FILE_NAME};")
    };
    if !content.lines().any(ours) {
        return None;
    }
    let mut out = String::with_capacity(content.len());
    for line in content.split_inclusive('\n') {
        if !ours(line) {
            out.push_str(line);
        }
    }
    Some(out)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    /// Upper bound on any helper (`xdg-mime`, `update-desktop-database`).
    const HELPER_TIMEOUT: Duration = Duration::from_secs(5);

    fn data_home() -> Option<PathBuf> {
        std::env::var_os("XDG_DATA_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .or_else(|| dirs::home_dir().map(|h| h.join(".local/share")))
    }

    fn config_home() -> Option<PathBuf> {
        std::env::var_os("XDG_CONFIG_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
            .or_else(|| dirs::home_dir().map(|h| h.join(".config")))
    }

    pub(super) fn applications_dir() -> Option<PathBuf> {
        data_home().map(|d| d.join("applications"))
    }

    pub(super) fn desktop_file_path() -> Option<PathBuf> {
        applications_dir().map(|d| d.join(DESKTOP_FILE_NAME))
    }

    fn on_path(program: &str) -> bool {
        std::env::var_os("PATH").is_some_and(|paths| {
            std::env::split_paths(&paths).any(|dir| {
                let candidate = dir.join(program);
                candidate.is_file()
                    && std::fs::metadata(&candidate).is_ok_and(|m| {
                        use std::os::unix::fs::PermissionsExt;
                        m.permissions().mode() & 0o111 != 0
                    })
            })
        })
    }

    /// Run a helper with null stdin/stderr, capturing stdout, killed after
    /// [`HELPER_TIMEOUT`]. `None` if it could not run, failed, or timed out.
    fn run_helper(program: &str, args: &[&str]) -> Option<String> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;
        let deadline = Instant::now() + HELPER_TIMEOUT;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    let mut out = String::new();
                    if let Some(mut stdout) = child.stdout.take() {
                        drop(std::io::Read::read_to_string(&mut stdout, &mut out));
                    }
                    return status.success().then_some(out);
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                _ => {
                    drop(child.kill());
                    drop(child.wait());
                    return None;
                }
            }
        }
    }

    /// Write or refresh the desktop entry and, when no handler is set, make it
    /// the default. Idempotent.
    pub(super) fn register(binary: &Path) -> Result<RegisterOutcome> {
        if !on_path("xdg-open") {
            // The handler opens the browser with xdg-open; without it (a
            // headless server, typically) registering would only create a
            // handler that cannot work.
            return Ok(RegisterOutcome::Skipped(
                "xdg-open is not installed (no desktop environment)",
            ));
        }
        let Some(entry) = render_desktop_entry(binary) else {
            return Ok(RegisterOutcome::Skipped(
                "the binary path cannot be written into a desktop entry",
            ));
        };
        let dir = applications_dir()
            .ok_or_else(|| anyhow::anyhow!("could not determine the applications directory"))?;
        let path = dir.join(DESKTOP_FILE_NAME);

        let entry_changed = std::fs::read_to_string(&path).ok().as_deref() != Some(&entry);
        if entry_changed {
            std::fs::create_dir_all(&dir)?;
            super::super::open_link::write_atomically(&path, entry.as_bytes())?;
            if on_path("update-desktop-database") {
                let dir_str = dir.to_string_lossy();
                drop(run_helper("update-desktop-database", &["-q", &dir_str]));
            }
        }

        let default_changed = if on_path("xdg-mime") {
            let current =
                run_helper("xdg-mime", &["query", "default", SCHEME_MIME_TYPE]).unwrap_or_default();
            match decide_default_action(&current) {
                DefaultAction::SetOurs => {
                    run_helper(
                        "xdg-mime",
                        &["default", DESKTOP_FILE_NAME, SCHEME_MIME_TYPE],
                    )
                    .ok_or_else(|| anyhow::anyhow!("xdg-mime default failed"))?;
                    true
                }
                DefaultAction::Keep => false,
                DefaultAction::LeaveForeign(other) => {
                    if entry_changed {
                        return Ok(RegisterOutcome::Registered);
                    }
                    return Ok(RegisterOutcome::ForeignDefault(other));
                }
            }
        } else {
            false
        };

        Ok(if entry_changed || default_changed {
            RegisterOutcome::Registered
        } else {
            RegisterOutcome::AlreadyCurrent
        })
    }

    /// Remove our desktop entry and our `mimeapps.list` line. Returns whether
    /// anything was removed.
    pub(super) fn unregister() -> Result<bool> {
        let mut removed = false;
        if let Some(path) = desktop_file_path() {
            if std::fs::read_to_string(&path).is_ok_and(|c| c.contains(DESKTOP_MARKER)) {
                std::fs::remove_file(&path)?;
                removed = true;
                if on_path("update-desktop-database") {
                    if let Some(dir) = path.parent() {
                        let dir_str = dir.to_string_lossy();
                        drop(run_helper("update-desktop-database", &["-q", &dir_str]));
                    }
                }
            }
        }
        if let Some(list) = config_home().map(|c| c.join("mimeapps.list")) {
            if let Ok(content) = std::fs::read_to_string(&list) {
                if let Some(stripped) = strip_mimeapps_association(&content) {
                    super::super::open_link::write_atomically(&list, stripped.as_bytes())?;
                    removed = true;
                }
            }
        }
        Ok(removed)
    }

    /// Whether `binary` is what the installed user unit runs. Compares
    /// against the exact `ExecStart=` line `generate_user_service_file`
    /// emits, so a hand-written unit, a system unit, a Docker entrypoint or a
    /// test binary never qualifies.
    pub(super) fn is_managed_install(binary: &Path) -> bool {
        let Some(unit) = dirs::home_dir().map(|h| h.join(".config/systemd/user/freenet.service"))
        else {
            return false;
        };
        let Ok(content) = std::fs::read_to_string(unit) else {
            return false;
        };
        let expected = format!("ExecStart={} network", binary.display());
        content.lines().any(|line| line == expected)
    }
}

// ── Windows ─────────────────────────────────────────────────────────────────

/// The `shell\open\command` value for `exe`: the exe path quoted, the link as
/// `"%1"` after a literal `--`.
#[cfg(any(target_os = "windows", test))]
pub fn windows_open_command(exe: &str) -> String {
    format!("\"{exe}\" open -- \"%1\"")
}

/// The quoted executable at the start of a registry command line, if any.
#[cfg(any(target_os = "windows", test))]
pub fn windows_command_exe(command: &str) -> Option<&str> {
    let rest = command.trim_start().strip_prefix('"')?;
    rest.split_once('"').map(|(exe, _)| exe)
}

/// Whether a registry command line launches a `freenet.exe`, i.e. is a
/// registration Freenet may overwrite or remove.
#[cfg(any(target_os = "windows", test))]
pub fn windows_command_is_ours(command: &str) -> bool {
    windows_command_exe(command).is_some_and(|exe| {
        exe.rsplit(['\\', '/'])
            .next()
            .is_some_and(|name| name.eq_ignore_ascii_case("freenet.exe"))
    })
}

#[cfg(target_os = "windows")]
mod windows {
    use super::*;
    use winreg::RegKey;
    use winreg::enums::HKEY_CURRENT_USER;

    fn current_command() -> Option<String> {
        RegKey::predef(HKEY_CURRENT_USER)
            .open_subkey(format!(r"{WINDOWS_CLASS_KEY}\shell\open\command"))
            .ok()?
            .get_value::<String, _>("")
            .ok()
    }

    pub(super) fn register(exe: &std::path::Path) -> Result<RegisterOutcome> {
        let exe = exe
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("executable path is not valid UTF-8"))?;
        if exe.contains('"') {
            return Ok(RegisterOutcome::Skipped(
                "the executable path contains a quote",
            ));
        }
        let command = windows_open_command(exe);
        match current_command() {
            Some(existing) if existing == command => return Ok(RegisterOutcome::AlreadyCurrent),
            Some(existing) if !windows_command_is_ours(&existing) => {
                return Ok(RegisterOutcome::ForeignDefault(existing));
            }
            _ => {}
        }
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let (class, _) = hkcu.create_subkey(WINDOWS_CLASS_KEY)?;
        class.set_value("", &"URL:Freenet Protocol")?;
        class.set_value("URL Protocol", &"")?;
        let (icon, _) = class.create_subkey("DefaultIcon")?;
        icon.set_value("", &format!("\"{exe}\",0"))?;
        let (cmd, _) = class.create_subkey(r"shell\open\command")?;
        cmd.set_value("", &command)?;
        Ok(RegisterOutcome::Registered)
    }

    pub(super) fn unregister() -> Result<bool> {
        match current_command() {
            Some(existing) if windows_command_is_ours(&existing) => {
                RegKey::predef(HKEY_CURRENT_USER).delete_subkey_all(WINDOWS_CLASS_KEY)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Whether `exe` is what the Run key launches (`"<exe>" service run-wrapper`).
    pub(super) fn is_managed_install(exe: &std::path::Path) -> bool {
        let Some(run) = RegKey::predef(HKEY_CURRENT_USER)
            .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
            .ok()
            .and_then(|k| k.get_value::<String, _>("Freenet").ok())
        else {
            return false;
        };
        let (Some(registered), Some(exe)) = (windows_command_exe(&run), exe.to_str()) else {
            return false;
        };
        registered.eq_ignore_ascii_case(exe)
    }
}

// ── Platform dispatch ───────────────────────────────────────────────────────

/// The binary the registration should point at: this executable.
#[cfg(any(target_os = "linux", target_os = "windows"))]
fn handler_binary() -> Result<PathBuf> {
    Ok(std::env::current_exe()?)
}

/// Register the handler for the running binary. Idempotent.
pub fn register() -> Result<RegisterOutcome> {
    #[cfg(target_os = "linux")]
    {
        linux::register(&handler_binary()?)
    }
    #[cfg(target_os = "windows")]
    {
        windows::register(&handler_binary()?)
    }
    #[cfg(target_os = "macos")]
    {
        Ok(RegisterOutcome::Skipped(
            "on macOS the Freenet.app bundle registers freenet:// itself",
        ))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        Ok(RegisterOutcome::Skipped("not supported on this platform"))
    }
}

/// Remove the handler registration if Freenet wrote it. Returns whether
/// anything was removed.
pub fn unregister() -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        linux::unregister()
    }
    #[cfg(target_os = "windows")]
    {
        windows::unregister()
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        Ok(false)
    }
}

/// Register as part of an explicit install, printing one line about it.
/// Never fails the install: the node works without the handler.
pub fn register_for_install() {
    match register() {
        Ok(RegisterOutcome::Registered) => {
            println!("Registered freenet:// links to open with this Freenet install.");
        }
        Ok(RegisterOutcome::AlreadyCurrent) => {}
        Ok(RegisterOutcome::ForeignDefault(other)) => {
            println!(
                "Note: freenet:// links are set to open with another application ({other}); \
                 left unchanged."
            );
        }
        Ok(RegisterOutcome::Skipped(reason)) => {
            println!("freenet:// link handler not registered: {reason}.");
        }
        Err(e) => {
            eprintln!("Warning: could not register the freenet:// link handler: {e:#}");
        }
    }
}

/// Unregister as part of an explicit uninstall. Never fails the uninstall.
pub fn unregister_for_uninstall() {
    match unregister() {
        Ok(true) => println!("Removed the freenet:// link handler."),
        Ok(false) => {}
        Err(e) => eprintln!("Warning: could not remove the freenet:// link handler: {e:#}"),
    }
}

/// Whether the running binary belongs to a Freenet-managed install, so that
/// registering it on start is appropriate.
fn running_binary_is_managed() -> bool {
    #[cfg(target_os = "linux")]
    {
        handler_binary().is_ok_and(|b| linux::is_managed_install(&b))
    }
    #[cfg(target_os = "windows")]
    {
        handler_binary().is_ok_and(|b| windows::is_managed_install(&b))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    {
        false
    }
}

/// Register the handler in the background when the node starts, so installs
/// that predate the handler get it from the auto-updater alone.
///
/// Runs on a detached thread and returns immediately. It acts only for a
/// managed install (see the module docs), writes only when something differs,
/// swallows every error and panic, and logs only when it changed something or
/// failed. It holds no lock and touches no file the node, wrapper or updater
/// use, so it cannot affect startup, shutdown or the exit-42 update path.
pub fn spawn_self_registration() {
    let spawned = std::thread::Builder::new()
        .name("freenet-url-handler".into())
        .spawn(|| {
            let result = std::panic::catch_unwind(|| {
                if !running_binary_is_managed() {
                    return None;
                }
                Some(register())
            });
            match result {
                Ok(None) | Ok(Some(Ok(RegisterOutcome::AlreadyCurrent))) => {}
                Ok(Some(Ok(RegisterOutcome::Registered))) => {
                    tracing::info!("Registered the freenet:// link handler for this install");
                }
                Ok(Some(Ok(RegisterOutcome::ForeignDefault(other)))) => {
                    tracing::debug!(%other, "freenet:// links open with another application");
                }
                Ok(Some(Ok(RegisterOutcome::Skipped(reason)))) => {
                    tracing::debug!(reason, "freenet:// link handler not registered");
                }
                Ok(Some(Err(e))) => {
                    tracing::info!(error = %e, "Could not register the freenet:// link handler");
                }
                Err(_) => {
                    tracing::info!("freenet:// link handler registration panicked; ignored");
                }
            }
        });
    if let Err(e) = spawned {
        tracing::debug!(error = %e, "could not spawn the freenet:// registration thread");
    }
}

/// `freenet service url-handler ...`
#[derive(Subcommand, Debug, Clone)]
pub enum UrlHandlerCommand {
    /// Register this binary as the handler for freenet:// links.
    ///
    /// `freenet service install` already does this. Use it for installs
    /// without the service (Nix, cargo install, a hand-run binary).
    Register,
    /// Remove the freenet:// link handler registration.
    Unregister,
}

impl UrlHandlerCommand {
    pub fn run(&self) -> Result<()> {
        match self {
            UrlHandlerCommand::Register => {
                match register()? {
                    RegisterOutcome::Registered => {
                        println!("Registered freenet:// links to open with this binary.")
                    }
                    RegisterOutcome::AlreadyCurrent => {
                        println!("The freenet:// link handler is already registered.")
                    }
                    RegisterOutcome::ForeignDefault(other) => println!(
                        "freenet:// links are set to open with another application ({other}); \
                         left unchanged. Change it in your system's default-applications settings."
                    ),
                    RegisterOutcome::Skipped(reason) => println!("Not registered: {reason}."),
                }
                Ok(())
            }
            UrlHandlerCommand::Unregister => {
                if unregister()? {
                    println!("Removed the freenet:// link handler.");
                } else {
                    println!("No Freenet freenet:// link handler was registered.");
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_entry_quotes_path_and_passes_link_after_double_dash() {
        let entry = render_desktop_entry(Path::new("/home/u/.local/bin/freenet")).unwrap();
        assert!(entry.contains("\nExec=\"/home/u/.local/bin/freenet\" open -- %u\n"));
        assert!(entry.contains("\nMimeType=x-scheme-handler/freenet;\n"));
        assert!(entry.contains("\nNoDisplay=true\n"));
        assert!(entry.contains(DESKTOP_MARKER));
        assert!(entry.starts_with("[Desktop Entry]\n"));
        // Spaces are fine inside the quotes.
        let spaced = render_desktop_entry(Path::new("/opt/my apps/freenet")).unwrap();
        assert!(spaced.contains("Exec=\"/opt/my apps/freenet\" open -- %u"));
    }

    #[test]
    fn desktop_entry_refuses_paths_needing_escapes() {
        for bad in [
            "/tmp/a\"b/freenet",
            "/tmp/a`id`/freenet",
            "/tmp/$HOME/freenet",
            "/tmp/a\\b/freenet",
            "/tmp/100%/freenet",
            "/tmp/a\nExec=evil/freenet",
            "relative/freenet",
            "",
        ] {
            assert!(
                render_desktop_entry(Path::new(bad)).is_none(),
                "accepted {bad:?}"
            );
        }
    }

    #[test]
    fn default_action_respects_a_foreign_choice() {
        assert_eq!(decide_default_action(""), DefaultAction::SetOurs);
        assert_eq!(decide_default_action("  \n"), DefaultAction::SetOurs);
        assert_eq!(
            decide_default_action("freenet-url-handler.desktop\n"),
            DefaultAction::Keep
        );
        assert_eq!(
            decide_default_action("other.desktop\n"),
            DefaultAction::LeaveForeign("other.desktop".into())
        );
    }

    #[test]
    fn mimeapps_strip_removes_only_our_line() {
        let content = "[Default Applications]\n\
                       text/html=firefox.desktop\n\
                       x-scheme-handler/freenet=freenet-url-handler.desktop\n\
                       x-scheme-handler/magnet=qbittorrent.desktop\n\
                       \n[Added Associations]\n\
                       x-scheme-handler/freenet=freenet-url-handler.desktop;\n";
        let stripped = strip_mimeapps_association(content).unwrap();
        assert_eq!(
            stripped,
            "[Default Applications]\n\
             text/html=firefox.desktop\n\
             x-scheme-handler/magnet=qbittorrent.desktop\n\
             \n[Added Associations]\n"
        );
        assert_eq!(strip_mimeapps_association(&stripped), None);
        // A line naming another handler too is not ours to edit.
        let shared = "x-scheme-handler/freenet=freenet-url-handler.desktop;other.desktop;\n";
        assert_eq!(strip_mimeapps_association(shared), None);
        // No trailing newline on the last line.
        assert_eq!(
            strip_mimeapps_association("a=b\nx-scheme-handler/freenet=freenet-url-handler.desktop")
                .unwrap(),
            "a=b\n"
        );
    }

    #[test]
    fn windows_command_quotes_exe_and_link() {
        let exe = r"C:\Users\A B\AppData\Local\Freenet\bin\freenet.exe";
        let cmd = windows_open_command(exe);
        assert_eq!(cmd, format!("\"{exe}\" open -- \"%1\""));
        assert_eq!(windows_command_exe(&cmd), Some(exe));
        assert!(windows_command_is_ours(&cmd));
        assert!(windows_command_is_ours(r#""C:\x\FREENET.EXE" open "%1""#));
        assert!(!windows_command_is_ours(r#""C:\x\other.exe" "%1""#));
        assert!(!windows_command_is_ours(r#"C:\x\freenet.exe open "%1""#));
        assert!(!windows_command_is_ours(""));
    }
}
