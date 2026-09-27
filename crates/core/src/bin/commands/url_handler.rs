//! Registration of the `freenet://` URL scheme with the operating system (#5726).
//!
//! What gets registered, per platform:
//!
//! * **Linux**: `~/.local/share/applications/freenet-url-handler.desktop` with
//!   `MimeType=x-scheme-handler/freenet;` and `Exec=<bin> open -- %u`. Then
//!   `xdg-mime default` makes it the handler, but only when `xdg-mime` reports
//!   that no handler is set (a handler the user chose is never replaced).
//! * **Windows**: `HKCU\Software\Classes\freenet` with `URL Protocol` and
//!   `shell\open\command` = `"<exe>" open -- "%1"`. HKCU, so no admin rights.
//! * **macOS**: nothing at runtime. The `Freenet.app` bundle declares the scheme
//!   in its `Info.plist` (`scripts/package-macos.sh`) and LaunchServices
//!   delivers links to the running menu-bar wrapper (`tray.rs`), launching the
//!   app first if it is not running. A bare binary outside a bundle cannot
//!   receive URLs on macOS.
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
//! `ExecStart`, or the Windows Run key), never after the user ran
//! `freenet service url-handler unregister`, does nothing when the
//! registration is already current, and can never fail or delay the node.
//!
//! `scripts/uninstall.sh` repeats the Linux removal rules (the marker line and
//! the exact `mimeapps.list` line) for installs whose binary is already gone;
//! change both together.

#[cfg(any(target_os = "linux", test))]
use std::path::Path;
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

/// Name of the marker file that records "the user unregistered the handler;
/// do not put it back on start". Lives in `<data_local_dir>/freenet/`.
const OPT_OUT_MARKER: &str = ".url-handler-opt-out";

/// What a registration attempt did.
#[cfg_attr(target_os = "macos", allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOutcome {
    /// The handler was written or updated.
    Registered,
    /// Everything was already current; nothing was written.
    AlreadyCurrent,
    /// Another application is the default handler for `freenet://`; left as
    /// the user chose it.
    ForeignDefault(String),
    /// Not applicable on this system (reason given).
    Skipped(&'static str),
}

/// Why registration is happening.
#[cfg_attr(target_os = "macos", allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterMode {
    /// `freenet service url-handler register`: the only mode that clears the
    /// opt-out. Always re-checks the default handler.
    UserRequested,
    /// `freenet service install` (also run by install.sh, the Windows setup
    /// wizard and `service doctor`): honours the opt-out, always re-checks
    /// the default handler.
    Install,
    /// The node starting: honours the opt-out, and on Linux only consults
    /// `xdg-mime` when the desktop entry had to be (re)written, so a
    /// crash-looping node does not spawn helpers on every restart.
    OnStart,
}

/// Whether the opt-out marker at `marker` is present.
fn opted_out_at(marker: &std::path::Path) -> bool {
    marker.exists()
}

/// Write the opt-out marker at `marker`.
fn write_opt_out_marker_at(marker: &std::path::Path) -> Result<()> {
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(marker, b"freenet service url-handler unregister\n")?;
    Ok(())
}

/// Remove the opt-out marker at `marker` (absent is fine).
fn clear_opt_out_marker_at(marker: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(marker) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(target_os = "linux")]
fn opt_out_marker_in(data_local_dir: &std::path::Path) -> PathBuf {
    data_local_dir.join("freenet").join(OPT_OUT_MARKER)
}

// ── Linux ───────────────────────────────────────────────────────────────────

#[cfg(any(target_os = "linux", test))]
/// Characters that cannot be represented safely in a desktop-entry `Exec` key
/// without the spec's two-level escaping or quoting (the spec's reserved set,
/// plus `%`). Paths containing them are refused rather than escaped: the
/// simplest escape that is obviously correct is none. A space is allowed and
/// handled by quoting.
fn exec_path_is_representable(path: &str) -> bool {
    !path.is_empty()
        && path.starts_with('/')
        && !path.chars().any(|c| {
            c.is_control()
                || matches!(
                    c,
                    '"' | '`'
                        | '$'
                        | '\\'
                        | '%'
                        | '\''
                        | '>'
                        | '<'
                        | '~'
                        | '|'
                        | '&'
                        | ';'
                        | '*'
                        | '?'
                        | '#'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '\t'
                )
        })
}

#[cfg(any(target_os = "linux", test))]
/// A path as one `Exec` argument: quoted only if it contains a space; `None`
/// if it cannot be represented (see [`exec_path_is_representable`]).
fn exec_arg(path: &Path) -> Option<String> {
    let path = path.to_str()?;
    if !exec_path_is_representable(path) {
        return None;
    }
    Some(if path.contains(' ') {
        format!("\"{path}\"")
    } else {
        path.to_string()
    })
}

#[cfg(target_os = "linux")]
/// Whether an existing desktop entry (ours) was registered for `binary` WITH a
/// `--config-dir` (`url-handler register --config-dir`). Node start must not
/// replace that explicit choice with the default-config entry.
fn entry_has_config_dir_for(entry: &str, binary: &Path) -> bool {
    exec_arg(binary).is_some_and(|program| {
        entry
            .lines()
            .any(|l| l.starts_with(&format!("Exec={program} --config-dir ")))
    })
}

#[cfg(any(target_os = "linux", test))]
/// Render the desktop entry for `binary` (and, for a node with a non-default
/// config, its `--config-dir`). Returns `None` for a path that cannot be
/// written into `Exec` safely.
///
/// The link is passed as `%u` after a literal `--`: the launcher substitutes
/// `%u` as ONE argv entry with no shell, and `--` stops the link being read as
/// a flag. The path is quoted only when it contains a space. The spec allows
/// quoting always, but xdg-utils 1.1.3's generic `xdg-open` (still Ubuntu
/// 24.04's, and used under window managers it does not recognise) takes the
/// quote characters as part of the program name and fails; verified in a
/// container, where the quoted form exited 4 and the unquoted one opened the
/// link.
pub fn render_desktop_entry(binary: &Path, config_dir: Option<&Path>) -> Option<String> {
    let mut program = exec_arg(binary)?;
    if let Some(dir) = config_dir {
        program = format!("{program} --config-dir {}", exec_arg(dir)?);
    }
    Some(format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=Freenet\n\
         Comment=Open freenet:// links with your local Freenet peer\n\
         Exec={program} open -- %u\n\
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
    /// The query failed or timed out, so the current default is unknown:
    /// change nothing rather than risk replacing a user's choice.
    Unknown,
}

#[cfg(any(target_os = "linux", test))]
/// Pure decision on the `xdg-mime query default` output (`None` = the query
/// did not succeed).
pub fn decide_default_action(current_default: Option<&str>) -> DefaultAction {
    let Some(current) = current_default.map(str::trim) else {
        return DefaultAction::Unknown;
    };
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
    use std::ffi::OsString;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    /// Upper bound on any helper (`xdg-mime`, `update-desktop-database`).
    const HELPER_TIMEOUT: Duration = Duration::from_secs(5);

    /// The directories and `PATH` registration works against. Built from the
    /// process environment in production and from a temp tree in tests.
    pub(super) struct LinuxEnv {
        pub home: PathBuf,
        pub data_home: PathBuf,
        pub config_home: PathBuf,
        pub path: OsString,
        pub helper_timeout: Duration,
    }

    fn absolute_env_dir(var: &str) -> Option<PathBuf> {
        std::env::var_os(var)
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
    }

    impl LinuxEnv {
        pub(super) fn from_process() -> Option<Self> {
            let home = dirs::home_dir()?;
            Some(LinuxEnv {
                data_home: absolute_env_dir("XDG_DATA_HOME")
                    .unwrap_or_else(|| home.join(".local/share")),
                config_home: absolute_env_dir("XDG_CONFIG_HOME")
                    .unwrap_or_else(|| home.join(".config")),
                path: std::env::var_os("PATH").unwrap_or_default(),
                helper_timeout: HELPER_TIMEOUT,
                home,
            })
        }

        fn applications_dir(&self) -> PathBuf {
            self.data_home.join("applications")
        }

        fn desktop_file(&self) -> PathBuf {
            self.applications_dir().join(DESKTOP_FILE_NAME)
        }

        /// Every `mimeapps.list` an association may have been written to:
        /// the current XDG location, and the one older xdg-utils used.
        fn mimeapps_lists(&self) -> [PathBuf; 2] {
            [
                self.config_home.join("mimeapps.list"),
                self.applications_dir().join("mimeapps.list"),
            ]
        }

        /// Where the opt-out marker is written, then every place it is
        /// honoured: the node's data dir under `XDG_DATA_HOME`, and the
        /// default `~/.local/share/freenet`, because a shell that sets
        /// `XDG_DATA_HOME` and a systemd user manager that does not would
        /// otherwise disagree about where it is.
        pub(super) fn opt_out_markers(&self) -> Vec<PathBuf> {
            let mut markers = vec![opt_out_marker_in(&self.data_home)];
            let default = opt_out_marker_in(&self.home.join(".local/share"));
            if !markers.contains(&default) {
                markers.push(default);
            }
            markers
        }

        fn find(&self, program: &str) -> Option<PathBuf> {
            std::env::split_paths(&self.path)
                .map(|dir| dir.join(program))
                .find(|candidate| {
                    std::fs::metadata(candidate).is_ok_and(|m| {
                        use std::os::unix::fs::PermissionsExt;
                        m.is_file() && m.permissions().mode() & 0o111 != 0
                    })
                })
        }

        /// Run a helper found on this env's `PATH`, with null stdin/stderr,
        /// killed after `helper_timeout`. Stdout goes to an anonymous temp
        /// file rather than a pipe, so a daemon the helper leaves behind
        /// holding stdout open cannot make the read block. `None` if it could
        /// not run, failed, or timed out.
        fn run_helper(&self, program: &str, args: &[&str]) -> Option<String> {
            let exe = self.find(program)?;
            let mut out = tempfile::tempfile().ok()?;
            let mut cmd = Command::new(exe);
            // The node runs with umask 077; helpers such as xdg-mime rewrite
            // shared files like mimeapps.list and must not leave them 0600.
            // SAFETY: umask(2) is async-signal-safe and touches no memory.
            unsafe {
                use std::os::unix::process::CommandExt;
                cmd.pre_exec(|| {
                    libc::umask(0o022);
                    Ok(())
                });
            }
            let mut child = cmd
                .args(args)
                .env("PATH", &self.path)
                .env("HOME", &self.home)
                .env("XDG_DATA_HOME", &self.data_home)
                .env("XDG_CONFIG_HOME", &self.config_home)
                .stdin(Stdio::null())
                .stdout(Stdio::from(out.try_clone().ok()?))
                .stderr(Stdio::null())
                .spawn()
                .ok()?;
            let deadline = Instant::now() + self.helper_timeout;
            let status = loop {
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    _ => {
                        drop(child.kill());
                        drop(child.wait());
                        return None;
                    }
                }
            };
            if !status.success() {
                return None;
            }
            use std::io::{Read, Seek};
            out.rewind().ok()?;
            let mut text = String::new();
            out.read_to_string(&mut text).ok()?;
            Some(text)
        }
    }

    fn is_ours(desktop_entry: &str) -> bool {
        desktop_entry.lines().any(|l| l == DESKTOP_MARKER)
    }

    /// Write `contents` to `path`, following a symlink so a dotfile-managed
    /// `mimeapps.list` stays a symlink to the (updated) real file, and keeping
    /// the file's permissions (the temp file would otherwise leave it 0600).
    fn write_through_symlink(path: &Path, contents: &[u8]) -> Result<()> {
        let target = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let permissions = std::fs::metadata(&target).ok().map(|m| m.permissions());
        super::super::open_link::write_atomically(&target, contents)?;
        if let Some(permissions) = permissions {
            std::fs::set_permissions(&target, permissions)?;
        }
        Ok(())
    }

    /// Write or refresh the desktop entry and, when no handler is set, make it
    /// the default. Idempotent.
    pub(super) fn register(
        env: &LinuxEnv,
        binary: &Path,
        config_dir: Option<&Path>,
        mode: RegisterMode,
    ) -> Result<RegisterOutcome> {
        if env.find("xdg-open").is_none() {
            // The handler opens the browser with xdg-open; without it (a
            // headless server, typically) registering would only create a
            // handler that cannot work.
            return Ok(RegisterOutcome::Skipped(
                "xdg-open is not installed (no desktop environment)",
            ));
        }
        let Some(entry) = render_desktop_entry(binary, config_dir) else {
            return Ok(RegisterOutcome::Skipped(
                "the binary or config path cannot be written into a desktop entry",
            ));
        };
        let dir = env.applications_dir();
        let path = env.desktop_file();

        let existing = match std::fs::read_to_string(&path) {
            Ok(content) => Some(content),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            // Unreadable (permissions, I/O): we cannot tell whose it is, so
            // leave it alone rather than overwrite it.
            Err(_) => {
                return Ok(RegisterOutcome::Skipped(
                    "freenet-url-handler.desktop exists but cannot be read",
                ));
            }
        };
        if existing.as_deref().is_some_and(|c| !is_ours(c)) {
            // Something else put a file at our path: not ours to replace.
            return Ok(RegisterOutcome::Skipped(
                "an unrelated freenet-url-handler.desktop is already installed",
            ));
        }
        if mode == RegisterMode::OnStart
            && config_dir.is_none()
            && existing
                .as_deref()
                .is_some_and(|c| entry_has_config_dir_for(c, binary))
        {
            return Ok(RegisterOutcome::AlreadyCurrent);
        }
        let entry_changed = existing.as_deref() != Some(&entry);
        if entry_changed {
            std::fs::create_dir_all(&dir)?;
            super::super::open_link::write_atomically(&path, entry.as_bytes())?;
            let dir_str = dir.to_string_lossy();
            drop(env.run_helper("update-desktop-database", &["-q", &dir_str]));
        }

        // On start, an unchanged entry means this already ran for this
        // binary; skip the helper rather than spawn it on every restart.
        if mode == RegisterMode::OnStart && !entry_changed {
            return Ok(RegisterOutcome::AlreadyCurrent);
        }

        // `xdg-mime query default` reports the configured default, or, when
        // none is configured, whatever its fallback resolution picks (any app
        // claiming the scheme). Either way an answer other than ours is left
        // alone: it is safer to under-claim than to take over.
        let default_changed = if env.find("xdg-mime").is_some() {
            let current = env.run_helper("xdg-mime", &["query", "default", SCHEME_MIME_TYPE]);
            match decide_default_action(current.as_deref()) {
                DefaultAction::SetOurs => {
                    env.run_helper(
                        "xdg-mime",
                        &["default", DESKTOP_FILE_NAME, SCHEME_MIME_TYPE],
                    )
                    .ok_or_else(|| anyhow::anyhow!("xdg-mime default failed"))?;
                    true
                }
                DefaultAction::Keep | DefaultAction::Unknown => false,
                DefaultAction::LeaveForeign(other) => {
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

    /// Remove our desktop entry and our `mimeapps.list` lines. Returns
    /// whether anything was removed.
    pub(super) fn unregister(env: &LinuxEnv) -> Result<bool> {
        let mut removed = false;
        let path = env.desktop_file();
        match std::fs::read_to_string(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                // Unreadable: cannot establish ownership, so touch nothing.
                anyhow::bail!("could not read {} ({e}); left it unchanged", path.display());
            }
            Ok(content) if is_ours(&content) => {
                std::fs::remove_file(&path)?;
                removed = true;
                let dir = env.applications_dir();
                let dir_str = dir.to_string_lossy();
                drop(env.run_helper("update-desktop-database", &["-q", &dir_str]));
            }
            // An unmarked file at our path belongs to someone else, and so
            // does the association naming it: leave both.
            Ok(_) => return Ok(false),
            Err(_) => {}
        }
        for list in env.mimeapps_lists() {
            if let Ok(content) = std::fs::read_to_string(&list) {
                if let Some(stripped) = strip_mimeapps_association(&content) {
                    write_through_symlink(&list, stripped.as_bytes())?;
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
    pub(super) fn is_managed_install(env: &LinuxEnv, binary: &Path) -> bool {
        let unit = env.home.join(".config/systemd/user/freenet.service");
        let Ok(content) = std::fs::read_to_string(unit) else {
            return false;
        };
        let expected = format!("ExecStart={} network", binary.display());
        content.lines().any(|line| line == expected)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// A fake home with stub `xdg-open`, `xdg-mime` and
        /// `update-desktop-database` on its own `PATH`. The `xdg-mime` stub
        /// answers `query default` from a file and logs every call. `PATH`
        /// holds ONLY the stubs, so a real xdg-open on the test machine cannot
        /// leak in; the stubs therefore call other tools by absolute path.
        struct Fixture {
            _dir: tempfile::TempDir,
            env: LinuxEnv,
            bin: PathBuf,
        }

        impl Fixture {
            fn new(tools: &[&str]) -> Self {
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().to_path_buf();
                let bin = root.join("bin");
                std::fs::create_dir_all(&bin).unwrap();
                for tool in tools {
                    let script = match *tool {
                        "xdg-mime" => format!(
                            "#!/bin/sh\necho \"$@\" >> '{log}'\n\
                             if [ \"$1\" = query ]; then\n\
                               [ -f '{fail}' ] && exit 1\n\
                               /bin/cat '{default}' 2>/dev/null\n\
                               exit 0\n\
                             else\n\
                               echo \"$2\" > '{default}'\nfi\n",
                            log = root.join("xdg-mime.log").display(),
                            fail = root.join("query-fails").display(),
                            default = root.join("default").display(),
                        ),
                        _ => "#!/bin/sh\nexit 0\n".to_string(),
                    };
                    let path = bin.join(tool);
                    std::fs::write(&path, script).unwrap();
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                        .unwrap();
                }
                let home = root.join("home");
                std::fs::create_dir_all(&home).unwrap();
                Fixture {
                    env: LinuxEnv {
                        data_home: home.join(".local/share"),
                        config_home: home.join(".config"),
                        path: bin.clone().into_os_string(),
                        helper_timeout: HELPER_TIMEOUT,
                        home,
                    },
                    bin,
                    _dir: dir,
                }
            }

            fn root(&self) -> &Path {
                self.bin.parent().unwrap()
            }

            fn set_default(&self, value: &str) {
                std::fs::write(self.root().join("default"), value).unwrap();
            }

            fn default(&self) -> String {
                std::fs::read_to_string(self.root().join("default")).unwrap_or_default()
            }

            fn xdg_mime_calls(&self) -> usize {
                std::fs::read_to_string(self.root().join("xdg-mime.log"))
                    .map(|s| s.lines().count())
                    .unwrap_or(0)
            }
        }

        const BIN: &str = "/home/u/.local/bin/freenet";
        const ALL: &[&str] = &["xdg-open", "xdg-mime", "update-desktop-database"];

        #[test]
        fn register_writes_entry_sets_default_and_is_idempotent() {
            let f = Fixture::new(ALL);
            let out = register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert_eq!(out, RegisterOutcome::Registered);
            let entry = std::fs::read_to_string(f.env.desktop_file()).unwrap();
            assert!(entry.contains(&format!("\nExec={BIN} open -- %u\n")));
            assert_eq!(f.default().trim(), DESKTOP_FILE_NAME);

            let again = register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert_eq!(again, RegisterOutcome::AlreadyCurrent);
        }

        #[test]
        fn on_start_with_current_entry_runs_no_helper() {
            let f = Fixture::new(ALL);
            register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            let calls = f.xdg_mime_calls();
            let out = register(&f.env, Path::new(BIN), None, RegisterMode::OnStart).unwrap();
            assert_eq!(out, RegisterOutcome::AlreadyCurrent);
            assert_eq!(
                f.xdg_mime_calls(),
                calls,
                "xdg-mime ran on an unchanged start"
            );
        }

        #[test]
        fn a_moved_binary_rewrites_the_entry() {
            let f = Fixture::new(ALL);
            register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            let out = register(
                &f.env,
                Path::new("/opt/freenet/freenet"),
                None,
                RegisterMode::OnStart,
            )
            .unwrap();
            assert_eq!(out, RegisterOutcome::Registered);
            let entry = std::fs::read_to_string(f.env.desktop_file()).unwrap();
            assert!(entry.contains("Exec=/opt/freenet/freenet open -- %u"));
        }

        #[test]
        fn a_foreign_default_is_left_alone() {
            let f = Fixture::new(ALL);
            f.set_default("other.desktop\n");
            let out = register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert_eq!(out, RegisterOutcome::ForeignDefault("other.desktop".into()));
            assert_eq!(f.default().trim(), "other.desktop");
        }

        #[test]
        fn a_failed_query_does_not_replace_the_default() {
            let f = Fixture::new(ALL);
            f.set_default("other.desktop\n");
            std::fs::write(f.root().join("query-fails"), "").unwrap();
            register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert_eq!(f.default().trim(), "other.desktop");
        }

        #[test]
        fn without_xdg_open_nothing_is_written() {
            let f = Fixture::new(&["xdg-mime"]);
            let out = register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert!(matches!(out, RegisterOutcome::Skipped(_)));
            assert!(!f.env.desktop_file().exists());
        }

        #[test]
        fn unregister_removes_ours_from_every_mimeapps_list_and_keeps_the_rest() {
            let f = Fixture::new(ALL);
            register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            let line = format!("{SCHEME_MIME_TYPE}={DESKTOP_FILE_NAME}\n");
            let [current, legacy] = f.env.mimeapps_lists();
            std::fs::create_dir_all(current.parent().unwrap()).unwrap();
            std::fs::write(
                &current,
                format!("[Default Applications]\n{line}a/b=c.desktop\n"),
            )
            .unwrap();
            std::fs::write(&legacy, format!("[Default Applications]\n{line}")).unwrap();

            assert!(unregister(&f.env).unwrap());
            assert!(!f.env.desktop_file().exists());
            assert_eq!(
                std::fs::read_to_string(&current).unwrap(),
                "[Default Applications]\na/b=c.desktop\n"
            );
            assert_eq!(
                std::fs::read_to_string(&legacy).unwrap(),
                "[Default Applications]\n"
            );
            assert!(
                !unregister(&f.env).unwrap(),
                "second unregister found something"
            );
        }

        #[test]
        fn unregister_writes_through_a_symlinked_mimeapps_list() {
            let f = Fixture::new(ALL);
            let real = f.root().join("dotfiles-mimeapps.list");
            std::fs::write(
                &real,
                format!("{SCHEME_MIME_TYPE}={DESKTOP_FILE_NAME}\nx=y\n"),
            )
            .unwrap();
            let [current, _] = f.env.mimeapps_lists();
            std::fs::create_dir_all(current.parent().unwrap()).unwrap();
            std::os::unix::fs::symlink(&real, &current).unwrap();
            assert!(unregister(&f.env).unwrap());
            assert!(
                std::fs::symlink_metadata(&current)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            assert_eq!(std::fs::read_to_string(&real).unwrap(), "x=y\n");
        }

        #[test]
        fn an_unmarked_entry_and_its_association_are_never_touched() {
            let f = Fixture::new(ALL);
            std::fs::create_dir_all(f.env.applications_dir()).unwrap();
            let foreign = "[Desktop Entry]\nName=Other\n";
            std::fs::write(f.env.desktop_file(), foreign).unwrap();
            let [current, _] = f.env.mimeapps_lists();
            std::fs::create_dir_all(current.parent().unwrap()).unwrap();
            let list = format!("{SCHEME_MIME_TYPE}={DESKTOP_FILE_NAME}\n");
            std::fs::write(&current, &list).unwrap();

            let out = register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            assert!(matches!(out, RegisterOutcome::Skipped(_)), "{out:?}");
            assert_eq!(
                std::fs::read_to_string(f.env.desktop_file()).unwrap(),
                foreign
            );

            assert!(!unregister(&f.env).unwrap());
            assert_eq!(
                std::fs::read_to_string(f.env.desktop_file()).unwrap(),
                foreign
            );
            assert_eq!(std::fs::read_to_string(&current).unwrap(), list);
        }

        /// The node runs with umask 077; helpers must run with 022 so files
        /// they write (mimeapps.list) stay readable as before.
        #[test]
        fn helpers_run_with_a_normal_umask() {
            let f = Fixture::new(&["xdg-open"]);
            let probe = f.bin.join("print-umask");
            std::fs::write(&probe, "#!/bin/sh\numask\n").unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
            assert_eq!(f.env.run_helper("print-umask", &[]).unwrap().trim(), "0022");
        }

        #[test]
        fn node_start_keeps_an_explicit_config_dir_registration() {
            let f = Fixture::new(ALL);
            register(
                &f.env,
                Path::new(BIN),
                Some(Path::new("/srv/cfg")),
                RegisterMode::UserRequested,
            )
            .unwrap();
            let out = register(&f.env, Path::new(BIN), None, RegisterMode::OnStart).unwrap();
            assert_eq!(out, RegisterOutcome::AlreadyCurrent);
            let entry = std::fs::read_to_string(f.env.desktop_file()).unwrap();
            assert!(entry.contains(&format!("Exec={BIN} --config-dir /srv/cfg open -- %u")));
            // An install (explicit, default config) does replace it.
            register(&f.env, Path::new(BIN), None, RegisterMode::Install).unwrap();
            let entry = std::fs::read_to_string(f.env.desktop_file()).unwrap();
            assert!(entry.contains(&format!("Exec={BIN} open -- %u")));
        }

        #[test]
        fn a_hanging_helper_is_killed_at_the_timeout() {
            let mut f = Fixture::new(&["xdg-open"]);
            let slow = f.bin.join("slow-helper");
            std::fs::write(&slow, "#!/bin/sh\nexec /bin/sleep 30\n").unwrap();
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&slow, std::fs::Permissions::from_mode(0o755)).unwrap();
            f.env.helper_timeout = Duration::from_millis(300);
            let started = Instant::now();
            assert_eq!(f.env.run_helper("slow-helper", &[]), None);
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "{:?}",
                started.elapsed()
            );
        }

        #[test]
        fn opt_out_marker_lives_in_the_data_dir_and_round_trips() {
            let f = Fixture::new(ALL);
            let markers = f.env.opt_out_markers();
            let marker = markers[0].clone();
            assert!(marker.starts_with(&f.env.data_home));
            assert!(
                markers.contains(&f.env.home.join(".local/share/freenet/.url-handler-opt-out")),
                "the default location is always honoured"
            );
            assert!(!opted_out_at(&marker));
            write_opt_out_marker_at(&marker).unwrap();
            assert!(opted_out_at(&marker));
            clear_opt_out_marker_at(&marker).unwrap();
            assert!(!opted_out_at(&marker));
            clear_opt_out_marker_at(&marker).unwrap();
        }

        #[test]
        fn managed_install_matches_only_the_generated_unit_line() {
            let f = Fixture::new(ALL);
            let unit_dir = f.env.home.join(".config/systemd/user");
            std::fs::create_dir_all(&unit_dir).unwrap();
            let unit = unit_dir.join("freenet.service");
            let log_dir = f.env.home.join(".local/state/freenet");
            let generated =
                super::super::super::service::generate_user_service_file(Path::new(BIN), &log_dir);
            std::fs::write(&unit, &generated).unwrap();
            assert!(is_managed_install(&f.env, Path::new(BIN)));
            assert!(!is_managed_install(
                &f.env,
                Path::new("/tmp/target/debug/freenet")
            ));

            std::fs::write(
                &unit,
                format!("[Service]\nExecStart={BIN} network --id x\n"),
            )
            .unwrap();
            assert!(
                !is_managed_install(&f.env, Path::new(BIN)),
                "hand-edited unit"
            );
            std::fs::remove_file(&unit).unwrap();
            assert!(!is_managed_install(&f.env, Path::new(BIN)), "no unit");
        }
    }
}

// ── Windows ─────────────────────────────────────────────────────────────────

/// The `shell\open\command` value for `exe`: the exe path quoted, the link as
/// `"%1"` after a literal `--`.
#[cfg(any(target_os = "windows", test))]
pub fn windows_open_command(exe: &str, config_dir: Option<&str>) -> String {
    match config_dir {
        Some(dir) => {
            // In a quoted argument, backslashes before the closing quote
            // escape it; doubling a trailing backslash (a drive root, `C:\`)
            // keeps it literal.
            let dir = if dir.ends_with('\\') {
                format!("{dir}\\")
            } else {
                dir.to_string()
            };
            format!("\"{exe}\" --config-dir \"{dir}\" open -- \"%1\"")
        }
        None => format!("\"{exe}\" open -- \"%1\""),
    }
}

/// Whether `exe` can go into a registry command line. Explorer expands `%1`,
/// `%*`, `%L` and friends ANYWHERE in the string, so a `%` in the path would
/// splice the link into the executable path; a `"` would end the quoting.
#[cfg(any(target_os = "windows", test))]
pub fn windows_exe_is_representable(exe: &str) -> bool {
    !exe.is_empty() && !exe.contains(['"', '%']) && !exe.chars().any(char::is_control)
}

/// The quoted executable at the start of a registry command line, if any.
#[cfg(any(target_os = "windows", test))]
pub fn windows_command_exe(command: &str) -> Option<&str> {
    let rest = command.trim_start().strip_prefix('"')?;
    rest.split_once('"').map(|(exe, _)| exe)
}

/// Registry value (under [`WINDOWS_CLASS_KEY`]) marking a registration
/// Freenet wrote, the Windows counterpart of the desktop entry's marker line.
/// Only a key carrying it is ever overwritten or removed.
#[cfg(target_os = "windows")]
pub const WINDOWS_MARKER_VALUE: &str = "FreenetManaged";

#[cfg(target_os = "windows")]
mod windows {
    use super::*;
    use winreg::RegKey;
    use winreg::enums::HKEY_CURRENT_USER;

    fn class_key() -> Option<RegKey> {
        RegKey::predef(HKEY_CURRENT_USER)
            .open_subkey(WINDOWS_CLASS_KEY)
            .ok()
    }

    fn current_command() -> Option<String> {
        class_key()?
            .open_subkey(r"shell\open\command")
            .ok()?
            .get_value::<String, _>("")
            .ok()
    }

    fn has_url_protocol_value() -> bool {
        class_key().is_some_and(|k| k.get_value::<String, _>("URL Protocol").is_ok())
    }

    /// Whether the registration was written by Freenet (carries the marker).
    fn is_ours() -> bool {
        class_key().is_some_and(|k| k.get_value::<String, _>(WINDOWS_MARKER_VALUE).is_ok())
    }

    pub(super) fn register(
        exe: &std::path::Path,
        config_dir: Option<&std::path::Path>,
        mode: RegisterMode,
    ) -> Result<RegisterOutcome> {
        let mode_keeps_config_dir = mode == RegisterMode::OnStart && config_dir.is_none();
        let exe = exe
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("executable path is not valid UTF-8"))?;
        let config_dir = match config_dir {
            Some(d) => Some(
                d.to_str()
                    .ok_or_else(|| anyhow::anyhow!("config dir is not valid UTF-8"))?,
            ),
            None => None,
        };
        if !windows_exe_is_representable(exe)
            || config_dir.is_some_and(|d| !windows_exe_is_representable(d))
        {
            return Ok(RegisterOutcome::Skipped(
                "a path contains a quote or percent sign",
            ));
        }
        let command = windows_open_command(exe, config_dir);
        let ours = is_ours();
        if class_key().is_some() && !ours {
            // A key we did not write (another application's, even one with
            // no `open` command): leave it.
            return Ok(RegisterOutcome::ForeignDefault(
                current_command().unwrap_or_else(|| WINDOWS_CLASS_KEY.to_string()),
            ));
        }
        match current_command() {
            Some(existing) if existing == command && has_url_protocol_value() => {
                return Ok(RegisterOutcome::AlreadyCurrent);
            }
            // Node start must not replace an explicit `--config-dir`
            // registration for this same exe with the default-config one.
            Some(existing)
                if mode_keeps_config_dir
                    && existing.starts_with(&format!("\"{exe}\" --config-dir "))
                    && has_url_protocol_value() =>
            {
                return Ok(RegisterOutcome::AlreadyCurrent);
            }
            _ => {}
        }
        let hkcu = RegKey::predef(HKEY_CURRENT_USER);
        let (class, _) = hkcu.create_subkey(WINDOWS_CLASS_KEY)?;
        // Marker FIRST: if a later write fails, the half-written key is still
        // recognised as ours, so the next start repairs it and unregister can
        // remove it.
        class.set_value(WINDOWS_MARKER_VALUE, &"1")?;
        class.set_value("", &"URL:Freenet Protocol")?;
        class.set_value("URL Protocol", &"")?;
        let (icon, _) = class.create_subkey("DefaultIcon")?;
        icon.set_value("", &format!("\"{exe}\",0"))?;
        let (cmd, _) = class.create_subkey(r"shell\open\command")?;
        cmd.set_value("", &command)?;
        Ok(RegisterOutcome::Registered)
    }

    pub(super) fn unregister() -> Result<bool> {
        if !is_ours() {
            return Ok(false);
        }
        RegKey::predef(HKEY_CURRENT_USER).delete_subkey_all(WINDOWS_CLASS_KEY)?;
        Ok(true)
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

    /// Runs only under `CI` (a throwaway runner): it writes the real
    /// `HKCU\...\Run\Freenet` value, restoring whatever was there.
    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn managed_install_follows_the_run_key() {
            if std::env::var_os("CI").is_none() {
                eprintln!("skipped: writes the real HKCU Run key; set CI=1 to run");
                return;
            }
            let (run, _) = RegKey::predef(HKEY_CURRENT_USER)
                .create_subkey(r"Software\Microsoft\Windows\CurrentVersion\Run")
                .unwrap();
            let previous = run.get_value::<String, _>("Freenet").ok();
            let exe = std::path::Path::new(r"C:\Users\T\AppData\Local\Freenet\bin\freenet.exe");
            run.set_value(
                "Freenet",
                &format!("\"{}\" service run-wrapper", exe.display()),
            )
            .unwrap();
            let managed = is_managed_install(exe);
            let other =
                is_managed_install(std::path::Path::new(r"C:\dev\target\debug\freenet.exe"));
            let case_insensitive = is_managed_install(std::path::Path::new(
                r"c:\users\t\appdata\local\freenet\bin\FREENET.EXE",
            ));
            match previous {
                Some(v) => run.set_value("Freenet", &v).unwrap(),
                None => drop(run.delete_value("Freenet")),
            }
            assert!(managed);
            assert!(!other);
            assert!(case_insensitive);
        }
    }
}

// ── Platform dispatch ───────────────────────────────────────────────────────

/// The opt-out marker for this user, if a data directory can be found.
/// The opt-out marker locations: written to all, honoured at any. They sit in
/// the node's data directory, so `uninstall --purge` removes them.
fn opt_out_markers() -> Vec<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        linux::LinuxEnv::from_process()
            .map(|env| env.opt_out_markers())
            .unwrap_or_default()
    }
    #[cfg(not(target_os = "linux"))]
    {
        directories::ProjectDirs::from("", "The Freenet Project Inc", "Freenet")
            .map(|d| vec![d.data_local_dir().join(OPT_OUT_MARKER)])
            .unwrap_or_default()
    }
}

/// What the opt-out marker means for a registration in `mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarkerAction {
    /// Remove the marker, then register.
    Clear,
    /// The user turned the handler off: do nothing.
    Skip,
    /// Register.
    Proceed,
}

/// Pure decision, so it is unit-tested.
fn marker_action(mode: RegisterMode, opted_out: bool) -> MarkerAction {
    match (mode, opted_out) {
        (RegisterMode::UserRequested, _) => MarkerAction::Clear,
        (_, true) => MarkerAction::Skip,
        (_, false) => MarkerAction::Proceed,
    }
}

const OPTED_OUT: &str = "turned off with `freenet service url-handler unregister`";

/// Register the handler for the running binary. Idempotent. `config_dir` is
/// passed to the handler as `--config-dir` (only for `url-handler register
/// --config-dir`; installs and node start use the default config).
pub fn register(
    mode: RegisterMode,
    config_dir: Option<&std::path::Path>,
) -> Result<RegisterOutcome> {
    register_with(
        mode,
        &opt_out_markers(),
        || register_platform(mode, config_dir),
        unregister,
    )
}

/// [`register`] with the platform steps injected, so the opt-out handling
/// (including the race undo) is unit-tested.
fn register_with(
    mode: RegisterMode,
    markers: &[PathBuf],
    platform: impl FnOnce() -> Result<RegisterOutcome>,
    undo: impl FnOnce() -> Result<bool>,
) -> Result<RegisterOutcome> {
    let opted_out = || markers.iter().any(|m| opted_out_at(m));
    match marker_action(mode, opted_out()) {
        MarkerAction::Clear => {
            for m in markers {
                clear_opt_out_marker_at(m)?;
            }
        }
        MarkerAction::Skip => return Ok(RegisterOutcome::Skipped(OPTED_OUT)),
        MarkerAction::Proceed => {}
    }
    let outcome = platform();
    // `unregister` writes the marker BEFORE removing anything, so if one ran
    // while this was registering, the marker is visible now. Undo whatever
    // this wrote (whatever it returned: Linux writes the desktop entry before
    // it can fail or find a foreign default), so the user's opt-out wins the
    // race. `unregister` only removes Freenet's own files, so running it when
    // nothing was written is harmless.
    if mode != RegisterMode::UserRequested && opted_out() {
        undo()?;
        return Ok(RegisterOutcome::Skipped(OPTED_OUT));
    }
    outcome
}

fn register_platform(
    mode: RegisterMode,
    config_dir: Option<&std::path::Path>,
) -> Result<RegisterOutcome> {
    #[cfg(target_os = "linux")]
    {
        let env = linux::LinuxEnv::from_process()
            .ok_or_else(|| anyhow::anyhow!("could not determine the home directory"))?;
        linux::register(&env, &std::env::current_exe()?, config_dir, mode)
    }
    #[cfg(target_os = "windows")]
    {
        windows::register(&std::env::current_exe()?, config_dir, mode)
    }
    #[cfg(target_os = "macos")]
    {
        let _ = (mode, config_dir);
        Ok(RegisterOutcome::Skipped(
            "on macOS the Freenet.app bundle registers freenet:// itself",
        ))
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        let _ = (mode, config_dir);
        Ok(RegisterOutcome::Skipped("not supported on this platform"))
    }
}

/// Remove the handler registration if Freenet wrote it. Returns whether
/// anything was removed.
pub fn unregister() -> Result<bool> {
    #[cfg(target_os = "linux")]
    {
        let env = linux::LinuxEnv::from_process()
            .ok_or_else(|| anyhow::anyhow!("could not determine the home directory"))?;
        linux::unregister(&env)
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

/// Record that the user opted out, so the node does not re-register on start.
fn write_opt_out_marker() -> Result<()> {
    let markers = opt_out_markers();
    if markers.is_empty() {
        anyhow::bail!("could not determine the data directory");
    }
    // Every location: a node started with a different XDG_DATA_HOME than
    // this shell checks the other one.
    for marker in &markers {
        write_opt_out_marker_at(marker)?;
    }
    Ok(())
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
/// Register as part of an explicit install, printing one line about it.
/// Never fails the install: the node works without the handler.
pub fn register_for_install() {
    match register(RegisterMode::Install, None) {
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
        match (linux::LinuxEnv::from_process(), std::env::current_exe()) {
            (Some(env), Ok(bin)) => linux::is_managed_install(&env, &bin),
            _ => false,
        }
    }
    #[cfg(target_os = "windows")]
    {
        std::env::current_exe().is_ok_and(|b| windows::is_managed_install(&b))
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
/// managed install (see the module docs) the user has not opted out of,
/// writes only when something differs, swallows every error and panic, and
/// logs only when it changed something or failed. It holds no lock and
/// touches no file the node, wrapper or updater use, so it cannot affect
/// startup, shutdown or the exit-42 update path.
pub fn spawn_self_registration() {
    let spawned = std::thread::Builder::new()
        .name("freenet-url-handler".into())
        .spawn(|| {
            let result = std::panic::catch_unwind(|| {
                super::open_link::sweep_stale_fallback_pages();
                if !running_binary_is_managed() {
                    return None;
                }
                // `register` honours the opt-out marker in this mode.
                Some(register(RegisterMode::OnStart, None))
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
    /// without the service (Nix, cargo install, a hand-run binary), or to
    /// undo `unregister`.
    Register {
        /// The node's config directory, if not the default, so the handler
        /// reads the right `ws-api-port`.
        #[arg(long)]
        config_dir: Option<PathBuf>,
    },
    /// Remove the freenet:// link handler registration, and stop the node
    /// from registering it again when it starts.
    Unregister,
}

impl UrlHandlerCommand {
    pub fn run(&self) -> Result<()> {
        match self {
            UrlHandlerCommand::Register { config_dir } => {
                // Absolute, without resolving symlinks (and without Windows'
                // `\\?\` verbatim prefix that `canonicalize` adds).
                let config_dir = match config_dir {
                    Some(dir) if dir.is_dir() => Some(std::path::absolute(dir)?),
                    Some(dir) => anyhow::bail!("config dir {} does not exist", dir.display()),
                    None => None,
                };
                match register(RegisterMode::UserRequested, config_dir.as_deref())? {
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
                if cfg!(target_os = "macos") {
                    println!(
                        "On macOS, Freenet.app itself receives freenet:// links (its Info.plist \
                         declares the scheme), so there is nothing to unregister. Remove the app \
                         to stop it handling them."
                    );
                    return Ok(());
                }
                // Marker FIRST: if removal fails halfway, the node still will
                // not put the handler back, and a registration racing with
                // this one sees the marker and undoes itself.
                write_opt_out_marker()?;
                let removed = unregister()?;
                if removed {
                    println!("Removed the freenet:// link handler.");
                } else {
                    println!("No Freenet freenet:// link handler was registered.");
                }
                println!(
                    "Freenet will not register it again on start. Undo with \
                     `freenet service url-handler register`."
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_entry_passes_link_after_double_dash_and_quotes_only_spaced_paths() {
        let entry = render_desktop_entry(Path::new("/home/u/.local/bin/freenet"), None).unwrap();
        assert!(entry.contains("\nExec=/home/u/.local/bin/freenet open -- %u\n"));
        assert!(entry.contains("\nMimeType=x-scheme-handler/freenet;\n"));
        assert!(entry.contains("\nNoDisplay=true\n"));
        assert!(entry.contains(DESKTOP_MARKER));
        assert!(entry.starts_with("[Desktop Entry]\n"));
        let spaced = render_desktop_entry(Path::new("/opt/my apps/freenet"), None).unwrap();
        assert!(spaced.contains("Exec=\"/opt/my apps/freenet\" open -- %u"));
        let with_config = render_desktop_entry(
            Path::new("/opt/freenet"),
            Some(Path::new("/srv/freenet cfg")),
        )
        .unwrap();
        assert!(
            with_config
                .contains("\nExec=/opt/freenet --config-dir \"/srv/freenet cfg\" open -- %u\n")
        );
    }

    #[test]
    fn desktop_entry_refuses_paths_needing_escapes() {
        for bad in [
            "/tmp/a\"b/freenet",
            "/tmp/a`id`/freenet",
            "/tmp/$HOME/freenet",
            "/tmp/a\\b/freenet",
            "/tmp/100%/freenet",
            "/tmp/it's/freenet",
            "/tmp/a\nExec=evil/freenet",
            "relative/freenet",
            "",
        ] {
            assert!(
                render_desktop_entry(Path::new(bad), None).is_none(),
                "accepted {bad:?}"
            );
            assert!(
                render_desktop_entry(Path::new("/bin/freenet"), Some(Path::new(bad))).is_none(),
                "accepted config dir {bad:?}"
            );
        }
    }

    #[test]
    fn default_action_respects_a_foreign_choice_and_an_unknown_one() {
        assert_eq!(decide_default_action(Some("")), DefaultAction::SetOurs);
        assert_eq!(decide_default_action(Some("  \n")), DefaultAction::SetOurs);
        assert_eq!(
            decide_default_action(Some("freenet-url-handler.desktop\n")),
            DefaultAction::Keep
        );
        assert_eq!(
            decide_default_action(Some("other.desktop\n")),
            DefaultAction::LeaveForeign("other.desktop".into())
        );
        assert_eq!(decide_default_action(None), DefaultAction::Unknown);
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
        let shared = "x-scheme-handler/freenet=freenet-url-handler.desktop;other.desktop;\n";
        assert_eq!(strip_mimeapps_association(shared), None);
        assert_eq!(
            strip_mimeapps_association("a=b\nx-scheme-handler/freenet=freenet-url-handler.desktop")
                .unwrap(),
            "a=b\n"
        );
    }

    #[test]
    fn windows_command_quotes_exe_and_link() {
        let exe = r"C:\Users\A B\AppData\Local\Freenet\bin\freenet.exe";
        let cmd = windows_open_command(exe, None);
        assert_eq!(cmd, format!("\"{exe}\" open -- \"%1\""));
        assert_eq!(
            windows_open_command(exe, Some(r"C:\cfg dir")),
            format!("\"{exe}\" --config-dir \"C:\\cfg dir\" open -- \"%1\"")
        );
        assert_eq!(
            windows_open_command(exe, Some("C:\\")),
            format!("\"{exe}\" --config-dir \"C:\\\\\" open -- \"%1\"")
        );
        assert_eq!(windows_command_exe(&cmd), Some(exe));
        assert_eq!(windows_command_exe(r#"C:\x\freenet.exe open "%1""#), None);
        assert_eq!(windows_command_exe(""), None);
    }

    #[test]
    fn windows_exe_paths_that_would_break_the_command_are_refused() {
        assert!(windows_exe_is_representable(
            r"C:\Users\A B\AppData\Local\Freenet\bin\freenet.exe"
        ));
        assert!(!windows_exe_is_representable(r"C:\Users\100%\freenet.exe"));
        assert!(!windows_exe_is_representable(r"C:\Users\%1\freenet.exe"));
        assert!(!windows_exe_is_representable(r#"C:\a"b\freenet.exe"#));
        assert!(!windows_exe_is_representable(""));
    }

    /// An `unregister` that lands while a registration is in flight wins:
    /// whatever the platform step returned, the registration undoes itself.
    #[test]
    fn a_racing_opt_out_is_honoured_whatever_registration_returned() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("freenet").join(OPT_OUT_MARKER);
        let markers = vec![marker.clone()];
        let outcomes: Vec<fn() -> Result<RegisterOutcome>> = vec![
            || Ok(RegisterOutcome::Registered),
            || Ok(RegisterOutcome::ForeignDefault("x.desktop".into())),
            || anyhow::bail!("xdg-mime default failed"),
        ];
        for outcome in outcomes {
            clear_opt_out_marker_at(&marker).unwrap();
            let mut undone = false;
            let out = register_with(
                RegisterMode::OnStart,
                &markers,
                || {
                    // The user's `unregister` writes the marker mid-flight.
                    write_opt_out_marker_at(&marker).unwrap();
                    outcome()
                },
                || {
                    undone = true;
                    Ok(true)
                },
            )
            .unwrap();
            assert!(undone, "registration was not undone");
            assert_eq!(out, RegisterOutcome::Skipped(OPTED_OUT));
        }
        // A user's explicit register clears the marker and is never undone.
        let out = register_with(
            RegisterMode::UserRequested,
            &markers,
            || Ok(RegisterOutcome::Registered),
            || panic!("undo must not run for an explicit register"),
        )
        .unwrap();
        assert_eq!(out, RegisterOutcome::Registered);
        assert!(!opted_out_at(&marker));
    }

    #[test]
    fn only_a_user_request_overrides_the_opt_out() {
        use MarkerAction::*;
        assert_eq!(marker_action(RegisterMode::UserRequested, true), Clear);
        assert_eq!(marker_action(RegisterMode::UserRequested, false), Clear);
        // `service install` (install.sh re-runs, the setup wizard, `service
        // doctor`) and node start must respect it.
        assert_eq!(marker_action(RegisterMode::Install, true), Skip);
        assert_eq!(marker_action(RegisterMode::OnStart, true), Skip);
        assert_eq!(marker_action(RegisterMode::Install, false), Proceed);
        assert_eq!(marker_action(RegisterMode::OnStart, false), Proceed);
    }

    /// The registration calls that make existing installs get the handler
    /// (node start) and new installs get it and lose it (service install /
    /// uninstall) must stay wired in. A silent drop would pass every other
    /// test. Each call must sit at statement position inside its function,
    /// so a commented-out call fails too.
    #[test]
    fn registration_stays_wired_into_start_install_and_uninstall() {
        fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
            let start = src
                .find(&format!("\n{signature}"))
                .unwrap_or_else(|| panic!("`{signature}` not found at column 0"));
            let test_mod = src.find("\n#[cfg(test)]\nmod tests").unwrap_or(src.len());
            assert!(
                start < test_mod,
                "`{signature}` matched inside the test module"
            );
            let body = &src[start + 1..];
            // A method's closing brace is indented like its signature.
            let indent: String = signature.chars().take_while(|c| *c == ' ').collect();
            let end = body.find(&format!("\n{indent}}}\n")).expect("function end");
            &body[..end]
        }
        fn called_at_statement(body: &str, call: &str) -> bool {
            body.match_indices(call).any(|(i, _)| {
                let line_start = body[..i].rfind('\n').map_or(0, |n| n + 1);
                body[line_start..i].trim().is_empty()
            })
        }
        // Windows checkouts may have CRLF line endings.
        let freenet = &include_str!("../freenet.rs").replace("\r\n", "\n");
        assert!(called_at_statement(
            fn_body(freenet, "async fn run_network("),
            "commands::url_handler::spawn_self_registration();"
        ));
        let linux = &include_str!("service/linux.rs").replace("\r\n", "\n");
        assert!(called_at_statement(
            fn_body(linux, "fn install_user_service("),
            "super::super::url_handler::register_for_install();"
        ));
        assert!(called_at_statement(
            fn_body(linux, "pub(super) fn uninstall_service("),
            "super::super::url_handler::unregister_for_uninstall();"
        ));
        let windows = &include_str!("service/windows.rs").replace("\r\n", "\n");
        assert!(called_at_statement(
            fn_body(windows, "pub(super) fn install_service("),
            "super::super::url_handler::register_for_install();"
        ));
        assert!(called_at_statement(
            fn_body(windows, "pub(super) fn uninstall_service("),
            "super::super::url_handler::unregister_for_uninstall();"
        ));
        let uninstall = &include_str!("uninstall.rs").replace("\r\n", "\n");
        assert!(called_at_statement(
            fn_body(uninstall, "    pub fn run("),
            "super::url_handler::unregister_for_uninstall();"
        ));
    }
}
