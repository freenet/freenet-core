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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterMode {
    /// An explicit user action (`service install`, `url-handler register`):
    /// clears the opt-out and always re-checks the default handler.
    Explicit,
    /// The node starting: honours the opt-out, and on Linux only consults
    /// `xdg-mime` when the desktop entry had to be (re)written, so a
    /// crash-looping node does not spawn helpers on every restart.
    OnStart,
}

fn opt_out_marker_in(data_local_dir: &std::path::Path) -> PathBuf {
    data_local_dir.join("freenet").join(OPT_OUT_MARKER)
}

// ── Linux ───────────────────────────────────────────────────────────────────

#[cfg(any(target_os = "linux", test))]
/// Characters that cannot be represented safely in a desktop-entry `Exec` key
/// without the spec's two-level escaping. Paths containing them are refused
/// rather than escaped: the simplest escape that is obviously correct is none.
fn exec_path_is_representable(path: &str) -> bool {
    !path.is_empty()
        && path.starts_with('/')
        && !path
            .chars()
            .any(|c| c.is_control() || matches!(c, '"' | '`' | '$' | '\\' | '%' | '\''))
}

#[cfg(any(target_os = "linux", test))]
/// Render the desktop entry for `binary`. Returns `None` for a path that
/// cannot be written into `Exec` safely.
///
/// The link is passed as `%u` after a literal `--`: the launcher substitutes
/// `%u` as ONE argv entry with no shell, and `--` stops the link being read as
/// a flag. The path is quoted only when it contains a space. The spec allows
/// quoting always, but xdg-utils 1.1.3's generic `xdg-open` (still Ubuntu
/// 24.04's, and used under window managers it does not recognise) takes the
/// quote characters as part of the program name and fails; verified in a
/// container, where the quoted form exited 4 and the unquoted one opened the
/// link.
pub fn render_desktop_entry(binary: &Path) -> Option<String> {
    let path = binary.to_str()?;
    if !exec_path_is_representable(path) {
        return None;
    }
    let program = if path.contains(' ') {
        format!("\"{path}\"")
    } else {
        path.to_string()
    };
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

        pub(super) fn opt_out_marker(&self) -> PathBuf {
            opt_out_marker_in(&self.data_home)
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
        /// killed after [`HELPER_TIMEOUT`]. Stdout goes to an anonymous temp
        /// file rather than a pipe, so a daemon the helper leaves behind
        /// holding stdout open cannot make the read block. `None` if it could
        /// not run, failed, or timed out.
        fn run_helper(&self, program: &str, args: &[&str]) -> Option<String> {
            let exe = self.find(program)?;
            let mut out = tempfile::tempfile().ok()?;
            let mut child = Command::new(exe)
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
            let deadline = Instant::now() + HELPER_TIMEOUT;
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
    /// `mimeapps.list` stays a symlink to the (updated) real file.
    fn write_through_symlink(path: &Path, contents: &[u8]) -> Result<()> {
        let target = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        super::super::open_link::write_atomically(&target, contents)
    }

    /// Write or refresh the desktop entry and, when no handler is set, make it
    /// the default. Idempotent.
    pub(super) fn register(
        env: &LinuxEnv,
        binary: &Path,
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
        let Some(entry) = render_desktop_entry(binary) else {
            return Ok(RegisterOutcome::Skipped(
                "the binary path cannot be written into a desktop entry",
            ));
        };
        let dir = env.applications_dir();
        let path = env.desktop_file();

        let existing = std::fs::read_to_string(&path).ok();
        if existing.as_deref().is_some_and(|c| !is_ours(c)) {
            // Something else put a file at our path: not ours to replace.
            return Ok(RegisterOutcome::Skipped(
                "an unrelated freenet-url-handler.desktop is already installed",
            ));
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
            let out = register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            assert_eq!(out, RegisterOutcome::Registered);
            let entry = std::fs::read_to_string(f.env.desktop_file()).unwrap();
            assert!(entry.contains(&format!("\nExec={BIN} open -- %u\n")));
            assert_eq!(f.default().trim(), DESKTOP_FILE_NAME);

            let again = register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            assert_eq!(again, RegisterOutcome::AlreadyCurrent);
        }

        #[test]
        fn on_start_with_current_entry_runs_no_helper() {
            let f = Fixture::new(ALL);
            register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            let calls = f.xdg_mime_calls();
            let out = register(&f.env, Path::new(BIN), RegisterMode::OnStart).unwrap();
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
            register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            let out = register(
                &f.env,
                Path::new("/opt/freenet/freenet"),
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
            let out = register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            assert_eq!(out, RegisterOutcome::ForeignDefault("other.desktop".into()));
            assert_eq!(f.default().trim(), "other.desktop");
        }

        #[test]
        fn a_failed_query_does_not_replace_the_default() {
            let f = Fixture::new(ALL);
            f.set_default("other.desktop\n");
            std::fs::write(f.root().join("query-fails"), "").unwrap();
            register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            assert_eq!(f.default().trim(), "other.desktop");
        }

        #[test]
        fn without_xdg_open_nothing_is_written() {
            let f = Fixture::new(&["xdg-mime"]);
            let out = register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
            assert!(matches!(out, RegisterOutcome::Skipped(_)));
            assert!(!f.env.desktop_file().exists());
        }

        #[test]
        fn unregister_removes_ours_from_every_mimeapps_list_and_keeps_the_rest() {
            let f = Fixture::new(ALL);
            register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
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

            let out = register(&f.env, Path::new(BIN), RegisterMode::Explicit).unwrap();
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
pub fn windows_open_command(exe: &str) -> String {
    format!("\"{exe}\" open -- \"%1\"")
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

/// Whether a registry command line launches a `freenet.exe`, i.e. is a
/// registration Freenet may overwrite or remove. Matched by file name, not
/// path: a reinstall to another directory must be able to replace its own
/// stale registration. An unrelated program that happens to be called
/// `freenet.exe` would be treated as ours.
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

    pub(super) fn register(exe: &std::path::Path) -> Result<RegisterOutcome> {
        let exe = exe
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("executable path is not valid UTF-8"))?;
        if !windows_exe_is_representable(exe) {
            return Ok(RegisterOutcome::Skipped(
                "the executable path contains a quote or percent sign",
            ));
        }
        let command = windows_open_command(exe);
        match current_command() {
            Some(existing) if existing == command && has_url_protocol_value() => {
                return Ok(RegisterOutcome::AlreadyCurrent);
            }
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

/// The opt-out marker for this user, if a data directory can be found.
fn opt_out_marker() -> Option<PathBuf> {
    #[cfg(target_os = "linux")]
    {
        linux::LinuxEnv::from_process().map(|env| env.opt_out_marker())
    }
    #[cfg(not(target_os = "linux"))]
    {
        dirs::data_local_dir().map(|d| opt_out_marker_in(&d))
    }
}

/// Register the handler for the running binary. Idempotent.
pub fn register(mode: RegisterMode) -> Result<RegisterOutcome> {
    if mode == RegisterMode::Explicit {
        if let Some(marker) = opt_out_marker() {
            match std::fs::remove_file(&marker) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    #[cfg(target_os = "linux")]
    {
        let env = linux::LinuxEnv::from_process()
            .ok_or_else(|| anyhow::anyhow!("could not determine the home directory"))?;
        linux::register(&env, &std::env::current_exe()?, mode)
    }
    #[cfg(target_os = "windows")]
    {
        windows::register(&std::env::current_exe()?)
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
    let marker = opt_out_marker()
        .ok_or_else(|| anyhow::anyhow!("could not determine the data directory"))?;
    if let Some(parent) = marker.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&marker, b"freenet service url-handler unregister\n")?;
    Ok(())
}

/// Register as part of an explicit install, printing one line about it.
/// Never fails the install: the node works without the handler.
pub fn register_for_install() {
    match register(RegisterMode::Explicit) {
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

/// What the on-start registration should do. Pure, so it is unit-tested.
fn should_register_on_start(managed: bool, opted_out: bool) -> bool {
    managed && !opted_out
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
                let opted_out = opt_out_marker().is_some_and(|m| m.exists());
                if !should_register_on_start(running_binary_is_managed(), opted_out) {
                    return None;
                }
                Some(register(RegisterMode::OnStart))
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
    Register,
    /// Remove the freenet:// link handler registration, and stop the node
    /// from registering it again when it starts.
    Unregister,
}

impl UrlHandlerCommand {
    pub fn run(&self) -> Result<()> {
        match self {
            UrlHandlerCommand::Register => {
                match register(RegisterMode::Explicit)? {
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
                let removed = unregister()?;
                write_opt_out_marker()?;
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
        let entry = render_desktop_entry(Path::new("/home/u/.local/bin/freenet")).unwrap();
        assert!(entry.contains("\nExec=/home/u/.local/bin/freenet open -- %u\n"));
        assert!(entry.contains("\nMimeType=x-scheme-handler/freenet;\n"));
        assert!(entry.contains("\nNoDisplay=true\n"));
        assert!(entry.contains(DESKTOP_MARKER));
        assert!(entry.starts_with("[Desktop Entry]\n"));
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
            "/tmp/it's/freenet",
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
        let cmd = windows_open_command(exe);
        assert_eq!(cmd, format!("\"{exe}\" open -- \"%1\""));
        assert_eq!(windows_command_exe(&cmd), Some(exe));
        assert!(windows_command_is_ours(&cmd));
        assert!(windows_command_is_ours(r#""C:\x\FREENET.EXE" open "%1""#));
        assert!(!windows_command_is_ours(r#""C:\x\other.exe" "%1""#));
        assert!(!windows_command_is_ours(r#"C:\x\freenet.exe open "%1""#));
        assert!(!windows_command_is_ours(""));
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

    #[test]
    fn on_start_registration_needs_a_managed_install_and_no_opt_out() {
        assert!(should_register_on_start(true, false));
        assert!(!should_register_on_start(true, true));
        assert!(!should_register_on_start(false, false));
        assert!(!should_register_on_start(false, true));
    }
}
