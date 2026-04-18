//! Full uninstallation of Freenet: service, data, and binaries.

use anyhow::{Context, Result};
use clap::Args;
use std::path::{Path, PathBuf};

#[derive(Args, Debug, Clone)]
pub struct UninstallCommand {
    /// Also remove all Freenet data, config, and logs
    #[arg(long, conflicts_with = "keep_data")]
    pub purge: bool,
    /// Only remove service and binaries (skip interactive prompt about data)
    #[arg(long, conflicts_with = "purge")]
    pub keep_data: bool,
    /// Operate on the system-wide service instead of user service (Linux only)
    #[arg(long)]
    pub system: bool,
}

impl UninstallCommand {
    pub fn run(&self) -> Result<()> {
        // Step 1: Uninstall the service if installed (reuses service.rs logic)
        let service_removed = super::service::stop_and_remove_service(self.system)?;
        if service_removed {
            println!("Freenet service removed.");
        }

        // Step 2: Determine whether to purge data
        let do_purge = super::service::should_purge(self.purge, self.keep_data)?;
        if do_purge {
            super::service::purge_data(self.system)?;
            println!("All Freenet data, config, and logs removed.");
        }

        // Step 3: Remove binaries from every known install location.
        // Users often have multiple installations at once (e.g. `curl | sh` put
        // the binary in ~/.local/bin and they later ran `cargo install freenet`
        // which also dropped one in ~/.cargo/bin). Visiting only the directory
        // of the running executable leaves stale copies behind.
        let env = RuntimeEnv::from_process();
        let install_dirs = get_install_dirs(None, &env);
        let mut removed_any = false;
        for dir in &install_dirs {
            if !dir.exists() {
                continue;
            }
            let removed = remove_binaries(dir)?;
            if !removed.is_empty() {
                removed_any = true;
            }
        }

        // On Windows the PowerShell installer creates %LOCALAPPDATA%\Freenet\bin
        // just for our binaries, so if it's now empty we can collapse the
        // enclosing %LOCALAPPDATA%\Freenet\ folder as well. Other platforms
        // drop binaries into shared dirs (~/.local/bin, ~/.cargo/bin) whose
        // parents belong to unrelated tools — don't touch those.
        #[cfg(target_os = "windows")]
        if let Some(ref local_app_data) = env.local_app_data {
            collapse_windows_bin_tree(local_app_data);
        }

        // Summary
        if !service_removed && !removed_any && !do_purge {
            println!("Freenet does not appear to be installed.");
        } else {
            println!();
            println!("Freenet has been completely uninstalled.");
        }

        Ok(())
    }
}

/// Captures the process-environment values `get_install_dirs` needs. Pulling
/// these out into a value-typed struct lets tests supply explicit inputs
/// instead of mutating process-global env vars (which races with parallel
/// `cargo test` threads — see #3905 testing review).
#[derive(Debug, Clone, Default)]
struct RuntimeEnv {
    freenet_install_dir: Option<PathBuf>,
    home: Option<PathBuf>,
    current_exe_parent: Option<PathBuf>,
    #[cfg_attr(not(target_os = "windows"), allow(dead_code))]
    local_app_data: Option<PathBuf>,
}

impl RuntimeEnv {
    fn from_process() -> Self {
        Self {
            freenet_install_dir: std::env::var_os("FREENET_INSTALL_DIR").map(PathBuf::from),
            home: dirs::home_dir(),
            current_exe_parent: std::env::current_exe()
                .ok()
                .and_then(|e| e.parent().map(|p| p.to_path_buf())),
            local_app_data: std::env::var_os("LOCALAPPDATA").map(PathBuf::from),
        }
    }
}

/// Remove Freenet binaries from the given install dir. Returns the list of
/// files that were removed.
fn remove_binaries(install_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut removed = Vec::new();

    for name in binary_names() {
        let path = install_dir.join(name);
        if path.exists() {
            // On Windows, a running executable cannot delete itself. Skip with a hint.
            #[cfg(target_os = "windows")]
            if is_current_exe(&path) {
                println!(
                    "Cannot remove running binary: {}\n\
                     Please delete it manually after this command exits.",
                    path.display()
                );
                continue;
            }

            std::fs::remove_file(&path)
                .with_context(|| format!("Failed to remove {}", path.display()))?;
            println!("Removed {}", path.display());
            removed.push(path);
        }
    }

    // Also remove the macOS wrapper script if present
    let wrapper = install_dir.join("freenet-service-wrapper.sh");
    if wrapper.exists() {
        std::fs::remove_file(&wrapper)
            .with_context(|| format!("Failed to remove {}", wrapper.display()))?;
        println!("Removed {}", wrapper.display());
        removed.push(wrapper);
    }

    Ok(removed)
}

/// Check if the given path is the currently running executable.
#[cfg(target_os = "windows")]
fn is_current_exe(path: &Path) -> bool {
    std::env::current_exe()
        .ok()
        .and_then(|exe| std::fs::canonicalize(&exe).ok())
        .zip(std::fs::canonicalize(path).ok())
        .is_some_and(|(a, b)| a == b)
}

/// Get every directory that Freenet binaries may have been installed into,
/// in no particular order (duplicates are removed).
///
/// If `override_dir` or `env.freenet_install_dir` is non-empty, only that
/// location is returned — the caller has explicitly told us where to look.
/// Otherwise we return the union of the install.sh default (`~/.local/bin`),
/// the Cargo default (`~/.cargo/bin`), the Windows PowerShell installer
/// default (`%LOCALAPPDATA%\Freenet\bin`), and the parent directory of the
/// currently running executable.
fn get_install_dirs(override_dir: Option<&Path>, env: &RuntimeEnv) -> Vec<PathBuf> {
    if let Some(dir) = override_dir {
        return vec![dir.to_path_buf()];
    }

    if let Some(dir) = env
        .freenet_install_dir
        .as_ref()
        .filter(|p| !p.as_os_str().is_empty())
    {
        return vec![dir.clone()];
    }

    let mut dirs: Vec<PathBuf> = Vec::new();

    if let Some(parent) = env.current_exe_parent.as_ref() {
        dirs.push(parent.clone());
    }

    if let Some(home) = env.home.as_ref() {
        dirs.push(home.join(".local/bin")); // install.sh default
        dirs.push(home.join(".cargo/bin")); // `cargo install freenet` default
    }

    #[cfg(target_os = "windows")]
    if let Some(local_app_data) = env.local_app_data.as_ref() {
        // install.ps1 default — not normally on PATH, so exe parent detection
        // misses it when the user ran a different `freenet.exe` (e.g. from a
        // crate install).
        dirs.push(local_app_data.join("Freenet").join("bin"));
    }

    dirs.sort();
    dirs.dedup();
    dirs
}

/// On Windows, collapse `%LOCALAPPDATA%\Freenet\bin` and its enclosing
/// `%LOCALAPPDATA%\Freenet\` folder if each is now empty. Extracted so the
/// behavior is testable on any platform (unlike the caller, which is
/// `#[cfg(target_os = "windows")]` and can't run in Linux CI).
#[cfg_attr(not(target_os = "windows"), allow(dead_code))]
fn collapse_windows_bin_tree(local_app_data: &Path) {
    let bin_dir = local_app_data.join("Freenet").join("bin");
    super::service::remove_dir_if_empty_pub(&bin_dir);
    if let Some(parent) = bin_dir.parent() {
        super::service::remove_dir_if_empty_pub(parent);
    }
}

/// Get the names of binaries to remove.
fn binary_names() -> &'static [&'static str] {
    #[cfg(target_os = "windows")]
    {
        &["freenet.exe", "fdev.exe"]
    }
    #[cfg(not(target_os = "windows"))]
    {
        &["freenet", "fdev"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_with(home: Option<&Path>, exe_parent: Option<&Path>) -> RuntimeEnv {
        RuntimeEnv {
            freenet_install_dir: None,
            home: home.map(Path::to_path_buf),
            current_exe_parent: exe_parent.map(Path::to_path_buf),
            local_app_data: None,
        }
    }

    #[test]
    fn test_get_install_dirs_with_override() {
        let env = env_with(Some(Path::new("/home/u")), Some(Path::new("/exe")));
        let dirs = get_install_dirs(Some(Path::new("/tmp/test-freenet")), &env);
        assert_eq!(dirs, vec![PathBuf::from("/tmp/test-freenet")]);
    }

    #[test]
    fn test_get_install_dirs_without_override_includes_defaults() {
        let home = PathBuf::from("/home/u");
        let exe_parent = PathBuf::from("/exe");
        let env = env_with(Some(&home), Some(&exe_parent));

        let dirs = get_install_dirs(None, &env);
        assert!(dirs.contains(&home.join(".local/bin")), "{dirs:?}");
        assert!(dirs.contains(&home.join(".cargo/bin")), "{dirs:?}");
        assert!(dirs.contains(&exe_parent), "{dirs:?}");
    }

    #[test]
    fn test_get_install_dirs_env_var_takes_precedence() {
        let mut env = env_with(Some(Path::new("/home/u")), Some(Path::new("/exe")));
        env.freenet_install_dir = Some(PathBuf::from("/custom/path"));

        let dirs = get_install_dirs(None, &env);

        assert_eq!(dirs, vec![PathBuf::from("/custom/path")]);
    }

    #[test]
    fn test_get_install_dirs_empty_env_var_falls_through() {
        // FREENET_INSTALL_DIR="" (explicitly empty) must not short-circuit
        // to an empty PathBuf — that would collapse to the current dir and
        // silently skip the defaults.
        let mut env = env_with(Some(Path::new("/home/u")), None);
        env.freenet_install_dir = Some(PathBuf::from(""));

        let dirs = get_install_dirs(None, &env);

        assert!(
            dirs.contains(&PathBuf::from("/home/u/.local/bin")),
            "empty env var should fall through to defaults, got {dirs:?}",
        );
        assert!(!dirs.contains(&PathBuf::from("")), "{dirs:?}");
    }

    #[test]
    fn test_get_install_dirs_override_beats_env_var() {
        // Explicit `override_dir` wins over `FREENET_INSTALL_DIR`.
        let mut env = env_with(Some(Path::new("/home/u")), None);
        env.freenet_install_dir = Some(PathBuf::from("/env/value"));

        let dirs = get_install_dirs(Some(Path::new("/explicit/override")), &env);

        assert_eq!(dirs, vec![PathBuf::from("/explicit/override")]);
    }

    #[test]
    fn test_get_install_dirs_deduplicates_exe_parent_overlap() {
        // If the running exe is already inside ~/.local/bin, both the
        // exe-parent entry and the curl-installer-default entry resolve
        // to the same path — make sure we only keep one copy.
        let home = PathBuf::from("/home/u");
        let exe_parent = home.join(".local/bin");
        let env = env_with(Some(&home), Some(&exe_parent));

        let dirs = get_install_dirs(None, &env);
        let mut expected = dirs.clone();
        expected.sort();
        expected.dedup();
        assert_eq!(dirs.len(), expected.len(), "{dirs:?}");
        let local_bin_count = dirs.iter().filter(|d| **d == exe_parent).count();
        assert_eq!(local_bin_count, 1, "{dirs:?}");
    }

    #[test]
    fn test_get_install_dirs_missing_home() {
        // With no home directory available (e.g. chrooted environment), we
        // still return something usable rather than an empty vec.
        let env = env_with(None, Some(Path::new("/opt/freenet/bin")));

        let dirs = get_install_dirs(None, &env);

        assert_eq!(dirs, vec![PathBuf::from("/opt/freenet/bin")]);
    }

    #[test]
    fn test_binary_names() {
        let names = binary_names();
        assert!(names.len() >= 2);
        #[cfg(not(target_os = "windows"))]
        {
            assert!(names.contains(&"freenet"));
            assert!(names.contains(&"fdev"));
        }
    }

    #[test]
    fn test_remove_binaries_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let removed = remove_binaries(tmp.path()).unwrap();
        assert!(removed.is_empty());
    }

    #[test]
    fn test_remove_binaries_removes_files() {
        let tmp = tempfile::tempdir().unwrap();

        // Create fake binaries
        std::fs::write(tmp.path().join("freenet"), b"fake").unwrap();
        std::fs::write(tmp.path().join("fdev"), b"fake").unwrap();
        std::fs::write(tmp.path().join("freenet-service-wrapper.sh"), b"fake").unwrap();

        let removed = remove_binaries(tmp.path()).unwrap();
        #[cfg(not(target_os = "windows"))]
        assert_eq!(removed.len(), 3);

        // Verify files are gone
        assert!(!tmp.path().join("freenet").exists());
        assert!(!tmp.path().join("fdev").exists());
        assert!(!tmp.path().join("freenet-service-wrapper.sh").exists());
    }

    #[test]
    fn test_remove_binaries_handles_partial_install() {
        // A directory that contains only `freenet` (not `fdev`) must not
        // error. Importantly, we also assert that `fdev` is still absent
        // afterward — we must NOT synthesize a file we didn't find.
        let tmp = tempfile::tempdir().unwrap();
        #[cfg(not(target_os = "windows"))]
        {
            std::fs::write(tmp.path().join("freenet"), b"fake").unwrap();
            let removed = remove_binaries(tmp.path()).unwrap();
            assert_eq!(removed.len(), 1);
            assert!(!tmp.path().join("freenet").exists());
            assert!(!tmp.path().join("fdev").exists());
        }
        #[cfg(target_os = "windows")]
        {
            std::fs::write(tmp.path().join("freenet.exe"), b"fake").unwrap();
            let removed = remove_binaries(tmp.path()).unwrap();
            assert_eq!(removed.len(), 1);
            assert!(!tmp.path().join("freenet.exe").exists());
            assert!(!tmp.path().join("fdev.exe").exists());
        }
    }

    #[test]
    fn test_collapse_windows_bin_tree_removes_empty_tree() {
        // Simulates the layout install.ps1 creates: `<LocalAppData>\Freenet\bin\`
        // with both levels empty after our binary cleanup. Expected: both
        // levels collapse, but the `<LocalAppData>` root is left alone.
        let tmp = tempfile::tempdir().unwrap();
        let local_app_data = tmp.path();
        let freenet = local_app_data.join("Freenet");
        let bin = freenet.join("bin");
        std::fs::create_dir_all(&bin).unwrap();

        collapse_windows_bin_tree(local_app_data);

        assert!(!bin.exists(), "bin should be gone");
        assert!(!freenet.exists(), "Freenet parent should collapse");
        assert!(local_app_data.exists(), "LocalAppData root must stay");
    }

    #[test]
    fn test_collapse_windows_bin_tree_preserves_non_empty_sibling() {
        // If another tool created `<LocalAppData>\Freenet\data\` (not us,
        // but imagine a future subcommand), we must NOT remove `Freenet\`
        // when only `bin\` is empty.
        let tmp = tempfile::tempdir().unwrap();
        let local_app_data = tmp.path();
        let freenet = local_app_data.join("Freenet");
        let bin = freenet.join("bin");
        let foreign = freenet.join("other-tool");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&foreign).unwrap();
        std::fs::write(foreign.join("state.bin"), b"unrelated").unwrap();

        collapse_windows_bin_tree(local_app_data);

        assert!(!bin.exists(), "empty bin should still collapse");
        assert!(freenet.exists(), "non-empty Freenet parent must stay");
        assert!(foreign.join("state.bin").exists(), "foreign data preserved");
    }

    #[test]
    fn test_collapse_windows_bin_tree_missing_tree_is_noop() {
        // Most Linux installs don't have this tree at all. The helper must
        // not panic or complain in that case.
        let tmp = tempfile::tempdir().unwrap();
        collapse_windows_bin_tree(tmp.path());
        assert!(tmp.path().exists());
    }
}
