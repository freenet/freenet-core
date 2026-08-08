use std::{
    io::{self, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::Child,
    time::Duration,
};

use serde::de::DeserializeOwned;

use crate::wasm_runtime::DeserializationFmt;

pub fn deserialize<T, R>(deser_format: Option<DeserializationFmt>, data: &R) -> anyhow::Result<T>
where
    T: DeserializeOwned,
    R: AsRef<[u8]> + ?Sized,
{
    match deser_format {
        Some(DeserializationFmt::Json) => {
            let deser = serde_json::from_slice(data.as_ref())?;
            Ok(deser)
        }
        _ => Ok(bincode::deserialize(data.as_ref())?),
    }
}

pub(crate) fn pipe_std_streams(mut child: Child) -> anyhow::Result<()> {
    let c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let c_stderr = child.stderr.take().expect("Failed to open command stderr");

    let write_child_stderr = move || -> anyhow::Result<()> {
        let mut stderr = io::stderr();
        let mut reader = BufReader::new(c_stderr);
        let mut buffer = [0; 1024];
        while let Ok(n) = reader.read(&mut buffer) {
            if n == 0 {
                break;
            }
            stderr.write_all(&buffer[..n])?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> anyhow::Result<()> {
        let mut stdout = io::stdout();
        let mut reader = BufReader::new(c_stdout);
        let mut buffer = [0; 1024];
        while let Ok(n) = reader.read(&mut buffer) {
            if n == 0 {
                break;
            }
            stdout.write_all(&buffer[..n])?;
        }
        Ok(())
    };
    std::thread::spawn(write_child_stdout);
    std::thread::spawn(write_child_stderr);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    anyhow::bail!("exit with status: {status}");
                }
                break;
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

/// Gets the target directory for the crate being built at `crate_dir`: the
/// `CARGO_TARGET_DIR` environment variable when set, otherwise the `target/` of the
/// workspace that `crate_dir` belongs to, otherwise the crate's own `target/`.
///
/// The search starts at **the crate being built**, not at `env!("CARGO_MANIFEST_DIR")`.
/// That macro is expanded when *fdev itself* is compiled, so on a released binary it is
/// the path on the machine that built the release — a path that does not exist for the
/// user. Walking its ancestors found no workspace root and the `.expect` below panicked,
/// so `fdev build` could not run at all without `CARGO_TARGET_DIR` set, even from inside
/// a perfectly ordinary Cargo workspace.
pub fn get_workspace_target_dir(crate_dir: &Path) -> PathBuf {
    const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";
    resolve_target_dir(std::env::var(TARGET_DIR_VAR).ok(), crate_dir)
}

/// The resolution itself, with the environment passed in rather than read.
///
/// Split out so the tests can exercise the no-`CARGO_TARGET_DIR` path deterministically:
/// `cargo test` sets `CARGO_TARGET_DIR` in its own child environment, so a test that
/// reads the variable directly would take the short-circuit branch and pass no matter
/// what the fallback does. (Mutating the process environment instead is not an option —
/// `set_var` is unsound with threads and `unsafe` from Rust 2024.) Matches the injected-
/// getter shape already used by `auto_update::detect_supervisor_status`.
fn resolve_target_dir(env_target_dir: Option<String>, crate_dir: &Path) -> PathBuf {
    if let Some(dir) = env_target_dir {
        return PathBuf::from(dir);
    }
    find_workspace_root_from(crate_dir)
        .unwrap_or_else(|| crate_dir.to_path_buf())
        .join("target")
}

/// Finds the workspace root directory starting from the given path.
/// Walks up the directory tree looking for a Cargo.toml with [workspace] section.
///
/// Returns `None` when there is none: a crate that belongs to no workspace is an
/// ordinary thing to build, and cargo puts its artefacts in the crate's own `target/`.
/// This must not panic — it runs on the `fdev build` path, where the only thing the user
/// could do with a panic is guess.
fn find_workspace_root_from(start_path: &Path) -> Option<PathBuf> {
    start_path
        .ancestors()
        .find(|p| {
            p.join("Cargo.toml").exists() && {
                let content = std::fs::read_to_string(p.join("Cargo.toml")).unwrap_or_default();
                content.contains("[workspace]")
            }
        })
        .map(Path::to_path_buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write the smallest thing that reads as a crate at `dir`.
    fn crate_at(dir: &Path, manifest: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("Cargo.toml"), manifest).unwrap();
    }

    /// Regression test for the panic in `fdev build` without `CARGO_TARGET_DIR`.
    ///
    /// The resolution must start from THE CRATE BEING BUILT. Before the fix it started
    /// from `env!("CARGO_MANIFEST_DIR")` — fdev's own compile-time path — so on a source
    /// build a user's crate resolved to freenet-core's own workspace, and on a released
    /// binary it resolved to a path that does not exist on the user's machine and
    /// panicked with "Could not find workspace root".
    ///
    /// Goes through `resolve_target_dir` with `None` rather than
    /// `get_workspace_target_dir`, because `cargo test` sets `CARGO_TARGET_DIR` for its
    /// children: reading the real environment here would take the short-circuit branch
    /// and the test would pass against the unfixed code.
    #[test]
    fn resolves_the_workspace_of_the_crate_being_built_not_fdevs_own() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        let member = ws.join("member");
        crate_at(
            &ws,
            "[workspace]\nmembers = [\"member\"]\nresolver = \"2\"\n",
        );
        crate_at(
            &member,
            "[package]\nname = \"member\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );

        assert_eq!(
            resolve_target_dir(None, &member),
            ws.join("target"),
            "a workspace member must resolve to ITS workspace's target dir"
        );
    }

    /// A crate that belongs to no workspace is ordinary, not an error: cargo puts its
    /// artefacts in the crate's own `target/`. Before the fix, missing a workspace root
    /// was a panic.
    #[test]
    fn a_crate_in_no_workspace_gets_its_own_target_dir_and_does_not_panic() {
        let tmp = tempfile::tempdir().unwrap();
        // `[workspace]` in the crate's OWN manifest is how a standalone crate detaches
        // itself, and it also makes this independent of whatever lies above the temp dir
        // on the machine running the test.
        let solo = tmp.path().join("solo");
        crate_at(
            &solo,
            "[package]\nname = \"solo\"\nversion = \"0.1.0\"\nedition = \"2021\"\n[workspace]\n",
        );

        assert_eq!(resolve_target_dir(None, &solo), solo.join("target"));
    }

    /// `CARGO_TARGET_DIR` still wins, and still short-circuits the search entirely.
    #[test]
    fn an_explicit_target_dir_wins_over_the_search() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path().join("ws");
        let member = ws.join("member");
        crate_at(&ws, "[workspace]\nmembers = [\"member\"]\n");
        crate_at(
            &member,
            "[package]\nname = \"member\"\nversion = \"0.1.0\"\n",
        );

        assert_eq!(
            resolve_target_dir(Some("/explicit/target".to_string()), &member),
            PathBuf::from("/explicit/target"),
            "an explicit CARGO_TARGET_DIR must not be second-guessed"
        );
    }

    /// No workspace inside the tree under test, none reported from it — and no panic,
    /// which is what the `.expect` used to do.
    #[test]
    fn find_workspace_root_from_returns_none_when_there_is_no_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let lone = tmp.path().join("a/b/c");
        std::fs::create_dir_all(&lone).unwrap();
        let found = find_workspace_root_from(&lone);
        assert!(
            found.is_none() || !found.as_deref().unwrap().starts_with(tmp.path()),
            "no workspace exists inside the temp tree, so none may be reported from it"
        );
    }
}
