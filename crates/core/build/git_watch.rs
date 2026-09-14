//! Which files cargo must watch so `build.rs` reruns when the git metadata it
//! embeds could have changed, and otherwise leaves the `freenet` crate alone.
//!
//! Every rerun of the build script emits a fresh `BUILD_TIMESTAMP`, which
//! recompiles the whole crate, so a trigger that fires spuriously costs a full
//! crate build. Cargo treats a watched path that does not exist as always
//! changed: the old relative `.git/HEAD` resolved against `crates/core/`,
//! where no `.git` exists, so `freenet` recompiled on every cargo invocation
//! (#5667). Only paths that exist are returned.
//!
//! Shared by `build.rs` (via `#[path]`) and a `#[cfg(test)]` module in
//! `lib.rs`, so it must stay dependency-free: the build script sees only
//! `[build-dependencies]`.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Paths to emit as `cargo:rerun-if-changed`, all of which exist.
///
/// Returns `None` when `manifest_dir` is not in a git work tree or git is not
/// installed (a crates.io tarball, or a Docker build whose context excludes
/// `.git`). The caller then emits nothing and cargo falls back to its default:
/// rerun when any file in the package changes.
pub(crate) fn rerun_if_changed_paths(manifest_dir: &Path) -> Option<Vec<PathBuf>> {
    // `--git-path` resolves the real locations, including a linked worktree,
    // whose HEAD and index live in `.git/worktrees/<name>/` while its branch
    // refs and `packed-refs` live in the common dir. Paths come back relative
    // to the directory git ran in, so they are resolved against it.
    let resolved = git_paths(
        manifest_dir,
        &[
            "rev-parse",
            "--show-toplevel",
            "--git-path",
            "HEAD",
            "--git-path",
            "index",
            "--git-path",
            "logs/HEAD",
            "--git-path",
            "packed-refs",
        ],
    )?;
    let [toplevel, head, index, head_log, packed_refs] = resolved.as_slice() else {
        return None;
    };

    // HEAD changes on checkout, and on commit when detached. On a commit to a
    // branch only the branch ref changes, so watch that too. A ref that has
    // been packed has no loose file, and its value lives in `packed-refs`
    // instead; watch that only in this case, since it is shared by every
    // worktree of the repository and any of them can rewrite it.
    //
    // `logs/HEAD` is not redundant with the ref: it is the only trigger for
    // the first commit onto a packed branch, which creates a loose ref file
    // that did not exist when the script last ran, and leaves `packed-refs`
    // untouched. It is per-worktree, so other worktrees do not disturb it.
    let mut watched = vec![head.clone(), index.clone(), head_log.clone()];
    let branch_ref = git_stdout(manifest_dir, &["symbolic-ref", "-q", "HEAD"])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(branch_ref) = branch_ref {
        match git_paths(manifest_dir, &["rev-parse", "--git-path", &branch_ref]).as_deref() {
            Some([loose]) if loose.exists() => watched.push(loose.clone()),
            _ => watched.push(packed_refs.clone()),
        }
    }

    // `GIT_DIRTY` gates auto-update (#3245), so it must also follow edits to
    // what the binary is built from, which do not touch any git file until
    // they are staged. Cargo recompiles `freenet` after such an edit anyway,
    // so rerunning the script then costs nothing extra.
    //
    // Unstaged edits elsewhere in the repository (other crates, docs, this
    // package's tests) deliberately do NOT rerun it, so `GIT_DIRTY` can read
    // clean while `git status` does not. None of those files is compiled into
    // the `freenet` binary: its only in-repo path dependency is a
    // dev-dependency. Add a path here if that changes.
    watched.extend([
        manifest_dir.join("src"),
        manifest_dir.join("Cargo.toml"),
        toplevel.join("Cargo.toml"),
        toplevel.join("Cargo.lock"),
    ]);

    watched.retain(|p| p.exists());
    Some(watched)
}

fn git_stdout(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

fn git_paths(dir: &Path, args: &[&str]) -> Option<Vec<PathBuf>> {
    let stdout = git_stdout(dir, args)?;
    Some(stdout.lines().map(|line| dir.join(line)).collect())
}

#[cfg(test)]
mod tests {
    use super::rerun_if_changed_paths;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    /// Run git in `dir` with an identity and settings that do not depend on
    /// the machine's global config (signing, default branch name).
    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args([
                "-c",
                "user.name=freenet-test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.logAllRefUpdates=true",
            ])
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git must be installed to run these tests");
        assert!(status.success(), "git {args:?} failed in {}", dir.display());
    }

    fn canonical(paths: impl IntoIterator<Item = PathBuf>) -> BTreeSet<PathBuf> {
        paths
            .into_iter()
            .map(|p| {
                p.canonicalize()
                    .unwrap_or_else(|e| panic!("{} does not exist: {e}", p.display()))
            })
            .collect()
    }

    /// A repository shaped like this workspace: the package lives two levels
    /// below the git top level, where the old relative `.git/HEAD` never
    /// existed. Returns the package dir.
    fn workspace_repo(root: &Path) -> PathBuf {
        let pkg = root.join("crates").join("pkg");
        std::fs::create_dir_all(pkg.join("src")).unwrap();
        std::fs::write(pkg.join("src").join("lib.rs"), "").unwrap();
        std::fs::write(pkg.join("Cargo.toml"), "").unwrap();
        std::fs::write(root.join("Cargo.toml"), "").unwrap();
        std::fs::write(root.join("Cargo.lock"), "").unwrap();
        git(root, &["init", "-q"]);
        // Pin the branch name regardless of `init.defaultBranch`.
        git(root, &["symbolic-ref", "HEAD", "refs/heads/main"]);
        git(root, &["add", "-A"]);
        git(root, &["commit", "-q", "-m", "init"]);
        pkg
    }

    fn package_paths(root: &Path, pkg: &Path) -> Vec<PathBuf> {
        vec![
            pkg.join("src"),
            pkg.join("Cargo.toml"),
            root.join("Cargo.toml"),
            root.join("Cargo.lock"),
        ]
    }

    #[test]
    fn plain_clone_watches_head_index_reflog_and_branch_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let pkg = workspace_repo(root);
        let git_dir = root.join(".git");

        let got = rerun_if_changed_paths(&pkg).expect("inside a git work tree");

        let mut expected = vec![
            git_dir.join("HEAD"),
            git_dir.join("index"),
            git_dir.join("logs").join("HEAD"),
            git_dir.join("refs").join("heads").join("main"),
        ];
        expected.extend(package_paths(root, &pkg));
        assert_eq!(canonical(got), canonical(expected));
    }

    #[test]
    fn linked_worktree_watches_its_own_head_and_the_shared_branch_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("main");
        std::fs::create_dir(&main).unwrap();
        workspace_repo(&main);
        let wt = tmp.path().join("wt");
        git(
            &main,
            &["worktree", "add", "-q", "-b", "wt", wt.to_str().unwrap()],
        );
        let wt_pkg = wt.join("crates").join("pkg");
        let common = main.join(".git");
        // In a linked worktree `.git` is a file; HEAD, index and the reflog
        // are per-worktree, the branch ref lives in the common dir.
        assert!(wt.join(".git").is_file());
        let wt_git = common.join("worktrees").join("wt");

        let got = rerun_if_changed_paths(&wt_pkg).expect("inside a git work tree");

        let mut expected = vec![
            wt_git.join("HEAD"),
            wt_git.join("index"),
            wt_git.join("logs").join("HEAD"),
            common.join("refs").join("heads").join("wt"),
        ];
        expected.extend(package_paths(&wt, &wt_pkg));
        assert_eq!(canonical(got), canonical(expected));
    }

    #[test]
    fn packed_branch_ref_watches_packed_refs_instead_of_the_missing_loose_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let pkg = workspace_repo(root);
        git(root, &["pack-refs", "--all", "--prune"]);
        let git_dir = root.join(".git");
        assert!(!git_dir.join("refs").join("heads").join("main").exists());

        let got = rerun_if_changed_paths(&pkg).expect("inside a git work tree");

        let mut expected = vec![
            git_dir.join("HEAD"),
            git_dir.join("index"),
            git_dir.join("logs").join("HEAD"),
            git_dir.join("packed-refs"),
        ];
        expected.extend(package_paths(root, &pkg));
        assert_eq!(canonical(got), canonical(expected));
    }

    #[test]
    fn detached_head_watches_head_without_any_branch_ref() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let pkg = workspace_repo(root);
        git(root, &["checkout", "-q", "--detach"]);
        let git_dir = root.join(".git");

        let got = rerun_if_changed_paths(&pkg).expect("inside a git work tree");

        let mut expected = vec![
            git_dir.join("HEAD"),
            git_dir.join("index"),
            git_dir.join("logs").join("HEAD"),
        ];
        expected.extend(package_paths(root, &pkg));
        assert_eq!(canonical(got), canonical(expected));
    }

    #[test]
    fn outside_a_git_work_tree_watches_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let inside_repo = Command::new("git")
            .args(["rev-parse", "--is-inside-work-tree"])
            .current_dir(tmp.path())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if inside_repo {
            // The temp dir sits inside some enclosing repository, so the "no
            // git" case cannot be constructed here. Say so rather than pass.
            eprintln!(
                "skipping: {} is inside a git work tree",
                tmp.path().display()
            );
            return;
        }
        std::fs::create_dir(tmp.path().join("src")).unwrap();

        assert_eq!(rerun_if_changed_paths(tmp.path()), None);
    }

    /// The real checkout this crate is being tested from: the watched HEAD and
    /// index must be the ones git itself uses, never a path under the package
    /// dir.
    #[test]
    fn this_checkout_watches_the_head_and_index_git_uses() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let Some(head) = Command::new("git")
            .args(["rev-parse", "--git-path", "HEAD", "--git-path", "index"])
            .current_dir(manifest_dir)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8(o.stdout).unwrap())
        else {
            eprintln!("skipping: not built from a git checkout");
            return;
        };
        let git_files = canonical(head.lines().map(|l| manifest_dir.join(l)));

        let got = canonical(rerun_if_changed_paths(manifest_dir).expect("inside a git work tree"));

        assert!(
            git_files.is_subset(&got),
            "watched {got:?}, missing git's own {git_files:?}"
        );
        assert!(!got.contains(&manifest_dir.join(".git").join("HEAD")));
    }
}
