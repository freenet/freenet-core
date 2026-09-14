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
/// Always includes the package's own sources and manifest. Any `rerun-if-*`
/// directive switches off cargo's default of rerunning when any package file
/// changes, and `build.rs` emits `rerun-if-env-changed` regardless, so
/// without these a build with no git (a crates.io tarball, or a Docker build
/// whose context excludes `.git`) would never refresh `BUILD_TIMESTAMP` after
/// an edit. Inside a git work tree it adds the git files that change when the
/// commit or the working-tree state does.
pub(crate) fn rerun_if_changed_paths(manifest_dir: &Path) -> Vec<PathBuf> {
    // `GIT_DIRTY` gates auto-update (#3245), so it must also follow edits to
    // what the binary is built from, which do not touch any git file until
    // they are staged. Cargo recompiles `freenet` after such an edit anyway,
    // so rerunning the script then costs nothing extra.
    //
    // `scripts/` is here because `macos-bundle-updater.sh` is embedded in the
    // shipped binary by `include_str!`. Unstaged edits elsewhere in the
    // repository (other crates, docs, this package's tests) deliberately do
    // NOT rerun it, so `GIT_DIRTY` can read clean while `git status` does
    // not: none of those files is compiled into the `freenet` binary, whose
    // only in-repo path dependency is a dev-dependency. A new embedded file
    // outside these paths fails `every_embedded_file_is_watched_or_test_only`.
    // Any write under these paths reruns the script, editor swap files
    // included; that costs an extra recompile only when nothing else changed.
    let mut watched = vec![
        manifest_dir.join("src"),
        manifest_dir.join("scripts"),
        manifest_dir.join("Cargo.toml"),
        manifest_dir.join("Cargo.lock"),
    ];
    watched.extend(git_metadata_paths(manifest_dir).unwrap_or_default());
    watched.retain(|p| p.exists());
    watched
}

/// The git files behind `GIT_COMMIT_HASH` and `GIT_DIRTY`, plus the workspace
/// manifest and lockfile, or `None` outside a git work tree or without git.
/// May include paths that do not exist; the caller filters them.
fn git_metadata_paths(manifest_dir: &Path) -> Option<Vec<PathBuf>> {
    // `--git-path` resolves the real locations, including a linked worktree,
    // whose HEAD and index live in `.git/worktrees/<name>/` while its branch
    // refs and `packed-refs` live in the common dir. Paths come back relative
    // to the directory git ran in, so they are resolved against it.
    let resolved = git_paths(
        manifest_dir,
        &[
            "rev-parse",
            "--show-toplevel",
            "--git-dir",
            "--git-path",
            "HEAD",
            "--git-path",
            "index",
            "--git-path",
            "logs/HEAD",
            "--git-path",
            "packed-refs",
            "--git-path",
            "reftable/tables.list",
            "--git-common-dir",
        ],
    )?;
    let [
        toplevel,
        git_dir,
        head,
        index,
        head_log,
        packed_refs,
        reftable,
        common_dir,
    ] = resolved.as_slice()
    else {
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
    // With `core.logAllRefUpdates=false` there is no reflog, so that first
    // commit onto a packed branch goes unnoticed until the next watched
    // change; watching `refs/heads/` instead would rerun every worktree's
    // build on any other worktree's commit.
    //
    // A reftable repository (the default for new repositories from Git 3.0)
    // keeps HEAD and every ref in `reftable/`, whose `tables.list` is
    // rewritten on each ref update: per-worktree refs under this worktree's
    // git dir, branches under the common dir. Neither exists in a repository
    // using loose files, so both are dropped there.
    let mut watched = vec![
        head.clone(),
        index.clone(),
        head_log.clone(),
        reftable.clone(),
        common_dir.join("reftable").join("tables.list"),
    ];
    let branch_ref = git_stdout(manifest_dir, &["symbolic-ref", "-q", "HEAD"])
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if let Some(branch_ref) = branch_ref {
        match git_paths(manifest_dir, &["rev-parse", "--git-path", &branch_ref]).as_deref() {
            Some([loose]) if loose.exists() => watched.push(loose.clone()),
            _ => watched.push(packed_refs.clone()),
        }
    }

    // Before the first commit the index, the reflog and the branch ref do not
    // exist yet, and it is the first `git add` or `git commit` that creates
    // them, so none of the files above would notice it. Watch this worktree's
    // git dir itself until HEAD resolves; the rerun that follows switches to
    // the precise list.
    if git_stdout(manifest_dir, &["rev-parse", "-q", "--verify", "HEAD"]).is_none() {
        watched.push(git_dir.clone());
    }

    watched.extend([toplevel.join("Cargo.toml"), toplevel.join("Cargo.lock")]);
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

    /// Run git in `dir`, isolated from the machine and the caller: no global
    /// or system config (signing, hooks, templates, default branch or ref
    /// format), and none of the variables a git hook exports. A hook that
    /// runs `cargo test` hands its children `GIT_DIR`/`GIT_INDEX_FILE` for
    /// the real repository, and the fixtures' `init`, `add` and `commit`
    /// would otherwise write there. (`rerun_if_changed_paths` itself only
    /// reads, so under such a hook these tests fail rather than do damage.)
    fn git(dir: &Path, args: &[&str]) {
        let status = Command::new("git")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .env_remove("GIT_COMMON_DIR")
            .env_remove("GIT_OBJECT_DIRECTORY")
            .env_remove("GIT_PREFIX")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "-c",
                "user.name=freenet-test",
                "-c",
                "user.email=test@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "-c",
                "core.logAllRefUpdates=true",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "init.templateDir=",
                // The fixtures assert the loose-file layout; reftable (the
                // Git 3.0 default) is covered by the watched-path list, not
                // by these fixtures.
                "-c",
                "init.defaultRefFormat=files",
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
        std::fs::create_dir_all(pkg.join("scripts")).unwrap();
        std::fs::write(pkg.join("scripts").join("embedded.sh"), "").unwrap();
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
            pkg.join("scripts"),
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

        let got = rerun_if_changed_paths(&pkg);

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

        let got = rerun_if_changed_paths(&wt_pkg);

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

        let got = rerun_if_changed_paths(&pkg);

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

        let got = rerun_if_changed_paths(&pkg);

        let mut expected = vec![
            git_dir.join("HEAD"),
            git_dir.join("index"),
            git_dir.join("logs").join("HEAD"),
        ];
        expected.extend(package_paths(root, &pkg));
        assert_eq!(canonical(got), canonical(expected));
    }

    /// Before the first commit the index, reflog and branch ref do not exist,
    /// and the first `git add`/`git commit` creates them, so the git dir itself
    /// is watched until HEAD resolves.
    #[test]
    fn unborn_head_watches_the_git_dir_until_the_first_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let pkg = root.join("crates").join("pkg");
        std::fs::create_dir_all(pkg.join("src")).unwrap();
        std::fs::write(pkg.join("src").join("lib.rs"), "").unwrap();
        git(root, &["init", "-q"]);
        let git_dir = root.join(".git").canonicalize().unwrap();

        let unborn = canonical(rerun_if_changed_paths(&pkg));
        assert!(unborn.contains(&git_dir), "watched {unborn:?}");

        git(root, &["add", "-A"]);
        git(root, &["commit", "-q", "-m", "init"]);
        let born = canonical(rerun_if_changed_paths(&pkg));
        assert!(!born.contains(&git_dir), "watched {born:?}");
        assert!(born.contains(&git_dir.join("index")), "watched {born:?}");
    }

    /// Any `rerun-if-*` directive disables cargo's default package scan, and
    /// `build.rs` always emits one, so without git the package sources must
    /// still be watched or `BUILD_TIMESTAMP` would never refresh after an edit.
    #[test]
    fn outside_a_git_work_tree_watches_only_the_package() {
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
        std::fs::write(tmp.path().join("Cargo.toml"), "").unwrap();

        assert_eq!(
            canonical(rerun_if_changed_paths(tmp.path())),
            canonical([tmp.path().join("src"), tmp.path().join("Cargo.toml")])
        );
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

        let watched = rerun_if_changed_paths(manifest_dir);
        // A watched path that does not exist is what made every build rerun
        // (#5667), so check the raw paths before canonicalising them.
        for path in &watched {
            assert!(path.exists(), "{} is watched but missing", path.display());
        }
        let got = canonical(watched);

        assert!(
            git_files.is_subset(&got),
            "watched {got:?}, missing git's own {git_files:?}"
        );
    }

    /// Every file this crate embeds with `include_str!`/`include_bytes!` from
    /// outside `src/` must be watched, so that editing it refreshes
    /// `GIT_DIRTY` (#3245), or be named below as embedded only by tests.
    #[test]
    fn every_embedded_file_is_watched_or_test_only() {
        // Repo-relative; each is embedded only inside a `#[cfg(test)]` module.
        const TEST_ONLY: &[&str] = &[
            "docs/architecture/contracts/README.md",
            "docker/freenet-node/Dockerfile",
            "docker/freenet-node/release-signing-key.der",
        ];
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let src = manifest_dir.join("src").canonicalize().unwrap();
        let repo_root = manifest_dir.join("../..").canonicalize().unwrap();
        let watched = canonical(rerun_if_changed_paths(manifest_dir));

        let mut embedded = BTreeSet::new();
        let mut dirs = vec![src.clone()];
        while let Some(dir) = dirs.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap();
                for mac in ["include_str!(", "include_bytes!("] {
                    for (at, _) in text.match_indices(mac) {
                        let rest = &text[at + mac.len()..];
                        let Some(open) = rest.find('"') else {
                            continue;
                        };
                        // A literal path directly inside the parens only.
                        if !rest[..open].trim().is_empty() {
                            continue;
                        }
                        let Some(len) = rest[open + 1..].find('"') else {
                            continue;
                        };
                        let target = path.parent().unwrap().join(&rest[open + 1..open + 1 + len]);
                        // Prose quoting the macro may name no real file.
                        if let Ok(target) = target.canonicalize() {
                            embedded.insert(target);
                        }
                    }
                }
            }
        }

        let outside_src: Vec<_> = embedded.iter().filter(|t| !t.starts_with(&src)).collect();
        assert!(
            !outside_src.is_empty(),
            "found no file embedded from outside src/, but update.rs embeds \
             scripts/macos-bundle-updater.sh: the scan above is broken"
        );
        for target in outside_src {
            let rel = target.strip_prefix(&repo_root).unwrap_or(target);
            let is_watched = watched.iter().any(|w| target.starts_with(w));
            let test_only = TEST_ONLY.iter().any(|t| rel == Path::new(t));
            assert!(
                is_watched || test_only,
                "{} is embedded from outside src/ but build.rs does not watch \
                 it, so editing it leaves GIT_DIRTY stale. Watch it in \
                 build/git_watch.rs, or add it to TEST_ONLY if only tests embed it.",
                rel.display()
            );
        }
    }
}
