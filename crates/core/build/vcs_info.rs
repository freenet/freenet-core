//! The commit a crates.io package was published from.
//!
//! `cargo publish` writes `.cargo_vcs_info.json` into every package it
//! uploads, with the commit as `git.sha1`. A `cargo install freenet` builds
//! from that package, which has no `.git`, so `git rev-parse` finds nothing;
//! this file is the only record of the commit left. `build.rs` reads it, and when it
//! is present it replaces both git probes (see [`resolve_git_provenance`]).
//!
//! Shared by `build.rs` (via `#[path]`) and a `#[cfg(test)]` module in
//! `lib.rs`, so it must stay dependency-free: the build script sees only
//! `[build-dependencies]`.

/// The name `cargo publish` gives the file, in the package root.
pub(crate) const VCS_INFO_FILE: &str = ".cargo_vcs_info.json";

/// What `cargo publish` recorded about the source it packaged.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Published {
    /// The commit, shortened to the 12 characters `git rev-parse --short=12`
    /// gives.
    pub(crate) commit: String,
    /// `cargo publish --allow-dirty` records `"dirty": true`.
    pub(crate) dirty: bool,
}

/// The commit and dirty flag in `contents` (the text of [`VCS_INFO_FILE`]), or
/// `None` if there is no `"sha1"` holding exactly 40 hex characters.
///
/// The commit becomes a `cargo:rustc-env` directive, which is newline
/// delimited, so anything that is not plain hex is refused rather than
/// trimmed into shape.
pub(crate) fn published(contents: &str) -> Option<Published> {
    let sha = json_value(contents, "sha1")?;
    let sha = sha.strip_prefix('"')?;
    let sha = &sha[..sha.find('"')?];
    if sha.len() != 40 || !sha.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let dirty = json_value(contents, "dirty").is_some_and(|v| v.starts_with("true"));
    Some(Published {
        commit: sha[..12].to_ascii_lowercase(),
        dirty,
    })
}

/// The text after `"key":` in `contents`, leading whitespace removed. Enough
/// for the two keys read here: the file is machine-written and flat.
fn json_value<'a>(contents: &'a str, key: &str) -> Option<&'a str> {
    let quoted = format!("\"{key}\"");
    let after_key = &contents[contents.find(&quoted)? + quoted.len()..];
    Some(after_key.trim_start().strip_prefix(':')?.trim_start())
}

/// The commit and dirty flag `build.rs` reports (`GIT_COMMIT_HASH`,
/// `GIT_DIRTY`), decided from every source of them.
///
/// `GIT_DIRTY` is the auto-update kill switch (`auto_update_is_disabled` in
/// `src/bin/freenet.rs`), so this order is load-bearing.
/// - An override (`FREENET_GIT_COMMIT_HASH`, `FREENET_GIT_IS_DIRTY`) wins, per
///   field.
/// - Otherwise a packaged source (`published` is `Some`) takes both from the
///   package and runs NEITHER git probe. A package has no `.git` of its own, so
///   a probe finds only whatever repository happens to enclose the unpacked
///   source (a `CARGO_HOME` inside a tracked home directory), and would stamp
///   that repository's commit and untracked files on the binary.
/// - Otherwise the probes run, and a missing commit is `"unknown"`.
pub(crate) fn resolve_git_provenance(
    commit_override: Option<String>,
    dirty_override: Option<bool>,
    published: Option<Published>,
    probe_commit: impl FnOnce() -> Option<String>,
    probe_dirty: impl FnOnce() -> bool,
) -> (String, bool) {
    let (commit, dirty) = match published {
        Some(p) => (Some(p.commit), Some(p.dirty)),
        None => (None, None),
    };
    let commit = commit_override
        .or(commit)
        .or_else(probe_commit)
        .unwrap_or_else(|| "unknown".to_string());
    let dirty = dirty_override.or(dirty).unwrap_or_else(probe_dirty);
    (commit, dirty)
}

#[cfg(test)]
mod tests {
    use super::{Published, VCS_INFO_FILE, published, resolve_git_provenance};

    /// The name is cargo's, and `build/git_watch.rs` watches the same file.
    /// `build.rs` has no test harness, and the decision above only matters if
    /// the script feeds it and uses its result for BOTH stamps. Deleting the
    /// call, or going back to a direct git probe, would pass every other test
    /// while `GIT_DIRTY` (the auto-update kill switch) came from the wrong repo.
    #[test]
    fn build_rs_stamps_both_values_from_the_resolver() {
        let code: String = include_str!("../build.rs")
            .lines()
            .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
            .collect::<Vec<_>>()
            .join("\n");
        let start = code
            .find("fn emit_build_metadata()")
            .expect("emit_build_metadata not found");
        let (body, _) = code[start..]
            .split_once("\n}")
            .expect("could not locate the end of emit_build_metadata");
        let body: String = body.chars().filter(|c| !c.is_whitespace()).collect();
        let call = body
            .find("let(git_hash,git_dirty)=vcs_info::resolve_git_provenance(")
            .expect("build.rs must take both values from resolve_git_provenance");
        let stamp_commit = body
            .find("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}")
            .expect("GIT_COMMIT_HASH must be stamped from the resolver's commit");
        let suffix = body
            .find("letdirty_suffix=ifgit_dirty{\"-dirty\"}else{\"\"};")
            .expect("the dirty suffix must come from the resolver's flag");
        let stamp_dirty = body
            .find("cargo:rustc-env=GIT_DIRTY={dirty_suffix}")
            .expect("GIT_DIRTY must be stamped from the dirty suffix");
        assert!(call < stamp_commit && call < suffix && suffix < stamp_dirty);
        // The resolver is fed the package file build.rs read: passing `None`
        // brings back the enclosing-repository stamp with every other test
        // green.
        assert!(
            body.contains(
                "vcs_info::resolve_git_provenance(commit_override,dirty_override,published,"
            ),
            "build.rs must pass the published file's contents to the resolver"
        );
        assert!(
            body.contains("letpublished=")
                && body.contains("join(vcs_info::VCS_INFO_FILE)")
                && body.contains(".and_then(|contents|vcs_info::published(&contents));"),
            "`published` must be read from the package's VCS_INFO_FILE"
        );
        // Both git probes live inside the resolver's arguments, nowhere else.
        let probes: Vec<usize> = body
            .match_indices("Command::new(\"git\")")
            .map(|(at, _)| at)
            .collect();
        assert_eq!(probes.len(), 2, "expected the two git probes");
        assert!(
            probes.iter().all(|&at| at > call && at < stamp_commit),
            "a git probe runs outside the resolver"
        );
    }

    #[test]
    fn the_file_is_the_one_cargo_publish_writes() {
        assert_eq!(VCS_INFO_FILE, ".cargo_vcs_info.json");
    }

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    fn pkg(dirty: bool) -> Option<Published> {
        Some(Published {
            commit: "0123456789ab".to_string(),
            dirty,
        })
    }

    #[test]
    fn reads_the_commit_cargo_publish_writes() {
        let file = format!(
            "{{\n  \"git\": {{\n    \"sha1\": \"{SHA}\"\n  }},\n  \"path_in_vcs\": \"crates/core\"\n}}"
        );
        assert_eq!(published(&file), pkg(false));
        // Compact JSON.
        let compact = format!("{{\"git\":{{\"sha1\":\"{SHA}\"}}}}");
        assert_eq!(published(&compact), pkg(false));
    }

    #[test]
    fn reads_the_dirty_flag_of_an_allow_dirty_package() {
        let dirty = format!("{{\"git\":{{\"sha1\":\"{SHA}\",\"dirty\":true}}}}");
        assert_eq!(published(&dirty), pkg(true));
        let spaced = format!("{{\"git\": {{\"sha1\": \"{SHA}\", \"dirty\" : true}}}}");
        assert_eq!(published(&spaced), pkg(true));
        let explicit_false = format!("{{\"git\":{{\"sha1\":\"{SHA}\",\"dirty\":false}}}}");
        assert_eq!(published(&explicit_false), pkg(false));
    }

    #[test]
    fn refuses_anything_but_forty_hex_characters() {
        for bad in [
            String::new(),
            "{}".to_string(),
            "{\"git\":{\"sha1\":\"0123\"}}".to_string(),
            format!("{{\"git\":{{\"sha1\":\"{SHA}0\"}}}}"),
            format!("{{\"git\":{{\"sha1\":\"{}\"}}}}", "g".repeat(40)),
            format!(
                "{{\"git\":{{\"sha1\":\"0123456789ab\\ncargo:rustc-env=X=1{}\"}}}}",
                "0".repeat(10)
            ),
            format!("{{\"git\":{{\"sha1\":{SHA}}}}}"),
            "{\"git\":{\"sha1\":\"".to_string(),
        ] {
            assert_eq!(published(&bad), None, "{bad}");
        }
    }

    /// A packaged source unpacked inside someone else's dirty repository: the
    /// probes would report that repository, so they must not run at all.
    #[test]
    fn a_packaged_clean_source_inside_a_dirty_repo_is_clean() {
        let got = resolve_git_provenance(
            None,
            None,
            pkg(false),
            || panic!("commit probe ran"),
            || panic!("dirty probe ran"),
        );
        assert_eq!(got, ("0123456789ab".to_string(), false));
    }

    #[test]
    fn a_packaged_dirty_source_is_dirty() {
        let got = resolve_git_provenance(
            None,
            None,
            pkg(true),
            || panic!("commit probe ran"),
            || panic!("dirty probe ran"),
        );
        assert_eq!(got, ("0123456789ab".to_string(), true));
    }

    #[test]
    fn without_a_vcs_file_the_git_probes_decide() {
        let got = resolve_git_provenance(
            None,
            None,
            None,
            || Some("aaaaaaaaaaaa".to_string()),
            || true,
        );
        assert_eq!(got, ("aaaaaaaaaaaa".to_string(), true));
        let got = resolve_git_provenance(None, None, None, || None, || false);
        assert_eq!(got, ("unknown".to_string(), false));
    }

    #[test]
    fn the_overrides_win_over_the_vcs_file_per_field() {
        let got = resolve_git_provenance(
            Some("bbbbbbbbbbbb".to_string()),
            Some(false),
            pkg(true),
            || panic!("commit probe ran"),
            || panic!("dirty probe ran"),
        );
        assert_eq!(got, ("bbbbbbbbbbbb".to_string(), false));
        // One override leaves the other field to the package.
        let got = resolve_git_provenance(None, Some(true), pkg(false), || None, || false);
        assert_eq!(got, ("0123456789ab".to_string(), true));
        let got = resolve_git_provenance(Some("c".to_string()), None, pkg(true), || None, || false);
        assert_eq!(got, ("c".to_string(), true));
    }
}
