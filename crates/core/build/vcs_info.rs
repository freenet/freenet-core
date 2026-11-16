//! The commit a crates.io package was published from.
//!
//! `cargo publish` writes `.cargo_vcs_info.json` into every package it
//! uploads, with the commit as `git.sha1`. A `cargo install freenet` builds
//! from that package, which has no `.git`, so `git rev-parse` finds nothing;
//! this file is the only record of the commit left. `build.rs` reads it after
//! the git probe and before falling back to `"unknown"`.
//!
//! Shared by `build.rs` (via `#[path]`) and a `#[cfg(test)]` module in
//! `lib.rs`, so it must stay dependency-free: the build script sees only
//! `[build-dependencies]`.

/// The name `cargo publish` gives the file, in the package root.
pub(crate) const VCS_INFO_FILE: &str = ".cargo_vcs_info.json";

/// The commit in `contents` (the text of [`VCS_INFO_FILE`]), shortened to the
/// 12 characters `git rev-parse --short=12` gives, or `None` if there is no
/// `"sha1"` holding exactly 40 hex characters.
///
/// The value becomes a `cargo:rustc-env` directive, which is newline
/// delimited, so anything that is not plain hex is refused rather than
/// trimmed into shape.
pub(crate) fn published_commit(contents: &str) -> Option<String> {
    let after_key = &contents[contents.find("\"sha1\"")? + "\"sha1\"".len()..];
    let after_colon = after_key.trim_start().strip_prefix(':')?.trim_start();
    let value = after_colon.strip_prefix('"')?;
    let sha = &value[..value.find('"')?];
    (sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| sha[..12].to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::published_commit;

    const SHA: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn reads_the_commit_cargo_publish_writes() {
        let file = format!(
            "{{\n  \"git\": {{\n    \"sha1\": \"{SHA}\"\n  }},\n  \"path_in_vcs\": \"crates/core\"\n}}"
        );
        assert_eq!(published_commit(&file).as_deref(), Some("0123456789ab"));
        // Compact JSON and a dirty publish (`--allow-dirty` adds "dirty": true).
        let compact = format!("{{\"git\":{{\"sha1\":\"{SHA}\",\"dirty\":true}}}}");
        assert_eq!(published_commit(&compact).as_deref(), Some("0123456789ab"));
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
            assert_eq!(published_commit(&bad), None, "{bad}");
        }
    }
}
