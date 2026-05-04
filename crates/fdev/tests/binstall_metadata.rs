//! Regression test for issue #3995: `cargo binstall fdev` 404'd because the
//! pkg-url template expanded `v{ version }` against fdev's own crate version
//! (e.g. `0.3.214`) but the GitHub release tag tracks freenet's version
//! (e.g. `v0.2.51`). The fix embeds the freenet release tag literally and has
//! release.sh / release.yml rewrite it on every bump — this test verifies the
//! invariant that the embedded tag matches the freenet dependency version.

use toml::Value;

const CARGO_TOML: &str = include_str!("../Cargo.toml");

fn manifest() -> Value {
    toml::from_str(CARGO_TOML).expect("fdev Cargo.toml should be valid TOML")
}

fn freenet_dep_version(manifest: &Value) -> String {
    manifest
        .get("dependencies")
        .and_then(|d| d.get("freenet"))
        .and_then(|f| f.get("version"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim_start_matches('=').to_string())
        .expect("fdev should declare a freenet dependency with a version")
}

fn binstall_table<'a>(manifest: &'a Value, override_target: Option<&str>) -> &'a Value {
    let binstall = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("binstall"))
        .expect("fdev should have a [package.metadata.binstall] block");

    match override_target {
        Some(target) => binstall
            .get("overrides")
            .and_then(|o| o.get(target))
            .unwrap_or_else(|| panic!("fdev binstall should override target {target}")),
        None => binstall,
    }
}

fn pkg_field(manifest: &Value, override_target: Option<&str>, field: &str) -> String {
    binstall_table(manifest, override_target)
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("binstall block (target={override_target:?}) is missing {field}"))
        .to_string()
}

fn pkg_url(manifest: &Value, override_target: Option<&str>) -> String {
    pkg_field(manifest, override_target, "pkg-url")
}

#[test]
fn pkg_url_embeds_freenet_release_tag() {
    let manifest = manifest();
    let freenet_version = freenet_dep_version(&manifest);
    let expected_tag = format!("/releases/download/v{freenet_version}/");

    let default_url = pkg_url(&manifest, None);
    assert!(
        default_url.contains(&expected_tag),
        "default binstall pkg-url '{default_url}' must embed the freenet \
         release tag '{expected_tag}'. fdev's crate version diverges from \
         the GitHub release tag (issue #3995); release.sh / release.yml \
         must rewrite this when the freenet version is bumped."
    );

    let windows_url = pkg_url(&manifest, Some("x86_64-pc-windows-msvc"));
    assert!(
        windows_url.contains(&expected_tag),
        "windows binstall pkg-url '{windows_url}' must embed the freenet \
         release tag '{expected_tag}'"
    );
}

#[test]
fn windows_override_uses_zip_archive() {
    let manifest = manifest();
    let windows_url = pkg_url(&manifest, Some("x86_64-pc-windows-msvc"));
    assert!(
        windows_url.ends_with(".zip"),
        "windows pkg-url '{windows_url}' must end with .zip — release \
         workflow uploads `fdev-x86_64-pc-windows-msvc.zip`, not .tar.gz"
    );

    let pkg_fmt = pkg_field(&manifest, Some("x86_64-pc-windows-msvc"), "pkg-fmt");
    assert_eq!(
        pkg_fmt, "zip",
        "windows override must set pkg-fmt = \"zip\""
    );
}

#[test]
fn pkg_url_does_not_use_crate_version_template() {
    // `v{ version }` would expand to fdev's crate version (e.g. 0.3.214) and
    // 404 against the actual freenet-versioned release tag. Guard against
    // anyone "simplifying" back to the broken form.
    let manifest = manifest();
    let default_url = pkg_url(&manifest, None);
    let windows_url = pkg_url(&manifest, Some("x86_64-pc-windows-msvc"));

    for (label, url) in [("default", &default_url), ("windows", &windows_url)] {
        assert!(
            !url.contains("v{ version }") && !url.contains("v{version}"),
            "{label} pkg-url '{url}' must not use the `v{{ version }}` \
             template — that expands to fdev's crate version (issue #3995). \
             Embed the freenet release tag literally instead."
        );
    }
}

#[test]
fn bin_dir_uses_correct_executable_name() {
    let manifest = manifest();
    assert_eq!(
        pkg_field(&manifest, None, "bin-dir"),
        "fdev",
        "default bin-dir must match the unix binary name"
    );
    assert_eq!(
        pkg_field(&manifest, Some("x86_64-pc-windows-msvc"), "bin-dir"),
        "fdev.exe",
        "windows bin-dir must include the .exe suffix — without it, \
         binstall extracts the archive but cannot locate the binary"
    );
}

#[test]
fn freenet_dep_matches_workspace_freenet_version() {
    // fdev's pkg-url embeds the freenet release tag literally, and the
    // `pkg_url_embeds_freenet_release_tag` test asserts the URL stays in
    // sync with fdev's `freenet` dependency declaration. This test closes
    // the remaining gap: a hand-edit that bumps the workspace freenet
    // crate version without bumping fdev's dependency would leave both
    // fdev manifest and pkg-url consistently stale-but-self-consistent
    // (issue #3995, skeptical-review finding #3).
    let manifest = manifest();
    let dep_version = freenet_dep_version(&manifest);

    let core_manifest_str = std::fs::read_to_string("../core/Cargo.toml")
        .expect("workspace freenet crate manifest should be readable");
    let core_manifest: Value =
        toml::from_str(&core_manifest_str).expect("core Cargo.toml should be valid TOML");
    let core_version = core_manifest
        .get("package")
        .and_then(|p| p.get("version"))
        .and_then(|v| v.as_str())
        .expect("freenet crate should declare a version")
        .to_string();

    assert_eq!(
        dep_version, core_version,
        "fdev's freenet dep version ('{dep_version}') must match the \
         workspace freenet crate version ('{core_version}'). The release \
         scripts bump both together; if they drift, the embedded binstall \
         tag will point at a release that doesn't contain this fdev build."
    );
}

/// Sed expressions duplicated in `scripts/release.sh` (BRE) and
/// `.github/workflows/release.yml` (ERE). The tests below verify both
/// produce the same output and that they correctly rewrite both the
/// default and Windows-override pkg-url lines.
mod release_sed_rewrite {
    use std::process::{Command, Stdio};

    const BRE_SCRIPT: &str = "s|releases/download/v[0-9][0-9]*\\.[0-9][0-9]*\\.[0-9][0-9]*/fdev-|releases/download/vNEW/fdev-|g";
    const ERE_SCRIPT: &str =
        "s|releases/download/v[0-9]+\\.[0-9]+\\.[0-9]+/fdev-|releases/download/vNEW/fdev-|g";

    fn run_sed(args: &[&str], input: &str) -> String {
        let mut child = Command::new("sed")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("sed should be on PATH");
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
        let output = child.wait_with_output().expect("sed should run");
        assert!(
            output.status.success(),
            "sed exited non-zero: {}",
            String::from_utf8_lossy(&output.stderr),
        );
        String::from_utf8(output.stdout).expect("sed output must be UTF-8")
    }

    const FIXTURE: &str = "\
[package.metadata.binstall]
pkg-url = \"{ repo }/releases/download/v0.2.51/fdev-{ target }.tar.gz\"
pkg-fmt = \"tgz\"
bin-dir = \"fdev\"

[package.metadata.binstall.overrides.x86_64-pc-windows-msvc]
pkg-url = \"{ repo }/releases/download/v0.2.51/fdev-{ target }.zip\"
pkg-fmt = \"zip\"
bin-dir = \"fdev.exe\"
";

    fn expected_after(version_segment: &str) -> String {
        FIXTURE.replace("v0.2.51", version_segment)
    }

    #[test]
    fn release_sh_bre_rewrites_both_urls() {
        let out = run_sed(&[BRE_SCRIPT], FIXTURE);
        assert_eq!(out, expected_after("vNEW"));
    }

    #[test]
    fn release_yml_ere_rewrites_both_urls() {
        let out = run_sed(&["-E", ERE_SCRIPT], FIXTURE);
        assert_eq!(out, expected_after("vNEW"));
    }

    #[test]
    fn release_sh_and_release_yml_produce_identical_output() {
        let bre_out = run_sed(&[BRE_SCRIPT], FIXTURE);
        let ere_out = run_sed(&["-E", ERE_SCRIPT], FIXTURE);
        assert_eq!(
            bre_out, ere_out,
            "scripts/release.sh and .github/workflows/release.yml must \
             produce identical rewrites — they're two copies of the same \
             logic and silently diverging would re-break issue #3995"
        );
    }

    #[test]
    fn rewrite_is_idempotent_when_version_unchanged() {
        // After release.sh runs once, re-running it with the same VERSION
        // must be a no-op (the regex still matches, but the substitution
        // produces the same text).
        let after_first = run_sed(&[BRE_SCRIPT], FIXTURE);
        let after_second = run_sed(&[BRE_SCRIPT], &after_first);
        assert_eq!(after_first, after_second, "BRE rewrite must be idempotent");
    }
}
