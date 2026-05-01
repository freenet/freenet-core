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

fn pkg_url(manifest: &Value, override_target: Option<&str>) -> String {
    let binstall = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("binstall"))
        .expect("fdev should have a [package.metadata.binstall] block");

    let table = match override_target {
        Some(target) => binstall
            .get("overrides")
            .and_then(|o| o.get(target))
            .unwrap_or_else(|| panic!("fdev binstall should override target {target}")),
        None => binstall,
    };

    table
        .get("pkg-url")
        .and_then(|u| u.as_str())
        .unwrap_or_else(|| panic!("binstall block (target={override_target:?}) is missing pkg-url"))
        .to_string()
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

    let pkg_fmt = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("binstall"))
        .and_then(|b| b.get("overrides"))
        .and_then(|o| o.get("x86_64-pc-windows-msvc"))
        .and_then(|w| w.get("pkg-fmt"))
        .and_then(|f| f.as_str());
    assert_eq!(
        pkg_fmt,
        Some("zip"),
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
