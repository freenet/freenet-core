//! Regression tests for issue #3995: the freenet crate's binstall metadata
//! lacked a Windows override, so `cargo binstall freenet` 404'd on
//! `x86_64-pc-windows-msvc` (the release uploads `freenet-…-msvc.zip`, not
//! `.tar.gz`).

use toml::Value;

const CARGO_TOML: &str = include_str!("../Cargo.toml");

fn manifest() -> Value {
    toml::from_str(CARGO_TOML).expect("freenet Cargo.toml should be valid TOML")
}

fn binstall_table<'a>(manifest: &'a Value, override_target: Option<&str>) -> &'a Value {
    let binstall = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("binstall"))
        .expect("freenet should have a [package.metadata.binstall] block");

    match override_target {
        Some(target) => binstall
            .get("overrides")
            .and_then(|o| o.get(target))
            .unwrap_or_else(|| panic!("freenet binstall should override target {target}")),
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

#[test]
fn default_pkg_url_uses_version_template() {
    // Unlike fdev (whose crate version diverges from the release tag),
    // freenet's crate version IS the release tag, so the standard
    // `v{ version }` template resolves correctly. Lock that in so a
    // well-meaning copy of fdev's literal-tag pattern doesn't accidentally
    // break freenet.
    let manifest = manifest();
    let url = pkg_field(&manifest, None, "pkg-url");
    assert!(
        url.contains("v{ version }"),
        "freenet default pkg-url '{url}' should keep the `v{{ version }}` \
         template — for freenet, the crate version matches the release tag"
    );
    assert!(
        url.ends_with(".tar.gz"),
        "freenet default pkg-url '{url}' should target the .tar.gz asset"
    );
}

#[test]
fn windows_override_uses_zip_archive() {
    let manifest = manifest();
    assert_eq!(
        pkg_field(&manifest, Some("x86_64-pc-windows-msvc"), "pkg-fmt"),
        "zip",
        "windows override pkg-fmt"
    );
    let pkg_url = pkg_field(&manifest, Some("x86_64-pc-windows-msvc"), "pkg-url");
    assert!(
        pkg_url.ends_with(".zip"),
        "windows pkg-url '{pkg_url}' must end with .zip — release \
         workflow uploads `freenet-x86_64-pc-windows-msvc.zip`, not .tar.gz"
    );
}

#[test]
fn bin_dir_uses_correct_executable_name() {
    let manifest = manifest();
    assert_eq!(
        pkg_field(&manifest, None, "bin-dir"),
        "freenet",
        "default bin-dir must match the unix binary name"
    );
    assert_eq!(
        pkg_field(&manifest, Some("x86_64-pc-windows-msvc"), "bin-dir"),
        "freenet.exe",
        "windows bin-dir must include the .exe suffix — without it, \
         binstall extracts the archive but cannot locate the binary"
    );
}
