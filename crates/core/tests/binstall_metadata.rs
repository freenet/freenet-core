//! Regression test for issue #3995: the freenet crate's binstall metadata
//! lacked a Windows override, so `cargo binstall freenet` 404'd on
//! `x86_64-pc-windows-msvc` (the release uploads `freenet-…-msvc.zip`, not
//! `.tar.gz`).

use toml::Value;

const CARGO_TOML: &str = include_str!("../Cargo.toml");

fn manifest() -> Value {
    toml::from_str(CARGO_TOML).expect("freenet Cargo.toml should be valid TOML")
}

#[test]
fn windows_override_uses_zip_archive() {
    let manifest = manifest();
    let windows = manifest
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("binstall"))
        .and_then(|b| b.get("overrides"))
        .and_then(|o| o.get("x86_64-pc-windows-msvc"))
        .expect(
            "freenet should override x86_64-pc-windows-msvc binstall metadata \
             — the release uploads .zip on Windows, not .tar.gz",
        );

    let pkg_fmt = windows.get("pkg-fmt").and_then(|f| f.as_str());
    assert_eq!(
        pkg_fmt,
        Some("zip"),
        "windows override must set pkg-fmt = \"zip\""
    );

    let pkg_url = windows
        .get("pkg-url")
        .and_then(|u| u.as_str())
        .expect("windows override must define pkg-url");
    assert!(
        pkg_url.ends_with(".zip"),
        "windows pkg-url '{pkg_url}' must end with .zip"
    );
}
