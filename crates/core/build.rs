use std::process::Command;

fn main() {
    // Emit build metadata for startup logging
    emit_build_metadata();

    // Emit min-compatible version for range-based version checking
    emit_min_compatible_version();

    // On Windows, embed an application manifest that declares ComCtl32 v6
    // dependency. Without this, native-windows-gui crashes at startup with
    // "GetWindowSubclass Entry Point Not Found" because Windows loads the
    // legacy ComCtl32 v5 by default. Also embeds the Freenet icon.
    #[cfg(target_os = "windows")]
    {
        let mut res = winres::WindowsResource::new();
        res.set_manifest_file("freenet.manifest");
        res.set_icon("src/bin/commands/assets/freenet.ico");
        res.compile().expect("failed to compile Windows resources");
    }

    // Flatbuffers codegen is intentionally NOT run automatically.
    // The generated file (src/generated/topology_generated.rs) is checked in
    // and only needs regeneration when schemas/flatbuffers/topology.fbs changes.
    //
    // To regenerate:
    //   flatc --rust -o crates/core/src/generated ../../schemas/flatbuffers/topology.fbs
    //   cargo fmt -p freenet
}

fn emit_min_compatible_version() {
    // Priority for min-compatible version:
    // 1. FREENET_MIN_COMPATIBLE_VERSION env var (set by release.sh)
    // 2. package.metadata.freenet.min-compatible-version in Cargo.toml
    // 3. CARGO_PKG_VERSION (strict match — same as old behavior)
    //
    // Source (2) ensures cross-compile CI builds (which don't set the env var)
    // still get the correct min-compatible version from the committed Cargo.toml.
    let pkg_version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let min_compat = std::env::var("FREENET_MIN_COMPATIBLE_VERSION")
        .ok()
        .or_else(read_min_compatible_from_cargo_toml)
        .unwrap_or_else(|| pkg_version.clone());

    // Validate min_compatible format and constraints.
    let pkg_parts: Vec<&str> = pkg_version.split('.').collect();
    let min_parts: Vec<&str> = min_compat.split('.').collect();

    // Must be a valid X.Y.Z version.
    if min_parts.len() < 3 {
        panic!("FREENET_MIN_COMPATIBLE_VERSION ({min_compat}) must be in X.Y.Z format");
    }

    // Must share major.minor (wire format only encodes min_patch).
    if pkg_parts.len() >= 2 && (pkg_parts[0] != min_parts[0] || pkg_parts[1] != min_parts[1]) {
        panic!(
            "FREENET_MIN_COMPATIBLE_VERSION ({min_compat}) must share major.minor \
             with package version ({pkg_version}). The wire format only encodes \
             min_patch; major.minor is inherited from the version field."
        );
    }

    // Must not exceed the package version (would reject all peers including ourselves).
    // Compare numerically, not lexicographically ("0.1.9" > "0.1.152" in string order).
    let min_patch: u64 = min_parts[2].parse().unwrap_or_else(|_| {
        panic!("FREENET_MIN_COMPATIBLE_VERSION ({min_compat}) has non-numeric patch")
    });
    let pkg_patch: u64 = pkg_parts[2]
        .parse()
        .unwrap_or_else(|_| panic!("CARGO_PKG_VERSION ({pkg_version}) has non-numeric patch"));
    if min_patch > pkg_patch {
        panic!(
            "FREENET_MIN_COMPATIBLE_VERSION ({min_compat}) must be <= package version \
             ({pkg_version}). A min_compatible higher than our own version would \
             reject all peers."
        );
    }

    println!("cargo:rustc-env=FREENET_MIN_COMPATIBLE_VERSION={min_compat}");
    println!("cargo:rerun-if-env-changed=FREENET_MIN_COMPATIBLE_VERSION");
}

/// Read min-compatible-version from [package.metadata.freenet] in Cargo.toml.
/// Returns None if the field is missing or unreadable.
fn read_min_compatible_from_cargo_toml() -> Option<String> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").ok()?;
    let cargo_toml = std::path::Path::new(&manifest_dir).join("Cargo.toml");
    let contents = std::fs::read_to_string(cargo_toml).ok()?;
    // Simple line-based parsing — avoids adding a toml dependency to build.rs.
    // Looks for: min-compatible-version = "X.Y.Z"
    for line in contents.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("min-compatible-version") {
            if let Some(value) = trimmed.split('=').nth(1) {
                let version = value.trim().trim_matches('"').trim().to_string();
                if !version.is_empty() {
                    return Some(version);
                }
            }
        }
    }
    None
}

fn emit_build_metadata() {
    // Git commit hash
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}");

    // Git dirty flag
    let git_dirty = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    let dirty_suffix = if git_dirty { "-dirty" } else { "" };
    println!("cargo:rustc-env=GIT_DIRTY={dirty_suffix}");

    // Build timestamp (ISO 8601)
    let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    println!("cargo:rustc-env=BUILD_TIMESTAMP={timestamp}");

    // Rebuild if git HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
