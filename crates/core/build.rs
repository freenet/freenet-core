use std::process::Command;

fn main() {
    // Emit build metadata for startup logging
    emit_build_metadata();

    // Emit min-compatible version for range-based version checking
    emit_min_compatible_version();

    // Flatbuffers codegen is intentionally NOT run automatically.
    // The generated file (src/generated/topology_generated.rs) is checked in
    // and only needs regeneration when schemas/flatbuffers/topology.fbs changes.
    //
    // To regenerate:
    //   flatc --rust -o crates/core/src/generated ../../schemas/flatbuffers/topology.fbs
    //   cargo fmt -p freenet
}

fn emit_min_compatible_version() {
    // Allow overriding min-compatible version at build time.
    // Default: current package version (strict match, same as old behavior).
    let pkg_version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let min_compat =
        std::env::var("FREENET_MIN_COMPATIBLE_VERSION").unwrap_or_else(|_| pkg_version.clone());

    // Validate: min_compatible must share major.minor with the package version.
    // The wire format only encodes min_patch (major.minor inherited from version).
    let pkg_parts: Vec<&str> = pkg_version.split('.').collect();
    let min_parts: Vec<&str> = min_compat.split('.').collect();
    if pkg_parts.len() >= 2
        && min_parts.len() >= 2
        && (pkg_parts[0] != min_parts[0] || pkg_parts[1] != min_parts[1])
    {
        panic!(
            "FREENET_MIN_COMPATIBLE_VERSION ({min_compat}) must share major.minor \
             with package version ({pkg_version}). The wire format only encodes \
             min_patch; major.minor is inherited from the version field."
        );
    }

    println!("cargo:rustc-env=FREENET_MIN_COMPATIBLE_VERSION={min_compat}");
    println!("cargo:rerun-if-env-changed=FREENET_MIN_COMPATIBLE_VERSION");
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
