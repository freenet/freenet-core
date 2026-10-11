use chrono::TimeZone;
use std::process::Command;

// Unit-tested from the library via a `#[cfg(test)]` module in `src/lib.rs`.
#[path = "build/git_watch.rs"]
mod git_watch;

fn main() {
    // Emit build metadata for startup logging
    emit_build_metadata();

    // Emit min-compatible version for range-based version checking
    emit_min_compatible_version();

    // On Windows, embed an application manifest that declares ComCtl32 v6
    // dependency. Required by tray-icon/muda for modern Common Controls
    // (context menus, tooltips). Also embeds the Freenet icon.
    //
    // Uses #[cfg] to gate on the host OS. The winres crate is only available
    // as a build-dependency on Windows hosts (cfg(windows) in Cargo.toml).
    // Since we build Windows binaries natively on windows-latest CI runners
    // (not cross-compiled from Linux), this is correct.
    #[cfg(target_os = "windows")]
    {
        let mut res = winres::WindowsResource::new();
        res.set_manifest_file("freenet.manifest");
        res.set_icon("src/bin/commands/assets/freenet.ico");
        res.compile().expect("failed to compile Windows resources");
        println!("cargo:rerun-if-changed=freenet.manifest");
        println!("cargo:rerun-if-changed=src/bin/commands/assets/freenet.ico");
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
    // Git commit hash. `FREENET_GIT_COMMIT_HASH` supplies it for builds with no
    // reachable repository -- a release tarball, a `nix build`, a distro source
    // drop. EMPTY (or unset) means "no override" and falls through to the probe
    // below, which yields "unknown" when there is no repository either.
    //
    // A caller with no VCS information MUST pass empty rather than a placeholder
    // like "unknown": the validation below rejects a non-hex value outright, so
    // a placeholder turns "I have no commit hash" into a failed build.
    let git_hash = match std::env::var("FREENET_GIT_COMMIT_HASH") {
        Ok(v) if !v.trim().is_empty() => {
            let v = v.trim().to_string();
            // Validated because a `cargo:` directive is newline-delimited: an
            // embedded newline would let this value emit further directives of
            // its own choosing.
            if v.len() > 40 || !v.chars().all(|c| c.is_ascii_hexdigit()) {
                panic!(
                    "FREENET_GIT_COMMIT_HASH ({v}) must be <= 40 hex characters. \
                     Pass an EMPTY value when there is no VCS information."
                );
            }
            v
        }
        _ => Command::new("git")
            .args(["rev-parse", "--short=12", "HEAD"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string()),
    };
    println!("cargo:rustc-env=GIT_COMMIT_HASH={git_hash}");

    // Git dirty flag. `GIT_OPTIONAL_LOCKS=0` stops `git status` from
    // rewriting the index to refresh stat info: the index is one of the
    // files watched below, so a write here would make the next build rerun
    // this script for nothing.
    //
    // `FREENET_GIT_IS_DIRTY` overrides the probe for the same no-repository
    // callers. Parsed STRICTLY, and that is load-bearing: `GIT_DIRTY` is the
    // auto-update kill switch (`auto_update_is_disabled` in
    // `src/bin/freenet.rs`), so a lenient "any non-empty value is dirty" rule
    // would read `=false` as DIRTY and ship a binary that never updates itself.
    // An unrecognised value fails the build rather than being guessed at in
    // either direction.
    let git_dirty = match std::env::var("FREENET_GIT_IS_DIRTY")
        .as_deref()
        .map(str::trim)
    {
        Ok("1") | Ok("true") => true,
        Ok("0") | Ok("false") => false,
        Ok("") | Err(_) => Command::new("git")
            .args(["status", "--porcelain"])
            .env("GIT_OPTIONAL_LOCKS", "0")
            .output()
            .ok()
            .map(|o| !o.stdout.is_empty())
            .unwrap_or(false),
        Ok(other) => panic!("FREENET_GIT_IS_DIRTY ({other}) must be 1, true, 0, false, or empty"),
    };
    let dirty_suffix = if git_dirty { "-dirty" } else { "" };
    println!("cargo:rustc-env=GIT_DIRTY={dirty_suffix}");

    // Build timestamp (ISO 8601). `SOURCE_DATE_EPOCH` is the cross-ecosystem
    // reproducible-builds convention and nixpkgs' stdenv exports it, so honour
    // it -- otherwise two builds of identical source differ only by this field.
    //
    // CAUTION inside `nix develop`: stdenv's default value is 315532800
    // (1980-01-01), so a plain `cargo build` in such a shell would stamp a 1980
    // timestamp into the binary -- and that field is what ops uses to correlate
    // a running node's log with the artifact it came from. The flake's devShell
    // unsets the variable for exactly this reason.
    //
    // EMPTY means "no override", exactly as it does for the two variables
    // above. Set-but-empty is what a build wrapper produces when it forwards a
    // variable it has not got (`SOURCE_DATE_EPOCH=$SOMETHING_UNSET`), and
    // panicking on it made this the one of the three that fails the build over
    // an absent value rather than falling through to the probe.
    let now = match std::env::var("SOURCE_DATE_EPOCH").as_deref().map(str::trim) {
        Ok("") | Err(_) => chrono::Utc::now(),
        Ok(val) => {
            let epoch: i64 = val
                .parse()
                .unwrap_or_else(|_| panic!("SOURCE_DATE_EPOCH ({val}) is not a valid integer"));
            chrono::Utc
                .timestamp_opt(epoch, 0)
                .single()
                .unwrap_or_else(|| panic!("SOURCE_DATE_EPOCH ({val}) is not a valid timestamp"))
        }
    };
    let timestamp = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    println!("cargo:rustc-env=BUILD_TIMESTAMP={timestamp}");

    // Required: emitting any `rerun-if-*` directive makes Cargo honour only the
    // declared set, so without these a changed (or newly unset) override leaves
    // stale provenance baked in -- and a stale `GIT_DIRTY` silently disables
    // auto-update.
    println!("cargo:rerun-if-env-changed=FREENET_GIT_COMMIT_HASH");
    println!("cargo:rerun-if-env-changed=FREENET_GIT_IS_DIRTY");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");

    // Rerun only when the package sources, or the commit or working tree
    // described above, change (see `build/git_watch.rs`, #5667).
    if let Ok(manifest_dir) = std::env::var("CARGO_MANIFEST_DIR") {
        for path in git_watch::rerun_if_changed_paths(std::path::Path::new(&manifest_dir)) {
            println!("cargo:rerun-if-changed={}", path.display());
        }
    }
}
