use std::process::Command;

fn main() {
    // Emit build metadata for startup logging
    emit_build_metadata();

    // Skip flatbuffers generation for cross-compilation
    if std::env::var("CARGO_BUILD_TARGET").is_ok() {
        return;
    }

    let status = Command::new("flatc")
        .arg("--rust")
        .arg("-o")
        .arg("src/generated")
        .arg("../../schemas/flatbuffers/topology.fbs")
        .status();
    if let Err(err) = status {
        println!("failed compiling flatbuffers schema: {err}");
        println!("refer to https://github.com/google/flatbuffers to install the flatc compiler");
    } else {
        let _ = Command::new("cargo").arg("fmt").status();
    }
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
