//! Utilities for working with the Cargo workspace

use std::path::PathBuf;

/// Gets the target directory for the workspace, either from CARGO_TARGET_DIR
/// environment variable or by finding the workspace root and using its target directory.
pub fn get_workspace_target_dir() -> PathBuf {
    const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";

    // 1. Check environment variable first
    if let Ok(target_dir) = std::env::var(TARGET_DIR_VAR) {
        return PathBuf::from(target_dir);
    }

    // 2. Try cargo metadata (most reliable)
    if let Ok(target_dir) = get_target_from_cargo_metadata() {
        return target_dir;
    }

    // 3. Try from current working directory
    if let Ok(workspace_root) = find_workspace_from_cwd() {
        return workspace_root.join("target");
    }

    // 4. Fallback to compile-time detection
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let workspace_root = find_workspace_root_from(manifest_dir);
    workspace_root.join("target")
}

/// Gets the target directory by querying cargo metadata directly.
fn get_target_from_cargo_metadata() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let output = std::process::Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()?;

    if !output.status.success() {
        return Err("cargo metadata command failed".into());
    }

    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let target_dir = metadata["target_directory"]
        .as_str()
        .ok_or("target_directory not found in cargo metadata")?;

    Ok(PathBuf::from(target_dir))
}

/// Finds the workspace root directory starting from the current working directory.
fn find_workspace_from_cwd() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let current_dir = std::env::current_dir()?;

    current_dir
        .ancestors()
        .find(|p| {
            p.join("Cargo.toml").exists() && {
                if let Ok(content) = std::fs::read_to_string(p.join("Cargo.toml")) {
                    content.contains("[workspace]")
                } else {
                    false
                }
            }
        })
        .map(|p| p.to_path_buf())
        .ok_or("Could not find workspace root".into())
}

/// Finds the workspace root directory starting from the given path.
/// Walks up the directory tree looking for a Cargo.toml with [workspace] section.
pub fn find_workspace_root_from(start_path: &str) -> PathBuf {
    PathBuf::from(start_path)
        .ancestors()
        .find(|p| {
            p.join("Cargo.toml").exists() && {
                let content = std::fs::read_to_string(p.join("Cargo.toml")).unwrap_or_default();
                content.contains("[workspace]")
            }
        })
        .expect("Could not find workspace root")
        .to_path_buf()
}
