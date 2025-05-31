//! Utilities for working with the Cargo workspace

use std::path::PathBuf;

/// Gets the target directory for the workspace, either from CARGO_TARGET_DIR
/// environment variable or by finding the workspace root and using its target directory.
pub fn get_workspace_target_dir() -> PathBuf {
    const TARGET_DIR_VAR: &str = "CARGO_TARGET_DIR";
    
    std::env::var(TARGET_DIR_VAR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let manifest_dir = env!("CARGO_MANIFEST_DIR");
            let workspace_root = find_workspace_root_from(manifest_dir);
            workspace_root.join("target")
        })
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