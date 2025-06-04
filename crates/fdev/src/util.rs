use std::{
    io::{self, BufReader, Read, Write},
    path::PathBuf,
    process::Child,
    time::Duration,
};

use serde::de::DeserializeOwned;

use crate::wasm_runtime::DeserializationFmt;

pub fn deserialize<T, R>(deser_format: Option<DeserializationFmt>, data: &R) -> anyhow::Result<T>
where
    T: DeserializeOwned,
    R: AsRef<[u8]> + ?Sized,
{
    match deser_format {
        Some(DeserializationFmt::Json) => {
            let deser = serde_json::from_slice(data.as_ref())?;
            Ok(deser)
        }
        _ => Ok(bincode::deserialize(data.as_ref())?),
    }
}

pub(crate) fn pipe_std_streams(mut child: Child) -> anyhow::Result<()> {
    let c_stdout = child.stdout.take().expect("Failed to open command stdout");
    let c_stderr = child.stderr.take().expect("Failed to open command stderr");

    let write_child_stderr = move || -> anyhow::Result<()> {
        let mut stderr = io::stderr();
        let mut reader = BufReader::new(c_stderr);
        let mut buffer = [0; 1024];
        while let Ok(n) = reader.read(&mut buffer) {
            if n == 0 {
                break;
            }
            stderr.write_all(&buffer[..n])?;
        }
        Ok(())
    };

    let write_child_stdout = move || -> anyhow::Result<()> {
        let mut stdout = io::stdout();
        let mut reader = BufReader::new(c_stdout);
        let mut buffer = [0; 1024];
        while let Ok(n) = reader.read(&mut buffer) {
            if n == 0 {
                break;
            }
            stdout.write_all(&buffer[..n])?;
        }
        Ok(())
    };
    std::thread::spawn(write_child_stdout);
    std::thread::spawn(write_child_stderr);

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    anyhow::bail!("exit with status: {status}");
                }
                break;
            }
            Ok(None) => {
                std::thread::sleep(Duration::from_millis(500));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    Ok(())
}

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
fn find_workspace_root_from(start_path: &str) -> PathBuf {
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
