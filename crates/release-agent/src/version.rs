use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use semver::Version;
use tokio::process::Command;

pub async fn current_version(binary_path: &Path) -> Result<Version> {
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        Command::new(binary_path).arg("--version").output(),
    )
    .await
    .context("freenet --version timed out")?
    .with_context(|| format!("failed to spawn {}", binary_path.display()))?;

    if !output.status.success() {
        anyhow::bail!(
            "freenet --version exited {}: stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    parse_version_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_version_output(out: &str) -> Result<Version> {
    out.split_whitespace()
        .find_map(|t| Version::parse(t.trim_start_matches('v')).ok())
        .with_context(|| format!("could not parse version from {out:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_output() {
        let v = parse_version_output("freenet 0.2.56\n").unwrap();
        assert_eq!(v, Version::new(0, 2, 56));
    }

    #[test]
    fn parses_v_prefixed_output() {
        let v = parse_version_output("freenet v0.2.56").unwrap();
        assert_eq!(v, Version::new(0, 2, 56));
    }

    #[test]
    fn parses_pre_release() {
        let v = parse_version_output("freenet 0.2.56-rc1\n").unwrap();
        assert_eq!(v.major, 0);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 56);
        assert!(!v.pre.is_empty());
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_version_output("no version here").is_err());
    }
}
