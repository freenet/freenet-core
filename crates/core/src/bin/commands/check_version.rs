use anyhow::{Context, Result};
use clap::Args;
use semver::Version;

use super::utils::get_latest_release;

#[derive(Args, Debug, Clone)]
pub struct CheckVersionCommand {}

impl CheckVersionCommand {
    pub fn run(&self, current_version: &str) -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(self.run_async(current_version))
    }

    async fn run_async(&self, current_version: &str) -> Result<()> {
        println!("Current version: {}", current_version);
        println!("Checking for updates...");

        let latest = get_latest_release().await?;
        let latest_version = latest.tag_name.trim_start_matches('v');
        println!("Latest version: {}", latest_version);

        let current_ver = Version::parse(current_version)
            .context("Failed to parse current version as semver")?;
        let latest_ver = Version::parse(latest_version)
            .context("Failed to parse latest version as semver")?;

        if latest_ver > current_ver {
            println!(
                "Update available: {} -> {}",
                current_version, latest_version
            );
            println!("Run 'freenet update' to install the latest version.");
        } else {
            println!("You are already running the latest version.");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        let current = Version::parse("0.1.0").unwrap();
        let newer = Version::parse("0.1.1").unwrap();
        let older = Version::parse("0.0.9").unwrap();

        assert!(newer > current);
        assert!(older < current);
        assert!(current == Version::parse("0.1.0").unwrap());
    }
}
