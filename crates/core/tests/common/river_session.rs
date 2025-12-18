//! River session utilities for integration tests.
//!
//! Provides a `RiverSession` abstraction for creating rooms, inviting users,
//! and sending messages via `riverctl`.

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

/// Identifies a user in a River session.
#[derive(Clone, Copy, Debug)]
pub enum RiverUser {
    Alice,
    Bob,
}

/// Configuration for retry behavior in riverctl commands.
#[derive(Clone, Debug)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub retry_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        }
    }
}

impl RetryConfig {
    /// No retries - fail immediately on error.
    pub fn no_retry() -> Self {
        Self {
            max_retries: 1,
            retry_delay: Duration::ZERO,
        }
    }
}

/// A River session managing two users (Alice and Bob) in a shared room.
pub struct RiverSession {
    riverctl: PathBuf,
    alice_dir: TempDir,
    bob_dir: TempDir,
    alice_url: String,
    bob_url: String,
    room_key: String,
    invite_regex: Regex,
    room_regex: Regex,
    retry_config: RetryConfig,
}

impl RiverSession {
    /// Initialize a new River session with Alice creating a room and Bob joining.
    pub async fn initialize(riverctl: PathBuf, alice_url: String, bob_url: String) -> Result<Self> {
        Self::initialize_with_retry(riverctl, alice_url, bob_url, RetryConfig::default()).await
    }

    /// Initialize with custom retry configuration.
    pub async fn initialize_with_retry(
        riverctl: PathBuf,
        alice_url: String,
        bob_url: String,
        retry_config: RetryConfig,
    ) -> Result<Self> {
        let alice_dir = TempDir::new().context("failed to create Alice temp config dir")?;
        let bob_dir = TempDir::new().context("failed to create Bob temp config dir")?;

        let mut session = Self {
            riverctl,
            alice_dir,
            bob_dir,
            alice_url,
            bob_url,
            room_key: String::new(),
            invite_regex: Regex::new(r"[A-Za-z0-9+/=]{40,}").unwrap(),
            room_regex: Regex::new(r"[A-Za-z0-9]{40,}").unwrap(),
            retry_config,
        };

        session.setup_room().await?;
        Ok(session)
    }

    /// Initialize without retries (useful for testing propagation races).
    pub async fn initialize_no_retry(
        riverctl: PathBuf,
        alice_url: String,
        bob_url: String,
    ) -> Result<Self> {
        Self::initialize_with_retry(riverctl, alice_url, bob_url, RetryConfig::no_retry()).await
    }

    async fn setup_room(&mut self) -> Result<()> {
        let create_output = self
            .run_riverctl(
                RiverUser::Alice,
                &[
                    "room",
                    "create",
                    "--name",
                    "test-room",
                    "--nickname",
                    "Alice",
                ],
            )
            .await?;
        self.room_key = self
            .room_regex
            .find(&create_output)
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("failed to parse room owner key from riverctl output"))?;

        let invite_output = self
            .run_riverctl(
                RiverUser::Alice,
                &["invite", "create", self.room_key.as_str()],
            )
            .await?;
        let invitation_code = self
            .invite_regex
            .find_iter(&invite_output)
            .filter(|m| m.as_str() != self.room_key)
            .last()
            .map(|m| m.as_str().to_string())
            .ok_or_else(|| anyhow!("failed to parse invitation code from riverctl output"))?;

        self.run_riverctl(
            RiverUser::Bob,
            &["invite", "accept", &invitation_code, "--nickname", "Bob"],
        )
        .await?;

        Ok(())
    }

    /// Send a message as the specified user.
    pub async fn send_message(&self, user: RiverUser, body: &str) -> Result<()> {
        self.run_riverctl(user, &["message", "send", self.room_key.as_str(), body])
            .await
            .map(|_| ())
    }

    /// List messages visible to the specified user.
    pub async fn list_messages(&self, user: RiverUser) -> Result<String> {
        self.run_riverctl(user, &["message", "list", self.room_key.as_str()])
            .await
    }

    /// Get the room key (owner key) for this session.
    pub fn room_key(&self) -> &str {
        &self.room_key
    }

    /// Run a riverctl command with retry logic.
    pub async fn run_riverctl(&self, user: RiverUser, args: &[&str]) -> Result<String> {
        let (url, config_dir) = match user {
            RiverUser::Alice => (&self.alice_url, self.alice_dir.path()),
            RiverUser::Bob => (&self.bob_url, self.bob_dir.path()),
        };

        for attempt in 1..=self.retry_config.max_retries {
            let mut cmd = tokio::process::Command::new(&self.riverctl);
            cmd.arg("--node-url").arg(url);
            cmd.args(args);
            cmd.env("RIVER_CONFIG_DIR", config_dir);

            let output = cmd
                .output()
                .await
                .context("failed to execute riverctl command")?;
            if output.status.success() {
                return Ok(String::from_utf8_lossy(&output.stdout).to_string());
            }

            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            // Only retry on transient infrastructure errors, not application-level bugs.
            // "missing contract" errors indicate contract propagation bugs and should fail
            // immediately rather than being masked by retries (see issue #2306).
            let retriable = stderr.contains("Timeout waiting for")
                || stderr.contains("connection refused")
                || stderr.contains("HTTP request failed");
            if attempt == self.retry_config.max_retries || !retriable {
                bail!("riverctl failed (user {:?}): {}", user, stderr);
            }
            println!(
                "riverctl attempt {}/{} failed for {:?}: {}; retrying in {}s",
                attempt,
                self.retry_config.max_retries,
                user,
                stderr.trim(),
                self.retry_config.retry_delay.as_secs()
            );
            sleep(self.retry_config.retry_delay).await;
        }

        unreachable!("riverctl retry loop should always return or bail")
    }
}
