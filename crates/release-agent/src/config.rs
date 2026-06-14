use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub binary_path: PathBuf,
    pub update_command: PathBuf,
    pub hmac_secret_path: PathBuf,
    #[serde(default = "default_repo")]
    pub github_repo: String,
    /// Defaults to `true` (safe-by-default). A missing or partial config can
    /// never silently enable live mode. Combined with `deny_unknown_fields`
    /// above, a typo like `dry-run = false` (with a dash) is rejected at
    /// load time rather than treated as "live".
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_rate_limit")]
    pub rate_limit_seconds: u64,
    #[serde(default = "default_skew")]
    pub clock_skew_tolerance_seconds: u32,

    /// Path to the `announce-to-river.sh` script. Empty (default) disables
    /// the `POST /announce/river` endpoint; the endpoint returns 503.
    /// Only nova has this configured today — vega has no Freenet node /
    /// signing key, so it can't post to River.
    #[serde(default)]
    pub river_announce_command: PathBuf,

    /// User under which `riverctl` runs (typically `ian` on nova). The
    /// agent itself runs as `freenet-update`; it `sudo -u <user>`s to
    /// the owner of `~/.config/freenet-river-official/`. Default empty.
    #[serde(default)]
    pub river_announce_user: String,

    /// systemd unit name of the gateway service this agent manages, WITHOUT
    /// the `.service` suffix (e.g. `freenet-gateway`, or `freenet-gateway-hector`
    /// on vega's secondary instance). `GET /version` queries
    /// `systemctl is-active <managed_service>` and reports the result as
    /// `service_active`, so the release workflow can tell a successful binary
    /// swap apart from a gateway whose service failed to restart. Defaults to
    /// `freenet-gateway`, matching the unit name used by
    /// `deploy-local-gateway.sh` and the gateway setup guide.
    #[serde(default = "default_managed_service")]
    pub managed_service: String,
}

fn default_repo() -> String {
    "freenet/freenet-core".to_string()
}
fn default_true() -> bool {
    true
}
fn default_rate_limit() -> u64 {
    600
}
fn default_skew() -> u32 {
    300
}
fn default_managed_service() -> String {
    "freenet-gateway".to_string()
}

impl Config {
    pub fn from_path(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        toml::from_str(&raw).context("parse config TOML")
    }

    /// Warn (at load time) if `rate_limit_seconds` is configured below the
    /// updater's `MAX_UPDATE_HOLD`. The rate-limit window is the second
    /// backstop for the in-flight overlap guard: if a real update runs past
    /// `MAX_UPDATE_HOLD`, the guard frees the slot while the child is still
    /// running, and only the rate-limiter then stops a second POST from racing
    /// the in-flight restart (the #4271 double-stop). With the prod default
    /// (`rate_limit_seconds = 600 > 300`) that backstop holds; a misconfig that
    /// sets it lower silently removes it. Surface that to the operator rather
    /// than failing closed — the agent still functions, it's just lost a
    /// defense-in-depth layer.
    pub fn warn_if_rate_limit_below_max_hold(&self) {
        let max_hold = crate::updater::MAX_UPDATE_HOLD.as_secs();
        if self.rate_limit_seconds < max_hold {
            tracing::warn!(
                rate_limit_seconds = self.rate_limit_seconds,
                max_update_hold_seconds = max_hold,
                "rate_limit_seconds is below the updater max-hold; the rate-limit \
                 backstop for the in-flight overlap guard is disabled. If an update \
                 runs longer than {max_hold}s, a second update could race its \
                 restart and trigger the #4271 double-stop. Set rate_limit_seconds \
                 >= {max_hold} (prod default is 600)."
            );
        }
    }

    /// Load the HMAC secret. The file is always parsed as hex (matching
    /// `install.sh`'s `openssl rand -hex 32` output); any whitespace is
    /// trimmed. The previous heuristic of "hex with fallback to raw" was
    /// removed after PR #4082 review because a 32-byte raw key whose bytes
    /// happen to be valid hex digits would be silently halved to 16 bytes.
    pub fn load_secret(&self) -> Result<Vec<u8>> {
        let bytes = std::fs::read(&self.hmac_secret_path)
            .with_context(|| format!("read HMAC secret {}", self.hmac_secret_path.display()))?;
        let trimmed: Vec<u8> = bytes
            .iter()
            .copied()
            .filter(|b| !b.is_ascii_whitespace())
            .collect();
        let decoded = hex::decode(&trimmed).with_context(|| {
            format!(
                "HMAC secret at {} is not valid hex (expected `openssl rand -hex 32` output)",
                self.hmac_secret_path.display()
            )
        })?;
        if decoded.len() < 32 {
            anyhow::bail!(
                "HMAC secret decoded to {} bytes; need at least 32 (64 hex chars)",
                decoded.len()
            );
        }
        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parses_minimal_config_and_defaults_to_dry_run() {
        let toml_src = r#"
listen_addr = "127.0.0.1:9876"
binary_path = "/usr/local/bin/freenet"
update_command = "/usr/local/bin/gateway-auto-update.sh"
hmac_secret_path = "/etc/freenet-release-agent/hmac.key"
"#;
        let cfg: Config = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.github_repo, "freenet/freenet-core");
        assert_eq!(cfg.rate_limit_seconds, 600);
        assert_eq!(cfg.clock_skew_tolerance_seconds, 300);
        assert!(
            cfg.dry_run,
            "missing dry_run must default to true (safe-by-default)"
        );
        assert_eq!(
            cfg.managed_service, "freenet-gateway",
            "missing managed_service must default to the standard gateway unit"
        );
    }

    #[test]
    fn parses_custom_managed_service() {
        // vega's secondary instance runs as `freenet-gateway-hector`.
        let toml_src = r#"
listen_addr = "127.0.0.1:9876"
binary_path = "/usr/local/bin/freenet"
update_command = "/usr/local/bin/gateway-auto-update.sh"
hmac_secret_path = "/etc/freenet-release-agent/hmac.key"
managed_service = "freenet-gateway-hector"
"#;
        let cfg: Config = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.managed_service, "freenet-gateway-hector");
    }

    #[test]
    fn parses_full_config() {
        let toml_src = r#"
listen_addr = "127.0.0.1:9876"
binary_path = "/usr/local/bin/freenet"
update_command = "/usr/local/bin/gateway-auto-update.sh"
hmac_secret_path = "/etc/freenet-release-agent/hmac.key"
github_repo = "freenet/freenet-core"
dry_run = false
rate_limit_seconds = 60
clock_skew_tolerance_seconds = 120
"#;
        let cfg: Config = toml::from_str(toml_src).unwrap();
        assert!(!cfg.dry_run);
        assert_eq!(cfg.rate_limit_seconds, 60);
        assert_eq!(cfg.clock_skew_tolerance_seconds, 120);
    }

    #[test]
    fn unknown_field_is_rejected() {
        // `dry-run` (hyphen) is a likely typo that previously silently left
        // dry_run at its default. With deny_unknown_fields it's a hard error.
        let toml_src = r#"
listen_addr = "127.0.0.1:9876"
binary_path = "/usr/local/bin/freenet"
update_command = "/usr/local/bin/gateway-auto-update.sh"
hmac_secret_path = "/etc/freenet-release-agent/hmac.key"
dry-run = false
"#;
        let err = toml::from_str::<Config>(toml_src).unwrap_err().to_string();
        assert!(
            err.contains("unknown field") || err.contains("dry-run"),
            "expected unknown-field error, got: {err}"
        );
    }

    #[test]
    fn load_secret_accepts_hex_with_newline() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        )
        .unwrap();
        let cfg = test_config(tmp.path());
        let secret = cfg.load_secret().unwrap();
        assert_eq!(secret.len(), 32);
    }

    #[test]
    fn load_secret_rejects_non_hex() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "this is plainly not hex").unwrap();
        assert!(test_config(tmp.path()).load_secret().is_err());
    }

    #[test]
    fn load_secret_rejects_short_hex() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "deadbeefdeadbeef").unwrap();
        assert!(test_config(tmp.path()).load_secret().is_err());
    }

    fn test_config(secret_path: &Path) -> Config {
        Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            binary_path: PathBuf::from("/bin/true"),
            update_command: PathBuf::from("/bin/true"),
            hmac_secret_path: secret_path.to_path_buf(),
            github_repo: "x/y".into(),
            dry_run: true,
            rate_limit_seconds: 0,
            clock_skew_tolerance_seconds: 0,
            river_announce_command: PathBuf::new(),
            river_announce_user: String::new(),
            managed_service: "freenet-gateway".into(),
        }
    }
}
