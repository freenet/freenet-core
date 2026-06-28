//! Cross-process status the service wrapper publishes for the local homepage.
//!
//! On macOS/Linux the node runs under a supervising wrapper process (a launchd
//! plist / systemd unit shell script that re-launches `freenet network`). When
//! that wrapper detects it is stuck in a repeated, identical failure loop — the
//! dominant case being a stale orphan `freenet network` still holding the
//! dashboard port (#3967) — it writes a small JSON status file into the
//! platform log directory (#4382/#4288).
//!
//! Two consumers read or clear that file, and BOTH live in the `freenet`
//! library while the writer lives in the `freenet` binary's wrapper command.
//! Sharing the struct + filename + helpers here (rather than duplicating a
//! read-side copy) keeps the on-disk schema and location from drifting silently
//! between writer and reader:
//!
//! - The homepage (`server::home_page`) reads it to render an operator banner.
//!   This is the cross-platform surface for surfacing a stuck wrapper: Linux has
//!   no `osascript` notification, and even on macOS the banner reaches an
//!   operator who is staring at the (stale) dashboard rather than at
//!   Notification Center.
//! - The node clears it once it successfully binds its client-API port
//!   (`server::clear_stuck_status_on_startup`): a successful bind proves the
//!   port-holding stuck condition is resolved, so any banner a now-dead wrapper
//!   left behind must not linger on the recovered node.

use std::path::Path;

/// File name (under the log directory returned by
/// [`crate::tracing::tracer::get_log_dir`]) where the wrapper records a
/// stuck-loop status (failure count + last error). The wrapper is the only
/// writer; the homepage is the only reader.
pub const STUCK_STATUS_FILE_NAME: &str = "wrapper-stuck-status.json";

/// Status the wrapper writes to [`STUCK_STATUS_FILE_NAME`] once a repeated,
/// identical failure has crossed the wrapper's notify threshold (#4382/#4288).
///
/// Serialized as JSON so the homepage reader can render an operator banner even
/// when the served node is a stale orphan answering on the port — the orphan
/// reads the same file the wrapper wrote.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StuckWrapperStatus {
    /// Child exit code that keeps repeating.
    pub exit_code: i32,
    /// Whether the repeated failure was detected as a port conflict.
    pub is_port_conflict: bool,
    /// How many consecutive identical failures have occurred.
    pub failure_count: u32,
    /// Human-readable last-error summary (exit code + cause).
    pub last_error: String,
    /// One-line recovery hint for the operator.
    pub recovery_hint: String,
}

impl StuckWrapperStatus {
    pub fn new(exit_code: i32, is_port_conflict: bool, failure_count: u32) -> Self {
        let cause = if is_port_conflict {
            // exit 43 / "address already in use" against a held port is the
            // #3967 stale-orphan signature.
            format!("exit {exit_code} — the dashboard port is held by another process")
        } else {
            format!("exit {exit_code}")
        };
        let last_error =
            format!("Node failed {failure_count} times in a row with the same cause: {cause}.");
        let recovery_hint = if is_port_conflict {
            "The background service looks stuck on an old process holding the port. \
             Recover with: freenet service stop && freenet service start \
             (or kill the stale 'freenet network' process holding the dashboard port)."
                .to_string()
        } else {
            "The background service is repeatedly crashing on startup. \
             Recover with: freenet service stop && freenet service start, \
             then check the wrapper logs for the underlying error."
                .to_string()
        };
        Self {
            exit_code,
            is_port_conflict,
            failure_count,
            last_error,
            recovery_hint,
        }
    }

    /// Single log line summarizing the stuck condition.
    pub fn log_line(&self) -> String {
        format!(
            "Wrapper appears stuck: {} {}",
            self.last_error, self.recovery_hint
        )
    }
}

/// Write the stuck status to [`STUCK_STATUS_FILE_NAME`] under the log directory.
/// Best-effort: a write failure must never block the wrapper loop.
pub fn write_stuck_status_file(log_dir: &Path, status: &StuckWrapperStatus) {
    let path = log_dir.join(STUCK_STATUS_FILE_NAME);
    match serde_json::to_string_pretty(status) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                eprintln!("Failed to write stuck-status file {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("Failed to serialize stuck-status: {e}"),
    }
}

/// Remove the stuck status file (best-effort) once the wrapper recovers, so a
/// stale banner doesn't linger. A missing file is not an error.
pub fn clear_stuck_status_file(log_dir: &Path) {
    let path = log_dir.join(STUCK_STATUS_FILE_NAME);
    if path.exists() {
        drop(std::fs::remove_file(&path));
    }
}

/// Read the stuck status file, if present and parseable. A missing or malformed
/// file is treated as "no stuck status" (`None`) — the homepage simply renders
/// no banner. Never errors: this is a best-effort read on the homepage hot path.
pub fn read_stuck_status_file(log_dir: &Path) -> Option<StuckWrapperStatus> {
    let path = log_dir.join(STUCK_STATUS_FILE_NAME);
    let json = std::fs::read_to_string(&path).ok()?;
    serde_json::from_str(&json).ok()
}

/// Clear any stuck-status file in the default platform log directory, called
/// once the node has successfully bound its client-API port at startup.
///
/// A successful bind proves the port-holding stuck condition is resolved, so
/// this is the authoritative "recovered" signal: it clears a banner a now-dead
/// wrapper left behind (the wrapper's own recovery clear only fires while the
/// wrapper is still iterating, which it may not be once launchd has stopped
/// relaunching it). Best-effort and idempotent; a missing file or unknown log
/// directory is a no-op. The stale orphan that wedged the node never reaches
/// this path — it bound the port long ago and is not restarting — so the orphan
/// keeps showing the banner while the freshly-recovered node clears it.
pub fn clear_stuck_status_on_startup() {
    if let Some(log_dir) = crate::tracing::tracer::get_log_dir() {
        clear_stuck_status_file(&log_dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stuck_status_file_round_trips_and_clears() {
        let tmp = tempfile::tempdir().unwrap();
        let status = StuckWrapperStatus::new(43, true, 3);
        write_stuck_status_file(tmp.path(), &status);

        let parsed = read_stuck_status_file(tmp.path()).expect("must read back");
        assert_eq!(parsed, status);
        assert_eq!(parsed.failure_count, 3);
        assert!(parsed.is_port_conflict);

        clear_stuck_status_file(tmp.path());
        assert!(
            read_stuck_status_file(tmp.path()).is_none(),
            "file must be gone after clear"
        );
        // Clearing a missing file is a no-op, not an error.
        clear_stuck_status_file(tmp.path());
    }

    #[test]
    fn read_missing_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read_stuck_status_file(tmp.path()).is_none());
    }

    #[test]
    fn read_malformed_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join(STUCK_STATUS_FILE_NAME), "{not json").unwrap();
        assert!(
            read_stuck_status_file(tmp.path()).is_none(),
            "malformed JSON must not panic or surface a partial status"
        );
    }

    #[test]
    fn status_messages_differ_by_cause() {
        let port = StuckWrapperStatus::new(43, true, 3);
        assert!(port.recovery_hint.contains("holding the port"));
        assert!(port.last_error.contains("dashboard port is held"));

        let crash = StuckWrapperStatus::new(1, false, 3);
        assert!(crash.recovery_hint.contains("repeatedly crashing"));
        assert!(!crash.last_error.contains("dashboard port is held"));
    }
}
