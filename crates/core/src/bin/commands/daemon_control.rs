//! Persistent enable/disable control for the background daemon.
//!
//! `freenet service disable` writes a marker file that `freenet network`
//! checks at startup. While the marker is present the node refuses to run and
//! parks itself in an inert idle state instead, so no supervisor — systemd,
//! launchd, or the tray wrapper — can bring the node back on reboot or
//! re-login. `freenet service enable` removes the marker.
//!
//! The marker lives in the *config* directory (not a runtime/tmp dir) so it
//! survives restarts, reboots, and reinstalls of the same config — the whole
//! point of the feature is a disable that persists "indefinitely until turned
//! on again".
//!
//! This module is the single source of truth for the marker, shared by both the
//! node startup path (`crates/core/src/bin/freenet.rs::run_network`) and the
//! `service disable`/`enable`/`status` CLI commands
//! (`crates/core/src/bin/commands/service.rs`). Keeping the path/predicate here
//! (rather than duplicating the `join(...)` at each site) means the two sides
//! can never disagree about where "disabled" is recorded.

use std::path::{Path, PathBuf};

/// Marker file name written under the config directory. A present file (any
/// content) means the daemon is disabled.
pub(crate) const DISABLED_MARKER_FILE: &str = "daemon-disabled";

/// Absolute path of the daemon-disabled marker for a given config directory.
pub(crate) fn disabled_marker_path(config_dir: &Path) -> PathBuf {
    config_dir.join(DISABLED_MARKER_FILE)
}

/// Whether the background daemon has been disabled via `freenet service
/// disable`. The presence of the marker file is authoritative; its contents are
/// human-facing only.
pub(crate) fn is_daemon_disabled(config_dir: &Path) -> bool {
    disabled_marker_path(config_dir).exists()
}

/// Write the daemon-disabled marker. Idempotent: re-disabling an already
/// disabled daemon simply rewrites the marker. Returns whether the daemon was
/// *already* disabled before this call, so the CLI can tailor its message.
pub(crate) fn write_disabled_marker(config_dir: &Path) -> std::io::Result<bool> {
    let already = is_daemon_disabled(config_dir);
    std::fs::create_dir_all(config_dir)?;
    let body = "This file disables the Freenet background daemon.\n\
                While it exists, `freenet network` refuses to start and stays idle instead,\n\
                so no service supervisor can bring the node back across restarts or reboots.\n\
                Delete this file, or run `freenet service enable`, to re-enable the daemon.\n";
    std::fs::write(disabled_marker_path(config_dir), body)?;
    Ok(already)
}

/// Remove the daemon-disabled marker. Idempotent. Returns whether a marker was
/// actually present (i.e. the daemon *was* disabled before this call), so the
/// CLI can distinguish "re-enabled" from "was not disabled".
pub(crate) fn remove_disabled_marker(config_dir: &Path) -> std::io::Result<bool> {
    match std::fs::remove_file(disabled_marker_path(config_dir)) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_marker_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        // Use a not-yet-created nested path to prove write_disabled_marker
        // creates the config dir on demand (mirrors a fresh install).
        let config_dir = tmp.path().join("nested").join("freenet-config");

        // Initially enabled (no marker, and the dir doesn't even exist yet).
        assert!(!is_daemon_disabled(&config_dir));

        // First disable writes the marker and reports "not already disabled".
        let already = write_disabled_marker(&config_dir).unwrap();
        assert!(!already, "first disable must report not-already-disabled");
        assert!(is_daemon_disabled(&config_dir));
        assert!(disabled_marker_path(&config_dir).exists());

        // Disabling again is idempotent and reports already-disabled.
        let already = write_disabled_marker(&config_dir).unwrap();
        assert!(already, "second disable must report already-disabled");
        assert!(is_daemon_disabled(&config_dir));

        // Enabling removes the marker and reports it was present.
        let was_disabled = remove_disabled_marker(&config_dir).unwrap();
        assert!(was_disabled, "enable must report the daemon was disabled");
        assert!(!is_daemon_disabled(&config_dir));

        // Enabling again is idempotent and reports nothing was removed.
        let was_disabled = remove_disabled_marker(&config_dir).unwrap();
        assert!(!was_disabled, "second enable must report nothing to do");
        assert!(!is_daemon_disabled(&config_dir));
    }

    #[test]
    fn disabled_marker_path_is_under_config_dir() {
        let dir = Path::new("/some/config");
        assert_eq!(disabled_marker_path(dir), dir.join(DISABLED_MARKER_FILE));
        assert!(disabled_marker_path(dir).starts_with(dir));
    }

    #[test]
    fn marker_contents_are_present_but_not_load_bearing() {
        // The predicate keys on presence, not contents: an empty marker (e.g.
        // hand-created, or truncated) must still read as disabled.
        let tmp = tempfile::tempdir().unwrap();
        let config_dir = tmp.path().to_path_buf();
        std::fs::write(disabled_marker_path(&config_dir), "").unwrap();
        assert!(is_daemon_disabled(&config_dir));
    }
}
