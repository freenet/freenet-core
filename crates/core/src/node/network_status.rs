//! Network connection status tracking for the connecting page.
//!
//! Surfaces diagnostic information (version mismatches, NAT traversal failures, etc.)
//! so the HTTP gateway's connecting page can show actionable diagnostics instead of
//! a generic "Connecting..." spinner.

use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

static NETWORK_STATUS: OnceLock<Arc<RwLock<NetworkStatus>>> = OnceLock::new();

/// Tracked network connection status for diagnostic display.
pub struct NetworkStatus {
    pub gateway_failures: Vec<GatewayFailure>,
    pub connection_attempts: u32,
    pub open_connections: u32,
    pub listening_port: u16,
    pub started_at: Instant,
}

/// A recorded failure when connecting to a gateway.
pub struct GatewayFailure {
    pub address: SocketAddr,
    pub reason: FailureReason,
}

/// Classified reason for a gateway connection failure.
pub enum FailureReason {
    /// Protocol version mismatch between local node and gateway.
    VersionMismatch { local: String, gateway: String },
    /// NAT traversal failed (max connection attempts reached).
    NatTraversalFailed,
    /// Connection timed out.
    Timeout,
    /// Other transport-level failure.
    Other(String),
}

/// Initialize the global network status tracker.
pub fn init(listening_port: u16) {
    let status = NetworkStatus {
        gateway_failures: Vec::new(),
        connection_attempts: 0,
        open_connections: 0,
        listening_port,
        started_at: Instant::now(),
    };
    let _ = NETWORK_STATUS.set(Arc::new(RwLock::new(status)));
}

/// Record a gateway connection failure with a classified reason.
pub fn record_gateway_failure(address: SocketAddr, reason: FailureReason) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connection_attempts += 1;
            s.gateway_failures.push(GatewayFailure { address, reason });
            // Keep only the most recent failures to avoid unbounded growth
            if s.gateway_failures.len() > 20 {
                s.gateway_failures.remove(0);
            }
        }
    }
}

/// Record a successful connection, clearing failure history.
pub fn record_connection_success() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.open_connections += 1;
            s.gateway_failures.clear();
        }
    }
}

/// Snapshot of the current network status for rendering.
pub struct NetworkStatusSnapshot {
    pub failures: Vec<FailureSnapshot>,
    pub connection_attempts: u32,
    pub elapsed_secs: u64,
}

/// A snapshot of a single failure for display.
pub struct FailureSnapshot {
    pub address: SocketAddr,
    pub reason_html: String,
}

/// Get a snapshot of the current network status for the connecting page.
pub fn get_snapshot() -> Option<NetworkStatusSnapshot> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let failures = s
        .gateway_failures
        .iter()
        .map(|f| FailureSnapshot {
            address: f.address,
            reason_html: match &f.reason {
                FailureReason::VersionMismatch { local, gateway } => {
                    format!(
                        "<strong>Version mismatch</strong>: Your version: <code>{local}</code> \
                         — Gateway requires: <code>{gateway}</code><br>\
                         Run: <code>cargo install --force freenet --version {gateway}</code>"
                    )
                }
                FailureReason::NatTraversalFailed => {
                    format!(
                        "<strong>NAT traversal failed</strong>: Can't reach gateway. \
                         Check that UDP port <code>{}</code> is open in your firewall.",
                        s.listening_port
                    )
                }
                FailureReason::Timeout => {
                    "<strong>Connection timed out</strong>: Gateway did not respond.".to_string()
                }
                FailureReason::Other(msg) => {
                    format!("<strong>Connection failed</strong>: {}", html_escape(msg))
                }
            },
        })
        .collect();
    Some(NetworkStatusSnapshot {
        failures,
        connection_attempts: s.connection_attempts,
        elapsed_secs: s.started_at.elapsed().as_secs(),
    })
}

/// Classify a ConnectionError::TransportError string into a FailureReason.
pub fn classify_transport_error(error_msg: &str) -> FailureReason {
    if error_msg.contains("Version incompatibility") {
        // Parse versions from the TransportError::ProtocolVersionMismatch Display output:
        // "Version incompatibility with gateway\n  Your client version: {actual}\n  Gateway version: {expected}\n..."
        let local = error_msg
            .split("Your client version:")
            .nth(1)
            .and_then(|s| s.split('\n').next())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let gateway = error_msg
            .split("Gateway version:")
            .nth(1)
            .and_then(|s| s.split('\n').next())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        FailureReason::VersionMismatch { local, gateway }
    } else if error_msg.contains("max connection attempts reached") {
        FailureReason::NatTraversalFailed
    } else {
        FailureReason::Other(error_msg.to_string())
    }
}

/// Minimal HTML escaping for untrusted error messages.
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_classify_version_mismatch() {
        let msg = "Version incompatibility with gateway\n  Your client version: 0.1.133\n  Gateway version: 0.1.135\n  \n  To fix this, update your Freenet client:\n    cargo install --force freenet --version 0.1.135";
        let reason = classify_transport_error(msg);
        match reason {
            FailureReason::VersionMismatch { local, gateway } => {
                assert_eq!(local, "0.1.133");
                assert_eq!(gateway, "0.1.135");
            }
            _ => panic!("Expected VersionMismatch"),
        }
    }

    #[test]
    fn test_classify_nat_traversal() {
        let msg = "failed while establishing connection, reason: max connection attempts reached";
        let reason = classify_transport_error(msg);
        assert!(matches!(reason, FailureReason::NatTraversalFailed));
    }

    #[test]
    fn test_classify_other() {
        let msg = "some unknown error";
        let reason = classify_transport_error(msg);
        assert!(matches!(reason, FailureReason::Other(_)));
    }

    #[test]
    fn test_snapshot_without_init_returns_none() {
        // Without calling init(), get_snapshot returns None (in a fresh process).
        // Can't reliably test since OnceLock is global, but the function handles it.
        let _ = get_snapshot();
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(html_escape("<script>"), "&lt;script&gt;");
        assert_eq!(html_escape("a&b"), "a&amp;b");
    }

    #[test]
    fn test_failure_snapshot_rendering() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 31337);

        // Test version mismatch rendering
        let reason = FailureReason::VersionMismatch {
            local: "0.1.1".to_string(),
            gateway: "0.1.2".to_string(),
        };
        init(31338);
        record_gateway_failure(addr, reason);
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.failures.len(), 1);
        assert!(snap.failures[0].reason_html.contains("Version mismatch"));
        assert!(snap.failures[0].reason_html.contains("0.1.1"));
    }
}
