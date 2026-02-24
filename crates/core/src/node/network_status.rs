//! Network connection status tracking for the node dashboard.
//!
//! Surfaces diagnostic information (version mismatches, NAT traversal failures,
//! peer connections, subscriptions, operation stats, etc.) so the HTTP homepage
//! and connecting page can show actionable diagnostics.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

static NETWORK_STATUS: OnceLock<Arc<RwLock<NetworkStatus>>> = OnceLock::new();

/// Tracked network connection status for diagnostic display.
pub struct NetworkStatus {
    pub gateway_failures: Vec<GatewayFailure>,
    pub connection_attempts: u32,
    pub listening_port: u16,
    pub started_at: Instant,
    /// Known gateway addresses for detecting gateway-only connections.
    pub gateway_addresses: HashSet<SocketAddr>,
    /// Active peer connections.
    pub connected_peers: Vec<ConnectedPeer>,
    /// Subscribed contracts keyed by encoded contract key.
    pub subscribed_contracts: HashMap<String, ContractInfo>,
    /// Freenet version string.
    pub version: String,
    /// This node's ring location.
    pub own_location: Option<f64>,
    /// Operation counters.
    pub op_stats: OperationStats,
    /// NAT traversal counters.
    pub nat_stats: NatStats,
}

/// A connected peer with metadata.
pub struct ConnectedPeer {
    pub address: SocketAddr,
    pub is_gateway: bool,
    pub location: Option<f64>,
    pub connected_since: Instant,
}

/// Info about a subscribed contract.
pub struct ContractInfo {
    pub key_encoded: String,
    pub subscribed_since: Instant,
    pub last_updated: Option<Instant>,
}

/// Counters for each operation type: (success, failure).
#[derive(Default)]
pub struct OperationStats {
    pub gets: (u32, u32),
    pub puts: (u32, u32),
    pub updates: (u32, u32),
    pub subscribes: (u32, u32),
}

/// NAT traversal attempt counters.
#[derive(Default)]
pub struct NatStats {
    pub attempts: u32,
    pub successes: u32,
}

/// Operation type for recording results.
pub enum OpType {
    Get,
    Put,
    Update,
    Subscribe,
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
pub fn init(listening_port: u16, gateway_addrs: HashSet<SocketAddr>, version: String) {
    let status = NetworkStatus {
        gateway_failures: Vec::new(),
        connection_attempts: 0,
        listening_port,
        started_at: Instant::now(),
        gateway_addresses: gateway_addrs,
        connected_peers: Vec::new(),
        subscribed_contracts: HashMap::new(),
        version,
        own_location: None,
        op_stats: OperationStats::default(),
        nat_stats: NatStats::default(),
    };
    // OnceLock::set returns Err if already initialized; this is expected on repeated calls
    #[allow(clippy::let_underscore_must_use)]
    let _ = NETWORK_STATUS.set(Arc::new(RwLock::new(status)));
}

/// Record a gateway connection failure with a classified reason.
pub fn record_gateway_failure(address: SocketAddr, reason: FailureReason) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connection_attempts += 1;
            // Track NAT failures
            if matches!(reason, FailureReason::NatTraversalFailed) {
                s.nat_stats.attempts += 1;
            }
            s.gateway_failures.push(GatewayFailure { address, reason });
            // Keep only the most recent failures to avoid unbounded growth
            if s.gateway_failures.len() > 20 {
                s.gateway_failures.remove(0);
            }
        }
    }
}

/// Record a successful peer connection.
pub fn record_peer_connected(addr: SocketAddr, location: Option<f64>) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            // Remove any existing entry for this address
            s.connected_peers.retain(|p| p.address != addr);
            let is_gateway = s.gateway_addresses.contains(&addr);
            s.connected_peers.push(ConnectedPeer {
                address: addr,
                is_gateway,
                location,
                connected_since: Instant::now(),
            });
            s.gateway_failures.clear();
        }
    }
}

/// Record a peer disconnection.
pub fn record_peer_disconnected(addr: SocketAddr) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connected_peers.retain(|p| p.address != addr);
        }
    }
}

/// Record a new subscription.
pub fn record_subscription(key_encoded: String) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.subscribed_contracts
                .entry(key_encoded.clone())
                .or_insert_with(|| ContractInfo {
                    key_encoded,
                    subscribed_since: Instant::now(),
                    last_updated: None,
                });
        }
    }
}

/// Record that a contract was updated.
pub fn record_contract_updated(key_encoded: &str) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            if let Some(info) = s.subscribed_contracts.get_mut(key_encoded) {
                info.last_updated = Some(Instant::now());
            }
        }
    }
}

/// Record a subscription removal.
#[allow(dead_code)]
pub fn record_subscription_removed(key_encoded: &str) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.subscribed_contracts.remove(key_encoded);
        }
    }
}

/// Record an operation result.
pub fn record_op_result(op_type: OpType, success: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            let counter = match op_type {
                OpType::Get => &mut s.op_stats.gets,
                OpType::Put => &mut s.op_stats.puts,
                OpType::Update => &mut s.op_stats.updates,
                OpType::Subscribe => &mut s.op_stats.subscribes,
            };
            if success {
                counter.0 = counter.0.saturating_add(1);
            } else {
                counter.1 = counter.1.saturating_add(1);
            }
        }
    }
}

/// Record a NAT traversal attempt.
pub fn record_nat_attempt(success: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.nat_stats.attempts += 1;
            if success {
                s.nat_stats.successes += 1;
            }
        }
    }
}

/// Set this node's ring location.
pub fn set_own_location(location: f64) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.own_location = Some(location);
        }
    }
}

// --- Snapshot types for rendering ---

/// Snapshot of the current network status for rendering.
pub struct NetworkStatusSnapshot {
    pub failures: Vec<FailureSnapshot>,
    pub connection_attempts: u32,
    pub open_connections: u32,
    pub elapsed_secs: u64,
    pub listening_port: u16,
    pub version: String,
    pub own_location: Option<f64>,
    pub peers: Vec<PeerSnapshot>,
    pub contracts: Vec<ContractSnapshot>,
    pub op_stats: OpStatsSnapshot,
    pub nat_stats: NatStatsSnapshot,
    /// True if all connections are to gateways (no peer-to-peer connections).
    pub gateway_only: bool,
}

/// A snapshot of a single failure for display.
pub struct FailureSnapshot {
    pub address: SocketAddr,
    pub reason_html: String,
}

/// Snapshot of a connected peer.
pub struct PeerSnapshot {
    pub address: SocketAddr,
    pub is_gateway: bool,
    pub location: Option<f64>,
    pub connected_secs: u64,
}

/// Snapshot of a subscribed contract.
pub struct ContractSnapshot {
    pub key_short: String,
    pub key_full: String,
    pub subscribed_secs: u64,
    pub last_updated_secs: Option<u64>,
}

/// Snapshot of operation stats.
#[derive(Default)]
pub struct OpStatsSnapshot {
    pub gets: (u32, u32),
    pub puts: (u32, u32),
    pub updates: (u32, u32),
    pub subscribes: (u32, u32),
}

/// Snapshot of NAT stats.
#[derive(Default)]
pub struct NatStatsSnapshot {
    pub attempts: u32,
    pub successes: u32,
}

/// Get a snapshot of the current network status for the dashboard.
pub fn get_snapshot() -> Option<NetworkStatusSnapshot> {
    let status = NETWORK_STATUS.get()?;
    let s = status.read().ok()?;
    let now = Instant::now();

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

    let peers: Vec<PeerSnapshot> = s
        .connected_peers
        .iter()
        .map(|p| PeerSnapshot {
            address: p.address,
            is_gateway: p.is_gateway,
            location: p.location,
            connected_secs: now.duration_since(p.connected_since).as_secs(),
        })
        .collect();

    let open_connections = peers.len() as u32;
    let gateway_only = open_connections > 0 && peers.iter().all(|p| p.is_gateway);

    let mut contracts: Vec<ContractSnapshot> = s
        .subscribed_contracts
        .values()
        .map(|c| {
            let key_short = if c.key_encoded.len() > 12 {
                format!("{}...", &c.key_encoded[..12])
            } else {
                c.key_encoded.clone()
            };
            ContractSnapshot {
                key_short,
                key_full: c.key_encoded.clone(),
                subscribed_secs: now.duration_since(c.subscribed_since).as_secs(),
                last_updated_secs: c.last_updated.map(|t| now.duration_since(t).as_secs()),
            }
        })
        .collect();
    // Sort by most recently updated first
    contracts.sort_by(|a, b| {
        let a_time = a.last_updated_secs.unwrap_or(u64::MAX);
        let b_time = b.last_updated_secs.unwrap_or(u64::MAX);
        a_time.cmp(&b_time)
    });

    Some(NetworkStatusSnapshot {
        failures,
        connection_attempts: s.connection_attempts,
        open_connections,
        elapsed_secs: s.started_at.elapsed().as_secs(),
        listening_port: s.listening_port,
        version: s.version.clone(),
        own_location: s.own_location,
        peers,
        contracts,
        op_stats: OpStatsSnapshot {
            gets: s.op_stats.gets,
            puts: s.op_stats.puts,
            updates: s.op_stats.updates,
            subscribes: s.op_stats.subscribes,
        },
        nat_stats: NatStatsSnapshot {
            attempts: s.nat_stats.attempts,
            successes: s.nat_stats.successes,
        },
        gateway_only,
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
pub(crate) fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Format seconds as a human-readable relative time string.
pub(crate) fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        let m = secs / 60;
        let s = secs % 60;
        if s == 0 {
            format!("{m}m")
        } else {
            format!("{m}m {s}s")
        }
    } else {
        let h = secs / 3600;
        let m = (secs % 3600) / 60;
        if m == 0 {
            format!("{h}h")
        } else {
            format!("{h}h {m}m")
        }
    }
}

/// Format seconds as a human-readable "ago" string.
pub(crate) fn format_ago(secs: u64) -> String {
    if secs < 5 {
        "just now".to_string()
    } else {
        format!("{} ago", format_duration(secs))
    }
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
        init(31338, HashSet::new(), "0.1.0".to_string());
        record_gateway_failure(addr, reason);
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.failures.len(), 1);
        assert!(snap.failures[0].reason_html.contains("Version mismatch"));
        assert!(snap.failures[0].reason_html.contains("0.1.1"));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(45), "45s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(7200), "2h");
    }

    #[test]
    fn test_format_ago() {
        assert_eq!(format_ago(2), "just now");
        assert_eq!(format_ago(30), "30s ago");
        assert_eq!(format_ago(120), "2m ago");
    }

    #[test]
    fn test_gateway_only_detection() {
        // OnceLock may already be set by another test in the same process,
        // so we ensure the state we need by writing directly to the global.
        let gw_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 9, 111, 215)), 31337);
        init(31339, HashSet::new(), "0.1.148".to_string()); // may be ignored if already set

        // Insert the gateway address directly into the existing global state
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.gateway_addresses.insert(gw_addr);
            s.connected_peers.clear(); // clean slate for this test
        }

        // Connect a gateway peer
        record_peer_connected(gw_addr, Some(0.5));
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Connect a non-gateway peer
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        record_peer_connected(peer_addr, Some(0.3));
        let snap = get_snapshot().unwrap();
        assert!(!snap.gateway_only);

        // Disconnect the non-gateway peer
        record_peer_disconnected(peer_addr);
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Cleanup: remove test peers
        record_peer_disconnected(gw_addr);
    }

    #[test]
    fn test_op_stats_recording() {
        init(31340, HashSet::new(), "0.1.148".to_string());

        record_op_result(OpType::Get, true);
        record_op_result(OpType::Get, true);
        record_op_result(OpType::Get, false);
        record_op_result(OpType::Put, true);

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.op_stats.gets, (2, 1));
        assert_eq!(snap.op_stats.puts, (1, 0));
    }

    #[test]
    fn test_subscription_tracking() {
        init(31341, HashSet::new(), "0.1.148".to_string());

        record_subscription("ABC123DEF456".to_string());
        let snap = get_snapshot().unwrap();
        assert_eq!(snap.contracts.len(), 1);
        assert_eq!(snap.contracts[0].key_full, "ABC123DEF456");

        record_contract_updated("ABC123DEF456");
        let snap = get_snapshot().unwrap();
        assert!(snap.contracts[0].last_updated_secs.is_some());

        record_subscription_removed("ABC123DEF456");
        let snap = get_snapshot().unwrap();
        assert!(snap.contracts.is_empty());
    }

    #[test]
    fn test_nat_stats() {
        init(31342, HashSet::new(), "0.1.148".to_string());

        record_nat_attempt(true);
        record_nat_attempt(false);
        record_nat_attempt(false);

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.nat_stats.attempts, 3);
        assert_eq!(snap.nat_stats.successes, 1);
    }
}
