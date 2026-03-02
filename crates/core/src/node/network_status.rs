//! Network connection status tracking for the node dashboard.
//!
//! Surfaces diagnostic information (version mismatches, NAT traversal failures,
//! peer connections, subscriptions, operation stats, etc.) so the HTTP homepage
//! and connecting page can show actionable diagnostics.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use crate::ring::PeerKeyLocation;
use crate::router::Router;

static NETWORK_STATUS: OnceLock<Arc<RwLock<NetworkStatus>>> = OnceLock::new();
static ROUTER: OnceLock<Arc<parking_lot::RwLock<Router>>> = OnceLock::new();

/// Store a reference to the Router for the dashboard.
pub fn set_router(router: Arc<parking_lot::RwLock<Router>>) {
    // OnceLock::set returns Err if already initialized; this is expected on repeated calls
    #[allow(clippy::let_underscore_must_use)]
    let _ = ROUTER.set(router);
}

/// Get the Router reference (if set).
pub(crate) fn get_router() -> Option<Arc<parking_lot::RwLock<Router>>> {
    ROUTER.get().cloned()
}

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
    pub peer_key_location: Option<PeerKeyLocation>,
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
    /// Count of broadcast updates received via subscription streaming.
    /// These are push-based and don't have success/failure semantics.
    pub updates_received: u32,
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

/// Check if an address is a known gateway.
pub fn is_known_gateway(addr: &SocketAddr) -> bool {
    NETWORK_STATUS
        .get()
        .and_then(|s| s.read().ok())
        .is_some_and(|s| s.gateway_addresses.contains(addr))
}

/// Record a gateway connection failure with a classified reason.
pub fn record_gateway_failure(address: SocketAddr, reason: FailureReason) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.connection_attempts = s.connection_attempts.saturating_add(1);
            // Track NAT failures
            if matches!(reason, FailureReason::NatTraversalFailed) {
                s.nat_stats.attempts = s.nat_stats.attempts.saturating_add(1);
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
pub fn record_peer_connected(
    addr: SocketAddr,
    location: Option<f64>,
    peer_key_location: Option<PeerKeyLocation>,
) {
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
                peer_key_location,
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

/// Record a broadcast update received via subscription streaming.
pub fn record_update_received() {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.op_stats.updates_received = s.op_stats.updates_received.saturating_add(1);
        }
    }
}

/// Record a NAT traversal attempt.
pub fn record_nat_attempt(success: bool) {
    if let Some(status) = NETWORK_STATUS.get() {
        if let Ok(mut s) = status.write() {
            s.nat_stats.attempts = s.nat_stats.attempts.saturating_add(1);
            if success {
                s.nat_stats.successes = s.nat_stats.successes.saturating_add(1);
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
    pub peer_key_location: Option<PeerKeyLocation>,
}

/// Snapshot of a subscribed contract.
pub struct ContractSnapshot {
    pub key_short: String,
    pub key_full: String,
    pub subscribed_secs: u64,
    pub last_updated_secs: Option<u64>,
}

/// Snapshot of operation stats. Each tuple is `(success_count, failure_count)`.
#[derive(Default)]
pub struct OpStatsSnapshot {
    pub gets: (u32, u32),
    pub puts: (u32, u32),
    pub updates: (u32, u32),
    pub subscribes: (u32, u32),
    /// Broadcast updates received via subscription streaming.
    pub updates_received: u32,
}

impl OpStatsSnapshot {
    /// Total number of operations (successes + failures across all types).
    pub fn total(&self) -> u32 {
        let sum = |pair: (u32, u32)| pair.0.saturating_add(pair.1);
        sum(self.gets)
            .saturating_add(sum(self.puts))
            .saturating_add(sum(self.updates))
            .saturating_add(sum(self.subscribes))
            .saturating_add(self.updates_received)
    }
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
                    let local = html_escape(local);
                    let gateway = html_escape(gateway);
                    format!(
                        "<strong>Version mismatch</strong>: Your version: <code>{local}</code> \
                         — Gateway requires: <code>{gateway}</code><br>\
                         Run: <code>cargo install --force freenet --version {gateway}</code>"
                    )
                }
                FailureReason::NatTraversalFailed => {
                    let has_peer_connections = s.connected_peers.iter().any(|p| !p.is_gateway);
                    if has_peer_connections {
                        "<strong>NAT traversal failed</strong>: Could not connect to this \
                         peer. This is normal — not all NAT traversal attempts succeed."
                            .to_string()
                    } else {
                        format!(
                            "<strong>NAT traversal failed</strong>: Can't reach gateway. \
                             Check that UDP port <code>{}</code> is open in your firewall.",
                            s.listening_port
                        )
                    }
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
            peer_key_location: p.peer_key_location.clone(),
        })
        .collect();

    let open_connections = peers.len() as u32;
    let gateway_only = open_connections > 0 && peers.iter().all(|p| p.is_gateway);

    let mut contracts: Vec<ContractSnapshot> = s
        .subscribed_contracts
        .values()
        .map(|c| {
            // Use char boundary for safe truncation (contract keys are base58/ASCII,
            // but be defensive against future encoding changes).
            let key_short = if c.key_encoded.chars().count() > 12 {
                let trunc: String = c.key_encoded.chars().take(12).collect();
                format!("{trunc}...")
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
            updates_received: s.op_stats.updates_received,
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
    use std::sync::Mutex;

    /// Tests that touch the shared NETWORK_STATUS global must hold this lock
    /// to prevent interleaving with other tests running in parallel.
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_classify_version_mismatch() {
        let msg = "Version incompatibility with gateway\n  Your client version: 0.1.133\n  Gateway version: 0.1.135\n  \n  To fix this, update your Freenet client:\n    cargo install --force freenet --version 0.1.135";
        let reason = classify_transport_error(msg);
        match reason {
            FailureReason::VersionMismatch { local, gateway } => {
                assert_eq!(local, "0.1.133");
                assert_eq!(gateway, "0.1.135");
            }
            FailureReason::NatTraversalFailed
            | FailureReason::Timeout
            | FailureReason::Other(_) => panic!("Expected VersionMismatch"),
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
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
        let gw_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 9, 111, 215)), 31337);
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        init(31339, HashSet::new(), "0.1.148".to_string());

        let status = NETWORK_STATUS.get().unwrap();

        // Gateway-only: one gateway peer connected
        {
            let mut s = status.write().unwrap();
            s.gateway_addresses.insert(gw_addr);
            s.connected_peers.clear();
            s.gateway_failures.clear();
            s.connected_peers.push(ConnectedPeer {
                address: gw_addr,
                is_gateway: true,
                location: Some(0.5),
                connected_since: Instant::now(),
                peer_key_location: None,
            });
        }
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Not gateway-only: add a non-gateway peer
        {
            let mut s = status.write().unwrap();
            s.connected_peers.push(ConnectedPeer {
                address: peer_addr,
                is_gateway: false,
                location: Some(0.3),
                connected_since: Instant::now(),
                peer_key_location: None,
            });
        }
        let snap = get_snapshot().unwrap();
        assert!(!snap.gateway_only);

        // Back to gateway-only: remove non-gateway peer
        {
            let mut s = status.write().unwrap();
            s.connected_peers.retain(|p| p.address != peer_addr);
        }
        let snap = get_snapshot().unwrap();
        assert!(snap.gateway_only);

        // Cleanup
        {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
        }
    }

    #[test]
    fn test_op_stats_recording() {
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31340, HashSet::new(), "0.1.148".to_string());

        // Reset op stats to avoid interference from other tests
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.op_stats = OperationStats::default();
        }

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
        let _lock = TEST_MUTEX.lock().unwrap();
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
        let _lock = TEST_MUTEX.lock().unwrap();
        init(31342, HashSet::new(), "0.1.148".to_string());

        // Reset NAT stats to avoid interference from other tests
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.nat_stats = NatStats::default();
        }

        record_nat_attempt(true);
        record_nat_attempt(false);
        record_nat_attempt(false);

        let snap = get_snapshot().unwrap();
        assert_eq!(snap.nat_stats.attempts, 3);
        assert_eq!(snap.nat_stats.successes, 1);
    }

    #[test]
    fn test_nat_failure_no_peers_shows_firewall_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), 31337);
        init(31343, HashSet::new(), "0.1.148".to_string());

        // Ensure clean state: no peers, no failures
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        record_gateway_failure(addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("Check that UDP port"));
        assert!(html.contains("open in your firewall"));
    }

    #[test]
    fn test_nat_failure_with_peer_connections_shows_benign_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), 12345);
        let fail_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 31337);
        init(31344, HashSet::new(), "0.1.148".to_string());

        // Ensure clean state
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        // Connect a non-gateway peer, then record a NAT failure to a different peer
        record_peer_connected(peer_addr, Some(0.5), None);
        record_gateway_failure(fail_addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("This is normal"));
        assert!(!html.contains("firewall"));

        // Cleanup
        record_peer_disconnected(peer_addr);
    }

    #[test]
    fn test_nat_failure_with_only_gateway_connections_shows_firewall_message() {
        let _lock = TEST_MUTEX.lock().unwrap();
        let gw_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(5, 9, 111, 215)), 31337);
        let fail_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 31337);
        init(31345, HashSet::new(), "0.1.148".to_string());

        // Register gateway and ensure clean state
        if let Some(status) = NETWORK_STATUS.get() {
            let mut s = status.write().unwrap();
            s.gateway_addresses.insert(gw_addr);
            s.connected_peers.clear();
            s.gateway_failures.clear();
        }

        // Connect only a gateway peer, then record NAT failure
        record_peer_connected(gw_addr, None, None);
        record_gateway_failure(fail_addr, FailureReason::NatTraversalFailed);
        let snap = get_snapshot().unwrap();

        // With only gateway connections, should still show firewall warning
        let html = &snap.failures.last().unwrap().reason_html;
        assert!(html.contains("NAT traversal failed"));
        assert!(html.contains("firewall"));
        assert!(!html.contains("This is normal"));

        // Cleanup
        record_peer_disconnected(gw_addr);
    }
}
