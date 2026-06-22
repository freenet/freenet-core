use std::sync::Arc;

use chrono::Utc;
use either::Either;
use freenet_stdlib::prelude::*;
use futures::{FutureExt, future::BoxFuture};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{
    message::{NetMessage, NetMessageV1, Transaction},
    node::PeerId,
    operations::{
        connect,
        get::{GetMsg, GetMsgResult},
        put::PutMsg,
        subscribe::{SubscribeMsg, SubscribeMsgResult},
        update::UpdateMsg,
    },
    ring::Location,
    router::RouteEvent,
};

#[cfg(feature = "trace-ot")]
pub(crate) use metrics_client::OTEventRegister;
pub(crate) use test::TestEventListener;

// Re-export for use in tests
pub use event_aggregator::{
    AOFEventSource, EventLogAggregator, EventSource, RoutingPath, TransactionFlowEvent,
    WebSocketEventCollector,
};
pub use state_verifier::{StateVerifier, VerificationReport};

use crate::node::OpManager;

/// An append-only log for network events.
mod aof;

/// Event aggregation across multiple nodes for debugging and testing.
pub mod event_aggregator;

/// Telemetry reporting to central collector.
pub mod telemetry;
pub use telemetry::TelemetryReporter;

/// Automatic state verification through telemetry linearization.
pub mod state_verifier;

/// Compute a full hash of contract state for convergence verification.
/// Returns all 32 bytes of Blake3 hash as 64 hex characters.
///
/// This provides cryptographically strong verification that states are identical.
/// With 256 bits, collision is computationally infeasible.
pub fn state_hash_full(state: &WrappedState) -> String {
    let hash = blake3::hash(state.as_ref());
    hash.to_hex().to_string()
}

/// Compute a short hash of contract state for telemetry display.
/// Returns first 4 bytes of Blake3 hash as 8 hex characters.
///
/// This is designed for quick visual comparison in logs and telemetry dashboards,
/// not for verification. Use `state_hash_full` for convergence checking.
pub fn state_hash_short(state: &WrappedState) -> String {
    let hash = blake3::hash(state.as_ref());
    let bytes = hash.as_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3]
    )
}

/// Register machinery: NetEventRegister trait, EventRegister, event log.
pub(crate) mod register;

/// Metrics server client functions and OpenTelemetry tracer.
pub(crate) mod metrics_client;

/// Event kind types for network events.
pub mod event_kind;

#[cfg(feature = "trace")]
pub mod tracer;

// Re-export moved items into the tracing root namespace.
// These are needed by the test module (via `use super::*`) and by
// other crate modules that import from crate::tracing.
#[cfg(feature = "trace-ot")]
pub(crate) use register::CombinedRegister;
pub(crate) use register::{
    DynamicRegister, EventRegister, ListenerLogId, NetEventLog, NetEventRegister,
};
// `NetLogMessage` and `EventFlushHandle` are part of the public `tracing` API
// (used by external crates, e.g. crates/core/tests), so they must stay `pub`.
pub use register::{EventFlushHandle, NetLogMessage};
// NEW_RECORDS_TS is needed by metrics_client's opentelemetry_tracer
pub(crate) use event_kind::{ConnectEvent, GetEvent, PutEvent, SubscribeEvent, UpdateEvent};
pub use event_kind::{
    ConnectionType, DisconnectReason, EventKind, HostingStoppedReason, InterestSyncEvent,
    OperationFailure, PeerLifecycleEvent, TransferDirection, TransferEvent,
};
pub(crate) use metrics_client::{
    connect_to_metrics_server, received_from_metrics_server, send_to_metrics_server,
};
pub(crate) use register::NEW_RECORDS_TS;

pub(super) mod test {
    use dashmap::DashMap;
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering::SeqCst},
    };

    use super::*;
    use crate::{node::testing_impl::NodeLabel, ring::Distance, transport::TransportPublicKey};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
        node_labels: Arc<DashMap<NodeLabel, TransportPublicKey>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        pub(crate) logs: Arc<tokio::sync::Mutex<Vec<NetLogMessage>>>,
        network_metrics_server:
            Arc<tokio::sync::Mutex<Option<WebSocketStream<MaybeTlsStream<TcpStream>>>>>,
    }

    impl TestEventListener {
        pub async fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(tokio::sync::Mutex::new(Vec::new())),
                network_metrics_server: Arc::new(tokio::sync::Mutex::new(
                    connect_to_metrics_server().await,
                )),
            }
        }

        pub fn add_node(&mut self, label: NodeLabel, peer: TransportPublicKey) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: &TransportPublicKey) -> bool {
            let Ok(logs) = self.logs.try_lock() else {
                return false;
            };
            logs.iter().any(|log| {
                log.peer_id.pub_key() == peer
                    && matches!(log.kind, EventKind::Connect(ConnectEvent::Connected { .. }))
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(
            &self,
            key: &TransportPublicKey,
        ) -> Box<dyn Iterator<Item = (PeerId, Distance)>> {
            let Ok(logs) = self.logs.try_lock() else {
                return Box::new([].into_iter());
            };
            let disconnects = logs
                .iter()
                .filter(|l| matches!(l.kind, EventKind::Disconnected { .. }))
                .fold(HashMap::<_, Vec<_>>::new(), |mut map, log| {
                    map.entry(log.peer_id.clone())
                        .or_default()
                        .push(log.datetime);
                    map
                });

            let iter = logs
                .iter()
                .filter_map(|l| {
                    if let EventKind::Connect(ConnectEvent::Connected {
                        this, connected, ..
                    }) = &l.kind
                    {
                        let connected_id =
                            PeerId::new(connected.pub_key().clone(), connected.socket_addr()?);
                        let disconnected = disconnects
                            .get(&connected_id)
                            .iter()
                            .flat_map(|dcs| dcs.iter())
                            .any(|dc| dc > &l.datetime);
                        if let Some((this_loc, conn_loc)) =
                            this.location().zip(connected.location())
                        {
                            if this.pub_key() == key && !disconnected {
                                return Some((connected_id, conn_loc.distance(this_loc)));
                            }
                        }
                    }
                    None
                })
                .collect::<HashMap<_, _>>()
                .into_iter();
            Box::new(iter)
        }

        fn create_log(log: NetEventLog) -> (NetLogMessage, ListenerLogId) {
            let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
            let NetEventLog { peer_id, kind, .. } = log;
            let msg_log = NetLogMessage {
                datetime: Utc::now(),
                tx: *log.tx,
                peer_id,
                kind,
            };
            (msg_log, log_id)
        }
    }

    impl super::NetEventRegister for TestEventListener {
        fn register_events<'a>(
            &'a self,
            logs: Either<NetEventLog<'a>, Vec<NetEventLog<'a>>>,
        ) -> BoxFuture<'a, ()> {
            async {
                match logs {
                    Either::Left(log) => {
                        let tx = log.tx;
                        let (msg_log, log_id) = Self::create_log(log);
                        if let Some(conn) = &mut *self.network_metrics_server.lock().await {
                            send_to_metrics_server(conn, &msg_log).await;
                        }
                        self.logs.lock().await.push(msg_log);
                        self.tx_log.entry(*tx).or_default().push(log_id);
                    }
                    Either::Right(logs) => {
                        let logs_list = &mut *self.logs.lock().await;
                        let mut lock = self.network_metrics_server.lock().await;
                        for log in logs {
                            let tx = log.tx;
                            let (msg_log, log_id) = Self::create_log(log);
                            if let Some(conn) = &mut *lock {
                                send_to_metrics_server(conn, &msg_log).await;
                            }
                            logs_list.push(msg_log);
                            self.tx_log.entry(*tx).or_default().push(log_id);
                        }
                    }
                }
            }
            .boxed()
        }

        fn trait_clone(&self) -> Box<dyn NetEventRegister> {
            Box::new(self.clone())
        }

        fn notify_of_time_out(
            &mut self,
            _: Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> BoxFuture<'_, ()> {
            async {}.boxed()
        }

        fn get_router_events(
            &self,
            _number: usize,
        ) -> BoxFuture<'_, anyhow::Result<Vec<RouteEvent>>> {
            async { Ok(vec![]) }.boxed()
        }
    }

    #[tokio::test]
    async fn test_get_connections() -> anyhow::Result<()> {
        let peer_id = PeerId::random();
        let tx = Transaction::new::<connect::ConnectMsg>();
        // Create other peers - location is now computed from their addresses
        let other_peers = [PeerId::random(), PeerId::random(), PeerId::random()];

        let listener = TestEventListener::new().await;
        let futs = futures::stream::FuturesUnordered::from_iter(other_peers.iter().map(|other| {
            listener.register_events(Either::Left(NetEventLog {
                tx: &tx,
                peer_id: peer_id.clone(),
                kind: EventKind::Connect(ConnectEvent::Connected {
                    this: peer_id.as_peer_key_location(),
                    connected: other.as_peer_key_location(),
                    elapsed_ms: None,
                    connection_type: ConnectionType::Direct,
                    latency_ms: None,
                    this_peer_connection_count: 0,
                    initiated_by: None,
                }),
            }))
        }));

        futures::future::join_all(futs).await;

        let distances: Vec<_> = listener.connections(peer_id.pub_key()).collect();
        assert_eq!(distances.len(), 3, "Should have 3 connections");
        // Verify each distance is valid (between 0 and 0.5 on the ring)
        for (_, dist) in &distances {
            assert!(
                dist.as_f64() >= 0.0 && dist.as_f64() <= 0.5,
                "Distance should be valid ring distance"
            );
        }
        Ok(())
    }

    #[test]
    fn test_state_hash_short() {
        use freenet_stdlib::prelude::WrappedState;

        // Test with known input produces consistent 8-char hex output
        let state = WrappedState::new(vec![1, 2, 3, 4, 5]);
        let hash = super::state_hash_short(&state);

        // Should be exactly 8 hex chars (4 bytes)
        assert_eq!(hash.len(), 8, "Hash should be 8 hex characters");
        assert!(
            hash.chars().all(|c| c.is_ascii_hexdigit()),
            "Hash should only contain hex digits"
        );

        // Same input produces same output (deterministic)
        assert_eq!(
            hash,
            super::state_hash_short(&state),
            "Hash should be deterministic"
        );

        // Different input produces different output
        let state2 = WrappedState::new(vec![5, 4, 3, 2, 1]);
        assert_ne!(
            hash,
            super::state_hash_short(&state2),
            "Different states should produce different hashes"
        );

        // Empty state still produces valid 8-char hash
        let empty = WrappedState::new(vec![]);
        let empty_hash = super::state_hash_short(&empty);
        assert_eq!(
            empty_hash.len(),
            8,
            "Empty state should still produce 8-char hash"
        );
    }
}

/// Per-attempt-transaction GET outcome summary (#4361).
///
/// The raw event stream multi-counts GET outcomes:
///
/// - a failed attempt registers a `GetNotFound` TWICE on the originator's
///   own node — once directly from the relay driver's exhaustion branch
///   and once when the loopback `Response{NotFound}` re-enters inbound
///   dispatch (`from_inbound_msg_v1`);
/// - multi-hop responses register one outcome event at every hop they
///   bubble through, so a single terminal outcome can appear N times.
///
/// Counting raw events therefore measures message traversal, not
/// operation outcomes. Grouping by attempt transaction — with success
/// dominating any co-registered failure events — yields exactly one
/// outcome per attempt.
///
/// Per-tx classification precedence: success > failure (any
/// `GetFailure` event) > timeout (max elapsed >=
/// [`GET_TIMEOUT_CLASSIFICATION_MS`]) > not_found.
///
/// Semantics caveat: these are WIRE-level attempt outcomes, not
/// client-visible outcomes. A `Found` that bubbles up after the
/// originator's per-attempt timeout still registers `GetSuccess` for
/// that attempt tx and counts as a success here, even though the
/// client saw NotFound; conversely each failed attempt of an
/// ultimately-successful GET counts as its own not_found. Suitable for
/// reliability diagnostics; not a client-SLA metric.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GetOutcomeSummary {
    pub successes: u64,
    pub not_found: u64,
    pub failures: u64,
    pub timeouts: u64,
    /// Subset of `successes` whose wire `hop_count >= 1` — the GET
    /// actually traversed the network rather than completing on a node
    /// that already held the contract locally.
    pub network_successes: u64,
    /// Elapsed ms per successful attempt (max across the hops that
    /// registered the success — the originator registers last, with the
    /// largest elapsed). Sorted ascending for deterministic output.
    pub success_elapsed_ms: Vec<u64>,
}

impl GetOutcomeSummary {
    pub fn total(&self) -> u64 {
        self.successes + self.not_found + self.failures + self.timeouts
    }
}

/// Failed GET outcomes with elapsed time at or above this threshold are
/// classified as timeouts rather than NotFound (close to the 60s
/// `OPERATION_TTL`).
pub const GET_TIMEOUT_CLASSIFICATION_MS: u64 = 55_000;

/// Summarize GET outcomes from an event log, deduplicated per attempt
/// transaction. See [`GetOutcomeSummary`] for why raw event counting is
/// wrong.
// Wildcard is deliberate, mirroring `hop_count`: only the three terminal
// GET variants matter here; new variants should not require updates.
#[allow(clippy::wildcard_enum_match_arm)]
pub fn summarize_get_outcomes_per_tx(logs: &[NetLogMessage]) -> GetOutcomeSummary {
    use std::collections::HashMap;

    #[derive(Default)]
    struct TxAgg {
        success: bool,
        success_elapsed: Option<u64>,
        max_hop: Option<usize>,
        saw_failure: bool,
        max_failure_elapsed: Option<u64>,
    }

    let mut per_tx: HashMap<Transaction, TxAgg> = HashMap::new();
    for log in logs {
        // Match variants directly rather than going through
        // `get_outcome()`, which collapses `GetNotFound` and `GetFailure`
        // into the same bucket — classifying genuine network/system
        // failures as contract absence (Codex review of #4364).
        let agg = match &log.kind {
            EventKind::Get(GetEvent::GetSuccess { .. }) => {
                let agg = per_tx.entry(log.tx).or_default();
                agg.success = true;
                if let Some(ms) = log.kind.get_elapsed_ms() {
                    agg.success_elapsed = Some(agg.success_elapsed.map_or(ms, |cur| cur.max(ms)));
                }
                if let Some(hops) = log.kind.hop_count() {
                    agg.max_hop = Some(agg.max_hop.map_or(hops, |cur| cur.max(hops)));
                }
                continue;
            }
            EventKind::Get(GetEvent::GetNotFound { .. }) => per_tx.entry(log.tx).or_default(),
            EventKind::Get(GetEvent::GetFailure { .. }) => {
                let agg = per_tx.entry(log.tx).or_default();
                agg.saw_failure = true;
                agg
            }
            _ => continue,
        };
        if let Some(ms) = log.kind.get_elapsed_ms() {
            agg.max_failure_elapsed = Some(agg.max_failure_elapsed.map_or(ms, |cur| cur.max(ms)));
        }
    }

    let mut summary = GetOutcomeSummary::default();
    for agg in per_tx.values() {
        if agg.success {
            summary.successes += 1;
            if agg.max_hop.unwrap_or(0) >= 1 {
                summary.network_successes += 1;
            }
            if let Some(ms) = agg.success_elapsed {
                summary.success_elapsed_ms.push(ms);
            }
        } else if agg.saw_failure {
            summary.failures += 1;
        } else if let Some(ms) = agg.max_failure_elapsed {
            if ms >= GET_TIMEOUT_CLASSIFICATION_MS {
                summary.timeouts += 1;
            } else {
                summary.not_found += 1;
            }
        } else {
            // Unreachable today (all three terminal GET events carry
            // elapsed_ms) but kept as a defensive bucket.
            summary.failures += 1;
        }
    }
    summary.success_elapsed_ms.sort_unstable();
    summary
}

#[cfg(test)]
mod get_outcome_summary_tests {
    use super::*;
    use crate::operations::get::GetMsg;
    use crate::ring::PeerKeyLocation;
    use crate::transport::TransportPublicKey;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId, ContractKey};
    use std::net::SocketAddr;

    fn make_peer_id(port: u16) -> PeerId {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let key = TransportPublicKey::from_bytes([port as u8; 32]);
        PeerId::new(key, addr)
    }

    fn make_pkl(port: u16) -> PeerKeyLocation {
        let key = TransportPublicKey::from_bytes([port as u8; 32]);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        PeerKeyLocation::new(key, addr)
    }

    fn make_key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
    }

    fn base_time() -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    fn not_found_event(tx: Transaction, port: u16, elapsed_ms: u64) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetNotFound {
                id: tx,
                requester: make_pkl(port),
                instance_id: *make_key().id(),
                target: make_pkl(port),
                hop_count: Some(0),
                elapsed_ms,
                timestamp: 100,
            }),
        }
    }

    fn success_event(
        tx: Transaction,
        port: u16,
        hop_count: Option<usize>,
        elapsed_ms: u64,
    ) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetSuccess {
                id: tx,
                requester: make_pkl(port),
                target: make_pkl(port),
                key: make_key(),
                hop_count,
                elapsed_ms,
                timestamp: 100,
                state_hash: None,
            }),
        }
    }

    /// Regression for #4361: one failed GET attempt registers TWO
    /// `GetNotFound` events on the originator node (relay-direct +
    /// loopback-Response inbound). Per-tx dedup must count it once.
    #[test]
    fn failed_attempt_double_registration_counts_once() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![not_found_event(tx, 3001, 5), not_found_event(tx, 3001, 6)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(
            summary.not_found, 1,
            "double-registered NotFound must dedup"
        );
        assert_eq!(summary.total(), 1);
    }

    /// A success bubbling through multiple hops registers one event per
    /// hop; it is still one outcome — and success dominates any
    /// co-registered NotFound on the same tx (a relay that exhausted one
    /// branch before another found the contract).
    #[test]
    fn multi_hop_success_counts_once_and_dominates() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![
            not_found_event(tx, 3003, 4),
            success_event(tx, 3002, Some(2), 10),
            success_event(tx, 3001, Some(2), 15),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1);
        assert_eq!(summary.not_found, 0, "success must dominate per tx");
        assert_eq!(
            summary.network_successes, 1,
            "hop_count >= 1 is a network success"
        );
        assert_eq!(
            summary.success_elapsed_ms,
            vec![15],
            "originator-side (max) elapsed wins"
        );
    }

    /// hop_count == 0 means the GET completed on a node that already had
    /// the contract — counted as success but NOT as a network success.
    #[test]
    fn local_hit_is_not_a_network_success() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![success_event(tx, 3001, Some(0), 1)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1);
        assert_eq!(summary.network_successes, 0);
    }

    fn failure_event(tx: Transaction, port: u16, elapsed_ms: u64) -> NetLogMessage {
        NetLogMessage {
            tx,
            datetime: base_time(),
            peer_id: make_peer_id(port),
            kind: EventKind::Get(GetEvent::GetFailure {
                id: tx,
                requester: make_pkl(port),
                instance_id: *make_key().id(),
                target: make_pkl(port),
                hop_count: Some(0),
                reason: OperationFailure::ConnectionDropped,
                elapsed_ms,
                timestamp: 100,
            }),
        }
    }

    /// `GetFailure` events classify as failures — not as not_found —
    /// regardless of elapsed time. Regression for the Codex review
    /// finding on #4364: classifying by elapsed time alone collapsed
    /// genuine network/system failures into "contract absent".
    #[test]
    fn get_failure_classifies_as_failure_not_not_found() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![failure_event(tx, 3001, 10)];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.failures, 1, "GetFailure must land in failures");
        assert_eq!(summary.not_found, 0);
        assert_eq!(summary.total(), 1);
    }

    /// failure > timeout precedence: a `GetFailure` at or above the
    /// timeout threshold is still a failure — the timeout bucket is a
    /// heuristic for NotFound without an explicit reason. Pins the
    /// branch order in the per-tx fold (#4364 testing review).
    #[test]
    fn get_failure_above_timeout_threshold_stays_failure() {
        let tx = Transaction::new::<GetMsg>();
        let logs = vec![failure_event(
            tx,
            3001,
            GET_TIMEOUT_CLASSIFICATION_MS + 1_000,
        )];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(
            summary.failures, 1,
            "failure must outrank the timeout heuristic"
        );
        assert_eq!(summary.timeouts, 0);
        assert_eq!(summary.total(), 1);
    }

    /// success > failure precedence on the same tx — and a mixed
    /// NotFound + Failure tx resolves to failure.
    #[test]
    fn success_dominates_failure_and_failure_dominates_not_found() {
        let tx1 = Transaction::new::<GetMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let logs = vec![
            failure_event(tx1, 3002, 5),
            success_event(tx1, 3001, Some(2), 20),
            not_found_event(tx2, 3003, 5),
            failure_event(tx2, 3003, 6),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.successes, 1, "success must dominate failure per tx");
        assert_eq!(
            summary.failures, 1,
            "failure must dominate not_found per tx"
        );
        assert_eq!(summary.not_found, 0);
        assert_eq!(summary.total(), 2);
    }

    /// Failed outcomes at or above the timeout threshold classify as
    /// timeouts; distinct transactions stay distinct.
    #[test]
    fn timeout_classification_and_distinct_txs() {
        let tx1 = Transaction::new::<GetMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let logs = vec![
            not_found_event(tx1, 3001, GET_TIMEOUT_CLASSIFICATION_MS),
            not_found_event(tx2, 3002, 10),
        ];
        let summary = summarize_get_outcomes_per_tx(&logs);
        assert_eq!(summary.timeouts, 1);
        assert_eq!(summary.not_found, 1);
        assert_eq!(summary.total(), 2);
    }
}
