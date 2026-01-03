//! Event aggregation infrastructure for correlating transactions across multiple nodes.
//!
//! This module provides tools to collect and analyze events from multiple sources:
//! - AOF files (append-only log files from nodes)
//! - WebSocket streams (real-time collection during tests)
//! - TestEventListener (in-memory collection for unit tests)
//!
//! # Architecture Overview
//!
//! ## Production Behavior (EventRegister)
//!
//! Each node in production runs EventRegister which:
//! 1. Writes events to its own local AOF file (separated per node)
//! 2. Optionally reports events to a centralized WebSocket server (Network Monitor)
//! 3. Continues working even if WebSocket server is unavailable
//!
//! This means nodes ALWAYS have separated event logs, just like in production.
//!
//! ## WebSocketEventCollector = Centralized Server
//!
//! The WebSocketEventCollector is NOT a replacement for EventRegister.
//! It's a CENTRALIZED SERVER that:
//! - Listens for WebSocket connections from multiple nodes
//! - Receives copies of events from each node
//! - Aggregates them in real-time for debugging during tests
//!
//! Each node still:
//! - Has its own EventRegister instance
//! - Writes to its own separated AOF file
//! - Independently reports to the collector
//!
//! # Example Usage
//!
//! ## Integration Tests with WebSocket (Real-time Aggregation)
//! ```ignore
//! // 1. Start centralized collector BEFORE nodes
//! let collector = EventLogAggregator::with_websocket_collector(55010).await?;
//!
//! // 2. Configure nodes to report to collector
//! std::env::set_var("FDEV_NETWORK_METRICS_SERVER_PORT", "55010");
//!
//! // 3. Start nodes - each has its own EventRegister + AOF file
//! //    They will ALSO report to the collector via WebSocket
//! let (config_a, temp_a) = create_node_config(...).await?;  // Has own AOF
//! let (config_b, temp_b) = create_node_config(...).await?;  // Has own AOF
//! let node_a = start_node(config_a).await?;
//! let node_b = start_node(config_b).await?;
//!
//! // 4. Run operations...
//! let tx = make_put(&mut client, state, contract).await?;
//!
//! // 5. Query collector DURING test (real-time from WebSocket)
//! let flow = collector.get_transaction_flow(&tx).await?;
//! println!("Real-time flow: {:#?}", flow);
//!
//! // 6. Can ALSO read from AOF files post-test
//! let aof_aggregator = TestAggregatorBuilder::new()
//!     .add_node("node-a", temp_a.path().join("_EVENT_LOG_LOCAL"))
//!     .add_node("node-b", temp_b.path().join("_EVENT_LOG_LOCAL"))
//!     .build().await?;
//! ```
//!
//! ## Post-Test AOF Analysis (Recommended for Most Tests)
//! ```ignore
//! // Simpler: Just read AOF files after test completes
//! // No WebSocket setup needed, behavior identical to production
//! let aggregator = EventLogAggregator::from_aof_files(vec![
//!     (node_a_temp_dir.path().join("_EVENT_LOG_LOCAL"), Some("node-a".into())),
//!     (node_b_temp_dir.path().join("_EVENT_LOG_LOCAL"), Some("node-b".into())),
//! ]).await?;
//!
//! // Analyze
//! let flow = aggregator.get_transaction_flow(&tx).await?;
//! let routing_path = aggregator.get_routing_path(&tx).await?;
//! ```

use super::{EventKind, NetLogMessage};
use crate::{config::GlobalExecutor, message::Transaction, node::PeerId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

/// Trait for sources that provide event logs.
///
/// Implementations include:
/// - AOFEventSource: Historical events from disk files
/// - WebSocketEventCollector: Real-time events during test execution
/// - TestEventListenerSource: In-memory events from unit tests
///
/// This trait uses Rust's native async fn (RPITIT - Return Position Impl Trait in Traits).
/// We use generics rather than `dyn EventSource` since we never mix source types.
/// The `Send` bound on the trait ensures all futures are `Send`.
#[allow(async_fn_in_trait)]
pub trait EventSource: Send + Sync {
    /// Get all events collected by this source.
    async fn get_events(&self) -> Result<Vec<NetLogMessage>>;

    /// Get events for a specific transaction.
    async fn get_events_for_transaction(&self, tx: &Transaction) -> Result<Vec<NetLogMessage>> {
        let all_events = self.get_events().await?;
        Ok(all_events
            .into_iter()
            .filter(|event| &event.tx == tx)
            .collect())
    }

    /// Get a human-readable label for this source (e.g., "node-a", "gateway").
    fn get_label(&self) -> Option<String> {
        None
    }
}

/// Event source that reads from AOF (append-only file) logs.
#[derive(Clone)]
pub struct AOFEventSource {
    path: PathBuf,
    label: Option<String>,
    cached_events: Arc<RwLock<Option<Vec<NetLogMessage>>>>,
}

impl AOFEventSource {
    /// Create a new AOF event source from a file path.
    ///
    /// # Arguments
    /// * `path` - Path to the event log file
    /// * `label` - Optional human-readable label (e.g., "node-a")
    pub fn new(path: PathBuf, label: Option<String>) -> Self {
        Self {
            path,
            label,
            cached_events: Arc::new(RwLock::new(None)),
        }
    }
}

impl EventSource for AOFEventSource {
    async fn get_events(&self) -> Result<Vec<NetLogMessage>> {
        // Check cache first
        {
            let cached = self.cached_events.read().await;
            if let Some(events) = cached.as_ref() {
                return Ok(events.clone());
            }
        }

        // Read from file
        let events = super::aof::LogFile::read_all_events(&self.path).await?;

        // Cache for future calls
        {
            let mut cached = self.cached_events.write().await;
            *cached = Some(events.clone());
        }

        Ok(events)
    }

    fn get_label(&self) -> Option<String> {
        self.label.clone()
    }
}

/// Centralized WebSocket server that collects events from multiple nodes in real-time.
///
/// # Architecture
///
/// This is a **CENTRALIZED SERVER** that runs during tests to aggregate events
/// from multiple nodes. It does NOT replace EventRegister on nodes.
///
/// ## How It Works
///
/// 1. **Start collector** before starting any nodes
/// 2. **Nodes connect** to the collector via WebSocket (port 55010 by default)
/// 3. **Each node still has** its own EventRegister writing to separated AOF files
/// 4. **Nodes report copies** of events to the collector in real-time
/// 5. **Collector aggregates** events from all nodes in memory
/// 6. **Tests can query** the collector while nodes are running
///
/// ## Node Behavior
///
/// Each node continues to work exactly like in production:
/// - Has its own EventRegister instance
/// - Writes to its own local AOF file (e.g., `/tmp/node_a/.freenet/event_log`)
/// - Independently connects to the collector WebSocket server
/// - Sends copies of events to the collector
/// - Works even if collector is unavailable (falls back to AOF-only)
///
/// ## Use Cases
///
/// - **Real-time debugging**: Query transaction flows while test is running
/// - **Live monitoring**: Watch events as they happen across nodes
/// - **Test assertions**: Verify expected flows before test completes
///
/// ## Example
///
/// ```ignore
/// // Start collector
/// let collector = WebSocketEventCollector::new(55010).await?;
///
/// // Tell nodes to report to collector
/// std::env::set_var("FDEV_NETWORK_METRICS_SERVER_PORT", "55010");
///
/// // Start nodes (each with own EventRegister + AOF)
/// let node_a = start_node(config_a).await?;  // Writes to own AOF + reports to collector
/// let node_b = start_node(config_b).await?;  // Writes to own AOF + reports to collector
///
/// // Query collector in real-time
/// let events = collector.get_events().await?;
/// ```
pub struct WebSocketEventCollector {
    events: Arc<RwLock<Vec<NetLogMessage>>>,
    peer_labels: Arc<RwLock<HashMap<PeerId, String>>>,
    port: u16,
}

impl WebSocketEventCollector {
    /// Create and start a centralized WebSocket event collector server.
    ///
    /// This starts a WebSocket server that listens for connections from nodes.
    /// Nodes will connect and stream copies of their events to this collector.
    ///
    /// # Arguments
    /// * `port` - Port to listen on (default: 55010)
    ///
    /// # Note
    /// This does NOT replace EventRegister on nodes. Each node still maintains
    /// its own separated AOF file and EventRegister instance.
    pub async fn new(port: u16) -> Result<Self> {
        let events = Arc::new(RwLock::new(Vec::new()));
        let peer_labels = Arc::new(RwLock::new(HashMap::new()));

        let collector = Self {
            events: events.clone(),
            peer_labels: peer_labels.clone(),
            port,
        };

        // Start WebSocket server in background
        GlobalExecutor::spawn(Self::run_server(port, events, peer_labels));

        // Wait a bit for server to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(collector)
    }

    async fn run_server(
        port: u16,
        events: Arc<RwLock<Vec<NetLogMessage>>>,
        _peer_labels: Arc<RwLock<HashMap<PeerId, String>>>,
    ) -> Result<()> {
        use futures::StreamExt;
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let addr = format!("127.0.0.1:{}", port);
        let listener = TcpListener::bind(&addr).await?;
        tracing::info!("WebSocket event collector listening on {}", addr);

        while let Ok((stream, _)) = listener.accept().await {
            let events = events.clone();

            GlobalExecutor::spawn(async move {
                if let Ok(ws_stream) = accept_async(stream).await {
                    let (_write, mut read) = ws_stream.split();

                    while let Some(Ok(msg)) = read.next().await {
                        if let Ok(event) = Self::parse_message(msg) {
                            events.write().await.push(event);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    fn parse_message(msg: tokio_tungstenite::tungstenite::Message) -> Result<NetLogMessage> {
        use tokio_tungstenite::tungstenite::Message;

        match msg {
            Message::Binary(data) => {
                // Parse FlatBuffers message and convert to NetLogMessage
                Self::parse_flatbuffer(&data)
            }
            _ => anyhow::bail!("Unexpected message type"),
        }
    }

    fn parse_flatbuffer(_data: &[u8]) -> Result<NetLogMessage> {
        // TODO: Implement FlatBuffer parsing
        // This requires the generated topology types from FlatBuffers schema
        // For now, this is a placeholder that will be implemented when needed
        anyhow::bail!("FlatBuffer parsing not yet implemented - use AOF files for now")
    }

    /// Register a label for a peer ID.
    pub async fn register_peer_label(&self, peer_id: PeerId, label: String) {
        self.peer_labels.write().await.insert(peer_id, label);
    }

    /// Get the port this collector is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl EventSource for WebSocketEventCollector {
    async fn get_events(&self) -> Result<Vec<NetLogMessage>> {
        Ok(self.events.read().await.clone())
    }
}

/// Event source that wraps a TestEventListener for unit tests.
#[derive(Clone)]
pub struct TestEventListenerSource {
    listener: Arc<super::TestEventListener>,
}

impl TestEventListenerSource {
    /// Create a new source from a TestEventListener.
    #[allow(private_interfaces)]
    pub fn new(listener: Arc<super::TestEventListener>) -> Self {
        Self { listener }
    }
}

impl EventSource for TestEventListenerSource {
    async fn get_events(&self) -> Result<Vec<NetLogMessage>> {
        let logs = self.listener.logs.lock().await;
        Ok(logs.clone())
    }
}

/// Transaction flow event - a single event in a transaction's journey.
#[derive(Debug, Clone)]
pub struct TransactionFlowEvent {
    /// The peer that generated this event
    pub peer_id: PeerId,
    /// Human-readable label for the peer (if available)
    pub peer_label: Option<String>,
    /// The event kind
    pub event_kind: EventKind,
    /// When this event occurred
    pub timestamp: DateTime<Utc>,
}

/// A complete routing path for a transaction.
#[derive(Debug, Clone)]
pub struct RoutingPath {
    /// Transaction ID
    pub transaction: Transaction,
    /// Ordered list of peers the transaction traversed
    pub path: Vec<(PeerId, Option<String>)>,
    /// Total duration from start to end
    pub duration: Option<chrono::Duration>,
}

/// Main aggregator that collects and correlates events from multiple sources.
pub struct EventLogAggregator<S: EventSource> {
    sources: Vec<S>,
    cached_events: Arc<RwLock<Option<Vec<NetLogMessage>>>>,
}

impl<S: EventSource> EventLogAggregator<S> {
    /// Create a new aggregator from multiple event sources.
    pub fn new(sources: Vec<S>) -> Self {
        Self {
            sources,
            cached_events: Arc::new(RwLock::new(None)),
        }
    }

    /// Get all events from all sources.
    pub async fn get_all_events(&self) -> Result<Vec<NetLogMessage>> {
        // Check cache
        {
            let cached = self.cached_events.read().await;
            if let Some(events) = cached.as_ref() {
                return Ok(events.clone());
            }
        }

        // Collect from all sources
        let mut all_events = Vec::new();
        for source in &self.sources {
            let events = source.get_events().await?;
            all_events.extend(events);
        }

        // Sort by timestamp
        all_events.sort_by_key(|e| e.datetime);

        // Cache
        {
            let mut cached = self.cached_events.write().await;
            *cached = Some(all_events.clone());
        }

        Ok(all_events)
    }

    /// Get all events for a specific transaction, ordered by timestamp.
    pub async fn get_transaction_flow(
        &self,
        tx: &Transaction,
    ) -> Result<Vec<TransactionFlowEvent>> {
        let all_events = self.get_all_events().await?;

        let flow: Vec<TransactionFlowEvent> = all_events
            .into_iter()
            .filter(|event| &event.tx == tx)
            .map(|event| TransactionFlowEvent {
                peer_id: event.peer_id.clone(),
                peer_label: None, // TODO: Map peer_id to label
                event_kind: event.kind.clone(),
                timestamp: event.datetime,
            })
            .collect();

        Ok(flow)
    }

    /// Get the routing path for a transaction.
    ///
    /// This reconstructs which nodes the transaction traversed.
    pub async fn get_routing_path(&self, tx: &Transaction) -> Result<RoutingPath> {
        let flow = self.get_transaction_flow(tx).await?;

        let mut path = Vec::new();
        let mut seen_peers = std::collections::HashSet::new();

        for event in &flow {
            if seen_peers.insert(event.peer_id.clone()) {
                path.push((event.peer_id.clone(), event.peer_label.clone()));
            }
        }

        let duration = if flow.len() >= 2 {
            Some(flow.last().unwrap().timestamp - flow.first().unwrap().timestamp)
        } else {
            None
        };

        Ok(RoutingPath {
            transaction: *tx,
            path,
            duration,
        })
    }

    /// Export a Mermaid diagram for a transaction.
    ///
    /// Returns a Mermaid markdown string that can be rendered as a flowchart.
    pub async fn export_mermaid_graph(&self, tx: &Transaction) -> Result<String> {
        let flow = self.get_transaction_flow(tx).await?;

        let mut mermaid = String::from("```mermaid\ngraph TD\n");

        // Add nodes and edges
        let mut prev_id: Option<String> = None;
        for (idx, event) in flow.iter().enumerate() {
            let node_id = format!("N{}", idx);
            let peer_label = event
                .peer_label
                .clone()
                .unwrap_or_else(|| format!("{:.8}", event.peer_id.to_string()));
            let event_desc = format!("{:?}", event.event_kind);

            mermaid.push_str(&format!(
                "    {}[\"{}\\n{}\"]\n",
                node_id, peer_label, event_desc
            ));

            if let Some(prev) = prev_id {
                mermaid.push_str(&format!("    {} --> {}\n", prev, node_id));
            }

            prev_id = Some(node_id);
        }

        mermaid.push_str("```\n");
        Ok(mermaid)
    }

    /// Clear the event cache (useful if you want to re-read from sources).
    pub async fn clear_cache(&self) {
        *self.cached_events.write().await = None;
    }
}

// Factory methods for specific source types
impl EventLogAggregator<AOFEventSource> {
    /// Create an aggregator from multiple AOF files.
    ///
    /// # Example
    /// ```ignore
    /// let aggregator = EventLogAggregator::from_aof_files(vec![
    ///     (PathBuf::from("/tmp/node_a/event_log"), Some("node-a".into())),
    ///     (PathBuf::from("/tmp/node_b/event_log"), Some("node-b".into())),
    /// ]).await?;
    /// ```
    pub async fn from_aof_files(paths: Vec<(PathBuf, Option<String>)>) -> Result<Self> {
        let sources: Vec<AOFEventSource> = paths
            .into_iter()
            .map(|(path, label)| AOFEventSource::new(path, label))
            .collect();

        Ok(Self::new(sources))
    }
}

impl EventLogAggregator<WebSocketEventCollector> {
    /// Create an aggregator with a centralized WebSocket collector server.
    ///
    /// This starts a WebSocket server that nodes will connect to and report events.
    /// Each node still maintains its own EventRegister and AOF file.
    ///
    /// # Behavior
    ///
    /// - Starts WebSocket server on specified port
    /// - Nodes connect and stream copies of events
    /// - Each node still has separated AOF files
    /// - Collector aggregates events from all nodes in real-time
    ///
    /// # Usage
    ///
    /// 1. Start collector BEFORE starting nodes
    /// 2. Set `FDEV_NETWORK_METRICS_SERVER_PORT` environment variable
    /// 3. Start nodes (they will connect to collector)
    /// 4. Query aggregator during or after test
    ///
    /// # Example
    /// ```ignore
    /// // 1. Start collector
    /// let aggregator = EventLogAggregator::with_websocket_collector(55010).await?;
    ///
    /// // 2. Configure nodes to report to collector
    /// std::env::set_var("FDEV_NETWORK_METRICS_SERVER_PORT", "55010");
    ///
    /// // 3. Start nodes (each with own EventRegister + AOF file)
    /// let node_a = start_node(config_a).await?;
    /// let node_b = start_node(config_b).await?;
    ///
    /// // 4. Query in real-time
    /// let flow = aggregator.get_transaction_flow(&tx).await?;
    /// ```
    pub async fn with_websocket_collector(port: u16) -> Result<Self> {
        let collector = WebSocketEventCollector::new(port).await?;
        Ok(Self::new(vec![collector]))
    }
}

impl EventLogAggregator<TestEventListenerSource> {
    /// Create an aggregator from a TestEventListener (for unit tests).
    #[allow(private_interfaces)]
    pub fn from_test_listener(listener: Arc<super::TestEventListener>) -> Self {
        Self::new(vec![TestEventListenerSource::new(listener)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aggregator_creation() {
        let aggregator: EventLogAggregator<AOFEventSource> = EventLogAggregator::new(vec![]);
        let events = aggregator.get_all_events().await.unwrap();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_aggregator_with_aof_sources() {
        // Create mock event log paths
        let temp_dir = tempfile::tempdir().unwrap();
        let log1 = temp_dir.path().join("node1_log");
        let log2 = temp_dir.path().join("node2_log");

        // Create empty log files
        std::fs::write(&log1, []).unwrap();
        std::fs::write(&log2, []).unwrap();

        // Create aggregator with AOF sources using factory method
        let aggregator = EventLogAggregator::<AOFEventSource>::from_aof_files(vec![
            (log1, Some("node-1".into())),
            (log2, Some("node-2".into())),
        ])
        .await
        .unwrap();

        // Verify we can get events (should be empty for this test)
        let events = aggregator.get_all_events().await.unwrap();
        assert_eq!(events.len(), 0, "No events expected from empty logs");

        // Verify we can call it multiple times (caching works)
        let events2 = aggregator.get_all_events().await.unwrap();
        assert_eq!(events2.len(), 0);
    }

    #[tokio::test]
    async fn test_aggregator_cache_clearing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let log = temp_dir.path().join("node_log");
        std::fs::write(&log, []).unwrap();

        let aggregator =
            EventLogAggregator::<AOFEventSource>::from_aof_files(vec![(log, Some("node".into()))])
                .await
                .unwrap();

        // Get events (populates cache)
        let _events = aggregator.get_all_events().await.unwrap();

        // Clear cache
        aggregator.clear_cache().await;

        // Should still work after clearing
        let events = aggregator.get_all_events().await.unwrap();
        assert_eq!(events.len(), 0);
    }
}
