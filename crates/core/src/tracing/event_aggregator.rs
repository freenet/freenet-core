//! Event aggregation infrastructure for correlating transactions across multiple nodes.
//!
//! This module provides tools to collect and analyze events from multiple sources:
//! - AOF files (append-only log files from nodes)
//! - WebSocket streams (real-time collection during tests)
//! - TestEventListener (in-memory collection for unit tests)
//!
//! # Example Usage
//!
//! ## Integration Tests with WebSocket
//! ```ignore
//! // Start WebSocket collector before nodes
//! let aggregator = EventLogAggregator::with_websocket_collector(55010).await?;
//!
//! // Start nodes (they connect automatically via FDEV_NETWORK_METRICS_SERVER_PORT)
//! std::env::set_var("FDEV_NETWORK_METRICS_SERVER_PORT", "55010");
//! let node_a = start_node(config_a).await?;
//! let node_b = start_node(config_b).await?;
//!
//! // Run operations...
//! let tx = make_put(&mut client, state, contract).await?;
//!
//! // Query transaction flow
//! let flow = aggregator.get_transaction_flow(&tx).await?;
//! println!("Transaction flow: {:#?}", flow);
//!
//! // Export graph
//! let graph = aggregator.export_mermaid_graph(&tx)?;
//! println!("{}", graph);
//! ```
//!
//! ## Post-Test AOF Analysis
//! ```ignore
//! // After test completes, aggregate from AOF files
//! let aggregator = EventLogAggregator::from_aof_files(vec![
//!     node_a_temp_dir.path().join("event_log"),
//!     node_b_temp_dir.path().join("event_log"),
//!     gateway_temp_dir.path().join("event_log"),
//! ]).await?;
//!
//! // Analyze
//! let flow = aggregator.get_transaction_flow(&tx)?;
//! let routing_path = aggregator.get_routing_path(&tx)?;
//! ```

use super::{EventKind, NetLogMessage};
use crate::{message::Transaction, node::PeerId};
use anyhow::Result;
use chrono::{DateTime, Utc};
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

/// A source of network events that can be queried by the aggregator.
///
/// This trait abstracts over different event sources:
/// - AOF files (historical events from disk)
/// - WebSocket streams (real-time events during test execution)
/// - TestEventListener (in-memory events from unit tests)
#[async_trait::async_trait]
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

#[async_trait::async_trait]
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

/// Event source that collects events via WebSocket in real-time.
///
/// This starts a WebSocket server that nodes can connect to and push events.
/// Compatible with the existing EventRegister WebSocket client.
pub struct WebSocketEventCollector {
    events: Arc<RwLock<Vec<NetLogMessage>>>,
    peer_labels: Arc<RwLock<HashMap<PeerId, String>>>,
    port: u16,
}

impl WebSocketEventCollector {
    /// Create and start a WebSocket event collector.
    ///
    /// # Arguments
    /// * `port` - Port to listen on (default: 55010)
    pub async fn new(port: u16) -> Result<Self> {
        let events = Arc::new(RwLock::new(Vec::new()));
        let peer_labels = Arc::new(RwLock::new(HashMap::new()));

        let collector = Self {
            events: events.clone(),
            peer_labels: peer_labels.clone(),
            port,
        };

        // Start WebSocket server in background
        tokio::spawn(Self::run_server(port, events, peer_labels));

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

            tokio::spawn(async move {
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

#[async_trait::async_trait]
impl EventSource for WebSocketEventCollector {
    async fn get_events(&self) -> Result<Vec<NetLogMessage>> {
        Ok(self.events.read().await.clone())
    }
}

/// Event source that wraps a TestEventListener for unit tests.
pub struct TestEventListenerSource {
    listener: Arc<super::TestEventListener>,
}

impl TestEventListenerSource {
    /// Create a new source from a TestEventListener.
    pub fn new(listener: Arc<super::TestEventListener>) -> Self {
        Self { listener }
    }
}

#[async_trait::async_trait]
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
pub struct EventLogAggregator {
    sources: Vec<Box<dyn EventSource>>,
    cached_events: Arc<RwLock<Option<Vec<NetLogMessage>>>>,
}

impl EventLogAggregator {
    /// Create a new aggregator from multiple event sources.
    pub fn new(sources: Vec<Box<dyn EventSource>>) -> Self {
        Self {
            sources,
            cached_events: Arc::new(RwLock::new(None)),
        }
    }

    /// Create an aggregator from multiple AOF files.
    ///
    /// # Example
    /// ```ignore
    /// let aggregator = EventLogAggregator::from_aof_files(vec![
    ///     PathBuf::from("/tmp/node_a/event_log"),
    ///     PathBuf::from("/tmp/node_b/event_log"),
    /// ]).await?;
    /// ```
    pub async fn from_aof_files(paths: Vec<(PathBuf, Option<String>)>) -> Result<Self> {
        let sources: Vec<Box<dyn EventSource>> = paths
            .into_iter()
            .map(|(path, label)| Box::new(AOFEventSource::new(path, label)) as Box<dyn EventSource>)
            .collect();

        Ok(Self::new(sources))
    }

    /// Create an aggregator with a WebSocket collector.
    ///
    /// This starts a WebSocket server that nodes can connect to.
    /// Set `FDEV_NETWORK_METRICS_SERVER_PORT` environment variable to the port.
    ///
    /// # Example
    /// ```ignore
    /// let aggregator = EventLogAggregator::with_websocket_collector(55010).await?;
    /// std::env::set_var("FDEV_NETWORK_METRICS_SERVER_PORT", "55010");
    /// // Now start your nodes...
    /// ```
    pub async fn with_websocket_collector(port: u16) -> Result<Self> {
        let collector = WebSocketEventCollector::new(port).await?;
        Ok(Self::new(vec![Box::new(collector)]))
    }

    /// Create an aggregator from a TestEventListener (for unit tests).
    pub fn from_test_listener(listener: Arc<super::TestEventListener>) -> Self {
        Self::new(vec![Box::new(TestEventListenerSource::new(listener))])
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
    pub async fn get_transaction_flow(&self, tx: &Transaction) -> Result<Vec<TransactionFlowEvent>> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_aggregator_creation() {
        let aggregator = EventLogAggregator::new(vec![]);
        let events = aggregator.get_all_events().await.unwrap();
        assert_eq!(events.len(), 0);
    }
}
