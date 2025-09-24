use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use freenet_stdlib::prelude::Transaction;
use regex::Regex;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::Level;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: Instant,
    pub node_id: String,
    pub level: Level,
    pub message: String,
    pub transaction_id: Option<Transaction>,
    pub operation_type: Option<OperationType>,
    pub fields: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Put,
    Get,
    Subscribe,
    Update,
    Connect,
}

pub struct UnifiedLogCollector {
    pub timeline: Arc<Mutex<BTreeMap<Instant, LogEntry>>>,
    pub node_streams: Arc<Mutex<HashMap<String, LogStream>>>,
    pub correlation_ids: Arc<Mutex<HashMap<Transaction, Vec<LogEntry>>>>,
    start_time: Instant,
}

pub struct LogStream {
    pub node_id: String,
    pub path: PathBuf,
    pub reader_task: Option<JoinHandle<Result<()>>>,
}

pub struct TransactionTrace {
    pub id: Transaction,
    pub events: Vec<LogEntry>,
    pub nodes_involved: Vec<String>,
    pub duration: std::time::Duration,
}

impl UnifiedLogCollector {
    pub fn new() -> Self {
        UnifiedLogCollector {
            timeline: Arc::new(Mutex::new(BTreeMap::new())),
            node_streams: Arc::new(Mutex::new(HashMap::new())),
            correlation_ids: Arc::new(Mutex::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    pub fn add_node(&mut self, node_id: &str, log_path: &Path) -> Result<()> {
        let stream = LogStream {
            node_id: node_id.to_string(),
            path: log_path.to_path_buf(),
            reader_task: None,
        };

        let timeline = self.timeline.clone();
        let correlation_ids = self.correlation_ids.clone();
        let node_id_clone = node_id.to_string();
        let path_clone = log_path.to_path_buf();

        // Spawn task to tail log file
        let reader_task = tokio::spawn(async move {
            Self::tail_log_file(node_id_clone, path_clone, timeline, correlation_ids).await
        });

        let mut streams = tokio::task::block_in_place(|| {
            futures::executor::block_on(self.node_streams.lock())
        });

        streams.insert(
            node_id.to_string(),
            LogStream {
                node_id: node_id.to_string(),
                path: log_path.to_path_buf(),
                reader_task: Some(reader_task),
            },
        );

        Ok(())
    }

    async fn tail_log_file(
        node_id: String,
        log_path: PathBuf,
        timeline: Arc<Mutex<BTreeMap<Instant, LogEntry>>>,
        correlation_ids: Arc<Mutex<HashMap<Transaction, Vec<LogEntry>>>>,
    ) -> Result<()> {
        // Wait for file to exist
        for _ in 0..100 {
            if log_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let file = File::open(&log_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if let Some(entry) = Self::parse_log_line(&node_id, &line) {
                let timestamp = entry.timestamp;
                let tx_id = entry.transaction_id;

                // Add to timeline
                timeline.lock().await.insert(timestamp, entry.clone());

                // Add to correlation map if there's a transaction ID
                if let Some(tx) = tx_id {
                    correlation_ids
                        .lock()
                        .await
                        .entry(tx)
                        .or_insert_with(Vec::new)
                        .push(entry);
                }
            }
        }

        Ok(())
    }

    fn parse_log_line(node_id: &str, line: &str) -> Option<LogEntry> {
        // Parse common log formats
        // Example: "2024-01-24T12:34:56.789Z DEBUG freenet::operations: Subscribe request tx=abc123"

        let timestamp_regex = Regex::new(r"^(\S+)\s+(\S+)\s+(.+)").ok()?;
        let tx_regex = Regex::new(r"tx=(\S+)").ok()?;

        let caps = timestamp_regex.captures(line)?;
        let level_str = caps.get(2)?.as_str();
        let message = caps.get(3)?.as_str().to_string();

        let level = match level_str.to_uppercase().as_str() {
            "ERROR" => Level::ERROR,
            "WARN" => Level::WARN,
            "INFO" => Level::INFO,
            "DEBUG" => Level::DEBUG,
            "TRACE" => Level::TRACE,
            _ => Level::INFO,
        };

        let transaction_id = tx_regex.captures(&message)
            .and_then(|c| c.get(1))
            .and_then(|m| m.as_str().parse::<u64>().ok())
            .map(Transaction::new::<u64>);

        let operation_type = Self::detect_operation_type(&message);

        Some(LogEntry {
            timestamp: Instant::now(), // In real impl, parse from log
            node_id: node_id.to_string(),
            level,
            message,
            transaction_id,
            operation_type,
            fields: HashMap::new(),
        })
    }

    fn detect_operation_type(message: &str) -> Option<OperationType> {
        if message.contains("Put") || message.contains("PUT") {
            Some(OperationType::Put)
        } else if message.contains("Get") || message.contains("GET") {
            Some(OperationType::Get)
        } else if message.contains("Subscribe") || message.contains("SUBSCRIBE") {
            Some(OperationType::Subscribe)
        } else if message.contains("Update") || message.contains("UPDATE") {
            Some(OperationType::Update)
        } else if message.contains("Connect") || message.contains("CONNECT") {
            Some(OperationType::Connect)
        } else {
            None
        }
    }

    pub async fn get_timeline(&self) -> String {
        let timeline = self.timeline.lock().await;
        let mut output = String::new();

        for (instant, entry) in timeline.iter() {
            let elapsed = instant.duration_since(self.start_time);
            writeln!(
                output,
                "[{:?}] [{}] [{}] {}",
                elapsed, entry.node_id, entry.level, entry.message
            ).unwrap();
        }

        output
    }

    pub async fn trace_transaction(&self, tx: Transaction) -> TransactionTrace {
        let correlation_ids = self.correlation_ids.lock().await;
        let events = correlation_ids
            .get(&tx)
            .cloned()
            .unwrap_or_default();

        let nodes_involved: Vec<String> = events
            .iter()
            .map(|e| e.node_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let duration = if events.len() >= 2 {
            let first = events.first().unwrap().timestamp;
            let last = events.last().unwrap().timestamp;
            last.duration_since(first)
        } else {
            std::time::Duration::ZERO
        };

        TransactionTrace {
            id: tx,
            events,
            nodes_involved,
            duration,
        }
    }

    pub async fn save_timeline(&self, path: &str) -> Result<()> {
        let timeline = self.get_timeline().await;
        let mut file = File::create(path)?;
        file.write_all(timeline.as_bytes())?;
        Ok(())
    }

    pub async fn generate_report(&self) -> String {
        let mut report = String::new();

        report.push_str("=== Log Analysis Report ===\n\n");

        // Count operations by type
        let timeline = self.timeline.lock().await;
        let mut op_counts = HashMap::new();

        for entry in timeline.values() {
            if let Some(op_type) = &entry.operation_type {
                *op_counts.entry(format!("{:?}", op_type)).or_insert(0) += 1;
            }
        }

        report.push_str("Operation Counts:\n");
        for (op_type, count) in op_counts {
            report.push_str(&format!("  {}: {}\n", op_type, count));
        }

        // Count log levels
        let mut level_counts = HashMap::new();
        for entry in timeline.values() {
            *level_counts.entry(entry.level.to_string()).or_insert(0) += 1;
        }

        report.push_str("\nLog Level Distribution:\n");
        for (level, count) in level_counts {
            report.push_str(&format!("  {}: {}\n", level, count));
        }

        // Transaction summary
        let correlation_ids = self.correlation_ids.lock().await;
        report.push_str(&format!("\nTotal Transactions: {}\n", correlation_ids.len()));

        report
    }
}

impl TransactionTrace {
    pub fn to_sequence_diagram(&self) -> String {
        let mut diagram = String::new();

        diagram.push_str("```mermaid\n");
        diagram.push_str("sequenceDiagram\n");

        for event in &self.events {
            if event.message.contains("->") {
                // Parse message flow
                diagram.push_str(&format!("    {} ->> {}: {}\n",
                    event.node_id, "NextNode", event.message));
            } else {
                diagram.push_str(&format!("    Note over {}: {}\n",
                    event.node_id, event.message));
            }
        }

        diagram.push_str("```\n");
        diagram
    }
}