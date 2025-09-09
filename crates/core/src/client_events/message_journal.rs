//! Message journal for durable message logging and replay
//!
//! This module provides infrastructure for:
//! - Persistent message logging for SessionActor recovery
//! - Idempotent message replay during actor restart
//! - State checkpointing to reduce replay time
//! - Transaction ordering guarantees

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::client_events::{ClientId, RequestId, HostResult};
use crate::contract::SessionMessage;
use crate::message::Transaction;

/// Journalable session message (simplified for serialization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JournalableSessionMessage {
    RegisterTransaction {
        tx: Transaction,
        client_id: ClientId,
        request_id: RequestId,
    },
    ClientDisconnect {
        client_id: ClientId,
    },
    DeliverHostResponse {
        tx: Transaction,
        // Note: We don't serialize the actual response for space efficiency
        // Recovery will need to handle missing responses appropriately
    },
    RegisterClient {
        client_id: ClientId,
        request_id: RequestId,
        // Note: transport_tx and token cannot be serialized
    },
    DeliverResult {
        tx: Transaction,
        // Note: We don't serialize the actual result for space efficiency  
    },
}

impl From<&SessionMessage> for JournalableSessionMessage {
    fn from(msg: &SessionMessage) -> Self {
        match msg {
            SessionMessage::RegisterTransaction { tx, client_id, request_id } => {
                JournalableSessionMessage::RegisterTransaction {
                    tx: *tx,
                    client_id: *client_id,
                    request_id: *request_id,
                }
            }
            SessionMessage::ClientDisconnect { client_id } => {
                JournalableSessionMessage::ClientDisconnect {
                    client_id: *client_id,
                }
            }
            SessionMessage::DeliverHostResponse { tx, .. } => {
                JournalableSessionMessage::DeliverHostResponse {
                    tx: *tx,
                }
            }
            SessionMessage::RegisterClient { client_id, request_id, .. } => {
                JournalableSessionMessage::RegisterClient {
                    client_id: *client_id,
                    request_id: *request_id,
                }
            }
            SessionMessage::DeliverResult { tx, .. } => {
                JournalableSessionMessage::DeliverResult {
                    tx: *tx,
                }
            }
        }
    }
}

/// Sequence number for message ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub fn new(value: u64) -> Self {
        Self(value)
    }
    
    pub fn value(&self) -> u64 {
        self.0
    }
    
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl std::fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq:{}", self.0)
    }
}

/// Journal entry with sequence number and timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub sequence: SequenceNumber,
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub message: JournalableSessionMessage,
}

impl JournalEntry {
    pub fn new(sequence: SequenceNumber, message: SessionMessage) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
            
        Self {
            sequence,
            timestamp,
            message: JournalableSessionMessage::from(&message),
        }
    }
}

/// Configuration for message journal
#[derive(Debug, Clone)]
pub struct JournalConfig {
    /// Directory for journal files
    pub journal_dir: PathBuf,
    /// Maximum journal file size before rotation
    pub max_file_size: u64,
    /// Maximum number of journal files to keep
    pub max_files: usize,
    /// Flush interval for durability
    pub flush_interval: Duration,
    /// Enable compression for journal files
    pub enable_compression: bool,
    /// Maximum replay window (entries to keep in memory)
    pub max_replay_entries: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            journal_dir: PathBuf::from("./data/journal"),
            max_file_size: 100 * 1024 * 1024, // 100MB per file
            max_files: 10, // Keep 10 files max
            flush_interval: Duration::from_millis(100), // Flush every 100ms
            enable_compression: true,
            max_replay_entries: 10_000, // Keep last 10k entries for replay
        }
    }
}

/// Message journal for durable logging and replay
pub struct MessageJournal {
    config: JournalConfig,
    current_file: Arc<Mutex<Option<BufWriter<File>>>>,
    current_sequence: Arc<AtomicU64>,
    current_file_size: Arc<AtomicU64>,
    journal_dir: PathBuf,
    
    // In-memory cache for fast replay
    recent_entries: Arc<RwLock<Vec<JournalEntry>>>,
}

impl MessageJournal {
    /// Create a new message journal
    pub async fn new(config: JournalConfig) -> Result<Self> {
        // Ensure journal directory exists
        std::fs::create_dir_all(&config.journal_dir)
            .with_context(|| format!("Failed to create journal directory: {:?}", config.journal_dir))?;

        let journal = Self {
            current_file: Arc::new(Mutex::new(None)),
            current_sequence: Arc::new(AtomicU64::new(0)),
            current_file_size: Arc::new(AtomicU64::new(0)),
            journal_dir: config.journal_dir.clone(),
            recent_entries: Arc::new(RwLock::new(Vec::new())),
            config,
        };

        // Initialize from existing journals
        journal.initialize_from_existing().await?;
        
        // Open current journal file
        journal.open_current_file().await?;
        
        info!(
            "Message journal initialized at {:?}, starting sequence: {}",
            journal.journal_dir,
            journal.current_sequence.load(Ordering::SeqCst)
        );
        
        Ok(journal)
    }

    /// Append a message to the journal
    pub async fn append(&self, message: SessionMessage) -> Result<SequenceNumber> {
        let sequence = SequenceNumber::new(
            self.current_sequence.fetch_add(1, Ordering::SeqCst) + 1
        );
        
        let entry = JournalEntry::new(sequence, message);
        
        // Serialize entry
        let serialized = self.serialize_entry(&entry)?;
        
        // Write to journal file
        {
            let mut file_guard = self.current_file.lock().await;
            if let Some(ref mut writer) = &mut *file_guard {
                writer.write_all(&serialized)?;
                writer.write_all(b"\n")?; // Line separator
                
                // Update file size
                let new_size = self.current_file_size.fetch_add(
                    serialized.len() as u64 + 1, 
                    Ordering::SeqCst
                ) + serialized.len() as u64 + 1;
                
                // Check if we need to rotate the file
                if new_size > self.config.max_file_size {
                    self.rotate_journal_file().await?;
                }
            } else {
                return Err(anyhow!("Journal file not open"));
            }
        }
        
        // Update in-memory cache
        {
            let mut entries = self.recent_entries.write().await;
            entries.push(entry);
            
            // Keep only recent entries
            let len = entries.len();
            if len > self.config.max_replay_entries {
                entries.drain(0..len - self.config.max_replay_entries);
            }
        }
        
        debug!("Appended message to journal: {}", sequence);
        Ok(sequence)
    }

    /// Append a journalable message to the journal
    pub async fn append_journalable(&self, message: JournalableSessionMessage) -> Result<SequenceNumber> {
        let sequence = SequenceNumber::new(
            self.current_sequence.fetch_add(1, Ordering::SeqCst) + 1
        );
        
        let entry = JournalEntry {
            sequence,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            message,
        };
        
        // Serialize entry
        let serialized = self.serialize_entry(&entry)?;
        
        // Write to journal file
        {
            let mut file_guard = self.current_file.lock().await;
            if let Some(ref mut writer) = &mut *file_guard {
                writer.write_all(&serialized)?;
                writer.write_all(b"\n")?; // Line separator
                
                // Update file size
                let new_size = self.current_file_size.fetch_add(
                    serialized.len() as u64 + 1, 
                    Ordering::SeqCst
                ) + serialized.len() as u64 + 1;
                
                // Check if we need to rotate the file
                if new_size > self.config.max_file_size {
                    self.rotate_journal_file().await?;
                }
            } else {
                return Err(anyhow!("Journal file not open"));
            }
        }
        
        // Update in-memory cache
        {
            let mut entries = self.recent_entries.write().await;
            entries.push(entry);
            
            // Keep only recent entries
            let len = entries.len();
            if len > self.config.max_replay_entries {
                entries.drain(0..len - self.config.max_replay_entries);
            }
        }
        
        debug!("Appended journalable message to journal: {}", sequence);
        Ok(sequence)
    }

    /// Replay messages from a given sequence number
    pub async fn replay_from(&self, from_sequence: SequenceNumber) -> Result<Vec<JournalEntry>> {
        let mut result = Vec::new();
        
        // First, check in-memory cache
        {
            let entries = self.recent_entries.read().await;
            for entry in entries.iter() {
                if entry.sequence >= from_sequence {
                    result.push(entry.clone());
                }
            }
        }
        
        // If we have enough entries in memory, return them
        if !result.is_empty() {
            info!(
                "Replaying {} messages from memory cache starting at {}",
                result.len(), 
                from_sequence
            );
            return Ok(result);
        }
        
        // Otherwise, read from journal files
        result = self.replay_from_files(from_sequence).await?;
        
        info!(
            "Replaying {} messages from journal files starting at {}",
            result.len(), 
            from_sequence
        );
        
        Ok(result)
    }

    /// Get current sequence number
    pub fn current_sequence(&self) -> SequenceNumber {
        SequenceNumber::new(self.current_sequence.load(Ordering::SeqCst))
    }

    /// Force flush journal to disk
    pub async fn flush(&self) -> Result<()> {
        let mut file_guard = self.current_file.lock().await;
        if let Some(ref mut writer) = &mut *file_guard {
            writer.flush()?;
        }
        Ok(())
    }

    /// Initialize journal from existing files
    async fn initialize_from_existing(&self) -> Result<()> {
        let mut max_sequence = 0u64;
        let mut recent_entries = Vec::new();
        
        // Read existing journal files
        let entries = match std::fs::read_dir(&self.journal_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(()), // Directory doesn't exist yet
        };
        
        let mut journal_files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "journal") {
                journal_files.push(path);
            }
        }
        
        // Sort files by name (which includes timestamp)
        journal_files.sort();
        
        let journal_file_count = journal_files.len();
        
        // Read entries from files
        for file_path in journal_files {
            let file_entries = self.read_journal_file(&file_path).await?;
            
            for entry in file_entries {
                max_sequence = max_sequence.max(entry.sequence.value());
                recent_entries.push(entry);
            }
        }
        
        // Keep only recent entries in memory
        let len = recent_entries.len();
        if len > self.config.max_replay_entries {
            recent_entries.drain(0..len - self.config.max_replay_entries);
        }
        
        // Update state
        self.current_sequence.store(max_sequence, Ordering::SeqCst);
        *self.recent_entries.write().await = recent_entries;
        
        info!(
            "Initialized from {} journal files, max sequence: {}",
            journal_file_count,
            max_sequence
        );
        
        Ok(())
    }

    /// Open current journal file for writing
    async fn open_current_file(&self) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
            
        let filename = format!("journal-{}.journal", timestamp);
        let file_path = self.journal_dir.join(filename);
        
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .with_context(|| format!("Failed to open journal file: {:?}", file_path))?;
            
        let writer = BufWriter::new(file);
        *self.current_file.lock().await = Some(writer);
        self.current_file_size.store(0, Ordering::SeqCst);
        
        info!("Opened journal file: {:?}", file_path);
        Ok(())
    }

    /// Rotate journal file when it gets too large
    async fn rotate_journal_file(&self) -> Result<()> {
        // Close current file
        {
            let mut file_guard = self.current_file.lock().await;
            if let Some(writer) = file_guard.take() {
                writer.into_inner()?.sync_all()?;
            }
        }
        
        // Open new file
        self.open_current_file().await?;
        
        // Clean up old files
        self.cleanup_old_files().await?;
        
        info!("Rotated journal file");
        Ok(())
    }

    /// Clean up old journal files
    async fn cleanup_old_files(&self) -> Result<()> {
        let entries = match std::fs::read_dir(&self.journal_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(()),
        };
        
        let mut journal_files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "journal") {
                journal_files.push(path);
            }
        }
        
        // Sort files by modification time (oldest first)
        journal_files.sort_by_key(|path| {
            path.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });
        
        // Remove old files if we exceed max_files
        if journal_files.len() > self.config.max_files {
            let files_to_remove = journal_files.len() - self.config.max_files;
            
            for file_path in journal_files.iter().take(files_to_remove) {
                match std::fs::remove_file(file_path) {
                    Ok(()) => info!("Removed old journal file: {:?}", file_path),
                    Err(e) => warn!("Failed to remove journal file {:?}: {}", file_path, e),
                }
            }
        }
        
        Ok(())
    }

    /// Read entries from a specific journal file
    async fn read_journal_file(&self, file_path: &Path) -> Result<Vec<JournalEntry>> {
        let file = File::open(file_path)
            .with_context(|| format!("Failed to open journal file: {:?}", file_path))?;
        
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            match self.deserialize_entry(&line) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    warn!("Failed to deserialize journal entry: {} ({})", e, line);
                    // Continue reading other entries
                }
            }
        }
        
        Ok(entries)
    }

    /// Replay messages from journal files
    async fn replay_from_files(&self, from_sequence: SequenceNumber) -> Result<Vec<JournalEntry>> {
        let entries = match std::fs::read_dir(&self.journal_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(Vec::new()),
        };
        
        let mut journal_files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && path.extension().map_or(false, |ext| ext == "journal") {
                journal_files.push(path);
            }
        }
        
        // Sort files by name
        journal_files.sort();
        
        let mut result = Vec::new();
        
        for file_path in journal_files {
            let file_entries = self.read_journal_file(&file_path).await?;
            
            for entry in file_entries {
                if entry.sequence >= from_sequence {
                    result.push(entry);
                }
            }
        }
        
        Ok(result)
    }

    /// Serialize a journal entry
    fn serialize_entry(&self, entry: &JournalEntry) -> Result<Vec<u8>> {
        if self.config.enable_compression {
            // For now, just use JSON - could add compression later
            Ok(serde_json::to_vec(entry)?)
        } else {
            Ok(serde_json::to_vec(entry)?)
        }
    }

    /// Deserialize a journal entry
    fn deserialize_entry(&self, data: &str) -> Result<JournalEntry> {
        Ok(serde_json::from_str(data)?)
    }
}

/// State checkpoint for quick SessionActor recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateCheckpoint {
    pub sequence: SequenceNumber,
    pub timestamp: u64,
    pub client_transactions: HashMap<Transaction, Vec<ClientId>>, // Simplified for serialization
    pub client_request_ids: HashMap<String, RequestId>, // Simplified key format: "tx:client_id"
    pub active_clients: Vec<ClientId>,
}

impl StateCheckpoint {
    /// Create a new checkpoint from SessionActor state
    pub fn new(
        sequence: SequenceNumber,
        client_transactions: &HashMap<Transaction, std::collections::HashSet<ClientId>>,
        client_request_ids: &HashMap<(Transaction, ClientId), RequestId>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        // Convert data structures to serializable formats
        let client_transactions: HashMap<Transaction, Vec<ClientId>> = client_transactions
            .iter()
            .map(|(tx, clients)| (*tx, clients.iter().copied().collect()))
            .collect();
        
        let client_request_ids: HashMap<String, RequestId> = client_request_ids
            .iter()
            .map(|((tx, client_id), request_id)| {
                (format!("{}:{}", tx, client_id), *request_id)
            })
            .collect();
        
        let active_clients: Vec<ClientId> = client_transactions
            .values()
            .flatten()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        
        Self {
            sequence,
            timestamp,
            client_transactions,
            client_request_ids,
            active_clients,
        }
    }
}

/// Checkpoint manager for creating and restoring state snapshots
pub struct CheckpointManager {
    checkpoint_dir: PathBuf,
    max_checkpoints: usize,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(checkpoint_dir: PathBuf, max_checkpoints: usize) -> Result<Self> {
        std::fs::create_dir_all(&checkpoint_dir)
            .with_context(|| format!("Failed to create checkpoint directory: {:?}", checkpoint_dir))?;
        
        Ok(Self {
            checkpoint_dir,
            max_checkpoints,
        })
    }

    /// Create a new checkpoint
    pub async fn create_checkpoint(&self, checkpoint: StateCheckpoint) -> Result<PathBuf> {
        let filename = format!("checkpoint-{}-{}.json", checkpoint.sequence.value(), checkpoint.timestamp);
        let file_path = self.checkpoint_dir.join(filename);
        
        let serialized = serde_json::to_vec_pretty(&checkpoint)
            .context("Failed to serialize checkpoint")?;
        
        tokio::fs::write(&file_path, serialized).await
            .with_context(|| format!("Failed to write checkpoint: {:?}", file_path))?;
        
        info!("Created checkpoint: {:?} (sequence: {})", file_path, checkpoint.sequence);
        
        // Clean up old checkpoints
        self.cleanup_old_checkpoints().await?;
        
        Ok(file_path)
    }

    /// Load the most recent checkpoint
    pub async fn load_latest_checkpoint(&self) -> Result<Option<StateCheckpoint>> {
        let entries = match std::fs::read_dir(&self.checkpoint_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(None),
        };
        
        let mut checkpoint_files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() && 
               path.extension().map_or(false, |ext| ext == "json") &&
               path.file_name()
                   .and_then(|name| name.to_str())
                   .map_or(false, |name| name.starts_with("checkpoint-")) {
                checkpoint_files.push(path);
            }
        }
        
        if checkpoint_files.is_empty() {
            return Ok(None);
        }
        
        // Sort by filename (which includes sequence number and timestamp)
        checkpoint_files.sort();
        let latest_file = checkpoint_files.last().unwrap();
        
        let data = tokio::fs::read(latest_file).await
            .with_context(|| format!("Failed to read checkpoint: {:?}", latest_file))?;
        
        let checkpoint: StateCheckpoint = serde_json::from_slice(&data)
            .with_context(|| format!("Failed to deserialize checkpoint: {:?}", latest_file))?;
        
        info!("Loaded checkpoint: {:?} (sequence: {})", latest_file, checkpoint.sequence);
        Ok(Some(checkpoint))
    }

    /// Clean up old checkpoints
    async fn cleanup_old_checkpoints(&self) -> Result<()> {
        let entries = match std::fs::read_dir(&self.checkpoint_dir) {
            Ok(entries) => entries,
            Err(_) => return Ok(()),
        };
        
        let mut checkpoint_files = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() &&
               path.extension().map_or(false, |ext| ext == "json") &&
               path.file_name()
                   .and_then(|name| name.to_str())
                   .map_or(false, |name| name.starts_with("checkpoint-")) {
                checkpoint_files.push((
                    path,
                    entry.metadata()?.modified().unwrap_or(SystemTime::UNIX_EPOCH)
                ));
            }
        }
        
        // Sort by modification time (oldest first)
        checkpoint_files.sort_by_key(|(_, time)| *time);
        
        // Remove old checkpoints if we exceed max_checkpoints
        if checkpoint_files.len() > self.max_checkpoints {
            let files_to_remove = checkpoint_files.len() - self.max_checkpoints;
            
            for (file_path, _) in checkpoint_files.iter().take(files_to_remove) {
                match std::fs::remove_file(file_path) {
                    Ok(()) => info!("Removed old checkpoint: {:?}", file_path),
                    Err(e) => warn!("Failed to remove checkpoint {:?}: {}", file_path, e),
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::client_events::HostResult;

    fn create_test_config() -> (JournalConfig, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = JournalConfig {
            journal_dir: temp_dir.path().to_path_buf(),
            max_file_size: 1024, // Small size for testing
            max_files: 3,
            flush_interval: Duration::from_millis(1),
            enable_compression: false,
            max_replay_entries: 100,
        };
        (config, temp_dir)
    }

    #[tokio::test]
    async fn test_message_journal_basic_operations() {
        let (config, _temp_dir) = create_test_config();
        let journal = MessageJournal::new(config).await.unwrap();
        
        use crate::operations::put::PutMsg;
        
        // Test appending messages  
        let msg1 = SessionMessage::RegisterTransaction {
            tx: Transaction::new::<PutMsg>(),
            client_id: ClientId::FIRST,
            request_id: RequestId::new(),
        };
        
        let seq1 = journal.append(msg1.clone()).await.unwrap();
        assert_eq!(seq1.value(), 1);
        
        let msg2 = SessionMessage::ClientDisconnect {
            client_id: ClientId::FIRST,
        };
        
        let seq2 = journal.append(msg2.clone()).await.unwrap();
        assert_eq!(seq2.value(), 2);
        
        // Test replay
        let entries = journal.replay_from(SequenceNumber::new(1)).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, seq1);
        assert_eq!(entries[1].sequence, seq2);
    }

    #[tokio::test]
    async fn test_checkpoint_creation_and_loading() {
        let temp_dir = TempDir::new().unwrap();
        let checkpoint_manager = CheckpointManager::new(temp_dir.path().to_path_buf(), 5).unwrap();
        
        // Create test data
        let mut client_transactions = HashMap::new();
        let mut clients = std::collections::HashSet::new();
        clients.insert(ClientId::FIRST);
        let tx = Transaction::new::<crate::operations::put::PutMsg>();
        client_transactions.insert(tx, clients);
        
        let mut client_request_ids = HashMap::new();
        client_request_ids.insert((tx, ClientId::FIRST), RequestId::new());
        
        let checkpoint = StateCheckpoint::new(
            SequenceNumber::new(10),
            &client_transactions,
            &client_request_ids,
        );
        
        // Create checkpoint
        let checkpoint_path = checkpoint_manager.create_checkpoint(checkpoint.clone()).await.unwrap();
        assert!(checkpoint_path.exists());
        
        // Load checkpoint
        let loaded = checkpoint_manager.load_latest_checkpoint().await.unwrap();
        assert!(loaded.is_some());
        
        let loaded = loaded.unwrap();
        assert_eq!(loaded.sequence, checkpoint.sequence);
        assert_eq!(loaded.client_transactions.len(), 1);
        assert_eq!(loaded.active_clients.len(), 1);
    }

    #[tokio::test]
    async fn test_journal_persistence() {
        let (config, temp_dir) = create_test_config();
        
        // Create journal and add some messages
        {
            let journal = MessageJournal::new(config.clone()).await.unwrap();
            
            let msg = SessionMessage::RegisterTransaction {
                tx: Transaction::new::<crate::operations::put::PutMsg>(),
                client_id: ClientId::FIRST,
                request_id: RequestId::new(),
            };
            
            journal.append(msg).await.unwrap();
            journal.flush().await.unwrap();
        }
        
        // Create new journal instance and verify messages were persisted
        {
            let journal = MessageJournal::new(config).await.unwrap();
            assert_eq!(journal.current_sequence().value(), 1);
            
            let entries = journal.replay_from(SequenceNumber::new(1)).await.unwrap();
            assert_eq!(entries.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_journal_file_rotation() {
        let (mut config, _temp_dir) = create_test_config();
        config.max_file_size = 100; // Very small size to trigger rotation
        
        let journal = MessageJournal::new(config).await.unwrap();
        
        // Add many messages to trigger rotation
        for _ in 0..20 {
            let msg = SessionMessage::RegisterTransaction {
                tx: Transaction::new::<crate::operations::put::PutMsg>(),
                client_id: ClientId::FIRST,
                request_id: RequestId::new(),
            };
            
            journal.append(msg).await.unwrap();
        }
        
        journal.flush().await.unwrap();
        
        // Verify we can still replay all messages
        let entries = journal.replay_from(SequenceNumber::new(1)).await.unwrap();
        assert_eq!(entries.len(), 20);
    }
}