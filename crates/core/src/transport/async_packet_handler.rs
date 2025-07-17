#![allow(dead_code)] // Allow dead code for development and testing infrastructure

use crate::transport::crypto::TransportSecretKey;
use crate::transport::packet_data::{PacketData, UnknownEncryption};
use crate::transport::peer_connection::StreamId;
use crate::transport::TransportError;
use aes_gcm::Aes128Gcm;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Unique identifier for packet handlers
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct HandlerId(pub u64);

/// Result of processing a packet
#[derive(Debug)]
pub enum ProcessResult {
    /// Keep-alive packet processed
    KeepAlive,
    /// Regular message processed
    Message(Vec<u8>),
    /// Stream fragment processed (may contribute to stream completion)
    Fragment {
        stream_id: StreamId,
        fragment_data: Vec<u8>,
    },
    /// Complete stream assembled and processed
    #[allow(dead_code)]
    StreamCompleted(Vec<u8>),
    /// Packet was malformed or couldn't be processed
    ProcessingFailed(String),
}

/// Tracks a spawned packet handler
#[derive(Debug)]
pub struct PacketHandler {
    pub id: HandlerId,
    pub started_at: Instant,
    pub handle: JoinHandle<Result<ProcessResult, TransportError>>,
}

/// Statistics for monitoring packet processing performance
#[derive(Debug, Default)]
pub struct PacketProcessingStats {
    pub total_packets_processed: u64,
    pub packets_dropped_flood: u64,
    pub slow_processing_count: u64,
    pub very_slow_processing_count: u64,
    pub processing_failures: u64,
    pub keepalive_packets: u64,
    pub message_packets: u64,
    pub fragment_packets: u64,
    pub stream_completions: u64,
}

/// Comprehensive monitoring statistics for handler performance
#[derive(Debug)]
pub struct MonitoringStats {
    pub active_handlers: usize,
    pub oldest_handler_age: Option<std::time::Duration>,
    pub newest_handler_age: Option<std::time::Duration>,
    pub flood_threshold: usize,
    pub flood_protection_active: bool,
    pub total_packets_processed: u64,
    pub packets_dropped_flood: u64,
    pub slow_processing_count: u64,
    pub very_slow_processing_count: u64,
    pub processing_failures: u64,
}

/// Manages spawned packet handlers with flood protection and monitoring
pub struct PacketHandlerManager {
    active_handlers: HashMap<HandlerId, PacketHandler>,
    next_handler_id: AtomicU64,
    stats: PacketProcessingStats,
    flood_threshold: usize,
}

impl PacketHandlerManager {
    pub fn new(flood_threshold: usize) -> Self {
        Self {
            active_handlers: HashMap::new(),
            next_handler_id: AtomicU64::new(1),
            stats: PacketProcessingStats::default(),
            flood_threshold,
        }
    }

    /// Generate next unique handler ID
    pub fn next_handler_id(&self) -> HandlerId {
        HandlerId(self.next_handler_id.fetch_add(1, Ordering::SeqCst))
    }

    /// Check if we should apply flood protection
    pub fn should_apply_flood_protection(&self) -> bool {
        self.active_handlers.len() >= self.flood_threshold
    }

    /// Add a new packet handler to tracking
    pub fn add_handler(&mut self, handler: PacketHandler) {
        info!(
            handler_id = ?handler.id,
            active_count = self.active_handlers.len(),
            "Adding packet handler"
        );
        self.active_handlers.insert(handler.id, handler);
        self.stats.total_packets_processed += 1;
    }

    /// Get current number of active handlers
    pub fn active_handler_count(&self) -> usize {
        self.active_handlers.len()
    }

    /// Get reference to current statistics
    pub fn stats(&self) -> &PacketProcessingStats {
        &self.stats
    }

    /// Clean up completed handlers and collect their results
    pub async fn cleanup_completed_handlers(
        &mut self,
    ) -> Vec<(HandlerId, Result<ProcessResult, TransportError>)> {
        let mut completed = Vec::new();
        let mut to_remove = Vec::new();

        for (id, handler) in &self.active_handlers {
            if handler.handle.is_finished() {
                to_remove.push(*id);
            }
        }

        for id in to_remove {
            if let Some(handler) = self.active_handlers.remove(&id) {
                match handler.handle.await {
                    Ok(result) => {
                        self.update_stats_for_result(&result);
                        completed.push((id, result));
                    }
                    Err(join_error) => {
                        error!(
                            handler_id = ?id,
                            error = ?join_error,
                            "Handler join failed"
                        );
                        self.stats.processing_failures += 1;
                    }
                }
            }
        }

        if !completed.is_empty() {
            info!(
                completed_count = completed.len(),
                active_count = self.active_handlers.len(),
                "Cleaned up completed handlers"
            );
        }

        completed
    }

    /// Check for slow handlers and log warnings
    pub fn check_slow_handlers(&mut self) {
        const SLOW_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(100);
        const VERY_SLOW_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(1000);
        const CRITICAL_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(5000);

        let now = Instant::now();
        let mut handlers_to_warn = Vec::new();

        for handler in self.active_handlers.values() {
            let processing_time = now.duration_since(handler.started_at);

            if processing_time > CRITICAL_THRESHOLD {
                handlers_to_warn.push((handler.id, processing_time, "CRITICAL"));
            } else if processing_time > VERY_SLOW_THRESHOLD {
                handlers_to_warn.push((handler.id, processing_time, "VERY_SLOW"));
            } else if processing_time > SLOW_THRESHOLD {
                handlers_to_warn.push((handler.id, processing_time, "SLOW"));
            }
        }

        // Log warnings and update stats
        for (handler_id, processing_time, severity) in handlers_to_warn {
            match severity {
                "CRITICAL" => {
                    error!(
                        handler_id = ?handler_id,
                        processing_time_ms = processing_time.as_millis(),
                        active_handlers = self.active_handlers.len(),
                        "CRITICAL: Handler taking excessively long - possible deadlock"
                    );
                    self.stats.very_slow_processing_count += 1;
                }
                "VERY_SLOW" => {
                    error!(
                        handler_id = ?handler_id,
                        processing_time_ms = processing_time.as_millis(),
                        "Very slow packet processing detected"
                    );
                    self.stats.very_slow_processing_count += 1;
                }
                "SLOW" => {
                    warn!(
                        handler_id = ?handler_id,
                        processing_time_ms = processing_time.as_millis(),
                        "Slow packet processing detected"
                    );
                    self.stats.slow_processing_count += 1;
                }
                _ => {}
            }
        }
    }

    /// Get detailed monitoring statistics
    pub fn get_monitoring_stats(&self) -> MonitoringStats {
        let now = Instant::now();
        let mut handler_ages = Vec::new();
        let mut oldest_handler_age = None;
        let mut newest_handler_age = None;

        for handler in self.active_handlers.values() {
            let age = now.duration_since(handler.started_at);
            handler_ages.push(age);

            if oldest_handler_age.is_none() || age > oldest_handler_age.unwrap() {
                oldest_handler_age = Some(age);
            }
            if newest_handler_age.is_none() || age < newest_handler_age.unwrap() {
                newest_handler_age = Some(age);
            }
        }

        MonitoringStats {
            active_handlers: self.active_handlers.len(),
            oldest_handler_age,
            newest_handler_age,
            flood_threshold: self.flood_threshold,
            flood_protection_active: self.should_apply_flood_protection(),
            total_packets_processed: self.stats.total_packets_processed,
            packets_dropped_flood: self.stats.packets_dropped_flood,
            slow_processing_count: self.stats.slow_processing_count,
            very_slow_processing_count: self.stats.very_slow_processing_count,
            processing_failures: self.stats.processing_failures,
        }
    }

    /// Check if the handler manager is in a healthy state
    pub fn is_healthy(&self) -> bool {
        let now = Instant::now();
        const MAX_CRITICAL_HANDLERS: usize = 3;
        const CRITICAL_THRESHOLD: std::time::Duration = std::time::Duration::from_millis(5000);

        let critical_handlers = self
            .active_handlers
            .values()
            .filter(|handler| now.duration_since(handler.started_at) > CRITICAL_THRESHOLD)
            .count();

        critical_handlers < MAX_CRITICAL_HANDLERS
    }

    /// Update statistics based on processing result
    fn update_stats_for_result(&mut self, result: &Result<ProcessResult, TransportError>) {
        match result {
            Ok(ProcessResult::KeepAlive) => {
                self.stats.keepalive_packets += 1;
            }
            Ok(ProcessResult::Message(_)) => {
                self.stats.message_packets += 1;
            }
            Ok(ProcessResult::Fragment { .. }) => {
                self.stats.fragment_packets += 1;
            }
            Ok(ProcessResult::StreamCompleted(_)) => {
                self.stats.stream_completions += 1;
            }
            Ok(ProcessResult::ProcessingFailed(_)) => {
                self.stats.processing_failures += 1;
            }
            Err(_) => {
                self.stats.processing_failures += 1;
            }
        }
    }

    /// Record a packet dropped due to flood protection
    pub fn record_flood_drop(&mut self) {
        self.stats.packets_dropped_flood += 1;
        warn!(
            active_handlers = self.active_handlers.len(),
            flood_threshold = self.flood_threshold,
            total_dropped = self.stats.packets_dropped_flood,
            "Packet dropped due to flood protection"
        );
    }

    /// Spawn a new packet handler for processing
    #[allow(clippy::too_many_arguments)]
    pub async fn spawn_packet_handler(
        &mut self,
        packet_data: PacketData<UnknownEncryption>,
        inbound_symmetric_key: &Aes128Gcm,
        inbound_symmetric_key_bytes: [u8; 16],
        transport_secret_key: &TransportSecretKey,
        outbound_symmetric_key: &Aes128Gcm,
        outbound_packets: &mpsc::Sender<(SocketAddr, Arc<[u8]>)>,
        remote_addr: SocketAddr,
    ) -> Result<HandlerId, TransportError> {
        // Check for flood protection
        if self.should_apply_flood_protection() {
            self.stats.packets_dropped_flood += 1;
            return Err(TransportError::FloodProtection);
        }

        let handler_id = self.next_handler_id();

        let inbound_symmetric_key = inbound_symmetric_key.clone();
        let transport_secret_key = transport_secret_key.clone();
        let outbound_symmetric_key = outbound_symmetric_key.clone();
        let outbound_packets = outbound_packets.clone();

        let handle = tokio::spawn(async move {
            process_packet_fully(
                packet_data,
                &inbound_symmetric_key,
                inbound_symmetric_key_bytes,
                &transport_secret_key,
                &outbound_symmetric_key,
                &outbound_packets,
                remote_addr,
            )
            .await
        });

        let handler = PacketHandler {
            id: handler_id,
            handle,
            started_at: Instant::now(),
        };

        self.add_handler(handler);
        info!(
            handler_id = ?handler_id,
            active_count = self.active_handlers.len(),
            "Spawned packet handler"
        );

        Ok(handler_id)
    }

    /// Check for completed handlers and return the first one found
    pub async fn next_completed_handler(
        &mut self,
    ) -> Option<(HandlerId, Result<ProcessResult, TransportError>)> {
        // Check each active handler to see if it's completed
        let mut completed_handler = None;

        for (handler_id, handler) in &mut self.active_handlers {
            if handler.handle.is_finished() {
                completed_handler = Some(*handler_id);
                break;
            }
        }

        if let Some(handler_id) = completed_handler {
            if let Some(handler) = self.active_handlers.remove(&handler_id) {
                match handler.handle.await {
                    Ok(result) => {
                        self.stats.total_packets_processed += 1;
                        Some((handler_id, result))
                    }
                    Err(join_error) => {
                        self.stats.processing_failures += 1;
                        Some((handler_id, Err(TransportError::Other(join_error.into()))))
                    }
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Process a packet fully through decryption, deserialization, and message handling
pub async fn process_packet_fully(
    packet_data: crate::transport::packet_data::PacketData<
        crate::transport::packet_data::UnknownEncryption,
    >,
    inbound_symmetric_key: &aes_gcm::Aes128Gcm,
    inbound_symmetric_key_bytes: [u8; 16],
    transport_secret_key: &crate::transport::crypto::TransportSecretKey,
    outbound_symmetric_key: &aes_gcm::Aes128Gcm,
    outbound_packets: &tokio::sync::mpsc::Sender<(std::net::SocketAddr, std::sync::Arc<[u8]>)>,
    remote_addr: std::net::SocketAddr,
) -> Result<ProcessResult, TransportError> {
    use crate::transport::symmetric_message::{SymmetricMessage, SymmetricMessagePayload};

    // Try symmetric decryption first
    let decrypted = match packet_data.try_decrypt_sym(inbound_symmetric_key) {
        Ok(decrypted) => {
            tracing::trace!(
                target: "freenet_core::transport::async_packet_handler::decrypt",
                remote_addr = %remote_addr,
                packet_size = packet_data.size,
                "DECRYPT_SUCCESS: Successfully decrypted packet with symmetric key"
            );
            decrypted
        }
        Err(decrypt_error) => {
            // Check if this is a 256-byte RSA intro packet
            if packet_data.data().len() == 256 {
                tracing::debug!(
                    target: "freenet_core::transport::async_packet_handler::decrypt",
                    remote_addr = %remote_addr,
                    packet_size = packet_data.size,
                    "DECRYPT_RETRY: Symmetric decryption failed, trying RSA for intro packet"
                );
                match transport_secret_key.decrypt(packet_data.data()) {
                    Ok(_decrypted_intro) => {
                        tracing::info!(
                            target: "freenet_core::transport::async_packet_handler::intro",
                            remote_addr = %remote_addr,
                            "INTRO_PACKET: Successfully decrypted RSA intro packet, sending ACK"
                        );
                        // Send ACK response for intro packet
                        let ack_packet = SymmetricMessage::ack_ok(
                            outbound_symmetric_key,
                            inbound_symmetric_key_bytes,
                            remote_addr,
                        )?;
                        let _ = outbound_packets
                            .send((remote_addr, ack_packet.data().into()))
                            .await;
                        return Ok(ProcessResult::ProcessingFailed(
                            "RSA intro packet processed".to_string(),
                        ));
                    }
                    Err(rsa_error) => {
                        tracing::debug!(
                            target: "freenet_core::transport::async_packet_handler::decrypt",
                            remote_addr = %remote_addr,
                            packet_size = packet_data.size,
                            symmetric_error = ?decrypt_error,
                            rsa_error = ?rsa_error,
                            "DECRYPT_FAILED: Both symmetric and RSA decryption failed"
                        );
                        return Ok(ProcessResult::ProcessingFailed(
                            "Failed to decrypt packet".to_string(),
                        ));
                    }
                }
            } else {
                tracing::debug!(
                    target: "freenet_core::transport::async_packet_handler::decrypt",
                    remote_addr = %remote_addr,
                    packet_size = packet_data.size,
                    error = ?decrypt_error,
                    "DECRYPT_FAILED: Symmetric decryption failed for non-intro packet"
                );
                return Ok(ProcessResult::ProcessingFailed(
                    "Failed to decrypt packet".to_string(),
                ));
            }
        }
    };

    // Deserialize the message
    let msg = match SymmetricMessage::deser(decrypted.data()) {
        Ok(msg) => {
            tracing::trace!(
                target: "freenet_core::transport::async_packet_handler::deserialize",
                remote_addr = %remote_addr,
                packet_id = msg.packet_id,
                receipts_count = msg.confirm_receipt.len(),
                "DESERIALIZE_SUCCESS: Successfully deserialized symmetric message"
            );
            msg
        }
        Err(deser_error) => {
            tracing::debug!(
                target: "freenet_core::transport::async_packet_handler::deserialize",
                remote_addr = %remote_addr,
                data_len = decrypted.data().len(),
                error = ?deser_error,
                "DESERIALIZE_FAILED: Failed to deserialize symmetric message"
            );
            return Ok(ProcessResult::ProcessingFailed(
                "Failed to deserialize message".to_string(),
            ));
        }
    };

    let SymmetricMessage { payload, .. } = msg;

    // Process the payload
    match payload {
        SymmetricMessagePayload::NoOp => Ok(ProcessResult::KeepAlive),
        SymmetricMessagePayload::ShortMessage { payload } => Ok(ProcessResult::Message(payload)),
        SymmetricMessagePayload::StreamFragment {
            stream_id, payload, ..
        } => Ok(ProcessResult::Fragment {
            stream_id,
            fragment_data: payload,
        }),
        SymmetricMessagePayload::AckConnection { result: Ok(_) } => {
            // Send ACK response
            let ack_packet = SymmetricMessage::ack_ok(
                outbound_symmetric_key,
                inbound_symmetric_key_bytes,
                remote_addr,
            )?;
            let _ = outbound_packets
                .send((remote_addr, ack_packet.data().into()))
                .await;
            Ok(ProcessResult::ProcessingFailed(
                "AckConnection processed".to_string(),
            ))
        }
        SymmetricMessagePayload::AckConnection { result: Err(cause) } => Ok(
            ProcessResult::ProcessingFailed(format!("Connection establishment failure: {cause}")),
        ),
    }
}

/// Ultra-minimal main loop that spawns packet handlers before decryption
/// This prevents keep-alive packet starvation by ensuring the main loop never blocks
#[allow(clippy::too_many_arguments)]
pub async fn minimal_packet_processing_loop(
    mut inbound_packet_recv: tokio::sync::mpsc::Receiver<
        crate::transport::packet_data::PacketData<crate::transport::packet_data::UnknownEncryption>,
    >,
    inbound_symmetric_key: aes_gcm::Aes128Gcm,
    inbound_symmetric_key_bytes: [u8; 16],
    transport_secret_key: crate::transport::crypto::TransportSecretKey,
    outbound_symmetric_key: aes_gcm::Aes128Gcm,
    outbound_packets: tokio::sync::mpsc::Sender<(std::net::SocketAddr, std::sync::Arc<[u8]>)>,
    remote_addr: std::net::SocketAddr,
    flood_threshold: usize,
) -> Result<(), TransportError> {
    let mut handler_manager = PacketHandlerManager::new(flood_threshold);

    // Main loop that never blocks
    loop {
        tokio::select! {
            // Handle incoming packets by spawning handlers immediately
            inbound_packet = inbound_packet_recv.recv() => {
                let packet_data = match inbound_packet {
                    Some(packet) => packet,
                    None => {
                        tracing::info!("Inbound packet channel closed, exiting loop");
                        break;
                    }
                };

                // Apply flood protection
                if handler_manager.should_apply_flood_protection() {
                    handler_manager.record_flood_drop();
                    tracing::warn!(
                        target: "freenet_core::transport::async_packet_handler::flood_protection",
                        active_handlers = handler_manager.active_handler_count(),
                        flood_threshold = flood_threshold,
                        remote_addr = %remote_addr,
                        packet_size = packet_data.size,
                        "FLOOD_PROTECTION: Dropping packet due to flood protection"
                    );
                    continue;
                }

                // Spawn handler BEFORE decryption to prevent blocking
                let handler_id = handler_manager.next_handler_id();
                let inbound_key = inbound_symmetric_key.clone();
                let transport_key = transport_secret_key.clone();
                let outbound_key = outbound_symmetric_key.clone();
                let outbound_sender = outbound_packets.clone();

                tracing::debug!(
                    target: "freenet_core::transport::async_packet_handler::spawn",
                    handler_id = ?handler_id,
                    remote_addr = %remote_addr,
                    packet_size = packet_data.size,
                    active_handlers = handler_manager.active_handler_count(),
                    "SPAWN_HANDLER: Spawning packet handler before decryption"
                );

                let handle = tokio::spawn(async move {
                    let start_time = std::time::Instant::now();
                    let result = process_packet_fully(
                        packet_data,
                        &inbound_key,
                        inbound_symmetric_key_bytes,
                        &transport_key,
                        &outbound_key,
                        &outbound_sender,
                        remote_addr,
                    ).await;

                    let processing_time = start_time.elapsed();
                    tracing::debug!(
                        target: "freenet_core::transport::async_packet_handler::process_complete",
                        handler_id = ?handler_id,
                        processing_time_ms = processing_time.as_millis(),
                        result = ?result,
                        "PROCESS_COMPLETE: Handler finished processing"
                    );

                    result
                });

                let packet_handler = PacketHandler {
                    id: handler_id,
                    started_at: std::time::Instant::now(),
                    handle,
                };

                handler_manager.add_handler(packet_handler);
            }

            // Clean up completed handlers and check health every 10ms
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(10)) => {
                let completed_handlers = handler_manager.cleanup_completed_handlers().await;

                // Process completed results
                for (handler_id, result) in completed_handlers {
                    match result {
                        Ok(ProcessResult::KeepAlive) => {
                            tracing::trace!(
                                target: "freenet_core::transport::async_packet_handler::keepalive",
                                handler_id = ?handler_id,
                                remote_addr = %remote_addr,
                                "KEEPALIVE_PROCESSED: Keep-alive packet processed"
                            );
                        }
                        Ok(ProcessResult::Message(payload)) => {
                            tracing::info!(
                                target: "freenet_core::transport::async_packet_handler::message",
                                handler_id = ?handler_id,
                                payload_len = payload.len(),
                                remote_addr = %remote_addr,
                                "MESSAGE_PROCESSED: Message packet processed"
                            );
                            // TODO: Forward message to higher-level processor
                        }
                        Ok(ProcessResult::Fragment { stream_id, fragment_data }) => {
                            tracing::debug!(
                                target: "freenet_core::transport::async_packet_handler::fragment",
                                handler_id = ?handler_id,
                                stream_id = ?stream_id,
                                fragment_len = fragment_data.len(),
                                remote_addr = %remote_addr,
                                "FRAGMENT_PROCESSED: Stream fragment processed"
                            );
                            // TODO: Forward fragment to stream reassembly
                        }
                        Ok(ProcessResult::StreamCompleted(payload)) => {
                            tracing::info!(
                                target: "freenet_core::transport::async_packet_handler::stream_complete",
                                handler_id = ?handler_id,
                                payload_len = payload.len(),
                                remote_addr = %remote_addr,
                                "STREAM_COMPLETED: Stream completed"
                            );
                            // TODO: Forward completed stream to higher-level processor
                        }
                        Ok(ProcessResult::ProcessingFailed(reason)) => {
                            tracing::debug!(
                                target: "freenet_core::transport::async_packet_handler::processing_failed",
                                handler_id = ?handler_id,
                                reason = %reason,
                                remote_addr = %remote_addr,
                                "PROCESSING_FAILED: Packet processing failed"
                            );
                        }
                        Err(transport_error) => {
                            tracing::error!(
                                target: "freenet_core::transport::async_packet_handler::transport_error",
                                handler_id = ?handler_id,
                                error = ?transport_error,
                                remote_addr = %remote_addr,
                                "TRANSPORT_ERROR: Handler transport error"
                            );
                        }
                    }
                }

                // Check for slow handlers periodically
                handler_manager.check_slow_handlers();

                // Check overall health and log warnings if needed
                if !handler_manager.is_healthy() {
                    let stats = handler_manager.get_monitoring_stats();
                    tracing::warn!(
                        target: "freenet_core::transport::async_packet_handler::health",
                        active_handlers = stats.active_handlers,
                        oldest_handler_age_ms = stats.oldest_handler_age.map(|d| d.as_millis()),
                        flood_protection_active = stats.flood_protection_active,
                        remote_addr = %remote_addr,
                        "HEALTH_WARNING: Handler manager in unhealthy state"
                    );
                }
            }

            // Periodic health monitoring every 5 seconds
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                let stats = handler_manager.get_monitoring_stats();
                tracing::info!(
                    target: "freenet_core::transport::async_packet_handler::stats",
                    active_handlers = stats.active_handlers,
                    total_processed = stats.total_packets_processed,
                    flood_drops = stats.packets_dropped_flood,
                    slow_handlers = stats.slow_processing_count,
                    very_slow_handlers = stats.very_slow_processing_count,
                    failures = stats.processing_failures,
                    oldest_handler_age_ms = stats.oldest_handler_age.map(|d| d.as_millis()),
                    remote_addr = %remote_addr,
                    flood_threshold = flood_threshold,
                    "STATS: Packet handler periodic statistics"
                );
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_handler_id_generation() {
        let manager = PacketHandlerManager::new(100);
        let id1 = manager.next_handler_id();
        let id2 = manager.next_handler_id();
        assert_ne!(id1, id2);
        assert_eq!(id1.0, 1);
        assert_eq!(id2.0, 2);
    }

    #[tokio::test]
    async fn test_flood_protection_threshold() {
        let manager = PacketHandlerManager::new(2);
        assert!(!manager.should_apply_flood_protection());

        // Add handlers up to threshold
        let mut manager = manager;
        for i in 0..2 {
            let handle = tokio::spawn(async move {
                sleep(Duration::from_millis(100)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i),
                started_at: Instant::now(),
                handle,
            });
        }

        assert!(manager.should_apply_flood_protection());
    }

    #[tokio::test]
    async fn test_handler_cleanup() {
        let mut manager = PacketHandlerManager::new(100);

        // Add a quick handler
        let handle = tokio::spawn(async { Ok(ProcessResult::KeepAlive) });
        manager.add_handler(PacketHandler {
            id: HandlerId(1),
            started_at: Instant::now(),
            handle,
        });

        // Give it time to complete
        sleep(Duration::from_millis(10)).await;

        let completed = manager.cleanup_completed_handlers().await;
        assert_eq!(completed.len(), 1);
        assert_eq!(manager.active_handler_count(), 0);
        assert_eq!(manager.stats().keepalive_packets, 1);
    }

    #[tokio::test]
    async fn test_async_packet_processing() {
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::packet_data::PacketData;
        use crate::transport::symmetric_message::{SymmetricMessage, SymmetricMessagePayload};
        use aes_gcm::KeyInit;
        use std::net::SocketAddr;

        // Create test keys
        let keypair = TransportKeypair::new();
        let inbound_key = aes_gcm::Aes128Gcm::new(&[42u8; 16].into());
        let outbound_key = aes_gcm::Aes128Gcm::new(&[43u8; 16].into());

        // Create test packet (keep-alive) - simulate incoming packet
        let symmetric_packet = SymmetricMessage::serialize_msg_to_packet_data(
            1,
            SymmetricMessagePayload::NoOp,
            &inbound_key,
            vec![],
        )
        .unwrap();

        // Convert to UnknownEncryption packet to simulate incoming data
        let test_packet = PacketData::from_buf(symmetric_packet.data());

        let test_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        // Process the packet
        let result = process_packet_fully(
            test_packet,
            &inbound_key,
            [42u8; 16],
            &keypair.secret,
            &outbound_key,
            &tx,
            test_addr,
        )
        .await;

        // Verify keep-alive was processed correctly
        assert!(result.is_ok());
        matches!(result.unwrap(), ProcessResult::KeepAlive);
    }

    #[tokio::test]
    async fn test_flood_protection() {
        let mut manager = PacketHandlerManager::new(2);

        // Add handlers up to threshold
        for i in 0..2 {
            let handle = tokio::spawn(async move {
                sleep(Duration::from_millis(100)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i),
                started_at: Instant::now(),
                handle,
            });
        }

        // Should now trigger flood protection
        assert!(manager.should_apply_flood_protection());

        // Test flood drop recording
        manager.record_flood_drop();
        assert_eq!(manager.stats().packets_dropped_flood, 1);
    }

    #[tokio::test]
    async fn test_monitoring_stats() {
        let mut manager = PacketHandlerManager::new(100);

        // Add some handlers with different processing times
        for i in 0..3 {
            let delay = (i + 1) * 50; // 50ms, 100ms, 150ms delays
            let handle = tokio::spawn(async move {
                sleep(Duration::from_millis(delay)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i),
                started_at: Instant::now(),
                handle,
            });
        }

        // Give handlers time to start
        sleep(Duration::from_millis(25)).await;

        let stats = manager.get_monitoring_stats();
        assert_eq!(stats.active_handlers, 3);
        assert_eq!(stats.flood_threshold, 100);
        assert!(!stats.flood_protection_active);
        assert!(stats.oldest_handler_age.is_some());
        assert!(stats.newest_handler_age.is_some());

        // Check health
        assert!(manager.is_healthy());
    }

    #[tokio::test]
    async fn test_slow_handler_detection() {
        let mut manager = PacketHandlerManager::new(100);

        // Add a slow handler
        let handle = tokio::spawn(async {
            sleep(Duration::from_millis(200)).await; // Above SLOW_THRESHOLD
            Ok(ProcessResult::KeepAlive)
        });
        manager.add_handler(PacketHandler {
            id: HandlerId(1),
            started_at: Instant::now(),
            handle,
        });

        // Wait for handler to become slow
        sleep(Duration::from_millis(150)).await;

        // Check for slow handlers
        manager.check_slow_handlers();

        // Should detect one slow handler
        assert_eq!(manager.stats().slow_processing_count, 1);
    }

    #[tokio::test]
    async fn test_health_check() {
        let mut manager = PacketHandlerManager::new(100);

        // Add many fast handlers - should be healthy
        for i in 0..5 {
            let handle = tokio::spawn(async move {
                sleep(Duration::from_millis(10)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i),
                started_at: Instant::now(),
                handle,
            });
        }

        assert!(manager.is_healthy());

        // Add critical handlers - should become unhealthy
        for i in 5..8 {
            let handle = tokio::spawn(async move {
                sleep(Duration::from_secs(10)).await; // Very long delay
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i),
                started_at: Instant::now() - Duration::from_secs(6), // Already been running for 6 seconds
                handle,
            });
        }

        assert!(!manager.is_healthy());
    }

    #[tokio::test]
    async fn test_keepalive_starvation_prevention() {
        // This test simulates the original problem: stream processing starving keep-alive packets
        // With the new async handler system, this should NOT happen

        let mut manager = PacketHandlerManager::new(100);
        let mut keepalive_processed = 0;
        let mut stream_processed = 0;

        // Simulate many long-running stream handlers (like the original problem)
        for i in 0..10 {
            let handle = tokio::spawn(async move {
                // Simulate long stream processing (like the original 10+ second transfers)
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(ProcessResult::Fragment {
                    stream_id: crate::transport::peer_connection::StreamId::next(),
                    fragment_data: vec![0u8; 1024],
                })
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i as u64),
                started_at: Instant::now(),
                handle,
            });
        }

        // Simulate keep-alive packets arriving while streams are processing
        for i in 10..20 {
            let handle = tokio::spawn(async move {
                // Keep-alive should be processed quickly
                tokio::time::sleep(Duration::from_millis(10)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i as u64),
                started_at: Instant::now(),
                handle,
            });
        }

        // Process all handlers
        let mut all_completed = false;
        while !all_completed {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let completed = manager.cleanup_completed_handlers().await;

            for (_, result) in completed {
                match result {
                    Ok(ProcessResult::KeepAlive) => keepalive_processed += 1,
                    Ok(ProcessResult::Fragment { .. }) => stream_processed += 1,
                    _ => {}
                }
            }

            all_completed = manager.active_handler_count() == 0;
        }

        // Both keep-alive and stream packets should be processed
        // Keep-alive should not be starved by stream processing
        assert_eq!(keepalive_processed, 10);
        assert_eq!(stream_processed, 10);

        // Verify health remained good throughout
        assert!(manager.is_healthy());
    }

    #[tokio::test]
    async fn test_flood_protection_under_load() {
        // Test that flood protection works correctly under high load
        let mut manager = PacketHandlerManager::new(5); // Low threshold for testing

        // Add more handlers than the threshold
        for i in 0..10 {
            let handle = tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(ProcessResult::KeepAlive)
            });
            manager.add_handler(PacketHandler {
                id: HandlerId(i as u64),
                started_at: Instant::now(),
                handle,
            });

            // Should hit flood protection after 5 handlers
            if i >= 5 {
                assert!(manager.should_apply_flood_protection());
            }
        }

        // Simulate flood protection dropping packets
        for _ in 0..5 {
            manager.record_flood_drop();
        }

        assert_eq!(manager.stats().packets_dropped_flood, 5);

        // Cleanup should still work
        tokio::time::sleep(Duration::from_millis(150)).await;
        let completed = manager.cleanup_completed_handlers().await;
        assert_eq!(completed.len(), 10); // All handlers should complete
    }

    #[tokio::test]
    async fn test_comprehensive_packet_processing() {
        // Test processing different types of packets in realistic scenarios
        use crate::transport::crypto::TransportKeypair;
        use crate::transport::packet_data::PacketData;
        use crate::transport::symmetric_message::{SymmetricMessage, SymmetricMessagePayload};
        use aes_gcm::KeyInit;
        use std::net::SocketAddr;

        let keypair = TransportKeypair::new();
        let inbound_key = aes_gcm::Aes128Gcm::new(&[42u8; 16].into());
        let outbound_key = aes_gcm::Aes128Gcm::new(&[43u8; 16].into());
        let test_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(10);

        // Test keep-alive packet
        let keepalive_packet = SymmetricMessage::serialize_msg_to_packet_data(
            1,
            SymmetricMessagePayload::NoOp,
            &inbound_key,
            vec![],
        )
        .unwrap();

        let result = process_packet_fully(
            PacketData::from_buf(keepalive_packet.data()),
            &inbound_key,
            [42u8; 16],
            &keypair.secret,
            &outbound_key,
            &tx,
            test_addr,
        )
        .await;

        assert!(matches!(result, Ok(ProcessResult::KeepAlive)));

        // Test regular message packet
        let message_packet = SymmetricMessage::serialize_msg_to_packet_data(
            2,
            SymmetricMessagePayload::ShortMessage {
                payload: vec![1, 2, 3, 4],
            },
            &inbound_key,
            vec![],
        )
        .unwrap();

        let result = process_packet_fully(
            PacketData::from_buf(message_packet.data()),
            &inbound_key,
            [42u8; 16],
            &keypair.secret,
            &outbound_key,
            &tx,
            test_addr,
        )
        .await;

        assert!(matches!(result, Ok(ProcessResult::Message(_))));

        // Test stream fragment
        let test_stream_id = crate::transport::peer_connection::StreamId::next();
        let fragment_packet = SymmetricMessage::serialize_msg_to_packet_data(
            3,
            SymmetricMessagePayload::StreamFragment {
                stream_id: test_stream_id,
                total_length_bytes: 1000,
                fragment_number: 0,
                payload: vec![5, 6, 7, 8],
            },
            &inbound_key,
            vec![],
        )
        .unwrap();

        let result = process_packet_fully(
            PacketData::from_buf(fragment_packet.data()),
            &inbound_key,
            [42u8; 16],
            &keypair.secret,
            &outbound_key,
            &tx,
            test_addr,
        )
        .await;

        assert!(
            matches!(result, Ok(ProcessResult::Fragment { stream_id, .. }) if stream_id == test_stream_id)
        );
    }
}
