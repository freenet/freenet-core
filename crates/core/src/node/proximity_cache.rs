use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, trace};

use super::PeerId;

/// Proximity cache manager - tracks what contracts this node and its neighbors are caching
pub struct ProximityCacheManager {
    /// Contracts we are caching locally (u32 hashes for efficiency)
    my_cache: Arc<RwLock<HashSet<u32>>>,

    /// What we know about our neighbors' caches
    /// PeerId -> Set of contract hashes they're caching
    neighbor_caches: Arc<DashMap<PeerId, NeighborCache>>,

    /// Statistics for monitoring
    stats: Arc<RwLock<ProximityStats>>,

    /// Last time we sent a batch announcement
    last_batch_announce: Arc<RwLock<Instant>>,

    /// Pending removals to be sent in the next batch announcement
    pending_removals: Arc<RwLock<HashSet<u32>>>,
}

#[derive(Clone, Debug)]
struct NeighborCache {
    /// Contract hashes this neighbor is caching
    contracts: HashSet<u32>,
    /// Last time we received an update from this neighbor
    last_update: Instant,
}

#[derive(Clone, Debug, Default)]
pub struct ProximityStats {
    pub cache_announces_sent: u64,
    pub cache_announces_received: u64,
    pub updates_via_proximity: u64,
    pub updates_via_subscription: u64,
    pub false_positive_forwards: u64,
}

/// Message types for proximity cache protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum ProximityCacheMessage {
    /// Announce contracts we're caching (immediate for additions, batched for removals)
    CacheAnnounce {
        /// Contracts we're now caching
        added: Vec<u32>,
        /// Contracts we're no longer caching
        removed: Vec<u32>,
    },
    /// Request neighbor's cache state (for new connections)
    CacheStateRequest,
    /// Response with full cache state
    CacheStateResponse { contracts: Vec<u32> },
}

impl ProximityCacheManager {
    pub fn new() -> Self {
        Self {
            my_cache: Arc::new(RwLock::new(HashSet::new())),
            neighbor_caches: Arc::new(DashMap::new()),
            stats: Arc::new(RwLock::new(ProximityStats::default())),
            last_batch_announce: Arc::new(RwLock::new(Instant::now())),
            pending_removals: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Generate a u32 hash from a ContractInstanceId
    fn hash_contract(contract_id: &ContractInstanceId) -> u32 {
        // Use first 4 bytes of the ContractInstanceId as hash
        let bytes = contract_id.as_bytes();
        u32::from_le_bytes([
            bytes.first().copied().unwrap_or(0),
            bytes.get(1).copied().unwrap_or(0),
            bytes.get(2).copied().unwrap_or(0),
            bytes.get(3).copied().unwrap_or(0),
        ])
    }

    /// Called when we cache a new contract (PUT or successful GET)
    pub async fn on_contract_cached(
        &self,
        contract_key: &ContractKey,
    ) -> Option<ProximityCacheMessage> {
        let hash = Self::hash_contract(contract_key.id());

        let mut cache = self.my_cache.write().await;
        if cache.insert(hash) {
            info!(
                contract = %contract_key,
                hash = hash,
                "PROXIMITY_PROPAGATION: Added contract to cache"
            );

            // Immediate announcement for new cache entries
            Some(ProximityCacheMessage::CacheAnnounce {
                added: vec![hash],
                removed: vec![],
            })
        } else {
            trace!(
                contract = %contract_key,
                hash = hash,
                "PROXIMITY_PROPAGATION: Contract already in cache"
            );
            None
        }
    }

    /// Called when we evict a contract from cache
    #[allow(dead_code)] // TODO: This will be called when contract eviction is implemented
    pub async fn on_contract_evicted(&self, contract_key: &ContractKey) {
        let hash = Self::hash_contract(contract_key.id());

        let mut cache = self.my_cache.write().await;
        if cache.remove(&hash) {
            debug!(
                contract = %contract_key,
                hash = hash,
                "PROXIMITY_PROPAGATION: Removed contract from cache, adding to pending removals"
            );
            // Add to pending removals for batch processing
            let mut pending = self.pending_removals.write().await;
            pending.insert(hash);
        }
    }

    /// Process a proximity cache message from a neighbor
    /// Returns an optional response message that should be sent back to the peer
    pub async fn handle_message(
        &self,
        peer_id: PeerId,
        message: ProximityCacheMessage,
    ) -> Option<ProximityCacheMessage> {
        match message {
            ProximityCacheMessage::CacheAnnounce { added, removed } => {
                let mut stats = self.stats.write().await;
                stats.cache_announces_received += 1;
                drop(stats);

                // Update our knowledge of this neighbor's cache
                self.neighbor_caches
                    .entry(peer_id.clone())
                    .and_modify(|cache| {
                        for hash in &added {
                            cache.contracts.insert(*hash);
                        }
                        for hash in &removed {
                            cache.contracts.remove(hash);
                        }
                        cache.last_update = Instant::now();
                    })
                    .or_insert_with(|| NeighborCache {
                        contracts: added.iter().copied().collect(),
                        last_update: Instant::now(),
                    });

                debug!(
                    peer = %peer_id,
                    added = added.len(),
                    removed = removed.len(),
                    "PROXIMITY_PROPAGATION: Updated neighbor cache knowledge"
                );
                None
            }

            ProximityCacheMessage::CacheStateRequest => {
                // Send our full cache state
                let cache = self.my_cache.read().await;
                let response = ProximityCacheMessage::CacheStateResponse {
                    contracts: cache.iter().copied().collect(),
                };
                drop(cache);

                let cache_size =
                    if let ProximityCacheMessage::CacheStateResponse { contracts } = &response {
                        contracts.len()
                    } else {
                        0
                    };
                debug!(
                    peer = %peer_id,
                    cache_size = cache_size,
                    "PROXIMITY_PROPAGATION: Sending cache state to neighbor"
                );

                Some(response)
            }

            ProximityCacheMessage::CacheStateResponse { contracts } => {
                // Update our knowledge of this neighbor's full cache
                self.neighbor_caches.insert(
                    peer_id.clone(),
                    NeighborCache {
                        contracts: contracts.into_iter().collect(),
                        last_update: Instant::now(),
                    },
                );

                info!(
                    peer = %peer_id,
                    contracts = self.neighbor_caches.get(&peer_id).map(|c| c.contracts.len()).unwrap_or(0),
                    "PROXIMITY_PROPAGATION: Received full cache state from neighbor"
                );
                None
            }
        }
    }

    /// Generate a cache state request for a new peer connection
    /// This should be called when a new peer connection is established
    pub fn request_cache_state_from_peer(&self) -> ProximityCacheMessage {
        debug!("PROXIMITY_PROPAGATION: Generating cache state request for new peer");
        ProximityCacheMessage::CacheStateRequest
    }

    /// Check if any neighbors might have this contract cached (for update forwarding)
    pub async fn neighbors_with_contract(&self, contract_key: &ContractKey) -> Vec<PeerId> {
        let hash = Self::hash_contract(contract_key.id());

        let mut neighbors = Vec::new();
        for entry in self.neighbor_caches.iter() {
            if entry.value().contracts.contains(&hash) {
                neighbors.push(entry.key().clone());
            }
        }

        if !neighbors.is_empty() {
            debug!(
                contract = %contract_key,
                hash = hash,
                neighbor_count = neighbors.len(),
                "PROXIMITY_PROPAGATION: Found neighbors with contract"
            );
        }

        neighbors
    }

    /// Generate a batch announcement for pending removals (called periodically)
    pub async fn generate_batch_announcement(&self) -> Option<ProximityCacheMessage> {
        let mut last_announce = self.last_batch_announce.write().await;

        // Only send batch announcements every 30 seconds
        if last_announce.elapsed() < Duration::from_secs(30) {
            return None;
        }

        *last_announce = Instant::now();

        // Get pending removals and clear the list
        let mut pending = self.pending_removals.write().await;
        if pending.is_empty() {
            return None;
        }

        let removals: Vec<u32> = pending.iter().copied().collect();
        pending.clear();
        drop(pending); // Release lock early
        drop(last_announce); // Release lock early

        info!(
            removal_count = removals.len(),
            "PROXIMITY_PROPAGATION: Generated batch announcement for removals"
        );

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.cache_announces_sent += 1;

        Some(ProximityCacheMessage::CacheAnnounce {
            added: vec![],
            removed: removals,
        })
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> ProximityStats {
        self.stats.read().await.clone()
    }

    /// Get introspection data for debugging
    pub async fn get_introspection_data(&self) -> (Vec<u32>, HashMap<String, Vec<u32>>) {
        let my_cache = self.my_cache.read().await.iter().copied().collect();

        let mut neighbor_data = HashMap::new();
        for entry in self.neighbor_caches.iter() {
            neighbor_data.insert(
                entry.key().to_string(), // Convert PeerId to String for introspection
                entry.value().contracts.iter().copied().collect(),
            );
        }

        (my_cache, neighbor_data)
    }

    /// Record that an update was forwarded via proximity
    #[allow(dead_code)]
    pub async fn record_proximity_forward(&self) {
        let mut stats = self.stats.write().await;
        stats.updates_via_proximity += 1;
    }

    /// Record that an update was forwarded via subscription
    #[allow(dead_code)]
    pub async fn record_subscription_forward(&self) {
        let mut stats = self.stats.write().await;
        stats.updates_via_subscription += 1;
    }

    /// Record a false positive (forwarded to a peer that didn't actually have the contract)
    #[allow(dead_code)]
    pub async fn record_false_positive(&self) {
        let mut stats = self.stats.write().await;
        stats.false_positive_forwards += 1;
    }

    /// Get list of all known neighbor peer IDs for sending batch announcements
    pub fn get_neighbor_ids(&self) -> Vec<PeerId> {
        self.neighbor_caches
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Create a periodic task for batch announcements that sends through the event loop
    /// This should be spawned as a background task when the node starts
    pub fn spawn_periodic_batch_announcements(
        self: Arc<Self>,
        event_loop_notifier: crate::node::EventLoopNotificationsSender,
        op_manager: std::sync::Weak<crate::node::OpManager>,
    ) {
        use crate::{
            config::GlobalExecutor,
            message::{NetMessage, NetMessageV1},
        };

        GlobalExecutor::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            info!("PROXIMITY_PROPAGATION: Periodic batch announcement task started");

            loop {
                interval.tick().await;

                // Check if the op_manager is still alive
                let op_manager = match op_manager.upgrade() {
                    Some(manager) => manager,
                    None => {
                        info!("PROXIMITY_PROPAGATION: OpManager dropped, stopping batch announcement task");
                        break;
                    }
                };

                // Generate batch announcement if there are pending removals
                if let Some(announcement) = self.generate_batch_announcement().await {
                    let neighbor_ids = self.get_neighbor_ids();

                    if neighbor_ids.is_empty() {
                        debug!("PROXIMITY_PROPAGATION: No neighbors to send batch announcement to");
                        continue;
                    }

                    // Get our own peer ID
                    let own_peer_id = match op_manager.ring.connection_manager.get_peer_key() {
                        Some(peer_id) => peer_id,
                        None => {
                            debug!("PROXIMITY_PROPAGATION: No peer key available, skipping batch announcement");
                            continue;
                        }
                    };

                    info!(
                        neighbor_count = neighbor_ids.len(),
                        removal_count = match &announcement {
                            ProximityCacheMessage::CacheAnnounce { removed, .. } => removed.len(),
                            _ => 0,
                        },
                        "PROXIMITY_PROPAGATION: Sending periodic batch announcement to neighbors"
                    );

                    // Send to all neighbors through the event loop notification system
                    for peer_id in neighbor_ids {
                        let message = NetMessage::V1(NetMessageV1::ProximityCache {
                            from: own_peer_id.clone(),
                            message: announcement.clone(),
                        });

                        // Send the message through the event loop notifications
                        if event_loop_notifier
                            .notifications_sender()
                            .send(either::Either::Left(message))
                            .await
                            .is_err()
                        {
                            debug!(
                                peer = %peer_id,
                                "PROXIMITY_PROPAGATION: Failed to send batch announcement to event loop"
                            );
                        }
                    }
                }
            }

            info!("PROXIMITY_PROPAGATION: Periodic batch announcement task stopped");
        });
    }

    /// Handle peer disconnection by removing them from the neighbor cache
    /// This prevents stale data from accumulating and avoids forwarding updates to disconnected peers
    pub fn on_peer_disconnected(&self, peer_id: &PeerId) {
        if let Some((_, removed_cache)) = self.neighbor_caches.remove(peer_id) {
            debug!(
                peer = %peer_id,
                cached_contracts = removed_cache.contracts.len(),
                "PROXIMITY_CACHE: Removed disconnected peer from neighbor cache"
            );
        }
    }

    /// Cleanup stale neighbor entries based on last_update timestamp
    /// This provides an alternative to explicit disconnect notifications
    pub async fn cleanup_stale_neighbors(&self, max_age: Duration) {
        let now = Instant::now();
        let mut removed_count = 0;

        // Collect stale peer IDs to avoid holding references while removing
        let stale_peers: Vec<PeerId> = self
            .neighbor_caches
            .iter()
            .filter_map(|entry| {
                let peer_id = entry.key().clone();
                let cache = entry.value();
                if now.duration_since(cache.last_update) > max_age {
                    Some(peer_id)
                } else {
                    None
                }
            })
            .collect();

        // Remove stale entries
        for peer_id in stale_peers {
            if let Some((_, removed_cache)) = self.neighbor_caches.remove(&peer_id) {
                removed_count += 1;
                debug!(
                    peer = %peer_id,
                    cached_contracts = removed_cache.contracts.len(),
                    age = ?now.duration_since(removed_cache.last_update),
                    "PROXIMITY_CACHE: Removed stale neighbor cache entry"
                );
            }
        }

        if removed_count > 0 {
            info!(
                removed_peers = removed_count,
                max_age = ?max_age,
                "PROXIMITY_CACHE: Cleaned up stale neighbor cache entries"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::ContractInstanceId;
    use std::time::Duration;

    fn create_test_contract_key() -> ContractKey {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        ContractKey::from(contract_id)
    }

    #[tokio::test]
    async fn test_contract_caching_and_eviction() {
        let cache = ProximityCacheManager::new();
        let contract_key = create_test_contract_key();

        // Test caching a contract generates immediate announcement
        let announcement = cache.on_contract_cached(&contract_key).await;
        assert!(announcement.is_some());

        if let Some(ProximityCacheMessage::CacheAnnounce { added, removed }) = announcement {
            assert_eq!(added.len(), 1);
            assert!(removed.is_empty());
        } else {
            panic!("Expected CacheAnnounce message");
        }

        // Test evicting a contract adds to pending removals but doesn't generate immediate announcement
        cache.on_contract_evicted(&contract_key).await;

        // Check that the contract is in pending removals
        let pending = cache.pending_removals.read().await;
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_batch_announcement_generation() {
        let cache = ProximityCacheManager::new();
        let contract_key = create_test_contract_key();

        // Add a contract to pending removals manually
        let hash = ProximityCacheManager::hash_contract(contract_key.id());
        {
            let mut pending = cache.pending_removals.write().await;
            pending.insert(hash);
        }

        // Force time to pass for batch announcement
        {
            let mut last_announce = cache.last_batch_announce.write().await;
            *last_announce = Instant::now() - Duration::from_secs(31);
        }

        // Generate batch announcement
        let announcement = cache.generate_batch_announcement().await;
        assert!(announcement.is_some());

        if let Some(ProximityCacheMessage::CacheAnnounce { added, removed }) = announcement {
            assert!(added.is_empty());
            assert_eq!(removed.len(), 1);
            assert_eq!(removed[0], hash);
        } else {
            panic!("Expected CacheAnnounce message");
        }

        // Check that pending removals are cleared
        let pending = cache.pending_removals.read().await;
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn test_no_batch_announcement_when_no_pending_removals() {
        let cache = ProximityCacheManager::new();

        // Force time to pass for batch announcement
        {
            let mut last_announce = cache.last_batch_announce.write().await;
            *last_announce = Instant::now() - Duration::from_secs(31);
        }

        // Generate batch announcement - should be None since no pending removals
        let announcement = cache.generate_batch_announcement().await;
        assert!(announcement.is_none());
    }

    #[tokio::test]
    async fn test_batch_announcement_rate_limiting() {
        let cache = ProximityCacheManager::new();
        let contract_key = create_test_contract_key();

        // Add a contract to pending removals
        let hash = ProximityCacheManager::hash_contract(contract_key.id());
        {
            let mut pending = cache.pending_removals.write().await;
            pending.insert(hash);
        }

        // Try to generate batch announcement too soon - should be rate limited
        let announcement = cache.generate_batch_announcement().await;
        assert!(announcement.is_none());

        // Check that pending removals are still there
        let pending = cache.pending_removals.read().await;
        assert_eq!(pending.len(), 1);
    }
}
