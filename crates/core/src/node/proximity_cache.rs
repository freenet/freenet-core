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
        }
    }

    /// Generate a u32 hash from a ContractInstanceId
    fn hash_contract(contract_id: &ContractInstanceId) -> u32 {
        // Use first 4 bytes of the ContractInstanceId as hash
        let bytes = contract_id.as_bytes();
        u32::from_le_bytes([
            bytes.get(0).copied().unwrap_or(0),
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
    pub async fn on_contract_evicted(&self, contract_key: &ContractKey) {
        let hash = Self::hash_contract(contract_key.id());

        let mut cache = self.my_cache.write().await;
        if cache.remove(&hash) {
            debug!(
                contract = %contract_key,
                hash = hash,
                "PROXIMITY_PROPAGATION: Removed contract from cache"
            );
            // Removals are batched, not sent immediately
        }
    }

    /// Process a proximity cache message from a neighbor
    pub async fn handle_message(&self, peer_id: PeerId, message: ProximityCacheMessage) {
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
            }

            ProximityCacheMessage::CacheStateRequest => {
                // Send our full cache state
                let cache = self.my_cache.read().await;
                let _response = ProximityCacheMessage::CacheStateResponse {
                    contracts: cache.iter().copied().collect(),
                };
                drop(cache);

                debug!(
                    peer = %peer_id,
                    "PROXIMITY_PROPAGATION: Sending cache state to neighbor"
                );

                // TODO: Send response to peer
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
            }
        }
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

    /// Generate a batch announcement for removed contracts (called periodically)
    pub async fn generate_batch_announcement(&self) -> Option<ProximityCacheMessage> {
        let mut last_announce = self.last_batch_announce.write().await;

        // Only send batch announcements every 30 seconds
        if last_announce.elapsed() < Duration::from_secs(30) {
            return None;
        }

        *last_announce = Instant::now();

        // For now, we don't track removals separately, so return None
        // In a full implementation, we'd track pending removals here
        None
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
    pub async fn record_proximity_forward(&self) {
        let mut stats = self.stats.write().await;
        stats.updates_via_proximity += 1;
    }

    /// Record that an update was forwarded via subscription
    pub async fn record_subscription_forward(&self) {
        let mut stats = self.stats.write().await;
        stats.updates_via_subscription += 1;
    }

    /// Record a false positive (forwarded to a peer that didn't actually have the contract)
    pub async fn record_false_positive(&self) {
        let mut stats = self.stats.write().await;
        stats.false_positive_forwards += 1;
    }
}
