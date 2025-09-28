use super::*;
use crate::{
    message::Transaction,
    node::PeerId,
    ring::{Location, PeerKeyLocation},
    util::Contains,
};
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use std::collections::HashSet;

/// TestRing implements only the methods used by subscription routing
#[allow(clippy::type_complexity)]
struct TestRing {
    pub k_closest_calls: std::sync::Arc<tokio::sync::Mutex<Vec<(ContractKey, Vec<PeerId>, usize)>>>,
    pub candidates: Vec<PeerKeyLocation>,
}

impl TestRing {
    fn new(candidates: Vec<PeerKeyLocation>, _own_location: PeerKeyLocation) -> Self {
        Self {
            k_closest_calls: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
            candidates,
        }
    }

    pub async fn k_closest_potentially_caching(
        &self,
        key: &ContractKey,
        skip_list: impl Contains<PeerId> + Clone,
        k: usize,
    ) -> Vec<PeerKeyLocation> {
        // Record the call - use async lock
        let skip_vec: Vec<PeerId> = self
            .candidates
            .iter()
            .filter(|peer| skip_list.has_element(peer.peer.clone()))
            .map(|peer| peer.peer.clone())
            .collect();

        // Use async lock
        self.k_closest_calls.lock().await.push((*key, skip_vec, k));

        // Return candidates not in skip list
        self.candidates
            .iter()
            .filter(|peer| !skip_list.has_element(peer.peer.clone()))
            .take(k)
            .cloned()
            .collect()
    }
}