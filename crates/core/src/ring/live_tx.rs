use crate::{message::Transaction, node::PeerId};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct LiveTransactionTracker {
    tx_per_peer: Arc<DashMap<PeerId, Vec<Transaction>>>,
}

impl LiveTransactionTracker {
    pub fn add_transaction(&self, peer: PeerId, tx: Transaction) {
        self.tx_per_peer.entry(peer).or_default().push(tx);
    }

    pub fn remove_finished_transaction(&self, tx: Transaction) {
        let keys_to_remove: Vec<PeerId> = self
            .tx_per_peer
            .iter()
            .filter(|entry| entry.value().iter().any(|otx| otx == &tx))
            .map(|entry| entry.key().clone())
            .collect();

        for k in keys_to_remove {
            self.tx_per_peer.remove_if_mut(&k, |_, v| {
                v.retain(|otx| otx != &tx);
                v.is_empty()
            });
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            tx_per_peer: Arc::new(DashMap::default()),
        }
    }

    pub(crate) fn prune_transactions_from_peer(&self, peer: &PeerId) {
        self.tx_per_peer.remove(peer);
    }

    pub(crate) fn has_live_connection(&self, peer: &PeerId) -> bool {
        self.tx_per_peer.contains_key(peer)
    }

    pub(crate) fn still_alive(&self, tx: &Transaction) -> bool {
        self.tx_per_peer.iter().any(|e| e.value().contains(tx))
    }
}
