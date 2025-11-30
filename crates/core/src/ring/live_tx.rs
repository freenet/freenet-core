use crate::message::Transaction;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Tracks live transactions per peer address.
///
/// Uses `SocketAddr` as the key since transactions are tied to network connections,
/// not cryptographic identities.
#[derive(Clone)]
pub struct LiveTransactionTracker {
    tx_per_peer: Arc<DashMap<SocketAddr, Vec<Transaction>>>,
}

impl LiveTransactionTracker {
    pub fn add_transaction(&self, peer_addr: SocketAddr, tx: Transaction) {
        self.tx_per_peer.entry(peer_addr).or_default().push(tx);
    }

    pub fn remove_finished_transaction(&self, tx: Transaction) {
        let keys_to_remove: Vec<SocketAddr> = self
            .tx_per_peer
            .iter()
            .filter(|entry| entry.value().iter().any(|otx| otx == &tx))
            .map(|entry| *entry.key())
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

    pub(crate) fn prune_transactions_from_peer(&self, peer_addr: SocketAddr) {
        self.tx_per_peer.remove(&peer_addr);
    }

    pub(crate) fn has_live_connection(&self, peer_addr: SocketAddr) -> bool {
        self.tx_per_peer.contains_key(&peer_addr)
    }

    pub(crate) fn len(&self) -> usize {
        self.tx_per_peer.len()
    }

    /// Returns the total number of active transactions across all peers.
    pub(crate) fn active_transaction_count(&self) -> usize {
        self.tx_per_peer
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }
}
