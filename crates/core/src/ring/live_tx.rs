use crate::message::{Transaction, TransactionType};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;

/// Tracks live transactions per peer address.
///
/// Uses `SocketAddr` as the key since transactions are tied to network connections,
/// not cryptographic identities.
///
/// Maintains a reverse index (tx -> peer) for O(1) transaction removal instead of
/// O(n) full-map iteration. This significantly reduces lock contention under load.
#[derive(Clone)]
pub struct LiveTransactionTracker {
    tx_per_peer: Arc<DashMap<SocketAddr, Vec<Transaction>>>,
    /// Reverse index: Transaction -> SocketAddr for O(1) lookup during removal.
    /// Without this, remove_finished_transaction would need to iterate all peers.
    peer_for_tx: Arc<DashMap<Transaction, SocketAddr>>,
}

impl LiveTransactionTracker {
    pub fn add_transaction(&self, peer_addr: SocketAddr, tx: Transaction) {
        self.tx_per_peer.entry(peer_addr).or_default().push(tx);
        self.peer_for_tx.insert(tx, peer_addr);
    }

    pub fn remove_finished_transaction(&self, tx: Transaction) {
        // O(1) lookup using reverse index instead of O(n) full-map iteration
        if let Some((_, peer_addr)) = self.peer_for_tx.remove(&tx) {
            self.tx_per_peer.remove_if_mut(&peer_addr, |_, v| {
                v.retain(|otx| otx != &tx);
                v.is_empty()
            });
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            tx_per_peer: Arc::new(DashMap::default()),
            peer_for_tx: Arc::new(DashMap::default()),
        }
    }

    pub(crate) fn prune_transactions_from_peer(&self, peer_addr: SocketAddr) {
        // Remove all transactions for this peer from the reverse index
        if let Some((_, txs)) = self.tx_per_peer.remove(&peer_addr) {
            for tx in txs {
                self.peer_for_tx.remove(&tx);
            }
        }
    }

    pub(crate) fn has_live_connection(&self, peer_addr: SocketAddr) -> bool {
        self.tx_per_peer.contains_key(&peer_addr)
    }

    pub(crate) fn len(&self) -> usize {
        self.tx_per_peer.len()
    }

    /// Returns the total number of active transactions across all peers.
    #[cfg(test)]
    pub(crate) fn active_transaction_count(&self) -> usize {
        self.tx_per_peer
            .iter()
            .map(|entry| entry.value().len())
            .sum()
    }

    /// Returns the number of active Connect transactions across all peers.
    /// Used to limit concurrent connection acquisition attempts.
    pub(crate) fn active_connect_transaction_count(&self) -> usize {
        self.tx_per_peer
            .iter()
            .map(|entry| {
                entry
                    .value()
                    .iter()
                    .filter(|tx| tx.transaction_type() == TransactionType::Connect)
                    .count()
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::connect::ConnectMsg;
    use crate::operations::get::GetMsg;
    use crate::operations::put::PutMsg;

    #[test]
    fn active_transaction_count_empty() {
        let tracker = LiveTransactionTracker::new();
        assert_eq!(tracker.active_transaction_count(), 0);
    }

    #[test]
    fn active_transaction_count_single_peer() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        tracker.add_transaction(addr, Transaction::new::<ConnectMsg>());
        assert_eq!(tracker.active_transaction_count(), 1);

        tracker.add_transaction(addr, Transaction::new::<ConnectMsg>());
        assert_eq!(tracker.active_transaction_count(), 2);
    }

    #[test]
    fn active_transaction_count_multiple_peers() {
        let tracker = LiveTransactionTracker::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        tracker.add_transaction(addr1, Transaction::new::<ConnectMsg>());
        tracker.add_transaction(addr1, Transaction::new::<ConnectMsg>());
        tracker.add_transaction(addr2, Transaction::new::<ConnectMsg>());

        assert_eq!(tracker.active_transaction_count(), 3);
    }

    #[test]
    fn active_transaction_count_after_removal() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let tx1 = Transaction::new::<ConnectMsg>();
        let tx2 = Transaction::new::<ConnectMsg>();

        tracker.add_transaction(addr, tx1);
        tracker.add_transaction(addr, tx2);
        assert_eq!(tracker.active_transaction_count(), 2);

        tracker.remove_finished_transaction(tx1);
        assert_eq!(tracker.active_transaction_count(), 1);

        tracker.remove_finished_transaction(tx2);
        assert_eq!(tracker.active_transaction_count(), 0);
    }

    #[test]
    fn active_connect_transaction_count_filters_by_type() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Add mixed transaction types
        tracker.add_transaction(addr, Transaction::new::<ConnectMsg>());
        tracker.add_transaction(addr, Transaction::new::<GetMsg>());
        tracker.add_transaction(addr, Transaction::new::<PutMsg>());
        tracker.add_transaction(addr, Transaction::new::<ConnectMsg>());

        // Total count should be 4
        assert_eq!(tracker.active_transaction_count(), 4);
        // Connect count should only be 2
        assert_eq!(tracker.active_connect_transaction_count(), 2);
    }

    #[test]
    fn active_connect_transaction_count_empty() {
        let tracker = LiveTransactionTracker::new();
        assert_eq!(tracker.active_connect_transaction_count(), 0);
    }

    #[test]
    fn active_connect_transaction_count_no_connects() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        // Add only non-connect transactions
        tracker.add_transaction(addr, Transaction::new::<GetMsg>());
        tracker.add_transaction(addr, Transaction::new::<PutMsg>());

        assert_eq!(tracker.active_transaction_count(), 2);
        assert_eq!(tracker.active_connect_transaction_count(), 0);
    }
}
