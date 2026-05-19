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
    /// Register that transaction `tx` is in flight to `peer_addr`.
    ///
    /// Idempotent and rebind-safe:
    /// - Same `(peer_addr, tx)` re-registration is a no-op (no Vec duplication).
    /// - Re-registering `tx` to a different peer scrubs the previous
    ///   peer's entry, so a tx that flows through multiple peers ends up
    ///   tracked against exactly the most recent one. Pre-#4154 a CONNECT
    ///   ride-along (`Ring::initiate_connect` + `handle_op_execution`)
    ///   silently double-counted in `active_connect_transaction_count`;
    ///   a relay re-sending response-side to its upstream after sending
    ///   the request-side downstream left a stale entry on the
    ///   downstream peer that only cleaned up on disconnect.
    ///
    /// Concurrency note: the reverse-index insert happens first so a
    /// racing `remove_finished_transaction` always sees a consistent
    /// `(tx → peer)` mapping and cleans the matching forward-index
    /// entry. The cross-peer scrub uses `remove_if_mut`'s atomic
    /// retain-and-prune.
    pub fn add_transaction(&self, peer_addr: SocketAddr, tx: Transaction) {
        let prev = self.peer_for_tx.insert(tx, peer_addr);
        match prev {
            Some(prev_addr) if prev_addr == peer_addr => {
                // Idempotent re-registration; don't duplicate in tx_per_peer's Vec.
                return;
            }
            Some(prev_addr) => {
                // Different peer: scrub the stale forward-index entry.
                self.tx_per_peer.remove_if_mut(&prev_addr, |_, v| {
                    v.retain(|otx| otx != &tx);
                    v.is_empty()
                });
            }
            None => {}
        }
        self.tx_per_peer.entry(peer_addr).or_default().push(tx);
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

    /// Prune all transactions associated with a peer and return them.
    ///
    /// Returns the list of transactions that were associated with this peer,
    /// allowing callers to handle them appropriately (e.g., retry via alternate routes).
    pub(crate) fn prune_transactions_from_peer(&self, peer_addr: SocketAddr) -> Vec<Transaction> {
        // Remove all transactions for this peer from the reverse index
        if let Some((_, txs)) = self.tx_per_peer.remove(&peer_addr) {
            for tx in &txs {
                self.peer_for_tx.remove(tx);
            }
            txs
        } else {
            Vec::new()
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

    #[test]
    fn prune_transactions_from_peer_cleans_both_indices() {
        let tracker = LiveTransactionTracker::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let tx1 = Transaction::new::<ConnectMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let tx3 = Transaction::new::<PutMsg>();

        // Add transactions for two peers
        tracker.add_transaction(addr1, tx1);
        tracker.add_transaction(addr1, tx2);
        tracker.add_transaction(addr2, tx3);

        assert_eq!(tracker.active_transaction_count(), 3);
        assert_eq!(tracker.peer_for_tx.len(), 3);

        // Prune peer1
        tracker.prune_transactions_from_peer(addr1);

        // peer1's transactions should be gone from both indices
        assert_eq!(tracker.active_transaction_count(), 1);
        assert_eq!(tracker.peer_for_tx.len(), 1);
        assert!(!tracker.peer_for_tx.contains_key(&tx1));
        assert!(!tracker.peer_for_tx.contains_key(&tx2));
        assert!(tracker.peer_for_tx.contains_key(&tx3));

        // peer2's transaction should still exist
        assert!(tracker.has_live_connection(addr2));
        assert!(!tracker.has_live_connection(addr1));
    }

    #[test]
    fn prune_transactions_from_peer_returns_transactions() {
        let tracker = LiveTransactionTracker::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let tx1 = Transaction::new::<ConnectMsg>();
        let tx2 = Transaction::new::<GetMsg>();
        let tx3 = Transaction::new::<PutMsg>();

        // Add transactions for two peers
        tracker.add_transaction(addr1, tx1);
        tracker.add_transaction(addr1, tx2);
        tracker.add_transaction(addr2, tx3);

        // Prune peer1 and check returned transactions
        let pruned = tracker.prune_transactions_from_peer(addr1);
        assert_eq!(pruned.len(), 2);
        assert!(pruned.contains(&tx1));
        assert!(pruned.contains(&tx2));

        // Prune peer2 and check returned transaction
        let pruned = tracker.prune_transactions_from_peer(addr2);
        assert_eq!(pruned.len(), 1);
        assert!(pruned.contains(&tx3));

        // Prune nonexistent peer returns empty
        let pruned = tracker.prune_transactions_from_peer(addr1);
        assert!(pruned.is_empty());
    }

    /// Regression for #4154: re-registering the same `(peer, tx)` pair
    /// must be a no-op (no Vec duplication). The CONNECT ride-along
    /// path (`Ring::initiate_connect` adds `(gw, tx)`, then
    /// `handle_op_execution` re-adds the same pair when the request
    /// flows through `send_to_and_collect_replies`) otherwise inflates
    /// `active_connect_transaction_count` and throttles concurrent
    /// CONNECT acquisitions.
    #[test]
    fn add_transaction_is_idempotent_for_same_pair() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let tx = Transaction::new::<ConnectMsg>();

        tracker.add_transaction(addr, tx);
        tracker.add_transaction(addr, tx);
        tracker.add_transaction(addr, tx);

        assert_eq!(tracker.active_transaction_count(), 1);
        assert_eq!(tracker.active_connect_transaction_count(), 1);
        assert_eq!(tracker.peer_for_tx.len(), 1);

        // Single removal must clean both indices fully.
        tracker.remove_finished_transaction(tx);
        assert_eq!(tracker.active_transaction_count(), 0);
        assert!(!tracker.has_live_connection(addr));
        assert_eq!(tracker.peer_for_tx.len(), 0);
    }

    /// Regression for #4154: a relay that forwards a request downstream
    /// then later sends a response back upstream re-registers the same
    /// tx against a different peer. Before the fix, the old peer's
    /// forward-index Vec retained the tx until that peer disconnected.
    /// After the fix, the second registration scrubs the stale entry.
    #[test]
    fn add_transaction_rebinds_to_new_peer() {
        let tracker = LiveTransactionTracker::new();
        let downstream: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let upstream: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let tx = Transaction::new::<GetMsg>();

        // Relay first forwards request to downstream.
        tracker.add_transaction(downstream, tx);
        assert!(tracker.has_live_connection(downstream));
        assert!(!tracker.has_live_connection(upstream));

        // Then forwards the response back to upstream — `tx` migrates.
        tracker.add_transaction(upstream, tx);
        assert!(
            !tracker.has_live_connection(downstream),
            "downstream's stale forward-index entry must be scrubbed"
        );
        assert!(tracker.has_live_connection(upstream));
        assert_eq!(tracker.peer_for_tx.len(), 1);
        assert_eq!(tracker.active_transaction_count(), 1);

        // Pruning the (now-stale) downstream peer must NOT find `tx`.
        let pruned = tracker.prune_transactions_from_peer(downstream);
        assert!(
            pruned.is_empty(),
            "rebinding must leave the old peer with no orphan claim on tx"
        );

        // Upstream's prune still picks up tx as expected.
        let pruned = tracker.prune_transactions_from_peer(upstream);
        assert_eq!(pruned, vec![tx]);
    }
}
