use crate::message::Transaction;
use dashmap::{DashMap, DashSet};
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
    /// CONNECT transactions this node *initiated itself* via `Ring::acquire_new`
    /// (i.e. genuine connection-acquisition attempts), as opposed to CONNECTs it
    /// is merely relaying for other peers.
    ///
    /// `connection_maintenance` throttles concurrent acquisition attempts on
    /// this count. It must NOT include relayed CONNECTs: a relay holds a
    /// `tx_per_peer` entry for each CONNECT it forwards (registered in
    /// `p2p_protoc`) until the op completes or hits the 60s TTL, so counting
    /// those would let a relay-heavy node's own acquisition budget be consumed
    /// by other peers' traffic — once at/above `min_connections` the budget is
    /// only `BASE = 3`, so a few relayed CONNECTs would stall the node's own
    /// growth flat just above `min_connections` (#4348). Entries are removed in
    /// lockstep with `remove_finished_transaction` / `prune_transactions_from_peer`,
    /// inheriting their 60s-TTL release backstop, so this set cannot leak.
    acquisition_txs: Arc<DashSet<Transaction>>,
}

impl LiveTransactionTracker {
    pub fn add_transaction(&self, peer_addr: SocketAddr, tx: Transaction) {
        // Insert to reverse index first to prevent race condition:
        // If remove_finished_transaction runs concurrently, it will find the tx
        // in peer_for_tx and clean up properly. If we did tx_per_peer first,
        // a concurrent remove could miss the tx in peer_for_tx and leave orphans.
        //
        // NOTE: this is the pre-#4154 behavior. Re-registering the same
        // `tx` against multiple peers leaves stale entries in the older
        // peers' `tx_per_peer` Vec. Rebind-safe semantics were attempted
        // in PR #4164 but interacted badly with topology maintenance:
        // the "inflated" Vec entries acted as a soft signal that masked
        // peers from neighbor consideration, and tightening that
        // semantic caused `test_six_peer_contract_lifecycle` to diverge
        // (CRDT broadcast missed one peer). Until topology maintenance
        // is decoupled from `has_live_connection`, keep the loose
        // semantics here.
        self.peer_for_tx.insert(tx, peer_addr);
        self.tx_per_peer.entry(peer_addr).or_default().push(tx);
    }

    pub fn remove_finished_transaction(&self, tx: Transaction) {
        // Clear the acquisition gauge in lockstep with the live-tx removal so a
        // completed acquisition frees its maintenance-throttle slot. No-op for
        // relayed CONNECTs (never registered as acquisitions).
        self.acquisition_txs.remove(&tx);
        // O(1) lookup using reverse index instead of O(n) full-map iteration
        if let Some((_, peer_addr)) = self.peer_for_tx.remove(&tx) {
            self.tx_per_peer.remove_if_mut(&peer_addr, |_, v| {
                v.retain(|otx| otx != &tx);
                v.is_empty()
            });
        }
    }

    /// Mark `tx` as a self-initiated connection-acquisition attempt so it counts
    /// toward the `connection_maintenance` concurrency throttle. Call this only
    /// from `Ring::acquire_new`; relayed CONNECTs must NOT be registered here.
    pub(crate) fn register_acquisition(&self, tx: Transaction) {
        self.acquisition_txs.insert(tx);
    }

    pub(crate) fn new() -> Self {
        Self {
            tx_per_peer: Arc::new(DashMap::default()),
            peer_for_tx: Arc::new(DashMap::default()),
            acquisition_txs: Arc::new(DashSet::default()),
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
                self.acquisition_txs.remove(tx);
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

    /// Returns the number of in-flight connection-acquisition attempts this node
    /// initiated itself (via `Ring::acquire_new`). `connection_maintenance` uses
    /// this to throttle concurrent acquisitions.
    ///
    /// This deliberately excludes CONNECTs the node is relaying for other peers
    /// (those are tracked in `tx_per_peer` for cancellation/`has_live_connection`
    /// but never registered as acquisitions), so relay load cannot consume the
    /// node's own acquisition budget — see the `acquisition_txs` field (#4348).
    pub(crate) fn active_acquisition_transaction_count(&self) -> usize {
        self.acquisition_txs.len()
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
    fn acquisition_count_empty() {
        let tracker = LiveTransactionTracker::new();
        assert_eq!(tracker.active_acquisition_transaction_count(), 0);
    }

    /// Regression test for the residual half of #4348: the
    /// `connection_maintenance` acquisition throttle must count ONLY CONNECTs
    /// this node initiated itself, not CONNECTs it is relaying for other peers.
    /// Relayed CONNECTs land in `tx_per_peer` (via `add_transaction`) and are
    /// held until the op completes or hits the 60s TTL; counting them let a
    /// relay-heavy node's own acquisition budget (only `BASE = 3` once at/above
    /// `min_connections`) be consumed by other peers' traffic, stalling its
    /// growth flat just above `min_connections`.
    #[test]
    fn acquisition_count_excludes_relayed_connects() {
        let tracker = LiveTransactionTracker::new();
        let peer1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let peer2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        // Three CONNECTs this node is merely relaying for others.
        tracker.add_transaction(peer1, Transaction::new::<ConnectMsg>());
        tracker.add_transaction(peer1, Transaction::new::<ConnectMsg>());
        tracker.add_transaction(peer2, Transaction::new::<ConnectMsg>());

        // None of them are this node's own acquisition attempts.
        assert_eq!(
            tracker.active_acquisition_transaction_count(),
            0,
            "relayed CONNECTs must not consume the acquisition throttle budget"
        );

        // One CONNECT this node initiated itself via acquire_new.
        let own = Transaction::new::<ConnectMsg>();
        tracker.add_transaction(peer1, own);
        tracker.register_acquisition(own);
        assert_eq!(tracker.active_acquisition_transaction_count(), 1);
    }

    /// A self-initiated CONNECT rebound across hops (acquire_new registers once,
    /// then the tx is forwarded to successive peers via `add_transaction`) is
    /// still a single in-flight acquisition.
    #[test]
    fn acquisition_count_unaffected_by_cross_peer_rebind() {
        let tracker = LiveTransactionTracker::new();
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let tx = Transaction::new::<ConnectMsg>();

        tracker.register_acquisition(tx);
        tracker.add_transaction(addr1, tx);
        tracker.add_transaction(addr2, tx); // rebind to a second hop

        assert_eq!(tracker.active_acquisition_transaction_count(), 1);
    }

    /// Completing an acquisition frees its throttle slot, and a peer disconnect
    /// (prune) does too — so the gauge cannot leak and pin acquisition at the cap.
    #[test]
    fn acquisition_count_drains_on_completion_and_prune() {
        let tracker = LiveTransactionTracker::new();
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();

        let done = Transaction::new::<ConnectMsg>();
        tracker.add_transaction(addr, done);
        tracker.register_acquisition(done);
        let pruned = Transaction::new::<ConnectMsg>();
        tracker.add_transaction(addr, pruned);
        tracker.register_acquisition(pruned);
        assert_eq!(tracker.active_acquisition_transaction_count(), 2);

        tracker.remove_finished_transaction(done);
        assert_eq!(tracker.active_acquisition_transaction_count(), 1);

        tracker.prune_transactions_from_peer(addr);
        assert_eq!(tracker.active_acquisition_transaction_count(), 0);
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

    /// Pins the load-bearing invariant for `Ring::connection_maintenance`
    /// (`crates/core/src/ring.rs:2360-2365`): once a `tx` has been
    /// registered against a peer, `has_live_connection(that_peer)` must
    /// remain `true` until the tx is explicitly cleared, even after the
    /// same `tx` is re-registered against another peer.
    ///
    /// Tightening this (`add_transaction` made rebind-safe in an earlier
    /// commit of #4164) caused `test_six_peer_contract_lifecycle` to
    /// diverge: topology saw a wider candidate-neighbor set and made a
    /// different RemoveConnections decision, breaking a CRDT broadcast
    /// chain. See the topology-coupling note in
    /// `LiveTransactionTracker::add_transaction`'s rustdoc.
    #[test]
    fn add_transaction_rebind_preserves_old_peer_has_live_connection() {
        let tracker = LiveTransactionTracker::new();
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();
        let tx = Transaction::new::<GetMsg>();

        tracker.add_transaction(addr1, tx);
        tracker.add_transaction(addr2, tx);

        assert!(
            tracker.has_live_connection(addr1),
            "old peer must retain has_live_connection=true after rebind \
             (topology-coupling invariant for Ring::connection_maintenance)"
        );
        assert!(tracker.has_live_connection(addr2));
    }
}
