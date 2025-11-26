use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use dashmap::DashMap;
use tokio::time::sleep;

use super::Location;
use crate::node::PeerId;

/// Metadata tracked for a transient connection that hasn't been promoted yet.
#[derive(Clone)]
pub(crate) struct TransientEntry {
    pub location: Option<Location>,
}

/// Centralized manager for transient connection bookkeeping and lifecycle.
#[derive(Clone)]
pub(crate) struct TransientConnectionManager {
    entries: Arc<DashMap<PeerId, TransientEntry>>,
    in_use: Arc<AtomicUsize>,
    budget: usize,
    ttl: Duration,
}

impl TransientConnectionManager {
    pub fn new(budget: usize, ttl: Duration) -> Self {
        Self {
            entries: Arc::new(DashMap::new()),
            in_use: Arc::new(AtomicUsize::new(0)),
            budget,
            ttl,
        }
    }

    /// Reserve a transient slot for the peer, updating the location if it already exists.
    /// Returns `false` when the budget is exhausted.
    pub fn try_reserve(&self, peer: PeerId, location: Option<Location>) -> bool {
        use dashmap::mapref::entry::Entry;

        match self.entries.entry(peer.clone()) {
            Entry::Occupied(mut occ) => {
                occ.get_mut().location = location;
                true
            }
            Entry::Vacant(vac) => {
                let current = self.in_use.load(Ordering::Acquire);
                if current >= self.budget {
                    return false;
                }
                vac.insert(TransientEntry { location });
                let prev = self.in_use.fetch_add(1, Ordering::SeqCst);
                if prev >= self.budget {
                    // Undo if we raced past the budget.
                    self.entries.remove(&peer);
                    self.in_use.fetch_sub(1, Ordering::SeqCst);
                    return false;
                }
                true
            }
        }
    }

    /// Remove a transient entry (promotion or drop) and return its metadata.
    pub fn remove(&self, peer: &PeerId) -> Option<TransientEntry> {
        let removed = self.entries.remove(peer).map(|(_, entry)| entry);
        if removed.is_some() {
            self.in_use.fetch_sub(1, Ordering::SeqCst);
        }
        removed
    }

    pub fn is_transient(&self, peer: &PeerId) -> bool {
        self.entries.contains_key(peer)
    }

    pub fn count(&self) -> usize {
        self.in_use.load(Ordering::Acquire)
    }

    pub fn budget(&self) -> usize {
        self.budget
    }

    /// Schedule expiry for a transient peer; executes `on_expire` if the entry still exists after TTL.
    pub fn schedule_expiry<F, Fut>(&self, peer: PeerId, on_expire: F)
    where
        F: FnOnce(PeerId) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let ttl = self.ttl;
        let manager = self.clone();
        tokio::spawn(async move {
            sleep(ttl).await;
            if manager.remove(&peer).is_some() {
                on_expire(peer).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TransportKeypair, TransportPublicKey};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::atomic::AtomicBool;
    use tokio::time::timeout;

    fn make_peer(port: u16, pub_key: TransportPublicKey) -> PeerId {
        PeerId::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            pub_key,
        )
    }

    #[tokio::test]
    async fn respects_budget_and_releases_on_remove() {
        let keypair = TransportKeypair::new();
        let pub_key = keypair.public().clone();
        let manager = TransientConnectionManager::new(2, Duration::from_secs(1));

        let p1 = make_peer(1000, pub_key.clone());
        let p2 = make_peer(1001, pub_key.clone());
        let p3 = make_peer(1002, pub_key.clone());

        assert!(manager.try_reserve(p1.clone(), None));
        assert!(manager.try_reserve(p2.clone(), None));
        assert!(!manager.try_reserve(p3.clone(), None));
        assert_eq!(manager.count(), 2);

        manager.remove(&p1);
        assert_eq!(manager.count(), 1);
        assert!(manager.try_reserve(p3.clone(), None));
        assert_eq!(manager.count(), 2);
        assert!(manager.is_transient(&p2));
        assert!(manager.is_transient(&p3));
    }

    #[tokio::test]
    async fn expires_and_invokes_callback() {
        let keypair = TransportKeypair::new();
        let pub_key = keypair.public().clone();
        let ttl = Duration::from_millis(20);
        let manager = TransientConnectionManager::new(1, ttl);

        let peer = make_peer(2000, pub_key);
        assert!(manager.try_reserve(peer.clone(), None));

        let fired = Arc::new(AtomicBool::new(false));
        let fired_ref = fired.clone();
        let peer_for_expiry = peer.clone();
        manager.schedule_expiry(peer_for_expiry.clone(), move |p| {
            let fired = fired_ref.clone();
            async move {
                assert_eq!(p, peer_for_expiry);
                fired.store(true, Ordering::SeqCst);
            }
        });

        timeout(Duration::from_millis(200), async {
            while manager.is_transient(&peer) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("transient should expire within timeout");

        assert!(fired.load(Ordering::SeqCst));
        assert!(!manager.is_transient(&peer));
    }

    #[tokio::test]
    async fn concurrent_reserve_same_peer() {
        // Test that concurrent try_reserve calls for the same peer don't corrupt the counter.
        let keypair = TransportKeypair::new();
        let pub_key = keypair.public().clone();
        let manager = Arc::new(TransientConnectionManager::new(10, Duration::from_secs(60)));

        let peer = make_peer(3000, pub_key);
        let num_tasks = 50;

        let handles: Vec<_> = (0..num_tasks)
            .map(|_| {
                let mgr = manager.clone();
                let p = peer.clone();
                tokio::spawn(async move { mgr.try_reserve(p, None) })
            })
            .collect();

        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("task panicked"))
            .collect();

        // All should succeed since it's the same peer (entry update after first insert).
        assert!(results.iter().all(|&r| r));
        // Only one entry should exist.
        assert_eq!(manager.count(), 1);
        assert!(manager.is_transient(&peer));
    }

    #[tokio::test]
    async fn concurrent_reserve_different_peers_respects_budget() {
        // Test that concurrent try_reserve for different peers correctly enforces budget.
        let keypair = TransportKeypair::new();
        let pub_key = keypair.public().clone();
        let budget = 5;
        let manager = Arc::new(TransientConnectionManager::new(
            budget,
            Duration::from_secs(60),
        ));

        let num_peers = 20;
        let peers: Vec<_> = (0..num_peers)
            .map(|i| make_peer(4000 + i, pub_key.clone()))
            .collect();

        let handles: Vec<_> = peers
            .iter()
            .map(|p| {
                let mgr = manager.clone();
                let peer = p.clone();
                tokio::spawn(async move { mgr.try_reserve(peer, None) })
            })
            .collect();

        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.expect("task panicked"))
            .collect();

        let successes = results.iter().filter(|&&r| r).count();
        // Exactly `budget` reservations should succeed.
        assert_eq!(successes, budget);
        assert_eq!(manager.count(), budget);
    }

    #[tokio::test]
    async fn concurrent_reserve_and_remove() {
        // Test concurrent reserve and remove operations maintain counter consistency.
        let keypair = TransportKeypair::new();
        let pub_key = keypair.public().clone();
        let manager = Arc::new(TransientConnectionManager::new(
            100,
            Duration::from_secs(60),
        ));

        // First, reserve some peers.
        let initial_peers: Vec<_> = (0..10)
            .map(|i| make_peer(5000 + i, pub_key.clone()))
            .collect();
        for p in &initial_peers {
            assert!(manager.try_reserve(p.clone(), None));
        }
        assert_eq!(manager.count(), 10);

        // Concurrently remove and add peers.
        let new_peers: Vec<_> = (0..10)
            .map(|i| make_peer(6000 + i, pub_key.clone()))
            .collect();

        let remove_handles: Vec<_> = initial_peers
            .iter()
            .map(|p| {
                let mgr = manager.clone();
                let peer = p.clone();
                tokio::spawn(async move { mgr.remove(&peer) })
            })
            .collect();

        let add_handles: Vec<_> = new_peers
            .iter()
            .map(|p| {
                let mgr = manager.clone();
                let peer = p.clone();
                tokio::spawn(async move { mgr.try_reserve(peer, None) })
            })
            .collect();

        futures::future::join_all(remove_handles).await;
        futures::future::join_all(add_handles).await;

        // Counter should equal actual entries.
        let actual_count = manager.entries.len();
        assert_eq!(manager.count(), actual_count);
    }
}
