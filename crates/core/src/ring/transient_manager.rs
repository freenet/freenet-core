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
        if let Some(mut entry) = self.entries.get_mut(&peer) {
            entry.location = location;
            return true;
        }

        let current = self.in_use.load(Ordering::Acquire);
        if current >= self.budget {
            return false;
        }

        let key = peer.clone();
        self.entries.insert(peer, TransientEntry { location });
        let prev = self.in_use.fetch_add(1, Ordering::SeqCst);
        if prev >= self.budget {
            // Undo if we raced past the budget.
            self.entries.remove(&key);
            self.in_use.fetch_sub(1, Ordering::SeqCst);
            return false;
        }

        true
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
