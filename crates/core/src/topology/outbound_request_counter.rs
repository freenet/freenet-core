use crate::ring::PeerKeyLocation;
use std::collections::{HashMap, VecDeque};

pub(crate) struct OutboundRequestCounter {
    window_size: usize,
    request_window: VecDeque<PeerKeyLocation>,
    counts_by_peer: HashMap<PeerKeyLocation, usize>,
    total_count: usize,
}

impl OutboundRequestCounter {
    pub(crate) fn new(window_size: usize) -> Self {
        OutboundRequestCounter {
            window_size,
            request_window: VecDeque::new(),
            counts_by_peer: HashMap::new(),
            total_count: 0,
        }
    }

    pub(crate) fn record_request(&mut self, peer: PeerKeyLocation) {
        self.request_window.push_back(peer.clone());
        self.counts_by_peer
            .entry(peer)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        self.total_count += 1;
        while self.request_window.len() > self.window_size {
            let removed_request = self.request_window.pop_front().unwrap();
            let count = self.counts_by_peer.get_mut(&removed_request).unwrap();
            *count -= 1;
            self.total_count -= 1;
            if *count == 0 {
                self.counts_by_peer.remove(&removed_request);
            }
        }
    }

    pub(crate) fn get_request_count(&self, peer: &PeerKeyLocation) -> usize {
        self.counts_by_peer.get(peer).copied().unwrap_or(0)
    }
}
