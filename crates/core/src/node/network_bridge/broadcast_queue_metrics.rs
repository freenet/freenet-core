//! Fixed-cardinality diagnostics for the production broadcast scheduler.
//!
//! These are lifetime counters sampled by the existing five-minute
//! `router_snapshot` task and exported in the wide diagnostic every 30 minutes.
//! They deliberately contain no peer or contract identifiers, and recording
//! them never changes queue behavior.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// Cumulative production-queue measurements used to decide the scheduler fix.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub(crate) struct BroadcastQueueEfficiencySnapshot {
    pub capacity_evictions: u64,
    pub dedup_replacements: u64,
    pub enqueues_while_pair_active: u64,
    pub large_head_blocking_incidents: u64,
    pub large_head_blocked_millis: u64,
    pub small_entry_millis_blocked: u64,
    pub queued_large_actual_small: u64,
    pub queued_small_actual_large: u64,
    pub queued_large_actual_small_bytes: u64,
    pub queued_small_actual_large_bytes: u64,
    pub scheduled_small: u64,
    pub scheduled_large: u64,
    pub scheduled_small_state_bytes: u64,
    pub scheduled_large_state_bytes: u64,
    pub active_tracking_overflow: u64,
}

/// Lock-free sink shared by the queue's enqueue, scheduling, and delivery paths.
#[derive(Debug, Default)]
pub(crate) struct BroadcastQueueEfficiencyMetrics {
    capacity_evictions: AtomicU64,
    dedup_replacements: AtomicU64,
    enqueues_while_pair_active: AtomicU64,
    large_head_blocking_incidents: AtomicU64,
    large_head_blocked_millis: AtomicU64,
    small_entry_millis_blocked: AtomicU64,
    queued_large_actual_small: AtomicU64,
    queued_small_actual_large: AtomicU64,
    queued_large_actual_small_bytes: AtomicU64,
    queued_small_actual_large_bytes: AtomicU64,
    scheduled_small: AtomicU64,
    scheduled_large: AtomicU64,
    scheduled_small_state_bytes: AtomicU64,
    scheduled_large_state_bytes: AtomicU64,
    active_tracking_overflow: AtomicU64,
}

// The simulation feature replaces the production scheduler, so its mutation
// sites are intentionally absent while snapshots remain part of the node API.
#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
impl BroadcastQueueEfficiencyMetrics {
    pub(crate) fn record_capacity_eviction(&self) {
        self.capacity_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dedup_replacement(&self) {
        self.dedup_replacements.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_enqueue_while_active(&self) {
        self.enqueues_while_pair_active
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_large_head_block(
        &self,
        blocked_millis: u64,
        small_entry_millis_blocked: u64,
    ) {
        self.large_head_blocking_incidents
            .fetch_add(1, Ordering::Relaxed);
        self.large_head_blocked_millis
            .fetch_add(blocked_millis, Ordering::Relaxed);
        self.small_entry_millis_blocked
            .fetch_add(small_entry_millis_blocked, Ordering::Relaxed);
    }

    pub(crate) fn record_scheduled(&self, large: bool, state_bytes: usize) {
        let state_bytes = u64::try_from(state_bytes).unwrap_or(u64::MAX);
        let (count, bytes) = if large {
            (&self.scheduled_large, &self.scheduled_large_state_bytes)
        } else {
            (&self.scheduled_small, &self.scheduled_small_state_bytes)
        };
        count.fetch_add(1, Ordering::Relaxed);
        bytes.fetch_add(state_bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_queued_large_actual_small(&self, payload_bytes: usize) {
        self.queued_large_actual_small
            .fetch_add(1, Ordering::Relaxed);
        self.queued_large_actual_small_bytes.fetch_add(
            u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_queued_small_actual_large(&self, payload_bytes: usize) {
        self.queued_small_actual_large
            .fetch_add(1, Ordering::Relaxed);
        self.queued_small_actual_large_bytes.fetch_add(
            u64::try_from(payload_bytes).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_active_tracking_overflow(&self) {
        self.active_tracking_overflow
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> BroadcastQueueEfficiencySnapshot {
        BroadcastQueueEfficiencySnapshot {
            capacity_evictions: self.capacity_evictions.load(Ordering::Relaxed),
            dedup_replacements: self.dedup_replacements.load(Ordering::Relaxed),
            enqueues_while_pair_active: self.enqueues_while_pair_active.load(Ordering::Relaxed),
            large_head_blocking_incidents: self
                .large_head_blocking_incidents
                .load(Ordering::Relaxed),
            large_head_blocked_millis: self.large_head_blocked_millis.load(Ordering::Relaxed),
            small_entry_millis_blocked: self.small_entry_millis_blocked.load(Ordering::Relaxed),
            queued_large_actual_small: self.queued_large_actual_small.load(Ordering::Relaxed),
            queued_small_actual_large: self.queued_small_actual_large.load(Ordering::Relaxed),
            queued_large_actual_small_bytes: self
                .queued_large_actual_small_bytes
                .load(Ordering::Relaxed),
            queued_small_actual_large_bytes: self
                .queued_small_actual_large_bytes
                .load(Ordering::Relaxed),
            scheduled_small: self.scheduled_small.load(Ordering::Relaxed),
            scheduled_large: self.scheduled_large.load(Ordering::Relaxed),
            scheduled_small_state_bytes: self.scheduled_small_state_bytes.load(Ordering::Relaxed),
            scheduled_large_state_bytes: self.scheduled_large_state_bytes.load(Ordering::Relaxed),
            active_tracking_overflow: self.active_tracking_overflow.load(Ordering::Relaxed),
        }
    }
}

/// Production has one queue per process. Simulation uses a separate fan-out
/// path, matching the existing `BROADCAST_STREAM_METRICS` scope.
pub(crate) static BROADCAST_QUEUE_EFFICIENCY_METRICS: BroadcastQueueEfficiencyMetrics =
    BroadcastQueueEfficiencyMetrics {
        capacity_evictions: AtomicU64::new(0),
        dedup_replacements: AtomicU64::new(0),
        enqueues_while_pair_active: AtomicU64::new(0),
        large_head_blocking_incidents: AtomicU64::new(0),
        large_head_blocked_millis: AtomicU64::new(0),
        small_entry_millis_blocked: AtomicU64::new(0),
        queued_large_actual_small: AtomicU64::new(0),
        queued_small_actual_large: AtomicU64::new(0),
        queued_large_actual_small_bytes: AtomicU64::new(0),
        queued_small_actual_large_bytes: AtomicU64::new(0),
        scheduled_small: AtomicU64::new(0),
        scheduled_large: AtomicU64::new(0),
        scheduled_small_state_bytes: AtomicU64::new(0),
        scheduled_large_state_bytes: AtomicU64::new(0),
        active_tracking_overflow: AtomicU64::new(0),
    };

#[cfg(test)]
mod tests {
    use super::BroadcastQueueEfficiencyMetrics;

    #[test]
    fn snapshot_reconciles_all_queue_harm_counters() {
        let metrics = BroadcastQueueEfficiencyMetrics::default();
        metrics.record_capacity_eviction();
        metrics.record_dedup_replacement();
        metrics.record_enqueue_while_active();
        metrics.record_large_head_block(250, 750);
        metrics.record_scheduled(false, 10);
        metrics.record_scheduled(true, 20);
        metrics.record_queued_large_actual_small(30);
        metrics.record_queued_small_actual_large(40);
        metrics.record_active_tracking_overflow();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.capacity_evictions, 1);
        assert_eq!(snapshot.dedup_replacements, 1);
        assert_eq!(snapshot.enqueues_while_pair_active, 1);
        assert_eq!(snapshot.large_head_blocking_incidents, 1);
        assert_eq!(snapshot.large_head_blocked_millis, 250);
        assert_eq!(snapshot.small_entry_millis_blocked, 750);
        assert_eq!(snapshot.queued_large_actual_small, 1);
        assert_eq!(snapshot.queued_small_actual_large, 1);
        assert_eq!(snapshot.queued_large_actual_small_bytes, 30);
        assert_eq!(snapshot.queued_small_actual_large_bytes, 40);
        assert_eq!(snapshot.scheduled_small, 1);
        assert_eq!(snapshot.scheduled_large, 1);
        assert_eq!(snapshot.scheduled_small_state_bytes, 10);
        assert_eq!(snapshot.scheduled_large_state_bytes, 20);
        assert_eq!(snapshot.active_tracking_overflow, 1);
    }
}
