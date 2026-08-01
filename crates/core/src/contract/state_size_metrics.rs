//! Fixed-cardinality counters for executor hard-limit rejections.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy)]
pub(crate) enum StateSizeRejectionStage {
    /// An incoming full state was rejected before contract WASM ran.
    PreWasmFullState,
    /// A merged state was rejected at the canonical commit chokepoint.
    PostMergeCommit,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StateSizeRejectionSnapshot {
    pub pre_wasm_count: u64,
    pub pre_wasm_max_bytes: u64,
    pub post_merge_count: u64,
    pub post_merge_max_bytes: u64,
}

#[derive(Debug, Default)]
struct StateSizeRejectionMetrics {
    pre_wasm_count: AtomicU64,
    pre_wasm_max_bytes: AtomicU64,
    post_merge_count: AtomicU64,
    post_merge_max_bytes: AtomicU64,
}

impl StateSizeRejectionMetrics {
    fn record(&self, stage: StateSizeRejectionStage, size_bytes: usize) {
        let size_bytes = u64::try_from(size_bytes).unwrap_or(u64::MAX);
        let (count, max) = match stage {
            StateSizeRejectionStage::PreWasmFullState => {
                (&self.pre_wasm_count, &self.pre_wasm_max_bytes)
            }
            StateSizeRejectionStage::PostMergeCommit => {
                (&self.post_merge_count, &self.post_merge_max_bytes)
            }
        };
        count.fetch_add(1, Ordering::Relaxed);
        max.fetch_max(size_bytes, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StateSizeRejectionSnapshot {
        StateSizeRejectionSnapshot {
            pre_wasm_count: self.pre_wasm_count.load(Ordering::Relaxed),
            pre_wasm_max_bytes: self.pre_wasm_max_bytes.load(Ordering::Relaxed),
            post_merge_count: self.post_merge_count.load(Ordering::Relaxed),
            post_merge_max_bytes: self.post_merge_max_bytes.load(Ordering::Relaxed),
        }
    }
}

static STATE_SIZE_REJECTION_METRICS: StateSizeRejectionMetrics = StateSizeRejectionMetrics {
    pre_wasm_count: AtomicU64::new(0),
    pre_wasm_max_bytes: AtomicU64::new(0),
    post_merge_count: AtomicU64::new(0),
    post_merge_max_bytes: AtomicU64::new(0),
};

pub(crate) fn record_state_size_rejection(stage: StateSizeRejectionStage, size_bytes: usize) {
    STATE_SIZE_REJECTION_METRICS.record(stage, size_bytes);
}

pub(crate) fn state_size_rejection_snapshot() -> StateSizeRejectionSnapshot {
    STATE_SIZE_REJECTION_METRICS.snapshot()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_maxima_are_separate_by_stage() {
        let metrics = StateSizeRejectionMetrics::default();
        metrics.record(StateSizeRejectionStage::PreWasmFullState, 12);
        metrics.record(StateSizeRejectionStage::PreWasmFullState, 8);
        metrics.record(StateSizeRejectionStage::PostMergeCommit, 20);

        assert_eq!(
            metrics.snapshot(),
            StateSizeRejectionSnapshot {
                pre_wasm_count: 2,
                pre_wasm_max_bytes: 12,
                post_merge_count: 1,
                post_merge_max_bytes: 20,
            }
        );
    }
}
