//! Tracks contract initialization state to handle race conditions between PUT and UPDATE operations.
//!
//! When a new contract is being stored (PUT), we need to validate its state before accepting
//! any UPDATE operations. This module provides a state machine to track which contracts are
//! currently being initialized and queue any operations that arrive during that window.
//!
//! # Determinism
//!
//! This module never calls `Instant::now()` or any wall-clock function internally.
//! All time values are passed in as `u64` nanoseconds by the caller, which obtains
//! them from the appropriate `TimeSource` (or `tokio::time::Instant` under turmoil).
//! This makes the tracker fully deterministic for simulation testing (DST).

use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// Returns the current time as nanoseconds elapsed since `tokio::time::Instant`'s epoch.
///
/// This uses `tokio::time::Instant` (NOT `std::time::Instant`) which is deterministic
/// under both turmoil and `start_paused = true` tokio runtimes. All callers of
/// `ContractInitTracker` methods should use this function to obtain time values.
pub(crate) fn now_nanos() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    Instant::now().duration_since(*epoch).as_nanos() as u64
}

/// Initialization taking longer than this logs a warning
pub(crate) const SLOW_INIT_THRESHOLD: Duration = Duration::from_secs(1);

/// Initializations older than this are considered stale and will be cleaned up.
pub(crate) const STALE_INIT_THRESHOLD: Duration = Duration::from_secs(30);

use either::Either;
use freenet_stdlib::prelude::*;

/// Result of checking whether a contract is being initialized
#[derive(Debug)]
pub(crate) enum InitCheckResult {
    /// Contract is not being initialized, proceed normally
    NotInitializing,
    /// Operation was queued because contract is initializing
    /// Contains the current queue size after adding this operation
    Queued { queue_size: usize },
    /// Cannot perform PUT while contract is already initializing
    PutDuringInit,
}

/// A queued operation waiting for contract initialization to complete
#[derive(Debug)]
pub(crate) struct QueuedOperation {
    pub update: Either<WrappedState, StateDelta<'static>>,
    pub related_contracts: RelatedContracts<'static>,
    /// When this operation was queued, as nanoseconds from the caller's time source
    pub queued_at_nanos: u64,
}

/// Information about completed initialization
#[derive(Debug)]
pub(crate) struct InitCompletionInfo {
    /// Operations that were queued during initialization
    pub queued_ops: Vec<QueuedOperation>,
    /// How long initialization took
    pub init_duration: Duration,
}

/// Information about a stale initialization that was cleaned up.
#[derive(Debug)]
pub(crate) struct StaleInitInfo {
    /// The contract key that was stale
    pub key: ContractKey,
    /// How long it had been initializing
    pub age: Duration,
    /// Number of queued operations that were dropped
    pub dropped_ops: usize,
}

/// Tracks the initialization state of contracts.
///
/// All time-dependent operations accept a `now_nanos` parameter from the caller
/// rather than reading the clock internally, ensuring deterministic behavior
/// in simulation tests.
#[derive(Debug)]
pub(crate) struct ContractInitTracker {
    states: HashMap<ContractKey, InitState>,
}

#[derive(Debug)]
struct InitState {
    queued_ops: Vec<QueuedOperation>,
    /// When initialization started, as nanoseconds from the caller's time source
    started_at_nanos: u64,
}

impl Default for ContractInitTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ContractInitTracker {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Check if a contract is being initialized and handle the incoming operation accordingly.
    ///
    /// - If not initializing, returns `NotInitializing` and the caller should proceed normally.
    /// - If initializing and this is an UPDATE (no code), queues the operation and returns `Queued`.
    /// - If initializing and this is a PUT (has code), returns `PutDuringInit` error.
    ///
    /// `now_nanos` is the current time from the caller's time source.
    pub fn check_and_maybe_queue(
        &mut self,
        key: &ContractKey,
        has_code: bool,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
        now_nanos: u64,
    ) -> InitCheckResult {
        let Some(state) = self.states.get_mut(key) else {
            return InitCheckResult::NotInitializing;
        };

        // Cannot PUT while already initializing
        if has_code {
            return InitCheckResult::PutDuringInit;
        }

        // Queue the UPDATE operation
        state.queued_ops.push(QueuedOperation {
            update,
            related_contracts,
            queued_at_nanos: now_nanos,
        });

        InitCheckResult::Queued {
            queue_size: state.queued_ops.len(),
        }
    }

    /// Returns true if the contract is currently being initialized
    #[allow(dead_code)] // Used in tests
    pub fn is_initializing(&self, key: &ContractKey) -> bool {
        self.states.contains_key(key)
    }

    /// Mark a contract as starting initialization.
    ///
    /// This should be called when a new contract is being stored for the first time.
    /// `now_nanos` is the current time from the caller's time source.
    pub fn start_initialization(&mut self, key: ContractKey, now_nanos: u64) {
        self.states.insert(
            key,
            InitState {
                queued_ops: Vec::new(),
                started_at_nanos: now_nanos,
            },
        );
    }

    /// Mark initialization as complete and return any queued operations.
    ///
    /// Returns `None` if the contract wasn't being initialized.
    /// `now_nanos` is the current time from the caller's time source.
    pub fn complete_initialization(
        &mut self,
        key: &ContractKey,
        now_nanos: u64,
    ) -> Option<InitCompletionInfo> {
        self.states.remove(key).map(|state| {
            let elapsed_nanos = now_nanos.saturating_sub(state.started_at_nanos);
            InitCompletionInfo {
                queued_ops: state.queued_ops,
                init_duration: Duration::from_nanos(elapsed_nanos),
            }
        })
    }

    /// Mark initialization as failed and drop any queued operations.
    ///
    /// Returns the number of operations that were dropped, or `None` if not initializing.
    pub fn fail_initialization(&mut self, key: &ContractKey) -> Option<usize> {
        self.states.remove(key).map(|state| state.queued_ops.len())
    }

    /// Get the number of queued operations for a contract being initialized
    #[allow(dead_code)] // Used in tests
    pub fn queued_count(&self, key: &ContractKey) -> usize {
        self.states
            .get(key)
            .map(|s| s.queued_ops.len())
            .unwrap_or(0)
    }

    /// Remove stale initializations that have been running longer than `max_age`.
    ///
    /// Returns information about each stale initialization that was cleaned up.
    /// This should be called periodically to prevent resource leaks from
    /// initializations that never complete (e.g., due to bugs or crashes).
    ///
    /// `now_nanos` is the current time from the caller's time source.
    pub fn cleanup_stale_initializations(
        &mut self,
        max_age: Duration,
        now_nanos: u64,
    ) -> Vec<StaleInitInfo> {
        let max_age_nanos = max_age.as_nanos() as u64;
        let stale_keys: Vec<_> = self
            .states
            .iter()
            .filter_map(|(key, state)| {
                let age_nanos = now_nanos.saturating_sub(state.started_at_nanos);
                if age_nanos > max_age_nanos {
                    Some((
                        *key,
                        Duration::from_nanos(age_nanos),
                        state.queued_ops.len(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        stale_keys
            .into_iter()
            .map(|(key, age, dropped_ops)| {
                self.states.remove(&key);
                StaleInitInfo {
                    key,
                    age,
                    dropped_ops,
                }
            })
            .collect()
    }

    /// Get the number of contracts currently being initialized
    #[allow(dead_code)] // Useful for monitoring
    pub fn initializing_count(&self) -> usize {
        self.states.len()
    }

    /// Compute how long a queued operation has been waiting.
    ///
    /// `now_nanos` is the current time from the caller's time source.
    pub fn queue_wait_duration(op: &QueuedOperation, now_nanos: u64) -> Duration {
        Duration::from_nanos(now_nanos.saturating_sub(op.queued_at_nanos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_key() -> ContractKey {
        let code = ContractCode::from(vec![1, 2, 3, 4]);
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }

    fn make_test_state(data: &[u8]) -> WrappedState {
        WrappedState::new(data.to_vec())
    }

    #[test]
    fn test_not_initializing_returns_not_initializing() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();
        let state = make_test_state(&[1, 2, 3]);

        let result = tracker.check_and_maybe_queue(
            &key,
            false,
            Either::Left(state),
            RelatedContracts::default(),
            1_000_000,
        );

        assert!(matches!(result, InitCheckResult::NotInitializing));
    }

    #[test]
    fn test_put_during_init_returns_error() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        let state = make_test_state(&[1, 2, 3]);
        let result = tracker.check_and_maybe_queue(
            &key,
            true, // has_code = true means this is a PUT
            Either::Left(state),
            RelatedContracts::default(),
            2_000_000,
        );

        assert!(matches!(result, InitCheckResult::PutDuringInit));
    }

    #[test]
    fn test_update_during_init_is_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        let state = make_test_state(&[1, 2, 3]);
        let result = tracker.check_and_maybe_queue(
            &key,
            false, // has_code = false means this is an UPDATE
            Either::Left(state),
            RelatedContracts::default(),
            2_000_000,
        );

        assert!(matches!(result, InitCheckResult::Queued { queue_size: 1 }));
        assert_eq!(tracker.queued_count(&key), 1);
    }

    #[test]
    fn test_multiple_updates_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        for i in 0..3 {
            let state = make_test_state(&[i]);
            let now = 2_000_000 + (i as u64) * 1_000_000;
            let result = tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
                now,
            );
            assert!(matches!(
                result,
                InitCheckResult::Queued { queue_size } if queue_size == (i as usize + 1)
            ));
        }

        assert_eq!(tracker.queued_count(&key), 3);
    }

    #[test]
    fn test_complete_initialization_returns_queued_ops() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        // Queue some operations
        for i in 0..2 {
            let state = make_test_state(&[i]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
                2_000_000 + (i as u64) * 1_000_000,
            );
        }

        let completion = tracker.complete_initialization(&key, 10_000_000).unwrap();

        assert_eq!(completion.queued_ops.len(), 2);
        assert_eq!(completion.init_duration, Duration::from_nanos(9_000_000));
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_fail_initialization_drops_queued_ops() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        // Queue some operations
        for i in 0..3 {
            let state = make_test_state(&[i]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
                2_000_000 + (i as u64) * 1_000_000,
            );
        }

        let dropped_count = tracker.fail_initialization(&key).unwrap();

        assert_eq!(dropped_count, 3);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_complete_nonexistent_returns_none() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        assert!(tracker.complete_initialization(&key, 1_000_000).is_none());
    }

    #[test]
    fn test_fail_nonexistent_returns_none() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        assert!(tracker.fail_initialization(&key).is_none());
    }

    #[test]
    fn test_is_initializing() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        assert!(!tracker.is_initializing(&key));

        tracker.start_initialization(key, 1_000_000);
        assert!(tracker.is_initializing(&key));

        tracker.complete_initialization(&key, 2_000_000);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_delta_update_can_be_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key, 1_000_000);

        let delta = StateDelta::from(vec![10, 20, 30]);
        let result = tracker.check_and_maybe_queue(
            &key,
            false,
            Either::Right(delta),
            RelatedContracts::default(),
            2_000_000,
        );

        assert!(matches!(result, InitCheckResult::Queued { queue_size: 1 }));

        let completion = tracker.complete_initialization(&key, 3_000_000).unwrap();
        assert!(matches!(completion.queued_ops[0].update, Either::Right(_)));
    }

    #[test]
    fn test_cleanup_stale_removes_old_entries() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        // Start initialization at t=0
        tracker.start_initialization(key, 0);

        // Queue an operation
        let state = make_test_state(&[1, 2, 3]);
        tracker.check_and_maybe_queue(
            &key,
            false,
            Either::Left(state),
            RelatedContracts::default(),
            1_000_000,
        );

        // At t=31s, with 30s threshold, everything started at t=0 is stale
        let stale = tracker.cleanup_stale_initializations(Duration::from_secs(30), 31_000_000_000);

        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].key, key);
        assert_eq!(stale[0].dropped_ops, 1);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_cleanup_stale_keeps_fresh_entries() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        // Start initialization at t=100s
        let start_nanos = 100_000_000_000;
        tracker.start_initialization(key, start_nanos);

        // At t=101s, with 3600s threshold, nothing is stale
        let stale = tracker
            .cleanup_stale_initializations(Duration::from_secs(3600), start_nanos + 1_000_000_000);

        assert!(stale.is_empty());
        assert!(tracker.is_initializing(&key));
    }

    #[test]
    fn test_cleanup_stale_multiple_contracts() {
        let mut tracker = ContractInitTracker::new();

        let key1 = make_contract_key_with_code(&[1]);
        let key2 = make_contract_key_with_code(&[2]);
        let key3 = make_contract_key_with_code(&[3]);

        tracker.start_initialization(key1, 0);
        tracker.start_initialization(key2, 1_000_000);
        tracker.start_initialization(key3, 2_000_000);

        assert_eq!(tracker.initializing_count(), 3);

        // Clean up all with zero threshold at a later time
        let stale = tracker.cleanup_stale_initializations(Duration::ZERO, 10_000_000);

        assert_eq!(stale.len(), 3);
        assert_eq!(tracker.initializing_count(), 0);
    }

    #[test]
    fn test_initializing_count() {
        let mut tracker = ContractInitTracker::new();

        assert_eq!(tracker.initializing_count(), 0);

        let key1 = make_contract_key_with_code(&[1]);
        let key2 = make_contract_key_with_code(&[2]);

        tracker.start_initialization(key1, 1_000_000);
        assert_eq!(tracker.initializing_count(), 1);

        tracker.start_initialization(key2, 2_000_000);
        assert_eq!(tracker.initializing_count(), 2);

        tracker.complete_initialization(&key1, 3_000_000);
        assert_eq!(tracker.initializing_count(), 1);
    }

    /// Verify that queue_wait_duration computes correctly from nanos values
    #[test]
    fn test_queue_wait_duration() {
        let op = QueuedOperation {
            update: Either::Left(make_test_state(&[1])),
            related_contracts: RelatedContracts::default(),
            queued_at_nanos: 5_000_000_000, // 5 seconds
        };

        let wait = ContractInitTracker::queue_wait_duration(&op, 8_000_000_000); // 8 seconds
        assert_eq!(wait, Duration::from_secs(3));
    }

    /// Verify that init_duration is correctly computed as the difference between
    /// start and completion times
    #[test]
    fn test_init_duration_deterministic() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        let start = 1_000_000_000; // 1 second
        let end = 1_500_000_000; // 1.5 seconds
        tracker.start_initialization(key, start);
        let info = tracker.complete_initialization(&key, end).unwrap();

        assert_eq!(info.init_duration, Duration::from_millis(500));
    }

    /// Verify that the same sequence of operations always produces the same result
    /// regardless of when it's called (no wall-clock dependency).
    #[test]
    fn test_fully_deterministic_sequence() {
        // Run the same sequence twice with identical time values
        let run = || {
            let mut tracker = ContractInitTracker::new();
            let key = make_test_key();

            tracker.start_initialization(key, 100);

            let state = make_test_state(&[42]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
                200,
            );

            let stale = tracker.cleanup_stale_initializations(Duration::from_secs(1), 300);
            assert!(stale.is_empty(), "should not be stale yet");

            let completion = tracker.complete_initialization(&key, 400).unwrap();
            (completion.init_duration, completion.queued_ops.len())
        };

        let (dur1, ops1) = run();
        let (dur2, ops2) = run();
        assert_eq!(dur1, dur2);
        assert_eq!(ops1, ops2);
        assert_eq!(dur1, Duration::from_nanos(300));
    }

    /// Behavioral test: init_tracker produces identical results when given identical
    /// time inputs, regardless of which thread or when the test runs.
    /// This is the core guarantee that makes the tracker DST-compatible.
    #[test]
    fn test_deterministic_stale_cleanup_with_explicit_time() {
        // Two runs with identical time values must produce identical results
        let run = |start: u64, queue_time: u64, cleanup_time: u64, max_age_secs: u64| {
            let mut tracker = ContractInitTracker::new();
            let key = make_test_key();

            tracker.start_initialization(key, start);

            let state = make_test_state(&[99]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
                queue_time,
            );

            let stale = tracker
                .cleanup_stale_initializations(Duration::from_secs(max_age_secs), cleanup_time);
            (stale.len(), tracker.is_initializing(&key))
        };

        // With cleanup at t=5s and max_age=10s, init at t=0 is NOT stale
        let (stale1, init1) = run(0, 1_000_000_000, 5_000_000_000, 10);
        let (stale2, init2) = run(0, 1_000_000_000, 5_000_000_000, 10);
        assert_eq!((stale1, init1), (0, true));
        assert_eq!((stale1, init1), (stale2, init2));

        // With cleanup at t=15s and max_age=10s, init at t=0 IS stale
        let (stale3, init3) = run(0, 1_000_000_000, 15_000_000_000, 10);
        let (stale4, init4) = run(0, 1_000_000_000, 15_000_000_000, 10);
        assert_eq!((stale3, init3), (1, false));
        assert_eq!((stale3, init3), (stale4, init4));
    }

    /// Behavioral test: queue_wait_duration is a pure function of the input nanos values
    #[test]
    fn test_queue_wait_duration_is_pure() {
        let make_op = |queued_at: u64| QueuedOperation {
            update: Either::Left(make_test_state(&[1])),
            related_contracts: RelatedContracts::default(),
            queued_at_nanos: queued_at,
        };

        // Same inputs always produce same output
        let op = make_op(1_000_000_000);
        let d1 = ContractInitTracker::queue_wait_duration(&op, 3_000_000_000);
        let d2 = ContractInitTracker::queue_wait_duration(&op, 3_000_000_000);
        assert_eq!(d1, d2);
        assert_eq!(d1, Duration::from_secs(2));

        // Saturating subtraction: if now < queued_at, duration is zero (no panic)
        let d3 = ContractInitTracker::queue_wait_duration(&op, 500_000_000);
        assert_eq!(d3, Duration::ZERO);
    }

    fn make_contract_key_with_code(code_bytes: &[u8]) -> ContractKey {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }
}
