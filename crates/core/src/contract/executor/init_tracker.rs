//! Tracks contract initialization state to handle race conditions between PUT and UPDATE operations.
//!
//! When a new contract is being stored (PUT), we need to validate its state before accepting
//! any UPDATE operations. This module provides a state machine to track which contracts are
//! currently being initialized and queue any operations that arrive during that window.

use std::collections::HashMap;
use std::time::{Duration, Instant};

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
    /// When this operation was queued
    pub queued_at: Instant,
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

/// Tracks the initialization state of contracts
#[derive(Debug)]
pub(crate) struct ContractInitTracker {
    states: HashMap<ContractKey, InitState>,
}

#[derive(Debug)]
struct InitState {
    queued_ops: Vec<QueuedOperation>,
    started_at: Instant,
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
    pub fn check_and_maybe_queue(
        &mut self,
        key: &ContractKey,
        has_code: bool,
        update: Either<WrappedState, StateDelta<'static>>,
        related_contracts: RelatedContracts<'static>,
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
            queued_at: Instant::now(),
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
    pub fn start_initialization(&mut self, key: ContractKey) {
        self.states.insert(
            key,
            InitState {
                queued_ops: Vec::new(),
                started_at: Instant::now(),
            },
        );
    }

    /// Mark initialization as complete and return any queued operations.
    ///
    /// Returns `None` if the contract wasn't being initialized.
    pub fn complete_initialization(&mut self, key: &ContractKey) -> Option<InitCompletionInfo> {
        self.states.remove(key).map(|state| InitCompletionInfo {
            queued_ops: state.queued_ops,
            init_duration: state.started_at.elapsed(),
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
    pub fn cleanup_stale_initializations(&mut self, max_age: Duration) -> Vec<StaleInitInfo> {
        let now = Instant::now();
        let stale_keys: Vec<_> = self
            .states
            .iter()
            .filter_map(|(key, state)| {
                let age = now.duration_since(state.started_at);
                if age > max_age {
                    Some((*key, age, state.queued_ops.len()))
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
        );

        assert!(matches!(result, InitCheckResult::NotInitializing));
    }

    #[test]
    fn test_put_during_init_returns_error() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        let state = make_test_state(&[1, 2, 3]);
        let result = tracker.check_and_maybe_queue(
            &key,
            true, // has_code = true means this is a PUT
            Either::Left(state),
            RelatedContracts::default(),
        );

        assert!(matches!(result, InitCheckResult::PutDuringInit));
    }

    #[test]
    fn test_update_during_init_is_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        let state = make_test_state(&[1, 2, 3]);
        let result = tracker.check_and_maybe_queue(
            &key,
            false, // has_code = false means this is an UPDATE
            Either::Left(state),
            RelatedContracts::default(),
        );

        assert!(matches!(result, InitCheckResult::Queued { queue_size: 1 }));
        assert_eq!(tracker.queued_count(&key), 1);
    }

    #[test]
    fn test_multiple_updates_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        for i in 0..3 {
            let state = make_test_state(&[i]);
            let result = tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
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

        tracker.start_initialization(key);

        // Queue some operations
        for i in 0..2 {
            let state = make_test_state(&[i]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
            );
        }

        let completion = tracker.complete_initialization(&key).unwrap();

        assert_eq!(completion.queued_ops.len(), 2);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_fail_initialization_drops_queued_ops() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        // Queue some operations
        for i in 0..3 {
            let state = make_test_state(&[i]);
            tracker.check_and_maybe_queue(
                &key,
                false,
                Either::Left(state),
                RelatedContracts::default(),
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

        assert!(tracker.complete_initialization(&key).is_none());
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

        tracker.start_initialization(key);
        assert!(tracker.is_initializing(&key));

        tracker.complete_initialization(&key);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_delta_update_can_be_queued() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        let delta = StateDelta::from(vec![10, 20, 30]);
        let result = tracker.check_and_maybe_queue(
            &key,
            false,
            Either::Right(delta),
            RelatedContracts::default(),
        );

        assert!(matches!(result, InitCheckResult::Queued { queue_size: 1 }));

        let completion = tracker.complete_initialization(&key).unwrap();
        assert!(matches!(completion.queued_ops[0].update, Either::Right(_)));
    }

    #[test]
    fn test_cleanup_stale_removes_old_entries() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        // Queue an operation
        let state = make_test_state(&[1, 2, 3]);
        tracker.check_and_maybe_queue(
            &key,
            false,
            Either::Left(state),
            RelatedContracts::default(),
        );

        // With zero duration threshold, everything is stale
        let stale = tracker.cleanup_stale_initializations(Duration::ZERO);

        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].key, key);
        assert_eq!(stale[0].dropped_ops, 1);
        assert!(!tracker.is_initializing(&key));
    }

    #[test]
    fn test_cleanup_stale_keeps_fresh_entries() {
        let mut tracker = ContractInitTracker::new();
        let key = make_test_key();

        tracker.start_initialization(key);

        // With very long threshold, nothing is stale
        let stale = tracker.cleanup_stale_initializations(Duration::from_secs(3600));

        assert!(stale.is_empty());
        assert!(tracker.is_initializing(&key));
    }

    #[test]
    fn test_cleanup_stale_multiple_contracts() {
        let mut tracker = ContractInitTracker::new();

        let key1 = make_contract_key_with_code(&[1]);
        let key2 = make_contract_key_with_code(&[2]);
        let key3 = make_contract_key_with_code(&[3]);

        tracker.start_initialization(key1);
        tracker.start_initialization(key2);
        tracker.start_initialization(key3);

        assert_eq!(tracker.initializing_count(), 3);

        // Clean up all with zero threshold
        let stale = tracker.cleanup_stale_initializations(Duration::ZERO);

        assert_eq!(stale.len(), 3);
        assert_eq!(tracker.initializing_count(), 0);
    }

    #[test]
    fn test_initializing_count() {
        let mut tracker = ContractInitTracker::new();

        assert_eq!(tracker.initializing_count(), 0);

        let key1 = make_contract_key_with_code(&[1]);
        let key2 = make_contract_key_with_code(&[2]);

        tracker.start_initialization(key1);
        assert_eq!(tracker.initializing_count(), 1);

        tracker.start_initialization(key2);
        assert_eq!(tracker.initializing_count(), 2);

        tracker.complete_initialization(&key1);
        assert_eq!(tracker.initializing_count(), 1);
    }

    fn make_contract_key_with_code(code_bytes: &[u8]) -> ContractKey {
        let code = ContractCode::from(code_bytes.to_vec());
        let params = Parameters::from(vec![5, 6, 7, 8]);
        ContractKey::from_params_and_code(&params, &code)
    }
}
