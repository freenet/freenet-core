//! Tests for WASM execution handling edge cases.
//!
//! These tests verify the behavior of the sync polling loop in `handle_execution_call`,
//! including timeout handling, panic recovery, and store management.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Simulates the polling loop behavior without actual WASM execution.
/// This allows us to test the timeout and completion detection logic.
fn simulate_execution_polling(
    thread_handle: JoinHandle<Result<i64, &'static str>>,
    max_execution_seconds: f64,
) -> Result<i64, TestError> {
    let timeout = Duration::from_secs_f64(max_execution_seconds);
    let start = Instant::now();

    loop {
        if thread_handle.is_finished() {
            break;
        }

        if start.elapsed() >= timeout {
            return Err(TestError::Timeout);
        }

        thread::sleep(Duration::from_millis(10));
    }

    thread_handle
        .join()
        .map_err(|_| TestError::Panic)?
        .map_err(|_| TestError::ExecutionFailed)
}

#[derive(Debug, PartialEq)]
enum TestError {
    Timeout,
    Panic,
    ExecutionFailed,
}

// =============================================================================
// Timeout Tests
// =============================================================================

#[test]
fn test_timeout_triggers_correctly() {
    // Thread that runs forever
    let handle = thread::spawn(|| -> Result<i64, &'static str> {
        loop {
            thread::sleep(Duration::from_millis(100));
        }
    });

    let start = Instant::now();
    let result = simulate_execution_polling(handle, 0.1); // 100ms timeout
    let elapsed = start.elapsed();

    assert_eq!(result, Err(TestError::Timeout));
    // Should timeout around 100ms, allow some variance
    assert!(
        elapsed >= Duration::from_millis(100) && elapsed < Duration::from_millis(200),
        "Elapsed: {:?}",
        elapsed
    );
}

#[test]
fn test_very_short_timeout() {
    let handle = thread::spawn(|| -> Result<i64, &'static str> {
        thread::sleep(Duration::from_secs(10));
        Ok(42)
    });

    let result = simulate_execution_polling(handle, 0.02); // 20ms timeout
    assert_eq!(result, Err(TestError::Timeout));
}

#[test]
fn test_completion_just_before_timeout() {
    // Thread that completes just before timeout
    let handle = thread::spawn(|| -> Result<i64, &'static str> {
        thread::sleep(Duration::from_millis(40));
        Ok(42)
    });

    let result = simulate_execution_polling(handle, 0.1); // 100ms timeout
    assert_eq!(result, Ok(42));
}

#[test]
fn test_immediate_completion() {
    let handle = thread::spawn(|| -> Result<i64, &'static str> { Ok(123) });

    let result = simulate_execution_polling(handle, 1.0);
    assert_eq!(result, Ok(123));
}

// =============================================================================
// Panic Handling Tests
// =============================================================================

#[test]
fn test_thread_panic_detected() {
    let handle = thread::spawn(|| -> Result<i64, &'static str> { panic!("intentional panic") });

    // Give thread time to panic
    thread::sleep(Duration::from_millis(50));

    let result = simulate_execution_polling(handle, 1.0);
    assert_eq!(result, Err(TestError::Panic));
}

#[test]
fn test_thread_panic_with_string_message() {
    let handle = thread::spawn(|| -> Result<i64, &'static str> {
        panic!("panic with String message: {}", 42)
    });

    thread::sleep(Duration::from_millis(50));
    let result = simulate_execution_polling(handle, 1.0);
    assert_eq!(result, Err(TestError::Panic));
}

#[test]
fn test_thread_panic_with_custom_type() {
    struct CustomPanic;
    let handle = thread::spawn(|| -> Result<i64, &'static str> {
        std::panic::panic_any(CustomPanic);
    });

    thread::sleep(Duration::from_millis(50));
    let result = simulate_execution_polling(handle, 1.0);
    assert_eq!(result, Err(TestError::Panic));
}

// =============================================================================
// Concurrent Execution Tests
// =============================================================================

#[test]
fn test_multiple_concurrent_executions() {
    let completed = Arc::new(AtomicU32::new(0));
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let completed = completed.clone();
            thread::spawn(move || {
                let handle = thread::spawn(move || -> Result<i64, &'static str> {
                    thread::sleep(Duration::from_millis(50 + i * 10));
                    Ok(i as i64)
                });

                let result = simulate_execution_polling(handle, 1.0);
                if result.is_ok() {
                    completed.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(completed.load(Ordering::SeqCst), 4);
}

#[test]
fn test_rapid_successive_executions() {
    for i in 0..10 {
        let handle = thread::spawn(move || -> Result<i64, &'static str> {
            thread::sleep(Duration::from_millis(5));
            Ok(i)
        });

        let result = simulate_execution_polling(handle, 0.5);
        assert_eq!(result, Ok(i));
    }
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_zero_timeout() {
    let handle = thread::spawn(|| -> Result<i64, &'static str> { Ok(42) });

    // Zero timeout should immediately timeout (or catch already-finished thread)
    let result = simulate_execution_polling(handle, 0.0);
    // Either timeout immediately or catch the completed thread
    // (race condition - both are valid outcomes)
    assert!(result == Err(TestError::Timeout) || result == Ok(42));
}

// Note: Negative timeout values cause Duration::from_secs_f64 to panic.
// The actual implementation should never receive negative values, so we
// don't test that case. In real code, RuntimeConfig::max_execution_seconds
// defaults to 5.0 and negative values would be configuration errors.

#[test]
fn test_thread_finishes_exactly_at_timeout_boundary() {
    // This tests the race condition at the timeout boundary
    let finished = Arc::new(AtomicBool::new(false));
    let finished_clone = finished.clone();

    let handle = thread::spawn(move || -> Result<i64, &'static str> {
        thread::sleep(Duration::from_millis(100));
        finished_clone.store(true, Ordering::SeqCst);
        Ok(42)
    });

    let result = simulate_execution_polling(handle, 0.1); // Exactly 100ms

    // At the boundary, either outcome is acceptable
    match result {
        Ok(42) => assert!(finished.load(Ordering::SeqCst)),
        Err(TestError::Timeout) => {
            // Thread may or may not have finished
        }
        other => panic!("Unexpected result: {:?}", other),
    }
}

// =============================================================================
// Store Recovery Tests
// =============================================================================

/// Simulates the store recovery pattern used in handle_execution_call
#[test]
fn test_store_returned_on_success() {
    let _store_returned = Arc::new(AtomicBool::new(false));

    let handle = thread::spawn(move || -> (Result<i64, &'static str>, bool) {
        thread::sleep(Duration::from_millis(10));
        (Ok(42), true) // Simulating (result, store)
    });

    let timeout = Duration::from_millis(500);
    let start = Instant::now();

    loop {
        if handle.is_finished() {
            break;
        }
        if start.elapsed() >= timeout {
            panic!("Should not timeout");
        }
        thread::sleep(Duration::from_millis(10));
    }

    let (result, store) = handle.join().unwrap();
    assert!(store); // Store should be returned
    assert_eq!(result, Ok(42));
}

#[test]
fn test_store_lost_on_timeout() {
    // When we timeout, we abandon the thread and lose the store
    let store_lost = Arc::new(AtomicBool::new(true));
    let store_lost_clone = store_lost.clone();

    let handle = thread::spawn(move || -> (Result<i64, &'static str>, bool) {
        thread::sleep(Duration::from_secs(10)); // Long sleep
        store_lost_clone.store(false, Ordering::SeqCst);
        (Ok(42), true)
    });

    let timeout = Duration::from_millis(50);
    let start = Instant::now();

    loop {
        if handle.is_finished() {
            break;
        }
        if start.elapsed() >= timeout {
            // Timeout - we abandon the thread without joining
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }

    // Store is lost because we didn't join the thread
    assert!(store_lost.load(Ordering::SeqCst));
}

// =============================================================================
// Polling Interval Tests
// =============================================================================

#[test]
fn test_polling_does_not_spin() {
    let poll_count = Arc::new(AtomicU32::new(0));
    let poll_count_clone = poll_count.clone();

    // Thread that takes 100ms
    let handle = thread::spawn(move || -> Result<i64, &'static str> {
        thread::sleep(Duration::from_millis(100));
        Ok(42)
    });

    let timeout = Duration::from_millis(500);
    let start = Instant::now();

    loop {
        poll_count_clone.fetch_add(1, Ordering::SeqCst);
        if handle.is_finished() {
            break;
        }
        if start.elapsed() >= timeout {
            panic!("Should not timeout");
        }
        thread::sleep(Duration::from_millis(10)); // 10ms sleep between polls
    }

    let _ = handle.join();

    // With 10ms polling interval and 100ms execution, we should have ~10-15 polls
    // (not hundreds or thousands which would indicate spinning)
    let polls = poll_count.load(Ordering::SeqCst);
    assert!(
        (5..=20).contains(&polls),
        "Expected 5-20 polls, got {}",
        polls
    );
}

// =============================================================================
// Orphaned Thread Tests
// =============================================================================

#[test]
fn test_orphaned_threads_eventually_complete() {
    let completed = Arc::new(AtomicU32::new(0));
    let handles: Vec<_> = (0..3)
        .map(|i| {
            let completed = completed.clone();
            thread::spawn(move || -> Result<i64, &'static str> {
                thread::sleep(Duration::from_millis(200 + i * 100));
                completed.fetch_add(1, Ordering::SeqCst);
                Ok(i as i64)
            })
        })
        .collect();

    // Timeout all of them with a short timeout
    for handle in handles {
        let _ = simulate_execution_polling(handle, 0.05);
    }

    // All were abandoned, but give them time to complete in background
    thread::sleep(Duration::from_millis(600));

    // All orphaned threads should have completed
    assert_eq!(completed.load(Ordering::SeqCst), 3);
}
