//! Tests for RuntimePool edge cases.
//!
//! These tests verify the behavior of the executor pool, including:
//! - Pool creation and sizing
//! - Executor borrowing and returning
//! - Semaphore-based concurrency control
//! - Health checking and replacement
//! - Behavior under load

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::timeout;

// =============================================================================
// MockPool for Unit Testing
// =============================================================================

/// A simplified mock pool that matches RuntimePool's core behavior.
/// Uses a simpler design than the real pool to avoid deadlocks in tests.
struct MockPool {
    executors: Vec<Option<MockExecutor>>,
    available: Semaphore,
    borrow_count: AtomicU32,
    return_count: AtomicU32,
}

#[derive(Clone)]
struct MockExecutor {
    id: u32,
    healthy: bool,
}

impl MockPool {
    fn new(size: usize) -> Self {
        let executors: Vec<_> = (0..size)
            .map(|i| {
                Some(MockExecutor {
                    id: i as u32,
                    healthy: true,
                })
            })
            .collect();

        Self {
            executors,
            available: Semaphore::new(size),
            borrow_count: AtomicU32::new(0),
            return_count: AtomicU32::new(0),
        }
    }

    async fn pop_executor(&mut self) -> MockExecutor {
        let permit = self.available.acquire().await.unwrap();
        // Consume the permit without returning it to the semaphore.
        // The permit will be restored in `return_executor` via `add_permits(1)`.
        permit.forget();

        self.borrow_count.fetch_add(1, Ordering::SeqCst);

        for slot in &mut self.executors {
            if let Some(executor) = slot.take() {
                return executor;
            }
        }
        unreachable!("No executors available despite semaphore permit")
    }

    fn return_executor(&mut self, executor: MockExecutor) {
        self.return_count.fetch_add(1, Ordering::SeqCst);

        if let Some(empty_slot) = self.executors.iter_mut().find(|slot| slot.is_none()) {
            *empty_slot = Some(executor);
            self.available.add_permits(1);
        } else {
            unreachable!("No empty slot found in the pool")
        }
    }

    fn available_permits(&self) -> usize {
        self.available.available_permits()
    }
}

// =============================================================================
// Pool Creation Tests
// =============================================================================

#[tokio::test]
async fn test_pool_creation_with_various_sizes() {
    for size in [1, 2, 4, 8, 16] {
        let pool = MockPool::new(size);
        assert_eq!(pool.executors.len(), size);
        assert_eq!(pool.available.available_permits(), size);
    }
}

#[tokio::test]
async fn test_pool_creation_single_executor() {
    let pool = MockPool::new(1);
    assert_eq!(pool.executors.len(), 1);
    assert_eq!(pool.available.available_permits(), 1);
}

// =============================================================================
// Executor Borrowing Tests
// =============================================================================

#[tokio::test]
async fn test_borrow_and_return_single_executor() {
    let mut pool = MockPool::new(1);

    let executor = pool.pop_executor().await;
    assert_eq!(pool.available_permits(), 0);

    pool.return_executor(executor);
    assert_eq!(pool.available_permits(), 1);
}

#[tokio::test]
async fn test_borrow_all_executors() {
    let mut pool = MockPool::new(4);

    // Borrow all executors
    let mut borrowed = Vec::new();
    for _ in 0..4 {
        borrowed.push(pool.pop_executor().await);
    }

    assert_eq!(pool.available_permits(), 0);

    // Return all
    for executor in borrowed {
        pool.return_executor(executor);
    }

    assert_eq!(pool.available_permits(), 4);
}

#[tokio::test]
async fn test_borrow_blocks_when_pool_exhausted() {
    // Test that the semaphore properly blocks when all permits are consumed
    let semaphore = Arc::new(Semaphore::new(1));

    // Acquire the only permit
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    permit.forget(); // Consume without returning

    assert_eq!(semaphore.available_permits(), 0);

    // Try to acquire another permit - should block
    let sem_clone = semaphore.clone();
    let acquire_future = async move {
        let _ = sem_clone.acquire().await.unwrap();
    };

    let result = timeout(Duration::from_millis(50), acquire_future).await;
    assert!(result.is_err(), "Should timeout waiting for permit");

    // Add permit back
    semaphore.add_permits(1);
    assert_eq!(semaphore.available_permits(), 1);
}

// =============================================================================
// Concurrent Access Tests (Semaphore-based)
// =============================================================================

#[tokio::test]
async fn test_semaphore_concurrent_access() {
    // Test that the semaphore correctly limits concurrent access
    let semaphore = Arc::new(Semaphore::new(4));
    let completed = Arc::new(AtomicU32::new(0));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let current = Arc::new(AtomicU32::new(0));

    let handles: Vec<_> = (0..8)
        .map(|i| {
            let semaphore = semaphore.clone();
            let completed = completed.clone();
            let max_concurrent = max_concurrent.clone();
            let current = current.clone();

            tokio::spawn(async move {
                // Acquire permit (blocking if none available)
                let permit = semaphore.acquire().await.unwrap();

                // Track concurrent access
                let now_current = current.fetch_add(1, Ordering::SeqCst) + 1;
                max_concurrent.fetch_max(now_current, Ordering::SeqCst);

                // Simulate work
                tokio::time::sleep(Duration::from_millis(10 + i * 5)).await;

                current.fetch_sub(1, Ordering::SeqCst);
                completed.fetch_add(1, Ordering::SeqCst);

                drop(permit);
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(completed.load(Ordering::SeqCst), 8);
    // Max concurrent should never exceed semaphore permits
    assert!(max_concurrent.load(Ordering::SeqCst) <= 4);
}

#[tokio::test]
async fn test_rapid_borrow_return_cycles() {
    let mut pool = MockPool::new(1);

    for _ in 0..100 {
        let executor = pool.pop_executor().await;
        pool.return_executor(executor);
    }

    assert_eq!(pool.borrow_count.load(Ordering::SeqCst), 100);
    assert_eq!(pool.return_count.load(Ordering::SeqCst), 100);
    assert_eq!(pool.available_permits(), 1);
}

// =============================================================================
// Health Check Tests
// =============================================================================

#[tokio::test]
async fn test_healthy_executor_reused() {
    let mut pool = MockPool::new(1);

    let executor = pool.pop_executor().await;
    assert!(executor.healthy);
    let id = executor.id;

    pool.return_executor(executor);

    let executor2 = pool.pop_executor().await;
    assert_eq!(executor2.id, id); // Same executor reused
    assert!(executor2.healthy);
}

#[tokio::test]
async fn test_unhealthy_executor_detected() {
    let mut pool = MockPool::new(1);

    let mut executor = pool.pop_executor().await;
    executor.healthy = false; // Simulate corruption

    // Return unhealthy executor
    pool.return_executor(executor);

    // Next borrow gets the unhealthy executor
    let executor = pool.pop_executor().await;
    assert!(!executor.healthy);
}

#[tokio::test]
async fn test_health_check_and_replace_pattern() {
    let mut pool = MockPool::new(2);

    // Borrow executor, mark unhealthy, create replacement
    let mut executor = pool.pop_executor().await;
    executor.healthy = false;

    // Simulate replacement: create new healthy executor
    let replacement = MockExecutor {
        id: 100, // New ID
        healthy: true,
    };

    // Return replacement instead of broken one
    pool.return_executor(replacement);

    // Next borrow should get the replacement
    let next = pool.pop_executor().await;
    assert_eq!(next.id, 100);
    assert!(next.healthy);
}

// =============================================================================
// Pool Exhaustion Tests
// =============================================================================

#[tokio::test]
async fn test_pool_exhaustion_with_timeout() {
    let semaphore = Arc::new(Semaphore::new(2));

    // Acquire both permits
    let permit1 = semaphore.clone().acquire_owned().await.unwrap();
    let permit2 = semaphore.clone().acquire_owned().await.unwrap();
    permit1.forget();
    permit2.forget();

    // Try to acquire a third - should timeout
    let sem_clone = semaphore.clone();
    let borrow_result = timeout(Duration::from_millis(100), async move {
        let _ = sem_clone.acquire().await.unwrap();
    })
    .await;

    assert!(borrow_result.is_err(), "Should timeout");

    // Return permits
    semaphore.add_permits(2);
    assert_eq!(semaphore.available_permits(), 2);
}

#[tokio::test]
async fn test_pool_recovers_after_exhaustion() {
    let semaphore = Arc::new(Semaphore::new(1));

    // Exhaust pool
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    permit.forget();

    // Spawn task that will wait for permit
    let sem_clone = semaphore.clone();
    let waiter = tokio::spawn(async move {
        let _ = sem_clone.acquire().await.unwrap();
    });

    // Add permit back after delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    semaphore.add_permits(1);

    // Waiter should succeed
    let result = timeout(Duration::from_millis(100), waiter).await;
    assert!(result.is_ok());
}

// =============================================================================
// Stress Tests
// =============================================================================

#[tokio::test]
async fn test_high_contention_semaphore() {
    let semaphore = Arc::new(Semaphore::new(4));
    let operations = Arc::new(AtomicU32::new(0));

    let handles: Vec<_> = (0..20)
        .map(|_| {
            let semaphore = semaphore.clone();
            let operations = operations.clone();
            tokio::spawn(async move {
                for _ in 0..10 {
                    let permit = semaphore.acquire().await.unwrap();
                    // Very short work
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    drop(permit);
                    operations.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(operations.load(Ordering::SeqCst), 200);
    assert_eq!(semaphore.available_permits(), 4);
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[tokio::test]
async fn test_return_without_borrow_panics() {
    let mut pool = MockPool::new(1);

    // First, borrow and return normally
    let executor = pool.pop_executor().await;
    pool.return_executor(executor.clone());

    // Try to return again - should panic due to no empty slot
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pool.return_executor(executor);
    }));

    assert!(result.is_err());
}

#[tokio::test]
async fn test_mixed_healthy_unhealthy_executors() {
    let mut pool = MockPool::new(4);

    // Borrow all, mark some unhealthy
    let mut executors = Vec::new();
    for _ in 0..4 {
        executors.push(pool.pop_executor().await);
    }

    executors[0].healthy = false;
    executors[2].healthy = false;

    // Return all
    for e in executors {
        pool.return_executor(e);
    }

    // Verify we can still borrow 4 executors
    let mut reborrowed = Vec::new();
    for _ in 0..4 {
        reborrowed.push(pool.pop_executor().await);
    }

    let healthy_count = reborrowed.iter().filter(|e| e.healthy).count();
    let unhealthy_count = reborrowed.iter().filter(|e| !e.healthy).count();

    assert_eq!(healthy_count, 2);
    assert_eq!(unhealthy_count, 2);
}

// =============================================================================
// Permit Accounting Tests
// =============================================================================

#[tokio::test]
async fn test_permit_count_remains_stable() {
    // Test that after many borrow/return cycles, permits stay at pool size
    let mut pool = MockPool::new(4);

    // Many cycles
    for _ in 0..50 {
        let e1 = pool.pop_executor().await;
        let e2 = pool.pop_executor().await;
        pool.return_executor(e1);
        pool.return_executor(e2);
    }

    // Permits should still be exactly 4
    assert_eq!(pool.available_permits(), 4);
}

#[tokio::test]
async fn test_permit_count_after_partial_borrow() {
    let mut pool = MockPool::new(4);

    // Borrow 2
    let e1 = pool.pop_executor().await;
    let e2 = pool.pop_executor().await;
    assert_eq!(pool.available_permits(), 2);

    // Return 1
    pool.return_executor(e1);
    assert_eq!(pool.available_permits(), 3);

    // Return the other
    pool.return_executor(e2);
    assert_eq!(pool.available_permits(), 4);
}
