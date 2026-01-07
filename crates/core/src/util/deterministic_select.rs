//! Deterministic select macro for simulation testing.
//!
//! This module provides a `deterministic_select!` macro that behaves like `tokio::select!`
//! but uses `GlobalRng` for branch ordering instead of tokio's internal RNG. This makes
//! the selection deterministic when the RNG is seeded, which is essential for deterministic
//! simulation testing (DST).
//!
//! # Why this exists
//!
//! `tokio::select!` without the `biased` keyword uses random branch ordering via an internal
//! RNG that is NOT controlled by turmoil's simulation seeding. This causes non-determinism
//! in tests even when all other RNG sources are seeded.
//!
//! Using `biased` makes selection deterministic but always polls branches in declaration order,
//! which can cause starvation. This macro provides fair selection that is still deterministic.
//!
//! # Usage
//!
//! ```ignore
//! use freenet::deterministic_select;
//!
//! let result = deterministic_select! {
//!     val = async_operation1() => handle_result1(val),
//!     val = async_operation2() => handle_result2(val),
//!     _ = shutdown_signal() => return,
//! };
//! ```

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::config::GlobalRng;

/// Internal helper to poll futures in a deterministic but fair order.
///
/// This function polls futures in an order determined by GlobalRng, ensuring
/// determinism when the RNG is seeded while still providing fairness (not always
/// checking the same branch first).
#[inline]
pub fn deterministic_poll_order(num_branches: usize) -> impl Iterator<Item = usize> {
    let start = if num_branches > 1 {
        GlobalRng::random_range(0..num_branches)
    } else {
        0
    };
    (0..num_branches).map(move |i| (start + i) % num_branches)
}

/// A future that selects from two futures using deterministic ordering.
pub struct Select2<F1, F2> {
    fut1: F1,
    fut2: F2,
}

impl<F1, F2> Select2<F1, F2> {
    pub fn new(fut1: F1, fut2: F2) -> Self {
        Self { fut1, fut2 }
    }
}

/// Result of a two-way select.
pub enum Select2Result<T1, T2> {
    First(T1),
    Second(T2),
}

impl<F1, F2, T1, T2> Future for Select2<F1, F2>
where
    F1: Future<Output = T1> + Unpin,
    F2: Future<Output = T2> + Unpin,
{
    type Output = Select2Result<T1, T2>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for idx in deterministic_poll_order(2) {
            match idx {
                0 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut1).poll(cx) {
                        return Poll::Ready(Select2Result::First(val));
                    }
                }
                1 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut2).poll(cx) {
                        return Poll::Ready(Select2Result::Second(val));
                    }
                }
                _ => unreachable!(),
            }
        }
        Poll::Pending
    }
}

/// A future that selects from three futures using deterministic ordering.
pub struct Select3<F1, F2, F3> {
    fut1: F1,
    fut2: F2,
    fut3: F3,
}

impl<F1, F2, F3> Select3<F1, F2, F3> {
    pub fn new(fut1: F1, fut2: F2, fut3: F3) -> Self {
        Self { fut1, fut2, fut3 }
    }
}

/// Result of a three-way select.
pub enum Select3Result<T1, T2, T3> {
    First(T1),
    Second(T2),
    Third(T3),
}

impl<F1, F2, F3, T1, T2, T3> Future for Select3<F1, F2, F3>
where
    F1: Future<Output = T1> + Unpin,
    F2: Future<Output = T2> + Unpin,
    F3: Future<Output = T3> + Unpin,
{
    type Output = Select3Result<T1, T2, T3>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for idx in deterministic_poll_order(3) {
            match idx {
                0 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut1).poll(cx) {
                        return Poll::Ready(Select3Result::First(val));
                    }
                }
                1 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut2).poll(cx) {
                        return Poll::Ready(Select3Result::Second(val));
                    }
                }
                2 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut3).poll(cx) {
                        return Poll::Ready(Select3Result::Third(val));
                    }
                }
                _ => unreachable!(),
            }
        }
        Poll::Pending
    }
}

/// A future that selects from four futures using deterministic ordering.
pub struct Select4<F1, F2, F3, F4> {
    fut1: F1,
    fut2: F2,
    fut3: F3,
    fut4: F4,
}

impl<F1, F2, F3, F4> Select4<F1, F2, F3, F4> {
    pub fn new(fut1: F1, fut2: F2, fut3: F3, fut4: F4) -> Self {
        Self {
            fut1,
            fut2,
            fut3,
            fut4,
        }
    }
}

/// Result of a four-way select.
pub enum Select4Result<T1, T2, T3, T4> {
    First(T1),
    Second(T2),
    Third(T3),
    Fourth(T4),
}

impl<F1, F2, F3, F4, T1, T2, T3, T4> Future for Select4<F1, F2, F3, F4>
where
    F1: Future<Output = T1> + Unpin,
    F2: Future<Output = T2> + Unpin,
    F3: Future<Output = T3> + Unpin,
    F4: Future<Output = T4> + Unpin,
{
    type Output = Select4Result<T1, T2, T3, T4>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for idx in deterministic_poll_order(4) {
            match idx {
                0 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut1).poll(cx) {
                        return Poll::Ready(Select4Result::First(val));
                    }
                }
                1 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut2).poll(cx) {
                        return Poll::Ready(Select4Result::Second(val));
                    }
                }
                2 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut3).poll(cx) {
                        return Poll::Ready(Select4Result::Third(val));
                    }
                }
                3 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut4).poll(cx) {
                        return Poll::Ready(Select4Result::Fourth(val));
                    }
                }
                _ => unreachable!(),
            }
        }
        Poll::Pending
    }
}

/// A future that selects from five futures using deterministic ordering.
pub struct Select5<F1, F2, F3, F4, F5> {
    fut1: F1,
    fut2: F2,
    fut3: F3,
    fut4: F4,
    fut5: F5,
}

impl<F1, F2, F3, F4, F5> Select5<F1, F2, F3, F4, F5> {
    pub fn new(fut1: F1, fut2: F2, fut3: F3, fut4: F4, fut5: F5) -> Self {
        Self {
            fut1,
            fut2,
            fut3,
            fut4,
            fut5,
        }
    }
}

/// Result of a five-way select.
pub enum Select5Result<T1, T2, T3, T4, T5> {
    First(T1),
    Second(T2),
    Third(T3),
    Fourth(T4),
    Fifth(T5),
}

impl<F1, F2, F3, F4, F5, T1, T2, T3, T4, T5> Future for Select5<F1, F2, F3, F4, F5>
where
    F1: Future<Output = T1> + Unpin,
    F2: Future<Output = T2> + Unpin,
    F3: Future<Output = T3> + Unpin,
    F4: Future<Output = T4> + Unpin,
    F5: Future<Output = T5> + Unpin,
{
    type Output = Select5Result<T1, T2, T3, T4, T5>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        for idx in deterministic_poll_order(5) {
            match idx {
                0 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut1).poll(cx) {
                        return Poll::Ready(Select5Result::First(val));
                    }
                }
                1 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut2).poll(cx) {
                        return Poll::Ready(Select5Result::Second(val));
                    }
                }
                2 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut3).poll(cx) {
                        return Poll::Ready(Select5Result::Third(val));
                    }
                }
                3 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut4).poll(cx) {
                        return Poll::Ready(Select5Result::Fourth(val));
                    }
                }
                4 => {
                    if let Poll::Ready(val) = Pin::new(&mut self.fut5).poll(cx) {
                        return Poll::Ready(Select5Result::Fifth(val));
                    }
                }
                _ => unreachable!(),
            }
        }
        Poll::Pending
    }
}

/// Deterministically select from multiple async operations using GlobalRng for ordering.
///
/// This macro behaves like `tokio::select!` but uses `GlobalRng` to determine the
/// polling order of branches, making it deterministic when the RNG is seeded.
///
/// # Syntax
///
/// ```ignore
/// deterministic_select! {
///     pattern1 = future1 => expression1,
///     pattern2 = future2 => expression2,
///     // ... up to 5 branches
/// }
/// ```
///
/// # Example
///
/// ```ignore
/// use freenet::deterministic_select;
/// use tokio::sync::mpsc;
///
/// let (tx1, mut rx1) = mpsc::channel::<i32>(1);
/// let (tx2, mut rx2) = mpsc::channel::<i32>(1);
///
/// // With GlobalRng seeded, this will always select branches in the same order
/// deterministic_select! {
///     val = rx1.recv() => println!("Got {} from rx1", val.unwrap()),
///     val = rx2.recv() => println!("Got {} from rx2", val.unwrap()),
/// }
/// ```
///
/// # Notes
///
/// - Unlike `tokio::select!`, this macro does not support preconditions (`if` guards)
/// - The `else` branch is not supported
/// - Up to 5 branches are supported (add more Select* types if needed)
/// - Futures must be `Unpin` or will be pinned using `Box::pin`
#[macro_export]
macro_rules! deterministic_select {
    // 2 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr $(,)?
    ) => {{
        use $crate::util::deterministic_select::{Select2, Select2Result};

        let fut1 = $fut1;
        let fut2 = $fut2;

        // Pin the futures
        tokio::pin!(fut1);
        tokio::pin!(fut2);

        match Select2::new(fut1, fut2).await {
            Select2Result::First($pat1) => $body1,
            Select2Result::Second($pat2) => $body2,
        }
    }};

    // 3 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr $(,)?
    ) => {{
        use $crate::util::deterministic_select::{Select3, Select3Result};

        let fut1 = $fut1;
        let fut2 = $fut2;
        let fut3 = $fut3;

        tokio::pin!(fut1);
        tokio::pin!(fut2);
        tokio::pin!(fut3);

        match Select3::new(fut1, fut2, fut3).await {
            Select3Result::First($pat1) => $body1,
            Select3Result::Second($pat2) => $body2,
            Select3Result::Third($pat3) => $body3,
        }
    }};

    // 4 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr $(,)?
    ) => {{
        use $crate::util::deterministic_select::{Select4, Select4Result};

        let fut1 = $fut1;
        let fut2 = $fut2;
        let fut3 = $fut3;
        let fut4 = $fut4;

        tokio::pin!(fut1);
        tokio::pin!(fut2);
        tokio::pin!(fut3);
        tokio::pin!(fut4);

        match Select4::new(fut1, fut2, fut3, fut4).await {
            Select4Result::First($pat1) => $body1,
            Select4Result::Second($pat2) => $body2,
            Select4Result::Third($pat3) => $body3,
            Select4Result::Fourth($pat4) => $body4,
        }
    }};

    // 5 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr,
        $pat5:pat = $fut5:expr => $body5:expr $(,)?
    ) => {{
        use $crate::util::deterministic_select::{Select5, Select5Result};

        let fut1 = $fut1;
        let fut2 = $fut2;
        let fut3 = $fut3;
        let fut4 = $fut4;
        let fut5 = $fut5;

        tokio::pin!(fut1);
        tokio::pin!(fut2);
        tokio::pin!(fut3);
        tokio::pin!(fut4);
        tokio::pin!(fut5);

        match Select5::new(fut1, fut2, fut3, fut4, fut5).await {
            Select5Result::First($pat1) => $body1,
            Select5Result::Second($pat2) => $body2,
            Select5Result::Third($pat3) => $body3,
            Select5Result::Fourth($pat4) => $body4,
            Select5Result::Fifth($pat5) => $body5,
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalRng;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    // Use std::future::ready for tests to ensure futures are immediately ready
    // This avoids any potential non-determinism from mpsc channel internals

    #[tokio::test(flavor = "current_thread")]
    async fn test_select2_determinism() {
        // Test with immediately ready futures - no runtime interaction
        GlobalRng::set_seed(12345);
        let mut results_run1 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            results_run1.push(result);
        }

        GlobalRng::set_seed(12345);
        let mut results_run2 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            results_run2.push(result);
        }

        assert_eq!(results_run1, results_run2, "Results should be deterministic with same seed");

        // Verify we got a mix of 1s and 2s (not always the same branch)
        let ones = results_run1.iter().filter(|&&x| x == 1).count();
        let twos = results_run1.iter().filter(|&&x| x == 2).count();
        assert!(ones > 0 && twos > 0, "Should select from both branches, got {} ones and {} twos", ones, twos);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select2_different_seeds_differ() {
        GlobalRng::set_seed(11111);
        let mut results_seed1 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            results_seed1.push(result);
        }

        GlobalRng::set_seed(22222);
        let mut results_seed2 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
            };
            results_seed2.push(result);
        }

        // Different seeds should (very likely) produce different results
        assert_ne!(results_seed1, results_seed2, "Different seeds should produce different results");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select3_determinism() {
        GlobalRng::set_seed(54321);
        let mut results_run1 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
            };
            results_run1.push(result);
        }

        GlobalRng::set_seed(54321);
        let mut results_run2 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
            };
            results_run2.push(result);
        }

        assert_eq!(results_run1, results_run2, "Results should be deterministic with same seed");

        // Verify we got a mix of all three values
        let ones = results_run1.iter().filter(|&&x| x == 1).count();
        let twos = results_run1.iter().filter(|&&x| x == 2).count();
        let threes = results_run1.iter().filter(|&&x| x == 3).count();
        assert!(ones > 0 && twos > 0 && threes > 0,
            "Should select from all branches, got {} ones, {} twos, {} threes", ones, twos, threes);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select4_determinism() {
        GlobalRng::set_seed(99999);
        let mut results_run1 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
                v = std::future::ready(4) => v,
            };
            results_run1.push(result);
        }

        GlobalRng::set_seed(99999);
        let mut results_run2 = Vec::new();
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
                v = std::future::ready(4) => v,
            };
            results_run2.push(result);
        }

        assert_eq!(results_run1, results_run2, "Results should be deterministic with same seed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select5_determinism() {
        GlobalRng::set_seed(77777);
        let mut results_run1 = Vec::new();
        for _ in 0..50 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
                v = std::future::ready(4) => v,
                v = std::future::ready(5) => v,
            };
            results_run1.push(result);
        }

        GlobalRng::set_seed(77777);
        let mut results_run2 = Vec::new();
        for _ in 0..50 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3) => v,
                v = std::future::ready(4) => v,
                v = std::future::ready(5) => v,
            };
            results_run2.push(result);
        }

        assert_eq!(results_run1, results_run2, "Results should be deterministic with same seed");
    }

    #[tokio::test]
    async fn test_select_with_single_ready_future() {
        GlobalRng::set_seed(12345);

        let (tx1, mut rx1) = mpsc::channel::<i32>(1);
        let (_tx2, mut rx2) = mpsc::channel::<i32>(1);

        // Only send to rx1
        tx1.send(42).await.unwrap();

        // Should always get from rx1 since rx2 is not ready
        let result = deterministic_select! {
            v = rx1.recv() => v.unwrap(),
            v = rx2.recv() => v.unwrap(),
        };
        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_select_pattern_binding() {
        GlobalRng::set_seed(12345);

        let (tx1, mut rx1) = mpsc::channel::<i32>(1);
        let (tx2, mut rx2) = mpsc::channel::<i32>(1);

        tx1.send(1).await.unwrap();
        tx2.send(2).await.unwrap();

        // Test that pattern binding works - using _ to ignore and val to capture
        let result = deterministic_select! {
            val = rx1.recv() => val.map(|v| v * 10),
            val = rx2.recv() => val.map(|v| v * 10),
        };
        // Result should be either Some(10) or Some(20)
        assert!(result == Some(10) || result == Some(20));
    }

    #[tokio::test]
    async fn test_poll_order_distribution() {
        // Test that deterministic_poll_order produces fair distribution
        GlobalRng::set_seed(42424);

        let mut counts = [0usize; 5];
        for _ in 0..1000 {
            let first = deterministic_poll_order(5).next().unwrap();
            counts[first] += 1;
        }

        // Each index should be selected roughly 200 times (1000/5)
        // Allow for some variance (should be between 100 and 300)
        for (i, &count) in counts.iter().enumerate() {
            assert!(
                count > 100 && count < 300,
                "Index {} selected {} times, expected ~200", i, count
            );
        }
    }

    #[tokio::test]
    async fn test_select_with_async_blocks() {
        GlobalRng::set_seed(12345);

        let counter = Arc::new(AtomicUsize::new(0));
        let counter1 = counter.clone();
        let counter2 = counter.clone();

        let result = deterministic_select! {
            _ = async {
                counter1.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                1
            } => "first",
            _ = async {
                counter2.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                2
            } => "second",
        };

        assert!(result == "first" || result == "second");
        // Both futures should have been polled at least once
        assert!(counter.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn test_select_immediate_ready() {
        GlobalRng::set_seed(12345);

        // Test with immediately ready futures
        let result = deterministic_select! {
            v = std::future::ready(1) => v,
            v = std::future::ready(2) => v,
        };
        assert!(result == 1 || result == 2);
    }
}
