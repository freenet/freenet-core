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

use crate::config::GlobalRng;

/// Internal helper to get the starting index for deterministic but fair polling.
#[inline]
pub fn deterministic_start_index(num_branches: usize) -> usize {
    if num_branches > 1 {
        GlobalRng::random_range(0..num_branches)
    } else {
        0
    }
}

/// Output enum for 2-branch select
pub enum Out2<A, B> {
    _0(A),
    _1(B),
}

/// Output enum for 3-branch select
pub enum Out3<A, B, C> {
    _0(A),
    _1(B),
    _2(C),
}

/// Output enum for 4-branch select
pub enum Out4<A, B, C, D> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
}

/// Output enum for 5-branch select
pub enum Out5<A, B, C, D, E> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
    _4(E),
}

/// The `deterministic_select!` macro waits on multiple async operations simultaneously,
/// returning when one of them completes. Branch ordering is determined by `GlobalRng`
/// for deterministic but fair selection.
///
/// # Notes
///
/// - Unlike `tokio::select!`, this macro does not support preconditions (`if` guards)
/// - The `else` branch is not supported
/// - Up to 5 branches are supported
#[macro_export]
macro_rules! deterministic_select {
    // 2 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out2;

        // Determine poll order using GlobalRng BEFORE creating futures
        let start = $crate::util::deterministic_select::deterministic_start_index(2);

        // Scope futures so they're dropped before handlers run
        // This allows handlers to borrow from the same source as futures
        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                for i in 0..2 {
                    let idx = (start + i) % 2;
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out2::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out2::_1(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        }; // fut1 and fut2 dropped here

        // Handlers run after futures are dropped
        match output {
            Out2::_0($pat1) => $body1,
            Out2::_1($pat2) => $body2,
        }
    }};

    // 3 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out3;

        let start = $crate::util::deterministic_select::deterministic_start_index(3);

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
                for i in 0..3 {
                    let idx = (start + i) % 3;
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out3::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out3::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out3::_2(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out3::_0($pat1) => $body1,
            Out3::_1($pat2) => $body2,
            Out3::_2($pat3) => $body3,
        }
    }};

    // 4 branches
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out4;

        let start = $crate::util::deterministic_select::deterministic_start_index(4);

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            let mut fut4 = $fut4;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);
            tokio::pin!(fut4);

            std::future::poll_fn(|cx| {
                for i in 0..4 {
                    let idx = (start + i) % 4;
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out4::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out4::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out4::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out4::_3(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out4::_0($pat1) => $body1,
            Out4::_1($pat2) => $body2,
            Out4::_2($pat3) => $body3,
            Out4::_3($pat4) => $body4,
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
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out5;

        let start = $crate::util::deterministic_select::deterministic_start_index(5);

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            let mut fut4 = $fut4;
            let mut fut5 = $fut5;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);
            tokio::pin!(fut4);
            tokio::pin!(fut5);

            std::future::poll_fn(|cx| {
                for i in 0..5 {
                    let idx = (start + i) % 5;
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out5::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out5::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out5::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out5::_3(val));
                            }
                        }
                        4 => {
                            if let Poll::Ready(val) = fut5.as_mut().poll(cx) {
                                return Poll::Ready(Out5::_4(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out5::_0($pat1) => $body1,
            Out5::_1($pat2) => $body2,
            Out5::_2($pat3) => $body3,
            Out5::_3($pat4) => $body4,
            Out5::_4($pat5) => $body5,
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn test_select2_determinism() {
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
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_with_single_ready_future() {
        GlobalRng::set_seed(12345);

        for _ in 0..10 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::pending::<i32>() => v,
            };
            assert_eq!(result, 1, "Ready future should always win against pending");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_pattern_binding() {
        GlobalRng::set_seed(12345);

        let result = deterministic_select! {
            val = std::future::ready(42) => val * 2,
            val = std::future::ready(10) => val + 5,
        };

        assert!(result == 84 || result == 15, "Got unexpected result: {}", result);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_with_borrowed_data() {
        struct Data {
            value: i32,
        }

        impl Data {
            async fn get_a(&self) -> i32 {
                self.value
            }
            async fn get_b(&self) -> i32 {
                self.value * 2
            }
        }

        let data = Data { value: 21 };
        GlobalRng::set_seed(12345);

        let result = deterministic_select! {
            v = data.get_a() => v,
            v = data.get_b() => v,
        };

        assert!(result == 21 || result == 42, "Got unexpected result: {}", result);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_different_return_types() {
        GlobalRng::set_seed(12345);

        let result: i32 = deterministic_select! {
            v = std::future::ready(42i32) => v,
            _ = std::future::ready(()) => 0,
        };

        assert!(result == 42 || result == 0, "Got unexpected result: {}", result);
    }
}
