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
//! # Supported features
//!
//! - Up to 5 branches
//! - Optional `if` guards: `pat = fut, if condition => body`
//! - Optional `else` branch: `else => body` (runs when all guards are false)
//!
//! # Example
//!
//! ```ignore
//! deterministic_select! {
//!     msg = rx.recv() => handle_msg(msg),
//!     item = queue.next(), if !queue.is_empty() => handle_item(item),
//!     else => handle_all_disabled(),
//! }
//! ```

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

/// Internal helper to get a deterministic ordering for enabled branches
#[inline]
pub fn deterministic_order_enabled(enabled: &[usize]) -> Vec<usize> {
    if enabled.len() <= 1 {
        return enabled.to_vec();
    }
    let start = GlobalRng::random_range(0..enabled.len());
    let mut result = Vec::with_capacity(enabled.len());
    for i in 0..enabled.len() {
        result.push(enabled[(start + i) % enabled.len()]);
    }
    result
}

/// Output enum for 2-branch select (with optional else)
pub enum Out2<A, B, E> {
    _0(A),
    _1(B),
    _Else(E),
}

/// Output enum for 3-branch select (with optional else)
pub enum Out3<A, B, C, E> {
    _0(A),
    _1(B),
    _2(C),
    _Else(E),
}

/// Output enum for 4-branch select (with optional else)
pub enum Out4<A, B, C, D, E> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
    _Else(E),
}

/// Output enum for 5-branch select (with optional else)
pub enum Out5<A, B, C, D, E, F> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
    _4(E),
    _Else(F),
}

/// The `deterministic_select!` macro waits on multiple async operations simultaneously,
/// returning when one of them completes. Branch ordering is determined by `GlobalRng`
/// for deterministic but fair selection.
///
/// # Features
///
/// - Supports `if` guards: `pat = fut, if condition => body`
/// - Supports `else` branch: runs when all branches are disabled by guards
/// - Up to 5 branches are supported
#[macro_export]
macro_rules! deterministic_select {
    // ==================== 2 BRANCHES ====================

    // 2 branches, no guards, no else
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out2;

        let start = $crate::util::deterministic_select::deterministic_start_index(2);

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
                                return Poll::Ready(Out2::<_, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_1(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out2::_0($pat1) => $body1,
            Out2::_1($pat2) => $body2,
            Out2::_Else(()) => unreachable!(),
        }
    }};

    // 2 branches with guards, no else
    (
        $pat1:pat = $fut1:expr, if $guard1:expr => $body1:expr,
        $pat2:pat = $fut2:expr, if $guard2:expr => $body2:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out2;

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                // Evaluate guards and collect enabled branches
                let mut enabled = Vec::new();
                if $guard1 { enabled.push(0usize); }
                if $guard2 { enabled.push(1usize); }

                if enabled.is_empty() {
                    return Poll::Pending;
                }

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_1(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out2::_0($pat1) => $body1,
            Out2::_1($pat2) => $body2,
            Out2::_Else(()) => unreachable!(),
        }
    }};

    // 2 branches: first with guard, second without
    (
        $pat1:pat = $fut1:expr, if $guard1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out2;

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                let mut enabled = Vec::new();
                if $guard1 { enabled.push(0usize); }
                enabled.push(1usize); // Always enabled

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_1(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out2::_0($pat1) => $body1,
            Out2::_1($pat2) => $body2,
            Out2::_Else(()) => unreachable!(),
        }
    }};

    // 2 branches: first without guard, second with guard
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr, if $guard2:expr => $body2:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out2;

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                let mut enabled = Vec::new();
                enabled.push(0usize); // Always enabled
                if $guard2 { enabled.push(1usize); }

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out2::<_, _, ()>::_1(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            }).await
        };

        match output {
            Out2::_0($pat1) => $body1,
            Out2::_1($pat2) => $body2,
            Out2::_Else(()) => unreachable!(),
        }
    }};

    // ==================== 3 BRANCHES ====================

    // 3 branches, no guards, no else
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
                                return Poll::Ready(Out3::<_, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_2(val));
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
            Out3::_Else(()) => unreachable!(),
        }
    }};

    // 3 branches: first two without guard, third with guard
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out3;

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
                let mut enabled = vec![0usize, 1usize];
                if $guard3 { enabled.push(2usize); }

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_2(val));
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
            Out3::_Else(()) => unreachable!(),
        }
    }};

    // 3 branches: first without guard, second and third with guards
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr, if $guard2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out3;

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
                let mut enabled = vec![0usize];
                if $guard2 { enabled.push(1usize); }
                if $guard3 { enabled.push(2usize); }

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out3::<_, _, _, ()>::_2(val));
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
            Out3::_Else(()) => unreachable!(),
        }
    }};

    // 3 branches with else: all without guards
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr,
        else => $else_body:expr $(,)?
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
            Out3::_Else(_) => $else_body,
        }
    }};

    // ==================== 4 BRANCHES ====================

    // 4 branches, no guards, no else
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
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_3(val));
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
            Out4::_Else(()) => unreachable!(),
        }
    }};

    // 4 branches with else: all without guards
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr,
        else => $else_body:expr $(,)?
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
            Out4::_Else(_) => $else_body,
        }
    }};

    // 4 branches: third with guard, others without
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out4;

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
                let mut enabled = vec![0usize, 1usize];
                if $guard3 { enabled.push(2usize); }
                enabled.push(3usize);

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out4::<_, _, _, _, ()>::_3(val));
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
            Out4::_Else(()) => unreachable!(),
        }
    }};

    // 4 branches with else: third with guard
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr,
        else => $else_body:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out4;

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
                let mut enabled = vec![0usize, 1usize];
                if $guard3 { enabled.push(2usize); }
                enabled.push(3usize);

                let order = $crate::util::deterministic_select::deterministic_order_enabled(&enabled);

                for &idx in &order {
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
            Out4::_Else(_) => $else_body,
        }
    }};

    // ==================== 5 BRANCHES ====================

    // 5 branches, no guards, no else
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
                                return Poll::Ready(Out5::<_, _, _, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out5::<_, _, _, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out5::<_, _, _, _, _, ()>::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out5::<_, _, _, _, _, ()>::_3(val));
                            }
                        }
                        4 => {
                            if let Poll::Ready(val) = fut5.as_mut().poll(cx) {
                                return Poll::Ready(Out5::<_, _, _, _, _, ()>::_4(val));
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
            Out5::_Else(()) => unreachable!(),
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

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_with_if_guard() {
        GlobalRng::set_seed(12345);

        // When guard is false, only enabled branch should be selected
        let should_enable = false;
        let result = deterministic_select! {
            v = std::future::ready(1), if should_enable => v,
            v = std::future::ready(2) => v,
        };
        assert_eq!(result, 2, "When guard is false, only branch 2 should be selected");

        // When guard is true, both branches can be selected
        let should_enable = true;
        let mut got_1 = false;
        let mut got_2 = false;
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1), if should_enable => v,
                v = std::future::ready(2) => v,
            };
            if result == 1 { got_1 = true; }
            if result == 2 { got_2 = true; }
        }
        assert!(got_1 && got_2, "When guard is true, both branches should be selectable");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_with_if_guard_second_branch() {
        GlobalRng::set_seed(12345);

        // When guard is false, only enabled branch should be selected
        let should_enable = false;
        let result = deterministic_select! {
            v = std::future::ready(1) => v,
            v = std::future::ready(2), if should_enable => v,
        };
        assert_eq!(result, 1, "When guard is false, only branch 1 should be selected");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select3_with_guard_on_third() {
        GlobalRng::set_seed(12345);

        let should_enable = false;
        let mut results = Vec::new();
        for _ in 0..50 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3), if should_enable => v,
            };
            results.push(result);
        }
        assert!(results.iter().all(|&r| r != 3), "Branch 3 should never be selected when guard is false");

        let should_enable = true;
        let mut got_3 = false;
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3), if should_enable => v,
            };
            if result == 3 { got_3 = true; }
        }
        assert!(got_3, "Branch 3 should be selectable when guard is true");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_guard_determinism() {
        // Guards with deterministic random selection should be reproducible
        GlobalRng::set_seed(99999);
        let mut results_run1 = Vec::new();
        for i in 0..100 {
            let guard = i % 2 == 0;
            let result = deterministic_select! {
                v = std::future::ready(1), if guard => v,
                v = std::future::ready(2) => v,
            };
            results_run1.push(result);
        }

        GlobalRng::set_seed(99999);
        let mut results_run2 = Vec::new();
        for i in 0..100 {
            let guard = i % 2 == 0;
            let result = deterministic_select! {
                v = std::future::ready(1), if guard => v,
                v = std::future::ready(2) => v,
            };
            results_run2.push(result);
        }

        assert_eq!(results_run1, results_run2, "Results should be deterministic with same seed and guards");
    }
}
