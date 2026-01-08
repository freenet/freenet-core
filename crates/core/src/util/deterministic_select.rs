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

/// Output enum for 6-branch select (with optional else)
pub enum Out6<A, B, C, D, E, F, G> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
    _4(E),
    _5(F),
    _Else(G),
}

/// Output enum for 7-branch select (with optional else)
pub enum Out7<A, B, C, D, E, F, G, H> {
    _0(A),
    _1(B),
    _2(C),
    _3(D),
    _4(E),
    _5(F),
    _6(G),
    _Else(H),
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

        // Compute random start ONCE, outside poll_fn
        let start = $crate::util::deterministic_select::deterministic_start_index(2);
        // Pre-compute polling order (stack allocated)
        let order: [usize; 2] = [start % 2, (start + 1) % 2];

        let output = {
            let fut1 = $fut1;
            let fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
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
            })
            .await
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = {
            let mut d = 0u8;
            if !$guard1 {
                d |= 1 << 0;
            }
            if !$guard2 {
                d |= 1 << 1;
            }
            d
        };

        // Count enabled branches and build order array (stack allocated)
        let mut order: [usize; 2] = [0, 0];
        let mut enabled_count = 0usize;
        if disabled & (1 << 0) == 0 {
            order[enabled_count] = 0;
            enabled_count += 1;
        }
        if disabled & (1 << 1) == 0 {
            order[enabled_count] = 1;
            enabled_count += 1;
        }

        // If all disabled, panic (no else branch provided)
        if enabled_count == 0 {
            panic!("all branches are disabled and there is no else branch");
        }

        // Compute random start ONCE and shuffle the enabled portion
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            if start == 1 {
                order.swap(0, 1);
            }
        }

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = if !$guard1 { 1 << 0 } else { 0 };

        // Build order array (stack allocated)
        let mut order: [usize; 2] = [0, 0];
        let mut enabled_count = 0usize;
        if disabled & (1 << 0) == 0 {
            order[enabled_count] = 0;
            enabled_count += 1;
        }
        order[enabled_count] = 1;
        enabled_count += 1; // Branch 1 always enabled

        // Compute random start ONCE and shuffle
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            if start == 1 {
                order.swap(0, 1);
            }
        }

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = if !$guard2 { 1 << 1 } else { 0 };

        // Build order array (stack allocated)
        let mut order: [usize; 2] = [0, 0];
        let mut enabled_count = 0usize;
        order[enabled_count] = 0;
        enabled_count += 1; // Branch 0 always enabled
        if disabled & (1 << 1) == 0 {
            order[enabled_count] = 1;
            enabled_count += 1;
        }

        // Compute random start ONCE and shuffle
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            if start == 1 {
                order.swap(0, 1);
            }
        }

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            tokio::pin!(fut1);
            tokio::pin!(fut2);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
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

        // Compute random start ONCE
        let start = $crate::util::deterministic_select::deterministic_start_index(3);
        let order: [usize; 3] = [start % 3, (start + 1) % 3, (start + 2) % 3];

        let output = {
            let fut1 = $fut1;
            let fut2 = $fut2;
            let fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
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
            })
            .await
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = if !$guard3 { 1 << 2 } else { 0 };

        // Build order array (stack allocated)
        let mut order: [usize; 3] = [0, 1, 0];
        let mut enabled_count = 2usize; // Branches 0,1 always enabled
        if disabled & (1 << 2) == 0 {
            order[enabled_count] = 2;
            enabled_count += 1;
        }

        // Compute random start ONCE and rotate
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            order[..enabled_count].rotate_left(start);
        }

        let output = {
            let fut1 = $fut1;
            let fut2 = $fut2;
            let fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = {
            let mut d = 0u8;
            if !$guard2 {
                d |= 1 << 1;
            }
            if !$guard3 {
                d |= 1 << 2;
            }
            d
        };

        // Build order array (stack allocated)
        let mut order: [usize; 3] = [0, 0, 0];
        let mut enabled_count = 1usize; // Branch 0 always enabled
        if disabled & (1 << 1) == 0 {
            order[enabled_count] = 1;
            enabled_count += 1;
        }
        if disabled & (1 << 2) == 0 {
            order[enabled_count] = 2;
            enabled_count += 1;
        }

        // Compute random start ONCE and rotate
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            order[..enabled_count].rotate_left(start);
        }

        let output = {
            let mut fut1 = $fut1;
            let mut fut2 = $fut2;
            let mut fut3 = $fut3;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
        };

        match output {
            Out3::_0($pat1) => $body1,
            Out3::_1($pat2) => $body2,
            Out3::_2($pat3) => $body3,
            Out3::_Else(()) => unreachable!(),
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

        // Compute random start ONCE
        let start = $crate::util::deterministic_select::deterministic_start_index(4);
        let order: [usize; 4] = [start % 4, (start + 1) % 4, (start + 2) % 4, (start + 3) % 4];

        let output = {
            let fut1 = $fut1;
            let fut2 = $fut2;
            let fut3 = $fut3;
            let fut4 = $fut4;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);
            tokio::pin!(fut4);

            std::future::poll_fn(|cx| {
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
            })
            .await
        };

        match output {
            Out4::_0($pat1) => $body1,
            Out4::_1($pat2) => $body2,
            Out4::_2($pat3) => $body3,
            Out4::_3($pat4) => $body4,
            Out4::_Else(()) => unreachable!(),
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

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = if !$guard3 { 1 << 2 } else { 0 };

        // Build order array (stack allocated)
        let mut order: [usize; 4] = [0, 1, 3, 0];
        let mut enabled_count = 3usize; // Branches 0,1,3 always enabled
        if disabled & (1 << 2) == 0 {
            // Insert branch 2 at position 2
            order[3] = order[2];
            order[2] = 2;
            enabled_count = 4;
        }

        // Compute random start ONCE and rotate
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            order[..enabled_count].rotate_left(start);
        }

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
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
        };

        match output {
            Out4::_0($pat1) => $body1,
            Out4::_1($pat2) => $body2,
            Out4::_2($pat3) => $body3,
            Out4::_3($pat4) => $body4,
            Out4::_Else(()) => unreachable!(),
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

        // Compute random start ONCE
        let start = $crate::util::deterministic_select::deterministic_start_index(5);
        let order: [usize; 5] = [
            start % 5,
            (start + 1) % 5,
            (start + 2) % 5,
            (start + 3) % 5,
            (start + 4) % 5,
        ];

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
                for &idx in &order {
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
            })
            .await
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

    // 5 branches: first without guard, second and third with guards, fourth without, fifth with guard
    // Pattern: connection_handler.rs recv loop
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr, if $guard2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr,
        $pat5:pat = $fut5:expr, if $guard5:expr => $body5:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out5;

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = {
            let mut d = 0u8;
            if !$guard2 {
                d |= 1 << 1;
            }
            if !$guard3 {
                d |= 1 << 2;
            }
            if !$guard5 {
                d |= 1 << 4;
            }
            d
        };

        // Build order array (stack allocated)
        // Branches 0, 3 are always enabled; 1, 2, 4 depend on guards
        let mut order: [usize; 5] = [0, 3, 0, 0, 0];
        let mut enabled_count = 2usize;
        if disabled & (1 << 1) == 0 {
            order[enabled_count] = 1;
            enabled_count += 1;
        }
        if disabled & (1 << 2) == 0 {
            order[enabled_count] = 2;
            enabled_count += 1;
        }
        if disabled & (1 << 4) == 0 {
            order[enabled_count] = 4;
            enabled_count += 1;
        }

        // Compute random start ONCE and rotate
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            order[..enabled_count].rotate_left(start);
        }

        let output = {
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

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
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
            })
            .await
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

    // ==================== 7 BRANCHES ====================

    // 7 branches: first without guard, second and third with guards, rest without guards
    // Pattern: peer_connection.rs recv loop
    (
        $pat1:pat = $fut1:expr => $body1:expr,
        $pat2:pat = $fut2:expr, if $guard2:expr => $body2:expr,
        $pat3:pat = $fut3:expr, if $guard3:expr => $body3:expr,
        $pat4:pat = $fut4:expr => $body4:expr,
        $pat5:pat = $fut5:expr => $body5:expr,
        $pat6:pat = $fut6:expr => $body6:expr,
        $pat7:pat = $fut7:expr => $body7:expr $(,)?
    ) => {{
        use std::future::Future;
        use std::task::Poll;
        use $crate::util::deterministic_select::Out7;

        // IMPORTANT: Evaluate guards FIRST, before creating futures!
        let disabled: u8 = {
            let mut d = 0u8;
            if !$guard2 {
                d |= 1 << 1;
            }
            if !$guard3 {
                d |= 1 << 2;
            }
            d
        };

        // Build order array (stack allocated)
        // Branches 0, 3, 4, 5, 6 are always enabled; 1, 2 depend on guards
        let mut order: [usize; 7] = [0, 3, 4, 5, 6, 0, 0];
        let mut enabled_count = 5usize;
        if disabled & (1 << 1) == 0 {
            order[enabled_count] = 1;
            enabled_count += 1;
        }
        if disabled & (1 << 2) == 0 {
            order[enabled_count] = 2;
            enabled_count += 1;
        }

        // Compute random start ONCE and rotate
        if enabled_count > 1 {
            let start =
                $crate::util::deterministic_select::deterministic_start_index(enabled_count);
            order[..enabled_count].rotate_left(start);
        }

        let output = {
            let fut1 = $fut1;
            let fut2 = $fut2;
            let fut3 = $fut3;
            let fut4 = $fut4;
            let fut5 = $fut5;
            let fut6 = $fut6;
            let fut7 = $fut7;
            tokio::pin!(fut1);
            tokio::pin!(fut2);
            tokio::pin!(fut3);
            tokio::pin!(fut4);
            tokio::pin!(fut5);
            tokio::pin!(fut6);
            tokio::pin!(fut7);

            std::future::poll_fn(|cx| {
                for i in 0..enabled_count {
                    let idx = order[i];
                    match idx {
                        0 => {
                            if let Poll::Ready(val) = fut1.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_0(val));
                            }
                        }
                        1 => {
                            if let Poll::Ready(val) = fut2.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_1(val));
                            }
                        }
                        2 => {
                            if let Poll::Ready(val) = fut3.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_2(val));
                            }
                        }
                        3 => {
                            if let Poll::Ready(val) = fut4.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_3(val));
                            }
                        }
                        4 => {
                            if let Poll::Ready(val) = fut5.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_4(val));
                            }
                        }
                        5 => {
                            if let Poll::Ready(val) = fut6.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_5(val));
                            }
                        }
                        6 => {
                            if let Poll::Ready(val) = fut7.as_mut().poll(cx) {
                                return Poll::Ready(Out7::<_, _, _, _, _, _, _, ()>::_6(val));
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                Poll::Pending
            })
            .await
        };

        match output {
            Out7::_0($pat1) => $body1,
            Out7::_1($pat2) => $body2,
            Out7::_2($pat3) => $body3,
            Out7::_3($pat4) => $body4,
            Out7::_4($pat5) => $body5,
            Out7::_5($pat6) => $body6,
            Out7::_6($pat7) => $body7,
            Out7::_Else(()) => unreachable!(),
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

        assert_eq!(
            results_run1, results_run2,
            "Results should be deterministic with same seed"
        );

        let ones = results_run1.iter().filter(|&&x| x == 1).count();
        let twos = results_run1.iter().filter(|&&x| x == 2).count();
        assert!(
            ones > 0 && twos > 0,
            "Should select from both branches, got {} ones and {} twos",
            ones,
            twos
        );
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

        assert_ne!(
            results_seed1, results_seed2,
            "Different seeds should produce different results"
        );
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

        assert_eq!(
            results_run1, results_run2,
            "Results should be deterministic with same seed"
        );
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

        assert!(
            result == 84 || result == 15,
            "Got unexpected result: {}",
            result
        );
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

        assert!(
            result == 21 || result == 42,
            "Got unexpected result: {}",
            result
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_select_different_return_types() {
        GlobalRng::set_seed(12345);

        let result: i32 = deterministic_select! {
            v = std::future::ready(42i32) => v,
            _ = std::future::ready(()) => 0,
        };

        assert!(
            result == 42 || result == 0,
            "Got unexpected result: {}",
            result
        );
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
        assert_eq!(
            result, 2,
            "When guard is false, only branch 2 should be selected"
        );

        // When guard is true, both branches can be selected
        let should_enable = true;
        let mut got_1 = false;
        let mut got_2 = false;
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1), if should_enable => v,
                v = std::future::ready(2) => v,
            };
            if result == 1 {
                got_1 = true;
            }
            if result == 2 {
                got_2 = true;
            }
        }
        assert!(
            got_1 && got_2,
            "When guard is true, both branches should be selectable"
        );
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
        assert_eq!(
            result, 1,
            "When guard is false, only branch 1 should be selected"
        );
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
        assert!(
            results.iter().all(|&r| r != 3),
            "Branch 3 should never be selected when guard is false"
        );

        let should_enable = true;
        let mut got_3 = false;
        for _ in 0..100 {
            let result = deterministic_select! {
                v = std::future::ready(1) => v,
                v = std::future::ready(2) => v,
                v = std::future::ready(3), if should_enable => v,
            };
            if result == 3 {
                got_3 = true;
            }
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

        assert_eq!(
            results_run1, results_run2,
            "Results should be deterministic with same seed and guards"
        );
    }
}
