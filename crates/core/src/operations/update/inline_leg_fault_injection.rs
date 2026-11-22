//! Test seam reproducing the #6143 failure class: a node whose INLINE
//! broadcast legs (`BroadcastTo*` sent as one message, not as a caller-named
//! stream) are accepted by the bridge and then silently lost.
//!
//! In #6143 the transport assigned the node's internal stream ids a reserved
//! bit after days of uptime, so every oversized inline message it sent was
//! parked by the receiver as an unclaimed stream. The bridge reported success,
//! the sender cached its own summary as the peer's, and nothing told it the
//! peer never applied the update. The explicit broadcast streams kept working.
//!
//! A rule names the SENDER's address and the contract. A matching inline leg
//! is reported as sent (the caller's success arm runs, exactly as it did in
//! #6143) and never reaches the bridge. The originator's routed
//! `RequestUpdate` is an inline message too and is swallowed the same way.
//! Streamed legs are untouched.
//!
//! [`lift`] removes the rules for a contract and keeps its count, so a test
//! can lose one update's legs and then let later traffic (a heal) through.
//!
//! A delay rule instead delivers matching inline legs late, on their own task:
//! a slow but healthy path. The originator's routed `RequestUpdate` is dropped
//! under a delay rule, so the other hosts learn the update only from the late
//! legs.
//!
//! A stream rule refuses the next `n` broadcast STREAMS a sender starts for a
//! contract (the stream's metadata send fails), so a test can fail a
//! verify-soon forced stream through its real failure path.
//! Ported from the confirmed-delivery branch (PR #6292).

#[cfg(any(test, feature = "testing"))]
pub mod seam {
    use freenet_stdlib::prelude::ContractKey;
    use std::collections::{HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Mutex, OnceLock};

    #[derive(Default)]
    struct State {
        rules: HashSet<(SocketAddr, ContractKey)>,
        delays: HashMap<(SocketAddr, ContractKey), std::time::Duration>,
        /// Inline legs swallowed, per contract. Per contract rather than one
        /// process-wide count because simulation tests run in parallel in one
        /// process; each test uses its own contract.
        swallowed: HashMap<ContractKey, usize>,
        /// Broadcast streams still to refuse, per (sender, contract).
        streams_to_fail: HashMap<(SocketAddr, ContractKey), usize>,
        /// Broadcast streams refused, per contract.
        streams_failed: HashMap<ContractKey, usize>,
    }

    fn state() -> std::sync::MutexGuard<'static, State> {
        static STATE: OnceLock<Mutex<State>> = OnceLock::new();
        STATE
            .get_or_init(|| Mutex::new(State::default()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Silently drop every inline broadcast leg `sender` sends for `key`.
    pub fn drop_inline_legs_from(sender: SocketAddr, key: ContractKey) {
        state().rules.insert((sender, key));
    }

    /// Deliver every inline broadcast leg `sender` sends for `key` `delay`
    /// late.
    pub fn delay_inline_legs_from(
        sender: SocketAddr,
        key: ContractKey,
        delay: std::time::Duration,
    ) {
        state().delays.insert((sender, key), delay);
    }

    /// Refuse the next `n` broadcast streams `sender` starts for `key`.
    pub fn fail_next_streams_from(sender: SocketAddr, key: ContractKey, n: usize) {
        state().streams_to_fail.insert((sender, key), n);
    }

    /// Broadcast streams refused for `key` since it was last cleared.
    pub fn streams_failed(key: &ContractKey) -> usize {
        state().streams_failed.get(key).copied().unwrap_or(0)
    }

    /// Whether the broadcast stream `sender` is starting for `key` is to be
    /// refused. Counts each one it refuses.
    pub(crate) fn stream_fails(sender: Option<SocketAddr>, key: &ContractKey) -> bool {
        let Some(sender) = sender else {
            return false;
        };
        let mut state = state();
        match state.streams_to_fail.get_mut(&(sender, *key)) {
            Some(left) if *left > 0 => {
                *left -= 1;
                *state.streams_failed.entry(*key).or_insert(0) += 1;
                true
            }
            _ => false,
        }
    }

    /// Stop dropping legs for `key`, keeping the swallowed count.
    pub fn lift(key: &ContractKey) {
        state().rules.retain(|(_, k)| k != key);
    }

    /// Inline legs swallowed for `key` since it was last cleared.
    pub fn swallowed(key: &ContractKey) -> usize {
        state().swallowed.get(key).copied().unwrap_or(0)
    }

    /// Remove every rule and count for `key`.
    pub fn clear(key: &ContractKey) {
        let mut state = state();
        state.rules.retain(|(_, k)| k != key);
        state.delays.retain(|(_, k), _| k != key);
        state.swallowed.remove(key);
        state.streams_to_fail.retain(|(_, k), _| k != key);
        state.streams_failed.remove(key);
    }

    /// What happens to an inline leg from `sender` for `key`. Counts each one
    /// it swallows.
    pub(crate) fn fate(sender: Option<SocketAddr>, key: &ContractKey) -> super::LegFate {
        let Some(sender) = sender else {
            return super::LegFate::Send;
        };
        let mut state = state();
        if state.rules.contains(&(sender, *key)) {
            *state.swallowed.entry(*key).or_insert(0) += 1;
            return super::LegFate::Swallow;
        }
        match state.delays.get(&(sender, *key)) {
            Some(delay) => super::LegFate::Delay(*delay),
            None => super::LegFate::Send,
        }
    }
}

/// What the test seam does with an inline leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegFate {
    /// Send it now (always, outside test builds).
    Send,
    /// Report it sent and drop it.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    Swallow,
    /// Report it sent and deliver it this much later.
    #[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
    Delay(std::time::Duration),
}

/// What the test seam does with the inline leg about to be sent for `key`.
/// Always [`LegFate::Send`] outside test builds.
#[inline]
pub(crate) fn inline_leg_fate(
    op_manager: &crate::node::OpManager,
    key: &freenet_stdlib::prelude::ContractKey,
) -> LegFate {
    #[cfg(any(test, feature = "testing"))]
    {
        seam::fate(op_manager.ring.connection_manager.get_own_addr(), key)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let _ = (op_manager, key);
        LegFate::Send
    }
}

/// Whether the test seam refuses the broadcast stream about to start for
/// `key`. Always `false` outside test builds.
#[inline]
pub(crate) fn broadcast_stream_refused(
    op_manager: &crate::node::OpManager,
    key: &freenet_stdlib::prelude::ContractKey,
) -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        seam::stream_fails(op_manager.ring.connection_manager.get_own_addr(), key)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let _ = (op_manager, key);
        false
    }
}
