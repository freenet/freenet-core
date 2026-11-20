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
        /// Inline legs swallowed, per contract. Per contract rather than one
        /// process-wide count because simulation tests run in parallel in one
        /// process; each test uses its own contract.
        swallowed: HashMap<ContractKey, usize>,
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
        state.swallowed.remove(key);
    }

    /// Whether an inline leg from `sender` for `key` is to be swallowed.
    /// Counts each one it swallows.
    pub(crate) fn swallows(sender: Option<SocketAddr>, key: &ContractKey) -> bool {
        let Some(sender) = sender else {
            return false;
        };
        let mut state = state();
        let hit = state.rules.contains(&(sender, *key));
        if hit {
            *state.swallowed.entry(*key).or_insert(0) += 1;
        }
        hit
    }
}

/// Whether the inline leg about to be sent for `key` is swallowed by the
/// test seam. Always `false` outside test builds.
#[inline]
pub(crate) fn inline_leg_swallowed(
    op_manager: &crate::node::OpManager,
    key: &freenet_stdlib::prelude::ContractKey,
) -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        seam::swallows(op_manager.ring.connection_manager.get_own_addr(), key)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let _ = (op_manager, key);
        false
    }
}
