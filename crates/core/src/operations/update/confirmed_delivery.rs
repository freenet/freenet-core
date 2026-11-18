//! Confirmed delivery for a node's own originated UPDATE broadcasts (#6143).
//!
//! See `inline_leg_fault_injection` for the test seam that reproduces the
//! #6143 failure class.

/// Test seam reproducing the #6143 failure class: a node whose INLINE
/// broadcast legs (`BroadcastTo*` sent as one message, not as a caller-named
/// stream) are accepted by the bridge and then silently lost.
///
/// In #6143 the transport assigned the node's internal stream ids a reserved
/// bit after days of uptime, so every oversized inline message it sent was
/// parked by the receiver as an unclaimed stream. The bridge reported success,
/// the sender cached its own summary as the peer's, and nothing told it the
/// peer never applied the update. The explicit broadcast streams kept working.
///
/// A rule names the SENDER's address and the contract. A matching inline leg
/// is reported as sent (the caller's success arm runs, exactly as it did in
/// #6143) and never reaches the bridge. The originator's routed
/// `RequestUpdate` is an inline message too and is swallowed the same way.
/// Streamed legs are untouched.
#[cfg(any(test, feature = "testing"))]
pub mod inline_leg_fault_injection {
    use freenet_stdlib::prelude::ContractKey;
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock};

    /// Inline legs the seam has swallowed, over the process lifetime.
    pub static DROPPED_INLINE_LEGS: AtomicUsize = AtomicUsize::new(0);

    fn rules() -> &'static Mutex<HashSet<(SocketAddr, ContractKey)>> {
        static RULES: OnceLock<Mutex<HashSet<(SocketAddr, ContractKey)>>> = OnceLock::new();
        RULES.get_or_init(|| Mutex::new(HashSet::new()))
    }

    /// Silently drop every inline broadcast leg `sender` sends for `key`.
    pub fn drop_inline_legs_from(sender: SocketAddr, key: ContractKey) {
        rules()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((sender, key));
    }

    /// Remove every rule for `key`.
    pub fn clear(key: &ContractKey) {
        rules()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(_, k)| k != key);
    }

    /// Whether an inline leg from `sender` for `key` is to be swallowed.
    /// Counts each one it swallows.
    pub(crate) fn swallows(sender: Option<SocketAddr>, key: &ContractKey) -> bool {
        let Some(sender) = sender else {
            return false;
        };
        let hit = rules()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&(sender, *key));
        if hit {
            DROPPED_INLINE_LEGS.fetch_add(1, Ordering::SeqCst);
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
        inline_leg_fault_injection::swallows(op_manager.ring.connection_manager.get_own_addr(), key)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let _ = (op_manager, key);
        false
    }
}
