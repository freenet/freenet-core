//! Bound on relay drivers in flight, per source and node-wide
//! (ANT-2026-K0YQ8CG4).
//!
//! Each inbound relay GET, PUT (streaming or not) and UPDATE from a peer
//! starts a driver task, and each driver sends one event to the contract
//! handler's bounded relay lane (`contract::handler::RELAY_CHANNEL_CAPACITY`)
//! and waits for the answer. A permit is taken when the driver starts and held
//! for the driver's lifetime, so the drivers serving one source, and the
//! node's total, are bounded.
//!
//! - **Per source**, at most [`PER_SOURCE_CAP`] drivers. A source is the
//!   previous hop's IPv4 address or IPv6 /64, the same unit as
//!   `put::stream_admission`. A host holding a larger IPv6 prefix (a /48 holds
//!   65,536 /64s) counts as that many sources, so the per-source cap bounds
//!   one /64 and the node-wide cap is what bounds such a host.
//! - **Node-wide**, at most [`NODE_WIDE_CAP`].
//!
//! Over either cap the request is refused and answered upstream at once, one
//! reply per request: GET with `NotFound` (the requester moves on to its next
//! peer), PUT with `PutMsg::Error` carrying [`PUT_RELAY_OVERLOADED_CAUSE`],
//! on which the originator's PUT driver retries on a different first hop. A
//! refused UPDATE
//! broadcast is repaired the way a rate-limited one is (`node.rs`). The node's
//! own client operations, which reach the same drivers by originator
//! loopback, are not counted.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::put::stream_admission::Source;

/// Relay GET and PUT drivers one source may have in flight at once.
///
/// A quarter of the relay lane, so at least four sources share it before any
/// of them is refused there. An honest gateway relays its joiners' requests
/// under its own address, so this is set well above what one peer's own
/// traffic needs.
pub(crate) const PER_SOURCE_CAP: usize = 256;

/// Relay GET and PUT drivers this node holds across all sources.
pub(crate) const NODE_WIDE_CAP: usize = 4096;

// One source cannot fill the contract handler's relay lane on its own.
const _: () =
    assert!(PER_SOURCE_CAP > 0 && PER_SOURCE_CAP < crate::contract::RELAY_CHANNEL_CAPACITY);

/// The `PutMsg::Error` cause a relay sends when it cannot take a PUT for
/// overload. The originator's PUT driver advances to its next peer on it
/// instead of failing the PUT (`put::op_ctx_task::classify_reply`). Short, so
/// `bound_cause` passes it through unchanged on every hop.
pub(crate) const PUT_RELAY_OVERLOADED_CAUSE: &str = "PUT relay: peer overloaded, try another";

/// Relay requests refused by any [`RelayDriverAdmission`], for telemetry.
static REFUSED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Relay GETs answered `NotFound` because this node could not take them
/// (permit refused, or the contract handler refused the local query), as
/// opposed to a real miss. For telemetry.
static OVERLOAD_NOT_FOUND_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_overload_not_found() {
    OVERLOAD_NOT_FOUND_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// `(relay admission refusals, overload NotFounds)` so far, for the telemetry
/// snapshot.
pub(crate) fn telemetry_counts() -> (u64, u64) {
    (
        REFUSED_TOTAL.load(Ordering::Relaxed),
        OVERLOAD_NOT_FOUND_TOTAL.load(Ordering::Relaxed),
    )
}

/// Simulation hook (ANT-2026-K0YQ8CG4): nodes, by address, whose contract
/// handler is treated as refusing every relay query, as a full relay lane
/// does. Process-global; a test that sets it runs in its own process (nextest)
/// and clears it when done.
#[cfg(any(test, feature = "testing"))]
pub(crate) mod sim_overload {
    use std::net::SocketAddr;
    use std::sync::OnceLock;

    fn saturated() -> &'static dashmap::DashSet<SocketAddr> {
        static SATURATED: OnceLock<dashmap::DashSet<SocketAddr>> = OnceLock::new();
        SATURATED.get_or_init(dashmap::DashSet::new)
    }

    pub(crate) fn saturate(addr: SocketAddr) {
        saturated().insert(addr);
    }

    pub(crate) fn clear() {
        saturated().clear();
    }

    pub(crate) fn is_saturated(addr: Option<SocketAddr>) -> bool {
        addr.is_some_and(|addr| saturated().contains(&addr))
    }
}

/// Why a request was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Refusal {
    SourceAtCap,
    NodeAtCap,
}

pub(crate) struct RelayDriverAdmission {
    state: parking_lot::Mutex<State>,
    per_source_cap: usize,
    node_wide_cap: usize,
    refused_total: AtomicU64,
}

#[derive(Default)]
struct State {
    total: usize,
    per_source: HashMap<Source, usize>,
}

/// One admitted driver. Dropping it releases the slot.
pub(crate) struct RelayDriverPermit {
    admission: Arc<RelayDriverAdmission>,
    source: Source,
}

impl Drop for RelayDriverPermit {
    fn drop(&mut self) {
        let mut state = self.admission.state.lock();
        state.total = state.total.saturating_sub(1);
        if let Some(count) = state.per_source.get_mut(&self.source) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_source.remove(&self.source);
            }
        }
    }
}

impl RelayDriverAdmission {
    pub(crate) fn new() -> Self {
        Self::with_caps(PER_SOURCE_CAP, NODE_WIDE_CAP)
    }

    pub(crate) fn with_caps(per_source_cap: usize, node_wide_cap: usize) -> Self {
        Self {
            state: parking_lot::Mutex::new(State::default()),
            per_source_cap: per_source_cap.max(1),
            node_wide_cap: node_wide_cap.max(1),
            refused_total: AtomicU64::new(0),
        }
    }

    /// Admit one driver for a request from `from`, or refuse it.
    pub(crate) fn try_admit(
        self: &Arc<Self>,
        from: SocketAddr,
    ) -> Result<RelayDriverPermit, Refusal> {
        let source = Source::of(from);
        let mut state = self.state.lock();
        let refusal = if state.per_source.get(&source).copied().unwrap_or(0) >= self.per_source_cap
        {
            Some(Refusal::SourceAtCap)
        } else if state.total >= self.node_wide_cap {
            Some(Refusal::NodeAtCap)
        } else {
            None
        };
        if let Some(refusal) = refusal {
            drop(state);
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            REFUSED_TOTAL.fetch_add(1, Ordering::Relaxed);
            return Err(refusal);
        }
        state.total += 1;
        *state.per_source.entry(source).or_insert(0) += 1;
        drop(state);
        Ok(RelayDriverPermit {
            admission: self.clone(),
            source,
        })
    }

    /// [`Self::try_admit`], logging a refusal at info at powers of two of
    /// this admission's running total, so sustained refusals are a handful of
    /// lines. `op` names the driver in the log.
    pub(crate) fn try_admit_logged(
        self: &Arc<Self>,
        from: SocketAddr,
        op: &'static str,
    ) -> Result<RelayDriverPermit, Refusal> {
        let result = self.try_admit(from);
        if let Err(refusal) = &result {
            let refused = self.refused_total();
            if refused.is_power_of_two() {
                tracing::info!(
                    op,
                    upstream = %from,
                    ?refusal,
                    refused_total = refused,
                    in_flight = self.in_flight(),
                    phase = "relay_admission_refused",
                    "Relay request refused: too many relay requests in flight. \
                     Logged at powers of two of the running total"
                );
            }
        }
        result
    }

    /// Requests refused so far.
    pub(crate) fn refused_total(&self) -> u64 {
        self.refused_total.load(Ordering::Relaxed)
    }

    /// Drivers currently holding a permit.
    pub(crate) fn in_flight(&self) -> usize {
        self.state.lock().total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    /// One source at its cap is refused; a second source is still admitted.
    /// FALSIFY: drop the per-source check in `try_admit`.
    #[test]
    fn one_saturated_source_does_not_lock_out_another() {
        let admission = Arc::new(RelayDriverAdmission::new());
        let held: Vec<_> = (0..PER_SOURCE_CAP)
            .map(|i| {
                admission
                    .try_admit(addr(&format!("203.0.113.7:{}", 1000 + i)))
                    .expect("under the source cap")
            })
            .collect();
        assert_eq!(
            admission.try_admit(addr("203.0.113.7:60000")).err(),
            Some(Refusal::SourceAtCap),
            "the saturated source is refused, on any port"
        );
        assert!(
            admission.try_admit(addr("198.51.100.1:1")).is_ok(),
            "a second source is still admitted"
        );
        assert_eq!(admission.refused_total(), 1);
        drop(held);
        assert!(
            admission.try_admit(addr("203.0.113.7:1")).is_ok(),
            "released permits return the source's budget"
        );
    }

    /// The node-wide cap holds across sources.
    /// FALSIFY: drop the node-wide check in `try_admit`.
    #[test]
    fn the_node_wide_cap_holds_across_sources() {
        let admission = Arc::new(RelayDriverAdmission::with_caps(2, 3));
        let _a = admission.try_admit(addr("192.0.2.1:1")).unwrap();
        let _b = admission.try_admit(addr("192.0.2.2:1")).unwrap();
        let c = admission.try_admit(addr("192.0.2.3:1")).unwrap();
        assert_eq!(
            admission.try_admit(addr("192.0.2.4:1")).err(),
            Some(Refusal::NodeAtCap)
        );
        drop(c);
        assert_eq!(admission.in_flight(), 2);
        assert!(admission.try_admit(addr("192.0.2.4:1")).is_ok());
    }

    /// A driver holds its permit as a value, so the slot comes back however
    /// the driver ends: it returns, returns an error, panics, or is aborted
    /// (its future dropped, as on node shutdown) before or after it started.
    /// FALSIFY: release the slot only on an explicit call instead of in `Drop`.
    #[tokio::test]
    async fn a_permit_is_released_however_its_driver_ends() {
        let admission = Arc::new(RelayDriverAdmission::with_caps(1, 1));
        let from = addr("192.0.2.1:1");

        let permit = admission.try_admit(from).unwrap();
        tokio::spawn(async move {
            let _permit = permit;
        })
        .await
        .unwrap();
        assert_eq!(admission.in_flight(), 0, "returned");

        let permit = admission.try_admit(from).unwrap();
        let errored: Result<(), &str> = tokio::spawn(async move {
            let _permit = permit;
            Err("driver error")
        })
        .await
        .unwrap();
        assert!(errored.is_err());
        assert_eq!(admission.in_flight(), 0, "errored");

        let permit = admission.try_admit(from).unwrap();
        let panicked = tokio::spawn(async move {
            let _permit = permit;
            panic!("driver panicked");
        })
        .await;
        assert!(panicked.is_err());
        assert_eq!(admission.in_flight(), 0, "panicked");

        // Aborted while waiting (a driver parked on the handler's answer).
        let permit = admission.try_admit(from).unwrap();
        let waiting = tokio::spawn(async move {
            let _permit = permit;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        assert_eq!(admission.in_flight(), 1);
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(admission.in_flight(), 0, "aborted while waiting");

        // Dropped before its first poll.
        let permit = admission.try_admit(from).unwrap();
        let never_polled = async move {
            let _permit = permit;
        };
        drop(never_polled);
        assert_eq!(admission.in_flight(), 0, "dropped unpolled");
        assert!(admission.try_admit(from).is_ok());
    }

    #[test]
    fn an_ipv6_slash_64_is_one_source() {
        let admission = Arc::new(RelayDriverAdmission::with_caps(1, 10));
        let _a = admission.try_admit(addr("[2001:db8:1:2::1]:1")).unwrap();
        assert_eq!(
            admission.try_admit(addr("[2001:db8:1:2::ffff]:2")).err(),
            Some(Refusal::SourceAtCap)
        );
        assert!(admission.try_admit(addr("[2001:db8:1:3::1]:1")).is_ok());
    }
}
