//! One INFO line per PUT hop decision.
//!
//! Why: a relay's PUT forward used to be logged at DEBUG only, which release
//! builds compile out, and a gateway logging at INFO therefore showed the
//! request arriving and nothing after it. That reads exactly like "the PUT
//! never left this node" and led an investigation to the wrong conclusion.
//! These lines make each hop's decision visible at INFO.
//!
//! Volume: each helper is called at most once per hop per PUT (a decision, not
//! a message or a fragment), the same cadence as the existing
//! `PUT relay: processing Request` line. These helpers add no limiter of their
//! own, but the process-wide per-callsite limiter (`util/rate_limit_layer.rs`,
//! 30 events/s per callsite by default) still applies, and `log_forwarded` is one
//! callsite shared by the streaming and non-streaming drivers, so a node past
//! 30 forwarded PUT hops a second drops the excess (counted in the limiter's
//! periodic summary line).
//!
//! Fields are limited to values the drivers already hold. The geometrically
//! closest candidate is not logged: finding it needs a second routing pass over
//! the ring, which this hot path does not do.

use std::net::SocketAddr;

use freenet_stdlib::prelude::ContractKey;

use crate::message::Transaction;
use crate::ring::Location;

/// Ring distance from `loc` to `target`, or `None` when the location is unknown.
fn distance_to(loc: Option<Location>, target: Location) -> Option<f64> {
    loc.map(|l| l.distance(target).as_f64())
}

fn loc_f64(loc: Option<Location>) -> Option<f64> {
    loc.map(|l| l.as_f64())
}

/// Where a forwarded PUT's next hop came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HopSource {
    /// `Ring::closest_potentially_hosting`, the router's pick.
    Router,
    /// Empty ring: a configured bootstrap gateway.
    BootstrapGateway,
}

impl HopSource {
    fn label(self) -> &'static str {
        match self {
            HopSource::Router => "router_closest_potentially_hosting",
            HopSource::BootstrapGateway => "bootstrap_gateway",
        }
    }
}

/// Why a PUT stopped at this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FinalizeReason {
    /// Greedy-routing terminus guard (#4363): the chain descended here and the
    /// best next hop moves away from the key.
    TerminusGuard,
    /// No candidate next hop and no bootstrap gateway.
    NoNextHop,
    /// The hop budget is spent.
    HtlZero,
    /// A next hop exists but the payload is under the streaming threshold, so
    /// the streaming driver does not pipe it.
    BelowStreamingThreshold,
}

impl FinalizeReason {
    fn label(self) -> &'static str {
        match self {
            FinalizeReason::TerminusGuard => "terminus_guard",
            FinalizeReason::NoNextHop => "no_next_hop",
            FinalizeReason::HtlZero => "htl_zero",
            FinalizeReason::BelowStreamingThreshold => "below_streaming_threshold",
        }
    }

    /// The reason for a hop with no next hop to forward to.
    pub(super) fn for_missing_next_hop(htl: usize) -> Self {
        if htl == 0 {
            FinalizeReason::HtlZero
        } else {
            FinalizeReason::NoNextHop
        }
    }
}

/// This hop forwarded the PUT.
#[allow(clippy::too_many_arguments)]
pub(super) fn log_forwarded(
    tx: Transaction,
    key: ContractKey,
    streaming: bool,
    new_htl: usize,
    next_hop_addr: SocketAddr,
    next_hop_loc: Option<Location>,
    own_loc: Option<Location>,
    target: Location,
    source: HopSource,
) {
    tracing::info!(
        tx = %tx,
        contract = %key,
        streaming,
        htl = new_htl,
        next_hop = %next_hop_addr,
        next_hop_location = ?loc_f64(next_hop_loc),
        next_hop_distance = ?distance_to(next_hop_loc, target),
        own_location = ?loc_f64(own_loc),
        own_distance = ?distance_to(own_loc, target),
        target_location = %target.as_f64(),
        hop_source = source.label(),
        phase = "put_hop_forwarded",
        "PUT hop: forwarded"
    );
}

/// This hop finalized the PUT instead of forwarding it.
///
/// `replicate_to` is the peer a terminus replication copy was addressed to and
/// `replicate_sent` whether that copy went out (it is skipped for payloads that
/// need streaming and can fail to dispatch).
#[allow(clippy::too_many_arguments)]
pub(super) fn log_finalized(
    tx: Transaction,
    key: ContractKey,
    streaming: bool,
    reason: FinalizeReason,
    htl: usize,
    upstream_loc: Option<Location>,
    own_loc: Option<Location>,
    next_hop_loc: Option<Location>,
    target: Location,
    replicate_to: Option<SocketAddr>,
    replicate_sent: bool,
) {
    tracing::info!(
        tx = %tx,
        contract = %key,
        streaming,
        reason = reason.label(),
        htl,
        upstream_location = ?loc_f64(upstream_loc),
        own_location = ?loc_f64(own_loc),
        own_distance = ?distance_to(own_loc, target),
        next_hop_location = ?loc_f64(next_hop_loc),
        next_hop_distance = ?distance_to(next_hop_loc, target),
        target_location = %target.as_f64(),
        replicate_to = ?replicate_to,
        replicate_sent,
        phase = "put_hop_finalized",
        "PUT hop: finalized here"
    );
}

/// Outcome of the originator's summary-first probe.
#[allow(clippy::too_many_arguments)]
pub(super) fn log_summary_first_outcome(
    tx: Transaction,
    key: ContractKey,
    outcome: &'static str,
    holder_found: Option<bool>,
    probed_addr: Option<SocketAddr>,
    probed_loc: Option<Location>,
    target: Location,
    full_state_followed: bool,
) {
    tracing::info!(
        tx = %tx,
        contract = %key,
        outcome,
        holder_found = ?holder_found,
        probed_peer = ?probed_addr,
        probed_location = ?loc_f64(probed_loc),
        probed_distance = ?distance_to(probed_loc, target),
        target_location = %target.as_f64(),
        full_state_followed,
        phase = "put_summary_first_outcome",
        "PUT summary-first: outcome"
    );
}

/// A node answered a `ProbeRequest` from its own state. `source` says why it
/// counts as receiving updates (the holder signal); `None` when the answer is
/// `holder_found=false` for a reason other than the holder branch.
#[allow(clippy::too_many_arguments)]
pub(super) fn log_probe_answered(
    tx: Transaction,
    key: ContractKey,
    upstream: SocketAddr,
    own_loc: Option<Location>,
    holder_found: bool,
    answer: &'static str,
    source: &crate::ring::ReceivingUpdatesSource,
) {
    tracing::info!(
        tx = %tx,
        contract = %key,
        %upstream,
        own_location = ?loc_f64(own_loc),
        own_distance = ?distance_to(own_loc, Location::from(&key)),
        holder_found,
        answer,
        receiving_via = source.label(),
        lease_age_secs = ?source.lease_age.map(|d| d.as_secs_f64()),
        lease_remaining_secs = ?source.lease_remaining.map(|d| d.as_secs_f64()),
        lease_provisional = source.lease_provisional,
        local_clients = source.local_clients,
        local_delegates = source.local_delegates,
        phase = "put_probe_answered",
        "PUT probe: answered from local state"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    fn key() -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([7u8; 32]), CodeHash::new([9u8; 32]))
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn captured(f: impl FnOnce()) -> Vec<String> {
        let (logs, guard) = crate::util::test_log_capture::install();
        f();
        drop(guard);
        logs.lock().unwrap().clone()
    }

    #[test]
    fn forwarded_line_is_info_and_carries_hop_geometry() {
        let target = Location::new(0.5);
        let lines = captured(|| {
            log_forwarded(
                Transaction::new::<crate::operations::put::PutMsg>(),
                key(),
                false,
                7,
                addr(4001),
                Some(Location::new(0.55)),
                Some(Location::new(0.9)),
                target,
                HopSource::Router,
            )
        });
        let hits: Vec<_> = lines
            .iter()
            .filter(|l| l.contains("put_hop_forwarded"))
            .collect();
        assert_eq!(hits.len(), 1, "exactly one line; got {lines:?}");
        let l = hits[0];
        assert!(l.starts_with("INFO"), "must be INFO: {l}");
        for want in [
            "htl=7",
            "10.0.0.1:4001",
            "next_hop_distance=Some(0.05",
            "own_distance=Some(0.4",
            "router_closest_potentially_hosting",
        ] {
            assert!(l.contains(want), "missing {want} in {l}");
        }
    }

    #[test]
    fn finalized_line_names_reason_and_replication() {
        let lines = captured(|| {
            log_finalized(
                Transaction::new::<crate::operations::put::PutMsg>(),
                key(),
                false,
                FinalizeReason::TerminusGuard,
                5,
                Some(Location::new(0.2)),
                Some(Location::new(0.49)),
                Some(Location::new(0.8)),
                Location::new(0.5),
                Some(addr(4002)),
                true,
            )
        });
        let l = lines
            .iter()
            .find(|l| l.contains("put_hop_finalized"))
            .unwrap_or_else(|| panic!("no finalized line in {lines:?}"));
        assert!(l.starts_with("INFO"), "must be INFO: {l}");
        for want in [
            "reason=\"terminus_guard\"",
            "replicate_sent=true",
            "10.0.0.1:4002",
            "upstream_location=Some(0.2)",
        ] {
            assert!(l.contains(want), "missing {want} in {l}");
        }
    }

    #[test]
    fn missing_next_hop_reason_follows_htl() {
        assert_eq!(
            FinalizeReason::for_missing_next_hop(0),
            FinalizeReason::HtlZero
        );
        assert_eq!(
            FinalizeReason::for_missing_next_hop(3),
            FinalizeReason::NoNextHop
        );
    }

    #[test]
    fn summary_first_line_reports_holder_peer_and_fallthrough() {
        let lines = captured(|| {
            log_summary_first_outcome(
                Transaction::new::<crate::operations::put::PutMsg>(),
                key(),
                "new_contract",
                Some(false),
                Some(addr(4003)),
                Some(Location::new(0.6)),
                Location::new(0.5),
                true,
            )
        });
        let l = lines
            .iter()
            .find(|l| l.contains("put_summary_first_outcome"))
            .unwrap_or_else(|| panic!("no outcome line in {lines:?}"));
        assert!(l.starts_with("INFO"), "must be INFO: {l}");
        for want in [
            "holder_found=Some(false)",
            "10.0.0.1:4003",
            "probed_distance=Some(0.09",
            "full_state_followed=true",
        ] {
            assert!(l.contains(want), "missing {want} in {l}");
        }
    }
}
