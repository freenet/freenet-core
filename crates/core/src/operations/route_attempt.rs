//! Per-attempt route-outcome labelling for the router's failure-probability
//! model (#4485).
//!
//! The router asks "if I route a request for this contract via this peer,
//! will it deliver?". Before this module, the originator drivers for GET, PUT
//! and SUBSCRIBE fed the router only the FINAL success of an operation, and
//! the relay drivers recorded a downstream `NotFound` as a success. A
//! production gateway logged 361 route events in 2.7 h with 2 failures: the
//! model was trained almost exclusively on positive labels.
//!
//! Everything that labels a NON-success attempt goes through
//! [`RouteAttemptRecorder`]. Success events keep their existing call sites
//! (they carry op-specific timing) and additionally tell the recorder that the
//! contract exists via [`RouteAttemptRecorder::contract_exists`].
//!
//! # Labels
//!
//! | Attempt outcome | Label | When |
//! |---|---|---|
//! | timeout, peer disconnected / send failure | `Failure` | immediately |
//! | `NotFound`, later attempt in the same op proved the contract exists | `Failure` | when existence is proven |
//! | `NotFound`, op ended without proving existence | [`ambiguous_not_found_policy`] | when the recorder is dropped |
//!
//! Failures here feed the router ONLY — never `peer_health` (whose 90 %
//! failure-rate / zero-success criteria evict connections) and never the
//! topology manager. A peer that promptly answers "I don't have this" is not
//! an unhealthy connection, and an originator-side timeout covers the whole
//! downstream chain, not just the first hop. See
//! [`crate::ring::Ring::record_route_failure`].
//!
//! # Attribution at the originator ([`AttemptHopRegistry`])
//!
//! GET and PUT originators send each attempt to their OWN node
//! (`OpCtx::send_and_await`), where the originator-loopback relay driver picks
//! the real first hop and fire-and-forgets the request to it. The client
//! driver's `current_target` is only its own guess (PUT picks it with a
//! different function, and GET retries re-pick it from a `tried` set the
//! loopback relay never sees), so it cannot be used to blame a peer. The retry
//! loop therefore registers a slot per attempt transaction, the loopback relay
//! fills it with the peer it actually forwarded to, and the loop reads it back
//! when the attempt resolves. An empty slot means no remote peer was attempted
//! (local completion, no routing candidates, dispatch failure) and nothing is
//! recorded.

use std::sync::Arc;

use dashmap::DashMap;

use crate::message::Transaction;
use crate::node::network_status::OpType;
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

/// What to do with `NotFound` attempts from an operation that ended without
/// ever proving the contract exists.
///
/// Such a `NotFound` is ambiguous: either the contract exists and routing
/// dead-ended (a real routing failure of that peer), or the contract does not
/// exist at all (no information about the peer). The two cannot be told apart
/// locally.
///
/// The current policy rests on a working assumption, not a measurement:
/// requests for absent contracts are spread uniformly around the ring, so the
/// label noise they add is peer-independent and washes out at scale. Two known
/// ways that assumption fails:
///
/// 1. **Hot missing keys.** Many requests for the same absent contract (a
///    stale link, a mistyped key, a deleted app) all route toward the same key
///    location, so the peers nearest that location collect a concentrated run
///    of blameless failures and get de-prioritised for every contract near it.
/// 2. **Exhaustion depth.** An all-`NotFound` operation labels every candidate
///    it tried. Lower-ranked candidates are only tried after the better ones
///    fail, so for absent contracts they collect failures in proportion to how
///    often the search gets that deep, a bias against peers that are rarely
///    first choice.
///
/// Whether either matters at production scale is an open question (a
/// synthetic bake-off with clustered hot missing keys is planned). Switching
/// policy is a local change to [`ambiguous_not_found_policy`]; a delayed
/// labelling store (release parked attempts only once something proves the
/// contract exists) would slot in as a third variant settled in the same
/// place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmbiguousNotFoundPolicy {
    /// Label every ambiguous `NotFound` attempt as `RouteOutcome::Failure`.
    TrainAsFailure,
    /// Drop ambiguous `NotFound` attempts without training on them.
    // Not selected in production today; it is the swap seam described on the
    // enum and is exercised by the unit tests.
    #[cfg_attr(not(test), allow(dead_code))]
    DoNotTrain,
}

/// The one place that decides how ambiguous `NotFound` attempts are labelled.
/// See [`AmbiguousNotFoundPolicy`] for the assumption and its failure modes.
pub(crate) const fn ambiguous_not_found_policy() -> AmbiguousNotFoundPolicy {
    AmbiguousNotFoundPolicy::TrainAsFailure
}

/// A non-success outcome of one attempt against one peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttemptFailure {
    /// The peer answered that it could not find the contract.
    NotFound,
    /// No terminal reply arrived within the attempt deadline.
    Timeout,
    /// The request could not be delivered, or the connection to the peer was
    /// dropped while awaiting the reply.
    SendFailure,
}

/// Which side of the operation the recorder labels for. Relay events also
/// advance the `RELAY_*_ROUTE_EVENT_COUNT` test hooks, exactly like
/// [`crate::operations::record_relay_route_event`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttemptOrigin {
    Originator,
    Relay,
}

/// Where failure labels go. Implemented by [`crate::ring::Ring`] (router
/// only); unit tests substitute a recording sink.
pub(crate) trait RouteFailureSink: Send + Sync {
    fn record_route_failure(&self, event: RouteEvent);
}

impl RouteFailureSink for crate::ring::Ring {
    fn record_route_failure(&self, event: RouteEvent) {
        crate::ring::Ring::record_route_failure(self, event);
    }
}

/// Owns the labelling of every non-success attempt of ONE operation (one
/// originator op, or one relay driver invocation). Task-local; never shared.
///
/// Dropping the recorder settles the operation: any `NotFound` attempts still
/// pending (the contract was never proven to exist) are handled by
/// [`ambiguous_not_found_policy`]. Settling on `Drop` means no exit path — an
/// early `return`, a `?`, an exhausted retry budget — can forget them.
pub(crate) struct RouteAttemptRecorder {
    sink: Option<Arc<dyn RouteFailureSink>>,
    contract_location: Location,
    op_type: OpType,
    origin: AttemptOrigin,
    policy: AmbiguousNotFoundPolicy,
    pending_not_found: Vec<PeerKeyLocation>,
    contract_known_to_exist: bool,
}

impl RouteAttemptRecorder {
    pub(crate) fn new(
        sink: Arc<dyn RouteFailureSink>,
        contract_location: Location,
        op_type: OpType,
        origin: AttemptOrigin,
    ) -> Self {
        Self::with_policy(
            Some(sink),
            contract_location,
            op_type,
            origin,
            ambiguous_not_found_policy(),
        )
    }

    /// A recorder that records nothing. Used by drivers that deliberately do
    /// not feed the router (sub-operation GETs).
    pub(crate) fn disabled(contract_location: Location, op_type: OpType) -> Self {
        Self::with_policy(
            None,
            contract_location,
            op_type,
            AttemptOrigin::Originator,
            ambiguous_not_found_policy(),
        )
    }

    fn with_policy(
        sink: Option<Arc<dyn RouteFailureSink>>,
        contract_location: Location,
        op_type: OpType,
        origin: AttemptOrigin,
        policy: AmbiguousNotFoundPolicy,
    ) -> Self {
        Self {
            sink,
            contract_location,
            op_type,
            origin,
            policy,
            pending_not_found: Vec::new(),
            contract_known_to_exist: false,
        }
    }

    /// Record a non-success outcome of one attempt.
    ///
    /// `peer` is the peer the request was ACTUALLY sent to. `None` means no
    /// remote peer can be blamed and nothing is recorded — never substitute a
    /// guessed target here.
    pub(crate) fn record_attempt(
        &mut self,
        peer: Option<&PeerKeyLocation>,
        outcome: AttemptFailure,
    ) {
        let Some(peer) = peer else {
            return;
        };
        match outcome {
            AttemptFailure::Timeout | AttemptFailure::SendFailure => {
                self.emit_failure(peer.clone());
            }
            AttemptFailure::NotFound if self.contract_known_to_exist => {
                self.emit_failure(peer.clone());
            }
            AttemptFailure::NotFound => self.pending_not_found.push(peer.clone()),
        }
    }

    /// Evidence from THIS operation that the contract exists (a found reply, a
    /// streaming header, a subscription, a local copy). Every pending
    /// `NotFound` is a genuine routing failure; later ones are labelled
    /// immediately. Idempotent.
    pub(crate) fn contract_exists(&mut self) {
        self.contract_known_to_exist = true;
        for peer in std::mem::take(&mut self.pending_not_found) {
            self.emit_failure(peer);
        }
    }

    fn emit_failure(&self, peer: PeerKeyLocation) {
        let Some(sink) = &self.sink else {
            return;
        };
        if self.origin == AttemptOrigin::Relay {
            crate::operations::count_relay_route_event(self.op_type);
        }
        sink.record_route_failure(RouteEvent {
            peer,
            contract_location: self.contract_location,
            outcome: RouteOutcome::Failure,
            op_type: Some(self.op_type),
        });
    }
}

impl Drop for RouteAttemptRecorder {
    fn drop(&mut self) {
        let pending = std::mem::take(&mut self.pending_not_found);
        match self.policy {
            AmbiguousNotFoundPolicy::TrainAsFailure => {
                for peer in pending {
                    self.emit_failure(peer);
                }
            }
            AmbiguousNotFoundPolicy::DoNotTrain => {}
        }
    }
}

/// Per-attempt record of the peer an originator's request was actually
/// forwarded to. See the module docs for why the client driver cannot know it.
///
/// The retry loop [`register`](Self::register)s a slot BEFORE sending (the
/// loopback relay may run before `send_and_await` returns), the loopback relay
/// fills an existing slot via [`record_hop`](Self::record_hop), and the
/// returned [`AttemptHopGuard`] removes the slot on every exit, including
/// cancellation. `record_hop` never inserts, so a late relay cannot leak an
/// entry: the registry holds at most one entry per in-flight attempt.
#[derive(Default)]
pub(crate) struct AttemptHopRegistry {
    slots: DashMap<Transaction, Option<PeerKeyLocation>>,
}

impl AttemptHopRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(self: &Arc<Self>, tx: Transaction) -> AttemptHopGuard {
        self.slots.insert(tx, None);
        AttemptHopGuard {
            registry: self.clone(),
            tx,
        }
    }

    /// Called by the originator-loopback relay immediately before it dispatches
    /// the request to `peer`. A no-op when no attempt is registered for `tx`
    /// (a relay hop for a remote upstream, or an attempt already resolved).
    pub(crate) fn record_hop(&self, tx: &Transaction, peer: &PeerKeyLocation) {
        if let Some(mut slot) = self.slots.get_mut(tx) {
            *slot = Some(peer.clone());
        }
    }

    /// Undo [`record_hop`](Self::record_hop) when the dispatch failed locally,
    /// so the attempt is not blamed on a peer that never saw the request.
    pub(crate) fn clear_hop(&self, tx: &Transaction) {
        if let Some(mut slot) = self.slots.get_mut(tx) {
            *slot = None;
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

/// Owns one [`AttemptHopRegistry`] slot; removes it on drop.
pub(crate) struct AttemptHopGuard {
    registry: Arc<AttemptHopRegistry>,
    tx: Transaction,
}

impl AttemptHopGuard {
    /// The peer the attempt was forwarded to, if the loopback relay recorded one.
    pub(crate) fn hop(&self) -> Option<PeerKeyLocation> {
        self.registry
            .slots
            .get(&self.tx)
            .and_then(|slot| slot.clone())
    }
}

impl Drop for AttemptHopGuard {
    fn drop(&mut self) {
        self.registry.slots.remove(&self.tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::get::GetMsg;
    use crate::transport::TransportKeypair;
    use parking_lot::Mutex;

    #[derive(Default)]
    struct VecSink(Mutex<Vec<RouteEvent>>);

    impl RouteFailureSink for VecSink {
        fn record_route_failure(&self, event: RouteEvent) {
            self.0.lock().push(event);
        }
    }

    impl VecSink {
        fn failed_peers(&self) -> Vec<std::net::SocketAddr> {
            self.0
                .lock()
                .iter()
                .map(|e| {
                    assert!(matches!(e.outcome, RouteOutcome::Failure));
                    e.peer.socket_addr().expect("test peers have addresses")
                })
                .collect()
        }
    }

    fn peer(port: u16) -> PeerKeyLocation {
        let addr: std::net::SocketAddr = format!("10.0.0.1:{port}").parse().unwrap();
        PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr)
    }

    fn recorder(sink: &Arc<VecSink>, policy: AmbiguousNotFoundPolicy) -> RouteAttemptRecorder {
        RouteAttemptRecorder::with_policy(
            Some(sink.clone() as Arc<dyn RouteFailureSink>),
            Location::new(0.25),
            OpType::Get,
            AttemptOrigin::Originator,
            policy,
        )
    }

    fn addr(p: &PeerKeyLocation) -> std::net::SocketAddr {
        p.socket_addr().unwrap()
    }

    #[test]
    fn production_policy_trains_ambiguous_not_found_as_failure() {
        assert_eq!(
            ambiguous_not_found_policy(),
            AmbiguousNotFoundPolicy::TrainAsFailure
        );
    }

    #[test]
    fn timeout_and_send_failure_are_labelled_immediately() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::TrainAsFailure);
        rec.record_attempt(Some(&a), AttemptFailure::Timeout);
        assert_eq!(sink.failed_peers(), vec![addr(&a)]);
        rec.record_attempt(Some(&b), AttemptFailure::SendFailure);
        assert_eq!(sink.failed_peers(), vec![addr(&a), addr(&b)]);
        drop(rec);
        assert_eq!(sink.failed_peers().len(), 2, "drop must not re-emit");
    }

    #[test]
    fn not_found_is_not_labelled_until_the_operation_resolves() {
        let sink = Arc::new(VecSink::default());
        let a = peer(1);
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::TrainAsFailure);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound);
        assert!(sink.failed_peers().is_empty());
        drop(rec);
        assert_eq!(sink.failed_peers(), vec![addr(&a)]);
    }

    #[test]
    fn not_found_then_success_labels_each_not_found_peer_exactly_once() {
        let sink = Arc::new(VecSink::default());
        let (a, b, c) = (peer(1), peer(2), peer(3));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::DoNotTrain);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound);
        rec.record_attempt(Some(&b), AttemptFailure::NotFound);
        rec.contract_exists();
        // Proven even under the DoNotTrain policy: these are not ambiguous.
        assert_eq!(sink.failed_peers(), vec![addr(&a), addr(&b)]);
        // A NotFound after existence was proven is labelled immediately.
        rec.record_attempt(Some(&c), AttemptFailure::NotFound);
        rec.contract_exists();
        drop(rec);
        assert_eq!(sink.failed_peers(), vec![addr(&a), addr(&b), addr(&c)]);
    }

    #[test]
    fn exhausted_all_not_found_labels_each_attempted_peer_once() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::TrainAsFailure);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound);
        rec.record_attempt(Some(&b), AttemptFailure::Timeout);
        assert_eq!(sink.failed_peers(), vec![addr(&b)]);
        drop(rec);
        assert_eq!(sink.failed_peers(), vec![addr(&b), addr(&a)]);
    }

    #[test]
    fn do_not_train_policy_drops_ambiguous_not_found() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::DoNotTrain);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound);
        rec.record_attempt(Some(&b), AttemptFailure::Timeout);
        drop(rec);
        assert_eq!(
            sink.failed_peers(),
            vec![addr(&b)],
            "timeouts are never ambiguous; only the NotFound is dropped"
        );
    }

    #[test]
    fn unattributable_attempt_and_zero_attempts_record_nothing() {
        let sink = Arc::new(VecSink::default());
        {
            let _untouched = recorder(&sink, AmbiguousNotFoundPolicy::TrainAsFailure);
        }
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::TrainAsFailure);
        rec.record_attempt(None, AttemptFailure::NotFound);
        rec.record_attempt(None, AttemptFailure::Timeout);
        rec.record_attempt(None, AttemptFailure::SendFailure);
        drop(rec);
        assert!(sink.failed_peers().is_empty());
    }

    #[test]
    fn disabled_recorder_records_nothing() {
        let mut rec = RouteAttemptRecorder::disabled(Location::new(0.5), OpType::Get);
        let a = peer(1);
        rec.record_attempt(Some(&a), AttemptFailure::Timeout);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound);
        rec.contract_exists();
        // Nothing to assert on beyond "no panic": there is no sink.
    }

    #[test]
    fn events_carry_contract_location_and_op_type() {
        let sink = Arc::new(VecSink::default());
        let a = peer(1);
        let mut rec = RouteAttemptRecorder::with_policy(
            Some(sink.clone() as Arc<dyn RouteFailureSink>),
            Location::new(0.75),
            OpType::Subscribe,
            AttemptOrigin::Originator,
            AmbiguousNotFoundPolicy::TrainAsFailure,
        );
        rec.record_attempt(Some(&a), AttemptFailure::Timeout);
        let events = sink.0.lock();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].contract_location, Location::new(0.75));
        assert_eq!(events[0].op_type, Some(OpType::Subscribe));
    }

    #[test]
    fn hop_registry_reports_recorded_hop_and_cleans_up() {
        let registry = Arc::new(AttemptHopRegistry::new());
        let tx = Transaction::new::<GetMsg>();
        let a = peer(1);

        // Unregistered tx: recording is a no-op and leaves nothing behind.
        registry.record_hop(&tx, &a);
        assert_eq!(registry.len(), 0);

        let guard = registry.register(tx);
        assert!(guard.hop().is_none(), "no hop until the relay forwards");
        registry.record_hop(&tx, &a);
        assert_eq!(guard.hop().as_ref().map(addr), Some(addr(&a)));
        registry.clear_hop(&tx);
        assert!(guard.hop().is_none(), "a failed dispatch blames nobody");
        registry.record_hop(&tx, &a);
        drop(guard);
        assert_eq!(registry.len(), 0, "guard drop must remove the slot");

        // A relay that records after the attempt resolved cannot leak.
        registry.record_hop(&tx, &a);
        assert_eq!(registry.len(), 0);
    }
}
