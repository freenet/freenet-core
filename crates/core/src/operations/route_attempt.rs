//! Per-attempt route-outcome labelling for the router's failure-probability
//! model (#5657).
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
//! | Attempt outcome | Label |
//! |---|---|
//! | timeout, send failure, connection to the attempted peer dropped | `Failure`, immediately |
//! | `NotFound`, and a later reply in the SAME operation proves the contract exists | `Failure`, when existence is proven |
//! | `NotFound`, operation ends without that proof | [`ambiguous_not_found_policy`] (default: not trained) |
//!
//! Each peer is labelled a failure at most ONCE per operation, however many
//! times it timed out or answered `NotFound` within it.
//!
//! Failures here feed the router ONLY, never `peer_health` (whose 90 %
//! failure-rate / zero-success criteria evict connections). A peer that
//! promptly answers "I don't have this" is not an unhealthy connection, and an
//! originator-side timeout covers the whole downstream chain, not just the
//! first hop: CHAIN BLAME, accepted for router-only labels and watched through
//! the `timeout_label_*` histogram on the router snapshot. The failure inputs
//! `peer_health` always had (a GET stream that never arrived, a client GET
//! whose delivery failed) are kept separately; see
//! [`crate::ring::Ring::report_route_outcome_to_health`].
//!
//! Existence proof requires state this node stored: state for the requested
//! contract, from a peer this operation contacted, that this node's store
//! accepted or already held. This node's own copy, a local completion or
//! later evidence proves nothing. A SUBSCRIBE reply carries no state, so a
//! SUBSCRIBE never has existence proof and its `NotFound`s are never trained.
//!
//! `FREENET_ROUTING_LEGACY_LABELS=1` restores the pre-#5657 labels exactly
//! ([`label_mode`]).
//!
//! # Attribution at the originator ([`AttemptHopRegistry`])
//!
//! GET and PUT originators send each attempt to their OWN node
//! (`OpCtx::send_and_await`), where the originator-loopback relay driver picks
//! the real first hop and fire-and-forgets the request to it. The client
//! driver's `current_target` is only its own guess, so it must not be blamed
//! or credited. The retry loop registers a slot per attempt transaction, the
//! loopback relay fills it with the peer it actually forwarded to, and the
//! loop reads it back when the attempt resolves. An empty slot means no remote
//! peer was attempted (local completion, no routing candidates, dispatch
//! failure) and nothing is recorded, success or failure.

use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;

use crate::message::Transaction;
use crate::node::network_status::OpType;
use crate::ring::{Location, PeerKeyLocation};
use crate::router::{RouteEvent, RouteOutcome};

/// What to do with `NotFound` attempts from an operation that ended without
/// ever proving the contract exists.
///
/// Such a `NotFound` is ambiguous: either the contract exists and routing
/// dead-ended (a real routing failure of that peer), or the contract does not
/// exist, or did not exist YET (no information about the peer). The two cannot
/// be told apart locally, not even later: a contract that exists now may not
/// have existed when the `NotFound` was returned (a GET before its PUT gets
/// correct `NotFound`s from exactly the peers the PUT then stores at).
///
/// Training on them ([`Self::Naive`]) assumes requests for absent contracts
/// wash out. A synthetic bake-off (branch `exp/estimator-bakeoff`, commit
/// e26a92c1d) tested that with absent keys spread UNIFORMLY around the ring,
/// the most favourable case: naive labelling still raised model error
/// 1.5-1.8x at 5 % absent requests and 6-9x at 20 %, and cut how often the
/// truly best of the 10 nearest candidates was picked by 7-11 points.
///
/// [`Self::Untrained`] drops them. Residual bias it accepts: a dead-end on a
/// contract that does exist, in an operation that never found it, is never
/// learned; the router learns about that peer only from timeouts and from
/// operations that later found the contract elsewhere.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmbiguousNotFoundPolicy {
    /// Label every ambiguous `NotFound` attempt `RouteOutcome::Failure` when
    /// the operation ends.
    // Not selected in production; kept as the swap seam and exercised by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    Naive,
    /// Never train on an ambiguous `NotFound` attempt.
    Untrained,
}

/// The one place that decides how ambiguous `NotFound` attempts are labelled.
/// See [`AmbiguousNotFoundPolicy`] for the evidence behind the choice.
pub(crate) const fn ambiguous_not_found_policy() -> AmbiguousNotFoundPolicy {
    AmbiguousNotFoundPolicy::Untrained
}

/// A non-success outcome of one attempt against one peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AttemptFailure {
    /// The peer answered that it could not find the contract.
    NotFound,
    /// No terminal reply arrived within the attempt deadline.
    Timeout,
    /// The request could not be delivered, the connection to the attempted
    /// peer was dropped while awaiting the reply, or the peer announced a
    /// streamed reply that never arrived.
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

/// Which labelling rules are in force. See [`label_mode`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LabelMode {
    /// The #5657 rules described in the module docs.
    Current,
    /// Exactly the labelling that shipped before #5657, restored by
    /// `FREENET_ROUTING_LEGACY_LABELS=1`: originators label no attempt (only
    /// their final outcome, against `current_target`), relays label a
    /// downstream `NotFound` as `SuccessUntimed` and every transport failure
    /// as `Failure`.
    Legacy,
}

/// The labelling rules in force, from `FREENET_ROUTING_LEGACY_LABELS` (read
/// once per process; `1`/`true`/`yes`/`on`, case-insensitive, selects
/// [`LabelMode::Legacy`]; anything else, or unset, keeps
/// [`LabelMode::Current`]). A kill switch for the soak: it restores the
/// pre-#5657 router inputs without a rebuild.
pub(crate) fn label_mode() -> LabelMode {
    // Thread-local test override, for the same reason as the router's
    // `residual_correction_enabled`: a process-global OnceLock is resolved by
    // whichever test touches it first, which would make one branch untestable
    // and let tests interfere under plain `cargo test`.
    #[cfg(test)]
    {
        if let Some(mode) = TEST_LABEL_MODE.with(|cell| cell.get()) {
            return mode;
        }
    }
    static MODE: std::sync::OnceLock<LabelMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| parse_legacy_labels(std::env::var("FREENET_ROUTING_LEGACY_LABELS").ok()))
}

/// Fail-safe parse of `FREENET_ROUTING_LEGACY_LABELS`: only an explicit
/// affirmative value selects the legacy rules.
fn parse_legacy_labels(value: Option<String>) -> LabelMode {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => LabelMode::Legacy,
        _ => LabelMode::Current,
    }
}

#[cfg(test)]
thread_local! {
    static TEST_LABEL_MODE: std::cell::Cell<Option<LabelMode>> = const { std::cell::Cell::new(None) };
}

/// Force a [`LabelMode`] on this thread until the guard drops. Test-only.
#[cfg(test)]
pub(crate) fn force_label_mode(mode: LabelMode) -> LabelModeGuard {
    let previous = TEST_LABEL_MODE.with(|cell| cell.replace(Some(mode)));
    LabelModeGuard { previous }
}

#[cfg(test)]
pub(crate) struct LabelModeGuard {
    previous: Option<LabelMode>,
}

#[cfg(test)]
impl Drop for LabelModeGuard {
    fn drop(&mut self) {
        TEST_LABEL_MODE.with(|cell| cell.set(self.previous));
    }
}

/// Where labels go. Implemented by [`crate::ring::Ring`] (router only); unit
/// tests substitute a recording sink. `cause` is what the recorder was told
/// about the attempt; it is counted, not trained on.
pub(crate) trait RouteFailureSink: Send + Sync {
    /// `origin` tags the event `Originator` or `Relay` in the routing
    /// dataset (#5648).
    fn record_route_failure(&self, event: RouteEvent, cause: AttemptFailure, origin: AttemptOrigin);
    /// A non-failure event under [`LabelMode::Legacy`] (a relay's downstream
    /// `NotFound` labelled `SuccessUntimed`). Router only; always a relay
    /// observation.
    fn record_legacy_route_event(&self, event: RouteEvent);
    /// `count` ambiguous `NotFound`s were dropped untrained when an operation
    /// ended without proof the contract exists. Counted, never trained on.
    fn record_untrained_not_founds(&self, count: u64);
}

impl AttemptOrigin {
    /// The routing-dataset tag for events this origin records.
    pub(crate) fn route_source(self) -> crate::router::dataset::RouteSource {
        match self {
            AttemptOrigin::Originator => crate::router::dataset::RouteSource::Originator,
            AttemptOrigin::Relay => crate::router::dataset::RouteSource::Relay,
        }
    }
}

impl RouteFailureSink for crate::ring::Ring {
    fn record_route_failure(
        &self,
        event: RouteEvent,
        cause: AttemptFailure,
        origin: AttemptOrigin,
    ) {
        crate::ring::Ring::record_route_failure(self, event, cause, origin.route_source());
    }

    fn record_legacy_route_event(&self, event: RouteEvent) {
        crate::ring::Ring::record_route_event_router_only(
            self,
            event,
            crate::router::dataset::RouteSource::Relay,
        );
    }

    fn record_untrained_not_founds(&self, count: u64) {
        crate::ring::Ring::record_untrained_not_founds(self, count);
    }
}

/// The reply-side condition for existence proof for this operation's routing
/// labels; callers also require that this node stored the reply's state (see
/// the module docs).
pub(crate) fn is_existence_proof(
    requested: &ContractInstanceId,
    key: &freenet_stdlib::prelude::ContractKey,
    envelope_instance_id: Option<&ContractInstanceId>,
) -> bool {
    key.id() == requested && envelope_instance_id.is_none_or(|id| id == requested)
}

/// An originator's route outcome for telemetry, `peer_health` and the
/// topology manager, reported exactly as before #5657: the same event (against
/// the driver's `current_target`), at the same sites, under the same
/// conditions. `NetEventLog::route_event` is emitted in both label modes, so
/// the collector's `route_success` / `route_failure` series do not move with the
/// labelling. Under [`LabelMode::Legacy`] the router gets this event too
/// (`routing_finished`); under [`LabelMode::Current`] the caller feeds the
/// router separately, against the recorded hop.
pub(crate) async fn report_originator_route_outcome(
    op_manager: &crate::node::OpManager,
    tx: &Transaction,
    event: RouteEvent,
    mode: LabelMode,
) {
    if let Some(log_event) = crate::tracing::NetEventLog::route_event(tx, &op_manager.ring, &event)
    {
        op_manager
            .ring
            .register_events(either::Either::Left(log_event))
            .await;
    }
    match mode {
        LabelMode::Legacy => op_manager.ring.routing_finished(event),
        LabelMode::Current => op_manager.ring.report_route_outcome_to_health(&event),
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
    instance_id: ContractInstanceId,
    op_type: OpType,
    origin: AttemptOrigin,
    policy: AmbiguousNotFoundPolicy,
    mode: LabelMode,
    pending_not_found: Vec<PeerKeyLocation>,
    /// Peers already labelled a failure in this operation. A peer is labelled
    /// at most once per operation.
    failed: HashSet<PeerKeyLocation>,
    contract_known_to_exist: bool,
}

impl RouteAttemptRecorder {
    pub(crate) fn new(
        sink: Arc<dyn RouteFailureSink>,
        instance_id: ContractInstanceId,
        op_type: OpType,
        origin: AttemptOrigin,
    ) -> Self {
        Self::with_rules(
            Some(sink),
            instance_id,
            op_type,
            origin,
            ambiguous_not_found_policy(),
            label_mode(),
        )
    }

    /// A recorder that records nothing. Used by drivers that deliberately do
    /// not feed the router (sub-operation GETs).
    pub(crate) fn disabled(instance_id: ContractInstanceId, op_type: OpType) -> Self {
        Self::with_rules(
            None,
            instance_id,
            op_type,
            AttemptOrigin::Originator,
            ambiguous_not_found_policy(),
            label_mode(),
        )
    }

    pub(crate) fn with_rules(
        sink: Option<Arc<dyn RouteFailureSink>>,
        instance_id: ContractInstanceId,
        op_type: OpType,
        origin: AttemptOrigin,
        policy: AmbiguousNotFoundPolicy,
        mode: LabelMode,
    ) -> Self {
        Self {
            sink,
            instance_id,
            op_type,
            origin,
            policy,
            mode,
            pending_not_found: Vec::new(),
            failed: HashSet::new(),
            contract_known_to_exist: false,
        }
    }

    /// The labelling rules this recorder applies. Driver code that owns a
    /// label outside the recorder (a terminal success, a failed stream) must
    /// follow the same mode.
    pub(crate) fn mode(&self) -> LabelMode {
        self.mode
    }

    /// Record a non-success outcome of one attempt.
    ///
    /// `peer` is the peer the request was ACTUALLY sent to. `None` means no
    /// remote peer can be blamed and nothing is recorded — never substitute a
    /// guessed target here.
    ///
    /// `attributable` is false when the outcome is known not to be the peer's
    /// doing under the current rules: a local callback drop, a disconnect of
    /// some other peer, a renewal's clamped-budget timeout, a renewal
    /// `NotFound`. Such outcomes are never labelled under
    /// [`LabelMode::Current`]; under [`LabelMode::Legacy`] a relay labels them
    /// as it did before #5657.
    pub(crate) fn record_attempt(
        &mut self,
        peer: Option<&PeerKeyLocation>,
        outcome: AttemptFailure,
        attributable: bool,
    ) {
        let Some(peer) = peer else {
            return;
        };
        if self.mode == LabelMode::Legacy {
            self.record_legacy(peer, outcome);
            return;
        }
        if !attributable {
            return;
        }
        match outcome {
            AttemptFailure::Timeout | AttemptFailure::SendFailure => {
                self.emit_failure(peer.clone(), outcome);
            }
            AttemptFailure::NotFound if self.contract_known_to_exist => {
                self.emit_failure(peer.clone(), outcome);
            }
            AttemptFailure::NotFound => {
                if !self.pending_not_found.contains(peer) {
                    self.pending_not_found.push(peer.clone());
                }
            }
        }
    }

    /// Pre-#5657 labelling: originators label no attempt; a relay labels a
    /// downstream `NotFound` `SuccessUntimed` and any transport failure
    /// `Failure`, immediately.
    fn record_legacy(&mut self, peer: &PeerKeyLocation, outcome: AttemptFailure) {
        if self.origin != AttemptOrigin::Relay {
            return;
        }
        match outcome {
            // Every failure, like `record_relay_route_event` did before #5657:
            // no per-peer deduplication in legacy mode.
            AttemptFailure::Timeout | AttemptFailure::SendFailure => {
                self.send_failure(peer.clone(), outcome);
            }
            AttemptFailure::NotFound => {
                let Some(sink) = &self.sink else {
                    return;
                };
                crate::operations::count_relay_route_event(self.op_type);
                sink.record_legacy_route_event(RouteEvent {
                    peer: peer.clone(),
                    contract_location: Location::from(&self.instance_id),
                    outcome: RouteOutcome::SuccessUntimed,
                    op_type: Some(self.op_type),
                });
            }
        }
    }

    /// Evidence from THIS operation that the contract exists: state from a
    /// REMOTE peer this operation actually contacted that this node then
    /// stored (see the module docs). A local copy or a local completion is
    /// not proof (the copy may be stale or held by this node alone). Every
    /// pending `NotFound` in this operation
    /// is then a genuine routing failure; later ones are labelled immediately.
    /// Idempotent. Has no effect under [`LabelMode::Legacy`], which never
    /// holds pending `NotFound`s and never reads the flag.
    pub(crate) fn contract_exists(&mut self) {
        self.contract_known_to_exist = true;
        for peer in std::mem::take(&mut self.pending_not_found) {
            self.emit_failure(peer, AttemptFailure::NotFound);
        }
    }

    /// A Failure label under the current rules: at most once per peer per
    /// operation.
    fn emit_failure(&mut self, peer: PeerKeyLocation, cause: AttemptFailure) {
        if self.sink.is_none() || !self.failed.insert(peer.clone()) {
            return;
        }
        self.send_failure(peer, cause);
    }

    /// Hand one Failure label to the sink, with no deduplication.
    fn send_failure(&self, peer: PeerKeyLocation, cause: AttemptFailure) {
        let Some(sink) = &self.sink else {
            return;
        };
        if self.origin == AttemptOrigin::Relay {
            crate::operations::count_relay_route_event(self.op_type);
        }
        sink.record_route_failure(
            RouteEvent {
                peer,
                contract_location: Location::from(&self.instance_id),
                outcome: RouteOutcome::Failure,
                op_type: Some(self.op_type),
            },
            cause,
            self.origin,
        );
    }
}

impl Drop for RouteAttemptRecorder {
    fn drop(&mut self) {
        let pending = std::mem::take(&mut self.pending_not_found);
        match self.policy {
            AmbiguousNotFoundPolicy::Naive => {
                for peer in pending {
                    self.emit_failure(peer, AttemptFailure::NotFound);
                }
            }
            AmbiguousNotFoundPolicy::Untrained => {
                if let (Some(sink), false) = (&self.sink, pending.is_empty()) {
                    sink.record_untrained_not_founds(pending.len() as u64);
                }
            }
        }
    }
}

/// Per-attempt handoff between an originator's retry loop and its own
/// originator-loopback relay: the peer the attempt was actually forwarded to
/// (see the module docs for why the client driver cannot know it), and the
/// peers the relay should not pick as that attempt's first hop (#5660).
///
/// The retry loop [`register`](Self::register)s a slot BEFORE sending (the
/// loopback relay may run before `send_and_await` returns), the loopback relay
/// reads the slot's [`first_hop_exclusions`](Self::first_hop_exclusions) and
/// fills it via [`record_hop`](Self::record_hop), and the returned
/// [`AttemptHopGuard`] removes the slot on every exit, including cancellation.
/// `record_hop` never inserts, so a late relay cannot leak an entry: the
/// registry holds at most one entry per in-flight attempt.
#[derive(Default)]
pub(crate) struct AttemptHopRegistry {
    slots: DashMap<Transaction, AttemptSlot>,
}

#[derive(Default)]
struct AttemptSlot {
    /// The peer the loopback relay forwarded the attempt to, and when its
    /// local dispatch returned (`None` until it has).
    hop: Option<(PeerKeyLocation, Option<tokio::time::Instant>)>,
    /// Peers the loopback relay must not pick as the attempt's first hop.
    /// Local to this node: never put in the forwarded request's visited bloom.
    first_hop_exclusions: Vec<std::net::SocketAddr>,
    /// The peer the retry loop chose as the attempt's first hop, which the
    /// loopback relay uses over its own ranking while that peer is still one
    /// of its routing candidates.
    first_hop_pin: Option<std::net::SocketAddr>,
}

impl AttemptHopRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn register(self: &Arc<Self>, tx: Transaction) -> AttemptHopGuard {
        self.register_excluding(tx, Vec::new(), None)
    }

    /// [`register`](Self::register), asking the loopback relay not to pick any
    /// of `first_hop_exclusions` as this attempt's first hop, and to use
    /// `first_hop_pin` instead while that peer is one of its candidates.
    pub(crate) fn register_excluding(
        self: &Arc<Self>,
        tx: Transaction,
        first_hop_exclusions: Vec<std::net::SocketAddr>,
        first_hop_pin: Option<std::net::SocketAddr>,
    ) -> AttemptHopGuard {
        self.slots.insert(
            tx,
            AttemptSlot {
                hop: None,
                first_hop_exclusions,
                first_hop_pin,
            },
        );
        AttemptHopGuard {
            registry: self.clone(),
            tx,
        }
    }

    /// The peers the originator's retry loop asked its loopback relay not to
    /// pick as `tx`'s first hop. Empty when no attempt is registered for `tx`
    /// (a relay hop for a remote upstream, or an attempt already resolved).
    pub(crate) fn first_hop_exclusions(&self, tx: &Transaction) -> Vec<std::net::SocketAddr> {
        self.slots
            .get(tx)
            .map(|slot| slot.first_hop_exclusions.clone())
            .unwrap_or_default()
    }

    /// The peer the originator's retry loop chose as `tx`'s first hop, if
    /// any. `None` when no attempt is registered for `tx`.
    pub(crate) fn first_hop_pin(&self, tx: &Transaction) -> Option<std::net::SocketAddr> {
        self.slots.get(tx).and_then(|slot| slot.first_hop_pin)
    }

    /// Called by the originator-loopback relay immediately before it dispatches
    /// the request to `peer`. A no-op when no attempt is registered for `tx`
    /// (a relay hop for a remote upstream, or an attempt already resolved).
    /// Its local dispatch has not returned yet, so the dispatch time stays
    /// unset until [`touch_hop`](Self::touch_hop).
    pub(crate) fn record_hop(&self, tx: &Transaction, peer: &PeerKeyLocation) {
        if let Some(mut slot) = self.slots.get_mut(tx) {
            slot.hop = Some((peer.clone(), None));
        }
    }

    /// Stamp the recorded hop's dispatch time as now, keeping its peer. The
    /// loopback relay calls it once its local dispatch (the hand-off to this
    /// node's event loop) has returned: the hop's share of the attempt is
    /// counted from then, and an attempt whose local dispatch never returned
    /// blames nobody for its timeout (#5657). A no-op when no hop is
    /// recorded.
    pub(crate) fn touch_hop(&self, tx: &Transaction) {
        if let Some((_, recorded_at)) = self
            .slots
            .get_mut(tx)
            .as_deref_mut()
            .and_then(|slot| slot.hop.as_mut())
        {
            *recorded_at = Some(tokio::time::Instant::now());
        }
    }

    /// Undo [`record_hop`](Self::record_hop) when the dispatch failed locally,
    /// so the attempt is not blamed on a peer that never saw the request.
    pub(crate) fn clear_hop(&self, tx: &Transaction) {
        if let Some(mut slot) = self.slots.get_mut(tx) {
            slot.hop = None;
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
    #[cfg(test)]
    pub(crate) fn hop(&self) -> Option<PeerKeyLocation> {
        self.hop_record().map(|(hop, _)| hop)
    }

    /// The peer the attempt was forwarded to, if the loopback relay recorded
    /// one, and when its local dispatch returned (`None` while it has not).
    pub(crate) fn hop_record(&self) -> Option<(PeerKeyLocation, Option<tokio::time::Instant>)> {
        self.registry
            .slots
            .get(&self.tx)
            .and_then(|slot| slot.hop.clone())
    }
}

impl Drop for AttemptHopGuard {
    fn drop(&mut self) {
        self.registry.slots.remove(&self.tx);
    }
}

/// The smallest share of an attempt's elapsed time the recorded hop must have
/// had for the attempt's timeout to be blamed on it (#5657). The loopback relay
/// stamps the hop once its local dispatch returns, which on an overloaded originator
/// can be late in the attempt: a hop forwarded to at 59 s of a 60 s budget did not
/// stall the attempt, the originator did. At one half, a hop is blamed when it
/// had at least as long as the originator took to reach it.
pub(crate) const MIN_HOP_SHARE_FOR_TIMEOUT_BLAME: f64 = 0.5;

/// Whether the hop of an attempt that started at `attempt_started` and timed
/// out at `timed_out_at` had enough of it to be blamed for the timeout. The
/// share runs from `dispatched_at`, when the loopback relay's local dispatch
/// (the hand-off to this node's event loop) returned; `None` means it never
/// returned, and nobody is blamed. See [`MIN_HOP_SHARE_FOR_TIMEOUT_BLAME`].
pub(crate) fn hop_had_budget_share(
    attempt_started: tokio::time::Instant,
    dispatched_at: Option<tokio::time::Instant>,
    timed_out_at: tokio::time::Instant,
) -> bool {
    let Some(dispatched_at) = dispatched_at else {
        return false;
    };
    let attempt = timed_out_at.saturating_duration_since(attempt_started);
    let hop = timed_out_at.saturating_duration_since(dispatched_at);
    hop.as_secs_f64() >= attempt.as_secs_f64() * MIN_HOP_SHARE_FOR_TIMEOUT_BLAME
}

/// Shared harness for the per-op driver tests: a real `OpManager` whose event
/// loop is replaced by a script, standing in for the originator-loopback relay
/// and the network behind it.
#[cfg(test)]
pub(crate) mod driver_test_support {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use freenet_stdlib::prelude::WrappedState;

    use crate::contract::{ContractHandlerEvent, StoreResponse};
    use crate::message::{MessageStats, NetMessage};
    use crate::node::{OpExecutionPayload, OpManager, WaiterReply};
    use crate::ring::{Location, PeerKeyLocation};

    /// `(peer address, outcome is a Failure)` for every `EventKind::Route`
    /// telemetry event a test node emitted, in order.
    type RouteLogEntries = Arc<parking_lot::Mutex<Vec<(Option<SocketAddr>, bool)>>>;

    fn route_logs() -> &'static dashmap::DashMap<String, RouteLogEntries> {
        static LOGS: std::sync::OnceLock<dashmap::DashMap<String, RouteLogEntries>> =
            std::sync::OnceLock::new();
        LOGS.get_or_init(dashmap::DashMap::new)
    }

    /// Captures the `NetEventLog::route_event`s a test node registers, which
    /// is what the telemetry collector turns into `route_success` /
    /// `route_failure`.
    #[derive(Clone, Default)]
    struct RouteLog(RouteLogEntries);

    impl crate::tracing::NetEventRegister for RouteLog {
        fn register_events<'a>(
            &'a self,
            events: either::Either<
                crate::tracing::NetEventLog<'a>,
                Vec<crate::tracing::NetEventLog<'a>>,
            >,
        ) -> futures::future::BoxFuture<'a, ()> {
            let logs = match events {
                either::Either::Left(log) => vec![log],
                either::Either::Right(logs) => logs,
            };
            for log in logs {
                if let crate::tracing::EventKind::Route(event) = &log.kind {
                    self.0.lock().push((
                        event.peer.socket_addr(),
                        matches!(event.outcome, crate::router::RouteOutcome::Failure),
                    ));
                }
            }
            Box::pin(async {})
        }

        fn notify_of_time_out(
            &mut self,
            _tx: crate::message::Transaction,
            _op_type: &str,
            _target_peer: Option<String>,
        ) -> futures::future::BoxFuture<'_, ()> {
            Box::pin(async {})
        }

        fn trait_clone(&self) -> Box<dyn crate::tracing::NetEventRegister> {
            Box::new(self.clone())
        }
    }

    /// The route telemetry events of the test node built with `id`.
    pub(crate) fn route_log(id: &str) -> Vec<(Option<SocketAddr>, bool)> {
        route_logs()
            .get(id)
            .map(|log| log.lock().clone())
            .unwrap_or_default()
    }

    fn store_rejections() -> &'static dashmap::DashSet<String> {
        static REJECTIONS: std::sync::OnceLock<dashmap::DashSet<String>> =
            std::sync::OnceLock::new();
        REJECTIONS.get_or_init(dashmap::DashSet::new)
    }

    /// Make the stub contract handler of the test node built with `id` refuse
    /// every store, answering as an executor that refuses the state.
    pub(crate) fn reject_stores(id: &str) {
        store_rejections().insert(id.to_string());
    }

    fn rejects_stores(id: &str) -> bool {
        store_rejections().contains(id)
    }

    /// The pre-#5657 non-router inputs for `peer`: its `peer_health`
    /// `(successes, failures)` and the topology manager's outbound-request
    /// count.
    pub(crate) fn health_inputs(
        op_manager: &OpManager,
        peer: &PeerKeyLocation,
    ) -> ((u64, u64), usize) {
        let counts = peer
            .socket_addr()
            .and_then(|addr| {
                op_manager
                    .ring
                    .connection_manager
                    .peer_health
                    .lock()
                    .counts(&addr)
            })
            .unwrap_or((0, 0));
        let outbound = op_manager
            .ring
            .connection_manager
            .outbound_request_count_for_test(peer);
        (counts, outbound)
    }

    /// What the scripted event loop does with one outbound attempt.
    pub(crate) struct Step {
        /// The peer the (simulated) loopback relay forwards the attempt to.
        /// `None` = no remote peer was attempted.
        pub hop: Option<PeerKeyLocation>,
        /// What the waiter receives.
        pub answer: Answer,
    }

    /// How the scripted event loop answers one attempt's waiter.
    #[allow(clippy::large_enum_variant)] // test harness; one value per attempt
    pub(crate) enum Answer {
        /// Deliver this terminal reply.
        Reply(NetMessage),
        /// Wake the waiter with `PeerDisconnected` for the hop (the connection
        /// was pruned mid-flight, #4313).
        PeerDisconnected,
        /// Wake the waiter with `PeerDisconnected` for some OTHER peer.
        PeerDisconnectedFor(SocketAddr),
        /// Never answer, so the attempt times out.
        Never,
        /// Drop the waiter without an answer: the driver sees a local
        /// `NotificationError`, which is not the peer's doing.
        DropWaiter,
        /// Never answer, and record the hop only after the delay: the loopback
        /// relay forwarded late in the attempt. Use with `Step::hop: None`.
        NeverWithHopAfter(std::time::Duration, PeerKeyLocation),
        /// Never answer, and record the hop without its local dispatch ever
        /// returning: the dispatch is still blocked on this node at the
        /// deadline. Use with `Step::hop: None`.
        NeverDispatched(PeerKeyLocation),
    }

    /// What the stub contract handler answers to a `GetQuery`: `None` = the
    /// contract is not stored locally.
    pub(crate) type LocalStore =
        Arc<parking_lot::Mutex<Option<(freenet_stdlib::prelude::ContractKey, WrappedState)>>>;

    /// Build a Local-mode `OpManager` with a known own address and `peers`
    /// ring connections, returning the op-execution receiver the drivers send
    /// attempts to. `guards` must be kept alive for the test's duration.
    pub(crate) async fn op_manager_with_peers(
        id: &str,
        peers: usize,
    ) -> (
        Arc<OpManager>,
        tokio::sync::mpsc::Receiver<OpExecutionPayload>,
        Vec<PeerKeyLocation>,
        Box<dyn std::any::Any>,
    ) {
        let (op_manager, rx, peers, guards, _store) =
            op_manager_with_peers_and_store(id, peers).await;
        (op_manager, rx, peers, guards)
    }

    /// [`op_manager_with_peers`] plus a stub contract handler: a `GetQuery` is
    /// answered from the returned [`LocalStore`], every other event is dropped
    /// unanswered (the caller sees a handler error).
    pub(crate) async fn op_manager_with_peers_and_store(
        id: &str,
        peers: usize,
    ) -> (
        Arc<OpManager>,
        tokio::sync::mpsc::Receiver<OpExecutionPayload>,
        Vec<PeerKeyLocation>,
        Box<dyn std::any::Any>,
        LocalStore,
    ) {
        op_manager_with_peers_and_store_on(id, peers, None).await
    }

    /// [`op_manager_with_peers_and_store`], with the event-loop channels
    /// sized `channel_capacity` (`None`: the default). A small capacity lets
    /// a test hold a dispatch up.
    // The stub contract handler answers two events and leaves every other
    // variant unanswered on purpose, so its wildcard arm is the point.
    #[allow(clippy::wildcard_enum_match_arm)]
    pub(crate) async fn op_manager_with_peers_and_store_on(
        id: &str,
        peers: usize,
        channel_capacity: Option<usize>,
    ) -> (
        Arc<OpManager>,
        tokio::sync::mpsc::Receiver<OpExecutionPayload>,
        Vec<PeerKeyLocation>,
        Box<dyn std::any::Any>,
        LocalStore,
    ) {
        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let (notification_rx, notification_tx) = match channel_capacity {
            Some(capacity) => crate::node::event_loop_notification_channel_with_capacity(capacity),
            None => crate::node::event_loop_notification_channel(),
        };
        let crate::node::EventLoopNotificationsReceiver {
            notifications_receiver,
            op_execution_receiver,
        } = notification_rx;
        let (ops_ch_channel, mut ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let store: LocalStore = Arc::new(parking_lot::Mutex::new(None));
        let handler_store = store.clone();
        let node_id = id.to_string();
        tokio::spawn(async move {
            while let Ok((id, event, _priority)) = ch_channel.recv_from_sender().await {
                match event {
                    ContractHandlerEvent::GetQuery { instance_id, .. } => {
                        let stored = handler_store
                            .lock()
                            .clone()
                            .filter(|(key, _)| *key.id() == instance_id);
                        let response = ContractHandlerEvent::GetResponse {
                            key: stored.as_ref().map(|(key, _)| *key),
                            response: Ok(StoreResponse {
                                state: stored.map(|(_, state)| state),
                                contract: None,
                            }),
                        };
                        let _answered = ch_channel.send_to_sender(id, response).await;
                    }
                    // A store carrying contract code is accepted, unless the
                    // test made this node refuse stores: then it is answered
                    // the way an executor refusing the state answers.
                    ContractHandlerEvent::PutQuery {
                        key,
                        state,
                        contract: Some(_),
                        ..
                    } => {
                        let refused = rejects_stores(&node_id);
                        let new_value = if refused {
                            Err(crate::contract::ExecutorError::other(anyhow::anyhow!(
                                "store refused by the test"
                            )))
                        } else {
                            *handler_store.lock() = Some((key, state.clone()));
                            Ok(state)
                        };
                        let response = ContractHandlerEvent::PutResponse {
                            new_value,
                            state_changed: !refused,
                        };
                        let _answered = ch_channel.send_to_sender(id, response).await;
                    }
                    // Every other event is deliberately left unanswered,
                    // whatever its variant.
                    _ => ch_channel.drop_waiting_response(id),
                }
            }
        });
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();
        let route_log = RouteLog::default();
        route_logs().insert(id.to_string(), route_log.0.clone());
        let op_manager = Arc::new(
            OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![Box::new(route_log)]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);
        op_manager
            .ring
            .connection_manager
            .set_own_addr_local_for_test("127.0.0.1:12000".parse().unwrap());

        let mut added = Vec::new();
        for i in 0..peers {
            let kp = crate::transport::TransportKeypair::new();
            let addr: SocketAddr = format!("127.0.0.1:{}", 30000 + i).parse().unwrap();
            assert!(op_manager.ring.connection_manager.add_connection(
                Location::new((0.05 + 0.13 * i as f64) % 1.0),
                addr,
                kp.public().clone(),
                false,
            ));
            added.push(PeerKeyLocation::new(kp.public().clone(), addr));
        }
        let guards: Box<dyn std::any::Any> = Box::new((
            notifications_receiver,
            wait_for_event,
            result_router_rx,
            task_monitor,
        ));
        (op_manager, op_execution_receiver, added, guards, store)
    }

    /// Serve outbound attempts of type `op` from `rx` with
    /// `script(attempt_index, outbound, target_addr)`. Records the step's hop in
    /// the attempt-hop registry exactly as the originator-loopback relay does,
    /// then answers as scripted. Payloads of any other transaction type (the
    /// ring's own background CONNECT traffic) are held open and not counted.
    /// Returns the number of attempts served so far.
    pub(crate) fn serve_attempts<F>(
        op_manager: Arc<OpManager>,
        mut rx: tokio::sync::mpsc::Receiver<OpExecutionPayload>,
        op: crate::message::TransactionType,
        mut script: F,
    ) -> Arc<AtomicUsize>
    where
        F: FnMut(usize, &NetMessage, Option<SocketAddr>) -> Step + Send + 'static,
    {
        let served = Arc::new(AtomicUsize::new(0));
        let counter = served.clone();
        tokio::spawn(async move {
            let mut held_open = Vec::new();
            while let Some((reply_tx, outbound, target)) = rx.recv().await {
                if outbound.id().transaction_type() != op {
                    held_open.push(reply_tx);
                    continue;
                }
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let step = script(index, &outbound, target);
                if let Some(hop) = &step.hop {
                    op_manager
                        .attempt_hop_registry()
                        .record_hop(outbound.id(), hop);
                    // The loopback relay's local dispatch returned at once.
                    op_manager.attempt_hop_registry().touch_hop(outbound.id());
                }
                match step.answer {
                    Answer::Reply(reply) => {
                        reply_tx
                            .try_send(WaiterReply::Reply(reply))
                            .expect("the attempt's waiter accepts its reply");
                    }
                    Answer::PeerDisconnected => {
                        let peer = step
                            .hop
                            .as_ref()
                            .and_then(|h| h.socket_addr())
                            .unwrap_or_else(|| "127.0.0.1:1".parse().unwrap());
                        reply_tx
                            .try_send(WaiterReply::PeerDisconnected { peer })
                            .expect("the attempt's waiter accepts the disconnect");
                    }
                    Answer::PeerDisconnectedFor(peer) => {
                        reply_tx
                            .try_send(WaiterReply::PeerDisconnected { peer })
                            .expect("the attempt's waiter accepts the disconnect");
                    }
                    Answer::Never => held_open.push(reply_tx),
                    Answer::DropWaiter => drop(reply_tx),
                    Answer::NeverWithHopAfter(delay, hop) => {
                        let registry = op_manager.attempt_hop_registry().clone();
                        let tx = *outbound.id();
                        tokio::spawn(async move {
                            tokio::time::sleep(delay).await;
                            registry.record_hop(&tx, &hop);
                            registry.touch_hop(&tx);
                        });
                        held_open.push(reply_tx);
                    }
                    Answer::NeverDispatched(hop) => {
                        op_manager
                            .attempt_hop_registry()
                            .record_hop(outbound.id(), &hop);
                        held_open.push(reply_tx);
                    }
                }
            }
        });
        served
    }

    /// The body of the PRODUCTION function whose signature starts with
    /// `signature`, brace-matched. Panics when the signature is missing, is
    /// found only inside a `#[cfg(test)] mod`, or matches more than once in
    /// production code, so a moved or renamed function fails the pin loudly
    /// instead of widening its region.
    pub(crate) fn production_fn_body<'a>(source: &'a str, signature: &str) -> &'a str {
        let production_end = source.find("#[cfg(test)]\nmod ").unwrap_or(source.len());
        let production = &source[..production_end];
        let matches = production.matches(signature).count();
        assert_eq!(
            matches, 1,
            "`{signature}` must appear exactly once in production code (found {matches})"
        );
        let start = production.find(signature).unwrap();
        let open = start
            + source[start..]
                .find('{')
                .unwrap_or_else(|| panic!("`{signature}` has no body"));
        let mut depth = 0usize;
        for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
            match byte {
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &source[open + 1..open + offset];
                    }
                }
                _ => {}
            }
        }
        panic!("unterminated body for `{signature}`");
    }

    /// `(peer address, result)` for every event in the failure estimator's
    /// window; `1.0` = failure.
    pub(crate) fn failure_window(op_manager: &OpManager) -> Vec<(Option<SocketAddr>, f64)> {
        op_manager.ring.router.read().failure_window_for_test()
    }

    /// `(peer address, routing-dataset source)` of every event the node's
    /// router ingested, in order.
    pub(crate) fn recorded_sources(
        op_manager: &OpManager,
    ) -> Vec<(Option<SocketAddr>, crate::router::dataset::RouteSource)> {
        op_manager.ring.router.read().recorded_sources_for_test()
    }

    /// Addresses blamed with a Failure, in order.
    pub(crate) fn failed_addrs(op_manager: &OpManager) -> Vec<SocketAddr> {
        failure_window(op_manager)
            .into_iter()
            .filter(|(_, r)| *r == 1.0)
            .map(|(a, _)| a.expect("test peers have addresses"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::get::GetMsg;
    use crate::transport::TransportKeypair;
    use parking_lot::Mutex;

    /// Records what the recorder asked of the sink.
    #[derive(Default)]
    struct VecSink {
        failures: Mutex<Vec<(RouteEvent, AttemptFailure, AttemptOrigin)>>,
        legacy: Mutex<Vec<RouteEvent>>,
        untrained: Mutex<Vec<u64>>,
    }

    impl RouteFailureSink for VecSink {
        fn record_route_failure(
            &self,
            event: RouteEvent,
            cause: AttemptFailure,
            origin: AttemptOrigin,
        ) {
            self.failures.lock().push((event, cause, origin));
        }
        fn record_legacy_route_event(&self, event: RouteEvent) {
            self.legacy.lock().push(event);
        }
        fn record_untrained_not_founds(&self, count: u64) {
            self.untrained.lock().push(count);
        }
    }

    impl VecSink {
        fn failed_peers(&self) -> Vec<std::net::SocketAddr> {
            self.failures
                .lock()
                .iter()
                .map(|(e, _, _)| {
                    assert!(matches!(e.outcome, RouteOutcome::Failure));
                    e.peer.socket_addr().expect("test peers have addresses")
                })
                .collect()
        }
        fn causes(&self) -> Vec<AttemptFailure> {
            self.failures.lock().iter().map(|(_, c, _)| *c).collect()
        }
        fn legacy_successes(&self) -> Vec<std::net::SocketAddr> {
            self.legacy
                .lock()
                .iter()
                .map(|e| {
                    assert!(matches!(e.outcome, RouteOutcome::SuccessUntimed));
                    e.peer.socket_addr().unwrap()
                })
                .collect()
        }
    }

    fn id() -> ContractInstanceId {
        ContractInstanceId::new([0x42; 32])
    }

    fn peer(port: u16) -> PeerKeyLocation {
        let addr: std::net::SocketAddr = format!("10.0.0.1:{port}").parse().unwrap();
        PeerKeyLocation::new(TransportKeypair::new().public().clone(), addr)
    }

    fn recorder_with(
        sink: &Arc<VecSink>,
        origin: AttemptOrigin,
        policy: AmbiguousNotFoundPolicy,
        mode: LabelMode,
    ) -> RouteAttemptRecorder {
        RouteAttemptRecorder::with_rules(
            Some(sink.clone() as Arc<dyn RouteFailureSink>),
            id(),
            OpType::Get,
            origin,
            policy,
            mode,
        )
    }

    fn recorder(sink: &Arc<VecSink>, policy: AmbiguousNotFoundPolicy) -> RouteAttemptRecorder {
        recorder_with(sink, AttemptOrigin::Originator, policy, LabelMode::Current)
    }

    fn addr(p: &PeerKeyLocation) -> std::net::SocketAddr {
        p.socket_addr().unwrap()
    }

    #[test]
    fn production_policy_is_untrained() {
        assert_eq!(
            ambiguous_not_found_policy(),
            AmbiguousNotFoundPolicy::Untrained
        );
    }

    #[test]
    fn legacy_labels_env_parsing_is_fail_safe() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert_eq!(
                parse_legacy_labels(Some(value.to_string())),
                LabelMode::Legacy,
                "{value:?}"
            );
        }
        for value in ["0", "false", "", "2", "legacy", "enable"] {
            assert_eq!(
                parse_legacy_labels(Some(value.to_string())),
                LabelMode::Current,
                "{value:?}"
            );
        }
        assert_eq!(parse_legacy_labels(None), LabelMode::Current);
    }

    #[test]
    fn label_mode_override_is_scoped_to_its_guard() {
        let outer = label_mode();
        {
            let _legacy = force_label_mode(LabelMode::Legacy);
            assert_eq!(label_mode(), LabelMode::Legacy);
            let sink = Arc::new(VecSink::default());
            let rec = RouteAttemptRecorder::new(
                sink as Arc<dyn RouteFailureSink>,
                id(),
                OpType::Get,
                AttemptOrigin::Relay,
            );
            assert_eq!(rec.mode(), LabelMode::Legacy, "new() follows label_mode()");
        }
        assert_eq!(label_mode(), outer);
    }

    #[test]
    fn timeout_and_send_failure_are_labelled_immediately() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::Untrained);
        rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
        assert_eq!(sink.failed_peers(), vec![addr(&a)]);
        rec.record_attempt(Some(&b), AttemptFailure::SendFailure, true);
        assert_eq!(sink.failed_peers(), vec![addr(&a), addr(&b)]);
        drop(rec);
        assert_eq!(sink.failed_peers().len(), 2, "drop must not re-emit");
        assert_eq!(
            sink.causes(),
            vec![AttemptFailure::Timeout, AttemptFailure::SendFailure]
        );
    }

    /// Every failure carries the recorder's origin to the sink, which is what
    /// tags it `originator` or `relay` in the routing dataset (#5648).
    #[test]
    fn failures_carry_the_recorder_origin() {
        use crate::router::dataset::RouteSource;
        for (origin, source) in [
            (AttemptOrigin::Originator, RouteSource::Originator),
            (AttemptOrigin::Relay, RouteSource::Relay),
        ] {
            assert_eq!(origin.route_source(), source);
            let sink = Arc::new(VecSink::default());
            let mut rec = recorder_with(
                &sink,
                origin,
                AmbiguousNotFoundPolicy::Untrained,
                LabelMode::Current,
            );
            rec.record_attempt(Some(&peer(1)), AttemptFailure::Timeout, true);
            rec.record_attempt(Some(&peer(2)), AttemptFailure::NotFound, true);
            rec.contract_exists();
            drop(rec);
            let origins: Vec<_> = sink.failures.lock().iter().map(|(_, _, o)| *o).collect();
            assert_eq!(origins, vec![origin, origin], "{origin:?}");
        }
    }

    #[test]
    fn unattributable_outcomes_are_never_labelled_under_current_rules() {
        for origin in [AttemptOrigin::Originator, AttemptOrigin::Relay] {
            let sink = Arc::new(VecSink::default());
            let a = peer(1);
            let mut rec = recorder_with(
                &sink,
                origin,
                AmbiguousNotFoundPolicy::Naive,
                LabelMode::Current,
            );
            rec.record_attempt(Some(&a), AttemptFailure::SendFailure, false);
            rec.record_attempt(Some(&a), AttemptFailure::Timeout, false);
            rec.record_attempt(Some(&a), AttemptFailure::NotFound, false);
            rec.contract_exists();
            drop(rec);
            assert!(sink.failed_peers().is_empty(), "{origin:?}");
            assert!(sink.legacy_successes().is_empty(), "{origin:?}");
        }
    }

    #[test]
    fn not_found_then_success_labels_each_not_found_peer_exactly_once() {
        for policy in [
            AmbiguousNotFoundPolicy::Untrained,
            AmbiguousNotFoundPolicy::Naive,
        ] {
            let sink = Arc::new(VecSink::default());
            let (a, b, c) = (peer(1), peer(2), peer(3));
            let mut rec = recorder(&sink, policy);
            rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
            rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
            assert!(sink.failed_peers().is_empty(), "{policy:?}: not yet proven");
            rec.contract_exists();
            assert_eq!(sink.failed_peers(), vec![addr(&a), addr(&b)], "{policy:?}");
            rec.record_attempt(Some(&c), AttemptFailure::NotFound, true);
            rec.contract_exists();
            drop(rec);
            assert_eq!(
                sink.failed_peers(),
                vec![addr(&a), addr(&b), addr(&c)],
                "{policy:?}"
            );
            assert!(sink.causes().iter().all(|c| *c == AttemptFailure::NotFound));
        }
    }

    #[test]
    fn untrained_policy_drops_ambiguous_not_found() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::Untrained);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
        rec.record_attempt(Some(&b), AttemptFailure::Timeout, true);
        rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
        drop(rec);
        assert_eq!(
            sink.failed_peers(),
            vec![addr(&b)],
            "only the timeout trains"
        );
        assert_eq!(
            *sink.untrained.lock(),
            vec![2],
            "the dropped NotFounds are counted, once, at settle"
        );
    }

    #[test]
    fn naive_policy_trains_ambiguous_not_found_at_settle() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::Naive);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
        rec.record_attempt(Some(&b), AttemptFailure::Timeout, true);
        assert_eq!(sink.failed_peers(), vec![addr(&b)]);
        drop(rec);
        assert_eq!(sink.failed_peers(), vec![addr(&b), addr(&a)]);
    }

    /// A peer that fails several times in one operation is labelled once.
    #[test]
    fn a_peer_is_labelled_at_most_once_per_operation() {
        for policy in [
            AmbiguousNotFoundPolicy::Untrained,
            AmbiguousNotFoundPolicy::Naive,
        ] {
            let sink = Arc::new(VecSink::default());
            let (a, b) = (peer(1), peer(2));
            let mut rec = recorder(&sink, policy);
            rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
            rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
            rec.record_attempt(Some(&a), AttemptFailure::SendFailure, true);
            rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
            rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
            rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
            rec.contract_exists();
            rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
            rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
            drop(rec);
            assert_eq!(
                sink.failed_peers(),
                vec![addr(&a), addr(&b)],
                "{policy:?}: one failure per peer per operation"
            );
        }
        let sink = Arc::new(VecSink::default());
        let a = peer(1);
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::Naive);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
        rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
        drop(rec);
        assert_eq!(sink.failed_peers(), vec![addr(&a)]);
    }

    /// `FREENET_ROUTING_LEGACY_LABELS`: an originator labels no attempt at
    /// all, and a relay labels exactly as before #5657 — a downstream
    /// NotFound as SuccessUntimed (renewal or not, proof or not) and every
    /// transport failure as Failure (attributable or not).
    #[test]
    fn legacy_mode_restores_pre_5657_labels() {
        let sink = Arc::new(VecSink::default());
        let (a, b) = (peer(1), peer(2));
        let mut rec = recorder_with(
            &sink,
            AttemptOrigin::Originator,
            AmbiguousNotFoundPolicy::Naive,
            LabelMode::Legacy,
        );
        rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
        rec.record_attempt(Some(&a), AttemptFailure::SendFailure, true);
        rec.record_attempt(Some(&b), AttemptFailure::NotFound, true);
        rec.contract_exists();
        drop(rec);
        assert!(
            sink.failed_peers().is_empty(),
            "legacy originators label nothing"
        );
        assert!(sink.legacy_successes().is_empty());

        let sink = Arc::new(VecSink::default());
        let (c, d, e) = (peer(3), peer(4), peer(5));
        let mut rec = recorder_with(
            &sink,
            AttemptOrigin::Relay,
            AmbiguousNotFoundPolicy::Untrained,
            LabelMode::Legacy,
        );
        rec.record_attempt(Some(&a), AttemptFailure::NotFound, true);
        rec.record_attempt(Some(&b), AttemptFailure::NotFound, false);
        rec.record_attempt(Some(&c), AttemptFailure::SendFailure, false);
        rec.record_attempt(Some(&d), AttemptFailure::Timeout, true);
        rec.record_attempt(Some(&d), AttemptFailure::Timeout, true);
        rec.contract_exists();
        rec.record_attempt(Some(&e), AttemptFailure::NotFound, true);
        drop(rec);
        assert_eq!(
            sink.legacy_successes(),
            vec![addr(&a), addr(&b), addr(&e)],
            "every relay NotFound is a legacy SuccessUntimed"
        );
        assert_eq!(
            sink.failed_peers(),
            vec![addr(&c), addr(&d), addr(&d)],
            "legacy relays record every failure, like main: no deduplication"
        );
        assert!(
            sink.untrained.lock().is_empty(),
            "legacy mode holds no pending NotFound"
        );
    }

    #[test]
    fn unattributable_attempt_and_zero_attempts_record_nothing() {
        let sink = Arc::new(VecSink::default());
        {
            let _untouched = recorder(&sink, AmbiguousNotFoundPolicy::Naive);
        }
        let mut rec = recorder(&sink, AmbiguousNotFoundPolicy::Naive);
        rec.record_attempt(None, AttemptFailure::NotFound, true);
        rec.record_attempt(None, AttemptFailure::Timeout, true);
        rec.record_attempt(None, AttemptFailure::SendFailure, true);
        rec.contract_exists();
        drop(rec);
        assert!(sink.failed_peers().is_empty());
    }

    #[test]
    fn events_carry_contract_location_and_op_type() {
        let sink = Arc::new(VecSink::default());
        let a = peer(1);
        let mut rec = RouteAttemptRecorder::with_rules(
            Some(sink.clone() as Arc<dyn RouteFailureSink>),
            id(),
            OpType::Subscribe,
            AttemptOrigin::Originator,
            AmbiguousNotFoundPolicy::Untrained,
            LabelMode::Current,
        );
        rec.record_attempt(Some(&a), AttemptFailure::Timeout, true);
        let events = sink.failures.lock();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0.contract_location, Location::from(&id()));
        assert_eq!(events[0].0.op_type, Some(OpType::Subscribe));
    }

    #[test]
    fn hop_registry_reports_recorded_hop_and_cleans_up() {
        let registry = Arc::new(AttemptHopRegistry::new());
        let tx = Transaction::new::<GetMsg>();
        let a = peer(1);

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

        registry.record_hop(&tx, &a);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn hop_budget_share_threshold() {
        use std::time::Duration;
        let start = tokio::time::Instant::now();
        let end = start + Duration::from_secs(60);
        assert!(hop_had_budget_share(start, Some(start), end));
        assert!(
            !hop_had_budget_share(start, None, end),
            "no blame while the local dispatch has not returned"
        );
        assert!(hop_had_budget_share(
            start,
            Some(start + Duration::from_secs(30)),
            end
        ));
        assert!(!hop_had_budget_share(
            start,
            Some(start + Duration::from_secs(31)),
            end
        ));
        assert!(!hop_had_budget_share(
            start,
            Some(start + Duration::from_secs(59)),
            end
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn touch_hop_restamps_only_a_recorded_hop() {
        let registry = Arc::new(AttemptHopRegistry::new());
        let tx = Transaction::new::<GetMsg>();
        let a = peer(1);
        registry.touch_hop(&tx);
        assert_eq!(registry.len(), 0, "touch never inserts a slot");
        let guard = registry.register(tx);
        registry.touch_hop(&tx);
        assert!(guard.hop_record().is_none(), "no hop to re-stamp");
        let recorded = tokio::time::Instant::now();
        registry.record_hop(&tx, &a);
        let (_, dispatched) = guard.hop_record().expect("hop recorded");
        assert!(dispatched.is_none(), "recorded, but not dispatched yet");
        tokio::time::advance(std::time::Duration::from_secs(40)).await;
        registry.touch_hop(&tx);
        let (hop, dispatched) = guard.hop_record().expect("hop kept");
        assert_eq!(addr(&hop), addr(&a), "the peer is kept");
        assert_eq!(
            dispatched,
            Some(recorded + std::time::Duration::from_secs(40))
        );
    }
}
