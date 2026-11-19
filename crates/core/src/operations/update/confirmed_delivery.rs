//! Confirmed delivery for a node's own originated UPDATE broadcasts (#6143).
//!
//! ## Why
//!
//! A sender counts "the bridge accepted the leg" as delivery and caches its
//! own summary as the peer's (`broadcast_queue::record_delivery_to_interest`).
//! No receiver tells the sender it applied an update, so a leg lost below the
//! bridge is invisible: the sender's next fan-out skips the peer as converged,
//! and only anti-entropy (about 40 minutes per contract at field shared-set
//! sizes) can correct it. In #6143 a transport bug lost every oversized inline
//! message one node sent, for days, and nothing noticed.
//!
//! Loss is only fatal at the ORIGIN: once one or two peers apply an update the
//! mesh's redundancy carries it. So only the originator's legs are confirmed,
//! and only for a client UPDATE that changed this node's state.
//!
//! ## Model: generations
//!
//! One [`Entry`] per (contract, peer) whose version decodes the ack-requested
//! variants. Each originated fan-out to the peer ARMS it, which increments
//! its `generation`. Every flagged leg carries the generation current when its
//! payload was built (read before the payload, so the payload includes at
//! least that generation's update), recorded in a bounded tx -> generation
//! map. The receiver answers a flagged leg, and only a flagged leg, with
//! `InterestMessage::BroadcastAck`. An Applied or AlreadyHad ack for a leg of
//! generation `G` sets `confirmed = max(confirmed, G)`; the entry settles when
//! `confirmed` reaches `generation`. An ack can therefore never confirm an
//! update armed after its leg's payload was built.
//!
//! Every leg of the pair, flagged or not, is counted IN FLIGHT from the moment
//! it is queued until its send finishes (a stream's completion included). No
//! timer runs against a pair with a leg in flight: a queued leg, one waiting
//! for a lane permit, or one still streaming is given its chance first.
//!
//! | Event | Effect |
//! |---|---|
//! | arm | `generation += 1`, record its arm time |
//! | flagged leg built | record `tx -> generation` |
//! | Applied / AlreadyHad ack for `G` | `confirmed = max(confirmed, G)`; settle (remove) once `confirmed >= generation`; a resend for a generation now confirmed is cancelled |
//! | ack for an unknown tx | nothing (counted `ack_unmatched`) |
//! | transient drop ack (rate limit, merge budget, overload) | nothing (counted); the timers handle it |
//! | final drop ack for `G == generation` | clear the belief; remove once no leg is in flight, re-clearing as each in-flight leg's bookkeeping ends |
//! | final drop ack for `G < generation` | nothing: a newer leg is pending |
//! | sweep, no resend outstanding, `confirmed < generation`, nothing in flight, the oldest unconfirmed arm >= [`ESCALATE_AFTER`] old, and the first flagged leg for it finished >= [`ACK_TIMEOUT`] ago (or never sent and [`UNSENT_TTL`] old) | queue ONE full-state resend (heal) |
//! | resend leg built | if its generation is already confirmed, it goes unflagged; else record it and mark the resend sent |
//! | sweep, resend sent, its send finished >= [`ACK_TIMEOUT`] ago, its generation unconfirmed | give up: clear the belief, count, WARN at most once per contract per [`GIVE_UP_WARN_INTERVAL`] |
//! | sweep, oldest unconfirmed arm >= [`MAX_ENTRY_AGE`] old | give up (bounds a pair whose legs are always in flight, or a resend that never went) |
//! | converged skip of a leg | settle only if no flagged leg was recorded and only the latest generation is unconfirmed |
//! | peer disconnects | drop its entries and counts |
//!
//! A confirmed resend lets a later unconfirmed generation escalate again, so a
//! pair whose inline path is broken but whose streams work keeps receiving
//! sustained writes, one resend per confirmed round trip.
//!
//! ## Bounds
//!
//! At most [`MAX_PENDING`] entries (an arm past that is counted, not tracked),
//! [`MAX_ARM_TIMES`] arm times and [`MAX_LEGS`] legs per entry. One sweep task
//! per node runs only while entries exist.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::message::{BroadcastAckOutcome, BroadcastDropReason, Transaction};
use crate::ring::PeerKey;

/// How long after a flagged leg's send finished the sender waits for its ack.
/// Above the 5 s floor of the design's band because the ack is sent after the
/// receiver's merge, which waits in its contract handler queue on a loaded
/// node; a spurious timeout costs one full-state resend.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// The oldest unconfirmed generation must be at least this old before its
/// resend is queued. Measured from its arm, which coalescing never moves, so
/// sustained writes cannot postpone it; long enough that a healthy writer
/// whose acks take several seconds is never resent to.
pub(crate) const ESCALATE_AFTER: Duration = Duration::from_secs(20);

/// A generation that was armed but never carried by a flagged leg (the leg was
/// skipped, evicted, or refused before it was flagged) is resent after this.
pub(crate) const UNSENT_TTL: Duration = Duration::from_secs(120);

/// Hard bound on an unconfirmed generation's age. Gives up on a pair whose
/// legs are always in flight, or whose resend never went.
pub(crate) const MAX_ENTRY_AGE: Duration = Duration::from_secs(600);

/// Hard cap on pending entries per node; overflow is counted, not tracked.
pub(crate) const MAX_PENDING: usize = 1024;

/// Arm times kept per entry (oldest dropped). Escalation reads the oldest
/// unconfirmed one; a dropped older time only makes escalation later.
pub(crate) const MAX_ARM_TIMES: usize = 16;

/// Flagged legs kept per entry (oldest dropped). An ack for a dropped leg is
/// unmatched and changes nothing.
pub(crate) const MAX_LEGS: usize = 8;

/// How often the sweep runs while entries exist.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// At most one give-up WARN per contract per this long.
pub(crate) const GIVE_UP_WARN_INTERVAL: Duration = Duration::from_secs(600);

/// Bound on the give-up WARN throttle map.
const MAX_WARN_ENTRIES: usize = 1024;

/// What a leg to a peer owes the confirmed-delivery protocol. The generation
/// is read when the duty is, before the leg's payload is built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegDuty {
    /// Nothing pending for this (contract, peer): an ordinary leg.
    None,
    /// Send it flagged, carrying this generation.
    RequestAck(u64),
    /// The queued resend: the FULL state, on the stream path, flagged,
    /// bypassing the converged-belief skip (the belief is what it distrusts).
    ResendFull(u64),
}

impl LegDuty {
    pub(crate) fn is_flagged(self) -> bool {
        !matches!(self, LegDuty::None)
    }

    pub(crate) fn is_resend(self) -> bool {
        matches!(self, LegDuty::ResendFull(_))
    }

    fn generation(self) -> Option<u64> {
        match self {
            LegDuty::None => None,
            LegDuty::RequestAck(generation) | LegDuty::ResendFull(generation) => Some(generation),
        }
    }
}

/// Outcome of arming one target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmOutcome {
    Armed,
    Coalesced,
    /// The table is full; this target is not tracked.
    Overflow,
}

/// Which send a flagged leg was, for counting at the send site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlaggedSend {
    /// The first flagged leg of this entry.
    First,
    /// A later flagged leg of the same entry. Not counted as a new request.
    Reflag,
    /// The resend.
    Resend,
    /// No entry: the pair settled while the leg was being built.
    Untracked,
}

/// What an inbound ack did to the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckEffect {
    /// The entry is confirmed up to its latest generation and removed.
    Confirmed { latency: Duration },
    /// An older generation was confirmed; a newer one is still pending.
    Partial,
    /// A final drop of the latest generation: the caller clears its belief.
    DroppedFinal {
        key: ContractKey,
        reason: BroadcastDropReason,
    },
    /// A drop the table ignores: transient, or of an older generation.
    DroppedIgnored { reason: BroadcastDropReason },
    /// No entry or leg for this transaction.
    Unmatched,
}

/// One thing the sweep decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SweepAction {
    /// Queue one full-state resend to `addr`.
    Resend {
        key: ContractKey,
        peer: PeerKey,
        addr: SocketAddr,
    },
    /// Stop: clear the belief, count, maybe WARN.
    GiveUp {
        key: ContractKey,
        peer: PeerKey,
        addr: SocketAddr,
    },
}

/// Whether a drop reason is transient load rather than a property of the
/// payload or the receiver. A transient drop is treated as no ack at all, so
/// the timers decide; a final one ends tracking of the latest generation.
pub(crate) fn is_transient(reason: BroadcastDropReason) -> bool {
    match reason {
        BroadcastDropReason::RateLimited
        | BroadcastDropReason::MergeBudget
        | BroadcastDropReason::Overloaded => true,
        BroadcastDropReason::NotHosted
        | BroadcastDropReason::Backoff
        | BroadcastDropReason::MergeFailed => false,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Leg {
    tx: Transaction,
    generation: u64,
    /// When its send finished; `None` while it is being sent.
    finished: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resend {
    None,
    /// Queued by the sweep; not yet built.
    Queued,
    /// Built, carrying `generation`; `deadline` once its send finished.
    Sent {
        generation: u64,
        deadline: Option<Instant>,
    },
}

#[derive(Debug, Clone)]
struct Entry {
    key: ContractKey,
    addr: SocketAddr,
    generation: u64,
    confirmed: u64,
    /// (generation, arm time) for unconfirmed generations, oldest first.
    arm_times: VecDeque<(u64, Instant)>,
    /// The last [`MAX_LEGS`] flagged legs.
    legs: VecDeque<Leg>,
    resend: Resend,
    /// A final drop of the latest generation arrived; the entry is removed
    /// once the pair has no leg in flight, re-clearing the belief as each
    /// in-flight leg's bookkeeping ends. The one piece of state beyond the
    /// generation model.
    final_drop: bool,
    flagged_any: bool,
}

impl Entry {
    fn new(key: ContractKey, addr: SocketAddr) -> Self {
        Self {
            key,
            addr,
            generation: 0,
            confirmed: 0,
            arm_times: VecDeque::new(),
            legs: VecDeque::new(),
            resend: Resend::None,
            final_drop: false,
            flagged_any: false,
        }
    }

    fn arm(&mut self, now: Instant) {
        if self.final_drop {
            // Everything up to the dropped generation is resolved; this is a
            // new update.
            self.final_drop = false;
            self.confirmed = self.generation;
            self.arm_times.clear();
            self.legs.clear();
            self.resend = Resend::None;
            self.flagged_any = false;
        }
        self.generation += 1;
        if self.arm_times.len() >= MAX_ARM_TIMES {
            self.arm_times.pop_front();
        }
        self.arm_times.push_back((self.generation, now));
    }

    fn settled(&self) -> bool {
        self.confirmed >= self.generation
    }

    fn confirm(&mut self, generation: u64) {
        self.confirmed = self.confirmed.max(generation);
        self.arm_times.retain(|(g, _)| *g > self.confirmed);
        self.legs.retain(|leg| leg.generation > self.confirmed);
        match self.resend {
            Resend::Sent { generation, .. } if generation <= self.confirmed => {
                self.resend = Resend::None;
            }
            Resend::None | Resend::Queued | Resend::Sent { .. } => {}
        }
    }

    /// Arm time of the oldest unconfirmed generation.
    fn oldest_unconfirmed(&self) -> Option<Instant> {
        self.arm_times.front().map(|(_, at)| *at)
    }

    /// When the first flagged leg carrying an unconfirmed generation finished.
    fn first_unconfirmed_leg_done(&self) -> Option<Instant> {
        self.legs.iter().filter_map(|leg| leg.finished).min()
    }
}

type PairKey = (ContractInstanceId, PeerKey);

#[derive(Default)]
struct Inner {
    entries: HashMap<PairKey, Entry>,
    /// Legs of each pair from enqueue until their send finished, for every
    /// pair (tracked or not), so the count never depends on whether an entry
    /// existed when the leg was queued.
    in_flight: HashMap<PairKey, u32>,
    /// Whether a sweep task is running. Read and written under the same lock
    /// as `entries`, so an arm cannot insert into a table the sweep has just
    /// seen empty and is leaving.
    sweeping: bool,
    last_warn: HashMap<ContractInstanceId, Instant>,
}

/// The sender's pending-confirmation table. See the module docs.
pub(crate) struct ConfirmedDelivery {
    inner: Mutex<Inner>,
    time_source: crate::util::time_source::DynTimeSource,
}

impl ConfirmedDelivery {
    pub(crate) fn new(time_source: crate::util::time_source::DynTimeSource) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            time_source,
        }
    }

    /// The table's clock (tokio-backed in production, like `BroadcastSpacing`,
    /// because the sweep waits on a tokio sleep).
    pub(crate) fn now(&self) -> Instant {
        self.time_source.now()
    }

    /// Arm `peer` for the originated update of `key` just fanned out. Returns
    /// the outcome and whether the caller must start the sweep task.
    pub(crate) fn arm(
        &self,
        key: ContractKey,
        peer: PeerKey,
        addr: SocketAddr,
        now: Instant,
    ) -> (ArmOutcome, bool) {
        let mut inner = self.inner.lock();
        let pair = (*key.id(), peer);
        let outcome = if let Some(entry) = inner.entries.get_mut(&pair) {
            entry.addr = addr;
            entry.arm(now);
            ArmOutcome::Coalesced
        } else if inner.entries.len() >= MAX_PENDING {
            return (ArmOutcome::Overflow, false);
        } else {
            let mut entry = Entry::new(key, addr);
            entry.arm(now);
            inner.entries.insert(pair, entry);
            ArmOutcome::Armed
        };
        let start_sweep = !inner.sweeping;
        inner.sweeping = true;
        (outcome, start_sweep)
    }

    /// A leg of the pair entered flight (queued, or started directly).
    pub(crate) fn leg_started(&self, key: &ContractInstanceId, peer: &PeerKey) {
        *self
            .inner
            .lock()
            .in_flight
            .entry((*key, peer.clone()))
            .or_insert(0) += 1;
    }

    /// A leg of the pair left flight: its send finished, or it was evicted,
    /// superseded or skipped. Called AFTER the leg's delivery bookkeeping.
    /// Returns true when the caller must clear its belief about the peer
    /// again, because a final drop arrived while this leg was in flight.
    pub(crate) fn leg_ended(&self, key: &ContractInstanceId, peer: &PeerKey) -> bool {
        let mut inner = self.inner.lock();
        let pair = (*key, peer.clone());
        let remaining = match inner.in_flight.get_mut(&pair) {
            Some(count) => {
                *count = count.saturating_sub(1);
                *count
            }
            None => 0,
        };
        if remaining == 0 {
            inner.in_flight.remove(&pair);
        }
        let Some(entry) = inner.entries.get(&pair) else {
            return false;
        };
        if !entry.final_drop {
            return false;
        }
        if remaining == 0 {
            inner.entries.remove(&pair);
        }
        true
    }

    fn in_flight(inner: &Inner, pair: &PairKey) -> u32 {
        inner.in_flight.get(pair).copied().unwrap_or(0)
    }

    /// What the next leg to `peer` for `key` owes. Reads only. Read BEFORE the
    /// leg's payload is built, so the generation it returns is one the payload
    /// includes.
    pub(crate) fn leg_duty(&self, key: &ContractInstanceId, peer: &PeerKey) -> LegDuty {
        let inner = self.inner.lock();
        match inner.entries.get(&(*key, peer.clone())) {
            None => LegDuty::None,
            Some(entry) if entry.final_drop || entry.settled() => LegDuty::None,
            Some(entry) if entry.resend == Resend::Queued => LegDuty::ResendFull(entry.generation),
            Some(entry) => LegDuty::RequestAck(entry.generation),
        }
    }

    /// A flagged leg with transaction `tx`, carrying `duty`'s generation, is
    /// about to be sent. Recorded BEFORE the send so an early ack matches.
    pub(crate) fn record_flagged_send(
        &self,
        key: &ContractInstanceId,
        peer: &PeerKey,
        tx: Transaction,
        duty: LegDuty,
    ) -> FlaggedSend {
        let Some(generation) = duty.generation() else {
            return FlaggedSend::Untracked;
        };
        let mut inner = self.inner.lock();
        let Some(entry) = inner.entries.get_mut(&(*key, peer.clone())) else {
            return FlaggedSend::Untracked;
        };
        if entry.final_drop || generation <= entry.confirmed {
            return FlaggedSend::Untracked;
        }
        if entry.legs.len() >= MAX_LEGS {
            entry.legs.pop_front();
        }
        entry.legs.push_back(Leg {
            tx,
            generation,
            finished: None,
        });
        let first = !entry.flagged_any;
        entry.flagged_any = true;
        if duty.is_resend() && entry.resend == Resend::Queued {
            entry.resend = Resend::Sent {
                generation,
                deadline: None,
            };
            return FlaggedSend::Resend;
        }
        if first {
            FlaggedSend::First
        } else {
            FlaggedSend::Reflag
        }
    }

    /// The flagged leg `tx` finished sending at `now` (a stream: its
    /// completion). Starts the resend's ack deadline if this was the resend.
    pub(crate) fn send_finished(
        &self,
        key: &ContractInstanceId,
        peer: &PeerKey,
        tx: Transaction,
        now: Instant,
    ) {
        let mut inner = self.inner.lock();
        let Some(entry) = inner.entries.get_mut(&(*key, peer.clone())) else {
            return;
        };
        let Some(leg) = entry.legs.iter_mut().find(|leg| leg.tx == tx) else {
            return;
        };
        leg.finished = Some(now);
        let generation = leg.generation;
        if let Resend::Sent {
            generation: sent,
            deadline: deadline @ None,
        } = &mut entry.resend
        {
            if *sent == generation {
                *deadline = Some(now + ACK_TIMEOUT);
            }
        }
    }

    /// A leg was skipped because our belief says the peer already holds our
    /// state. Only a just-armed update with no flagged leg recorded can match
    /// that honestly (the peer told us); once a flagged leg went, the belief
    /// may be what a lost leg wrote. Returns whether the entry settled.
    pub(crate) fn settle_converged(&self, key: &ContractInstanceId, peer: &PeerKey) -> bool {
        let mut inner = self.inner.lock();
        let pair = (*key, peer.clone());
        let settle = inner.entries.get(&pair).is_some_and(|entry| {
            !entry.final_drop
                && entry.legs.is_empty()
                && !entry.flagged_any
                && entry.confirmed + 1 == entry.generation
        });
        if settle {
            inner.entries.remove(&pair);
        }
        settle
    }

    /// An ack for `tx` arrived from `peer`.
    pub(crate) fn on_ack(
        &self,
        key: &ContractInstanceId,
        peer: &PeerKey,
        tx: Transaction,
        outcome: BroadcastAckOutcome,
        now: Instant,
    ) -> AckEffect {
        let mut inner = self.inner.lock();
        let pair = (*key, peer.clone());
        let in_flight = Self::in_flight(&inner, &pair);
        let Some(entry) = inner.entries.get_mut(&pair) else {
            return AckEffect::Unmatched;
        };
        let Some(leg) = entry.legs.iter().find(|leg| leg.tx == tx).copied() else {
            return AckEffect::Unmatched;
        };
        match outcome {
            BroadcastAckOutcome::Applied | BroadcastAckOutcome::AlreadyHad => {
                entry.confirm(leg.generation);
                if entry.settled() {
                    inner.entries.remove(&pair);
                    return AckEffect::Confirmed {
                        latency: leg
                            .finished
                            .map_or(Duration::ZERO, |done| now.saturating_duration_since(done)),
                    };
                }
                AckEffect::Partial
            }
            BroadcastAckOutcome::Dropped(reason) => {
                if is_transient(reason) || leg.generation < entry.generation {
                    return AckEffect::DroppedIgnored { reason };
                }
                let full_key = entry.key;
                if in_flight == 0 {
                    inner.entries.remove(&pair);
                } else {
                    entry.final_drop = true;
                }
                AckEffect::DroppedFinal {
                    key: full_key,
                    reason,
                }
            }
        }
    }

    /// The peer disconnected: its entries and in-flight counts are discarded.
    pub(crate) fn remove_peer(&self, peer: &PeerKey) -> usize {
        let mut inner = self.inner.lock();
        let before = inner.entries.len();
        inner.entries.retain(|(_, p), _| p != peer);
        inner.in_flight.retain(|(_, p), _| p != peer);
        before - inner.entries.len()
    }

    /// Advance every entry. Returns the actions, and whether the sweep should
    /// keep running (false when the table is empty, in which case the sweep
    /// flag is cleared under the same lock).
    pub(crate) fn sweep(&self, now: Instant) -> (Vec<SweepAction>, bool) {
        let mut inner = self.inner.lock();
        let Inner {
            entries, in_flight, ..
        } = &mut *inner;
        let mut actions = Vec::new();
        let mut give_up = Vec::new();
        for (pair, entry) in entries.iter_mut() {
            if entry.final_drop || entry.settled() {
                continue;
            }
            let Some(oldest) = entry.oldest_unconfirmed() else {
                continue;
            };
            let age = now.saturating_duration_since(oldest);
            if age >= MAX_ENTRY_AGE {
                give_up.push(pair.clone());
                continue;
            }
            match entry.resend {
                Resend::Sent {
                    generation,
                    deadline: Some(deadline),
                } if now >= deadline && entry.confirmed < generation => {
                    give_up.push(pair.clone());
                }
                Resend::Sent { .. } | Resend::Queued => {}
                Resend::None => {
                    let quiet = in_flight.get(pair).copied().unwrap_or(0) == 0;
                    let leg_waited = match entry.first_unconfirmed_leg_done() {
                        Some(done) => now.saturating_duration_since(done) >= ACK_TIMEOUT,
                        None => age >= UNSENT_TTL,
                    };
                    if quiet && age >= ESCALATE_AFTER && leg_waited {
                        entry.resend = Resend::Queued;
                        actions.push(SweepAction::Resend {
                            key: entry.key,
                            peer: pair.1.clone(),
                            addr: entry.addr,
                        });
                    }
                }
            }
        }
        for pair in give_up {
            if let Some(entry) = entries.remove(&pair) {
                actions.push(SweepAction::GiveUp {
                    key: entry.key,
                    peer: pair.1,
                    addr: entry.addr,
                });
            }
        }
        let keep_running = !entries.is_empty();
        if !keep_running {
            inner.sweeping = false;
        }
        (actions, keep_running)
    }

    /// The sweep task stopped without emptying the table (it panicked, or its
    /// node is gone): let the next arm start a new one.
    fn sweep_stopped(&self) {
        self.inner.lock().sweeping = false;
    }

    /// Whether a give-up for `key` at `now` may WARN (at most one per
    /// contract per [`GIVE_UP_WARN_INTERVAL`]). Records the WARN when it may.
    pub(crate) fn may_warn(&self, key: &ContractInstanceId, now: Instant) -> bool {
        let mut inner = self.inner.lock();
        if let Some(last) = inner.last_warn.get(key) {
            if now.saturating_duration_since(*last) < GIVE_UP_WARN_INTERVAL {
                return false;
            }
        }
        if inner.last_warn.len() >= MAX_WARN_ENTRIES && !inner.last_warn.contains_key(key) {
            inner
                .last_warn
                .retain(|_, last| now.saturating_duration_since(*last) < GIVE_UP_WARN_INTERVAL);
            if inner.last_warn.len() >= MAX_WARN_ENTRIES {
                return false;
            }
        }
        inner.last_warn.insert(*key, now);
        true
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    #[cfg(test)]
    fn is_sweeping(&self) -> bool {
        self.inner.lock().sweeping
    }
}

/// Counts one leg of a pair as in flight for as long as it lives, and ends it
/// on every exit, after the leg's delivery bookkeeping (it is dropped at the
/// end of the send function). A leg the broadcast queue already counted at
/// enqueue passes `counted_by_queue`; any other leg is counted here.
pub(crate) struct LegInFlight<'a> {
    op_manager: &'a crate::node::OpManager,
    key: ContractKey,
    peer: PeerKey,
}

impl<'a> LegInFlight<'a> {
    pub(crate) fn begin(
        op_manager: &'a crate::node::OpManager,
        key: ContractKey,
        peer: PeerKey,
        counted_by_queue: bool,
    ) -> Self {
        if !counted_by_queue {
            op_manager.confirmed_delivery.leg_started(key.id(), &peer);
        }
        Self {
            op_manager,
            key,
            peer,
        }
    }
}

impl Drop for LegInFlight<'_> {
    fn drop(&mut self) {
        if self
            .op_manager
            .confirmed_delivery
            .leg_ended(self.key.id(), &self.peer)
        {
            clear_belief(self.op_manager, &self.key, &self.peer);
        }
    }
}

/// Count a confirmed-delivery event for `key`, in the per-contract
/// propagation summary and the simulation's test metrics.
pub(crate) fn count(
    op_manager: &crate::node::OpManager,
    key: &ContractInstanceId,
    event: super::propagation_stats::PropagationEvent,
) {
    super::propagation_stats::record_propagation_event(op_manager, key, event);
    crate::config::GlobalTestMetrics::record_confirmed_delivery_event(event.label());
}

/// The propagation event for a dropped ack's reason.
pub(crate) fn dropped_event(
    reason: BroadcastDropReason,
) -> super::propagation_stats::PropagationEvent {
    use super::propagation_stats::PropagationEvent;
    match reason {
        BroadcastDropReason::NotHosted => PropagationEvent::AckDroppedNotHosted,
        BroadcastDropReason::Backoff => PropagationEvent::AckDroppedBackoff,
        BroadcastDropReason::RateLimited => PropagationEvent::AckDroppedRateLimited,
        BroadcastDropReason::MergeBudget => PropagationEvent::AckDroppedMergeBudget,
        BroadcastDropReason::MergeFailed => PropagationEvent::AckDroppedMergeFailed,
        BroadcastDropReason::Overloaded => PropagationEvent::AckDroppedOverloaded,
    }
}

/// The drop reason a receiver reports for a merge that returned `err`.
/// Saturation (a full contract queue, or a scheduler timeout before the merge
/// ran) is `Overloaded`, a transient reason; a merge that ran and failed is
/// `MergeFailed`, a final one.
pub(crate) fn drop_reason_for_merge_error(err: &crate::operations::OpError) -> BroadcastDropReason {
    if err.is_contract_queue_full() || err.is_scheduler_timeout() {
        BroadcastDropReason::Overloaded
    } else {
        BroadcastDropReason::MergeFailed
    }
}

/// The duty of a leg about to go to `peer_addr`, re-checking the version gate
/// at the send site: arming already checked it, but a queued leg can outlive a
/// reconnect at the same key, and the gate is what keeps undecodable variants
/// off the wire.
pub(crate) fn leg_duty_for(
    op_manager: &crate::node::OpManager,
    key: &ContractInstanceId,
    peer: &PeerKey,
    peer_addr: SocketAddr,
) -> LegDuty {
    if !op_manager
        .ring
        .connection_manager
        .supports_confirmed_delivery(peer_addr)
    {
        return LegDuty::None;
    }
    op_manager.confirmed_delivery.leg_duty(key, peer)
}

/// Arm every target of an ORIGINATED fan-out that can decode the flag.
/// Called once per originated `BroadcastStateChange`, before the legs go.
pub(crate) fn arm_fanout(
    op_manager: &Arc<crate::node::OpManager>,
    key: ContractKey,
    targets: &[crate::ring::PeerKeyLocation],
) {
    use super::propagation_stats::PropagationEvent;
    let now = op_manager.confirmed_delivery.now();
    let mut start_sweep = false;
    for target in targets {
        let Some(addr) = target.socket_addr() else {
            continue;
        };
        if !op_manager
            .ring
            .connection_manager
            .supports_confirmed_delivery(addr)
        {
            continue;
        }
        let peer = PeerKey::from(target.pub_key().clone());
        let (outcome, start) = op_manager.confirmed_delivery.arm(key, peer, addr, now);
        start_sweep |= start;
        if outcome == ArmOutcome::Overflow {
            count(op_manager, key.id(), PropagationEvent::PendingOverflow);
        }
    }
    if start_sweep {
        crate::config::GlobalExecutor::spawn(run_sweep(Arc::downgrade(op_manager)));
    }
}

/// The sender's handling of an inbound ack from `sender_addr`.
pub(crate) fn handle_ack(
    op_manager: &crate::node::OpManager,
    sender_addr: SocketAddr,
    key: ContractInstanceId,
    tx: Transaction,
    outcome: BroadcastAckOutcome,
) {
    use super::propagation_stats::PropagationEvent;
    let Some(peer) = op_manager
        .ring
        .connection_manager
        .get_peer_by_addr(sender_addr)
        .map(|pkl| PeerKey::from(pkl.pub_key().clone()))
    else {
        return;
    };
    let now = op_manager.confirmed_delivery.now();
    match op_manager
        .confirmed_delivery
        .on_ack(&key, &peer, tx, outcome, now)
    {
        AckEffect::Confirmed { latency } => {
            count(op_manager, &key, PropagationEvent::AckConfirmed);
            tracing::debug!(
                contract = %key,
                peer = %sender_addr,
                latency_ms = latency.as_millis() as u64,
                ?outcome,
                "confirmed delivery: originated update acknowledged"
            );
        }
        AckEffect::Partial => {
            count(op_manager, &key, PropagationEvent::AckPathProven);
        }
        AckEffect::DroppedFinal {
            key: full_key,
            reason,
        } => {
            count(op_manager, &key, dropped_event(reason));
            clear_belief(op_manager, &full_key, &peer);
            tracing::debug!(
                contract = %key,
                peer = %sender_addr,
                ?reason,
                "confirmed delivery: peer dropped the latest originated update; \
                 belief cleared, no resend"
            );
        }
        AckEffect::DroppedIgnored { reason } => {
            count(op_manager, &key, dropped_event(reason));
        }
        AckEffect::Unmatched => {
            count(op_manager, &key, PropagationEvent::AckUnmatched);
        }
    }
}

/// Forget what we believe `peer` holds for `key`, so the next broadcast and
/// anti-entropy treat the pair as unknown rather than converged.
pub(crate) fn clear_belief(op_manager: &crate::node::OpManager, key: &ContractKey, peer: &PeerKey) {
    op_manager.interest_manager.clear_peer_summary(
        key,
        peer,
        crate::ring::interest::SummaryMissingReason::ClearedByUnconfirmedDelivery,
    );
}

/// Resets the sweep flag if the sweep task ends early (a panic, or its node
/// is gone), so a later arm can start a new one.
struct SweepGuard(std::sync::Weak<crate::node::OpManager>, bool);

impl Drop for SweepGuard {
    fn drop(&mut self) {
        if self.1 {
            return;
        }
        if let Some(op_manager) = self.0.upgrade() {
            op_manager.confirmed_delivery.sweep_stopped();
        }
    }
}

/// The sweep task: runs while entries exist, then exits (a later arm starts
/// it again). Holds only a `Weak` between ticks so a dropped node ends it.
async fn run_sweep(weak: std::sync::Weak<crate::node::OpManager>) {
    use super::propagation_stats::PropagationEvent;
    let mut guard = SweepGuard(weak.clone(), false);
    loop {
        tokio::time::sleep(SWEEP_INTERVAL).await;
        let Some(op_manager) = weak.upgrade() else {
            return;
        };
        let now = op_manager.confirmed_delivery.now();
        let (actions, keep_running) = op_manager.confirmed_delivery.sweep(now);
        // One snapshot per contract per pass, however many peers resend.
        let mut snapshots: HashMap<
            ContractInstanceId,
            Option<freenet_stdlib::prelude::WrappedState>,
        > = HashMap::new();
        for action in actions {
            match action {
                SweepAction::Resend { key, peer, addr } => {
                    count(&op_manager, key.id(), PropagationEvent::AckTimeout);
                    // The heal leg reads the table again when it is built: if
                    // the resend's generation was confirmed meanwhile, it goes
                    // unflagged (and is usually skipped as converged).
                    let state = match snapshots.get(key.id()) {
                        Some(state) => state.clone(),
                        None => {
                            let state = crate::node::broadcast_contract_snapshot(&op_manager, &key)
                                .await
                                .map(|(state, _summary)| state);
                            snapshots.insert(*key.id(), state.clone());
                            state
                        }
                    };
                    let Some(state) = state else {
                        // No local state to resend; MAX_ENTRY_AGE gives up.
                        continue;
                    };
                    if op_manager
                        .try_notify_node_event(crate::message::NodeEvent::SyncStateToPeer {
                            key,
                            new_state: state,
                            target: addr,
                        })
                        .is_err()
                    {
                        // The event loop is saturated. The resend stays
                        // queued and MAX_ENTRY_AGE gives up on it.
                        tracing::debug!(
                            contract = %key,
                            peer = %addr,
                            ?peer,
                            "confirmed delivery: resend dropped, event channel full"
                        );
                    }
                }
                SweepAction::GiveUp { key, peer, addr } => {
                    count(&op_manager, key.id(), PropagationEvent::UnconfirmedGiveup);
                    clear_belief(&op_manager, &key, &peer);
                    if op_manager.confirmed_delivery.may_warn(key.id(), now) {
                        tracing::warn!(
                            contract = %key,
                            peer = %addr,
                            "confirmed delivery: an originated update was not \
                             confirmed by this peer after a full-state resend; \
                             belief about the peer cleared (#6143). At most one \
                             such line per contract per 10 minutes"
                        );
                    }
                }
            }
        }
        if !keep_running {
            // `sweep` cleared the flag under its lock; the guard must not.
            guard.1 = true;
            return;
        }
    }
}

// ── Receiver side ────────────────────────────────────────────────────────

tokio::task_local! {
    /// The terminal outcome a relay broadcast driver noted for the leg it is
    /// handling. Scoped by [`with_receiver_ack`]; the drivers note into it at
    /// each exit, so their signatures (and their many direct test callers)
    /// stay as they are.
    static RECEIVER_OUTCOME: std::cell::Cell<Option<BroadcastAckOutcome>>;
}

/// Record what the receiver did with the broadcast it is handling. A no-op
/// outside [`with_receiver_ack`].
pub(crate) fn note_receiver_outcome(outcome: BroadcastAckOutcome) {
    let _outside_a_driver = RECEIVER_OUTCOME.try_with(|slot| slot.set(Some(outcome)));
}

/// Run a relay broadcast driver and return what it noted, if anything.
pub(crate) async fn with_receiver_ack<F: std::future::Future>(
    driver: F,
) -> (F::Output, Option<BroadcastAckOutcome>) {
    RECEIVER_OUTCOME
        .scope(std::cell::Cell::new(None), async move {
            let result = driver.await;
            (result, RECEIVER_OUTCOME.with(|slot| slot.get()))
        })
        .await
}

/// Answer a flagged leg with what the driver noted. `None` (the driver ended
/// before deciding, e.g. the stream never arrived) sends nothing: the
/// sender's deadline is the right answer to a leg we never received.
///
/// `try_notify`: this runs on the driver's task, but an ack is advisory and a
/// full event channel must not hold the driver. A dropped ack costs one
/// resend.
pub(crate) fn send_receiver_ack(
    op_manager: &crate::node::OpManager,
    sender_addr: SocketAddr,
    key: ContractInstanceId,
    tx: Transaction,
    outcome: Option<BroadcastAckOutcome>,
) {
    let Some(outcome) = outcome else {
        return;
    };
    if op_manager
        .try_notify_node_event(crate::message::NodeEvent::SendInterestMessage {
            target: sender_addr,
            message: crate::message::InterestMessage::BroadcastAck { key, tx, outcome },
        })
        .is_err()
    {
        tracing::debug!(
            contract = %key,
            peer = %sender_addr,
            "confirmed delivery: ack dropped, event channel full"
        );
    }
}

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

    /// Inline legs swallowed for `key` since its rules were last cleared.
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
        inline_leg_fault_injection::swallows(op_manager.ring.connection_manager.get_own_addr(), key)
    }
    #[cfg(not(any(test, feature = "testing")))]
    {
        let _ = (op_manager, key);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The receiver's outcome is captured only inside a driver scope, and the
    /// last note wins (each driver notes once, at its terminal exit). Outside
    /// a scope a note is a no-op, so the non-broadcast drivers that share the
    /// code are unaffected.
    #[tokio::test]
    async fn the_receiver_outcome_is_scoped_to_the_driver() {
        note_receiver_outcome(BroadcastAckOutcome::Applied);
        let ((), outcome) = with_receiver_ack(async {}).await;
        assert_eq!(outcome, None, "a driver that notes nothing sends no ack");
        let ((), outcome) = with_receiver_ack(async {
            note_receiver_outcome(BroadcastAckOutcome::Dropped(BroadcastDropReason::Backoff));
        })
        .await;
        assert_eq!(
            outcome,
            Some(BroadcastAckOutcome::Dropped(BroadcastDropReason::Backoff))
        );
    }

    /// The load reasons are transient (no ack, timers decide); the others end
    /// tracking of the latest generation.
    #[test]
    fn transient_reasons_are_exactly_the_load_ones() {
        assert!(is_transient(BroadcastDropReason::RateLimited));
        assert!(is_transient(BroadcastDropReason::MergeBudget));
        assert!(is_transient(BroadcastDropReason::Overloaded));
        assert!(!is_transient(BroadcastDropReason::NotHosted));
        assert!(!is_transient(BroadcastDropReason::Backoff));
        assert!(!is_transient(BroadcastDropReason::MergeFailed));
    }

    /// A full contract queue is `Overloaded` (transient); any other merge
    /// error is `MergeFailed` (final). The scheduler-timeout case is covered
    /// next to its constructor in `contract::executor`.
    #[test]
    fn a_saturated_merge_is_overloaded_and_a_failed_one_is_final() {
        use crate::contract::{ContractQueueFull, ExecutorError};
        use crate::operations::OpError;
        let queue_full = OpError::from(ExecutorError::other(ContractQueueFull));
        assert_eq!(
            drop_reason_for_merge_error(&queue_full),
            BroadcastDropReason::Overloaded
        );
        let failed = OpError::from(ExecutorError::other(anyhow::anyhow!("merge rejected")));
        assert_eq!(
            drop_reason_for_merge_error(&failed),
            BroadcastDropReason::MergeFailed
        );
    }

    /// The body of `fn <sig>...{...}` in `src`, by brace matching.
    fn fn_body<'a>(src: &'a str, sig: &str) -> &'a str {
        let start = src.find(sig).unwrap_or_else(|| panic!("missing {sig}"));
        let open = start + src[start..].find('{').expect("fn body opens");
        let mut depth = 0usize;
        for (i, c) in src[open..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        return &src[open..open + i];
                    }
                }
                _ => {}
            }
        }
        panic!("{sig} does not close")
    }

    const QUEUE_SRC: &str = include_str!("../../node/network_bridge/broadcast_queue.rs");
    const SIM_SRC: &str = include_str!("../../node/network_bridge/p2p_protoc/broadcast.rs");

    /// Both send paths that build the ack-requested variants decide the flag
    /// through `leg_duty_for` (which re-checks the version gate), never
    /// through the raw table, and count every leg in flight.
    #[test]
    fn every_flagged_send_site_reads_its_duty_through_the_version_gate() {
        for (src, sig) in [
            (QUEUE_SRC, "pub(super) async fn broadcast_to_single_peer("),
            (SIM_SRC, "async fn broadcast_state_to_peers("),
        ] {
            let body = fn_body(src, sig);
            assert!(
                body.contains("BroadcastToAckRequested") && body.contains("leg_duty_for("),
                "{sig} must decide the flag through leg_duty_for"
            );
            assert!(
                !body.contains("confirmed_delivery.leg_duty("),
                "{sig} must not read the raw table duty (no version re-check)"
            );
            assert!(
                body.contains("LegInFlight::begin("),
                "{sig} must count each leg in flight until it ends"
            );
        }
    }

    /// The production send path: the leg is in flight from the start, and
    /// its flagged-leg finish (which starts the resend's ack deadline) comes
    /// AFTER the stream's completion wait and after the delivery bookkeeping,
    /// so a long stream is never timed while it is still sending.
    #[test]
    fn the_production_leg_finishes_after_its_stream_completes() {
        let body = fn_body(QUEUE_SRC, "pub(super) async fn broadcast_to_single_peer(");
        let begin = body.find("LegInFlight::begin(").expect("begins in flight");
        let duty = body.find("leg_duty_for(").expect("reads the duty");
        let wait = body
            .find("await_stream_completion_releasing_permit(")
            .expect("waits for the stream");
        let bookkeeping = body
            .rfind("record_delivery_to_interest(")
            .expect("records the delivery");
        let finish = body
            .rfind("finish_flagged_leg(")
            .expect("finishes the flagged leg");
        assert!(begin < duty, "the leg is in flight before its duty is read");
        assert!(
            wait < finish,
            "the stream completes before the leg finishes"
        );
        assert!(
            bookkeeping < finish,
            "the delivery bookkeeping precedes the finish"
        );
    }

    /// The broadcast queue counts a leg in flight BEFORE it can be dequeued,
    /// and ends the count for a superseded or evicted leg.
    #[test]
    fn the_queue_counts_legs_from_enqueue() {
        let body = fn_body(QUEUE_SRC, "pub(crate) async fn enqueue(");
        let started = body.find("leg_started(").expect("counts the leg");
        let inserted = body.find(".enqueue_in_lane(").expect("inserts the leg");
        assert!(started < inserted, "counted before it can be dequeued");
        assert!(
            body.matches("end_queued_leg(").count() >= 2,
            "a superseded and an evicted leg each end their count"
        );
    }
}

/// Behaviour tests for the sender table, written against a small event
/// harness (arm, enqueue, send, finish, ack, sweep) rather than its internal
/// state, so they state what a peer and a writer observe.
#[cfg(test)]
mod behaviour_tests {
    use super::*;
    use crate::transport::TransportKeypair;
    use std::cell::Cell;

    /// One (contract, peer) pair driven by events at explicit times (seconds
    /// from a common origin).
    struct Pair {
        table: ConfirmedDelivery,
        key: ContractKey,
        peer: PeerKey,
        t0: Instant,
        /// Legs enqueued but not yet sent.
        queued: Cell<u32>,
    }

    impl Pair {
        fn new() -> Self {
            Self::with_peer(PeerKey::from(TransportKeypair::new().public().clone()))
        }

        fn with_peer(peer: PeerKey) -> Self {
            Self {
                table: ConfirmedDelivery::new(Arc::new(
                    crate::util::time_source::InstantTimeSrc::new(),
                )),
                key: crate::operations::test_utils::make_contract_key(7),
                peer,
                t0: Instant::now(),
                queued: Cell::new(0),
            }
        }

        fn at(&self, secs: u64) -> Instant {
            self.t0 + Duration::from_secs(secs)
        }

        /// A client update was applied and fanned out to this peer.
        fn arm(&self, secs: u64) -> (ArmOutcome, bool) {
            self.table.arm(
                self.key,
                self.peer.clone(),
                SocketAddr::from(([127, 0, 0, 1], 9)),
                self.at(secs),
            )
        }

        /// A leg for this pair entered the broadcast queue.
        fn enqueue(&self) {
            self.table.leg_started(self.key.id(), &self.peer);
            self.queued.set(self.queued.get() + 1);
        }

        /// A leg for this pair (a queued one if any) started sending, flagged
        /// as its duty says. Returns its transaction id.
        fn send(&self, _secs: u64) -> Transaction {
            if self.queued.get() > 0 {
                self.queued.set(self.queued.get() - 1);
            } else {
                self.table.leg_started(self.key.id(), &self.peer);
            }
            let tx = Transaction::new::<crate::operations::update::UpdateMsg>();
            let duty = self.duty();
            self.table
                .record_flagged_send(self.key.id(), &self.peer, tx, duty);
            tx
        }

        /// The leg's send finished (an inline leg as it is sent; a stream
        /// when its last fragment leaves) and its bookkeeping ended. Returns
        /// whether the belief must be cleared again.
        fn finish(&self, tx: Transaction, secs: u64) -> bool {
            self.table
                .send_finished(self.key.id(), &self.peer, tx, self.at(secs));
            self.table.leg_ended(self.key.id(), &self.peer)
        }

        fn ack(&self, tx: Transaction, outcome: BroadcastAckOutcome, secs: u64) -> AckEffect {
            self.table
                .on_ack(self.key.id(), &self.peer, tx, outcome, self.at(secs))
        }

        fn sweep(&self, secs: u64) -> Vec<SweepAction> {
            self.table.sweep(self.at(secs)).0
        }

        fn duty(&self) -> LegDuty {
            self.table.leg_duty(self.key.id(), &self.peer)
        }

        fn settled(&self) -> bool {
            self.table.len() == 0
        }

        /// One inline flagged leg, sent and finished at `secs`.
        fn leg(&self, secs: u64) -> Transaction {
            let tx = self.send(secs);
            self.finish(tx, secs);
            tx
        }
    }

    fn resends(actions: &[SweepAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, SweepAction::Resend { .. }))
            .count()
    }

    fn give_ups(actions: &[SweepAction]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, SweepAction::GiveUp { .. }))
            .count()
    }

    const APPLIED: BroadcastAckOutcome = BroadcastAckOutcome::Applied;

    fn dropped(reason: BroadcastDropReason) -> BroadcastAckOutcome {
        BroadcastAckOutcome::Dropped(reason)
    }

    // ── H1: an ack never confirms an update armed after its leg ───────────

    /// H1 (a): an ack for the leg of an OLDER update must not settle a newer
    /// update that coalesced into the entry after that leg went.
    #[test]
    fn an_older_legs_ack_does_not_confirm_a_newer_update() {
        let p = Pair::new();
        p.arm(0);
        let first = p.leg(0);
        p.arm(1);
        p.ack(first, APPLIED, 2);
        assert!(!p.settled(), "the update armed at 1 s is not confirmed");
        assert!(matches!(p.duty(), LegDuty::RequestAck(_)));
    }

    /// H1 (b): a resend whose snapshot was read before the latest arm does
    /// not confirm that latest update.
    #[test]
    fn a_resend_snapshot_older_than_the_latest_arm_does_not_confirm_it() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        assert_eq!(resends(&p.sweep(30)), 1);
        assert!(p.duty().is_resend());
        let resend = p.leg(31);
        p.arm(32);
        p.ack(resend, APPLIED, 33);
        assert!(!p.settled(), "the update armed at 32 s is not confirmed");
    }

    /// H1 (c): a final drop of an OLDER leg says nothing about a newer update.
    #[test]
    fn a_final_drop_of_an_older_leg_keeps_the_newer_update_tracked() {
        let p = Pair::new();
        p.arm(0);
        let first = p.leg(0);
        p.arm(1);
        p.ack(first, dropped(BroadcastDropReason::NotHosted), 2);
        assert!(!p.settled(), "the update armed at 1 s is still unconfirmed");
        assert!(p.duty().is_flagged());
    }

    // ── M1: a healthy fast writer pays nothing ─────────────────────────────

    /// M1: a healthy writer (an update every 3 s) whose acks take 7 s pays no
    /// resend and no give-up over two minutes. Also with 12 s acks: slower
    /// than [`ACK_TIMEOUT`], so only the escalation age keeps a writer whose
    /// every ack does arrive from being resent to.
    #[test]
    fn a_healthy_fast_writer_with_slow_acks_gets_no_resend() {
        for ack_latency in [7u64, 12] {
            healthy_writer(ack_latency);
        }
    }

    fn healthy_writer(ack_latency: u64) {
        let p = Pair::new();
        let mut pending: Vec<(u64, Transaction)> = Vec::new();
        let mut actions = Vec::new();
        for t in 0..=120u64 {
            pending.retain(|(due, tx)| {
                if *due == t {
                    p.ack(*tx, APPLIED, t);
                    false
                } else {
                    true
                }
            });
            if t % 3 == 0 {
                p.arm(t);
                let tx = p.leg(t);
                pending.push((t + ack_latency, tx));
            }
            actions.extend(p.sweep(t));
        }
        assert!(
            actions.is_empty(),
            "{ack_latency} s acks: no resend or give-up: {actions:?}"
        );
    }

    /// The other side of M1: the same writer whose legs are all lost reaches
    /// the resend WHILE the writes continue (coalescing cannot postpone it).
    #[test]
    fn a_lossy_fast_writer_is_resent_to_during_the_writes() {
        let p = Pair::new();
        let mut first_resend = None;
        for t in 0..=60u64 {
            if t % 3 == 0 {
                p.arm(t);
                p.leg(t);
            }
            if resends(&p.sweep(t)) > 0 && first_resend.is_none() {
                first_resend = Some(t);
            }
        }
        let at = first_resend.expect("the resend fired");
        assert!(
            at <= ESCALATE_AFTER.as_secs() + 3,
            "escalated at {at} s, from the first arm"
        );
    }

    // ── M2: a late ack cancels a due resend ────────────────────────────────

    /// M2: a late ack for the leg that timed out cancels the queued resend if
    /// it lands before the resend is built: the heal then goes unflagged.
    #[test]
    fn a_late_ack_cancels_a_due_resend() {
        let p = Pair::new();
        p.arm(0);
        let first = p.leg(0);
        assert_eq!(resends(&p.sweep(30)), 1);
        assert!(matches!(
            p.ack(first, APPLIED, 31),
            AckEffect::Confirmed { .. }
        ));
        assert!(p.settled());
        assert_eq!(p.duty(), LegDuty::None, "the heal is no longer a resend");
    }

    // ── M3/M4: no timer while a leg is in flight ──────────────────────────

    /// M3: no resend while one of the pair's legs is still queued (which
    /// includes waiting for a lane permit: the leg is in flight until its
    /// send finishes).
    #[test]
    fn no_resend_while_a_leg_is_queued() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        p.arm(5);
        p.enqueue();
        assert_eq!(resends(&p.sweep(300)), 0, "a leg of the pair is queued");
        let tx = p.send(301);
        assert_eq!(resends(&p.sweep(301)), 0, "and then sending");
        p.finish(tx, 301);
        // The oldest unconfirmed update's own leg had its ack window long
        // ago, so the resend follows as soon as nothing is in flight.
        assert_eq!(resends(&p.sweep(301)), 1);
    }

    /// M4: no resend while a leg is still streaming, however long; the ack
    /// window runs from its completion.
    #[test]
    fn no_resend_while_a_leg_is_streaming() {
        let p = Pair::new();
        p.arm(0);
        let streaming = p.send(0);
        assert_eq!(resends(&p.sweep(500)), 0, "the leg is still streaming");
        p.finish(streaming, 500);
        assert_eq!(resends(&p.sweep(509)), 0);
        assert_eq!(resends(&p.sweep(510)), 1);
    }

    /// A pair whose legs never stop being in flight is still bounded.
    #[test]
    fn a_pair_always_in_flight_is_given_up_on() {
        let p = Pair::new();
        p.arm(0);
        let _ = p.send(0);
        assert_eq!(give_ups(&p.sweep(MAX_ENTRY_AGE.as_secs() - 1)), 0);
        assert_eq!(give_ups(&p.sweep(MAX_ENTRY_AGE.as_secs())), 1);
        assert!(p.settled());
    }

    // ── confirmation ──────────────────────────────────────────────────────

    #[test]
    fn an_ack_confirms_and_removes_the_entry() {
        for outcome in [APPLIED, BroadcastAckOutcome::AlreadyHad] {
            let p = Pair::new();
            p.arm(0);
            let tx = p.leg(0);
            assert!(matches!(p.ack(tx, outcome, 1), AckEffect::Confirmed { .. }));
            assert!(p.settled());
        }
    }

    /// The ack for the newest leg confirms every older update with it.
    #[test]
    fn the_newest_legs_ack_confirms_all_older_updates() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        p.arm(3);
        let newest = p.leg(3);
        assert!(matches!(
            p.ack(newest, APPLIED, 4),
            AckEffect::Confirmed { .. }
        ));
        assert!(p.settled());
    }

    #[test]
    fn an_ack_that_beats_the_send_finish_still_matches() {
        let p = Pair::new();
        p.arm(0);
        let tx = p.send(0);
        assert!(matches!(p.ack(tx, APPLIED, 0), AckEffect::Confirmed { .. }));
        p.finish(tx, 0);
        assert!(p.settled());
    }

    #[test]
    fn an_unknown_ack_changes_nothing() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        let stray = Transaction::new::<crate::operations::update::UpdateMsg>();
        assert_eq!(p.ack(stray, APPLIED, 1), AckEffect::Unmatched);
        assert!(!p.settled());
        // An ack from another peer for this tx does not reach this pair.
        let other = Pair::with_peer(PeerKey::from(TransportKeypair::new().public().clone()));
        assert_eq!(other.ack(stray, APPLIED, 1), AckEffect::Unmatched);
    }

    // ── drops ─────────────────────────────────────────────────────────────

    /// A final drop of the latest update stops tracking it: no resend.
    #[test]
    fn a_final_drop_of_the_latest_update_ends_tracking() {
        let p = Pair::new();
        p.arm(0);
        let tx = p.leg(0);
        assert!(matches!(
            p.ack(tx, dropped(BroadcastDropReason::Backoff), 1),
            AckEffect::DroppedFinal { .. }
        ));
        assert!(p.settled());
        assert!(p.sweep(MAX_ENTRY_AGE.as_secs()).is_empty());
    }

    /// A final drop that arrives while a leg of the pair is still in flight:
    /// the belief is cleared again when that leg's bookkeeping ends, and the
    /// entry goes once nothing is in flight.
    #[test]
    fn a_final_drop_during_a_send_reclears_after_its_bookkeeping() {
        let p = Pair::new();
        p.arm(0);
        let tx = p.send(0);
        assert!(matches!(
            p.ack(tx, dropped(BroadcastDropReason::NotHosted), 0),
            AckEffect::DroppedFinal { .. }
        ));
        assert_eq!(p.duty(), LegDuty::None, "no further leg is flagged");
        assert!(p.finish(tx, 0), "re-clear after the bookkeeping");
        assert!(p.settled());
    }

    /// A transient drop is treated as no ack: the timers resend later.
    #[test]
    fn a_transient_drop_is_left_to_the_timers() {
        let p = Pair::new();
        p.arm(0);
        let tx = p.leg(0);
        assert!(matches!(
            p.ack(tx, dropped(BroadcastDropReason::RateLimited), 1),
            AckEffect::DroppedIgnored { .. }
        ));
        assert_eq!(resends(&p.sweep(30)), 1);
    }

    // ── resend and give-up ────────────────────────────────────────────────

    #[test]
    fn no_ack_resends_once_then_gives_up() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        assert_eq!(resends(&p.sweep(ESCALATE_AFTER.as_secs() - 1)), 0);
        assert_eq!(resends(&p.sweep(ESCALATE_AFTER.as_secs())), 1);
        assert!(p.duty().is_resend());
        assert_eq!(resends(&p.sweep(25)), 0, "one resend queued, not two");
        let resend = p.leg(26);
        assert!(!p.duty().is_resend(), "the resend went");
        assert!(p.sweep(35).is_empty(), "its ack window is still open");
        assert_eq!(give_ups(&p.sweep(36)), 1);
        assert!(p.settled());
        assert!(!p.table.is_sweeping(), "an empty table stops the sweep");
        assert!(p.arm(40).1, "and the next arm starts it again");
        let _ = resend;
    }

    #[test]
    fn the_resend_is_confirmed_by_its_own_ack() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        p.sweep(30);
        let resend = p.leg(31);
        assert!(matches!(
            p.ack(resend, APPLIED, 32),
            AckEffect::Confirmed { .. }
        ));
        assert!(p.settled());
    }

    /// A generation no flagged leg ever carried (the leg was skipped or
    /// refused before it was flagged) is resent after the unsent bound.
    #[test]
    fn an_update_that_never_went_is_resent() {
        let p = Pair::new();
        p.arm(0);
        assert_eq!(resends(&p.sweep(UNSENT_TTL.as_secs() - 1)), 0);
        assert_eq!(resends(&p.sweep(UNSENT_TTL.as_secs())), 1);
    }

    /// A confirmed resend lets a later unconfirmed update escalate again.
    #[test]
    fn a_confirmed_resend_lets_a_later_update_escalate() {
        let p = Pair::new();
        p.arm(0);
        p.leg(0);
        p.sweep(30);
        let resend = p.leg(31);
        p.arm(32);
        p.leg(32);
        assert_eq!(p.ack(resend, APPLIED, 33), AckEffect::Partial);
        assert_eq!(resends(&p.sweep(52)), 1, "the update armed at 32 s");
    }

    // ── converged skip ────────────────────────────────────────────────────

    #[test]
    fn a_converged_skip_settles_only_before_any_flagged_leg() {
        let p = Pair::new();
        p.arm(0);
        assert!(p.table.settle_converged(p.key.id(), &p.peer));
        assert!(p.settled());

        p.arm(1);
        p.leg(1);
        // A later leg to the pair (a relayed fan-out, say) skips as
        // converged: the belief it reads was written by the flagged leg that
        // may have been lost, so it must not settle the update.
        assert!(
            !p.table.settle_converged(p.key.id(), &p.peer),
            "the skip reads a belief the unconfirmed leg wrote"
        );
        p.arm(2);
        assert!(!p.table.settle_converged(p.key.id(), &p.peer));
        assert!(!p.settled());
    }

    // ── bounds and bookkeeping ────────────────────────────────────────────

    #[test]
    fn coalescing_keeps_one_entry_per_pair() {
        let p = Pair::new();
        assert_eq!(p.arm(0), (ArmOutcome::Armed, true));
        for t in 1..50 {
            assert_eq!(p.arm(t), (ArmOutcome::Coalesced, false));
        }
        assert_eq!(p.table.len(), 1);
    }

    #[test]
    fn the_table_is_capped_and_an_overflow_is_not_tracked() {
        let p = Pair::new();
        let now = p.at(0);
        for port in 0..MAX_PENDING as u16 {
            let peer = PeerKey::from(TransportKeypair::new().public().clone());
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            assert_eq!(p.table.arm(p.key, peer, addr, now).0, ArmOutcome::Armed);
        }
        assert_eq!(p.arm(0), (ArmOutcome::Overflow, false));
        assert_eq!(p.duty(), LegDuty::None);
    }

    #[test]
    fn a_disconnect_discards_that_peers_entries_only() {
        let p = Pair::new();
        let q = Pair::with_peer(PeerKey::from(TransportKeypair::new().public().clone()));
        p.arm(0);
        // `q`'s pair in the SAME table.
        p.table.arm(
            q.key,
            q.peer.clone(),
            SocketAddr::from(([127, 0, 0, 1], 10)),
            p.at(0),
        );
        assert_eq!(p.table.remove_peer(&p.peer), 1);
        assert_eq!(p.table.len(), 1);
        assert!(p.table.leg_duty(q.key.id(), &q.peer).is_flagged());
    }

    #[test]
    fn the_give_up_warn_is_throttled_per_contract() {
        let p = Pair::new();
        let (a, b) = (
            crate::operations::test_utils::make_contract_key(1),
            crate::operations::test_utils::make_contract_key(2),
        );
        assert!(p.table.may_warn(a.id(), p.at(0)));
        assert!(
            !p.table
                .may_warn(a.id(), p.at(GIVE_UP_WARN_INTERVAL.as_secs() - 1))
        );
        assert!(p.table.may_warn(b.id(), p.at(0)));
        assert!(
            p.table
                .may_warn(a.id(), p.at(GIVE_UP_WARN_INTERVAL.as_secs()))
        );
    }
}
