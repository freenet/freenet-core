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
//! ## Mechanism
//!
//! 1. **Arm.** The originated fan-out arms one [`ConfirmedDelivery`] entry per
//!    target whose version decodes the ack-requested variants. One entry per
//!    (contract, peer): a newer originated update re-arms it (coalescing).
//! 2. **Ask.** The entry IS the flag. A leg to an armed peer is sent as
//!    `UpdateMsg::BroadcastToAckRequested` (or the streaming twin), and the
//!    entry records the leg's transaction id and a deadline.
//! 3. **Confirm.** The receiver answers a flagged leg, and only a flagged leg,
//!    with `InterestMessage::BroadcastAck`: applied, already had it, or
//!    dropped with a reason. A matching ack removes the entry.
//! 4. **Resend once.** No ack by the deadline: one full-state resend on the
//!    caller-named stream path (the path that kept working in #6143), flagged
//!    again. No ack to that either: clear the cached belief about the peer, so
//!    later broadcasts and anti-entropy stop skipping it, count, and WARN at
//!    most once per contract per [`GIVE_UP_WARN_INTERVAL`]. A "dropped" ack
//!    means no resend (the receiver would drop it too) and clears the belief.
//!
//! ## Bounds
//!
//! At most [`MAX_PENDING`] entries; an arm past that is counted and not
//! tracked. At most two sends per (originated update, peer). Entries end on an
//! ack, a give-up, the peer disconnecting, or [`UNSENT_TTL`]. One sweep task
//! per node runs only while entries exist, so an idle node pays nothing.
//!
//! The node layer has no per-peer RTT, so the deadline is a fixed
//! [`ACK_TIMEOUT`] from when the leg's send FINISHED (for a stream, from its
//! completion), which already absorbs transfer time.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::{ContractInstanceId, ContractKey};
use parking_lot::Mutex;
use tokio::time::Instant;

use crate::message::{BroadcastAckOutcome, BroadcastDropReason, Transaction};
use crate::ring::PeerKey;

/// How long after a flagged leg's send finished the sender waits for its ack.
///
/// Inside the design's 5-20 s band. Above the floor because the ack is sent
/// after the receiver's merge, which waits in its contract handler queue on a
/// loaded node; a spurious timeout costs one full-state resend.
pub(crate) const ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// Hard cap on pending entries per node. Field volume is tens of originated
/// updates per 15 minutes network-wide, times fan-out (~16), so the cap is
/// reached only by a node flooding its own client writes across many
/// contracts; the overflow is counted.
pub(crate) const MAX_PENDING: usize = 1024;

/// An entry armed or resend-due whose leg never went (queue eviction, a skip,
/// a send refused before the flag was recorded) is resolved after this long.
/// An unsent armed entry gets the full-state resend; an unsent resend gives up.
pub(crate) const UNSENT_TTL: Duration = Duration::from_secs(120);

/// How often the sweep looks for expired deadlines while entries exist.
pub(crate) const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// At most one give-up WARN per contract per this long.
pub(crate) const GIVE_UP_WARN_INTERVAL: Duration = Duration::from_secs(600);

/// Bound on the give-up WARN throttle map.
const MAX_WARN_ENTRIES: usize = 1024;

/// An entry whose flagged legs have all gone unconfirmed for this long since
/// it was first armed is escalated to the resend, even while newer updates
/// keep coalescing into it and pushing each leg's own deadline (#6292 round
/// 1, Must Fix 2). Measured from `first_unconfirmed_at`, which coalescing
/// never moves, and never earlier than [`ACK_TIMEOUT`] after the FIRST
/// flagged leg's send finished, so a long first stream is not cut short.
pub(crate) const ESCALATE_AFTER: Duration = Duration::from_secs(20);

/// Hard ceiling on how long an armed entry may wait while its leg sits in the
/// broadcast queue (#6292 round 1, Must Fix 1). A queued or active leg does
/// not age the [`UNSENT_TTL`] bound; this is the GC bound on that exemption,
/// for a leg the queue dropped without a hook (it is evicted with a hook).
pub(crate) const QUEUED_TTL: Duration = Duration::from_secs(600);

/// What a leg to a peer owes the confirmed-delivery protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegDuty {
    /// Nothing pending for this (contract, peer): an ordinary leg.
    None,
    /// Send it flagged, so the receiver acks it.
    RequestAck,
    /// The first leg went unconfirmed: send the FULL state, on the stream
    /// path, flagged, bypassing the converged-belief skip (that belief is what
    /// this resend distrusts).
    ResendFull,
}

/// Outcome of arming one target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmOutcome {
    /// A new entry.
    Armed,
    /// An entry existed and now also covers this newer update.
    Coalesced,
    /// The table is full; this target is not tracked.
    Overflow,
}

/// Which send a flagged leg was, for counting at the send site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FlaggedSend {
    /// The first flagged leg of this entry.
    First,
    /// A later flagged leg of the same entry (a coalesced newer update, or a
    /// relayed leg to the same pair). Not counted as a new request.
    Reflag,
    /// The one full-state resend.
    Resend,
    /// No entry: the pair settled while the leg was in flight.
    Untracked,
}

/// What finishing a flagged leg's send asks of the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendFinished {
    Nothing,
    /// A final "dropped" ack for this leg beat the send's own bookkeeping,
    /// which has just re-cached our summary as the peer's: clear it again
    /// (#6292 round 1, Should Fix 4).
    ReclearBelief,
}

/// After a "dropped" ack: what happens next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DropNext {
    /// A transient drop of the first leg: the full-state resend follows
    /// after [`ACK_TIMEOUT`]. The belief is left alone; the resend bypasses it.
    Retry,
    /// Stop: the entry is gone and the caller clears its belief.
    Stop,
}

/// What an inbound ack did to the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckEffect {
    /// The peer applied or already held the update; the entry is removed.
    Confirmed { latency: Duration },
    /// An Applied/AlreadyHad ack for the PREVIOUS flagged leg of a coalesced
    /// entry. It does not confirm the newer update, but it proves the path
    /// delivers, so the escalation clock restarts.
    PathProven,
    /// The peer dropped the leg.
    Dropped {
        key: ContractKey,
        reason: BroadcastDropReason,
        next: DropNext,
    },
    /// No entry waits on this transaction id (a superseded leg, a stale ack,
    /// or an ack we never asked for).
    Unmatched,
}

/// One thing the sweep decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SweepAction {
    /// Send one full-state resend to `addr`. `timed_out` is true when an ack
    /// deadline passed, false for a leg that never went or a transient drop.
    Resend {
        key: ContractKey,
        peer: PeerKey,
        addr: SocketAddr,
        timed_out: bool,
    },
    /// The resend went unconfirmed (or never went): stop, clear the belief,
    /// count, maybe WARN.
    GiveUp {
        key: ContractKey,
        peer: PeerKey,
        addr: SocketAddr,
    },
}

/// Whether a drop reason is transient load rather than a property of the
/// payload or the receiver, and so worth the one resend.
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
struct InFlight {
    tx: Transaction,
    sent_at: Instant,
    deadline: Instant,
    /// The send finished: `deadline` is the real ack deadline, not the
    /// provisional bound on the send itself.
    send_done: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    /// Armed by the fan-out; no flagged leg has gone yet. `queued` while the
    /// pair's leg waits in (or runs from) the broadcast queue: then only
    /// [`QUEUED_TTL`] ages it.
    Armed { since: Instant, queued: bool },
    /// A flagged leg went; waiting for its ack.
    AwaitingAck(InFlight),
    /// The first leg was dropped for a transient reason: the resend goes at
    /// `at`.
    RetryAt { at: Instant },
    /// The full-state resend is due.
    ResendDue { since: Instant },
    /// The resend went; waiting for its ack.
    AwaitingResendAck(InFlight),
    /// A final drop for `tx` arrived before that leg's send finished. Held so
    /// `send_finished` can undo the belief its bookkeeping rewrites.
    DroppedBeforeSendDone { tx: Transaction, since: Instant },
}

#[derive(Debug, Clone)]
struct Entry {
    key: ContractKey,
    addr: SocketAddr,
    stage: Stage,
    /// When the oldest unconfirmed update in this entry was armed. Coalescing
    /// never moves it; only a confirmation or a proven path does.
    first_unconfirmed_at: Instant,
    /// When the first flagged leg since `first_unconfirmed_at` finished.
    first_done_at: Option<Instant>,
    /// The previous flagged leg of this entry, so its late ack still counts
    /// as proof the path delivers.
    prev_tx: Option<Transaction>,
    /// Whether any flagged leg has gone for this entry.
    flagged_any: bool,
}

impl Entry {
    fn fresh(key: ContractKey, addr: SocketAddr, now: Instant) -> Self {
        Self {
            key,
            addr,
            stage: Stage::Armed {
                since: now,
                queued: false,
            },
            first_unconfirmed_at: now,
            first_done_at: None,
            prev_tx: None,
            flagged_any: false,
        }
    }

    /// Whether an `AwaitingAck` leg is due for the resend at `now`.
    fn ack_overdue(&self, leg: &InFlight, now: Instant) -> bool {
        if now >= leg.deadline {
            return true;
        }
        if !leg.send_done {
            return false;
        }
        let escalate = self.first_unconfirmed_at + ESCALATE_AFTER;
        let escalate = match self.first_done_at {
            Some(done) => escalate.max(done + ACK_TIMEOUT),
            None => escalate,
        };
        now >= escalate
    }
}

type PairKey = (ContractInstanceId, PeerKey);

#[derive(Default)]
struct Inner {
    entries: HashMap<PairKey, Entry>,
    /// Whether a sweep task is running. Read and written under the same lock
    /// as `entries`, so an arm cannot insert into a table the sweep has just
    /// seen empty and is leaving (the lost-wakeup race).
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

    /// The table's clock. The deadlines are waited on by a tokio sleep, so
    /// production passes the same tokio-backed source as `BroadcastSpacing`.
    pub(crate) fn now(&self) -> Instant {
        self.time_source.now()
    }

    /// Arm `peer` for the originated update of `key` just fanned out.
    ///
    /// A newer update coalesces into an existing entry WITHOUT moving its
    /// stage or its escalation clock: a due or in-flight resend stays due or
    /// in flight, and the next leg (which carries the newest state) is what
    /// the newer update rides on.
    ///
    /// Returns the outcome and whether the caller must start the sweep task.
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
            if matches!(entry.stage, Stage::DroppedBeforeSendDone { .. }) {
                // The previous update was finally dropped; this is a new one.
                *entry = Entry::fresh(key, addr, now);
            }
            ArmOutcome::Coalesced
        } else if inner.entries.len() >= MAX_PENDING {
            return (ArmOutcome::Overflow, false);
        } else {
            inner.entries.insert(pair, Entry::fresh(key, addr, now));
            ArmOutcome::Armed
        };
        let start_sweep = !inner.sweeping;
        inner.sweeping = true;
        (outcome, start_sweep)
    }

    /// The pair's leg was handed to the broadcast queue: while it waits there
    /// an armed entry does not age toward [`UNSENT_TTL`].
    // Called by the production broadcast queue only; the simulation fan-out
    // has no queue.
    #[cfg_attr(feature = "simulation_tests", allow(dead_code))]
    pub(crate) fn note_queued(&self, key: &ContractInstanceId, peer: &PeerKey) {
        let mut inner = self.inner.lock();
        if let Some(Entry {
            stage: Stage::Armed { queued, .. },
            ..
        }) = inner.entries.get_mut(&(*key, peer.clone()))
        {
            *queued = true;
        }
    }

    /// The pair's queued leg left the queue (it started running, or was
    /// evicted): the [`UNSENT_TTL`] bound restarts from `now`.
    pub(crate) fn note_dequeued(&self, key: &ContractInstanceId, peer: &PeerKey, now: Instant) {
        let mut inner = self.inner.lock();
        if let Some(Entry {
            stage: Stage::Armed { since, queued },
            ..
        }) = inner.entries.get_mut(&(*key, peer.clone()))
        {
            if *queued {
                *queued = false;
                *since = now;
            }
        }
    }

    /// What the next leg to `peer` for `key` owes. Reads only.
    pub(crate) fn leg_duty(&self, key: &ContractInstanceId, peer: &PeerKey) -> LegDuty {
        let inner = self.inner.lock();
        match inner.entries.get(&(*key, peer.clone())).map(|e| e.stage) {
            None | Some(Stage::DroppedBeforeSendDone { .. }) => LegDuty::None,
            Some(Stage::ResendDue { .. }) => LegDuty::ResendFull,
            Some(
                Stage::Armed { .. }
                | Stage::AwaitingAck(_)
                | Stage::RetryAt { .. }
                | Stage::AwaitingResendAck(_),
            ) => LegDuty::RequestAck,
        }
    }

    /// A flagged leg with transaction `tx` is about to be sent at `now`.
    ///
    /// Recorded BEFORE the send so an ack that arrives before the sender's
    /// own bookkeeping still matches. `provisional` bounds the send itself
    /// until [`Self::send_finished`] starts the real deadline: [`ACK_TIMEOUT`]
    /// for an inline leg, the stream completion wait plus [`ACK_TIMEOUT`] for
    /// a stream (#6292 round 1, Must Fix 1).
    ///
    /// Only a `full_resend` leg consumes a due resend. A leg that read its
    /// duty before the sweep made the resend due is tracked (its ack still
    /// proves the path) but leaves the resend due (Should Fix 6).
    pub(crate) fn record_flagged_send(
        &self,
        key: &ContractInstanceId,
        peer: &PeerKey,
        tx: Transaction,
        full_resend: bool,
        provisional: Duration,
        now: Instant,
    ) -> FlaggedSend {
        let mut inner = self.inner.lock();
        let Some(entry) = inner.entries.get_mut(&(*key, peer.clone())) else {
            return FlaggedSend::Untracked;
        };
        let leg = InFlight {
            tx,
            sent_at: now,
            deadline: now + provisional,
            send_done: false,
        };
        let first = !entry.flagged_any;
        entry.flagged_any = true;
        match entry.stage {
            Stage::Armed { .. } => {
                entry.stage = Stage::AwaitingAck(leg);
                if first {
                    FlaggedSend::First
                } else {
                    FlaggedSend::Reflag
                }
            }
            Stage::AwaitingAck(previous) => {
                entry.prev_tx = Some(previous.tx);
                entry.stage = Stage::AwaitingAck(leg);
                FlaggedSend::Reflag
            }
            Stage::ResendDue { .. } if full_resend => {
                entry.stage = Stage::AwaitingResendAck(leg);
                FlaggedSend::Resend
            }
            Stage::RetryAt { .. } | Stage::ResendDue { .. } | Stage::AwaitingResendAck(_) => {
                // Not the resend: track it for path proof only.
                entry.prev_tx = Some(tx);
                FlaggedSend::Reflag
            }
            Stage::DroppedBeforeSendDone { .. } => FlaggedSend::Untracked,
        }
    }

    /// The flagged leg `tx` finished sending at `now` (for a stream, its
    /// completion; success or not, since a failed send is what the deadline is
    /// for). Starts the [`ACK_TIMEOUT`] deadline, unless the entry has moved
    /// on. Called AFTER the send's delivery bookkeeping, so a final drop that
    /// beat it can be re-applied.
    pub(crate) fn send_finished(
        &self,
        key: &ContractInstanceId,
        peer: &PeerKey,
        tx: Transaction,
        now: Instant,
    ) -> SendFinished {
        let mut inner = self.inner.lock();
        let pair = (*key, peer.clone());
        let Some(entry) = inner.entries.get_mut(&pair) else {
            return SendFinished::Nothing;
        };
        match &mut entry.stage {
            Stage::AwaitingAck(leg) | Stage::AwaitingResendAck(leg)
                if leg.tx == tx && !leg.send_done =>
            {
                leg.sent_at = now;
                leg.deadline = now + ACK_TIMEOUT;
                leg.send_done = true;
                if entry.first_done_at.is_none() {
                    entry.first_done_at = Some(now);
                }
                SendFinished::Nothing
            }
            Stage::DroppedBeforeSendDone { tx: dropped, .. } if *dropped == tx => {
                inner.entries.remove(&pair);
                SendFinished::ReclearBelief
            }
            Stage::Armed { .. }
            | Stage::AwaitingAck(_)
            | Stage::RetryAt { .. }
            | Stage::ResendDue { .. }
            | Stage::AwaitingResendAck(_)
            | Stage::DroppedBeforeSendDone { .. } => SendFinished::Nothing,
        }
    }

    /// The first leg to `peer` was skipped because our belief says the peer
    /// already holds our state. A just-originated update can only match the
    /// peer's summary if the peer told us so, so an entry NO flagged leg has
    /// touched is settled. Once one went unconfirmed, the belief may be the
    /// very thing that lost leg wrote. Returns whether an entry was removed.
    pub(crate) fn settle_converged(&self, key: &ContractInstanceId, peer: &PeerKey) -> bool {
        let mut inner = self.inner.lock();
        let pair = (*key, peer.clone());
        if matches!(
            inner.entries.get(&pair),
            Some(Entry {
                stage: Stage::Armed { .. },
                flagged_any: false,
                ..
            })
        ) {
            inner.entries.remove(&pair);
            return true;
        }
        false
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
        let Some(entry) = inner.entries.get_mut(&pair) else {
            return AckEffect::Unmatched;
        };
        let (current, is_resend) = match entry.stage {
            Stage::AwaitingAck(leg) => (Some(leg), false),
            Stage::AwaitingResendAck(leg) => (Some(leg), true),
            Stage::Armed { .. }
            | Stage::RetryAt { .. }
            | Stage::ResendDue { .. }
            | Stage::DroppedBeforeSendDone { .. } => (None, false),
        };
        match current {
            Some(leg) if leg.tx == tx => match outcome {
                BroadcastAckOutcome::Applied | BroadcastAckOutcome::AlreadyHad => {
                    inner.entries.remove(&pair);
                    AckEffect::Confirmed {
                        latency: now.saturating_duration_since(leg.sent_at),
                    }
                }
                BroadcastAckOutcome::Dropped(reason) => {
                    let full_key = entry.key;
                    if is_transient(reason) && !is_resend {
                        entry.stage = Stage::RetryAt {
                            at: now + ACK_TIMEOUT,
                        };
                        return AckEffect::Dropped {
                            key: full_key,
                            reason,
                            next: DropNext::Retry,
                        };
                    }
                    if leg.send_done {
                        inner.entries.remove(&pair);
                    } else {
                        entry.stage = Stage::DroppedBeforeSendDone { tx, since: now };
                    }
                    AckEffect::Dropped {
                        key: full_key,
                        reason,
                        next: DropNext::Stop,
                    }
                }
            },
            _ if entry.prev_tx == Some(tx)
                && matches!(
                    outcome,
                    BroadcastAckOutcome::Applied | BroadcastAckOutcome::AlreadyHad
                ) =>
            {
                entry.prev_tx = None;
                entry.first_unconfirmed_at = now;
                entry.first_done_at = None;
                AckEffect::PathProven
            }
            _ => AckEffect::Unmatched,
        }
    }

    /// The peer disconnected: its entries are discarded. Returns how many.
    pub(crate) fn remove_peer(&self, peer: &PeerKey) -> usize {
        let mut inner = self.inner.lock();
        let before = inner.entries.len();
        inner.entries.retain(|(_, p), _| p != peer);
        before - inner.entries.len()
    }

    /// Advance every expired entry. Returns the actions, and whether the
    /// sweep should keep running (false when the table is empty, in which
    /// case the sweep flag is cleared under the same lock).
    pub(crate) fn sweep(&self, now: Instant) -> (Vec<SweepAction>, bool) {
        let mut inner = self.inner.lock();
        let mut actions = Vec::new();
        let mut give_up = Vec::new();
        let mut forget = Vec::new();
        for (pair, entry) in inner.entries.iter_mut() {
            let resend = |entry: &mut Entry, timed_out: bool| {
                entry.stage = Stage::ResendDue { since: now };
                SweepAction::Resend {
                    key: entry.key,
                    peer: pair.1.clone(),
                    addr: entry.addr,
                    timed_out,
                }
            };
            match entry.stage {
                Stage::Armed { since, queued } => {
                    let bound = if queued { QUEUED_TTL } else { UNSENT_TTL };
                    if now.saturating_duration_since(since) >= bound {
                        actions.push(resend(entry, false));
                    }
                }
                Stage::AwaitingAck(leg) => {
                    if entry.ack_overdue(&leg, now) {
                        actions.push(resend(entry, true));
                    }
                }
                Stage::RetryAt { at } => {
                    if now >= at {
                        actions.push(resend(entry, false));
                    }
                }
                Stage::AwaitingResendAck(leg) => {
                    if now >= leg.deadline {
                        give_up.push(pair.clone());
                    }
                }
                Stage::ResendDue { since } => {
                    if now.saturating_duration_since(since) >= UNSENT_TTL {
                        give_up.push(pair.clone());
                    }
                }
                Stage::DroppedBeforeSendDone { since, .. } => {
                    if now.saturating_duration_since(since) >= UNSENT_TTL {
                        forget.push(pair.clone());
                    }
                }
            }
        }
        for pair in give_up {
            if let Some(entry) = inner.entries.remove(&pair) {
                actions.push(SweepAction::GiveUp {
                    key: entry.key,
                    peer: pair.1,
                    addr: entry.addr,
                });
            }
        }
        for pair in forget {
            inner.entries.remove(&pair);
        }
        let keep_running = !inner.entries.is_empty();
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
                // Every slot is inside its interval: stay quiet rather than
                // grow. The give-up is still counted.
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

/// The duty of a leg about to go to `peer_addr`, re-checking the version gate
/// at the send site (#6292 round 1, Should Fix 9). Arming already checked it,
/// but a queued leg can outlive a reconnect at the same key, and the version
/// gate is what keeps undecodable variants off the wire, so it is enforced
/// where the variant is chosen rather than only where the entry was created.
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
        AckEffect::PathProven => {
            count(op_manager, &key, PropagationEvent::AckPathProven);
        }
        AckEffect::Dropped {
            key: full_key,
            reason,
            next,
        } => {
            count(op_manager, &key, dropped_event(reason));
            match next {
                DropNext::Retry => {}
                DropNext::Stop => {
                    clear_belief(op_manager, &full_key, &peer);
                    if is_transient(reason) {
                        // The resend itself was dropped: the repair ends.
                        count(op_manager, &key, PropagationEvent::UnconfirmedGiveup);
                    }
                }
            }
            tracing::debug!(
                contract = %key,
                peer = %sender_addr,
                ?reason,
                ?next,
                "confirmed delivery: peer dropped the originated update"
            );
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
/// is gone), so a later arm can start a new one rather than leaving entries
/// with no sweep at all.
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
                SweepAction::Resend {
                    key,
                    peer,
                    addr,
                    timed_out,
                } => {
                    if timed_out {
                        count(&op_manager, key.id(), PropagationEvent::AckTimeout);
                    }
                    // The resend leg consults the table and finds it
                    // `ResendDue`, so it goes as full state on the stream
                    // path. Routed through the targeted heal, which applies
                    // the per-pair broadcast spacing like any other leg.
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
                        // No local state to resend; the unsent TTL gives up.
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
                        // The event loop is saturated. The entry stays
                        // resend-due and the unsent TTL gives up on it.
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
    use crate::transport::TransportKeypair;

    fn table() -> ConfirmedDelivery {
        ConfirmedDelivery::new(Arc::new(crate::util::time_source::InstantTimeSrc::new()))
    }

    fn key(seed: u8) -> ContractKey {
        crate::operations::test_utils::make_contract_key(seed)
    }

    fn peer() -> PeerKey {
        PeerKey::from(TransportKeypair::new().public().clone())
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn tx() -> Transaction {
        Transaction::new::<crate::operations::update::UpdateMsg>()
    }

    /// Drive one entry to `AwaitingAck` with a finished send at `t0`.
    fn flagged(t: &ConfirmedDelivery, k: ContractKey, p: &PeerKey, t0: Instant) -> Transaction {
        t.arm(k, p.clone(), addr(1), t0);
        let id = tx();
        assert_eq!(
            t.record_flagged_send(k.id(), p, id, false, ACK_TIMEOUT, t0),
            FlaggedSend::First
        );
        t.send_finished(k.id(), p, id, t0);
        id
    }

    #[test]
    fn a_newer_update_coalesces_into_one_entry_per_pair() {
        let t = table();
        let (k, p, now) = (key(1), peer(), Instant::now());
        assert_eq!(t.arm(k, p.clone(), addr(1), now), (ArmOutcome::Armed, true));
        for _ in 0..50 {
            assert_eq!(
                t.arm(k, p.clone(), addr(1), now),
                (ArmOutcome::Coalesced, false),
                "a burst of client writes costs one entry, and the sweep is \
                 started once"
            );
        }
        assert_eq!(t.len(), 1);
        // A different contract or peer is a different entry.
        t.arm(key(2), p.clone(), addr(1), now);
        t.arm(k, peer(), addr(2), now);
        assert_eq!(t.len(), 3);
    }

    /// Round 1 Must Fix 2 changed this contract. Coalescing used to restart
    /// the entry, which let a frequently written contract postpone its resend
    /// forever. Now a newer update keeps the pending leg and its deadline:
    /// the next leg (carrying the newest state) is what the newer update
    /// rides on, and an unconfirmed leg still comes due on time.
    #[test]
    fn coalescing_keeps_the_pending_leg_and_its_deadline() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);
        t.arm(k, p.clone(), addr(1), t0);
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::RequestAck);
        let (actions, _) = t.sweep(t0 + ACK_TIMEOUT);
        assert!(
            matches!(
                actions.as_slice(),
                [SweepAction::Resend {
                    timed_out: true,
                    ..
                }]
            ),
            "{actions:?}"
        );
    }

    #[test]
    fn the_table_is_capped_and_an_overflow_is_not_tracked() {
        let t = table();
        let now = Instant::now();
        let k = key(1);
        for port in 0..MAX_PENDING as u16 {
            assert_eq!(t.arm(k, peer(), addr(port), now).0, ArmOutcome::Armed);
        }
        let extra = peer();
        assert_eq!(
            t.arm(k, extra.clone(), addr(9), now),
            (ArmOutcome::Overflow, false)
        );
        assert_eq!(t.len(), MAX_PENDING);
        assert_eq!(t.leg_duty(k.id(), &extra), LegDuty::None);
    }

    #[test]
    fn a_matching_ack_confirms_and_removes_the_entry() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let id = flagged(&t, k, &p, t0);
        let later = t0 + Duration::from_millis(300);
        assert_eq!(
            t.on_ack(k.id(), &p, id, BroadcastAckOutcome::Applied, later),
            AckEffect::Confirmed {
                latency: Duration::from_millis(300)
            }
        );
        assert_eq!(t.len(), 0);
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::None);
        // "Already had it" confirms too.
        let id = flagged(&t, k, &p, t0);
        assert!(matches!(
            t.on_ack(k.id(), &p, id, BroadcastAckOutcome::AlreadyHad, t0),
            AckEffect::Confirmed { .. }
        ));
    }

    #[test]
    fn an_ack_for_another_leg_or_peer_settles_nothing() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let id = flagged(&t, k, &p, t0);
        // A stale ack for a superseded leg.
        assert_eq!(
            t.on_ack(k.id(), &p, tx(), BroadcastAckOutcome::Applied, t0),
            AckEffect::Unmatched
        );
        // The right tx from the wrong peer, or for the wrong contract.
        assert_eq!(
            t.on_ack(k.id(), &peer(), id, BroadcastAckOutcome::Applied, t0),
            AckEffect::Unmatched
        );
        assert_eq!(
            t.on_ack(key(2).id(), &p, id, BroadcastAckOutcome::Applied, t0),
            AckEffect::Unmatched
        );
        // An ack for an armed entry whose leg has not gone yet.
        let p2 = peer();
        t.arm(k, p2.clone(), addr(2), t0);
        assert_eq!(
            t.on_ack(k.id(), &p2, id, BroadcastAckOutcome::Applied, t0),
            AckEffect::Unmatched
        );
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn an_ack_that_beats_the_send_bookkeeping_still_matches() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        let id = tx();
        t.record_flagged_send(k.id(), &p, id, false, ACK_TIMEOUT, t0);
        assert!(matches!(
            t.on_ack(k.id(), &p, id, BroadcastAckOutcome::Applied, t0),
            AckEffect::Confirmed { .. }
        ));
        // `send_finished` for a settled entry is a no-op, not a resurrection.
        t.send_finished(k.id(), &p, id, t0);
        assert_eq!(t.len(), 0);
    }

    #[test]
    fn a_dropped_ack_removes_the_entry_without_a_resend() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let id = flagged(&t, k, &p, t0);
        assert_eq!(
            t.on_ack(
                k.id(),
                &p,
                id,
                BroadcastAckOutcome::Dropped(BroadcastDropReason::Backoff),
                t0
            ),
            AckEffect::Dropped {
                key: k,
                reason: BroadcastDropReason::Backoff,
                next: DropNext::Stop,
            }
        );
        let (actions, keep_running) = t.sweep(t0 + UNSENT_TTL * 2);
        assert!(actions.is_empty(), "no resend after a dropped ack");
        assert!(!keep_running);
    }

    #[test]
    fn no_ack_resends_once_then_gives_up() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);

        // Not yet due.
        let (actions, keep) = t.sweep(t0 + ACK_TIMEOUT - Duration::from_millis(1));
        assert!(actions.is_empty() && keep);

        // Due: one resend, and the next leg is the full-state resend.
        let t1 = t0 + ACK_TIMEOUT;
        let (actions, keep) = t.sweep(t1);
        assert_eq!(
            actions,
            vec![SweepAction::Resend {
                key: k,
                peer: p.clone(),
                addr: addr(1),
                timed_out: true
            }]
        );
        assert!(keep);
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::ResendFull);
        // Sweeping again before the resend goes does not resend twice.
        assert!(t.sweep(t1 + Duration::from_secs(1)).0.is_empty());

        let resend = tx();
        assert_eq!(
            t.record_flagged_send(k.id(), &p, resend, true, ACK_TIMEOUT, t1),
            FlaggedSend::Resend
        );
        t.send_finished(k.id(), &p, resend, t1);
        // While the resend awaits its ack, another leg to the peer asks for an
        // ack but does not restart the resend.
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::RequestAck);

        let (actions, keep) = t.sweep(t1 + ACK_TIMEOUT);
        assert_eq!(
            actions,
            vec![SweepAction::GiveUp {
                key: k,
                peer: p.clone(),
                addr: addr(1)
            }]
        );
        assert!(!keep, "the table is empty, so the sweep stops");
        assert_eq!(t.len(), 0);
        assert!(!t.is_sweeping());
        // And the next arm starts the sweep again.
        assert!(t.arm(k, p, addr(1), t1).1);
    }

    #[test]
    fn the_resend_is_confirmed_by_its_own_ack() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let first = flagged(&t, k, &p, t0);
        t.sweep(t0 + ACK_TIMEOUT);
        let resend = tx();
        t.record_flagged_send(k.id(), &p, resend, true, ACK_TIMEOUT, t0 + ACK_TIMEOUT);
        // A late ack for the first leg no longer matches.
        assert_eq!(
            t.on_ack(k.id(), &p, first, BroadcastAckOutcome::Applied, t0),
            AckEffect::Unmatched
        );
        assert!(matches!(
            t.on_ack(k.id(), &p, resend, BroadcastAckOutcome::Applied, t0),
            AckEffect::Confirmed { .. }
        ));
    }

    #[test]
    fn a_leg_that_never_went_is_resent_then_given_up_on() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        assert!(
            t.sweep(t0 + UNSENT_TTL - Duration::from_secs(1))
                .0
                .is_empty()
        );
        let t1 = t0 + UNSENT_TTL;
        assert_eq!(
            t.sweep(t1).0,
            vec![SweepAction::Resend {
                key: k,
                peer: p.clone(),
                addr: addr(1),
                timed_out: false
            }]
        );
        // The resend never goes either: give up after the same bound.
        assert_eq!(
            t.sweep(t1 + UNSENT_TTL).0,
            vec![SweepAction::GiveUp {
                key: k,
                peer: p,
                addr: addr(1)
            }]
        );
        assert_eq!(t.len(), 0);
    }

    /// A send that never finishes is still bounded, by its provisional
    /// deadline.
    #[test]
    fn a_send_that_never_finishes_is_bounded() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        t.record_flagged_send(k.id(), &p, tx(), false, ACK_TIMEOUT, t0);
        assert!(
            t.sweep(t0 + ACK_TIMEOUT - Duration::from_millis(1))
                .0
                .is_empty()
        );
        assert_eq!(t.sweep(t0 + ACK_TIMEOUT).0.len(), 1);
    }

    /// Round 1 Must Fix 1: a long stream is not resent mid-transfer. Its
    /// provisional deadline is the stream's completion wait plus the ack
    /// timeout, not the fixed unsent bound.
    #[test]
    fn a_long_stream_is_not_resent_mid_transfer() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        let id = tx();
        let stream_wait = Duration::from_secs(600);
        t.record_flagged_send(k.id(), &p, id, false, stream_wait + ACK_TIMEOUT, t0);
        assert!(
            t.sweep(t0 + UNSENT_TTL * 3).0.is_empty(),
            "the stream is still within its completion wait"
        );
        // The stream completes; only now does the ack deadline run, and the
        // escalation clock does not cut it short.
        let done = t0 + Duration::from_secs(500);
        t.send_finished(k.id(), &p, id, done);
        assert!(
            t.sweep(done + ACK_TIMEOUT - Duration::from_millis(1))
                .0
                .is_empty()
        );
        assert_eq!(t.sweep(done + ACK_TIMEOUT).0.len(), 1);
    }

    /// Round 1 Must Fix 1: an armed entry whose leg waits in the broadcast
    /// queue does not age, bounded by the queued ceiling; once it leaves the
    /// queue the unsent bound runs from then.
    #[test]
    fn a_queued_leg_does_not_age_its_entry() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        t.note_queued(k.id(), &p);
        assert!(t.sweep(t0 + UNSENT_TTL * 2).0.is_empty());
        let t1 = t0 + UNSENT_TTL * 3;
        t.note_dequeued(k.id(), &p, t1);
        assert!(
            t.sweep(t1 + UNSENT_TTL - Duration::from_secs(1))
                .0
                .is_empty()
        );
        assert_eq!(t.sweep(t1 + UNSENT_TTL).0.len(), 1);

        // The ceiling still bounds a leg the queue lost without a hook.
        let q = peer();
        t.arm(k, q.clone(), addr(2), t0);
        t.note_queued(k.id(), &q);
        assert!(
            t.sweep(t0 + QUEUED_TTL)
                .0
                .iter()
                .any(|a| matches!(a, SweepAction::Resend { peer, .. } if *peer == q))
        );
    }

    /// Round 1 Should Fix 6: a leg that read its duty before the resend came
    /// due does not consume it; only the full-state resend does.
    #[test]
    fn only_the_full_resend_consumes_a_due_resend() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);
        t.sweep(t0 + ACK_TIMEOUT);
        assert_eq!(
            t.record_flagged_send(k.id(), &p, tx(), false, ACK_TIMEOUT, t0 + ACK_TIMEOUT),
            FlaggedSend::Reflag
        );
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::ResendFull);
        assert_eq!(
            t.record_flagged_send(k.id(), &p, tx(), true, ACK_TIMEOUT, t0 + ACK_TIMEOUT),
            FlaggedSend::Resend
        );
    }

    /// Round 1 Should Fix 4: a final drop that beats the leg's own send
    /// bookkeeping is re-applied when the send finishes.
    #[test]
    fn a_drop_before_the_send_finishes_reclears_the_belief() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        let id = tx();
        t.record_flagged_send(k.id(), &p, id, false, ACK_TIMEOUT, t0);
        assert!(matches!(
            t.on_ack(
                k.id(),
                &p,
                id,
                BroadcastAckOutcome::Dropped(BroadcastDropReason::NotHosted),
                t0
            ),
            AckEffect::Dropped {
                next: DropNext::Stop,
                ..
            }
        ));
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::None);
        assert_eq!(
            t.send_finished(k.id(), &p, id, t0),
            SendFinished::ReclearBelief
        );
        assert_eq!(t.len(), 0);
    }

    /// Round 1 Must Fix 2: a late Applied ack for the PREVIOUS leg of a
    /// coalesced entry proves the path, restarting the escalation clock,
    /// without confirming the newer update.
    #[test]
    fn a_late_ack_for_the_previous_leg_proves_the_path() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let first = flagged(&t, k, &p, t0);
        let second = tx();
        let t3 = t0 + Duration::from_secs(3);
        t.arm(k, p.clone(), addr(1), t3);
        t.record_flagged_send(k.id(), &p, second, false, ACK_TIMEOUT, t3);
        t.send_finished(k.id(), &p, second, t0 + Duration::from_secs(15));
        assert_eq!(
            t.on_ack(
                k.id(),
                &p,
                first,
                BroadcastAckOutcome::Applied,
                t0 + Duration::from_secs(4)
            ),
            AckEffect::PathProven
        );
        assert_eq!(t.len(), 1, "the newer update is not confirmed by it");
        // Without the proof the escalation would fire at t0 + 20 s.
        assert!(t.sweep(t0 + Duration::from_secs(21)).0.is_empty());
    }

    /// Round 1 Should Fix 5: the transient reasons get the resend, the final
    /// ones do not, and a transient drop of the resend itself ends the repair.
    #[test]
    fn transient_reasons_are_exactly_the_load_ones() {
        assert!(is_transient(BroadcastDropReason::RateLimited));
        assert!(is_transient(BroadcastDropReason::MergeBudget));
        assert!(is_transient(BroadcastDropReason::Overloaded));
        assert!(!is_transient(BroadcastDropReason::NotHosted));
        assert!(!is_transient(BroadcastDropReason::Backoff));
        assert!(!is_transient(BroadcastDropReason::MergeFailed));

        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);
        t.sweep(t0 + ACK_TIMEOUT);
        let resend = tx();
        t.record_flagged_send(k.id(), &p, resend, true, ACK_TIMEOUT, t0 + ACK_TIMEOUT);
        t.send_finished(k.id(), &p, resend, t0 + ACK_TIMEOUT);
        assert!(matches!(
            t.on_ack(
                k.id(),
                &p,
                resend,
                BroadcastAckOutcome::Dropped(BroadcastDropReason::Overloaded),
                t0 + ACK_TIMEOUT
            ),
            AckEffect::Dropped {
                next: DropNext::Stop,
                ..
            }
        ));
        assert_eq!(t.len(), 0, "the one resend was spent");
    }

    #[test]
    fn only_a_just_armed_entry_settles_on_a_converged_skip() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        t.arm(k, p.clone(), addr(1), t0);
        assert!(t.settle_converged(k.id(), &p));
        assert_eq!(t.len(), 0);

        // Once a leg went, a converged belief is exactly what a lost leg
        // leaves behind, so it settles nothing.
        flagged(&t, k, &p, t0);
        assert!(!t.settle_converged(k.id(), &p));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn a_disconnect_discards_that_peers_entries_only() {
        let t = table();
        let (p, q, t0) = (peer(), peer(), Instant::now());
        t.arm(key(1), p.clone(), addr(1), t0);
        t.arm(key(2), p.clone(), addr(1), t0);
        t.arm(key(1), q.clone(), addr(2), t0);
        assert_eq!(t.remove_peer(&p), 2);
        assert_eq!(t.len(), 1);
        assert_eq!(t.leg_duty(key(1).id(), &q), LegDuty::RequestAck);
    }

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

    /// Round-1 Must Fix 2: a newer update must not demote a due resend back to
    /// a first leg. Before the fix `arm` reset every stage to `Armed`, so a
    /// contract written again during the resend window never got its resend.
    #[test]
    fn a_rearm_does_not_demote_a_due_resend() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);
        t.sweep(t0 + ACK_TIMEOUT);
        assert_eq!(t.leg_duty(k.id(), &p), LegDuty::ResendFull);
        t.arm(k, p.clone(), addr(1), t0 + ACK_TIMEOUT);
        assert_eq!(
            t.leg_duty(k.id(), &p),
            LegDuty::ResendFull,
            "re-arming must keep the resend that is already due"
        );
    }

    /// Round-1 Must Fix 2: sustained client writes (one every 3 s, each leg
    /// lost) must still reach the resend. Before the fix every re-arm and
    /// every flagged leg pushed the deadline forward, so the resend never
    /// came while the writes continued.
    #[test]
    fn sustained_writes_still_reach_the_resend() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let mut resent = false;
        for step in 0..20u32 {
            let now = t0 + Duration::from_secs(3) * step;
            if t.leg_duty(k.id(), &p) == LegDuty::ResendFull {
                resent = true;
                break;
            }
            t.arm(k, p.clone(), addr(1), now);
            let id = tx();
            t.record_flagged_send(k.id(), &p, id, false, ACK_TIMEOUT, now);
            t.send_finished(k.id(), &p, id, now);
            t.sweep(now);
        }
        assert!(
            resent,
            "a lossy pair written every 3 s must still be resent to within 60 s"
        );
    }

    /// Round-1 Must Fix 2 (and the converged-skip race): once a flagged leg
    /// has gone unconfirmed, a re-arm followed by a converged skip must not
    /// settle the entry. The lost leg is what made the belief converged.
    #[test]
    fn a_converged_skip_after_an_unconfirmed_leg_settles_nothing() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        flagged(&t, k, &p, t0);
        t.arm(k, p.clone(), addr(1), t0 + Duration::from_secs(1));
        assert!(
            !t.settle_converged(k.id(), &p),
            "the skip reads a belief the unconfirmed leg wrote"
        );
        assert_eq!(t.len(), 1);
    }

    /// Round-1 Should Fix 5: a TRANSIENT drop (rate limit, merge budget,
    /// overload) gets the one full-state resend after the ack timeout rather
    /// than ending the repair.
    #[test]
    fn a_transient_drop_gets_the_resend() {
        let t = table();
        let (k, p, t0) = (key(1), peer(), Instant::now());
        let id = flagged(&t, k, &p, t0);
        t.on_ack(
            k.id(),
            &p,
            id,
            BroadcastAckOutcome::Dropped(BroadcastDropReason::RateLimited),
            t0,
        );
        let (actions, _) = t.sweep(t0 + ACK_TIMEOUT);
        assert!(
            matches!(actions.as_slice(), [SweepAction::Resend { .. }]),
            "a rate-limited drop must be retried once; got {actions:?}"
        );
    }

    /// Round 1 Should Fix 9/10: both send paths that build the ack-requested
    /// variants decide the flag through `leg_duty_for` (which re-checks the
    /// version gate), never through the raw table. Bounded to each function.
    #[test]
    fn every_flagged_send_site_reads_its_duty_through_the_version_gate() {
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
        for (src, sig) in [
            (
                include_str!("../../node/network_bridge/broadcast_queue.rs"),
                "pub(super) async fn broadcast_to_single_peer(",
            ),
            (
                include_str!("../../node/network_bridge/p2p_protoc/broadcast.rs"),
                "async fn broadcast_state_to_peers(",
            ),
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
        }
    }

    #[test]
    fn the_give_up_warn_is_throttled_per_contract() {
        let t = table();
        let t0 = Instant::now();
        let (a, b) = (key(1), key(2));
        assert!(t.may_warn(a.id(), t0));
        assert!(!t.may_warn(a.id(), t0 + GIVE_UP_WARN_INTERVAL - Duration::from_secs(1)));
        assert!(
            t.may_warn(b.id(), t0),
            "another contract has its own budget"
        );
        assert!(t.may_warn(a.id(), t0 + GIVE_UP_WARN_INTERVAL));
    }
}
