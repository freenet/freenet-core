//! Verify-soon check for a node's own originated UPDATEs (#6143).
//!
//! A node records a fan-out leg as delivered when the bridge accepts it, and
//! from then on believes the peer holds that state. In #6143 a node's inline
//! legs were silently lost for days, so its peers stayed behind and nothing
//! noticed. This module asks instead of believing: a few seconds after an
//! originated write it sends the existing `SummaryRequest` for that contract to
//! each fan-out target. The replies go through the existing staleness check and
//! targeted heal in the `Summaries` arm (`node::handle_interest_sync_message`).
//! No new message, no wire change.
//!
//! One check per contract. A check has a round and a phase:
//!
//! | round \ phase | `Scheduled { due }` | `Sent { deadline }` |
//! |---|---|---|
//! | `First` | waits `CHECK_DELAY` after the write | asked every eligible target |
//! | `Confirm` | waits `CONFIRM_DELAY` (or a held peer's retry time) | asked only the peers still stale |
//!
//! A round resolves when every peer asked has answered, or at its deadline.
//! `First` with a stale peer moves to `Confirm`; a `Confirm` with a peer held
//! by something that clears runs again for those peers only; anything else
//! ends the check.
//!
//! A peer still stale in `Confirm` did not get the update from the original
//! legs or from the delta heal `First` triggered. Its heal is then forced to
//! full state on the caller-named stream, the path that kept working in #6143,
//! but only when all of these hold:
//! - the `Confirm` verdict rests on real evidence (`OutcomeEvidence::Verdict`):
//!   a byte-compare fallback (probe unavailable or out of budget) can call a
//!   converged peer stale;
//! - no forced stream to the pair was delivered in the last `FORCE_FLOOR`;
//! - if one was delivered in the last `FORCE_COOLDOWN`, the peer reports a
//!   different summary from the one that force was decided on: the force
//!   landed and the peer moved, so this is a new loss;
//! - no leg for (contract, peer) is queued or sending here: a slow but healthy
//!   peer behind a congested queue is not lost, its leg is waiting;
//! - the node's single forced slot is free: at most ONE forced leg is pending
//!   or in flight on this node at a time, whatever the contract or peer. The
//!   slot is taken when the force is decided and released when the forced
//!   stream's outcome is known, when its mark expires unused (`FORCE_TTL`),
//!   or by the `SLOT_MAX_HOLD` backstop (30 min). Its real longest hold is
//!   `FORCE_TTL` while pending plus the leg's stream completion wait in
//!   flight: at least 270 s, and about 24 min for the largest state.
//!
//! The summary may have changed between the two rounds: other writers keep a
//! shared contract moving, and what matters is that this node's state is still
//! missing at the peer. The floor, the cooldown and the summary they compare
//! against are armed by a DELIVERED forced stream. A failed one arms the floor
//! only (no progress gate: nothing reached the peer), so the same pair cannot
//! take the slot back at once; a mark that is never consumed arms nothing.
//!
//! A write during a check merges into it: the check carries on with its stale
//! and held peers, and a fresh `First` round for the write starts only when the
//! check would otherwise end, and not before the earliest held peer can clear.
//!
//! A hold by the floor, a busy pair, the slot or the pair cap retries the held
//! peers only, at the earliest time any of them can clear, at most
//! `MAX_RETRIES_PER_CHECK` times; when the retries run out while a held peer's
//! clearing time is later still, one final retry runs at that time.
//!
//! Bounds:
//! - later writes never move a scheduled check; they set `rewritten`, which
//!   starts one fresh `First` round when this check would end;
//! - at most `MAX_TRACKED_CONTRACTS` checks; a write beyond that is not checked
//!   (counted);
//! - at most `MAX_PEERS_PER_CHECK` requests per round (a random start when
//!   there are more targets, counted); one `First` round and at most
//!   `MAX_RETRIES_PER_CHECK + 2` `Confirm` rounds per check;
//! - requests leave through a token bucket of `REQUEST_BURST`, refilled at
//!   `REQUEST_BURST` per `REQUEST_REFILL`; rounds that find it empty wait;
//! - forced full-state streams: one at a time per node (the slot); per
//!   (contract, peer) at most one delivered per `FORCE_FLOOR`, and one per
//!   `FORCE_COOLDOWN` for a peer whose summary does not move;
//! - at most `MAX_FORCE_PAIRS` armed pairs remembered (a refusal at the cap is
//!   counted, after expired pairs are pruned); an unconsumed force mark expires
//!   after `FORCE_TTL` and frees the slot.
//!
//! Under a persistent inline fault, a write with one stale peer reaches it
//! within `PERSISTENT_FAULT_BOUND` of the write, plus the forced stream's own
//! transfer time; see that constant for what the figure assumes.
//!
//! Residuals:
//! - a peer that never replies costs one request per round and then nothing;
//!   anti-entropy remains its only repair;
//! - a reply carrying no summary is not a two-sided comparison, so it never
//!   reaches the check: the peer stays awaited until the round's deadline;
//! - a confirm verdict that is not real evidence (`Evidence` hold) is not
//!   retried; a contract whose staleness probe always errors therefore never
//!   escalates and is repaired only by the delta heals and anti-entropy;
//! - a peer that lies with a changing stale summary gets one forced full state
//!   per `FORCE_FLOOR` while this node keeps writing (the progress gate cannot
//!   tell a lie from a real loss);
//! - the slot is shared by every contract: while one large forced stream is in
//!   flight, every other escalation on the node waits, and a peer whose retries
//!   run out while it waits is left to the next write's check or anti-entropy;
//! - a request that the event channel refuses is counted and left to the
//!   round's deadline, not retried;
//! - a forced heal that the `Summaries` arm skips (emit budget, ban, no local
//!   state) holds the slot until its mark expires (`FORCE_TTL`);
//! - a failed forced stream with no later write is not re-checked: the pair
//!   waits for the next write's check or for anti-entropy.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::time::Duration;

use freenet_stdlib::prelude::ContractKey;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::ring::PeerKey;

/// Delay from an originated write to its check. Well above the per-pair
/// broadcast spacing (150 ms), so a healthy leg has landed before we ask, and
/// short enough that a lost update is repaired within seconds. It also
/// coalesces a burst of writes into one check.
pub(crate) const CHECK_DELAY: Duration = Duration::from_secs(5);
/// Delay from a `First` round that found a stale peer to its `Confirm` round:
/// time for the delta heal the first reply triggered to land. Also the least
/// time between two `Confirm` rounds of one check.
pub(crate) const CONFIRM_DELAY: Duration = Duration::from_secs(5);
/// How long a round waits for replies.
pub(crate) const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// Contracts with a check scheduled or in flight.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 1024;
/// Requests per round.
pub(crate) const MAX_PEERS_PER_CHECK: usize = 32;
/// Requests the token bucket holds; also how many it refills per
/// `REQUEST_REFILL`. The node's event channel holds 2048 events, so a full
/// bucket is well under it.
pub(crate) const REQUEST_BURST: usize = 128;
/// The bucket's refill period.
pub(crate) const REQUEST_REFILL: Duration = Duration::from_millis(250);
/// How long a force mark waits for the heal leg it was set for.
pub(crate) const FORCE_TTL: Duration = Duration::from_secs(30);
/// How long a pair whose summary has not moved since its last delivered force
/// stays held. Ten minutes is two anti-entropy heartbeats
/// (`INTEREST_HEARTBEAT_INTERVAL`, 5 min): a peer that never converges costs at
/// most six full states an hour per contract.
pub(crate) const FORCE_COOLDOWN: Duration = Duration::from_secs(600);
/// Minimum time between two delivered forced streams to one (contract, peer),
/// whatever the peer reports. Bounds a peer that lies with a changing summary
/// to about 60 full states an hour per contract, and only while this node
/// writes; under a persistent fault it is how late this node's updates may
/// reach a peer.
pub(crate) const FORCE_FLOOR: Duration = Duration::from_secs(60);
/// The longest the forced slot is held, whatever happens: a pending mark is
/// held at most `FORCE_TTL`, and an in-flight stream until its outcome, which
/// the leg waits for at most `stream_completion_timeout` (at least 270 s, about
/// 24 min for the largest state). This backstop frees it regardless, in case
/// an outcome is never reported.
pub(crate) const SLOT_MAX_HOLD: Duration = Duration::from_secs(30 * 60);
/// Armed (contract, peer) pairs remembered for the floor and the cooldown.
pub(crate) const MAX_FORCE_PAIRS: usize = 1024;
/// Retry `Confirm` rounds one check may run after floor, busy, slot or cap
/// holds, before the final one.
pub(crate) const MAX_RETRIES_PER_CHECK: u8 = 3;
/// How late, at most, a write reaches its ONE stale peer under a persistent
/// inline fault, with the forced slot free when it is needed, not counting the
/// forced stream's own transfer time. From the write: the previous delivered
/// force's floor ends at most `FORCE_FLOOR` later; the retry `Confirm` round
/// runs then and waits up to `REPLY_TIMEOUT` for the reply; the force is
/// decided on that reply. Each slot wait (another forced leg in flight) adds a
/// retry of up to `CONFIRM_DELAY + REPLY_TIMEOUT`, so a simulation with `n`
/// stale peers allows `n - 1` of them.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub const PERSISTENT_FAULT_BOUND: Duration =
    Duration::from_secs(FORCE_FLOOR.as_secs() + REPLY_TIMEOUT.as_secs());
/// What each slot wait adds to [`PERSISTENT_FAULT_BOUND`].
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub const SLOT_WAIT_BOUND: Duration =
    Duration::from_secs(CONFIRM_DELAY.as_secs() + REPLY_TIMEOUT.as_secs());

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Round {
    First,
    Confirm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Scheduled { due: Instant },
    Sent { deadline: Instant },
}

#[derive(Debug)]
struct Check {
    round: Round,
    phase: Phase,
    /// Peers asked in the current `Sent` round that have not answered.
    awaiting: HashSet<PeerKey>,
    /// Peers the next `Confirm` round asks. After `First`: the stale peers,
    /// with no time. After a `Confirm`: the peers held by something that
    /// clears, with the time it can.
    stale: HashMap<PeerKey, Option<Instant>>,
    /// An originated write landed after this check was scheduled.
    rewritten: bool,
    /// Retry `Confirm` rounds this check has run.
    retries: u8,
}

impl Check {
    fn scheduled(due: Instant) -> Self {
        Self {
            round: Round::First,
            phase: Phase::Scheduled { due },
            awaiting: HashSet::new(),
            stale: HashMap::new(),
            rewritten: false,
            retries: 0,
        }
    }
}

/// The node's one forced leg, from decision to outcome.
#[derive(Debug, Clone)]
struct Slot {
    key: ContractKey,
    peer: PeerKey,
    /// The summary the peer reported when the force was decided; armed into
    /// the pair only if the stream is delivered.
    summary_hash: u64,
    /// `Some(expiry)` while the mark waits for its leg; `None` once the leg
    /// took it and the stream is in flight (released by its outcome).
    mark_until: Option<Instant>,
    /// When the force was decided: the `SLOT_MAX_HOLD` backstop runs from
    /// here, and it tells this slot's outcome from a later slot's for the same
    /// pair.
    taken_at: Instant,
}

impl Slot {
    fn live(&self, now: Instant) -> bool {
        self.mark_until.is_none_or(|until| until > now) && now < self.taken_at + SLOT_MAX_HOLD
    }
}

/// What `note_originated` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Originated {
    /// A new check was scheduled.
    Scheduled,
    /// A check already exists; it was left where it is.
    Coalesced,
    /// The table is full; this write is not checked.
    DroppedCap,
}

/// A round to fire now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DueRound {
    pub key: ContractKey,
    pub round: Round,
    /// For `Confirm`, the peers to ask. Empty for `First` (ask every target).
    pub peers: Vec<PeerKey>,
}

/// What [`VerifySoon::take_due`] found.
#[derive(Debug, Default)]
pub(crate) struct Due {
    /// Rounds to fire now.
    pub rounds: Vec<DueRound>,
    /// Contracts whose round ended at its deadline with a peer unanswered.
    pub timed_out: Vec<ContractKey>,
}

/// Why a peer stale in `Confirm` was not forced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Hold {
    /// The verdict was a byte-compare fallback, not real evidence.
    Evidence,
    /// A forced stream to the pair was delivered within `FORCE_FLOOR`.
    Floor,
    /// A forced stream was delivered within `FORCE_COOLDOWN` and the peer
    /// still reports the summary it was decided on.
    NoProgress,
    /// `MAX_FORCE_PAIRS` pairs are armed.
    Cap,
    /// A leg for the pair is queued or sending here.
    Busy,
    /// The node's forced slot is held by another forced leg.
    Slot,
}

/// What a `Summaries` comparison meant to the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Not an answer to a round in flight.
    Ignored,
    Converged,
    /// Stale in `First`.
    Stale,
    /// Stale in `Confirm`: the heal that follows must be full state on the
    /// stream. The force mark (the slot) is already set.
    Escalate,
    /// Stale in `Confirm`, not forced.
    Held(Hold),
}

#[derive(Default)]
struct Inner {
    checks: HashMap<ContractKey, Check>,
    /// Pairs a forced stream armed: when it ended, and, if it was delivered,
    /// the summary it was decided on. A failed stream arms the floor only
    /// (`None`): no progress gate, since nothing reached the peer.
    armed: HashMap<(ContractKey, PeerKey), (Instant, Option<u64>)>,
    /// The node's one forced leg, pending or in flight.
    slot: Option<Slot>,
}

type PendingLegProbe = Box<dyn Fn(&ContractKey, &PeerKey) -> bool + Send + Sync>;

/// Per-node verify-soon table. Owned by `OpManager`; driven by
/// [`run_verify_soon`].
#[derive(Default)]
pub(crate) struct VerifySoon {
    inner: Mutex<Inner>,
    wake: Notify,
    /// Whether the broadcast queue holds or is sending a leg for a pair. Set
    /// by the production event loop, which owns the queue.
    pending_leg: OnceLock<PendingLegProbe>,
    /// The request token bucket (see [`RequestBucket`]).
    bucket: Mutex<Option<RequestBucket>>,
}

impl VerifySoon {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Install the broadcast queue's pending-leg query. First call wins.
    #[cfg_attr(feature = "simulation_tests", allow(dead_code))]
    pub(crate) fn set_pending_leg_probe(&self, probe: PendingLegProbe) {
        if self.pending_leg.set(probe).is_err() {
            tracing::debug!("verify-soon: pending-leg probe already installed");
        }
    }

    fn queue_has_leg(&self, key: &ContractKey, peer: &PeerKey) -> bool {
        self.pending_leg.get().is_some_and(|probe| probe(key, peer))
    }

    /// An originated write to `key` committed locally.
    pub(crate) fn note_originated(&self, key: ContractKey, now: Instant) -> Originated {
        let mut inner = self.inner.lock();
        if let Some(check) = inner.checks.get_mut(&key) {
            check.rewritten = true;
            return Originated::Coalesced;
        }
        if inner.checks.len() >= MAX_TRACKED_CONTRACTS {
            return Originated::DroppedCap;
        }
        inner
            .checks
            .insert(key, Check::scheduled(now + CHECK_DELAY));
        drop(inner);
        self.wake.notify_one();
        Originated::Scheduled
    }

    /// The earliest time something is due, if anything is tracked.
    pub(crate) fn next_wake(&self) -> Option<Instant> {
        let inner = self.inner.lock();
        let checks = inner.checks.values().map(|c| match c.phase {
            Phase::Scheduled { due } => due,
            Phase::Sent { deadline } => deadline,
        });
        let armed = inner
            .armed
            .values()
            .map(|(at, hash)| armed_until(*at, *hash));
        let mark = inner.slot.as_ref().and_then(|s| s.mark_until);
        checks.chain(armed).chain(mark).min()
    }

    /// Resolve every round whose deadline passed, forget expired pairs, free
    /// the slot of an expired mark, and return the rounds due to fire. The
    /// caller must register each fired round with [`Self::record_sent`]
    /// BEFORE sending its requests.
    pub(crate) fn take_due(&self, now: Instant) -> Due {
        let mut inner = self.inner.lock();
        prune(&mut inner, now);
        let mut due = Due::default();
        let mut ended = Vec::new();
        for (key, check) in inner.checks.iter_mut() {
            if let Phase::Sent { deadline } = check.phase {
                if deadline <= now {
                    if !check.awaiting.is_empty() {
                        due.timed_out.push(*key);
                    }
                    if !resolve(check, now) {
                        ended.push(*key);
                        continue;
                    }
                }
            }
            if let Phase::Scheduled { due: at } = check.phase {
                if at <= now {
                    let peers = match check.round {
                        Round::First => Vec::new(),
                        Round::Confirm => check.stale.keys().cloned().collect(),
                    };
                    due.rounds.push(DueRound {
                        key: *key,
                        round: check.round,
                        peers,
                    });
                }
            }
        }
        for key in ended {
            inner.checks.remove(&key);
        }
        due
    }

    /// The round for `key` is being fired at `now` and asks `peers`. Called
    /// before the requests go out, so a fast reply finds the round `Sent`.
    #[cfg(test)]
    pub(crate) fn record_sent(&self, key: &ContractKey, peers: Vec<PeerKey>, now: Instant) {
        self.record_sent_leaving(key, peers, Vec::new(), now);
    }

    /// As [`Self::record_sent`], where `left_out` are eligible peers this
    /// `Confirm` round could not ask (more than `MAX_PEERS_PER_CHECK`): they
    /// stay stale and drive a retry rather than being dropped.
    pub(crate) fn record_sent_leaving(
        &self,
        key: &ContractKey,
        peers: Vec<PeerKey>,
        left_out: Vec<PeerKey>,
        now: Instant,
    ) {
        let mut inner = self.inner.lock();
        let Some(check) = inner.checks.get_mut(key) else {
            return;
        };
        check.awaiting = peers.into_iter().collect();
        if check.round == Round::Confirm {
            // A peer neither asked nor left out is no longer a target, and
            // drives no retry.
            let awaiting = &check.awaiting;
            check
                .stale
                .retain(|peer, _| awaiting.contains(peer) || left_out.contains(peer));
            for peer in &left_out {
                if let Some(at) = check.stale.get_mut(peer) {
                    at.get_or_insert(now);
                }
            }
        }
        check.phase = Phase::Sent {
            deadline: now + REPLY_TIMEOUT,
        };
        // Nobody to ask: resolve now rather than wait out the deadline.
        if check.awaiting.is_empty() && !resolve(check, now) {
            inner.checks.remove(key);
        }
        // No `wake.notify_one()`: only the driver calls this, and it computes
        // its next wake right after.
    }

    /// A two-sided `Summaries` comparison for `key` from `peer`, which
    /// reported `their_summary`. `real_evidence` says whether `stale` rests on
    /// a real verdict rather than a byte-compare fallback. `pair_busy` is
    /// asked only when a force is otherwise due.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_comparison(
        &self,
        key: &ContractKey,
        peer: &PeerKey,
        stale: bool,
        real_evidence: bool,
        their_summary: &[u8],
        pair_busy: impl FnOnce() -> bool,
        now: Instant,
    ) -> Verdict {
        let mut inner = self.inner.lock();
        let Inner {
            checks,
            armed,
            slot,
        } = &mut *inner;
        let Some(check) = checks.get_mut(key) else {
            return Verdict::Ignored;
        };
        let Phase::Sent { deadline } = check.phase else {
            return Verdict::Ignored;
        };
        if !check.awaiting.remove(peer) {
            return Verdict::Ignored;
        }
        let verdict = match (check.round, stale) {
            (Round::First, false) => Verdict::Converged,
            (Round::First, true) => {
                check.stale.insert(peer.clone(), None);
                Verdict::Stale
            }
            (Round::Confirm, false) => {
                check.stale.remove(peer);
                Verdict::Converged
            }
            (Round::Confirm, true) => {
                // Pruned here, not on every comparison: only a stale confirm
                // verdict reads the pairs and the slot.
                prune_pairs(armed, slot, now);
                let pair = (*key, peer.clone());
                let previous = armed.get(&pair).copied();
                // Hashed only here, for a stale confirm verdict.
                let their_hash = summary_hash(their_summary);
                let (verdict, retry_at) = if !real_evidence {
                    (Verdict::Held(Hold::Evidence), None)
                } else if let Some((at, _)) = previous.filter(|(at, _)| now < *at + FORCE_FLOOR) {
                    (Verdict::Held(Hold::Floor), Some(at + FORCE_FLOOR))
                } else if previous.is_some_and(|(_, hash)| hash == Some(their_hash)) {
                    (Verdict::Held(Hold::NoProgress), None)
                } else if previous.is_none() && armed.len() >= MAX_FORCE_PAIRS {
                    (Verdict::Held(Hold::Cap), Some(now))
                } else if slot.is_some() {
                    (Verdict::Held(Hold::Slot), Some(now))
                } else if pair_busy() {
                    (Verdict::Held(Hold::Busy), Some(now))
                } else {
                    *slot = Some(Slot {
                        key: *key,
                        peer: peer.clone(),
                        summary_hash: their_hash,
                        mark_until: Some(now + FORCE_TTL),
                        taken_at: now,
                    });
                    (Verdict::Escalate, None)
                };
                match retry_at {
                    Some(at) => {
                        check.stale.insert(peer.clone(), Some(at));
                    }
                    None => {
                        check.stale.remove(peer);
                    }
                }
                verdict
            }
        };
        // The driver sleeps until this round's deadline. A last reply that
        // resolves the round early may schedule the next one sooner (a
        // `CONFIRM_DELAY` shorter than `REPLY_TIMEOUT`); only then is it woken,
        // so a reply leaves no permit otherwise.
        let mut sooner = false;
        if check.awaiting.is_empty() {
            if resolve(check, now) {
                sooner = matches!(check.phase, Phase::Scheduled { due } if due < deadline);
            } else {
                checks.remove(key);
            }
        }
        drop(inner);
        if sooner {
            self.wake.notify_one();
        }
        verdict
    }

    /// Consume the force mark for (`key`, `peer`), if the slot holds a live
    /// one for that pair: the forced leg is being built and the stream is now
    /// in flight. Its outcome must be reported with [`Self::forced_outcome`];
    /// [`ForcedLeg`] does that on every path. Returns the slot's identity for
    /// that report, or `None` when there is no live mark for the pair.
    pub(crate) fn take_forced(
        &self,
        key: &ContractKey,
        peer: &PeerKey,
        now: Instant,
    ) -> Option<Instant> {
        let mut inner = self.inner.lock();
        match inner.slot.as_mut() {
            Some(slot)
                if slot.key == *key
                    && slot.peer == *peer
                    && slot.mark_until.is_some_and(|until| until > now) =>
            {
                slot.mark_until = None;
                Some(slot.taken_at)
            }
            _ => None,
        }
    }

    /// The forced stream of the slot taken at `taken_at` ended: free the slot.
    /// A delivered stream arms the pair's floor, cooldown and progress gate; a
    /// failed one arms the floor only, so the same pair cannot take the slot
    /// again at once. Returns whether a delivery was recorded.
    pub(crate) fn forced_outcome(&self, taken_at: Instant, delivered: bool, now: Instant) -> bool {
        let mut inner = self.inner.lock();
        let Some(slot) = inner
            .slot
            .take_if(|s| s.taken_at == taken_at && s.mark_until.is_none())
        else {
            return false;
        };
        let hash = delivered.then_some(slot.summary_hash);
        #[cfg(any(test, feature = "testing"))]
        if delivered {
            crate::node::stale_peer_heal_observations::record_forced_delivered(&slot.key);
        }
        inner.armed.insert((slot.key, slot.peer), (now, hash));
        delivered
    }

    /// Whether a live, unconsumed force mark exists for (`key`, `peer`).
    #[cfg(feature = "simulation_tests")]
    pub(crate) fn has_forced(&self, key: &ContractKey, peer: &PeerKey, now: Instant) -> bool {
        self.inner.lock().slot.as_ref().is_some_and(|s| {
            s.key == *key && s.peer == *peer && s.mark_until.is_some_and(|until| until > now)
        })
    }

    /// Take `wanted` request tokens at `now`, all or none.
    fn try_take_tokens(&self, wanted: usize, now: Instant) -> bool {
        let mut bucket = self.bucket.lock();
        bucket
            .get_or_insert_with(|| RequestBucket::full(now))
            .try_take(wanted, now)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.inner.lock().checks.len()
    }
}

/// Forget pairs past their cooldown, and free the slot of a mark that expired
/// before its leg took it.
fn prune(inner: &mut Inner, now: Instant) {
    prune_pairs(&mut inner.armed, &mut inner.slot, now);
}

fn prune_pairs(
    armed: &mut HashMap<(ContractKey, PeerKey), (Instant, Option<u64>)>,
    slot: &mut Option<Slot>,
    now: Instant,
) {
    armed.retain(|_, (at, hash)| armed_until(*at, *hash) > now);
    if slot.as_ref().is_some_and(|s| !s.live(now)) {
        *slot = None;
    }
}

/// How long an armed pair matters: the cooldown after a delivered force, the
/// floor after a failed one.
fn armed_until(at: Instant, hash: Option<u64>) -> Instant {
    at + if hash.is_some() {
        FORCE_COOLDOWN
    } else {
        FORCE_FLOOR
    }
}

/// The node's request budget: a token bucket holding at most `REQUEST_BURST`
/// tokens and refilling continuously at `REQUEST_BURST` per `REQUEST_REFILL`
/// (about 512 requests a second), whatever the driver's wake pattern. A round
/// takes all its tokens or none, so it is never half-sent.
#[derive(Debug, Clone, Copy)]
struct RequestBucket {
    tokens: f64,
    at: Instant,
}

impl RequestBucket {
    fn full(now: Instant) -> Self {
        Self {
            tokens: REQUEST_BURST as f64,
            at: now,
        }
    }

    fn try_take(&mut self, wanted: usize, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        let rate = REQUEST_BURST as f64 / REQUEST_REFILL.as_secs_f64();
        self.tokens = (self.tokens + elapsed * rate).min(REQUEST_BURST as f64);
        self.at = now;
        if self.tokens >= wanted as f64 {
            self.tokens -= wanted as f64;
            true
        } else {
            false
        }
    }
}

/// Choose at most `MAX_PEERS_PER_CHECK` of `candidates`, from a random start
/// when there are more, so every target of a contract is asked with the same
/// chance each round whatever other contracts do. Returns whether any were
/// left out.
fn select_targets<T>(mut candidates: Vec<T>) -> (Vec<T>, Vec<T>) {
    if candidates.len() <= MAX_PEERS_PER_CHECK {
        return (candidates, Vec::new());
    }
    let len = candidates.len();
    candidates.rotate_left(crate::config::GlobalRng::random_range(0..len));
    let left_out = candidates.split_off(MAX_PEERS_PER_CHECK);
    (candidates, left_out)
}

/// Move a resolved round on. Returns `false` when the check has ended and
/// must be removed.
fn resolve(check: &mut Check, now: Instant) -> bool {
    // A peer that did not answer this round is not asked again.
    for peer in check.awaiting.drain() {
        check.stale.remove(&peer);
    }
    let earliest = check.stale.values().flatten().min().copied();
    let latest = check.stale.values().flatten().max().copied();
    // A write during the check merges into it: the check carries on with its
    // stale and held peers, and only when it would end does a fresh `First`
    // round start for the write, not before the earliest held peer can clear.
    // Restarting at once would discard the stale peers, so under a stream of
    // writes faster than `CHECK_DELAY` no `Confirm` round would ever run.
    let end = |check: &mut Check| {
        if !check.rewritten {
            return false;
        }
        let at = earliest.map_or(now + CHECK_DELAY, |t| t.max(now + CHECK_DELAY));
        *check = Check::scheduled(at);
        true
    };
    match check.round {
        Round::First if !check.stale.is_empty() => {
            check.round = Round::Confirm;
            check.phase = Phase::Scheduled {
                due: now + CONFIRM_DELAY,
            };
            true
        }
        Round::First => end(check),
        Round::Confirm => {
            let retry_at = if check.retries < MAX_RETRIES_PER_CHECK {
                earliest
            } else if check.retries == MAX_RETRIES_PER_CHECK {
                // Out of retries: one final round at the latest clearing time,
                // if any held peer clears later than now.
                latest.filter(|at| *at > now)
            } else {
                None
            };
            match retry_at {
                Some(at) => {
                    check.retries += 1;
                    check.phase = Phase::Scheduled {
                        due: at.max(now + CONFIRM_DELAY),
                    };
                    true
                }
                None => end(check),
            }
        }
    }
}

fn summary_hash(bytes: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut h);
    h.finish()
}

/// An originated write to `key` committed locally: schedule its check, and
/// count what happened.
pub(crate) fn note_originated(op_manager: &crate::node::OpManager, key: ContractKey) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    #[cfg(any(test, feature = "testing"))]
    if test_switch::is_disabled(&key) {
        return;
    }
    let event = match op_manager
        .verify_soon
        .note_originated(key, op_manager.ring.time_source.now())
    {
        Originated::Scheduled => Some(PropagationEvent::VerifyScheduled),
        Originated::Coalesced => None,
        Originated::DroppedCap => Some(PropagationEvent::VerifyDroppedCap),
    };
    if let Some(event) = event {
        record_propagation_event(op_manager, key.id(), event);
    }
}

/// Test-only per-contract off switch, so a simulation can measure the same
/// scenario without the check (a control arm). Keyed by contract because
/// simulation tests share one process; each test uses its own contract.
#[cfg(any(test, feature = "testing"))]
pub mod test_switch {
    use freenet_stdlib::prelude::ContractKey;
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    fn disabled() -> std::sync::MutexGuard<'static, HashSet<ContractKey>> {
        static SET: OnceLock<Mutex<HashSet<ContractKey>>> = OnceLock::new();
        SET.get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Originated writes to `key` schedule no check.
    pub fn disable_for(key: ContractKey) {
        disabled().insert(key);
    }

    /// Undo [`disable_for`].
    pub fn enable_for(key: &ContractKey) {
        disabled().remove(key);
    }

    pub(crate) fn is_disabled(key: &ContractKey) -> bool {
        disabled().contains(key)
    }
}

/// Whether this node holds or is sending a leg for (`key`, `peer`). Production
/// asks the broadcast queue; the simulation, which has no queue, asks the
/// spacing claim its inline sends hold.
fn pair_busy(op_manager: &crate::node::OpManager, key: &ContractKey, peer: &PeerKey) -> bool {
    #[cfg(feature = "simulation_tests")]
    if op_manager.broadcast_spacing.is_in_flight(key.id(), peer) {
        return true;
    }
    op_manager.verify_soon.queue_has_leg(key, peer)
}

/// Feed a two-sided `Summaries` comparison into the check for `key`, and count
/// what it decided. Called by the `Summaries` arm before it emits its heal, so
/// an escalation's force mark is in place when that heal's leg is built.
pub(crate) fn on_summary_comparison(
    op_manager: &crate::node::OpManager,
    key: &ContractKey,
    peer: &PeerKey,
    stale: bool,
    evidence: crate::ring::futile_repair::OutcomeEvidence,
    their_summary: &[u8],
) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    let verdict = op_manager.verify_soon.on_comparison(
        key,
        peer,
        stale,
        matches!(
            evidence,
            crate::ring::futile_repair::OutcomeEvidence::Verdict
        ),
        their_summary,
        || pair_busy(op_manager, key, peer),
        op_manager.ring.time_source.now(),
    );
    let events: &[PropagationEvent] = match verdict {
        Verdict::Ignored => &[],
        Verdict::Converged => &[PropagationEvent::VerifyPeerConverged],
        Verdict::Stale => &[PropagationEvent::VerifyPeerStale],
        Verdict::Escalate => &[
            PropagationEvent::VerifyPeerStale,
            PropagationEvent::VerifyPeerStaleConfirm,
            PropagationEvent::VerifyForcedFull,
        ],
        Verdict::Held(hold) => &[
            PropagationEvent::VerifyPeerStale,
            PropagationEvent::VerifyPeerStaleConfirm,
            match hold {
                Hold::Evidence => PropagationEvent::VerifyNotForcedEvidence,
                Hold::Floor => PropagationEvent::VerifyNotForcedFloor,
                Hold::NoProgress => PropagationEvent::VerifyNotForcedNoProgress,
                Hold::Cap => PropagationEvent::VerifyNotForcedCap,
                Hold::Busy => PropagationEvent::VerifyNotForcedBusy,
                Hold::Slot => PropagationEvent::VerifyNotForcedSlot,
            },
        ],
    };
    for ev in events {
        record_propagation_event(op_manager, key.id(), *ev);
    }
    #[cfg(any(test, feature = "testing"))]
    crate::node::stale_peer_heal_observations::record_verify_verdict(key, verdict);
}

/// The targets one due round asks: current fan-out targets that can decode
/// `SummaryRequest` (`supports_hash_first_summaries`), limited to the round's
/// peers for `Confirm`: at most `MAX_PEERS_PER_CHECK` to ask, and the eligible
/// rest.
type RoundTargets = (Vec<(std::net::SocketAddr, PeerKey)>, Vec<PeerKey>);

fn round_targets(op_manager: &crate::node::OpManager, due: &DueRound) -> RoundTargets {
    let eligible: Vec<(std::net::SocketAddr, PeerKey)> = op_manager
        .get_broadcast_targets_update(
            &due.key,
            &crate::ring::broadcast_coverage::BroadcastOrigin::local(),
        )
        .targets
        .into_iter()
        .filter_map(|target| {
            let addr = target.socket_addr()?;
            let peer = PeerKey::from(target.pub_key().clone());
            if due.round == Round::Confirm && !due.peers.contains(&peer) {
                return None;
            }
            op_manager
                .ring
                .connection_manager
                .supports_hash_first_summaries(addr)
                .then_some((addr, peer))
        })
        .collect();
    let (asked, left_out) = select_targets(eligible);
    (asked, left_out.into_iter().map(|(_, peer)| peer).collect())
}

/// Fire one due round: register it, then send its requests.
fn fire_round(
    op_manager: &crate::node::OpManager,
    due: DueRound,
    (asked, left_out): RoundTargets,
    now: Instant,
) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    use crate::message::{InterestMessage, NodeEvent};

    let DueRound { key, round, .. } = due;
    // Counted when the round goes out, not each time a deferral re-plans it.
    if !left_out.is_empty() {
        record_propagation_event(op_manager, key.id(), PropagationEvent::VerifyPeersTruncated);
    }
    // Register first: a reply can arrive before this function returns.
    op_manager.verify_soon.record_sent_leaving(
        &key,
        asked.iter().map(|(_, peer)| peer.clone()).collect(),
        left_out,
        now,
    );
    let hash = crate::ring::interest::contract_hash(&key);
    for (addr, _) in &asked {
        let sent = op_manager.try_notify_node_event(NodeEvent::SendInterestMessage {
            target: *addr,
            message: InterestMessage::SummaryRequest { hashes: vec![hash] },
        });
        let event = if sent.is_ok() {
            #[cfg(any(test, feature = "testing"))]
            crate::node::stale_peer_heal_observations::record_verify_request(&key, *addr);
            PropagationEvent::VerifyRequestSent
        } else {
            // Left to the round's deadline; see the module residuals.
            PropagationEvent::VerifyRequestDropped
        };
        record_propagation_event(op_manager, key.id(), event);
    }
    tracing::debug!(
        contract = %key,
        ?round,
        asked = asked.len(),
        "verify-soon: round fired"
    );
}

/// The driver's per-wake loop, without the node: `prepare` builds a round and
/// says how many requests it needs, `try_take` takes that many tokens (all or
/// none), `dispatch` sends it. Stops at the first round the bucket cannot pay
/// for, leaving it and the rest `Scheduled` for the next wake. Returns whether
/// any round was deferred.
fn drain_due<T>(
    rounds: Vec<DueRound>,
    mut prepare: impl FnMut(&DueRound) -> (T, usize),
    mut try_take: impl FnMut(usize) -> bool,
    mut dispatch: impl FnMut(DueRound, T),
) -> bool {
    for round in rounds {
        let (prepared, needed) = prepare(&round);
        if !try_take(needed) {
            return true;
        }
        dispatch(round, prepared);
    }
    false
}

/// When the driver next wakes, and whether a notification may wake it sooner.
/// Deferred rounds wait `REQUEST_REFILL` for the bucket and ignore
/// notifications; otherwise the next due time, never one already past (a due
/// time this wake did not consume would spin the loop).
fn next_sleep(deferred: bool, next_wake: Option<Instant>, now: Instant) -> (Option<Instant>, bool) {
    if deferred {
        return (Some(now + REQUEST_REFILL), false);
    }
    let next = next_wake.map(|at| if at > now { at } else { now + REQUEST_REFILL });
    (next, true)
}

/// The outcome reporter for one forced leg. Built when the leg takes its mark;
/// [`ForcedLeg::delivered`] reports a delivery, and dropping it unreported, on
/// any other path (send refused, stream failed or timed out, the task dropped),
/// reports a failure. Exactly one report, by construction: `delivered`
/// consumes the reporter before `Drop` can run.
pub(crate) struct ForcedLeg(Option<ForcedLegInner>);

struct ForcedLegInner {
    verify: std::sync::Arc<VerifySoon>,
    time: crate::util::time_source::DynTimeSource,
    taken_at: Instant,
}

impl ForcedLeg {
    pub(crate) fn new(
        verify: std::sync::Arc<VerifySoon>,
        time: crate::util::time_source::DynTimeSource,
        taken_at: Instant,
    ) -> Self {
        Self(Some(ForcedLegInner {
            verify,
            time,
            taken_at,
        }))
    }

    /// The forced stream was delivered. Returns whether the delivery armed the
    /// pair (`false` if the slot had already been freed, e.g. by the backstop).
    pub(crate) fn delivered(mut self) -> bool {
        let Some(leg) = self.0.take() else {
            return false;
        };
        let now = leg.time.now();
        leg.verify.forced_outcome(leg.taken_at, true, now)
    }
}

impl Drop for ForcedLeg {
    fn drop(&mut self) {
        if let Some(leg) = self.0.take() {
            let now = leg.time.now();
            leg.verify.forced_outcome(leg.taken_at, false, now);
        }
    }
}

/// The node's verify-soon driver: sleeps until the next due time (or until
/// woken by a new check), fires due rounds as the request bucket allows, and
/// ends when its `OpManager` is gone. On shutdown it parks rather than
/// returning, so the task monitor does not read a clean exit as a failure.
pub(crate) async fn run_verify_soon(ring: std::sync::Arc<crate::ring::Ring>) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    use crate::ring::OpManagerState;
    let shutdown = ring.shutdown_token();
    loop {
        if shutdown.is_cancelled() {
            std::future::pending::<()>().await;
        }
        let op_manager = match ring.op_manager_state() {
            OpManagerState::Live(op_manager) => op_manager,
            // Attached once and dropped: the node is gone. Park rather than
            // return: this task is registered with `BackgroundTaskMonitor`,
            // which reads any clean return as a fatal exit (#4292, the same
            // convention as `ring.rs`'s connection maintenance).
            OpManagerState::Detached => std::future::pending::<()>().await,
            OpManagerState::NotAttached => {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
        };
        let verify = op_manager.verify_soon.clone();
        let now = ring.time_source.now();
        let due = verify.take_due(now);
        for key in &due.timed_out {
            record_propagation_event(&op_manager, key.id(), PropagationEvent::VerifyRoundTimedOut);
            #[cfg(any(test, feature = "testing"))]
            crate::node::stale_peer_heal_observations::record_verify_timeout(key);
        }
        let deferred = drain_due(
            due.rounds,
            |round| {
                let targets = round_targets(&op_manager, round);
                let needed = targets.0.len();
                (targets, needed)
            },
            |needed| verify.try_take_tokens(needed, now),
            |round, asked| fire_round(&op_manager, round, asked, now),
        );
        let (next, listen) = next_sleep(deferred, verify.next_wake(), now);
        drop(op_manager);
        // Cancellation safety: every arm is a timer, a notification or the
        // shutdown token; dropping the losers discards no work. `Notify`
        // stores a permit, so a check scheduled between `next_wake` and this
        // wait is not missed. While deferred the notification is not
        // listened to: the deferred rounds are already due and wait for the
        // bucket.
        let sleep = async {
            match next {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };
        if listen {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = verify.wake.notified() => {}
                _ = sleep => {}
            }
        } else {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = sleep => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};
    use futures::FutureExt;
    use std::cell::Cell;

    fn key(n: u8) -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([n; 32]), CodeHash::new([n; 32]))
    }

    fn nth_key(n: usize) -> ContractKey {
        let mut id = [0u8; 32];
        id[..8].copy_from_slice(&(n as u64).to_le_bytes());
        ContractKey::from_id_and_code(ContractInstanceId::new(id), CodeHash::new(id))
    }

    fn peer() -> PeerKey {
        PeerKey::from(crate::transport::TransportKeypair::new().public().clone())
    }

    fn idle() -> bool {
        false
    }

    /// Answer the round due at `t` for `k` with one stale reply per peer, in
    /// order, and return the verdicts.
    fn answer_round(
        vs: &VerifySoon,
        k: ContractKey,
        t: Instant,
        replies: &[(&PeerKey, bool, &[u8])],
    ) -> Vec<Verdict> {
        let due = vs.take_due(t).rounds;
        assert_eq!(due.len(), 1, "one round due at {t:?}");
        vs.record_sent(
            &k,
            replies.iter().map(|(p, _, _)| (*p).clone()).collect(),
            t,
        );
        replies
            .iter()
            .map(|(p, busy, summary)| {
                let busy = *busy;
                vs.on_comparison(&k, p, true, true, summary, || busy, t)
            })
            .collect()
    }

    /// From the write at `t0`: a `First` round where `p` is stale, then the
    /// `Confirm` round. Returns its verdict and time.
    fn run_check(
        vs: &VerifySoon,
        k: ContractKey,
        p: &PeerKey,
        t0: Instant,
        busy: bool,
        summary: &[u8],
    ) -> (Verdict, Instant) {
        assert_eq!(vs.note_originated(k, t0), Originated::Scheduled);
        let t1 = t0 + CHECK_DELAY;
        assert_eq!(
            answer_round(vs, k, t1, &[(p, false, &[0])]),
            vec![Verdict::Stale]
        );
        let t2 = t1 + CONFIRM_DELAY;
        (answer_round(vs, k, t2, &[(p, busy, summary)])[0], t2)
    }

    /// As [`run_check`], then the forced leg takes its mark and reports
    /// `delivered`.
    fn force_and_report(
        vs: &VerifySoon,
        k: ContractKey,
        p: &PeerKey,
        t0: Instant,
        summary: &[u8],
        delivered: bool,
    ) -> Instant {
        let (verdict, t2) = run_check(vs, k, p, t0, false, summary);
        assert_eq!(verdict, Verdict::Escalate);
        let taken = vs.take_forced(&k, p, t2).expect("a live mark");
        assert_eq!(vs.forced_outcome(taken, delivered, t2), delivered);
        t2
    }

    #[tokio::test]
    async fn a_later_write_never_postpones_the_check() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        assert_eq!(vs.note_originated(key(1), t0), Originated::Scheduled);
        assert_eq!(
            vs.note_originated(key(1), t0 + Duration::from_secs(4)),
            Originated::Coalesced
        );
        assert_eq!(vs.next_wake(), Some(t0 + CHECK_DELAY));
        assert!(
            vs.take_due(t0 + CHECK_DELAY - Duration::from_millis(1))
                .rounds
                .is_empty()
        );
        assert_eq!(vs.take_due(t0 + CHECK_DELAY).rounds.len(), 1);
        assert_eq!(vs.tracked(), 1, "one check per contract");
    }

    #[tokio::test]
    async fn a_healthy_round_ends_the_check() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let (a, b) = (peer(), peer());
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone(), b.clone()], t1);
        assert_eq!(
            vs.on_comparison(&key(1), &a, false, true, &[0], idle, t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 1, "still waiting for b");
        assert_eq!(
            vs.on_comparison(&key(1), &b, false, true, &[0], idle, t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 0);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, &[0], idle, t1),
            Verdict::Ignored,
            "a comparison outside a round is not an answer"
        );
    }

    #[tokio::test]
    async fn a_round_with_nobody_to_ask_resolves_at_once() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), Vec::new(), t1);
        assert_eq!(
            vs.tracked(),
            0,
            "no target: the check ends without a deadline"
        );
    }

    #[tokio::test]
    async fn a_reply_before_the_round_is_registered_is_ignored() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, &[0], idle, t1),
            Verdict::Ignored
        );
    }

    #[tokio::test]
    async fn confirm_asks_only_the_stale_peers_and_forces_whatever_their_summary() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let (a, b, c) = (peer(), peer(), peer());
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone(), b.clone(), c.clone()], t1);
        vs.on_comparison(&key(1), &a, true, true, &[1], idle, t1);
        vs.on_comparison(&key(1), &b, true, false, &[1], idle, t1);
        assert_eq!(
            vs.on_comparison(&key(1), &c, false, true, &[1], idle, t1),
            Verdict::Converged
        );
        let t2 = t1 + CONFIRM_DELAY;
        let due = vs.take_due(t2).rounds;
        let mut asked = due[0].peers.clone();
        asked.sort_by_key(|p| format!("{p:?}"));
        let mut expected = vec![a.clone(), b.clone()];
        expected.sort_by_key(|p| format!("{p:?}"));
        assert_eq!(asked, expected, "Confirm asks exactly the stale peers");
        vs.record_sent(&key(1), vec![a.clone(), b.clone()], t2);
        // Different bytes from First's: other writers keep a shared contract
        // moving, and the fault persists.
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, &[2], idle, t2),
            Verdict::Escalate
        );
        assert_eq!(
            vs.on_comparison(&key(1), &b, false, true, &[2], idle, t2),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 0, "Confirm ends the check");
        assert!(vs.take_forced(&key(1), &a, t2).is_some());
        assert!(
            vs.take_forced(&key(1), &a, t2).is_none(),
            "the mark is consumed once"
        );
        assert!(vs.take_forced(&key(1), &b, t2).is_none());
    }

    #[tokio::test]
    async fn a_fallback_verdict_is_not_forced_nor_retried() {
        let vs = VerifySoon::new();
        let a = peer();
        let asked = Cell::new(false);
        let t0 = Instant::now();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        answer_round(&vs, key(1), t1, &[(&a, false, &[0])]);
        let t2 = t1 + CONFIRM_DELAY;
        vs.take_due(t2);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        let verdict = vs.on_comparison(
            &key(1),
            &a,
            true,
            false,
            &[0],
            || {
                asked.set(true);
                false
            },
            t2,
        );
        assert_eq!(verdict, Verdict::Held(Hold::Evidence));
        assert!(vs.take_forced(&key(1), &a, t2).is_none());
        assert!(
            !asked.get(),
            "the queue is not asked once the evidence fails"
        );
        assert_eq!(vs.tracked(), 0, "an evidence hold is not retried");
    }

    #[tokio::test]
    async fn one_forced_leg_at_a_time_per_node() {
        let vs = VerifySoon::new();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        let (verdict, t2) = run_check(&vs, key(1), &a, t0, false, &[0]);
        assert_eq!(verdict, Verdict::Escalate);
        // Another contract, another peer: the slot is taken.
        let (verdict, _) = run_check(&vs, key(2), &b, t0, false, &[0]);
        assert_eq!(verdict, Verdict::Held(Hold::Slot));
        // Still taken while the stream is in flight.
        let taken = vs.take_forced(&key(1), &a, t2).expect("a live mark");
        let t3 = t2 + CONFIRM_DELAY;
        assert_eq!(
            answer_round(&vs, key(2), t3, &[(&b, false, &[0])]),
            vec![Verdict::Held(Hold::Slot)]
        );
        // Freed by the outcome; the held peer's retry then forces it.
        vs.forced_outcome(taken, true, t3);
        let t4 = t3 + CONFIRM_DELAY;
        assert_eq!(
            answer_round(&vs, key(2), t4, &[(&b, false, &[0])]),
            vec![Verdict::Escalate]
        );
    }

    #[tokio::test]
    async fn an_unused_mark_frees_the_slot_and_arms_nothing() {
        // E.g. the forced heal was skipped by the emit budget or its event was
        // dropped: no leg was built.
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let (verdict, t2) = run_check(&vs, key(1), &a, t0, false, &[7]);
        assert_eq!(verdict, Verdict::Escalate);
        assert_eq!(vs.next_wake(), Some(t2 + FORCE_TTL));
        let late = t2 + FORCE_TTL;
        assert!(
            vs.take_forced(&key(1), &a, late).is_none(),
            "the mark expired"
        );
        vs.take_due(late);
        assert_eq!(vs.next_wake(), None, "slot free, nothing armed");
        // No floor and no progress gate: the same summary is forced again.
        assert_eq!(
            run_check(&vs, key(1), &a, late, false, &[7]).0,
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn a_failed_forced_stream_arms_the_floor_only() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let failed = force_and_report(&vs, key(1), &a, t0, &[7], false);
        assert_eq!(vs.next_wake(), Some(failed + FORCE_FLOOR), "the floor only");
        // Within the floor the pair cannot take the slot back.
        let (verdict, _) = run_check(&vs, key(1), &a, failed, false, &[7]);
        assert_eq!(verdict, Verdict::Held(Hold::Floor));
        vs.inner.lock().checks.clear();
        // Past it, the same summary is forced again: no progress gate, since
        // nothing reached the peer.
        let after = failed + FORCE_FLOOR;
        assert_eq!(
            run_check(&vs, key(1), &a, after, false, &[7]).0,
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn a_delivered_force_arms_the_floor_and_the_progress_gate() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let sent = force_and_report(&vs, key(1), &a, t0, &[7], true);
        // Within the floor: held, whatever the summary.
        let t1 = sent + Duration::from_secs(10);
        assert_eq!(
            run_check(&vs, key(1), &a, t1, false, &[8]).0,
            Verdict::Held(Hold::Floor)
        );
        assert_eq!(vs.tracked(), 1, "the floor schedules a retry");
        vs.inner.lock().checks.clear();
        // Past the floor, same summary as the delivered force: no progress.
        let t2 = sent + FORCE_FLOOR;
        assert_eq!(
            run_check(&vs, key(1), &a, t2, false, &[7]).0,
            Verdict::Held(Hold::NoProgress)
        );
        assert_eq!(vs.tracked(), 0, "a no-progress hold is not retried");
        // Past the floor, the peer moved: forced again.
        let t3 = t2 + Duration::from_secs(20);
        assert_eq!(
            run_check(&vs, key(1), &a, t3, false, &[8]).0,
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn the_progress_gate_lasts_the_cooldown() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let sent = force_and_report(&vs, key(1), &a, t0, &[7], true);
        let mut t = sent + FORCE_FLOOR;
        while t + CHECK_DELAY + CONFIRM_DELAY < sent + FORCE_COOLDOWN {
            assert_eq!(
                run_check(&vs, key(1), &a, t, false, &[7]).0,
                Verdict::Held(Hold::NoProgress)
            );
            t += Duration::from_secs(70);
        }
        let after = sent + FORCE_COOLDOWN;
        assert_eq!(
            run_check(&vs, key(1), &a, after, false, &[7]).0,
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn a_busy_pair_is_retried_asking_only_the_held_peers() {
        let vs = VerifySoon::new();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        answer_round(&vs, key(1), t1, &[(&a, false, &[0]), (&b, false, &[0])]);
        let t2 = t1 + CONFIRM_DELAY;
        let v = answer_round(&vs, key(1), t2, &[(&a, true, &[0]), (&b, false, &[0])]);
        assert_eq!(v, vec![Verdict::Held(Hold::Busy), Verdict::Escalate]);
        let t3 = t2 + CONFIRM_DELAY;
        let due = vs.take_due(t3).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::Confirm, "a retry skips First");
        assert_eq!(due[0].peers, vec![a.clone()], "and asks only the held peer");
    }

    #[tokio::test]
    async fn retries_are_bounded_and_the_last_waits_for_the_latest_holder() {
        let vs = VerifySoon::new();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        // a was forced (delivered) at `sent`: its floor clears at sent + 60 s.
        let sent = force_and_report(&vs, key(1), &a, t0, &[1], true);
        // A new write: a is floor-held, b is busy every round.
        let t1 = sent + Duration::from_secs(1);
        vs.note_originated(key(1), t1);
        let mut t = t1 + CHECK_DELAY;
        answer_round(&vs, key(1), t, &[(&a, false, &[2]), (&b, false, &[2])]);
        t += CONFIRM_DELAY;
        let v = answer_round(&vs, key(1), t, &[(&a, false, &[2]), (&b, true, &[2])]);
        assert_eq!(
            v,
            vec![Verdict::Held(Hold::Floor), Verdict::Held(Hold::Busy)]
        );
        // Each retry waits for the EARLIEST holder (b, busy): CONFIRM_DELAY.
        for _ in 0..MAX_RETRIES_PER_CHECK {
            t += CONFIRM_DELAY;
            assert_eq!(vs.next_wake(), Some(t));
            let v = answer_round(&vs, key(1), t, &[(&a, false, &[2]), (&b, true, &[2])]);
            assert_eq!(
                v,
                vec![Verdict::Held(Hold::Floor), Verdict::Held(Hold::Busy)]
            );
        }
        // Out of retries: one final round when a's floor clears, not before.
        assert_eq!(vs.next_wake(), Some(sent + FORCE_FLOOR));
        let v = answer_round(
            &vs,
            key(1),
            sent + FORCE_FLOOR,
            &[(&a, false, &[2]), (&b, true, &[2])],
        );
        assert_eq!(v, vec![Verdict::Escalate, Verdict::Held(Hold::Slot)]);
        assert_eq!(vs.tracked(), 0, "nothing after the final round");
    }

    #[tokio::test]
    async fn armed_pairs_are_capped_after_pruning() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let mut armed_at = t0;
        for n in 0..MAX_FORCE_PAIRS {
            armed_at = force_and_report(&vs, nth_key(n), &a, t0, &[0], true);
        }
        let (verdict, _) = run_check(&vs, nth_key(MAX_FORCE_PAIRS), &a, t0, false, &[0]);
        assert_eq!(verdict, Verdict::Held(Hold::Cap));
        assert_eq!(vs.tracked(), 1, "a cap hold schedules a retry");
        vs.inner.lock().checks.clear();
        // The armed pairs expire between firing a Confirm round and its reply:
        // they are pruned before the cap test on the reply itself.
        let k = nth_key(MAX_FORCE_PAIRS);
        let fire = armed_at + FORCE_COOLDOWN - Duration::from_secs(1);
        let write = fire - CHECK_DELAY - CONFIRM_DELAY;
        vs.note_originated(k, write);
        answer_round(&vs, k, write + CHECK_DELAY, &[(&a, false, &[0])]);
        assert_eq!(vs.take_due(fire).rounds.len(), 1);
        vs.record_sent(&k, vec![a.clone()], fire);
        let reply = fire + Duration::from_secs(2);
        assert_eq!(
            vs.on_comparison(&k, &a, true, true, &[0], idle, reply),
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn the_forced_leg_guard_frees_the_slot_on_every_path() {
        let vs = std::sync::Arc::new(VerifySoon::new());
        let time: crate::util::time_source::DynTimeSource =
            std::sync::Arc::new(crate::util::time_source::InstantTimeSrc::new());
        let a = peer();
        let t0 = time.now();
        // Dropped without delivery: slot freed, nothing armed.
        let (verdict, t2) = run_check(&vs, key(1), &a, t0, false, &[3]);
        assert_eq!(verdict, Verdict::Escalate);
        let taken = vs.take_forced(&key(1), &a, t2).expect("a live mark");
        drop(ForcedLeg::new(vs.clone(), time.clone(), taken));
        assert!(vs.inner.lock().slot.is_none());
        assert_eq!(
            vs.inner
                .lock()
                .armed
                .get(&(key(1), a.clone()))
                .map(|(_, h)| *h),
            Some(None),
            "a failure arms the floor only"
        );
        vs.inner.lock().armed.clear();
        // Delivered: slot freed and the pair armed.
        let (verdict, t3) = run_check(&vs, key(1), &a, t2, false, &[3]);
        assert_eq!(verdict, Verdict::Escalate);
        let taken = vs.take_forced(&key(1), &a, t3).expect("a live mark");
        assert!(ForcedLeg::new(vs.clone(), time.clone(), taken).delivered());
        assert!(vs.inner.lock().slot.is_none());
        assert!(
            vs.inner
                .lock()
                .armed
                .get(&(key(1), a.clone()))
                .is_some_and(|(_, h)| h.is_some()),
            "a delivery arms the progress gate too"
        );
    }

    #[tokio::test]
    async fn a_rewrite_waits_for_a_held_peer() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let sent = force_and_report(&vs, key(1), &a, t0, &[1], true);
        let t1 = sent + Duration::from_secs(1);
        vs.note_originated(key(1), t1);
        let t2 = t1 + CHECK_DELAY;
        answer_round(&vs, key(1), t2, &[(&a, false, &[2])]);
        // A write during the check, then a floor hold: the check carries on
        // with its held peer at the floor's end, as a Confirm.
        vs.note_originated(key(1), t2);
        let t3 = t2 + CONFIRM_DELAY;
        assert_eq!(
            answer_round(&vs, key(1), t3, &[(&a, false, &[2])]),
            vec![Verdict::Held(Hold::Floor)]
        );
        assert_eq!(vs.next_wake(), Some(sent + FORCE_FLOOR));
        let due = vs.take_due(sent + FORCE_FLOOR).rounds;
        assert_eq!(due[0].round, Round::Confirm);
    }

    #[tokio::test]
    async fn a_write_merges_into_the_check_instead_of_restarting_it() {
        let vs = VerifySoon::new();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone(), b.clone()], t1);
        // Writes keep arriving while the round is out.
        vs.note_originated(key(1), t1);
        vs.on_comparison(&key(1), &a, true, true, &[0], idle, t1);
        vs.on_comparison(&key(1), &b, false, true, &[0], idle, t1);
        // The stale peer is confirmed, not forgotten by a restart.
        let t2 = t1 + CONFIRM_DELAY;
        let due = vs.take_due(t2).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::Confirm);
        assert_eq!(due[0].peers, vec![a.clone()]);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, &[0], idle, t2),
            Verdict::Escalate
        );
        // Then the write gets its own First round over everyone.
        assert_eq!(
            vs.next_wake(),
            Some(t2 + CHECK_DELAY).min(Some(t2 + FORCE_TTL))
        );
        let due = vs.take_due(t2 + CHECK_DELAY).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::First);
    }

    #[tokio::test]
    async fn the_slot_backstop_frees_a_stream_that_never_reports() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        let (verdict, t2) = run_check(&vs, key(1), &a, t0, false, &[0]);
        assert_eq!(verdict, Verdict::Escalate);
        let stale_leg = vs.take_forced(&key(1), &a, t2).expect("a live mark");
        // In flight, never reported: held until the backstop, no longer.
        let late = t2 + SLOT_MAX_HOLD;
        vs.take_due(late);
        assert!(vs.inner.lock().slot.is_none(), "the backstop freed it");
        // A later slot for the same pair is not freed by the old leg's late
        // report.
        let (verdict, t4) = run_check(&vs, key(1), &a, late, false, &[0]);
        assert_eq!(verdict, Verdict::Escalate);
        let fresh = vs.take_forced(&key(1), &a, t4).expect("a live mark");
        assert!(!vs.forced_outcome(stale_leg, true, t4));
        assert!(vs.inner.lock().slot.is_some(), "the fresh slot survives");
        assert!(vs.forced_outcome(fresh, true, t4));
    }

    #[tokio::test]
    async fn peers_a_confirm_round_could_not_ask_are_asked_next() {
        let vs = VerifySoon::new();
        let (a, b) = (peer(), peer());
        let t0 = Instant::now();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        answer_round(&vs, key(1), t1, &[(&a, false, &[0]), (&b, false, &[0])]);
        let t2 = t1 + CONFIRM_DELAY;
        vs.take_due(t2);
        // Only a fits in this round; b is left out.
        vs.record_sent_leaving(&key(1), vec![a.clone()], vec![b.clone()], t2);
        assert_eq!(
            vs.on_comparison(&key(1), &a, false, true, &[0], idle, t2),
            Verdict::Converged
        );
        let due = vs.take_due(t2 + CONFIRM_DELAY).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].peers, vec![b.clone()], "b is asked, not dropped");
    }

    #[tokio::test]
    async fn a_round_with_no_reply_resolves_at_its_deadline() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone()], t1);
        assert_eq!(vs.next_wake(), Some(t1 + REPLY_TIMEOUT));
        let due = vs.take_due(t1 + REPLY_TIMEOUT);
        assert!(due.rounds.is_empty());
        assert_eq!(due.timed_out, vec![key(1)]);
        assert_eq!(vs.tracked(), 0, "a silent peer ends the check, no retry");
    }

    #[tokio::test]
    async fn a_write_during_a_check_starts_exactly_one_more() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone()], t1);
        vs.note_originated(key(1), t1);
        vs.note_originated(key(1), t1);
        vs.on_comparison(&key(1), &a, false, true, &[0], idle, t1);
        assert_eq!(vs.next_wake(), Some(t1 + CHECK_DELAY));
        let t2 = t1 + CHECK_DELAY;
        assert_eq!(vs.take_due(t2).rounds.len(), 1);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        vs.on_comparison(&key(1), &a, false, true, &[0], idle, t2);
        assert_eq!(vs.tracked(), 0, "no write since: no further check");
    }

    #[tokio::test]
    async fn the_table_is_capped() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        for n in 0..MAX_TRACKED_CONTRACTS {
            assert_eq!(vs.note_originated(nth_key(n), t0), Originated::Scheduled);
        }
        assert_eq!(
            vs.note_originated(nth_key(MAX_TRACKED_CONTRACTS), t0),
            Originated::DroppedCap
        );
    }

    #[test]
    fn truncation_asks_every_target_over_a_few_rounds() {
        let all: Vec<usize> = (0..MAX_PEERS_PER_CHECK + 8).collect();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..50 {
            let (asked, left_out) = select_targets(all.clone());
            assert_eq!(asked.len() + left_out.len(), all.len());
            assert_eq!(asked.len(), MAX_PEERS_PER_CHECK);
            seen.extend(asked);
        }
        assert_eq!(seen.len(), all.len(), "no target is left out for good");
        let few: Vec<usize> = (0..3).collect();
        assert_eq!(select_targets(few.clone()), (few, Vec::new()));
    }

    #[tokio::test]
    async fn the_request_bucket_refills_over_time_not_per_wake() {
        let t0 = Instant::now();
        let mut bucket = RequestBucket::full(t0);
        for _ in 0..REQUEST_BURST / MAX_PEERS_PER_CHECK {
            assert!(bucket.try_take(MAX_PEERS_PER_CHECK, t0));
        }
        assert!(!bucket.try_take(1, t0), "empty after one burst");
        assert!(
            !bucket.try_take(MAX_PEERS_PER_CHECK, t0 + REQUEST_REFILL / 8),
            "an eighth of the period refills 16, not 32"
        );
        assert!(bucket.try_take(MAX_PEERS_PER_CHECK, t0 + REQUEST_REFILL / 4));
        assert!(
            !bucket.try_take(REQUEST_BURST + 1, t0 + REQUEST_REFILL * 100),
            "never more than one burst"
        );
    }

    #[tokio::test]
    async fn drain_due_stops_at_the_first_round_it_cannot_pay_for() {
        let t0 = Instant::now();
        let mut bucket = RequestBucket::full(t0);
        let rounds: Vec<DueRound> = (0..5)
            .map(|n| DueRound {
                key: nth_key(n),
                round: Round::First,
                peers: Vec::new(),
            })
            .collect();
        let mut sent = Vec::new();
        let deferred = drain_due(
            rounds,
            |round| (round.key, MAX_PEERS_PER_CHECK),
            |n| bucket.try_take(n, t0),
            |_, key| sent.push(key),
        );
        assert!(deferred);
        assert_eq!(
            sent.len(),
            REQUEST_BURST / MAX_PEERS_PER_CHECK,
            "4 x 32 = 128"
        );
        let now = Instant::now();
        assert_eq!(
            next_sleep(true, Some(now), now),
            (Some(now + REQUEST_REFILL), false),
            "deferred rounds wait for the bucket and ignore notifications"
        );
        assert_eq!(
            next_sleep(false, Some(now + CHECK_DELAY), now),
            (Some(now + CHECK_DELAY), true)
        );
        assert_eq!(
            next_sleep(false, Some(now), now),
            (Some(now + REQUEST_REFILL), true),
            "never sleep until a time already past"
        );
    }

    #[tokio::test]
    async fn no_wake_permit_from_the_driver_or_replies() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        assert!(
            vs.wake.notified().now_or_never().is_some(),
            "a new check wakes"
        );
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone()], t1);
        vs.on_comparison(&key(1), &a, true, true, &[0], idle, t1);
        assert!(
            vs.wake.notified().now_or_never().is_none(),
            "a permit here would end the driver's deferral at once"
        );
    }

    /// The body of `fn name` in `src`: from its signature to the first line
    /// that closes a top-level item.
    fn fn_body<'a>(src: &'a str, signature: &str) -> &'a str {
        let start = src
            .find(signature)
            .unwrap_or_else(|| panic!("{signature} not found"));
        let rest = &src[start..];
        let end = rest.find("\n}\n").expect("end of the function");
        &rest[..end]
    }

    #[test]
    fn a_round_is_registered_before_its_requests_are_sent() {
        let src = include_str!("verify_soon.rs");
        let body = fn_body(src, "\nfn fire_round(");
        let registered = body
            .find(".record_sent_leaving(")
            .expect("fire_round registers the round");
        let sent = body
            .find("try_notify_node_event(")
            .expect("fire_round sends requests");
        assert!(
            registered < sent,
            "a reply can arrive before fire_round returns; register first"
        );
    }

    #[test]
    fn broadcast_to_single_peer_honours_the_force_mark() {
        let src = include_str!("../../node/network_bridge/broadcast_queue.rs");
        let body = fn_body(src, "\npub(super) async fn broadcast_to_single_peer(");
        let take = body
            .find(".take_forced(&key, &peer_key")
            .expect("the leg consults the force mark");
        let bind = body
            .find("let forced_slot =")
            .expect("the forced_slot binding");
        let bound_to: String = body[bind + "let forced_slot =".len()..take]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert_eq!(
            bound_to, "op_manager.verify_soon",
            "forced_slot is exactly the force mark, nothing else"
        );
        assert!(
            body.contains("let forced_full = forced_slot.is_some();"),
            "forced_full is exactly whether the mark was taken"
        );
        let skip = body
            .find("fanout_send_needed(")
            .expect("the converged skip");
        assert!(take < skip, "the mark is read before the converged skip");
        assert!(
            body.contains("(false, Some(ours), Some(theirs)) = (forced_full,"),
            "a forced leg bypasses the converged skip"
        );
        assert!(
            body.contains("_ if forced_full =>"),
            "a forced leg is full state"
        );
        assert!(
            body.contains("&& (forced_full\n"),
            "a forced leg always streams"
        );
        let guard = body
            .find("ForcedLeg::new(")
            .expect("a forced leg owns its outcome reporter");
        assert!(take < guard, "built once the mark is taken");
        // Only a delivered stream arms the pair: the one `forced_leg.take()`
        // in the body is the first statement inside `if delivered {`, right
        // after the delivery is recorded, so a failed stream drops the
        // reporter unreported (a failure).
        let report = "if forced_leg.take().is_some_and(|leg| leg.delivered()) {";
        assert_eq!(
            body.matches("forced_leg.take()").count(),
            1,
            "exactly one place reports the forced leg"
        );
        let at = body
            .find(report)
            .expect("a delivered forced stream arms the pair");
        let gate = "BROADCAST_STREAM_METRICS.record_attempt(delivered);";
        let gate_at = body.find(gate).expect("the delivery outcome is recorded");
        let between: String = body[gate_at + gate.len()..at]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        assert_eq!(
            between, "ifdelivered{",
            "the report is the first statement inside `if delivered {`"
        );
    }
}
