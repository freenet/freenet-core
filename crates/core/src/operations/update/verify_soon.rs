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
//! | `Confirm` | waits `CONFIRM_DELAY` after `First` | asked only the peers `First` found stale |
//!
//! A round resolves when every peer asked has answered, or at its deadline.
//! `First` with a stale peer moves to `Confirm`; anything else ends the check.
//!
//! A peer still stale in `Confirm` did not get the update from the original
//! legs or from the delta heal `First` triggered. Its heal is then forced to
//! full state on the caller-named stream, the path that kept working in #6143,
//! but only when all of these hold:
//! - the `Confirm` verdict rests on real evidence (`OutcomeEvidence::Verdict`):
//!   a byte-compare fallback (probe unavailable or out of budget) can call a
//!   converged peer stale;
//! - no leg for (contract, peer) is queued or sending here: a slow but healthy
//!   peer behind a congested queue is not lost, its leg is waiting;
//! - the pair has not been forced within `FORCE_COOLDOWN`, which bounds a peer
//!   that never converges (or lies) to one full-state stream per cooldown.
//!
//! The summary may have changed between the rounds: other writers keep a
//! shared contract moving, and what matters is that this node's state is still
//! missing at the peer.
//!
//! Bounds:
//! - later writes never move a scheduled check; they set `rewritten`, which
//!   starts one new check after this one ends;
//! - at most `MAX_TRACKED_CONTRACTS` checks; a write beyond that is not checked
//!   (counted);
//! - at most `MAX_PEERS_PER_CHECK` requests per round (rotating when there are
//!   more targets, counted), two rounds per check, and at most about
//!   `MAX_REQUESTS_PER_WAKE` requests per wake of the driver: the rest wait
//!   `DEFER_DELAY` so a burst cannot fill the node's event channel;
//! - one forced stream per (contract, peer) per `FORCE_COOLDOWN`, with at most
//!   `MAX_FORCE_PAIRS` pairs remembered (a refusal at the cap is counted); an
//!   unconsumed force mark expires after `FORCE_TTL`.
//!
//! Residuals:
//! - a peer that never replies costs one request per round and then nothing;
//!   anti-entropy remains its only repair;
//! - a reply carrying no summary is not a two-sided comparison, so it never
//!   reaches the check: the peer stays awaited until the round's deadline;
//! - a request that the event channel refuses is counted and left to the
//!   round's deadline, not retried;
//! - a forced heal that the `Summaries` arm skips (emit budget, ban, no local
//!   state) leaves its mark for the next leg to the pair until `FORCE_TTL`.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
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
/// time for the delta heal the first reply triggered to land.
pub(crate) const CONFIRM_DELAY: Duration = Duration::from_secs(5);
/// How long a round waits for replies.
pub(crate) const REPLY_TIMEOUT: Duration = Duration::from_secs(5);
/// Contracts with a check scheduled or in flight.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 1024;
/// Requests per round.
pub(crate) const MAX_PEERS_PER_CHECK: usize = 32;
/// Requests per wake of the driver before the rest are deferred. The node's
/// event channel holds 2048 events; this keeps one wake well under it.
pub(crate) const MAX_REQUESTS_PER_WAKE: usize = 128;
/// How long deferred rounds wait for the next wake.
pub(crate) const DEFER_DELAY: Duration = Duration::from_millis(250);
/// How long a force mark waits for the heal leg it was set for.
pub(crate) const FORCE_TTL: Duration = Duration::from_secs(30);
/// Minimum time between two forced full-state streams to one (contract, peer).
/// Ten minutes is two anti-entropy heartbeats (`INTEREST_HEARTBEAT_INTERVAL`,
/// 5 min): long enough that a peer which never converges, or lies, costs at
/// most six full states an hour per contract, and short enough that a fault
/// that comes back after a heal is forced again well within the hour.
pub(crate) const FORCE_COOLDOWN: Duration = Duration::from_secs(600);
/// (contract, peer) pairs remembered for force marks and cooldowns.
pub(crate) const MAX_FORCE_PAIRS: usize = 1024;

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
    /// Peers `First` found stale. `Confirm` asks exactly these.
    stale_first: HashSet<PeerKey>,
    /// An originated write landed after this check was scheduled.
    rewritten: bool,
}

impl Check {
    fn scheduled(due: Instant) -> Self {
        Self {
            round: Round::First,
            phase: Phase::Scheduled { due },
            awaiting: HashSet::new(),
            stale_first: HashSet::new(),
            rewritten: false,
        }
    }
}

/// A (contract, peer) pair that was forced.
#[derive(Debug, Clone, Copy)]
struct Forced {
    /// The force mark, until the heal leg consumes it or it expires.
    mark_until: Option<Instant>,
    /// No further force for the pair before this.
    cooldown_until: Instant,
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
    /// The pair was forced within `FORCE_COOLDOWN`.
    Cooldown,
    /// `MAX_FORCE_PAIRS` pairs are remembered.
    Cap,
    /// A leg for the pair is queued or sending here.
    Busy,
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
    /// stream. The force mark is already set.
    Escalate,
    /// Stale in `Confirm`, not forced.
    Held(Hold),
}

#[derive(Default)]
struct Inner {
    checks: HashMap<ContractKey, Check>,
    forced: HashMap<(ContractKey, PeerKey), Forced>,
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
    /// Start offset for the next round with more than `MAX_PEERS_PER_CHECK`
    /// targets, so truncation rotates instead of always asking the same peers.
    rotation: AtomicUsize,
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
        let pairs = inner.forced.values().map(|f| f.cooldown_until);
        checks.chain(pairs).min()
    }

    /// Resolve every round whose deadline passed, forget pairs whose cooldown
    /// ended, and return the rounds due to fire. The caller must register each
    /// fired round with [`Self::record_sent`] BEFORE sending its requests.
    pub(crate) fn take_due(&self, now: Instant) -> Due {
        let mut inner = self.inner.lock();
        inner.forced.retain(|_, f| f.cooldown_until > now);
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
                        Round::Confirm => check.stale_first.iter().cloned().collect(),
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
    pub(crate) fn record_sent(&self, key: &ContractKey, peers: Vec<PeerKey>, now: Instant) {
        let mut inner = self.inner.lock();
        let Some(check) = inner.checks.get_mut(key) else {
            return;
        };
        check.awaiting = peers.into_iter().collect();
        check.phase = Phase::Sent {
            deadline: now + REPLY_TIMEOUT,
        };
        // Nobody to ask: resolve now rather than wait out the deadline.
        if check.awaiting.is_empty() && !resolve(check, now) {
            inner.checks.remove(key);
        }
        drop(inner);
        self.wake.notify_one();
    }

    /// A two-sided `Summaries` comparison for `key` from `peer`.
    /// `real_evidence` says whether `stale` rests on a real verdict rather
    /// than a byte-compare fallback. `pair_busy` is asked only when a force
    /// is otherwise due.
    pub(crate) fn on_comparison(
        &self,
        key: &ContractKey,
        peer: &PeerKey,
        stale: bool,
        real_evidence: bool,
        pair_busy: impl FnOnce() -> bool,
        now: Instant,
    ) -> Verdict {
        let mut inner = self.inner.lock();
        let Inner { checks, forced } = &mut *inner;
        let Some(check) = checks.get_mut(key) else {
            return Verdict::Ignored;
        };
        if !matches!(check.phase, Phase::Sent { .. }) || !check.awaiting.remove(peer) {
            return Verdict::Ignored;
        }
        let verdict = match (check.round, stale) {
            (_, false) => Verdict::Converged,
            (Round::First, true) => {
                check.stale_first.insert(peer.clone());
                Verdict::Stale
            }
            (Round::Confirm, true) => {
                let pair = (*key, peer.clone());
                let previous = forced.get(&pair);
                if !real_evidence {
                    Verdict::Held(Hold::Evidence)
                } else if previous.is_some_and(|f| f.cooldown_until > now) {
                    Verdict::Held(Hold::Cooldown)
                } else if previous.is_none() && forced.len() >= MAX_FORCE_PAIRS {
                    Verdict::Held(Hold::Cap)
                } else if pair_busy() {
                    Verdict::Held(Hold::Busy)
                } else {
                    forced.insert(
                        pair,
                        Forced {
                            mark_until: Some(now + FORCE_TTL),
                            cooldown_until: now + FORCE_COOLDOWN,
                        },
                    );
                    Verdict::Escalate
                }
            }
        };
        if check.awaiting.is_empty() && !resolve(check, now) {
            checks.remove(key);
        }
        drop(inner);
        self.wake.notify_one();
        verdict
    }

    /// Consume the force mark for (`key`, `peer`), if one is live. The pair's
    /// cooldown stays.
    pub(crate) fn take_forced(&self, key: &ContractKey, peer: &PeerKey, now: Instant) -> bool {
        let mut inner = self.inner.lock();
        let Some(f) = inner.forced.get_mut(&(*key, peer.clone())) else {
            return false;
        };
        f.mark_until.take().is_some_and(|until| until > now)
    }

    /// Whether a live force mark exists for (`key`, `peer`), without consuming
    /// it.
    #[cfg(feature = "simulation_tests")]
    pub(crate) fn has_forced(&self, key: &ContractKey, peer: &PeerKey, now: Instant) -> bool {
        self.inner
            .lock()
            .forced
            .get(&(*key, peer.clone()))
            .and_then(|f| f.mark_until)
            .is_some_and(|until| until > now)
    }

    /// Choose at most `MAX_PEERS_PER_CHECK` of `candidates`, rotating the start
    /// when there are more. Returns whether any were left out.
    fn select<T>(&self, mut candidates: Vec<T>) -> (Vec<T>, bool) {
        if candidates.len() <= MAX_PEERS_PER_CHECK {
            return (candidates, false);
        }
        let start = self
            .rotation
            .fetch_add(MAX_PEERS_PER_CHECK, Ordering::Relaxed);
        let len = candidates.len();
        candidates.rotate_left(start % len);
        candidates.truncate(MAX_PEERS_PER_CHECK);
        (candidates, true)
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.inner.lock().checks.len()
    }
}

/// Move a resolved round on. Returns `false` when the check has ended and
/// must be removed.
fn resolve(check: &mut Check, now: Instant) -> bool {
    check.awaiting.clear();
    if check.round == Round::First && !check.stale_first.is_empty() {
        check.round = Round::Confirm;
        check.phase = Phase::Scheduled {
            due: now + CONFIRM_DELAY,
        };
        return true;
    }
    if check.rewritten {
        // A write landed during this check: one fresh check for it.
        *check = Check::scheduled(now + CHECK_DELAY);
        return true;
    }
    false
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
        || pair_busy(op_manager, key, peer),
        op_manager.ring.time_source.now(),
    );
    let events: &[PropagationEvent] = match verdict {
        Verdict::Ignored => &[],
        Verdict::Converged => &[PropagationEvent::VerifyPeerConverged],
        Verdict::Stale => &[PropagationEvent::VerifyPeerStale],
        Verdict::Escalate => &[
            PropagationEvent::VerifyPeerStale,
            PropagationEvent::VerifyForcedFull,
        ],
        Verdict::Held(hold) => &[
            PropagationEvent::VerifyPeerStale,
            match hold {
                Hold::Evidence => PropagationEvent::VerifyNotForcedEvidence,
                Hold::Cooldown => PropagationEvent::VerifyNotForcedCooldown,
                Hold::Cap => PropagationEvent::VerifyNotForcedCap,
                Hold::Busy => PropagationEvent::VerifyNotForcedBusy,
            },
        ],
    };
    for ev in events {
        record_propagation_event(op_manager, key.id(), *ev);
    }
    #[cfg(any(test, feature = "testing"))]
    crate::node::stale_peer_heal_observations::record_verify_verdict(key, verdict);
}

/// Fire one due round: register it, then ask each eligible target for its
/// summary of the contract. Eligible means a current fan-out target that can
/// decode `SummaryRequest` (`supports_hash_first_summaries`). Returns the
/// number of requests attempted, for the per-wake budget.
fn fire_round(op_manager: &crate::node::OpManager, due: DueRound, now: Instant) -> usize {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    use crate::message::{InterestMessage, NodeEvent};

    let DueRound { key, round, peers } = due;
    let eligible: Vec<(std::net::SocketAddr, PeerKey)> = op_manager
        .get_broadcast_targets_update(
            &key,
            &crate::ring::broadcast_coverage::BroadcastOrigin::local(),
        )
        .targets
        .into_iter()
        .filter_map(|target| {
            let addr = target.socket_addr()?;
            let peer = PeerKey::from(target.pub_key().clone());
            if round == Round::Confirm && !peers.contains(&peer) {
                return None;
            }
            op_manager
                .ring
                .connection_manager
                .supports_hash_first_summaries(addr)
                .then_some((addr, peer))
        })
        .collect();
    let (asked, truncated) = op_manager.verify_soon.select(eligible);
    if truncated {
        record_propagation_event(op_manager, key.id(), PropagationEvent::VerifyPeersTruncated);
    }
    // Register first: a reply can arrive before this function returns.
    op_manager.verify_soon.record_sent(
        &key,
        asked.iter().map(|(_, peer)| peer.clone()).collect(),
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
    asked.len()
}

/// The node's verify-soon driver: sleeps until the next due time (or until
/// woken by a new check), fires due rounds within a per-wake budget, and ends
/// when its `OpManager` is gone. On shutdown it parks rather than returning,
/// so the task monitor does not read a clean exit as a failure.
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
            // Attached once and dropped: the node is gone.
            OpManagerState::Detached => return,
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
        let mut budget = MAX_REQUESTS_PER_WAKE;
        let mut deferred = false;
        for round in due.rounds {
            if budget == 0 {
                // Still `Scheduled` and due: picked up by the next wake.
                deferred = true;
                break;
            }
            budget = budget.saturating_sub(fire_round(&op_manager, round, now));
        }
        let next = if deferred {
            Some(now + DEFER_DELAY)
        } else {
            verify.next_wake()
        };
        drop(op_manager);
        // Cancellation safety: every arm is a timer, a notification or the
        // shutdown token; dropping the losers discards no work. `Notify`
        // stores a permit, so a check scheduled between `next_wake` and this
        // wait is not missed.
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = verify.wake.notified() => {}
            _ = async {
                match next {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};
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

    /// Run one check for `k` against `p` up to its `Confirm` reply and return
    /// that reply's verdict. `t0` is the write's time.
    fn confirm_verdict(
        vs: &VerifySoon,
        k: ContractKey,
        p: &PeerKey,
        t0: Instant,
        real_evidence: bool,
        busy: bool,
    ) -> Verdict {
        assert_eq!(vs.note_originated(k, t0), Originated::Scheduled);
        let t1 = t0 + CHECK_DELAY;
        assert_eq!(vs.take_due(t1).rounds.len(), 1);
        vs.record_sent(&k, vec![p.clone()], t1);
        assert_eq!(
            vs.on_comparison(&k, p, true, true, idle, t1),
            Verdict::Stale
        );
        let t2 = t1 + CONFIRM_DELAY;
        let due = vs.take_due(t2).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::Confirm);
        vs.record_sent(&k, vec![p.clone()], t2);
        vs.on_comparison(&k, p, true, real_evidence, || busy, t2)
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
        let due = vs.take_due(t0 + CHECK_DELAY).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::First);
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
            vs.on_comparison(&key(1), &a, false, true, idle, t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 1, "still waiting for b");
        assert_eq!(
            vs.on_comparison(&key(1), &b, false, true, idle, t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 0);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, idle, t1),
            Verdict::Ignored,
            "a comparison outside a round is not an answer"
        );
    }

    #[tokio::test]
    async fn a_reply_before_the_round_is_registered_is_ignored() {
        // Why `fire_round` registers before it sends (pinned below).
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, idle, t1),
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
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, idle, t1),
            Verdict::Stale
        );
        assert_eq!(
            vs.on_comparison(&key(1), &b, true, false, idle, t1),
            Verdict::Stale,
            "First records a stale peer whatever the evidence"
        );
        assert_eq!(
            vs.on_comparison(&key(1), &c, false, true, idle, t1),
            Verdict::Converged
        );
        let t2 = t1 + CONFIRM_DELAY;
        let due = vs.take_due(t2).rounds;
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::Confirm);
        let mut asked = due[0].peers.clone();
        asked.sort_by_key(|p| format!("{p:?}"));
        let mut expected = vec![a.clone(), b.clone()];
        expected.sort_by_key(|p| format!("{p:?}"));
        assert_eq!(asked, expected, "Confirm asks exactly the stale peers");
        vs.record_sent(&key(1), vec![a.clone(), b.clone()], t2);
        // No summary is compared between rounds: on a contract other writers
        // keep changing, the peer's summary moves and the fault persists.
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, true, idle, t2),
            Verdict::Escalate
        );
        assert_eq!(
            vs.on_comparison(&key(1), &b, false, true, idle, t2),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 0, "Confirm ends the check");
        assert!(vs.take_forced(&key(1), &a, t2));
        assert!(
            !vs.take_forced(&key(1), &a, t2),
            "the mark is consumed once"
        );
        assert!(!vs.take_forced(&key(1), &b, t2));
    }

    #[tokio::test]
    async fn a_fallback_verdict_is_not_forced() {
        let vs = VerifySoon::new();
        let a = peer();
        let asked = Cell::new(false);
        let t0 = Instant::now();
        assert_eq!(vs.note_originated(key(1), t0), Originated::Scheduled);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone()], t1);
        vs.on_comparison(&key(1), &a, true, true, idle, t1);
        let t2 = t1 + CONFIRM_DELAY;
        vs.take_due(t2);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        assert_eq!(
            vs.on_comparison(
                &key(1),
                &a,
                true,
                false,
                || {
                    asked.set(true);
                    false
                },
                t2
            ),
            Verdict::Held(Hold::Evidence)
        );
        assert!(!vs.take_forced(&key(1), &a, t2));
        assert!(
            !asked.get(),
            "the queue is not asked once the evidence fails"
        );
    }

    #[tokio::test]
    async fn a_pair_with_a_leg_queued_or_sending_is_not_forced() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        assert_eq!(
            confirm_verdict(&vs, key(1), &a, t0, true, true),
            Verdict::Held(Hold::Busy)
        );
        assert!(!vs.take_forced(&key(1), &a, t0 + CHECK_DELAY + CONFIRM_DELAY));
        // Not a cooldown: the next check may force.
        let t3 = t0 + Duration::from_secs(60);
        assert_eq!(
            confirm_verdict(&vs, key(1), &a, t3, true, false),
            Verdict::Escalate
        );
    }

    #[tokio::test]
    async fn a_pair_is_forced_at_most_once_per_cooldown() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        assert_eq!(
            confirm_verdict(&vs, key(1), &a, t0, true, false),
            Verdict::Escalate
        );
        // A peer that keeps answering stale: every later check inside the
        // cooldown is held, so it costs no further full state.
        let mut t = t0 + Duration::from_secs(60);
        while t < t0 + CHECK_DELAY + CONFIRM_DELAY + FORCE_COOLDOWN - Duration::from_secs(20) {
            assert_eq!(
                confirm_verdict(&vs, key(1), &a, t, true, false),
                Verdict::Held(Hold::Cooldown)
            );
            t += Duration::from_secs(60);
        }
        let after = t0 + CHECK_DELAY + CONFIRM_DELAY + FORCE_COOLDOWN;
        vs.take_due(after);
        assert_eq!(
            confirm_verdict(&vs, key(1), &a, after, true, false),
            Verdict::Escalate,
            "after the cooldown the pair may be forced again"
        );
    }

    #[tokio::test]
    async fn remembered_pairs_are_capped() {
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        for n in 0..MAX_FORCE_PAIRS {
            assert_eq!(
                confirm_verdict(&vs, nth_key(n), &a, t0, true, false),
                Verdict::Escalate
            );
        }
        assert_eq!(
            confirm_verdict(&vs, nth_key(MAX_FORCE_PAIRS), &a, t0, true, false),
            Verdict::Held(Hold::Cap)
        );
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
        vs.on_comparison(&key(1), &a, false, true, idle, t1);
        assert_eq!(vs.tracked(), 1);
        assert_eq!(vs.next_wake(), Some(t1 + CHECK_DELAY));
        let t2 = t1 + CHECK_DELAY;
        assert_eq!(vs.take_due(t2).rounds.len(), 1);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        vs.on_comparison(&key(1), &a, false, true, idle, t2);
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

    #[tokio::test]
    async fn an_unconsumed_force_mark_expires_and_the_cooldown_stays() {
        // E.g. the forced heal was skipped by the Summaries arm's emit budget:
        // the next leg to the pair within FORCE_TTL is forced, none after.
        let vs = VerifySoon::new();
        let a = peer();
        let t0 = Instant::now();
        assert_eq!(
            confirm_verdict(&vs, key(1), &a, t0, true, false),
            Verdict::Escalate
        );
        let t2 = t0 + CHECK_DELAY + CONFIRM_DELAY;
        assert!(vs.inner.lock().forced.contains_key(&(key(1), a.clone())));
        assert!(
            !vs.take_forced(&key(1), &a, t2 + FORCE_TTL),
            "the mark expired"
        );
        assert_eq!(
            vs.next_wake(),
            Some(t2 + FORCE_COOLDOWN),
            "the pair is forgotten when its cooldown ends"
        );
        vs.take_due(t2 + FORCE_COOLDOWN);
        assert_eq!(vs.next_wake(), None);
    }

    #[tokio::test]
    async fn truncation_rotates_through_the_targets() {
        let vs = VerifySoon::new();
        let all: Vec<usize> = (0..MAX_PEERS_PER_CHECK + 8).collect();
        let (first, truncated) = vs.select(all.clone());
        assert!(truncated);
        assert_eq!(first.len(), MAX_PEERS_PER_CHECK);
        let (second, _) = vs.select(all.clone());
        let missed: Vec<_> = all.iter().filter(|n| !first.contains(n)).collect();
        assert!(
            missed.iter().all(|n| second.contains(n)),
            "the peers left out of one round are asked in the next"
        );
        let few: Vec<usize> = (0..3).collect();
        assert_eq!(vs.select(few.clone()), (few, false));
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
            .find(".record_sent(")
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
    }
}
