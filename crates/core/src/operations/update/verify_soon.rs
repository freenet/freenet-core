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
//! A peer found stale in `First` whose summary is UNCHANGED and still stale in
//! `Confirm` received neither the original legs nor the delta heal `First`
//! triggered. That is the #6143 shape, where every inline message is lost. Its
//! heal is then forced to full state on the caller-named stream, the path that
//! kept working: once per (contract, peer) per check.
//!
//! Bounds:
//! - later writes never move a scheduled check; they set `rewritten`, which
//!   starts one new check after this one ends;
//! - at most `MAX_TRACKED_CONTRACTS` checks; a write beyond that is not checked
//!   (counted);
//! - at most `MAX_PEERS_PER_CHECK` requests per round, two rounds per check;
//! - a force mark expires after `FORCE_TTL` if no leg consumes it, and there
//!   are at most `MAX_FORCE_MARKS`.

use std::collections::{HashMap, HashSet};
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
/// How long a force mark waits for the heal leg it was set for.
pub(crate) const FORCE_TTL: Duration = Duration::from_secs(30);
/// Force marks held at once.
pub(crate) const MAX_FORCE_MARKS: usize = 256;

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
    /// Peers `First` found stale, with the hash of the summary they reported.
    /// `Confirm` asks exactly these.
    stale_first: HashMap<PeerKey, u64>,
    /// An originated write landed after this check was scheduled.
    rewritten: bool,
}

impl Check {
    fn scheduled(due: Instant) -> Self {
        Self {
            round: Round::First,
            phase: Phase::Scheduled { due },
            awaiting: HashSet::new(),
            stale_first: HashMap::new(),
            rewritten: false,
        }
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

/// What a `Summaries` comparison meant to the check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Not an answer to a round in flight.
    Ignored,
    Converged,
    Stale,
    /// Stale again with the same summary: the heal that follows must be full
    /// state on the stream. The force mark is already set.
    Escalate,
}

#[derive(Default)]
struct Inner {
    checks: HashMap<ContractKey, Check>,
    force: HashMap<(ContractKey, PeerKey), Instant>,
}

/// Per-node verify-soon table. Owned by `OpManager`; driven by
/// [`run_verify_soon`].
#[derive(Default)]
pub(crate) struct VerifySoon {
    inner: Mutex<Inner>,
    wake: Notify,
}

impl VerifySoon {
    pub(crate) fn new() -> Self {
        Self::default()
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
        let marks = inner.force.values().copied();
        checks.chain(marks).min()
    }

    /// Resolve every round whose deadline passed, expire force marks, and
    /// return the rounds due to fire. The caller must report each with
    /// [`Self::record_sent`].
    pub(crate) fn take_due(&self, now: Instant) -> Vec<DueRound> {
        let mut inner = self.inner.lock();
        inner.force.retain(|_, expiry| *expiry > now);
        let mut due = Vec::new();
        let mut ended = Vec::new();
        for (key, check) in inner.checks.iter_mut() {
            if let Phase::Sent { deadline } = check.phase {
                if deadline <= now && !resolve(check, now) {
                    ended.push(*key);
                    continue;
                }
            }
            if let Phase::Scheduled { due: at } = check.phase {
                if at <= now {
                    let peers = match check.round {
                        Round::First => Vec::new(),
                        Round::Confirm => check.stale_first.keys().cloned().collect(),
                    };
                    due.push(DueRound {
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

    /// The round for `key` was fired at `now` and asked `peers`.
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

    /// A two-sided `Summaries` comparison for `key` from `peer`, which
    /// reported `their_summary`. Hashed only for a peer the check awaits, so
    /// the comparisons of untracked contracts cost a map lookup.
    pub(crate) fn on_comparison(
        &self,
        key: &ContractKey,
        peer: &PeerKey,
        stale: bool,
        their_summary: &[u8],
        now: Instant,
    ) -> Verdict {
        let mut inner = self.inner.lock();
        let Inner { checks, force } = &mut *inner;
        let Some(check) = checks.get_mut(key) else {
            return Verdict::Ignored;
        };
        if !matches!(check.phase, Phase::Sent { .. }) || !check.awaiting.remove(peer) {
            return Verdict::Ignored;
        }
        let their_hash = summary_hash(their_summary);
        let verdict = match (check.round, stale) {
            (_, false) => Verdict::Converged,
            (Round::First, true) => {
                check.stale_first.insert(peer.clone(), their_hash);
                Verdict::Stale
            }
            (Round::Confirm, true) => {
                if check.stale_first.get(peer) == Some(&their_hash) && force.len() < MAX_FORCE_MARKS
                {
                    force.insert((*key, peer.clone()), now + FORCE_TTL);
                    Verdict::Escalate
                } else {
                    Verdict::Stale
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

    /// Consume the force mark for (`key`, `peer`), if one is live.
    pub(crate) fn take_forced(&self, key: &ContractKey, peer: &PeerKey, now: Instant) -> bool {
        self.inner
            .lock()
            .force
            .remove(&(*key, peer.clone()))
            .is_some_and(|expiry| expiry > now)
    }

    /// Whether a live force mark exists for (`key`, `peer`), without consuming
    /// it.
    #[cfg(feature = "simulation_tests")]
    pub(crate) fn has_forced(&self, key: &ContractKey, peer: &PeerKey, now: Instant) -> bool {
        self.inner
            .lock()
            .force
            .get(&(*key, peer.clone()))
            .is_some_and(|expiry| *expiry > now)
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

/// A stable hash of a peer's summary bytes, for "unchanged since `First`".
pub(crate) fn summary_hash(bytes: &[u8]) -> u64 {
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

/// Feed a two-sided `Summaries` comparison into the check for `key`, and count
/// what it decided. Called by the `Summaries` arm before it emits its heal.
pub(crate) fn on_summary_comparison(
    op_manager: &crate::node::OpManager,
    key: &ContractKey,
    peer: &PeerKey,
    stale: bool,
    their_summary: &[u8],
) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    let verdict = op_manager.verify_soon.on_comparison(
        key,
        peer,
        stale,
        their_summary,
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
    };
    for ev in events {
        record_propagation_event(op_manager, key.id(), *ev);
    }
    #[cfg(any(test, feature = "testing"))]
    crate::node::stale_peer_heal_observations::record_verify_verdict(key, verdict);
}

/// Fire one due round: ask each eligible target for its summary of the
/// contract. Eligible means a current fan-out target that can decode
/// `SummaryRequest` (`supports_hash_first_summaries`).
fn fire_round(op_manager: &crate::node::OpManager, due: DueRound, now: Instant) {
    use super::propagation_stats::{PropagationEvent, record_propagation_event};
    use crate::message::{InterestMessage, NodeEvent};

    let DueRound { key, round, peers } = due;
    let targets = op_manager
        .get_broadcast_targets_update(
            &key,
            &crate::ring::broadcast_coverage::BroadcastOrigin::local(),
        )
        .targets;
    let hash = crate::ring::interest::contract_hash(&key);
    let mut asked = Vec::new();
    for target in targets {
        if asked.len() >= MAX_PEERS_PER_CHECK {
            break;
        }
        let Some(addr) = target.socket_addr() else {
            continue;
        };
        let peer = PeerKey::from(target.pub_key().clone());
        if round == Round::Confirm && !peers.contains(&peer) {
            continue;
        }
        if !op_manager
            .ring
            .connection_manager
            .supports_hash_first_summaries(addr)
        {
            continue;
        }
        let sent = op_manager.try_notify_node_event(NodeEvent::SendInterestMessage {
            target: addr,
            message: InterestMessage::SummaryRequest { hashes: vec![hash] },
        });
        if sent.is_ok() {
            record_propagation_event(op_manager, key.id(), PropagationEvent::VerifyRequestSent);
            #[cfg(any(test, feature = "testing"))]
            crate::node::stale_peer_heal_observations::record_verify_request(&key, addr);
            asked.push(peer);
        }
    }
    tracing::debug!(
        contract = %key,
        ?round,
        asked = asked.len(),
        "verify-soon: round fired"
    );
    op_manager.verify_soon.record_sent(&key, asked, now);
}

/// The node's verify-soon driver: sleeps until the next due time (or until
/// woken by a new check), fires due rounds, and exits only on shutdown.
pub(crate) async fn run_verify_soon(ring: std::sync::Arc<crate::ring::Ring>) {
    let shutdown = ring.shutdown_token();
    loop {
        let Some(op_manager) = ring.upgrade_op_manager() else {
            // Not attached yet, or the node is stopping.
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
            }
        };
        let verify = op_manager.verify_soon.clone();
        let now = ring.time_source.now();
        for due in verify.take_due(now) {
            fire_round(&op_manager, due, now);
        }
        let next = verify.next_wake();
        drop(op_manager);
        // Cancellation safety: every arm is a timer, a notification or the
        // shutdown token; dropping the losers discards no work. `Notify`
        // stores a permit, so a check scheduled between `next_wake` and this
        // wait is not missed.
        tokio::select! {
            _ = shutdown.cancelled() => return,
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

    fn key(n: u8) -> ContractKey {
        ContractKey::from_id_and_code(ContractInstanceId::new([n; 32]), CodeHash::new([n; 32]))
    }

    fn peer() -> PeerKey {
        PeerKey::from(crate::transport::TransportKeypair::new().public().clone())
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
                .is_empty()
        );
        let due = vs.take_due(t0 + CHECK_DELAY);
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
            vs.on_comparison(&key(1), &a, false, &[7], t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 1, "still waiting for b");
        assert_eq!(
            vs.on_comparison(&key(1), &b, false, &[7], t1),
            Verdict::Converged
        );
        assert_eq!(vs.tracked(), 0);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, &[7], t1),
            Verdict::Ignored,
            "a comparison outside a round is not an answer"
        );
    }

    #[tokio::test]
    async fn confirm_asks_only_the_stale_peers_and_escalates_on_the_same_summary() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let (a, b, c) = (peer(), peer(), peer());
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone(), b.clone(), c.clone()], t1);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, &[10], t1),
            Verdict::Stale
        );
        assert_eq!(
            vs.on_comparison(&key(1), &b, true, &[20], t1),
            Verdict::Stale
        );
        assert_eq!(
            vs.on_comparison(&key(1), &c, false, &[30], t1),
            Verdict::Converged
        );
        let t2 = t1 + CONFIRM_DELAY;
        let due = vs.take_due(t2);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].round, Round::Confirm);
        let mut asked = due[0].peers.clone();
        asked.sort_by_key(|p| format!("{p:?}"));
        let mut expected = vec![a.clone(), b.clone()];
        expected.sort_by_key(|p| format!("{p:?}"));
        assert_eq!(asked, expected, "Confirm asks exactly the stale peers");
        vs.record_sent(&key(1), vec![a.clone(), b.clone()], t2);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, &[10], t2),
            Verdict::Escalate,
            "stale with the same summary: nothing reached it"
        );
        assert_eq!(
            vs.on_comparison(&key(1), &b, true, &[21], t2),
            Verdict::Stale,
            "stale with a different summary: something reached it"
        );
        assert_eq!(vs.tracked(), 0, "Confirm ends the check");
        assert!(vs.take_forced(&key(1), &a, t2));
        assert!(!vs.take_forced(&key(1), &a, t2), "one escalation per mark");
        assert!(!vs.take_forced(&key(1), &b, t2));
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
        assert!(vs.take_due(t1 + REPLY_TIMEOUT).is_empty());
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
        vs.on_comparison(&key(1), &a, false, &[1], t1);
        assert_eq!(vs.tracked(), 1);
        assert_eq!(vs.next_wake(), Some(t1 + CHECK_DELAY));
        let t2 = t1 + CHECK_DELAY;
        assert_eq!(vs.take_due(t2).len(), 1);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        vs.on_comparison(&key(1), &a, false, &[1], t2);
        assert_eq!(vs.tracked(), 0, "no write since: no further check");
    }

    #[tokio::test]
    async fn the_table_is_capped() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        for n in 0..MAX_TRACKED_CONTRACTS {
            let mut id = [0u8; 32];
            id[..8].copy_from_slice(&(n as u64).to_le_bytes());
            let k = ContractKey::from_id_and_code(ContractInstanceId::new(id), CodeHash::new(id));
            assert_eq!(vs.note_originated(k, t0), Originated::Scheduled);
        }
        assert_eq!(vs.note_originated(key(255), t0), Originated::DroppedCap);
    }

    #[tokio::test]
    async fn an_unused_force_mark_expires() {
        let vs = VerifySoon::new();
        let t0 = Instant::now();
        let a = peer();
        vs.note_originated(key(1), t0);
        let t1 = t0 + CHECK_DELAY;
        vs.take_due(t1);
        vs.record_sent(&key(1), vec![a.clone()], t1);
        vs.on_comparison(&key(1), &a, true, &[5], t1);
        let t2 = t1 + CONFIRM_DELAY;
        vs.take_due(t2);
        vs.record_sent(&key(1), vec![a.clone()], t2);
        assert_eq!(
            vs.on_comparison(&key(1), &a, true, &[5], t2),
            Verdict::Escalate
        );
        assert_eq!(vs.next_wake(), Some(t2 + FORCE_TTL));
        vs.take_due(t2 + FORCE_TTL);
        assert_eq!(vs.next_wake(), None, "the expired mark is gone");
        assert!(!vs.take_forced(&key(1), &a, t2 + FORCE_TTL));
    }
}
