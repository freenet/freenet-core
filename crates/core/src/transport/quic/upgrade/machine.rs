//! The QUIC upgrade of one UDP session, as a pure state machine (#5940, step
//! e5a-5a).
//!
//! [`UpgradeMachine::step`] takes an [`Event`] and returns the [`Action`]s for
//! the caller to carry out. It does no I/O and reads no clock: time and the
//! random draw for jitter come in through [`Inputs`], so a test fixes both.
//! The caller sleeps until [`UpgradeMachine::deadline`] and then sends
//! [`Event::Tick`]. [`super::driver`] is that caller.
//!
//! The higher key is the QUIC server and the only side that offers. A key
//! comes only from [`Event::Promoted`], never from an upgrade message, so a
//! transport with no peer key has no role: it does not offer, and it answers
//! an offer with `NotReady`.
//!
//! ```text
//! state       event                         next        actions
//! ----------  ----------------------------  ----------  ----------------------
//! no role     Promoted, server, floor met   Idle        retry in 2-10 s
//! no role     Promoted, server, below floor Off         ineligible_version
//! no role     Promoted, client              Idle        -
//! no role     Promoted, equal keys          Off         role_conflict
//! no role     Offer                         no role     Decline{NotReady}
//! Idle        Tick due, server              Offered     allow peer, then Offer
//! Offered     OfferSent                     Offered     offered
//! Idle        Tick due, server, no permit   Idle        retry in BACKOFF_MIN, uncharged
//! Idle        Tick due, policy disabled     Off         -
//! Idle        Offer, client, all checks ok  Connecting  Accept, dial
//! Idle        Offer refused by a check      Idle        Decline{reason}
//! setup       Offer, client                 same        Decline{NotReady}
//! Active      Offer, client                 same        Decline{NotReady}
//! Rechecking  Offer, client                 same        Decline{NotReady}
//! Off         Offer, client or no role      Off         Decline{Disabled}
//! any         stale or repeated Offer       same        -
//! any         Offer, server                 same        role_conflict
//! Offered     Accept                        Connecting  accepted
//! Offered     Decline, Disabled or unknown  Idle        tear down, backoff
//! Offered     Decline, any other reason     Idle        tear down, retry in BACKOFF_MIN, uncharged
//! Offered     PathEstablished               Confirming  Ready (Accept still on its way)
//! Connecting  PathEstablished, server       Confirming  Ready
//! Connecting  PathEstablished, client       Confirming  -
//! Connecting  Ready, client                 Active      ReadyAck
//! Confirming  Ready, client                 Active      ReadyAck
//! Confirming  ReadyAck, server              Active      -
//! setup       Ready, server                 same        role_conflict
//! Offered     Tick past deadline            Idle        tear down, backoff (no_reply)
//! Connecting  Tick past deadline            Idle        tear down, backoff (timeout)
//! Confirming  Tick past deadline            Idle        tear down, backoff (timeout)
//! setup       control frame refused, new    Idle        close CONTROL_REFUSED, backoff
//! setup or Active  send Failed, timed out   Idle        tear down, backoff
//! setup or Active  send Failed, lost        as PathFailed
//! setup or Active  send Failed, stopped     as PathFailed (one stream: no effect)
//! setup or Active  IdentityMismatch         Idle        close IDENTITY_MISMATCH, 1 h
//! setup or Active  PathFailed               see classify()
//! Rechecking  Tick past deadline            Idle or Off backoff, or none if Down
//! any         EndpointNotUp, not Up         Off         tear down, no backoff
//! setup       SessionEnded                  Off         tear down, backoff
//! Active      SessionEnded, not yet stable  Off         tear down, backoff
//! Active      SessionEnded, stable          Off         tear down, clear the count
//! Rechecking  SessionEnded, not yet stable  Off         backoff
//! Rechecking  SessionEnded, stable          Off         clear the count
//! Idle or Off SessionEnded                  Off         -
//! ```
//!
//! "setup" is `Offered`, `Connecting` or `Confirming` ([`Phase`]). "no role" is
//! `Idle` with no promotion yet. "control frame refused, new" is one of an
//! unknown version or type. An event for any attempt but the
//! current one changes nothing, and so does every pair the table does not list.
//! A `SessionEnded` that finds the endpoint down takes no backoff.
//!
//! Each side counts a path as established on its own evidence: the client on
//! `Ready`, the server on `ReadyAck`. The two can disagree (a send may be
//! `MaybeDelivered`), and the side that waits times out and closes the path,
//! which ends the disagreement.
//!
//! The machine holds a [`QuicPathPermit`] only inside a state, from offering
//! or dialing through active, so leaving the state returns it.
//!
//! No failure downgrades a peer for good: every path to `Idle` leads to a new
//! offer after the backoff. `Off` is final for the session, also for a policy
//! that refuses now and would allow later: only the next session's machine can
//! offer. It follows the end of the endpoint or of the session, a policy that
//! refuses, equal keys, or a peer below the version floor. Of these only the
//! end of a session charges the peer, as the table says. In `Off` a client or
//! a machine with no role answers an offer with `Disabled`, and a server records
//! `role_conflict` and does not reply.
//!
//! A failure the peer is not to blame for does not raise the failure count:
//! a `Decline` of `Busy`, `NotReady` or `Backoff`, and a full allow-set, retry
//! after [`BACKOFF_MIN`]. So a peer that stays `NotReady` or in its own backoff
//! is asked once per [`BACKOFF_MIN`] for as long as that lasts.
//!
//! The count and the time of the next attempt outlive a machine through
//! [`BackoffSeed`]: the caller carries it per peer key to the machine of the
//! session that replaces this one, in [`Event::Promoted`], so a session that
//! flaps does not retry at session rate.
//!
//! What the caller must do:
//!
//! - Report [`Event::OfferSent`] before it steps any reply to that `Offer`. A
//!   later report records nothing, so the offer goes uncounted.
//! - Drop an `Offer` still deferred behind `allow_peer` once a
//!   [`Action::TearDown`] names its attempt.
//! - Read [`UpgradeMachine::backoff_seed`] after stepping
//!   [`Event::SessionEnded`], since that step can charge or clear the count.
//!   A machine that took no seed has none to hand on (`None`): a promotion that
//!   comes after `Off` is ignored, seed included, and the caller files nothing
//!   for that session.
//! - End and file any older session for the same peer key before stepping
//!   [`Event::Promoted`] on the new one. A seed from an older session never
//!   replaces the seed filed by a newer one, so the caller tracks session
//!   order. A max merge is wrong: it undoes the clear a stable path makes.
//! - Give every session the listener's clock clone: `RealTime::new()` starts
//!   its own epoch, and `retry_at` is on the clock of [`Inputs::now`].
//! - Give the client and server machines of a session one lifetime, the
//!   session's. Attempt ids restart with each session, and a client ignores
//!   with no reply an `Offer` at or below the highest it has seen, so a client
//!   machine that outlived its server's would not answer the new one.

#[cfg(test)]
mod tests;

use std::time::Duration;

use super::policy::{DisabledBy, PathRefusal, QuicPathPermit, QuicPolicy};
use crate::transport::TransportPublicKey;
use crate::transport::metrics::QuicCloseCode;
use crate::transport::quic::control::{ControlError, QuicControl, caps};
use crate::transport::quic::error::attribute;
use crate::transport::quic::liveness::QuicAvailability;
use crate::transport::quic::path::{BoundedSend, QuicSendError, SendFailure};
use crate::transport::symmetric_message::{DeclineReason, UpgradeControl, may_offer_upgrade};
use crate::transport::{QuicErrorKind, QuicRefusal};
use crate::util::backoff::ExponentialBackoff;

/// How long the server waits for a reply to its `Offer`.
pub(in crate::transport) const OFFER_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a side waits for the connection, from `Accept` to established.
pub(in crate::transport) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The bound on one `Ready` or `ReadyAck` send. For the caller.
pub(in crate::transport) const CONTROL_SEND_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a side with a connection waits for the other's frame. Covers the
/// peer's send and, on the server, its own `Ready` send before that.
pub(in crate::transport) const CONFIRM_TIMEOUT: Duration =
    Duration::from_secs(2 * CONTROL_SEND_TIMEOUT.as_secs());
/// How long a `Protocol{0x1}` waits to see whether the endpoint went down.
pub(in crate::transport) const RECHECK_TIMEOUT: Duration = Duration::from_secs(1);
/// The first delay after a failure, before jitter.
pub(in crate::transport) const BACKOFF_MIN: Duration = Duration::from_secs(60);
/// The longest delay after a failure, before jitter.
pub(in crate::transport) const BACKOFF_MAX: Duration = Duration::from_secs(60 * 60);
/// The delay after an identity mismatch, before jitter.
pub(in crate::transport) const IDENTITY_BACKOFF: Duration = BACKOFF_MAX;
/// A path that lasted this long clears the failure count when it ends. A path
/// that fails sooner keeps it, so one that establishes and then dies at once
/// is retried less and less often.
pub(in crate::transport) const STABLE_PATH: Duration = Duration::from_secs(10 * 60);
/// The longest wait a carried [`BackoffSeed`] may impose: the longest delay with
/// the most jitter. A seed read off a clock with another epoch cannot lock a
/// peer out for longer than this.
pub(in crate::transport) const SEED_WAIT_MAX: Duration =
    Duration::from_secs(BACKOFF_MAX.as_secs() * 6 / 5);
/// The earliest first offer after promotion.
pub(in crate::transport) const OFFER_DELAY_MIN: Duration = Duration::from_secs(2);
/// The latest first offer after promotion.
pub(in crate::transport) const OFFER_DELAY_MAX: Duration = Duration::from_secs(10);

const BACKOFF: ExponentialBackoff = ExponentialBackoff::new(BACKOFF_MIN, BACKOFF_MAX);

/// QUIC transport error INTERNAL_ERROR: an endpoint whose driver was dropped
/// reports it before the availability says `Down` (`QuicEndpoint::error_kind`).
const INTERNAL_ERROR: u64 = 0x1;

/// What a step needs from outside.
#[derive(Clone, Copy)]
pub(in crate::transport) struct Inputs<'a> {
    /// The time now, from the connection's `TimeSource::now`.
    pub(in crate::transport) now: Duration,
    /// A uniform draw in `0.0..1.0` (`GlobalRng`). Anything else is clamped.
    pub(in crate::transport) sample: f64,
    pub(in crate::transport) policy: &'a QuicPolicy,
}

/// What happened.
#[derive(Debug)]
pub(in crate::transport) enum Event {
    /// The node promoted this connection into the ring under `peer_key`
    /// (the transport took it: `PeerIdentityUpdate::Set` or `Same`). A later
    /// promotion changes nothing.
    ///
    /// `backoff` is what the machine of this peer's previous session left, or
    /// the default if none. It comes here and not at construction because an
    /// inbound session holds no key until promotion, and the seed is filed per
    /// key, while the machine must exist before that to answer an early Offer.
    Promoted {
        peer_key: TransportPublicKey,
        remote_version: Option<(u8, u8, u16)>,
        backoff: BackoffSeed,
    },
    /// [`UpgradeMachine::deadline`] may have passed.
    Tick,
    /// `allow_peer` worked and the `Offer` of `attempt` is on its way. Report
    /// it before taking the next event, so nothing can answer the `Offer`
    /// first.
    OfferSent { attempt: u32 },
    /// An upgrade message arrived on the UDP session.
    Upgrade(UpgradeControl),
    /// A control frame arrived on the path of its `attempt`.
    Control(QuicControl),
    /// The path of `attempt` has a connection: a client's `connect` returned,
    /// or a server's accept matched and finished its handshake.
    PathEstablished { attempt: u32 },
    /// A `Ready` or `ReadyAck` send of `attempt` ended.
    ControlSent { attempt: u32, outcome: BoundedSend },
    /// The path made for `attempt` could not read a control frame.
    ControlRefused { attempt: u32, error: ControlError },
    /// The key proved on the path of `attempt` is not the one on record.
    IdentityMismatch { attempt: u32 },
    /// `allow_peer`, the dial or the connection of `attempt` failed. The kind
    /// comes from `QuicEndpoint::error_kind`.
    PathFailed { attempt: u32, kind: QuicErrorKind },
    /// `QuicAvailabilityWatch::not_up` returned.
    EndpointNotUp(QuicAvailability),
    /// The UDP session is gone or replaced. Its path ends with it.
    SessionEnded,
}

/// What to do, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum Action {
    /// `allow_peer` for the promoted key, then, only if that worked, send
    /// this `Offer` and report [`Event::OfferSent`]. If it did not, report
    /// [`Event::PathFailed`].
    AllowPeerThenOffer { attempt: u32, caps: u32 },
    /// Send on the UDP session.
    SendUpgrade(UpgradeControl),
    /// Connect to the UDP session's address, pinned to the promoted key.
    Dial { attempt: u32 },
    /// Send on the path, bounded by [`CONTROL_SEND_TIMEOUT`], and report
    /// [`Event::ControlSent`].
    SendControl(QuicControl),
    /// Drop whatever `attempt` holds: the `allow_peer` permit, a dial, the
    /// connection. A connection still open closes with `code`.
    TearDown { attempt: u32, code: QuicCloseCode },
    /// Count it (e5a-6).
    Record(Outcome),
}

/// What a step decided, for telemetry, so a count comes from the decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum Outcome {
    Offered,
    /// The peer accepted this node's `Offer`.
    Accepted,
    /// This node accepted the peer's `Offer`.
    AcceptSent,
    /// The path is active on this side's evidence, as `Role`.
    Established(Role),
    /// The peer's `Decline`, `None` when the reason is one this node does
    /// not know. `uncharged` is whether the peer was spared the backoff
    /// penalty (the machine's own decision, see `on_decline`).
    Declined {
        reason: Option<DeclineReason>,
        uncharged: bool,
    },
    /// This node declined.
    DeclineSent(DeclineReason),
    /// The peer below `QUIC_UPGRADE_NEGOTIATES_IN` is never offered.
    IneligibleVersion,
    NoReply,
    Timeout,
    HandshakeFailed,
    IdentityMismatch,
    /// Both sides act as server, or the keys are equal: the two records of
    /// the keys disagree.
    RoleConflict,
    AllowSetFull,
    KeyConflict,
    /// A frame this node cannot read ended the setup.
    ControlRefused,
    /// A path ended, or an attempt ended for a reason no setup outcome names.
    Fallback(FallbackCause),
}

/// Why a path ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum FallbackCause {
    Idle,
    Reset,
    Protocol,
    DowngradeReceived,
    EndpointDown,
}

/// Which side of the QUIC connection this node is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::transport) enum Role {
    Server,
    Client,
}

impl Role {
    /// The higher key is the server. Equal keys have no role.
    pub(in crate::transport) fn of(
        local: &TransportPublicKey,
        peer: &TransportPublicKey,
    ) -> Option<Self> {
        match local.cmp(peer) {
            std::cmp::Ordering::Greater => Some(Self::Server),
            std::cmp::Ordering::Less => Some(Self::Client),
            std::cmp::Ordering::Equal => None,
        }
    }
}

/// The stages of setting a path up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Server: `Offer` sent, waiting for the reply.
    Offered,
    /// Server: waiting for the client's connection. Client: dialing.
    Connecting,
    /// Connected. Server: waiting for `ReadyAck`. Client: waiting for `Ready`.
    Confirming,
}

#[derive(Debug)]
enum State {
    /// No attempt. A server offers at `retry_at`.
    Idle,
    Setup {
        phase: Phase,
        attempt: u32,
        deadline: Duration,
        _permit: QuicPathPermit,
    },
    Active {
        attempt: u32,
        since: Duration,
        _permit: QuicPathPermit,
    },
    /// The path ended with `Protocol{0x1}`. Decide at `deadline` whether to
    /// charge the peer, unless the endpoint is reported down first.
    Rechecking {
        deadline: Duration,
        active_since: Option<Duration>,
    },
    /// Final: the endpoint or the session ended, the policy refuses, the keys
    /// are equal, or the peer is below the version floor.
    Off,
}

/// Why a machine goes [`State::Off`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    /// This node's own decision: nothing is recorded and the peer is not charged.
    Local,
    /// The endpoint is down: the peer is not charged, and an attempt under way
    /// records the fallback.
    EndpointDown,
    /// The UDP session ended. An attempt under way or a path that was not
    /// stable is charged, so a session that flaps is retried less often.
    Session,
}

/// Which backoff a failure takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Penalty {
    Normal,
    Identity,
    /// Nothing is wrong with the peer: the count stays, and the next try is
    /// after [`BACKOFF_MIN`].
    Uncharged,
}

/// What a [`QuicErrorKind`] means for the attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// One stream failed. The path stays.
    NoEffect,
    EndpointDown,
    Recheck,
    Fail {
        penalty: Penalty,
        in_setup: Outcome,
        when_active: Outcome,
    },
}

/// The whole mapping from an error to what follows. Exhaustive, so a new kind
/// does not compile until it has a line here.
fn classify(kind: QuicErrorKind) -> Verdict {
    let normal = |in_setup, cause| Verdict::Fail {
        penalty: Penalty::Normal,
        in_setup,
        when_active: Outcome::Fallback(cause),
    };
    match kind {
        QuicErrorKind::Reset { .. }
        | QuicErrorKind::Stopped { .. }
        | QuicErrorKind::InvalidFrame
        | QuicErrorKind::StreamClosed => Verdict::NoEffect,
        QuicErrorKind::EndpointDown => Verdict::EndpointDown,
        QuicErrorKind::Protocol {
            code: INTERNAL_ERROR,
        } => Verdict::Recheck,
        QuicErrorKind::Protocol { .. } => normal(Outcome::HandshakeFailed, FallbackCause::Protocol),
        QuicErrorKind::ConnectionLost => normal(Outcome::HandshakeFailed, FallbackCause::Reset),
        QuicErrorKind::TimedOut => normal(Outcome::Timeout, FallbackCause::Idle),
        QuicErrorKind::Refused { reason } => match reason {
            QuicRefusal::IdentityMismatch => Verdict::Fail {
                penalty: Penalty::Identity,
                in_setup: Outcome::IdentityMismatch,
                when_active: Outcome::IdentityMismatch,
            },
            // Close codes DOWNGRADE, SEND_ABORT_LIMIT, PEER_NOT_ACKNOWLEDGING
            // and CONTROL_REFUSED: fall back for this connection only.
            QuicRefusal::Downgrade => normal(
                Outcome::Fallback(FallbackCause::DowngradeReceived),
                FallbackCause::DowngradeReceived,
            ),
            // This node's own limit, so the peer is not charged.
            QuicRefusal::AllowSetFull => Verdict::Fail {
                penalty: Penalty::Uncharged,
                in_setup: Outcome::AllowSetFull,
                when_active: Outcome::Fallback(FallbackCause::Reset),
            },
            QuicRefusal::KeyConflict => normal(Outcome::KeyConflict, FallbackCause::Reset),
            QuicRefusal::Handshake
            | QuicRefusal::Version
            | QuicRefusal::ConnectionRefused
            | QuicRefusal::UnusableKey
            | QuicRefusal::Local => normal(Outcome::HandshakeFailed, FallbackCause::Reset),
        },
    }
}

/// `base` with jitter of +/-20%, from a draw in `0.0..1.0`.
fn jittered(base: Duration, sample: f64) -> Duration {
    base.mul_f64(0.8 + 0.4 * unit(sample))
}

/// A draw forced into `0.0..=1.0`, since `Duration::mul_f64` panics on NaN.
fn unit(sample: f64) -> f64 {
    if sample.is_finite() {
        sample.clamp(0.0, 1.0)
    } else {
        0.5
    }
}

/// What a machine hands to the machine of the session that replaces it, through
/// [`Event::Promoted`], so the backoff survives the replacement. `retry_at` is
/// on the clock of [`Inputs::now`]; a time in the past means no wait. At
/// promotion a wait is cut to [`SEED_WAIT_MAX`], and a count whose `retry_at` is
/// more than [`BACKOFF_MAX`] in the past has expired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::transport) struct BackoffSeed {
    pub(in crate::transport) failures: u32,
    pub(in crate::transport) retry_at: Duration,
}

/// Whether a path that became active at `active_since` lasted [`STABLE_PATH`].
fn is_stable(cx: Inputs<'_>, active_since: Option<Duration>) -> bool {
    active_since.is_some_and(|since| cx.now.saturating_sub(since) >= STABLE_PATH)
}

/// Whether the endpoint reads as down now. A connection-scope error of a dead
/// endpoint can arrive before [`Event::EndpointNotUp`].
fn endpoint_is_down(cx: Inputs<'_>) -> bool {
    cx.policy.disabled_by() == Some(DisabledBy::Endpoint(QuicAvailability::Down))
}

/// The upgrade of one UDP session. See the module docs.
#[derive(Debug)]
pub(in crate::transport) struct UpgradeMachine {
    local_key: TransportPublicKey,
    role: Option<Role>,
    state: State,
    /// A server's last attempt id; ids start at 1.
    last_attempt: u32,
    /// A client's highest `Offer` attempt seen: anything else is stale.
    last_offer: Option<u32>,
    failures: u32,
    /// No attempt before this.
    retry_at: Duration,
    /// Whether a seed came in through [`Event::Promoted`].
    seeded: bool,
}

impl UpgradeMachine {
    pub(in crate::transport) fn new(local_key: TransportPublicKey) -> Self {
        Self {
            local_key,
            role: None,
            state: State::Idle,
            last_attempt: 0,
            last_offer: None,
            failures: 0,
            retry_at: Duration::ZERO,
            seeded: false,
        }
    }

    /// The backoff to carry to the next session's machine, through
    /// [`Event::Promoted`]. `None` until this machine has taken a seed through
    /// that event: a session that ended first holds no count of its own, and
    /// filing its zeros would replace the peer's real count.
    pub(in crate::transport) fn backoff_seed(&self) -> Option<BackoffSeed> {
        self.seeded.then_some(BackoffSeed {
            failures: self.failures,
            retry_at: self.retry_at,
        })
    }

    /// When to send the next [`Event::Tick`], if any.
    pub(in crate::transport) fn deadline(&self) -> Option<Duration> {
        match &self.state {
            State::Idle => match self.role {
                Some(Role::Server) => Some(self.retry_at),
                Some(Role::Client) | None => None,
            },
            State::Setup { deadline, .. } | State::Rechecking { deadline, .. } => Some(*deadline),
            State::Active { .. } | State::Off => None,
        }
    }

    /// Whether the path is up, on this side's evidence.
    pub(in crate::transport) fn is_active(&self) -> bool {
        match self.state {
            State::Active { .. } => true,
            State::Idle | State::Setup { .. } | State::Rechecking { .. } | State::Off => false,
        }
    }

    /// Whether this machine is finished for good.
    pub(in crate::transport) fn is_off(&self) -> bool {
        match self.state {
            State::Off => true,
            State::Idle | State::Setup { .. } | State::Active { .. } | State::Rechecking { .. } => {
                false
            }
        }
    }

    /// Take one event.
    pub(in crate::transport) fn step(&mut self, cx: Inputs<'_>, event: Event) -> Vec<Action> {
        match event {
            Event::Promoted {
                peer_key,
                remote_version,
                backoff,
            } => self.on_promoted(cx, &peer_key, remote_version, backoff),
            Event::Tick => self.on_tick(cx),
            Event::OfferSent { attempt } => {
                if self.phase_of(attempt) == Some(Phase::Offered) {
                    vec![Action::Record(Outcome::Offered)]
                } else {
                    Vec::new()
                }
            }
            Event::Upgrade(UpgradeControl::Offer { attempt, .. }) => self.on_offer(cx, attempt),
            Event::Upgrade(UpgradeControl::Accept { attempt }) => self.on_accept(cx, attempt),
            Event::Upgrade(UpgradeControl::Decline { attempt, reason }) => {
                self.on_decline(cx, attempt, reason)
            }
            Event::Control(QuicControl::Ready { attempt, .. }) => self.on_ready(cx, attempt),
            Event::Control(QuicControl::ReadyAck { attempt, .. }) => self.on_ready_ack(cx, attempt),
            Event::PathEstablished { attempt } => self.on_established(cx, attempt),
            Event::ControlSent { attempt, outcome } => self.on_sent(cx, attempt, outcome),
            Event::ControlRefused { attempt, error } => self.on_refused(cx, attempt, error),
            Event::IdentityMismatch { attempt } => {
                if self.live_attempt() != Some(attempt) {
                    return Vec::new();
                }
                self.fail(
                    cx,
                    attempt,
                    QuicCloseCode::IdentityMismatch,
                    Penalty::Identity,
                    Outcome::IdentityMismatch,
                )
            }
            Event::PathFailed { attempt, kind } => self.on_failed(cx, attempt, kind),
            Event::EndpointNotUp(availability) => match availability {
                QuicAvailability::Up => Vec::new(),
                QuicAvailability::Down => self.shut_off(cx, Ending::EndpointDown),
                QuicAvailability::Disabled | QuicAvailability::Released => {
                    self.shut_off(cx, Ending::Local)
                }
            },
            Event::SessionEnded => self.shut_off(cx, Ending::Session),
        }
    }

    fn on_promoted(
        &mut self,
        cx: Inputs<'_>,
        peer_key: &TransportPublicKey,
        remote_version: Option<(u8, u8, u16)>,
        backoff: BackoffSeed,
    ) -> Vec<Action> {
        // Off covers what `role` cannot: equal keys leave no role, and a
        // session can end before its promotion. Neither is counted again.
        if self.role.is_some() || self.is_off() {
            return Vec::new();
        }
        // Before the role's verdict, so a machine that goes `Off` here still
        // hands the backoff on.
        self.take_seed(cx, backoff);
        self.role = Role::of(&self.local_key, peer_key);
        match self.role {
            None => {
                self.shut_off(cx, Ending::Local);
                vec![Action::Record(Outcome::RoleConflict)]
            }
            Some(Role::Client) => Vec::new(),
            Some(Role::Server) if !may_offer_upgrade(remote_version) => {
                self.shut_off(cx, Ending::Local);
                vec![Action::Record(Outcome::IneligibleVersion)]
            }
            Some(Role::Server) => {
                let spread = OFFER_DELAY_MAX.saturating_sub(OFFER_DELAY_MIN);
                let first_offer = cx.now + OFFER_DELAY_MIN + spread.mul_f64(unit(cx.sample));
                // A backoff carried from the replaced session may be longer.
                self.retry_at = self.retry_at.max(first_offer);
                Vec::new()
            }
        }
    }

    fn take_seed(&mut self, cx: Inputs<'_>, seed: BackoffSeed) {
        self.seeded = true;
        let expired = cx.now.saturating_sub(seed.retry_at) > BACKOFF_MAX;
        self.failures = if expired { 0 } else { seed.failures };
        self.retry_at = seed.retry_at.min(cx.now + SEED_WAIT_MAX);
    }

    fn on_tick(&mut self, cx: Inputs<'_>) -> Vec<Action> {
        match self.state {
            State::Idle => match self.role {
                Some(Role::Server) if cx.now >= self.retry_at => self.try_offer(cx),
                Some(Role::Server | Role::Client) | None => Vec::new(),
            },
            State::Setup {
                phase,
                attempt,
                deadline,
                ..
            } => {
                if cx.now < deadline {
                    return Vec::new();
                }
                let outcome = match phase {
                    Phase::Offered => Outcome::NoReply,
                    Phase::Connecting | Phase::Confirming => Outcome::Timeout,
                };
                self.fail(
                    cx,
                    attempt,
                    QuicCloseCode::Downgrade,
                    Penalty::Normal,
                    outcome,
                )
            }
            State::Rechecking {
                deadline,
                active_since,
            } => {
                if cx.now < deadline {
                    return Vec::new();
                }
                if endpoint_is_down(cx) {
                    return self.shut_off(cx, Ending::EndpointDown);
                }
                self.state = State::Idle;
                self.charge(cx, active_since, Penalty::Normal);
                let outcome = match active_since {
                    Some(_) => Outcome::Fallback(FallbackCause::Protocol),
                    None => Outcome::HandshakeFailed,
                };
                vec![Action::Record(outcome)]
            }
            State::Active { .. } | State::Off => Vec::new(),
        }
    }

    fn try_offer(&mut self, cx: Inputs<'_>) -> Vec<Action> {
        match cx.policy.admit() {
            Err(PathRefusal::Disabled(_)) => self.shut_off(cx, Ending::Local),
            // At the limit the server does not offer.
            Err(PathRefusal::Busy) => {
                self.charge(cx, None, Penalty::Uncharged);
                Vec::new()
            }
            Ok(permit) => {
                self.last_attempt = self.last_attempt.wrapping_add(1);
                let attempt = self.last_attempt;
                self.state = State::Setup {
                    phase: Phase::Offered,
                    attempt,
                    deadline: cx.now + OFFER_TIMEOUT,
                    _permit: permit,
                };
                vec![Action::AllowPeerThenOffer { attempt, caps: 0 }]
            }
        }
    }

    fn on_offer(&mut self, cx: Inputs<'_>, attempt: u32) -> Vec<Action> {
        if self.last_offer.is_some_and(|last| attempt <= last) {
            return Vec::new();
        }
        self.last_offer = Some(attempt);
        // No connection is made for it: the peer's own deadline ends its
        // attempt, and this side's offer is unaffected.
        if self.role == Some(Role::Server) {
            return vec![Action::Record(Outcome::RoleConflict)];
        }
        let reason = match &self.state {
            State::Off => DeclineReason::Disabled,
            State::Setup { .. } | State::Active { .. } | State::Rechecking { .. } => {
                DeclineReason::NotReady
            }
            State::Idle if self.role.is_none() => DeclineReason::NotReady,
            State::Idle if !cx.policy.allowed() => DeclineReason::Disabled,
            State::Idle if cx.now < self.retry_at => DeclineReason::Backoff,
            State::Idle => match cx.policy.admit() {
                Err(PathRefusal::Disabled(_)) => DeclineReason::Disabled,
                Err(PathRefusal::Busy) => DeclineReason::Busy,
                Ok(permit) => {
                    self.state = State::Setup {
                        phase: Phase::Connecting,
                        attempt,
                        deadline: cx.now + CONNECT_TIMEOUT,
                        _permit: permit,
                    };
                    return vec![
                        Action::SendUpgrade(UpgradeControl::Accept { attempt }),
                        Action::Dial { attempt },
                        Action::Record(Outcome::AcceptSent),
                    ];
                }
            },
        };
        vec![
            Action::SendUpgrade(UpgradeControl::Decline {
                attempt,
                reason: reason.wire(),
            }),
            Action::Record(Outcome::DeclineSent(reason)),
        ]
    }

    fn on_accept(&mut self, cx: Inputs<'_>, attempt: u32) -> Vec<Action> {
        if self.phase_of(attempt) != Some(Phase::Offered) {
            return Vec::new();
        }
        self.advance(Phase::Connecting, cx.now + CONNECT_TIMEOUT);
        vec![Action::Record(Outcome::Accepted)]
    }

    fn on_decline(&mut self, cx: Inputs<'_>, attempt: u32, reason: u8) -> Vec<Action> {
        if self.phase_of(attempt) != Some(Phase::Offered) {
            return Vec::new();
        }
        let reason = DeclineReason::from_wire(reason);
        let penalty = match reason {
            // These pass by themselves, so they do not lengthen the wait. A
            // client that is busy for a while must not then wait an hour.
            Some(DeclineReason::Busy | DeclineReason::NotReady | DeclineReason::Backoff) => {
                Penalty::Uncharged
            }
            // A peer with QUIC off is asked at most hourly. An unknown reason
            // is treated the same, as it may not pass.
            Some(DeclineReason::Disabled) | None => Penalty::Normal,
        };
        self.fail(
            cx,
            attempt,
            QuicCloseCode::Downgrade,
            penalty,
            Outcome::Declined {
                reason,
                uncharged: penalty == Penalty::Uncharged,
            },
        )
    }

    fn on_established(&mut self, cx: Inputs<'_>, attempt: u32) -> Vec<Action> {
        let Some(phase) = self.phase_of(attempt) else {
            return Vec::new();
        };
        match (phase, self.role) {
            // The connection can overtake an `Accept` on the UDP session.
            (Phase::Offered | Phase::Connecting, Some(Role::Server)) => {
                self.advance(Phase::Confirming, cx.now + CONFIRM_TIMEOUT);
                vec![Action::SendControl(QuicControl::Ready {
                    attempt,
                    caps: caps::KNOWN,
                })]
            }
            (Phase::Connecting, Some(Role::Client)) => {
                self.advance(Phase::Confirming, cx.now + CONFIRM_TIMEOUT);
                Vec::new()
            }
            (Phase::Confirming, _)
            | (Phase::Offered, Some(Role::Client))
            | (Phase::Offered | Phase::Connecting, None) => Vec::new(),
        }
    }

    fn on_ready(&mut self, cx: Inputs<'_>, attempt: u32) -> Vec<Action> {
        let Some(phase) = self.phase_of(attempt) else {
            return Vec::new();
        };
        match (self.role, phase) {
            (Some(Role::Server), Phase::Offered | Phase::Connecting | Phase::Confirming) => {
                vec![Action::Record(Outcome::RoleConflict)]
            }
            // `Ready` can only come over the attempt's connection, so it also
            // proves one if the report of the connection is still on its way.
            (Some(Role::Client), Phase::Connecting | Phase::Confirming) => {
                self.activate(cx.now);
                vec![
                    Action::SendControl(QuicControl::ReadyAck {
                        attempt,
                        caps: caps::KNOWN,
                    }),
                    Action::Record(Outcome::Established(Role::Client)),
                ]
            }
            (Some(Role::Client), Phase::Offered) | (None, _) => Vec::new(),
        }
    }

    fn on_ready_ack(&mut self, cx: Inputs<'_>, attempt: u32) -> Vec<Action> {
        if self.role != Some(Role::Server) || self.phase_of(attempt) != Some(Phase::Confirming) {
            return Vec::new();
        }
        self.activate(cx.now);
        vec![Action::Record(Outcome::Established(Role::Server))]
    }

    fn on_sent(&mut self, cx: Inputs<'_>, attempt: u32, outcome: BoundedSend) -> Vec<Action> {
        if self.live_attempt() != Some(attempt) {
            return Vec::new();
        }
        match outcome {
            // A `MaybeDelivered` may have reached the peer. A deadline or a
            // close from the side still waiting settles it.
            BoundedSend::Sent | BoundedSend::MaybeDelivered(_) => Vec::new(),
            // A lost connection means what it means anywhere else. The kind
            // is not attributed yet, unlike a `PathFailed`'s, and a stop is
            // one stream's, which the deadline settles.
            BoundedSend::Failed(SendFailure::Error(
                error @ (QuicSendError::ConnectionLost(_) | QuicSendError::Stopped(_)),
            )) => {
                let availability = if endpoint_is_down(cx) {
                    QuicAvailability::Down
                } else {
                    QuicAvailability::Up
                };
                self.on_failed(cx, attempt, attribute(availability, error.into()))
            }
            BoundedSend::Failed(
                SendFailure::TimedOut
                | SendFailure::Error(
                    QuicSendError::InvalidFrame(_)
                    | QuicSendError::LengthMismatch { .. }
                    | QuicSendError::Source(_)
                    | QuicSendError::Closed,
                ),
            ) => {
                let outcome = if self.is_active() {
                    Outcome::Fallback(FallbackCause::Reset)
                } else {
                    Outcome::HandshakeFailed
                };
                self.fail(
                    cx,
                    attempt,
                    QuicCloseCode::Downgrade,
                    Penalty::Normal,
                    outcome,
                )
            }
        }
    }

    fn on_refused(&mut self, cx: Inputs<'_>, attempt: u32, error: ControlError) -> Vec<Action> {
        if self.phase_of(attempt).is_none() {
            return Vec::new();
        }
        match error {
            // The sender's frame will never be understood, so it hears at
            // once. A frame of the known version with a wrong length is a
            // broken sender, left to the setup deadline.
            ControlError::UnknownVersion(_) | ControlError::UnknownType(_) => self.fail(
                cx,
                attempt,
                QuicCloseCode::ControlRefused,
                Penalty::Normal,
                Outcome::ControlRefused,
            ),
            ControlError::Empty | ControlError::WrongLength(_) => Vec::new(),
        }
    }

    fn on_failed(&mut self, cx: Inputs<'_>, attempt: u32, kind: QuicErrorKind) -> Vec<Action> {
        if self.live_attempt() != Some(attempt) {
            return Vec::new();
        }
        match classify(kind) {
            Verdict::NoEffect => Vec::new(),
            Verdict::EndpointDown => self.shut_off(cx, Ending::EndpointDown),
            Verdict::Recheck => {
                let active_since = self.leave_attempt();
                self.state = State::Rechecking {
                    deadline: cx.now + RECHECK_TIMEOUT,
                    active_since,
                };
                vec![Action::TearDown {
                    attempt,
                    code: QuicCloseCode::Downgrade,
                }]
            }
            Verdict::Fail {
                penalty,
                in_setup,
                when_active,
            } => {
                let outcome = if self.is_active() {
                    when_active
                } else {
                    in_setup
                };
                self.fail(cx, attempt, QuicCloseCode::Downgrade, penalty, outcome)
            }
        }
    }

    /// End the live `attempt`, charge the backoff, and go back to `Idle`.
    fn fail(
        &mut self,
        cx: Inputs<'_>,
        attempt: u32,
        code: QuicCloseCode,
        penalty: Penalty,
        outcome: Outcome,
    ) -> Vec<Action> {
        let active_since = self.leave_attempt();
        self.charge(cx, active_since, penalty);
        vec![Action::TearDown { attempt, code }, Action::Record(outcome)]
    }

    /// Back to `Idle`, which returns any permit. Returns when the path that
    /// just ended became active, if it did.
    fn leave_attempt(&mut self) -> Option<Duration> {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Active { since, .. } => Some(since),
            State::Idle | State::Setup { .. } | State::Rechecking { .. } | State::Off => None,
        }
    }

    fn charge(&mut self, cx: Inputs<'_>, active_since: Option<Duration>, penalty: Penalty) {
        self.clear_count_if_stable(cx, active_since);
        let delay = match penalty {
            Penalty::Normal => {
                self.failures = self.failures.saturating_add(1);
                BACKOFF.delay_for_failures(self.failures)
            }
            Penalty::Identity => {
                self.failures = self.failures.saturating_add(1);
                IDENTITY_BACKOFF
            }
            Penalty::Uncharged => BACKOFF_MIN,
        };
        self.retry_at = self.retry_at.max(cx.now + jittered(delay, cx.sample));
    }

    fn clear_count_if_stable(&mut self, cx: Inputs<'_>, active_since: Option<Duration>) {
        if is_stable(cx, active_since) {
            self.failures = 0;
        }
    }

    /// Final: no re-offer for this session. See [`Ending`] for what each way
    /// of getting here records and charges.
    fn shut_off(&mut self, cx: Inputs<'_>, ending: Ending) -> Vec<Action> {
        let mut actions = Vec::new();
        let mut tear_down = |attempt| {
            actions.push(Action::TearDown {
                attempt,
                code: QuicCloseCode::Downgrade,
            })
        };
        // A stable path that ends here clears the count too, since the count
        // can outlive this machine (`BackoffSeed`).
        let (was_attempting, active_since) = match std::mem::replace(&mut self.state, State::Off) {
            State::Setup { attempt, .. } => {
                tear_down(attempt);
                (true, None)
            }
            State::Active { attempt, since, .. } => {
                tear_down(attempt);
                (true, Some(since))
            }
            State::Rechecking { active_since, .. } => (true, active_since),
            State::Idle | State::Off => (false, None),
        };
        self.clear_count_if_stable(cx, active_since);
        match ending {
            Ending::EndpointDown if was_attempting => {
                actions.push(Action::Record(Outcome::Fallback(
                    FallbackCause::EndpointDown,
                )));
            }
            // A peer restart replaces the UDP session at once, while the old
            // QUIC connection waits for a close or an idle timeout, so this
            // often comes first and is the only report of the failure. From
            // `Idle` a failure has already charged, and from `Off` there is
            // nothing left to charge for.
            Ending::Session
                if was_attempting && !is_stable(cx, active_since) && !endpoint_is_down(cx) =>
            {
                self.charge(cx, active_since, Penalty::Normal);
            }
            Ending::Local | Ending::EndpointDown | Ending::Session => {}
        }
        actions
    }

    /// The attempt in setup or active, if any.
    pub(in crate::transport) fn live_attempt(&self) -> Option<u32> {
        match self.state {
            State::Setup { attempt, .. } | State::Active { attempt, .. } => Some(attempt),
            State::Idle | State::Rechecking { .. } | State::Off => None,
        }
    }

    /// The phase, when `attempt` is the one in setup.
    fn phase_of(&self, attempt: u32) -> Option<Phase> {
        match self.state {
            State::Setup {
                phase,
                attempt: current,
                ..
            } if current == attempt => Some(phase),
            State::Setup { .. }
            | State::Idle
            | State::Active { .. }
            | State::Rechecking { .. }
            | State::Off => None,
        }
    }

    fn advance(&mut self, next: Phase, next_deadline: Duration) {
        match &mut self.state {
            State::Setup {
                phase, deadline, ..
            } => {
                *phase = next;
                *deadline = next_deadline;
            }
            State::Idle | State::Active { .. } | State::Rechecking { .. } | State::Off => {}
        }
    }

    fn activate(&mut self, now: Duration) {
        match std::mem::replace(&mut self.state, State::Idle) {
            State::Setup {
                attempt, _permit, ..
            } => {
                self.state = State::Active {
                    attempt,
                    since: now,
                    _permit,
                };
            }
            other
            @ (State::Idle | State::Active { .. } | State::Rechecking { .. } | State::Off) => {
                self.state = other
            }
        }
    }
}
