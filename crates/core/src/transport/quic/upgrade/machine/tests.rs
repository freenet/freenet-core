//! Every (role, state, event) of the machine. [`expected`] writes out what
//! changes; anything it does not list must change nothing, so a transition
//! nobody meant to have fails too. The other tests cover what needs more than
//! one step: backoff, deadlines, permits, close codes and the end of the
//! endpoint.

use bytes::Bytes;
use quinn::{TransportErrorCode, VarInt};

use super::*;
use crate::config::NetworkApiConfig;
use crate::transport::crypto::TransportKeypair;
use crate::transport::quic::breaker::BreakerOutcome;
use crate::transport::quic::liveness::QuicAvailabilityWatch;
use crate::transport::quic::path::{QuicSendError, SendFailure};
use crate::transport::symmetric_message::{QUIC_UPGRADE_MIN_VERSION, QUIC_UPGRADE_NEGOTIATES_IN};

const SERVER_ATTEMPT: u32 = 1;
const CLIENT_ATTEMPT: u32 = 5;
const STALE: u32 = 7;
/// With this draw, jitter multiplies by exactly 1 (and the offer delay is 6 s).
const NO_JITTER: f64 = 0.5;

fn keys() -> (TransportPublicKey, TransportPublicKey) {
    let a = TransportKeypair::new().public().clone();
    let b = TransportKeypair::new().public().clone();
    assert_ne!(a, b);
    if a > b { (a, b) } else { (b, a) }
}

/// A machine, its policy and the time.
struct Rig {
    machine: UpgradeMachine,
    policy: QuicPolicy,
    availability: tokio::sync::watch::Sender<QuicAvailability>,
    /// What `Promoted` carries: the other side's key.
    peer: TransportPublicKey,
    now: Duration,
    sample: f64,
}

impl Rig {
    fn with_config(role: Role, mode: &str, max_paths: usize) -> Self {
        let (high, low) = keys();
        let (local, peer) = match role {
            Role::Server => (high, low),
            Role::Client => (low, high),
        };
        let (availability, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let mut config: NetworkApiConfig = toml::from_str("").expect("default network config");
        config.transport_quic = Some(mode.to_string());
        config.quic_max_paths = Some(max_paths.to_string());
        Self {
            machine: UpgradeMachine::new(local),
            policy: QuicPolicy::new(&config, BreakerOutcome::NotConsulted, watch),
            availability,
            peer,
            now: Duration::from_secs(1000),
            sample: NO_JITTER,
        }
    }

    fn new(role: Role) -> Self {
        Self::with_config(role, "shadow", 4)
    }

    fn promoted(role: Role) -> Self {
        let mut rig = Self::new(role);
        rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
        rig
    }

    fn promote(&mut self, remote_version: Option<(u8, u8, u16)>) -> Vec<Action> {
        self.promote_with(remote_version, BackoffSeed::default())
    }

    fn promote_with(
        &mut self,
        remote_version: Option<(u8, u8, u16)>,
        backoff: BackoffSeed,
    ) -> Vec<Action> {
        let peer_key = self.peer.clone();
        self.step(Event::Promoted {
            peer_key,
            remote_version,
            backoff,
        })
    }

    fn step(&mut self, event: Event) -> Vec<Action> {
        let cx = Inputs {
            now: self.now,
            sample: self.sample,
            policy: &self.policy,
        };
        self.machine.step(cx, event)
    }

    fn tick_at(&mut self, now: Duration) -> Vec<Action> {
        self.now = now;
        self.step(Event::Tick)
    }

    /// Past every deadline the machine holds.
    fn tick_due(&mut self) -> Vec<Action> {
        let due = self.machine.deadline().unwrap_or(self.now);
        self.tick_at(due.max(self.now))
    }

    fn offer(&mut self, attempt: u32) -> Vec<Action> {
        self.step(Event::Upgrade(UpgradeControl::Offer { attempt, caps: 0 }))
    }

    fn in_use(&self) -> usize {
        self.policy.permits().in_use()
    }

    /// The delay until `retry_at`.
    fn backoff(&self) -> Duration {
        self.machine.retry_at.saturating_sub(self.now)
    }

    /// A path of `attempt` up on either side, from a promoted machine.
    fn activate(&mut self) -> u32 {
        if self.machine.role == Some(Role::Server) {
            self.tick_due();
            let attempt = self.machine.last_attempt;
            self.step(Event::Upgrade(UpgradeControl::Accept { attempt }));
            self.step(Event::PathEstablished { attempt });
            self.step(Event::Control(QuicControl::ReadyAck { attempt, caps: 0 }));
            attempt
        } else {
            let attempt = self
                .machine
                .last_offer
                .map_or(CLIENT_ATTEMPT, |last| last + 1);
            self.offer(attempt);
            self.step(Event::PathEstablished { attempt });
            self.step(Event::Control(QuicControl::Ready { attempt, caps: 0 }));
            attempt
        }
    }

    fn close_with(&mut self, attempt: u32, code: u64) -> Vec<Action> {
        let kind = QuicErrorKind::from(closed_by_peer(code));
        self.step(Event::PathFailed { attempt, kind })
    }

    /// A server whose attempt ends with this `Decline` reason.
    fn declined(&mut self, reason: u8) -> Vec<Action> {
        self.now = self.machine.retry_at.max(self.now);
        self.tick_due();
        assert_eq!(label(&self.machine), S::Offered);
        self.step(Event::Upgrade(UpgradeControl::Decline {
            attempt: self.machine.last_attempt,
            reason,
        }))
    }
}

fn closed_by_peer(code: u64) -> quinn::ConnectionError {
    quinn::ConnectionError::ApplicationClosed(quinn::ApplicationClose {
        error_code: VarInt::from_u64(code).unwrap(),
        reason: Bytes::new(),
    })
}

fn send_failed(error: QuicSendError) -> BoundedSend {
    BoundedSend::Failed(SendFailure::Error(error))
}

/// The states, by name, for the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S {
    Unpromoted,
    Idle,
    Offered,
    Connecting,
    Confirming,
    Active,
    Rechecking,
    Off,
}

fn label(machine: &UpgradeMachine) -> S {
    match &machine.state {
        State::Idle => match machine.role {
            Some(_) => S::Idle,
            None => S::Unpromoted,
        },
        State::Setup { phase, .. } => match phase {
            Phase::Offered => S::Offered,
            Phase::Connecting => S::Connecting,
            Phase::Confirming => S::Confirming,
        },
        State::Active { .. } => S::Active,
        State::Rechecking { .. } => S::Rechecking,
        State::Off => S::Off,
    }
}

fn attempt_of(role: Role) -> u32 {
    match role {
        Role::Server => SERVER_ATTEMPT,
        Role::Client => CLIENT_ATTEMPT,
    }
}

/// A rig in `state`.
fn reach(role: Role, state: S) -> Rig {
    if state == S::Unpromoted {
        return Rig::new(role);
    }
    let mut rig = Rig::promoted(role);
    advance_to(&mut rig, role, state);
    rig
}

/// Takes a fresh promoted machine to `state`.
fn advance_to(rig: &mut Rig, role: Role, state: S) {
    let attempt = attempt_of(role);
    let go = |rig: &mut Rig, event: fn(u32) -> Event| {
        rig.step(event(attempt));
    };
    match (role, state) {
        (_, S::Unpromoted | S::Idle) => {}
        (_, S::Off) => go(rig, |_| Event::SessionEnded),
        (Role::Server, S::Offered) => {
            rig.tick_due();
        }
        (Role::Server, S::Connecting) => {
            rig.tick_due();
            go(rig, |attempt| {
                Event::Upgrade(UpgradeControl::Accept { attempt })
            });
        }
        (Role::Server, S::Confirming) => {
            rig.tick_due();
            go(rig, |attempt| Event::PathEstablished { attempt });
        }
        (Role::Server, S::Active | S::Rechecking) | (Role::Client, S::Active | S::Rechecking) => {
            assert_eq!(rig.activate(), attempt);
            if state == S::Rechecking {
                go(rig, |attempt| Event::PathFailed {
                    attempt,
                    kind: QuicErrorKind::Protocol { code: 1 },
                });
            }
        }
        (Role::Client, S::Connecting) => {
            rig.offer(attempt);
        }
        (Role::Client, S::Confirming) => {
            rig.offer(attempt);
            go(rig, |attempt| Event::PathEstablished { attempt });
        }
        (Role::Client, S::Offered) => unreachable!("a client does not offer"),
    }
    assert_eq!(label(&rig.machine), state, "reach {role:?}");
}

const ATTEMPT_EVENTS: [&str; 26] = [
    "offer_sent",
    "accept",
    "decline",
    "decline_disabled",
    "ready",
    "ready_ack",
    "established",
    "sent_ok",
    "sent_maybe",
    "sent_failed",
    "sent_lost",
    "sent_identity",
    "sent_protocol_1",
    "sent_stopped",
    "refused_version",
    "refused_type",
    "refused_length",
    "refused_empty",
    "identity",
    "path_lost",
    "path_timed_out",
    "path_protocol",
    "path_protocol_1",
    "path_endpoint_down",
    "path_stream",
    "path_allow_set_full",
];

const OTHER_EVENTS: [&str; 10] = [
    "promoted",
    "tick",
    "offer",
    "offer_next",
    "offer_older",
    "up",
    "down",
    "released",
    "disabled",
    "session_ended",
];

fn event(name: &str, attempt: u32, peer: &TransportPublicKey) -> Event {
    let failed = |kind| Event::PathFailed { attempt, kind };
    let refused = |error| Event::ControlRefused { attempt, error };
    let sent = |error| Event::ControlSent {
        attempt,
        outcome: send_failed(error),
    };
    match name {
        "promoted" => Event::Promoted {
            peer_key: peer.clone(),
            remote_version: Some(QUIC_UPGRADE_NEGOTIATES_IN),
            backoff: BackoffSeed::default(),
        },
        "tick" => Event::Tick,
        "offer" => Event::Upgrade(UpgradeControl::Offer { attempt, caps: 0 }),
        "offer_next" => Event::Upgrade(UpgradeControl::Offer {
            attempt: attempt + 1,
            caps: 0,
        }),
        "offer_older" => Event::Upgrade(UpgradeControl::Offer {
            attempt: attempt - 1,
            caps: 0,
        }),
        "offer_sent" => Event::OfferSent { attempt },
        "accept" => Event::Upgrade(UpgradeControl::Accept { attempt }),
        "decline" => Event::Upgrade(UpgradeControl::Decline {
            attempt,
            reason: DeclineReason::Busy.wire(),
        }),
        "decline_disabled" => Event::Upgrade(UpgradeControl::Decline {
            attempt,
            reason: DeclineReason::Disabled.wire(),
        }),
        "ready" => Event::Control(QuicControl::Ready { attempt, caps: 0 }),
        "ready_ack" => Event::Control(QuicControl::ReadyAck { attempt, caps: 0 }),
        "established" => Event::PathEstablished { attempt },
        "sent_ok" => Event::ControlSent {
            attempt,
            outcome: BoundedSend::Sent,
        },
        "sent_maybe" => Event::ControlSent {
            attempt,
            outcome: BoundedSend::MaybeDelivered(
                crate::transport::quic::path::MaybeDelivered::TimedOut,
            ),
        },
        "sent_failed" => Event::ControlSent {
            attempt,
            outcome: BoundedSend::Failed(SendFailure::TimedOut),
        },
        "sent_lost" => sent(QuicSendError::ConnectionLost(quinn::ConnectionError::Reset)),
        "sent_identity" => sent(QuicSendError::ConnectionLost(closed_by_peer(u64::from(
            QuicCloseCode::IdentityMismatch.code(),
        )))),
        "sent_protocol_1" => sent(QuicSendError::ConnectionLost(
            quinn::ConnectionError::TransportError(TransportErrorCode::INTERNAL_ERROR.into()),
        )),
        "sent_stopped" => sent(QuicSendError::Stopped(0x10)),
        "refused_version" => refused(ControlError::UnknownVersion(2)),
        "refused_type" => refused(ControlError::UnknownType(9)),
        "refused_length" => refused(ControlError::WrongLength(3)),
        "refused_empty" => refused(ControlError::Empty),
        "identity" => Event::IdentityMismatch { attempt },
        "path_lost" => failed(QuicErrorKind::ConnectionLost),
        "path_timed_out" => failed(QuicErrorKind::TimedOut),
        "path_protocol" => failed(QuicErrorKind::Protocol { code: 0xa }),
        "path_protocol_1" => failed(QuicErrorKind::Protocol { code: 1 }),
        "path_endpoint_down" => failed(QuicErrorKind::EndpointDown),
        "path_stream" => failed(QuicErrorKind::StreamClosed),
        "path_allow_set_full" => failed(QuicErrorKind::Refused {
            reason: QuicRefusal::AllowSetFull,
        }),
        "up" => Event::EndpointNotUp(QuicAvailability::Up),
        "down" => Event::EndpointNotUp(QuicAvailability::Down),
        "released" => Event::EndpointNotUp(QuicAvailability::Released),
        "disabled" => Event::EndpointNotUp(QuicAvailability::Disabled),
        "session_ended" => Event::SessionEnded,
        other => panic!("no event {other}"),
    }
}

/// The next state and actions, or `None` when nothing may change.
fn expected(role: Role, state: S, name: &str) -> Option<(S, Vec<Action>)> {
    use Action::{Record, SendControl, SendUpgrade, TearDown};
    let attempt = attempt_of(role);
    let setup = matches!(state, S::Offered | S::Connecting | S::Confirming);
    let live = setup || state == S::Active;
    let tear_down = |code| TearDown { attempt, code };
    let fail = |code, outcome| Some((S::Idle, vec![tear_down(code), Record(outcome)]));
    let fail_setup_or_active = |in_setup, active| {
        fail(
            QuicCloseCode::Downgrade,
            if setup { in_setup } else { active },
        )
    };
    let fallback = |cause| Outcome::Fallback(cause);
    let decline = |attempt, reason: DeclineReason| {
        vec![
            SendUpgrade(UpgradeControl::Decline {
                attempt,
                reason: reason.wire(),
            }),
            Record(Outcome::DeclineSent(reason)),
        ]
    };
    match (role, state, name) {
        // Before a key, there is no role.
        (_, S::Unpromoted, "promoted") => Some((S::Idle, vec![])),
        (_, S::Unpromoted, "offer") => Some((state, decline(attempt, DeclineReason::NotReady))),
        (_, S::Unpromoted, "offer_next") => {
            Some((state, decline(attempt + 1, DeclineReason::NotReady)))
        }
        (_, S::Unpromoted, "offer_older") => {
            Some((state, decline(attempt - 1, DeclineReason::NotReady)))
        }

        // A server never takes an offer, in any state.
        (Role::Server, _, "offer" | "offer_next" | "offer_older") => {
            Some((state, vec![Record(Outcome::RoleConflict)]))
        }
        // A client's offers.
        (Role::Client, S::Idle, "offer" | "offer_next" | "offer_older") => {
            let attempt = match name {
                "offer" => attempt,
                "offer_next" => attempt + 1,
                _ => attempt - 1,
            };
            Some((
                S::Connecting,
                vec![
                    SendUpgrade(UpgradeControl::Accept { attempt }),
                    Action::Dial { attempt },
                    Record(Outcome::AcceptSent),
                ],
            ))
        }
        (Role::Client, S::Off, "offer") => Some((state, decline(attempt, DeclineReason::Disabled))),
        (Role::Client, S::Off, "offer_next") => {
            Some((state, decline(attempt + 1, DeclineReason::Disabled)))
        }
        (Role::Client, S::Off, "offer_older") => {
            Some((state, decline(attempt - 1, DeclineReason::Disabled)))
        }
        // `offer` and `offer_older` repeat or precede the one in progress.
        (Role::Client, _, "offer_next") => {
            Some((state, decline(attempt + 1, DeclineReason::NotReady)))
        }

        // The server's way up.
        (Role::Server, S::Idle, "tick") => Some((
            S::Offered,
            vec![Action::AllowPeerThenOffer { attempt, caps: 0 }],
        )),
        (Role::Server, S::Offered, "offer_sent") => Some((state, vec![Record(Outcome::Offered)])),
        (Role::Server, S::Offered, "accept") => {
            Some((S::Connecting, vec![Record(Outcome::Accepted)]))
        }
        (Role::Server, S::Offered, "decline") => fail(
            QuicCloseCode::Downgrade,
            Outcome::Declined {
                reason: Some(DeclineReason::Busy),
                uncharged: true,
            },
        ),
        (Role::Server, S::Offered, "decline_disabled") => fail(
            QuicCloseCode::Downgrade,
            Outcome::Declined {
                reason: Some(DeclineReason::Disabled),
                uncharged: false,
            },
        ),
        (Role::Server, S::Offered | S::Connecting, "established") => Some((
            S::Confirming,
            vec![SendControl(QuicControl::Ready {
                attempt,
                caps: caps::KNOWN,
            })],
        )),
        (Role::Server, S::Confirming, "ready_ack") => {
            Some((S::Active, vec![Record(Outcome::Established(Role::Server))]))
        }
        (Role::Server, S::Offered | S::Connecting | S::Confirming, "ready") => {
            Some((state, vec![Record(Outcome::RoleConflict)]))
        }

        // The client's way up.
        (Role::Client, S::Connecting, "established") => Some((S::Confirming, vec![])),
        (Role::Client, S::Connecting | S::Confirming, "ready") => Some((
            S::Active,
            vec![
                SendControl(QuicControl::ReadyAck {
                    attempt,
                    caps: caps::KNOWN,
                }),
                Record(Outcome::Established(Role::Client)),
            ],
        )),

        // Deadlines, on either side.
        (Role::Server, S::Offered, "tick") => fail(QuicCloseCode::Downgrade, Outcome::NoReply),
        (_, S::Connecting | S::Confirming, "tick") => {
            fail(QuicCloseCode::Downgrade, Outcome::Timeout)
        }
        // Reached from Active. The endpoint stayed up, so the peer is charged.
        (_, S::Rechecking, "tick") => Some((
            S::Idle,
            vec![Record(Outcome::Fallback(FallbackCause::Protocol))],
        )),

        // Failures of the attempt.
        (_, _, "sent_failed" | "sent_lost") if live => {
            fail_setup_or_active(Outcome::HandshakeFailed, fallback(FallbackCause::Reset))
        }
        (_, _, "sent_identity") if live => {
            fail(QuicCloseCode::Downgrade, Outcome::IdentityMismatch)
        }
        (_, _, "path_allow_set_full") if live => {
            fail_setup_or_active(Outcome::AllowSetFull, fallback(FallbackCause::Reset))
        }
        (_, _, "refused_version" | "refused_type") if setup => {
            fail(QuicCloseCode::ControlRefused, Outcome::ControlRefused)
        }
        (_, _, "identity") if live => {
            fail(QuicCloseCode::IdentityMismatch, Outcome::IdentityMismatch)
        }
        (_, _, "path_lost") if live => {
            fail_setup_or_active(Outcome::HandshakeFailed, fallback(FallbackCause::Reset))
        }
        (_, _, "path_timed_out") if live => {
            fail_setup_or_active(Outcome::Timeout, fallback(FallbackCause::Idle))
        }
        (_, _, "path_protocol") if live => {
            fail_setup_or_active(Outcome::HandshakeFailed, fallback(FallbackCause::Protocol))
        }
        (_, _, "path_protocol_1" | "sent_protocol_1") if live => {
            Some((S::Rechecking, vec![tear_down(QuicCloseCode::Downgrade)]))
        }
        (_, _, "path_endpoint_down" | "down") if live => Some((
            S::Off,
            vec![
                tear_down(QuicCloseCode::Downgrade),
                Record(fallback(FallbackCause::EndpointDown)),
            ],
        )),

        // Final events.
        (_, S::Rechecking, "down") => {
            Some((S::Off, vec![Record(fallback(FallbackCause::EndpointDown))]))
        }
        (_, _, "released" | "disabled" | "session_ended") if live => {
            Some((S::Off, vec![tear_down(QuicCloseCode::Downgrade)]))
        }
        (_, S::Unpromoted | S::Idle | S::Rechecking, "released" | "disabled" | "session_ended") => {
            Some((S::Off, vec![]))
        }
        (_, S::Unpromoted | S::Idle, "down") => Some((S::Off, vec![])),

        _ => None,
    }
}

fn holds_a_permit(state: S) -> bool {
    matches!(
        state,
        S::Offered | S::Connecting | S::Confirming | S::Active
    )
}

/// What the machine keeps besides its state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Ledger {
    failures: u32,
    retry_at: Duration,
    last_attempt: u32,
    last_offer: Option<u32>,
}

impl Ledger {
    fn of(machine: &UpgradeMachine) -> Self {
        Self {
            failures: machine.failures,
            retry_at: machine.retry_at,
            last_attempt: machine.last_attempt,
            last_offer: machine.last_offer,
        }
    }
}

/// The ledger after a row, from `before` at `now`, with no jitter. A row that
/// is not listed in [`expected`] changes nothing. Written apart from the
/// machine, with the delays as literals; every table row starts with a count
/// of 0.
fn ledger_after(
    before: Ledger,
    now: Duration,
    role: Role,
    state: S,
    name: &str,
    attempt: u32,
    want: &Option<(S, Vec<Action>)>,
) -> Ledger {
    let Some((next, actions)) = want else {
        return before;
    };
    let mut after = before;
    match name {
        "offer" => after.last_offer = Some(attempt),
        "offer_next" => after.last_offer = Some(attempt + 1),
        "offer_older" => after.last_offer = Some(attempt - 1),
        "promoted" if role == Role::Server => after.retry_at = now + Duration::from_secs(6),
        _ => {}
    }
    for action in actions {
        if let Action::AllowPeerThenOffer { attempt, .. } = action {
            after.last_attempt = *attempt;
        }
    }
    // Leaving an attempt for Idle is what takes a backoff.
    if *next == S::Idle && !matches!(state, S::Idle | S::Unpromoted) {
        let recorded = |outcome: fn(&Outcome) -> bool| {
            actions
                .iter()
                .any(|action| matches!(action, Action::Record(o) if outcome(o)))
        };
        let delay = if recorded(|o| matches!(o, Outcome::IdentityMismatch)) {
            after.failures += 1;
            3600
        } else if name == "path_allow_set_full"
            || recorded(|o| {
                matches!(
                    o,
                    Outcome::Declined {
                        reason: Some(
                            DeclineReason::Busy | DeclineReason::NotReady | DeclineReason::Backoff
                        ),
                        ..
                    }
                )
            })
        {
            60
        } else {
            after.failures += 1;
            60
        };
        after.retry_at = before.retry_at.max(now + Duration::from_secs(delay));
    }
    // A session end charges an attempt under way; every row's path is young.
    if name == "session_ended"
        && matches!(
            state,
            S::Offered | S::Connecting | S::Confirming | S::Active | S::Rechecking
        )
    {
        after.failures += 1;
        after.retry_at = before.retry_at.max(now + Duration::from_secs(60));
    }
    after
}

/// One row: the event moves the machine as [`expected`] says, the path permit
/// is held exactly while the next state needs it, and the ledger moves only as
/// [`ledger_after`] says.
fn check(role: Role, state: S, name: &str, attempt: u32, want: Option<(S, Vec<Action>)>) {
    let case = format!("{role:?} {state:?} {name} attempt {attempt}");
    let mut rig = reach(role, state);
    let before_deadline = rig.machine.deadline();
    // Past every deadline, so "tick" is due wherever it can be.
    rig.now = rig.machine.deadline().unwrap_or(rig.now).max(rig.now);
    let peer = rig.peer.clone();
    let want_ledger = ledger_after(
        Ledger::of(&rig.machine),
        rig.now,
        role,
        state,
        name,
        attempt,
        &want,
    );
    let actions = rig.step(event(name, attempt, &peer));
    assert_eq!(Ledger::of(&rig.machine), want_ledger, "{case}: ledger");
    let (next, want_actions) = want.unwrap_or_else(|| (state, Vec::new()));
    assert_eq!(actions, want_actions, "{case}: actions");
    assert_eq!(label(&rig.machine), next, "{case}: next state");
    assert_eq!(
        rig.in_use(),
        usize::from(holds_a_permit(next)),
        "{case}: permit"
    );
    if next == state && !actions.iter().any(|a| matches!(a, Action::SendControl(_))) {
        assert_eq!(rig.machine.deadline(), before_deadline, "{case}: deadline");
    }
}

const ALL_STATES: [S; 8] = [
    S::Unpromoted,
    S::Idle,
    S::Offered,
    S::Connecting,
    S::Confirming,
    S::Active,
    S::Rechecking,
    S::Off,
];

fn pairs() -> impl Iterator<Item = (Role, S)> {
    [Role::Server, Role::Client].into_iter().flat_map(|role| {
        ALL_STATES
            .into_iter()
            .filter(move |state| (role, *state) != (Role::Client, S::Offered))
            .map(move |state| (role, state))
    })
}

/// Every event in every state, for the current attempt.
#[test]
fn every_transition_for_the_current_attempt() {
    for (role, state) in pairs() {
        for name in ATTEMPT_EVENTS.into_iter().chain(OTHER_EVENTS) {
            check(
                role,
                state,
                name,
                attempt_of(role),
                expected(role, state, name),
            );
        }
    }
}

/// Every event that names an attempt, for an attempt that is not the current
/// one, changes nothing in any state.
#[test]
fn a_stale_attempt_changes_nothing() {
    for (role, state) in pairs() {
        for name in ATTEMPT_EVENTS {
            check(role, state, name, attempt_of(role) + STALE, None);
            check(role, state, name, attempt_of(role) - 1, None);
        }
    }
}

/// Running both sides against each other reaches Active on both, each on its
/// own evidence: the client on `Ready`, the server on `ReadyAck`.
#[test]
fn a_server_and_a_client_reach_active_together() {
    let mut server = Rig::promoted(Role::Server);
    let mut client = Rig::promoted(Role::Client);

    let actions = server.tick_due();
    assert_eq!(
        actions[0],
        Action::AllowPeerThenOffer {
            attempt: 1,
            caps: 0
        }
    );
    let actions = client.offer(1);
    assert_eq!(
        actions[..2],
        [
            Action::SendUpgrade(UpgradeControl::Accept { attempt: 1 }),
            Action::Dial { attempt: 1 },
        ]
    );
    server.step(Event::Upgrade(UpgradeControl::Accept { attempt: 1 }));
    client.step(Event::PathEstablished { attempt: 1 });
    let actions = server.step(Event::PathEstablished { attempt: 1 });
    let [Action::SendControl(ready)] = actions[..] else {
        panic!("server sends Ready: {actions:?}");
    };
    assert_eq!(ready.attempt(), 1);
    assert!(!client.machine.is_active());
    let actions = client.step(Event::Control(ready));
    let Action::SendControl(ack) = actions[0] else {
        panic!("client sends ReadyAck: {actions:?}");
    };
    assert!(client.machine.is_active());
    assert!(!server.machine.is_active());
    server.step(Event::Control(ack));
    assert!(server.machine.is_active());
    assert_eq!(server.machine.deadline(), None);
    assert_eq!(client.machine.deadline(), None);
}

/// The higher key is the server: it offers and the lower never does, at any
/// time. Equal keys have no role and never upgrade.
#[test]
fn the_higher_key_is_the_server() {
    let (high, low) = keys();
    assert_eq!(Role::of(&high, &low), Some(Role::Server));
    assert_eq!(Role::of(&low, &high), Some(Role::Client));
    assert_eq!(Role::of(&high, &high), None);

    let mut client = Rig::promoted(Role::Client);
    assert_eq!(client.machine.deadline(), None);
    assert_eq!(client.tick_at(Duration::from_secs(1 << 30)), vec![]);

    let mut rig = Rig::new(Role::Server);
    rig.peer = rig.machine.local_key.clone();
    let actions = rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
    assert_eq!(actions, vec![Action::Record(Outcome::RoleConflict)]);
    assert!(rig.machine.is_off());
}

/// A transport holding no peer key is not eligible: it has no deadline, never
/// offers however long it waits, and declines an offer. It takes the key from
/// promotion only, and a later promotion cannot change its role.
#[test]
fn no_peer_key_means_no_upgrade() {
    for role in [Role::Server, Role::Client] {
        let mut rig = Rig::new(role);
        assert_eq!(rig.machine.deadline(), None);
        assert_eq!(rig.tick_at(Duration::from_secs(1 << 30)), vec![]);
        assert_eq!(rig.in_use(), 0);
        let actions = rig.offer(1);
        assert_eq!(
            actions[0],
            Action::SendUpgrade(UpgradeControl::Decline {
                attempt: 1,
                reason: DeclineReason::NotReady.wire(),
            })
        );
        assert_eq!(rig.in_use(), 0);
    }

    let mut rig = Rig::promoted(Role::Server);
    let other = rig.machine.local_key.clone();
    assert_eq!(rig.machine.role, Some(Role::Server));
    rig.peer = other; // Equal to the local key: would be no role if it applied.
    assert_eq!(rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN)), vec![]);
    assert_eq!(rig.machine.role, Some(Role::Server));
    assert!(!rig.machine.is_off());
}

/// A server offers only to a peer known to be at or above the floor, and
/// counts the rest apart. A client may take an offer from any version.
#[test]
fn the_version_floor_gates_the_offer() {
    let below_floor = [None, Some(QUIC_UPGRADE_MIN_VERSION), Some((0, 0, 1))];
    for version in below_floor {
        let mut rig = Rig::new(Role::Server);
        assert_eq!(
            rig.promote(version),
            vec![Action::Record(Outcome::IneligibleVersion)],
            "{version:?}"
        );
        assert_eq!(rig.tick_at(Duration::from_secs(1 << 30)), vec![]);
        assert_eq!(rig.in_use(), 0);

        let mut client = Rig::new(Role::Client);
        assert_eq!(client.promote(version), vec![]);
        assert_eq!(client.machine.deadline(), None);
        assert_eq!(client.offer(1)[2], Action::Record(Outcome::AcceptSent));
    }
    let mut rig = Rig::new(Role::Server);
    assert_eq!(rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN)), vec![]);
    assert!(rig.machine.deadline().is_some());
}

/// The first offer comes 2 to 10 s after promotion, by the draw.
#[test]
fn the_first_offer_waits_for_its_delay() {
    for (sample, delay) in [
        (0.0, OFFER_DELAY_MIN),
        (NO_JITTER, Duration::from_secs(6)),
        (1.0, OFFER_DELAY_MAX),
        (-3.0, OFFER_DELAY_MIN),
        (f64::NAN, Duration::from_secs(6)),
    ] {
        let mut rig = Rig::new(Role::Server);
        rig.sample = sample;
        rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
        let promoted_at = rig.now;
        assert_eq!(rig.machine.deadline(), Some(promoted_at + delay));
        assert_eq!(
            rig.tick_at(promoted_at + delay - Duration::from_nanos(1)),
            vec![]
        );
        assert_eq!(label(&rig.machine), S::Idle);
        rig.tick_at(promoted_at + delay);
        assert_eq!(label(&rig.machine), S::Offered);
    }
}

/// Each stage has its own timeout, restarted when the stage begins. It does
/// not fire a nanosecond early, and does at the deadline.
#[test]
fn each_stage_times_out_at_its_deadline() {
    let accept = |attempt| Event::Upgrade(UpgradeControl::Accept { attempt });
    let established = |attempt| Event::PathEstablished { attempt };
    type Stage = fn(u32) -> Event;
    let cases: Vec<(Role, Vec<Stage>, Duration)> = vec![
        (Role::Server, vec![], OFFER_TIMEOUT),
        (Role::Server, vec![accept], CONNECT_TIMEOUT),
        (Role::Server, vec![accept, established], CONFIRM_TIMEOUT),
        (Role::Client, vec![], CONNECT_TIMEOUT),
        (Role::Client, vec![established], CONFIRM_TIMEOUT),
    ];
    for (role, stages, timeout) in cases {
        let case = format!("{role:?}, {} stages", stages.len());
        let mut rig = Rig::promoted(role);
        let attempt = match role {
            Role::Server => {
                rig.tick_due();
                SERVER_ATTEMPT
            }
            Role::Client => {
                rig.offer(CLIENT_ATTEMPT);
                CLIENT_ATTEMPT
            }
        };
        for stage in stages {
            rig.now += Duration::from_secs(1);
            rig.step(stage(attempt));
        }
        let deadline = rig.now + timeout;
        assert_eq!(rig.machine.deadline(), Some(deadline), "{case}");
        assert_eq!(
            rig.tick_at(deadline - Duration::from_nanos(1)),
            vec![],
            "{case}"
        );
        assert_eq!(rig.in_use(), 1, "{case}");
        assert!(rig.tick_at(deadline).contains(&Action::TearDown {
            attempt,
            code: QuicCloseCode::Downgrade,
        }));
        assert_eq!(rig.in_use(), 0, "{case}");
        assert_eq!(rig.backoff(), BACKOFF_MIN, "{case}");
    }
}

/// Each close code the peer can send, through the real error mapping, ends an
/// active path on both roles: fallback, normal backoff, permit back, and then
/// a new upgrade. Never a permanent downgrade. A code this node does not know
/// is what an older node sees for a newer code, and behaves the same.
#[test]
fn every_close_code_falls_back_then_upgrades_again() {
    let unknown = u64::from(u32::MAX);
    let codes = QuicCloseCode::ALL
        .iter()
        .map(|code| u64::from(code.code()))
        .chain([unknown]);
    for code in codes {
        for role in [Role::Server, Role::Client] {
            let case = format!("{role:?} close code {code:#x}");
            let mut rig = Rig::promoted(role);
            let first = rig.activate();
            let actions = rig.close_with(first, code);
            assert_eq!(rig.in_use(), 0, "{case}");
            assert_eq!(label(&rig.machine), S::Idle, "{case}");
            assert!(
                actions.contains(&Action::TearDown {
                    attempt: first,
                    code: QuicCloseCode::Downgrade,
                }),
                "{case}"
            );
            let want = if code == u64::from(QuicCloseCode::IdentityMismatch.code()) {
                IDENTITY_BACKOFF
            } else {
                BACKOFF_MIN
            };
            assert_eq!(rig.backoff(), want, "{case}");
            rig.now = rig.machine.retry_at;
            let second = rig.activate();
            assert!(second > first, "{case}");
            assert!(rig.machine.is_active(), "{case}");
        }
    }
}

/// Close codes 1, 3, 4 and 5 read as a downgrade, and record that cause. An
/// unknown one, which is how an older node reads code 5, is a lost connection.
#[test]
fn close_codes_record_their_cause() {
    let downgrade = Outcome::Fallback(FallbackCause::DowngradeReceived);
    let lost = Outcome::Fallback(FallbackCause::Reset);
    for (code, want) in [
        (QuicCloseCode::NodeDroppedPeer.code(), lost),
        (QuicCloseCode::Downgrade.code(), downgrade),
        (
            QuicCloseCode::IdentityMismatch.code(),
            Outcome::IdentityMismatch,
        ),
        (QuicCloseCode::SendAbortLimit.code(), downgrade),
        (QuicCloseCode::PeerNotAcknowledging.code(), downgrade),
        (QuicCloseCode::ControlRefused.code(), downgrade),
        (0x0f, lost),
    ] {
        let mut rig = Rig::promoted(Role::Server);
        let attempt = rig.activate();
        let actions = rig.close_with(attempt, u64::from(code));
        assert_eq!(actions[1], Action::Record(want), "code {code:#x}");
    }
}

/// A refused frame closes at once rather than at the deadline, with the new
/// code and a normal backoff, on both roles; a frame of the known version with
/// a wrong length waits for the deadline; and once active, a frame a newer
/// peer may send leaves the path alone.
#[test]
fn a_refused_control_frame_closes_only_during_setup() {
    for role in [Role::Server, Role::Client] {
        for error in [
            ControlError::UnknownVersion(2),
            ControlError::UnknownType(7),
        ] {
            let mut rig = reach(role, S::Confirming);
            let attempt = attempt_of(role);
            let actions = rig.step(Event::ControlRefused { attempt, error });
            assert_eq!(
                actions,
                vec![
                    Action::TearDown {
                        attempt,
                        code: QuicCloseCode::ControlRefused,
                    },
                    Action::Record(Outcome::ControlRefused),
                ]
            );
            assert_eq!(rig.backoff(), BACKOFF_MIN);
            assert_eq!(rig.in_use(), 0);

            let mut rig = reach(role, S::Active);
            assert_eq!(rig.step(Event::ControlRefused { attempt, error }), vec![]);
            assert!(rig.machine.is_active());
        }
    }
}

/// `Protocol{0x1}` costs the peer nothing while the endpoint may be the cause:
/// the permit returns at once, and for up to a second the machine waits. If
/// the endpoint is reported or read as down there is no backoff; if a second
/// passes with it up, the peer is charged a normal backoff.
#[test]
fn protocol_0x1_rechecks_the_endpoint_before_charging_the_peer() {
    let protocol_1 = QuicErrorKind::Protocol { code: 1 };
    for start in [S::Connecting, S::Confirming, S::Active] {
        let attempt = attempt_of(Role::Client);

        // Reported down inside the window.
        let mut rig = reach(Role::Client, start);
        rig.step(Event::PathFailed {
            attempt,
            kind: protocol_1,
        });
        assert_eq!(rig.in_use(), 0);
        assert_eq!(rig.machine.deadline(), Some(rig.now + RECHECK_TIMEOUT));
        rig.now += RECHECK_TIMEOUT - Duration::from_nanos(1);
        assert_eq!(rig.step(Event::Tick), vec![]);
        assert_eq!(label(&rig.machine), S::Rechecking);
        let actions = rig.step(Event::EndpointNotUp(QuicAvailability::Down));
        assert_eq!(
            actions,
            vec![Action::Record(Outcome::Fallback(
                FallbackCause::EndpointDown
            ))]
        );
        assert!(rig.machine.is_off());
        assert_eq!(rig.machine.failures, 0, "no charge");
        assert_eq!(rig.machine.retry_at, Duration::ZERO, "no charge");

        // Read as down when the second is up, with no report yet.
        let mut rig = reach(Role::Client, start);
        rig.step(Event::PathFailed {
            attempt,
            kind: protocol_1,
        });
        rig.availability.send(QuicAvailability::Down).unwrap();
        rig.now += RECHECK_TIMEOUT;
        let actions = rig.step(Event::Tick);
        assert_eq!(
            actions,
            vec![Action::Record(Outcome::Fallback(
                FallbackCause::EndpointDown
            ))]
        );
        assert!(rig.machine.is_off());
        assert_eq!(rig.machine.failures, 0, "no charge");

        // Up when the second is up.
        let mut rig = reach(Role::Client, start);
        rig.step(Event::PathFailed {
            attempt,
            kind: protocol_1,
        });
        rig.now += RECHECK_TIMEOUT;
        let want = if start == S::Active {
            Outcome::Fallback(FallbackCause::Protocol)
        } else {
            Outcome::HandshakeFailed
        };
        assert_eq!(rig.step(Event::Tick), vec![Action::Record(want)]);
        assert_eq!(label(&rig.machine), S::Idle);
        assert_eq!(rig.machine.failures, 1);
        assert_eq!(rig.backoff(), BACKOFF_MIN);
    }

    // Any other code is charged at once, with no wait.
    let mut rig = reach(Role::Client, S::Active);
    rig.step(Event::PathFailed {
        attempt: attempt_of(Role::Client),
        kind: QuicErrorKind::Protocol { code: 2 },
    });
    assert_eq!(label(&rig.machine), S::Idle);
    assert_eq!(rig.machine.failures, 1);
}

/// Down is final from every state: no deadline, no offer at any later time,
/// no charge, and an offer gets `Disabled`. Every permit comes back, and a
/// stale report of the old path does nothing.
#[test]
fn down_is_terminal() {
    for (role, state) in pairs() {
        let case = format!("{role:?} {state:?}");
        let mut rig = reach(role, state);
        let failures = rig.machine.failures;
        let retry_at = rig.machine.retry_at;
        let was_off = state == S::Off;
        rig.step(Event::EndpointNotUp(QuicAvailability::Down));
        if !was_off {
            assert!(rig.machine.is_off(), "{case}");
        }
        assert_eq!(rig.in_use(), 0, "{case}");
        assert_eq!(rig.machine.failures, failures, "{case}");
        assert_eq!(rig.machine.retry_at, retry_at, "{case}");
        assert_eq!(rig.machine.deadline(), None, "{case}");
        assert_eq!(rig.tick_at(rig.now + BACKOFF_MAX * 10), vec![], "{case}");
        let peer = rig.peer.clone();
        for name in ATTEMPT_EVENTS {
            assert_eq!(
                rig.step(event(name, attempt_of(role), &peer)),
                vec![],
                "{case} {name}"
            );
        }
        rig.step(Event::Upgrade(UpgradeControl::Offer {
            attempt: 1000,
            caps: 0,
        }));
        assert!(rig.machine.is_off(), "{case}");
        assert_eq!(rig.in_use(), 0, "{case}");
    }

    let mut rig = Rig::new(Role::Client);
    rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
    rig.step(Event::EndpointNotUp(QuicAvailability::Down));
    assert_eq!(
        rig.offer(1)[0],
        Action::SendUpgrade(UpgradeControl::Decline {
            attempt: 1,
            reason: DeclineReason::Disabled.wire(),
        })
    );
}

/// `PathFailed` with `EndpointDown` is the same as the watch reporting Down.
#[test]
fn endpoint_down_as_an_error_is_terminal_and_free() {
    let mut rig = reach(Role::Server, S::Active);
    rig.step(Event::PathFailed {
        attempt: SERVER_ATTEMPT,
        kind: QuicErrorKind::EndpointDown,
    });
    assert!(rig.machine.is_off());
    assert_eq!(rig.machine.failures, 0);
    assert_eq!(rig.in_use(), 0);
}

/// A policy that refuses: a server shuts off at its first offer without taking
/// a permit, and a client declines with `Disabled`, for each cause.
#[test]
fn a_refusing_policy_stops_the_server_and_declines_for_the_client() {
    let mut rig = Rig::with_config(Role::Server, "off", 4);
    rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
    assert_eq!(rig.tick_due(), vec![]);
    assert!(rig.machine.is_off());

    let mut rig = Rig::promoted(Role::Server);
    rig.availability.send(QuicAvailability::Down).unwrap();
    assert_eq!(rig.tick_due(), vec![]);
    assert!(rig.machine.is_off());
    assert_eq!(rig.machine.failures, 0);

    let refused = [
        Rig::with_config(Role::Client, "off", 4),
        Rig::with_config(Role::Client, "shadow", 0),
    ];
    for mut rig in refused {
        rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
        assert_eq!(
            rig.offer(1)[1],
            Action::Record(Outcome::DeclineSent(DeclineReason::Disabled))
        );
        assert_eq!(rig.in_use(), 0);
    }
}

/// At the path limit a server does not offer and a client declines `Busy`.
/// Neither is charged, and each works once a permit is free.
#[test]
fn at_the_path_limit_no_one_is_charged() {
    let mut rig = Rig::with_config(Role::Server, "shadow", 1);
    rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
    let held = rig.policy.admit().expect("the one permit");
    assert_eq!(rig.tick_due(), vec![]);
    assert_eq!(label(&rig.machine), S::Idle);
    assert_eq!(rig.machine.failures, 0);
    assert_eq!(rig.backoff(), BACKOFF_MIN);
    drop(held);
    rig.tick_due();
    assert_eq!(label(&rig.machine), S::Offered);

    let mut rig = Rig::with_config(Role::Client, "shadow", 1);
    rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
    let held = rig.policy.admit().expect("the one permit");
    assert_eq!(
        rig.offer(1)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::Busy))
    );
    assert_eq!(rig.machine.failures, 0);
    drop(held);
    assert_eq!(rig.offer(2)[2], Action::Record(Outcome::AcceptSent));
}

/// A client in backoff declines, and takes the next offer once it is over.
/// An offer it has already seen gets no reply, in any state.
#[test]
fn a_client_declines_while_backing_off_and_ignores_old_offers() {
    let mut rig = reach(Role::Client, S::Connecting);
    rig.tick_due();
    assert_eq!(rig.backoff(), BACKOFF_MIN);

    assert_eq!(rig.offer(CLIENT_ATTEMPT), vec![], "seen");
    assert_eq!(rig.offer(CLIENT_ATTEMPT - 1), vec![], "older");
    assert_eq!(
        rig.offer(CLIENT_ATTEMPT + 1)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::Backoff))
    );
    assert_eq!(rig.in_use(), 0);
    rig.now = rig.machine.retry_at - Duration::from_nanos(1);
    assert_eq!(
        rig.offer(CLIENT_ATTEMPT + 2)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::Backoff))
    );
    rig.now = rig.machine.retry_at;
    assert_eq!(
        rig.offer(CLIENT_ATTEMPT + 3)[2],
        Action::Record(Outcome::AcceptSent)
    );
}

/// A `Decline` reason this node does not know still ends the attempt, with no
/// stated reason.
#[test]
fn an_unknown_decline_reason_is_a_decline() {
    let mut rig = reach(Role::Server, S::Offered);
    let actions = rig.step(Event::Upgrade(UpgradeControl::Decline {
        attempt: SERVER_ATTEMPT,
        reason: 200,
    }));
    assert_eq!(
        actions[1],
        Action::Record(Outcome::Declined {
            reason: None,
            uncharged: false
        })
    );
    assert_eq!(rig.in_use(), 0);
    assert_eq!(label(&rig.machine), S::Idle);
}

/// Each `allow_peer` refusal ends the attempt with its own outcome and a
/// 60 s delay. Only a full allow-set, this node's own limit, is not charged.
#[test]
fn allow_peer_refusals_end_the_attempt() {
    for (reason, outcome, failures) in [
        (QuicRefusal::AllowSetFull, Outcome::AllowSetFull, 0),
        (QuicRefusal::KeyConflict, Outcome::KeyConflict, 1),
        (QuicRefusal::UnusableKey, Outcome::HandshakeFailed, 1),
    ] {
        let mut rig = reach(Role::Server, S::Offered);
        let actions = rig.step(Event::PathFailed {
            attempt: SERVER_ATTEMPT,
            kind: QuicErrorKind::Refused { reason },
        });
        assert_eq!(actions[1], Action::Record(outcome));
        assert_eq!(rig.machine.failures, failures);
        assert_eq!(rig.backoff(), BACKOFF_MIN);
        assert_eq!(rig.in_use(), 0);
    }
}

/// The delays double from 60 s to the 1 h cap, and stay there.
#[test]
fn backoff_doubles_to_its_cap() {
    let mut rig = Rig::promoted(Role::Server);
    for seconds in [60, 120, 240, 480, 960, 1920, 3600, 3600, 3600] {
        rig.now = rig.machine.retry_at.max(rig.now);
        rig.declined(DeclineReason::Disabled.wire());
        assert_eq!(rig.backoff(), Duration::from_secs(seconds));
    }
}

/// Jitter is +/-20%, whatever the draw, and a draw outside the range does not
/// panic. Pinned at both ends, so a narrower or wider range fails.
#[test]
fn jitter_is_bounded() {
    let base = Duration::from_secs(100);
    assert_eq!(jittered(base, 0.0), Duration::from_secs(80));
    assert_eq!(jittered(base, 1.0), Duration::from_secs(120));
    assert_eq!(jittered(base, NO_JITTER), base);
    for sample in [
        -1.0,
        0.0,
        0.1,
        0.9,
        1.0,
        2.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        let delay = jittered(base, sample);
        assert!(
            (Duration::from_secs(80)..=Duration::from_secs(120)).contains(&delay),
            "{sample}: {delay:?}"
        );

        let mut rig = reach(Role::Server, S::Offered);
        rig.sample = sample;
        rig.step(Event::Upgrade(UpgradeControl::Decline {
            attempt: SERVER_ATTEMPT,
            reason: 1,
        }));
        let backoff = rig.backoff();
        assert!(
            backoff >= BACKOFF_MIN.mul_f64(0.8) && backoff <= BACKOFF_MIN.mul_f64(1.2),
            "{sample}: {backoff:?}"
        );
    }
}

/// An identity mismatch, seen here or reported by the peer, backs off for an
/// hour on the first failure. Jitter applies. Seen here, it closes with its
/// own code. It does not shorten a longer backoff, and the upgrade still comes
/// back after it.
#[test]
fn identity_mismatch_backs_off_for_an_hour() {
    for (sample, factor) in [(0.0, 0.8), (NO_JITTER, 1.0), (1.0, 1.2)] {
        for role in [Role::Server, Role::Client] {
            let mut rig = Rig::promoted(role);
            let attempt = rig.activate();
            rig.sample = sample;
            let actions = rig.step(Event::IdentityMismatch { attempt });
            assert_eq!(
                actions,
                vec![
                    Action::TearDown {
                        attempt,
                        code: QuicCloseCode::IdentityMismatch,
                    },
                    Action::Record(Outcome::IdentityMismatch),
                ]
            );
            assert_eq!(rig.backoff(), IDENTITY_BACKOFF.mul_f64(factor), "{sample}");
            assert_eq!(rig.in_use(), 0);
            rig.now = rig.machine.retry_at;
            rig.activate();
            assert!(rig.machine.is_active());
        }
    }

    let mut rig = reach(Role::Server, S::Active);
    let held = rig.now + BACKOFF_MAX * 2;
    rig.machine.retry_at = held;
    rig.step(Event::IdentityMismatch {
        attempt: SERVER_ATTEMPT,
    });
    assert_eq!(rig.machine.retry_at, held, "not shortened");
}

/// A path that lasted [`STABLE_PATH`] clears the count when it ends. One that
/// ends sooner does not, so a path that keeps dying soon after it comes up is
/// retried less and less often.
#[test]
fn only_a_stable_path_clears_the_failure_count() {
    let mut rig = Rig::promoted(Role::Server);
    for seconds in [60, 120, 240] {
        rig.now = rig.machine.retry_at.max(rig.now);
        let attempt = rig.activate();
        rig.now += STABLE_PATH - Duration::from_nanos(1);
        rig.step(Event::PathFailed {
            attempt,
            kind: QuicErrorKind::ConnectionLost,
        });
        assert_eq!(rig.backoff(), Duration::from_secs(seconds));
    }
    rig.now = rig.machine.retry_at;
    let attempt = rig.activate();
    rig.now += STABLE_PATH;
    rig.step(Event::PathFailed {
        attempt,
        kind: QuicErrorKind::ConnectionLost,
    });
    assert_eq!(rig.machine.failures, 1);
    assert_eq!(rig.backoff(), BACKOFF_MIN);
}

/// Stream-scope errors do not end the path, which is also what the table says
/// for one of them; this covers the rest.
#[test]
fn stream_errors_leave_the_path_alone() {
    for kind in [
        QuicErrorKind::Reset { code: 1 },
        QuicErrorKind::Stopped { code: 1 },
        QuicErrorKind::InvalidFrame,
        QuicErrorKind::StreamClosed,
    ] {
        let mut rig = reach(Role::Server, S::Active);
        assert_eq!(
            rig.step(Event::PathFailed {
                attempt: SERVER_ATTEMPT,
                kind
            }),
            vec![]
        );
        assert!(rig.machine.is_active());
        assert_eq!(rig.in_use(), 1);
    }
}

/// A new attempt gets a new id, and an event for the old one is stale.
#[test]
fn attempt_ids_increase_and_the_old_one_goes_stale() {
    let mut rig = reach(Role::Server, S::Offered);
    rig.tick_due();
    assert_eq!(label(&rig.machine), S::Idle);
    rig.tick_due();
    assert_eq!(rig.machine.last_attempt, SERVER_ATTEMPT + 1);
    assert_eq!(
        rig.step(Event::Upgrade(UpgradeControl::Accept {
            attempt: SERVER_ATTEMPT
        })),
        vec![]
    );
    assert_eq!(label(&rig.machine), S::Offered);
    assert_eq!(rig.in_use(), 1);
}

/// A replaced session's machine continues the backoff of the one before it:
/// the wait, the count, and a client's refusal during the wait. A seed in the
/// past leaves the promotion's own delay.
#[test]
fn a_replaced_session_continues_the_backoff() {
    let mut old = Rig::promoted(Role::Server);
    for _ in 0..3 {
        old.declined(DeclineReason::Disabled.wire());
    }
    let seed = old.machine.backoff_seed().expect("promoted");
    assert_eq!(seed.failures, 3);
    assert_eq!(seed.retry_at, old.now + Duration::from_secs(240));

    let mut server = Rig::new(Role::Server);
    server.now = old.now;
    server.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
    assert_eq!(server.machine.deadline(), Some(seed.retry_at));
    assert_eq!(
        server.tick_at(seed.retry_at - Duration::from_nanos(1)),
        vec![]
    );
    server.declined(DeclineReason::Disabled.wire());
    assert_eq!(
        server.backoff(),
        Duration::from_secs(480),
        "the count went on"
    );

    let mut client = Rig::new(Role::Client);
    client.now = old.now;
    client.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
    assert_eq!(
        client.offer(1)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::Backoff))
    );
    client.now = seed.retry_at;
    assert_eq!(client.offer(2)[2], Action::Record(Outcome::AcceptSent));

    let mut late = Rig::new(Role::Server);
    late.now = seed.retry_at + BACKOFF_MAX;
    late.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
    assert_eq!(
        late.machine.deadline(),
        Some(late.now + Duration::from_secs(6))
    );
}

/// A seed from a clock with another epoch cannot lock a peer out: the wait is
/// cut to the longest delay with the most jitter, on both roles. The count is
/// kept then, and only expires once `retry_at` is more than the longest delay
/// in the past.
#[test]
fn a_seed_is_bounded_when_it_is_taken() {
    let now = Duration::from_secs(100_000);
    let nano = Duration::from_nanos(1);
    let promoted = |role, retry_at, failures| {
        let mut rig = Rig::new(role);
        rig.now = now;
        rig.promote_with(
            Some(QUIC_UPGRADE_NEGOTIATES_IN),
            BackoffSeed { failures, retry_at },
        );
        rig
    };
    let limit = now + Duration::from_secs(4320);
    for (retry_at, taken) in [
        (limit - nano, limit - nano),
        (limit, limit),
        (limit + nano, limit),
        (limit * 1000, limit),
    ] {
        let rig = promoted(Role::Server, retry_at, 3);
        assert_eq!(rig.machine.deadline(), Some(taken), "{retry_at:?}");
        assert_eq!(rig.machine.failures, 3, "{retry_at:?}");

        let mut client = promoted(Role::Client, retry_at, 3);
        assert_eq!(client.machine.retry_at, taken, "{retry_at:?}");
        assert_eq!(client.machine.failures, 3, "{retry_at:?}");
        client.now = taken - nano;
        assert_eq!(
            client.offer(1)[1],
            Action::Record(Outcome::DeclineSent(DeclineReason::Backoff))
        );
        client.now = taken;
        assert_eq!(client.offer(2)[2], Action::Record(Outcome::AcceptSent));
    }

    let hour_ago = now - Duration::from_secs(3600);
    for role in [Role::Server, Role::Client] {
        assert_eq!(promoted(role, now, 3).machine.failures, 3);
        assert_eq!(promoted(role, hour_ago, 3).machine.failures, 3);
        assert_eq!(promoted(role, hour_ago - nano, 3).machine.failures, 0);
        assert_eq!(promoted(role, Duration::ZERO, 3).machine.failures, 0);
    }
}

/// An inbound session answers an Offer before it has a key, so it has no seed
/// yet. The seed applies from its promotion, on a machine that has already
/// seen an Offer, and a machine the promotion turns `Off` hands it on.
#[test]
fn a_seed_arrives_with_the_promotion() {
    let mut client = Rig::new(Role::Client);
    assert_eq!(
        client.offer(1)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::NotReady))
    );
    let seed = BackoffSeed {
        failures: 2,
        retry_at: client.now + Duration::from_secs(120),
    };
    client.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
    assert_eq!(client.machine.backoff_seed(), Some(seed));
    assert_eq!(
        client.offer(2)[1],
        Action::Record(Outcome::DeclineSent(DeclineReason::Backoff))
    );

    let mut below_floor = Rig::new(Role::Server);
    let seed = BackoffSeed {
        failures: 2,
        retry_at: below_floor.now + Duration::from_secs(120),
    };
    below_floor.promote_with(None, seed);
    assert!(below_floor.machine.is_off());
    assert_eq!(below_floor.machine.backoff_seed(), Some(seed));

    let mut equal_keys = Rig::new(Role::Server);
    equal_keys.peer = equal_keys.machine.local_key.clone();
    equal_keys.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
    assert!(equal_keys.machine.is_off());
    assert_eq!(equal_keys.machine.backoff_seed(), Some(seed));
}

/// A stable path clears the count whatever ends the machine, since the count
/// outlives it. A younger one keeps the count, and only a session end adds to
/// it. The age counts to the event that ends the machine, recheck included.
#[test]
fn a_stable_path_clears_the_count_however_the_machine_ends() {
    type End = fn(&mut Rig, u32);
    let session: End = |rig, _| {
        rig.step(Event::SessionEnded);
    };
    let down: End = |rig, _| {
        rig.step(Event::EndpointNotUp(QuicAvailability::Down));
    };
    let released: End = |rig, _| {
        rig.step(Event::EndpointNotUp(QuicAvailability::Released));
    };
    let disabled: End = |rig, _| {
        rig.step(Event::EndpointNotUp(QuicAvailability::Disabled));
    };
    let down_as_error: End = |rig, attempt| {
        rig.step(Event::PathFailed {
            attempt,
            kind: QuicErrorKind::EndpointDown,
        });
    };
    let send_lost_while_down: End = |rig, attempt| {
        rig.availability.send(QuicAvailability::Down).unwrap();
        let outcome = send_failed(QuicSendError::ConnectionLost(
            quinn::ConnectionError::LocallyClosed,
        ));
        rig.step(Event::ControlSent { attempt, outcome });
    };
    let recheck_reads_down: End = |rig, _| {
        rig.availability.send(QuicAvailability::Down).unwrap();
        rig.step(Event::Tick);
    };
    let cases: [(&str, S, End); 11] = [
        ("session", S::Active, session),
        ("session", S::Rechecking, session),
        ("down", S::Active, down),
        ("down", S::Rechecking, down),
        ("released", S::Active, released),
        ("released", S::Rechecking, released),
        ("disabled", S::Active, disabled),
        ("disabled", S::Rechecking, disabled),
        ("down as an error", S::Active, down_as_error),
        ("send lost while down", S::Active, send_lost_while_down),
        ("recheck reads down", S::Rechecking, recheck_reads_down),
    ];
    let young = STABLE_PATH - Duration::from_nanos(1);
    for (name, start, end) in cases {
        for age in [STABLE_PATH, young] {
            let case = format!("{name} from {start:?} at {age:?}");
            let mut rig = Rig::new(Role::Server);
            let seed = BackoffSeed {
                failures: 5,
                retry_at: Duration::ZERO,
            };
            rig.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
            let attempt = rig.activate();
            if start == S::Rechecking {
                rig.now += age - RECHECK_TIMEOUT;
                rig.step(Event::PathFailed {
                    attempt,
                    kind: QuicErrorKind::Protocol { code: 1 },
                });
                assert_eq!(label(&rig.machine), S::Rechecking, "{case}");
                rig.now += RECHECK_TIMEOUT;
            } else {
                rig.now += age;
            }
            end(&mut rig, attempt);
            assert!(rig.machine.is_off(), "{case}");
            let (failures, backoff) = match (age == STABLE_PATH, name == "session") {
                (true, _) => (0, 0),
                (false, true) => (6, 1920),
                (false, false) => (5, 0),
            };
            assert_eq!(rig.machine.failures, failures, "{case}");
            assert_eq!(rig.backoff(), Duration::from_secs(backoff), "{case}");
        }
    }
}

/// A session that keeps ending is retried less and less often, whichever state
/// it ends in and on both roles, and a stable path starts the count again.
#[test]
fn a_flapping_session_is_retried_less_and_less_often() {
    let starts = [
        (Role::Server, S::Offered),
        (Role::Server, S::Connecting),
        (Role::Server, S::Confirming),
        (Role::Server, S::Active),
        (Role::Server, S::Rechecking),
        (Role::Client, S::Connecting),
        (Role::Client, S::Confirming),
        (Role::Client, S::Active),
        (Role::Client, S::Rechecking),
    ];
    for (role, start) in starts {
        let case = format!("{role:?} {start:?}");
        let mut seed = BackoffSeed::default();
        for seconds in [60, 120, 240, 480] {
            let mut rig = Rig::new(role);
            rig.now = rig.now.max(seed.retry_at);
            rig.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
            advance_to(&mut rig, role, start);
            assert_eq!(
                rig.step(Event::SessionEnded).len(),
                usize::from(start != S::Rechecking),
                "{case}"
            );
            assert_eq!(rig.backoff(), Duration::from_secs(seconds), "{case}");
            seed = rig.machine.backoff_seed().expect("promoted");
        }
        assert_eq!(seed.failures, 4, "{case}");

        // A path that lasted clears the count and adds no wait.
        if start == S::Active {
            let mut rig = Rig::new(role);
            rig.now = seed.retry_at;
            rig.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed);
            advance_to(&mut rig, role, start);
            rig.now += STABLE_PATH;
            rig.step(Event::SessionEnded);
            assert_eq!(rig.machine.failures, 0, "{case}");
            assert_eq!(rig.backoff(), Duration::ZERO, "{case}");
        }
    }
}

/// One failure is charged once. A session end after a failure that charged
/// adds nothing, and a late report after a session end that charged adds
/// nothing. A session end with the endpoint down charges nothing at all.
#[test]
fn a_session_end_does_not_charge_a_failure_twice() {
    let attempt_lost = |attempt| Event::PathFailed {
        attempt,
        kind: QuicErrorKind::ConnectionLost,
    };
    for role in [Role::Server, Role::Client] {
        let attempt = attempt_of(role);
        for start in [S::Connecting, S::Confirming, S::Active] {
            let case = format!("{role:?} {start:?}");

            // The failure first.
            let mut rig = reach(role, start);
            rig.step(attempt_lost(attempt));
            let charged = Ledger::of(&rig.machine);
            assert_eq!(charged.failures, 1, "{case}");
            rig.now += Duration::from_secs(1);
            assert_eq!(rig.step(Event::SessionEnded), vec![], "{case}");
            assert!(rig.machine.is_off(), "{case}");
            assert_eq!(Ledger::of(&rig.machine), charged, "{case}");

            // The session end first; the path's report comes after it.
            let mut rig = reach(role, start);
            rig.step(Event::SessionEnded);
            let charged = Ledger::of(&rig.machine);
            assert_eq!(charged.failures, 1, "{case}");
            rig.now += Duration::from_secs(1);
            assert_eq!(rig.step(attempt_lost(attempt)), vec![], "{case}");
            assert_eq!(rig.step(Event::SessionEnded), vec![], "{case}");
            assert_eq!(Ledger::of(&rig.machine), charged, "{case}");

            // The endpoint read as down when the session ends.
            let mut rig = reach(role, start);
            let before = Ledger::of(&rig.machine);
            rig.availability.send(QuicAvailability::Down).unwrap();
            rig.step(Event::SessionEnded);
            assert_eq!(Ledger::of(&rig.machine), before, "{case}");
        }

        // A recheck charges once, at its deadline or at the session's end.
        let mut rig = reach(role, S::Rechecking);
        rig.now += RECHECK_TIMEOUT;
        rig.step(Event::Tick);
        let charged = Ledger::of(&rig.machine);
        assert_eq!(charged.failures, 1, "{role:?}");
        rig.step(Event::SessionEnded);
        assert_eq!(Ledger::of(&rig.machine), charged, "{role:?}");

        let mut rig = reach(role, S::Rechecking);
        rig.step(Event::SessionEnded);
        let charged = Ledger::of(&rig.machine);
        assert_eq!(charged.failures, 1, "{role:?}");
        rig.now += RECHECK_TIMEOUT;
        assert_eq!(rig.step(Event::Tick), vec![], "{role:?}");
        assert_eq!(Ledger::of(&rig.machine), charged, "{role:?}");

        let mut rig = reach(role, S::Rechecking);
        let before = Ledger::of(&rig.machine);
        rig.availability.send(QuicAvailability::Down).unwrap();
        rig.step(Event::SessionEnded);
        assert_eq!(Ledger::of(&rig.machine), before, "{role:?}");
    }
}

/// A failed `Ready` or `ReadyAck` send takes the path its error means: a
/// peer's identity close takes the hour, `Protocol{0x1}` rechecks, and a
/// connection lost to a dead endpoint is the endpoint's and not the peer's.
#[test]
fn a_failed_send_takes_the_path_its_error_means() {
    let lost = |error| send_failed(QuicSendError::ConnectionLost(error));
    for start in [S::Confirming, S::Active] {
        let attempt = attempt_of(Role::Server);

        let mut rig = reach(Role::Server, start);
        let outcome = lost(closed_by_peer(u64::from(
            QuicCloseCode::IdentityMismatch.code(),
        )));
        rig.step(Event::ControlSent { attempt, outcome });
        assert_eq!(rig.backoff(), Duration::from_secs(3600));

        let mut rig = reach(Role::Server, start);
        let outcome = lost(quinn::ConnectionError::TransportError(
            TransportErrorCode::INTERNAL_ERROR.into(),
        ));
        rig.step(Event::ControlSent { attempt, outcome });
        assert_eq!(label(&rig.machine), S::Rechecking);
        assert_eq!(rig.machine.failures, 0);

        let mut rig = reach(Role::Server, start);
        rig.availability.send(QuicAvailability::Down).unwrap();
        let outcome = lost(quinn::ConnectionError::LocallyClosed);
        let actions = rig.step(Event::ControlSent { attempt, outcome });
        assert_eq!(
            actions,
            vec![
                Action::TearDown {
                    attempt,
                    code: QuicCloseCode::Downgrade,
                },
                Action::Record(Outcome::Fallback(FallbackCause::EndpointDown)),
            ]
        );
        assert!(rig.machine.is_off());
        assert_eq!(rig.machine.failures, 0, "no charge");

        // A stop is one stream's: the path stays, and a deadline settles it.
        let mut rig = reach(Role::Server, start);
        let outcome = send_failed(QuicSendError::Stopped(0x10));
        assert_eq!(rig.step(Event::ControlSent { attempt, outcome }), vec![]);
        assert_eq!(label(&rig.machine), start);
        assert_eq!(rig.in_use(), 1);

        // A send that never left this node is a normal failure.
        let mut rig = reach(Role::Server, start);
        let outcome = send_failed(QuicSendError::Closed);
        rig.step(Event::ControlSent { attempt, outcome });
        assert_eq!(label(&rig.machine), S::Idle);
        assert_eq!(rig.machine.failures, 1);
    }
}

/// `Busy`, `NotReady` and `Backoff` retry at the normal delay and leave the
/// count where it was, whatever it was. `Disabled` and a reason this node does
/// not know escalate.
#[test]
fn a_decline_that_names_no_fault_leaves_the_count() {
    for reason in [
        DeclineReason::Busy,
        DeclineReason::NotReady,
        DeclineReason::Backoff,
    ] {
        let mut rig = Rig::promoted(Role::Server);
        rig.declined(DeclineReason::Disabled.wire());
        rig.declined(DeclineReason::Disabled.wire());
        assert_eq!(rig.machine.failures, 2);
        for _ in 0..3 {
            rig.declined(reason.wire());
            assert_eq!(rig.machine.failures, 2, "{reason:?}");
            assert_eq!(rig.backoff(), Duration::from_secs(60), "{reason:?}");
            assert_eq!(rig.in_use(), 0);
        }
    }
    for wire in [DeclineReason::Disabled.wire(), 200] {
        let mut rig = Rig::promoted(Role::Server);
        rig.declined(wire);
        rig.declined(wire);
        assert_eq!(rig.machine.failures, 2);
        assert_eq!(rig.backoff(), Duration::from_secs(120));
    }
}

/// `Offered` waits for the report that `allow_peer` worked, so a refused one
/// is not counted as an offer, and a late or stale report counts nothing.
#[test]
fn an_offer_is_recorded_only_once_it_is_sent() {
    let mut rig = Rig::promoted(Role::Server);
    assert_eq!(
        rig.tick_due(),
        vec![Action::AllowPeerThenOffer {
            attempt: SERVER_ATTEMPT,
            caps: 0
        }]
    );
    let stale = Event::OfferSent {
        attempt: SERVER_ATTEMPT + 1,
    };
    assert_eq!(rig.step(stale), vec![]);
    let actions = rig.step(Event::PathFailed {
        attempt: SERVER_ATTEMPT,
        kind: QuicErrorKind::Refused {
            reason: QuicRefusal::KeyConflict,
        },
    });
    assert!(!actions.contains(&Action::Record(Outcome::Offered)));
    let late = Event::OfferSent {
        attempt: SERVER_ATTEMPT,
    };
    assert_eq!(rig.step(late), vec![]);

    // A report that comes after the reply is one the caller was told not to
    // make: it records nothing and moves nothing.
    let mut rig = reach(Role::Server, S::Connecting);
    let deadline = rig.machine.deadline();
    let late = Event::OfferSent {
        attempt: SERVER_ATTEMPT,
    };
    assert_eq!(rig.step(late), vec![]);
    assert_eq!(label(&rig.machine), S::Connecting);
    assert_eq!(rig.machine.deadline(), deadline);
}

/// The timeouts and windows, written out: the tests above read the constants,
/// so they would follow a change to one.
#[test]
fn timeouts_and_windows_have_these_values() {
    for (value, seconds) in [
        (OFFER_TIMEOUT, 10),
        (CONNECT_TIMEOUT, 10),
        (CONTROL_SEND_TIMEOUT, 5),
        (CONFIRM_TIMEOUT, 10),
        (RECHECK_TIMEOUT, 1),
        (BACKOFF_MIN, 60),
        (BACKOFF_MAX, 3600),
        (IDENTITY_BACKOFF, 3600),
        (SEED_WAIT_MAX, 4320),
        (STABLE_PATH, 600),
        (OFFER_DELAY_MIN, 2),
        (OFFER_DELAY_MAX, 10),
    ] {
        assert_eq!(value, Duration::from_secs(seconds));
    }
}

/// A second promotion with a key that would give the other role changes
/// nothing, on either role.
#[test]
fn a_second_promotion_with_another_key_changes_nothing() {
    let mut sorted = [
        TransportKeypair::new().public().clone(),
        TransportKeypair::new().public().clone(),
        TransportKeypair::new().public().clone(),
    ];
    sorted.sort();
    let [low, middle, high] = sorted;
    for (first, second, role) in [
        (low.clone(), high.clone(), Role::Server),
        (high, low, Role::Client),
    ] {
        let mut rig = Rig::new(role);
        rig.machine = UpgradeMachine::new(middle.clone());
        rig.peer = first;
        rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN));
        assert_eq!(rig.machine.role, Some(role));
        let deadline = rig.machine.deadline();
        let ledger = Ledger::of(&rig.machine);

        rig.peer = second;
        rig.now += Duration::from_secs(1);
        let seed = BackoffSeed {
            failures: 9,
            retry_at: rig.now + BACKOFF_MAX,
        };
        assert_eq!(
            rig.promote_with(Some(QUIC_UPGRADE_NEGOTIATES_IN), seed),
            vec![]
        );
        assert_eq!(rig.machine.role, Some(role));
        assert_eq!(rig.machine.deadline(), deadline);
        assert_eq!(Ledger::of(&rig.machine), ledger);
    }
}

/// `Off` counts a promotion's verdict once: equal keys do not record a second
/// conflict, and a session that ended first records nothing and stays `Off`.
#[test]
fn a_promotion_after_off_records_nothing() {
    let mut rig = Rig::new(Role::Server);
    rig.peer = rig.machine.local_key.clone();
    assert_eq!(
        rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN)),
        vec![Action::Record(Outcome::RoleConflict)]
    );
    assert_eq!(rig.promote(Some(QUIC_UPGRADE_NEGOTIATES_IN)), vec![]);

    for version in [None, Some(QUIC_UPGRADE_NEGOTIATES_IN)] {
        for role in [Role::Server, Role::Client] {
            let mut rig = Rig::new(role);
            rig.step(Event::SessionEnded);
            assert_eq!(rig.promote(version), vec![], "{role:?} {version:?}");
            assert!(rig.machine.is_off());
            assert_eq!(rig.machine.role, None);
            assert_eq!(rig.machine.deadline(), None);
            assert_eq!(
                rig.offer(1)[1],
                Action::Record(Outcome::DeclineSent(DeclineReason::Disabled))
            );
        }
    }
}

const SERVER: usize = 0;
const CLIENT: usize = 1;
const LATENCY: Duration = Duration::from_millis(50);
/// When a side learns of a path whose close was lost: quinn's idle timeout.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// How long after the start they must be back up, the loss being a single one.
const ACTIVE_WITHIN: Duration = Duration::from_secs(600);

/// The one message a run of [`Pair`] loses, the first time it comes up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lost {
    Nothing,
    Offer,
    Accept,
    Decline,
    Dial,
    Ready,
    ReadyAck,
    /// Once both are active, the side named closes the path, and its close
    /// is lost.
    CloseFromServer,
    CloseFromClient,
}

/// How long after a loss the two sides may still disagree. Only a lost close
/// waits for the idle timeout. The others end at a setup deadline, and a lost
/// `Decline` also waits for the late promotion [`Pair::new`] gives the client.
fn settles_within(lost: Lost) -> Duration {
    match lost {
        Lost::CloseFromServer | Lost::CloseFromClient => IDLE_TIMEOUT + Duration::from_secs(5),
        Lost::Nothing
        | Lost::Offer
        | Lost::Accept
        | Lost::Decline
        | Lost::Dial
        | Lost::Ready
        | Lost::ReadyAck => Duration::from_secs(30),
    }
}

/// Two machines joined by a wire with a fixed delay, on one clock.
struct Pair {
    rigs: [Rig; 2],
    wire: Vec<(Duration, usize, Event)>,
    lost: Lost,
    lost_at: Option<Duration>,
    /// The attempt that has a QUIC connection between the two.
    connection: Option<u32>,
}

impl Pair {
    fn new(lost: Lost, samples: [f64; 2]) -> Self {
        let mut pair = Self {
            rigs: [Rig::new(Role::Server), Rig::new(Role::Client)],
            wire: Vec::new(),
            lost,
            lost_at: None,
            connection: None,
        };
        for side in [SERVER, CLIENT] {
            pair.rigs[side].sample = samples[side];
        }
        let promoted = |rig: &Rig| Event::Promoted {
            peer_key: rig.peer.clone(),
            remote_version: Some(QUIC_UPGRADE_NEGOTIATES_IN),
            backoff: BackoffSeed::default(),
        };
        let start = pair.now();
        // A client not yet promoted is what declines, so a Decline can be lost.
        let client_at = if lost == Lost::Decline {
            start + Duration::from_secs(30)
        } else {
            start
        };
        pair.wire
            .push((client_at, CLIENT, promoted(&pair.rigs[CLIENT])));
        pair.step(SERVER, promoted(&pair.rigs[SERVER]));
        pair
    }

    fn now(&self) -> Duration {
        self.rigs[SERVER].now
    }

    fn lose(&mut self, kind: Lost) -> bool {
        let lose = self.lost == kind && self.lost_at.is_none();
        if lose {
            self.lost_at = Some(self.now());
        }
        lose
    }

    fn send(&mut self, to: usize, kind: Lost, event: Event) {
        if !self.lose(kind) {
            self.wire.push((self.now() + LATENCY, to, event));
        }
    }

    fn step(&mut self, side: usize, event: Event) {
        let peer = 1 - side;
        for action in self.rigs[side].step(event) {
            match action {
                Action::AllowPeerThenOffer { attempt, caps } => {
                    let offer = UpgradeControl::Offer { attempt, caps };
                    self.send(peer, Lost::Offer, Event::Upgrade(offer));
                    self.step(side, Event::OfferSent { attempt });
                }
                Action::SendUpgrade(message) => {
                    let kind = match message {
                        UpgradeControl::Offer { .. } => Lost::Offer,
                        UpgradeControl::Accept { .. } => Lost::Accept,
                        UpgradeControl::Decline { .. } => Lost::Decline,
                    };
                    self.send(peer, kind, Event::Upgrade(message));
                }
                Action::SendControl(frame) => {
                    let kind = match frame {
                        QuicControl::Ready { .. } => Lost::Ready,
                        QuicControl::ReadyAck { .. } => Lost::ReadyAck,
                    };
                    self.send(peer, kind, Event::Control(frame));
                }
                Action::Dial { attempt } => {
                    if !self.lose(Lost::Dial) {
                        self.connection = Some(attempt);
                        let at = self.now() + LATENCY;
                        for to in [SERVER, CLIENT] {
                            self.wire.push((at, to, Event::PathEstablished { attempt }));
                        }
                    }
                }
                Action::TearDown { attempt, code } if self.connection == Some(attempt) => {
                    self.connection = None;
                    let kind = if side == SERVER {
                        Lost::CloseFromServer
                    } else {
                        Lost::CloseFromClient
                    };
                    let (delay, kind) = if self.lose(kind) {
                        (IDLE_TIMEOUT, QuicErrorKind::TimedOut)
                    } else {
                        let close = closed_by_peer(u64::from(code.code()));
                        (LATENCY, QuicErrorKind::from(close))
                    };
                    let at = self.now() + delay;
                    self.wire
                        .push((at, peer, Event::PathFailed { attempt, kind }));
                }
                Action::TearDown { .. } | Action::Record(_) => {}
            }
        }
    }

    /// Nothing on its way, and both up or both down with no permits.
    fn settled(&self) -> bool {
        let [server, client] = &self.rigs;
        let both = |state| label(&server.machine) == state && label(&client.machine) == state;
        self.wire.is_empty()
            && ((both(S::Active) && server.in_use() == 1 && client.in_use() == 1)
                || (both(S::Idle) && server.in_use() == 0 && client.in_use() == 0))
    }

    /// Runs until nothing is left to happen, and returns when the two sides
    /// first agreed after the loss.
    fn run(&mut self) -> Option<Duration> {
        let mut settled_at = None;
        for _ in 0..10_000 {
            if self.lost_at.is_some() && settled_at.is_none() && self.settled() {
                settled_at = Some(self.now());
            }
            let closer = match self.lost {
                Lost::CloseFromServer => Some(SERVER),
                Lost::CloseFromClient => Some(CLIENT),
                Lost::Nothing
                | Lost::Offer
                | Lost::Accept
                | Lost::Decline
                | Lost::Dial
                | Lost::Ready
                | Lost::ReadyAck => None,
            };
            let both_active = self.settled() && self.rigs[SERVER].machine.is_active();
            if let (Some(side), None, true) = (closer, self.lost_at, both_active) {
                let attempt = self.connection.expect("both are active on a connection");
                let outcome = BoundedSend::Failed(SendFailure::TimedOut);
                self.step(side, Event::ControlSent { attempt, outcome });
                continue;
            }

            let on_wire = self
                .wire
                .iter()
                .enumerate()
                .min_by_key(|(_, (at, ..))| *at)
                .map(|(index, (at, ..))| (*at, index));
            let due = [SERVER, CLIENT]
                .into_iter()
                .filter_map(|side| Some((self.rigs[side].machine.deadline()?, side)))
                .min();
            let at = match (on_wire, due) {
                (None, None) => return settled_at,
                (Some((at, _)), None) | (None, Some((at, _))) => at,
                (Some((wire, _)), Some((due, _))) => wire.min(due),
            };
            let at = at.max(self.now());
            for rig in &mut self.rigs {
                rig.now = at;
            }
            match on_wire {
                Some((wire_at, index)) if wire_at <= at => {
                    let (_, to, event) = self.wire.remove(index);
                    self.step(to, event);
                }
                _ => {
                    let (_, side) = due.expect("a time came from one of the two");
                    self.step(side, Event::Tick);
                }
            }
        }
        panic!("the machines never went quiet: {:?}", self.lost);
    }
}

/// Lose each kind of message once, at several draws of the jitter. The two
/// sides end up both active or both idle with no permits within a bound, and
/// the later attempts bring the path up on both.
#[test]
fn two_machines_agree_after_any_one_lost_message() {
    for lost in [
        Lost::Nothing,
        Lost::Offer,
        Lost::Accept,
        Lost::Decline,
        Lost::Dial,
        Lost::Ready,
        Lost::ReadyAck,
        Lost::CloseFromServer,
        Lost::CloseFromClient,
    ] {
        for samples in [[NO_JITTER, NO_JITTER], [0.0, 1.0], [1.0, 0.0]] {
            let case = format!("{lost:?} {samples:?}");
            let mut pair = Pair::new(lost, samples);
            let start = pair.now();
            let settled_at = pair.run();
            if lost != Lost::Nothing {
                let lost_at = pair.lost_at.unwrap_or_else(|| panic!("{case}: not lost"));
                let settled_at = settled_at.unwrap_or_else(|| panic!("{case}: never settled"));
                assert!(
                    settled_at - lost_at <= settles_within(lost),
                    "{case}: settled {:?} after the loss",
                    settled_at - lost_at
                );
            }
            assert!(pair.settled(), "{case}: not settled at the end");
            for rig in &pair.rigs {
                assert!(rig.machine.is_active(), "{case}");
            }
            assert!(pair.now() - start <= ACTIVE_WITHIN, "{case}");
        }
    }
}

/// A machine that was never promoted has no seed to hand on, even after its
/// session ends: filing its zeros would replace the peer's real count.
#[test]
fn a_machine_that_took_no_seed_has_none_to_hand_on() {
    let mut rig = Rig::new(Role::Server);
    rig.step(Event::SessionEnded);
    assert_eq!(rig.machine.backoff_seed(), None);

    let mut rig = Rig::promoted(Role::Server);
    rig.step(Event::SessionEnded);
    assert!(rig.machine.backoff_seed().is_some());
}
