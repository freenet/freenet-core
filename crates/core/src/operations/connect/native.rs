//! The QUIC-native connect offer, the acceptor's decision, and the pair slice
//! (#5940).
//!
//! A joiner's `ConnectRequest` carries a [`NativeOffer`]; the acceptor alone
//! decides between a native QUIC connection and today's UDP setup, and its
//! `ConnectResponse` carries the [`NativeDecision`]. Both travel in the wire
//! forms of `ConnectMsg` that only peers at or above
//! [`crate::node::QUIC_NATIVE_MIN_VERSION`] receive (`connect::wire`).
//!
//! On `Native` the joiner and the acceptor each dial a native connection
//! ([`DialTransport::Native`]), and a failed setup falls back to today's UDP
//! dial. In production nothing changes yet: [`NATIVE_DIAL_READY`] is false,
//! so every offer carries percent 0 and every decision is
//! [`NativeDecision::Udp`]; a simulation turns it on per node
//! ([`NativeConnectState`]). A `Native` the joiner cannot honour fails that
//! attempt ([`joiner_response_action`]).
//!
//! The pair slice:
//!
//! ```text
//! bucket(a, b) = first 8 bytes of BLAKE3("freenet-quic-native-slice-v1" || min(a, b) || max(a, b)),
//!                big-endian, mod 100
//! ```
//!
//! It is the data slice's function (`transport::quic::upgrade::slice`) with its
//! own domain string, so the two slices pick independent pairs. The domain has
//! no per-release epoch: a pair in at 5% stays in at 25%. Pinned by a test
//! vector computed outside this code.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

use crate::transport::TransportPublicKey;
use crate::transport::metrics::ConnectArm;
use crate::transport::metrics::TRANSPORT_METRICS;

/// Whether this build can dial a native connection. False until the native
/// connection exists and is wired into the connect op; while false, every offer
/// this node sends carries percent 0 and it never decides `Native`.
pub(crate) const NATIVE_DIAL_READY: bool = false;

/// This release's share of pairs, 0 to 100, that set up a native connection
/// when both sides are ready. Each step is its own release; the acceptor's
/// release governs, since only the acceptor decides.
pub(crate) const QUIC_NATIVE_PERCENT: u8 = 0;

/// Offer level: the joiner can set up a native connection, and also UDP.
pub(crate) const LEVEL_NATIVE_CAPABLE: u8 = 1;

/// Offer level reserved for a node that has no UDP path. Never sent by this
/// release, and not read by it: no decision here depends on the level.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const LEVEL_NATIVE_ONLY: u8 = 2;

/// Strikes, across distinct acceptors, after which a joiner offers percent 0
/// for [`DEMOTION_COOLDOWN`]. A strike is a RESCUE: a native setup with that
/// acceptor failed on the path (`NativeSetupOutcome::counts_toward_demotion`)
/// and UDP to the same acceptor then connected. A native failure where UDP
/// failed too (a symmetric or otherwise incompatible pair, a peer that never
/// dialled, a peer gone) is no evidence against native and is not counted.
pub(crate) const DEMOTION_FAILURES: usize = 3;

/// Strikes, across distinct joiners, after which a node decides `Udp` as
/// acceptor for [`DEMOTION_COOLDOWN`]. A strike is a rescue, as for the joiner
/// ([`DEMOTION_FAILURES`]). Field data (0.2.162, independent-mapping nodes)
/// put a healthy acceptor's raw failure share at about 71% of the dials it
/// takes, almost all caused by the joiner (endpoint-dependent joiners, joiners
/// that never dial); counting those demoted a healthy acceptor within about
/// an hour at 25%. A rescue needs UDP to that joiner to work, which those
/// cases do not give, while an acceptor whose own native path is broken
/// collects a rescue on every pair UDP can connect.
pub(crate) const ACCEPTOR_DEMOTION_FAILURES: usize = 3;

/// How long a strike counts: a streak drops strikes older than this, so
/// strikes spread thinly over days (a node that rarely uses native) do not
/// add up to a demotion. Equal to [`DEMOTION_COOLDOWN`]: a node whose native
/// path is broken collects its strikes at its rate of UDP-rescued native
/// setups, which at 25% is several an hour for a node that accepts or joins
/// at all, so three fall well inside 6 h; and a fault that does not recur
/// within one cooldown would not have kept native off for long either.
pub(crate) const STRIKE_WINDOW: Duration = DEMOTION_COOLDOWN;

/// How long a demoted node offers (or decides) nothing native.
pub(crate) const DEMOTION_COOLDOWN: Duration = Duration::from_secs(6 * 3600);

const SLICE_DOMAIN: &[u8] = b"freenet-quic-native-slice-v1";

/// What a joiner offers in its `ConnectRequest`.
///
/// `percent` is the joiner's effective share, 0 to 100 (0 while it cannot dial
/// native). Both bytes are kept exactly as received, so a relay on this
/// release passes on a level or percent a later release sends without
/// rewriting it. They are read only where this node decides: the level not at
/// all (every level reads as [`LEVEL_NATIVE_CAPABLE`]), and the percent
/// through [`NativeOffer::effective_percent`], which caps it at 100.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NativeOffer {
    pub level: u8,
    pub percent: u8,
}

impl NativeOffer {
    /// The percent this node reads from the offer: as sent, capped at 100.
    pub(crate) fn effective_percent(self) -> u8 {
        self.percent.min(100)
    }

    /// A level-1 offer with `percent`, capped at 100.
    pub(crate) fn capable(percent: u8) -> Self {
        Self {
            level: LEVEL_NATIVE_CAPABLE,
            percent: percent.min(100),
        }
    }
}

/// The acceptor's answer, carried by its `ConnectResponse`.
///
/// Encoded as one byte: 0 is `Udp`, 1 is `Native`. Any other value is a
/// decision a later release added: it decodes (so it can never close a
/// connection to this one), a relay passes it on unchanged, and a joiner on
/// this release reads it as `Udp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "u8", into = "u8")]
pub(crate) enum NativeDecision {
    /// Set up the connection over UDP as today, reusing a transport if one
    /// exists.
    Udp,
    /// Set up a native QUIC connection.
    Native,
    /// A value this release does not know, kept as received. This node never
    /// decides it.
    Unknown(u8),
}

impl From<u8> for NativeDecision {
    fn from(wire: u8) -> Self {
        match wire {
            0 => Self::Udp,
            1 => Self::Native,
            other => Self::Unknown(other),
        }
    }
}

impl From<NativeDecision> for u8 {
    fn from(decision: NativeDecision) -> Self {
        match decision {
            NativeDecision::Udp => 0,
            NativeDecision::Native => 1,
            NativeDecision::Unknown(wire) => wire,
        }
    }
}

/// How a node encodes and decodes the QUIC-native connect variants, read by
/// each connection's listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeWire {
    /// The version a peer must have negotiated to be sent the variants.
    pub floor: (u8, u8, u16),
    /// Simulation-only pre-floor decoder mode
    /// (`NodeConfig::native_wire_pre_floor`): never emit the variants, and
    /// fail to decode them.
    pub pre_floor: bool,
}

impl NativeWire {
    pub(crate) fn new(floor_override: Option<(u8, u8, u16)>, pre_floor: bool) -> Self {
        Self {
            floor: floor_override.unwrap_or(crate::node::QUIC_NATIVE_MIN_VERSION),
            pre_floor,
        }
    }

    /// Serialize `msg` for the peer that negotiated `remote_version`.
    pub(crate) fn encode<T: Serialize>(
        self,
        msg: &T,
        remote_version: Option<(u8, u8, u16)>,
    ) -> bincode::Result<Vec<u8>> {
        if self.pre_floor {
            bincode::serialize(msg)
        } else {
            super::wire::encode_for_peer(msg, remote_version, self.floor)
        }
    }

    /// Run `decode` on received bytes, as a pre-floor release would in
    /// pre-floor mode.
    pub(crate) fn decode<R>(self, decode: impl FnOnce() -> R) -> R {
        if self.pre_floor {
            super::wire::decode_as_pre_floor(decode)
        } else {
            decode()
        }
    }
}

/// The bucket of a pair of transport keys, in `0..100`, the same whichever key
/// comes first.
pub(crate) fn native_bucket(a: &TransportPublicKey, b: &TransportPublicKey) -> u8 {
    let (low, high) = if a <= b { (a, b) } else { (b, a) };
    let mut hasher = blake3::Hasher::new();
    hasher.update(SLICE_DOMAIN);
    hasher.update(low.as_bytes());
    hasher.update(high.as_bytes());
    let mut first = [0u8; 8];
    hasher.finalize_xof().fill(&mut first);
    // Below 100, so it fits.
    (u64::from_be_bytes(first) % 100) as u8
}

/// A node's effective native percent: 0 unless the build can dial native, the
/// QUIC endpoint is up and the node is not demoted; otherwise the release's
/// percent, lowered (never raised) by a per-node override.
pub(crate) fn effective_native_percent(
    dial_ready: bool,
    endpoint_up: bool,
    demoted: bool,
    release_percent: u8,
    node_override: Option<u8>,
) -> u8 {
    if !(dial_ready && endpoint_up) || demoted {
        return 0;
    }
    let release_percent = release_percent.min(100);
    node_override.map_or(release_percent, |o| o.min(release_percent))
}

/// Which transport a `ConnectPeer` dials. `Native` sets up a QUIC-native
/// connection through `OutboundConnector::connect_native`, with no UDP
/// expectation; `Udp` is today's hole punch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DialTransport {
    #[default]
    Udp,
    /// `deadline`: when the setup must have ended, on the tokio clock. The
    /// setup turns it into what is left when it starts, so time the dial
    /// spends reaching the transport comes out of it. The setup's own window
    /// (8 s for the client, 12 s for the server) cuts it further, and a setup
    /// with less than 4 s left is not started. A joiner passes its UDP
    /// fallback time, so its native attempt never runs past it; an acceptor
    /// passes [`NATIVE_CONNECT_WITHIN`] from its decision.
    Native { deadline: tokio::time::Instant },
}

/// Which end of the CONNECT a native setup is: the joiner that dialled the
/// acceptor, or the acceptor that punched back. Any other origin (a gateway
/// dial, a ring peer) never dials native today and keeps its own label.
fn native_setup_side(origin: crate::transport::NatAttemptOrigin) -> &'static str {
    use crate::transport::NatAttemptOrigin::{
        ConnectAcceptor, ConnectJoiner, Gateway, RingPeerNoTransport,
    };
    match origin {
        ConnectJoiner => "joiner",
        ConnectAcceptor => "acceptor",
        Gateway | RingPeerNoTransport => origin.label(),
    }
}

/// The body of the `native_setup_outcome` event, one per native setup at each
/// end (gate 22), which the collector joins on `tx`: the CONNECT transaction both
/// ends' native dials carry, the remote's key (in the form of the key before
/// the `@` of a `peer_id`, so the two ends can be crossed), how this end's
/// setup ended (a `NativeSetupOutcome` label) and which end this is. No
/// address: the join needs keys only.
pub(crate) fn native_setup_outcome_body(
    transaction: &crate::message::Transaction,
    peer_key: &TransportPublicKey,
    outcome: &str,
    origin: crate::transport::NatAttemptOrigin,
) -> serde_json::Value {
    serde_json::json!({
        "tx": transaction.to_string(),
        "peer": peer_key.to_string(),
        "outcome": outcome,
        "side": native_setup_side(origin),
    })
}

impl DialTransport {
    /// A native dial bounded only by the setup's own window.
    pub(crate) fn native_now() -> Self {
        // The tokio clock, not a `TimeSource`: the deadline is read back on
        // the same runtime (`connect_outbound`), and a simulation's paused
        // runtime keeps it deterministic, as the connect op's other dial
        // timing is.
        Self::Native {
            deadline: tokio::time::Instant::now() + NATIVE_CONNECT_WITHIN,
        }
    }

    pub(crate) fn is_native(self) -> bool {
        matches!(self, Self::Native { .. })
    }
}

/// How long a native dial may wait when its caller has no deadline of its
/// own. The setup's own window is shorter and ends it first (8 s for the
/// client, 12 s for the server), so this only bounds a setup that never
/// reports.
pub(crate) const NATIVE_CONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// This node's readiness for native connections in one role: as joiner
/// ([`NativeConnectState::readiness`]) or as acceptor
/// ([`NativeConnectState::acceptor_readiness`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeReadiness {
    /// The build can dial native, the endpoint is up and the node is not
    /// demoted.
    pub ready: bool,
    /// The effective percent ([`effective_native_percent`]).
    pub percent: u8,
    /// A demotion holds native off for this readiness (other causes may as
    /// well): the joiner streak for [`NativeConnectState::readiness`], either
    /// streak for [`NativeConnectState::acceptor_readiness`].
    /// [`acceptor_arm`] then counts the pair in neither arm.
    pub demoted: bool,
}

impl NativeReadiness {
    /// Readiness of a node in this release with no transport open yet: never
    /// ready, offering 0. A node reads its own through
    /// [`NativeConnectState::readiness`].
    #[cfg(test)]
    pub(crate) fn current() -> Self {
        NativeConnectState::new(None).readiness()
    }

    #[cfg(test)]
    pub(crate) fn from_inputs(dial_ready: bool, endpoint_up: bool, demoted: bool) -> Self {
        Self::from_parts(dial_ready, endpoint_up, demoted, QUIC_NATIVE_PERCENT)
    }

    /// As [`Self::from_inputs`], at `release_percent` in place of
    /// [`QUIC_NATIVE_PERCENT`].
    pub(crate) fn from_parts(
        dial_ready: bool,
        endpoint_up: bool,
        demoted: bool,
        release_percent: u8,
    ) -> Self {
        Self {
            ready: dial_ready && endpoint_up && !demoted,
            demoted,
            percent: effective_native_percent(
                dial_ready,
                endpoint_up,
                demoted,
                release_percent,
                None,
            ),
        }
    }

    /// The offer a joiner with this readiness sends.
    pub(crate) fn offer(self) -> NativeOffer {
        NativeOffer::capable(self.percent)
    }
}

/// The acceptor's decision for a request that carried `offer`.
///
/// Native only when all of these hold: the offer reached it with
/// `percent > 0`; the acceptor is ready; the pair's bucket is below the lower
/// of the two percents; and the acceptor holds no transport to the joiner's
/// key or address (an existing transport is reused, so the answer is `Udp`).
/// A missing offer reads as percent 0.
pub(crate) fn decide_native(
    offer: Option<NativeOffer>,
    own: NativeReadiness,
    bucket: u8,
    holds_transport: bool,
) -> NativeDecision {
    if acceptor_arm(offer, own, bucket, holds_transport) == Some(ConnectArm::Treated) {
        NativeDecision::Native
    } else {
        NativeDecision::Udp
    }
}

/// The comparison arm of the acceptor's decision (QUIC-NATIVE-CONNECT 3.2),
/// from the same inputs as [`decide_native`], which takes its answer from it:
/// `Treated` is exactly a `Native` decision. `Control` is a request that
/// carried an offer, where the acceptor holds no transport to the joiner and
/// the bucket is at or above the lower of the two percents, so the pair dials
/// UDP because of the slice (every such pair while that percent is 0).
/// `None`: no offer, a transport already held (reused, so no dial is decided),
/// a pair below the percent that this node is not ready to dial, or a node
/// that is demoted (either role). A demoted node is left out of both arms:
/// demotion follows native failures, so counting its pairs as control would
/// fill the control arm with exactly the pairs where native fails.
pub(crate) fn acceptor_arm(
    offer: Option<NativeOffer>,
    own: NativeReadiness,
    bucket: u8,
    holds_transport: bool,
) -> Option<ConnectArm> {
    let offered = offer?.effective_percent();
    if holds_transport || own.demoted {
        return None;
    }
    if bucket < offered.min(own.percent.min(100)) {
        own.ready.then_some(ConnectArm::Treated)
    } else {
        Some(ConnectArm::Control)
    }
}

/// Whether the joiner's dial to an acceptor that answered `decision` is in
/// the control arm. The joiner knows its own percent, not the acceptor's, so
/// it counts a pair only when the bucket is at or above its own: that is at
/// or above the lower of the two too, so such a pair is control at the
/// acceptor as well. A pair below the joiner's percent that the acceptor sent
/// to UDP is not counted here (the acceptor's own count has it), and nor is
/// one with no decision (an acceptor or a relay below the floor).
///
/// It includes pairs the acceptor leaves out of both arms ([`acceptor_arm`]
/// gives `None`), which the joiner cannot see: those where it already held a
/// transport to the joiner and reused it without dialling, and those decided
/// while it was demoted. It also includes every UDP-answered pair of a
/// demoted joiner, whose offer reads percent 0. So the acceptor's counters
/// are the ones for rates.
pub(crate) fn joiner_in_control(
    sent: Option<NativeOffer>,
    decision: Option<NativeDecision>,
    bucket: u8,
) -> bool {
    matches!(decision, Some(NativeDecision::Udp))
        && sent.is_some_and(|offer| bucket >= offer.effective_percent())
}

/// What an acceptor that has no UDP path answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum NoUdpAcceptorAnswer {
    /// Accept and decide as usual.
    Decide,
    /// Answer `Rejected`, so the joiner routes elsewhere at once instead of
    /// punching UDP at a node that cannot answer.
    Rejected,
}

/// The rule for an acceptor without UDP: a request with no offer (a joiner
/// that cannot go native, or one whose offer an older relay dropped) is
/// rejected. Not wired: every node in this release has UDP.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn no_udp_acceptor_answer(
    offer: Option<NativeOffer>,
    udp_available: bool,
) -> NoUdpAcceptorAnswer {
    if offer.is_none() && !udp_available {
        NoUdpAcceptorAnswer::Rejected
    } else {
        NoUdpAcceptorAnswer::Decide
    }
}

/// Why a joiner could not use a response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinerDecisionFailure {
    /// `Native` answered a request that carried no offer, or percent 0. Only a
    /// bug or a misbehaving relay produces it.
    DecisionWithoutOffer,
    /// `Native` answered a real offer, but the joiner can no longer dial
    /// native (demoted, or its QUIC endpoint is not up).
    NativeUnsupported,
}

/// How the joiner dials an acceptor, given the offer it sent, the response's
/// decision and whether it can dial native now (`can_dial`, its current
/// readiness). `None`, `Udp` and a decision this release does not know keep
/// today's UDP dial; `Native` dials native when the joiner offered and can
/// still dial, and fails the attempt otherwise.
pub(crate) fn joiner_response_action(
    sent: Option<NativeOffer>,
    decision: Option<NativeDecision>,
    can_dial: bool,
) -> Result<DialTransport, JoinerDecisionFailure> {
    match decision {
        None | Some(NativeDecision::Udp | NativeDecision::Unknown(_)) => Ok(DialTransport::Udp),
        Some(NativeDecision::Native) => {
            if sent.is_none_or(|o| o.effective_percent() == 0) {
                Err(JoinerDecisionFailure::DecisionWithoutOffer)
            } else if !can_dial {
                Err(JoinerDecisionFailure::NativeUnsupported)
            } else {
                Ok(DialTransport::native_now())
            }
        }
    }
}

/// A node's QUIC-native connect state: whether it may dial native and at what
/// percent, and the demotion its native failures drive. One per node, in the
/// `ConnectionManager`.
#[derive(Debug)]
pub(crate) struct NativeConnectState {
    /// Simulation only: native dials on, at this release percent, in place of
    /// [`NATIVE_DIAL_READY`] and [`QUIC_NATIVE_PERCENT`]
    /// (`NodeConfig::native_connect_override`). `None` in production.
    release_override: Option<u8>,
    /// Driven by native setups this node made as joiner. Lowers its offer
    /// and, through [`Self::acceptor_readiness`], its decisions as acceptor
    /// too.
    demotion: parking_lot::Mutex<NativeDemotion>,
    /// Driven only by rescues of native setups this node made as acceptor,
    /// across distinct joiners. Lowers only its decisions as acceptor
    /// ([`Self::acceptor_readiness`]): an acceptor whose own native path is
    /// broken (a gateway, a node at its target that rarely joins) would
    /// otherwise keep deciding `Native` for every joiner, and each of those
    /// joiners would wait out the fallback (and take a strike against it,
    /// unless the acceptor is one of its configured gateways, whose dials are
    /// labelled `Gateway`).
    acceptor_demotion: parking_lot::Mutex<NativeDemotion>,
    /// Simulation only: this node never offers native as joiner, but decides
    /// as acceptor as usual (`NodeConfig::native_joiner_off`), standing in
    /// for a node that rarely joins. `false` in production.
    joiner_off: bool,
    /// This node's QUIC endpoint, set when its transport opens. Until then,
    /// and when it is not up, the node cannot dial native. Read per node, not
    /// from the process-wide endpoint metric, which several simulated nodes
    /// in one process share.
    endpoint: std::sync::OnceLock<crate::transport::QuicAvailabilityWatch>,
}

impl NativeConnectState {
    pub(crate) fn new(release_override: Option<u8>) -> Self {
        Self {
            release_override,
            demotion: parking_lot::Mutex::new(NativeDemotion::new(DEMOTION_FAILURES)),
            acceptor_demotion: parking_lot::Mutex::new(NativeDemotion::new(
                ACCEPTOR_DEMOTION_FAILURES,
            )),
            joiner_off: false,
            endpoint: std::sync::OnceLock::new(),
        }
    }

    /// Simulation only: as [`Self::new`], never offering native as joiner
    /// when `joiner_off` (`NodeConfig::native_joiner_off`).
    pub(crate) fn with_joiner_off(mut self, joiner_off: bool) -> Self {
        self.joiner_off = joiner_off;
        self
    }

    /// The node's transport opened with this QUIC endpoint. Only the first
    /// call counts: a node opens one transport.
    pub(crate) fn set_endpoint(&self, endpoint: crate::transport::QuicAvailabilityWatch) {
        self.endpoint.get_or_init(|| endpoint);
    }

    /// This node's readiness now, as joiner: the offer it sends and whether
    /// it can dial a `Native` decision. Demoted only by its joiner failures.
    pub(crate) fn readiness(&self) -> NativeReadiness {
        self.readiness_with(false)
    }

    /// This node's readiness now, as acceptor, read when it decides: as
    /// [`Self::readiness`], and also off while its acceptor demotion runs.
    pub(crate) fn acceptor_readiness(&self) -> NativeReadiness {
        self.readiness_with(true)
    }

    fn readiness_with(&self, as_acceptor: bool) -> NativeReadiness {
        let endpoint_up = self
            .endpoint
            .get()
            .is_some_and(|e| e.get() == crate::transport::QuicAvailability::Up);
        let (dial_ready, release_percent) = match self.release_override {
            Some(percent) => (true, percent),
            None => (NATIVE_DIAL_READY, QUIC_NATIVE_PERCENT),
        };
        let dial_ready = dial_ready && (as_acceptor || !self.joiner_off);
        let now = Instant::now();
        let joiner_demoted = self.demotion.lock().is_demoted(now);
        let acceptor_demoted = as_acceptor && self.acceptor_demotion.lock().is_demoted(now);
        NativeReadiness::from_parts(
            dial_ready,
            endpoint_up,
            joiner_demoted || acceptor_demoted,
            release_percent,
        )
    }

    /// A strike in the joiner streak with `peer`: a rescue (a native setup
    /// that failed on the path, then UDP to `peer` connected;
    /// [`DEMOTION_FAILURES`]). Returns whether it demoted the node, which is
    /// counted.
    pub(crate) fn record_failure(&self, peer: &TransportPublicKey) -> bool {
        let demoted = self.demotion.lock().record_failure(peer, Instant::now());
        if demoted {
            TRANSPORT_METRICS.record_connect_native_demotion();
            tracing::info!(
                cooldown_secs = DEMOTION_COOLDOWN.as_secs(),
                "QUIC-native connects off after native failures with distinct peers"
            );
        }
        demoted
    }

    /// A native setup succeeded.
    pub(crate) fn record_success(&self) {
        self.demotion.lock().record_success();
    }

    /// A strike in the acceptor streak with `joiner`: a rescue (a native
    /// setup this node made as ACCEPTOR failed on the path, then UDP to
    /// `joiner` connected; [`ACCEPTOR_DEMOTION_FAILURES`]). Counted apart from
    /// the joiner streak, across distinct joiners, so one joiner counts once.
    /// Returns whether it demoted the acceptor role, which is counted.
    pub(crate) fn record_acceptor_failure(&self, joiner: &TransportPublicKey) -> bool {
        let demoted = self
            .acceptor_demotion
            .lock()
            .record_failure(joiner, Instant::now());
        if demoted {
            TRANSPORT_METRICS.record_connect_native_acceptor_demotion();
            tracing::info!(
                cooldown_secs = DEMOTION_COOLDOWN.as_secs(),
                "QUIC-native decisions off as acceptor after native failures with distinct joiners"
            );
        }
        demoted
    }

    /// A native setup this node made as acceptor succeeded: the acceptor
    /// streak starts over.
    pub(crate) fn record_acceptor_success(&self) {
        self.acceptor_demotion.lock().record_success();
    }

    /// How many times this node was demoted.
    #[cfg(test)]
    pub(crate) fn demotions(&self) -> u64 {
        self.demotion.lock().demotions()
    }

    /// How many times this node's acceptor role was demoted.
    #[cfg(test)]
    pub(crate) fn acceptor_demotions(&self) -> u64 {
        self.acceptor_demotion.lock().demotions()
    }

    /// Distinct joiners in the acceptor role's current failure streak.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn acceptor_streak(&self) -> usize {
        self.acceptor_demotion.lock().streak.len()
    }

    /// Distinct acceptors in the joiner role's current strike streak.
    #[cfg(test)]
    pub(crate) fn joiner_streak(&self) -> usize {
        self.demotion.lock().streak.len()
    }
}

/// One role's demotion after repeated native failures
/// ([`NativeConnectState`] holds one for the joiner role and one for the
/// acceptor role). A failure is always followed by a UDP dial to the same
/// peer, so demotion only stops new native attempts.
///
/// `threshold` failures in a row ([`DEMOTION_FAILURES`] as joiner,
/// [`ACCEPTOR_DEMOTION_FAILURES`] as acceptor), each with a different peer,
/// demote the role for [`DEMOTION_COOLDOWN`]; a success in that role resets
/// the streak. Repeated failures with one peer count once, so one broken peer
/// cannot demote a node. It only ever lowers: it can make a node offer 0 or
/// decide `Udp`, never more.
#[derive(Debug)]
pub(crate) struct NativeDemotion {
    /// Distinct peers in the current streak, each with when its strike was
    /// counted; strikes older than [`STRIKE_WINDOW`] are dropped. At most
    /// `threshold`.
    streak: Vec<(TransportPublicKey, Instant)>,
    threshold: usize,
    demoted_until: Option<Instant>,
    demotions: u64,
}

impl NativeDemotion {
    pub(crate) fn new(threshold: usize) -> Self {
        Self {
            streak: Vec::with_capacity(threshold),
            threshold,
            demoted_until: None,
            demotions: 0,
        }
    }

    /// A strike with `peer` (a rescue, see [`DEMOTION_FAILURES`]). Returns
    /// whether it demoted the role.
    pub(crate) fn record_failure(&mut self, peer: &TransportPublicKey, now: Instant) -> bool {
        if self.is_demoted(now) {
            return false;
        }
        self.streak
            .retain(|(_, at)| now.saturating_duration_since(*at) < STRIKE_WINDOW);
        if !self.streak.iter().any(|(key, _)| key == peer) {
            self.streak.push((peer.clone(), now));
        }
        if self.streak.len() >= self.threshold {
            self.streak.clear();
            self.demoted_until = Some(now + DEMOTION_COOLDOWN);
            self.demotions += 1;
            return true;
        }
        false
    }

    /// A native attempt succeeded: the streak starts over.
    pub(crate) fn record_success(&mut self) {
        self.streak.clear();
    }

    pub(crate) fn is_demoted(&self, now: Instant) -> bool {
        self.demoted_until.is_some_and(|until| now < until)
    }

    /// How many times the node was demoted.
    #[cfg(test)]
    pub(crate) fn demotions(&self) -> u64 {
        self.demotions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u32) -> TransportPublicKey {
        TransportPublicKey::from_bytes(*blake3::hash(&seed.to_be_bytes()).as_bytes())
    }

    /// The body names the CONNECT transaction, the remote's key in the form
    /// the collector crosses it by, the outcome and the side, and no address.
    #[test]
    fn the_setup_outcome_body_names_tx_peer_outcome_and_side() {
        use crate::transport::NatAttemptOrigin::{ConnectAcceptor, ConnectJoiner, Gateway};
        let tx = crate::message::Transaction::new::<crate::operations::connect::ConnectMsg>();
        let peer = key(7);
        let body = native_setup_outcome_body(&tx, &peer, "hello_lost", ConnectJoiner);
        assert_eq!(
            body,
            serde_json::json!({
                "tx": tx.to_string(),
                "peer": peer.to_string(),
                "outcome": "hello_lost",
                "side": "joiner",
            })
        );
        assert_eq!(
            native_setup_outcome_body(&tx, &peer, "established", ConnectAcceptor)["side"],
            "acceptor"
        );
        assert_eq!(
            native_setup_outcome_body(&tx, &peer, "established", Gateway)["side"],
            "gateway"
        );
        assert_eq!(body.as_object().unwrap().len(), 4, "keys only, no address");
    }

    /// Both lifecycle arms report a native setup's outcome, near the top of the
    /// arm and only for a native dial; deleting either call, or reporting a UDP
    /// dial, leaves the pair join one-sided with every other test green.
    #[test]
    fn both_outbound_arms_report_a_native_setup_outcome() {
        let code: String =
            include_str!("../../node/network_bridge/p2p_protoc/connection_lifecycle.rs")
                .lines()
                .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                .collect::<Vec<_>>()
                .join("\n");
        let squeezed: String = code.chars().filter(|c| !c.is_whitespace()).collect();
        for (arm, wants) in [
            (
                "HandshakeEvent::OutboundEstablished{transaction,peer,connection,transient,transport,origin,}=>{",
                "iftransport.is_native(){self.report_native_setup_outcome(&transaction,&peer,crate::transport::NativeSetupOutcome::Established.label(),origin,);}",
            ),
            (
                "HandshakeEvent::OutboundFailed{transaction,peer,error,transient,transport,native_outcome,origin,}=>{",
                "iftransport.is_native(){self.report_native_setup_outcome(&transaction,&peer,native_outcome.map_or(\"unknown\",|outcome|outcome.label()),origin,);}",
            ),
        ] {
            let at = squeezed
                .find(arm)
                .unwrap_or_else(|| panic!("arm not found: {arm}"));
            let after = &squeezed[at + arm.len()..];
            assert!(
                after.starts_with(wants),
                "the arm must open with the native-only report: {arm}"
            );
        }
        assert!(
            squeezed.contains("\"native_setup_outcome\","),
            "the event name the collector reads"
        );
    }

    fn pairs() -> impl Iterator<Item = (TransportPublicKey, TransportPublicKey)> {
        (0..2000).map(|i| (key(2 * i), key(2 * i + 1)))
    }

    fn ready(percent: u8) -> NativeReadiness {
        NativeReadiness {
            demoted: false,
            ready: true,
            percent,
        }
    }

    /// Computed outside this code: `b3sum` of the domain string, 32 bytes of 1
    /// and 32 bytes of 2 starts b8fbbec1ddadaf48, which is
    /// 13329457261990227784, and that mod 100 is 84.
    #[test]
    fn the_slice_has_this_test_vector() {
        let low = TransportPublicKey::from_bytes([1; 32]);
        let high = TransportPublicKey::from_bytes([2; 32]);
        assert_eq!(native_bucket(&low, &high), 84);
        assert_eq!(native_bucket(&high, &low), 84);
    }

    #[test]
    fn the_slice_is_symmetric_and_in_range() {
        let mut orders = [0, 0];
        for (a, b) in pairs() {
            orders[usize::from(a < b)] += 1;
            let bucket = native_bucket(&a, &b);
            assert_eq!(bucket, native_bucket(&b, &a));
            assert!(bucket < 100);
        }
        assert!(orders[0] > 0 && orders[1] > 0, "both orders were tried");
    }

    #[test]
    fn the_slice_holds_about_its_share() {
        for (percent, low, high) in [(5, 60, 140), (25, 420, 580), (50, 900, 1100)] {
            let held = pairs()
                .filter(|(a, b)| native_bucket(a, b) < percent)
                .count();
            assert!((low..=high).contains(&held), "{percent}%: {held} of 2000");
        }
    }

    /// The arm is the decision's own: `Treated` exactly when it says `Native`,
    /// for every offer, readiness, bucket and held-transport input.
    #[test]
    fn acceptor_arm_treated_is_exactly_a_native_decision() {
        let offers = [
            None,
            Some(NativeOffer::capable(0)),
            Some(NativeOffer::capable(25)),
            Some(NativeOffer::capable(100)),
        ];
        let readiness = [
            ready(0),
            ready(25),
            ready(100),
            NativeReadiness {
                demoted: false,
                ready: false,
                percent: 25,
            },
        ];
        for offer in offers {
            for own in readiness {
                for bucket in 0..100u8 {
                    for holds in [false, true] {
                        // The formula `decide_native` had before it took its
                        // answer from `acceptor_arm`, kept here as the oracle.
                        let offered = offer.map_or(0, NativeOffer::effective_percent);
                        let native = offered > 0
                            && own.ready
                            && bucket < offered.min(own.percent.min(100))
                            && !holds;
                        assert_eq!(
                            acceptor_arm(offer, own, bucket, holds) == Some(ConnectArm::Treated),
                            native,
                            "arm {offer:?} {own:?} bucket {bucket} holds {holds}"
                        );
                        assert_eq!(
                            decide_native(offer, own, bucket, holds) == NativeDecision::Native,
                            native,
                            "decision {offer:?} {own:?} bucket {bucket} holds {holds}"
                        );
                    }
                }
            }
        }
    }

    /// Treated below the lower percent, control at or above it, nothing with
    /// no offer or with a transport already held.
    #[test]
    fn acceptor_arm_splits_at_the_lower_percent() {
        let offer = Some(NativeOffer::capable(50));
        assert_eq!(
            acceptor_arm(offer, ready(25), 24, false),
            Some(ConnectArm::Treated)
        );
        assert_eq!(
            acceptor_arm(offer, ready(25), 25, false),
            Some(ConnectArm::Control)
        );
        assert_eq!(
            acceptor_arm(offer, ready(100), 49, false),
            Some(ConnectArm::Treated)
        );
        assert_eq!(
            acceptor_arm(offer, ready(100), 50, false),
            Some(ConnectArm::Control)
        );
        assert_eq!(acceptor_arm(None, ready(100), 99, false), None);
        assert_eq!(acceptor_arm(offer, ready(100), 0, true), None);
        // Below the percent but not ready to dial: in neither arm.
        let not_ready = NativeReadiness {
            demoted: false,
            ready: false,
            percent: 50,
        };
        assert_eq!(acceptor_arm(offer, not_ready, 0, false), None);
    }

    /// While the release's percent is 0 (0.2.163), every pair where both ends
    /// offered is control, at every bucket, so the control counters are
    /// non-zero in the field before any pair is treated.
    #[test]
    fn at_percent_zero_every_offered_pair_is_control() {
        let at_release = NativeReadiness::from_parts(true, true, false, 0);
        let not_dialling = NativeReadiness::from_parts(false, false, false, 0);
        for own in [at_release, not_dialling] {
            assert_eq!(own.percent, 0);
            for bucket in 0..100u8 {
                assert_eq!(
                    acceptor_arm(Some(own.offer()), own, bucket, false),
                    Some(ConnectArm::Control),
                    "bucket {bucket}"
                );
                assert!(joiner_in_control(
                    Some(own.offer()),
                    Some(NativeDecision::Udp),
                    bucket
                ));
            }
        }
    }

    /// The joiner counts a pair as control only when it can be sure: a `Udp`
    /// decision to its offer, and a bucket at or above its own percent.
    #[test]
    fn joiner_in_control_needs_an_offer_a_udp_decision_and_a_high_bucket() {
        let sent = Some(NativeOffer::capable(25));
        let udp = Some(NativeDecision::Udp);
        assert!(joiner_in_control(sent, udp, 25));
        assert!(joiner_in_control(sent, udp, 99));
        assert!(
            !joiner_in_control(sent, udp, 24),
            "may be the acceptor's lower percent"
        );
        assert!(!joiner_in_control(sent, Some(NativeDecision::Native), 99));
        assert!(!joiner_in_control(
            sent,
            Some(NativeDecision::Unknown(7)),
            99
        ));
        assert!(
            !joiner_in_control(sent, None, 99),
            "an acceptor or relay below the floor"
        );
        assert!(!joiner_in_control(None, udp, 99), "no offer was sent");
        // The acceptor held a transport and reused it: it counts no arm, but
        // the joiner sees the same `Udp` decision and counts control.
        let ready_acceptor = ready(100);
        assert_eq!(acceptor_arm(sent, ready_acceptor, 99, true), None);
        assert!(
            joiner_in_control(sent, udp, 99),
            "joiner control includes transport reuse"
        );
    }

    /// The decision's truth table: each condition alone keeps it at `Udp`.
    #[test]
    fn decide_native_needs_every_condition() {
        let offer = Some(NativeOffer::capable(50));
        assert_eq!(
            decide_native(offer, ready(50), 10, false),
            NativeDecision::Native
        );
        // No offer, or an offer of 0.
        assert_eq!(
            decide_native(None, ready(50), 0, false),
            NativeDecision::Udp
        );
        assert_eq!(
            decide_native(Some(NativeOffer::capable(0)), ready(50), 0, false),
            NativeDecision::Udp
        );
        // Not ready, even with a percent.
        let not_ready = NativeReadiness {
            demoted: false,
            ready: false,
            percent: 50,
        };
        assert_eq!(
            decide_native(offer, not_ready, 0, false),
            NativeDecision::Udp
        );
        // Ready at 0.
        assert_eq!(
            decide_native(offer, ready(0), 0, false),
            NativeDecision::Udp
        );
        // A transport to the joiner already exists: it is reused.
        assert_eq!(
            decide_native(offer, ready(100), 0, true),
            NativeDecision::Udp
        );
        // Bucket at or above the lower percent.
        assert_eq!(
            decide_native(offer, ready(50), 50, false),
            NativeDecision::Udp
        );
    }

    /// The lower of the two percents decides, in both directions, and a joiner
    /// at 5% and an acceptor at 25% agree for every bucket.
    #[test]
    fn decide_native_uses_the_lower_percent() {
        for bucket in 0..100u8 {
            let joiner_low = decide_native(Some(NativeOffer::capable(5)), ready(25), bucket, false);
            let acceptor_low =
                decide_native(Some(NativeOffer::capable(25)), ready(5), bucket, false);
            let expected = if bucket < 5 {
                NativeDecision::Native
            } else {
                NativeDecision::Udp
            };
            assert_eq!(joiner_low, expected, "bucket {bucket}");
            assert_eq!(acceptor_low, expected, "bucket {bucket}");
        }
        for bucket in 0..100u8 {
            assert_eq!(
                decide_native(Some(NativeOffer::capable(100)), ready(100), bucket, false),
                NativeDecision::Native
            );
        }
    }

    #[test]
    fn effective_percent_only_lowers() {
        assert_eq!(effective_native_percent(false, true, false, 100, None), 0);
        assert_eq!(effective_native_percent(true, false, false, 100, None), 0);
        assert_eq!(effective_native_percent(true, true, true, 100, None), 0);
        assert_eq!(effective_native_percent(true, true, false, 25, None), 25);
        assert_eq!(effective_native_percent(true, true, false, 25, Some(5)), 5);
        assert_eq!(
            effective_native_percent(true, true, false, 25, Some(90)),
            25
        );
        // Not ready: an override cannot raise it.
        assert_eq!(
            effective_native_percent(false, true, false, 25, Some(100)),
            0
        );
    }

    /// This build cannot dial native, so it offers 0 and never decides
    /// `Native`, whatever the endpoint state.
    #[test]
    fn this_release_offers_zero_and_decides_udp() {
        const { assert!(!NATIVE_DIAL_READY) };
        for endpoint_up in [false, true] {
            let own = NativeReadiness::from_inputs(NATIVE_DIAL_READY, endpoint_up, false);
            assert!(!own.ready);
            assert_eq!(own.offer(), NativeOffer::capable(0));
            for bucket in 0..100 {
                assert_eq!(
                    decide_native(Some(NativeOffer::capable(100)), own, bucket, false),
                    NativeDecision::Udp
                );
            }
        }
        assert_eq!(NativeReadiness::current().offer().percent, 0);
    }

    #[test]
    fn no_udp_acceptor_rejects_only_an_offerless_request() {
        use NoUdpAcceptorAnswer::*;
        let offer = Some(NativeOffer::capable(0));
        assert_eq!(no_udp_acceptor_answer(None, false), Rejected);
        assert_eq!(no_udp_acceptor_answer(offer, false), Decide);
        assert_eq!(no_udp_acceptor_answer(None, true), Decide);
        assert_eq!(no_udp_acceptor_answer(offer, true), Decide);
    }

    /// `None`, `Udp` and an unknown decision dial UDP whatever was offered.
    /// `Native` dials native only when the joiner offered a percent above 0
    /// and can still dial; it fails otherwise, naming which.
    #[test]
    fn joiner_dials_native_only_when_it_offered_and_can() {
        use JoinerDecisionFailure::*;
        let zero = Some(NativeOffer::capable(0));
        let five = Some(NativeOffer::capable(5));
        for can_dial in [false, true] {
            for sent in [None, zero, five] {
                assert_eq!(
                    joiner_response_action(sent, None, can_dial),
                    Ok(DialTransport::Udp)
                );
                assert_eq!(
                    joiner_response_action(sent, Some(NativeDecision::Udp), can_dial),
                    Ok(DialTransport::Udp)
                );
            }
            let native = Some(NativeDecision::Native);
            assert_eq!(
                joiner_response_action(None, native, can_dial),
                Err(DecisionWithoutOffer)
            );
            assert_eq!(
                joiner_response_action(zero, native, can_dial),
                Err(DecisionWithoutOffer)
            );
        }
        let native = Some(NativeDecision::Native);
        assert_eq!(
            joiner_response_action(five, native, false),
            Err(NativeUnsupported)
        );
        assert!(joiner_response_action(five, native, true).is_ok_and(DialTransport::is_native));
    }

    /// A node's state reads its own endpoint, its override and its demotion
    /// into its readiness: it can dial only once its transport opened with
    /// an endpoint that is up, a simulation override turns dialling on at
    /// its percent, and three native failures with distinct peers stop it
    /// offering.
    #[test]
    fn native_connect_state_reads_its_endpoint_override_and_demotion() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let off = NativeConnectState::new(None);
        off.set_endpoint(watch.clone());
        assert!(!off.readiness().ready, "production: not ready to dial");
        let on = NativeConnectState::new(Some(100));
        assert!(!on.readiness().ready, "no transport yet");
        assert_eq!(on.readiness().percent, 0);
        on.set_endpoint(watch);
        assert!(on.readiness().ready);
        assert_eq!(on.readiness().percent, 100);
        endpoint.send(QuicAvailability::Down).unwrap();
        assert!(!on.readiness().ready, "the endpoint went down");
        endpoint.send(QuicAvailability::Up).unwrap();
        assert!(on.readiness().ready);
        for i in 1..=3 {
            on.record_failure(&key(i));
        }
        assert_eq!(on.demotions(), 1);
        assert!(!on.readiness().ready, "demoted");
        assert_eq!(on.readiness().percent, 0);
        assert!(
            !on.acceptor_readiness().ready,
            "a demoted joiner does not decide native as acceptor either"
        );
    }

    fn up_state(release: u8) -> NativeConnectState {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        // A watch keeps its last value after its sender drops.
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let state = NativeConnectState::new(Some(release));
        state.set_endpoint(watch);
        state
    }

    /// The acceptor role has its own streak (10.2 item 2). Failures as
    /// acceptor with `ACCEPTOR_DEMOTION_FAILURES` distinct joiners stop the
    /// node DECIDING native (its acceptor readiness, which `decide_native`
    /// reads, goes to percent 0 and counts as demoted) while its own offer as
    /// joiner is untouched; repeats with one joiner count once; one failure
    /// fewer does not demote; a joiner success does not touch the acceptor
    /// streak, and an acceptor success restarts it. The two streaks never feed
    /// each other.
    #[test]
    fn acceptor_failures_turn_off_only_the_acceptor_decision() {
        let on = up_state(100);
        for _ in 0..5 {
            assert!(!on.record_acceptor_failure(&key(9)));
        }
        for i in 10..(10 + ACCEPTOR_DEMOTION_FAILURES as u32 - 2) {
            assert!(!on.record_acceptor_failure(&key(i)));
        }
        assert!(
            on.acceptor_readiness().ready,
            "one failure short: one joiner counted once"
        );
        on.record_success();
        let counted =
            || crate::transport::metrics::ACCEPTOR_DEMOTIONS_ON_THIS_THREAD.with(|c| c.get());
        let before = counted();
        assert!(
            on.record_acceptor_failure(&key(100)),
            "a joiner success does not reset the acceptor streak, so this is the last"
        );
        assert_eq!(on.acceptor_demotions(), 1);
        assert_eq!(
            counted(),
            before + 1,
            "the demotion is counted for telemetry"
        );
        assert_eq!(on.demotions(), 0, "the joiner streak saw nothing");
        let joiner = on.readiness();
        assert!(joiner.ready, "its own offer is untouched");
        assert_eq!(joiner.percent, 100);
        assert!(!joiner.demoted);
        let acceptor = on.acceptor_readiness();
        assert!(!acceptor.ready);
        assert_eq!(acceptor.percent, 0);
        assert!(acceptor.demoted);
        for bucket in [0u8, 50, 99] {
            assert_eq!(
                decide_native(Some(NativeOffer::capable(100)), acceptor, bucket, false),
                NativeDecision::Udp
            );
        }

        let fresh = up_state(100);
        for i in 1..ACCEPTOR_DEMOTION_FAILURES as u32 {
            fresh.record_acceptor_failure(&key(i));
        }
        fresh.record_acceptor_success();
        fresh.record_acceptor_failure(&key(50));
        assert_eq!(fresh.acceptor_demotions(), 0, "an acceptor success resets");
        assert!(fresh.acceptor_readiness().ready);
        for i in 20..23 {
            fresh.record_failure(&key(i));
        }
        assert_eq!(fresh.acceptor_demotions(), 0, "joiner failures never count");
        assert!(
            fresh.acceptor_readiness().demoted,
            "but they stop decisions"
        );
    }

    /// Both roles demote on three strikes (rescues), strikes count for one
    /// cooldown, and a demotion lasts six hours.
    #[test]
    fn demotion_thresholds_have_these_values() {
        assert_eq!(DEMOTION_FAILURES, 3);
        assert_eq!(ACCEPTOR_DEMOTION_FAILURES, 3);
        assert_eq!(DEMOTION_COOLDOWN, Duration::from_secs(6 * 3600));
        assert_eq!(STRIKE_WINDOW, DEMOTION_COOLDOWN);
    }

    /// A strike older than `STRIKE_WINDOW` no longer counts: strikes spread
    /// over longer than the window never demote, while the same strikes
    /// inside it do. Applies to both roles (one `NativeDemotion` type).
    #[test]
    fn strikes_older_than_the_window_age_out() {
        let start = Instant::now();
        let mut spread = NativeDemotion::new(DEMOTION_FAILURES);
        for i in 0..10u32 {
            let at = start + (STRIKE_WINDOW / 2 + Duration::from_secs(1)) * i;
            assert!(!spread.record_failure(&key(i), at), "strike {i}");
        }
        assert_eq!(spread.demotions(), 0, "never three inside one window");
        let mut close = NativeDemotion::new(DEMOTION_FAILURES);
        let last = start + STRIKE_WINDOW - Duration::from_secs(1);
        assert!(!close.record_failure(&key(0), start));
        assert!(!close.record_failure(&key(1), start + Duration::from_secs(60)));
        assert!(
            close.record_failure(&key(2), last),
            "three inside the window"
        );
    }

    /// An acceptor whose readiness is demoted (by either role's streak)
    /// counts its pairs in neither arm, so
    /// the control arm does not collect the pairs where native failed; one
    /// at release percent 0 is control as before.
    #[test]
    fn a_demoted_acceptor_is_in_neither_arm() {
        let offer = Some(NativeOffer::capable(100));
        let demoted = NativeReadiness::from_parts(true, true, true, 100);
        assert!(demoted.demoted);
        for bucket in [0u8, 24, 50, 99] {
            assert_eq!(acceptor_arm(offer, demoted, bucket, false), None);
        }
        let at_zero = NativeReadiness::from_parts(true, true, false, 0);
        assert_eq!(
            acceptor_arm(offer, at_zero, 10, false),
            Some(ConnectArm::Control)
        );
    }

    /// The acceptor demotion lasts `DEMOTION_COOLDOWN` and then ends on its
    /// own, while a joiner demotion started later still holds decisions off:
    /// each role's cooldown is read separately.
    #[tokio::test(start_paused = true)]
    async fn each_roles_cooldown_ends_on_its_own() {
        let on = up_state(100);
        for i in 0..ACCEPTOR_DEMOTION_FAILURES as u32 {
            on.record_acceptor_failure(&key(i));
        }
        assert!(!on.acceptor_readiness().ready);
        tokio::time::advance(DEMOTION_COOLDOWN - Duration::from_secs(1)).await;
        assert!(!on.acceptor_readiness().ready, "still in the cooldown");
        for i in 40..43 {
            on.record_failure(&key(i));
        }
        assert_eq!(on.demotions(), 1);
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(
            !on.acceptor_readiness().ready,
            "the acceptor cooldown ended, the joiner one holds"
        );
        // The acceptor streak counts again, which it does not while demoted.
        for i in 100..(100 + ACCEPTOR_DEMOTION_FAILURES as u32) {
            on.record_acceptor_failure(&key(i));
        }
        assert_eq!(
            on.acceptor_demotions(),
            2,
            "the acceptor cooldown had ended"
        );
        tokio::time::advance(DEMOTION_COOLDOWN + Duration::from_secs(1)).await;
        assert!(on.acceptor_readiness().ready, "both ended");
        assert!(on.readiness().ready);
    }

    /// A node with its joiner role off (simulation only) offers 0 and cannot
    /// dial a `Native` decision, but still decides as acceptor at its
    /// release percent.
    #[test]
    fn joiner_off_keeps_the_acceptor_deciding() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let state = NativeConnectState::new(Some(100)).with_joiner_off(true);
        state.set_endpoint(watch);
        assert!(!state.readiness().ready);
        assert_eq!(state.readiness().offer().percent, 0);
        assert!(!state.readiness().demoted);
        assert!(state.acceptor_readiness().ready);
        assert_eq!(state.acceptor_readiness().percent, 100);
    }

    /// Every level and percent decodes and re-encodes to the same bytes, so a
    /// relay passes them on unchanged; a level does not change a decision, and
    /// a percent above 100 reads as 100.
    #[test]
    fn offers_keep_their_bytes_and_read_as_capable() {
        for level in [LEVEL_NATIVE_CAPABLE, LEVEL_NATIVE_ONLY, 0, 7, u8::MAX] {
            for percent in [0u8, 40, 100, 250] {
                let bytes = bincode::serialize(&(level, percent)).unwrap();
                let offer: NativeOffer = bincode::deserialize(&bytes).unwrap();
                assert_eq!((offer.level, offer.percent), (level, percent));
                assert_eq!(bincode::serialize(&offer).unwrap(), bytes);
                assert_eq!(offer.effective_percent(), percent.min(100));
                let own = ready(100);
                for bucket in [0, 50, 99] {
                    assert_eq!(
                        decide_native(Some(offer), own, bucket, false),
                        decide_native(Some(NativeOffer::capable(percent)), own, bucket, false),
                        "level {level} percent {percent} bucket {bucket}"
                    );
                }
            }
        }
        // The encoding is the two bytes, in order.
        assert_eq!(
            bincode::serialize(&NativeOffer::capable(7)).unwrap(),
            vec![1, 7]
        );
    }

    /// A decision is one byte. Every byte decodes and re-encodes to itself,
    /// so a relay passes on a value a later release adds; a joiner here reads
    /// an unknown one as `Udp`.
    #[test]
    fn decisions_are_one_byte_and_keep_unknown_values() {
        assert_eq!(bincode::serialize(&NativeDecision::Udp).unwrap(), vec![0]);
        assert_eq!(
            bincode::serialize(&NativeDecision::Native).unwrap(),
            vec![1]
        );
        for wire in 0..=u8::MAX {
            let decision: NativeDecision = bincode::deserialize(&[wire]).unwrap();
            assert_eq!(bincode::serialize(&decision).unwrap(), vec![wire]);
            if wire > 1 {
                assert_eq!(decision, NativeDecision::Unknown(wire));
                let offer = Some(NativeOffer::capable(50));
                assert_eq!(
                    joiner_response_action(offer, Some(decision), true),
                    Ok(DialTransport::Udp)
                );
            }
        }
    }

    #[test]
    fn demotion_after_three_distinct_acceptors() {
        let now = Instant::now();
        let mut demotion = NativeDemotion::new(DEMOTION_FAILURES);
        demotion.record_failure(&key(1), now);
        demotion.record_failure(&key(2), now);
        assert!(!demotion.is_demoted(now));
        demotion.record_failure(&key(3), now);
        assert!(demotion.is_demoted(now));
        assert_eq!(demotion.demotions(), 1);
        // Holds for the cooldown, then ends.
        assert!(demotion.is_demoted(now + DEMOTION_COOLDOWN - Duration::from_secs(1)));
        assert!(!demotion.is_demoted(now + DEMOTION_COOLDOWN));
    }

    #[test]
    fn one_acceptor_repeated_counts_once() {
        let now = Instant::now();
        let mut demotion = NativeDemotion::new(DEMOTION_FAILURES);
        for _ in 0..10 {
            demotion.record_failure(&key(1), now);
        }
        demotion.record_failure(&key(2), now);
        assert!(!demotion.is_demoted(now));
        demotion.record_failure(&key(3), now);
        assert!(demotion.is_demoted(now));
    }

    #[test]
    fn a_success_resets_the_streak() {
        let now = Instant::now();
        let mut demotion = NativeDemotion::new(DEMOTION_FAILURES);
        demotion.record_failure(&key(1), now);
        demotion.record_failure(&key(2), now);
        demotion.record_success();
        demotion.record_failure(&key(3), now);
        demotion.record_failure(&key(4), now);
        assert!(!demotion.is_demoted(now));
        demotion.record_failure(&key(5), now);
        assert!(demotion.is_demoted(now));
    }

    /// After the cooldown a fresh streak is needed; failures during it are not
    /// carried over.
    #[test]
    fn failures_during_the_cooldown_do_not_carry_over() {
        let now = Instant::now();
        let mut demotion = NativeDemotion::new(DEMOTION_FAILURES);
        for i in 1..=3 {
            demotion.record_failure(&key(i), now);
        }
        let during = now + Duration::from_secs(60);
        demotion.record_failure(&key(10), during);
        demotion.record_failure(&key(11), during);
        let after = now + DEMOTION_COOLDOWN;
        assert!(!demotion.is_demoted(after));
        demotion.record_failure(&key(12), after);
        assert!(!demotion.is_demoted(after));
        assert_eq!(demotion.demotions(), 1);
    }

    /// Demotion only lowers: a demoted node is not ready and offers 0.
    #[test]
    fn a_demoted_node_offers_zero() {
        let own = NativeReadiness::from_inputs(true, true, true);
        assert!(!own.ready);
        assert_eq!(own.percent, 0);
    }
}
