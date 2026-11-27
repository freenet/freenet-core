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
//! dial. In production [`NATIVE_DIAL_READY`] is true and
//! [`QUIC_NATIVE_PERCENT`] is 100 (the floor release), so a ready pair
//! decides `Native`; a simulation sets both per node
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

/// Whether this build can dial a native connection. True since A+ step 1;
/// while false, every offer this node sends carries percent 0 and it never
/// decides `Native`.
pub(crate) const NATIVE_DIAL_READY: bool = true;

/// This release's share of pairs, 0 to 100, that set up a native connection
/// when both sides are ready. Each step is its own release. Only the acceptor
/// decides, and it decides on `min(offered, own)`, so a pair goes native only
/// if both releases allow it: a reversal takes effect for any pair where either
/// the acceptor or the joiner has updated, as nodes update. There is no runtime
/// kill switch. 25 was A+ step 1; 100 is the floor release.
pub(crate) const QUIC_NATIVE_PERCENT: u8 = 100;

/// This release's share, 0 to 100, of (joiner, gateway) pairs whose join
/// tries a native connection to the gateway first (#5940, phase B), when the
/// gateway is marked native in the gateway index and this node can dial
/// native. Any failure falls back at once to today's join. The joiner alone
/// decides (there is no connection to the gateway yet to ask it), so the
/// joiner's release governs. At 0 no join tries native. 100 is the floor
/// release: every join to a gateway marked native tries native first. There is
/// no runtime kill switch; a reversal takes effect as joiners update, and the
/// gateway index mark is what a gateway operator controls.
pub(crate) const GATEWAY_NATIVE_JOIN_PERCENT: u8 = 100;

/// After a native join with a gateway fails, joins to that gateway go
/// straight to today's join for this long (provisional). The gateway index
/// is read at startup, so a running joiner keeps a mark that was taken away;
/// without this, a marked gateway that stops answering native joins would
/// cost every join to it a whole native window before the fallback. Only
/// lowers: it never makes a join try native.
pub(crate) const GATEWAY_NATIVE_FAILURE_COOLDOWN: Duration = Duration::from_secs(600);

/// Whether a join to a gateway tries a native connection first: this node
/// can dial native (`ready`), the gateway is marked native in the index, and
/// the pair's bucket ([`native_bucket`], the slice the connect op uses) is
/// below `percent`.
pub(crate) fn gateway_join_native(ready: bool, marked: bool, bucket: u8, percent: u8) -> bool {
    ready && marked && bucket < percent.min(100)
}

/// How a join to a gateway connects ([`NativeConnectState::plan_gateway_join`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GatewayJoinPlan {
    /// Today's join only.
    Legacy,
    /// A native connection first, then today's join if it gives none.
    NativeFirst,
    /// Would be [`Self::NativeFirst`], but a native join with this gateway
    /// failed within [`GATEWAY_NATIVE_FAILURE_COOLDOWN`]: today's join only.
    LegacyCoolingDown,
}

/// Offer level: the joiner can set up a native connection, and also UDP.
pub(crate) const LEVEL_NATIVE_CAPABLE: u8 = 1;

/// Offer level reserved for a node that has no UDP path. Never sent by this
/// release, and not read by it: no decision here depends on the level.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const LEVEL_NATIVE_ONLY: u8 = 2;

/// Native setups this node may have decided as acceptor and not yet seen end,
/// at a connection limit of `max_connections`: half of the node's native
/// setup slots (`QuicPolicy::native_setups_max`, which is
/// [`crate::transport::NATIVE_SETUPS_MAX`] lowered to the connection limit),
/// so its own dials as joiner keep the other half; at least 1.
pub(crate) fn acceptor_pending_setups_max(max_connections: usize) -> usize {
    (crate::transport::NATIVE_SETUPS_MAX.min(max_connections) / 2).max(1)
}

/// Of the acceptor's pending setups, how many may be for joiner addresses
/// in one source: an IPv4 address, or an IPv6 /64 (what one host or one
/// customer line usually gets). Provisional.
pub(crate) const ACCEPTOR_PENDING_SETUPS_PER_SOURCE: usize = 4;

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
/// QUIC endpoint is up; otherwise the release's
/// percent, lowered (never raised) by a per-node override.
pub(crate) fn effective_native_percent(
    dial_ready: bool,
    endpoint_up: bool,
    release_percent: u8,
    node_override: Option<u8>,
) -> u8 {
    if !(dial_ready && endpoint_up) {
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
/// client, 12 s for the server), so for the setup this only bounds one that
/// never reports. It ALSO sets how long an acceptor holds a native setup
/// admission ([`SetupTicket`]): from the dispatch of its native dial to this
/// long after it. Changing it changes the acceptor's admission throughput
/// (pending cap / this duration).
pub(crate) const NATIVE_CONNECT_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// This node's readiness for native connections in one role: as joiner
/// ([`NativeConnectState::readiness`]) or as acceptor
/// ([`NativeConnectState::acceptor_readiness`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeReadiness {
    /// The build can dial native and the endpoint is up.
    pub ready: bool,
    /// The effective percent ([`effective_native_percent`]).
    pub percent: u8,
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
    pub(crate) fn from_inputs(dial_ready: bool, endpoint_up: bool) -> Self {
        Self::from_parts(dial_ready, endpoint_up, QUIC_NATIVE_PERCENT)
    }

    /// As [`Self::from_inputs`], at `release_percent` in place of
    /// [`QUIC_NATIVE_PERCENT`].
    pub(crate) fn from_parts(dial_ready: bool, endpoint_up: bool, release_percent: u8) -> Self {
        Self {
            ready: dial_ready && endpoint_up,
            percent: effective_native_percent(dial_ready, endpoint_up, release_percent, None),
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
/// or a pair below the percent that this node is not ready to dial.
pub(crate) fn acceptor_arm(
    offer: Option<NativeOffer>,
    own: NativeReadiness,
    bucket: u8,
    holds_transport: bool,
) -> Option<ConnectArm> {
    acceptor_verdict(offer, own, bucket, holds_transport).and_then(|(arm, _)| arm)
}

/// Why an acceptor's arm is what it is, produced by the same branch that picks
/// the arm so the label cannot disagree with it. Read only by the simulation
/// trace (`decision_causes`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArmCause {
    /// In the slice, ready: decides `Native`.
    Native,
    /// This node already held a transport to the joiner.
    Held,
    /// In the slice but the build or endpoint is not ready.
    NotReady,
    /// The joiner offered percent 0 (it is not ready or below the
    /// floor), so no bucket is in the slice.
    OfferedZero,
    /// The bucket is at or above the lower of the two percents.
    OutsideSlice,
}

impl ArmCause {
    /// The label the simulation trace counts this cause under.
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Held => "held",
            Self::NotReady => "not_ready",
            Self::OfferedZero => "offered_zero",
            Self::OutsideSlice => "outside_slice",
        }
    }
}

/// [`acceptor_arm`] with the cause of the answer: `None` when the request
/// carried no offer.
pub(crate) fn acceptor_verdict(
    offer: Option<NativeOffer>,
    own: NativeReadiness,
    bucket: u8,
    holds_transport: bool,
) -> Option<(Option<ConnectArm>, ArmCause)> {
    let offered = offer?.effective_percent();
    if holds_transport {
        return Some((None, ArmCause::Held));
    }
    if bucket < offered.min(own.percent.min(100)) {
        Some(if own.ready {
            (Some(ConnectArm::Treated), ArmCause::Native)
        } else {
            (None, ArmCause::NotReady)
        })
    } else if offered == 0 {
        Some((Some(ConnectArm::Control), ArmCause::OfferedZero))
    } else {
        Some((Some(ConnectArm::Control), ArmCause::OutsideSlice))
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
/// transport to the joiner and reused it without dialling. So the acceptor's
/// counters are the ones for rates.
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
    /// native (its QUIC endpoint is not up).
    NativeUnsupported,
}

/// What a joiner does about an acceptor whose `Native` decision it cannot use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefusedDecisionPlan {
    /// Dial the acceptor over UDP at this time: the acceptor falls back to
    /// UDP when its native wait ends, `NATIVE_FALLBACK_AFTER` after its
    /// decision at the earliest, and two UDP hole punches through restricted
    /// NATs connect only when they overlap. A lone punch from the acceptor
    /// never gets through.
    DialUdpAt(Instant),
    /// Report the acceptor failed upstream (`ConnectFailed`), so the relay
    /// re-routes.
    ReportFailed,
}

/// The plan for a `Native` decision the joiner cannot use, answered at
/// `answered_at`. A joiner that offered native and lost its endpoint
/// before the response came still has UDP, and a failed native
/// setup must never cost the join, so it dials UDP at the common fallback
/// time, as after a native dial that failed. A decision that answered no
/// offer comes only from a bug or a misbehaving relay and is reported.
pub(crate) fn refused_decision_plan(
    failure: JoinerDecisionFailure,
    answered_at: Instant,
) -> RefusedDecisionPlan {
    match failure {
        JoinerDecisionFailure::NativeUnsupported => {
            RefusedDecisionPlan::DialUdpAt(answered_at + crate::transport::NATIVE_FALLBACK_AFTER)
        }
        JoinerDecisionFailure::DecisionWithoutOffer => RefusedDecisionPlan::ReportFailed,
    }
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

/// Where a joiner's native setup is counted for
/// [`ACCEPTOR_PENDING_SETUPS_PER_SOURCE`]: the crate's per-host source unit
/// (`put::stream_admission::Source`: an IPv4 address or an IPv6 /64). In test
/// builds only, a loopback or unspecified address is counted per port, since
/// local test networks run every node on one such address; a release build
/// counts it as one host.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetupSource {
    Host(crate::operations::put::stream_admission::Source),
    #[cfg(any(test, feature = "testing"))]
    Exact(std::net::SocketAddr),
}

impl SetupSource {
    fn of(addr: std::net::SocketAddr) -> Self {
        #[cfg(any(test, feature = "testing"))]
        {
            let ip = addr.ip().to_canonical();
            if ip.is_loopback() || ip.is_unspecified() {
                return Self::Exact(addr);
            }
        }
        Self::Host(crate::operations::put::stream_admission::Source::of(addr))
    }
}

/// Why the acceptor decided `Udp` for a pair it would otherwise have decided
/// `Native`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SetupAdmissionRefusal {
    /// The acceptor's pending setups are at its cap
    /// ([`acceptor_pending_setups_max`]).
    Full,
    /// [`ACCEPTOR_PENDING_SETUPS_PER_SOURCE`] joiner addresses are pending
    /// from the joiner's source.
    SourceFull,
    /// The joiner's address is not known, so there is nothing to count it by
    /// (or to dial).
    NoAddress,
}

/// The native setups this node decided as acceptor and has not yet seen end
/// ([`NativeConnectState::admit_acceptor_setup`]), by joiner address: the
/// unit the node's native setups, allow-set slots and dial coalescing all
/// use. A `Native` decision holds a native setup slot and an allow-set slot
/// for up to the server's window, so the acceptor admits a setup before
/// deciding `Native` rather than finding the slots taken when its setup
/// starts. A joiner that was refused is decided `Udp` at once: it never waits
/// out the native fallback because the acceptor had no room.
#[derive(Debug)]
pub(crate) struct NativeSetupAdmission {
    /// By joiner address: its source and how many tickets hold it (CONNECTs
    /// to one address share its one setup, since dials to one address
    /// coalesce).
    pending: std::collections::HashMap<std::net::SocketAddr, (SetupSource, usize)>,
    cap: usize,
}

impl NativeSetupAdmission {
    fn new(cap: usize) -> Self {
        Self {
            pending: std::collections::HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// Whether a ticket for `addr` would be admitted now, without taking one.
    fn check(&self, addr: Option<std::net::SocketAddr>) -> Result<(), SetupAdmissionRefusal> {
        let Some(addr) = addr else {
            return Err(SetupAdmissionRefusal::NoAddress);
        };
        if self.pending.contains_key(&addr) {
            return Ok(());
        }
        if self.pending.len() >= self.cap {
            return Err(SetupAdmissionRefusal::Full);
        }
        let source = SetupSource::of(addr);
        let from_source = self.pending.values().filter(|(s, _)| *s == source).count();
        if from_source >= ACCEPTOR_PENDING_SETUPS_PER_SOURCE {
            return Err(SetupAdmissionRefusal::SourceFull);
        }
        Ok(())
    }

    /// Takes a hold on `addr`'s entry, creating it if admitted.
    fn admit(
        &mut self,
        addr: Option<std::net::SocketAddr>,
    ) -> Result<std::net::SocketAddr, SetupAdmissionRefusal> {
        self.check(addr)?;
        let Some(addr) = addr else {
            return Err(SetupAdmissionRefusal::NoAddress);
        };
        self.pending
            .entry(addr)
            .or_insert((SetupSource::of(addr), 0))
            .1 += 1;
        Ok(addr)
    }

    /// Gives back one hold on `addr`'s entry, removing it with the last.
    fn release(&mut self, addr: std::net::SocketAddr) {
        if let Some((_, holds)) = self.pending.get_mut(&addr) {
            *holds = holds.saturating_sub(1);
            if *holds == 0 {
                self.pending.remove(&addr);
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.pending.len()
    }
}

/// One admitted native setup as acceptor, held from the decision until the
/// acceptor's native dial deadline, [`NATIVE_CONNECT_WITHIN`] after the dial
/// is dispatched (when the setup can no longer hold a setup slot); dropping
/// it releases the admission, so an accept that is never dispatched (or whose
/// dispatch fails) releases it at once.
///
/// Residuals: the cap bounds distinct pending joiner ADDRESSES, not
/// decisions (CONNECTs to one address share an entry); the node's own native
/// dials as joiner take setup slots uncapped (the other half is left for
/// them, not reserved); and once UDP is removed (phase D), "refused, so
/// decide UDP" no longer has a UDP to decide, so a refusal will need another
/// answer (a `Rejected`).
pub(crate) struct SetupTicket {
    /// `None` for a ticket a test made with no admission behind it.
    admission: Option<std::sync::Arc<parking_lot::Mutex<NativeSetupAdmission>>>,
    addr: std::net::SocketAddr,
}

impl SetupTicket {
    /// A ticket with no admission behind it, for tests of the decision.
    #[cfg(test)]
    pub(crate) fn detached(addr: std::net::SocketAddr) -> Self {
        Self {
            admission: None,
            addr,
        }
    }
}

impl std::fmt::Debug for SetupTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetupTicket")
            .field("addr", &self.addr)
            .finish()
    }
}

impl Drop for SetupTicket {
    fn drop(&mut self) {
        if let Some(admission) = &self.admission {
            admission.lock().release(self.addr);
        }
    }
}

/// A node's QUIC-native connect state: whether it may dial native and at what
/// percent. One per node, in the `ConnectionManager`.
#[derive(Debug)]
pub(crate) struct NativeConnectState {
    /// Simulation only: native dials on, at this release percent, in place of
    /// [`NATIVE_DIAL_READY`] and [`QUIC_NATIVE_PERCENT`]
    /// (`NodeConfig::native_connect_override`). `None` in production.
    release_override: Option<u8>,
    /// Native setups decided as acceptor and not yet ended.
    setup_admission: std::sync::Arc<parking_lot::Mutex<NativeSetupAdmission>>,
    /// Simulation only: this node never offers native as joiner, but decides
    /// as acceptor as usual (`NodeConfig::native_joiner_off`), standing in
    /// for a node that rarely joins. `false` in production.
    joiner_off: bool,
    /// Simulation only: this node offers native as joiner but cannot use a
    /// `Native` decision when the response comes
    /// (`NodeConfig::native_refuses_decisions`). `false` in production.
    refuses_decisions: bool,
    /// This node's QUIC endpoint, set when its transport opens. Until then,
    /// and when it is not up, the node cannot dial native. Read per node, not
    /// from the process-wide endpoint metric, which several simulated nodes
    /// in one process share.
    endpoint: std::sync::OnceLock<(crate::transport::QuicAvailabilityWatch, bool)>,
    /// Simulation only: native gateway joins on, at this percent, in place of
    /// [`NATIVE_DIAL_READY`] and [`GATEWAY_NATIVE_JOIN_PERCENT`]
    /// (`NodeConfig::native_gateway_join_override`). `None` in production.
    gateway_join_override: Option<u8>,
    /// The addresses of the configured gateways the gateway index marks
    /// native, set once when the node is built. Empty until then.
    native_gateways: std::sync::OnceLock<std::collections::HashSet<std::net::SocketAddr>>,
    /// Marked gateways whose last native join failed, and when. At most one
    /// entry per marked gateway, so bounded by the index.
    gateway_failures: parking_lot::Mutex<std::collections::HashMap<std::net::SocketAddr, Instant>>,
}

impl NativeConnectState {
    pub(crate) fn new(release_override: Option<u8>) -> Self {
        Self {
            release_override,
            setup_admission: std::sync::Arc::new(parking_lot::Mutex::new(
                NativeSetupAdmission::new(acceptor_pending_setups_max(
                    crate::transport::NATIVE_SETUPS_MAX,
                )),
            )),
            joiner_off: false,
            refuses_decisions: false,
            endpoint: std::sync::OnceLock::new(),
            gateway_join_override: None,
            native_gateways: std::sync::OnceLock::new(),
            gateway_failures: parking_lot::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Simulation only: offering native as joiner but refusing every `Native`
    /// decision when `refuses` (`NodeConfig::native_refuses_decisions`).
    pub(crate) fn with_refuses_decisions(mut self, refuses: bool) -> Self {
        self.refuses_decisions = refuses;
        self
    }

    /// Whether this node, as joiner, can dial native for a `Native` decision
    /// that arrives now: its readiness now, as an endpoint that went
    /// down since the offer would have lowered it.
    pub(crate) fn joiner_can_dial_now(&self) -> bool {
        self.readiness().ready && !self.refuses_decisions
    }

    /// Simulation only: as [`Self::new`], never offering native as joiner
    /// when `joiner_off` (`NodeConfig::native_joiner_off`).
    pub(crate) fn with_joiner_off(mut self, joiner_off: bool) -> Self {
        self.joiner_off = joiner_off;
        self
    }

    /// [`Self::new`], with native gateway joins at `percent` in a simulation
    /// (`NodeConfig::native_gateway_join_override`).
    pub(crate) fn with_gateway_join_override(mut self, percent: Option<u8>) -> Self {
        self.gateway_join_override = percent;
        self
    }

    /// The configured gateways the index marks native. Only the first call
    /// counts: a node is built once.
    pub(crate) fn set_native_gateways(
        &self,
        gateways: impl IntoIterator<Item = std::net::SocketAddr>,
    ) {
        self.native_gateways.get_or_init(|| {
            gateways
                .into_iter()
                .map(crate::transport::normalize_mapped_addr)
                .collect()
        });
    }

    /// How this node's join to the gateway at `addr`, with key `gateway`,
    /// connects. Never [`GatewayJoinPlan::NativeFirst`] unless the node can
    /// dial native now, the gateway is marked native, and the pair is in the
    /// slice ([`gateway_join_native`]); counted
    /// `gateway_native_join_skipped_cooldown` when a recent failure with the
    /// gateway holds it back.
    pub(crate) fn plan_gateway_join(
        &self,
        own: &TransportPublicKey,
        addr: std::net::SocketAddr,
        gateway: &TransportPublicKey,
    ) -> GatewayJoinPlan {
        let addr = crate::transport::normalize_mapped_addr(addr);
        let (dial_ready, percent) = match self.gateway_join_override {
            Some(percent) => (true, percent),
            None => (NATIVE_DIAL_READY, GATEWAY_NATIVE_JOIN_PERCENT),
        };
        let now = Instant::now();
        let ready = dial_ready && self.endpoint_up();
        let marked = self
            .native_gateways
            .get()
            .is_some_and(|gateways| gateways.contains(&addr));
        if !gateway_join_native(ready, marked, native_bucket(own, gateway), percent) {
            return GatewayJoinPlan::Legacy;
        }
        let mut failures = self.gateway_failures.lock();
        match failures.get(&addr) {
            Some(failed)
                if now.saturating_duration_since(*failed) < GATEWAY_NATIVE_FAILURE_COOLDOWN =>
            {
                TRANSPORT_METRICS.record_gateway_native_join(
                    crate::transport::metrics::GatewayNativeJoin::SkippedCooldown,
                    addr,
                );
                #[cfg(any(test, feature = "testing"))]
                crate::operations::connect::native_wire_trace::record_at(
                    addr,
                    crate::operations::connect::native_wire_trace::NativeWireEvent::GatewayJoinSkippedCooldown,
                );
                GatewayJoinPlan::LegacyCoolingDown
            }
            Some(_) => {
                failures.remove(&addr);
                GatewayJoinPlan::NativeFirst
            }
            None => GatewayJoinPlan::NativeFirst,
        }
    }

    /// A native join with the gateway at `addr` ended: a failure starts its
    /// cooldown ([`GATEWAY_NATIVE_FAILURE_COOLDOWN`]), a success ends it. Only
    /// marked gateways are remembered.
    pub(crate) fn record_gateway_join(&self, addr: std::net::SocketAddr, established: bool) {
        let addr = crate::transport::normalize_mapped_addr(addr);
        let mut failures = self.gateway_failures.lock();
        if established {
            failures.remove(&addr);
        } else if self
            .native_gateways
            .get()
            .is_some_and(|gateways| gateways.contains(&addr))
        {
            failures.insert(addr, Instant::now());
        }
    }

    /// A transport to `addr` was removed from the node's connection map.
    /// A native connection that closed before it carried one frame from the
    /// peer never worked: the setup and Hello completed, but nothing came
    /// back (a middlebox that lets the QUIC handshake through and drops the
    /// rest, or a gateway that closes an admitted join). That is a failed
    /// gateway join, so it starts the gateway's cooldown and the next join
    /// goes over UDP, as a failed setup does. Without this the setup's
    /// success is the only thing recorded, and a joiner could redial the same
    /// dead native link forever. A link that did carry a frame, or a UDP
    /// one, says nothing about native and is ignored.
    ///
    /// It cannot tell why the link went: a local removal before the first
    /// frame (shutdown, dropping all connections, a dedup or eviction just
    /// after the join, or `promote_held_native`, which installs a held
    /// connection with `frame_seen` false) counts the same. The cost is
    /// bounded: that gateway's next joins within
    /// [`GATEWAY_NATIVE_FAILURE_COOLDOWN`] go over UDP, as they do today. A
    /// join that completes is not affected, since the gateway's probe or its
    /// CONNECT answer marks the frame first.
    pub(crate) fn gateway_link_closed(
        &self,
        addr: std::net::SocketAddr,
        native: bool,
        frame_seen: bool,
    ) {
        if native && !frame_seen {
            #[cfg(any(test, feature = "testing"))]
            if self.native_gateways.get().is_some_and(|gateways| {
                gateways.contains(&crate::transport::normalize_mapped_addr(addr))
            }) {
                crate::operations::connect::native_wire_trace::record_at(
                    addr,
                    crate::operations::connect::native_wire_trace::NativeWireEvent::GatewayLinkDeadBeforeFrame,
                );
            }
            self.record_gateway_join(addr, false);
        }
    }

    fn endpoint_up(&self) -> bool {
        self.endpoint
            .get()
            .is_some_and(|(e, prefer)| *prefer && e.get() == crate::transport::QuicAvailability::Up)
    }

    /// The node's transport opened with this QUIC endpoint. Only the first
    /// call counts: a node opens one transport. `prefer` is whether the
    /// configured `transport-quic` mode is `prefer`: a native connection
    /// carries application data, which `shadow` promises to keep on UDP, so a
    /// node in any other mode is never ready, as joiner, acceptor or gateway
    /// joiner, however up its endpoint is.
    pub(crate) fn set_endpoint(
        &self,
        endpoint: crate::transport::QuicAvailabilityWatch,
        prefer: bool,
    ) {
        self.endpoint.get_or_init(|| (endpoint, prefer));
    }

    /// This node's readiness now, as joiner: the offer it sends and whether
    /// it can dial a `Native` decision.
    pub(crate) fn readiness(&self) -> NativeReadiness {
        self.readiness_with(false)
    }

    /// This node's readiness now, as acceptor, read when it decides: as
    /// [`Self::readiness`], but a node whose joiner side is off still decides.
    pub(crate) fn acceptor_readiness(&self) -> NativeReadiness {
        self.readiness_with(true)
    }

    fn readiness_with(&self, as_acceptor: bool) -> NativeReadiness {
        let endpoint_up = self.endpoint_up();
        let (dial_ready, release_percent) = match self.release_override {
            Some(percent) => (true, percent),
            None => (NATIVE_DIAL_READY, QUIC_NATIVE_PERCENT),
        };
        let dial_ready = dial_ready && (as_acceptor || !self.joiner_off);
        NativeReadiness::from_parts(dial_ready, endpoint_up, release_percent)
    }

    /// Sizes the acceptor's pending setups for a node whose connection
    /// limit is `max_connections` ([`acceptor_pending_setups_max`]).
    pub(crate) fn with_acceptor_setup_limit(self, max_connections: usize) -> Self {
        self.setup_admission.lock().cap = acceptor_pending_setups_max(max_connections);
        self
    }

    /// Admits a native setup with the joiner at `addr` before this node, as
    /// acceptor, decides `Native` for it; on a refusal it decides `Udp`,
    /// which is counted. The ticket holds the admission until it is dropped,
    /// at the acceptor's native dial deadline.
    pub(crate) fn admit_acceptor_setup(
        &self,
        addr: Option<std::net::SocketAddr>,
    ) -> Result<SetupTicket, SetupAdmissionRefusal> {
        let result = self.setup_admission.lock().admit(addr);
        match result {
            Ok(addr) => Ok(SetupTicket {
                admission: Some(std::sync::Arc::clone(&self.setup_admission)),
                addr,
            }),
            Err(refusal) => {
                TRANSPORT_METRICS.record_connect_native_admission_refused();
                tracing::debug!(
                    ?addr,
                    ?refusal,
                    "QUIC-native setup not admitted; deciding UDP"
                );
                Err(refusal)
            }
        }
    }

    /// Whether a native setup with the joiner at `addr` would be admitted
    /// now, without taking one: a control pair at an acceptor with no room
    /// is left out of the arms, as a refused treated pair is.
    pub(crate) fn acceptor_setup_room(
        &self,
        addr: Option<std::net::SocketAddr>,
    ) -> Result<(), SetupAdmissionRefusal> {
        self.setup_admission.lock().check(addr)
    }

    /// Joiner addresses with an admitted native setup as acceptor.
    #[cfg(test)]
    pub(crate) fn pending_acceptor_setups(&self) -> usize {
        self.setup_admission.lock().len()
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
        let at_release = NativeReadiness::from_parts(true, true, 0);
        let not_dialling = NativeReadiness::from_parts(false, false, 0);
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
        assert_eq!(effective_native_percent(false, true, 100, None), 0);
        assert_eq!(effective_native_percent(true, false, 100, None), 0);
        assert_eq!(effective_native_percent(true, true, 25, None), 25);
        assert_eq!(effective_native_percent(true, true, 25, Some(5)), 5);
        assert_eq!(effective_native_percent(true, true, 25, Some(90)), 25);
        // Not ready: an override cannot raise it.
        assert_eq!(effective_native_percent(false, true, 25, Some(100)), 0);
    }

    /// Floor release: this build dials native and offers 100 once its
    /// endpoint is up. A node whose endpoint is down offers 0 and decides UDP;
    /// an up node decides `Native` for every bucket. The only path to UDP is a
    /// down endpoint or a peer that offers less.
    #[test]
    fn this_release_offers_100_when_up_and_decides_native_in_every_bucket() {
        const { assert!(NATIVE_DIAL_READY) };
        const { assert!(QUIC_NATIVE_PERCENT == 100) };
        let down = NativeReadiness::from_inputs(NATIVE_DIAL_READY, false);
        assert!(!down.ready);
        assert_eq!(down.offer(), NativeOffer::capable(0));
        let up = NativeReadiness::from_inputs(NATIVE_DIAL_READY, true);
        assert!(up.ready);
        assert_eq!(up.offer(), NativeOffer::capable(100));
        for bucket in 0..100 {
            assert_eq!(
                decide_native(Some(NativeOffer::capable(100)), down, bucket, false),
                NativeDecision::Udp
            );
            let want = if bucket < QUIC_NATIVE_PERCENT {
                NativeDecision::Native
            } else {
                NativeDecision::Udp
            };
            assert_eq!(
                decide_native(Some(NativeOffer::capable(100)), up, bucket, false),
                want,
                "bucket {bucket}"
            );
        }
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

    /// A node's state reads its own endpoint and its override into its
    /// readiness: it can dial only once its transport opened with an
    /// endpoint that is up, and a simulation override turns dialling on at
    /// its percent.
    #[test]
    fn native_connect_state_reads_its_endpoint_and_override() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let off = NativeConnectState::new(None);
        off.set_endpoint(watch.clone(), true);
        assert!(
            off.readiness().ready,
            "production: ready once the endpoint is up"
        );
        assert_eq!(off.readiness().percent, QUIC_NATIVE_PERCENT);
        let no_endpoint = NativeConnectState::new(None);
        assert!(
            !no_endpoint.readiness().ready,
            "production: no transport yet"
        );
        assert_eq!(no_endpoint.readiness().percent, 0);
        let on = NativeConnectState::new(Some(100));
        assert!(!on.readiness().ready, "no transport yet");
        assert_eq!(on.readiness().percent, 0);
        on.set_endpoint(watch, true);
        assert!(on.readiness().ready);
        assert_eq!(on.readiness().percent, 100);
        endpoint.send(QuicAvailability::Down).unwrap();
        assert!(!on.readiness().ready, "the endpoint went down");
        endpoint.send(QuicAvailability::Up).unwrap();
        assert!(on.readiness().ready);
    }

    /// A joiner that offered and lost native dials the acceptor over UDP at
    /// the fallback time; a decision that answered no offer is reported.
    #[test]
    fn a_refused_native_decision_dials_udp_only_when_the_joiner_offered() {
        let answered_at = Instant::now();
        assert_eq!(
            refused_decision_plan(JoinerDecisionFailure::NativeUnsupported, answered_at),
            RefusedDecisionPlan::DialUdpAt(answered_at + crate::transport::NATIVE_FALLBACK_AFTER),
            "an offer it can no longer honour: UDP at the acceptor's fallback time"
        );
        assert_eq!(
            refused_decision_plan(JoinerDecisionFailure::DecisionWithoutOffer, answered_at),
            RefusedDecisionPlan::ReportFailed,
            "a decision that answered no offer is reported"
        );
    }

    /// A node with its joiner role off (simulation only) offers 0 and cannot
    /// dial a `Native` decision, but still decides as acceptor at its
    /// release percent.
    #[test]
    fn joiner_off_keeps_the_acceptor_deciding() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let state = NativeConnectState::new(Some(100)).with_joiner_off(true);
        state.set_endpoint(watch, true);
        assert!(!state.readiness().ready);
        assert_eq!(state.readiness().offer().percent, 0);
        assert!(state.acceptor_readiness().ready);
        assert_eq!(state.acceptor_readiness().percent, 100);
    }

    fn v4(a: u8, b: u8, port: u16) -> std::net::SocketAddr {
        std::net::SocketAddr::from(([198, 51, a, b], port))
    }

    /// The acceptor admits native setups up to its cap and its per-source
    /// cap, by joiner address, only with a known address. Holds on one
    /// address share its entry; the entry goes with the last hold.
    #[test]
    fn native_setup_admission_bounds_pending_setups() {
        let cap = 8;
        let mut admission = NativeSetupAdmission::new(cap);
        let per = ACCEPTOR_PENDING_SETUPS_PER_SOURCE as u16;
        // One source: up to the per-source cap, any ports.
        for port in 0..per {
            assert!(admission.admit(Some(v4(0, 1, 1000 + port))).is_ok());
        }
        assert_eq!(
            admission.admit(Some(v4(0, 1, 2000))),
            Err(SetupAdmissionRefusal::SourceFull)
        );
        assert_eq!(admission.admit(None), Err(SetupAdmissionRefusal::NoAddress));
        // Another hold on a pending address shares its entry, whatever the
        // caps say.
        assert!(admission.admit(Some(v4(0, 1, 1000))).is_ok());
        assert_eq!(admission.len(), per as usize);
        // The first release leaves the shared entry; the second removes it.
        admission.release(v4(0, 1, 1000));
        assert_eq!(admission.len(), per as usize);
        admission.release(v4(0, 1, 1000));
        assert_eq!(admission.len(), per as usize - 1);
        assert!(admission.admit(Some(v4(0, 1, 2000))).is_ok(), "room again");
        // Fill the cap from distinct sources.
        let mut b = 0u8;
        while admission.len() < cap {
            assert!(admission.admit(Some(v4(1, b, 1))).is_ok());
            b += 1;
        }
        assert_eq!(
            admission.admit(Some(v4(2, 1, 1))),
            Err(SetupAdmissionRefusal::Full)
        );
        assert_eq!(
            admission.check(Some(v4(2, 1, 1))),
            Err(SetupAdmissionRefusal::Full)
        );
        assert_eq!(
            admission.check(Some(v4(1, 0, 1))),
            Ok(()),
            "a pending address"
        );
        // A release of an address with no entry changes nothing.
        admission.release(v4(9, 9, 9));
        assert_eq!(admission.len(), cap);
    }

    /// The acceptor's cap is half its native setup slots, which are
    /// `NATIVE_SETUPS_MAX` lowered to the connection limit, and at least 1.
    #[test]
    fn the_acceptor_setup_cap_follows_the_connection_limit() {
        let max = crate::transport::NATIVE_SETUPS_MAX;
        assert_eq!(acceptor_pending_setups_max(200), max / 2);
        assert_eq!(acceptor_pending_setups_max(max), max / 2);
        assert_eq!(acceptor_pending_setups_max(16), 8);
        assert_eq!(acceptor_pending_setups_max(1), 1);
        let state = NativeConnectState::new(Some(100)).with_acceptor_setup_limit(4);
        let _first = state
            .admit_acceptor_setup(Some(v4(3, 1, 1)))
            .expect("first");
        let _second = state
            .admit_acceptor_setup(Some(v4(3, 2, 1)))
            .expect("second");
        assert_eq!(
            state.acceptor_setup_room(Some(v4(3, 3, 1))),
            Err(SetupAdmissionRefusal::Full),
            "a connection limit of 4 gives 2 acceptor setups"
        );
    }

    /// IPv6 joiners in one /64 share the per-source cap and another /64 does
    /// not; IPv4-mapped addresses count as IPv4; loopback (a local test
    /// network) is counted per port.
    #[test]
    fn native_setup_sources_group_by_v4_address_and_v6_prefix() {
        let v6 = |s: &str| std::net::SocketAddr::new(s.parse().unwrap(), 1);
        assert_eq!(
            SetupSource::of(v6("2001:db8:1:2::1")),
            SetupSource::of(v6("2001:db8:1:2:ffff::9"))
        );
        assert_ne!(
            SetupSource::of(v6("2001:db8:1:2::1")),
            SetupSource::of(v6("2001:db8:1:3::1"))
        );
        assert_eq!(
            SetupSource::of(v6("::ffff:198.51.0.1")),
            SetupSource::of(v4(0, 1, 7))
        );
        assert_eq!(SetupSource::of(v4(0, 1, 7)), SetupSource::of(v4(0, 1, 8)));
        let lo = |port| std::net::SocketAddr::from(([127, 0, 0, 1], port));
        assert_ne!(SetupSource::of(lo(1)), SetupSource::of(lo(2)));
        assert_ne!(
            SetupSource::of("[::1]:1".parse().unwrap()),
            SetupSource::of("[::1]:2".parse().unwrap())
        );
    }

    /// A ticket holds its admission until it is dropped; a refusal is
    /// counted for telemetry.
    #[test]
    fn a_ticket_holds_its_admission_and_a_refusal_is_counted() {
        let state = NativeConnectState::new(Some(100));
        let counted = || {
            crate::transport::metrics::NATIVE_ADMISSIONS_REFUSED_ON_THIS_THREAD.with(|c| c.get())
        };
        let before = counted();
        let ticket = state
            .admit_acceptor_setup(Some(v4(9, 9, 1)))
            .expect("admitted");
        let shared = state
            .admit_acceptor_setup(Some(v4(9, 9, 1)))
            .expect("shared");
        assert_eq!(counted(), before);
        assert_eq!(state.pending_acceptor_setups(), 1);
        drop(ticket);
        assert_eq!(
            state.pending_acceptor_setups(),
            1,
            "still held by the other"
        );
        drop(shared);
        assert_eq!(state.pending_acceptor_setups(), 0);
        assert_eq!(
            state.admit_acceptor_setup(None).map(|_| ()),
            Err(SetupAdmissionRefusal::NoAddress)
        );
        assert_eq!(counted(), before + 1);
        assert_eq!(state.pending_acceptor_setups(), 0);
    }

    /// Floor release: every join to a gateway marked native in the index
    /// tries native first, with the production state (endpoint up, dial ready,
    /// 100 percent). A gateway NOT marked native stays on the legacy join: the
    /// index mark, not this constant, is what a gateway operator controls.
    #[test]
    fn this_release_joins_every_marked_gateway_natively() {
        const { assert!(GATEWAY_NATIVE_JOIN_PERCENT == 100) };
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        // Production state, with every other condition met.
        let state = NativeConnectState::new(None);
        state.set_endpoint(watch, true);
        let gateway = "192.0.2.1:31337".parse().unwrap();
        state.set_native_gateways([gateway]);
        let unmarked = "192.0.2.2:31337".parse().unwrap();
        for seed in 0..200 {
            assert_eq!(
                state.plan_gateway_join(&key(seed), gateway, &key(10_000)),
                GatewayJoinPlan::NativeFirst,
                "seed {seed}"
            );
            assert_eq!(
                state.plan_gateway_join(&key(seed), unmarked, &key(10_000)),
                GatewayJoinPlan::Legacy
            );
        }
    }

    /// A native connection carries application data, which `transport-quic =
    /// shadow` promises to keep on UDP. A shadow (or off) node's endpoint can
    /// be up, yet it must offer 0, decide UDP and join gateways over UDP, in
    /// production state (no percent override), while a `prefer` node with the
    /// same endpoint does not.
    #[test]
    fn a_node_not_in_prefer_mode_never_goes_native() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let gateway = "192.0.2.1:31337".parse().unwrap();
        let build = |prefer: bool| {
            let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
            state.set_endpoint(watch.clone(), prefer);
            state.set_native_gateways([gateway]);
            state
        };
        let shadow = build(false);
        for readiness in [shadow.readiness(), shadow.acceptor_readiness()] {
            assert!(!readiness.ready);
            assert_eq!(readiness.offer(), NativeOffer::capable(0));
            for bucket in 0..100 {
                assert_eq!(
                    decide_native(Some(NativeOffer::capable(100)), readiness, bucket, false),
                    NativeDecision::Udp
                );
            }
        }
        for seed in 0..100 {
            assert_eq!(
                shadow.plan_gateway_join(&key(seed), gateway, &key(10_000)),
                GatewayJoinPlan::Legacy
            );
        }
        let prefer = build(true);
        assert!(prefer.readiness().ready, "premise: prefer, same endpoint");
        assert!(
            (0..100).any(|seed| {
                prefer.plan_gateway_join(&key(seed), gateway, &key(10_000))
                    != GatewayJoinPlan::Legacy
            }),
            "premise: the gateway override reaches native for a prefer node"
        );
    }

    /// A join tries native only when the node can dial, the gateway is
    /// marked and the pair is in the slice; each condition alone is enough to
    /// keep today's join.
    #[test]
    fn a_gateway_join_needs_readiness_a_mark_and_the_slice() {
        for (ready, marked) in [(false, true), (true, false), (false, false)] {
            for bucket in 0..100 {
                assert!(!gateway_join_native(ready, marked, bucket, 100));
            }
        }
        for bucket in 0..100 {
            assert!(gateway_join_native(true, true, bucket, 100));
            assert!(!gateway_join_native(true, true, bucket, 0));
            assert_eq!(gateway_join_native(true, true, bucket, 30), bucket < 30);
            // A percent above 100 reads as 100.
            assert!(gateway_join_native(true, true, bucket, 250));
        }
    }

    /// The state reads the override, its endpoint, the gateways marked in
    /// the index (only those); the slice is the connect
    /// slice of the (joiner, gateway) pair.
    #[test]
    fn plan_gateway_join_reads_endpoint_marks_and_slice() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let marked: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let unmarked: std::net::SocketAddr = "192.0.2.2:31337".parse().unwrap();
        let gateway = key(10_000);
        let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
        state.set_native_gateways([marked]);
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::Legacy,
            "no transport yet"
        );
        state.set_endpoint(watch, true);
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::NativeFirst
        );
        // An IPv4-mapped address is the same gateway.
        let mapped = std::net::SocketAddr::new(
            std::net::Ipv4Addr::new(192, 0, 2, 1)
                .to_ipv6_mapped()
                .into(),
            31337,
        );
        assert_eq!(
            state.plan_gateway_join(&key(1), mapped, &gateway),
            GatewayJoinPlan::NativeFirst
        );
        assert_eq!(
            state.plan_gateway_join(&key(1), unmarked, &gateway),
            GatewayJoinPlan::Legacy
        );
        endpoint.send(QuicAvailability::Down).unwrap();
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::Legacy,
            "the endpoint went down"
        );
        endpoint.send(QuicAvailability::Up).unwrap();

        // At 50%, exactly the joiners whose pair bucket is below 50.
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let half = NativeConnectState::new(None).with_gateway_join_override(Some(50));
        half.set_endpoint(watch, true);
        half.set_native_gateways([marked]);
        let mut native = 0;
        for seed in 0..400 {
            let own = key(seed);
            let plan = half.plan_gateway_join(&own, marked, &gateway);
            let want = if native_bucket(&own, &gateway) < 50 {
                native += 1;
                GatewayJoinPlan::NativeFirst
            } else {
                GatewayJoinPlan::Legacy
            };
            assert_eq!(plan, want);
        }
        assert!((100..300).contains(&native), "premise: both arms seen");
    }

    /// A gateway the index gives as an IPv4-mapped address is the gateway
    /// dialled at its IPv4 address: marks are stored, not only looked up,
    /// without the mapping.
    #[test]
    fn a_mark_given_as_an_ipv4_mapped_address_matches_the_ipv4_dial() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let mapped = std::net::SocketAddr::new(
            std::net::Ipv4Addr::new(192, 0, 2, 1)
                .to_ipv6_mapped()
                .into(),
            31337,
        );
        let v4: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
        state.set_endpoint(watch, true);
        state.set_native_gateways([mapped]);
        assert_eq!(
            state.plan_gateway_join(&key(1), v4, &key(10_000)),
            GatewayJoinPlan::NativeFirst
        );
    }

    /// A failed native join sends joins to that gateway straight to today's
    /// join for the cooldown; a success, or the cooldown passing,
    /// ends it. Failures with an unmarked gateway are not remembered.
    #[tokio::test(start_paused = true)]
    async fn a_failed_gateway_join_cools_that_gateway_down() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let marked: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let other: std::net::SocketAddr = "192.0.2.3:31337".parse().unwrap();
        let gateway = key(10_000);
        let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
        state.set_endpoint(watch, true);
        state.set_native_gateways([marked, other]);
        state.record_gateway_join(marked, false);
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::LegacyCoolingDown
        );
        assert_eq!(
            state.plan_gateway_join(&key(1), other, &gateway),
            GatewayJoinPlan::NativeFirst,
            "only that gateway cools down"
        );
        tokio::time::advance(GATEWAY_NATIVE_FAILURE_COOLDOWN - Duration::from_secs(1)).await;
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::LegacyCoolingDown
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::NativeFirst,
            "the cooldown passed"
        );
        state.record_gateway_join(marked, false);
        state.record_gateway_join(marked, true);
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::NativeFirst,
            "a success ends the cooldown"
        );
        for _ in 0..4 {
            state.record_gateway_join(other, false);
        }
        let unmarked: std::net::SocketAddr = "192.0.2.9:31337".parse().unwrap();
        state.record_gateway_join(unmarked, false);
        assert_eq!(
            state.gateway_failures.lock().len(),
            1,
            "only marked gateways"
        );
    }

    /// A failure recorded under the IPv4-mapped spelling of a gateway's
    /// address cools down the IPv4 dial: `record_gateway_join` normalises
    /// the address as `plan_gateway_join` and `set_native_gateways` do.
    #[test]
    fn a_failure_recorded_under_the_mapped_address_cools_down_the_ipv4_dial() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let v4: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let mapped = std::net::SocketAddr::new(
            std::net::Ipv4Addr::new(192, 0, 2, 1)
                .to_ipv6_mapped()
                .into(),
            31337,
        );
        let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
        state.set_endpoint(watch, true);
        state.set_native_gateways([v4]);
        state.record_gateway_join(mapped, false);
        assert_eq!(
            state.plan_gateway_join(&key(1), v4, &key(10_000)),
            GatewayJoinPlan::LegacyCoolingDown
        );
    }

    /// A native gateway link that closes before it carried a frame from the
    /// gateway never worked, whatever its setup said: it cools the gateway
    /// down, so the next join goes over UDP. A link that carried a frame, and
    /// a UDP link, say nothing about native.
    #[test]
    fn a_native_gateway_link_that_closes_before_a_frame_cools_the_gateway_down() {
        use crate::transport::{QuicAvailability, QuicAvailabilityWatch};
        let (_endpoint, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let marked: std::net::SocketAddr = "192.0.2.1:31337".parse().unwrap();
        let gateway = key(10_000);
        let state = NativeConnectState::new(None).with_gateway_join_override(Some(100));
        state.set_endpoint(watch, true);
        state.set_native_gateways([marked]);
        for (native, frame_seen) in [(true, true), (false, false), (false, true)] {
            state.gateway_link_closed(marked, native, frame_seen);
            assert_eq!(
                state.plan_gateway_join(&key(1), marked, &gateway),
                GatewayJoinPlan::NativeFirst,
                "native {native}, frame seen {frame_seen}"
            );
        }
        state.gateway_link_closed(marked, true, false);
        assert_eq!(
            state.plan_gateway_join(&key(1), marked, &gateway),
            GatewayJoinPlan::LegacyCoolingDown
        );
    }

    /// `P2pConnManager::remove_connection`, the one place a transport leaves
    /// the connection map, tells the native state about the closed link.
    #[test]
    fn removing_a_connection_reports_the_closed_link_to_the_native_state() {
        const SOURCE: &str = include_str!("../../node/network_bridge/p2p_protoc.rs");
        let start = SOURCE
            .find("fn remove_connection(&mut self, addr: SocketAddr)")
            .expect("remove_connection renamed");
        let body = &SOURCE[start..start + 900];
        let body: String = body.split_whitespace().collect();
        assert!(
            body.contains(
                ".native_connect().gateway_link_closed(addr,entry.native,entry.frame_seen)"
            ),
            "remove_connection must report the closed link: {body}"
        );
        assert!(
            body.find("gateway_link_closed").unwrap() < body.find("held_native.remove").unwrap(),
            "reported before the entry's session state goes"
        );
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

    /// `src` without comments or string and char literal contents, newlines
    /// kept so line numbers hold. Not a Rust lexer: raw strings are not
    /// recognised (a needle inside one would count, which fails loud), and
    /// block comments do not nest.
    fn code_only(src: &str) -> String {
        let chars: Vec<char> = src.chars().collect();
        let mut out = String::with_capacity(src.len());
        let (mut i, mut line, mut block, mut string) = (0, false, false, false);
        while i < chars.len() {
            let c = chars[i];
            let next = chars.get(i + 1).copied();
            if c == '\n' {
                line = false;
                out.push('\n');
            } else if line {
            } else if block {
                if c == '*' && next == Some('/') {
                    block = false;
                    i += 1;
                }
            } else if string {
                if c == '\\' {
                    i += 1;
                } else if c == '"' {
                    string = false;
                }
            } else if c == '/' && next == Some('/') {
                line = true;
                i += 1;
            } else if c == '/' && next == Some('*') {
                block = true;
                i += 1;
            } else if c == '"' {
                string = true;
            } else if c == '\'' && chars.get(i + 2) == Some(&'\'') {
                i += 2;
            } else if c == '\'' && next == Some('\\') {
                // An escaped char literal such as '\n' or '\''.
                i += 3;
                while i < chars.len() && chars[i] != '\'' {
                    i += 1;
                }
            } else {
                out.push(c);
            }
            i += 1;
        }
        out
    }

    /// Native-connect demotion is gone: no node state, constant, counter
    /// recorder or arm cause for it remains anywhere in the crate's sources
    /// or tests (comments and literals stripped). A node's native offer and
    /// decisions are lowered only by the endpoint and the release percent.
    /// The needles are built at run time so this file does not contain them.
    /// Mutation: restoring any one name in any source or test file fails
    /// this. It matches names, so it cannot see demotion re-added under
    /// another name: the behavioural guards are the lifecycle tests
    /// `rescues_in_either_role_never_turn_native_off` and
    /// `a_failed_native_setup_leaves_the_udp_fallback_open_and_native_on`,
    /// and the `test_quic_native` simulations.
    #[test]
    fn no_native_demotion_state_remains_in_the_crate() {
        let needles: Vec<String> = [
            ["Native", "Demotion"],
            ["DEMOTION", "_"],
            ["STRIKE", "_WINDOW"],
            ["record_connect_native_", "demotion"],
            ["record_connect_native_acceptor_", "demotion"],
            ["ArmCause::", "Demoted"],
            ["record_native_rescue_", "strike"],
            ["fn is_", "demoted"],
            ["Native", "Demoted"],
            ["Acceptor", "Demoted"],
        ]
        .iter()
        .map(|parts| parts.concat())
        .collect();
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut stack = vec![manifest.join("src"), manifest.join("tests")];
        let mut scanned = 0usize;
        let mut hits = Vec::new();
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read the source dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    let text = std::fs::read_to_string(&path).expect("read a source file");
                    scanned += 1;
                    for (n, line) in code_only(&text).lines().enumerate() {
                        for needle in &needles {
                            if line.contains(needle.as_str()) {
                                hits.push(format!("{}:{}: {needle}", path.display(), n + 1));
                            }
                        }
                    }
                }
            }
        }
        assert!(
            scanned > 200,
            "premise: the scan reached the crate's sources and tests ({scanned} files)"
        );
        assert!(hits.is_empty(), "native demotion is back: {hits:#?}");
    }

    /// The scanner behind the pin above drops comments and literals, and a
    /// `//` inside a string does not hide the rest of its line.
    #[test]
    fn the_demotion_pin_scanner_strips_comments_and_literals_only() {
        let src =
            "let a = \"https://x\"; // note\nlet b = 1; /* c\nd */ let e = '\"'; let f = needle;\n";
        let out = code_only(src);
        assert!(out.contains("let b = 1;"));
        assert!(out.contains("let f = needle;"), "{out:?}");
        assert!(!out.contains("https"));
        assert!(!out.contains("note"));
        assert_eq!(out.lines().count(), 3, "newlines are kept");
    }
}
