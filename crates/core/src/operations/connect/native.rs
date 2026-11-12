//! The QUIC-native connect offer, the acceptor's decision, and the pair slice
//! (#5940).
//!
//! A joiner's `ConnectRequest` carries a [`NativeOffer`]; the acceptor alone
//! decides between a native QUIC connection and today's UDP setup, and its
//! `ConnectResponse` carries the [`NativeDecision`]. Both travel in the wire
//! forms of `ConnectMsg` that only peers at or above
//! [`crate::node::QUIC_NATIVE_MIN_VERSION`] receive (`connect::wire`).
//!
//! Nothing here changes how a connection is set up yet: [`NATIVE_DIAL_READY`]
//! is false, so every offer carries percent 0 and every decision is
//! [`NativeDecision::Udp`]. A response that says `Native` cannot be honoured,
//! and the joiner fails that attempt ([`joiner_response_action`]).
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
use crate::transport::metrics::{QuicEndpointState, TRANSPORT_METRICS};

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

/// Consecutive native failures, across distinct acceptors, after which a
/// joiner offers percent 0 for [`DEMOTION_COOLDOWN`].
pub(crate) const DEMOTION_FAILURES: usize = 3;

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

/// This node's readiness for native connections, as both joiner and acceptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NativeReadiness {
    /// The build can dial native, the endpoint is up and the node is not
    /// demoted.
    pub ready: bool,
    /// The effective percent ([`effective_native_percent`]).
    pub percent: u8,
}

impl NativeReadiness {
    /// Readiness from this process's QUIC endpoint state. Demotion is not
    /// wired yet ([`NativeDemotion`]), so it never lowers this.
    pub(crate) fn current() -> Self {
        let endpoint_up = TRANSPORT_METRICS.quic_endpoint_state() == QuicEndpointState::Up;
        Self::from_inputs(NATIVE_DIAL_READY, endpoint_up, false)
    }

    pub(crate) fn from_inputs(dial_ready: bool, endpoint_up: bool, demoted: bool) -> Self {
        Self {
            ready: dial_ready && endpoint_up && !demoted,
            percent: effective_native_percent(
                dial_ready,
                endpoint_up,
                demoted,
                QUIC_NATIVE_PERCENT,
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
    let offered = offer.map_or(0, NativeOffer::effective_percent);
    let native =
        offered > 0 && own.ready && bucket < offered.min(own.percent.min(100)) && !holds_transport;
    if native {
        NativeDecision::Native
    } else {
        NativeDecision::Udp
    }
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
    /// `Native` answered a real offer, but this build has no native dial.
    NativeUnsupported,
}

/// What the joiner does with a response's decision, given the offer it sent.
/// `None`, `Udp` and a decision this release does not know keep today's
/// behaviour; `Native` fails the attempt, since this build cannot set up a
/// native connection.
pub(crate) fn joiner_response_action(
    sent: Option<NativeOffer>,
    decision: Option<NativeDecision>,
) -> Result<(), JoinerDecisionFailure> {
    match decision {
        None | Some(NativeDecision::Udp | NativeDecision::Unknown(_)) => Ok(()),
        Some(NativeDecision::Native) => {
            if sent.is_none_or(|o| o.effective_percent() == 0) {
                Err(JoinerDecisionFailure::DecisionWithoutOffer)
            } else {
                Err(JoinerDecisionFailure::NativeUnsupported)
            }
        }
    }
}

/// Lowers a node's native offer (or, on an acceptor, its decisions) after
/// repeated native failures. Not wired yet: the native dial it counts does not
/// exist in this release; the connect op wires it when it gains one.
///
/// [`DEMOTION_FAILURES`] failures in a row, each with a different acceptor,
/// demote the node for [`DEMOTION_COOLDOWN`]; a success resets the streak.
/// Repeated failures with one acceptor count once, so one broken peer cannot
/// demote a node. It only ever lowers: it can make a node offer 0, never more.
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct NativeDemotion {
    /// Distinct acceptors in the current failure streak, at most
    /// [`DEMOTION_FAILURES`].
    streak: Vec<TransportPublicKey>,
    demoted_until: Option<Instant>,
    demotions: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
impl NativeDemotion {
    pub(crate) fn new() -> Self {
        Self {
            streak: Vec::with_capacity(DEMOTION_FAILURES),
            demoted_until: None,
            demotions: 0,
        }
    }

    /// A native attempt with `acceptor` failed.
    pub(crate) fn record_failure(&mut self, acceptor: &TransportPublicKey, now: Instant) {
        if self.is_demoted(now) {
            return;
        }
        if !self.streak.contains(acceptor) {
            self.streak.push(acceptor.clone());
        }
        if self.streak.len() >= DEMOTION_FAILURES {
            self.streak.clear();
            self.demoted_until = Some(now + DEMOTION_COOLDOWN);
            self.demotions += 1;
        }
    }

    /// A native attempt succeeded: the streak starts over.
    pub(crate) fn record_success(&mut self) {
        self.streak.clear();
    }

    pub(crate) fn is_demoted(&self, now: Instant) -> bool {
        self.demoted_until.is_some_and(|until| now < until)
    }

    /// How many times the node was demoted.
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

    #[test]
    fn joiner_fails_only_on_native() {
        use JoinerDecisionFailure::*;
        let zero = Some(NativeOffer::capable(0));
        let five = Some(NativeOffer::capable(5));
        for sent in [None, zero, five] {
            assert_eq!(joiner_response_action(sent, None), Ok(()));
            assert_eq!(
                joiner_response_action(sent, Some(NativeDecision::Udp)),
                Ok(())
            );
        }
        let native = Some(NativeDecision::Native);
        assert_eq!(
            joiner_response_action(None, native),
            Err(DecisionWithoutOffer)
        );
        assert_eq!(
            joiner_response_action(zero, native),
            Err(DecisionWithoutOffer)
        );
        assert_eq!(joiner_response_action(five, native), Err(NativeUnsupported));
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
                assert_eq!(joiner_response_action(offer, Some(decision)), Ok(()));
            }
        }
    }

    #[test]
    fn demotion_after_three_distinct_acceptors() {
        let now = Instant::now();
        let mut demotion = NativeDemotion::new();
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
        let mut demotion = NativeDemotion::new();
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
        let mut demotion = NativeDemotion::new();
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
        let mut demotion = NativeDemotion::new();
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
