//! Gateway admission of native joins from sources nobody named (#5940,
//! phase B; A7b). **Dark**: [`GATEWAY_NATIVE_ADMISSION`] is false, so no
//! node builds any of this in production; tests turn it on per endpoint
//! ([`super::QuicEndpoint::enable_gateway_admission`]).
//!
//! Every other QUIC attempt a node accepts comes from an address it allowed
//! first, with the key it expects there. A gateway admitting a joiner it
//! has never heard of has neither, so, on a gateway with admission on:
//!
//! 1. The demux passes a datagram from a source NOT on the allow-set to a
//!    separate bounded queue only if it can open a connection: a long
//!    header, at least [`MIN_INITIAL_DATAGRAM`] bytes (RFC 9000 14.1: a
//!    client pads every datagram carrying an Initial to that size), and a
//!    QUIC version this endpoint speaks ([`unknown_source_datagram`]). The
//!    endpoint reads that queue only when the established queue is empty.
//!    Classifying by source first leaves the established path exactly as it
//!    is for sources on the allow-set.
//! 2. An attempt from such a source that has not returned a Retry token gets
//!    a Retry ([`Verdict::Retry`]), which proves the source can receive at
//!    its address and creates no state.
//! 3. At most one validated attempt per second per source address (IP and
//!    port), as the UDP transport limits intro decryption
//!    (`connection_handler::ASYM_DECRYPTION_RATE_LIMIT`).
//! 4. At most [`GLOBAL_RATE_PER_SEC`] validated attempts per second for the
//!    node, a token bucket of one second's worth.
//!
//! An attempt over a limit is ignored, with no reason given. An admitted one
//! puts its address on the allow-set (so the handshake's later, shorter
//! datagrams take the established path) and is accepted with
//! [`super::identity::ProvenClientKey`], offering the native ALPN only.
//!
//! The decision is synchronous and runs in the accept loop. Counted as
//! `quic_gateway_admission_<label>` ([`GatewayAdmission`]).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use crate::transport::metrics::GatewayAdmission;
use crate::transport::normalize_mapped_addr;

/// Whether a gateway admits native joins from sources it was not told
/// about. False until the B prerequisites hold (QUIC-NATIVE-CONNECT 3.7): the
/// change that turns it on is a one-line change here.
pub(crate) const GATEWAY_NATIVE_ADMISSION: bool = false;

/// Whether a node in this role admits native joins from unknown sources: a
/// gateway, and only while [`GATEWAY_NATIVE_ADMISSION`] is on.
pub(crate) fn enabled(is_gateway: bool) -> bool {
    enabled_with(is_gateway, GATEWAY_NATIVE_ADMISSION)
}

fn enabled_with(is_gateway: bool, switch: bool) -> bool {
    is_gateway && switch
}

/// The smallest datagram that can carry a client's first Initial (RFC 9000
/// section 14.1).
pub(crate) const MIN_INITIAL_DATAGRAM: usize = 1200;

/// The QUIC versions this endpoint speaks (`config::endpoint_config`).
const KNOWN_VERSIONS: [u32; 1] = [1];

/// How many datagrams from unknown sources the demux holds for the endpoint
/// (provisional).
pub(crate) const UNKNOWN_SOURCE_QUEUE_DATAGRAMS: usize = 128;

/// One validated attempt per source address per this long.
pub(crate) const PER_ADDRESS_INTERVAL: Duration = Duration::from_secs(1);

/// Validated attempts a gateway admits per second, at most (provisional,
/// sized at about eight times the measured peak join rate).
pub(crate) const GLOBAL_RATE_PER_SEC: u64 = 50;

/// The per-address record is pruned of entries older than
/// [`PER_ADDRESS_INTERVAL`] once it holds this many. Entries are added only
/// for admitted attempts, which the global rate bounds to about
/// `2 x GLOBAL_RATE_PER_SEC` per second, so live entries never come near it.
const PER_ADDRESS_PRUNE_AT: usize = 1024;

/// Why a datagram from an unknown source cannot open a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NotAnInitial {
    /// A short header: only a connection's established traffic uses one.
    ShortHeader,
    /// Shorter than [`MIN_INITIAL_DATAGRAM`].
    Runt,
    /// A version this endpoint does not speak.
    UnknownVersion,
}

impl NotAnInitial {
    /// The counter a refused datagram is recorded under.
    pub(crate) fn outcome(self) -> GatewayAdmission {
        match self {
            Self::ShortHeader => GatewayAdmission::ShortHeader,
            Self::Runt => GatewayAdmission::Runt,
            Self::UnknownVersion => GatewayAdmission::UnknownVersion,
        }
    }
}

/// Whether a QUIC-class datagram from a source not on the allow-set can
/// open a connection: a long header (form bit set), at least
/// [`MIN_INITIAL_DATAGRAM`] bytes, and a version this endpoint speaks.
pub(crate) fn unknown_source_datagram(datagram: &[u8]) -> Result<(), NotAnInitial> {
    let Some(&first) = datagram.first() else {
        return Err(NotAnInitial::ShortHeader);
    };
    if first & 0x80 == 0 {
        return Err(NotAnInitial::ShortHeader);
    }
    if datagram.len() < MIN_INITIAL_DATAGRAM {
        return Err(NotAnInitial::Runt);
    }
    let version = u32::from_be_bytes([datagram[1], datagram[2], datagram[3], datagram[4]]);
    if KNOWN_VERSIONS.contains(&version) {
        Ok(())
    } else {
        Err(NotAnInitial::UnknownVersion)
    }
}

/// What the accept loop does with an attempt from an unknown source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Answer with a Retry: the source has not shown it receives at its
    /// address.
    Retry,
    /// Ignore: this address had a validated attempt admitted within
    /// [`PER_ADDRESS_INTERVAL`].
    ThrottledAddress,
    /// Ignore: the node's rate is spent.
    ThrottledGlobal,
    /// Accept, with the client's own key proven in the handshake.
    Admit,
}

impl Verdict {
    /// The counter the verdict is recorded under.
    pub(crate) fn outcome(self) -> GatewayAdmission {
        match self {
            Self::Retry => GatewayAdmission::Retried,
            Self::ThrottledAddress => GatewayAdmission::ThrottledAddress,
            Self::ThrottledGlobal => GatewayAdmission::ThrottledGlobal,
            Self::Admit => GatewayAdmission::Admitted,
        }
    }
}

/// The per-address throttle and the global rate of one gateway endpoint.
pub(crate) struct Admission {
    /// Nanoseconds on the endpoint's clock.
    now: Box<dyn Fn() -> u64 + Send + Sync>,
    state: parking_lot::Mutex<State>,
}

struct State {
    /// When each address last had an attempt admitted.
    last_admitted: HashMap<SocketAddr, u64>,
    /// The global bucket's credit, in nanosecond-tokens: one admission costs
    /// one second's worth, `1e9`.
    credit: u64,
    /// When the credit was last refilled.
    refilled_at: u64,
}

const NANOS_PER_TOKEN: u64 = 1_000_000_000;

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission").finish_non_exhaustive()
    }
}

impl Admission {
    /// Admission on the clock `now` (nanoseconds), starting with a full
    /// bucket.
    pub(crate) fn new(now: impl Fn() -> u64 + Send + Sync + 'static) -> Self {
        let at = now();
        Self {
            now: Box::new(now),
            state: parking_lot::Mutex::new(State {
                last_admitted: HashMap::new(),
                credit: GLOBAL_RATE_PER_SEC * NANOS_PER_TOKEN,
                refilled_at: at,
            }),
        }
    }

    /// Decide an attempt from `from` (an address not on the allow-set).
    /// `validated` is whether it returned a Retry token. The checks run in
    /// order, Retry, address, global, and only an admission is recorded, so
    /// a refused attempt costs its address and the node nothing.
    pub(crate) fn decide(&self, from: SocketAddr, validated: bool) -> Verdict {
        if !validated {
            return Verdict::Retry;
        }
        let from = normalize_mapped_addr(from);
        let now = (self.now)();
        let mut state = self.state.lock();
        let interval = PER_ADDRESS_INTERVAL.as_nanos() as u64;
        if state
            .last_admitted
            .get(&from)
            .is_some_and(|at| *at <= now && now - *at < interval)
        {
            return Verdict::ThrottledAddress;
        }
        let elapsed = now.saturating_sub(state.refilled_at);
        state.credit = state
            .credit
            .saturating_add(elapsed.saturating_mul(GLOBAL_RATE_PER_SEC))
            .min(GLOBAL_RATE_PER_SEC * NANOS_PER_TOKEN);
        state.refilled_at = now;
        if state.credit < NANOS_PER_TOKEN {
            return Verdict::ThrottledGlobal;
        }
        state.credit -= NANOS_PER_TOKEN;
        if state.last_admitted.len() >= PER_ADDRESS_PRUNE_AT {
            state
                .last_admitted
                .retain(|_, at| *at > now || now - *at < interval);
        }
        state.last_admitted.insert(from, now);
        Verdict::Admit
    }

    /// Entries in the per-address record (tests).
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.state.lock().last_admitted.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn clock() -> (Arc<AtomicU64>, Admission) {
        let now = Arc::new(AtomicU64::new(1_000_000_000_000));
        let reader = now.clone();
        (now, Admission::new(move || reader.load(Ordering::Relaxed)))
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([192, 0, 2, 1], port))
    }

    const SECOND: u64 = 1_000_000_000;

    /// Admission is on only for a gateway with the switch on, and the switch
    /// ships off.
    #[test]
    fn admission_is_dark_and_gateway_only() {
        // A compile-time check: turning the switch on fails this build.
        const { assert!(!GATEWAY_NATIVE_ADMISSION, "ships dark until phase B") };
        assert!(!enabled(true));
        assert!(!enabled(false));
        assert!(enabled_with(true, true));
        assert!(!enabled_with(false, true), "never on a peer");
        assert!(!enabled_with(true, false));
    }

    /// The node turns admission on in one place, `bind_udp_listener`, and
    /// only inside the `enabled(is_gateway)` branch, so with the switch off
    /// no endpoint gets the unknown-source lane or an admission decision.
    #[test]
    fn the_node_enables_admission_only_behind_the_switch() {
        let source = include_str!("../connection_handler.rs");
        let gate = "if super::quic::admission::enabled(is_gateway) {";
        let gates: Vec<usize> = source.match_indices(gate).map(|(at, _)| at).collect();
        assert_eq!(gates.len(), 1, "one gate");
        let calls: Vec<usize> = source
            .match_indices(".enable_gateway_admission(")
            .map(|(at, _)| at)
            .filter(|at| {
                let line_start = source[..*at].rfind('\n').map_or(0, |n| n + 1);
                !source[line_start..*at].trim_start().starts_with("//")
            })
            .collect();
        assert_eq!(calls.len(), 2, "one call per clock");
        let gate_end = gates[0]
            + source[gates[0]..]
                .find("\n        }\n")
                .expect("gate block");
        for call in calls {
            assert!(
                (gates[0]..gate_end).contains(&call),
                "enable_gateway_admission called outside the gate"
            );
        }
    }

    /// An admitted joiner's key is read only after its handshake completed:
    /// `JoinerIncoming::establish` awaits the `Connecting` before
    /// `proven_client_key`, and nothing in the transport takes a 0.5-RTT
    /// connection (`into_0rtt`), on which rustls would already show the
    /// client's certificate before its Finished.
    #[test]
    fn proven_key_is_read_only_after_the_handshake_completes() {
        let quic = include_str!("../quic.rs");
        let signature =
            "    pub(crate) async fn establish(self) -> Result<QuicConnection, JoinerError> {";
        let start = quic.find(signature).expect("JoinerIncoming::establish");
        let len = quic[start..].find("\n    }\n").expect("its end");
        let body = &quic[start..start + len];
        let at_statement = |needle: &str| {
            body.match_indices(needle)
                .find(|(i, _)| {
                    let line = body[..*i].rfind('\n').map_or(0, |n| n + 1);
                    let before = body[line..*i].trim();
                    before.is_empty() || before.starts_with("let ")
                })
                .map(|(i, _)| i)
                .unwrap_or_else(|| panic!("`{needle}` is not called in establish"))
        };
        let awaited = at_statement("let connection = connecting.await?;");
        let read = at_statement("let Some(peer) = identity::proven_client_key(&connection)");
        assert!(
            awaited < read,
            "the key is read after the handshake is awaited"
        );
        for (name, source) in [
            ("quic.rs", quic),
            ("upgrade/driver.rs", include_str!("upgrade/driver.rs")),
            ("native/setup.rs", include_str!("native/setup.rs")),
            ("native/exchange.rs", include_str!("native/exchange.rs")),
        ] {
            assert!(
                !source.contains(".into_0rtt("),
                "{name} takes a 0.5-RTT connection"
            );
        }
    }

    /// Only a long-header datagram of at least 1200 bytes with a known
    /// version is passed on.
    #[test]
    fn only_a_possible_initial_is_passed_on() {
        let initial = |first: u8, len: usize, version: u32| {
            let mut datagram = vec![0u8; len];
            datagram[0] = first;
            datagram[1..5].copy_from_slice(&version.to_be_bytes());
            datagram
        };
        assert_eq!(unknown_source_datagram(&initial(0xC3, 1200, 1)), Ok(()));
        assert_eq!(unknown_source_datagram(&initial(0xC3, 1500, 1)), Ok(()));
        assert_eq!(
            unknown_source_datagram(&initial(0xC3, 1199, 1)),
            Err(NotAnInitial::Runt)
        );
        assert_eq!(
            unknown_source_datagram(&initial(0x43, 1200, 1)),
            Err(NotAnInitial::ShortHeader)
        );
        assert_eq!(unknown_source_datagram(&[]), Err(NotAnInitial::ShortHeader));
        for version in [0, 2, 0x6b33_43cf, 0xff00_001d] {
            assert_eq!(
                unknown_source_datagram(&initial(0xC3, 1200, version)),
                Err(NotAnInitial::UnknownVersion),
                "{version:#x}"
            );
        }
        // A runt is a runt whatever its version: nothing past the header's
        // first five bytes is read for one.
        assert_eq!(
            unknown_source_datagram(&initial(0xC3, 5, 7)),
            Err(NotAnInitial::Runt)
        );
    }

    /// An attempt without a token is retried, and a retry records nothing.
    #[test]
    fn an_unvalidated_attempt_is_retried_and_costs_nothing() {
        let (_now, admission) = clock();
        for _ in 0..1000 {
            assert_eq!(admission.decide(addr(1), false), Verdict::Retry);
        }
        assert_eq!(admission.tracked(), 0);
        assert_eq!(admission.decide(addr(1), true), Verdict::Admit);
    }

    /// One validated attempt per second per address, keyed by IP and port;
    /// a throttled attempt does not push the address's next turn back.
    #[test]
    fn one_attempt_per_second_per_address() {
        let (now, admission) = clock();
        assert_eq!(admission.decide(addr(1), true), Verdict::Admit);
        now.fetch_add(SECOND - 1, Ordering::Relaxed);
        assert_eq!(admission.decide(addr(1), true), Verdict::ThrottledAddress);
        assert_eq!(
            admission.decide(addr(2), true),
            Verdict::Admit,
            "another port of the same IP is another address"
        );
        let v6_mapped =
            SocketAddr::from((std::net::Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(), 1));
        assert_eq!(
            admission.decide(v6_mapped, true),
            Verdict::ThrottledAddress,
            "an IPv4-mapped address is the same address"
        );
        now.fetch_add(1, Ordering::Relaxed);
        assert_eq!(admission.decide(addr(1), true), Verdict::Admit);
    }

    /// The node admits a second's worth at once, then the rate.
    #[test]
    fn the_global_rate_is_fifty_per_second() {
        let (now, admission) = clock();
        let mut port = 0;
        let mut admitted = |n: usize| {
            (0..n)
                .filter(|_| {
                    port += 1;
                    admission.decide(addr(port), true) == Verdict::Admit
                })
                .count()
        };
        assert_eq!(admitted(60), 50, "a full bucket");
        now.fetch_add(SECOND / 10, Ordering::Relaxed);
        assert_eq!(admitted(60), 5, "a tenth of a second refills five");
        now.fetch_add(10 * SECOND, Ordering::Relaxed);
        assert_eq!(admitted(60), 50, "never more than a second's worth");
        assert_eq!(GLOBAL_RATE_PER_SEC, 50);
    }

    /// A throttled-by-rate attempt leaves its address free, and the record
    /// is pruned of entries older than the per-address interval once it
    /// reaches `PER_ADDRESS_PRUNE_AT`, so it never grows past that however
    /// many distinct addresses are admitted.
    #[test]
    fn refusals_record_nothing_and_old_entries_are_pruned() {
        let (now, admission) = clock();
        for port in 0..50 {
            assert_eq!(admission.decide(addr(port), true), Verdict::Admit);
        }
        assert_eq!(admission.decide(addr(100), true), Verdict::ThrottledGlobal);
        now.fetch_add(SECOND / 50, Ordering::Relaxed);
        assert_eq!(
            admission.decide(addr(100), true),
            Verdict::Admit,
            "a refusal by rate did not throttle the address"
        );
        // 2000 distinct addresses, 50 a second: each is admitted (the rate
        // refills each second), and without pruning the record would hold
        // all of them.
        let mut admitted = 0;
        for round in 0..40u16 {
            now.fetch_add(SECOND, Ordering::Relaxed);
            for i in 0..50u16 {
                let n = round * 50 + i;
                let from = SocketAddr::from(([198, 51, (n >> 8) as u8, n as u8], 9));
                if admission.decide(from, true) == Verdict::Admit {
                    admitted += 1;
                }
                assert!(admission.tracked() <= PER_ADDRESS_PRUNE_AT);
            }
        }
        assert_eq!(admitted, 2000);
        assert!(admission.tracked() < 2000, "pruned");
    }

    /// A clock that goes back does not throttle an address forever.
    #[test]
    fn a_clock_going_back_does_not_hold_an_address() {
        let (now, admission) = clock();
        assert_eq!(admission.decide(addr(1), true), Verdict::Admit);
        now.fetch_sub(100 * SECOND, Ordering::Relaxed);
        assert_eq!(admission.decide(addr(1), true), Verdict::Admit);
    }
}
