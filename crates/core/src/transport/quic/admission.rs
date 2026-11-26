//! Gateway admission of native joins from sources nobody named (#5940,
//! phase B; A7b). **On** since the floor release: [`GATEWAY_NATIVE_ADMISSION`]
//! is true, so every node that is a gateway (published in the index or not)
//! and configured `transport-quic = "prefer"` (the default) admits native
//! joins from unknown sources from release day. A `shadow` or `off` gateway
//! does not: native joins carry application data, which `shadow` keeps on
//! UDP. The index mark (`GatewayConfig::native`) only steers which gateways
//! default joiners dial natively; no gateway reads its own mark. There is no
//! switch in the binary; an operator turns it off on one gateway with
//! `transport-quic = "off"` (or `"shadow"`), which stays possible only while
//! UDP exists ([`super::breaker::UDP_FALLBACK`] is true; the UDP-removal
//! release drops it), and reversing it for everyone takes a release. Tests
//! also turn it on
//! per endpoint ([`super::QuicEndpoint::enable_gateway_admission`]) or per
//! listener ([`force_on_for_test`]).
//!
//! Every other QUIC attempt a node accepts comes from an address it allowed
//! first, with the key it expects there. A gateway admitting a joiner it
//! has never heard of has neither, so, on a gateway with admission on:
//!
//! 1. The demux passes a datagram from a source NOT on the allow-set to a
//!    separate bounded queue only if it can open a connection: a long
//!    header, at least [`MIN_INITIAL_DATAGRAM`] bytes (RFC 9000 14.1: a
//!    client pads every datagram carrying an Initial to that size), a QUIC
//!    version this endpoint speaks, and the Initial packet type
//!    ([`unknown_source_datagram`]). The endpoint reads that queue only when
//!    the established queue is empty. Classifying by source first leaves the
//!    established path as it is for sources on the allow-set.
//! 2. An attempt from such a source that has not returned a Retry token gets
//!    a Retry ([`Refusal::Retry`]), which proves the source can receive at
//!    its address and creates no state.
//! 3. At most one validated attempt per second per source address (IP and
//!    port), as the UDP transport limits intro decryption
//!    (`connection_handler::ASYM_DECRYPTION_RATE_LIMIT`).
//! 4. At most [`GLOBAL_RATE_PER_SEC`] validated attempts per second for the
//!    node, a token bucket of one second's worth.
//!
//! 5. A joiner takes a handshake-phase charge against the node's connection
//!    cap ([`super::upgrade::policy::QuicPathPermits`]) from its admission,
//!    released when its join ends or re-charged at the full rate at
//!    promotion; with no room, or no cap attached yet, the attempt is
//!    ignored ([`GatewayAdmission::JoinerCapFull`]). Its address is marked
//!    as a joiner on the allow-set, so all unpromoted joiners share one
//!    bounded part of the ingress queue.
//!
//! An attempt over a limit is ignored, with no reason given. An admitted one
//! is recorded at once, against both limits, as an [`AdmissionTicket`]; the
//! ticket gives the admission back (counted `refunded`) if it is dropped
//! before the join's handshake starts (the allow-set full, the attempt
//! dropped on a full queue or at shutdown). An admitted attempt puts its
//! address on the allow-set (so the handshake's later, shorter datagrams take
//! the established path) and is accepted with
//! [`super::identity::ProvenClientKey`], offering the native ALPN only.
//!
//! The decision is synchronous and runs in the accept loop. Counted as
//! `quic_gateway_admission_<label>` ([`GatewayAdmission`]).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::transport::metrics::GatewayAdmission;
use crate::transport::normalize_mapped_addr;

/// Whether a gateway admits native joins from sources it was not told
/// about. True since the floor release. Turning it on waited for:
///
/// - (done, A7b PR 6) joiners counted inside the node's connection cap at
///   the handshake-phase charge, and all unpromoted joiners held to one small
///   share of the QUIC ingress queue
///   (`demux::QUIC_INGRESS_JOINER_DATAGRAMS`);
/// - (done, phase B PR 2) the node takes the admitted connection
///   (`QuicUpgrades::hand_off_join`): its ConnectRequest arrives on it, and
///   the connect op promotes it;
/// - (done) the rest of the B prerequisites: the endpoint restart or exit on
///   `Down`, the gateway breaker action, the update check on repeated
///   handshake failure and before a fatal exit, the `ProvenClientKey`
///   property tests, and the RustSec watch.
///
/// Every gateway admits from release day, whether or not the index marks it
/// native: `enabled_at` reads only `is_gateway` and this constant. The mark
/// (an operations step after a gateway runs this build) only steers which
/// gateways default joiners dial natively. Accepted ordering: the unknown-source
/// lane is exposed while UDP fallback still exists, rather than at the
/// UDP-removal release, where it is mandatory.
pub(crate) const GATEWAY_NATIVE_ADMISSION: bool = true;

/// Whether a node in this role admits native joins from unknown sources: a
/// gateway, and only while [`GATEWAY_NATIVE_ADMISSION`] is on.
pub(crate) fn enabled(is_gateway: bool) -> bool {
    enabled_with(is_gateway, GATEWAY_NATIVE_ADMISSION)
}

/// [`enabled`] for the listener bound at `bind_addr`, which a test can turn
/// on for that one listener ([`force_on_for_test`]), as a simulation can
/// (`SimNetwork::with_gateway_native_admission`), or a simulation can turn it
/// off for one gateway (`SimNetwork::with_gateway_native_admission_off`, a
/// gateway on a release before admission); in a build without tests
/// or the `testing` feature it is [`enabled`].
pub(crate) fn enabled_at(is_gateway: bool, bind_addr: SocketAddr) -> bool {
    #[cfg(any(test, feature = "testing"))]
    {
        let forced_on = {
            #[cfg(test)]
            let by_test = FORCED_ON.contains(&bind_addr);
            #[cfg(not(test))]
            let by_test = false;
            by_test
                || crate::transport::in_memory_socket::gateway_native_admission_forced(bind_addr)
        };
        let forced_off =
            crate::transport::in_memory_socket::gateway_native_admission_forced_off(bind_addr);
        assert!(
            !(forced_on && forced_off),
            "a simulation turned admission both on and off for {bind_addr}"
        );
        if is_gateway && forced_on {
            return true;
        }
        if forced_off {
            return false;
        }
    }
    let _ = bind_addr;
    enabled(is_gateway)
}

fn enabled_with(is_gateway: bool, switch: bool) -> bool {
    is_gateway && switch
}

/// Listener addresses a test turned admission on for, keyed by address so
/// tests running in one process cannot turn it on for each other.
#[cfg(test)]
static FORCED_ON: std::sync::LazyLock<dashmap::DashSet<SocketAddr>> =
    std::sync::LazyLock::new(dashmap::DashSet::new);

/// Turn admission on for a gateway listener bound at `addr`, while the
/// returned guard lives (tests).
#[cfg(test)]
pub(crate) fn force_on_for_test(addr: SocketAddr) -> ForcedOn {
    FORCED_ON.insert(addr);
    ForcedOn(addr)
}

/// Undoes [`force_on_for_test`] when dropped.
#[cfg(test)]
pub(crate) struct ForcedOn(SocketAddr);

#[cfg(test)]
impl Drop for ForcedOn {
    fn drop(&mut self) {
        FORCED_ON.remove(&self.0);
    }
}

/// The smallest datagram that can carry a client's first Initial (RFC 9000
/// section 14.1).
pub(crate) const MIN_INITIAL_DATAGRAM: usize = 1200;

/// The QUIC versions this endpoint speaks (`config::endpoint_config`).
const KNOWN_VERSIONS: [u32; 1] = [1];

/// The long-header packet type bits (RFC 9000 17.2), and the value for an
/// Initial in QUIC version 1.
const LONG_PACKET_TYPE_MASK: u8 = 0x30;
const INITIAL_PACKET_TYPE: u8 = 0x00;

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
    /// A long header of a type other than Initial.
    NotInitial,
}

impl NotAnInitial {
    /// The counter a refused datagram is recorded under.
    pub(crate) fn outcome(self) -> GatewayAdmission {
        match self {
            Self::ShortHeader => GatewayAdmission::ShortHeader,
            Self::Runt => GatewayAdmission::Runt,
            Self::UnknownVersion => GatewayAdmission::UnknownVersion,
            Self::NotInitial => GatewayAdmission::NotInitial,
        }
    }
}

/// Whether a QUIC-class datagram from a source not on the allow-set can
/// open a connection: a long header (form bit set), at least
/// [`MIN_INITIAL_DATAGRAM`] bytes, a version this endpoint speaks, and, in
/// that version, the Initial packet type. The type bits are read only after
/// the version, since their meaning depends on it.
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
    if !KNOWN_VERSIONS.contains(&version) {
        return Err(NotAnInitial::UnknownVersion);
    }
    if first & LONG_PACKET_TYPE_MASK != INITIAL_PACKET_TYPE {
        return Err(NotAnInitial::NotInitial);
    }
    Ok(())
}

/// Why the accept loop does not admit an attempt from an unknown source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// Answer with a Retry: the source has not shown it receives at its
    /// address.
    Retry,
    /// Ignore: this address had a validated attempt admitted within
    /// [`PER_ADDRESS_INTERVAL`].
    ThrottledAddress,
    /// Ignore: the node's rate is spent.
    ThrottledGlobal,
}

impl Refusal {
    /// The counter the refusal is recorded under.
    pub(crate) fn outcome(self) -> GatewayAdmission {
        match self {
            Self::Retry => GatewayAdmission::Retried,
            Self::ThrottledAddress => GatewayAdmission::ThrottledAddress,
            Self::ThrottledGlobal => GatewayAdmission::ThrottledGlobal,
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
const CREDIT_CAP: u64 = GLOBAL_RATE_PER_SEC * NANOS_PER_TOKEN;

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission").finish_non_exhaustive()
    }
}

/// An admission recorded against both limits. Dropped without
/// [`Self::keep`], it is given back: the address's previous record is
/// restored (unless a later admission replaced it) and the token returned.
#[must_use = "dropping a ticket gives the admission back"]
pub(crate) struct AdmissionTicket {
    admission: Arc<Admission>,
    from: SocketAddr,
    at: u64,
    previous: Option<u64>,
    kept: bool,
}

impl AdmissionTicket {
    /// The admission is used: the join's handshake starts.
    pub(crate) fn keep(mut self) {
        self.kept = true;
    }
}

impl std::fmt::Debug for AdmissionTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionTicket")
            .field("from", &self.from)
            .finish_non_exhaustive()
    }
}

impl Drop for AdmissionTicket {
    fn drop(&mut self) {
        if self.kept {
            return;
        }
        self.admission.give_back(self.from, self.at, self.previous);
        crate::transport::TRANSPORT_METRICS.record_gateway_admission(GatewayAdmission::Refunded);
    }
}

impl Admission {
    /// Admission on the clock `now` (nanoseconds), starting with a full
    /// bucket.
    pub(crate) fn new(now: impl Fn() -> u64 + Send + Sync + 'static) -> Arc<Self> {
        let at = now();
        Arc::new(Self {
            now: Box::new(now),
            state: parking_lot::Mutex::new(State {
                last_admitted: HashMap::new(),
                credit: CREDIT_CAP,
                refilled_at: at,
            }),
        })
    }

    /// Decide an attempt from `from` (an address not on the allow-set).
    /// `validated` is whether it returned a Retry token. The checks run in
    /// order, Retry, address, global. A refusal records nothing; an
    /// admission is recorded against both limits and returned as a ticket,
    /// which gives it back unless kept.
    pub(crate) fn decide(
        self: &Arc<Self>,
        from: SocketAddr,
        validated: bool,
    ) -> Result<AdmissionTicket, Refusal> {
        if !validated {
            return Err(Refusal::Retry);
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
            return Err(Refusal::ThrottledAddress);
        }
        let elapsed = now.saturating_sub(state.refilled_at);
        state.credit = state
            .credit
            .saturating_add(elapsed.saturating_mul(GLOBAL_RATE_PER_SEC))
            .min(CREDIT_CAP);
        state.refilled_at = now;
        if state.credit < NANOS_PER_TOKEN {
            return Err(Refusal::ThrottledGlobal);
        }
        state.credit -= NANOS_PER_TOKEN;
        if state.last_admitted.len() >= PER_ADDRESS_PRUNE_AT {
            state
                .last_admitted
                .retain(|_, at| *at > now || now - *at < interval);
        }
        let previous = state.last_admitted.insert(from, now);
        Ok(AdmissionTicket {
            admission: self.clone(),
            from,
            at: now,
            previous,
            kept: false,
        })
    }

    /// Undo an admission at `at` for `from`: the token goes back, and the
    /// address's record goes back to `previous` if it is still this
    /// admission's.
    fn give_back(&self, from: SocketAddr, at: u64, previous: Option<u64>) {
        let mut state = self.state.lock();
        state.credit = state.credit.saturating_add(NANOS_PER_TOKEN).min(CREDIT_CAP);
        if state.last_admitted.get(&from) == Some(&at) {
            match previous {
                Some(earlier) => {
                    state.last_admitted.insert(from, earlier);
                }
                None => {
                    state.last_admitted.remove(&from);
                }
            }
        }
    }

    /// Entries in the per-address record (tests).
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.state.lock().last_admitted.len()
    }
}

#[cfg(test)]
pub(in crate::transport::quic) mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn clock() -> (Arc<AtomicU64>, Arc<Admission>) {
        let now = Arc::new(AtomicU64::new(1_000_000_000_000));
        let reader = now.clone();
        (now, Admission::new(move || reader.load(Ordering::Relaxed)))
    }

    /// A decision as a value, with an admission kept (its handshake started).
    #[derive(Debug, PartialEq, Eq)]
    pub(in crate::transport::quic) enum Verdict {
        Retry,
        ThrottledAddress,
        ThrottledGlobal,
        Admit,
    }

    pub(in crate::transport::quic) trait DecideKept {
        fn decide_kept(&self, from: SocketAddr, validated: bool) -> Verdict;
    }

    impl DecideKept for Arc<Admission> {
        fn decide_kept(&self, from: SocketAddr, validated: bool) -> Verdict {
            match self.decide(from, validated) {
                Ok(ticket) => {
                    ticket.keep();
                    Verdict::Admit
                }
                Err(Refusal::Retry) => Verdict::Retry,
                Err(Refusal::ThrottledAddress) => Verdict::ThrottledAddress,
                Err(Refusal::ThrottledGlobal) => Verdict::ThrottledGlobal,
            }
        }
    }

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([192, 0, 2, 1], port))
    }

    const SECOND: u64 = 1_000_000_000;

    /// Admission is on for a gateway and never for a peer; the switch ships
    /// on in the floor release.
    #[test]
    fn admission_is_on_for_gateways_only() {
        // A compile-time check: turning the switch off fails this build, so
        // reversing the floor is a deliberate edit of this pin.
        const { assert!(GATEWAY_NATIVE_ADMISSION, "on since the floor release") };
        assert!(enabled(true));
        assert!(!enabled(false), "never on a peer");
        assert!(enabled_with(true, true));
        assert!(!enabled_with(false, true), "never on a peer");
        assert!(!enabled_with(true, false));
    }

    /// The node turns admission on in one place, `bind_udp_listener`, and
    /// only inside the `enabled_at(is_gateway, bind_addr)` branch, so with the
    /// switch off no endpoint gets the unknown-source lane or an admission
    /// decision. Every source file of the crate is read (test modules in
    /// their own files are skipped), so a call added anywhere else fails.
    #[test]
    fn the_node_enables_admission_only_behind_the_switch() {
        fn sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
            for entry in std::fs::read_dir(dir).expect("source dir") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    sources(&path, out);
                } else if path.extension().is_some_and(|e| e == "rs") {
                    out.push(path);
                }
            }
        }
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        sources(&root, &mut files);
        assert!(files.len() > 100, "read the crate's sources");
        let mut production_calls = Vec::new();
        for file in &files {
            let is_test_file = file.file_name().is_some_and(|n| n == "tests.rs")
                || file.components().any(|c| c.as_os_str() == "tests");
            if is_test_file {
                continue;
            }
            let source = std::fs::read_to_string(file).expect("source file");
            // An inline test module (this pin's own needle is in one).
            let tests_at = source
                .match_indices("\n#[cfg(test)]\n")
                .map(|(at, cfg)| at + cfg.len())
                .find(|after| {
                    let line = source[*after..].lines().next().unwrap_or("");
                    line.ends_with("mod tests {")
                })
                .unwrap_or(source.len());
            for (at, _) in source[..tests_at].match_indices(".enable_gateway_admission(") {
                let line = source[..at].rfind('\n').map_or(0, |n| n + 1);
                if !source[line..at].trim_start().starts_with("//") {
                    production_calls.push((file.clone(), at));
                }
            }
        }
        let handler = root.join("transport").join("connection_handler.rs");
        let source = std::fs::read_to_string(&handler).expect("connection_handler.rs");
        let gate = "if super::quic::admission::enabled_at(is_gateway, bind_addr) {";
        let gates: Vec<usize> = source.match_indices(gate).map(|(at, _)| at).collect();
        assert_eq!(gates.len(), 1, "one gate");
        let gate_end = gates[0]
            + source[gates[0]..]
                .find("\n        }\n")
                .expect("gate block");
        assert_eq!(
            production_calls.len(),
            2,
            "one call per clock: {production_calls:?}"
        );
        for (file, at) in production_calls {
            assert_eq!(
                file, handler,
                "enable_gateway_admission called outside the gate"
            );
            assert!(
                (gates[0]..gate_end).contains(&at),
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
        let signature = "    pub(crate) async fn establish(self) -> Result<(QuicConnection, JoinerCharge), JoinerError> {";
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
        // The other long-header types of version 1: 0-RTT, Handshake, Retry.
        for first in [0xD3, 0xE3, 0xF3] {
            assert_eq!(
                unknown_source_datagram(&initial(first, 1200, 1)),
                Err(NotAnInitial::NotInitial),
                "{first:#x}"
            );
        }
        // The type bits are not read for a version this endpoint does not
        // speak: a version negotiation packet (version 0) is unknown.
        assert_eq!(
            unknown_source_datagram(&initial(0xE3, 1200, 0)),
            Err(NotAnInitial::UnknownVersion)
        );
    }

    /// A ticket dropped without being kept gives the admission back: the
    /// address can be admitted again at once and the node's token returns.
    #[test]
    fn a_dropped_ticket_gives_the_admission_back() {
        let (_now, admission) = clock();
        let ticket = admission.decide(addr(1), true).expect("admitted");
        assert_eq!(admission.tracked(), 1);
        drop(ticket);
        assert_eq!(admission.tracked(), 0, "the record is undone");
        // All 50 tokens are there again: 50 more admissions, then the rate.
        for port in 1..=50 {
            assert_eq!(admission.decide_kept(addr(port), true), Verdict::Admit);
        }
        assert_eq!(
            admission.decide_kept(addr(99), true),
            Verdict::ThrottledGlobal
        );
    }

    /// An attempt without a token is retried, and a retry records nothing.
    #[test]
    fn an_unvalidated_attempt_is_retried_and_costs_nothing() {
        let (_now, admission) = clock();
        for _ in 0..1000 {
            assert_eq!(admission.decide_kept(addr(1), false), Verdict::Retry);
        }
        assert_eq!(admission.tracked(), 0);
        assert_eq!(admission.decide_kept(addr(1), true), Verdict::Admit);
    }

    /// One validated attempt per second per address, keyed by IP and port;
    /// a throttled attempt does not push the address's next turn back.
    #[test]
    fn one_attempt_per_second_per_address() {
        let (now, admission) = clock();
        assert_eq!(admission.decide_kept(addr(1), true), Verdict::Admit);
        now.fetch_add(SECOND - 1, Ordering::Relaxed);
        assert_eq!(
            admission.decide_kept(addr(1), true),
            Verdict::ThrottledAddress
        );
        assert_eq!(
            admission.decide_kept(addr(2), true),
            Verdict::Admit,
            "another port of the same IP is another address"
        );
        let v6_mapped =
            SocketAddr::from((std::net::Ipv4Addr::new(192, 0, 2, 1).to_ipv6_mapped(), 1));
        assert_eq!(
            admission.decide_kept(v6_mapped, true),
            Verdict::ThrottledAddress,
            "an IPv4-mapped address is the same address"
        );
        now.fetch_add(1, Ordering::Relaxed);
        assert_eq!(admission.decide_kept(addr(1), true), Verdict::Admit);
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
                    admission.decide_kept(addr(port), true) == Verdict::Admit
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
            assert_eq!(admission.decide_kept(addr(port), true), Verdict::Admit);
        }
        assert_eq!(
            admission.decide_kept(addr(100), true),
            Verdict::ThrottledGlobal
        );
        now.fetch_add(SECOND / 50, Ordering::Relaxed);
        assert_eq!(
            admission.decide_kept(addr(100), true),
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
                if admission.decide_kept(from, true) == Verdict::Admit {
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
        assert_eq!(admission.decide_kept(addr(1), true), Verdict::Admit);
        now.fetch_sub(100 * SECOND, Ordering::Relaxed);
        assert_eq!(admission.decide_kept(addr(1), true), Verdict::Admit);
    }
}
