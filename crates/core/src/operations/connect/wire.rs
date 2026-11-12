//! The wire form of [`ConnectMsg`] (#5940).
//!
//! `ConnectMsg` is encoded through [`ConnectMsgWire`]. Its first five variants
//! are today's, with today's payload types, so their bytes are unchanged; the
//! QUIC-native offer and decision are `#[serde(skip)]` fields of the payloads
//! and do not appear in them. Two appended variants, `RequestNative` (index 5)
//! and `ResponseNative` (index 6), carry the payload together with the offer or
//! the decision. Decoding maps both forms to one internal value.
//!
//! An appended variant is emitted only through [`encode_for_peer`], for a peer
//! whose negotiated version is known and at or above the floor, and only when
//! the internal field is `Some`. Every other encode of a message (event logs,
//! broadcast metadata, simulation bridges) emits the five old variants: a path
//! that misses the gate loses the offer, so the pair stays on UDP, and never
//! sends a peer an index it cannot decode.

use std::cell::Cell;
use std::fmt;
use std::net::SocketAddr;
use std::thread::LocalKey;

use serde::{Deserialize, Serialize};

use super::native::{NativeDecision, NativeOffer};
use super::{ConnectMsg, ConnectRequest, ConnectResponse};
use crate::dev_tool::Location;
use crate::message::Transaction;

/// The encoding of [`ConnectMsg`]. Variant order is wire format: never
/// reorder, only append.
#[derive(Serialize, Deserialize)]
pub(crate) enum ConnectMsgWire {
    Request {
        id: Transaction,
        payload: ConnectRequest,
    },
    Response {
        id: Transaction,
        payload: ConnectResponse,
    },
    ObservedAddress {
        id: Transaction,
        address: SocketAddr,
    },
    Rejected {
        id: Transaction,
        desired_location: Location,
    },
    ConnectFailed {
        id: Transaction,
        failed_acceptor_addr: SocketAddr,
    },
    /// Index 5: a request with the joiner's offer.
    RequestNative {
        id: Transaction,
        payload: ConnectRequest,
        offer: NativeOffer,
    },
    /// Index 6: a response with the acceptor's decision.
    ResponseNative {
        id: Transaction,
        payload: ConnectResponse,
        decision: NativeDecision,
    },
}

/// The number of variants a release without the native fields knows.
const LEGACY_VARIANTS: u32 = 5;

thread_local! {
    /// Set only by [`encode_for_peer`], around one serialize: the receiving
    /// peer decodes the appended variants.
    static EMIT_NATIVE: Cell<bool> = const { Cell::new(false) };
    /// Set only in a simulation's pre-floor decoder mode, around one decode:
    /// the appended variants fail to decode as they do on an older release.
    static REJECT_NATIVE: Cell<bool> = const { Cell::new(false) };
}

/// Sets a flag for its lifetime and restores the previous value on drop,
/// unwinding included, so a panic inside a serialize cannot leave the flag set
/// for the next message encoded on this thread.
struct FlagGuard {
    flag: &'static LocalKey<Cell<bool>>,
    previous: bool,
}

impl FlagGuard {
    fn set(flag: &'static LocalKey<Cell<bool>>, value: bool) -> Self {
        let previous = flag.with(|f| f.replace(value));
        Self { flag, previous }
    }
}

impl Drop for FlagGuard {
    fn drop(&mut self) {
        let previous = self.previous;
        self.flag.with(|f| f.set(previous));
    }
}

impl From<ConnectMsg> for ConnectMsgWire {
    fn from(msg: ConnectMsg) -> Self {
        let native = EMIT_NATIVE.with(Cell::get);
        match msg {
            ConnectMsg::Request { id, payload } => match payload.native_offer {
                Some(offer) if native => Self::RequestNative { id, payload, offer },
                _ => Self::Request { id, payload },
            },
            ConnectMsg::Response { id, payload } => match payload.native_decision {
                Some(decision) if native => Self::ResponseNative {
                    id,
                    payload,
                    decision,
                },
                _ => Self::Response { id, payload },
            },
            ConnectMsg::ObservedAddress { id, address } => Self::ObservedAddress { id, address },
            ConnectMsg::Rejected {
                id,
                desired_location,
            } => Self::Rejected {
                id,
                desired_location,
            },
            ConnectMsg::ConnectFailed {
                id,
                failed_acceptor_addr,
            } => Self::ConnectFailed {
                id,
                failed_acceptor_addr,
            },
        }
    }
}

/// An appended variant refused in pre-floor decoder mode. Its message is the
/// one an older release's decoder produces for the same bytes.
#[derive(Debug)]
pub(crate) struct NativeVariantRejected(u32);

impl fmt::Display for NativeVariantRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid value: integer `{}`, expected variant index 0 <= i < {LEGACY_VARIANTS}",
            self.0
        )
    }
}

impl TryFrom<ConnectMsgWire> for ConnectMsg {
    type Error = NativeVariantRejected;

    fn try_from(wire: ConnectMsgWire) -> Result<Self, Self::Error> {
        Ok(match wire {
            ConnectMsgWire::Request { id, payload } => Self::Request { id, payload },
            ConnectMsgWire::Response { id, payload } => Self::Response { id, payload },
            ConnectMsgWire::ObservedAddress { id, address } => {
                Self::ObservedAddress { id, address }
            }
            ConnectMsgWire::Rejected {
                id,
                desired_location,
            } => Self::Rejected {
                id,
                desired_location,
            },
            ConnectMsgWire::ConnectFailed {
                id,
                failed_acceptor_addr,
            } => Self::ConnectFailed {
                id,
                failed_acceptor_addr,
            },
            ConnectMsgWire::RequestNative {
                id,
                mut payload,
                offer,
            } => {
                if REJECT_NATIVE.with(Cell::get) {
                    return Err(NativeVariantRejected(LEGACY_VARIANTS));
                }
                payload.native_offer = Some(offer);
                Self::Request { id, payload }
            }
            ConnectMsgWire::ResponseNative {
                id,
                mut payload,
                decision,
            } => {
                if REJECT_NATIVE.with(Cell::get) {
                    return Err(NativeVariantRejected(LEGACY_VARIANTS + 1));
                }
                payload.native_decision = Some(decision);
                Self::Response { id, payload }
            }
        })
    }
}

/// Whether a peer that negotiated `remote` decodes the appended variants:
/// its version is known and at least `floor`. Fails closed on an unknown
/// version.
pub(crate) fn native_wire_allowed(remote: Option<(u8, u8, u16)>, floor: (u8, u8, u16)) -> bool {
    remote.is_some_and(|v| v >= floor)
}

/// Serialize a message bound for the peer that negotiated `remote_version`.
///
/// The one encode that may emit the appended `ConnectMsg` variants, and only
/// when [`native_wire_allowed`]. `floor` is
/// [`crate::node::QUIC_NATIVE_MIN_VERSION`] in production; a test passes a
/// lower one, since until the shipping release the real floor is above every
/// version a test can run.
pub(crate) fn encode_for_peer<T: Serialize>(
    msg: &T,
    remote_version: Option<(u8, u8, u16)>,
    floor: (u8, u8, u16),
) -> bincode::Result<Vec<u8>> {
    let _guard = FlagGuard::set(&EMIT_NATIVE, native_wire_allowed(remote_version, floor));
    bincode::serialize(msg)
}

/// Run `decode` as a release below the floor would: an appended variant fails
/// to decode, exactly as an unknown variant index does there. Used only by a
/// simulation node in pre-floor decoder mode
/// (`NodeConfig::native_wire_pre_floor`).
pub(crate) fn decode_as_pre_floor<R>(decode: impl FnOnce() -> R) -> R {
    let _guard = FlagGuard::set(&REJECT_NATIVE, true);
    decode()
}

/// The `ConnectMsg` variant index of a serialized `NetMessage`, if it is a
/// connect message: `NetMessage::V1` (index 0), `NetMessageV1::Connect`
/// (index 0), then the variant, each a little-endian `u32` in bincode's
/// default encoding.
#[cfg_attr(not(any(test, feature = "testing")), allow(dead_code))]
pub(crate) fn connect_wire_index(bytes: &[u8]) -> Option<u32> {
    let word = |at: usize| -> Option<u32> {
        let slice = bytes.get(at..at + 4)?;
        Some(u32::from_le_bytes(slice.try_into().ok()?))
    };
    (word(0)? == 0 && word(4)? == 0).then_some(())?;
    word(8)
}

/// Whether `msg` is a connect request or response carrying a native offer or
/// decision, i.e. one the V2 encoding would carry and V1 drops.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn carries_native_fields(msg: &crate::message::NetMessage) -> bool {
    use crate::message::{NetMessage, NetMessageV1};
    match msg {
        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request { payload, .. })) => {
            payload.native_offer.is_some()
        }
        NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response { payload, .. })) => {
            payload.native_decision.is_some()
        }
        _ => false,
    }
}

/// Per-network record of the QUIC-native wire fields in a simulation, so a
/// test can assert that the appended variants actually went out and what the
/// connect op did with them (`SimNetwork::native_wire_trace`).
#[cfg(any(test, feature = "testing"))]
pub mod sim_trace {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Counts for one simulated network. Cloning shares the counts.
    #[derive(Clone, Debug, Default)]
    pub struct NativeWireTrace(Arc<Counts>);

    #[derive(Debug, Default)]
    struct Counts {
        sent: [AtomicU64; 7],
        native_fields_dropped: AtomicU64,
        undecodable_closes: AtomicU64,
        accepts_with_offer: AtomicU64,
        accepts_without_offer: AtomicU64,
        retry_accepts_with_offer: AtomicU64,
        joiner_decisions: AtomicU64,
        joiner_responses_without_decision: AtomicU64,
        /// Nodes standing in for a release below the floor
        /// ([`mark_pre_floor`]).
        pre_floor: parking_lot::Mutex<std::collections::HashSet<SocketAddr>>,
    }

    /// What the connect op saw, for [`record_at`].
    #[derive(Clone, Copy, Debug)]
    pub(crate) enum NativeWireEvent {
        /// A `ConnectMsg` of this wire index was handed to the transport.
        Sent(u32),
        /// A node not in pre-floor mode sent a request or response carrying a
        /// native field in today's encoding (index 0 or 1) to a peer whose
        /// version it knows, so the field was dropped because that peer is
        /// below the floor.
        NativeFieldsDropped,
        /// A connection was closed on a message that did not decode.
        UndecodableClose,
        /// An acceptor answered a request; `retry` for a re-routed one.
        ///
        /// `joiner` is the joiner's address. An answer without an offer to a
        /// joiner marked pre-floor ([`mark_pre_floor`]) is not counted: such a
        /// joiner sends no offer by design.
        Accepted {
            with_offer: bool,
            retry: bool,
            joiner: Option<SocketAddr>,
        },
        /// A joiner received a response.
        JoinerResponse { with_decision: bool },
    }

    static TRACES: LazyLock<dashmap::DashMap<String, NativeWireTrace>> =
        LazyLock::new(dashmap::DashMap::new);

    /// The trace of `network`, created at the first call.
    pub(crate) fn trace(network: &str) -> NativeWireTrace {
        TRACES.entry(network.to_string()).or_default().clone()
    }

    /// Records `event` in the trace of the network `addr` belongs to, if a
    /// test asked for one.
    pub(crate) fn record_at(addr: SocketAddr, event: NativeWireEvent) {
        let Some(network) = crate::transport::in_memory_socket::network_of(&addr) else {
            return;
        };
        let Some(trace) = TRACES.get(&network).map(|t| t.clone()) else {
            return;
        };
        let c = &trace.0;
        let counter = match event {
            NativeWireEvent::Sent(index) => match c.sent.get(index as usize) {
                Some(counter) => counter,
                None => return,
            },
            NativeWireEvent::NativeFieldsDropped => &c.native_fields_dropped,
            NativeWireEvent::UndecodableClose => &c.undecodable_closes,
            NativeWireEvent::Accepted {
                with_offer: true,
                retry,
                ..
            } => {
                if retry {
                    c.retry_accepts_with_offer.fetch_add(1, Ordering::Relaxed);
                }
                &c.accepts_with_offer
            }
            NativeWireEvent::Accepted {
                with_offer: false,
                joiner,
                ..
            } => {
                if joiner.is_some_and(|joiner| c.pre_floor.lock().contains(&joiner)) {
                    return;
                }
                &c.accepts_without_offer
            }
            NativeWireEvent::JoinerResponse {
                with_decision: true,
            } => &c.joiner_decisions,
            NativeWireEvent::JoinerResponse {
                with_decision: false,
            } => &c.joiner_responses_without_decision,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks the node at `addr` in `network` as standing in for a release
    /// below the floor, so answers to its offerless requests are not counted
    /// as offers dropped on the way.
    pub(crate) fn mark_pre_floor(network: &str, addr: SocketAddr) {
        trace(network).0.pre_floor.lock().insert(addr);
    }

    /// Removes the trace of `network` (on `SimNetwork` drop).
    pub(crate) fn remove(network: &str) {
        TRACES.remove(network);
    }

    impl NativeWireTrace {
        fn get(counter: &AtomicU64) -> u64 {
            counter.load(Ordering::Relaxed)
        }
        /// `ConnectMsg`s handed to the transport with wire `index` (0-6).
        pub fn sent(&self, index: usize) -> u64 {
            self.0.sent.get(index).map_or(0, Self::get)
        }
        /// Requests sent as `RequestNative` (index 5).
        pub fn requests_native(&self) -> u64 {
            self.sent(5)
        }
        /// Responses sent as `ResponseNative` (index 6).
        pub fn responses_native(&self) -> u64 {
            self.sent(6)
        }
        /// Requests and responses an at-floor node sent with their native
        /// field dropped, to a peer it knew to be below the floor.
        pub fn native_fields_dropped(&self) -> u64 {
            Self::get(&self.0.native_fields_dropped)
        }
        /// Connections closed on an undecodable message.
        pub fn undecodable_closes(&self) -> u64 {
            Self::get(&self.0.undecodable_closes)
        }
        /// Accepts of a request that carried an offer.
        pub fn accepts_with_offer(&self) -> u64 {
            Self::get(&self.0.accepts_with_offer)
        }
        /// Accepts of a request that carried no offer.
        pub fn accepts_without_offer(&self) -> u64 {
            Self::get(&self.0.accepts_without_offer)
        }
        /// Accepts after a `Rejected` or `ConnectFailed` re-route, of a request
        /// that carried an offer.
        pub fn retry_accepts_with_offer(&self) -> u64 {
            Self::get(&self.0.retry_accepts_with_offer)
        }
        /// Responses a joiner received with a decision.
        pub fn joiner_decisions(&self) -> u64 {
            Self::get(&self.0.joiner_decisions)
        }
        /// Responses a joiner received without one.
        pub fn joiner_responses_without_decision(&self) -> u64 {
            Self::get(&self.0.joiner_responses_without_decision)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{NetMessage, NetMessageV1};
    use crate::operations::VisitedPeers;
    use crate::ring::PeerKeyLocation;
    use crate::transport::TransportKeypair;

    /// Today's `ConnectMsg` and payloads, frozen: the shapes and derives of
    /// origin/main before the native fields. The reference for the golden
    /// bytes and for what an older release decodes.
    mod frozen {
        use super::*;

        #[derive(Serialize, Deserialize, Debug)]
        pub(super) struct ConnectRequest {
            pub desired_location: Location,
            pub joiner: PeerKeyLocation,
            pub ttl: u8,
            pub visited: VisitedPeers,
            #[serde(default = "default_uphill")]
            pub uphill_budget: u8,
        }

        fn default_uphill() -> u8 {
            8
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub(super) struct ConnectResponse {
            pub acceptor: PeerKeyLocation,
        }

        #[derive(Serialize, Deserialize, Debug)]
        pub(super) enum ConnectMsg {
            Request {
                id: Transaction,
                payload: ConnectRequest,
            },
            Response {
                id: Transaction,
                payload: ConnectResponse,
            },
            ObservedAddress {
                id: Transaction,
                address: SocketAddr,
            },
            Rejected {
                id: Transaction,
                desired_location: Location,
            },
            ConnectFailed {
                id: Transaction,
                failed_acceptor_addr: SocketAddr,
            },
        }
    }

    struct Fixture {
        id: Transaction,
        joiner: PeerKeyLocation,
        acceptor: PeerKeyLocation,
        visited: VisitedPeers,
        addr: SocketAddr,
        location: Location,
    }

    fn fixture() -> Fixture {
        let id = Transaction::new::<ConnectMsg>();
        let mut visited = VisitedPeers::new(&id);
        visited.mark_visited("10.0.0.1:1000".parse().unwrap());
        Fixture {
            id,
            joiner: PeerKeyLocation::new(
                TransportKeypair::new().public().clone(),
                "10.0.0.2:2000".parse().unwrap(),
            ),
            acceptor: PeerKeyLocation::with_unknown_addr(TransportKeypair::new().public().clone()),
            visited,
            addr: "192.0.2.7:3000".parse().unwrap(),
            location: Location::new(0.25),
        }
    }

    fn request(f: &Fixture, offer: Option<NativeOffer>) -> ConnectMsg {
        ConnectMsg::Request {
            id: f.id,
            payload: ConnectRequest {
                desired_location: f.location,
                joiner: f.joiner.clone(),
                ttl: 7,
                visited: f.visited.clone(),
                uphill_budget: 3,
                native_offer: offer,
            },
        }
    }

    fn response(f: &Fixture, decision: Option<NativeDecision>) -> ConnectMsg {
        ConnectMsg::Response {
            id: f.id,
            payload: ConnectResponse {
                acceptor: f.acceptor.clone(),
                native_decision: decision,
            },
        }
    }

    /// Each of today's variants, in the new type with every native field
    /// set, paired with the same message in the frozen type.
    fn pairs(f: &Fixture) -> Vec<(ConnectMsg, frozen::ConnectMsg)> {
        vec![
            (
                request(f, Some(NativeOffer::capable(40))),
                frozen::ConnectMsg::Request {
                    id: f.id,
                    payload: frozen::ConnectRequest {
                        desired_location: f.location,
                        joiner: f.joiner.clone(),
                        ttl: 7,
                        visited: f.visited.clone(),
                        uphill_budget: 3,
                    },
                },
            ),
            (
                response(f, Some(NativeDecision::Native)),
                frozen::ConnectMsg::Response {
                    id: f.id,
                    payload: frozen::ConnectResponse {
                        acceptor: f.acceptor.clone(),
                    },
                },
            ),
            (
                ConnectMsg::ObservedAddress {
                    id: f.id,
                    address: f.addr,
                },
                frozen::ConnectMsg::ObservedAddress {
                    id: f.id,
                    address: f.addr,
                },
            ),
            (
                ConnectMsg::Rejected {
                    id: f.id,
                    desired_location: f.location,
                },
                frozen::ConnectMsg::Rejected {
                    id: f.id,
                    desired_location: f.location,
                },
            ),
            (
                ConnectMsg::ConnectFailed {
                    id: f.id,
                    failed_acceptor_addr: f.addr,
                },
                frozen::ConnectMsg::ConnectFailed {
                    id: f.id,
                    failed_acceptor_addr: f.addr,
                },
            ),
        ]
    }

    fn net(msg: ConnectMsg) -> NetMessage {
        NetMessage::V1(NetMessageV1::Connect(msg))
    }

    fn index_of(msg: &ConnectMsg) -> u32 {
        connect_wire_index(&bincode::serialize(&net(msg.clone())).unwrap()).unwrap()
    }

    const BELOW: Option<(u8, u8, u16)> = Some((0, 2, 9));
    const FLOOR: (u8, u8, u16) = (0, 2, 10);

    /// Today's five variants encode byte for byte as before, with every
    /// native field set, through the default encode AND through
    /// `encode_for_peer` to a peer below the floor; and today's decoder reads
    /// them.
    #[test]
    fn today_variants_encode_byte_identically() {
        let f = fixture();
        for (index, (new, old)) in pairs(&f).into_iter().enumerate() {
            let expected = bincode::serialize(&old).unwrap();
            assert_eq!(
                bincode::serialize(&new).unwrap(),
                expected,
                "variant {index}"
            );
            assert_eq!(
                encode_for_peer(&new, BELOW, FLOOR).unwrap(),
                expected,
                "variant {index} below the floor"
            );
            assert_eq!(
                encode_for_peer(&new, None, FLOOR).unwrap(),
                expected,
                "variant {index} to an unknown version"
            );
            assert_eq!(&expected[..4], &(index as u32).to_le_bytes());
            let decoded: frozen::ConnectMsg = bincode::deserialize(&expected).unwrap();
            assert_eq!(
                bincode::serialize(&decoded).unwrap(),
                expected,
                "variant {index} decodes on a release without the fields"
            );
        }
        // A decision value this release does not know is dropped from V1 like
        // any other: the response encodes as today's.
        let unknown = response(&f, Some(NativeDecision::Unknown(9)));
        let today = bincode::serialize(&frozen::ConnectMsg::Response {
            id: f.id,
            payload: frozen::ConnectResponse {
                acceptor: f.acceptor.clone(),
            },
        })
        .unwrap();
        assert_eq!(bincode::serialize(&unknown).unwrap(), today);
        assert_eq!(encode_for_peer(&unknown, BELOW, FLOOR).unwrap(), today);
    }

    /// A V1 message decodes to the same internal value with the native fields
    /// `None`; bytes from an older release decode too.
    #[test]
    fn v1_round_trips_with_fields_none() {
        let f = fixture();
        for (new, old) in pairs(&f) {
            for bytes in [
                bincode::serialize(&new).unwrap(),
                bincode::serialize(&old).unwrap(),
            ] {
                let back: ConnectMsg = bincode::deserialize(&bytes).unwrap();
                match (&back, &new) {
                    (ConnectMsg::Request { payload: b, .. }, ConnectMsg::Request { .. }) => {
                        assert_eq!(b.native_offer, None);
                    }
                    (ConnectMsg::Response { payload: b, .. }, ConnectMsg::Response { .. }) => {
                        assert_eq!(b.native_decision, None);
                    }
                    _ => {}
                }
                assert_eq!(bincode::serialize(&back).unwrap(), bytes);
            }
        }
    }

    /// V2 needs both the flag and the field: no flag, or a `None` field,
    /// emits V1; both emit index 5 or 6, which decodes to the same internal
    /// value with the field set.
    #[test]
    fn v2_needs_the_flag_and_the_field() {
        let f = fixture();
        let offer = Some(NativeOffer::capable(40));
        let decision = Some(NativeDecision::Udp);
        let at = Some(FLOOR);

        assert_eq!(index_of(&request(&f, offer)), 0, "field set, no flag");
        assert_eq!(index_of(&response(&f, decision)), 1, "field set, no flag");
        for (msg, v1) in [(request(&f, None), 0), (response(&f, None), 1)] {
            let bytes = encode_for_peer(&net(msg), at, FLOOR).unwrap();
            assert_eq!(connect_wire_index(&bytes), Some(v1), "flag set, field None");
        }

        let bytes = encode_for_peer(&net(request(&f, offer)), at, FLOOR).unwrap();
        assert_eq!(connect_wire_index(&bytes), Some(5));
        let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Request { payload, .. })) =
            bincode::deserialize(&bytes).unwrap()
        else {
            panic!("a request")
        };
        assert_eq!(payload.native_offer, offer);
        assert_eq!(payload.uphill_budget, 3);
        assert_eq!(payload.joiner, f.joiner);

        let bytes = encode_for_peer(&net(response(&f, decision)), at, FLOOR).unwrap();
        assert_eq!(connect_wire_index(&bytes), Some(6));
        let NetMessage::V1(NetMessageV1::Connect(ConnectMsg::Response { payload, .. })) =
            bincode::deserialize(&bytes).unwrap()
        else {
            panic!("a response")
        };
        assert_eq!(payload.native_decision, decision);
        assert_eq!(payload.acceptor, f.acceptor);
    }

    /// `encode_for_peer` emits V2 at and above the floor and V1 below it, and
    /// leaves the flag clear afterwards.
    #[test]
    fn encode_for_peer_gates_on_the_floor() {
        let f = fixture();
        let msg = net(request(&f, Some(NativeOffer::capable(0))));
        let index = |remote| connect_wire_index(&encode_for_peer(&msg, remote, FLOOR).unwrap());
        assert_eq!(index(None), Some(0));
        assert_eq!(index(Some((0, 2, 9))), Some(0));
        assert_eq!(index(Some((0, 1, u16::MAX))), Some(0));
        assert_eq!(index(Some(FLOOR)), Some(5));
        assert_eq!(index(Some((0, 2, 11))), Some(5));
        assert_eq!(index(Some((0, 3, 0))), Some(5));
        assert!(!EMIT_NATIVE.with(Cell::get));
        assert_eq!(
            bincode::serialize(&msg)
                .map(|b| connect_wire_index(&b))
                .unwrap(),
            Some(0)
        );
    }

    /// A value whose serialize panics, to unwind out of `encode_for_peer`.
    struct Panics;

    impl Serialize for Panics {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            assert!(
                EMIT_NATIVE.with(Cell::get),
                "the flag is set during the encode"
            );
            panic!("serialize failed");
        }
    }

    #[test]
    fn the_flag_is_cleared_when_serialize_panics() {
        let caught = std::panic::catch_unwind(|| encode_for_peer(&Panics, Some(FLOOR), FLOOR));
        assert!(caught.is_err());
        assert!(
            !EMIT_NATIVE.with(Cell::get),
            "the flag leaked past the panic"
        );
        let f = fixture();
        assert_eq!(
            index_of(&request(&f, Some(NativeOffer::capable(0)))),
            0,
            "a later default encode emits V1"
        );
    }

    /// In pre-floor decoder mode the appended variants fail exactly as they
    /// do on a release without them, and the old variants still decode.
    #[test]
    fn pre_floor_mode_rejects_like_an_older_release() {
        let f = fixture();
        for msg in [
            request(&f, Some(NativeOffer::capable(0))),
            response(&f, Some(NativeDecision::Udp)),
        ] {
            let bytes = encode_for_peer(&msg, Some(FLOOR), FLOOR).unwrap();
            let old = bincode::deserialize::<frozen::ConnectMsg>(&bytes)
                .expect_err("an older release cannot decode it");
            let mode = decode_as_pre_floor(|| bincode::deserialize::<ConnectMsg>(&bytes))
                .expect_err("pre-floor mode refuses it");
            assert_eq!(mode.to_string(), old.to_string());
            assert!(!REJECT_NATIVE.with(Cell::get));
            assert!(bincode::deserialize::<ConnectMsg>(&bytes).is_ok());
        }
        for (new, _) in pairs(&f) {
            let bytes = bincode::serialize(&new).unwrap();
            assert!(decode_as_pre_floor(|| bincode::deserialize::<ConnectMsg>(&bytes)).is_ok());
        }
    }

    /// A node in the test-only pre-floor mode encodes as an older release
    /// does: today's bytes to every peer, at or above the floor too.
    #[test]
    fn pre_floor_mode_never_encodes_the_native_variants() {
        let f = fixture();
        let old = super::super::native::NativeWire {
            floor: FLOOR,
            pre_floor: true,
        };
        let new = super::super::native::NativeWire {
            floor: FLOOR,
            pre_floor: false,
        };
        let msg = net(request(&f, Some(NativeOffer::capable(40))));
        for remote in [None, Some(FLOOR), Some((0, 3, 0))] {
            let bytes = old.encode(&msg, remote).unwrap();
            assert_eq!(connect_wire_index(&bytes), Some(0), "{remote:?}");
            assert_eq!(bytes, bincode::serialize(&msg).unwrap());
        }
        assert_eq!(
            connect_wire_index(&new.encode(&msg, Some(FLOOR)).unwrap()),
            Some(5),
            "the same message goes out as V2 from an at-floor node"
        );
    }

    /// A relay on this release passes on an offer level and a decision value
    /// a later release sends, byte for byte: what it decodes and encodes again
    /// for the next at-floor hop is what it received.
    #[test]
    fn a_relay_passes_on_values_a_later_release_sends() {
        let f = fixture();
        let offer = NativeOffer {
            level: super::super::native::LEVEL_NATIVE_ONLY,
            percent: 250,
        };
        let decision = NativeDecision::Unknown(7);
        for msg in [request(&f, Some(offer)), response(&f, Some(decision))] {
            let received = encode_for_peer(&net(msg), Some(FLOOR), FLOOR).unwrap();
            let decoded: NetMessage = bincode::deserialize(&received).unwrap();
            let forwarded = encode_for_peer(&decoded, Some(FLOOR), FLOOR).unwrap();
            assert_eq!(forwarded, received);
        }
    }

    #[test]
    fn connect_wire_index_reads_only_connect_messages() {
        assert_eq!(connect_wire_index(&[0; 8]), None);
        assert_eq!(
            connect_wire_index(&[0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0]),
            None
        );
        assert_eq!(
            connect_wire_index(&[0, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0]),
            Some(6)
        );
    }
}
