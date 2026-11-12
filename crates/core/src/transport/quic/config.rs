//! Every quinn and rustls setting of the QUIC endpoint, pinned explicitly.
//!
//! quinn's defaults are tuned for a general internet server, and several of
//! them are wrong here: QUIC bit greasing would break the first-byte demux,
//! path MTU discovery and GSO would send datagrams larger than the UDP
//! transport's, migration would change a connection's address (which the node
//! uses as the peer's identity), and the flow-control windows allow far more
//! memory per connection than a node can give. So nothing is left to a
//! default: each value is set here.
//!
//! What `quic::tests` pins, and how: values quinn's `Debug` shows are pinned
//! field by field (endpoint, transport, server and client configurations);
//! the rustls settings are read off the rustls configs; Cubic is checked on
//! a live connection (`congestion_state`); the connection-ID generator is
//! checked by sending an endpoint a short-header packet with a random ID and
//! seeing no reply. NOT pinned, because quinn exposes them neither in `Debug`
//! nor in behaviour a test can observe cheaply: the token store
//! (`NoneTokenStore`), the initial destination CID provider, the
//! stateless-reset key, `min_reset_interval` and the server's system clock.
//!
//! Memory: [`QUIC_CONNECTION_FIXED_BYTES`], [`QUIC_ACK_TAIL_BYTES`],
//! [`QUIC_OPEN_STREAM_BYTES`] and [`QUIC_STOPPED_ENTRY_BYTES`] are
//! measurements of named terms under stated conditions (`quic::memory_tests`),
//! not a ceiling on what one connection can hold; the fixed charge per
//! connection ([`QUIC_CONNECTION_CHARGE_BYTES`]) sums the ones that do not
//! scale with the windows. Not measured:
//! - datagram sizes other than 1200 B (every stream shape measured pads its
//!   datagrams to the 1200-byte MTU);
//! - quinn's queue of datagrams from the endpoint driver to each connection;
//! - send-side packet records and the buffers behind sent data;
//! - allocators other than jemalloc on linux-gnu (macOS and Windows builds
//!   round differently);
//! - CRYPTO-frame reassembly, quinn-proto's second assembler (its data limit
//!   is [`QUIC_CRYPTO_BUFFER_BYTES`]; charged at the assembler's derived
//!   worst case, [`QUIC_CRYPTO_HOLD_BYTES`]);
//! - arrival orders other than a single gap at offset 0 of each stream.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use quinn::{
    ClientConfig, EndpointConfig, IdleTimeout, MtuDiscoveryConfig, NoneTokenLog, NoneTokenStore,
    ServerConfig, TransportConfig, ValidationTokenConfig, VarInt,
};
use ring_crypto::{hkdf, hmac};

use super::cid::MacConnectionIdGenerator;
use crate::config::GlobalRng;
use crate::simulation::TimeSource;
use crate::transport::global_bandwidth::GlobalBandwidthManager;

/// The largest UDP payload the endpoint sends or advertises it accepts: the
/// UDP transport's packet size and QUIC's minimum (RFC 9000 section 14), so
/// a QUIC datagram is never larger than a legacy one.
pub(crate) const MAX_UDP_PAYLOAD: u16 = 1200;

/// The only QUIC version spoken: version 1 (RFC 9000).
pub(crate) const QUIC_VERSION: u32 = 1;

/// The ALPN protocol both sides must offer. A peer offering anything else
/// fails the handshake.
pub(crate) const ALPN: &[u8] = b"freenet-quic/1";

/// The server name quinn requires for a connection attempt. With SNI off it
/// is never sent; peers are identified by their raw public keys.
pub(crate) const SERVER_NAME: &str = "freenet";

/// Keepalive interval: the UDP transport's NAT keepalive guarantee
/// (`peer_connection::MAX_KEEPALIVE_INTERVAL`, 15 s), so a QUIC connection
/// holds a NAT mapping open as long as a legacy one does.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Concurrent unidirectional streams a peer may open. The transport sends
/// one message per stream, so this bounds the messages in flight from one
/// peer. When all are in use, the sender waits in `open_uni` until the
/// receiver returns a credit: the cost of a limit that is too low is
/// sender back-pressure. quinn returns credit in batches (only once more
/// than an eighth of the limit has been freed), so a sender can wait with
/// as few as seven eighths of the limit open at the receiver.
///
/// Short messages and control frames take slots from the same pool as
/// streamed messages, and a streamed message holds its slot for its whole
/// transfer. A path caps its outbound streamed messages at
/// [`STREAMED_SEND_LIMIT`] to keep [`RESERVED_UNI_STREAMS`] slots for short
/// messages and control frames on a steady path (see there).
///
/// Set from the `inbound_concurrency` gauge (#5993), which estimates on
/// the UDP transport how many inbound streams each connection would hold
/// open on QUIC. Release 0.2.150 at steady state, 2026-10-26
/// 05:00-17:25Z, 1,432 peers and 30.6M connection-windows; the share of
/// windows whose peak reached each level:
///
/// | streams | share of windows |
/// |---------|------------------|
/// | >= 32   | 0.66%            |
/// | >= 64   | 0.133%           |
/// | >= 128  | 0.019%           |
/// | >= 256  | 0.0036%          |
///
/// With the credit batching above, a connection starts to wait somewhere
/// between the 32 and 64 rows, so on this proxy in 0.13% to 0.66% of
/// windows; these are shares of windows reaching a level on the UDP
/// transport, not measured stalls. Per peer the share is uneven: the
/// median peer reached 64 in 0.04% of its windows, the top 1% of peers in
/// 2.6% or more.
///
/// The estimate is built to err high (see
/// `peer_connection::inbound_concurrency`), so it can justify lowering
/// this value, never raising it. Shadow mode carries no data, so it
/// measures nothing about this value. The first measurement on real QUIC is
/// step 3 of the rollout (#5940: data on a slice of pairs, chosen by a hash
/// of the two keys).
pub(crate) const MAX_UNI_STREAMS: u32 = 64;

/// The uni streams every connection's peer may open until the path takes
/// data: enough for the upgrade's control frames (`Ready`, `ReadyAck`). A
/// path that accepts data raises it to [`DATA_START_UNI_STREAMS`] when it is
/// built (`QuicPath::with_budget`), and with load toward
/// [`MAX_UNI_STREAMS`]. quinn applies a raise at once and cannot take a
/// grant back, so a connection starts with what a shadow path needs.
pub(crate) const INITIAL_UNI_STREAMS: u32 = 4;

/// The uni streams a data path's peer may open at first. The limit doubles
/// toward [`MAX_UNI_STREAMS`] once the peer has used every stream at once and
/// as many again have completed with none failing and the buffer budget not
/// throttling the connection, among other conditions
/// (`path::inbound::StreamLimit`): the stream limit is the one term per
/// connection a raise cannot take back, so it is raised only under load.
pub(crate) const DATA_START_UNI_STREAMS: u32 = 16;

/// The connection receive window every connection starts with, until a data
/// path's buffer-budget meter raises it to the floor (`budget`). Small for the
/// reason [`INITIAL_UNI_STREAMS`] is: quinn cannot take credit back, and a
/// shadow path, which carries only control frames, needs little. Its receive
/// side is in the fixed charge at this window
/// ([`QUIC_CONNECTION_CHARGE_BYTES`]).
pub(crate) const INITIAL_RECEIVE_WINDOW_BYTES: u64 = 16 * 1024;

/// Uni stream slots left for short messages and control frames when every
/// streamed-message permit is held and the unannounced credit is at
/// [`UNANNOUNCED_CREDIT_SLACK`]. What that holds under is on
/// [`STREAMED_SEND_LIMIT`].
pub(crate) const RESERVED_UNI_STREAMS: u32 = 8;

/// Slots the receiver has freed but has not yet written a MAX_STREAMS frame
/// for: quinn writes one only once more than `MAX_UNI_STREAMS / 8` streams
/// have been freed since the last (quinn-proto 0.11.19 `queue_max_stream_id`),
/// and the sender cannot use a slot until the frame reaches it.
pub(crate) const UNANNOUNCED_CREDIT_SLACK: u32 = MAX_UNI_STREAMS / 8;

/// Streamed messages (`QuicPath::send_stream_bounded`,
/// `send_stream_chunks_bounded`, and a short message that does not fit a
/// packet) a path sends at once. The limit is divided as
/// `STREAMED_SEND_LIMIT + UNANNOUNCED_CREDIT_SLACK + RESERVED_UNI_STREAMS =
/// MAX_UNI_STREAMS` (48 + 8 + 8 = 64). A streamed send past it waits for one
/// of the path's permits before it opens its stream. Short messages of one
/// packet and control frames take no permit.
///
/// The reserve holds on a steady path: every streamed send still holds its
/// permit or its stream has been freed at the receiver, no MAX_STREAMS frame
/// is lost, queued or in flight, and short messages and control frames hold
/// no more than the reserve. A short message or control frame then finds a stream slot
/// whatever the streamed messages are doing (pinned by
/// `a_short_still_sends_while_streamed_messages_are_blocked`).
///
/// Constraints for carrying data on a path, none of which arises on a shadow
/// path because it sends only control frames:
/// - A permit can return before the slot does. A stream holds its permit
///   until it is acknowledged, also past a deadline after its end was sent,
///   so that case holds. A reset or a stop from the peer returns the permit
///   about one round trip before the receiver has freed the stream. An
///   acknowledgement is about receipt, so it returns the permit while the
///   receiver may not have read the stream to its end. That is short while the
///   consumer keeps up. With a full channel the readers wait for room, up to
///   the age limit, each holding a slot, so a slow consumer can use up the
///   reserve: one-packet messages then fail to open within their deadline and
///   go another way. Measured by `quic_open_uni_waited`.
/// - Credit the sender does not know about can exceed
///   [`UNANNOUNCED_CREDIT_SLACK`] while a MAX_STREAMS frame is lost, queued
///   or in flight. quinn counts credit as announced when it writes the frame
///   (`sent_max_remote`), so 9 freed slots on their way plus 8 freed since
///   are 17 the sender cannot use, with nothing lost.
/// - One-packet short messages are uncapped, so many at once
///   they can use up the reserve.
/// - A slot is not send capacity: a control frame that has a slot still
///   waits on the connection's shared flow control and send window
///   (quinn-proto `write_limit`).
/// - The division uses this node's [`MAX_UNI_STREAMS`], not the limit the
///   peer advertised (quinn does not expose it). A data path's peer starts
///   at [`DATA_START_UNI_STREAMS`] and raises only once all its streams are
///   busy, so capping the permits below its limit would stop the raise.
///   Until it raises, the reserve does not hold: a send past its limit waits
///   in `open_uni` until its open deadline and ends `NotOpened` (on UDP),
///   never `MaybeDelivered`, counted `quic_data_event_stream_limit_full`.
pub(crate) const STREAMED_SEND_LIMIT: u32 =
    MAX_UNI_STREAMS - RESERVED_UNI_STREAMS - UNANNOUNCED_CREDIT_SLACK;
const _: () = assert!(RESERVED_UNI_STREAMS > 0 && STREAMED_SEND_LIMIT > 0);

/// Concurrent bidirectional streams a peer may open: none. Messages and
/// control frames ([`super::control`]) go on unidirectional streams, and
/// quinn buffers what arrives on a stream whether or not anything accepts
/// it, so a stream nothing reads would hold unread data for the life of the
/// connection.
pub(crate) const MAX_BIDI_STREAMS: u32 = 0;

/// Connection attempts quinn holds before they are accepted or refused.
pub(crate) const MAX_INCOMING: usize = 64;

/// Bytes quinn buffers for one connection attempt before it is accepted.
pub(crate) const INCOMING_BUFFER_BYTES: u64 = 64 * 1024;

/// Bytes quinn buffers for all connection attempts together.
pub(crate) const INCOMING_BUFFER_TOTAL_BYTES: u64 = 1024 * 1024;

// Measured terms of the memory one QUIC connection holds, per side, from
// `quic::memory_tests` with quinn 0.11.12 / quinn-proto 0.11.19 / rustls
// 0.23.45, in bytes rounded up to jemalloc's size classes as the node's
// linux-gnu build allocates them (`util::alloc_count`). Each constant is a
// measurement of one named term under the conditions its doc states, plus
// a stated margin, and its test fails if a re-measurement exceeds it. They
// are NOT a ceiling on what a connection can hold: the conditions listed
// in this module's doc were not measured, and some quinn terms depend on
// traffic rate and time rather than on configuration. The per-path ceiling
// (memory gate M2, #5940) must account for those separately; none of these
// is in `declared_cache_ceiling` yet (memory gate M4, #5940).
//
// A bump of quinn, quinn-proto or rustls fails `QUIC_MEMORY_MEASURED_WITH`'s
// test until someone re-measures.

/// The heap one idle QUIC connection holds on one side, with the
/// `QuicPath` a node keeps on it: quinn's connection and driver task,
/// rustls's state and keys, the packet spaces, congestion and connection-ID
/// state, the connection watcher and the path's receive task and channels,
/// once the handshake's traffic has settled. Not included: what traffic
/// adds (stream data, the assembler, sent data, packet records);
/// per-peer allow-set and demux entries; keep-alive PINGs and their records
/// (keep-alive is off under `VirtualTime`); and socket buffers, which are
/// per endpoint.
///
/// Measured: two endpoints on `SimulationSocket` and one `VirtualTime`, on
/// one current-thread runtime, counting live bytes on that thread. After a
/// warm-up connection and a settle that may move the count by at most
/// 1 KiB (it moved 0 B in every run), N connections are opened and
/// settled, the network is cut, and each side is closed and drained in
/// turn; what each side frees is its cost. What is still allocated after
/// both sides closed ("retained") is charged to the larger side. It is
/// capacity kept once N connections have existed: opening and closing N
/// more, again and again, stopped adding to it after a few cycles, so it is
/// not per-connection state that is never freed. About 256 B per connection
/// of it is the simulated clock keeping each dropped timer's waker until
/// the timer's deadline (24 h under `VirtualTime`); the rest was not
/// attributed to a structure. Per connection, rounded (requested in
/// brackets):
///
/// | N  | client            | server            | retained        | per connection |
/// |----|-------------------|-------------------|-----------------|----------------|
/// | 1  | 52,376 (48,265) B | 52,248 (48,136) B | 480 (457) B     | 52,856 B       |
/// | 10 | 52,184 (48,093) B | 51,960 (47,879) B | 2,586 (2,336) B | 54,770 B       |
/// | 16 | 52,292 (48,189) B | 52,018 (47,928) B | 2,444 (2,209) B | 54,736 B       |
/// | 50 | 52,103 (48,018) B | 52,157 (48,052) B | 2,316 (2,069) B | 54,473 B       |
///
/// The test runs N = 16. Set to 64 KiB, 19.6% over the largest
/// measurement, 54,770 B.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_CONNECTION_FIXED_BYTES: usize = 64 * 1024;

/// The acknowledgement-only packet records quinn-proto tracked after the
/// last ack-eliciting send, up to its cap of 1000, with none of them
/// acknowledged.
///
/// quinn-proto keeps a record of every packet it sends until the peer
/// acknowledges it or it is declared lost, and it sent an ACK for about
/// every second packet it received. It keeps at most 1000
/// acknowledgement-only records after its last ack-eliciting packet
/// (`connection/spaces.rs`, `MAX_UNACKED_NON_ACK_ELICTING_TAIL`). Measured:
/// 212,480 B after 2003 such packets, 209,920 B after 4003. Set to
/// 212,480 B plus 15%, 240 KiB. The per-path ceiling (memory gate M2,
/// #5940) must re-check this term against quinn-proto's behaviour.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_ACK_TAIL_BYTES: usize = 240 * 1024;

/// One open uni stream with one byte on it, both sides together: quinn's
/// per-stream state on the sender and the receiver, and the receiver's reader
/// (`quic::memory_tests::an_open_stream_measures_within_its_pin`, at
/// `STREAMED_SEND_LIMIT` (48) streams on one connection, jemalloc-rounded;
/// the fixed charge applies it to all `MAX_UNI_STREAMS` (64): measuring 64
/// would wait for the warm-up stream's slot, which quinn returns only once
/// more than an eighth of the limit is freed).
/// Measured 988 B with quinn 0.11.12 / quinn-proto 0.11.19; the pin leaves
/// about 15%. The measurement includes the simulation's timer queue entry for
/// each stream's reader, which a production timer does not leave, so it
/// overstates a little.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_OPEN_STREAM_BYTES: usize = 1152;

/// What polling `stopped()` on a stream this side then resets leaves on the
/// connection until it closes: quinn's `stopped` map entry, an `Arc<Notify>`
/// and its share of the map's table (quinn 0.11.12 `send_stream.rs`; a local
/// reset produces neither event that removes it). Measured 88 B as the
/// difference between 1,024 resets with the poll and without
/// (`quic::memory_tests::polling_stopped_on_a_reset_stream_costs_at_most_its_entry`,
/// jemalloc-rounded); the pin leaves about 18%. Bounded per connection by
/// `recycle::SEND_ABORT_LIMIT` resets.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_STOPPED_ENTRY_BYTES: usize = 104;

/// quinn-proto's CRYPTO-frame data limit per packet space
/// (`crypto_buffer_size`): the span of offsets it accepts past what was read,
/// a data limit, not a memory bound (see [`QUIC_CRYPTO_HOLD_BYTES`]).
pub(crate) const QUIC_CRYPTO_BUFFER_BYTES: usize = 16 * 1024;

/// The memory quinn-proto's CRYPTO assembler can hold in one packet space,
/// derived, not measured: [`QUIC_CRYPTO_BUFFER_BYTES`] of data, plus the
/// over-allocation its assembler keeps before it defragments
/// (quinn-proto 0.11.19 `connection/assembler.rs`, `Assembler::insert`:
/// `max(32768, 1.5 x buffered)`, which is 32 KiB while buffered is at most
/// 16 KiB). After the handshake only the 1-RTT space is live, so the charge
/// takes one space; during the handshake up to three hold this each.
pub(crate) const QUIC_CRYPTO_HOLD_BYTES: usize = QUIC_CRYPTO_BUFFER_BYTES + 32 * 1024;

/// The fixed charge per QUIC connection, shadow paths included: the terms of
/// one connection's memory that are fixed per connection rather than scaled
/// with its windows, so the node-wide buffer budget
/// ([`quic_buffer_budget_for`]) does not meter them. Terms that depend on
/// traffic rate and time (the module doc's unmeasured list) are not in it.
///
/// | Term | Bytes |
/// |---|---|
/// | idle connection state ([`QUIC_CONNECTION_FIXED_BYTES`]) | 65,536 |
/// | [`MAX_UNI_STREAMS`] open streams x [`QUIC_OPEN_STREAM_BYTES`] | 73,728 |
/// | CRYPTO reassembly, one space, derived ([`QUIC_CRYPTO_HOLD_BYTES`]) | 49,152 |
/// | quinn's `stopped` map, `recycle::SEND_ABORT_LIMIT` x [`QUIC_STOPPED_ENTRY_BYTES`] | 425,984 |
/// | ACK-only tail ([`QUIC_ACK_TAIL_BYTES`]) | 245,760 |
/// | receive side at the initial window: unread credit and the staging cap (`budget::receive_charge` of [`INITIAL_RECEIVE_WINDOW_BYTES`]) | 49,152 |
/// | **total** | **909,312 (888 KiB)** |
///
/// A data path's receive side at its own window is in its buffer-budget
/// charge (`budget`); this term covers every connection, a shadow path
/// included, at the window it starts with.
///
/// The `stopped` map is charged at the limit's worst case. quinn adds an entry
/// when a stream's `stopped()` is polled and removes it only when the peer
/// stops the stream or the stream is finished and acknowledged, so every
/// stream this side resets after a poll keeps one until the connection
/// closes. Polling `stopped()` later would not avoid it: a writer blocked on
/// connection credit must still see the peer's stop, so the poll stays on
/// every wait. The reset rate and how close paths come to the limit are
/// reported (`quic_send_resets`, `quic_path_ms`,
/// `quic_send_resets_reached_<n>`) so field data can justify a lower charge.
///
/// Not yet summed in the node aggregate: the aggregate-accounting step of the
/// buffer budget (#5940) sums `quic_connections x` this, plus each data path's
/// floor grant (`budget::floor_grant`), and waits for the small-host decision.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_CONNECTION_CHARGE_BYTES: usize = QUIC_CONNECTION_FIXED_BYTES
    + MAX_UNI_STREAMS as usize * QUIC_OPEN_STREAM_BYTES
    + QUIC_CRYPTO_HOLD_BYTES
    + super::recycle::SEND_ABORT_LIMIT as usize * QUIC_STOPPED_ENTRY_BYTES
    + QUIC_ACK_TAIL_BYTES
    + super::budget::receive_charge(INITIAL_RECEIVE_WINDOW_BYTES) as usize;

/// The share of the node's memory for QUIC buffers that scale with the windows,
/// as a divisor: one sixteenth.
const QUIC_BUFFER_BUDGET_DIVISOR: usize = 16;

/// The node-wide budget for the growth of QUIC data paths' windows above
/// their floor windows (received data, send windows, gaps), node memory / 16:
/// 64 MiB at 1 GiB, 128 MiB at 2 GiB, 475 MiB at the 7600 MiB gateway. A data
/// path at the floor windows is charged its floor grant (`budget::floor_grant`),
/// which is per path and NOT in this budget; what does not scale with the
/// windows is charged per connection ([`QUIC_CONNECTION_CHARGE_BYTES`]). So the
/// node's QUIC memory is at most data paths x (that charge + floor grant) +
/// this budget, and the aggregate-accounting step (#5940) sums all three
/// terms. Each data path's windows are set from it (`budget::QuicBudget`).
pub(crate) fn quic_buffer_budget_for(memory_limit: usize) -> usize {
    memory_limit / QUIC_BUFFER_BUDGET_DIVISOR
}

/// The quinn, quinn-proto and rustls versions the figures above were
/// measured with, checked against `Cargo.lock` by `quic::memory_tests`.
#[cfg(test)]
pub(crate) const QUIC_MEMORY_MEASURED_WITH: [(&str, &str); 3] = [
    ("quinn", "0.11.12"),
    ("quinn-proto", "0.11.19"),
    ("rustls", "0.23.45"),
];

/// The rate a QUIC connection's window is capped at
/// (`recycle::CappedCubic`): the fair share of `total-bandwidth-limit` when
/// that is set, otherwise `bandwidth-limit`, otherwise none. The same rule as
/// the UDP transport's token bucket (`peer_connection::paced_rate`), which it
/// calls, so the two cannot drift. `bandwidth_limit` is the value after
/// `peer_connection::effective_bandwidth_limit`, as on the UDP side.
#[derive(Debug, Clone, Default)]
pub(crate) struct QuicRateLimit {
    bandwidth_limit: Option<usize>,
    global: Option<Arc<GlobalBandwidthManager>>,
    /// Tests only: build plain Cubic without the loss gate
    /// (`recycle::loss_gate`), for tests of what the path does when Cubic's
    /// window collapses under loss.
    #[cfg(test)]
    pub(crate) without_loss_gate: bool,
}

impl QuicRateLimit {
    pub(crate) fn new(
        bandwidth_limit: Option<usize>,
        global: Option<Arc<GlobalBandwidthManager>>,
    ) -> Self {
        Self {
            bandwidth_limit,
            global,
            #[cfg(test)]
            without_loss_gate: false,
        }
    }

    /// The cap in bytes per second now, or `None` for no cap. The fair
    /// share is read on each call, so it follows the connection count.
    pub(crate) fn rate(&self) -> Option<u64> {
        // `paced_rate` lowers a controller's rate to the cap; given no
        // controller rate (`usize::MAX`) it returns the cap itself, or
        // `usize::MAX` when there is none.
        // The node builds no manager for a `total-bandwidth-limit` of 0
        // (`NetworkApiConfig::global_bandwidth_manager`), for UDP and QUIC
        // alike.
        let global = self
            .global
            .as_ref()
            .map(|global| global.current_per_connection_rate());
        match crate::transport::peer_connection::paced_rate(
            usize::MAX,
            global,
            self.bandwidth_limit,
        ) {
            usize::MAX => None,
            rate => Some(rate as u64),
        }
    }
}

/// What the endpoint's settings are derived from.
#[derive(Debug, Clone)]
pub(crate) struct QuicSettings {
    /// The memory the node may use (host RAM, or a cgroup limit): sizes the
    /// buffer budget ([`quic_buffer_budget_for`]) and the floor window
    /// (`budget::floor_window_bytes`).
    pub memory_limit: usize,
    /// Simulation-only: quinn's idle timeout, in place of the time
    /// source's connection idle timeout (24 h on `VirtualTime`), so a
    /// simulation can see a silent QUIC path time out. `None` in production.
    pub idle_timeout: Option<Duration>,
    /// The window every connection starts with, in place of the floor.
    /// `None` in production; the memory measurements set it to measure quinn
    /// at a given window.
    pub initial_window: Option<u64>,
    /// The rate each connection's window is capped at. Set by the transport
    /// from its bandwidth settings when it binds (`bind_udp_listener`);
    /// none in what the config produces.
    pub rate_limit: QuicRateLimit,
}

impl QuicSettings {
    /// The flow-control window every connection starts with, per direction:
    /// the floor for the node's memory (`budget::floor_window_bytes`, 128 KiB,
    /// or 64 KiB under 2 GiB), never more than
    /// [`MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`]. A data path's windows then
    /// grow and shrink within the node's buffer budget (`budget`); a shadow
    /// path keeps these.
    ///
    /// [`MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`]: crate::transport::peer_connection::streaming::MAX_INBOUND_STREAM_BYTES_PER_CONNECTION
    pub(crate) fn window_bytes(&self) -> u64 {
        self.initial_window
            .unwrap_or_else(|| super::budget::floor_window_bytes(self.memory_limit))
            .min(PER_CONNECTION_INBOUND_CAP)
    }

    /// The connection receive window every connection starts with:
    /// [`INITIAL_RECEIVE_WINDOW_BYTES`], or the settings' initial window, at most
    /// [`Self::window_bytes`]. A data path's meter raises it to the floor.
    pub(crate) fn initial_receive_window(&self) -> u64 {
        self.initial_window
            .unwrap_or(INITIAL_RECEIVE_WINDOW_BYTES)
            .min(self.window_bytes())
    }
}

/// The per-stream receive window: the largest connection window
/// (`budget::MAX_WINDOW_BYTES`), within [`PER_CONNECTION_INBOUND_CAP`].
pub(crate) fn stream_window() -> u64 {
    super::budget::MAX_WINDOW_BYTES.min(PER_CONNECTION_INBOUND_CAP)
}

/// [`QuicSettings::window_bytes`]'s upper bound, and the per-stream receive
/// window's: the UDP transport's per-connection reassembly allowance.
const PER_CONNECTION_INBOUND_CAP: u64 =
    crate::transport::peer_connection::streaming::MAX_INBOUND_STREAM_BYTES_PER_CONNECTION;

/// Fill `out` from the OS entropy pool, for key material: the
/// stateless-reset key, the handshake-token key and the connection-ID MAC
/// key. These must not come from `GlobalRng`, whose seed is fixed in tests
/// and would make the keys predictable to anyone who reads the seed
/// (`.claude/rules/code-style.md`, OsRng exception). They do not affect the
/// order or timing of what the endpoint sends, so a seeded run still
/// repeats its event trace. ring's SystemRandom reads getrandom(2) (or the
/// platform equivalent); a failure is returned, so the endpoint is not built
/// and the node runs with QUIC off instead of panicking.
pub(crate) fn os_random(out: &mut [u8]) -> std::io::Result<()> {
    use ring_crypto::rand::SecureRandom;
    ring_crypto::rand::SystemRandom::new()
        .fill(out)
        .map_err(|_| std::io::Error::other("the OS entropy source failed"))
}

/// The endpoint configuration: version, payload size, greasing, the
/// stateless-reset key, the connection-ID generator and quinn's RNG seed.
/// Keys come from the OS ([`os_random`]); quinn's RNG seed comes from
/// [`GlobalRng`], so a seeded run is repeatable.
pub(crate) fn endpoint_config() -> std::io::Result<EndpointConfig> {
    endpoint_config_with_cid_key(MacConnectionIdGenerator::new_key()?)
}

/// [`endpoint_config`] with a given connection-ID MAC key, so a test can
/// make IDs the endpoint accepts.
pub(crate) fn endpoint_config_with_cid_key(cid_key: [u8; 32]) -> std::io::Result<EndpointConfig> {
    let mut reset_key = [0u8; 64];
    os_random(&mut reset_key)?;
    let mut config = EndpointConfig::new(Arc::new(hmac::Key::new(hmac::HMAC_SHA256, &reset_key)));
    config.cid_generator(move || Box::new(MacConnectionIdGenerator::new(cid_key)));
    let mut seed = [0u8; 32];
    GlobalRng::fill_bytes(&mut seed);
    config
        // MAX_UDP_PAYLOAD (1200) is within quinn's accepted 1200..=65527.
        .max_udp_payload_size(MAX_UDP_PAYLOAD)
        .unwrap_or_else(|_| unreachable!("1200 is quinn's minimum UDP payload size"))
        .supported_versions(vec![QUIC_VERSION])
        // Must stay false: a cleared fixed bit could make a short-header
        // packet start with 0x01 or 0x02, which the demux reads as a UDP
        // transport packet, or 0x03, which it drops as a native punch
        // (demux::PUNCH_FIRST_BYTE) (RFC 9287; transport::demux). Every
        // endpoint, native connections included, is built from this config.
        .grease_quic_bit(false)
        .min_reset_interval(Duration::from_millis(20))
        .rng_seed(Some(seed));
    Ok(config)
}

/// The transport configuration shared by both directions of a connection.
/// `time` decides the keepalive and the idle timeout, as for the UDP
/// transport: no keepalive and a long idle timeout where the time source
/// says so (simulation).
pub(crate) fn transport_config<T: TimeSource>(
    settings: &QuicSettings,
    time: &T,
) -> TransportConfig {
    let window = settings.window_bytes();
    let mut config = TransportConfig::default();
    // No `max_ack_delay` is set, so quinn's default (25 ms) applies; the
    // UDP-side inbound concurrency gauge assumes it
    // (`peer_connection::inbound_concurrency::ACK_DELAY_ALLOWANCE`). If it
    // is ever set here, change that constant to match.
    config
        .max_concurrent_uni_streams(VarInt::from_u32(INITIAL_UNI_STREAMS))
        .max_concurrent_bidi_streams(VarInt::from_u32(MAX_BIDI_STREAMS))
        .max_idle_timeout(Some(idle_timeout(
            settings
                .idle_timeout
                .unwrap_or_else(|| time.connection_idle_timeout()),
        )))
        .keep_alive_interval(time.supports_keepalive().then_some(KEEPALIVE_INTERVAL))
        // A stream may use all of the largest connection window a path can
        // grow to; the connection window, set per data path from the buffer
        // budget (`budget`), is what bounds the peer. Both are at most
        // budget::MAX_WINDOW_BYTES (8 MiB), far inside a VarInt.
        .stream_receive_window(VarInt::from_u32(stream_window() as u32))
        .receive_window(VarInt::from_u32(settings.initial_receive_window() as u32))
        .send_window(window)
        .send_fairness(true)
        .initial_mtu(MAX_UDP_PAYLOAD)
        .min_mtu(MAX_UDP_PAYLOAD)
        .mtu_discovery_config(None::<MtuDiscoveryConfig>)
        .pad_to_mtu(false)
        .enable_segmentation_offload(false)
        // Cubic, wrapped to cap the window to the configured bandwidth and to
        // count the packets the peer acknowledges (the path's acknowledgement
        // watch), `recycle::CappedCubic`.
        .congestion_controller_factory(Arc::new(super::recycle::CappedCubicFactory::new(
            settings.rate_limit.clone(),
        )))
        .initial_rtt(Duration::from_millis(333))
        .packet_threshold(3)
        .time_threshold(9.0 / 8.0)
        .persistent_congestion_threshold(3)
        .ack_frequency_config(None)
        .crypto_buffer_size(QUIC_CRYPTO_BUFFER_BYTES)
        // The spin bit lets an on-path observer measure a connection's RTT.
        .allow_spin(false)
        // No unreliable datagrams: messages go on streams.
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0);
    config
}

/// `timeout` as quinn's idle timeout, the largest quinn accepts if it is
/// longer.
fn idle_timeout(timeout: Duration) -> IdleTimeout {
    IdleTimeout::try_from(timeout).unwrap_or_else(|_| IdleTimeout::from(VarInt::MAX))
}

/// How long a Retry token stays valid. A client uses one for at most its
/// connect timeout (`upgrade::machine::CONNECT_TIMEOUT`), which this outlasts.
pub(crate) const RETRY_TOKEN_LIFETIME: Duration = Duration::from_secs(15);

/// A server configuration over `crypto`: no migration, no address
/// validation tokens from NEW_TOKEN frames (Retry tokens only), few and small
/// pending connection attempts, keys from [`GlobalRng`] and the clock from
/// `time`.
pub(crate) fn server_config(
    crypto: Arc<dyn quinn::crypto::ServerConfig>,
    transport: Arc<TransportConfig>,
    clock: Arc<dyn quinn::TimeSource>,
) -> std::io::Result<ServerConfig> {
    let mut master_key = [0u8; 64];
    os_random(&mut master_key)?;
    let token_key = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(&master_key);
    let mut tokens = ValidationTokenConfig::default();
    tokens.sent(0).log(Arc::new(NoneTokenLog));
    let mut config = ServerConfig::new(crypto, Arc::new(token_key));
    config
        .transport_config(transport)
        .validation_token_config(tokens)
        .retry_token_lifetime(RETRY_TOKEN_LIFETIME)
        // A migrated connection would change the remote address, which the
        // node uses as the peer's identity (#5940 decision 7).
        .migration(false)
        .preferred_address_v4(None)
        .preferred_address_v6(None)
        .max_incoming(MAX_INCOMING)
        .incoming_buffer_size(INCOMING_BUFFER_BYTES)
        .incoming_buffer_size_total(INCOMING_BUFFER_TOTAL_BYTES)
        .time_source(clock);
    Ok(config)
}

/// A client configuration over `crypto`: version 1, no stored tokens, and
/// initial destination connection IDs from [`GlobalRng`].
pub(crate) fn client_config(
    crypto: Arc<dyn quinn::crypto::ClientConfig>,
    transport: Arc<TransportConfig>,
) -> ClientConfig {
    let mut config = ClientConfig::new(crypto);
    config
        .transport_config(transport)
        .token_store(Arc::new(NoneTokenStore))
        .version(QUIC_VERSION)
        .initial_dst_cid_provider(Arc::new(|| {
            let mut bytes = [0u8; 16];
            GlobalRng::fill_bytes(&mut bytes);
            quinn::ConnectionId::new(&bytes)
        }));
    config
}

/// The rustls settings both sides share: the ring provider, TLS 1.3 only.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The TLS server configuration: TLS 1.3 only with the ring provider, the
/// raw public key in `key`, peers checked by `verifier`, the fixed ALPN, and
/// no session resumption, tickets or early data. `identity` builds the key
/// and the verifiers.
pub(crate) fn tls_server_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<quinn::crypto::rustls::QuicServerConfig, TlsConfigError> {
    let config = rustls_server_config(key, verifier)?;
    Ok(quinn::crypto::rustls::QuicServerConfig::try_from(
        Arc::new(config),
    )?)
}

/// The rustls half of [`tls_server_config`].
pub(crate) fn rustls_server_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(Arc::new(
            rustls::server::AlwaysResolvesServerRawPublicKeys::new(key),
        ));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.max_early_data_size = 0;
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.cert_compressors = Vec::new();
    config.cert_decompressors = Vec::new();
    Ok(config)
}

/// The TLS client configuration: TLS 1.3 only with the ring provider, the
/// raw public key in `key`, the server checked by `verifier`, the fixed
/// ALPN, no SNI, and no session resumption or early data. `identity` builds
/// the key and the verifier.
pub(crate) fn tls_client_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<quinn::crypto::rustls::QuicClientConfig, TlsConfigError> {
    let config = rustls_client_config(key, verifier)?;
    Ok(quinn::crypto::rustls::QuicClientConfig::try_from(
        Arc::new(config),
    )?)
}

/// The rustls half of [`tls_client_config`].
pub(crate) fn rustls_client_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<rustls::ClientConfig, rustls::Error> {
    let mut config = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(
            rustls::client::AlwaysResolvesClientRawPublicKeys::new(key),
        ));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.enable_sni = false;
    config.enable_early_data = false;
    config.resumption = rustls::client::Resumption::disabled();
    config.cert_compressors = Vec::new();
    config.cert_decompressors = Vec::new();
    Ok(config)
}

/// A TLS configuration quinn or rustls rejected.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsConfigError {
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    NoInitialCipherSuite(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
}

/// quinn's system clock (used to date handshake tokens) over `time`, so it
/// follows `VirtualTime` too. The epoch is arbitrary: tokens are not used,
/// and nothing compares this with real dates.
pub(crate) fn system_clock<T: TimeSource>(time: T) -> Arc<dyn quinn::TimeSource> {
    Arc::new(SystemClock(time))
}

struct SystemClock<T>(T);

impl<T: TimeSource> quinn::TimeSource for SystemClock<T> {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + self.0.now()
    }
}
