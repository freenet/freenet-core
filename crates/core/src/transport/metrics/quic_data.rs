//! Telemetry of QUIC data paths (#5940, e5b-8).
//!
//! Node-wide counts of what the paths' data sends and receives did, gathered from
//! the per-path counters (`QuicPathStats`) where each is recorded, so a count is
//! made once, at the decision. Exported per period in `transport_snapshot`, each
//! count only when non-zero and the gauge always. A family is the keys of one
//! label list under one prefix; no other key starts with a family's prefix, or
//! with the prefix its sibling families share (`quic_data_`), so summing either
//! never adds two kinds of count:
//!
//! | Key | What |
//! |---|---|
//! | `quic_data_send_<kind>_<result>` | data sends by kind and how they ended |
//! | `quic_data_event_<event>` | short overflow to UDP, refusals, stall closes, lost UDP sends, ack waits released at their bound |
//! | `quic_data_stop_<code>` | stops the peer sent to data sends, by code |
//! | `quic_inbound_<outcome>` | every `QuicInboundOutcome`, node-wide |
//! | `quic_enabled_data_paths` | paths where both sides set the data bit |
//! | `quic_live_data_paths` | paths carrying data now (a gauge, not reset, sent every period, 0 included); a path the peer refused data on is not one |
//! | `quic_sends`, `quic_open_uni_waited` | streams a path tried to open (data and control), and those whose open had to wait for a stream credit |
//! | `quic_streamed_sends`, `quic_stream_permit_waited` | streamed-send permits taken, and those that waited |
//! | `quic_bytes_sent`, `quic_bytes_received` | UDP payload bytes of QUIC paths, from quinn's counts |
//! | `quic_session_ended_with_data_path_<age>` | sessions that ended under a data path, by the path's age |
//! | `quic_sent_packets`, `quic_lost_packets`, `quic_lost_bytes`, `quic_congestion_events` | quinn's per-path counts (`PathStats`), shadow paths included, summed over the node's paths up to each path's last tick |
//! | `quic_loss_rate_<bucket>` | live paths that have sent at least 200 packets since they started, by their own lost/sent over that time (a gauge, not reset, every bucket sent every period, 0 included): `zero`, `lt_0_5pct`, `lt_1pct`, `lt_2pct`, `lt_5pct`, `lt_10pct`, `lt_20pct`, `ge_20pct` |
//! | `quic_ended_loss_rate_<bucket>` | paths that ended in the period, by the bucket they were in at the end (same buckets) |
//! | `quic_ended_paths_below_floor` | paths that ended in the period before sending 200 packets, so in no bucket |
//! | `quic_carried_packets_sent`, `quic_carried_packets_lost` | the period's packets sent, and declared lost, while their path carried data (a subset of `quic_sent_packets` and `quic_lost_packets`; the shadow share is the difference, a reconciliation residual: both operands are recorded by `record_path_loss` under the one `loss` lock, the exception the bug-prevention rule "metric describing a filtering decision, re-derived at the call site" allows, as for `broadcast_payload_mix`) |
//! | `quic_carrying_loss_rate_<bucket>`, `quic_carrying_ended_loss_rate_<bucket>`, `quic_carried_paths_ended_below_floor` | the same three as for every path, for paths that carry data, by their loss counted from the first report that found the path carrying (a path whose data bit came on after it started starts its carrying count then); every gauge bucket sent every period |
//! | `quic_srtt_<bucket>` | live QUIC paths that carry data, by their smoothed round-trip time (quinn's `PathStats::rtt`) at their last tick (a gauge, not reset, every bucket sent every period, 0 included): `lt_10ms`, `lt_25ms`, `lt_50ms`, `lt_100ms`, `lt_200ms`, `lt_400ms`, `ge_400ms`. A path is in a bucket from its first tick as a data path (it is built from an established connection, which has an RTT sample), and leaves when it stops carrying or ends; paths that carry no data are in none |
//! | `quic_tx_bps_<bucket>` | live QUIC paths that carry data, by the datagram bits per second quinn built on them over their last tick (about 2 s; a gauge, not reset, every bucket sent every period, 0 included): `lt_10kbps`, `lt_100kbps`, `lt_1mbps`, `lt_10mbps`, `lt_100mbps`, `ge_100mbps`. The bytes are UDP payload as quinn counts them (`udp_tx.bytes`): retransmissions, acknowledgements, padding and keepalives included, so it is neither goodput nor capacity. These two gauges describe path conditions; the speed of QUIC against UDP is answered from transfer events (bytes over duration, by transport and release), not from them. One tick's rate, sampled when the snapshot is taken, so it describes the fleet's distribution rather than one path's throughput; a tick of 0 ms is not counted |
//! | `quic_streams_sampled_paths`, `quic_streams_open_ge_<n>`, `quic_streams_peak_max` | per path and period, the peak of inbound streams read at once: the paths sampled, those whose peak reached each level, and the highest peak |
//! | `quic_gate_loss_<verdict>` | congestion events by the loss gate's verdict (`recycle::loss_gate`): passed to Cubic as `queue`, `share` (loss share above the ceiling), `persistent`, `ecn` (a congestion mark), `no_sample`, or `ignored` as random loss; summed over the node's paths up to each path's last tick. Over a path's life they sum to its `quic_congestion_events`; within one period they can differ by the events between the two reads of a tick |
//! | `quic_gate_ended_queue_<bucket>` | paths that ended in the period after at least 20 gate verdicts, by the share of them passed as `queue`: `lt_10pct`, `lt_50pct`, `ge_50pct`. A path whose losses mostly read as a queue runs as plain Cubic, whether the queue is real or jitter |
//! | `quic_carried_dark_episode_<label>` | the same, for the episodes of paths that carried data when the episode ended: a subset of `quic_dark_episode_<label>`, counted by the same decision |
//! | `quic_dark_episode_<label>` | dark episodes on QUIC paths (`recycle::AckWatch`: packets outstanding, nothing acknowledged and nothing heard) by how they ended: `recovered_lt_5s`, `lt_10s`, `lt_30s`, `lt_120s`, `recovered_ge_120s`, `unrecovered` (the path closed, or nothing was outstanding any more), `cap_held` (the egress cap held this side's sends, so darkness could no longer be told), `closed_at_window` (the dark-path rule closed a path that carries data and has no UDP session, a native path). A path that carries no data, and since 0.2.159 a data path with a UDP session (`recycle::DARK_CLOSES_WITH_UDP`), is never closed by that rule, so its episodes run to their end (a data path with a UDP session counts the close the rule would have made in `quic_stall_would_close_dark`); read at the watch's 2 s ticks |
//! | `quic_ack_wait_expired_<label>` | sends whose wait for the acknowledgement of their end ran out and whose stream was reset, by kind: `stream` (a send with an ack wait: a Stream, or a Short past one packet), `short` (a one-packet Short, past its 150 s bound) or `control` (a control frame, bounded as a one-packet Short); only those that count as strikes (`path::StrikeCheck`: evidence the end went out and the peer is alive). An honest peer acknowledges an end within a few round trips, so this should be 0 on every honest path (`recycle::ACK_WAIT_EXPIRY_LIMIT` in `ACK_WAIT_EXPIRY_WINDOW` closes a path with no UDP session and counts a would-close on one with a UDP session, `recycle::ACK_WAITS_EXPIRED_CLOSES_WITH_UDP`) |
//! | `quic_late_end_<label>` | sends whose end was acknowledged late, or not at all, though the peer had wholly acknowledged enough data-stream bytes since the end to have carried it (`path::LateEndCheck`), by the same kinds as `quic_ack_wait_expired_<label>`; expected 0 on every honest path. Each time `recycle::ACK_WAIT_EXPIRY_LIMIT` separate episodes (late ends within `recycle::LATE_END_EPISODE_GAP` of the last counted one are one episode) come within `ACK_WAIT_EXPIRY_WINDOW` a would-close is counted on every path (`quic_stall_would_close_late_ends`); a path with no UDP session (a native one) is closed under `late_ends` only once `recycle::LATE_ENDS_CLOSE_NATIVE` is set (false: dark until the D-3 flag day), a path with a UDP session never (`recycle::LATE_ENDS_CLOSES_WITH_UDP`) |
//! | `quic_late_end_judged_<label>` | sends whose end reached the proof point of the late-end check (`path::LateEndCheck`: the peer wholly acknowledged enough later data-stream bytes that an honest peer would have acknowledged the end), by the same kinds; the denominator of `quic_late_end_<label>`, counted once per end. A `quic_late_end_<label>` of 0 says little where this is also near 0 |
//! | `quic_stall_close_<label>` | paths the acknowledgement watch closed, by why: `not_acknowledging`, `dark`, `credit_starved`, `stream_credit_withheld`, `ack_waits_expired`, `late_ends` (`recycle::Stall`; `late_ends` closes a native path only once `recycle::LATE_ENDS_CLOSE_NATIVE` is set, `recycle::LATE_ENDS_CLOSES_WITH_UDP`) |
//! | `quic_stall_would_close_<label>` | times a rule that only observes would have closed a path, by the same labels; each count starts the rule's window afresh. Three rules observe today. `ack_waits_expired` counts, on a path with a UDP session (`recycle::ACK_WAITS_EXPIRED_CLOSES_WITH_UDP` is false; a native path is closed), each time `ACK_WAIT_EXPIRY_LIMIT` strikes came within `ACK_WAIT_EXPIRY_WINDOW`, for calibrating the rule before UDP removal (#6205). `dark` counts once per episode on a data path with a UDP session, where the dark rule does not close (`recycle::DARK_CLOSES_WITH_UDP` is false); the episode is counted to its end in `quic_carried_dark_episode_<label>`. `late_ends` counts, on every path (`recycle::LATE_ENDS_CLOSES_WITH_UDP` and `recycle::LATE_ENDS_CLOSE_NATIVE` are false), each time `ACK_WAIT_EXPIRY_LIMIT` late-end episodes came within `ACK_WAIT_EXPIRY_WINDOW` (`quic_late_end_<label>`) |
//! | `quic_pipe_source_<label>` | pipes (an inbound stream forwarded on a path, `quic_data_send_pipe_*`) that ended `sent` or `dropped`, by what fed the inbound stream: `quic_fed_sent`, `quic_fed_dropped`, `udp_fed_sent`, `udp_fed_dropped`; each sums to its `quic_data_send_pipe_<result>` |
//! | `quic_short_dropped_<label>` | the Shorts counted in `quic_data_send_short_dropped`, by cause: `not_opened_exhausted` (no stream opened within the deadline: the peer's stream credit after the native retries were used up, or a wait on this side's own resources, which is not retried), `timed_out`, `paced`, `connection_lost` (connection lost or path closed mid-send), `peer_refused_data` (stop `NOT_ACCEPTING_DATA`, or the path no longer sends data), `budget_refused` (stop `BUDGET_REFUSED`), `stopped_other` (any other stop code), `invalid_frame`, `source_error`, `queue_full` (native queue full), `path_ended` (path ended, no UDP fallback), `no_udp_session`; sums to `quic_data_send_short_dropped` |
//! | `quic_pipe_dropped_<label>` | the same dropped pipes by cause: `source_error` (the inbound stream failed or delivered the wrong length), `source_stall_timeout` (a deadline ran out while the pipe waited for its source), `own_path` (anything else: this path timed out, was lost, or the peer stopped the stream); sums to `quic_data_send_pipe_dropped` |
//! | `quic_send_resets`, `quic_path_ms` | sends a path reset on this side, and the time QUIC paths were up, shadow paths included, up to each path's last tick: resets per connection-hour is their ratio |
//! | `quic_budget_bytes`, `quic_budget_charged_bytes`, `quic_budget_charged_peak_bytes` | the node's QUIC buffer budget, what its data paths' windows have grown above their floor grants now (the floor grants are each path's fixed charge, not the budget's, so a node with every path at the floor reports 0 and uploads nothing; gauges, not reset, uploaded when not 0), and the most charged at once in the period (`quic::budget`) |
//! | `quic_staged_peak_pct` | the most any connection staged in the period (bytes read for streams whose header its reader had not taken yet) as a percentage of its staging cap (`quic::path::inbound::ConnStaging`); streams stopped at the cap are `quic_inbound_evicted_staged` |
//! | `quic_window_grew`, `quic_window_grow_refused`, `quic_window_shrank` | window-limited data paths whose receive window grew, those that found no room, and paths whose windows the budget lowered |
//! | `quic_ramp_share_pct` | the share of pairs, in percent, this node sets its data bit for now: its `quic-data-percent`, or the ramp's share at the snapshot (`upgrade::slice::ramp_percent`); 0 unless the node can carry data (`prefer`, no breaker, paths allowed, the endpoint up). Read on the path clock's wall time. A gauge, sent every period, 0 included, so the release watch can group each period by ramp step. One value per process: in a simulation every node in the process shares it |
//! | `quic_send_resets_reached_<n>`, `quic_send_resets_path_max` | paths whose lifetime reset count reached each level in the period, and the highest count a reset in the period took a path to: how close paths get to `recycle::SEND_ABORT_LIMIT` |
//!
//! What the loss rate is: quinn's lost over sent (quinn-proto 0.11.19). `sent`
//! counts every packet quinn built, acknowledgement-only ones included. `lost`
//! counts quinn's loss declarations (packet or time threshold), and a packet
//! declared lost that is later acknowledged is not taken back, so spurious
//! declarations count as losses. Lost PLPMTUD probes are left out of `lost` while
//! sent probes are in `sent`; MTU discovery is off (`quic::config`), so no path
//! sends any. Packets still in flight when a connection closes are dropped
//! by quinn, not declared lost, so a path's final bucket can understate its
//! loss at the moment it ended. It is quinn's view of loss, not a measurement
//! of the link. A shadow path's packets are mostly keepalives and
//! acknowledgements, so its rate is of small packets and may not hold for
//! data-sized ones. After a contained panic in a path's acknowledgement
//! watch the path stops ticking, so its counts from then on arrive in one
//! report when it ends.
//!
//! The waits are the step-3 stream-limit bars: `quic_open_uni_waited` over
//! `quic_sends` and `quic_stream_permit_waited` over `quic_streamed_sends` below 0.5%, and
//! `quic_streams_open_ge_48` below 0.2% of `quic_streams_sampled_paths`. Both
//! wait counts are of attempts: a send that waits and then times out is counted.
//!
//! The reset counts are for the per-connection fixed charge
//! (`quic::config::QUIC_CONNECTION_CHARGE_BYTES`): each reset of a stream
//! whose `stopped()` was polled leaves an entry in quinn's `stopped` map until
//! the connection closes, so the charge takes the limit's worst case
//! (`SEND_ABORT_LIMIT` x `QUIC_STOPPED_ENTRY_BYTES`). Field rates and peaks well
//! under the limit are what would justify a lower charge or limit. A path whose
//! acknowledgement watch is off or panicked stops adding to `quic_path_ms`,
//! while its resets are still counted.

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use super::QuicPathAge;
use crate::transport::quic::path::{
    QuicInboundOutcome, QuicSendEvent, QuicSendResult, SendKind, stop_code,
};
use crate::transport::quic::recycle::loss_gate::{GATE_VERDICTS, VERDICT_LABELS, Verdict};
use crate::transport::quic::upgrade::policy::DataShare;

/// The gauge's key, uploaded in every snapshot ([`QuicDataSnapshot::upload_keys`]).
const LIVE_DATA_PATHS: &str = "quic_live_data_paths";

/// The share gauge's key, uploaded in every snapshot.
const RAMP_SHARE_PCT: &str = "quic_ramp_share_pct";

/// Where `quic_ramp_share_pct` is read from: the node's data share, read on
/// its path clock's wall time, while its QUIC endpoint is up. A node that
/// cannot carry data (QUIC off, shadow, the crash-loop breaker, no paths, the
/// endpoint down or released) reports 0, so it does not dilute the group of a
/// share step with a share it cannot use. It is this node's own share: a pair
/// carries data only when both sides' shares cover it.
pub(crate) struct ShareSource {
    pub(crate) share: DataShare,
    pub(crate) availability: crate::transport::quic::liveness::QuicAvailabilityWatch,
    pub(crate) wall: Box<dyn Fn() -> std::time::SystemTime + Send + Sync>,
}

impl ShareSource {
    fn percent(&self) -> u64 {
        match self.availability.get() {
            crate::transport::quic::liveness::QuicAvailability::Up => {
                u64::from(self.share.percent_at((self.wall)()))
            }
            crate::transport::quic::liveness::QuicAvailability::Disabled
            | crate::transport::quic::liveness::QuicAvailability::Down
            | crate::transport::quic::liveness::QuicAvailability::Released => 0,
        }
    }
}

impl std::fmt::Debug for ShareSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareSource")
            .field("share", &self.share)
            .field("availability", &self.availability.get())
            .finish_non_exhaustive()
    }
}

/// The loss-rate gauges' prefix; every bucket is uploaded in every snapshot.
const LOSS_RATE_PREFIX: &str = "quic_loss_rate_";

/// The carrying-data loss-rate gauges' prefix; uploaded like
/// [`LOSS_RATE_PREFIX`].
const CARRYING_LOSS_RATE_PREFIX: &str = "quic_carrying_loss_rate_";

/// A path's loss rate is bucketed once it has sent this many packets since it
/// started; below it the rate says too little.
pub(crate) const LOSS_RATE_MIN_SENT: u64 = 200;

/// The upper bounds of the loss-rate buckets after `zero`, in lost packets per
/// 1000 sent; the last bucket is `ge_20pct`.
const LOSS_RATE_BOUNDS_PER_MILLE: [u64; 6] = [5, 10, 20, 50, 100, 200];

/// The loss-rate buckets' labels (`quic_loss_rate_<label>`), in order.
pub(crate) const LOSS_RATE_LABELS: [&str; 8] = [
    "zero",
    "lt_0_5pct",
    "lt_1pct",
    "lt_2pct",
    "lt_5pct",
    "lt_10pct",
    "lt_20pct",
    "ge_20pct",
];

/// The bucket of `lost` packets out of `sent`. Losses found in a window can
/// be of packets sent before it, so `lost` may pass `sent`: `ge_20pct`.
fn loss_rate_bucket(lost: u64, sent: u64) -> usize {
    if lost == 0 {
        return 0;
    }
    let per_mille_times_sent = lost.saturating_mul(1000);
    1 + LOSS_RATE_BOUNDS_PER_MILLE
        .iter()
        .take_while(|bound| per_mille_times_sent >= sent.saturating_mul(**bound))
        .count()
}

/// The per-path round-trip-time gauges' prefix; every bucket is uploaded in
/// every snapshot.
const PATH_SRTT_PREFIX: &str = "quic_srtt_";

/// The upper bounds of the round-trip-time buckets, in milliseconds; the last
/// bucket is `ge_400ms`.
const PATH_SRTT_BOUNDS_MS: [u128; 6] = [10, 25, 50, 100, 200, 400];

/// The round-trip-time buckets' labels (`quic_srtt_<label>`), in order.
pub(crate) const PATH_SRTT_LABELS: [&str; 7] = [
    "lt_10ms", "lt_25ms", "lt_50ms", "lt_100ms", "lt_200ms", "lt_400ms", "ge_400ms",
];

fn path_srtt_bucket(rtt: std::time::Duration) -> usize {
    let ms = rtt.as_millis();
    PATH_SRTT_BOUNDS_MS
        .iter()
        .take_while(|bound| ms >= **bound)
        .count()
}

/// The per-path send-rate gauges' prefix; uploaded like [`PATH_SRTT_PREFIX`].
const PATH_TX_BPS_PREFIX: &str = "quic_tx_bps_";

/// The upper bounds of the send-rate buckets, in bits per second; the last
/// bucket is `ge_100mbps`.
const PATH_TX_BPS_BOUNDS: [u128; 5] = [10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];

/// The send-rate buckets' labels (`quic_tx_bps_<label>`), in order.
pub(crate) const PATH_TX_BPS_LABELS: [&str; 6] = [
    "lt_10kbps",
    "lt_100kbps",
    "lt_1mbps",
    "lt_10mbps",
    "lt_100mbps",
    "ge_100mbps",
];

/// The bucket of `bytes` sent in `ms` (not 0) milliseconds.
fn path_tx_bps_bucket(bytes: u64, ms: u64) -> usize {
    let bits_per_second = u128::from(bytes) * 8 * 1000 / u128::from(ms);
    PATH_TX_BPS_BOUNDS
        .iter()
        .take_while(|bound| bits_per_second >= **bound)
        .count()
}

/// A path enters a `quic_gate_ended_queue_` bucket only after this many gate
/// verdicts; below it the share says too little.
pub(crate) const GATE_MIN_VERDICTS: u64 = 20;

/// The labels of the gate's per-path buckets
/// (`quic_gate_ended_queue_<label>`), by the share of verdicts passed as a
/// queue: under 10%, under 50%, 50% or more.
pub(crate) const GATE_QUEUE_LABELS: [&str; 3] = ["lt_10pct", "lt_50pct", "ge_50pct"];

/// The labels of `quic_ack_wait_expired_<label>`: sends whose wait for the
/// acknowledgement of their end ran out (`recycle::AckWatch`'s expiry rule),
/// by kind: a send with an ack wait (a Stream, or a Short past one packet),
/// a one-packet Short, or a control frame (whose background wait is bounded
/// as a one-packet Short's).
pub(crate) const ACK_WAIT_EXPIRED_LABELS: [&str; 3] = ["stream", "short", "control"];

/// `quic_pipe_source_<label>`: a pipe's end by what fed its inbound stream.
pub(crate) const PIPE_SOURCE_LABELS: [&str; 4] = [
    "quic_fed_sent",
    "quic_fed_dropped",
    "udp_fed_sent",
    "udp_fed_dropped",
];

/// `quic_pipe_dropped_<label>`: why a pipe was dropped ([`PipeDropCause`]).
pub(crate) const PIPE_DROP_LABELS: [&str; 3] = ["source_error", "source_stall_timeout", "own_path"];

/// Why a pipe was dropped, in the order of [`PIPE_DROP_LABELS`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PipeDropCause {
    /// The inbound stream failed, or delivered other than its length.
    SourceError,
    /// A deadline ran out while the pipe waited for its source.
    SourceStallTimeout,
    /// Anything else: the path timed out or was lost, or the peer stopped it.
    OwnPath,
}

/// `quic_short_dropped_<label>`: why a Short was dropped ([`ShortDropCause`]).
pub(crate) const SHORT_DROP_LABELS: [&str; 12] = [
    "not_opened_exhausted",
    "timed_out",
    "paced",
    "connection_lost",
    "peer_refused_data",
    "budget_refused",
    "stopped_other",
    "invalid_frame",
    "source_error",
    "queue_full",
    "path_ended",
    "no_udp_session",
];

/// Why a Short was dropped, in the order of [`SHORT_DROP_LABELS`]. Chosen where
/// the drop is decided (`QuicPath::settle`, `DataRoute::queue_short`,
/// `send_elsewhere`), never worked out from other counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ShortDropCause {
    /// No stream opened within the deadline: the peer's stream credit, after the
    /// native retries were used up, or a wait on this side's own resources
    /// (`NotOpened::TimedOut`), which is not retried.
    NotOpenedExhausted = 0,
    /// The send's deadline ran out.
    TimedOut = 1,
    /// The bandwidth pacer refused the send.
    Paced = 2,
    /// The connection was lost or the path closed during the send.
    ConnectionLost = 3,
    /// The peer stopped the stream with `NOT_ACCEPTING_DATA`, or the route
    /// found the path no longer sending data.
    PeerRefusedData = 4,
    /// The peer stopped the stream with `BUDGET_REFUSED`.
    BudgetRefused = 5,
    /// The peer stopped the stream with another code.
    StoppedOther = 6,
    /// The frame was invalid.
    InvalidFrame = 7,
    /// The send's source failed or delivered the wrong length.
    SourceError = 8,
    /// The native route's queue was full and the path has no UDP fallback.
    QueueFull = 9,
    /// The path had ended, with no UDP fallback to take the message.
    PathEnded = 10,
    /// The route had no UDP session to hand the message to.
    NoUdpSession = 11,
}

impl ShortDropCause {
    /// Every cause, in the order of [`SHORT_DROP_LABELS`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const ALL: [Self; SHORT_DROP_LABELS.len()] = [
        Self::NotOpenedExhausted,
        Self::TimedOut,
        Self::Paced,
        Self::ConnectionLost,
        Self::PeerRefusedData,
        Self::BudgetRefused,
        Self::StoppedOther,
        Self::InvalidFrame,
        Self::SourceError,
        Self::QueueFull,
        Self::PathEnded,
        Self::NoUdpSession,
    ];
}

/// What kind of send's ack wait ran out, for `quic_ack_wait_expired_<label>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckWaitKind {
    /// A send with an ack wait (`bounded::SendDeadlines::ack_wait`).
    Stream = 0,
    /// A one-packet Short, whose acknowledgement is waited for in the
    /// background up to its bound.
    Short = 1,
    /// A control frame, waited for as a one-packet Short.
    Control = 2,
}

/// The labels of `quic_stall_close_<label>`: paths the acknowledgement watch
/// closed, by why (`recycle::Stall::label`, in its order).
pub(crate) const STALL_CLOSE_LABELS: [&str; 6] = [
    "not_acknowledging",
    "dark",
    "credit_starved",
    "stream_credit_withheld",
    "ack_waits_expired",
    "late_ends",
];

/// The labels of `quic_dark_episode_<label>`: dark episodes on QUIC paths
/// (`recycle::AckWatch`: this side's packets outstanding, nothing
/// acknowledged and nothing heard), by how they ended: the peer was heard
/// from again within 5/10/30/120 s or later (episodes are read at the
/// watch's 2 s samples, so none recovers in under 2 s); the episode ended without that
/// (the path closed, at the latest on quinn's idle timeout, or nothing was
/// outstanding any more); the egress cap held this side's sends, so whether
/// the peer was dark could no longer be told; or the dark-path rule closed the path at its
/// window, on a path that carries data and has no UDP session (a native path).
/// A path that carries no data, and a data path with a UDP session
/// (`recycle::DARK_CLOSES_WITH_UDP`), is never closed by that rule, so its
/// episodes run to their end and fill the longer buckets: that is what the
/// dark-path threshold is to be checked against.
pub(crate) const DARK_EPISODE_LABELS: [&str; 8] = [
    "recovered_lt_5s",
    "recovered_lt_10s",
    "recovered_lt_30s",
    "recovered_lt_120s",
    "recovered_ge_120s",
    "unrecovered",
    "cap_held",
    "closed_at_window",
];

/// The upper bounds of the `recovered_` buckets of [`DARK_EPISODE_LABELS`],
/// in seconds.
pub(crate) const DARK_RECOVERED_BOUNDS_SECS: [u64; 4] = [5, 10, 30, 120];

/// [`DARK_EPISODE_LABELS`]' `recovered_ge_120s`, `unrecovered` and
/// `closed_at_window`.
const DARK_RECOVERED_LATE: usize = 4;
const DARK_UNRECOVERED: usize = 5;
const DARK_CAP_HELD: usize = 6;
const DARK_CLOSED: usize = 7;

/// How a dark episode ended, for `quic_dark_episode_<label>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DarkEnd {
    /// The peer was heard from or acknowledged something after this many
    /// nanoseconds.
    Recovered(u64),
    /// It ended without that.
    Unrecovered,
    /// The egress cap held this side's sends, so whether the peer was dark
    /// could no longer be told.
    CapHeld,
    /// The dark-path rule closed the path at its window (a native data path;
    /// one with a UDP session it does not close).
    Closed,
}

/// The bucket of a path that had `queue` of `total` verdicts passed as a queue.
fn gate_queue_bucket(queue: u64, total: u64) -> usize {
    if queue.saturating_mul(10) < total {
        0
    } else if queue.saturating_mul(2) < total {
        1
    } else {
        2
    }
}

/// One path's loss-gate verdicts since it started. Owned by the path's report
/// (`recycle::PathReport`); dropping it, when the path ends, counts the path in
/// its `quic_gate_ended_queue_` bucket.
#[derive(Debug)]
pub(crate) struct PathGateWindow {
    node: &'static QuicDataCounters,
    verdicts: [u64; GATE_VERDICTS],
}

impl PathGateWindow {
    pub(crate) fn new(node: &'static QuicDataCounters) -> Self {
        Self {
            node,
            verdicts: [0; GATE_VERDICTS],
        }
    }
}

impl Drop for PathGateWindow {
    fn drop(&mut self) {
        let total: u64 = self.verdicts.iter().sum();
        if total >= GATE_MIN_VERDICTS {
            let bucket = gate_queue_bucket(self.verdicts[GATE_QUEUE], total);
            self.node.gate_ended_queue[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// The index of `queue` among the gate's verdicts (`loss_gate::Verdict::ALL`).
const GATE_QUEUE: usize = 0;
const _: () = assert!(matches!(Verdict::ALL[GATE_QUEUE], Verdict::Queue));

/// One path's packets since it started, and the loss-rate bucket it is counted
/// in. Owned by the path's acknowledgement watch; dropping it, however the watch
/// ends, takes the path out of its bucket.
#[derive(Debug)]
pub(crate) struct PathLossWindow {
    node: &'static QuicDataCounters,
    /// Every packet of the path since it started.
    all: PathTally,
    /// The packets the path sent while it carried data, from the first report
    /// that found it carrying ([`QuicDataCounters::record_path_loss`]).
    carrying: PathTally,
    /// Some report found the path carrying data.
    carried: bool,
    /// The round-trip-time and send-rate buckets the path is counted in
    /// ([`QuicDataCounters::record_path_rates`]).
    srtt_bucket: Option<usize>,
    tx_bps_bucket: Option<usize>,
}

/// One path's packets of one kind and the bucket it is counted in.
#[derive(Debug, Default)]
struct PathTally {
    sent: u64,
    lost: u64,
    bucket: Option<usize>,
}

impl PathTally {
    /// Add `sent` and `lost`, and move the path to the bucket of its new
    /// lost/sent in `family` once it has sent [`LOSS_RATE_MIN_SENT`].
    fn add(&mut self, family: &mut LossFamily, sent: u64, lost: u64) {
        self.sent = self.sent.saturating_add(sent);
        self.lost = self.lost.saturating_add(lost);
        let bucket =
            (self.sent >= LOSS_RATE_MIN_SENT).then(|| loss_rate_bucket(self.lost, self.sent));
        if bucket != self.bucket {
            if let Some(old) = self.bucket {
                family.rates[old] = family.rates[old].saturating_sub(1);
            }
            if let Some(new) = bucket {
                family.rates[new] += 1;
            }
            self.bucket = bucket;
        }
    }

    /// The path ended: out of its bucket and counted among `family`'s ended
    /// paths by it, or as ended below the floor.
    fn end(&mut self, family: &mut LossFamily) {
        match self.bucket.take() {
            Some(bucket) => {
                family.rates[bucket] = family.rates[bucket].saturating_sub(1);
                family.ended[bucket] = family.ended[bucket].saturating_add(1);
            }
            None => {
                family.ended_below_floor = family.ended_below_floor.saturating_add(1);
            }
        }
    }
}

impl PathLossWindow {
    /// A window for a new path, counted in `node`.
    pub(crate) fn new(node: &'static QuicDataCounters) -> Self {
        Self {
            node,
            all: PathTally::default(),
            carrying: PathTally::default(),
            carried: false,
            srtt_bucket: None,
            tx_bps_bucket: None,
        }
    }
}

impl PathLossWindow {
    /// Whether the path is in a loss-rate bucket. Tests only.
    #[cfg(test)]
    pub(crate) fn is_bucketed(&self) -> bool {
        self.all.bucket.is_some()
    }
}

impl Drop for PathLossWindow {
    /// The path ended: out of its bucket, and counted among the period's ended
    /// paths by that bucket, so paths that fail lossy still show; a path that
    /// never reached the floor is counted as ended below it. A path that
    /// carried data is counted the same way in the carrying family.
    fn drop(&mut self) {
        let mut counts = self.node.loss.lock();
        if let Some(old) = self.srtt_bucket.take() {
            counts.path_srtt[old] = counts.path_srtt[old].saturating_sub(1);
        }
        if let Some(old) = self.tx_bps_bucket.take() {
            counts.path_tx_bps[old] = counts.path_tx_bps[old].saturating_sub(1);
        }
        self.all.end(&mut counts.all);
        if self.carried {
            self.carrying.end(&mut counts.carrying);
        }
    }
}

/// The loss sums of the period and the live paths by bucket. Every field is
/// behind the one lock, so a report's sums and its bucket move can only be
/// written together, and a snapshot reads and resets them together: that one
/// critical section, in `record_path_loss` and `snapshot`, is what keeps them
/// in one snapshot.
#[derive(Debug, Clone, Copy, Default)]
struct LossCounts {
    sent_packets: u64,
    lost_packets: u64,
    lost_bytes: u64,
    congestion_events: u64,
    /// Every path.
    all: LossFamily,
    /// The packets of the period sent while their path carried data.
    carrying_sent_packets: u64,
    carrying_lost_packets: u64,
    /// The paths that carried data, by their loss while they did.
    carrying: LossFamily,
    /// Live paths by round-trip-time bucket: a gauge, not reset.
    path_srtt: [u32; PATH_SRTT_LABELS.len()],
    /// Live paths by send-rate bucket: a gauge, not reset.
    path_tx_bps: [u32; PATH_TX_BPS_LABELS.len()],
}

/// Paths by loss-rate bucket: live ones and those that ended in the period.
#[derive(Debug, Clone, Copy, Default)]
struct LossFamily {
    /// A gauge of live paths, not reset by a snapshot.
    rates: [u32; LOSS_RATE_LABELS.len()],
    /// Paths that ended in the period, by their last bucket.
    ended: [u64; LOSS_RATE_LABELS.len()],
    /// Paths that ended in the period before reaching the floor.
    ended_below_floor: u64,
}

impl LossFamily {
    /// The period's counts reset; the gauge kept.
    fn next_period(self) -> Self {
        Self {
            rates: self.rates,
            ..Self::default()
        }
    }
}

/// The levels `quic_streams_open_ge_<n>` reports, up to the streamed-send limit
/// (`quic::config::STREAMED_SEND_LIMIT`), which is the bar.
pub(crate) const QUIC_STREAMS_OPEN_THRESHOLDS: [u32; 4] = [8, 16, 32, 48];

/// The levels `quic_send_resets_reached_<n>` reports, up to
/// `recycle::SEND_ABORT_LIMIT`: one more reset past the last closes the path.
pub(crate) const QUIC_SEND_RESET_LEVELS: [u64; 4] = [64, 512, 2048, 4096];
const _: () = assert!(
    QUIC_SEND_RESET_LEVELS[QUIC_SEND_RESET_LEVELS.len() - 1]
        == crate::transport::quic::recycle::SEND_ABORT_LIMIT,
    "the last level is the reset limit"
);

/// The streams-open counts of one period, under one lock with its period, as the
/// connection-level inbound concurrency counts are.
#[derive(Debug, Default, Clone, Copy)]
struct StreamsOpenCounts {
    paths_sampled: u32,
    ge: [u32; QUIC_STREAMS_OPEN_THRESHOLDS.len()],
    max: u32,
}

/// One path's peak of inbound streams read at once in the current period. Owned
/// by the path's receive task.
#[derive(Debug, Default)]
pub(crate) struct StreamsOpenPeak {
    /// The period the peak belongs to; 0 before the first sample.
    epoch: u64,
    peak: u32,
}

/// The node-wide counts. A field of `TransportMetrics`.
#[derive(Debug)]
pub(crate) struct QuicDataCounters {
    sends: [[AtomicU64; QuicSendResult::COUNT]; SendKind::ALL.len()],
    events: [AtomicU64; QuicSendEvent::COUNT],
    stops: [AtomicU64; stop_code::ALL.len()],
    inbound: [AtomicU64; QuicInboundOutcome::COUNT],
    data_enabled: AtomicU64,
    /// A gauge: paths with a data route now.
    data_paths: AtomicU64,
    sends_opened: AtomicU64,
    open_uni_waited: AtomicU64,
    permit_takes: AtomicU64,
    permit_waited: AtomicU64,
    bytes_sent: AtomicU64,
    bytes_received: AtomicU64,
    session_ended: [AtomicU64; QuicPathAge::COUNT],
    loss: Mutex<LossCounts>,
    streams_epoch: AtomicU64,
    streams: Mutex<StreamsOpenCounts>,
    send_resets: AtomicU64,
    path_ms: AtomicU64,
    send_resets_reached: [AtomicU64; QUIC_SEND_RESET_LEVELS.len()],
    send_resets_path_max: AtomicU64,
    budget_bytes: AtomicU64,
    budget_charged: AtomicU64,
    budget_charged_peak: AtomicU64,
    window_changes: [AtomicU64; 3],
    staged_peak_pct: AtomicU64,
    gate_loss: [AtomicU64; GATE_VERDICTS],
    gate_ended_queue: [AtomicU64; GATE_QUEUE_LABELS.len()],
    dark_episodes: [AtomicU64; DARK_EPISODE_LABELS.len()],
    carried_dark_episodes: [AtomicU64; DARK_EPISODE_LABELS.len()],
    /// Where this node's data share is read from, set when its upgrades are
    /// built; `None` unless it may carry data (`policy::QuicPolicy::share_source`).
    share: Mutex<Option<ShareSource>>,
    ack_wait_expired: [AtomicU64; ACK_WAIT_EXPIRED_LABELS.len()],
    late_end: [AtomicU64; ACK_WAIT_EXPIRED_LABELS.len()],
    late_end_judged: [AtomicU64; ACK_WAIT_EXPIRED_LABELS.len()],
    stall_closes: [AtomicU64; STALL_CLOSE_LABELS.len()],
    stall_would_close: [AtomicU64; STALL_CLOSE_LABELS.len()],
    pipe_source: [AtomicU64; PIPE_SOURCE_LABELS.len()],
    pipe_dropped: [AtomicU64; PIPE_DROP_LABELS.len()],
    short_dropped: [AtomicU64; SHORT_DROP_LABELS.len()],
}

impl QuicDataCounters {
    pub(crate) fn new() -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            sends: std::array::from_fn(|_| std::array::from_fn(|_| zero())),
            events: std::array::from_fn(|_| zero()),
            stops: std::array::from_fn(|_| zero()),
            inbound: std::array::from_fn(|_| zero()),
            data_enabled: zero(),
            data_paths: zero(),
            sends_opened: zero(),
            open_uni_waited: zero(),
            permit_takes: zero(),
            permit_waited: zero(),
            bytes_sent: zero(),
            bytes_received: zero(),
            session_ended: std::array::from_fn(|_| zero()),
            loss: Mutex::new(LossCounts::default()),
            streams_epoch: AtomicU64::new(1),
            streams: Mutex::new(StreamsOpenCounts::default()),
            send_resets: zero(),
            path_ms: zero(),
            send_resets_reached: std::array::from_fn(|_| zero()),
            send_resets_path_max: zero(),
            budget_bytes: zero(),
            budget_charged: zero(),
            budget_charged_peak: zero(),
            window_changes: std::array::from_fn(|_| zero()),
            staged_peak_pct: zero(),
            gate_loss: std::array::from_fn(|_| zero()),
            gate_ended_queue: std::array::from_fn(|_| zero()),
            dark_episodes: std::array::from_fn(|_| zero()),
            carried_dark_episodes: std::array::from_fn(|_| zero()),
            share: Mutex::new(None),
            ack_wait_expired: std::array::from_fn(|_| zero()),
            late_end: std::array::from_fn(|_| zero()),
            late_end_judged: std::array::from_fn(|_| zero()),
            stall_closes: std::array::from_fn(|_| zero()),
            stall_would_close: std::array::from_fn(|_| zero()),
            pipe_source: std::array::from_fn(|_| zero()),
            pipe_dropped: std::array::from_fn(|_| zero()),
            short_dropped: std::array::from_fn(|_| zero()),
        }
    }

    /// A pipe that ended `sent`, fed by a QUIC path or by UDP.
    pub(crate) fn record_pipe_sent(&self, quic_fed: bool) {
        let index = if quic_fed { 0 } else { 2 };
        self.pipe_source[index].fetch_add(1, Ordering::Relaxed);
    }

    /// A pipe that ended `dropped`, fed by a QUIC path or by UDP, for `cause`.
    pub(crate) fn record_pipe_dropped(&self, quic_fed: bool, cause: PipeDropCause) {
        let index = if quic_fed { 1 } else { 3 };
        self.pipe_source[index].fetch_add(1, Ordering::Relaxed);
        self.pipe_dropped[cause as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// Where this node's data share is read from
    /// (`policy::QuicPolicy::share_source`), read at each snapshot as
    /// `quic_ramp_share_pct`.
    pub(crate) fn set_share(&self, share: Option<ShareSource>) {
        *self.share.lock() = share;
    }

    /// The cause of a Short counted as `short_dropped`. Called only by
    /// `QuicPathStats::record_short_dropped`, which also counts the send, so the
    /// causes sum to `quic_data_send_short_dropped`.
    pub(crate) fn record_short_drop_cause(&self, cause: ShortDropCause) {
        self.short_dropped[cause as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_send(&self, kind: SendKind, result: QuicSendResult) {
        self.sends[kind as usize][result as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_event(&self, event: QuicSendEvent) {
        self.events[event as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A stop from the peer; a code this node does not know is not counted here.
    pub(crate) fn record_stop(&self, code: u64) {
        if let Some(index) = stop_code::ALL.iter().position(|c| u64::from(*c) == code) {
            self.stops[index].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_inbound(&self, outcome: QuicInboundOutcome) {
        self.inbound[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_data_enabled(&self) {
        self.data_enabled.fetch_add(1, Ordering::Relaxed);
    }

    /// A path took a data route. Pair with [`Self::data_path_ended`].
    pub(crate) fn data_path_started(&self) {
        self.data_paths.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn data_path_ended(&self) {
        // A failed update means the gauge was already 0: nothing to undo.
        if self
            .data_paths
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .is_err()
        {
            tracing::debug!("quic_live_data_paths was already 0 when a data path ended");
        }
    }

    /// A stream was opened on a path, data or control; `waited` when the open did
    /// not complete on its first poll, so the peer's stream credit was used up.
    pub(crate) fn record_open(&self, waited: bool) {
        self.sends_opened.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.open_uni_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A streamed send took a permit; `waited` when none was free at once.
    pub(crate) fn record_permit(&self, waited: bool) {
        self.permit_takes.fetch_add(1, Ordering::Relaxed);
        if waited {
            self.permit_waited.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// UDP payload bytes a path sent and received since its last report.
    pub(crate) fn record_bytes(&self, sent: u64, received: u64) {
        self.bytes_sent.fetch_add(sent, Ordering::Relaxed);
        self.bytes_received.fetch_add(received, Ordering::Relaxed);
    }

    /// A path's packet counts changed by `delta` since its last report. Summed
    /// node-wide for the period, and added to the path's own window, which runs
    /// from the path's start: once it holds [`LOSS_RATE_MIN_SENT`] sent packets
    /// the path is counted in the bucket of its lost/sent, and moved when that
    /// changes, so each live path is in at most one bucket. See the module doc
    /// for what the rate is. When the path `carrying` data, the same packets are
    /// also counted in the carrying sums and the path's carrying window, which
    /// so starts at the first report that finds the path carrying, however long
    /// after the path started; a path that stops carrying (the peer refused
    /// data) keeps its carrying bucket until it ends. The sums and the bucket
    /// moves are written under the one lock ([`LossCounts`]), the total and the
    /// carrying subset in the same call, so the shadow share (total minus
    /// carrying) is a reconciliation residual of two values one recording
    /// produced, not a count re-derived from things another filter touched.
    pub(crate) fn record_path_loss(
        &self,
        window: &mut PathLossWindow,
        delta: crate::transport::quic::recycle::PathLoss,
        carrying: bool,
    ) {
        debug_assert!(
            std::ptr::eq(window.node, self),
            "a window of these counters"
        );
        let mut counts = self.loss.lock();
        counts.sent_packets = counts.sent_packets.saturating_add(delta.sent_packets);
        counts.lost_packets = counts.lost_packets.saturating_add(delta.lost_packets);
        counts.lost_bytes = counts.lost_bytes.saturating_add(delta.lost_bytes);
        counts.congestion_events = counts
            .congestion_events
            .saturating_add(delta.congestion_events);
        window
            .all
            .add(&mut counts.all, delta.sent_packets, delta.lost_packets);
        if carrying {
            counts.carrying_sent_packets = counts
                .carrying_sent_packets
                .saturating_add(delta.sent_packets);
            counts.carrying_lost_packets = counts
                .carrying_lost_packets
                .saturating_add(delta.lost_packets);
            window
                .carrying
                .add(&mut counts.carrying, delta.sent_packets, delta.lost_packets);
            window.carried = true;
        }
    }

    /// A tick of a path. While the path `carrying` data, move it to the bucket
    /// of its smoothed round-trip time `srtt` and, when `sent` bytes went out
    /// in the `ms` (not 0) milliseconds since the last tick, to the bucket of
    /// that rate; a path that is not carrying data is in neither (a path that
    /// stops carrying leaves its buckets), so idle shadow paths do not fill the
    /// lowest buckets. A tick of 0 ms leaves the rate where it was, and its
    /// bytes are not counted in any rate. A path is in one bucket of each
    /// family while it carries, and [`PathLossWindow`]'s drop takes it out.
    /// `sent` is the datagram bytes quinn built (retransmissions,
    /// acknowledgements, padding and keepalives included), not goodput.
    ///
    /// The buckets are moved under the `loss` lock, in a critical section of
    /// their own: they are read by the same snapshot but not written with a
    /// loss report's sums.
    pub(crate) fn record_path_rates(
        &self,
        window: &mut PathLossWindow,
        srtt: std::time::Duration,
        sent: u64,
        ms: u64,
        carrying: bool,
    ) {
        debug_assert!(
            std::ptr::eq(window.node, self),
            "a window of these counters"
        );
        let srtt_bucket = carrying.then(|| path_srtt_bucket(srtt));
        let tx_bps_bucket = match (carrying, ms) {
            (false, _) => None,
            (true, 0) => window.tx_bps_bucket,
            (true, _) => Some(path_tx_bps_bucket(sent, ms)),
        };
        let mut counts = self.loss.lock();
        if srtt_bucket != window.srtt_bucket {
            if let Some(old) = window.srtt_bucket {
                counts.path_srtt[old] = counts.path_srtt[old].saturating_sub(1);
            }
            if let Some(new) = srtt_bucket {
                counts.path_srtt[new] += 1;
            }
            window.srtt_bucket = srtt_bucket;
        }
        if tx_bps_bucket != window.tx_bps_bucket {
            if let Some(old) = window.tx_bps_bucket {
                counts.path_tx_bps[old] = counts.path_tx_bps[old].saturating_sub(1);
            }
            if let Some(new) = tx_bps_bucket {
                counts.path_tx_bps[new] += 1;
            }
            window.tx_bps_bucket = tx_bps_bucket;
        }
    }

    /// A session ended while its path carried data, `age` after it became active.
    pub(crate) fn record_session_ended_with_data_path(&self, age: std::time::Duration) {
        self.session_ended[QuicPathAge::bucket(age) as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A path reads `open` inbound streams at once. Counted per path and period as
    /// `TransportMetrics::record_inbound_concurrency` counts connections: a path
    /// is sampled once a period and counted once in each level its peak reaches,
    /// and a sample that is no new peak in a period already counted takes no lock.
    pub(crate) fn record_streams_open(&self, path: &mut StreamsOpenPeak, open: u32) {
        if path.epoch == self.streams_epoch.load(Ordering::Relaxed) && open <= path.peak {
            return;
        }
        let mut counts = self.streams.lock();
        let epoch = self.streams_epoch.load(Ordering::Relaxed);
        if path.epoch != epoch {
            path.epoch = epoch;
            path.peak = 0;
            counts.paths_sampled += 1;
        }
        if open <= path.peak {
            return;
        }
        for (count, &level) in counts.ge.iter_mut().zip(&QUIC_STREAMS_OPEN_THRESHOLDS) {
            if path.peak < level && level <= open {
                *count += 1;
            }
        }
        path.peak = open;
        counts.max = counts.max.max(open);
    }

    /// A path reset a send on this side, its `path_count`th
    /// (`recycle::SendAborts`, while the connection is open).
    pub(crate) fn record_send_reset(&self, path_count: u64) {
        self.send_resets.fetch_add(1, Ordering::Relaxed);
        // A path's count rises by one per reset, so it equals each level once.
        if let Some(i) = QUIC_SEND_RESET_LEVELS
            .iter()
            .position(|level| *level == path_count)
        {
            self.send_resets_reached[i].fetch_add(1, Ordering::Relaxed);
        }
        self.send_resets_path_max
            .fetch_max(path_count, Ordering::Relaxed);
    }

    /// A path's loss gate gave `delta` more verdicts, in the order of
    /// `loss_gate::Verdict::ALL`, since its last report: summed node-wide for
    /// the period and added to the path's window.
    pub(crate) fn record_gate(&self, window: &mut PathGateWindow, delta: [u64; GATE_VERDICTS]) {
        debug_assert!(
            std::ptr::eq(window.node, self),
            "a window of these counters"
        );
        for (i, n) in delta.into_iter().enumerate() {
            if n != 0 {
                self.gate_loss[i].fetch_add(n, Ordering::Relaxed);
                window.verdicts[i] = window.verdicts[i].saturating_add(n);
            }
        }
    }

    /// A path was up for `ms` more milliseconds (`recycle::PathReport`).
    pub(crate) fn record_path_time(&self, ms: u64) {
        self.path_ms.fetch_add(ms, Ordering::Relaxed);
    }

    /// The node's QUIC buffer budget, set when the endpoint is built.
    pub(crate) fn set_budget_bytes(&self, bytes: u64) {
        self.budget_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The acknowledgement watch closed a path, for the reason at `index` of
    /// [`STALL_CLOSE_LABELS`].
    pub(crate) fn record_stall_close(&self, index: usize) {
        if let Some(count) = self.stall_closes.get(index) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A rule that only observes would have closed a path, for the reason
    /// at `index` of [`STALL_CLOSE_LABELS`].
    pub(crate) fn record_stall_would_close(&self, index: usize) {
        if let Some(count) = self.stall_would_close.get(index) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A send's end was late (`path::LateEndCheck`).
    pub(crate) fn record_late_end(&self, kind: AckWaitKind) {
        self.late_end[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A send's end reached the proof point of the late-end check
    /// (`path::LateEndCheck`): enough later data was acknowledged that an
    /// honest peer would have acknowledged it. The denominator of
    /// `quic_late_end_<label>`.
    pub(crate) fn record_late_end_judged(&self, kind: AckWaitKind) {
        self.late_end_judged[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A send's wait for the acknowledgement of its end ran out.
    pub(crate) fn record_ack_wait_expired(&self, kind: AckWaitKind) {
        self.ack_wait_expired[kind as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A dark episode ended as `ended` says, on a path that `carried` data
    /// when it ended (counted in `quic_carried_dark_episode_<label>` as well).
    pub(crate) fn record_dark_episode(&self, ended: DarkEnd, carried: bool) {
        let bucket = match ended {
            DarkEnd::Recovered(nanos) => {
                let secs = nanos / 1_000_000_000;
                match DARK_RECOVERED_BOUNDS_SECS
                    .iter()
                    .position(|bound| secs < *bound)
                {
                    Some(i) => i,
                    None => DARK_RECOVERED_LATE,
                }
            }
            DarkEnd::Unrecovered => DARK_UNRECOVERED,
            DarkEnd::CapHeld => DARK_CAP_HELD,
            DarkEnd::Closed => DARK_CLOSED,
        };
        self.dark_episodes[bucket].fetch_add(1, Ordering::Relaxed);
        if carried {
            self.carried_dark_episodes[bucket].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// What the node's data paths are charged against the budget now.
    pub(crate) fn set_budget_charged(&self, bytes: u64) {
        self.budget_charged.store(bytes, Ordering::Relaxed);
        self.budget_charged_peak.fetch_max(bytes, Ordering::Relaxed);
    }

    /// What one evaluation did to a path's windows.
    pub(crate) fn record_window_change(
        &self,
        change: crate::transport::quic::budget::WindowChange,
    ) {
        use crate::transport::quic::budget::WindowChange;
        let index = match change {
            WindowChange::Grew => 0,
            WindowChange::Refused => 1,
            WindowChange::Shrank => 2,
        };
        self.window_changes[index].fetch_add(1, Ordering::Relaxed);
    }

    /// A connection's staged bytes reached `pct` percent of its staging cap.
    pub(crate) fn record_staged_level(&self, pct: u64) {
        self.staged_peak_pct.fetch_max(pct, Ordering::Relaxed);
    }

    /// The period's counts, reset when `reset`. The gauge is read, never reset.
    pub(crate) fn snapshot(&self, reset: bool) -> QuicDataSnapshot {
        let get = |a: &AtomicU64| {
            if reset {
                a.swap(0, Ordering::Relaxed)
            } else {
                a.load(Ordering::Relaxed)
            }
        };
        let loss = {
            let mut counts = self.loss.lock();
            let now = *counts;
            if reset {
                // The sums are of the period; the buckets are a gauge.
                *counts = LossCounts {
                    all: now.all.next_period(),
                    carrying: now.carrying.next_period(),
                    path_srtt: now.path_srtt,
                    path_tx_bps: now.path_tx_bps,
                    ..LossCounts::default()
                };
            }
            now
        };
        let streams = {
            let mut counts = self.streams.lock();
            if reset {
                self.streams_epoch.fetch_add(1, Ordering::Relaxed);
                std::mem::take(&mut *counts)
            } else {
                *counts
            }
        };
        QuicDataSnapshot {
            sends: self.sends.iter().flatten().map(get).collect(),
            events: self.events.iter().map(get).collect(),
            stops: self.stops.iter().map(get).collect(),
            inbound: self.inbound.iter().map(get).collect(),
            data_enabled: get(&self.data_enabled),
            data_paths: self.data_paths.load(Ordering::Relaxed),
            sends_opened: get(&self.sends_opened),
            open_uni_waited: get(&self.open_uni_waited),
            permit_takes: get(&self.permit_takes),
            permit_waited: get(&self.permit_waited),
            bytes_sent: get(&self.bytes_sent),
            bytes_received: get(&self.bytes_received),
            session_ended_with_data_path: self.session_ended.iter().map(get).collect(),
            sent_packets: loss.sent_packets,
            lost_packets: loss.lost_packets,
            lost_bytes: loss.lost_bytes,
            congestion_events: loss.congestion_events,
            loss_rate: loss.all.rates.to_vec(),
            ended_loss_rate: loss.all.ended.to_vec(),
            ended_below_floor: loss.all.ended_below_floor,
            carrying_sent_packets: loss.carrying_sent_packets,
            carrying_lost_packets: loss.carrying_lost_packets,
            carrying_loss_rate: loss.carrying.rates.to_vec(),
            carrying_ended_loss_rate: loss.carrying.ended.to_vec(),
            carrying_ended_below_floor: loss.carrying.ended_below_floor,
            path_srtt: loss.path_srtt.to_vec(),
            path_tx_bps: loss.path_tx_bps.to_vec(),
            streams_open_paths_sampled: streams.paths_sampled,
            streams_open_ge: streams.ge.to_vec(),
            streams_open_max: streams.max,
            send_resets: get(&self.send_resets),
            path_ms: get(&self.path_ms),
            send_resets_reached: self.send_resets_reached.iter().map(get).collect(),
            send_resets_path_max: get(&self.send_resets_path_max),
            budget_bytes: self.budget_bytes.load(Ordering::Relaxed),
            budget_charged: self.budget_charged.load(Ordering::Relaxed),
            // The peak of the period starts again from what is charged now.
            budget_charged_peak: if reset {
                self.budget_charged_peak.swap(
                    self.budget_charged.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                )
            } else {
                self.budget_charged_peak.load(Ordering::Relaxed)
            },
            window_grew: get(&self.window_changes[0]),
            window_grow_refused: get(&self.window_changes[1]),
            window_shrank: get(&self.window_changes[2]),
            staged_peak_pct: get(&self.staged_peak_pct),
            gate_loss: self.gate_loss.iter().map(get).collect(),
            gate_ended_queue: self.gate_ended_queue.iter().map(get).collect(),
            dark_episodes: self.dark_episodes.iter().map(get).collect(),
            carried_dark_episodes: self.carried_dark_episodes.iter().map(get).collect(),
            ramp_share_pct: self.share.lock().as_ref().map_or(0, ShareSource::percent),
            ack_wait_expired: self.ack_wait_expired.iter().map(get).collect(),
            late_end: self.late_end.iter().map(get).collect(),
            late_end_judged: self.late_end_judged.iter().map(get).collect(),
            stall_closes: self.stall_closes.iter().map(get).collect(),
            stall_would_close: self.stall_would_close.iter().map(get).collect(),
            pipe_source: self.pipe_source.iter().map(get).collect(),
            pipe_dropped: self.pipe_dropped.iter().map(get).collect(),
            short_dropped: self.short_dropped.iter().map(get).collect(),
        }
    }
}

/// The QUIC data counts of one period. Vectors, in the order of their label lists,
/// so a family can pass 32 entries; [`Self::keys`] names every value.
///
/// Fields added after a release carry `#[serde(default)]`, so JSON written
/// before them still decodes. That does not hold for bincode, which is
/// positional: the node's local event log cannot read a snapshot an older
/// release wrote, so after an update that adds a field the older records are
/// skipped once, with a WARN, as `TransportSnapshot::upload_json` notes.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(arbitrary::Arbitrary))]
pub struct QuicDataSnapshot {
    /// By `SendKind`, then `QuicSendResult`.
    pub sends: Vec<u64>,
    pub events: Vec<u64>,
    /// In the order of `stop_code::ALL`.
    pub stops: Vec<u64>,
    pub inbound: Vec<u64>,
    pub data_enabled: u64,
    pub data_paths: u64,
    pub sends_opened: u64,
    pub open_uni_waited: u64,
    pub permit_takes: u64,
    pub permit_waited: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// By `QuicPathAge`.
    pub session_ended_with_data_path: Vec<u64>,
    pub sent_packets: u64,
    pub lost_packets: u64,
    pub lost_bytes: u64,
    pub congestion_events: u64,
    /// Live paths by loss-rate bucket, in the order of `LOSS_RATE_LABELS`: a
    /// gauge, not reset.
    pub loss_rate: Vec<u32>,
    /// Paths that ended in the period, by their last loss-rate bucket.
    pub ended_loss_rate: Vec<u64>,
    /// Paths that ended in the period before sending
    /// [`LOSS_RATE_MIN_SENT`] packets, so in no bucket.
    pub ended_below_floor: u64,
    /// The period's packets sent, and declared lost, while their path carried
    /// data: a subset of `sent_packets` and `lost_packets`.
    #[serde(default)]
    pub carrying_sent_packets: u64,
    #[serde(default)]
    pub carrying_lost_packets: u64,
    /// Live paths that carry data, by their loss while carrying, in the order
    /// of `LOSS_RATE_LABELS`: a gauge, not reset.
    #[serde(default)]
    pub carrying_loss_rate: Vec<u32>,
    /// Paths that carried data and ended in the period, by their last carrying
    /// bucket.
    #[serde(default)]
    pub carrying_ended_loss_rate: Vec<u64>,
    /// Paths that carried data and ended in the period before sending
    /// [`LOSS_RATE_MIN_SENT`] packets while carrying.
    #[serde(default)]
    pub carrying_ended_below_floor: u64,
    /// Live paths by smoothed round-trip-time bucket, in the order of
    /// `PATH_SRTT_LABELS`: a gauge, not reset.
    #[serde(default)]
    pub path_srtt: Vec<u32>,
    /// Live paths by send-rate bucket over their last tick, in the order of
    /// `PATH_TX_BPS_LABELS`: a gauge, not reset.
    #[serde(default)]
    pub path_tx_bps: Vec<u32>,
    pub streams_open_paths_sampled: u32,
    /// By `QUIC_STREAMS_OPEN_THRESHOLDS`.
    pub streams_open_ge: Vec<u32>,
    pub streams_open_max: u32,
    pub send_resets: u64,
    pub path_ms: u64,
    /// By `QUIC_SEND_RESET_LEVELS`.
    pub send_resets_reached: Vec<u64>,
    pub send_resets_path_max: u64,
    /// A gauge, not reset.
    pub budget_bytes: u64,
    /// A gauge, not reset.
    pub budget_charged: u64,
    pub budget_charged_peak: u64,
    pub window_grew: u64,
    pub window_grow_refused: u64,
    pub window_shrank: u64,
    pub staged_peak_pct: u64,
    /// Congestion events by loss-gate verdict, by `loss_gate::Verdict::ALL`.
    /// Defaulted, so JSON from before the gate decodes. This does not help
    /// bincode: the local event log drops a snapshot written by an older
    /// release (see `upload_json`).
    #[serde(default)]
    pub gate_loss: Vec<u64>,
    /// Paths that ended in the period, by `GATE_QUEUE_LABELS`.
    #[serde(default)]
    pub gate_ended_queue: Vec<u64>,
    /// Dark episodes that ended in the period, by `DARK_EPISODE_LABELS`.
    #[serde(default)]
    pub dark_episodes: Vec<u64>,
    /// The dark episodes of paths that carried data when the episode ended, by
    /// `DARK_EPISODE_LABELS`: a subset of `dark_episodes`.
    #[serde(default)]
    pub carried_dark_episodes: Vec<u64>,
    /// The share of pairs this node sets its data bit for at the snapshot, in
    /// percent: a gauge.
    #[serde(default)]
    pub ramp_share_pct: u64,
    /// Ack waits that ran out in the period, by `ACK_WAIT_EXPIRED_LABELS`.
    #[serde(default)]
    pub ack_wait_expired: Vec<u64>,
    /// Paths the acknowledgement watch closed in the period, by
    /// `STALL_CLOSE_LABELS`.
    #[serde(default)]
    pub stall_closes: Vec<u64>,
    /// Times a rule that only observes would have closed a path, in the
    /// period, by `STALL_CLOSE_LABELS`.
    #[serde(default)]
    pub stall_would_close: Vec<u64>,
    /// Pipes that ended sent or dropped in the period, by what fed them, by
    /// `PIPE_SOURCE_LABELS`.
    #[serde(default)]
    pub pipe_source: Vec<u64>,
    /// Pipes dropped in the period, by `PIPE_DROP_LABELS`.
    #[serde(default)]
    pub pipe_dropped: Vec<u64>,
    /// Shorts dropped in the period, by `SHORT_DROP_LABELS`; sums to
    /// `quic_data_send_short_dropped`.
    #[serde(default)]
    pub short_dropped: Vec<u64>,
    /// Sends whose end was late in the period (`path::LateEndCheck`), by
    /// `ACK_WAIT_EXPIRED_LABELS`.
    #[serde(default)]
    pub late_end: Vec<u64>,
    /// Ends that reached the late-end proof point in the period, by
    /// `ACK_WAIT_EXPIRED_LABELS`: the denominator of `late_end`.
    #[serde(default)]
    pub late_end_judged: Vec<u64>,
}

impl QuicDataSnapshot {
    /// Every key with its value, in a fixed order. A value with no label (a
    /// vector longer than its list, which only a decoded foreign snapshot has) is
    /// left out.
    pub fn keys(&self) -> Vec<(String, u64)> {
        self.keys_by_family()
            .into_iter()
            .map(|(_, key, value)| (key, value))
            .collect()
    }

    /// [`Self::keys`], each with the prefix of the family it is in, or `None` for
    /// a key of its own. The prefixes the keys are built from, so a test can check
    /// them against every key.
    pub(crate) fn keys_by_family(&self) -> Vec<(Option<&'static str>, String, u64)> {
        // Destructured, so a new field cannot go unexported.
        let Self {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            sent_packets,
            lost_packets,
            lost_bytes,
            congestion_events,
            loss_rate,
            ended_loss_rate,
            ended_below_floor,
            carrying_sent_packets,
            carrying_lost_packets,
            carrying_loss_rate,
            carrying_ended_loss_rate,
            carrying_ended_below_floor,
            path_srtt,
            path_tx_bps,
            streams_open_paths_sampled,
            streams_open_ge,
            streams_open_max,
            send_resets,
            path_ms,
            send_resets_reached,
            send_resets_path_max,
            budget_bytes,
            budget_charged,
            budget_charged_peak,
            window_grew,
            window_grow_refused,
            window_shrank,
            staged_peak_pct,
            gate_loss,
            gate_ended_queue,
            dark_episodes,
            carried_dark_episodes,
            ramp_share_pct,
            ack_wait_expired,
            stall_closes,
            stall_would_close,
            pipe_source,
            pipe_dropped,
            short_dropped,
            late_end,
            late_end_judged,
        } = self;
        let mut keys = Vec::new();
        let mut family = |prefix: &'static str, labels: Vec<String>, values: Vec<u64>| {
            keys.extend(
                labels
                    .into_iter()
                    .zip(values)
                    .map(|(label, value)| (Some(prefix), format!("{prefix}{label}"), value)),
            );
        };
        family(
            "quic_data_send_",
            SendKind::ALL
                .iter()
                .flat_map(|kind| {
                    QuicSendResult::ALL
                        .iter()
                        .map(move |result| format!("{}_{}", kind.label(), result.label()))
                })
                .collect(),
            sends.clone(),
        );
        family(
            "quic_data_event_",
            QuicSendEvent::ALL
                .iter()
                .map(|e| e.label().to_string())
                .collect(),
            events.clone(),
        );
        family(
            "quic_data_stop_",
            stop_code::ALL
                .iter()
                .map(|code| stop_code::label(*code).to_string())
                .collect(),
            stops.clone(),
        );
        family(
            "quic_inbound_",
            QuicInboundOutcome::ALL
                .iter()
                .map(|o| o.label().to_string())
                .collect(),
            inbound.clone(),
        );
        family(
            "quic_session_ended_with_data_path_",
            QuicPathAge::ALL
                .iter()
                .map(|a| a.label().to_string())
                .collect(),
            session_ended_with_data_path.clone(),
        );
        family(
            LOSS_RATE_PREFIX,
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            loss_rate.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_ended_loss_rate_",
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            ended_loss_rate.clone(),
        );
        family(
            CARRYING_LOSS_RATE_PREFIX,
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            carrying_loss_rate.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_carrying_ended_loss_rate_",
            LOSS_RATE_LABELS.iter().map(|l| l.to_string()).collect(),
            carrying_ended_loss_rate.clone(),
        );
        family(
            PATH_SRTT_PREFIX,
            PATH_SRTT_LABELS.iter().map(|l| l.to_string()).collect(),
            path_srtt.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            PATH_TX_BPS_PREFIX,
            PATH_TX_BPS_LABELS.iter().map(|l| l.to_string()).collect(),
            path_tx_bps.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_streams_open_ge_",
            QUIC_STREAMS_OPEN_THRESHOLDS
                .iter()
                .map(|level| level.to_string())
                .collect(),
            streams_open_ge.iter().map(|n| u64::from(*n)).collect(),
        );
        family(
            "quic_send_resets_reached_",
            QUIC_SEND_RESET_LEVELS
                .iter()
                .map(|level| level.to_string())
                .collect(),
            send_resets_reached.clone(),
        );
        family(
            "quic_gate_loss_",
            VERDICT_LABELS.iter().map(|l| l.to_string()).collect(),
            gate_loss.clone(),
        );
        family(
            "quic_gate_ended_queue_",
            GATE_QUEUE_LABELS.iter().map(|l| l.to_string()).collect(),
            gate_ended_queue.clone(),
        );
        family(
            "quic_dark_episode_",
            DARK_EPISODE_LABELS.iter().map(|l| l.to_string()).collect(),
            dark_episodes.clone(),
        );
        family(
            "quic_carried_dark_episode_",
            DARK_EPISODE_LABELS.iter().map(|l| l.to_string()).collect(),
            carried_dark_episodes.clone(),
        );
        family(
            "quic_ack_wait_expired_",
            ACK_WAIT_EXPIRED_LABELS
                .iter()
                .map(|l| l.to_string())
                .collect(),
            ack_wait_expired.clone(),
        );
        family(
            "quic_stall_close_",
            STALL_CLOSE_LABELS.iter().map(|l| l.to_string()).collect(),
            stall_closes.clone(),
        );
        family(
            "quic_stall_would_close_",
            STALL_CLOSE_LABELS.iter().map(|l| l.to_string()).collect(),
            stall_would_close.clone(),
        );
        family(
            "quic_pipe_source_",
            PIPE_SOURCE_LABELS.iter().map(|l| l.to_string()).collect(),
            pipe_source.clone(),
        );
        family(
            "quic_pipe_dropped_",
            PIPE_DROP_LABELS.iter().map(|l| l.to_string()).collect(),
            pipe_dropped.clone(),
        );
        family(
            "quic_short_dropped_",
            SHORT_DROP_LABELS.iter().map(|l| l.to_string()).collect(),
            short_dropped.clone(),
        );
        family(
            "quic_late_end_",
            ACK_WAIT_EXPIRED_LABELS
                .iter()
                .map(|l| l.to_string())
                .collect(),
            late_end.clone(),
        );
        family(
            "quic_late_end_judged_",
            ACK_WAIT_EXPIRED_LABELS
                .iter()
                .map(|l| l.to_string())
                .collect(),
            late_end_judged.clone(),
        );
        for (key, value) in [
            ("quic_enabled_data_paths", *data_enabled),
            (LIVE_DATA_PATHS, *data_paths),
            ("quic_sends", *sends_opened),
            ("quic_open_uni_waited", *open_uni_waited),
            ("quic_streamed_sends", *permit_takes),
            ("quic_stream_permit_waited", *permit_waited),
            ("quic_bytes_sent", *bytes_sent),
            ("quic_bytes_received", *bytes_received),
            ("quic_sent_packets", *sent_packets),
            ("quic_lost_packets", *lost_packets),
            ("quic_lost_bytes", *lost_bytes),
            ("quic_congestion_events", *congestion_events),
            ("quic_ended_paths_below_floor", *ended_below_floor),
            // Not under `quic_carrying_`, which the two carrying families share.
            ("quic_carried_packets_sent", *carrying_sent_packets),
            ("quic_carried_packets_lost", *carrying_lost_packets),
            (
                "quic_carried_paths_ended_below_floor",
                *carrying_ended_below_floor,
            ),
            (
                "quic_streams_sampled_paths",
                u64::from(*streams_open_paths_sampled),
            ),
            ("quic_streams_peak_max", u64::from(*streams_open_max)),
            ("quic_send_resets", *send_resets),
            ("quic_path_ms", *path_ms),
            ("quic_send_resets_path_max", *send_resets_path_max),
            ("quic_budget_bytes", *budget_bytes),
            ("quic_budget_charged_bytes", *budget_charged),
            ("quic_budget_charged_peak_bytes", *budget_charged_peak),
            ("quic_window_grew", *window_grew),
            ("quic_window_grow_refused", *window_grow_refused),
            ("quic_window_shrank", *window_shrank),
            ("quic_staged_peak_pct", *staged_peak_pct),
            (RAMP_SHARE_PCT, *ramp_share_pct),
        ] {
            keys.push((None, key.to_string(), value));
        }
        keys
    }

    /// The keys the snapshot uploads: every count that is not 0, and the gauges
    /// (`quic_live_data_paths`, `quic_ramp_share_pct`, every loss-rate,
    /// round-trip-time and send-rate bucket) always, so a
    /// gauge of 0 is told apart from one not reported.
    pub fn upload_keys(&self) -> impl Iterator<Item = (String, u64)> {
        self.keys_by_family()
            .into_iter()
            .filter(|(family, key, value)| {
                *value != 0
                    || key == LIVE_DATA_PATHS
                    || key == RAMP_SHARE_PCT
                    || *family == Some(LOSS_RATE_PREFIX)
                    || *family == Some(CARRYING_LOSS_RATE_PREFIX)
                    || *family == Some(PATH_SRTT_PREFIX)
                    || *family == Some(PATH_TX_BPS_PREFIX)
            })
            .map(|(_, key, value)| (key, value))
    }

    /// Whether the period counted anything. The gauge alone is not activity, as
    /// `quic_paths_active` is not.
    pub(super) fn has_activity(&self) -> bool {
        let QuicDataSnapshot {
            sends,
            events,
            stops,
            inbound,
            data_enabled,
            data_paths: _,
            sends_opened,
            open_uni_waited,
            permit_takes,
            permit_waited,
            bytes_sent,
            bytes_received,
            session_ended_with_data_path,
            sent_packets,
            lost_packets,
            lost_bytes,
            congestion_events,
            loss_rate: _,
            ended_loss_rate,
            ended_below_floor,
            carrying_sent_packets,
            carrying_lost_packets,
            carrying_loss_rate: _,
            carrying_ended_loss_rate,
            carrying_ended_below_floor,
            path_srtt: _,
            path_tx_bps: _,
            streams_open_paths_sampled,
            streams_open_ge: _,
            streams_open_max: _,
            send_resets,
            path_ms,
            send_resets_reached,
            send_resets_path_max: _,
            budget_bytes: _,
            budget_charged: _,
            budget_charged_peak: _,
            window_grew,
            window_grow_refused,
            window_shrank,
            staged_peak_pct: _,
            gate_loss,
            gate_ended_queue,
            dark_episodes,
            carried_dark_episodes,
            ramp_share_pct: _,
            ack_wait_expired,
            stall_closes,
            stall_would_close,
            pipe_source,
            pipe_dropped,
            short_dropped,
            late_end,
            late_end_judged,
        } = self;
        let any = |values: &[u64]| values.iter().any(|n| *n != 0);
        any(sends)
            || any(events)
            || any(stops)
            || any(inbound)
            || any(session_ended_with_data_path)
            || any(ended_loss_rate)
            || any(send_resets_reached)
            || any(gate_loss)
            || any(gate_ended_queue)
            || any(dark_episodes)
            || any(carried_dark_episodes)
            || any(carrying_ended_loss_rate)
            || any(ack_wait_expired)
            || any(stall_closes)
            || any(stall_would_close)
            || any(pipe_source)
            || any(pipe_dropped)
            || any(short_dropped)
            || any(late_end)
            || any(late_end_judged)
            || [
                *data_enabled,
                *sends_opened,
                *open_uni_waited,
                *permit_takes,
                *permit_waited,
                *bytes_sent,
                *bytes_received,
                *sent_packets,
                *lost_packets,
                *lost_bytes,
                *congestion_events,
                *ended_below_floor,
                *carrying_sent_packets,
                *carrying_lost_packets,
                *carrying_ended_below_floor,
                u64::from(*streams_open_paths_sampled),
                *send_resets,
                *path_ms,
                *window_grew,
                *window_grow_refused,
                *window_shrank,
            ]
            .iter()
            .any(|n| *n != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each Short drop cause reaches `quic_short_dropped_<its label>`, in the
    /// order of the labels.
    #[test]
    fn each_short_drop_cause_reaches_its_key() {
        let counters = QuicDataCounters::new();
        assert!(
            counters
                .snapshot(true)
                .keys()
                .iter()
                .filter(|(k, _)| k.starts_with("quic_short_dropped_"))
                .all(|(_, n)| *n == 0),
            "nothing counted"
        );
        for (i, cause) in ShortDropCause::ALL.iter().enumerate() {
            for _ in 0..=i {
                counters.record_short_drop_cause(*cause);
            }
        }
        let keys: std::collections::BTreeMap<_, _> =
            counters.snapshot(true).keys().into_iter().collect();
        for (i, label) in SHORT_DROP_LABELS.iter().enumerate() {
            assert_eq!(keys[&format!("quic_short_dropped_{label}")], i as u64 + 1);
        }
    }

    /// Each family is counted where it is recorded, read back under its own keys
    /// with its own values, and reset by the snapshot, except the gauge.
    #[test]
    fn each_count_reaches_its_key_and_resets() {
        let counters = QuicDataCounters::new();
        for (i, kind) in SendKind::ALL.iter().enumerate() {
            for (j, result) in QuicSendResult::ALL.iter().enumerate() {
                for _ in 0..(i * 10 + j + 1) {
                    counters.record_send(*kind, *result);
                }
            }
        }
        counters.record_event(QuicSendEvent::StalledClosed);
        counters.record_stop(u64::from(stop_code::NOT_ACCEPTING_DATA));
        counters.record_stop(0xff);
        counters.record_inbound(QuicInboundOutcome::ShortDroppedFull);
        counters.record_data_enabled();
        counters.data_path_started();
        counters.data_path_started();
        counters.data_path_ended();
        counters.record_open(false);
        counters.record_open(true);
        counters.record_permit(true);
        counters.record_bytes(100, 200);
        counters.record_session_ended_with_data_path(std::time::Duration::from_secs(90));
        let mut path = StreamsOpenPeak::default();
        counters.record_streams_open(&mut path, 20);
        counters.record_streams_open(&mut path, 10);

        let snap = counters.snapshot(true);
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_data_send_short_sent"], 1);
        assert_eq!(keys["quic_data_send_large_short_maybe_delivered"], 12);
        assert_eq!(keys["quic_data_send_pipe_dropped"], 35);
        assert_eq!(keys["quic_data_event_stalled_closed"], 1);
        assert_eq!(keys["quic_data_stop_not_accepting_data"], 1);
        assert_eq!(keys["quic_inbound_short_dropped_full"], 1);
        assert_eq!(keys["quic_enabled_data_paths"], 1);
        assert_eq!(keys["quic_live_data_paths"], 1);
        assert_eq!(keys["quic_sends"], 2);
        assert_eq!(keys["quic_open_uni_waited"], 1);
        assert_eq!(keys["quic_streamed_sends"], 1);
        assert_eq!(keys["quic_stream_permit_waited"], 1);
        assert_eq!(keys["quic_bytes_sent"], 100);
        assert_eq!(keys["quic_bytes_received"], 200);
        assert_eq!(keys["quic_session_ended_with_data_path_lt_10m"], 1);
        assert_eq!(keys["quic_streams_sampled_paths"], 1);
        assert_eq!(keys["quic_streams_open_ge_16"], 1);
        assert_eq!(keys["quic_streams_open_ge_32"], 0);
        assert_eq!(keys["quic_streams_peak_max"], 20);
        assert!(snap.has_activity());

        let after = counters.snapshot(true);
        assert_eq!(after.data_paths, 1, "the gauge is not reset");
        assert!(!after.has_activity(), "a gauge alone is not activity");
        assert!(after.upload_keys().all(|(key, value)| {
            key == "quic_live_data_paths"
                || (key == "quic_ramp_share_pct" && value == 0)
                || ((key.starts_with("quic_loss_rate_")
                    || key.starts_with("quic_carrying_loss_rate_")
                    || key.starts_with("quic_srtt_")
                    || key.starts_with("quic_tx_bps_"))
                    && value == 0)
        }));
        // A new period counts the path again.
        counters.record_streams_open(&mut path, 5);
        assert_eq!(counters.snapshot(true).streams_open_paths_sampled, 1);
    }

    /// The gate's verdicts reach their keys and reset; a path that ends after
    /// enough verdicts is counted once in the bucket of its share passed as a
    /// queue, and one with too few in none.
    #[test]
    fn gate_verdicts_reach_their_keys_and_ended_paths_their_bucket() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        // queue, share, persistent, ecn, no_sample, ignored.
        let mut cubic_like = PathGateWindow::new(counters);
        counters.record_gate(&mut cubic_like, [12, 0, 0, 0, 0, 4]);
        counters.record_gate(&mut cubic_like, [3, 1, 0, 0, 0, 0]);
        let mut gated = PathGateWindow::new(counters);
        counters.record_gate(&mut gated, [1, 0, 1, 2, 1, 30]);
        let mut middle = PathGateWindow::new(counters);
        counters.record_gate(&mut middle, [5, 0, 0, 0, 0, 15]);
        let mut short = PathGateWindow::new(counters);
        counters.record_gate(&mut short, [19, 0, 0, 0, 0, 0]);
        let snap = counters.snapshot(true);
        assert!(snap.has_activity());
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_gate_loss_queue"], 40);
        assert_eq!(keys["quic_gate_loss_share"], 1);
        assert_eq!(keys["quic_gate_loss_persistent"], 1);
        assert_eq!(keys["quic_gate_loss_ecn"], 2);
        assert_eq!(keys["quic_gate_loss_no_sample"], 1);
        assert_eq!(keys["quic_gate_loss_ignored"], 49);
        assert!(
            snap.gate_ended_queue.iter().all(|n| *n == 0),
            "no path ended"
        );

        drop((cubic_like, gated, middle, short));
        let ended = counters.snapshot(true);
        let keys: std::collections::BTreeMap<_, _> = ended.keys().into_iter().collect();
        // 1 of 35 (3%), 5 of 20 (25%), 15 of 20 (75%); 19 verdicts is too few.
        assert_eq!(keys["quic_gate_ended_queue_lt_10pct"], 1);
        assert_eq!(keys["quic_gate_ended_queue_lt_50pct"], 1);
        assert_eq!(keys["quic_gate_ended_queue_ge_50pct"], 1);
        assert_eq!(keys["quic_gate_loss_queue"], 0, "reset by the snapshot");
        assert!(ended.has_activity(), "an ended path alone is activity");
        assert!(!counters.snapshot(true).has_activity());
    }

    /// A dark episode is counted in the bucket of how it ended, and the counts
    /// are activity and reset with the snapshot.
    #[test]
    fn dark_episodes_are_counted_by_how_they_ended() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        const S: u64 = 1_000_000_000;
        for ended in [
            DarkEnd::Recovered(2 * S),
            DarkEnd::Recovered(9 * S),
            DarkEnd::Recovered(30 * S - 1),
            DarkEnd::Recovered(30 * S),
            DarkEnd::Recovered(200 * S),
            DarkEnd::Unrecovered,
            DarkEnd::CapHeld,
            DarkEnd::Closed,
        ] {
            counters.record_dark_episode(ended, false);
        }
        let snap = counters.snapshot(true);
        assert!(snap.has_activity());
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_dark_episode_recovered_lt_5s"], 1);
        assert!(!keys.contains_key("quic_dark_episode_recovered_lt_2s"));
        assert_eq!(keys["quic_dark_episode_recovered_lt_10s"], 1);
        assert_eq!(keys["quic_dark_episode_recovered_lt_30s"], 1);
        assert_eq!(keys["quic_dark_episode_recovered_lt_120s"], 1);
        assert_eq!(keys["quic_dark_episode_recovered_ge_120s"], 1);
        assert_eq!(keys["quic_dark_episode_unrecovered"], 1);
        assert_eq!(keys["quic_dark_episode_cap_held"], 1);
        assert_eq!(keys["quic_dark_episode_closed_at_window"], 1);
        assert!(!counters.snapshot(true).has_activity(), "reset");
    }

    #[test]
    fn the_gate_bucket_bounds() {
        assert_eq!(gate_queue_bucket(0, 20), 0);
        assert_eq!(gate_queue_bucket(1, 20), 0);
        assert_eq!(gate_queue_bucket(2, 20), 1, "10% is not under 10%");
        assert_eq!(gate_queue_bucket(9, 20), 1);
        assert_eq!(gate_queue_bucket(10, 20), 2, "50% is not under 50%");
        assert_eq!(gate_queue_bucket(20, 20), 2);
    }

    /// The gauge goes out in every snapshot, 0 included, so 0 is told apart from
    /// not reported; a count of 0 does not.
    #[test]
    fn the_gauges_are_uploaded_at_zero_and_counts_are_not() {
        let counters = QuicDataCounters::new();
        let uploaded: Vec<_> = counters.snapshot(true).upload_keys().collect();
        assert_eq!(
            uploaded.len(),
            2 * LOSS_RATE_LABELS.len() + PATH_SRTT_LABELS.len() + PATH_TX_BPS_LABELS.len() + 2,
            "the gauges and nothing else: {uploaded:?}"
        );
        assert!(uploaded.iter().all(|(key, value)| {
            *value == 0
                && (key == "quic_live_data_paths"
                    || key == "quic_ramp_share_pct"
                    || key.starts_with("quic_loss_rate_")
                    || key.starts_with("quic_carrying_loss_rate_")
                    || key.starts_with("quic_srtt_")
                    || key.starts_with("quic_tx_bps_"))
        }));
        counters.data_path_started();
        counters.record_open(false);
        let uploaded: std::collections::BTreeMap<_, _> =
            counters.snapshot(true).upload_keys().collect();
        assert_eq!(
            uploaded.len(),
            2 * LOSS_RATE_LABELS.len() + PATH_SRTT_LABELS.len() + PATH_TX_BPS_LABELS.len() + 3,
            "{uploaded:?}"
        );
        assert_eq!(uploaded["quic_live_data_paths"], 1);
        assert_eq!(uploaded["quic_sends"], 1);
    }

    /// The share gauge reads the node's share at each snapshot: a fixed share
    /// as set, a ramp's share at the snapshot's time, 0 with no share.
    #[test]
    fn the_ramp_share_gauge_reads_the_share_at_the_snapshot() {
        use crate::transport::quic::liveness::{QuicAvailability, QuicAvailabilityWatch};
        use crate::transport::quic::upgrade::slice::ramp_percent;
        const HOUR: u64 = 3600;
        let counters = QuicDataCounters::new();
        let share = |counters: &QuicDataCounters| {
            counters
                .snapshot(true)
                .upload_keys()
                .find(|(key, _)| key == RAMP_SHARE_PCT)
                .map(|(_, value)| value)
        };
        assert_eq!(share(&counters), Some(0), "no share: 0, still uploaded");
        // The share is read on the source's own clock, at each snapshot.
        let t0 = 1_800_000_000;
        let clock = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(t0));
        let (endpoint, availability) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let source = |share: DataShare| {
            let clock = clock.clone();
            Some(ShareSource {
                share,
                availability: availability.clone(),
                wall: Box::new(move || {
                    std::time::UNIX_EPOCH
                        + std::time::Duration::from_secs(clock.load(Ordering::Relaxed))
                }),
            })
        };
        counters.set_share(source(DataShare::Fixed(40)));
        assert_eq!(share(&counters), Some(40));
        counters.set_share(source(DataShare::Ramp {
            start_unix_secs: t0,
        }));
        for hours in [0, 1, 7, 13, 30] {
            clock.store(t0 + hours * HOUR, Ordering::Relaxed);
            let want = u64::from(ramp_percent(std::time::Duration::from_secs(hours * HOUR)));
            assert_eq!(share(&counters), Some(want), "{hours} h");
        }
        // An endpoint that is down cannot carry data: 0, until it is up again.
        for down in [QuicAvailability::Down, QuicAvailability::Released] {
            endpoint.send_replace(down);
            assert_eq!(share(&counters), Some(0), "{down:?}");
        }
        endpoint.send_replace(QuicAvailability::Up);
        assert_eq!(share(&counters), Some(100));
        counters.set_share(None);
        assert_eq!(share(&counters), Some(0));
    }

    /// The keys with the family each is in, from `keys_by_family`; every key the
    /// rest of the snapshot has, from its serde fields; and the prefix of each of
    /// its fields that is an array, which goes out as a key per label
    /// (`quic_upgrade_<outcome>`).
    #[allow(clippy::type_complexity)]
    fn all_keys() -> (
        Vec<(Option<&'static str>, String)>,
        Vec<String>,
        Vec<String>,
    ) {
        let ours = QuicDataCounters::new()
            .snapshot(false)
            .keys_by_family()
            .into_iter()
            .map(|(family, key, _)| (family, key))
            .collect();
        let json = serde_json::to_value(crate::transport::metrics::TransportSnapshot::default())
            .expect("serializes");
        let map = json.as_object().expect("an object");
        let others = map
            .keys()
            .filter(|key| *key != "quic_data")
            .cloned()
            .collect();
        let arrays = map
            .iter()
            .filter(|(_, value)| value.is_array())
            .map(|(key, _)| format!("{key}_"))
            .collect();
        (ours, others, arrays)
    }

    /// A key's prefixes that end at a `_` and have two words or more: `quic_data_`
    /// and `quic_data_send_` of `quic_data_send_short_sent`.
    fn stems(key: &str) -> impl Iterator<Item = &str> {
        key.match_indices('_')
            .map(move |(at, _)| &key[..=at])
            .skip(1)
    }

    /// Every key is distinct, and none is under a family it is not in: not under
    /// another family's prefix, nor under the prefix sibling families share
    /// (`quic_data_` of the send, event and stop families), nor under one of the
    /// snapshot's arrays (`quic_upgrade_`) or a prefix two of its other keys share
    /// (`quic_paths_`). The families come from the keys and the other keys from
    /// the snapshot, so a new key or family is checked without editing a list.
    #[test]
    fn no_key_is_under_another_family() {
        let (ours, others, arrays) = all_keys();
        let keys: Vec<&str> = ours
            .iter()
            .map(|(_, key)| key.as_str())
            .chain(others.iter().map(String::as_str))
            .collect();
        let distinct: std::collections::BTreeSet<_> = keys.iter().collect();
        assert_eq!(distinct.len(), keys.len(), "a key is used twice");

        let families: std::collections::BTreeSet<&str> =
            ours.iter().filter_map(|(family, _)| *family).collect();
        // A family's prefix, and each prefix it shares with a sibling, with the
        // families under it.
        let mut claimed: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
        for family in &families {
            claimed.entry(family).or_default().push(family);
            for parent in stems(family).filter(|stem| stem != family) {
                if families
                    .iter()
                    .any(|other| other != family && other.starts_with(parent))
                {
                    claimed.entry(parent).or_default().push(family);
                }
            }
        }
        for (key_family, key) in &ours {
            for (prefix, owners) in &claimed {
                if key.starts_with(prefix) {
                    assert!(
                        key_family.is_some_and(|f| owners.contains(&f)),
                        "{key} is under {prefix}, of {owners:?}"
                    );
                }
            }
        }
        for key in &others {
            for prefix in claimed.keys() {
                assert!(
                    !key.starts_with(prefix),
                    "the snapshot's {key} is under {prefix}"
                );
            }
        }
        // The prefixes two of the snapshot's own keys share.
        let mut shared: std::collections::BTreeMap<&str, u32> = Default::default();
        for key in &others {
            for stem in stems(key) {
                *shared.entry(stem).or_default() += 1;
            }
        }
        let taken = shared
            .iter()
            .filter(|(_, n)| **n >= 2)
            .map(|(stem, _)| *stem)
            .chain(arrays.iter().map(String::as_str));
        for stem in taken {
            for (_, key) in &ours {
                assert!(
                    !key.starts_with(stem),
                    "{key} is under the snapshot's {stem}"
                );
            }
        }
    }

    /// A path's peak counts once in each level it reaches, a level at its exact
    /// value included, and a new period counts it again from 0.
    #[test]
    fn a_peak_counts_once_per_level_and_period() {
        let counters = QuicDataCounters::new();
        let mut rising = StreamsOpenPeak::default();
        for open in [20, 40, 40] {
            counters.record_streams_open(&mut rising, open);
        }
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(
            snap.streams_open_ge,
            vec![1, 1, 1, 0],
            "8, 16, 32 and not 48"
        );
        assert_eq!(snap.streams_open_max, 40);

        let mut at_16 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_16, 16);
        let mut at_48 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_48, 48);
        let mut at_7 = StreamsOpenPeak::default();
        counters.record_streams_open(&mut at_7, 7);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 3);
        assert_eq!(snap.streams_open_ge, vec![2, 2, 1, 1]);
        assert_eq!(snap.streams_open_max, 48);

        // After the reset the rising path starts again: 9 is a new peak.
        counters.record_streams_open(&mut rising, 9);
        let snap = counters.snapshot(true);
        assert_eq!(snap.streams_open_paths_sampled, 1);
        assert_eq!(snap.streams_open_ge, vec![1, 0, 0, 0]);
        assert_eq!(snap.streams_open_max, 9);
    }

    fn loss(sent: u64, lost: u64) -> crate::transport::quic::recycle::PathLoss {
        crate::transport::quic::recycle::PathLoss {
            sent_packets: sent,
            lost_packets: lost,
            lost_bytes: lost * 1200,
            congestion_events: u64::from(lost > 0),
        }
    }

    /// Each bucket's bounds: a rate at a bound is in the bucket above it, and
    /// more losses than sends (losses of packets sent earlier) is `ge_20pct`.
    #[test]
    fn loss_rate_buckets_have_these_bounds() {
        for (lost, bucket) in [
            (0, "zero"),
            (1, "lt_0_5pct"),
            (4, "lt_0_5pct"),
            (5, "lt_1pct"),
            (9, "lt_1pct"),
            (10, "lt_2pct"),
            (19, "lt_2pct"),
            (20, "lt_5pct"),
            (49, "lt_5pct"),
            (50, "lt_10pct"),
            (99, "lt_10pct"),
            (100, "lt_20pct"),
            (199, "lt_20pct"),
            (200, "ge_20pct"),
            (5000, "ge_20pct"),
        ] {
            assert_eq!(
                LOSS_RATE_LABELS[loss_rate_bucket(lost, 1000)],
                bucket,
                "{lost}"
            );
        }
    }

    /// The sums take every report and are of the period. A live path is in no
    /// bucket until it has sent 200 packets since it started, then in exactly
    /// one, moved as its rate changes, across periods; a path that ends leaves
    /// its bucket.
    #[test]
    fn a_path_is_in_one_loss_bucket_once_it_has_sent_enough() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut path = PathLossWindow::new(counters);
        let mut quiet = PathLossWindow::new(counters);
        counters.record_path_loss(&mut path, loss(150, 0), false);
        counters.record_path_loss(&mut quiet, loss(199, 3), false);
        assert_eq!(counters.snapshot(false).loss_rate.iter().sum::<u32>(), 0);
        counters.record_path_loss(&mut path, loss(50, 0), false);
        let snap = counters.snapshot(false);
        assert_eq!(
            snap.loss_rate,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
            "200 sent, none lost"
        );
        // 3 lost of 400: under 1%.
        counters.record_path_loss(&mut path, loss(200, 3), false);
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate, vec![0, 0, 1, 0, 0, 0, 0, 0]);
        assert_eq!(snap.sent_packets, 599);
        assert_eq!(snap.lost_packets, 6);
        assert_eq!(snap.lost_bytes, 6 * 1200);
        assert_eq!(snap.congestion_events, 2);
        // The next period keeps the path's window: 50 more lost of 100 more
        // sent is 53 of 500, under 20%, and the sums start again.
        counters.record_path_loss(&mut path, loss(100, 50), false);
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate, vec![0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(snap.lost_packets, 50);
        // With no report, the gauge stays and the period is not active.
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 1, "a gauge");
        assert!(!snap.has_activity(), "a gauge alone is not activity");
        drop(path);
        drop(quiet);
        assert_eq!(
            counters.snapshot(true).loss_rate.iter().sum::<u32>(),
            0,
            "it ended"
        );
    }

    #[test]
    fn rtt_buckets_have_these_bounds() {
        use std::time::Duration;
        for (ms, label) in [
            (0, "lt_10ms"),
            (9, "lt_10ms"),
            (10, "lt_25ms"),
            (24, "lt_25ms"),
            (25, "lt_50ms"),
            (50, "lt_100ms"),
            (100, "lt_200ms"),
            (200, "lt_400ms"),
            (399, "lt_400ms"),
            (400, "ge_400ms"),
            (60_000, "ge_400ms"),
        ] {
            assert_eq!(
                PATH_SRTT_LABELS[path_srtt_bucket(Duration::from_millis(ms))],
                label,
                "{ms} ms"
            );
        }
    }

    #[test]
    fn send_rate_buckets_have_these_bounds() {
        // Bytes in one second are bits per second divided by 8.
        for (bytes, ms, label) in [
            (0, 2000, "lt_10kbps"),
            (1249, 1000, "lt_10kbps"),
            (1250, 1000, "lt_100kbps"),
            (12_500, 1000, "lt_1mbps"),
            (125_000, 1000, "lt_10mbps"),
            (1_250_000, 1000, "lt_100mbps"),
            (12_500_000, 1000, "ge_100mbps"),
            // The same rate over another interval is the same bucket.
            (2_500_000, 2000, "lt_100mbps"),
            (u64::MAX, 1, "ge_100mbps"),
        ] {
            assert_eq!(
                PATH_TX_BPS_LABELS[path_tx_bps_bucket(bytes, ms)],
                label,
                "{bytes} bytes in {ms} ms"
            );
        }
    }

    /// A live path is in one bucket of each family, moved by its ticks and
    /// taken out when it ends; both gauges survive a snapshot's reset, and
    /// a tick of 0 ms leaves the rate where it was.
    #[test]
    fn a_path_is_in_one_rtt_and_one_rate_bucket_while_it_lives() {
        use std::time::Duration;
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut a = PathLossWindow::new(counters);
        let mut b = PathLossWindow::new(counters);
        assert_eq!(counters.snapshot(false).path_srtt.iter().sum::<u32>(), 0);
        counters.record_path_rates(&mut a, Duration::from_millis(30), 0, 2000, true);
        counters.record_path_rates(&mut b, Duration::from_millis(500), 250_000, 2000, true);
        let snap = counters.snapshot(true);
        assert_eq!(snap.path_srtt, vec![0, 0, 1, 0, 0, 0, 1]);
        assert_eq!(snap.path_tx_bps, vec![1, 0, 0, 1, 0, 0]);
        // `a` speeds up and its RTT falls; `b`'s tick has no time in it.
        counters.record_path_rates(&mut a, Duration::from_millis(8), 2_500_000, 2000, true);
        counters.record_path_rates(&mut b, Duration::from_millis(500), 9, 0, true);
        let snap = counters.snapshot(true);
        assert_eq!(snap.path_srtt, vec![1, 0, 0, 0, 0, 0, 1]);
        assert_eq!(snap.path_tx_bps, vec![0, 0, 0, 1, 1, 0]);
        assert!(!snap.has_activity(), "gauges alone are not activity");
        drop(a);
        let snap = counters.snapshot(true);
        assert_eq!(snap.path_srtt, vec![0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(snap.path_tx_bps, vec![0, 0, 0, 1, 0, 0]);
        drop(b);
        let snap = counters.snapshot(true);
        assert_eq!(snap.path_srtt.iter().sum::<u32>(), 0);
        assert_eq!(snap.path_tx_bps.iter().sum::<u32>(), 0);
    }

    /// Only a path that carries data is in the gauges: an idle shadow path is
    /// in neither, and a path that stops carrying leaves its buckets.
    #[test]
    fn a_path_that_does_not_carry_data_is_in_neither_gauge() {
        use std::time::Duration;
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut shadow = PathLossWindow::new(counters);
        let mut data = PathLossWindow::new(counters);
        counters.record_path_rates(&mut shadow, Duration::from_millis(30), 100, 2000, false);
        let snap = counters.snapshot(false);
        assert_eq!(snap.path_srtt.iter().sum::<u32>(), 0);
        assert_eq!(snap.path_tx_bps.iter().sum::<u32>(), 0);
        counters.record_path_rates(&mut data, Duration::from_millis(30), 0, 2000, true);
        let snap = counters.snapshot(false);
        assert_eq!(snap.path_srtt.iter().sum::<u32>(), 1);
        assert_eq!(snap.path_tx_bps.iter().sum::<u32>(), 1);
        // The peer refused data: the path leaves both.
        counters.record_path_rates(&mut data, Duration::from_millis(30), 0, 2000, false);
        let snap = counters.snapshot(false);
        assert_eq!(snap.path_srtt.iter().sum::<u32>(), 0);
        assert_eq!(snap.path_tx_bps.iter().sum::<u32>(), 0);
    }

    /// Every bucket of both gauges goes out in every snapshot, 0 included.
    #[test]
    fn the_rtt_and_rate_gauges_are_uploaded_at_zero() {
        let counters = QuicDataCounters::new();
        let uploaded: std::collections::BTreeMap<String, u64> =
            counters.snapshot(true).upload_keys().collect();
        for label in PATH_SRTT_LABELS {
            assert_eq!(uploaded.get(&format!("quic_srtt_{label}")), Some(&0));
        }
        for label in PATH_TX_BPS_LABELS {
            assert_eq!(uploaded.get(&format!("quic_tx_bps_{label}")), Some(&0));
        }
    }

    /// A path that ends leaves the gauge and is counted once among the period's
    /// ended paths by the bucket it was in; one that never reached 200 packets
    /// is counted as ended below the floor. The ended counts are of the period.
    #[test]
    fn a_path_that_ends_is_counted_by_its_last_bucket() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut lossy = PathLossWindow::new(counters);
        let mut short = PathLossWindow::new(counters);
        counters.record_path_loss(&mut lossy, loss(300, 90), false);
        counters.record_path_loss(&mut short, loss(150, 0), false);
        assert_eq!(
            counters.snapshot(true).loss_rate,
            vec![0, 0, 0, 0, 0, 0, 0, 1]
        );
        drop(lossy);
        drop(short);
        let snap = counters.snapshot(true);
        assert_eq!(snap.loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.ended_loss_rate, vec![0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(snap.ended_below_floor, 1, "the short path");
        assert!(snap.has_activity(), "an ended path is activity");
        let next = counters.snapshot(true);
        assert_eq!(next.ended_loss_rate.iter().sum::<u64>(), 0);
        assert_eq!(next.ended_below_floor, 0);
        // Ended below the floor is activity on its own.
        drop(PathLossWindow::new(counters));
        assert!(counters.snapshot(true).has_activity());
    }

    /// Every loss-rate gauge is uploaded each period, an emptied bucket as 0,
    /// while a count of 0 (the ended family) is not.
    #[test]
    fn the_loss_rate_gauges_are_uploaded_at_zero() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut path = PathLossWindow::new(counters);
        counters.record_path_loss(&mut path, loss(300, 90), false);
        drop(path);
        let uploaded: std::collections::HashMap<String, u64> =
            counters.snapshot(true).upload_keys().collect();
        for label in LOSS_RATE_LABELS {
            assert_eq!(
                uploaded.get(&format!("quic_loss_rate_{label}")),
                Some(&0),
                "{label} is uploaded at 0"
            );
        }
        assert_eq!(uploaded.get("quic_ended_loss_rate_ge_20pct"), Some(&1));
        assert!(!uploaded.contains_key("quic_ended_loss_rate_zero"));
        assert!(
            !uploaded.contains_key("quic_ended_paths_below_floor"),
            "a count of 0 is not uploaded"
        );
        // A path that ends below the floor is uploaded under its key.
        drop(PathLossWindow::new(counters));
        let uploaded: std::collections::HashMap<String, u64> =
            counters.snapshot(true).upload_keys().collect();
        assert_eq!(uploaded.get("quic_ended_paths_below_floor"), Some(&1));
    }

    /// Resets reach their keys: the total, each level a path's count passes,
    /// and the highest count; path time sums. All reset by the snapshot.
    #[test]
    fn send_resets_and_path_time_reach_their_keys_and_reset() {
        let counters = QuicDataCounters::new();
        // One path to 600 resets, another to 3.
        for count in 1..=600 {
            counters.record_send_reset(count);
        }
        for count in 1..=3 {
            counters.record_send_reset(count);
        }
        counters.record_path_time(2_000);
        counters.record_path_time(1_500);
        let snap = counters.snapshot(true);
        assert!(snap.has_activity());
        let keys: std::collections::BTreeMap<_, _> = snap.keys().into_iter().collect();
        assert_eq!(keys["quic_send_resets"], 603);
        assert_eq!(keys["quic_path_ms"], 3_500);
        assert_eq!(keys["quic_send_resets_reached_64"], 1);
        assert_eq!(keys["quic_send_resets_reached_512"], 1);
        assert_eq!(keys["quic_send_resets_reached_2048"], 0);
        assert_eq!(keys["quic_send_resets_reached_4096"], 0);
        assert_eq!(keys["quic_send_resets_path_max"], 600);

        let after = counters.snapshot(true);
        assert!(!after.has_activity());
        assert!(after.keys().iter().all(|(_, value)| *value == 0));
    }

    /// The reset and path-time counts are activity on their own, so a period with
    /// only them is uploaded.
    #[test]
    fn resets_or_path_time_alone_are_activity() {
        let counters = QuicDataCounters::new();
        counters.record_send_reset(1);
        assert!(counters.snapshot(true).has_activity(), "a reset");
        counters.record_path_time(1);
        assert!(counters.snapshot(true).has_activity(), "path time");
        let reached = QuicDataSnapshot {
            send_resets_reached: vec![0, 1, 0, 0],
            ..QuicDataSnapshot::default()
        };
        assert!(reached.has_activity(), "a level reached");
        assert!(!counters.snapshot(true).has_activity());
    }

    /// A path's carrying-data count starts at the first report that finds it
    /// carrying, apart from its count over its whole life: 300 packets without
    /// data and no loss, then 300 with data and 30 lost, is 30 of 600 (5%) in the
    /// all-paths buckets and 30 of 300 (10%) in the carrying ones, and only the
    /// second 300 are in the carrying sums. A path that stops carrying keeps its
    /// carrying bucket until it ends; a path that never carried is in no
    /// carrying count. Both families count an ended path once.
    #[test]
    fn a_path_counts_its_carrying_packets_from_the_flip() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut flips = PathLossWindow::new(counters);
        let mut shadow = PathLossWindow::new(counters);
        counters.record_path_loss(&mut flips, loss(300, 0), false);
        counters.record_path_loss(&mut shadow, loss(300, 0), false);
        let snap = counters.snapshot(true);
        assert_eq!(snap.carrying_loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.carrying_sent_packets, 0);
        counters.record_path_loss(&mut flips, loss(300, 30), true);
        let snap = counters.snapshot(true);
        // 30 of 600 is 5%: `lt_10pct`; 30 of 300 is 10%: `lt_20pct`.
        assert_eq!(snap.loss_rate, vec![1, 0, 0, 0, 0, 1, 0, 0]);
        assert_eq!(snap.carrying_loss_rate, vec![0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(
            (snap.sent_packets, snap.lost_packets),
            (300, 30),
            "all paths' sums of the period"
        );
        assert_eq!(
            (snap.carrying_sent_packets, snap.carrying_lost_packets),
            (300, 30)
        );
        // It stops carrying: no more carrying packets, the bucket stays.
        counters.record_path_loss(&mut flips, loss(100, 0), false);
        let snap = counters.snapshot(true);
        assert_eq!(snap.carrying_sent_packets, 0);
        assert_eq!(snap.carrying_loss_rate, vec![0, 0, 0, 0, 0, 0, 1, 0]);
        drop(flips);
        drop(shadow);
        let snap = counters.snapshot(true);
        assert_eq!(snap.carrying_loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.carrying_ended_loss_rate, vec![0, 0, 0, 0, 0, 0, 1, 0]);
        assert_eq!(
            snap.carrying_ended_below_floor, 0,
            "the shadow path never carried"
        );
        assert_eq!(snap.ended_loss_rate.iter().sum::<u64>(), 2);
        assert!(
            snap.has_activity(),
            "a carrying path that ended is activity"
        );
        // Ended below the carrying floor: it carried, but too few packets.
        let mut brief = PathLossWindow::new(counters);
        counters.record_path_loss(&mut brief, loss(50, 0), true);
        drop(brief);
        let snap = counters.snapshot(true);
        assert_eq!(snap.carrying_ended_below_floor, 1);
        assert_eq!(snap.ended_below_floor, 1);
    }

    /// A path that never carries data adds nothing to any carrying count, live
    /// or ended, however lossy it is; one whose bit comes on mid-life adds only
    /// what it sends from then (its carrying window starts at the flip).
    #[test]
    fn a_path_that_never_carries_adds_nothing_to_carrying() {
        let counters: &'static QuicDataCounters = Box::leak(Box::new(QuicDataCounters::new()));
        let mut shadow = PathLossWindow::new(counters);
        counters.record_path_loss(&mut shadow, loss(400, 100), false);
        counters.record_path_loss(&mut shadow, loss(400, 100), false);
        drop(shadow);
        let snap = counters.snapshot(true);
        assert_eq!(snap.sent_packets, 800);
        assert_eq!(
            (snap.carrying_sent_packets, snap.carrying_lost_packets),
            (0, 0)
        );
        assert_eq!(snap.carrying_loss_rate.iter().sum::<u32>(), 0);
        assert_eq!(snap.carrying_ended_loss_rate.iter().sum::<u64>(), 0);
        assert_eq!(snap.carrying_ended_below_floor, 0);
        assert_eq!(snap.ended_loss_rate.iter().sum::<u64>(), 1, "it ended");

        let mut flips = PathLossWindow::new(counters);
        counters.record_path_loss(&mut flips, loss(400, 100), false);
        counters.record_path_loss(&mut flips, loss(250, 0), true);
        let snap = counters.snapshot(true);
        assert_eq!(
            (snap.carrying_sent_packets, snap.carrying_lost_packets),
            (250, 0),
            "only what it sent after the flip"
        );
        assert_eq!(
            snap.carrying_loss_rate,
            vec![1, 0, 0, 0, 0, 0, 0, 0],
            "0 of 250 since the flip, not 100 of 650"
        );
        drop(flips);
    }

    /// The carrying gauges are uploaded at 0 like the all-paths ones, and the
    /// carrying counts only when not 0.
    #[test]
    fn the_carrying_gauges_are_uploaded_at_zero() {
        let uploaded: std::collections::HashMap<String, u64> = QuicDataCounters::new()
            .snapshot(true)
            .upload_keys()
            .collect();
        for label in LOSS_RATE_LABELS {
            assert_eq!(
                uploaded.get(&format!("quic_carrying_loss_rate_{label}")),
                Some(&0),
                "{label}"
            );
        }
        for key in [
            "quic_carried_packets_sent",
            "quic_carried_packets_lost",
            "quic_carried_paths_ended_below_floor",
            "quic_carrying_ended_loss_rate_zero",
        ] {
            assert!(!uploaded.contains_key(key), "{key} is a count");
        }
    }

    /// The snapshot round-trips through bincode, as the event log stores it.
    #[test]
    fn snapshot_round_trips_through_bincode() {
        let counters = QuicDataCounters::new();
        counters.record_send(SendKind::Stream, QuicSendResult::FailedUdp);
        counters.record_inbound(QuicInboundOutcome::StreamRefusedBudget);
        let snap = counters.snapshot(true);
        let back: QuicDataSnapshot =
            bincode::deserialize(&bincode::serialize(&snap).unwrap()).unwrap();
        assert_eq!(back, snap);
    }
}
