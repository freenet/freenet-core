//! Opt-in, local-only recorder of the router's raw inputs, for offline
//! evaluation of routing predictors against real traffic (#4485).
//!
//! # Why this exists
//!
//! Every question about which failure predictor is better — the legacy blend,
//! the residual correction, or a design not yet written — is ultimately a
//! question about real traffic, and a synthetic harness can only answer it for
//! the structure it was built to contain. The router's inputs are small: one
//! [`RouteEvent`](super::RouteEvent) per observed outcome. Recording that
//! stream with timestamps lets any candidate predictor be replayed
//! prequentially against the traffic a real node saw, without redeploying a
//! node per candidate.
//!
//! Alongside each outcome it records what every existing failure layer
//! forecast *before* ingesting it, so the recording also carries the in-process
//! prequential scores as a cross-check on any replay.
//!
//! # What it is and is not
//!
//! - Its `route` lines are a **prediction-accuracy** dataset: the outcome for
//!   the peer that was actually tried. The optional `decision` lines (below) add
//!   the candidate set each routing decision chose from, which supports
//!   **ranking** evaluation over those candidates, but still not "what if a
//!   different peer had been picked" (see *Candidate sets*).
//! - It covers the router's `add_event` stream only. The separate CONNECT
//!   forward-acceptance estimator is out of scope.
//! - Events are tagged `originator` or `relay`: relay hops record outcomes under
//!   their own conventions (see `record_relay_route_event`, and
//!   `operations::route_attempt` for how failures of either origin are
//!   labelled since #5657), and replays may need
//!   to treat the two populations differently. `originator` means the node
//!   started the operation, which includes sub-operations it starts while
//!   serving someone else, not only its own clients' requests.
//! - A run may append after an earlier run's `truncated` line if that run
//!   stopped with room to spare (a large record did not fit); split on `start`.
//! - `t_ms` is absolute. The predictor's time feature is relative to process
//!   start, so a replay re-bases on the `start` line that opens each run.
//!
//! Peer attributes (connection age, protocol version, gateway status, transfer
//! volume, routing health) are recorded as periodic snapshots rather than per
//! event, because they live behind connection-manager locks that must not be
//! taken under the router's write lock. They join to events offline on `peer`
//! and time. Their distinctive value is population coverage — they include
//! connected peers that were never routed to and so never appear as events. The
//! routing counts are all-contract, originator-only aggregates; per-contract
//! density comes from the event stream itself.
//!
//! # Candidate sets (`decision` lines)
//!
//! With `FREENET_ROUTING_DATASET_CANDIDATES` set as well as a recording path,
//! each routing decision whose outcome this node records as a route event (GET,
//! PUT and SUBSCRIBE, at the originator's loopback relay and at relays) writes
//! one `"kind":"decision"` line: every candidate the router scored (the
//! `consider_n_closest_peers` distance window, in distance order), BOTH models'
//! estimates for each — the legacy stack as routing would act on it with the
//! hierarchical flag off, and the hierarchical estimator as routing would act on
//! it with the flag on (legacy fallback for any stage it cannot yet estimate,
//! flagged per stage in `hierarchical_stages`) — each candidate's rank under
//! each model, and which candidates the router actually returned
//! (`selected_position`, 0 = first choice). `acting_model` says which of the two
//! routed. The record is captured inside the decision from the values the router
//! sorted, not reconstructed afterwards; ranks use the router's own cost
//! ordering ([`cost_order_key`], stable over distance order).
//!
//! **Which selections log.** Every caller of the ring's selection functions
//! says whether its selection is a routing decision ([`DecisionLog`]). NOT
//! logged: driver-side pre-selections the loopback relay then re-decides (GET
//! and PUT client drivers, sub-op GETs), `first_hop_candidate` admission
//! probes, diagnostics, PUT probe forwards (no route event), all UPDATE routing
//! (UPDATE feeds the router nothing, so no UPDATE line could ever join), and
//! CONNECT peer selection. A selection that returns no peer writes nothing.
//!
//! **Sampling and pacing.** The switch takes a SAMPLING RATE: `1`/`true`/`yes`
//! /`on` capture every decision, a fraction such as `0.05` captures that share
//! (drawn per decision with `GlobalRng`, so setting a fraction perturbs a
//! seeded simulation's RNG stream; unset draws nothing), anything else is off.
//! Captures are also PACED so the sample spreads over the run rather than
//! spending the budget in the cold-router hours after a start: by time `t` into
//! a run, captured and uncaptured decision lines may have used at most the
//! budget times `t` over [`DATASET_CANDIDATES_PACE_HOURS_ENV`] (default
//! [`DEFAULT_CANDIDATES_PACE_HOURS`], `0` disables pacing), plus
//! [`PACE_BURST_BYTES`]; a decision over that allowance is written uncaptured
//! with reason `paced`. The `start` line's `t_ms` gives each decision's time
//! since run start, and `prior_failure_events` how warm the router was. At most
//! [`MAX_RECORDED_CANDIDATES`] candidates are written per decision; beyond that
//! every selected candidate and then the acting model's best are kept,
//! `candidates_omitted` counts the rest, and ranks stay those over the full
//! window.
//!
//! **Uncaptured decisions and bypasses.** A logged decision that is not
//! captured, and a route chosen WITHOUT ring selection, writes a ~120-byte
//! `"kind":"decision_uncaptured"` line — op, contract location, the selected
//! peers, and `reason`: `sampled_out`, `paced`, `distance_based` (too little
//! history to predict), or a bypass: `pinned_first_hop` (a GET retry pinned by
//! the driver, #5660), `bootstrap_gateway` (empty-ring fallback, #4361),
//! `directed_first_hop` or `any_connection_fallback` (SUBSCRIBE). Its only job
//! is to stop that route's outcome joining an older captured decision.
//!
//! **Decision lines cannot exhaust the file.** They have their OWN byte budget
//! ([`DATASET_CANDIDATES_MAX_BYTES_ENV`], default
//! [`DEFAULT_CANDIDATES_MAX_BYTES`]) and, across ALL runs appending to one file,
//! may only occupy the first half of it: each run's budget is at most half the
//! usable file minus everything already in it, so the second half is left to
//! route and peer lines however often the node restarts (by induction, the sum
//! of every run's decision bytes is at most half). Running out of budget, or
//! finding the file itself full, writes one `"kind":"decisions_truncated"` line
//! (with its `cause`) and stops ONLY decision capture for the rest of the run:
//! route recording, and the hierarchical estimator that is computed while the
//! recorder is recording, carry on. They do still share the file and the
//! 8192-slot channel with route records: decisions shorten the route runway by
//! at most half the file, and hold at most [`MAX_QUEUED_DECISIONS`] channel
//! slots, so under a stalled disk route records can find the channel full
//! somewhat sooner (those drops are counted as route drops, as always).
//! Decision records that never reach the file (queue bound, full channel, or
//! still queued when the writer stops) are counted apart from route drops, and
//! each change of that count is written as `"kind":"decisions_dropped"`.
//!
//! **Cost.** Measured in a release build (25-candidate window): a captured
//! decision costs about +10% under the router READ lock while legacy routes
//! (about 356 to 391 µs) but about 10x while the hierarchical estimator routes
//! (about 40 to 400 µs), because the legacy stack's per-candidate Renegade
//! queries then run only for the log. That is why the switch samples: at rate
//! `r` the added read-lock time averages `r` times that (at `0.05`, about +2 µs
//! and +18 µs per decision respectively), and a single captured decision holds
//! the lock no longer than every legacy-routed decision already does. Building
//! a record takes about 6 µs after the lock is released and serialising it
//! about 23 µs on the writer thread; a line is about 16 KB. An uncaptured
//! decision adds nothing under the lock: its line is about 0.3 µs to build and
//! serialise.
//!
//! **Joining a decision to its outcome.** No identifier is threaded through the
//! operation, so the join is by value, within one run (split on `start`): a
//! `route` line belongs to the most recent earlier `decision` or
//! `decision_uncaptured` line with the same `op`, the same `contract_location`
//! (both are `Location::as_f64` of the same contract, so equal in the JSON), and
//! a selected candidate whose `peer` equals the route line's `peer`. Discard the
//! route line when that most recent line is uncaptured, when a
//! `decisions_dropped` line lies between it and the route line (the decision
//! that really routed may be the one dropped), or when it comes after a
//! `decisions_truncated` line. Decision lines carry no originator/relay role,
//! while route lines do.
//!
//! The join is BIASED, in a known direction. Two decisions for the same op and
//! contract on one node that select the same peer are indistinguishable, and the
//! later one wins. The longer an outcome takes to arrive, the likelier another
//! such decision lands in between, so joined outcomes under-represent SLOW
//! outcomes and FAILURES (timeouts take seconds, successes milliseconds), most
//! on hot contracts that relays route to the same best peer, and most for the
//! first attempt of a GET that is then retried to the same peer. To measure it:
//! call a join ambiguous when any other decision-kind line for the same op,
//! contract and peer lies between the decision and the route line, and report
//! the ambiguous fraction alongside any result. Decisions with no joined
//! outcome are expected (an ambiguous `NotFound` is not trained, see
//! `operations::route_attempt`; a relay that terminates a PUT routes nowhere),
//! and they are not a random sample.
//!
//! **What it can and cannot measure.** It lets a replay ask, over the
//! candidates the CURRENT model chose among, whether another model would have
//! ranked the peer that succeeded above the one that failed, and how the two
//! models' orderings differ. It has no exploration: outcomes exist only for peers
//! the acting model selected, so how an unselected candidate would have fared is
//! never observed, and any "model B would have done better" estimate over
//! unselected peers is extrapolation, not measurement. The candidate window is
//! the router's distance cut, so peers outside it are invisible to both models.
//!
//! # Off by default, and local
//!
//! Nothing is recorded unless the operator sets `FREENET_ROUTING_DATASET` to a
//! file path. The file is written locally and never transmitted. It is not
//! anonymised: peers are keyed by a hash of their public key (linkable by anyone
//! who holds the key), and ring locations are derived from masked IP addresses.
//! Treat it like a node log.
//!
//! Give each node process its own path. The cap is enforced per process, and
//! two writers appending to one file can interleave lines longer than the write
//! buffer — so two gateways on one host must not share the variable's value.
//!
//! # Never blocks the caller
//!
//! Records are handed to a dedicated writer thread through a bounded channel
//! with `try_send`; the caller — which holds the router's write lock — never
//! waits on disk. A full channel drops the record and counts it, and the count
//! is written INTO the file (`"kind":"dropped"`), so an incomplete recording
//! says so itself rather than reading as a quiet period. The byte cap works the
//! same way: a record that would cross it is replaced by a final
//! `"kind":"truncated"` line and recording stops. A write error cannot be
//! recorded in the file it failed to write, so it is logged with its cause.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{
    Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError, sync_channel,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::ring::PeerKeyLocation;
use crate::transport::TransportPublicKey;

/// Environment variable naming the file to record into. Unset → no recording.
pub(crate) const DATASET_PATH_ENV: &str = "FREENET_ROUTING_DATASET";
/// Environment variable overriding [`DEFAULT_MAX_BYTES`], in plain bytes.
pub(crate) const DATASET_MAX_BYTES_ENV: &str = "FREENET_ROUTING_DATASET_MAX_BYTES";

/// Environment variable that adds a `decision` line per routing decision: an
/// affirmative value or a sampling fraction in (0, 1]. Off otherwise, and inert
/// without [`DATASET_PATH_ENV`].
pub(crate) const DATASET_CANDIDATES_ENV: &str = "FREENET_ROUTING_DATASET_CANDIDATES";

/// Environment variable overriding [`DEFAULT_CANDIDATES_MAX_BYTES`].
pub(crate) const DATASET_CANDIDATES_MAX_BYTES_ENV: &str =
    "FREENET_ROUTING_DATASET_CANDIDATES_MAX_BYTES";

/// Default decision-line budget per run, before the half-of-the-file clamp.
pub(crate) const DEFAULT_CANDIDATES_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Environment variable overriding [`DEFAULT_CANDIDATES_PACE_HOURS`].
pub(crate) const DATASET_CANDIDATES_PACE_HOURS_ENV: &str =
    "FREENET_ROUTING_DATASET_CANDIDATES_PACE_HOURS";

/// Hours over which a run's decision budget is released: a week, the length of
/// a routing soak.
pub(crate) const DEFAULT_CANDIDATES_PACE_HOURS: f64 = 168.0;

/// Decision bytes a run may write before pacing allows any, so a run starts
/// with a useful sample (about 500 captured decisions) rather than none.
pub(crate) const PACE_BURST_BYTES: u64 = 8 * 1024 * 1024;

/// Most candidates written for one decision. Above the default window (25), so
/// it only binds when an operator widens `consider_n_closest_peers`.
pub(crate) const MAX_RECORDED_CANDIDATES: usize = 32;

/// Most decision records waiting for the writer at once. A decision line is
/// several kilobytes, so the shared channel capacity alone would let a stalled
/// disk hold tens of megabytes of them in memory.
pub(crate) const MAX_QUEUED_DECISIONS: usize = 1024;

/// Default size cap, so a forgotten setting cannot fill a disk.
pub(crate) const DEFAULT_MAX_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// Channel capacity. Route records are a few hundred bytes; a peer snapshot is
/// larger (one entry per connection) but arrives once a minute.
const CHANNEL_CAPACITY: usize = 8192;

/// How long the writer waits for a record before re-checking for drops.
const IDLE_WAKE: Duration = Duration::from_secs(5);

/// Records written between flushes when the channel never drains. Flushing
/// whenever the channel empties bounds loss on a crash to what was in flight,
/// even on a node busy enough that it is never idle.
const MAX_RECORDS_PER_FLUSH: usize = 1024;

/// Bytes at the top of the cap kept free for the truncation marker. Records
/// stop at `max_bytes - MARKER_RESERVE`; the marker may use the reserve but
/// never the byte beyond it, so the file never exceeds `max_bytes` however many
/// times the node restarts onto it.
const MARKER_RESERVE: u64 = 512;

/// The single clock every record is stamped with.
///
/// Host wall clock, deliberately not `TimeSource`: the predictor this data
/// exists to replay derives its own time feature from the host wall clock
/// (`routing_predictor::wall_clock_hours`), so a replay needs records on that
/// same clock. Route events and peer snapshots MUST share this function, or they
/// stop joining the moment either side's clock is changed.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// Stable 64-bit identity for a peer: FNV-1a over the full 32-byte public key.
///
/// Stable across builds and processes (unlike `DefaultHasher`), so events and
/// peer snapshots from different runs join on the same value.
pub(crate) fn peer_hash(peer: &PeerKeyLocation) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in peer.pub_key().as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Which code path observed the outcome.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RouteSource {
    /// The node that started the operation: `Ring::routing_finished`, and
    /// since #5657 also the originator's per-attempt labels and hop-credited
    /// successes (`Ring::record_route_event_router_only`).
    Originator,
    /// A relay hop observing its downstream peer (`record_relay_route_event`).
    Relay,
}

/// What every failure layer forecast for an event, before ingesting it.
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) struct FailureForecasts {
    /// The global isotonic distance curve.
    pub global: f64,
    /// The global curve with the per-peer EWMA adjustment.
    pub adjusted: f64,
    /// The legacy fixed-weight blend of `adjusted` with Renegade.
    pub blended: f64,
    /// The residual correction composed on the global curve.
    pub corrected: f64,
    /// Shrinkage applied to the correction, when one was formed.
    pub lambda: Option<f64>,
    /// Effective evidence behind the correction, when one was formed.
    pub n_eff: Option<f64>,
    /// The hierarchical empirical-Bayes estimator (#4485), once it has a curve.
    pub hierarchical: Option<f64>,
    /// FLOORED AT 1 ms: every `log_response_time_*` field is `ln(max(t, 0.001))`,
    /// while routing acts on the unfloored value and `time_to_response_start_s`
    /// is recorded raw. Floor `time_to_response_start_s` the same way before any
    /// log-scale scoring against these, or a 0 s outcome becomes `-inf`.
    ///
    /// `ln(seconds)` to response start the legacy stack would act on (including
    /// the residual correction when that flag is on), forecast for every event
    /// whether or not it turns out to be timed, so timing can be scored offline
    /// on the timed subset. `None` without a timing estimate, and recorded only
    /// while the hierarchical estimator is computed.
    pub log_response_time_legacy: Option<f64>,
    /// The same forecast from the hierarchical estimator: `ln E[T]`, the value
    /// routing would act on, not the log-scale location.
    pub log_response_time_hierarchical: Option<f64>,
    /// `ln(bytes/s)` of the transfer speed the legacy stack would act on.
    pub log_transfer_speed_legacy: Option<f64>,
    /// `ln(bytes/s)` of the hierarchical estimator's effective speed (so that
    /// `bytes / speed` is its expected transfer time).
    pub log_transfer_speed_hierarchical: Option<f64>,
}

/// One observed routing outcome.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct RouteRecord {
    pub t_ms: u64,
    pub source: RouteSource,
    pub peer: String,
    pub peer_location: Option<f64>,
    pub contract_location: f64,
    /// `None` when the peer's location is unknown. (The router itself falls
    /// back to 0.5 in that case; the recording does not pretend it knew.)
    pub distance: Option<f64>,
    pub op: Option<&'static str>,
    /// `success`, `success_untimed` or `failure`.
    pub outcome: &'static str,
    pub time_to_response_start_s: Option<f64>,
    pub payload_bytes: Option<usize>,
    pub payload_transfer_s: Option<f64>,
    /// Failure events the global curve had absorbed before this one — how
    /// warmed-up the forecasts below were.
    pub prior_failure_events: usize,
    /// `None` until the estimators can produce a forecast at all.
    pub forecasts: Option<FailureForecasts>,
}

/// Attributes of one connected peer at snapshot time.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct PeerAttributes {
    pub peer: String,
    pub location: Option<f64>,
    pub connected_s: f64,
    /// `None` when the configured gateway list was unavailable.
    pub is_configured_gateway: Option<bool>,
    pub version: Option<String>,
    pub routing_successes: Option<u64>,
    pub routing_failures: Option<u64>,
    /// Transport counters, which are LRU-bounded per address and so can reset
    /// for a peer that has been evicted from tracking.
    pub bytes_sent: Option<u64>,
    pub bytes_received: Option<u64>,
}

/// Which estimator stack routing acted on for a decision.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RoutingModel {
    Legacy,
    Hierarchical,
}

/// One model's estimate for one candidate, in the units routing acts on
/// (unfloored; `expected_total_time` is the cost the router sorts by).
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub(crate) struct ModelEstimate {
    pub failure_probability: f64,
    pub time_to_response_start_s: f64,
    pub transfer_speed_bps: f64,
    pub expected_total_time: f64,
}

/// Which stages the hierarchical estimator supplied itself for a candidate;
/// a `false` stage in its [`ModelEstimate`] is the legacy fallback.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub(crate) struct HierarchicalStages {
    pub failure: bool,
    pub response_time: bool,
    pub transfer_speed: bool,
}

/// The router's sort key for a candidate's cost: its expected total time, or
/// last when it has no prediction. The router's sort and the recorded ranks
/// both use this, so the ranks cannot drift from the ordering routing applied.
pub(crate) fn cost_order_key(expected_total_time: Option<f64>) -> f64 {
    expected_total_time.unwrap_or(f64::MAX)
}

/// One candidate as the router scored it, captured inside the decision.
#[derive(Debug, Clone)]
pub(crate) struct CapturedCandidate<'a> {
    pub peer: &'a PeerKeyLocation,
    pub legacy: Option<ModelEstimate>,
    pub hierarchical: Option<ModelEstimate>,
    pub hierarchical_stages: HierarchicalStages,
    /// Position in the list the router returned, if it was returned.
    pub selected_position: Option<usize>,
}

/// Everything a [`DecisionRecord`] is built from, filled in by the router while
/// it decides. It borrows the candidates rather than cloning them, and the
/// record is assembled from it only after the router lock is released.
#[derive(Debug, Clone)]
pub(crate) struct DecisionCapture<'a> {
    pub contract_location: crate::ring::Location,
    pub acting_model: RoutingModel,
    /// Whether any candidate had no prediction under the acting model, so the
    /// router ranked it last.
    pub prediction_fallback: bool,
    pub k: usize,
    /// Peers offered to the decision before the distance window was applied.
    pub candidates_available: usize,
    pub prior_failure_events: usize,
    /// Every candidate the router scored, in distance order.
    pub candidates: Vec<CapturedCandidate<'a>>,
}

/// One candidate of a recorded decision.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct CandidateRecord {
    pub peer: String,
    pub peer_location: Option<f64>,
    /// `None` when the peer's location is unknown (the router uses 0.5).
    pub distance: Option<f64>,
    /// 0 = closest of the scored window.
    pub distance_rank: usize,
    /// Position in the router's returned list, `None` if not returned.
    pub selected_position: Option<usize>,
    /// Rank under each model's cost over the WHOLE scored window, 0 = best.
    pub rank_legacy: usize,
    pub rank_hierarchical: usize,
    pub legacy: Option<ModelEstimate>,
    pub hierarchical: Option<ModelEstimate>,
    pub hierarchical_stages: HierarchicalStages,
}

/// One prediction-based routing decision and its candidate set.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct DecisionRecord {
    pub t_ms: u64,
    pub op: &'static str,
    pub contract_location: f64,
    pub acting_model: RoutingModel,
    pub prediction_fallback: bool,
    pub k: usize,
    pub candidates_available: usize,
    /// Size of the scored window.
    pub candidates_considered: usize,
    /// Scored candidates not written because of [`MAX_RECORDED_CANDIDATES`].
    pub candidates_omitted: usize,
    pub prior_failure_events: usize,
    /// Rank under each model of the router's first choice.
    pub chosen_rank_legacy: Option<usize>,
    pub chosen_rank_hierarchical: Option<usize>,
    /// In distance order.
    pub candidates: Vec<CandidateRecord>,
}

/// Whether a ring selection is a routing decision to log, and for which op.
/// Every caller of `Ring::k_closest_potentially_hosting` and
/// `Ring::closest_potentially_hosting` states it, so a new call site cannot
/// silently add unjoinable lines or silently log nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecisionLog {
    /// The node routes by this selection and records the outcome as a route
    /// event for this op.
    Joinable(crate::node::network_status::OpType),
    /// A pre-selection, probe or diagnostic, or a route whose outcome is never
    /// recorded: never logged.
    Unlogged,
}

/// Why a decision line carries no candidate set.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UncapturedReason {
    /// Not drawn at the sampling rate.
    SampledOut,
    /// Drawn, but over the run's paced decision allowance.
    Paced,
    /// The router had too little history to predict, so ranked by distance.
    DistanceBased,
    /// A GET attempt pinned to a peer by its retry driver (#5660).
    PinnedFirstHop,
    /// An empty ring routed via a configured gateway (#4361).
    BootstrapGateway,
    /// A SUBSCRIBE whose first hop the caller named.
    DirectedFirstHop,
    /// A SUBSCRIBE that fell back to any connected peer.
    AnyConnectionFallback,
}

/// Why decision capture stopped for the run.
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DecisionsStopCause {
    /// The run's decision budget is spent.
    Budget,
    /// The file itself has no room for the line.
    FileFull,
}

/// A routing decision without a candidate set: enough to stop its outcome
/// joining an older captured decision.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub(crate) struct UncapturedDecision {
    pub t_ms: u64,
    pub op: &'static str,
    pub contract_location: f64,
    pub reason: UncapturedReason,
    /// Peer hashes in the order the router returned them.
    pub selected: Vec<String>,
}

/// Candidate logging for one routing decision, decided BEFORE the router lock
/// is taken: whether to capture it at all is `capture`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CandidateLog<'a> {
    recorder: &'a RoutingDataset,
    /// `true` to capture; otherwise the decision is written uncaptured with
    /// `skipped`.
    pub capture: bool,
    skipped: UncapturedReason,
}

impl CandidateLog<'_> {
    /// Write the decision, after the router lock is released: the full record
    /// when the router captured it, otherwise an uncaptured line. A selection
    /// that returned no peer writes nothing: there is no outcome to join.
    pub(crate) fn record(
        &self,
        op: crate::node::network_status::OpType,
        contract_location: crate::ring::Location,
        selected: &[&PeerKeyLocation],
        distance_based: bool,
        capture: Option<DecisionCapture<'_>>,
    ) {
        if selected.is_empty() {
            return;
        }
        let t_ms = now_ms();
        match capture {
            Some(capture) => self.recorder.record_decision(capture.into_record(op, t_ms)),
            None => self.recorder.record_uncaptured(UncapturedDecision {
                t_ms,
                op: op.as_str(),
                contract_location: contract_location.as_f64(),
                reason: if distance_based {
                    UncapturedReason::DistanceBased
                } else {
                    self.skipped
                },
                selected: selected.iter().map(|peer| peer_hash(peer)).collect(),
            }),
        }
    }
}

/// Decision-line bookkeeping, kept apart from the recorder's own counters so
/// that nothing about decisions can stop, or be mistaken for a gap in, route
/// recording.
#[derive(Debug, Default)]
struct DecisionState {
    /// Decision records sent and not yet taken by the writer.
    queued: AtomicUsize,
    /// Set once capture stops for the run: recording does not.
    stopped: AtomicBool,
    /// Decision records that never reached the file.
    dropped: AtomicU64,
    /// This run's decision budget, published by the writer once it knows the
    /// file's size. `u64::MAX` until then, so pacing allows the burst rather
    /// than refusing every decision made before the writer has started; the
    /// writer enforces the real budget either way.
    limit: AtomicU64,
    /// Decision bytes this run has written.
    written: AtomicU64,
}

/// Decision bytes a run may have used `elapsed_ms` into it: the budget released
/// linearly over `pace_ms`, plus a burst. `pace_ms == 0` releases it all.
pub(crate) fn paced_allowance(limit: u64, elapsed_ms: u64, pace_ms: u64) -> u64 {
    if pace_ms == 0 || elapsed_ms >= pace_ms {
        return limit;
    }
    let released = (limit as f64 * (elapsed_ms as f64 / pace_ms as f64)) as u64;
    released.saturating_add(PACE_BURST_BYTES).min(limit)
}

/// Rank of every candidate under one model: a stable sort of distance order by
/// [`cost_order_key`], the router's own sort.
fn ranks_by(
    candidates: &[CapturedCandidate<'_>],
    estimate: impl Fn(&CapturedCandidate<'_>) -> Option<ModelEstimate>,
) -> Vec<usize> {
    let key =
        |index: usize| cost_order_key(estimate(&candidates[index]).map(|e| e.expected_total_time));
    let mut order: Vec<usize> = (0..candidates.len()).collect();
    order.sort_by(|&a, &b| key(a).total_cmp(&key(b)));
    let mut ranks = vec![0; candidates.len()];
    for (rank, index) in order.into_iter().enumerate() {
        ranks[index] = rank;
    }
    ranks
}

impl DecisionCapture<'_> {
    /// Build the record. Runs outside the router lock.
    pub(crate) fn into_record(
        self,
        op: crate::node::network_status::OpType,
        t_ms: u64,
    ) -> DecisionRecord {
        let rank_legacy = ranks_by(&self.candidates, |c| c.legacy);
        let rank_hierarchical = ranks_by(&self.candidates, |c| c.hierarchical);
        let acting_rank = match self.acting_model {
            RoutingModel::Legacy => &rank_legacy,
            RoutingModel::Hierarchical => &rank_hierarchical,
        };
        let considered = self.candidates.len();
        let chosen = self
            .candidates
            .iter()
            .position(|c| c.selected_position == Some(0));

        // Over the cap: keep every selected candidate, then the acting model's
        // best, and write them back in distance order.
        let mut keep: Vec<usize> = (0..considered).collect();
        if considered > MAX_RECORDED_CANDIDATES {
            keep.sort_by_key(|&i| {
                (
                    self.candidates[i].selected_position.is_none(),
                    acting_rank[i],
                )
            });
            keep.truncate(MAX_RECORDED_CANDIDATES);
            keep.sort_unstable();
        }
        let candidates: Vec<CandidateRecord> = keep
            .iter()
            .map(|&i| {
                let captured = &self.candidates[i];
                let location = captured.peer.location();
                CandidateRecord {
                    peer: peer_hash(captured.peer),
                    peer_location: location.map(|l| l.as_f64()),
                    distance: location.map(|l| self.contract_location.distance(l).as_f64()),
                    distance_rank: i,
                    selected_position: captured.selected_position,
                    rank_legacy: rank_legacy[i],
                    rank_hierarchical: rank_hierarchical[i],
                    legacy: captured.legacy,
                    hierarchical: captured.hierarchical,
                    hierarchical_stages: captured.hierarchical_stages,
                }
            })
            .collect();
        DecisionRecord {
            t_ms,
            op: op.as_str(),
            contract_location: self.contract_location.as_f64(),
            acting_model: self.acting_model,
            prediction_fallback: self.prediction_fallback,
            k: self.k,
            candidates_available: self.candidates_available,
            candidates_considered: considered,
            candidates_omitted: considered - candidates.len(),
            prior_failure_events: self.prior_failure_events,
            chosen_rank_legacy: chosen.map(|i| rank_legacy[i]),
            chosen_rank_hierarchical: chosen.map(|i| rank_hierarchical[i]),
            candidates,
        }
    }
}

/// The inputs a peer snapshot is assembled from, gathered by the caller so
/// that assembly itself takes no locks and can be tested directly.
pub(crate) struct PeerSnapshotInputs<'a> {
    /// Each connected peer with how long it has been connected, in seconds.
    pub connections: &'a [(PeerKeyLocation, f64)],
    pub gateways: Option<&'a [TransportPublicKey]>,
    pub versions: &'a HashMap<SocketAddr, (u8, u8, u16)>,
    /// `(successes, failures)` per address.
    pub health: &'a HashMap<SocketAddr, (u64, u64)>,
    /// `(sent, received)` per address.
    pub transfer: &'a HashMap<SocketAddr, (u64, u64)>,
}

/// Assemble peer attributes. Every per-address join is a keyed lookup, so no
/// field can be attributed to the wrong peer by an ordering assumption.
pub(crate) fn peer_attributes(inputs: &PeerSnapshotInputs<'_>) -> Vec<PeerAttributes> {
    inputs
        .connections
        .iter()
        .map(|(peer, connected_s)| {
            let addr = peer.socket_addr();
            let lookup =
                |map: &HashMap<SocketAddr, (u64, u64)>| addr.and_then(|a| map.get(&a).copied());
            let health = lookup(inputs.health);
            let transfer = lookup(inputs.transfer);
            PeerAttributes {
                peer: peer_hash(peer),
                location: peer.location().map(|location| location.as_f64()),
                connected_s: *connected_s,
                is_configured_gateway: inputs
                    .gateways
                    .map(|gateways| gateways.contains(peer.pub_key())),
                version: addr
                    .and_then(|a| inputs.versions.get(&a))
                    .map(|(major, minor, patch)| format!("{major}.{minor}.{patch}")),
                routing_successes: health.map(|(successes, _)| successes),
                routing_failures: health.map(|(_, failures)| failures),
                bytes_sent: transfer.map(|(sent, _)| sent),
                bytes_received: transfer.map(|(_, received)| received),
            }
        })
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Line<'a> {
    Start {
        t_ms: u64,
        version: &'static str,
    },
    Route(&'a RouteRecord),
    Decision(&'a DecisionRecord),
    DecisionUncaptured(&'a UncapturedDecision),
    DecisionsDropped {
        t_ms: u64,
        total: u64,
    },
    DecisionsTruncated {
        t_ms: u64,
        cause: DecisionsStopCause,
        max_bytes: u64,
        decisions_dropped_total: u64,
    },
    Peers {
        t_ms: u64,
        peers: &'a [PeerAttributes],
    },
    Dropped {
        t_ms: u64,
        total: u64,
    },
    Truncated {
        t_ms: u64,
        max_bytes: u64,
        dropped_total: u64,
    },
}

enum Record {
    // Boxed: the forecasts make a route record several times the size of a
    // peers record, and every slot of the bounded channel is sized to the
    // largest variant.
    Route(Box<RouteRecord>),
    Decision(Box<DecisionRecord>),
    DecisionUncaptured(Box<UncapturedDecision>),
    Peers {
        t_ms: u64,
        peers: Vec<PeerAttributes>,
    },
}

/// Handle to a running recorder. Cheap to call from hot paths.
pub(crate) struct RoutingDataset {
    tx: SyncSender<Record>,
    dropped: Arc<AtomicU64>,
    /// Set by the writer when it exits, so callers stop building records that
    /// could only ever be dropped.
    stopped: Arc<AtomicBool>,
    decisions: Arc<DecisionState>,
    /// When this handle's run started, for pacing ([`now_ms`] clock).
    run_start_ms: u64,
    /// Pacing period in ms; 0 disables pacing.
    pace_ms: u64,
}

impl std::fmt::Debug for RoutingDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingDataset")
            .field("dropped", &self.dropped.load(Ordering::Relaxed))
            .field("stopped", &self.stopped.load(Ordering::Relaxed))
            .finish()
    }
}

impl RoutingDataset {
    /// Open `path` for appending and start the writer thread, with the
    /// default decision budget and no pacing.
    #[cfg(test)]
    pub(crate) fn open(path: &Path, max_bytes: u64) -> std::io::Result<Self> {
        Self::open_with_decisions(path, max_bytes, DEFAULT_CANDIDATES_MAX_BYTES, 0)
    }

    /// [`Self::open`] with an explicit decision-line budget and pacing period
    /// (see the module doc's "Candidate sets").
    pub(crate) fn open_with_decisions(
        path: &Path,
        max_bytes: u64,
        decision_max_bytes: u64,
        pace_ms: u64,
    ) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let (mut dataset, rx) = Self::unstarted();
        dataset.pace_ms = pace_ms;
        let dropped = dataset.dropped.clone();
        let stopped = dataset.stopped.clone();
        let decisions = dataset.decisions.clone();
        // A plain OS thread, not a runtime task: its whole job is blocking file
        // I/O, and it lives exactly as long as the process. It exits when every
        // sender is gone, the cap is reached, or a write fails.
        std::thread::Builder::new()
            .name("routing-dataset".into())
            .spawn(move || {
                write_loop(
                    rx,
                    file,
                    Limits {
                        max_bytes,
                        decision_max_bytes,
                    },
                    &dropped,
                    &stopped,
                    &decisions,
                )
            })?;
        Ok(dataset)
    }

    /// A handle that has already stopped, as after its byte cap or a write
    /// error, for tests of callers that must notice.
    #[cfg(test)]
    pub(crate) fn stopped_for_test() -> Self {
        let (dataset, _rx) = Self::unstarted();
        dataset.stopped.store(true, Ordering::Relaxed);
        dataset
    }

    /// A handle whose writer has not been started; the caller owns the receiver.
    fn unstarted() -> (Self, Receiver<Record>) {
        let (tx, rx) = sync_channel(CHANNEL_CAPACITY);
        let dataset = Self {
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
            stopped: Arc::new(AtomicBool::new(false)),
            decisions: Arc::new(DecisionState {
                limit: AtomicU64::new(u64::MAX),
                ..DecisionState::default()
            }),
            run_start_ms: now_ms(),
            pace_ms: 0,
        };
        (dataset, rx)
    }

    /// Whether records are still being written. Callers check this before
    /// building a record, so a stopped recorder costs nothing further.
    pub(crate) fn is_recording(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }

    pub(crate) fn record_route(&self, record: RouteRecord) {
        self.send(Record::Route(Box::new(record)));
    }

    /// Whether decision lines are still being captured: the recorder is
    /// recording and the decision budget is not spent.
    pub(crate) fn is_capturing_decisions(&self) -> bool {
        self.is_recording() && !self.decisions.stopped.load(Ordering::Relaxed)
    }

    pub(crate) fn record_decision(&self, record: DecisionRecord) {
        self.send_decision(Record::Decision(Box::new(record)));
    }

    pub(crate) fn record_uncaptured(&self, record: UncapturedDecision) {
        self.send_decision(Record::DecisionUncaptured(Box::new(record)));
    }

    /// Whether this run's paced allowance leaves room for a capture at
    /// `now_ms`.
    fn within_pace(&self, now_ms: u64) -> bool {
        let limit = self.decisions.limit.load(Ordering::Relaxed);
        let elapsed = now_ms.saturating_sub(self.run_start_ms);
        self.decisions.written.load(Ordering::Relaxed)
            < paced_allowance(limit, elapsed, self.pace_ms)
    }

    /// Queue a decision-kind record, unless [`MAX_QUEUED_DECISIONS`] are
    /// already waiting. Every refusal counts as a DECISION drop, never a route
    /// drop.
    fn send_decision(&self, record: Record) {
        let decisions = &self.decisions;
        if decisions.queued.fetch_add(1, Ordering::Relaxed) >= MAX_QUEUED_DECISIONS {
            decisions.queued.fetch_sub(1, Ordering::Relaxed);
            decisions.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.tx.try_send(record).is_err() {
            decisions.queued.fetch_sub(1, Ordering::Relaxed);
            decisions.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_peers(&self, t_ms: u64, peers: Vec<PeerAttributes>) {
        self.send(Record::Peers { t_ms, peers });
    }

    fn send(&self, record: Record) {
        match self.tx.try_send(record) {
            Ok(()) => {}
            // Full: drop and count — never wait under the router lock.
            // Disconnected: the writer has stopped; counting keeps the total
            // honest for anything still in flight when it did.
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn decisions_dropped(&self) -> u64 {
        self.decisions.dropped.load(Ordering::Relaxed)
    }
}

enum WriteError {
    /// The line would cross the byte cap; nothing was written.
    Cap,
    Io(std::io::Error),
}

struct Writer {
    out: BufWriter<File>,
    written: u64,
    max_bytes: u64,
    reported_dropped: u64,
    /// This run's decision-line budget, fixed at start (see [`Limits`]).
    decision_limit: u64,
    decision_written: u64,
    reported_decision_drops: u64,
}

/// The byte limits a writer runs under.
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_bytes: u64,
    /// Configured decision budget, before the half-of-the-file clamp.
    decision_max_bytes: u64,
}

impl Writer {
    /// Write one ordinary line, unless it would enter the marker reserve.
    fn line(&mut self, line: &Line<'_>) -> Result<(), WriteError> {
        self.line_within(line, self.max_bytes.saturating_sub(MARKER_RESERVE))
    }

    fn line_within(&mut self, line: &Line<'_>, limit: u64) -> Result<(), WriteError> {
        let Some(bytes) = encode(line) else {
            // Cannot happen for these types; skipping beats stopping.
            return Ok(());
        };
        self.bytes_within(&bytes, limit)
    }

    fn bytes_within(&mut self, bytes: &[u8], limit: u64) -> Result<(), WriteError> {
        let len = bytes.len() as u64;
        if self.written.saturating_add(len) > limit {
            return Err(WriteError::Cap);
        }
        self.out.write_all(bytes).map_err(WriteError::Io)?;
        self.written += len;
        Ok(())
    }

    /// Write a decision-kind line inside the decision budget. A line that does
    /// not fit (budget or file) stops capture for the run and writes one
    /// marker, and the result is still `Ok`: this path must never stop route
    /// recording, so it never returns [`WriteError::Cap`].
    fn decision_line(
        &mut self,
        line: &Line<'_>,
        decisions: &DecisionState,
    ) -> Result<(), WriteError> {
        if decisions.stopped.load(Ordering::Relaxed) {
            decisions.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        // A drop before this line is marked before it, so a join cannot cross it.
        self.report_decision_drops(decisions)?;
        let Some(bytes) = encode(line) else {
            return Ok(());
        };
        let len = bytes.len() as u64;
        let cause = if self.decision_written.saturating_add(len) > self.decision_limit {
            DecisionsStopCause::Budget
        } else {
            match self.bytes_within(&bytes, self.max_bytes.saturating_sub(MARKER_RESERVE)) {
                Ok(()) => {
                    self.decision_written += len;
                    decisions
                        .written
                        .store(self.decision_written, Ordering::Relaxed);
                    return Ok(());
                }
                // The file is full: route lines filled their share, and this
                // decision is not what stops them.
                Err(WriteError::Cap) => DecisionsStopCause::FileFull,
                Err(error) => return Err(error),
            }
        };
        decisions.stopped.store(true, Ordering::Relaxed);
        decisions.dropped.fetch_add(1, Ordering::Relaxed);
        let marker = Line::DecisionsTruncated {
            t_ms: now_ms(),
            cause,
            max_bytes: self.decision_limit,
            decisions_dropped_total: decisions.dropped.load(Ordering::Relaxed),
        };
        // May use the marker reserve; if even that is gone, stay silent.
        match self.line_within(&marker, self.max_bytes) {
            Ok(()) | Err(WriteError::Cap) => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// Write a `decisions_dropped` line when decision records have been lost
    /// since the last one. Never stops the recorder: a full file just skips it.
    fn report_decision_drops(&mut self, decisions: &DecisionState) -> Result<(), WriteError> {
        let total = decisions.dropped.load(Ordering::Relaxed);
        if total == self.reported_decision_drops || decisions.stopped.load(Ordering::Relaxed) {
            return Ok(());
        }
        let line = Line::DecisionsDropped {
            t_ms: now_ms(),
            total,
        };
        match self.line_within(&line, self.max_bytes.saturating_sub(MARKER_RESERVE)) {
            Ok(()) => {
                self.reported_decision_drops = total;
                Ok(())
            }
            Err(WriteError::Cap) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn report_drops(&mut self, dropped: &AtomicU64) -> Result<(), WriteError> {
        let total = dropped.load(Ordering::Relaxed);
        if total == self.reported_dropped {
            return Ok(());
        }
        self.line(&Line::Dropped {
            t_ms: now_ms(),
            total,
        })?;
        self.reported_dropped = total;
        Ok(())
    }

    /// Write a record. One the cap turns away is counted as dropped, like any
    /// other record that never reaches the file.
    fn record(
        &mut self,
        record: &Record,
        dropped: &AtomicU64,
        decisions: &DecisionState,
    ) -> Result<(), WriteError> {
        let result = match record {
            Record::Route(route) => self.line(&Line::Route(route)),
            Record::Decision(decision) => {
                return self.decision_line(&Line::Decision(decision), decisions);
            }
            Record::DecisionUncaptured(decision) => {
                return self.decision_line(&Line::DecisionUncaptured(decision), decisions);
            }
            Record::Peers { t_ms, peers } => self.line(&Line::Peers { t_ms: *t_ms, peers }),
        };
        if let Err(WriteError::Cap) = result {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn flush(&mut self) -> Result<(), WriteError> {
        self.out.flush().map_err(WriteError::Io)
    }
}

/// One JSON line, newline-terminated.
fn encode(line: &Line<'_>) -> Option<Vec<u8>> {
    let mut bytes = serde_json::to_vec(line).ok()?;
    bytes.push(b'\n');
    Some(bytes)
}

fn write_loop(
    rx: Receiver<Record>,
    file: File,
    limits: Limits,
    dropped: &AtomicU64,
    stopped: &AtomicBool,
    decisions: &DecisionState,
) {
    let max_bytes = limits.max_bytes;
    let outcome = run_writer(&rx, file, limits, dropped, stopped, decisions);
    // Mark stopped before the receiver drops, so callers stop building records.
    stopped.store(true, Ordering::Relaxed);
    // Anything that slipped into the queue after the final drain is counted
    // here, so the in-memory total stays exact even where the file's cannot.
    count_undelivered(&rx, dropped, decisions);
    match outcome {
        Ok(()) => {}
        Err(WriteError::Cap) => tracing::info!(
            max_bytes,
            "routing dataset: size cap reached; recording stopped"
        ),
        Err(WriteError::Io(error)) => tracing::warn!(
            %error,
            dropped = dropped.load(Ordering::Relaxed),
            "routing dataset: write failed; recording stopped"
        ),
    }
}

fn run_writer(
    rx: &Receiver<Record>,
    file: File,
    limits: Limits,
    dropped: &AtomicU64,
    stopped: &AtomicBool,
    decisions: &DecisionState,
) -> Result<(), WriteError> {
    let max_bytes = limits.max_bytes;
    // Appending to an existing recording counts its bytes against the cap. A
    // file without room for a start line gets nothing at all, so a restart loop
    // cannot grow it past the cap.
    let written = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let mut writer = Writer {
        out: BufWriter::new(file),
        written,
        max_bytes,
        reported_dropped: 0,
        decision_limit: 0,
        decision_written: 0,
        reported_decision_drops: 0,
    };
    writer.line(&Line::Start {
        t_ms: now_ms(),
        version: env!("CARGO_PKG_VERSION"),
    })?;
    writer.flush()?;
    // Decisions may only fill the first half of the usable file, counting
    // everything earlier runs left in it, so the sum over every run appending
    // to this file stays within half and route lines always keep the rest.
    let half = max_bytes.saturating_sub(MARKER_RESERVE) / 2;
    writer.decision_limit = limits
        .decision_max_bytes
        .min(half.saturating_sub(writer.written));
    decisions
        .limit
        .store(writer.decision_limit, Ordering::Relaxed);

    let result = drain(rx, &mut writer, dropped, decisions);
    if let Err(WriteError::Cap) = result {
        // Stop producers FIRST, then count what is still queued, so the marker's
        // total covers every record that will never be written. The one residual
        // window is a sender that checked `is_recording` just before this store
        // and enqueues just after the drain: its record is missing from the
        // marker, though `write_loop` still adds it to the in-memory total.
        stopped.store(true, Ordering::Relaxed);
        count_undelivered(rx, dropped, decisions);
        // The marker may use the reserve. If even that is gone, an earlier run
        // already marked the file and this one adds nothing.
        let marker = Line::Truncated {
            t_ms: now_ms(),
            max_bytes,
            dropped_total: dropped.load(Ordering::Relaxed),
        };
        match writer.line_within(&marker, max_bytes) {
            Ok(()) | Err(WriteError::Cap) => {}
            Err(error) => return Err(error),
        }
    } else {
        // Whatever ended the loop, a drop count not yet written is written now.
        if let Err(WriteError::Io(error)) = writer.report_drops(dropped) {
            return Err(WriteError::Io(error));
        }
    }
    writer.flush()?;
    result
}

/// Release a decision record's slot in the queue bound once the writer has it.
fn taken(record: Record, decisions: &DecisionState) -> Record {
    if let Record::Decision(_) | Record::DecisionUncaptured(_) = record {
        decisions.queued.fetch_sub(1, Ordering::Relaxed);
    }
    record
}

/// Count records still queued when the writer stops as dropped, decision
/// records against the decision counter so they never inflate route drops.
fn count_undelivered(rx: &Receiver<Record>, dropped: &AtomicU64, decisions: &DecisionState) {
    for record in rx.try_iter() {
        match taken(record, decisions) {
            Record::Decision(_) | Record::DecisionUncaptured(_) => {
                decisions.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Record::Route(_) | Record::Peers { .. } => {
                dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn drain(
    rx: &Receiver<Record>,
    writer: &mut Writer,
    dropped: &AtomicU64,
    decisions: &DecisionState,
) -> Result<(), WriteError> {
    loop {
        let first = match rx.recv_timeout(IDLE_WAKE) {
            Ok(record) => Some(taken(record, decisions)),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        writer.report_drops(dropped)?;
        writer.report_decision_drops(decisions)?;
        if let Some(record) = first {
            writer.record(&record, dropped, decisions)?;
            // Take whatever else is already queued, then flush: a busy node is
            // flushed every batch, not only when it happens to go idle.
            for _ in 1..MAX_RECORDS_PER_FLUSH {
                match rx.try_recv() {
                    Ok(record) => writer.record(&taken(record, decisions), dropped, decisions)?,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        writer.flush()?;
                        return Ok(());
                    }
                }
            }
        }
        writer.flush()?;
    }
}

/// Parse the max-bytes override. Anything unparsable falls back to the
/// default with a warning, so `4G` is not silently read as something else.
fn parse_max_bytes(value: Option<&str>) -> u64 {
    parse_byte_count(value, DEFAULT_MAX_BYTES, DATASET_MAX_BYTES_ENV)
}

fn parse_byte_count(value: Option<&str>, default: u64, variable: &str) -> u64 {
    let Some(value) = value else {
        return default;
    };
    match value.trim().parse::<u64>() {
        Ok(bytes) => bytes,
        Err(_) => {
            tracing::warn!(
                value,
                default,
                "routing dataset: {variable} must be a plain byte count; using the default"
            );
            default
        }
    }
}

/// Parse [`DATASET_CANDIDATES_PACE_HOURS_ENV`]: non-negative hours, `0`
/// disabling pacing; anything else falls back to the default with a warning.
fn parse_pace_hours(value: Option<&str>) -> f64 {
    let Some(value) = value else {
        return DEFAULT_CANDIDATES_PACE_HOURS;
    };
    match value.trim().parse::<f64>() {
        Ok(hours) if hours.is_finite() && hours >= 0.0 => hours,
        _ => {
            tracing::warn!(
                value,
                default = DEFAULT_CANDIDATES_PACE_HOURS,
                "routing dataset: {DATASET_CANDIDATES_PACE_HOURS_ENV} must be non-negative hours; using the default"
            );
            DEFAULT_CANDIDATES_PACE_HOURS
        }
    }
}

/// Parse [`DATASET_CANDIDATES_ENV`] into a sampling rate in [0, 1]. An
/// affirmative flag is 1, a fraction in (0, 1] is itself, and anything else is
/// 0 (off) — with a warning unless it was plainly off, so a typo cannot quietly
/// turn capture on or leave an operator believing it is on.
pub(crate) fn parse_candidate_rate(value: Option<&str>) -> f64 {
    let Some(raw) = value else {
        return 0.0;
    };
    if super::parse_routing_flag(Some(raw)) {
        return 1.0;
    }
    match raw.trim().parse::<f64>() {
        Ok(rate) if rate > 0.0 && rate <= 1.0 => rate,
        _ => {
            let lowered = raw.trim().to_ascii_lowercase();
            let zero = lowered.parse::<f64>().is_ok_and(|rate| rate == 0.0);
            if !zero && !matches!(lowered.as_str(), "" | "false" | "no" | "off") {
                tracing::warn!(
                    value = raw,
                    "routing dataset: {DATASET_CANDIDATES_ENV} must be 1/true or a fraction in (0, 1]; candidate logging is off"
                );
            }
            0.0
        }
    }
}

/// The process-wide recorder, if the operator enabled one.
///
/// Resolved once. A path that cannot be opened disables recording with a
/// warning rather than failing the node: this is a diagnostic, not a
/// dependency.
///
/// Always `None` in unit tests: a process-global resolved once is decided by
/// whichever test touches it first, and a developer's exported environment
/// variable would otherwise make every router test write to their file. Tests
/// supply a recorder explicitly instead.
#[cfg(not(test))]
pub(crate) fn global() -> Option<&'static RoutingDataset> {
    static DATASET: std::sync::OnceLock<Option<RoutingDataset>> = std::sync::OnceLock::new();
    DATASET
        .get_or_init(|| {
            let path = std::path::PathBuf::from(std::env::var_os(DATASET_PATH_ENV)?);
            let max_bytes = parse_max_bytes(std::env::var(DATASET_MAX_BYTES_ENV).ok().as_deref());
            let decision_max_bytes = parse_byte_count(
                std::env::var(DATASET_CANDIDATES_MAX_BYTES_ENV)
                    .ok()
                    .as_deref(),
                DEFAULT_CANDIDATES_MAX_BYTES,
                DATASET_CANDIDATES_MAX_BYTES_ENV,
            );
            let pace_hours = parse_pace_hours(
                std::env::var(DATASET_CANDIDATES_PACE_HOURS_ENV)
                    .ok()
                    .as_deref(),
            );
            let pace_ms = (pace_hours * 3_600_000.0) as u64;
            match RoutingDataset::open_with_decisions(&path, max_bytes, decision_max_bytes, pace_ms)
            {
                Ok(dataset) => {
                    tracing::info!(
                        path = %path.display(),
                        max_bytes,
                        "routing dataset: recording route events"
                    );
                    Some(dataset)
                }
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        %error,
                        "routing dataset: cannot open; recording disabled"
                    );
                    None
                }
            }
        })
        .as_ref()
}

#[cfg(test)]
pub(crate) fn global() -> Option<&'static RoutingDataset> {
    None
}

// Test-only: a recorder and sampling rate for `candidate_log` and
// `record_bypass`, so tests can drive the ring's real call sites. Thread-local,
// like the routing flag overrides, so parallel tests cannot see each other's.
#[cfg(test)]
thread_local! {
    static TEST_CANDIDATE_LOG: std::cell::Cell<Option<(&'static RoutingDataset, f64)>> =
        const { std::cell::Cell::new(None) };
}

/// Route candidate logging on this thread to `recorder` at `rate` (captures
/// are never drawn below rate 1) until the guard drops.
#[cfg(test)]
pub(crate) fn force_candidate_log(
    recorder: &'static RoutingDataset,
    rate: f64,
) -> CandidateLogOverrideGuard {
    let previous = TEST_CANDIDATE_LOG.with(|cell| cell.replace(Some((recorder, rate))));
    CandidateLogOverrideGuard { previous }
}

#[cfg(test)]
pub(crate) struct CandidateLogOverrideGuard {
    previous: Option<(&'static RoutingDataset, f64)>,
}

#[cfg(test)]
impl Drop for CandidateLogOverrideGuard {
    fn drop(&mut self) {
        TEST_CANDIDATE_LOG.with(|cell| cell.set(self.previous));
    }
}

/// The process's candidate-logging recorder and rate: the operator's settings,
/// or a test override.
fn candidate_settings() -> (Option<&'static RoutingDataset>, f64) {
    #[cfg(test)]
    if let Some((recorder, rate)) = TEST_CANDIDATE_LOG.with(std::cell::Cell::get) {
        return (Some(recorder), rate);
    }
    static RATE: std::sync::OnceLock<f64> = std::sync::OnceLock::new();
    let rate = *RATE.get_or_init(|| {
        let rate = parse_candidate_rate(std::env::var(DATASET_CANDIDATES_ENV).ok().as_deref());
        if rate > 0.0 && !configured() {
            tracing::warn!(
                "routing dataset: {DATASET_CANDIDATES_ENV} is set but {DATASET_PATH_ENV} is not; \
                 no decisions will be recorded"
            );
        }
        rate
    });
    if rate <= 0.0 {
        return (None, rate);
    }
    (global(), rate)
}

/// Candidate logging for the routing decision about to be made, decided before
/// the router lock is taken: the process recorder, only while it is capturing
/// decisions and only when [`DATASET_CANDIDATES_ENV`] enables it, with this
/// decision drawn at the sampling rate and checked against the run's pacing.
/// `None` costs routing nothing further.
pub(crate) fn candidate_log() -> Option<CandidateLog<'static>> {
    #[cfg(test)]
    if let Some((recorder, rate)) = TEST_CANDIDATE_LOG.with(std::cell::Cell::get) {
        return candidate_log_from(rate, || Some(recorder), |_| false, now_ms);
    }
    let (recorder, rate) = candidate_settings();
    candidate_log_from(
        rate,
        || recorder,
        crate::config::GlobalRng::random_bool,
        now_ms,
    )
}

/// [`candidate_log`]'s decision, with its inputs supplied. Neither the recorder,
/// the sampler nor the clock is consulted while the rate is zero.
pub(crate) fn candidate_log_from<'a>(
    rate: f64,
    recorder: impl FnOnce() -> Option<&'a RoutingDataset>,
    sample: impl FnOnce(f64) -> bool,
    now: impl FnOnce() -> u64,
) -> Option<CandidateLog<'a>> {
    if rate <= 0.0 {
        return None;
    }
    let recorder = recorder().filter(|recorder| recorder.is_capturing_decisions())?;
    let (capture, skipped) = if !(rate >= 1.0 || sample(rate)) {
        (false, UncapturedReason::SampledOut)
    } else if !recorder.within_pace(now()) {
        (false, UncapturedReason::Paced)
    } else {
        (true, UncapturedReason::SampledOut)
    };
    Some(CandidateLog {
        recorder,
        capture,
        skipped,
    })
}

/// Record a route chosen WITHOUT ring selection (see [`UncapturedReason`]), so
/// its outcome cannot join an older captured decision. Draws nothing and does
/// nothing unless candidate logging is enabled.
pub(crate) fn record_bypass(
    op: crate::node::network_status::OpType,
    contract_location: crate::ring::Location,
    peer: &PeerKeyLocation,
    reason: UncapturedReason,
) {
    let (Some(recorder), rate) = candidate_settings() else {
        return;
    };
    if rate <= 0.0 || !recorder.is_capturing_decisions() {
        return;
    }
    recorder.record_uncaptured(UncapturedDecision {
        t_ms: now_ms(),
        op: op.as_str(),
        contract_location: contract_location.as_f64(),
        reason,
        selected: vec![peer_hash(peer)],
    });
}

/// Whether `FREENET_ROUTING_DATASET` is set, so a missing recorder means it
/// failed to open rather than was never asked for.
#[cfg(not(test))]
pub(crate) fn configured() -> bool {
    std::env::var_os(DATASET_PATH_ENV).is_some()
}

#[cfg(test)]
pub(crate) fn configured() -> bool {
    false
}

/// Read a recording once the asynchronous writer has produced what `want`
/// describes — polling the artifact rather than sleeping a fixed amount.
#[cfg(test)]
pub(crate) fn lines_eventually(
    path: &Path,
    want: impl Fn(&[serde_json::Value]) -> bool,
) -> Vec<serde_json::Value> {
    for _ in 0..200 {
        let lines = read_lines(path);
        if want(&lines) {
            return lines;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!(
        "expected lines never appeared in {}: {:?}",
        path.display(),
        std::fs::read_to_string(path)
    );
}

#[cfg(test)]
fn read_lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::TransportKeypair;

    fn route(peer: &str) -> RouteRecord {
        RouteRecord {
            t_ms: 1,
            source: RouteSource::Relay,
            peer: peer.to_string(),
            peer_location: Some(0.25),
            contract_location: 0.5,
            distance: Some(0.25),
            op: Some("GET"),
            outcome: "failure",
            time_to_response_start_s: None,
            payload_bytes: None,
            payload_transfer_s: None,
            prior_failure_events: 3,
            forecasts: Some(FailureForecasts {
                global: 0.1,
                adjusted: 0.2,
                blended: 0.15,
                corrected: 0.12,
                lambda: Some(0.5),
                n_eff: Some(4.0),
                hierarchical: Some(0.11),
                log_response_time_legacy: Some(-1.5),
                log_response_time_hierarchical: None,
                log_transfer_speed_legacy: Some(9.0),
                log_transfer_speed_hierarchical: None,
            }),
        }
    }

    fn peer_at(addr: &str) -> PeerKeyLocation {
        PeerKeyLocation::new(
            TransportKeypair::new().public().clone(),
            addr.parse().unwrap(),
        )
    }

    /// Run the writer synchronously over records already queued.
    fn write_queued(path: &Path, max_bytes: u64, records: Vec<RouteRecord>) -> RoutingDataset {
        run_records(
            path,
            Limits {
                max_bytes,
                decision_max_bytes: DEFAULT_CANDIDATES_MAX_BYTES,
            },
            |dataset| {
                for record in records {
                    dataset.record_route(record);
                }
            },
        )
    }

    #[test]
    fn records_round_trip_as_tagged_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let dataset = RoutingDataset::open(&path, DEFAULT_MAX_BYTES).unwrap();
        assert!(dataset.is_recording());

        dataset.record_route(route("00000000000000aa"));
        dataset.record_peers(
            7,
            vec![PeerAttributes {
                peer: "00000000000000aa".into(),
                location: Some(0.25),
                connected_s: 12.5,
                is_configured_gateway: Some(true),
                version: Some("0.2.135".into()),
                routing_successes: Some(4),
                routing_failures: Some(1),
                bytes_sent: Some(10),
                bytes_received: Some(20),
            }],
        );
        drop(dataset);

        let lines = lines_eventually(&path, |lines| lines.len() >= 3);
        assert_eq!(lines[0]["kind"], "start");
        assert_eq!(lines[1]["kind"], "route");
        assert_eq!(lines[1]["source"], "relay");
        assert_eq!(lines[1]["peer"], "00000000000000aa");
        assert_eq!(lines[1]["outcome"], "failure");
        assert_eq!(lines[1]["forecasts"]["corrected"], 0.12);
        assert_eq!(lines[1]["forecasts"]["hierarchical"], 0.11);
        assert_eq!(lines[1]["forecasts"]["log_response_time_legacy"], -1.5);
        assert_eq!(lines[1]["forecasts"]["log_transfer_speed_legacy"], 9.0);
        assert!(lines[1]["forecasts"]["log_transfer_speed_hierarchical"].is_null());
        assert!(
            lines[1]["forecasts"]["log_response_time_hierarchical"].is_null(),
            "an absent forecast must be recorded as null, not omitted or zero"
        );
        assert_eq!(lines[2]["kind"], "peers");
        assert_eq!(lines[2]["t_ms"], 7);
        assert_eq!(lines[2]["peers"][0]["is_configured_gateway"], true);
    }

    #[test]
    fn a_record_that_would_cross_the_cap_is_replaced_by_the_truncation_marker() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // Room for the start line and one and a half route records.
        let one_route = serde_json::to_vec(&Line::Route(&route("00000000000000aa")))
            .unwrap()
            .len() as u64
            + 1;
        let start_len = 80;
        let cap = MARKER_RESERVE + start_len + one_route + one_route / 2;
        let dataset = write_queued(
            &path,
            cap,
            vec![
                route("00000000000000aa"),
                route("00000000000000bb"),
                route("00000000000000cc"),
            ],
        );

        let lines = read_lines(&path);
        let kinds: Vec<&str> = lines
            .iter()
            .map(|line| line["kind"].as_str().unwrap())
            .collect();
        assert_eq!(kinds, ["start", "route", "truncated"], "{lines:?}");
        assert_eq!(lines[1]["peer"], "00000000000000aa");
        assert_eq!(
            lines[2]["dropped_total"], 2,
            "the marker counts the record the cap turned away and the one still queued"
        );
        assert_eq!(dataset.dropped(), 2);
        let content_before_marker: u64 = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .take(2)
            .map(|line| line.len() as u64 + 1)
            .sum();
        assert!(
            content_before_marker <= cap - MARKER_RESERVE,
            "records must stay out of the marker reserve"
        );
        assert!(std::fs::metadata(&path).unwrap().len() <= cap);
        assert!(
            !dataset.is_recording(),
            "a capped writer reports itself stopped"
        );
    }

    #[test]
    fn reopening_a_file_at_the_cap_does_not_grow_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // Room for a start line but not a route record: the first run writes a
        // start line and a truncation marker.
        let cap = MARKER_RESERVE + 100;
        write_queued(&path, cap, vec![route("00000000000000aa"); 4]);
        let kinds: Vec<String> = read_lines(&path)
            .iter()
            .map(|line| line["kind"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(kinds, ["start", "truncated"]);
        let size = std::fs::metadata(&path).unwrap().len();

        // A restart loop onto the same file must not grow it at all.
        for _ in 0..5 {
            let dataset = write_queued(&path, cap, vec![route("00000000000000bb")]);
            assert!(!dataset.is_recording());
            assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
        }
        assert!(size <= cap);
    }

    #[test]
    fn a_full_channel_counts_drops_and_the_file_carries_the_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        const OVERFLOW: usize = 5;
        // Writer not started while sending, so the channel genuinely fills: the
        // saturated state, reached deterministically rather than by racing a
        // disk. Every send must return even though nothing is draining.
        let dataset = write_queued(
            &path,
            DEFAULT_MAX_BYTES,
            vec![route("00000000000000aa"); CHANNEL_CAPACITY + OVERFLOW],
        );
        assert_eq!(dataset.dropped(), OVERFLOW as u64);

        let lines = read_lines(&path);
        let routes = lines.iter().filter(|line| line["kind"] == "route").count();
        assert_eq!(routes, CHANNEL_CAPACITY, "every queued record is written");
        let reported: Vec<u64> = lines
            .iter()
            .filter(|line| line["kind"] == "dropped")
            .filter_map(|line| line["total"].as_u64())
            .collect();
        assert_eq!(
            reported,
            vec![OVERFLOW as u64],
            "the recording must say it is incomplete, exactly once"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_write_error_stops_the_writer_and_later_records_count_as_dropped() {
        // /dev/full accepts the open and fails every write with ENOSPC.
        let dataset = RoutingDataset::open(Path::new("/dev/full"), DEFAULT_MAX_BYTES).unwrap();
        for _ in 0..200 {
            if !dataset.is_recording() {
                break;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        assert!(
            !dataset.is_recording(),
            "a failed write must stop the writer"
        );
        for _ in 0..200 {
            dataset.record_route(route("00000000000000aa"));
            if dataset.dropped() > 0 {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!("records sent to a stopped writer must be counted as dropped");
    }

    #[test]
    fn opening_an_unwritable_path_fails_instead_of_recording_nowhere() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-dir").join("routing.jsonl");
        assert!(RoutingDataset::open(&missing, DEFAULT_MAX_BYTES).is_err());
    }

    #[test]
    fn max_bytes_parsing_accepts_plain_counts_and_falls_back_otherwise() {
        assert_eq!(parse_max_bytes(None), DEFAULT_MAX_BYTES);
        assert_eq!(parse_max_bytes(Some(" 1048576 ")), 1_048_576);
        assert_eq!(parse_max_bytes(Some("0")), 0);
        assert_eq!(parse_max_bytes(Some("1G")), DEFAULT_MAX_BYTES);
        assert_eq!(parse_max_bytes(Some("")), DEFAULT_MAX_BYTES);
    }

    #[test]
    fn peer_attributes_join_every_field_by_address() {
        let gateway = peer_at("192.0.2.1:31337");
        let ordinary = peer_at("192.0.2.2:31338");
        let never_seen = peer_at("192.0.2.3:31339");
        let connections = [
            (gateway.clone(), 10.0),
            (ordinary.clone(), 20.0),
            (never_seen.clone(), 30.0),
        ];
        let gateways = [gateway.pub_key().clone()];
        let addr = |peer: &PeerKeyLocation| peer.socket_addr().unwrap();
        // Map insertion order deliberately differs from connection order, so an
        // ordering-based join would misattribute.
        let versions = HashMap::from([
            (addr(&ordinary), (0, 2, 135)),
            (addr(&gateway), (0, 2, 134)),
        ]);
        let health = HashMap::from([(addr(&ordinary), (7, 3)), (addr(&gateway), (100, 1))]);
        let transfer = HashMap::from([(addr(&gateway), (5, 6))]);

        let attributes = peer_attributes(&PeerSnapshotInputs {
            connections: &connections,
            gateways: Some(&gateways),
            versions: &versions,
            health: &health,
            transfer: &transfer,
        });

        assert_eq!(attributes.len(), 3);
        let [gw, ord, unseen] = [&attributes[0], &attributes[1], &attributes[2]];
        assert_eq!(gw.peer, peer_hash(&gateway));
        assert_eq!(gw.is_configured_gateway, Some(true));
        assert_eq!(gw.version.as_deref(), Some("0.2.134"));
        assert_eq!(
            (gw.routing_successes, gw.routing_failures),
            (Some(100), Some(1))
        );
        assert_eq!((gw.bytes_sent, gw.bytes_received), (Some(5), Some(6)));
        assert_eq!(gw.connected_s, 10.0);

        assert_eq!(ord.peer, peer_hash(&ordinary));
        assert_eq!(ord.is_configured_gateway, Some(false));
        assert_eq!(ord.version.as_deref(), Some("0.2.135"));
        assert_eq!(
            (ord.routing_successes, ord.routing_failures),
            (Some(7), Some(3))
        );
        assert_eq!((ord.bytes_sent, ord.bytes_received), (None, None));

        assert_eq!(unseen.version, None);
        assert_eq!(
            (unseen.routing_successes, unseen.routing_failures),
            (None, None)
        );
        assert_eq!(unseen.location, never_seen.location().map(|l| l.as_f64()));

        let unknown = peer_attributes(&PeerSnapshotInputs {
            connections: &connections,
            gateways: None,
            versions: &versions,
            health: &health,
            transfer: &transfer,
        });
        assert!(
            unknown
                .iter()
                .all(|peer| peer.is_configured_gateway.is_none()),
            "an unavailable gateway list must read as unknown, not as 'not a gateway'"
        );
    }

    #[test]
    fn operator_facing_variable_names_are_stable() {
        // These names are what an operator's service unit sets; renaming one
        // silently turns recording off on every node that uses it.
        assert_eq!(DATASET_PATH_ENV, "FREENET_ROUTING_DATASET");
        assert_eq!(DATASET_MAX_BYTES_ENV, "FREENET_ROUTING_DATASET_MAX_BYTES");
        assert_eq!(DATASET_CANDIDATES_ENV, "FREENET_ROUTING_DATASET_CANDIDATES");
        assert_eq!(
            DATASET_CANDIDATES_MAX_BYTES_ENV,
            "FREENET_ROUTING_DATASET_CANDIDATES_MAX_BYTES"
        );
        assert_eq!(
            DATASET_CANDIDATES_PACE_HOURS_ENV,
            "FREENET_ROUTING_DATASET_CANDIDATES_PACE_HOURS"
        );
    }

    fn estimate(expected_total_time: f64) -> ModelEstimate {
        ModelEstimate {
            failure_probability: 0.1,
            time_to_response_start_s: 0.2,
            transfer_speed_bps: 1000.0,
            expected_total_time,
        }
    }

    /// A capture over `peers` in the given (distance) order, with per-candidate
    /// legacy and hierarchical costs and the router's returned positions.
    fn capture<'a>(
        peers: &'a [PeerKeyLocation],
        legacy: &[Option<f64>],
        hierarchical: &[Option<f64>],
        selected: &[(usize, usize)],
    ) -> DecisionCapture<'a> {
        let mut candidates: Vec<CapturedCandidate<'a>> = peers
            .iter()
            .enumerate()
            .map(|(i, peer)| CapturedCandidate {
                peer,
                legacy: legacy[i].map(estimate),
                hierarchical: hierarchical[i].map(estimate),
                hierarchical_stages: HierarchicalStages {
                    failure: true,
                    response_time: i % 2 == 0,
                    transfer_speed: false,
                },
                selected_position: None,
            })
            .collect();
        for &(index, position) in selected {
            candidates[index].selected_position = Some(position);
        }
        DecisionCapture {
            contract_location: crate::ring::Location::new(0.5),
            acting_model: RoutingModel::Legacy,
            prediction_fallback: legacy.iter().any(Option::is_none),
            k: selected.len(),
            candidates_available: peers.len() + 10,
            prior_failure_events: 77,
            candidates,
        }
    }

    fn peers(count: u32) -> Vec<PeerKeyLocation> {
        (0..count)
            .map(|i| peer_at(&format!("198.51.{}.{}:31337", i / 250, i % 250 + 1)))
            .collect()
    }

    #[test]
    fn a_decision_record_ranks_every_candidate_under_both_models() {
        let peers = peers(4);
        // Legacy order: 1, 3, 0, 2 (2 has no prediction, so last, as in the
        // router). Hierarchical order: 2, 0, 1, 3 — the tie between 1 and 3
        // breaks by distance order, again as in the router's stable sort.
        let record = capture(
            &peers,
            &[Some(3.0), Some(1.0), None, Some(2.0)],
            &[Some(2.0), Some(5.0), Some(1.0), Some(5.0)],
            &[(1, 0), (3, 1)],
        )
        .into_record(crate::node::network_status::OpType::Subscribe, 42);

        let ranks: Vec<(usize, usize)> = record
            .candidates
            .iter()
            .map(|c| (c.rank_legacy, c.rank_hierarchical))
            .collect();
        assert_eq!(ranks, [(2, 1), (0, 2), (3, 0), (1, 3)]);
        assert_eq!(record.chosen_rank_legacy, Some(0));
        assert_eq!(record.chosen_rank_hierarchical, Some(2));
        assert_eq!(record.op, "SUBSCRIBE");
        assert_eq!(record.t_ms, 42);
        assert!(record.prediction_fallback);
        assert_eq!((record.k, record.candidates_available), (2, 14));
        assert_eq!(
            (record.candidates_considered, record.candidates_omitted),
            (4, 0)
        );
        for (index, (candidate, peer)) in record.candidates.iter().zip(&peers).enumerate() {
            assert_eq!(candidate.peer, peer_hash(peer), "peers keep distance order");
            assert_eq!(candidate.distance_rank, index);
            let location = peer.location().unwrap();
            assert_eq!(candidate.peer_location, Some(location.as_f64()));
            assert_eq!(
                candidate.distance,
                Some(crate::ring::Location::new(0.5).distance(location).as_f64())
            );
        }
        assert!(record.candidates[2].legacy.is_none());
        assert_eq!(
            record
                .candidates
                .iter()
                .map(|c| c.selected_position)
                .collect::<Vec<_>>(),
            [None, Some(0), None, Some(1)]
        );
    }

    #[test]
    fn a_decision_over_the_candidate_cap_keeps_the_selection_and_counts_the_rest() {
        let extra = 8;
        let count = MAX_RECORDED_CANDIDATES + extra;
        let peers = peers(count as u32);
        // Legacy cost rises with index, so the acting model's worst is last —
        // and the router selected that worst one anyway (as it would with k
        // larger than the cap, or an ordering this test does not model).
        let legacy: Vec<Option<f64>> = (0..count).map(|i| Some(i as f64)).collect();
        let hierarchical: Vec<Option<f64>> = (0..count).map(|i| Some((count - i) as f64)).collect();
        let record = capture(&peers, &legacy, &hierarchical, &[(count - 1, 0)])
            .into_record(crate::node::network_status::OpType::Get, 1);

        assert_eq!(record.candidates.len(), MAX_RECORDED_CANDIDATES);
        assert_eq!(record.candidates_considered, count);
        assert_eq!(record.candidates_omitted, extra);
        let kept: Vec<usize> = record.candidates.iter().map(|c| c.distance_rank).collect();
        let mut expected: Vec<usize> = (0..MAX_RECORDED_CANDIDATES - 1).collect();
        expected.push(count - 1);
        assert_eq!(
            kept, expected,
            "the selection, then the acting model's best, in distance order"
        );
        let chosen = record.candidates.last().unwrap();
        assert_eq!(chosen.selected_position, Some(0));
        assert_eq!(
            (chosen.rank_legacy, chosen.rank_hierarchical),
            (count - 1, 0),
            "ranks are over the whole window, not the written subset"
        );
        assert_eq!(record.chosen_rank_legacy, Some(count - 1));
    }

    #[test]
    fn decision_lines_round_trip_as_tagged_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let dataset = RoutingDataset::open(&path, DEFAULT_MAX_BYTES).unwrap();
        let peers = peers(2);
        dataset.record_decision(
            capture(&peers, &[Some(1.0), None], &[None, Some(1.0)], &[(0, 0)])
                .into_record(crate::node::network_status::OpType::Put, 9),
        );
        drop(dataset);

        let lines = lines_eventually(&path, |lines| lines.len() >= 2);
        let decision = &lines[1];
        assert_eq!(decision["kind"], "decision");
        assert_eq!(decision["op"], "PUT");
        assert_eq!(decision["t_ms"], 9);
        assert_eq!(decision["contract_location"], 0.5);
        assert_eq!(decision["acting_model"], "legacy");
        assert_eq!(decision["candidates_omitted"], 0);
        assert_eq!(decision["chosen_rank_hierarchical"], 1);
        let first = &decision["candidates"][0];
        assert_eq!(first["peer"], peer_hash(&peers[0]));
        assert_eq!(first["selected_position"], 0);
        assert_eq!(first["legacy"]["expected_total_time"], 1.0);
        assert_eq!(first["legacy"]["transfer_speed_bps"], 1000.0);
        assert!(
            first["hierarchical"].is_null(),
            "an absent estimate is null, not omitted"
        );
        assert_eq!(first["hierarchical_stages"]["response_time"], true);
        assert!(decision["candidates"][1]["selected_position"].is_null());
        assert_eq!(decision["candidates"][1]["rank_hierarchical"], 0);
    }

    #[test]
    fn candidate_rate_parses_flags_and_fractions_and_fails_off() {
        assert_eq!(parse_candidate_rate(None), 0.0, "unset must be off");
        for on in ["1", "true", " YES ", "on"] {
            assert_eq!(parse_candidate_rate(Some(on)), 1.0, "{on:?}");
        }
        assert_eq!(parse_candidate_rate(Some("0.05")), 0.05);
        assert_eq!(parse_candidate_rate(Some(" 0.5 ")), 0.5);
        for off in [
            "0", "", "off", "false", "1.5", "-0.1", "NaN", "inf", "5%", "ture",
        ] {
            assert_eq!(parse_candidate_rate(Some(off)), 0.0, "{off:?}");
        }
    }

    /// Run a writer synchronously over `records`, queued in order, appending
    /// to `path`. Returns the handle's counters.
    fn run_records(
        path: &Path,
        limits: Limits,
        records: impl FnOnce(&RoutingDataset),
    ) -> RoutingDataset {
        let (dataset, rx) = RoutingDataset::unstarted();
        records(&dataset);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        let (dropped, stopped, decisions) = (
            dataset.dropped.clone(),
            dataset.stopped.clone(),
            dataset.decisions.clone(),
        );
        let RoutingDataset {
            tx,
            run_start_ms,
            pace_ms,
            ..
        } = dataset;
        drop(tx);
        write_loop(rx, file, limits, &dropped, &stopped, &decisions);
        let (tx, _rx) = sync_channel(0);
        RoutingDataset {
            tx,
            dropped,
            stopped,
            decisions,
            run_start_ms,
            pace_ms,
        }
    }

    fn decision(peers: &[PeerKeyLocation]) -> DecisionRecord {
        let costs: Vec<Option<f64>> = (0..peers.len()).map(|i| Some(i as f64)).collect();
        capture(peers, &costs, &costs, &[(0, 0)])
            .into_record(crate::node::network_status::OpType::Get, 1)
    }

    fn kinds_of(lines: &[serde_json::Value], kind: &str) -> usize {
        lines.iter().filter(|line| line["kind"] == kind).count()
    }

    fn line_len(line: &Line<'_>) -> u64 {
        encode(line).unwrap().len() as u64
    }

    #[test]
    fn candidate_logging_is_off_unless_switched_on_and_capturing() {
        let dir = tempfile::tempdir().unwrap();
        let recording =
            RoutingDataset::open(&dir.path().join("r.jsonl"), DEFAULT_MAX_BYTES).unwrap();
        let stopped = RoutingDataset::stopped_for_test();
        let (spent, _rx) = RoutingDataset::unstarted();
        spent.decisions.stopped.store(true, Ordering::Relaxed);
        let never = |_: f64| -> bool { panic!("must not sample") };
        let no_clock = || -> u64 { panic!("must not read the clock") };

        assert!(
            candidate_log_from(
                0.0,
                || -> Option<&RoutingDataset> {
                    panic!("a disabled switch must not even look the recorder up")
                },
                never,
                no_clock,
            )
            .is_none()
        );
        assert!(candidate_log_from(1.0, || None, never, no_clock).is_none());
        assert!(
            candidate_log_from(1.0, || Some(&stopped), never, no_clock).is_none(),
            "a recorder that stopped must stop candidate capture too"
        );
        assert!(
            spent.is_recording()
                && candidate_log_from(1.0, || Some(&spent), never, no_clock).is_none(),
            "spent decision capture stops while recording goes on"
        );
        // Unpaced: rate 1 captures without drawing.
        let all = candidate_log_from(1.0, || Some(&recording), never, || 0).unwrap();
        assert!(all.capture);
        let drawn = candidate_log_from(
            0.25,
            || Some(&recording),
            |rate| {
                assert_eq!(rate, 0.25);
                false
            },
            no_clock,
        )
        .unwrap();
        assert!(
            !drawn.capture,
            "a sampled-out decision still logs, uncaptured"
        );
        assert_eq!(drawn.skipped, UncapturedReason::SampledOut);
        assert!(
            candidate_log_from(0.25, || Some(&recording), |_| true, || 0)
                .unwrap()
                .capture
        );
    }

    /// Decisions made before the writer thread has published the run's budget
    /// must not be refused as paced: that race made the first decisions of a
    /// run read `paced` with pacing switched off.
    #[test]
    fn decisions_before_the_writer_starts_are_not_paced() {
        let (mut recorder, _rx) = RoutingDataset::unstarted();
        assert!(recorder.within_pace(recorder.run_start_ms), "unpaced");
        recorder.pace_ms = 3_600_000;
        assert!(
            recorder.within_pace(recorder.run_start_ms),
            "paced: the burst is available at once"
        );
    }

    #[test]
    fn paced_allowance_releases_the_budget_over_the_period() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(paced_allowance(GIB, 0, 1000), PACE_BURST_BYTES);
        assert_eq!(paced_allowance(GIB, 500, 1000), GIB / 2 + PACE_BURST_BYTES);
        assert_eq!(paced_allowance(GIB, 1000, 1000), GIB);
        assert_eq!(paced_allowance(GIB, 5000, 1000), GIB);
        assert_eq!(
            paced_allowance(GIB, 0, 0),
            GIB,
            "a zero period disables pacing"
        );
        assert_eq!(
            paced_allowance(1000, 0, 1000),
            1000,
            "the burst never exceeds the budget"
        );
    }

    /// Pacing is what keeps the sample from being spent in the cold-router
    /// hours right after a start.
    #[test]
    fn captures_over_the_paced_allowance_are_written_uncaptured() {
        const LIMIT: u64 = 1024 * 1024 * 1024;
        const PACE_MS: u64 = 3_600_000;
        let (mut recorder, _rx) = RoutingDataset::unstarted();
        recorder.pace_ms = PACE_MS;
        recorder.run_start_ms = 1_000;
        recorder.decisions.limit.store(LIMIT, Ordering::Relaxed);
        recorder
            .decisions
            .written
            .store(PACE_BURST_BYTES + LIMIT / 4, Ordering::Relaxed);
        let at =
            |now_ms: u64| candidate_log_from(1.0, || Some(&recorder), |_| true, || now_ms).unwrap();

        let early = at(1_000 + PACE_MS / 10);
        assert!(
            !early.capture,
            "a quarter of the budget spent a tenth of the way in"
        );
        assert_eq!(early.skipped, UncapturedReason::Paced);
        assert!(
            at(1_000 + PACE_MS / 2).capture,
            "half the budget released by half-time"
        );
        assert!(at(1_000 + 2 * PACE_MS).capture);
    }

    #[test]
    fn an_uncaptured_decision_writes_its_reason_and_an_empty_selection_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let dataset = RoutingDataset::open(&path, DEFAULT_MAX_BYTES).unwrap();
        let peers = peers(2);
        let selected = [&peers[1], &peers[0]];
        let log = |capture, skipped| CandidateLog {
            recorder: &dataset,
            capture,
            skipped,
        };
        let loc = crate::ring::Location::new(0.25);
        log(false, UncapturedReason::SampledOut).record(
            crate::node::network_status::OpType::Put,
            loc,
            &selected,
            false,
            None,
        );
        log(false, UncapturedReason::Paced).record(
            crate::node::network_status::OpType::Get,
            loc,
            &selected[..1],
            false,
            None,
        );
        // Distance-based wins over the sampling outcome: it says why no
        // candidate set could exist at all.
        log(false, UncapturedReason::SampledOut).record(
            crate::node::network_status::OpType::Get,
            loc,
            &selected[..1],
            true,
            None,
        );
        // A selection that returned nobody routes nowhere: no line.
        log(true, UncapturedReason::SampledOut).record(
            crate::node::network_status::OpType::Get,
            loc,
            &[],
            false,
            None,
        );
        dataset.record_route(route("00000000000000ee"));
        drop(dataset);

        let lines = lines_eventually(&path, |lines| lines.len() >= 5);
        let kinds: Vec<&str> = lines.iter().map(|l| l["kind"].as_str().unwrap()).collect();
        assert_eq!(
            kinds,
            [
                "start",
                "decision_uncaptured",
                "decision_uncaptured",
                "decision_uncaptured",
                "route"
            ]
        );
        assert_eq!(lines[1]["op"], "PUT");
        assert_eq!(lines[1]["contract_location"], 0.25);
        assert_eq!(lines[1]["reason"], "sampled_out");
        assert_eq!(
            lines[1]["selected"],
            serde_json::json!([peer_hash(&peers[1]), peer_hash(&peers[0])]),
            "selected peers in the router's order"
        );
        assert_eq!(lines[2]["reason"], "paced");
        assert_eq!(lines[3]["reason"], "distance_based");
    }

    #[test]
    fn candidate_rate_zero_spelled_as_a_fraction_is_plainly_off() {
        assert_eq!(parse_candidate_rate(Some("0.0")), 0.0);
        assert_eq!(parse_pace_hours(None), DEFAULT_CANDIDATES_PACE_HOURS);
        assert_eq!(parse_pace_hours(Some("0")), 0.0);
        assert_eq!(parse_pace_hours(Some(" 12.5 ")), 12.5);
        for bad in ["-1", "NaN", "inf", "1h"] {
            assert_eq!(
                parse_pace_hours(Some(bad)),
                DEFAULT_CANDIDATES_PACE_HOURS,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn queued_decisions_are_bounded_and_every_loss_is_marked_before_the_next_decision() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let peers = peers(1);
        const OVER: usize = 3;
        let dataset = run_records(
            &path,
            Limits {
                max_bytes: DEFAULT_MAX_BYTES,
                decision_max_bytes: DEFAULT_CANDIDATES_MAX_BYTES,
            },
            |dataset| {
                // Writer not started, so nothing drains while sending.
                for _ in 0..MAX_QUEUED_DECISIONS + OVER {
                    dataset.record_decision(decision(&peers));
                }
                assert_eq!(dataset.decisions_dropped(), OVER as u64);
                assert_eq!(dataset.dropped(), 0, "a decision drop is not a route drop");
                assert_eq!(
                    dataset.decisions.queued.load(Ordering::Relaxed),
                    MAX_QUEUED_DECISIONS
                );
            },
        );
        assert_eq!(
            dataset.decisions.queued.load(Ordering::Relaxed),
            0,
            "every written decision frees its slot"
        );
        let lines = read_lines(&path);
        assert_eq!(kinds_of(&lines, "decision"), MAX_QUEUED_DECISIONS);
        assert_eq!(
            kinds_of(&lines, "dropped"),
            0,
            "decision drops must not mark the route stream incomplete"
        );
        let marks: Vec<usize> = lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line["kind"] == "decisions_dropped")
            .map(|(index, _)| index)
            .collect();
        assert_eq!(marks.len(), 1, "{:?}", &lines[..3]);
        assert_eq!(lines[marks[0]]["total"], OVER as u64);
        let first_decision = lines.iter().position(|l| l["kind"] == "decision").unwrap();
        assert!(
            marks[0] < first_decision,
            "a loss must be marked before any later decision a join could cross it to"
        );
    }

    /// Decision lines must never be able to stop the route recording they
    /// enrich — nor, through `is_recording`, the hierarchical estimator that is
    /// computed only while the recorder records.
    #[test]
    fn a_spent_decision_budget_stops_only_decision_capture() {
        let _flag_off = super::super::force_hierarchical_routing(false);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        // A small file and an unlimited configured decision budget: the
        // half-of-the-file clamp is what must hold the decisions back.
        const MAX_BYTES: u64 = 64_000;
        let dataset = RoutingDataset::open_with_decisions(&path, MAX_BYTES, u64::MAX, 0).unwrap();
        let peers = peers(2);
        const DECISIONS: usize = 200;
        const ROUTES: usize = 40;
        for _ in 0..DECISIONS {
            dataset.record_decision(decision(&peers));
        }
        for i in 0..ROUTES {
            dataset.record_route(route(&format!("{i:016x}")));
        }

        let lines = lines_eventually(&path, |lines| kinds_of(lines, "route") == ROUTES);
        assert_eq!(kinds_of(&lines, "decisions_truncated"), 1, "{lines:?}");
        let marker = lines
            .iter()
            .find(|l| l["kind"] == "decisions_truncated")
            .unwrap();
        assert_eq!(marker["cause"], "budget");
        assert_eq!(
            kinds_of(&lines, "truncated"),
            0,
            "the recorder itself must not stop"
        );
        assert_eq!(kinds_of(&lines, "dropped"), 0, "no route record was lost");
        let decision_lines = kinds_of(&lines, "decision");
        assert!(decision_lines > 0 && decision_lines < DECISIONS);
        assert!(
            dataset.is_recording(),
            "route recording goes on after the decision budget is spent"
        );
        assert!(
            super::super::hierarchical_computed(Some(&dataset)),
            "and so does the hierarchical estimator on a flag-off node"
        );
        assert!(!dataset.is_capturing_decisions());
        assert_eq!(
            dataset.decisions_dropped(),
            (DECISIONS - decision_lines) as u64
        );
    }

    /// The half-of-the-file reservation must hold however many runs append:
    /// a per-run "half of what is left" lets restarts hand decisions nearly
    /// the whole file.
    #[test]
    fn decisions_keep_to_half_the_file_across_appending_runs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        const MAX_BYTES: u64 = 64_000;
        let limits = Limits {
            max_bytes: MAX_BYTES,
            decision_max_bytes: u64::MAX,
        };
        let usable = MAX_BYTES - MARKER_RESERVE;
        let peers = peers(2);
        for _ in 0..4 {
            run_records(&path, limits, |dataset| {
                for _ in 0..200 {
                    dataset.record_decision(decision(&peers));
                }
            });
        }
        let decision_bytes: u64 = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .filter(|line| line.contains(r#""kind":"decision""#))
            .map(|line| line.len() as u64 + 1)
            .sum();
        assert!(
            decision_bytes <= usable / 2,
            "{decision_bytes} decision bytes over four runs exceed half of {usable}"
        );

        // A fifth run's routes, sized to need most of the half reserved for them.
        let route_len = line_len(&Line::Route(&route("0000000000000000")));
        let routes = (usable * 45 / 100) / route_len;
        run_records(&path, limits, |dataset| {
            for i in 0..routes {
                dataset.record_route(route(&format!("{i:016x}")));
            }
        });
        let lines = read_lines(&path);
        assert_eq!(kinds_of(&lines, "truncated"), 0);
        assert_eq!(kinds_of(&lines, "route") as u64, routes);
    }

    /// The realistic end of a recording: route lines fill the file while the
    /// decision budget is unspent. The decision that no longer fits must stop
    /// only decision capture, so a smaller route line after it still lands.
    #[test]
    fn a_decision_that_fits_its_budget_but_not_the_file_stops_only_capture() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let peers = peers(8);
        let route_len = line_len(&Line::Route(&route("0000000000000000")));
        let decision_len = line_len(&Line::Decision(&decision(&peers)));
        assert!(
            decision_len > 3 * route_len,
            "{decision_len} vs {route_len}"
        );
        let start_len = line_len(&Line::Start {
            t_ms: now_ms(),
            version: env!("CARGO_PKG_VERSION"),
        });
        // Routes that leave between one route line and one decision line free.
        const ROUTES: u64 = 100;
        let gap = (route_len + decision_len) / 2;
        let max_bytes = MARKER_RESERVE + start_len + ROUTES * route_len + gap;
        let limits = Limits {
            max_bytes,
            decision_max_bytes: u64::MAX,
        };
        run_records(&path, limits, |dataset| {
            for i in 0..ROUTES {
                dataset.record_route(route(&format!("{i:016x}")));
            }
            dataset.record_decision(decision(&peers));
            dataset.record_route(route("ffffffffffffffff"));
        });
        let lines = read_lines(&path);
        assert_eq!(kinds_of(&lines, "decision"), 0);
        let marker = lines
            .iter()
            .find(|l| l["kind"] == "decisions_truncated")
            .expect("capture stops with a marker");
        assert_eq!(marker["cause"], "file_full");
        assert_eq!(
            kinds_of(&lines, "truncated"),
            0,
            "the recorder must not stop"
        );
        assert_eq!(kinds_of(&lines, "route") as u64, ROUTES + 1);
        assert_eq!(lines.last().unwrap()["peer"], "ffffffffffffffff");
    }

    /// Records still queued when the route cap stops the writer are dropped;
    /// the route stream's own count must include only route records.
    #[test]
    fn records_queued_at_the_route_cap_are_counted_by_kind() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("routing.jsonl");
        let peers = peers(1);
        let route_len = line_len(&Line::Route(&route("0000000000000000")));
        const DECISIONS: u64 = 5;
        // Room for the start line and about ten routes.
        let max_bytes = MARKER_RESERVE + 100 + 10 * route_len;
        let dataset = run_records(
            &path,
            Limits {
                max_bytes,
                // No decision may be written, so every queued one stays queued
                // behind the routes or is refused at the budget.
                decision_max_bytes: 0,
            },
            |dataset| {
                for i in 0..20 {
                    dataset.record_route(route(&format!("{i:016x}")));
                }
                for _ in 0..DECISIONS {
                    dataset.record_decision(decision(&peers));
                }
            },
        );
        let lines = read_lines(&path);
        let written = kinds_of(&lines, "route") as u64;
        let marker = lines.iter().find(|l| l["kind"] == "truncated").unwrap();
        assert_eq!(marker["dropped_total"], 20 - written);
        assert_eq!(dataset.dropped(), 20 - written);
        assert_eq!(dataset.decisions_dropped(), DECISIONS);
    }

    #[test]
    fn peer_hash_is_stable_and_uses_the_whole_key() {
        // Distinct keys on purpose: `PeerKeyLocation::random()` reuses one cached
        // key per thread, so two of those differ only in address — which this
        // hash must ignore.
        let key = TransportKeypair::new().public().clone();
        let a = PeerKeyLocation::new(key.clone(), "192.0.2.1:31337".parse().unwrap());
        let b = peer_at("192.0.2.1:31337");
        assert_eq!(
            peer_hash(&a),
            peer_hash(&PeerKeyLocation::new(
                key,
                "192.0.2.2:31338".parse().unwrap()
            )),
            "identity is the key; a peer that changes address is the same peer"
        );
        assert_ne!(peer_hash(&a), peer_hash(&b));
        assert_eq!(peer_hash(&a).len(), 16);
    }
}
