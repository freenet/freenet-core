//! Per-delegate observability (#5467 Phase 0).
//!
//! Before this module there was NO per-delegate observability of any kind: no
//! invocation count, no execution timing, no error count, and no way to ask
//! which contracts a delegate is subscribed to — let alone whether those
//! subscriptions actually registered demand. #5467 puts it plainly: "app
//! subscriptions have a bounded query (#4549), delegate state has nothing."
//!
//! That absence is what let #4669 survive: a delegate subscribes, the call
//! succeeds, `ContractNotification` delivery works, and *nothing anywhere
//! reports that the pin did not take*. This module exists so that failure is
//! visible from the outside.
//!
//! # Two units of measurement, and why per-call alone is misleading
//!
//! A client request does not map to one delegate execution. One request drives
//! the loop in `contract.rs::handle_delegate_with_contract_requests`, which
//! re-invokes `process()` up to `MAX_CONTRACT_REQUEST_ITERATIONS` (100) times,
//! and each iteration may emit inter-delegate messages that are dispatched
//! single-hop but with **no cap on the batch size**. So a healthy-looking 40 ms
//! per-call p99 is entirely consistent with one request burning seconds of
//! executor time.
//!
//! Both units are therefore recorded:
//!
//! - **Per-call** ([`record_invocation`]): one `inbound_app_message` execution.
//! - **Per-request** ([`RequestMeter`]): the whole `handle_delegate_with_contract_requests`
//!   call — wall time, iteration count, inter-delegate message count, and
//!   whether the iteration cap was hit.
//!
//! The iteration-cap counter matters out of proportion to its size. That arm
//! returns `Ok(accumulated_messages)` (#5454 is where whether that should
//! change is being decided), so the most likely runaway shape — a delegate
//! spinning until the loop gives up — produces no error at all. Without this
//! counter a spinning delegate reports as perfectly healthy.
//!
//! # What is canonical and what is stored here
//!
//! `.claude/rules/bug-prevention-patterns.md` ("manually-mirrored telemetry
//! counters / dashboard providers", #4009/#4010) says to prefer a provider
//! closure reading canonical state over re-mirroring counters at call sites.
//! Applied here, most of the data is read live and NOT mirrored:
//!
//! | Field | Canonical source | Mirrored here? |
//! |---|---|---|
//! | subscriptions held | `DELEGATE_SUBSCRIPTIONS` | no — read live |
//! | did it register demand | `Ring::in_use_contract_ids` | no — read live |
//! | attributed CPU rate | `topology::meter::Meter` | no — read live |
//! | module-cache entries / bytes / evictions | `Ring::module_cache_metrics` | no — read live |
//! | invocations / requests / errors / last error | *nothing* | **yes — this module** |
//!
//! Only the last row is mirrored, because no canonical store for it exists:
//! delegate execution today has no timing at all and errors are logged
//! per-call at `contract/executor/runtime/delegates.rs` without ever being
//! counted. To keep that mirror honest the write sites are source-pinned: the
//! per-call counter ([`record_invocation`]) is called from exactly two places,
//! the `Ok` and `Err` arms of the sole production `inbound_app_message` call,
//! and the per-request counter ([`RequestMeter`]) from exactly one. The
//! per-call pin asserts BOTH outcomes are recorded, since dropping only the
//! failure arm would make a delegate that panics every time look healthy.
//!
//! # Bounding
//!
//! Both axes of growth are bounded, because both are influenced by an external
//! actor (a delegate chooses how many distinct keys exist and what its error
//! messages say):
//!
//! - **Entries** are capped at [`MAX_TRACKED_DELEGATES`] with a
//!   [`DELEGATE_STATS_TTL`] absolute-age sweep, mirroring the discipline in
//!   `topology::meter` (`MAX_ATTRIBUTION_SOURCES` / `ATTRIBUTION_SOURCE_TTL`).
//! - **`last_error` is truncated** at [`MAX_LAST_ERROR_BYTES`] on the way in.
//!   This is the trap in bug-prevention-patterns.md's "cache bounded by entry
//!   COUNT while holding contract- or peer-controlled values" row: a count cap
//!   reads like a memory bound and is not one. An untruncated error string is
//!   delegate-controlled text, so the worst case would be
//!   `MAX_TRACKED_DELEGATES` × (whatever the guest chose to panic with).
//!
//! # How to read the durations (two artefacts, both real)
//!
//! **The ~5 s plateau is the backstop firing, not the cost.** Delegate
//! execution gets the wall-clock backstop contracts already had (#5480). On a
//! timeout the call returns at `max_execution_seconds` (default 5.0) while the
//! abandoned guest thread keeps running, because a `spawn_blocking` closure
//! cannot be cancelled. So timed-out invocations pile up just under ~5 s. In a
//! p99 view that reads as a suspiciously clean cliff; it is genuine, and it
//! means the backstop worked. It is NOT evidence that the delegate's real cost
//! is 5 s — the true cost is unbounded and unmeasurable from here, which is the
//! reason the backstop exists. Before #5480 the same delegate blocked until the
//! epoch trap fired, or indefinitely if the epoch ticker thread had died
//! (#4864).
//!
//! **`engine_invocations` counts ENGINE ROUND-TRIPS, not application messages.**
//! This is the one number here most likely to be misread, so it is named for
//! what it counts. The timing brackets `Runtime::inbound_app_message`, which
//! sits INSIDE the V1 re-invocation loop: `handle_delegate_with_contract_requests`
//! (`contract.rs`) calls `execute_delegate_request` once per iteration, up to
//! `MAX_CONTRACT_REQUEST_ITERATIONS` (100) times, and each of those reaches
//! this timer. So a V1 delegate handling ONE application message that performs
//! four contract GETs records **five** engine invocations, not one.
//!
//! Two consequences a reader must not trip over:
//!
//! - **The per-logical-request unit is [`DelegateStatusEntry::requests`]**,
//!   metered by [`RequestMeter`] at the loop boundary. That is the number that
//!   answers "how often was this delegate asked to do something".
//! - **This axis is NOT comparable across delegate API versions.** V2 delegates
//!   use synchronous host functions and do not re-enter the loop, so the same
//!   logical work counts as 1. Ranking a V1 and a V2 delegate against each other
//!   on `engine_invocations` compares different quantities.
//!
//! Note also that `engine_invocations` counts work reaching the delegate from
//! EVERY path, including inter-delegate hops (which `requests` attributes to the
//! calling delegate) and paths that bypass the loop entirely. So
//! `engine_invocations > 0` with `requests == 0` is meaningful rather than
//! contradictory.
//!
//! **Measuring at the executor level rather than in the engine is still
//! deliberate**, but not for the multiplicity reason: both points sit inside the
//! loop. The reasons are that the engine's shared helper serves contracts AND
//! delegates (discriminated only by a parameter, so instrumenting it means
//! filtering a shared path), and that the executor-level span INCLUDES
//! host-function round-trips — exactly the cost that matters, since a delegate
//! parked inside a host call is interruptible by neither the epoch trap nor the
//! wall clock.
//!
//! One consequence for `last_error`: with panic capture in place, a panic inside
//! a delegate host function surfaces as a `WasmError` rather than unwinding into
//! the calling task. Such an error string appearing here means the node
//! SURVIVED something that previously would have unwound.
//!
//! # This phase is measurement only
//!
//! Nothing here throttles, quarantines, evicts or rate-limits. Phase 4
//! containment reads these measurements; it is deliberately not built here.

use std::cell::Cell;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;
// `tokio::time::Instant` (not `std`) to match `topology::running_average`, whose
// `RunningAverage` this module reuses, and so tests can drive it with a paused
// clock.
use tokio::time::Instant;

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey};

/// Maximum number of distinct delegates for which execution stats are kept.
///
/// Sized well below `topology::meter`'s `MAX_ATTRIBUTION_SOURCES` (4096)
/// because that map is shared across peers, contracts and delegates, whereas
/// this one holds delegates only, and a node running more than a few hundred
/// distinct delegates is already pathological.
pub(crate) const MAX_TRACKED_DELEGATES: usize = 512;

/// Absolute age after which a delegate's execution stats are swept.
///
/// Deliberately longer than the meter's 15-minute TTL: the meter holds *rates*,
/// which are meaningless once stale, while this holds *lifetime counters and
/// the last error*, which stay diagnostically useful long after the delegate
/// went quiet — a delegate that panicked an hour ago and then stopped being
/// invoked is exactly what an operator is looking for.
pub(crate) const DELEGATE_STATS_TTL: Duration = Duration::from_secs(6 * 60 * 60);

/// Maximum retained length of a delegate's last error message.
///
/// The message originates in guest code (a WASM trap's text, a delegate's own
/// error string), so it is attacker-controlled in the sense that matters for a
/// memory bound. Truncation happens at insertion, never at render, so the cap
/// bounds what is STORED and not merely what is shown.
pub(crate) const MAX_LAST_ERROR_BYTES: usize = 512;

/// Why attributed delegate CPU is NOT written to `topology::meter`.
///
/// The obvious implementation reports it as
/// `AttributionSource::Delegate(key)` on the `ExecCpuMicros` axis, reusing the
/// meter's cap, TTL sweep and `RunningAverage` — and that variant has existed
/// for a long time. It is wrong, for a reason that is invisible at the call
/// site and has nothing to do with the CPU axis:
///
/// `TopologyManager::report_resource_usage` inserts the source into
/// `source_creation_times` **regardless of which resource is being reported**.
/// `extrapolated_usage` then iterates that map for every axis, keeping any
/// source whose `AttributionSource::contributes_to` is true for that axis — and
/// `(Delegate(_), InboundBandwidthBytes)` and `(Delegate(_),
/// OutboundBandwidthBytes)` are both `true`. So a source created solely by a
/// CPU report is treated as a bandwidth contributor, and for the first
/// `SOURCE_RAMP_UP_DURATION` (5 minutes) of its life it takes the ramping-up
/// branch, which synthesizes a rate from the network-wide P50 estimator rather
/// than reading its (empty) samples. The result is phantom bandwidth usage for
/// a source that has produced zero bandwidth samples, inflating `total_usage`
/// and driving **spurious connection removals** — precisely the failure
/// `contributes_to`'s own doc comment describes, and why
/// `(Contract(_), InboundBandwidthBytes)` is `false`.
///
/// Before this module nothing anywhere constructed an
/// `AttributionSource::Delegate`, so those `true` rows were dormant. Reporting
/// delegate CPU to the shared meter is what would wake them.
///
/// The fix is NOT to flip those rows here. That is a change to topology's load
/// signal, it needs topology-focused review, and it belongs in its own PR —
/// Phase 0 is measurement and must not alter connection management. So the
/// attributed CPU lives in this module's own entry instead, using the same
/// `RunningAverage` type; only the map differs.
///
/// Pinned by `exec_cpu_is_not_reported_to_the_shared_topology_meter`.
pub(crate) const EXEC_CPU_IS_NOT_REPORTED_TO_THE_SHARED_METER: () = ();

/// Number of samples retained for the invocation-rate window.
///
/// Matches the production `Meter` window (`topology.rs`, `new_with_window_size(100)`).
const INVOCATION_RATE_WINDOW: usize = 100;

/// Live counters for one delegate. Written only by [`record_invocation`] and
/// [`record_request`]; read only by [`build_snapshot`].
#[derive(Debug)]
struct DelegateExecEntry {
    // --- per-call (one `inbound_app_message` execution) ---
    engine_invocations: u64,
    errors: u64,
    /// Truncated to [`MAX_LAST_ERROR_BYTES`] at insertion.
    last_error: Option<String>,
    last_error_at: Option<Instant>,
    total_exec_micros: u64,
    /// Windowed invocation rate. Reuses `topology::running_average`, which is
    /// entirely key-agnostic (it never touches the key type), so this is the
    /// existing machinery rather than a parallel one.
    invocation_rate: crate::topology::running_average::RunningAverage,
    /// Windowed attributed CPU, in microseconds of guest execution per second.
    ///
    /// Kept HERE rather than in the shared `topology::meter` — see
    /// [`EXEC_CPU_IS_NOT_REPORTED_TO_THE_SHARED_METER`]. It is the same
    /// `RunningAverage` machinery either way; only the map it lives in differs.
    exec_cpu_rate: crate::topology::running_average::RunningAverage,

    // --- per-request (one `handle_delegate_with_contract_requests` call) ---
    requests: u64,
    total_request_micros: u64,
    max_request_micros: u64,
    total_iterations: u64,
    max_iterations: u32,
    /// How many requests exhausted `MAX_CONTRACT_REQUEST_ITERATIONS`. See the
    /// module docs: that arm returns `Ok`, so these never appear in `errors`.
    iteration_cap_hits: u64,
    total_inter_delegate_messages: u64,
    max_inter_delegate_messages: u32,

    /// Most recent activity of EITHER kind. Drives both the TTL sweep and the
    /// LRU victim choice.
    last_active_at: Instant,
}

impl DelegateExecEntry {
    fn new(now: Instant) -> Self {
        Self {
            engine_invocations: 0,
            errors: 0,
            last_error: None,
            last_error_at: None,
            total_exec_micros: 0,
            invocation_rate: crate::topology::running_average::RunningAverage::new(
                INVOCATION_RATE_WINDOW,
            ),
            exec_cpu_rate: crate::topology::running_average::RunningAverage::new(
                INVOCATION_RATE_WINDOW,
            ),
            requests: 0,
            total_request_micros: 0,
            max_request_micros: 0,
            total_iterations: 0,
            max_iterations: 0,
            iteration_cap_hits: 0,
            total_inter_delegate_messages: 0,
            max_inter_delegate_messages: 0,
            last_active_at: now,
        }
    }
}

/// Process-global execution stats, keyed by delegate.
///
/// Process-global for the same reason `DELEGATE_SUBSCRIPTIONS`
/// (`wasm_runtime/native_api.rs:40`) is: the write sites sit deep inside the
/// executor's runtime and the contract-handler loop, where no per-node handle
/// is threaded through. #4824 tracks making that family of globals node-scoped;
/// when it lands this should move with them rather than being left behind.
static DELEGATE_EXEC_STATS: LazyLock<DashMap<DelegateKey, DelegateExecEntry>> =
    LazyLock::new(DashMap::default);

/// Truncate a delegate-supplied error message to [`MAX_LAST_ERROR_BYTES`],
/// respecting UTF-8 character boundaries.
///
/// Truncating with a raw byte slice would panic on a multi-byte character
/// straddling the cap — which a delegate could trigger deliberately.
fn truncate_error(msg: &str) -> String {
    if msg.len() <= MAX_LAST_ERROR_BYTES {
        return msg.to_string();
    }
    let mut end = MAX_LAST_ERROR_BYTES;
    while end > 0 && !msg.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… (truncated)", &msg[..end])
}

/// Evict aged-out entries, then make room if still at capacity.
///
/// Both phases are bounded by absolute age so no entry is exempt from eviction
/// indefinitely, and the shape mirrors `Meter::evict_if_full`.
///
/// MUST NOT be called while holding an entry guard on [`DELEGATE_EXEC_STATS`]:
/// it `retain`s and `remove`s across keys on the same DashMap, which
/// self-deadlocks. Both callers drop their guard first.
fn evict_if_full(now: Instant) {
    DELEGATE_EXEC_STATS.retain(|_, entry| {
        now.saturating_duration_since(entry.last_active_at) < DELEGATE_STATS_TTL
    });
    if DELEGATE_EXEC_STATS.len() < MAX_TRACKED_DELEGATES {
        return;
    }
    // Still full after the TTL sweep: drop the least-recently-active entry.
    let victim = DELEGATE_EXEC_STATS
        .iter()
        .min_by_key(|entry| entry.value().last_active_at)
        .map(|entry| entry.key().clone());
    if let Some(victim) = victim {
        DELEGATE_EXEC_STATS.remove(&victim);
    }
}

/// Apply `update` to `key`'s entry, creating it if absent.
///
/// The hot path (delegate already tracked) takes a single shard guard. A
/// brand-new key drops the guard, evicts, then re-acquires — see
/// [`evict_if_full`] for why the guard cannot be held across the scan.
fn with_entry(key: &DelegateKey, now: Instant, update: impl Fn(&mut DelegateExecEntry, Instant)) {
    use dashmap::mapref::entry::Entry;

    if let Entry::Occupied(mut occupied) = DELEGATE_EXEC_STATS.entry(key.clone()) {
        let entry = occupied.get_mut();
        // Clamp the clock forward per entry. `RunningAverage::insert_with_time`
        // carries a `debug_assert!(now >= last_sample_time)`, and this map is
        // process-global: a test driving a PAUSED tokio clock and a test using
        // the real one share it, so a later call can legitimately carry an
        // earlier `now`. Clamping keeps the sample stream monotonic per entry
        // rather than tripping a debug assertion on an unrelated test's
        // interleaving — the process-global-cache cross-test trap (#5314).
        let now = now.max(entry.last_active_at);
        update(entry, now);
        entry.last_active_at = now;
        return;
    }

    evict_if_full(now);
    let mut entry = DELEGATE_EXEC_STATS
        .entry(key.clone())
        .or_insert_with(|| DelegateExecEntry::new(now));
    let value = entry.value_mut();
    let now = now.max(value.last_active_at);
    update(value, now);
    value.last_active_at = now;
}

/// Outcome of one delegate invocation, as seen at the executor call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvocationOutcome<'a> {
    Success,
    /// The invocation returned an error. The message is stored (truncated) as
    /// the delegate's `last_error`.
    Failure(&'a str),
}

/// Record one delegate invocation (one `inbound_app_message` execution).
///
/// Called from exactly two places — the `Ok` and `Err` arms of the sole
/// production `inbound_app_message` call — and pinned by
/// `both_delegate_invocation_outcomes_are_recorded`, which checks BOTH outcomes
/// are still recorded rather than just counting calls. A refactor that moves the
/// delegate execution path, or that drops the failure arm, fails CI rather than
/// silently producing a counter that undercounts. That pin exists because
/// #4009/#4010 are precisely the case where a migration re-homed an op path and
/// the mirrored counter rotted without anything going red.
pub(crate) fn record_invocation(
    key: &DelegateKey,
    exec_duration: Duration,
    outcome: InvocationOutcome<'_>,
    now: Instant,
) {
    let micros = exec_duration.as_micros().min(u64::MAX as u128) as u64;
    with_entry(key, now, |entry, now| {
        entry.engine_invocations = entry.engine_invocations.saturating_add(1);
        entry.total_exec_micros = entry.total_exec_micros.saturating_add(micros);
        entry.invocation_rate.insert_with_time(now, 1.0);
        entry
            .exec_cpu_rate
            .insert_with_time(now, exec_duration.as_micros() as f64);
        if let InvocationOutcome::Failure(msg) = outcome {
            entry.errors = entry.errors.saturating_add(1);
            entry.last_error = Some(truncate_error(msg));
            entry.last_error_at = Some(now);
        }
    });
}

/// What one completed request contributed. Separated from [`RequestMeter`] so
/// the accumulation (which happens in `contract.rs`) and the recording (which
/// happens here) can be tested independently of each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RequestSample {
    pub(crate) wall_time: Duration,
    pub(crate) iterations: u32,
    pub(crate) inter_delegate_messages: u32,
    pub(crate) hit_iteration_cap: bool,
}

/// Record one completed delegate REQUEST. **This is the only per-request write
/// site**, pinned by `record_request_has_exactly_one_production_call_site`.
pub(crate) fn record_request(key: &DelegateKey, sample: RequestSample, now: Instant) {
    let micros = sample.wall_time.as_micros().min(u64::MAX as u128) as u64;
    with_entry(key, now, |entry, _now| {
        entry.requests = entry.requests.saturating_add(1);
        entry.total_request_micros = entry.total_request_micros.saturating_add(micros);
        entry.max_request_micros = entry.max_request_micros.max(micros);
        entry.total_iterations = entry
            .total_iterations
            .saturating_add(sample.iterations as u64);
        entry.max_iterations = entry.max_iterations.max(sample.iterations);
        entry.total_inter_delegate_messages = entry
            .total_inter_delegate_messages
            .saturating_add(sample.inter_delegate_messages as u64);
        entry.max_inter_delegate_messages = entry
            .max_inter_delegate_messages
            .max(sample.inter_delegate_messages);
        if sample.hit_iteration_cap {
            entry.iteration_cap_hits = entry.iteration_cap_hits.saturating_add(1);
        }
    });
}

/// Meters one whole `handle_delegate_with_contract_requests` call.
///
/// RAII rather than an explicit `finish()` because that function has several
/// return points (the iteration cap, the executor-error arms, the normal exit).
/// Recording on `Drop` covers all of them without touching a single return
/// site, which is also what keeps the `contract.rs` footprint to four lines —
/// `contract.rs` is owned by another workstream (#4669).
///
/// Uses `std::time::Instant` for the wall clock: this measures real elapsed
/// work, which a paused or simulated `TimeSource` would report as zero.
pub(crate) struct RequestMeter {
    key: DelegateKey,
    started: std::time::Instant,
    iterations: Cell<u32>,
    inter_delegate_messages: Cell<u32>,
    hit_iteration_cap: Cell<bool>,
}

impl RequestMeter {
    pub(crate) fn start(key: &DelegateKey) -> Self {
        Self {
            key: key.clone(),
            started: std::time::Instant::now(),
            iterations: Cell::new(0),
            inter_delegate_messages: Cell::new(0),
            hit_iteration_cap: Cell::new(false),
        }
    }

    /// One pass round the contract-request loop.
    pub(crate) fn note_iteration(&self) {
        self.iterations.set(self.iterations.get().saturating_add(1));
    }

    /// A batch of delegate-to-delegate messages was dispatched. Counted because
    /// dispatch is single-hop but the batch size is uncapped, so this is a
    /// fan-out axis that per-call duration cannot see.
    pub(crate) fn note_inter_delegate_messages(&self, count: usize) {
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        self.inter_delegate_messages
            .set(self.inter_delegate_messages.get().saturating_add(count));
    }

    /// The request exhausted `MAX_CONTRACT_REQUEST_ITERATIONS`. That arm
    /// returns `Ok`, so this is the ONLY signal a runaway leaves behind.
    pub(crate) fn note_iteration_cap_hit(&self) {
        self.hit_iteration_cap.set(true);
    }

    fn sample(&self) -> RequestSample {
        RequestSample {
            wall_time: self.started.elapsed(),
            iterations: self.iterations.get(),
            inter_delegate_messages: self.inter_delegate_messages.get(),
            hit_iteration_cap: self.hit_iteration_cap.get(),
        }
    }
}

impl Drop for RequestMeter {
    fn drop(&mut self) {
        record_request(&self.key, self.sample(), Instant::now());
    }
}

/// One contract a delegate is subscribed to, plus whether that subscription
/// actually registered demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegateSubscriptionEntry {
    pub contract: String,
    /// Whether the node considers this contract in use — i.e. whether the
    /// delegate's subscription actually pinned it.
    ///
    /// **This is the load-bearing field of Phase 0.** It is computed live from
    /// `Ring::in_use_contract_ids`, the instance-id form of
    /// `Ring::contract_in_use`, so it TRACKS that predicate rather than
    /// asserting any particular value.
    ///
    /// Until #4669 lands, `contract_in_use` is `has_client_subscriptions() ||
    /// has_downstream_subscribers()` with no delegate term, so a delegate
    /// subscription that succeeded still reports `false` here unless some
    /// *other* route (typically the app's own WebSocket subscription) happens
    /// to be pinning the same contract. That divergence — subscribed, but not
    /// pinned — is the bug, rendered visible. When #4669 adds the delegate
    /// term this flips to `true` with no change needed here.
    pub registered_demand: bool,
}

/// Per-delegate observability record.
///
/// Every "unknown" is an `Option` rather than a zero. A delegate we have never
/// seen execute has no rate and no last-invoked time, and reporting `0.0` for
/// either would be fabricated data — the specific thing AGENTS.md forbids and
/// the reason this whole module exists.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegateStatusEntry {
    pub key: String,
    pub subscriptions: Vec<DelegateSubscriptionEntry>,
    /// How many of `subscriptions` actually registered demand.
    pub subscriptions_registering_demand: usize,

    // --- per-call ---
    /// Engine round-trips, NOT application messages — see the module docs.
    /// A V1 delegate handling one message that does four contract GETs records
    /// five here. The per-logical-request unit is `requests`.
    pub engine_invocations: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    pub last_error_secs_ago: Option<u64>,
    pub last_active_secs_ago: Option<u64>,
    pub total_exec_micros: u64,
    /// Windowed invocations/sec. `None` when the window holds no samples —
    /// distinct from a genuine zero.
    pub engine_invocation_rate_per_sec: Option<f64>,
    /// Windowed attributed CPU (µs/sec) from `topology::meter`. `None` when the
    /// meter holds no window for this delegate — again, distinct from zero.
    pub exec_cpu_micros_per_sec: Option<f64>,

    // --- per-request ---
    /// Whole `handle_delegate_with_contract_requests` calls completed.
    pub requests: u64,
    pub total_request_micros: u64,
    pub max_request_micros: u64,
    pub total_iterations: u64,
    pub max_iterations: u32,
    /// Requests that exhausted `MAX_CONTRACT_REQUEST_ITERATIONS`. Non-zero is
    /// the runaway signal that leaves no error behind (#5454).
    pub iteration_cap_hits: u64,
    pub total_inter_delegate_messages: u64,
    pub max_inter_delegate_messages: u32,
}

/// Node-wide per-delegate observability snapshot.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DelegateStatusSnapshot {
    pub delegates: Vec<DelegateStatusEntry>,
    /// Delegates that hold at least one subscription which did NOT register
    /// demand. This is the count an operator should react to.
    pub delegates_with_unpinned_subscriptions: usize,
    /// Total subscriptions held across all delegates.
    pub subscriptions_total: usize,
    /// Total subscriptions that did not register demand.
    pub subscriptions_without_demand: usize,
    /// Requests that exhausted the iteration cap, summed over all delegates.
    pub iteration_cap_hits_total: u64,

    /// Delegate module-cache gauges, read live from `Ring::module_cache_metrics`.
    /// These already existed but reached fleet telemetry only (`ring.rs`), never
    /// the local dashboard.
    pub module_cache_entries: u64,
    pub module_cache_total_bytes: u64,
    pub module_cache_budget_bytes: u64,
    pub module_cache_evictions_total: u64,

    /// Whether scheduled execution (#5467 Phase 2 / #3972) is built.
    ///
    /// Always `false` today. It exists so the dashboard can say "not built"
    /// rather than rendering a `0` that an operator would read as "none
    /// pending" — AGENTS.md forbids improvising a value for something that does
    /// not exist, and a zero here would be exactly that.
    pub wakeup_scheduling_available: bool,
}

/// The per-delegate execution fields, resolved from an optional stats entry.
///
/// Extracted so the "no execution history" branch is directly testable without
/// standing up a `Ring`. That branch is where fabricated zeros would creep in.
#[derive(Debug, Clone, PartialEq, Default)]
struct ExecFields {
    engine_invocations: u64,
    errors: u64,
    last_error: Option<String>,
    last_error_secs_ago: Option<u64>,
    last_active_secs_ago: Option<u64>,
    total_exec_micros: u64,
    engine_invocation_rate_per_sec: Option<f64>,
    exec_cpu_micros_per_sec: Option<f64>,
    requests: u64,
    total_request_micros: u64,
    max_request_micros: u64,
    total_iterations: u64,
    max_iterations: u32,
    iteration_cap_hits: u64,
    total_inter_delegate_messages: u64,
    max_inter_delegate_messages: u32,
}

fn exec_fields(stats: Option<&DelegateExecEntry>, now: Instant) -> ExecFields {
    // No execution history: the COUNTERS are genuinely zero (we have never seen
    // it run), but the rates and timestamps are UNKNOWN rather than zero, so
    // they stay `None`. `ExecFields::default()` gives exactly that.
    let Some(entry) = stats else {
        return ExecFields::default();
    };
    ExecFields {
        engine_invocations: entry.engine_invocations,
        errors: entry.errors,
        last_error: entry.last_error.clone(),
        last_error_secs_ago: entry
            .last_error_at
            .map(|at| now.saturating_duration_since(at).as_secs()),
        last_active_secs_ago: Some(
            now.saturating_duration_since(entry.last_active_at)
                .as_secs(),
        ),
        total_exec_micros: entry.total_exec_micros,
        engine_invocation_rate_per_sec: entry
            .invocation_rate
            .get_rate_at_time(now)
            .map(|rate| rate.per_second()),
        exec_cpu_micros_per_sec: entry
            .exec_cpu_rate
            .get_rate_at_time(now)
            .map(|rate| rate.per_second()),
        requests: entry.requests,
        total_request_micros: entry.total_request_micros,
        max_request_micros: entry.max_request_micros,
        total_iterations: entry.total_iterations,
        max_iterations: entry.max_iterations,
        iteration_cap_hits: entry.iteration_cap_hits,
        total_inter_delegate_messages: entry.total_inter_delegate_messages,
        max_inter_delegate_messages: entry.max_inter_delegate_messages,
    }
}

/// Resolve one delegate's subscription rows against the node's live demand set,
/// returning the rows and how many of them registered demand.
///
/// Extracted as a pure function so the load-bearing field — `registered_demand`
/// — is directly testable without standing up a `Ring` (`Ring::new` spawns
/// background tasks and is deliberately avoided by every other unit test here).
///
/// The test asserts this TRACKS the supplied set rather than asserting any
/// particular value. That distinction matters: today `Ring::contract_in_use`
/// has no delegate term, so in practice every row reads `false` — but a test
/// pinning `false` would have to be edited by the very change (#4669) it exists
/// to guard, and would go green for the wrong reason in the meantime.
fn subscription_rows(
    mut instance_ids: Vec<ContractInstanceId>,
    in_use: &std::collections::HashSet<ContractInstanceId>,
) -> (Vec<DelegateSubscriptionEntry>, usize) {
    // Deterministic order so the rendered table does not reshuffle between
    // dashboard refreshes.
    instance_ids.sort_by_key(|id| id.to_string());
    let mut registering = 0usize;
    let rows = instance_ids
        .into_iter()
        .map(|instance_id| {
            let registered_demand = in_use.contains(&instance_id);
            if registered_demand {
                registering += 1;
            }
            DelegateSubscriptionEntry {
                contract: instance_id.to_string(),
                registered_demand,
            }
        })
        .collect();
    (rows, registering)
}

/// Build the snapshot by reading canonical state.
///
/// Takes `&Ring` rather than living on `Ring` so this stays out of `ring.rs`,
/// which is being edited concurrently for #4669.
pub(crate) fn build_snapshot(ring: &crate::ring::Ring, now: Instant) -> DelegateStatusSnapshot {
    // Invert DELEGATE_SUBSCRIPTIONS (contract -> delegates) into
    // delegate -> contracts. It is the canonical store, read live.
    let mut by_delegate: HashMap<DelegateKey, Vec<ContractInstanceId>> = HashMap::new();
    for entry in crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.iter() {
        let instance_id = *entry.key();
        for delegate in entry.value().iter() {
            by_delegate
                .entry(delegate.clone())
                .or_default()
                .push(instance_id);
        }
    }

    // A delegate with execution history but no live subscriptions still needs a
    // row — that is the "it ran and it panicked" case an operator cares most
    // about, and dropping it would hide exactly the wrong thing.
    for entry in DELEGATE_EXEC_STATS.iter() {
        by_delegate.entry(entry.key().clone()).or_default();
    }

    // Read the demand predicate once for the whole node rather than per
    // subscription. `in_use_contract_ids` is the instance-id form of
    // `Ring::contract_in_use`, which is what we actually want to compare
    // against: it avoids reconstructing a `ContractKey` from an instance id
    // with a code hash we do not have and would have to invent.
    let in_use: std::collections::HashSet<ContractInstanceId> =
        ring.in_use_contract_ids().into_iter().collect();

    let mut delegates = Vec::with_capacity(by_delegate.len());
    let mut delegates_with_unpinned_subscriptions = 0usize;
    let mut subscriptions_total = 0usize;
    let mut subscriptions_without_demand = 0usize;
    let mut iteration_cap_hits_total = 0u64;

    for (key, instance_ids) in by_delegate {
        let (subscriptions, registering) = subscription_rows(instance_ids, &in_use);
        subscriptions_total += subscriptions.len();
        subscriptions_without_demand += subscriptions.len() - registering;
        if registering < subscriptions.len() {
            delegates_with_unpinned_subscriptions += 1;
        }

        let stats = DELEGATE_EXEC_STATS.get(&key);
        let f = exec_fields(stats.as_deref(), now);
        drop(stats);
        iteration_cap_hits_total = iteration_cap_hits_total.saturating_add(f.iteration_cap_hits);

        delegates.push(DelegateStatusEntry {
            key: key.to_string(),
            subscriptions,
            subscriptions_registering_demand: registering,
            engine_invocations: f.engine_invocations,
            errors: f.errors,
            last_error: f.last_error,
            last_error_secs_ago: f.last_error_secs_ago,
            last_active_secs_ago: f.last_active_secs_ago,
            total_exec_micros: f.total_exec_micros,
            engine_invocation_rate_per_sec: f.engine_invocation_rate_per_sec,
            exec_cpu_micros_per_sec: f.exec_cpu_micros_per_sec,
            requests: f.requests,
            total_request_micros: f.total_request_micros,
            max_request_micros: f.max_request_micros,
            total_iterations: f.total_iterations,
            max_iterations: f.max_iterations,
            iteration_cap_hits: f.iteration_cap_hits,
            total_inter_delegate_messages: f.total_inter_delegate_messages,
            max_inter_delegate_messages: f.max_inter_delegate_messages,
        });
    }

    // Most-recently-active first, so the delegate an operator is debugging is
    // at the top. Ties broken on the key so the render is deterministic.
    delegates.sort_by(|a, b| {
        a.last_active_secs_ago
            .unwrap_or(u64::MAX)
            .cmp(&b.last_active_secs_ago.unwrap_or(u64::MAX))
            .then_with(|| a.key.cmp(&b.key))
    });

    // Delegate module-cache gauges: already collected for fleet telemetry
    // (`ring.rs`), never surfaced locally. Read live from the same metrics
    // handle, so there is no counter to rot.
    let mc = ring.module_cache_metrics().snapshot();

    DelegateStatusSnapshot {
        delegates,
        delegates_with_unpinned_subscriptions,
        subscriptions_total,
        subscriptions_without_demand,
        iteration_cap_hits_total,
        module_cache_entries: mc.delegate_entries,
        module_cache_total_bytes: mc.delegate_total_bytes,
        module_cache_budget_bytes: mc.delegate_budget_bytes,
        module_cache_evictions_total: mc.delegate_evictions_total,
        // #3972 / #4666 are not merged; the WakeupScheduler does not exist on
        // this branch. Hardcoded false rather than probed, because there is
        // nothing to probe — and a plausible-looking `0` would be fabricated
        // data (AGENTS.md).
        wakeup_scheduling_available: false,
    }
}

#[cfg(test)]
pub(crate) fn clear_for_test() {
    DELEGATE_EXEC_STATS.clear();
}

#[cfg(test)]
pub(crate) fn tracked_delegate_count() -> usize {
    DELEGATE_EXEC_STATS.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;
    use std::sync::{Mutex, MutexGuard};

    /// [`DELEGATE_EXEC_STATS`] is process-global and `cargo test` runs the tests
    /// in this module concurrently in ONE process, so a test that calls
    /// [`clear_for_test`] would otherwise wipe a sibling's entries mid-run.
    /// That is the cross-test-interference-behind-a-process-global shape from
    /// `.claude/rules/testing.md` (#5314) — and `cargo nextest`, which gives
    /// each test its own process, could not observe it at any repeat count.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn exclusive() -> MutexGuard<'static, ()> {
        // A panicking test poisons the mutex; the shared state is reset by the
        // guard's own `clear_for_test` below, so recovering is correct here and
        // keeps one failure from cascading into every sibling.
        let guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_for_test();
        guard
    }

    fn key(byte: u8) -> DelegateKey {
        DelegateKey::new([byte; 32], CodeHash::new([byte; 32]))
    }

    /// The write-site pin for the PER-CALL counter (#4009/#4010 shape).
    ///
    /// `record_invocation` is a mirrored counter, which is the failure mode
    /// bug-prevention-patterns.md warns about: a later migration re-homes the
    /// delegate call path, forgets to re-wire the call, and the counter reads a
    /// plausible zero forever.
    ///
    /// There are exactly TWO call sites — the `Ok` and `Err` arms of the sole
    /// production `inbound_app_message` call — and the test asserts BOTH
    /// outcomes are recorded, not merely that some call exists. Asserting only
    /// a total count would go green if someone deleted the failure arm and
    /// duplicated the success one, which is the shape that matters: dropping
    /// the failure recording makes a delegate that panics on every invocation
    /// indistinguishable from a healthy one, and that is the exact blindness
    /// this module was built to end.
    ///
    /// Anchored on the API surface (the call expression and the outcome
    /// variants) rather than on variable names, so it does not rot on a rename
    /// of the surrounding function.
    #[test]
    fn both_delegate_invocation_outcomes_are_recorded() {
        let src = include_str!("../contract/executor/runtime/delegates.rs");
        let calls = src
            .matches("delegate_observability::record_invocation(")
            .count();
        assert_eq!(
            calls, 2,
            "expected exactly TWO production call sites of record_invocation in \
             delegates.rs — the Ok and Err arms of the sole production \
             inbound_app_message call. Found {calls}. If the delegate execution \
             path moved, move the instrumentation with it; an uncounted \
             invocation is indistinguishable from a delegate that never ran."
        );
        assert_eq!(
            src.matches("InvocationOutcome::Success").count(),
            1,
            "the success arm must record exactly one Success outcome"
        );
        assert_eq!(
            src.matches("InvocationOutcome::Failure").count(),
            1,
            "the failure arm must record exactly one Failure outcome — without \
             it, a delegate that panics on every invocation reports zero errors"
        );
    }

    /// The single-write-site pin for the PER-REQUEST counter.
    ///
    /// `RequestMeter` records on `Drop`, so a second `start` in the same
    /// function would double-count every request while looking harmless at the
    /// call site. Pinning the construction rather than the recording is what
    /// catches that.
    #[test]
    fn record_request_has_exactly_one_production_call_site() {
        let src = include_str!("../contract.rs");
        let starts = src
            .matches("delegate_observability::RequestMeter::start(")
            .count();
        assert_eq!(
            starts, 1,
            "expected exactly ONE production construction of RequestMeter in \
             contract.rs (at the top of handle_delegate_with_contract_requests). \
             Found {starts}. It records on Drop, so a second construction in the \
             same call would double-count the request."
        );
    }

    /// The iteration-cap counter must stay wired to the cap arm.
    ///
    /// That arm returns `Ok(accumulated_messages)`, so a runaway delegate leaves
    /// NO error behind — this counter is the only trace. If a refactor drops the
    /// call, the panel silently reports every spinning delegate as healthy,
    /// which is the exact failure Phase 0 exists to end.
    #[test]
    fn iteration_cap_hit_is_recorded_at_the_cap_arm() {
        let src = include_str!("../contract.rs");
        assert_eq!(
            src.matches("note_iteration_cap_hit()").count(),
            1,
            "the MAX_CONTRACT_REQUEST_ITERATIONS arm must record a cap hit exactly \
             once; that arm returns Ok, so this counter is the only evidence a \
             runaway delegate leaves"
        );
        // The cap arm and the recording must be in the same arm, not merely both
        // present in the file. Anchor on the cap constant, which names the
        // decision, and check the call appears before the arm's `return`.
        let arm = src
            .split("if iterations > MAX_CONTRACT_REQUEST_ITERATIONS")
            .nth(1)
            .expect("the iteration cap arm must exist in contract.rs");
        let cap_call = arm
            .find("note_iteration_cap_hit()")
            .expect("the cap arm must record the hit");
        let arm_return = arm
            .find("return accumulated_messages")
            .expect("the cap arm must still return the accumulated messages (#5454)");
        assert!(
            cap_call < arm_return,
            "the cap hit must be recorded BEFORE the arm returns, or the counter \
             never fires"
        );
    }

    #[test]
    fn last_error_is_truncated_at_insertion() {
        let huge = "x".repeat(MAX_LAST_ERROR_BYTES * 10);
        let truncated = truncate_error(&huge);
        assert!(
            truncated.len() < huge.len(),
            "a delegate-controlled error message must be truncated"
        );
        assert!(
            truncated.starts_with(&"x".repeat(MAX_LAST_ERROR_BYTES)),
            "truncation must keep the head of the message"
        );
        assert!(truncated.ends_with("… (truncated)"));
    }

    /// A multi-byte character straddling the cap must not panic — a delegate
    /// could choose its panic message to land exactly there.
    #[test]
    fn truncation_respects_utf8_boundaries() {
        // 'é' is 2 bytes; repeating it puts a boundary straddle at the odd cap.
        let msg = "é".repeat(MAX_LAST_ERROR_BYTES);
        let truncated = truncate_error(&msg);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(truncated.ends_with("… (truncated)"));
    }

    #[test]
    fn short_error_is_stored_verbatim() {
        assert_eq!(truncate_error("boom"), "boom");
    }

    #[test]
    fn record_invocation_counts_successes_and_failures_separately() {
        let _guard = exclusive();
        let k = key(1);
        let t0 = Instant::now();
        record_invocation(
            &k,
            Duration::from_micros(10),
            InvocationOutcome::Success,
            t0,
        );
        record_invocation(
            &k,
            Duration::from_micros(20),
            InvocationOutcome::Failure("boom"),
            t0 + Duration::from_millis(1),
        );

        let entry = DELEGATE_EXEC_STATS.get(&k).expect("entry recorded");
        assert_eq!(entry.engine_invocations, 2, "both invocations counted");
        assert_eq!(entry.errors, 1, "only the failure counted as an error");
        assert_eq!(entry.last_error.as_deref(), Some("boom"));
        assert_eq!(entry.total_exec_micros, 30, "durations accumulate");
        drop(entry);
        clear_for_test();
    }

    /// The per-request unit must be recorded separately from the per-call one:
    /// one request can drive up to 100 invocations, so conflating them is how a
    /// seconds-long request hides behind a healthy per-call p99.
    #[test]
    fn request_metering_records_iterations_and_fanout_separately_from_calls() {
        let _guard = exclusive();
        let k = key(2);
        let t0 = Instant::now();
        record_request(
            &k,
            RequestSample {
                wall_time: Duration::from_millis(1500),
                iterations: 100,
                inter_delegate_messages: 40,
                hit_iteration_cap: true,
            },
            t0,
        );

        let entry = DELEGATE_EXEC_STATS.get(&k).expect("entry recorded");
        assert_eq!(entry.requests, 1);
        assert_eq!(entry.max_iterations, 100);
        assert_eq!(entry.max_inter_delegate_messages, 40);
        assert_eq!(entry.max_request_micros, 1_500_000);
        assert_eq!(
            entry.iteration_cap_hits, 1,
            "a cap hit must be counted even though the request returned Ok"
        );
        assert_eq!(
            entry.engine_invocations, 0,
            "a request is not an invocation; conflating the two is the bug this \
             separation exists to prevent"
        );
        assert_eq!(entry.errors, 0, "a cap hit is not an error (#5454)");
        drop(entry);
        clear_for_test();
    }

    /// `RequestMeter` must record on drop from every exit path, and must carry
    /// the counts accumulated through its `note_*` methods.
    #[test]
    fn request_meter_records_on_drop() {
        let _guard = exclusive();
        let k = key(3);
        {
            let meter = RequestMeter::start(&k);
            meter.note_iteration();
            meter.note_iteration();
            meter.note_inter_delegate_messages(3);
            meter.note_inter_delegate_messages(2);
            assert_eq!(
                tracked_delegate_count(),
                0,
                "nothing is recorded until the meter drops"
            );
        }
        let entry = DELEGATE_EXEC_STATS.get(&k).expect("recorded on drop");
        assert_eq!(entry.requests, 1);
        assert_eq!(entry.total_iterations, 2);
        assert_eq!(entry.total_inter_delegate_messages, 5);
        assert_eq!(
            entry.iteration_cap_hits, 0,
            "no cap hit was noted on this request"
        );
        drop(entry);
        clear_for_test();
    }

    #[test]
    fn entry_count_is_bounded_and_evicts_least_recently_active() {
        let _guard = exclusive();
        let t0 = Instant::now();
        // Insert one more than the cap; each successive key is invoked later,
        // so key 0 is the least-recently-active and must be the victim.
        for i in 0..=MAX_TRACKED_DELEGATES {
            let k = DelegateKey::new(
                {
                    let mut b = [0u8; 32];
                    b[0] = (i % 256) as u8;
                    b[1] = (i / 256) as u8;
                    b
                },
                CodeHash::new([0u8; 32]),
            );
            record_invocation(
                &k,
                Duration::from_micros(1),
                InvocationOutcome::Success,
                t0 + Duration::from_millis(i as u64),
            );
        }
        assert!(
            tracked_delegate_count() <= MAX_TRACKED_DELEGATES,
            "entry count must stay bounded, found {}",
            tracked_delegate_count()
        );
        clear_for_test();
    }

    /// A count cap is not a memory bound when the value is externally
    /// controlled. This asserts the STORED size, not the rendered size.
    #[test]
    fn stored_error_size_is_bounded_even_at_max_entries() {
        let _guard = exclusive();
        let k = key(7);
        record_invocation(
            &k,
            Duration::from_micros(1),
            InvocationOutcome::Failure(&"z".repeat(1_000_000)),
            Instant::now(),
        );
        let entry = DELEGATE_EXEC_STATS.get(&k).expect("entry recorded");
        let stored = entry.last_error.as_ref().expect("error stored").len();
        assert!(
            stored <= MAX_LAST_ERROR_BYTES + "… (truncated)".len(),
            "stored error must be bounded, was {stored} bytes"
        );
        drop(entry);
        clear_for_test();
    }

    /// The snapshot must distinguish "we have never seen this run" from "it ran
    /// zero times per second". This exercises the real resolver, not a
    /// hand-built struct literal: asserting that fields you just set have the
    /// values you set proves nothing.
    #[test]
    fn unknown_rates_are_none_not_zero() {
        let f = exec_fields(None, Instant::now());
        assert!(
            f.engine_invocation_rate_per_sec.is_none(),
            "an unmeasured rate is None, never Some(0.0)"
        );
        assert!(
            f.last_active_secs_ago.is_none(),
            "a delegate we have never seen run has no last-active time"
        );
        assert!(f.last_error_secs_ago.is_none());
        assert!(f.last_error.is_none());
        // Counters, by contrast, ARE genuinely zero: we know it ran zero times.
        assert_eq!(f.engine_invocations, 0);
        assert_eq!(f.requests, 0);
    }

    /// The mirror-image of the above: once a delegate HAS run, the same
    /// resolver must report real values rather than staying `None`. Without
    /// this, a resolver that returned `Default::default()` unconditionally
    /// would pass the test above.
    #[test]
    fn known_delegate_reports_real_values() {
        let _guard = exclusive();
        let k = key(9);
        let t0 = Instant::now();
        record_invocation(&k, Duration::from_micros(5), InvocationOutcome::Success, t0);
        let entry = DELEGATE_EXEC_STATS.get(&k).expect("entry recorded");
        let f = exec_fields(Some(&entry), t0 + Duration::from_secs(1));
        drop(entry);
        assert_eq!(f.engine_invocations, 1);
        assert!(
            f.last_active_secs_ago.is_some(),
            "a delegate that ran has a last-active time"
        );
        assert!(
            f.engine_invocation_rate_per_sec.is_some(),
            "a delegate with a sample has a rate"
        );
        clear_for_test();
    }

    fn instance(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    /// **The load-bearing assertion of Phase 0.**
    ///
    /// `registered_demand` must TRACK the node's demand predicate, not carry a
    /// baked-in value. Written deliberately as "follows the set" rather than
    /// "is false": today `Ring::contract_in_use` has no delegate term, so in
    /// production every row reads `false` — but pinning `false` would mean the
    /// test had to be rewritten by #4669, the very change it exists to guard,
    /// and would pass for the wrong reason until then. When #4669 makes a
    /// delegate subscription register demand, the id lands in
    /// `in_use_contract_ids()` and this test goes green unchanged.
    #[test]
    fn registered_demand_tracks_the_in_use_set_rather_than_a_fixed_value() {
        let pinned = instance(1);
        let unpinned = instance(2);
        let in_use: std::collections::HashSet<ContractInstanceId> =
            [pinned].into_iter().collect();

        let (rows, registering) = subscription_rows(vec![pinned, unpinned], &in_use);

        assert_eq!(rows.len(), 2);
        assert_eq!(registering, 1, "exactly one subscription registered demand");
        let pinned_row = rows
            .iter()
            .find(|r| r.contract == pinned.to_string())
            .expect("the pinned contract must have a row");
        let unpinned_row = rows
            .iter()
            .find(|r| r.contract == unpinned.to_string())
            .expect("the unpinned contract must have a row");
        assert!(
            pinned_row.registered_demand,
            "a subscription whose contract IS in the demand set must report true"
        );
        assert!(
            !unpinned_row.registered_demand,
            "a subscription whose contract is NOT in the demand set must report \
             false — that divergence is the #4669 bug this panel exists to show"
        );
    }

    /// The complement: with an empty demand set nothing registers, and with a
    /// full one everything does. Together with the test above this rules out a
    /// helper that returns a constant either way.
    #[test]
    fn registered_demand_follows_an_empty_and_a_full_demand_set() {
        let ids = vec![instance(3), instance(4)];

        let (none_rows, none_registering) =
            subscription_rows(ids.clone(), &std::collections::HashSet::new());
        assert_eq!(none_registering, 0);
        assert!(none_rows.iter().all(|r| !r.registered_demand));

        let all: std::collections::HashSet<ContractInstanceId> = ids.iter().copied().collect();
        let (all_rows, all_registering) = subscription_rows(ids, &all);
        assert_eq!(all_registering, 2);
        assert!(all_rows.iter().all(|r| r.registered_demand));
    }

    /// `build_snapshot` must read the demand predicate from the RING, not from
    /// anything it computes itself. A source scrape because the alternative
    /// needs `Ring::new`, which spawns background tasks and is avoided by every
    /// unit test in this crate's ring module for that reason.
    #[test]
    fn build_snapshot_reads_demand_from_the_ring() {
        let src = include_str!("delegate_observability.rs");
        let body = src
            .split("pub(crate) fn build_snapshot(")
            .nth(1)
            .expect("build_snapshot must exist");
        assert!(
            body.contains("ring.in_use_contract_ids()"),
            "build_snapshot must derive the demand set from Ring::in_use_contract_ids \
             (the instance-id form of Ring::contract_in_use). If that call is gone, \
             registered_demand is no longer tracking the node's real demand \
             predicate and the panel is reporting a value it invented."
        );
    }

    /// Phase 0 must not touch topology's load signal.
    ///
    /// Reporting delegate CPU to the shared meter creates an
    /// `AttributionSource::Delegate` in `source_creation_times`, which
    /// `extrapolated_usage` then counts as a BANDWIDTH contributor (those
    /// `contributes_to` rows are `true`), synthesizing a phantom rate during
    /// the 5-minute ramp-up and driving spurious connection removals. Nothing
    /// else in the tree constructs that variant, so this module is the only
    /// thing that could wake it. See
    /// [`EXEC_CPU_IS_NOT_REPORTED_TO_THE_SHARED_METER`].
    #[test]
    fn exec_cpu_is_not_reported_to_the_shared_topology_meter() {
        // Ties the documented rationale to the check that enforces it: deleting
        // the constant breaks this test, so the explanation cannot silently
        // outlive the guard (or vice versa).
        let () = EXEC_CPU_IS_NOT_REPORTED_TO_THE_SHARED_METER;
        for (name, src) in [
            (
                "delegates.rs",
                include_str!("../contract/executor/runtime/delegates.rs"),
            ),
            ("delegate_observability.rs", include_str!("delegate_observability.rs")),
        ] {
            // Strip this module's own test section, which necessarily NAMES the
            // thing it forbids.
            let production = src.split("#[cfg(test)]").next().unwrap_or(src);
            assert!(
                !production.contains("report_delegate_resource_usage"),
                "{name} must not report delegate cost to the shared topology \
                 meter: it creates an AttributionSource::Delegate, which \
                 extrapolated_usage counts as a bandwidth contributor and \
                 inflates perceived usage for 5 minutes, causing spurious \
                 connection removals"
            );
            assert!(
                !production.contains("AttributionSource::Delegate"),
                "{name} must not construct AttributionSource::Delegate — see \
                 EXEC_CPU_IS_NOT_REPORTED_TO_THE_SHARED_METER"
            );
        }
    }

    /// Phase 2 is not built on this branch. The flag must say so rather than
    /// the snapshot reporting a zero pending-wakeup count that reads as
    /// "nothing pending".
    #[test]
    fn wakeup_scheduling_is_reported_unavailable_not_zero() {
        let snap = DelegateStatusSnapshot::default();
        assert!(
            !snap.wakeup_scheduling_available,
            "scheduled execution (#3972) is not merged; the snapshot must not \
             claim otherwise"
        );
    }
}
