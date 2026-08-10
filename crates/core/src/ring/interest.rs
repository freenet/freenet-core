//! Unified interest tracking for delta-based state synchronization.
//!
//! NOTE: This module provides foundation infrastructure for delta-based updates.
//! Many items are marked `#[allow(dead_code)]` because they will be used in
//! follow-up PRs that integrate the full delta sync workflow.
#![allow(dead_code)]
//!
//! This module provides the infrastructure for tracking which peers are interested
//! in which contracts, along with their state summaries. This enables delta-based
//! updates where we send only the changes rather than full contract state.
//!
//! # Core Concepts
//!
//! ## Interest vs Subscribe
//!
//! - **Interest** (neighbor-scoped): "Update me if you have it"
//!   - Exchanged between directly connected peers
//!   - No network propagation if peer doesn't have state
//!   - Used for proximity-style coordination
//!
//! - **Subscribe** (network-scoped): "Update me, and subscribe upstream if needed"
//!   - May propagate through the network
//!   - Establishes subscription tree
//!   - Used when client explicitly requests a contract
//!
//! Both result in summary exchange for delta computation. The update/delta mechanism
//! doesn't care WHY a peer is interested - only which peers want updates and their
//! current state summaries.
//!
//! ## Interest Lifecycle
//!
//! Interests expire after a TTL (20 minutes) unless refreshed. A background
//! heartbeat task sends `Interests { hashes }` to each connected peer every
//! 5 minutes, which refreshes the TTL. The TTL is 4x the heartbeat interval
//! to tolerate up to 3 consecutive missed heartbeats before expiry.
//!
//! Additional refresh triggers:
//! - Sending/receiving updates
//! - Summaries exchange (the TTL refresh rides `PeerInterest::set_summary`,
//!   so it happens wherever a summary is stored, not as a separate step)
//! - Receiving `ChangeInterests { added }`
//!
//! Caveat for the hash-first exchange (#4965): a digest that AGREES stores the
//! summary and therefore refreshes the TTL as usual, but a digest that needs
//! bytes DEFERS both by one round trip — the refresh lands when the requested
//! `Summaries` arrives. If that request or its reply is lost, the refresh is
//! lost for the cycle too. Harmless at a 4x-heartbeat TTL (three consecutive
//! losses are tolerated), but worth knowing before assuming every digest
//! exchange refreshes.
//!
//! This self-healing mechanism catches forgotten cleanup and prevents zombie interests.

use dashmap::DashMap;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, StateDelta, StateSummary};
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use crate::ring::futile_repair::{FutileRepairDetector, FutileRepairSnapshot};
use crate::transport::TransportPublicKey;
use crate::util::time_source::TimeSource;

/// Interval between interest heartbeat messages sent to each peer.
/// Each heartbeat sends a full `Interests { hashes }` message which refreshes
/// the peer's interest entries on the remote side.
pub const INTEREST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// TTL for peer interests. Set to 4x the heartbeat interval so that up to
/// 3 consecutive missed heartbeats are tolerated before expiry.
pub const INTEREST_TTL: Duration = Duration::from_secs(INTEREST_HEARTBEAT_INTERVAL.as_secs() * 4); // 20 minutes

/// Interval for background sweep to clean up expired interests.
pub const INTEREST_SWEEP_INTERVAL: Duration = Duration::from_secs(60); // 1 minute

/// Max distinct peers tracked as interested in a single contract.
/// Matches MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT (hosting.rs) so the two
/// broadcast-target sources are symmetrically bounded (#3798 Gap 2).
pub(crate) const MAX_INTERESTED_PEERS_PER_CONTRACT: usize = 512;

/// Grace period before removing a disconnected peer's interests.
///
/// When a peer disconnects, we defer interest removal for this duration instead of
/// wiping immediately. If the peer reconnects within the grace period, the pending
/// removal is cancelled and interests are preserved. This prevents permanent interest
/// loss for peers with unstable connections (e.g., stale pending reservations causing
/// ~60s disconnect/reconnect cycles). Set to 90s to comfortably survive such cycles.
pub const INTEREST_DISCONNECT_GRACE_PERIOD: Duration = Duration::from_secs(90);

use crate::config::GlobalExecutor;
use crate::config::GlobalRng;

/// Maximum number of entries in the delta memoization cache.
///
// TODO(fast-follow): size this by hosted×neighbors rather than a flat 1024, so
// the interest-heartbeat staleness probes (`peer_summary_has_pending_state`)
// and broadcast deltas keep their working set cached across cycles on
// large-hosted-set peers. Deferred: the per-message probe budget
// (`MAX_STALENESS_PROBES_PER_SUMMARIES`) already bounds the cold-cache
// worst-case load, and summaries are memoized outside WASM so byte keys stay
// stable while state is unchanged.
const DELTA_CACHE_SIZE: usize = 1024;

/// Minimum interval between queue-full `ResyncRequest`s to the same peer for
/// the same contract (issue #4857).
///
/// A `ContractQueueFull` broadcast drop is silent: the receiver never applied
/// the delta, but the SENDER cached its own summary as ours on send-Ok
/// (`broadcast_queue.rs::record_delivery_to_interest`), so it believes we are
/// current and will never re-send the dropped change. Left unhealed, a
/// rarely-changing field diverges permanently until the ~5-min InterestSync
/// heartbeat happens to correct it. Emitting a `ResyncRequest` makes the sender
/// clear its cached summary of us and re-send full state — but issue #4251
/// showed that one request per dropped delta amplifies into a full-state storm
/// onto the same saturated queue. This interval bounds that amplification to at
/// most one request per (contract, peer) window while still healing far faster
/// than the heartbeat backstop.
///
/// `pub(crate)` so the UPDATE queue-full retry (#4857 P2) can size its own
/// tokio-clock liveness backstop to exactly one reservation window — see
/// `operations::update::op_ctx_task::resend_queue_full_resync_request`.
pub(crate) const RESYNC_REQUEST_MIN_INTERVAL: Duration = Duration::from_secs(30);

/// Bound on the number of (contract, peer) entries in the queue-full
/// `ResyncRequest` throttle. The key is influenced by remote peers (any peer
/// can broadcast any contract to us), so it MUST be bounded — see the
/// per-key-collection rule in `.claude/rules/code-style.md`. LRU eviction fails
/// open: forgetting an entry merely permits one extra healing `ResyncRequest`,
/// which is safe.
const RESYNC_THROTTLE_CACHE_SIZE: usize = 4096;

/// Bound on the number of peers tracked by the full-bytes summary fallback
/// rotation cursor (#5155). Keyed by remote socket address, so it MUST be
/// bounded — see the per-key-collection rule in `.claude/rules/code-style.md`.
///
/// Eviction costs a peer its place in the cycle, not coverage: a forgotten
/// cursor restarts that peer's rotation at a random offset, so the contracts
/// re-sent are ones it may have seen recently rather than ones it is owed.
///
/// The random restart is load-bearing here, not decoration. Under a fixed
/// restart, a single eviction would be harmless, but SUSTAINED eviction — more
/// concurrently-syncing fallback peers than cache slots — would return every
/// peer to the head of its set every round and starve the tail permanently. The
/// cap is well above `max_connections`, so that is not the expected regime; the
/// randomisation is what makes it a slow cycle rather than a silent hole if it
/// ever is. See [`InterestManager::fallback_window_start`].
const SUMMARY_FALLBACK_CURSOR_CACHE_SIZE: usize = 4096;

/// Bounds diagnostic correlation state influenced by remote (contract, peer)
/// pairs. Eviction only loses classification detail; it never changes routing.
///
/// Live production data (2026-08-01/02, v0.2.117 field telemetry, #5090/#5091)
/// showed the previous 4,096-entry cap saturated on busy nodes: the overflow
/// counter (`NetworkEfficiencyV1::corr_ovf[0]`) fired on ~31% of new-pair
/// registrations fleet-wide, and a single busy gateway (nova) hosts ~2,811
/// contracts across ~150 connections — a theoretical (contract, peer) space
/// (~421K) far above the old cap; real overlap is sparser than that upper
/// bound, but no density measurement pins it down precisely. Once an LRU of
/// this shape is saturated, eviction is continuous (every new never-seen pair
/// evicts the least-recent entry), so the overflow counter reflects steady-
/// state new-pair *arrival rate*, not a one-time capacity breach — it will
/// not reach exactly zero at any finite cap, only a lower rate. 65,536 (16x)
/// is a substantial headroom increase chosen to reduce that rate
/// significantly, not a precise working-set derivation; memory cost is a
/// worst-case ~14 MB fully populated (up from ~1 MB), paid on every node
/// including small ones since this is a single fleet-wide constant rather
/// than one scaled per-node from `max_connections` (a possible future
/// refinement, matching the code-style.md "derive thresholds from
/// configuration" convention, if a fleet-wide constant proves insufficient).
/// Re-measure `corr_ovf[0]` after deploying this to see how much the rate
/// actually drops before considering a further bump or an unbounded-but-swept
/// alternative.
const MISSING_SUMMARY_HISTORY_SIZE: usize = 65536;

/// Bounds in-flight missing-summary send correlation. At capacity sends still
/// proceed, and the visible overflow counter records the lost correlation.
///
/// Left UNCHANGED from its original value: the same production measurement
/// that justified bumping [`MISSING_SUMMARY_HISTORY_SIZE`] showed
/// `corr_ovf[1]` (active-tracking overflow) at exactly 0 even under the old
/// cap — no evidence this bound is a bottleneck, so it is not bumped
/// speculatively.
const MISSING_SUMMARY_ACTIVE_SIZE: usize = 256;

/// Telemetry field order for the first missing-summary send age histogram:
/// <1s, 1-9s, 10-59s, 60-299s, and >=300s.
pub(crate) const FIRST_MISSING_SUMMARY_SEND_AGE_LABELS: [&str; 5] =
    ["lt_1s", "1_9s", "10_59s", "60_299s", "gte_300s"];

/// Node-wide cap on concurrently-outstanding queue-full-resync retry tasks
/// (#4862 P1). The per-(contract, peer) throttle above is a bounded LRU; under
/// saturation plus key churn (a peer cycling through more than
/// [`RESYNC_THROTTLE_CACHE_SIZE`] contracts) the LRU can EVICT still-active
/// reservations, so a revisited key re-grants and spawns ANOTHER retry task —
/// unbounded overlapping tasks that also defeat the per-window send cap. This
/// node-wide cap bounds the aggregate retry-task count (and thus timer wakeups
/// and the full-state `ResyncResponse` fan-out those retries induce) regardless
/// of LRU churn. At cap the immediate `ResyncRequest` still sends; only the
/// best-effort retry is skipped. See
/// [`InterestManager::try_reserve_resync_retry_slot`].
pub(crate) const MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES: usize = 256;

/// RAII reservation for one outstanding queue-full-resync retry task (#4862 P1).
///
/// Held by the spawned retry task; its `Drop` decrements the node's
/// outstanding-retry counter, so the slot is freed when the task completes,
/// is dropped, or panics. Obtain via
/// [`InterestManager::try_reserve_resync_retry_slot`].
pub(crate) struct ResyncRetrySlot(Arc<AtomicUsize>);

impl Drop for ResyncRetrySlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Timeout for contract handler queries in the broadcast path (summary and
/// delta computation). Much shorter than the default 300s to prevent spawned
/// broadcast tasks from accumulating when the contract handler is slow.
const BROADCAST_CH_TIMEOUT: Duration = Duration::from_secs(10);

/// Identifies a peer for interest tracking purposes.
///
/// Uses the peer's public key rather than socket address, since addresses
/// can change (NAT, reconnection) but the key is stable.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PeerKey(pub TransportPublicKey);

impl From<TransportPublicKey> for PeerKey {
    fn from(key: TransportPublicKey) -> Self {
        Self(key)
    }
}

/// Why a tracked peer's cached summary is absent.
///
/// A tracked peer with no cached summary forces a FULL STATE broadcast
/// (`PayloadArm::FullNoTheirSummaryTracked`). On the aged 0.2.109 fleet that
/// arm was 26.9% of broadcast bytes at a 357 KB mean — the single largest
/// remaining bandwidth arm and the main cause of the 4-20s propagation
/// latency in #4961 — but the rollup could not say WHICH of the paths below
/// produced it, and the three have completely different fixes. This tag is
/// what makes that distinguishable; see #4961.
///
/// The tag is only meaningful while `summary` is `None`; read it through
/// [`PeerInterest::summary_missing_reason`], which enforces that.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryMissingReason {
    /// The entry was created without a summary and has never had one written.
    ///
    /// The interest-registration chain (`register_local_hosting` → `Interests`
    /// → `register_peer_interest`) creates entries with `summary: None`; they
    /// stay that way until a delivery or a `Summaries` report seeds one. A
    /// large share here means the seeding chain isn't firing (or an
    /// `Interests` full-replace is wiping seeded entries every ~5 min).
    NeverPopulated,

    /// The peer itself reported `None` in an InterestSync `Summaries` message,
    /// so we dropped what we had cached.
    ///
    /// Suspect path: we may be discarding a summary we seeded from an actual
    /// delivery because the peer's own report raced ahead of its state write.
    ClearedByNoneReport,

    /// We received a `ResyncRequest` from the peer, which invalidates our
    /// cached view of what they hold.
    ClearedByResync,

    /// A delta we sent failed to apply on the peer, so our cached summary for
    /// them was provably wrong.
    ClearedByDeltaApplyFailure,
}

impl SummaryMissingReason {
    pub const COUNT: usize = 4;

    /// Every reason, in telemetry field order.
    pub const ALL: [SummaryMissingReason; Self::COUNT] = [
        SummaryMissingReason::NeverPopulated,
        SummaryMissingReason::ClearedByNoneReport,
        SummaryMissingReason::ClearedByResync,
        SummaryMissingReason::ClearedByDeltaApplyFailure,
    ];

    /// Dense index into a per-reason counter array.
    pub fn index(self) -> usize {
        match self {
            SummaryMissingReason::NeverPopulated => 0,
            SummaryMissingReason::ClearedByNoneReport => 1,
            SummaryMissingReason::ClearedByResync => 2,
            SummaryMissingReason::ClearedByDeltaApplyFailure => 3,
        }
    }

    /// Stable label for telemetry field names.
    pub fn as_str(self) -> &'static str {
        match self {
            SummaryMissingReason::NeverPopulated => "never_populated",
            SummaryMissingReason::ClearedByNoneReport => "none_report",
            SummaryMissingReason::ClearedByResync => "resync",
            SummaryMissingReason::ClearedByDeltaApplyFailure => "delta_apply_failed",
        }
    }
}

/// Stable, fixed-cardinality origin of an interest registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InterestRegistrationSource {
    Interests,
    ChangeInterests,
    SubscribeOriginator,
    SubscribeDownstream,
    SubscribeRelay,
    Get,
    Unknown,
}

impl InterestRegistrationSource {
    pub(crate) const COUNT: usize = 7;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::Interests,
        Self::ChangeInterests,
        Self::SubscribeOriginator,
        Self::SubscribeDownstream,
        Self::SubscribeRelay,
        Self::Get,
        Self::Unknown,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Interests => "interests",
            Self::ChangeInterests => "change_interests",
            Self::SubscribeOriginator => "subscribe_originator",
            Self::SubscribeDownstream => "subscribe_downstream",
            Self::SubscribeRelay => "subscribe_relay",
            Self::Get => "get",
            Self::Unknown => "unknown",
        }
    }
}

/// Stable reason an interest stopped being tracked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InterestRemovalCause {
    InterestsReplace,
    ChangeInterests,
    Unsubscribe,
    DisconnectGrace,
    TtlExpiry,
    Eviction,
    Unknown,
}

impl InterestRemovalCause {
    pub(crate) const COUNT: usize = 7;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::InterestsReplace,
        Self::ChangeInterests,
        Self::Unsubscribe,
        Self::DisconnectGrace,
        Self::TtlExpiry,
        Self::Eviction,
        Self::Unknown,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::InterestsReplace => "interests_replace",
            Self::ChangeInterests => "change_interests",
            Self::Unsubscribe => "unsubscribe",
            Self::DisconnectGrace => "disconnect_grace",
            Self::TtlExpiry => "ttl_expiry",
            Self::Eviction => "eviction",
            Self::Unknown => "unknown",
        }
    }
}

/// Stable source of a peer-summary write.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SummaryPopulationSource {
    Delivery,
    InterestSummary,
    DigestAgreement,
    InboundBroadcast,
    ResyncResponse,
    Unknown,
}

impl SummaryPopulationSource {
    pub(crate) const COUNT: usize = 6;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::Delivery,
        Self::InterestSummary,
        Self::DigestAgreement,
        Self::InboundBroadcast,
        Self::ResyncResponse,
        Self::Unknown,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Delivery => "delivery",
            Self::InterestSummary => "interest_summary",
            Self::DigestAgreement => "digest_agreement",
            Self::InboundBroadcast => "inbound_broadcast",
            Self::ResyncResponse => "resync_response",
            Self::Unknown => "unknown",
        }
    }
}

/// Result of a summary population attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SummaryPopulationOutcome {
    FilledMissing,
    RefreshedKnown,
    CreatedUntracked,
    RejectedAtCap,
}

impl SummaryPopulationOutcome {
    pub(crate) const COUNT: usize = 4;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::FilledMissing,
        Self::RefreshedKnown,
        Self::CreatedUntracked,
        Self::RejectedAtCap,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FilledMissing => "filled_missing",
            Self::RefreshedKnown => "refreshed_known",
            Self::CreatedUntracked => "created_untracked",
            Self::RejectedAtCap => "rejected_at_cap",
        }
    }
}

/// Why a delivered broadcast had no usable peer summary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MissingSummaryClass {
    TrackedFirstNew,
    TrackedFirstRecreated,
    TrackedFirstOverwriteKnown,
    TrackedFirstOverwriteMissing,
    TrackedRepeatInflight,
    TrackedRepeatSequential,
    UntrackedFirstObserved,
    UntrackedFirstRecreated,
    UntrackedRepeatInflight,
    UntrackedRepeatSequential,
}

impl MissingSummaryClass {
    pub(crate) const COUNT: usize = 10;
    pub(crate) const ALL: [Self; Self::COUNT] = [
        Self::TrackedFirstNew,
        Self::TrackedFirstRecreated,
        Self::TrackedFirstOverwriteKnown,
        Self::TrackedFirstOverwriteMissing,
        Self::TrackedRepeatInflight,
        Self::TrackedRepeatSequential,
        Self::UntrackedFirstObserved,
        Self::UntrackedFirstRecreated,
        Self::UntrackedRepeatInflight,
        Self::UntrackedRepeatSequential,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::TrackedFirstNew => "tracked_first_new",
            Self::TrackedFirstRecreated => "tracked_first_recreated",
            Self::TrackedFirstOverwriteKnown => "tracked_first_overwrite_known",
            Self::TrackedFirstOverwriteMissing => "tracked_first_overwrite_missing",
            Self::TrackedRepeatInflight => "tracked_repeat_inflight",
            Self::TrackedRepeatSequential => "tracked_repeat_sequential",
            Self::UntrackedFirstObserved => "untracked_first_observed",
            Self::UntrackedFirstRecreated => "untracked_first_recreated",
            Self::UntrackedRepeatInflight => "untracked_repeat_inflight",
            Self::UntrackedRepeatSequential => "untracked_repeat_sequential",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NeverPopulatedOrigin {
    New { recreated: bool },
    OverwriteKnown,
    OverwriteMissing,
}

/// Size buckets for `delivered_size_hist`.
///
/// Log-4 scale with the tails merged: <4 KiB, <16 KiB, <64 KiB, <256 KiB,
/// <1 MiB, >=1 MiB. The measured mean full-state payload sits around 60-95
/// KiB, so resolution is kept on both sides of ~64 KiB while the extremes,
/// which a full-state broadcast rarely occupies, share a bucket. Six rather
/// than eight because eight put the busy-fleet `network_efficiency_v1` block
/// 31 bytes over its explicit 5120-byte budget, and the two merged tails cost
/// far less information than raising that budget would cost in trust.
pub(crate) const MISSING_SUMMARY_SIZE_BUCKETS: usize = 6;

/// Buckets for `untracked_prior_removal_age`.
pub(crate) const UNTRACKED_PRIOR_REMOVAL_BUCKETS: usize = 6;

// Both bucket functions below HARDCODE their match arms while these constants
// size the arrays those arms index. Nothing in the type system couples them, so
// trimming a constant to save telemetry bytes — exactly the edit this module's
// own budget pressure invites — would compile clean and then panic with
// index-out-of-bounds inside the broadcast task on the first oversized payload.
// `SIZE_HIST_ROWS` is immune because it derives from `.len()`; these only LOOK
// as safe.
const _: () = assert!(MISSING_SUMMARY_SIZE_BUCKETS == 6);
const _: () = assert!(UNTRACKED_PRIOR_REMOVAL_BUCKETS == 6);

/// Classes carrying a size histogram, in row order.
///
/// Deliberately NOT all ten classes. A 10x8 matrix pushed the busy-fleet
/// `network_efficiency_v1` block to 5499 bytes against its explicit 5120-byte
/// budget, and most of that spend was on classes that do not fire: both
/// `*_overwrite_*` classes are a measured ZERO fleet-wide, and all four
/// `*_repeat_*` classes round to zero. These four carry ~99% of observed
/// missing-summary bytes.
///
/// The trade-off, stated so it is not rediscovered: a repeat/overwrite class
/// that later becomes significant would have no SIZE distribution here. Its
/// COUNT and BYTES remain fully visible in `ms_s`/`ms_b`, which keep all ten
/// classes, so the growth itself could not hide — only its shape.
pub(crate) const SIZE_HIST_CLASSES: [MissingSummaryClass; 4] = [
    MissingSummaryClass::TrackedFirstNew,
    MissingSummaryClass::TrackedFirstRecreated,
    MissingSummaryClass::UntrackedFirstObserved,
    MissingSummaryClass::UntrackedFirstRecreated,
];

/// Rows in `delivered_size_hist`.
pub(crate) const SIZE_HIST_ROWS: usize = SIZE_HIST_CLASSES.len();

#[derive(Clone, Copy, Debug, Default)]
struct MissingPairHistory {
    send_starts: u32,
    last_observed: Option<Instant>,
    recent_removal: Option<(InterestRemovalCause, Instant)>,
}

/// Fixed-cardinality lifecycle counters copied into telemetry snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InterestLifecycleSnapshot {
    pub(crate) delivered_sends: [u64; MissingSummaryClass::COUNT],
    pub(crate) delivered_bytes: [u64; MissingSummaryClass::COUNT],
    pub(crate) first_send_age: [u64; 5],
    pub(crate) delivered_size_hist: [[u64; MISSING_SUMMARY_SIZE_BUCKETS]; SIZE_HIST_ROWS],
    pub(crate) untracked_prior_removal_age: [u64; UNTRACKED_PRIOR_REMOVAL_BUCKETS],
    pub(crate) registration_overwrite_known: [u64; InterestRegistrationSource::COUNT],
    pub(crate) registration_overwrite_missing: [u64; InterestRegistrationSource::COUNT],
    pub(crate) registration_new_known: [u64; InterestRegistrationSource::COUNT],
    pub(crate) registration_new_missing: [u64; InterestRegistrationSource::COUNT],
    pub(crate) removals: [u64; InterestRemovalCause::COUNT],
    pub(crate) recreated_after_removal: [u64; InterestRemovalCause::COUNT],
    pub(crate) population: [[u64; SummaryPopulationOutcome::COUNT]; SummaryPopulationSource::COUNT],
    pub(crate) registration_cap_rejected: [u64; InterestRegistrationSource::COUNT],
    /// Current entries: known, then missing by `SummaryMissingReason::ALL`.
    pub(crate) current_summary_state: [u64; SummaryMissingReason::COUNT + 1],
    pub(crate) history_overflow: u64,
    pub(crate) active_overflow: u64,
}

struct InterestLifecycleMetrics {
    delivered_sends: [AtomicU64; MissingSummaryClass::COUNT],
    delivered_bytes: [AtomicU64; MissingSummaryClass::COUNT],
    first_send_age: [AtomicU64; 5],
    /// Per-class size histogram of delivered missing-summary payloads.
    delivered_size_hist: [[AtomicU64; MISSING_SUMMARY_SIZE_BUCKETS]; SIZE_HIST_ROWS],
    /// Untracked sends bucketed by how long ago the pair's entry was removed.
    untracked_prior_removal_age: [AtomicU64; UNTRACKED_PRIOR_REMOVAL_BUCKETS],
    registration_overwrite_known: [AtomicU64; InterestRegistrationSource::COUNT],
    registration_overwrite_missing: [AtomicU64; InterestRegistrationSource::COUNT],
    registration_new_known: [AtomicU64; InterestRegistrationSource::COUNT],
    registration_new_missing: [AtomicU64; InterestRegistrationSource::COUNT],
    removals: [AtomicU64; InterestRemovalCause::COUNT],
    recreated_after_removal: [AtomicU64; InterestRemovalCause::COUNT],
    population: [[AtomicU64; SummaryPopulationOutcome::COUNT]; SummaryPopulationSource::COUNT],
    registration_cap_rejected: [AtomicU64; InterestRegistrationSource::COUNT],
    history_overflow: AtomicU64,
    active_overflow: AtomicU64,
}

pub(crate) struct MissingSummaryAttempt {
    key: (ContractKey, PeerKey),
    class: MissingSummaryClass,
    first_age_bucket: Option<usize>,
    /// Set only on the UNTRACKED path: how long ago this pair's interest entry
    /// was removed, or bucket 5 when there is no record of one.
    untracked_prior_removal_bucket: Option<usize>,
    active_tracked: bool,
}

/// One atomic observation of the cached peer summary used by a broadcast.
pub(crate) enum PeerSummaryForBroadcast {
    Known(StateSummary<'static>),
    Missing {
        reason: Option<SummaryMissingReason>,
        attempt: Option<MissingSummaryAttempt>,
    },
}

impl InterestLifecycleMetrics {
    fn new() -> Self {
        Self {
            delivered_sends: std::array::from_fn(|_| AtomicU64::new(0)),
            delivered_bytes: std::array::from_fn(|_| AtomicU64::new(0)),
            first_send_age: std::array::from_fn(|_| AtomicU64::new(0)),
            delivered_size_hist: std::array::from_fn(|_| {
                std::array::from_fn(|_| AtomicU64::new(0))
            }),
            untracked_prior_removal_age: std::array::from_fn(|_| AtomicU64::new(0)),
            registration_overwrite_known: std::array::from_fn(|_| AtomicU64::new(0)),
            registration_overwrite_missing: std::array::from_fn(|_| AtomicU64::new(0)),
            registration_new_known: std::array::from_fn(|_| AtomicU64::new(0)),
            registration_new_missing: std::array::from_fn(|_| AtomicU64::new(0)),
            removals: std::array::from_fn(|_| AtomicU64::new(0)),
            recreated_after_removal: std::array::from_fn(|_| AtomicU64::new(0)),
            population: std::array::from_fn(|_| std::array::from_fn(|_| AtomicU64::new(0))),
            registration_cap_rejected: std::array::from_fn(|_| AtomicU64::new(0)),
            history_overflow: AtomicU64::new(0),
            active_overflow: AtomicU64::new(0),
        }
    }
}

/// Tracking information for a peer's interest in a specific contract.
#[derive(Clone, Debug)]
pub struct PeerInterest {
    /// The peer's current state summary. None if interested but has no state yet.
    pub summary: Option<StateSummary<'static>>,

    /// Why [`Self::summary`] is absent. Stale (and unread) whenever `summary`
    /// is `Some` — always read it via [`Self::summary_missing_reason`], which
    /// returns `None` in that case rather than a misleading last-clear cause.
    summary_absence: SummaryMissingReason,

    /// Diagnostic-only provenance for the current NeverPopulated epoch.
    never_populated_origin: NeverPopulatedOrigin,

    /// Start time and send-attempt count for that epoch.
    never_populated_since: Instant,
    never_populated_send_starts: u32,

    /// When this interest entry was last refreshed.
    /// Used for TTL-based expiration.
    pub last_refreshed: Instant,

    /// Whether this peer is our upstream in the subscription tree.
    /// Internal routing hint, not exposed to protocol.
    pub is_upstream: bool,
}

impl PeerInterest {
    /// Create a new peer interest entry with the given timestamp.
    ///
    /// A `None` summary here is [`SummaryMissingReason::NeverPopulated`] by
    /// construction — this is the only constructor, so an entry cannot come
    /// into existence summaryless without carrying that tag.
    pub fn new(summary: Option<StateSummary<'static>>, is_upstream: bool, now: Instant) -> Self {
        Self {
            summary,
            summary_absence: SummaryMissingReason::NeverPopulated,
            never_populated_origin: NeverPopulatedOrigin::New { recreated: false },
            never_populated_since: now,
            never_populated_send_starts: 0,
            last_refreshed: now,
            is_upstream,
        }
    }

    /// Refresh the TTL timestamp with the given current time.
    pub fn refresh(&mut self, now: Instant) {
        self.last_refreshed = now;
    }

    /// Check if this interest has expired relative to the given current time.
    pub fn is_expired_at(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_refreshed) > INTEREST_TTL
    }

    /// Why this peer has no cached summary, or `None` when one IS cached.
    pub fn summary_missing_reason(&self) -> Option<SummaryMissingReason> {
        self.summary.is_none().then_some(self.summary_absence)
    }

    /// Cache a summary for this peer and refresh TTL.
    pub fn set_summary(&mut self, summary: StateSummary<'static>, now: Instant) {
        self.summary = Some(summary);
        self.refresh(now);
    }

    /// Drop the cached summary, recording why, and refresh TTL.
    ///
    /// Taking `reason` by value (rather than accepting an `Option` summary) is
    /// deliberate: it makes an untagged clear unrepresentable, so a future
    /// clear site cannot silently land in the `NeverPopulated` bucket and
    /// mis-aim the next fix.
    pub fn clear_summary(&mut self, reason: SummaryMissingReason, now: Instant) {
        self.summary = None;
        self.summary_absence = reason;
        if reason == SummaryMissingReason::NeverPopulated {
            self.never_populated_origin = NeverPopulatedOrigin::New { recreated: false };
            self.never_populated_since = now;
            self.never_populated_send_starts = 0;
        }
        self.refresh(now);
    }
}

/// Tracks local reasons for interest in a contract.
///
/// A peer can be interested for multiple reasons. We only deregister interest
/// when ALL reasons are removed.
#[derive(Clone, Debug, Default)]
pub struct LocalInterest {
    /// Whether we're hosting this contract (in our local cache).
    pub hosting: bool,

    /// Number of local WebSocket clients subscribed to this contract.
    pub local_client_count: usize,

    /// Number of downstream peers subscribed through us.
    pub downstream_subscriber_count: usize,
}

impl LocalInterest {
    /// Check if we have any reason to be interested in this contract.
    pub fn is_interested(&self) -> bool {
        self.hosting || self.local_client_count > 0 || self.downstream_subscriber_count > 0
    }

    /// Increment the local client count and return whether this is the first client.
    pub fn add_client(&mut self) -> bool {
        let was_first = self.local_client_count == 0;
        self.local_client_count += 1;
        was_first && !self.hosting && self.downstream_subscriber_count == 0
    }

    /// Decrement the local client count and return whether interest was lost.
    pub fn remove_client(&mut self) -> bool {
        self.local_client_count = self.local_client_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Increment the downstream subscriber count and return whether this is the first.
    pub fn add_downstream(&mut self) -> bool {
        let was_first =
            self.downstream_subscriber_count == 0 && self.local_client_count == 0 && !self.hosting;
        self.downstream_subscriber_count += 1;
        was_first
    }

    /// Decrement the downstream subscriber count and return whether interest was lost.
    pub fn remove_downstream(&mut self) -> bool {
        self.downstream_subscriber_count = self.downstream_subscriber_count.saturating_sub(1);
        !self.is_interested()
    }

    /// Set hosting status and return whether interest state changed.
    pub fn set_hosting(&mut self, hosting: bool) -> bool {
        let was_interested = self.is_interested();
        self.hosting = hosting;
        let is_interested = self.is_interested();
        was_interested != is_interested
    }
}

/// Key for delta cache using hashes to avoid allocation on every lookup.
///
/// Instead of storing full summary bytes, we hash them to u64. This makes
/// cache lookups O(1) without any heap allocation. Hash collisions are
/// extremely rare and only cause cache misses (not correctness issues).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct DeltaCacheKey {
    contract: ContractKey,
    peer_summary_hash: u64,
    our_summary_hash: u64,
}

/// Hash bytes to u64 for cache key construction.
/// Uses DefaultHasher for good distribution.
fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(bytes);
    hasher.finish()
}

/// Compute a fast hash of a contract key for connection-time discovery.
///
/// Uses FNV-1a for speed. Collisions are acceptable - they just mean we'll
/// check contracts that aren't actually shared.
///
/// This is a standalone function to avoid requiring type parameters when called.
pub fn contract_hash(contract: &ContractKey) -> u32 {
    // FNV-1a parameters
    const FNV_OFFSET: u32 = 2166136261;
    const FNV_PRIME: u32 = 16777619;

    let id_bytes = contract.id().as_bytes();
    let mut hash = FNV_OFFSET;
    for byte in id_bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Length in bytes of a [`SummaryDigest`].
///
/// 16 bytes (128 bits). The digest replaces a full `StateSummary` on the wire
/// (~33 KB for a River room), so the cost of a wide digest is a rounding
/// error, while a narrow one would make accidental collisions — which read as
/// "the peer agrees with us" and therefore SUPPRESS a heal — thinkable. At 128
/// bits they are not.
pub const SUMMARY_DIGEST_LEN: usize = 16;

/// A digest of a contract's `StateSummary` **bytes**, used by the hash-first
/// `InterestSync` exchange (#4965) to say "my summary is X" without shipping
/// the summary itself.
pub type SummaryDigest = [u8; SUMMARY_DIGEST_LEN];

/// Digest of a contract's state-summary bytes: truncated BLAKE3.
///
/// # This is NOT [`contract_hash`]
///
/// The two hashes live next to each other on purpose, because conflating them
/// is the obvious mistake and they answer different questions:
///
/// | | [`contract_hash`] | [`summary_digest`] |
/// |---|---|---|
/// | input | contract INSTANCE ID | the contract's state SUMMARY bytes |
/// | answers | *which* contract | *what state* we hold of it |
/// | changes when | never, for a given contract | every state change |
/// | algorithm | FNV-1a, 32-bit | BLAKE3, truncated to 128-bit |
/// | collisions | fine (extra comparisons) | must not happen (suppresses a heal) |
///
/// `SummaryDigestEntry` carries BOTH: `hash` identifies the contract,
/// `summary_digest` describes our state of it.
///
/// # Why BLAKE3 and not a `Hasher`
///
/// The digest is a WIRE value compared across peers and across restarts, so it
/// must be identical for identical bytes on every node forever.
/// `DefaultHasher` is explicitly not stable across releases (and `RandomState`
/// is per-process random): a digest that varies per node would make every
/// comparison mismatch, which degrades to "always ship the bytes plus an extra
/// round trip" — worse than not doing this at all — and re-creates the #4857
/// storm shape where every heartbeat looks like divergence. BLAKE3 over the
/// raw bytes has no endianness or platform freedom: the input is a byte slice
/// and the output is a byte array, so no integer is ever serialized.
///
/// The truncation takes the FIRST [`SUMMARY_DIGEST_LEN`] bytes of the 32-byte
/// BLAKE3 output, which is the standard way to shorten it (BLAKE3's output is
/// uniformly distributed, so any fixed slice is a sound shorter digest).
pub fn summary_digest(summary_bytes: &[u8]) -> SummaryDigest {
    let full = blake3::hash(summary_bytes);
    let mut digest = [0u8; SUMMARY_DIGEST_LEN];
    digest.copy_from_slice(&full.as_bytes()[..SUMMARY_DIGEST_LEN]);
    digest
}

/// How much smaller full state must be before the post-compute gate
/// ([`InterestManager::gate_delta_size`]) abandons a computed delta for it.
///
/// Switching payload kinds is not free: a delta keeps the receiver's
/// peer-summary cache warm and keeps fan-out off the full-state path that
/// #4233 / #4956 are about. Below this margin the byte win is a rounding
/// error and not worth those costs — at 1 KiB, a small CRDT contract whose
/// delta marginally exceeds its state keeps sending deltas, while the
/// poisoned-summary population this gate targets (state-sized deltas at
/// 550-840 KB) still refuses by a wide margin.
const MIN_FULL_STATE_SAVING_BYTES: usize = 1024;

/// Heuristic: would a delta *probably* be efficient compared to sending full
/// state, judging only by the peer's summary size?
///
/// Returns true if summary size is less than 50% of state size.
///
/// History (#4923): this used to be a PRE-compute gate inside
/// [`InterestManager::compute_delta`] — a refusal to even ask the contract for
/// a delta when the peer's summary was large. That inverted the trade-off:
/// the fallback to a refused delta is sending FULL STATE, which is never
/// smaller than the delta the gate declined to compute, and in production the
/// resulting full-state sends were 41% of ALL network wire bytes (87.4% for
/// the hottest contract). `compute_delta` now always computes and gates on
/// the ACTUAL delta size afterwards, so this summary-size proxy has no
/// production caller. It is deliberately kept (not deleted) as the documented
/// wire-efficiency heuristic with its unit tests — do not re-wire it as a
/// pre-compute refusal.
///
/// This is a standalone function to avoid requiring type parameters when called.
#[cfg_attr(not(test), allow(dead_code))]
pub fn is_delta_efficient(summary_size: usize, state_size: usize) -> bool {
    if state_size == 0 {
        return false;
    }
    summary_size * 2 < state_size
}

/// Why [`InterestManager::compute_delta`] could not hand back a delta.
///
/// Typed rather than a bare `String` so callers can tell the two cases apart:
/// they have different remedies, and the fan-out's payload-mix telemetry
/// ([`crate::node::network_bridge::broadcast_payload_mix`]) reports them as
/// separate arms. Every caller falls back to sending FULL STATE, which is
/// never smaller than the delta that was declined — see #3335.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaUnavailable {
    /// The delta WAS computed (or found cached) but is not smaller than our
    /// full state, so the caller's full-state fallback is the genuinely
    /// optimal payload (equal or smaller bytes, and no delta-apply on the
    /// receiver).
    ///
    /// History (#4923): this variant used to mean the [`is_delta_efficient`]
    /// summary-size proxy refused *before* any delta was computed ("no
    /// contract code ran"). That pre-compute refusal is gone — the gate now
    /// runs POST-compute on the actual delta size. The variant name and
    /// field shape are unchanged on purpose: the #4938 payload-mix telemetry
    /// keys off them.
    NotEfficient {
        summary_size: usize,
        state_size: usize,
    },
    /// A delta was attempted and the contract handler failed — WASM error,
    /// timeout, or an unexpected response.
    ComputeFailed(String),
}

impl std::fmt::Display for DeltaUnavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Keep the historical prefix so existing log greps still match;
            // the sizes are additive.
            DeltaUnavailable::NotEfficient {
                summary_size,
                state_size,
            } => write!(
                f,
                "Delta not efficient for this contract (summary {summary_size} B, \
                 state {state_size} B)"
            ),
            DeltaUnavailable::ComputeFailed(msg) => write!(f, "{msg}"),
        }
    }
}

/// Decide whether a peer that reported `their_summary` is stale relative to our
/// `our_summary` — i.e. whether the InterestSync heartbeat should heal it with
/// our state.
///
/// A raw byte comparison of the two summaries is WRONG on its own: a contract
/// whose `summarize_state` serializes a `HashMap`/`HashSet` (non-deterministic
/// iteration order, per-process `RandomState`) produces DIFFERENT summary bytes
/// for the SAME logical state on different peers. Byte-inequality then flags a
/// fully-converged peer stale and fires a full-state heal on every ~5-min
/// heartbeat — the 2.56M rate-limited `summarize_contract_state` storm observed
/// on the 0.2.102 gateway (freenet/freenet-core#4857 secondary finding). The
/// core cannot canonicalize the opaque summary bytes itself (it does not know
/// the contract's encoding), so it delegates the judgement to the contract via
/// its own `get_state_delta` — surfaced here as `delta_indicates_change`:
///
/// - `Some(true)`: our state holds data the peer's summary lacks (a non-empty
///   delta), so the peer is genuinely stale — heal it.
/// - `Some(false)`: the contract's delta is empty, so the peer is logically
///   converged despite differing summary bytes — NOT stale.
/// - `None`: no semantic verdict is available (the delta probe was unavailable
///   or timed out), so fall back to the raw byte comparison, i.e. the
///   conservative pre-fix behaviour, so a genuine divergence is never skipped.
///
/// Convergence safety: the returned "stale" set is a strict subset of the
/// pre-fix byte-compare set. The only peers this newly treats as NOT stale are
/// those whose summary bytes differ yet whose contract-computed delta is empty —
/// exactly the copies that already hold our state, for which the removed heal
/// would have transferred nothing. Any real divergence still yields a non-empty
/// delta (`Some(true)`) and heals, on this and every subsequent heartbeat.
pub(crate) fn summary_indicates_stale_peer(
    our_summary: &StateSummary<'static>,
    their_summary: &StateSummary<'static>,
    delta_indicates_change: Option<bool>,
) -> bool {
    // Byte-identical summaries are trivially converged. Equal bytes were never
    // stale under the pre-fix logic either, so short-circuit before consulting
    // any (potentially contract-buggy) delta verdict: identical bytes must
    // never heal.
    if our_summary.as_ref() == their_summary.as_ref() {
        return false;
    }
    // Bytes differ. Trust the contract's semantic verdict when present (robust
    // to non-deterministic summary serialization); otherwise preserve the
    // conservative pre-fix behaviour (differing bytes => stale) so a real
    // divergence is never silently missed.
    delta_indicates_change.unwrap_or(true)
}

/// Manages interest tracking and delta computation for all contracts.
///
/// This is the central data structure for the delta-based synchronization system.
/// It unifies what was previously split between the subscription tree and proximity cache.
///
/// Generic over `T: TimeSource` to support deterministic simulation testing.
///
/// **Dual-tracking with `HostingManager::downstream_subscribers`:** both must be
/// kept in sync during register/remove operations. This manager drives UPDATE
/// broadcast targeting and upstream peer lookup; `downstream_subscribers` drives
/// unsubscribe-upstream decisions. See the Unsubscribe handler in
/// `operations/subscribe.rs` for the sync point.
pub struct InterestManager<T: TimeSource> {
    /// Track interested peers and their summaries for each contract.
    /// Key: ContractKey, Value: Map of PeerKey -> PeerInterest
    interested_peers: DashMap<ContractKey, HashMap<PeerKey, PeerInterest>>,

    /// Reverse index: which contracts is each peer interested in?
    /// Enables O(1) cleanup when a peer disconnects instead of O(contracts) scan.
    peer_contracts: DashMap<PeerKey, HashSet<ContractKey>>,

    /// Track our local interest reasons for each contract.
    local_interests: DashMap<ContractKey, LocalInterest>,

    /// Cache for memoizing delta computations.
    /// Avoids recomputing the same delta for multiple peers with identical summaries.
    delta_cache: Mutex<LruCache<DeltaCacheKey, StateDelta<'static>>>,

    /// Fast hash index for connection-time discovery.
    /// Maps u32 hash of contract ID -> list of ContractKeys (handles collisions).
    contract_hash_index: DashMap<u32, Vec<ContractKey>>,

    /// Time source for testability (DST-compatible).
    time_source: T,

    // === Delta Sync Metrics ===
    /// Number of times we sent a delta instead of full state.
    delta_sends: AtomicU64,

    /// Number of times we sent full state (no peer summary available or delta failed).
    full_state_sends: AtomicU64,

    /// Total bytes saved by sending deltas instead of full state.
    /// Calculated as: sum of (state_size - delta_size) for each delta send.
    delta_bytes_saved: AtomicU64,

    /// Number of ResyncRequests received (indicates delta application failures at remote peer).
    /// This counter helps detect incorrect summary caching issues (see PR #2763).
    resync_requests_received: AtomicU64,

    /// Throttle timestamps for proactive summary notifications.
    /// After applying a broadcast update, we notify interested peers of our new summary
    /// so they can skip sending us data we already have. This DashMap tracks the last
    /// notification time per contract to avoid flooding (minimum 100ms interval).
    summary_notify_timestamps: DashMap<ContractKey, Instant>,

    /// Deferred interest removals for disconnected peers.
    ///
    /// Instead of immediately wiping a peer's interests on disconnect, we record a
    /// deadline (now + INTEREST_DISCONNECT_GRACE_PERIOD). The sweep task executes
    /// the removal after the deadline passes. If the peer reconnects before the
    /// deadline, the entry is removed from this map and interests are preserved.
    pending_removals: DashMap<PeerKey, Instant>,

    /// Rate-limit gate for queue-full `ResyncRequest`s, keyed by
    /// (contract, target peer address). Bounded LRU so remote peers cannot grow
    /// it without bound. See [`InterestManager::begin_resync_request`] and
    /// issue #4857.
    resync_request_throttle: Mutex<LruCache<(ContractKey, SocketAddr), Instant>>,

    /// Rotation cursor for the bounded full-bytes summary fallback (#5155),
    /// keyed by the peer's socket address.
    ///
    /// Holds the contract id of the LAST entry included in that peer's previous
    /// fallback reply — a KEY, not an index. That distinction is what preserves
    /// the coverage BOUND when the shared set changes between rounds.
    ///
    /// A stored index names a position, and a removal below it shifts every
    /// later contract down by one, so the resume lands one past where it should
    /// and that contract goes unadvertised for the rest of the cycle. Wrapping
    /// means it is eventually revisited, so the failure is not permanent
    /// starvation — but "eventually" is not the property this change is sold
    /// on. The whole safety argument is a stated upper bound on how long a
    /// divergence can go unnoticed, and under ordinary contract churn an index
    /// cursor degrades that from `ceil(n / limit)` rounds to no bound at all.
    ///
    /// A key resumes after what was actually SENT, so consecutive rounds are
    /// contiguous in id space no matter what happened to the set in between:
    /// an id inserted above the cursor is in the very next window, an id
    /// inserted below it is picked up on the wrap, and a removed cursor id
    /// still orders correctly against the rest because the comparison is
    /// against the stored bytes rather than a lookup that could now fail.
    /// `rotation_does_not_lose_the_coverage_bound_when_contracts_are_removed`
    /// runs both designs over the same removal schedule and shows the index
    /// one missing contracts the key one covers.
    ///
    /// Only the full-bytes fallback consults this. Digest-capable peers keep
    /// receiving the complete set every round and never touch it.
    summary_fallback_cursor: Mutex<LruCache<SocketAddr, ContractInstanceId>>,

    /// Count of concurrently-outstanding queue-full-resync retry tasks (#4862 P1).
    /// Bounds aggregate retry tasks node-wide, independent of the throttle LRU
    /// (which can evict active reservations under key churn). See
    /// [`InterestManager::try_reserve_resync_retry_slot`] and
    /// [`MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES`]. `Arc` so a slot guard can
    /// outlive the borrow (it is moved into the spawned retry task and
    /// decrements the count on drop).
    resync_retry_slots: Arc<AtomicUsize>,

    /// Bounded diagnostic-only state used to distinguish first, recreated,
    /// in-flight duplicate, and sequential missing-summary sends.
    missing_summary_history: Mutex<LruCache<(ContractKey, PeerKey), MissingPairHistory>>,
    /// Per-key entries are updated through DashMap's shard-local `entry()`
    /// API, so same-key increment/decrement stays atomic. The total-size
    /// bound checked in `begin_active_attempt` is a soft diagnostic cap (not
    /// a security invariant): `.len()` is read before the per-key `entry()`
    /// lock is taken, so concurrent first-time inserts for distinct new keys
    /// can race past `MISSING_SUMMARY_ACTIVE_SIZE` (bounded by the number of
    /// concurrent racers, not fixed at one).
    missing_summary_active: DashMap<(ContractKey, PeerKey), u16>,
    interest_lifecycle_metrics: InterestLifecycleMetrics,

    /// SHADOW MODE. Counts (contract, peer) edges whose repairs keep failing to
    /// converge — the observable signature of a contract whose merge is not
    /// commutative. Observes only: it never gates, throttles, or suppresses a
    /// heal. See [`crate::ring::futile_repair`].
    futile_repair: FutileRepairDetector,
}

/// Delivery-gated lifecycle accounting. Dropping an unmarked guard records no
/// bytes, so failed or cancelled sends cannot masquerade as network impact.
pub(crate) struct MissingSummaryAttemptGuard<'a, T: TimeSource + Sync> {
    manager: &'a InterestManager<T>,
    attempt: Option<MissingSummaryAttempt>,
    delivered_bytes: Option<u64>,
}

impl<T: TimeSource + Sync> MissingSummaryAttemptGuard<'_, T> {
    pub(crate) fn mark_delivered(&mut self, bytes: usize) {
        self.delivered_bytes = Some(bytes as u64);
    }
}

impl<T: TimeSource + Sync> Drop for MissingSummaryAttemptGuard<'_, T> {
    fn drop(&mut self) {
        if let Some(attempt) = self.attempt.take() {
            self.manager
                .finish_missing_summary_attempt(attempt, self.delivered_bytes);
        }
    }
}

impl<T: TimeSource + Sync> InterestManager<T> {
    /// Create a new interest manager with the given time source.
    pub fn new(time_source: T) -> Self {
        Self {
            interested_peers: DashMap::new(),
            peer_contracts: DashMap::new(),
            local_interests: DashMap::new(),
            delta_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(DELTA_CACHE_SIZE).expect("DELTA_CACHE_SIZE must be > 0"),
            )),
            contract_hash_index: DashMap::new(),
            time_source,
            delta_sends: AtomicU64::new(0),
            full_state_sends: AtomicU64::new(0),
            delta_bytes_saved: AtomicU64::new(0),
            resync_requests_received: AtomicU64::new(0),
            summary_notify_timestamps: DashMap::new(),
            pending_removals: DashMap::new(),
            resync_retry_slots: Arc::new(AtomicUsize::new(0)),
            resync_request_throttle: Mutex::new(LruCache::new(
                NonZeroUsize::new(RESYNC_THROTTLE_CACHE_SIZE)
                    .expect("RESYNC_THROTTLE_CACHE_SIZE must be > 0"),
            )),
            summary_fallback_cursor: Mutex::new(LruCache::new(
                NonZeroUsize::new(SUMMARY_FALLBACK_CURSOR_CACHE_SIZE)
                    .expect("SUMMARY_FALLBACK_CURSOR_CACHE_SIZE must be > 0"),
            )),
            missing_summary_history: Mutex::new(LruCache::new(
                NonZeroUsize::new(MISSING_SUMMARY_HISTORY_SIZE)
                    .expect("MISSING_SUMMARY_HISTORY_SIZE must be > 0"),
            )),
            missing_summary_active: DashMap::new(),
            interest_lifecycle_metrics: InterestLifecycleMetrics::new(),
            futile_repair: FutileRepairDetector::new(),
        }
    }

    /// Record that a delta was sent instead of full state.
    ///
    /// Call this when successfully sending a delta to a peer.
    /// `state_size` is the full state size, `delta_size` is the delta size.
    pub fn record_delta_send(&self, state_size: usize, delta_size: usize) {
        self.delta_sends.fetch_add(1, Ordering::Relaxed);
        let bytes_saved = state_size.saturating_sub(delta_size);
        self.delta_bytes_saved
            .fetch_add(bytes_saved as u64, Ordering::Relaxed);
    }

    /// Record that full state was sent (no delta available).
    ///
    /// Call this when sending full state because no peer summary was available
    /// or delta computation failed.
    pub fn record_full_state_send(&self) {
        self.full_state_sends.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a ResyncRequest was received from a peer.
    ///
    /// This indicates the peer couldn't apply a delta we sent, likely because
    /// we had incorrect cached summary for them (the bug PR #2763 fixed).
    pub fn record_resync_request_received(&self) {
        self.resync_requests_received
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current time from the configured `TimeSource`.
    ///
    /// Use this to pass DST-compatible timestamps to components that need
    /// the current time (e.g., `BroadcastDedupCache`).
    pub fn now(&self) -> Instant {
        self.time_source.now()
    }

    /// Atomically reads the peer summary and, when it is NeverPopulated (or
    /// the peer is untracked), starts bounded lifecycle correlation for the
    /// broadcast that will use that observation.
    pub(crate) fn begin_peer_summary_broadcast(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
    ) -> PeerSummaryForBroadcast {
        let now = self.time_source.now();
        // The overwhelmingly common known-summary path is read-only. Keep it
        // on a shared shard guard; only NeverPopulated needs mutation for the
        // bounded attempt correlation below.
        if let Some(peers) = self.interested_peers.get(contract)
            && let Some(interest) = peers.get(peer)
        {
            if let Some(summary) = interest.summary.clone() {
                return PeerSummaryForBroadcast::Known(summary);
            }
            let reason = interest.summary_missing_reason();
            if reason != Some(SummaryMissingReason::NeverPopulated) {
                return PeerSummaryForBroadcast::Missing {
                    reason,
                    attempt: None,
                };
            }
        }

        // Re-check after upgrading to an exclusive guard: a population or
        // removal may have raced the shared observation above.
        if let Some(mut peers) = self.interested_peers.get_mut(contract)
            && let Some(interest) = peers.get_mut(peer)
        {
            if let Some(summary) = interest.summary.clone() {
                return PeerSummaryForBroadcast::Known(summary);
            }
            let reason = interest.summary_missing_reason();
            if reason != Some(SummaryMissingReason::NeverPopulated) {
                return PeerSummaryForBroadcast::Missing {
                    reason,
                    attempt: None,
                };
            }

            let key = (*contract, peer.clone());
            let (inflight, active_tracked) = self.begin_active_attempt(&key);
            let first = interest.never_populated_send_starts == 0;
            let class = if inflight {
                MissingSummaryClass::TrackedRepeatInflight
            } else if !first {
                MissingSummaryClass::TrackedRepeatSequential
            } else {
                match interest.never_populated_origin {
                    NeverPopulatedOrigin::New { recreated: false } => {
                        MissingSummaryClass::TrackedFirstNew
                    }
                    NeverPopulatedOrigin::New { recreated: true } => {
                        MissingSummaryClass::TrackedFirstRecreated
                    }
                    NeverPopulatedOrigin::OverwriteKnown => {
                        MissingSummaryClass::TrackedFirstOverwriteKnown
                    }
                    NeverPopulatedOrigin::OverwriteMissing => {
                        MissingSummaryClass::TrackedFirstOverwriteMissing
                    }
                }
            };
            interest.never_populated_send_starts =
                interest.never_populated_send_starts.saturating_add(1);
            let first_age_bucket = first.then(|| {
                Self::first_send_age_bucket(
                    now.saturating_duration_since(interest.never_populated_since),
                )
            });
            return PeerSummaryForBroadcast::Missing {
                reason,
                attempt: Some(MissingSummaryAttempt {
                    key,
                    class,
                    first_age_bucket,
                    untracked_prior_removal_bucket: None,
                    active_tracked,
                }),
            };
        }

        let key = (*contract, peer.clone());
        let mut history = self.missing_summary_history.lock();
        let was_present = history.peek(&key).is_some();
        let mut record = history.get(&key).copied().unwrap_or_default();
        if record
            .last_observed
            .is_some_and(|observed| now.saturating_duration_since(observed) > INTEREST_TTL)
        {
            // Reset the SEND counter only. `recent_removal` must NOT be wiped
            // here: `last_observed` is stamped exclusively on this untracked
            // path, so it says nothing about when the pair's interest entry was
            // removed. Its freshness is enforced independently, by the
            // `removed_at` filter in BOTH readers — the one immediately below,
            // and `register_peer_interest_from`'s, which drives
            // `NeverPopulatedOrigin::New { recreated }` and
            // `recreated_after_removal`. So the wipe was redundant with those
            // filters and could only ever suppress true positives.
            //
            // Wiping it conflated the two clocks and hid recreation: a pair
            // observed untracked at T0, tracked, removed at T1, then broadcast
            // to again at T2 > T0 + INTEREST_TTL lost its T1 removal and
            // reported `UntrackedFirstObserved` however recent T1 was.
            //
            // Scope of the correction, stated precisely because it is easy to
            // overclaim: this moves sends between `UntrackedFirstObserved` and
            // `UntrackedFirstRecreated` (and, via the second reader, between
            // `TrackedFirstNew` and `TrackedFirstRecreated`). It does NOT
            // change the First-vs-Repeat split, which is decided by
            // `send_starts` — still reset on the line below — and it does not
            // change the total number of missing-summary sends or bytes. The
            // affected population is only pairs that were broadcast to while
            // untracked more than INTEREST_TTL ago AND removed within the last
            // INTEREST_TTL AND whose history entry survived the LRU across
            // both; a pair with `last_observed == None` never tripped the wipe
            // and already classified correctly.
            record.send_starts = 0;
        }
        let first = record.send_starts == 0;
        let recreated = record
            .recent_removal
            .filter(|(_, removed_at)| now.saturating_duration_since(*removed_at) <= INTEREST_TTL)
            .is_some();
        // Deliberately NOT filtered by INTEREST_TTL, unlike `recreated` above:
        // the whole question this counter answers is how the age is
        // distributed, so clamping it to the TTL first would discard the tail
        // that distinguishes "recently lost its entry" from "no record at all".
        let prior_removal_age = record
            .recent_removal
            .map(|(_, removed_at)| now.saturating_duration_since(removed_at));
        let (inflight, active_tracked) = self.begin_active_attempt(&key);
        let class = if inflight {
            MissingSummaryClass::UntrackedRepeatInflight
        } else if !first {
            MissingSummaryClass::UntrackedRepeatSequential
        } else if recreated {
            MissingSummaryClass::UntrackedFirstRecreated
        } else {
            MissingSummaryClass::UntrackedFirstObserved
        };
        record.send_starts = record.send_starts.saturating_add(1);
        record.last_observed = Some(now);
        if !was_present && history.len() == MISSING_SUMMARY_HISTORY_SIZE {
            self.interest_lifecycle_metrics
                .history_overflow
                .fetch_add(1, Ordering::Relaxed);
        }
        history.put(key.clone(), record);
        PeerSummaryForBroadcast::Missing {
            reason: None,
            attempt: Some(MissingSummaryAttempt {
                key,
                class,
                first_age_bucket: None,
                untracked_prior_removal_bucket: Some(Self::untracked_prior_removal_bucket(
                    prior_removal_age,
                )),
                active_tracked,
            }),
        }
    }

    fn begin_active_attempt(&self, key: &(ContractKey, PeerKey)) -> (bool, bool) {
        // `.len()` is read BEFORE `.entry()` so no shard guard is held while
        // it runs (it read-locks every shard; doing that while holding a
        // write lock on one of them, from `entry()` below, would
        // self-deadlock — DashMap's shard RwLock is not reentrant). The one
        // `entry()` match that follows then holds a single shard's guard for
        // its whole arm, so the occupied-increment and vacant-insert are each
        // atomic per key: two callers racing on the same never-before-seen
        // key can no longer clobber each other (only the cross-key cap
        // check above stays a benign soft race, documented on the field).
        let over_cap = self.missing_summary_active.len() >= MISSING_SUMMARY_ACTIVE_SIZE;
        match self.missing_summary_active.entry(key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let count = entry.get_mut();
                let inflight = *count > 0;
                *count = count.saturating_add(1);
                (inflight, true)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                if over_cap {
                    self.interest_lifecycle_metrics
                        .active_overflow
                        .fetch_add(1, Ordering::Relaxed);
                    return (false, false);
                }
                entry.insert(1);
                (false, true)
            }
        }
    }

    fn first_send_age_bucket(age: Duration) -> usize {
        match age.as_secs() {
            0 => 0,
            1..=9 => 1,
            10..=59 => 2,
            60..=299 => 3,
            _ => 4,
        }
    }

    pub(crate) fn missing_summary_attempt_guard(
        &self,
        attempt: MissingSummaryAttempt,
    ) -> MissingSummaryAttemptGuard<'_, T> {
        MissingSummaryAttemptGuard {
            manager: self,
            attempt: Some(attempt),
            delivered_bytes: None,
        }
    }

    /// Row for `class` in `delivered_size_hist`, if it carries one.
    fn size_hist_row(class: MissingSummaryClass) -> Option<usize> {
        SIZE_HIST_CLASSES.iter().position(|c| *c == class)
    }

    /// Bucket a delivered missing-summary payload by size.
    ///
    /// Exists because the 0.2.120 investigation found that full-state BYTES
    /// rose ~63% while full-state SEND COUNT rose only ~12% (#5153). Every
    /// counter available at the time measured counts, so the axis that actually
    /// moved could not be observed at all. A mean would not have settled it
    /// either: a mean cannot distinguish "every payload grew" from "the mix
    /// shifted toward a few large contracts", and those want different fixes.
    fn delivered_size_bucket(bytes: u64) -> usize {
        match bytes {
            0..=4095 => 0,
            4096..=16383 => 1,
            16384..=65535 => 2,
            65536..=262_143 => 3,
            262_144..=1_048_575 => 4,
            _ => 5,
        }
    }

    /// Bucket an untracked send by how long ago the pair's entry was removed.
    ///
    /// `ms_age` cannot answer this: `first_age_bucket` is computed only on the
    /// tracked path and is hardcoded `None` on the untracked one, so the class
    /// that grew (`untracked_first_observed`, ~4x the population `ms_age`
    /// covers) had no age signal at all.
    ///
    /// An untracked pair has no interest entry, so it has no entry age to
    /// report. What it does have is the history record's `recent_removal`, and
    /// the distinction that matters is whether we are broadcasting to a peer
    /// that RECENTLY lost its entry (a churn/removal problem) or one we hold no
    /// removal record for. Bucket 5 is the latter.
    ///
    /// Bucket 5 is NOT "genuine first contact", for two reasons, and both bias
    /// it the same way — toward concluding that churn is not the cause:
    ///
    /// 1. It also counts REPEAT untracked sends (`UntrackedRepeat*`), which are
    ///    by definition not first contact.
    /// 2. It absorbs LRU evictions. `recent_removal` lives in
    ///    `missing_summary_history` (bounded, LRU), and `history.get` PROMOTES
    ///    on every untracked send — so the entries most likely to be evicted are
    ///    the oldest un-rebroadcast removals, which is precisely the bucket-4
    ///    tail. Bucket 4 is therefore systematically undercounted INTO bucket 5.
    ///    Cross-check against `corr_ovf[0]` before reading bucket 5 as evidence
    ///    of anything; `recreated` carries the same caveat for the same reason.
    fn untracked_prior_removal_bucket(age: Option<Duration>) -> usize {
        match age {
            Some(d) if d < Duration::from_secs(10) => 0,
            Some(d) if d < Duration::from_secs(60) => 1,
            Some(d) if d < Duration::from_secs(300) => 2,
            Some(d) if d < Duration::from_secs(1200) => 3,
            Some(_) => 4,
            None => 5,
        }
    }

    fn finish_missing_summary_attempt(
        &self,
        attempt: MissingSummaryAttempt,
        delivered_bytes: Option<u64>,
    ) {
        if attempt.active_tracked {
            // Single `entry()` match: decrement-or-remove stays atomic per
            // key, so a concurrent `begin_active_attempt` increment on this
            // exact key can never be silently dropped by this decrement.
            if let dashmap::mapref::entry::Entry::Occupied(mut entry) =
                self.missing_summary_active.entry(attempt.key.clone())
            {
                let count = entry.get_mut();
                if *count <= 1 {
                    entry.remove();
                } else {
                    *count -= 1;
                }
            }
        }
        if let Some(bytes) = delivered_bytes {
            let index = attempt.class.index();
            self.interest_lifecycle_metrics.delivered_sends[index].fetch_add(1, Ordering::Relaxed);
            self.interest_lifecycle_metrics.delivered_bytes[index]
                .fetch_add(bytes, Ordering::Relaxed);
            if let Some(bucket) = attempt.first_age_bucket {
                self.interest_lifecycle_metrics.first_send_age[bucket]
                    .fetch_add(1, Ordering::Relaxed);
            }
            if let Some(row) = Self::size_hist_row(attempt.class) {
                self.interest_lifecycle_metrics.delivered_size_hist[row]
                    [Self::delivered_size_bucket(bytes)]
                .fetch_add(1, Ordering::Relaxed);
            }
            if let Some(bucket) = attempt.untracked_prior_removal_bucket {
                self.interest_lifecycle_metrics.untracked_prior_removal_age[bucket]
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// SHADOW MODE — record that a `SyncStateToPeer` heal was actually emitted
    /// for this (contract, peer) edge.
    ///
    /// Call from the one site that emits the heal
    /// (`node::emit_stale_peer_syncs`) and only for contracts that really got
    /// one: a contract skipped for being banned, having no local state, or
    /// exceeding the per-message emit budget is not an attempt. Changes no
    /// behaviour — see [`crate::ring::futile_repair`].
    pub(crate) fn record_repair_attempt(&self, contract: &ContractKey, peer: &PeerKey) {
        self.futile_repair
            .record_repair_attempt(contract, peer, self.now());
    }

    /// SHADOW MODE — record the verdict of a TWO-SIDED summary comparison for
    /// this (contract, peer) edge, settling any outstanding repair attempt.
    ///
    /// `converged` is the anti-entropy staleness verdict inverted: pass
    /// `!is_stale` from the comparison that produced it. Only call this where
    /// BOTH sides reported a real summary — a one-sided comparison is not an
    /// outcome, because there was no divergence to repair. Changes no
    /// behaviour.
    pub(crate) fn record_repair_outcome(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        converged: bool,
    ) {
        self.futile_repair
            .record_repair_outcome(contract, peer, converged, self.now());
    }

    pub(crate) fn futile_repair_snapshot(&self) -> FutileRepairSnapshot {
        self.futile_repair.snapshot()
    }

    pub(crate) fn interest_lifecycle_snapshot(&self) -> InterestLifecycleSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let mut current_summary_state = [0u64; SummaryMissingReason::COUNT + 1];
        for contract_entry in &self.interested_peers {
            for interest in contract_entry.value().values() {
                let index = interest
                    .summary_missing_reason()
                    .map_or(0, |reason| reason.index() + 1);
                current_summary_state[index] = current_summary_state[index].saturating_add(1);
            }
        }
        InterestLifecycleSnapshot {
            delivered_sends: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.delivered_sends[i])
            }),
            delivered_bytes: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.delivered_bytes[i])
            }),
            first_send_age: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.first_send_age[i])
            }),
            delivered_size_hist: std::array::from_fn(|class| {
                std::array::from_fn(|bucket| {
                    load(&self.interest_lifecycle_metrics.delivered_size_hist[class][bucket])
                })
            }),
            untracked_prior_removal_age: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.untracked_prior_removal_age[i])
            }),
            registration_overwrite_known: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.registration_overwrite_known[i])
            }),
            registration_overwrite_missing: std::array::from_fn(|i| {
                load(
                    &self
                        .interest_lifecycle_metrics
                        .registration_overwrite_missing[i],
                )
            }),
            registration_new_known: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.registration_new_known[i])
            }),
            registration_new_missing: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.registration_new_missing[i])
            }),
            removals: std::array::from_fn(|i| load(&self.interest_lifecycle_metrics.removals[i])),
            recreated_after_removal: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.recreated_after_removal[i])
            }),
            population: std::array::from_fn(|source| {
                std::array::from_fn(|outcome| {
                    load(&self.interest_lifecycle_metrics.population[source][outcome])
                })
            }),
            registration_cap_rejected: std::array::from_fn(|i| {
                load(&self.interest_lifecycle_metrics.registration_cap_rejected[i])
            }),
            current_summary_state,
            history_overflow: load(&self.interest_lifecycle_metrics.history_overflow),
            active_overflow: load(&self.interest_lifecycle_metrics.active_overflow),
        }
    }

    /// Register a peer's interest in a contract.
    ///
    /// Returns true if this is a new interest (peer wasn't previously tracked).
    pub fn register_peer_interest(
        &self,
        contract: &ContractKey,
        peer: PeerKey,
        summary: Option<StateSummary<'static>>,
        is_upstream: bool,
    ) -> bool {
        self.register_peer_interest_from(
            contract,
            peer,
            summary,
            is_upstream,
            InterestRegistrationSource::Unknown,
        )
    }

    pub(crate) fn register_peer_interest_from(
        &self,
        contract: &ContractKey,
        peer: PeerKey,
        summary: Option<StateSummary<'static>>,
        is_upstream: bool,
        source: InterestRegistrationSource,
    ) -> bool {
        let now = self.time_source.now();
        // Hold the `interested_peers` shard guard across `peer_contracts`
        // insertion and `index_contract_hash` to keep the three writes
        // atomic against a concurrent `remove_peer_interest` (which would
        // otherwise observe a fully-removed peer and unindex a contract
        // we're about to re-index, leaving a zombie entry).
        // This intentionally undoes the PR #4129 `significant_drop_tightening`
        // change for these four sites — see PR notes.
        let mut entry = self.interested_peers.entry(*contract).or_default();
        let is_new = !entry.contains_key(&peer);

        // Cap distinct interested peers per contract to bound an adversarial
        // broadcast-amplification vector (#3798 Gap 2). Reject BEFORE the
        // reverse-index/hash writes below so a rejected peer leaves no zombie
        // `peer_contracts` / `contract_hash_index` entry. Only a NEW peer at
        // capacity is rejected — renewals of an already-tracked peer always
        // proceed so a legit at-capacity contract keeps serving its peers.
        // Returns `is_new = false` so a rejected adversary is not treated as a
        // new viable target and cannot trigger the #4359 pending-broadcast flush.
        if is_new && entry.len() >= MAX_INTERESTED_PEERS_PER_CONTRACT {
            self.interest_lifecycle_metrics.registration_cap_rejected[source.index()]
                .fetch_add(1, Ordering::Relaxed);
            drop(entry);
            tracing::warn!(
                contract = %contract,
                limit = MAX_INTERESTED_PEERS_PER_CONTRACT,
                "Interested-peer limit reached, rejecting peer"
            );
            return false;
        }

        if is_new {
            let counters = if summary.is_some() {
                &self.interest_lifecycle_metrics.registration_new_known
            } else {
                &self.interest_lifecycle_metrics.registration_new_missing
            };
            counters[source.index()].fetch_add(1, Ordering::Relaxed);
        }

        let mut interest = PeerInterest::new(summary, is_upstream, now);
        if interest.summary.is_none() {
            if let Some(previous) = entry.get(&peer) {
                if previous.summary.is_some() {
                    interest.never_populated_origin = NeverPopulatedOrigin::OverwriteKnown;
                    self.interest_lifecycle_metrics.registration_overwrite_known[source.index()]
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    interest.never_populated_origin = NeverPopulatedOrigin::OverwriteMissing;
                    self.interest_lifecycle_metrics
                        .registration_overwrite_missing[source.index()]
                    .fetch_add(1, Ordering::Relaxed);
                }
            } else {
                let recreated = self
                    .missing_summary_history
                    .lock()
                    .get(&(*contract, peer.clone()))
                    .and_then(|history| history.recent_removal)
                    .filter(|(_, removed_at)| {
                        now.saturating_duration_since(*removed_at) <= INTEREST_TTL
                    });
                interest.never_populated_origin = NeverPopulatedOrigin::New {
                    recreated: recreated.is_some(),
                };
                if let Some((cause, _)) = recreated {
                    self.interest_lifecycle_metrics.recreated_after_removal[cause.index()]
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        entry.insert(peer.clone(), interest);

        // Maintain reverse index for O(1) peer disconnect cleanup
        self.peer_contracts
            .entry(peer)
            .or_default()
            .insert(*contract);

        // Also index by hash for fast lookup
        self.index_contract_hash(contract);

        drop(entry);
        is_new
    }

    /// Remove a peer's interest in a contract.
    ///
    /// Returns true if the peer was actually removed.
    pub fn remove_peer_interest(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        self.remove_peer_interest_for(contract, peer, InterestRemovalCause::Unknown)
    }

    pub(crate) fn remove_peer_interest_for(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        cause: InterestRemovalCause,
    ) -> bool {
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            let removed_interest = entry.remove(peer);
            let removed = removed_interest.is_some();

            if removed {
                self.interest_lifecycle_metrics.removals[cause.index()]
                    .fetch_add(1, Ordering::Relaxed);
                let now = self.time_source.now();
                let key = (*contract, peer.clone());
                let mut history = self.missing_summary_history.lock();
                let was_present = history.peek(&key).is_some();
                let mut record = history.get(&key).copied().unwrap_or_default();
                if let Some(interest) = removed_interest.as_ref()
                    && interest.summary_missing_reason()
                        == Some(SummaryMissingReason::NeverPopulated)
                {
                    record.send_starts =
                        record.send_starts.max(interest.never_populated_send_starts);
                }
                record.recent_removal = Some((cause, now));
                if !was_present && history.len() == MISSING_SUMMARY_HISTORY_SIZE {
                    self.interest_lifecycle_metrics
                        .history_overflow
                        .fetch_add(1, Ordering::Relaxed);
                }
                history.put(key, record);

                // Maintain reverse index
                if let Some(mut peer_entry) = self.peer_contracts.get_mut(peer) {
                    peer_entry.remove(contract);
                    if peer_entry.is_empty() {
                        drop(peer_entry);
                        self.peer_contracts.remove_if(peer, |_, v| v.is_empty());
                    }
                }
            }

            // Clean up empty entries using remove_if to avoid race condition
            // between dropping the entry guard and removing the contract.
            if entry.is_empty() {
                drop(entry);
                self.interested_peers
                    .remove_if(contract, |_, v| v.is_empty());
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }

            removed
        } else {
            false
        }
    }

    /// Update a peer's summary for a contract and refresh TTL.
    pub fn update_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        summary: StateSummary<'static>,
    ) {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.set_summary(summary, now);
            }
        }
    }

    /// Drop a peer's cached summary for a contract, recording why.
    ///
    /// Every clear MUST name its cause: a tracked peer with no cached summary
    /// is what forces a full-state broadcast, and #4961 could not tell the
    /// three clear paths apart in the rollup. The `reason` is surfaced on the
    /// `FullNoTheirSummaryTracked` arm of `broadcast_payload_mix`.
    ///
    /// Like [`Self::update_peer_summary`], this is a silent no-op for an
    /// untracked peer — clearing something we never cached is a no-op by
    /// definition, and creating an entry just to hold `None` would inflate the
    /// map from unauthenticated input.
    pub fn clear_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        reason: SummaryMissingReason,
    ) {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.clear_summary(reason, now);
            }
        }
    }

    /// Cache a peer's known summary, creating the interest entry when absent.
    ///
    /// [`Self::update_peer_summary`] deliberately no-ops for an untracked peer
    /// (pinned by `update_peer_summary_is_a_silent_noop_for_an_untracked_peer`)
    /// so summary writes of unknown provenance cannot grow the map. This upsert
    /// exists for the callers that KNOW the peer holds the summarized state:
    /// the post-delivery cache in `broadcast_queue::record_delivery_to_interest`
    /// (we just delivered exactly that state to them) and the InterestSync
    /// `Summaries` handler (the peer itself reported the summary). Without it,
    /// an advertised co-host that is untracked at broadcast time is a
    /// full-state fixed point: every broadcast to it ships full state, the
    /// post-delivery summary write silently no-ops, and the next broadcast
    /// ships full state again (#4952 — 58% of fleet broadcast bytes).
    ///
    /// The insert respects [`MAX_INTERESTED_PEERS_PER_CONTRACT`] (returns
    /// `false` at cap with no side writes, same shape as
    /// [`Self::register_peer_interest`]) and creates the entry with
    /// `is_upstream = false`. It does NOT touch the demand counters
    /// (`downstream_subscriber_count` / `local_client_count`) that feed
    /// eviction's demand ranking — an upserted entry is summary bookkeeping
    /// plus fan-out of the small `Summaries` notifications, never a state
    /// broadcast target (Source-2 removal, `update.rs::get_broadcast_targets_update`).
    pub fn upsert_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        summary: StateSummary<'static>,
    ) -> bool {
        self.upsert_peer_summary_from(contract, peer, summary, SummaryPopulationSource::Unknown)
            != SummaryPopulationOutcome::RejectedAtCap
    }

    pub(crate) fn upsert_peer_summary_from(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        summary: StateSummary<'static>,
        source: SummaryPopulationSource,
    ) -> SummaryPopulationOutcome {
        let now = self.time_source.now();
        // Hold the `interested_peers` shard guard across the `peer_contracts`
        // and hash-index writes — same #4129/#4171 discipline as
        // `register_peer_interest`, preventing a concurrent remover from
        // leaving a zombie reverse-index entry.
        let mut entry = self.interested_peers.entry(*contract).or_default();
        if let Some(interest) = entry.get_mut(peer) {
            let outcome = if interest.summary.is_some() {
                SummaryPopulationOutcome::RefreshedKnown
            } else {
                SummaryPopulationOutcome::FilledMissing
            };
            interest.set_summary(summary, now);
            self.missing_summary_history
                .lock()
                .pop(&(*contract, peer.clone()));
            self.interest_lifecycle_metrics.population[source.index()][outcome.index()]
                .fetch_add(1, Ordering::Relaxed);
            return outcome;
        }
        if entry.len() >= MAX_INTERESTED_PEERS_PER_CONTRACT {
            // At cap the entry is non-empty, so no cleanup is needed; the
            // caller simply keeps sending full state to this peer (pre-upsert
            // behavior), bounded per contract. debug! (compiled out of
            // release) rather than register_peer_interest's warn!: this runs
            // per delivered broadcast, and the condition is near-unreachable
            // in practice (entries require connected peers, and
            // max_connections < the 512 cap), but a stuck-at-cap contract
            // should be diagnosable in a dev build.
            tracing::debug!(
                contract = %contract,
                limit = MAX_INTERESTED_PEERS_PER_CONTRACT,
                "upsert_peer_summary: at interested-peer cap, peer stays untracked (full-state sends continue)"
            );
            let outcome = SummaryPopulationOutcome::RejectedAtCap;
            self.interest_lifecycle_metrics.population[source.index()][outcome.index()]
                .fetch_add(1, Ordering::Relaxed);
            return outcome;
        }
        entry.insert(peer.clone(), PeerInterest::new(Some(summary), false, now));
        self.peer_contracts
            .entry(peer.clone())
            .or_default()
            .insert(*contract);
        self.index_contract_hash(contract);
        self.missing_summary_history
            .lock()
            .pop(&(*contract, peer.clone()));
        let outcome = SummaryPopulationOutcome::CreatedUntracked;
        self.interest_lifecycle_metrics.population[source.index()][outcome.index()]
            .fetch_add(1, Ordering::Relaxed);
        drop(entry);
        outcome
    }

    /// Refresh the TTL for a peer's interest, leaving `is_upstream` and any
    /// cached summary untouched.
    ///
    /// Returns `true` if an entry existed and was refreshed, `false` if there
    /// was nothing to refresh — so a caller can express refresh-if-present /
    /// register-if-absent in ONE map acquisition.
    ///
    /// The `get_peer_interest(..).is_some()` form this return value replaces was
    /// wrong twice over. [`Self::get_peer_interest`] returns an owned
    /// [`PeerInterest`], so testing presence deep-copied the cached
    /// `StateSummary` — up to ~840 KB of alloc+memcpy per call for exactly the
    /// state-sized-summary contracts that make this path expensive — and threw
    /// the clone away. And the two lookups are not atomic: a
    /// [`Self::remove_peer_interest`] landing between them makes the refresh a
    /// silent no-op *and* skips the register, so the caller's interest is never
    /// recorded at all.
    pub fn refresh_peer_interest(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.refresh(now);
                return true;
            }
        }
        false
    }

    /// Refresh the TTL for a peer's interest **and** set its `is_upstream`
    /// flag, preserving any cached summary.
    ///
    /// Exists for the subscribe paths, which must assert upstream-ness on an
    /// entry that may already exist. Their only previous option was a bare
    /// [`Self::register_peer_interest`], which inserts a fresh
    /// [`PeerInterest::new`] over the existing one and therefore **wipes the
    /// cached delta-sync summary** — the entry then reports
    /// [`SummaryMissingReason::NeverPopulated`], which is both wrong (it WAS
    /// populated) and expensive (every subsequent broadcast to that peer falls
    /// back to full state until the summary is re-seeded).
    ///
    /// That matters at renewal cadence: `SUBSCRIPTION_RENEWAL_INTERVAL` is 120s
    /// against an 8-minute lease, and a renewal re-registers through the same
    /// outbound-SUBSCRIBE machinery as a client request, so an unguarded call
    /// site clobbers roughly 30 times per subscribed contract per hour.
    ///
    /// Deliberately SETS `is_upstream` rather than leaving it alone: the bare
    /// `register_peer_interest` this replaces also set it, and the flag is the
    /// `Unsubscribe` routing target. Only the summary-preservation behaviour
    /// changes. Do NOT "simplify" this into [`Self::refresh_peer_interest`] —
    /// that one intentionally leaves the flag untouched, and the two call-site
    /// families rely on the difference.
    ///
    /// Returns `true` if an entry existed and was updated, `false` if there was
    /// nothing to refresh (the caller should then register).
    pub fn refresh_peer_interest_with_upstream(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        is_upstream: bool,
    ) -> bool {
        let now = self.time_source.now();
        if let Some(mut entry) = self.interested_peers.get_mut(contract) {
            if let Some(interest) = entry.get_mut(peer) {
                interest.refresh(now);
                interest.is_upstream = is_upstream;
                return true;
            }
        }
        false
    }

    /// Get all peers interested in a contract.
    pub fn get_interested_peers(&self, contract: &ContractKey) -> Vec<(PeerKey, PeerInterest)> {
        let mut peers: Vec<(PeerKey, PeerInterest)> = self
            .interested_peers
            .get(contract)
            .map(|entry| entry.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        // Sort by PeerKey bytes for deterministic ordering (critical for simulation tests)
        peers.sort_by(|(a, _), (b, _)| a.0.as_bytes().cmp(b.0.as_bytes()));
        peers
    }

    /// Get a specific peer's interest info for a contract.
    pub fn get_peer_interest(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
    ) -> Option<PeerInterest> {
        self.interested_peers
            .get(contract)
            .and_then(|entry| entry.get(peer).cloned())
    }

    /// Get all contracts a peer has interest entries for.
    ///
    /// Uses the `peer_contracts` reverse index for O(1) lookup.
    /// Used by the heartbeat handler to implement full-replace semantics.
    pub fn get_contracts_for_peer(&self, peer: &PeerKey) -> HashSet<ContractKey> {
        self.peer_contracts
            .get(peer)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    /// Get the peer's cached summary for a contract.
    pub fn get_peer_summary(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
    ) -> Option<StateSummary<'static>> {
        self.interested_peers
            .get(contract)
            .and_then(|entry| entry.get(peer).and_then(|i| i.summary.clone()))
    }

    /// Whether we hold a cached summary for `peer` on `contract`, WITHOUT
    /// cloning it.
    ///
    /// [`Self::get_peer_summary`] clones the summary; the broadcast queue only
    /// needs the yes/no answer to predict whether the wire payload will be a
    /// delta or the full state, and it asks once per fan-out TARGET on the
    /// event-loop path, so the clone is pure waste there. This is the same
    /// DashMap read without it.
    pub fn has_peer_summary(&self, contract: &ContractKey, peer: &PeerKey) -> bool {
        self.interested_peers
            .get(contract)
            .is_some_and(|entry| entry.get(peer).is_some_and(|i| i.summary.is_some()))
    }

    /// Check if enough time has elapsed to send a proactive summary notification
    /// for this contract. Returns `true` if at least 100ms has passed since the last
    /// notification (or if no notification was ever sent). Updates the timestamp on success.
    ///
    /// This prevents flooding peers with summary notifications when multiple broadcasts
    /// are applied in rapid succession.
    pub fn should_send_summary_notification(&self, contract: &ContractKey) -> bool {
        let now = self.time_source.now();
        let min_interval = Duration::from_millis(100);

        let mut entry = self.summary_notify_timestamps.entry(*contract).or_insert(
            // Use a timestamp far in the past so the first check always succeeds
            now - min_interval - Duration::from_millis(1),
        );

        if now.duration_since(*entry.value()) >= min_interval {
            *entry.value_mut() = now;
            true
        } else {
            false
        }
    }

    /// RESERVE the per-(contract, target) queue-full `ResyncRequest` throttle
    /// window: atomically checks AND records the send under the throttle's own
    /// lock. Returns `Some(deadline)` — where `deadline` is the instant this
    /// reservation window closes (`now + RESYNC_REQUEST_MIN_INTERVAL`) on the
    /// manager's `TimeSource` — when at least [`RESYNC_REQUEST_MIN_INTERVAL`]
    /// has elapsed since the last such request (or none was ever sent), or
    /// `None` when still throttled.
    ///
    /// The `begin`/[`Self::cancel_resync_request`] reservation-commit pair
    /// restores atomicity under the double-gate (#4864 round-6 item 2): recording
    /// under the lock means two concurrent queue-full callbacks for the same
    /// (contract, target) cannot both pass, so they cannot both consume the global
    /// burst and emit duplicates inside the window. If a later gate (the global
    /// per-contract emit cap) then rejects, the caller `cancel`s the reservation
    /// so the window is released — preserving the round-5 improvement that a
    /// globally-suppressed emit does not burn the 30s window.
    ///
    /// The returned deadline lets a caller that performs a bounded retry burst
    /// (the UPDATE queue-full path, #4857/#4862 P2) anchor every retry to THIS
    /// reservation window on the SAME clock the throttle stamped it with, so a
    /// burst can never spill into or overlap the next reservation — keeping the
    /// #4251 steady-state cap rigorous even if the caller's first dispatch
    /// blocked for a large fraction of the window before retrying.
    ///
    /// Issue #4857: a `ContractQueueFull` broadcast drop is silent — the receiver
    /// never applied the delta, but the SENDER cached its own summary as ours on
    /// send-Ok, so it believes we are current and will never re-send the dropped
    /// change. A `ResyncRequest` makes the sender clear that cached summary and
    /// re-send full state. Issue #4251 suppressed the request entirely because
    /// one-per-dropped-delta amplifies into a full-state storm; this gate keeps
    /// the healing signal but bounds it to one per window.
    pub fn begin_resync_request(
        &self,
        contract: &ContractKey,
        target: SocketAddr,
    ) -> Option<Instant> {
        let now = self.time_source.now();
        let key = (*contract, target);
        let mut throttle = self.resync_request_throttle.lock();
        if let Some(&last) = throttle.get(&key) {
            if now.duration_since(last) < RESYNC_REQUEST_MIN_INTERVAL {
                return None;
            }
        }
        throttle.put(key, now);
        Some(now + RESYNC_REQUEST_MIN_INTERVAL)
    }

    /// Try to reserve a slot for a queue-full-resync retry task (#4862 P1).
    ///
    /// Returns a [`ResyncRetrySlot`] guard (which frees the slot on drop) when
    /// fewer than [`MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES`] retry tasks are
    /// currently outstanding, or `None` when at cap. This bounds the aggregate
    /// number of concurrent retry tasks node-wide even when the per-(contract,
    /// peer) throttle LRU evicts active reservations under key churn (which would
    /// otherwise let each revisited key spawn another task without bound). When
    /// `None`, the caller skips the retry only — the immediate `ResyncRequest`
    /// still sends. Hard cap (CAS loop): never exceeds the maximum.
    pub(crate) fn try_reserve_resync_retry_slot(&self) -> Option<ResyncRetrySlot> {
        let mut cur = self.resync_retry_slots.load(Ordering::Relaxed);
        loop {
            if cur >= MAX_OUTSTANDING_QUEUE_FULL_RESYNC_RETRIES {
                return None;
            }
            match self.resync_retry_slots.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(ResyncRetrySlot(self.resync_retry_slots.clone())),
                Err(actual) => cur = actual,
            }
        }
    }

    /// Current number of outstanding queue-full-resync retry tasks (#4862 P1).
    #[cfg(test)]
    pub(crate) fn outstanding_resync_retries(&self) -> usize {
        self.resync_retry_slots.load(Ordering::Relaxed)
    }

    /// Cancel a reservation made by [`Self::begin_resync_request`]: removes the
    /// just-recorded throttle window for (contract, target) so the sender can
    /// retry as soon as the OTHER gate (the global emit cap) refills, rather than
    /// waiting out the full [`RESYNC_REQUEST_MIN_INTERVAL`] (#4864 round-6 item 2).
    /// Call only after a `begin` that returned `true` whose send was then rejected
    /// downstream.
    pub fn cancel_resync_request(&self, contract: &ContractKey, target: SocketAddr) {
        self.resync_request_throttle
            .lock()
            .pop(&(*contract, target));
    }

    /// Remove all interests for a peer (called on peer disconnect).
    ///
    /// Uses the reverse index for O(1) lookup instead of O(contracts) scan.
    /// Returns the number of contracts from which the peer was actually removed.
    ///
    /// # Concurrency
    ///
    /// This is a *secondary-origin* remover: it starts from the
    /// `peer_contracts` reverse index. It does NOT remove the
    /// `peer_contracts` entry up front and then mutate `interested_peers`
    /// directly — that older shape had a bidirectional-consistency race
    /// (issue #4174): a concurrent `register_peer_interest(C, peer, ..)`
    /// running between the up-front `peer_contracts.remove` and the
    /// per-contract `interested_peers` mutation could re-insert `peer`
    /// into both maps, after which this method would strip `peer` from
    /// `interested_peers[C]` while leaving the reverse entry in
    /// `peer_contracts[peer]` behind.
    ///
    /// Instead it merely *snapshots* the contract set and delegates each
    /// per-contract cleanup to `remove_peer_interest`, which holds the
    /// `interested_peers[contract]` shard guard across the matching
    /// `peer_contracts` update — so every per-contract removal is
    /// atomic and the invariant
    /// `peer ∈ peer_contracts[peer] ⇔ peer ∈ interested_peers[contract]`
    /// is preserved even under a concurrent re-registration.
    pub fn remove_all_peer_interests(&self, peer: &PeerKey) -> usize {
        self.remove_all_peer_interests_for(peer, InterestRemovalCause::Unknown)
    }

    fn remove_all_peer_interests_for(&self, peer: &PeerKey, cause: InterestRemovalCause) -> usize {
        // Snapshot the contracts this peer is interested in WITHOUT
        // removing the reverse-index entry — `remove_peer_interest`
        // owns the `peer_contracts` update for each contract so the
        // two maps stay consistent (issue #4174).
        let contracts: Vec<ContractKey> = self
            .peer_contracts
            .get(peer)
            .map(|entry| entry.value().iter().cloned().collect())
            .unwrap_or_default();

        // Delegate each per-contract cleanup to `remove_peer_interest`,
        // which atomically updates both `interested_peers` and
        // `peer_contracts` under the contract's shard guard and also
        // runs `cleanup_contract_if_no_interest`. Count only the
        // contracts from which the peer was actually removed (a
        // concurrent `remove_peer_interest` for the same pair may have
        // already cleared an entry between the snapshot and here).
        let removed_count = contracts
            .iter()
            .filter(|contract| self.remove_peer_interest_for(contract, peer, cause))
            .count();

        if removed_count > 0 {
            tracing::debug!(removed_count, "Removed peer interests on disconnect");
        }

        removed_count
    }

    /// Schedule deferred removal of a peer's interests after a grace period.
    ///
    /// Instead of immediately wiping interests on disconnect, this records a deadline.
    /// The sweep task will execute the actual removal after the grace period expires.
    /// If the peer reconnects before the deadline (via `cancel_deferred_removal`),
    /// interests are preserved — avoiding permanent interest loss during connection blips.
    pub fn schedule_deferred_removal(&self, peer: &PeerKey) {
        let deadline = self.time_source.now() + INTEREST_DISCONNECT_GRACE_PERIOD;
        self.pending_removals.insert(peer.clone(), deadline);
        tracing::debug!(
            peer = %peer.0,
            grace_secs = INTEREST_DISCONNECT_GRACE_PERIOD.as_secs(),
            "Scheduled deferred interest removal"
        );
    }

    /// Cancel a pending deferred removal for a reconnecting peer.
    ///
    /// Returns true if a pending removal was cancelled (peer reconnected in time).
    pub fn cancel_deferred_removal(&self, peer: &PeerKey) -> bool {
        let cancelled = self.pending_removals.remove(peer).is_some();
        if cancelled {
            tracing::debug!(
                peer = %peer.0,
                "Cancelled deferred interest removal — peer reconnected"
            );
        }
        cancelled
    }

    /// Execute any deferred removals whose grace period has expired.
    ///
    /// Called by the sweep task alongside expired-interest cleanup.
    /// Returns the number of peers whose interests were removed.
    pub fn execute_pending_removals(&self) -> usize {
        let now = self.time_source.now();
        let expired_peers: Vec<PeerKey> = self
            .pending_removals
            .iter()
            .filter(|entry| now >= *entry.value())
            .map(|entry| entry.key().clone())
            .collect();

        let mut executed = 0;
        for peer in &expired_peers {
            // Atomically remove from pending_removals. If `cancel_deferred_removal`
            // already removed it (peer reconnected between collect and here), skip
            // the interest removal to avoid a TOCTOU race.
            if self.pending_removals.remove(peer).is_some() {
                let removed =
                    self.remove_all_peer_interests_for(peer, InterestRemovalCause::DisconnectGrace);
                tracing::info!(
                    peer = %peer.0,
                    removed_interests = removed,
                    "Executed deferred interest removal — peer did not reconnect"
                );
                executed += 1;
            }
        }
        executed
    }

    /// Register local interest in a contract (for tracking our reasons).
    ///
    /// Currently unused inside the workspace but kept `pub` for external
    /// consumers; same lock-across-index discipline as
    /// [`Self::register_local_hosting`] applies so the method is not a
    /// PR #4129–shaped race footgun.
    pub fn register_local_interest(&self, contract: &ContractKey) -> &Self {
        let entry = self.local_interests.entry(*contract).or_default();
        self.index_contract_hash(contract);
        drop(entry);
        self
    }

    /// Register that we're hosting a contract locally.
    /// Returns true if this caused us to become interested (wasn't interested before).
    pub fn register_local_hosting(&self, contract: &ContractKey) -> bool {
        // Hold the `local_interests` shard guard across `index_contract_hash`
        // so a concurrent `remove_local_client` / `unregister_local_hosting`
        // for the last reason cannot run its cleanup (unindex no-op) before
        // we index, leaving a zombie entry in `contract_hash_index`.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let was_interested = entry.is_interested();
        entry.hosting = true;
        self.index_contract_hash(contract);
        drop(entry);
        !was_interested
    }

    /// Unregister that we're hosting a contract locally.
    /// Returns true if this caused us to lose interest (no other reasons remain).
    pub fn unregister_local_hosting(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            entry.hosting = false;
            let lost_interest = !entry.is_interested();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Add a local client subscription.
    /// Returns true if this caused us to become interested.
    pub fn add_local_client(&self, contract: &ContractKey) -> bool {
        // Same lock-across-index discipline as `register_local_hosting`:
        // hold the `local_interests` shard guard across
        // `index_contract_hash` to prevent a concurrent
        // `remove_local_client` from unindexing-before-we-index.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_client();
        self.index_contract_hash(contract);
        drop(entry);
        became_interested
    }

    /// Remove a local client subscription.
    /// Returns true if this caused us to lose interest.
    pub fn remove_local_client(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            let lost_interest = entry.remove_client();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Add a downstream subscriber.
    /// Returns true if this caused us to become interested.
    pub fn add_downstream_subscriber(&self, contract: &ContractKey) -> bool {
        // Same lock-across-index discipline as `register_local_hosting`.
        let mut entry = self.local_interests.entry(*contract).or_default();
        let became_interested = entry.add_downstream();
        self.index_contract_hash(contract);
        drop(entry);
        became_interested
    }

    /// Remove a downstream subscriber.
    /// Returns true if this caused us to lose interest.
    pub fn remove_downstream_subscriber(&self, contract: &ContractKey) -> bool {
        if let Some(mut entry) = self.local_interests.get_mut(contract) {
            let lost_interest = entry.remove_downstream();
            if lost_interest {
                drop(entry);
                self.local_interests.remove(contract);
                // Clean up hash index if no interest remains
                self.cleanup_contract_if_no_interest(contract);
            }
            lost_interest
        } else {
            false
        }
    }

    /// Mirror a subscriber-primary eviction that tore down a still-in-use
    /// contract's hosting subscription state (#4642 invariant 3, PR #4734).
    ///
    /// The `InterestManager` lives on `OpManager`, NOT on `HostingManager`, so
    /// when `HostingManager::teardown_evicted_in_use_contract` clears the
    /// hosting maps (`downstream_subscribers` + `client_subscriptions`) the
    /// eviction CONSUMER must replay the identical removals here or ghost
    /// `interested_peers` / `peer_contracts` / `local_client_count` entries
    /// survive. Those ghosts are load-bearing — they drive UPDATE broadcast
    /// targeting (`get_interested_peers`) and upstream interest counts — and do
    /// NOT self-heal, because the reconcilers iterate the very hosting maps the
    /// teardown just emptied.
    ///
    /// Mirrors, exactly:
    /// - `handle_unsubscribe_inbound` per downstream peer: `remove_peer_interest`
    ///   (clears `interested_peers` / `peer_contracts`) + `remove_downstream_subscriber`
    ///   (decrements the local `downstream_subscriber_count`).
    /// - the client-disconnect path per local client: `remove_local_client`
    ///   (decrements `local_client_count`).
    ///
    /// Idempotent and safe on an already-clean contract (each removal is a
    /// no-op when absent).
    pub fn remove_evicted_in_use(
        &self,
        contract: &ContractKey,
        downstream_peers: &[PeerKey],
        local_client_count: usize,
    ) {
        for peer in downstream_peers {
            self.remove_peer_interest_for(contract, peer, InterestRemovalCause::Eviction);
            self.remove_downstream_subscriber(contract);
        }
        for _ in 0..local_client_count {
            self.remove_local_client(contract);
        }
    }

    /// Get or create local interest entry, returning mutable reference.
    pub fn with_local_interest<F, R>(&self, contract: &ContractKey, f: F) -> R
    where
        F: FnOnce(&mut LocalInterest) -> R,
    {
        let mut entry = self.local_interests.entry(*contract).or_default();
        f(entry.value_mut())
    }

    /// Check if we have any local interest in a contract.
    pub fn has_local_interest(&self, contract: &ContractKey) -> bool {
        self.local_interests
            .get(contract)
            .map(|entry| entry.is_interested())
            .unwrap_or(false)
    }

    /// Count contracts backed by *real demand*: a local client subscription or
    /// a downstream subscriber. This deliberately EXCLUDES the cache-only
    /// `hosting` reason, so it does not grow with the hosting cache.
    ///
    /// This is the denominator for the #3763 no-storm invariant: renewal /
    /// subscription volume must scale with active demand, not with cache size.
    /// `LocalInterest::is_interested()` (which folds in `hosting`) is the wrong
    /// signal for that check — see the sim assertions in
    /// `simulation_integration.rs` and the unit test
    /// `test_contracts_needing_renewal_bounded_by_active_interest`.
    ///
    /// Test/sim-only accessor (reached via `Ring::active_demand_count`).
    #[cfg(any(test, feature = "testing"))]
    pub fn active_demand_count(&self) -> usize {
        self.local_interests
            .iter()
            .filter(|entry| {
                let li = entry.value();
                li.local_client_count > 0 || li.downstream_subscriber_count > 0
            })
            .count()
    }

    /// Remove local interest entry if no longer interested.
    pub fn cleanup_local_interest(&self, contract: &ContractKey) {
        if let Some(entry) = self.local_interests.get(contract) {
            if !entry.is_interested() {
                drop(entry);
                self.local_interests.remove(contract);
            }
        }
    }

    /// Sweep expired peer interests.
    ///
    /// Returns list of (contract, peer) pairs that were removed.
    pub fn sweep_expired_interests(&self) -> Vec<(ContractKey, PeerKey)> {
        let now = self.time_source.now();
        let mut expired = Vec::new();

        // Collect and sort contracts for deterministic iteration order
        let mut contracts: Vec<_> = self
            .interested_peers
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        contracts.sort_by(|(a, _), (b, _)| a.id().as_bytes().cmp(b.id().as_bytes()));

        for (contract, peers_map) in contracts {
            // Collect and sort peers for deterministic iteration order
            let mut peers_to_remove: Vec<PeerKey> = peers_map
                .iter()
                .filter(|(_, interest)| interest.is_expired_at(now))
                .map(|(peer, _)| peer.clone())
                .collect();
            peers_to_remove.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

            for peer in peers_to_remove {
                expired.push((contract, peer));
            }
        }

        // Remove expired entries
        for (contract, peer) in &expired {
            self.remove_peer_interest_for(contract, peer, InterestRemovalCause::TtlExpiry);
        }

        if !expired.is_empty() {
            tracing::debug!(
                expired_count = expired.len(),
                "Interest sweep: removed expired entries"
            );
        }

        expired
    }

    /// Start the background sweep task for expired peer interests.
    ///
    /// This spawns a task that runs periodically to clean up expired entries.
    /// Should be called once after the interest manager is set up.
    ///
    /// Note: The sweep interval uses real time (tokio::time) for scheduling,
    /// but expiration checking uses the TimeSource. In tests, manually call
    /// `sweep_expired_interests()` after advancing mock time.
    pub fn start_sweep_task(manager: std::sync::Arc<Self>)
    where
        T: Send + Sync + 'static,
    {
        GlobalExecutor::spawn(Self::sweep_task(manager));
    }

    /// Background task to sweep expired peer interests.
    async fn sweep_task(manager: std::sync::Arc<Self>)
    where
        T: Send + Sync + 'static,
    {
        // Add random initial delay to prevent synchronized sweeps across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(10u64..=30u64));
        tokio::time::sleep(initial_delay).await;

        let mut interval = tokio::time::interval(INTEREST_SWEEP_INTERVAL);
        interval.tick().await; // Skip first immediate tick

        loop {
            interval.tick().await;

            // Execute any deferred removals whose grace period has expired
            manager.execute_pending_removals();

            // Capture stats before sweep for the health snapshot
            let stats = manager.stats();
            let expired = manager.sweep_expired_interests();

            if !expired.is_empty() {
                tracing::info!(
                    expired_count = expired.len(),
                    "Interest sweep: cleaned up expired peer interests"
                );

                // Emit per-entry expiration telemetry
                for (contract, peer) in &expired {
                    crate::tracing::telemetry::send_standalone_event(
                        "interest_expired",
                        serde_json::json!({
                            "contract": contract.to_string(),
                            "peer": peer.0.to_string(),
                        }),
                    );
                }
            }

            // Emit periodic health snapshot
            crate::tracing::telemetry::send_standalone_event(
                "subscription_health_snapshot",
                serde_json::json!({
                    "contracts_with_interests": stats.total_contracts,
                    "total_interest_entries": stats.total_peer_interests,
                    "expired_this_sweep": expired.len(),
                }),
            );
        }
    }

    /// Index a contract by its hash for fast lookup.
    fn index_contract_hash(&self, contract: &ContractKey) {
        let hash = contract_hash(contract);
        let mut entry = self.contract_hash_index.entry(hash).or_default();
        // Only add if not already present (dedup without Ord)
        if !entry.contains(contract) {
            entry.push(*contract);
        }
    }

    /// Remove a contract from the hash index.
    fn unindex_contract_hash(&self, contract: &ContractKey) {
        let hash = contract_hash(contract);
        if let Some(mut entry) = self.contract_hash_index.get_mut(&hash) {
            entry.retain(|c| c != contract);
            if entry.is_empty() {
                drop(entry);
                self.contract_hash_index.remove(&hash);
            }
        }
    }

    /// Clean up hash index for a contract if there's no remaining interest.
    /// Called after removing peer or local interest.
    fn cleanup_contract_if_no_interest(&self, contract: &ContractKey) {
        let has_peer_interest = self.interested_peers.contains_key(contract);
        let has_local_interest = self.has_local_interest(contract);

        if !has_peer_interest && !has_local_interest {
            self.unindex_contract_hash(contract);
            // Clean up summary notification timestamp when no interest remains
            self.summary_notify_timestamps.remove(contract);
        }
    }

    /// Look up contracts by hash. Returns all contracts that hash to this value
    /// (handles collisions by returning multiple candidates).
    pub fn lookup_by_hash(&self, hash: u32) -> Vec<ContractKey> {
        self.contract_hash_index
            .get(&hash)
            .as_deref()
            .cloned()
            .unwrap_or_default()
    }

    /// Get all contract hashes we're interested in.
    ///
    /// Uses the existing hash index for O(1) access - no rehashing needed.
    pub fn get_all_interest_hashes(&self) -> Vec<u32> {
        let mut hashes: Vec<u32> = self.contract_hash_index.iter().map(|e| *e.key()).collect();
        // Sort for deterministic ordering (critical for simulation tests)
        hashes.sort_unstable();
        hashes
    }

    /// Get contracts we're interested in that match the given hashes.
    pub fn get_matching_contracts(&self, hashes: &[u32]) -> Vec<ContractKey> {
        let hash_set: std::collections::HashSet<u32> = hashes.iter().copied().collect();

        let mut contracts: Vec<ContractKey> = self
            .contract_hash_index
            .iter()
            .filter(|entry| hash_set.contains(entry.key()))
            .flat_map(|entry| entry.value().clone())
            .collect();
        // Sort by contract ID bytes for deterministic ordering (critical for simulation tests)
        contracts.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        contracts
    }

    /// Index at which `peer`'s next full-bytes fallback window starts, given
    /// the shared-contract set in [`Self::get_matching_contracts`] order
    /// (ascending by contract id).
    ///
    /// Mid-cycle this resumes immediately after the last contract SENT to that
    /// peer, which is what makes successive windows contiguous in id space
    /// (see [`first_index_after`]). At a cycle BOUNDARY — no cursor yet, the
    /// cursor evicted or lost, or the previous window ending on the highest id
    /// — the next cycle starts at a RANDOM offset rather than at 0.
    ///
    /// A fixed restart at 0 is the failure this codebase has already rejected
    /// twice, for the same reason each time: see the rotations in
    /// `emit_stale_peer_syncs` and in the `SummaryDigests` arm, both of which
    /// note that a fixed prefix "would starve the tail on every cycle
    /// forever". The two ways to land back at a boundary repeatedly are both
    /// reachable here, and neither needs an attacker:
    ///
    /// - The cursor is in-memory and keyed by address, so it is lost on our
    ///   own restart, on LRU eviction, and whenever a peer reconnects from a
    ///   new source port. A peer that reconnects more often than one cycle
    ///   completes would, with a fixed restart, only ever be told about the
    ///   head of the set.
    /// - `sorted` is the INTERSECTION with the hash list the peer advertised,
    ///   so the peer influences where the cursor lands. Advertising a single
    ///   high-id contract parks the cursor at the end, and the following full
    ///   round would restart at 0 — repeat, and everything past the first
    ///   window is never advertised.
    ///
    /// Randomising the boundary costs nothing in the ordinary case: a cycle
    /// beginning at any offset still advances contiguously and still covers
    /// the whole set in `ceil(len / limit)` rounds, because the window wraps.
    /// It only removes the guarantee that the SAME contracts are the ones
    /// covered first every time.
    ///
    /// `GlobalRng` keeps this deterministic under simulation and test.
    pub(crate) fn fallback_window_start(&self, peer: SocketAddr, sorted: &[ContractKey]) -> usize {
        if sorted.is_empty() {
            return 0;
        }
        let after = { self.summary_fallback_cursor.lock().peek(&peer).copied() };
        let resumed = after.map(|after| first_index_after(sorted, &after));
        match resumed {
            // Mid-cycle: continue exactly where the last reply stopped.
            Some(start) if start < sorted.len() => start,
            // Cycle boundary (cursor absent, or exhausted past the end).
            _ => crate::config::GlobalRng::random_range(0..sorted.len()),
        }
    }

    /// Record the contract id of the last entry actually included in `peer`'s
    /// fallback reply, so the next reply resumes after it.
    ///
    /// Takes what was SENT, not what was selected: the byte budget can cut a
    /// window short, and advancing past entries we dropped would skip them
    /// until the rotation wrapped all the way round.
    pub(crate) fn record_fallback_cursor(&self, peer: SocketAddr, last_sent: ContractInstanceId) {
        self.summary_fallback_cursor.lock().put(peer, last_sent);
    }

    /// Test accessor for the stored cursor.
    #[cfg(test)]
    pub(crate) fn peek_fallback_cursor(&self, peer: SocketAddr) -> Option<ContractInstanceId> {
        self.summary_fallback_cursor.lock().peek(&peer).copied()
    }
}

/// First index in `sorted` (ascending by contract id, as
/// [`InterestManager::get_matching_contracts`] returns it) whose id is strictly
/// greater than `after`. Returns `sorted.len()` when `after` is at or past the
/// end, which [`InterestManager::fallback_window_start`] reads as the end of a
/// cycle and answers with a fresh random offset.
///
/// This is the churn-safe half of the rotation. Because it is a binary search
/// over ids rather than a stored offset, it behaves correctly when the set
/// changed since the cursor was recorded:
///
/// - The cursor's own contract removed: nothing else moves relative to the
///   remaining ids, so the search still lands on the same successor.
/// - A contract inserted BELOW the cursor: it sorts before the resume point and
///   is picked up when the rotation wraps, not skipped.
/// - A contract inserted ABOVE the cursor: it is inside the very next window.
///
/// An index-based cursor gets the removal case wrong: the position it named
/// now holds a different contract, so the round steps over one. Wrapping means
/// that contract is seen again eventually, so this is not permanent
/// starvation — it is the loss of the `ceil(n / limit)` bound, which is the
/// only thing that makes bounding the reply defensible in the first place.
pub(crate) fn first_index_after(sorted: &[ContractKey], after: &ContractInstanceId) -> usize {
    sorted.partition_point(|c| c.id().as_bytes() <= after.as_bytes())
}

/// Indices of the next rotation window: up to `limit` entries beginning at
/// `start`, wrapping past the end of the set.
///
/// Wrapping is what makes the coverage argument hold. A window that stopped at
/// the end of the set would give the tail a shorter turn than the head on every
/// cycle; wrapping makes each round advance by a full `limit` entries.
///
/// That gives `ceil(len / limit)` rounds to cover a stable set — but ONLY when
/// every selected entry is actually sent. The caller may send fewer than the
/// window it asked for (the byte budget in `node.rs` cuts a reply once it has
/// spent its allowance) and then records the cursor against what it SENT, so
/// the real cycle length is set by bytes rather than by entry count whenever
/// summaries are large. See `MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY` for the
/// honest bound; do not quote `ceil(len / limit)` as the cycle time without
/// checking which of the two limits binds.
pub(crate) fn rotation_window_indices(len: usize, start: usize, limit: usize) -> Vec<usize> {
    if len == 0 || limit == 0 {
        return Vec::new();
    }
    let start = if start >= len { 0 } else { start };
    (0..limit.min(len)).map(|i| (start + i) % len).collect()
}

impl<T: TimeSource + Sync> InterestManager<T> {
    /// Cache a computed delta for reuse.
    pub fn cache_delta(
        &self,
        contract: &ContractKey,
        peer_summary: &[u8],
        our_summary: &[u8],
        delta: StateDelta<'static>,
    ) {
        let key = DeltaCacheKey {
            contract: *contract,
            peer_summary_hash: hash_bytes(peer_summary),
            our_summary_hash: hash_bytes(our_summary),
        };
        self.delta_cache.lock().put(key, delta);
    }

    /// Look up a cached delta.
    pub fn get_cached_delta(
        &self,
        contract: &ContractKey,
        peer_summary: &[u8],
        our_summary: &[u8],
    ) -> Option<StateDelta<'static>> {
        let key = DeltaCacheKey {
            contract: *contract,
            peer_summary_hash: hash_bytes(peer_summary),
            our_summary_hash: hash_bytes(our_summary),
        };
        self.delta_cache.lock().get(&key).cloned()
    }

    /// Get the current state summary for a contract.
    ///
    /// Uses the contract handler to compute the summary via the contract's
    /// `summarize_state` method.
    pub async fn get_contract_summary(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
    ) -> Option<StateSummary<'static>> {
        use crate::contract::ContractHandlerEvent;

        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetSummaryQuery { key: *key },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetSummaryResponse { summary: Ok(s), .. }) => Some(s),
            Ok(ContractHandlerEvent::GetSummaryResponse {
                summary: Err(e), ..
            }) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Failed to get contract summary"
                );
                None
            }
            Ok(other) => {
                tracing::warn!(
                    contract = %key,
                    response = ?other,
                    "Unexpected response to GetSummaryQuery"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Error getting contract summary"
                );
                None
            }
        }
    }

    /// Get the size (in bytes) of the locally-stored state for a contract.
    ///
    /// Mirrors [`get_contract_summary`](Self::get_contract_summary): a bounded
    /// (`BROADCAST_CH_TIMEOUT`) `GetQuery` against the contract handler,
    /// returning the stored state's `size()` or `None` if it can't be read.
    /// Used by the summary-first PUT reverse leg to feed
    /// [`compute_delta`](Self::compute_delta)'s post-compute efficiency check
    /// with the holder's own state size (the holder-side mirror of the
    /// originator's `merged_value.size()`).
    pub async fn get_contract_state_size(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
    ) -> Option<usize> {
        use crate::contract::ContractHandlerEvent;

        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetQuery {
                    instance_id: *key.id(),
                    return_contract_code: false,
                },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetResponse {
                response: Ok(store_response),
                ..
            }) => store_response.state.map(|state| state.size()),
            Ok(ContractHandlerEvent::GetResponse {
                response: Err(e), ..
            }) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Failed to get contract state size"
                );
                None
            }
            Ok(other) => {
                tracing::warn!(
                    contract = %key,
                    response = ?other,
                    "Unexpected response to GetQuery (state size)"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Error getting contract state size"
                );
                None
            }
        }
    }

    /// Compute a state delta for a peer given their cached summary.
    ///
    /// Uses the contract handler to compute the delta via the contract's
    /// `get_state_delta` method (bounded by `BROADCAST_CH_TIMEOUT`). Results
    /// are cached (keyed by contract + both summaries) to avoid recomputation
    /// for peers with the same summary.
    ///
    /// Returns `Ok(None)` when the contract returns an empty delta (zero bytes),
    /// meaning the peer's state is logically equivalent to ours despite differing
    /// summary bytes (e.g., due to non-deterministic serialization order).
    ///
    /// Returns [`DeltaUnavailable::NotEfficient`] when the COMPUTED delta is
    /// not smaller than our full state (`delta.len() >= our_state_size`), so
    /// the caller's full-state fallback is genuinely optimal. Until #4923 this
    /// refusal fired BEFORE computing anything, off the [`is_delta_efficient`]
    /// summary-size proxy (`summary * 2 >= state`) — but the fallback to a
    /// refused delta is sending FULL STATE, which is never smaller than the
    /// delta that was declined, so the pre-compute gate could only ever trade
    /// one bounded WASM call for strictly more wire bytes. In production that
    /// arm was 41% of ALL network wire bytes. The gate now runs post-compute,
    /// on the real delta size, on both the cache-hit and fresh-compute paths.
    ///
    /// # Arguments
    /// * `our_summary` - Our current state summary (used for cache key)
    /// * `our_state_size` - Size of our current state (for the post-compute
    ///   efficiency check)
    pub async fn compute_delta(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
        their_summary: &StateSummary<'static>,
        our_summary: &StateSummary<'static>,
        our_state_size: usize,
    ) -> Result<Option<StateDelta<'static>>, DeltaUnavailable> {
        use crate::contract::ContractHandlerEvent;

        // Use slices directly - cache methods hash internally, no allocation needed
        let their_summary_bytes = their_summary.as_ref();
        let our_summary_bytes = our_summary.as_ref();

        // Check cache first (keyed by hash of contract + summaries)
        if let Some(cached) = self.get_cached_delta(key, their_summary_bytes, our_summary_bytes) {
            if cached.as_ref().is_empty() {
                tracing::trace!(contract = %key, "Cached empty delta (no change)");
                return Ok(None);
            }
            tracing::trace!(contract = %key, "Using cached delta");
            // The post-compute wire gate applies to cached deltas too: an
            // oversized delta cached here (or by the staleness probe, which
            // shares this cache and never gates) must produce the same
            // NotEfficient refusal a fresh computation would — otherwise a
            // cache hit would hand the caller a payload larger than the full
            // state it exists to avoid.
            return Self::gate_delta_size(cached, their_summary_bytes.len(), our_state_size);
        }

        // Compute delta via contract handler (short timeout for broadcast
        // path). No pre-compute size gate here — see the method docs (#4923):
        // refusing to compute forces a full-state send that is never smaller
        // than the delta being declined, so the only correct place to judge
        // efficiency is on the ACTUAL computed delta, below.
        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetDeltaQuery {
                    key: *key,
                    their_summary: their_summary.clone(),
                },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Ok(d), .. }) => {
                if d.as_ref().is_empty() {
                    // Empty delta means no change needed — cache it so we don't
                    // re-invoke the contract on subsequent broadcast cycles
                    self.cache_delta(key, their_summary_bytes, our_summary_bytes, d);
                    tracing::trace!(
                        contract = %key,
                        "Contract returned empty delta (no change)"
                    );
                    Ok(None)
                } else {
                    // Cache the result (includes contract key to prevent
                    // cross-contract pollution) BEFORE the size gate, even when
                    // the delta is oversized — deliberately:
                    // 1. `cached_staleness_verdict` maps any NON-EMPTY cached
                    //    delta to "peer is stale", which is correct here: an
                    //    oversized delta is still a genuine divergence, so the
                    //    fan-out must still send (it will just send full state).
                    //    Not caching would instead force the staleness path
                    //    back through a WASM probe.
                    // 2. Memoization: the next compute_delta for the same
                    //    (contract, summaries) pair hits the cache above and
                    //    re-applies the same gate — a consistent NotEfficient
                    //    verdict with zero further WASM work, instead of
                    //    re-running the contract on every fan-out target.
                    self.cache_delta(key, their_summary_bytes, our_summary_bytes, d.clone());
                    Self::gate_delta_size(d, their_summary_bytes.len(), our_state_size)
                }
            }
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Err(e), .. }) => Err(
                DeltaUnavailable::ComputeFailed(format!("Delta computation failed: {}", e)),
            ),
            Ok(other) => Err(DeltaUnavailable::ComputeFailed(format!(
                "Unexpected response to GetDeltaQuery: {:?}",
                other
            ))),
            Err(e) => Err(DeltaUnavailable::ComputeFailed(format!(
                "Error computing delta: {}",
                e
            ))),
        }
    }

    /// Post-compute wire-efficiency gate (#4923): hand back the computed
    /// (non-empty) delta unless full state would be smaller by at least
    /// [`MIN_FULL_STATE_SAVING_BYTES`], in which case refuse with
    /// [`DeltaUnavailable::NotEfficient`] so the caller's full-state fallback
    /// is taken as the genuinely cheaper payload.
    ///
    /// The margin is load-bearing, not slop. A bare `delta.len() >=
    /// state_size` comparison is byte-optimal but behaviorally wrong at small
    /// sizes: a 144-byte delta against a 136-byte state would flip the payload
    /// to full state to save EIGHT bytes, and doing that for every small
    /// contract re-creates the full-state fan-out shape that #4233 exists to
    /// prevent (`test_sustained_update_fanout_no_full_state_storm` pins
    /// `delta_sends > full_state_sends` and catches exactly this). Deltas are
    /// also what keeps a receiver's peer-summary cache warm, so trading them
    /// away for a rounding error is a bad deal even ignoring the pin.
    ///
    /// So the rule is: prefer the delta by default, and switch to full state
    /// only when that genuinely saves bandwidth worth the switch. The
    /// pathological case this gate exists for — a contract whose summary (and
    /// therefore delta) is state-sized, the #4956 poisoned-summary population
    /// running at 550-840 KB — clears a 1 KiB margin by orders of magnitude.
    ///
    /// Note for callers whose fallback is NOT full state: a refusal here means
    /// the summary-first PUT reverse leg
    /// (`put::op_ctx_task::reverse_delta_from_compute_result`) ships NOTHING,
    /// not full state, and the originator heals later via GET/anti-entropy.
    /// That asymmetry predates this change (the old pre-compute gate refused
    /// the same way) but the margin makes it much rarer.
    fn gate_delta_size(
        delta: StateDelta<'static>,
        summary_size: usize,
        our_state_size: usize,
    ) -> Result<Option<StateDelta<'static>>, DeltaUnavailable> {
        if delta.as_ref().len() >= our_state_size.saturating_add(MIN_FULL_STATE_SAVING_BYTES) {
            tracing::trace!(
                delta_size = delta.as_ref().len(),
                state_size = our_state_size,
                margin = MIN_FULL_STATE_SAVING_BYTES,
                "Computed delta exceeds full state by more than the switch \
                 margin — caller should send full state"
            );
            Err(DeltaUnavailable::NotEfficient {
                summary_size,
                state_size: our_state_size,
            })
        } else {
            Ok(Some(delta))
        }
    }

    /// In-memory-only staleness verdict derived from the shared delta cache.
    ///
    /// Returns `Some(true)` if a cached delta for the `(their_summary,
    /// our_summary)` pair is non-empty (the peer is missing state we hold),
    /// `Some(false)` if it is empty (logically converged despite differing
    /// summary bytes), or `None` if no delta is cached (the caller must fall
    /// back to a contract round-trip via [`peer_summary_has_pending_state`]).
    ///
    /// This touches only the in-process LRU cache — never the contract handler
    /// loop — so it is safe to call on the hot heartbeat path.
    pub fn cached_staleness_verdict(
        &self,
        key: &ContractKey,
        their_summary: &[u8],
        our_summary: &[u8],
    ) -> Option<bool> {
        self.get_cached_delta(key, their_summary, our_summary)
            .map(|delta| !delta.as_ref().is_empty())
    }

    /// Ask the contract whether our state holds anything the peer's summary
    /// lacks — the semantic form of "is this peer stale?" used by the
    /// InterestSync heartbeat in place of a raw summary byte comparison.
    ///
    /// Returns `Some(true)` when the contract's `get_state_delta` yields a
    /// non-empty delta (genuine divergence), `Some(false)` when it is empty
    /// (converged despite differing summary bytes — the non-deterministic
    /// serialization case that drove the #4857 summarize storm), or `None`
    /// when the delta could not be computed (caller falls back to the byte
    /// comparison via [`summary_indicates_stale_peer`]).
    ///
    /// Unlike [`compute_delta`](Self::compute_delta) this deliberately does NOT
    /// apply the post-compute wire-efficiency gate: staleness detection wants
    /// the semantic answer (empty vs non-empty) regardless of the delta's
    /// SIZE, because the alternative it replaces is a spurious FULL-STATE heal
    /// on every heartbeat — strictly more expensive than one delta
    /// computation. (Since #4923 `compute_delta` also always runs the
    /// contract; the remaining difference is only that it refuses to RETURN a
    /// delta that is not smaller than full state, while this probe has no
    /// notion of size at all.)
    ///
    /// Steady-state cost: the result rides the SAME delta cache as
    /// `compute_delta` (keyed by contract + both summary hashes). Note that
    /// outer cache is BYTE-keyed, so under non-deterministic summary
    /// serialization it *can* miss even for an unchanged pair. The real reason
    /// the per-heartbeat load stays flat is upstream of this cache: (a) contract
    /// summaries are memoized OUTSIDE the WASM boundary keyed on a
    /// state-change-detector hash (`bridged_summarize_contract_state`), so a
    /// peer's `our_summary`/`their_summary` bytes are STABLE while state is
    /// unchanged — which keeps this cache's byte key stable across heartbeats —
    /// and (b) the executor-level delta cache is keyed on `state_hash`
    /// (`bridged_get_contract_state_delta`), so even a byte-key miss here elides
    /// the WASM call when the state has not changed. The per-message probe
    /// budget (`MAX_STALENESS_PROBES_PER_SUMMARIES`) bounds the residual
    /// cold-cache worst case. Net: a converged pair costs at most one WASM
    /// `get_state_delta` per state change, not one per heartbeat.
    ///
    /// Convergence caveat: the `Some(false)` "converged" verdict is only as
    /// correct as the contract's `get_state_delta` being a correct semilattice
    /// diff (empty delta iff our state adds nothing over their summary). A
    /// contract with a buggy diff could under-report divergence here exactly as
    /// it already would on the broadcast delta-optimization path; this reuses
    /// that same, pre-existing trust assumption rather than adding a new one.
    pub async fn peer_summary_has_pending_state(
        &self,
        op_manager: &crate::node::OpManager,
        key: &ContractKey,
        their_summary: &StateSummary<'static>,
        our_summary: &StateSummary<'static>,
    ) -> Option<bool> {
        use crate::contract::ContractHandlerEvent;

        let their_bytes = their_summary.as_ref();
        let our_bytes = our_summary.as_ref();

        // Fast path: shared in-memory delta cache, no contract round-trip.
        if let Some(verdict) = self.cached_staleness_verdict(key, their_bytes, our_bytes) {
            return Some(verdict);
        }

        // Slow path: ask the contract for the delta of our state against their
        // summary. `GetDeltaQuery` is the same event `compute_delta` uses, and
        // we cache the result under the same key so both paths share it.
        //
        // Priority: this runs at the DEFAULT (NetworkRelay) priority that
        // `notify_contract_handler_with_timeout` uses — deliberately NOT
        // `Priority::Background`. A Background probe would be SHED first under
        // contract-handler saturation, returning `None` → the caller falls back
        // to the byte-compare, which flags the (converged-but-byte-differing)
        // peer stale and fires a FULL-STATE heal — re-enabling the very storm
        // this probe suppresses, exactly when the node is most loaded. The probe
        // is strictly cheaper than the heal it prevents, so it must run even
        // under load; the per-message `MAX_STALENESS_PROBES_PER_SUMMARIES` cap
        // (not de-prioritization) is what bounds its cost.
        match op_manager
            .notify_contract_handler_with_timeout(
                ContractHandlerEvent::GetDeltaQuery {
                    key: *key,
                    their_summary: their_summary.clone(),
                },
                BROADCAST_CH_TIMEOUT,
            )
            .await
        {
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Ok(d), .. }) => {
                let has_change = !d.as_ref().is_empty();
                self.cache_delta(key, their_bytes, our_bytes, d);
                Some(has_change)
            }
            Ok(ContractHandlerEvent::GetDeltaResponse { delta: Err(e), .. }) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Staleness delta probe failed — falling back to summary byte comparison"
                );
                None
            }
            Ok(other) => {
                tracing::warn!(
                    contract = %key,
                    response = ?other,
                    "Unexpected response to GetDeltaQuery (staleness probe)"
                );
                None
            }
            Err(e) => {
                tracing::debug!(
                    contract = %key,
                    error = %e,
                    "Error computing staleness delta probe — falling back to byte comparison"
                );
                None
            }
        }
    }

    /// Get statistics about the interest manager state.
    pub fn stats(&self) -> InterestManagerStats {
        let total_contracts = self.interested_peers.len();
        let total_peer_interests: usize = self
            .interested_peers
            .iter()
            .map(|entry| entry.value().len())
            .sum();
        let local_interests = self.local_interests.len();
        let hash_index_size = self.contract_hash_index.len();

        InterestManagerStats {
            total_contracts,
            total_peer_interests,
            local_interests,
            hash_index_size,
            delta_sends: self.delta_sends.load(Ordering::Relaxed),
            full_state_sends: self.full_state_sends.load(Ordering::Relaxed),
            delta_bytes_saved: self.delta_bytes_saved.load(Ordering::Relaxed),
            resync_requests_received: self.resync_requests_received.load(Ordering::Relaxed),
        }
    }
}

/// Statistics about the interest manager state.
#[derive(Debug, Clone)]
pub struct InterestManagerStats {
    /// Number of contracts with at least one interested peer.
    pub total_contracts: usize,
    /// Total number of peer interest entries across all contracts.
    pub total_peer_interests: usize,
    /// Number of contracts with local interest.
    pub local_interests: usize,
    /// Size of the contract hash index.
    pub hash_index_size: usize,
    /// Number of times a delta was sent instead of full state.
    pub delta_sends: u64,
    /// Number of times full state was sent.
    pub full_state_sends: u64,
    /// Total bytes saved by sending deltas.
    pub delta_bytes_saved: u64,
    /// Number of ResyncRequests received (indicates delta failures at remote peers).
    /// With correct summary caching (PR #2763), this should be zero in normal operation.
    pub resync_requests_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;
    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    /// Type alias for tests using mock time
    type TestInterestManager = InterestManager<SharedMockTimeSource>;

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    /// Like `make_contract_key` but with a `u32` seed for tests that need
    /// many distinct contracts.
    fn make_unique_contract_key(seed: u32) -> ContractKey {
        let s = seed.to_le_bytes();
        let mut id = [0u8; 32];
        id[0..4].copy_from_slice(&s);
        let mut code = [0u8; 32];
        code[0..4].copy_from_slice(&s);
        code[4] = 0xAB;
        ContractKey::from_id_and_code(ContractInstanceId::new(id), CodeHash::new(code))
    }

    /// Build a deterministic peer key from a seed.
    ///
    /// Deterministic-and-distinct so tests never rely on RNG distinctness:
    /// distinct seeds always yield distinct keys, and the same seed always
    /// yields the same key (mirrors the sibling `hosting.rs` test helper).
    fn make_peer_key(seed: u8) -> PeerKey {
        make_unique_peer_key(seed as u32)
    }

    /// Like `make_peer_key` but with a `u32` seed for tests that need more
    /// than 256 pairwise-distinct peers (mirrors `make_unique_contract_key`).
    fn make_unique_peer_key(seed: u32) -> PeerKey {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&seed.to_le_bytes());
        PeerKey(crate::transport::TransportPublicKey::from_bytes(bytes))
    }

    fn make_manager() -> (TestInterestManager, SharedMockTimeSource) {
        let time_source = SharedMockTimeSource::new();
        let manager = InterestManager::new(time_source.clone());
        (manager, time_source)
    }

    /// [`summary_digest`] must be a FIXED function of the bytes — identical on
    /// every node, every platform, and every release (#4965).
    ///
    /// Pinned against a hard-coded vector, not merely "two calls agree". Two
    /// calls agree for `DefaultHasher` too *within one process*, and it is
    /// exactly the cross-process disagreement that would be catastrophic here:
    /// a per-node digest makes every comparison mismatch, which degrades to
    /// "ship the bytes AND an extra round trip" — worse than not doing this at
    /// all — and re-creates the #4857 shape where every heartbeat looks like
    /// divergence. Only a pinned vector catches a swap to a non-stable hasher.
    ///
    /// If BLAKE3 itself is ever intentionally replaced, this vector changes AND
    /// `HASH_FIRST_SUMMARIES_MIN_VERSION` must be re-floored, because peers on
    /// either side of the change would disagree about identical state.
    #[test]
    fn summary_digest_is_pinned_to_truncated_blake3() {
        let got = summary_digest(b"freenet summary digest test vector");

        // The TRUNCATION rule: first SUMMARY_DIGEST_LEN bytes, not last, not
        // folded. This half is computed with blake3, so it moves with the
        // implementation — the hard pin is the literal below.
        let truncated = {
            let full = blake3::hash(b"freenet summary digest test vector");
            let mut d = [0u8; SUMMARY_DIGEST_LEN];
            d.copy_from_slice(&full.as_bytes()[..SUMMARY_DIGEST_LEN]);
            d
        };
        assert_eq!(
            got, truncated,
            "summary_digest must be the FIRST {SUMMARY_DIGEST_LEN} bytes of \
             BLAKE3 over the raw summary bytes"
        );

        // The ALGORITHM, pinned to a literal. Verified independently of this
        // codebase with the `b3sum` CLI:
        //   printf 'freenet summary digest test vector' | b3sum
        // Swapping BLAKE3 for anything else fails here even though the
        // truncation assertion above would happily follow the swap.
        assert_eq!(
            hex::encode(got),
            "c6d93b99cde492c9edc177db79b27e2b",
            "summary_digest changed value for a fixed input — this is a WIRE \
             change: peers on either side of it disagree about identical \
             state. If deliberate, re-floor HASH_FIRST_SUMMARIES_MIN_VERSION."
        );
    }

    /// Two independently constructed but byte-identical summaries must digest
    /// identically, and any difference must change the digest.
    ///
    /// The first half is the property the exchange relies on for its 98.1%
    /// win; the second is what keeps a digest match from hiding real
    /// divergence.
    #[test]
    fn summary_digest_agrees_on_equal_bytes_and_differs_otherwise() {
        // Built by different routes so no shared allocation can mask a bug.
        let a: Vec<u8> = (0u8..64).collect();
        let mut b = Vec::with_capacity(64);
        for i in 0u8..64 {
            b.push(i);
        }
        assert_eq!(a, b, "precondition: the two summaries are byte-identical");
        assert_eq!(
            summary_digest(&a),
            summary_digest(&b),
            "identical summary bytes MUST digest identically — two peers that \
             agree must see a digest match"
        );

        let mut c = a.clone();
        c[63] ^= 0x01;
        assert_ne!(
            summary_digest(&a),
            summary_digest(&c),
            "a one-bit difference must change the digest, or divergence goes \
             undetected and the heal never fires"
        );

        assert_ne!(
            summary_digest(b""),
            summary_digest(&a),
            "an empty summary must not digest like a populated one"
        );
    }

    /// [`summary_digest`] and [`contract_hash`] answer different questions and
    /// must not be confused: the digest tracks STATE (so it changes when state
    /// changes) while `contract_hash` identifies a CONTRACT (so it never does).
    ///
    /// `SummaryDigestEntry` carries both, and overloading either one for the
    /// other's job is the obvious way to get this wrong.
    #[test]
    fn summary_digest_and_contract_hash_are_different_functions() {
        let contract = make_contract_key(1);
        let h = contract_hash(&contract);

        // Same contract, two different states → same contract hash, different
        // digests.
        let d1 = summary_digest(b"state one");
        let d2 = summary_digest(b"state two");
        assert_eq!(
            h,
            contract_hash(&contract),
            "contract_hash must not depend on state"
        );
        assert_ne!(
            d1, d2,
            "summary_digest MUST depend on state — a digest that tracked the \
             contract id instead would report 'agree' for every peer holding \
             the contract, silently disabling every heal"
        );

        // And the digest must not be a widened contract hash.
        assert_ne!(
            &summary_digest(contract.id().as_bytes())[..4],
            &h.to_le_bytes()[..],
            "summary_digest must not reduce to contract_hash"
        );
    }

    #[test]
    fn test_register_and_remove_peer_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        assert!(manager.register_peer_interest(&contract, peer.clone(), None, false));

        // Duplicate registration returns false
        assert!(!manager.register_peer_interest(&contract, peer.clone(), None, false));

        // Verify interest exists
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Remove interest
        assert!(manager.remove_peer_interest(&contract, &peer));

        // Verify removed
        assert!(manager.get_peer_interest(&contract, &peer).is_none());

        // Remove again returns false
        assert!(!manager.remove_peer_interest(&contract, &peer));
    }

    /// The SIZE histogram must bucket by the delivered payload, per class.
    ///
    /// The 0.2.120 investigation could not tell "every payload grew" from "the
    /// mix shifted toward large contracts", because every counter measured
    /// COUNTS. A histogram answers both; a mean would answer neither.
    #[test]
    fn delivered_size_histogram_buckets_by_payload_and_class() {
        let (manager, _time) = make_manager();

        // Two DIFFERENT untracked pairs, so both sends are first-observed, with
        // payloads that must land in different buckets of the same row.
        for (seed, bytes) in [(41u8, 512usize), (45u8, 200_000usize)] {
            let contract = make_contract_key(seed);
            let peer = make_peer_key(seed);
            let PeerSummaryForBroadcast::Missing {
                attempt: Some(attempt),
                ..
            } = manager.begin_peer_summary_broadcast(&contract, &peer)
            else {
                panic!("an untracked pair must start lifecycle accounting");
            };
            assert_eq!(attempt.class, MissingSummaryClass::UntrackedFirstObserved);
            let mut guard = manager.missing_summary_attempt_guard(attempt);
            guard.mark_delivered(bytes);
            drop(guard);
        }

        let snap = manager.interest_lifecycle_snapshot();
        // Literal, NOT `size_hist_row(..)`. Deriving the expected row from the
        // function under test makes the assertion self-referential: permuting
        // SIZE_HIST_CLASSES would keep this green while silently mislabelling
        // every ms_size row against the order router.rs promises the dashboard.
        let row = 2;
        assert_eq!(
            SIZE_HIST_CLASSES,
            [
                MissingSummaryClass::TrackedFirstNew,
                MissingSummaryClass::TrackedFirstRecreated,
                MissingSummaryClass::UntrackedFirstObserved,
                MissingSummaryClass::UntrackedFirstRecreated,
            ],
            "ms_size row order is a WIRE contract with the dashboard \
             (router.rs documents it); reordering silently reattributes every \
             row to the wrong class"
        );
        // 512 B -> bucket 0; 200 KB -> bucket 3 (65536..=262_143).
        assert_eq!(
            snap.delivered_size_hist[row][0], 1,
            "the 512-byte send must be counted in bucket 0 (<4 KiB)"
        );
        assert_eq!(
            snap.delivered_size_hist[row][3], 1,
            "the 200KiB send must land in bucket 3 — separating these two is \
             the whole point, since a mean cannot tell 'everything grew' from \
             'the mix shifted toward large contracts'"
        );

        // The narrowing is deliberate: a REPEAT send carries no size row.
        assert!(
            TestInterestManager::size_hist_row(MissingSummaryClass::UntrackedRepeatSequential)
                .is_none(),
            "repeat classes are a measured zero fleet-wide and are excluded to \
             stay inside the network_efficiency_v1 byte budget"
        );
    }

    /// Every bucket boundary of both classifiers, including the exact edges.
    ///
    /// The arms are hardcoded literals, so an off-by-one at an edge is invisible
    /// to any test that only samples interior values — which is all the others
    /// did.
    #[test]
    fn bucket_classifiers_are_exact_at_every_boundary() {
        type M = TestInterestManager;

        // Size: contiguous, no gap, no overlap, exhaustive over u64.
        for (bytes, want) in [
            (0u64, 0usize),
            (4095, 0),
            (4096, 1),
            (16_383, 1),
            (16_384, 2),
            (65_535, 2),
            (65_536, 3),
            (262_143, 3),
            (262_144, 4),
            (1_048_575, 4),
            (1_048_576, 5),
            (u64::MAX, 5),
        ] {
            assert_eq!(
                M::delivered_size_bucket(bytes),
                want,
                "size bucket for {bytes} bytes"
            );
        }

        // Age: strict `<`, so each stated edge belongs to the NEXT bucket up.
        // The 1200s edge is INTEREST_TTL, which is what makes bucket 4 mean
        // "older than the adjacent `recreated` filter would have kept".
        for (secs, want) in [
            (0u64, 0usize),
            (9, 0),
            (10, 1),
            (59, 1),
            (60, 2),
            (299, 2),
            (300, 3),
            (1199, 3),
            (1200, 4),
            (86_400, 4),
        ] {
            assert_eq!(
                M::untracked_prior_removal_bucket(Some(Duration::from_secs(secs))),
                want,
                "age bucket for {secs}s"
            );
        }
        assert_eq!(
            M::untracked_prior_removal_bucket(None),
            5,
            "no removal record is its own bucket, never folded into the tail"
        );
        assert_eq!(
            Duration::from_secs(1200),
            INTEREST_TTL,
            "the 1200s edge is INTEREST_TTL by intent, not coincidence; if the \
             TTL moves, bucket 4 stops meaning 'beyond what `recreated` keeps'"
        );
    }

    /// The age read must NOT be clamped by `INTEREST_TTL`.
    ///
    /// This is the one novel decision in the change: `recreated` (three lines
    /// above the read) filters by `INTEREST_TTL`, and this deliberately does
    /// not, because the whole question is how the age is DISTRIBUTED and
    /// clamping would discard the tail that separates "recently lost its entry"
    /// from "no record at all".
    ///
    /// Without this test the decision is unpinned: adding the filter back moves
    /// bucket 4's contents into bucket 5 and every other test stays green.
    #[test]
    fn untracked_age_keeps_the_tail_beyond_interest_ttl() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(46);
        let peer = make_peer_key(46);

        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.remove_peer_interest_for(
            &contract,
            &peer,
            InterestRemovalCause::DisconnectGrace
        ));

        // Well past INTEREST_TTL, so a TTL-filtered read would report None.
        time.advance_time(INTEREST_TTL + Duration::from_secs(600));

        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("untracked pair");
        };
        let mut guard = manager.missing_summary_attempt_guard(attempt);
        guard.mark_delivered(1024);
        drop(guard);

        let snap = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snap.untracked_prior_removal_age[4], 1,
            "a removal older than INTEREST_TTL must stay in the older-than-20m \
             bucket. If this lands in bucket 5 the read has been clamped by the \
             TTL and the counter can no longer tell a churned pair from one it \
             has no record of"
        );
        assert_eq!(
            snap.untracked_prior_removal_age[5], 0,
            "and must NOT be folded into the no-record bucket"
        );
    }

    /// A send that is never delivered must not be counted in the histogram.
    ///
    /// `delivered_bytes` gates the existing counters, and the new histogram has
    /// to sit inside the same gate or it would over-count exactly the sends the
    /// byte counters deliberately exclude.
    #[test]
    fn undelivered_send_is_absent_from_the_size_histogram() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(42);
        let peer = make_peer_key(42);

        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("an untracked pair must start lifecycle accounting");
        };
        // Dropped WITHOUT mark_delivered.
        drop(manager.missing_summary_attempt_guard(attempt));

        let snap = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snap.delivered_size_hist.iter().flatten().sum::<u64>(),
            0,
            "an undelivered send must not appear in the size histogram"
        );
        assert_eq!(
            snap.untracked_prior_removal_age.iter().sum::<u64>(),
            0,
            "nor in the untracked-age histogram"
        );
    }

    /// The untracked-age histogram must separate "recently lost its entry" from
    /// "no record of one at all".
    ///
    /// This is the population `ms_age` structurally cannot see: `first_age_bucket`
    /// is computed only on the tracked path, so `untracked_first_observed` — the
    /// class that grew under 0.2.120, and ~4x the population `ms_age` covers —
    /// had no age signal whatsoever.
    #[test]
    fn untracked_age_histogram_separates_recent_removal_from_no_record() {
        let (manager, time) = make_manager();

        // Pair A: never seen before -> the "no record" bucket (5).
        let contract_a = make_contract_key(43);
        let peer_a = make_peer_key(43);
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract_a, &peer_a)
        else {
            panic!("untracked pair");
        };
        let mut guard = manager.missing_summary_attempt_guard(attempt);
        guard.mark_delivered(1024);
        drop(guard);

        // Pair B: tracked, removed, then broadcast to 30s later -> bucket 1
        // (<1m), NOT the no-record bucket.
        let contract_b = make_contract_key(44);
        let peer_b = make_peer_key(44);
        manager.register_peer_interest(&contract_b, peer_b.clone(), None, false);
        assert!(manager.remove_peer_interest_for(
            &contract_b,
            &peer_b,
            InterestRemovalCause::DisconnectGrace
        ));
        time.advance_time(Duration::from_secs(30));
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract_b, &peer_b)
        else {
            panic!("untracked pair");
        };
        let mut guard = manager.missing_summary_attempt_guard(attempt);
        guard.mark_delivered(1024);
        drop(guard);

        let snap = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snap.untracked_prior_removal_age[5], 1,
            "a pair with no removal record must land in the no-record bucket"
        );
        assert_eq!(
            snap.untracked_prior_removal_age[1], 1,
            "a pair whose entry was removed 30s ago must land in the <1m \
             bucket, which is the whole distinction this counter exists to draw"
        );
    }

    /// The untracked-path staleness reset must not wipe `recent_removal`.
    ///
    /// `last_observed` is stamped only when a pair is broadcast to while
    /// UNTRACKED, so it measures a different clock from the removal. Wiping the
    /// removal alongside the send counter made a pair whose removal was recent,
    /// but whose last untracked observation was stale, report
    /// `UntrackedFirstObserved` ("never seen before") rather than
    /// `UntrackedFirstRecreated`. Without the fix, step T2 below observes
    /// `UntrackedFirstObserved`.
    ///
    /// Three things are pinned, because each guards a different way this can
    /// regress:
    /// - T2: a recent removal survives a stale-`last_observed` reset.
    /// - T3: a removal older than `INTEREST_TTL` is still refused — the
    ///   `removed_at` filter is the remaining guard, so the fix cannot drift
    ///   into honouring an arbitrarily old removal.
    /// - T4: the SECOND reader (`register_peer_interest_from`) sees it too.
    ///   That reader drives `recreated_after_removal` and the tracked-arm
    ///   `TrackedFirstNew`/`TrackedFirstRecreated` split, so it steps on this
    ///   change as well and would otherwise be unpinned.
    #[test]
    fn untracked_staleness_reset_preserves_a_recent_removal() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(15);
        let peer = make_peer_key(15);

        // T0: broadcast to an untracked pair — stamps `last_observed`.
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("an untracked pair must start lifecycle accounting");
        };
        assert_eq!(attempt.class, MissingSummaryClass::UntrackedFirstObserved);
        drop(manager.missing_summary_attempt_guard(attempt));

        // T1 = T0 + 21m: the pair becomes tracked and is then torn down. This is
        // PAST `INTEREST_TTL` from T0, so the next untracked observation trips
        // the staleness reset.
        time.advance_time(INTEREST_TTL + Duration::from_secs(60));
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.remove_peer_interest_for(
            &contract,
            &peer,
            InterestRemovalCause::DisconnectGrace
        ));

        // T2 = T1 + 1m: the removal is RECENT, even though the last untracked
        // observation is not.
        time.advance_time(Duration::from_secs(60));
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("the pair is untracked again");
        };
        assert_eq!(
            attempt.class,
            MissingSummaryClass::UntrackedFirstRecreated,
            "a removal one minute ago must classify as a recreation regardless of \
             how long ago the pair was last observed untracked"
        );
        drop(manager.missing_summary_attempt_guard(attempt));

        // T3 = T2 + 21m: the removal is now STALE. The surviving `removed_at`
        // filter must still refuse to call this a recreation.
        time.advance_time(INTEREST_TTL + Duration::from_secs(60));
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("the pair is still untracked");
        };
        assert_eq!(
            attempt.class,
            MissingSummaryClass::UntrackedFirstObserved,
            "a removal older than INTEREST_TTL must not be honoured just because \
             the wipe is gone — the removed_at filter is the remaining guard"
        );
        drop(manager.missing_summary_attempt_guard(attempt));

        // T4: the SECOND reader. `register_peer_interest_from` reads the same
        // field to set `NeverPopulatedOrigin::New { recreated }` and to bump
        // `recreated_after_removal`, so it steps on this change too. Re-run the
        // T0→T2 shape and assert on the counter rather than the class.
        let contract = make_contract_key(16);
        let peer = make_peer_key(16);
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("an untracked pair must start lifecycle accounting");
        };
        drop(manager.missing_summary_attempt_guard(attempt));

        time.advance_time(INTEREST_TTL + Duration::from_secs(60));
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.remove_peer_interest_for(
            &contract,
            &peer,
            InterestRemovalCause::DisconnectGrace
        ));

        time.advance_time(Duration::from_secs(60));
        // Trip the staleness reset on the untracked path first, exactly as
        // production would before the peer re-registers.
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("the pair is untracked again");
        };
        drop(manager.missing_summary_attempt_guard(attempt));

        let before = manager
            .interest_lifecycle_snapshot()
            .recreated_after_removal[InterestRemovalCause::DisconnectGrace.index()];
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        let after = manager
            .interest_lifecycle_snapshot()
            .recreated_after_removal[InterestRemovalCause::DisconnectGrace.index()];
        assert_eq!(
            after - before,
            1,
            "the registration reader must also see the preserved removal — \
             without the fix this counter does not move"
        );
    }

    #[test]
    fn missing_summary_lifecycle_is_delivery_gated_and_classifies_repeats() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(71);
        let peer = make_peer_key(71);
        manager.register_peer_interest_from(
            &contract,
            peer.clone(),
            None,
            false,
            InterestRegistrationSource::Interests,
        );

        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("new summaryless interest must start lifecycle accounting");
        };
        assert_eq!(attempt.class, MissingSummaryClass::TrackedFirstNew);
        // A failed/cancelled send must release the in-flight slot but record no
        // delivered bytes.
        drop(manager.missing_summary_attempt_guard(attempt));
        assert_eq!(
            manager.interest_lifecycle_snapshot().delivered_sends,
            [0; MissingSummaryClass::COUNT]
        );

        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("summary is still missing");
        };
        assert_eq!(attempt.class, MissingSummaryClass::TrackedRepeatSequential);
        let mut guard = manager.missing_summary_attempt_guard(attempt);
        guard.mark_delivered(3 * 1024 * 1024);
        drop(guard);
        let snapshot = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snapshot.delivered_sends[MissingSummaryClass::TrackedRepeatSequential.index()],
            1
        );
        assert_eq!(
            snapshot.delivered_bytes[MissingSummaryClass::TrackedRepeatSequential.index()],
            3 * 1024 * 1024
        );
    }

    #[test]
    fn lifecycle_records_overwrite_recreation_and_population_sources() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(72);
        let peer = make_peer_key(72);
        let summary = StateSummary::from(vec![1, 2, 3]);
        manager.register_peer_interest_from(
            &contract,
            peer.clone(),
            Some(summary.clone()),
            false,
            InterestRegistrationSource::Interests,
        );
        assert!(!manager.register_peer_interest_from(
            &contract,
            peer.clone(),
            None,
            false,
            InterestRegistrationSource::ChangeInterests,
        ));
        let snapshot = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snapshot.registration_overwrite_known
                [InterestRegistrationSource::ChangeInterests.index()],
            1
        );

        assert!(manager.remove_peer_interest_for(
            &contract,
            &peer,
            InterestRemovalCause::InterestsReplace,
        ));
        assert!(manager.register_peer_interest_from(
            &contract,
            peer.clone(),
            None,
            false,
            InterestRegistrationSource::Interests,
        ));
        let PeerSummaryForBroadcast::Missing {
            attempt: Some(attempt),
            ..
        } = manager.begin_peer_summary_broadcast(&contract, &peer)
        else {
            panic!("recreated entry must be summaryless");
        };
        assert_eq!(attempt.class, MissingSummaryClass::TrackedFirstRecreated);
        drop(manager.missing_summary_attempt_guard(attempt));

        assert_eq!(
            manager.upsert_peer_summary_from(
                &contract,
                &peer,
                summary,
                SummaryPopulationSource::InterestSummary,
            ),
            SummaryPopulationOutcome::FilledMissing
        );
        let snapshot = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snapshot.recreated_after_removal[InterestRemovalCause::InterestsReplace.index()],
            1
        );
        assert_eq!(
            snapshot.population[SummaryPopulationSource::InterestSummary.index()]
                [SummaryPopulationOutcome::FilledMissing.index()],
            1
        );
    }

    #[test]
    fn lifecycle_snapshot_has_registration_removal_current_and_inflight_denominators() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(73);
        let known_peer = make_peer_key(73);
        let missing_peer = make_peer_key(74);
        assert!(manager.register_peer_interest_from(
            &contract,
            known_peer.clone(),
            Some(StateSummary::from(vec![1])),
            false,
            InterestRegistrationSource::Get,
        ));
        assert!(manager.register_peer_interest_from(
            &contract,
            missing_peer.clone(),
            None,
            false,
            InterestRegistrationSource::SubscribeRelay,
        ));

        let first = match manager.begin_peer_summary_broadcast(&contract, &missing_peer) {
            PeerSummaryForBroadcast::Missing {
                attempt: Some(attempt),
                ..
            } => attempt,
            _ => panic!("missing peer must produce a tracked attempt"),
        };
        let second = match manager.begin_peer_summary_broadcast(&contract, &missing_peer) {
            PeerSummaryForBroadcast::Missing {
                attempt: Some(attempt),
                ..
            } => attempt,
            _ => panic!("overlapping missing send must produce another attempt"),
        };
        assert_eq!(second.class, MissingSummaryClass::TrackedRepeatInflight);
        drop(manager.missing_summary_attempt_guard(first));
        drop(manager.missing_summary_attempt_guard(second));
        assert!(manager.remove_peer_interest_for(
            &contract,
            &known_peer,
            InterestRemovalCause::Unsubscribe,
        ));

        let snapshot = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snapshot.registration_new_known[InterestRegistrationSource::Get.index()],
            1
        );
        assert_eq!(
            snapshot.registration_new_missing[InterestRegistrationSource::SubscribeRelay.index()],
            1
        );
        assert_eq!(
            snapshot.removals[InterestRemovalCause::Unsubscribe.index()],
            1
        );
        assert_eq!(snapshot.current_summary_state[0], 0);
        assert_eq!(
            snapshot.current_summary_state[SummaryMissingReason::NeverPopulated.index() + 1],
            1
        );
    }

    #[test]
    fn lifecycle_first_send_age_bucket_boundaries_are_exact() {
        for (seconds, expected) in [
            (0, 0),
            (1, 1),
            (9, 1),
            (10, 2),
            (59, 2),
            (60, 3),
            (299, 3),
            (300, 4),
        ] {
            assert_eq!(
                TestInterestManager::first_send_age_bucket(Duration::from_secs(seconds)),
                expected
            );
        }
    }

    #[test]
    fn lifecycle_correlation_overflow_is_bounded_and_observable() {
        let (manager, _time) = make_manager();

        for seed in 0..=MISSING_SUMMARY_HISTORY_SIZE as u32 {
            let attempt = match manager.begin_peer_summary_broadcast(
                &make_unique_contract_key(seed),
                &make_unique_peer_key(seed),
            ) {
                PeerSummaryForBroadcast::Missing {
                    attempt: Some(attempt),
                    ..
                } => attempt,
                _ => panic!("untracked pair must produce an attempt"),
            };
            drop(manager.missing_summary_attempt_guard(attempt));
        }

        let mut guards = Vec::new();
        // Seeds must stay disjoint from the first loop's `0..=HISTORY_SIZE`
        // range (freenet-core#5097) — otherwise these "untracked" pairs are
        // actually already-tracked recreations, breaking the panic below.
        let active_probe_start = MISSING_SUMMARY_HISTORY_SIZE as u32 + 10_000;
        for seed in active_probe_start..active_probe_start + MISSING_SUMMARY_ACTIVE_SIZE as u32 + 1
        {
            let attempt = match manager.begin_peer_summary_broadcast(
                &make_unique_contract_key(seed),
                &make_unique_peer_key(seed),
            ) {
                PeerSummaryForBroadcast::Missing {
                    attempt: Some(attempt),
                    ..
                } => attempt,
                _ => panic!("untracked pair must produce an attempt"),
            };
            guards.push(manager.missing_summary_attempt_guard(attempt));
        }

        let snapshot = manager.interest_lifecycle_snapshot();
        assert!(snapshot.history_overflow >= 1);
        assert_eq!(snapshot.active_overflow, 1);
        drop(guards);
    }

    /// Regression for the undersized correlation cache (freenet-core#5097).
    /// Live production telemetry showed the OLD 4,096-entry
    /// `MISSING_SUMMARY_HISTORY_SIZE` overflowing on ~31% of new
    /// registrations on a single busy gateway. This test registers a
    /// working set (8,192 distinct pairs) that comfortably exceeds that old
    /// cap but stays well under the new one: it fails (asserts wrongly)
    /// against the pre-fix 4,096 cap, and passes against the current one.
    #[test]
    fn lifecycle_correlation_survives_realistic_working_set_without_overflow() {
        let (manager, _time) = make_manager();
        const WORKING_SET: u32 = 8_192;
        assert!(
            WORKING_SET as usize > 4_096,
            "must exceed the pre-fix cap to be a real regression test"
        );
        assert!(
            WORKING_SET as usize <= MISSING_SUMMARY_HISTORY_SIZE,
            "must stay within the current cap or this test degenerates into \
             the overflow test above"
        );

        for seed in 0..WORKING_SET {
            let attempt = match manager.begin_peer_summary_broadcast(
                &make_unique_contract_key(seed),
                &make_unique_peer_key(seed),
            ) {
                PeerSummaryForBroadcast::Missing {
                    attempt: Some(attempt),
                    ..
                } => attempt,
                _ => panic!("untracked pair must produce an attempt"),
            };
            drop(manager.missing_summary_attempt_guard(attempt));
        }

        let snapshot = manager.interest_lifecycle_snapshot();
        assert_eq!(
            snapshot.history_overflow, 0,
            "a working set well within the current cap must not overflow \
             the correlation history"
        );
    }

    /// Regression for the InterestManager desync on subscribed eviction
    /// (PR #4734 Fix 1). When a subscriber-primary eviction shed + tore down a
    /// still-in-use contract, the hosting maps are cleared by
    /// `HostingManager::teardown_evicted_in_use_contract`, but the
    /// InterestManager lives on `OpManager` and must be synced separately by the
    /// consumer via `remove_evicted_in_use`. Before this fix, ghost
    /// `interested_peers` / `peer_contracts` / `local_client_count` entries
    /// survived (they drive UPDATE broadcast targeting + upstream interest
    /// counts) and did NOT self-heal. Assert every map is ZERO afterward — the
    /// gap the HostingManager-level `torn_down_...` test did not cover.
    #[test]
    fn remove_evicted_in_use_clears_all_interest_maps() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let downstream_a = make_peer_key(1);
        let downstream_b = make_peer_key(2);

        // Mirror the real add path: per downstream peer,
        // register_peer_interest (interested_peers/peer_contracts) +
        // add_downstream_subscriber (downstream_subscriber_count). Plus two
        // local client subscriptions (local_client_count).
        manager.register_peer_interest(&contract, downstream_a.clone(), None, false);
        manager.add_downstream_subscriber(&contract);
        manager.register_peer_interest(&contract, downstream_b.clone(), None, false);
        manager.add_downstream_subscriber(&contract);
        manager.add_local_client(&contract);
        manager.add_local_client(&contract);

        // Sanity: interest present in all three maps before teardown.
        assert_eq!(manager.get_interested_peers(&contract).len(), 2);
        assert!(!manager.get_contracts_for_peer(&downstream_a).is_empty());
        assert!(!manager.get_contracts_for_peer(&downstream_b).is_empty());
        manager.with_local_interest(&contract, |li| {
            assert_eq!(li.local_client_count, 2);
            assert_eq!(li.downstream_subscriber_count, 2);
        });

        // Replay the hosting teardown against the InterestManager exactly as the
        // eviction consumers do.
        manager.remove_evicted_in_use(&contract, &[downstream_a.clone(), downstream_b.clone()], 2);

        // interested_peers / peer_contracts / local_client_count all ZERO — no
        // ghost survives to mis-target UPDATE broadcasts or inflate counts.
        assert!(
            manager.get_interested_peers(&contract).is_empty(),
            "interested_peers must be cleared for the evicted contract"
        );
        assert!(
            manager.get_contracts_for_peer(&downstream_a).is_empty(),
            "peer_contracts[downstream_a] must be cleared"
        );
        assert!(
            manager.get_contracts_for_peer(&downstream_b).is_empty(),
            "peer_contracts[downstream_b] must be cleared"
        );
        // has_local_interest reads via `.get` (no entry re-creation), so a false
        // result proves the local_interests entry — local_client_count and
        // downstream_subscriber_count — is fully gone.
        assert!(
            !manager.has_local_interest(&contract),
            "no local interest (client or downstream count) may remain"
        );
        let stats = manager.stats();
        assert_eq!(
            stats.total_contracts, 0,
            "no contract may retain interested peers"
        );
        assert_eq!(stats.total_peer_interests, 0);
        assert_eq!(
            stats.local_interests, 0,
            "no local_interests entry may survive"
        );
        assert_eq!(
            stats.hash_index_size, 0,
            "the contract hash index must be cleaned up once no interest remains"
        );

        // Idempotent: replaying on an already-clean contract is a no-op.
        manager.remove_evicted_in_use(&contract, &[downstream_a], 1);
        assert_eq!(manager.stats().total_contracts, 0);
        assert_eq!(manager.stats().local_interests, 0);
    }

    #[test]
    fn test_register_peer_interest_caps_at_max() {
        // #3798 Gap 2: a single contract's interested_peers map must be bounded
        // so a peer flooding distinct identities cannot amplify every broadcast.
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Fill to exactly MAX distinct peers — each is new and accepted.
        // Keys are deterministic AND pairwise-distinct (derived from a u32
        // counter), so the test never relies on RNG distinctness: a leaked
        // thread-local GlobalRng seed or a one-in-a-billion keypair collision
        // can no longer make the 513th registration spuriously non-new and
        // skip the cap branch (the cold-build flake this hardening fixes).
        let mut peers = Vec::with_capacity(MAX_INTERESTED_PEERS_PER_CONTRACT);
        for i in 0..MAX_INTERESTED_PEERS_PER_CONTRACT {
            let peer = make_unique_peer_key(i as u32);
            assert!(
                manager.register_peer_interest(&contract, peer.clone(), None, false),
                "registering a fresh peer below capacity must return is_new = true"
            );
            peers.push(peer);
        }
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT
        );

        // One MORE distinct peer is rejected: returns is_new = false (so it does
        // NOT trigger the #4359 first-viable-target broadcast flush) and the map
        // length is unchanged. Its seed is past the fill range, so it is
        // guaranteed not already tracked.
        let overflow_peer = make_unique_peer_key(MAX_INTERESTED_PEERS_PER_CONTRACT as u32);
        assert!(
            !manager.register_peer_interest(&contract, overflow_peer.clone(), None, false),
            "a new peer at capacity must be rejected (is_new = false)"
        );
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT,
            "capacity must not be exceeded"
        );

        // Invariant: the rejected peer left NO zombie reverse-index entry.
        assert!(
            manager.get_contracts_for_peer(&overflow_peer).is_empty(),
            "rejected peer must not appear in the peer_contracts reverse index"
        );

        // Renewals of an ALREADY-tracked peer are never rejected by capacity:
        // re-registering an existing peer with an updated summary returns false
        // (not new) but still refreshes the entry.
        let existing = peers[0].clone();
        let summary = StateSummary::from(vec![9, 9, 9]);
        assert!(
            !manager.register_peer_interest(
                &contract,
                existing.clone(),
                Some(summary.clone()),
                false
            ),
            "renewal of an existing peer must return is_new = false"
        );
        assert_eq!(
            manager.get_interested_peers(&contract).len(),
            MAX_INTERESTED_PEERS_PER_CONTRACT,
            "renewal must not change capacity"
        );
        let refreshed = manager
            .get_peer_summary(&contract, &existing)
            .expect("existing peer must still be present after renewal");
        assert_eq!(
            refreshed.as_ref(),
            summary.as_ref(),
            "renewal must update the existing peer's summary"
        );
    }

    #[test]
    fn test_update_peer_summary() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register without summary
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.get_peer_summary(&contract, &peer).is_none());

        // Update with summary
        let summary = StateSummary::from(vec![1, 2, 3]);
        manager.update_peer_summary(&contract, &peer, summary.clone());

        let retrieved = manager.get_peer_summary(&contract, &peer);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_ref(), summary.as_ref());
    }

    /// `has_peer_summary` is the clone-free form of `get_peer_summary`, used by
    /// the broadcast queue to predict whether a send will carry a delta (#4961).
    /// It must agree with `get_peer_summary` in every state, including the two
    /// that differ: no interest entry at all, and an entry with no summary.
    #[test]
    fn has_peer_summary_agrees_with_get_peer_summary() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        for stage in ["untracked", "tracked-without-summary", "with-summary"] {
            match stage {
                "tracked-without-summary" => {
                    assert!(manager.register_peer_interest(&contract, peer.clone(), None, false));
                }
                "with-summary" => {
                    manager.update_peer_summary(
                        &contract,
                        &peer,
                        StateSummary::from(vec![1, 2, 3]),
                    );
                }
                _ => {}
            }
            assert_eq!(
                manager.has_peer_summary(&contract, &peer),
                manager.get_peer_summary(&contract, &peer).is_some(),
                "[{stage}] the cheap predicate must not drift from the cloning one"
            );
        }
        assert!(
            manager.has_peer_summary(&contract, &peer),
            "sanity: the loop above ended with a cached summary"
        );
    }

    /// Issue #4857: `begin_resync_request` must emit at most one
    /// `ResyncRequest` per (contract, peer) per `RESYNC_REQUEST_MIN_INTERVAL`.
    /// The first drop heals immediately (returning `Some(deadline)`); a burst of
    /// further drops within the window is throttled (`None`, bounding the #4251
    /// amplification); after the window elapses a fresh request is allowed
    /// again. The returned deadline is the reservation window close
    /// (`now + RESYNC_REQUEST_MIN_INTERVAL`) on the manager's clock (#4857 P2).
    #[test]
    fn begin_resync_request_rate_limits_per_contract_peer() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let addr: SocketAddr = "127.0.0.1:5001".parse().unwrap();

        // First drop for this (contract, peer) → allowed (immediate heal), and
        // the returned deadline is exactly now + RESYNC_REQUEST_MIN_INTERVAL.
        let deadline = manager
            .begin_resync_request(&contract, addr)
            .expect("first ResyncRequest for a fresh (contract, peer) must be allowed");
        assert_eq!(
            deadline,
            manager.now() + RESYNC_REQUEST_MIN_INTERVAL,
            "reservation deadline must be now + RESYNC_REQUEST_MIN_INTERVAL (#4857 P2)"
        );
        // Immediate repeat within the window → throttled.
        assert!(
            manager.begin_resync_request(&contract, addr).is_none(),
            "a second ResyncRequest within RESYNC_REQUEST_MIN_INTERVAL must be throttled"
        );

        // A DIFFERENT peer for the same contract is an independent bucket.
        let other_addr: SocketAddr = "127.0.0.1:5002".parse().unwrap();
        assert!(
            manager
                .begin_resync_request(&contract, other_addr)
                .is_some(),
            "a distinct peer must not share the first peer's throttle bucket"
        );
        // A DIFFERENT contract for the same peer is also independent.
        let other_contract = make_contract_key(2);
        assert!(
            manager
                .begin_resync_request(&other_contract, addr)
                .is_some(),
            "a distinct contract must not share the first contract's throttle bucket"
        );

        // Just before the interval elapses → still throttled.
        time.advance_time(RESYNC_REQUEST_MIN_INTERVAL - Duration::from_millis(1));
        assert!(
            manager.begin_resync_request(&contract, addr).is_none(),
            "ResyncRequest must stay throttled until the full interval elapses"
        );
        // After the interval elapses → allowed again.
        time.advance_time(Duration::from_millis(2));
        assert!(
            manager.begin_resync_request(&contract, addr).is_some(),
            "ResyncRequest must be allowed again once RESYNC_REQUEST_MIN_INTERVAL has elapsed"
        );
    }

    /// #4864 round-6 item 2: the begin/cancel reservation-commit semantics.
    /// `begin` reserves the window (records under the lock); `cancel` releases it
    /// (so a downstream rejection does not burn the 30s window); `begin` without a
    /// `cancel` holds the window (a second immediate begin is rejected).
    #[test]
    fn begin_cancel_resync_request_reservation_semantics() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let addr: SocketAddr = "127.0.0.1:5003".parse().unwrap();

        // begin reserves; cancel releases → a subsequent begin succeeds immediately
        // (the window was NOT burned).
        assert!(
            manager.begin_resync_request(&contract, addr).is_some(),
            "first begin reserves"
        );
        manager.cancel_resync_request(&contract, addr);
        assert!(
            manager.begin_resync_request(&contract, addr).is_some(),
            "begin after cancel must succeed — cancel releases the reserved window"
        );

        // This last begin was NOT cancelled → the window is held: an immediate
        // second begin is rejected (the reservation stands, as after a real emit).
        assert!(
            manager.begin_resync_request(&contract, addr).is_none(),
            "a begin without a matching cancel must hold the 30s window (second begin rejected)"
        );

        // cancel is idempotent-safe on an absent bucket (no panic) and, once the
        // interval elapses, begin is allowed again.
        time.advance_time(RESYNC_REQUEST_MIN_INTERVAL);
        assert!(
            manager.begin_resync_request(&contract, addr).is_some(),
            "begin allowed once RESYNC_REQUEST_MIN_INTERVAL elapses"
        );
    }

    #[test]
    fn test_local_interest_tracking() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Initially no interest
        assert!(!manager.has_local_interest(&contract));

        // Add hosting interest
        manager.with_local_interest(&contract, |interest| {
            interest.set_hosting(true);
        });
        assert!(manager.has_local_interest(&contract));

        // Add client interest
        manager.with_local_interest(&contract, |interest| {
            interest.add_client();
        });
        assert!(manager.has_local_interest(&contract));

        // Remove hosting - still interested due to client
        manager.with_local_interest(&contract, |interest| {
            interest.set_hosting(false);
        });
        assert!(manager.has_local_interest(&contract));

        // Remove client - no longer interested
        manager.with_local_interest(&contract, |interest| {
            interest.remove_client();
        });
        assert!(!manager.has_local_interest(&contract));
    }

    #[test]
    fn test_local_interest_transitions() {
        let mut interest = LocalInterest::default();

        // Initially not interested
        assert!(!interest.is_interested());

        // First client triggers interest
        assert!(interest.add_client()); // Returns true - gained interest
        assert!(interest.is_interested());

        // Second client doesn't change interest state
        assert!(!interest.add_client()); // Returns false - already interested
        assert!(interest.is_interested());

        // Remove one client - still interested
        assert!(!interest.remove_client()); // Returns false - still interested
        assert!(interest.is_interested());

        // Remove last client - interest lost
        assert!(interest.remove_client()); // Returns true - lost interest
        assert!(!interest.is_interested());
    }

    #[test]
    fn test_contract_hash_consistency() {
        let contract = make_contract_key(42);

        // Same contract should produce same hash
        let hash1 = contract_hash(&contract);
        let hash2 = contract_hash(&contract);
        assert_eq!(hash1, hash2);

        // Different contracts should (usually) produce different hashes
        let other_contract = make_contract_key(43);
        let other_hash = contract_hash(&other_contract);
        // Note: hash collision is theoretically possible but extremely unlikely
        // for these test values
        assert_ne!(hash1, other_hash);
    }

    #[test]
    fn test_contract_hash_index() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest (should also index the hash)
        manager.register_peer_interest(&contract, peer, None, false);

        // Look up by hash
        let hash = contract_hash(&contract);
        let retrieved = manager.lookup_by_hash(hash);
        assert_eq!(retrieved, vec![contract]);

        // Unknown hash returns empty vec
        assert!(manager.lookup_by_hash(12345).is_empty());
    }

    #[test]
    fn test_get_all_interest_hashes() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer = make_peer_key(1);

        // Register interests (use methods that properly index)
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_local_hosting(&contract2);

        let hashes = manager.get_all_interest_hashes();
        assert_eq!(hashes.len(), 2);
        assert!(hashes.contains(&contract_hash(&contract1)));
        assert!(hashes.contains(&contract_hash(&contract2)));
    }

    /// Pins the [`is_delta_efficient`] heuristic itself. Since #4923 the
    /// function is no longer consulted by `compute_delta` (the efficiency
    /// gate moved POST-compute, onto the actual delta size — see
    /// `oversized_computed_delta_returns_not_efficient`); it is kept as the
    /// documented summary-size heuristic, and these assertions pin its
    /// boundary behavior.
    #[test]
    fn test_delta_efficiency_check() {
        // Small summary relative to state - efficient
        assert!(is_delta_efficient(100, 1000));

        // Summary is 50% of state - not efficient
        assert!(!is_delta_efficient(500, 1000));

        // Summary larger than state - not efficient
        assert!(!is_delta_efficient(1500, 1000));

        // Zero state size - not efficient
        assert!(!is_delta_efficient(100, 0));
    }

    #[test]
    fn test_delta_cache() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);

        let peer_summary = vec![1, 2, 3];
        let our_summary = vec![4, 5, 6];
        let delta = StateDelta::from(vec![7, 8, 9]);

        // Cache miss
        assert!(
            manager
                .get_cached_delta(&contract1, &peer_summary, &our_summary)
                .is_none()
        );

        // Cache the delta for contract1
        manager.cache_delta(&contract1, &peer_summary, &our_summary, delta.clone());

        // Cache hit for contract1
        let cached = manager.get_cached_delta(&contract1, &peer_summary, &our_summary);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().as_ref(), delta.as_ref());

        // Cache miss for contract2 with same summaries (contract key isolates cache entries)
        assert!(
            manager
                .get_cached_delta(&contract2, &peer_summary, &our_summary)
                .is_none()
        );
    }

    #[test]
    fn test_sweep_expired_interests() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time past TTL
        time.advance_time(INTEREST_TTL + Duration::from_secs(1));

        // Sweep should remove expired entry
        let expired = manager.sweep_expired_interests();
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, contract);

        // Verify removed
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_refresh_prevents_expiration() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time to nearly expired
        time.advance_time(INTEREST_TTL - Duration::from_secs(10));

        // Refresh the interest
        manager.refresh_peer_interest(&contract, &peer);

        // Advance time a bit more (past original registration but not past refresh)
        time.advance_time(Duration::from_secs(20));

        // Sweep should not remove it (refresh reset the TTL)
        let expired = manager.sweep_expired_interests();
        assert!(expired.is_empty());
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_stats() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer1 = make_peer_key(1);
        let peer2 = make_peer_key(2);

        // Add various interests
        manager.register_peer_interest(&contract1, peer1.clone(), None, false);
        manager.register_peer_interest(&contract1, peer2.clone(), None, false);
        manager.register_peer_interest(&contract2, peer1, None, true);
        manager.with_local_interest(&contract1, |i| i.set_hosting(true));

        let stats = manager.stats();
        assert_eq!(stats.total_contracts, 2);
        assert_eq!(stats.total_peer_interests, 3);
        assert_eq!(stats.local_interests, 1);
        assert!(stats.hash_index_size >= 2);
    }

    #[test]
    fn test_delta_sync_metrics() {
        let (manager, _time) = make_manager();

        // Initially all metrics should be zero
        let stats = manager.stats();
        assert_eq!(stats.delta_sends, 0);
        assert_eq!(stats.full_state_sends, 0);
        assert_eq!(stats.delta_bytes_saved, 0);

        // Record some delta sends
        // state_size=1000, delta_size=100 -> 900 bytes saved
        manager.record_delta_send(1000, 100);
        manager.record_delta_send(2000, 200);

        // Record a full state send
        manager.record_full_state_send();
        manager.record_full_state_send();

        let stats = manager.stats();
        assert_eq!(stats.delta_sends, 2);
        assert_eq!(stats.full_state_sends, 2);
        // 900 + 1800 = 2700 bytes saved
        assert_eq!(stats.delta_bytes_saved, 2700);
    }

    #[test]
    fn test_get_matching_contracts() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);

        // Register local interest in contracts 1 and 2 (using set_hosting which indexes)
        manager.register_local_hosting(&contract1);
        manager.register_local_hosting(&contract2);

        // Get hashes
        let hash1 = contract_hash(&contract1);
        let hash2 = contract_hash(&contract2);
        let hash3 = contract_hash(&contract3);

        // Matching with partial overlap
        let matching = manager.get_matching_contracts(&[hash1, hash3]);
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(&contract1));

        // Matching with full overlap
        let matching = manager.get_matching_contracts(&[hash1, hash2]);
        assert_eq!(matching.len(), 2);
        assert!(matching.contains(&contract1));
        assert!(matching.contains(&contract2));

        // No overlap
        let matching = manager.get_matching_contracts(&[hash3, 99999]);
        assert!(matching.is_empty());

        // Empty input
        let matching = manager.get_matching_contracts(&[]);
        assert!(matching.is_empty());
    }

    #[test]
    fn test_interest_sync_flow_simulation() {
        // Simulate the Interests -> Summaries flow that handle_interest_sync_message uses
        let (manager_a, _time_a) = make_manager();
        let (manager_b, _time_b) = make_manager();

        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);

        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);

        let summary1 = StateSummary::from(vec![1, 1, 1]);
        let summary2 = StateSummary::from(vec![2, 2, 2]);

        // Setup: A is interested in contracts 1, 2 (using set_hosting which indexes)
        manager_a.register_local_hosting(&contract1);
        manager_a.register_local_hosting(&contract2);

        // Setup: B is interested in contracts 2, 3 (using set_hosting which indexes)
        manager_b.register_local_hosting(&contract2);
        manager_b.register_local_hosting(&contract3);

        // Step 1: A sends its interest hashes to B
        let a_hashes = manager_a.get_all_interest_hashes();
        assert_eq!(a_hashes.len(), 2);

        // Step 2: B finds matching contracts and registers A's interest
        let matching = manager_b.get_matching_contracts(&a_hashes);
        // Only contract2 is in both A and B's interests
        assert_eq!(matching.len(), 1);
        assert!(matching.contains(&contract2));

        // B registers A's interest in the matching contract
        for contract in &matching {
            manager_b.register_peer_interest(contract, peer_a.clone(), None, false);
        }

        // Verify B now tracks A's interest in contract2
        assert!(
            manager_b
                .get_interested_peers(&contract2)
                .iter()
                .any(|(pk, _)| pk == &peer_a)
        );

        // Step 3: B sends summaries back for matching contracts
        // A receives and updates B's summary
        manager_a.register_peer_interest(&contract2, peer_b.clone(), Some(summary2.clone()), false);

        // Verify A has B's summary
        let cached_summary = manager_a.get_peer_summary(&contract2, &peer_b);
        assert!(cached_summary.is_some());
        assert_eq!(cached_summary.unwrap().as_ref(), summary2.as_ref());

        // Step 4: A sends its summary back
        manager_b.update_peer_summary(&contract2, &peer_a, summary1.clone());

        // Verify B has A's summary
        let cached_summary = manager_b.get_peer_summary(&contract2, &peer_a);
        assert!(cached_summary.is_some());
        assert_eq!(cached_summary.unwrap().as_ref(), summary1.as_ref());
    }

    #[test]
    fn test_change_interests_flow_simulation() {
        // Simulate the ChangeInterests flow
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let peer = make_peer_key(1);

        let hash1 = contract_hash(&contract1);
        let hash2 = contract_hash(&contract2);

        // Setup: local interest in contract1 (using set_hosting which indexes)
        manager.register_local_hosting(&contract1);

        // Peer declares interest in contract1 and contract2
        let added_hashes = vec![hash1, hash2];

        // For each added hash, lookup contracts and register if we have local interest
        for hash in &added_hashes {
            for contract in manager.lookup_by_hash(*hash) {
                if manager.has_local_interest(&contract) {
                    manager.register_peer_interest(&contract, peer.clone(), None, false);
                }
            }
        }

        // Only contract1 should have peer interest (we have local interest in it)
        assert!(
            manager
                .get_interested_peers(&contract1)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
        // contract2 wasn't registered because we don't have local interest
        assert!(
            !manager
                .get_interested_peers(&contract2)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );

        // Later: peer removes interest in contract1
        let removed_hashes = vec![hash1];
        for hash in &removed_hashes {
            for contract in manager.lookup_by_hash(*hash) {
                manager.remove_peer_interest(&contract, &peer);
            }
        }

        // Verify peer is no longer interested
        assert!(
            !manager
                .get_interested_peers(&contract1)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
    }

    #[test]
    fn test_resync_clears_summary() {
        // Simulate ResyncRequest clearing a peer's summary
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1, 2, 3]);

        // Setup: register peer with summary
        manager.register_peer_interest(&contract, peer.clone(), Some(summary.clone()), false);

        // Verify summary is cached
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(cached.is_some());

        // Simulate ResyncRequest: clear the summary
        manager.clear_peer_summary(&contract, &peer, SummaryMissingReason::ClearedByResync);

        // Verify summary is now None
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(cached.is_none());

        // Peer should still be interested (just no summary)
        assert!(
            manager
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer)
        );
    }

    /// #4961: an entry that never had a summary written reports
    /// `NeverPopulated`, and one that HAS a summary reports no reason at all.
    ///
    /// The second half is the load-bearing one: `summary_absence` keeps its
    /// last value once a summary is cached, so a naive field read would
    /// attribute a live, summary-holding peer to whichever path last cleared
    /// it. Only the accessor's `is_none()` guard prevents that, and that is
    /// exactly the mis-attribution this instrumentation exists to avoid.
    #[test]
    fn summary_missing_reason_is_never_populated_until_cleared_and_absent_when_cached() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert_eq!(
            manager
                .get_peer_interest(&contract, &peer)
                .and_then(|i: PeerInterest| i.summary_missing_reason()),
            Some(SummaryMissingReason::NeverPopulated),
            "a fresh summaryless entry must report NeverPopulated"
        );

        manager.update_peer_summary(&contract, &peer, StateSummary::from(vec![1u8, 2, 3]));
        assert_eq!(
            manager
                .get_peer_interest(&contract, &peer)
                .and_then(|i: PeerInterest| i.summary_missing_reason()),
            None,
            "a peer WITH a cached summary must report no missing-reason — \
             reading the raw field here would mis-attribute it"
        );
    }

    /// #4961: each clear path is distinguishable, and a re-cached summary
    /// hides the reason again.
    ///
    /// Without the per-path tag the `full_no_their_summary_tracked` arm (26.9%
    /// of broadcast bytes on the aged 0.2.109 fleet) is one number covering
    /// three causes with three different fixes.
    #[test]
    fn clear_peer_summary_records_the_distinguishing_reason() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1u8, 2, 3]);

        for reason in SummaryMissingReason::ALL {
            manager.register_peer_interest(&contract, peer.clone(), Some(summary.clone()), false);
            manager.clear_peer_summary(&contract, &peer, reason);
            assert_eq!(
                manager
                    .get_peer_interest(&contract, &peer)
                    .and_then(|i: PeerInterest| i.summary_missing_reason()),
                Some(reason),
                "clear must record {reason:?}, not a different path's tag"
            );

            // Re-caching hides the reason; the arm no longer applies.
            manager.update_peer_summary(&contract, &peer, summary.clone());
            assert_eq!(
                manager
                    .get_peer_interest(&contract, &peer)
                    .and_then(|i: PeerInterest| i.summary_missing_reason()),
                None
            );
        }
    }

    /// Every reason has a distinct index and label — a collision would silently
    /// merge two causes into one telemetry bucket, which is the exact failure
    /// this split exists to prevent.
    #[test]
    fn summary_missing_reason_indices_and_labels_are_distinct() {
        let indices: std::collections::HashSet<_> = SummaryMissingReason::ALL
            .iter()
            .map(|r| r.index())
            .collect();
        assert_eq!(
            indices.len(),
            SummaryMissingReason::ALL.len(),
            "duplicate index would merge two causes into one counter"
        );
        assert!(
            indices.iter().all(|i| *i < SummaryMissingReason::ALL.len()),
            "index must stay in bounds of the counter array"
        );
        let labels: std::collections::HashSet<_> = SummaryMissingReason::ALL
            .iter()
            .map(|r| r.as_str())
            .collect();
        assert_eq!(
            labels.len(),
            SummaryMissingReason::ALL.len(),
            "duplicate label would collide as a JSON field name"
        );
    }

    /// `update_peer_summary` is a SILENT no-op for a peer that has no
    /// `PeerInterest` entry for the contract — it cannot create one.
    ///
    /// This is the mechanism behind the `FullNoTheirSummaryUntracked` payload
    /// arm, and it is load-bearing rather than incidental. Since #4642 step 9
    /// removed the interest-manager fan-out arm, live broadcast targets are
    /// resolved from `neighbor_hosting` (advertised co-hosts) while the
    /// peer-summary cache still lives here, keyed on interest registration.
    /// The two populations are maintained by independent mechanisms — the
    /// advertisement exchange never touches `InterestManager`.
    ///
    /// So for a target present in one and absent from the other,
    /// `get_peer_summary` returns None (the fan-out sends FULL STATE) and the
    /// post-delivery `update_peer_summary` that is supposed to fix that
    /// (#4442's fix for exactly this chicken-and-egg) silently does nothing —
    /// so the pair never escapes to deltas via THIS method. This was a fixed
    /// point until #4952 routed the delivery path (and the Summaries handler)
    /// through `upsert_peer_summary`, which creates the entry; the no-op
    /// semantics pinned here remain correct and load-bearing for writes of
    /// unknown provenance. Historically it was a fixed point, not a cold
    /// start.
    ///
    /// The pre-existing broadcast-path tests all `register_peer_interest`
    /// first, so none of them exercise this state.
    #[test]
    fn update_peer_summary_is_a_silent_noop_for_an_untracked_peer() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1u8, 2, 3]);

        // No register_peer_interest: this peer is an advertised co-host that
        // the InterestSync heartbeat has not registered.
        assert!(
            manager.get_peer_interest(&contract, &peer).is_none(),
            "precondition: the peer must be untracked"
        );

        manager.update_peer_summary(&contract, &peer, summary);

        assert!(
            manager.get_peer_summary(&contract, &peer).is_none(),
            "update_peer_summary silently dropped the write for an untracked \
             peer. A broadcast target in this state can never cache a summary, \
             so every broadcast to it is FULL STATE forever — if this ever \
             starts passing, the structural full-state trap is closed and the \
             FullNoTheirSummaryUntracked arm should go to zero in production."
        );

        // And it stays that way no matter how many deliveries land.
        for _ in 0..5 {
            manager.update_peer_summary(&contract, &peer, StateSummary::from(vec![9u8]));
        }
        assert!(
            manager.get_peer_summary(&contract, &peer).is_none(),
            "repeated deliveries must not accumulate a summary either — the \
             trap is a fixed point, not a slow warm-up"
        );

        // Contrast: once the peer IS tracked, the very same call sticks.
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        manager.update_peer_summary(&contract, &peer, StateSummary::from(vec![7u8]));
        assert_eq!(
            manager
                .get_peer_summary(&contract, &peer)
                .map(|s| s.as_ref().to_vec()),
            Some(vec![7u8]),
            "a TRACKED peer caches the summary, so it escapes to deltas — this \
             is what makes the untracked case a distinct bug rather than cold \
             start"
        );
    }

    /// #4952 regression: `upsert_peer_summary` closes the untracked-co-host
    /// full-state fixed point that `update_peer_summary` (pinned no-op above)
    /// cannot. The post-delivery cache in
    /// `broadcast_queue::record_delivery_to_interest` routes through the
    /// upsert, so one delivered full state seeds the summary and every later
    /// broadcast to the same peer can be a delta.
    #[test]
    fn upsert_peer_summary_seeds_summary_for_untracked_peer() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        assert!(
            manager.get_peer_interest(&contract, &peer).is_none(),
            "precondition: the peer must be untracked"
        );

        assert!(manager.upsert_peer_summary(&contract, &peer, StateSummary::from(vec![1u8, 2])));

        assert_eq!(
            manager
                .get_peer_summary(&contract, &peer)
                .map(|s| s.as_ref().to_vec()),
            Some(vec![1u8, 2]),
            "the upsert must CREATE the entry so the pair escapes to deltas"
        );
        let interest = manager
            .get_peer_interest(&contract, &peer)
            .expect("entry created");
        assert!(
            !interest.is_upstream,
            "a delivery-seeded entry is not our upstream"
        );

        // Later deliveries keep the cached summary current.
        assert!(manager.upsert_peer_summary(&contract, &peer, StateSummary::from(vec![9u8])));
        assert_eq!(
            manager
                .get_peer_summary(&contract, &peer)
                .map(|s| s.as_ref().to_vec()),
            Some(vec![9u8]),
        );

        // The reverse index is maintained, so peer-disconnect cleanup works.
        assert!(manager.get_contracts_for_peer(&peer).contains(&contract));
        assert!(manager.remove_peer_interest(&contract, &peer));
        assert!(manager.get_peer_summary(&contract, &peer).is_none());
        assert!(!manager.get_contracts_for_peer(&peer).contains(&contract));

        // Summary bookkeeping must not fabricate local demand (invariant 3):
        // no local-interest entry appears as a side effect.
        assert!(
            !manager.has_local_interest(&contract),
            "upsert must not create local interest / demand state"
        );
    }

    /// A subscribe RENEWAL must not wipe the cached delta-sync summary.
    ///
    /// `finalize_originator_subscribe` / `finalize_host_subscribe` previously
    /// called a bare `register_peer_interest`, which inserts a fresh
    /// `PeerInterest` over the existing entry. The summary was silently lost and
    /// the entry then reported `NeverPopulated` — both wrong (it HAD been
    /// populated) and expensive, since every subsequent broadcast to that peer
    /// falls back to full state. Renewals run at 120s against an 8-minute lease,
    /// so this fired ~30x per subscribed contract per hour.
    #[test]
    fn refresh_with_upstream_preserves_summary_and_sets_flag() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // A downstream entry that has since been seeded by a real delivery.
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.upsert_peer_summary(&contract, &peer, StateSummary::from(vec![7u8, 7])));

        // The renewal path asserts upstream-ness on the existing entry.
        assert!(
            manager.refresh_peer_interest_with_upstream(&contract, &peer, true),
            "an existing entry must report as refreshed, so the caller does not \
             fall through to register_peer_interest"
        );

        assert_eq!(
            manager
                .get_peer_summary(&contract, &peer)
                .map(|s| s.as_ref().to_vec()),
            Some(vec![7u8, 7]),
            "the cached summary MUST survive a renewal — losing it is the \
             never_populated clobber this method exists to prevent"
        );
        let interest = manager
            .get_peer_interest(&contract, &peer)
            .expect("entry still present");
        assert!(
            interest.is_upstream,
            "the flag must be SET, not merely left alone: it is the Unsubscribe \
             routing target and the bare register it replaces also set it"
        );
        assert!(
            interest.summary_missing_reason().is_none(),
            "a preserved summary must not be tagged with an absence reason"
        );
    }

    /// The absent case: nothing to refresh, so the caller must register.
    #[test]
    fn refresh_with_upstream_reports_false_for_untracked_peer() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        assert!(
            !manager.refresh_peer_interest_with_upstream(&contract, &peer, true),
            "an untracked peer must report false so the caller registers it"
        );
        assert!(
            manager.get_peer_interest(&contract, &peer).is_none(),
            "the refresh must not create an entry as a side effect"
        );
    }

    /// The production-code slice of a source file: everything before its test
    /// module.
    ///
    /// `subscribe.rs` declares its tests as `#[cfg(test)] mod tests;` — an
    /// EXTERNAL module with no brace — so a cut at `"\nmod tests {"` finds
    /// nothing and silently returns the whole file, test modules included.
    /// `subscribe.rs` also has an INLINE `#[cfg(test)] mod source_pin_tests {`,
    /// so a needle added to a future pin there would inflate the counts below
    /// and mask a real drift. Cutting at the EARLIER of the two markers covers
    /// both shapes (the `cfg-test-cut-disarms-source-pins` trap). Earlier, not
    /// first-found: `#[cfg(test)]` precedes the `mod tests {` it annotates, so
    /// preferring the brace form would leave the attribute in the "production"
    /// slice.
    fn prod_source(src: &str) -> String {
        let cut = [src.find("\nmod tests {"), src.find("\n#[cfg(test)]")]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(src.len());
        src[..cut].chars().filter(|c| !c.is_whitespace()).collect()
    }

    /// The cut above must actually cut. A fallback that silently no-ops leaves
    /// every count below reading the test modules too, which is how a source
    /// pin stops pinning without anyone noticing.
    #[test]
    fn prod_source_cut_excludes_test_modules() {
        for (name, src) in [
            (
                "subscribe.rs",
                include_str!("../operations/subscribe.rs") as &str,
            ),
            (
                "get/op_ctx_task.rs",
                include_str!("../operations/get/op_ctx_task.rs") as &str,
            ),
        ] {
            let prod = prod_source(src);
            let whole: String = src.chars().filter(|c| !c.is_whitespace()).collect();
            assert!(
                prod.len() < whole.len(),
                "{name}: prod_source must exclude the test module(s); it returned \
                 the whole file, so every count derived from it is reading test \
                 code as production code"
            );
            assert!(
                !prod.contains("#[cfg(test)]"),
                "{name}: prod_source must cut BEFORE the first #[cfg(test)]"
            );
        }
    }

    /// Source pin: neither subscribe finalizer may go back to a bare
    /// `register_peer_interest` without first trying the refresh. Guarding by
    /// convention already failed once — three sites had the guard and these two
    /// did not, and the drift was invisible because the symptom is a bandwidth
    /// regression rather than a broken test.
    #[test]
    fn subscribe_finalizers_do_not_clobber_cached_summaries() {
        let stripped = prod_source(include_str!("../operations/subscribe.rs"));

        let bare = stripped
            .matches("register_peer_interest_from(&key,peer_key,None,true,")
            .count();
        let guarded = stripped
            .matches("refresh_peer_interest_with_upstream(&key,&peer_key,true)")
            .count();

        assert_eq!(
            guarded, 2,
            "both subscribe finalizers must consult refresh_peer_interest_with_upstream \
             before registering (found {guarded})"
        );
        assert_eq!(
            bare, 2,
            "the two register calls must remain as the else-branch of that guard \
             (found {bare}); if this changed, re-check that neither path can wipe \
             a cached summary"
        );
        // Bind each guard to its register: the counts alone would stay green if
        // one finalizer refreshed and then registered unconditionally.
        assert_eq!(
            stripped
                .matches(
                    "refresh_peer_interest_with_upstream(&key,&peer_key,true){false}else{op_manager.interest_manager.register_peer_interest_from(&key,peer_key,None,true,"
                )
                .count(),
            2,
            "each subscribe finalizer's register must be the ELSE branch of its \
             own refresh guard, not a separate unconditional statement"
        );

        // Third site, `register_downstream_subscriber`: same clobber, but with
        // the PLAIN refresh — it registers with is_upstream=false, and
        // asserting that on an existing entry would downgrade a real upstream.
        assert!(
            stripped.contains(
                "refresh_peer_interest(key,&peer_key){op_manager.interest_manager.register_peer_interest_from(key,peer_key,None,false,"
            ),
            "register_downstream_subscriber must register only when the refresh \
             reports no existing entry (a bare register wipes the cached summary \
             on every lease renewal)"
        );
    }

    /// Same clobber, third site (#4672): the remote-GET interest registration.
    ///
    /// Guarded with plain `refresh_peer_interest`, NOT the `_with_upstream`
    /// variant — this call passes `is_upstream = false`, and asserting that on
    /// an existing entry would DOWNGRADE a peer that is legitimately our
    /// upstream, which is the `Unsubscribe` routing target. A GET requester's
    /// interest must not clear an upstream edge established by SUBSCRIBE.
    #[test]
    fn get_interest_registration_does_not_clobber_or_downgrade() {
        let stripped = prod_source(include_str!("../operations/get/op_ctx_task.rs"));

        // Bind the guard to the register rather than merely asserting both
        // appear: the needle spans `refresh(..) { false } else { register(..) }`,
        // so a register that runs regardless of the refresh fails here.
        assert!(
            stripped.contains(
                "refresh_peer_interest(&key,&peer_key){false}else{op_manager.interest_manager.register_peer_interest_from(&key,peer_key,None,false,"
            ),
            "the remote-GET registration must register ONLY when the refresh \
             reports no existing entry — otherwise it inserts a fresh \
             PeerInterest over the existing one and wipes its cached summary \
             (#4672)"
        );

        // Exactly one of each in the whole production file: a second, unguarded
        // register elsewhere would leave the assertion above green while
        // reintroducing the clobber.
        assert_eq!(
            stripped.matches("register_peer_interest_from(").count(),
            1,
            "get/op_ctx_task.rs must contain exactly ONE register_peer_interest \
             call — the guarded one; a second is an unguarded clobber"
        );
        assert_eq!(
            stripped.matches("refresh_peer_interest(").count(),
            1,
            "exactly one refresh_peer_interest call — the one gating that register"
        );

        // Falsifiable form of "must not downgrade": the `_with_upstream`
        // variant must not appear here AT ALL. Using it on this path would
        // assert is_upstream=false on an existing entry, clearing an upstream
        // edge established by SUBSCRIBE and breaking Unsubscribe routing.
        assert_eq!(
            stripped
                .matches("refresh_peer_interest_with_upstream(")
                .count(),
            0,
            "the remote-GET path must use the plain refresh; the _with_upstream \
             variant SETS the flag, and this call site's is_upstream is false"
        );
    }

    /// #4952: at the per-contract cap the upsert must reject a NEW peer (no
    /// amplification vector, no zombie side-writes) while still updating a
    /// peer that is already tracked.
    #[test]
    fn upsert_peer_summary_respects_interested_peer_cap() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        for i in 0..MAX_INTERESTED_PEERS_PER_CONTRACT {
            assert!(manager.register_peer_interest(
                &contract,
                make_unique_peer_key(i as u32),
                None,
                false
            ));
        }

        let newcomer = make_unique_peer_key(u32::MAX);
        assert!(
            !manager.upsert_peer_summary(&contract, &newcomer, StateSummary::from(vec![1u8])),
            "a new peer at cap must be rejected"
        );
        assert!(manager.get_peer_interest(&contract, &newcomer).is_none());
        assert!(
            !manager
                .get_contracts_for_peer(&newcomer)
                .contains(&contract),
            "a rejected upsert must leave no reverse-index zombie"
        );

        // An EXISTING peer at cap must still take the update path — the
        // get_mut-before-cap-check branch order is load-bearing: popular
        // 512-peer contracts are exactly the #4952 population, and a
        // register_peer_interest-shaped refactor (cap check first) would
        // silently freeze summary refreshes for all of them.
        let existing = make_unique_peer_key(0);
        assert!(
            manager.upsert_peer_summary(&contract, &existing, StateSummary::from(vec![42u8])),
            "an already-tracked peer at cap must still update"
        );
        assert_eq!(
            manager
                .get_peer_summary(&contract, &existing)
                .map(|s| s.as_ref().to_vec()),
            Some(vec![42u8]),
        );
    }

    /// #4952: upserting an already-tracked peer takes the update path —
    /// summary replaced, `is_upstream` preserved.
    #[test]
    fn upsert_peer_summary_updates_existing_entry_preserving_upstream_flag() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, true);
        assert!(manager.upsert_peer_summary(&contract, &peer, StateSummary::from(vec![5u8])));

        let interest = manager
            .get_peer_interest(&contract, &peer)
            .expect("tracked");
        assert!(
            interest.is_upstream,
            "upsert on an existing entry must not clobber the upstream flag"
        );
        assert_eq!(
            interest.summary.map(|s| s.as_ref().to_vec()),
            Some(vec![5u8])
        );
    }

    /// #4952: an upsert-created entry is ordinary interest state — the TTL
    /// sweep removes it (entry + reverse index) with no GC exemption, per the
    /// cleanup-exemptions-must-be-time-bounded rule.
    #[test]
    fn upsert_created_entry_expires_via_ttl_sweep() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        assert!(manager.upsert_peer_summary(&contract, &peer, StateSummary::from(vec![1u8])));
        time.advance_time(INTEREST_TTL + Duration::from_secs(60));
        manager.sweep_expired_interests();

        assert!(manager.get_peer_interest(&contract, &peer).is_none());
        assert!(!manager.get_contracts_for_peer(&peer).contains(&contract));
    }

    #[test]
    fn test_resync_full_flow() {
        // Simulate the complete ResyncRequest -> ResyncResponse flow
        // Peer A has corrupted state and requests resync from Peer B
        let (manager_a, _time_a) = make_manager();
        let (manager_b, _time_b) = make_manager();

        let contract = make_contract_key(1);
        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);

        let old_summary = StateSummary::from(vec![1, 2, 3]); // A's corrupted summary
        let new_summary = StateSummary::from(vec![4, 5, 6]); // B's correct summary

        // Setup: both peers have interest in the contract
        manager_a.register_local_hosting(&contract);
        manager_b.register_local_hosting(&contract);

        // A tracks B's summary, B tracks A's summary
        manager_a.register_peer_interest(
            &contract,
            peer_b.clone(),
            Some(new_summary.clone()),
            false,
        );
        manager_b.register_peer_interest(
            &contract,
            peer_a.clone(),
            Some(old_summary.clone()),
            false,
        );

        // Step 1: A sends ResyncRequest
        // B receives it and clears A's cached summary
        manager_b.clear_peer_summary(&contract, &peer_a, SummaryMissingReason::ClearedByResync);

        // Verify B cleared A's summary
        let cached = manager_b.get_peer_summary(&contract, &peer_a);
        assert!(cached.is_none(), "B should have cleared A's summary");

        // Step 2: B sends ResyncResponse with full state and summary
        // A receives it and updates B's summary
        manager_a.update_peer_summary(&contract, &peer_b, new_summary.clone());

        // Verify A has B's new summary
        let cached = manager_a.get_peer_summary(&contract, &peer_b);
        assert!(cached.is_some(), "A should have B's summary");
        assert_eq!(
            cached.unwrap().as_ref(),
            new_summary.as_ref(),
            "A should have B's correct summary"
        );

        // Both peers should still be interested
        assert!(
            manager_a
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer_b)
        );
        assert!(
            manager_b
                .get_interested_peers(&contract)
                .iter()
                .any(|(pk, _)| pk == &peer_a)
        );
    }

    #[test]
    fn test_delta_vs_full_state_decision() {
        // This test verifies the inputs to the delta-vs-full-state decision:
        // 1. Whether we have peer's summary (None = full state)
        // 2. The is_delta_efficient summary-size heuristic's boundaries.
        //
        // NOTE (#4923): the heuristic is no longer a pre-compute refusal in
        // `compute_delta` — a large summary now still gets a real delta
        // computed, and only a delta that is not smaller than the full state
        // is refused (post-compute). The assertions below pin the heuristic
        // function itself, not the (removed) gate wiring.

        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let peer_with_summary = make_peer_key(1);
        let peer_without_summary = make_peer_key(2);

        // Register local hosting to index the contract
        manager.register_local_hosting(&contract);

        // Small summary (efficient for delta)
        let small_summary = StateSummary::from(vec![1; 100]); // 100 bytes
        let large_state_size = 1000; // 1000 bytes -> summary is 10%, delta efficient

        // Large summary (not efficient for delta)
        let large_summary = StateSummary::from(vec![1; 600]); // 600 bytes
        // 600/1000 = 60% > 50%, delta NOT efficient

        // Register peer1 with small summary (delta should be efficient)
        manager.register_peer_interest(
            &contract,
            peer_with_summary.clone(),
            Some(small_summary.clone()),
            false,
        );

        // Register peer2 with no summary (should send full state)
        manager.register_peer_interest(&contract, peer_without_summary.clone(), None, false);

        // Test 1: Peer with summary - check if delta is efficient
        let peer_summary = manager.get_peer_summary(&contract, &peer_with_summary);
        assert!(peer_summary.is_some(), "peer should have summary");
        let summary = peer_summary.unwrap();
        assert!(
            is_delta_efficient(summary.as_ref().len(), large_state_size),
            "small summary should be efficient for delta"
        );

        // Test 2: Peer without summary - should send full state
        let peer_summary = manager.get_peer_summary(&contract, &peer_without_summary);
        assert!(
            peer_summary.is_none(),
            "peer without summary should trigger full state"
        );

        // Test 3: Large summary - delta not efficient
        assert!(
            !is_delta_efficient(large_summary.as_ref().len(), large_state_size),
            "large summary (>50% of state) should not be efficient for delta"
        );

        // Test 4: Edge case - summary exactly 50% of state size
        let half_summary = StateSummary::from(vec![1; 500]); // 500 bytes
        // 500 * 2 = 1000, not < 1000, so not efficient
        assert!(
            !is_delta_efficient(half_summary.as_ref().len(), large_state_size),
            "summary at exactly 50% boundary should not be efficient"
        );

        // Test 5: Summary just under 50%
        let just_under_half = StateSummary::from(vec![1; 499]); // 499 bytes
        // 499 * 2 = 998 < 1000, so efficient
        assert!(
            is_delta_efficient(just_under_half.as_ref().len(), large_state_size),
            "summary just under 50% should be efficient"
        );
    }

    #[test]
    fn test_broadcast_peer_selection() {
        // Test that we correctly identify which peers to broadcast to
        // and whether to use delta or full state for each

        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        let peer1 = make_peer_key(1); // Has summary
        let peer2 = make_peer_key(2); // No summary
        let peer3 = make_peer_key(3); // Has summary

        let summary1 = StateSummary::from(vec![1, 2, 3]);
        let summary3 = StateSummary::from(vec![3, 2, 1]);

        // Setup: register all peers with interest
        manager.register_local_hosting(&contract);
        manager.register_peer_interest(&contract, peer1.clone(), Some(summary1.clone()), false);
        manager.register_peer_interest(&contract, peer2.clone(), None, false);
        manager.register_peer_interest(&contract, peer3.clone(), Some(summary3.clone()), false);

        // Get all interested peers
        let interested = manager.get_interested_peers(&contract);
        assert_eq!(interested.len(), 3);

        // For each peer, check what type of update they should receive
        let mut delta_peers = Vec::new();
        let mut full_state_peers = Vec::new();

        for (peer_key, _interest) in &interested {
            if let Some(_summary) = manager.get_peer_summary(&contract, peer_key) {
                delta_peers.push(peer_key.clone());
            } else {
                full_state_peers.push(peer_key.clone());
            }
        }

        // Verify classification
        assert_eq!(delta_peers.len(), 2);
        assert!(delta_peers.contains(&peer1));
        assert!(delta_peers.contains(&peer3));

        assert_eq!(full_state_peers.len(), 1);
        assert!(full_state_peers.contains(&peer2));
    }

    #[test]
    fn test_get_contracts_for_peer() {
        let (manager, _time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);
        let peer = make_peer_key(1);

        // Initially no contracts for peer
        let contracts = manager.get_contracts_for_peer(&peer);
        assert!(contracts.is_empty());

        // Register peer interest in contracts 1 and 2
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_peer_interest(&contract2, peer.clone(), None, false);

        let contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(contracts.len(), 2);
        assert!(contracts.contains(&contract1));
        assert!(contracts.contains(&contract2));
        assert!(!contracts.contains(&contract3));

        // Remove interest in contract1
        manager.remove_peer_interest(&contract1, &peer);
        let contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(contracts.len(), 1);
        assert!(contracts.contains(&contract2));
    }

    #[test]
    fn test_full_replace_interest_sync() {
        // Simulate the full-replace semantics used by heartbeat handler:
        // receiving Interests { hashes } should add new entries, refresh shared
        // entries, and remove entries not in the incoming set.
        let (manager, time) = make_manager();
        let contract1 = make_contract_key(1);
        let contract2 = make_contract_key(2);
        let contract3 = make_contract_key(3);
        let peer = make_peer_key(1);

        // We have local interest in all three contracts
        manager.register_local_hosting(&contract1);
        manager.register_local_hosting(&contract2);
        manager.register_local_hosting(&contract3);

        // Initial state: peer is interested in contracts 1 and 2
        manager.register_peer_interest(&contract1, peer.clone(), None, false);
        manager.register_peer_interest(&contract2, peer.clone(), None, false);

        // Advance time so we can verify refresh
        time.advance_time(Duration::from_secs(60));

        // Simulate heartbeat: peer now sends hashes for contracts 2 and 3
        // (dropped 1, kept 2, added 3)
        let incoming_hashes: HashSet<u32> = [contract_hash(&contract2), contract_hash(&contract3)]
            .into_iter()
            .collect();

        // Step 1: Get peer's current interest set
        let current_contracts = manager.get_contracts_for_peer(&peer);
        assert_eq!(current_contracts.len(), 2);

        // Step 2: Remove entries whose hash is NOT in incoming set
        // (mirrors the handler's hash-domain comparison, not resolved keys)
        for contract in &current_contracts {
            let h = contract_hash(contract);
            if !incoming_hashes.contains(&h) {
                manager.remove_peer_interest(contract, &peer);
            }
        }

        // Step 3: Find matching contracts and register/refresh
        let matching =
            manager.get_matching_contracts(&incoming_hashes.iter().copied().collect::<Vec<_>>());
        for contract in &matching {
            if manager.get_peer_interest(contract, &peer).is_some() {
                // Existing entry: refresh TTL (preserves cached summary)
                manager.refresh_peer_interest(contract, &peer);
            } else {
                // New entry
                manager.register_peer_interest(contract, peer.clone(), None, false);
            }
        }

        // Verify: contract1 removed, contract2 refreshed, contract3 added
        assert!(
            manager.get_peer_interest(&contract1, &peer).is_none(),
            "contract1 should have been removed"
        );
        assert!(
            manager.get_peer_interest(&contract2, &peer).is_some(),
            "contract2 should still exist (refreshed)"
        );
        assert!(
            manager.get_peer_interest(&contract3, &peer).is_some(),
            "contract3 should have been added"
        );

        // Verify contract2 was refreshed (TTL reset)
        let interest2 = manager.get_peer_interest(&contract2, &peer).unwrap();
        assert!(
            !interest2.is_expired_at(time.now()),
            "contract2 interest should not be expired after refresh"
        );
    }

    #[test]
    fn test_refresh_preserves_summary() {
        // Verify that refresh_peer_interest preserves the cached summary,
        // unlike register_peer_interest which overwrites it.
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);
        let summary = StateSummary::from(vec![1, 2, 3]);

        // Register with a summary
        manager.register_peer_interest(&contract, peer.clone(), Some(summary.clone()), false);

        // Advance time
        time.advance_time(Duration::from_secs(60));

        // Refresh TTL (should preserve summary)
        manager.refresh_peer_interest(&contract, &peer);

        // Verify summary is still there
        let cached = manager.get_peer_summary(&contract, &peer);
        assert!(
            cached.is_some(),
            "summary should be preserved after refresh"
        );
        assert_eq!(cached.unwrap().as_ref(), summary.as_ref());

        // Verify TTL was reset
        let interest = manager.get_peer_interest(&contract, &peer).unwrap();
        assert!(
            !interest.is_expired_at(time.now()),
            "interest should not be expired after refresh"
        );
    }

    #[test]
    fn test_is_upstream_flag_registration() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let upstream_peer = make_peer_key(1);
        let downstream_peer = make_peer_key(2);

        // Register upstream peer with is_upstream=true
        manager.register_peer_interest(&contract, upstream_peer.clone(), None, true);

        // Register downstream peer with is_upstream=false
        manager.register_peer_interest(&contract, downstream_peer.clone(), None, false);

        // Verify the is_upstream flag is preserved correctly
        let upstream_interest = manager
            .get_peer_interest(&contract, &upstream_peer)
            .unwrap();
        assert!(
            upstream_interest.is_upstream,
            "Peer registered with is_upstream=true should have is_upstream=true"
        );

        let downstream_interest = manager
            .get_peer_interest(&contract, &downstream_peer)
            .unwrap();
        assert!(
            !downstream_interest.is_upstream,
            "Peer registered with is_upstream=false should have is_upstream=false"
        );

        // Verify get_interested_peers returns both with correct flags
        let peers = manager.get_interested_peers(&contract);
        assert_eq!(peers.len(), 2);

        let upstream_entry = peers.iter().find(|(k, _)| k == &upstream_peer).unwrap();
        assert!(upstream_entry.1.is_upstream);

        let downstream_entry = peers.iter().find(|(k, _)| k == &downstream_peer).unwrap();
        assert!(!downstream_entry.1.is_upstream);
    }

    /// Primitive-contract pin underpinning the D2 clobber fix in the
    /// `ChangeInterests` interest-sync handler (`node.rs`): a peer that is
    /// already our UPSTREAM host (`is_upstream = true`, set when we subscribed
    /// through it) must not be downgraded to a plain downstream interest when it
    /// re-advertises interest via a `ChangeInterests { added }` gossip. (That
    /// gossip is EVENT-DRIVEN — emitted only on a 0->1 interest transition, not
    /// on the ~5-min heartbeat, which sends the already-guarded `Interests`
    /// full-replace arm.)
    ///
    /// `register_peer_interest(.., is_upstream = false)` overwrites the whole
    /// `PeerInterest`, flipping `is_upstream` true -> false and wiping the
    /// cached delta-sync summary to `None`. The handler therefore guards the
    /// re-registration of an EXISTING entry on `refresh_peer_interest()`'s own
    /// return value, and the refresh preserves both.
    ///
    /// SCOPE: this exercises the InterestManager PRIMITIVES directly, so it is a
    /// characterization of the contract the guard relies on — it PASSES on the
    /// pre-fix handler too (the bug was the handler calling the wrong
    /// primitive, not a primitive misbehaving). The handler-WIRING regression
    /// signal — the test that FAILS on the pre-fix unguarded arm — is
    /// `change_interests_arm_guards_register_with_refresh_pin` in `node.rs`.
    /// What this pins:
    ///   1. `refresh` PRESERVES `is_upstream` + summary, so
    ///      `send_unsubscribe_upstream`'s lookup
    ///      (`get_interested_peers().find(|i| i.is_upstream)`) still resolves,
    ///      keeping event-driven chain collapse working; and
    ///   2. a bare `register(false)` CLOBBERS both — the failure mode the
    ///      `ChangeInterests` handler exhibited before the guard was added.
    #[test]
    fn upstream_interest_survives_refresh_but_bare_register_clobbers_it() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);
        let upstream = make_peer_key(1);
        let summary = StateSummary::from(vec![1u8, 2, 3]);

        // We subscribed through `upstream`, so it is registered as our upstream
        // host with a cached delta-sync summary.
        manager.register_peer_interest(&contract, upstream.clone(), Some(summary.clone()), true);
        let before = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(before.is_upstream);
        assert_eq!(before.summary.as_ref(), Some(&summary));

        // FIXED handler path: an existing entry is refreshed, not
        // re-registered. Refresh preserves is_upstream AND the cached summary.
        manager.refresh_peer_interest(&contract, &upstream);
        let after_refresh = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(
            after_refresh.is_upstream,
            "refresh must preserve is_upstream so send_unsubscribe_upstream can \
             still find the upstream"
        );
        assert_eq!(
            after_refresh.summary.as_ref(),
            Some(&summary),
            "refresh must preserve the cached delta-sync summary"
        );

        // `send_unsubscribe_upstream` locates the upstream exactly this way;
        // assert it still resolves after the heartbeat refresh.
        let found = manager
            .get_interested_peers(&contract)
            .into_iter()
            .find(|(_, i)| i.is_upstream)
            .map(|(p, _)| p);
        assert_eq!(
            found,
            Some(upstream.clone()),
            "the upstream lookup used by send_unsubscribe_upstream must still \
             find the peer after a refresh"
        );

        // BUG path (the pre-fix unguarded `ChangeInterests` handler): a bare
        // register(false) overwrites the entry, clobbering BOTH fields.
        manager.register_peer_interest(&contract, upstream.clone(), None, false);
        let clobbered = manager.get_peer_interest(&contract, &upstream).unwrap();
        assert!(
            !clobbered.is_upstream,
            "documents the clobber: a bare register(false) flips is_upstream \
             true -> false"
        );
        assert!(
            clobbered.summary.is_none(),
            "documents the clobber: a bare register(false) wipes the cached summary"
        );
        assert!(
            manager
                .get_interested_peers(&contract)
                .into_iter()
                .all(|(_, i)| !i.is_upstream),
            "after the clobber, send_unsubscribe_upstream can no longer find any \
             upstream — the event-driven-collapse defeat this fix prevents"
        );
    }

    #[test]
    fn test_register_peer_interest_resets_ttl() {
        // Verify that register_peer_interest resets TTL for existing entries.
        // The heartbeat relies on this for new-entry registration.
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time to nearly expired
        time.advance_time(INTEREST_TTL - Duration::from_secs(10));

        // Re-register (as heartbeat would for a new entry)
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Advance time past original registration but not past re-registration
        time.advance_time(Duration::from_secs(20));

        // Should not be expired
        let expired = manager.sweep_expired_interests();
        assert!(expired.is_empty(), "re-registration should have reset TTL");
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_subscribe_registers_local_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        assert!(!manager.has_local_interest(&contract));

        let became_interested = manager.add_local_client(&contract);
        assert!(became_interested);
        assert!(manager.has_local_interest(&contract));

        // Second call should not report "became interested" (already was)
        let became_interested_again = manager.add_local_client(&contract);
        assert!(!became_interested_again);
        assert!(manager.has_local_interest(&contract));
    }

    /// Regression test for #3467: relay nodes must have has_local_interest() = true
    /// when they have downstream subscribers, otherwise ChangeInterests processing
    /// is blocked and interest-based broadcast targeting breaks.
    #[test]
    fn test_downstream_subscriber_creates_local_interest() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(50);

        // Before adding downstream: no local interest
        assert!(!manager.has_local_interest(&contract));

        // Add downstream subscriber — should create local interest
        let became_interested = manager.add_downstream_subscriber(&contract);
        assert!(
            became_interested,
            "First downstream subscriber should create interest"
        );
        assert!(
            manager.has_local_interest(&contract),
            "Relay node with downstream subscriber must have local interest"
        );

        // Second downstream subscriber should not re-report "became interested"
        let became_interested_again = manager.add_downstream_subscriber(&contract);
        assert!(!became_interested_again);
        assert!(manager.has_local_interest(&contract));

        // Remove one downstream — still have one left
        let lost_interest = manager.remove_downstream_subscriber(&contract);
        assert!(!lost_interest, "Still have one downstream subscriber");
        assert!(manager.has_local_interest(&contract));

        // Remove last downstream — should lose interest
        let lost_interest = manager.remove_downstream_subscriber(&contract);
        assert!(lost_interest, "Last downstream subscriber removed");
        assert!(
            !manager.has_local_interest(&contract),
            "No downstream subscribers left — should lose interest"
        );
    }

    /// `active_demand_count()` counts only contracts backed by REAL demand — a
    /// local client subscription or a downstream subscriber — and EXCLUDES
    /// cache-only `hosting` interest. This is the denominator for the #3763
    /// no-storm invariant, so the exclusion of hosting-only contracts is the
    /// load-bearing behavior and is asserted directly here (not just logged in
    /// the sim harness).
    #[test]
    fn test_active_demand_count_excludes_cache_only_hosting() {
        let (manager, _time) = make_manager();

        assert_eq!(manager.active_demand_count(), 0, "empty manager → 0 demand");

        // Cache-only hosting (no client, no downstream) is interest but NOT demand.
        let hosting_only = make_contract_key(1);
        manager.register_local_hosting(&hosting_only);
        assert!(
            manager.has_local_interest(&hosting_only),
            "register_local_hosting creates local interest"
        );
        assert_eq!(
            manager.active_demand_count(),
            0,
            "a hosting-only contract is interest but must NOT count as active demand"
        );

        // A local client subscription IS demand.
        let client = make_contract_key(2);
        manager.add_local_client(&client);
        assert_eq!(
            manager.active_demand_count(),
            1,
            "a local client subscription is active demand"
        );

        // A downstream subscriber IS demand.
        let downstream = make_contract_key(3);
        manager.add_downstream_subscriber(&downstream);
        assert_eq!(
            manager.active_demand_count(),
            2,
            "a downstream subscriber is active demand"
        );

        // Adding cache-only hosting on top of the client-demand contract must
        // neither double-count it nor change the total.
        manager.register_local_hosting(&client);
        assert_eq!(
            manager.active_demand_count(),
            2,
            "hosting layered on top of an already-demanded contract does not change the count"
        );

        // The hosting-only contract is genuinely tracked as interest — the
        // point is that interest (which includes hosting) and demand (which
        // does not) are distinct: it is interested but excluded from demand.
        assert!(
            manager.has_local_interest(&hosting_only),
            "the hosting-only contract is still tracked as local interest"
        );
        assert_eq!(
            manager.active_demand_count(),
            2,
            "...yet it is still excluded from the active-demand count"
        );
    }

    #[test]
    fn test_deferred_removal_executes_after_grace_period() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Schedule deferred removal
        manager.schedule_deferred_removal(&peer);

        // Before grace period expires, interests should still exist
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD - Duration::from_secs(1));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // After grace period expires, interests should be removed
        time.advance_time(Duration::from_secs(2));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 1);
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_deferred_removal_cancelled_on_reconnect() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        // Register interest
        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // Schedule deferred removal (peer disconnected)
        manager.schedule_deferred_removal(&peer);

        // Peer reconnects within grace period
        time.advance_time(Duration::from_secs(30));
        let cancelled = manager.cancel_deferred_removal(&peer);
        assert!(cancelled);

        // Even after grace period, interests should still exist
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD);
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(manager.get_peer_interest(&contract, &peer).is_some());
    }

    #[test]
    fn test_deferred_removal_replaces_on_repeated_disconnect() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, false);

        // First disconnect
        manager.schedule_deferred_removal(&peer);
        time.advance_time(Duration::from_secs(60));

        // Second disconnect before first grace period expires — resets deadline
        manager.schedule_deferred_removal(&peer);

        // Original deadline would have passed, but new one hasn't
        time.advance_time(Duration::from_secs(60));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0, "Second schedule should have reset the deadline");
        assert!(manager.get_peer_interest(&contract, &peer).is_some());

        // Now exceed the second deadline
        time.advance_time(Duration::from_secs(31));
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 1);
        assert!(manager.get_peer_interest(&contract, &peer).is_none());
    }

    #[test]
    fn test_cancel_deferred_removal_returns_false_when_none_pending() {
        let (manager, _time) = make_manager();
        let peer = make_peer_key(1);

        // No pending removal — cancel should return false
        assert!(!manager.cancel_deferred_removal(&peer));
    }

    /// Regression test: if cancel_deferred_removal runs between the collect phase
    /// and the removal phase of execute_pending_removals, the removal must be
    /// skipped (the peer reconnected). Without the guard on pending_removals.remove(),
    /// interests would be wiped even though the peer is back.
    #[test]
    fn test_execute_skips_removal_if_cancelled_between_collect_and_remove() {
        let (manager, time) = make_manager();
        let contract = make_contract_key(1);
        let peer = make_peer_key(1);

        manager.register_peer_interest(&contract, peer.clone(), None, false);
        manager.schedule_deferred_removal(&peer);

        // Advance past grace period
        time.advance_time(INTEREST_DISCONNECT_GRACE_PERIOD + Duration::from_secs(1));

        // Simulate reconnect cancelling the pending removal before sweep executes
        manager.cancel_deferred_removal(&peer);

        // execute_pending_removals should return 0 — the entry was already cancelled
        let removed = manager.execute_pending_removals();
        assert_eq!(removed, 0);
        assert!(
            manager.get_peer_interest(&contract, &peer).is_some(),
            "Interests must be preserved when peer reconnected before sweep executed"
        );
    }

    /// Verify that summary mismatch detection correctly identifies stale peers
    /// and that only the specific stale peer needs updating (not all subscribers).
    ///
    /// Regression test for #3791: summary mismatch triggered BroadcastStateChange
    /// to ALL subscribers instead of SyncStateToPeer to just the stale peer,
    /// causing O(peers^2) broadcast storms.
    #[test]
    fn test_summary_mismatch_targets_only_stale_peer() {
        let (manager, _time) = make_manager();

        let contract = make_contract_key(1);
        let peer_a = make_peer_key(1);
        let peer_b = make_peer_key(2);
        let peer_c = make_peer_key(3);

        manager.register_local_hosting(&contract);

        // Our state summary
        let our_summary = StateSummary::from(vec![1, 2, 3]);

        // Peer A and C have our current summary (up to date)
        manager.register_peer_interest(&contract, peer_a.clone(), Some(our_summary.clone()), false);
        manager.register_peer_interest(&contract, peer_c.clone(), Some(our_summary.clone()), false);

        // Peer B has an old summary (stale)
        let stale_summary = StateSummary::from(vec![0, 0, 0]);
        manager.register_peer_interest(
            &contract,
            peer_b.clone(),
            Some(stale_summary.clone()),
            false,
        );

        // Use the same stale-detection logic as production (node.rs):
        // zip both Option<StateSummary> and compare bytes.
        let peer_b_summary = manager.get_peer_summary(&contract, &peer_b);
        let is_stale = Some(&our_summary)
            .zip(peer_b_summary.as_ref())
            .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());
        assert!(is_stale, "Peer B should be detected as stale");

        // Peers A and C have our current summary and should NOT be stale
        for (label, peer) in [("A", &peer_a), ("C", &peer_c)] {
            let summary = manager.get_peer_summary(&contract, peer);
            let stale = Some(&our_summary)
                .zip(summary.as_ref())
                .is_some_and(|(ours, theirs)| ours.as_ref() != theirs.as_ref());
            assert!(!stale, "Peer {label} should NOT be stale");
        }

        // The fix (#3791): only peer B needs a state sync, not all 3 peers.
        // Before the fix, BroadcastStateChange would send to all 3 peers.
        // After the fix, SyncStateToPeer sends only to peer B.
        let interested_peers = manager.get_interested_peers(&contract);
        assert_eq!(
            interested_peers.len(),
            3,
            "All 3 peers should be interested"
        );

        // Count how many peers actually need syncing
        let stale_count = interested_peers
            .iter()
            .filter(|(pk, _)| {
                let summary = manager.get_peer_summary(&contract, pk);
                summary
                    .as_ref()
                    .map(|s| s.as_ref() != our_summary.as_ref())
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(
            stale_count,
            1,
            "Only 1 peer (B) should need syncing, not all {}",
            interested_peers.len()
        );
    }

    /// Regression test for the PR #4129 add-then-index race in
    /// `InterestManager`.
    ///
    /// Before the fix, `add_local_client` / `register_local_hosting` /
    /// `add_downstream_subscriber` / `register_peer_interest` /
    /// `register_local_interest` released the `local_interests` (or
    /// `interested_peers`) shard guard before calling
    /// `index_contract_hash`. A concurrent `remove_*` for the same
    /// contract could then acquire the guard, decrement the last reason,
    /// run `cleanup_contract_if_no_interest` → `unindex_contract_hash`
    /// (a no-op because we haven't indexed yet), and the deferred index
    /// would leak a zombie entry into `contract_hash_index`.
    ///
    /// Two properties make this race awkward to test:
    ///
    /// 1. The zombie only PERSISTS if the contract sees no further
    ///    activity — a later add re-establishes backing interest, a
    ///    later remove's cleanup unindexes it. A stress test that
    ///    hammers one shared contract therefore continuously heals it.
    /// 2. The racy window (`local_interests` guard drop → deferred
    ///    `index_contract_hash`) is a handful of instructions wide.
    ///
    /// This test addresses both: each ROUND uses a fresh contract and
    /// runs exactly ONE add racing exactly ONE remove. A barrier
    /// releases the adder and remover simultaneously to maximize
    /// overlap. Because there is only one add and one remove, nothing
    /// can heal a zombie once created — it persists to the post-round
    /// check, which reads the three maps directly and calls no
    /// `remove_*` (which would trigger cleanup and heal it).
    ///
    /// Each of the four real add/remove PAIRS is exercised round-robin:
    /// `register_peer_interest`/`remove_peer_interest`,
    /// `register_local_hosting`/`unregister_local_hosting`,
    /// `add_local_client`/`remove_local_client`,
    /// `add_downstream_subscriber`/`remove_downstream_subscriber`. The
    /// fifth fixed site, `register_local_interest`, gets the same
    /// lock-across-index discipline but is NOT raced here: it is dead
    /// code (no workspace caller) with no symmetric remove operation, so
    /// there is no natural pair to race it against. It is structurally
    /// identical to the tested `register_local_hosting` and is guarded
    /// by code review plus the `.claude/rules/ring.md` rule entry.
    ///
    /// The fix holds the shard guard across `index_contract_hash`, so
    /// the racy interleaving cannot occur and no round produces a
    /// zombie.
    #[test]
    fn test_concurrent_add_remove_preserves_hash_index_invariant() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let (manager, _time) = make_manager();
        let manager = Arc::new(manager);

        let rounds: u32 = 120_000;

        // Per-round spec shared with the two worker threads:
        // (contract, which-pair, stop-sentinel).
        let spec: Arc<Mutex<(ContractKey, u32, bool)>> =
            Arc::new(Mutex::new((make_unique_contract_key(0), 0, false)));
        // 3 parties: adder, remover, main.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));

        // Single shared peer key for the peer-interest pair. The remover
        // drains by enumerating `interested_peers` so it needs no key.
        let peer = make_peer_key(0);

        let adder = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, which, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    match which {
                        0 => {
                            manager.register_peer_interest(&contract, peer.clone(), None, false);
                        }
                        1 => {
                            manager.register_local_hosting(&contract);
                        }
                        2 => {
                            manager.add_local_client(&contract);
                        }
                        _ => {
                            manager.add_downstream_subscriber(&contract);
                        }
                    }
                    round_end.wait();
                }
            })
        };

        let remover = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, which, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    match which {
                        0 => {
                            let peers: Vec<PeerKey> = manager
                                .interested_peers
                                .get(&contract)
                                .map(|e| e.keys().cloned().collect())
                                .unwrap_or_default();
                            for p in peers {
                                manager.remove_peer_interest(&contract, &p);
                            }
                        }
                        1 => {
                            manager.unregister_local_hosting(&contract);
                        }
                        2 => {
                            manager.remove_local_client(&contract);
                        }
                        _ => {
                            manager.remove_downstream_subscriber(&contract);
                        }
                    }
                    round_end.wait();
                }
            })
        };

        let mut zombies: Vec<(u32, u32)> = Vec::new();
        for round in 0..rounds {
            let contract = make_unique_contract_key(round);
            let which = round % 4;
            *spec.lock().unwrap() = (contract, which, false);

            round_start.wait(); // release adder + remover simultaneously
            round_end.wait(); // both have completed their single op

            // Activity on `contract` has fully stopped — exactly one add
            // and one remove ran, nothing can heal a zombie now. Check
            // the shard-consistency invariant directly, calling no
            // `remove_*` (which would trigger cleanup). A zombie =
            // indexed in `contract_hash_index`, absent from BOTH
            // `local_interests` and `interested_peers`.
            // `lookup_by_hash` returns every contract sharing the 32-bit
            // hash, so check membership of THIS contract specifically —
            // `!is_empty()` would false-positive on a hash collision.
            let in_chi = manager
                .lookup_by_hash(contract_hash(&contract))
                .contains(&contract);
            let in_li = manager.local_interests.contains_key(&contract);
            let in_ip = manager.interested_peers.contains_key(&contract);
            if in_chi && !in_li && !in_ip {
                zombies.push((round, which));
            }
        }

        // Signal both workers to exit, then release them off round_start.
        *spec.lock().unwrap() = (make_unique_contract_key(0), 0, true);
        round_start.wait();
        adder.join().unwrap();
        remover.join().unwrap();

        assert!(
            zombies.is_empty(),
            "{} of {rounds} single-add/single-remove rounds leaked a \
             zombie entry into contract_hash_index (no backing \
             local_interests or interested_peers). This is the PR #4129 \
             race that PR #4171 fixes. First offenders (round, pair): \
             {:?}",
            zombies.len(),
            &zombies[..zombies.len().min(10)]
        );
    }

    /// Regression test for issue #4174: `remove_all_peer_interests` must
    /// preserve the bidirectional invariant
    /// `peer ∈ peer_contracts[peer] ⇔ peer ∈ interested_peers[contract]`
    /// when racing against a concurrent `register_peer_interest`.
    ///
    /// The bug: the old `remove_all_peer_interests` removed the
    /// `peer_contracts[peer]` entry up front, captured a snapshot of the
    /// contract set, then iterated that snapshot and mutated
    /// `interested_peers` directly. A concurrent
    /// `register_peer_interest(C, peer, ..)` for a contract `C` that is
    /// already in the snapshot — running in the window AFTER the up-front
    /// `peer_contracts.remove` but BEFORE the per-contract
    /// `interested_peers[C]` mutation — re-inserts `peer` into BOTH maps.
    /// `remove_all_peer_interests` then strips `peer` from
    /// `interested_peers[C]` (it still has `C` in its stale snapshot) but
    /// the freshly-re-created reverse entry in `peer_contracts[peer]`
    /// survives — leaving a one-sided "ghost": `peer ∈ peer_contracts`
    /// while `peer ∉ interested_peers[C]`.
    ///
    /// The fix delegates per-contract cleanup to `remove_peer_interest`,
    /// which holds the `interested_peers[contract]` shard guard across
    /// the `peer_contracts` update so each removal is atomic against a
    /// concurrent `register_peer_interest`.
    ///
    /// Test design (mirrors
    /// `test_concurrent_add_remove_preserves_hash_index_invariant`):
    /// barrier-synced rounds with a fresh contract per round. CRITICAL —
    /// to reproduce the race the contract must already be in the peer's
    /// `peer_contracts` set when `remove_all_peer_interests` snapshots
    /// it, so each round PRE-REGISTERS the contract on the main thread
    /// before opening the barrier. The two workers then race a
    /// re-`register_peer_interest` (refresh) of that already-registered
    /// contract against `remove_all_peer_interests`. After each round
    /// all activity on the contract has stopped, so the bidirectional
    /// invariant must hold regardless of interleaving — any violation is
    /// a real ghost left behind by the race.
    ///
    /// Sensitivity: with the fix reverted to the racy body, this test
    /// caught the race in 10/10 runs of 200_000 rounds each (the
    /// pre-registration is what makes it reliable — without it the
    /// snapshot never contains the raced contract and the test cannot
    /// see the bug). With the fix applied it passes 10/10.
    #[test]
    fn test_concurrent_remove_all_preserves_bidirectional_invariant() {
        use std::sync::{Arc, Barrier, Mutex};
        use std::thread;

        let (manager, _time) = make_manager();
        let manager = Arc::new(manager);

        let rounds: u32 = 200_000;

        // Per-round spec shared with the two worker threads:
        // (contract, stop-sentinel).
        let spec: Arc<Mutex<(ContractKey, bool)>> =
            Arc::new(Mutex::new((make_unique_contract_key(0), false)));
        // 3 parties: registrar, remover, main.
        let round_start = Arc::new(Barrier::new(3));
        let round_end = Arc::new(Barrier::new(3));

        // Single shared peer key raced across every round.
        let peer = make_peer_key(0);

        // Registrar: re-registers (refreshes) the peer's interest in the
        // round's contract — which the main thread has already
        // registered before the barrier opened.
        let registrar = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (contract, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    manager.register_peer_interest(&contract, peer.clone(), None, false);
                    round_end.wait();
                }
            })
        };

        // Remover: wipes ALL of the peer's interests, racing the
        // registrar above.
        let remover = {
            let manager = Arc::clone(&manager);
            let spec = Arc::clone(&spec);
            let round_start = Arc::clone(&round_start);
            let round_end = Arc::clone(&round_end);
            let peer = peer.clone();
            thread::spawn(move || {
                loop {
                    round_start.wait();
                    let (_contract, stop) = *spec.lock().unwrap();
                    if stop {
                        break;
                    }
                    manager.remove_all_peer_interests(&peer);
                    round_end.wait();
                }
            })
        };

        let mut ghosts: Vec<u32> = Vec::new();
        for round in 0..rounds {
            let contract = make_unique_contract_key(round);

            // Pre-register the contract BEFORE opening the barrier so it
            // is guaranteed to be in `remove_all_peer_interests`'s
            // snapshot — this is what makes the #4174 race observable.
            manager.register_peer_interest(&contract, peer.clone(), None, false);

            *spec.lock().unwrap() = (contract, false);

            round_start.wait(); // release registrar + remover simultaneously
            round_end.wait(); // both have completed their single op

            // Activity on `contract` has fully stopped. Check the
            // bidirectional invariant directly, without calling any
            // `remove_*` (which would trigger cleanup and mask a
            // ghost). A ghost = `peer` present on exactly one side:
            //   peer ∈ peer_contracts[peer]  XOR  peer ∈ interested_peers[contract]
            let in_peer_contracts = manager
                .peer_contracts
                .get(&peer)
                .map(|e| e.value().contains(&contract))
                .unwrap_or(false);
            let in_interested_peers = manager
                .interested_peers
                .get(&contract)
                .map(|e| e.contains_key(&peer))
                .unwrap_or(false);
            if in_peer_contracts != in_interested_peers {
                ghosts.push(round);
            }

            // Clean slate for the next round: if the registrar won the
            // race the contract may still be registered. Drop it so the
            // peer's contract set does not grow unboundedly (which would
            // slow every later `remove_all_peer_interests` snapshot).
            manager.remove_peer_interest(&contract, &peer);
        }

        // Signal both workers to exit, then release them off round_start.
        *spec.lock().unwrap() = (make_unique_contract_key(0), true);
        round_start.wait();
        registrar.join().unwrap();
        remover.join().unwrap();

        assert!(
            ghosts.is_empty(),
            "{} of {rounds} register/remove-all rounds left a one-sided \
             ghost: `peer` present in exactly one of peer_contracts / \
             interested_peers for the round's contract. This is the \
             issue #4174 bidirectional-consistency race. First offending \
             rounds: {:?}",
            ghosts.len(),
            &ghosts[..ghosts.len().min(10)]
        );
    }

    // ---- Semantic staleness (#4857 secondary finding / summarize storm) ----
    //
    // The InterestSync heartbeat used to decide "is this peer stale?" with a
    // raw byte comparison of `summarize_state` output. A contract whose summary
    // serializes non-deterministically (HashMap/HashSet iteration order,
    // per-process RandomState) produces DIFFERENT summary bytes for the SAME
    // logical state across peers, so the byte compare flagged a fully-converged
    // peer stale and fired a full-state heal every heartbeat — the 2.56M
    // rate-limited `summarize_contract_state` storm observed on the 0.2.102
    // gateway. The fix asks the CONTRACT (via its own `get_state_delta`,
    // surfaced here through the shared delta cache / `cached_staleness_verdict`)
    // whether we actually hold state the peer lacks.

    #[test]
    fn nondeterministic_summary_does_not_flag_converged_peer_stale() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(1);

        // Two summaries of the SAME logical state that serialize to DIFFERENT
        // bytes (models cross-peer HashMap/HashSet iteration-order divergence).
        let ours = StateSummary::from(vec![1u8, 2, 3]);
        let theirs = StateSummary::from(vec![3u8, 2, 1]);

        // Precondition / reproduction: the pre-fix logic was exactly
        // `is_stale = our_bytes != their_bytes`, which flags this converged
        // peer stale and triggers the spurious heal.
        assert_ne!(
            ours.as_ref(),
            theirs.as_ref(),
            "precondition: summaries differ byte-wise (the false-stale trigger)"
        );
        assert!(
            summary_indicates_stale_peer(&ours, &theirs, None),
            "pre-fix byte comparison (no contract verdict) flags the converged \
             peer stale — this is the storm we are reproducing"
        );

        // The contract, asked for the delta of our state against their summary,
        // returns an EMPTY delta: logically converged despite differing bytes.
        // Model it exactly as production does — via the shared delta cache the
        // staleness oracle consults.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(Vec::<u8>::new()),
        );
        let verdict = manager.cached_staleness_verdict(&contract, theirs.as_ref(), ours.as_ref());
        assert_eq!(
            verdict,
            Some(false),
            "an empty cached delta means the contract sees the peer as converged"
        );

        // FIX: byte-differing summaries + empty delta => NOT stale => no heal.
        assert!(
            !summary_indicates_stale_peer(&ours, &theirs, verdict),
            "empty delta must suppress the spurious heal (fixes the storm)"
        );
    }

    #[test]
    fn genuinely_diverged_peer_is_still_flagged_stale() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(2);

        let ours = StateSummary::from(vec![9u8, 9, 9]);
        let theirs = StateSummary::from(vec![1u8]);

        // Contract returns a NON-EMPTY delta: our state holds data theirs lacks.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(vec![42u8]),
        );
        let verdict = manager.cached_staleness_verdict(&contract, theirs.as_ref(), ours.as_ref());
        assert_eq!(
            verdict,
            Some(true),
            "a non-empty delta is a real divergence"
        );

        // A genuine divergence must STILL heal — the fix only removes spurious
        // heals, never a real one.
        assert!(
            summary_indicates_stale_peer(&ours, &theirs, verdict),
            "genuine divergence must still be flagged stale and heal"
        );
    }

    #[test]
    fn identical_summaries_are_never_stale_without_probing() {
        let ours = StateSummary::from(vec![7u8, 7, 7]);
        let theirs = StateSummary::from(vec![7u8, 7, 7]);

        // Byte-identical summaries are trivially converged; the decision is
        // `false` regardless of (indeed, without needing) any delta verdict.
        assert!(!summary_indicates_stale_peer(&ours, &theirs, None));
        assert!(!summary_indicates_stale_peer(&ours, &theirs, Some(true)));
    }

    #[test]
    fn missing_delta_verdict_falls_back_to_byte_comparison() {
        let ours = StateSummary::from(vec![1u8, 2, 3]);
        let theirs_differ = StateSummary::from(vec![3u8, 2, 1]);
        let theirs_same = StateSummary::from(vec![1u8, 2, 3]);

        // When no semantic verdict is available (probe failed/timed out), we
        // preserve the conservative pre-fix behaviour: bytes differ => stale,
        // bytes equal => not stale. This guarantees we never SILENTLY skip a
        // real heal just because the delta probe was unavailable.
        assert!(summary_indicates_stale_peer(&ours, &theirs_differ, None));
        assert!(!summary_indicates_stale_peer(&ours, &theirs_same, None));
    }

    #[test]
    fn cached_staleness_verdict_reports_absence_and_emptiness() {
        let (manager, _time) = make_manager();
        let contract = make_contract_key(3);
        let ours = StateSummary::from(vec![5u8]);
        let theirs = StateSummary::from(vec![6u8]);

        // Not cached yet => no verdict (caller falls back to a contract probe).
        assert_eq!(
            manager.cached_staleness_verdict(&contract, theirs.as_ref(), ours.as_ref()),
            None
        );

        // Empty delta => converged; non-empty => diverged.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(Vec::<u8>::new()),
        );
        assert_eq!(
            manager.cached_staleness_verdict(&contract, theirs.as_ref(), ours.as_ref()),
            Some(false)
        );

        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(vec![1u8]),
        );
        assert_eq!(
            manager.cached_staleness_verdict(&contract, theirs.as_ref(), ours.as_ref()),
            Some(true)
        );
    }

    // ---- Post-compute efficiency gate (#4923) ------------------------------
    //
    // Production incident: `compute_delta` refused to even ASK the contract
    // for a delta whenever the peer's summary was >= 50% of our state size
    // (the pre-compute `is_delta_efficient` gate), and every caller answers
    // that refusal by sending FULL STATE — which is never smaller than the
    // delta that was declined. On the live network that arm was 41% of ALL
    // wire bytes (87.4% for the hottest contract), flat over time. The gate
    // now runs POST-compute, on the actual delta size. These tests drive the
    // real `compute_delta` against a real `OpManager` whose contract-handler
    // side is a mock responder task, so the whole path (cache lookup →
    // `GetDeltaQuery` → post-compute gate) is exercised.

    /// Build a real `OpManager` backed by a temp-dir `Config` (mirrors
    /// `summarize_delta_cache_tests::build_op_manager`) and spawn a mock
    /// contract handler that answers every `GetDeltaQuery` with
    /// `delta_bytes`, counting the queries it serves. The returned guard
    /// bundle keeps the other channel receivers + task monitor alive for the
    /// whole test (dropping them mid-run would tear down the OpManager's
    /// channels).
    async fn op_manager_with_mock_delta_handler(
        id: &str,
        delta_bytes: Vec<u8>,
    ) -> (
        std::sync::Arc<crate::node::OpManager>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        Box<dyn std::any::Any>,
    ) {
        use crate::contract::ContractHandlerEvent;

        let config_args = crate::config::ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(crate::contract::OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");

        let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        let (ops_ch_channel, mut ch_channel, wait_for_event) =
            crate::contract::contract_handler_channel();
        let connection_manager = crate::ring::ConnectionManager::new(&node_config);
        let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
        let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

        let op_manager = std::sync::Arc::new(
            crate::node::OpManager::new(
                notification_tx,
                ops_ch_channel,
                &node_config,
                crate::tracing::DynamicRegister::new(vec![]),
                connection_manager,
                result_router_tx,
                &task_monitor,
            )
            .expect("build OpManager"),
        );
        op_manager.ring.attach_op_manager(&op_manager);

        // The mock contract handler: serve `delta_bytes` for every
        // GetDeltaQuery, exactly as a real handler would after running the
        // contract's `get_state_delta`.
        let queries_served = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = queries_served.clone();
        let responder = tokio::spawn(async move {
            while let Ok((id, ev, _priority)) = ch_channel.recv_from_sender().await {
                if let ContractHandlerEvent::GetDeltaQuery { key, .. } = ev {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let sent = ch_channel
                        .send_to_sender(
                            id,
                            ContractHandlerEvent::GetDeltaResponse {
                                key,
                                delta: Ok(StateDelta::from(delta_bytes.clone())),
                            },
                        )
                        .await;
                    if sent.is_err() {
                        // The querying side dropped (test teardown) — stop.
                        break;
                    }
                }
            }
        });

        let guards: Box<dyn std::any::Any> = Box::new((
            notification_rx,
            wait_for_event,
            result_router_rx,
            task_monitor,
            responder,
        ));
        (op_manager, queries_served, guards)
    }

    /// THE incident pin (#4923): a peer whose cached summary is large (here
    /// state-sized, so the removed pre-compute gate would refuse outright:
    /// `1000 * 2 >= 1000`) must no longer force a full-state fallback when
    /// the contract's ACTUAL delta is small. Pre-fix this returned
    /// `Err(NotEfficient)` without running any contract code, and the caller
    /// (`broadcast_to_single_peer`) shipped the entire state — 41% of all
    /// network wire bytes in production. Post-fix the delta is computed and
    /// returned.
    #[tokio::test(flavor = "current_thread")]
    async fn oversized_peer_summary_no_longer_forces_full_state_when_delta_is_small() {
        let small_delta = vec![42u8, 43, 44]; // 3 bytes vs a 1000-byte state
        let (op_manager, queries_served, _guards) =
            op_manager_with_mock_delta_handler("post_gate_incident_pin", small_delta.clone()).await;

        let key = make_contract_key(101);
        let our_state_size = 1000usize;
        // State-sized peer summary: the exact shape production saw for the
        // hot contract (their_summary.len() * 2 >= our_state_size).
        let their_summary = StateSummary::from(vec![7u8; 1000]);
        let our_summary = StateSummary::from(vec![1u8, 2, 3]);

        let result = op_manager
            .interest_manager
            .compute_delta(
                &op_manager,
                &key,
                &their_summary,
                &our_summary,
                our_state_size,
            )
            .await;

        let delta = result
            .expect(
                "an oversized peer summary must no longer refuse the delta \
                 pre-compute — the fallback (full state) is never smaller than \
                 the delta being declined (#4923)",
            )
            .expect("the contract returned a non-empty delta");
        assert_eq!(
            delta.as_ref(),
            small_delta.as_slice(),
            "the computed small delta must be handed back verbatim"
        );
        assert_eq!(
            queries_served.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the contract handler must have been consulted exactly once"
        );
        // The result is memoized in the shared delta cache.
        assert!(
            op_manager
                .interest_manager
                .get_cached_delta(&key, their_summary.as_ref(), our_summary.as_ref())
                .is_some(),
            "the computed delta must be cached for subsequent fan-out targets"
        );
    }

    /// The post-compute gate: a delta that comes back NOT smaller than our
    /// full state still yields `NotEfficient` — so the caller's full-state
    /// fallback is taken exactly when it is genuinely optimal. Also pins the
    /// deliberate cache interaction: the oversized delta IS cached (so
    /// `cached_staleness_verdict` still reports genuine divergence and no
    /// WASM re-runs), and a second `compute_delta` call answers from the
    /// cache — same refusal, zero additional contract queries.
    #[tokio::test(flavor = "current_thread")]
    async fn oversized_computed_delta_returns_not_efficient() {
        // Must exceed the state by more than MIN_FULL_STATE_SAVING_BYTES for
        // the switch to full state to be worth making.
        let oversized_delta = vec![9u8; 4 + MIN_FULL_STATE_SAVING_BYTES + 1];
        let (op_manager, queries_served, _guards) =
            op_manager_with_mock_delta_handler("post_gate_oversized_delta", oversized_delta).await;

        let key = make_contract_key(102);
        let our_state_size = 4usize;
        // Small peer summary: the OLD pre-compute gate would have let this
        // through (1 * 2 < 4), so this failure mode is reachable only via the
        // post-compute check.
        let their_summary = StateSummary::from(vec![5u8]);
        let our_summary = StateSummary::from(vec![6u8, 6, 6]);

        for pass in 1..=2u32 {
            let result = op_manager
                .interest_manager
                .compute_delta(
                    &op_manager,
                    &key,
                    &their_summary,
                    &our_summary,
                    our_state_size,
                )
                .await;
            assert_eq!(
                result,
                Err(DeltaUnavailable::NotEfficient {
                    summary_size: their_summary.as_ref().len(),
                    state_size: our_state_size,
                }),
                "pass {pass}: a computed delta >= full state must refuse with \
                 NotEfficient so the caller's full-state fallback is optimal"
            );
        }
        assert_eq!(
            queries_served.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the second call must be served from the delta cache (memoized \
             refusal), not a second WASM run"
        );
        // The oversized delta is cached ON PURPOSE: it is still a genuine
        // divergence, so the staleness machinery must keep reporting "peer is
        // stale" (the fan-out then heals with full state).
        assert_eq!(
            op_manager.interest_manager.cached_staleness_verdict(
                &key,
                their_summary.as_ref(),
                our_summary.as_ref()
            ),
            Some(true),
            "an oversized (non-empty) cached delta must still read as genuine \
             divergence for the staleness verdict"
        );
    }

    /// Boundary pin for the switch margin. A delta merely EQUAL to (or a few
    /// bytes larger than) our state must still be SHIPPED: flipping to full
    /// state there buys nothing and re-creates the #4233 full-state fan-out
    /// shape for every small contract. Only a delta that clears
    /// `state + MIN_FULL_STATE_SAVING_BYTES` refuses.
    ///
    /// The 144-vs-136 case is the real one observed in
    /// `test_summary_first_put_holder_found_ships_delta`: a bare `>=`
    /// comparison abandoned the delta to save 8 bytes and broke both the
    /// summary-first PUT reverse leg and the storm pin.
    #[tokio::test(flavor = "current_thread")]
    async fn delta_slightly_larger_than_state_is_still_shipped() {
        let our_state_size = 136usize;
        let slightly_larger = vec![3u8; 144]; // the observed real-world pair
        let (op_manager, _queries_served, _guards) =
            op_manager_with_mock_delta_handler("post_gate_margin_delta", slightly_larger.clone())
                .await;

        let key = make_contract_key(103);
        let their_summary = StateSummary::from(vec![4u8]);
        let our_summary = StateSummary::from(vec![5u8, 5]);

        let delta = op_manager
            .interest_manager
            .compute_delta(
                &op_manager,
                &key,
                &their_summary,
                &our_summary,
                our_state_size,
            )
            .await
            .expect(
                "a delta only 8 bytes larger than the state must NOT be \
                 refused — switching to full state to save 8 bytes is the \
                 #4233 full-state fan-out shape",
            )
            .expect("the contract returned a non-empty delta");
        assert_eq!(delta.as_ref(), slightly_larger.as_slice());
    }

    /// The exact refusal threshold: `state + MIN_FULL_STATE_SAVING_BYTES` is
    /// the first size that loses. One byte under it must still ship, so an
    /// off-by-one in the margin comparison is caught in both directions.
    #[tokio::test(flavor = "current_thread")]
    async fn delta_at_margin_threshold_refuses_but_one_byte_under_ships() {
        let our_state_size = 100usize;
        let key = make_contract_key(104);
        let their_summary = StateSummary::from(vec![4u8]);
        let our_summary = StateSummary::from(vec![5u8, 5]);

        // One byte UNDER the threshold: still shipped.
        let under = vec![1u8; 100 + MIN_FULL_STATE_SAVING_BYTES - 1];
        let (op_under, _q, _g) =
            op_manager_with_mock_delta_handler("post_gate_margin_under", under).await;
        assert!(
            op_under
                .interest_manager
                .compute_delta(
                    &op_under,
                    &key,
                    &their_summary,
                    &our_summary,
                    our_state_size
                )
                .await
                .is_ok(),
            "one byte under the switch margin must still ship the delta"
        );

        // Exactly AT the threshold: refused.
        let at = vec![1u8; 100 + MIN_FULL_STATE_SAVING_BYTES];
        let (op_at, _q2, _g2) = op_manager_with_mock_delta_handler("post_gate_margin_at", at).await;
        assert_eq!(
            op_at
                .interest_manager
                .compute_delta(&op_at, &key, &their_summary, &our_summary, our_state_size)
                .await,
            Err(DeltaUnavailable::NotEfficient {
                summary_size: their_summary.as_ref().len(),
                state_size: our_state_size,
            }),
            "a delta at state + MIN_FULL_STATE_SAVING_BYTES must refuse"
        );
    }

    /// Converged-peer companion to the incident pin: with the SAME oversized
    /// peer summary the pre-compute gate used to refuse before the contract
    /// could report an EMPTY delta, so a logically-converged peer was
    /// re-flooded with full state. Post-#4923 the empty delta is seen and
    /// `Ok(None)` lets the caller skip the send entirely.
    #[tokio::test(flavor = "current_thread")]
    async fn oversized_peer_summary_with_empty_delta_reports_converged() {
        let (op_manager, queries_served, _guards) =
            op_manager_with_mock_delta_handler("post_gate_empty_delta", Vec::new()).await;

        let key = make_contract_key(103);
        let their_summary = StateSummary::from(vec![8u8; 1000]); // state-sized
        let our_summary = StateSummary::from(vec![4u8, 2]);

        let result = op_manager
            .interest_manager
            .compute_delta(&op_manager, &key, &their_summary, &our_summary, 1000)
            .await;
        assert_eq!(
            result,
            Ok(None),
            "an empty delta behind an oversized peer summary must report \
             converged (skip), not NotEfficient (full-state re-flood)"
        );
        assert_eq!(
            queries_served.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the contract must have been consulted for the verdict"
        );
    }

    // ===== #5155: bounded, rotating full-bytes summary fallback =====

    /// One round of the rotation over `sorted`, resuming after `cursor`.
    /// Returns the ids covered and the new cursor.
    ///
    /// `cursor: None` starts at index 0 here rather than at the random offset
    /// production uses at a cycle boundary. That is deliberate: these tests
    /// pin the WITHIN-cycle contiguity and coverage properties, which must hold
    /// from any starting offset, so the tests below sweep the starts explicitly
    /// instead of sampling them. `fallback_window_start_randomises_the_cycle_
    /// boundary` covers the production entry point.
    fn rotation_round(
        sorted: &[ContractKey],
        cursor: Option<ContractInstanceId>,
        limit: usize,
    ) -> (Vec<ContractInstanceId>, Option<ContractInstanceId>) {
        let start = match cursor {
            Some(ref after) => first_index_after(sorted, after),
            None => 0,
        };
        let sent: Vec<ContractInstanceId> = rotation_window_indices(sorted.len(), start, limit)
            .into_iter()
            .map(|i| *sorted[i].id())
            .collect();
        let next = sent.last().copied();
        (sent, next)
    }

    /// `sorted` in the order `get_matching_contracts` produces (ascending by
    /// contract id), which the rotation's binary search depends on.
    fn sorted_keys(seeds: impl IntoIterator<Item = u32>) -> Vec<ContractKey> {
        let mut keys: Vec<ContractKey> = seeds.into_iter().map(make_unique_contract_key).collect();
        keys.sort_by(|a, b| a.id().as_bytes().cmp(b.id().as_bytes()));
        keys
    }

    /// The window never exceeds the limit, never exceeds the set, and wraps
    /// past the end rather than returning a short tail.
    ///
    /// The wrap is the part worth pinning: a window that truncated at the end
    /// of the set would give the tail a smaller share of every cycle than the
    /// head, so the "covered in ceil(n/k) rounds" claim below would be false
    /// for exactly the contracts furthest from the cursor's starting point.
    #[test]
    fn rotation_window_is_bounded_and_wraps() {
        assert!(rotation_window_indices(0, 0, 64).is_empty(), "empty set");
        assert!(rotation_window_indices(10, 0, 0).is_empty(), "zero limit");

        // Limit binds.
        assert_eq!(rotation_window_indices(10, 0, 4), vec![0, 1, 2, 3]);
        // Set size binds.
        assert_eq!(rotation_window_indices(3, 0, 64), vec![0, 1, 2]);
        // Wraps rather than truncating.
        assert_eq!(rotation_window_indices(10, 8, 4), vec![8, 9, 0, 1]);
        // A start past the end (every id below the cursor) restarts at 0
        // instead of producing nothing, which would stall the rotation.
        assert_eq!(rotation_window_indices(4, 4, 2), vec![0, 1]);
        assert_eq!(rotation_window_indices(4, 99, 2), vec![0, 1]);

        for len in [1usize, 2, 7, 64, 65, 266, 2448] {
            for start in [0usize, 1, len / 2, len - 1] {
                let w = rotation_window_indices(len, start, 64);
                assert!(w.len() <= 64, "window exceeded the limit at len={len}");
                assert!(w.len() <= len, "window exceeded the set at len={len}");
                assert!(
                    w.iter().collect::<HashSet<_>>().len() == w.len(),
                    "window repeated an index at len={len} start={start}"
                );
            }
        }
    }

    /// A stable shared set is fully covered within `ceil(n / limit)` rounds.
    ///
    /// This is the convergence claim the bound rests on: bounding the reply
    /// only trades detection LATENCY, never detection itself. If this fails,
    /// some contract is never advertised to that peer at all, which is
    /// permanent silent divergence rather than a slower safety net.
    /// Coverage must hold from ANY starting offset, not just from index 0, and
    /// for set sizes that do not divide evenly by the limit.
    ///
    /// Both conditions matter. Production starts each cycle at a random offset,
    /// so a coverage property that only holds from 0 would not describe it. And
    /// a set size divisible by the limit never produces a short final round,
    /// which is exactly the case where a window that truncated at the end of
    /// the set instead of wrapping would still look correct.
    #[test]
    fn rotation_covers_every_contract_within_ceil_n_over_limit_rounds() {
        const LIMIT: usize = 64;
        for n in [1usize, 5, 63, 64, 65, 100, 128, 266, 2448] {
            let sorted = sorted_keys(0..n as u32);
            let expected: HashSet<ContractInstanceId> =
                sorted.iter().map(|k| *k.id()).collect::<HashSet<_>>();
            let rounds = n.div_ceil(LIMIT);

            // Sweep the starting offsets a random cycle boundary could pick.
            for first in [0usize, 1, n / 3, n / 2, n - 1] {
                let mut covered: HashSet<ContractInstanceId> = HashSet::new();
                // Seed the cursor so round one begins at `first`.
                let mut cursor = if first == 0 {
                    None
                } else {
                    Some(*sorted[first - 1].id())
                };
                for _ in 0..rounds {
                    let (sent, next) = rotation_round(&sorted, cursor, LIMIT);
                    assert!(
                        sent.len() <= LIMIT,
                        "a round exceeded the entry bound at n={n}"
                    );
                    covered.extend(sent);
                    cursor = next;
                }
                assert_eq!(
                    covered, expected,
                    "n={n} starting at {first} was not fully covered in \
                     {rounds} rounds (ceil({n}/{LIMIT}))"
                );
            }
        }
    }

    /// Contracts removed mid-cycle must not cost the rotation its coverage
    /// BOUND: every contract still shared after `ceil(n / limit)` rounds must
    /// have been advertised within those rounds.
    ///
    /// This is the property that justifies bounding the reply at all. The
    /// safety argument is "a divergence goes unnoticed for at most one cycle",
    /// and a cursor that loses entries under churn downgrades that to "at some
    /// point", which is not a bound and not what the change was reviewed on.
    ///
    /// The second half runs the identical removal schedule with an INDEX
    /// cursor. It is not permanently starved — wrapping revisits everything
    /// eventually — but it does miss contracts inside the cycle, which is what
    /// makes the assertion above discriminating rather than true of any cursor.
    #[test]
    fn rotation_does_not_lose_the_coverage_bound_when_contracts_are_removed() {
        const LIMIT: usize = 4;
        const N: usize = 12;
        let rounds = N.div_ceil(LIMIT);
        let full = sorted_keys(0..N as u32);

        // Key-based cursor.
        let mut sorted = full.clone();
        let mut covered: HashSet<ContractInstanceId> = HashSet::new();
        let mut cursor = None;
        for _ in 0..rounds {
            let (sent, next) = rotation_round(&sorted, cursor, LIMIT);
            covered.extend(sent);
            cursor = next;
            // Drop the lowest-sorting contract, i.e. one at or below the
            // cursor — the direction that shifts positions under a cursor
            // that stored one.
            if !sorted.is_empty() {
                sorted.remove(0);
            }
        }
        let survivors: HashSet<ContractInstanceId> = sorted.iter().map(|k| *k.id()).collect();
        let missed: Vec<_> = survivors.difference(&covered).collect();
        assert!(
            missed.is_empty(),
            "key cursor left {} still-shared contract(s) unadvertised within \
             {rounds} rounds — the coverage bound the change is sold on",
            missed.len()
        );

        // The same schedule with an INDEX cursor, as the control.
        let mut sorted = full.clone();
        let mut idx_covered: HashSet<ContractInstanceId> = HashSet::new();
        let mut idx_cursor = 0usize;
        for _ in 0..rounds {
            let w = rotation_window_indices(sorted.len(), idx_cursor, LIMIT);
            for i in &w {
                idx_covered.insert(*sorted[*i].id());
            }
            // Resume at the position after the last one sent. Nothing records
            // WHICH contract that was, which is the whole defect.
            idx_cursor = w.last().map_or(0, |i| i + 1);
            if !sorted.is_empty() {
                sorted.remove(0);
            }
        }
        let idx_survivors: HashSet<ContractInstanceId> = sorted.iter().map(|k| *k.id()).collect();
        assert!(
            !idx_survivors.is_subset(&idx_covered),
            "premise of this test: under this removal schedule an index cursor \
             is supposed to miss a contract inside the cycle. If it no longer \
             does, the scenario stopped discriminating between the two designs \
             and the assertion above proves nothing about the key cursor."
        );
    }

    /// A contract added mid-cycle is picked up rather than waiting behind a
    /// cursor that has already passed its position.
    ///
    /// Insertion ABOVE the cursor lands in the very next window; insertion
    /// BELOW it is covered when the rotation wraps. Both are bounded, which is
    /// the property that matters — neither is skipped indefinitely.
    #[test]
    fn rotation_picks_up_contracts_added_mid_cycle() {
        const LIMIT: usize = 4;
        let mut sorted = sorted_keys(0..12);
        let mut cursor = None;
        // Two rounds in, so the cursor sits in the middle of the set.
        for _ in 0..2 {
            let (_, next) = rotation_round(&sorted, cursor, LIMIT);
            cursor = next;
        }

        // Add contracts on both sides of the cursor.
        sorted = sorted_keys((0..12).chain(100..112));
        let added: HashSet<ContractInstanceId> = sorted_keys(100..112)
            .iter()
            .map(|k| *k.id())
            .collect::<HashSet<_>>();

        let mut covered: HashSet<ContractInstanceId> = HashSet::new();
        let rounds = sorted.len().div_ceil(LIMIT);
        for _ in 0..rounds {
            let (sent, next) = rotation_round(&sorted, cursor, LIMIT);
            covered.extend(sent);
            cursor = next;
        }
        let missed: Vec<_> = added.difference(&covered).collect();
        assert!(
            missed.is_empty(),
            "{} newly added contract(s) were not covered within one full cycle",
            missed.len()
        );
    }

    /// `first_index_after` resumes correctly when the cursor's own contract is
    /// the one that was removed — the cursor is compared as bytes, not looked
    /// up, so a missing id still orders against the rest.
    #[test]
    fn first_index_after_handles_a_removed_cursor() {
        let sorted = sorted_keys(0..8);
        let cursor = *sorted[3].id();

        assert_eq!(
            first_index_after(&sorted, &cursor),
            4,
            "with the cursor present, resume at the next contract"
        );

        let mut without = sorted.clone();
        without.remove(3);
        let resumed = first_index_after(&without, &cursor);
        assert_eq!(
            *without[resumed].id(),
            *sorted[4].id(),
            "with the cursor's own contract gone, resume at the SAME successor \
             rather than stepping over it"
        );

        // Cursor at the end: signals a completed cycle to the caller, which
        // restarts at a random offset rather than at 0.
        let beyond = *sorted[7].id();
        assert_eq!(first_index_after(&sorted, &beyond), sorted.len());
    }

    /// Mid-cycle the stored cursor round-trips and resumes deterministically,
    /// and cursors do not leak between peers.
    #[test]
    fn fallback_cursor_round_trips_and_resumes_mid_cycle() {
        let (mgr, _clock) = make_manager();
        let peer: SocketAddr = "127.0.0.1:9100".parse().unwrap();
        let sorted = sorted_keys(0..8);

        assert_eq!(mgr.peek_fallback_cursor(peer), None);

        mgr.record_fallback_cursor(peer, *sorted[2].id());
        assert_eq!(mgr.peek_fallback_cursor(peer), Some(*sorted[2].id()));
        assert_eq!(
            mgr.fallback_window_start(peer, &sorted),
            3,
            "mid-cycle the resume point must be exactly after the last id sent"
        );

        // Cursors are per peer: one peer's progress must not advance another's.
        let other: SocketAddr = "127.0.0.1:9101".parse().unwrap();
        mgr.record_fallback_cursor(other, *sorted[6].id());
        assert_eq!(mgr.fallback_window_start(peer, &sorted), 3);
        assert_eq!(mgr.fallback_window_start(other, &sorted), 7);

        // An empty shared set has no valid offset; it must not panic or draw.
        assert_eq!(mgr.fallback_window_start(peer, &[]), 0);
    }

    /// At a CYCLE BOUNDARY the start is random, not a fixed 0.
    ///
    /// A boundary is reached with no cursor (first reply, our own restart, LRU
    /// eviction, a peer reconnecting on a new port) or with a cursor already at
    /// the highest id. Restarting at 0 every time would re-send the head of the
    /// set and starve the tail for any peer that keeps returning to a boundary,
    /// which is the failure `emit_stale_peer_syncs` and the `SummaryDigests`
    /// arm both already rotate to avoid.
    ///
    /// The peer influences this: `sorted` is the intersection with the hash
    /// list it advertised, so advertising one high-id contract parks the cursor
    /// at the end. With a fixed restart that alternation pins the window to the
    /// head forever; with a random one it cannot.
    #[test]
    fn fallback_window_start_randomises_the_cycle_boundary() {
        let (mgr, _clock) = make_manager();
        let sorted = sorted_keys(0..64);

        // No cursor: many draws, all in range, and not all the same value.
        let mut seen = HashSet::new();
        for i in 0..40u32 {
            let peer: SocketAddr = format!("127.0.0.1:{}", 9200 + i).parse().unwrap();
            let start = mgr.fallback_window_start(peer, &sorted);
            assert!(start < sorted.len(), "start {start} out of range");
            seen.insert(start);
        }
        assert!(
            seen.len() > 1,
            "a missing cursor must not always restart at the same offset — 40 \
             draws over 64 contracts all landed on {seen:?}"
        );

        // Cursor at the highest id: also a boundary, also randomised.
        let peer: SocketAddr = "127.0.0.1:9300".parse().unwrap();
        let mut seen_wrapped = HashSet::new();
        for _ in 0..40 {
            mgr.record_fallback_cursor(peer, *sorted[sorted.len() - 1].id());
            seen_wrapped.insert(mgr.fallback_window_start(peer, &sorted));
        }
        assert!(
            seen_wrapped.iter().all(|s| *s < sorted.len()),
            "wrapped start out of range"
        );
        assert!(
            seen_wrapped.len() > 1,
            "a cursor at the end of the set must restart at a random offset, \
             not deterministically at 0 — got {seen_wrapped:?}"
        );
    }

    /// Source pin: the cycle-boundary offset must come from `GlobalRng`.
    ///
    /// Two things ride on this and neither is visible in the behavioural test
    /// above: a non-random restart re-opens the tail-starvation failure, and an
    /// offset drawn from anything other than `GlobalRng` is not reproducible
    /// under the deterministic simulation harness, which would make any
    /// convergence simulation covering this path silently non-deterministic.
    #[test]
    fn fallback_window_start_draws_its_offset_from_global_rng() {
        let src = include_str!("interest.rs");
        let at = src
            .find("pub(crate) fn fallback_window_start(")
            .expect("fallback_window_start not found");
        let body_end = at + src[at..].find("\n    }\n").expect("body end not found");
        let body = &src[at..body_end];
        assert!(
            body.contains("GlobalRng::random_range"),
            "the cycle-boundary offset is no longer drawn from GlobalRng — a \
             fixed restart starves the tail of the set for any peer that keeps \
             returning to a boundary, and a non-GlobalRng source breaks \
             simulation determinism"
        );
    }
}
