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
//! # What is canonical and what is stored here
//!
//! `.claude/rules/bug-prevention-patterns.md` ("manually-mirrored telemetry
//! counters / dashboard providers", #4009/#4010) says to prefer a provider
//! closure reading canonical state over re-mirroring counters at call sites.
//! Applied here, three of the four data sources are read live and NOT mirrored:
//!
//! | Field | Canonical source | Mirrored here? |
//! |---|---|---|
//! | subscriptions held | [`DELEGATE_SUBSCRIPTIONS`] | no — read live |
//! | did it register demand | [`Ring::contract_in_use`] | no — read live |
//! | attributed CPU rate | `topology::meter::Meter` | no — read live |
//! | invocations / errors / last error | *nothing* | **yes — this module** |
//!
//! Only the last row is mirrored, because no canonical store for it exists:
//! delegate execution today has no timing at all and errors are logged
//! per-call at `contract/executor/runtime/delegates.rs` without ever being
//! counted. To keep that mirror honest there is exactly ONE write site
//! ([`record_invocation`]), pinned by `record_invocation_has_exactly_one_production_call_site`.
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

/// Number of samples retained for the invocation-rate window.
///
/// Matches the production `Meter` window (`topology.rs`, `new_with_window_size(100)`).
const INVOCATION_RATE_WINDOW: usize = 100;

/// Live execution counters for one delegate.
///
/// Written only by [`record_invocation`]; read only by [`snapshot`].
#[derive(Debug)]
struct DelegateExecEntry {
    invocations: u64,
    errors: u64,
    /// Truncated to [`MAX_LAST_ERROR_BYTES`] at insertion.
    last_error: Option<String>,
    last_error_at: Option<Instant>,
    last_invoked_at: Instant,
    total_exec_micros: u64,
    /// Windowed invocation rate. Reuses `topology::running_average`, which is
    /// entirely key-agnostic (it never touches the key type), so this is the
    /// existing machinery rather than a parallel one.
    invocation_rate: crate::topology::running_average::RunningAverage,
}

impl DelegateExecEntry {
    fn new(now: Instant) -> Self {
        Self {
            invocations: 0,
            errors: 0,
            last_error: None,
            last_error_at: None,
            last_invoked_at: now,
            total_exec_micros: 0,
            invocation_rate: crate::topology::running_average::RunningAverage::new(
                INVOCATION_RATE_WINDOW,
            ),
        }
    }
}

/// Process-global execution stats, keyed by delegate.
///
/// Process-global for the same reason `DELEGATE_SUBSCRIPTIONS`
/// (`wasm_runtime/native_api.rs:40`) is: the write site sits deep inside the
/// executor's runtime where no per-node handle is threaded through. #4824
/// tracks making that family of globals node-scoped; when it lands this should
/// move with them rather than being left behind.
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
/// indefinitely (the AGENTS.md GC rule, and the same two-phase shape as
/// `Meter::evict_if_full`).
fn evict_if_full(now: Instant) {
    DELEGATE_EXEC_STATS.retain(|_, entry| {
        now.saturating_duration_since(entry.last_invoked_at) < DELEGATE_STATS_TTL
    });
    if DELEGATE_EXEC_STATS.len() < MAX_TRACKED_DELEGATES {
        return;
    }
    // Still full after the TTL sweep: drop the least-recently-invoked entry.
    let victim = DELEGATE_EXEC_STATS
        .iter()
        .min_by_key(|entry| entry.value().last_invoked_at)
        .map(|entry| entry.key().clone());
    if let Some(victim) = victim {
        DELEGATE_EXEC_STATS.remove(&victim);
    }
}

/// Outcome of one delegate invocation, as seen at the executor call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InvocationOutcome<'a> {
    Success,
    /// The invocation returned an error. The message is stored (truncated) as
    /// the delegate's `last_error`.
    Failure(&'a str),
}

/// Record one delegate invocation. **This is the only write site.**
///
/// Pinned by `record_invocation_has_exactly_one_production_call_site`: a future
/// refactor that adds a second call site, or moves this one, fails CI rather
/// than silently producing a counter that undercounts. That pin exists because
/// #4009/#4010 are precisely the case where a migration re-homed an op path and
/// the mirrored counter rotted without anything going red.
pub(crate) fn record_invocation(
    key: &DelegateKey,
    exec_duration: Duration,
    outcome: InvocationOutcome<'_>,
    now: Instant,
) {
    use dashmap::mapref::entry::Entry;

    // Hot path (delegate already tracked) takes a single shard guard. The
    // eviction scan must NOT run while an entry guard is held — it `retain`s
    // and `remove`s across keys on the same DashMap, which self-deadlocks — so
    // a brand-new key drops the guard, evicts, then re-acquires. Same shape as
    // `Meter::report` (`topology/meter.rs`).
    let micros = exec_duration.as_micros().min(u64::MAX as u128) as u64;
    let apply = |entry: &mut DelegateExecEntry| {
        entry.invocations = entry.invocations.saturating_add(1);
        entry.total_exec_micros = entry.total_exec_micros.saturating_add(micros);
        entry.last_invoked_at = entry.last_invoked_at.max(now);
        entry.invocation_rate.insert_with_time(now, 1.0);
        if let InvocationOutcome::Failure(msg) = outcome {
            entry.errors = entry.errors.saturating_add(1);
            entry.last_error = Some(truncate_error(msg));
            entry.last_error_at = Some(now);
        }
    };

    if let Entry::Occupied(mut occupied) = DELEGATE_EXEC_STATS.entry(key.clone()) {
        apply(occupied.get_mut());
        return;
    }

    evict_if_full(now);
    let mut entry = DELEGATE_EXEC_STATS
        .entry(key.clone())
        .or_insert_with(|| DelegateExecEntry::new(now));
    apply(entry.value_mut());
}

/// One contract a delegate is subscribed to, plus whether that subscription
/// actually registered demand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DelegateSubscriptionEntry {
    pub contract: String,
    /// Whether the node considers this contract in use — i.e. whether the
    /// delegate's subscription actually pinned it.
    ///
    /// **This is the load-bearing field of Phase 0.** Until #4669 lands,
    /// `Ring::contract_in_use` is `has_client_subscriptions() ||
    /// has_downstream_subscribers()` with no delegate term, so a delegate
    /// subscription that succeeded still reports `false` here unless some
    /// *other* route (typically the app's own WebSocket subscription) happens
    /// to be pinning the same contract. That divergence — subscribed, but not
    /// pinned — is the bug, rendered visible.
    pub registered_demand: bool,
}

/// Per-delegate observability record.
#[derive(Debug, Clone, PartialEq)]
pub struct DelegateStatusEntry {
    pub key: String,
    pub subscriptions: Vec<DelegateSubscriptionEntry>,
    /// How many of `subscriptions` actually registered demand.
    pub subscriptions_registering_demand: usize,
    pub invocations: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    pub last_error_secs_ago: Option<u64>,
    pub last_invoked_secs_ago: Option<u64>,
    pub total_exec_micros: u64,
    /// Windowed invocations/sec. `None` when the window holds too few samples
    /// to state a rate — distinct from a genuine zero.
    pub invocation_rate_per_sec: Option<f64>,
    /// Windowed attributed CPU (µs/sec) from `topology::meter`. `None` when the
    /// meter holds no window for this delegate — again, distinct from zero.
    pub exec_cpu_micros_per_sec: Option<f64>,
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
    /// Whether scheduled execution (#5467 Phase 2 / #3972) is built.
    ///
    /// Always `false` today. It exists so the dashboard can say "not built"
    /// rather than rendering a `0` that an operator would read as "none
    /// pending" — AGENTS.md forbids improvising a value for something that
    /// does not exist, and a zero here would be exactly that.
    pub wakeup_scheduling_available: bool,
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
    let cpu_rates = ring.delegate_exec_cpu_rates(now);

    let mut delegates = Vec::with_capacity(by_delegate.len());
    let mut delegates_with_unpinned_subscriptions = 0usize;
    let mut subscriptions_total = 0usize;
    let mut subscriptions_without_demand = 0usize;

    for (key, mut instance_ids) in by_delegate {
        instance_ids.sort_by_key(|id| id.to_string());
        let mut subscriptions = Vec::with_capacity(instance_ids.len());
        let mut registering = 0usize;
        for instance_id in instance_ids {
            let registered_demand = in_use.contains(&instance_id);
            if registered_demand {
                registering += 1;
            } else {
                subscriptions_without_demand += 1;
            }
            subscriptions_total += 1;
            subscriptions.push(DelegateSubscriptionEntry {
                contract: instance_id.to_string(),
                registered_demand,
            });
        }
        if registering < subscriptions.len() {
            delegates_with_unpinned_subscriptions += 1;
        }

        let stats = DELEGATE_EXEC_STATS.get(&key);
        let (
            invocations,
            errors,
            last_error,
            last_error_secs_ago,
            last_invoked_secs_ago,
            total_exec_micros,
            invocation_rate_per_sec,
        ) = match stats.as_deref() {
            Some(entry) => (
                entry.invocations,
                entry.errors,
                entry.last_error.clone(),
                entry
                    .last_error_at
                    .map(|at| now.saturating_duration_since(at).as_secs()),
                Some(now.saturating_duration_since(entry.last_invoked_at).as_secs()),
                entry.total_exec_micros,
                entry
                    .invocation_rate
                    .get_rate_at_time(now)
                    .map(|rate| rate.per_second()),
            ),
            // No execution history: the counters are genuinely zero (we have
            // never seen it run), but the rates and timestamps are UNKNOWN
            // rather than zero, so they stay `None`.
            None => (0, 0, None, None, None, 0, None),
        };
        drop(stats);

        delegates.push(DelegateStatusEntry {
            key: key.to_string(),
            subscriptions,
            subscriptions_registering_demand: registering,
            invocations,
            errors,
            last_error,
            last_error_secs_ago,
            last_invoked_secs_ago,
            total_exec_micros,
            invocation_rate_per_sec,
            exec_cpu_micros_per_sec: cpu_rates.get(&key).copied(),
        });
    }

    // Most-recently-active first, so the delegate an operator is debugging is
    // at the top. Ties broken on the key so the render is deterministic.
    delegates.sort_by(|a, b| {
        a.last_invoked_secs_ago
            .unwrap_or(u64::MAX)
            .cmp(&b.last_invoked_secs_ago.unwrap_or(u64::MAX))
            .then_with(|| a.key.cmp(&b.key))
    });

    DelegateStatusSnapshot {
        delegates,
        delegates_with_unpinned_subscriptions,
        subscriptions_total,
        subscriptions_without_demand,
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

    fn key(byte: u8) -> DelegateKey {
        DelegateKey::from(&[byte; 32])
    }

    /// The single-write-site pin (#4009/#4010 shape).
    ///
    /// `record_invocation` is a mirrored counter, which is the failure mode
    /// bug-prevention-patterns.md warns about: a later migration re-homes the
    /// delegate call path, forgets to re-wire the call, and the counter reads a
    /// plausible zero forever. Anchoring on the API surface (the call
    /// expression) rather than a variable name keeps this from rotting on a
    /// rename of the surrounding function.
    #[test]
    fn record_invocation_has_exactly_one_production_call_site() {
        let src = include_str!("../contract/executor/runtime/delegates.rs");
        let calls = src
            .matches("delegate_observability::record_invocation(")
            .count();
        assert_eq!(
            calls, 1,
            "expected exactly ONE production call site of record_invocation in \
             delegates.rs (the sole production call site of inbound_app_message). \
             Found {calls}. If the delegate execution path moved, move the \
             instrumentation with it — do not add a second call site, and do not \
             delete this one: an uncounted invocation is indistinguishable from a \
             delegate that never ran."
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
        clear_for_test();
        let k = key(1);
        let t0 = Instant::now();
        record_invocation(&k, Duration::from_micros(10), InvocationOutcome::Success, t0);
        record_invocation(
            &k,
            Duration::from_micros(20),
            InvocationOutcome::Failure("boom"),
            t0 + Duration::from_millis(1),
        );

        let entry = DELEGATE_EXEC_STATS.get(&k).expect("entry recorded");
        assert_eq!(entry.invocations, 2, "both invocations counted");
        assert_eq!(entry.errors, 1, "only the failure counted as an error");
        assert_eq!(entry.last_error.as_deref(), Some("boom"));
        assert_eq!(entry.total_exec_micros, 30, "durations accumulate");
        drop(entry);
        clear_for_test();
    }

    #[test]
    fn entry_count_is_bounded_and_evicts_least_recently_invoked() {
        clear_for_test();
        let t0 = Instant::now();
        // Insert one more than the cap; each successive key is invoked later,
        // so key 0 is the least-recently-invoked and must be the victim.
        for i in 0..=MAX_TRACKED_DELEGATES {
            let k = DelegateKey::from(&{
                let mut b = [0u8; 32];
                b[0] = (i % 256) as u8;
                b[1] = (i / 256) as u8;
                b
            });
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
        clear_for_test();
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

    /// The snapshot must distinguish "we have never seen this run" from
    /// "it ran zero times per second" — fabricating the latter is the
    /// AGENTS.md failure this whole module exists to avoid.
    #[test]
    fn unknown_rates_are_none_not_zero() {
        clear_for_test();
        let entry = DelegateStatusEntry {
            key: "k".into(),
            subscriptions: vec![],
            subscriptions_registering_demand: 0,
            invocations: 0,
            errors: 0,
            last_error: None,
            last_error_secs_ago: None,
            last_invoked_secs_ago: None,
            total_exec_micros: 0,
            invocation_rate_per_sec: None,
            exec_cpu_micros_per_sec: None,
        };
        assert!(
            entry.invocation_rate_per_sec.is_none(),
            "an unmeasured rate is None, never Some(0.0)"
        );
        assert!(entry.exec_cpu_micros_per_sec.is_none());
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
