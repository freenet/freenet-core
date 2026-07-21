use std::collections::BTreeMap;
use std::sync::RwLock;
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use freenet_stdlib::prelude::*;

use crate::ring::PeerKeyLocation;
use crate::topology::rate::Rate;

use super::running_average::RunningAverage;

// Default usage is assumed to be the 50th percentile of usage for the resource.
const DEFAULT_USAGE_PERCENTILE: f64 = 0.5;

// Recache the estimated usage rate this often
const ESTIMATED_USAGE_RATE_CACHE_TIME: Duration = Duration::from_secs(60);

/// Hard ceiling on the number of distinct `AttributionSource` entries the
/// meter will retain at once.
///
/// `attribution_meters` is keyed by data that external actors influence
/// (peers we exchanged bytes with, contracts/delegates whose work we
/// executed). Per `.claude/rules/code-style.md` "NEVER use unbounded
/// per-key collections for data that external actors can influence" this
/// map must be size-bounded at insertion time. Peer churn is already
/// pruned by `retain_peer_sources`, but Contract/Delegate sources are
/// deliberately retained across that prune and would otherwise accumulate
/// one entry per distinct contract/delegate ever reported. The cap is the
/// backstop that bounds the worst case regardless of source variant.
///
/// Sized generously relative to a node's realistic concurrent-source
/// working set (live peers plus actively-executing contracts/delegates)
/// so legitimate traffic is never evicted, while still capping the map at
/// a few MB of running-average state.
///
/// Shared with [`crate::topology::TopologyManager::source_creation_times`],
/// which is keyed by the same `AttributionSource` and shares this meter's
/// lifecycle, so both bounded maps use one ceiling.
pub(crate) const MAX_ATTRIBUTION_SOURCES: usize = 4096;

/// Absolute age after which an attribution entry is eligible for eviction
/// regardless of the live-peer prune.
///
/// Per the AGENTS.md GC rule ("cleanup exemptions MUST be time-bounded"),
/// every entry carries an absolute-age threshold: an entry that has not
/// been reported against for longer than this is stale and can be dropped
/// on the next insertion that needs room. This is the TTL that keeps
/// Contract/Delegate sources — which `retain_peer_sources` intentionally
/// never prunes — from living forever after their last sample.
///
/// Shared with [`crate::topology::TopologyManager::source_creation_times`]
/// (same keyspace, same lifecycle) so both bounded maps age entries out on
/// the same schedule.
pub(crate) const ATTRIBUTION_SOURCE_TTL: Duration = Duration::from_secs(15 * 60);

/// A structure that keeps track of the usage of dynamic resources which are consumed over time.
/// It provides methods to report and query resource usage, both total and attributed to specific
/// sources.
pub(crate) struct Meter {
    attribution_meters: AttributionMeters,
    running_average_window_size: usize,
    cached_estimated_usage_rate: RwLock<BTreeMap<ResourceType, (Rate, Instant)>>,
}

impl Meter {
    /// Creates a new `Meter`.
    pub fn new_with_window_size(running_average_window_size: usize) -> Self {
        Meter {
            attribution_meters: DashMap::new(),
            running_average_window_size,
            cached_estimated_usage_rate: RwLock::new(BTreeMap::new()),
        }
    }

    /// The measured usage rate for a resource attributed to a specific source.
    pub(crate) fn attributed_usage_rate(
        &self,
        attribution: &AttributionSource,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        match self.attribution_meters.get(attribution) {
            Some(attribution_meters) => {
                match attribution_meters.map.get(resource) {
                    Some(meter) => {
                        // Get the current measurement value
                        meter.get_rate_at_time(at_time)
                    }
                    None => Some(Rate::new(0.0, Duration::from_secs(1))), // No meter found for the given resource
                }
            }
            None => None, // No AttributionMeters found for the given attribution
        }
    }

    /// Returns the estimated usage rate for a resource of a given type.
    ///
    /// This function uses a percentile defined by `DEFAULT_USAGE_PERCENTILE` to estimate the usage rate
    /// for resources with unknown usage. It caches the estimated rates and refreshes them every
    /// `ESTIMATED_USAGE_RATE_CACHE_TIME` duration to avoid frequent recalculations.
    ///
    /// # Arguments
    ///
    /// * `resource` - A reference to the type of resource for which the usage rate is estimated.
    /// * `now` - The current `Instant` used to determine if the cached value is still valid.
    ///
    /// # Returns
    ///
    /// An `Option<Rate>` which is `Some(rate)` if an estimated rate is available, or `None` if it can't be determined.
    pub(crate) fn get_adjusted_usage_rate(
        &mut self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        {
            let cache = self.cached_estimated_usage_rate.read().unwrap();
            if let Some((cached_rate, cached_time)) = cache.get(resource) {
                if at_time - *cached_time <= ESTIMATED_USAGE_RATE_CACHE_TIME {
                    return Some(*cached_rate);
                }
            }
        }

        match self.calculate_estimated_usage_rate(resource, at_time) {
            Some(estimated_usage_rate) => {
                let mut cache = self.cached_estimated_usage_rate.write().unwrap();
                cache.insert(*resource, (estimated_usage_rate, at_time));
                Some(estimated_usage_rate)
            }
            None => None,
        }
    }

    /// Returns a BTreeMap of AttributionSource to Rate of the usage rate for
    /// each attribution source. This does not adjust the usage rate for sources
    /// that are ramping up.
    pub(crate) fn get_usage_rates(
        &self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> BTreeMap<AttributionSource, Rate> {
        let mut rates = BTreeMap::new();

        for entry in self.attribution_meters.iter() {
            if let Some(meter) = entry.value().map.get(resource) {
                if let Some(rate) = meter.get_rate_at_time(at_time) {
                    rates.insert(entry.key().clone(), rate);
                }
            }
        }

        rates
    }

    /// Per-CONTRACT attributed usage rates for one cost axis, plus their sum,
    /// for the cost-pressure eviction trigger (cost-aware eviction, #4861).
    ///
    /// Iterates the attribution meters, keeping only
    /// [`AttributionSource::Contract`] entries (peer/delegate bandwidth sources
    /// never participate in contract cost eviction), and reads each contract's
    /// rate via [`RunningAverage::windowed_rate`]: sparse samples are diluted
    /// over at least `min_window` (a lone burst cannot masquerade as a
    /// sustained storm), while a saturated sample buffer divides by its actual
    /// span so a sustained high-frequency storm's TRUE rate is representable
    /// (the count-truncation under-read that hid the #4861 profile).
    ///
    /// Returns `(total_rate, per_contract_rate)` in axis units per second.
    /// EVERY positive-rate contract counts toward `total_rate` (the share
    /// denominator), but the per-contract map — the eviction CANDIDACY input —
    /// contains only contracts whose reporting is SUSTAINED (first sample at
    /// least `min_window / 2` old). A contract absent from the map has zero
    /// attributed cost for candidacy purposes, so a single large fan-out
    /// burst (first report moments ago) can never make its contract a cost
    /// victim, no matter how big the burst.
    pub(crate) fn contract_cost_rates(
        &self,
        resource: &ResourceType,
        at_time: Instant,
        min_window: Duration,
    ) -> (f64, std::collections::HashMap<ContractInstanceId, f64>) {
        let sustained_min_age = min_window / 2;
        let mut per_contract = std::collections::HashMap::new();
        let mut total = 0.0_f64;
        for entry in self.attribution_meters.iter() {
            let AttributionSource::Contract(id) = entry.key() else {
                continue;
            };
            let Some(avg) = entry.value().map.get(resource) else {
                continue;
            };
            let Some(windowed) = avg.windowed_rate(at_time, min_window) else {
                continue;
            };
            let per_second = windowed.rate.per_second();
            if per_second > 0.0 {
                total += per_second;
                if windowed.first_sample_age >= sustained_min_age {
                    per_contract.insert(*id, per_second);
                }
            }
        }
        (total, per_contract)
    }

    /// Estimates the usage rate for a given resource type based on existing data.
    ///
    /// This function calculates the estimated usage rate by taking the 50th percentile value (or another
    /// specified percentile defined by [DEFAULT_USAGE_PERCENTILE]) from the set of known rates for the
    /// specified resource type. It disregards resources with no known rate (which may leader to
    /// higher estimates).
    ///
    /// # Arguments
    ///
    /// * `resource` - A reference to the resource type for which the usage rate is to be estimated.
    ///
    /// # Returns
    ///
    /// An `Option<Rate>` which is `Some(rate)` if an estimated rate can be determined from available data,
    /// or `None` if no data is available for the given resource type.
    ///
    /// # Panics
    ///
    /// This function may panic if `DEFAULT_USAGE_PERCENTILE` is set to an invalid value that is not within
    /// the range [0.0, 1.0].
    fn calculate_estimated_usage_rate(
        &self,
        resource: &ResourceType,
        at_time: Instant,
    ) -> Option<Rate> {
        let rates: Vec<Rate> = self
            .attribution_meters
            .iter()
            // Filter out resources with no Rate and collect their rates
            .filter_map(|t| {
                t.value()
                    .map
                    .get(resource)
                    .and_then(|m| m.get_rate_at_time(at_time))
            })
            .collect();

        if rates.is_empty() {
            return None;
        }

        // Sort the collected rates
        let mut sorted_rates = rates;
        sorted_rates.sort_unstable(); // Using sort_unstable for potentially better performance

        // Calculate the index for the estimated usage rate
        let percentile_index =
            (DEFAULT_USAGE_PERCENTILE * sorted_rates.len() as f64).round() as usize;
        let estimated_index = percentile_index.min(sorted_rates.len().saturating_sub(1));

        sorted_rates.get(estimated_index).cloned()
    }

    /// Drop the per-source meters for every `AttributionSource::Peer` whose
    /// inner `PeerKeyLocation` is NOT in `live`. Non-`Peer` sources (Contract,
    /// Delegate) are always retained — only the peer-attributed bandwidth
    /// samples are bounded by the live connection set.
    ///
    /// Without this, a peer that ever exchanged bytes leaves a permanent entry
    /// in `attribution_meters`, so under connection churn the map (and the
    /// per-tick work that iterates it) grows without bound. See #3453 review.
    pub(crate) fn retain_peer_sources(&self, live: &std::collections::HashSet<PeerKeyLocation>) {
        self.attribution_meters.retain(|source, _| match source {
            AttributionSource::Peer(peer) => live.contains(peer),
            // Non-peer sources are not bounded by the live connection set;
            // enumerated explicitly (not `_`) so a future AttributionSource
            // variant must consciously decide its retention policy here.
            AttributionSource::Delegate(_) | AttributionSource::Contract(_) => true,
        });
    }

    /// Report the use of a resource. This should be done in the lowest-level
    /// functions that consume the resource, taking an AttributionMeter
    /// as a parameter.
    ///
    /// Takes `&self`: the underlying [`DashMap`] provides per-shard interior
    /// mutability (per `.claude/rules/code-style.md` — DashMap over
    /// `RwLock<HashMap>`).
    ///
    /// NOTE: this method does not yet deliver concurrent reporting in
    /// production. The sole caller, `Ring::report_contract_resource_usage`,
    /// still holds the outer `RwLock<TopologyManager>` write guard across
    /// this call (see `ring.rs`), so reporters serialize on that coarse lock
    /// regardless of the DashMap's per-shard locking. The DashMap swap is
    /// groundwork; relieving that outer-lock contention requires decoupling
    /// the contract meter from `TopologyManager` (the TopologyMeter /
    /// GovernanceMeter split tracked in #4276) and is a separate change.
    pub(crate) fn report(
        &self,
        attribution: &AttributionSource,
        resource: ResourceType,
        value: f64,
        at_time: Instant,
    ) {
        // Hot path (existing source) is a SINGLE shard acquisition: take the
        // `entry()` once and, when the source already exists, record the
        // sample inline under that one guard. The eviction scan must NOT run
        // while an entry guard is held (it `retain`s/`remove`s across keys on
        // the same DashMap → self-deadlock), so for a brand-new source we
        // drop the guard, run the bounded eviction, then re-acquire to insert.
        //
        // Bounding on the new-source path enforces the cap at insertion time
        // (code-style.md: per-key collections influenced by external actors
        // must be size-bounded at insertion). Reporting against an existing
        // source never grows the map, so the scan is skipped on the hot path.
        use dashmap::mapref::entry::Entry;
        match self.attribution_meters.entry(attribution.clone()) {
            Entry::Occupied(mut occupied) => {
                let totals = occupied.get_mut();
                totals.last_reported = totals.last_reported.max(at_time);
                totals
                    .map
                    .entry(resource)
                    .or_insert_with(|| RunningAverage::new(self.running_average_window_size))
                    .insert_with_time(at_time, value);
                return;
            }
            // Drop the vacant guard without inserting; eviction needs an
            // unlocked map. We re-acquire the entry below after pruning.
            Entry::Vacant(_) => {}
        }

        self.evict_if_full(at_time);

        let mut totals = self
            .attribution_meters
            .entry(attribution.clone())
            .or_insert_with(|| ResourceTotals::new(at_time));
        totals.last_reported = totals.last_reported.max(at_time);
        totals
            .map
            .entry(resource)
            .or_insert_with(|| RunningAverage::new(self.running_average_window_size))
            .insert_with_time(at_time, value);
    }

    /// Make room for a new attribution source when the map is at capacity.
    ///
    /// Two-phase, both phases bounded by an absolute-age threshold so no
    /// entry can be exempted from eviction indefinitely (AGENTS.md GC rule):
    ///
    /// 1. Drop every entry whose last report is older than
    ///    [`ATTRIBUTION_SOURCE_TTL`]. This alone usually keeps the map well
    ///    under the cap for a healthy node.
    /// 2. If still at [`MAX_ATTRIBUTION_SOURCES`], evict the single
    ///    least-recently-reported entry (LRU) so the new source can be
    ///    inserted. Bounding by recency means a flood of new sources cannot
    ///    push the map past the cap.
    ///
    /// Note on the DashMap multi-key caveat (code-style.md): this is not an
    /// atomic read-modify-write across keys — TTL pruning and LRU selection
    /// only ever *remove* whole entries, and the subsequent insert in
    /// `report` is independent. No entry guard is held across the scan, so
    /// there is no self-deadlock risk.
    fn evict_if_full(&self, now: Instant) {
        // Phase 1: TTL prune.
        self.attribution_meters.retain(|_, totals| {
            now.saturating_duration_since(totals.last_reported) < ATTRIBUTION_SOURCE_TTL
        });

        if self.attribution_meters.len() < MAX_ATTRIBUTION_SOURCES {
            return;
        }

        // Phase 2: LRU eviction. Find the least-recently-reported key
        // without holding its guard across the removal.
        let oldest = self
            .attribution_meters
            .iter()
            .min_by_key(|entry| entry.value().last_reported)
            .map(|entry| entry.key().clone());
        if let Some(key) = oldest {
            self.attribution_meters.remove(&key);
        }
    }
}

/// What a resource sample is attributed to.
///
/// Peer and Delegate variants are the original cost-attribution targets.
/// Contract was added as part of contract-hardening: every WASM call,
/// state write, broadcast, and message decode that has a `ContractInstanceId`
/// in scope can attribute its cost both to the originating peer AND to
/// the contract, so the per-contract governance scoring can run on the
/// same meter infrastructure that previously only fed peer-side
/// load-shedding.
///
/// See `docs/design/contract-hardening.md` — "Shared governance module".
#[allow(dead_code)] // variants constructed incrementally as reporters are wired up
#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub(crate) enum AttributionSource {
    Peer(PeerKeyLocation),
    Delegate(DelegateKey),
    Contract(ContractInstanceId),
}

impl PartialOrd for AttributionSource {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl AttributionSource {
    /// Whether this source can plausibly contribute samples to the given
    /// resource type. Used to filter the `source_creation_times`
    /// iteration in `topology::extrapolated_usage` so a Contract source
    /// (which never produces bandwidth samples) doesn't get a phantom
    /// non-zero bandwidth rate synthesized for it during its 5-min
    /// ramp-up window — that synthesized rate would otherwise inflate
    /// the topology's perceived bandwidth usage and trigger spurious
    /// connection removals every time a contract is reported.
    pub(crate) fn contributes_to(&self, resource: &ResourceType) -> bool {
        use AttributionSource::*;
        use ResourceType::*;
        // Enumerate every (source, resource) pair explicitly so a future
        // ResourceType variant fails the match exhaustiveness check
        // instead of silently falling into a default. Codex re-reviewer
        // of PR #4260 flagged the `_ => false` wildcard as a foot-gun:
        // a new resource added without revisiting this predicate would
        // silently treat every source as non-contributing to it.
        match (self, resource) {
            // Peer sources produce the bandwidth samples that drive
            // topology load-shedding.
            (Peer(_), InboundBandwidthBytes) => true,
            (Peer(_), OutboundBandwidthBytes) => true,
            (Peer(_), ExecCpuMicros) => false,
            (Peer(_), ExecFuelUnits) => false,
            (Peer(_), StateBytesWritten) => false,
            (Peer(_), BroadcastFanoutCost) => false,
            (Peer(_), BroadcastMessagesSent) => false,
            // Delegate sources predate this PR; their existing usage
            // pattern is bandwidth-relevant for accounting purposes.
            (Delegate(_), InboundBandwidthBytes) => true,
            (Delegate(_), OutboundBandwidthBytes) => true,
            (Delegate(_), ExecCpuMicros) => false,
            (Delegate(_), ExecFuelUnits) => false,
            (Delegate(_), StateBytesWritten) => false,
            (Delegate(_), BroadcastFanoutCost) => false,
            (Delegate(_), BroadcastMessagesSent) => false,
            // Contract sources contribute the four contract-governance
            // resource types (CPU, fuel, state-bytes, fan-out cost) and
            // NEVER bandwidth — those are peer-attributed even when the
            // contract is the originator.
            (Contract(_), InboundBandwidthBytes) => false,
            (Contract(_), OutboundBandwidthBytes) => false,
            (Contract(_), ExecCpuMicros) => true,
            (Contract(_), ExecFuelUnits) => true,
            (Contract(_), StateBytesWritten) => true,
            (Contract(_), BroadcastFanoutCost) => true,
            (Contract(_), BroadcastMessagesSent) => true,
        }
    }
}

impl Ord for AttributionSource {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Variant discriminant defines the cross-variant ordering;
        // intra-variant comparisons use the inner key's natural ordering
        // (or its Debug formatting where the inner type doesn't implement
        // Ord — DelegateKey today, kept for cross-variant compat).
        fn rank(source: &AttributionSource) -> u8 {
            match source {
                AttributionSource::Peer(_) => 0,
                AttributionSource::Delegate(_) => 1,
                AttributionSource::Contract(_) => 2,
            }
        }
        match (self, other) {
            (AttributionSource::Peer(a), AttributionSource::Peer(b)) => a.cmp(b),
            (AttributionSource::Delegate(a), AttributionSource::Delegate(b)) => {
                // DelegateKey doesn't implement Ord; fall back to Debug.
                format!("{:?}", a).cmp(&format!("{:?}", b))
            }
            (AttributionSource::Contract(a), AttributionSource::Contract(b)) => a.cmp(b),
            (a, b) => rank(a).cmp(&rank(b)),
        }
    }
}

/// What kind of resource was consumed.
///
/// The first two variants (Inbound/OutboundBandwidthBytes) are the
/// peer-side cost dimensions used by `topology::adjust_topology` for
/// connection load-shedding.
///
/// The remaining variants were added for per-contract governance scoring:
/// CPU and fuel from WASM execution, on-disk state-write volume, and the
/// `Σ(subscriber × per-emit cost)` for state broadcast fan-out. Each is
/// reported alongside the corresponding `AttributionSource::Contract`
/// entry from the executor / runtime / broadcast pipeline.
#[derive(Eq, Hash, PartialEq, PartialOrd, Ord, Clone, Copy, Debug)]
pub(crate) enum ResourceType {
    InboundBandwidthBytes,
    OutboundBandwidthBytes,
    ExecCpuMicros,
    ExecFuelUnits,
    StateBytesWritten,
    BroadcastFanoutCost,
    /// Per-peer broadcast MESSAGES dispatched for a contract (count units,
    /// one per target per fan-out, one per targeted stale-peer heal). Added
    /// for cost-aware eviction (#4861): a tiny-payload contract fanning to
    /// many co-hosts at a high message RATE burns per-send overhead
    /// (syscall / encryption / queue work) that byte-denominated
    /// [`Self::BroadcastFanoutCost`] cannot see — 121-byte messages at storm
    /// frequency read as a negligible byte rate while dominating the node's
    /// real broadcast capacity. This axis counts sends, so N tiny sends
    /// register as N.
    BroadcastMessagesSent,
}

impl ResourceType {
    /// Resource types that participate in topology-side bandwidth
    /// capacity decisions (see `Limits::get` and
    /// `calculate_usage_proportion`). Non-bandwidth resources (CPU /
    /// fuel / state / fanout) are NOT included here: they are tracked
    /// by the meter for contract-governance purposes but have no
    /// rate-ceiling style limit configured.
    pub(crate) fn all() -> [ResourceType; 2] {
        [
            ResourceType::InboundBandwidthBytes,
            ResourceType::OutboundBandwidthBytes,
        ]
    }

    /// Every resource type the meter understands, including non-bandwidth
    /// resources added for contract governance.
    #[allow(dead_code)] // wired up incrementally by per-resource-type reporters
    pub(crate) fn all_tracked() -> [ResourceType; 7] {
        [
            ResourceType::InboundBandwidthBytes,
            ResourceType::OutboundBandwidthBytes,
            ResourceType::ExecCpuMicros,
            ResourceType::ExecFuelUnits,
            ResourceType::StateBytesWritten,
            ResourceType::BroadcastFanoutCost,
            ResourceType::BroadcastMessagesSent,
        ]
    }
}

type AttributionMeters = DashMap<AttributionSource, ResourceTotals>;

/// A structure that holds running averages of resource usage for different resource types.
struct ResourceTotals {
    pub map: BTreeMap<ResourceType, RunningAverage>,
    /// Most recent time this source was reported against. Drives the TTL +
    /// LRU eviction in [`Meter::evict_if_full`] so a source that stops
    /// producing samples eventually ages out of the bounded map.
    last_reported: Instant,
}

impl ResourceTotals {
    fn new(at_time: Instant) -> Self {
        ResourceTotals {
            map: BTreeMap::new(),
            last_reported: at_time,
        }
    }
}

// Tests
#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_meter() {
        let meter = Meter::new_with_window_size(100);

        assert!(
            meter
                .attributed_usage_rate(
                    &AttributionSource::Peer(PeerKeyLocation::random()),
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now(),
                )
                .is_none()
        );
        assert!(meter.attribution_meters.is_empty());
    }

    fn contract_source(byte: u8) -> AttributionSource {
        AttributionSource::Contract(ContractInstanceId::new([byte; 32]))
    }

    #[test]
    fn test_meter_attributed_usage() {
        let meter = Meter::new_with_window_size(100);

        // Test that the attributed usage is 0.0 for all resources
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        assert!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .is_none()
        );
        assert!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::OutboundBandwidthBytes,
                    Instant::now()
                )
                .is_none()
        );

        // Report some usage and test that the attributed usage is updated
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            100.0
        );
    }

    #[test]
    fn test_meter_report() -> anyhow::Result<()> {
        let meter = Meter::new_with_window_size(100);

        // Report some usage and test that the total and attributed usage are updated
        let attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            100.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            100.0
        );

        // Report more usage and test that the total and attributed usage are updated
        meter.report(
            &attribution,
            ResourceType::InboundBandwidthBytes,
            200.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            300.0
        );

        // Report usage for a different attribution and test that the total and attributed usage are updated
        let other_attribution = AttributionSource::Peer(PeerKeyLocation::random());
        meter.report(
            &other_attribution,
            ResourceType::InboundBandwidthBytes,
            150.0,
            Instant::now(),
        );
        assert_eq!(
            meter
                .attributed_usage_rate(
                    &other_attribution,
                    &ResourceType::InboundBandwidthBytes,
                    Instant::now()
                )
                .unwrap()
                .per_second(),
            150.0
        );
        Ok(())
    }

    /// `contract_cost_rates` (cost-aware eviction, #4861) aggregates ONLY
    /// Contract-attributed sources; sparse samples are amortized over the
    /// minimum window (a lone burst cannot masquerade as a sustained storm);
    /// a SATURATED sample buffer reads its true rate over its actual span
    /// (the count-truncation fix — without it a sustained high-frequency
    /// storm's rate is capped at samples×value/window and can hide under the
    /// floor); and the per-contract candidacy map admits only SUSTAINED
    /// sources (first sample at least half the window old) while every
    /// positive rate still counts toward the total.
    #[test]
    fn contract_cost_rates_aggregates_contract_sources_with_min_window() {
        let meter = Meter::new_with_window_size(100);
        let t0 = Instant::now();
        let min_window = Duration::from_secs(300);
        let now = t0 + Duration::from_secs(600);

        // Sustained sparse source: two 30_000µs samples over 10 minutes.
        meter.report(
            &contract_source(1),
            ResourceType::ExecCpuMicros,
            30_000.0,
            t0,
        );
        meter.report(
            &contract_source(1),
            ResourceType::ExecCpuMicros,
            30_000.0,
            t0 + Duration::from_secs(590),
        );
        // Burst source: one huge sample 1s before the read.
        meter.report(
            &contract_source(2),
            ResourceType::ExecCpuMicros,
            30_000_000.0,
            now - Duration::from_secs(1),
        );
        // Peer (bandwidth) source: must be excluded entirely.
        meter.report(
            &AttributionSource::Peer(PeerKeyLocation::random()),
            ResourceType::InboundBandwidthBytes,
            999_999.0,
            t0,
        );
        // Saturated high-frequency source on a different axis: 150 samples of
        // 58 messages at 1.6s cadence (storm profile). The ring keeps the last
        // 100, spanning ~158.4s.
        for i in 0..150u64 {
            meter.report(
                &contract_source(3),
                ResourceType::BroadcastMessagesSent,
                58.0,
                t0 + Duration::from_millis(1600 * i),
            );
        }
        let msgs_now = t0 + Duration::from_millis(1600 * 149) + Duration::from_secs(1);

        let (cpu_total, cpu_rates) =
            meter.contract_cost_rates(&ResourceType::ExecCpuMicros, now, min_window);
        let id1 = ContractInstanceId::new([1u8; 32]);
        let id2 = ContractInstanceId::new([2u8; 32]);
        // Sustained sparse source: 60_000µs over its 600s retained span.
        assert!((cpu_rates[&id1] - 60_000.0 / 600.0).abs() < 1e-6);
        // Burst source: counted in the TOTAL (diluted over min_window: 30M/300s
        // = 100_000/s) but EXCLUDED from the candidacy map (first sample 1s
        // old — not sustained), so one burst can never nominate a victim.
        assert!(
            !cpu_rates.contains_key(&id2),
            "burst must not be a candidate"
        );
        let expected_total = 60_000.0 / 600.0 + 30_000_000.0 / 300.0;
        assert!((cpu_total - expected_total).abs() < 1e-6);

        // Saturated storm source: true rate over the RETAINED span (~158.4s),
        // NOT diluted to 100×58/300s ≈ 19.3/s. 100×58/158.4 ≈ 36.6/s.
        let (msgs_total, msgs_rates) =
            meter.contract_cost_rates(&ResourceType::BroadcastMessagesSent, msgs_now, min_window);
        let id3 = ContractInstanceId::new([3u8; 32]);
        let rate3 = msgs_rates[&id3];
        assert!(
            rate3 > 30.0,
            "saturated buffer must read the true storm rate (~36.6/s), got {rate3}/s"
        );
        assert!((msgs_total - rate3).abs() < 1e-9);
    }

    #[test]
    fn test_eviction_skipped_below_cap() {
        // Boundary: a handful of distinct sources stays well under the cap,
        // so nothing is ever evicted and every entry remains queryable.
        let meter = Meter::new_with_window_size(100);
        let now = Instant::now();
        for i in 0..8u8 {
            meter.report(
                &contract_source(i),
                ResourceType::StateBytesWritten,
                1.0,
                now,
            );
        }
        assert_eq!(meter.attribution_meters.len(), 8);
        for i in 0..8u8 {
            assert!(
                meter
                    .attributed_usage_rate(
                        &contract_source(i),
                        &ResourceType::StateBytesWritten,
                        now
                    )
                    .is_some()
            );
        }
    }

    #[test]
    fn test_ttl_evicts_stale_source_on_insert() {
        // An entry older than the TTL is dropped the next time a NEW source
        // is inserted, even though the map is nowhere near the cap.
        let meter = Meter::new_with_window_size(100);
        let t0 = Instant::now();
        let stale = contract_source(1);
        meter.report(&stale, ResourceType::StateBytesWritten, 1.0, t0);
        assert_eq!(meter.attribution_meters.len(), 1);

        // Report a different source far enough in the future that `stale`
        // has aged past ATTRIBUTION_SOURCE_TTL.
        let later = t0 + ATTRIBUTION_SOURCE_TTL + Duration::from_secs(1);
        let fresh = contract_source(2);
        meter.report(&fresh, ResourceType::StateBytesWritten, 1.0, later);

        assert!(!meter.attribution_meters.contains_key(&stale));
        assert!(meter.attribution_meters.contains_key(&fresh));
        assert_eq!(meter.attribution_meters.len(), 1);
    }

    #[test]
    fn test_ttl_refreshed_by_repeated_reports() {
        // A source reported against again resets its TTL, so it survives an
        // insert that would otherwise have aged it out.
        let meter = Meter::new_with_window_size(100);
        let t0 = Instant::now();
        let kept = contract_source(1);
        meter.report(&kept, ResourceType::StateBytesWritten, 1.0, t0);

        // Refresh just before the TTL would expire.
        let refresh = t0 + ATTRIBUTION_SOURCE_TTL - Duration::from_secs(1);
        meter.report(&kept, ResourceType::StateBytesWritten, 1.0, refresh);

        // New source inserted slightly later: `kept` was last reported at
        // `refresh`, which is still within the TTL window, so it stays.
        let later = refresh + Duration::from_secs(2);
        meter.report(
            &contract_source(2),
            ResourceType::StateBytesWritten,
            1.0,
            later,
        );

        assert!(meter.attribution_meters.contains_key(&kept));
    }

    #[test]
    fn test_cap_enforced_via_lru_eviction() {
        // Filling to the cap with fresh sources (so TTL never fires) and
        // inserting one more must evict exactly the least-recently-reported
        // entry, keeping the map at the cap rather than growing past it.
        let meter = Meter::new_with_window_size(100);
        let base = Instant::now();

        // Use distinct, monotonically increasing timestamps so there's an
        // unambiguous LRU victim. Keep within the TTL window so phase-1
        // pruning is a no-op and we exercise phase-2 (LRU) deterministically.
        for i in 0..MAX_ATTRIBUTION_SOURCES {
            let src = AttributionSource::Contract(ContractInstanceId::new(id_bytes(i as u32)));
            let at = base + Duration::from_millis(i as u64);
            meter.report(&src, ResourceType::StateBytesWritten, 1.0, at);
        }
        assert_eq!(meter.attribution_meters.len(), MAX_ATTRIBUTION_SOURCES);

        // The oldest (i == 0) is the LRU victim.
        let oldest = AttributionSource::Contract(ContractInstanceId::new(id_bytes(0)));
        let newcomer = AttributionSource::Contract(ContractInstanceId::new(id_bytes(
            MAX_ATTRIBUTION_SOURCES as u32,
        )));
        let at = base + Duration::from_millis(MAX_ATTRIBUTION_SOURCES as u64);
        meter.report(&newcomer, ResourceType::StateBytesWritten, 1.0, at);

        assert_eq!(meter.attribution_meters.len(), MAX_ATTRIBUTION_SOURCES);
        assert!(!meter.attribution_meters.contains_key(&oldest));
        assert!(meter.attribution_meters.contains_key(&newcomer));
    }

    #[test]
    fn test_combined_phase_ttl_prune_avoids_lru() {
        // Combined-phase boundary: the map is AT the cap, but every existing
        // entry is older than the TTL. Inserting a new source must drop the
        // stale entries in phase 1 (TTL prune), bringing the map below the
        // cap so phase 2 (LRU eviction of a live entry) is SKIPPED. The new
        // entry is then inserted into the now-small map.
        let meter = Meter::new_with_window_size(100);
        let base = Instant::now();

        // Fill exactly to the cap.
        for i in 0..MAX_ATTRIBUTION_SOURCES {
            let src = AttributionSource::Contract(ContractInstanceId::new(id_bytes(i as u32)));
            meter.report(&src, ResourceType::StateBytesWritten, 1.0, base);
        }
        assert_eq!(meter.attribution_meters.len(), MAX_ATTRIBUTION_SOURCES);

        // Report a new source far enough ahead that every existing entry is
        // past the TTL. Phase 1 should evict ALL of them, so the map ends
        // with just the newcomer — proving phase 2 did not run (it would
        // have left the map at the cap).
        let later = base + ATTRIBUTION_SOURCE_TTL + Duration::from_secs(1);
        let newcomer = AttributionSource::Contract(ContractInstanceId::new(id_bytes(
            MAX_ATTRIBUTION_SOURCES as u32,
        )));
        meter.report(&newcomer, ResourceType::StateBytesWritten, 1.0, later);

        assert_eq!(
            meter.attribution_meters.len(),
            1,
            "TTL prune should have dropped all stale entries before LRU ran"
        );
        assert!(meter.attribution_meters.contains_key(&newcomer));
    }

    /// Encode a u32 into a 32-byte contract-id array so each index maps to a
    /// distinct `ContractInstanceId` (the single-byte `[byte; 32]` helper
    /// only yields 256 distinct ids — not enough to reach the cap).
    fn id_bytes(i: u32) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&i.to_le_bytes());
        bytes
    }
}
