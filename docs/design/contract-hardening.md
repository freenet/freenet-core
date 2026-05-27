# Freenet Contract Hardening Plan

> Draft for review · 2026-05-25 · context: tonight's `4PjqN5…` incident PRs (#4253, #4231, #4252, #4212)

## Design principle: dashboard reflects back-end, not the other way around

The dashboard visualises what the system already does (or what the
governance algorithm in this plan adds). It does **not** invent
operator affordances that would require new back-end mechanisms to
exist behind them.

Concrete failure mode this principle prevents: showing a "pinned" or
"protected" state for a contract in the UI implies the operator can
mark a contract as exempt. That requires storage, a CLI, reconciliation,
and persistence — none of which exist. A future agent seeing the UI
would then feel pressure to build the back-end to match the mockup.
Tail wagging dog.

When you come to implementation, the constraint is: every state, badge,
event, and number in the dashboard must correspond to data the system
actually produces. If a panel can't be sourced from real state, the
panel doesn't ship.

Reasons for "why a contract is hosted" come from real sources:
`HostingManager.client_subscriptions`, `downstream_subscribers`,
`hosting_cache` access type / recency. Reasons for "why a contract is
flagged or evicted" come from the governance algorithm landing in
Phase 4. Banning comes from the automatic Phase 7 repeat-offender TTL.
Nothing else.

## Goal

Make a misbehaving contract bounded in its blast radius **without operator intervention**, while keeping legitimate popular-contract traffic (e.g. River official room) on the same defaults. All decisions local to the node — no gossiped reputation, no globally-coordinated block-list.

### Load-bearing distinction

**Disk pressure** and **activity pressure** are different problems with different signals and different responses. Conflating them produces either a forgetful network (eviction too aggressive) or an exploitable one (subscribers grant immortality).

- **Disk pressure** — LRU with subscriber exemption. About *storage*. Idle persistence is preserved.
- **Activity pressure** — governance + reaper. About *currently burning CPU/fanout*. Idle contracts have zero cost, never reaped.

A contract with no active demand has near-zero activity cost in the meter. Governance never fires on idle contracts — the network's content-addressed persistence property is preserved by construction.

## Cost/benefit framing

The intuition "GET + SUBSCRIBE = benefit, PUT + UPDATE = cost" is approximately right but conflates triggers with costs. Cleaner split:

| Benefit (count, decayed)                                | Cost (measured, decayed)                              |
|---------------------------------------------------------|-------------------------------------------------------|
| Demand events. *Someone asked us to do work for this.*  | Work performed. *What we actually spent.*             |
| `local_get` — our client GET'd                          | `exec_cpu_us` — wall-clock CPU across WASM entries    |
| `forwarded_get` — peer asked us to forward              | `exec_fuel` — wasmtime fuel consumed                  |
| `local_subscribe` — our client subscribed               | `state_bytes_written` — disk write volume             |
| `forwarded_subscribe` — downstream peer subscribed      | `broadcast_bytes` — bytes emitted on contract's behalf|
| `subscriber_minutes` — sustained subscription weighting | `fanout_cost` — Σ(subscriber × per-emit cost)         |

PUT and UPDATE disappear from both columns — they're *causes* of cost, the cost shows up in the measured columns. Cleaner because it's what the meter actually records.

### Sybil weighting falls out naturally

Demand events split into **local** (our client made the request) and **forwarded** (peer asked us to forward). Local crosses the trust boundary — those are *our* users. Forwarded is attacker-synthesizable.

- `local_get_weight = 1.0`, `forwarded_get_weight = 0.1`
- Same shape for SUBSCRIBE
- Long-lived subscribers (active > N minutes) get a multiplier — short-lived subs are cheap to fake

This is the only Sybil resistance we get without an identity layer, and it's free.

### Threshold by anomaly detection, not by hardcoded budget

Initial draft proposed `cost_budget = base + per_benefit_credit × benefit_score` with operator-set knobs. That's wrong in principle: it pins the network to fixed assumptions about reasonable cost levels, which go stale as hardware improves and as new use cases (realtime audio, video) emerge that legitimately consume more.

Cleaner framing: **flag contracts whose cost-per-benefit is dramatically outside the norms this node is already willing to host.** The network defines its own thresholds by observing itself.

#### What MAD is (brief)

**MAD = Median Absolute Deviation.** A single number measuring spread, analogous to standard deviation but robust to outliers.

Recipe: find the median of your values. For each value, compute its distance from that median. Then take the median of those distances. That's MAD.

Why it matters here: standard deviation gets corrupted by a single extreme outlier (the abuser inflates σ enough to hide itself); MAD doesn't, because it's a median-of-distances. Up to 50% of the data can be malicious before MAD itself stops working. That's exactly what we need when the thing we're measuring (network norms) is what an attacker would try to skew.

#### The formula

```
log_ratios = [ log(cost_used(c) / benefit_score(c)) for c in known_contracts ]
m          = median(log_ratios)
mad        = median(|x − m| for x in log_ratios)
threshold  = m + k × mad
flag(c) iff  log(cost/benefit) > threshold
```

This is principled — not "pick a multiplier that feels right" — for three reasons:

- **MAD has a 50% breakdown point.** Up to half the network can be malicious before MAD itself gets corrupted. Standard deviation breaks under a single outlier; MAD doesn't.
- **Log-space handles heavy tails.** Cost ratios compound multiplicatively. A contract 1000× the median is roughly 3× as far in log-space as one at 10× — matches the actual signal.
- **`k` translates directly to false-positive rate.** Under a roughly log-normal honest population: k=3 ≈ p99.7, k=4 ≈ 1-in-16k, **k=5 ≈ 1-in-a-million**, k=6 ≈ 1-in-500M. **k=5 default.** These sigma-equivalent rates hold because the implementation scales MAD by the Gaussian consistency constant `1.4826` (so `σ ≈ 1.4826 × MAD` under normality) — see `MAD_GAUSSIAN_CONSISTENCY` in `crates/core/src/governance.rs`. Without that scaling, raw `k=5` would correspond to only ≈3.37σ.

### Sanity guardrails (also principled)

- **Minimum sample size.** MAD unreliable below n≈30 (standard statistical guidance). Below threshold, fall back to absolute floor + ramp-up; don't flag anything.
- **Capacity-based absolute ceiling.** `threshold × max_concurrent_outliers ≤ node_capacity × safety_factor`. Derived from hardware limits, not picked.
- **Trimmed MAD on contaminated networks.** Compute on the bottom 95% of samples — removes top-tail contamination without arbitrary thresholds. Standard robust-statistics practice.
- **Cold-start ramp-up.** New contracts use the network's percentile-estimated cost for first N minutes (reuses the existing `SOURCE_RAMP_UP_DURATION` pattern from `topology.rs`). Stops new high-bandwidth contracts tripping before they accumulate subscribers.

### Worked example

Imagine 100 known contracts on a node, with log10 cost/benefit ratios distributed around median = −1.0, MAD = 0.4.

Threshold (k=5): `median + k × 1.4826 × MAD = −1.0 + 5 × 1.4826 × 0.4 ≈ +1.965` → flag if log-ratio above that, i.e. cost/benefit ≳ 92× the typical ratio.

| Scenario | Log-ratio | Verdict |
|----------|-----------|---------|
| River (high cost, high benefit, ratio near median) | ≈ −0.8 | Under threshold ✓ |
| Efficient audio (50 KB/s, 50 real subs) | ≈ −0.5 | Under ✓ |
| Inefficient audio (500 KB/s, 50 real subs) | ≈ +0.5 | Below threshold — tolerated ✓ |
| `4PjqN5…`-style (very high cost, low benefit) | ≈ +2.5 | Flagged |
| New audio stream (1 self-sub, building) | n/a | Ramp-up window applies — not flagged |

*If realtime audio becomes common, the median shifts up, MAD grows (the spread widens), and the threshold accommodates it automatically.*

### Operator-facing philosophy

We're not making policy decisions about resource budgets. We're making statistical decisions about anomaly detection. The single knob (`outlier_severity_k`, default 5) has a real false-positive interpretation. The network sets cost norms by observing itself and tolerates anything within its own historical norms.

Contracts that exceed the threshold can still exist — they just can't conscript volunteer infrastructure to subsidize them.

## Overlap with peer-side resource tracking

### What's actually live on the peer side today

| Component | Status |
|-----------|--------|
| `OutboundRequestCounter` (per-peer request count) | Live — fired from `ring.rs:1658` |
| `RequestDensityTracker` (per-location density) | Live — drives gap-targeted growth |
| `adjust_topology` (Add/Remove/Swap decisions) | Live on ring tick (`ring.rs:2573`) |
| min/max connections enforcement | Live |
| Topology swap (replace least-routed peer) | Live |
| `Meter::report` (bandwidth into meter) | **Dead** — `#[allow(dead_code)] // fixme: use this` |
| Cost/benefit removal via `select_connections_to_remove` | Wired but starved — meter empty |

So the peer side has the cost/benefit removal pipeline assembled but the bandwidth signal isn't reaching it. `adjust_topology` sits permanently in the "under-utilized" branch.

### Direct reuse for contract side

- `Meter`, `AttributionSource`, `ResourceType`, `RunningAverage` — same data model, add `Contract(ContractInstanceId)` variant.
- `SOURCE_RAMP_UP_DURATION` cold-start grace — designed for exactly this case; reuse verbatim.
- `routing_value = requests / bandwidth` at `topology.rs:636` **is** the cost/benefit ratio applied to peers.
- `select_connections_to_remove` candidate ranking — normalize, composite score, pick worst.

### Where they diverge

| Axis | Peer | Contract |
|------|------|----------|
| Cost dimensions | One (bandwidth) | Multi (CPU, fuel, state, broadcast, fanout) |
| Benefit signal | *We asked them* (outbound requests) | *They asked us* (GET, SUB, client-attach) |
| Action | Drop connection. Peer keeps existing. | Unsubscribe-then-evict + repeat-ban TTL. |
| Forcing function | Hard min/max bounds | No target count — contracts come and go |
| Sybil cost | Real peer = real handshake | Re-PUT cost = near-zero |

### Architectural win

Wiring `report_resource_usage` from real bandwidth-counting sites:

- Completes the latent peer-side meter — `select_connections_to_remove` becomes a real load-shedder for the first time.
- Adds per-contract attribution at the same call sites (any message carrying a `ContractInstanceId` reports to both `Peer` and `Contract`).
- Closes both dead-code annotations in one PR.

## Subscriber obligation handling

The hard question: when a downstream peer is subscribed to a contract through us, we're "kinda obligated" to host it. How do we reconcile with abuse protection?

### Three positions

1. **Absolute obligation.** Never reap while subscribed. One Sybil subscriber = permanent abuse. *Essentially today.*
2. **Obligation within network norms.** Subscribers add to `benefit_score`, which shifts the contract's position in the network's distribution of log(cost/benefit) ratios. A contract with genuine demand stays out of the outlier tail naturally — the obligation is honored implicitly by the math. When the contract *is* flagged as an outlier **despite** subscriber-buoyed benefit, send explicit unsubscribe-cancellation, then evict.
3. **Eviction without warning.** Drops subscriber notifications silently. Strictly worse than (2).

**Plan adopts (2)** — mandatory unsubscribe-before-evict. The "honor / exceed" gate is the same MAD-based outlier flag that governs the rest of the reaper. Subscribers don't grant immortality, but they do raise the contract's benefit_score which makes it harder to trip the threshold. No magic per-contract budget.

### Ban TTL — repeat offenders only

- **No ban on first eviction.** Attacker can re-PUT. Budget restarts; if cost re-exceeds, evict again. Each cycle the attacker pays PUT cost + we pay validate cost — never pinned.
- **Ban on second eviction within a window** (e.g. 1 hour). Demonstrated repeat offender.
- Ban TTL is short, memory-only, no gossip.

## Two LRU refinements (orthogonal, ship anytime)

### "Recently abandoned" priority bucket

When `contract_in_use` transitions `true → false`, mark the entry. Under disk pressure, the LRU sweep targets this bucket first before older-but-still-touched entries.

*Doesn't change behavior in the no-pressure case* — idle persistence preserved.

### Rejected: immediate evict on last-subscriber-leave

Initial draft proposed this. Withdrawn — would make the network too forgetful. Content-addressed persistence beyond active demand is core.

## Shared governance module

The MAD-based machinery transfers cleanly from contracts to peers. A single `crate::governance` module serves both call sites.

|  | Peer side | Contract side |
|---|-----------|---------------|
| Ratio | `routing_value = requests / bandwidth` | `benefit_score / cost_used` |
| Direction | Flag bottom tail (low requests-per-byte) | Flag bottom tail (low benefit-per-cost) |
| Action | Drop connection, reconnect-eligible | Unsubscribe-then-evict, repeat-ban TTL |
| Sample size | 10–50 peers — often below n=30 | typically 100s — above n=30 |
| Forcing function | Hard min/max connection caps | None — discretionary |

### Three differences worth designing around

- **Discretionary vs. forced.** For peers, MAD detection is supplementary. For contracts, it's primary.
- **Small-sample fallback is the common case for peers.** Existing routing+topology composite remains production path most of the time.
- **Genuinely new capability for peers:** today's logic acts on capacity or aggregate-usage; doesn't catch an *individual* anomalously-bad peer below those thresholds.

### Module shape

```rust
// crate::governance

pub(crate) struct OutlierConfig {
    pub k: f64,             // default 5
    pub min_samples: usize, // default 30
    pub trim_fraction: f64, // default 0.05
}

pub(crate) struct OutlierResult<K> {
    pub flagged: Vec<K>,
    pub median_log_ratio: Option<f64>,
    pub mad: Option<f64>,
    pub threshold: Option<f64>,
    pub sample_size: usize,
}

pub(crate) fn detect_outliers<K, S>(
    samples: &HashMap<K, S>,
    extract_log_ratio: impl Fn(&S) -> Option<f64>,
    config: &OutlierConfig,
    capacity_ceiling_log: f64,
) -> OutlierResult<K>
where K: Clone + Eq + Hash;
```

The same dry-run release that calibrates k=5 for contracts simultaneously calibrates it for peers. One distribution analysis, two pipelines validated.

## Dashboard observability

Bugs in governance will be hard to diagnose in the live network. Operators need to see what governance is thinking, in real time, on their own node.

### Per-contract view

- Cost score breakdown: `cpu_us`, `fuel`, `state_bytes_written`, `broadcast_bytes`, `fanout_cost`
- Benefit score breakdown: local/forwarded GETs, local/forwarded SUBSCRIBEs, subscriber-minutes
- Current `log(cost/benefit)` and where it sits vs. the threshold
- Lifecycle state: `Normal | Suspect | WouldReap(dry_run) | Reaped | Banned(expires_at)`
- Recently-reaped / recently-banned history

### Per-peer view

- Routing value, position in the network log-ratio distribution (distance from median, measured in MAD-units), would-drop flag during dry-run
- Existing connection metadata (location, uptime, topology contribution)

### Network-norms view

- Current `m` (median log-ratio), `MAD`, `k`, sample size, computed threshold
- Log-space distribution histogram showing every contract/peer relative to the threshold line
- Capacity ceiling indicator
- **Cascade-latency metric.** Wall-clock time from first-node reap to last-node reap across observed peers. Surfaces whether the natural cascade is fast enough or whether explicit cascade-on-Cancelled is needed.

### Surfacing

- **Local dashboard:** governance panel. Contracts and peers tabs. Distribution histogram as SVG. Live-updating.
- **`freenet service report`:** include governance snapshot in diagnostic bundle.
- **Tracing:** every reap/ban/drop decision logs at INFO with full breakdown as structured fields. Exported to OpenTelemetry collector on nova.

## Testing strategy

Governance bugs are silent in two directions:

- **False positives** — visible to users as content disappearing, operator might assume normal LRU eviction.
- **False negatives** — looks like normal slow performance degradation. Indistinguishable from "the network is just busy" without governance dashboards.

### Unit-level — primitives in isolation

- **MAD computation:** property-based tests — invariance under translation, scaling, contamination up to 50%.
- **Edge cases:** empty sample, n=1, all-identical values (MAD=0 → threshold collapses), near-zero benefit, NaN/inf guards.
- **Trimmed-MAD vs. naive MAD:** assert trimmed version unchanged when bottom 95% is contaminant-free.
- **Capacity-ceiling interaction:** assert ceiling clamps when MAD would exceed; doesn't clamp when MAD is conservative.

### Integration-level — governance in the executor

- Reaper fires on a synthesized "abusive" contract in mixed population of 100+ honest contracts.
- Reaper does NOT fire during cold-start (ramp-up window respected).
- Repeat-offender ban triggers on second eviction within window, not first.
- Unsubscribe-before-evict: downstream peer receives `Cancelled`, re-SUBSCRIBE lands on a different upstream.
- Dry-run mode: governance logs *would-reap* decisions, no actual eviction occurs.

### Simulation-level — full-network behavior

- **Mixed-population network:** 50 nodes, 200 contracts, 5% deliberately abusive. Assert abusers reaped, zero legitimate reaped, capacity bounded.
- **Realtime-audio simulation:** high-bandwidth legitimate contract with growing subscriber base. Assert survival.
- **Network adaptation:** introduce new class of high-cost legitimate contracts gradually. Assert median shifts up, threshold adapts.
- **Coordinated multi-contract abuse:** 100 individually-under-threshold contracts collectively monopolizing. Assert aggregate caps catch this or file as known gap.

### Calibration tests on real distributions

- Snapshot the cost/benefit distribution from a production gateway. Replay through governance offline with k ∈ {3, 4, 5, 6, 7}.
- Assert: River never flagged at any k ≥ 3. `4PjqN5…`-style flagged at k ≤ 6. Gap > 2 in k-units.
- If gap < 2: block Phase 8 (enforce) until resolved.

### The dry-run release is part of the test suite

Phase 4 ships governance in `dry_run` mode. During that release, the dashboard logs every *would-reap* decision with full context. Phase 8 (enforce) only ships once dry-run data shows zero would-reap on known-legitimate contracts, would-reap on abusers, and stable distribution statistics.

## Design choices settled

1. **Attribution key:** `ContractInstanceId` (matches existing keying).
2. **Sybil resistance for benefit:** local/forwarded weighting + subscriber-minutes only. Distinct-peer-identity ranking out of scope for v1.
3. **Score persistence:** memory-only.
4. **Threshold model:** MAD-based outlier detection in log-space (k=5 default). Not hardcoded budgets.
5. **Shared governance module:** peers and contracts both consume `governance::detect_outliers`.
6. **Idle persistence:** preserved. Governance fires on activity cost only.

## Phased plan

### Phase 0 — WASM fuel coverage audit

Confirm wasmtime fuel is set + enforced on every entry: `validate_state`, `update_state`, `summarize_state`, `get_state_delta`, plus host-callback re-entries. Add evil-contract fixture and assert engine kills it within budget.

### Phase 1 — LRU recently-abandoned priority bucket

Mark entries when `contract_in_use` transitions `true → false`. Under disk pressure, target this bucket first.

### Phase 2 — Per-(peer, contract) UPDATE rate limit

`TrackedBackoff<(PeerId, ContractInstanceId)>`. Apply at `SyncStateToPeer` emit + originator UPDATE entry. Reject with typed marker.

### Phase 3 — Per-contract resource attribution (no behavior change)

Extend `AttributionSource` with `Contract(ContractInstanceId)`. Extend `ResourceType`. Swap `RwLock<BTreeMap>` for `DashMap`. Wire `report` at WASM call wrappers, state commit, broadcast dispatch, message-decode bandwidth attribution. Drop both `#[allow(dead_code)] // fixme: use this` annotations.

### Phase 4 — Shared governance module (dry-run)

New `crates/core/src/governance.rs`. Trimmed-MAD computation in log space with capacity ceiling clamp. Config `contract_governance = off | dry_run | enforce`, default `dry_run` for one release. Drop `contract_in_use` unconditional exemption.

### Phase 4.5 — Dashboard + diagnostics

Governance panel. Distribution histogram. Cascade-latency metric (forcing function for explicit-cascade follow-up). Service report integration. Structured tracing.

### Phase 5 — Unsubscribe-before-evict + reaper

`SubscribeMsg::Cancelled` variant. Wire-format-additive; bumps `min-compatible-version`.

### Phase 6 — DROPPED (was: Operator pin/ban list)

Originally proposed `freenet contract {pin,ban,unpin,unban} <key>` as
operator-facing CLI affordances. Removed after design review surfaced
two problems:

1. **No "marking" mechanism exists.** Pin/unpin requires a persistent
   per-contract operator flag with its own storage, CLI surface, and
   reconciliation logic. The existing `HostingManager` has nothing
   like it.

2. **The math already protects valuable contracts.** A contract with
   real subscribers (local clients or downstream peers) has a high
   `benefit_score` and therefore needs a much higher cost to trip the
   threshold. River-style popular contracts are protected because
   people use them, not because the operator declared exempt status.

3. **The dashboard should visualize what the system does, not invent
   affordances that then have to be built.** A future agent looking
   at "pinned" in the UI mock would feel pressure to implement the
   mechanism behind it — tail wagging dog. The dashboard surfaces
   *automatic* state (which contracts the math is acting on, why
   they're hosted via existing subscription tracking); it does not
   show operator-defined overrides that don't exist in the back-end.

If a future use case genuinely demands per-contract operator override,
revisit then with concrete justification — but not as a speculative
feature.

**Banning is preserved**, but only as the **automatic** behaviour in
Phase 7 — the system bans a contract from itself after observed
repeat eviction. No operator CLI invokes a ban.

### Phase 7 — Repeat-offender ban TTL

Memory-only ban map with TTL. Trigger on second eviction within
window. **Automatic** — no operator action; the system reaches this
state on its own based on observed repeat-evict behaviour.

### Phase 8 — Flip governance to `enforce`

One release after Phase 4. Operators have had a release to see dry-run logs and tune.

## Implementation: split into two PRs

- **PR #1 (foundational, no wire changes):** Phase 0 + Phase 1 + Phase 3. Lights up both peer-side and contract-side meters. Pure observability — no reaping behavior added.
- **PR #2 (governance system):** Phase 5 + Phase 4 + Phase 4.5 + Phase 2 + Phase 7. Adds wire variant, governance module (dry-run default), dashboards, refinements. Phase 6 was dropped — see its section above.

Both PRs ship in the same release. No release window required between them. Phase 8 (flip to enforce) is a deliberate later release-cycle gate after calibration data has been collected.

## Cross-cutting concerns

### Migration risk

Phase 4's `dry_run` default for a full release is the mitigation. Phase 8 is the deliberate flip, gated on empirical separation criteria.

### Performance

The meter sits on every executor hot path after Phase 3. `DashMap` swap addresses contention. MAD recomputation is O(n log n) per tick — cache on a coarse interval.

### Determinism

Governance decisions depend on `TimeSource` and meter readings. Thread `TimeSource` through `ContractScore` (matches core conventions).

### Remaining arbitrary parameters — honest audit

MAD-based detection moves the **load-bearing threshold** from arbitrary to principled. But the plan still contains parameters that aren't statistically derived:

| Parameter | Default | Status |
|-----------|---------|--------|
| `k` (outlier severity) | 5 | **Principled** — false-positive rate under log-normal assumption |
| `min_samples` | 30 | **Principled** — standard statistical guidance |
| `trim_fraction` | 0.05 | **Convention** — robust-statistics default |
| Capacity ceiling safety factor | 0.1 | **Derived** — hardware-driven capacity-share argument |
| Cold-start ramp-up window | existing `SOURCE_RAMP_UP_DURATION` | **Reuse** |
| Ban TTL window | 1 hour | **Judgment call** — low blast radius |
| "Repeat offender" window | same as ban TTL | **Judgment call** |
| Local vs. forwarded weight (1.0 / 0.1) | arbitrary ratio | **Judgment call**, mitigated by MAD adapting |
| Subscriber-minutes long-lived threshold | (unspecified) | **Judgment call** — could be replaced with continuous age weighting |

Load-bearing thresholds are principled; ergonomic parameters around the edges are judgment calls.

### Failure modes worth pre-planning

- **MAD collapses to zero** (all samples identical) → threshold = median. Sanity: when MAD < epsilon, skip governance entirely, log the condition.
- **Capacity ceiling permanently binding** → MAD wants higher threshold than node capacity allows. Surface in dashboard as warning.
- **Honest contract reaped due to bug** → operator manually un-bans, files report. Service report bundle includes governance state.

## What this plan deliberately does NOT do

- **No gossiped reputation.** Local-only decisions.
- **No identity layer.** Sybil resistance is the local/forwarded weighting only.
- **No state-size in governance.** Storage cost is the disk-pressure path's concern.
- **No changes to WASM execution model.** Fuel + timeouts remain the per-call ceiling; governance is the cross-call ceiling.
- **No immediate eviction on last-subscriber-leave.** Idle persistence preserved.
- **No automatic re-PUT of reaped contracts.** Operator action required to revive.

## Open questions

1. **Wire format for unsubscribe-cancellation:** new variant on `SubscribeMsg` vs. new top-level message?
2. **Cost dimensions normalization weights.** Each cost dimension needs a weight to combine into a single number. Those weights are themselves k-style choices with less obvious principled basis.
3. **Local/forwarded weight defaults (1.0 / 0.1).** Less load-bearing now that MAD adapts.
4. **Subscriber-minutes implementation.** Cheap if we track `(subscriber, first_seen)`, but every `Subscribe`/`Unsubscribe` event updates. Acceptable hot-path cost?
5. **Peer-side small-sample fallback.** When n < 30 (common case), MAD skips. Widen sample window to include recently-disconnected peers?
6. **Coordinated multi-contract abuse.** 100 individually-under-threshold contracts collectively monopolizing. Per-contract governance doesn't see it. Probably follow-up.
7. **Calibration sample source.** Nova vs. vega vs. typical user node have different traffic shapes.
8. **Explicit cascade on `SubscribeMsg::Cancelled`.** Deferred optimization. Natural cascade works without it — each peer reaps within its own window — but worst-case latency = chain_depth × window_length. **Trigger condition for revisiting: cascade-latency metric in the dashboard.** If observed cascade latency stays low post-deploy, optimization is unnecessary. If it grows, the metric is the forcing function. Filed as the canonical "remember to revisit" mechanism — the metric surfaces autonomously when implementation matters.
