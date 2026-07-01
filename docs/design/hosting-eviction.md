# Demand-driven hosting: eviction & retention (piece A)
Design resolved 2026-06-30. Tracker: freenet/freenet-core#4642. Invariants: `.claude/rules/hosting-invariants.md`.

## Goal
A peer hosts the contracts it is most likely to be asked for, within a budget sized by its own scarcest resource.

## Design principles
- **Instrumentation is horizontal.** Every piece of this redesign ships with the telemetry needed to observe its OWN behavior in production. This is a PR acceptance criterion: a feature PR that adds a mechanism but no instrumentation for it gets bounced in review. Rationale: the behavior here is emergent and resource-dependent, so it cannot be fully tested pre-production — production telemetry IS the validation instrument, and it must grow WITH each piece rather than being deferred to a later "observability" PR. (This is why A1, telemetry, is the foundational sub-PR.)
- **Telemetry shape: per-node aggregate scalars/rates only.** Emit resource/behavior metrics as per-node aggregate scalars and rates carried on the existing periodic snapshot. NEVER per-contract or per-request event streams — nova ingests the whole fleet, so a per-contract/per-request stream multiplied across every peer is an ingestion DoS on the collector.

## Eviction (Greedy-Dual, demand-ordered)
- Each hosted contract X has `keep_score(X) = eviction_floor + predicted_demand(X)`, set on insert and refreshed on every read (GET/SUBSCRIBE).
- `eviction_floor`: one per-peer running value; when over budget, evict the min keep_score and set `eviction_floor = keep_score(evicted)`. Greedy-Dual aging, measured in cache-contention, not wall-clock.
- `predicted_demand(X)`: per-contract estimate of X's read-request rate at this peer. Seeded at a proximity prior; blended toward X's own observed read rate as evidence accrues (prior washes out with data). Only RATIOS matter (scale-invariant), so no absolute calibration needed.
- Proximity prior `g(distance)`: isotonic regression (monotone non-increasing) fit from this peer's observed (distance-to-key, request-rate) data, binned by distance, refit periodically. Reuse the existing isotonic/PAVA machinery.
- Interest signals: GET and SUBSCRIBE = read-demand (same weight); PUT only SEEDS (sets predicted_demand to the proximity prior, not demand); an active SUBSCRIBE PINS the contract (exempt from eviction; grace clock starts only at subscription termination).

## Budget (capability-relative)
- The budget is how many contracts the peer can host before its SCARCEST resource saturates. Empirically the binding cost is per-contract MEMORY (~1 MB) and update FANOUT, not storage bytes (~650 KB/contract, negligible). Binding resource differs by hardware (laptop=memory, small VM=CPU, residential=uplink).
- Memory axis: reuse `module_cache.rs::read_total_ram_bytes()` (min(RAM, cgroup)) to size the hosting budget instead of the flat 1 GiB `DEFAULT_HOSTING_BUDGET_BYTES`. Addresses #4565.
- Within budget per-contract cost is ~uniform, so eviction is near-pure demand-ordering; storage bytes are a minor tiebreaker + hard ceiling. Fanout cost is an ADMISSION concern, not eviction (fanout only accrues while pinned).

## Admission (piece B, summary)
- GET hosts-on-first and lets the gauge evict one-offs (demand-driven, so NOT the relay-caching anti-pattern; guard-comment the site). SUBSCRIBE builds a chain of demand-pinned hosts. An over-committed peer REFUSES new subscriptions.

## Sub-PRs (sequenced; share ring/hosting)
A1 resource telemetry (this PR); A2 memory-capability budget (`read_total_ram_bytes` -> hosting budget); A3 demand-ordered eviction policy; A4 blend.

## Validation
Sim health metrics (subscribe/GET success, tree formation, time-to-first-op) + a check that a real-demand contract (e.g. a River room) is NOT evicted ahead of junk (the #4338 miscalibration test).

[AI-assisted - Claude]
