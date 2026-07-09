# Demand-driven hosting: eviction & retention (piece A)

Design resolved 2026-06-30; eviction model settled 2026-07-06/08. Tracker:
freenet/freenet-core#4642. Invariants: `.claude/rules/hosting-invariants.md`.
Canonical intent and rationale: `docs/design/demand-driven-hosting.md`.

## Goal
A peer hosts the contracts it is most likely to be asked for, within a budget sized by its own scarcest resource.

## Design principles
- **Instrumentation is horizontal.** Every piece of this redesign ships with the telemetry needed to observe its OWN behavior in production. This is a PR acceptance criterion: a feature PR that adds a mechanism but no instrumentation for it gets bounced in review. Rationale: the behavior here is emergent and resource-dependent, so it cannot be fully tested pre-production, so production telemetry IS the validation instrument and it must grow WITH each piece rather than being deferred to a later "observability" PR. (This is why A1, telemetry, is the foundational sub-PR.)
- **Telemetry shape: per-node aggregate scalars/rates only.** Emit resource/behavior metrics as per-node aggregate scalars and rates carried on the existing periodic snapshot. NEVER per-contract or per-request event streams, because nova ingests the whole fleet and a per-contract/per-request stream multiplied across every peer is an ingestion DoS on the collector.

## Eviction (one demand-ordered decision)
Eviction is a single demand-ordered decision. When a peer is over budget it sheds its lowest-demand contracts first. The same ranking runs whether the peer is relieving resource pressure or fitting a new arrival, so there is no separate admission decision, no OOM valve, and no pin on subscribed contracts.

- **Victim order.** Contracts are ordered ascending by `(local_subscription_count, downstream_subscriber_count, recency, key_bytes)`: fewest local client subscriptions first (a contract the node's own client is subscribed to is evicted LAST), then fewest downstream subscribers, then least-recent real GET/PUT access (recency), then contract-key bytes as a final deterministic tiebreak. Local and downstream subscribers are SEPARATE ranking dimensions, local ranked above downstream.
- **Demand signal is subscriptions AND real GET/PUT reads.** Reads and PUTs are a permanent demand signal, not merely a recency tiebreak: a contract that is only ever GET-read or PUT and never subscribed (the River UI container and web/UI container contracts are the canonical case) is kept renewed in the update mesh and retained by its GET/PUT recency, and does not fall out of the network for lack of a subscription. Subscriptions outrank reads: while a contract has an active subscription its recency clock does not tick (it is held by the subscription tier), and the clock starts only at subscription TERMINATION, after which (and for never-subscribed read-only contracts) retention is by real GET/PUT recency. Only genuine client access (a real GET or PUT) resets recency; automatic subscription-renewal traffic does not.
- **Subscriptions are not a pin.** A subscribed contract IS evicted when the peer is over budget and holds nothing that ranks lower; lower-ranked contracts are always evicted first. A locally-subscribed contract is evicted LAST (only in the extreme where every contract the peer holds carries a local subscription and it is still over budget does the least-recently-read local one go), but it is not absolutely pinned, so there is no OOM-stuck corner.
- **No `min_ttl` cold-start floor.** A real GET or PUT resets recency, so a freshly-accessed contract is protected simply by being the most-recently-accessed, and a fresh PUT resets recency at PUT time without waiting for a first read. Op-scoped protection is recency-reset-at-op-start plus a backstop hard guard against a recency-reset contract being evicted before its op completes; it is not a hard op-scoped pin.
- **Distance is not an eviction input.** Distance's causal pull on demand already flows through subscriber count via keyward routing gravity (counting both double-counts), and locality is delivered by routing, not by eviction. The proximity-prior read-demand estimator in `demand.rs` is retained for telemetry only, not as a ranking input.

## Budget (capability-relative)
- The budget is how many contracts the peer can host before its SCARCEST resource saturates. Empirically the binding cost is per-contract MEMORY (~1 MB) and update FANOUT, not storage bytes (~650 KB/contract, negligible). Binding resource differs by hardware (laptop=memory, small VM=CPU, residential=uplink).
- Memory axis: reuse `module_cache.rs::read_total_ram_bytes()` (min(RAM, cgroup)) to size the hosting budget instead of the flat 1 GiB `DEFAULT_HOSTING_BUDGET_BYTES`. Addresses #4565.
- Within budget per-contract cost is ~uniform, so eviction is near-pure demand-ordering, and storage bytes are a minor tiebreaker plus a hard ceiling. The one variable cost, update fanout, is weighed when taking on a subscription's ongoing fanout given headroom; a protected multi-subscriber contract whose fanout outgrows the uplink is absorbed by graceful-degradation coalescing, never by eviction.

## Admission
There is no separate admission decision. A newcomer competes in the same eviction as everything else. Because it arrives with at most one subscriber, it can never displace a 2+-subscriber incumbent (so multi-subscriber contracts are protected from churn for free, with no admission machinery), and a fewest-subscriber newcomer is simply the one evicted (an emergent refusal). A zero-subscriber every-hop copy is the lowest-value eviction candidate, so subscriber-primary eviction is the spam bound on every-hop placement.

## Sub-PRs (sequenced; share ring/hosting)
A1 resource telemetry; A2 memory-capability budget (`read_total_ram_bytes` -> hosting budget); A3 demand-ordered eviction policy. The A4 predicted-demand blend from the original piece-A plan is superseded: the demand signal is subscriber count plus real GET/PUT recency, with no distance-derived read-rate estimate in the ranking.

## Validation
Sim health metrics (subscribe/GET success, tree formation, time-to-first-op) + a check that a real-demand contract (e.g. a River room) is NOT evicted ahead of junk (the #4338 miscalibration test).

[AI-assisted - Claude]
