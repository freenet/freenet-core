# replay-harness

Offline test harness for evaluating Phase 2 shadow-RTT controllers (issue
[#4074][issue]) against synthetic scenarios and OTLP telemetry, before
shipping any controller to production.

Not a production component. No code path in `freenet` reaches this crate.

## Why

The previous three congestion-control attempts (LEDBAT++, BBRv3, custom
adaptive) all failed in production because there was no offline test loop.
LEDBAT's death spiral on a single packet loss only showed up after deploy.
BBR's stale `min_RTT` across reroutes was caught by manual debugging weeks
after the algorithm went live.

Phase 1.5 ([#4292][p15]) just landed the per-node tagged telemetry we need.
The replay harness is the next piece: before any Phase 2 controller ships,
it must pass a battery of scripted scenarios that pin known failure modes,
then be evaluated against real OTLP data without touching production.

[issue]: https://github.com/freenet/freenet-core/issues/4074
[p15]: https://github.com/freenet/freenet-core/pull/4292

## Quick start

```bash
# List the scenarios and controllers shipped with v1.
cargo run -p replay-harness -- scenarios
cargo run -p replay-harness -- controllers

# Run one scenario against the default controller (rfc_draft).
cargo run -p replay-harness -- synthetic correlated_inflation

# Run every scenario against rfc_draft (the algorithm sketched in #4074).
cargo run -p replay-harness -- synthetic all

# Same against the no-op baseline.
cargo run -p replay-harness -- synthetic all --controller fixed_rate

# Run the assertion suite (every scenario × every controller, with each
# (controller, scenario) outcome pinned).
cargo test -p replay-harness
```

## What's pinned

Ten scenarios covering the failure modes Phase 2 design has to clear. "Sane
behaviour" is what a *correct* controller must do; some current controllers
deliberately disagree (see below), and those disagreements are pinned per
`(controller, scenario)` in `tests/scenarios_pin.rs`.

| Scenario | What it pins | Sane behaviour |
|---|---|---|
| `idle_steady_state` | The ~55 ms ambient overlay noise we measured in production | MUST NOT fire |
| `correlated_inflation` | Every peer's RTT rises together — the actual contention case | MUST fire at least once |
| `single_peer_outlier` | One peer at 500 ms, four others at baseline | MUST NOT fire (cross-peer median rejects) |
| `small_n` | Only 2 peers — below the `rolling_rtt_stats.rs` N≥3 trustworthy threshold | MUST NOT fire (N≥3 guard) |
| `single_packet_loss` | A transient 10× spike on one peer | MUST NOT fire (transient single-connection outlier) |
| `slow_routing_drift` | Baseline drifts up 5 ms/min for 30 min (a real path change) | MUST NOT fire (5-min baseline tracks the drift) |
| `churning_peers` | Peers join/leave every 30 s with healthy RTTs | MUST NOT fire (churn alone is not a signal) |
| `cold_start` | New peers on a slow path, no baseline history | MUST NOT fire (high RTT ≠ rising RTT; no panic) |
| `reference_diverges_from_overlay` | Overlay inflates, reference path stays flat | MUST NOT fire (overlay queueing, not uplink contention) |
| `reference_tracks_overlay` | Overlay and reference inflate together | MUST fire (shared cause is the local uplink) |

### Controllers, and their documented failures

- **`FixedRate`** (production default) passes the universal sanity check of
  "never fires on any scenario."
- **`RfcDraft`** (the algorithm sketched in #4074) intentionally **fails** two
  pins, and those failures are the point:
  - `idle_steady_state` — fires 48 times across 300 s. The 30 ms inflation
    threshold sits below the ambient overlay noise floor Phase 1 telemetry
    measured, so it reacts to healthy noise.
  - `reference_diverges_from_overlay` — fires because it is reference-blind. It
    cannot distinguish overlay queueing from uplink contention, which is the
    entire reason the Phase 1.5 reference-ping signal (#4292) exists. It gets
    `reference_tracks_overlay` "right" only by luck (it fires on both). A
    reference-aware Phase 2 controller must fire on `tracks` and hold on
    `diverges`.
- **`ledbat`** (a stripped-down LEDBAT++) exists to reproduce the **death
  spiral** that got LEDBAT++ abandoned in production. It reacts to the *worst*
  single connection's queueing delay, so on `single_packet_loss` one peer's
  transient spike drives a deep multiplicative cut (down to a few percent of
  the starting rate) followed by slow additive recovery — while `RfcDraft`'s
  cross-peer median rejects the same outlier and never moves. The asymmetric
  drop/recover trajectory is pinned in `tests/scenarios_pin.rs`.

## Adding a controller

1. Drop a new file in `src/controllers/`.
2. Implement [`Controller`](src/controllers.rs).
3. Re-export it from `src/controllers.rs`.
4. Wire it into the binary's `match controller_name { … }` in `src/main.rs`.
5. Run `cargo test -p replay-harness` — it auto-runs every scenario
   against your controller; if your design philosophy diverges from the
   default expectation for any scenario, extend `expected_fires` in
   `tests/scenarios_pin.rs`.

## Adding a scenario

1. Drop a new file in `src/scenarios/`.
2. Implement a `pub fn scenario() -> Scenario`.
3. Add the module to `src/scenarios.rs` and append to `all_scenarios()`.
4. `cargo test` — the new scenario runs against every controller
   automatically.

## Why not real OTLP replay yet

The current OTLP `shadow_rtt_aggregate` event emits the pre-computed
aggregate (`active_peers`, `peers_with_recent`, `median_inflation_us`) —
not the underlying per-peer RTT samples that `RollingRttStats` needs to
recompute the snapshot at each tick. Feeding OTLP into the harness
requires either (a) ALSO emitting per-peer samples via OTLP, or (b) an
aggregates-replay mode that fakes per-peer snapshots from the aggregate.
Both have caveats; both are deferred to a follow-up.

In the meantime, synthetic scenarios are the higher-value testing layer
anyway — they pin *specific* failure modes rather than measuring against
a single instance of "what happened in production last week."

## Not in scope

- Per-tick controller telemetry to the OTLP collector (offline tool only).
- GUI / plots (decision-trace dump is CSV/JSON-friendly; plot externally).
- Live mode (reading the collector in real-time).
- Any change to production transport behaviour.
