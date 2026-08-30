---
paths:
  - "crates/core/**"
---

# Testing Rules

## Decision Tree: Choosing Test Approach

```
Is it a single function/algorithm?
  → YES: Unit test with #[test]
  → NO: Continue...

Does it need network simulation?
  → NO: Unit test with mocks (MockNetworkBridge, MockRing)
  → YES: Continue...

How many nodes?
  → 1 gateway: #[freenet_test] macro
  → 2-10 nodes: #[freenet_test] with multiple nodes
  → 20+ nodes: freenet-test-network

Need fault injection?
  → YES: SimNetwork with FaultConfig
```

## Test Coverage Requirements

### Bug fixes (`fix:` PRs) MUST include regression tests

```
WHEN fixing a bug:
  1. Write a test that REPRODUCES the bug BEFORE writing the fix
  2. Verify the test FAILS without the fix
  3. Apply the fix
  4. Verify the test PASSES with the fix

  The test must be specific enough that:
  - It would catch this exact bug if reintroduced
  - It documents the failure mode (name describes the scenario)
  - Example: test_loss_pause_allows_packet_through, not test_loss_pause

  CI will reject fix: PRs that don't add at least one new #[test] function.
```

### Edge cases and boundary conditions

```
WHEN writing tests for any behavioral change:
  → Happy path is NECESSARY but NOT SUFFICIENT
  → You MUST also test:

  1. Boundary values (zero, one, max, overflow)
  2. Error paths (what happens when it fails?)
  3. Concurrent/racy scenarios (if async or multi-threaded)
  4. Scale edge cases (empty collection, single element, at capacity)
  5. State transitions (invalid states, repeated calls, out-of-order)

  WRONG:
    fn test_send_packet() { /* verify one packet sends */ }

  CORRECT:
    fn test_send_packet_at_cwnd_limit() { ... }
    fn test_send_packet_during_loss_pause() { ... }
    fn test_send_packet_zero_cwnd() { ... }
    fn test_send_packet_after_recovery() { ... }
```

## Trigger-Action Rules

### When writing new code in `crates/core/`

**BEFORE writing any function that needs current time:**
→ Check: Am I using `TimeSource` trait?
→ If using `std::time::Instant::now()` or `tokio::time::sleep()`: STOP. Refactor to accept `impl TimeSource`.

**BEFORE writing any code that needs randomness:**
→ Check: Am I using `GlobalRng`?
→ If using `rand::random()` or `rand::thread_rng()`: STOP. Use `GlobalRng::random_u64()` or `GlobalRng::fill_bytes()`.

**BEFORE writing any socket code in tests:**
→ Check: Is this a simulation test?
→ If YES and using `tokio::net::UdpSocket`: STOP. Use `SimulationSocket::bind()`.

### When a test fails

```
Test failed?
  → DO NOT delete the test
  → DO NOT comment it out without explanation
  → Flaky tests are broken tests — investigate the root cause

  Is it flaky (intermittent failure)?
    → DO NOT add #[ignore], retries, or increased timeouts
    → Investigate root cause: timing assumptions, shared state, race conditions
    → Fix the underlying bug — the flaky test is telling you something
    → See: flaky-tests.md global rule

  Is it broken by your change?
    → Fix the test or fix your code — don't ignore it
    → If the test expectations are genuinely wrong (new semantics):
      1. Add #[ignore] attribute
      2. Add comment explaining the semantic change and referencing the PR
      3. Keep as historical documentation
```

### When running tests

```
Running simulation tests?
  → Use: cargo test -p freenet --features "simulation_tests,testing" --test simulation_integration

Running all tests?
  → Use: cargo test -p freenet

Running specific integration test?
  → Use: cargo test -p freenet --test simulation_integration
```

### Cross-test interference is invisible to CI — only plain `cargo test` can see it

Every CI job runs `cargo nextest`, which executes each test in its own
process. That is deliberate and mostly good (see "Running determinism tests"
below), but it means **no CI job can observe a bug in which one test
interferes with another through process-global state** — there is no shared
process for the interference to happen in. `ci.yml` says "process isolation
handles it (#3051)", which is true of the symptom and false of the coverage:
it is blindness, not a guard. `AGENTS.md` tells you to run `cargo test` before
committing, and that is the only runner that can expose this class.

The mechanism generalises past any one library: **any process-global cache
whose value is decided by whichever thread arrives first** is order-dependent,
and per-process isolation hides it completely. The instance that motivated
this entry (#4927, fixed in #5314): `tracing` caches each callsite's
`Interest` process-globally on first touch, and resolves it against the
*registering* thread's subscriber, so a test with no subscriber could pin a
callsite to `never` and blind another test's thread-local log capture to that
one callsite. Measured: 29 failures in 1000 `cargo test -p freenet --lib
subscriber_limit_tests` runs across three tests, 0 in 2000 runs after the fix
— and **unreachable under `cargo nextest` at any repeat count**.

`[profile.ci] retries = 2` in `.config/nextest.toml` is the *smaller* half of
this gap: it can only launder a failure CI was otherwise able to see.

```
WHEN adding or changing a test:
  Ask: could this interact with other tests through process-global state?
       (a global/static cache, a `set_global_default`, an env var, a
        singleton registry, an ambient `tracing`/`log` subscriber, a shared
        temp path)

  → YES: run plain `cargo test` locally at scale (hundreds of runs of a
         filter that includes the plausible competitors) and say so in the PR.
         A green nextest CI run has NOT examined it.
  → Regression test for such a bug: it must control the interfering
    population, which usually means a child process (re-exec of the test
    binary filtered to that one test) — in-process, whether the bug can
    occur at all depends on what else happens to be running. See
    `crate::util::test_log_capture` for a worked example, including asserting
    the child actually ran a test so a rename fails closed.
```

The fix shape for the `tracing` instance is non-obvious enough to record:
keep **two** permanently-registered dispatchers alive (`tracing_core`'s
`has_just_one` fast path triggers at `live <= 1`, so one is not enough),
report `Interest::sometimes()` rather than `always()` so other subscribers'
per-event filtering is preserved, and do **not** install as the global
default — that slot belongs to `test_log`, and taking it silently stops
`RUST_LOG=… cargo test` printing anything.

## Deliberately breaking code to verify a test: mark it `MUTATION_APPLIED`

A test or a source-scrape pin is not verified until you have watched it go RED
under a deliberate break and GREEN again when the break is reverted. Inspection
is not verification — see the vacuous-pin row in
`.claude/rules/bug-prevention-patterns.md`. So mutating code is a normal,
encouraged part of writing one. These rules are about not leaving the mutation
behind.

### One fixed token

Mark every deliberately-broken line with the exact string `MUTATION_APPLIED` —
not `MUTANT`, not `MUTATION:`, not a comment that merely reads "temporary".
One token, so that `grep -rn MUTATION_APPLIED` is a complete answer.

**The point is not tidiness, it is that the person who greps is usually not the
person who mutated.** On 2026-09-04 four agents hit an account session limit
simultaneously, mid-mutation. The cleanup grepped `MUTANT|MUTATION_APPLIED` and
missed a stranded break in another PR, because that agent had written
`MUTATION:`. Nothing shipped, but only because a human looked twice.

That is the same failure shape as the bug being fixed in the PR that prompted
this rule: a source scanner whose docstring said it skipped "comments" and which
skipped one kind of them. **Searching for one variant of the thing you are
looking for is not searching for the thing.** A convention with variants is a
search with a blind spot; a fixed token is not.

### The rules

1. **Commit or stash BEFORE mutating.** Restoring is then `git checkout --`,
   never retyping from memory. This matters more than the marker: a session can
   die between the break and the restore, and it did.
2. **Mark every broken line** with `MUTATION_APPLIED`.
3. **Before committing, `grep -rn MUTATION_APPLIED` and confirm it is empty.**
   Better, where you have a pre-mutation commit: confirm `git diff` against it
   shows only the change you intend.
4. **When cleaning up after someone else's dead session, do not rely on the
   marker.** Grep for it, AND diff every dirty worktree against its HEAD. A
   mutation applied by editing an existing line leaves no marker at all if its
   author forgot one, and only the diff catches that.
5. **Say which mutation in the commit message**, and say what went red. "Verified
   by inspection" is not verification. Both directions are worth stating when the
   fix is to a shared helper: that the bug turns the guard red, and that reverting
   only the fix — with the bug still in place — turns it green again, is what
   distinguishes a guard that works from one that happens to be passing.


**A mutation harness leaves the tree dirty, and that is its own hazard.** The
check above requires editing the source to break it, so for the duration of a
run the worktree holds a deliberately broken line. During one such run
`git status` showed `contract.rs` modified and a routine `git add -u` would
have committed a stubbed-out call — the very line the branch existed to add —
under a commit message about documentation. This is the cross-worktree
contamination shape from `never-trust-cwd-with-parallel-agents.md`, arriving
from your OWN tooling, where the usual guard ("am I in someone else's tree?")
does not fire because you are in yours.

```
WHEN writing a mutation/falsification script:
  → restore from git in an EXIT trap, not at the end of the happy path
  → write a marker file (e.g. `.mutation-in-progress`) before the first
    mutation and remove it in the SAME trap, so the two cannot disagree
  → stage with an explicit pathspec while any run may be live, never
    `git add -u`/`-A`, and confirm with `git show --stat` afterwards

A SIGKILL then leaves the marker beside the dirty tree, which is the right way
round: the loud state is the unsafe one.
```

## Reference Patterns

**Time injection:**
```rust
fn new(time_source: impl TimeSource) -> Self { ... }
```
See: `crates/core/src/ring/hosting/cache.rs`

**RNG usage:**
```rust
GlobalRng::random_u64()
GlobalRng::fill_bytes(&mut buf)
```
See: `crates/core/src/ring/location.rs:68`

**Simulation socket:**
```rust
SimulationSocket::bind(addr).await
```
See: `crates/core/src/transport/in_memory_socket.rs`

## Choosing a Simulation Runner

```
Need mid-simulation fault injection (partitions, crashes, churn)?
  → Partitions/targeted crashes: Use Turmoil runner (.run() / run_simulation())
  → Node churn (random crash/recover): Use direct runner with ChurnConfig
  → Neither: Continue...

Scale > 50 nodes or virtual time > 5 minutes?
  → YES: Use direct runner (.run_direct() / run_simulation_direct())
  → NO: Either runner works; prefer direct for 100% determinism
```

- **Direct runner** (`run_simulation_direct`): Single `current_thread` + `start_paused(true)` tokio runtime. 100% deterministic. Scales to 500+ nodes. Supports `ChurnConfig` for automated crash/recover cycles via fault injection. Used by fdev CLI and nightly tests.
- **Turmoil runner** (`run_simulation`): Turmoil scheduler. ~99% deterministic. Supports mid-simulation fault injection via closures (partitions, targeted crashes). Better for fine-grained fault tolerance tests.

## Fault Injection in Turmoil Tests

When testing fault tolerance scenarios with `run_simulation()`:

```rust
// 1. Capture addresses BEFORE run_simulation consumes self
let node_addrs = sim.all_node_addresses().clone();
let network_name = "my-test".to_string();

// 2. Inject faults from the test closure via global registry
let result = sim.run_simulation::<SmallRng, _, _>(
    SEED, contracts, iterations, duration, event_wait,
    move || async move {
        if let Some(inj) = freenet::dev_tool::get_fault_injector(&network_name) {
            let mut state = inj.lock().unwrap();
            state.config.crash_node(addr);       // or add_partition, etc.
        }
        // ... wait, then recover ...
        Ok(())
    },
);
```

**Key:** Use `iterations >= num_peers * 15` to ensure enough `gen_event`
budget for contract creation (the `iterations` parameter controls both
event signal count and per-peer generation budget).

See: `crates/core/tests/simulation_integration.rs` — `test_partition_heal_convergence`,
`test_crash_recover_convergence`, `test_multi_step_churn`

## Simulation Test Realism

```
WHEN writing a simulation test for connection/topology behavior:
  → Use realistic parameters, not minimal ones

WRONG:
  5 nodes, min_connections=3, 5 virtual minutes
  // Too small to detect growth ceilings or compounding bugs

CORRECT:
  50 nodes, min_connections=10, 1 virtual hour
  // Exercises realistic topology formation and exposes plateau behavior

WHEN asserting on connection counts:
  → Assert against min_connections, not arbitrary low thresholds
  → Assert a high percentage (>=90%) of nodes reach min_connections
  → Include a fault injection phase to verify no death spiral

WHY: Small topologies with low thresholds masked a 9-month bug where
nodes plateaued at 4-9 connections. The stall only manifests clearly
with min_connections significantly above hardcoded internal thresholds
and enough nodes + time for the growth ceiling to appear.

See: test_connection_growth_stall_regression in simulation_integration.rs
```

## Anomaly Detection

After any simulation test, use `StateVerifier` to check for consistency anomalies:

```rust
let report = rt.block_on(async {
    let logs = logs_handle.lock().await;
    let verifier = freenet::tracing::StateVerifier::from_events(logs.clone());
    verifier.verify()
});
// Check: report.anomalies, report.divergences(), report.stale_peers(), etc.
```

Or chain `.verify_state_report()` on `TestResult` for non-asserting anomaly logging.

Common findings: `StateOscillation` (dominant), `StalePeer` during faults,
`FinalDivergence = 0` (network self-heals).

See: `crates/core/src/tracing/state_verifier.rs`

## Determinism Tests

Determinism tests run the same simulation multiple times with an identical seed
and assert that every run produces the exact same event trace. They rely on
nextest's per-process isolation to guarantee clean global state.

### Rules

```
WHEN writing or modifying a determinism test:

1. Each sequential run MUST use a unique network name
   (e.g., "test-run1", "test-run2") so per-network cleanup works.

2. Call setup_deterministic_state(seed) at the start of each run.

3. SimNetwork::Drop handles per-network global state cleanup.
   Do NOT call clear_all_* functions — they break concurrent tests.

4. Use TraceFingerprint for hash-based cross-run verification
   in addition to field-by-field assertions.

5. nextest runs each test in its own process, so DashMap state
   from other tests cannot leak into determinism comparisons.
```

### Running determinism tests

```bash
# With nextest (recommended — per-process isolation):
cargo nextest run -p freenet --no-default-features \
  --features trace,websocket,redb,wasmtime-backend,simulation_tests,testing \
  -E 'test(determinism)'

# With cargo test (legacy — requires single-threaded execution):
cargo test -p freenet --features "simulation_tests,testing" \
  --test simulation_integration -- --test-threads=1 determinism
```

See: `crates/core/tests/simulation_integration.rs` — `test_strict_determinism_*`,
`test_turmoil_determinism_*`, `test_deterministic_replay_*`, `test_determinism_parallel_safe`,
`test_direct_runner_determinism` (direct runner, 3-run comparison with EventKey verification)
