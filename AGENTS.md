# Freenet Core – Agent Guide

@~/.claude/freenet-local.md

## Behavioral Rules

### BEFORE modifying any file

```
1. Is this in crates/core/?
   → Check .claude/rules/testing.md for DST requirements
   → Verify: TimeSource for time, GlobalRng for randomness

2. Which module are you modifying?
   → ring/router/     → Check .claude/rules/ring.md
   → operations/      → Check .claude/rules/operations.md
   → transport/       → Check .claude/rules/transport.md
   → contract/wasm_runtime/ → Check .claude/rules/contracts.md

3. Is this Rust code?
   → Check .claude/rules/code-style.md
```

### BEFORE committing

```
1. Run: cargo fmt && cargo clippy --all-targets && cargo test
2. Check: Does commit message follow conventional commits?
3. Check .claude/rules/git-workflow.md for PR requirements
```

### WHEN a test fails

```
DO NOT delete or comment out the test.
→ Add #[ignore] with // TODO-MUST-FIX: [reason] #[issue]
→ Create GitHub issue
```

### WHEN writing cleanup/GC logic

```
Cleanup exemptions MUST be time-bounded.

Any condition that exempts an entry from garbage collection
(is_transient, has_pending, etc.) MUST either:
  1. Expire via TTL, OR
  2. Be overridden by an absolute age threshold

WHY: Unbounded exemptions create permanent GC blind spots.
This is a recurring meta-pattern where a fix introduces cleanup with
exemptions, then a follow-up discovers the exemptions themselves are
buggy (permanently refreshable, missing TTL enforcement).
Exemptions in GC deserve the same scrutiny as the original bug.

See: docs/weekly-fix-review-2025-02.md (befb0bd → 0b88945 cycle)
```

### WHEN adding channels, queues, or collection storage

```
NEVER use unbounded channels or unbounded per-key collections
for data that external actors (clients, network peers) can influence.

1. Channels carrying notifications, responses, or events MUST be bounded.
   → Use tokio::sync::mpsc::channel(N), NOT unbounded_channel()
   → Use try_send() in non-async contexts to avoid blocking executors
   → Drop messages when full (lossy) rather than blocking or growing

2. Per-key collections (subscribers per contract, peers per resource)
   MUST have a maximum size enforced at insertion time.
   → Reject new entries when the limit is reached
   → Return an error or false so callers know registration was rejected

3. Per-client/per-peer resource counts MUST be bounded.
   → A single client must not hold unbounded subscriptions across all keys
   → A single peer must not register unbounded interest across all contracts

4. Fan-out patterns (one event → N recipients) MUST cap the cost.
   → Cap expensive per-recipient work (e.g., WASM calls) to a fixed limit
   → Fall back to cheaper alternatives (e.g., full state vs computed delta)
   → Log warnings when fan-out exceeds a threshold

WHY: Unbounded queues and collections are amplification vectors.
An attacker who can register N subscribers or open N channels can
multiply the cost of every state update by N, exhausting memory,
CPU (WASM execution), and executor time. Bounded channels provide
backpressure; per-key caps prevent amplification; per-client caps
prevent resource spreading attacks.

See: executor.rs constants (MAX_SUBSCRIBERS_PER_CONTRACT,
SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE, MAX_DELTA_COMPUTATIONS_PER_FANOUT,
MAX_SUBSCRIPTIONS_PER_CLIENT) and hosting.rs (MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT)
```

### WHEN you discover outdated or missing documentation

```
1. Is information in AGENTS.md, .claude/rules/, or CLAUDE.md incorrect?
   → Fix it immediately in the same commit or PR

2. Did you learn something important that would help future work?
   → Add it to the appropriate file:
     - Project-wide patterns → AGENTS.md
     - Code conventions → .claude/rules/code-style.md
     - Testing patterns → .claude/rules/testing.md
     - Git/PR workflow → .claude/rules/git-workflow.md
     - Crate-specific → crates/*/CLAUDE.md

3. Is a file reference (line number, path) stale?
   → Update or remove it
```

## Quick Reference

### Commands

```bash
cargo build                    # Build
cargo test -p freenet          # Test all
cargo fmt && cargo clippy --all-targets --all-features  # Lint
```

### Repository Structure

```
crates/
├── core/           # Runtime (node, transport, contracts, operations)
├── fdev/           # Developer CLI
└── freenet-macros/ # Test macros
apps/               # Example applications
docs/architecture/  # Design docs
```

### Core Modules (`crates/core/src/`)

| Module | Purpose |
|--------|---------|
| `node/` | Event loop, coordination |
| `operations/` | State machines (GET, PUT, UPDATE, SUBSCRIBE, CONNECT) |
| `contract/` | WASM execution |
| `transport/` | UDP networking, encryption |
| `ring/` | DHT topology |
| `simulation/` | DST framework |

### Key Abstractions

| Need | Use | Location |
|------|-----|----------|
| Time | `TimeSource` | `crates/core/src/simulation/` |
| RNG | `GlobalRng` | `crates/core/src/config/mod.rs` |
| Sockets | `Socket` trait | `crates/core/src/transport/` |

## Documentation

### Architecture Docs

| Topic | Location |
|-------|----------|
| Architecture | `docs/architecture/README.md` |
| Ring/DHT | `docs/architecture/ring/README.md` |
| Operations | `docs/architecture/operations/README.md` |
| Transport | `docs/architecture/transport/README.md` |
| Testing | `docs/architecture/testing/README.md` |

### Module Rules (path-scoped)

| Module | Rules |
|--------|-------|
| Ring/Router | `.claude/rules/ring.md` |
| Operations | `.claude/rules/operations.md` |
| Transport | `.claude/rules/transport.md` |
| Contracts | `.claude/rules/contracts.md` |

### General Rules

| Topic | Location |
|-------|----------|
| Code style | `.claude/rules/code-style.md` |
| Git workflow | `.claude/rules/git-workflow.md` |
| DST testing | `.claude/rules/testing.md` |
| Deployment | `.claude/rules/deployment.md` |

## External Resources

- API docs: https://docs.rs/freenet
- Manual: https://freenet.org/resources/manual/
