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
1. Run: cargo fmt && cargo clippy -- -D warnings && cargo test
2. Check: Does commit message follow conventional commits?
3. Check .claude/rules/git-workflow.md for PR requirements
```

### WHEN a test fails

```
DO NOT delete or comment out the test.

Is the test broken/flaky?
  → Add #[ignore] with a comment explaining why
  → Create GitHub issue immediately
  → Reference the issue in the comment: // Ignored: [reason] #[issue]

Is the test superseded by new semantics?
  → Add #[ignore] with a comment explaining the semantic change
  → Reference the PR that changed the behavior
  → Keep the test as historical documentation
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
cargo fmt && cargo clippy -- -D warnings  # Lint (must match CI)
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
| RNG | `GlobalRng` | `crates/core/src/config.rs` |
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
