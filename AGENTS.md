# Freenet Core – Agent Guide

@~/.claude/freenet-local.md

## Behavioral Rules

### BEFORE modifying any file

```
1. Is this in crates/core/?
   → Check .claude/rules/testing.md for DST requirements
   → Verify: TimeSource for time, GlobalRng for randomness

2. Is this Rust code?
   → Check .claude/rules/code-style.md
```

### BEFORE committing

```
1. Run: cargo fmt && cargo clippy --all-targets --all-features && cargo test
2. Check: Does commit message follow conventional commits?
3. Check .claude/rules/git-workflow.md for PR requirements
```

### WHEN a test fails

```
DO NOT delete or comment out the test.
→ Add #[ignore] with // TODO-MUST-FIX: [reason] #[issue]
→ Create GitHub issue
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
cargo test -p freenet -- --test-threads=1  # Deterministic mode
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

| Topic | Location |
|-------|----------|
| Architecture | `docs/architecture/README.md` |
| Ring/DHT | `docs/architecture/ring/README.md` |
| Operations | `docs/architecture/operations/README.md` |
| Transport | `docs/architecture/transport/README.md` |
| Testing | `docs/architecture/testing/README.md` |
| Code style | `.claude/rules/code-style.md` |
| Git workflow | `.claude/rules/git-workflow.md` |
| DST rules | `.claude/rules/testing.md` |

## External Resources

- API docs: https://docs.rs/freenet
- Manual: https://freenet.org/resources/manual/
