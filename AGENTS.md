# Freenet Core – Agent Guide

@~/.claude/freenet-local.md

## Project Overview

Freenet Core is the peer-to-peer runtime for the Freenet ecosystem. It provides the networking stack, contract execution environment, and developer tooling.

## Repository Structure

```
freenet-core/
├── crates/
│   ├── core/           # Main runtime (node, transport, contracts, operations)
│   ├── fdev/           # Developer CLI tool
│   └── freenet-macros/ # Test infrastructure macros
├── apps/               # Example applications (ping, email, microblogging)
├── tests/              # Integration test contracts and delegates
├── docs/architecture/  # Design documentation
├── scripts/            # CI and helper scripts
└── network-monitor/    # Monitoring dashboard (TypeScript/React)
```

## Essential Commands

```bash
# Setup
git submodule update --init --recursive

# Build & Test
cargo build
cargo test
cargo fmt
cargo clippy --all-targets --all-features

# Install binaries
cargo install --path crates/core   # freenet binary
cargo install --path crates/fdev   # fdev CLI
```

## Testing

See `docs/architecture/testing/README.md` for full testing guide.

**Quick reference:**
```bash
cargo test -p freenet                                # All tests
cargo test -p freenet --test simulation_integration  # SimNetwork tests
cargo test -p freenet -- --test-threads=1            # Deterministic mode
```

**Key rules:**
- Use `TODO-MUST-FIX:` marker when disabling tests temporarily
- Never remove failing tests without understanding root cause
- See `.claude/rules/testing.md` for DST guidelines

## Pull Requests

**Title format:** Conventional Commits (`feat:`, `fix:`, `docs:`, etc.)

**Description must explain WHY, not just WHAT:**
```markdown
## Problem
[Bug/issue and user impact]

## Solution
[Key insight and why this approach]

## Testing
[What tests validate this]

## Fixes
Closes #XXXX
```

See `.claude/rules/git-workflow.md` for details.

## Architecture

### Core Components (`crates/core/src/`)

| Module | Purpose |
|--------|---------|
| `node/` | Central event loop, coordination |
| `operations/` | State machines (GET, PUT, UPDATE, SUBSCRIBE, CONNECT) |
| `contract/` | WASM execution, state management |
| `transport/` | UDP networking, encryption, congestion control |
| `ring/` | DHT ring topology, peer routing |
| `server/` | WebSocket API, HTTP gateway |
| `simulation/` | Deterministic simulation framework |

### Data Flow

```
Client → WebSocket API → Node Event Loop → Operations
                              ↓
                    Contract Handler ←→ Network Bridge
                              ↓
                    Transport Layer (UDP)
```

### Key Abstractions

| Concern | Abstraction | Location |
|---------|-------------|----------|
| Time | `TimeSource` trait | `crates/core/src/simulation/` |
| RNG | `GlobalRng` | `crates/core/src/config/mod.rs` |
| Sockets | `Socket` trait | `crates/core/src/transport/` |

### Documentation

- **Transport:** `docs/architecture/transport/README.md`
- **Testing:** `docs/architecture/testing/README.md`
- **Architecture:** `docs/architecture/README.md`

## AI Agent Skills

Skills from [freenet-agent-skills](https://github.com/freenet/freenet-agent-skills):

- **dapp-builder** – Building decentralized applications
- **pr-creation** – PR guidelines and review methodology
- **systematic-debugging** – Hypothesis-driven debugging

## Additional Resources

- `README.md` – Build instructions
- https://docs.rs/freenet – API documentation
- https://freenet.org/resources/manual/ – User manual
- Matrix channels linked in README.md for questions
