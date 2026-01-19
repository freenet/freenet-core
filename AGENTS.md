# Freenet Core – Agent Guide

@~/.claude/freenet-local.md

## Project Overview
Freenet Core is the peer-to-peer runtime that underpins applications in the Freenet ecosystem. The crates in this workspace implement the networking stack, contract execution environment, and developer tooling used by higher-level projects such as River.

## ⚠️ CRITICAL: Git Worktree Requirement

**BEFORE STARTING ANY WORK, verify your working directory:**

```bash
pwd  # Must be <repo-root>/<branch-name>
     # NOT <repo-root>/main
git branch --show-current  # Should be your feature branch, NOT 'main'
```

### ❌ DO NOT work in the main worktree directory

The main worktree should **always** remain on the `main` branch. Multiple agents working in main will conflict and corrupt branches.

### ✅ Creating a Worktree for Your Branch

1. Create a worktree as a sibling directory:
   ```bash
   cd <repo-root>/main  # Your main checkout directory
   git worktree add ../fix-<issue-number> <your-branch-name>
   cd ../fix-<issue-number>
   ```

2. Verify you're in the correct location:
   ```bash
   pwd  # Should show .../freenet-core/fix-<issue-number>
   git branch --show-current  # Should show your branch
   ```

3. Now you can safely commit and push from this worktree.

### Worktree Naming Convention

Include the issue or PR number for clarity:
- `fix-2107` for PR #2107
- `issue-2092` for issue #2092
- `fix-i2021-blocked-peers` for issue #2021 with descriptive name

### Managing Worktrees

```bash
git worktree list  # See all active worktrees
git worktree remove ../fix-2107  # Remove when branch merges
```

**After a PR merges, remove the worktree** to free disk space (each worktree has its own `target/` directory consuming 10-50GB).

## ⚠️ SPAWNING SUB-AGENTS (Claude/Codex)

When Claude Code needs to spawn Codex or Claude sub-agents for parallel work, follow this checklist:

### Pre-Flight Checklist

**BEFORE spawning any agent:**

1. ✅ **Verify/create worktree** for the agent's work
2. ✅ **Use correct CLI parameters** (see below)
3. ✅ **Don't use `--layout compact`** when creating tabs (causes UI inconsistency)
4. ✅ **Verify disk space** (needs ~10-50GB for builds)

### Correct Agent Launch Commands

```bash
# Codex (for tough bugs and deep debugging)
codex -s danger-full-access -a never

# Claude Code (for human communication and planning)
claude --permission-mode bypassPermissions
```

**CRITICAL:** Always use these exact parameters. Without them, agents run in restrictive mode and can't execute commands.

### Agent Spawning Template

**Note:** This template assumes you have terminal multiplexer tooling set up. Adapt paths and commands to your local environment.

```bash
# 1. Create/verify worktree
cd <repo-root>/main
git worktree add ../fix-ISSUE-NUMBER branch-name

# 2. Start agent in new terminal/tab
# Navigate to worktree and launch with correct parameters
cd <repo-root>/fix-ISSUE-NUMBER
codex -s danger-full-access -a never
# OR: claude --permission-mode bypassPermissions

# 3. Send task to agent
# (Use your terminal automation/multiplexer tooling)
# Example task: "Fix issue #XXXX - [description]. [AI-assisted - Claude]"
```

### Common Pitfalls

- ❌ **Spawning agents in main worktree** → Creates branch conflicts
- ❌ **Using restrictive permission modes** → Agent can't execute commands
- ❌ **Sending messages while agent is executing** → Corrupts input buffer
- ❌ **Complex bash with subshells** → Use temp scripts instead
- ❌ **Using `--layout compact`** → Causes tab bar position inconsistency
- ❌ **Not verifying disk space** → Builds fail with cryptic errors

## Repository Layout
- `crates/` – core libraries, binaries, and developer tooling (`core`, `gateway`, `fdev`, etc.)
- `apps/` – integration binaries (benchmarks, diagnostic tools)
- `docs/` – design notes and protocol documentation
- `scripts/` – helper scripts used in CI and local workflows
- `tests/` – end-to-end and integration test suites

Refer to `README.md` for a more detailed component map.

## Bootstrapping & Tooling
```bash
git submodule update --init --recursive
cargo install --path crates/core   # Provides the `freenet` binary
cargo install --path crates/fdev   # Utility CLI used in development
```

## Common Commands
```bash
cargo build
cargo test
cargo fmt
cargo clippy --all-targets --all-features
```
Run these in any worktree before pushing a branch or opening a PR.

## Testing Guidance
- See `docs/architecture/testing/README.md` and `docs/architecture/testing/simulation-testing.md` for detailed testing documentation and best practices.
- The repository uses the special `TODO-` `MUST-FIX` marker to block commits that temporarily disable tests. If a test must be skipped, leave a `// TODO-` `MUST-FIX:` comment explaining why and create a follow-up issue.
- Never remove or ignore failing tests without understanding the root cause.

### Deterministic Simulation Testing (DST) Guidelines

The codebase uses deterministic simulation testing to ensure reproducible test results. When working with simulation code or tests in `crates/core`, follow these guidelines:

#### ❌ DON'T: Use Non-Deterministic Time Sources

**Avoid:**
- `std::time::Instant::now()` - uses real wall-clock time
- `tokio::time::sleep()` - uses tokio's real-time scheduler
- `tokio::time::timeout()` - depends on real time progression

#### ✅ DO: Use VirtualTime and TimeSource

**For time operations in components:**
- Use `TimeSource` trait for dependency injection
- **Example:** `crates/core/src/ring/seeding_cache.rs:76` - TimeSource injection in constructor
- **Example:** `crates/core/src/transport/peer_connection.rs:1674` - VirtualTime initialization for testing

**For time advancement in simulation tests:**
- **See:** `crates/core/tests/simulation_smoke.rs:29-42` - `let_network_run()` function
- **Pattern:** Call `sim.advance_time(step)` + `tokio::task::yield_now().await` in a loop

#### ❌ DON'T: Use Non-Deterministic Random Sources

**Avoid:**
- `rand::random()` - uses system entropy, non-deterministic
- `rand::thread_rng()` - creates unseeded RNG

#### ✅ DO: Use GlobalRng

**API Methods:**
- `GlobalRng::random_u64()`, `random_range()` - generate random values
- `GlobalRng::fill_bytes()` - fill byte arrays with random data
- `GlobalRng::with_rng()` - complex RNG operations with closure

**Examples in codebase:**
- `crates/core/src/ring/location.rs:68` - `random_range()` for random locations
- `crates/core/src/transport/peer_connection.rs:1667-1669` - `fill_bytes()` for crypto keys
- `crates/core/src/topology/small_world_rand.rs:11-14` - `with_rng()` for complex distributions

#### ❌ DON'T: Use Real Network I/O in Simulation

**Avoid:**
- `tokio::net::UdpSocket` - uses real network I/O, not deterministic

#### ✅ DO: Use SimulationSocket

**For simulation tests:**
- Use `SimulationSocket` for in-memory, deterministic networking
- **See:** `crates/core/src/transport/in_memory_socket.rs` - Full SimulationSocket implementation
- **Pattern:** `SimulationSocket::bind(addr).await` - Same API as UdpSocket

#### Key Abstractions

| Concern | Abstraction | Location |
|---------|-------------|----------|
| Time | `TimeSource` trait, `VirtualTime`, `RealTime` | `crates/core/src/simulation/` |
| RNG | `GlobalRng`, `GlobalSimulationTime` | `crates/core/src/config/mod.rs` |
| Sockets | `Socket` trait, `SimulationSocket` | `crates/core/src/transport/` |
| Async execution | `GlobalExecutor` | `crates/core/src/config/mod.rs` |

#### Test Pattern: Advancing Virtual Time

**For simulation tests**, use explicit VirtualTime advancement to control time progression.

**Complete example:**
- **See:** `crates/core/tests/simulation_smoke.rs:29-42` - `let_network_run()` helper function
- **See:** `crates/core/tests/simulation_integration.rs` - Full test suite with VirtualTime

**Pattern:**
1. Create `SimNetwork` (provides VirtualTime)
2. Loop: `sim.advance_time(step)` + `tokio::task::yield_now().await`
3. Optionally add small real sleep (1ms) for task scheduling

**Important:** Simulation tests use `SimNetwork::advance_time()` for deterministic time control. This works with Turmoil's deterministic scheduler and is independent of tokio's internal time handling.

**Note:** Raw tokio tests can use `#[tokio::test(start_paused = true)]`, but simulation tests should use explicit VirtualTime control for full determinism.

#### GlobalSimulationTime: Deterministic Timestamps

For deterministic ULID generation and timestamps in simulation tests.

**API Methods:**
- `GlobalSimulationTime::set_time_ms(ms)` - Set base simulation time
- `GlobalSimulationTime::new_ulid()` - Generate deterministic ULID
- `GlobalSimulationTime::current_time_ms()` - Get current simulation time

**Examples in codebase:**
- `crates/core/src/node/testing_impl.rs:2769-2776` - Seed-based time initialization in `run_simulation()`
- `crates/core/src/message.rs:50` - `new_ulid()` usage in Transaction::new()

**Note:** `GlobalSimulationTime` is automatically set by `SimNetwork::run_simulation()` based on the provided seed.

#### TimeSource Behavioral Differences

The `TimeSource` trait has methods with different behaviors in simulation vs. real time.

**Key differences:**

| Method | VirtualTime | RealTime | Reason |
|--------|-------------|----------|--------|
| `connection_idle_timeout()` | 24 hours | 120 seconds | Prevent premature timeout during auto-advance |
| `supports_keepalive()` | `false` | `true` | Prevent hangs with auto-advance interactions |

**Implementation:**
- `crates/core/src/simulation/time.rs:127-129` - RealTime implementation
- `crates/core/src/simulation/time.rs:507-514` - VirtualTime implementation

**Important:** Components using TimeSource should be aware these values differ between simulation and production environments.

### Integration Testing with `freenet-test-network`
- Use the `freenet-test-network` crate from https://github.com/freenet/freenet-test-network to spin up gateways and peers for integration tests.
- Add it as a dev-dependency using either a path (if cloned locally) or git dependency, and construct networks with the builder API.
- Sample pattern:
  ```rust
  use freenet_test_network::TestNetwork;
  use std::sync::LazyLock;

  static NETWORK: LazyLock<TestNetwork> = LazyLock::new(|| {
      TestNetwork::builder()
          .gateways(1)
          .peers(5)
          .build_sync()
          .expect("start test network")
  });
  ```
- Tests can share the static network and access `NETWORK.gateway(0).ws_url()` to communicate via `freenet_stdlib::client_api::WebApi`.
- Run the crate’s suite with `cargo test -p freenet-test-network`. When `preserve_temp_dirs_on_failure(true)` is set, failing startups keep logs under `/tmp/freenet-test-network-<timestamp>/` for inspection.
- A larger soak test lives in `crates/core/tests/large_network.rs`. It is `#[ignore]` by default—run it manually with `cargo test -p freenet --test large_network -- --ignored --nocapture` once you have `riverctl` installed. The test writes diagnostics snapshots to the network’s `run_root()` directory for later analysis.

## Pull Requests & Reviews

### PR Title Format
- All PR titles must follow Conventional Commits (`feat:`, `fix:`, `docs:`, etc.). CI fails non-conforming titles.
- Substantial changes require review from another developer before merging.
- Prefer stacked PRs for large efforts; rebase dependent branches after feedback.

### PR Description Requirements

**CRITICAL:** PR descriptions must explain **WHY**, not just **WHAT**.

**Bad PR description (too terse):**
```markdown
## Summary
- Add observed_addr field to ConnectRequest
- Update relay to fill in socket
- Add regression test
```
This lists changes but gives reviewers no context about the problem being solved or the reasoning behind the approach.

**Good PR description (explains context and reasoning):**
```markdown
## Problem
[Describe the bug/issue and its user-visible impact]
[Explain why existing tests didn't catch it]

## Previous Approaches (if applicable)
[Mention rejected approaches and why they were inadequate]

## This Solution
[Explain the key insight or design decision]
[For each major change, explain WHY it's needed, not just what it does]

## Testing
[New tests added and what scenarios they validate]
[Local validation steps you performed]

## Fixes
Closes #XXXX
```

**Key principles for reviewable PRs:**
1. **Start with the problem** – What's broken? What's the user impact? Why does it matter?
2. **Explain the approach** – Why this solution over alternatives? What's the key insight?
3. **Connect changes to reasoning** – Don't just list what changed, explain why each change solves part of the problem
4. **Call out lessons learned** – "Why CI didn't catch this", "Why approach X was rejected", "What assumption was wrong"
5. **Make it self-contained** – Reviewer should understand your thinking without having to read the entire issue thread

**Example of good reasoning:**
> "The joiner can't know its public address until someone observes it from the outside. Previous approach (PR #2088) tried rewriting addresses at transport boundary, but that's a hack—we shouldn't need generic address mutation. This PR lets the gateway fill in the observed socket naturally since it already sees the real UDP source."

This explains the constraint (joiner doesn't know its address), the rejected approach (and why), and the insight that makes the new approach clean.

**When writing PRs:**
- Imagine the reviewer knows the codebase but hasn't followed this issue
- Explain your reasoning process, not just your conclusions
- If you debugged for hours before finding the root cause, share that journey briefly
- Link to related issues/PRs and explain the relationship

After creating a PR with `gh pr create`, **immediately review the description** and ask yourself: "Could someone understand why these changes are necessary without reading the issue or digging through code?" If not, use `gh pr edit <number> --body "..."` to improve it before requesting review.

## AI Agent Skills

This repository is configured to use [freenet-agent-skills](https://github.com/freenet/freenet-agent-skills) which provides specialized skills for Freenet development:

- **dapp-builder** – Building decentralized applications (contracts, delegates, UI)
- **pr-creation** – PR guidelines and multi-perspective review methodology
- **systematic-debugging** – Hypothesis-driven debugging for complex issues

### Claude Code

Skills are automatically available via the marketplace. When you trust this repository, Claude Code will prompt to install the `freenet-agent-skills` marketplace with plugins enabled.

Configuration: `.claude/settings.json`

### OpenCode

The `opencode.json` configures the `opencode-agent-skills` plugin for skill discovery and pre-approves freenet skills. However, skills must be installed locally:

```bash
# Option 1: Using openskills CLI
npm i -g openskills
openskills install freenet/freenet-agent-skills
openskills sync

# Option 2: Manual clone
git clone https://github.com/freenet/freenet-agent-skills ~/.opencode/skills/freenet
# Then symlink individual skills to ~/.opencode/skills/
```

Configuration: `opencode.json`

> **Note:** Once freenet-agent-skills is published as an npm package, OpenCode users will simply add `"freenet-agent-skills"` to their plugins array and skills will load automatically.

## Additional Resources
- `PRE_COMMIT_HOOK_GUIDE.md` – configures local linting hooks.
- `README.md` – high-level introduction and build instructions.
- https://docs.rs/freenet – API documentation for published crates.
- https://freenet.org/resources/manual/ – end-user manual explaining contracts, delegates, and network operation.

Questions or blockers should be raised in the Freenet Matrix channels or GitHub discussions linked from `README.md`.
