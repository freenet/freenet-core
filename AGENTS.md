# Freenet Core – Agent Guide

## Project Overview
Freenet Core is the peer-to-peer runtime that underpins applications in the Freenet ecosystem. The crates in this workspace implement the networking stack, contract execution environment, and developer tooling used by higher-level projects such as River.

## ⚠️ CRITICAL: Git Worktree Requirement

**BEFORE STARTING ANY WORK, verify your working directory:**

```bash
pwd  # Must be ~/code/freenet/freenet-core/<branch-name>
     # NOT ~/code/freenet/freenet-core/main
git branch --show-current  # Should be your feature branch, NOT 'main'
```

### ❌ DO NOT work in ~/code/freenet/freenet-core/main

The main worktree should **always** remain on the `main` branch. Multiple agents working in main will conflict and corrupt branches.

### ✅ Creating a Worktree for Your Branch

1. Create a worktree as a sibling directory:
   ```bash
   cd ~/code/freenet/freenet-core/main
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

```bash
# 1. Create/verify worktree
cd ~/code/freenet/freenet-core/main
git worktree add ../fix-ISSUE-NUMBER branch-name

# 2. Create zellij tab (NO --layout flag!)
zellij action new-tab --name codex-iISSUE-description

# 3. Switch to mcp tab to avoid input corruption
zellij action go-to-tab-name mcp

# 4. Start agent with correct params
~/code/mcp/skills/zellij-agent-manager/scripts/send-to-agent.sh \
  codex-iISSUE-description \
  "cd ~/code/freenet/freenet-core/fix-ISSUE-NUMBER && codex -s danger-full-access -a never"

# 5. Wait for agent to start, then send task
sleep 3
~/code/mcp/skills/zellij-agent-manager/scripts/send-to-agent.sh \
  codex-iISSUE-description \
  "Your task description here [AI-assisted - Claude]"
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
- See `docs/TESTING.md` for mandatory scenarios and expectations.
- The repository uses the special `TODO-` `MUST-FIX` marker to block commits that temporarily disable tests. If a test must be skipped, leave a `// TODO-` `MUST-FIX:` comment explaining why and create a follow-up issue.
- Never remove or ignore failing tests without understanding the root cause.

### Integration Testing with `freenet-test-network`
- Use the `freenet-test-network` crate located at `~/code/freenet/freenet-test-network` to spin up gateways and peers for integration tests.
- Add it as a dev-dependency in your worktree (`freenet-test-network = { path = "../freenet-test-network" }`) and construct networks with the builder API.
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

## Additional Resources
- `PRE_COMMIT_HOOK_GUIDE.md` – configures local linting hooks.
- `README.md` – high-level introduction and build instructions.
- https://docs.rs/freenet – API documentation for published crates.
- https://freenet.org/resources/manual/ – end-user manual explaining contracts, delegates, and network operation.

Questions or blockers should be raised in the Freenet Matrix channels or GitHub discussions linked from `README.md`.
