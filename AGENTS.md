# Freenet Core – Agent Guide

## Project Overview
Freenet Core is the peer-to-peer runtime that underpins applications in the Freenet ecosystem. The crates in this workspace implement the networking stack, contract execution environment, and developer tooling used by higher-level projects such as River.

## Repository Layout
- `crates/` – core libraries, binaries, and developer tooling (`core`, `gateway`, `fdev`, etc.)
- `apps/` – integration binaries (benchmarks, diagnostic tools)
- `docs/` – design notes and protocol documentation
- `scripts/` – helper scripts used in CI and local workflows
- `tests/` – end-to-end and integration test suites

Refer to `README.md` for a more detailed component map.

## Working with Git Worktrees
- Keep a checkout on the `main` branch (for example, this directory) and create per-branch worktrees as siblings:  
  ```bash
  git worktree add ../my-feature-branch feature/my-feature-branch
  ```
- Run `git worktree list` to see active worktrees and `git worktree remove ../my-feature-branch` when a branch merges.
- Avoid committing from the `main` checkout; perform branch work inside the corresponding worktree directory to prevent conflicts between contributors.

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

### Gateway Test Framework
Integration testing is performed with the `freenet-testing-tools` repository (specifically `gateway-testing/`):
```bash
python gateway_test_framework.py --local             # Smoke test against local build
python gateway_test_framework.py --version v0.1.19   # Regression against a release
python gateway_test_framework.py --version pr:123    # Validate a pull request
```
Results land in `gateway-testing/results/<user>/<date>/` and include markdown summaries and compressed logs. Use the `--extended-stability`, `--multi-room`, or `--debug-locations` flags for deeper investigations.

## Pull Requests & Reviews
- All PR titles must follow Conventional Commits (`feat:`, `fix:`, `docs:`, etc.). CI fails non-conforming titles.
- Substantial changes require review from another developer before merging.
- Prefer stacked PRs for large efforts; rebase dependent branches after feedback.

## Additional Resources
- `PRE_COMMIT_HOOK_GUIDE.md` – configures local linting hooks.
- `README.md` – high-level introduction and build instructions.
- https://docs.rs/freenet – API documentation for published crates.
- https://freenet.org/resources/manual/ – end-user manual explaining contracts, delegates, and network operation.

Questions or blockers should be raised in the Freenet Matrix channels or GitHub discussions linked from `README.md`.
