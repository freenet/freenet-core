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

### WHEN fixing a bug (fix: PRs)

```
EVERY bug fix MUST include a regression test.

1. Write a test that reproduces the bug FIRST
2. Verify it FAILS without the fix
3. Apply the fix
4. Verify the test PASSES

CI enforces this: fix: PRs without new test functions are rejected.

Also: don't just test the happy path. Test edge cases and boundary
conditions. See .claude/rules/testing.md for specifics.
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
├── core/             # Runtime (node, transport, contracts, operations)
├── fdev/             # Developer CLI
├── freenet-macros/   # Test macros
└── release-agent/    # HTTP service on each gateway for triggering
                      # auto-updates from the release workflow (#4073)
apps/                 # Example applications
docs/architecture/    # Design docs
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

## Release Workflow & RELEASE_PAT

The release pipeline (`.github/workflows/release.yml` →
`.github/workflows/cross-compile.yml` → downstream `gateway-update.yml` /
`release-announce.yml`) relies on a `RELEASE_PAT` repo secret to fire
all the workflow events that make releases zero-touch.

### Why a PAT is required

GitHub's `GITHUB_TOKEN` deliberately suppresses workflow-triggering
events as an anti-recursion safeguard:

> When you use the repository's `GITHUB_TOKEN` to perform tasks,
> events triggered by the `GITHUB_TOKEN` will not create a new
> workflow run.

That means any `gh` call inside a workflow that *should* wake up
another workflow has to authenticate with a personal access token
(PAT) instead. Two concrete failure modes hit the v0.2.57 release:

1. **Bump PR has 0 check-runs.** `release.yml` opens the
   `release/vX.Y.Z` PR via `gh pr create`. With `GITHUB_TOKEN`, no
   `pull_request` event fires, so `ci.yml` never runs and the PR's
   required checks never go green. Workaround was
   `gh pr close && gh pr reopen`, which broke `wait_for_pr` polling.

2. **`release.published` doesn't fire downstream workflows.**
   `cross-compile.yml`'s `attach-to-release` job ends with
   `gh release edit --draft=false`. With `GITHUB_TOKEN`, the
   `release.published` event is suppressed, so `gateway-update.yml`
   and `release-announce.yml` don't auto-fire. v0.2.57 had to trigger
   them manually via `workflow_dispatch`.

### Configuring the secret

`RELEASE_PAT` must be a fine-grained (or classic) personal access
token with at minimum:

- **`repo`** (Contents: read/write, Pull requests: read/write,
  Metadata: read) for `gh pr create`, `gh release edit`, and push of
  release branches.
- **`workflow`** required for any token that lands on
  `.github/workflows/`-adjacent code paths.

Add it under
**Settings → Secrets and variables → Actions → Repository secrets**
as `RELEASE_PAT`. Both `release.yml` and `cross-compile.yml`
coalesce on it. The coalesce appears in two forms:

```yaml
# for actions/checkout (controls what `git push` uses later):
- uses: actions/checkout@v6
  with:
    token: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}

# for `gh` CLI steps:
env:
  GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
```

When the secret is missing the workflows degrade gracefully: they
still complete, but the bump-PR CI, the tag-push → cross-compile
trigger, and the release-published cascade won't auto-fire and a
human has to intervene. `release.yml` emits a GitHub `::warning::`
on every run that's missing the secret, so the gap is visible
early.

`.github/workflows/check-token-coalesce.yml` runs on every PR and
fails if any new event-emitting step regresses to bare
`GITHUB_TOKEN` / `github.token` — see issue #4118 for the regression
class this guard prevents.

### Maintenance

The PAT belongs to a maintainer's account (currently @sanity). Rotate
it on the GitHub PAT expiry schedule, and update the `RELEASE_PAT`
secret in the freenet-core repo when you do. If `release.yml` prints
the "RELEASE_PAT not set" warning unexpectedly, the secret has
expired and the same rotation procedure applies.

### Cutting a release

End-to-end procedure for any maintainer is in
[`docs/RELEASING.md`](docs/RELEASING.md). One-line summary:
`gh workflow run release.yml --field version=X.Y.Z`. The `RELEASE_PAT`
section above is the prerequisite for the cascade to fire automatically.

## Delegate secrets-at-rest

Server-side delegate secret encryption evolved across PRs #4143 / #4144
/ #4146 (tracker #4137). The current model:

  node KEK (32 bytes, in OS keyring / systemd cred / file)
    └── per-delegate DEK = HKDF-SHA256(KEK, salt=delegate_key.encode(),
                                            info="freenet-delegate-dek-v1")
          └── per-write random nonce (24 bytes, OsRng — PR #4143)

The KEK is provisioned on first start by the resolver in
`crates/core/src/config/kek.rs`, which tries OS keyring → systemd
credential → file backend in order and persists the choice in
`secrets_dir/kek_backend`. Subsequent starts load STRICTLY from the
recorded backend — transient keyring outage is a hard error, never a
silent demotion.

### Operator migration matrix

| Source release           | Cipher source on disk                  | Status after PR #4146 |
|--------------------------|----------------------------------------|------------------------|
| ≤ 0.2.58 (pre-#4143)     | `LEGACY_DEFAULT_CIPHER` (world-known)  | Decryptable via legacy-migration tier. |
| 0.2.59 (#4143)           | `LEGACY_DEFAULT_CIPHER`, versioned     | Decryptable via legacy-migration tier. |
| 0.2.60+ (#4144)          | per-install random `delegate_cipher`   | Decryptable via `default_encryption` (legacy_chain[0]). |
| post-#4146               | HKDF-derived DEK from node KEK         | Default path. |

### BREAKING semantic change introduced by #4146

`RegisterDelegate { cipher, nonce }` server-side semantics changed:
client-supplied `cipher` and `nonce` are now IGNORED (logged at INFO).
The wire format is unchanged so older clients continue to function,
but **any prior deployment that wrote secrets under a client-supplied
non-default cipher will lose access to those blobs** unless that
cipher coincidentally matches the new HKDF-derived DEK (it won't).

Deployments that only ever used `DelegateRequest::DEFAULT_CIPHER` or
the per-install auto-generated cipher are covered by the legacy chain
and migrate transparently.

If you ran any client that explicitly constructed `RegisterDelegate {
cipher: custom, .. }` against a freenet-core ≤ 0.2.60 node, restore
the secrets dir from a pre-upgrade backup, decrypt with the custom
cipher manually (the on-disk format is documented in
`crates/core/src/wasm_runtime/secrets_store.rs::decrypt_secret_blob`),
and re-upload via the post-#4146 client API. The HKDF derivation will
produce a new DEK and the secrets will be re-encrypted under it on
first write.

### `fdev secrets` CLI

```
fdev secrets kek-status   --secrets-dir <path>
fdev secrets kek-migrate  --secrets-dir <path> --to {keyring|systemd|file} --yes
fdev secrets kek-rotate   --secrets-dir <path> --yes   # NOT YET IMPLEMENTED (#4137)
```

Node MUST be stopped before running migrate. `kek-rotate` currently
bails with a clear error pointing operators at the temporary
kek-migrate workflow; crash-safe two-phase rotation (`.rot` shadow
files + recovery on next start) is tracked as a follow-up under #4137.

## External Resources

- API docs: https://docs.rs/freenet
- Manual: https://freenet.org/resources/manual/
