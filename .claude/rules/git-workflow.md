# Git & PR Workflow

## Trigger-Action Rules

### BEFORE every commit

```
1. Run: cargo fmt
2. Run: cargo clippy -- -D warnings
3. Run: cargo test
```

### WHEN creating a commit message

```
Does the subject line follow conventional commits?
  → NO: Rewrite as: feat|fix|docs|refactor|test|build: description

Is subject under 72 characters?
  → NO: Shorten it

Does the body explain WHY, not WHAT?
  → NO: Add reasoning. Code diff shows WHAT; message explains WHY.
```

**Valid prefixes:**
- `feat:` – new feature
- `fix:` – bug fix
- `docs:` – documentation only
- `refactor:` – code change that doesn't fix bug or add feature
- `test:` – adding/updating tests
- `build:` – build system or dependencies

### WHEN creating a PR

```
1. Check title: Does it follow conventional commits?
   → NO: CI will fail. Fix title first.

2. Check description: Does it have these sections?
   - ## Problem (what's broken, user impact)
   - ## Solution (key insight, why this approach)
   - ## Testing (what validates this)
   - ## Fixes (closes #XXXX)
   → Missing sections: Add them. Reviewer shouldn't need to read issue thread.

3. Ask yourself: Can reviewer understand my reasoning from PR alone?
   → NO: Add more context to description.
```

### WHEN a test starts failing

```
DO NOT:
  ✗ Delete the test
  ✗ Comment it out
  ✗ Skip without documentation

Is the test broken or flaky?
  DO:
    ✓ Add #[ignore] attribute
    ✓ Add comment: // Ignored: [reason] #[issue]
    ✓ Create GitHub issue immediately

Is the test superseded by a semantic change?
  DO:
    ✓ Add #[ignore] attribute
    ✓ Add comment explaining the semantic change and referencing the PR
    ✓ Keep as historical documentation of the old behavior
```

Example (broken test):
```rust
// Ignored: Flaky under parallel execution, see #1234
#[ignore]
#[test]
fn flaky_test() { ... }
```

Example (superseded test):
```rust
// Superseded: hosted-only contracts no longer renewed after #3363.
// Replaced by test_contracts_needing_renewal_excludes_hosted_only.
#[ignore]
#[test]
fn test_old_behavior() { ... }
```

### WHEN reviewing code

```
Does PR explain WHY changes were made?
  → NO: Request explanation before approving

Are there new #[ignore] tests?
  → YES: Verify tracking issue exists (broken) or PR reference (superseded)

Does test coverage match changed code?
  → NO: Request tests for uncovered paths
```

### WHEN a freenet-core PR depends on an unpublished freenet-stdlib change

**Stdlib-first release policy.** The stdlib PR MUST merge to main and
publish to crates.io BEFORE the freenet-core PR that consumes it opens
for review.

```
Tempted to coordinate with [patch.crates-io] git overrides?
  → DON'T. Multiple repeating sources of pain:

  1. CI cannot resolve `path = "..."` to a sibling worktree on a fresh
     checkout. Every workspace cargo invocation fails at
     `cargo metadata` with "failed to load source for dependency".

  2. Cargo's [patch.crates-io] is NOT inherited by nested workspaces.
     If apps/freenet-ping/ or any other nested workspace path-deps
     freenet-core, the override must be duplicated there. Forgetting
     this manifests as "failed to select a version" errors that look
     like a dep resolver bug but are actually a workspace inheritance
     gap.

  3. Even when the git override works on CI, the PR cannot merge in
     that state — every reviewer sees a "MUST be removed before merge"
     comment and a pre-merge cleanup checklist that exists only because
     the wrong workflow was chosen upstream.

CORRECT workflow:
  1. Open the stdlib PR with the new variant / type / API.
  2. Get reviews + CI green on stdlib alone.
  3. Merge stdlib → main.
  4. `cargo publish` the new stdlib version to crates.io.
  5. THEN open the freenet-core PR with `freenet-stdlib = "X.Y.Z"`
     pointing at the published version. No git overrides, no path
     deps, no nested-workspace duplication, no pre-merge cleanup.

The only friction this adds is one extra round-trip on the stdlib
release; the friction it AVOIDS is a multi-commit cleanup pass and
broken CI on every fresh checkout of the consumer PR. Pay the
round-trip — it's cheaper.
```

### WHEN bumping `freenet-stdlib` to a version that adds enum variants

```
The wire-boundary enums (MessageOrigin, InboundDelegateMsg,
OutboundDelegateMsg, UpdateData, ContractError, DelegateError, etc.)
are all `#[non_exhaustive]` since stdlib 0.6.0. That means:

1. Adding a variant to any of them is wire-format-additive — bincode
   discriminants for existing variants stay byte-identical, and
   deployed contract/delegate WASM compiled against any earlier 0.x
   stdlib continues to deserialize the existing variants unchanged.

2. Bumping freenet-core to a new stdlib MAY require adding wildcard
   arms to existing match sites in core. The compiler will tell you
   exactly where — fix each site by deciding what the "unknown
   variant" semantic should be (usually: log+reject, log+ignore, or
   forward through the WASM boundary unmodified).

3. The cheap-win pattern: list known variants exhaustively, then add
   a wildcard arm with `#[allow(clippy::wildcard_enum_match_arm)]`
   and a comment explaining that the wildcard exists ONLY to satisfy
   `non_exhaustive`. Don't be tempted to expand the wildcard into a
   `pat | _` listing — that defeats the safety net the wildcard
   provides for future variants.

4. Wire-format pin tests in stdlib (`*_wire_format_is_stable`) lock
   variant tag numbers. Reordering enum variants is a wire-format
   break and MUST trip these tests during PR review.
```
