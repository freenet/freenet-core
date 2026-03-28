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
