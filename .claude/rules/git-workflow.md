# Git & PR Workflow

## PR Title Format

All PR titles must follow Conventional Commits. CI fails non-conforming titles.

```
feat: add new feature
fix: resolve bug in X
docs: update architecture docs
refactor: simplify Y module
test: add integration tests for Z
build: update dependencies
```

## PR Description Template

PR descriptions must explain **WHY**, not just **WHAT**.

```markdown
## Problem
[What's broken and user-visible impact]
[Why existing tests didn't catch it]

## Solution
[Key insight or design decision]
[Why this approach over alternatives]

## Testing
[New tests and what they validate]
[Local validation steps]

## Fixes
Closes #XXXX
```

**Key principle:** Reviewer should understand your reasoning without reading the issue thread.

## Commit Messages

- Focus on "why" rather than "what"
- Reference issue numbers when applicable
- Keep subject line under 72 characters

## Before Pushing

```bash
cargo fmt
cargo clippy --all-targets --all-features
cargo test
```

## TODO-MUST-FIX Marker

The repository blocks commits with `// TODO-MUST-FIX:` comments. Use this marker when temporarily disabling tests, then create a follow-up issue.

```rust
// TODO-MUST-FIX: Re-enable after fixing #1234
#[ignore]
#[test]
fn flaky_test() { ... }
```

Never remove failing tests without understanding the root cause.
