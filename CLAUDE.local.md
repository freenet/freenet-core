# Claude Code Development Guidelines

This document contains important guidelines for Claude Code when working on the Freenet project.

## TODO-MUST-FIX Pattern

To prevent committing broken or incomplete code, we use a special comment pattern that blocks commits:

### Usage

When you need to temporarily disable a test or leave code in an incomplete state, use the `TODO-MUST-FIX` comment:

```rust
// TODO-MUST-FIX: This test hangs during node startup and needs investigation
// The test attempts to start a gateway and peer but appears to deadlock
// during the node initialization phase. This needs to be fixed before release.
#[tokio::test]
#[ignore = "Test hangs - see TODO-MUST-FIX above"]
async fn test_gateway_reconnection() -> TestResult {
    // ...
}
```

### How it Works

The git pre-commit hook (`.git/hooks/pre-commit`) will:
1. Scan all staged files for `TODO-MUST-FIX` comments
2. If found, block the commit and display the files and line numbers
3. Force you to resolve the issue before committing

### When to Use

Use `TODO-MUST-FIX` when:
- Disabling a failing test that needs investigation
- Leaving a critical bug unfixed temporarily
- Implementing a temporary workaround that must be properly fixed
- Any code that MUST be addressed before the next release

### Benefits

- Prevents forgetting about disabled tests
- Ensures critical issues aren't accidentally merged
- Creates a clear signal for what needs immediate attention
- Maintains code quality by preventing the accumulation of technical debt

## Other Guidelines

### Dead Code
- Don't just ignore unused variables by prepending `_`, understand why they are unused
- Either use or remove dead code so it doesn't cause confusion
- Don't ignore dead code warnings

### Test Management
- NEVER fix a unit or integration test by just disabling it
- If a test must be temporarily disabled, use `TODO-MUST-FIX` to ensure it's not forgotten
- Always investigate why a test is failing before disabling it