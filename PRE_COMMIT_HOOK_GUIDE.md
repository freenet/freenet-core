# Pre-Commit Hook with Claude Test Detection

## Overview

The pre-commit hook now includes Claude-powered detection of disabled tests to prevent accidentally committing code with disabled tests.

## How It Works

The hook performs the following checks:
1. **Rust formatting** - Ensures code follows standard formatting
2. **Clippy linting** - Catches common Rust mistakes and anti-patterns
3. **TODO-MUST-FIX detection** - Blocks commits with TODO-MUST-FIX comments
4. **Disabled test detection** - Uses Claude to analyze the diff and detect disabled tests

## Disabled Test Detection

Claude will detect various patterns indicating disabled tests:
- Rust: `#[ignore]`, `#[ignore = "reason"]`
- JavaScript/TypeScript: `it.skip()`, `describe.skip()`, `test.skip()`
- Python: `@pytest.mark.skip`, `@unittest.skip`
- Commented out test functions
- Any other language-specific test disabling patterns

When disabled tests are detected, the hook will:
1. Block the commit
2. Show exactly where the disabled tests were found
3. Provide guidance on how to proceed

## Example Output

```bash
Checking for disabled tests with Claude...
✗ Claude detected disabled tests in the commit
Disabled tests found at:
test_example.rs:6-9 - test marked with #[ignore] attribute
Tests should not be disabled in commits. If a test must be temporarily disabled:
1. Add a TODO-MUST-FIX comment explaining why
2. Fix the underlying issue before committing
3. Or exclude the test changes from this commit
```

## Handling Disabled Tests

If you need to temporarily disable a test:

1. **Add TODO-MUST-FIX comment**: This will also block the commit, forcing you to address it
   ```rust
   // TODO-MUST-FIX: This test hangs during startup
   #[ignore = "See TODO-MUST-FIX above"]
   fn test_broken() { }
   ```

2. **Fix the test**: The preferred solution is to fix the underlying issue

3. **Exclude from commit**: Use `git add -p` to selectively stage changes, excluding the disabled test

## Requirements

- Claude CLI must be installed at `~/.claude/local/claude`
- The hook will gracefully skip Claude checks if the CLI is not available

## Troubleshooting

- If Claude checks are failing unexpectedly, check that the Claude CLI is working:
  ```bash
  ~/.claude/local/claude --help
  ```
- The hook won't fail the commit if Claude itself has an error (only if disabled tests are found)