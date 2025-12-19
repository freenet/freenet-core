# Pre-Commit Hook Setup Guide

## Overview

This repository uses the [pre-commit](https://pre-commit.com/) framework to run automated checks before each commit. This catches common issues early and maintains code quality.

## Quick Start

### 1. Install pre-commit framework

```bash
# Using pip
pip install pre-commit

# Or using homebrew (macOS)
brew install pre-commit

# Or using asdf
asdf plugin add pre-commit
asdf install pre-commit latest
```

### 2. Install the hooks

```bash
cd /path/to/freenet-core
pre-commit install
```

That's it! The hooks will now run automatically on `git commit`.

## What Gets Checked

The pre-commit hooks run the following checks:

### 1. **Rust Formatting** (`cargo fmt`)
- Ensures all Rust code follows standard formatting
- **Action if it fails:** Run `cargo fmt` to auto-fix
- Fast check (~1-2 seconds on changed files)

### 2. **Rust Linting** (`cargo clippy`)
- Catches common Rust mistakes and anti-patterns
- **Action if it fails:** Fix the warnings shown in the output
- Runs with `-D warnings` (warnings are treated as errors)
- Slower check (~10-30 seconds depending on changes)

### 3. **TODO-MUST-FIX Detection**
- Blocks commits containing `TODO-MUST-FIX` markers
- **Action if it fails:** Fix the underlying issue or remove from this commit
- Per repository policy: tests should not be disabled without fixing the issue

### 4. **General Checks**
- Check for merge conflict markers
- Validate YAML syntax
- Fix trailing whitespace and end-of-file markers

## Usage

### Normal Workflow
```bash
# Make your changes
git add .

# Commit - hooks run automatically
git commit -m "fix: your changes"

# If checks fail, fix the issues and try again
cargo fmt              # Fix formatting
cargo clippy --fix     # Auto-fix some clippy issues
git add .
git commit -m "fix: your changes"
```

### Skip Hooks (Not Recommended)
```bash
# Emergency bypass (use sparingly!)
git commit --no-verify -m "emergency fix"
```

### Run Hooks Manually
```bash
# Run on staged files
pre-commit run

# Run on all files
pre-commit run --all-files

# Run specific hook
pre-commit run cargo-fmt
pre-commit run cargo-clippy
```

### Update Hooks
```bash
# Update to latest versions
pre-commit autoupdate

# Clean and reinstall
pre-commit clean
pre-commit install
```

## Performance Tips

- **Clippy is slow on first run** - Subsequent runs are faster due to caching
- **Only changed files are checked** - fmt and clippy are smart about this
- **Skip clippy for WIP commits**: Use `SKIP=cargo-clippy git commit -m "wip"`
- **CI runs strict checks** - Pre-commit is developer-friendly, CI is strict

## Troubleshooting

### Hooks not running
```bash
# Verify installation
pre-commit --version
ls -la .git/hooks/pre-commit

# Reinstall if needed
pre-commit uninstall
pre-commit install
```

### Clippy taking too long
```bash
# Skip clippy for this commit only
SKIP=cargo-clippy git commit -m "your message"

# Or disable clippy temporarily in .pre-commit-config.yaml
# (Don't commit this change!)
```

### False positives
```bash
# Run the check manually to see full output
cargo clippy --all-targets --all-features

# If it's a legitimate issue, file a bug or add to exclusions
```

## Configuration

The configuration is in `.pre-commit-config.yaml` at the repository root.

To modify which checks run, edit that file and run:
```bash
pre-commit install --install-hooks
```

## CI vs Pre-Commit

- **Pre-commit**: Developer-friendly, fast feedback
- **CI**: Strict enforcement, all features tested, comprehensive checks

Both are important! Pre-commit catches issues early, CI ensures nothing slips through.

---

## TODO: Future Enhancements

### Claude-Powered Disabled Test Detection

**Status**: Not yet implemented

**Planned feature**: Integrate Claude CLI to automatically detect disabled tests in commits.

**How it would work**:
- Use Claude to analyze git diffs and detect various patterns indicating disabled tests
- Patterns to detect:
  - Rust: `#[ignore]`, `#[ignore = "reason"]`
  - JavaScript/TypeScript: `it.skip()`, `describe.skip()`, `test.skip()`
  - Python: `@pytest.mark.skip`, `@unittest.skip`
  - Commented out test functions
  - Any other language-specific test disabling patterns

**When disabled tests are detected**, the hook would:
1. Block the commit
2. Show exactly where the disabled tests were found
3. Provide guidance on how to proceed

**Example output**:
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

**Requirements**:
- Claude CLI at `~/.claude/local/claude`
- Would gracefully skip if CLI not available

**To implement**: Create a custom pre-commit hook in `.pre-commit-config.yaml` that calls the Claude CLI to analyze the diff.
