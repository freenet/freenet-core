# Git Hooks

This directory contains git hooks that enforce code quality and commit message standards.

## Installation

To install these hooks, run from the repository root:

```bash
# Install commit-msg hook
cp .githooks/commit-msg .git/hooks/
chmod +x .git/hooks/commit-msg

# Or configure git to use this hooks directory
git config core.hooksPath .githooks
```

## Available Hooks

### commit-msg

Validates that commit messages follow [Conventional Commits](https://www.conventionalcommits.org/) format.

**Required format:**
```
type(scope): description

[optional body]

[optional footer(s)]
```

**Allowed types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or modifying tests
- `build`: Build system or dependency changes
- `ci`: CI/CD changes

**Examples:**
```bash
fix: resolve WebSocket connection timeout
feat(network): add peer reputation scoring
build(deps): bump tokio from 1.0 to 1.1
fix!: breaking change to API
```

**Important:**
- Type must be **lowercase** (e.g., `fix:` not `Fix:`)
- This matches the CI check on GitHub PRs
- Breaking changes can be indicated with `!` after the type

### pre-commit

The pre-commit hook (already installed in `.git/hooks/pre-commit`) checks:
- Code formatting with `cargo fmt`
- Clippy lints
- Special TODO comments that block commits
- Disabled tests using Claude

**Note:** The pre-commit hook is specific to your local setup and is already installed by git hooks installation scripts. It's not included in this directory as it's environment-specific.

## Why This Matters

- **CI Compatibility**: Our GitHub Actions CI checks PR titles for Conventional Commits format
- **Consistency**: Everyone uses the same commit message style
- **Automation**: Makes it easier to generate changelogs and release notes
- **Early Feedback**: Catch issues locally before pushing to GitHub
