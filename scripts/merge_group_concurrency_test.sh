#!/usr/bin/env bash
# Regression tests for the merge-queue concurrency guard (#5170).
#
# The bug: a workflow that triggers on `merge_group` runs once per merge-queue
# entry, so its `concurrency.group` must vary per entry. `claude-pr-review.yml`
# keyed on `github.event.pull_request.number`, which does not exist on a
# merge_group event, so the key collapsed to the constant `claude-pr-review-`.
# With cancel-in-progress, each new queue entry cancelled the previous entry's
# run of a REQUIRED check; a cancelled check never reports, so the older entry
# went UNMERGEABLE and stalled the queue behind it. Nothing showed on the PR.
#
# `.github/scripts/check_merge_group_concurrency.py` has its own `--self-test`
# over synthetic fixtures. This file tests the things that self-test cannot:
# that the linter fires on the REAL pre-fix file, that the repo satisfies the
# invariant today, and that the linter is actually wired into CI — a guard
# nobody runs is not a guard.
#
# Run manually with: bash scripts/merge_group_concurrency_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_merge_group_concurrency.py"
CI_YML="$REPO_ROOT/.github/workflows/ci.yml"
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

FAILURES=0
check() {
    # check <description> <actual> <expected>
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# The linter's own fixture suite must pass.
SELF_TEST_RC=0
python3 "$LINTER" --self-test >/dev/null 2>&1 || SELF_TEST_RC=$?
check "linter self-test passes" "$SELF_TEST_RC" "0"

# The exact key that shipped and caused the incident. If the linter does not
# flag this, it would not have caught the bug it exists to catch.
mkdir -p "$TEST_TMP/prefix"
cat > "$TEST_TMP/prefix/claude-pr-review.yml" <<'YAML'
name: Claude PR Rule Review
on:
  pull_request_target:
    types: [opened, synchronize]
  merge_group:

concurrency:
  group: claude-pr-review-${{ github.event.pull_request.number }}
  cancel-in-progress: true
YAML
PREFIX_RC=0
PREFIX_OUT=$(python3 "$LINTER" "$TEST_TMP/prefix" 2>&1) || PREFIX_RC=$?
check "flags the pre-fix claude-pr-review key" "$PREFIX_RC" "1"
check "names the collapsing group in the message" \
    "$(echo "$PREFIX_OUT" | grep -c 'claude-pr-review-\${{ github.event.pull_request.number }}')" "1"

# A workflow with no merge_group trigger is none of this linter's business.
# Over-reach would turn it into noise and get it disabled.
mkdir -p "$TEST_TMP/unrelated"
cat > "$TEST_TMP/unrelated/nightly.yml" <<'YAML'
name: Nightly
on:
  schedule:
    - cron: '0 3 * * *'

concurrency:
  group: netcheck-nightly
YAML
UNRELATED_RC=0
python3 "$LINTER" "$TEST_TMP/unrelated" >/dev/null 2>&1 || UNRELATED_RC=$?
check "ignores workflows without a merge_group trigger" "$UNRELATED_RC" "0"

# The repo must satisfy the invariant as it stands.
REPO_RC=0
python3 "$LINTER" "$REPO_ROOT/.github/workflows" >/dev/null 2>&1 || REPO_RC=$?
check "every merge_group workflow in this repo varies its group" "$REPO_RC" "0"

# Pin the CI wiring. Without this, deleting the step from ci.yml silently
# retires the guard while every test above still passes.
check "ci.yml runs the linter" \
    "$(grep -c 'check_merge_group_concurrency\.py .github/workflows' "$CI_YML")" "1"
check "ci.yml runs the linter's self-test" \
    "$(grep -c 'check_merge_group_concurrency\.py --self-test' "$CI_YML")" "1"

if (( FAILURES > 0 )); then
    echo ""
    echo "$FAILURES test(s) failed" >&2
    exit 1
fi
echo ""
echo "All tests passed"
