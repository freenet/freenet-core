#!/usr/bin/env bash
# Regression tests for the required-check path-filter guard (#5451).
#
# The bug: `ci.yml` and `claude-pr-review.yml` carried a `paths-ignore` covering
# `docs/**` on their pull-request triggers. Seven of the eight required status
# contexts came from those two workflows, and a workflow skipped by a path
# filter reports NOTHING. GitHub treats a required context that never reported
# as "expected" rather than satisfied, so the merge queue refused entry — #5322
# was structurally unmergeable from 2026-08-14 until #5571. The `merge_group:`
# trigger was no rescue: it only runs for a PR already admitted to the queue.
#
# `.github/scripts/check_required_check_path_filters.py` has its own
# `--self-test` over synthetic fixtures. This file tests what that self-test
# cannot: that the linter fires on the REAL pre-fix files as they stood on
# `main`, that the repo satisfies the invariant today, and that the linter is
# actually wired into CI — a guard nobody runs is not a guard. The real pre-fix
# text is the part that matters most, because #5571 deletes it from the repo:
# after that, the hand-written fixtures are the only other record.
#
# Run manually with: bash scripts/required_check_path_filters_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_required_check_path_filters.py"
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

# The exact `on:` blocks that shipped and caused the incident, reproduced
# verbatim from the pre-#5571 files. If the linter does not flag these, it would
# not have caught the bug it exists to catch.
mkdir -p "$TEST_TMP/prefix"
cat > "$TEST_TMP/prefix/ci.yml" <<'YAML'
name: CI
on:
  push:
    branches: [main]
    paths-ignore:
      - 'docs/**'
      - '*.md'
      - 'LICENSE'
      - '.github/ISSUE_TEMPLATE/**'
      - '.github/FUNDING.yml'
  pull_request:
    paths-ignore:
      - 'docs/**'
      - '*.md'
      - 'LICENSE'
      - '.github/ISSUE_TEMPLATE/**'
      - '.github/FUNDING.yml'
  merge_group:

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
  clippy_check:
    name: Clippy
    runs-on: ubicloud-standard-8
  rule_lint:
    name: Rule Lint
    runs-on: ubuntu-latest
YAML
cat > "$TEST_TMP/prefix/claude-pr-review.yml" <<'YAML'
name: Claude PR Rule Review
on:
  pull_request_target:
    types: [opened, synchronize, ready_for_review, reopened, labeled]
    paths-ignore:
      - 'docs/**'
      - '*.md'
      - 'LICENSE'
      - '.github/ISSUE_TEMPLATE/**'
      - '.github/FUNDING.yml'
  merge_group:

jobs:
  rule-review:
    name: Claude Rule Review
    runs-on: ubuntu-latest
YAML
PREFIX_RC=0
PREFIX_OUT=$(python3 "$LINTER" --no-coverage-check "$TEST_TMP/prefix" 2>&1) || PREFIX_RC=$?
check "flags the pre-fix ci.yml and claude-pr-review.yml" "$PREFIX_RC" "1"
check "flags the pull_request filter, not the push one" \
    "$(echo "$PREFIX_OUT" | grep -c "on the .pull_request:. trigger")" "1"
check "flags the pull_request_target filter" \
    "$(echo "$PREFIX_OUT" | grep -c "on the .pull_request_target:. trigger")" "1"
check "names the required contexts at risk" \
    "$(echo "$PREFIX_OUT" | grep -c "'Clippy', 'Fmt', 'Rule Lint'")" "1"

# The one-line flow form is equally valid YAML, equally destructive, and is the
# style this repo already uses for `branches: [main]` — so it is the likeliest
# way the optimization comes back. It must not slip past.
mkdir -p "$TEST_TMP/flow"
cat > "$TEST_TMP/flow/ci.yml" <<'YAML'
name: CI
on:
  pull_request:
    paths-ignore: ['docs/**', '*.md']

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
YAML
FLOW_RC=0
python3 "$LINTER" --no-coverage-check "$TEST_TMP/flow" >/dev/null 2>&1 || FLOW_RC=$?
check "flags the one-line flow-sequence filter" "$FLOW_RC" "1"

# A workflow that provides no required context may filter freely. Over-reach
# would turn this linter into noise and get it disabled.
mkdir -p "$TEST_TMP/unrelated"
cat > "$TEST_TMP/unrelated/benchmarks.yml" <<'YAML'
name: Benchmarks
on:
  pull_request:
    paths:
      - 'crates/core/benches/**'

jobs:
  bench:
    name: Run benchmarks
    runs-on: ubuntu-latest
YAML
UNRELATED_RC=0
python3 "$LINTER" --no-coverage-check "$TEST_TMP/unrelated" >/dev/null 2>&1 || UNRELATED_RC=$?
check "ignores workflows that provide no required context" "$UNRELATED_RC" "0"

# The repo must satisfy both halves of the invariant as it stands: no filter on
# a gating trigger, and every required context actually reported by something.
REPO_RC=0
python3 "$LINTER" "$REPO_ROOT/.github/workflows" >/dev/null 2>&1 || REPO_RC=$?
check "this repo's required-check workflows carry no PR path filter" "$REPO_RC" "0"

# Pin the CI wiring. Without this, deleting the steps from ci.yml silently
# retires the guard while every test above still passes.
check "ci.yml runs the linter over the workflows dir" \
    "$(grep -c 'check_required_check_path_filters\.py .github/workflows' "$CI_YML")" "1"
check "ci.yml runs the linter's self-test" \
    "$(grep -c 'check_required_check_path_filters\.py --self-test' "$CI_YML")" "1"
check "ci.yml runs this pin test" \
    "$(grep -c 'scripts/required_check_path_filters_test\.sh' "$CI_YML")" "1"
# The coverage half is only on when the repo scan passes no opt-out. If someone
# adds --no-coverage-check to the ci.yml invocation, the job-rename guard is
# gone and nothing else would notice.
check "ci.yml does not disable the coverage half" \
    "$(grep -c 'check_required_check_path_filters\.py .*--no-coverage-check' "$CI_YML")" "0"

if (( FAILURES > 0 )); then
    echo ""
    echo "$FAILURES test(s) failed" >&2
    exit 1
fi
echo ""
echo "All tests passed"
