#!/usr/bin/env bash
# Regression test for the v0.2.93 release failure (2026-07-07).
#
# release.yml's `update_versions` job auto-merges the version-bump PR with
# `gh pr merge ... --squash --auto`. The repo's merge queue (strict-strategy
# enforcement enabled between v0.2.92 and v0.2.93) OWNS the merge strategy and
# rejects an explicit `--squash`, so `gh pr merge` exits non-zero. Under
# `shell: bash -e` that fails the step, fails the `update_versions` job, and
# skips the ENTIRE publish cascade (wait_for_pr -> publish_crates -> tag ->
# gateway update). Net effect: the bump PR is created but nothing publishes.
#
# The fix drops `--squash` (the queue applies its configured strategy). This
# test pins that no `gh pr merge` invocation in release.yml re-adds `--squash`,
# and that the auto-merge step still enables auto-merge with `--auto`.
#
# Run manually with: bash scripts/release_mergequeue_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"

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

# Non-comment `gh pr merge` invocations in release.yml.
MERGE_LINES=$(grep -E 'gh pr merge' "$RELEASE_YML" | grep -vE '^\s*#' || true)

check "release.yml still has a gh pr merge invocation" \
  "$([ -n "$MERGE_LINES" ] && echo present || echo missing)" "present"

# The merge queue owns the strategy; an explicit --squash makes `gh pr merge`
# exit non-zero and skips the whole publish cascade (broke v0.2.93).
check "gh pr merge does NOT pass --squash" \
  "$(echo "$MERGE_LINES" | grep -q -- '--squash' && echo has-squash || echo no-squash)" "no-squash"

# Auto-merge must still be enabled, or wait_for_pr times out.
check "gh pr merge still enables --auto" \
  "$(echo "$MERGE_LINES" | grep -q -- '--auto' && echo has-auto || echo no-auto)" "has-auto"

if [ "$FAILURES" -eq 0 ]; then
    echo "PASS: release.yml auto-merge is merge-queue-compatible (--auto, no --squash)."
else
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
