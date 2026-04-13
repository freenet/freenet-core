#!/usr/bin/env bash
# Regression test for the FDEV_VERSION double-bump bug surfaced by the
# v0.2.42 release (see build(release): restore FDEV_VERSION from state on
# resume).
#
# The release script computes FDEV_VERSION at startup by reading the current
# crates/fdev/Cargo.toml patch version and adding one. On a resume *after*
# the release PR has merged and the operator has pulled main, the local
# Cargo.toml already contains the just-released version, so the +1 produces
# a value one patch ahead of what actually shipped. The fix is that
# load_state_file restores FDEV_VERSION from the persisted state file,
# overriding the tentative top-level compute.
#
# This test exercises the restore logic in isolation: it seeds a state file
# with the correct FDEV_VERSION, clobbers the in-memory variable to the
# buggy double-bumped value, invokes the restore logic, and asserts the
# in-memory value is corrected back to the persisted one.
#
# Run manually with: bash scripts/release_state_restore_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_SH="$SCRIPT_DIR/release.sh"

if [[ ! -f "$RELEASE_SH" ]]; then
    echo "FAIL: $RELEASE_SH not found" >&2
    exit 1
fi

# Extract the load_state_file function from release.sh and source it into
# this test script with the state file path we control. Keeping the test
# self-contained means release.sh can stay a single file and does not need
# to be refactored for testability.
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
STATE_FILE="$TMP/release.state"
declare -A COMPLETED_STEPS

# Pull the function body verbatim from release.sh so the test is guaranteed
# to exercise the real implementation, not a copy that can drift.
eval "$(awk '/^load_state_file\(\) \{/,/^}/' "$RELEASE_SH")"

test_restores_persisted_value() {
    local name="$1" persisted="$2" tentative="$3" expected="$4"
    cat > "$STATE_FILE" <<EOF
# Release state for v0.2.42
VERSION=0.2.42
FDEV_VERSION=$persisted
COMPLETED_PR_CREATED=1
COMPLETED_PR_MERGED=1
EOF
    FDEV_VERSION="$tentative"
    COMPLETED_STEPS=()
    load_state_file > /dev/null
    if [[ "$FDEV_VERSION" != "$expected" ]]; then
        echo "FAIL [$name]: expected FDEV_VERSION=$expected, got $FDEV_VERSION" >&2
        exit 1
    fi
    if [[ "${COMPLETED_STEPS[PR_CREATED]:-}" != "1" ]]; then
        echo "FAIL [$name]: PR_CREATED not restored from state" >&2
        exit 1
    fi
    if [[ "${COMPLETED_STEPS[PR_MERGED]:-}" != "1" ]]; then
        echo "FAIL [$name]: PR_MERGED not restored from state" >&2
        exit 1
    fi
    echo "PASS [$name]"
}

test_no_persisted_keeps_tentative() {
    rm -f "$STATE_FILE"
    FDEV_VERSION="0.3.205"
    COMPLETED_STEPS=()
    load_state_file > /dev/null
    if [[ "$FDEV_VERSION" != "0.3.205" ]]; then
        echo "FAIL [no state]: tentative should survive; got $FDEV_VERSION" >&2
        exit 1
    fi
    echo "PASS [no state]"
}

# v0.2.42 regression: state has the correct 0.3.205, but the in-memory
# variable has been double-bumped to 0.3.206 by the top-level compute
# reading the post-merge Cargo.toml. Restore must win.
test_restores_persisted_value "v0.2.42 regression" "0.3.205" "0.3.206" "0.3.205"

# No-op case: persisted value matches tentative value, nothing to do.
test_restores_persisted_value "matching values" "0.3.205" "0.3.205" "0.3.205"

# Fresh run: no state file exists, tentative value must be used as-is.
test_no_persisted_keeps_tentative

echo "All tests passed."
