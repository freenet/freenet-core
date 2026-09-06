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

# ---------------------------------------------------------------------------
# detect_crates_state: the CRATES_PUBLISHED resume flag must require BOTH crates
#
# Same family as the bug above -- resume state that is wrong in a way the
# operator cannot see -- and a regression introduced while making a check more
# accurate, which is why it is worth a permanent test rather than a comment.
#
# `publish_crates` returns early on CRATES_PUBLISHED, ABOVE its own per-crate
# logic. So a flag set on freenet alone makes the fdev branch unreachable
# exactly when it is needed: attach-to-release publishes freenet, the fdev
# publish fails (docs/RELEASING.md records that fdev has no packaging
# pre-flight anywhere, so this is the expected discovery point), the operator
# resumes release.sh, and it reports the crates step already complete with fdev
# never published.
#
# This checked freenet only for as long as it used `cargo search`, whose index
# lag usually answered "not published" -- so the flag went unset and
# publish_crates ran. The inaccuracy was the only thing holding the gap shut.
# ---------------------------------------------------------------------------
eval "$(awk '/^detect_crates_state\(\) \{/,/^}/' "$RELEASE_SH")"

test_detect_crates_state() {
    local name="$1" freenet_up="$2" fdev_up="$3" expected="$4" got
    # Stub the registry lookup the real function calls.
    crate_version_on_crates_io() {
        case "$1" in
            freenet) [[ "$freenet_up" == yes ]] ;;
            fdev)    [[ "$fdev_up"    == yes ]] ;;
            *)       return 1 ;;
        esac
    }
    # Read by the extracted detect_crates_state, not by this function.
    # shellcheck disable=SC2034  # consumed by the eval'd release.sh function
    VERSION="9.9.9"
    FDEV_VERSION="0.9.9"
    COMPLETED_STEPS=()
    detect_crates_state > /dev/null
    got="${COMPLETED_STEPS[CRATES_PUBLISHED]:-unset}"
    if [[ "$got" != "$expected" ]]; then
        echo "FAIL [$name]: expected CRATES_PUBLISHED=$expected, got $got" >&2
        exit 1
    fi
    echo "PASS [$name]"
}

# THE regression: a partial publish must NOT mark the step complete, or fdev is
# silently stranded and the driver reports a successful release.
test_detect_crates_state "partial publish leaves crates step incomplete" yes no  unset
# ...and the normal cases, so the assertion above cannot be satisfied by a
# function that simply never sets the flag.
test_detect_crates_state "both crates published marks step complete"     yes yes 1
test_detect_crates_state "neither published leaves step incomplete"      no  no  unset
test_detect_crates_state "fdev-only leaves step incomplete"              no  yes unset

echo "All tests passed."
