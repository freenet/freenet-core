#!/usr/bin/env bash
# Tests for scripts/wan-bench-classify.sh, the per-seed classifier behind the
# WAN Bench R1 "known failures" step in .github/workflows/simulation-nightly.yml.
#
# The classifier it replaced matched each reason line on its own, so a
# reason allowed only as a consequence of an abort ("no evidence the spike
# drove") also passed on a seed that never aborted. These cases pin the
# per-seed pairing, the abort cap, the verdict check, and the two ends of the
# contract the classifier relies on: the line format r1_tests.rs prints, and
# the workflow calling the classifier.
#
# Run manually with: bash scripts/wan-bench-classify_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLASSIFY="$SCRIPT_DIR/wan-bench-classify.sh"
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

# classify <max-abort-seeds> <log lines...>: prints the exit code.
classify() {
    local max="$1"
    shift
    printf '%s\n' "$@" > "$TEST_TMP/log"
    local rc=0
    bash "$CLASSIFY" "$TEST_TMP/log" "$max" > "$TEST_TMP/out" || rc=$?
    echo "$rc"
}

VERDICT='R1b failed for BBR (rerun one seed with FREENET_WAN_BENCH_SEEDS=<seed>):'
SLOW1='  seed 0x1c20: did not complete within 28.915824s'
SLOW2='  seed 0x1c20: goodput never reached 50% of the bottleneck within 11 RTTs (2.75s) of the path clearing'
ABORT='  seed 0x1c21: sender gave up on the stream (1 outbound stream failure(s), e.g. the cwnd-wait timeout)'
EVIDENCE_ABORTED='  seed 0x1c21: no evidence the spike drove the sender'"'"'s timers: largest RTT sample 0ns (spike 2.4s) and 0% of what it sent during the window was retransmitted'
EVIDENCE_ALONE='  seed 0x1c22: no evidence the spike drove the sender'"'"'s timers: largest RTT sample 0ns (spike 2.4s) and 0% of what it sent during the window was retransmitted'

check "slow seeds only: known failure" \
    "$(classify all "$VERDICT" "$SLOW1" "$SLOW2")" 0
check "an abort is allowed" \
    "$(classify all "$VERDICT" "$SLOW1" "$ABORT")" 0
check "no evidence on a seed that aborted: known failure" \
    "$(classify all "$VERDICT" "$ABORT" "$EVIDENCE_ABORTED")" 0
check "no evidence on a seed that did NOT abort: new failure" \
    "$(classify all "$VERDICT" "$ABORT" "$EVIDENCE_ALONE")" 1
check "no evidence without an abort is named in the output" \
    "$(grep -c 'seed 0x1c22: no evidence the spike drove the sender, without an abort' "$TEST_TMP/out")" 1
check "another seed's abort does not cover it" \
    "$(classify all "$VERDICT" "$EVIDENCE_ALONE" "${ABORT/0x1c21/0x1c23}")" 1
check "aborts at the cap: known failure" \
    "$(classify 1 "$VERDICT" "$ABORT")" 0
check "aborts over the cap: new failure" \
    "$(classify 1 "$VERDICT" "$ABORT" "${ABORT/0x1c21/0x1c23}")" 1
check "a seed that panicked: new failure" \
    "$(classify all "$VERDICT" '  seed 0x1cae: panicked: connect a: ConnectionEstablishmentFailure')" 1
check "an unknown reason: new failure" \
    "$(classify all "$VERDICT" '  seed 0x1c20: sender took an RTT sample of 49.974ms, below the true base RTT 250ms')" 1
check "no verdict line: new failure" \
    "$(classify all "$SLOW1")" 1
check "across-seed lines are not per-seed reasons" \
    "$(classify all "$VERDICT" "$SLOW1" '  across seeds: median completion is 2.42x ideal over 2 seeds, limit 2x')" 0

# The line format the classifier parses, at its source: the verdict is the
# failure list joined with "\n  ", and each entry starts "seed {:#x}: ".
R1_TESTS="$REPO_ROOT/crates/core/src/transport/link_model/r1_tests.rs"
check "r1_tests.rs prints each report failure as 'seed 0x..: <reason>'" \
    "$(grep -cF 'failed.push(format!("seed {:#x}: {f}", r.seed));' "$R1_TESTS")" 1
check "r1_tests.rs prints a panicked seed as 'seed 0x..: panicked: ..'" \
    "$(grep -cF '"seed {seed:#x}: panicked: {}",' "$R1_TESTS")" 1
check "r1_tests.rs indents each verdict line by two spaces" \
    "$(grep -cF 'failed.join("\n  ")' "$R1_TESTS")" 1

# The workflow runs the classifier for each of the four known failures.
NIGHTLY="$REPO_ROOT/.github/workflows/simulation-nightly.yml"
for test_name in r1_spike_recovery_bbr r1_spike_recovery_ledbat \
    r1b_spike_recovery_bbr r1b_spike_recovery_ledbat; do
    check "simulation-nightly.yml checks $test_name" \
        "$(grep -cE "^ *check $test_name [0-9a-z]+$" "$NIGHTLY")" 1
done
# shellcheck disable=SC2016 # the workflow's literal text, not an expansion
check "simulation-nightly.yml's check() calls the classifier" \
    "$(grep -cF 'bash scripts/wan-bench-classify.sh "${test_name}.log" "$max_aborts"' "$NIGHTLY")" 1

if [ "$FAILURES" -gt 0 ]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "all checks passed"
