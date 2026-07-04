#!/usr/bin/env bash
# Regression test for verify-version-decision.sh — the decision gate that
# gateway-update.yml uses to decide whether a gateway update succeeded.
#
# This is the exact vega v0.2.71 incident path:
#   - present `service_active:false` MUST NOT report success (the load-bearing
#     assertion: that is what let the down gateway look healthy);
#   - an ABSENT field classifies as `success-fallback` (backward-compat token
#     for agents predating the field). NOTE: the token is not itself an
#     "accept" — since #4492 the consumer workflow fails closed on it by
#     default; this test pins the classifier, not the consumer's policy;
#   - present `service_active:true` + matching version MUST report success.
#
# The real function is sourced (not copied) so the test cannot drift from the
# code the workflow runs — mirroring announce-to-river_test.sh.
#
# Run manually: bash scripts/release-agent/verify-version-decision_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DECISION_SH="$SCRIPT_DIR/verify-version-decision.sh"

if [[ ! -f "$DECISION_SH" ]]; then
    echo "FAIL: $DECISION_SH not found" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "FAIL: jq is required for this test" >&2
    exit 1
fi

# Source the real implementation.
# shellcheck source=scripts/release-agent/verify-version-decision.sh
source "$DECISION_SH"

FAILURES=0
check() {
    # check <description> <response-json> <target-version> <expected-decision>
    local desc="$1" response="$2" target="$3" expected="$4" actual
    actual=$(verify_version_decision "$response" "$target")
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

TARGET="0.2.71"

# ── New agent, binary on target ──────────────────────────────────────

# Service active → confirmed success.
check "new agent: binary on target + service active → success" \
    '{"version":"0.2.71","binary_path":"/x","service_active":true,"managed_service":"freenet-gateway"}' \
    "$TARGET" "success"

# THE load-bearing assertion: service failed (the vega case) → MUST keep
# waiting, never report success. If this regresses, a down gateway looks
# healthy again.
check "new agent: binary on target + service FAILED (vega case) → wait" \
    '{"version":"0.2.71","binary_path":"/x","service_active":false,"managed_service":"freenet-gateway"}' \
    "$TARGET" "wait"

# Custom managed_service name (vega's secondary instance) still works.
check "new agent: custom managed_service + failed → wait" \
    '{"version":"0.2.71","service_active":false,"managed_service":"freenet-gateway-hector"}' \
    "$TARGET" "wait"

# ── Old agent: no service_active field (backward compat) ─────────────

check "old agent: binary on target, no service_active → success-fallback" \
    '{"version":"0.2.71","binary_path":"/x"}' \
    "$TARGET" "success-fallback"

# ── Not yet converged ────────────────────────────────────────────────

check "new agent: binary still old + service active → wait" \
    '{"version":"0.2.70","binary_path":"/x","service_active":true}' \
    "$TARGET" "wait"

check "old agent: binary still old, no field → wait" \
    '{"version":"0.2.70","binary_path":"/x"}' \
    "$TARGET" "wait"

# ── Unreachable / malformed responses ────────────────────────────────

# Empty body (curl failed / connection refused during restart) → wait.
check "unreachable gateway (empty response) → wait" \
    '' "$TARGET" "wait"

# Malformed JSON → jq errors are swallowed, treated as not-converged.
check "malformed JSON response → wait" \
    'this is not json' "$TARGET" "wait"

# A null service_active with matching version is treated as not-active.
# (Field is present-but-null: has() is true, value coalesces to false → wait.)
check "new agent: service_active null + binary on target → wait" \
    '{"version":"0.2.71","service_active":null}' \
    "$TARGET" "wait"

# ── fallback_decision: consumer policy for the success-fallback token (#4492) ──
#
# This is the load-bearing fail-closed default: a missing service_active field
# must NOT be accepted as success unless the operator explicitly opts in.

check_fallback() {
    # check_fallback <description> <token> <allow> <expected>
    local desc="$1" token="$2" allow="$3" expected="$4" actual
    actual=$(fallback_decision "$token" "$allow")
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# Default (no opt-in): fall back is NOT accepted — fail closed. The empty
# string is what the release-trigger path passes (no workflow input).
check_fallback "success-fallback + allow empty → hold (fail closed, release path)" \
    "success-fallback" "" "hold"
check_fallback "success-fallback + allow=false → hold (fail closed)" \
    "success-fallback" "false" "hold"
check_fallback "success-fallback + allow=garbage → hold (only exact 'true' opts in)" \
    "success-fallback" "yes" "hold"
# Explicit opt-in: accepted.
check_fallback "success-fallback + allow=true → accept (explicit opt-in)" \
    "success-fallback" "true" "accept"
# Any non-fallback token never routes through here as accept.
check_fallback "wait token + allow=true → hold (only success-fallback can accept)" \
    "wait" "true" "hold"

# ── stability gate: consecutive-success requirement (#4567) ───────────
#
# The verify loop must require N CONSECUTIVE `success` polls before declaring
# convergence, so a gateway that comes up active then dies within the window
# (flap-after-up) is NOT reported green off a single lucky poll.

# Assertion dispatcher: `check <kind> <description> <args...>`.
# `kind` selects the function under test (streak | confirmed | replay).
# Kept as a single `check` front-door so the assertions read uniformly and
# stay visible to the fix:-PR regression-test lint (ci.yml counts added
# `check ` lines in *_test.sh self-tests).
check() {
    local kind="$1" desc="$2"; shift 2
    local expected actual
    case "$kind" in
        streak)
            # check streak <description> <token> <prev> <expected-new-streak>
            expected="$3"
            actual=$(stability_streak "$1" "$2")
            ;;
        confirmed)
            # check confirmed <description> <streak> <required> <expected>
            expected="$3"
            actual=$(stability_confirmed "$1" "$2")
            ;;
        replay)
            # check replay <description> <expected> <required> <token...>
            expected="$1"; shift
            actual=$(replay_confirms "$@")
            ;;
        *)
            echo "FAIL - unknown check kind '$kind'" >&2
            FAILURES=$((FAILURES + 1))
            return
            ;;
    esac
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# stability_streak: success increments, anything else resets to 0.
check streak "success increments the streak" "success" "1" "2"
check streak "success from 0 starts the streak" "success" "0" "1"
check streak "wait resets the streak (flap-after-up)" "wait" "2" "0"
check streak "success-fallback resets the streak" "success-fallback" "2" "0"

# stability_confirmed: confirm only once the streak reaches the requirement.
check confirmed "below requirement → keep polling" "1" "3" "keep-polling"
check confirmed "one short → keep polling" "2" "3" "keep-polling"
check confirmed "at requirement → confirm" "3" "3" "confirm"
check confirmed "above requirement → confirm" "4" "3" "confirm"
# Misconfiguration guard: required<1 must still demand at least one success
# (never confirm on a zero streak — that would disable the gate entirely).
check confirmed "required=0 clamped to 1: zero streak → keep polling" "0" "0" "keep-polling"
check confirmed "required=0 clamped to 1: one success → confirm" "1" "0" "confirm"

# ── end-to-end poll sequences: the load-bearing #4567 assertions ──────
#
# Replays a sequence of /version responses through the SAME streak logic the
# workflow runs, asserting whether the update would be confirmed within the
# window. `confirmed` is true iff the streak reaches REQUIRED at any poll.
replay_confirms() {
    # replay_confirms <required> <token...> → echoes "yes" or "no"
    local required="$1"; shift
    local streak=0 token
    for token in "$@"; do
        streak=$(stability_streak "$token" "$streak")
        if [[ "$(stability_confirmed "$streak" "$required")" == "confirm" ]]; then
            echo "yes"
            return 0
        fi
    done
    echo "no"
}

# THE bug: up on the first poll, then dies → must NOT confirm (was the latch).
check replay "flap-after-up (success then dead) must NOT confirm" \
    "no" 3 success wait wait wait wait
# Sustained health → confirms.
check replay "sustained up (3 consecutive) confirms" \
    "yes" 3 success success success
# A single transient false DURING convergence must not permanently fail: the
# streak resets but a subsequent sustained run still confirms.
check replay "transient false mid-convergence then sustained-up confirms" \
    "yes" 3 wait success wait success success success
# Crash-loop that keeps flapping never accumulates the run → never confirms
# (fails closed at the deadline in the workflow).
check replay "crash-loop flapping never confirms" \
    "no" 3 success wait success wait success wait
# Normal restart gap BEFORE first success only delays the streak start.
check replay "restart gap before first success still confirms" \
    "yes" 3 wait wait success success success

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES verify-version-decision.sh test(s) failed" >&2
    exit 1
fi
echo "All verify-version-decision.sh tests passed."
