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

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES verify-version-decision.sh test(s) failed" >&2
    exit 1
fi
echo "All verify-version-decision.sh tests passed."
