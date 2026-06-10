#!/usr/bin/env bash
# Regression test for verify-version-decision.sh — the decision gate that
# gateway-update.yml uses to decide whether a gateway update succeeded.
#
# This is the exact vega v0.2.71 incident path:
#   - present `service_active:false` MUST NOT report success (the load-bearing
#     assertion: that is what let the down gateway look healthy);
#   - an ABSENT field MUST fall back to binary-only success (backward compat
#     with agents that predate the field);
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

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES verify-version-decision.sh test(s) failed" >&2
    exit 1
fi
echo "All verify-version-decision.sh tests passed."
