#!/usr/bin/env bash
# Decision logic for gateway-update.yml's "Verify version after update" poll.
#
# Extracted into a sourceable function so the workflow and its regression
# test (verify-version-decision_test.sh) exercise the SAME code and cannot
# drift. The inline-in-YAML version of this logic had zero automated
# coverage, which is exactly the gap that let the vega v0.2.71 incident
# through (binary swapped, service down, workflow reported success).
#
# Usage (sourced):
#   source verify-version-decision.sh
#   decision=$(verify_version_decision "$RESPONSE_JSON" "$TARGET_VERSION")
#
# Echoes exactly one decision token on stdout:
#   success           binary is on the target version AND the service is active
#                     (new-agent path) — the update is confirmed.
#   success-fallback  binary is on the target version but the agent predates
#                     the `service_active` field — fall back to binary-only
#                     verification (with a warning). Backward compatibility.
#   wait              not yet converged: binary not on target, OR binary on
#                     target but service not active (the vega case), OR the
#                     gateway is unreachable / returned malformed JSON. The
#                     caller keeps polling until its deadline, then fails.

# WHY binary-on-target + service-active is sufficient (no swap race): the
# update script (deploy-local-gateway.sh, spawned via gateway-auto-update.sh)
# does stop -> swap binary -> start. The service is DOWN while the binary is
# replaced, so the binary only reads as the new version once the service has
# been restarted onto it. There is no window where the OLD process is still
# active while the binary already reads as new. If that ordering ever changes
# to swap-then-restart, this function must additionally verify the RUNNING
# process is the new binary (e.g. unit ActiveEnterTimestamp vs. issued_at).
verify_version_decision() {
    local response="$1"
    local target="$2"

    local installed has_service_field service_active
    installed=$(printf '%s' "$response" | jq -r '.version // empty' 2>/dev/null || echo "")
    # has() distinguishes "old agent omits the field" (success-fallback) from
    # "new agent reports service_active:false" (wait — the vega case). This
    # only matters once the binary version already matches; the installed!=
    # target branch below handles the unreachable-gateway case (empty
    # installed). The `|| echo "false"` only guards jq erroring on a malformed
    # body.
    has_service_field=$(printf '%s' "$response" | jq -r 'has("service_active") // false' 2>/dev/null || echo "false")
    service_active=$(printf '%s' "$response" | jq -r '.service_active // false' 2>/dev/null || echo "false")

    if [[ "$installed" != "$target" ]]; then
        echo "wait"
        return 0
    fi

    if [[ "$has_service_field" == "true" ]]; then
        if [[ "$service_active" == "true" ]]; then
            echo "success"
        else
            # Binary is new but the service is not active (vega v0.2.71).
            echo "wait"
        fi
    else
        # Old agent without the field: fall back to binary-only verification.
        echo "success-fallback"
    fi
}
