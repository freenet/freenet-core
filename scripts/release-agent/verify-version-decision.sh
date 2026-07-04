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
#                     the `service_active` field, so service health is
#                     UNVERIFIABLE. This is a classification only — it does NOT
#                     mean "accept". As of #4492 the consumer (gateway-update.yml)
#                     fails closed on this token by default and accepts a
#                     binary-only check only when explicitly opted in via the
#                     `allow_binary_only_fallback` input. The policy lives in the
#                     workflow, not here.
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
        # Old agent without the field: service health is unverifiable here.
        # Emit the token; the consumer decides whether to accept binary-only
        # (off by default since #4492 — see the header).
        echo "success-fallback"
    fi
}

# Consumer-side policy for the `success-fallback` token (#4492). Extracted here
# (rather than inline in gateway-update.yml) so the fail-closed default is unit-
# tested and cannot silently regress — the inline-in-YAML version is exactly the
# kind of untested logic this file exists to replace.
#
# Usage:
#   decision=$(fallback_decision "$DECISION_TOKEN" "$ALLOW_BINARY_ONLY_FALLBACK")
#
# Echoes one token on stdout:
#   accept  the workflow may treat this as a successful update. ONLY for
#           `success-fallback` WHEN `allow` is exactly "true" (the explicit
#           opt-in). A genuine `success` is the workflow's own concern and is
#           NOT routed through here.
#   hold    do NOT accept: keep polling and fail closed at the deadline. This is
#           the default for `success-fallback` (missing service_active → service
#           health unverifiable → must not be reported as success).
#
# `allow` is treated as opt-in: anything other than the exact string "true"
# (empty on the release-trigger path, "false", garbage) yields `hold`.
fallback_decision() {
    local token="$1" allow="$2"
    if [[ "$token" == "success-fallback" && "$allow" == "true" ]]; then
        echo "accept"
    else
        echo "hold"
    fi
}

# Stability gate for the "Verify version after update" poll (#4567). Extracted
# here (rather than inline in gateway-update.yml) so the consecutive-success
# semantics are unit-tested and cannot silently regress.
#
# WHY: the verify loop used to `exit 0` on the FIRST poll that produced a
# `success` token. That confirms health at a single instant only, so a gateway
# that comes up `active` and then dies within the remaining verify window
# (flap-after-up: the racing-updater shape from the nova v0.2.78 incident, or a
# crash-loop that happens to be up on the first poll) was still reported green.
# Requiring N CONSECUTIVE `success` polls exploits the release-agent's 2s
# service_active TTL cache: a crash-loop flaps service_active across polls, so
# the streak resets and the update is not confirmed until the deadline fails it
# closed.
#
# Usage (per poll):
#   streak=$(stability_streak "$DECISION" "$streak")   # updated running count
#   if [[ "$(stability_confirmed "$streak" "$REQUIRED")" == "confirm" ]]; then
#       exit 0
#   fi
#
# stability_streak echoes the new consecutive-success count: the previous count
# + 1 on a `success` token, otherwise 0 (any non-success poll — wait,
# unreachable, malformed — breaks the streak). Resetting on non-success is what
# catches flap-after-up: an up→down sequence cannot accumulate the required run.
# A normal restart gap happens BEFORE the first success (the service is down
# while the binary is swapped), so it only delays the start of the streak; it
# does not reset a streak already in progress. A restart AFTER first success
# simply restarts the count within the same budget — failing closed only at the
# deadline, which is the conservative, correct behaviour.
stability_streak() {
    local token="$1" prev="$2"
    if [[ "$token" == "success" ]]; then
        echo $(( prev + 1 ))
    else
        echo 0
    fi
}

# stability_confirmed echoes `confirm` once the streak has reached the required
# number of consecutive successes, otherwise `keep-polling`. `required` <= 0 is
# treated as 1 (a misconfiguration must not disable the gate entirely — at least
# one success is always needed).
stability_confirmed() {
    local streak="$1" required="$2"
    if (( required < 1 )); then
        required=1
    fi
    if (( streak >= required )); then
        echo "confirm"
    else
        echo "keep-polling"
    fi
}
