#!/usr/bin/env bash
# Regression test for auto-update-canary.sh -- the two-sided assertion that
# decides whether a release's auto-updater actually works (#5222).
#
# The fixtures below are VERBATIM log lines captured from real runs on
# 2026-08-08, not invented strings:
#   - the BROKEN fixture is released v0.2.121 failing to parse the v0.2.122
#     tag: the live #5221 regression that broke auto-update fleet-wide;
#   - the HEALTHY fixture is released v0.2.119 correctly detecting v0.2.122
#     and requesting the update.
#
# What this test is FOR: `assert_detection_healthy` is the load-bearing part
# of the canary, and its whole value is that it can go RED. A canary nobody
# has ever seen fail is indistinguishable from one that cannot fail. So the
# cases that matter most here are the negative ones -- especially
# `vacuous: clean log with no check` and `disabled`, which are precisely the
# inputs a one-sided "no error in the log" assertion would wave through.
#
# The real function is sourced (not copied) so the test cannot drift from the
# code CI runs -- mirroring release-agent/verify-version-decision_test.sh.
#
# Run manually: bash scripts/auto-update-canary_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANARY_SH="$SCRIPT_DIR/auto-update-canary.sh"

if [[ ! -f "$CANARY_SH" ]]; then
    echo "FAIL: $CANARY_SH not found" >&2
    exit 1
fi

# Source the real implementation.
# shellcheck source=scripts/auto-update-canary.sh
source "$CANARY_SH"

FAILURES=0
TMPROOT="$(mktemp -d)"
trap 'rm -rf "$TMPROOT"' EXIT

# check <description> <expected-exit> <log-content> [expected-message-substring]
#
# The optional message assertion is not decoration. Several branches of
# `assert_detection_healthy` return the SAME exit code for different reasons,
# so an exit-code-only test cannot tell them apart -- and mutation testing
# confirmed it: deleting the `Auto-update is DISABLED` branch entirely left an
# exit-code-only suite fully green, because a disabled node also has no
# "check ran" line and fails the next assertion anyway. What that branch
# actually contributes is the correct DIAGNOSIS, so that is what gets pinned.
check() {
    local desc="$1" expected="$2" content="$3" want_msg="${4:-}"
    local dir actual stderr
    dir="$(mktemp -d "$TMPROOT/case.XXXXXX")"
    if [[ -n "$content" ]]; then
        printf '%s\n' "$content" > "$dir/freenet.2026-08-08-02.log"
    fi
    stderr="$(assert_detection_healthy "$dir" 2>&1 >/dev/null)"
    actual=$?
    if [[ "$actual" != "$expected" ]]; then
        echo "FAIL - $desc (got exit $actual, expected $expected)" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    if [[ -n "$want_msg" && "$stderr" != *"$want_msg"* ]]; then
        echo "FAIL - $desc (exit $actual correct, but diagnosis wrong)" >&2
        echo "       wanted message containing: $want_msg" >&2
        echo "       got: $stderr" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    echo "ok   - $desc"
}

# Verbatim from a real v0.2.119 run, 2026-08-08T02:02:59Z.
HEALTHY='2026-08-08T02:02:59.369148Z  INFO freenet: Startup update check against GitHub current="0.2.119" jitter_secs=38
2026-08-08T02:02:59.538127Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.122'

# Verbatim from a real v0.2.121 run, 2026-08-08T01:59:35Z -- the #5221 break.
BROKEN='2026-08-08T01:59:35.950835Z  INFO freenet: Startup update check against GitHub current="0.2.121" jitter_secs=40
2026-08-08T01:59:36.111073Z  WARN freenet::commands::auto_update: Startup update check: failed to parse latest version '"'"'v0.2.122'"'"': unexpected character '"'"'v'"'"' while parsing major version number'

# Verbatim from framework, 2026-08-07T15:36:35Z -- the stale #5040 drop-in.
DISABLED='2026-08-07T15:36:35.289311Z  WARN freenet: Auto-update is DISABLED by configuration (--disable-auto-update): this node will NOT detect or apply updates and will stay on version 0.2.120 until you update it out-of-band.'

DIRTY='2026-08-08T02:00:00.000000Z  WARN freenet: Auto-update is DISABLED for this dirty (locally modified) build: this node will NOT detect or apply updates and will stay on version 0.2.122 until you act.'

FETCH_FAIL='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.121" jitter_secs=12
2026-08-08T02:00:00.500000Z  WARN freenet::commands::auto_update: Startup update check: failed to fetch latest version: error sending request. Continuing with current binary.'

# --- the positive case ------------------------------------------------------
check "healthy: check ran and parsed -> pass" 0 "$HEALTHY"

# --- the regression this canary exists to catch -----------------------------
check "broken: #5221 unparseable tag -> fail" 1 "$BROKEN" \
    "could not parse the version GitHub returned"

# --- THE VACUOUS-PASS CASES -------------------------------------------------
# Each of these contains NO error line. A one-sided "grep -q 'failed to parse'
# && fail" assertion passes all of them, which is exactly how a dead updater
# comes to look identical to a working one.
check "vacuous: log with no update check at all -> fail" 1 \
    '2026-08-08T02:00:00.000000Z  INFO freenet: Node started, listening on [::]:31337' \
    "the startup update check never ran"
check "vacuous: empty log directory -> fail" 1 "" \
    "no node logs at all"

# Deliverable of #5222: "auto-update disabled on the canary" is a RED BUILD,
# not something a human has to remember. Both disable paths must be named as
# such -- the exit code alone would be satisfied by the missing-check branch,
# so the diagnosis is the assertion (see the note on `check` above).
check "disabled: --disable-auto-update (the #5040 drop-in) -> fail" 1 "$DISABLED" \
    "auto-update is DISABLED on the canary node"
check "disabled: dirty build silently skips the check -> fail" 1 "$DIRTY" \
    "auto-update is DISABLED on the canary node"

# --- infrastructure vs product bug ------------------------------------------
# GitHub unreachable is NOT a broken updater. It must be distinguishable, or
# a network blip either fails a good release or (worse) gets papered over with
# a retry that also swallows a real parse failure.
check "indeterminate: GitHub unreachable -> retry, not fail" 2 "$FETCH_FAIL"

# --- MARKER_TRIGGERED must not match the REFUSAL line ------------------------
# Found in review. `crates/core/src/bin/freenet.rs` logs
# "...not triggering auto-update (#4073)" when a newer version is locally
# blocked (crash-loop pin / repeated install failures). The obvious short
# marker 'triggering auto-update' is a substring of that, so a node that
# deliberately REFUSED an update would be read as one that requested it --
# Gate B would then wait for an exit 42 that is never coming and misreport the
# cause. Both directions are pinned so neither can regress.
NOT_TRIGGERED='2026-08-08T02:00:00.000000Z  WARN freenet: Startup check: newer version is locally blocked (crash-loop known-bad pin or repeated install failures); not triggering auto-update (#4073)'
REALLY_TRIGGERED='2026-08-08T02:02:59.538127Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.122'

if printf '%s' "$NOT_TRIGGERED" | grep -qF "$MARKER_TRIGGERED"; then
    echo "FAIL - MARKER_TRIGGERED matches the '#4073 not triggering' refusal line" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - MARKER_TRIGGERED does not match the #4073 refusal line"
fi

if printf '%s' "$REALLY_TRIGGERED" | grep -qF "$MARKER_TRIGGERED"; then
    echo "ok   - MARKER_TRIGGERED matches a real update trigger"
else
    echo "FAIL - MARKER_TRIGGERED no longer matches the real trigger line" >&2
    FAILURES=$((FAILURES + 1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All auto-update-canary assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
