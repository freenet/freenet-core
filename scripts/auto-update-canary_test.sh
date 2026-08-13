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
# Chain, do not replace: sourcing auto-update-canary.sh installed its own
# `trap cleanup EXIT`, and overwriting it leaks that script's workdir on every
# CI run.
trap 'rm -rf "$TMPROOT"; cleanup' EXIT

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

# The OTHER healthy shape, and the one Gate A actually sees: the binary about
# to ship is NEWER than the latest release, so the check finishes without
# triggering anything. Until #5236 that outcome was a `debug!` -- compiled out
# of release builds -- so a healthy Gate A run produced no ending at all and
# was byte-for-byte indistinguishable from a run cut short.
HEALTHY_UP_TO_DATE='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.412000Z  INFO freenet: Startup update check complete: staying on the current version current="0.2.123"'

# The vacuous pass #5236 closed: the check STARTED and the log stops there,
# because the canary killed the node while GitHub was still answering. Every
# negative assertion is satisfied; none of them can see that nothing happened.
PENDING='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7'

# Verbatim from a real v0.2.121 run, 2026-08-08T01:59:35Z -- the #5221 break.
BROKEN='2026-08-08T01:59:35.950835Z  INFO freenet: Startup update check against GitHub current="0.2.121" jitter_secs=40
2026-08-08T01:59:36.111073Z  WARN freenet::commands::auto_update: Startup update check: failed to parse latest version '"'"'v0.2.122'"'"': unexpected character '"'"'v'"'"' while parsing major version number'

# Verbatim from framework, 2026-08-07T15:36:35Z -- the stale #5040 drop-in.
DISABLED='2026-08-07T15:36:35.289311Z  WARN freenet: Auto-update is DISABLED by configuration (--disable-auto-update): this node will NOT detect or apply updates and will stay on version 0.2.120 until you update it out-of-band.'

DIRTY='2026-08-08T02:00:00.000000Z  WARN freenet: Auto-update is DISABLED for this dirty (locally modified) build: this node will NOT detect or apply updates and will stay on version 0.2.122 until you act.'

FETCH_FAIL='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.121" jitter_secs=12
2026-08-08T02:00:00.500000Z  WARN freenet::commands::auto_update: Startup update check: failed to fetch latest version: error sending request. Continuing with current binary.'

# --- fixtures for the POSITIVE-EQUALITY check (#5236, review finding 32) ----
#
# The shape Gate A sees on a healthy run, now carrying the observed latest.
SEEN_OK='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.122
2026-08-08T02:00:00.412000Z  INFO freenet: Startup update check complete: staying on the current version current="0.2.123"'

# A SILENTLY WRONG comparator, which is the whole point of the check. This is
# what a `version_from_tag` that truncates `0.2.122` to `0.2.12` emits: it does
# not fail to parse, it parses the WRONG thing. Every other assertion in
# assert_detection_healthy passes on it -- the check ran, no parse error, no
# fetch error, it completed -- and before this check the canary called it OK.
SEEN_TRUNCATED='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.12
2026-08-08T02:00:00.412000Z  INFO freenet: Startup update check complete: staying on the current version current="0.2.123"'

# The other silently-wrong shape: a comparator pinned to a constant.
SEEN_CONSTANT='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.0.0
2026-08-08T02:00:00.412000Z  INFO freenet: Startup update check complete: staying on the current version current="0.2.123"'

# --- the positive cases -----------------------------------------------------
check "healthy: check ran, parsed, triggered -> pass" 0 "$HEALTHY"
check "healthy: check ran and completed up-to-date -> pass" 0 "$HEALTHY_UP_TO_DATE"

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
check "indeterminate: GitHub unreachable -> retry, not fail" 2 "$FETCH_FAIL" \
    "could not reach GitHub to fetch the latest version"

# --- unknown must never read as OK (#5236) ----------------------------------
# The check started and the log stops. Distinguishing this from success is the
# entire assertion: "no parse error" is evidence that parsing worked only if
# the check is known to have got as far as parsing. Exit 2, not 0 and not 1 --
# nothing was detected, nothing was proved, and the caller retries.
check "unknown: check started but logged no outcome -> indeterminate, NOT ok" 2 "$PENDING" \
    "never logged an outcome"

# --- the update-trigger detector --------------------------------------------
# `node_decided_to_update` must fire for ALL FOUR "triggering auto-update"
# sites in freenet.rs and must NOT fire for the #4073 refusal, which shares the
# phrase ("...not triggering auto-update"). Two ways to get this wrong, both
# found in review: the bare substring counts the refusal as a trigger, and
# anchoring on one site's full phrase misses the other three -- reporting "did
# not decide to update" for a node that did.
trigger_case() {
    # trigger_case <description> <expect: yes|no> <log-line>
    local desc="$1" expect="$2" line="$3"
    local dir
    dir="$(mktemp -d "$TMPROOT/trig.XXXXXX")"
    printf '%s\n' "$line" > "$dir/freenet.2026-08-08-02.log"
    if node_decided_to_update "$dir"; then local got=yes; else local got=no; fi
    if [[ "$got" == "$expect" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (detector said '$got', expected '$expect')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

trigger_case "trigger: startup check" yes \
    '2026-08-08T02:02:59Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.122'
trigger_case "trigger: post-stagger confirm" yes \
    '2026-08-08T02:02:59Z  INFO freenet: Update confirmed on GitHub after stagger, triggering auto-update new_version=0.2.122'
trigger_case "trigger: peer-signal confirm" yes \
    '2026-08-08T02:02:59Z  INFO freenet: Newer version confirmed on GitHub, triggering auto-update new_version=0.2.122'
trigger_case "trigger: periodic re-poll" yes \
    '2026-08-08T02:02:59Z  INFO freenet: Periodic re-poll: newer version on GitHub, triggering auto-update new_version=0.2.122'
# The FIFTH site, and the one the fixed-string marker silently missed: it says
# "triggering IMMEDIATE auto-update", so `triggering auto-update` did not match
# and a node that took the urgent path was reported as never having decided to
# update. Fail-closed, so nothing broke loudly -- which is why it survived.
trigger_case "trigger: urgent path (the site the fixed-string marker missed)" yes \
    '2026-08-08T02:02:59Z  INFO freenet: Urgent update confirmed on GitHub, triggering immediate auto-update new_version=0.2.122'
trigger_case "NOT a trigger: #4073 locally-blocked refusal" no \
    '2026-08-08T02:02:59Z  WARN freenet: Startup check: newer version is locally blocked (crash-loop known-bad pin or repeated install failures); not triggering auto-update (#4073)'

# --- the POSITIVE-EQUALITY check (#5236, review finding 32) -----------------
#
# Everything above this point is satisfied by a comparator that is silently
# WRONG rather than broken. These drive assert_detection_healthy with
# CANARY_EXPECTED_LATEST set, which is how Gate A runs it.
check_vs_expected() {
    # check_vs_expected <description> <expected-latest> <expected-exit> <log> [msg]
    local desc="$1" expected_latest="$2" expected="$3" content="$4" want_msg="${5:-}"
    CANARY_EXPECTED_LATEST="$expected_latest" check "$desc" "$expected" "$content" "$want_msg"
}

check_vs_expected "equality: node compared against the right release -> pass" \
    "0.2.122" 0 "$SEEN_OK"
# The two silently-wrong comparators. Before this check both reported OK.
check_vs_expected "equality: TRUNCATED tag (0.2.122 -> 0.2.12) -> fail" \
    "0.2.122" 1 "$SEEN_TRUNCATED" "compared against the WRONG release"
check_vs_expected "equality: comparator pinned to a constant -> fail" \
    "0.2.122" 1 "$SEEN_CONSTANT" "compared against the WRONG release"
# A binary that never logs the observed value cannot be checked at all, and
# "cannot be checked" must not read as "checked and fine".
check_vs_expected "equality: no observed-latest line at all -> fail" \
    "0.2.122" 1 "$HEALTHY_UP_TO_DATE" "never logged which release it compared against"
# Unset is the pre-#5236 behaviour and must still work (the lifecycle test and
# `assert-logs` drive it that way), but it must SAY it proved less.
#
# The message assertion is the whole case, and it was missing: with only the
# exit code compared, replacing the entire `note "NOTE: CANARY_EXPECTED_LATEST
# is unset..."` in auto-update-canary.sh with `:` left this green. The skip
# branch could be silently emptied -- and a silent skip is precisely the
# vacuous pass this gate exists to remove, since a reader of a green log would
# have no way to tell the equality check ran from it having been skipped.
#
# Explicitly unset rather than relying on the variable happening to be unset.
# It is not leaking from `check_vs_expected` today -- bash restores a
# `VAR=x func` assignment when the function returns, verified -- but this is
# the one case whose meaning depends on ambient environment, and the way it
# would go wrong is silent: SEEN_OK carries latest=0.2.122, so an inherited
# CANARY_EXPECTED_LATEST=0.2.122 sends it down the EQUALITY branch, still
# exiting 0, testing the opposite of what its name says. The `unset` costs
# nothing and makes the case mean one thing.
unset CANARY_EXPECTED_LATEST
check "equality: unset expected-latest -> still passes, but says it skipped" \
    0 "$SEEN_OK" "CANARY_EXPECTED_LATEST is unset, so the positive-equality check was SKIPPED"

# --- the tag normaliser -----------------------------------------------------
# It has to agree with version_from_tag exactly. If it strips differently, the
# equality check above compares two spellings of the same release and fails a
# release for a difference that is not a bug.
norm_case() {
    # norm_case <input> <expected>
    local got
    got="$(normalise_release_tag "$1")"
    if [[ "$got" == "$2" ]]; then
        echo "ok   - normalise_release_tag '$1' -> '$2'"
    else
        echo "FAIL - normalise_release_tag '$1' gave '$got', expected '$2'" >&2
        FAILURES=$((FAILURES + 1))
    fi
}
norm_case "v0.2.122" "0.2.122"
norm_case "0.2.122"  "0.2.122"
# At most ONE `v`, matching `strip_prefix` rather than the greedy
# `trim_start_matches` -- the hazard version_from_tag's rustdoc calls out.
norm_case "vv1.2.3"  "v1.2.3"

# --- the SIGPIPE regression (review finding 35) -----------------------------
# `grep -q` at the end of a pipe exits at its first match and SIGPIPEs the
# upstream grep; under `pipefail` that 141 became the pipeline's status and the
# detector answered "no" for a node that plainly did decide to update. It only
# bites once output passes the 64 KB pipe buffer, so a small fixture cannot see
# it -- this one is deliberately large enough to.
sigpipe_dir="$(mktemp -d "$TMPROOT/sigpipe.XXXXXX")"
{
    for _ in $(seq 1 700); do
        echo '2026-08-08T02:02:59Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.122'
    done
} > "$sigpipe_dir/freenet.2026-08-08-02.log"
if node_decided_to_update "$sigpipe_dir"; then
    echo "ok   - trigger detection survives >64KB of matching output (no SIGPIPE)"
else
    echo "FAIL - trigger detection returned FALSE on a log full of triggers." >&2
    echo "       This is the pipefail+SIGPIPE regression: a trailing 'grep -q' closes the" >&2
    echo "       pipe at the first match, the upstream grep dies 141, and pipefail makes" >&2
    echo "       that the pipeline's status -- so a node that decided to update reads as" >&2
    echo "       one that did not." >&2
    FAILURES=$((FAILURES + 1))
fi

# The SAME defect, in `assert_detection_healthy` itself, which is the part the
# release gate calls. The two helpers above were fixed when the mechanism was
# first diagnosed; the four checks inside the function they serve were not, and
# unlike the helpers this one was not latent -- it hit 2 of 3 real preflight
# runs. Gate A's normal path is a binary NEWER than latest, so the node does
# not exit 42 and keeps logging (~33 KB/s) until the canary kills it seconds
# later, which puts the markers far behind the 64 KB pipe buffer.
#
# The fixtures elsewhere in this file are a few hundred bytes, so none of them
# can see it: the verdict was a function of log VOLUME, not of what the node
# did. These two are deliberately past the buffer, with the markers FIRST and
# the bulk after them -- the real geometry.
#
# Both directions are pinned, and both test the same property: that the verdict
# does not change with log VOLUME. The healthy one is what actually broke (a
# good release blocked by "the startup update check never ran", naming the wrong
# subsystem). The broken one covers the far worse direction -- a SIGPIPE'd
# negative check reading the #5221 signature as ABSENT, so the gate goes GREEN
# on the exact bug it exists to catch.
#
# What the broken case does NOT test, despite an earlier version of this comment
# saying so, is check ORDERING. `assert_detection_healthy` reaches the parse
# check via `log_has`, which greps the fixture FILES directly, so the marker is
# found whatever order the checks run in and a reordering leaves this green.
# Volume-resistance is what is pinned here; it is real, and it is the property
# that broke.
BULK="$(for _ in $(seq 1 1200); do
    echo '2026-08-08T02:00:01.000000Z  INFO freenet::node: connection established peer=abc123 remaining=7'
done)"
check "volume: healthy markers behind >64KB of later output -> still pass" \
    0 "$SEEN_OK
$BULK"
check_vs_expected "volume: equality check still sees the observed-latest line behind >64KB" \
    "0.2.122" 0 "$SEEN_OK
$BULK"
check "volume: #5221 parse failure behind >64KB of later output -> still fail" \
    1 "$BROKEN
$BULK" "could not parse the version GitHub returned"

# --- numeric-override validation (review finding 36) ------------------------
# A non-numeric CANARY_TIMEOUT_SECS reaches an arithmetic context and, under
# `set -u`, kills the canary with a shell error instead of a verdict -- a
# release-blocking failure whose message says nothing about the release. Its two
# neighbours were already guarded; this one was not.
timeout_guard_case() {
    # timeout_guard_case <override> <expected-effective-value>
    local got
    got="$(CANARY_TIMEOUT_SECS="$1" bash -c '
        set -uo pipefail
        # shellcheck source=/dev/null
        source "$1" >/dev/null 2>&1 || true
        printf "%s" "$CANARY_TIMEOUT_SECS"' _ "$CANARY_SH")"
    if [[ "$got" == "$2" ]]; then
        echo "ok   - CANARY_TIMEOUT_SECS='$1' is sanitised to $2"
    else
        echo "FAIL - CANARY_TIMEOUT_SECS='$1' became '$got', expected '$2'" >&2
        FAILURES=$((FAILURES + 1))
    fi
}
timeout_guard_case "abc" "240"
timeout_guard_case "0"   "240"
timeout_guard_case ""    "240"
timeout_guard_case "90"  "90"

# --- Gate A must actually ARM the equality check -----------------------------
# `assert_detection_healthy` skips the positive-equality check when
# CANARY_EXPECTED_LATEST is unset, which is right for the pure/unit-testable
# shape but means the check is only as real as the caller that sets it. Nothing
# else pins that, so a refactor dropping the assignment would leave every
# assertion here green while Gate A silently reverted to "the node did not
# complain" -- the exact vacuous shape this PR exists to remove.
#
# Scoped to cmd_preflight's body, not a whole-file grep: the variable is named
# in comments elsewhere in the file, and a file-wide match would be satisfied by
# the prose describing the mechanism rather than the code implementing it.
preflight_body="$(awk '/^cmd_preflight\(\) \{/{f=1} f{print} f&&/^\}/{exit}' "$CANARY_SH")"
# shellcheck disable=SC2016  # the needles below match LITERAL source text, so
# the `$(...)` inside them must not expand -- that is the point of the pin.
if [[ -z "$preflight_body" ]]; then
    echo "FAIL - could not locate cmd_preflight() in $(basename "$CANARY_SH")" >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$preflight_body" != *'CANARY_EXPECTED_LATEST="$(resolve_expected_latest)"'* ]]; then
    echo "FAIL - cmd_preflight no longer resolves CANARY_EXPECTED_LATEST." >&2
    echo "       Gate A's only positive assertion is skipped when that is unset, so the gate" >&2
    echo "       drops back to 'the node did not complain' -- which a silently-wrong" >&2
    echo "       comparator satisfies (#5236, review finding 32)." >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$preflight_body" != *'export CANARY_EXPECTED_LATEST'* ]]; then
    echo "FAIL - cmd_preflight resolves CANARY_EXPECTED_LATEST but does not export it." >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - cmd_preflight resolves and exports CANARY_EXPECTED_LATEST (the equality check is armed)"
fi
# ...and refuses rather than passing when it cannot resolve it. A resolution
# failure that fell through would run the gate with the check skipped.
if [[ "$preflight_body" == *'if ! CANARY_EXPECTED_LATEST='*'return 1'* ]]; then
    echo "ok   - cmd_preflight refuses (returns non-zero) when the expected release cannot be resolved"
else
    echo "FAIL - cmd_preflight does not refuse when resolve_expected_latest fails." >&2
    echo "       Falling through would run Gate A with its positive check silently skipped." >&2
    FAILURES=$((FAILURES + 1))
fi

# --- Gate B must arm the equality check too ---------------------------------
# `cmd_selfupdate` runs in its own process, so nothing Gate A exported reaches
# it: before this, the deliberately-loud "CANARY_EXPECTED_LATEST is unset" NOTE
# fired on EVERY healthy Gate B run. A warning that appears on every good
# release is one everybody learns to scroll past. Same pin shape as
# cmd_preflight's above, and for the same reason -- the skip branch is only as
# harmless as the callers that do not take it.
selfupdate_body="$(awk '/^cmd_selfupdate\(\) \{/{f=1} f{print} f&&/^\}/{exit}' "$CANARY_SH")"
# shellcheck disable=SC2016  # literal source text; must not expand
if [[ -z "$selfupdate_body" ]]; then
    echo "FAIL - could not locate cmd_selfupdate() in $(basename "$CANARY_SH")" >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$selfupdate_body" != *'export CANARY_EXPECTED_LATEST="$expected_version"'* ]]; then
    echo "FAIL - cmd_selfupdate does not arm the positive-equality check." >&2
    echo "       Gate B knows exactly which release it just published; without exporting it, the" >&2
    echo "       gate drops to 'the node did not complain' and prints the unset NOTE on every" >&2
    echo "       healthy release until nobody reads it (#5236)." >&2
    FAILURES=$((FAILURES + 1))
# ...and it must arm it from the version it was told to expect, not from a
# second lookup. Re-resolving would introduce a source allowed to disagree with
# the caller -- the failure-that-is-not-a-bug this file already avoids in
# resolve_expected_latest.
elif [[ "$selfupdate_body" == *'resolve_expected_latest'* ]]; then
    echo "FAIL - cmd_selfupdate re-resolves the expected release instead of using its argument." >&2
    echo "       Two sources that may disagree produce a failed release that is not a bug." >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - cmd_selfupdate arms the equality check from its expected-version argument"
fi

# --- environmental classification: the alarm must not blame the fleet -------
# `node_could_not_reach_github` is what stops the post-publish Matrix alarm
# saying "a node on the previous release may not be able to auto-update to this
# one" because a hosted runner could not open a socket.
#
# It is a real risk rather than a tidy-up, and the numbers matter: Gate B's
# subject is a PUBLISHED binary, whose startup fetch retries ZERO times
# (`startup_update_check_with_fetcher` warns and returns on the first Err), and
# `framework` logged that WARN twice on 2026-08-11 -- 17:08:04Z and 00:35:13Z.
#
# Both directions, because only one of them is safe to get wrong. Classifying a
# real fault as environmental would quieten the exact alarm this canary exists
# to raise, so the negative case is the load-bearing one.
env_case() {
    # env_case <description> <expect yes|no> <log-content>
    local desc="$1" expect="$2" content="$3" dir got
    dir="$(mktemp -d "$TMPROOT/env.XXXXXX")"
    printf '%s\n' "$content" > "$dir/freenet.2026-08-08-02.log"
    if node_could_not_reach_github "$dir"; then got=yes; else got=no; fi
    if [[ "$got" == "$expect" ]]; then
        echo "ok   - environmental classification: $desc"
    else
        echo "FAIL - environmental classification: $desc (got '$got', expected '$expect')" >&2
        if [[ "$expect" == no ]]; then
            echo "       Classifying this as environmental silences the #5221 alarm for a run" >&2
            echo "       that found a real fault." >&2
        else
            echo "       Not classifying it means a runner that could not reach github.com" >&2
            echo "       tells the dev room the fleet may be stranded." >&2
        fi
        FAILURES=$((FAILURES + 1))
    fi
}
env_case "the node's own fetch-failure WARN" yes "$FETCH_FAIL"
env_case "a real #5221 parse failure is NOT environmental" no "$BROKEN"
env_case "a healthy run is NOT environmental"              no "$HEALTHY"
env_case "a truncated run (no outcome logged) is NOT environmental" no "$PENDING"
env_case "a disabled updater is NOT environmental"         no "$DISABLED"

# --- and the DECISION the classification feeds -------------------------------
# `node_could_not_reach_github` establishes CANDIDACY for the quiet path;
# `gate_b_unverified_class` decides. The split exists because the quiet message
# tells the dev room a red release needs no action, so a single WARN from the
# node is not enough to earn it: a persistent problem would otherwise report
# "environmental" release after release, nobody would be alarmed, and we would
# believe we had a post-publish gate while verifying nothing.
#
# The GitHub case therefore demands corroboration from a second observer in the
# same run -- this runner's own fetch. Stubbed here, so the decision is tested
# without a network.
#
# The real implementation is captured and put back afterwards rather than
# re-sourcing the script: sourcing it again would run its top-level `mktemp -d`
# a second time and re-register `trap cleanup EXIT`, leaking the first workdir
# on every CI run. A stub left installed would be worse -- it would silently
# weaken any later assertion that reaches it.
real_runner_can_reach_github="$(declare -f runner_can_reach_github)"
if [[ -z "$real_runner_can_reach_github" ]]; then
    echo "FAIL - runner_can_reach_github is not defined; the corroboration tests would" >&2
    echo "       stub a function nothing calls and pass vacuously." >&2
    FAILURES=$((FAILURES + 1))
fi
class_case() {
    # class_case <description> <env-cause> <runner-reachable yes|no> <expected>
    local desc="$1" cause="$2" reachable="$3" expect="$4" got
    if [[ "$reachable" == yes ]]; then
        runner_can_reach_github() { return 0; }
    else
        runner_can_reach_github() { return 1; }
    fi
    got="$(gate_b_unverified_class "$cause")"
    if [[ "$got" == "$expect" ]]; then
        echo "ok   - unverified class: $desc"
    else
        echo "FAIL - unverified class: $desc (got '$got', expected '$expect')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}
class_case "node could not reach GitHub and neither can the runner -> quiet" \
    github no  environmental
class_case "node could not reach GitHub but the RUNNER can -> loud" \
    github yes fault
class_case "port collision needs no network corroboration -> quiet" \
    ports  yes environmental
class_case "port collision, runner offline too -> still quiet" \
    ports  no  environmental
class_case "no cause recorded (hung updater) -> loud even with a dead network" \
    ""     no  fault
class_case "an unrecognised cause is loud, not quiet" \
    weather no fault
eval "$real_runner_can_reach_github"

# Gate B's use of it. Three separate things can each silently undo the split,
# and the first two fail in the QUIET direction:
#   - returning 1 instead of the distinct code, so cross-compile.yml cannot
#     tell the cases apart and every blip fires the #5221 alarm again;
#   - classifying without consulting the logs (e.g. treating every rc=2 as
#     environmental), which quietens a genuine no-outcome run;
#   - dropping the retry loop, so one blip in a ~40s window decides a release.
# shellcheck disable=SC2016  # literal source text; must not expand
if [[ "$selfupdate_body" != *'return "$EXIT_UNVERIFIED_ENVIRONMENTAL"'* ]]; then
    echo "FAIL - cmd_selfupdate no longer returns the distinct environmental exit code." >&2
    echo "       cross-compile.yml keys the WORDING of its Matrix message off that code." >&2
    echo "       Without it a runner that could not reach GitHub tells the dev room that a" >&2
    echo "       node on the previous release may not be able to auto-update -- the #5221" >&2
    echo "       text -- and an alarm that cries wolf on a network blip stops being read." >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$selfupdate_body" != *'node_could_not_reach_github "$work/logs"'* ]]; then
    echo "FAIL - cmd_selfupdate classifies without consulting the node's logs." >&2
    echo "       assert_detection_healthy returns 2 for two different situations. Treating" >&2
    echo "       both as environmental quietens a run where the check started and never" >&2
    echo "       logged an outcome, which is what a HUNG updater looks like." >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$selfupdate_body" != *'gate_b_unverified_class "$env_cause"'* ]]; then
    echo "FAIL - cmd_selfupdate no longer routes the quiet path through gate_b_unverified_class." >&2
    echo "       Taking the node's WARN as sufficient on its own restores the silent-skip" >&2
    echo "       shape: a persistent problem reports 'environmental' release after release," >&2
    echo "       nobody is alarmed, and the post-publish gate verifies nothing while looking" >&2
    echo "       maintained. The corroboration probe is what earns the quiet message." >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$selfupdate_body" != *'for attempt in $(seq 1 "$CANARY_ATTEMPTS")'* ]]; then
    echo "FAIL - cmd_selfupdate no longer retries the indeterminate case." >&2
    echo "       Gate B had no retry at all until this was added: CANARY_ATTEMPTS was read" >&2
    echo "       only by cmd_preflight, so a single transient blip in a ~40s window decided" >&2
    echo "       a release's post-publish verdict." >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$selfupdate_body" != *'rm -rf "${work:?}/home"'* ]]; then
    echo "FAIL - cmd_selfupdate does not wipe the node's state between attempts." >&2
    echo "       It must clear \$work/home and friends but NOT \$work/bin, which holds the" >&2
    echo "       downloaded previous release -- the SUBJECT of the test, not state. Gate A's" >&2
    echo "       'rm -rf \${work:?}' would delete the binary and every retry would run" >&2
    echo "       nothing; keeping \$work/home makes the retry re-read the node's persisted" >&2
    echo "       GitHub poll cooldown and reproduce the same INDETERMINATE without asking" >&2
    echo "       GitHub at all. A retry that cannot produce a different answer is not one." >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - cmd_selfupdate retries, classifies from the logs, and keeps \$work/bin"
fi

# The version gate around it. The observed-latest marker is new in #5236 and
# Gate B drives the PREVIOUS release, so for one release there is no line to
# compare; the gate must skip rather than fail. Both halves are pinned, because
# either one alone is wrong: no gate blocks a release on its predecessor's age,
# and no arming leaves Gate B permanently vacuous.
#
# Pinned in two parts, because a single literal-text grep was not enough. The
# earlier form matched `version_at_least "$prev_version" "$MARKER_LATEST_SEEN_SINCE"`
# anywhere in the body -- and `if ! version_at_least …` CONTAINS that string, so
# inverting the gate left all assertions green. Demonstrated on this branch.
# So: the decision now lives in `prev_emits_latest_seen`, whose BEHAVIOUR is
# tested below, and the call site is matched INCLUDING its `if ` prefix so a
# `!` cannot slip between them.
# shellcheck disable=SC2016  # literal source text; must not expand
if [[ "$selfupdate_body" != *'if prev_emits_latest_seen "$prev_version"; then'* ]]; then
    echo "FAIL - cmd_selfupdate no longer gates the equality check on prev_emits_latest_seen." >&2
    echo "       Expected the call site verbatim, INCLUDING the 'if ' prefix:" >&2
    echo '           if prev_emits_latest_seen "$prev_version"; then' >&2
    echo "       Matching the bare call would also match a NEGATED one. Un-gated, a previous" >&2
    echo "       release built before #5236 emits no observed-latest line and Gate B fails for" >&2
    echo "       a line that binary was never built to emit; negated, Gate B skips the check on" >&2
    echo "       every modern release and goes permanently vacuous." >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - cmd_selfupdate gates the equality check on prev_emits_latest_seen (un-negated)"
fi

# ...and that the DECISION reads the constant rather than a literal of its
# current value. Nothing else covers this: hardcoding `version_at_least "$1"
# "0.2.125"` inside the function leaves the whole suite green, the freeze
# included, because the constant is untouched and the freeze has nothing to
# disagree with. It matters at exactly the moment the constant is supposed to
# move -- a correct reword bump would then silently not take effect, which is
# the same shape as the reword hazard the freeze above exists to close.
# shellcheck disable=SC2016  # literal source text; must not expand
if [[ "$(declare -f prev_emits_latest_seen)" == *'"$MARKER_LATEST_SEEN_SINCE"'* ]]; then
    echo "ok   - prev_emits_latest_seen compares against the CONSTANT, not a literal"
else
    echo "FAIL - prev_emits_latest_seen no longer reads \$MARKER_LATEST_SEEN_SINCE." >&2
    echo "       Its body:" >&2
    declare -f prev_emits_latest_seen | sed 's/^/         /' >&2
    echo "       A literal threshold here decouples the decision from the constant, so" >&2
    echo "       moving the constant -- which is exactly what a marker reword requires --" >&2
    echo "       changes nothing and the whole suite stays green." >&2
    FAILURES=$((FAILURES + 1))
fi

# ...and what that gate DECIDES, which the text match above cannot see. An
# inversion inside the function flips both of these.
gate_b_arm_case() {
    # gate_b_arm_case <prev-version> <yes|no>
    local got
    if prev_emits_latest_seen "$1"; then got=yes; else got=no; fi
    if [[ "$got" == "$2" ]]; then
        echo "ok   - prev_emits_latest_seen '$1' -> $2 (Gate B equality check ${2/yes/arms})"
    else
        echo "FAIL - prev_emits_latest_seen '$1' said '$got', expected '$2'." >&2
        echo "       Inverted, Gate B skips its only positive assertion on every release from" >&2
        echo "       v$MARKER_LATEST_SEEN_SINCE onward -- vacuous, and silent about it." >&2
        FAILURES=$((FAILURES + 1))
    fi
}
gate_b_arm_case "$MARKER_LATEST_SEEN_SINCE" yes   # the first release that emits it
# Was a literal `0.2.125`, which is the constant's own value and so duplicated
# the case above rather than covering "after it". The constant is frozen (pinned
# below), so a later release is a literal one release on.
gate_b_arm_case "0.2.126"                   yes   # every release after it
gate_b_arm_case "0.3.0"                     yes
# BOTH releases below the constant must skip, and both are listed on purpose.
# With only 0.2.123 here the cases straddled the gap: 0.2.123 no / 0.2.125 yes is
# satisfied by ANY threshold in {0.2.124, 0.2.125}, so nothing pinned that the
# decision reads the CONSTANT rather than a literal. Verified vacuity: replacing
# the body of `prev_emits_latest_seen` with a hardcoded
# `version_at_least "$1" "0.2.124"` left the entire suite green. 0.2.124 is the
# case that closes it -- and it is the interesting one anyway, being the release
# whose tree HAS the marker but which was never published.
gate_b_arm_case "0.2.124"                   no    # in-tree but never published
gate_b_arm_case "0.2.123"                   no    # predates the marker entirely
gate_b_arm_case "0.2.99"                    no    # numeric, not lexical

# Malformed input ARMS the check rather than skipping it. Skipping is the silent
# direction; arming an unknown binary costs at worst a loud red on a release
# whose version string was already unreadable. Note this is the OPPOSITE of what
# `version_at_least` returns for the same input (below) -- deliberately, because
# the two answer different questions. See the comment on prev_emits_latest_seen.
gate_b_arm_case "not-a-version"             yes   # unreadable -> assert, don't skip
gate_b_arm_case ""                          yes

# `version_at_least` decides whether the gate above arms, so it gets its own
# cases: an off-by-one here silently disarms Gate B's only positive assertion.
version_ge_case() {
    # version_ge_case <a> <b> <yes|no>
    local got
    if version_at_least "$1" "$2"; then got=yes; else got=no; fi
    if [[ "$got" == "$3" ]]; then
        echo "ok   - version_at_least '$1' '$2' -> $3"
    else
        echo "FAIL - version_at_least '$1' '$2' said '$got', expected '$3'" >&2
        FAILURES=$((FAILURES + 1))
    fi
}
version_ge_case "0.2.124" "0.2.124" yes   # equal
version_ge_case "0.2.125" "0.2.124" yes
version_ge_case "0.2.123" "0.2.124" no    # strictly below
version_ge_case "0.3.0"   "0.2.124" yes
# Numeric, not lexical: a lexical compare puts 0.2.99 above 0.2.124 and would
# disarm the gate for every release in between.
version_ge_case "0.2.99"  "0.2.124" no

# MALFORMED INPUT MUST NOT COMPARE TRUE. The bare `sort -V` form answered TRUE
# for all three of these -- non-numeric text sorts after digits, and an empty
# operand loses to anything -- so an unreadable version or an emptied constant
# passed a comparison that has no answer. Measured on GNU coreutils 9.4 before
# the guard: cases 1 and 2 below both returned yes.
version_ge_case "not-a-version" "0.2.125" no
version_ge_case "0.2.125"       ""        no
version_ge_case ""              ""        no
# Rejected rather than ordered, because `sort -V` puts a pre-release ABOVE the
# release where semver puts it below. This repo cuts no rc tags; accepting a
# form the comparator gets backwards would be worse than refusing it.
version_ge_case "0.2.125-rc.1"  "0.2.125" no

# --- no status-consuming pipe into a short-circuiting reader ----------------
# The defect that produced this rule: `printf '%s' "$logs" | grep -aqF …` under
# `set -o pipefail`. `grep -q` exits at its first match, the producer dies with
# SIGPIPE (141), pipefail promotes 141 to the pipeline's status, and the `if`
# reads a marker that IS PRESENT as ABSENT. It is volume-dependent, so it does
# not fire below the 64 KB pipe buffer and no small fixture can see it -- it
# waits for a real log. On a 3.65 MB node log, `grep -acF` found the line while
# the piped `grep -q` exited 141 and Gate A blamed the wrong subsystem.
#
# The reason this is a pin and not just a fix: the commit that first diagnosed
# it fixed two helper functions and left four call sites inside the very
# function those helpers serve, plus two more elsewhere in these scripts. A fix
# applied where you happened to be reading is how this pattern survives.
#
# Scope is the release-gate scripts that set `pipefail`, where a wrong answer
# blocks or waves through a release. Alternatives, all used above: grep the
# FILE directly (`log_has`), match the variable with a bash glob
# (`[[ "$x" == *needle* ]]`), or take a count and test that.
#
# `head` is the same class but is NOT banned here: `$(… | head -1)` is used for
# its stdout, not its status, and banning it would be noise. Watch it manually
# when the pipeline's status is consumed.
#
# `release.sh` is in scope because it is the DRIVER: it sets `pipefail`, and its
# `verify_required_binaries` used `echo "$assets" | grep -xqF` -- the same shape,
# on the path that decides whether a release's binaries exist. Measured at 46
# false "missing" verdicts in 20000 iterations under 24-way CPU load (0 in a
# quiet window), which is why it survived: it needs a contended runner. The
# consequence was the worst-placed one in the file, `wait_for_binaries` failing
# AFTER publish and BEFORE the gateway updates and announcements -- exactly what
# the comment above `verify_release_published` warns about. The file was never
# out of the regex's reach, only out of this list's.
SIGPIPE_SCRIPTS=(
    "$CANARY_SH"
    "$SCRIPT_DIR/auto-update-canary_test.sh"
    "$SCRIPT_DIR/auto-update-canary_lifecycle_test.sh"
    "$SCRIPT_DIR/release_wait_for_binaries_test.sh"
    "$SCRIPT_DIR/release_canary_wiring_test.sh"
    "$SCRIPT_DIR/release.sh"
)
# A renamed or moved entry must fail LOUDLY. Without this, `grep`'s complaint
# about a missing file goes to the `2>/dev/null` below and the entry simply
# stops being audited -- the pin keeps reporting "ok" over a file it no longer
# reads. Same reason `pin_marker` checks `[[ -f ]]` before scraping.
for _sigpipe_script in "${SIGPIPE_SCRIPTS[@]}"; do
    if [[ ! -f "$_sigpipe_script" ]]; then
        echo "FAIL - SIGPIPE_SCRIPTS names a file that does not exist: $_sigpipe_script" >&2
        echo "       A renamed entry would otherwise drop out of the audit silently." >&2
        FAILURES=$((FAILURES + 1))
    fi
done
# Verified not to match its own defining line: after the `|` comes `[`, not
# whitespace-then-grep. A pin that finds its own needle is the self-satisfying
# shape this repo's rules file documents separately.
SIGPIPE_RE='\|[[:space:]]*grep[[:space:]]+-[a-zA-Z]*q'
sigpipe_hits="$(grep -nE "$SIGPIPE_RE" "${SIGPIPE_SCRIPTS[@]}" 2>/dev/null \
    | grep -vE ':[[:space:]]*#')"
if [[ -z "$sigpipe_hits" ]]; then
    echo "ok   - no 'pipe into grep -q' in the pipefail release-gate scripts (SIGPIPE/pipefail hazard)"
else
    echo "FAIL - a pipeline ending in 'grep -q' has come back in a pipefail script." >&2
    echo "       Under pipefail the producer's SIGPIPE (141) becomes the pipeline's status, so a" >&2
    echo "       marker that IS present reads as ABSENT once the producer exceeds the 64 KB pipe" >&2
    echo "       buffer. Small fixtures cannot see it; a real node log can. Grep the file directly," >&2
    echo "       or match the variable with [[ \"\$x\" == *needle* ]]." >&2
    printf '%s\n' "$sigpipe_hits" >&2
    FAILURES=$((FAILURES + 1))
fi

# --- TMPDIR must be scoped before either process starts ---------------------
# `client_api.rs` unconditionally `create_dir_all`s
# `std::env::temp_dir()/freenet/webs` at router construction. The directory is
# vestigial (nothing reads it), but the mkdir can FAIL -- `$TMPDIR/freenet`
# being a file, or another user's directory -- and it panics when it does, exit
# 101, before the update task spawns. `cross-compile.yml` stages the binary it
# is about to gate at exactly `/tmp/freenet`, so with an unisolated TMPDIR that
# is not a hypothetical: it blocked v0.2.124, a release whose binary was fine.
#
# This is the WEAKER of the two checks on this fix, and it should be read that
# way. `auto-update-canary_lifecycle_test.sh` case 9 covers the Gate A half
# BEHAVIOURALLY: its fake node IS the regular file staged at `$TMPDIR/freenet`,
# so a missing isolation blocks a healthy binary on a real kernel ENOTDIR. That
# is the one that matters. A source scrape asserts TEXT and cannot tell a
# working `export TMPDIR="$work/tmp"` from `export TMPDIR=/tmp`.
#
# Two things it still adds, which is why it stays:
#   - the GATE B half. Isolating the `freenet update` subshell cannot be
#     exercised without downloading and installing a real release, so for that
#     one a scrape is what there is.
#   - ORDER. An export that lands after `exec` never runs at all (exec replaces
#     the shell), and one after the `freenet update` call is equally
#     decorative.
# Cross-file (it scrapes CANARY_SH), so it cannot be satisfied by its own
# assertion text.
tmpdir_first_export="$(grep -n '^[[:space:]]*export TMPDIR=' "$CANARY_SH" | head -1 | cut -d: -f1)"
tmpdir_exports="$(grep -c '^[[:space:]]*export TMPDIR=' "$CANARY_SH")"
node_exec_line="$(grep -n '^[[:space:]]*exec timeout' "$CANARY_SH" | head -1 | cut -d: -f1)"
update_call_line="$(grep -n 'freenet" update' "$CANARY_SH" | head -1 | cut -d: -f1)"
# The anchors themselves must resolve, or the pin quietly stops auditing.
if [[ -z "$node_exec_line" || -z "$update_call_line" ]]; then
    echo "FAIL - TMPDIR pin: cannot locate the canary's launch sites (exec timeout: '${node_exec_line:-none}', freenet update: '${update_call_line:-none}')." >&2
    echo "       The pin scrapes for those two anchors; if they were renamed, update this pin rather than dropping it." >&2
    FAILURES=$((FAILURES + 1))
elif [[ -z "$tmpdir_first_export" ]]; then
    echo "FAIL - TMPDIR pin: auto-update-canary.sh no longer exports TMPDIR at all." >&2
    echo "       The node create_dir_all's \$TMPDIR/freenet/webs and PANICS (exit 101) if that path" >&2
    echo "       is a file or another user's directory. cross-compile.yml stages the gated binary at" >&2
    echo "       /tmp/freenet, which is that path -- this is what blocked v0.2.124 on a healthy binary." >&2
    FAILURES=$((FAILURES + 1))
elif (( tmpdir_first_export >= node_exec_line )); then
    echo "FAIL - TMPDIR pin: the node's 'exec timeout' (line $node_exec_line) is not preceded by an" >&2
    echo "       'export TMPDIR=' (first one at line $tmpdir_first_export). exec REPLACES the shell, so an" >&2
    echo "       export below it never runs and the node inherits the ambient temp dir." >&2
    FAILURES=$((FAILURES + 1))
elif (( tmpdir_exports < 2 )); then
    echo "FAIL - TMPDIR pin: only $tmpdir_exports 'export TMPDIR=' in auto-update-canary.sh; Gate B's" >&2
    echo "       'freenet update' subshell (line $update_call_line) needs its own. download_and_install stages" >&2
    echo "       the release tarball in tempfile::tempdir(), which follows TMPDIR, so without it Gate B" >&2
    echo "       writes outside the workdir and the header's isolation claim is true of Gate A only." >&2
    FAILURES=$((FAILURES + 1))
else
    # The Gate B export must be inside that subshell: after the node's exec,
    # before the update call. A second export anywhere else does not isolate it.
    tmpdir_gate_b="$(grep -n '^[[:space:]]*export TMPDIR=' "$CANARY_SH" | awk -F: -v lo="$node_exec_line" -v hi="$update_call_line" '$1 > lo && $1 < hi {print $1; exit}')"
    if [[ -z "$tmpdir_gate_b" ]]; then
        echo "FAIL - TMPDIR pin: no 'export TMPDIR=' between the node's exec (line $node_exec_line) and the" >&2
        echo "       'freenet update' call (line $update_call_line), so Gate B's installer runs with the ambient" >&2
        echo "       temp dir and stages a release tarball outside the canary's workdir." >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   - source pin: TMPDIR is scoped before the node exec (line $tmpdir_first_export < $node_exec_line) and before \`freenet update\` (line $tmpdir_gate_b < $update_call_line)"
    fi
fi

# --- markers must still exist in the Rust source ----------------------------
# Without this the fixtures above are a self-consistent copy of strings that
# may no longer be emitted: the canary would go quietly blind while its own
# test stayed green. Pin against the source of truth instead.
SRC="$SCRIPT_DIR/../crates/core/src/bin/freenet.rs"
AU_SRC="$SCRIPT_DIR/../crates/core/src/bin/commands/auto_update.rs"
pin_marker() {
    # pin_marker <description> <file> <needle>
    local desc="$1" file="$2" needle="$3"
    if [[ ! -f "$file" ]]; then
        echo "FAIL - $desc (source file not found: $file)" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    # Rust wraps a long string literal two ways: a plain wrap, and a
    # `\`-continuation, which also swallows the next line's indentation.
    # Squeezing newlines into spaces handled only the first -- a continuation
    # left a stray `\` mid-phrase, so the needle silently failed to match.
    # `not triggering auto-update` is emitted at two sites in freenet.rs and
    # only one has the phrase unbroken, so this pin was passing on the
    # coincidence of which site rustfmt happened to leave intact; reflowing
    # that one site would have reported the marker gone while it was still
    # emitted. Drop the continuation backslash first, then strip whitespace
    # from both sides (as the INFO-level pin below already does), so the pin
    # tracks the marker rather than the formatting.
    #
    # A bash glob match on a command substitution rather than `... | grep -qF`,
    # matching the shape the sibling pins below already use. The pipe version
    # consumed the PIPELINE's status, which under `pipefail` is 141 whenever
    # `grep -q` short-circuits and the producer still has more than a pipe
    # buffer to write -- so a marker that IS present reads as absent. Measured
    # on auto_update.rs (165 KB), matching a string on line 1:
    #     sed ... | grep -qF 'Auto-update'              -> rc=141
    #     sed ... | tr -d '[:space:]' | grep -qF ...    -> rc=0
    # It passed only because `tr` deletes every newline, leaving one enormous
    # line that grep must read to EOF before it can report a match. That is an
    # accident of the whitespace stripping, not a property of the pin: anyone
    # "simplifying" the `tr` away would silently arm the hazard on every source
    # pin in this file at once. Take the status out of the pipeline instead.
    if [[ "$(sed 's/\\$//' "$file" | tr -d '[:space:]')" == *"${needle//[[:space:]]/}"* ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc: '$needle' no longer appears in $(basename "$file")" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

pin_marker "source pin: startup-check marker"   "$SRC"    "$MARKER_CHECK_RAN"
# The completion marker is load-bearing in a way the others are not: if it stops
# being emitted, every healthy run becomes INDETERMINATE and Gate A blocks every
# release. It must also stay at INFO -- a `debug!` is compiled out of release
# builds entirely (`release_max_level_info`), which is exactly how this outcome
# came to be invisible in the first place (#5236).
pin_marker "source pin: check-complete marker" "$SRC"    "$MARKER_CHECK_COMPLETE"
# Whitespace stripped from BOTH sides, so this pins the macro rather than the
# formatting: a rustfmt reflow of the same call must not decide whether the
# canary is protected.
if [[ "$(tr -d '[:space:]' < "$SRC")" == *"tracing::info!(current=build_info::VERSION,\"${MARKER_CHECK_COMPLETE// /}"* ]]; then
    echo "ok   - source pin: check-complete marker is emitted at INFO"
else
    echo "FAIL - source pin: the '$MARKER_CHECK_COMPLETE' line is no longer an INFO-level tracing::info! in freenet.rs -- release builds compile out anything below INFO, so the canary would go blind (#5236)" >&2
    FAILURES=$((FAILURES + 1))
fi
# The trigger phrase gets the same NEGATIVE SUBTRACTION the runtime detector
# does, instead of a plain pin_marker. `MARKER_TRIGGERED` whitespace-stripped is
# `triggeringauto-update`, which is a SUBSTRING of the #4073 refusal line
# `not triggering auto-update`. So the refusal alone satisfied a plain
# containment check: the pin was tracking a line whose job is to say the
# OPPOSITE of the thing it claimed to pin. Demonstrated by rewording all four
# plain trigger sites in freenet.rs -- that assertion stayed green, and only the
# count pin below went red.
#
# `node_decided_to_update` has always subtracted the refusals; this brings the
# source pin into line with the detector it is supposed to protect. Counting
# OCCURRENCES rather than testing containment is what makes the subtraction
# possible at all.
TRIG_NEEDLE="${MARKER_TRIGGERED// /}"
SRC_SQUEEZED="$(sed 's/\\$//' "$SRC" | tr -d '[:space:]')"
# `grep -o | wc -l`: occurrences, not lines -- the squeezed source is ONE line,
# so `grep -c` would answer 1 no matter how many sites there are. Neither stage
# short-circuits, so this is not the `| grep -q` SIGPIPE shape banned below.
trig_all="$(printf '%s' "$SRC_SQUEEZED" | grep -oF -- "$TRIG_NEEDLE" | wc -l)"
trig_neg="$(printf '%s' "$SRC_SQUEEZED" | grep -oF -- "not$TRIG_NEEDLE" | wc -l)"
trig_pos=$(( trig_all - trig_neg ))
if [[ "$trig_pos" -gt 0 ]]; then
    echo "ok   - source pin: trigger phrase appears at $trig_pos site(s) that are NOT the #4073 refusal"
else
    echo "FAIL - source pin: every '$MARKER_TRIGGERED' occurrence in freenet.rs is part of" >&2
    echo "       '$MARKER_NOT_TRIGGERED' ($trig_all total, $trig_neg of them refusals)." >&2
    echo "       No site actually announces a trigger with this wording, so the canary's" >&2
    echo "       fixed-string half is dead. A containment check cannot see this: the" >&2
    echo "       refusal CONTAINS the trigger phrase, which is how this pin passed while" >&2
    echo "       all four plain trigger sites were reworded." >&2
    FAILURES=$((FAILURES + 1))
fi
pin_marker "source pin: #4073 refusal phrase"   "$SRC"    "$MARKER_NOT_TRIGGERED"
pin_marker "source pin: disabled marker"        "$SRC"    "$MARKER_DISABLED"

# --- the (marker text, first release that shipped it) PAIR ------------------
# Gate B's version gate is pinned in both directions (the behavioural cases on
# `prev_emits_latest_seen`, and the un-negated call site), but neither looks at
# the CONSTANT they compare against. Raising it 0.2.124 -> 0.2.999 left the
# whole suite green while permanently disarming Gate B's only positive
# assertion -- the same silent direction as the `!` inversion, reached by
# editing a different line.
#
# NO EQUALITY-STYLE RELATION PIN HERE. Two were tried and each was right in one
# phase and wrong in another. Writing C for the crate version, and noting that a
# dev tree's C equals the last PUBLISHED release (the bump happens inside the
# release commit), so a marker reworded today first ships in C+1 and the correct
# constant during a reword is C+1:
#
#   constant >= C   (#5290)  rests on "no published release emits the marker
#                            yet"; true until 0.2.125 published, false one
#                            second later. Detonates on the next bump.
#   constant <= C   (first   C+1 <= C is FALSE, so it goes RED for the CORRECT
#                   attempt) value during a reword, and the value that makes it
#                            green makes Gate B demand new text from a binary
#                            emitting the old one.
#
# The constant tracks RELEASE HISTORY and C tracks THIS TREE, so a relation
# asserting they stay in step cannot hold in both phases. That is the reason
# neither of the above works, and it is a claim about EQUALITY-STYLE relations
# only.
#
# CORRECTION, and it is worth stating because the wrong version of it was
# repeated through two reviews and into this comment: a LOOSE bound of the form
# `constant <= C+1` does NOT fire on a reword. C+1 <= C+1 passes. It was
# described as firing, the reviewer who proposed it withdrew it on that basis,
# and nobody checked the arithmetic until a later reviewer did. Such a bound is
# phase-independent and would catch a constant more than one release ahead. It
# is deliberately NOT added here -- the freeze below already rejects every wrong
# value, so a second overlapping pin needs its own justification -- but the
# reason is redundancy, not unsoundness. Do not re-cite the refuted claim.
#
# The freeze below is phase-independent and catches strictly more than either
# relation did: every wrong value, including the empty string and the most
# likely wrong one (the version being cut). It also cannot be tripped by
# `version.workspace = true`, which broke the relation pin's Cargo.toml read.
#
# WHY THE TWO VALUES ARE ONE BLOB, and why it is encoded. Both properties were
# forced by mutation, in two rounds:
#
#   Plaintext expectation -> a reword is done as a `sed` sweep, the sweep
#   rewrote the expectation too, suite green. An expectation stored as a copy of
#   the value it guards follows any rename of that value.
#
#   Two adjacent encoded assertions -> the reword went red, but following this
#   pin's OWN failure message (regenerate the text blob) went green again with
#   the version constant untouched, because the message never asked about it.
#   A freeze forces a decision only if the remediation cannot be performed
#   without making that decision.
#
# One blob of both values fixes both: a sweep cannot reach it, and the recipe
# cannot be run without supplying a version. Mutation-test the REMEDIATION PATH,
# not just the regression, before trusting any replacement.
#
# WHAT IT STILL CANNOT DO, so nobody reads more into a green run than is there:
# it makes the question unavoidable, it does not verify the answer. Regenerate
# the blob with the new text and the OLD version and this goes green -- measured.
# No local check can know which release will ship a given wording, and any
# relation that tried to infer it is back to the phase problem above. The value
# here is that the version cannot be left unconsidered, not that it is correct.
#
# 0.2.125 is the first release a running node can REACH whose binary emits this
# text: the marker landed in the 0.2.124 tree, but 0.2.124 was never published
# (Gate A blocked it, #5290) and a draft does not appear at `/releases/latest`.
# Ground-truthed by downloading the published v0.2.125 musl asset and finding
# the string in it.
#
# WHAT THIS DOES NOT COVER. `assert_detection_healthy` greps SEVEN markers, and
# in Gate B every one of them is put to the PREVIOUS release's binary -- the one
# place in the pipeline where a published binary is asked for text this tree
# chose. Two are frozen: MARKER_LATEST_SEEN here, MARKER_PARSE_FAIL immediately
# below. The remaining five are not:
#
#   MARKER_DISABLED, MARKER_CHECK_RAN, MARKER_CHECK_COMPLETE,
#   MARKER_TRIGGERED_RE        -- the four tracked by #5309
#   MARKER_FETCH_FAIL          -- grepped too, and NOT named in #5309's
#                                 enumeration (that issue counts five markers
#                                 and misses this one and MARKER_PARSE_FAIL).
#                                 Its reword direction is the loud one: an old
#                                 binary's fetch failure would stop being
#                                 classified INDETERMINATE and fall through to
#                                 the equality check, which reports a missing
#                                 observed-latest line. Wrong diagnosis, but red
#                                 rather than green.
#
# So this class is NOT closed, and neither freeze closes it. The asymmetry that
# creates it is real rather than an oversight: Gate A runs a binary built from
# THIS tree, so a reword there is self-consistent, and Gate B is the only place
# an older binary is read.
#
# To change it deliberately, re-state BOTH values:
#   printf '%s\n%s' '<marker text>' '<first release shipping it>' | base64 | tr -d '\n'
# (`base64 -w0` is GNU-only and fails on macOS, which is where someone reading
# this failure is most likely to be.)
MARKER_PAIR_FROZEN_B64='U3RhcnR1cCB1cGRhdGUgY2hlY2s6IEdpdEh1YiByZXBvcnRzIGxhdGVzdCByZWxlYXNlCjAuMi4xMjU='
marker_pair_frozen="$(printf '%s' "$MARKER_PAIR_FROZEN_B64" | base64 -d)"
marker_pair_live="$(printf '%s\n%s' "$MARKER_LATEST_SEEN" "$MARKER_LATEST_SEEN_SINCE")"

if [[ -z "$marker_pair_frozen" ]]; then
    # A failed decode would leave the expectation empty and make the comparison
    # vacuous in the quiet direction.
    echo "FAIL - could not decode MARKER_PAIR_FROZEN_B64; the marker/version freeze is not running" >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$marker_pair_live" == "$marker_pair_frozen" ]]; then
    echo "ok   - (MARKER_LATEST_SEEN, MARKER_LATEST_SEEN_SINCE) frozen as a pair at v$MARKER_LATEST_SEEN_SINCE"
else
    echo "FAIL - the frozen (marker text, first release that shipped it) pair changed." >&2
    echo "         was: '$(printf '%s' "$marker_pair_frozen" | head -1)' @ v$(printf '%s' "$marker_pair_frozen" | tail -1)" >&2
    echo "         now: '$MARKER_LATEST_SEEN' @ v$MARKER_LATEST_SEEN_SINCE" >&2
    echo "       These are frozen TOGETHER because they only make sense together." >&2
    echo "       If the TEXT changed: no published binary emits the new wording yet, so" >&2
    echo "       MARKER_LATEST_SEEN_SINCE must move to the release that will first SHIP it" >&2
    echo "       -- normally the NEXT one, since the bump happens inside the release commit." >&2
    echo "       Leave it and Gate B demands the new wording from a binary that emits the old" >&2
    echo "       one: a POST-PUBLISH red canary and a Matrix alarm that blames the node." >&2
    echo "       Nothing else in this suite would have told you -- the source pin interpolates" >&2
    echo "       \$MARKER_LATEST_SEEN and follows a rename by construction." >&2
    echo "       If only the VERSION changed: raising it skips Gate B's only positive" >&2
    echo "       assertion for every release below the new value, silently. Unless you are" >&2
    echo "       here because of a reword, the fix is to put it back." >&2
    echo "       Once decided, re-state BOTH:" >&2
    # printf, not echo: the recipe contains \n sequences that must reach the
    # reader literally, and `echo`'s handling of those is shell-dependent.
    # shellcheck disable=SC2016  # the `$(...)` is literal recipe text for the reader
    printf '         MARKER_PAIR_FROZEN_B64="$(printf %s %s | base64 | tr -d %s)"\n' \
        "'%s\\n%s'" "'$MARKER_LATEST_SEEN' '<release>'" "'\\n'" >&2
    FAILURES=$((FAILURES + 1))
fi
# --- the #5221 signature text, frozen ---------------------------------------
# MARKER_PARSE_FAIL is the marker this whole canary was built around: it is the
# #5221 regression's log signature, and `assert_detection_healthy`'s only
# NEGATIVE check greps for it.
#
# WHY IT NEEDS A FREEZE WHEN IT ALREADY HAS TWO SOURCE PINS. `pin_warn_literal`
# below interpolates $MARKER_PARSE_FAIL, so it follows a rename BY CONSTRUCTION
# -- the same defect that left MARKER_LATEST_SEEN's source pin unable to notice
# a reword. Measured on this branch: a `sed` sweep replacing the text in the
# three files a developer must touch (auto-update-canary.sh, this file's
# fixtures, auto_update.rs) leaves ALL assertions green. Sweeping only the first
# two of those goes red -- which is worse than useless as a guard, because it
# tells whoever does the incomplete sweep that finishing it is the fix.
#
# WHY THIS ONE IS THE DANGEROUS MEMBER OF THE CLASS, and the reason it is frozen
# ahead of #5309's four. The other markers feed POSITIVE checks, so losing them
# makes Gate B red against a healthy release: loud, and someone investigates.
# This one feeds a negative check. Reword it and the grep stops matching the
# text every ALREADY-PUBLISHED binary emits, so a previous release carrying the
# live #5221 bug logs check-ran + reworded-warn + check-complete and Gate B
# reports "OK: parsed GitHub's response". A silent false PASS, on the exact
# failure this canary exists to catch, on the exact binary Gate B exists to
# question.
#
# WHY NO COMPANION VERSION CONSTANT, i.e. why this is a single value and not the
# pair above. MARKER_LATEST_SEEN needs MARKER_LATEST_SEEN_SINCE because Gate B
# SKIPS its positive check for binaries that predate the marker, and that skip
# needs a version to compare against. There is no skip branch here and no
# constant to leave stale, so there is no second value a remediation could
# quietly avoid restating.
#
# WHAT TO ACTUALLY DO IF THIS FIRES ON A DELIBERATE REWORD is in the failure
# message, and it is not "regenerate the blob and move on": no published binary
# emits the new wording, so re-stating the freeze alone hands Gate B a grep that
# matches nothing older than the next release.
MARKER_PARSE_FAIL_FROZEN_B64='U3RhcnR1cCB1cGRhdGUgY2hlY2s6IGZhaWxlZCB0byBwYXJzZQ=='
# A decode failure must not leave the expectation empty and the comparison
# vacuous -- the quiet direction, and the one this freeze exists to remove.
# `printf | base64 -d` reads to EOF and cannot short-circuit, so it is not the
# `| grep -q` SIGPIPE shape banned elsewhere in this file.
if ! parse_fail_frozen="$(printf '%s' "$MARKER_PARSE_FAIL_FROZEN_B64" | base64 -d 2>/dev/null)"; then
    parse_fail_frozen=""
fi
if [[ -z "$parse_fail_frozen" ]]; then
    echo "FAIL - could not decode MARKER_PARSE_FAIL_FROZEN_B64; the #5221 signature freeze is not running" >&2
    FAILURES=$((FAILURES + 1))
elif [[ "$MARKER_PARSE_FAIL" == "$parse_fail_frozen" ]]; then
    echo "ok   - MARKER_PARSE_FAIL (the #5221 signature) frozen against a rename sweep"
else
    echo "FAIL - the frozen #5221 signature text changed." >&2
    echo "         was: '$parse_fail_frozen'" >&2
    echo "         now: '$MARKER_PARSE_FAIL'" >&2
    echo "       This marker feeds the canary's only NEGATIVE check, so a reword fails" >&2
    echo "       SILENTLY and in the passing direction. Gate B greps the PREVIOUS" >&2
    echo "       release's binary, which emits the OLD text; a grep for the new text" >&2
    echo "       matches nothing, so a published release carrying the live #5221 bug" >&2
    echo "       logs check-ran + warn + check-complete and Gate B reports" >&2
    echo "       \"OK: parsed GitHub's response\". Every already-published binary is" >&2
    echo "       affected, not just the next one." >&2
    echo "       Nothing else in this suite would have told you: pin_warn_literal" >&2
    echo "       interpolates \$MARKER_PARSE_FAIL and follows a rename by construction." >&2
    echo "       DECIDE THIS BEFORE RE-STATING THE FREEZE -- re-stating it alone is not" >&2
    echo "       the fix, it just makes the suite agree with the blind spot:" >&2
    echo "         (a) keep matching the OLD text as well, so Gate B can still see" >&2
    echo "             #5221 on binaries that are already out there. MARKER_TRIGGERED_RE" >&2
    echo "             is the precedent for an alternation marker in this file; or" >&2
    echo "         (b) accept knowingly that Gate B cannot detect #5221 on any release" >&2
    echo "             published before the new wording ships." >&2
    echo "       Once decided, re-state it:" >&2
    # printf, not echo: the recipe must reach the reader literally, and echo's
    # handling of backslash sequences is shell-dependent. `base64 -w0` is
    # GNU-only and fails on macOS, which is where someone reading this is most
    # likely to be -- hence `tr -d '\n'`.
    # shellcheck disable=SC2016  # the `$(...)` is literal recipe text for the reader
    printf '         MARKER_PARSE_FAIL_FROZEN_B64="$(printf %s %s | base64 | tr -d %s)"\n' \
        "'%s'" "'$MARKER_PARSE_FAIL'" "'\\n'" >&2
    FAILURES=$((FAILURES + 1))
fi

# --- ...and the BEHAVIOURAL half, which is the one that forces the decision --
# The freeze above notifies. It cannot force, and the difference is the whole
# lesson of this file's history, so it is worth being exact about what was
# measured rather than asserting a property.
#
# Reword the marker, then do EXACTLY what the failure message's recipe says and
# nothing else: suite green, every assertion passing. Then hand the resulting
# canary a verbatim real v0.2.121 log -- the live #5221 break, the thing Gate B
# exists to catch on the previous release -- and it answers:
#
#     OK: startup update check ran to completion and parsed GitHub's response.
#     RC=0
#
# That is the round-4 shape restated at one remove: "a freeze forces a decision
# only if the remediation cannot be performed without making that decision," and
# re-stating a single value can always be performed.
#
# So the DETECTOR is asserted against a log line no sweep can rewrite: the
# historical WARN, base64 here, driven through the real `assert_detection_healthy`
# rather than compared against a constant. After a reword this stays RED until
# the canary can actually still read an already-published binary -- which is
# option (a) in the message above, an alternation over old and new wording. The
# only way to green it otherwise is to delete this assertion, which is the
# deliberate, reviewable form of option (b).
#
# Scaffolding lines interpolate the LIVE $MARKER_CHECK_RAN on purpose, so this
# stays a pin on MARKER_PARSE_FAIL alone and does not go red for a reword of a
# marker #5309 owns. The diagnosis is asserted as well as the exit code: rc=1 is
# also what "the startup update check never ran" returns, and a pin that cannot
# tell those apart would pass while reporting the wrong subsystem.
PARSE_FAIL_HISTORICAL_WARN_B64='MjAyNi0wOC0wOFQwMTo1OTozNi4xMTEwNzNaICBXQVJOIGZyZWVuZXQ6OmNvbW1hbmRzOjphdXRvX3VwZGF0ZTogU3RhcnR1cCB1cGRhdGUgY2hlY2s6IGZhaWxlZCB0byBwYXJzZSBsYXRlc3QgdmVyc2lvbiAndjAuMi4xMjInOiB1bmV4cGVjdGVkIGNoYXJhY3RlciAndicgd2hpbGUgcGFyc2luZyBtYWpvciB2ZXJzaW9uIG51bWJlcg=='
if ! historical_warn="$(printf '%s' "$PARSE_FAIL_HISTORICAL_WARN_B64" | base64 -d 2>/dev/null)"; then
    historical_warn=""
fi
if [[ -z "$historical_warn" ]]; then
    echo "FAIL - could not decode PARSE_FAIL_HISTORICAL_WARN_B64; the #5221 detection test is not running" >&2
    FAILURES=$((FAILURES + 1))
else
    historical_dir="$(mktemp -d "$TMPROOT/historical.XXXXXX")"
    printf '%s\n%s\n' \
        "2026-08-08T01:59:35.950835Z  INFO freenet: $MARKER_CHECK_RAN current=\"0.2.121\" jitter_secs=40" \
        "$historical_warn" \
        > "$historical_dir/freenet.2026-08-08-01.log"
    historical_stderr="$(assert_detection_healthy "$historical_dir" 2>&1 >/dev/null)"
    historical_rc=$?
    if [[ "$historical_rc" == 1 && "$historical_stderr" == *"could not parse the version GitHub returned"* ]]; then
        echo "ok   - the canary still detects #5221 in a log an ALREADY-PUBLISHED binary emits"
    else
        echo "FAIL - the canary no longer detects #5221 in the log a published binary emits." >&2
        echo "         fixture line: $historical_warn" >&2
        echo "         got exit $historical_rc, wanted 1 with 'could not parse the version GitHub returned'" >&2
        echo "         stderr: ${historical_stderr:-<none>}" >&2
        echo "       This is Gate B's subject: it greps the PREVIOUS release's binary, and that" >&2
        echo "       binary emits the text above no matter what this tree calls the marker." >&2
        echo "       A canary that cannot match it reports \"OK: parsed GitHub's response\" for a" >&2
        echo "       release carrying the live #5221 bug -- measured, exit 0." >&2
        echo "       If you are here after rewording MARKER_PARSE_FAIL: re-stating the frozen" >&2
        echo "       blob above does NOT fix this, and going green is not the goal. Make the" >&2
        echo "       detector match the OLD wording as well (MARKER_TRIGGERED_RE is the" >&2
        echo "       precedent for an alternation), or delete this assertion deliberately and" >&2
        echo "       say in the commit message that Gate B is now blind to #5221 on every" >&2
        echo "       release published before the new wording ships." >&2
        FAILURES=$((FAILURES + 1))
    fi
fi

# The parse-failure marker gets a STRONGER pin than pin_marker can give.
# `failed to parse latest version` appears twice in auto_update.rs: the
# production warn!, and a comment inside its own `#[cfg(test)] mod tests`
# block. A whole-file grep is satisfied by the COMMENT, so rewording the
# real warn! left every assertion green -- and a node carrying the #5221 bug
# then logs check-ran + reworded-warn + check-complete, which the canary
# reports as "OK: parsed GitHub's response". The gate this PR exists to
# install would have been removable by an ordinary log reword, with CI green
# throughout. Bound the pin to the emitting call instead, so what is pinned
# is the code that runs. Both arms are pinned: they fail the same way and
# neither may drift silently.
pin_warn_literal() {
    # pin_warn_literal <description> <file> <literal-prefix>
    local desc="$1" file="$2" literal="$3"
    if [[ ! -f "$file" ]]; then
        echo "FAIL - $desc (source file not found: $file)" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    # Whitespace stripped from both sides, as the INFO-level pin above does,
    # so a rustfmt reflow cannot decide whether the canary is protected.
    if [[ "$(tr -d '[:space:]' < "$file")" == *"tracing::warn!(\"${literal//[[:space:]]/}"* ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc: no 'tracing::warn!' in $(basename "$file") still emits" >&2
        echo "       '$literal' -- the canary greps for that text, so rewording it here" >&2
        echo "       makes a broken updater indistinguishable from a healthy one (#5236)." >&2
        FAILURES=$((FAILURES + 1))
    fi
}
pin_warn_literal "source pin: parse-failure marker (latest-version arm)" \
    "$AU_SRC" "$MARKER_PARSE_FAIL latest version"
pin_warn_literal "source pin: parse-failure marker (current-version arm)" \
    "$AU_SRC" "$MARKER_PARSE_FAIL current version"
pin_marker "source pin: fetch-failure marker"   "$AU_SRC" "$MARKER_FETCH_FAIL"

# The observed-latest marker, pinned to its emitting `tracing::info!` for the
# same reason the parse-failure arms are pinned to their `warn!`: a whole-file
# grep tracks prose, and this one carries the gate's only POSITIVE assertion.
# It must also stay at INFO -- `release_max_level_info` compiles out anything
# below, so a `debug!` here would delete the equality check from every shipped
# binary while leaving all 30 assertions green.
if [[ "$(tr -d '[:space:]' < "$AU_SRC")" == *"tracing::info!(latest=%latest,\"${MARKER_LATEST_SEEN//[[:space:]]/}"* ]]; then
    echo "ok   - source pin: observed-latest marker is emitted at INFO with a latest= field"
else
    echo "FAIL - source pin: no 'tracing::info!(latest = %latest, \"$MARKER_LATEST_SEEN\")' in auto_update.rs." >&2
    echo "       Gate A's only positive assertion reads that line and that field. Without it the" >&2
    echo "       gate falls back to 'the node did not complain', which a silently-wrong comparator" >&2
    echo "       satisfies (#5236, review finding 32). A 'debug!' here is equally fatal: release" >&2
    echo "       builds compile it out." >&2
    FAILURES=$((FAILURES + 1))
fi

# --- the trigger-site ENUMERATION -------------------------------------------
# `MARKER_TRIGGERED_RE` has to match every site that requests an update. It
# missed the urgent one at :609 for as long as that site has existed, because
# the marker was a fixed string and the site says "triggering IMMEDIATE
# auto-update".
#
# The expected count must NOT come from the regex being audited. The first
# version of this pin computed it as `grep -cE "$MARKER_TRIGGERED_RE"`, so a
# site the regex failed to match was invisible to the count as well -- the pin
# could not detect the one thing it exists to detect. Demonstrated: adding a
# sixth site worded "triggering a fresh auto-update" left this suite fully
# green, including this assertion. (Rewording an EXISTING site was caught, so
# the pin was not useless, just blind in the direction that matters most.)
#
# Derive the expectation from the CODE DECISION instead. Every real trigger
# ends in `update_tx.send(...)`, which is what makes the node exit 42; the log
# line is commentary on that send. Two anchors, neither of them the regex:
#
#   total sends            -- every path that requests an update, whatever it
#                             logs. Catches a site added with a send spelled
#                             some other way.
#   versioned sends        -- `update_tx.send(new_version)`, the sites that
#                             detected a specific newer release. These are
#                             exactly the sites that must carry a trigger log
#                             line, so this is the number the regex must find.
#
# The remaining sends are the two forced-exit paths that send a SENTINEL rather
# than a detected version (`"unknown (hard timeout)"`, `"unknown (gateway
# mismatch)"`). They deliberately carry no trigger phrase -- they are "leave
# for auto-update", not "this release detected". `node_decided_to_update` does
# not see them, which is correct for the gates: neither is reachable in a
# canary run (both need hours of isolation with a version mismatch).
EXPECTED_SEND_SITES=7
EXPECTED_TRIGGER_SITES=5
# shellcheck disable=SC2016  # literal source text, must not expand
total_sends="$(grep -cF 'update_tx.send(' "$SRC" 2>/dev/null || echo 0)"
# shellcheck disable=SC2016
versioned_sends="$(grep -cF 'update_tx.send(new_version)' "$SRC" 2>/dev/null || echo 0)"
actual_sites="$(grep -cE "$MARKER_TRIGGERED_RE" "$SRC" 2>/dev/null || echo 0)"
actual_refusals="$(grep -cF "$MARKER_NOT_TRIGGERED" "$SRC" 2>/dev/null || echo 0)"
actual_triggers=$((actual_sites - actual_refusals))

if [[ "$total_sends" -eq "$EXPECTED_SEND_SITES" && "$versioned_sends" -eq "$EXPECTED_TRIGGER_SITES" ]]; then
    echo "ok   - source pin: freenet.rs has $EXPECTED_SEND_SITES update_tx.send sites, $EXPECTED_TRIGGER_SITES of them version-detecting"
else
    echo "FAIL - source pin: freenet.rs has $total_sends 'update_tx.send(' sites ($versioned_sends versioned)," >&2
    echo "       expected $EXPECTED_SEND_SITES ($EXPECTED_TRIGGER_SITES versioned). An auto-update trigger path was added or removed." >&2
    echo "       Update the enumeration comment in auto-update-canary.sh, MARKER_TRIGGERED_RE if the new" >&2
    echo "       site's wording needs it, and these two counts -- together." >&2
    grep -nF 'update_tx.send(' "$SRC" >&2
    FAILURES=$((FAILURES + 1))
fi

# ...and the regex must match every one of the version-detecting sites. This is
# the assertion the old count could not make, because both sides of it were the
# same grep.
if [[ "$actual_triggers" -eq "$versioned_sends" ]]; then
    echo "ok   - source pin: MARKER_TRIGGERED_RE matches all $versioned_sends version-detecting trigger sites"
else
    echo "FAIL - source pin: MARKER_TRIGGERED_RE matches $actual_triggers trigger log lines, but freenet.rs has" >&2
    echo "       $versioned_sends version-detecting trigger sites ('update_tx.send(new_version)')." >&2
    echo "       ($actual_sites regex matches minus $actual_refusals refusals.) If the regex matches FEWER, a" >&2
    echo "       trigger site is worded so the canary cannot see it -- a node that took that path reads as one" >&2
    echo "       that never decided to update, which is how the urgent site at :609 went unseen. If it matches" >&2
    echo "       MORE, the regex is picking up prose. Either way, reconcile MARKER_TRIGGERED_RE with the" >&2
    echo "       enumeration comment in auto-update-canary.sh." >&2
    grep -nE "$MARKER_TRIGGERED_RE" "$SRC" >&2
    FAILURES=$((FAILURES + 1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All auto-update-canary assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
