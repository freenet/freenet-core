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
check "equality: unset expected-latest -> still passes, but says it skipped" \
    0 "$SEEN_OK"

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
    if sed 's/\\$//' "$file" | tr -d '[:space:]' | grep -qF "${needle//[[:space:]]/}"; then
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
pin_marker "source pin: trigger phrase"         "$SRC"    "$MARKER_TRIGGERED"
pin_marker "source pin: #4073 refusal phrase"   "$SRC"    "$MARKER_NOT_TRIGGERED"
pin_marker "source pin: disabled marker"        "$SRC"    "$MARKER_DISABLED"
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
# auto-update". Pin the COUNT so a sixth site cannot be added silently: a new
# site that the regex does not match makes the count too low, and one it does
# match makes it too high -- either way the enumeration in the canary's marker
# comment gets revisited instead of quietly rotting.
EXPECTED_TRIGGER_SITES=5
actual_sites="$(grep -cE "$MARKER_TRIGGERED_RE" "$SRC" 2>/dev/null || echo 0)"
actual_refusals="$(grep -cF "$MARKER_NOT_TRIGGERED" "$SRC" 2>/dev/null || echo 0)"
actual_triggers=$((actual_sites - actual_refusals))
if [[ "$actual_triggers" -eq "$EXPECTED_TRIGGER_SITES" ]]; then
    echo "ok   - source pin: freenet.rs has exactly $EXPECTED_TRIGGER_SITES trigger sites, all matched by MARKER_TRIGGERED_RE"
else
    echo "FAIL - source pin: expected $EXPECTED_TRIGGER_SITES auto-update trigger sites in freenet.rs, found $actual_triggers" >&2
    echo "       ($actual_sites regex matches minus $actual_refusals refusals). Either a site was added/removed," >&2
    echo "       or a new one is worded so MARKER_TRIGGERED_RE does not match it -- which is how the" >&2
    echo "       urgent site at :609 went unseen. Update the enumeration comment in" >&2
    echo "       auto-update-canary.sh and this count together." >&2
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
