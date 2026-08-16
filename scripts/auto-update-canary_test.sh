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

# --- fixtures for the DECISION check ----------------------------------------
#
# All three pass every one of `assert_detection_healthy`'s seven checks. That is
# the point: the decision the node reached is not one of the things those checks
# look at, so these are the logs a broken comparator produces and the old gate
# called healthy.
#
# The binary under test in Gate A is NEWER than `releases/latest` by
# construction (its own release is still a draft), so this node found something
# newer than itself that does not exist. An inverted `latest_ver > current_ver`
# in compare_versions_for_startup produces exactly it, and the update requested
# is a self-DOWNGRADE to 0.2.122 from 0.2.123.
SEEN_TRIGGERED='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.122
2026-08-08T02:00:00.412000Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.122'

# The #4073 refusal, verbatim from freenet.rs. It reaches the completion line
# like every other non-triggering outcome, so before the decision check this log
# was indistinguishable from an ordinary healthy one -- and it means the node
# will refuse EVERY release it is ever offered.
SEEN_REFUSED_4073='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.122
2026-08-08T02:00:00.350000Z  WARN freenet: Startup check: newer version is locally blocked (crash-loop known-bad pin or repeated install failures); not triggering auto-update (#4073)
2026-08-08T02:00:00.412000Z  INFO freenet: Startup update check complete: staying on the current version current="0.2.123"'

# --- the positive cases -----------------------------------------------------
check "healthy: check ran, parsed, triggered -> pass" 0 "$HEALTHY"
check "healthy: check ran and completed up-to-date -> pass" 0 "$HEALTHY_UP_TO_DATE"

# --- the regression this canary exists to catch -----------------------------
check "broken: #5221 unparseable tag -> fail" 1 "$BROKEN" \
    "could not parse the version GitHub returned"

# BOTH markers in one log, which pins the ORDER of the two branches. The
# parse-failure check runs BEFORE the fetch-failure check, and that ordering is
# the FIRST of two things keeping a real #5221 out of the environmental
# classification
# Gate B gained this round. Swap the two branches -- an innocent-looking tidy,
# "infra check before product check" -- and this log returns 2 instead of 1,
# Gate B treats it as a candidate for the environmental class. The runner probe
# is the second guard and would usually catch it from there, so this is an
# ordering hazard rather than a guaranteed false quiet -- but the two together
# are what make it safe, and a guard that only holds when its sibling also does
# is worth its own case.
#
# Realistic rather than contrived: a node whose startup fetch failed and whose
# periodic re-poll then returned an unparseable tag logs exactly this, and so
# does the reverse. Until this case existed no fixture in the suite held both.
BROKEN_PLUS_FETCH_FAIL="$BROKEN
2026-08-08T02:05:00.000000Z  WARN freenet::commands::auto_update: Startup update check: failed to fetch latest version: error sending request. Continuing with current binary."
check "broken: a parse failure OUTRANKS a fetch failure in the same log" 1 \
    "$BROKEN_PLUS_FETCH_FAIL" "could not parse the version GitHub returned"

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

# --- WHICH DECISION the updater reached -------------------------------------
#
# Every assertion above this point is satisfied by a node that reached the WRONG
# DECISION. `assert_detection_healthy` never looks at one: `node_check_settled`
# accepts a completion line OR a trigger, the equality check validates the
# version compared AGAINST rather than what was done with it, and the #4073
# refusal reaches the completion line like any other non-triggering outcome. So
# a shipping binary with an inverted `latest_ver > current_ver` passes all seven
# checks and ships -- and it does not quietly do nothing, it requests a
# self-DOWNGRADE to the older release, exits 42, and the supervisor loop repeats
# it forever. `docs/RELEASING.md` has described this hole since #5236.
#
# TWO functions, tested separately, because they fail differently:
#   gate_a_expected_decision -- WHICH decision is obliged, from the two versions
#   assert_gate_a_decision   -- whether the node reached it
#
# The split is what keeps the gate from false-BLOCKING. Gate A's subject is
# newer than `releases/latest` on the normal release path, but `cross-compile.yml`
# is checked out AT THE TAG, so re-running an older release's workflow (or a
# hotfix cut on an older line) puts a genuinely older binary under the gate,
# where deciding to update is CORRECT. A blocking gate that fails a healthy
# release does not cost one release; it teaches people to override it.
expect_case() {
    # expect_case <shipping> <latest> <expected: decline|update|unknown>
    local got
    got="$(gate_a_expected_decision "$1" "$2")"
    if [[ "$got" == "$3" ]]; then
        echo "ok   - gate_a_expected_decision '$1' vs '$2' -> $3"
    else
        echo "FAIL - gate_a_expected_decision '$1' vs '$2' said '$got', expected '$3'" >&2
        FAILURES=$((FAILURES + 1))
    fi
}
expect_case "0.2.127" "0.2.126" decline   # the normal release path: draft, so newer
expect_case "0.2.126" "0.2.126" decline   # equal is NOT newer -- compare_versions_for_startup
                                          # returns Some only for STRICTLY greater
expect_case "0.2.126" "0.2.127" update    # a workflow re-run at an older tag
# Numeric, not lexical, and this is the load-bearing pair: a lexical compare puts
# 0.2.9 ABOVE 0.2.10, so it would demand a DECLINE from a node that must update
# and block the release for being right.
expect_case "0.2.9"   "0.2.10"  update
expect_case "0.2.10"  "0.2.9"   decline
# Undecidable rather than guessed. Guessing either way risks blocking a healthy
# release on a version string nobody can read, with a message about auto-update.
expect_case ""              "0.2.126" unknown
expect_case "not-a-version" "0.2.126" unknown
expect_case "0.2.126"       ""        unknown

# The optional <want-stdout> is separate from <msg> because the two streams carry
# different things and only one of them was ever asserted. Failures go to STDERR
# via `fail`/`note`/`warn`; the `OK:` CONFIRMATIONS go to STDOUT via `log`.
#
# Those confirmations are what the PR describes as letting a reader of a green job
# tell WHICH assertion actually ran -- and they were themselves unasserted:
# deleting `log "OK: the node declined to update, ..."` outright left every suite
# GREEN, because the green-side cases matched "MUST decline to update", which
# `cmd_preflight` logs from its OWN arm-selection `case`. A borrowed neighbour
# again, in the one direction that makes a blocking gate less legible rather than
# less correct. Message-only severity, but the whole purpose of those lines is
# that a human can see them, so they get an anchor at the site that emits them.
decision_case() {
    # decision_case <desc> <expected-decision-arg> <expected-exit> <log> [msg] [want-stdout]
    local desc="$1" expect="$2" want_rc="$3" content="$4" want_msg="${5:-}" want_out="${6:-}"
    local dir actual stderr stdout outfile
    dir="$(mktemp -d "$TMPROOT/dec.XXXXXX")"
    outfile="$(mktemp "$TMPROOT/decout.XXXXXX")"
    if [[ -n "$content" ]]; then
        printf '%s\n' "$content" > "$dir/freenet.2026-08-08-02.log"
    fi
    stderr="$(assert_gate_a_decision "$dir" "$expect" 2>&1 >"$outfile")"
    actual=$?
    stdout="$(cat "$outfile")"
    rm -f "$outfile"
    if [[ "$actual" != "$want_rc" ]]; then
        echo "FAIL - decision: $desc (got exit $actual, expected $want_rc)" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    if [[ -n "$want_msg" && "$stderr" != *"$want_msg"* ]]; then
        echo "FAIL - decision: $desc (exit $actual correct, but diagnosis wrong)" >&2
        echo "       wanted message containing: $want_msg" >&2
        echo "       got: ${stderr:-<nothing on stderr>}" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    if [[ -n "$want_out" && "$stdout" != *"$want_out"* ]]; then
        echo "FAIL - decision: $desc (exit $actual correct, but the OK confirmation is missing)" >&2
        echo "       wanted stdout containing: $want_out" >&2
        echo "       got: ${stdout:-<nothing on stdout>}" >&2
        echo "       These 'OK:' lines are how a reader of a GREEN job tells which assertion" >&2
        echo "       ran. Without them a passing gate and a skipped one look identical." >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    echo "ok   - decision: $desc"
}

# The GREEN side first, and it matters as much as the red ones: this gate BLOCKS
# publication, so a version of it that could only fail would stall the first
# release that ran it and be indistinguishable from a working one until then.
# The 6th argument anchors the OK: confirmation on STDOUT at the site that emits
# it. Without it the green side asserted only cmd_preflight's arm-selection log,
# and both `log "OK: ..."` lines could be deleted with every suite still green.
decision_case "healthy Gate A run declines, as it must -> pass" \
    decline 0 "$SEEN_OK" "" "OK: the node declined to update"
# ...and the two red ones, which are the whole reason the check exists.
decision_case "decided to UPDATE with nothing newer to update to -> fail" \
    decline 1 "$SEEN_TRIGGERED" "DECIDED TO UPDATE"
decision_case "refused via the #4073 local gate on a fresh HOME -> fail" \
    decline 1 "$SEEN_REFUSED_4073" "locally blocked (#4073)"

# The OTHER direction, so the conditional is tested rather than assumed. Here the
# binary really is older than the latest release and updating is correct.
decision_case "older binary decides to update, as it must -> pass" \
    update 0 "$SEEN_TRIGGERED" "" "OK: the node decided to update"
decision_case "older binary stays put on a published newer release -> fail" \
    update 1 "$SEEN_OK" "did NOT decide to update"

# The #4073 check is DIRECTION-INDEPENDENT: it rests only on the canary's fresh
# isolated HOME, which holds in every arm. These two are what pin that -- delete
# the refusal check and the `decline` case above still fails on the trigger, so
# on its own it does not prove the refusal check runs at all.
decision_case "the #4073 refusal is a fault in the update arm too" \
    update 1 "$SEEN_REFUSED_4073" "locally blocked (#4073)"
decision_case "the #4073 refusal is a fault even when the direction is unknown" \
    unknown 1 "$SEEN_REFUSED_4073" "locally blocked (#4073)"

# An undecidable direction skips ONE arm, and says so. Silence here would be the
# vacuous escape hatch this gate family keeps having to remove: a reader of a
# green log could not tell an asserted decision from an unasserted one.
decision_case "an unknown direction skips the decision arm LOUDLY" \
    unknown 0 "$SEEN_TRIGGERED" "did NOT assert which decision"
# ...as a GitHub ANNOTATION, asserted HERE rather than only through cmd_preflight.
# Measured: with the annotation pinned only on the end-to-end case, swapping this
# arm's `warn` back to a plain `note` left the whole suite GREEN -- because
# cmd_preflight emits its own `::warning::` when it picks the unknown arm, so the
# end-to-end assertion was satisfied by the CALLER's annotation and never looked
# at this one. The pure function has no such neighbour to borrow from, which is
# what makes this the anchor that can actually fail.
decision_case "the unknown-arm skip is a ::warning:: annotation, not a plain line" \
    unknown 0 "$SEEN_TRIGGERED" "::warning::"

# Volume-resistance, for the reason the block above this one exists: the real
# Gate A log is megabytes, and a status-consuming pipe that short-circuits reads
# a marker that IS present as ABSENT once it passes the 64 KB pipe buffer. Both
# directions, because a false GREEN here ships the downgrade loop.
decision_case "volume: a trigger behind >64KB of later output still fails" \
    decline 1 "$SEEN_TRIGGERED
$BULK" "DECIDED TO UPDATE"
decision_case "volume: a healthy decline behind >64KB of later output still passes" \
    decline 0 "$SEEN_OK
$BULK"

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
# --- the probe itself, before the decision that consumes it -----------------
# `runner_can_reach_github` is the corroboration. Every way it can MALFUNCTION
# used to be indistinguishable from "the network is down", because it went
# through `resolve_expected_latest`, which collapses a 403, a captive portal, a
# changed redirect shape and a missing curl all to `return 1`. Read as
# corroboration, every one of those bought the quiet path.
#
# Only connect-class curl exits may now answer "cannot reach". Stubbing `curl`
# rather than the function, so what is tested is the classification of exit
# codes and not a restatement of it.
probe_case() {
    # probe_case <description> <curl-exit> <expected reachable|unreachable>
    local desc="$1" curl_rc="$2" expect="$3" got
    if (
        curl() { return "$curl_rc"; }
        runner_can_reach_github
    ); then got=reachable; else got=unreachable; fi
    if [[ "$got" == "$expect" ]]; then
        echo "ok   - probe: $desc"
    else
        echo "FAIL - probe: $desc (got '$got', expected '$expect')" >&2
        if [[ "$expect" == reachable ]]; then
            echo "       Reading this as 'the network is down' hands the quiet path to a" >&2
            echo "       probe malfunction, which is the direction that hides things." >&2
        fi
        FAILURES=$((FAILURES + 1))
    fi
}
probe_case "curl succeeds"                        0   reachable
probe_case "could not resolve host (6)"           6   unreachable
probe_case "failed to connect (7)"                7   unreachable
probe_case "operation timed out (28)"             28  unreachable
probe_case "SSL connect error (35)"               35  unreachable
probe_case "HTTP error, e.g. 403 rate limit (22)" 22  reachable
probe_case "unsupported protocol / bad URL (1)"   1   reachable
probe_case "curl not installed (127)"             127 reachable

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

# --- Gate B END TO END, driven per attempt ----------------------------------
# Everything above tests a PIECE. This drives the real `cmd_selfupdate` over a
# scripted sequence of attempts and asserts BOTH the returned exit code and the
# attempt count, which is the only way several of these properties can be
# observed at all.
#
# WHY, CONCRETELY. Three mutations that break the classifier in the QUIET
# direction were confirmed to leave the whole suite green before this existed:
#
#   1. `[ "$rc" -eq 2 ] && node_could_not_reach_github …` -> `||`, so every
#      indeterminate becomes environmental. The source pin survives because the
#      CALL TEXT is still there -- the same defect the round-2
#      `prev_emits_latest_seen` fix was written to close, reappearing one
#      function over. A pin that greps for a call cannot see the operator
#      joining it to anything.
#   2. `-eq 1` -> `-ne 1` on the corroboration, swapping 75 and 1.
#   3. deleting the `attempt_cause=""` reset.
#
# And the run-level latch -- an unexplained indeterminate on ANY attempt must
# never be overwritten by a later environmental one -- is a property OF THE
# SEQUENCE. No single-attempt test can express it.
#
# `run_node_until_check` is stubbed rather than booting a node: the sequence is
# the subject, and a real boot costs seconds per attempt. The lifecycle test
# covers the same retry property by actually running `run_node_until_check`
# against a FAKE NODE BINARY -- a real process, real ports, real log files, but
# not a real node. So the two differ in how much of the harness executes, not in
# whether the subject is genuine; neither is a live cross-check against a real
# release. (An earlier version of this claimed "against REAL boots", which
# overstated the lifecycle test in the direction of "you need not check this".)
# `curl`/`tar` are shadowed so the
# download preamble is a no-op. Same shape as
# `release_wait_for_binaries_test.sh`'s `check_call_count`.
#
# The stub and its two globals are SHARED with the Gate A driver further down --
# the gates differ in which command they drive, not in how a scripted sequence of
# node boots is faked. Hence the neutral names.
CANARY_STUB_ATTEMPT=0
CANARY_STUB_SCRIPT=()
run_node_until_check_stub() {
    local work="$2" spec exit_code logvar rest outvar
    CANARY_STUB_ATTEMPT=$((CANARY_STUB_ATTEMPT + 1))
    # Past the end of the script means the loop ran more times than the case
    # expects; repeat the last entry so the attempt-COUNT assertion is what
    # reports it, with a number, rather than an unbound-variable abort.
    if [[ "$CANARY_STUB_ATTEMPT" -le "${#CANARY_STUB_SCRIPT[@]}" ]]; then
        spec="${CANARY_STUB_SCRIPT[$((CANARY_STUB_ATTEMPT - 1))]}"
    else
        spec="${CANARY_STUB_SCRIPT[$((${#CANARY_STUB_SCRIPT[@]} - 1))]}"
    fi
    # "<node-exit>:<log-fixture>" or "<node-exit>:<log-fixture>:<node.out-fixture>".
    # The third field exists because `node.out` is a SEPARATE observation surface
    # from the log dir: the node's fatal-abort CRITICAL lines are `eprintln!`, so
    # they land there and nowhere else, and the exit-42 classification reads them.
    exit_code="${spec%%:*}"
    rest="${spec#*:}"
    logvar="${rest%%:*}"
    outvar="${rest#*:}"
    # No colon in `rest` leaves it unchanged by the strip, which is how a
    # two-field spec is told from a three-field one.
    [[ "$outvar" == "$rest" ]] && outvar=""
    NODE_EXIT="$exit_code"
    mkdir -p "$work/logs"
    if [[ -n "$logvar" ]]; then
        printf '%s\n' "${!logvar}" > "$work/logs/freenet.2026-08-08-02.log"
    fi
    if [[ -n "$outvar" ]]; then
        printf '%s\n' "${!outvar}" > "$work/node.out"
    fi
}

real_run_node_until_check="$(declare -f run_node_until_check)"
if [[ -z "$real_run_node_until_check" ]]; then
    echo "FAIL - run_node_until_check is not defined; the Gate B driver would stub" >&2
    echo "       nothing and every case below would pass vacuously." >&2
    FAILURES=$((FAILURES + 1))
fi

# THE MESSAGE IS ASSERTED, not just the code. Exit 75 is reached from two
# branches whose operator-facing text differs, and the quiet Matrix message
# deliberately points the reader at the job log line as the ONLY thing that says
# which -- so that line is the disambiguator and it was untested. Measured:
# deleting the entire `ports)` arm of the message `case` left all four suites
# green. `check()` in this same file exists for exactly this reason and says so
# in its own comment; this is that gap reintroduced one function over.
gate_b_case() {
    # gate_b_case <desc> <expected-rc> <expected-attempts> <runner-reachable> \
    #             <expected-message-substring> <spec...>
    # Each <spec> is "<node-exit>:<fixture-variable-name>" for one attempt.
    local desc="$1" want_rc="$2" want_attempts="$3" reachable="$4" want_msg="$5"
    shift 5
    CANARY_STUB_SCRIPT=("$@")
    CANARY_STUB_ATTEMPT=0
    local got_rc got_attempts out err errfile
    errfile="$(mktemp "$TMPROOT/gateb.XXXXXX")"
    out="$(
        CANARY_ATTEMPTS="${#CANARY_STUB_SCRIPT[@]}"
        CANARY_RETRY_SLEEP=0
        curl() { :; }
        tar()  { :; }
        run_node_until_check() { run_node_until_check_stub "$@"; }
        if [[ "$reachable" == yes ]]; then
            runner_can_reach_github() { return 0; }
        else
            runner_can_reach_github() { return 1; }
        fi
        mkdir -p "$CANARY_WORKDIR/selfupdate/bin"
        printf '#!/bin/sh\necho "Freenet version: 0.2.121"\n' \
            > "$CANARY_WORKDIR/selfupdate/bin/freenet"
        chmod +x "$CANARY_WORKDIR/selfupdate/bin/freenet"
        cmd_selfupdate 0.2.121 0.2.122 2>"$errfile" >/dev/null
        printf '%s %s' "$?" "$CANARY_STUB_ATTEMPT"
    )"
    got_rc="${out%% *}"
    got_attempts="${out##* }"
    err="$(cat "$errfile")"
    rm -f "$errfile"
    if [[ "$got_rc" != "$want_rc" || "$got_attempts" != "$want_attempts" ]]; then
        echo "FAIL - Gate B: $desc" >&2
        echo "         got exit $got_rc after $got_attempts attempt(s);" >&2
        echo "         wanted exit $want_rc after $want_attempts" >&2
        FAILURES=$((FAILURES + 1))
    elif [[ "$err" != *"$want_msg"* ]]; then
        echo "FAIL - Gate B: $desc (exit $got_rc correct, but the operator sees the wrong text)" >&2
        echo "         wanted a message containing: $want_msg" >&2
        echo "         got: ${err:-<nothing on stderr>}" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   - Gate B: $desc"
    fi
}

# The load-bearing one, and the reason the retry is gated on rc=2. Each attempt
# wipes the tree and boots a fresh node, so retrying a REAL failure lets any
# intermittent fault produce one passing attempt, and Gate B then reports a
# broken release healthy. A flaky pass on the post-publish gate is worse than
# no gate.
gate_b_case "a real #5221 failure is never retried" 1 1 no \
    "could not parse the version GitHub returned" "0:BROKEN" "0:BROKEN"
gate_b_case "an indeterminate IS retried, then exits 75" 75 2 no \
    "could not reach GitHub on 2 of 2 attempt(s), and this runner cannot reach it either" \
    "0:FETCH_FAIL" "0:FETCH_FAIL"
gate_b_case "a port collision exits 75 without a network probe" 75 2 yes \
    "every attempt hit a port collision on this host" "43:" "43:"
# The corroboration: the same node-side symptom, opposite runner state.
# The COUNT is asserted, not just the phrase. The message used to say "on all
# $CANARY_ATTEMPTS attempts" unconditionally, which sticky-by-strength made
# false the moment one attempt was a port collision -- and the original
# assertion picked a substring that did not span the number, so it passed.
gate_b_case "node cannot reach GitHub but the runner can -> loud, not 75" 1 2 yes \
    "could not reach GitHub on 2 of 2 attempt(s), but THIS RUNNER reached" \
    "0:FETCH_FAIL" "0:FETCH_FAIL"
# A hung updater must never be quiet.
gate_b_case "no outcome logged on every attempt -> loud" 1 2 no \
    "at least one attempt started the update check and never logged an outcome" \
    "0:PENDING" "0:PENDING"
# THE LATCH, top tier. An unexplained indeterminate on attempt 1 must survive an
# environmental attempt 2. Last-writer-wins here returned 75 and told the dev
# room a hung updater was "not a stranded fleet and needs no fleet action".
gate_b_case "unexplained THEN environmental stays loud (the latch)" 1 2 no \
    "at least one attempt started the update check and never logged an outcome" \
    "0:PENDING" "43:"
# ...and in the other order, so the latch is not merely "the first attempt wins".
gate_b_case "environmental THEN unexplained stays loud (the latch)" 1 2 no \
    "at least one attempt started the update check and never logged an outcome" \
    "43:" "0:PENDING"

# STICKY BY STRENGTH, one tier down: `github` must never be downgraded to
# `ports`. Both orderings, because they disagreed. The second was the live bug:
# the probe never ran, the run exited 75 quiet, and the message claimed every
# attempt hit a port collision -- which the message assertion is what catches,
# since the exit code alone is 75 in the correct `ports`-only case too.
gate_b_case "ports THEN github runs the probe and goes loud" 1 2 yes \
    "could not reach GitHub on 1 of 2 attempt(s), but THIS RUNNER reached" \
    "43:" "0:FETCH_FAIL"
gate_b_case "github THEN ports still runs the probe and goes loud" 1 2 yes \
    "could not reach GitHub on 1 of 2 attempt(s), but THIS RUNNER reached" \
    "0:FETCH_FAIL" "43:"

# The QUIET github branch, which the three cases above never reach: they all
# have the runner UP, so they land on the loud sibling. With the runner DOWN a
# mixed run is environmental, and that branch printed "in $CANARY_ATTEMPTS
# attempts" for a further commit after its sibling was fixed -- the same false
# unanimity, in the branch nobody had a case for. Found because a reviewer's
# mutation reported PATTERN NOT FOUND, which is why an unapplied mutation is
# never a pass.
gate_b_case "ports THEN github with the runner DOWN is quiet, and counts honestly" 75 2 no \
    "could not reach GitHub on 1 of 2 attempt(s), and this runner cannot reach it either" \
    "43:" "0:FETCH_FAIL"
gate_b_case "github THEN ports with the runner DOWN is quiet, and counts honestly" 75 2 no \
    "could not reach GitHub on 1 of 2 attempt(s), and this runner cannot reach it either" \
    "0:FETCH_FAIL" "43:"

# --- Gate B's own decision assertion, past the log check --------------------
# Gate B does NOT need Gate A's decision gate: its direction is fixed (the
# release is published by the time it runs, so the previous binary must always
# decide to update) and it already asserts that, plus exit 42. These two cases
# reach that assertion for the first time -- every case above fails earlier --
# and pin the ONE thing that was missing: which of the two causes it names.
#
# Both are a real failure and both are red. The distinction is the diagnosis,
# and it is the same reason `check()` at the top of this file asserts messages:
# "the node stayed put" sends the reader after the comparator, while the #4073
# refusal is a wrongly-matching local gate in rollback.rs. Sending an on-call
# reader at the wrong subsystem on a post-publish alarm is the cost.
#
# A spare attempt each, for the reason documented on `gate_a_case` below: these
# are the only Gate B cases whose verdict comes from a loop that exited on rc=0,
# so with one spec nothing would notice if that break stopped working. Asserting
# one consumed attempt out of two available pins it.
gate_b_case "the previous release simply stayed put -> loud, named as such" 1 1 yes \
    "did NOT decide to update to v0.2.122" "0:SEEN_OK" "0:SEEN_OK"
gate_b_case "the previous release refused via the #4073 local gate -> named as THAT" 1 1 yes \
    "REFUSED it as locally blocked (#4073" "0:SEEN_REFUSED_4073" "0:SEEN_REFUSED_4073"

# --- Gate A END TO END, driven per attempt ----------------------------------
# The Gate A counterpart of the block above, sharing its stub. Two properties
# cannot be observed anywhere else:
#
#   1. THE EXIT-CODE ASSERTION. `NODE_EXIT` is caller state, so it is asserted in
#      `cmd_preflight` rather than in the pure function, and no fixture-driven
#      test of `assert_gate_a_decision` can reach it. It is a SECOND, independent
#      observer of the same decision: the log check reads a phrase this repo
#      chooses, while exit 42 is the supervisor contract itself, so a trigger site
#      worded past `MARKER_TRIGGERED_RE` is invisible to one and caught by the
#      other.
#   2. THAT THE CONDITIONAL IS WIRED AT ALL. The pure function is told which arm
#      to take; only cmd_preflight decides it, from the shipping binary's
#      `--version` and the resolved latest tag. A gate that always demanded a
#      DECLINE would block a re-run of an older release's workflow -- healthy
#      behaviour, blocked -- and nothing below the wiring can see that.
#
# The shipping version is given as a whole `--version` LINE so the unreadable
# case is driven through the same field split the real gate uses, rather than
# around it.
# --- node.out fixtures: the OTHER two producers of exit 42 -------------------
#
# 42 is overloaded. `FATAL_LISTENER_EXIT_CODE` (crates/core/src/node/p2p_impl.rs)
# is the same number, both of its producers are enabled unconditionally in the
# real binary (`enable_abort_on_fatal_listener_exit` / `enable_abort_on_redb_poison`
# at the top of freenet.rs's node path), and the distinct code 45 is opted into
# only via SYSTEMD_FAST_CRASH_ENV_VAR, which the canary does not export -- so 42
# is used at every uptime, including the ~40s a canary run lasts.
#
# These are `eprintln!`, so they appear in node.out and NOT in the log dir, which
# is why the predicate that reads them takes a workdir.
# shellcheck disable=SC2034
FATAL_LISTENER_OUT='2026-08-08T02:00:05Z  INFO freenet: node running
CRITICAL: Network event listener exited (fatal): transport error: connection reset by peer'
# shellcheck disable=SC2034
REDB_POISON_OUT='CRITICAL: contract storage (redb) is poisoned by an I/O error and cannot recover in-process: Io(Custom { kind: Other, error: "input/output error" }). Exiting with code 42 so the service manager restarts the node with a fresh database handle (#4604).'
# The ordinary shape: a node that was stopped by the canary says nothing special.
# shellcheck disable=SC2034
CLEAN_OUT='2026-08-08T02:00:05Z  INFO freenet: node running
2026-08-08T02:00:09Z  INFO freenet: received SIGTERM, shutting down'

# Read BY NAME through the stub's `${!logvar}`, so shellcheck cannot see the use.
# shellcheck disable=SC2034
SEEN_TRIGGERED_OLD='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.122" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.123
2026-08-08T02:00:00.412000Z  INFO freenet: Startup check: newer version on GitHub, triggering auto-update new_version=0.2.123'

# A CASE THAT ASSERTS A HARD BLOCK MUST SUPPLY MORE SPECS THAN IT EXPECTS TO
# CONSUME. This is a rule about the fixture, not about style, and getting it
# wrong silently disarms the case:
#
# `CANARY_ATTEMPTS` is set below to the NUMBER OF SPECS. With exactly one spec it
# is 1, and then rc=1 (a real defect, returns immediately) and rc=2 (an
# environmental verdict, retried until the budget runs out and the tail `fail`
# returns 1) produce the SAME exit code after the SAME one attempt. Every
# discriminator between "block" and "retry" becomes invisible to the outcome, and
# only the message text is left to notice it.
#
# That is not a hypothetical: mutation R-a (`node_exited_on_fatal_abort` stuck
# TRUE, the escape-hatch direction that launders a real downgrade bug into
# "environmental") was caught by NO outcome assertion in this file, because every
# hard-block case here supplied one spec. The REAL gate defaults to
# `CANARY_ATTEMPTS=2` (auto-update-canary.sh), so with a retry available the same
# mutation returns 0 and the release publishes. The fixture could not produce the
# fault because its environment differed from production in exactly the dimension
# the fault needed -- the same shape as the #5271 fixture that wrapped a real log
# string in a type the system cannot emit.
#
# So: give a hard-block case a spare attempt it must NOT reach, and assert the
# attempt COUNT. `gate_b_case "a real #5221 failure is never retried"` above is
# the existing precedent, and it had this right all along.
#
# rc=0 cases are exempt: rc=0 leaves the loop immediately, so any mutation that
# turns one into rc=1 or rc=2 changes the exit code at one attempt.
gate_a_case() {
    # gate_a_case <desc> <version-line> <expected-latest> <want-rc> \
    #             <want-attempts> <want-message-substring> <spec...>
    # Each <spec> is "<node-exit>:<fixture-variable-name>" for one attempt.
    local desc="$1" version_line="$2" latest="$3" want_rc="$4" want_attempts="$5" want_msg="$6"
    shift 6
    CANARY_STUB_SCRIPT=("$@")
    CANARY_STUB_ATTEMPT=0
    local got_rc got_attempts out output outfile bindir
    outfile="$(mktemp "$TMPROOT/gatea.XXXXXX")"
    bindir="$(mktemp -d "$TMPROOT/gateabin.XXXXXX")"
    # BASH, not `/bin/sh`: `%q` renders a string containing a newline as bash's
    # ANSI-C `$'...'` form, which dash does not understand -- it would emit the
    # quoting syntax literally and the multi-line case below would fail for a
    # reason that has nothing to do with the gate.
    printf '#!/usr/bin/env bash\nprintf "%%s\\n" %q\n' "$version_line" > "$bindir/freenet"
    chmod +x "$bindir/freenet"
    out="$(
        CANARY_ATTEMPTS="${#CANARY_STUB_SCRIPT[@]}"
        CANARY_RETRY_SLEEP=0
        # Pinned so cmd_preflight does not reach GitHub from a test. It cannot
        # DISARM anything: an empty value is treated as unset and sends the gate
        # to `resolve_expected_latest`, so every case here supplies a real one.
        export CANARY_EXPECTED_LATEST="$latest"
        run_node_until_check() { run_node_until_check_stub "$@"; }
        cmd_preflight "$bindir/freenet" >"$outfile" 2>&1
        printf '%s %s' "$?" "$CANARY_STUB_ATTEMPT"
    )"
    got_rc="${out%% *}"
    got_attempts="${out##* }"
    output="$(cat "$outfile")"
    rm -f "$outfile"
    if [[ "$got_rc" != "$want_rc" || "$got_attempts" != "$want_attempts" ]]; then
        echo "FAIL - Gate A: $desc" >&2
        echo "         got exit $got_rc after $got_attempts attempt(s);" >&2
        echo "         wanted exit $want_rc after $want_attempts" >&2
        echo "         output: ${output:-<nothing>}" >&2
        FAILURES=$((FAILURES + 1))
    elif [[ "$want_msg" == '!'* && "$output" == *"${want_msg#!}"* ]]; then
        # A `!`-prefixed needle asserts ABSENCE. Needed because `fail` writes to
        # stderr and does not replace what an earlier `fail` already wrote, so a
        # branch that should NOT have run is invisible to a presence check --
        # both messages simply appear. Measured: dropping the `rc -ne 1` guard on
        # the exit-42 observer lets it overwrite a parse-failure verdict, and the
        # positive form of this case stayed GREEN because the parse-failure text
        # was still in the output alongside it.
        echo "FAIL - Gate A: $desc (exit $got_rc correct, but a message that should NOT appear did)" >&2
        echo "         must NOT contain: ${want_msg#!}" >&2
        echo "         got: ${output:-<nothing>}" >&2
        FAILURES=$((FAILURES + 1))
    elif [[ "$want_msg" != '!'* && "$output" != *"$want_msg"* ]]; then
        echo "FAIL - Gate A: $desc (exit $got_rc correct, but the operator sees the wrong text)" >&2
        echo "         wanted a message containing: $want_msg" >&2
        echo "         got: ${output:-<nothing>}" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   - Gate A: $desc"
    fi
}

# THE GREEN SIDE, and for a BLOCKING gate it carries as much weight as the red
# ones: a version of this check that could only fail would stall the first
# release that ran it, and the first person to hit that learns to override the
# gate. The message assertion pins that the arm taken is stated in the log --
# a reader of a green job must be able to tell WHICH assertion ran.
gate_a_case "healthy release: newer than latest, declines -> pass" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 0 1 \
    "MUST decline to update" "0:SEEN_OK"

# The version line is found BY ITS MARKER, not by position. `--version` already
# prints a second line (`Build timestamp: ...`), so anything that printed a line
# BEFORE the version -- a deprecation banner, a build warning -- would make a
# positional read return an unparseable field, send the gate down the `unknown`
# arm, and skip both new assertions while the job stayed green. The format pin
# further down cannot see that: the format is untouched, only its line moved.
#
# The `\n` in the version-line argument is what makes this a real multi-line
# `--version`; `gate_a_case` writes it through `printf`, so the fake binary emits
# two lines exactly as the node does.
gate_a_case "a banner line before the version does not disarm the gate" \
    "NOTE: this build is unsupported
Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 0 1 \
    "MUST decline to update" "0:SEEN_OK"

# The hole this closes. Everything `assert_detection_healthy` reads is exactly
# what a healthy run produces; the node simply reached the wrong answer.
#
# Two specs, one expected attempt, per the rule on `gate_a_case`: the spare
# attempt is what makes "blocked" distinguishable from "retried into the same
# exit code".
gate_a_case "inverted comparator: decides to update with nothing newer -> BLOCK" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "DECIDED TO UPDATE" "0:SEEN_TRIGGERED" "0:SEEN_OK"

# The exit-code observer on its own. The log here is a HEALTHY decline -- no
# trigger line at all -- so every log-based check passes and only NODE_EXIT
# speaks. This is the case a log-only assertion cannot reach. No `node.out` at
# all, so it also covers the fail-closed reading of a missing one.
gate_a_case "exit 42 with a clean log still blocks (the second observer)" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "exited 42" "42:SEEN_OK" "0:SEEN_OK"

# A real defect is deterministic, so it must not be retried -- which is what the
# spare attempt asserts, and what a one-spec fixture could not.
gate_a_case "the #4073 refusal blocks, and names rollback.rs" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "rollback.rs" "0:SEEN_REFUSED_4073" "0:SEEN_OK"

# THE OTHER ARM. `cross-compile.yml` is checked out AT THE TAG, so re-running an
# older release's workflow after newer releases have published puts a genuinely
# older binary under Gate A. Deciding to update is then CORRECT, and exiting 42
# is correct with it -- both of these would be blocked by an unconditional
# "must have declined" assertion, on a release with nothing wrong.
gate_a_case "older binary than latest: decides to update -> pass" \
    "Freenet version: 0.2.122 (deadbeefcafe)" 0.2.123 0 1 \
    "MUST decide to update" "0:SEEN_TRIGGERED_OLD"
gate_a_case "older binary than latest: exit 42 is correct there, not a block" \
    "Freenet version: 0.2.122 (deadbeefcafe)" 0.2.123 0 1 \
    "MUST decide to update" "42:SEEN_TRIGGERED_OLD"

# An unreadable version skips ONE arm and says so, rather than guessing a
# direction and blocking a release on the guess. The exit-42 assertion is scoped
# to the `decline` arm, so it must not fire here either.
#
# `::warning::` and not a plain line: this arm disables BOTH new assertions, so
# reaching it silently returns Gate A to its pre-decision-check strength while the
# job stays green. A release log runs to thousands of lines; an annotation is seen
# without reading it. (The `--version` format pin further down is the other half
# -- it makes a change to that output shape fail LOUDLY here rather than widen
# this arm quietly.)
gate_a_case "unreadable shipping version: skips the decision arm LOUDLY" \
    "Freenet" 0.2.122 0 1 \
    "::warning::" "42:SEEN_OK"
gate_a_case "unreadable shipping version: the warning says WHAT was skipped" \
    "Freenet" 0.2.122 0 1 \
    "did NOT assert which decision" "42:SEEN_OK"
# ...and cmd_preflight's OWN annotation, anchored on the PREFIX AND the
# caller-unique text TOGETHER.
#
# Both needles above are produced by `assert_gate_a_decision`, so they were
# satisfied by the pure function and said nothing about the caller: reverting
# cmd_preflight's `warn` to a plain `note` left the whole suite GREEN, measured.
# That is the borrowed anchor this file's own comment (on the unknown-arm
# decision case) describes, running in the opposite direction -- the pure
# function was pinned against the caller and the caller against nothing.
#
# THE FIRST ATTEMPT AT THIS CASE ALSO FAILED TO CATCH IT, and the reason is worth
# keeping: it asserted only the caller-unique TEXT, which `note` prints just as
# `warn` does. Neither half is sufficient alone -- `::warning::` alone is
# borrowable from the pure function, and the text alone survives the downgrade.
# The needle must therefore span the boundary between them, which is only
# possible because the annotation prefix is immediately followed by the message.
gate_a_case "unreadable shipping version: cmd_preflight raises its OWN annotation" \
    "Freenet" 0.2.122 0 1 \
    "::warning::could not compare the shipping version" "42:SEEN_OK"

# --- exit 42 is OVERLOADED, and must not false-block a healthy release -------
#
# `FATAL_LISTENER_EXIT_CODE` is also 42 (crates/core/src/node/p2p_impl.rs). Both of
# its producers run in the real binary, and the distinct code 45 is opted into only
# via SYSTEMD_FAST_CRASH_ENV_VAR, which the canary does not set -- so a HEALTHY
# binary that declines the update and then loses its network event listener (a
# CI-runner transport error) or poisons redb (disk EIO) exits 42 for a reason that
# has nothing to do with the updater.
#
# THE OVERLAP IS EXACTLY COINCIDENT WITH THE CHECK'S VALUE, which is why this is a
# fix and not a note: when the comparator IS inverted the trigger line is in the
# log and the decision check already blocks, so exit 42 adds nothing there. Exit 42
# is load-bearing ONLY when there is no trigger line -- and "42 with no trigger
# line" is precisely the fatal-abort signature. So the code alone cannot carry it;
# the node's own CRITICAL line is what separates them.
#
# ENVIRONMENTAL means rc=2, which is RETRIED. With one attempt that ends as a
# blocked release (unverified is not verified) but with the right diagnosis.
#
# These two DELIBERATELY supply one spec -- they are asserting the exhaustion
# path and its tail message, so the budget must run out. They therefore cannot
# discriminate rc=2 from rc=1 by exit code; the retry case below is what does
# that, and the hard-block cases after it are what stop rc=2 from being handed
# to a real defect.
gate_a_case "exit 42 after a fatal listener abort is environmental, not an updater fault" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "CRITICAL fatal abort" "42:SEEN_OK:FATAL_LISTENER_OUT"
# ...and the redb path separately, because it was added later (#4604) and reuses
# the listener path's exit-code decision. A version of this that knew only about
# the listener would hard-block a healthy release on a disk error.
gate_a_case "exit 42 after a redb poison is environmental too" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "CRITICAL fatal abort" "42:SEEN_OK:REDB_POISON_OUT"
# THE ONE THAT MATTERS: retried, and a healthy release is NOT blocked by one bad
# attempt. This is the whole point of classifying it as 2 rather than 1 -- and it
# is the case a hard block would have failed.
gate_a_case "a fatal abort is RETRIED and the healthy retry passes" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 0 2 \
    "MUST decline to update" "42:SEEN_OK:FATAL_LISTENER_OUT" "0:SEEN_OK:CLEAN_OUT"
# THE OTHER DIRECTION, AND THE STRONGEST ATTACK ON THIS WHOLE PATH. 42 with node
# output carrying NO fatal-abort line is a real downgrade bug and must block on
# attempt 1, with the retry budget untouched.
#
# The spare attempt is the entire assertion. The rc=2 path introduced above is an
# escape hatch by construction: anything that makes the discriminator answer
# "environmental" for a genuine defect converts a blocked release into a
# published one. With one spec that conversion is INVISIBLE -- rc=1 and rc=2 both
# exit 1 after one attempt -- and mutation R-a (the discriminator stuck TRUE) was
# caught here by message text alone. With the spare attempt R-a yields
# `exit 0 after 2 attempts`: the release publishes. The real gate runs
# CANARY_ATTEMPTS=2, so 2 is also the production shape.
gate_a_case "a real exit-42 downgrade blocks on attempt 1 even when a retry is available" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "downgrade-and-restart loop" "42:SEEN_OK:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"
# ...and it names the alternative cause rather than confidently blaming the
# comparator. A blocking gate that points at the wrong subsystem is how people
# learn to override it.
gate_a_case "the hard block names the fatal-abort alternative it ruled out" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "42 is also FATAL_LISTENER_EXIT_CODE" "42:SEEN_OK:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"

# --- the exit-42 observer must survive an INDETERMINATE log verdict ----------
#
# Found by an external (Codex) review pass, reproduced twice, and it made the
# observer unreachable on the one input it exists for.
#
# `freenet.rs` `return`s immediately after `update_tx.send`, so the completion
# line is NEVER emitted on the trigger path. If the trigger's wording drifts out
# of `MARKER_TRIGGERED_RE`, or the line is dropped, the log then has no matched
# trigger AND no completion -- `node_check_settled` is false and
# `assert_detection_healthy` returns 2 (INDETERMINATE). The observer was gated on
# `rc -eq 0`, so it skipped; Gate A burned both attempts and reported "produced no
# verdict -- GitHub unreachable, or the check never logged an outcome" for a
# deterministic self-downgrade. Fail-closed, so no false green -- but the
# diagnosis named the wrong subsystem, and the file CLAIMED this case was covered.
#
# The gate is now `rc -ne 1`, so an INDETERMINATE log verdict no longer disarms
# it while a definite fault (rc=1, which localises better) still wins.
# shellcheck disable=SC2034  # read BY NAME through the stub's `${!logvar}`
SEEN_TRIGGER_REWORDED='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.122
2026-08-08T02:00:00.412000Z  INFO freenet: Startup check: newer release found, starting auto-update new_version=0.2.122'
# shellcheck disable=SC2034  # read BY NAME through the stub's `${!logvar}`
SEEN_TRIGGER_DROPPED='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.123" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.122'

# Both must BLOCK, on attempt 1, with the self-downgrade diagnosis -- not with
# the "no verdict" message, and not by consuming the retry budget.
gate_a_case "reworded trigger + exit 42 blocks despite an INDETERMINATE log" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "downgrade-and-restart loop" "42:SEEN_TRIGGER_REWORDED:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"
gate_a_case "dropped trigger line + exit 42 blocks despite an INDETERMINATE log" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "downgrade-and-restart loop" "42:SEEN_TRIGGER_DROPPED:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"
# ...and the operator is told why there is no trigger line to find, so the
# missing line does not read as evidence against the verdict.
gate_a_case "the block explains the absent trigger line" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "no matched trigger, and no completion line either" \
    "42:SEEN_TRIGGER_REWORDED:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"
# THE UPDATE ARM'S BEHAVIOUR ON THE SAME DEFECT, pinned because the comment
# describing it in auto-update-canary.sh was WRONG twice and a reader has no way
# to check it without this.
#
# The intuitive answer -- "the update arm catches it as 'did NOT decide to
# update'" -- is false. Every trigger site `return`s straight after
# `update_tx.send`, so a node that triggered emits no completion line either;
# `rc` is 2 and `assert_gate_a_decision` is never reached (it is gated on rc=0).
# The run burns BOTH attempts and blocks as UNVERIFIED, with the same
# wrong-subsystem "produced no verdict" text this observer removes from the
# decline arm -- on a node that behaved correctly. Fail-closed, and not fixed
# here: the exit-42 observer cannot be extended to this arm, because exiting 42
# is CORRECT when the binary really is older.
#
# Two specs and two expected attempts, because burning the retry budget is the
# behaviour being asserted.
# shellcheck disable=SC2034  # read BY NAME through the stub's `${!logvar}`
SEEN_TRIGGER_REWORDED_OLD='2026-08-08T02:00:00.000000Z  INFO freenet: Startup update check against GitHub current="0.2.122" jitter_secs=7
2026-08-08T02:00:00.300000Z  INFO freenet::commands::auto_update: Startup update check: GitHub reports latest release latest=0.2.123
2026-08-08T02:00:00.412000Z  INFO freenet: Startup check: newer release found, starting auto-update new_version=0.2.123'
gate_a_case "update arm + reworded trigger blocks as UNVERIFIED, not as a decision failure" \
    "Freenet version: 0.2.122 (deadbeefcafe)" 0.2.123 1 2 \
    "produced no verdict" "42:SEEN_TRIGGER_REWORDED_OLD:CLEAN_OUT" "42:SEEN_TRIGGER_REWORDED_OLD:CLEAN_OUT"
gate_a_case "...and NOT with the decision-failure diagnosis" \
    "Freenet version: 0.2.122 (deadbeefcafe)" 0.2.123 1 2 \
    '!did NOT decide to update' "42:SEEN_TRIGGER_REWORDED_OLD:CLEAN_OUT" "42:SEEN_TRIGGER_REWORDED_OLD:CLEAN_OUT"

# The corroboration still governs this path: same INDETERMINATE log, but a
# fatal-abort CRITICAL present means the 42 is explained and must stay
# environmental rather than becoming a hard block.
gate_a_case "an INDETERMINATE log + exit 42 + fatal abort stays environmental" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "CRITICAL fatal abort" "42:SEEN_TRIGGER_DROPPED:FATAL_LISTENER_OUT"
# And rc=1 must still win: a parse failure localises better than "it exited 42",
# so the observer must not run on top of it. TWO cases, and the second is the
# load-bearing one -- `fail` appends to stderr rather than replacing, so the
# positive check below passes even when the observer DID overwrite the verdict
# (both messages are present). Only the absence assertion can see it; measured,
# the positive form alone left the guard-removal mutation GREEN.
gate_a_case "a parse failure still outranks the exit-42 observer" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    "could not parse the version GitHub returned" "42:BROKEN:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"
gate_a_case "...and the exit-42 diagnosis is NOT also emitted over it" \
    "Freenet version: 0.2.123 (deadbeefcafe)" 0.2.122 1 1 \
    '!downgrade-and-restart loop' "42:BROKEN:CLEAN_OUT" "0:SEEN_OK:CLEAN_OUT"

# The pure predicate underneath all of that.
fatal_out_case() {
    # fatal_out_case <desc> <expect yes|no> <node.out content>
    local desc="$1" expect="$2" content="$3" dir got
    dir="$(mktemp -d "$TMPROOT/fatal.XXXXXX")"
    if [[ -n "$content" ]]; then
        printf '%s\n' "$content" > "$dir/node.out"
    fi
    if node_exited_on_fatal_abort "$dir"; then got=yes; else got=no; fi
    if [[ "$got" == "$expect" ]]; then
        echo "ok   - fatal-abort detection: $desc"
    else
        echo "FAIL - fatal-abort detection: $desc (got '$got', expected '$expect')" >&2
        if [[ "$expect" == no ]]; then
            echo "       Reading this as a fatal abort downgrades a REAL downgrade-loop bug to" >&2
            echo "       'environmental' and retries it -- the direction that ships the bug." >&2
        else
            echo "       Not reading this as a fatal abort blocks a healthy release and blames" >&2
            echo "       compare_versions_for_startup, which is fine." >&2
        fi
        FAILURES=$((FAILURES + 1))
    fi
}
fatal_out_case "the network-event-listener CRITICAL"   yes "$FATAL_LISTENER_OUT"
fatal_out_case "the redb-poison CRITICAL"              yes "$REDB_POISON_OUT"
fatal_out_case "an ordinary SIGTERM shutdown is NOT one" no "$CLEAN_OUT"
# A missing node.out must read as "no fatal abort", i.e. the hard block stands.
# The opposite would hand every exit 42 the environmental path the moment the
# harness stopped capturing output.
fatal_out_case "a missing node.out is NOT a fatal abort" no ""

eval "$real_run_node_until_check"

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
elif [[ "$selfupdate_body" != *'saw_unexplained=1'* ]]; then
    echo "FAIL - cmd_selfupdate no longer latches an UNEXPLAINED indeterminate." >&2
    echo "       The per-attempt cause is last-writer-wins without it: an attempt that" >&2
    echo "       started the check and logged no outcome (a hung updater) followed by an" >&2
    echo "       attempt that lost a port race reports exit 75 and the quiet 'no fleet" >&2
    echo "       action' message. The behavioural cases above cover both orderings." >&2
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

# --- WHY THERE ARE NO SOURCE PINS ON THE TWO FATAL-ABORT MARKERS -------------
#
# There were, briefly. `MARKER_FATAL_LISTENER` and `MARKER_REDB_POISONED` decide
# whether an exit 42 is a genuine self-downgrade bug (block the release) or a
# fatal-listener / redb abort (environmental, retry), so a reword that the canary
# does not follow is worth warning about, and two pins were added to do it by
# scraping `p2p_impl.rs` for text inside `println!`/`eprintln!` bodies.
#
# THEY WERE REMOVED, and the reason is not that they were imperfect -- it is that
# a line-oriented approximation of Rust lexing produced BOTH failure directions.
# Five review rounds each closed the shape they were shown (whole-line comments,
# trailing comments, `#[cfg(test)]` modules, blob-vs-line matching, single-line
# string literals) and each round produced a NEW verified defect rather than a
# diminishing one:
#
#   FALSE PASSES (text the binary never prints satisfying the pin): multi-line
#   string literals, plain and raw, defeat string masking entirely, since the
#   masker resets per line; `#[cfg(all(test, ...))]` / `#[cfg(any(test, ...))]`
#   bypass the test-module cut; the span cap admits an unprinted `const` near a
#   macro that does not terminate on `);`; `/* */` blocks are not handled at all.
#
#   FALSE BLOCKS (a CORRECT tree failing the gate), which is what settled it.
#   Two independent routes. One: the `#[cfg(test)]` cut matched the TEXT, so a
#   doc comment merely MENTIONING the attribute truncated the file and produced
#   two blocking failures on a tree whose binary still printed the markers. Two,
#   and this is the one no reordering fixes: `sed 's/\\$//'` strips a
#   continuation backslash WITHOUT JOINING THE LINES, so a marker split across a
#   `\`-continuation goes red -- while Rust joins the literal, the binary prints
#   ONE line, and the canary's `grep -aqF` matches it perfectly. Verified by
#   compiling and running the binary. An earlier version of this file justified
#   per-line matching as "a single source line is a requirement of the runtime
#   grep"; for that form the claim is simply false, and it is recorded here so
#   nobody re-derives the same half-fix.
#
# A blocking gate that a doc comment or a line wrap can turn red trains people to
# override it, and the limits comment warns the next AUTHOR while the cost is
# paid by the release engineer standing in front of a red gate. So the pins went
# rather than the release-blocking risk.
#
# WHAT THIS COSTS: an early warning if someone rewords either CRITICAL line
# without updating the marker here. The canary then hard-blocks on the next
# environmental exit 42 and names the wrong subsystem -- loud and fail-closed,
# but with no pointer. That is the trade, taken deliberately.
#
# THE FIX THAT CLOSES THE CLASS is freenet/freenet-core#5345: lift each marker
# into a `const` in the Rust source, interpolate it at the emission site, and
# assert it from a Rust `#[test]`. The compiler, not a tokenizer, becomes the
# oracle -- no comment, doc comment, string literal, test module or `cfg`
# spelling can fool it -- and the canary's marker and the node's output get a
# single source of truth so they cannot drift. #5345 also carries the fallback
# options if that turns out bigger than expected, and the ranking of the five
# whole-file `pin_marker` scrapes that remain in this file.
#
# DO NOT re-add a scrape-based pin for these two markers without reading #5345.

P2P_SRC="$SCRIPT_DIR/../crates/core/src/node/p2p_impl.rs"

# ...and that 42 really is still the code those paths use. If FATAL_LISTENER_EXIT_CODE
# ever moves off 42 the corroboration above becomes dead weight that quietly
# weakens the exit-42 assertion -- an exit 42 could then only mean "update
# requested", and the environmental branch would be an escape hatch with no
# legitimate input. Pinned so that change forces a decision here.
# Whole-line `//` comments dropped first, so a historical `// was: ... = 42;`
# cannot satisfy it. (A const declaration is not inside a macro, so the
# emitted-text helper above does not apply here.)
#
# Same `/* */` gap as that helper, and stated for the same reason: a
# `/* const FATAL_LISTENER_EXIT_CODE: i32 = 42; */` left beside a changed const
# keeps this green. Symmetric with the disclosed limit above rather than a
# separate defect, and closing it properly means parsing Rust.
p2p_code_only="$(grep -v '^[[:space:]]*//' "$P2P_SRC" | tr -d '[:space:]')"
if [[ "$p2p_code_only" == *"constFATAL_LISTENER_EXIT_CODE:i32=42;"* ]]; then
    echo "ok   - source pin: FATAL_LISTENER_EXIT_CODE is still 42 (so the overload is real)"
else
    echo "FAIL - source pin: FATAL_LISTENER_EXIT_CODE is no longer 42 in p2p_impl.rs." >&2
    echo "       The canary treats an exit 42 with a CRITICAL fatal-abort line as ENVIRONMENTAL" >&2
    echo "       because that code is shared with the update-requested exit. If they are no" >&2
    echo "       longer the same number, that branch has no legitimate input and is now an" >&2
    echo "       escape hatch on a BLOCKING gate -- delete it rather than leaving it." >&2
    FAILURES=$((FAILURES + 1))
fi

# --- the `--version` output format ------------------------------------------
# cmd_preflight reads the shipping version from field 3 of the `Freenet version:`
# line and uses it to choose WHICH decision to assert. A change to that output
# shape does not fail anything on its own: the field split yields something
# unparseable, the direction becomes `unknown`, and BOTH new assertions silently
# stop running while the job stays green. So the format is pinned to the
# `println!` that produces it.
#
# SCOPE, stated accurately rather than sold: this pins the FORMAT, and
# cmd_preflight selects the line by its `Freenet version:` marker rather than by
# position, so a banner line printed ahead of it no longer shifts the field read.
# Between them the class is closed; neither alone closes it, and the `::warning::`
# on the `unknown` arm is the backstop if some third thing gets past both.
# `cmd_selfupdate` reads the same field of the same output, so this protects both.
#
# A WHOLE-FILE scrape, like the five `tracing::` pins above and unlike the
# emitted-macro extraction that was removed (see the note further up). It is
# WEAKER -- a comment quoting the old format satisfies it -- and it is kept
# because its failure direction is safe: adding text to the file can only make a
# containment check PASS, so no comment, doc comment or line wrap can turn this
# red on a correct tree. That is the whole reason the extraction went and this
# stayed. #5345 replaces both.
#
# THE WHITESPACE STRIPPING ALSO BLINDS IT TO A MISSING SEPARATOR INSIDE THE
# LITERAL, which is worth naming because it is the ONE edit that actually
# defeats what this pin protects. Delete the space after the colon --
# `"Freenet version:{} ({}{})"` -- and `tr -d '[:space:]'` normalises the change
# away, so the pin stays green while `awk '{print $3}'` yields `(deadbeefcafe)`,
# `gate_a_expected_decision` answers `unknown`, and BOTH of Gate A's new
# assertions are skipped. Found by an external review pass, on code two earlier
# passes had examined.
#
# NOT FIXED HERE, and the rejected fix is recorded so it is not re-proposed.
# Squeezing instead of deleting (`tr -s '[:space:]' ' '`) does catch it -- and
# reintroduces a false-BLOCK route, which is the trade this file must never
# make. Measured: with the needle reduced to the format string alone, a rustfmt
# reflow and a deeper indent both stay green, but a `\`-continuation splitting
# the literal mid-word goes RED while the binary prints the text correctly. That
# is the exact shape that forced the removal of the other three source pins.
# Preserving "semantic" whitespace is worse still -- it restores the rustfmt
# fragility the stripping exists to prevent.
#
# So this stays a known blind spot, tracked in #5345, which replaces the pin
# with a marker const plus a Rust test that asserts against real `--version`
# OUTPUT rather than scraping source. The degradation is LOUD in the meantime:
# the `unknown` arm it leads to emits a `::warning::` naming both unparseable
# values, which is exactly the backstop that arm was added for.
if [[ "$(sed 's/\\$//' "$SRC" | tr -d '[:space:]')" == *'println!("Freenetversion:{}({}{})",'* ]]; then
    echo "ok   - source pin: --version still prints 'Freenet version: X (sha)' (field 3 is the version)"
else
    echo "FAIL - source pin: freenet.rs no longer prints 'Freenet version: {} ({}{})'." >&2
    echo "       cmd_preflight takes the shipping version from field 3 of that line and uses it" >&2
    echo "       to pick which decision to assert. A different shape makes the direction" >&2
    echo "       'unknown', which skips the decision check AND the exit-42 check -- Gate A" >&2
    echo "       silently returns to its pre-decision-check strength, green. cmd_selfupdate's" >&2
    echo "       final version comparison reads the same field. Update BOTH awk splits, or the" >&2
    echo "       gate is weaker than it looks." >&2
    FAILURES=$((FAILURES + 1))
fi

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
#                                 It is now the highest-priority member of that
#                                 set, and the reason CHANGED under this PR.
#                                 It used to be simply loud: an old binary's
#                                 fetch failure would stop being classified
#                                 INDETERMINATE and fall through to the equality
#                                 check, reporting a missing observed-latest
#                                 line -- wrong diagnosis, but red. Since Gate B
#                                 gained the environmental classification the
#                                 marker is ALSO the input to
#                                 `node_could_not_reach_github`, so a reword
#                                 silently disables that classification and
#                                 sends every network blip back to the loud
#                                 #5221 alarm: it reinstates precisely the false
#                                 fleet alarm that change removed, by editing a
#                                 different line. Still not a silent PASS, which
#                                 is why it is handed to #5309 rather than
#                                 frozen here alongside MARKER_PARSE_FAIL.
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
    # THREE lines, and the completion line is not padding. `freenet.rs` emits
    # `Startup update check complete` on every non-triggering outcome, so a real
    # #5221 log has it, and WITHOUT it this fixture returns 2 (indeterminate)
    # rather than the 0 the failure message below claims a reworded marker
    # produces. The assertion goes red either way, but a fixture that fails for
    # a different reason than its message names is how a pin comes to be
    # trusted for the wrong thing. Measured: two lines -> rc=2, three -> rc=0.
    printf '%s\n%s\n%s\n' \
        "2026-08-08T01:59:35.950835Z  INFO freenet: $MARKER_CHECK_RAN current=\"0.2.121\" jitter_secs=40" \
        "$historical_warn" \
        "2026-08-08T01:59:36.200000Z  INFO freenet: $MARKER_CHECK_COMPLETE: staying on the current version current=\"0.2.121\"" \
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
        echo "       (Deleting this block IS a way out and nothing stops it: ci.yml's" >&2
        echo "       removed-tests guard scans crates/core/**/*.rs only, so a deleted" >&2
        echo "       SHELL assertion is flagged nowhere, and the counter this PR widened" >&2
        echo "       measures additions rather than deletions. Accepted, and said out" >&2
        echo "       loud so nobody reads 'the suite went green' as 'the property holds'.)" >&2
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
# binary while leaving every assertion in this file green.
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
# missed the urgent one ("triggering IMMEDIATE auto-update") for as long as that
# site has existed, because
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
    echo "       that never decided to update, which is how the urgent site went unseen. If it matches" >&2
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
