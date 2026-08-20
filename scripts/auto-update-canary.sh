#!/usr/bin/env bash
#
# Auto-update release canary (#5222).
#
# WHY THIS EXISTS
# ---------------
# #5104 changed the node's release-tag fetch to return the tag VERBATIM
# ("v0.2.121") and normalised it at only one of its two consumers. The node's
# DETECTION path kept the raw tag, `semver::Version::parse("v0.2.121")` failed,
# and every update was dropped with a `warn!`. Auto-update was broken
# fleet-wide for v0.2.120 AND v0.2.121 -- silently, for two releases -- until
# ~1,100 nodes had to be told to run `freenet update` by hand. A broken
# updater cannot ship its own fix.
#
# Nothing caught it. Every existing signal was one-sided: the release built,
# published, installed and ran. The one machine positioned to notice
# (`framework`, the real-NAT pre-release smoke peer) had been running with
# `--disable-auto-update` since a #5040 measurement window nine days earlier.
#
# THE ASSERTION IS TWO-SIDED, AND THAT IS THE WHOLE POINT
# -------------------------------------------------------
# "No `failed to parse latest version` in the log" is NOT evidence that
# parsing works. It is equally consistent with the check never running at all
# -- which is exactly what `--disable-auto-update`, a dirty build, or a node
# that never reached the update task all produce. A canary that can only go
# green is worth nothing.
#
# So `assert_detection_healthy` requires ALL of:
#   (+) the "Startup update check against GitHub" INFO line is PRESENT
#       -- proves the check actually STARTED
#   (+) a terminal outcome line is PRESENT -- either "Startup update check
#       complete" or a trigger
#       -- proves the check FINISHED. Absence of a parse error is evidence
#          that parsing worked only if the check is known to have got that
#          far; a run stopped mid-request has no parse error either. See
#          `run_node_until_check` for the specific way that used to happen.
#   (-) no "failed to parse latest version" WARN
#       -- proves it parsed what GitHub returned
#   (-) no "Auto-update is DISABLED" WARN
#       -- proves nobody silenced the canary itself (the #5040 drop-in failure
#          mode, now a red build instead of a thing someone has to remember)
#
# TWO GATES
# ---------
#   preflight  -- Gate A, BLOCKING, runs before the release is un-drafted.
#                 Does the binary we are ABOUT to ship parse the tag of the
#                 CURRENT latest release? This is the gate that would have
#                 caught #5104 at v0.2.120, before it reached anyone.
#
#   selfupdate -- Gate B, runs after publication. Does the PREVIOUS release
#                 actually detect this one, exit 42, and self-replace via
#                 `freenet update`? End-to-end proof of the real fleet
#                 transition. It cannot run before publication: the node's
#                 detection path is hardwired to GitHub's `/releases/latest`,
#                 and a draft release does not appear there.
#
# Run either locally. Both keep their FILES to their own temp directory
# (isolated HOME, TMPDIR, webapp cache, and config/data/log dirs), so nothing
# outside it is touched.
#
# TMPDIR is in that list for a reason and must stay there, and the reason is not
# the one it looks like. `client_api.rs` unconditionally `create_dir_all`s
# `std::env::temp_dir()/freenet/webs` (`let contract_web_path =`) when it builds
# the router. That directory is VESTIGIAL: nothing in the tree reads or writes
# it, and web contracts are unpacked elsewhere (`default_webapp_cache_dir`). Its
# one surviving effect is the panic when the mkdir FAILS -- `$TMPDIR/freenet`
# being a FILE, or a directory owned by another user. Through v0.2.124 this file
# did not set TMPDIR, and `cross-compile.yml` stages the binary under test at
# `/tmp/freenet`, which is exactly that path: ENOTDIR, panic (exit 101) before
# the update task spawned, and Gate A blocked a release whose binary was
# perfectly healthy. See `run_node_until_check`.
#
# PORTS are the exception, and an earlier version of this header overstated it
# by calling the runs "safe to run on a machine already running a node". They
# bind real ports, so a run collides with anything already holding them --
# including ANOTHER canary run, which is the likelier case. The node then exits
# 43 (EXIT_CODE_ALREADY_RUNNING, `freenet.rs`). Two mitigations, neither a
# guarantee: the ports are drawn from a random per-run block (below) rather than
# fixed constants, and a 43 is classified as ENVIRONMENTAL so the gate retries on
# a fresh block instead of reporting an auto-update fault. Reproduced before
# that: two `preflight` runs started 2s apart, the second reporting "the startup
# update check never ran" for what was purely a port collision.
#
set -uo pipefail

# --- log markers (must match crates/core/src/bin/) --------------------------
# freenet.rs      -- emitted unconditionally at the top of the startup check
MARKER_CHECK_RAN='Startup update check against GitHub'
# auto_update.rs  -- the #5221 regression signature. Deliberately stops at
# `parse` so it covers BOTH arms of compare_versions_for_startup: the LATEST
# version (the #5221 break) and the CURRENT one. Both return None, both then
# reach the completion line, so a node that failed the current-version parse
# looked identical to a healthy one under the longer marker. The
# `Startup update check: ` prefix is load-bearing, not decoration -- without
# it the string also occurs in a comment inside auto_update.rs's own test
# module, which is enough to satisfy a whole-file source pin.
MARKER_PARSE_FAIL='Startup update check: failed to parse'
# auto_update.rs  -- GitHub unreachable / rate-limited: infrastructure, not a bug
MARKER_FETCH_FAIL='failed to fetch latest version'
# freenet.rs      -- either --disable-auto-update or a dirty build
MARKER_DISABLED='Auto-update is DISABLED'
# freenet.rs -- detection succeeded and an update was requested. There are
# FIVE such sites and one REFUSAL that shares the phrase:
# Cited by PHRASE, never by line number. The six that were here were all low
# by 12 within one release, and this file's own rules already say a line number
# in a note about stale pins rots faster than the thing it describes. The
# phrases are also what the detector actually matches.
#   "Startup check: newer version on GitHub, triggering auto-update"
#   "Urgent update confirmed on GitHub, triggering immediate auto-update"
#   "Update confirmed on GitHub after stagger, triggering auto-update"
#   "Newer version confirmed on GitHub, triggering auto-update"
#   "Periodic re-poll: newer version on GitHub, triggering auto-update"
#   "...repeated install failures); not triggering auto-update (#4073)"  <- the refusal
# Matching the bare substring counts the refusal as a trigger; anchoring on any
# ONE site's full phrase misses the others, reporting "did not decide to
# update" for a node that did. So: match the phrase, subtract the refusal.
#
# A REGEX, not a fixed string, and that is the whole point: the urgent site
# says "triggering IMMEDIATE auto-update", so the fixed substring
# `triggering auto-update` did not match it. A node that took the urgent path
# was reported as never having decided to update. It failed CLOSED (Gate B
# refuses rather than passes), so nothing broke visibly -- which is precisely
# why an enumeration that had been wrong since the urgent path was added went
# unnoticed. `auto-update-canary_test.sh` pins the count of
# `update_tx.send(new_version)` call sites -- a structural anchor this regex
# cannot influence -- and then requires this regex to match all of them. A
# sixth site therefore cannot be added silently whether or not the regex
# happens to match its wording. (Deriving the expected count FROM this regex,
# as the first version of that pin did, made a site the regex missed invisible
# to the count as well: the two errors cancelled.)
MARKER_TRIGGERED_RE='triggering ([a-z]+ )?auto-update'
# Kept for the negative subtraction and for messages: the refusal is a fixed
# string and matching it loosely would swallow real triggers.
# shellcheck disable=SC2034  # read by auto-update-canary_test.sh, which sources
# this file and pins the literal against freenet.rs; the regex above is what the
# runtime detector uses.
MARKER_TRIGGERED='triggering auto-update'
MARKER_NOT_TRIGGERED='not triggering auto-update'
# freenet.rs -- the check ENDED without requesting an update. Emitted on every
# non-triggering outcome (already up to date, GitHub unreachable, unparseable
# tag, #4073 locally-blocked version), so it is a completion signal, not a
# verdict: it says the check got to the end, and the WARN above it, if any,
# says what it found. This was a `debug!` until #5236, which release builds
# compile out entirely (`release_max_level_info`) -- so on a shipped binary the
# most common outcome of the whole check was invisible, and "finished, staying
# put" was indistinguishable from "killed mid-request".
#
# A binary built BEFORE #5236 never emits it. That matters only in Gate B,
# whose subject is the PREVIOUS release: a healthy one triggers an update and
# settles on that, but an old one that neither triggers nor fails now reports
# UNVERIFIED instead of "did NOT decide to update". Still a refusal, and still
# the right one -- just a less specific message, and only until the previous
# release is itself post-#5236.
MARKER_CHECK_COMPLETE='Startup update check complete'
# auto_update.rs -- the OBSERVED latest release, emitted with a `latest=` field
# as soon as the fetch succeeds and before the comparison happens.
#
# Every other marker here is about what the node DECIDED. This one is about what
# it decided ABOUT, and that difference is what lets the gate assert a positive
# fact. Without it the healthy verdict is "no error appeared", which a silently
# WRONG comparator satisfies just as well as a correct one: a `version_from_tag`
# regressed to a constant, or a normaliser that truncated `0.2.121` to `0.2.12`,
# parses, compares, declines to update and logs a clean completion. The log is
# byte-identical to a healthy node's. With this marker the canary can compare
# the value against the tag GitHub actually published (see
# CANARY_EXPECTED_LATEST) and fail on a mismatch.
MARKER_LATEST_SEEN='Startup update check: GitHub reports latest release'
# The first PUBLISHED release whose binary emits the exact text above. Gate B's
# subject is whatever release is currently newest-published, and a binary older
# than this value has no observed-latest line to compare -- arming the equality
# check against it would fail the gate for a line that binary was never built to
# emit. So Gate B arms the check only from this version onwards.
#
# 0.2.125, not 0.2.124: the marker landed in the 0.2.124 tree, but that release
# was never PUBLISHED (Gate A blocked it on the TMPDIR harness bug, #5290) and a
# draft is invisible to `/releases/latest`. So no release a node can reach emits
# this line until 0.2.125, and Gate B must not demand it from 0.2.123.
#
# WHEN THIS CONSTANT SHOULD AND SHOULD NOT MOVE
#
# It is a fact about release history -- which published release first shipped
# this exact text -- so under ordinary releases it does NOT move. Raising it to
# the version being cut skips Gate B's only positive assertion for that release,
# and a constant kept level with the crate version disarms the gate on every
# release while looking maintained. Do not "refresh" it.
#
# But there is one edit where moving it IS correct, and it is easy to miss:
# REWORDING OR REMOVING THE MARKER. Change the text above and the value below
# must move to the first release that will SHIP the new text -- normally the
# NEXT release, since the bump happens inside the release commit. Leave it and
# Gate B demands the new wording from a published binary that emits the old one,
# which surfaces as a POST-PUBLISH red canary and a Matrix alarm whose text
# blames the node ("may not be able to auto-update"). Nothing else in the suite
# notices: the source pin interpolates $MARKER_LATEST_SEEN, so it follows a
# rename by construction. `auto-update-canary_test.sh` therefore freezes the
# marker text and this version TOGETHER, so a reword fails there and forces the
# author to decide about this line in the same edit.
#
# If a Gate B run reports "never logged which release it compared against" for a
# previous release at or above this version, work out which of the two it is:
# the marker was REMOVED from the node (a real finding), or the marker was
# REWORDED and this constant was not moved with it (fix it here).
MARKER_LATEST_SEEN_SINCE='0.2.125'

# p2p_impl.rs -- the two OTHER ways a node exits 42, and the reason Gate A cannot
# read an exit 42 as "it asked to be updated" on its own.
#
# 42 is OVERLOADED. It is `FATAL_LISTENER_EXIT_CODE` (`crates/core/src/node/
# p2p_impl.rs`) as well as the update-requested code, and BOTH producers are
# enabled in the real binary: `enable_abort_on_fatal_listener_exit` and
# `enable_abort_on_redb_poison` are called unconditionally at the top of
# `freenet.rs`'s node path. The distinct `FAST_CRASH_EXIT_CODE` (45) is opted
# into only when `SYSTEMD_FAST_CRASH_ENV_VAR` is set, which the canary does not
# export -- so `fatal_listener_exit_code` returns 42 at EVERY uptime here,
# including the ~40s window a canary run occupies.
#
# So a perfectly healthy binary that declines the update and then loses its
# network event listener (a CI-runner transport error, a monitored task exiting)
# or poisons redb (disk EIO) exits 42 for a reason that has nothing to do with
# the updater. Without these markers the gate would BLOCK that release and send
# the operator to `compare_versions_for_startup`, which is fine.
#
# The overlap is not incidental to the exit-42 assertion, it is exactly
# coincident with it: when the comparator IS inverted the trigger line is in the
# log and `assert_gate_a_decision` already blocks, so exit 42 adds nothing there.
# Exit 42 is load-bearing ONLY when there is no trigger line -- and "42 with no
# trigger line" is precisely the fatal-listener / redb signature. Hence the
# corroboration rather than a bare code test.
#
# Both are `eprintln!`, so they land in `node.out` (the subshell in
# `run_node_until_check` redirects stdout+stderr there) and NOT in the log dir.
# That is why the predicate below takes a WORKDIR and the log predicates take a
# log dir.
MARKER_FATAL_LISTENER='CRITICAL: Network event listener exited (fatal)'
MARKER_REDB_POISONED='CRITICAL: contract storage (redb) is poisoned'

MUSL_ASSET='freenet-x86_64-unknown-linux-musl.tar.gz'
RELEASE_BASE='https://github.com/freenet/freenet-core/releases/download'
# The endpoint the NODE uses for its startup check (the 302 from
# `/releases/latest`). Named once because two callers must hit exactly the same
# URL: `resolve_expected_latest`, which reads the tag out of the redirect, and
# `runner_can_reach_github`, whose whole job is to answer whether the runner can
# reach the endpoint the node just failed on. Two copies that could drift would
# make the corroboration compare different things.
RELEASES_LATEST_URL='https://github.com/freenet/freenet-core/releases/latest'

# The node's exit code for "another instance already holds my WS port"
# (EXIT_CODE_ALREADY_RUNNING in crates/core/src/bin/freenet.rs). Environmental,
# not an updater fault -- see where it is classified in the gate commands.
EXIT_CODE_ALREADY_RUNNING=43

# THIS SCRIPT's exit code for "the run was environmental; nothing was learned
# about the updater". Gate B only. `cross-compile.yml` keys the WORDING of its
# Matrix message off this, so the dev room stops being told the fleet may be
# stranded because a runner could not reach github.com for four seconds.
#
# 75 is sysexits.h's EX_TEMPFAIL, "temporary failure; the user is invited to
# retry", which is the convention this file already follows with its `exit 64`
# (EX_USAGE) for bad arguments.
#
# DELIBERATELY NOT 43. That is the NODE's exit code for a port collision, and
# reusing it here would make "the canary exited 43" mean either "another process
# held the port" or "the node started fine and GitHub was unreachable" --
# indistinguishable at the one moment someone is reading it under time pressure,
# on a release. The port collision is one of the two things that produce a 75;
# it is not what 75 means.
#
# The job still goes RED on a 75. Unverified is not verified, and reporting green
# on a run that proved nothing is the vacuous pass this whole file exists to
# remove. What changes is only what the alarm SAYS.
#
# Gate A deliberately does NOT use this: it BLOCKS, an unverified blocking gate
# leaves the release stuck as a draft, and a stuck draft always needs a human
# whatever the cause.
EXIT_UNVERIFIED_ENVIRONMENTAL=75

# Ports deliberately off the node defaults (31337 / 7509) so a canary run does
# not collide with a real node on the same host.
#
# Drawn from a random 8-port BLOCK per run rather than fixed constants. Fixed
# ports made two canary runs on one host collide by construction: reproduced
# with two `preflight` runs started 2s apart, the second dying with exit 43 and
# being reported as "the startup update check never ran". CI is a fresh VM per
# job today, but other jobs in this repo already use self-hosted runners, where
# that becomes a silent and permanently-misdiagnosed release blocker -- and it
# breaks the local-debugging path this file's header invites, which is exactly
# when someone is trying to understand a blocked release.
#
# Block of 8 with the WS port at +4. Both ports are incremented in LOCKSTEP at
# the top of each attempt in both gates, so the +4 gap is invariant and the
# network port can never walk onto this run's own WS port -- for any attempt
# count. (An earlier version of this comment said "at most +2, for
# CANARY_ATTEMPTS=3", which was wrong twice over: the increment happens before
# each run, so it is +N for N attempts, and the hazard it described is
# unreachable anyway because both ports move together. Gate B now has an attempt
# loop of its own, so the stale arithmetic described that too.) The block still
# earns its keep by keeping a run's ports away from a neighbouring run's.
# Both stay overridable; the lifecycle test pins them.
CANARY_PORT_BLOCK="${CANARY_PORT_BLOCK:-$(( 39000 + (RANDOM % 320) * 8 ))}"
CANARY_NETWORK_PORT="${CANARY_NETWORK_PORT:-$CANARY_PORT_BLOCK}"
CANARY_WS_PORT="${CANARY_WS_PORT:-$(( CANARY_PORT_BLOCK + 4 ))}"

# sanitise_positive_int <value> <fallback> -- echoes <value> if it is a positive
# integer, otherwise <fallback>.
#
# One function for all three budgets below, because the hand-written guard they
# each carried had the same hole and fixing one instance is how this file's own
# rules say not to do it. The guard was
# `case "$v" in ''|*[!0-9]*|0) v=$default ;; esac`, and `00` passes it: it is
# all digits and is not the literal string `0`. `seq 1 00` is EMPTY, so with
# `CANARY_ATTEMPTS=00` the retry loop never executes, `rc` is never assigned,
# and the read after the loop aborts under `set -u` with "rc: unbound variable"
# -- a shell error where a release verdict should be. `$((10#$v))` folds `00`,
# `007` and similar to their decimal value so the `-lt 1` test can see it; the
# digits-only case guard above it is what keeps `10#` itself safe.
sanitise_positive_int() {
  local value="$1" fallback="$2"
  case "$value" in
    ''|*[!0-9]*) printf '%s' "$fallback"; return ;;
  esac
  value=$((10#$value))
  if [ "$value" -lt 1 ]; then
    printf '%s' "$fallback"
  else
    printf '%s' "$value"
  fi
}

# How long to let the node run before giving up on the startup check. The
# check fires after a 0-60s anti-thundering-herd jitter, so this must clear
# 60s by a healthy margin; it is a ceiling, not a wait (both gates return as
# soon as they have their answer, typically ~40s).
CANARY_TIMEOUT_SECS="${CANARY_TIMEOUT_SECS:-240}"
# Validated like its two neighbours below. Without this a non-numeric override
# reaches the `$((...))` in the lifecycle guard and, under `set -u`, kills the
# canary with a shell arithmetic error rather than a canary verdict -- a
# release-blocking failure whose message says nothing about the release.
CANARY_TIMEOUT_SECS="$(sanitise_positive_int "$CANARY_TIMEOUT_SECS" 240)"

# Retry budget for the INDETERMINATE (GitHub unreachable) case only. Kept
# small on purpose: this sits on the release critical path inside a job with a
# fixed timeout, and an over-generous retry budget turns a network blip into a
# cancelled job and a permanently stuck draft -- worse than the failure it was
# trying to ride out.
CANARY_ATTEMPTS="${CANARY_ATTEMPTS:-2}"
# A non-numeric or zero override would make the retry loop body never execute
# and the script report "could not reach GitHub in 0 attempts" -- a blocking
# failure backed by no attempt at all.
CANARY_ATTEMPTS="$(sanitise_positive_int "$CANARY_ATTEMPTS" 2)"
CANARY_RETRY_SLEEP="${CANARY_RETRY_SLEEP:-20}"

# How long to wait, AFTER the "check started" line appears, for the check to
# log an outcome. The floor is set by production, not by taste: the check's
# network round trip is bounded by PROBE_CHAIN_TIMEOUT (10s, whole-chain --
# DNS, connect, redirects; crates/core/src/bin/commands/auto_update.rs), the
# parse is immediate after it, and the "check started" line is logged BEFORE
# the request begins. So any outcome that is ever going to be logged is logged
# within ~10s of that line, and this is 2x that. Below PROBE_CHAIN_TIMEOUT the
# canary stops the node mid-request and reads the resulting silence as health
# (#5236).
CANARY_OUTCOME_WAIT_SECS="$(sanitise_positive_int "${CANARY_OUTCOME_WAIT_SECS:-20}" 20)"

log()  { printf '%s\n' "$*"; }
fail() { printf '::error::%s\n' "$*" >&2; }
# An UNVERIFIED result, not a detected fault. Deliberately not `::error::`:
# annotating an unreachable GitHub as a workflow error trains people to ignore
# the annotation, and the caller decides whether to retry or fail.
note() { printf '%s\n' "$*" >&2; }
# A real GitHub ANNOTATION for something that is not a failure but must not be
# scrolled past. `note` is plain text, and a release job's log runs to several
# thousand lines -- which is fine for a line the caller is about to act on
# (a retry, a classification) and not fine for one saying an ASSERTION WAS
# SKIPPED. A skipped assertion that only whispers is the vacuous-pass shape this
# file keeps having to remove; `::warning::` puts it in the run summary where
# somebody sees it without reading the log.
warn() { printf '::warning::%s\n' "$*" >&2; }

# True when the logs show the node DECIDED to update. See the marker comments
# above for why this is a subtraction rather than a single grep.
#
# No trailing `| grep -q .`: `grep -q` exits at its FIRST match and closes the
# pipe, so the upstream grep takes SIGPIPE and dies 141. Under `set -o pipefail`
# (set at the top of this file) 141 becomes the pipeline's status, and the
# function reports "did not decide to update" for a node that plainly did.
# Measured: rc=0 at 400 matching lines, rc=141 at 700, 30/30 reproducible once
# the output passes the 64 KB pipe buffer. Canary logs never get that big, so
# this was latent rather than live -- but it fails in the direction of a wrong
# answer, not a loud one, and the fix is to not truncate the reader.
node_decided_to_update() {
  local logdir="$1" hits
  hits="$(grep -ahE "$MARKER_TRIGGERED_RE" "$logdir"/freenet.*.log 2>/dev/null \
    | grep -vF "$MARKER_NOT_TRIGGERED")"
  [ -n "$hits" ]
}

# True when the node REFUSED to update because the newer version is locally
# blocked -- the #4073 gate (`is_version_pinned_bad` / `is_version_install_gated`,
# crates/core/src/bin/commands/rollback.rs).
#
# A named predicate rather than an inline `log_has`, for the reason
# `prev_emits_latest_seen` is one: the DECISION it feeds is what a mutation would
# break, and a decision that has no name has nothing to test.
#
# WHY THIS IS A FAULT SIGNAL AND NOT MERELY INFORMATION. Both gates run the node
# under a HOME created fresh by `run_node_until_check` and wiped between
# attempts, and both of those #4073 predicates read their state from under
# `$HOME/.local/state`. A canary node therefore has no crash-loop pin and no
# install-failure history by construction, so neither predicate has anything it
# could legitimately match. If this line appears, the gating logic matched
# something that is not there -- which strands a node exactly as effectively as
# a parse bug, and looks identical to a healthy run in every marker the gate
# read before.
#
# No pipe, for the SIGPIPE reason documented on `node_decided_to_update`.
node_refused_locally_gated() {
  local logdir="$1"
  grep -aqF -- "$MARKER_NOT_TRIGGERED" "$logdir"/freenet.*.log 2>/dev/null
}

# True when the node exited 42 for one of the two NON-UPDATE reasons -- a fatal
# network-event-listener exit, or a poisoned redb. See the marker comments above
# for why 42 cannot be read as "an update was requested" without this.
#
# Takes the WORKDIR, not the log dir: both are `eprintln!`, so they are in
# `node.out`. A `pure` function still -- file in, boolean out -- so
# `auto-update-canary_test.sh` drives it from fixtures like its neighbours.
#
# Reads BOTH markers rather than one: the redb path was added separately
# (#4604) and reuses the listener path's exit-code decision, so a version of
# this that knew only about the listener would hard-block a healthy release on
# a disk error. No pipe, for the SIGPIPE reason on `node_decided_to_update`.
node_exited_on_fatal_abort() {
  local work="$1"
  grep -aqF -- "$MARKER_FATAL_LISTENER" "$work/node.out" 2>/dev/null && return 0
  grep -aqF -- "$MARKER_REDB_POISONED" "$work/node.out" 2>/dev/null
}

# True when the startup check reached a TERMINAL outcome -- any outcome, healthy
# or not. Either it ran to the end (MARKER_CHECK_COMPLETE, emitted on every
# non-triggering path) or it decided to update and returned early.
#
# This is the difference between "the check found nothing wrong" and "we stopped
# watching before it said anything", which every negative assertion in this file
# silently depends on and none of them can see on its own.
# No pipe here either, for the SIGPIPE reason documented on
# `node_decided_to_update`: `grep -q` reads the files directly instead.
node_check_settled() {
  local logdir="$1"
  if grep -aqF "$MARKER_CHECK_COMPLETE" "$logdir"/freenet.*.log 2>/dev/null; then
    return 0
  fi
  node_decided_to_update "$logdir"
}

# True when the node itself said it could not reach GitHub.
#
# `assert_detection_healthy` returns 2 for TWO different situations -- this one,
# and "the check started but never logged an outcome" -- and Gate B needs to
# tell them apart to word its alarm. It cannot: an exit code is one number, and
# the function is deliberately pure so it stays fixture-testable. So the caller
# re-reads the logs through this predicate rather than the function growing an
# out-parameter.
#
# WHY THIS MATTERS, and it is a live risk rather than a tidy-up. The node's
# startup fetch has NO retry (`startup_update_check_with_fetcher`, one
# `fetcher().await`, warn and return on Err), and it demonstrably fails: two
# WARNs on the `framework` laptop on 2026-08-11 alone, 17:08:04Z and 00:35:13Z,
#     Startup update check: failed to fetch latest version: error sending
#     request for url (.../releases/latest). Continuing with current binary.
# Gate B's subject is a PUBLISHED binary, so that no-retry fetch is a property
# of the thing under test and cannot be fixed from here. Adding one to the node
# is worth doing and is deliberately out of this file's scope.
#
# No pipe, for the SIGPIPE reason documented on `node_decided_to_update`.
node_could_not_reach_github() {
  local logdir="$1"
  grep -aqF "$MARKER_FETCH_FAIL" "$logdir"/freenet.*.log 2>/dev/null
}

# Fixed-string presence / extraction over the node's log FILES.
#
# These exist because the obvious spelling is broken in the same way the two
# functions above are, and it bit a REAL run rather than staying latent: with
# the logs slurped into a shell variable, `printf '%s' "$logs" | grep -aqF ...`
# has `grep -q` exit at its first match and SIGPIPE the `printf`, which dies
# 141; `set -o pipefail` (top of this file) promotes that to the pipeline's
# status, so the `if` reads a marker that IS PRESENT as ABSENT.
#
# It only fires once the log passes the 64 KB pipe buffer, which is why the
# fixtures never saw it -- but Gate A's normal path does: when the shipping
# binary is newer than latest the node does not exit 42, so it keeps logging
# (~33 KB/s measured) until the canary kills it 1-4s later. Measured on a real
# 3.65 MB node log: `grep -acF` = 1 (the line is there), piped `grep -q` = 141,
# direct `grep -q` = 0. Same content with only trailing volume varied: 1 KB
# passes, 200 KB reports "the startup update check never ran". It hit 2 of 3
# preflight runs, and `cmd_preflight` does not retry an rc=1 -- so a healthy
# release was blocked by an error naming the wrong subsystem.
#
# `grep -a`: the node writes some non-UTF8 bytes, and without it grep treats the
# file as binary. Keep it -- but for the opposite reason an earlier version of
# this comment gave. It claimed dropping `-a` "would silently satisfy every
# NEGATIVE check", i.e. a vacuous pass. Measured on GNU grep 3.11 with a NUL in
# the log, that is wrong: `grep -q` still reports matches in a binary file, so
# `log_has` is unaffected in BOTH directions (present -> rc=0, absent -> rc=1).
# What breaks is `log_lines`, whose STDOUT goes empty ("binary file matches"
# goes to stderr instead of the line). The first consumer to notice is the
# equality check's `seen_line`, which reads empty and fails with "the node never
# logged which release it compared against" -- a spurious BLOCKED release, not a
# vacuous pass. Fail-closed, so the direction matters to whoever is diagnosing
# it at the time.
log_has() {
  # log_has <log-dir> <fixed-string>
  grep -aqF -- "$2" "$1"/freenet.*.log 2>/dev/null
}
log_lines() {
  # log_lines <log-dir> <fixed-string>  -- matching lines on stdout, no headers
  grep -ahF -- "$2" "$1"/freenet.*.log 2>/dev/null
}

# NOTE on the glob above: `freenet.*.log` also matches `freenet.error.*.log`,
# which the node writes as a WARN+ subset of the same events. So a WARN-level
# marker is read from two files and matches twice. Harmless for every consumer
# today -- `log_has` only wants presence, and `log_lines` output is consumed by
# `head -2` or `tail -1` -- but a future count-based assertion would silently
# double for WARN markers and not for INFO ones. Narrow the glob before adding
# one.

# dump_node_evidence <workdir> -- the node's own output, to stderr.
#
# Gate A blocking with no evidence is the failure mode this exists for. The two
# branches most likely to fire on a HEALTHY release -- "the check never ran" and
# "started but never logged an outcome" -- printed no node output at all, unlike
# the parse-fail and fetch-fail branches which echo the offending lines. The
# EXIT trap then deletes the workdir, so a real blocking run left nothing
# behind, while `docs/RELEASING.md` told the operator to "read the job log; it
# names the offending line" -- true for a parse failure, false for exactly the
# two branches most likely to block a good release.
#
# Called by the gate commands rather than from inside `assert_detection_healthy`
# on purpose: that function is pure (log dir in, verdict out), which is what
# makes it unit-testable against fixtures, and it never sees the workdir.
dump_node_evidence() {
  local work="$1"
  printf '::group::canary node evidence (%s)\n' "$work" >&2
  if [ -s "$work/node.out" ]; then
    printf -- '--- node.out (last 40 lines) ---\n' >&2
    tail -40 "$work/node.out" >&2
  else
    printf -- '--- node.out is empty or absent ---\n' >&2
  fi
  # `ls` first: "which log files exist" is itself the answer when none do.
  printf -- '--- %s/logs ---\n' "$work" >&2
  ls -la "$work/logs" >&2 2>/dev/null || printf 'no log directory\n' >&2
  if grep -aq '' "$work/logs"/freenet.*.log 2>/dev/null; then
    printf -- '--- node log (last 40 lines) ---\n' >&2
    tail -q -n 40 "$work/logs"/freenet.*.log >&2 2>/dev/null
  fi
  printf '::endgroup::\n' >&2
}


# One workdir for the whole run, cleaned by a single EXIT trap.
#
# This was originally a `local` in each gate with a `trap ... RETURN`. Under
# `set -u` the trap fired after the local had gone out of scope, so EVERY gate
# exited 1 -- including a healthy binary. A canary that reports failure on
# success is worse than no canary: the first person to hit it learns to
# override it, and then it never catches anything real.
CANARY_WORKDIR="$(mktemp -d)"
if [ -z "$CANARY_WORKDIR" ] || [ ! -d "$CANARY_WORKDIR" ]; then
  # Without this, a failed mktemp leaves CANARY_WORKDIR empty and the later
  # `rm -rf "${work:?}"` operates on "/preflight" -- non-empty, so `:?` does
  # not catch it. The guard only protects against unset/empty, not against a
  # wrong-but-non-empty path.
  printf '::error::%s\n' "could not create a temp workdir for the canary" >&2
  exit 1
fi
cleanup() { rm -rf "$CANARY_WORKDIR"; }
trap cleanup EXIT

# ---------------------------------------------------------------------------
# assert_detection_healthy <log-dir>
#
# The two-sided assertion. Pure: reads log files, writes a verdict, touches
# nothing else -- which is what makes it unit-testable (see
# auto-update-canary_test.sh, which drives it with both a green and a red
# fixture; a canary nobody has ever seen go red is not a canary).
#
# Exit: 0 healthy, 1 broken, 2 indeterminate (GitHub unreachable -- infra, retry)
# ---------------------------------------------------------------------------
assert_detection_healthy() {
  local logdir="$1"

  # Every read below goes through `log_has`/`log_lines`, which grep the FILES.
  # Slurping them into a variable and piping it is what produced the
  # pipefail+SIGPIPE misreads documented on `log_has` -- do not reintroduce it.
  #
  # Emptiness is likewise tested without slurping: `grep -q ''` matches any
  # line, so this is "at least one non-empty log file exists". A missing glob
  # makes grep fail to open the literal name, which is also (correctly) empty.
  if ! grep -aq '' "$logdir"/freenet.*.log 2>/dev/null; then
    # Distinct wording on purpose: this is NOT evidence that the updater is
    # broken. The update task is spawned well inside network-node startup, so
    # anything that stops the node booting (port bind, config, gateway list)
    # lands here. Saying "auto-update is broken" would point whoever is on
    # call at the wrong subsystem.
    fail "canary produced no node logs at all in $logdir -- the node never started, so the updater was never reached. Investigate node startup, not the update path."
    return 1
  fi

  # (-) Did something silence the updater? Checked FIRST: it explains a missing
  #     startup line, and reporting "check never ran" instead would send the
  #     reader hunting for a parsing bug that isn't there.
  if log_has "$logdir" "$MARKER_DISABLED"; then
    fail "auto-update is DISABLED on the canary node. The canary cannot test the updater while the updater is turned off -- this is the #5040 drop-in failure mode that hid #5221 for two releases."
    log_lines "$logdir" "$MARKER_DISABLED" | head -2 >&2
    return 1
  fi

  # (+) POSITIVE side. Without this, every assertion below passes vacuously on
  #     a node that never checked for updates.
  if ! log_has "$logdir" "$MARKER_CHECK_RAN"; then
    # Hedged the same way the empty-log branch above is, and for the same
    # reason. The update task is spawned well inside network-node startup, so
    # anything that kills the node before it gets there lands HERE, not in that
    # branch -- the tracer is already up, so the log dir is non-empty. The
    # common one is a fresh config dir with no `gateways.toml`, which makes
    # `NodeConfig::new` fetch the remote gateway index
    # (`crates/core/src/config.rs`); if the runner cannot reach freenet.org the
    # node dies before the updater exists. Naming only the update path sends the
    # reader after a parsing bug that is not there, on a release someone is
    # waiting for.
    fail "the startup update check never ran: no '$MARKER_CHECK_RAN' line. Absence of a parse error here proves NOTHING -- the check did not happen. This is NOT by itself evidence that auto-update is broken: anything that stops the node reaching the update task lands here too (gateway-list fetch, config, port bind). Check the node output below for a startup failure BEFORE investigating the update path."
    return 1
  fi

  # (-) NEGATIVE side: the #5221 signature.
  if log_has "$logdir" "$MARKER_PARSE_FAIL"; then
    fail "the node could not parse the version GitHub returned -- auto-update is BROKEN. This is the #5221 regression: the release tag reached the detection path without being normalised."
    log_lines "$logdir" "$MARKER_PARSE_FAIL" | head -2 >&2
    return 1
  fi

  # Infrastructure, not a product bug: GitHub was unreachable or rate-limited,
  # so the check ran but learned nothing. Distinct exit code so the caller can
  # retry instead of failing a release on a transient network blip.
  if log_has "$logdir" "$MARKER_FETCH_FAIL"; then
    note "INDETERMINATE: could not reach GitHub to fetch the latest version."
    log_lines "$logdir" "$MARKER_FETCH_FAIL" | head -2 >&2
    return 2
  fi

  # (+) The check STARTED (asserted above) but never reached an outcome: no
  #     completion line, no trigger, and none of the failure markers either.
  #
  #     This is NOT success, and the whole point of the canary is that the two
  #     are told apart. Every negative check above is satisfied by a log that
  #     simply stops early, so without this branch a truncated run reports OK
  #     -- and a binary carrying the #5221 unparseable-tag bug passes Gate A
  #     and ships, as long as GitHub answered slower than the canary waited
  #     (#5236). Returning 2 rather than 1 because it is genuinely unknown:
  #     the caller retries, and a run that still cannot produce an answer
  #     fails the gate as UNVERIFIED instead of masquerading as a verdict.
  if ! node_check_settled "$logdir"; then
    note "INDETERMINATE: the startup update check started but never logged an outcome (no '$MARKER_CHECK_COMPLETE', no trigger, no failure). The check did not finish inside the canary's window, so this run proves NOTHING about the updater -- it is not evidence that parsing works."
    return 2
  fi

  # (+) POSITIVE EQUALITY. Everything above is satisfied by a comparator that is
  #     silently WRONG rather than broken: nothing so far has looked at the
  #     value the node compared against, only at whether it complained. Assert
  #     the observed latest equals the tag GitHub actually published.
  #
  #     `CANARY_EXPECTED_LATEST` is supplied by the CALLER, not fetched here, so
  #     this function stays pure and unit-testable against log fixtures. The
  #     caller (cmd_preflight) resolves it from the GitHub API and returns
  #     INDETERMINATE if it cannot -- so "unset" never reaches here on the
  #     release path, and the skip below cannot silently disarm the gate on a
  #     real run. `auto-update-canary_test.sh` pins that cmd_preflight resolves
  #     it, exports it, and refuses when it cannot -- without that pin this
  #     skip branch would be exactly the vacuous escape hatch the gate exists
  #     to remove.
  if [ -n "${CANARY_EXPECTED_LATEST:-}" ]; then
    local seen_line seen
    seen_line="$(log_lines "$logdir" "$MARKER_LATEST_SEEN" | tail -1)"
    if [ -z "$seen_line" ]; then
      fail "the node never logged which release it compared against (no '$MARKER_LATEST_SEEN'). Without it a comparator that silently returns the wrong version -- a constant, or a truncated tag -- produces a log byte-identical to a healthy one, so 'no error' is not evidence that detection works."
      return 1
    fi
    # `latest=0.2.121` -- Display-formatted, so unquoted; take the last field
    # and strip any trailing punctuation the formatter may add.
    seen="${seen_line##*latest=}"
    seen="${seen%% *}"
    seen="$(printf '%s' "$seen" | tr -d '"'"'"'\r')"
    if [ "$seen" != "$CANARY_EXPECTED_LATEST" ]; then
      fail "the node compared against the WRONG release: it logged latest='$seen' but GitHub's latest published release is '$CANARY_EXPECTED_LATEST'. Detection is silently broken -- it did not fail to parse, it parsed the wrong thing, which is why every other check above passed. A constant-returning or truncating version_from_tag looks exactly like this."
      printf '%s\n' "$seen_line" >&2
      return 1
    fi
    log "OK: the node compared against '$seen', which matches GitHub's latest release."
  else
    # Deliberately loud. Reaching this on the release path would mean the
    # caller stopped resolving the expected tag, and the gate would quietly
    # drop from "compared against the right release" to "did not complain".
    note "NOTE: CANARY_EXPECTED_LATEST is unset, so the positive-equality check was SKIPPED. This run does not prove the node compared against the right release."
  fi

  log "OK: startup update check ran to completion and parsed GitHub's response."
  log_lines "$logdir" "$MARKER_CHECK_RAN" | head -2
  return 0
}

# ---------------------------------------------------------------------------
# gate_a_expected_decision <shipping-version> <observed-latest>
#
# WHICH DECISION a healthy updater is obliged to reach, given the two versions
# it is comparing. Echoes exactly one of:
#
#   decline  -- shipping >= latest. Nothing newer exists, so the only correct
#               outcome is to stay put. This is the normal release path: Gate A
#               runs while its own release is still a DRAFT, so the binary under
#               test is newer than `/releases/latest` by construction.
#   update   -- shipping <  latest. Something newer really does exist, so the
#               only correct outcome is to request it. RARE, and worth being
#               exact about how rare, because the obvious framing overstates
#               it: `cross-compile.yml` sparse-checks-out this script at the
#               TAG being gated ("the script version matches the release it is
#               gating"), so re-running an OLDER release's workflow runs THAT
#               tag's copy of this file, which does not contain this code at
#               all. What is actually left is a hotfix branch cut on an older
#               line and tagged after a newer release has published (the branch
#               carries this script, and its binary really is behind
#               `/releases/latest`), a re-run of a tag cut after this landed,
#               and a manual `preflight` run on an older binary -- the
#               local-debugging path this file's header invites.
#   unknown  -- one of the two is not a bare dotted version, so the direction
#               cannot be established.
#
# THE DIRECTION IS THE WHOLE REASON THIS EXISTS. Asserting "must have declined"
# unconditionally would be right on the normal path and WRONG on the paths
# above, where it would block a release for the correct behaviour of a healthy
# updater. A blocking gate that false-fails does not merely cost one release: it
# teaches people to override it, and an overridden gate catches nothing. That
# those paths are narrow is a reason to keep the conditional cheap, not a reason
# to drop it -- the cost of getting it wrong is paid on a release nobody expected
# to be blocked, which is the worst moment to be debugging the gate.
#
# `version_at_least` is the comparator because it is `sort -V` (0.2.9 < 0.2.10,
# which a lexical compare gets wrong) and because it is already tested. The
# `is_dotted_version` guard runs FIRST so the `unknown` case is decided here
# rather than by `version_at_least`'s own refusal, which would also emit its
# malformed-input note for an input we are choosing to handle.
#
# EQUAL counts as `decline`, matching `compare_versions_for_startup`: it returns
# Some(latest) only for latest STRICTLY greater than current.
# ---------------------------------------------------------------------------
gate_a_expected_decision() {
  if ! is_dotted_version "$1" || ! is_dotted_version "$2"; then
    printf 'unknown'
  elif version_at_least "$1" "$2"; then
    printf 'decline'
  else
    printf 'update'
  fi
}

# ---------------------------------------------------------------------------
# assert_gate_a_decision <log-dir> <expected-decision>
#
# WHICH DECISION the updater actually reached, asserted positively.
#
# `assert_detection_healthy` deliberately does not do this, and the gap is not
# an oversight there -- it is shared by both gates, which require OPPOSITE
# decisions, so it can only assert the parts common to both. What it leaves
# uncovered is the decision itself:
#
#   * `node_check_settled` accepts a completion line OR a trigger, so a node
#     that decided to UPDATE counts as settled;
#   * the positive-equality check validates the version the node compared
#     AGAINST, not what it did with it;
#   * the #4073 refusal reaches the completion line too, so it reads as an
#     ordinary healthy ending.
#
# So a shipping binary with an inverted comparison passes all seven checks and
# ships. That is the mirror image of #5221 and strands peers identically:
# `latest < current` is TRUE for a Gate A run, so an inverted comparator returns
# the OLDER release and the node requests a self-DOWNGRADE. `docs/RELEASING.md`
# has described this hole since #5236.
#
# PURE, like `assert_detection_healthy` and for the same reason: log dir and a
# decision word in, verdict out, so it is driven by fixtures in
# `auto-update-canary_test.sh`. The expected decision is passed in rather than
# computed here because computing it needs the shipping binary's version, which
# is caller state. `NODE_EXIT` is caller state too, and its assertion therefore
# lives in `cmd_preflight`.
#
# Exit: 0 the node reached the decision it had to, 1 it did not.
# There is deliberately no 2: this runs only after `assert_detection_healthy`
# has returned 0, so the check is already KNOWN to have started, parsed and
# settled. Nothing left here is indeterminate, and a wrong decision is not a
# thing a re-run fixes.
# ---------------------------------------------------------------------------
assert_gate_a_decision() {
  local logdir="$1" expect="$2"

  # DIRECTION-INDEPENDENT, and checked first because it is the sharper finding.
  # It rests only on the canary's fresh isolated HOME (see
  # `node_refused_locally_gated`), which holds in every arm, so no version
  # relationship can make this refusal legitimate.
  #
  # WHERE IT ACTUALLY EARNS ITS KEEP, stated honestly rather than sold: the
  # `update` and `unknown` arms, plus Gate B (which names it separately). In the
  # `decline` arm it is close to unreachable, because the node only reaches the
  # #4073 branch at all from inside
  # `if let Some(new_version) = startup_update_check(...)` -- so entering it on
  # the normal path already requires an inverted comparator, and with a fresh
  # HOME the pin/gate would then not match either, so the TRIGGER fires and the
  # decline arm below catches it first. Two independent defects, not one. It is
  # kept in every arm regardless because it is free, it cannot false-positive
  # (the premise holds unconditionally), and it is genuinely reached where it
  # matters: the refusal falls through to the completion line, so
  # `assert_detection_healthy` returns 0 and this function runs.
  if node_refused_locally_gated "$logdir"; then
    fail "the node REFUSED to update because it treated the newer version as locally blocked (#4073) -- but this canary node has a fresh, isolated HOME with no crash-loop pin and no install-failure history, so there is nothing for that gate to have matched. The locally-blocked check (is_version_pinned_bad / is_version_install_gated in crates/core/src/bin/commands/rollback.rs) is matching wrongly. A node that refuses every release it is offered is stranded exactly as thoroughly as one that cannot parse the tag."
    log_lines "$logdir" "$MARKER_NOT_TRIGGERED" | head -2 >&2
    return 1
  fi

  case "$expect" in
    decline)
      if node_decided_to_update "$logdir"; then
        fail "the shipping binary DECIDED TO UPDATE, and there is nothing for it to update to: its own release is still a draft, so it is at or ahead of everything GitHub publishes. A healthy updater stays put here. This is the mirror image of #5221 -- detection did not fail, it reached the WRONG ANSWER, which is why every other check passed. An inverted comparison in compare_versions_for_startup (crates/core/src/bin/commands/auto_update.rs) produces exactly this, and it makes the node request a self-DOWNGRADE to the older release, then exit 42 and do it again on every restart. If you are running \`preflight\` by hand on a binary that really IS older than the latest release, that is the other explanation, and the log line above this one says which versions were compared."
        grep -ahE "$MARKER_TRIGGERED_RE" "$logdir"/freenet.*.log 2>/dev/null \
          | grep -vF "$MARKER_NOT_TRIGGERED" | head -2 >&2
        return 1
      fi
      log "OK: the node declined to update, which is the only correct outcome when nothing newer than the shipping binary is published."
      ;;
    update)
      if ! node_decided_to_update "$logdir"; then
        fail "the binary under test is genuinely OLDER than GitHub's latest release and did NOT decide to update to it. A node in that position must request the update; one that stays put can never leave the version it is on, which is the #5221 outcome reached by a different route. (Gate A is running in its off-normal arm here -- a re-run of an older release's workflow, or a hotfix cut on an older line -- and the log line above says which versions were compared.)"
        return 1
      fi
      log "OK: the node decided to update, which is the correct outcome for a binary older than GitHub's latest release."
      ;;
    *)
      # LOUD, and never silent. A skipped arm that printed nothing would be the
      # vacuous escape hatch this gate family keeps having to remove: a reader
      # of a green log could not tell an asserted decision from an unasserted
      # one. The refusal check above still ran, so this is a partial skip, not
      # a skipped function.
      #
      # `warn`, not `note`, and the difference is the whole point of the branch.
      # This arm disables the decision check AND (because that check is scoped to
      # the `decline` arm in `cmd_preflight`) the exit-42 check with it, so
      # reaching it silently reverts Gate A to its pre-decision-check strength
      # while the job stays green. One change to the shape of `--version`'s
      # output is all it takes. `::warning::` surfaces in the run summary rather
      # than in line 4,000 of a release log; the source pin on the `--version`
      # format in auto-update-canary_test.sh is the other half.
      warn "the canary could not establish whether the shipping binary is newer or older than GitHub's latest release, so it did NOT assert which decision the updater had to reach, and the exit-42 check is skipped with it. The #4073 refusal check above still ran. This run does not prove the comparator answered correctly -- see the version line logged by cmd_preflight for the two values it could not compare."
      ;;
  esac
  return 0
}

# ---------------------------------------------------------------------------
# run_node_until_check <binary> <workdir>
#
# Boot the node in an isolated tree and stop as soon as the startup check has
# produced a verdict (or the timeout expires). Sets NODE_EXIT.
# ---------------------------------------------------------------------------
NODE_EXIT=""
run_node_until_check() {
  local binary="$1" work="$2"
  # `$work/tmp` is created HERE, with the rest of the tree, rather than inside
  # the subshell next to the `export TMPDIR` that uses it. Gate A would not
  # care -- the node's own `create_dir_all` builds its parents -- but Gate B
  # does: the real updater stages through `tempfile::tempdir()` (`update.rs`),
  # which requires TMPDIR to EXIST and will not create it. A `mkdir` inside the
  # backgrounded subshell also fails invisibly (no `set -e`, and its output
  # goes to `node.out`), so a broken workdir would surface as a mystery
  # update-path failure rather than as itself.
  if ! mkdir -p "$work/home/.local/state/freenet" "$work/cfg" "$work/data" \
                "$work/logs" "$work/tmp"; then
    fail "could not create the canary workdir under $work -- this is a harness/disk problem, not an auto-update fault."
  fi

  # An isolated HOME matters for more than tidiness: the node keeps its GitHub
  # poll token-bucket under $HOME/.local/state/freenet, so a shared HOME would
  # let one gate's budget throttle the other's check.
  # Job control, so this background job gets its own process group, AND the
  # `exec` below, which is the half that actually makes the kill work.
  #
  # `kill $node_pid` alone reaps only the subshell and leaves the
  # `timeout`/`freenet` grandchild alive (verified), still holding the UDP and
  # WS ports and burning CPU while the next attempt tries to boot. But `set -m`
  # by itself is NOT enough either: job control is inherited, so the `timeout`
  # inside the subshell starts a process group of its OWN and a group kill on
  # the subshell misses it (also verified -- the pgids differ). `exec` collapses
  # the two: the subshell BECOMES timeout, so the job pid is timeout's pid and
  # its child shares the group. The exports still apply, since they run before
  # the exec replaces the shell.
  set -m
  (
    # shellcheck disable=SC2030  # scoping HOME to this subshell is the point:
    # the node keeps its GitHub poll bucket under $HOME, and the caller's HOME
    # must not be touched on a machine that is already running a node.
    export HOME="$work/home"
    # Tell the node a supervisor is present, exactly as the systemd unit does,
    # so it takes the real exit-42 path rather than logging a "no supervisor"
    # error and staying put.
    export FREENET_SUPERVISED=1
    # The gate reads the node's log, and release builds rate-limit that log
    # (1000 events/s aggregate plus a per-callsite cap, tracing/tracer.rs:557).
    # A dropped line is indistinguishable from a line that was never emitted,
    # and the directions are not symmetric: losing MARKER_CHECK_RAN or the
    # completion line fails the gate LOUDLY (red / indeterminate), but losing
    # the parse-failure WARN while the completion line survives leaves the
    # negative check satisfied and the canary reporting OK on a binary carrying
    # the #5221 bug -- a false GREEN, the exact class this canary exists to
    # remove. The startup check emits a handful of lines and comes nowhere near
    # either cap, so this is a latent risk rather than an observed one; the env
    # var removes the class outright for the cost of one line. Only the
    # canary's own throwaway node is affected.
    export FREENET_DISABLE_LOG_RATE_LIMIT=1
    # `client_api.rs` unconditionally `create_dir_all`s
    # `std::env::temp_dir()/freenet/webs` (`let contract_web_path =`) when it
    # builds the router, and that path does NOT follow `--data-dir`. The
    # directory itself is VESTIGIAL -- nothing reads or writes it (`"webs"` has
    # exactly one occurrence in `crates/`, the mkdir), and unpacked web
    # contracts live under `default_webapp_cache_dir` instead. So the only thing
    # it can still do is PANIC when the mkdir fails, which it does two ways:
    #
    #   1. `$TMPDIR/freenet` is a FILE. `cross-compile.yml` stages the binary it
    #      is about to gate at `/tmp/freenet`, exactly that path, so
    #      `create_dir_all` hits ENOTDIR against the binary and the node panics
    #      (exit 101) before the update task ever spawns. Gate A then reports
    #      "the startup update check never ran" and blocks. That is what
    #      happened to v0.2.124: the shipping binary was fine, the gate's own
    #      harness was not. Measured on the SHIPPED v0.2.124 artifact staged at
    #      that path, this fix against a reverted-fix control:
    #          fixed    rc=0, "compared against '0.2.123'"
    #          control  rc=1, node exited 101, panicked in `client_api.rs`
    #                   ("Not a directory (os error 20)")
    #   2. `$TMPDIR/freenet` is a directory owned by ANOTHER USER and has no
    #      `webs` child yet -- EACCES, same panic, which is what the panic's own
    #      "may happen if ... was created by another user" text is about. Two
    #      canary runs on one host do NOT collide here: they would share a
    #      directory that is always empty and never read. PORTS are the shared
    #      resource that actually collides; see the header.
    #
    # Relocating the staged binary in `cross-compile.yml` WOULD have unblocked
    # v0.2.124 -- tested: pre-fix script, a scratch TMPDIR with no `freenet`
    # entry in it, binary staged outside it, and the gate returns rc=0 "the node
    # compared against '0.2.123'". An earlier version of this comment claimed
    # relocation was "NOT sufficient"; that was wrong, and it was wrong for an
    # instructive reason -- the run behind it was on a host where
    # `/tmp/freenet` already existed as another user's directory, i.e. case (2),
    # generalised into a claim about case (1).
    #
    # Isolation is still the better fix, for (2) rather than (1): relocation
    # leaves the canary sharing the caller's temp dir, so on a machine already
    # running a node under another user the gate can still panic on a
    # `/tmp/freenet` it does not own -- an auto-update verdict decided by who
    # owns a directory. Scoped to this subshell, so only the canary's throwaway
    # node is affected.
    # shellcheck disable=SC2030  # subshell-local is the point, same as HOME
    # above: the caller's TMPDIR must not move on a machine already running a
    # node. Gate B's `freenet update` sets its own (see `cmd_selfupdate`),
    # which is what makes shellcheck notice this one at all.
    export TMPDIR="$work/tmp"
    # The webapp cache does not follow TMPDIR or --data-dir either: it resolves
    # through `directories::ProjectDirs` (`default_webapp_cache_dir`), which
    # reads XDG_CACHE_HOME AHEAD of HOME, and `FREENET_WEBAPP_CACHE_DIR`
    # overrides both. `WebappCache::with_root` create_dir_all's it on every
    # boot, so on a host with XDG_CACHE_HOME set the canary would write outside
    # its workdir. Unlike the vestigial mkdir above this one fails SOFT (a warn,
    # the node carries on), so it never blocked a release -- it is here to make
    # the header's isolation claim true rather than nearly true. Setting the
    # explicit override is what closes it; XDG_CACHE_HOME is scoped too so any
    # other cache-dir consumer lands in the workdir as well.
    export XDG_CACHE_HOME="$work/cache"
    export FREENET_WEBAPP_CACHE_DIR="$work/cache/freenet-webapp"
    exec timeout "$CANARY_TIMEOUT_SECS" "$binary" network \
      --config-dir "$work/cfg" \
      --data-dir "$work/data" \
      --log-dir "$work/logs" \
      --network-port "$CANARY_NETWORK_PORT" \
      --ws-api-port "$CANARY_WS_PORT" \
      >"$work/node.out" 2>&1
  ) &
  local node_pid=$!
  set +m

  # Poll for a verdict rather than sleeping the full timeout: the check fires
  # after a 0-60s jitter, so this normally returns in well under a minute and
  # adds no meaningful time to a release.
  #
  # Measured against the CLOCK rather than by counting `sleep 3` iterations,
  # so the budget means what it says regardless of how long a pass takes.
  #
  # Not a bug fix, and deliberately not described as one: the counting version
  # charged 3s per pass while each pass cost slightly more, which makes the
  # loop run marginally LONGER than nominal, not shorter. The false "the
  # startup update check never ran" verdict that prompted this was caused by
  # leaked nodes from earlier runs stealing CPU (see the `exec` note above),
  # not by this loop.
  local deadline=$(( $(date +%s) + CANARY_TIMEOUT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if ! kill -0 "$node_pid" 2>/dev/null; then
      break   # node exited on its own (exit 42 on the selfupdate path)
    fi
    if grep -aqF "$MARKER_CHECK_RAN" "$work/logs"/freenet.*.log 2>/dev/null; then
      # The check has STARTED. Wait for it to FINISH.
      #
      # This was a flat `sleep 5`, which was shorter than the timeout the check
      # itself runs under. The marker matched above is logged BEFORE the network
      # request begins, and that request is bounded by PROBE_CHAIN_TIMEOUT (10s).
      # So a GitHub that answered in 6s -- a loaded runner, a redirect hop, a
      # mild rate-limit backoff -- had its node SIGTERMed before it could log
      # success OR a parse failure OR a fetch failure, and
      # `assert_detection_healthy` then read that silence as health. A binary
      # carrying the exact #5221 bug this canary exists to catch would have
      # passed Gate A and been published (#5236).
      #
      # Poll for the outcome instead, on a budget with real headroom over
      # PROBE_CHAIN_TIMEOUT, and stop the moment it arrives -- so the common
      # case is FASTER than the old fixed sleep, not slower. If the outcome
      # never arrives the logs say so and `assert_detection_healthy` returns
      # INDETERMINATE; the one thing that must not happen is reporting OK.
      local outcome_deadline=$(( $(date +%s) + CANARY_OUTCOME_WAIT_SECS ))
      while [ "$(date +%s)" -lt "$outcome_deadline" ] && [ "$(date +%s)" -lt "$deadline" ]; do
        node_check_settled "$work/logs" && break
        # A node that has exited has logged everything it is going to log.
        kill -0 "$node_pid" 2>/dev/null || break
        sleep 1
      done
      # If the node decided to update, it exits 42 on its own. Killing it here
      # would replace that with 143 and silently defeat Gate B's exit-42
      # assertion -- the canary would report "no update requested" for a node
      # that requested one. Let it finish.
      if node_decided_to_update "$work/logs"; then
        # Clamped by `deadline` as well as by its own 60s budget. Every other
        # wait in this loop honours the outer ceiling; this was the one arm that
        # ignored it, so CANARY_TIMEOUT_SECS could be overrun by up to a minute.
        # It was bounded in practice only because the node dies at its own
        # `timeout $CANARY_TIMEOUT_SECS` -- an accident of a sibling mechanism,
        # not a guarantee this loop makes.
        local settle_deadline=$(( $(date +%s) + 60 ))
        [ "$settle_deadline" -gt "$deadline" ] && settle_deadline="$deadline"
        while kill -0 "$node_pid" 2>/dev/null && [ "$(date +%s)" -lt "$settle_deadline" ]; do
          sleep 2
        done
      fi
      break
    fi
    sleep 3
  done

  # Stop the node if it is still up, then reap it for its exit code. Kill the
  # whole process GROUP (see `set -m` above) so no node outlives this call.
  kill -- "-$node_pid" 2>/dev/null || kill "$node_pid" 2>/dev/null
  wait "$node_pid"
  NODE_EXIT=$?
  log "node exited with code $NODE_EXIT"
}

# ---------------------------------------------------------------------------
# resolve_expected_latest
#
# The tag GitHub currently publishes as "latest", normalised the way
# `version_from_tag` normalises it (strip AT MOST one leading `v`). Echoes it
# on stdout; returns 1 if it cannot be determined.
#
# Deliberately the SAME source the node itself uses --
# `github.com/{repo}/releases/latest`, read from the 302 `Location` -- and not
# `api.github.com`. Two reasons, both load-bearing:
#
#   1. Comparing the node's answer against a DIFFERENT endpoint would compare
#      two things that are allowed to disagree, and the mismatch would fail a
#      release for a reason that is not a bug.
#   2. The REST API allows 60 unauthenticated requests/hour per source IP,
#      shared across everything on that runner. The redirect endpoint is served
#      by the web front end and draws on no such budget -- which is exactly why
#      #5102 moved the node off the API. Spending REST quota here would
#      reintroduce that cost on the release critical path.
#
# During Gate A our own release is still a DRAFT, so this correctly resolves to
# the PREVIOUS release -- the same thing the node under test sees.
# ---------------------------------------------------------------------------

# normalise_release_tag <tag>
#
# Split out from the fetch so it can be tested without a network: the whole
# check turns on this matching what the node does, and a normaliser nobody can
# test is how the mismatch it exists to catch would be introduced.
#
# `${tag#v}` strips AT MOST ONE leading `v`, mirroring version_from_tag's
# `strip_prefix` -- deliberately not the greedy `${tag##v*}`, because
# `trim_start_matches` semantics would turn `vv1.2.3` into `1.2.3` and lose what
# is needed to address the release. The rustdoc on version_from_tag documents
# the same hazard.
normalise_release_tag() {
  printf '%s' "${1#v}"
}

# is_dotted_version <s> -- true for a bare dotted numeric version (`0.2.125`).
#
# Deliberately narrower than semver: this repo cuts no pre-release or build
# metadata, `sort -V` orders `0.2.125-rc.1` ABOVE `0.2.125` where semver puts it
# below, and accepting a form the comparator gets wrong is worse than refusing
# it. If rc tags are ever cut, fix the comparator before widening this.
is_dotted_version() {
  case "$1" in
    ''|*[!0-9.]*|.*|*.) return 1 ;;
    *..*)               return 1 ;;
    *)                  return 0 ;;
  esac
}

# version_at_least <a> <b> -- true when dotted version <a> is >= <b>.
#
# `sort -V` rather than a field split: it gets 0.2.9 < 0.2.10 right, which a
# lexical compare does not, and equal inputs land on the last line either way.
#
# MALFORMED INPUT IS REFUSED, LOUDLY, and that is not defensive tidying -- the
# bare comparison answers TRUE for garbage. Measured on GNU coreutils 9.4:
# `version_at_least "not-a-version" "0.2.125"` is TRUE (non-numeric sorts after
# digits under `sort -V`), and `version_at_least "0.2.125" ""` is TRUE. So an
# unreadable version or an emptied constant silently PASSES a comparison it
# should not even be able to answer. Refusing returns false, so callers must
# decide what an unanswerable comparison means for them rather than inheriting
# an accidental yes; `prev_emits_latest_seen` below does exactly that.
version_at_least() {
  if ! is_dotted_version "$1" || ! is_dotted_version "$2"; then
    note "version_at_least: refusing to compare '$1' with '$2' -- not both bare dotted versions. Treating as FALSE; the comparison cannot be answered, and answering it by accident is how a malformed value passes a version gate."
    return 1
  fi
  [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | tail -1)" = "$1" ]
}

# prev_emits_latest_seen <prev-version> -- true when a binary at that version
# emits MARKER_LATEST_SEEN, so Gate B's positive-equality check can arm.
#
# A named function rather than the comparison inlined at its one call site, so
# the DECISION can be tested directly. Inlined, the only thing pinning it was a
# grep for the literal call text -- and `if ! version_at_least …` contains that
# text just as `if version_at_least …` does, so the pin stayed green under an
# inversion. Inverted, Gate B arms against pre-#5236 binaries (a spurious red)
# and SKIPS for post-#5236 ones, which is the silent direction: its only
# positive assertion goes permanently vacuous while every release looks fine.
#
# An UNUSABLE version arms the check rather than skipping it, which is the
# opposite of what `version_at_least` returns for the same input and is
# deliberate. The two functions answer different questions. "Is a >= b" has no
# answer for garbage, so the comparator refuses. "Should Gate B assert anything"
# does have one: skipping is the silent direction this whole file exists to
# remove, and arming against an unknown binary costs at worst a loud red on a
# release whose version string was already unreadable. Fail toward asserting.
prev_emits_latest_seen() {
  if ! is_dotted_version "$1"; then
    note "could not read the previous release's version from '$1', so whether it predates the observed-latest marker is unknown. ARMING Gate B's positive-equality check anyway: a skip here is silent and permanent, while a spurious red is visible and cheap. Check how the previous release was resolved."
    return 0
  fi
  version_at_least "$1" "$MARKER_LATEST_SEEN_SINCE"
}

resolve_expected_latest() {
  local url tag
  # `--retry`: this call has no second chance anywhere else. Its failure returns
  # 1 from cmd_preflight BEFORE the attempt loop is entered, so unlike every
  # node-side indeterminate -- which gets CANARY_ATTEMPTS tries -- a single
  # transient blip here blocks the release outright. The comment above justifies
  # the no-retry stance with "not something a re-run fixes", which is true of the
  # NODE's verdict and not of one curl. `--retry-all-errors` because the
  # interesting failures (connection reset, DNS blip) are not HTTP statuses,
  # which is all bare `--retry` covers.
  url="$(curl -fsS --max-time 30 --retry 2 --retry-all-errors \
    -o /dev/null -w '%{redirect_url}' \
    "$RELEASES_LATEST_URL" 2>/dev/null)" || return 1
  case "$url" in
    */releases/tag/*) tag="${url##*/releases/tag/}" ;;
    *) return 1 ;;
  esac
  [ -n "$tag" ] || return 1
  normalise_release_tag "$tag"
}

# True when THIS RUNNER can reach the release endpoint the node uses.
#
# A named function rather than the call inlined, so the decision below can be
# unit-tested by overriding it. Same reason `prev_emits_latest_seen` exists.
#
# IT ASKS CURL DIRECTLY rather than going through `resolve_expected_latest`,
# and the difference is the whole point. That function collapses every
# non-answer to `return 1` -- a 403, a captive portal, a changed redirect shape,
# curl missing from the image -- so "the probe malfunctioned" and "the network
# is down" become the same value. Read as corroboration, every one of those
# falls to the QUIET path, which is the direction that hides things.
#
# So only the CONNECT-CLASS curl exits count as "cannot reach". Anything else,
# including success and including an HTTP error, means the runner did reach
# GitHub and the node's inability to is not explained by the network -- loud.
# An unusable body is likewise not evidence of an outage. Curl absent (127)
# lands in the same arm, which is right: a broken harness must not buy silence.
#
#   6  could not resolve host      7  failed to connect
#   28 operation timed out         35 SSL connect error
#
# Not a full closure of the class: a transient probe failure coinciding with a
# genuine node-side fault still yields one quiet run. A PERSISTENT probe break
# is caught earlier -- Gate A exercises the same path on the shipping binary and
# blocks -- and the quiet message's own consecutive-releases text covers the
# rest.
runner_can_reach_github() {
  local rc=0
  curl -fsS --max-time 30 -o /dev/null "$RELEASES_LATEST_URL" 2>/dev/null || rc=$?
  case "$rc" in
    6|7|28|35) return 1 ;;
    *)         return 0 ;;
  esac
}

# gate_b_unverified_class <env-cause> -- "environmental" or "fault", for a Gate
# B run that produced no verdict. Echoes; never exits.
#
# WHY A DECISION AND NOT JUST A LOOKUP. The quiet Matrix message says the run
# tells us nothing and needs no fleet action, so every input that reaches it is
# a release NOT verified by the gate that exists to verify it. Trusting a single
# WARN from the node to send us down that path is how a persistent problem
# becomes a permanent silence: five releases in a row report "environmental",
# nobody is alarmed, and we believe we have a post-publish gate when we have
# verified nothing since v0.2.125.
#
# So the GitHub case demands corroboration from a second, independent observer
# in the same run -- this runner's own fetch of the same endpoint. That splits
# two situations a single WARN cannot:
#
#   node failed, runner ALSO cannot reach GitHub -> the network really is bad
#       right now. Genuinely environmental, quiet, re-run.
#   node failed on EVERY attempt, runner is fine -> the published binary
#       consistently cannot do something this runner can, seconds apart. That is
#       not a blip. It may still not be a fault (a poll-budget cooldown is the
#       obvious candidate, #5102) but it is not something to reassure anyone
#       about, so it takes the loud path.
#
# THE RESIDUAL WINDOW, stated rather than implied: a condition that persistently
# breaks the node's fetch AND this runner's probe but NOT the release-asset
# download would still be quiet every time. Both are github.com and Gate B
# downloads the previous release's tarball moments earlier -- if that fails the
# gate has already returned a loud failure -- so a sustained CI-wide outage
# cannot reach the quiet path at all. What is left is narrow.
#
# The PORT case needs no probe: another process holding a port says nothing
# about the network, and demanding a network probe for it would make a
# collision on a healthy runner take the loud path for no reason.
#
# ONE MORE THING THIS CANNOT SEE, and it is why the quiet message says what it
# says about recurrence. The GitHub branch trusts the node's self-report of WHY
# its fetch failed, and that same WARN fires for a fetch-side REGRESSION too --
# a malformed URL, a TLS or user-agent change, a 403 from a rate-limited
# endpoint (#5102 moved the node off `api.github.com` for exactly that). Such a
# regression would look environmental on every release, permanently. Gate A
# narrows it, because it runs the same code path on the binary being shipped, so
# a regression introduced in THIS tree fails there first. What is left is a
# regression already published, which is why "if this recurs across consecutive
# releases it is not the runner" belongs in the message rather than in a comment.
gate_b_unverified_class() {
  case "$1" in
    ports)
      printf 'environmental'
      ;;
    github)
      if runner_can_reach_github; then
        printf 'fault'
      else
        printf 'environmental'
      fi
      ;;
    *)
      # Includes the empty cause: the check started and never logged an
      # outcome, which is what a HUNG updater looks like. Never quiet.
      printf 'fault'
      ;;
  esac
}

# ---------------------------------------------------------------------------
# Gate A: preflight -- BLOCKS publication.
#
# Runs the binary we are about to ship against the CURRENT latest release and
# asserts its detection path is healthy. Catches "the updater we are shipping
# cannot read GitHub's release tags" while the release is still a draft.
# ---------------------------------------------------------------------------
cmd_preflight() {
  local binary="$1"
  local work="$CANARY_WORKDIR/preflight"
  mkdir -p "$work"

  log "=== Gate A: auto-update pre-flight on the binary about to ship ==="
  # Captured rather than just printed: the version is now an INPUT to the gate
  # (it decides which decision the updater is obliged to reach), not decoration.
  local version_output version_line shipping_version gate_a_expect
  version_output="$("$binary" --version)"
  log "$version_output"
  # Selected BY ITS MARKER, not by line position, and that is deliberate.
  # `--version` already prints a second line (`Build timestamp: ...`), so the
  # output is not one line and never was; taking `head -1` would make the gate's
  # input depend on nothing more than the ORDER of two `println!`s. Add a banner
  # line ahead of the version -- a deprecation notice, a build warning -- and a
  # positional read silently yields an unparseable field, which sends the whole
  # gate down the `unknown` arm and skips both new assertions while the job stays
  # green. Matching the line that identifies itself costs nothing and removes
  # that class outright.
  #
  # `| head -1` is used for its stdout, not its status, which is what keeps it
  # clear of the SIGPIPE ban documented in auto-update-canary_test.sh.
  version_line="$(printf '%s\n' "$version_output" | grep -F 'Freenet version:' | head -1)"
  # Field 3 of `Freenet version: 0.2.127 (deadbeefcafe)`, the same split
  # cmd_selfupdate uses on the same output.
  shipping_version="$(printf '%s' "$version_line" | awk '{print $3}')"

  # Resolve what the node SHOULD see before booting it, so the log assertion can
  # be a positive equality rather than an absence-of-error. Failing to resolve
  # it is INDETERMINATE, never a pass: without it the gate silently weakens to
  # "the node did not complain", which is what a silently-wrong comparator
  # produces. Returning 1 here (not 2) because the retry loop below re-runs the
  # whole attempt for rc=2, and a resolution failure is not something a node
  # re-run fixes -- it is an infrastructure problem the operator must see.
  # A caller may pin the expected release (the lifecycle test does, to stay off
  # the network). What a pinned value cannot do is DISARM the check: skipping
  # needs an EMPTY value, and empty is treated as unset here, so
  # `CANARY_EXPECTED_LATEST=` falls through to resolving from GitHub or
  # refusing.
  #
  # It CAN make the check PASS, though, and an earlier version of this comment
  # claimed otherwise. Pin a value that happens to agree with a comparator that
  # is silently wrong and the equality check confirms the wrong answer -- the
  # asserted-instead-of-resolved shape this gate exists to replace. So the
  # release path must never pin it; the workflow does not, and
  # `release_canary_wiring_test.sh` pins that it stays that way.
  if [ -n "${CANARY_EXPECTED_LATEST:-}" ]; then
    log "using the caller-supplied expected release '$CANARY_EXPECTED_LATEST' (not resolving from GitHub)."
  elif ! CANARY_EXPECTED_LATEST="$(resolve_expected_latest)"; then
    fail "could not resolve GitHub's latest release tag, so the canary cannot check WHICH release the node compared against. This is an UNVERIFIED result, not a detected bug: re-run this job. Do NOT un-draft the release by hand -- an unverified gate is not a passed gate."
    return 1
  fi
  export CANARY_EXPECTED_LATEST
  log "GitHub's latest published release is '$CANARY_EXPECTED_LATEST'; the node must compare against exactly that."

  # Which DECISION the updater is obliged to reach, decided once here because
  # neither version changes across attempts. Logged in every arm, so a reader of
  # a green job can tell which assertion actually ran -- an unstated arm is how
  # a gate comes to be trusted for something it did not check.
  gate_a_expect="$(gate_a_expected_decision "$shipping_version" "$CANARY_EXPECTED_LATEST")"
  case "$gate_a_expect" in
    decline)
      log "the binary under test (v$shipping_version) is at or ahead of GitHub's latest (v$CANARY_EXPECTED_LATEST) -- the normal Gate A shape, since this release is still a draft. A healthy updater MUST decline to update, and that is what is asserted."
      ;;
    update)
      log "the binary under test (v$shipping_version) is BEHIND GitHub's latest (v$CANARY_EXPECTED_LATEST). This is NOT the normal release path -- a re-run of an older release's workflow, or a hotfix cut on an older line, produces it. A healthy updater MUST decide to update, so that is what is asserted instead."
      ;;
    *)
      warn "could not compare the shipping version ('$shipping_version') with GitHub's latest ('$CANARY_EXPECTED_LATEST') -- at least one is not a bare dotted version. Gate A will NOT assert which decision the updater reached, nor check its exit code. Every other check still runs; the arms skipped are the ones that catch an inverted comparator."
      ;;
  esac

  # Retry only the INDETERMINATE case. A parse failure is deterministic and
  # retrying it just burns release time; a GitHub blip is worth a second look
  # before we stall a release on it.
  # `saw_fatal_abort` LATCHES, for the reason Gate B's `saw_unexplained` does:
  # it changes only the WORDING of the final blocking message, and a run where
  # one attempt died on a fatal abort must not be described as "GitHub
  # unreachable, or the check never logged an outcome" just because a later
  # attempt was. It never makes the gate greener -- every path below is still a
  # blocked release.
  local attempt rc saw_fatal_abort=0
  for attempt in $(seq 1 "$CANARY_ATTEMPTS"); do
    log "--- attempt $attempt/$CANARY_ATTEMPTS ---"
    # Wipe the WHOLE tree, not just the logs. The node persists its GitHub
    # poll token-bucket and rate-limit cooldown under $work/home; reusing them
    # means a retry after a 429 re-reads the same persisted cooldown and
    # reports the identical INDETERMINATE without ever asking GitHub again.
    # A retry that cannot produce a different answer is not a retry.
    rm -rf "${work:?}"
    mkdir -p "$work"
    # Distinct ports per attempt. The process-group kill above should already
    # guarantee the previous node is gone; this makes a retry survive even if
    # some future refactor reintroduces a lingering child.
    CANARY_NETWORK_PORT=$((CANARY_NETWORK_PORT + 1))
    CANARY_WS_PORT=$((CANARY_WS_PORT + 1))
    run_node_until_check "$binary" "$work"
    # Classify the port collision BEFORE the log assertion, because the log
    # assertion cannot see it: `assert_detection_healthy` never consults
    # NODE_EXIT, so a node that died on exit 43 without an update-check line
    # reads as "the startup update check never ran" -- an auto-update fault
    # reported for another process holding the port. Reproduced with two
    # `preflight` runs 2s apart.
    #
    # Returning 2 (environmental) rather than 1 is the load-bearing half: rc=1
    # skips the retry loop, and the ports are redrawn every attempt, so the very
    # next attempt would have succeeded. This is precisely what rc=2 is for.
    if [ "$NODE_EXIT" = "$EXIT_CODE_ALREADY_RUNNING" ]; then
      note "INDETERMINATE: the node exited $EXIT_CODE_ALREADY_RUNNING (another instance already holds ports $CANARY_NETWORK_PORT/$CANARY_WS_PORT). This is a port collision on this host -- another canary run, or a local node -- not an auto-update fault. Retrying on a fresh port."
      rc=2
    else
      assert_detection_healthy "$work/logs"
      rc=$?
      # WHICH DECISION, asserted only once the log assertion is happy: that one
      # localises the failure, and a node whose parse failed reaches no decision
      # worth reporting on. See `assert_gate_a_decision` for why the seven
      # checks above cannot see this.
      if [ "$rc" -eq 0 ]; then
        assert_gate_a_decision "$work/logs" "$gate_a_expect"
        rc=$?
      fi
      # The SECOND, independent observer of the same decision, and the reason it
      # is worth having both: the log check reads a phrase this repo chooses,
      # while exit 42 is the supervisor contract itself (see "NOTE ON
      # AUTO-UPDATE" at the top of crates/core/src/bin/freenet.rs) -- what
      # actually makes the fleet restart onto another binary.
      #
      # `-ne 1`, NOT `-eq 0`, AND THAT IS THE WHOLE POINT OF THE OBSERVER.
      # It was `-eq 0`, which made the observer unreachable on precisely the
      # input it exists for. A trigger site whose wording `MARKER_TRIGGERED_RE`
      # misses -- or a dropped trigger line -- means the node logs NO matched
      # trigger, and `freenet.rs` `return`s immediately after `update_tx.send`,
      # so the completion line is never emitted either. `node_check_settled` is
      # then false, `assert_detection_healthy` returns 2, and an `-eq 0` gate
      # skips. Gate A burned both attempts and reported "produced no verdict --
      # GitHub unreachable, or the check never logged an outcome" for a
      # deterministic self-downgrade. Reproduced on both variants.
      #
      # rc=1 is deliberately left alone: `assert_detection_healthy` has already
      # found a definite fault (an unparseable tag, a disabled updater, a node
      # that never started), and those localise better than "it exited 42". That
      # is also what keeps this off the "check never started" case the exit code
      # alone cannot explain.
      #
      # `-ne 1` ADMITS TWO rc=2 ROUTES, not one, and the second is worth naming.
      # `assert_detection_healthy` also returns 2 for `MARKER_FETCH_FAIL`. If the
      # node could not reach GitHub it cannot have learned of a newer release, so
      # a 42 on that run must be the fatal listener -- which
      # `node_exited_on_fatal_abort` catches, giving rc=2 and a retry. The gap is
      # narrow but real: if the CRITICAL wording drifts, this branch hard-blocks
      # with a message asserting "the only release available is OLDER than
      # itself" about a run where the node never learned what was available. It
      # needs BOTH conditions at once and the wording is pinned, so it is
      # recorded rather than guarded. Under `-eq 0` it was unreachable.
      #
      # It covers ONE of the log check's two blind directions, not both, and the
      # OTHER one is worth stating exactly because the obvious guess is wrong.
      # In the `decline` arm a reworded or missing trigger is caught here. In the
      # `update` arm it is NOT caught, and it does NOT surface as "did NOT decide
      # to update": every trigger site `return`s straight after `update_tx.send`,
      # so a node that triggered emits no completion line either, `rc` is 2, and
      # `assert_gate_a_decision` is never reached at all (it is gated on rc=0
      # above). The run burns both attempts and blocks as UNVERIFIED with
      # "produced no verdict" -- the same wrong-subsystem diagnosis this observer
      # removes from the `decline` arm, on a node that behaved correctly. Still a
      # false BLOCK, so still fail-closed; the mechanism is just not the decision
      # check. Not fixed here because the exit-42 observer must stay scoped to
      # `decline` (exiting 42 is CORRECT in the update arm, so the code cannot
      # discriminate), and that arm is off the normal release route.
      # Latent rather than live -- `run_node_until_check` exports
      # FREENET_DISABLE_LOG_RATE_LIMIT=1 for exactly this class, and Gate B
      # depends on the same line anyway -- but the asymmetry is worth knowing
      # before treating the pair as a full cross-check.
      #
      # Only in the `decline` arm. In the `update` arm exiting 42 is the CORRECT
      # behaviour, and asserting it there instead would add a false-block risk
      # (a node that triggers late may still be alive when the canary stops it)
      # for a path that is off the normal release route anyway.
      #
      # AND ONLY WITH CORROBORATION, because 42 is overloaded: it is also
      # `FATAL_LISTENER_EXIT_CODE` (see the marker comments at the top of this
      # file), which a HEALTHY binary emits when its network event listener dies
      # or redb is poisoned -- neither of which says anything about the updater.
      # Blocking a release on that would send the operator to
      # `compare_versions_for_startup`, code that is fine, which is how a
      # blocking gate earns the reputation that gets it overridden.
      #
      # So the node's own CRITICAL line decides which it was:
      #   present -> ENVIRONMENTAL. rc=2, retried on a fresh tree, and a run
      #              that only ever produces this fails as UNVERIFIED rather
      #              than as an updater fault.
      #   absent  -> the updater really did ask to be replaced. rc=1, no retry:
      #              deterministic, and no re-run changes it.
      # Both messages name the other cause. A gate that confidently blames the
      # wrong subsystem is worse than one that says it is unsure.
      if [ "$rc" -ne 1 ] && [ "$gate_a_expect" = "decline" ] && [ "$NODE_EXIT" = "42" ]; then
        if node_exited_on_fatal_abort "$work"; then
          note "INDETERMINATE: the node exited 42, but its output carries a CRITICAL fatal-abort line (network event listener exited, or redb poisoned). 42 is ALSO FATAL_LISTENER_EXIT_CODE (crates/core/src/node/p2p_impl.rs), and the canary does not set SYSTEMD_FAST_CRASH_ENV_VAR, so that path uses 42 at every uptime. This is NOT an auto-update fault and NOT evidence the updater is healthy -- the node died for an unrelated reason. Retrying on a fresh tree."
          saw_fatal_abort=1
          rc=2
        else
          fail "the shipping binary exited 42 (the update-requested code the supervisor acts on) -- it asked its supervisor to replace it, and the only release available is OLDER than itself. A healthy Gate A run ends at 143, because the canary SIGTERMs a node that is still going; reaching 42 means this node exited on its own. Publishing this strands the fleet in a downgrade-and-restart loop: the supervisor installs the older binary, that binary detects this one as newer, and the cycle repeats. Check compare_versions_for_startup in crates/core/src/bin/commands/auto_update.rs for an inverted comparison. (42 is also FATAL_LISTENER_EXIT_CODE, but the canary looked for the CRITICAL fatal-abort line in the node output below and did not find one, so this is not a listener or redb exit.) If you cannot find a 'triggering auto-update' line in the log below, that is expected and is NOT evidence against this verdict: a trigger site whose wording MARKER_TRIGGERED_RE no longer matches produces exactly this shape -- no matched trigger, and no completion line either, because freenet.rs returns as soon as it sends. Check MARKER_TRIGGERED_RE against the trigger sites in freenet.rs before concluding the exit code is wrong."
          rc=1
        fi
      fi
    fi
    # Keep the evidence before the next attempt wipes the tree, or the EXIT trap
    # deletes it. Only on a non-zero verdict, so a healthy release stays quiet.
    [ "$rc" -eq 0 ] || dump_node_evidence "$work"
    [ "$rc" -eq 2 ] || return "$rc"
    if [ "$attempt" -lt "$CANARY_ATTEMPTS" ]; then
      log "indeterminate (no verdict from the update check); retrying in ${CANARY_RETRY_SLEEP}s"
      sleep "$CANARY_RETRY_SLEEP"
    fi
  done

  if [ "$saw_fatal_abort" -eq 1 ]; then
    fail "the shipping binary's update check produced no verdict in $CANARY_ATTEMPTS attempts, and on at least one of them the node exited 42 after a CRITICAL fatal abort (network event listener exited, or redb poisoned -- see the node output above). That is NOT the update-requested exit and NOT an auto-update fault: 42 is also FATAL_LISTENER_EXIT_CODE (crates/core/src/node/p2p_impl.rs), and the canary does not opt into the distinct code 45. Nothing was learned about the updater, so this is an UNVERIFIED result rather than a detected bug: re-run this job, and if it recurs look at why the node's listener or storage is dying on this runner. Do NOT un-draft the release by hand -- an unverified gate is not a passed gate."
    return 1
  fi
  fail "the shipping binary's update check produced no verdict in $CANARY_ATTEMPTS attempts -- GitHub unreachable, or the check never logged an outcome. Cannot confirm its updater works. This is an UNVERIFIED result, not a detected bug: re-run this job. Do NOT un-draft the release by hand to work around it -- an unverified gate is not a passed gate."
  return 1
}

# ---------------------------------------------------------------------------
# Gate B: selfupdate -- end-to-end, after publication.
#
# Takes the PREVIOUS release, points it at the real network, and requires it
# to detect this release, exit 42, and self-replace. This is the assertion
# that actually matters to the fleet: not "the log looks right" but "a node on
# the old version ends up on the new one".
# ---------------------------------------------------------------------------
cmd_selfupdate() {
  local prev_version="$1" expected_version="$2"
  local work="$CANARY_WORKDIR/selfupdate"
  mkdir -p "$work"

  log "=== Gate B: does v$prev_version self-update to v$expected_version? ==="
  mkdir -p "$work/bin"
  if ! curl -fsSL --max-time 300 -o "$work/prev.tar.gz" \
       "$RELEASE_BASE/v${prev_version}/${MUSL_ASSET}"; then
    fail "could not download the previous release (v$prev_version) -- cannot run the canary."
    return 1
  fi
  if ! tar xzf "$work/prev.tar.gz" -C "$work/bin" || [ ! -s "$work/bin/freenet" ]; then
    fail "the previous release archive (v$prev_version) did not extract to a usable binary -- cannot run the canary. This is a download/packaging problem, not an updater problem."
    return 1
  fi
  chmod +x "$work/bin/freenet"

  local starting
  starting="$("$work/bin/freenet" --version | head -1)"
  log "starting from: $starting"

  # Arm the positive-equality check, as Gate A does. Gate B runs AFTER
  # publication, so `releases/latest` IS this release: the previous release's
  # binary must observe `expected_version`, and there is no need to re-resolve
  # it from GitHub -- the caller already knows which release it just published,
  # and asking again would only introduce a second source allowed to disagree.
  #
  # Without this the deliberately-loud "CANARY_EXPECTED_LATEST is unset" NOTE
  # fired on EVERY healthy Gate B run (the command runs in its own process, so
  # nothing Gate A exported reaches it). A warning that appears on every good
  # release is one everybody learns to scroll past, which is worse than no
  # warning: it is the same alarm-fatigue failure that let `--disable-auto-update`
  # sit on `framework` for nine days.
  #
  # Version-gated for the one release where the previous binary predates the
  # marker -- see MARKER_LATEST_SEEN_SINCE.
  if prev_emits_latest_seen "$prev_version"; then
    export CANARY_EXPECTED_LATEST="$expected_version"
  else
    unset CANARY_EXPECTED_LATEST
    note "NOTE: v$prev_version predates the observed-latest log line (#5236, first emitted by v$MARKER_LATEST_SEEN_SINCE), so Gate B's positive-equality check is SKIPPED. It arms itself once the previous release is v$MARKER_LATEST_SEEN_SINCE or newer; no action needed."
  fi

  # RETRY the environmental cases, exactly as Gate A does. Gate B had NO retry
  # at all -- `CANARY_ATTEMPTS` was read only by `cmd_preflight` -- so a single
  # transient blip in a ~40s window decided a release's post-publish verdict,
  # and the blip is not hypothetical: the previous release's own startup fetch
  # has no retry (see `node_could_not_reach_github`).
  #
  # `$work/bin` is preserved across attempts and everything else is wiped, which
  # is the opposite of Gate A's `rm -rf "${work:?}"` and deliberate: the
  # downloaded previous release is the SUBJECT, not state. State must go, for
  # Gate A's reason -- the node persists its GitHub poll token-bucket and
  # rate-limit cooldown under `$work/home`, so an attempt that reuses them
  # re-reads the same cooldown and reports the identical INDETERMINATE without
  # ever asking GitHub. A retry that cannot produce a different answer is not a
  # retry.
  #
  # ONLY rc=2 (no verdict) and the port collision loop, and that boundary is the
  # load-bearing part of this whole block rather than an optimisation.
  #
  # A retry over a REAL assertion failure is the disaster this canary exists to
  # prevent, arriving by way of the canary. Each attempt wipes the tree and boots
  # a fresh node, so a genuine detection fault that happens to be intermittent --
  # a race, a timing-dependent parse, a node that reaches the update path only
  # sometimes -- would eventually produce one passing attempt, and Gate B would
  # report the release healthy. A flaky pass on the post-publish gate is strictly
  # worse than no gate: it is a green light nobody re-examines.
  #
  # rc=1 therefore breaks IMMEDIATELY, and that is pinned behaviourally in
  # `auto-update-canary_lifecycle_test.sh` by counting node boots, not by
  # grepping for this comment.
  #
  # The loop also cannot run twice after a decision to update: that path leaves
  # the loop with rc=0.
  # `saw_unexplained` LATCHES, and `env_cause` alone would not. It records that
  # some attempt produced an indeterminate the node could not explain -- the
  # hung-updater shape -- and once that has happened no later attempt may buy
  # the quiet message.
  #
  # Without the latch the flag is last-writer-wins, and the sequence that breaks
  # it is ordinary rather than contrived: attempt 1 starts the check and logs no
  # outcome (a real finding), attempt 2 loses a port race (environmental), and
  # the dev room is told a hung updater on the previous release is "not a
  # stranded fleet and needs no fleet action". A false quiet on exactly the
  # class this gate exists to raise, produced by the retry that was added to
  # reduce false alarms. This file's own rule: a gate's unknown case belongs on
  # the side that gets read.
  #
  # `rc=1` up front so a `CANARY_ATTEMPTS` that somehow yields an empty `seq`
  # cannot reach the read below unset. Under `set -u` that is an "unbound
  # variable" abort, which exits 1 and takes the LOUD path -- safe, but the
  # operator gets a shell error where a verdict should be.
  local attempt rc=1 env_cause="" attempt_cause="" saw_unexplained=0 github_attempts=0
  for attempt in $(seq 1 "$CANARY_ATTEMPTS"); do
    log "--- attempt $attempt/$CANARY_ATTEMPTS ---"
    rm -rf "${work:?}/home" "${work:?}/cfg" "${work:?}/data" \
           "${work:?}/logs" "${work:?}/tmp" "${work:?}/cache"
    # Distinct ports per attempt, as Gate A does, so a collision that caused the
    # retry cannot cause the next one too.
    CANARY_NETWORK_PORT=$((CANARY_NETWORK_PORT + 1))
    CANARY_WS_PORT=$((CANARY_WS_PORT + 1))
    run_node_until_check "$work/bin/freenet" "$work"

    # Classified BEFORE the log assertion, for the same reason as in Gate A: the
    # assertion never consults NODE_EXIT, so a node that died on a port
    # collision without an update-check line reads as "the startup update check
    # never ran" -- an auto-update fault reported for another process holding a
    # port.
    if [ "$NODE_EXIT" = "$EXIT_CODE_ALREADY_RUNNING" ]; then
      note "INDETERMINATE: the node exited $EXIT_CODE_ALREADY_RUNNING (another instance already holds ports $CANARY_NETWORK_PORT/$CANARY_WS_PORT). A port collision on this host -- another canary run, or a local node -- not an auto-update fault."
      rc=2
      attempt_cause=ports
    else
      # The two-sided log assertion first: it LOCALISES the failure. If detection
      # is broken the version check below would also fail, but with a far less
      # useful message.
      assert_detection_healthy "$work/logs"
      rc=$?
      # Which KIND of indeterminate THIS attempt was. Deliberately per-attempt
      # and NOT the run's answer: the fold below decides that, and it latches.
      #
      # (This comment used to read "recorded from the LAST attempt", which was
      # true of the last-writer-wins version and became false when the latch
      # landed. A reviewer read the tree, read this line, and correctly
      # concluded the bug was still present -- from the comment, not the code.
      # A stale comment on a fixed bug costs a review round.)
      #
      # Only the node saying outright that it could not reach GitHub is a
      # CANDIDATE for the quiet path; "the check started and never logged an
      # outcome" is not, because a hung updater produces exactly that.
      # Candidacy is not the verdict -- see `gate_b_unverified_class`, which
      # demands corroboration on top.
      if [ "$rc" -eq 2 ] && node_could_not_reach_github "$work/logs"; then
        attempt_cause=github
      else
        attempt_cause=""
      fi
    fi

    # Fold this attempt into the run-level classification, STICKY BY STRENGTH
    # rather than by recency:
    #
    #     unexplained  >  github  >  ports
    #
    # `saw_unexplained` is the top tier and is handled separately below. The
    # same rule has to apply one level down, and the first version of this got
    # that wrong: `env_cause="$attempt_cause"` unconditionally, so the two
    # orderings of the same pair of attempts disagreed.
    #
    #   ports THEN github  -> env_cause=github -> the runner probe runs -> loud.
    #   github THEN ports  -> env_cause=ports  -> the probe NEVER RUNS, because
    #                         a port collision is not probed, so the run exits
    #                         75 quiet AND claims "every attempt hit a port
    #                         collision on this host", which is false.
    #
    # Reachable at the default two attempts. A recorded `github` must therefore
    # never be downgraded to `ports`: ports is the weakest cause because it is
    # the only one that buys the quiet path without any corroboration.
    if [ "$rc" -eq 2 ]; then
      case "$attempt_cause" in
        '')     saw_unexplained=1 ;;
        github) env_cause=github; github_attempts=$((github_attempts + 1)) ;;
        ports)  [ "$env_cause" = "github" ] || env_cause=ports ;;
        *)      # An unrecognised cause is not quiet-eligible; treat it as
                # unexplained rather than letting it fall through unrecorded.
                saw_unexplained=1 ;;
      esac
    fi

    # Keep the node's own output before the next attempt wipes the tree, or the
    # EXIT trap deletes it. Only on a non-zero verdict, so a healthy release
    # stays quiet.
    [ "$rc" -eq 0 ] || dump_node_evidence "$work"
    [ "$rc" -eq 2 ] || break
    if [ "$attempt" -lt "$CANARY_ATTEMPTS" ]; then
      log "indeterminate (no verdict from the update check); retrying in ${CANARY_RETRY_SLEEP}s"
      sleep "$CANARY_RETRY_SLEEP"
    fi
  done

  if [ "$rc" -eq 2 ]; then
    # EVERY branch below is a release this gate did NOT verify. They differ only
    # in whether we can say why, and therefore in how loudly to say it -- never
    # in whether the job is red.
    #
    # The latch is READ here. It is SET inside the loop, by the precedence fold
    # (an earlier version of this said the loop "stays a plain record of what
    # each attempt saw", which stopped being true when the fold moved in).
    if [ "$saw_unexplained" -eq 1 ]; then
      fail "UNVERIFIED: at least one attempt started the update check and never logged an outcome, so the canary could not determine whether v$prev_version reaches v$expected_version. This is NOT evidence that auto-update is broken, and NOT evidence that it works -- but it is also NOT environmental noise, whatever a later attempt ran into: the node did not report a failed GitHub fetch on the attempt that produced this. A hung updater looks exactly like it. Re-run the job."
      return 1
    fi
    if [ "$(gate_b_unverified_class "$env_cause")" = "environmental" ]; then
      # Distinct exit code so `cross-compile.yml` can word the Matrix message
      # accordingly: the alarm that says a node "may not be able to auto-update
      # to this one" must not fire for a runner that could not open a socket.
      # Still non-zero, still a red job -- unverified is not verified.
      case "$env_cause" in
        ports)
          fail "UNVERIFIED (ENVIRONMENTAL): every attempt hit a port collision on this host, so Gate B never got to test the updater. v$expected_version has NOT been verified as reachable by a node on v$prev_version -- this run says nothing either way. Another canary run or a local node is holding the ports. Re-run the job; if it recurs, something on this runner is holding them persistently and the gate is not working."
          ;;
        *)
          fail "UNVERIFIED (ENVIRONMENTAL): v$prev_version could not reach GitHub on $github_attempts of $CANARY_ATTEMPTS attempt(s), and this runner cannot reach it either, so the canary never got to test whether v$prev_version reaches v$expected_version. v$expected_version has NOT been verified as reachable by auto-update -- this run is not evidence in either direction. The node's startup fetch has no retry, so a bad network moment produces exactly this. Re-run the job. If Gate B reports this on CONSECUTIVE releases it is not the runner: the same WARN is also what a published fetch-side regression logs (a bad URL, a TLS or user-agent change, a rate-limited endpoint), and either way the post-publish gate is not working and nothing has been verified since the last green run."
          ;;
      esac
      return "$EXIT_UNVERIFIED_ENVIRONMENTAL"
    fi
    if [ "$env_cause" = "github" ]; then
      # The node said it could not reach GitHub on at least one attempt -- and
      # this runner reached the same endpoint moments later. Not a blip, so not
      # something to reassure anyone about.
      #
      # "at least one", not "every": sticky-by-strength promotes `github` over
      # `ports`, so a mixed run lands here with one github attempt. Both this
      # branch and its environmental SIBLING above print `$github_attempts`
      # rather than `$CANARY_ATTEMPTS` for that reason. The sibling was missed
      # when this one was fixed, and stated the false unanimity for a further
      # commit -- see the enumeration note in
      # `.claude/rules/bug-prevention-patterns.md`.
      fail "UNVERIFIED: v$prev_version reported it could not reach GitHub on $github_attempts of $CANARY_ATTEMPTS attempt(s), but THIS RUNNER reached the same endpoint immediately afterwards. So the network is not simply down: the published binary consistently cannot do something this runner can. That may still not be an auto-update fault -- a poll-budget cooldown persisted under the node's HOME is the obvious candidate (#5102) -- but it is not environmental noise, and v$expected_version is NOT verified as reachable. Read the node output above before re-running."
      return 1
    fi
    # Unreachable today: every rc=2 either records a cause or sets
    # `saw_unexplained`, both handled above. Kept because "the classifier grew a
    # case nobody routed" must not fall out of the function with rc=2 read as a
    # SUCCESS -- the shape that let #5236 ship.
    fail "UNVERIFIED: the update check produced no verdict in $CANARY_ATTEMPTS attempts and the canary could not classify why (env_cause='"'"'$env_cause'"'"'). This is a canary bug, not a verdict about v$prev_version. Re-run the job and report this message."
    return 1
  fi
  [ "$rc" -eq 0 ] || return 1

  # Gate B needs no mirror of `assert_gate_a_decision`: it already asserts its own
  # direction, here and at the exit-42 check below, and its direction is fixed
  # (the release IS published by now, so the previous binary must always decide
  # to update). What it did not do is say WHY it stayed put when the node told
  # it -- so the #4073 refusal, the one non-obvious cause, gets named. Message
  # only; the verdict is unchanged either way.
  if ! node_decided_to_update "$work/logs"; then
    if node_refused_locally_gated "$work/logs"; then
      fail "v$prev_version saw v$expected_version and REFUSED it as locally blocked (#4073: crash-loop known-bad pin or repeated install failures). The canary runs it under a fresh, isolated HOME with no such history, so there is nothing for that gate to have matched -- is_version_pinned_bad / is_version_install_gated (crates/core/src/bin/commands/rollback.rs) is matching wrongly, and every node on v$prev_version will refuse this release the same way. Do NOT read this as the ordinary 'stayed put' case."
      log_lines "$work/logs" "$MARKER_NOT_TRIGGERED" | head -2 >&2
      return 1
    fi
    fail "v$prev_version parsed GitHub's response but did NOT decide to update to v$expected_version. The release is published and visible, so a node on the previous version is choosing to stay put -- the fleet will not converge."
    return 1
  fi

  if [ "$NODE_EXIT" != "42" ]; then
    fail "expected the node to exit 42 (update requested) but it exited $NODE_EXIT. The supervisor contract is what applies the update; without exit 42 the fleet never restarts onto the new binary."
    return 1
  fi

  # The supervisor half of the contract, exactly as the systemd unit does it:
  # exit 42 -> `freenet update` -> restart onto the new binary.
  log "--- node requested an update (exit 42); running \`freenet update\` as the supervisor would ---"
  if ! (
    # shellcheck disable=SC2031  # deliberate: `freenet update` must read the
    # same isolated state dir the node wrote, and nothing outside it.
    export HOME="$work/home"
    # `download_and_install` stages the downloaded tarball in
    # `tempfile::tempdir()` (`update.rs`, and the macOS DMG path likewise),
    # which follows TMPDIR. Without this the header's "both runs keep their
    # FILES to their own temp directory" was true of Gate A only: Gate B wrote
    # a release tarball into the ambient system temp dir. Safe for the swap --
    # `replace_binary` copies to a `.freenet.new.tmp` beside the DESTINATION
    # and renames there, so the atomic same-filesystem rename never involves
    # TMPDIR. The directory itself is created with the rest of the workdir in
    # `run_node_until_check`, which Gate B always runs before reaching here.
    # shellcheck disable=SC2031  # deliberate, as for HOME above: this subshell
    # sets its own copy; nothing outside it reads the change.
    export TMPDIR="$work/tmp"
    "$work/bin/freenet" update --quiet
  ); then
    fail "\`freenet update\` failed -- the node asked for an update and the installer could not apply it."
    return 1
  fi

  local final
  final="$("$work/bin/freenet" --version | head -1)"
  log "ended at: $final"

  # Field-exact, not a substring: `grep -F 0.2.12` also matches "0.2.121".
  if [ "$(printf '%s' "$final" | awk '{print $3}')" != "$expected_version" ]; then
    fail "self-update did NOT land on v$expected_version. Started at '$starting', ended at '$final'. A node on the previous release will not reach this one on its own."
    return 1
  fi

  log "OK: v$prev_version -> v$expected_version end-to-end (detect, exit 42, install)."
  return 0
}

usage() {
  cat <<'EOF'
Usage:
  auto-update-canary.sh preflight <path-to-freenet-binary>
      Gate A (blocking, pre-publish): the binary about to ship can parse the
      current latest release tag.

  auto-update-canary.sh selfupdate <prev-version> <expected-version>
      Gate B (post-publish): the previous release self-updates to this one,
      end to end. Versions are bare semver, no leading "v".

  auto-update-canary.sh assert-logs <log-dir>
      Run just the two-sided log assertion over an existing log directory.
EOF
}

main() {
  case "${1:-}" in
    preflight)   [ $# -eq 2 ] || { usage; exit 64; }; cmd_preflight "$2" ;;
    selfupdate)  [ $# -eq 3 ] || { usage; exit 64; }; cmd_selfupdate "$2" "$3" ;;
    assert-logs) [ $# -eq 2 ] || { usage; exit 64; }; assert_detection_healthy "$2" ;;
    *)           usage; exit 64 ;;
  esac
}

# Only run main when executed directly, so the test script can source this
# file and drive `assert_detection_healthy` without booting a node.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
  main "$@"
fi
