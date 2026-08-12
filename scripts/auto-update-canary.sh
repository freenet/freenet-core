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
# Run either locally; both are self-contained and touch nothing outside their
# own temp directory (isolated HOME, config, data, log dirs, non-default
# ports), so this is safe to run on a machine already running a node.
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
#   :524 "Startup check: newer version on GitHub, triggering auto-update"
#   :609 "Urgent update confirmed on GitHub, triggering immediate auto-update"
#   :670 "Update confirmed on GitHub after stagger, triggering auto-update"
#   :751 "Newer version confirmed on GitHub, triggering auto-update"
#   :873 "Periodic re-poll: newer version on GitHub, triggering auto-update"
#   :519 "...repeated install failures); not triggering auto-update (#4073)"
# Matching the bare substring counts the refusal as a trigger; anchoring on any
# ONE site's full phrase misses the others, reporting "did not decide to
# update" for a node that did. So: match the phrase, subtract the refusal.
#
# A REGEX, not a fixed string, and that is the whole point: the urgent site at
# :609 says "triggering IMMEDIATE auto-update", so the fixed substring
# `triggering auto-update` did not match it. A node that took the urgent path
# was reported as never having decided to update. It failed CLOSED (Gate B
# refuses rather than passes), so nothing broke visibly -- which is precisely
# why an enumeration that had been wrong since the urgent path was added went
# unnoticed. `auto-update-canary_test.sh` now pins the COUNT at five, so a
# sixth site cannot be added silently.
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

MUSL_ASSET='freenet-x86_64-unknown-linux-musl.tar.gz'
RELEASE_BASE='https://github.com/freenet/freenet-core/releases/download'

# Ports deliberately off the defaults (31337 / 7509) so a canary run never
# collides with a real node on the same host.
CANARY_NETWORK_PORT="${CANARY_NETWORK_PORT:-39337}"
CANARY_WS_PORT="${CANARY_WS_PORT:-39509}"

# How long to let the node run before giving up on the startup check. The
# check fires after a 0-60s anti-thundering-herd jitter, so this must clear
# 60s by a healthy margin; it is a ceiling, not a wait (both gates return as
# soon as they have their answer, typically ~40s).
CANARY_TIMEOUT_SECS="${CANARY_TIMEOUT_SECS:-240}"
# Validated like its two neighbours below. Without this a non-numeric override
# reaches the `$((...))` in the lifecycle guard and, under `set -u`, kills the
# canary with a shell arithmetic error rather than a canary verdict -- a
# release-blocking failure whose message says nothing about the release.
case "$CANARY_TIMEOUT_SECS" in
  ''|*[!0-9]*|0) CANARY_TIMEOUT_SECS=240 ;;
esac

# Retry budget for the INDETERMINATE (GitHub unreachable) case only. Kept
# small on purpose: this sits on the release critical path inside a job with a
# fixed timeout, and an over-generous retry budget turns a network blip into a
# cancelled job and a permanently stuck draft -- worse than the failure it was
# trying to ride out.
CANARY_ATTEMPTS="${CANARY_ATTEMPTS:-2}"
# A non-numeric or zero override would make the retry loop body never execute
# and the script report "could not reach GitHub in 0 attempts" -- a blocking
# failure backed by no attempt at all.
case "$CANARY_ATTEMPTS" in
  ''|*[!0-9]*|0) CANARY_ATTEMPTS=2 ;;
esac
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
CANARY_OUTCOME_WAIT_SECS="${CANARY_OUTCOME_WAIT_SECS:-20}"
case "$CANARY_OUTCOME_WAIT_SECS" in
  ''|*[!0-9]*|0) CANARY_OUTCOME_WAIT_SECS=20 ;;
esac

log()  { printf '%s\n' "$*"; }
fail() { printf '::error::%s\n' "$*" >&2; }
# An UNVERIFIED result, not a detected fault. Deliberately not `::error::`:
# annotating an unreachable GitHub as a workflow error trains people to ignore
# the annotation, and the caller decides whether to retry or fail.
note() { printf '%s\n' "$*" >&2; }

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
  local logs
  # `grep -a` everywhere below: the node writes some non-UTF8 bytes, and
  # without it grep calls the file binary and prints nothing -- which would
  # silently satisfy every NEGATIVE check. Exactly the vacuous-pass shape this
  # canary exists to prevent.
  logs="$(cat "$logdir"/freenet.*.log 2>/dev/null)"

  if [ -z "$logs" ]; then
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
  if printf '%s' "$logs" | grep -aqF "$MARKER_DISABLED"; then
    fail "auto-update is DISABLED on the canary node. The canary cannot test the updater while the updater is turned off -- this is the #5040 drop-in failure mode that hid #5221 for two releases."
    printf '%s' "$logs" | grep -aF "$MARKER_DISABLED" | head -2 >&2
    return 1
  fi

  # (+) POSITIVE side. Without this, every assertion below passes vacuously on
  #     a node that never checked for updates.
  if ! printf '%s' "$logs" | grep -aqF "$MARKER_CHECK_RAN"; then
    fail "the startup update check never ran: no '$MARKER_CHECK_RAN' line. Absence of a parse error here proves NOTHING -- the check did not happen."
    return 1
  fi

  # (-) NEGATIVE side: the #5221 signature.
  if printf '%s' "$logs" | grep -aqF "$MARKER_PARSE_FAIL"; then
    fail "the node could not parse the version GitHub returned -- auto-update is BROKEN. This is the #5221 regression: the release tag reached the detection path without being normalised."
    printf '%s' "$logs" | grep -aF "$MARKER_PARSE_FAIL" | head -2 >&2
    return 1
  fi

  # Infrastructure, not a product bug: GitHub was unreachable or rate-limited,
  # so the check ran but learned nothing. Distinct exit code so the caller can
  # retry instead of failing a release on a transient network blip.
  if printf '%s' "$logs" | grep -aqF "$MARKER_FETCH_FAIL"; then
    note "INDETERMINATE: could not reach GitHub to fetch the latest version."
    printf '%s' "$logs" | grep -aF "$MARKER_FETCH_FAIL" | head -2 >&2
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
    seen_line="$(printf '%s' "$logs" | grep -aF "$MARKER_LATEST_SEEN" | tail -1)"
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
  printf '%s' "$logs" | grep -aF "$MARKER_CHECK_RAN" | head -2
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
  mkdir -p "$work/home/.local/state/freenet" "$work/cfg" "$work/data" "$work/logs"

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
        local settle=0
        while kill -0 "$node_pid" 2>/dev/null && [ "$settle" -lt 60 ]; do
          sleep 2
          settle=$((settle + 2))
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

resolve_expected_latest() {
  local url tag
  url="$(curl -fsS --max-time 30 -o /dev/null -w '%{redirect_url}' \
    'https://github.com/freenet/freenet-core/releases/latest' 2>/dev/null)" || return 1
  case "$url" in
    */releases/tag/*) tag="${url##*/releases/tag/}" ;;
    *) return 1 ;;
  esac
  [ -n "$tag" ] || return 1
  normalise_release_tag "$tag"
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
  "$binary" --version

  # Resolve what the node SHOULD see before booting it, so the log assertion can
  # be a positive equality rather than an absence-of-error. Failing to resolve
  # it is INDETERMINATE, never a pass: without it the gate silently weakens to
  # "the node did not complain", which is what a silently-wrong comparator
  # produces. Returning 1 here (not 2) because the retry loop below re-runs the
  # whole attempt for rc=2, and a resolution failure is not something a node
  # re-run fixes -- it is an infrastructure problem the operator must see.
  if ! CANARY_EXPECTED_LATEST="$(resolve_expected_latest)"; then
    fail "could not resolve GitHub's latest release tag, so the canary cannot check WHICH release the node compared against. This is an UNVERIFIED result, not a detected bug: re-run this job. Do NOT un-draft the release by hand -- an unverified gate is not a passed gate."
    return 1
  fi
  export CANARY_EXPECTED_LATEST
  log "GitHub's latest published release is '$CANARY_EXPECTED_LATEST'; the node must compare against exactly that."

  # Retry only the INDETERMINATE case. A parse failure is deterministic and
  # retrying it just burns release time; a GitHub blip is worth a second look
  # before we stall a release on it.
  local attempt rc
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
    assert_detection_healthy "$work/logs"
    rc=$?
    [ "$rc" -eq 2 ] || return "$rc"
    if [ "$attempt" -lt "$CANARY_ATTEMPTS" ]; then
      log "indeterminate (no verdict from the update check); retrying in ${CANARY_RETRY_SLEEP}s"
      sleep "$CANARY_RETRY_SLEEP"
    fi
  done

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

  run_node_until_check "$work/bin/freenet" "$work"

  # The two-sided log assertion first: it LOCALISES the failure. If detection
  # is broken the version check below would also fail, but with a far less
  # useful message.
  assert_detection_healthy "$work/logs"
  local rc=$?
  if [ "$rc" -eq 2 ]; then
    # Infrastructure, not a stranded fleet. Still a failure -- reporting green
    # on an unverified run is the vacuous-pass this canary exists to prevent --
    # but worded so nobody reads it as "the fleet is broken" and learns to
    # ignore the alarm.
    fail "UNVERIFIED: the update check produced no verdict (GitHub unreachable, or it never logged an outcome), so the canary could not determine whether v$prev_version reaches v$expected_version. This is NOT evidence that auto-update is broken, and NOT evidence that it works. Re-run the job."
    return 1
  fi
  [ "$rc" -eq 0 ] || return 1

  if ! node_decided_to_update "$work/logs"; then
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
