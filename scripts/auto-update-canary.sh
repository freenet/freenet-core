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
# So `assert_detection_healthy` requires BOTH:
#   (+) the "Startup update check against GitHub" INFO line is PRESENT
#       -- proves the check actually ran
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
# auto_update.rs  -- the #5221 regression signature
MARKER_PARSE_FAIL='failed to parse latest version'
# auto_update.rs  -- GitHub unreachable / rate-limited: infrastructure, not a bug
MARKER_FETCH_FAIL='failed to fetch latest version'
# freenet.rs      -- either --disable-auto-update or a dirty build
MARKER_DISABLED='Auto-update is DISABLED'
# freenet.rs      -- detection succeeded and an update was requested.
# Anchored on the full positive phrase, NOT on "triggering auto-update": the
# #4073 rollback path logs "...not triggering auto-update", and a substring
# match on the short form treats a node that deliberately REFUSED an update as
# one that requested it.
MARKER_TRIGGERED='newer version on GitHub, triggering auto-update'

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
CANARY_TIMEOUT_SECS="${CANARY_TIMEOUT_SECS:-180}"

# Retry budget for the INDETERMINATE (GitHub unreachable) case only. Kept
# small on purpose: this sits on the release critical path inside a job with a
# fixed timeout, and an over-generous retry budget turns a network blip into a
# cancelled job and a permanently stuck draft -- worse than the failure it was
# trying to ride out.
CANARY_ATTEMPTS="${CANARY_ATTEMPTS:-2}"
CANARY_RETRY_SLEEP="${CANARY_RETRY_SLEEP:-20}"

log()  { printf '%s\n' "$*"; }
fail() { printf '::error::%s\n' "$*" >&2; }

# One workdir for the whole run, cleaned by a single EXIT trap.
#
# This was originally a `local` in each gate with a `trap ... RETURN`. Under
# `set -u` the trap fired after the local had gone out of scope, so EVERY gate
# exited 1 -- including a healthy binary. A canary that reports failure on
# success is worse than no canary: the first person to hit it learns to
# override it, and then it never catches anything real.
CANARY_WORKDIR="$(mktemp -d)"
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
    log "INDETERMINATE: could not reach GitHub to fetch the latest version."
    printf '%s' "$logs" | grep -aF "$MARKER_FETCH_FAIL" | head -2
    return 2
  fi

  log "OK: startup update check ran and parsed GitHub's response."
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
  (
    # shellcheck disable=SC2030  # scoping HOME to this subshell is the point:
    # the node keeps its GitHub poll bucket under $HOME, and the caller's HOME
    # must not be touched on a machine that is already running a node.
    export HOME="$work/home"
    # Tell the node a supervisor is present, exactly as the systemd unit does,
    # so it takes the real exit-42 path rather than logging a "no supervisor"
    # error and staying put.
    export FREENET_SUPERVISED=1
    timeout "$CANARY_TIMEOUT_SECS" "$binary" network \
      --config-dir "$work/cfg" \
      --data-dir "$work/data" \
      --log-dir "$work/logs" \
      --network-port "$CANARY_NETWORK_PORT" \
      --ws-api-port "$CANARY_WS_PORT" \
      >"$work/node.out" 2>&1
  ) &
  local node_pid=$!

  # Poll for a verdict rather than sleeping the full timeout: the check fires
  # after a 0-60s jitter, so this normally returns in well under a minute and
  # adds no meaningful time to a release.
  local waited=0
  while [ "$waited" -lt "$CANARY_TIMEOUT_SECS" ]; do
    if ! kill -0 "$node_pid" 2>/dev/null; then
      break   # node exited on its own (exit 42 on the selfupdate path)
    fi
    if grep -aqF "$MARKER_CHECK_RAN" "$work/logs"/freenet.*.log 2>/dev/null; then
      # The check ran. Give it a moment to log the OUTCOME (parse failure,
      # trigger, or fetch failure) before we read the verdict.
      sleep 5
      # If the node decided to update, it exits 42 on its own. Killing it here
      # would replace that with 143 and silently defeat Gate B's exit-42
      # assertion -- the canary would report "no update requested" for a node
      # that requested one. Let it finish.
      if grep -aqF "$MARKER_TRIGGERED" "$work/logs"/freenet.*.log 2>/dev/null; then
        local settle=0
        while kill -0 "$node_pid" 2>/dev/null && [ "$settle" -lt 60 ]; do
          sleep 2
          settle=$((settle + 2))
        done
      fi
      break
    fi
    sleep 3
    waited=$((waited + 3))
  done

  # Stop the node if it is still up, then reap it for its exit code.
  kill "$node_pid" 2>/dev/null
  wait "$node_pid"
  NODE_EXIT=$?
  log "node exited with code $NODE_EXIT"
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
    run_node_until_check "$binary" "$work"
    assert_detection_healthy "$work/logs"
    rc=$?
    [ "$rc" -eq 2 ] || return "$rc"
    if [ "$attempt" -lt "$CANARY_ATTEMPTS" ]; then
      log "indeterminate (GitHub unreachable); retrying in ${CANARY_RETRY_SLEEP}s"
      sleep "$CANARY_RETRY_SLEEP"
    fi
  done

  fail "could not reach GitHub in $CANARY_ATTEMPTS attempts -- cannot confirm the shipping binary's updater works. This is an UNVERIFIED result, not a detected bug: re-run this job once GitHub is reachable. Do NOT un-draft the release by hand to work around it."
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
    fail "UNVERIFIED: GitHub was unreachable, so the canary could not determine whether v$prev_version reaches v$expected_version. This is NOT evidence that auto-update is broken, and NOT evidence that it works. Re-run the job."
    return 1
  fi
  [ "$rc" -eq 0 ] || return 1

  if ! grep -ahqF "$MARKER_TRIGGERED" "$work/logs"/freenet.*.log 2>/dev/null; then
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
