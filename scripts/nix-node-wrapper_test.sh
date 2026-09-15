#!/usr/bin/env bash
#
# Self-test for nix/freenet-node.sh, the supervisor the Nix `freenet-node`
# package runs.
#
# WHY THIS EXISTS
# ---------------
# The whole point of the Nix deployment path is that a peer AUTO-UPDATES: a peer
# that does not is a liability for the network. The supervisor is what makes
# that true -- the node exits 42 and relies on something to run `freenet update`
# and restart it. Nothing else in the tree checks that this particular
# something does.
#
# It is deliberately NOT a nix test. `nix build .#freenet-node` compiles the
# whole dependency graph first, so it is the wrong place to learn that an exit
# code is handled backwards; and `writeShellApplication` only ever lints the
# script, it never runs it. This drives the real file with a fake `freenet`.
#
# THE ASSERTIONS ARE TWO-SIDED ON PURPOSE. "The updater ran" is worth little
# without "the updater did NOT run for a clean exit": an updater invoked on
# every stop scores an operator's `systemctl stop` as a probation crash and can
# roll a healthy release back (#5227). So every case asserts both what must
# happen and what must not.
#
# AND THEY ASSERT *WHICH BINARY* RAN, not merely that something did. The seed
# (/nix/store, read-only) and the state-dir binary are otherwise
# indistinguishable, and running either `network` or `update` from the store
# path is the single worst failure this package has: `current_exe()` lands in a
# read-only filesystem, every `freenet update` fails with EROFS, and after
# MAX_UPDATE_FAILURES the node stops exiting 42 at all -- a silently, and
# permanently, stale peer. The fake therefore logs `$0`, and the cases below
# pin it. Three mutations of the wrapper used to pass this suite 29/29:
# running `network` from the seed, running `update` from the seed, and seeding
# by symlink instead of by copy. Re-apply any of them and this file must go red.
set -euo pipefail

REPO_ROOT="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
WRAPPER="$REPO_ROOT/nix/freenet-node.sh"

PASS=0
FAIL=0

ok() {
  PASS=$((PASS + 1))
  echo "ok   - $1"
}

bad() {
  FAIL=$((FAIL + 1))
  echo "FAIL - $1" >&2
}

assert_eq() {
  if [ "$1" = "$2" ]; then
    ok "$3"
  else
    bad "$3 (expected '$2', got '$1')"
  fi
}

assert_contains() {
  # Bash glob, not a pipe into `grep -q`: under `pipefail` the producer takes
  # SIGPIPE when grep short-circuits and a PRESENT marker reads as absent.
  # See .claude/rules/bug-prevention-patterns.md.
  if [[ "$1" == *"$2"* ]]; then
    ok "$3"
  else
    bad "$3 (missing '$2' in: $1)"
  fi
}

assert_not_contains() {
  if [[ "$1" != *"$2"* ]]; then
    ok "$3"
  else
    bad "$3 (unexpectedly found '$2' in: $1)"
  fi
}

# ---------------------------------------------------------------------------
# A fake `freenet`: records how it was called (and AS WHAT PATH), reports a
# version baked in at write time, and exits with the Nth code from a plan file
# on its Nth `network` invocation.
# ---------------------------------------------------------------------------
write_fake_freenet() {
  local path="$1" version="${2:-0.2.100}"
  # First heredoc unquoted so the version is baked in; the body quoted so
  # nothing else expands at write time.
  cat >"$path" <<FAKE
#!/usr/bin/env bash
set -euo pipefail
FAKE_VERSION='$version'
FAKE
  cat >>"$path" <<'FAKE'
case "${1:-}" in
  --version)
    printf 'Freenet version: %s (abc1234)\n' "$FAKE_VERSION"
    printf 'Build timestamp: 1970-01-01T00:00:00Z\n'
    exit 0
    ;;
  network)
    shift
    n=0
    [ -f "$FAKE_COUNT" ] && n="$(cat "$FAKE_COUNT")"
    n=$((n + 1))
    printf '%s' "$n" >"$FAKE_COUNT"
    # `self=$0` is the load-bearing field: it is the ONLY way this suite can
    # tell a node running from the writable state dir from one running out of
    # the read-only /nix/store seed.
    printf 'network|args=%s|supervised=%s|self=%s\n' "$*" "${FREENET_SUPERVISED:-<unset>}" "$0" >>"$FAKE_LOG"
    code="$(sed -n "${n}p" "$FAKE_PLAN")"
    [ -n "$code" ] || code=0
    exit "$code"
    ;;
  update)
    shift
    printf 'update|args=%s|poststop=%s|self=%s\n' "$*" "${FREENET_POST_STOP_EXIT_CODE:-<unset>}" "$0" >>"$FAKE_LOG"
    if [ -n "${FAKE_UPDATE_SLEEP:-}" ]; then
      # `exec` so a TERM from `timeout` lands on the sleep itself: bash defers
      # a signal until a FOREGROUND child finishes, which would make this hang
      # test measure bash's deferral rather than the wrapper's timeout.
      exec sleep "$FAKE_UPDATE_SLEEP"
    fi
    exit 0
    ;;
  *)
    printf 'unexpected|args=%s\n' "$*" >>"$FAKE_LOG"
    exit 64
    ;;
esac
FAKE
  chmod +x "$path"
}

# Optional knobs for run_wrapper, reset after every call.
WRAP_SEED_VERSION=""
WRAP_STATE_VERSION=""
WRAP_RESTART_SECS=""
WRAP_MAX_SECS=""
WRAP_UPDATER_TIMEOUT=""
WRAP_UPDATE_SLEEP=""

# run_wrapper <plan-as-newline-separated-exit-codes> [wrapper args...]
# Sets LOG, STDOUT, RC, WORK, STATE and SEED for the caller.
run_wrapper() {
  local plan="$1"
  shift
  WORK="$(mktemp -d)"
  STATE="$WORK/state"
  SEED="$WORK/seed-freenet"
  write_fake_freenet "$SEED" "${WRAP_SEED_VERSION:-0.2.100}"
  if [ -n "$WRAP_STATE_VERSION" ]; then
    mkdir -p "$STATE/bin"
    write_fake_freenet "$STATE/bin/freenet" "$WRAP_STATE_VERSION"
  fi
  printf '%s\n' "$plan" >"$WORK/plan"
  : >"$WORK/log"
  RC=0
  env -u XDG_STATE_HOME \
    FAKE_LOG="$WORK/log" \
    FAKE_PLAN="$WORK/plan" \
    FAKE_COUNT="$WORK/count" \
    FAKE_UPDATE_SLEEP="$WRAP_UPDATE_SLEEP" \
    FREENET_NIX_SEED_BINARY="$SEED" \
    STATE_DIRECTORY="$STATE" \
    FREENET_NODE_RESTART_SECS="${WRAP_RESTART_SECS:-0}" \
    FREENET_NODE_RESTART_MAX_SECS="${WRAP_MAX_SECS:-300}" \
    FREENET_NODE_UPDATER_TIMEOUT_SECS="${WRAP_UPDATER_TIMEOUT:-600}" \
    bash "$WRAPPER" "$@" >"$WORK/stdout" 2>&1 || RC=$?
  LOG="$(cat "$WORK/log")"
  STDOUT="$(cat "$WORK/stdout")"
  WRAP_SEED_VERSION=""
  WRAP_STATE_VERSION=""
  WRAP_RESTART_SECS=""
  WRAP_MAX_SECS=""
  WRAP_UPDATER_TIMEOUT=""
  WRAP_UPDATE_SLEEP=""
}

starts_of() {
  # Count of `network` lines in $LOG, without a status-consuming pipe.
  local n
  n="$(grep -c '^network|' "$WORK/log" || true)"
  printf '%s' "$n"
}

self_of() {
  # The `self=` field of the first "$1|" line: WHICH binary actually ran.
  # `grep -m1` rather than a pipe into `head`, for the SIGPIPE reason above.
  local line
  line="$(grep -m1 "^$1|" "$WORK/log" || true)"
  case "$line" in
    *"|self="*) printf '%s' "${line##*|self=}" ;;
    *) printf '<no %s line>' "$1" ;;
  esac
}

is_symlink() {
  if [ -L "$1" ]; then printf 'symlink'; else printf 'regular'; fi
}

same_bytes() {
  if cmp -s "$1" "$2"; then printf 'same'; else printf 'differ'; fi
}

echo "== nix/freenet-node.sh supervisor =="

# ---------------------------------------------------------------------------
# 1. Exit 42 -> `freenet update` runs -> the node is restarted.
#    This is the auto-update contract. If only one case in this file can be
#    trusted, it is this one.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '42\n0')"
assert_eq "$RC" "0" "exit 42 then a clean exit leaves the wrapper exiting 0"
assert_eq "$(starts_of)" "2" "exit 42 RESTARTS the node (two starts)"
assert_contains "$LOG" "update|args=--quiet" "exit 42 runs 'freenet update --quiet'"
assert_contains "$LOG" "poststop=42" \
  "the node's exit status is forwarded to the updater, so crash-loop rollback can classify it (#4073)"

# The two assertions the suite was missing entirely. Run either of these from
# $FREENET_NIX_SEED_BINARY and the peer can never update again (EROFS on
# /nix/store, then the MAX_UPDATE_FAILURES lockout), silently and forever.
assert_eq "$(self_of network)" "$STATE/bin/freenet" \
  "the NODE runs from the writable state dir, never from the read-only /nix/store seed"
assert_eq "$(self_of update)" "$STATE/bin/freenet" \
  "the UPDATER runs from the writable state dir, so 'current_exe()' is renameable (no EROFS)"

# ---------------------------------------------------------------------------
# 2. The supervised marker, and argument forwarding.
# ---------------------------------------------------------------------------
run_wrapper "0" --config-dir /tmp/nowhere --ws-api-port 1234
assert_contains "$LOG" "supervised=1" \
  "FREENET_SUPERVISED=1 is set on the node, so it reports the exit-42 path calmly (#4580)"
assert_contains "$LOG" "args=--config-dir /tmp/nowhere --ws-api-port 1234" \
  "wrapper arguments are forwarded to 'freenet network'"

# ---------------------------------------------------------------------------
# 3. Exit 0: a clean shutdown is not a crash and is not an update.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '0\n0')"
assert_eq "$RC" "0" "a clean exit 0 propagates as 0"
assert_eq "$(starts_of)" "1" "exit 0 does NOT restart the node"
assert_not_contains "$LOG" "update|" "exit 0 does NOT run the updater"

# ---------------------------------------------------------------------------
# 4. Exit 43 (another instance already holds the port): stand down and let the
#    service manager decide when to retry. RestartPreventExitStatus=43 in the
#    systemd unit; restarting immediately would only lose the same race again,
#    and running the updater would treat another peer's port as a fault.
#
#    This is NOT a claim that the holder is healthy -- see the #3967 KNOWN
#    DIVERGENCE in the wrapper header. It is why docs/nix.md specifies
#    `Restart = "always"`: a stood-down wrapper must be retried, not left dead.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '43\n0')"
assert_eq "$RC" "0" "exit 43 is reported as success (SuccessExitStatus=42 43)"
assert_eq "$(starts_of)" "1" "exit 43 does NOT restart the node in-process"
assert_not_contains "$LOG" "update|" "exit 43 does NOT run the updater"

# ---------------------------------------------------------------------------
# 5. A signal aimed at the NODE ALONE is not a stop of the service, so it must
#    restart and self-heal like any other unexpected death.
#
#    The wrapper cannot reach this path when the SERVICE was asked to stop: a
#    `systemctl stop` signals the whole control group and Ctrl-C the whole
#    foreground process group, so the wrapper's own TERM/INT/HUP trap runs and
#    it exits 0 first. Reaching here therefore means something signalled the
#    leaf -- `pkill freenet network`, `systemctl kill --kill-who=main`, a
#    container runtime -- which systemd restarts (Restart=always) and whose
#    ExecStopPost hook still runs, because the unit's #5227 guard fires only
#    for $SERVICE_RESULT="success" + $EXIT_CODE="killed", i.e. only for a stop
#    job. Standing down here left the peer dead with nothing to revive it.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '143\n0')"
assert_eq "$RC" "0" "a SIGTERM-shaped node status eventually exits cleanly"
assert_eq "$(starts_of)" "2" \
  "a signal aimed at the node alone RESTARTS it -- the peer is not stood down permanently"
assert_contains "$LOG" "poststop=143" \
  "a signalled node still runs the updater, as the unit's ExecStopPost does for any status but 0/43"

# The genuine service stop still works, via the trap rather than this arm.
# Covered by case 15 below, which SIGTERMs the wrapper itself.

# ---------------------------------------------------------------------------
# 6. A genuine crash runs the updater (self-heal forward, or roll back) and
#    restarts.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '101\n0')"
assert_eq "$(starts_of)" "2" "a panic (exit 101) restarts the node"
assert_contains "$LOG" "poststop=101" \
  "a crash forwards its status to the updater so a probationary version can be rolled back (#4073)"

# ---------------------------------------------------------------------------
# 7. Crash-loop limiter (StartLimitBurst=5 / StartLimitIntervalSec=120, #4551):
#    a no-fix tight loop stops loudly instead of restarting forever.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '101\n101\n101\n101\n101\n101\n101\n101\n0')"
assert_eq "$RC" "1" "a tight crash loop stops the wrapper with a failure status"
assert_eq "$(starts_of)" "6" "the crash loop stops after burst+1 starts, not forever"
assert_contains "$STDOUT" "failed starts within" "the crash-loop stop says why"

# ---------------------------------------------------------------------------
# 8. ...but an UPDATE loop is not a crash loop. Exit 42 is a clean exit
#    (SuccessExitStatus=42) and must not consume the burst budget, or a peer
#    stepping through several releases would take itself offline.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '42\n42\n42\n42\n42\n42\n42\n42\n0')"
assert_eq "$RC" "0" "eight consecutive updates do NOT trip the crash-loop limiter"
assert_eq "$(starts_of)" "9" "the node is restarted after every update"

# ---------------------------------------------------------------------------
# 9. Bootstrap: seed once, as a REGULAR FILE, then never overwrite from a store
#    binary that is not newer. Re-seeding on every start would silently pin the
#    node to the flake's version, which is the exact failure this design exists
#    to avoid.
# ---------------------------------------------------------------------------
run_wrapper "0"
assert_eq "$([ -x "$STATE/bin/freenet" ] && echo yes || echo no)" "yes" \
  "the first run seeds an executable binary into the state directory"
assert_contains "$STDOUT" "seeded" "the first run says it seeded the binary"
# `-x` alone is satisfied by a symlink into /nix/store, which is read-only and
# therefore un-renameable-over: the node could never update itself again.
assert_eq "$(is_symlink "$STATE/bin/freenet")" "regular" \
  "the seeded binary is a REGULAR FILE, never a symlink into the read-only store"
assert_eq "$(same_bytes "$SEED" "$STATE/bin/freenet")" "same" \
  "the seeded binary is a faithful byte-for-byte copy of the store seed"
assert_eq "$(self_of network)" "$STATE/bin/freenet" \
  "the freshly-seeded node is launched from the state dir, not the seed path"

# Second run against a state dir that already holds a DIFFERENT binary of the
# same version: the wrapper must run that one and leave it alone.
WORK2="$(mktemp -d)"
STATE2="$WORK2/state"
mkdir -p "$STATE2/bin"
write_fake_freenet "$STATE2/bin/freenet"
printf '# ALREADY-UPDATED\n' >>"$STATE2/bin/freenet"
BEFORE="$(cksum <"$STATE2/bin/freenet")"
SEED2="$WORK2/seed-freenet"
write_fake_freenet "$SEED2"
printf '0\n' >"$WORK2/plan"
: >"$WORK2/log"
RC2=0
env -u XDG_STATE_HOME \
  FAKE_LOG="$WORK2/log" \
  FAKE_PLAN="$WORK2/plan" \
  FAKE_COUNT="$WORK2/count" \
  FREENET_NIX_SEED_BINARY="$SEED2" \
  STATE_DIRECTORY="$STATE2" \
  FREENET_NODE_RESTART_SECS=0 \
  bash "$WRAPPER" >"$WORK2/stdout" 2>&1 || RC2=$?
AFTER="$(cksum <"$STATE2/bin/freenet")"
assert_eq "$RC2" "0" "a pre-existing state binary runs normally"
assert_eq "$AFTER" "$BEFORE" \
  "an EXISTING state-dir binary is never overwritten by a store seed that is not newer"
assert_not_contains "$(cat "$WORK2/stdout")" "seeded" "a second run does not re-seed"

# ---------------------------------------------------------------------------
# 10. State-directory resolution falls back to XDG when systemd did not set one.
# ---------------------------------------------------------------------------
WORK3="$(mktemp -d)"
SEED3="$WORK3/seed-freenet"
write_fake_freenet "$SEED3"
printf '0\n' >"$WORK3/plan"
: >"$WORK3/log"
RC3=0
env -u STATE_DIRECTORY \
  FAKE_LOG="$WORK3/log" \
  FAKE_PLAN="$WORK3/plan" \
  FAKE_COUNT="$WORK3/count" \
  FREENET_NIX_SEED_BINARY="$SEED3" \
  XDG_STATE_HOME="$WORK3/xdg" \
  FREENET_NODE_RESTART_SECS=0 \
  bash "$WRAPPER" >"$WORK3/stdout" 2>&1 || RC3=$?
assert_eq "$RC3" "0" "the XDG fallback runs the node"
assert_eq "$([ -x "$WORK3/xdg/freenet/bin/freenet" ] && echo yes || echo no)" "yes" \
  "with no STATE_DIRECTORY the binary is seeded under \$XDG_STATE_HOME/freenet/bin"

# ---------------------------------------------------------------------------
# 11. A PARTIAL seed is repaired, not enshrined.
#
#     `install` writes the destination in place, so an OOM kill, a `systemctl
#     stop`, a reboot or ENOSPC part-way through the first copy leaves a
#     truncated file at the binary path -- mode 0600, because GNU install
#     applies the mode last. A bare `[ ! -e ]` gate then treats the peer as
#     seeded forever: it can never start and can never re-seed. Permanently
#     dead, human required.
# ---------------------------------------------------------------------------
WORK4="$(mktemp -d)"
STATE4="$WORK4/state"
mkdir -p "$STATE4/bin"
printf '#!/usr/bin/env bash\n# truncated mid-cop' >"$STATE4/bin/freenet"
chmod 0600 "$STATE4/bin/freenet"
SEED4="$WORK4/seed-freenet"
write_fake_freenet "$SEED4"
printf '0\n' >"$WORK4/plan"
: >"$WORK4/log"
RC4=0
env -u XDG_STATE_HOME \
  FAKE_LOG="$WORK4/log" \
  FAKE_PLAN="$WORK4/plan" \
  FAKE_COUNT="$WORK4/count" \
  FREENET_NIX_SEED_BINARY="$SEED4" \
  STATE_DIRECTORY="$STATE4" \
  FREENET_NODE_RESTART_SECS=0 \
  bash "$WRAPPER" >"$WORK4/stdout" 2>&1 || RC4=$?
assert_eq "$RC4" "0" "a truncated half-written binary is repaired and the node starts"
assert_contains "$(cat "$WORK4/stdout")" "partially-written" \
  "the repair says what it found, so a partial seed is not silent"
assert_eq "$(same_bytes "$SEED4" "$STATE4/bin/freenet")" "same" \
  "the repaired binary is the full seed, not the truncated remains"
assert_contains "$(cat "$WORK4/log")" "network|" "the repaired peer actually runs"

# The seed itself must be atomic, so the wreckage above cannot be created in
# the first place: the destination is written under a temp name in the same
# directory and renamed into place. No stray temp file may survive.
assert_eq "$(find "$STATE4/bin" -name '.freenet.seed.*' | wc -l)" "0" \
  "seeding leaves no temp file behind (it is renamed into place, never written in place)"

# ...and the atomicity itself, by making the copy genuinely fail part-way.
# `ulimit -f` makes the write past the limit raise SIGXFSZ, which is the same
# shape as the OOM kill / reboot / ENOSPC that produced the wreckage above:
# install(1) dies mid-copy having already created the destination. The property
# is that $binary is ABSENT OR COMPLETE, never truncated -- which is true only
# if the copy lands on a temp name and is renamed into place.
WORK4B="$(mktemp -d)"
STATE4B="$WORK4B/state"
SEED4B="$WORK4B/seed-freenet"
write_fake_freenet "$SEED4B"
printf '0\n' >"$WORK4B/plan"
: >"$WORK4B/log"
(
  ulimit -c 0
  ulimit -f 1
  env -u XDG_STATE_HOME \
    FAKE_LOG="$WORK4B/log" \
    FAKE_PLAN="$WORK4B/plan" \
    FAKE_COUNT="$WORK4B/count" \
    FREENET_NIX_SEED_BINARY="$SEED4B" \
    STATE_DIRECTORY="$STATE4B" \
    FREENET_NODE_RESTART_SECS=0 \
    bash "$WRAPPER" >/dev/null 2>&1
) || true
assert_eq "$([ -e "$STATE4B/bin/freenet" ] && echo present || echo absent)" "absent" \
  "a seed that dies mid-copy leaves NO binary at all, never a truncated one: the copy goes to a temp name and is renamed into place"

# ---------------------------------------------------------------------------
# 12. A symlink at the binary path is replaced, not run.
#
#     This wrapper never creates one, but a symlink into /nix/store is the
#     precise shape that makes `current_exe()` read-only and every future
#     update fail with EROFS, so it is repaired rather than inherited.
# ---------------------------------------------------------------------------
WORK5="$(mktemp -d)"
STATE5="$WORK5/state"
mkdir -p "$STATE5/bin"
SEED5="$WORK5/seed-freenet"
write_fake_freenet "$SEED5"
ln -s "$SEED5" "$STATE5/bin/freenet"
printf '0\n' >"$WORK5/plan"
: >"$WORK5/log"
RC5=0
env -u XDG_STATE_HOME \
  FAKE_LOG="$WORK5/log" \
  FAKE_PLAN="$WORK5/plan" \
  FAKE_COUNT="$WORK5/count" \
  FREENET_NIX_SEED_BINARY="$SEED5" \
  STATE_DIRECTORY="$STATE5" \
  FREENET_NODE_RESTART_SECS=0 \
  bash "$WRAPPER" >"$WORK5/stdout" 2>&1 || RC5=$?
assert_eq "$RC5" "0" "a symlinked binary path still ends with a running node"
assert_eq "$(is_symlink "$STATE5/bin/freenet")" "regular" \
  "a symlink at the binary path is replaced by a real copy the updater can rename over"

# ---------------------------------------------------------------------------
# 13. The only escape from a state binary that can no longer update itself.
#
#     A `-dirty` build never auto-updates (that is what the dirty flag means),
#     and a binary that has hit MAX_UPDATE_FAILURES has stopped trying. Either
#     way the node never exits 42 again, so nothing else in this design can move
#     it forward: without this, `nix run` in a dirty checkout seeds a peer that
#     is stale forever. Re-seed only when the store binary is STRICTLY NEWER,
#     which keeps "never pin backwards" intact.
# ---------------------------------------------------------------------------
WRAP_STATE_VERSION="0.2.100" WRAP_SEED_VERSION="0.2.135" run_wrapper "0"
assert_contains "$STDOUT" "is newer than the state binary" \
  "a STRICTLY NEWER store binary re-seeds, so a locked-out or dirty peer can still move forward"
assert_eq "$(same_bytes "$SEED" "$STATE/bin/freenet")" "same" \
  "the re-seed actually replaced the state binary"

WRAP_STATE_VERSION="0.2.135" WRAP_SEED_VERSION="0.2.100" run_wrapper "0"
assert_not_contains "$STDOUT" "seeded" \
  "an OLDER store binary never re-seeds -- the node is never pinned backwards to the flake's version"

WRAP_STATE_VERSION="0.2.100" WRAP_SEED_VERSION="0.2.100" run_wrapper "0"
assert_not_contains "$STDOUT" "seeded" "an EQUAL store binary never re-seeds"

# 0.2.9 vs 0.2.10 is the case a lexical comparison gets backwards.
WRAP_STATE_VERSION="0.2.9" WRAP_SEED_VERSION="0.2.10" run_wrapper "0"
assert_contains "$STDOUT" "is newer than the state binary" \
  "version comparison is numeric, not lexical (0.2.10 is newer than 0.2.9)"

WRAP_STATE_VERSION="0.2.10" WRAP_SEED_VERSION="0.2.9" run_wrapper "0"
assert_not_contains "$STDOUT" "seeded" \
  "and numeric in the other direction too (0.2.9 is NOT newer than 0.2.10)"

# An unreadable version must mean "do nothing", never "re-seed anyway".
WRAP_STATE_VERSION="not-a-version" WRAP_SEED_VERSION="0.2.135" run_wrapper "0"
assert_not_contains "$STDOUT" "seeded" \
  "an unparseable state version is never treated as older -- parse failure means do not act"

# ---------------------------------------------------------------------------
# 14. Restart backoff GROWS and is CAPPED (RestartSteps / RestartMaxDelaySec).
#
#     Exit 42 is burst-exempt -- correctly, an update chain must not take a peer
#     offline -- and is ALSO FATAL_LISTENER_EXIT_CODE (node/p2p_impl.rs). So a
#     node whose listener keeps failing exits 42 on every boot, is never
#     counted by the limiter, and without a growing delay reconnects to the
#     gateways every RestartSec forever. That is exactly what the unit's own
#     comment says the growing delay exists to stop.
# ---------------------------------------------------------------------------
WRAP_RESTART_SECS=1 WRAP_MAX_SECS=2 run_wrapper "$(printf '42\n42\n42\n0')"
assert_eq "$RC" "0" "the backoff run completes"
assert_contains "$STDOUT" "attempt 1, base 1s" "the first restart waits RestartSec"
assert_contains "$STDOUT" "attempt 2, base 2s" "the delay GROWS on a repeat that made no progress"
assert_contains "$STDOUT" "attempt 3, base 2s" "and is CAPPED at RestartMaxDelaySec"

# ---------------------------------------------------------------------------
# 15. A hung `freenet update` does not strand the supervisor with no node.
#     The unit gets this bound for free from TimeoutStopSec; a foreground
#     wrapper does not.
#     ...and a SIGTERM aimed at the WRAPPER is honoured promptly even in the
#     middle of a long restart pause -- that is the genuine operator stop, and
#     the reason case 5 can safely restart on a signalled node.
# ---------------------------------------------------------------------------
START_T="$(date +%s)"
WRAP_UPDATE_SLEEP=60 WRAP_UPDATER_TIMEOUT=1 run_wrapper "$(printf '101\n0')"
ELAPSED="$(($(date +%s) - START_T))"
assert_eq "$RC" "0" "a hung updater does not fail the supervisor"
assert_eq "$([ "$ELAPSED" -lt 30 ] && echo fast || echo "stuck(${ELAPSED}s)")" "fast" \
  "a hung 'freenet update' is bounded by a timeout instead of blocking the supervisor forever"
assert_eq "$(starts_of)" "2" "the node is restarted after the hung updater is cut short"

WORK6="$(mktemp -d)"
STATE6="$WORK6/state"
SEED6="$WORK6/seed-freenet"
write_fake_freenet "$SEED6"
printf '101\n101\n101\n101\n0\n' >"$WORK6/plan"
: >"$WORK6/log"
env -u XDG_STATE_HOME \
  FAKE_LOG="$WORK6/log" \
  FAKE_PLAN="$WORK6/plan" \
  FAKE_COUNT="$WORK6/count" \
  FREENET_NIX_SEED_BINARY="$SEED6" \
  STATE_DIRECTORY="$STATE6" \
  FREENET_NODE_RESTART_SECS=120 \
  bash "$WRAPPER" >"$WORK6/stdout" 2>&1 &
WPID=$!
# Wait for the wrapper to enter its (120s) restart pause.
PAUSED=no
for _ in $(seq 1 100); do
  if grep -q "restarting in" "$WORK6/stdout" 2>/dev/null; then
    PAUSED=yes
    break
  fi
  sleep 0.1
done
assert_eq "$PAUSED" "yes" "the wrapper reaches its restart pause"
kill -TERM "$WPID" 2>/dev/null || true
STOPPED=no
for _ in $(seq 1 50); do
  if ! kill -0 "$WPID" 2>/dev/null; then
    STOPPED=yes
    break
  fi
  sleep 0.1
done
wait "$WPID" 2>/dev/null || true
assert_eq "$STOPPED" "yes" \
  "SIGTERM to the WRAPPER stops it promptly mid-pause -- bash defers a trap across a foreground sleep, so the pause must be backgrounded and waited on"

echo
echo "passed: $PASS, failed: $FAIL"
[ "$FAIL" -eq 0 ]
