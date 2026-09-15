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
# A fake `freenet`: records how it was called, and exits with the Nth code from
# a plan file on its Nth `network` invocation.
# ---------------------------------------------------------------------------
write_fake_freenet() {
  cat >"$1" <<'FAKE'
#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  network)
    shift
    n=0
    [ -f "$FAKE_COUNT" ] && n="$(cat "$FAKE_COUNT")"
    n=$((n + 1))
    printf '%s' "$n" >"$FAKE_COUNT"
    printf 'network|args=%s|supervised=%s\n' "$*" "${FREENET_SUPERVISED:-<unset>}" >>"$FAKE_LOG"
    code="$(sed -n "${n}p" "$FAKE_PLAN")"
    [ -n "$code" ] || code=0
    exit "$code"
    ;;
  update)
    shift
    printf 'update|args=%s|poststop=%s\n' "$*" "${FREENET_POST_STOP_EXIT_CODE:-<unset>}" >>"$FAKE_LOG"
    exit 0
    ;;
  *)
    printf 'unexpected|args=%s\n' "$*" >>"$FAKE_LOG"
    exit 64
    ;;
esac
FAKE
  chmod +x "$1"
}

# run_wrapper <plan-as-newline-separated-exit-codes> [wrapper args...]
# Sets LOG, RC and STATE for the caller.
run_wrapper() {
  local plan="$1"
  shift
  WORK="$(mktemp -d)"
  STATE="$WORK/state"
  SEED="$WORK/seed-freenet"
  write_fake_freenet "$SEED"
  printf '%s\n' "$plan" >"$WORK/plan"
  : >"$WORK/log"
  RC=0
  env -u XDG_STATE_HOME \
    FAKE_LOG="$WORK/log" \
    FAKE_PLAN="$WORK/plan" \
    FAKE_COUNT="$WORK/count" \
    FREENET_NIX_SEED_BINARY="$SEED" \
    STATE_DIRECTORY="$STATE" \
    FREENET_NODE_RESTART_SECS=0 \
    bash "$WRAPPER" "$@" >"$WORK/stdout" 2>&1 || RC=$?
  LOG="$(cat "$WORK/log")"
  STDOUT="$(cat "$WORK/stdout")"
}

starts_of() {
  # Count of `network` lines in $LOG, without a status-consuming pipe.
  local n
  n="$(grep -c '^network|' "$WORK/log" || true)"
  printf '%s' "$n"
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
# 4. Exit 43 (another instance already holds the port): stand down.
#    RestartPreventExitStatus=43 in the systemd unit; restarting would only lose
#    the same race again, and running the updater would treat a healthy peer's
#    port as a fault.
# ---------------------------------------------------------------------------
run_wrapper "$(printf '43\n0')"
assert_eq "$RC" "0" "exit 43 is reported as success (SuccessExitStatus=42 43)"
assert_eq "$(starts_of)" "1" "exit 43 does NOT restart the node"
assert_not_contains "$LOG" "update|" "exit 43 does NOT run the updater"

# ---------------------------------------------------------------------------
# 5. A signal-shaped status is a deliberate stop, not a crash (#5227).
# ---------------------------------------------------------------------------
run_wrapper "$(printf '143\n0')"
assert_eq "$RC" "0" "a SIGTERM-shaped status (143) exits cleanly"
assert_eq "$(starts_of)" "1" "a SIGTERM-shaped status does NOT restart the node"
assert_not_contains "$LOG" "update|" \
  "a SIGTERM-shaped status does NOT run the updater, so a clean stop is never scored as a probation crash (#5227)"

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
# 9. Bootstrap: seed once, then NEVER overwrite. Re-seeding on every start would
#    silently pin the node to the flake's version, which is the exact failure
#    this design exists to avoid.
# ---------------------------------------------------------------------------
run_wrapper "0"
assert_eq "$([ -x "$STATE/bin/freenet" ] && echo yes || echo no)" "yes" \
  "the first run seeds an executable binary into the state directory"
assert_contains "$STDOUT" "seeded" "the first run says it seeded the binary"

# Second run against a state dir that already holds a DIFFERENT binary: the
# wrapper must run that one and leave it alone.
WORK2="$(mktemp -d)"
STATE2="$WORK2/state"
mkdir -p "$STATE2/bin"
write_fake_freenet "$STATE2/bin/freenet"
printf 'ALREADY-UPDATED\n' >>"$STATE2/bin/freenet"
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
  "an EXISTING state-dir binary is never overwritten by the store's seed (the node owns it)"
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

echo
echo "passed: $PASS, failed: $FAIL"
[ "$FAIL" -eq 0 ]
