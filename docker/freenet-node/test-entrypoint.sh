#!/bin/sh
#
# Tests for the container entrypoint's update-supervisor semantics.
#
# These exist because the supervisor is the piece that keeps a containerized
# node current, and a silent regression in it does not surface as a failed
# build. It surfaces months later as a fleet of stale containers that have
# fallen below min-compatible-version and been refused by every peer.
#
# The node is replaced by a fake binary that records how it was invoked and
# exits with a scripted status, so every branch of the loop (clean exit, exit
# 42, exit 43, crash, SIGTERM) is exercised without a network or a real node.
#
# The fake carries its version INSIDE the file, the way a real binary does, and
# `update` rewrites that file in place the way a real self-update replaces
# current_exe(). That matters: an earlier draft kept the version in a file
# beside the binary, so an image copy and a volume copy always reported the
# same version and the seeding tests passed no matter what seed_binary did.
#
# Run: docker/freenet-node/test-entrypoint.sh

# shellcheck disable=SC2034
# Several variables below are consumed by freenet-node-startup.sh, which this
# script sources with `.`. shellcheck cannot follow that include, so it reports
# every such variable as unused. The suppression is file-scoped because the
# assignments are spread across the helper-test sections.

set -eu

HERE="$(cd "$(dirname "$0")" && pwd)"
ENTRYPOINT="${HERE}/freenet-node-startup.sh"

if [ "$(id -u)" -eq 0 ]; then
    echo "SKIP: must run as a non-root user (the root path re-execs under gosu)" >&2
    exit 0
fi

failures=0
checks=0

check() {
    description="$1"
    expected="$2"
    actual="$3"
    checks=$((checks + 1))
    if [ "$expected" = "$actual" ]; then
        printf 'ok   %s\n' "$description"
    else
        printf 'FAIL %s\n       expected: %s\n       actual:   %s\n' \
            "$description" "$expected" "$actual"
        failures=$((failures + 1))
    fi
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

STATE="$WORK/state"
mkdir -p "$STATE"
export FREENET_FAKE_STATE="$STATE"

# ---------------------------------------------------------------------------
# A stand-in for the node binary, written to $1 with version $2 baked in.
#
#   --version        prints the version baked into this file
#   network ...      appends to $STATE/network.log, exits with the next status
#                    from $STATE/exit-codes (one per line), defaulting to 0
#   update --quiet   appends to $STATE/update.log along with the exit status the
#                    supervisor forwarded, then rewrites its own file with the
#                    version in $STATE/update-to, mimicking a self-update
# ---------------------------------------------------------------------------
make_fake_node() {
    target="$1"
    version="$2"
    mkdir -p "$(dirname "$target")"
    cat > "$target" <<FAKE
#!/bin/sh
VERSION="${version}"
FAKE
    cat >> "$target" <<'FAKE'
state="$FREENET_FAKE_STATE"
case "${1:-}" in
    --version)
        echo "Freenet version: $VERSION (deadbeefcafe)"
        echo "Build timestamp: 2026-01-01T00:00:00Z"
        exit 0
        ;;
    network)
        echo "network $*" >> "$state/network.log"
        # Recorded separately so assertions can check what the supervisor
        # actually handed the node, not merely how it was invoked.
        echo "env supervised=${FREENET_SUPERVISED:-unset} fastcrash=${FREENET_SYSTEMD_FAST_CRASH:-unset}" \
            >> "$state/network.log"
        if [ -s "$state/exit-codes" ]; then
            code="$(head -n 1 "$state/exit-codes")"
            sed -i '1d' "$state/exit-codes"
        else
            code=0
        fi
        if [ "$code" = "sleep-fail" ]; then
            # Stops on TERM but reports failure, modelling a node killed
            # mid-drain or erroring during shutdown.
            trap 'echo terminated >> "$state/network.log"; exit 7' TERM
            i=0
            while [ "$i" -lt 200 ]; do
                sleep 0.1 &
                wait $!
                i=$((i + 1))
            done
            exit 0
        fi
        if [ "$code" = "sleep-drain" ]; then
            # Takes a measurable time to shut down, so a supervisor that stops
            # waiting as soon as the signal arrives can be told apart from one
            # that waits for the child to actually finish.
            trap 'sleep 2; echo drained >> "$state/network.log"; exit 0' TERM
            i=0
            while [ "$i" -lt 200 ]; do
                sleep 0.1 &
                wait $!
                i=$((i + 1))
            done
            exit 0
        fi
        if [ "$code" = "sleep" ]; then
            # Used by the signal test: run until told to stop, and record that
            # the TERM actually arrived rather than the process being killed.
            trap 'echo terminated >> "$state/network.log"; exit 0' TERM
            i=0
            while [ "$i" -lt 200 ]; do
                sleep 0.1 &
                wait $!
                i=$((i + 1))
            done
            exit 0
        fi
        exit "$code"
        ;;
    update)
        echo "update post_stop=${FREENET_POST_STOP_EXIT_CODE:-unset}" >> "$state/update.log"
        if [ -s "$state/update-fails" ]; then
            exit 1
        fi
        if [ -s "$state/slow-update" ]; then
            # Widens the update step into a window a stop signal can land in.
            sleep 3
        fi
        if [ -s "$state/corrupt-on-update" ]; then
            # Models an update that installed a broken or truncated binary.
            echo 'not a runnable binary' > "$0"
            : > "$state/corrupt-on-update"
            exit 0
        fi
        if [ -s "$state/update-to" ]; then
            new="$(cat "$state/update-to")"
            sed -i "s/^VERSION=.*/VERSION=\"$new\"/" "$0"
            : > "$state/update-to"
        fi
        exit 0
        ;;
esac
exit 64
FAKE
    chmod +x "$target"
}

reset_state() {
    : > "$STATE/network.log"
    : > "$STATE/update.log"
    : > "$STATE/exit-codes"
    : > "$STATE/update-to"
    : > "$STATE/corrupt-on-update"
    : > "$STATE/slow-update"
    : > "$STATE/update-fails"
}

# Runs the entrypoint against a fake node, printing its exit status. The image
# binary and the volume binary are deliberately separate files in separate
# directories, as they are in the real image.
#
# Bounded by `timeout`, because the interesting regressions here are supervisor
# WEDGES rather than wrong return values. Without the per-iteration re-seed, for
# example, the loop spins forever restarting a binary that cannot run. Unbounded
# that hangs the suite, and a hung CI job reads as infrastructure flakiness
# rather than as the defect it is. Exceeding the bound surfaces as status 124,
# which fails whichever check expected a real exit code.
run_entrypoint() {
    (
        timeout "${ENTRYPOINT_TIMEOUT:-30}" env \
        FREENET_CONFIG_DIR="$WORK/config" \
        FREENET_DATA_DIR="$WORK/data" \
        FREENET_BIN_DIR="$WORK/volume" \
        LOG_DIR="$WORK/logs" \
        FREENET_HOME_DIR="$WORK/home" \
        FREENET_PID_FILE="$WORK/node.pid" \
        FREENET_UPDATING_FILE="$WORK/updating" \
        FREENET_IMAGE_BIN="$WORK/image/freenet" \
        FREENET_RESTART_DELAY_MIN=0 \
        FREENET_RESTART_DELAY_MAX=0 \
        FREENET_RESTART_JITTER_PCT=0 \
        FREENET_HEALTHY_RUN_SECS=99999 \
        "$ENTRYPOINT" "$@" >> "$WORK/entrypoint.log" 2>&1
        echo $? > "$WORK/status"
    ) || true
    cat "$WORK/status"
}

# ---------------------------------------------------------------------------
# Unit tests for the helpers, sourced without starting a node.
# ---------------------------------------------------------------------------
make_fake_node "$WORK/image/freenet" "0.2.100"
reset_state

# Plain assignments, NOT a `VAR=x . file` prefix. Prefix assignments to a
# special builtin such as `.` persist after the command, and in bash's POSIX
# mode they are also EXPORTED. That leaked FREENET_ENTRYPOINT_SOURCE_ONLY into
# every child process, so the entrypoint returned immediately and all fourteen
# end-to-end checks below failed under bash while passing under dash. Found by
# an external review pass; keep these as plain assignments and unset the guard.
FREENET_ENTRYPOINT_SOURCE_ONLY=1
FREENET_BIN_DIR="$WORK/volume"
FREENET_IMAGE_BIN="$WORK/image/freenet"
FREENET_RESTART_DELAY_MIN=10
FREENET_RESTART_DELAY_MAX=300
FREENET_RESTART_JITTER_PCT=0
# shellcheck source=/dev/null
. "$ENTRYPOINT"
unset FREENET_ENTRYPOINT_SOURCE_ONLY
unset FREENET_BIN_DIR
unset FREENET_IMAGE_BIN
unset FREENET_RESTART_DELAY_MIN
unset FREENET_RESTART_DELAY_MAX
unset FREENET_RESTART_JITTER_PCT

check "binary_version parses the node's --version output" \
    "0.2.100" "$(binary_version "$WORK/image/freenet")"
check "binary_version is empty for a missing binary" \
    "" "$(binary_version "$WORK/definitely-absent")"

check "restart_delay starts at the minimum" 10 "$(restart_delay 1)"
check "restart_delay doubles" 20 "$(restart_delay 2)"
check "restart_delay doubles again" 40 "$(restart_delay 3)"
check "restart_delay saturates at the maximum" 300 "$(restart_delay 9)"

# With jitter on, the delay must stay near the base but must not be constant.
# A bad release wedges every container at once, so identical backoff would have
# them all poll GitHub in lockstep for as long as it is out.
RESTART_JITTER_PCT=20
jitter_samples=""
jitter_out_of_range=0
jitter_i=0
while [ "$jitter_i" -lt 25 ]; do
    jitter_value="$(jitter 100)"
    if [ "$jitter_value" -lt 80 ] || [ "$jitter_value" -gt 120 ]; then
        jitter_out_of_range=$((jitter_out_of_range + 1))
    fi
    jitter_samples="${jitter_samples}${jitter_value} "
    jitter_i=$((jitter_i + 1))
done
RESTART_JITTER_PCT=0

check "jitter stays within +/-20% of the base delay" 0 "$jitter_out_of_range"
check "jitter actually varies rather than returning a constant" \
    "yes" "$([ "$(printf '%s' "$jitter_samples" | tr ' ' '\n' | sort -u | grep -c .)" -gt 1 ] && echo yes || echo no)"
check "jitter is a no-op when disabled" 100 "$(jitter 100)"

# Asserted through restart_delay, not on jitter() alone. Testing the helper says
# nothing about whether the caller still uses it: dropping the jitter call from
# restart_delay leaves jitter() intact and every direct test above still green,
# which is exactly what happened before this check existed.
RESTART_JITTER_PCT=20
saved_min="$RESTART_DELAY_MIN"
saved_max="$RESTART_DELAY_MAX"
RESTART_DELAY_MIN=100
RESTART_DELAY_MAX=1000
rd_samples=""
rd_out_of_range=0
rd_i=0
while [ "$rd_i" -lt 25 ]; do
    rd_value="$(restart_delay 1)"
    if [ "$rd_value" -lt 80 ] || [ "$rd_value" -gt 120 ]; then
        rd_out_of_range=$((rd_out_of_range + 1))
    fi
    rd_samples="${rd_samples}${rd_value} "
    rd_i=$((rd_i + 1))
done
RESTART_JITTER_PCT=0
RESTART_DELAY_MIN="$saved_min"
RESTART_DELAY_MAX="$saved_max"

check "restart_delay actually applies the jitter, not just jitter() alone" \
    "yes" "$([ "$(printf '%s' "$rd_samples" | tr ' ' '\n' | sort -u | grep -c .)" -gt 1 ] && echo yes || echo no)"
check "restart_delay's jittered value stays within +/-20% of the base" \
    0 "$rd_out_of_range"

# The ceiling is jittered too, so a fleet that has all reached the maximum does
# not resynchronise there.
RESTART_JITTER_PCT=20
RESTART_DELAY_MIN=100
RESTART_DELAY_MAX=1000
cap_samples=""
cap_i=0
while [ "$cap_i" -lt 25 ]; do
    cap_samples="${cap_samples}$(restart_delay 40) "
    cap_i=$((cap_i + 1))
done
RESTART_JITTER_PCT=0
RESTART_DELAY_MIN="$saved_min"
RESTART_DELAY_MAX="$saved_max"

check "the backoff ceiling is jittered as well" \
    "yes" "$([ "$(printf '%s' "$cap_samples" | tr ' ' '\n' | sort -u | grep -c .)" -gt 1 ] && echo yes || echo no)"
check "restart_delay never exceeds the maximum" 300 "$(restart_delay 40)"

# ---------------------------------------------------------------------------
# The 0|43 skip set must match the systemd unit's ExecStopPost. Getting this
# wrong means either updating on a clean shutdown (pointless work on every
# stop) or, far worse, NOT updating on a crash, which removes the self-heal
# path that lets a later fixed release rescue a wedged node.
# ---------------------------------------------------------------------------
RUN_BIN="$WORK/image/freenet"

reset_state
update_after_exit 0
check "no update on a clean exit" "" "$(cat "$STATE/update.log")"

reset_state
update_after_exit 43
check "no update on exit 43 (already running)" "" "$(cat "$STATE/update.log")"

reset_state
update_after_exit 42
check "update runs on exit 42, with the status forwarded" \
    "update post_stop=42" "$(cat "$STATE/update.log")"

reset_state
update_after_exit 101
check "update runs on an arbitrary crash, with the status forwarded" \
    "update post_stop=101" "$(cat "$STATE/update.log")"

# ---------------------------------------------------------------------------
# Binary seeding. The volume copy is what the node runs, and a self-updated
# volume binary must never be dragged backwards by a container restart.
#
# shellcheck disable=SC2034
# IMAGE_BIN, RUN_BIN and BIN_DIR are read by seed_binary, which was sourced
# from the entrypoint above; shellcheck cannot see across the `.` include.
# ---------------------------------------------------------------------------
rm -rf "$WORK/volume"
mkdir -p "$WORK/volume"
IMAGE_BIN="$WORK/image/freenet"
RUN_BIN="$WORK/volume/freenet"
BIN_DIR="$WORK/volume"

seed_binary
check "a missing volume binary is seeded from the image" \
    "0.2.100" "$(binary_version "$WORK/volume/freenet")"

# Image moves ahead of the volume: pulling a newer image must upgrade the node.
make_fake_node "$WORK/image/freenet" "0.2.150"
seed_binary
check "a newer image binary replaces an older volume binary" \
    "0.2.150" "$(binary_version "$WORK/volume/freenet")"

# Volume ahead of the image: a node that self-updated past the image's version
# must keep its newer binary rather than being rolled back on every restart.
make_fake_node "$WORK/volume/freenet" "0.2.200"
make_fake_node "$WORK/image/freenet" "0.2.150"
seed_binary
check "an older image binary does NOT overwrite a self-updated volume binary" \
    "0.2.200" "$(binary_version "$WORK/volume/freenet")"

# Equal versions must not churn the file.
make_fake_node "$WORK/volume/freenet" "0.2.150"
seed_binary
check "an equal image version leaves the volume binary alone" \
    "0.2.150" "$(binary_version "$WORK/volume/freenet")"

# A truncated or wrong-architecture volume binary reports no version and must
# be replaced from the image rather than wedging the container.
echo 'not a binary' > "$WORK/volume/freenet"
chmod +x "$WORK/volume/freenet"
seed_binary
check "an unrunnable volume binary is re-seeded from the image" \
    "0.2.150" "$(binary_version "$WORK/volume/freenet")"

# ---------------------------------------------------------------------------
# End-to-end supervise-loop behaviour.
# ---------------------------------------------------------------------------
fresh_run() {
    rm -rf "$WORK/volume"
    make_fake_node "$WORK/image/freenet" "0.2.100"
    reset_state
}

fresh_run
printf '0\n' > "$STATE/exit-codes"
check "a clean node exit stops the container with status 0" 0 "$(run_entrypoint)"
# Paired with a positive check on purpose: "update.log is empty" is also true
# when the entrypoint never ran, so on its own it proves nothing.
check "the node was actually started for the clean-exit case" \
    1 "$(grep -c '^network ' "$STATE/network.log")"
check "no update is attempted on a clean exit" "" "$(cat "$STATE/update.log")"

fresh_run
printf '43\n' > "$STATE/exit-codes"
check "exit 43 stops rather than restarting" 43 "$(run_entrypoint)"
check "exit 43 attempts no update" "" "$(cat "$STATE/update.log")"
check "exit 43 starts the node exactly once" \
    1 "$(grep -c '^network ' "$STATE/network.log")"

# The case this whole image exists for: an available update must be applied
# and the node
# restarted on the new version, without any operator involvement.
fresh_run
printf '42\n0\n' > "$STATE/exit-codes"
echo "0.2.201" > "$STATE/update-to"
check "exit 42 applies the update and restarts" 0 "$(run_entrypoint)"
check "the update ran with the exit status forwarded" \
    "update post_stop=42" "$(cat "$STATE/update.log")"
check "the node was started again after updating" \
    2 "$(grep -c '^network ' "$STATE/network.log")"
check "the update was applied to the binary the node runs from" \
    "0.2.201" "$(binary_version "$WORK/volume/freenet")"
check "the update did NOT touch the image's binary" \
    "0.2.100" "$(binary_version "$WORK/image/freenet")"
check "the image binary is still runnable after an update" \
    "yes" "$([ -n "$(binary_version "$WORK/image/freenet")" ] && echo yes || echo no)"

# A crash must also trigger an update attempt: being out of date is a common
# cause of a crash, and a later fixed release is the self-heal path.
fresh_run
printf '101\n0\n' > "$STATE/exit-codes"
check "a crash triggers an update and a restart" 0 "$(run_entrypoint)"
check "the crash status was forwarded to the updater" \
    "update post_stop=101" "$(cat "$STATE/update.log")"

fresh_run
printf '101\n101\n101\n0\n' > "$STATE/exit-codes"
check "repeated crashes keep restarting rather than giving up" 0 "$(run_entrypoint)"
check "every crash attempted an update" 3 "$(wc -l < "$STATE/update.log")"

# ---------------------------------------------------------------------------
# The node must be told explicitly where its config and data live.
#
# This script's FREENET_CONFIG_DIR / FREENET_DATA_DIR are its OWN variables. The
# node reads CONFIG_DIR / DATA_DIR. An earlier revision dropped the flags and
# relied on the environment, so the node fell back to $HOME and wrote its
# contracts, delegates, secrets, db and peer identity into the container's
# writable layer, to be destroyed by the documented upgrade command. Nothing
# about the volume looked wrong: the directories existed, were correctly owned,
# and were simply empty. Assert on the ARGUMENTS the node receives, because that
# is the only place the mistake is visible.
# ---------------------------------------------------------------------------
fresh_run
printf '0\n' > "$STATE/exit-codes"
run_entrypoint > /dev/null
check "the node is given an explicit --config-dir" \
    1 "$(grep -c -- "--config-dir $WORK/config" "$STATE/network.log")"
check "the node is given an explicit --data-dir" \
    1 "$(grep -c -- "--data-dir $WORK/data" "$STATE/network.log")"
check "HOME points at the volume, not the image layer" \
    "yes" "$([ -d "$WORK/home" ] && echo yes || echo no)"

# ---------------------------------------------------------------------------
# The entrypoint must write only inside the directories it is configured with.
#
# Regression guard: when log files moved onto the data volume, LOG_DIR was added
# to the entrypoint but not to this harness, so the entrypoint tried to create
# /data/logs on the host, died before starting the node, and every end-to-end
# check above failed at once. A bare `mkdir: Permission denied` in a log nobody
# reads is a poor way to learn that.
# ---------------------------------------------------------------------------
fresh_run
printf '0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
run_entrypoint > /dev/null
check "the entrypoint writes only inside its configured directories" \
    0 "$(grep -c 'Permission denied' "$WORK/entrypoint.log")"
check "the configured log directory is created" \
    "yes" "$([ -d "$WORK/logs" ] && echo yes || echo no)"

# An unwritable directory must explain itself rather than emitting a bare
# mkdir error, because this is what an operator hits with `docker run --user`.
fresh_run
: > "$WORK/entrypoint.log"
(
    FREENET_CONFIG_DIR="$WORK/config" \
    FREENET_DATA_DIR="$WORK/data" \
    FREENET_BIN_DIR="$WORK/volume" \
    LOG_DIR="/proc/cannot-create-here" \
    FREENET_IMAGE_BIN="$WORK/image/freenet" \
        "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1
    echo $? > "$WORK/status"
) || echo 1 > "$WORK/status"
check "an uncreatable directory fails with a clear message" \
    1 "$(grep -c 'FATAL: cannot create /proc/cannot-create-here' "$WORK/entrypoint.log")"

# ---------------------------------------------------------------------------
# A volume binary that will not run must self-heal from the image copy rather
# than wedging the container. Without the per-iteration re-seed, a failed update
# leaves the node unable to start AND unable to update, backing off forever.
# ---------------------------------------------------------------------------
fresh_run
mkdir -p "$WORK/volume"
echo 'not a runnable binary' > "$WORK/volume/freenet"
chmod +x "$WORK/volume/freenet"
printf '0\n' > "$STATE/exit-codes"
check "a corrupt volume binary present at startup is replaced" \
    0 "$(run_entrypoint)"
check "the node ran after that self-heal" \
    1 "$(grep -c '^network ' "$STATE/network.log")"

# The case only the per-iteration re-seed can handle: the update itself installs
# a broken binary. Seeding once before the loop cannot recover from this, and
# without recovery the node can neither start nor update, so it backs off
# forever with a working binary sitting unused in the image.
fresh_run
printf '42\n0\n' > "$STATE/exit-codes"
echo 1 > "$STATE/corrupt-on-update"
check "an update that installs a broken binary self-heals on the next attempt" \
    0 "$(run_entrypoint)"
check "the node started again after the broken update was replaced" \
    2 "$(grep -c '^network ' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# The single property this whole image exists to preserve: unless the operator
# explicitly opts out, the node must NOT be started with --disable-auto-update.
#
# Asserting only the opted-out case is not enough. Flipping the default to
# disabled leaves every opted-out assertion true, so the suite passes while every
# container silently stops updating. An assertion that something IS present says
# nothing about the case where it must be ABSENT.
# ---------------------------------------------------------------------------
fresh_run
printf '0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
run_entrypoint > /dev/null
check "auto-update is NOT disabled by default" \
    0 "$(grep -c -- '--disable-auto-update' "$STATE/network.log")"
check "the banner reports auto-update as enabled by default" \
    1 "$(grep -c 'auto-update  : enabled' "$WORK/entrypoint.log")"

# The banner must describe the decision that was made, not recompute it. A
# banner derived separately can say "enabled" over a node that was handed
# --disable-auto-update.
fresh_run
printf '0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
FREENET_DISABLE_AUTO_UPDATE=1 run_entrypoint > /dev/null
check "the banner reports auto-update as DISABLED when it is" \
    1 "$(grep -c 'auto-update  : DISABLED' "$WORK/entrypoint.log")"

# ---------------------------------------------------------------------------
# Arguments after the image name are documented as reaching `freenet network`.
# Dropping "$@" is invisible to any assertion that only counts starts.
# ---------------------------------------------------------------------------
fresh_run
printf '0\n' > "$STATE/exit-codes"
run_entrypoint --gateway "example:31337,abc" > /dev/null
check "operator arguments are forwarded to the node" \
    1 "$(grep -c -- '--gateway example:31337,abc' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# An image whose binary will not run must fail loudly at startup. Without the
# guard the loop would spin trying to execute it.
# ---------------------------------------------------------------------------
fresh_run
echo 'not a runnable binary' > "$WORK/image/freenet"
chmod +x "$WORK/image/freenet"
: > "$WORK/entrypoint.log"
check "an unrunnable image binary exits rather than looping" \
    1 "$(run_entrypoint)"
check "an unrunnable image binary says so" \
    1 "$(grep -c 'FATAL: image binary' "$WORK/entrypoint.log")"

# ---------------------------------------------------------------------------
# The auto-update opt-out has to hold on EVERY path. Honouring it on the
# ordinary path and ignoring it after a crash is the half an operator is least
# likely to be watching.
# ---------------------------------------------------------------------------
fresh_run
printf '101\n0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
FREENET_DISABLE_AUTO_UPDATE=1 run_entrypoint > /dev/null
check "no update runs after a crash when the operator opted out" \
    "" "$(cat "$STATE/update.log")"
check "the node still restarts after a crash when opted out" \
    2 "$(grep -c '^network ' "$STATE/network.log")"
check "the node is told auto-update is disabled" \
    2 "$(grep -c -- '--disable-auto-update' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# The node is opted in to the distinct fast-crash exit code, so a boot wedge
# reports 45 rather than reusing 42. Exit 45 must count as a failure AND still
# attempt an update, which is the contract that marker asserts.
# ---------------------------------------------------------------------------
fresh_run
printf '45\n0\n' > "$STATE/exit-codes"
check "a fast-crash exit 45 restarts and attempts an update" \
    0 "$(run_entrypoint)"
check "exit 45 forwarded its status to the updater" \
    "update post_stop=45" "$(cat "$STATE/update.log")"

# Without the opt-in the node never emits 45 at all, so the handling above would
# be dead code and the test above would pass regardless. Assert the marker the
# node actually receives.
check "the node is opted in to the distinct fast-crash exit code" \
    2 "$(grep -c 'fastcrash=1' "$STATE/network.log")"
check "the node is told it is supervised" \
    2 "$(grep -c 'supervised=1' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# Exit 42 is not only "an update is available". The fatal-listener path reuses
# the same code so the systemd update hook fires on a wedge, so a node that
# wedges instantly at boot exits 42 immediately. Treating that as a routine
# update reset the backoff, and the container restarted every RESTART_DELAY_MIN
# seconds forever with a GitHub round-trip each time.
# ---------------------------------------------------------------------------
fresh_run
printf '42\n42\n42\n0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
FREENET_UPDATE_MIN_UPTIME_SECS=30 run_entrypoint > /dev/null
check "an instant exit 42 is treated as a crash, not a routine update" \
    3 "$(grep -c 'treating as a crash rather than a routine update' "$WORK/entrypoint.log")"

# A 42 after a decent uptime is the ordinary update path and must NOT be
# reclassified, or every real update would start growing the backoff.
fresh_run
printf '42\n0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
FREENET_UPDATE_MIN_UPTIME_SECS=0 run_entrypoint > /dev/null
check "a normal exit 42 is still treated as a routine update" \
    0 "$(grep -c 'treating as a crash rather than a routine update' "$WORK/entrypoint.log")"

# ---------------------------------------------------------------------------
# The healthcheck needs to know which process is ours and when an update is in
# flight. Without the pid it cannot tell our node from anything else answering
# on the API port, which matters under the documented `network_mode: host`.
# ---------------------------------------------------------------------------
fresh_run
printf '42\n0\n' > "$STATE/exit-codes"
run_entrypoint > /dev/null
check "the update window marker is cleared once the update finishes" \
    "no" "$([ -f "$WORK/updating" ] && echo yes || echo no)"
check "the pid file is cleared after the node exits" \
    "no" "$([ -f "$WORK/node.pid" ] && echo yes || echo no)"

# ---------------------------------------------------------------------------
# SIGTERM must reach the node. This shell is PID 1, so if it does not forward
# the signal the node never drains and is SIGKILLed when the grace period ends,
# aborting every in-flight operation on every stop.
# ---------------------------------------------------------------------------
fresh_run
printf 'sleep\n' > "$STATE/exit-codes"
FREENET_CONFIG_DIR="$WORK/config" \
FREENET_DATA_DIR="$WORK/data" \
FREENET_BIN_DIR="$WORK/volume" \
LOG_DIR="$WORK/logs" \
FREENET_HOME_DIR="$WORK/home" \
FREENET_PID_FILE="$WORK/node.pid" \
FREENET_UPDATING_FILE="$WORK/updating" \
FREENET_IMAGE_BIN="$WORK/image/freenet" \
FREENET_RESTART_DELAY_MIN=0 \
FREENET_RESTART_DELAY_MAX=0 \
FREENET_RESTART_JITTER_PCT=0 \
    "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1 &
supervisor_pid=$!

# Wait for the fake node to actually be running before signalling.
waited=0
while [ "$waited" -lt 100 ]; do
    if [ -s "$STATE/network.log" ]; then
        break
    fi
    sleep 0.1
    waited=$((waited + 1))
done

kill -TERM "$supervisor_pid" 2>/dev/null || true
if wait "$supervisor_pid"; then
    term_status=0
else
    term_status=$?
fi

check "SIGTERM stops the supervisor with status 0" 0 "$term_status"
check "SIGTERM was forwarded to the node so it could drain" \
    1 "$(grep -c '^terminated$' "$STATE/network.log")"
check "the node was not restarted after a stop signal" \
    1 "$(grep -c '^network ' "$STATE/network.log")"
check "no update is attempted while shutting down" \
    "" "$(cat "$STATE/update.log")"

# ---------------------------------------------------------------------------
# The same stop, but the node exits NON-ZERO on its way down. Without the
# stopping guard the loop reads that as a crash: it runs `freenet update` and
# restarts the node, in the middle of a container shutdown that Docker is
# already timing. The clean-exit case above cannot catch this, because a node
# that exits 0 takes the ordinary "exited cleanly" path either way.
# ---------------------------------------------------------------------------
fresh_run
printf 'sleep-fail\n' > "$STATE/exit-codes"
FREENET_CONFIG_DIR="$WORK/config" \
FREENET_DATA_DIR="$WORK/data" \
FREENET_BIN_DIR="$WORK/volume" \
LOG_DIR="$WORK/logs" \
FREENET_HOME_DIR="$WORK/home" \
FREENET_PID_FILE="$WORK/node.pid" \
FREENET_UPDATING_FILE="$WORK/updating" \
FREENET_IMAGE_BIN="$WORK/image/freenet" \
FREENET_RESTART_DELAY_MIN=0 \
FREENET_RESTART_DELAY_MAX=0 \
FREENET_RESTART_JITTER_PCT=0 \
    "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1 &
supervisor_fail_pid=$!

waited=0
while [ "$waited" -lt 100 ]; do
    if [ -s "$STATE/network.log" ]; then
        break
    fi
    sleep 0.1
    waited=$((waited + 1))
done

kill -TERM "$supervisor_fail_pid" 2>/dev/null || true
if wait "$supervisor_fail_pid"; then
    fail_status=0
else
    fail_status=$?
fi

check "a non-zero exit during shutdown still stops the container cleanly" \
    0 "$fail_status"
check "a non-zero exit during shutdown does not trigger an update" \
    "" "$(cat "$STATE/update.log")"
check "a non-zero exit during shutdown does not restart the node" \
    1 "$(grep -c '^network ' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# A stop signal arriving DURING the update step, rather than while the node is
# running. The guard covering this is a different one from the guard the tests
# above exercise: those deliver TERM while the node runs, so the first check
# always fires and the later ones are never reached. Without them a `docker
# stop` here is ignored for up to RESTART_DELAY_MAX (300s, far past the 45s
# grace period), so Docker SIGKILLs the container and the supervisor starts a
# fresh node on the way out.
# ---------------------------------------------------------------------------
fresh_run
printf '101\n0\n' > "$STATE/exit-codes"
echo 1 > "$STATE/slow-update"
FREENET_CONFIG_DIR="$WORK/config" \
FREENET_DATA_DIR="$WORK/data" \
FREENET_BIN_DIR="$WORK/volume" \
LOG_DIR="$WORK/logs" \
FREENET_HOME_DIR="$WORK/home" \
FREENET_PID_FILE="$WORK/node.pid" \
FREENET_UPDATING_FILE="$WORK/updating" \
FREENET_IMAGE_BIN="$WORK/image/freenet" \
FREENET_RESTART_DELAY_MIN=60 \
FREENET_RESTART_DELAY_MAX=60 \
FREENET_RESTART_JITTER_PCT=0 \
    "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1 &
update_stop_pid=$!

# Wait until the update step has actually begun.
waited=0
while [ "$waited" -lt 150 ]; do
    if [ -s "$STATE/update.log" ]; then
        break
    fi
    sleep 0.1
    waited=$((waited + 1))
done

stop_started="$(date +%s)"
kill -TERM "$update_stop_pid" 2>/dev/null || true
if wait "$update_stop_pid"; then
    update_stop_status=0
else
    update_stop_status=$?
fi
stop_took=$(( $(date +%s) - stop_started ))

check "a stop during the update step exits cleanly" 0 "$update_stop_status"
check "a stop during the update step does not wait out the restart backoff" \
    "yes" "$([ "$stop_took" -lt 30 ] && echo yes || echo no)"
check "a stop during the update step does not start the node again" \
    1 "$(grep -c '^network ' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# `wait` returns 128+signum the instant a trapped signal arrives, while the node
# is still draining. Without the re-wait the supervisor stops watching there:
# the real exit status is lost and the drain is cut short, which is the whole
# reason the node is given a 45s grace period.
# ---------------------------------------------------------------------------
fresh_run
printf 'sleep-drain\n' > "$STATE/exit-codes"
FREENET_CONFIG_DIR="$WORK/config" \
FREENET_DATA_DIR="$WORK/data" \
FREENET_BIN_DIR="$WORK/volume" \
LOG_DIR="$WORK/logs" \
FREENET_HOME_DIR="$WORK/home" \
FREENET_PID_FILE="$WORK/node.pid" \
FREENET_UPDATING_FILE="$WORK/updating" \
FREENET_IMAGE_BIN="$WORK/image/freenet" \
FREENET_RESTART_DELAY_MIN=0 \
FREENET_RESTART_DELAY_MAX=0 \
FREENET_RESTART_JITTER_PCT=0 \
    "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1 &
drain_pid=$!

waited=0
while [ "$waited" -lt 150 ]; do
    if [ -s "$STATE/network.log" ]; then
        break
    fi
    sleep 0.1
    waited=$((waited + 1))
done

drain_started="$(date +%s)"
kill -TERM "$drain_pid" 2>/dev/null || true
wait "$drain_pid" 2>/dev/null || true
drain_took=$(( $(date +%s) - drain_started ))

check "the supervisor waits for the node to finish draining" \
    "yes" "$([ "$drain_took" -ge 2 ] && echo yes || echo no)"
check "the node completed its drain before the supervisor exited" \
    1 "$(grep -c '^drained$' "$STATE/network.log")"

# ---------------------------------------------------------------------------
# The restart backoff must actually grow across consecutive failures, and reset
# once the node exits for a routine reason. A condition stuck at either extreme
# is invisible to any test that only checks the node restarted.
# ---------------------------------------------------------------------------
fresh_run
printf '101\n101\n101\n0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
(
    FREENET_CONFIG_DIR="$WORK/config" \
    FREENET_DATA_DIR="$WORK/data" \
    FREENET_BIN_DIR="$WORK/volume" \
    LOG_DIR="$WORK/logs" \
    FREENET_HOME_DIR="$WORK/home" \
    FREENET_PID_FILE="$WORK/node.pid" \
    FREENET_UPDATING_FILE="$WORK/updating" \
    FREENET_IMAGE_BIN="$WORK/image/freenet" \
    FREENET_RESTART_DELAY_MIN=1 \
    FREENET_RESTART_DELAY_MAX=8 \
    FREENET_RESTART_JITTER_PCT=0 \
    FREENET_HEALTHY_RUN_SECS=99999 \
        timeout 60 "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1
) || true

check "the backoff grows on the first failure" \
    1 "$(grep -c 'restarting in 1s' "$WORK/entrypoint.log")"
check "the backoff doubles on the second failure" \
    1 "$(grep -c 'restarting in 2s' "$WORK/entrypoint.log")"
check "the backoff doubles again on the third failure" \
    1 "$(grep -c 'restarting in 4s' "$WORK/entrypoint.log")"

# A routine update restart must reset the backoff, not inherit the growth from
# whatever happened before it.
fresh_run
printf '101\n42\n0\n' > "$STATE/exit-codes"
: > "$WORK/entrypoint.log"
(
    FREENET_CONFIG_DIR="$WORK/config" \
    FREENET_DATA_DIR="$WORK/data" \
    FREENET_BIN_DIR="$WORK/volume" \
    LOG_DIR="$WORK/logs" \
    FREENET_HOME_DIR="$WORK/home" \
    FREENET_PID_FILE="$WORK/node.pid" \
    FREENET_UPDATING_FILE="$WORK/updating" \
    FREENET_IMAGE_BIN="$WORK/image/freenet" \
    FREENET_RESTART_DELAY_MIN=1 \
    FREENET_RESTART_DELAY_MAX=8 \
    FREENET_RESTART_JITTER_PCT=0 \
    FREENET_UPDATE_MIN_UPTIME_SECS=0 \
    FREENET_HEALTHY_RUN_SECS=99999 \
        timeout 60 "$ENTRYPOINT" >> "$WORK/entrypoint.log" 2>&1
) || true

check "a routine update restart resets the backoff to the minimum" \
    2 "$(grep -c 'restarting in 1s' "$WORK/entrypoint.log")"

# ---------------------------------------------------------------------------
# A failed update attempt must never be fatal. No network, GitHub unreachable,
# or already-current all land in the same branch, and the node still has to be
# restarted.
# ---------------------------------------------------------------------------
fresh_run
printf '101\n0\n' > "$STATE/exit-codes"
echo 1 > "$STATE/update-fails"
check "a failed update attempt does not kill the supervisor" \
    0 "$(run_entrypoint)"
check "the node is restarted after a failed update attempt" \
    2 "$(grep -c '^network ' "$STATE/network.log")"

printf '\n%s checks, %s failures\n' "$checks" "$failures"
[ "$failures" -eq 0 ]
