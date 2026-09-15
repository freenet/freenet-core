#!/usr/bin/env bash
#
# freenet-node -- the supervised, self-updating Freenet node, for Nix.
#
# WHY THIS EXISTS, AND WHY IT DOES NOT RESOLVE RELEASES ITSELF
# ------------------------------------------------------------
# `freenet update` replaces the RUNNING binary in place: it resolves
# `std::env::current_exe()` and renames a freshly-downloaded file over it
# (`replace_binary`, crates/core/src/bin/commands/update.rs), after verifying
# `SHA256SUMS.txt` against the ed25519 `FREENET_RELEASE_PUBKEY` baked into the
# binary. `/nix/store` is read-only, so that proven updater cannot run from a
# store path.
#
# So nix does not own the running binary. It SEEDS one into a mutable state
# directory, once, and from then on the node self-updates from there exactly as
# it does on every other platform -- same signature verification, same rollback
# snapshot, same crash probation, same known-bad pinning.
#
# This script is a faithful port of the systemd unit the node generates for
# itself (`generate_user_service_file`, crates/core/src/bin/commands/service/
# linux.rs). The load-bearing property of that unit is that **systemd makes no
# update decisions**: it does not resolve tags, poll GitHub, or count releases.
# It restarts the node and runs `freenet update`; the UPDATER decides
# everything. Port that faithfully and this file needs no HTTP client, no tag
# parsing, no jitter and no release-poll interval.
#
#   IF YOU FIND YOURSELF WRITING UPDATE LOGIC IN THIS FILE, STOP. It belongs in
#   the updater, where it is tested, signature-checked and shared with every
#   other platform.
#
# The systemd directives this mirrors, and where each lands below:
#
#   Environment=FREENET_SUPERVISED=1  -> `export` below; the node logs calmly on
#                                       the exit-42 path instead of erroring
#                                       that nothing will apply the update
#                                       (#4580, auto_update.rs).
#   Restart=always / RestartSec=10    -> the `while` loop + `sleep`.
#   SuccessExitStatus=42 43           -> 42 and 43 are not counted as failures.
#   RestartPreventExitStatus=43       -> exit 43 stands down instead of looping.
#   ExecStopPost=... update --quiet   -> `run_updater`, for every exit except
#                                       0 and 43.
#   StartLimitBurst / IntervalSec     -> `count_failure`.
#
# Deliberate divergences from that unit, each for a stated reason:
#
#   * `Restart=always` restarts even on a clean exit 0. This wrapper EXITS on 0,
#     matching the macOS launchd wrapper (`generate_wrapper_script`,
#     service/macos.rs, "Normal shutdown"). systemd can distinguish an operator
#     `systemctl stop` from the service exiting by itself; a foreground wrapper
#     cannot, so restarting on 0 would make the node impossible to stop.
#   * `FREENET_SYSTEMD_FAST_CRASH` is deliberately NOT set. The node emits the
#     distinct fast-crash exit code 45 only when it sees that marker, and 45
#     wants `SuccessExitStatus` / `StartLimitBurst` handling this wrapper does
#     not reproduce exactly. Without it the node keeps emitting the
#     self-healing exit 42, which this wrapper does handle (auto_update.rs,
#     `SYSTEMD_FAST_CRASH_ENV_VAR`).
#   * `RestartSteps` / `RestartMaxDelaySec` (growing backoff) are not
#     reproduced: the restart delay is a flat `RestartSec`. That is exactly how
#     the unit itself degrades on systemd < 254, which silently ignores both
#     directives, so it is a behaviour the project already ships.
set -euo pipefail

# Store path of the nix-built binary used to SEED the mutable state directory.
# `nix/node.nix` assigns it immediately above this file's contents. The default
# keeps the file runnable -- and `shellcheck -x` clean -- on its own, which is
# what `scripts/nix-node-wrapper_test.sh` relies on.
: "${FREENET_NIX_SEED_BINARY:=}"

# RestartSec. Overridable so the wrapper's own test does not sleep for real;
# operators have no reason to change it.
RESTART_SECS="${FREENET_NODE_RESTART_SECS:-10}"

# StartLimitBurst / StartLimitIntervalSec (#4551): more than 5 counted failures
# inside 120s stops the wrapper, rather than restart-looping forever. As in the
# unit, StartLimitAction is "stop loudly", not "reboot the host".
START_LIMIT_BURST="${FREENET_NODE_START_LIMIT_BURST:-5}"
START_LIMIT_INTERVAL_SECS="${FREENET_NODE_START_LIMIT_INTERVAL_SECS:-120}"

# ---------------------------------------------------------------------------
# Locate (and, on first run only, seed) the mutable binary.
# ---------------------------------------------------------------------------
state_dir="${STATE_DIRECTORY:-}"
# systemd passes StateDirectory= as a colon-separated LIST; take the first.
state_dir="${state_dir%%:*}"
if [ -n "$state_dir" ]; then
    : # systemd already told us where to put state.
elif [ -n "${XDG_STATE_HOME:-}" ]; then
    state_dir="$XDG_STATE_HOME/freenet"
elif [ -n "${HOME:-}" ]; then
    state_dir="$HOME/.local/state/freenet"
else
    echo "freenet-node: neither STATE_DIRECTORY, XDG_STATE_HOME nor HOME is set, so there is nowhere to put a writable binary." >&2
    exit 78
fi

bin_dir="$state_dir/bin"
binary="$bin_dir/freenet"

if [ ! -e "$binary" ]; then
    if [ -z "$FREENET_NIX_SEED_BINARY" ]; then
        echo "freenet-node: no binary at $binary and no seed binary configured (FREENET_NIX_SEED_BINARY is empty)." >&2
        exit 78
    fi
    mkdir -p "$bin_dir"
    # Copied, never symlinked: the node must be able to rename a new file over
    # this path, and it must survive `nix-collect-garbage` removing the store
    # path this generation was built from.
    install -m 0755 "$FREENET_NIX_SEED_BINARY" "$binary"
    echo "freenet-node: seeded $binary from $FREENET_NIX_SEED_BINARY (the node owns it from now on and will update it in place)."
fi

# NEVER overwrite an existing binary from the store. After the first update the
# state-dir binary is a newer release than this derivation was built from, and
# re-seeding on every start would silently pin the node to the flake's version
# -- which is the whole failure this design exists to avoid.

# ---------------------------------------------------------------------------
# Supervise.
# ---------------------------------------------------------------------------

# Positive evidence for the node that SOMETHING will catch exit 42 and run
# `freenet update` before restarting it (#4580). Set only because the loop below
# genuinely honours that contract.
export FREENET_SUPERVISED=1

child=0

terminate() {
    trap - TERM INT HUP
    if [ "$child" -ne 0 ]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    exit 0
}
trap terminate TERM INT HUP

# ExecStopPost. Forwards the node's exit status so crash-loop auto-rollback
# (#4073) can tell a post-stop restart from a manual update and count crashes of
# a version still on probation. Never fatal: the unit's '-' prefix means the
# hook's own result never affects the restart.
run_updater() {
    FREENET_POST_STOP_EXIT_CODE="$1" "$binary" update --quiet || true
}

failures=()

count_failure() {
    local now kept=() t
    now="$(date +%s)"
    for t in ${failures[@]+"${failures[@]}"}; do
        if [ "$((now - t))" -lt "$START_LIMIT_INTERVAL_SECS" ]; then
            kept+=("$t")
        fi
    done
    kept+=("$now")
    failures=("${kept[@]}")
    if [ "${#failures[@]}" -gt "$START_LIMIT_BURST" ]; then
        echo "freenet-node: more than $START_LIMIT_BURST failed starts within ${START_LIMIT_INTERVAL_SECS}s. Stopping so this does not restart-loop forever; see the logs above for why the node is failing." >&2
        exit 1
    fi
}

while true; do
    # Backgrounded and `wait`ed rather than run in the foreground, so a SIGTERM
    # aimed at this wrapper alone runs the trap immediately instead of being
    # deferred until the node happens to exit.
    "$binary" network "$@" &
    child=$!
    exit_code=0
    wait "$child" || exit_code=$?
    child=0

    case "$exit_code" in
        0)
            echo "freenet-node: the node shut down cleanly; stopping."
            exit 0
            ;;
        43)
            # RestartPreventExitStatus=43: another instance already holds the
            # port and is healthy. Restarting would just lose the race again.
            # Reported as success, as SuccessExitStatus=42 43 does, and as the
            # macOS wrapper does.
            echo "freenet-node: another Freenet instance is already running; stopping."
            exit 0
            ;;
        129 | 130 | 141 | 143)
            # A DELIBERATE STOP IS NOT A CRASH (#5227). HUP/INT/PIPE/TERM under
            # the default disposition are what an operator's stop looks like,
            # and the node has no path that reports a FAULT by dying on one of
            # them. Running the updater here would score a clean stop as a
            # probation crash and could roll a healthy release back.
            echo "freenet-node: the node was terminated by a signal (status $exit_code); treating it as a deliberate stop."
            exit 0
            ;;
        42)
            # The node checked GitHub, verified a newer release exists and asked
            # to be updated. Not counted toward the crash-loop limit: an update
            # is the system working, not failing.
            echo "freenet-node: the node requested an update (exit 42); running 'freenet update'."
            run_updater "$exit_code"
            ;;
        *)
            # Panic (101), early-startup error (1), fast crash, signal-kill
            # codes the guard above does not cover. `freenet update` either
            # steps forward onto a release that fixes it, or -- for a version
            # still on probation -- counts the crash and eventually rolls back
            # (#4073).
            echo "freenet-node: the node exited $exit_code; running 'freenet update' to step forward or roll back." >&2
            run_updater "$exit_code"
            count_failure
            ;;
    esac

    sleep "$RESTART_SECS"
done
