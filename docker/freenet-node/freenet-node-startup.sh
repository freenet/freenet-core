#!/bin/sh
#
# Freenet node container entrypoint and update supervisor.
#
# WHY THIS EXISTS
#
# Freenet ships releases frequently, sometimes several times a day, and peers
# are expected to converge on a new release within hours. Falling behind is not
# merely cosmetic: `min-compatible-version` is enforced as a hard gate at the
# transport handshake (crates/core/src/transport/connection_handler/
# version_cmp.rs), so a node below the floor has its connections refused and is
# cut off from the network.
#
# The node does not update itself unattended. It detects a new release and exits
# with code 42, expecting a supervisor to run `freenet update` and restart it.
# Every other platform provides one: systemd on Linux, a launchd wrapper on
# macOS, the tray wrapper on Windows. A container has none, which is why this
# script exists. It is the container's equivalent of the generated systemd unit
# and deliberately mirrors that unit's contract (see
# crates/core/src/bin/commands/service/linux.rs):
#
#   Restart=always                    -> the supervise loop below
#   RestartSec=10 .. RestartMaxDelaySec=300
#                                     -> restart_delay(), growing backoff
#   SuccessExitStatus=42 43           -> 42 and 43 are not failures
#   RestartPreventExitStatus=43       -> exit 43 stops rather than restarts
#   ExecStopPost=... case 0|43) ;; *) freenet update --quiet
#                                     -> update_after_exit(), same 0|43 skip set
#   Environment=FREENET_SUPERVISED=1  -> exported before running the node
#
# The exit status is forwarded to `freenet update` in
# FREENET_POST_STOP_EXIT_CODE, exactly as the unit does, so the updater's
# crash-probation and automatic-rollback logic (crates/core/src/bin/commands/
# rollback.rs) works in a container too.
#
# WHERE THE BINARY LIVES
#
# The image ships a pristine binary at $FREENET_IMAGE_BIN and never writes to
# it, so the image layer stays immutable. The node RUNS from a copy on the data
# volume, because `freenet update` replaces std::env::current_exe() via a temp
# file plus rename(2) on the same filesystem and therefore needs a writable
# location. Keeping it on the volume also means an applied update survives
# container recreation instead of silently reverting to the image's version.
# On start the newer of (image binary, volume binary) wins, so pulling a newer
# image still upgrades a node whose volume holds an older self-updated binary.

set -eu

CONFIG_DIR="${FREENET_CONFIG_DIR:-/data/config}"
DATA_DIR="${FREENET_DATA_DIR:-/data/node}"
BIN_DIR="${FREENET_BIN_DIR:-/data/bin}"
# Kept on the volume so logs survive a container recreate and are present for
# `freenet service report`. Bounded by the node's own log-directory budget.
LOG_DIR="${LOG_DIR:-/data/logs}"
# Read by healthcheck.sh: which process is ours, and whether an update is in
# flight. Without the pid the healthcheck cannot tell our node from any other
# process answering on the API port, which matters under `network_mode: host`.
PID_FILE="${FREENET_PID_FILE:-/data/node.pid}"
UPDATING_FILE="${FREENET_UPDATING_FILE:-/data/updating}"
IMAGE_BIN="${FREENET_IMAGE_BIN:-/usr/local/lib/freenet/freenet}"
RUN_BIN="${BIN_DIR}/freenet"

# The node keeps some state at paths derived from $HOME with no CLI flag to
# move them: the auto-updater's GitHub poll bucket and, more importantly, the
# rollback subsystem's known-good binary copy and probation markers
# (auto_update.rs `state_dir`, rollback.rs). gosu preserves the environment, so
# without this the node would inherit HOME=/root from Docker and write them into
# the container's writable layer, where they are destroyed on every recreate.
# That would silently disable crash-rollback across exactly the upgrade the
# rollback exists to protect.
HOME="${FREENET_HOME_DIR:-/data/home}"
export HOME

# Mirrors RestartSec / RestartMaxDelaySec from the systemd unit. The unit grows
# the delay over RestartSteps=10 restarts; this doubles instead, which reaches
# the same ceiling sooner.
#
# DIVERGENCE, deliberate: the systemd unit gives up. StartLimitBurst=5 over
# StartLimitIntervalSec=120 with StartLimitAction=none puts a crash-looping unit
# into a terminal `failed` state. This does not, because stopping is not
# available to a container in the same way: `restart: unless-stopped` would
# restart the container anyway, and a fresh supervisor would start over with no
# memory of the previous attempts. Retrying at the ceiling, with an update
# attempt each time, is the closest useful behaviour, since a later fixed
# release is what heals a wedged node.
RESTART_DELAY_MIN="${FREENET_RESTART_DELAY_MIN:-10}"
RESTART_DELAY_MAX="${FREENET_RESTART_DELAY_MAX:-300}"

# A run lasting at least this long counts as healthy and resets the backoff, so
# an occasional restart days apart does not accumulate toward the ceiling.
HEALTHY_RUN_SECS="${FREENET_HEALTHY_RUN_SECS:-600}"

# Below this uptime, an exit 42 is treated as a crash rather than as a routine
# update restart. See the backoff decision in the supervise loop.
UPDATE_MIN_UPTIME_SECS="${FREENET_UPDATE_MIN_UPTIME_SECS:-30}"

# Percentage of jitter applied to each restart delay. Set to 0 only for tests
# that assert an exact delay; a real deployment wants the spread.
RESTART_JITTER_PCT="${FREENET_RESTART_JITTER_PCT:-20}"

# Set by update_after_exit so the backoff decision can tell "an update was
# applied" from "an update was wanted and failed".
last_update_failed=0

log() {
    echo "[freenet-entrypoint] $*"
}

# Prints the semantic version of the binary at $1, or nothing if it is missing
# or will not run (wrong architecture, truncated download, partially written
# update). Callers treat "no version" as "needs replacing", which is what makes
# a corrupt volume binary self-heal from the image copy.
binary_version() {
    [ -x "$1" ] || return 0
    "$1" --version 2>/dev/null \
        | sed -n 's/^Freenet version: \([^ ][^ ]*\).*/\1/p' \
        | head -n 1
}

# Copy the image binary into place atomically. The temp file is created in the
# destination directory so the rename cannot cross a filesystem boundary, and a
# reader never observes a half-written binary.
install_image_binary() {
    # Clear leftovers first: an interrupted earlier attempt (signal, ENOSPC,
    # container killed mid-copy) leaves a binary-sized temp file behind, and
    # nothing else ever removes it.
    rm -f "${RUN_BIN}".new.* 2>/dev/null || true
    tmp="${RUN_BIN}.new.$$"
    cp "$IMAGE_BIN" "$tmp"
    chmod 0755 "$tmp"
    mv -f "$tmp" "$RUN_BIN"
}

seed_binary() {
    image_version="$(binary_version "$IMAGE_BIN" || true)"
    run_version="$(binary_version "$RUN_BIN" || true)"

    if [ -z "$image_version" ]; then
        log "FATAL: image binary ${IMAGE_BIN} is missing or unrunnable"
        exit 1
    fi

    if [ -z "$run_version" ]; then
        log "seeding ${RUN_BIN} from image binary ${image_version}"
        install_image_binary
        return
    fi

    if [ "$image_version" = "$run_version" ]; then
        return
    fi

    # `sort -V` orders semantic versions; the image only wins when it is
    # strictly newer, so a node that has self-updated past the image's version
    # is never dragged backwards by a container restart.
    #
    # Caveat: `sort -V` orders a pre-release AFTER its release (0.3.0 then
    # 0.3.0-rc.1), which is the opposite of semver. Freenet releases are plain
    # X.Y.Z, so this does not arise today, and the consequence is bounded if it
    # ever does: the worst case keeps a pre-release that the node's own updater
    # then replaces on its next check, since `freenet update` compares with a
    # real semver implementation rather than with `sort`. One extra update
    # cycle, not a stuck node.
    newest="$(printf '%s\n%s\n' "$image_version" "$run_version" | sort -V | tail -n 1)"
    if [ "$newest" = "$image_version" ]; then
        # NOTE: this path bypasses `freenet update`, so it does not arm the
        # crash-rollback snapshot the way a self-update does. If a pulled image
        # ships a binary that crash-loops, there is no known-good copy to fall
        # back to until the node next updates itself. Writing rollback state
        # from shell would couple this script to that subsystem's on-disk
        # format, which is a worse trade; an operator who pulled a bad image can
        # pull the previous tag. Recorded so the gap is a known one.
        log "image binary ${image_version} is newer than ${run_version} on the volume; replacing"
        install_image_binary
    else
        log "keeping self-updated binary ${run_version} (image ships ${image_version})"
    fi
}

update_after_exit() {
    # NOT named exit_code: shell functions share the caller's scope, and the
    # supervise loop below holds the node's status in exit_code. Reusing the
    # name silently overwrote it.
    stop_status="$1"
    case "$stop_status" in
        0 | 43)
            # Same skip set as the unit's ExecStopPost. 0 is a clean stop and 43
            # is "another instance already holds the port", neither of which an
            # update would fix.
            return 0
            ;;
    esac
    if [ -n "${FREENET_DISABLE_AUTO_UPDATE:-}" ]; then
        # The operator opted out. Updating here anyway would honour the flag on
        # the ordinary path and quietly ignore it on the crash path, which is
        # the half an operator is least likely to be watching.
        log "not updating after exit ${stop_status}: auto-update is disabled by configuration"
        return 0
    fi
    log "applying update after exit ${stop_status}"
    # Marks the node as legitimately down so the healthcheck does not report the
    # container unhealthy for the duration of a normal update.
    # `touch`, not `: >`: a failed redirection on the `:` special builtin makes
    # the shell exit outright, so an unwritable path would kill the supervisor
    # rather than skip the marker.
    touch "$UPDATING_FILE" 2>/dev/null || true
    if FREENET_POST_STOP_EXIT_CODE="$stop_status" "$RUN_BIN" update --quiet; then
        log "update step completed"
        last_update_failed=0
    else
        last_update_failed=1
        # Never fatal. No network, GitHub unreachable, or already current all
        # land here, and the node must still be restarted.
        log "update step did not complete; continuing with the current binary"
    fi
    rm -f "$UPDATING_FILE" 2>/dev/null || true
}

# Applies +/-20% jitter, matching the repo's retry/backoff rule. That rule is
# scoped to Rust, but its reason applies here with more force rather than less:
# if a release ships that wedges the node at boot, every container in the world
# is crash-looping on the same schedule, because they all restarted when that
# release landed. Deterministic backoff keeps them in lockstep and has them poll
# GitHub together for as long as the bad release is out.
#
# Jitter is skipped for a zero delay, which is what the tests use, so the logged
# delay stays exactly predictable there.
jitter() {
    base="$1"
    if [ "$base" -le 0 ] || [ "$RESTART_JITTER_PCT" -le 0 ]; then
        echo "$base"
        return 0
    fi
    spread=$(( base * RESTART_JITTER_PCT / 100 ))
    if [ "$spread" -le 0 ]; then
        spread=1
    fi
    rand=$(od -An -N2 -tu2 < /dev/urandom | tr -d ' ')
    # 0..2*spread, shifted down by spread so the range is centred on base.
    offset=$(( (rand % (2 * spread + 1)) - spread ))
    result=$(( base + offset ))
    if [ "$result" -le 0 ]; then
        result=1
    fi
    echo "$result"
}

restart_delay() {
    consecutive="$1"
    delay="$RESTART_DELAY_MIN"
    i=1
    while [ "$i" -lt "$consecutive" ]; do
        delay=$((delay * 2))
        if [ "$delay" -ge "$RESTART_DELAY_MAX" ]; then
            jitter "$RESTART_DELAY_MAX"
            return 0
        fi
        i=$((i + 1))
    done
    jitter "$delay"
}

# Sourced by the unit tests in test-entrypoint.sh, which exercise the helpers
# above without starting a node.
if [ -n "${FREENET_ENTRYPOINT_SOURCE_ONLY:-}" ]; then
    return 0
fi

# ---------------------------------------------------------------------------
# Privilege drop
# ---------------------------------------------------------------------------
#
# A freshly created named volume is root-owned, so the directories have to be
# initialized before dropping privileges. Re-exec'ing this same script under
# gosu (rather than duplicating the supervise loop) means the loop, the update
# step and the node all run unprivileged, and `freenet update` writes to a
# directory the running user owns.
if [ "$(id -u)" -eq 0 ]; then
    mkdir -p "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME"
    # Only the directories themselves, and only when the owner is actually
    # wrong. A recursive chown walks the whole contract store and database on
    # every start, and this image restarts the node far more often than a
    # package install does. It would also rewrite ownership inside a
    # bind-mounted host directory, which is not what `-v /srv/freenet:/data`
    # is asking for. A first run has empty directories, so the recursive pass
    # bought nothing that this does not.
    for dir in "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME"; do
        if [ "$(stat -c '%u:%g' "$dir")" != "1000:1000" ]; then
            chown --no-dereference freenet:freenet "$dir"
        fi
    done
    exec gosu freenet "$0" "$@"
fi

# Reached either by the re-exec above or directly under `docker run --user`, in
# which case the caller owns the mounted paths and is responsible for making
# them writable.
for dir in "$CONFIG_DIR" "$DATA_DIR" "$BIN_DIR" "$LOG_DIR" "$HOME"; do
    if ! mkdir -p "$dir" 2>/dev/null; then
        log "FATAL: cannot create ${dir} as uid $(id -u)."
        log "FATAL: with 'docker run --user', make the mounted /data writable by"
        log "FATAL: that user first, or point the *_DIR variables somewhere it owns."
        exit 1
    fi
done
seed_binary

# ---------------------------------------------------------------------------
# Auto-update opt-out
# ---------------------------------------------------------------------------
#
# Deliberately awkward and loud. A node that stops updating eventually falls
# below min-compatible-version and is refused by every peer, so this is only for
# a private or offline test network, never for a node on the real network.
auto_update_args=''
if [ -n "${FREENET_DISABLE_AUTO_UPDATE:-}" ]; then
    auto_update_args='--disable-auto-update'
    log "WARNING: auto-update is DISABLED by FREENET_DISABLE_AUTO_UPDATE."
    log "WARNING: this node will stay on its current version and will eventually"
    log "WARNING: be refused by peers once it falls below the network's minimum"
    log "WARNING: compatible version. Only do this on a private test network."
fi

# Tells the node a supervisor is present, so on detecting an update it logs an
# informational message and exits 42 for us to act on, rather than warning that
# it is unsupervised and will exit without updating.
export FREENET_SUPERVISED=1

# Opts the node in to the distinct fast-crash exit code 45 (#4551), so a node
# that dies within MIN_HEALTHY_UPTIME_FOR_UPDATE_EXIT reports 45 rather than
# reusing 42. Without it a boot wedge is indistinguishable from "an update is
# available", and the supervisor cannot tell a self-healing restart from a
# crash loop.
#
# The variable is named for systemd because the systemd unit was the first
# supervisor to qualify, but what it asserts is a CAPABILITY, not an identity:
# that this supervisor keeps 45 out of its success set (so it counts toward the
# crash backoff) and still runs `freenet update` on 42 OR 45. Both hold here:
# 45 falls through to the default arm below, which counts a failure, and
# update_after_exit runs for every code outside {0, 43}. Do not set this in a
# supervisor that does not honour both halves; the node deliberately keeps
# emitting 42 when it cannot confirm the supervisor understands 45.
export FREENET_SYSTEMD_FAST_CRASH=1

# Derived from the arguments the node is actually given, NOT re-derived from
# the environment. A banner that recomputes this can disagree with the flag it
# is describing, and "auto-update: enabled" over a node that was passed
# --disable-auto-update is worse than no banner at all.
if [ -n "$auto_update_args" ]; then
    auto_update_state='DISABLED'
else
    auto_update_state='enabled'
fi

# Do not claim loopback-only when the operator has bound the API elsewhere. That
# API is fully privileged, so a banner that misreports its reach is worse than
# one that says nothing.
if [ -n "${FREENET_WS_API_ADDRESS:-}" ]; then
    ws_api_description="${FREENET_WS_API_ADDRESS}:${WS_API_PORT:-7509} (NOT loopback-only; this API is fully privileged)"
else
    ws_api_description="127.0.0.1:${WS_API_PORT:-7509} (loopback only)"
fi

cat <<BANNER
[freenet-entrypoint] Freenet node container
[freenet-entrypoint]   version      : $(binary_version "$RUN_BIN")
[freenet-entrypoint]   image ships  : $(binary_version "$IMAGE_BIN")
[freenet-entrypoint]   binary       : ${RUN_BIN}
[freenet-entrypoint]   config dir   : ${CONFIG_DIR}
[freenet-entrypoint]   data dir     : ${DATA_DIR}
[freenet-entrypoint]   log dir      : ${LOG_DIR}
[freenet-entrypoint]   home         : ${HOME}
[freenet-entrypoint]   transport    : UDP ${NETWORK_PORT:-31337}
[freenet-entrypoint]   client API   : ${ws_api_description}
[freenet-entrypoint]   auto-update  : ${auto_update_state}
BANNER

# ---------------------------------------------------------------------------
# Supervise loop
# ---------------------------------------------------------------------------

node_pid=''
stopping=0

# The node drains in-flight operations on SIGTERM (shutdown_drain_secs, 30s by
# default). This shell is PID 1, so without forwarding the signal the node would
# never see the stop and would be SIGKILLed when the grace period expired,
# aborting every in-flight operation on every restart.
forward_signal() {
    stopping=1
    if [ -n "$node_pid" ]; then
        kill -TERM "$node_pid" 2>/dev/null || true
    fi
}
trap forward_signal TERM INT

consecutive_failures=0

while :; do
    # Re-checked every iteration, not just at startup. A failed or interrupted
    # update can leave an unrunnable binary on the volume, and without this the
    # node would fail, the update step would fail too, and the container would
    # back off forever with a perfectly good binary sitting in the image. This
    # cannot undo a successful update: seed_binary only replaces the volume copy
    # when the image is strictly newer or the volume copy will not run.
    seed_binary

    started_at="$(date +%s)"

    # --config-dir and --data-dir are passed EXPLICITLY and must stay that way.
    # This script's FREENET_CONFIG_DIR / FREENET_DATA_DIR are its own variables;
    # the node's clap env names are CONFIG_DIR and DATA_DIR (config.rs
    # ConfigPathsArgs). Dropping these flags and relying on the environment made
    # the node fall back to $HOME, so contracts, delegates, secrets, the db and
    # the peer identity all landed in the container's writable layer and were
    # destroyed by `docker compose pull && docker compose up -d`. The volume
    # looked correct from the outside: /data/config and /data/node existed, were
    # owned by the right user, and stayed empty.
    #
    # shellcheck disable=SC2086
    # auto_update_args is intentionally word-split: it is either empty or a
    # single flag, and quoting it would pass an empty argument to the node.
    "$RUN_BIN" network \
        --config-dir "$CONFIG_DIR" \
        --data-dir "$DATA_DIR" \
        $auto_update_args "$@" &
    node_pid=$!
    echo "$node_pid" > "$PID_FILE" 2>/dev/null || true
    # A stop signal arriving between the `&` above and this assignment would
    # have seen an empty node_pid and signalled nothing, leaving the node to be
    # SIGKILLed with no drain. Narrow, but free to close.
    if [ "$stopping" -eq 1 ]; then
        kill -TERM "$node_pid" 2>/dev/null || true
    fi

    # `wait` returns 128+signum as soon as a trapped signal arrives, while the
    # node is still draining. Keep waiting until the child has actually gone,
    # otherwise the real exit status is lost and the drain is cut short.
    while :; do
        if wait "$node_pid"; then
            exit_code=0
        else
            exit_code=$?
        fi
        if [ "$exit_code" -gt 128 ] && kill -0 "$node_pid" 2>/dev/null; then
            continue
        fi
        break
    done
    node_pid=''
    rm -f "$PID_FILE" 2>/dev/null || true

    if [ "$stopping" -eq 1 ]; then
        log "node stopped for container shutdown (exit ${exit_code})"
        exit 0
    fi

    ran_for=$(( $(date +%s) - started_at ))

    case "$exit_code" in
        0)
            log "node exited cleanly; stopping"
            exit 0
            ;;
        43)
            # RestartPreventExitStatus=43. Another instance holds the port, so
            # restarting would loop forever without ever making progress.
            log "another Freenet instance is already running (exit 43); stopping"
            exit 43
            ;;
        42)
            log "update available (exit 42) after ${ran_for}s"
            ;;
        *)
            log "node exited with code ${exit_code} after ${ran_for}s"
            ;;
    esac

    update_after_exit "$exit_code"

    # The update step runs in the foreground, so a stop signal can arrive during
    # it. Without this check the container would sit through the whole restart
    # backoff before noticing, delaying shutdown by up to RESTART_DELAY_MAX.
    if [ "$stopping" -eq 1 ]; then
        log "shutdown requested during the update step"
        exit 0
    fi

    # An update restart is expected and routine, so it must not count toward the
    # crash backoff. Only genuine failures do.
    #
    # But exit 42 is not only "an update is available": the fatal-listener path
    # reuses the same code (FATAL_LISTENER_EXIT_CODE, p2p_impl.rs) so that the
    # systemd ExecStopPost update hook fires on a wedge. Treating every 42 as
    # routine therefore let a node that wedges immediately at boot restart every
    # RESTART_DELAY_MIN seconds forever, with a GitHub round-trip each time and
    # no backoff growth. A 42 only counts as routine if the node actually ran
    # for a while first; an instant one is treated as the crash it is.
    if [ "$exit_code" = 42 ] && [ "$ran_for" -lt "$UPDATE_MIN_UPTIME_SECS" ]; then
        log "exit 42 after only ${ran_for}s; treating as a crash rather than a routine update"
        exit_code_is_routine=0
    elif [ "$exit_code" = 42 ] && [ "${last_update_failed:-0}" = 1 ]; then
        # An update was wanted and could not be installed. Restarting at the
        # floor delay would re-detect the same update and retry immediately,
        # hitting GitHub every RESTART_DELAY_MIN seconds indefinitely.
        log "exit 42 but the update did not install; backing off"
        exit_code_is_routine=0
    elif [ "$exit_code" = 42 ]; then
        exit_code_is_routine=1
    else
        exit_code_is_routine=0
    fi

    if [ "$exit_code_is_routine" = 1 ] || [ "$ran_for" -ge "$HEALTHY_RUN_SECS" ]; then
        consecutive_failures=0
        delay="$RESTART_DELAY_MIN"
    else
        consecutive_failures=$((consecutive_failures + 1))
        delay="$(restart_delay "$consecutive_failures")"
    fi

    log "restarting in ${delay}s"
    # Backgrounded so a stop signal arriving mid-wait is handled immediately
    # rather than after the full delay has elapsed.
    sleep "$delay" &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null || true
    if [ "$stopping" -eq 1 ]; then
        log "shutdown requested while waiting to restart"
        exit 0
    fi
done
