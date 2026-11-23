#!/usr/bin/env bash
# Restart a companion gateway unit some minutes AFTER its primary, once the
# primary has been up and healthy for a while (freenet-core#6336).
#
# Why: restarting both nova gateways within seconds of each other was measured
# to double the share of SUBSCRIBEs needing extra hops for 10+ hours. Staggering
# keeps one hub serving while the other re-converges.
#
# Run as root (systemd starts it via freenet-gateway-stagger.service, which
# gateway-auto-update.sh triggers after a successful primary deploy).
# Exit status is non-zero if the companion could not be restarted onto the
# installed binary, so the unit shows as `failed` (visible in
# `systemctl --failed` and the journal). It is never silent.
#
# Usage: gateway-staggered-restart.sh [--primary UNIT] [--companion UNIT]
#                                      [--min-primary-uptime SECS]
#                                      [--primary-timeout SECS] [--dry-run]
# Env overrides (for tests): INSTALL_PATH, SYSTEMCTL, PROC_ROOT.

set -euo pipefail

PRIMARY="freenet-gateway"
COMPANION="freenet-gateway-2"
# 10 min: a subscription lease expires after 8 min and renews every 2 min, so by
# then peers that were attached to the primary have re-attached through it.
MIN_PRIMARY_UPTIME=600
# Give up waiting for the primary this long after we were started.
PRIMARY_TIMEOUT=1500
POLL=15
SETTLE=20
DRY_RUN=false
INSTALL_PATH="${INSTALL_PATH:-/usr/local/bin/freenet}"
SYSTEMCTL="${SYSTEMCTL:-systemctl}"
PROC_ROOT="${PROC_ROOT:-/proc}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] gateway-staggered-restart: $*"; }
die() { log "ERROR: $*"; exit 1; }

while [[ $# -gt 0 ]]; do
    case "$1" in
        --primary) PRIMARY="$2"; shift 2 ;;
        --companion) COMPANION="$2"; shift 2 ;;
        --min-primary-uptime) MIN_PRIMARY_UPTIME="$2"; shift 2 ;;
        --primary-timeout) PRIMARY_TIMEOUT="$2"; shift 2 ;;
        --poll) POLL="$2"; shift 2 ;;
        --settle) SETTLE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true; shift ;;
        *) die "unknown argument: $1" ;;
    esac
done

for v in MIN_PRIMARY_UPTIME PRIMARY_TIMEOUT POLL SETTLE; do
    [[ "${!v}" =~ ^[0-9]+$ ]] || die "$v must be a non-negative integer, got '${!v}'"
done

# Seconds the unit has been continuously active, or empty if it is not active.
unit_uptime() {
    local unit="$1" state since now
    state=$("$SYSTEMCTL" show -p ActiveState --value "$unit.service" 2>/dev/null || true)
    [[ "$state" == "active" ]] || return 0
    since=$("$SYSTEMCTL" show -p ActiveEnterTimestampMonotonic --value "$unit.service" 2>/dev/null || true)
    [[ "$since" =~ ^[0-9]+$ ]] || return 0
    now=$(awk '{printf "%d", $1 * 1000000}' "$PROC_ROOT/uptime")
    echo $(( (now - since) / 1000000 ))
}

unit_prop() { "$SYSTEMCTL" show -p "$2" --value "$1.service" 2>/dev/null || true; }

# True if the unit's main process runs the binary that is on disk now (not a
# deleted inode left over from before the swap).
runs_installed_binary() {
    local unit="$1" pid exe
    pid=$("$SYSTEMCTL" show -p MainPID --value "$unit.service" 2>/dev/null || true)
    [[ "$pid" =~ ^[0-9]+$ && "$pid" != "0" ]] || return 1
    exe=$(readlink "$PROC_ROOT/$pid/exe" 2>/dev/null || true)
    [[ -n "$exe" && "$exe" != *"(deleted)" ]] || return 1
    [[ "$(readlink -f "$exe")" == "$(readlink -f "$INSTALL_PATH")" ]]
}

if runs_installed_binary "$COMPANION"; then
    log "$COMPANION already runs the installed binary; nothing to do"
    exit 0
fi

log "waiting for $PRIMARY to be active for >= ${MIN_PRIMARY_UPTIME}s (timeout ${PRIMARY_TIMEOUT}s)"
start=$SECONDS
while :; do
    up=$(unit_uptime "$PRIMARY")
    if [[ -n "$up" ]] && (( up >= MIN_PRIMARY_UPTIME )); then
        log "$PRIMARY has been active for ${up}s"
        break
    fi
    (( SECONDS - start < PRIMARY_TIMEOUT )) \
        || die "$PRIMARY not healthy (uptime '${up:-inactive}') after ${PRIMARY_TIMEOUT}s; NOT restarting $COMPANION, which stays on its old binary"
    sleep "$POLL"
done

if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY RUN] would run: $SYSTEMCTL restart $COMPANION.service"
    exit 0
fi

log "restarting $COMPANION"
"$SYSTEMCTL" restart "$COMPANION.service" || die "systemctl restart $COMPANION failed"

# The unit has Restart=always and RestartSec=10, so a process that crashes right
# after start is replaced by a new, active one and a single check would pass.
# Require the same main PID and an unchanged restart counter across the wait.
pid_before=$(unit_prop "$COMPANION" MainPID)
nr_before=$(unit_prop "$COMPANION" NRestarts)
sleep "$SETTLE"
state=$("$SYSTEMCTL" is-active "$COMPANION.service" 2>/dev/null || true)
[[ "$state" == "active" ]] || die "$COMPANION is '$state' ${SETTLE}s after restart"
pid_after=$(unit_prop "$COMPANION" MainPID)
nr_after=$(unit_prop "$COMPANION" NRestarts)
[[ "$pid_before" == "$pid_after" && "$nr_before" == "$nr_after" ]] \
    || die "$COMPANION is crash-looping: MainPID $pid_before -> $pid_after, NRestarts $nr_before -> $nr_after within ${SETTLE}s"
runs_installed_binary "$COMPANION" || die "$COMPANION is active but not running the installed binary $INSTALL_PATH"
log "OK: $COMPANION restarted onto the installed binary"
