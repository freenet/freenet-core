#!/bin/bash
# macOS DMG-swap updater. Launched detached by `freenet update` when the
# running process is inside a .app bundle. Waits for the running Freenet
# to exit (so CURRENT_APP is no longer in use), atomically swaps the
# bundle with the staged copy, then relaunches the new bundle.
#
# Usage (called from Rust):
#   /bin/bash macos-bundle-updater.sh <current_app> <staged_app> <log_file>
#
# where:
#   current_app: absolute path of the installed bundle
#                (e.g. /Applications/Freenet.app)
#   staged_app:  absolute path of the new bundle, staged as a SIBLING of
#                current_app so the final mv stays on one APFS volume
#                (e.g. /Applications/.Freenet.app.staging.12345)
#   log_file:    absolute path of a file to append diagnostic log lines
#                to (e.g. ~/Library/Caches/Freenet/updater/updater.log)
#
# Exit codes:
#   0  swap + relaunch succeeded
#   1  refused to swap because Freenet was still running past deadline
#      (the old bundle is untouched; user is not left without an install)
#   2  swap failed; bundle has been restored from backup
#   3  swap failed AND restore failed (user's install may be broken)
#   4  swap succeeded but relaunch via /usr/bin/open failed

set -u

CURRENT_APP="${1:-}"
STAGED_APP="${2:-}"
LOG_FILE="${3:-/tmp/freenet-macos-updater.log}"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "$LOG_FILE" 2>/dev/null || true
}

if [[ -z "$CURRENT_APP" || -z "$STAGED_APP" ]]; then
    log "ERROR: usage: $0 <current_app> <staged_app> [log_file]"
    exit 1
fi

log "Updater started: CURRENT_APP=$CURRENT_APP STAGED_APP=$STAGED_APP pid=$$"

# Wait for every freenet process whose executable lives under CURRENT_APP
# to exit. Bounded deadline so a stuck child doesn't freeze the update
# indefinitely.
#
# The pattern uses pgrep's -f (match full command line) and ERE syntax.
# Regex metacharacters in CURRENT_APP are escaped so a path like
# `/Applications (Beta)/Freenet.app` doesn't break grouping.
escape_ere() {
    # Escape ERE metacharacters: \ . * + ? | ( ) [ ] { } ^ $
    printf '%s' "$1" | sed 's/[][\\.*+?|(){}^$]/\\&/g'
}
CURRENT_APP_RE="$(escape_ere "$CURRENT_APP")"

deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if ! pgrep -f "^${CURRENT_APP_RE}/Contents/MacOS/" > /dev/null 2>&1; then
        log "All Freenet processes under $CURRENT_APP have exited"
        break
    fi
    sleep 1
done

if pgrep -f "^${CURRENT_APP_RE}/Contents/MacOS/" > /dev/null 2>&1; then
    # REFUSE to swap. Historically this script would proceed anyway, but
    # mv'ing the old bundle aside + later rm'ing it while a descendant
    # still had libraries mmap'd from it would SEGV the running process
    # (skeptical review M2). Better to leave the install untouched and
    # let the next update attempt succeed once Freenet has exited.
    log "ERROR: timeout waiting for Freenet to exit; aborting swap to avoid crashing the running instance"
    # Clean up the staged bundle so the staging area doesn't accumulate.
    rm -rf "$STAGED_APP" 2>>"$LOG_FILE" || true
    exit 1
fi

if [[ ! -d "$STAGED_APP" ]]; then
    log "ERROR: staged bundle not found at $STAGED_APP; aborting"
    exit 1
fi

# Move the old bundle aside. Same-volume rename (APFS within the
# /Applications tree) is atomic on the kernel side.
BACKUP="${CURRENT_APP}.oldupdate.$$"
if [[ -e "$CURRENT_APP" ]]; then
    if ! mv "$CURRENT_APP" "$BACKUP" 2>>"$LOG_FILE"; then
        log "ERROR: failed to move current bundle aside; aborting"
        rm -rf "$STAGED_APP" 2>>"$LOG_FILE" || true
        exit 2
    fi
fi

# Move the staged bundle into place. Because STAGED_APP is a sibling of
# CURRENT_APP (per bundle_staging_path in update.rs), both operands are
# on the same APFS volume and rename(2) completes atomically.
if ! mv "$STAGED_APP" "$CURRENT_APP" 2>>"$LOG_FILE"; then
    log "ERROR: failed to move staged bundle into place; attempting rollback"
    if [[ -d "$BACKUP" ]]; then
        if ! mv "$BACKUP" "$CURRENT_APP" 2>>"$LOG_FILE"; then
            log "CRITICAL: failed to restore backup; bundle is at $BACKUP"
            exit 3
        fi
    fi
    exit 2
fi

# Success. Remove the backup.
if [[ -d "$BACKUP" ]]; then
    rm -rf "$BACKUP" 2>>"$LOG_FILE" || true
fi

log "Bundle swap complete. Launching new bundle: $CURRENT_APP"

# `open` re-enters LaunchServices bundle resolution: icon, quarantine-
# approval check, Gatekeeper re-scan of the stapled notarization, NSApp
# setup, and (for LSUIElement apps) menu-bar status item registration.
# This is why we can't just exec the inner binary directly.
if ! /usr/bin/open "$CURRENT_APP" 2>>"$LOG_FILE"; then
    log "ERROR: /usr/bin/open failed; new bundle is installed but not running"
    exit 4
fi

log "Updater complete"
exit 0
