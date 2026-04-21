#!/bin/bash
# macOS DMG-swap updater. Launched detached by `freenet update` when the
# running process is inside a .app bundle. Waits for the running Freenet
# to exit (so /Applications/Freenet.app is no longer in use), atomically
# swaps the bundle with the staged copy, then relaunches the new bundle.
#
# Usage (called from Rust):
#   /bin/bash macos-bundle-updater.sh <current_app> <staged_app> <log_file>
#
# where:
#   current_app: absolute path of the installed bundle
#                (e.g. /Applications/Freenet.app)
#   staged_app:  absolute path of the newly-downloaded bundle
#                (e.g. ~/Library/Caches/Freenet/staging/Freenet.app)
#   log_file:    absolute path of a file to append diagnostic log lines
#                to (e.g. ~/Library/Caches/Freenet/updater/updater.log)
#
# Exit codes:
#   0  swap + relaunch succeeded
#   1  swap failed; bundle has been restored from backup
#   2  swap failed AND restore failed (user's install may be broken)

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

log "Updater started: CURRENT_APP=$CURRENT_APP STAGED_APP=$STAGED_APP"

# Wait for every freenet process whose executable lives under CURRENT_APP
# to exit. Bounded deadline so a stuck child doesn't freeze the update
# indefinitely. The regex matches both the shell wrapper CFBundleExecutable
# (/.../Freenet.app/Contents/MacOS/Freenet) and the inner binary
# (/.../Freenet.app/Contents/MacOS/freenet-bin), but NOT unrelated binaries
# that happen to contain "Freenet" in their path.
deadline=$(( $(date +%s) + 120 ))
while (( $(date +%s) < deadline )); do
    if ! pgrep -f "^${CURRENT_APP}/Contents/MacOS/" > /dev/null 2>&1; then
        log "All Freenet processes under $CURRENT_APP have exited"
        break
    fi
    sleep 1
done

if pgrep -f "^${CURRENT_APP}/Contents/MacOS/" > /dev/null 2>&1; then
    log "WARNING: timeout waiting for Freenet to exit; proceeding with swap anyway"
fi

if [[ ! -d "$STAGED_APP" ]]; then
    log "ERROR: staged bundle not found at $STAGED_APP; aborting"
    exit 1
fi

# Move the old bundle aside (atomic rename within the same filesystem).
# Name includes a timestamp so concurrent updates don't collide, though
# we expect at most one updater at a time.
BACKUP="${CURRENT_APP}.oldupdate.$$"
if [[ -e "$CURRENT_APP" ]]; then
    if ! mv "$CURRENT_APP" "$BACKUP" 2>>"$LOG_FILE"; then
        log "ERROR: failed to move current bundle aside; aborting"
        exit 1
    fi
fi

# Move the staged bundle into place.
if ! mv "$STAGED_APP" "$CURRENT_APP" 2>>"$LOG_FILE"; then
    log "ERROR: failed to move staged bundle into place; attempting rollback"
    if [[ -d "$BACKUP" ]]; then
        if ! mv "$BACKUP" "$CURRENT_APP" 2>>"$LOG_FILE"; then
            log "CRITICAL: failed to restore backup; bundle is at $BACKUP"
            exit 2
        fi
    fi
    exit 1
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
/usr/bin/open "$CURRENT_APP" 2>>"$LOG_FILE"

log "Updater complete"
