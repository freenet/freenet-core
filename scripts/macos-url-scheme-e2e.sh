#!/bin/bash
# End-to-end check of the freenet:// scheme handler on macOS (#5726).
#
# Builds an unsigned single-architecture Freenet.app with the real
# package-macos.sh, registers it with LaunchServices, and checks that:
#   1. Info.plist declares the freenet URL scheme;
#   2. LaunchServices resolves freenet:// links to this bundle;
#   3. `open freenet://...` launches the app and the menu-bar wrapper
#      handles the link (its log records the outcome), for a valid link and
#      an invalid one, and that a second link reaches the RUNNING instance
#      rather than a second copy.
#
# Usage: scripts/macos-url-scheme-e2e.sh <path-to-freenet-binary>
# Runs on a macOS CI runner. It launches Freenet.app (which starts a node)
# and quits it at the end.

set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "macos-url-scheme-e2e.sh: requires macOS" >&2
    exit 1
fi

BIN="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
RIVER="raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv"
LSREGISTER=/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister
LOG_DIR="$HOME/Library/Logs/freenet"

fail() {
    echo "FAIL: $*" >&2
    echo "--- wrapper log ---" >&2
    cat "$LOG_DIR"/freenet-wrapper.*.log >&2 2>/dev/null || true
    exit 1
}

OUT="$(mktemp -d)"
VERSION=0.0.0-ci FREENET_ARM64_BIN="$BIN" FREENET_X86_BIN="$BIN" \
    FREENET_SINGLE_ARCH_BUNDLE=1 CREATE_DMG=false OUTPUT_DIR="$OUT" \
    "$REPO/scripts/package-macos.sh"
# LaunchServices may ignore bundles under temp directories; install where a
# user would (per-user Applications folder, no admin needed).
mkdir -p "$HOME/Applications"
rm -rf "$HOME/Applications/Freenet.app"
ditto "$OUT/Freenet.app" "$HOME/Applications/Freenet.app"
APP="$HOME/Applications/Freenet.app"

# 1. The plist claims the scheme.
scheme="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleURLTypes:0:CFBundleURLSchemes:0' \
    "$APP/Contents/Info.plist")"
[[ "$scheme" == "freenet" ]] || fail "Info.plist scheme is '$scheme'"
plutil -lint "$APP/Contents/Info.plist" >/dev/null || fail "Info.plist does not lint"
echo "ok - Info.plist declares the freenet scheme"

# 2. LaunchServices maps freenet:// to this bundle.
"$LSREGISTER" -f -R "$APP"
resolve_handler() {
    osascript -l JavaScript -e '
ObjC.import("AppKit");
var u = $.NSWorkspace.sharedWorkspace.URLForApplicationToOpenURL(
    $.NSURL.URLWithString("freenet://x"));
u.isNil() ? "" : u.path.js;'
}
# Registration reaches lsd asynchronously; give it time.
handler=""
for _ in $(seq 1 30); do
    handler="$(resolve_handler)"
    [[ -n "$handler" ]] && break
    sleep 1
done
if [[ -z "$handler" ]]; then
    "$LSREGISTER" -dump 2>/dev/null | grep -n -B3 -A3 -i 'freenet' | head -60 >&2 || true
    fail "LaunchServices has no handler for freenet://"
fi
[[ "$(cd "$handler" && pwd -P)" == "$(cd "$APP" && pwd -P)" ]] \
    || fail "freenet:// resolves to $handler, not $APP"
echo "ok - LaunchServices resolves freenet:// to the bundle"

# 3. Clicking a link launches the app and the wrapper handles it. The dry run
# stops the handler opening a browser; it is set for apps LaunchServices
# starts in this session.
launchctl setenv FREENET_OPEN_DRY_RUN 1
# The app starts a real node. Keep it out of the production telemetry. (Its
# auto-update is off because CI builds this binary with FREENET_GIT_IS_DIRTY=1,
# so a newer release cannot swap the bundle out mid-test.)
launchctl setenv FREENET_TELEMETRY_ENABLED false
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/freenet-wrapper.*.log

handled_count() {
    cat "$LOG_DIR"/freenet-wrapper.*.log 2>/dev/null \
        | grep -c 'Handled a freenet:// link from LaunchServices' || true
}

wait_for_count() {
    local want="$1"
    # The app may have to start, and the handler waits up to 30s for the node.
    for _ in $(seq 1 180); do
        if [[ "$(handled_count)" -ge "$want" ]]; then
            return 0
        fi
        sleep 1
    done
    return 1
}

open "freenet://$RIVER/?x=1#y"
wait_for_count 1 || fail "the launched app never handled the link"
grep -q 'outcome=invalid-link' "$LOG_DIR"/freenet-wrapper.*.log \
    && fail "a valid link was treated as invalid"
echo "ok - a link launches Freenet.app and is handled"

# An all-'1' id is invalid on every platform. (Not a %2e%2e traversal: tao
# parses the link into a url::Url before the handler sees it, which already
# resolves dot segments, so that case is covered by the unit tests instead.)
open "freenet://11111111111111111111111111111111/"
# A second instance launched for this link would lose the single-instance
# lock and exit without handling it, so reaching count 2 proves the RUNNING
# instance received it.
wait_for_count 2 || fail "the running app never handled the second link"
grep -q 'outcome=invalid-link' "$LOG_DIR"/freenet-wrapper.*.log \
    || fail "an invalid link was not refused"
echo "ok - a second link reaches the running instance, and a hostile one is refused"

# 4. The instance started at login is launchd's, not LaunchServices'. Links
# must reach that one too. First run registered the Launch-at-Login agent;
# stop the app and start it the way login does.
AGENT="gui/$(id -u)/org.freenet.Freenet"
if [[ -f "$HOME/Library/LaunchAgents/org.freenet.Freenet.plist" ]]; then
    pkill -f "Freenet.app/Contents/MacOS/freenet-bin service run-wrapper" || true
    for _ in $(seq 1 30); do
        pgrep -f "Freenet.app/Contents/MacOS/freenet-bin service run-wrapper" >/dev/null || break
        sleep 1
    done
    launchctl kickstart -k "$AGENT" || fail "could not start the Launch-at-Login agent"
    for _ in $(seq 1 60); do
        pgrep -f "Freenet.app/Contents/MacOS/freenet-bin service run-wrapper" >/dev/null && break
        sleep 1
    done
    sleep 5
    open "freenet://$RIVER/"
    wait_for_count 3 || fail "the launchd-started instance never handled the link"
    echo "ok - a link reaches the instance launchd started (as at login)"
    launchctl bootout "$AGENT" >/dev/null 2>&1 || true
else
    echo "note - no Launch-at-Login agent was registered; skipped the launchd-started check"
fi

launchctl unsetenv FREENET_OPEN_DRY_RUN
launchctl unsetenv FREENET_TELEMETRY_ENABLED
osascript -e 'tell application id "org.freenet.Freenet" to quit' >/dev/null 2>&1 || true
echo "All macOS freenet:// checks passed."
