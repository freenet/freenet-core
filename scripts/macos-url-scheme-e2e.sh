#!/bin/bash
# End-to-end check of Freenet.app on macOS: starting it, its menu bar icon,
# Launch at Login, and the freenet:// scheme handler (#5726, #6093).
#
# Builds an unsigned single-architecture Freenet.app with the real
# package-macos.sh, registers it with LaunchServices, and checks that:
#   1. Info.plist declares the freenet URL scheme;
#   2. LaunchServices resolves freenet:// links to this bundle;
#   2b. a fresh install starts through LaunchServices;
#   2c. its menu bar icon is really on screen, and it registers a
#      Launch-at-Login agent;
#   2d. an agent from before #6093 still starts the app with an icon, and is
#      repaired without the wrapper killing itself;
#   3. `open freenet://...` launches the app and the menu-bar wrapper
#      handles the link (its log records the outcome), for a valid link and
#      an invalid one, and that a second link reaches the RUNNING instance
#      rather than a second copy;
#   4. launchd's wrapper, started from the repaired agent, shows the icon and
#      receives links.
#
# The icon checks only prove anything on macOS 26 or later, so it fails on
# anything older.
#
# Usage: scripts/macos-url-scheme-e2e.sh <path-to-freenet-binary>
# Runs on a macOS CI runner. It launches Freenet.app (which starts a node)
# and quits it at the end.

set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "macos-url-scheme-e2e.sh: requires macOS" >&2
    exit 1
fi

# It replaces ~/Applications/Freenet.app, kills the running Freenet wrapper,
# rewrites the Launch-at-Login agent and sets session-wide launchctl variables:
# fine on a throwaway CI runner, destructive on a developer's Mac.
if [[ -z "${CI:-}" ]]; then
    echo "macos-url-scheme-e2e.sh: refusing to run outside CI (set CI=1 on a throwaway machine)" >&2
    exit 1
fi

# The wrapper, however it was started: LaunchServices passes no arguments,
# launchd passes the subcommand. Must not match `freenet-bin network`.
RUN_WRAPPER='Freenet.app/Contents/MacOS/freenet-bin( service run-wrapper)?$'
NODE='Freenet.app/Contents/MacOS/freenet-bin network'

cleanup() {
    for var in FREENET_OPEN_DRY_RUN FREENET_TELEMETRY_ENABLED DATA_DIR CONFIG_DIR; do
        launchctl unsetenv "$var" 2>/dev/null || true
    done
    pkill -f "$RUN_WRAPPER" 2>/dev/null || true
    pkill -f "$NODE" 2>/dev/null || true
}
trap cleanup EXIT

BIN="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
RIVER="raAqMhMG7KUpXBU2SxgCQ3Vh4PYjttxdSWd9ftV7RLv"
LSREGISTER=/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister
LOG_DIR="$HOME/Library/Logs/freenet"

fail() {
    echo "FAIL: $*" >&2
    echo "--- wrapper log ---" >&2
    cat "$LOG_DIR"/freenet-wrapper.*.log >&2 2>/dev/null || true
    echo "--- processes ---" >&2
    pgrep -fl 'freenet|Freenet' >&2 || true
    echo "--- ~/Library/Logs/freenet ---" >&2
    ls -la "$LOG_DIR" >&2 2>/dev/null || true
    echo "--- unified log (last 10m, Freenet + LaunchServices) ---" >&2
    log show --last 10m --style compact \
        --predicate 'process CONTAINS[c] "freenet" OR eventMessage CONTAINS[c] "org.freenet" OR eventMessage CONTAINS[c] "Freenet.app"' \
        2>/dev/null | grep -v 'com.apple.network' | tail -120 >&2 || true
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

# Session environment for apps LaunchServices and launchd start from now on.
# FREENET_OPEN_DRY_RUN: the handler prints instead of opening a browser.
# DATA_DIR / CONFIG_DIR: this is a DEBUG build, and debug builds only know a
# temp-dir default for these, which the service path refuses (a release
# build uses the platform directories); explicit directories sidestep that.
# FREENET_TELEMETRY_ENABLED: the app starts a real node; keep it out of the
# production telemetry. (Its auto-update is off because CI builds this binary
# with FREENET_GIT_IS_DIRTY=1, so a newer release cannot swap the bundle out
# mid-test.)
mkdir -p "$OUT/data" "$OUT/config"
launchctl setenv FREENET_OPEN_DRY_RUN 1
launchctl setenv FREENET_TELEMETRY_ENABLED false
launchctl setenv DATA_DIR "$OUT/data"
launchctl setenv CONFIG_DIR "$OUT/config"

# Stops the app and its node, which outlives a killed wrapper. Repeats the kill
# because a wrapper launchd started can take over the lock from the one killed.
stop_app() {
    for _ in $(seq 1 30); do
        pgrep -f "$RUN_WRAPPER" >/dev/null || pgrep -f "$NODE" >/dev/null || return 0
        pkill -f "$RUN_WRAPPER" || true
        pkill -f "$NODE" || true
        sleep 1
    done
    fail "the app did not stop"
}

# launchd can briefly refuse a label it is still tearing down after a bootout.
load_agent() {
    local error=""
    for _ in $(seq 1 10); do
        error="$(launchctl bootstrap "gui/$(id -u)" "$PLIST" 2>&1)" && return 0
        sleep 1
    done
    fail "could not load the Launch-at-Login agent: $error"
}

# `open` has failed with LaunchServices error -600 (no such process) once in CI,
# right after the previous instance was stopped, when LaunchServices can still
# be tracking it. It could also mean a duplicate launch exited before taking
# the link, which would be a lost link, so every repeat is logged where a
# reader of the CI log will see it (#5956). Any other error fails at once.
click_link() {
    local attempt error=""
    for attempt in 1 2 3 4 5; do
        error="$(open "$1" 2>&1)" && return 0
        [[ "$error" == *"-600"* ]] || break
        echo "note - open failed with -600 on attempt $attempt, repeating the click" >&2
        sleep 1
    done
    fail "open could not deliver $1: $error"
}

wait_for_status_item_count_above() {
    # A first launch of a debug build on a busy runner can take a while.
    for _ in $(seq 1 60); do
        [[ "$("$OUT/status-item-count")" -gt "$1" ]] && return 0
        sleep 1
    done
    return 1
}

# The item that appeared was Freenet's: it goes when Freenet goes. Without this
# anything else adding a status item on the runner would satisfy the check.
wait_for_status_item_count_back_to() {
    local count
    for _ in $(seq 1 15); do
        # An empty count would compare as zero, so a failed helper must not pass.
        count="$("$OUT/status-item-count")" || return 1
        [[ -n "$count" && "$count" -le "$1" ]] && return 0
        sleep 1
    done
    return 1
}

# The icon checks below only mean anything on an OS with the bug (#6093). On
# macOS 15 a broken bundle shows the icon as well.
sw_vers
[[ "$(sw_vers -productVersion | cut -d. -f1)" -ge 26 ]] \
    || fail "this must run on macOS 26 or later, or the icon checks prove nothing"
swiftc -O "$REPO/scripts/macos-status-item-count.swift" -o "$OUT/status-item-count"
status_items_before="$("$OUT/status-item-count")" || fail "could not count the status items"

AGENT="gui/$(id -u)/org.freenet.Freenet"
PLIST="$HOME/Library/LaunchAgents/org.freenet.Freenet.plist"
rm -f "$PLIST"

agent_starts_the_binary() {
    grep -qF "$APP/Contents/MacOS/freenet-bin" "$PLIST" 2>/dev/null \
        && grep -qF run-wrapper "$PLIST"
}

# 2b. A fresh install: can the app start at all here? Launch it directly (no
# link) first, so a failure below can be told apart: "the app cannot start in
# this environment" vs "the link was not delivered".
open -a "$APP" || fail "open -a could not launch the app"
started=false
for _ in $(seq 1 30); do
    if pgrep -f "$RUN_WRAPPER" >/dev/null; then started=true; break; fi
    sleep 1
done
if ! $started; then
    echo "--- the app did not start via open -a; running its executable directly ---" >&2
    ( DATA_DIR="$OUT/data" CONFIG_DIR="$OUT/config" "$APP/Contents/MacOS/freenet-bin" \
        > "$OUT/direct.log" 2>&1 & echo $! > "$OUT/direct.pid" )
    sleep 15
    head -c 4000 "$OUT/direct.log" >&2 || true
    kill "$(cat "$OUT/direct.pid")" 2>/dev/null || true
    codesign -dv --verbose=2 "$APP/Contents/MacOS/freenet-bin" >&2 2>&1 || true
    spctl -a -vv "$APP" >&2 2>&1 || true
    fail "Freenet.app does not start in this environment"
fi
echo "ok - Freenet.app starts via LaunchServices"

# 2c. The menu bar icon is really displayed. Started this way, macOS 26 hides
# the item of an app whose CFBundleExecutable execs the real binary, and the
# app has no way to tell (#6093).
wait_for_status_item_count_above "$status_items_before" \
    || fail "the menu bar icon is not displayed (before: $status_items_before)"
echo "ok - the menu bar icon is displayed"
agent_starts_the_binary || fail "the first launch did not register a Launch-at-Login agent"
launchctl print "$AGENT" >/dev/null 2>&1 || fail "launchd did not load the new Launch-at-Login agent"
echo "ok - the first launch registers a Launch-at-Login agent"
stop_app
wait_for_status_item_count_back_to "$status_items_before" \
    || fail "the status item outlived the app, so 2c did not observe Freenet's"
echo "ok - the status item was Freenet's"
launchctl bootout "$AGENT" >/dev/null 2>&1 || true

# 2d. A Mac updated from before #6093 whose relaunch failed: nothing has
# repaired its LaunchAgent, which runs the bundle's script, so its next login
# looks like this. Freenet must start, show its icon, and repair the agent
# without booting itself out.
cat > "$PLIST" <<XML
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>org.freenet.Freenet</string>
<key>ProgramArguments</key><array><string>$APP/Contents/MacOS/Freenet</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><false/>
<key>ProcessType</key><string>Interactive</string>
</dict></plist>
XML
load_agent
wait_for_status_item_count_above "$status_items_before" \
    || fail "the menu bar icon is not displayed when an old agent starts the app"
migrated=false
for _ in $(seq 1 15); do
    if agent_starts_the_binary; then
        migrated=true
        break
    fi
    sleep 1
done
$migrated || fail "the app left a Launch-at-Login agent naming the script"
wrapper_pid="$(pgrep -f "$RUN_WRAPPER" | sed -n 1p || true)"
[[ -n "$wrapper_pid" ]] || fail "repairing its Launch-at-Login agent ended the wrapper"
sleep 5
kill -0 "$wrapper_pid" 2>/dev/null || fail "repairing its Launch-at-Login agent killed the wrapper"
echo "ok - an old Launch-at-Login agent starts the app and is repaired"
stop_app
wait_for_status_item_count_back_to "$status_items_before" \
    || fail "the status item outlived the app, so it was not Freenet's"
launchctl bootout "$AGENT" >/dev/null 2>&1 || true

# 3. Clicking a link launches the app and the wrapper handles it. The dry run
# stops the handler opening a browser; it is set for apps LaunchServices
# starts in this session.
# (The session environment was set up before step 2b.)
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/freenet-wrapper.*.log

# Links handled by the running wrapper ("from LaunchServices") or by a
# duplicate launch that lost the single-instance lock ("in a duplicate
# launch"); both open the link.
handled_count() {
    cat "$LOG_DIR"/freenet-wrapper.*.log 2>/dev/null \
        | grep -c 'Handled a freenet:// link' || true
}

running_instance_count() {
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

click_link "freenet://$RIVER/?x=1#y"
wait_for_count 1 || fail "the launched app never handled the link"
# `opened` (not merely "not invalid"): the handler found the node the app
# started and it answered the /v1/version probe like Freenet.
grep -q 'outcome=opened' "$LOG_DIR"/freenet-wrapper.*.log \
    || fail "the link was not opened against the running node"
echo "ok - a link launches Freenet.app and is opened against its node"

# Let any duplicate launch exit after losing the lock, so the next link has
# only the settled app to go to.
for _ in $(seq 1 30); do
    [[ "$(pgrep -f "$RUN_WRAPPER" | wc -l | tr -d ' ')" -le 1 ]] && break
    sleep 1
done

# An all-'1' id is invalid on every platform. (Not a %2e%2e traversal: tao
# parses the link into a url::Url before the handler sees it, which already
# resolves dot segments, so that case is covered by the unit tests instead.)
click_link "freenet://11111111111111111111111111111111/"
wait_for_count 2 || fail "the running app never handled the second link"
# The app was already running and settled, so LaunchServices must have given
# the link to it, not to a duplicate launch.
[[ "$(running_instance_count)" -ge 2 ]] \
    || fail "the second link went to a duplicate launch, not the running app"
grep -q 'outcome=invalid-link' "$LOG_DIR"/freenet-wrapper.*.log \
    || fail "an invalid link was not refused"
echo "ok - a second link reaches the running instance, and a hostile one is refused"

# 4. The instance started at login is launchd's, not LaunchServices'. Links
# must reach that one too. Stop the app and load the agent repaired in 2d, as
# the next login will.
stop_app
wait_for_status_item_count_back_to "$status_items_before" \
    || fail "the status item outlived the app"
load_agent
# Nothing else can be running here, so the icon is launchd's wrapper's.
wait_for_status_item_count_above "$status_items_before" \
    || fail "the menu bar icon is not displayed when launchd starts the app"
pgrep -f 'Freenet.app/Contents/MacOS/freenet-bin service run-wrapper$' >/dev/null \
    || fail "the icon appeared but launchd's wrapper is not running"
echo "ok - the menu bar icon is displayed when launchd starts the app"
# The usual case: a link clicked some time after login.
click_link "freenet://$RIVER/"
wait_for_count 3 || fail "a link was not handled once launchd's wrapper had settled"
echo "ok - a link works once the instance launchd started has settled"
stop_app
wait_for_status_item_count_back_to "$status_items_before" \
    || fail "the status item outlived the app"
launchctl bootout "$AGENT" >/dev/null 2>&1 || true

load_agent
# Click at once, without waiting for the new wrapper to settle: the link must
# be handled whether LaunchServices already sees the launchd-started instance
# or launches a duplicate for it, and either copy may win the lock.
click_link "freenet://$RIVER/"
wait_for_count 4 || fail "the launchd-started instance never handled the link"
echo "ok - a link reaches the instance launchd started (as at login)"
launchctl bootout "$AGENT" >/dev/null 2>&1 || true

echo "All macOS freenet:// checks passed."
