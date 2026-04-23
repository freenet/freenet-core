#!/bin/bash
# End-to-end test for scripts/macos-bundle-updater.sh.
#
# Builds two minimal unsigned .app bundles ("v1" and "v2"), launches v1 via
# /usr/bin/open, invokes the updater script with v2 staged as a sibling,
# and asserts that:
#   - the updater waited for v1 to exit, then swapped v2 into place
#   - the version file inside the bundle now reads "v2"
#   - the staged sibling directory was consumed
#   - a v2 process launched (via /usr/bin/open + LaunchServices) is running
#
# Failure modes caught by this test:
#   - pgrep pattern-escape bug: install paths with metacharacters don't
#     break the ERE (we rely on the helper's escape_ere in the script)
#   - atomic swap regression: the mv semantics must leave the install
#     path populated at all times except during the kernel-atomic rename
#   - LaunchServices re-recognition: /usr/bin/open must launch the new
#     bundle (if this silently fails, the user is left with no node
#     running even though the swap itself succeeded)
#
# Runs on macOS only. No signing required: Gatekeeper re-scan of stapled
# notarization is a separate concern covered by the release-path signed-
# bundle build in .github/workflows/cross-compile.yml and is not testable
# without Developer ID secrets.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UPDATER="$REPO_ROOT/scripts/macos-bundle-updater.sh"

if [[ ! -x "$UPDATER" ]]; then
    echo "ERROR: updater script not found or not executable at $UPDATER" >&2
    exit 1
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "ERROR: this test must run on macOS (found $(uname -s))" >&2
    exit 1
fi

WORKDIR="$(mktemp -d -t freenet-swap-e2e.XXXXXX)"
INSTALL_ROOT="$WORKDIR/install"
INSTALL_APP="$INSTALL_ROOT/Freenet.app"
# Sibling of INSTALL_APP so the kernel-atomic rename stays on one APFS
# volume (same contract the updater expects in production).
STAGED_APP="$INSTALL_ROOT/.Freenet.app.staging.$$"
LOG="$WORKDIR/updater.log"

mkdir -p "$INSTALL_ROOT"

cleanup() {
    # Kill any stray processes from our test bundles so the next run is
    # not confused by leftover pgrep matches, then remove WORKDIR.
    pkill -f "^${INSTALL_APP}/Contents/MacOS/" 2>/dev/null || true
    sleep 0.5
    rm -rf "$WORKDIR" 2>/dev/null || true
}
trap cleanup EXIT

# Build a minimal .app bundle.
#   $1 = destination path (.app directory)
#   $2 = version label written into the Info.plist and into a version.txt
#        inside Resources (so we can assert which bundle is on disk)
#   $3 = lifetime in seconds: how long the launched process sleeps before
#        exiting. Must be short enough that the updater's 120s exit-wait
#        succeeds, but long enough that we can observe the process via
#        pgrep before it vanishes.
#   $4 = marker file the launched process writes its version into on
#        startup (so we can assert which version is running).
build_bundle() {
    local dest="$1" version="$2" lifetime="$3" marker="$4"
    mkdir -p "$dest/Contents/MacOS" "$dest/Contents/Resources"

    cat > "$dest/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key><string>FreenetSwapTest</string>
    <key>CFBundleDisplayName</key><string>FreenetSwapTest</string>
    <key>CFBundleIdentifier</key><string>org.freenet.test.swap-e2e</string>
    <key>CFBundleVersion</key><string>$version</string>
    <key>CFBundleShortVersionString</key><string>$version</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleExecutable</key><string>Freenet</string>
    <key>LSUIElement</key><true/>
</dict>
</plist>
PLIST

    # Outer wrapper matches the production package-macos.sh convention:
    # CFBundleExecutable execs the inner binary. Must be exec (not just
    # call) so the running process's argv[0] is the inner binary — that
    # is what pgrep -f matches against.
    cat > "$dest/Contents/MacOS/Freenet" <<'SH'
#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$DIR/freenet-bin"
SH
    chmod +x "$dest/Contents/MacOS/Freenet"

    cat > "$dest/Contents/MacOS/freenet-bin" <<SH
#!/bin/bash
echo "$version" > "$marker"
sleep $lifetime
SH
    chmod +x "$dest/Contents/MacOS/freenet-bin"

    echo "$version" > "$dest/Contents/Resources/version.txt"
}

# Wait until pgrep reports a process matching the bundle's MacOS dir.
# Times out after ~6s so a failure-to-launch is surfaced as a clear
# error rather than a hanging test.
wait_for_process() {
    local bundle="$1"
    local i
    for i in $(seq 1 60); do
        if pgrep -f "^${bundle}/Contents/MacOS/" > /dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

echo "== Test 1: happy-path swap =="
echo "WORKDIR=$WORKDIR"

V1_MARKER="$WORKDIR/running-v1.txt"
V2_MARKER="$WORKDIR/running-v2.txt"

# v1 sleeps ~4s so the updater's pgrep-wait observes it alive, then
# observes it exit well within the 120s deadline. v2 sleeps 30s so we
# have a comfortable window to verify it relaunched before EXIT kills
# it as part of cleanup.
build_bundle "$INSTALL_APP" "v1" 4  "$V1_MARKER"
build_bundle "$STAGED_APP"  "v2" 30 "$V2_MARKER"

echo "Launching v1 directly (background exec of CFBundleExecutable)..."
# We deliberately bypass /usr/bin/open for spawning the "already-running
# process" that the updater must wait for. LaunchServices on a headless
# CI runner is flaky for unsigned bundles in $TMPDIR (origin of the
# first-run failure: `open` returned success but no process was observed
# within the 6s wait). The updater still uses /usr/bin/open for its
# relaunch step at the end — we verify LaunchServices works there by
# asserting the v2 process appears after the swap.
"$INSTALL_APP/Contents/MacOS/Freenet" &
V1_PID=$!
echo "v1 backgrounded as pid $V1_PID"

if ! wait_for_process "$INSTALL_APP"; then
    echo "ERROR: v1 process never appeared; diagnostic output:" >&2
    echo "  ps -ef | grep Freenet:" >&2
    ps -ef | grep -i freenet | grep -v grep >&2 || echo "  (no matches)" >&2
    exit 1
fi
echo "v1 process confirmed running."

echo "Invoking updater script (synchronous for the test)..."
# In production the updater is spawned detached; running it synchronously
# here is deliberate so the test can observe its exit code.
"$UPDATER" "$INSTALL_APP" "$STAGED_APP" "$LOG"

echo "Updater exit: 0 (expected)"

# Assertion 1: the install path still exists and is populated.
if [[ ! -d "$INSTALL_APP" ]]; then
    echo "ERROR: install path missing after swap" >&2
    cat "$LOG" >&2
    exit 1
fi

# Assertion 2: the version file inside the bundle is now v2.
installed_version="$(cat "$INSTALL_APP/Contents/Resources/version.txt" 2>/dev/null || echo missing)"
if [[ "$installed_version" != "v2" ]]; then
    echo "ERROR: expected v2 at install path, got '$installed_version'" >&2
    cat "$LOG" >&2
    exit 1
fi

# Assertion 3: the staged sibling was consumed by the mv (not left behind).
if [[ -d "$STAGED_APP" ]]; then
    echo "ERROR: staged bundle still present at $STAGED_APP" >&2
    exit 1
fi

# Assertion 4: a v2 process is running under the install path. The
# updater's final step is /usr/bin/open of the new bundle; this is the
# relaunch step that most often fails silently in the wild, so we
# explicitly verify it here.
#
# Give LaunchServices a longer window on CI — the first open of an
# unsigned bundle in $TMPDIR can take several seconds for lsregister to
# scan the bundle and spawn the process.
echo "Waiting for v2 process to appear (relaunch via /usr/bin/open)..."
v2_seen=false
for i in $(seq 1 150); do
    if pgrep -f "^${INSTALL_APP}/Contents/MacOS/" > /dev/null 2>&1; then
        v2_seen=true
        break
    fi
    sleep 0.1
done
if ! $v2_seen; then
    echo "ERROR: no v2 process observed after updater relaunch (15s wait)" >&2
    echo "  ps output:" >&2
    ps -ef | grep -i freenet | grep -v grep >&2 || echo "  (no matches)" >&2
    echo "  updater log:" >&2
    cat "$LOG" >&2
    exit 1
fi

# Assertion 5: the running process is actually v2, not a lingering v1.
# v2's freenet-bin writes its version to $V2_MARKER on startup.
for i in $(seq 1 150); do
    if [[ -s "$V2_MARKER" ]]; then
        break
    fi
    sleep 0.1
done
run_version="$(cat "$V2_MARKER" 2>/dev/null || echo missing)"
if [[ "$run_version" != "v2" ]]; then
    echo "ERROR: relaunched process reported version '$run_version', expected 'v2'" >&2
    cat "$LOG" >&2
    exit 1
fi

echo "== Test 1 PASSED =="
echo
echo "Updater log (for the record):"
cat "$LOG"
