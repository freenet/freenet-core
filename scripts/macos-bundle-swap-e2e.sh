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
STUB_SRC="$REPO_ROOT/scripts/macos-bundle-swap-e2e-stub.c"

if [[ ! -x "$UPDATER" ]]; then
    echo "ERROR: updater script not found or not executable at $UPDATER" >&2
    exit 1
fi

if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "ERROR: this test must run on macOS (found $(uname -s))" >&2
    exit 1
fi

if [[ ! -f "$STUB_SRC" ]]; then
    echo "ERROR: stub source not found at $STUB_SRC" >&2
    exit 1
fi

WORKDIR="$(mktemp -d -t freenet-swap-e2e.XXXXXX)"
INSTALL_ROOT="$WORKDIR/install"
INSTALL_APP="$INSTALL_ROOT/Freenet.app"
STUB_BIN="$WORKDIR/freenet-bin-stub"

echo "Compiling stub binary..."
clang -O0 -o "$STUB_BIN" "$STUB_SRC"
chmod +x "$STUB_BIN"
# Sibling of INSTALL_APP so the kernel-atomic rename stays on one APFS
# volume (same contract the updater expects in production).
STAGED_APP="$INSTALL_ROOT/.Freenet.app.staging.$$"
LOG="$WORKDIR/updater.log"

mkdir -p "$INSTALL_ROOT"

cleanup() {
    # Kill any stray processes from our test bundles. No `^` anchor:
    # /usr/bin/open canonicalizes the bundle path through /var->/private/var,
    # so a post-swap relaunched process shows up as /private/var/... but
    # INSTALL_APP is /var/...; an anchored pattern would miss it.
    pkill -f "${INSTALL_APP}/Contents/MacOS/" 2>/dev/null || true
    pkill -f "${PREFLIGHT_APP:-__unset__}/Contents/MacOS/" 2>/dev/null || true
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
    # CFBundleExecutable execs the inner binary with args. Using `exec`
    # (not a plain call) is required so the running process's argv[0]
    # is the Mach-O stub at the bundle path — that is what pgrep -f in
    # the updater script matches against. A shell-script freenet-bin
    # would appear as `/bin/bash /path/...` under ps and silently break
    # the match.
    cat > "$dest/Contents/MacOS/Freenet" <<SH
#!/bin/bash
DIR="\$(cd "\$(dirname "\$0")" && pwd)"
exec "\$DIR/freenet-bin" "$marker" "$version" "$lifetime"
SH
    chmod +x "$dest/Contents/MacOS/Freenet"

    # Copy the compiled stub in place (not a shell script — see comment
    # above and scripts/macos-bundle-swap-e2e-stub.c for the rationale).
    cp "$STUB_BIN" "$dest/Contents/MacOS/freenet-bin"
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

# ── Preflight: does /usr/bin/open launch an unsigned bundle here? ──
#
# On a headless CI runner, /usr/bin/open against an unsigned bundle in
# $TMPDIR can return success without actually spawning the process.
# That's a LaunchServices environmental limitation, not a bug in the
# updater. We run a preflight to decide whether to enforce the post-
# swap "v2 process running" assertion. On dev machines and signed-
# build CI jobs, preflight passes and the assertion is enforced; on
# headless unsigned-bundle environments it is skipped with a warning
# so the swap-mechanics half of the test still blocks merges.

PREFLIGHT_APP="$WORKDIR/preflight/Preflight.app"
PREFLIGHT_MARKER="$WORKDIR/preflight-marker.txt"
mkdir -p "$PREFLIGHT_APP/Contents/MacOS"
cat > "$PREFLIGHT_APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key><string>Preflight</string>
    <key>CFBundleIdentifier</key><string>org.freenet.test.preflight</string>
    <key>CFBundleVersion</key><string>1</string>
    <key>CFBundleShortVersionString</key><string>1</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleExecutable</key><string>Preflight</string>
    <key>LSUIElement</key><true/>
</dict>
</plist>
PLIST
cat > "$PREFLIGHT_APP/Contents/MacOS/Preflight" <<SH
#!/bin/bash
DIR="\$(cd "\$(dirname "\$0")" && pwd)"
exec "\$DIR/Preflight-stub" "$PREFLIGHT_MARKER" "preflight" 3
SH
chmod +x "$PREFLIGHT_APP/Contents/MacOS/Preflight"
cp "$STUB_BIN" "$PREFLIGHT_APP/Contents/MacOS/Preflight-stub"
chmod +x "$PREFLIGHT_APP/Contents/MacOS/Preflight-stub"

echo "Preflight: can /usr/bin/open launch an unsigned bundle in this env?"
/usr/bin/open "$PREFLIGHT_APP" 2>&1 || true
launchservices_works=false
for i in $(seq 1 100); do
    if [[ -s "$PREFLIGHT_MARKER" ]]; then
        launchservices_works=true
        break
    fi
    sleep 0.1
done
if $launchservices_works; then
    echo "Preflight OK: /usr/bin/open launches unsigned bundles here."
else
    echo "Preflight SKIP: /usr/bin/open does not spawn unsigned bundles in this env."
    echo "  Swap-mechanics assertions will still run; post-swap relaunch assertion will be skipped."
fi
# Clean up preflight process if it's still around. No `^` anchor for the
# same reason as the cleanup trap — /usr/bin/open yields /private/var/...
pkill -f "${PREFLIGHT_APP}/Contents/MacOS/" 2>/dev/null || true

echo
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
# verify it here — but only when the preflight above showed that
# /usr/bin/open actually spawns unsigned bundles in this environment.
# On headless CI runners it doesn't, and the updater's own log shows
# the `open` call returning success without anything launching. That
# is a LaunchServices/CI-env limitation, not a regression in the code
# under test, so we skip the assertion with a loud note rather than
# pretend it ran.
if $launchservices_works; then
    # Verify relaunch via the marker file, not pgrep: when /usr/bin/open
    # launches a bundle sitting under $TMPDIR, macOS canonicalizes the
    # path through the /var -> /private/var symlink, so the running
    # process's argv[0] looks like /private/var/... while INSTALL_APP is
    # /var/... — the two forms don't compare equal under pgrep's ERE.
    # In production that mismatch doesn't arise (/Applications is not a
    # symlink) so the updater's own pgrep is fine; here we'd produce a
    # false negative if we reused it. The marker file the stub writes
    # on startup is the actual signal we care about: if it's present
    # AND contains "v2", then the swapped bundle ran.
    echo "Waiting for v2 startup marker (relaunch via /usr/bin/open)..."
    for i in $(seq 1 150); do
        if [[ -s "$V2_MARKER" ]]; then
            break
        fi
        sleep 0.1
    done
    if [[ ! -s "$V2_MARKER" ]]; then
        echo "ERROR: v2 startup marker never written after updater relaunch" >&2
        echo "  ps output:" >&2
        ps -ef | grep -i freenet | grep -v grep >&2 || echo "  (no matches)" >&2
        echo "  updater log:" >&2
        cat "$LOG" >&2
        exit 1
    fi
    run_version="$(cat "$V2_MARKER")"
    if [[ "$run_version" != "v2" ]]; then
        echo "ERROR: relaunched process reported version '$run_version', expected 'v2'" >&2
        cat "$LOG" >&2
        exit 1
    fi
else
    echo "(Relaunch assertion skipped: preflight said /usr/bin/open does not"
    echo " spawn unsigned bundles in this environment. Swap mechanics still"
    echo " verified above.)"
fi

echo "== Test 1 PASSED =="
echo
echo "Updater log (for the record):"
cat "$LOG"
