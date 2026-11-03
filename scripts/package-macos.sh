#!/bin/bash
# Build a signed and notarized Freenet.app + Freenet.dmg from cross-
# compiled macOS binaries.
#
# Runs on macOS. Expects the following environment variables:
#
#   Required:
#     FREENET_ARM64_BIN   Path to aarch64-apple-darwin freenet binary
#     FREENET_X86_BIN     Path to x86_64-apple-darwin freenet binary
#     VERSION             Version string for the bundle (e.g. 0.2.48)
#
#   Optional (all three must be set together, or none, to enable signing):
#     CODESIGN_IDENTITY   "Developer ID Application: Ian CLARKE (R55ZESJCXG)"
#     ASC_API_KEY_PATH    Path to the App Store Connect API .p8 file
#     ASC_API_KEY_ID      10-char key ID (e.g. HZLB6C37WG)
#     ASC_API_ISSUER_ID   Issuer UUID from App Store Connect
#
#   Other optional:
#     FREENET_ARM64_FDEV_BIN  Path to aarch64-apple-darwin fdev binary.
#     FREENET_X86_FDEV_BIN    Path to x86_64-apple-darwin fdev binary.
#                             When BOTH are set, fdev is bundled alongside
#                             freenet (Contents/MacOS/fdev) so the wrapper's
#                             first-launch CLI symlink setup (service/
#                             cli_symlinks.rs) can put both `freenet` and
#                             `fdev` on PATH. When unset, the bundle ships
#                             without fdev (fine for a local .app smoke-test).
#     OUTPUT_DIR          Where to place artifacts (default: dist/macos)
#     ICON_ICNS           Path to a .icns file (default: generic app icon)
#     CREATE_DMG          "true" to produce a .dmg; "false" to stop at .app
#                         (default: true)
#     FREENET_SINGLE_ARCH_BUNDLE  "1" to bundle FREENET_ARM64_BIN alone, for an
#                         unsigned local/CI smoke-test .app (refused when
#                         signing is enabled)
#
# Produces:
#   $OUTPUT_DIR/Freenet.app        (signed + notarized if credentials set)
#   $OUTPUT_DIR/Freenet-$VERSION.dmg  (signed + notarized, if CREATE_DMG)
#
# Local smoke-test:
#   VERSION=0.2.48 \
#   FREENET_ARM64_BIN=target/aarch64-apple-darwin/release/freenet \
#   FREENET_X86_BIN=target/x86_64-apple-darwin/release/freenet \
#   ./scripts/package-macos.sh
#
# (No CODESIGN_IDENTITY = unsigned build; the .app still runs locally
# via right-click-Open but will fail Gatekeeper on a fresh download.)

set -euo pipefail

: "${FREENET_ARM64_BIN:?FREENET_ARM64_BIN is required}"
: "${FREENET_X86_BIN:?FREENET_X86_BIN is required}"
: "${VERSION:?VERSION is required}"
OUTPUT_DIR="${OUTPUT_DIR:-dist/macos}"
CREATE_DMG="${CREATE_DMG:-true}"
ICON_ICNS="${ICON_ICNS:-}"

# Sanity: refuse to run on non-macOS. Some of the tools below (codesign,
# xcrun notarytool, lipo, hdiutil, create-dmg) are macOS-only.
if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "package-macos.sh: this script requires macOS (found $(uname -s))" >&2
    exit 1
fi

# Validate that signing env vars are all set or all unset.
sign_vars_set=0
for v in CODESIGN_IDENTITY ASC_API_KEY_PATH ASC_API_KEY_ID ASC_API_ISSUER_ID; do
    if [[ -n "${!v:-}" ]]; then
        sign_vars_set=$((sign_vars_set + 1))
    fi
done
if [[ "$sign_vars_set" -ne 0 && "$sign_vars_set" -ne 4 ]]; then
    echo "package-macos.sh: signing env vars must all be set or all unset" >&2
    echo "  CODESIGN_IDENTITY=${CODESIGN_IDENTITY:-<unset>}" >&2
    echo "  ASC_API_KEY_PATH=${ASC_API_KEY_PATH:-<unset>}" >&2
    echo "  ASC_API_KEY_ID=${ASC_API_KEY_ID:-<unset>}" >&2
    echo "  ASC_API_ISSUER_ID=${ASC_API_ISSUER_ID:-<unset>}" >&2
    exit 1
fi
SIGNING_ENABLED=$([[ "$sign_vars_set" -eq 4 ]] && echo true || echo false)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENTITLEMENTS="$SCRIPT_DIR/macos-entitlements.plist"
if [[ ! -f "$ENTITLEMENTS" ]]; then
    echo "package-macos.sh: missing $ENTITLEMENTS" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
APP_DIR="$OUTPUT_DIR/Freenet.app"
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"

if [[ "${FREENET_SINGLE_ARCH_BUNDLE:-}" == "1" ]]; then
    # A single-architecture local/CI smoke-test bundle from FREENET_ARM64_BIN
    # alone (lipo refuses two inputs of one architecture). Opt-in only, and
    # refused when signing, so a release can never ship one by accident.
    if [[ "$SIGNING_ENABLED" == "true" ]]; then
        echo "package-macos.sh: FREENET_SINGLE_ARCH_BUNDLE=1 is for unsigned test bundles only" >&2
        exit 1
    fi
    echo ">> Single-architecture test bundle (FREENET_SINGLE_ARCH_BUNDLE=1)"
    cp "$FREENET_ARM64_BIN" "$APP_DIR/Contents/MacOS/freenet-bin"
else
    echo ">> Building universal binary"
    lipo -create -output "$APP_DIR/Contents/MacOS/freenet-bin" \
        "$FREENET_ARM64_BIN" "$FREENET_X86_BIN"
fi
chmod +x "$APP_DIR/Contents/MacOS/freenet-bin"

if [[ -n "${FREENET_ARM64_FDEV_BIN:-}" && -n "${FREENET_X86_FDEV_BIN:-}" ]]; then
    echo ">> Building universal fdev binary"
    lipo -create -output "$APP_DIR/Contents/MacOS/fdev" \
        "$FREENET_ARM64_FDEV_BIN" "$FREENET_X86_FDEV_BIN"
    chmod +x "$APP_DIR/Contents/MacOS/fdev"
elif [[ -n "${FREENET_ARM64_FDEV_BIN:-}" || -n "${FREENET_X86_FDEV_BIN:-}" ]]; then
    echo "package-macos.sh: FREENET_ARM64_FDEV_BIN and FREENET_X86_FDEV_BIN must both be set (or both unset)" >&2
    exit 1
else
    echo ">> Skipping fdev (FREENET_ARM64_FDEV_BIN/FREENET_X86_FDEV_BIN not set)"
fi

# CFBundleExecutable must be the real binary, not a script or launcher that
# execs it: macOS 26 does not display a status item created after such an exec
# when the app was launched through LaunchServices, so the menu bar icon is
# silently missing (#6093). LaunchServices passes no arguments, so freenet-bin
# enters the wrapper by itself when started that way (is_macos_app_launch).
if ! file -b "$APP_DIR/Contents/MacOS/freenet-bin" | grep -F 'Mach-O' > /dev/null; then
    echo "package-macos.sh: freenet-bin is not a Mach-O binary (#6093)" >&2
    exit 1
fi

echo ">> Writing launchd target for older Launch at Login agents"
# Not the bundle's executable. LaunchAgents written by earlier releases run this
# path, and only a running new version repairs them, so without it a Mac whose
# relaunch failed after an auto-update would not start Freenet at login either.
# launchd is not affected by the exec problem above. Removal is tracked in #6099.
cat > "$APP_DIR/Contents/MacOS/Freenet" <<'SH'
#!/bin/bash
DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$DIR/freenet-bin" service run-wrapper
SH
chmod +x "$APP_DIR/Contents/MacOS/Freenet"

echo ">> Writing Info.plist"
cat > "$APP_DIR/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleName</key><string>Freenet</string>
    <key>CFBundleDisplayName</key><string>Freenet</string>
    <key>CFBundleIdentifier</key><string>org.freenet.Freenet</string>
    <key>CFBundleVersion</key><string>$VERSION</string>
    <key>CFBundleShortVersionString</key><string>$VERSION</string>
    <key>CFBundlePackageType</key><string>APPL</string>
    <key>CFBundleExecutable</key><string>freenet-bin</string>
    <key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
    <key>LSUIElement</key><true/>
    <key>LSMinimumSystemVersion</key><string>11.0</string>
    <key>NSPrincipalClass</key><string>NSApplication</string>
    <key>NSHighResolutionCapable</key><true/>
    <key>CFBundleURLTypes</key>
    <array>
        <dict>
            <key>CFBundleURLName</key><string>org.freenet.Freenet.link</string>
            <key>CFBundleURLSchemes</key>
            <array><string>freenet</string></array>
        </dict>
    </array>
$(if [[ -n "$ICON_ICNS" ]]; then echo "    <key>CFBundleIconFile</key><string>Freenet</string>"; fi)
</dict>
</plist>
PLIST

echo -n "APPL????" > "$APP_DIR/Contents/PkgInfo"

if [[ -n "$ICON_ICNS" ]]; then
    if [[ ! -f "$ICON_ICNS" ]]; then
        echo "package-macos.sh: ICON_ICNS=$ICON_ICNS not found" >&2
        exit 1
    fi
    cp "$ICON_ICNS" "$APP_DIR/Contents/Resources/Freenet.icns"
fi

if [[ "$SIGNING_ENABLED" == "true" ]]; then
    echo ">> Signing Freenet.app with $CODESIGN_IDENTITY"
    # --deep signs nested content; --options runtime enables Hardened
    # Runtime (required by notarization); --timestamp is mandatory for
    # Developer ID signatures.
    codesign --deep --force \
        --options runtime \
        --entitlements "$ENTITLEMENTS" \
        --sign "$CODESIGN_IDENTITY" \
        --timestamp \
        "$APP_DIR"
    codesign --verify --deep --strict --verbose=2 "$APP_DIR"

    echo ">> Notarizing Freenet.app"
    # Notarization requires submitting a zip (or dmg/pkg). We zip the .app
    # for submission, wait for the result, then staple the ticket to the
    # original .app bundle so offline Gatekeeper sees approval immediately.
    NOTARIZE_ZIP="$OUTPUT_DIR/Freenet-app-for-notarization.zip"
    ditto -c -k --keepParent "$APP_DIR" "$NOTARIZE_ZIP"
    xcrun notarytool submit "$NOTARIZE_ZIP" \
        --key "$ASC_API_KEY_PATH" \
        --key-id "$ASC_API_KEY_ID" \
        --issuer "$ASC_API_ISSUER_ID" \
        --wait
    rm "$NOTARIZE_ZIP"
    xcrun stapler staple "$APP_DIR"
    xcrun stapler validate "$APP_DIR"
else
    echo ">> Skipping signing + notarization (credentials not provided)"
fi

if [[ "$CREATE_DMG" == "true" ]]; then
    echo ">> Creating Freenet-$VERSION.dmg"
    DMG_PATH="$OUTPUT_DIR/Freenet-$VERSION.dmg"
    rm -f "$DMG_PATH"
    if ! command -v create-dmg >/dev/null 2>&1; then
        echo "package-macos.sh: create-dmg not found (brew install create-dmg)" >&2
        exit 1
    fi
    create-dmg \
        --volname "Freenet $VERSION" \
        --window-pos 200 120 \
        --window-size 600 400 \
        --icon-size 100 \
        --icon "Freenet.app" 150 190 \
        --app-drop-link 450 190 \
        --hide-extension "Freenet.app" \
        "$DMG_PATH" \
        "$APP_DIR"

    if [[ "$SIGNING_ENABLED" == "true" ]]; then
        echo ">> Signing + notarizing Freenet-$VERSION.dmg"
        codesign --force --sign "$CODESIGN_IDENTITY" --timestamp "$DMG_PATH"
        xcrun notarytool submit "$DMG_PATH" \
            --key "$ASC_API_KEY_PATH" \
            --key-id "$ASC_API_KEY_ID" \
            --issuer "$ASC_API_ISSUER_ID" \
            --wait
        xcrun stapler staple "$DMG_PATH"
        xcrun stapler validate "$DMG_PATH"

        # A script's signature is held in extended attributes, so check that
        # it survived into what users will actually install.
        echo ">> Verifying the signature inside the DMG"
        DMG_MOUNT="$(mktemp -d)"
        hdiutil attach -nobrowse -readonly -mountpoint "$DMG_MOUNT" "$DMG_PATH" > /dev/null
        verified=true
        codesign --verify --deep --strict --verbose=2 "$DMG_MOUNT/Freenet.app" || verified=false
        # Forced because the image is read-only, so no data is at risk, and a
        # busy detach must not fail a release whose DMG just passed the check.
        hdiutil detach -force "$DMG_MOUNT" > /dev/null \
            || echo "package-macos.sh: could not detach $DMG_MOUNT" >&2
        rmdir "$DMG_MOUNT" 2>/dev/null || true
        if [[ "$verified" != "true" ]]; then
            echo "package-macos.sh: Freenet.app in the DMG fails verification" >&2
            exit 1
        fi
    fi

    echo ">> $DMG_PATH"
fi

echo ">> Done. Artifacts in $OUTPUT_DIR"
ls -la "$OUTPUT_DIR"
