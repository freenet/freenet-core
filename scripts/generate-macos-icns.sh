#!/bin/bash
# Generate a macOS .icns icon from the Freenet SVG logo.
#
# Usage: generate-macos-icns.sh [SVG_PATH] [OUTPUT_ICNS]
#   SVG_PATH      Path to the source SVG (default:
#                 crates/core/src/bin/commands/assets/freenet_logo.svg)
#   OUTPUT_ICNS   Destination .icns path (default: ./freenet.icns)
#
# Requires:
#   - rsvg-convert  (brew install librsvg   /  apt install librsvg2-bin)
#   - iconutil on macOS, or png2icns (libicns) on Linux
#
# Produces a standard Apple iconset (16, 32, 128, 256, 512 at 1x + 2x) and
# packs it into a .icns. The Freenet logo SVG uses a square 640x640 viewBox
# centered around the original 640x471 F-shape, so the rendered square
# output has equal margin top and bottom with no horizontal stretching.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SVG_PATH="${1:-$REPO_ROOT/crates/core/src/bin/commands/assets/freenet_logo.svg}"
OUTPUT_ICNS="${2:-$(pwd)/freenet.icns}"

if [[ ! -f "$SVG_PATH" ]]; then
    echo "generate-macos-icns.sh: SVG not found at $SVG_PATH" >&2
    exit 1
fi

if ! command -v rsvg-convert >/dev/null 2>&1; then
    echo "generate-macos-icns.sh: rsvg-convert is required" >&2
    echo "  macOS:  brew install librsvg" >&2
    echo "  Linux:  apt install librsvg2-bin" >&2
    exit 1
fi

ICONSET_DIR="$(mktemp -d)/Freenet.iconset"
mkdir -p "$ICONSET_DIR"

if command -v iconutil >/dev/null 2>&1; then
    # macOS path: full Apple iconset with 1x + 2x (Retina) variants so
    # iconutil can pack the @2x Retina-specific slots in the .icns.
    for size in 16 32 128 256 512; do
        rsvg-convert -w "$size" -h "$size" "$SVG_PATH" \
            -o "$ICONSET_DIR/icon_${size}x${size}.png"
        double=$((size * 2))
        rsvg-convert -w "$double" -h "$double" "$SVG_PATH" \
            -o "$ICONSET_DIR/icon_${size}x${size}@2x.png"
    done
    iconutil -c icns "$ICONSET_DIR" -o "$OUTPUT_ICNS"
elif command -v png2icns >/dev/null 2>&1; then
    # Linux/libicns path (for local dev smoke-tests). libicns maps PNGs
    # to .icns slots by pixel size, so it rejects the @2x duplicates that
    # iconutil handles natively. Generate one PNG per unique pixel size.
    for size in 16 32 64 128 256 512 1024; do
        rsvg-convert -w "$size" -h "$size" "$SVG_PATH" \
            -o "$ICONSET_DIR/icon_${size}.png"
    done
    png2icns "$OUTPUT_ICNS" "$ICONSET_DIR"/icon_*.png >/dev/null
else
    echo "generate-macos-icns.sh: need iconutil (macOS) or png2icns (libicns)" >&2
    exit 1
fi

echo "Generated: $OUTPUT_ICNS"
