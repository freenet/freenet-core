#!/usr/bin/env bash
# Resolve release.yml's `dry_run` input into the ONE value every mutating step
# checks: `live=true` or `live=false` on $GITHUB_OUTPUT.
#
# Why a single resolved value rather than `if: ${{ !inputs.dry_run }}` on each
# step: that is how #5936 happened. Each step carried its own guard, the
# version-bump job had none, and a "dry run" pushed a release branch, opened
# the `build: release` PR and auto-merged it into main. A guard that has to be
# remembered per step is missed by the next edit. Now `inputs.dry_run` is read
# HERE ONLY, and every step that pushes, opens a PR, merges, tags or creates a
# release is gated on `needs.validate.outputs.live == 'true'`.
# `.github/scripts/check_release_dry_run.py` (run by Rule Lint) enforces both.
#
# FAILS CLOSED. Only the exact string `false` yields a live run. Anything else
# (`true`, empty because the input was never wired, a typo) is a dry run, and
# a mutating step whose guard reads an empty or missing output is skipped.
#
# Run manually with:
#   DRY_RUN_INPUT=true GITHUB_OUTPUT=/dev/stdout bash scripts/resolve_release_mode.sh
# Tests: scripts/release_dry_run_test.sh

set -euo pipefail

OUTPUT_FILE="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
DRY_RUN_INPUT="${DRY_RUN_INPUT-}"

if [ "$DRY_RUN_INPUT" = "false" ]; then
    echo "live=true" >> "$OUTPUT_FILE"
    echo "🚀 LIVE release: this run pushes the bump branch, merges the bump PR, tags and creates the draft release."
    exit 0
fi

echo "live=false" >> "$OUTPUT_FILE"
if [ "$DRY_RUN_INPUT" != "true" ]; then
    echo "::warning title=Treating this run as a dry run::The dry_run input was '$DRY_RUN_INPUT', not 'true' or 'false'. Refusing to guess that a release was meant, so nothing will be pushed."
fi
echo "::notice title=DRY RUN::Nothing will be pushed, merged, tagged or released. The version bump is computed and committed locally, packaged with 'cargo publish --dry-run', and what a live run would create is printed instead."
