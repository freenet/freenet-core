#!/usr/bin/env bash
# Prove the working tree is the commit the release was pinned to, before
# anything irreversible (a crates.io publish, a tag push) happens to it.
#
# Two checks, deliberately of different kinds:
#
#   1. HEAD == $RELEASE_SHA. Cheap, but note it is NEARLY a tautology: the
#      same expression feeds `ref:` on actions/checkout, which guarantees the
#      match. It earns its place only against an edit that changes one and not
#      the other, or that reintroduces a `git pull` after checkout.
#
#   2. The checked-out manifest carries $EXPECTED_VERSION. This is the real
#      oracle. It is derived independently of the SHA — from `validate`'s
#      resolved version rather than from `wait_for_pr`'s output — so it fails
#      on ANY wrong tree, including the one case check 1 cannot see: the
#      `|| github.sha` fallback silently engaging and putting us on the LAUNCH
#      commit, which does not contain the version bump. Publishing or tagging
#      that tree would ship a crate whose version disagrees with the tag.
#
# Must run AFTER checkout and BEFORE publishing or tagging. See #5233.
#
# Run manually with:
#   RELEASE_SHA=<sha> EXPECTED_VERSION=<x.y.z> bash scripts/verify_release_checkout.sh
# Tests: scripts/release_mergequeue_test.sh

set -euo pipefail

RELEASE_SHA="${RELEASE_SHA:?RELEASE_SHA is required}"
EXPECTED_VERSION="${EXPECTED_VERSION:?EXPECTED_VERSION is required}"
# Empty exactly when `wait_for_pr` produced no SHA and the job fell back to the
# launch commit. Reported separately so the failure names the real cause
# instead of blaming the tree for a version mismatch it was always going to
# have. Unset (rather than empty) means "not wired up", which is not an error.
RELEASE_SHA_RESOLVED="${RELEASE_SHA_RESOLVED-unset}"
# Overridable so the tests can point at a fixture manifest.
MANIFEST="${MANIFEST:-crates/core/Cargo.toml}"

if [ -z "$RELEASE_SHA_RESOLVED" ]; then
    echo "::error title=No validated release commit::wait_for_pr did not resolve a release commit, so this job fell back to the launch commit $RELEASE_SHA. That commit predates the version bump, so it is not the release. Fix whatever made wait_for_pr fail and re-run (#5233)."
    exit 1
fi

ACTUAL_SHA=$(git rev-parse HEAD)

if [ "$ACTUAL_SHA" != "$RELEASE_SHA" ]; then
    echo "::error title=Release checkout drifted::Expected to build the release from $RELEASE_SHA but the checkout is at $ACTUAL_SHA. Refusing to publish or tag a commit the pipeline did not validate (#5233)."
    exit 1
fi

if [ ! -f "$MANIFEST" ]; then
    echo "::error title=Release checkout is not a freenet tree::Expected $MANIFEST in the checkout at $ACTUAL_SHA, but it is missing."
    exit 1
fi

# Split the grep from the cut: under `pipefail` a no-match grep would abort
# the script HERE, killing it before the message below that explains what is
# wrong — turning a legible failure into a blank step.
VERSION_LINE=$(grep -m1 -E '^version = "' "$MANIFEST" || true)
ACTUAL_VERSION=$(printf '%s' "$VERSION_LINE" | cut -d'"' -f2)

if [ "$ACTUAL_VERSION" != "$EXPECTED_VERSION" ]; then
    echo "::error title=Release checkout is the wrong tree::Commit $ACTUAL_SHA declares version '$ACTUAL_VERSION' in $MANIFEST, but this release is $EXPECTED_VERSION. The checkout is not the version-bump commit — publishing or tagging it would ship a crate whose version disagrees with the tag (#5233)."
    exit 1
fi

echo "✅ Release checkout verified: $ACTUAL_SHA declares version $ACTUAL_VERSION"
