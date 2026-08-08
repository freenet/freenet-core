#!/usr/bin/env bash
# Defence in depth for #5233: prove the working tree is the commit the release
# was pinned to before anything irreversible happens to it.
#
# The pin itself (`ref: $RELEASE_SHA` on actions/checkout) is what fixes the
# bug. This guard exists because the failure it catches is otherwise SILENT:
# the release simply ships a different tree and nothing in the run log says so.
# Keep it even though the pin makes it redundant today — a future edit that
# reintroduces a moving reference, or a `git pull` slipped back in, then turns
# into a red job instead of a silently wrong artifact.
#
# Must run AFTER checkout and BEFORE publishing or tagging.
#
# Run manually with: RELEASE_SHA=<sha> bash scripts/verify_release_checkout.sh
# Tests: scripts/release_mergequeue_test.sh

set -euo pipefail

RELEASE_SHA="${RELEASE_SHA:?RELEASE_SHA is required}"

ACTUAL_SHA=$(git rev-parse HEAD)

if [ "$ACTUAL_SHA" != "$RELEASE_SHA" ]; then
    echo "::error title=Release checkout drifted::Expected to build the release from $RELEASE_SHA but the checkout is at $ACTUAL_SHA. Refusing to publish or tag a commit the pipeline did not validate (#5233)."
    exit 1
fi

echo "✅ Release checkout verified at $ACTUAL_SHA"
