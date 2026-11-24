#!/usr/bin/env bash
# Wait for ci.yml's `push` run on the release commit B to pass, in release.yml's
# `wait_for_ci` job. B is the ship commit plus the version bump, pushed as
# branch `release/vX.Y.Z`; ci.yml runs its full suite on that push, and this is
# the gate between it and the tag.
#
# Before waiting, it checks that B is exactly what release.yml meant to make:
#
#   1. B has ONE parent, and it is SHIP_SHA (the soaked commit).
#   2. B changes nothing but the version files listed in ALLOWED_FILES.
#
# Then it polls `gh run list --workflow ci.yml --commit B --event push` and:
#
#   - fails at once when the newest such run completes with any conclusion but
#     `success`;
#   - on `success`, also requires every job in REQUIRED_JOBS to be present in
#     the run with conclusion `success`. A run whose heavy jobs were skipped by
#     an `if:` still concludes `success`, so the run's conclusion alone would
#     let a misconfigured ci.yml wave an untested commit through;
#   - fails when no run has appeared after NO_RUN_TIMEOUT (a push made with
#     GITHUB_TOKEN starts no workflow, #4118), or nothing concluded within
#     MAX_WAIT;
#   - treats a failed `gh` call as transient and retries until MAX_WAIT.
#
# On success it writes `release_sha=B` to GITHUB_OUTPUT.
#
# Inputs (env): RELEASE_SHA (B), SHIP_SHA, GITHUB_REPOSITORY, GH_TOKEN,
# GITHUB_OUTPUT. Optional: RELEASE_BRANCH (only runs on this branch count),
# MAX_WAIT (s of wall time, default 4500; release.yml's job timeout must exceed
# it with margin, pinned by the test), NO_RUN_TIMEOUT (s, default 900),
# WAIT_INTERVAL (s, default 30).
# `--print-required-jobs` prints REQUIRED_JOBS, one per line, and exits; the
# test compares it with the jobs ci.yml actually runs on a release push.
# Tests: scripts/wait_for_release_ci_test.sh

set -euo pipefail

# The display names of the ci.yml jobs a release push must run and pass. The
# test (wait_for_release_ci_test.sh) requires this set to EQUAL the set of jobs
# whose `if:` admits a push to `release/v*`, so a job added to or dropped from
# the release gate must be changed in both places.
REQUIRED_JOBS=(
    "Fmt"
    "Clippy"
    "Windows Check"
    "Unit & Integration"
    "macOS Service Unit"
    "Windows Service Unit"
    "Simulation"
    "NAT Validation"
)
# Everything the version bump may touch (scripts/apply_version_bump.sh plus the
# two lockfile refreshes in update_versions).
ALLOWED_FILES=(
    "Cargo.lock"
    "crates/core/Cargo.toml"
    "crates/fdev/Cargo.toml"
    "apps/freenet-ping/Cargo.lock"
)

if [ "${1:-}" = "--print-required-jobs" ]; then
    printf '%s\n' "${REQUIRED_JOBS[@]}"
    exit 0
fi

B="${RELEASE_SHA:?RELEASE_SHA is required}"
SHIP="${SHIP_SHA:?SHIP_SHA is required}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
OUT="${GITHUB_OUTPUT:?GITHUB_OUTPUT is required}"
MAX_WAIT="${MAX_WAIT:-4500}"
NO_RUN_TIMEOUT="${NO_RUN_TIMEOUT:-900}"
WAIT_INTERVAL="${WAIT_INTERVAL:-30}"
RELEASE_BRANCH="${RELEASE_BRANCH:-}"
# Wall-clock time since start (bash's SECONDS), so time spent inside slow or
# hanging `gh` calls counts against the budget too, not only the sleeps.
START=$SECONDS
ELAPSED=0

for v in "$B" "$SHIP"; do
    if [[ ! "$v" =~ ^[0-9a-f]{40}$ ]]; then
        echo "::error title=Not a commit id::Expected a 40-hex commit, got '$v'."
        exit 1
    fi
done

pause() {
    sleep "$WAIT_INTERVAL"
    ELAPSED=$((SECONDS - START))
}

# --- 1 and 2: B is the ship commit plus the bump, nothing else ---------------
PARENTS=""
FILES_JSON=""
while :; do
    PARENTS=$(gh api "repos/$REPO/commits/$B" --jq '[.parents[].sha] | join(" ")' 2>/dev/null) || PARENTS="__error__"
    FILES_JSON=$(gh api "repos/$REPO/compare/$SHIP...$B" \
        --jq '{status, ahead_by, files: [.files[].filename]}' 2>/dev/null) || FILES_JSON="__error__"
    if [ "$PARENTS" != "__error__" ] && [ "$FILES_JSON" != "__error__" ]; then
        break
    fi
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        echo "::error title=Could not inspect the release commit::GitHub did not answer for $B within ${MAX_WAIT}s."
        exit 1
    fi
    echo "  ⚠️  transient gh error reading the release commit, will retry (${ELAPSED}s elapsed)"
    pause
done

if [ "$PARENTS" != "$SHIP" ]; then
    echo "::error title=Release commit is not on the ship commit::$B has parent(s) '$PARENTS', expected exactly the ship commit $SHIP. Something other than update_versions made this commit, or it was rebased."
    exit 1
fi
STATUS=$(jq -r '.status' <<< "$FILES_JSON")
AHEAD=$(jq -r '.ahead_by' <<< "$FILES_JSON")
if [ "$STATUS" != "ahead" ] || [ "$AHEAD" != "1" ]; then
    echo "::error title=Release commit is not one commit past the ship commit::compare $SHIP...$B says status=$STATUS ahead_by=$AHEAD."
    exit 1
fi
UNEXPECTED=$(jq -r --args '.files - $ARGS.positional | .[]' "${ALLOWED_FILES[@]}" <<< "$FILES_JSON")
if [ -n "$UNEXPECTED" ]; then
    echo "::error title=Release commit changes more than the version::$B changes files the version bump never touches: $(paste -sd' ' - <<< "$UNEXPECTED"). Allowed: ${ALLOWED_FILES[*]}."
    exit 1
fi
if [ "$(jq '.files | length' <<< "$FILES_JSON")" -eq 0 ]; then
    echo "::error title=Release commit changes nothing::$B is one commit past $SHIP but changes no file."
    exit 1
fi
echo "✅ $B = $SHIP + version bump ($(jq -r '.files | join(", ")' <<< "$FILES_JSON"))"

# --- 3: the push CI run on B -------------------------------------------------
echo "⏳ Waiting for ci.yml's push run on $B${RELEASE_BRANCH:+ ($RELEASE_BRANCH)}..."
SEEN_RUN=""
while [ "$ELAPSED" -lt "$MAX_WAIT" ]; do
    if ! RUNS=$(gh run list --repo "$REPO" --workflow ci.yml --commit "$B" --event push \
            --limit 20 --json databaseId,status,conclusion,url,headBranch,createdAt 2>/dev/null) \
        || ! LATEST=$(jq -c --arg br "$RELEASE_BRANCH" \
            '[.[] | select($br == "" or .headBranch == $br)] | max_by(.createdAt) // empty' <<< "$RUNS" 2>/dev/null); then
        echo "  ⚠️  transient gh error listing CI runs, will retry (${ELAPSED}s elapsed)"
        pause
        continue
    fi
    if [ -z "$LATEST" ]; then
        if [ -z "$SEEN_RUN" ] && [ "$ELAPSED" -ge "$NO_RUN_TIMEOUT" ]; then
            echo "::error title=No CI run on the release commit::No ci.yml push run appeared for $B after ${ELAPSED}s. Was the branch pushed with GITHUB_TOKEN instead of RELEASE_PAT (#4118)? Does ci.yml still trigger on pushes to release/v*?"
            exit 1
        fi
        echo "  Still waiting... no ci.yml push run for $B yet (${ELAPSED}s elapsed)"
        pause
        continue
    fi
    SEEN_RUN=yes
    RUN_ID=$(jq -r '.databaseId' <<< "$LATEST")
    RUN_STATUS=$(jq -r '.status' <<< "$LATEST")
    RUN_CONCLUSION=$(jq -r '.conclusion // ""' <<< "$LATEST")
    RUN_URL=$(jq -r '.url' <<< "$LATEST")
    if [ "$RUN_STATUS" != "completed" ]; then
        echo "  Still waiting... run $RUN_ID is $RUN_STATUS (${ELAPSED}s elapsed) $RUN_URL"
        pause
        continue
    fi
    if [ "$RUN_CONCLUSION" != "success" ]; then
        echo "::error title=CI failed on the release commit::ci.yml concluded '$RUN_CONCLUSION' on $B: $RUN_URL. Nothing has been tagged. See scripts/RELEASE_RECOVERY.md."
        exit 1
    fi
    if ! JOBS=$(gh run view "$RUN_ID" --repo "$REPO" --json jobs \
            --jq '[.jobs[] | {name, conclusion}]' 2>/dev/null); then
        echo "  ⚠️  transient gh error reading run $RUN_ID's jobs, will retry (${ELAPSED}s elapsed)"
        pause
        continue
    fi
    MISSING=""
    for job in "${REQUIRED_JOBS[@]}"; do
        c=$(jq -r --arg n "$job" '[.[] | select(.name == $n) | .conclusion] | first // "absent"' <<< "$JOBS")
        if [ "$c" != "success" ]; then
            MISSING="$MISSING '$job'=$c"
        fi
    done
    if [ -n "$MISSING" ]; then
        echo "::error title=The release gate did not run in full::ci.yml concluded success on $B, but these required jobs did not pass:$MISSING. A skipped job reads as success at run level, so this would have tagged an untested commit. Check the jobs' if: conditions in ci.yml: $RUN_URL"
        exit 1
    fi
    echo "✅ ci.yml passed on $B with every required job: $RUN_URL"
    echo "release_sha=$B" >> "$OUT"
    exit 0
done

echo "::error title=Release CI timeout::ci.yml did not conclude on $B within ${MAX_WAIT}s. Nothing has been tagged. See scripts/RELEASE_RECOVERY.md."
exit 1
