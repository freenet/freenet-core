#!/usr/bin/env bash
# Regression tests for the release workflows' interaction with the merge queue,
# and for #5233 (the release is cut from ONE immutable commit).
#
# Merge queue. The v0.2.93 release broke because the auto-merge step ran
# `gh pr merge ... --squash --auto`: the repo's merge queue (strict-strategy
# enforcement) OWNS the merge strategy and rejects an explicit `--squash`, so
# `gh pr merge` exited non-zero under `bash -e`. release.yml no longer opens or
# merges any PR (it ships ship_sha plus a bump on `release/vX` and gates on that
# branch's push CI); the one auto-merge left in the release path is the
# merge-back PR, opened by scripts/release_sync.sh from release-sync.yml. These
# checks pin that it still enables `--auto`, never re-adds `--squash`, and that
# release.yml has no merge of its own.
#
# The waiter for the bump PR's merge queue (wait_for_release_pr.sh, #5084) and
# the merge-commit resolver (resolve_release_sha.sh) were retired with the PR;
# the release CI waiter that replaced them is tested by
# scripts/wait_for_release_ci_test.sh.
#
# Run manually with: bash scripts/release_mergequeue_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"
SYNC_SCRIPT="$SCRIPT_DIR/release_sync.sh"
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

FAILURES=0
check() {
    # check <description> <actual> <expected>
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# Executable `gh pr merge` invocations (comments dropped).
MERGE_LINES=$(grep -E 'gh pr merge' "$SYNC_SCRIPT" | grep -vE '^\s*#' || true)
check "release_sync.sh still has a gh pr merge invocation" \
  "$([ -n "$MERGE_LINES" ] && echo present || echo missing)" "present"
# The merge queue owns the strategy; an explicit --squash makes `gh pr merge`
# exit non-zero (broke v0.2.93).
check "gh pr merge does NOT pass --squash" \
  "$(echo "$MERGE_LINES" | grep -q -- '--squash' && echo has-squash || echo no-squash)" "no-squash"
check "gh pr merge still enables --auto" \
  "$(echo "$MERGE_LINES" | grep -q -- '--auto' && echo has-auto || echo no-auto)" "has-auto"
check "release.yml merges no PR itself" \
  "$(grep -E 'gh pr (merge|create)' "$RELEASE_YML" | grep -cvE '^\s*#' || true)" "0"

# The waiter reads Actions workflow runs. Top-level `permissions:` makes every
# unspecified GITHUB_TOKEN scope `none`, so this job must explicitly opt into
# Actions read access. Use the job's least-privilege GITHUB_TOKEN rather than a
# RELEASE_PAT that may not carry the Actions permission.
WAIT_JOB=$(sed -n '/^  wait_for_ci:/,/^  verify_publishable:/p' "$RELEASE_YML")
check "wait_for_ci grants Actions read access" \
  "$(grep -qE '^      actions: read$' <<< "$WAIT_JOB" && echo present || echo missing)" "present"
# The GitHub expression is intentionally matched literally.
# shellcheck disable=SC2016
check "wait_for_ci uses its least-privilege GITHUB_TOKEN" \
  "$(grep -qF 'GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}' <<< "$WAIT_JOB" && echo correct || echo wrong)" "correct"
check "wait_for_ci invokes the tested waiter script" \
  "$(grep -qE '^[[:space:]]*run: bash scripts/wait_for_release_ci\.sh[[:space:]]*$' <<< "$WAIT_JOB" && echo correct || echo wrong)" "correct"

# ---------------------------------------------------------------------------
# #5233: the release must be cut from ONE immutable commit.
#
# `verify_publishable` (then named `publish_crates`) and `create_release` used
# to check out `ref: main` and then
# `git pull origin main`. Both ran AFTER the bump PR's wait (now `wait_for_ci`), which blocked ~20 minutes
# on the merge queue, so any commit that landed on main during that window was
# silently published and tagged. v0.2.122 shipped an unrelated commit this way.
# ---------------------------------------------------------------------------

VERIFY_SCRIPT="$SCRIPT_DIR/verify_release_checkout.sh"

# Job bodies, bounded to the job. An unbounded grep over the whole file would
# match a neighbouring job and pass vacuously.
PUBLISH_JOB=$(sed -n '/^  verify_publishable:/,/^  create_release:/p' "$RELEASE_YML")
CREATE_RELEASE_JOB=$(sed -n '/^  create_release:/,/^  summary:/p' "$RELEASE_YML")

check "verify_publishable job body was located" \
  "$([ -n "$PUBLISH_JOB" ] && echo present || echo missing)" "present"
check "create_release job body was located" \
  "$([ -n "$CREATE_RELEASE_JOB" ] && echo present || echo missing)" "present"

# A sed range whose closing anchor is renamed silently runs to EOF, so the
# job-scoped pins below would start matching neighbouring jobs and pass
# vacuously. Assert each range actually terminated on its anchor.
check "verify_publishable range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$PUBLISH_JOB")" = "  create_release:" ] && echo bounded || echo ran-to-eof)" "bounded"
check "create_release range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$CREATE_RELEASE_JOB")" = "  summary:" ] && echo bounded || echo ran-to-eof)" "bounded"
# WAIT_JOB is extracted by the pre-existing merge-queue checks above and needs
# the same guard: renaming verify_publishable silently widens it to EOF, after
# which every wait_for_ci pin starts matching text from other jobs.
check "wait_for_ci range is bounded by the next job" \
  "$([ "$(tail -n1 <<< "$WAIT_JOB")" = "  verify_publishable:" ] && echo bounded || echo ran-to-eof)" "bounded"

# The bug itself: no job may resolve a moving reference.
#
# Every needle below is matched against EXECUTABLE lines (`^[^#]*`) or in a
# whole-line anchored form. A bare `grep -qF` substring would be satisfied by
# `# TODO: re-enable <the thing>`, so commenting a guard out would keep this
# suite green — which is exactly the regression these pins exist to catch.
# `github.ref` is included because on `workflow_dispatch` it IS `refs/heads/main`,
# so `ref: ${{ github.ref }}` is the same bug wearing an expression.
check "release.yml never checks out a moving 'main' reference" \
  "$(grep -qE "^[[:space:]]*ref:[[:space:]]*(['\"]?(main|refs/heads/main)['\"]?|\\\$\\{\\{[[:space:]]*github\\.ref[[:space:]]*\\}\\})[[:space:]]*(#.*)?$" "$RELEASE_YML" && echo moving-ref || echo pinned)" "pinned"

# `git pull origin main` was the original second half of the bug, but ANY
# shell-level re-pointing of the checkout reopens the same window — and does so
# where the runtime verifier, which has already run, cannot see it.
for moving_cmd in \
    'git pull[[:space:]]' \
    'git checkout[[:space:]]+(-[^[:space:]]+[[:space:]]+)*main' \
    'git reset[[:space:]]+--hard' \
    'git merge[[:space:]]+origin/main'; do
    check "release.yml never re-points the checkout with '$moving_cmd'" \
      "$(grep -qE "^[^#]*${moving_cmd}" "$RELEASE_YML" && echo moves || echo pinned)" "pinned"
done

# The pin has to come from somewhere: wait_for_ci resolves it once and exports it.
# The GitHub expressions below are intentionally matched literally.
# shellcheck disable=SC2016
check "wait_for_ci exports the release SHA its waiter wrote" \
  "$(grep -qE '^[[:space:]]*release_sha: \$\{\{ steps\.wait\.outputs\.release_sha \}\}[[:space:]]*$' <<< "$WAIT_JOB" && echo present || echo missing)" "present"

# Both downstream jobs must consume that SHA and re-check it after checkout.
#
# The GUARDED act each job must never reach unverified. For `create_release`
# that act is still irreversible: pushing the tag. For `verify_publishable` it
# no longer is -- the real `cargo publish` moved to cross-compile.yml's
# `attach-to-release`, downstream of the blocking pre-flight canary, and what
# remains here is `cargo publish --dry-run`.
#
# The check is KEPT for it anyway, and deliberately so. A dry run exists to
# predict whether the real publish will succeed, and it can only predict that
# for the tree it actually ran against: performed on a tree that drifted off
# the pinned release commit, it reports publishability for something else and
# the prediction is worthless. The needle is unchanged (`cargo publish` matches
# the dry run too), so this is a rename of the LABEL to something true, not a
# relaxation of the assertion.
#
# The real irreversible act's ordering is pinned where it now lives, in
# scripts/release_canary_wiring_test.sh (assertions 2b and 2c): the crates.io
# publish must sit between Gate A and the un-draft, and release.yml must
# contain no non-dry-run `cargo publish` at all. The coverage moved files
# along with the step it covers; nothing is left unpinned.
#
# The regex needles below are intentionally literal shell text, not expansions.
# shellcheck disable=SC2016
for job_name in verify_publishable create_release; do
    case "$job_name" in
      verify_publishable) JOB_BODY="$PUBLISH_JOB"; GUARDED='cargo publish'
                          GUARDED_DESC='dry-run publish' ;;
      create_release)     JOB_BODY="$CREATE_RELEASE_JOB"; GUARDED='git push origin "v\$VERSION"'
                          GUARDED_DESC='irreversible step' ;;
    esac

    check "$job_name checks out needs.wait_for_ci.outputs.release_sha" \
      "$(grep -qE '^[[:space:]]*ref: \$\{\{ needs\.wait_for_ci\.outputs\.release_sha' <<< "$JOB_BODY" && echo pinned || echo missing)" "pinned"
    check "$job_name verifies the checkout" \
      "$(grep -qE '^[[:space:]]*run: bash "\$RUNNER_TEMP/release-tools/verify_release_checkout\.sh"[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    check "$job_name exports RELEASE_SHA for the verifier" \
      "$(grep -qE '^[[:space:]]*RELEASE_SHA: \$\{\{ needs\.wait_for_ci\.outputs\.release_sha' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    # Without this the verifier degenerates to comparing the checkout ref
    # against itself, which cannot fail.
    check "$job_name gives the verifier an independent version oracle" \
      "$(grep -qE '^[[:space:]]*EXPECTED_VERSION: \$\{\{ needs\.validate\.outputs\.version \}\}[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"
    # Lets the verifier distinguish the `|| github.sha` fallback from a
    # genuinely wrong tree.
    check "$job_name tells the verifier whether a SHA was actually resolved" \
      "$(grep -qE '^[[:space:]]*RELEASE_SHA_RESOLVED: \$\{\{ needs\.wait_for_ci\.outputs\.release_sha \}\}[[:space:]]*$' <<< "$JOB_BODY" && echo present || echo missing)" "present"

    # Presence is not enough. A guard placed AFTER the guarded step verifies
    # nothing — for create_release the tag has already been pushed, and for
    # verify_publishable the dry run has already reported on whatever tree was
    # checked out — so assert ORDER. Line numbers are relative to the extracted
    # job body.
    VERIFY_LINE=$(grep -nE '^[[:space:]]*run: bash "\$RUNNER_TEMP/release-tools/verify_release_checkout\.sh"[[:space:]]*$' <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)
    CHECKOUT_LINE=$(grep -nE '^[[:space:]]*ref: \$\{\{ needs\.wait_for_ci\.outputs\.release_sha' <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)
    GUARDED_LINE=$(grep -nE "^[^#]*${GUARDED}" <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)

    check "$job_name's $GUARDED_DESC was located" \
      "$([ -n "$GUARDED_LINE" ] && echo found || echo missing)" "found"
    check "$job_name verifies BEFORE its $GUARDED_DESC, not after" \
      "$([ -n "$VERIFY_LINE" ] && [ -n "$GUARDED_LINE" ] && [ "$VERIFY_LINE" -lt "$GUARDED_LINE" ] && echo before || echo after)" "before"
    check "$job_name verifies AFTER the checkout, not before" \
      "$([ -n "$VERIFY_LINE" ] && [ -n "$CHECKOUT_LINE" ] && [ "$VERIFY_LINE" -gt "$CHECKOUT_LINE" ] && echo after || echo before)" "after"
    # The verifier is the dispatch ref's copy, kept aside BEFORE the release
    # commit replaces the working tree (whose scripts are the ship commit's,
    # possibly older).
    KEEP_LINE=$(grep -nE '^[^#]*cp scripts/verify_release_checkout\.sh "\$RUNNER_TEMP/release-tools/"' <<< "$JOB_BODY" | head -1 | cut -d: -f1 || true)
    check "$job_name keeps the dispatch ref's verifier before checking out the release commit" \
      "$([ -n "$KEEP_LINE" ] && [ -n "$CHECKOUT_LINE" ] && [ "$KEEP_LINE" -lt "$CHECKOUT_LINE" ] && echo before || echo "keep=$KEEP_LINE checkout=$CHECKOUT_LINE")" "before"
done

# create_release can only read the output if wait_for_ci is in its needs.
check "create_release depends on wait_for_ci so the SHA is in scope" \
  "$(grep -qE '^    needs: \[validate, wait_for_ci, verify_publishable\]$' <<< "$CREATE_RELEASE_JOB" && echo present || echo missing)" "present"

# The notes must describe the commit being released, not whatever main is now.
# The shell variable reference is intentionally matched literally.
# shellcheck disable=SC2016
check "release notes are generated for the pinned commit" \
  "$(grep -qE '^[^#]*-f target_commitish="\$RELEASE_SHA"' <<< "$CREATE_RELEASE_JOB" && echo pinned || echo missing)" "pinned"
check "release notes never fall back to target_commitish=main" \
  "$(grep -qE '^[^#]*-f target_commitish=main' <<< "$CREATE_RELEASE_JOB" && echo moving || echo pinned)" "pinned"

# --- behavioural: scripts/verify_release_checkout.sh -------------------------

RELEASE_VERSION="0.2.123"
VERIFY_REPO="$TEST_TMP/verify-repo"
mkdir -p "$VERIFY_REPO/crates/core"
printf '[package]\nname = "freenet"\nversion = "%s"\n' "$RELEASE_VERSION" \
  > "$VERIFY_REPO/crates/core/Cargo.toml"
(
  cd "$VERIFY_REPO"
  git init -q .
  git add crates/core/Cargo.toml
  git -c user.email=t@example.invalid -c user.name=t commit -q -m "build: release $RELEASE_VERSION"
) >/dev/null 2>&1
VERIFY_HEAD=$(cd "$VERIFY_REPO" && git rev-parse HEAD)

run_verify() {
    local expected_sha="$1" expected_version="$2" output_file="$3" status
    set +e
    ( cd "$VERIFY_REPO" && RELEASE_SHA="$expected_sha" EXPECTED_VERSION="$expected_version" \
        RELEASE_SHA_RESOLVED="$expected_sha" \
        bash --noprofile --norc "$VERIFY_SCRIPT" ) > "$output_file" 2>&1
    status=$?
    set -e
    echo "$status"
}

VERIFY_OK_OUTPUT="$TEST_TMP/verify-ok.txt"
check "verifier accepts the pinned commit carrying the release version" \
  "$(run_verify "$VERIFY_HEAD" "$RELEASE_VERSION" "$VERIFY_OK_OUTPUT")" "0"

VERIFY_DRIFT_OUTPUT="$TEST_TMP/verify-drift.txt"
DRIFT_SHA="0123456789012345678901234567890123456789"
check "verifier rejects a checkout that drifted off the pinned commit" \
  "$([ "$(run_verify "$DRIFT_SHA" "$RELEASE_VERSION" "$VERIFY_DRIFT_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier names both SHAs when it rejects" \
  "$(grep -qF "$DRIFT_SHA" "$VERIFY_DRIFT_OUTPUT" && grep -qF "$VERIFY_HEAD" "$VERIFY_DRIFT_OUTPUT" && echo present || echo missing)" "present"

# The oracle that is NOT a restatement of the checkout ref: the `|| github.sha`
# fallback would put us on the launch commit, whose manifest still carries the
# PREVIOUS version. HEAD would match RELEASE_SHA and only this check fires.
VERIFY_WRONG_TREE_OUTPUT="$TEST_TMP/verify-wrong-tree.txt"
check "verifier rejects the right SHA carrying the wrong version" \
  "$([ "$(run_verify "$VERIFY_HEAD" "0.2.122" "$VERIFY_WRONG_TREE_OUTPUT")" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier explains which version it found versus expected" \
  "$(grep -qF "declares version '$RELEASE_VERSION'" "$VERIFY_WRONG_TREE_OUTPUT" && grep -qF 'this release is 0.2.122' "$VERIFY_WRONG_TREE_OUTPUT" && echo present || echo missing)" "present"

# The `|| github.sha` fallback path: wait_for_ci resolved nothing, so the job
# is sitting on the launch commit. The failure must name THAT, not report a
# version mismatch and send the operator to inspect the tree.
VERIFY_FALLBACK_OUTPUT="$TEST_TMP/verify-fallback.txt"
set +e
( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_FALLBACK_OUTPUT" 2>&1
VERIFY_FALLBACK_STATUS=$?
set -e
check "verifier refuses to act when no release commit was resolved" \
  "$([ "$VERIFY_FALLBACK_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier names wait_for_ci as the cause, not the tree" \
  "$(grep -qF 'wait_for_ci did not resolve a release commit' "$VERIFY_FALLBACK_OUTPUT" && echo correct || echo misattributed)" "correct"

# A manifest with no `^version = "` line must produce the explanatory error,
# not die silently inside the pipeline that reads it.
VERIFY_NOVER_REPO="$TEST_TMP/verify-noversion-repo"
mkdir -p "$VERIFY_NOVER_REPO/crates/core"
printf '[package]\nname = "freenet"\nversion.workspace = true\n' \
  > "$VERIFY_NOVER_REPO/crates/core/Cargo.toml"
(
  cd "$VERIFY_NOVER_REPO"
  git init -q .
  git add crates/core/Cargo.toml
  git -c user.email=t@example.invalid -c user.name=t commit -q -m "workspace-inherited version"
) >/dev/null 2>&1
VERIFY_NOVER_HEAD=$(cd "$VERIFY_NOVER_REPO" && git rev-parse HEAD)
VERIFY_NOVER_OUTPUT="$TEST_TMP/verify-noversion.txt"
set +e
( cd "$VERIFY_NOVER_REPO" && RELEASE_SHA="$VERIFY_NOVER_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="$VERIFY_NOVER_HEAD" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_NOVER_OUTPUT" 2>&1
VERIFY_NOVER_STATUS=$?
set -e
check "verifier fails when the manifest has no readable version" \
  "$([ "$VERIFY_NOVER_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
check "verifier explains itself rather than dying silently in a pipeline" \
  "$(grep -qF '::error' "$VERIFY_NOVER_OUTPUT" && echo explained || echo silent)" "explained"

VERIFY_NO_MANIFEST_OUTPUT="$TEST_TMP/verify-no-manifest.txt"
set +e
( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
    RELEASE_SHA_RESOLVED="$VERIFY_HEAD" \
    MANIFEST="crates/core/Absent.toml" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
  > "$VERIFY_NO_MANIFEST_OUTPUT" 2>&1
VERIFY_NO_MANIFEST_STATUS=$?
set -e
check "verifier rejects a checkout with no freenet manifest" \
  "$([ "$VERIFY_NO_MANIFEST_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"

for missing_var in RELEASE_SHA EXPECTED_VERSION; do
    VERIFY_UNSET_OUTPUT="$TEST_TMP/verify-unset-$missing_var.txt"
    set +e
    ( cd "$VERIFY_REPO" && RELEASE_SHA="$VERIFY_HEAD" EXPECTED_VERSION="$RELEASE_VERSION" \
        RELEASE_SHA_RESOLVED="$VERIFY_HEAD" \
        env -u "$missing_var" bash --noprofile --norc "$VERIFY_SCRIPT" ) \
      > "$VERIFY_UNSET_OUTPUT" 2>&1
    VERIFY_UNSET_STATUS=$?
    set -e
    check "verifier fails closed when $missing_var is unset" \
      "$([ "$VERIFY_UNSET_STATUS" -ne 0 ] && echo failed || echo passed)" "failed"
done


# --- the REAL manifest must satisfy the verifier's version grep -------------
#
# The verifier's fixture is synthetic, so nothing else pins that `^version = "`
# picks the PACKAGE version out of the real manifest. A move to
# `version.workspace = true`, or a sub-table whose `version = "..."` sorts
# first, would break every release at the guard with no other warning.
REAL_MANIFEST="$SCRIPT_DIR/../crates/core/Cargo.toml"
REAL_VERSION=$(grep -m1 -E '^version = "' "$REAL_MANIFEST" | cut -d'"' -f2 || true)
check "the verifier's grep finds a version in the real core manifest" \
  "$([[ "$REAL_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+ ]] && echo found || echo "missing($REAL_VERSION)")" "found"
check "the real core manifest's first '^version =' is the package version" \
  "$(awk '/^\[/{sec=$0} /^version = "/{print sec; exit}' "$REAL_MANIFEST")" "[package]"

if [ "$FAILURES" -eq 0 ]; then
    echo "PASS: the release merge-back is merge-queue-compatible and release.yml is pinned to one validated commit."
else
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
