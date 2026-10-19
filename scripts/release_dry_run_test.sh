#!/usr/bin/env bash
# Regression tests for #5936: release.yml's `dry_run=true` was not a dry run.
#
# The version-bump job had no dry-run guard, so a "dry run" pushed
# `release/vX.Y.Z`, opened the real `build: release X.Y.Z` PR and enabled
# auto-merge on it, landing the bump on main. Only the tag and release steps
# carried their own `if: ${{ !inputs.dry_run }}`.
#
# The fix resolves the input ONCE (scripts/resolve_release_mode.sh, exported as
# `needs.validate.outputs.live`), gates every mutating step on it, and makes
# each mutating script refuse on its own when not live.
# `.github/scripts/check_release_dry_run.py` enforces that shape and has its own
# `--self-test` over synthetic fixtures. This file tests what that cannot:
#
#   1. the resolver fails closed (only the literal `false` is live);
#   2. the linter fires on the REAL pre-fix job, and passes the repo today;
#   3. the obvious future regressions of the real file are caught: each is
#      applied to a copy of release.yml and the linter must go red;
#   4. the job/step topology of BOTH modes, by evaluating every `if:` for
#      live and for dry (`--simulate`), so a guard flipped the wrong way --
#      which would break a LIVE release -- fails here;
#   5. every mutating step of the real release.yml, EXECUTED with stubbed
#      git/gh and its real `env:`, refuses before calling anything when not
#      live, and when live reaches its command with no empty argument (so the
#      refusal is what stops it, and the live plumbing is intact);
#   6. the linter and this test are wired into CI.
#
# Run manually with: bash scripts/release_dry_run_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_release_dry_run.py"
RESOLVER="$REPO_ROOT/scripts/resolve_release_mode.sh"
RELEASE_YML="$REPO_ROOT/.github/workflows/release.yml"
CI_YML="$REPO_ROOT/.github/workflows/ci.yml"
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

# --- 1. the resolver ---------------------------------------------------------

resolve() {
    # resolve <value|__unset__> -> the `live=` value written, plus stdout
    local out="$TEST_TMP/mode.out" rc=0
    : > "$out"
    if [[ "$1" == "__unset__" ]]; then
        env -u DRY_RUN_INPUT GITHUB_OUTPUT="$out" bash "$RESOLVER" > "$TEST_TMP/mode.log" 2>&1 || rc=$?
    else
        DRY_RUN_INPUT="$1" GITHUB_OUTPUT="$out" bash "$RESOLVER" > "$TEST_TMP/mode.log" 2>&1 || rc=$?
    fi
    [[ "$rc" -eq 0 ]] || { echo "resolver-exit-$rc"; return; }
    grep -E '^live=' "$out" | paste -sd, -
}

check "resolver: dry_run=false is a live run" "$(resolve false)" "live=true"
check "resolver: dry_run=true is a dry run" "$(resolve true)" "live=false"
check "resolver: an empty input is a dry run (fail closed)" "$(resolve '')" "live=false"
check "resolver: an unset input is a dry run (fail closed)" "$(resolve __unset__)" "live=false"
check "resolver: 'False' is not 'false', so a dry run" "$(resolve False)" "live=false"
resolve '' > /dev/null
check "resolver: an unexpected value is reported, not silently accepted" \
    "$(grep -c 'Treating this run as a dry run' "$TEST_TMP/mode.log")" "1"

# --- 2. the linter against real files ----------------------------------------

SELF_RC=0
python3 "$LINTER" --self-test > /dev/null 2>&1 || SELF_RC=$?
check "linter self-test passes" "$SELF_RC" "0"

REPO_RC=0
python3 "$LINTER" "$REPO_ROOT/.github/workflows" > "$TEST_TMP/repo.out" 2>&1 || REPO_RC=$?
check "linter passes the repo's workflows" "$REPO_RC" "0"
[[ "$REPO_RC" -eq 0 ]] || cat "$TEST_TMP/repo.out" >&2

# The pre-fix shape, abbreviated from origin/main 9c09d2dbf (the three
# mutating steps of the version-bump job are as they were; the rest is cut to
# the minimum): guarded tag steps and the UNGUARDED version-bump job. If the linter does not flag this,
# it would not have caught the bug it exists for.
mkdir -p "$TEST_TMP/prefix"
cat > "$TEST_TMP/prefix/release.yml" <<'YAML'
name: Release
on:
  workflow_dispatch:
    inputs:
      dry_run:
        description: 'Dry run - show what would be done without publishing'
        required: false
        type: boolean
        default: false
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v7
  update_versions:
    name: Update versions and create PR
    runs-on: ubuntu-latest
    needs: validate
    steps:
      - name: Checkout code
        uses: actions/checkout@v7
        with:
          token: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}

      - name: Create release branch
        run: |
          BRANCH="release/v${{ needs.validate.outputs.version }}"
          git checkout -b "$BRANCH"
          git add -A
          git commit -m "chore: bump versions"
          git push origin "$BRANCH"
          echo "RELEASE_BRANCH=$BRANCH" >> $GITHUB_ENV

      - name: Create Pull Request
        id: create_pr
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
        run: |
          PR_URL=$(gh pr create \
            --title "build: release ${{ needs.validate.outputs.version }}" \
            --base main \
            --head "release/v${{ needs.validate.outputs.version }}")

      - name: Enable auto-merge
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
        run: |
          gh pr merge ${{ steps.create_pr.outputs.pr_number }} --auto
  create_release:
    runs-on: ubuntu-latest
    needs: [validate]
    if: always() && (needs.verify_publishable.result == 'success' || inputs.dry_run)
    steps:
      - name: Create and push tag
        if: ${{ !inputs.dry_run }}
        run: |
          git push origin "v$VERSION"
YAML
PREFIX_RC=0
python3 "$LINTER" "$TEST_TMP/prefix" > "$TEST_TMP/prefix.out" 2>&1 || PREFIX_RC=$?
check "linter fails the pre-fix release.yml" "$PREFIX_RC" "1"
for step in "Create release branch" "Create Pull Request" "Enable auto-merge"; do
    check "pre-fix: names the unguarded step '$step'" \
        "$(grep -cF "step \`$step\` mutates" "$TEST_TMP/prefix.out" | tr -d ' ' | sed 's/^[1-9][0-9]*$/flagged/')" "flagged"
done
check "pre-fix: names the per-step inputs.dry_run reads" \
    "$(grep -c 'read outside the mode step' "$TEST_TMP/prefix.out" | sed 's/^[1-9][0-9]*$/flagged/')" "flagged"

# --- 3. likely future regressions of the REAL file ---------------------------
#
# Each mutation is a small python edit of a copy of release.yml. A mutation
# that fails to apply is a FAILURE, not a pass: an unapplied mutation proves
# nothing about the linter (it is how earlier pins in this repo went vacuous).

mutate_case() {
    # mutate_case <description> <expected diagnostic substring> <python-expression-over-s>
    local desc="$1" needle="$2" expr="$3" dir="$TEST_TMP/mut" rc=0
    rm -rf "$dir" && mkdir -p "$dir"
    if ! python3 - "$RELEASE_YML" "$dir/release.yml" "$expr" <<'PY'
import sys
src, dst, expr = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(src).read()
new = eval(expr, {"s": s})
if new == s:
    sys.exit(3)
open(dst, "w").write(new)
PY
    then
        echo "FAIL - mutation did not apply: $desc" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    python3 "$LINTER" "$dir" > "$dir/out" 2>&1 || rc=$?
    # The specific diagnostic, not just a non-zero exit: a mutation that also
    # trips an unrelated rule must not keep this green after the intended
    # rule breaks.
    check "linter catches: $desc" \
        "$rc:$(grep -cF -- "$needle" "$dir/out" | sed 's/^[1-9][0-9]*$/named/')" "1:named"
}

mutate_case "the push step loses its if: guard" "step \`Push the release branch\` mutates" \
    "s.replace(\"      - name: Push the release branch\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Push the release branch\n\", 1)"
mutate_case "the push step's guard is ORed away" "step \`Push the release branch\` mutates" \
    "s.replace(\"      - name: Push the release branch\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Push the release branch\n        if: always() || needs.validate.outputs.live == 'true'\n\", 1)"
mutate_case "the auto-merge step loses its in-script refusal" "does not refuse in its own script" \
    "s.replace('          [ \"\${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error title=Refusing in a dry run::Not enabling auto-merge: this run is not live (#5936).\"; exit 1; }\n', '', 1)"
mutate_case "create_release loses its job-level live guard and the tag step its own" "step \`Create and push tag\` mutates" \
    "s.replace(\" && needs.validate.outputs.live == 'true'\n    env:\n      RELEASE_LIVE\", \"\n    env:\n      RELEASE_LIVE\", 1).replace(\"      - name: Create and push tag\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Create and push tag\n\", 1)"
mutate_case "a step goes back to reading inputs.dry_run" "read outside the mode step" \
    "s.replace(\"      - name: Create GitHub release\n        if: needs.validate.outputs.live == 'true'\", \"      - name: Create GitHub release\n        if: \${{ !inputs.dry_run }}\", 1)"
mutate_case "a new unguarded step edits a release in a dry-run job" "step \`Sneaky\` mutates" \
    "s.replace('      - name: Compose the release PR\n', '      - name: Sneaky\n        run: gh release edit v1 --draft=false\n\n      - name: Compose the release PR\n', 1)"
mutate_case "a new unguarded step hides a merge in bash -c" "step \`Sneaky\` mutates" \
    "s.replace('      - name: Compose the release PR\n', '      - name: Sneaky\n        run: bash -c \"gh pr merge 1 --auto\"\n\n      - name: Compose the release PR\n', 1)"
mutate_case "update_versions persists its push credential in a dry run" "persist-credentials" \
    "s.replace(\"          persist-credentials: \${{ needs.validate.outputs.live == 'true' }}\n\", '', 1)"
mutate_case "the mode step stops using the tested resolver" "resolve_release_mode.sh" \
    "s.replace('run: bash scripts/resolve_release_mode.sh', 'run: echo live=true >> \"\$GITHUB_OUTPUT\"', 1)"
mutate_case "the dry_run input is renamed, taking release.yml out of scope" "nothing was checked" \
    "s.replace('      dry_run:\n', '      rehearse:\n', 1)"
mutate_case "the rehearsal job gets a write token" "read-only \`permissions:\`" \
    "s.replace('    permissions:\n      contents: read\n    env:\n      RELEASE_SHA:', '    permissions:\n      contents: write\n    env:\n      RELEASE_SHA:', 1)"
# Live-path plumbing: each of these left a live release broken while every
# dry-run check stayed green.
mutate_case "Create Pull Request stops writing pr_number" "never writes \`pr_number=\`" \
    "s.replace('          echo \"pr_number=\$PR_NUMBER\" >> \"\$GITHUB_OUTPUT\"\n', '', 1)"
mutate_case "update_versions stops exporting pr_number" "does not declare" \
    "s.replace('      pr_number: \${{ steps.create_pr.outputs.pr_number }}\n', '', 1)"

# --- 4. topology of both modes -------------------------------------------------

simulate() { python3 "$LINTER" --simulate "$RELEASE_YML" "$1"; }
LIVE_RUN=$(simulate live)
DRY_RUN=$(simulate dry)
check "live: exactly these jobs run" \
    "$(grep -v ' / ' <<< "$LIVE_RUN" | paste -sd' ' -)" \
    "validate update_versions wait_for_pr verify_publishable create_release summary"
check "dry: exactly these jobs run" \
    "$(grep -v ' / ' <<< "$DRY_RUN" | paste -sd' ' -)" \
    "validate update_versions dry_run_rehearsal summary"
for step in "update_versions / Push the release branch" "update_versions / Create Pull Request" \
            "update_versions / Enable auto-merge" "create_release / Create and push tag" \
            "create_release / Create GitHub release" \
            "validate / Verify the crates.io token before anything is created"; do
    check "live runs '$step'" "$(grep -cxF "$step" <<< "$LIVE_RUN")" "1"
    check "dry does not run '$step'" "$(grep -cxF "$step" <<< "$DRY_RUN")" "0"
done
for step in "update_versions / Dry run: report what a live run would push" \
            "update_versions / Dry run: hand the bump commit to the rehearsal job"; do
    check "dry runs '$step'" "$(grep -cxF "$step" <<< "$DRY_RUN")" "1"
    check "live does not run '$step'" "$(grep -cxF "$step" <<< "$LIVE_RUN")" "0"
done

# --- 5. execute every mutating step ------------------------------------------
#
# Stubs record every call. In a dry run each step must exit non-zero with the
# refusal and record NOTHING. Live, each must reach a stubbed command -- the
# control that shows the extraction is the real script, so the dry-run half
# is not passing because the script was empty or broke early -- and with the
# step's real `env:` exported, no argument may be empty: a renamed env key
# would otherwise push `git push origin ""` on a live release.

STUBS="$TEST_TMP/stubs"
mkdir -p "$STUBS"
for tool in git gh cargo curl docker npm; do
    cat > "$STUBS/$tool" <<'SH'
#!/usr/bin/env bash
echo "$(basename "$0") $*" >> "$STUB_CALLS"
for a in "$@"; do [ -n "$a" ] || echo "EMPTYARG" >> "$STUB_CALLS"; done
if [[ "$(basename "$0")" == gh && "$1 $2" == "pr create" ]]; then
    echo "https://github.com/example/repo/pull/123"
fi
exit 0
SH
    chmod +x "$STUBS/$tool"
done

RUNS="$TEST_TMP/runs"
LIVE_CALLS=""
python3 "$LINTER" --extract-mutating-runs "$RELEASE_YML" "$RUNS"
EXTRACTED=$(cat "$RUNS"/*.name 2>/dev/null || true)
for expected in \
    "update_versions / Push the release branch" \
    "update_versions / Create Pull Request" \
    "update_versions / Enable auto-merge" \
    "create_release / Create and push tag" \
    "create_release / Create GitHub release"; do
    check "extractor finds mutating step '$expected'" \
        "$(grep -cxF "$expected" <<< "$EXTRACTED")" "1"
done
# Exactly those five. The extractor shares the linter's detector, so a step it
# misses is neither guarded nor executed here; a new mutating step must be
# added to the list above deliberately.
check "extractor finds exactly the five known mutating steps" \
    "$(grep -c . <<< "$EXTRACTED")" "5"

run_step() {
    # run_step <script> <RELEASE_LIVE value|__unset__> -> "rc=<n> calls=<n>"
    local script="$1" live="$2" calls="$TEST_TMP/calls" out="$TEST_TMP/step.out" rc=0
    : > "$calls"
    local -a envs=()
    mapfile -t envs < "${script%.sh}.env"
    envs+=(PATH="$STUBS:$PATH" STUB_CALLS="$calls" GITHUB_OUTPUT="$TEST_TMP/gh_output"
           GITHUB_ENV="$TEST_TMP/gh_env" RUNNER_TEMP="$TEST_TMP")
    if [[ "$live" == "__unset__" ]]; then
        env -u RELEASE_LIVE "${envs[@]}" bash -e "$script" > "$out" 2>&1 || rc=$?
    else
        env "${envs[@]}" RELEASE_LIVE="$live" bash -e "$script" > "$out" 2>&1 || rc=$?
    fi
    echo "rc=$rc calls=$(wc -l < "$calls" | tr -d ' ')"
}

for script in "$RUNS"/*.sh; do
    name=$(cat "${script%.sh}.name")
    for mode in false "" __unset__ TRUE; do
        result=$(run_step "$script" "$mode")
        check "'$name' refuses with RELEASE_LIVE=${mode:-<empty>} and calls nothing" \
            "$(sed -E 's/^rc=[1-9][0-9]* /rc=nonzero /' <<< "$result")" "rc=nonzero calls=0"
        check "'$name' names the dry run when it refuses (RELEASE_LIVE=${mode:-<empty>})" \
            "$(grep -c 'Refusing in a dry run' "$TEST_TMP/step.out")" "1"
    done
    result=$(run_step "$script" true)
    check "'$name' reaches its command when live (control)" \
        "$(sed -E 's/^rc=0 calls=[1-9][0-9]*$/reached/' <<< "$result")" "reached"
    check "'$name' passes no empty argument when live" \
        "$(grep -c EMPTYARG "$TEST_TMP/calls")" "0"
    LIVE_CALLS="$LIVE_CALLS$(cat "$TEST_TMP/calls")"$'\n'
done
# The commands a live release actually issues, with every value present.
for call in "git push origin PLACEHOLDER" "gh pr merge PLACEHOLDER --auto" \
            "git push origin vPLACEHOLDER"; do
    check "live issues '$call'" "$(grep -cxF "$call" <<< "$LIVE_CALLS")" "1"
done
check "live opens the PR from the composed title and body file" \
    "$(grep -cE '^gh pr create --title PLACEHOLDER --body-file PLACEHOLDER --base main --head PLACEHOLDER$' <<< "$LIVE_CALLS")" "1"

# --- 6. wiring ----------------------------------------------------------------
#
# A guard nobody runs is not a guard. Executable lines only, so a commented-out
# step does not count.

check "Rule Lint runs the linter's self-test" \
    "$(grep -cE '^[[:space:]]*run: python3 \.github/scripts/check_release_dry_run\.py --self-test[[:space:]]*$' "$CI_YML")" "1"
check "Rule Lint runs the linter over .github/workflows" \
    "$(grep -cE '^[[:space:]]*run: python3 \.github/scripts/check_release_dry_run\.py \.github/workflows[[:space:]]*$' "$CI_YML")" "1"
check "CI runs this test" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/release_dry_run_test\.sh[[:space:]]*$' "$CI_YML")" "1"
check "release.yml runs the tested resolver" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/resolve_release_mode\.sh[[:space:]]*$' "$RELEASE_YML")" "1"

if [[ "$FAILURES" -ne 0 ]]; then
    echo "FAILED: $FAILURES check(s)" >&2
    exit 1
fi
echo "PASS: release.yml's dry_run is side-effect free and guarded against regressions."
