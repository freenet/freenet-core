#!/usr/bin/env bash
# Self-test for scripts/pr-backlog-sweep.sh.
#
# WHAT THIS PINS, and why it is not decoration. Two properties, and the second
# is the one the sweep exists for:
#
#   1. CLASSIFICATION. A `statusCheckRollup` entry is either a CheckRun (carries
#      .status/.conclusion) or a legacy StatusContext (carries .state). Reading
#      only one of the two reads CI as permanently pending -- an easy mistake to
#      make and an invisible one, because the symptom is a report that just
#      never lists anything as green. Both shapes, plus queued/failed/unknown,
#      are exercised here against fixtures.
#
#   2. THAT A BROKEN SWEEP CANNOT REPORT SUCCESS. The sweep's whole value is
#      that "nothing is stalled" and "the query failed" look different. That is
#      a property of the FAILURE paths, which never run in a healthy CI run and
#      would otherwise be tested by nobody. The cases below drive it with a stub
#      `gh` that fails, one that returns an empty list while the corroborating
#      REST probe says otherwise, and one that truncates at the fetch limit;
#      each must exit non-zero. The precedent is a backup job that logged
#      "unreachable, skipping" and exited 0, reporting SUCCESS for 37 straight
#      days while no backups ran.
#
# Also pinned: an author-controlled PR title reaches the report as inert text,
# and never reaches the chat notification at all.
#
# Run manually: bash scripts/pr-backlog-sweep_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SWEEP="$SCRIPT_DIR/pr-backlog-sweep.sh"
# Absolute, because some cases below run the sweep with a deliberately empty
# PATH and `bash` itself would otherwise be unfindable.
BASH_BIN="$(command -v bash)"
FAILURES=0

if [[ ! -x "$SWEEP" ]]; then
    echo "FAIL - $SWEEP is missing or not executable" >&2
    exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/empty-path"

# Fixed clock so ages are deterministic.
NOW=1787594400
iso() { date -u -d "@$1" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -r "$1" +%Y-%m-%dT%H:%M:%SZ; }
days_ago() { iso $((NOW - $1 * 86400)); }

check() {
    # check <description> <expected> <actual>
    local desc="$1" want="$2" got="$3"
    if [[ "$got" == "$want" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc" >&2
        echo "         wanted: $want" >&2
        echo "         got:    $got" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

check_contains() {
    # check_contains <description> <haystack> <needle> <yes|no>
    local desc="$1" hay="$2" needle="$3" want="$4" got="no"
    case "$hay" in *"$needle"*) got="yes" ;; esac
    if [[ "$got" == "$want" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (wanted contains=$want, got contains=$got)" >&2
        echo "         needle: $needle" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

section() {
    # section <report> <heading substring> -> that section's body.
    # Searching the WHOLE report for a PR url is nearly vacuous: every stalled
    # PR appears in the idle section too, so "the green bucket lists #2" would
    # pass with the green bucket empty. Mutation-tested: dropping the
    # StatusContext branch left the whole-report form green.
    printf '%s\n' "$1" | awk -v h="$2" '
        index($0, "### ") == 1 { inblock = (index($0, h) > 0); next }
        inblock { print }'
}

sweep_counts() {
    # Prints "<green> <idle> <conflicting> <awaiting>" pulled back out of the rendered
    # headline, so the test reads the same numbers a human would.
    printf '%s\n' "$1" | grep -m1 'open PRs\.' \
        | grep -oE '\*\*[0-9]+\*\*' | tr -d '*' | paste -sd' ' -
}

# --- Fixture: one PR per classification case ---------------------------------
# Everything is 20 days old unless stated, so each case isolates the CI-state or
# mergeable dimension rather than the clock.
OLD="$(days_ago 20)"
FRESH="$(days_ago 1)"

cat > "$WORK/mixed.json" <<EOF
[
  {
    "number": 1, "title": "green via CheckRun", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/1",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"},
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SKIPPED", "completedAt": "$OLD"}
    ]
  },
  {
    "number": 2, "title": "green via legacy StatusContext", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/2",
    "statusCheckRollup": [
      {"__typename": "StatusContext", "state": "SUCCESS"}
    ]
  },
  {
    "number": 3, "title": "queued check is NOT green", "author": {"login": "bob"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/3",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"},
      {"__typename": "CheckRun", "status": "QUEUED", "conclusion": "", "completedAt": "0001-01-01T00:00:00Z"}
    ]
  },
  {
    "number": 4, "title": "failing check is NOT green", "author": {"login": "bob"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/4",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "FAILURE", "completedAt": "$OLD"}
    ]
  },
  {
    "number": 5, "title": "unrecognised rollup shape is NOT green", "author": {"login": "bob"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/5",
    "statusCheckRollup": [
      {"__typename": "SomethingNew", "verdict": "fine"}
    ]
  },
  {
    "number": 6, "title": "no checks at all is NOT green", "author": {"login": "bob"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/6",
    "statusCheckRollup": []
  },
  {
    "number": 7, "title": "green draft is excluded from the green bucket", "author": {"login": "carol"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": true, "url": "https://example.invalid/7",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  },
  {
    "number": 8, "title": "green, freshly commented", "author": {"login": "carol"},
    "createdAt": "$OLD", "updatedAt": "$FRESH", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/8",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  },
  {
    "number": 9, "title": "conflicting", "author": {"login": "dave"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "CONFLICTING",
    "isDraft": false, "url": "https://example.invalid/9",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "FAILURE", "completedAt": "$OLD"}
    ]
  }
]
EOF

REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/mixed.json" 2>/dev/null)"
# green: #1, #2, #8 (draft #7 excluded; #3/#4/#5/#6 are not green)
# idle:  everything except #8, whose updatedAt is 1 day old  = 8
# conflicting: #9
check "mixed fixture classifies green / idle / conflicting / awaiting" "3 8 1 0" "$(sweep_counts "$REPORT")"
GREEN_SECTION="$(section "$REPORT" "Green and unmerged")"
check_contains "green bucket lists the CheckRun-green PR" "$GREEN_SECTION" "example.invalid/1" yes
check_contains "green bucket lists the legacy StatusContext-green PR" "$GREEN_SECTION" "example.invalid/2" yes
check_contains "green bucket excludes a PR with a queued check" "$GREEN_SECTION" "example.invalid/3" no
check_contains "green bucket excludes a PR with a failing check" "$GREEN_SECTION" "example.invalid/4" no
check_contains "green bucket excludes an unrecognised rollup shape" "$GREEN_SECTION" "example.invalid/5" no
check_contains "green bucket excludes a PR with no checks" "$GREEN_SECTION" "example.invalid/6" no
check_contains "green bucket excludes the green draft" "$GREEN_SECTION" "example.invalid/7" no
# The point of measuring green-age from the last check rather than updatedAt: a
# comment on a green PR must not reset the clock and hide it from the report.
check_contains "a comment does not reset the green age" "$GREEN_SECTION" "/8) 20d" yes
check_contains "a draft is labelled as one in the idle bucket" "$(section "$REPORT" "No activity")" "_(draft)_" yes
check_contains "the report is grouped by author" "$GREEN_SECTION" "**@alice** (2)" yes

# --- (d) Runs held for maintainer approval ------------------------------------
# Grouped BY BRANCH, not by run. 22 pending runs on one branch is ONE stuck PR;
# reporting the raw run count would overstate the problem by an order of
# magnitude, which is the failure mode this grouping exists to avoid.
cat > "$WORK/runs.json" <<EOF
[
  {"databaseId": 1, "headBranch": "contrib-branch", "workflowName": "CI", "createdAt": "$(days_ago 30)"},
  {"databaseId": 2, "headBranch": "contrib-branch", "workflowName": "CI", "createdAt": "$(days_ago 12)"},
  {"databaseId": 3, "headBranch": "contrib-branch", "workflowName": "Netcheck", "createdAt": "$(days_ago 12)"},
  {"databaseId": 4, "headBranch": "abandoned-branch", "workflowName": "CI", "createdAt": "$(days_ago 5)"}
]
EOF
# One PR sits on contrib-branch; abandoned-branch has none.
jq --arg b "contrib-branch" '.[0].headRefName = $b' "$WORK/mixed.json" > "$WORK/mixed-branches.json"

RUNS_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/mixed-branches.json" --runs-input "$WORK/runs.json" 2>/dev/null)"
check_contains "branches are counted, not runs" "$RUNS_REPORT" "**2** branch(es) awaiting CI approval" yes
check_contains "the per-branch pending run count is reported" "$RUNS_REPORT" "3 run(s) pending" yes
# The age must be of the OLDEST pending run, not the newest or the average:
# 30 days, not 12.
check_contains "the age is that of the oldest pending run" "$RUNS_REPORT" "oldest 30d" yes
check_contains "a branch with an open PR resolves to it" "$RUNS_REPORT" '3 run(s) pending, oldest 30d — #1' yes
check_contains "a branch with no open PR says so" "$RUNS_REPORT" "no open PR (stale runs)" yes
check_contains "the raw pending-run total is still stated" "$RUNS_REPORT" "4 pending run(s) across 2 branch(es)" yes
# The gate is correct; only the fact that nobody watches it is the bug. If this
# text goes, so does the reason the next reader does not "fix" the queue by
# letting untrusted forks run workflows on this repo.
check_contains "the report says not to loosen the approval policy" "$RUNS_REPORT" "do not loosen the policy" yes

NO_RUNS_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/mixed.json" 2>/dev/null)"
check_contains "an empty approval queue renders as none" "$NO_RUNS_REPORT" "**0** branch(es) awaiting CI approval" yes

# --- An author-controlled title must not be able to run anything -------------
CANARY="$WORK/pwned"
cat > "$WORK/inject.json" <<EOF
[
  {
    "number": 42, "title": "\$(touch $CANARY) \`touch $CANARY\` ; touch $CANARY",
    "author": {"login": "mallory"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/42",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  }
]
EOF
INJECT_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/inject.json" 2>/dev/null)"
if [[ -e "$CANARY" ]]; then
    echo "FAIL - a crafted PR title EXECUTED: $CANARY exists" >&2
    FAILURES=$((FAILURES + 1))
else
    echo "ok   - a crafted PR title does not execute"
fi
check_contains "the crafted title is rendered as inert text" "$INJECT_REPORT" 'touch' yes

# The report is read as markdown on a maintainer-only page, so a title of the
# form "[click me](https://elsewhere)" must not become a working link.
cat > "$WORK/markdown.json" <<EOF
[
  {
    "number": 43, "title": "[click me](https://evil.invalid) and \`code\` and <b>",
    "author": {"login": "mallory"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/43", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  }
]
EOF
MD_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/markdown.json" 2>/dev/null)"
check_contains "a link in a PR title is escaped, not rendered" "$MD_REPORT" '\[click me\](https://evil.invalid)' yes
check_contains "the raw unescaped link form is absent" "$MD_REPORT" '— [click me](https://evil.invalid)' no
# The needle is deliberately literal: it is the escaped text the report should
# contain, backticks included, so SC2016 is an accurate observation about a
# string that must not expand.
# shellcheck disable=SC2016
check_contains "backticks and angle brackets in a title are escaped" "$MD_REPORT" '\`code\` and \<b\>' yes

# --- The failure paths: a broken sweep must never look clean -----------------
# A stub `gh` drives the abort paths that a healthy CI run never touches.
STUB="$WORK/stub"
mkdir -p "$STUB"

write_stub_gh() {
    # write_stub_gh <fail|empty|full|runs-fail> <count the REST probe reports>
    # MODE and the probe count are baked in at write time; the stub dispatches
    # on its own first argument to tell `gh api` / `gh run` / `gh pr` apart.
    cat > "$STUB/gh" <<STUBEOF
#!/usr/bin/env bash
MODE="$1"
case "\${1:-}" in
  api)
    echo "$2"
    exit 0
    ;;
  run)
    if [ "\$MODE" = "runs-fail" ]; then
      echo "HTTP 403: Resource not accessible by integration" >&2
      exit 1
    fi
    echo "[]"
    exit 0
    ;;
esac
case "\$MODE" in
  fail)  echo "HTTP 401: Bad credentials" >&2; exit 1 ;;
  empty) echo "[]"; exit 0 ;;
  full)  jq -cn --argjson n 5 '[range(\$n) | {number: ., title: "x", author: {login: "a"}, createdAt: "$OLD", updatedAt: "$OLD", mergeable: "MERGEABLE", isDraft: false, url: "https://example.invalid/x", headRefName: "b", statusCheckRollup: []}]' ;;
  runs-fail) jq -cn '[{number: 1, title: "x", author: {login: "a"}, createdAt: "$OLD", updatedAt: "$OLD", mergeable: "MERGEABLE", isDraft: false, url: "https://example.invalid/1", headRefName: "b", statusCheckRollup: []}]' ;;
esac
STUBEOF
    chmod +x "$STUB/gh"
}

sweep_rc() {
    local rc=0
    SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 PATH="$STUB:$PATH" \
        "$BASH_BIN" "$SWEEP" >/dev/null 2>&1 || rc=$?
    echo "$rc"
}

write_stub_gh fail 0
check "an API/auth failure aborts non-zero" "1" "$(sweep_rc)"

write_stub_gh empty 3
check "an empty listing the REST probe contradicts aborts non-zero" "1" "$(sweep_rc)"

write_stub_gh full 5
check "a listing truncated at the fetch limit aborts non-zero" "1" "$(sweep_rc)"

# The action_required query is the SECOND unwatched queue this sweep reports.
# If it fails, "no runs are waiting for approval" is exactly what a broken query
# prints, so it aborts rather than reporting a clean CI-approval queue.
write_stub_gh runs-fail 1
check "a failed action_required query aborts non-zero" "1" "$(sweep_rc)"

# The one case that legitimately reports nothing: both paths agree the repo has
# no open PRs. This must SUCCEED, or the sweep cries wolf forever.
write_stub_gh empty 0
EMPTY_RC=0
EMPTY_OUT="$(SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 PATH="$STUB:$PATH" \
    "$BASH_BIN" "$SWEEP" 2>/dev/null)" || EMPTY_RC=$?
check "a genuinely empty backlog exits 0" "0" "$EMPTY_RC"
check_contains "a genuinely empty backlog still renders a report" "$EMPTY_OUT" "0 open PRs." yes

# `gh` absent entirely is the same class of breakage as `gh` failing.
NO_GH_RC=0
SWEEP_NOW_EPOCH="$NOW" PATH="$WORK/empty-path" "$BASH_BIN" "$SWEEP" >/dev/null 2>&1 || NO_GH_RC=$?
check "a missing gh CLI aborts non-zero" "1" "$NO_GH_RC"

# --- Step summary and notification wiring ------------------------------------
SUMMARY="$WORK/summary.md"
OUTPUTS="$WORK/outputs.txt"
: > "$SUMMARY"
: > "$OUTPUTS"
SWEEP_NOW_EPOCH="$NOW" GITHUB_STEP_SUMMARY="$SUMMARY" GITHUB_OUTPUT="$OUTPUTS" \
    "$BASH_BIN" "$SWEEP" --input "$WORK/mixed.json" >/dev/null 2>&1
check_contains "the step summary is written" "$(cat "$SUMMARY")" "PR backlog sweep" yes
check_contains "notify is requested when there is something to report" "$(cat "$OUTPUTS")" "notify=true" yes
check_contains "the headline carries the counts" "$(cat "$OUTPUTS")" "headline=3 PR(s) green" yes
check_contains "the headline carries the CI-approval count" "$(cat "$OUTPUTS")" "awaiting CI approval" yes
# The headline is forwarded to a chat room. If a PR title could reach it, a
# crafted title would be forwarded verbatim into that message.
check_contains "the headline carries no PR title" "$(grep '^headline=' "$OUTPUTS")" "green via CheckRun" no

# Branch names are attacker-controlled too, and section (d) is the one place a
# branch name enters the report. It must not ride out to the chat room either.
: > "$OUTPUTS"
SWEEP_NOW_EPOCH="$NOW" GITHUB_OUTPUT="$OUTPUTS" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/mixed-branches.json" --runs-input "$WORK/runs.json" >/dev/null 2>&1
# The paired positive check matters: `check_contains ... no` passes trivially on
# an empty haystack, so without this a broken $OUTPUTS would look like a pass.
check_contains "a headline was written for the branch run" "$(grep '^headline=' "$OUTPUTS")" "awaiting CI approval" yes
check_contains "the headline carries no branch name" "$(grep '^headline=' "$OUTPUTS")" "contrib-branch" no

# A backlog whose ONLY finding is an unapproved CI queue must still notify --
# otherwise the queue that had gone unwatched for a month stays unwatched.
cat > "$WORK/clean.json" <<EOF
[
  {
    "number": 1, "title": "fresh and green", "author": {"login": "alice"},
    "createdAt": "$FRESH", "updatedAt": "$FRESH", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/1", "headRefName": "contrib-branch",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$FRESH"}
    ]
  }
]
EOF
: > "$OUTPUTS"
ONLY_RUNS="$(SWEEP_NOW_EPOCH="$NOW" GITHUB_OUTPUT="$OUTPUTS" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/clean.json" --runs-input "$WORK/runs.json" 2>/dev/null)"
check "only the approval queue is stalled" "0 0 0 2" "$(sweep_counts "$ONLY_RUNS")"
check_contains "an approval queue alone still notifies" "$(cat "$OUTPUTS")" "notify=true" yes

write_stub_gh empty 0
: > "$OUTPUTS"
SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 GITHUB_OUTPUT="$OUTPUTS" PATH="$STUB:$PATH" \
    "$BASH_BIN" "$SWEEP" >/dev/null 2>&1
check_contains "a clean sweep does NOT notify" "$(cat "$OUTPUTS")" "notify=false" yes

# --- The workflow must actually run this script ------------------------------
# A self-test nothing invokes is coverage in a listing and a gate on nothing;
# the same is true of a sweep no workflow runs.
WF="$SCRIPT_DIR/../.github/workflows/pr-backlog-sweep.yml"
if [[ -f "$WF" ]]; then
    WF_TEXT="$(cat "$WF")"
    check_contains "the workflow invokes scripts/pr-backlog-sweep.sh" "$WF_TEXT" "scripts/pr-backlog-sweep.sh" yes
    check_contains "the workflow is scheduled" "$WF_TEXT" "schedule:" yes
    check_contains "the workflow can be run on demand" "$WF_TEXT" "workflow_dispatch:" yes
    # If the sweep step itself were continue-on-error, every abort above would
    # go back to being a green run with an empty report -- the exact shape the
    # failure paths exist to prevent.
    # Comments are stripped first: this step's comment block explains WHY it is
    # not continue-on-error, and a substring search would otherwise match the
    # explanation and pass no matter what the step actually does.
    SWEEP_STEP="$(printf '%s\n' "$WF_TEXT" \
        | sed -n '/- name: Run the sweep/,/^      - name:/p' \
        | sed -E 's/^[[:space:]]*#.*$//')"
    check_contains "the sweep step block was located" "$SWEEP_STEP" "pr-backlog-sweep.sh" yes
    check_contains "the sweep step is not continue-on-error" "$SWEEP_STEP" "continue-on-error:" no
else
    echo "FAIL - .github/workflows/pr-backlog-sweep.yml is missing; nothing runs the sweep" >&2
    FAILURES=$((FAILURES + 1))
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All PR backlog sweep assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
