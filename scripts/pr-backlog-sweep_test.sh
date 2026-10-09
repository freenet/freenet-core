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
    # Prints "<green> <idle> <conflicting> <awaiting>" pulled back out of the
    # rendered headline, so the test reads the same numbers a human would.
    #
    # `awk NR==1{...;exit}` rather than `grep -m1`: `-m1` is a short-circuiting
    # consumer, and the repo's SIGPIPE-under-pipefail audit grep looks for
    # `grep -[a-z]*q`, `head` and `read` -- it would not have SEEN this one. It
    # was benign here (the status is discarded), but a form the project's own
    # audit cannot find is the wrong form to leave lying around in a file whose
    # subject is instruments that fail invisibly.
    printf '%s\n' "$1" \
        | awk '/open PRs\./ { print; exit }' \
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

# --- Gaps found by review (each of these mutations previously SURVIVED) ------

# A PENDING legacy StatusContext must not read as green. Every open PR on this
# repo carries StatusContexts (license/cla, rule-review/warnings, snyk), so a
# mutation making a pending one read as PASS would ship a wrong green list
# silently -- and `check_state` is the function this file's header says it
# exists to protect. Only SUCCESS was ever fed to that branch before.
cat > "$WORK/pending-status.json" <<EOF
[
  {
    "number": 11, "title": "pending legacy status is NOT green", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/11", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"},
      {"__typename": "StatusContext", "state": "PENDING", "startedAt": "$OLD"}
    ]
  },
  {
    "number": 12, "title": "expected legacy status is NOT green", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/12", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "StatusContext", "state": "EXPECTED", "startedAt": "$OLD"}
    ]
  }
]
EOF
PENDING_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/pending-status.json" 2>/dev/null)"
check "a pending or expected StatusContext is not green" "0 2 0 0" "$(sweep_counts "$PENDING_REPORT")"

# A StatusContext carries startedAt and no completedAt, so a
# StatusContext-only PR used to fall back to updatedAt -- meaning a comment DID
# reset its green age, the one case the CheckRun-based pin cannot see.
cat > "$WORK/status-only.json" <<EOF
[
  {
    "number": 13, "title": "green via StatusContext, freshly commented", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$FRESH", "mergeable": "MERGEABLE",
    "isDraft": false, "url": "https://example.invalid/13", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "StatusContext", "state": "SUCCESS", "startedAt": "$OLD"}
    ]
  }
]
EOF
STATUS_ONLY="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/status-only.json" 2>/dev/null)"
check_contains "a StatusContext-only PR dates its green age from the status, not a comment" \
    "$(section "$STATUS_ONLY" "Green and unmerged")" "/13) 20d" yes

# A green PR that is ALSO conflicting must not be listed under "Nothing is
# blocking these" -- it is blocked, and the report contradicted itself by
# putting the same PR in both sections. Live run at the time: 3 of 8.
cat > "$WORK/green-conflicting.json" <<EOF
[
  {
    "number": 14, "title": "green checks but conflicting", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "CONFLICTING",
    "isDraft": false, "url": "https://example.invalid/14", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  }
]
EOF
GC_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/green-conflicting.json" 2>/dev/null)"
check "a conflicting PR is not counted as green-and-unblocked" "0 1 1 0" "$(sweep_counts "$GC_REPORT")"
check_contains "the conflicting PR is absent from the green section" \
    "$(section "$GC_REPORT" "Green and unmerged")" "example.invalid/14" no
check_contains "the conflicting PR is present in the conflicting section" \
    "$(section "$GC_REPORT" "Decayed to CONFLICTING")" "example.invalid/14" yes

# The UNKNOWN-mergeability note is the honesty mechanism for the conflict count
# -- it says the number is a LOWER BOUND. It fired on live data the day this
# landed (3 PRs), and deleting it previously left the suite green.
cat > "$WORK/unknown-mergeable.json" <<EOF
[
  {
    "number": 15, "title": "mergeability not yet computed", "author": {"login": "alice"},
    "createdAt": "$OLD", "updatedAt": "$OLD", "mergeable": "UNKNOWN",
    "isDraft": false, "url": "https://example.invalid/15", "headRefName": "b",
    "statusCheckRollup": [
      {"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}
    ]
  }
]
EOF
UNK_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$WORK/unknown-mergeable.json" 2>/dev/null)"
check_contains "an UNKNOWN mergeability is declared a LOWER BOUND" "$UNK_REPORT" "LOWER BOUND" yes
check_contains "a fully-resolved listing carries no lower-bound note" "$REPORT" "LOWER BOUND" no

# Branch-to-PR resolution needs headRefName. mixed.json deliberately has none,
# so combining it with the runs fixture exercises the warning that says so --
# without it, every branch would silently read "no open PR", a wrong answer
# that looks like a finding.
NO_REF="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/mixed.json" --runs-input "$WORK/runs.json" 2>/dev/null)"
check_contains "a listing with no headRefName says branches could not be resolved" \
    "$NO_REF" "carries no \`headRefName\`" yes
check_contains "a listing WITH headRefName carries no such warning" \
    "$RUNS_REPORT" "carries no \`headRefName\`" no

# Two forks can use the same branch name, and neither gh field carries the
# owner, so such a group merges distinct PRs and the branch count understates
# them. It must be labelled rather than presented as one stuck PR.
cat > "$WORK/fork-collision.json" <<EOF
[
  {"number": 100, "title": "fork A", "author": {"login": "a"}, "createdAt": "$OLD",
   "updatedAt": "$OLD", "mergeable": "MERGEABLE", "isDraft": false,
   "url": "https://example.invalid/100", "headRefName": "patch-1",
   "statusCheckRollup": [{"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}]},
  {"number": 101, "title": "fork B", "author": {"login": "b"}, "createdAt": "$OLD",
   "updatedAt": "$OLD", "mergeable": "MERGEABLE", "isDraft": false,
   "url": "https://example.invalid/101", "headRefName": "patch-1",
   "statusCheckRollup": [{"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}]}
]
EOF
cat > "$WORK/fork-runs.json" <<EOF
[{"databaseId": 9, "headBranch": "patch-1", "headSha": "deadbeef", "workflowName": "CI", "createdAt": "$OLD"}]
EOF
FORK_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/fork-collision.json" --runs-input "$WORK/fork-runs.json" 2>/dev/null)"
# Needle is the ROW marker "⚠ AMBIGUOUS:", not the bare word: the footer prose
# explains that such groups are "marked AMBIGUOUS above", so a bare-word search
# matches the explanation and the negative case can never fail. Same anchor trap
# as the workflow pins above, hit twice in one file.
check_contains "a branch name shared by two open PRs is flagged ambiguous" "$FORK_REPORT" "⚠ AMBIGUOUS:" yes
check_contains "an unambiguous branch is not flagged" "$RUNS_REPORT" "⚠ AMBIGUOUS:" no

# A run whose branch matches nothing is still resolved when its head SHA does.
# SHA cannot collide across forks, so this is the resolution that survives the
# ambiguity above.
cat > "$WORK/sha-runs.json" <<EOF
[{"databaseId": 10, "headBranch": "renamed-since", "headSha": "cafe1234", "workflowName": "CI", "createdAt": "$OLD"}]
EOF
cat > "$WORK/sha-prs.json" <<EOF
[{"number": 200, "title": "matched by sha", "author": {"login": "a"}, "createdAt": "$OLD",
  "updatedAt": "$OLD", "mergeable": "MERGEABLE", "isDraft": false,
  "url": "https://example.invalid/200", "headRefName": "other-name", "headRefOid": "cafe1234",
  "statusCheckRollup": [{"__typename": "CheckRun", "status": "COMPLETED", "conclusion": "SUCCESS", "completedAt": "$OLD"}]}]
EOF
SHA_REPORT="$(SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" \
    --input "$WORK/sha-prs.json" --runs-input "$WORK/sha-runs.json" 2>/dev/null)"
check_contains "a run is resolved to its PR by head SHA when the branch name does not match" \
    "$SHA_REPORT" "oldest 20d — #200" yes

# The field-blindness gates: the REST probe corroborates that the LISTING is
# real, these corroborate that the FIELDS inside it arrived. A rollup that came
# back empty for every PR would report "0 green" and exit 0 -- indistinguishable
# from a clean backlog, and the green bucket is what the sweep is FOR.
BLIND_ROLLUP="$WORK/blind-rollup.json"
jq -c 'map(. + {statusCheckRollup: []})' "$WORK/mixed.json" > "$BLIND_ROLLUP"
BLIND_RC=0
SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$BLIND_ROLLUP" >/dev/null 2>&1 || BLIND_RC=$?
check "a listing whose every rollup is empty aborts non-zero" "1" "$BLIND_RC"

BLIND_MERGE="$WORK/blind-mergeable.json"
jq -c 'map(. + {mergeable: null})' "$WORK/mixed.json" > "$BLIND_MERGE"
BLIND_M_RC=0
SWEEP_NOW_EPOCH="$NOW" "$BASH_BIN" "$SWEEP" --input "$BLIND_MERGE" >/dev/null 2>&1 || BLIND_M_RC=$?
check "a listing whose every mergeable is null aborts non-zero" "1" "$BLIND_M_RC"

# ...but a SINGLE PR legitimately having no checks must NOT abort, or the sweep
# cries wolf on a normal repo. mixed.json's #6 is exactly that case and the
# suite above already runs it clean; assert it explicitly so a future tightening
# of the gate to per-PR is caught here rather than in production.
check_contains "a single PR with no checks does not abort the sweep" "$REPORT" "PR backlog sweep" yes

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

# The mergeability refetch is a live-network path that nothing exercised:
# deleting it whole (sleep included) previously left the suite green. The stub
# below returns UNKNOWN on the first `pr list` and CONFLICTING on the second, so
# only a sweep that actually refetches reports the conflict. SWEEP_REFETCH_SLEEP
# exists so this costs no wall-clock; it defaults to the real 10s.
cat > "$STUB/gh" <<STUBEOF
#!/usr/bin/env bash
if [ "\${1:-}" = "api" ]; then echo 1; exit 0; fi
if [ "\${1:-}" = "run" ]; then echo "[]"; exit 0; fi
CALLS="$WORK/prlist-calls"
n=\$(( \$(cat "\$CALLS" 2>/dev/null || echo 0) + 1 ))
echo "\$n" > "\$CALLS"
if [ "\$n" -eq 1 ]; then M=UNKNOWN; else M=CONFLICTING; fi
jq -cn --arg m "\$M" '[{number: 1, title: "x", author: {login: "a"}, createdAt: "$OLD", updatedAt: "$OLD", mergeable: \$m, isDraft: false, url: "https://example.invalid/1", headRefName: "b", headRefOid: "abc", statusCheckRollup: [{__typename: "CheckRun", status: "COMPLETED", conclusion: "SUCCESS", completedAt: "$OLD"}]}]'
STUBEOF
chmod +x "$STUB/gh"
rm -f "$WORK/prlist-calls"
REFETCH_OUT="$(SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 SWEEP_REFETCH_SLEEP=0 PATH="$STUB:$PATH" \
    "$BASH_BIN" "$SWEEP" 2>/dev/null)"
check "an UNKNOWN mergeability triggers exactly one refetch" "2" "$(cat "$WORK/prlist-calls")"
check "the refetched mergeability is the one reported" "0 1 1 0" "$(sweep_counts "$REFETCH_OUT")"

# The action_required listing gets the same truncation guard as the PR listing.
# Its PR-side twin was covered and this one was not -- an asymmetry that would
# have let a truncated approval queue report a short, confident number.
cat > "$STUB/gh" <<STUBEOF
#!/usr/bin/env bash
if [ "\${1:-}" = "api" ]; then echo 1; exit 0; fi
if [ "\${1:-}" = "run" ]; then
  jq -cn --argjson n 5 '[range(\$n) | {databaseId: ., headBranch: "b", headSha: "s", workflowName: "CI", createdAt: "$OLD"}]'
  exit 0
fi
jq -cn '[{number: 1, title: "x", author: {login: "a"}, createdAt: "$OLD", updatedAt: "$OLD", mergeable: "MERGEABLE", isDraft: false, url: "https://example.invalid/1", headRefName: "b", headRefOid: "abc", statusCheckRollup: [{__typename: "CheckRun", status: "COMPLETED", conclusion: "SUCCESS", completedAt: "$OLD"}]}]'
STUBEOF
chmod +x "$STUB/gh"
RUNS_TRUNC_RC=0
SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 SWEEP_REFETCH_SLEEP=0 PATH="$STUB:$PATH" \
    "$BASH_BIN" "$SWEEP" >/dev/null 2>&1 || RUNS_TRUNC_RC=$?
check "an action_required listing truncated at the fetch limit aborts non-zero" "1" "$RUNS_TRUNC_RC"

# gh writes upgrade notices to stderr while exiting 0. Merging stderr into the
# JSON capture poisoned $PRS and produced a false red X blaming "auth or API
# problem" -- the wrong cause, which is worse than the wrong verdict.
cat > "$STUB/gh" <<STUBEOF
#!/usr/bin/env bash
echo "A new release of gh is available: 2.86.0 -> 2.87.0" >&2
if [ "\${1:-}" = "api" ]; then echo 1; exit 0; fi
if [ "\${1:-}" = "run" ]; then echo "[]"; exit 0; fi
jq -cn '[{number: 1, title: "x", author: {login: "a"}, createdAt: "$OLD", updatedAt: "$OLD", mergeable: "MERGEABLE", isDraft: false, url: "https://example.invalid/1", headRefName: "b", headRefOid: "abc", statusCheckRollup: [{__typename: "CheckRun", status: "COMPLETED", conclusion: "SUCCESS", completedAt: "$OLD"}]}]'
STUBEOF
chmod +x "$STUB/gh"
NOISY_RC=0
NOISY_OUT="$(SWEEP_NOW_EPOCH="$NOW" SWEEP_LIMIT=5 SWEEP_REFETCH_SLEEP=0 PATH="$STUB:$PATH" \
    "$BASH_BIN" "$SWEEP" 2>/dev/null)" || NOISY_RC=$?
check "a gh that warns on stderr but succeeds does not poison the listing" "0" "$NOISY_RC"
check "the noisy-gh run still classifies correctly" "1 1 0 0" "$(sweep_counts "$NOISY_OUT")"

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

# An unwritable $GITHUB_STEP_SUMMARY / $GITHUB_OUTPUT is the same defect one
# level down: the queries all worked, and the run would still go green with the
# report attached nowhere and the notify step's trigger never written. Both
# appends are therefore fatal, and these two cases are the only thing that
# exercises them.
UNWRITABLE="$WORK/no-such-dir/summary.md"
SUMMARY_FAIL_RC=0
SWEEP_NOW_EPOCH="$NOW" GITHUB_STEP_SUMMARY="$UNWRITABLE" \
    "$BASH_BIN" "$SWEEP" --input "$WORK/mixed.json" >/dev/null 2>&1 || SUMMARY_FAIL_RC=$?
check "an unwritable step summary aborts non-zero" "1" "$SUMMARY_FAIL_RC"

OUTPUT_FAIL_RC=0
SWEEP_NOW_EPOCH="$NOW" GITHUB_OUTPUT="$WORK/no-such-dir/outputs.txt" \
    "$BASH_BIN" "$SWEEP" --input "$WORK/mixed.json" >/dev/null 2>&1 || OUTPUT_FAIL_RC=$?
check "an unwritable output file aborts non-zero" "1" "$OUTPUT_FAIL_RC"

# --- The workflow must actually run this script ------------------------------
# A self-test nothing invokes is coverage in a listing and a gate on nothing;
# the same is true of a sweep no workflow runs.
WF="$SCRIPT_DIR/../.github/workflows/pr-backlog-sweep.yml"
if [[ -f "$WF" ]]; then
    WF_TEXT="$(cat "$WF")"
    # Comment-stripped copy for every pin that must find CODE. Mutation-tested:
    # searching the raw text for the failure-notifier's step NAME passed with
    # the step deleted, because the file's own header comment quotes that name
    # while explaining why the step exists. A pin satisfied by the prose that
    # justifies it is the repo's documented anchor trap, one file over.
    WF_CODE="$(printf '%s\n' "$WF_TEXT" | sed -E 's/^[[:space:]]*#.*$//')"
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

    # A red X on the Actions tab is not a notification. This workflow exists
    # because a queue nobody watches accumulates silently, and scheduled runs
    # are exactly such a queue -- so a BROKEN sweep must page the dev room too,
    # or the instrument fails into the same blind spot it was built to close.
    check_contains "a failed sweep notifies the dev room" "$WF_CODE" \
        "Notify the dev room that the sweep is BROKEN" yes
    check_contains "the failure notifier is gated on the sweep step failing" "$WF_CODE" \
        "failure() && steps.sweep.outcome == 'failure'" yes
    # And both notifiers must stay best-effort: a riverctl hiccup must never be
    # mistaken for a sweep failure, nor add a second red X to an already-red run.
    NOTIFY_STEPS="$(printf '%s\n' "$WF_CODE" \
        | sed -n '/- name: Notify the dev room/,$p' \
        | sed -E 's/^[[:space:]]*#.*$//' | grep -c 'continue-on-error: true')"
    check "both notifier steps are continue-on-error" "2" "$NOTIFY_STEPS"
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
