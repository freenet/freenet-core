#!/usr/bin/env bash
# Report open PRs that have stalled: green-but-unmerged, idle, decayed into
# conflicts, or waiting on CI approval. Driven by
# .github/workflows/pr-backlog-sweep.yml, runnable by hand.
#
# WHY THIS EXISTS. Measured 2026-08-24: 59 open PRs, 26 of them green and simply
# never merged, the oldest untouched since 2026-07-28 and a team member's PR
# five weeks without a reply. Nine had rotted from mergeable to CONFLICTING and
# were conflicting with EACH OTHER -- ring.rs was contested by five open PRs at
# once. Nothing in the repo was measuring that, so nothing reported it.
#
# The same measurement found a SECOND unwatched queue: 58 workflow runs sitting
# in `action_required`, some since 2026-07-26. Outside-contributor CI needs a
# maintainer to approve each run, and nobody was doing it, so most community PRs
# had never had the main test suite run at all. They were not unreviewed; they
# were unreviewABLE, and any "that PR is red/green" claim made about them was
# made without evidence. The approval gate itself is CORRECT -- letting an
# untrusted fork run workflows on this repo's runners is a real security
# problem, and far worse than a slow queue. The gate is not the bug. Nobody
# watching it was. Section (d) below makes it visible; do not "simplify" this
# sweep by loosening the approval policy.
#
# THE DESIGN CONSTRAINT THAT MATTERS MOST: this is an INSTRUMENT, not a mask.
# If it cannot enumerate PRs -- no `gh`, no credentials, an API error, a
# truncated page -- it exits NON-ZERO and says why. It must never print an empty
# report and exit 0, because then "nothing is stalled" and "the sweep is broken"
# look identical, and the broken one wins by default. This project has been
# burned by exactly that shape once already: a backup job that logged
# "unreachable, skipping" and exited 0 reported SUCCESS for 37 consecutive days
# while no backups ran.
#
# Test hooks (all also useful for debugging by hand):
#   --input FILE        classify this `gh pr list --json ...` array instead of
#                       querying GitHub. Skips the fetch and its integrity
#                       checks; the classification below is what it exercises.
#   --runs-input FILE   likewise for the `gh run list --status action_required`
#                       array. Defaults to empty when only --input is given.
#   SWEEP_NOW_EPOCH     pretend "now" is this unix timestamp.
#   SWEEP_REPO          owner/name to sweep (default freenet/freenet-core).
#   SWEEP_GREEN_HOURS   green-and-unmerged threshold (default 48).
#   SWEEP_IDLE_DAYS     no-activity threshold (default 7).
#   SWEEP_LIMIT         max PRs to fetch; hitting it is a hard error, not a
#                       silent truncation (default 300).
#
# Self-test: bash scripts/pr-backlog-sweep_test.sh

set -uo pipefail

REPO="${SWEEP_REPO:-freenet/freenet-core}"
GREEN_HOURS="${SWEEP_GREEN_HOURS:-48}"
IDLE_DAYS="${SWEEP_IDLE_DAYS:-7}"
LIMIT="${SWEEP_LIMIT:-300}"
NOW="${SWEEP_NOW_EPOCH:-$(date +%s)}"

INPUT=""
RUNS_INPUT=""
while [ "$#" -gt 0 ]; do
    case "$1" in
        --input)
            INPUT="${2:-}"
            if [ -z "$INPUT" ]; then
                echo "::error::--input requires a file argument" >&2
                exit 2
            fi
            shift 2
            ;;
        --runs-input)
            RUNS_INPUT="${2:-}"
            if [ -z "$RUNS_INPUT" ]; then
                echo "::error::--runs-input requires a file argument" >&2
                exit 2
            fi
            shift 2
            ;;
        -h|--help)
            sed -n '2,45p' "$0"
            exit 0
            ;;
        *)
            echo "::error::unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

die() {
    # Every abort goes through here so a broken sweep is impossible to mistake
    # for a clean one: a GitHub-annotated error line AND a non-zero exit.
    echo "::error::PR backlog sweep FAILED: $*" >&2
    echo "The sweep could not enumerate PRs, so this run reports NOTHING about" >&2
    echo "the backlog. Do not read the absence of findings as an empty backlog." >&2
    exit 1
}

FIELDS='number,title,author,createdAt,updatedAt,mergeable,isDraft,url,headRefName,statusCheckRollup'
RUN_FIELDS='databaseId,headBranch,workflowName,createdAt'

if [ -n "$RUNS_INPUT" ]; then
    [ -f "$RUNS_INPUT" ] || die "--runs-input file not found: $RUNS_INPUT"
    RUNS="$(cat "$RUNS_INPUT")"
else
    RUNS=""
fi

if [ -n "$INPUT" ]; then
    [ -f "$INPUT" ] || die "--input file not found: $INPUT"
    PRS="$(cat "$INPUT")"
    [ -n "$RUNS" ] || RUNS='[]'
else
    command -v gh >/dev/null 2>&1 || die "the 'gh' CLI is not on PATH"
    command -v jq >/dev/null 2>&1 || die "'jq' is not on PATH"

    if ! PRS="$(gh pr list --repo "$REPO" --state open --limit "$LIMIT" --json "$FIELDS" 2>&1)"; then
        die "gh pr list failed for $REPO: $PRS"
    fi

    # GitHub computes mergeability lazily: the first query after a push returns
    # UNKNOWN. One cheap refetch resolves most of them; whatever is still
    # UNKNOWN is REPORTED as unknown below rather than quietly counted as clean,
    # so the conflict count is never silently understated.
    if printf '%s' "$PRS" | jq -e 'type == "array" and (map(select(.mergeable == "UNKNOWN")) | length > 0)' >/dev/null 2>&1; then
        sleep 10
        if ! REFETCH="$(gh pr list --repo "$REPO" --state open --limit "$LIMIT" --json "$FIELDS" 2>&1)"; then
            die "gh pr list (mergeability refetch) failed for $REPO: $REFETCH"
        fi
        PRS="$REFETCH"
    fi

    # The second unwatched queue: workflow runs held for maintainer approval.
    # Same instrument rule as above -- if this query fails we abort rather than
    # report "no runs are waiting", which is what a broken query looks like.
    if [ -z "$RUNS" ]; then
        if ! RUNS="$(gh run list --repo "$REPO" --status action_required --limit "$LIMIT" --json "$RUN_FIELDS" 2>&1)"; then
            die "gh run list (action_required) failed for $REPO: $RUNS"
        fi
    fi
fi

printf '%s' "$PRS" | jq -e 'type == "array"' >/dev/null 2>&1 \
    || die "the PR query did not return a JSON array (auth or API problem)"
printf '%s' "$RUNS" | jq -e 'type == "array"' >/dev/null 2>&1 \
    || die "the action_required workflow-run query did not return a JSON array (auth or API problem)"

COUNT="$(printf '%s' "$PRS" | jq 'length')"
RUN_COUNT="$(printf '%s' "$RUNS" | jq 'length')"

if [ -z "$INPUT" ]; then
    [ "$COUNT" -lt "$LIMIT" ] \
        || die "fetched $COUNT PRs, which is the fetch limit ($LIMIT): the list is TRUNCATED and this report would be incomplete. Raise SWEEP_LIMIT."
    [ "$RUN_COUNT" -lt "$LIMIT" ] \
        || die "fetched $RUN_COUNT action_required runs, which is the fetch limit ($LIMIT): the list is TRUNCATED. Raise SWEEP_LIMIT."

    # An empty result and a broken query must not look alike. A second,
    # independent path (REST rather than the GraphQL-backed `gh pr list`) has to
    # agree that there are genuinely no open PRs before we accept "nothing to
    # report" as a fact about the repo.
    if ! PROBE="$(gh api "repos/$REPO/pulls?state=open&per_page=1" --jq 'length' 2>&1)"; then
        die "corroborating REST probe failed for $REPO: $PROBE"
    fi
    case "$PROBE" in
        ''|*[!0-9]*) die "corroborating REST probe returned a non-number: $PROBE" ;;
    esac
    if [ "$COUNT" -eq 0 ] && [ "$PROBE" -ne 0 ]; then
        die "gh pr list returned 0 open PRs but the REST API sees at least $PROBE. The listing is wrong; refusing to report an empty backlog."
    fi
fi

# jq emits ONE json object carrying both the rendered markdown and the counts,
# so the notification headline can never disagree with the table a human reads.
RESULT="$(printf '%s' "$PRS" | jq -c \
    --argjson runs "$RUNS" \
    --argjson now "$NOW" \
    --argjson green_hours "$GREEN_HOURS" \
    --argjson idle_days "$IDLE_DAYS" \
    --arg repo "$REPO" '
    # A rollup entry is a CheckRun (carries .status/.conclusion) or a legacy
    # StatusContext (carries .state). Reading only one of the two fields is the
    # classic way to misread CI as permanently pending, so both are handled and
    # anything unrecognised degrades to UNKNOWN -- which is NOT green.
    def check_state:
      if .__typename == "CheckRun" then
        (if ((.status // "") | ascii_upcase) != "COMPLETED" then "PENDING"
         else (((.conclusion // "") | ascii_upcase) as $c
               | if ($c == "SUCCESS" or $c == "SKIPPED" or $c == "NEUTRAL")
                 then "PASS" else "FAIL" end)
         end)
      elif .__typename == "StatusContext" then
        (((.state // "") | ascii_upcase) as $s
         | if $s == "SUCCESS" then "PASS"
           elif ($s == "PENDING" or $s == "EXPECTED") then "PENDING"
           else "FAIL" end)
      else "UNKNOWN" end;

    def epoch: if . == null or . == "" then null
               else (try (. | fromdateiso8601) catch null) end;

    # PR titles and branch names are written by whoever opened the PR, and this
    # report is read as markdown. Rendered raw, a title of the form
    # "[click me](https://elsewhere)" becomes a working link in a maintainer-only
    # page. Newlines collapse (they would break the list item); the markdown
    # metacharacters are backslash-escaped so the text renders as itself.
    # This is presentation only -- nothing here reaches a shell either way.
    def md: gsub("[\\r\\n]"; " ") | gsub("(?<x>[\\[\\]<>`\\\\])"; "\\\(.x)");

    # "Green since" is when the LAST check finished, not when the PR was last
    # touched: a comment on a green PR must not reset its green age.
    def green_since:
      ([.statusCheckRollup[]? | .completedAt? | epoch
        | select(. != null and . > 0)] | max) // (.updatedAt | epoch);

    def fmt_age($t): if $t == null then "?"
                     else (((($now - $t) / 86400) * 10 | floor) / 10 | tostring) end;

    def render($prs; $agefield):
      ($prs | group_by(.author.login) | sort_by(.[0] | .[$agefield]))
      | map("- **@" + (.[0].author.login) + "** (" + (length | tostring) + ")\n"
            + (map("  - [#" + (.number | tostring) + "](" + .url + ") "
                   + (if .isDraft then "_(draft)_ " else "" end)
                   + fmt_age(.[$agefield]) + "d — "
                   # Titles are author-controlled text. jq renders them into
                   # markdown here; they never reach a shell as script text.
                   + (.title | md))
               | join("\n")))
      | if length == 0 then "_none_" else join("\n") end;

    map(. + {
      states: [.statusCheckRollup[]? | check_state],
      idle_epoch: (.updatedAt | epoch),
      created_epoch: (.createdAt | epoch),
      green_epoch: green_since,
    })
    | map(. + { is_green: ((.states | length) > 0 and (.states | all(. == "PASS"))) })
    | . as $all

    | ($all | map(select(.is_green and (.isDraft | not)
          and .green_epoch != null
          and ($now - .green_epoch) > ($green_hours * 3600)))
        | sort_by(.green_epoch)) as $stale_green
    | ($all | map(select(.idle_epoch != null
          and ($now - .idle_epoch) > ($idle_days * 86400)))
        | sort_by(.idle_epoch)) as $idle
    | ($all | map(select(.mergeable == "CONFLICTING")) | sort_by(.created_epoch)) as $conflicting
    | ($all | map(select(.mergeable == "UNKNOWN")) | length) as $unknown_mergeable

    # (d) Runs held for maintainer approval. Grouped BY BRANCH, not by run: 22
    # pending runs on one branch is ONE stuck PR, and reporting the raw run
    # count would overstate the problem by an order of magnitude. The PR number
    # is resolved by matching headRefName; a branch with no open PR (a stale
    # run, or a closed PR) is still listed, because a queue nobody drains is
    # the finding regardless of what is in it.
    | ($runs
        | group_by(.headBranch)
        | map({
            branch: .[0].headBranch,
            pending: length,
            oldest: ([.[] | .createdAt | epoch | select(. != null)] | min),
          })
        | map(. as $g | $g + { prs: [$all[] | select(.headRefName == $g.branch) | .number] })
        | sort_by(.oldest // 0)) as $awaiting

    | {
        stale_green: ($stale_green | length),
        idle: ($idle | length),
        conflicting: ($conflicting | length),
        awaiting_approval: ($awaiting | length),
        pending_runs: ($runs | length),
        total_open: ($all | length),
        body: (
            "## PR backlog sweep — \($repo)\n\n"
          + "\($all | length) open PRs. "
          + "**\($stale_green | length)** green >\($green_hours)h · "
          + "**\($idle | length)** idle >\($idle_days)d · "
          + "**\($conflicting | length)** conflicting · "
          + "**\($awaiting | length)** branch(es) awaiting CI approval.\n"
          + (if $unknown_mergeable > 0
             then "\n> \($unknown_mergeable) PR(s) still report `mergeable: UNKNOWN` after a refetch, so the conflict count is a LOWER BOUND.\n"
             else "" end)
          + "\n### Green and unmerged >\($green_hours)h\n"
          + "Nothing is blocking these. Age is time since the last check finished. Drafts excluded.\n\n"
          + render($stale_green; "green_epoch") + "\n"
          + "\n### No activity >\($idle_days)d\n\n"
          + render($idle; "idle_epoch") + "\n"
          + "\n### Decayed to CONFLICTING\n"
          + "Every day these wait, the cost of landing them rises.\n\n"
          + render($conflicting; "created_epoch") + "\n"
          + "\n### Waiting on CI approval (`action_required`)\n"
          + "Outside-contributor runs need a maintainer to approve them. Until "
          + "someone does, the test suite has NOT run on these branches — they "
          + "are not unreviewed, they are unreviewable, and any claim that one "
          + "is red or green is made without evidence.\n\n"
          + "The approval gate itself is correct and must stay: it is what "
          + "stops an untrusted fork from running workflows on the CI runners "
          + "of this repository. "
          + "Approve the runs; do not loosen the policy.\n\n"
          # Branch-to-PR resolution needs headRefName. If the PR listing carries
          # none, every branch would silently read as "no open PR" -- a wrong
          # answer that looks like a finding. Say so instead.
          + (if ($awaiting | length) > 0 and ($all | length) > 0
                and ([$all[] | .headRefName] | all(. == null))
             then "> The PR listing carries no `headRefName`, so branches could not be resolved to PRs below.\n\n"
             else "" end)
          + (if ($awaiting | length) == 0 then "_none_"
             else ($awaiting
                   # Bold rather than a code span: a git ref may legitimately
                   # contain a backtick, and there is no way to escape one
                   # INSIDE a code span, so a code span here could be broken
                   # out of by a fork branch name.
                   | map("- **" + (.branch | md) + "** — "
                         + (.pending | tostring) + " run(s) pending, oldest "
                         + fmt_age(.oldest) + "d"
                         + (if (.prs | length) > 0
                            then " — " + (.prs | map("#" + tostring) | join(", "))
                            else " — no open PR (stale runs)" end))
                   | join("\n"))
             end) + "\n"
          + "\n_\($runs | length) pending run(s) across \($awaiting | length) branch(es). "
          + "Counted per BRANCH: many pending runs on one branch is one stuck PR, not many._\n"
        ),
      }
    ')"

if [ -z "$RESULT" ]; then
    die "the report renderer produced no output (jq failed on the PR listing)"
fi

BODY="$(printf '%s' "$RESULT" | jq -r '.body')" || die "could not read the rendered report"
STALE_GREEN="$(printf '%s' "$RESULT" | jq -r '.stale_green')"
IDLE="$(printf '%s' "$RESULT" | jq -r '.idle')"
CONFLICTING="$(printf '%s' "$RESULT" | jq -r '.conflicting')"
AWAITING="$(printf '%s' "$RESULT" | jq -r '.awaiting_approval')"

for v in "$STALE_GREEN" "$IDLE" "$CONFLICTING" "$AWAITING"; do
    case "$v" in
        ''|*[!0-9]*) die "could not parse the sweep counts out of the rendered report" ;;
    esac
done
[ -n "$BODY" ] || die "the rendered report body is empty"

printf '%s\n' "$BODY"

# Always write the summary, including when everything is clean: a run that
# reports "0 / 0 / 0" is evidence the sweep RAN. A run that writes nothing is
# indistinguishable from a run that never happened.
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    printf '%s\n' "$BODY" >> "$GITHUB_STEP_SUMMARY"
fi

TOTAL=$((STALE_GREEN + IDLE + CONFLICTING + AWAITING))
if [ -n "${GITHUB_OUTPUT:-}" ]; then
    {
        echo "stale_green=$STALE_GREEN"
        echo "idle=$IDLE"
        echo "conflicting=$CONFLICTING"
        echo "awaiting_approval=$AWAITING"
        # Notify only when there IS something to say. A daily "all clear" ping is
        # how a channel learns to ignore this notifier.
        if [ "$TOTAL" -gt 0 ]; then echo "notify=true"; else echo "notify=false"; fi
        # Counts only -- deliberately no PR titles, no author logins and no
        # branch names, so no repository-controlled string is ever forwarded to
        # the notifier. Pinned by the self-test.
        echo "headline=${STALE_GREEN} PR(s) green >${GREEN_HOURS}h unmerged, ${IDLE} idle >${IDLE_DAYS}d, ${CONFLICTING} conflicting, ${AWAITING} branch(es) awaiting CI approval"
    } >> "$GITHUB_OUTPUT"
fi

echo "sweep ok: stale_green=$STALE_GREEN idle=$IDLE conflicting=$CONFLICTING awaiting_approval=$AWAITING" >&2
exit 0
