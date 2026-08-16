#!/usr/bin/env bash
# Regression test for the WIRING of the blocking auto-update pre-flight canary
# (#5222/#5236) -- as distinct from the canary's own logic, which
# auto-update-canary_test.sh covers.
#
# THE GAP THIS CLOSES. The canary only gates anything because of where it sits
# in .github/workflows/cross-compile.yml: inside the `attach-to-release` job,
# AFTER the assets are uploaded and BEFORE `gh release edit --draft=false`.
# That position is the entire mechanism. Delete the step, move the publish
# above it, or mark it `continue-on-error`, and the gate becomes a no-op while
# every other test in this repo stays green -- which is precisely the
# silently-removable-gate shape the canary was introduced to eliminate. A gate
# whose removal is invisible is not a gate.
#
# It also pins the ORDER OF THE IRREVERSIBLE STEP, for the same reason. The
# crates.io publish moved out of release.yml into this job, between the canary
# and the un-draft, because a published crate version is the one thing in a
# release that cannot be taken back: with it upstream of the gate, a Gate A
# block cost v0.2.124 its version number instead of a re-run. Moving it back --
# in either file -- restores that, and would otherwise be invisible to every
# test in this repo. See assertions 2b and 2c.
#
# And it pins the RECOVERY RUNBOOK, which is the other route around the gate:
# scripts/RELEASE_RECOVERY.md tells a human what to do when a release has
# already gone wrong, and its `gh release create` invocations lacked `--draft`
# (#5288). A release created published is a release Gate A never sees. Pinning
# the workflow while leaving the documented manual path ungated protects the
# machine and not the human, and the human is the one acting under pressure.
# See assertion 2d.
#
# It also pins the two ends of a string that must agree across files:
# release.sh's ATTACH_JOB_NAME is how the release driver finds this job's
# status, and nothing else checks that the name still matches. Rename the job
# in the workflow and the driver waits for a job that will never appear, then
# times out ~20 minutes later reporting UNKNOWN -- on a release that in fact
# published fine.
#
# Same shape as release_mergequeue_test.sh, which greps release.yml for the
# `gh pr merge` invocation it must keep.
#
# Run manually: bash scripts/release_canary_wiring_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WF="$SCRIPT_DIR/../.github/workflows/cross-compile.yml"
RELEASE_SH="$SCRIPT_DIR/release.sh"
WF_BASENAME="$(basename "$WF")"

FAILURES=0

fail() {
    echo "FAIL - $1" >&2
    shift
    for line in "$@"; do echo "       $line" >&2; done
    FAILURES=$((FAILURES + 1))
}
pass() { echo "ok   - $1"; }

for f in "$WF" "$RELEASE_SH"; do
    if [[ ! -f "$f" ]]; then
        echo "FAIL: $f not found" >&2
        exit 1
    fi
done

# The `attach-to-release:` job block: from its own key to the next top-level
# job key. Job keys sit at exactly two spaces; everything inside the job is
# indented further, so this needs no YAML parser and cannot be fooled by a
# matching string in a comment elsewhere in the file.
# Comment-only lines are dropped, but the original line NUMBERS are kept, so
# ordering comparisons stay meaningful. This is load-bearing: the job's own
# comments discuss `--draft=false` (explaining the RELEASE_PAT coalesce) well
# above the step that runs it, so matching raw text would compare the canary
# against a sentence and report the gate inverted. A pin that fails on prose is
# no better than one that passes on prose.
# ONE extraction for every job block in this file, so the comment filter cannot
# be forgotten at a new call site. It was forgotten at two: `selfupdate_block`
# (fixed after a mutation of `|| rc=$?` hit the comment quoting it and the pin
# stayed green) and then `notify_block`, where the consequence was worse --
# widening the quiet notification's `if:` to `result == 'failure'` while leaving
# a one-line comment recording the old condition left ALL FOUR suites green,
# with the reassuring "no fleet action is indicated" message firing on every red
# Gate B run including a real #5221. Two single-site fixes in a row is what
# turned this into a function.
#
# (That second one was saved only by luck: the comment had to be on ONE line,
# because the `# ` on a wrapped continuation breaks the whitespace-collapsed
# match. Nobody designed that.)
#
# `--numbered` keeps the original line NUMBERS so ordering comparisons stay
# meaningful; the others do not need them.
yaml_job_block() {
    # yaml_job_block <job-key> [--numbered]
    local job="$1" numbered="${2:-}"
    awk -v jobre="^  $job:[[:space:]]*\$" -v numbered="$numbered" '
        { prefix = (numbered != "" ? NR ":" : "") }
        $0 ~ jobre                        { inblock = 1; print prefix $0; next }
        inblock && /^  [A-Za-z_.-]+:/     { inblock = 0 }
        inblock && $0 !~ /^[[:space:]]*#/ { print prefix $0 }
    ' "$WF"
}

# The `attach-to-release:` job block: from its own key to the next top-level
# job key. Job keys sit at exactly two spaces; everything inside the job is
# indented further, so this needs no YAML parser and cannot be fooled by a
# matching string in a comment elsewhere in the file.
JOB_BLOCK="$(yaml_job_block attach-to-release --numbered)"

if [[ -z "$JOB_BLOCK" ]]; then
    fail "the 'attach-to-release' job no longer exists in cross-compile.yml" \
        "That job is where the canary gates publication; release.sh watches it by name."
    echo
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
pass "cross-compile.yml still has an 'attach-to-release' job"

# line_of <extended-regex> -- first matching line number INSIDE the job block.
line_of() {
    printf '%s\n' "$JOB_BLOCK" | grep -E "$1" | head -1 | cut -d: -f1
}

# step_block <line> -- the whole STEP containing <line>, as `NNN:text` lines.
#
# Bounded by the `- name:` at or above <line> and the next one below it (or the
# end of the job). Step keys such as `if:` and `continue-on-error:` sit ABOVE
# the `run:` that holds the invocation, so anything anchored on the invocation
# line alone cannot see them -- which is exactly how the publish step's own
# `if:` went unpinned while the canary step's was covered.
step_block() {
    local at="$1" start end
    start="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$at" '$1 <= a && /^[0-9]+:      - name:/ { n = $1 } END { print n }')"
    end="$(printf '%s\n' "$JOB_BLOCK" \
        | awk -F: -v a="$at" '$1 > a && /^[0-9]+:      - name:/ { print $1; exit }')"
    [[ -z "$end" ]] && end=999999
    printf '%s\n' "$JOB_BLOCK" | awk -F: -v a="$start" -v b="$end" '$1 >= a && $1 < b'
}

# status_swallowers <text> [allow-rc-capture]
#
# Emits every line that lets a command's NON-ZERO exit status be discarded.
# Empty output means nothing swallows.
#
# THIS IS NOT THE GUARANTEE. Assertion 2g is: it EXECUTES the gate steps with a
# failing stub canary and asserts the step's own exit status. This function
# enumerates SPELLINGS, and an enumeration cannot close the class -- it has been
# beaten four times, by `|| echo`, by `if ! ...; then ...; fi` (no `||` at all,
# and `if` conditions are errexit-exempt), by `&` backgrounding, and by
# `set +o errexit` (the long-option spelling this regex still does not match).
#
# It is kept because it names the specific route in its failure text and fails
# in milliseconds, which is genuinely useful when it does fire. But if you are
# here because a new swallow spelling got through: DO NOT ADD A FIFTH PATTERN.
# That approach has failed every time. Check that 2g covers the case instead --
# it should already, because it does not care how the swallow is spelled.
#
# WORKED EXAMPLE, so this is a decision rather than an oversight:
# `set +o errexit` is the long-option spelling of `set +e` and this regex does
# NOT match it. That is deliberate, and it is left unmatched on purpose.
#
# Measured under Actions' real `bash -e {0}` rather than reasoned about:
#
#     set +o errexit; <canary>                  -> exit 1   gate HOLDS
#     set +o errexit; <canary>; echo done       -> exit 0   gate DEFEATED
#     <canary>; exit 0                          -> exit 1   gate HOLDS
#     { <canary>; }; true                       -> exit 1   gate HOLDS
#     if ! <canary>; then echo warn; fi         -> exit 0   gate DEFEATED
#     <canary> &; echo started                  -> exit 0   gate DEFEATED
#
# So disabling errexit is harmless on its own -- the canary is the last command
# and its status becomes the script's -- and dangerous only combined with a
# trailing zero-status command. Three of the six spellings above do not defeat
# the gate at all, and a regex cannot tell which is which, because the
# difference is not in the text of any one line. 2g catches all three that DO
# defeat it and correctly stays green on the three that do not. Adding
# `\+o[[:space:]]+errexit` here would flag a harmless line while still missing
# the combinations, which is the treadmill in miniature.
#
# WHY THIS IS A FUNCTION, AND WHY IT IS NOT ANCHORED AT END OF LINE.
# The three gate sites (Gate A's canary step, the CARGO_REGISTRY_TOKEN
# fail-fast, Gate B's step) each had their own copy of
# `\|\|[[:space:]]*(true|:)[[:space:]]*$`, which recognises exactly two
# swallows and only at end of line. Everything else walked straight through:
#
#     ... auto-update-canary.sh preflight /tmp/freenet || echo "::warning::soft"
#     ... || exit 0
#     ... || :; echo done
#
# The first of those was mutation-tested against the whole suite and passed
# 37/37 while Gate A was completely neutered -- next to a comment in this very
# file calling `|| true` "the likelier of the two neutering routes, and the
# worse one ... which is exactly why it has to fail loudly here." It did not.
# An enumeration of known-bad spellings is not a defence; the RULE is that
# these steps must not consume their command's status AT ALL.
#
# So: ANY `||` counts, plus `set +e`, plus a trailing `; true` / `; :`. None of
# these steps has a legitimate `||` -- verified: the canary step and the token
# check contain none, and Gate B contains exactly one, its deliberate
# `|| rc=$?` capture, which is opted in per call site rather than baked in
# here. A future step that genuinely needs `||` should have to come here and
# say so, which is the point.
#
# Callers pass comment-stripped text (yaml_job_block drops comment lines), so
# prose discussing `|| true` cannot fire this.
status_swallowers() {
    local text="$1" allow_rc="${2:-}"
    local hits
    hits="$(printf '%s\n' "$text" \
        | grep -E '\|\||set[[:space:]]+\+e|;[[:space:]]*(true|:)[[:space:]]*$')"
    if [[ -n "$allow_rc" ]]; then
        # Gate B runs the canary as `... || rc=$?` precisely so it can classify
        # exit 75, and re-raises with `exit "$rc"`. That pairing is asserted
        # separately; only the capture spelling is exempted here.
        hits="$(printf '%s\n' "$hits" | grep -vE '\|\|[[:space:]]*rc=\$\?[[:space:]]*$')"
    fi
    printf '%s\n' "$hits" | grep -vE '^[[:space:]]*$' || true
}

# --- 1. the canary step still runs ------------------------------------------
CANARY_LINE="$(line_of 'auto-update-canary\.sh preflight')"
if [[ -n "$CANARY_LINE" ]]; then
    pass "the pre-flight canary still runs in attach-to-release (line $CANARY_LINE)"
else
    fail "the pre-flight canary is GONE from the attach-to-release job" \
        "Nothing now blocks publication on the shipping binary's updater working." \
        "This is the #5222 regression the canary exists to prevent: v0.2.120 and" \
        "v0.2.121 both shipped with a dead updater and every signal stayed green."
fi

# --- 2. it runs BEFORE the release is un-drafted ----------------------------
# Presence alone does not gate anything: a canary that runs after the publish
# is a report, not a gate, and the release has already reached users by then.
PUBLISH_LINE="$(line_of '\-\-draft=false')"
if [[ -z "$PUBLISH_LINE" ]]; then
    fail "no 'gh release edit --draft=false' in the attach-to-release job" \
        "If publication moved elsewhere, the canary no longer gates it."
elif [[ -n "$CANARY_LINE" ]]; then
    if [[ "$CANARY_LINE" -lt "$PUBLISH_LINE" ]]; then
        pass "the canary runs BEFORE '--draft=false' (canary $CANARY_LINE < publish $PUBLISH_LINE)"
    else
        fail "the canary runs AFTER the release is published (canary $CANARY_LINE > publish $PUBLISH_LINE)" \
            "Steps in a job run in file order, so this is no longer a gate: the" \
            "release is public before the updater is ever exercised. The whole" \
            "point of Gate A is that a failure costs a stuck DRAFT, not a" \
            "stranded fleet."
    fi
fi

# --- 2b. the crates.io publish sits BETWEEN the canary and the un-draft ------
# The ordering this pins is the fix for the 0.2.124 loss. The publish used to
# run in release.yml before the tag existed, so the one irreversible step in a
# release happened UPSTREAM of the gate that decides whether to ship: when
# Gate A blocked v0.2.124 its crates were already permanent, the release could
# only stay a draft, and the version number was spent.
#
# Both bounds matter and they fail differently:
#   above the canary -> back to burning a version number on every gate block;
#   below the un-draft -> the release goes public before its crates exist, so
#     `cargo install freenet` fails for whoever reads the announcement first.
#
# Ordering is checked by line number because steps in a job run in file order,
# which is the same mechanism assertion 2 relies on.
PUBLISH_CRATES_LINE="$(line_of 'cargo publish -p freenet')"
if [[ -z "$PUBLISH_CRATES_LINE" ]]; then
    fail "no 'cargo publish -p freenet' in the attach-to-release job" \
        "The crates.io publish lives here so that it runs AFTER the blocking" \
        "pre-flight canary. If it has moved back into release.yml (or anywhere" \
        "upstream of the tag), a Gate A block again costs a permanently-spent" \
        "crates.io version instead of a deletable tag -- the v0.2.124 loss."
else
    pass "the crates.io publish runs in attach-to-release (line $PUBLISH_CRATES_LINE)"

    if [[ -n "$CANARY_LINE" ]]; then
        if [[ "$CANARY_LINE" -lt "$PUBLISH_CRATES_LINE" ]]; then
            pass "the canary runs BEFORE the crates.io publish (canary $CANARY_LINE < publish $PUBLISH_CRATES_LINE)"
        else
            fail "the crates.io publish runs BEFORE the canary (publish $PUBLISH_CRATES_LINE < canary $CANARY_LINE)" \
                "The publish is irreversible and the canary is the gate. In this order a" \
                "Gate A block leaves the crates on crates.io forever and the release" \
                "unpublishable -- exactly what happened to v0.2.124."
        fi
    fi

    if [[ -n "$PUBLISH_LINE" ]]; then
        if [[ "$PUBLISH_CRATES_LINE" -lt "$PUBLISH_LINE" ]]; then
            pass "the crates.io publish runs BEFORE '--draft=false' (crates $PUBLISH_CRATES_LINE < un-draft $PUBLISH_LINE)"
        else
            fail "the release is un-drafted BEFORE its crates are published (un-draft $PUBLISH_LINE < crates $PUBLISH_CRATES_LINE)" \
                "The release becomes public, the announcement cascade fires, and" \
                "'cargo install freenet@<version>' does not work yet. Publish first."
        fi
    fi

    PUBLISH_CRATES_STEP="$(step_block "$PUBLISH_CRATES_LINE")"
    if [[ -z "$PUBLISH_CRATES_STEP" ]]; then
        # Every assertion below scans this variable, and an empty scan target
        # makes each of them pass by finding nothing. Fail loudly instead:
        # `step_block` returning nothing means its `- name:` bounds moved, not
        # that the step is clean.
        fail "could not extract the crates.io publish STEP around line $PUBLISH_CRATES_LINE" \
            "step_block found no enclosing '- name:'. Every check below scans this" \
            "text, so an empty extraction would make all of them pass vacuously."
    fi

    # The step's `if:` is pinned to one exact expression rather than forbidden.
    #
    # It needs an `if:` at all because this workflow's `on:` includes
    # `branches: [main]`, so every push to main runs it. The job's own
    # `if: startsWith(github.ref, 'refs/tags/v')` already blocks the job there,
    # but a step that uploads to crates.io should be structurally incapable of
    # firing off a tag rather than incapable only while that job condition
    # survives editing. It is also what bounds CARGO_REGISTRY_TOKEN's reach.
    #
    # And it needs `success() &&` in front, spelled out. A step `if:` with no
    # status-check function has `success()` applied implicitly, so the plain
    # tag test would still require every earlier step -- including Gate A -- to
    # have passed. Writing it removes the gate's dependence on that implicit
    # rule; dropping it for `if: always() && startsWith(...)` would publish
    # after a failed canary, and that one word is the whole difference.
    #
    # Exact-match, not "contains no always()", for the reason the un-draft
    # step's assertion gives: deciding whether an arbitrary expression can
    # evaluate true after a failed step is not a job for a grep. Any change to
    # this line is a deliberate edit to a release gate and should update this
    # expectation on purpose.
    PC_WANT_IF="        if: \${{ success() && startsWith(github.ref, 'refs/tags/v') }}"
    PC_GOT_IF="$(printf '%s\n' "$PUBLISH_CRATES_STEP" | grep -E '^[0-9]+:        if:' | cut -d: -f2-)"
    if [[ "$PC_GOT_IF" == "$PC_WANT_IF" ]]; then
        pass "the crates.io publish step is gated on success() AND a v* tag ref"
    else
        fail "the crates.io publish step's 'if:' is not the expected gate" \
            "expected: $PC_WANT_IF" \
            "got:      ${PC_GOT_IF:-<none>}" \
            "Without the tag test a push to main can reach a crates.io upload" \
            "(this workflow runs on 'branches: [main]' too). Without 'success() &&'" \
            "the step's implicit success() default is being relied on, and any" \
            "later edit to 'always()' publishes after a failed Gate A."
    fi

    # `continue-on-error: true` is the other neutering route, and it is not
    # covered by the exact-match above: it would let a FAILED upload proceed to
    # the un-draft, publishing a release whose crates do not exist.
    # ...and it must be a REAL publish. Stated directly as well as being
    # exercised by the fixtures below, because this is the mutation a reader of
    # THIS FILE is primed to make: assertion 2c deliberately whitelists
    # `--dry-run` in release.yml, so "add --dry-run until it stops failing" is a
    # move the file itself teaches. Here it would mean Gate A runs, nothing is
    # uploaded, the release un-drafts, the cascade fires, and
    # `cargo install freenet@X` 404s for everyone who reads the announcement.
    PC_DRYRUN="$(printf '%s\n' "$PUBLISH_CRATES_STEP" | grep -E 'cargo publish.*--dry-run')"
    if [[ -z "$PC_DRYRUN" ]]; then
        pass "the crates.io publish step really publishes (no --dry-run)"
    else
        fail "the crates.io publish step has become a DRY RUN" \
            "$(printf '%s\n' "$PC_DRYRUN")" \
            "This step is the release's only real crates.io upload. As a dry run the" \
            "job still goes green, Gate A still passes, and the release still" \
            "un-drafts -- publishing binaries and firing the announcement cascade for" \
            "a version that is not on crates.io. release.yml's dry run is the" \
            "whitelisted one (assertion 2c); this one must never be."
    fi

    PC_COE="$(printf '%s\n' "$PUBLISH_CRATES_STEP" \
        | grep -E '^[0-9]+:        continue-on-error:')"
    if [[ -z "$PC_COE" ]]; then
        pass "the crates.io publish step has no 'continue-on-error:'"
    else
        fail "the crates.io publish step has acquired a 'continue-on-error:'" \
            "$(printf '%s\n' "$PC_COE")" \
            "A failed upload would then reach the un-draft, publishing a release" \
            "whose crates are not on crates.io."
    fi
fi

# --- 2b-bis. the publish step's tree guard, EXECUTED not scraped -------------
# The #5233 guard in the form this job can express it. release.yml's two jobs
# run verify_release_checkout.sh before their irreversible act, because they
# check out a SHA a moving `main` could have displaced. Here the ref is the tag,
# so the equivalent question is whether the tag and the tree agree -- and it
# must be asked BEFORE the upload, because a crates.io version cannot be
# replaced once sent.
#
# RUN the step rather than grepping it, and the reason is a mistake made while
# writing this file. The first version asserted the step contained the text
# `crates/core/Cargo.toml` above its first `cargo publish`. Mutation-testing it
# -- replacing the real `sed` read with `CORE_VERSION="$VERSION"`, which
# compares the tag against itself and can never fail -- left the assertion
# GREEN, because the step's own `::error::` message mentions the same filename.
# A scrape satisfied by the error text of the guard it is checking is the
# `not triggering auto-update` shape again: pinning a line that is talking
# ABOUT the thing rather than doing it.
#
# The step's `run:` is extracted from the YAML and executed against fixture
# trees with `cargo`, `curl` and `sleep` stubbed, so what is asserted is what it
# DOES: refuses a mismatched tree without calling cargo, skips a version already
# on crates.io, and actually publishes otherwise. The third case matters as much
# as the first -- a guard that refuses everything would satisfy the other two.
extract_step_run() {
    # extract_step_run <job-key> <step name> -- that step's `run:` script,
    # dedented, from THAT JOB ONLY.
    #
    # The job scoping is not decoration. This function used to awk the whole
    # file for the first `- name: <step>`, while `line_of` and `step_block`
    # are scoped to `$JOB_BLOCK` -- so the two could describe DIFFERENT STEPS
    # and nothing noticed. Mutation-tested: gutting the real tree guard to
    # `CORE_VERSION="$VERSION"` (which compares the tag against itself and can
    # never fail) AND adding an `if: false` decoy step named
    # `Publish crates to crates.io` to an earlier job, carrying the correct
    # script, left ALL FIVE behavioural cases green -- including "refuses a
    # tree whose version does not match the tag" -- because they were executing
    # the decoy. First-in-file won, and first-in-file was not the step being
    # gated.
    #
    # Any extractor in this file that is not scoped to the thing it claims to
    # describe is the same defect waiting. See the consistency check at the
    # PC_RUN call site, which asserts the two extractors agree rather than
    # trusting that they do.
    local job="$1" name="$2"
    awk -v jobre="^  $job:[[:space:]]*\$" -v want="      - name: $name" '
        $0 ~ jobre                       { injob = 1; next }
        injob && /^  [A-Za-z_.-]+:/      { injob = 0 }
        !injob                           { next }
        $0 == want                       { instep = 1; next }
        instep && /^      - name:/       { exit }
        instep && /^        run: \|/     { inrun = 1; next }
        inrun {
            if ($0 !~ /^[[:space:]]*$/ && $0 !~ /^          /) exit
            sub(/^          /, "")
            print
        }
    ' "$WF"
}

# A step name that occurs more than once in the file is itself the hazard: two
# extractors can then disagree about which one they describe. Job scoping fixes
# WHICH step is read; this names the ambiguity so a decoy is reported rather
# than silently ignored.
PC_NAME_COUNT="$(grep -cE '^      - name: Publish crates to crates\.io[[:space:]]*$' "$WF")"
if [[ "$PC_NAME_COUNT" -eq 1 ]]; then
    pass "the 'Publish crates to crates.io' step name occurs exactly once in $WF_BASENAME"
else
    fail "the step name 'Publish crates to crates.io' occurs $PC_NAME_COUNT times" \
        "Duplicate step names let a job-scoped extractor and an unscoped one" \
        "describe DIFFERENT steps. A decoy carrying a correct-looking script --" \
        "even an 'if: false' one that never runs -- can then satisfy the" \
        "behavioural cases while the step that actually ships is unexamined."
fi

PC_RUN="$(extract_step_run attach-to-release 'Publish crates to crates.io')"
if [[ -z "$PC_RUN" || "$PC_RUN" != *'cargo publish -p freenet'* ]]; then
    # Never a silent skip: every case below would pass on an empty script.
    fail "could not extract the 'Publish crates to crates.io' step's run: block" \
        "The behavioural cases below execute it, so an empty or wrong extraction" \
        "would make all of them pass vacuously. Check the step name and that its" \
        "body is still a 'run: |' block indented by 10 spaces."
elif ! command -v jq >/dev/null 2>&1; then
    # Also never a silent skip. The step itself needs jq at release time, so a
    # missing jq is a real problem, not a reason to stop checking.
    fail "jq is not installed, so the publish step's guard cannot be exercised" \
        "The step uses jq to read crates.io's answer; it would fail at release" \
        "time too. Install jq."
else
    pass "extracted the crates.io publish step's run: block ($(printf '%s\n' "$PC_RUN" | wc -l) lines)"

    # The two extractors must describe the SAME step.
    #
    # `PUBLISH_CRATES_STEP` (via line_of/step_block) is what the ORDERING and
    # `if:` assertions judge; `PC_RUN` is what the behavioural cases EXECUTE.
    # They are found by different mechanisms, so they can drift apart -- and
    # when they do, this file reports confidently about two different steps:
    # the gated one is pinned for position while the executed one is some other
    # step entirely. That is not hypothetical, it is the N4 mutation, and job
    # scoping alone only closes the instance rather than the class. This asserts
    # the agreement instead of assuming it.
    PC_STEP_TEXT="$(printf '%s\n' "$PUBLISH_CRATES_STEP" | sed 's/^[0-9]*://')"
    pc_missing=0
    # Blank and COMMENT lines are skipped: `yaml_job_block` strips comment-only
    # lines when building JOB_BLOCK, while `extract_step_run` reads the raw
    # file, so the run block's own shell comments are legitimately present in
    # one side and absent from the other. Comparing them would fail on every
    # clean tree, which is how a check like this ends up deleted rather than
    # fixed. The executable lines are the ones that must agree.
    while IFS= read -r _line; do
        _stripped="${_line#"${_line%%[![:space:]]*}"}"
        [[ -z "$_stripped" ]] && continue
        [[ "$_stripped" == \#* ]] && continue
        case "$PC_STEP_TEXT" in
            *"$_line"*) ;;
            *) pc_missing=$((pc_missing + 1)) ;;
        esac
    done <<< "$PC_RUN"

    if [[ "$pc_missing" -eq 0 ]]; then
        pass "the executed run: block belongs to the same step the ordering assertions pin"
    else
        fail "the EXECUTED publish script is not the step the ordering assertions describe" \
            "$pc_missing line(s) of the extracted run: block do not appear in the" \
            "job-scoped step at line $PUBLISH_CRATES_LINE." \
            "The behavioural cases below would be exercising one step while the" \
            "ordering and 'if:' assertions above pin a different one -- so this file" \
            "would report a correct, gated, guarded publish while the step that" \
            "actually ships is neither. Check for a duplicate step name elsewhere in" \
            "the workflow."
    fi

    # The crates.io answer is per-CRATE, not one fixed body, because the case
    # that matters most is the asymmetric one: `freenet` uploaded, then `fdev`
    # failed. A re-run must skip the first and retry only the second. A stub
    # that answers identically for both cannot express that, and would let a
    # step that skips or publishes ALL-or-NOTHING pass as if it were
    # per-crate.
    pc_run_case() {
        # pc_run_case <desc> <core-version> <tag> <freenet-body> <fdev-body> <want-rc> <want-published>
        #   want-published: none | freenet | fdev | both
        local desc="$1" core_ver="$2" tag="$3" freenet_body="$4" fdev_body="$5"
        local want_rc="$6" want_published="$7"
        local work rc got_freenet got_fdev got_published
        work="$(mktemp -d)"
        mkdir -p "$work/crates/core" "$work/crates/fdev" "$work/bin"
        printf '[package]\nname = "freenet"\nversion = "%s"\n' "$core_ver" \
            > "$work/crates/core/Cargo.toml"
        printf '[package]\nname = "fdev"\nversion = "0.9.9"\n' \
            > "$work/crates/fdev/Cargo.toml"
        : > "$work/cargo.log"
        # Unquoted heredocs: `$work` and the bodies expand, while `\$*`, `\$@`
        # and the `\n` in the format strings reach the stub literally.
        cat > "$work/bin/cargo" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/cargo.log"
EOF
        # Dispatches on the crate name in the request URL, exactly as the real
        # endpoint does (\`/api/v1/crates/<name>/<version>\`).
        cat > "$work/bin/curl" <<EOF
#!/usr/bin/env bash
for arg in "\$@"; do
  case "\$arg" in
    */crates/freenet/*) printf '%s' '$freenet_body'; exit 0 ;;
    */crates/fdev/*)    printf '%s' '$fdev_body';    exit 0 ;;
  esac
done
echo "stub curl: no crate in args: \$*" >&2
exit 1
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        chmod +x "$work/bin/cargo" "$work/bin/curl" "$work/bin/sleep"
        printf '%s\n' "$PC_RUN" > "$work/step.sh"

        ( cd "$work" && PATH="$work/bin:$PATH" GITHUB_REF="refs/tags/$tag" \
            bash "$work/step.sh" ) > "$work/out" 2>&1
        rc=$?
        # A REAL publish, not merely an invocation mentioning the crate.
        # `--dry-run` is excluded deliberately: without that exclusion this
        # classification is satisfied by `cargo publish -p freenet --dry-run`,
        # so appending `--dry-run` to both invocations left EVERY assertion in
        # this file green while the pipeline uploaded nothing -- Gate A runs,
        # nothing publishes, the release un-drafts, the announcement cascade
        # fires, and `cargo install freenet@X` 404s for everyone who reads it.
        # That is the split-channel state assertion 3b's comment calls
        # impossible, reached by adding one flag.
        #
        # It is also a flag this very file TEACHES: assertion 2c whitelists
        # `--dry-run` in release.yml, so "add --dry-run to make it stop
        # failing" is a move a reader is actively primed to make. A pin whose
        # subject can be satisfied by the thing it exists to exclude is not a
        # pin -- the same lesson as #5303, and the same lesson as the tree
        # guard two blocks below.
        #
        # `grep -c` on the file, not `| grep -q`: the pipefail/SIGPIPE hazard
        # audited by auto-update-canary_test.sh.
        got_freenet=0; got_fdev=0
        [[ "$(grep -cE '(^| )publish +-p +freenet( |$)' "$work/cargo.log")" -gt 0 ]] \
            && [[ "$(grep -cE '(^| )publish +-p +freenet .*--dry-run' "$work/cargo.log")" -eq 0 ]] \
            && got_freenet=1
        [[ "$(grep -cE '(^| )publish +-p +fdev( |$)' "$work/cargo.log")" -gt 0 ]] \
            && [[ "$(grep -cE '(^| )publish +-p +fdev .*--dry-run' "$work/cargo.log")" -eq 0 ]] \
            && got_fdev=1
        case "$got_freenet$got_fdev" in
            00) got_published=none ;;
            10) got_published=freenet ;;
            01) got_published=fdev ;;
            11) got_published=both ;;
        esac

        local ok=1
        case "$want_rc" in
            0)       [[ "$rc" -eq 0 ]] || ok=0 ;;
            nonzero) [[ "$rc" -ne 0 ]] || ok=0 ;;
        esac
        [[ "$got_published" == "$want_published" ]] || ok=0

        if [[ "$ok" -eq 1 ]]; then
            pass "publish step: $desc (rc=$rc, published: $got_published)"
        else
            fail "publish step: $desc" \
                "expected rc $want_rc and published=$want_published," \
                "got rc=$rc and published=$got_published" \
                "--- step output ---" \
                "$(head -20 "$work/out")"
        fi
        rm -rf "$work"
    }

    NOT_ON_CRATES_IO='{"errors":[{"detail":"Not Found"}]}'
    FREENET_ON_CRATES_IO='{"version":{"num":"9.9.9"}}'
    FDEV_ON_CRATES_IO='{"version":{"num":"0.9.9"}}'

    # The guard itself: a tree whose version disagrees with the tag must stop
    # BEFORE anything reaches crates.io. `published: none` is the load-bearing
    # half -- a refusal that happens after the upload is not one.
    pc_run_case "refuses a tree whose version does not match the tag" \
        "0.0.1" "v9.9.9" "$NOT_ON_CRATES_IO" "$NOT_ON_CRATES_IO" nonzero none

    # The normal path. Without this case every other one here is satisfied by a
    # step that never publishes at all.
    pc_run_case "publishes both when the tree matches and neither is on crates.io" \
        "9.9.9" "v9.9.9" "$NOT_ON_CRATES_IO" "$NOT_ON_CRATES_IO" 0 both

    # Full re-run after a failure downstream of the upload (a cancelled runner,
    # a failed un-draft). Nothing is uploaded twice and the job proceeds.
    pc_run_case "skips both when both are already on crates.io" \
        "9.9.9" "v9.9.9" "$FREENET_ON_CRATES_IO" "$FDEV_ON_CRATES_IO" 0 none

    # THE case this requirement is about: the publish died between the two
    # uploads. A re-run must skip freenet -- `cargo publish` would refuse a
    # duplicate and fail the job, costing another tag -- and retry fdev alone.
    pc_run_case "re-run after a partial publish retries only the missing crate" \
        "9.9.9" "v9.9.9" "$FREENET_ON_CRATES_IO" "$NOT_ON_CRATES_IO" 0 fdev

    # ...and the mirror, so the per-crate dispatch is not passing by accident of
    # the order the step happens to check them in.
    pc_run_case "re-run publishes freenet alone when only fdev is already up" \
        "9.9.9" "v9.9.9" "$NOT_ON_CRATES_IO" "$FDEV_ON_CRATES_IO" 0 freenet
fi

# --- 2b-ter. the crates.io credential is checked EARLY -----------------------
# Moving the publish to the end of the pipeline created a new way to lose a
# release: a missing or dead CARGO_REGISTRY_TOKEN is now discovered after the
# bump PR, the tag, ~30 minutes of cross-compilation and Gate A. That is F3's
# shape one step further down -- an avoidable cost paid at the most expensive
# possible moment -- so both entry points check the credential early.
#
# "Early" means something DIFFERENT at each entry point, and the difference is
# the whole point of the two bounds below. release.yml's `validate` checks it
# genuinely first, before anything exists to unwind. attach-to-release checks it
# AFTER the asset upload and BEFORE the canary -- deliberately not first,
# because this job also delivers binaries and un-drafts, neither of which needs
# a crates.io token.
# Located by STEP NAME, not by the log text inside it. The log-text locator
# worked, but the delta added ~30 lines of prose immediately above this step
# discussing the same token, and `head -1` takes whichever comes first --
# a growing collision surface for no benefit. A step name is structural.
TOKEN_CHECK_LINE="$(line_of '^[0-9]+:      - name: Check the crates\.io credential is present')"
if [[ -z "$TOKEN_CHECK_LINE" ]]; then
    fail "attach-to-release does not check for CARGO_REGISTRY_TOKEN" \
        "This job ends in a crates.io upload, and a missing credential is" \
        "deterministic, so it should fail BEFORE THE CANARY -- there is no point" \
        "spending the canary's minutes if the publish cannot happen." \
        "Put it AFTER the asset upload, not first: this job also attaches" \
        "binaries and un-drafts, and neither needs a crates.io token, so a check" \
        "at step 1 blocks binary delivery on a credential only the publish" \
        "requires. The two ordering assertions below enforce both bounds."
elif [[ -n "$CANARY_LINE" && "$TOKEN_CHECK_LINE" -gt "$CANARY_LINE" ]]; then
    fail "attach-to-release checks CARGO_REGISTRY_TOKEN only AFTER the canary (token $TOKEN_CHECK_LINE > canary $CANARY_LINE)" \
        "The point of the check is to fail fast. After the canary it saves nothing."
else
    # ...and it must sit AFTER the asset upload, which is the other bound and
    # the less obvious one.
    #
    # This check began as the job's FIRST step, and that coupled BINARY
    # DELIVERY to a crates.io credential. This job is the only path that
    # attaches binaries and un-drafts, and neither needs a crates token; an
    # expired token therefore blocked the reversible half on a credential only
    # the irreversible half requires -- the exact inversion of this PR's own
    # argument. The documented `gh workflow run cross-compile.yml --ref vX.Y.Z`
    # asset-recovery would have died at step 1 having done nothing.
    #
    # Both bounds are pinned because they fail in opposite directions: moved
    # BELOW the canary it stops being a fail-fast, moved ABOVE the upload it
    # starts blocking work that does not need it.
    UPLOAD_LINE="$(line_of 'gh release upload')"
    if [[ -z "$UPLOAD_LINE" ]]; then
        fail "no 'gh release upload' in the attach-to-release job" \
            "The credential check is positioned relative to it, so its absence" \
            "would make the ordering assertion below pass vacuously."
    elif [[ "$TOKEN_CHECK_LINE" -lt "$UPLOAD_LINE" ]]; then
        fail "the CARGO_REGISTRY_TOKEN check runs BEFORE the asset upload (token $TOKEN_CHECK_LINE < upload $UPLOAD_LINE)" \
            "That couples binary delivery to a crates.io credential it does not" \
            "need. An expired token would block asset attachment and the un-draft," \
            "and an operator re-running this job purely to re-attach assets would" \
            "get a job that fails having done nothing -- fixable only by rotating a" \
            "repo secret. Keep the check between the upload and the canary."
    else
        pass "attach-to-release checks CARGO_REGISTRY_TOKEN after the upload and before the canary (upload $UPLOAD_LINE < token $TOKEN_CHECK_LINE < canary $CANARY_LINE)"
    fi
fi

# ...and release.yml checks it in its FIRST job, which is the one that matters:
# before the bump PR exists there is nothing to unwind.
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"
if [[ ! -f "$RELEASE_YML" ]]; then
    fail "release.yml not found at $RELEASE_YML"
else
    VALIDATE_JOB="$(awk '
        /^  validate:[[:space:]]*$/       { inblock = 1; next }
        inblock && /^  [A-Za-z_.-]+:/     { inblock = 0 }
        inblock && $0 !~ /^[[:space:]]*#/ { print }
    ' "$RELEASE_YML")"
    if [[ -z "$VALIDATE_JOB" ]]; then
        fail "could not locate release.yml's 'validate' job" \
            "The credential check below scans it, so an empty extraction would" \
            "pass vacuously."
    elif [[ "$VALIDATE_JOB" != *'CARGO_REGISTRY_TOKEN'* ]]; then
        fail "release.yml's 'validate' job no longer checks CARGO_REGISTRY_TOKEN" \
            "It is the first job in the pipeline, so a missing credential caught" \
            "there costs nothing. Caught at the publish instead, it costs the bump" \
            "PR, the tag and the whole build."
    else
        # Scoped to the credential-check STEP, not to the whole job, and that
        # narrowing is not fussiness -- it is a mutation this pin previously
        # SURVIVED. The `validate` job contains other `exit 1`s (the version
        # format check, the crates.io auto-bump resolution), so a job-wide scan
        # for `exit 1` is satisfied by those: replacing the credential check's
        # own `exit 1` with `true`, turning a blocking check into a warning,
        # left the assertion GREEN. The pin was reading a neighbour's refusal
        # and reporting it as this one's -- the same shape as scanning a whole
        # job block for text that a different step supplies.
        # Extracted from `$VALIDATE_JOB`, NOT by re-awking the whole file.
        # Found during the N4 sibling sweep: this had the same unscoped shape,
        # taking the first step in release.yml whose name mentions "crates.io
        # token" regardless of which job it sits in. Only `validate` has one
        # today, so it was correct by luck rather than by construction -- and
        # "correct by luck" is what the N4 decoy turned into a green suite over
        # a neutered gate.
        VALIDATE_TOKEN_STEP="$(printf '%s\n' "$VALIDATE_JOB" | awk '
            /^      - name: .*crates\.io token/ { instep = 1; print; next }
            instep && /^      - name: /         { exit }
            instep                              { print }
        ' | grep -vE '^[[:space:]]*#')"
        if [[ -z "$VALIDATE_TOKEN_STEP" ]]; then
            fail "could not locate release.yml's crates.io token-check STEP" \
                "The refusal check below scans it, so an empty extraction would" \
                "pass vacuously. Expected a step whose name mentions 'crates.io token'."
        elif [[ "$VALIDATE_TOKEN_STEP" != *'exit 1'* ]]; then
            fail "release.yml's crates.io token check cannot refuse" \
                "No 'exit 1' in that STEP. A credential check that only warns does" \
                "not stop the release: it proceeds to the tag, the full build and" \
                "Gate A, and fails at the publish -- which is the expensive moment" \
                "this check exists to come before. Note the job has other 'exit 1's;" \
                "this deliberately does not count them."
        else
            pass "release.yml's crates.io token check is present in 'validate' and can refuse"
        fi
    fi
fi

# --- 2c. release.yml must not publish for real -------------------------------
# The other half of the same invariant, and the one a well-meaning revert would
# reach for: re-adding `cargo publish -p freenet` to release.yml restores the
# old upstream-of-the-gate order even with everything above still green,
# because release.yml runs entirely before the tag that triggers this workflow.
#
# `--dry-run` is explicitly allowed and deliberately kept there: it is not
# irreversible and it catches a packaging break before a tag is burned.
# (RELEASE_YML is set above, by the credential check.)
if [[ ! -f "$RELEASE_YML" ]]; then
    fail "release.yml not found at $RELEASE_YML"
else
    # Comment lines dropped for the same reason JOB_BLOCK drops them: this
    # file's comments discuss the publish at length.
    REAL_PUBLISH="$(grep -vE '^[[:space:]]*#' "$RELEASE_YML" \
        | grep -E 'cargo publish' | grep -vE '\-\-dry-run')"
    if [[ -z "$REAL_PUBLISH" ]]; then
        pass "release.yml contains no non-dry-run 'cargo publish' (the real one is gated here)"
    else
        fail "release.yml runs a real 'cargo publish'" \
            "$REAL_PUBLISH" \
            "release.yml runs BEFORE the tag exists, so anything it publishes is" \
            "upstream of the pre-flight canary. That ordering is what made a Gate A" \
            "block cost v0.2.124 its version number rather than a re-run. Only" \
            "'cargo publish --dry-run' belongs there."
    fi
fi

# --- 2c-bis. the fail-fast credential check can still FAIL -------------------
# 2b-ter pins that the check EXISTS and sits before the canary. Neither pins
# that it can still refuse. `continue-on-error: true` on it, or an `if:` that
# stops it running, leaves a step that looks like a fail-fast, logs like a
# fail-fast, and lets the release proceed to discover the missing credential
# after the tag and the full build -- which is the entire cost this step was
# added to avoid. Same neutering route assertion 3 closes for the canary; a
# check that cannot fail is not a check.
if [[ -n "$TOKEN_CHECK_LINE" ]]; then
    TOKEN_STEP="$(step_block "$TOKEN_CHECK_LINE")"
    if [[ -z "$TOKEN_STEP" ]]; then
        fail "could not extract the CARGO_REGISTRY_TOKEN check STEP around line $TOKEN_CHECK_LINE" \
            "The checks below scan this text, so an empty extraction passes vacuously."
    else
        # Step keys and shell swallows are separate routes to the same
        # outcome; both are checked, the shell half via the shared helper.
        TOKEN_KEYS="$(printf '%s\n' "$TOKEN_STEP" \
            | grep -E '^[0-9]+:        (continue-on-error:|if:)')"
        TOKEN_SWALLOW="$(status_swallowers "$TOKEN_STEP")"
        TOKEN_NEUTERED=0
        [[ -n "$TOKEN_KEYS$TOKEN_SWALLOW" ]] && TOKEN_NEUTERED=1
        # The step keys are only half of it: the step must still be ABLE to
        # refuse. Checked FIRST, because it is the mutation that survived.
        # This assertion previously located the step by its log text
        # (`CARGO_REGISTRY_TOKEN not set`) and then only scanned for
        # `continue-on-error:`/`if:`/`|| true`/`set +e` -- so replacing the
        # step's `exit 1` with `echo "proceeding without it"`, leaving the
        # `::error title=CARGO_REGISTRY_TOKEN not set::` line the locator greps
        # untouched, left this file reporting "the fail-fast check is not
        # disabled in place" about a check that could no longer fail.
        #
        # An IDENTITY pin wearing the label of a BEHAVIOUR pin, and the exact
        # defect already fixed for release.yml's twin in assertion 2b-ter --
        # fixed there because mutation testing caught it, and left armed at
        # nothing here because it was not mutated. Enumerate every site of a
        # shape, including the sites in your own test file.
        if [[ "$TOKEN_STEP" != *'exit 1'* ]]; then
            fail "the CARGO_REGISTRY_TOKEN fail-fast check cannot refuse" \
                "No 'exit 1' in the step. It still logs an ::error:: title, which is" \
                "what makes this shape dangerous -- the log looks like a blocked" \
                "release while the job proceeds to the canary and the publish, and" \
                "fails there instead, after the expensive part this check exists to" \
                "come before."
        elif [[ "$TOKEN_NEUTERED" -eq 0 ]]; then
            pass "the CARGO_REGISTRY_TOKEN fail-fast check can refuse and is not disabled in place"
        else
            fail "the CARGO_REGISTRY_TOKEN fail-fast check can no longer fail the job" \
                "$TOKEN_KEYS$TOKEN_SWALLOW" \
                "It still runs and still logs, but the job proceeds without the" \
                "credential -- so the failure lands at the crates.io publish instead," \
                "after the tag, ~30 minutes of cross-compilation and Gate A. A" \
                "fail-fast that does not fail saves nothing."
        fi
    fi
fi

# --- 2d. the RECOVERY RUNBOOK routes through the gate, not around it ---------
# The other route past Gate A, and the one that matters most, because it is the
# path a HUMAN takes when a release has already gone wrong (#5288).
# scripts/RELEASE_RECOVERY.md's `gh release create` invocations carried no
# `--draft`, so anyone following the documented recovery published a release the
# gate never sees -- and made it `/releases/latest` before any asset existed.
# Pinning the workflow and leaving the documented manual path ungated protects
# the machine and not the operator, who is the one acting under time pressure.
#
# Two traps this file family has been caught by, both avoided deliberately:
#
#   PROSE. The runbook now ARGUES for `--draft` at length, so a naive grep for
#   `gh release create` matches sentences ABOUT the command and a grep for
#   `--draft` is satisfied by the paragraph explaining it. Only fenced CODE
#   BLOCKS are scanned, so what is checked is what an operator would COPY.
#
#   VACUITY. Presence alone would pass if every invocation were deleted, so the
#   COUNT is asserted against a floor: the runbook has two `gh release create`
#   sites (Step 3, and step 4 of the full-manual section) and both must exist
#   and both must carry `--draft`. Fewer than two means one was removed rather
#   than fixed, and this fails rather than reporting success on an empty set.
#
# `--draft=false` is deliberately NOT accepted: that is the UN-draft, the thing
# only the workflow may do after Gate A has passed. Matching it here would let
# the exact inversion this assertion exists to catch pass as a fix.
# First the AUTOMATED twin of the same defect, which nothing pinned either.
# release.yml creates the release with `--draft` and cross-compile.yml un-drafts
# it after Gate A; drop that one flag and the release is born published, the
# gate never sees it, and the `release.published` cascade fires before a single
# binary is attached. Identical consequence to the runbook defect below, in the
# path that runs on every release rather than only on a bad day -- so it is
# pinned in the same place, next to its sibling, rather than left to be
# discovered the same way.
# (RELEASE_YML is set above, by the credential check.)
if [[ -f "$RELEASE_YML" ]]; then
    RY_CREATE="$(grep -vE '^[[:space:]]*#' "$RELEASE_YML" \
        | tr '\n' '\r' | sed 's/\\\r[[:space:]]*/ /g' | tr '\r' '\n' \
        | grep -E 'gh release create')"
    # NOT `| grep -q`: under `pipefail` the producer's SIGPIPE becomes the
    # pipeline's status, so a needle that IS present can read as ABSENT once the
    # producer outruns the pipe buffer. auto-update-canary_test.sh audits this
    # file for exactly that shape. Take the non-matching SET and test it for
    # emptiness instead, as assertion 2d does below.
    RY_UNDRAFTED="$(printf '%s\n' "$RY_CREATE" \
        | grep -vE '(^|[[:space:]])--draft([[:space:]]|$)')"
    if [[ -z "$RY_CREATE" ]]; then
        fail "release.yml no longer creates a GitHub release" \
            "The draft it creates is what cross-compile.yml attaches binaries to," \
            "gates behind Gate A and un-drafts. Without it there is nothing to gate."
    elif [[ -n "$RY_UNDRAFTED" ]]; then
        fail "release.yml creates the GitHub release WITHOUT --draft" \
            "$(printf '%s\n' "$RY_UNDRAFTED")" \
            "A release created published is a release Gate A never sees: it becomes" \
            "/releases/latest before any binary is attached, the downstream" \
            "release.published cascade fires immediately, and the crates.io publish" \
            "that lives behind the gate never runs. The whole pipeline order depends" \
            "on this one flag."
    else
        pass "release.yml creates the GitHub release with --draft"
    fi
fi

# Every operator-facing doc that documents creating the release, not just the
# runbook. RELEASING.md's manual-tag section carries its own `gh release create`
# and was correct but UNPINNED -- the same "one site of the shape covered, its
# sibling left armed at nothing" gap as the credential assertions above. A
# reader fixing the runbook has no reason to look in RELEASING.md, and the flag
# matters identically in both.
#
# Each doc declares its own expected count, so deleting invocations fails rather
# than passing on a smaller set.
#   scripts/RELEASE_RECOVERY.md : Step 3, and the full-manual section       -> 2
#   docs/RELEASING.md           : the "tag by hand" recovery section        -> 1
draft_docs=(
    "$SCRIPT_DIR/RELEASE_RECOVERY.md:2"
    "$SCRIPT_DIR/../docs/RELEASING.md:1"
)

for spec in "${draft_docs[@]}"; do
    doc="${spec%:*}"
    want="${spec##*:}"
    label="$(basename "$doc")"

    if [[ ! -f "$doc" ]]; then
        fail "$doc not found" \
            "It documents creating the release by hand; without it this" \
            "assertion checks nothing."
        continue
    fi

    # ANCHORED at the start of the logical command (`^gh release create`), so a
    # pipeline DIAGRAM line inside a fence is not counted as an invocation --
    # RELEASING.md has `└─→ gh release create --draft (release exists but
    # draft)` in its flow diagram. This matters for the FLOOR rather than for
    # the flag: counting the diagram would let someone delete the one real
    # invocation and still satisfy a floor of 1, which is exactly the vacuity
    # the floor exists to prevent.
    #
    # Logical commands from fenced code blocks only, with `\` continuations
    # joined so a multi-line invocation is judged as ONE command -- the runbook's
    # Step 3 spreads its flags over four lines, and per-LINE matching would
    # report its `--draft` missing purely for being on a different line from the
    # verb. Fencing also keeps PROSE out: both docs now argue for `--draft` at
    # length, so a whole-file grep would match sentences ABOUT the command and be
    # satisfied by the paragraph explaining it.
    logical="$(awk '
        /^```/          { infence = !infence; next }
        !infence        { next }
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            # Drop comment lines outright. The `^gh release create` anchor on
            # the FLOOR already excludes them, but the floor must not rest on
            # that anchor alone -- an operator does not copy a comment, and a
            # commented-out invocation inflating the floor is exactly the
            # mutation that got through once.
            if (line ~ /^#/) next
            acc = acc (acc == "" ? "" : " ") line
            if (line ~ /\\$/) { sub(/[[:space:]]*\\$/, "", acc); next }
            print acc
            acc = ""
        }
        END { if (acc != "") print acc }
    ' "$doc")"

    # TWO matchers over the same logical commands, and they are deliberately
    # different strengths. Conflating them cost a real hole:
    #
    #   FLOOR  -- anchored (`^gh release create`). Counts only line-initial
    #             invocations, so a pipeline DIAGRAM line inside a fence
    #             (`└─→ gh release create --draft ...` in RELEASING.md) cannot
    #             satisfy the floor on its own. Without the anchor, deleting the
    #             one real invocation still meets a floor of 1.
    #
    #   FLAG   -- UNANCHORED (word-boundary only). Every invocation an operator
    #             could copy must carry `--draft`, wherever the verb sits on the
    #             line.
    #
    # An earlier revision anchored BOTH, to close the diagram-line hole, and in
    # doing so blinded the FLAG check to any invocation that is not line-initial:
    # `GH_TOKEN="$PAT" gh release create ...` with no `--draft` passed the whole
    # suite, which reported "all 2 invocations pass --draft" while silent about
    # the third it could not see. That is #5288 reintroduced by the fix for a
    # different hole in the same assertion. Evading prefixes are anything before
    # the verb on the joined line: an env assignment, `sudo `, `&& `, a `$`
    # prompt. Leading whitespace is already stripped by the awk, and a `#`
    # comment correctly matches neither.
    #
    # GENERAL LESSON, worth more than this instance: tightening a matcher is a
    # behaviour change to EVERY input, not just the one that motivated it. When
    # you narrow a pattern, mutation-test both directions -- what you newly
    # catch, and what you could already catch.
    creates_anchored="$(printf '%s\n' "$logical" | grep -E '^gh release create')"
    creates_any="$(printf '%s\n' "$logical" | grep -E '(^|[[:space:]]|;|&&|\|\|)gh release create')"

    if [[ -z "$creates_anchored" ]]; then
        total=0
    else
        total="$(printf '%s\n' "$creates_anchored" | wc -l | tr -d ' ')"
    fi
    # `--draft` as its own flag: `--draft=false` must NOT satisfy it. That is
    # the un-draft, the thing only the workflow may do once Gate A has passed,
    # and accepting it here would let the exact inversion this assertion exists
    # to catch pass as a fix.
    undrafted="$(printf '%s\n' "$creates_any" \
        | grep -vE '(^|[[:space:]])--draft([[:space:]]|$)')"

    if [[ "$total" -lt "$want" ]]; then
        fail "$label has $total 'gh release create' invocation(s) in code blocks, expected at least $want" \
            "A lower count means an invocation was deleted rather than gated -- and" \
            "with none left this assertion would otherwise report success having" \
            "checked nothing."
    elif [[ -z "$undrafted" ]]; then
        pass "all $total 'gh release create' invocations in $label pass --draft"
    else
        fail "$label documents creating a release WITHOUT --draft" \
            "$(printf '%s\n' "$undrafted")" \
            "An operator following this creates a PUBLISHED release: Gate A never" \
            "sees it, the crates.io publish that lives behind the gate never runs," \
            "and it becomes /releases/latest before any binary is attached. That is" \
            "#5288 -- the documented procedure being the way around the gate. The" \
            "workflow creates its release with --draft for exactly this reason; the" \
            "manual paths must match it."
    fi
done

# --- shared: LOGICAL LINES ---------------------------------------------------
# `NNN:text` per logical shell/YAML command, with `\` continuations joined and
# UNQUOTED trailing comments stripped. Both scanners below use it, because both
# were defeated by the same two tricks:
#
#   * a `\`-continued invocation, so a per-line matcher saw only a fragment
#     (`cargo \` on one line, `publish` on the next);
#   * a trailing code comment, so `cargo publish -p freenet  # not a --dry-run`
#     satisfied a `--dry-run` exclusion, and
#     `gh release create ...  # dropped --draft here` satisfied a `--draft`
#     check. Only LEADING `#` lines were being dropped.
#
# That second one is one root cause wearing two costumes: "the string appears
# somewhere on the line" was being read as "the flag is in effect". Stripping
# comments at the point of extraction fixes both, once.
#
# The strip is QUOTE-AWARE -- it cuts at the first `#` that is unquoted and at a
# word boundary -- so a legitimate `--notes "fixes #5288"` is not truncated. A
# naive cut would silently shorten real commands and make the scanners miss what
# came after.
logical_lines() {
    awk '
        function strip_comment(str,   i, c, q, out) {
            q = ""
            out = ""
            for (i = 1; i <= length(str); i++) {
                c = substr(str, i, 1)
                if (q == "") {
                    if (c == "\"" || c == "'"'"'") { q = c }
                    else if (c == "#" && (i == 1 || substr(str, i - 1, 1) ~ /[[:space:]]/)) { break }
                } else if (c == q) { q = "" }
                out = out c
            }
            return out
        }
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            if (acc == "") startln = NR
            acc = acc (acc == "" ? "" : " ") line
            if (line ~ /\\$/) { sub(/[[:space:]]*\\$/, "", acc); next }
            out = strip_comment(acc)
            sub(/[[:space:]]+$/, "", out)
            if (out != "") print startln ":" out
            acc = ""
        }
        END {
            if (acc != "") {
                out = strip_comment(acc)
                sub(/[[:space:]]+$/, "", out)
                if (out != "") print startln ":" out
            }
        }
    ' "$1"
}

# A real `cargo publish`, tolerant of everything cargo accepts between the two
# words. `cargo[[:space:]]+publish` demanded adjacency, so `cargo +stable
# publish` -- and equally `cargo -q publish`, `cargo --offline publish` --
# walked straight past a scan whose own comment claimed to forbid exactly that
# step.
# `^[0-9]+:` because logical_lines emits `NNN:text` -- a bare `^` or
# `[[:space:]]` boundary never matches a command at the start of its line,
# since the character before `cargo` is the prefix colon. Caught by the
# assertion going to zero on a clean tree rather than by review.
# The leading boundary is "any character that cannot be part of a command word"
# rather than whitespace. Whitespace alone missed `echo "$(cargo publish ...)"`,
# where the character before `cargo` is `(` -- a command substitution smuggling a
# real publish past the scan. Excluding alnum/_/./- still refuses to match
# `mycargo publish`.
CARGO_PUBLISH_RE='(^[0-9]+:|[^[:alnum:]_./-])cargo([[:space:]]+[-+][^[:space:]]+)*[[:space:]]+publish([[:space:]]|$)'

# Prose mentions inside an echo are not invocations -- ci.yml documents the
# publish in one, and release.sh prints guidance quoting it. Narrow on purpose:
# no command separator AND no substitution between the `echo` and the words, so
# `echo hi; cargo publish` and `echo "$(cargo publish -p freenet)"` both remain
# visible. The substitution case is not theoretical; it was used to smuggle a
# real publish past the previous filter.
ECHO_PROSE_RE='echo[^;|&$`]*cargo([[:space:]]+[-+][^[:space:]]+)*[[:space:]]+publish'

# real_publishes <file> -- `NNN:text` for every non-dry-run cargo publish.
real_publishes() {
    logical_lines "$1" \
        | grep -E "$CARGO_PUBLISH_RE" \
        | grep -vE -- '--dry-run' \
        | grep -vE "$ECHO_PROSE_RE"
}

# --- 2c-ter. the ONLY real `cargo publish` anywhere is the gated one --------
# The whole-invariant check, and the one whose absence made every assertion
# above locally true and globally meaningless.
#
# This PR's stated invariant is "nothing irreversible may precede the blocking
# gate". Assertions 2b/2c enforce that in exactly one file and one job, so a
# `Push crates early` step added to `build-macos-dmg` -- an ordinary job that
# `attach-to-release` merely `needs:`, hence strictly earlier -- publishes both
# crates before Gate A with every assertion still true. Six lines reproduce the
# v0.2.124 loss in the file this suite claims to protect.
#
# So the rule is global: across the entire workflow set, every real
# (non-`--dry-run`) `cargo publish` must fall INSIDE the gated step's line
# range. Dry runs are whitelisted wherever they appear -- they upload nothing.
if [[ -n "$PUBLISH_CRATES_LINE" && -n "$PUBLISH_CRATES_STEP" ]]; then
    PC_FIRST="$(printf '%s\n' "$PUBLISH_CRATES_STEP" | head -1 | cut -d: -f1)"
    PC_LAST="$(printf '%s\n' "$PUBLISH_CRATES_STEP" | tail -1 | cut -d: -f1)"
    WF_DIR="$SCRIPT_DIR/../.github/workflows"
    WF_BASE="$(basename "$WF")"

    rogue_publishes=""
    gated_publishes=0
    scanned_files=0

    for _wf in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
        [[ -f "$_wf" ]] || continue
        scanned_files=$((scanned_files + 1))
        _base="$(basename "$_wf")"
        while IFS= read -r _hit; do
            [[ -z "$_hit" ]] && continue
            _ln="${_hit%%:*}"
            if [[ "$_base" == "$WF_BASE" && "$_ln" -ge "$PC_FIRST" && "$_ln" -le "$PC_LAST" ]]; then
                gated_publishes=$((gated_publishes + 1))
            else
                rogue_publishes+="$_base:$_hit"$'\n'
            fi
        done < <(real_publishes "$_wf")
    done

    if [[ "$scanned_files" -eq 0 ]]; then
        fail "found no workflow files to scan for stray 'cargo publish'" \
            "Every check below would pass having examined nothing."
    elif [[ -n "$rogue_publishes" ]]; then
        fail "a real 'cargo publish' exists OUTSIDE the gated step" \
            "$(printf '%s' "$rogue_publishes")" \
            "The gated step (lines $PC_FIRST-$PC_LAST of $WF_BASE) is the only place a" \
            "release may upload to crates.io, because it is the only place that runs" \
            "AFTER Gate A. A publish anywhere else -- including in a job that" \
            "attach-to-release merely 'needs:' -- happens BEFORE the gate, which is" \
            "precisely the ordering that cost v0.2.124 its version number."
    elif [[ "$gated_publishes" -lt 2 ]]; then
        fail "only $gated_publishes real 'cargo publish' invocation(s) inside the gated step, expected 2" \
            "The step publishes freenet and fdev. Fewer means one was removed or" \
            "turned into a dry run -- and with none left, the 'no rogue publishes'" \
            "check above would report success having found nothing to compare." \
            "(Scanned $scanned_files workflow file(s).)"
    else
        pass "the only real 'cargo publish' in $scanned_files workflow file(s) is the gated one ($gated_publishes invocations, lines $PC_FIRST-$PC_LAST)"
    fi
fi

# --- 2c-quater. no shell script publishes outside publish_crates() ----------
# The same question for the scripts, RECURSIVELY. The previous form globbed
# `"$SCRIPT_DIR"/*.sh`, which is not recursive, so `scripts/release-agent/` --
# a real release-path directory that CI already runs tests from -- was invisible
# to it. Wrong file set, same class as the workflow scan being job-scoped.
#
# release.sh legitimately CAN publish: it is the fallback for when the workflow
# path is broken. But only from `publish_crates()`, which the driver calls after
# `wait_for_binaries`, i.e. after the gate. Every other script: never.
#
# `*_test.sh` is excluded because test files stub and log `cargo publish` by
# design, and because this file's own text would otherwise match itself.
SH_ROGUE=""
SH_GATED=0
SH_SCANNED=0
PUBLISH_FN_START=""
PUBLISH_FN_END=""
if [[ -f "$RELEASE_SH" ]]; then
    PUBLISH_FN_START="$(grep -nE '^publish_crates\(\) \{' "$RELEASE_SH" | head -1 | cut -d: -f1)"
    if [[ -n "$PUBLISH_FN_START" ]]; then
        PUBLISH_FN_END="$(awk -v s="$PUBLISH_FN_START" 'NR > s && /^}/ { print NR; exit }' "$RELEASE_SH")"
        [[ -z "$PUBLISH_FN_END" ]] && PUBLISH_FN_END=999999
    fi
fi

while IFS= read -r _sh; do
    [[ -f "$_sh" ]] || continue
    case "$(basename "$_sh")" in *_test.sh) continue ;; esac
    SH_SCANNED=$((SH_SCANNED + 1))
    _base="$(basename "$_sh")"
    while IFS= read -r _hit; do
        [[ -z "$_hit" ]] && continue
        _ln="${_hit%%:*}"
        if [[ "$_sh" == "$RELEASE_SH" && -n "$PUBLISH_FN_START" \
              && "$_ln" -ge "$PUBLISH_FN_START" && "$_ln" -le "$PUBLISH_FN_END" ]]; then
            SH_GATED=$((SH_GATED + 1))
        else
            SH_ROGUE+="$_base:$_hit"$'\n'
        fi
    done < <(real_publishes "$_sh")
done < <(find "$SCRIPT_DIR" -name '*.sh' -type f | sort)

if [[ "$SH_SCANNED" -eq 0 ]]; then
    fail "found no shell scripts to scan for stray 'cargo publish'" \
        "The checks below would pass having examined nothing."
elif [[ -z "$PUBLISH_FN_START" ]]; then
    fail "release.sh has no publish_crates() function" \
        "The containment check has nothing to contain."
elif [[ -n "$SH_ROGUE" ]]; then
    fail "a shell script runs a real 'cargo publish' outside publish_crates()" \
        "$(printf '%s' "$SH_ROGUE")" \
        "publish_crates() is called after wait_for_binaries, i.e. after the gate." \
        "A publish anywhere else -- another function, another script, a helper in" \
        "scripts/release-agent/ -- runs at whatever point that code is reached," \
        "with nothing between it and the tag."
elif [[ "$SH_GATED" -lt 2 ]]; then
    fail "release.sh's publish_crates() has $SH_GATED real 'cargo publish' invocation(s), expected 2" \
        "It is the manual backstop for freenet and fdev. With none, the check" \
        "above would report success having found nothing." \
        "(Scanned $SH_SCANNED shell script(s).)"
else
    pass "across $SH_SCANNED shell script(s), real 'cargo publish' occurs only in publish_crates() ($SH_GATED)"
fi

# --- 2e. EVERY executable site that creates a release passes --draft --------
# The `--draft` invariant enumerated over CODE, not over the sites it was first
# written for. It was pinned in release.yml and the two markdown runbooks and
# missed `scripts/release.sh` -- the one that runs on EVERY NORMAL RELEASE
# rather than only on a bad day.
#
# Uses the same `logical_lines`, so a `\`-continued invocation is judged whole
# and a trailing `# dropped --draft here` comment cannot satisfy the flag check.
# The file set is RECURSIVE for the same reason as 2c-quater.
#
# Docs are covered by assertion 2d, which needs fence extraction; this needs
# none. `*_test.sh` is excluded: this file contains `gh release create` in its
# own greps and failure text, and a scan matching its own source is the
# self-satisfying shape this suite exists to remove.
CODE_CREATE_SITES=()
for _f in "$SCRIPT_DIR"/../.github/workflows/*.yml "$SCRIPT_DIR"/../.github/workflows/*.yaml; do
    [[ -f "$_f" ]] && CODE_CREATE_SITES+=("$_f")
done
while IFS= read -r _f; do
    [[ -f "$_f" ]] || continue
    case "$(basename "$_f")" in *_test.sh) continue ;; esac
    CODE_CREATE_SITES+=("$_f")
done < <(find "$SCRIPT_DIR" -name '*.sh' -type f | sort)

code_creates_total=0
code_creates_undrafted=""
for _f in "${CODE_CREATE_SITES[@]}"; do
    _base="$(basename "$_f")"
    while IFS= read -r _hit; do
        [[ -z "$_hit" ]] && continue
        code_creates_total=$((code_creates_total + 1))
        # Shell punctuation normalised so `--draft"` and `--draft)` count, while
        # `=` is left alone so `--draft=false` -- the UN-draft -- still fails.
        _flags=" $(printf '%s' "${_hit#*:}" | tr '"'"'"'"()' '    ') "
        case "$_flags" in
            *" --draft "*) ;;
            *) code_creates_undrafted+="$_base:$_hit"$'\n' ;;
        esac
    done < <(logical_lines "$_f" | grep -E 'gh release create')
done
code_creates_undrafted="$(printf '%s' "$code_creates_undrafted" | grep -vE '^[[:space:]]*$' || true)"

if [[ "$code_creates_total" -lt 2 ]]; then
    fail "only $code_creates_total 'gh release create' invocation(s) found in workflows + scripts, expected at least 2" \
        "release.yml creates the draft on the automated path and release.sh creates" \
        "it on the manual one. Fewer means one was removed -- and with none, the" \
        "--draft check below would report success having examined nothing."
elif [[ -z "$code_creates_undrafted" ]]; then
    pass "all $code_creates_total 'gh release create' invocations in workflows + scripts pass --draft"
else
    fail "an executable path creates a GitHub release WITHOUT --draft" \
        "$(printf '%s' "$code_creates_undrafted")" \
        "A release created published is a release Gate A never sees: it becomes" \
        "/releases/latest before any binary is attached, the release.published" \
        "cascade fires immediately, and the gated crates.io publish never runs." \
        "This is #5288 in code rather than in documentation, and release.sh's copy" \
        "runs on every normal release, not only during a recovery."
fi

# --- 2f. release.sh publishes AFTER the gate, not before --------------------
# The second invariant, enumerated over the human path.
#
# `scripts/release.sh` holds its own copy of the ordering this whole PR is
# about, and moving its `publish_crates` call back above `create_github_release`
# reproduces the v0.2.124 loss on the manual path with every suite green. The
# line directly above that call says "Moving it back up is what cost v0.2.124
# its version number" -- a comment asserting the invariant, with nothing
# enforcing it. A comment is not a gate.
#
# The tag push that triggers cross-compile.yml (and therefore Gate A) happens
# inside `create_github_release`, and `wait_for_binaries` is what blocks until
# that workflow has run. So `publish_crates` must come after BOTH: after the
# first because otherwise it publishes before the gate exists, and after the
# second because otherwise it publishes while the gate is still running.
#
# Matched on bare top-level calls (`^name$`), which is how release.sh's main
# flow invokes them -- definitions are `name() {` and so cannot collide.
if [[ -f "$RELEASE_SH" ]]; then
    _line_of_call() {
        grep -nE "^$1\$" "$RELEASE_SH" | head -1 | cut -d: -f1
    }
    RS_PUBLISH="$(_line_of_call publish_crates)"
    RS_CREATE="$(_line_of_call create_github_release)"
    RS_WAIT="$(_line_of_call wait_for_binaries)"

    if [[ -z "$RS_PUBLISH" || -z "$RS_CREATE" || -z "$RS_WAIT" ]]; then
        fail "could not locate release.sh's main-flow calls" \
            "publish_crates=${RS_PUBLISH:-<none>} create_github_release=${RS_CREATE:-<none>} wait_for_binaries=${RS_WAIT:-<none>}" \
            "The ordering assertions below compare these line numbers, so a missing" \
            "one would make them pass vacuously. If the driver was restructured," \
            "re-establish the ordering guarantee here rather than deleting this."
    else
        _rs_bad=""
        [[ "$RS_PUBLISH" -lt "$RS_CREATE" ]] && \
            _rs_bad+="publish_crates ($RS_PUBLISH) runs BEFORE create_github_release ($RS_CREATE)"$'\n'
        [[ "$RS_PUBLISH" -lt "$RS_WAIT" ]] && \
            _rs_bad+="publish_crates ($RS_PUBLISH) runs BEFORE wait_for_binaries ($RS_WAIT)"$'\n'
        if [[ -z "$_rs_bad" ]]; then
            pass "release.sh publishes after the tag and the gate (create $RS_CREATE < wait $RS_WAIT < publish $RS_PUBLISH)"
        else
            fail "release.sh's crates.io publish runs BEFORE the gate" \
                "$(printf '%s' "$_rs_bad")" \
                "create_github_release pushes the tag that triggers cross-compile.yml," \
                "and wait_for_binaries blocks until that workflow -- including Gate A --" \
                "has run. A publish above either one uploads to crates.io before the" \
                "gate can block, which is exactly the ordering that cost v0.2.124 its" \
                "version number. This is the same invariant the workflow assertions" \
                "above enforce, on the path a human drives."
        fi
    fi
fi

# --- 2g. THE GATE STEPS ARE EXECUTED, not pattern-matched -------------------
#
# WHY THIS EXISTS, and why the static "does it swallow?" pins below are no
# longer the guarantee.
#
# Those pins enumerate SPELLINGS of "ignore the exit status": `|| true`, then
# `|| echo`, then any `||`, then `set +e`. Shell has unbounded ways to spell it,
# so the enumeration lost four times in one day, each time to a spelling nobody
# had listed:
#
#     if ! bash .../auto-update-canary.sh preflight /tmp/freenet; then
#       echo "::warning::soft-failed"; fi     # no `||` anywhere; `if` is
#                                             # errexit-exempt and `fi` returns 0
#     bash .../auto-update-canary.sh preflight /tmp/freenet & echo started
#     set +o errexit                          # the long-option spelling
#     rc=0                                    # inserted just above `exit "$rc"`
#
# The last one is the proof that no line-existence pin can close this class:
# assertion 6e requires `|| rc=$?` AND `exit "$rc"`, BOTH of which are still
# present and correct. The invariant between them is broken by a THIRD line
# that contains nothing suspicious. A pin that asserts lines EXIST cannot see
# an invariant that BREAKS BETWEEN them.
#
# So the guarantee is behavioural, using the technique this file already proves
# on the publish step: extract the step's `run:` block, execute it against a
# stubbed canary, and assert THE STEP'S OWN EXIT STATUS. Every spelling above
# dies against that, including the next one neither reviewer nor author has
# thought of, because it stops asking "what does the text look like" and starts
# asking "does a failing canary fail the step".
#
# The static pins are KEPT and are not redundant: `continue-on-error:` and
# `if: always()` are ACTIONS-level, and executing a `run:` block cannot see
# them. Execution covers the shell; the static pins cover the step keys.
# Neither subsumes the other -- do not delete either believing the other
# covers it.
#
# FIDELITY. Actions runs a `run:` block with `bash -e {0}` when the step sets
# no `shell:` (errexit; pipefail applies only to an explicit `shell: bash`).
# Neither gate step sets one and this workflow has no `defaults:` block, so the
# harness uses `bash -e`. That assumption is pinned immediately below -- adding
# `shell: bash {0}` (no `-e`) would change real behaviour while leaving this
# harness testing the old semantics and green, which is the N4
# harness-diverges-from-reality hole in a new place.

for _gate in "attach-to-release:Auto-update pre-flight canary (blocks publish)" \
             "auto-update-selfupdate-canary:Previous release self-updates to this release"; do
    _gjob="${_gate%%:*}"
    _gname="${_gate#*:}"
    _gblock="$(yaml_job_block "$_gjob" --numbered)"
    # `--` is load-bearing: the pattern begins with `-`, so without it grep
    # parses it as options and matches nothing -- which made this pin report
    # "could not find the step" on a tree where the step was present.
    _gline="$(printf '%s\n' "$_gblock" | grep -F -- "- name: $_gname" | head -1 | cut -d: -f1)"
    if [[ -z "$_gline" ]]; then
        fail "could not find the step '$_gname' in job '$_gjob'" \
            "The shell-override pin below would pass having examined nothing."
        continue
    fi
    _gstep="$(printf '%s\n' "$_gblock" \
        | awk -F: -v a="$_gline" '$1 >= a' \
        | awk -F: 'NR == 1 { print; next } /^[0-9]+:      - name:/ { exit } { print }')"
    _gshell="$(printf '%s\n' "$_gstep" | grep -E '^[0-9]+:        shell:')"
    if [[ -z "$_gshell" ]]; then
        pass "'$_gname' sets no 'shell:', so the harness's 'bash -e' matches Actions' default"
    else
        fail "'$_gname' now overrides 'shell:'" \
            "$(printf '%s\n' "$_gshell")" \
            "The behavioural harness below runs this block under 'bash -e', which is" \
            "Actions' default for a step with no 'shell:'. An override changes the" \
            "real semantics -- 'shell: bash {0}' drops errexit entirely -- while the" \
            "harness keeps testing the old ones and stays green. Update the harness" \
            "deliberately, or drop the override."
    fi
done

# exec_step_status <job> <step-name> <canary-exit> <substitutions-file> -- runs
# the step's `run:` block with a stub canary and echoes "<rc>|<classification>".
#
# The stub is installed at BOTH paths the two gates invoke (`_canary/scripts/`
# for Gate A, `scripts/` for Gate B) so one helper serves both.
exec_step_status() {
    local job="$1" name="$2" canary_rc="$3" work rc cls
    local script
    script="$(extract_step_run "$job" "$name")"
    if [[ -z "$script" ]]; then
        echo "EXTRACT-FAILED|"
        return
    fi
    work="$(mktemp -d)"
    mkdir -p "$work/bin" "$work/scripts" "$work/_canary/scripts"

    # The canary stub: the ONLY thing under test is whether its exit status
    # reaches the step's exit status.
    printf '#!/usr/bin/env bash\nexit %s\n' "$canary_rc" > "$work/scripts/auto-update-canary.sh"
    cp "$work/scripts/auto-update-canary.sh" "$work/_canary/scripts/auto-update-canary.sh"
    chmod +x "$work/scripts/auto-update-canary.sh" "$work/_canary/scripts/auto-update-canary.sh"

    # Gate A untars and chmods the shipped binary; neither is under test.
    printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/tar"
    printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/chmod"
    chmod +x "$work/bin/tar" "$work/bin/chmod"

    # GitHub expressions do not expand outside Actions. Substitute the ones
    # these blocks use, then REFUSE if any `${{` survives: an unsubstituted
    # expression is a bash bad-substitution that exits non-zero on its own,
    # which would satisfy a "must exit non-zero" case for entirely the wrong
    # reason. That is the vacuity this whole file keeps having to remove.
    script="${script//\$\{\{ steps.prev.outputs.prev_version \}\}/0.0.1}"
    script="${script//\$\{\{ steps.prev.outputs.this_version \}\}/0.0.2}"
    # shellcheck disable=SC2016  # literal GitHub expression syntax; must not expand
    if [[ "$script" == *'${{'* ]]; then
        rm -rf "$work"
        echo "UNSUBSTITUTED|"
        return
    fi
    printf '%s\n' "$script" > "$work/step.sh"

    : > "$work/gh_output"
    (
        cd "$work" || exit 99
        PATH="$work/bin:$PATH" GITHUB_OUTPUT="$work/gh_output" \
            bash -e "$work/step.sh"
    ) > "$work/out" 2>&1
    rc=$?
    cls="$(sed -n 's/^classification=//p' "$work/gh_output" | head -1)"
    rm -rf "$work"
    echo "$rc|$cls"
}

# gate_case <desc> <job> <step> <canary-exit> <want-rc-spec> [want-classification]
#   want-rc-spec: an exact number, or "nonzero"
gate_case() {
    local desc="$1" job="$2" step="$3" cexit="$4" want="$5" want_cls="${6:-}"
    local got rc cls ok=1
    got="$(exec_step_status "$job" "$step" "$cexit")"
    rc="${got%%|*}"
    cls="${got#*|}"

    case "$rc" in
        EXTRACT-FAILED)
            fail "gate behaviour: $desc" \
                "could not extract the step's run: block -- every case would" \
                "otherwise pass on an empty script."
            return ;;
        UNSUBSTITUTED)
            fail "gate behaviour: $desc" \
                "the run: block still contains an unexpanded \${{ }} expression." \
                "Running it would exit non-zero for that reason alone, which would" \
                "satisfy a 'must fail' case without testing the guard at all." \
                "Add the substitution to exec_step_status."
            return ;;
    esac

    case "$want" in
        nonzero) [[ "$rc" -ne 0 ]] || ok=0 ;;
        *)       [[ "$rc" -eq "$want" ]] || ok=0 ;;
    esac
    [[ -n "$want_cls" && "$cls" != "$want_cls" ]] && ok=0

    if [[ "$ok" -eq 1 ]]; then
        pass "gate behaviour: $desc (canary exit $cexit -> step rc=$rc${want_cls:+, classification=$cls})"
    else
        local _got_cls_msg="" _want_cls_msg=""
        if [[ -n "$want_cls" ]]; then
            _got_cls_msg=" with classification=$cls"
            _want_cls_msg=" and classification $want_cls"
        fi
        fail "gate behaviour: $desc" \
            "canary stub exited $cexit; step exited $rc$_got_cls_msg" \
            "expected rc $want$_want_cls_msg" \
            "A failing canary that does not fail its step is a gate that does not" \
            "gate: for Gate A the release publishes anyway, for Gate B the job goes" \
            "green on a broken updater and the notify job never fires."
    fi
}

GATE_A_JOB=attach-to-release
GATE_A_STEP='Auto-update pre-flight canary (blocks publish)'
GATE_B_JOB=auto-update-selfupdate-canary
GATE_B_STEP='Previous release self-updates to this release'

# THE case: a failing canary must fail the step. Kills every swallow spelling.
gate_case "Gate A fails when the canary fails" "$GATE_A_JOB" "$GATE_A_STEP" 1 nonzero
# The POSITIVE CONTROL. Without it, a step that is simply broken -- or one whose
# extraction produced nonsense -- satisfies the case above for the wrong reason.
gate_case "Gate A passes when the canary passes" "$GATE_A_JOB" "$GATE_A_STEP" 0 0

# Gate B additionally owes the notify job a classification, so each case pins
# the exit status AND the value written to GITHUB_OUTPUT.
gate_case "Gate B fails, and classifies 'fault', when the canary fails" \
    "$GATE_B_JOB" "$GATE_B_STEP" 1 nonzero fault
gate_case "Gate B re-raises exit 75 and classifies 'environmental'" \
    "$GATE_B_JOB" "$GATE_B_STEP" 75 75 environmental
gate_case "Gate B passes and classifies 'ok' when the canary passes" \
    "$GATE_B_JOB" "$GATE_B_STEP" 0 0 ok

# --- 3. it is not neutered in place -----------------------------------------
# `continue-on-error: true` leaves the step present, running, and visibly
# green-ish in the UI while the job proceeds to publish regardless -- the
# cheapest way to disable this gate without appearing to remove it.
#
# The scan must cover the whole STEP, not the run: line onwards. Step keys
# (`continue-on-error`, `if`, `timeout-minutes`) sit ABOVE the `run:` that
# contains the invocation, so a scan anchored on the invocation line misses
# them entirely -- verified by mutation: this assertion did not fire until the
# bounds were widened to the step.
if [[ -n "$CANARY_LINE" ]]; then
    CANARY_STEP="$(step_block "$CANARY_LINE")"
    NEUTERED="$(printf '%s\n' "$CANARY_STEP" \
        | grep -cE 'continue-on-error:[[:space:]]*true|^[0-9]+:        if:[[:space:]]*false')"
    if [[ "$NEUTERED" -eq 0 ]]; then
        pass "the canary step is not disabled in place"
    else
        fail "the canary step is disabled in place ('continue-on-error: true' or 'if: false')" \
            "It still runs and still reports, but the job publishes the release" \
            "whatever it finds. That is a gate in appearance only."
    fi

    # --- 3a. ...and its shell does not swallow the canary's exit status ------
    # SECOND LINE OF DEFENCE, not the first. Assertion 2g executes this step
    # against a failing stub canary and asserts it exits non-zero, which covers
    # every spelling including the ones this pattern misses. What 2g CANNOT see
    # is anything above the `run:` block -- `continue-on-error:`, `if: always()`
    # -- because those are Actions-level and a harness that extracts and runs
    # the script never encounters them. That is what assertion 3 covers, and
    # why neither assertion subsumes the other. Do not delete one believing the
    # other covers it.
    # The step-key checks above are blind to the SHELL. Appending `|| true` to
    # the invocation leaves the step present, before the publish, and without
    # `continue-on-error` -- every assertion here passed under exactly that
    # mutation, because assertion 1 matches the invocation as a SUBSTRING and
    # anything appended to the line is invisible to it.
    #
    # This is the likelier of the two neutering routes, and the worse one.
    # `|| true` is the reflex fix when a gate false-positives at 2am, and it
    # does not read as disabling a gate -- which is exactly why it has to fail
    # loudly here. The value of this gate is that removing it cannot be quiet.
    #
    # Scanning the whole step rather than just the canary line: `set +e`
    # anywhere in the run block has the same effect, and the step is three
    # lines, so there is no legitimate use of these to trip over. Comment-only
    # lines were dropped when JOB_BLOCK was built, so a `# || true` in prose
    # cannot fire this.
    SWALLOWED="$(status_swallowers "$CANARY_STEP")"
    if [[ -z "$SWALLOWED" ]]; then
        pass "the canary step's shell does not swallow its own exit status"
    else
        fail "the canary step swallows its own exit status (any '||', 'set +e' or trailing '; true')" \
            "$SWALLOWED" \
            "The step still runs, still reports, and still sits before the publish," \
            "but it can no longer fail -- so nothing blocks publication. This is the" \
            "cheapest possible way to disable the gate and the least visible: it looks" \
            "like error handling, not like removing a release gate."
    fi
fi

# --- 3b. ...and the PUBLISH step is not made unconditional ------------------
# The mirror image of the check above, and the hole it left. Assertion 3 asks
# whether the CANARY is disabled; nothing asked whether the PUBLISH step was
# made to run regardless of it. Both produce the same outcome -- a red canary
# that no longer blocks publication -- and only one was pinned.
#
# Demonstrated on this branch: adding `if: always()` to the `Publish release`
# step neutered Gate A completely and all six assertions here stayed GREEN.
# release.sh's belt-and-suspenders check gives no cover either, because the
# workflow really did publish, so `isDraft` reads false.
#
# Steps default to running only if every earlier step in the job succeeded, and
# that default IS the gate. The publish step has no `if:` today, and this pins
# that state rather than trying to judge which conditionals are safe.
#
# That same default now carries a SECOND invariant, since the crates.io upload
# moved into this job above the un-draft: a failed `cargo publish` must also
# stop the un-draft. Give this step `if: always()` and a release whose crates
# never uploaded is published anyway, which splits the two distribution
# channels -- `freenet update` and the direct downloads serve the new version
# while `cargo install freenet` silently still serves the previous one. The
# correct state after a failed publish is the one the default produces: a draft
# with its assets attached and no crates, consistent and recoverable by hand
# (scripts/RELEASE_RECOVERY.md step 4). Both invariants rest on this one
# absent `if:`, which is why it is pinned here rather than restated twice.
#
# Deliberately stricter than "no always()/failure()/cancelled()". Whether an
# expression can evaluate true after a failed step is not something a grep
# should be deciding -- `success() || github.actor == 'x'` overrides the default
# without naming any of those functions. An `if:` on the one step whose
# conditional execution IS the release gate deserves a human look, so any `if:`
# at all fails here. If a legitimate one is ever needed, the person adding it
# updates this assertion on purpose, which is the point.
if [[ -n "$PUBLISH_LINE" ]]; then
    PUBLISH_STEP="$(step_block "$PUBLISH_LINE")"
    PUBLISH_IF="$(printf '%s\n' "$PUBLISH_STEP" | grep -E '^[0-9]+:        if:')"
    if [[ -z "$PUBLISH_IF" ]]; then
        pass "the publish step has no 'if:', so it still runs only when the canary passed"
    else
        fail "the publish step has acquired an 'if:'" \
            "$(printf '%s\n' "$PUBLISH_IF")" \
            "Steps run only after every earlier step succeeded, and that default is" \
            "the ENTIRE mechanism by which the canary blocks publication. An 'if:'" \
            "here can override it -- 'if: always()' publishes the release even when" \
            "the canary failed, and the gate is gone without the canary step being" \
            "touched at all. If this conditional is genuinely wanted, confirm it" \
            "cannot evaluate true after a failed step, then update this assertion."
    fi
fi

# --- 4. CI must not pre-set CANARY_EXPECTED_LATEST --------------------------
# Gate A resolves the expected release itself, from the same `releases/latest`
# redirect the node uses, and refuses if it cannot. A value supplied by the
# workflow would displace that resolution with a hand-maintained string.
#
# Note what this does and does not protect against, because the commit that
# introduced the skip branch overstated it. A pinned value can only make the
# check FAIL -- it cannot make it PASS vacuously -- provided the pinned value
# is WRONG. Pin it CORRECTLY (say to the tag being released, which during
# Gate A is not yet what `releases/latest` returns) and you have replaced a
# resolved fact with an asserted one: the gate then compares the node's answer
# against a constant somebody typed, which is precisely the class of check this
# canary exists to replace. Either way it should not be here, so pin its
# absence rather than reasoning about which failure mode it would cause.
#
# Nothing sets it today; that is the state being pinned.
ARMED="$(printf '%s\n' "$JOB_BLOCK" | grep -cE 'CANARY_EXPECTED_LATEST')"
if [[ "$ARMED" -eq 0 ]]; then
    pass "the workflow does not pre-set CANARY_EXPECTED_LATEST (Gate A resolves it)"
else
    fail "the attach-to-release job sets CANARY_EXPECTED_LATEST" \
        "Gate A resolves the expected release from the same redirect the node reads," \
        "and refuses if it cannot. A workflow-supplied value replaces that resolved" \
        "fact with a hand-maintained constant -- and if it is wrong, it fails a" \
        "healthy release for a difference that is not a bug." \
        "$(printf '%s\n' "$JOB_BLOCK" | grep -E 'CANARY_EXPECTED_LATEST')"
fi

# --- 5. release.sh and the workflow agree on the job name -------------------
# release.sh reads this job's status by DISPLAY NAME. Nothing else pins the
# pair, and a rename on either side is silent: the driver simply never sees the
# job, waits out its 20-minute timeout, and reports UNKNOWN for a release that
# published normally.
WF_JOB_NAME="$(printf '%s\n' "$JOB_BLOCK" \
    | sed -n 's/^[0-9]*:    name:[[:space:]]*//p' | head -1 \
    | sed "s/^['\"]//;s/['\"]$//")"
SH_JOB_NAME="$(sed -n "s/^ATTACH_JOB_NAME=//p" "$RELEASE_SH" | head -1 \
    | sed "s/^['\"]//;s/['\"]$//")"

# ...and the RECOVERY RUNBOOK reads the same two names out of this workflow.
#
# The rewritten gate-confirm command selects the job with
# `startswith("Attach binaries")` and the step with
# `startswith("Auto-update pre-flight")`. Neither was pinned, which made it a
# THIRD site of the shape this file exists to close -- and it was added by the
# commit that argues "one site covered, its sibling left armed" is the gap
# being closed. Rename either name and the command silently selects nothing.
#
# It fails CLOSED (the runbook says "no line at all ... means Gate A did not
# pass. Stop"), so the risk is a false alarm blocking a good release rather than
# a publish slipping through -- which is why this is a prefix check rather than
# an exact-match, matching what the `--jq` actually does.
RUNBOOK_STEP_NAME="$(printf '%s\n' "$JOB_BLOCK" \
    | sed -n 's/^[0-9]*:      - name:[[:space:]]*\(Auto-update pre-flight.*\)$/\1/p' | head -1)"
for _pair in "Attach binaries:${WF_JOB_NAME:-}" "Auto-update pre-flight:${RUNBOOK_STEP_NAME:-}"; do
    _prefix="${_pair%%:*}"
    _actual="${_pair#*:}"
    if [[ -z "$_actual" ]]; then
        fail "cross-compile.yml has no name starting with '$_prefix'" \
            "scripts/RELEASE_RECOVERY.md's Gate A confirmation selects it with" \
            "startswith(\"$_prefix\"). With nothing matching, the documented check" \
            "prints no output -- which the runbook tells the operator to read as" \
            "'Gate A did not pass', so a healthy release looks blocked."
    elif [[ "$_actual" == "$_prefix"* ]]; then
        pass "RELEASE_RECOVERY.md's startswith(\"$_prefix\") still matches ('$_actual')"
    else
        fail "RELEASE_RECOVERY.md selects on a prefix cross-compile.yml no longer has" \
            "runbook expects a name starting with: '$_prefix'" \
            "cross-compile.yml has:                 '$_actual'" \
            "The documented Gate A confirmation would print nothing, which the" \
            "runbook says to treat as a failed gate. Update both together."
    fi
done

if [[ -z "$WF_JOB_NAME" ]]; then
    fail "the attach-to-release job has no 'name:' in cross-compile.yml" \
        "release.sh matches on the display name, which defaults to the job KEY" \
        "when 'name:' is absent -- so removing it silently breaks the driver."
elif [[ -z "$SH_JOB_NAME" ]]; then
    fail "ATTACH_JOB_NAME not found in release.sh"
elif [[ "$WF_JOB_NAME" == "$SH_JOB_NAME" ]]; then
    pass "release.sh ATTACH_JOB_NAME matches the workflow job name ('$WF_JOB_NAME')"
else
    fail "release.sh and cross-compile.yml disagree on the attach job's name" \
        "cross-compile.yml: '$WF_JOB_NAME'" \
        "release.sh:        '$SH_JOB_NAME'" \
        "The driver polls for the workflow's name, so it would wait for a job" \
        "that never appears and time out reporting UNKNOWN."
fi

# --- 6. the failure notification still covers BOTH gates --------------------
# Gate A blocks publication by failing, which leaves the release stuck as a
# DRAFT -- a silent state. Nobody is watching the Actions tab during a release;
# the Matrix message is how anyone finds out. Gate B runs after publication and
# cannot block anything, so the notification is its ONLY output.
#
# Neither the notify job nor Gate B's job was referenced by any test in this
# repo before this. Demonstrated on this branch: deleting the two
# `needs.attach-to-release.result` clauses from the notify job's `if:` --
# reinstating exactly the silent-Gate-A regression the workflow comment
# describes -- left all four suites GREEN.
#
# Whole-file scan, not the attach-to-release block: these are separate
# top-level jobs.
notify_block="$(yaml_job_block notify-auto-update-canary-failure)"

if [[ -z "$notify_block" ]]; then
    fail "the 'notify-auto-update-canary-failure' job is gone from cross-compile.yml" \
        "A failed Gate A leaves the release stuck as a draft and says nothing;" \
        "a failed Gate B has no other output at all. This job is how either" \
        "one reaches a human."
else
    pass "cross-compile.yml still has the pre-flight failure notification job"

    # Each gate contributes two result states. `failure` alone is not enough:
    # a cancelled job is not a passed one, and treating it as passed is how a
    # timed-out gate goes unreported.
    missing=()
    for clause in \
        "needs.attach-to-release.result == 'failure'" \
        "needs.attach-to-release.result == 'cancelled'" \
        "needs.auto-update-selfupdate-canary.result == 'failure'" \
        "needs.auto-update-selfupdate-canary.result == 'cancelled'"
    do
        [[ "$notify_block" == *"$clause"* ]] || missing+=("$clause")
    done

    if [[ ${#missing[@]} -eq 0 ]]; then
        pass "the notification fires for failure AND cancellation of both gates"
    else
        fail "the notification no longer covers every gate outcome" \
            "Missing from the notify job's 'if:':" \
            "${missing[@]}" \
            "A gate whose failure notifies nobody is a gate nobody acts on. Gate A" \
            "failing leaves the release a silent DRAFT; Gate B has no other output."
    fi

    # `always()` is what lets the job run at all after a needed job failed.
    # Without it the notification is skipped in exactly the case it exists for.
    if [[ "$notify_block" == *'always()'* ]]; then
        pass "the notify job runs under always() (so a failed gate does not skip it)"
    else
        fail "the notify job's 'if:' no longer calls always()" \
            "A job whose dependency failed is SKIPPED unless its condition calls" \
            "always(). Without it this job never runs on the one path it exists for."
    fi

    # It must also still DEPEND on both, or the results it tests are always ''.
    for needed in attach-to-release auto-update-selfupdate-canary; do
        if [[ "$notify_block" == *"needs:"*"$needed"* ]]; then
            pass "the notify job still needs '$needed'"
        else
            fail "the notify job no longer lists '$needed' in 'needs:'" \
                "A result expression for a job that is not needed evaluates to the" \
                "empty string, so every clause above silently stops matching."
        fi
    done

    # --- 6b. the ENVIRONMENTAL split cannot swallow the real alarm ----------
    # The notify job now picks between two messages: a quiet one when the
    # canary classified the run as environmental (it could not reach GitHub, so
    # it learned nothing), and the loud #5221 one otherwise. Silencing the
    # channel is now a two-line edit -- widen the quiet branch's condition, or
    # delete the loud step -- and either would look like tidying.
    #
    # Three properties, each of which alone is enough to lose the alarm:
    #
    #   (a) the loud message still exists at all;
    #   (b) the quiet branch requires BOTH that the publish job succeeded and
    #       that the classification is exactly 'environmental'. Drop the first
    #       and a release stuck as a DRAFT gets the reassuring message; drop
    #       the second and everything does;
    #   (c) the loud branch is the NEGATION of the quiet one rather than its
    #       own enumeration of results. That is what puts every unanticipated
    #       state -- a cancelled job, an empty classification because the step
    #       never ran -- on the side that gets read.
    if [[ "$notify_block" == *'#5221 failure mode'* ]]; then
        pass "the notify job still carries the loud #5221 message"
    else
        fail "the loud '#5221 failure mode' message is gone from the notify job" \
            "That text is the only thing that tells the room a node on the previous" \
            "release may be unable to reach this one. If it was removed to stop" \
            "false alarms, the fix is the environmental classification, not silence."
    fi

    quiet_cond="needs.attach-to-release.result == 'success' &&"
    quiet_cond+=" needs.auto-update-selfupdate-canary.outputs.classification == 'environmental'"

    # PER-STEP, and that is not fussiness. The first version of this scanned the
    # whole job block for "$quiet_cond", which the LOUD step satisfies all by
    # itself -- its condition is `!(<quiet_cond>)`, so the quiet condition is a
    # substring of it. Mutation-tested: replacing the quiet step's `if:` with a
    # bare `result == 'failure'`, so the reassuring message covers every red
    # run including a real #5221, left this suite fully GREEN. A pin whose
    # subject can be satisfied by the thing it is distinguishing from is not a
    # pin, which is the whole subject of #5303.
    #
    # Steps are split on their leading `- name:` / `- uses:` key, so nothing
    # here depends on step ORDER. How each one is then IDENTIFIED is stated at
    # the two `step_containing` calls below, and deliberately not restated
    # here: an earlier version of this comment said "identified by text from
    # their own message", which the very next commit made false, leaving two
    # comments fifteen lines apart contradicting each other. A reviewer reading
    # top-down would have recognised the anti-pattern and reported a defect
    # already fixed.
    notify_steps="$(printf '%s\n' "$notify_block" | awk '
        /^      - (name|uses):/ { n++ }
        { print n "\t" $0 }
    ')"
    step_containing() {
        # step_containing <needle> -- the whole step chunk holding <needle>,
        # whitespace-collapsed, or empty if no single step holds it.
        local needle="$1" n
        n="$(printf '%s\n' "$notify_steps" | grep -F -- "$needle" | head -1 | cut -f1)"
        [[ -n "$n" ]] || return 0
        printf '%s\n' "$notify_steps" | awk -F'\t' -v n="$n" '$1 == n { print $2 }' \
            | tr -s '[:space:]' ' '
    }

    # Located by `id:`, not by a phrase from the message. Locating them by
    # message text was tried and an ordinary reword of the quiet message broke
    # the pin on the first edit -- the same "expectation stored as a copy"
    # shape this PR exists to remove. An `id:` is structural and exists to be
    # referenced; a rename of one still fails here, but loudly and for a reason
    # the message names.
    quiet_step="$(step_containing 'id: notify-environmental')"
    loud_step="$(step_containing 'id: notify-fault')"

    if [[ -z "$quiet_step" ]]; then
        fail "the environmental (quiet) notification step is gone from the notify job" \
            "Without it every red Gate B run takes the loud #5221 branch again, so a" \
            "runner that could not reach github.com tells the room the fleet may be" \
            "stranded. That is the alarm-fatigue failure this split exists to stop."
    elif [[ "$quiet_step" == *"$quiet_cond"* && "$quiet_step" != *'!('* ]]; then
        pass "the quiet step requires a successful publish AND an environmental classification"
    else
        fail "the environmental notification's own condition changed" \
            "Its step must be conditioned on exactly:" \
            "  $quiet_cond" \
            "Widened, the reassuring 'nothing to worry about' message starts covering" \
            "real faults and a release stuck as a DRAFT. Its condition is currently:" \
            "  $quiet_step"
    fi

    if [[ -n "$loud_step" && "$loud_step" == *"!($quiet_cond)"* ]]; then
        pass "the loud step is the exact negation of the quiet one (unknown states stay loud)"
    else
        fail "the loud notification is no longer the exact negation of the quiet one" \
            "Written as its own list of results instead, any state neither branch" \
            "anticipated -- a cancelled job, an empty classification because the" \
            "step never ran -- falls through both and nothing is sent at all."
    fi
fi

# --- 6c. Gate B's step still classifies, and still re-raises its exit code ---
# The classification only reaches the notify job because the canary step writes
# it to GITHUB_OUTPUT and the job re-exports it. Two ways to break that
# silently: drop the job-level `outputs:` (every clause above reads '' and the
# loud branch fires for everything, which is merely noisy), or swallow the
# script's exit code so the job goes GREEN on a failed canary, which is not
# noisy at all.
# COMMENT-ONLY LINES ARE DROPPED, exactly as `JOB_BLOCK` drops them for
# `attach-to-release`, and for the reason this repo keeps rediscovering: a pin
# that scans raw text is satisfied by PROSE. This block's own comments quote
# `|| rc=$?` and `exit "$rc"` while explaining why they must stay, so the
# assertions below would have passed against the explanation after the code
# was removed. Found by mutation, and only because the mutation hit the comment
# first and the pin stayed green -- the harness accident that exposed it.
selfupdate_block="$(yaml_job_block auto-update-selfupdate-canary)"

# shellcheck disable=SC2016  # literal workflow text: the `${{ }}` must not expand
if [[ "$selfupdate_block" == *'classification: ${{ steps.canary.outputs.classification }}'* ]]; then
    pass "Gate B's job still exports the 'classification' output"
else
    fail "Gate B's job no longer exports 'classification'" \
        "The notify job reads it to choose between the quiet and the loud message." \
        "Missing, it evaluates to '' and every run takes the loud #5221 branch."
fi

# --- 6d. the environmental exit code is the SAME number on both sides -------
# The canary defines `EXIT_UNVERIFIED_ENVIRONMENTAL` and the workflow tests a
# LITERAL. They are one contract written in two files, and nothing made them
# agree. Change the constant and every environmental run silently takes the
# loud #5221 branch instead of the quiet one -- the direction is safe, which is
# exactly why it would go unnoticed. Read from the canary rather than restated
# here, so this test cannot be the thing that is stale.
canary_env_code="$(sed -n 's/^EXIT_UNVERIFIED_ENVIRONMENTAL=\([0-9]\{1,\}\).*/\1/p' \
    "$SCRIPT_DIR/auto-update-canary.sh" | head -1)"
if [[ -z "$canary_env_code" ]]; then
    fail "could not read EXIT_UNVERIFIED_ENVIRONMENTAL from auto-update-canary.sh" \
        "Without it this assertion would compare the workflow against an empty" \
        "string and pass on anything."
elif [[ "$selfupdate_block" == *"-eq $canary_env_code "* ]]; then
    pass "the workflow tests the same environmental exit code the canary returns ($canary_env_code)"
else
    fail "cross-compile.yml does not test exit $canary_env_code, the canary's EXIT_UNVERIFIED_ENVIRONMENTAL" \
        "The two are one contract in two files. If they disagree, every" \
        "environmental run is classified 'fault' and takes the loud #5221" \
        "branch -- safe, and therefore silent."
fi

# --- 6e. the step CAPTURES the exit status as well as re-raising it ---------
# SUPERSEDED AS A GUARANTEE by assertion 2g, and the reason is worth keeping.
# This pin requires that `|| rc=$?` exists AND that `exit "$rc"` exists. Both
# can be present and correct while the invariant BETWEEN them is broken by a
# third line -- inserting a bare `rc=0` immediately above the `exit` leaves both
# needles matching, contains nothing suspicious, and makes Gate B report success
# on a broken updater with the notify job never firing. A pin that asserts LINES
# EXIST cannot see an invariant that breaks between them; only executing the
# step can. 2g does exactly that, and kills the `rc=0` insertion.
# Kept for its specific, fast diagnostics when a line really is deleted.
# BOTH halves, because pinning only the second leaves the cheaper mutation wide
# open. Confirmed by mutation: change `|| rc=$?` to `|| true` and `rc` stays 0,
# `classification` is reported as `ok`, `exit "$rc"` exits 0 -- so Gate B goes
# GREEN on a genuinely broken updater, the job result is `success`, and the
# notify job never fires. The `exit "$rc"` pin sees nothing, because that line
# is untouched.
#
# Assertion 3a closes the same neutering route for GATE A only: it scans
# `$CANARY_STEP`, the preflight invocation inside `attach-to-release`. Gate B is
# a different top-level job and was never scanned.
#
# One directive for the whole compound command: shellcheck rejects a directive
# in front of an individual `elif` branch (SC1123).
# shellcheck disable=SC2016  # literal workflow text; `$rc` must not expand
if [[ "$selfupdate_block" != *'|| rc=$?'* ]]; then
    fail "Gate B's step no longer captures the canary's exit status into \$rc" \
        "It must run the canary as '... || rc=\$?' and re-raise at the end." \
        "Replaced with '|| true' the step exits 0 whatever the canary found:" \
        "Gate B is green on a broken updater and the notify job never fires," \
        "because the job result is 'success'. The 'exit \$rc' pin below cannot" \
        "see this -- that line is left untouched by the mutation."
elif [[ "$selfupdate_block" != *'exit "$rc"'* ]]; then
    fail "Gate B's step no longer re-raises the canary's exit status" \
        "It runs the canary with '|| rc=\$?' so it can classify exit 75. That is" \
        "only safe while the code is raised again at the end of the step. Without" \
        "the re-raise the step always exits 0, Gate B is green on a broken" \
        "updater, and nothing notifies -- the exact silent fail this file exists" \
        "to prevent, reached by deleting one line."
else
    # `allow-rc-capture`: Gate B's deliberate `|| rc=$?` is exempt, everything
    # else -- including `|| echo`, `|| exit 0` -- is not.
    swallowed_b="$(status_swallowers "$selfupdate_block" allow-rc-capture)"
    if [[ -z "$swallowed_b" ]]; then
        pass "Gate B's step captures AND re-raises the canary's exit code, and swallows nothing"
    else
        fail "Gate B's step swallows an exit status (any '||' other than '|| rc=\$?', 'set +e', trailing '; true')" \
            "$swallowed_b" \
            "Same route assertion 3a closes for Gate A: the step still runs and" \
            "still reports, but it can no longer fail."
    fi
fi

# --- 7. Gate B's job still exists -------------------------------------------
# The notify job's clauses are only meaningful if the job they name is real.
if grep -qE '^  auto-update-selfupdate-canary:[[:space:]]*$' "$WF"; then
    pass "cross-compile.yml still has the 'auto-update-selfupdate-canary' job (Gate B)"
else
    fail "the 'auto-update-selfupdate-canary' job (Gate B) is gone from cross-compile.yml" \
        "Gate B is what proves a node on the PREVIOUS release can actually reach" \
        "this one -- the #5221 failure mode. Nothing else covers it."
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release canary wiring assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
