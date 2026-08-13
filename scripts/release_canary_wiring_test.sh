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
JOB_BLOCK="$(awk '
    /^  attach-to-release:[[:space:]]*$/ { inblock = 1; print NR ":" $0; next }
    inblock && /^  [A-Za-z_.-]+:/        { inblock = 0 }
    inblock && $0 !~ /^[[:space:]]*#/    { print NR ":" $0 }
' "$WF")"

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

    # Same two neutering routes assertion 3 covers for the canary. A publish
    # step that cannot fail is not the problem here -- the problem is a publish
    # step that runs when the canary DIDN'T pass, which `if:` buys and
    # `continue-on-error` does not. Pin both anyway: `continue-on-error: true`
    # here would let a failed upload proceed to the un-draft, publishing a
    # release whose crates do not exist.
    PC_OVERRIDE="$(printf '%s\n' "$PUBLISH_CRATES_STEP" \
        | grep -E '^[0-9]+:        (if|continue-on-error):')"
    if [[ -z "$PC_OVERRIDE" ]]; then
        pass "the crates.io publish step has no 'if:' or 'continue-on-error:'"
    else
        fail "the crates.io publish step has acquired an 'if:' or 'continue-on-error:'" \
            "$(printf '%s\n' "$PC_OVERRIDE")" \
            "Steps run only after every earlier step succeeded, and that default is" \
            "what puts this publish downstream of the canary. An 'if:' can override" \
            "it; 'continue-on-error: true' lets a failed upload reach the un-draft."
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
    # extract_step_run <step name> -- that step's `run:` script, dedented.
    awk -v want="      - name: $1" '
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

PC_RUN="$(extract_step_run 'Publish crates to crates.io')"
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

    pc_run_case() {
        # pc_run_case <desc> <core-version> <tag> <crates.io-body> <want-rc> <want-cargo:yes|no>
        local desc="$1" core_ver="$2" tag="$3" api_body="$4" want_rc="$5" want_cargo="$6"
        local work rc got_cargo
        work="$(mktemp -d)"
        mkdir -p "$work/crates/core" "$work/crates/fdev" "$work/bin"
        printf '[package]\nname = "freenet"\nversion = "%s"\n' "$core_ver" \
            > "$work/crates/core/Cargo.toml"
        printf '[package]\nname = "fdev"\nversion = "0.9.9"\n' \
            > "$work/crates/fdev/Cargo.toml"
        : > "$work/cargo.log"
        # Unquoted heredocs: `$work` and `$api_body` expand, `\$*` and the `\n`
        # in the format string reach the stub literally.
        cat > "$work/bin/cargo" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/cargo.log"
EOF
        cat > "$work/bin/curl" <<EOF
#!/usr/bin/env bash
printf '%s' '$api_body'
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        chmod +x "$work/bin/cargo" "$work/bin/curl" "$work/bin/sleep"
        printf '%s\n' "$PC_RUN" > "$work/step.sh"

        ( cd "$work" && PATH="$work/bin:$PATH" GITHUB_REF="refs/tags/$tag" \
            bash "$work/step.sh" ) > "$work/out" 2>&1
        rc=$?
        if grep -qF 'publish -p freenet' "$work/cargo.log"; then
            got_cargo=yes
        else
            got_cargo=no
        fi

        local ok=1
        case "$want_rc" in
            0)      [[ "$rc" -eq 0 ]] || ok=0 ;;
            nonzero) [[ "$rc" -ne 0 ]] || ok=0 ;;
        esac
        [[ "$got_cargo" == "$want_cargo" ]] || ok=0

        if [[ "$ok" -eq 1 ]]; then
            pass "publish step: $desc (rc=$rc, cargo publish called: $got_cargo)"
        else
            fail "publish step: $desc" \
                "expected rc $want_rc and cargo-publish-called=$want_cargo," \
                "got rc=$rc and cargo-publish-called=$got_cargo" \
                "--- step output ---" \
                "$(head -20 "$work/out")"
        fi
        rm -rf "$work"
    }

    NOT_ON_CRATES_IO='{"errors":[{"detail":"Not Found"}]}'
    ON_CRATES_IO='{"version":{"num":"9.9.9"}}'

    # The guard itself: a tree whose version disagrees with the tag must stop
    # BEFORE anything reaches crates.io. `cargo publish called: no` is the
    # load-bearing half -- a refusal that happens after the upload is not one.
    pc_run_case "refuses a tree whose version does not match the tag" \
        "0.0.1" "v9.9.9" "$NOT_ON_CRATES_IO" nonzero no

    # Idempotent re-run (the recovery path the docs point at): the version is
    # already published, so nothing is uploaded twice and the job proceeds to
    # the un-draft.
    pc_run_case "skips a version already on crates.io, and still succeeds" \
        "9.9.9" "v9.9.9" "$ON_CRATES_IO" 0 no

    # ...and it is not just refusing everything: on the normal path it really
    # does publish. Without this case the two above are satisfied by a step
    # that never publishes at all.
    pc_run_case "publishes when the tree matches and the version is new" \
        "9.9.9" "v9.9.9" "$NOT_ON_CRATES_IO" 0 yes
fi

# --- 2c. release.yml must not publish for real -------------------------------
# The other half of the same invariant, and the one a well-meaning revert would
# reach for: re-adding `cargo publish -p freenet` to release.yml restores the
# old upstream-of-the-gate order even with everything above still green,
# because release.yml runs entirely before the tag that triggers this workflow.
#
# `--dry-run` is explicitly allowed and deliberately kept there: it is not
# irreversible and it catches a packaging break before a tag is burned.
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"
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
    SWALLOWED="$(printf '%s\n' "$CANARY_STEP" \
        | grep -cE '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')"
    if [[ "$SWALLOWED" -eq 0 ]]; then
        pass "the canary step's shell does not swallow its own exit status"
    else
        fail "the canary step swallows its own exit status ('|| true', '|| :' or 'set +e')" \
            "$(printf '%s\n' "$CANARY_STEP" | grep -E '\|\|[[:space:]]*(true|:)[[:space:]]*$|set[[:space:]]+\+e')" \
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
notify_block="$(awk '
    /^  notify-auto-update-canary-failure:[[:space:]]*$/ { inblock = 1; print; next }
    inblock && /^  [A-Za-z_.-]+:/                        { inblock = 0 }
    inblock                                              { print }
' "$WF")"

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
