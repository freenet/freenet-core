#!/usr/bin/env bash
# Behavioural tests for scripts/release.sh -- the MANUAL release driver.
#
# WHY THESE ARE BEHAVIOURAL AND NOT SOURCE PINS.
#
# release_canary_wiring_test.sh pins release.sh by pattern: assertion 2f checks
# that `^publish_crates$` appears after `^create_github_release$`, and the
# global scan checks that real `cargo publish` lines live inside
# `publish_crates()`. Both look at TEXT, and both were defeated by mutations
# that leave the text intact:
#
#   * Replace the fdev branch's condition with `[[ "$freenet_published" == "true" ]]`.
#     That variable is true in BOTH arms of the freenet block above, so the fdev
#     branch always takes the "already published" path and `cargo publish -p fdev`
#     becomes UNREACHABLE. The line is still present, so the source scan's floor
#     of two invocations is satisfied. fdev is then stranded on EVERY RELEASE --
#     not merely on a resume -- and every suite stayed green.
#
#   * Insert `if true; then publish_crates; fi` before `create_release_pr`,
#     leaving the real bare call in place. `^publish_crates$` still matches the
#     later line, so the ordering pin is satisfied, while the driver publishes
#     to crates.io before the tag exists and therefore before Gate A. That is
#     v0.2.124 reproduced on the human path, with the suite green.
#
# Neither is a regex that needs improving: one is an unreachable branch and the
# other is a second call site in a different call FORM. The property is "does
# fdev actually get published" and "does publishing happen after the tag", and
# the only way to assert a property is to run the thing.
#
# Functions are extracted verbatim from release.sh (`eval "$(awk ...)"`),
# matching release_state_restore_test.sh's idiom, so these tests cannot drift
# from the implementation.
#
# Run manually: bash scripts/release_driver_test.sh
# Wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE_SH="$SCRIPT_DIR/release.sh"

FAILURES=0
fail() {
    echo "FAIL - $1" >&2
    shift
    for line in "$@"; do echo "       $line" >&2; done
    FAILURES=$((FAILURES + 1))
}
pass() { echo "ok   - $1"; }

# code_lines <file> -- `NNN:code` per line, with UNQUOTED trailing comments
# stripped and comment-only lines dropped.
#
# EVERY source scan in this file goes through this. Two did not, and one of them
# was defeated by exactly the trick this exists to stop: appending
# `  # bare, refusal swallowed` to a bare `publish_draft_release` call left the
# whole suite green, including the line "no bare 'publish_draft_release' call;
# every site propagates its refusal" -- which was then vacuously false while the
# fail-open shape it exists to catch sat in the file.
#
# release_canary_wiring_test.sh already had a `logical_lines` helper built for
# this failure mode, used by its gh-release and User-Agent scans. This file's
# scans did not use one. That is the fixed-one-site-left-the-sibling-armed
# pattern again, this time against a helper already written for the job -- so
# the helper is duplicated here deliberately (these suites are independently
# runnable and neither sources the other) and every scan is routed through it.
#
# QUOTE-AWARE, so `--notes "fixes #5288"` is not truncated: a naive cut would
# silently shorten real commands and make the scans miss what came after.
code_lines() {
    awk '
        function strip_comment(str,   i, c, q, out) {
            q = ""; out = ""
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
            out = strip_comment($0)
            sub(/[[:space:]]+$/, "", out)
            if (out !~ /^[[:space:]]*$/) print NR ":" out
        }
    ' "$1"
}

if [[ ! -f "$RELEASE_SH" ]]; then
    echo "FAIL: $RELEASE_SH not found" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. publish_crates actually publishes each crate that is missing
# ---------------------------------------------------------------------------
PUBLISH_FN="$(awk '/^publish_crates\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ -z "$PUBLISH_FN" ]] || [[ "$PUBLISH_FN" != *"cargo publish -p fdev"* ]]; then
    fail "could not extract publish_crates() from release.sh" \
        "Every case below runs it, so an empty or wrong extraction would make" \
        "all of them pass vacuously."
else
    # run_case <desc> <freenet-on-crates-io> <fdev-on-crates-io> <expected>
    #   expected: none | freenet | fdev | both
    publish_case() {
        local desc="$1" fup="$2" dup="$3" want="$4"
        local work got_f got_d got
        work="$(mktemp -d)"
        mkdir -p "$work/bin"
        # `cargo` logs its arguments; nothing is uploaded.
        cat > "$work/bin/cargo" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/cargo.log"
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        chmod +x "$work/bin/cargo" "$work/bin/sleep"
        : > "$work/cargo.log"

        # Everything in this subshell is consumed by the eval'd publish_crates,
        # so shellcheck cannot see the uses: the variables look unset-and-unused
        # (SC2034) and the stubs look unreachable (SC2317). Both are the point --
        # the function under test is injected at runtime.
        # shellcheck disable=SC2030,SC2034,SC2317
        (
            PATH="$work/bin:$PATH"
            VERSION=9.9.9
            FDEV_VERSION=0.9.9
            DRY_RUN=false
            FREENET_UP="$fup"
            FDEV_UP="$dup"
            # Collaborators stubbed; only publish_crates is under test.
            is_step_completed() { return 1; }
            mark_completed() { :; }
            run_cmd() { shift; "$@"; }
            crate_version_on_crates_io() {
                case "$1" in
                    freenet) [[ "$FREENET_UP" == yes ]] ;;
                    fdev)    [[ "$FDEV_UP"    == yes ]] ;;
                    *)       return 1 ;;
                esac
            }
            eval "$PUBLISH_FN"
            publish_crates
        ) > "$work/out" 2>&1

        # A REAL publish, not merely an invocation mentioning the crate.
        # `--dry-run` is excluded deliberately: with a plain substring match,
        # appending `--dry-run` to both invocations left this file reporting
        # "publishes both" while nothing would ever be uploaded. The property
        # is claimed as "actually publishes"; without this it is only
        # "mentions". Same fix, same reason, as the publish-step fixtures in
        # release_canary_wiring_test.sh.
        got_f=0; got_d=0
        [[ "$(grep -cE '(^| )publish +-p +freenet( |$)' "$work/cargo.log")" -gt 0 ]] \
            && [[ "$(grep -cE '(^| )publish +-p +freenet .*--dry-run' "$work/cargo.log")" -eq 0 ]] \
            && got_f=1
        [[ "$(grep -cE '(^| )publish +-p +fdev( |$)' "$work/cargo.log")" -gt 0 ]] \
            && [[ "$(grep -cE '(^| )publish +-p +fdev .*--dry-run' "$work/cargo.log")" -eq 0 ]] \
            && got_d=1
        case "$got_f$got_d" in
            00) got=none ;; 10) got=freenet ;; 01) got=fdev ;; 11) got=both ;;
        esac

        if [[ "$got" == "$want" ]]; then
            pass "publish_crates: $desc (published: $got)"
        else
            fail "publish_crates: $desc" \
                "expected to publish: $want" \
                "actually published:  $got" \
                "--- output ---" \
                "$(head -20 "$work/out")"
        fi
        rm -rf "$work"
    }

    # THE case M12 defeats: with nothing on crates.io, BOTH must be published.
    # An unreachable fdev branch shows up here as `freenet` instead of `both`.
    publish_case "publishes both when neither is on crates.io" no  no  both
    # The partial-publish resume, which is the documented reason this function
    # is per-crate at all.
    publish_case "publishes only fdev when freenet is already up" yes no  fdev
    # Mirror, so the dispatch cannot pass by accident of check order.
    publish_case "publishes only freenet when fdev is already up" no  yes freenet
    # Full no-op, so the cases above cannot be satisfied by a function that
    # publishes unconditionally.
    publish_case "publishes neither when both are already up"     yes yes none
fi

# ---------------------------------------------------------------------------
# 2. the main flow publishes AFTER the tag and the gate
# ---------------------------------------------------------------------------
# The whole main-flow region is executed with every release.sh function stubbed
# to announce itself, and the resulting ORDER is asserted. Taking the region as
# "everything after the final function definition" rather than a fixed range
# means a call inserted anywhere in it is still observed -- including one in a
# form no `^name$` pattern would match, which is exactly the mutation this
# replaces a source pin for.
LAST_BRACE="$(grep -n '^}' "$RELEASE_SH" | tail -1 | cut -d: -f1)"
if [[ -z "$LAST_BRACE" ]]; then
    fail "could not find the end of the last function definition in release.sh" \
        "The main-flow extraction below depends on it."
else
    # Exported to the reachability case below, which reuses the same extraction
    # rather than re-deriving it (two extractions of one thing drift apart).
    MAIN_FLOW="$(tail -n "+$((LAST_BRACE + 1))" "$RELEASE_SH")"
    FN_NAMES="$(grep -oE '^[a-z_][a-z_0-9]*\(\) \{' "$RELEASE_SH" | sed 's/() {//')"
    if [[ -z "$MAIN_FLOW" || -z "$FN_NAMES" ]]; then
        fail "could not extract release.sh's main flow or its function names" \
            "The ordering assertion below would pass having observed nothing."
    else
        work="$(mktemp -d)"
        {
            # Every function announces itself and does nothing else.
            while IFS= read -r fn; do
                [[ -z "$fn" ]] && continue
                printf '%s() { echo "CALL:%s"; }\n' "$fn" "$fn"
            done <<< "$FN_NAMES"
            # Variables the main flow prints; values are irrelevant.
            printf 'VERSION=9.9.9\nFDEV_VERSION=0.9.9\nSTATE_FILE=/dev/null\n'
            printf '%s\n' "$MAIN_FLOW"
        } > "$work/flow.sh"

        ORDER="$(bash "$work/flow.sh" 2>/dev/null | sed -n 's/^CALL://p')"
        idx_of() { printf '%s\n' "$ORDER" | grep -nxF "$1" | head -1 | cut -d: -f1; }

        I_PUBLISH="$(idx_of publish_crates)"
        I_CREATE="$(idx_of create_github_release)"
        I_WAIT="$(idx_of wait_for_binaries)"

        if [[ -z "$I_PUBLISH" || -z "$I_CREATE" || -z "$I_WAIT" ]]; then
            fail "release.sh's main flow did not call one of the ordered steps" \
                "publish_crates=${I_PUBLISH:-<never>} create_github_release=${I_CREATE:-<never>} wait_for_binaries=${I_WAIT:-<never>}" \
                "Observed call order:" \
                "$(printf '%s\n' "$ORDER" | tr '\n' ' ')" \
                "A step that is never called cannot be ordered, and this assertion" \
                "must not report success on a flow that skips one."
        elif [[ "$I_PUBLISH" -gt "$I_CREATE" && "$I_PUBLISH" -gt "$I_WAIT" ]]; then
            pass "release.sh publishes after the tag and the gate (create $I_CREATE < wait $I_WAIT < publish $I_PUBLISH)"
        else
            fail "release.sh's FIRST crates.io publish happens before the gate" \
                "call order: $(printf '%s\n' "$ORDER" | tr '\n' ' ')" \
                "create_github_release=$I_CREATE wait_for_binaries=$I_WAIT publish_crates=$I_PUBLISH" \
                "create_github_release pushes the tag that triggers cross-compile.yml," \
                "and wait_for_binaries blocks until that workflow -- including Gate A --" \
                "has run. Publishing before either uploads to crates.io before the gate" \
                "can block, which is what cost v0.2.124 its version number." \
                "This observes the FIRST call in the MAIN FLOW, so an earlier call there" \
                "in any form -- conditional, eval, variable, subshell -- fails here even" \
                "when the original bare call is untouched. Calls from inside a FUNCTION" \
                "body are covered separately below, because every function is stubbed" \
                "here and so a call inside one never executes."
        fi
        rm -rf "$work"
    fi
fi

# ---------------------------------------------------------------------------
# 3. the driver REFUSES to publish when the workflow path failed
# ---------------------------------------------------------------------------
# THIS ASSERTION WAS PREVIOUSLY THE EXACT OPPOSITE, and the inversion is the
# most important thing in this file.
#
# It asserted that `publish_crates` still RUNS when `wait_for_binaries` fails,
# on the reasoning that the fallback was a harmless confirmation and a backstop
# for a `CARGO_REGISTRY_TOKEN` that never reached CI. Walking the failure modes
# shows publishing is wrong on all but one of them.
#
# THE ENUMERATION LIVES IN ONE PLACE: the refusal branch in `scripts/release.sh`,
# which lists every mode with its Gate A implication. This comment deliberately
# does NOT restate it.
#
# It used to. That copy said "five failure paths" and listed five rows, omitting
# the refusal inherited from `publish_draft_release` -- the exact sixth member,
# and the exact undercount already corrected twice in release.sh. Three copies
# of one list produced three different versions of it, and this one sat in the
# file that release.sh points a reader at as the behavioural pin, so following
# that pointer landed you on the wrong count.
#
# The assertions below never depended on the number, which is why it drifted
# unnoticed. Restating a list is how copies diverge; the fix is to stop having
# copies, not to keep fixing them.
#
# `publish_crates` consults only the resume flag, DRY_RUN and crates.io
# presence -- never the gate's verdict. And on the rejected path the crate is
# genuinely absent (cross-compile.yml runs the canary before its publish step),
# so the already-published check says "no" and the upload really happens.
#
# The backstop justification was also unachievable: the credential check runs
# BEFORE the canary in cross-compile.yml, so a missing token cannot reach this
# branch -- and here missing-token and canary-rejected are indistinguishable,
# both arriving as the same non-success conclusion.
#
# The general lesson, and it is why this file now asserts the opposite: the
# fallback was DEAD (bare call under `set -e`) and making it live removed the
# accident that was preventing an unsafe publish. Before making dead code
# reachable, ask what its being dead was protecting.
if [[ -z "${MAIN_FLOW:-}" || -z "${FN_NAMES:-}" ]]; then
    fail "main flow not extracted; the refusal case cannot run" \
        "It would otherwise report success having executed nothing."
else
    refuse_case() {  # refuse_case <desc> <wfb-rc> <want-publish: yes|no> <want-exit: zero|nonzero>
        local desc="$1" wfb_rc="$2" want_pub="$3" want_exit="$4"
        local work rc got_pub got_exit
        work="$(mktemp -d)"
        {
            while IFS= read -r fn; do
                [[ -z "$fn" ]] && continue
                if [[ "$fn" == "wait_for_binaries" ]]; then
                    printf '%s() { echo "CALL:%s"; return %s; }\n' "$fn" "$fn" "$wfb_rc"
                else
                    printf '%s() { echo "CALL:%s"; }\n' "$fn" "$fn"
                fi
            done <<< "$FN_NAMES"
            printf 'VERSION=9.9.9\nFDEV_VERSION=0.9.9\nSTATE_FILE=/dev/null\n'
            printf 'PROJECT_ROOT=/nonexistent\n'
            # `set -e` only; errexit is the mechanism, `-u` would abort on
            # variables the extraction did not carry.
            printf 'set -e\n'
            printf '%s\n' "$MAIN_FLOW"
        } > "$work/flow.sh"

        bash "$work/flow.sh" > "$work/out" 2>&1
        rc=$?
        if grep -qxF 'CALL:publish_crates' "$work/out"; then got_pub=yes; else got_pub=no; fi
        if [[ "$rc" -eq 0 ]]; then got_exit=zero; else got_exit=nonzero; fi

        if [[ "$got_pub" == "$want_pub" && "$got_exit" == "$want_exit" ]]; then
            pass "driver: $desc (published=$got_pub, exit=$got_exit)"
        else
            fail "driver: $desc" \
                "wait_for_binaries returned $wfb_rc" \
                "expected published=$want_pub exit=$want_exit; got published=$got_pub exit=$got_exit" \
                "Publishing on a failed workflow path uploads a version Gate A never" \
                "passed -- on every failure mode except one, including the one where" \
                "the canary REJECTED the binary. That permanently spends the version" \
                "number. The branch must refuse and print the recovery procedure." \
                "--- output ---" \
                "$(head -20 "$work/out")"
        fi
        rm -rf "$work"
    }

    # THE property: a failed workflow path must NOT publish, and must fail loudly.
    refuse_case "refuses to publish when wait_for_binaries fails" 1 no nonzero
    # The control, so the case above cannot be satisfied by a driver that never
    # publishes at all.
    refuse_case "still publishes on the success path" 0 yes zero
fi

# --- 3b. every publish_draft_release call site is guarded -------------------
# A source scan, by necessity. Case 4 below drives the REAL `wait_for_binaries`
# but only reaches its FIRST `publish_draft_release` call site -- the
# "binaries already available" branch. The second is inside the polling loop and
# would need a stubbed `gh`, a run id and several poll iterations to reach;
# mutation-testing confirmed the behavioural case does not cover it (reverting
# that site alone stayed green).
#
# Rather than leave the sibling unprotected -- the exact fix-the-named-site-only
# habit this PR keeps finding -- both sites are pinned structurally: a call to
# `publish_draft_release` must be guarded, never bare, because bare calls have
# their failure swallowed when `wait_for_binaries` is invoked from an `if !`
# condition.
PDR_BARE="$(code_lines "$RELEASE_SH" | grep -E '^[0-9]+:[[:space:]]*publish_draft_release[[:space:]]*$')"
if [[ -z "$PDR_BARE" ]]; then
    PDR_CALLS="$(code_lines "$RELEASE_SH" | grep -cE 'publish_draft_release')"
    # Each guard must also RETURN NONZERO. "Guarded" is syntax; "propagates" is
    # the property, and they are not the same: changing a guard's body from
    # `return 1` to an `echo` leaves the call guarded, the refusal swallowed,
    # and a syntax-only scan green. That mutation IS caught at site 1 by the
    # behavioural case below; this closes it at site 2, which the behavioural
    # case cannot reach (it lives in the polling loop, past a stubbed `gh`, a
    # run id and several iterations).
    #
    # So the two sites are protected UNEQUALLY and that is stated rather than
    # glossed: site 1 behaviourally, site 2 by syntax plus this shape check.
    # "Pinned structurally" was a stronger claim than the earlier scan
    # delivered.
    PDR_GUARDS="$(code_lines "$RELEASE_SH" | grep -cE 'if ! publish_draft_release; then')"
    PDR_BAD_BODY=""
    while IFS= read -r _g; do
        [[ -z "$_g" ]] && continue
        _gl="${_g%%:*}"
        # WINDOW INCLUDES THE GUARD LINE ITSELF (`>= a`, not `> a`), because the
        # semantically-identical one-liner
        # `if ! publish_draft_release; then return 1; fi` puts the return ON that
        # line. Excluding it REJECTED a correct fix -- and a lint that fails on
        # correct code is one that gets deleted, which is the reasoning applied
        # to the orphan gate's false-block and now owed to this check.
        #
        # MATCHED IN COMMAND POSITION, not as a substring. `*"return 1"*`
        # accepted `echo "would return 1 here"`, so a guard whose body only
        # TALKS about returning passed -- at site 2 that took the whole suite
        # green with the fail-open shape in place, the one spot this file
        # claimed coverage it did not have. `return` now has to follow
        # start-of-line, `;`, `then`, `&&`, `||` or `{`.
        _body="$(code_lines "$RELEASE_SH" \
            | awk -F: -v a="$_gl" '$1 >= a && $1 <= a + 4')"
        if printf '%s\n' "$_body" \
            | grep -qE '(^[0-9]+:[[:space:]]*|;[[:space:]]*|then[[:space:]]+|&&[[:space:]]*|\|\|[[:space:]]*|\{[[:space:]]*)return[[:space:]]+[1-9]'
        then
            :
        else
            PDR_BAD_BODY+="line $_gl: guard body does not return nonzero"$'\n'
        fi
    done < <(code_lines "$RELEASE_SH" | grep -E 'if ! publish_draft_release; then')

    if [[ "$PDR_CALLS" -lt 3 ]]; then
        fail "expected publish_draft_release's definition plus at least 2 call sites, found $PDR_CALLS references" \
            "The bare-call scan above would pass having nothing to examine."
    elif [[ "$PDR_GUARDS" -lt 2 ]]; then
        fail "expected 2 guarded publish_draft_release call sites, found $PDR_GUARDS" \
            "Both call sites must be guarded; a bare one has its refusal swallowed."
    elif [[ -n "$PDR_BAD_BODY" ]]; then
        fail "a publish_draft_release guard does not propagate the refusal" \
            "$(printf '%s' "$PDR_BAD_BODY")" \
            "The call being guarded is not enough -- the guard body must return" \
            "nonzero. An 'echo' there leaves wait_for_binaries reporting success" \
            "for an unpublished draft, which is the failure this guard exists to" \
            "prevent."
    else
        pass "both publish_draft_release call sites are guarded AND return nonzero"
    fi
else
    fail "publish_draft_release is called BARE, so its refusal can be swallowed" \
        "$PDR_BARE" \
        "Bash suspends errexit for the whole dynamic extent of a command in an" \
        "'if !' condition, including the callee's body. wait_for_binaries is called" \
        "that way, so a bare failing call here falls through to the adjacent" \
        "'return 0' and reports SUCCESS for an unpublished draft." \
        "Use 'if ! publish_draft_release; then return 1; fi'."
fi

# ---------------------------------------------------------------------------
# 4. wait_for_binaries propagates a refusal INHERITED from publish_draft_release
# ---------------------------------------------------------------------------
# Runs the REAL `wait_for_binaries` -- not a stub -- with only
# `publish_draft_release` replaced. Case 2 above cannot express this: it stubs
# every function including `wait_for_binaries` itself, so the real body never
# executes. That harness is scoped to main-flow reachability and does exactly
# what it claims; this is the interaction it structurally cannot see, and the
# two are kept separate rather than one being stretched to cover both.
#
# WHAT BROKE, and why it was invisible. Guarding the main-flow call as
# `if ! wait_for_binaries` fixed an unreachable fallback -- and re-armed a worse
# bug, because bash suspends errexit for the entire DYNAMIC EXTENT of a command
# in an `if`/`!` condition, including the whole body of the callee. Inside
# `wait_for_binaries`, `publish_draft_release` was called BARE and immediately
# followed by `return 0`. So its two deliberate refusals -- draft state unknown,
# and still-a-draft with the Gate A canary not concluded -- stopped aborting,
# fell through to that `return 0`, and `wait_for_binaries` reported SUCCESS.
# The driver then published crates, updated gateways and announced to Matrix and
# River, for a release still sitting as an unpublished draft, possibly one the
# blocking canary had rejected.
#
# Fail-CLOSED became fail-OPEN, which is strictly worse than the bug the guard
# fixed. `publish_draft_release`'s own comments name that outcome twice as the
# thing its `return 1`s exist to prevent; those returns had become `return 0`.
#
# NOTE ON THE TEST SHAPE: each mode is invoked as a TOP-LEVEL `if !` statement.
# Wrapping the call in `|| echo` (or any other errexit-suspending context) makes
# both modes behave identically and the test vacuous -- the same suspension
# being tested would be doing the confounding.
WFB_FN="$(awk '/^wait_for_binaries\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ -z "$WFB_FN" ]] || [[ "$WFB_FN" != *"publish_draft_release"* ]]; then
    fail "could not extract wait_for_binaries() from release.sh" \
        "The cases below run it, so an empty extraction would pass vacuously."
else
    inherit_case() {  # inherit_case <desc> <publish_draft_release-rc> <want: fires|passes>
        local desc="$1" pdr_rc="$2" want="$3" work rc got
        work="$(mktemp -d)"
        {
            printf 'set -e\n'
            printf 'DRY_RUN=false\n'
            # Return 0 so the real function reaches its publish_draft_release
            # call on the "binaries already available" path.
            printf 'verify_required_binaries() { return 0; }\n'
            printf 'publish_draft_release() { echo "PDR:%s"; return %s; }\n' "$pdr_rc" "$pdr_rc"
            printf '%s\n' "$WFB_FN"
            # TOP-LEVEL guard, mirroring the real main flow. Not `|| ...`.
            printf 'if ! wait_for_binaries; then echo "GUARD_FIRED"; exit 7; fi\n'
            printf 'echo "NO_GUARD"\nexit 0\n'
        } > "$work/t.sh"

        bash "$work/t.sh" > "$work/out" 2>&1
        rc=$?
        if [[ "$rc" -eq 7 ]]; then got=fires; else got=passes; fi

        if [[ "$got" == "$want" ]]; then
            pass "wait_for_binaries: publish_draft_release returns $pdr_rc -> guard $got ($desc)"
        else
            fail "wait_for_binaries: $desc" \
                "publish_draft_release returned $pdr_rc; expected the guard to $want, it $got (exit $rc)" \
                "--- output ---" \
                "$(head -20 "$work/out")" \
                "A refusal from publish_draft_release MUST reach the caller. Swallowed," \
                "wait_for_binaries reports success for an unpublished draft and the" \
                "driver announces a release that never shipped. Guard the inner call" \
                "('if ! publish_draft_release; then return 1; fi'); a bare call is" \
                "not enough, because errexit is suspended for the whole callee when" \
                "wait_for_binaries is itself invoked from an 'if !' condition."
        fi
        rm -rf "$work"
    }

    # THE regression: the refusal must propagate.
    inherit_case "refusal propagates to the caller" 1 fires
    # The control, so the case above cannot be satisfied by a function that
    # always fails.
    inherit_case "success is not turned into a failure" 0 passes
fi

# ---------------------------------------------------------------------------
# 5. no FUNCTION BODY calls publish_crates
# ---------------------------------------------------------------------------
# The blind spot of the ordering test above, stated plainly rather than papered
# over. That test stubs every function to announce itself, so a `publish_crates`
# call INSIDE `update_versions()` never executes and is never observed -- while
# the later bare call still satisfies the ordering property. Measured: the
# driver publishes before the PR, the tag and Gate A, with 5 ok / 0 FAIL.
#
# Execution cannot see it (the body never runs) and the ordering property is not
# violated (the first OBSERVED call is still late), so this one is a source scan
# by necessity, not by preference. It is narrow and exact: `publish_crates` may
# be called from the main flow only.
#
# Comment lines are stripped so the prose above `publish_crates` in release.sh --
# which discusses the call at length -- cannot satisfy or trip this.
LAST_BRACE_FN="$(grep -n '^}' "$RELEASE_SH" | tail -1 | cut -d: -f1)"
if [[ -z "$LAST_BRACE_FN" ]]; then
    fail "could not locate the end of release.sh's function definitions" \
        "The function-body scan below would examine nothing."
else
    # Also via code_lines. It only stripped FULL-LINE comments before, so a
    # trailing `# ... publish_crates ...` in prose would have false-positived --
    # the opposite direction from the bare-call scan's hole, and the direction
    # that gets a check deleted for crying wolf.
    BODY_CALLS="$(code_lines "$RELEASE_SH" \
        | awk -F: -v last="$LAST_BRACE_FN" '$1 <= last' \
        | grep -E '(^[0-9]+:|[^#[:alnum:]_])publish_crates([[:space:]]|$|\))' \
        | grep -vE '^[0-9]+:publish_crates\(\) \{' \
        | grep -vE 'echo[^;|&$`]*publish_crates')"
    if [[ -z "$BODY_CALLS" ]]; then
        pass "no function body in release.sh calls publish_crates (only the main flow does)"
    else
        fail "a function body in release.sh calls publish_crates" \
            "$BODY_CALLS" \
            "The ordering test above stubs every function, so a call from inside one" \
            "never executes and is never observed -- while the later bare call keeps" \
            "the ordering property satisfied. A publish reached that way runs at" \
            "whatever point that function is called, which for update_versions() is" \
            "before the PR, the tag and Gate A." \
            "publish_crates belongs in the main flow only."
    fi
fi

# ---------------------------------------------------------------------------
# N. The handshake floor ships as committed (#5833)
# ---------------------------------------------------------------------------
# release.sh used to default --min-compatible to the version before the bump
# and write it into the bump, which would make every updated peer refuse every
# peer that had not updated yet: a network split on an ordinary release. Now it
# never changes the floor, refuses --min-compatible, refuses a committed floor
# it cannot read (build.rs would fall back to the package version), and clears
# FREENET_MIN_COMPATIBLE_VERSION so a value in the caller's shell cannot reach
# the builds.
FLOOR_FN="$(awk '/^current_min_compatible\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ "$FLOOR_FN" != *"min-compatible-version"* ]]; then
    fail "could not extract current_min_compatible() from release.sh" \
        "Every floor case below runs it, so an empty extraction would make them" \
        "all pass vacuously."
else
    eval "$FLOOR_FN"
    # floor_case <desc> <Cargo.toml body> <expected output or FAIL>
    floor_case() {
        local desc="$1" body="$2" want="$3" work got
        work="$(mktemp -d)"
        printf '%s\n' "$body" > "$work/Cargo.toml"
        if got="$(current_min_compatible "$work/Cargo.toml")"; then :; else got="FAIL"; fi
        rm -rf "$work"
        if [[ "$got" == "$want" ]]; then
            pass "$desc"
        else
            fail "$desc" "got '$got', expected '$want'"
        fi
    }
    floor_case "the floor read is min-compatible-version, not the package version" \
        $'[package]\nname = "freenet"\nversion = "0.2.200"\n\n[package.metadata.freenet]\nmin-compatible-version = "0.2.122"' \
        "0.2.122"
    floor_case "a floor written without spaces around '=' is read" \
        $'[package]\nversion = "0.2.200"\n[package.metadata.freenet]\nmin-compatible-version="0.2.150"' "0.2.150"
    floor_case "a missing min-compatible-version is refused" \
        $'[package]\nname = "freenet"\nversion = "0.2.200"' "FAIL"
    floor_case "a malformed min-compatible-version is refused" \
        $'[package]\nversion = "0.2.200"\n[package.metadata.freenet]\nmin-compatible-version = "next"' "FAIL"

    REAL_TOML="$SCRIPT_DIR/../crates/core/Cargo.toml"
    if got="$(current_min_compatible "$REAL_TOML")"; then
        pass "the real crates/core/Cargo.toml has a readable floor ($got)"
    else
        fail "the real crates/core/Cargo.toml has no readable min-compatible-version" \
            "Every release.sh run would refuse."
    fi
fi

# The call site, run for real: release.sh is copied into a throwaway repo with
# a stub `cargo`. The first thing the driver does after the floor check is
# `cargo search`, with stderr discarded, so the stub records that it was
# reached, and what FREENET_MIN_COMPATIBLE_VERSION it saw, in a file.
# floor_run_case <desc> <expect: refuse|proceed> <error text or ""> <Cargo.toml body> <args...>
floor_run_case() {
    local desc="$1" expect="$2" want_err="$3" toml_body="$4" out rc work reached seen
    shift 4
    work="$(mktemp -d)"
    mkdir -p "$work/scripts" "$work/crates/core" "$work/bin"
    cp "$RELEASE_SH" "$work/scripts/release.sh"
    printf '%s\n' "$toml_body" > "$work/crates/core/Cargo.toml"
    printf '#!/usr/bin/env bash\nprintf "%%s\\n" "${FREENET_MIN_COMPATIBLE_VERSION-<unset>}" > "%s/reached_cargo"\nexit 1\n' \
        "$work" > "$work/bin/cargo"
    chmod +x "$work/bin/cargo"
    (cd "$work" && GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 git init -q .)
    rc=0
    out="$(cd "$work" && PATH="$work/bin:$PATH" GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \
        FREENET_MIN_COMPATIBLE_VERSION=0.0.1 bash scripts/release.sh "$@" 2>&1)" || rc=$?
    reached=no
    seen=""
    if [[ -e "$work/reached_cargo" ]]; then
        reached=yes
        seen="$(cat "$work/reached_cargo")"
    fi
    rm -rf "$work"
    case "$expect" in
        refuse)
            if [[ "$rc" -ne 0 ]] && [[ "$reached" == no ]] && [[ "$out" == *"$want_err"* ]]; then
                pass "$desc"
            else
                fail "$desc" "exit=$rc, reached cargo=$reached, wanted '$want_err'" "$out"
            fi
            ;;
        proceed)
            # A shell value of the override must not reach the build steps.
            if [[ "$reached" == yes ]] && [[ "$seen" == "<unset>" ]]; then
                pass "$desc"
            else
                fail "$desc" "reached cargo=$reached, FREENET_MIN_COMPATIBLE_VERSION seen='$seen'" "$out"
            fi
            ;;
    esac
}
TOML_FLOOR=$'[package]\nname = "freenet"\nversion = "0.2.200"\n[package.metadata.freenet]\nmin-compatible-version = "0.2.122"'
floor_run_case "release.sh proceeds past the floor check with a committed floor, override cleared" \
    proceed "" "$TOML_FLOOR" --version 0.2.201 --dry-run
floor_run_case "release.sh refuses before any work when Cargo.toml has no floor" \
    refuse "Error: could not read an X.Y.Z min-compatible-version" \
    $'[package]\nname = "freenet"\nversion = "0.2.200"' --version 0.2.201 --dry-run
floor_run_case "release.sh refuses --min-compatible before any work" \
    refuse "Error: --min-compatible is no longer supported" \
    "$TOML_FLOOR" --version 0.2.201 --min-compatible 0.2.165 --dry-run
floor_run_case "release.sh refuses an empty --min-compatible too" \
    refuse "Error: --min-compatible is no longer supported" \
    "$TOML_FLOOR" --version 0.2.201 --dry-run --min-compatible ""
floor_run_case "release.sh refuses --min-compatible given without a value" \
    refuse "Error: --min-compatible is no longer supported" \
    "$TOML_FLOOR" --version 0.2.201 --dry-run --min-compatible
floor_run_case "release.sh refuses --min-compatible=X.Y.Z with the same message" \
    refuse "Error: --min-compatible is no longer supported" \
    "$TOML_FLOOR" --version 0.2.201 --dry-run --min-compatible=0.2.165

# The runs above stop at the first cargo call, and update_versions cannot run
# on its own (it defines its helpers inline and edits the tree), so these two
# read the code, by allow-list rather than by the tool a writer would use:
#   - min-compatible-version may appear only in the reader's grep and in
#     echo lines whose whole argument is one quoted string, so a sed,
#     perl, awk, toml-tool or `echo > file` rewrite anywhere fails. These are
#     tripwires for mistakes, not proof against deliberate evasion;
#   - FREENET_MIN_COMPATIBLE_VERSION may appear only in the one `unset`, so
#     an export or assignment added anywhere later fails.
FLOOR_LINES="$(code_lines "$RELEASE_SH" \
    | grep -F 'min-compatible-version' \
    | grep -vE '^[0-9]+:[[:space:]]*echo "[^"]*"$' \
    | grep -vF "value=\$(grep -E '^min-compatible-version[[:space:]]*=' \"\$1\"" || true)"
if [[ -z "$FLOOR_LINES" ]]; then
    pass "release.sh never rewrites min-compatible-version (only the reader and messages name it)"
else
    fail "release.sh names min-compatible-version outside the reader and messages" "$FLOOR_LINES" \
        "Raising the floor is its own reviewed PR (docs/RELEASING.md, #5833)."
fi
ENV_LINES="$(code_lines "$RELEASE_SH" \
    | grep -F 'FREENET_MIN_COMPATIBLE_VERSION' \
    | grep -vE '^[0-9]+:unset FREENET_MIN_COMPATIBLE_VERSION$' || true)"
ENV_UNSETS="$(code_lines "$RELEASE_SH" | grep -cE '^[0-9]+:unset FREENET_MIN_COMPATIBLE_VERSION$' || true)"
if [[ -z "$ENV_LINES" ]] && [[ "$ENV_UNSETS" -eq 1 ]]; then
    pass "FREENET_MIN_COMPATIBLE_VERSION is only ever unset by release.sh"
else
    fail "release.sh sets FREENET_MIN_COMPATIBLE_VERSION, or does not unset it exactly once" \
        "unset lines: $ENV_UNSETS" "${ENV_LINES:-}"
fi

# --- create_github_release tags the bump PR's MERGE COMMIT --------------------
#
# It used to tag local HEAD: after `git pull` that includes anything merged
# since the bump (#5233), and after a PR-wait timeout it was the UNMERGED
# release branch. Run the real function against a stub gh/git and check which
# commit `git tag -a` names, and that a stale tag elsewhere refuses.
TAG_FN="$(awk '/^create_github_release\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ -z "$TAG_FN" ]] || [[ "$TAG_FN" != *"git tag -a"* ]]; then
    fail "could not extract create_github_release() from release.sh"
else
    MERGE_SHA=1111111111111111111111111111111111111111
    # tag_case <desc> <local tag sha or ""> <remote tag sha or ""> <Cargo version> <expected>
    #          [ls-remote output] [gh pr list output]
    #   expected: "tag+push" | "push" | "skip" | "refuse"
    # The two optional arguments replace the stubs' default answers: the
    # ls-remote lines built from <remote tag sha>, and the merge SHA.
    tag_case() {
        local desc="$1" local_sha="$2" remote_sha="$3" cargo_ver="$4" want="$5"
        local work got rc=0
        work="$(mktemp -d)"
        mkdir -p "$work/bin"
        if [[ $# -ge 6 ]]; then
            printf '%b' "$6" > "$work/lsremote"
        elif [[ -n "$remote_sha" ]]; then
            printf '%s\trefs/tags/v9.9.9\n' "$remote_sha" > "$work/lsremote"
        else
            : > "$work/lsremote"
        fi
        if [[ $# -ge 7 ]]; then
            printf '%s' "$7" > "$work/prlist"
        else
            printf '%s\n' "$MERGE_SHA" > "$work/prlist"
        fi
        cat > "$work/bin/gh" <<EOF
#!/usr/bin/env bash
case "\$1 \$2" in
    "pr list") cat "$work/prlist" ;;
    "release view") exit 1 ;;
    "release create") echo "https://example/release" ;;
esac
EOF
        cat > "$work/bin/git" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/git.log"
case "\$1" in
    tag) [ "\$2" = -l ] && { [ -n "$local_sha" ] && echo "v9.9.9"; exit 0; } ;;
    rev-parse) echo "$local_sha" ;;
    show) printf '[package]\nname = "freenet"\nversion = "$cargo_ver"\n' ;;
    ls-remote) cat "$work/lsremote" ;;
esac
exit 0
EOF
        chmod +x "$work/bin/gh" "$work/bin/git"
        : > "$work/git.log"
        # shellcheck disable=SC2030,SC2031,SC2034,SC2317
        (
            PATH="$work/bin:$PATH"
            VERSION=9.9.9
            FDEV_VERSION=0.9.9
            DRY_RUN=false
            is_step_completed() { return 1; }
            mark_completed() { :; }
            run_cmd() { shift; "$@"; }
            generate_release_notes() { echo notes; }
            eval "$TAG_FN"
            create_github_release
        ) > "$work/out" 2>&1 || rc=$?
        if [[ "$rc" -ne 0 ]]; then
            got=refuse
        elif grep -qxF "tag -a v9.9.9 -m Release v9.9.9 $MERGE_SHA" "$work/git.log" \
            && grep -qxF "push origin v9.9.9" "$work/git.log"; then
            got=tag+push
        elif grep -q '^tag -a' "$work/git.log"; then
            got="tag-on-wrong-commit"
        elif grep -qxF "push origin v9.9.9" "$work/git.log"; then
            got=push
        else
            got=skip
        fi
        if [[ "$got" == "$want" ]]; then
            pass "create_github_release: $desc"
        else
            fail "create_github_release: $desc (got '$got', expected '$want')" "$(cat "$work/out")"
        fi
        rm -rf "$work"
    }
    tag_case "fresh: tags the merge commit and pushes" "" "" 9.9.9 tag+push
    tag_case "local tag already on the merge commit: pushes it" "$MERGE_SHA" "" 9.9.9 push
    tag_case "remote tag already on the merge commit: nothing to do" "$MERGE_SHA" "$MERGE_SHA" 9.9.9 skip
    tag_case "local tag on another commit: refuses" 2222222222222222222222222222222222222222 "" 9.9.9 refuse
    tag_case "remote tag on another commit: refuses" "" 2222222222222222222222222222222222222222 9.9.9 refuse
    tag_case "merge commit carries another version: refuses" "" "" 9.9.8 refuse
    # An annotated tag lists twice: the tag object's own SHA on the plain ref,
    # the commit on the peeled `^{}` ref. Only the peeled line names the commit.
    TAGOBJ=3333333333333333333333333333333333333333
    tag_case "annotated remote tag on the merge commit (peeled line): nothing to do" "$MERGE_SHA" "" 9.9.9 skip \
        "$TAGOBJ\trefs/tags/v9.9.9\n$MERGE_SHA\trefs/tags/v9.9.9^{}\n"
    tag_case "annotated remote tag on another commit: refuses" "$MERGE_SHA" "" 9.9.9 refuse \
        "$TAGOBJ\trefs/tags/v9.9.9\n2222222222222222222222222222222222222222\trefs/tags/v9.9.9^{}\n"
    # No merged bump PR means no validated commit; tagging must not fall back
    # to HEAD or to an empty SHA.
    tag_case "no merged release PR: refuses" "" "" 9.9.9 refuse "" ""
fi

# --- create_release_pr: auto-merge and the PR wait -----------------------------
#
# Auto-merge used to be `gh pr merge --squash --auto ... || true`: the merge
# queue rejects an explicit strategy, the rejection was swallowed, and the wait
# below then timed out. The timeout itself returned 0, so the driver went on to
# tag HEAD, which was the unmerged release branch. Run the real function on an
# already-open bump PR with a stub gh, a no-op git and an instant sleep.
CRPR_FN="$(awk '/^create_release_pr\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ -z "$CRPR_FN" ]] || [[ "$CRPR_FN" != *"gh pr merge"* ]]; then
    fail "could not extract create_release_pr() from release.sh"
else
    # pr_case <desc> <"STATE|auto-merge-on" from gh pr view> <gh pr merge rc>
    #         <PR state while waiting> <expected "rc=N merges=M squash=S">
    pr_case() {
        local desc="$1" mstate="$2" mrc="$3" wstate="$4" want="$5"
        local work got rc=0
        work="$(mktemp -d)"
        mkdir -p "$work/bin"
        cat > "$work/bin/gh" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/gh.log"
case "\$1 \$2" in
    "pr list") echo "77|OPEN|build: release 9.9.9" ;;
    "api graphql")
        case "\$*" in
            *isInMergeQueue*) echo "$mstate" ;;
        esac ;;
    "pr view")
        case "\$*" in
            # gh has no such --json field and rejects the whole query.
            *isInMergeQueue*) echo 'Unknown JSON field: "isInMergeQueue"' >&2; exit 1 ;;
            *statusCheckRollup*) echo "[]" ;;
            *) echo "$wstate" ;;
        esac ;;
    "pr merge") [ "$mrc" = 0 ] || { echo "merge refused" >&2; exit "$mrc"; } ;;
esac
exit 0
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/git"
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        chmod +x "$work/bin/gh" "$work/bin/git" "$work/bin/sleep"
        : > "$work/gh.log"
        # shellcheck disable=SC2030,SC2031,SC2034,SC2317
        (
            PATH="$work/bin:$PATH"
            VERSION=9.9.9
            FDEV_VERSION=0.9.9
            DRY_RUN=false
            is_step_completed() { return 1; }
            mark_completed() { :; }
            run_cmd() { shift; "$@"; }
            eval "$CRPR_FN"
            create_release_pr
        ) > "$work/out" 2>&1 || rc=$?
        got="rc=$rc merges=$(grep -c '^pr merge' "$work/gh.log") squash=$(grep -c -- '--squash' "$work/gh.log")"
        if [[ "$got" == "$want" ]]; then
            pass "create_release_pr: $desc"
        else
            fail "create_release_pr: $desc (got '$got', expected '$want')" "$(tail -5 "$work/out")"
        fi
        rm -rf "$work"
    }
    pr_case "enables auto-merge without --squash, then sees the merge" "OPEN|false" 0 MERGED "rc=0 merges=1 squash=0"
    pr_case "a refused auto-merge fails the driver" "OPEN|false" 1 MERGED "rc=1 merges=1 squash=0"
    pr_case "auto-merge already on: no merge call" "OPEN|true" 0 MERGED "rc=0 merges=0 squash=0"
    pr_case "already merged: no merge call" "MERGED|false" 0 MERGED "rc=0 merges=0 squash=0"
    pr_case "a PR that never merges times out NON-ZERO" "OPEN|false" 0 OPEN "rc=1 merges=1 squash=0"

    # The PR number comes from the URL `gh pr create` prints. It used to be
    # grepped from stdout with stderr discarded, so a failed create produced
    # an empty number. Run the create path (no existing PR, a version change
    # to commit) and check which PR the driver goes on to merge.
    # create_case <desc> <gh pr create output> <gh pr create rc> <expected "rc=N merge=<argv or none>">
    create_case() {
        local desc="$1" create_out="$2" create_rc="$3" want="$4"
        local work got rc=0
        work="$(mktemp -d)"
        mkdir -p "$work/bin"
        printf '%s\n' "$create_out" > "$work/create_out"
        cat > "$work/bin/gh" <<EOF
#!/usr/bin/env bash
printf '%s\n' "\$*" >> "$work/gh.log"
case "\$1 \$2" in
    "pr create") cat "$work/create_out"; exit $create_rc ;;
    "api graphql") echo "OPEN|false" ;;
    "pr view")
        case "\$*" in
            *) echo MERGED ;;
        esac ;;
esac
exit 0
EOF
        # diff reports changes and show-ref finds no branch, so the driver
        # commits, pushes and creates the PR.
        cat > "$work/bin/git" <<'EOF'
#!/usr/bin/env bash
case "$1" in
    diff|show-ref) exit 1 ;;
esac
exit 0
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        chmod +x "$work/bin"/*
        : > "$work/gh.log"
        # shellcheck disable=SC2030,SC2031,SC2034,SC2317
        (
            PATH="$work/bin:$PATH"
            VERSION=9.9.9
            FDEV_VERSION=0.9.9
            DRY_RUN=false
            is_step_completed() { return 1; }
            mark_completed() { :; }
            run_cmd() { shift; "$@"; }
            eval "$CRPR_FN"
            create_release_pr
        ) > "$work/out" 2>&1 || rc=$?
        got="rc=$rc merge=$(grep '^pr merge' "$work/gh.log" | paste -sd'|' -)"
        [[ "$got" == *"merge=" ]] && got="${got}none"
        if [[ "$got" == "$want" ]]; then
            pass "create_release_pr: $desc"
        else
            fail "create_release_pr: $desc (got '$got', expected '$want')" "$(tail -5 "$work/out")"
        fi
        rm -rf "$work"
    }
    create_case "the PR number is read from gh's URL" \
        "https://github.com/freenet/freenet-core/pull/123" 0 "rc=0 merge=pr merge 123 --auto"
    create_case "output with no PR number refuses" "something went sideways" 0 "rc=1 merge=none"
    create_case "a failed gh pr create refuses" "HTTP 422: Validation Failed" 1 "rc=1 merge=none"
fi

# --- check_prerequisites refuses an unreadable or empty CI status ---------------
#
# A failed `gh api` used to become `{}`, and zero checks used to print a
# warning and proceed, so a release could start on a main nobody had checked.
CP_FN="$(awk '/^check_prerequisites\(\) \{/,/^\}/' "$RELEASE_SH")"
if [[ -z "$CP_FN" ]] || [[ "$CP_FN" != *"check-runs"* ]]; then
    fail "could not extract check_prerequisites() from release.sh"
else
    # ci_case <desc> <check-runs JSON, or FAIL> <expected "rc=N"> [message substring]
    ci_case() {
        local desc="$1" answer="$2" want="$3" msg="${4:-}" work got rc=0
        work="$(mktemp -d)"
        mkdir -p "$work/bin"
        printf '%s\n' "$answer" > "$work/checks"
        cat > "$work/bin/gh" <<EOF
#!/usr/bin/env bash
[ "\$1" = api ] || exit 0
[ "\$(cat "$work/checks")" = FAIL ] && exit 1
cat "$work/checks"
EOF
        cat > "$work/bin/git" <<'EOF'
#!/usr/bin/env bash
case "$1" in
    branch) echo main ;;
    rev-parse) echo 4444444444444444444444444444444444444444 ;;
esac
exit 0
EOF
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/sleep"
        printf '#!/usr/bin/env bash\nexit 0\n' > "$work/bin/cargo"
        chmod +x "$work/bin"/*
        # shellcheck disable=SC2030,SC2031,SC2034,SC2317
        (
            PATH="$work/bin:$PATH"
            PROJECT_ROOT="$work"
            DRY_RUN=false
            SKIP_TESTS=false
            eval "$CP_FN"
            check_prerequisites
        ) > "$work/out" 2>&1 || rc=$?
        got="rc=$rc"
        if [[ "$got" == "$want" ]] && { [[ -z "$msg" ]] || grep -qF "$msg" "$work/out"; }; then
            pass "check_prerequisites: $desc"
        else
            fail "check_prerequisites: $desc (got '$got', expected '$want' with '$msg')" "$(tail -5 "$work/out")"
        fi
        rm -rf "$work"
    }
    ci_case "green main passes" \
        '{"total_count":1,"check_runs":[{"name":"Test","status":"completed","conclusion":"success"}]}' "rc=0"
    ci_case "a failed gh api refuses, naming the unreadable status" FAIL "rc=1" "Could not read main's CI status"
    ci_case "zero checks on main's HEAD refuses" '{"total_count":0,"check_runs":[]}' "rc=1" "no CI checks"
fi

# ---------------------------------------------------------------------------
# release.sh refuses a LIVE cut
# ---------------------------------------------------------------------------
# Its bump-PR flow no longer matches release.yml (exact ship_sha, release/vX
# branch, merge-back by release-sync.yml), so a live run must stop before it
# touches anything, while --dry-run still runs. The script is run whole, as a
# copy in a throwaway repo with no remote, so even a refusal that stopped
# working could not act on this checkout; gh, cargo and curl are stubs that
# record any call.
LIVE_WORK="$(mktemp -d)"
mkdir -p "$LIVE_WORK/repo/scripts" "$LIVE_WORK/repo/crates/core" "$LIVE_WORK/bin"
cp "$RELEASE_SH" "$LIVE_WORK/repo/scripts/release.sh"
# The real core manifest, so the dry run gets past release.sh's committed
# handshake-floor read (#6355) to its first external call.
cp "$SCRIPT_DIR/../crates/core/Cargo.toml" "$LIVE_WORK/repo/crates/core/Cargo.toml"
git -C "$LIVE_WORK/repo" init -q -b main
for tool in gh cargo curl; do
    printf '#!/usr/bin/env bash\necho "%s $*" >> "%s/calls"\nexit 1\n' "$tool" "$LIVE_WORK" > "$LIVE_WORK/bin/$tool"
    chmod +x "$LIVE_WORK/bin/$tool"
done
: > "$LIVE_WORK/calls"
live_rc=0
PATH="$LIVE_WORK/bin:$PATH" bash "$LIVE_WORK/repo/scripts/release.sh" --version 9.9.9 \
    > "$LIVE_WORK/out" 2>&1 || live_rc=$?
if [[ "$live_rc" -ne 0 ]] && [[ ! -s "$LIVE_WORK/calls" ]] \
    && grep -qF 'no longer cuts live releases' "$LIVE_WORK/out" \
    && grep -qF 'release.yml' "$LIVE_WORK/out"; then
    pass "release.sh refuses a live cut before calling anything, pointing at release.yml"
else
    fail "release.sh did not refuse a live cut cleanly" \
        "rc=$live_rc calls=$(paste -sd';' "$LIVE_WORK/calls") out=$(head -c 400 "$LIVE_WORK/out")"
fi
: > "$LIVE_WORK/calls"
PATH="$LIVE_WORK/bin:$PATH" bash "$LIVE_WORK/repo/scripts/release.sh" --version 9.9.9 --dry-run \
    > "$LIVE_WORK/out" 2>&1 || true
if ! grep -qF 'no longer cuts live releases' "$LIVE_WORK/out" && [[ -s "$LIVE_WORK/calls" ]]; then
    pass "release.sh --dry-run gets past the live-cut refusal"
else
    fail "release.sh --dry-run was refused or stopped before doing anything" \
        "out=$(head -c 400 "$LIVE_WORK/out")"
fi
rm -rf "$LIVE_WORK"

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release driver behaviour assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
