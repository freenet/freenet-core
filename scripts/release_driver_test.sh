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
# Both functions are extracted verbatim from release.sh (`eval "$(awk ...)"`),
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
        # shellcheck disable=SC2034,SC2317
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
# for a `CARGO_REGISTRY_TOKEN` that never reached CI. Walking the five failure
# paths shows publishing is wrong on four of them:
#
#   no workflow run found        Gate A never ran        -> publish ungated
#   attach job never reported    Gate A state unknown    -> publish ungated
#   attach conclusion != success Gate A REJECTED it      -> publish what it blocked
#   timeout                      Gate A undecided        -> pre-empt the gate
#   assets missing, attach OK    Gate A passed           -> the only safe one
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
                "passed -- on four of the five failure paths, including the one where" \
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
        _body="$(code_lines "$RELEASE_SH" \
            | awk -F: -v a="$_gl" '$1 > a && $1 <= a + 4')"
        case "$_body" in
            *"return 1"*) ;;
            *) PDR_BAD_BODY+="line $_gl: guard body does not return nonzero"$'\n' ;;
        esac
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

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release driver behaviour assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
