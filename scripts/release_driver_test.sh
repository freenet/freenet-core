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

        got_f=0; got_d=0
        [[ "$(grep -cF 'publish -p freenet' "$work/cargo.log")" -gt 0 ]] && got_f=1
        [[ "$(grep -cF 'publish -p fdev'    "$work/cargo.log")" -gt 0 ]] && got_d=1
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
                "Note this observes the FIRST call, so adding an earlier one in any" \
                "form -- a conditional, a subshell, a helper -- fails here even when" \
                "the original bare call is left untouched."
        fi
        rm -rf "$work"
    fi
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
    echo "All release driver behaviour assertions passed."
else
    echo "$FAILURES assertion(s) FAILED." >&2
    exit 1
fi
