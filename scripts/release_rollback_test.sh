#!/usr/bin/env bash
# Behavioural tests for scripts/release-rollback.sh -- the release ROLLBACK
# tool, i.e. the thing an operator reaches for when a release has already gone
# wrong.
#
# WHY THESE ARE BEHAVIOURAL AND NOT SOURCE PINS. Both bugs they cover are
# properties of what the script DOES, and both were invisible from the outside:
#
#   * The fdev version to yank was COMPUTED from the freenet version as
#     `0.$((minor + 2)).$patch`. fdev's version is independent of freenet's --
#     release.yml bumps fdev's own patch in crates/fdev/Cargo.toml -- so the two
#     drifted apart long ago: for freenet 0.2.129 the formula asks crates.io to
#     yank fdev 0.4.129, a version that has never existed.
#
#   * That failure was then SWALLOWED. The yank's verdict was printed
#     ("✗ (failed or not published)") and consumed by nothing, so the script
#     went on to print "✅ Rollback complete!" and exit 0. `--yank-crates` has
#     therefore never yanked an fdev version, and always reported success.
#
# A source pin ("does the script mention crates/fdev/Cargo.toml?") would be
# satisfied by a mention. The property is "which version does it actually pass
# to `cargo yank`, and does a failure reach the exit status", so the script is
# run end to end against a throwaway git repo with `cargo`, `curl` and `gh`
# stubbed on PATH -- the release_driver_test.sh idiom.
#
# Run manually: bash scripts/release_rollback_test.sh
# Wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROLLBACK_SH="$SCRIPT_DIR/release-rollback.sh"

FAILURES=0
fail() {
    echo "FAIL - $1" >&2
    shift
    for line in "$@"; do echo "       $line" >&2; done
    FAILURES=$((FAILURES + 1))
}
pass() { echo "ok   - $1"; }

if [[ ! -f "$ROLLBACK_SH" ]]; then
    echo "FAIL - $ROLLBACK_SH not found" >&2
    exit 1
fi

# The version numbers are deliberately far apart, and all three are distinct:
#
#   TAG_FDEV     what shipped with the release being rolled back  (the answer)
#   MAIN_FDEV    what the working tree says NOW, main having moved on
#   0.4.129      what the old `minor + 2` arithmetic produces
#
# A test whose expected value is reachable by more than one route cannot say
# which route produced it.
FREENET_VERSION="0.2.129"
TAG_FDEV="0.3.291"
MAIN_FDEV="0.3.300"
ARITHMETIC_FDEV="0.4.129"

SANDBOX_ROOT="$(mktemp -d)"
trap 'rm -rf "$SANDBOX_ROOT"' EXIT

# make_sandbox [--no-tag]
#
# Builds a self-contained git repo holding a COPY of the script under test (so
# the script's own `git rev-parse --show-toplevel` resolves to the sandbox, not
# to freenet-core), an origin it can push to, a release tag whose tree names
# TAG_FDEV, and a later commit on main naming MAIN_FDEV.
make_sandbox() {
    local with_tag=true
    [[ "${1:-}" == "--no-tag" ]] && with_tag=false

    SANDBOX="$(mktemp -d -p "$SANDBOX_ROOT")"
    BIN="$SANDBOX/bin"
    REPO="$SANDBOX/repo"
    ORIGIN="$SANDBOX/origin.git"
    STUB_LOG="$SANDBOX/stub.log"
    OUT="$SANDBOX/out.txt"
    mkdir -p "$BIN"
    : > "$STUB_LOG"

    git init --quiet --bare "$ORIGIN"
    git init --quiet -b main "$REPO"
    git -C "$REPO" config user.email "test@example.invalid"
    git -C "$REPO" config user.name "Rollback Test"
    git -C "$REPO" remote add origin "$ORIGIN"

    mkdir -p "$REPO/scripts" "$REPO/crates/fdev"
    cp "$ROLLBACK_SH" "$REPO/scripts/release-rollback.sh"

    write_fdev_manifest "$TAG_FDEV"
    git -C "$REPO" add scripts crates >/dev/null
    git -C "$REPO" commit --quiet -m "build: release $FREENET_VERSION"
    if [[ "$with_tag" == "true" ]]; then
        git -C "$REPO" tag -a "v$FREENET_VERSION" -m "Release v$FREENET_VERSION"
    fi

    # main moves on: fdev is bumped again by the NEXT release's prep, so the
    # working tree no longer names the version that shipped with the tag.
    write_fdev_manifest "$MAIN_FDEV"
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "build: bump fdev"

    git -C "$REPO" push --quiet origin main
    if [[ "$with_tag" == "true" ]]; then
        git -C "$REPO" push --quiet origin "refs/tags/v$FREENET_VERSION"
    fi

    write_stubs
}

write_fdev_manifest() {
    cat > "$REPO/crates/fdev/Cargo.toml" <<EOF
[package]
name = "fdev"
version = "$1"
edition = "2024"

[dependencies]
freenet = { path = "../core", version = "0.2.129" }
serde = { version = "9.9.9" }
EOF
}

# cargo / curl / gh stubs. Each logs its argv so the assertions can ask what the
# script actually invoked, and each takes its behaviour from the environment.
write_stubs() {
    cat > "$BIN/cargo" <<'EOF'
#!/usr/bin/env bash
echo "cargo $*" >> "$STUB_LOG"
case "${CARGO_YANK_BEHAVIOUR:-ok}" in
    fail)
        echo "error: api errored with status 500 Internal Server Error" >&2
        exit 1
        ;;
    already)
        echo "error: failed to yank crate: crate version is already yanked" >&2
        exit 1
        ;;
    *)
        echo "        Yank ok"
        exit 0
        ;;
esac
EOF

    # Mirrors crates.io's REST shape: the last argument is the URL, and the
    # crate name is its second-to-last path segment.
    cat > "$BIN/curl" <<'EOF'
#!/usr/bin/env bash
echo "curl $*" >> "$STUB_LOG"
url="${*: -1}"
crate="$(basename "$(dirname "$url")")"
case "$crate" in
    freenet) printf '%s' "${CRATES_IO_CODE_FREENET:-200}" ;;
    fdev)    printf '%s' "${CRATES_IO_CODE_FDEV:-200}" ;;
    *)       printf '000' ;;
esac
exit 0
EOF

    cat > "$BIN/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_LOG"
if [[ "${1:-}" == "release" && "${2:-}" == "view" ]]; then
    if [[ "${GH_RELEASE_EXISTS:-0}" == "1" ]]; then exit 0; fi
    exit 1
fi
if [[ "${1:-}" == "release" && "${2:-}" == "delete" ]]; then
    if [[ "${GH_DELETE_FAIL:-0}" == "1" ]]; then
        echo "gh: release delete failed" >&2
        exit 1
    fi
    exit 0
fi
exit 0
EOF

    chmod +x "$BIN/cargo" "$BIN/curl" "$BIN/gh"
}

# run_rollback <args...> -- answers the confirmation prompt, captures combined
# output in $OUT and the exit status in $RC.
run_rollback() {
    (
        cd "$REPO" || exit 99
        PATH="$BIN:$PATH" STUB_LOG="$STUB_LOG" \
            bash scripts/release-rollback.sh "$@" <<<"yes"
    ) > "$OUT" 2>&1
    RC=$?
}

# Bash-glob matching, never `printf | grep -q`: under `pipefail` a
# short-circuiting grep kills the producer with SIGPIPE and a PRESENT string
# reads as absent (see .claude/rules/bug-prevention-patterns.md).
logged() { [[ "$(cat "$STUB_LOG")" == *"$1"* ]]; }
printed() { [[ "$(cat "$OUT")" == *"$1"* ]]; }

dump() {
    echo "--- exit status: $RC"
    echo "--- output:"
    sed 's/^/       | /' "$OUT"
    echo "--- stub invocations:"
    sed 's/^/       | /' "$STUB_LOG"
}

# 1. THE BUG: which fdev version does the script actually ask cargo to yank?
#
# Three-way: it must be the tag's version, and must be NEITHER the arithmetic
# answer (the bug) NOR the working tree's (the plausible half-fix that yanks a
# different release's fdev).
test_fdev_version_comes_from_the_release_tag() {
    make_sandbox
    run_rollback --version "$FREENET_VERSION" --yank-crates

    if logged "cargo yank --version $TAG_FDEV fdev"; then
        pass "fdev yank targets the version recorded at the release tag ($TAG_FDEV)"
    else
        fail "fdev yank did not target the tag's fdev version ($TAG_FDEV)" "$(dump)"
    fi

    if logged "$ARITHMETIC_FDEV"; then
        fail "fdev version was computed from the freenet version ($ARITHMETIC_FDEV)" "$(dump)"
    else
        pass "no trace of the 'minor + 2' arithmetic ($ARITHMETIC_FDEV)"
    fi

    if logged "$MAIN_FDEV"; then
        fail "fdev version came from the working tree ($MAIN_FDEV), not the tag" "$(dump)"
    else
        pass "the working tree's later fdev version ($MAIN_FDEV) is not used"
    fi

    if logged "cargo yank --version $FREENET_VERSION freenet"; then
        pass "freenet yank targets the released version"
    else
        fail "freenet was not yanked at $FREENET_VERSION" "$(dump)"
    fi

    if [[ $RC -eq 0 ]] && printed "Rollback complete"; then
        pass "a fully successful rollback still exits 0"
    else
        fail "successful rollback did not report success" "$(dump)"
    fi
}

# 2. THE OTHER HALF OF THE BUG: a failed yank must not be papered over with
# "✅ Rollback complete!" and exit 0.
test_failed_yank_is_not_reported_as_success() {
    make_sandbox
    CARGO_YANK_BEHAVIOUR=fail run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]]; then
        pass "a failed yank makes the script exit non-zero"
    else
        fail "a failed yank exited 0" "$(dump)"
    fi

    if printed "Rollback INCOMPLETE" && ! printed "Rollback complete"; then
        pass "a failed yank reports INCOMPLETE, not complete"
    else
        fail "a failed yank still printed a success summary" "$(dump)"
    fi

    if printed "yank fdev v$TAG_FDEV"; then
        pass "the failure summary names the step that failed"
    else
        fail "the failure summary does not name the failed yank" "$(dump)"
    fi
}

# 3. "Genuinely not published" is a normal outcome, not a failure. A rollback
# often runs for a release that never got as far as crates.io.
test_absent_crate_version_is_not_a_failure() {
    make_sandbox
    CRATES_IO_CODE_FREENET=404 CRATES_IO_CODE_FDEV=404 \
        run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -eq 0 ]] && printed "not published, skipping"; then
        pass "a 404 from crates.io is reported as 'not published', not as an error"
    else
        fail "an unpublished version was treated as a failure" "$(dump)"
    fi

    if logged "cargo yank"; then
        fail "cargo yank was invoked for a version crates.io says is absent" "$(dump)"
    else
        pass "no yank is attempted for a version that was never published"
    fi
}

# 4. The 403 trap RELEASE_RECOVERY.md documents: crates.io answers 403 to a
# request with no descriptive User-Agent. Reading that as "not published" is how
# a rollback silently skips a version that IS live.
test_unknown_crates_io_status_is_not_read_as_absent() {
    make_sandbox
    CRATES_IO_CODE_FDEV=403 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]]; then
        pass "an undetermined crates.io status fails the rollback"
    else
        fail "HTTP 403 was swallowed and the rollback reported success" "$(dump)"
    fi

    if printed "UNKNOWN"; then
        pass "the 403 is reported as UNKNOWN rather than as 'not published'"
    else
        fail "a 403 was not distinguished from a 404" "$(dump)"
    fi

    if logged "cargo yank --version $TAG_FDEV fdev"; then
        fail "yanked fdev despite not knowing whether it is published" "$(dump)"
    else
        pass "no yank is attempted while the publish state is unknown"
    fi
}

# 5. The User-Agent is the reason case 4 is a hypothetical rather than the
# normal case: drop it and crates.io answers 403 to EVERY probe.
test_crates_io_probe_sends_a_descriptive_user_agent() {
    make_sandbox
    run_rollback --version "$FREENET_VERSION" --yank-crates

    if logged "-A freenet-release-driver"; then
        pass "the crates.io probe identifies itself (403-avoidance)"
    else
        fail "the crates.io probe carries no descriptive User-Agent" "$(dump)"
    fi
}

# 6. Yanking something already yanked is the expected state on a re-run; it must
# not be re-reported as a failure.
test_already_yanked_is_success() {
    make_sandbox
    CARGO_YANK_BEHAVIOUR=already run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -eq 0 ]] && printed "already yanked"; then
        pass "an already-yanked version is a success, so a re-run is safe"
    else
        fail "re-running the rollback reported the already-yanked crate as failed" "$(dump)"
    fi
}

# 7. Same silent-success shape one step earlier: the remote tag deletion used to
# end in `|| true` and print ✓ regardless.
test_failed_remote_tag_deletion_is_reported() {
    make_sandbox
    # A pre-receive hook that declines, rather than `receive.denyDeletes`:
    # verified that denyDeletes does NOT reject this push on a local-path
    # remote, which would have made the case vacuous. The second assertion
    # below re-checks that the refusal really happened.
    printf '#!/bin/sh\nexit 1\n' > "$ORIGIN/hooks/pre-receive"
    chmod +x "$ORIGIN/hooks/pre-receive"
    run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]] && printed "delete remote tag v$FREENET_VERSION"; then
        pass "a refused remote tag deletion reaches the exit status"
    else
        fail "a refused remote tag deletion was reported as success" "$(dump)"
    fi

    if git -C "$ORIGIN" rev-parse "refs/tags/v$FREENET_VERSION" >/dev/null 2>&1; then
        pass "the sandbox really did refuse the deletion (the case is not vacuous)"
    else
        fail "the remote tag was deleted, so this case proved nothing" "$(dump)"
    fi
}

# 7b. The other way a remote tag deletion goes quietly missing: the lookup that
# decides whether there IS a remote tag fails, and "could not ask" is read as
# "not there". Same silent-skip outcome, no failure anywhere.
test_unqueryable_origin_is_not_read_as_no_remote_tag() {
    make_sandbox
    mv "$ORIGIN" "$SANDBOX/origin-gone.git"
    run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]] && printed "could not query origin"; then
        pass "an unreachable origin is a failure, not 'no remote tag'"
    else
        fail "an unreachable origin was reported as 'not found, skipping'" "$(dump)"
    fi
}

# 8. If the version cannot be established, stop BEFORE the destructive steps --
# steps 1 and 2 delete the tag the version is read from, so failing afterwards
# would leave the operator with nothing to look it up from.
test_unresolvable_fdev_version_stops_before_deleting_anything() {
    make_sandbox --no-tag
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "cannot determine which fdev version"; then
        pass "an unresolvable fdev version is a hard error"
    else
        fail "an unresolvable fdev version did not stop the rollback" "$(dump)"
    fi

    if logged "gh release delete"; then
        fail "destructive steps ran before the fdev version was resolved" "$(dump)"
    else
        pass "nothing destructive runs before the fdev version is resolved"
    fi

    if printed "--fdev-version"; then
        pass "the error tells the operator how to supply the version by hand"
    else
        fail "the error does not mention the --fdev-version escape hatch" "$(dump)"
    fi
}

# 9. That escape hatch has to work, since it is the documented answer to case 8.
test_explicit_fdev_version_overrides_the_tag() {
    make_sandbox
    run_rollback --version "$FREENET_VERSION" --yank-crates --fdev-version 0.3.777

    if logged "cargo yank --version 0.3.777 fdev" && ! logged "cargo yank --version $TAG_FDEV fdev"; then
        pass "--fdev-version overrides the value read from the tag"
    else
        fail "--fdev-version was ignored" "$(dump)"
    fi
}

# 10. A dry run must resolve and SHOW the version (that is most of its value
# here, given the number used to be wrong) without touching crates.io.
test_dry_run_shows_the_fdev_version_without_yanking() {
    make_sandbox
    run_rollback --version "$FREENET_VERSION" --yank-crates --dry-run

    if [[ $RC -eq 0 ]] && printed "$TAG_FDEV"; then
        pass "a dry run prints the fdev version it would yank"
    else
        fail "a dry run did not show the fdev version" "$(dump)"
    fi

    if logged "cargo yank"; then
        fail "a dry run invoked cargo yank" "$(dump)"
    else
        pass "a dry run does not invoke cargo yank"
    fi
}

test_fdev_version_comes_from_the_release_tag
test_failed_yank_is_not_reported_as_success
test_absent_crate_version_is_not_a_failure
test_unknown_crates_io_status_is_not_read_as_absent
test_crates_io_probe_sends_a_descriptive_user_agent
test_already_yanked_is_success
test_failed_remote_tag_deletion_is_reported
test_unqueryable_origin_is_not_read_as_no_remote_tag
test_unresolvable_fdev_version_stops_before_deleting_anything
test_explicit_fdev_version_overrides_the_tag
test_dry_run_shows_the_fdev_version_without_yanking

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "release-rollback.sh: all assertions passed"
    exit 0
fi
echo "release-rollback.sh: $FAILURES assertion(s) failed" >&2
exit 1
