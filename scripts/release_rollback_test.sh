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
# SAFETY. This suite runs a tool that deletes tags and yanks crates, with a
# REAL released version number, and the script hardcodes --repo
# freenet/freenet-core. Every run therefore goes through `assert_stubs_intercept`
# first, and the subshell clears the credentials a real `cargo`/`gh` would need.
# Without that, one failed `chmod +x` in the setup would point the whole suite
# at production.
#
# Run manually: bash scripts/release_rollback_test.sh
# Wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROLLBACK_SH="$SCRIPT_DIR/release-rollback.sh"
REAL_FDEV_MANIFEST="$SCRIPT_DIR/../crates/fdev/Cargo.toml"

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

# The version numbers are deliberately far apart, and all distinct:
#
#   TAG_FDEV     what shipped with the release being rolled back  (the answer)
#   MAIN_FDEV    what the working tree says NOW, main having moved on
#   STALE_FDEV   what a leftover LOCAL tag of the same name says
#   0.4.129      what the old `minor + 2` arithmetic produces
#
# A test whose expected value is reachable by more than one route cannot say
# which route produced it.
FREENET_VERSION="0.2.129"
TAG_FDEV="0.3.291"
MAIN_FDEV="0.3.300"
STALE_FDEV="0.3.100"
ARITHMETIC_FDEV="0.4.129"
# What a FORK's tag of the same name claims, for the case where `origin` is not
# freenet/freenet-core. The yank always reaches the real crates.io regardless.
FORK_FDEV="0.3.999"

SANDBOX_ROOT="$(mktemp -d)"
trap 'rm -rf "$SANDBOX_ROOT"' EXIT

# make_sandbox [--no-tag | --no-manifest]
#
# Builds a self-contained git repo holding a COPY of the script under test (so
# the script's own `git rev-parse --show-toplevel` resolves to the sandbox, not
# to freenet-core), an origin it can push to, a release tag whose tree names
# TAG_FDEV, and a later commit on main naming MAIN_FDEV.
#
#   --no-tag       no release tag anywhere (local or origin)
#   --no-manifest  the tag EXISTS, on both sides, but has no fdev manifest --
#                  so resolution fails with the tag still present, which is what
#                  makes the "stops before deleting anything" case non-vacuous
make_sandbox() {
    local mode="${1:-normal}"

    SANDBOX="$(mktemp -d -p "$SANDBOX_ROOT")"
    BIN="$SANDBOX/bin"
    REPO="$SANDBOX/repo"
    # The script refuses to take the fdev version from an origin that is not
    # freenet/freenet-core (the yank reaches the real crates.io whatever origin
    # is). Give the sandbox's bare repo a path the real matcher accepts, rather
    # than adding a test-only backdoor to a destructive script.
    ORIGIN="$SANDBOX/github.com/freenet/freenet-core.git"
    STUB_LOG="$SANDBOX/stub.log"
    OUT="$SANDBOX/out.txt"
    REPLY_INPUT="yes"
    mkdir -p "$BIN"
    : > "$STUB_LOG"

    mkdir -p "$(dirname "$ORIGIN")"
    git init --quiet --bare "$ORIGIN"
    git init --quiet -b main "$REPO"
    git -C "$REPO" config user.email "test@example.invalid"
    git -C "$REPO" config user.name "Rollback Test"
    git -C "$REPO" remote add origin "$ORIGIN"
    # The hostile-but-legal config the resolution fetch has to survive: with
    # `tagOpt = --tags` a fetch of ONE ref also downloads every tag, so a
    # resolution -- including one inside a DRY RUN -- creates local tags as a
    # side effect. Set on every sandbox so the --no-tags guard is exercised by
    # the whole suite rather than by one case that remembers to opt in.
    git -C "$REPO" config remote.origin.tagOpt --tags

    mkdir -p "$REPO/scripts" "$REPO/crates/fdev"
    cp "$ROLLBACK_SH" "$REPO/scripts/release-rollback.sh"

    if [[ "$mode" == "--no-manifest" ]]; then
        echo "placeholder" > "$REPO/crates/fdev/README.md"
    else
        write_fdev_manifest "$TAG_FDEV"
    fi
    git -C "$REPO" add scripts crates >/dev/null
    git -C "$REPO" commit --quiet -m "build: release $FREENET_VERSION"
    if [[ "$mode" != "--no-tag" ]]; then
        git -C "$REPO" tag -a "v$FREENET_VERSION" -m "Release v$FREENET_VERSION"
    fi

    # main moves on: fdev is bumped again by the NEXT release's prep, so the
    # working tree no longer names the version that shipped with the tag.
    write_fdev_manifest "$MAIN_FDEV"
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "build: bump fdev"

    git -C "$REPO" push --quiet origin main
    if [[ "$mode" != "--no-tag" ]]; then
        git -C "$REPO" push --quiet origin "refs/tags/v$FREENET_VERSION"
    fi

    write_stubs
}

# What an aborted release attempt leaves behind: a LOCAL tag pointing at a
# commit naming STALE_FDEV, while origin's tag of the same name still points at
# the commit that actually shipped (TAG_FDEV). release.sh skips tag creation
# when a local tag of that name exists, which is how the two diverge.
make_local_tag_stale() {
    local shipped_sha
    shipped_sha="$(git -C "$REPO" rev-parse "refs/tags/v$FREENET_VERSION")"
    write_fdev_manifest "$STALE_FDEV"
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "aborted release attempt"
    git -C "$REPO" tag -d "v$FREENET_VERSION" >/dev/null
    git -C "$REPO" tag -a "v$FREENET_VERSION" -m "stale local tag"
    git -C "$REPO" push --quiet --force origin "$shipped_sha:refs/tags/v$FREENET_VERSION"
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

    # "release not found" is gh's real wording for an absent release; an auth or
    # transport error says something else entirely, which is the distinction the
    # script now depends on.
    cat > "$BIN/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_LOG"
if [[ "${1:-}" == "release" && "${2:-}" == "view" ]]; then
    if [[ "${GH_VIEW_ERROR:-0}" == "1" ]]; then
        echo "HTTP 401: Bad credentials (https://api.github.com/repos/freenet/freenet-core/releases/tags/x)" >&2
        exit 1
    fi
    if [[ "${GH_RELEASE_EXISTS:-0}" == "1" ]]; then
        echo '{"id":"RE_stub"}'
        exit 0
    fi
    echo "release not found" >&2
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

# The suite drives a destructive tool with a REAL released version number. If a
# stub were missing (a failed chmod, a typo'd path) the script would reach the
# real `cargo yank` and the real `gh release delete` -- on a machine that is
# authenticated for both. Refuse to run at all in that case.
assert_stubs_intercept() {
    local tool resolved
    for tool in cargo curl gh; do
        resolved="$(PATH="$BIN:$PATH" command -v "$tool" 2>/dev/null)"
        if [[ "$resolved" != "$BIN/$tool" ]]; then
            echo "FATAL - $tool would resolve to '${resolved:-nothing}', not the stub at $BIN/$tool." >&2
            echo "        Refusing to run a destructive script against the real registry." >&2
            exit 99
        fi
    done
}

# run_rollback <args...> -- answers the confirmation prompt with $REPLY_INPUT,
# captures combined output in $OUT and the exit status in $RC.
#
# The environment is scrubbed on purpose: no ambient git config (a global
# core.hooksPath would neutralise the pre-receive hook case below), and no
# registry or GitHub credentials, so a stub that somehow failed to intercept
# cannot authenticate against production either.
_invoke_rollback() {
    cd "$REPO" || exit 99
    PATH="$BIN:$PATH" STUB_LOG="$STUB_LOG" \
        GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \
        CARGO_HOME="$SANDBOX/cargo-home" CARGO_REGISTRY_TOKEN='' \
        GH_TOKEN='' GITHUB_TOKEN='' GH_CONFIG_DIR="$SANDBOX/gh-config" \
        bash scripts/release-rollback.sh "$@"
}

run_rollback() {
    assert_stubs_intercept
    ( _invoke_rollback "$@" <<<"$REPLY_INPUT" ) > "$OUT" 2>&1
    RC=$?
}

# As run_rollback, but with stdin at EOF -- what cron, CI, a systemd unit or a
# `< /dev/null` invocation actually look like to the confirmation prompt.
run_rollback_no_stdin() {
    assert_stubs_intercept
    ( _invoke_rollback "$@" </dev/null ) > "$OUT" 2>&1
    RC=$?
}

# Bash-glob matching, never `printf | grep -q`: under `pipefail` a
# short-circuiting grep kills the producer with SIGPIPE and a PRESENT string
# reads as absent (see .claude/rules/bug-prevention-patterns.md).
logged() { [[ "$(cat "$STUB_LOG")" == *"$1"* ]]; }
printed() { [[ "$(cat "$OUT")" == *"$1"* ]]; }
local_tag_exists() { git -C "$REPO" rev-parse -q --verify "refs/tags/v$FREENET_VERSION" >/dev/null 2>&1; }
origin_tag_exists() { git -C "$ORIGIN" rev-parse -q --verify "refs/tags/v$FREENET_VERSION" >/dev/null 2>&1; }

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
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

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

    # The rollback must actually roll back. Without these, making step 1 or 3 a
    # no-op leaves the whole suite green.
    if ! local_tag_exists && ! origin_tag_exists; then
        pass "the local and origin tags are actually deleted"
    else
        fail "a tag survived the rollback (local: $(local_tag_exists && echo yes || echo no), origin: $(origin_tag_exists && echo yes || echo no))" "$(dump)"
    fi

    # `git tag -d` prints "Deleted tag 'vX' (was <sha>)" -- the only record of
    # where the tag pointed, printed immediately before the script deletes that
    # tag from origin and deletes the release page. Capturing it and showing it
    # only on FAILURE loses it on the path that actually destroys things.
    if printed "Deleted tag 'v$FREENET_VERSION' (was "; then
        pass "the deleted tag's SHA is reported on success, not only on failure"
    else
        fail "the deleted tag's SHA was swallowed on the successful path" "$(dump)"
    fi

    if logged "gh release delete v$FREENET_VERSION"; then
        pass "the GitHub release is actually deleted"
    else
        fail "the GitHub release was never deleted" "$(dump)"
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

    if printed "api errored with status 500"; then
        pass "the failing command's own output reaches the operator"
    else
        fail "the underlying error text was swallowed" "$(dump)"
    fi

    if printed "--fdev-version $TAG_FDEV"; then
        pass "the failure summary carries the fdev version a re-run will need"
    else
        fail "the failure summary omits the fdev version, which the re-run cannot re-derive" "$(dump)"
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
    # remote, which would have made the case vacuous. The tag-survival assertion
    # below re-checks that the refusal really happened.
    printf '#!/bin/sh\nexit 1\n' > "$ORIGIN/hooks/pre-receive"
    chmod +x "$ORIGIN/hooks/pre-receive"
    run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]] && printed "delete remote tag v$FREENET_VERSION" && ! printed "could not query origin"; then
        pass "a refused remote tag deletion reaches the exit status"
    else
        fail "a refused remote tag deletion was reported as success" "$(dump)"
    fi

    if origin_tag_exists; then
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

# 7c. Step 3 had the same two-state shape: `gh release view` exits non-zero both
# for "no such release" and for gh being broken/unauthenticated, and reading the
# second as the first leaves the release live under a "complete" rollback.
test_gh_query_error_is_not_read_as_no_release() {
    make_sandbox
    GH_VIEW_ERROR=1 run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]] && printed "could not query GitHub"; then
        pass "a GitHub lookup error is a failure, not 'no such release'"
    else
        fail "a GitHub lookup error was reported as 'not found, skipping'" "$(dump)"
    fi

    if printed "Bad credentials"; then
        pass "gh's own error text reaches the operator"
    else
        fail "gh's error text was swallowed" "$(dump)"
    fi
}

# 7d. And the deletion itself failing must reach the exit status, like every
# other step.
test_failed_github_release_deletion_is_reported() {
    make_sandbox
    GH_RELEASE_EXISTS=1 GH_DELETE_FAIL=1 run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]] && printed "delete GitHub release v$FREENET_VERSION"; then
        pass "a failed GitHub release deletion reaches the exit status"
    else
        fail "a failed GitHub release deletion was reported as success" "$(dump)"
    fi
}

# 8. If the version cannot be established, stop BEFORE the destructive steps --
# steps 1 to 3 delete the tag and release the version is read from, so failing
# afterwards would leave the operator with nothing to look it up from.
#
# The tag EXISTS here (on both sides) and resolution fails for a different
# reason, so the case can actually observe the ordering: with the resolve moved
# below the tag deletions, the two survival assertions go red.
test_unresolvable_fdev_version_stops_before_deleting_anything() {
    make_sandbox --no-manifest
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "cannot determine which fdev version"; then
        pass "an unresolvable fdev version is a hard error"
    else
        fail "an unresolvable fdev version did not stop the rollback" "$(dump)"
    fi

    if local_tag_exists && origin_tag_exists; then
        pass "both tags survive: nothing destructive ran before resolution"
    else
        fail "a tag was deleted before the fdev version was resolved" "$(dump)"
    fi

    if logged "gh release delete"; then
        fail "the GitHub release was deleted before the fdev version was resolved" "$(dump)"
    else
        pass "the GitHub release survives too"
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

    # ... but silently yanking a version the tag contradicts is how a near-miss
    # takes out a GOOD release: adjacent fdev patches all exist on crates.io.
    if printed "does not match" && printed "$TAG_FDEV"; then
        pass "an override that contradicts the tag is called out before the prompt"
    else
        fail "an override contradicting the tag passed without comment" "$(dump)"
    fi
}

# 10. A dry run must resolve and SHOW the version (that is most of its value
# here, given the number used to be wrong) and probe crates.io, without yanking.
test_dry_run_shows_the_fdev_version_without_yanking() {
    make_sandbox
    run_rollback --version "$FREENET_VERSION" --yank-crates --dry-run

    if [[ $RC -eq 0 ]] && printed "Yanking fdev v$TAG_FDEV"; then
        pass "a dry run names the fdev version it would yank"
    else
        fail "a dry run did not show the fdev version" "$(dump)"
    fi

    if logged "crates.io/api/v1/crates/fdev/$TAG_FDEV"; then
        pass "a dry run checks crates.io, so it can preview the irreversible step"
    else
        fail "a dry run does not verify the version is even published" "$(dump)"
    fi

    if logged "cargo yank"; then
        fail "a dry run invoked cargo yank" "$(dump)"
    else
        pass "a dry run does not invoke cargo yank"
    fi

    if local_tag_exists && origin_tag_exists; then
        pass "a dry run deletes nothing"
    else
        fail "a dry run deleted a tag" "$(dump)"
    fi
}

# 11. The re-run path this script's own failure summary points at: the local tag
# is already gone, and only origin still has it.
test_fdev_version_resolves_from_origin_when_the_local_tag_is_gone() {
    make_sandbox
    git -C "$REPO" tag -d "v$FREENET_VERSION" >/dev/null

    # A DRY RUN first, and that is the whole point of the ordering: a dry run
    # deletes nothing, so a local tag created by the RESOLUTION FETCH is still
    # there to be seen. After a real run step 1 deletes the tag either way, so
    # the same check there would pass whether or not the fetch created it.
    #
    # This is the --no-tags guard. The sandbox sets `tagOpt = --tags`, under
    # which `git fetch origin refs/tags/<tag>` also downloads every other tag --
    # so a dry run, whose contract is to touch nothing, silently creates local
    # tags.
    run_rollback --version "$FREENET_VERSION" --yank-crates --dry-run

    if local_tag_exists; then
        fail "a DRY RUN's resolution fetch created a local tag" "$(dump)"
    else
        pass "the resolution fetch creates no local tag, not even under --dry-run"
    fi

    run_rollback --version "$FREENET_VERSION" --yank-crates

    if logged "cargo yank --version $TAG_FDEV fdev"; then
        pass "the fdev version is read from origin when the local tag is gone"
    else
        fail "resolution failed with the tag still on origin" "$(dump)"
    fi
}

# 12. A STALE local tag is the dangerous version of case 11: release.sh skips
# tag creation when a local tag of that name exists, so an aborted run leaves one
# behind pointing somewhere else. Origin's tag is what shipped.
test_stale_local_tag_does_not_win_over_origin() {
    make_sandbox
    make_local_tag_stale
    run_rollback --version "$FREENET_VERSION" --yank-crates

    if logged "cargo yank --version $TAG_FDEV fdev"; then
        pass "origin's tag decides the fdev version, not a stale local tag"
    else
        fail "a stale local tag chose the fdev version to yank" "$(dump)"
    fi

    if logged "$STALE_FDEV"; then
        fail "the stale local tag's fdev version ($STALE_FDEV) was yanked" "$(dump)"
    else
        pass "the stale local tag's version ($STALE_FDEV) is never used"
    fi

    if printed "warning: local v$FREENET_VERSION names fdev $STALE_FDEV"; then
        pass "the disagreement between local and origin is reported, not hidden"
    else
        fail "the local/origin disagreement passed silently" "$(dump)"
    fi
}

# 13. Answering anything but "yes" must stop before every destructive action.
test_declining_the_prompt_does_nothing() {
    make_sandbox
    REPLY_INPUT="no"
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "Aborted"; then
        pass "declining the confirmation aborts with a non-zero status"
    else
        fail "declining the confirmation did not abort" "$(dump)"
    fi

    if logged "cargo yank" || logged "gh release delete" || ! local_tag_exists || ! origin_tag_exists; then
        fail "something destructive ran after the operator declined" "$(dump)"
    else
        pass "nothing was deleted or yanked after declining"
    fi
}

# 14. The fixture manifest is hand-written, so on its own it cannot notice the
# real one moving out from under the parser (`version.workspace = true`, a
# layout change). Parse the REPO'S OWN crates/fdev/Cargo.toml and require the
# answer to match what that file declares.
test_parses_the_repositorys_real_fdev_manifest() {
    if [[ ! -f "$REAL_FDEV_MANIFEST" ]]; then
        fail "the repo's crates/fdev/Cargo.toml is missing at $REAL_FDEV_MANIFEST"
        return
    fi

    local expected
    expected="$(awk -F'"' '/^version = "/ { print $2; exit }' "$REAL_FDEV_MANIFEST")"

    make_sandbox
    cp "$REAL_FDEV_MANIFEST" "$REPO/crates/fdev/Cargo.toml"
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "real manifest"
    git -C "$REPO" tag -d "v$FREENET_VERSION" >/dev/null
    git -C "$REPO" tag -a "v$FREENET_VERSION" -m "real manifest"
    git -C "$REPO" push --quiet --force origin "refs/tags/v$FREENET_VERSION"

    run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ -n "$expected" ]] && logged "cargo yank --version $expected fdev"; then
        pass "the parser reads the repo's real fdev manifest ($expected)"
    else
        fail "the parser could not read the repo's real crates/fdev/Cargo.toml (expected '$expected')" "$(dump)"
    fi
}

# 15. The parse is anchored to [package] rather than taking the first
# line-anchored `version = `, and this is the shape that makes the difference: a
# manifest that inherits its own version from the workspace, with a dependency
# table whose version IS line-anchored. Unanchored, the parser returns the
# DEPENDENCY's version -- freenet's -- and yanks it as though it were fdev's.
# Anchored, nothing parses and the script stops and asks.
#
# Found by mutation: dropping the [package] anchor left the whole suite green
# until this case existed.
test_workspace_inherited_version_is_refused_not_misparsed() {
    make_sandbox
    cat > "$REPO/crates/fdev/Cargo.toml" <<EOF
[package]
name = "fdev"
version.workspace = true
edition = "2024"

[dependencies.freenet]
path = "../core"
version = "0.2.129"
EOF
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "workspace-inherited version"
    git -C "$REPO" tag -d "v$FREENET_VERSION" >/dev/null
    git -C "$REPO" tag -a "v$FREENET_VERSION" -m "workspace-inherited version"
    git -C "$REPO" push --quiet --force origin "refs/tags/v$FREENET_VERSION"

    run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "cannot determine which fdev version"; then
        pass "a manifest with no literal [package] version stops the rollback"
    else
        fail "a workspace-inherited fdev version did not stop the rollback" "$(dump)"
    fi

    if logged "cargo yank --version 0.2.129 fdev"; then
        fail "a DEPENDENCY's version was parsed as fdev's and yanked" "$(dump)"
    else
        pass "no dependency version is mistaken for the [package] version"
    fi
}

# 16. The confirmation prompt is the last thing standing between an UNATTENDED
# caller -- cron, CI, a systemd unit, anything with stdin closed -- and an
# irreversible-in-practice yank. At EOF `read` must abort, never fall through
# with an empty or defaulted REPLY.
#
# Found by mutation: making EOF set REPLY=yes left all 45 other assertions in
# this file green. Case 13 pipes "no", which such a mutant also rejects, so
# nothing else here separates "declined" from "never asked".
test_no_stdin_aborts_before_anything_destructive() {
    make_sandbox
    GH_RELEASE_EXISTS=1 run_rollback_no_stdin --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]]; then
        pass "a run with no stdin exits non-zero rather than auto-confirming"
    else
        fail "a run with no stdin exited 0 -- the prompt was not a gate" "$(dump)"
    fi

    if printed "Aborted (no answer given)"; then
        pass "the abort says the prompt was never answered"
    else
        fail "an EOF on the prompt was not reported as an unanswered prompt" "$(dump)"
    fi

    if logged "cargo yank" || logged "gh release delete" || ! local_tag_exists || ! origin_tag_exists; then
        fail "something destructive ran with nobody there to confirm it" "$(dump)"
    else
        pass "no tag, release or crate was touched with stdin at EOF"
    fi
}

# 17. The re-run this script's own summary steers the operator into: it has just
# deleted the tag locally AND on origin, so a second run has nothing left to read
# the fdev version from. The --no-tag fixture is that state.
test_missing_tag_everywhere_demands_an_explicit_fdev_version() {
    make_sandbox --no-tag
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "cannot determine which fdev version"; then
        pass "with no tag locally or on origin, the rollback stops instead of guessing"
    else
        fail "a missing tag on both sides did not stop the rollback" "$(dump)"
    fi

    if logged "cargo yank"; then
        fail "cargo yank ran with no tag to read the fdev version from" "$(dump)"
    else
        pass "nothing is yanked when there is no tag to resolve from"
    fi

    # ... and the documented escape hatch has to carry that re-run through.
    make_sandbox --no-tag
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates --fdev-version "$TAG_FDEV"

    if [[ $RC -eq 0 ]] && logged "cargo yank --version $TAG_FDEV fdev"; then
        pass "--fdev-version carries the tagless re-run through"
    else
        fail "--fdev-version did not rescue a re-run with no tag anywhere" "$(dump)"
    fi

    # And a SUCCESSFUL tag-only rollback in the same state must not suggest a
    # command that cannot run. It used to print "run: ... --yank-crates" and
    # exit 0 -- but that command hard-errors, because the tag it would read the
    # fdev version from is exactly what this run just deleted.
    make_sandbox --no-tag
    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION"

    if [[ $RC -ne 0 ]]; then
        fail "the tag-only rollback did not succeed, so the follow-up hint was never reached" "$(dump)"
    elif printed "--yank-crates --fdev-version X.Y.Z"; then
        pass "the follow-up hint names the version lookup, not a command that would fail"
    else
        fail "the follow-up hint omits --fdev-version, so the suggested re-run hard-errors" "$(dump)"
    fi

    # ... and specifically not the BARE ready-to-run form, which is what exits
    # 0 here and then stops with an error on the re-run.
    if printed "To yank crates, run: "; then
        fail "the follow-up hint still offers a ready-to-run command it has no version for" "$(dump)"
    else
        pass "no ready-to-run re-run command is offered with no version to put in it"
    fi
}

# 18. THE COMPOSED FAILURE: a stale LOCAL tag plus an origin that cannot be
# reached. Both halves are routine -- release.sh skips tag creation when a local
# tag exists, so an aborted run leaves one behind, and a dead origin mid-incident
# is ordinary -- and together they used to yank whatever the local tag named.
test_local_tag_alone_never_decides_the_yank() {
    make_sandbox
    make_local_tag_stale
    mv "$ORIGIN" "$SANDBOX/origin-gone.git"

    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if [[ $RC -ne 0 ]] && printed "cannot determine which fdev version"; then
        pass "a local-tag-only resolution is a hard stop, not a yank"
    else
        fail "the rollback proceeded on a local tag alone" "$(dump)"
    fi

    if logged "cargo yank"; then
        fail "cargo yank ran on a version read from a possibly-stale local tag" "$(dump)"
    else
        pass "nothing is yanked from a local-tag reading"
    fi

    if printed "--fdev-version X.Y.Z"; then
        pass "the stop names the escape hatch"
    else
        fail "the stop does not tell the operator how to supply the version" "$(dump)"
    fi

    # The banner used to reprint the local reading as though it were the fact of
    # the matter ("fdev: 0.3.100"), one line above the confirmation prompt.
    if printed "fdev:        $STALE_FDEV"; then
        fail "the banner presented a local-tag guess as the fdev version" "$(dump)"
    else
        pass "the banner never states a local-tag guess as fact"
    fi

    # And the tag-only rollback must not hand that guess back inside a
    # ready-to-paste command: pasted, it becomes an "explicit" --fdev-version,
    # which on the next run also silences the mismatch warning.
    make_sandbox
    make_local_tag_stale
    mv "$ORIGIN" "$SANDBOX/origin-gone.git"
    run_rollback --version "$FREENET_VERSION"

    if printed "--fdev-version $STALE_FDEV"; then
        fail "the summary handed back a local-tag guess as a command to run" "$(dump)"
    else
        pass "no local-tag guess is laundered into the follow-up command"
    fi
}

# 19. `origin` decides which tag is read, but `cargo yank` always reaches the
# REAL crates.io and step 3 deletes from a hardcoded --repo freenet/freenet-core.
# With origin pointed at a fork, a tag of the same name naming a different fdev
# would therefore take a GOOD release's crate off the real registry.
test_fork_origin_is_not_a_version_source() {
    make_sandbox

    local fork="$SANDBOX/github.com/somebody/freenet-core.git"
    mkdir -p "$(dirname "$fork")"
    git init --quiet --bare "$fork"
    write_fdev_manifest "$FORK_FDEV"
    git -C "$REPO" add crates >/dev/null
    git -C "$REPO" commit --quiet -m "fork's release"
    git -C "$REPO" tag -a "fork-tag" -m "fork" >/dev/null
    git -C "$REPO" push --quiet "$fork" "refs/tags/fork-tag:refs/tags/v$FREENET_VERSION"
    git -C "$REPO" tag -d "fork-tag" >/dev/null
    git -C "$REPO" remote set-url origin "$fork"

    GH_RELEASE_EXISTS=1 run_rollback --version "$FREENET_VERSION" --yank-crates

    if printed "origin is not freenet/freenet-core"; then
        pass "a non-freenet origin is called out rather than trusted"
    else
        fail "origin being a fork passed without comment" "$(dump)"
    fi

    if logged "$FORK_FDEV"; then
        fail "a fork's tag chose the version yanked from the real crates.io" "$(dump)"
    else
        pass "a fork's tag is never the source of the version to yank"
    fi

    if [[ $RC -ne 0 ]] && ! logged "cargo yank"; then
        pass "the run stops instead of yanking on a fork's say-so"
    else
        fail "the rollback yanked with origin pointed at a fork" "$(dump)"
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
test_gh_query_error_is_not_read_as_no_release
test_failed_github_release_deletion_is_reported
test_unresolvable_fdev_version_stops_before_deleting_anything
test_explicit_fdev_version_overrides_the_tag
test_dry_run_shows_the_fdev_version_without_yanking
test_fdev_version_resolves_from_origin_when_the_local_tag_is_gone
test_stale_local_tag_does_not_win_over_origin
test_declining_the_prompt_does_nothing
test_parses_the_repositorys_real_fdev_manifest
test_workspace_inherited_version_is_refused_not_misparsed
test_no_stdin_aborts_before_anything_destructive
test_missing_tag_everywhere_demands_an_explicit_fdev_version
test_local_tag_alone_never_decides_the_yank
test_fork_origin_is_not_a_version_source

echo
if [[ $FAILURES -eq 0 ]]; then
    echo "release-rollback.sh: all assertions passed"
    exit 0
fi
echo "release-rollback.sh: $FAILURES assertion(s) failed" >&2
exit 1
