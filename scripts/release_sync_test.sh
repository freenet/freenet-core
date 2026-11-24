#!/usr/bin/env bash
# Tests for scripts/release_sync.sh (release-sync.yml's merge-back), run in
# throwaway git repos with a local bare `origin` and stubbed gh and cargo.
#
# Pins: main below the release gets a bump commit of exactly the version files
# on `release-sync/vX`, force-pushed, with ONE PR titled
# `build: sync main to X after release` (never `build: release ...`) and
# auto-merge without --squash; fdev comes from the tag's tree; a re-run after
# main moved regenerates the branch on the new main and reuses the PR; main at
# or above X does nothing; lower-version sync PRs are closed as superseded and
# higher ones are left alone. Also pins release-sync.yml's trigger and wiring.
#
# Run manually with: bash scripts/release_sync_test.sh

set -euo pipefail
unset GIT_DIR GIT_WORK_TREE GH_TOKEN GITHUB_TOKEN
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SYNC="$SCRIPT_DIR/release_sync.sh"
SYNC_YML="$REPO_ROOT/.github/workflows/release-sync.yml"
command -v jq > /dev/null || { echo "FAIL - this test needs jq" >&2; exit 2; }
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

FAILURES=0
check() {
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

STUBS="$TEST_TMP/bin"
mkdir -p "$STUBS"
# gh answers from $STUB_DIR files, applying the script's --jq with real jq.
cat > "$STUBS/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_DIR/calls"
jqarg=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    [[ "${args[i]}" == --jq ]] && jqarg="${args[i+1]}"
done
case "$1 $2" in
    "pr list")
        if [[ "$*" == *"--head "* ]]; then json=$(cat "$STUB_DIR/head_prs"); else
            # open_prs may hold one answer per call, separated by lines of ---;
            # the last answer repeats.
            n=$(cat "$STUB_DIR/open_n" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$STUB_DIR/open_n"
            json=$(awk -v n="$n" 'BEGIN{k=1} /^---$/{k++; next} {part[k]=part[k] $0 "\n"} END{if (n > k) n = k; printf "%s", part[n]}' "$STUB_DIR/open_prs")
        fi ;;
    "pr create")
        # Optional hook: something lands on main while the sync runs.
        if [ -x "$STUB_DIR/on_create" ]; then "$STUB_DIR/on_create" >> "$STUB_DIR/hook.log" 2>&1; fi
        echo "https://github.com/freenet/freenet-core/pull/777"; exit 0 ;;
    "pr view") json=$(cat "$STUB_DIR/view") ;;
    "pr merge"|"pr close") exit 0 ;;
    *) echo "unexpected gh $*" >&2; exit 99 ;;
esac
jq -r "${jqarg:-.}" <<< "$json"
EOF
# cargo: `update` touches Cargo.lock so the commit carries a lockfile, like the
# real one; `metadata` refreshes the nested lock.
cat > "$STUBS/cargo" <<'EOF'
#!/usr/bin/env bash
echo "cargo $*" >> "$STUB_DIR/calls"
case "$1" in
    update) echo "# lock $(grep -m1 '^version' crates/core/Cargo.toml)" > Cargo.lock ;;
    metadata) echo "# ping lock" >> apps/freenet-ping/Cargo.lock ;;
esac
EOF
chmod +x "$STUBS"/*

ORIGIN="$TEST_TMP/origin.git"
SEED="$TEST_TMP/seed"
git init -q --bare -b main "$ORIGIN"
mkdir -p "$SEED/crates/core" "$SEED/crates/fdev" "$SEED/apps/freenet-ping"
cp "$REPO_ROOT/crates/core/Cargo.toml" "$SEED/crates/core/"
cp "$REPO_ROOT/crates/fdev/Cargo.toml" "$SEED/crates/fdev/"
echo "# lock" > "$SEED/Cargo.lock"
echo "# ping lock" > "$SEED/apps/freenet-ping/Cargo.lock"
G() { git -C "$SEED" -c user.email=t@example.invalid -c user.name=t "$@"; }
G init -q -b main
G add -A
G commit -q -m base
# Pin main at known versions so the test does not depend on the real ones.
sed -i 's/^version = ".*"/version = "0.2.100"/' "$SEED/crates/core/Cargo.toml"
sed -i 's/^version = ".*"/version = "0.3.200"/' "$SEED/crates/fdev/Cargo.toml"
G commit -qam "main at 0.2.100"
G remote add origin "$ORIGIN"
G push -q origin main
# The release: a commit off main carrying 0.2.101 / fdev 0.3.205, tagged.
G checkout -q -b release/v0.2.101
( cd "$SEED" && bash "$SCRIPT_DIR/apply_version_bump.sh" 0.2.101 0.3.205 > /dev/null )
G commit -qam "chore: bump versions to 0.2.101"
G tag -a v0.2.101 -m v0.2.101
G push -q origin release/v0.2.101 v0.2.101
G checkout -q main

run_sync() {
    # run_sync <workdir> <version> -> rc; stub answers already in $STUB_DIR
    local dir="$1" version="$2" rc=0
    ( cd "$dir" && PATH="$STUBS:$PATH" VERSION="$version" GITHUB_REPOSITORY=freenet/freenet-core \
        GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@example.invalid GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@example.invalid \
        bash "$SYNC" ) > "$STUB_DIR/log" 2>&1 || rc=$?
    echo "$rc"
}
fresh_clone() { rm -rf "$1"; git clone -q "$ORIGIN" "$1"; git -C "$1" checkout -q main; }
remote_file() { git -C "$ORIGIN" show "release-sync/v0.2.101:$1"; }
pkg_version() { awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0} p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}'; }

export STUB_DIR="$TEST_TMP/stub"
reset_stub() {
    rm -rf "$STUB_DIR" && mkdir -p "$STUB_DIR"
    echo "${1:-[]}" > "$STUB_DIR/open_prs"
    echo "${2:-[]}" > "$STUB_DIR/head_prs"
    echo '{"autoMergeRequest":null}' > "$STUB_DIR/view"
    : > "$STUB_DIR/calls"
}

# --- 1. main below the release ------------------------------------------------
# The second listing (after our PR opens) no longer shows the closed #11.
reset_stub $'[{"number":11,"headRefName":"release-sync/v0.2.99"},{"number":13,"headRefName":"fix/x"}]\n---\n[{"number":777,"headRefName":"release-sync/v0.2.101"},{"number":13,"headRefName":"fix/x"}]'
fresh_clone "$TEST_TMP/w1"
check "main below the release: the sync succeeds" "$(run_sync "$TEST_TMP/w1" 0.2.101)" "0"
[ -s "$STUB_DIR/log" ] && grep -q '::error' "$STUB_DIR/log" && cat "$STUB_DIR/log" >&2
check "the branch carries freenet 0.2.101" "$(remote_file crates/core/Cargo.toml | pkg_version)" "0.2.101"
check "fdev comes from the tag's tree (0.3.205), not main's +1" "$(remote_file crates/fdev/Cargo.toml | pkg_version)" "0.3.205"
check "fdev's binstall URLs point at the release" "$(remote_file crates/fdev/Cargo.toml | grep -c 'releases/download/v0.2.101/fdev-')" "2"
check "the branch is one commit on top of main" \
    "$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101^)" "$(git -C "$ORIGIN" rev-parse main)"
check "the sync commit touches only version files and lockfiles" \
    "$(git -C "$ORIGIN" diff --name-only main release-sync/v0.2.101 | paste -sd' ' -)" \
    "Cargo.lock apps/freenet-ping/Cargo.lock crates/core/Cargo.toml crates/fdev/Cargo.toml"
check "the commit subject is the PR title" \
    "$(git -C "$ORIGIN" log -1 --format=%s release-sync/v0.2.101)" "build: sync main to 0.2.101 after release"
check "one PR is opened, with the sync title, into main" \
    "$(grep -c '^gh pr create --repo freenet/freenet-core --base main --head release-sync/v0.2.101 --title build: sync main to 0.2.101 after release ' "$STUB_DIR/calls")" "1"
check "the PR title does not start with 'build: release' (the legacy full-suite merge-group gate)" \
    "$(grep '^gh pr create' "$STUB_DIR/calls" | grep -c -- '--title build: release')" "0"
check "auto-merge is enabled on the new PR" "$(grep -cx 'gh pr merge 777 --repo freenet/freenet-core --auto' "$STUB_DIR/calls")" "1"
check "auto-merge never passes --squash (the queue owns the strategy, v0.2.93)" \
    "$(grep '^gh pr merge' "$STUB_DIR/calls" | grep -c -- '--squash' || true)" "0"
check "the lower-version sync PR is closed as superseded" \
    "$(grep -c '^gh pr close 11 --repo freenet/freenet-core --delete-branch --comment Superseded by the sync to 0.2.101.' "$STUB_DIR/calls")" "1"
check "unrelated PRs are left alone" \
    "$(grep -cE '^gh pr close 13 ' "$STUB_DIR/calls" || true)" "0"
check "lockfiles are refreshed" "$(grep -cE '^cargo (update --workspace|metadata .*apps/freenet-ping/Cargo.toml)' "$STUB_DIR/calls")" "2"

# --- 1b. a higher sync is already open: this one is superseded -----------------
BEFORE_1B=$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101)
reset_stub '[{"number":777,"headRefName":"release-sync/v0.2.101"},{"number":12,"headRefName":"release-sync/v0.2.102"}]'
fresh_clone "$TEST_TMP/w1b"
check "a higher open sync: the lower sync succeeds as a no-op" "$(run_sync "$TEST_TMP/w1b" 0.2.101)" "0"
check "a higher open sync: nothing is pushed" "$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101)" "$BEFORE_1B"
check "a higher open sync: no PR is opened, no cargo" "$(grep -cE '^(cargo|gh pr create)' "$STUB_DIR/calls" || true)" "0"
check "a higher open sync: our own open PR is closed as superseded" \
    "$(grep -c '^gh pr close 777 --repo freenet/freenet-core --delete-branch --comment Superseded by the open sync to 0.2.102.' "$STUB_DIR/calls")" "1"
check "a higher open sync: the higher PR is left alone" "$(grep -c '^gh pr close 12 ' "$STUB_DIR/calls" || true)" "0"

# A higher sync that opens its PR WHILE this one runs (overlapping per-version
# runs): the second superseding pass, after our PR is opened, closes ours.
reset_stub $'[]\n---\n[{"number":777,"headRefName":"release-sync/v0.2.101"},{"number":12,"headRefName":"release-sync/v0.2.102"}]'
fresh_clone "$TEST_TMP/w1c"
check "a racing higher sync: succeeds" "$(run_sync "$TEST_TMP/w1c" 0.2.101)" "0"
check "a racing higher sync: our just-opened PR is closed after the fact" \
    "$(grep -c '^gh pr close 777 .*Superseded by the open sync to 0.2.102.' "$STUB_DIR/calls")" "1"

# --- 1d. main reaches the version while the sync runs ---------------------------
# (another sync or a hand bump merges between the first read and auto-merge).
# A PR bumping main to a version it already has must be closed, not armed.
cat > "$TEST_TMP/bump_main.sh" <<HOOK
#!/usr/bin/env bash
set -e
d=\$(mktemp -d)
git clone -q "$ORIGIN" "\$d/m"
sed -i 's/^version = ".*"/version = "0.2.101"/' "\$d/m/crates/core/Cargo.toml"
git -C "\$d/m" -c user.email=t@example.invalid -c user.name=t commit -qam "main reaches 0.2.101"
git -C "\$d/m" push -q origin main
HOOK
chmod +x "$TEST_TMP/bump_main.sh"
MAIN_BEFORE_1D=$(git -C "$ORIGIN" rev-parse main)
reset_stub
cp "$TEST_TMP/bump_main.sh" "$STUB_DIR/on_create"
fresh_clone "$TEST_TMP/w1d"
check "main reaching the version mid-sync: succeeds" "$(run_sync "$TEST_TMP/w1d" 0.2.101)" "0"
check "main reaching the version mid-sync: the hook really moved main" \
    "$([ "$(git -C "$ORIGIN" rev-parse main)" != "$MAIN_BEFORE_1D" ] && echo moved || echo "not moved: $(cat "$STUB_DIR/hook.log")")" "moved"
check "main reaching the version mid-sync: the PR is closed" \
    "$(grep -c '^gh pr close 777 --repo [^ ]* --delete-branch .*main is already at 0.2.101' "$STUB_DIR/calls")" "1"
check "main reaching the version mid-sync: auto-merge is never armed" "$(grep -c '^gh pr merge' "$STUB_DIR/calls" || true)" "0"
# main ABOVE the release by the time of the re-read: closed the same way.
sed 's/0\.2\.101/0.2.102/g' "$TEST_TMP/bump_main.sh" > "$TEST_TMP/bump_main_above.sh"
chmod +x "$TEST_TMP/bump_main_above.sh"
git -C "$SEED" fetch -q origin main
git -C "$SEED" reset -q --hard origin/main
git -C "$SEED" checkout -q "$MAIN_BEFORE_1D" -- crates/core/Cargo.toml
G commit -q --allow-empty -am "main back at 0.2.100 (1d above)"
G push -q origin main
reset_stub
cp "$TEST_TMP/bump_main_above.sh" "$STUB_DIR/on_create"
fresh_clone "$TEST_TMP/w1d2"
check "main passing the version mid-sync: succeeds" "$(run_sync "$TEST_TMP/w1d2" 0.2.101)" "0"
check "main passing the version mid-sync: the PR is closed" \
    "$(grep -c '^gh pr close 777 --repo [^ ]* --delete-branch .*main is already at 0.2.102' "$STUB_DIR/calls")" "1"
check "main passing the version mid-sync: auto-merge is never armed" "$(grep -c '^gh pr merge' "$STUB_DIR/calls" || true)" "0"
# main's manifest unreadable at the re-read: refuse, leave the PR unarmed.
cat > "$TEST_TMP/break_main.sh" <<HOOK
#!/usr/bin/env bash
set -e
d=\$(mktemp -d)
git clone -q "$ORIGIN" "\$d/m"
printf 'not a manifest\\n' > "\$d/m/crates/core/Cargo.toml"
git -C "\$d/m" -c user.email=t@example.invalid -c user.name=t commit -qam "garbage manifest"
git -C "\$d/m" push -q origin main
HOOK
chmod +x "$TEST_TMP/break_main.sh"
git -C "$SEED" fetch -q origin main
git -C "$SEED" reset -q --hard origin/main
git -C "$SEED" checkout -q "$MAIN_BEFORE_1D" -- crates/core/Cargo.toml
G commit -q --allow-empty -am "main back at 0.2.100 (1d unreadable)"
G push -q origin main
reset_stub
cp "$TEST_TMP/break_main.sh" "$STUB_DIR/on_create"
fresh_clone "$TEST_TMP/w1d3"
check "main unreadable at the re-read: fails" "$(run_sync "$TEST_TMP/w1d3" 0.2.101)" "1"
check "main unreadable at the re-read: auto-merge is never armed" "$(grep -c '^gh pr merge' "$STUB_DIR/calls" || true)" "0"
# Put main back below the release for the cases that follow.
git -C "$SEED" fetch -q origin main
git -C "$SEED" reset -q --hard origin/main
git -C "$SEED" checkout -q "$MAIN_BEFORE_1D" -- crates/core/Cargo.toml
G commit -q --allow-empty -am "main back at 0.2.100"
G push -q origin main

# --- 2. re-run after main moved: regenerated and force-pushed, PR reused ------
G commit -q --allow-empty -m "fix: something landed after the release"
G push -q origin main
reset_stub '[{"number":777,"headRefName":"release-sync/v0.2.101"}]' '[{"number":777}]'
echo '{"autoMergeRequest":{"enabledAt":"x"}}' > "$STUB_DIR/view"
fresh_clone "$TEST_TMP/w2"
check "re-run: the sync succeeds" "$(run_sync "$TEST_TMP/w2" 0.2.101)" "0"
check "re-run: the branch is regenerated on the new main" \
    "$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101^)" "$(git -C "$ORIGIN" rev-parse main)"
check "re-run: no second PR is opened" "$(grep -c '^gh pr create' "$STUB_DIR/calls" || true)" "0"
check "re-run: auto-merge already on is not re-requested" "$(grep -c '^gh pr merge' "$STUB_DIR/calls" || true)" "0"
check "re-run: its own open PR is not closed" "$(grep -c '^gh pr close' "$STUB_DIR/calls" || true)" "0"

# --- 3. main already at or above the release -----------------------------------
sed -i 's/^version = ".*"/version = "0.2.101"/' "$SEED/crates/core/Cargo.toml"
G commit -qam "build: sync main to 0.2.101 after release"
G push -q origin main
BEFORE=$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101)
reset_stub '[{"number":777,"headRefName":"release-sync/v0.2.101"}]'
fresh_clone "$TEST_TMP/w3"
check "main at the release: no-op succeeds" "$(run_sync "$TEST_TMP/w3" 0.2.101)" "0"
check "main at the release: nothing is pushed" "$(git -C "$ORIGIN" rev-parse release-sync/v0.2.101)" "$BEFORE"
check "main at the release: no PR is opened" "$(grep -c '^gh pr create' "$STUB_DIR/calls" || true)" "0"
check "main at the release: a leftover sync PR for it is closed" \
    "$(grep -c '^gh pr close 777 ' "$STUB_DIR/calls")" "1"
reset_stub
fresh_clone "$TEST_TMP/w4"
check "main ABOVE the release (an older release published late): no-op" "$(run_sync "$TEST_TMP/w4" 0.2.50)" "0"
check "main above the release: no cargo, no PR" "$(grep -cE '^(cargo|gh pr create)' "$STUB_DIR/calls" || true)" "0"
reset_stub
check "a malformed version refuses" "$(run_sync "$TEST_TMP/w4" 0.2)" "1"

# --- 4. the workflow -------------------------------------------------------------
check "release-sync.yml parses and fires on release published and workflow_dispatch" \
    "$(python3 - "$SYNC_YML" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
on = s.split("\non:\n", 1)[1].split("\n\n", 1)[0]
ok = re.search(r"^  release:\n    types: \[published\]$", on, re.M) and re.search(r"^  workflow_dispatch:$", on, re.M)
print("yes" if ok else "no")
PY
)" "yes"
check "release-sync.yml runs the tested script" "$(grep -cE '^[[:space:]]*run: bash scripts/release_sync\.sh[[:space:]]*$' "$SYNC_YML")" "1"
# shellcheck disable=SC2016  # the expressions are matched literally
check "release-sync.yml pushes and opens the PR with RELEASE_PAT (coalesced)" \
    "$(grep -cE '^[[:space:]]*(token|GH_TOKEN): \$\{\{ secrets\.RELEASE_PAT \|\| secrets\.GITHUB_TOKEN \}\}[[:space:]]*$' "$SYNC_YML")" "2"
# shellcheck disable=SC2016  # matched literally
check "release-sync.yml's concurrency group is per version (a pending newer sync is never cancelled)" \
    "$(grep -cE '^  group: release-sync-\$\{\{ ' "$SYNC_YML")" "1"
# The key, evaluated for each way a version arrives, must come out the same
# for one version (a dispatch and the event share a group) and differ between
# versions (a newer sync is never queued behind, or cancelled by, an older one).
CONC_EXPR=$(sed -n 's/^  group: release-sync-\${{ \(.*\) }}$/\1/p' "$SYNC_YML")
conc_key() {
    python3 - "$CONC_EXPR" "$1" "$2" <<'PYK'
import re, sys
expr, tag, inp = sys.argv[1:]
ctx = {"github.event.release.tag_name": tag, "inputs.version": inp}
TOK = re.compile(r"\s*(\|\||&&|\(|\)|,|'[^']*'|[A-Za-z_][\w.]*)")
toks, pos = [], 0
while pos < len(expr):
    m = TOK.match(expr, pos)
    if not m:
        sys.exit(f"cannot parse {expr!r}")
    toks.append(m.group(1))
    pos = m.end()
i = 0
def peek():
    return toks[i] if i < len(toks) else None
def take():
    global i
    i += 1
    return toks[i - 1]
def falsy(v):
    return v in ("", False, None)
def or_():
    v = and_()
    while peek() == "||":
        take()
        r = and_()
        v = r if falsy(v) else v
    return v
def and_():
    v = atom()
    while peek() == "&&":
        take()
        r = atom()
        v = v if falsy(v) else r
    return v
def atom():
    t = take()
    if t == "(":
        v = or_()
        take()
        return v
    if t.startswith("'"):
        return t[1:-1]
    if peek() == "(":
        take()
        args = []
        while peek() != ")":
            args.append(or_())
            if peek() == ",":
                take()
        take()
        if t == "startsWith":
            return str(args[0]).lower().startswith(str(args[1]).lower())
        if t == "format":
            return re.sub(r"\{(\d+)\}", lambda m: str(args[1 + int(m.group(1))]), args[0])
        sys.exit(f"unsupported {t}")
    return ctx.get(t, "")
print(or_())
PYK
}
check "concurrency key: release event v0.2.165" "$(conc_key v0.2.165 '')" "v0.2.165"
check "concurrency key: dispatch 0.2.165 matches the event" "$(conc_key '' 0.2.165)" "v0.2.165"
check "concurrency key: dispatch v0.2.165 matches the event" "$(conc_key '' v0.2.165)" "v0.2.165"
check "concurrency key: versions differ" "$(conc_key '' 0.2.166)" "v0.2.166"
check "release-sync.yml never cancels a sync in progress" "$(grep -cE '^  cancel-in-progress: false$' "$SYNC_YML")" "1"
# Without RELEASE_PAT the PR starts no CI and can never merge: the first step
# must refuse. Run it as the workflow does, both ways.
LINTER="$REPO_ROOT/.github/scripts/check_release_dry_run.py"
PAT_STEP=$(python3 "$LINTER" --dump-step "$SYNC_YML" sync "Check RELEASE_PAT is configured" | sed '/^@@ENV@@$/,$d')
pat_rc() { local rc=0; RELEASE_PAT_PRESENT="$1" bash -c "$PAT_STEP" > "$TEST_TMP/pat.out" 2>&1 || rc=$?; echo "rc=$rc"; }
check "release-sync refuses loudly without RELEASE_PAT" "$(pat_rc false) $(grep -c '^::error title=RELEASE_PAT not set' "$TEST_TMP/pat.out")" "rc=1 1"
check "release-sync proceeds with RELEASE_PAT" "$(pat_rc true)" "rc=0"
check "the RELEASE_PAT check is the sync job's first step" \
    "$(python3 - "$LINTER" "$SYNC_YML" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("lint", sys.argv[1]); m = importlib.util.module_from_spec(spec)
sys.modules["lint"] = m
spec.loader.exec_module(m)
jobs, _ = m.parse_workflow(open(sys.argv[2]).read())
job = next(j for j in jobs if j.name == "sync")
first = job.steps[0]
print(first.label, "|", "if" in first.keys, "|", first.env.get("RELEASE_PAT_PRESENT", ""))
PY
)" "Check RELEASE_PAT is configured | False | \${{ secrets.RELEASE_PAT != '' }}"
# shellcheck disable=SC2016  # matched literally
check "release-sync.yml skips pre-releases" "$(grep -c 'PRERELEASE: \${{ github.event.release.prerelease }}' "$SYNC_YML")" "1"
check "CI runs this test" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/release_sync_test\.sh[[:space:]]*$' "$REPO_ROOT/.github/workflows/ci.yml")" "1"

if [ "$FAILURES" -ne 0 ]; then
    echo "$FAILURES failure(s)" >&2
    exit 1
fi
echo "All release_sync tests passed."
