# Freenet Release Recovery Guide

When the release script fails mid-process, use this guide to complete the release manually.

## Read this before running anything

**Publication is gated, and the gate is not optional.** The order is:

```
bump PR merges
  -> tag vX.Y.Z pushed          (this is what triggers cross-compile.yml)
  -> DRAFT GitHub release created
  -> cross-compile.yml:  build -> attach binaries
                         -> Gate A: auto-update pre-flight   BLOCKING
                         -> cargo publish (crates.io)        IRREVERSIBLE
                         -> gh release edit --draft=false
```

Two consequences for anyone recovering by hand. Both are about ORDER, not about
never touching these commands — **Step 4 is the sanctioned way to do both by
hand, and it exists because sometimes you have to.** What is never allowed is
doing either one *ahead of the gate*.

- **Never create the GitHub release without `--draft`, and do not un-draft it
  yourself until Gate A has passed** (Step 4 shows how to confirm that, and is
  the one place that un-drafts by hand). Gate A only sees a draft; a release
  created published skips the gate entirely, which is the failure mode issue
  #5288 was filed for. The workflow's own comment says the same thing: "Do NOT
  un-draft it by hand" — meaning to bypass a gate that has not passed. Gate A
  exists because auto-update was dead fleet-wide for two releases (v0.2.120,
  v0.2.121) and every other signal stayed green. A broken updater cannot ship
  its own fix.

- **Never `cargo publish` by hand before Gate A has passed** — after it has, and
  only when the workflow cannot do it, Step 4 is the procedure. The crates.io
  upload is the only step in a release that can never be undone. It used to run
  first, which is why v0.2.124 is permanently a draft with its crates already
  published: the gate blocked it, and the version number was already spent. It
  now runs inside `attach-to-release`, after the gate. Leave it there unless
  the workflow itself is broken.

**Which is why a blocked release is usually cheap now.** If Gate A fails, no
crates were uploaded. Delete the tag, fix, re-cut on the corrected commit:

```bash
gh release delete vX.Y.Z --repo freenet/freenet-core --yes   # it is still a draft
git push --delete origin vX.Y.Z
git tag -d vX.Y.Z
```

Check whether that exact version is on crates.io first (see the Quick
Reference below — `cargo search` answers about the NEWEST version, not
yours). If the crates for that version
*are* already on crates.io, the version is spent — do not re-tag it; cut the
next patch version instead.

## Quick Reference

```bash
# Check current state
gh pr view <PR_NUMBER> --json state,mergedAt
git tag -l "v*" | tail -5
gh release list --limit 5
gh release view vX.Y.Z --json isDraft,assets --jq '{isDraft, assets: [.assets[].name]}'

# Is THIS version on crates.io? `cargo search` only ever reports a crate's
# newest version, and reads the search index, which lags the registry — so it
# can answer 'no' about a version that is published. Ask for the version.
curl -sS https://crates.io/api/v1/crates/freenet/0.1.X | jq -e '.version.num' >/dev/null && echo published || echo 'not published'

# Did the gate run, and what did it say?
gh run list --workflow=cross-compile.yml --branch vX.Y.Z --limit 3

# Resume from where you left off
# See detailed steps below based on where the failure occurred
```

## Release Steps & Recovery

### Step 1: PR Created but Not Merged

**Symptoms:**
- PR exists but isn't merged
- CI may be failing

**Recovery:**
1. Fix any CI failures
2. If conventional commits check failed:
   ```bash
   gh pr edit <PR_NUMBER> --title "chore: release X.Y.Z"
   ```
3. If other CI failures, fix code and push to release branch
4. Wait for PR to auto-merge or merge manually

### Step 2: PR Merged but Tag Not Created

**Symptoms:**
- PR is merged to main
- No git tag exists for version

**Recovery:**

Tag the **release commit**, not `main`. The release is cut from one pinned
commit (the version-bump PR's merge commit), and `main` may have moved past it
while the workflow was waiting on the merge queue. Tagging `main` by hand
reintroduces #5233 — the bug the pipeline now prevents.

```bash
cd ~/code/freenet/freenet-core/main
git fetch origin

# The bump PR is the "build: release X.Y.Z" one. The release.yml run log also
# prints "📌 Release pinned to <sha>".
RELEASE_SHA=$(gh pr view <BUMP_PR> --repo freenet/freenet-core \
  --json mergeCommit --jq '.mergeCommit.oid')

# Confirm it really is the bump commit before tagging it.
git show "$RELEASE_SHA:crates/core/Cargo.toml" | grep '^version'

git tag -a v0.1.X "$RELEASE_SHA" -m "Release v0.1.X"
git push origin v0.1.X
```

### Step 3: Tag Created but GitHub Release Missing

**Symptoms:**
- Git tag exists
- No GitHub release

**Recovery:**

`--draft` is REQUIRED. The pre-flight canary (Gate A) inspects the draft and
un-drafts it itself, after publishing to crates.io. A release created without
`--draft` is published the moment it exists: the gate never sees it, the
crates.io publish that lives behind the gate never runs, and the downstream
`release.published` cascade fires against a release with no binaries attached.

```bash
gh release create v0.1.X \
  --repo freenet/freenet-core \
  --title "v0.1.X" \
  --draft \
  --notes "$(gh api repos/freenet/freenet-core/releases/generate-notes \
    -f tag_name=v0.1.X -f target_commitish="$RELEASE_SHA" --jq .body)"
```

Then let the workflow finish the job. If it did not start (a lapsed
`RELEASE_PAT` suppresses the tag event — see AGENTS.md), start it by hand
against the tag; `attach-to-release` requires a `refs/tags/v*` ref, so the
`--ref` must be the tag, not a branch:

**Check nothing is already running for this tag first.** `cross-compile.yml`
sets `cancel-in-progress` for any ref that is not `main`, so dispatching against
a tag **cancels the in-flight run for that same tag**. The moment you reach for
this command — "did it start? is it stuck?" — is exactly when a run may be in
flight, and a cancellation can now land between the two crate publishes, or
between the publish and the un-draft. Both states are recoverable (Step 4
re-runs safely, skipping whatever already published), but do not create them for
no reason.

```bash
# Is one already running? If so, watch it instead of dispatching.
gh run list --repo freenet/freenet-core --workflow=cross-compile.yml \
  --branch v0.1.X --limit 5 --json databaseId,status,conclusion

gh workflow run cross-compile.yml --repo freenet/freenet-core --ref v0.1.X
```

### Step 4: Binaries Attached but Crates Not Published

**Symptoms:**
- Draft GitHub release exists with all assets
- Crates not on crates.io

**Recovery:**

Normally: re-run the `attach-to-release` job. Its publish step skips any
version already on crates.io, so a re-run is safe, and it keeps the publish and
the un-draft in the right order.

```bash
REPO=freenet/freenet-core
TAG=v0.1.X

RUN_ID=$(gh run list --repo "$REPO" --workflow=cross-compile.yml \
  --branch "$TAG" --limit 1 --json databaseId --jq '.[0].databaseId')

# `--job` wants the job's databaseId, which is NOT the number in the browser
# URL. Look it up rather than copying it from the address bar.
JOB_ID=$(gh run view "$RUN_ID" --repo "$REPO" --json jobs \
  --jq '.jobs[] | select(.name | startswith("Attach binaries")) | .databaseId')

gh run rerun --repo "$REPO" --job "$JOB_ID"
```

Only if that path is unavailable, publish by hand — and **check first that
Gate A actually passed**, because publishing is what makes the version
permanent:

```bash
REPO=freenet/freenet-core
TAG=v0.1.X

# Find the run for this tag.
RUN_ID=$(gh run list --repo "$REPO" --workflow=cross-compile.yml \
  --branch "$TAG" --limit 1 --json databaseId --jq '.[0].databaseId')

# Confirm Gate A actually PASSED, by reading the step's CONCLUSION.
#
# Do NOT confirm this by grepping the log for "Gate A". The canary prints
# "=== Gate A: auto-update pre-flight on the binary about to ship ===" as the
# FIRST line of cmd_preflight, before it checks anything, so that grep returns
# the same output whether the gate passed, blocked on a real fault, or gave no
# verdict at all. It looks like confirmation and confirms nothing — directly
# above an irreversible publish.
gh run view "$RUN_ID" --repo "$REPO" --json jobs --jq '
  .jobs[]
  | select(.name | startswith("Attach binaries"))
  | .steps[]
  | select(.name | startswith("Auto-update pre-flight"))
  | "\(.name): \(.conclusion)"'
```

**That must print `success`.** `failure`, `cancelled`, `skipped` — or no line
at all, which means the step never ran — all mean Gate A did not pass. Stop and
go to Step 4b. Only continue past this point on `success`:

```bash
cd ~/code/freenet/freenet-core/main
git fetch origin
# Publish from the release commit, not from whatever main is now (see Step 2).
git checkout "$RELEASE_SHA"

# Per crate, because a partial publish is the usual reason to be here:
# RELEASING.md routes "fdev failed but freenet succeeded" to this step, and an
# unconditional `cargo publish -p freenet` would just error on the crate that
# already worked. Mirrors what attach-to-release and release.sh both do.
published() {  # published <crate> <version>
  local body
  body="$(curl -sS --max-time 30 "https://crates.io/api/v1/crates/$1/$2")" || return 1
  jq -e '.version.num? // empty' >/dev/null 2>&1 <<<"$body"
}

published freenet 0.1.X \
  && echo "freenet 0.1.X already published, skipping" \
  || { cargo publish -p freenet; sleep 30; }   # fdev resolves freenet from the registry

published fdev 0.Y.Z \
  && echo "fdev 0.Y.Z already published, skipping" \
  || cargo publish -p fdev

# Verify both, by exact version. `cargo search` reports only a crate's NEWEST
# version and reads the search index, which lags the registry — it can say "no"
# about a version that is published.
published freenet 0.1.X && published fdev 0.Y.Z && echo "both on crates.io"

# ONLY now, and only if Gate A reported success above, un-draft:
gh release edit "$TAG" --repo "$REPO" --draft=false
```

### Step 4b: Gate A blocked the release

**Symptoms:**
- Draft release with all assets attached
- `attach-to-release` red at the "Auto-update pre-flight canary" step
- Matrix notification saying the pre-flight did not pass

**This is the gate working.** The binary about to ship cannot read GitHub's
release tags, or the run could not prove that it can. Do not un-draft.

First establish which side of the irreversible step you are on:

```bash
# Is THIS version already on crates.io? (not `cargo search` — see Quick Reference)
curl -sS https://crates.io/api/v1/crates/freenet/0.1.X | jq -e '.version.num' >/dev/null && echo published || echo 'not published'
```

- **Not published** (the normal case, since the publish runs after the canary):
  nothing irreversible has happened. Read the job log — the canary dumps the
  node's own output when it blocks — then fix, delete the tag and draft, and
  re-cut on the corrected commit (commands at the top of this file).
- **Already published**: the version number is spent. Do not re-tag it. Fix,
  and cut the next patch version.

If the canary reported UNVERIFIED rather than a fault (GitHub unreachable, or
no verdict inside its window), that is infrastructure, not a bug in the binary.
Re-run the job. An unverified gate is still not a passed gate — do not
un-draft to work around it.

### Step 5: Crates Published but Local Not Deployed

**Symptoms:**
- Everything published
- Local gateway not updated

**Recovery:**
```bash
cd ~/code/freenet/freenet-core/main
cargo build --release --bin freenet

# Deploy to gateway only
./scripts/deploy-local-gateway.sh

# Deploy to all instances (gateway + 10 peers)
./scripts/deploy-local-gateway.sh --all-instances
```

### Step 6: Deployed but Matrix Not Announced

**Symptoms:**
- Release complete
- No Matrix announcement

**Recovery:**
```bash
matrix-commander -r '#freenet-locutus:matrix.org' -m "🎉 **Freenet v0.1.X Released!**

📦 Published to crates.io:
  • freenet v0.1.X
  • fdev v0.Y.Z

🔗 Release: https://github.com/freenet/freenet-core/releases/tag/v0.1.X

[AI-assisted release announcement]"
```

## Common Issues

### Issue: "Text file busy" during deployment

**Cause:** Systemd services have `Restart=always` and keep respawning

**Solution:**
```bash
# Stop all services and disable auto-restart
sudo systemctl stop freenet-gateway freenet-peer-{01..10}
sudo systemctl disable freenet-gateway freenet-peer-{01..10}

# Wait for binary to be released
while sudo lsof /usr/local/bin/freenet; do sleep 1; done

# Deploy new binary
sudo rm /usr/local/bin/freenet
sudo cp target/release/freenet /usr/local/bin/freenet

# Re-enable and start
sudo systemctl enable freenet-gateway freenet-peer-{01..10}
sudo systemctl start freenet-gateway freenet-peer-{01..10}
```

### Issue: Conventional Commits CI failure

**Cause:** PR title doesn't follow conventional commit format

**Solution:**
```bash
gh pr edit <PR_NUMBER> --title "chore: release X.Y.Z"

# Trigger CI rerun
git checkout release/vX.Y.Z
git commit --allow-empty -m "chore: trigger CI rerun"
git push origin release/vX.Y.Z
```

### Issue: Crates.io publishing fails

**Cause:** Version already published, credentials issue, or dependency problems

**Solution:**
```bash
# Check if THIS version is already published. Not `cargo search` -- it reports
# only the NEWEST version and reads the lagging search index (see the Quick
# Reference); here that would answer about some other version entirely.
curl -sS https://crates.io/api/v1/crates/freenet/0.1.X \
  | jq -e '.version.num' >/dev/null && echo published || echo 'not published'

# Verify credentials
cargo login

# Check for dependency issues
cargo package --list -p freenet
cargo publish --dry-run -p freenet
```

## Full Manual Release Process

If you need to do everything manually:

Steps 5 and 6 below are the ones the gate owns. Push the tag, create the
release as a DRAFT, and let `cross-compile.yml` do the rest — it is the only
path that runs Gate A before the irreversible publish.

```bash
# 1. Create PR and merge
cd ~/code/freenet/freenet-core/main
# Edit Cargo.toml versions manually
git checkout -b release/v0.1.X
git add -A
git commit -m "chore: release 0.1.X"
git push origin release/v0.1.X
gh pr create --title "chore: release 0.1.X" --body "Release v0.1.X" --base main

# 2. Wait for CI and merge (or use gh pr merge --auto)

# 3. Create tag — on the bump PR's merge commit, NOT on main (see Step 2)
git fetch origin
RELEASE_SHA=$(gh pr view <BUMP_PR> --repo freenet/freenet-core \
  --json mergeCommit --jq '.mergeCommit.oid')
git tag -a v0.1.X "$RELEASE_SHA" -m "Release v0.1.X"
git push origin v0.1.X          # this is what triggers cross-compile.yml

# 4. Create the GitHub release as a DRAFT (see Step 3 for why --draft matters)
gh release create v0.1.X --repo freenet/freenet-core --draft --generate-notes

# 5. Let cross-compile.yml attach the binaries, run Gate A, publish to
#    crates.io and un-draft. Watch it; do not race it:
gh run watch "$(gh run list --workflow=cross-compile.yml \
  --branch v0.1.X --limit 1 --json databaseId --jq '.[0].databaseId')"

# 6. Verify it got there, rather than assuming
gh release view v0.1.X --json isDraft --jq '.isDraft'   # must be false
# `cargo search` is CORRECT here and should not be "fixed": after a successful
# release the newest published version IS the one just cut, which is exactly
# what it reports. The rule is `cargo search` may only answer questions about
# the NEWEST version -- fine here, wrong wherever the question is "is version X
# published?" (see Step 4b and the Quick Reference).
cargo search freenet --limit 1

# 7. Deploy locally
cargo build --release --bin freenet
./scripts/deploy-local-gateway.sh --all-instances

# 8. Announce to Matrix
matrix-commander -r '#freenet-locutus:matrix.org' -m "..."
```

## Rollback

If you need to rollback a release:

```bash
./scripts/release-rollback.sh --version 0.1.X

# To also yank from crates.io (cannot be undone!)
./scripts/release-rollback.sh --version 0.1.X --yank-crates
```

## Verification Checklist

After recovery, verify:

- [ ] PR merged: `gh pr view <NUMBER> --json state`
- [ ] Tag exists: `git tag -l "v0.1.X"`
- [ ] GitHub release: `gh release view v0.1.X`
- [ ] Crates published (by exact version, not `cargo search`):
      `curl -sS https://crates.io/api/v1/crates/freenet/0.1.X | jq -e .version.num`
- [ ] Local gateway updated: `/usr/local/bin/freenet --version`
- [ ] Services running: `systemctl status freenet-gateway freenet-peer-01`
- [ ] Matrix announced: Check #freenet-locutus channel
