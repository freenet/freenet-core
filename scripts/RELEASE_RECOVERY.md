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

Two consequences for anyone recovering by hand:

- **Never create the GitHub release without `--draft`, and never un-draft it
  yourself.** Gate A only sees a draft; a release created published skips the
  gate entirely, which is the failure mode issue #5288 was filed for. The
  workflow's own comment says the same thing: "Do NOT un-draft it by hand."
  Gate A exists because auto-update was dead fleet-wide for two releases
  (v0.2.120, v0.2.121) and every other signal stayed green. A broken updater
  cannot ship its own fix.

- **Never `cargo publish` by hand before Gate A has passed.** The crates.io
  upload is the only step in a release that can never be undone. It used to run
  first, which is why v0.2.124 is permanently a draft with its crates already
  published: the gate blocked it, and the version number was already spent. It
  now runs inside `attach-to-release`, after the gate. Leave it there.

**Which is why a blocked release is usually cheap now.** If Gate A fails, no
crates were uploaded. Delete the tag, fix, re-cut on the corrected commit:

```bash
gh release delete vX.Y.Z --repo freenet/freenet-core --yes   # it is still a draft
git push --delete origin vX.Y.Z
git tag -d vX.Y.Z
```

Check `cargo search freenet --limit 1` first. If the crates for that version
*are* already on crates.io, the version is spent — do not re-tag it; cut the
next patch version instead.

## Quick Reference

```bash
# Check current state
gh pr view <PR_NUMBER> --json state,mergedAt
git tag -l "v*" | tail -5
gh release list --limit 5
gh release view vX.Y.Z --json isDraft,assets --jq '{isDraft, assets: [.assets[].name]}'
cargo search freenet --limit 1

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

```bash
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
gh run rerun <RUN_ID> --repo freenet/freenet-core --job <ATTACH_JOB_ID>
```

Only if that path is unavailable, publish by hand — and **check first that
Gate A actually passed**, because publishing is what makes the version
permanent:

```bash
# Confirm the pre-flight canary passed for this run before doing anything.
gh run view <RUN_ID> --repo freenet/freenet-core --log \
  | grep -F 'Gate A' | tail -20

cd ~/code/freenet/freenet-core/main
git fetch origin
# Publish from the release commit, not from whatever main is now (see Step 2).
git checkout "$RELEASE_SHA"

cargo publish -p freenet
sleep 30                # fdev resolves freenet from the registry
cargo publish -p fdev

cargo search freenet --limit 1

# ONLY now, and only if Gate A passed, un-draft:
gh release edit v0.1.X --repo freenet/freenet-core --draft=false
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
cargo search freenet --limit 1     # is this version already on crates.io?
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
# Check if already published
cargo search freenet --limit 1

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
- [ ] Crates published: `cargo search freenet --limit 1`
- [ ] Local gateway updated: `/usr/local/bin/freenet --version`
- [ ] Services running: `systemctl status freenet-gateway freenet-peer-01`
- [ ] Matrix announced: Check #freenet-locutus channel
