# Freenet Release Recovery Guide

When the release script fails mid-process, use this guide to complete the release manually.

## Quick Reference

```bash
# Check current state
gh pr view <PR_NUMBER> --json state,mergedAt
git tag -l "v*" | tail -5
gh release list --limit 5
cargo search freenet --limit 1

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
```bash
cd ~/code/freenet/freenet-core/main
git pull origin main
git tag v0.1.X
git push origin v0.1.X
```

### Step 3: Tag Created but GitHub Release Missing

**Symptoms:**
- Git tag exists
- No GitHub release

**Recovery:**
```bash
gh release create v0.1.X \
  --repo freenet/freenet-core \
  --title "v0.1.X" \
  --notes "$(gh api repos/freenet/freenet-core/releases/generate-notes \
    -f tag_name=v0.1.X -f target_commitish=main --jq .body)"
```

### Step 4: Release Created but Crates Not Published

**Symptoms:**
- GitHub release exists
- Crates not on crates.io

**Recovery:**
```bash
cd ~/code/freenet/freenet-core/main
git pull origin main

# Publish freenet crate
cargo publish -p freenet

# Publish fdev crate
cargo publish -p fdev

# Verify
cargo search freenet --limit 1
```

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

# 3. Create tag
git checkout main
git pull
git tag v0.1.X
git push origin v0.1.X

# 4. Create GitHub release
gh release create v0.1.X --repo freenet/freenet-core --generate-notes

# 5. Publish crates
cargo publish -p freenet
cargo publish -p fdev

# 6. Deploy locally
cargo build --release --bin freenet
./scripts/deploy-local-gateway.sh --all-instances

# 7. Announce to Matrix
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
