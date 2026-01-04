# Proposed: Merge-Base Baseline Strategy

## Problem

Current: PRs compare against "most recent main baseline" via cache `restore-keys`.

If commits land on main with performance improvements, your PR (without those changes)
shows as "regressed" even though YOU didn't change anything.

## Solution

Compare against the **merge-base commit** - the point where your branch diverged from main.

### Implementation

```yaml
- name: Determine Baseline Commit
  id: baseline-commit
  run: |
    if [ "${{ github.event_name }}" == "pull_request" ]; then
      # For PRs: use merge-base (where branch diverged from main)
      BASE_SHA=$(git merge-base origin/${{ github.base_ref }} HEAD)
      echo "Using merge-base: $BASE_SHA"
      echo "sha=$BASE_SHA" >> $GITHUB_OUTPUT
    else
      # For main branch: use current commit
      echo "Using current commit: ${{ github.sha }}"
      echo "sha=${{ github.sha }}" >> $GITHUB_OUTPUT
    fi

- name: Download Baseline from Merge-Base
  uses: actions/cache/restore@v5
  with:
    path: target/criterion
    key: criterion-baseline-main-${{ runner.os }}-${{ steps.baseline-commit.outputs.sha }}
    restore-keys: |
      criterion-baseline-main-${{ runner.os }}-
```

### Impact

- ✅ PRs always compare against the performance when they branched
- ✅ Eliminates "you don't have the new improvements" false positives
- ⚠️ Baseline might be old if branch is stale (but that's a real signal to rebase!)
