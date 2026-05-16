# Cutting a Freenet release

The release pipeline is fully automated. Any maintainer with workflow-run access
can cut a release by triggering one workflow; everything else cascades:
crates.io publish, GitHub release with binaries, gateway updates, and the
Matrix/River announcements.

This doc covers the developer-facing happy path and the few places things can
go wrong.

## Quickstart

For a routine patch release, just run the workflow with no input — it
auto-bumps the patch from the latest crates.io version:

```bash
gh workflow run release.yml --repo freenet/freenet-core
```

For a minor or major bump (or to pin a specific version), pass `version`:

```bash
gh workflow run release.yml \
    --repo freenet/freenet-core \
    --field version=0.3.0
```

That's it. Watch progress at <https://github.com/freenet/freenet-core/actions/workflows/release.yml>.

The auto-bump reads `https://crates.io/api/v1/crates/freenet` to find the
highest released version and increments the patch. So after `0.2.59` ships,
the next bare run cuts `0.2.60`. Minor / major bumps are intentionally
explicit — they're not auto-decidable from commit history.

Within ~30–60 minutes you should see:

1. The `Release` workflow's `validate` → `update_versions` → `wait_for_pr` →
   `publish_crates` → `create_release` jobs complete.
2. An auto-created bump PR titled `build: release X.Y.Z` that merges itself.
3. `freenet` and `fdev` published to crates.io.
4. A `vX.Y.Z` git tag pushed, which triggers `Build and Cross-Compile`.
5. Cross-compile builds Linux musl + macOS (Intel + arm64) + Windows + signed
   DMG, attaches all 14 artifacts to the draft release, then undrafts it.
6. The undraft fires `release.published` → `Gateway Update` and
   `Release Announcements` both auto-trigger.
7. nova and vega gateways converge to the new version (verified by the
   workflow polling `/version` after the update).
8. A Matrix message lands in `#freenet-locutus:matrix.org`. A River chat
   announcement is sent via nova's release-agent.

## Prerequisites

These repo secrets must be configured under
**Settings → Secrets and variables → Actions**. The workflow degrades
gracefully when any are missing — it just won't auto-cascade and you'll get
a `::warning::` annotation telling you what to fix.

| Secret | Used by | Failure mode if missing |
|---|---|---|
| `RELEASE_PAT` | release.yml, cross-compile.yml | Bump PR has no CI; `release.published` doesn't auto-fire downstream workflows. The workflows emit a `::warning::` on every run. |
| `CARGO_REGISTRY_TOKEN` | release.yml `publish_crates` | crates.io publish fails. |
| `MATRIX_HOMESERVER_URL` | release-announce.yml | Matrix job warns + skips (success, no post). |
| `MATRIX_ACCESS_TOKEN` | release-announce.yml | Matrix job warns + skips. |
| `RELEASE_AGENT_HMAC_NOVA` | gateway-update.yml, release-announce.yml | nova update + River announce fail (HTTP 401). |
| `RELEASE_AGENT_HMAC_VEGA` | gateway-update.yml | vega update fails (HTTP 401). |

`RELEASE_PAT` is a personal access token with `repo` (Contents, Pull
requests, Metadata) and `workflow` scopes. See AGENTS.md → "Release Workflow
& RELEASE_PAT" for the full rationale (GITHUB_TOKEN suppresses
workflow-triggering events as an anti-recursion safeguard, so PAT is
required for the cascade to fire automatically).

## What fires when

```
gh workflow run release.yml
    └─→ release.yml: validate
    └─→ release.yml: update_versions
            └─→ creates "build: release X.Y.Z" PR
            └─→ ci.yml runs on PR (using RELEASE_PAT scope)
            └─→ PR auto-merges to main
    └─→ release.yml: publish_crates
            └─→ cargo publish freenet
            └─→ cargo publish fdev
    └─→ release.yml: create_release
            └─→ git tag -a vX.Y.Z; git push
                    └─→ tag push triggers cross-compile.yml
            └─→ gh release create --draft (release exists but draft)
    cross-compile.yml: matrix builds + DMG sign/notarize
    cross-compile.yml: attach-to-release
            └─→ uploads 14 artifacts
            └─→ gh release edit --draft=false  ← uses RELEASE_PAT
                    └─→ fires release.published event
                            └─→ gateway-update.yml fires
                                    └─→ POST /update to nova (HTTPS)
                                    └─→ POST /update to vega (HTTPS:8443)
                            └─→ release-announce.yml fires
                                    └─→ Matrix message
                                    └─→ POST /announce/river to nova
                                            └─→ nova runs riverctl locally
```

## How to monitor

- **All in one place**:
  <https://github.com/freenet/freenet-core/actions> — release.yml,
  cross-compile.yml, gateway-update.yml, and release-announce.yml runs all
  show up here in rough chronological order.
- **Gateway versions**:
  - `curl https://nova.locut.us/release-agent/version`
  - `curl https://vega.locut.us:8443/release-agent/version`
- **Bump PR**:
  `gh pr list --repo freenet/freenet-core --search "build: release"` —
  there should be exactly one open per release, gone within a few minutes.
- **crates.io propagation**:
  <https://crates.io/crates/freenet> and <https://crates.io/crates/fdev>.

## What to do when something fails

The workflow's failure annotations point to the broken step. The most common
failure modes seen so far:

### `wait_for_pr` timed out

The bump PR is stuck in CI or the merge queue. Open the PR (linked from the
release.yml run), unblock it (push a fix to the branch if needed), let it
merge, then **re-run only the failed jobs** in the release.yml workflow:

```bash
gh run rerun --failed <RUN_ID> --repo freenet/freenet-core
```

The re-run picks up where it left off — `wait_for_pr` will see the merged
state and proceed to `publish_crates`.

### `publish_crates` failed

If it failed with "please provide a non-empty token" or similar, the
`CARGO_REGISTRY_TOKEN` secret is missing or invalid. Update the secret,
then `gh run rerun --failed`.

If a single crate failed mid-publish (e.g. `fdev` failed but `freenet`
succeeded), `cargo publish -p fdev` from a clean main checkout, then
manually move forward to tag + draft release as below.

### `create_release` failed

Most likely cause: tag push failed because git identity wasn't configured on
the runner. Fixed by [PR #4135] — if it recurs, check that the
`Create and push tag` step still has `git config user.name` /
`git config user.email`.

To unblock manually:

```bash
git tag -a vX.Y.Z <main-sha> -m "Release vX.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --title "vX.Y.Z" \
    --notes "Release vX.Y.Z (binaries attached by cross-compile workflow)" \
    --draft
```

`cross-compile.yml` will fire on the tag push and complete the cascade.

### Cascade didn't auto-fire after undraft

Symptom: release is published with all assets attached, but no
`Gateway Update` or `Release Announcements` run shows up.

Root cause: `RELEASE_PAT` is missing or expired. The release was undrafted
using `GITHUB_TOKEN`, which suppresses `release.published`. Update
`RELEASE_PAT` and manually trigger:

```bash
gh workflow run gateway-update.yml --field version=X.Y.Z --field gateways=all
gh workflow run release-announce.yml --field version=X.Y.Z
```

### One gateway didn't converge

The `Gateway Update` workflow polls `/version` for 120 s after the POST. If
that times out the workflow reports failure for that gateway and the
remaining gateway doesn't try (fail-fast). Recovery:

1. Check the agent's status: `ssh ian@<host> 'sudo systemctl status
   freenet-release-agent && sudo journalctl -u freenet-release-agent
   --since "10 min ago"'`
2. If the agent is fine but the script failed, the manual fix is
   `ssh ian@<host> 'sudo /usr/local/bin/gateway-auto-update.sh --force'`.
3. Re-run the gateway-update workflow against just the failed gateway:
   `gh workflow run gateway-update.yml --field version=X.Y.Z --field
   gateways=vega`.

### River announcement failed but Matrix worked

Known issue tracked in [river#241]: the committed delegate/contract WASMs
have wasm-bindgen placeholders that make `riverctl message send` fail with
`unknown import: __wbindgen_placeholder__::__wbindgen_describe`. Until that
is fixed in the River repo, manually post to the Freenet Official room
from a working `riverctl` setup.

## Versioning

Standard semver. The workflow updates the version in:

- `crates/core/Cargo.toml` (freenet)
- `crates/fdev/Cargo.toml` (fdev: patch-bumped each release; freenet
  path-dep version matched)
- `crates/fdev/Cargo.toml` `[package.metadata.binstall].pkg-url` (rewritten
  to embed `vX.Y.Z` so `cargo binstall fdev` finds the freenet release
  artifact)

If you're bumping a major or minor version, double-check the binstall
URL rewrite (a regression test in `crates/fdev/tests/binstall_metadata.rs`
covers this).

## Rollback

The release-agent refuses downgrades by design (`X.Y.Z < installed` returns
403). To recover from a botched release, cut a new patch with the fix:

```bash
gh workflow run release.yml --field version=X.Y.Z+1
```

For a serious regression that needs immediate rollback, the operator path
is `ssh ian@<host> 'sudo gateway-auto-update.sh --force --target-version
vX.Y.Z'` against the previous good version — the script accepts a downgrade
when invoked directly, the agent doesn't.

## Skip mechanisms

`scripts/release.sh` (the legacy local release script) honors two env vars:

- `FREENET_RELEASE_SKIP_ANNOUNCEMENTS=1` — skip the local Matrix + River
  posts (the workflow handles them).
- `FREENET_RELEASE_SKIP_GATEWAY_SSH=1` — skip SSH-based gateway updates
  (the workflow handles them).

Set both if you ever need to run `release.sh` while the workflow is also in
play.

## Post-release verification

After the cascade completes, do these checks (or use the `freenet-release`
verification skill if you have it):

1. <https://crates.io/crates/freenet> shows the new version.
2. <https://github.com/freenet/freenet-core/releases/tag/vX.Y.Z> is
   published (not draft) with 14 assets.
3. `curl https://nova.locut.us/release-agent/version` and
   `curl https://vega.locut.us:8443/release-agent/version` both return the
   new version.
4. Matrix room shows the announcement.
5. `sudo journalctl -u freenet-gateway --since "30 min ago"` on each
   gateway shows no errors.

[PR #4135]: https://github.com/freenet/freenet-core/pull/4135
[river#241]: https://github.com/freenet/river/issues/241
